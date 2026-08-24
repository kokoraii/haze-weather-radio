package playlist

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// Separate pools reserve alert capacity when routine renderers are slow.
	routinePreparationWorkerLimit  = 4
	priorityPreparationWorkerLimit = 4
	priorityPreparationBacklog     = 64
)

type routinePreparation struct {
	planner *feedPlanner
	items   []playlistItem
	ok      bool
}

type priorityPreparationJob struct {
	token   uint64
	feedID  string
	alertID string
	version uint64
	data    map[string]any
}

type preparationResult struct {
	routine  *routinePreparationResult
	priority *priorityPreparationResult
}

type routinePreparationResult struct {
	feedID     string
	token      uint64
	generation uint64
	startup    bool
	insert     bool
	prepared   routinePreparation
}

type priorityPreparationResult struct {
	job      priorityPreparationJob
	prepared priorityAlertPreparation
	err      error
}

type preparationCoordinator struct {
	results         chan preparationResult
	resultSlots     chan struct{}
	routineSlots    chan struct{}
	prioritySlots   chan struct{}
	prepareRoutine  func(context.Context, *feedPlanner, bool, time.Time) routinePreparation
	prepareInsert   func(context.Context, *feedPlanner, map[string]any) routinePreparation
	preparePriority func(context.Context, *feedPlanner, map[string]any) (priorityAlertPreparation, error)
	workers         sync.WaitGroup
}

func newPreparationCoordinator(routineWorkers int, priorityWorkers int) *preparationCoordinator {
	if routineWorkers <= 0 {
		routineWorkers = routinePreparationWorkerLimit
	}
	if priorityWorkers <= 0 {
		priorityWorkers = priorityPreparationWorkerLimit
	}
	return &preparationCoordinator{
		results:       make(chan preparationResult, routineWorkers+priorityWorkers),
		resultSlots:   make(chan struct{}, routineWorkers+priorityWorkers),
		routineSlots:  make(chan struct{}, routineWorkers),
		prioritySlots: make(chan struct{}, priorityWorkers),
		prepareRoutine: func(ctx context.Context, planner *feedPlanner, startup bool, now time.Time) routinePreparation {
			if startup {
				return routinePreparation{planner: planner, items: planner.prepareStartupPrimer(ctx, now), ok: true}
			}
			item, ok := planner.nextPlannedItem(ctx, now)
			if !ok {
				return routinePreparation{planner: planner}
			}
			return routinePreparation{planner: planner, items: []playlistItem{item}, ok: true}
		},
		prepareInsert: func(ctx context.Context, planner *feedPlanner, data map[string]any) routinePreparation {
			return planner.prepareInsert(ctx, data)
		},
		preparePriority: func(ctx context.Context, planner *feedPlanner, data map[string]any) (priorityAlertPreparation, error) {
			return planner.preparePriorityAlert(ctx, data)
		},
	}
}

func (c *preparationCoordinator) tryStartRoutine(parent context.Context, planner *feedPlanner, token uint64, generation uint64, startup bool, now time.Time) (context.CancelFunc, bool) {
	select {
	case c.routineSlots <- struct{}{}:
	default:
		return nil, false
	}
	if !c.reserveResultSlot() {
		<-c.routineSlots
		return nil, false
	}
	workCtx, cancel := context.WithCancel(parent)
	snapshot := cloneFeedPlanner(planner)
	c.workers.Add(1)
	go func() {
		defer c.workers.Done()
		slotHeld := true
		defer func() {
			if slotHeld {
				<-c.routineSlots
			}
		}()
		prepared := c.prepareRoutine(workCtx, snapshot, startup, now)
		<-c.routineSlots
		slotHeld = false
		result := preparationResult{routine: &routinePreparationResult{
			feedID:     planner.feed.ID,
			token:      token,
			generation: generation,
			startup:    startup,
			prepared:   prepared,
		}}
		select {
		case c.results <- result:
		case <-parent.Done():
			discardRoutinePreparation(prepared)
			c.releaseResultSlot()
		}
	}()
	return cancel, true
}

func (c *preparationCoordinator) tryStartInsert(parent context.Context, planner *feedPlanner, token uint64, generation uint64, data map[string]any) (context.CancelFunc, bool) {
	select {
	case c.routineSlots <- struct{}{}:
	default:
		return nil, false
	}
	if !c.reserveResultSlot() {
		<-c.routineSlots
		return nil, false
	}
	workCtx, cancel := context.WithCancel(parent)
	snapshot := cloneFeedPlanner(planner)
	c.workers.Add(1)
	go func() {
		defer c.workers.Done()
		slotHeld := true
		defer func() {
			if slotHeld {
				<-c.routineSlots
			}
		}()
		prepared := c.prepareInsert(workCtx, snapshot, data)
		<-c.routineSlots
		slotHeld = false
		result := preparationResult{routine: &routinePreparationResult{
			feedID:     planner.feed.ID,
			token:      token,
			generation: generation,
			insert:     true,
			prepared:   prepared,
		}}
		select {
		case c.results <- result:
		case <-parent.Done():
			discardRoutinePreparation(prepared)
			c.releaseResultSlot()
		}
	}()
	return cancel, true
}

func (c *preparationCoordinator) tryStartPriority(parent context.Context, planner *feedPlanner, job priorityPreparationJob) (context.CancelFunc, bool) {
	select {
	case c.prioritySlots <- struct{}{}:
	default:
		return nil, false
	}
	if !c.reserveResultSlot() {
		<-c.prioritySlots
		return nil, false
	}
	workCtx, cancel := context.WithCancel(parent)
	snapshot := cloneFeedPlanner(planner)
	c.workers.Add(1)
	go func() {
		defer c.workers.Done()
		slotHeld := true
		defer func() {
			if slotHeld {
				<-c.prioritySlots
			}
		}()
		prepared, err := c.preparePriority(workCtx, snapshot, job.data)
		<-c.prioritySlots
		slotHeld = false
		result := preparationResult{priority: &priorityPreparationResult{job: job, prepared: prepared, err: err}}
		select {
		case c.results <- result:
		case <-parent.Done():
			snapshot.discardPriorityAlertPreparation(prepared)
			c.releaseResultSlot()
		}
	}()
	return cancel, true
}

func (c *preparationCoordinator) reserveResultSlot() bool {
	if c.resultSlots == nil {
		return true
	}
	select {
	case c.resultSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *preparationCoordinator) releaseResultSlot() {
	if c.resultSlots == nil {
		return
	}
	select {
	case <-c.resultSlots:
	default:
	}
}

func (c *preparationCoordinator) wait() {
	c.workers.Wait()
}

func (s *Service) discardUncommittedPreparations() {
	if s.preparations == nil {
		return
	}
	for {
		select {
		case result := <-s.preparations.results:
			s.preparations.releaseResultSlot()
			if result.routine != nil {
				discardRoutinePreparation(result.routine.prepared)
			}
			if result.priority != nil {
				if planner := s.feeds[result.priority.job.feedID]; planner != nil {
					planner.discardPriorityAlertPreparation(result.priority.prepared)
				}
			}
		default:
			return
		}
	}
}

func cloneFeedPlanner(planner *feedPlanner) *feedPlanner {
	clone := *planner
	clone.queue = append([]playlistItem(nil), planner.queue...)
	if planner.current != nil {
		current := *planner.current
		clone.current = &current
	}
	clone.lastFixed = make(map[string]time.Time, len(planner.lastFixed))
	for key, value := range planner.lastFixed {
		clone.lastFixed[key] = value
	}
	return &clone
}

func discardRoutinePreparation(prepared routinePreparation) {
	planner := prepared.planner
	if planner == nil || strings.TrimSpace(planner.cfg.OutputDir) == "" {
		return
	}
	root, err := filepath.Abs(planner.cfg.OutputDir)
	if err != nil {
		return
	}
	for _, item := range prepared.items {
		path := strings.TrimSpace(item.AudioPath)
		if path == "" {
			continue
		}
		if strings.TrimSpace(item.QueueID) == "" {
			continue
		}
		queueID := safeID(item.QueueID)
		if !strings.HasPrefix(filepath.Base(path), queueID) {
			continue
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		relative, err := filepath.Rel(root, absolute)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		_ = os.Remove(absolute)
	}
}

type activePriorityPreparation struct {
	token   uint64
	alertID string
	version uint64
	cancel  context.CancelFunc
}

type feedPreparationState struct {
	routineGeneration uint64
	routineToken      uint64
	routineInFlight   bool
	routineCancel     context.CancelFunc
	priorityVersions  map[string]uint64
	priorityActive    *activePriorityPreparation
}

func (s *Service) preparationState(feedID string) *feedPreparationState {
	if s.preparationStates == nil {
		s.preparationStates = make(map[string]*feedPreparationState, len(s.feeds))
	}
	state := s.preparationStates[feedID]
	if state == nil {
		state = &feedPreparationState{priorityVersions: map[string]uint64{}}
		s.preparationStates[feedID] = state
	}
	if state.priorityVersions == nil {
		state.priorityVersions = map[string]uint64{}
	}
	return state
}

func (s *Service) tickFeeds(ctx context.Context, now time.Time, completedFeedID string) {
	if ctx.Err() != nil {
		return
	}
	for feedID, planner := range s.feeds {
		if feedID == completedFeedID {
			continue
		}
		if s.preparations == nil {
			planner.tick(ctx, now, s.options.Lookahead)
			continue
		}
		s.tickFeedAsync(ctx, planner, now)
	}
	if completedFeedID == "" {
		return
	}
	planner := s.feeds[completedFeedID]
	if planner == nil {
		return
	}
	if s.preparations == nil {
		planner.tick(ctx, now, s.options.Lookahead)
		return
	}
	s.tickFeedAsync(ctx, planner, now)
}

func (s *Service) tickFeedAsync(ctx context.Context, planner *feedPlanner, now time.Time) {
	if !feedRoutineEnabled(planner.feed) {
		return
	}
	if planner.mode != "running" || planner.priorityActive > 0 || s.hasPriorityPreparation(planner.feed.ID) {
		planner.writeState()
		return
	}
	if planner.startupPrimerPending {
		if now.Before(planner.startupPrimerAt) {
			planner.writeState()
			return
		}
		s.startRoutinePreparation(ctx, planner, now, true)
		planner.writeState()
		return
	}
	planner.dropCompleted()
	planner.replayPendingItemsIfDue(now)
	if !planner.nextRoutineRetryAt.IsZero() && now.Before(planner.nextRoutineRetryAt) {
		planner.writeState()
		return
	}
	maxQueued := planner.cfg.Root.Services.Go.Playlist.MaxQueued
	lookahead := s.options.Lookahead
	if lookahead <= 0 {
		lookahead = 2 * time.Minute
	}
	if planner.queuedCount() >= maxQueued || !planner.timelineEnd(now).Before(now.Add(lookahead)) {
		planner.writeState()
		return
	}
	s.startRoutinePreparation(ctx, planner, now, false)
	planner.writeState()
}

func (s *Service) hasPriorityPreparation(feedID string) bool {
	if state := s.preparationStates[feedID]; state != nil && state.priorityActive != nil {
		return true
	}
	for _, pending := range s.priorityPending {
		if pending.feedID == feedID {
			return true
		}
	}
	return false
}

func (s *Service) startRoutinePreparation(ctx context.Context, planner *feedPlanner, now time.Time, startup bool) {
	state := s.preparationState(planner.feed.ID)
	if state.routineInFlight {
		return
	}
	state.routineToken++
	token := state.routineToken
	cancel, ok := s.preparations.tryStartRoutine(ctx, planner, token, state.routineGeneration, startup, now)
	if !ok {
		return
	}
	state.routineInFlight = true
	state.routineCancel = cancel
}

func (s *Service) startRoutineInsert(ctx context.Context, planner *feedPlanner, data map[string]any) {
	state := s.preparationState(planner.feed.ID)
	token := state.routineToken + 1
	cancel, ok := s.preparations.tryStartInsert(ctx, planner, token, state.routineGeneration, data)
	if !ok {
		planner.lastError = "routine preparation worker pool is full; insert was dropped"
		planner.writeState()
		return
	}
	state.routineToken = token
	state.routineInFlight = true
	state.routineCancel = cancel
}

func (s *Service) invalidateRoutinePreparation(planner *feedPlanner) {
	if s.preparations == nil || planner == nil {
		return
	}
	state := s.preparationState(planner.feed.ID)
	state.routineGeneration++
	if state.routineCancel != nil {
		state.routineCancel()
	}
}

func (s *Service) enqueuePriorityPreparation(ctx context.Context, planner *feedPlanner, data map[string]any) error {
	data, alertID := normalizePriorityAlertRequest(data)
	if priorityAlertRequestStale(data, time.Now().UTC()) {
		cleanupSupersededAlertQueueParts(planner.cfg.BaseDir, planner.feed.ID, alertID, "")
		planner.lastError = ""
		planner.writeState()
		return fmt.Errorf("priority alert request is stale")
	}
	s.invalidateRoutinePreparation(planner)
	state := s.preparationState(planner.feed.ID)
	s.pruneStalePriorityPreparations(time.Now().UTC())
	filtered := make([]priorityPreparationJob, 0, len(s.priorityPending))
	for _, pending := range s.priorityPending {
		if pending.feedID == planner.feed.ID && pending.alertID == alertID {
			continue
		}
		filtered = append(filtered, pending)
	}
	limit := s.maxPriorityPending
	if limit <= 0 {
		limit = priorityPreparationBacklog
	}
	if len(filtered) >= limit {
		planner.lastError = fmt.Sprintf("priority preparation backlog is full (%d)", limit)
		planner.writeState()
		return fmt.Errorf("enqueue priority alert: %s", planner.lastError)
	}
	s.priorityPending = filtered
	state.priorityVersions[alertID]++
	version := state.priorityVersions[alertID]
	if state.priorityActive != nil && state.priorityActive.alertID == alertID {
		state.priorityActive.cancel()
	}
	s.nextPreparationToken++
	s.priorityPending = append(s.priorityPending, priorityPreparationJob{
		token:   s.nextPreparationToken,
		feedID:  planner.feed.ID,
		alertID: alertID,
		version: version,
		data:    data,
	})
	s.startPendingPriorityPreparations(ctx)
	return nil
}

func (s *Service) startPendingPriorityPreparations(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	s.pruneStalePriorityPreparations(time.Now().UTC())
	for {
		index := -1
		for candidate, pending := range s.priorityPending {
			planner := s.feeds[pending.feedID]
			if planner == nil || s.preparationState(pending.feedID).priorityActive != nil {
				continue
			}
			index = candidate
			break
		}
		if index < 0 {
			return
		}
		job := s.priorityPending[index]
		planner := s.feeds[job.feedID]
		cancel, ok := s.preparations.tryStartPriority(ctx, planner, job)
		if !ok {
			return
		}
		s.priorityPending = append(s.priorityPending[:index], s.priorityPending[index+1:]...)
		s.preparationState(job.feedID).priorityActive = &activePriorityPreparation{
			token: job.token, alertID: job.alertID, version: job.version, cancel: cancel,
		}
	}
}

func (s *Service) pruneStalePriorityPreparations(now time.Time) {
	if len(s.priorityPending) == 0 {
		return
	}
	filtered := s.priorityPending[:0]
	removed := make([]priorityPreparationJob, 0)
	for _, pending := range s.priorityPending {
		if priorityAlertRequestStale(pending.data, now) {
			if planner := s.feeds[pending.feedID]; planner != nil {
				cleanupSupersededAlertQueueParts(planner.cfg.BaseDir, pending.feedID, pending.alertID, "")
			}
			removed = append(removed, pending)
			continue
		}
		filtered = append(filtered, pending)
	}
	s.priorityPending = filtered
	for _, pending := range removed {
		s.releasePriorityVersion(pending.feedID, pending.alertID)
	}
}

func (s *Service) cancelPriorityPreparations(planner *feedPlanner, data map[string]any) {
	if s.preparations == nil || planner == nil {
		return
	}
	ids := priorityAlertIDs(data)
	if len(ids) == 0 {
		return
	}
	state := s.preparationState(planner.feed.ID)
	cancelled := make(map[string]struct{}, len(ids))
	for _, alertID := range ids {
		cancelled[alertID] = struct{}{}
		state.priorityVersions[alertID]++
	}
	filtered := s.priorityPending[:0]
	for _, pending := range s.priorityPending {
		if pending.feedID == planner.feed.ID {
			if _, ok := cancelled[pending.alertID]; ok {
				continue
			}
		}
		filtered = append(filtered, pending)
	}
	s.priorityPending = filtered
	if state.priorityActive != nil {
		if _, ok := cancelled[state.priorityActive.alertID]; ok {
			state.priorityActive.cancel()
		}
	}
	for _, alertID := range ids {
		s.releasePriorityVersion(planner.feed.ID, alertID)
	}
}

func (s *Service) releasePriorityVersion(feedID string, alertID string) {
	state := s.preparationStates[feedID]
	if state == nil {
		return
	}
	if state.priorityActive != nil && state.priorityActive.alertID == alertID {
		return
	}
	for _, pending := range s.priorityPending {
		if pending.feedID == feedID && pending.alertID == alertID {
			return
		}
	}
	delete(state.priorityVersions, alertID)
}

func priorityAlertIDs(data map[string]any) []string {
	ids := stringListAny(firstValue(nil, data, "alert_ids"))
	if id := firstText(nil, data, "alert_id", "id", "subject"); id != "" {
		ids = append(ids, id)
	}
	return uniqueStrings(ids)
}

func (s *Service) handlePreparationResult(ctx context.Context, result preparationResult) {
	if s.preparations != nil {
		s.preparations.releaseResultSlot()
	}
	if result.routine != nil {
		s.handleRoutinePreparationResult(ctx, *result.routine)
	}
	if result.priority != nil {
		s.handlePriorityPreparationResult(ctx, *result.priority)
	}
}

func (s *Service) handleRoutinePreparationResult(ctx context.Context, result routinePreparationResult) {
	planner := s.feeds[result.feedID]
	if planner == nil {
		return
	}
	state := s.preparationState(result.feedID)
	if state.routineInFlight && state.routineToken == result.token {
		if state.routineCancel != nil {
			state.routineCancel()
		}
		state.routineInFlight = false
		state.routineCancel = nil
	}
	if result.generation != state.routineGeneration || state.routineToken != result.token {
		discardRoutinePreparation(result.prepared)
		s.tickFeeds(ctx, time.Now(), result.feedID)
		return
	}
	applyRoutinePlanningState(planner, result.prepared.planner)
	if result.insert {
		if result.prepared.ok {
			for _, item := range result.prepared.items {
				item = planner.rebasePreparedItem(item, time.Now())
				planner.queue = append(planner.queue, item)
				if err := planner.publishReady(item); err != nil {
					planner.lastError = err.Error()
					break
				}
			}
		}
		planner.writeState()
		s.tickFeeds(ctx, time.Now(), result.feedID)
		return
	}
	if result.startup {
		planner.startupPrimerPending = false
	}
	if result.prepared.ok && planner.mode == "running" && planner.priorityActive == 0 {
		for _, item := range result.prepared.items {
			item = planner.rebasePreparedItem(item, time.Now())
			planner.queue = append(planner.queue, item)
			if err := planner.publishReady(item); err != nil {
				planner.lastError = err.Error()
				break
			}
		}
	} else {
		discardRoutinePreparation(result.prepared)
	}
	planner.writeState()
	s.tickFeeds(ctx, time.Now(), result.feedID)
}

func applyRoutinePlanningState(planner *feedPlanner, prepared *feedPlanner) {
	if prepared == nil {
		return
	}
	planner.cursor = prepared.cursor
	planner.routineLanguageCount = prepared.routineLanguageCount
	planner.routineAltLangIndex = prepared.routineAltLangIndex
	planner.nextRoutineRetryAt = prepared.nextRoutineRetryAt
	planner.lastError = prepared.lastError
	planner.lastFixed = prepared.lastFixed
}

func (p *feedPlanner) rebasePreparedItem(item playlistItem, now time.Time) playlistItem {
	target := parseTime(item.TargetStartAt)
	start := predictedStart(now, p.timelineEnd(now), target)
	item.QueuedAt = now.UTC().Format(time.RFC3339Nano)
	item.PredictedStartAt = start.UTC().Format(time.RFC3339Nano)
	item.PredictedFinishAt = start.Add(p.itemScheduleDuration(item.DurationMS)).UTC().Format(time.RFC3339Nano)
	item.Status = "queued"
	return item
}

func (s *Service) handlePriorityPreparationResult(ctx context.Context, result priorityPreparationResult) {
	planner := s.feeds[result.job.feedID]
	if planner == nil {
		return
	}
	state := s.preparationState(result.job.feedID)
	active := state.priorityActive
	if active != nil && active.token == result.job.token {
		active.cancel()
		state.priorityActive = nil
	}
	defer s.releasePriorityVersion(result.job.feedID, result.job.alertID)
	current := state.priorityVersions[result.job.alertID] == result.job.version
	if active == nil || active.token != result.job.token || !current || priorityAlertRequestStale(result.job.data, time.Now().UTC()) {
		planner.discardPriorityAlertPreparation(result.prepared)
		if current && priorityAlertRequestStale(result.job.data, time.Now().UTC()) {
			cleanupSupersededAlertQueueParts(planner.cfg.BaseDir, planner.feed.ID, result.job.alertID, "")
			planner.publishAutomationDispatchStatus(result.job.data, "failed", "priority alert request became stale during preparation")
		}
		s.startPendingPriorityPreparations(ctx)
		return
	}
	if result.err != nil {
		planner.discardPriorityAlertPreparation(result.prepared)
		planner.lastError = result.err.Error()
		planner.writeState()
		planner.publishAutomationDispatchStatus(result.job.data, "failed", planner.lastError)
		s.startPendingPriorityPreparations(ctx)
		return
	}
	if err := planner.commitPriorityAlert(result.prepared); err != nil {
		planner.discardPriorityAlertPreparation(result.prepared)
		planner.lastError = err.Error()
		planner.publishAutomationDispatchStatus(result.job.data, "failed", planner.lastError)
	} else {
		planner.lastError = ""
	}
	planner.writeState()
	s.startPendingPriorityPreparations(ctx)
}
