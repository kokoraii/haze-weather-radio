package playlist

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/alerttext"
	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
	"github.com/meowraii/haze-weather-radio/services/go/internal/lead"
)

const serviceID = "haze-playlist"
const routineRetryDelay = 30 * time.Second
const startupPrimerDelay = 2 * time.Second
const pendingReplayInterval = 2 * time.Second
const cachedRoutineFallbackMaxAge = 15 * time.Minute
const cachedStartupFallbackMaxAge = 30 * time.Minute
const productSegmentWorkerLimit = 4
const configuredAlertLeadMaxBytes int64 = 16 << 20

var errSystemShutdown = errors.New("system shutdown requested")

func Run(ctx context.Context, options Options) error {
	if strings.TrimSpace(options.ConfigPath) == "" {
		options.ConfigPath = "config.yaml"
	}
	if options.Tick <= 0 {
		options.Tick = 500 * time.Millisecond
	}
	if options.Lookahead <= 0 {
		options.Lookahead = 2 * time.Minute
	}
	for {
		cfg, err := loadConfig(options.ConfigPath, options.OutDir)
		if err != nil {
			return err
		}
		store, err := datastore.Open(ctx, cfg.Root.Storage, cfg.BaseDir)
		if err != nil {
			return err
		}
		cfg.Store = store
		bridge, err := connectBridge(ctx, options.BridgeAddr)
		if err != nil {
			store.Close()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("playlist waiting for event bridge: %v", err)
			sleepOrDone(ctx, time.Second)
			continue
		}
		service := newService(cfg, bridge, options)
		err = service.runConnected(ctx)
		_ = bridge.Close()
		store.Close()
		if errors.Is(err, errSystemShutdown) {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("playlist event bridge disconnected: %v", err)
		sleepOrDone(ctx, time.Second)
	}
}

type Service struct {
	cfg                  loadedConfig
	bridge               *bridgeClient
	options              Options
	feeds                map[string]*feedPlanner
	preparations         *preparationCoordinator
	preparationStates    map[string]*feedPreparationState
	priorityPending      []priorityPreparationJob
	nextPreparationToken uint64
	maxPriorityPending   int
}

func newService(cfg loadedConfig, bridge *bridgeClient, options Options) *Service {
	feeds := map[string]*feedPlanner{}
	for _, feed := range cfg.enabledFeeds() {
		feeds[feed.ID] = newFeedPlanner(cfg, bridge, feed)
	}
	return &Service{
		cfg:                cfg,
		bridge:             bridge,
		options:            options,
		feeds:              feeds,
		preparations:       newPreparationCoordinator(0, 0),
		preparationStates:  make(map[string]*feedPreparationState, len(feeds)),
		maxPriorityPending: priorityPreparationBacklog,
	}
}

func (s *Service) runConnected(ctx context.Context) error {
	connectedCtx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		if s.preparations != nil {
			s.preparations.wait()
			s.discardUncommittedPreparations()
		}
	}()
	_ = s.bridge.Publish(map[string]any{
		"type":   "service.ready",
		"source": serviceID,
		"data": map[string]any{
			"service": serviceID,
			"feeds":   len(s.feeds),
		},
	})
	ticker := time.NewTicker(s.options.Tick)
	defer ticker.Stop()
	var preparationResults <-chan preparationResult
	if s.preparations != nil {
		preparationResults = s.preparations.results
	}
	events := s.bridge.Events()
	criticalEvents := s.bridge.CriticalEvents()
	for events != nil || criticalEvents != nil {
		// Give CAP broadcast and cancellation traffic a chance to run before
		// ordinary bridge traffic when both lanes are ready.
		if criticalEvents != nil {
			select {
			case event, ok := <-criticalEvents:
				if !ok {
					criticalEvents = nil
					continue
				}
				if stringAt(event, "type") == "system.shutdown" {
					return errSystemShutdown
				}
				s.handleEvent(connectedCtx, event)
				continue
			default:
			}
		}
		select {
		case <-connectedCtx.Done():
			return connectedCtx.Err()
		case event, ok := <-events:
			if !ok {
				events = nil
				if criticalEvents == nil {
					return fmt.Errorf("bridge event stream closed")
				}
				continue
			}
			if stringAt(event, "type") == "system.shutdown" {
				return errSystemShutdown
			}
			s.handleEvent(connectedCtx, event)
		case event, ok := <-criticalEvents:
			if !ok {
				criticalEvents = nil
				if events == nil {
					return fmt.Errorf("bridge event stream closed")
				}
				continue
			}
			if stringAt(event, "type") == "system.shutdown" {
				return errSystemShutdown
			}
			s.handleEvent(connectedCtx, event)
		case result := <-preparationResults:
			s.handlePreparationResult(connectedCtx, result)
		case now := <-ticker.C:
			s.tickFeeds(connectedCtx, now, "")
		}
	}
	return fmt.Errorf("bridge event stream closed")
}

func (s *Service) handleEvent(ctx context.Context, event map[string]any) {
	switch stringAt(event, "type") {
	case "playlist.control":
		data := mapAt(event, "data")
		feedID := firstText(event, data, "feed_id")
		action := firstText(event, data, "action")
		for _, planner := range s.matchFeeds(feedID) {
			s.invalidateRoutinePreparation(planner)
			planner.applyControl(action)
		}
	case "playlist.insert":
		data := mapAt(event, "data")
		feedID := firstText(event, data, "feed_id")
		for _, planner := range s.matchFeeds(feedID) {
			s.invalidateRoutinePreparation(planner)
			if s.preparations == nil {
				planner.insert(ctx, data)
				continue
			}
			planner.prepareInsertPosition(data)
			s.startRoutineInsert(ctx, planner, data)
		}
	case "cap.alert.broadcast.requested":
		data := eventDataWithIdentity(event)
		for _, planner := range s.matchFeedsFromEvent(event) {
			if s.preparations == nil {
				planner.publishAutomationDispatchStatus(data, "accepted", "")
				planner.queuePriorityAlert(ctx, data)
				continue
			}
			if err := s.enqueuePriorityPreparation(ctx, planner, data); err != nil {
				planner.publishAutomationDispatchStatus(data, "failed", err.Error())
				continue
			}
			planner.publishAutomationDispatchStatus(data, "accepted", "")
		}
	case "cap.alert.cancelled":
		data := eventDataWithIdentity(event)
		for _, planner := range s.matchFeedsFromEvent(event) {
			s.cancelPriorityPreparations(planner, data)
			s.invalidateRoutinePreparation(planner)
			planner.cancelPriorityAlerts(data)
			planner.invalidateRoutineAlerts(true)
		}
	case "cap.alert.registry.updated":
		for _, planner := range s.matchFeedsFromEvent(event) {
			s.invalidateRoutinePreparation(planner)
			planner.invalidateRoutineAlerts(false)
		}
	case "playout.started":
		data := mapAt(event, "data")
		feedID := firstText(event, data, "feed_id")
		if feedID == "" {
			feedID = stringAt(event, "feed_id")
		}
		if planner := s.feeds[feedID]; planner != nil {
			planner.markStarted(firstText(event, data, "queue_id"))
		}
	case "playout.interrupted":
		data := mapAt(event, "data")
		feedID := firstText(event, data, "feed_id")
		if feedID == "" {
			feedID = stringAt(event, "feed_id")
		}
		if planner := s.feeds[feedID]; planner != nil {
			planner.markInterrupted(firstText(event, data, "queue_id"))
		}
	case "playout.completed":
		data := mapAt(event, "data")
		feedID := firstText(event, data, "feed_id")
		if feedID == "" {
			feedID = stringAt(event, "feed_id")
		}
		if planner := s.feeds[feedID]; planner != nil {
			planner.markCompleted(firstText(event, data, "queue_id"))
		}
	case "alert.playout.started":
		for _, planner := range s.matchFeedsFromEvent(event) {
			s.invalidateRoutinePreparation(planner)
			planner.markPriorityStarted()
		}
	case "alert.playout.completed":
		for _, planner := range s.matchFeedsFromEvent(event) {
			planner.markPriorityCompleted()
		}
	case "service.ready":
		if !isPlayoutReadyEvent(event) {
			return
		}
		for _, planner := range s.feeds {
			planner.replayPendingItems()
		}
	}
}

func eventDataWithIdentity(event map[string]any) map[string]any {
	data := mapAt(event, "data")
	copyData := make(map[string]any, len(data)+2)
	for key, value := range data {
		copyData[key] = value
	}
	if firstText(nil, copyData, "alert_id", "id", "subject") == "" {
		if subject := firstText(event, nil, "alert_id", "id", "subject"); subject != "" {
			copyData["alert_id"] = subject
		}
	}
	if firstText(nil, copyData, "feed_id") == "" {
		if feedID := firstText(event, nil, "feed_id"); feedID != "" {
			copyData["feed_id"] = feedID
		}
	}
	if _, ok := copyData["alert_ids"]; !ok {
		if alertIDs, ok := event["alert_ids"]; ok {
			copyData["alert_ids"] = alertIDs
		}
	}
	return copyData
}

func (s *Service) matchFeeds(feedID string) []*feedPlanner {
	feedID = strings.TrimSpace(feedID)
	if feedID == "" || feedID == "*" {
		out := make([]*feedPlanner, 0, len(s.feeds))
		for _, planner := range s.feeds {
			out = append(out, planner)
		}
		return out
	}
	if planner := s.feeds[feedID]; planner != nil {
		return []*feedPlanner{planner}
	}
	return nil
}

func (s *Service) matchFeedsFromEvent(event map[string]any) []*feedPlanner {
	data := mapAt(event, "data")
	out := []*feedPlanner{}
	seen := map[string]struct{}{}
	add := func(feedID string) {
		for _, planner := range s.matchFeeds(feedID) {
			if _, ok := seen[planner.feed.ID]; ok {
				continue
			}
			seen[planner.feed.ID] = struct{}{}
			out = append(out, planner)
		}
	}
	if feedID := firstText(event, data, "feed_id"); feedID != "" {
		add(feedID)
	}
	for _, feedID := range stringListAny(firstValue(event, data, "feed_ids")) {
		add(feedID)
	}
	return out
}

type feedPlanner struct {
	cfg                  loadedConfig
	bridge               *bridgeClient
	feed                 feedXML
	sameGenerator        sameGeneratorFunc
	segmentSynthesizer   func(context.Context, int, string) ([]byte, error)
	mode                 string
	modeBeforePriority   string
	priorityActive       int
	startupPrimerAt      time.Time
	startupPrimerPending bool
	pendingAfterCurrent  string
	cursor               int
	routineLanguageCount int
	routineAltLangIndex  int
	nextRoutineRetryAt   time.Time
	lastPendingReplayAt  time.Time
	queue                []playlistItem
	current              *playlistItem
	lastFixed            map[string]time.Time
	lastError            string
}

type playlistItem struct {
	QueueID           string `json:"queue_id"`
	FeedID            string `json:"feed_id"`
	Kind              string `json:"kind"`
	PackageID         string `json:"package_id,omitempty"`
	Title             string `json:"title"`
	AudioPath         string `json:"audio_path"`
	DurationMS        int64  `json:"duration_ms"`
	QueuedAt          string `json:"queued_at"`
	TargetStartAt     string `json:"target_start_at"`
	PredictedStartAt  string `json:"predicted_start_at"`
	PredictedFinishAt string `json:"predicted_finish_at"`
	Status            string `json:"status"`
	Source            string `json:"source"`
}

type priorityAlertManifest struct {
	ID                 string             `json:"id"`
	AlertID            string             `json:"alert_id,omitempty"`
	AlertPacket        *alertmodel.Packet `json:"alert_packet,omitempty"`
	Type               string             `json:"type"`
	Status             string             `json:"status"`
	CreatedAt          string             `json:"created_at"`
	FeedIDs            []string           `json:"feed_ids"`
	Header             string             `json:"header"`
	Event              string             `json:"event"`
	AlertText          string             `json:"alert_text,omitempty"`
	BannerText         string             `json:"banner_text,omitempty"`
	AlertSentAt        string             `json:"alert_sent_at,omitempty"`
	AlertExpiresAt     string             `json:"alert_expires_at,omitempty"`
	MessageType        string             `json:"message_type,omitempty"`
	BroadcastImmediate bool               `json:"broadcast_immediate,omitempty"`
	AudioPath          string             `json:"audio_path"`
	Format             string             `json:"format"`
	SampleRate         int                `json:"sample_rate"`
	Channels           int                `json:"channels"`
	AudioBytes         int                `json:"audio_bytes"`
	Source             string             `json:"source"`
	Priority           string             `json:"priority"`
	AuthoritativeURL   string             `json:"authoritative_url,omitempty"`
	LastError          string             `json:"last_error,omitempty"`
}

type priorityAlertPreparation struct {
	Manifest       priorityAlertManifest
	Data           map[string]any
	AlertText      string
	SAMEHeader     string
	AudioPath      string
	FinalAudioPath string
}

type fixedEvent struct {
	Kind      string
	PackageID string
	Title     string
	Target    time.Time
}

func newFeedPlanner(cfg loadedConfig, bridge *bridgeClient, feed feedXML) *feedPlanner {
	return &feedPlanner{
		cfg:                  cfg,
		bridge:               bridge,
		feed:                 feed,
		mode:                 "running",
		startupPrimerAt:      time.Now().Add(startupPrimerDelay),
		startupPrimerPending: true,
		lastFixed:            map[string]time.Time{},
	}
}

func (p *feedPlanner) tick(ctx context.Context, now time.Time, lookahead time.Duration) {
	if !feedRoutineEnabled(p.feed) {
		return
	}
	if p.mode != "running" {
		p.writeState()
		return
	}
	if p.priorityActive > 0 {
		p.writeState()
		return
	}
	if p.startupPrimerPending {
		if now.Before(p.startupPrimerAt) {
			p.writeState()
			return
		}
		p.queueStartupPrimer(ctx, now)
		p.startupPrimerPending = false
		p.writeState()
		return
	}
	p.dropCompleted()
	p.replayPendingItemsIfDue(now)
	maxQueued := p.cfg.Root.Services.Go.Playlist.MaxQueued
	for p.queuedCount() < maxQueued && p.timelineEnd(now).Before(now.Add(lookahead)) {
		next, ok := p.nextPlannedItem(ctx, now)
		if !ok {
			break
		}
		p.queue = append(p.queue, next)
		if err := p.publishReady(next); err != nil {
			p.lastError = err.Error()
			break
		}
	}
	p.writeState()
}

func (p *feedPlanner) nextPlannedItem(ctx context.Context, now time.Time) (playlistItem, bool) {
	if !p.nextRoutineRetryAt.IsZero() && now.Before(p.nextRoutineRetryAt) {
		return playlistItem{}, false
	}
	timelineEnd := p.timelineEnd(now)
	nextFixed, hasFixed := p.nextFixedEvent(now, timelineEnd)
	if hasFixed && shouldFrontLoadFixed(now, nextFixed, p.cfg.Root.Services.Go.Playlist.FixedToleranceS) {
		item, err := p.buildFixed(ctx, nextFixed, timelineEnd, now)
		if err != nil {
			p.lastError = err.Error()
			return playlistItem{}, false
		}
		p.lastFixed[fixedEventKey(nextFixed)] = nextFixed.Target
		return item, true
	}
	pkgID := p.nextRoutinePackage()
	failures := make([]string, 0, len(p.cfg.Root.Playout.PlaylistOrder))
	for attempts := 0; pkgID != "" && attempts < len(p.cfg.Root.Playout.PlaylistOrder); attempts++ {
		item, err := p.buildProduct(ctx, pkgID, "routine", "", timelineEnd, now)
		if err == nil {
			p.nextRoutineRetryAt = time.Time{}
			p.lastError = ""
			if hasFixed &&
				shouldFrontLoadFixed(now, nextFixed, p.cfg.Root.Services.Go.Playlist.FixedToleranceS) &&
				routineItemWouldCrowdFixed(item, nextFixed, p.cfg.Root.Services.Go.Playlist.FixedToleranceS) {
				fixed, fixedErr := p.buildFixed(ctx, nextFixed, timelineEnd, now)
				if fixedErr != nil {
					p.lastError = fixedErr.Error()
					return playlistItem{}, false
				}
				p.lastFixed[fixedEventKey(nextFixed)] = nextFixed.Target
				return fixed, true
			}
			return item, true
		}
		p.lastError = err.Error()
		failures = append(failures, fmt.Sprintf("%s: %v", pkgID, err))
		pkgID = p.nextRoutinePackage()
	}
	p.nextRoutineRetryAt = now.Add(routineRetryDelay)
	detail := strings.Join(failures, "; ")
	if detail == "" {
		detail = "no routine package could be selected"
	}
	p.lastError = fmt.Sprintf("routine products unavailable (%s); retrying at %s", detail, p.nextRoutineRetryAt.UTC().Format(time.RFC3339))
	return playlistItem{}, false
}

func routineItemWouldCrowdFixed(item playlistItem, event fixedEvent, toleranceSeconds int) bool {
	if event.Target.IsZero() {
		return false
	}
	finish := parseTime(item.PredictedFinishAt)
	if finish.IsZero() {
		return false
	}
	tolerance := time.Duration(toleranceSeconds) * time.Second
	return finish.After(event.Target.Add(tolerance))
}

func shouldFrontLoadFixed(timelineEnd time.Time, event fixedEvent, toleranceSeconds int) bool {
	if event.Target.IsZero() {
		return false
	}
	tolerance := time.Duration(toleranceSeconds) * time.Second
	return !timelineEnd.Before(event.Target.Add(-tolerance))
}

func (p *feedPlanner) nextRoutinePackage() string {
	order := p.cfg.Root.Playout.PlaylistOrder
	for range order {
		pkgID := strings.TrimSpace(order[p.cursor%len(order)])
		p.cursor++
		if pkgID == "" {
			continue
		}
		if pkgID == "alerts" && !p.hasRoutineAlerts(time.Now()) {
			continue
		}
		return pkgID
	}
	return ""
}

func (p *feedPlanner) advanceRoutineCursorPast(pkgID string) {
	order := p.cfg.Root.Playout.PlaylistOrder
	if len(order) == 0 {
		return
	}
	for index, candidate := range order {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(pkgID)) {
			p.cursor = index + 1
			return
		}
	}
}

func (p *feedPlanner) productLanguageForBuild(source string) string {
	if !strings.EqualFold(source, "routine") {
		return feedLanguage(p.feed)
	}
	return p.nextRoutineLanguage()
}

func (p *feedPlanner) nextRoutineLanguage() string {
	languages := feedLanguages(p.feed)
	if len(languages) == 0 {
		return "en-US"
	}
	if len(languages) == 1 {
		return languages[0].Code
	}
	p.routineLanguageCount++
	count := p.routineLanguageCount
	due := make([]feedLangXML, 0, len(languages)-1)
	for _, lang := range languages[1:] {
		period := languageInterval(lang.Interval) + 1
		if count%period == 0 {
			due = append(due, lang)
		}
	}
	if len(due) == 0 {
		return languages[0].Code
	}
	selected := due[p.routineAltLangIndex%len(due)].Code
	p.routineAltLangIndex++
	return selected
}

func languageInterval(raw string) int {
	interval, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || interval < 1 {
		return 1
	}
	return interval
}

func productQueuePrefix(pkgID string, language string) string {
	language = normalizeLangKey(language)
	if language == "" {
		return pkgID
	}
	return pkgID + "-" + language
}

func (p *feedPlanner) nextFixedEvent(now time.Time, timelineEnd time.Time) (fixedEvent, bool) {
	loc := feedLocation(p.feed)
	from := now.In(loc).Add(-time.Second)
	until := now.In(loc).Add(3 * time.Minute)
	candidates := []fixedEvent{}
	if p.cfg.Root.Playout.StationIDSchedule.enabled(true) {
		for _, minute := range p.cfg.Root.Playout.StationIDSchedule.minutesOr([]int{0, 15, 30, 45}) {
			if target, ok := minuteTarget(from, until, minute); ok {
				candidates = append(candidates, fixedEvent{Kind: "station_id", PackageID: "station_id", Title: "Station Identification", Target: target})
			}
		}
	}
	if p.cfg.Root.Playout.DateTimeSchedule.enabled(true) {
		for _, minute := range p.cfg.Root.Playout.DateTimeSchedule.minutesOr([]int{0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}) {
			if target, ok := minuteTarget(from, until, minute); ok {
				candidates = append(candidates, fixedEvent{Kind: "date_time", PackageID: "date_time", Title: "Date and Time", Target: target})
			}
		}
	}
	if p.cfg.Root.Playout.Chimes.Enabled {
		for _, minute := range []int{0, 30} {
			if minute == 0 && !p.cfg.Root.Playout.Chimes.TopOfHour.Enabled {
				continue
			}
			if minute == 30 && !p.cfg.Root.Playout.Chimes.HalfHour.Enabled {
				continue
			}
			if target, ok := minuteTarget(from, until, minute); ok {
				candidates = append(candidates, fixedEvent{Kind: "chime", PackageID: "chime", Title: "Chime", Target: target})
			}
		}
	}
	var best fixedEvent
	for _, candidate := range candidates {
		if last, ok := p.lastFixed[fixedEventKey(candidate)]; ok && last.Equal(candidate.Target) {
			continue
		}
		if !candidate.Target.After(now.Add(-2 * time.Second)) {
			continue
		}
		if best.Target.IsZero() || candidate.Target.Before(best.Target) {
			best = candidate
		}
	}
	if best.Target.IsZero() {
		return fixedEvent{}, false
	}
	_ = timelineEnd
	return best, true
}

func fixedEventKey(event fixedEvent) string {
	return event.Kind + ":" + event.Target.UTC().Format(time.RFC3339)
}

func minuteTarget(from time.Time, until time.Time, minute int) (time.Time, bool) {
	base := time.Date(from.Year(), from.Month(), from.Day(), from.Hour(), minute, 0, 0, from.Location())
	for base.Before(from) {
		base = base.Add(time.Hour)
	}
	if base.After(until) {
		return time.Time{}, false
	}
	return base, true
}

func (p *feedPlanner) buildFixed(ctx context.Context, event fixedEvent, timelineEnd time.Time, now time.Time) (playlistItem, error) {
	if event.Kind == "chime" {
		return p.buildChime(event, timelineEnd, now)
	}
	return p.buildProduct(ctx, event.PackageID, "fixed", event.Target.Format(time.RFC3339Nano), timelineEnd, now)
}

func (p *feedPlanner) buildProduct(ctx context.Context, pkgID string, source string, targetRaw string, timelineEnd time.Time, now time.Time) (playlistItem, error) {
	language := p.productLanguageForBuild(source)
	queueID := queueID(productQueuePrefix(pkgID, language))
	product, err := p.renderProductForBuild(ctx, queueID, pkgID, source, language, now)
	if err != nil {
		return playlistItem{}, err
	}
	if item, ok, err := p.buildProductAudioItem(ctx, product, pkgID, source, targetRaw, timelineEnd, now, queueID); ok || err != nil {
		return item, err
	}
	if !productContainsAudioSegment(product) {
		if cached, ok := p.startupCachedProductItem(pkgID, product, source, targetRaw, timelineEnd, now); ok {
			return cached, nil
		}
	}
	if item, ok, segmentErr := p.buildSegmentedProductItem(ctx, product, pkgID, source, targetRaw, timelineEnd, now, queueID); ok || segmentErr != nil {
		if segmentErr == nil {
			return item, nil
		}
		if cached, cachedOK := p.cachedProductItem(pkgID, product, source, targetRaw, timelineEnd, now, p.cacheFallbackMaxAge(source)); cachedOK {
			p.lastError = fmt.Sprintf("using cached %s audio after segmented synthesis failed: %v", pkgID, segmentErr)
			return cached, nil
		}
		return playlistItem{}, segmentErr
	}
	if strings.TrimSpace(product.Text) == "" {
		return playlistItem{}, fmt.Errorf("product %s rendered empty text", pkgID)
	}
	if cached, ok := p.startupCachedProductItem(pkgID, product, source, targetRaw, timelineEnd, now); ok {
		return cached, nil
	}
	outputPath := filepath.Join(p.cfg.OutputDir, safeID(p.feed.ID), queueID+".wav")
	synthCtx, cancel := context.WithTimeout(ctx, p.synthesisTimeoutForBuild(pkgID, source, product.Language))
	defer cancel()
	wavPath, err := p.bridge.Synthesize(synthCtx, synthJob{
		ID:               queueID,
		Text:             product.Text,
		ReaderID:         product.ReaderID,
		Language:         fallbackText(product.Language, feedLanguage(p.feed)),
		Timezone:         feedTimezone(p.feed),
		OutputPath:       outputPath,
		TargetSampleRate: p.cfg.Root.Playout.SampleRate,
		TargetChannels:   p.cfg.Root.Playout.Channels,
		Priority:         "normal",
	})
	if err != nil {
		if cached, ok := p.cachedProductItem(pkgID, product, source, targetRaw, timelineEnd, now, p.cacheFallbackMaxAge(source)); ok {
			p.lastError = fmt.Sprintf("using cached %s audio after synthesis failed: %v", pkgID, err)
			return cached, nil
		}
		return playlistItem{}, err
	}
	info, err := wavInfo(wavPath)
	if err != nil {
		if cached, ok := p.cachedProductItem(pkgID, product, source, targetRaw, timelineEnd, now, p.cacheFallbackMaxAge(source)); ok {
			p.lastError = fmt.Sprintf("using cached %s audio after synthesized WAV could not be read: %v", pkgID, err)
			return cached, nil
		}
		return playlistItem{}, err
	}
	target := parseTime(targetRaw)
	start := predictedStart(now, timelineEnd, target)
	finish := start.Add(p.itemScheduleDuration(info.DurationMS))
	return playlistItem{
		QueueID:           queueID,
		FeedID:            p.feed.ID,
		Kind:              "product",
		PackageID:         pkgID,
		Title:             fallbackText(product.Title, pkgID),
		AudioPath:         wavPath,
		DurationMS:        info.DurationMS,
		QueuedAt:          now.UTC().Format(time.RFC3339Nano),
		TargetStartAt:     formatOptionalTime(target),
		PredictedStartAt:  start.UTC().Format(time.RFC3339Nano),
		PredictedFinishAt: finish.UTC().Format(time.RFC3339Nano),
		Status:            "queued",
		Source:            source,
	}, nil
}

func (p *feedPlanner) synthesisTimeoutForBuild(pkgID string, source string, language string) time.Duration {
	if strings.EqualFold(source, "startup") {
		return 8 * time.Second
	}
	if p.cachedProductAudioEligible(pkgID) {
		if _, _, ok := p.latestCachedProductAudio(pkgID, language, cachedRoutineFallbackMaxAge); ok {
			return 12 * time.Second
		}
	}
	return 90 * time.Second
}

func (p *feedPlanner) startupCachedProductItem(pkgID string, product renderedProduct, source string, targetRaw string, timelineEnd time.Time, now time.Time) (playlistItem, bool) {
	if !strings.EqualFold(source, "startup") || strings.EqualFold(pkgID, "date_time") {
		return playlistItem{}, false
	}
	return p.cachedProductItem(pkgID, product, source, targetRaw, timelineEnd, now, cachedStartupFallbackMaxAge)
}

func (p *feedPlanner) cacheFallbackMaxAge(source string) time.Duration {
	if strings.EqualFold(source, "startup") {
		return cachedStartupFallbackMaxAge
	}
	return cachedRoutineFallbackMaxAge
}

func (p *feedPlanner) cachedProductItem(pkgID string, product renderedProduct, source string, targetRaw string, timelineEnd time.Time, now time.Time, maxAge time.Duration) (playlistItem, bool) {
	if !p.cachedProductAudioEligible(pkgID) {
		return playlistItem{}, false
	}
	path, info, ok := p.latestCachedProductAudio(pkgID, product.Language, maxAge)
	if !ok {
		return playlistItem{}, false
	}
	target := parseTime(targetRaw)
	start := predictedStart(now, timelineEnd, target)
	finish := start.Add(p.itemScheduleDuration(info.DurationMS))
	return playlistItem{
		QueueID:           queueID(productQueuePrefix(pkgID, product.Language)),
		FeedID:            p.feed.ID,
		Kind:              "product",
		PackageID:         pkgID,
		Title:             fallbackText(product.Title, pkgID),
		AudioPath:         path,
		DurationMS:        info.DurationMS,
		QueuedAt:          now.UTC().Format(time.RFC3339Nano),
		TargetStartAt:     formatOptionalTime(target),
		PredictedStartAt:  start.UTC().Format(time.RFC3339Nano),
		PredictedFinishAt: finish.UTC().Format(time.RFC3339Nano),
		Status:            "queued",
		Source:            fallbackText(source, "cached"),
	}, true
}

func (p *feedPlanner) cachedProductAudioEligible(pkgID string) bool {
	switch strings.ToLower(strings.TrimSpace(pkgID)) {
	case "alerts", "date_time":
		return false
	default:
		return true
	}
}

func (p *feedPlanner) latestCachedProductAudio(pkgID string, language string, maxAge time.Duration) (string, audioInfo, bool) {
	dir := filepath.Join(p.cfg.OutputDir, safeID(p.feed.ID))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", audioInfo{}, false
	}
	prefixes := []string{safeID(productQueuePrefix(pkgID, language)) + "-"}
	if normalizeLangKey(language) == normalizeLangKey(feedLanguage(p.feed)) {
		legacy := safeID(pkgID) + "-"
		if legacy != prefixes[0] {
			prefixes = append(prefixes, legacy)
		}
	}
	for _, prefix := range prefixes {
		bestPath := latestCachedProductAudioPath(dir, entries, prefix, maxAge)
		if bestPath == "" {
			continue
		}
		info, err := wavInfo(bestPath)
		if err != nil {
			continue
		}
		if info.SampleRate != p.cfg.Root.Playout.SampleRate || info.Channels != p.cfg.Root.Playout.Channels {
			continue
		}
		return bestPath, info, true
	}
	return "", audioInfo{}, false
}

func latestCachedProductAudioPath(dir string, entries []os.DirEntry, prefix string, maxAge time.Duration) string {
	var bestPath string
	var bestMod time.Time
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(strings.ToLower(name), ".wav") {
			continue
		}
		if isProductSegmentScratchName(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		mod := info.ModTime()
		if maxAge > 0 && time.Since(mod) > maxAge {
			continue
		}
		if bestPath == "" || mod.After(bestMod) {
			bestPath = filepath.Join(dir, name)
			bestMod = mod
		}
	}
	return bestPath
}

func isProductSegmentScratchName(name string) bool {
	base := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(name)), ".wav")
	for _, marker := range []string{"-text-", "-audio-"} {
		index := strings.LastIndex(base, marker)
		if index < 0 {
			continue
		}
		suffix := base[index+len(marker):]
		if len(suffix) < 2 {
			continue
		}
		digits := true
		for _, char := range suffix {
			if char < '0' || char > '9' {
				digits = false
				break
			}
		}
		if digits {
			return true
		}
	}
	return false
}

func (p *feedPlanner) renderProductForBuild(ctx context.Context, queueID string, pkgID string, source string, language string, now time.Time) (renderedProduct, error) {
	if source == "startup" && (pkgID == "station_id" || pkgID == "date_time") {
		return p.staticProduct(pkgID, language, now)
	}
	renderCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	product, err := p.bridge.RenderProduct(renderCtx, queueID, p.feed.ID, pkgID, language)
	if err != nil {
		fallback, fallbackErr := p.staticProduct(pkgID, language, now)
		if fallbackErr != nil {
			return renderedProduct{}, err
		}
		product = fallback
	}
	return product, nil
}

func (p *feedPlanner) queueStartupPrimer(ctx context.Context, now time.Time) {
	for _, item := range p.prepareStartupPrimer(ctx, now) {
		p.queue = append(p.queue, item)
		if err := p.publishReady(item); err != nil {
			p.lastError = err.Error()
			return
		}
	}
}

func (p *feedPlanner) prepareStartupPrimer(ctx context.Context, now time.Time) []playlistItem {
	timelineEnd := p.timelineEnd(now)
	items := make([]playlistItem, 0, 3)
	for index, pkgID := range []string{"station_id", "date_time", "current_conditions"} {
		if ctx.Err() != nil || p.mode != "running" || p.priorityActive > 0 {
			return items
		}
		targetRaw := ""
		if index == 0 && !p.startupPrimerAt.IsZero() {
			targetRaw = p.startupPrimerAt.UTC().Format(time.RFC3339Nano)
		}
		item, err := p.buildProduct(ctx, pkgID, "startup", targetRaw, timelineEnd, now)
		if err != nil {
			if pkgID != "current_conditions" {
				p.lastError = err.Error()
			}
			continue
		}
		items = append(items, item)
		if pkgID == "current_conditions" {
			p.advanceRoutineCursorPast(pkgID)
		}
		if finish := parseTime(item.PredictedFinishAt); !finish.IsZero() {
			timelineEnd = finish
		}
		now = time.Now()
	}
	return items
}

func (p *feedPlanner) buildProductAudioItem(ctx context.Context, product renderedProduct, pkgID string, source string, targetRaw string, timelineEnd time.Time, now time.Time, queueID string) (playlistItem, bool, error) {
	if !strings.EqualFold(metadataText(product.Metadata, "content_type"), "audio") {
		return playlistItem{}, false, nil
	}
	audioPath := metadataText(product.Metadata, "audio_path")
	audioURL := metadataText(product.Metadata, "audio_url")
	if audioPath == "" && audioURL == "" {
		return playlistItem{}, true, fmt.Errorf("product %s declared audio content without audio_path or audio_url", pkgID)
	}
	outputPath := filepath.Join(p.cfg.OutputDir, safeID(p.feed.ID), queueID+".wav")
	finalPath := ""
	if audioURL != "" {
		path, err := p.downloadRoutineAudio(ctx, audioURL, queueID, outputPath)
		if err != nil {
			return playlistItem{}, true, err
		}
		finalPath = path
	} else {
		path, err := p.prepareRoutineAudio(ctx, audioPath, outputPath)
		if err != nil {
			return playlistItem{}, true, err
		}
		finalPath = path
	}
	info, err := wavInfo(finalPath)
	if err != nil {
		return playlistItem{}, true, err
	}
	target := parseTime(targetRaw)
	start := predictedStart(now, timelineEnd, target)
	finish := start.Add(p.itemScheduleDuration(info.DurationMS))
	return playlistItem{
		QueueID:           queueID,
		FeedID:            p.feed.ID,
		Kind:              "audio",
		PackageID:         pkgID,
		Title:             fallbackText(product.Title, pkgID),
		AudioPath:         finalPath,
		DurationMS:        info.DurationMS,
		QueuedAt:          now.UTC().Format(time.RFC3339Nano),
		TargetStartAt:     formatOptionalTime(target),
		PredictedStartAt:  start.UTC().Format(time.RFC3339Nano),
		PredictedFinishAt: finish.UTC().Format(time.RFC3339Nano),
		Status:            "queued",
		Source:            source,
	}, true, nil
}

func (p *feedPlanner) buildSegmentedProductItem(ctx context.Context, product renderedProduct, pkgID string, source string, targetRaw string, timelineEnd time.Time, now time.Time, queueID string) (playlistItem, bool, error) {
	segments := playableProductSegments(product)
	if !productShouldUseSegments(segments) {
		return playlistItem{}, false, nil
	}
	outputPath := filepath.Join(p.cfg.OutputDir, safeID(p.feed.ID), queueID+".wav")
	outputDir := filepath.Dir(outputPath)
	sampleRate := p.cfg.Root.Playout.SampleRate
	if sampleRate <= 0 {
		sampleRate = 48000
	}
	channels := p.cfg.Root.Playout.Channels
	if channels <= 0 {
		channels = 1
	}
	stream, err := newPCM16WAVStream(outputPath, sampleRate, channels)
	if err != nil {
		return playlistItem{}, true, err
	}
	defer stream.Abort()
	hasAudio := false
	pausePCM := map[time.Duration][]byte{}
	for batchStart := 0; batchStart < len(segments); batchStart += productSegmentWorkerLimit {
		batchEnd := min(batchStart+productSegmentWorkerLimit, len(segments))
		preparedSegments, err := p.prepareProductSegments(ctx, segments[batchStart:batchEnd], batchStart, product, source, outputDir, queueID, sampleRate, channels)
		if err != nil {
			return playlistItem{}, true, err
		}
		for batchIndex, prepared := range preparedSegments {
			index := batchStart + batchIndex
			segment := segments[index]
			for _, pcm := range [][]byte{prepared.audioPCM, prepared.textPCM} {
				if len(pcm) == 0 {
					continue
				}
				if err := stream.Append(pcm); err != nil {
					return playlistItem{}, true, err
				}
				hasAudio = true
			}
			if index < len(segments)-1 {
				pause := productSegmentPause(segment)
				pcm := pausePCM[pause]
				if pcm == nil {
					pcm = silencePCM(sampleRate, channels, pause)
					pausePCM[pause] = pcm
				}
				if err := stream.Append(pcm); err != nil {
					return playlistItem{}, true, err
				}
			}
		}
	}
	if !hasAudio {
		return playlistItem{}, true, fmt.Errorf("product %s rendered no playable segments", pkgID)
	}
	if err := stream.Commit(); err != nil {
		return playlistItem{}, true, err
	}
	info, err := wavInfo(outputPath)
	if err != nil {
		return playlistItem{}, true, err
	}
	target := parseTime(targetRaw)
	start := predictedStart(now, timelineEnd, target)
	finish := start.Add(p.itemScheduleDuration(info.DurationMS))
	return playlistItem{
		QueueID:           queueID,
		FeedID:            p.feed.ID,
		Kind:              "product",
		PackageID:         pkgID,
		Title:             fallbackText(product.Title, pkgID),
		AudioPath:         outputPath,
		DurationMS:        info.DurationMS,
		QueuedAt:          now.UTC().Format(time.RFC3339Nano),
		TargetStartAt:     formatOptionalTime(target),
		PredictedStartAt:  start.UTC().Format(time.RFC3339Nano),
		PredictedFinishAt: finish.UTC().Format(time.RFC3339Nano),
		Status:            "queued",
		Source:            source,
	}, true, nil
}

type preparedProductSegment struct {
	audioPCM []byte
	textPCM  []byte
	err      error
}

func (p *feedPlanner) prepareProductSegments(ctx context.Context, segments []renderedSegment, indexOffset int, product renderedProduct, source string, outputDir string, queueID string, sampleRate int, channels int) ([]preparedProductSegment, error) {
	type segmentJob struct {
		index   int
		segment renderedSegment
	}
	type segmentResult struct {
		index    int
		prepared preparedProductSegment
	}
	workerCount := min(len(segments), productSegmentWorkerLimit)
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan segmentJob, workerCount)
	results := make(chan segmentResult, len(segments))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for job := range jobs {
				segmentIndex := indexOffset + job.index
				prepared := preparedProductSegment{}
				if err := workCtx.Err(); err != nil {
					prepared.err = err
					results <- segmentResult{index: job.index, prepared: prepared}
					continue
				}
				if audioPath := strings.TrimSpace(job.segment.AudioPath); audioPath != "" {
					scratch := filepath.Join(outputDir, fmt.Sprintf("%s-audio-%02d.wav", queueID, segmentIndex))
					prepared.audioPCM, prepared.err = p.routineAudioSegmentPCM(workCtx, audioPath, scratch, sampleRate, channels)
				}
				if prepared.err == nil {
					if text := strings.TrimSpace(job.segment.Text); text != "" {
						scratch := filepath.Join(outputDir, fmt.Sprintf("%s-text-%02d.wav", queueID, segmentIndex))
						prepared.textPCM, prepared.err = p.prepareTextSegmentPCM(workCtx, queueID, segmentIndex, text, product, source, scratch, sampleRate, channels)
					}
				}
				results <- segmentResult{index: job.index, prepared: prepared}
			}
		}()
	}
	for index, segment := range segments {
		jobs <- segmentJob{index: index, segment: segment}
	}
	close(jobs)
	prepared := make([]preparedProductSegment, len(segments))
	var preparationErr error
	for range segments {
		result := <-results
		prepared[result.index] = result.prepared
		if result.prepared.err != nil && preparationErr == nil {
			preparationErr = result.prepared.err
			cancel()
		}
	}
	workers.Wait()
	if preparationErr != nil {
		return nil, preparationErr
	}
	return prepared, nil
}

func (p *feedPlanner) prepareTextSegmentPCM(ctx context.Context, queueID string, index int, text string, product renderedProduct, source string, outputPath string, sampleRate int, channels int) ([]byte, error) {
	if p.segmentSynthesizer != nil {
		return p.segmentSynthesizer(ctx, index, text)
	}
	return p.synthesizeSegmentPCM(ctx, queueID, index, text, product, source, outputPath, sampleRate, channels)
}

func playableProductSegments(product renderedProduct) []renderedSegment {
	segments := make([]renderedSegment, 0, len(product.Segments))
	for _, segment := range product.Segments {
		if strings.TrimSpace(segment.AudioPath) == "" && strings.TrimSpace(segment.Text) == "" {
			continue
		}
		segments = append(segments, segment)
	}
	return segments
}

func productShouldUseSegments(segments []renderedSegment) bool {
	if len(segments) > 1 {
		return true
	}
	return len(segments) == 1 && strings.TrimSpace(segments[0].AudioPath) != ""
}

func productContainsAudioSegment(product renderedProduct) bool {
	for _, segment := range product.Segments {
		if strings.TrimSpace(segment.AudioPath) != "" {
			return true
		}
	}
	return false
}

func productSegmentPause(segment renderedSegment) time.Duration {
	switch strings.ToLower(strings.TrimSpace(segment.Kind)) {
	case "audio", "opener":
		return 400 * time.Millisecond
	case "warning":
		return 350 * time.Millisecond
	case "closure":
		return 300 * time.Millisecond
	default:
		return 240 * time.Millisecond
	}
}

func (p *feedPlanner) synthesizeSegmentPCM(ctx context.Context, queueID string, index int, text string, product renderedProduct, source string, outputPath string, sampleRate int, channels int) ([]byte, error) {
	synthCtx, cancel := context.WithTimeout(ctx, p.synthesisTimeoutForBuild(product.PackageID, source, product.Language))
	defer cancel()
	wavPath, err := p.bridge.Synthesize(synthCtx, synthJob{
		ID:               fmt.Sprintf("%s-seg-%02d", queueID, index),
		VoiceRouteKey:    queueID,
		Text:             text,
		ReaderID:         product.ReaderID,
		Language:         fallbackText(product.Language, feedLanguage(p.feed)),
		Timezone:         feedTimezone(p.feed),
		OutputPath:       outputPath,
		TargetSampleRate: sampleRate,
		TargetChannels:   channels,
		Priority:         "normal",
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = os.Remove(outputPath)
		_ = os.Remove(outputPath + ".normalized.wav")
		if filepath.Clean(wavPath) != filepath.Clean(outputPath) {
			_ = os.Remove(wavPath)
		}
	}()
	return playbackWAVPCM(ctx, wavPath, outputPath+".normalized.wav", sampleRate, channels)
}

func (p *feedPlanner) routineAudioSegmentPCM(ctx context.Context, rawPath string, scratchPath string, sampleRate int, channels int) ([]byte, error) {
	sourcePath, err := p.resolveRoutineAudioPath(rawPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if filepath.Clean(sourcePath) != filepath.Clean(scratchPath) {
			_ = os.Remove(scratchPath)
		}
	}()
	return playbackWAVPCM(ctx, sourcePath, scratchPath, sampleRate, channels)
}

func playbackWAVPCM(ctx context.Context, sourcePath string, normalizedPath string, sampleRate int, channels int) ([]byte, error) {
	raw, readErr := os.ReadFile(sourcePath)
	if readErr == nil {
		if pcm, info, wavErr := wavPCM16(raw); wavErr == nil && info.SampleRate == sampleRate && info.Channels == channels {
			return pcm, nil
		}
	}
	convertCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := convertAudioToWAV(convertCtx, sourcePath, normalizedPath, sampleRate, channels); err != nil {
		if readErr != nil {
			return nil, readErr
		}
		return nil, err
	}
	raw, err := os.ReadFile(normalizedPath)
	if err != nil {
		return nil, err
	}
	pcm, info, err := wavPCM16(raw)
	if err != nil {
		return nil, err
	}
	if info.SampleRate != sampleRate || info.Channels != channels {
		return nil, fmt.Errorf("normalized WAV format mismatch")
	}
	return pcm, nil
}

func (p *feedPlanner) downloadRoutineAudio(ctx context.Context, sourceURL string, queueID string, outputPath string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(sourceURL))
	if err != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("audio URL must be http or https")
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	inputPath := filepath.Join(filepath.Dir(outputPath), queueID+".download")
	if err := downloadFile(ctx, parsed.String(), inputPath, 20<<20); err != nil {
		return "", err
	}
	if _, err := wavInfo(inputPath); err == nil {
		return inputPath, nil
	}
	if err := convertAudioToWAV(ctx, inputPath, outputPath, p.cfg.Root.Playout.SampleRate, p.cfg.Root.Playout.Channels); err != nil {
		return "", err
	}
	return outputPath, nil
}

func (p *feedPlanner) prepareRoutineAudio(ctx context.Context, rawPath string, outputPath string) (string, error) {
	sourcePath, err := p.resolveRoutineAudioPath(rawPath)
	if err != nil {
		return "", err
	}
	if _, err := wavInfo(sourcePath); err == nil {
		return sourcePath, nil
	}
	convertCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := convertAudioToWAV(convertCtx, sourcePath, outputPath, p.cfg.Root.Playout.SampleRate, p.cfg.Root.Playout.Channels); err != nil {
		return "", err
	}
	return outputPath, nil
}

func (p *feedPlanner) resolveRoutineAudioPath(rawPath string) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", fmt.Errorf("audio path is required")
	}
	clean := filepath.Clean(rawPath)
	if filepath.IsAbs(clean) {
		return clean, nil
	}
	slash := filepath.ToSlash(clean)
	if slash == ".." || strings.HasPrefix(slash, "../") || strings.HasPrefix(slash, "/") {
		return "", fmt.Errorf("audio path must stay inside the bundle directory")
	}
	return filepath.Join(p.cfg.BaseDir, clean), nil
}

func metadataText(metadata map[string]string, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	for rawKey, value := range metadata {
		if strings.EqualFold(strings.TrimSpace(rawKey), key) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (p *feedPlanner) staticProduct(pkgID string, language string, now time.Time) (renderedProduct, error) {
	language = fallbackText(language, feedLanguage(p.feed))
	switch strings.ToLower(strings.TrimSpace(pkgID)) {
	case "station_id":
		onAirName := spokenText(p.cfg.Root.Operator.OnAirName)
		if onAirName == "" {
			onAirName = "Haze Weather Radio"
		}
		parts := []string{fmt.Sprintf("You are listening to %s.", onAirName)}
		site := feedName(p.feed)
		callsign := feedCallsign(p.feed)
		frequency := feedFrequencyMHz(p.feed)
		if callsignSpoken := spokenCallsign(callsign); callsignSpoken != "" {
			parts = append(parts, fmt.Sprintf("Callsign %s.", callsignSpoken))
		}
		if frequency != "" {
			parts = append(parts, fmt.Sprintf("Broadcasting from %s on a frequency of %s megahertz.", fallbackText(site, p.feed.ID), frequency))
		} else if site != "" {
			parts = append(parts, fmt.Sprintf("Serving %s.", site))
		}
		if replacement := replacementStationStatement(p.feed); replacement != "" {
			parts = append(parts, replacement)
		}
		return renderedProduct{
			ID:        fmt.Sprintf("%s-%s-%d", p.feed.ID, pkgID, now.UnixNano()),
			FeedID:    p.feed.ID,
			PackageID: pkgID,
			Title:     "Station Identification",
			Text:      strings.Join(parts, " "),
			ReaderID:  "00",
			Language:  language,
		}, nil
	case "date_time":
		local := now
		if loc := feedLocation(p.feed); loc != nil {
			local = now.In(loc)
		}
		return renderedProduct{
			ID:        fmt.Sprintf("%s-%s-%d", p.feed.ID, pkgID, now.UnixNano()),
			FeedID:    p.feed.ID,
			PackageID: pkgID,
			Title:     "Date and Time",
			Text:      dateTimeAnnouncement(local, language),
			ReaderID:  "00",
			Language:  language,
		}, nil
	default:
		return renderedProduct{}, fmt.Errorf("no static fallback for %s", pkgID)
	}
}

func replacementStationStatement(feed feedXML) string {
	transmitter, ok := replacementTransmitter(feed)
	if !ok {
		return ""
	}
	callsign := strings.TrimSpace(transmitter.Callsign)
	site := strings.TrimSpace(transmitter.SiteName)
	if callsign == "" && site == "" {
		return ""
	}
	network := strings.TrimSpace(spokenNetworkName(transmitter.Network))
	if network == "" {
		network = "Weatheradio Canada"
	}
	parts := []string{"This station replaces former", network, "station"}
	if callsign != "" {
		parts = append(parts, callsign)
	}
	if site != "" {
		parts = append(parts, "in", site)
	}
	return strings.Join(parts, " ") + "."
}

func (p *feedPlanner) buildChime(event fixedEvent, timelineEnd time.Time, now time.Time) (playlistItem, error) {
	source := filepath.Join(p.cfg.BaseDir, "audio", "8-step_chime.wav")
	info, err := wavInfo(source)
	if err != nil {
		return playlistItem{}, err
	}
	queueID := queueID("chime")
	start := predictedStart(now, timelineEnd, event.Target)
	finish := start.Add(p.itemScheduleDuration(info.DurationMS))
	return playlistItem{
		QueueID:           queueID,
		FeedID:            p.feed.ID,
		Kind:              "audio",
		PackageID:         "chime",
		Title:             "Chime",
		AudioPath:         source,
		DurationMS:        info.DurationMS,
		QueuedAt:          now.UTC().Format(time.RFC3339Nano),
		TargetStartAt:     event.Target.UTC().Format(time.RFC3339Nano),
		PredictedStartAt:  start.UTC().Format(time.RFC3339Nano),
		PredictedFinishAt: finish.UTC().Format(time.RFC3339Nano),
		Status:            "queued",
		Source:            "fixed",
	}, nil
}

func (p *feedPlanner) insert(ctx context.Context, data map[string]any) {
	p.prepareInsertPosition(data)
	prepared := p.prepareInsert(ctx, data)
	applyRoutinePlanningState(p, prepared.planner)
	if !prepared.ok {
		p.writeState()
		return
	}
	for _, item := range prepared.items {
		p.queue = append(p.queue, item)
		if err := p.publishReady(item); err != nil {
			p.lastError = err.Error()
			break
		}
	}
	p.writeState()
}

func (p *feedPlanner) prepareInsertPosition(data map[string]any) {
	position := strings.ToLower(firstText(nil, data, "position"))
	if position != "next" {
		return
	}
	p.queue = nil
	if p.bridge != nil {
		_ = p.bridge.Publish(map[string]any{
			"type":    "playlist.control",
			"source":  serviceID,
			"feed_id": p.feed.ID,
			"data":    map[string]any{"feed_id": p.feed.ID, "action": "flush_pending"},
		})
	}
}

func (p *feedPlanner) prepareInsert(ctx context.Context, data map[string]any) routinePreparation {
	kind := strings.ToLower(firstText(nil, data, "kind"))
	var item playlistItem
	var err error
	now := time.Now()
	if p.bridge == nil && (kind == "product" || kind == "tts") {
		p.lastError = "event bridge is unavailable"
		return routinePreparation{planner: p}
	}
	switch kind {
	case "product":
		item, err = p.buildProduct(ctx, firstText(nil, data, "package_id", "pkg_id"), "operator", "", p.timelineEnd(now), now)
	case "tts":
		item, err = p.buildTextItem(ctx, firstText(nil, data, "title"), firstText(nil, data, "text"), now)
	case "audio":
		item, err = p.buildAudioItem(firstText(nil, data, "title"), firstText(nil, data, "audio_path"), now)
	case "same":
		err = fmt.Errorf("SAME playlist insertion is staged for the SAME queue integration path")
	default:
		err = fmt.Errorf("unknown insert kind")
	}
	if err != nil {
		p.lastError = err.Error()
		return routinePreparation{planner: p}
	}
	return routinePreparation{planner: p, items: []playlistItem{item}, ok: true}
}

func (p *feedPlanner) queuePriorityAlert(ctx context.Context, data map[string]any) {
	data, alertID := normalizePriorityAlertRequest(data)
	if priorityAlertRequestStale(data, time.Now().UTC()) {
		cleanupSupersededAlertQueueParts(p.cfg.BaseDir, p.feed.ID, alertID, "")
		p.lastError = ""
		p.writeState()
		p.publishAutomationDispatchStatus(data, "failed", "priority alert request is stale")
		return
	}
	prepared, err := p.preparePriorityAlert(ctx, data)
	if err != nil {
		p.lastError = err.Error()
		p.writeState()
		p.publishAutomationDispatchStatus(data, "failed", p.lastError)
		return
	}
	if priorityAlertRequestStale(data, time.Now().UTC()) {
		cleanupSupersededAlertQueueParts(p.cfg.BaseDir, p.feed.ID, alertID, "")
		p.discardPriorityAlertPreparation(prepared)
		p.lastError = ""
		p.writeState()
		p.publishAutomationDispatchStatus(data, "failed", "priority alert request became stale during preparation")
		return
	}
	if err := p.commitPriorityAlert(prepared); err != nil {
		p.lastError = err.Error()
		p.writeState()
		p.publishAutomationDispatchStatus(data, "failed", p.lastError)
		return
	}
	p.lastError = ""
	p.writeState()
}

func normalizePriorityAlertRequest(data map[string]any) (map[string]any, string) {
	merged := alertmodel.MergePacketFields(data)
	normalized := make(map[string]any, len(merged)+1)
	for key, value := range merged {
		normalized[key] = value
	}
	alertID := firstText(nil, normalized, "alert_id", "id", "subject")
	if alertID == "" {
		alertID = fmt.Sprintf("cap-%d", time.Now().UnixNano())
	}
	normalized["alert_id"] = alertID
	return normalized, alertID
}

func (p *feedPlanner) preparePriorityAlert(ctx context.Context, data map[string]any) (prepared priorityAlertPreparation, err error) {
	data, alertID := normalizePriorityAlertRequest(data)
	includeSame := includeSameAlert(data)
	
	isUpdate := strings.EqualFold(firstText(nil, data, "status"), "updated")
	if isUpdate {
		policy := strings.ToLower(p.cfg.Daemon.AlertUpdateTonePolicy)
		switch policy {
		case "none":
			includeSame = false
		case "all":
			includeSame = includeSameAlert(data)
		case "new_locations":
			includeSame = includeSameAlert(data) && len(sameLocationsFromData(data)) > 0
		}
	}
	includeAttentionTone := !includeSame && alertAttentionToneEnabled(data)
	var sameRequest sameGenerateRequest
	var sameHeader sameAudioPayload
	var sameEOM sameAudioPayload
	var attentionTone sameAudioPayload
	alertSampleRate := p.cfg.Root.Playout.SampleRate
	alertChannels := p.cfg.Root.Playout.Channels
	if includeSame {
		var err error
		sameRequest, sameHeader, sameEOM, err = p.generatePrioritySAMEPair(ctx, data)
		if err != nil {
			return priorityAlertPreparation{}, err
		}
		if sameHeader.SampleRate != sameEOM.SampleRate || sameHeader.Channels != sameEOM.Channels {
			return priorityAlertPreparation{}, fmt.Errorf("SAME header and EOM formats do not match")
		}
		alertSampleRate = sameHeader.SampleRate
		alertChannels = sameHeader.Channels
	} else if includeAttentionTone {
		var err error
		var toneResult map[string]any
		_, toneResult, err = p.generatePrioritySAME(ctx, data, "tone")
		if err != nil {
			return priorityAlertPreparation{}, fmt.Errorf("attention tone generation failed: %w", err)
		}
		attentionTone, err = sameAudioFromResult(toneResult, p.cfg.Root.Playout.SampleRate, p.cfg.Root.Playout.Channels)
		if err != nil {
			return priorityAlertPreparation{}, fmt.Errorf("attention tone generation failed: %w", err)
		}
		alertSampleRate = attentionTone.SampleRate
		alertChannels = attentionTone.Channels
	}
	workID := queueID("alert-prepare")
	alertQueueID := safeID("001_" + p.feed.ID + "_" + alertID + "_cap")
	if includeSame {
		alertQueueID = safeID("000_" + p.feed.ID + "_" + alertID + "_same")
	}
	audioRel := filepath.ToSlash(filepath.Join("runtime", "audio", "alerts", alertQueueID+".pcm16le"))
	finalAudioPath := filepath.Join(p.cfg.BaseDir, filepath.FromSlash(audioRel))
	// Keep partial and cancelled work invisible until the serialized commit.
	audioPath := finalAudioPath + "." + workID + ".tmp"
	defer func() {
		if err != nil {
			_ = os.Remove(audioPath)
			_ = os.Remove(audioPath + ".tmp")
			_ = os.Remove(audioPath + ".voice")
		}
	}()
	leadIn, leadOut, leadName := p.configuredAlertLeadAudio(ctx, data, workID, alertSampleRate, alertChannels)
	hasConfiguredLead := len(leadIn) > 0 || len(leadOut) > 0
	voicePath := audioPath
	if includeSame || includeAttentionTone || hasConfiguredLead {
		voicePath = audioPath + ".voice"
		defer func() { _ = os.Remove(voicePath) }()
	}
	synthesisID := workID
	title := fallbackText(firstText(nil, data, "title", "header"), "Weather Alert")
	eventName := fallbackText(firstText(nil, data, "event"), "CAP")
	alertText := p.alertTextFromData(data)
	bannerText := p.bannerTextFromData(data, alertText)
	source := "cap-tts"
	authoritativeURL := strings.TrimSpace(firstText(nil, data, "audio_url", "authoritative_url"))
	var lastErr error
	if localAudioPath := strings.TrimSpace(firstText(nil, data, "audio_path")); localAudioPath != "" {
		if err := p.prepareLocalAlertAudio(ctx, localAudioPath, voicePath, alertSampleRate, alertChannels, data); err == nil {
			source = "cap-broadcast-audio"
		} else {
			lastErr = err
		}
	}
	if source != "cap-broadcast-audio" && authoritativeURL != "" {
		if err := p.downloadAndConvertAlertAudio(ctx, authoritativeURL, voicePath, alertSampleRate, alertChannels); err == nil {
			source = "cap-broadcast-audio"
		} else {
			lastErr = err
		}
	}
	if source != "cap-broadcast-audio" {
		if err := p.renderAlertTTSAsPCM(ctx, synthesisID, voicePath, alertText, alertSampleRate, alertChannels, data); err != nil {
			if lastErr != nil {
				return priorityAlertPreparation{}, fmt.Errorf("broadcast audio failed: %v; TTS fallback failed: %w", lastErr, err)
			}
			return priorityAlertPreparation{}, fmt.Errorf("alert TTS fallback failed: %w", err)
		}
	}
	if includeSame {
		if err := combineConfiguredLeadAlertAudio(audioPath, leadIn, sameHeader.Audio, voicePath, sameEOM.Audio, leadOut, alertSampleRate, alertChannels); err != nil {
			return priorityAlertPreparation{}, fmt.Errorf("SAME alert assembly failed: %w", err)
		}
		if source == "cap-broadcast-audio" {
			source = "cap-same-broadcast-audio"
		} else {
			source = "cap-same-tts"
		}
	} else if includeAttentionTone {
		if err := combineConfiguredLeadAlertAudio(audioPath, leadIn, attentionTone.Audio, voicePath, nil, leadOut, alertSampleRate, alertChannels); err != nil {
			return priorityAlertPreparation{}, fmt.Errorf("attention tone alert assembly failed: %w", err)
		}
		if source == "cap-broadcast-audio" {
			source = "cap-tone-broadcast-audio"
		} else {
			source = "cap-tone-tts"
		}
	} else if hasConfiguredLead {
		if err := combineConfiguredLeadAlertAudio(audioPath, leadIn, nil, voicePath, nil, leadOut, alertSampleRate, alertChannels); err != nil {
			return priorityAlertPreparation{}, fmt.Errorf("alert lead assembly failed: %w", err)
		}
		if source == "cap-broadcast-audio" {
			source = "cap-lead-broadcast-audio"
		} else {
			source = "cap-lead-tts"
		}
	}
	if hasConfiguredLead && leadName != "" {
		log.Printf("priority alert %s applied lead statement %q", alertID, leadName)
	}
	info, err := pcmInfo(audioPath, alertSampleRate, alertChannels)
	if err != nil {
		return priorityAlertPreparation{}, err
	}
	manifest := priorityAlertManifest{
		ID:                 alertQueueID,
		AlertID:            alertID,
		Type:               "cap_alert",
		Status:             "pending",
		CreatedAt:          time.Now().UTC().Format(time.RFC3339Nano),
		FeedIDs:            []string{p.feed.ID},
		Header:             title,
		Event:              eventName,
		AlertText:          strings.TrimSpace(alertText),
		BannerText:         strings.TrimSpace(bannerText),
		AlertSentAt:        firstText(nil, data, "alert_sent_at", "sent"),
		AlertExpiresAt:     firstText(nil, data, "alert_expires_at", "expires"),
		MessageType:        firstText(nil, data, "message_type", "msg_type"),
		BroadcastImmediate: boolAny(firstValue(nil, data, "broadcast_immediate")),
		AudioPath:          audioRel,
		Format:             "pcm_s16le",
		SampleRate:         info.SampleRate,
		Channels:           info.Channels,
		AudioBytes:         int(info.Bytes),
		Source:             source,
		Priority:           "cap",
		AuthoritativeURL:   authoritativeURL,
	}
	if includeSame {
		manifest.Type = "same_alert"
		manifest.Event = sameRequest.Event
		manifest.Priority = "same"
	}
	if lastErr != nil && !broadcastAudioSourceSucceeded(source) {
		manifest.LastError = "broadcast audio fallback: " + lastErr.Error()
	}
	packet := p.alertPacketForManifest(data, manifest, alertText, sameHeader.Header)
	manifest.AlertPacket = &packet
	return priorityAlertPreparation{
		Manifest:       manifest,
		Data:           data,
		AlertText:      alertText,
		SAMEHeader:     sameHeader.Header,
		AudioPath:      audioPath,
		FinalAudioPath: finalAudioPath,
	}, nil
}

func (p *feedPlanner) publishAutomationDispatchStatus(data map[string]any, status string, errorText string) {
	dispatchKey := firstText(nil, data, "dispatch_key")
	if p.bridge == nil || dispatchKey == "" {
		return
	}
	status = strings.ToLower(strings.TrimSpace(status))
	switch status {
	case "accepted", "completed", "failed":
	default:
		return
	}
	statusData := map[string]any{
		"dispatch_key":  dispatchKey,
		"automation_id": firstText(nil, data, "automation_id"),
		"feed_id":       p.feed.ID,
		"scheduled_for": firstText(nil, data, "scheduled_for"),
		"alert_id":      firstText(nil, data, "alert_id"),
	}
	if strings.TrimSpace(errorText) != "" {
		statusData["error"] = strings.TrimSpace(errorText)
	}
	if err := p.bridge.Publish(map[string]any{
		"type":    "automation.dispatch." + status,
		"source":  serviceID,
		"subject": dispatchKey,
		"data":    statusData,
	}); err != nil {
		log.Printf(
			"playlist automation %s acknowledgement failed for %s: %v",
			status,
			dispatchKey,
			err,
		)
	}
}

func (p *feedPlanner) commitPriorityAlert(prepared priorityAlertPreparation) error {
	manifest := prepared.Manifest
	cleanupSupersededAlertQueueParts(p.cfg.BaseDir, p.feed.ID, manifest.AlertID, manifest.ID)
	if err := installPreparedAlertAudio(prepared.AudioPath, prepared.FinalAudioPath); err != nil {
		return err
	}
	if err := writePriorityAlertManifest(filepath.Join(p.cfg.BaseDir, "runtime", "queues", "alerts", manifest.ID+".json"), manifest); err != nil {
		_ = os.Remove(prepared.FinalAudioPath)
		return err
	}
	p.publishAlertAudioReady(manifest, prepared.Data, prepared.AlertText, prepared.SAMEHeader)
	p.publishAutomationDispatchStatus(prepared.Data, "completed", "")
	return nil
}

func (p *feedPlanner) discardPriorityAlertPreparation(prepared priorityAlertPreparation) {
	if strings.TrimSpace(prepared.AudioPath) == "" {
		return
	}
	for _, path := range []string{prepared.AudioPath, prepared.AudioPath + ".tmp", prepared.AudioPath + ".voice"} {
		_ = os.Remove(path)
	}
}

func installPreparedAlertAudio(sourcePath string, targetPath string) error {
	if strings.TrimSpace(sourcePath) == "" || strings.TrimSpace(targetPath) == "" {
		return fmt.Errorf("prepared alert audio path is missing")
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return err
	}
	return replaceFileAtomically(sourcePath, targetPath)
}

func (p *feedPlanner) publishAlertAudioReady(manifest priorityAlertManifest, data map[string]any, alertText string, sameHeader string) {
	if p.bridge == nil {
		return
	}
	packet := p.alertPacketForManifest(data, manifest, alertText, sameHeader)
	base := map[string]any{
		"feed_id":              p.feed.ID,
		"alert_id":             manifest.AlertID,
		"queue_id":             manifest.ID,
		"manifest_id":          manifest.ID,
		"audio_path":           manifest.AudioPath,
		"audio_format":         manifest.Format,
		"sample_rate":          manifest.SampleRate,
		"channels":             manifest.Channels,
		"audio_bytes":          manifest.AudioBytes,
		"source":               manifest.Source,
		"priority":             manifest.Priority,
		"message_type":         manifest.MessageType,
		"alert_sent_at":        manifest.AlertSentAt,
		"alert_expires_at":     manifest.AlertExpiresAt,
		"broadcast_immediate":  manifest.BroadcastImmediate,
		"authoritative_url":    manifest.AuthoritativeURL,
		"title":                fallbackText(firstText(nil, data, "headline", "title", "header"), manifest.Header),
		"header":               manifest.Header,
		"event":                fallbackText(firstText(nil, data, "same_event", "event"), manifest.Event),
		"alert_text":           fallbackText(alertText, firstText(nil, data, "alert_text", "tts_text", "text", "message")),
		"banner_text":          fallbackText(manifest.BannerText, firstText(nil, data, "banner_text")),
		"description":          firstText(nil, data, "description"),
		"instruction":          firstText(nil, data, "instruction"),
		"severity":             firstText(nil, data, "severity"),
		"urgency":              firstText(nil, data, "urgency"),
		"certainty":            firstText(nil, data, "certainty"),
		"same_event":           firstText(nil, data, "same_event"),
		"same_event_name":      firstText(nil, data, "same_event_name"),
		"same_originator":      firstText(nil, data, "same_originator"),
		"same_originator_name": firstText(nil, data, "same_originator_name"),
		"same_weather_service": firstText(nil, data, "same_weather_service"),
		"same_duration":        firstText(nil, data, "same_duration"),
		"same_sent_at":         firstText(nil, data, "same_sent_at"),
		"same_begins_at":       firstText(nil, data, "same_begins_at"),
		"same_expires_at":      firstText(nil, data, "same_expires_at"),
		"same_tone":            firstText(nil, data, "same_tone"),
		"same_header":          sameHeader,
		"background_color":     firstText(nil, data, "background_color"),
	}
	payload := alertmodel.WithLegacyFields(packet, base)
	if locations := stringListAny(firstValue(nil, data, "same_locations", "locations")); len(locations) > 0 {
		payload["same_locations"] = locations
	}
	if err := p.bridge.Publish(map[string]any{
		"type":    "cap.alert.audio.ready",
		"source":  serviceID,
		"feed_id": p.feed.ID,
		"subject": manifest.AlertID,
		"data":    payload,
	}); err != nil {
		log.Printf("[%s] alert webhook event publish failed: %v", p.feed.ID, err)
	}
}

func broadcastAudioSourceSucceeded(source string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(source)), "broadcast-audio")
}

func (p *feedPlanner) cancelPriorityAlerts(data map[string]any) {
	ids := stringListAny(firstValue(nil, data, "alert_ids"))
	if id := firstText(nil, data, "alert_id", "id", "subject"); id != "" {
		ids = append(ids, id)
	}
	ids = uniqueStrings(ids)
	for _, alertID := range ids {
		cleanupSupersededAlertQueueParts(p.cfg.BaseDir, p.feed.ID, alertID, "")
	}
	p.writeState()
}

func (p *feedPlanner) invalidateRoutineAlerts(stopCurrent bool) {
	removed := false
	filtered := p.queue[:0]
	for _, item := range p.queue {
		if strings.EqualFold(strings.TrimSpace(item.PackageID), "alerts") {
			removed = true
			continue
		}
		filtered = append(filtered, item)
	}
	p.queue = filtered
	if stopCurrent && p.current != nil && strings.EqualFold(strings.TrimSpace(p.current.PackageID), "alerts") {
		p.current = nil
		removed = true
		p.publishPlayoutControl("flush_restart")
	}
	if removed {
		p.writeState()
	}
}

func priorityAlertRequestStale(data map[string]any, now time.Time) bool {
	if strings.EqualFold(firstText(nil, data, "message_type", "msg_type"), "Cancel") {
		return true
	}
	header := strings.ToLower(firstText(nil, data, "title", "header"))
	if strings.Contains(header, "ended") || strings.Contains(header, "cancelled") || strings.Contains(header, "canceled") {
		return true
	}
	if boolAny(firstValue(nil, data, "force_broadcast", "force", "rebroadcast")) {
		return false
	}
	if expires := parseTime(firstText(nil, data, "alert_expires_at", "expires")); !expires.IsZero() && !now.Before(expires) {
		return true
	}
	sent := parseTime(firstText(nil, data, "alert_sent_at", "sent"))
	if sent.IsZero() || now.Before(sent) {
		return false
	}
	event := strings.ToUpper(firstText(nil, data, "same_event", "event"))
	limit := time.Hour
	if event == "SVR" || event == "TOR" || strings.Contains(header, "severe thunderstorm warning") || strings.Contains(header, "tornado warning") {
		limit = 30 * time.Minute
	}
	return now.Sub(sent) >= limit
}

func cleanupSupersededAlertQueueParts(baseDir string, feedID string, alertID string, keepID string) {
	ids := []string{
		safeID("000_" + feedID + "_" + alertID + "_same_header"),
		safeID("000_" + feedID + "_" + alertID + "_same"),
		safeID("001_" + feedID + "_" + alertID + "_cap"),
		safeID("002_" + feedID + "_" + alertID + "_same_eom"),
		safeID("cap_alert_" + feedID + "_" + alertID),
	}
	for _, id := range ids {
		if id == "" || id == keepID {
			continue
		}
		manifestPath := filepath.Join(baseDir, "runtime", "queues", "alerts", id+".json")
		if preserveInFlightAlertManifest(manifestPath) {
			continue
		}
		_ = os.Remove(manifestPath)
		_ = os.Remove(filepath.Join(baseDir, "runtime", "audio", "alerts", id+".pcm16le"))
	}
}

func preserveInFlightAlertManifest(path string) bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var manifest struct {
		Status    string `json:"status"`
		ClaimedAt string `json:"claimed_at"`
		PlayedAt  string `json:"played_at"`
		FailedAt  string `json:"failed_at"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return false
	}
	if strings.TrimSpace(manifest.PlayedAt) != "" || strings.TrimSpace(manifest.FailedAt) != "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(manifest.Status)) {
	case "queued", "claimed", "event_queued", "playing":
		return true
	}
	return strings.TrimSpace(manifest.ClaimedAt) != ""
}

func (p *feedPlanner) downloadAndConvertAlertAudio(ctx context.Context, sourceURL string, outputPath string, sampleRate int, channels int) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	tempDir := filepath.Join(p.cfg.BaseDir, "runtime", "audio", "alerts")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return err
	}
	inputPath := filepath.Join(tempDir, safeID(fmt.Sprintf("download-%d", time.Now().UnixNano()))+".bin")
	defer os.Remove(inputPath)
	if err := downloadFile(ctx, sourceURL, inputPath, 10<<20); err != nil {
		return err
	}
	return convertAlertAudioToPCM(ctx, inputPath, outputPath, sampleRate, channels)
}

func (p *feedPlanner) prepareLocalAlertAudio(ctx context.Context, rawPath string, outputPath string, sampleRate int, channels int, data map[string]any) error {
	sourcePath, err := p.resolveAlertAudioPath(rawPath)
	if err != nil {
		return err
	}
	audioFormat := strings.ToLower(strings.TrimSpace(firstText(nil, data, "audio_format", "format")))
	sourceRate := intFromAny(firstValue(nil, data, "audio_sample_rate", "sample_rate"), 48000)
	sourceChannels := intFromAny(firstValue(nil, data, "audio_channels", "channels"), 1)
	if audioFormat == "pcm_s16le" || audioFormat == "raw" || audioFormat == "s16le" || strings.HasSuffix(strings.ToLower(sourcePath), ".pcm16le") || strings.HasSuffix(strings.ToLower(sourcePath), ".s16le") {
		raw, err := os.ReadFile(sourcePath)
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			return fmt.Errorf("alert audio is empty")
		}
		if sourceRate == sampleRate && sourceChannels == channels {
			return writeAlertAudioFile(outputPath, raw)
		}
		convertCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		return convertRawPCM16FileToPCM(convertCtx, sourcePath, outputPath, sourceRate, sourceChannels, sampleRate, channels)
	}
	raw, err := os.ReadFile(sourcePath)
	if err == nil {
		if pcm, info, wavErr := wavPCM16(raw); wavErr == nil {
			if info.SampleRate == sampleRate && info.Channels == channels {
				return writeAlertAudioFile(outputPath, pcm)
			}
		}
	}
	convertCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	return convertAlertAudioToPCM(convertCtx, sourcePath, outputPath, sampleRate, channels)
}

func (p *feedPlanner) configuredAlertLeadAudio(ctx context.Context, data map[string]any, workID string, sampleRate int, channels int) ([]byte, []byte, string) {
	configured := mapAt(data, "alert_lead")
	if len(configured) == 0 {
		return nil, nil, ""
	}
	name := strings.TrimSpace(stringAt(configured, "name"))
	load := func(slot string) []byte {
		rawPath := strings.TrimSpace(stringAt(configured, slot))
		if rawPath == "" {
			return nil
		}
		pcm, err := p.loadConfiguredAlertLeadAudio(ctx, rawPath, workID+"-"+slot, sampleRate, channels)
		if err != nil {
			log.Printf("priority alert lead %q %s skipped: %v", fallbackText(name, "unnamed"), slot, err)
			return nil
		}
		return pcm
	}
	return load("lead_in"), load("lead_out"), name
}

func (p *feedPlanner) loadConfiguredAlertLeadAudio(ctx context.Context, rawPath string, workID string, sampleRate int, channels int) ([]byte, error) {
	normalized, err := lead.NormalizeAudioPath(rawPath)
	if err != nil {
		return nil, err
	}
	if normalized == "" {
		return nil, nil
	}
	sourcePath, err := lead.ResolveAudioPath(p.cfg.BaseDir, normalized)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(sourcePath)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("configured lead audio is not a regular file")
	}
	if info.Size() <= 0 || info.Size() > configuredAlertLeadMaxBytes {
		return nil, fmt.Errorf("configured lead audio must be between 1 byte and %d MiB", configuredAlertLeadMaxBytes>>20)
	}
	tempDir := filepath.Join(p.cfg.BaseDir, "runtime", "audio", "alerts")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return nil, err
	}
	temp, err := os.CreateTemp(tempDir, safeID("lead-"+workID)+"-*.pcm16le")
	if err != nil {
		return nil, err
	}
	tempPath := temp.Name()
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return nil, err
	}
	if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	defer func() { _ = os.Remove(tempPath) }()
	if err := p.prepareLocalAlertAudio(ctx, normalized, tempPath, sampleRate, channels, nil); err != nil {
		return nil, err
	}
	pcm, err := os.ReadFile(tempPath)
	if err != nil {
		return nil, err
	}
	if len(pcm) == 0 || len(pcm) > int(configuredAlertLeadMaxBytes*2) {
		return nil, fmt.Errorf("converted lead audio is outside the allowed size")
	}
	return pcm, nil
}

func (p *feedPlanner) resolveAlertAudioPath(rawPath string) (string, error) {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", fmt.Errorf("audio path is required")
	}
	clean := filepath.Clean(rawPath)
	if filepath.IsAbs(clean) {
		return "", fmt.Errorf("alert audio path must be relative to the bundle directory")
	}
	slash := filepath.ToSlash(clean)
	if slash == ".." || strings.HasPrefix(slash, "../") || strings.HasPrefix(slash, "/") {
		return "", fmt.Errorf("alert audio path must stay inside the bundle directory")
	}
	target := filepath.Join(p.cfg.BaseDir, clean)
	root, err := filepath.Abs(p.cfg.BaseDir)
	if err != nil {
		return "", err
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, absTarget)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("alert audio path must stay inside the bundle directory")
	}
	return absTarget, nil
}

func alertTextFromData(data map[string]any) string {
	return alerttext.SpeechFromData(data)
}

func (p *feedPlanner) alertTextFromData(data map[string]any) string {
	if packet, ok := packetFromData(data); ok {
		compatData := alertmodel.WithLegacyFields(packet, data)
		if text := p.alertTextFromPacket(packet, compatData); text != "" {
			return text
		}
	}
	capSource := firstText(nil, data, "cap_source")
	if strings.EqualFold(capSource, "nws") {
		if intro := p.sameSpeechIntroFromData(data); intro != "" {
			parts := []string{intro}
			if description := strings.TrimSpace(firstText(nil, data, "description")); description != "" {
				parts = append(parts, cleanAlertBodyText(capSource, description))
			}
			if instruction := strings.TrimSpace(firstText(nil, data, "instruction")); instruction != "" {
				parts = append(parts, cleanAlertBodyText(capSource, instruction))
			}
			return strings.TrimSpace(strings.Join(nonEmptyStrings(parts), " "))
		}
	}
	return alertTextFromData(data)
}

func (p *feedPlanner) bannerTextFromData(data map[string]any, alertText string) string {
	if packet, ok := packetFromData(data); ok {
		compatData := alertmodel.WithLegacyFields(packet, data)
		if text := p.bannerTextFromPacket(packet, compatData, alertText); text != "" {
			return text
		}
	}
	if text := strings.TrimSpace(firstText(nil, data, "banner_text")); text != "" {
		return text
	}
	parts := []string{}
	if intro := p.sameIntroFromData(data); intro != "" {
		parts = append(parts, intro)
	}
	capSource := firstText(nil, data, "cap_source")
	if description := strings.TrimSpace(firstText(nil, data, "description")); description != "" {
		parts = append(parts, cleanAlertBodyText(capSource, description))
	}
	if instruction := strings.TrimSpace(firstText(nil, data, "instruction")); instruction != "" {
		parts = append(parts, cleanAlertBodyText(capSource, instruction))
	}
	if len(parts) <= 1 {
		if custom := customAlertTextFromData(data); custom != "" && !sameText(parts, custom) {
			parts = append(parts, custom)
		}
	}
	if len(parts) == 0 {
		return strings.TrimSpace(alertText)
	}
	return strings.TrimSpace(strings.Join(nonEmptyStrings(parts), " "))
}

func packetFromData(data map[string]any) (alertmodel.Packet, bool) {
	if _, ok := data["alert_packet"]; !ok {
		return alertmodel.Packet{}, false
	}
	return alertmodel.FromMap(data)
}

func (p *feedPlanner) alertTextFromPacket(packet alertmodel.Packet, data map[string]any) string {
	capSource := fallbackText(packet.Source, firstText(nil, data, "cap_source"))
	if strings.EqualFold(capSource, "nws") {
		parts := []string{}
		if intro := p.sameSpeechIntroFromData(data); intro != "" {
			parts = append(parts, intro)
		}
		parts = append(parts, cleanPacketBodyParts(capSource, packet)...)
		return strings.TrimSpace(strings.Join(nonEmptyStrings(parts), " "))
	}
	if text := strings.TrimSpace(packet.Presentation.SpeechText); text != "" {
		return text
	}
	parts := []string{}
	if packet.Options.PrependSAMETranslation {
		if intro := p.sameSpeechIntroFromData(data); intro != "" {
			parts = append(parts, intro)
		}
	}
	parts = append(parts, cleanPacketBodyParts(capSource, packet)...)
	return strings.TrimSpace(strings.Join(nonEmptyStrings(parts), " "))
}

func (p *feedPlanner) bannerTextFromPacket(packet alertmodel.Packet, data map[string]any, alertText string) string {
	if text := strings.TrimSpace(packet.Presentation.BannerText); text != "" {
		return text
	}
	capSource := fallbackText(packet.Source, firstText(nil, data, "cap_source"))
	parts := []string{}
	if intro := p.sameIntroFromData(data); intro != "" {
		parts = append(parts, intro)
	}
	parts = append(parts, cleanPacketBodyParts(capSource, packet)...)
	if len(parts) <= 1 {
		if custom := customAlertTextFromData(data); custom != "" && !sameText(parts, custom) {
			parts = append(parts, custom)
		}
	}
	if len(parts) == 0 {
		return strings.TrimSpace(alertText)
	}
	return strings.TrimSpace(strings.Join(nonEmptyStrings(parts), " "))
}

func cleanPacketBodyParts(capSource string, packet alertmodel.Packet) []string {
	if text := strings.TrimSpace(packet.Content.CustomText); text != "" {
		return []string{cleanAlertBodyText(capSource, text)}
	}
	parts := []string{}
	if description := strings.TrimSpace(packet.Content.Description); description != "" {
		parts = append(parts, cleanAlertBodyText(capSource, description))
	}
	if instruction := strings.TrimSpace(packet.Content.Instruction); instruction != "" {
		parts = append(parts, cleanAlertBodyText(capSource, instruction))
	}
	return parts
}

func cleanAlertBodyText(capSource string, raw string) string {
	if strings.EqualFold(capSource, "nws") {
		return alerttext.CleanNWSAlertText(raw)
	}
	return alerttext.CleanAlertText(raw)
}

func (p *feedPlanner) alertPacketForManifest(data map[string]any, manifest priorityAlertManifest, alertText string, sameHeader string) alertmodel.Packet {
	packet, ok := alertmodel.FromMap(data)
	if !ok {
		packet = alertmodel.LegacyFromMap(data)
	}
	packet.ID = fallbackText(packet.ID, manifest.AlertID)
	packet.FeedID = p.feed.ID
	packet.FeedIDs = []string{p.feed.ID}
	packet.MessageType = fallbackText(packet.MessageType, manifest.MessageType)
	packet.Content.Title = fallbackText(packet.Content.Title, manifest.Header)
	packet.Content.Event = fallbackText(packet.Content.Event, manifest.Event)
	packet.Timing.SentAt = fallbackText(packet.Timing.SentAt, manifest.AlertSentAt)
	packet.Timing.ExpiresAt = fallbackText(packet.Timing.ExpiresAt, manifest.AlertExpiresAt)
	packet.Options.BroadcastImmediate = packet.Options.BroadcastImmediate || manifest.BroadcastImmediate
	packet.Presentation.SpeechText = strings.TrimSpace(alertText)
	packet.Presentation.BannerText = strings.TrimSpace(manifest.BannerText)
	if packet.SAME == nil && manifest.Priority == "same" {
		packet.SAME = &alertmodel.SAME{Include: true}
	}
	if packet.SAME != nil {
		packet.SAME.Header = fallbackText(packet.SAME.Header, sameHeader)
		packet.SAME.Event = fallbackText(packet.SAME.Event, manifest.Event)
	}
	if packet.Audio == nil {
		packet.Audio = &alertmodel.Audio{}
	}
	packet.Audio.Path = manifest.AudioPath
	packet.Audio.Format = manifest.Format
	packet.Audio.SampleRate = manifest.SampleRate
	packet.Audio.Channels = manifest.Channels
	packet.Audio.Bytes = manifest.AudioBytes
	packet.Audio.Source = manifest.Source
	packet.Audio.URL = fallbackText(packet.Audio.URL, manifest.AuthoritativeURL)
	packet.Audio.Authoritative = packet.Audio.Authoritative || manifest.AuthoritativeURL != ""
	return packet.Normalize()
}

func (p *feedPlanner) sameIntroFromData(data map[string]any) string {
	return p.sameIntroFromDataWithOptions(data, true)
}

func (p *feedPlanner) sameSpeechIntroFromData(data map[string]any) string {
	return p.sameIntroFromDataWithOptions(data, false)
}

func (p *feedPlanner) sameIntroFromDataWithOptions(data map[string]any, includeSourceLabel bool) string {
	if intro := strings.TrimSpace(firstText(nil, data, "same_translation", "same_intro")); intro != "" {
		if !sameIntroNeedsRebuild(data, intro) {
			if !includeSourceLabel {
				return stripTrailingSourceLabel(intro)
			}
			return intro
		}
	}
	event := strings.TrimSpace(firstText(nil, data, "same_event", "event"))
	locations := stringListAny(firstValue(nil, data, "same_locations", "locations"))
	if event == "" || len(locations) == 0 {
		return ""
	}
	configPath := filepath.Join(p.cfg.BaseDir, "config.yaml")
	duration := normalizeSAMEDuration(fallbackText(firstText(nil, data, "same_duration", "duration"), "0015"))
	beginsAt := dataTimeValue(data, "same_begins_at", "alert_begins_at", "onset", "effective", "same_sent_at", "alert_sent_at", "sent")
	sentAt := dataTimeValue(data, "same_sent_at", "alert_sent_at", "sent")
	expiresAt := dataTimeValue(data, "same_expires_at", "alert_expires_at", "expires")
	if expiresAt.IsZero() && !beginsAt.IsZero() {
		expiresAt = beginsAt.Add(sameDurationToDuration(duration))
	}
	if beginsAt.IsZero() && !sentAt.IsZero() {
		beginsAt = sentAt
	}
	return alerttext.BuildSAMETranslation(alerttext.SAMERequest{
		Originator:     fallbackText(firstText(nil, data, "same_originator", "originator"), "WXR"),
		OriginatorName: strings.TrimSpace(firstText(nil, data, "same_originator_name", "originator_name", "sender_name")),
		Event:          event,
		EventName:      fallbackText(firstText(nil, data, "same_event_name", "event_name"), alerttext.EventName(configPath, event)),
		Locations:      locations,
		AreaNames:      alerttext.ResolveAreaNames(configPath, stringListAny(firstValue(nil, data, "area_names")), locations),
		Callsign:       sameIntroCallsign(data, p.feed, includeSourceLabel),
		WeatherService: strings.TrimSpace(firstText(nil, data, "same_weather_service", "weather_service")),
		SentAt:         sentAt,
		BeginsAt:       beginsAt,
		ExpiresAt:      expiresAt,
		MimicENDEC:     fallbackText(firstText(nil, data, "mimic_endec"), "SAGE"),
	})
}

func sameIntroNeedsRebuild(data map[string]any, intro string) bool {
	if !strings.Contains(strings.ToLower(intro), "unknown location (") {
		return false
	}
	return len(stringListAny(firstValue(nil, data, "same_locations", "locations"))) > 0
}

func sameIntroCallsign(data map[string]any, feed feedXML, includeSourceLabel bool) string {
	if !includeSourceLabel {
		return ""
	}
	return fallbackText(firstText(nil, data, "same_callsign", "callsign"), feedCallsign(feed))
}

func stripTrailingSourceLabel(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || !strings.HasSuffix(trimmed, ")") && !strings.HasSuffix(trimmed, ").") {
		return trimmed
	}
	withoutFinalPeriod := strings.TrimSpace(strings.TrimSuffix(trimmed, "."))
	if !strings.HasSuffix(withoutFinalPeriod, ")") {
		return trimmed
	}
	open := strings.LastIndex(withoutFinalPeriod, "(")
	if open < 0 || open == 0 || strings.TrimSpace(withoutFinalPeriod[open+1:len(withoutFinalPeriod)-1]) == "" {
		return trimmed
	}
	prefix := strings.TrimSpace(withoutFinalPeriod[:open])
	if prefix == "" {
		return trimmed
	}
	return alerttext.CleanFragment(prefix)
}

func dataTimeValue(data map[string]any, keys ...string) time.Time {
	for _, key := range keys {
		raw := strings.TrimSpace(firstText(nil, data, key))
		if raw == "" {
			continue
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
			if parsed, err := time.Parse(layout, raw); err == nil {
				return parsed
			}
		}
		if parsed := alerttext.ParseCAPTime(raw); !parsed.IsZero() {
			return parsed
		}
	}
	return time.Time{}
}

func customAlertTextFromData(data map[string]any) string {
	text := strings.TrimSpace(firstText(nil, data, "alert_text", "tts_text", "text", "message"))
	if text == "" {
		return ""
	}
	for _, intro := range []string{
		firstText(nil, data, "same_translation"),
		firstText(nil, data, "same_intro"),
	} {
		intro = strings.TrimSpace(intro)
		if intro != "" && strings.HasPrefix(strings.ToLower(text), strings.ToLower(intro)) {
			return strings.TrimSpace(text[len(intro):])
		}
	}
	return text
}

func sameDurationToDuration(raw string) time.Duration {
	raw = normalizeSAMEDuration(raw)
	hours := intFromString(raw[:2], 0)
	minutes := intFromString(raw[2:], 15)
	if hours == 0 && minutes == 0 {
		minutes = 15
	}
	return time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute
}

func intFromString(raw string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value
}

func intFromAny(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return int(parsed)
		}
	case string:
		return intFromString(typed, fallback)
	}
	return fallback
}

func writeAlertAudioFile(path string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("alert audio is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func nonEmptyStrings(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if clean := strings.TrimSpace(value); clean != "" {
			out = append(out, clean)
		}
	}
	return out
}

func sameText(parts []string, value string) bool {
	value = strings.TrimSpace(value)
	for _, part := range parts {
		if strings.EqualFold(strings.TrimSpace(part), value) {
			return true
		}
	}
	return false
}

func alertAttentionToneEnabled(data map[string]any) bool {
	tone := strings.ToUpper(strings.TrimSpace(firstText(nil, data, "same_tone", "tone_type", "attention_tone")))
	switch tone {
	case "", "NONE", "NO", "OFF", "DISABLED":
		return false
	default:
		return true
	}
}

func (p *feedPlanner) renderAlertTTSAsPCM(ctx context.Context, queueID string, outputPath string, alertText string, sampleRate int, channels int, data map[string]any) error {
	readerID := strings.TrimSpace(firstText(nil, data, "reader_id", "tts_reader_id"))
	if readerID == "" {
		readerID = "00"
	}
	language := feedLanguage(p.feed)
	if strings.TrimSpace(alertText) == "" {
		renderCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		product, err := p.bridge.RenderProduct(renderCtx, queueID, p.feed.ID, "alerts", language)
		cancel()
		if err != nil {
			return err
		}
		alertText = product.Text
		if strings.TrimSpace(firstText(nil, data, "reader_id", "tts_reader_id")) == "" {
			readerID = fallbackText(product.ReaderID, readerID)
		}
		language = fallbackText(product.Language, language)
	}
	if strings.TrimSpace(alertText) == "" {
		return fmt.Errorf("rendered alert text is empty")
	}
	requestedWAVPath := filepath.Join(p.cfg.OutputDir, safeID(p.feed.ID), queueID+".wav")
	synthCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	wavPath, err := p.bridge.Synthesize(synthCtx, synthJob{
		ID:               queueID,
		Text:             alertText,
		ReaderID:         readerID,
		Language:         language,
		Timezone:         feedTimezone(p.feed),
		OutputPath:       requestedWAVPath,
		TargetSampleRate: sampleRate,
		TargetChannels:   channels,
		Priority:         "high",
	})
	if err != nil {
		_ = os.Remove(requestedWAVPath)
		return err
	}
	defer func() { _ = os.Remove(requestedWAVPath) }()
	if filepath.Clean(wavPath) != filepath.Clean(requestedWAVPath) {
		defer func() { _ = os.Remove(wavPath) }()
	}
	if err := wavToPCM16File(wavPath, outputPath, sampleRate, channels); err == nil {
		return nil
	}
	return convertAudioToPCM(ctx, wavPath, outputPath, sampleRate, channels)
}

func combineSAMEAlertAudio(outputPath string, header []byte, voicePath string, eom []byte, sampleRate int, channels int) error {
	return combineAlertAudio(outputPath, header, voicePath, eom, sampleRate, channels)
}

func combineAttentionAlertAudio(outputPath string, tone []byte, voicePath string, sampleRate int, channels int) error {
	return combineAlertAudio(outputPath, tone, voicePath, nil, sampleRate, channels)
}

func combineConfiguredLeadAlertAudio(outputPath string, leadIn []byte, headerOrTone []byte, voicePath string, eom []byte, leadOut []byte, sampleRate int, channels int) error {
	return combineAlertAudioSegments(outputPath, leadIn, headerOrTone, voicePath, eom, leadOut, sampleRate, channels)
}

func combineAlertAudio(outputPath string, lead []byte, voicePath string, tail []byte, sampleRate int, channels int) error {
	return combineAlertAudioSegments(outputPath, nil, lead, voicePath, tail, nil, sampleRate, channels)
}

func combineAlertAudioSegments(outputPath string, preRoll []byte, lead []byte, voicePath string, tail []byte, postRoll []byte, sampleRate int, channels int) error {
	voice, err := os.Open(voicePath)
	if err != nil {
		return err
	}
	defer voice.Close()
	voiceInfo, err := voice.Stat()
	if err != nil {
		return err
	}
	if voiceInfo.Size() == 0 {
		return fmt.Errorf("alert voice audio is empty")
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	tmp := outputPath + ".tmp"
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	writeErr := func() error {
		writeSegmentWithGap := func(segment []byte) error {
			if len(segment) == 0 {
				return nil
			}
			if _, err := file.Write(segment); err != nil {
				return err
			}
			if padding := alertTransitionPadding(segment, sampleRate, channels, time.Second); len(padding) > 0 {
				if _, err := file.Write(padding); err != nil {
					return err
				}
			}
			return nil
		}
		if err := writeSegmentWithGap(preRoll); err != nil {
			return err
		}
		if err := writeSegmentWithGap(lead); err != nil {
			return err
		}
		if _, err := io.Copy(file, voice); err != nil {
			return err
		}
		if len(tail) > 0 {
			if _, err := file.Write(tail); err != nil {
				return err
			}
		}
		if len(postRoll) > 0 {
			if len(tail) > 0 {
				if padding := alertTransitionPadding(tail, sampleRate, channels, time.Second); len(padding) > 0 {
					if _, err := file.Write(padding); err != nil {
						return err
					}
				}
			} else if padding := silencePCM(sampleRate, channels, time.Second); len(padding) > 0 {
				if _, err := file.Write(padding); err != nil {
					return err
				}
			}
			if _, err := file.Write(postRoll); err != nil {
				return err
			}
		}
		return nil
	}()
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(tmp)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	return os.Rename(tmp, outputPath)
}

func alertTransitionPadding(lead []byte, sampleRate int, channels int, minimum time.Duration) []byte {
	if sampleRate <= 0 {
		sampleRate = 48000
	}
	if channels <= 0 {
		channels = 1
	}
	minimumFrames := int((minimum*time.Duration(sampleRate) + time.Second - 1) / time.Second)
	remainingFrames := minimumFrames - trailingPCM16SilentFrames(lead, channels)
	if remainingFrames <= 0 {
		return nil
	}
	return make([]byte, remainingFrames*channels*2)
}

func trailingPCM16Silence(pcm []byte, sampleRate int, channels int) time.Duration {
	if sampleRate <= 0 {
		sampleRate = 48000
	}
	if channels <= 0 {
		channels = 1
	}
	silentFrames := trailingPCM16SilentFrames(pcm, channels)
	return time.Duration(silentFrames) * time.Second / time.Duration(sampleRate)
}

func trailingPCM16SilentFrames(pcm []byte, channels int) int {
	if channels <= 0 {
		channels = 1
	}
	frameBytes := channels * 2
	completeBytes := len(pcm) - len(pcm)%frameBytes
	silentFrames := 0
	for offset := completeBytes - frameBytes; offset >= 0; offset -= frameBytes {
		silent := true
		for _, value := range pcm[offset : offset+frameBytes] {
			if value != 0 {
				silent = false
				break
			}
		}
		if !silent {
			break
		}
		silentFrames++
	}
	return silentFrames
}

func (p *feedPlanner) buildTextItem(ctx context.Context, title string, text string, now time.Time) (playlistItem, error) {
	if strings.TrimSpace(text) == "" {
		return playlistItem{}, fmt.Errorf("text is required")
	}
	queueID := queueID("operator_text")
	outputPath := filepath.Join(p.cfg.OutputDir, safeID(p.feed.ID), queueID+".wav")
	wavPath, err := p.bridge.Synthesize(ctx, synthJob{
		ID:               queueID,
		Text:             text,
		Language:         feedLanguage(p.feed),
		Timezone:         feedTimezone(p.feed),
		OutputPath:       outputPath,
		TargetSampleRate: p.cfg.Root.Playout.SampleRate,
		TargetChannels:   p.cfg.Root.Playout.Channels,
		Priority:         "normal",
	})
	if err != nil {
		return playlistItem{}, err
	}
	info, err := wavInfo(wavPath)
	if err != nil {
		return playlistItem{}, err
	}
	start := predictedStart(now, p.timelineEnd(now), time.Time{})
	return playlistItem{
		QueueID:           queueID,
		FeedID:            p.feed.ID,
		Kind:              "tts",
		Title:             fallbackText(title, "Operator Text"),
		AudioPath:         wavPath,
		DurationMS:        info.DurationMS,
		QueuedAt:          now.UTC().Format(time.RFC3339Nano),
		PredictedStartAt:  start.UTC().Format(time.RFC3339Nano),
		PredictedFinishAt: start.Add(p.itemScheduleDuration(info.DurationMS)).UTC().Format(time.RFC3339Nano),
		Status:            "queued",
		Source:            "operator",
	}, nil
}

func (p *feedPlanner) buildAudioItem(title string, path string, now time.Time) (playlistItem, error) {
	if strings.TrimSpace(path) == "" {
		return playlistItem{}, fmt.Errorf("audio path is required")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(p.cfg.BaseDir, path)
	}
	info, err := wavInfo(path)
	if err != nil {
		return playlistItem{}, err
	}
	start := predictedStart(now, p.timelineEnd(now), time.Time{})
	return playlistItem{
		QueueID:           queueID("operator_audio"),
		FeedID:            p.feed.ID,
		Kind:              "audio",
		Title:             fallbackText(title, filepath.Base(path)),
		AudioPath:         path,
		DurationMS:        info.DurationMS,
		QueuedAt:          now.UTC().Format(time.RFC3339Nano),
		PredictedStartAt:  start.UTC().Format(time.RFC3339Nano),
		PredictedFinishAt: start.Add(p.itemScheduleDuration(info.DurationMS)).UTC().Format(time.RFC3339Nano),
		Status:            "queued",
		Source:            "operator",
	}, nil
}

func (p *feedPlanner) applyControl(action string) {
	action = strings.ToLower(strings.TrimSpace(action))
	switch action {
	case "pause":
		p.mode = "paused"
	case "resume", "restart":
		p.mode = "running"
		if action == "restart" {
			p.queue = nil
			p.current = nil
			p.cursor = 0
		}
	case "flush_restart":
		p.queue = nil
		p.current = nil
		p.cursor = 0
		p.mode = "running"
	case "flush_stop":
		p.queue = nil
		p.current = nil
		p.mode = "stopped"
	case "pause_after_current", "flush_restart_after_current", "flush_stop_after_current":
		p.pendingAfterCurrent = action
	}
	p.writeState()
}

func (p *feedPlanner) markPriorityStarted() {
	if p.priorityActive == 0 {
		p.modeBeforePriority = p.mode
		p.publishPlayoutControl("pause")
	}
	p.priorityActive++
	p.mode = "priority"
	p.writeState()
}

func (p *feedPlanner) markPriorityCompleted() {
	if p.priorityActive > 0 {
		p.priorityActive--
	}
	if p.priorityActive == 0 {
		p.mode = fallbackText(p.modeBeforePriority, "running")
		p.modeBeforePriority = ""
		if p.mode == "running" {
			p.publishPlayoutControl("resume")
		}
	}
	p.writeState()
}

func (p *feedPlanner) publishPlayoutControl(action string) {
	if p.bridge == nil {
		return
	}
	if err := p.bridge.Publish(map[string]any{
		"type":    "playlist.control",
		"source":  serviceID,
		"feed_id": p.feed.ID,
		"data":    map[string]any{"feed_id": p.feed.ID, "action": action},
	}); err != nil {
		p.lastError = err.Error()
	}
}

func (p *feedPlanner) markStarted(queueID string) {
	if queueID == "" {
		return
	}
	if p.current != nil && p.current.QueueID == queueID {
		p.current.Status = "playing"
		p.writeState()
		return
	}
	for i := range p.queue {
		if p.queue[i].QueueID == queueID {
			item := p.queue[i]
			item.Status = "playing"
			p.current = &item
			p.queue = append(p.queue[:i], p.queue[i+1:]...)
			p.writeState()
			return
		}
	}
}

func (p *feedPlanner) markInterrupted(queueID string) {
	if p.current != nil && (queueID == "" || p.current.QueueID == queueID) {
		p.current.Status = "interrupted"
		p.writeState()
		return
	}
}

func (p *feedPlanner) markCompleted(queueID string) {
	if p.current != nil && (queueID == "" || p.current.QueueID == queueID) {
		p.current = nil
	}
	switch p.pendingAfterCurrent {
	case "pause_after_current":
		p.mode = "paused"
		p.queue = nil
	case "flush_restart_after_current":
		p.mode = "running"
		p.queue = nil
		p.cursor = 0
	case "flush_stop_after_current":
		p.mode = "stopped"
		p.queue = nil
	}
	p.pendingAfterCurrent = ""
	p.writeState()
}

func (p *feedPlanner) dropCompleted() {
	now := time.Now()
	filtered := p.queue[:0]
	for _, item := range p.queue {
		finish := parseTime(item.PredictedFinishAt)
		if !finish.IsZero() && finish.Before(now.Add(-5*time.Minute)) {
			continue
		}
		filtered = append(filtered, item)
	}
	p.queue = filtered
}

func (p *feedPlanner) queuedCount() int {
	if p.current != nil {
		return len(p.queue) + 1
	}
	return len(p.queue)
}

func (p *feedPlanner) itemScheduleDuration(durationMS int64) time.Duration {
	return time.Duration(durationMS)*time.Millisecond + p.packageGap()
}

func (p *feedPlanner) packageGap() time.Duration {
	gap := p.cfg.Root.Playout.Pacing.PackageGapS
	if gap <= 0 {
		return 0
	}
	return time.Duration(gap * float64(time.Second))
}

func (p *feedPlanner) timelineEnd(now time.Time) time.Time {
	end := now
	if p.current != nil {
		if finish := parseTime(p.current.PredictedFinishAt); finish.After(end) {
			end = finish
		}
	}
	for _, item := range p.queue {
		if finish := parseTime(item.PredictedFinishAt); finish.After(end) {
			end = finish
		}
	}
	return end
}

func (p *feedPlanner) publishReady(item playlistItem) error {
	if p.bridge == nil {
		return fmt.Errorf("event bridge is unavailable")
	}
	if err := p.bridge.Publish(map[string]any{
		"type":    "playlist.item.ready",
		"source":  serviceID,
		"feed_id": item.FeedID,
		"data": map[string]any{
			"queue_id":            item.QueueID,
			"feed_id":             item.FeedID,
			"kind":                item.Kind,
			"package_id":          item.PackageID,
			"title":               item.Title,
			"audio_path":          item.AudioPath,
			"duration_ms":         item.DurationMS,
			"queued_at":           item.QueuedAt,
			"target_start_at":     item.TargetStartAt,
			"predicted_start_at":  item.PredictedStartAt,
			"predicted_finish_at": item.PredictedFinishAt,
			"not_before":          item.PredictedStartAt,
			"source":              item.Source,
		},
	}); err != nil {
		return err
	}
	p.lastPendingReplayAt = time.Now()
	return nil
}

func (p *feedPlanner) replayPendingItems() {
	p.replayPendingItemsAt(time.Now())
}

func (p *feedPlanner) replayPendingItemsIfDue(now time.Time) {
	if len(p.queue) == 0 {
		return
	}
	if !p.lastPendingReplayAt.IsZero() && now.Sub(p.lastPendingReplayAt) < pendingReplayInterval {
		return
	}
	p.replayQueuedItemsAt(now)
}

func (p *feedPlanner) replayPendingItemsAt(now time.Time) {
	p.replayItemsAt(now, true)
}

func (p *feedPlanner) replayQueuedItemsAt(now time.Time) {
	p.replayItemsAt(now, false)
}

func (p *feedPlanner) replayItemsAt(now time.Time, includeCurrent bool) {
	p.lastPendingReplayAt = now
	items := make([]playlistItem, 0, len(p.queue)+1)
	if includeCurrent && p.current != nil {
		current := *p.current
		current.Status = "queued"
		items = append(items, current)
	}
	items = append(items, p.queue...)
	for _, item := range items {
		if err := p.publishReady(item); err != nil {
			p.lastError = err.Error()
			return
		}
	}
}

func (p *feedPlanner) writeState() {
	payload := map[string]any{
		"feed_id":               p.feed.ID,
		"feed_name":             feedName(p.feed),
		"mode":                  p.mode,
		"priority_active":       p.priorityActive > 0,
		"pending_after_current": p.pendingAfterCurrent,
		"current":               p.current,
		"queue":                 p.queue,
		"next":                  firstQueued(p.queue),
		"last_error":            p.lastError,
		"updated_at":            time.Now().UTC().Format(time.RFC3339Nano),
	}
	path := filepath.Join(p.cfg.BaseDir, "runtime", "playlists", safeID(p.feed.ID)+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return
	}
	tmp := fmt.Sprintf("%s.%d.tmp", path, time.Now().UnixNano())
	if os.WriteFile(tmp, append(raw, '\n'), 0o644) == nil {
		if err := os.Rename(tmp, path); err != nil {
			_ = os.Remove(path)
			_ = os.Rename(tmp, path)
		}
	}
}

func firstQueued(queue []playlistItem) *playlistItem {
	if len(queue) == 0 {
		return nil
	}
	item := queue[0]
	return &item
}

func isPlayoutReadyEvent(event map[string]any) bool {
	if strings.EqualFold(stringAt(event, "source"), "haze-playout") {
		return true
	}
	data := mapAt(event, "data")
	return strings.EqualFold(firstText(event, data, "service"), "haze-playout")
}

func predictedStart(now time.Time, timelineEnd time.Time, target time.Time) time.Time {
	start := timelineEnd
	if start.Before(now) {
		start = now
	}
	if !target.IsZero() && start.Before(target) {
		start = target
	}
	return start
}

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err == nil {
		return value
	}
	value, err = time.Parse(time.RFC3339, raw)
	if err == nil {
		return value
	}
	return time.Time{}
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func queueID(prefix string) string {
	var raw [6]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return safeID(fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()))
	}
	return safeID(fmt.Sprintf("%s-%d-%s", prefix, time.Now().UnixNano(), hex.EncodeToString(raw[:])))
}

func safeID(value string) string {
	var builder strings.Builder
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.' {
			builder.WriteRune(ch)
		}
	}
	if builder.Len() == 0 {
		return "item"
	}
	return builder.String()
}

func uniqueStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func sleepOrDone(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
