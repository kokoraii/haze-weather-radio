package playlist

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPriorityPreparationReturnsPromptlyAndCancellationDiscardsResult(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	workPath := filepath.Join(planner.cfg.BaseDir, "priority-work.pcm16le")
	finalPath := filepath.Join(planner.cfg.BaseDir, "runtime", "audio", "alerts", "final.pcm16le")
	service.preparations.preparePriority = func(_ context.Context, _ *feedPlanner, data map[string]any) (priorityAlertPreparation, error) {
		if err := os.WriteFile(workPath, []byte{1, 0, 2, 0}, 0o600); err != nil {
			return priorityAlertPreparation{}, err
		}
		started <- struct{}{}
		<-release
		return priorityAlertPreparation{
			Manifest: priorityAlertManifest{
				ID:        "priority-test",
				AlertID:   firstText(nil, data, "alert_id"),
				AudioPath: filepath.ToSlash(filepath.Join("runtime", "audio", "alerts", "final.pcm16le")),
			},
			Data:           data,
			AudioPath:      workPath,
			FinalAudioPath: finalPath,
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan struct{})
	go func() {
		service.handleEvent(ctx, map[string]any{
			"type": "cap.alert.broadcast.requested",
			"data": map[string]any{"feed_id": planner.feed.ID, "alert_id": "urn:test:async"},
		})
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("priority event handling blocked on preparation")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("priority preparation did not start")
	}

	service.handleEvent(ctx, map[string]any{
		"type": "cap.alert.cancelled",
		"data": map[string]any{"feed_id": planner.feed.ID, "alert_id": "urn:test:async"},
	})
	close(release)
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))

	if _, err := os.Stat(workPath); !os.IsNotExist(err) {
		t.Fatalf("cancelled preparation work file still exists, err=%v", err)
	}
	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Fatalf("cancelled preparation installed final audio, err=%v", err)
	}
	manifestPath := filepath.Join(planner.cfg.BaseDir, "runtime", "queues", "alerts", "priority-test.json")
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("cancelled preparation published a manifest, err=%v", err)
	}
}

func TestCancelledCoordinatorWaitCleansUncommittedPriorityWork(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	workPath := filepath.Join(planner.cfg.BaseDir, "shutdown-work.pcm16le")
	service.preparations.preparePriority = func(_ context.Context, _ *feedPlanner, data map[string]any) (priorityAlertPreparation, error) {
		if err := os.WriteFile(workPath, []byte{1, 0}, 0o600); err != nil {
			return priorityAlertPreparation{}, err
		}
		started <- struct{}{}
		<-release
		return priorityAlertPreparation{
			Manifest:  priorityAlertManifest{ID: "shutdown-test", AlertID: firstText(nil, data, "alert_id")},
			AudioPath: workPath,
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	service.enqueuePriorityPreparation(ctx, planner, map[string]any{"alert_id": "urn:test:shutdown"})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("priority preparation did not start")
	}
	cancel()
	close(release)
	service.preparations.wait()
	service.discardUncommittedPreparations()
	if _, err := os.Stat(workPath); !os.IsNotExist(err) {
		t.Fatalf("shutdown left uncommitted priority work, err=%v", err)
	}
}

func TestRoutinePreparationCannotConsumeReservedPriorityCapacity(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	routineStarted := make(chan struct{}, 1)
	releaseRoutine := make(chan struct{})
	priorityStarted := make(chan struct{}, 1)
	service.preparations.prepareRoutine = func(_ context.Context, snapshot *feedPlanner, _ bool, _ time.Time) routinePreparation {
		routineStarted <- struct{}{}
		<-releaseRoutine
		snapshot.nextRoutineRetryAt = time.Now().Add(time.Hour)
		return routinePreparation{planner: snapshot}
	}
	service.preparations.preparePriority = func(_ context.Context, _ *feedPlanner, _ map[string]any) (priorityAlertPreparation, error) {
		priorityStarted <- struct{}{}
		return priorityAlertPreparation{}, errors.New("test priority completion")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.tickFeedAsync(ctx, planner, time.Now())
	select {
	case <-routineStarted:
	case <-time.After(time.Second):
		t.Fatal("routine preparation did not start")
	}

	service.enqueuePriorityPreparation(ctx, planner, map[string]any{"alert_id": "urn:test:reserved"})
	select {
	case <-priorityStarted:
	case <-time.After(100 * time.Millisecond):
		close(releaseRoutine)
		t.Fatal("priority preparation waited for the routine worker")
	}
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
	close(releaseRoutine)
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
}

func TestPriorityPreparationCancelsAndSuspendsRoutineWork(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	routineStarted := make(chan struct{}, 1)
	routineCancelled := make(chan struct{}, 1)
	priorityStarted := make(chan struct{}, 1)
	releasePriority := make(chan struct{})
	var routineStarts atomic.Int32
	service.preparations.prepareRoutine = func(ctx context.Context, snapshot *feedPlanner, _ bool, _ time.Time) routinePreparation {
		routineStarts.Add(1)
		routineStarted <- struct{}{}
		<-ctx.Done()
		routineCancelled <- struct{}{}
		return routinePreparation{planner: snapshot}
	}
	service.preparations.preparePriority = func(_ context.Context, _ *feedPlanner, _ map[string]any) (priorityAlertPreparation, error) {
		priorityStarted <- struct{}{}
		<-releasePriority
		return priorityAlertPreparation{}, errors.New("test completion")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.tickFeedAsync(ctx, planner, time.Now())
	select {
	case <-routineStarted:
	case <-time.After(time.Second):
		t.Fatal("routine preparation did not start")
	}
	service.enqueuePriorityPreparation(ctx, planner, map[string]any{"alert_id": "urn:test:preempt"})
	select {
	case <-routineCancelled:
	case <-time.After(time.Second):
		t.Fatal("priority preparation did not cancel routine work")
	}
	select {
	case <-priorityStarted:
	case <-time.After(time.Second):
		t.Fatal("priority preparation did not start")
	}
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
	time.Sleep(25 * time.Millisecond)
	if got := routineStarts.Load(); got != 1 {
		t.Fatalf("routine preparation restarted during priority work, starts=%d", got)
	}
	close(releasePriority)
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
}

func TestPriorityPreparationConcurrencyIsBounded(t *testing.T) {
	feedIDs := []string{"feed-1", "feed-2", "feed-3", "feed-4", "feed-5"}
	service, _ := newAsyncTestService(t, feedIDs, 1, 2)
	started := make(chan struct{}, len(feedIDs))
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	service.preparations.preparePriority = func(_ context.Context, _ *feedPlanner, _ map[string]any) (priorityAlertPreparation, error) {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return priorityAlertPreparation{}, errors.New("test completion")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, feedID := range feedIDs {
		service.enqueuePriorityPreparation(ctx, service.feeds[feedID], map[string]any{"alert_id": "urn:test:" + feedID})
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("initial priority workers did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("priority worker limit was exceeded")
	case <-time.After(50 * time.Millisecond):
	}

	for completed := 0; completed < len(feedIDs); completed++ {
		release <- struct{}{}
		service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
		if completed+2 < len(feedIDs) {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("queued priority preparation did not start after capacity freed")
			}
		}
	}
	if got := maximum.Load(); got != 2 {
		t.Fatalf("maximum concurrent priority preparations = %d, want 2", got)
	}
}

func TestStalePendingPriorityPreparationIsPrunedBeforeWorkerStart(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	started := make(chan struct{}, 1)
	service.preparations.preparePriority = func(_ context.Context, _ *feedPlanner, _ map[string]any) (priorityAlertPreparation, error) {
		started <- struct{}{}
		return priorityAlertPreparation{}, errors.New("unexpected preparation")
	}
	service.priorityPending = []priorityPreparationJob{{
		token:   1,
		feedID:  planner.feed.ID,
		alertID: "urn:test:expired",
		version: 1,
		data: map[string]any{
			"alert_id":         "urn:test:expired",
			"alert_expires_at": time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano),
		},
	}}
	service.startPendingPriorityPreparations(context.Background())
	if len(service.priorityPending) != 0 {
		t.Fatalf("stale priority work remained pending: %#v", service.priorityPending)
	}
	select {
	case <-started:
		t.Fatal("stale priority work consumed a worker")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestInvalidatedRoutinePreparationDoesNotEnterQueue(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	service.preparations.prepareRoutine = func(_ context.Context, snapshot *feedPlanner, _ bool, now time.Time) routinePreparation {
		started <- struct{}{}
		<-release
		return routinePreparation{
			planner: snapshot,
			items: []playlistItem{{
				QueueID:          "stale-routine",
				FeedID:           planner.feed.ID,
				PackageID:        "alerts",
				DurationMS:       1000,
				PredictedStartAt: now.Format(time.RFC3339Nano),
			}},
			ok: true,
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.tickFeedAsync(ctx, planner, time.Now())
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("routine preparation did not start")
	}
	service.handleEvent(ctx, map[string]any{
		"type": "playlist.control",
		"data": map[string]any{"feed_id": planner.feed.ID, "action": "pause"},
	})
	close(release)
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
	if len(planner.queue) != 0 {
		t.Fatalf("invalidated routine result entered queue: %#v", planner.queue)
	}
}

func TestGeneratePrioritySAMEPairRunsIndependentPartsConcurrently(t *testing.T) {
	planner := &feedPlanner{}
	planner.cfg.Root.Playout.SampleRate = 48000
	planner.cfg.Root.Playout.Channels = 1
	started := make(chan string, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	planner.sameGenerator = func(_ context.Context, _ string, request sameGenerateRequest) (map[string]any, error) {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- request.Sequence
		<-release
		active.Add(-1)
		return testSAMEResult(request.Sequence), nil
	}
	type pairResult struct {
		request sameGenerateRequest
		header  sameAudioPayload
		eom     sameAudioPayload
		err     error
	}
	result := make(chan pairResult, 1)
	go func() {
		request, header, eom, err := planner.generatePrioritySAMEPair(context.Background(), map[string]any{
			"same_locations": []any{"123456"},
		})
		result <- pairResult{request: request, header: header, eom: eom, err: err}
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("SAME header and EOM did not both start")
		}
	}
	release <- struct{}{}
	release <- struct{}{}
	got := <-result
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.request.Sequence != "header" || got.header.Header != "header" || got.eom.Header != "eom" {
		t.Fatalf("SAME pair result = %#v, %#v, %#v", got.request, got.header, got.eom)
	}
	if maximum.Load() != 2 {
		t.Fatalf("maximum concurrent SAME generations = %d, want 2", maximum.Load())
	}
}

func TestPrepareProductSegmentsStartsIndependentSynthesisConcurrently(t *testing.T) {
	dir := t.TempDir()
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	bridge := &bridgeClient{
		conn:            clientConn,
		events:          make(chan map[string]any, 4),
		pendingProducts: map[string]chan productResult{},
		pendingSynth:    map[string]chan synthResult{},
	}
	go bridge.readLoop()
	requests := make(chan map[string]any, 2)
	go func() {
		decoder := json.NewDecoder(serverConn)
		for {
			var message map[string]any
			if decoder.Decode(&message) != nil {
				return
			}
			if stringAt(message, "type") == "tts.synthesize" {
				requests <- message
			}
		}
	}()
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir:   dir,
			OutputDir: filepath.Join(dir, "runtime", "audio", "playout"),
			Root:      rootConfig{Playout: playoutConfig{SampleRate: 48000, Channels: 1}},
		},
		bridge: bridge,
		feed:   feedXML{ID: "feed-1", Timezone: "UTC"},
	}
	segments := []renderedSegment{{Kind: "package", Text: "First."}, {Kind: "package", Text: "Second."}}
	product := renderedProduct{PackageID: "forecast", ReaderID: "00", Language: "en-CA"}
	type segmentPreparationResult struct {
		segments []preparedProductSegment
		err      error
	}
	prepared := make(chan segmentPreparationResult, 1)
	go func() {
		result, err := planner.prepareProductSegments(context.Background(), segments, 0, product, "routine", planner.cfg.OutputDir, "segment-concurrency", 48000, 1)
		prepared <- segmentPreparationResult{segments: result, err: err}
	}()
	first := waitSynthRequest(t, requests)
	second := waitSynthRequest(t, requests)
	responses := []map[string]any{second, first}
	encoder := json.NewEncoder(serverConn)
	for _, request := range responses {
		data := mapAt(request, "data")
		outputPath := firstText(request, data, "output_path")
		jobID := firstText(request, data, "job_id", "subject")
		value := byte(20)
		if strings.Contains(jobID, "-seg-00") {
			value = 10
		}
		pcm := []byte{value, 0, value + 1, 0}
		if err := writePCM16WAV(outputPath, pcm, 48000, 1); err != nil {
			t.Fatal(err)
		}
		if err := encoder.Encode(map[string]any{
			"type": "tts.synthesized",
			"data": map[string]any{"job_id": jobID, "output_path": outputPath},
		}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case result := <-prepared:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if len(result.segments) != 2 || len(result.segments[0].textPCM) == 0 || len(result.segments[1].textPCM) == 0 {
			t.Fatalf("prepared segments = %#v", result.segments)
		}
		if result.segments[0].textPCM[0] != 10 || result.segments[1].textPCM[0] != 20 {
			t.Fatalf("prepared segment order = %v then %v", result.segments[0].textPCM, result.segments[1].textPCM)
		}
	case <-time.After(time.Second):
		t.Fatal("segment preparation did not complete")
	}
	if matches, err := filepath.Glob(filepath.Join(planner.cfg.OutputDir, "segment-concurrency-*")); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("segmented preparation left scratch files: %v", matches)
	}
}

func TestAlertTransitionPaddingCountsExistingTrailingSilence(t *testing.T) {
	const sampleRate = 1000
	lead := append([]byte{1, 0}, silencePCM(sampleRate, 1, 250*time.Millisecond)...)
	padding := alertTransitionPadding(lead, sampleRate, 1, time.Second)
	if got, want := len(padding), len(silencePCM(sampleRate, 1, 750*time.Millisecond)); got != want {
		t.Fatalf("transition padding bytes = %d, want %d", got, want)
	}
	if got := trailingPCM16Silence(append(lead, padding...), sampleRate, 1); got != time.Second {
		t.Fatalf("combined trailing silence = %s, want 1s", got)
	}
}

func TestWritePriorityAlertManifestReplacesExistingVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerts", "same-id.json")
	if err := writePriorityAlertManifest(path, priorityAlertManifest{ID: "same-id", Header: "old"}); err != nil {
		t.Fatal(err)
	}
	if err := writePriorityAlertManifest(path, priorityAlertManifest{ID: "same-id", Header: "new"}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"header": "new"`) {
		t.Fatalf("replacement manifest = %s", raw)
	}
	backups, err := filepath.Glob(path + ".*.bak")
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 0 {
		t.Fatalf("replacement left backup files: %v", backups)
	}
}

func TestCommitPriorityAlertAtomicallyInstallsPreparedAudio(t *testing.T) {
	dir := t.TempDir()
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	published := make(chan map[string]any, 1)
	go func() {
		var message map[string]any
		if json.NewDecoder(serverConn).Decode(&message) == nil {
			published <- message
		}
	}()
	planner := &feedPlanner{
		cfg:    loadedConfig{BaseDir: dir},
		bridge: &bridgeClient{conn: clientConn},
		feed:   feedXML{ID: "feed-1"},
	}
	queueID := safeID("001_feed-1_test_cap")
	audioRel := filepath.ToSlash(filepath.Join("runtime", "audio", "alerts", queueID+".pcm16le"))
	finalPath := filepath.Join(dir, filepath.FromSlash(audioRel))
	workPath := finalPath + ".work"
	if err := os.MkdirAll(filepath.Dir(workPath), 0o755); err != nil {
		t.Fatal(err)
	}
	wantAudio := []byte{1, 0, 2, 0}
	if err := os.WriteFile(workPath, wantAudio, 0o600); err != nil {
		t.Fatal(err)
	}
	prepared := priorityAlertPreparation{
		Manifest: priorityAlertManifest{
			ID:         queueID,
			AlertID:    "test",
			Type:       "cap_alert",
			Status:     "pending",
			FeedIDs:    []string{planner.feed.ID},
			AudioPath:  audioRel,
			Format:     "pcm_s16le",
			SampleRate: 48000,
			Channels:   1,
			AudioBytes: len(wantAudio),
		},
		Data:           map[string]any{"alert_id": "test"},
		AudioPath:      workPath,
		FinalAudioPath: finalPath,
	}
	if err := planner.commitPriorityAlert(prepared); err != nil {
		t.Fatal(err)
	}
	gotAudio, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotAudio) != string(wantAudio) {
		t.Fatalf("installed audio = %v, want %v", gotAudio, wantAudio)
	}
	if _, err := os.Stat(workPath); !os.IsNotExist(err) {
		t.Fatalf("prepared work file remained after commit, err=%v", err)
	}
	manifestPath := filepath.Join(dir, "runtime", "queues", "alerts", queueID+".json")
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("priority manifest was not committed: %v", err)
	}
	select {
	case message := <-published:
		if stringAt(message, "type") != "cap.alert.audio.ready" {
			t.Fatalf("published event = %#v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("priority ready event was not published")
	}
}

func BenchmarkGeneratePrioritySAMEPair(b *testing.B) {
	planner := &feedPlanner{}
	planner.cfg.Root.Playout.SampleRate = 48000
	planner.cfg.Root.Playout.Channels = 1
	planner.sameGenerator = func(_ context.Context, _ string, request sameGenerateRequest) (map[string]any, error) {
		time.Sleep(2 * time.Millisecond)
		return testSAMEResult(request.Sequence), nil
	}
	data := map[string]any{"same_locations": []any{"123456"}}
	b.Run("serial_reference", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, headerResult, err := planner.generatePrioritySAME(context.Background(), data, "header")
			if err != nil {
				b.Fatal(err)
			}
			header, err := sameAudioFromResult(headerResult, 48000, 1)
			if err != nil {
				b.Fatal(err)
			}
			_, eomResult, err := planner.generatePrioritySAME(context.Background(), data, "eom")
			if err != nil {
				b.Fatal(err)
			}
			if _, err := sameAudioFromResult(eomResult, header.SampleRate, header.Channels); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, _, _, err := planner.generatePrioritySAMEPair(context.Background(), data); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPrepareProductSegments(b *testing.B) {
	planner := &feedPlanner{
		segmentSynthesizer: func(_ context.Context, _ int, _ string) ([]byte, error) {
			time.Sleep(2 * time.Millisecond)
			return []byte{1, 0, 2, 0}, nil
		},
	}
	segments := []renderedSegment{
		{Kind: "package", Text: "One."},
		{Kind: "package", Text: "Two."},
		{Kind: "package", Text: "Three."},
		{Kind: "package", Text: "Four."},
	}
	product := renderedProduct{PackageID: "forecast"}
	outputDir := b.TempDir()
	b.Run("serial_reference", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			for index, segment := range segments {
				if _, err := planner.segmentSynthesizer(context.Background(), index, segment.Text); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := planner.prepareProductSegments(context.Background(), segments, 0, product, "routine", outputDir, "segment-benchmark", 48000, 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestBridgeCriticalCAPEventsSurviveNormalBackpressure(t *testing.T) {
	normal := make(chan map[string]any, 1)
	critical := make(chan map[string]any, 2)
	bridge := &bridgeClient{
		events:         normal,
		criticalEvents: critical,
		stop:           make(chan struct{}),
	}

	bridge.enqueueEvent(map[string]any{"type": "cap.alert.received"})
	bridge.enqueueEvent(map[string]any{"type": "cap.alert.broadcast.requested", "subject": "broadcast-1"})
	bridge.enqueueEvent(map[string]any{"type": "cap.alert.cancelled", "subject": "cancel-1"})

	for _, want := range []string{"cap.alert.broadcast.requested", "cap.alert.cancelled"} {
		select {
		case event := <-critical:
			if got := stringAt(event, "type"); got != want {
				t.Fatalf("critical event = %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("critical event %q was not retained", want)
		}
	}
}

func TestPriorityPreparationBacklogIsBoundedAndReportsDrop(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	service.maxPriorityPending = 2
	service.priorityPending = []priorityPreparationJob{
		{feedID: planner.feed.ID, alertID: "pending-1", version: 1, data: map[string]any{"alert_id": "pending-1"}},
		{feedID: planner.feed.ID, alertID: "pending-2", version: 1, data: map[string]any{"alert_id": "pending-2"}},
	}

	service.enqueuePriorityPreparation(context.Background(), planner, map[string]any{"alert_id": "pending-3"})

	if got := len(service.priorityPending); got != service.maxPriorityPending {
		t.Fatalf("pending priority jobs = %d, want bound %d", got, service.maxPriorityPending)
	}
	if !strings.Contains(service.feeds[planner.feed.ID].lastError, "backlog is full") {
		t.Fatalf("drop error = %q", service.feeds[planner.feed.ID].lastError)
	}
}

func TestPriorityPreparationIsOrderedPerFeed(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	started := make(chan string, 2)
	release := make(chan struct{})
	service.preparations.preparePriority = func(_ context.Context, _ *feedPlanner, data map[string]any) (priorityAlertPreparation, error) {
		started <- firstText(nil, data, "alert_id")
		<-release
		return priorityAlertPreparation{}, errors.New("test priority completion")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service.enqueuePriorityPreparation(ctx, planner, map[string]any{"alert_id": "alert-1"})
	select {
	case got := <-started:
		if got != "alert-1" {
			t.Fatalf("first priority job = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("first priority job did not start")
	}
	service.enqueuePriorityPreparation(ctx, planner, map[string]any{"alert_id": "alert-2"})
	select {
	case got := <-started:
		t.Fatalf("second priority job started before first completed: %q", got)
	case <-time.After(50 * time.Millisecond):
	}

	release <- struct{}{}
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
	select {
	case got := <-started:
		if got != "alert-2" {
			t.Fatalf("second priority job = %q, want alert-2", got)
		}
	case <-time.After(time.Second):
		t.Fatal("second priority job did not start after first completed")
	}
	release <- struct{}{}
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
}

func TestCAPCancellationUsesTopLevelSubjectToInvalidatePreparation(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	service.preparations.preparePriority = func(_ context.Context, _ *feedPlanner, _ map[string]any) (priorityAlertPreparation, error) {
		started <- struct{}{}
		<-release
		return priorityAlertPreparation{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.enqueuePriorityPreparation(ctx, planner, map[string]any{"alert_id": "subject-alert"})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("priority preparation did not start")
	}

	service.handleEvent(ctx, map[string]any{
		"type":    "cap.alert.cancelled",
		"feed_id": planner.feed.ID,
		"subject": "subject-alert",
		"data":    map[string]any{"feed_id": planner.feed.ID},
	})
	if service.preparationState(planner.feed.ID).priorityVersions["subject-alert"] == 0 {
		t.Fatal("top-level cancellation subject did not invalidate preparation")
	}
	release <- struct{}{}
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
	if state := service.preparationState(planner.feed.ID); state.priorityActive != nil {
		t.Fatalf("cancelled preparation remained active: %#v", state.priorityActive)
	}
}

func TestPlaylistInsertPreparationDoesNotBlockEventHandling(t *testing.T) {
	service, planner := newAsyncTestService(t, []string{"sk-0001"}, 1, 1)
	planner.nextRoutineRetryAt = time.Now().Add(time.Hour)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	service.preparations.prepareInsert = func(_ context.Context, snapshot *feedPlanner, _ map[string]any) routinePreparation {
		started <- struct{}{}
		<-release
		return routinePreparation{planner: snapshot}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan struct{})
	go func() {
		service.handleEvent(ctx, map[string]any{
			"type": "playlist.insert",
			"data": map[string]any{"feed_id": planner.feed.ID, "kind": "tts", "text": "slow"},
		})
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("playlist.insert blocked on preparation")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("insert preparation did not start")
	}
	close(release)
	service.handlePreparationResult(ctx, waitPreparationResult(t, service.preparations.results))
}

func TestBridgeSynthesizePropagatesRoutineAndAlertPriorities(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	bridge := &bridgeClient{
		conn:            clientConn,
		pendingProducts: map[string]chan productResult{},
		pendingSynth:    map[string]chan synthResult{},
	}
	go bridge.readLoop()
	priorities := make(chan string, 2)
	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		for {
			var message map[string]any
			if decoder.Decode(&message) != nil {
				return
			}
			data := mapAt(message, "data")
			priorities <- firstText(message, data, "priority")
			jobID := firstText(message, data, "job_id", "subject")
			if encoder.Encode(map[string]any{
				"type": "tts.synthesized",
				"data": map[string]any{"job_id": jobID, "output_path": "test.wav"},
			}) != nil {
				return
			}
		}
	}()
	if _, err := bridge.Synthesize(context.Background(), synthJob{ID: "routine", Priority: "normal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.Synthesize(context.Background(), synthJob{ID: "alert", Priority: "high"}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"normal", "high"} {
		select {
		case got := <-priorities:
			if got != want {
				t.Fatalf("synthesis priority = %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing synthesis priority %q", want)
		}
	}
}

func TestBridgeSynthesizePropagatesRequestDeadline(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})
	bridge := &bridgeClient{
		conn:            clientConn,
		pendingProducts: map[string]chan productResult{},
		pendingSynth:    map[string]chan synthResult{},
	}
	go bridge.readLoop()
	deadlineValues := make(chan string, 1)
	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		var message map[string]any
		if decoder.Decode(&message) != nil {
			return
		}
		data := mapAt(message, "data")
		deadlineValues <- firstText(message, data, "deadline_at")
		jobID := firstText(message, data, "job_id", "subject")
		_ = encoder.Encode(map[string]any{
			"type": "tts.synthesized",
			"data": map[string]any{"job_id": jobID, "output_path": "test.wav"},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := bridge.Synthesize(ctx, synthJob{ID: "deadline"}); err != nil {
		t.Fatal(err)
	}
	select {
	case raw := <-deadlineValues:
		deadline, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			t.Fatalf("deadline = %q: %v", raw, err)
		}
		if deadline.Before(time.Now()) {
			t.Fatalf("propagated deadline is already expired: %s", deadline)
		}
	case <-time.After(time.Second):
		t.Fatal("missing propagated synthesis deadline")
	}
}

func TestRebasePreparedItemDoesNotCatchUpMissedTargetOrAddExtraGap(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	planner := &feedPlanner{}
	planner.cfg.Root.Playout.Pacing.PackageGapS = 1
	item := playlistItem{
		TargetStartAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		DurationMS:    2000,
	}

	got := planner.rebasePreparedItem(item, now)
	start := parseTime(got.PredictedStartAt)
	finish := parseTime(got.PredictedFinishAt)
	if start.Before(now) {
		t.Fatalf("prepared item caught up from %s to %s", now, start)
	}
	if want := start.Add(3 * time.Second); !finish.Equal(want) {
		t.Fatalf("scheduled finish = %s, want one 1s gap at %s", finish, want)
	}
}

func newAsyncTestService(t *testing.T, feedIDs []string, routineWorkers int, priorityWorkers int) (*Service, *feedPlanner) {
	t.Helper()
	dir := t.TempDir()
	cfg := loadedConfig{BaseDir: dir, OutputDir: filepath.Join(dir, "runtime", "audio", "playout")}
	cfg.Root.Playout.SampleRate = 48000
	cfg.Root.Playout.Channels = 1
	cfg.Root.Playout.PlaylistOrder = []string{"forecast"}
	cfg.Root.Services.Go.Playlist.MaxQueued = 2
	feeds := make(map[string]*feedPlanner, len(feedIDs))
	for _, feedID := range feedIDs {
		planner := newFeedPlanner(cfg, nil, feedXML{ID: feedID, Timezone: "UTC"})
		planner.startupPrimerPending = false
		planner.mode = "running"
		feeds[feedID] = planner
	}
	service := &Service{
		cfg:                cfg,
		options:            Options{Lookahead: time.Minute},
		feeds:              feeds,
		preparations:       newPreparationCoordinator(routineWorkers, priorityWorkers),
		preparationStates:  make(map[string]*feedPreparationState, len(feeds)),
		maxPriorityPending: 32,
	}
	return service, feeds[feedIDs[0]]
}

func waitPreparationResult(t *testing.T, results <-chan preparationResult) preparationResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for preparation result")
		return preparationResult{}
	}
}

func waitSynthRequest(t *testing.T, requests <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("independent segment synthesis did not start concurrently")
		return nil
	}
}

func testSAMEResult(sequence string) map[string]any {
	return map[string]any{
		"header":       sequence,
		"audio_base64": base64.StdEncoding.EncodeToString([]byte{1, 0, 2, 0}),
		"sample_rate":  48000,
		"channels":     1,
	}
}
