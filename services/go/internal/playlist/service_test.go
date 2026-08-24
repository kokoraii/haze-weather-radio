package playlist

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
)

func TestPredictedStartRespectsFixedTarget(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	timelineEnd := now.Add(10 * time.Second)
	target := now.Add(30 * time.Second)

	got := predictedStart(now, timelineEnd, target)

	if !got.Equal(target) {
		t.Fatalf("predicted start = %s", got)
	}
}

func TestPredictedStartUsesTimelineWhenLate(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	timelineEnd := now.Add(35 * time.Second)
	target := now.Add(30 * time.Second)

	got := predictedStart(now, timelineEnd, target)

	if !got.Equal(timelineEnd) {
		t.Fatalf("predicted start = %s", got)
	}
}

func TestItemScheduleDurationIncludesConfiguredGap(t *testing.T) {
	planner := &feedPlanner{}
	planner.cfg.Root.Playout.Pacing.PackageGapS = 1

	got := planner.itemScheduleDuration(2500)

	if got != 3500*time.Millisecond {
		t.Fatalf("schedule duration = %s", got)
	}
}

func TestEnabledFeedsIncludesAlertOnlyStandbyFeeds(t *testing.T) {
	routine := feedXML{ID: "sk-0001"}
	routine.Playout.Routine = "true"
	alertOnly := feedXML{ID: "CAP-IT-ALL"}
	alertOnly.Playout.Routine = "false"
	alertOnly.Playout.SAME = "true"
	disabled := feedXML{ID: "silent"}
	disabled.Playout.Routine = "false"
	disabled.Playout.SAME = "false"

	got := loadedConfig{Feeds: []feedXML{routine, alertOnly, disabled}}.enabledFeeds()

	ids := []string{}
	for _, feed := range got {
		ids = append(ids, feed.ID)
	}
	if strings.Join(ids, ",") != "sk-0001,CAP-IT-ALL" {
		t.Fatalf("enabled feeds = %q", strings.Join(ids, ","))
	}
}

func TestRoutineLanguageSchedulerUsesConfiguredIntervals(t *testing.T) {
	feed := feedXML{ID: "cwxr-bi"}
	feed.Languages.Langs = []feedLangXML{{Code: "en-US"}, {Code: "fr-CA", Interval: "1"}}
	planner := &feedPlanner{feed: feed}

	got := []string{
		planner.nextRoutineLanguage(),
		planner.nextRoutineLanguage(),
		planner.nextRoutineLanguage(),
		planner.nextRoutineLanguage(),
	}

	if strings.Join(got, ",") != "en-US,fr-CA,en-US,fr-CA" {
		t.Fatalf("language rotation = %q", strings.Join(got, ","))
	}
}

func TestRoutineLanguageSchedulerKeepsSingleLanguageFeedsPrimary(t *testing.T) {
	feed := feedXML{ID: "cwxr-en"}
	feed.Languages.Langs = []feedLangXML{{Code: "en-US"}}
	planner := &feedPlanner{feed: feed}

	if got := planner.nextRoutineLanguage(); got != "en-US" {
		t.Fatalf("language = %q, want en-US", got)
	}
	if planner.routineLanguageCount != 0 {
		t.Fatalf("single language feed advanced bilingual counter")
	}
}

func TestAlertOnlyStandbyFeedDoesNotRunRoutineTick(t *testing.T) {
	dir := t.TempDir()
	feed := feedXML{ID: "CAP-IT-ALL"}
	feed.Playout.Routine = "false"
	feed.Playout.SAME = "true"
	planner := newFeedPlanner(loadedConfig{BaseDir: dir}, nil, feed)

	planner.tick(context.Background(), time.Now().Add(startupPrimerDelay), time.Minute)

	if len(planner.queue) != 0 {
		t.Fatalf("standby feed queued routine items: %#v", planner.queue)
	}
	if _, err := os.Stat(filepath.Join(dir, "runtime", "playlists", "CAP-IT-ALL.json")); !os.IsNotExist(err) {
		t.Fatalf("standby tick wrote routine state, err=%v", err)
	}
}

func TestMatchFeedsFromEventReturnsAllTargetFeeds(t *testing.T) {
	service := &Service{feeds: map[string]*feedPlanner{
		"sk-0001":    {feed: feedXML{ID: "sk-0001"}},
		"CAP-IT-ALL": {feed: feedXML{ID: "CAP-IT-ALL"}},
	}}

	matched := service.matchFeedsFromEvent(map[string]any{
		"type": "cap.alert.broadcast.requested",
		"data": map[string]any{"feed_ids": []any{"sk-0001", "CAP-IT-ALL", "sk-0001"}},
	})

	ids := []string{}
	for _, planner := range matched {
		ids = append(ids, planner.feed.ID)
	}
	if strings.Join(ids, ",") != "sk-0001,CAP-IT-ALL" {
		t.Fatalf("matched feeds = %q", strings.Join(ids, ","))
	}
}

func TestPublishAutomationDispatchStatus(t *testing.T) {
	tests := []struct {
		name      string
		status    string
		errorText string
		wantError string
	}{
		{name: "accepted", status: "accepted"},
		{name: "completed", status: "completed"},
		{name: "failed", status: "failed", errorText: "tts unavailable", wantError: "tts unavailable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()
			planner := &feedPlanner{
				feed:   feedXML{ID: "cwxr-sk01"},
				bridge: &bridgeClient{conn: clientConn},
			}
			message := make(chan map[string]any, 1)
			decodeErr := make(chan error, 1)
			go func() {
				var event map[string]any
				if err := json.NewDecoder(serverConn).Decode(&event); err != nil {
					decodeErr <- err
					return
				}
				message <- event
			}()

			planner.publishAutomationDispatchStatus(map[string]any{
				"dispatch_key":  "required_weekly_test:cwxr-sk01:1786557600000",
				"automation_id": "required_weekly_test",
				"scheduled_for": "2026-08-12T18:00:00Z",
				"alert_id":      "automation-required_weekly_test-cwxr-sk01-1786557600000",
			}, test.status, test.errorText)

			select {
			case err := <-decodeErr:
				t.Fatalf("decode acknowledgement: %v", err)
			case event := <-message:
				if got := stringAt(event, "type"); got != "automation.dispatch."+test.status {
					t.Fatalf("event type = %q", got)
				}
				data := mapAt(event, "data")
				if got := firstText(nil, data, "dispatch_key"); got != "required_weekly_test:cwxr-sk01:1786557600000" {
					t.Fatalf("dispatch key = %q", got)
				}
				if got := firstText(nil, data, "error"); got != test.wantError {
					t.Fatalf("error = %q, want %q", got, test.wantError)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for automation acknowledgement")
			}
		})
	}
}

func TestCleanupSupersededAlertQueuePartsPreservesInFlightAlert(t *testing.T) {
	dir := t.TempDir()
	queueDir := filepath.Join(dir, "runtime", "queues", "alerts")
	audioDir := filepath.Join(dir, "runtime", "audio", "alerts")
	if err := os.MkdirAll(queueDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(audioDir, 0o755); err != nil {
		t.Fatal(err)
	}
	id := safeID("000_CAP-IT-ALL_urn:test_same")
	manifestPath := filepath.Join(queueDir, id+".json")
	audioPath := filepath.Join(audioDir, id+".pcm16le")
	if err := os.WriteFile(manifestPath, []byte(`{"id":"`+id+`","status":"playing","claimed_at":"2026-06-25T01:00:00Z"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(audioPath, []byte{0, 0, 0, 0}, 0o644); err != nil {
		t.Fatal(err)
	}

	cleanupSupersededAlertQueueParts(dir, "CAP-IT-ALL", "urn:test", "")

	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("in-flight manifest was removed: %v", err)
	}
	if _, err := os.Stat(audioPath); err != nil {
		t.Fatalf("in-flight audio was removed: %v", err)
	}
}

func TestCleanupSupersededAlertQueuePartsRemovesPendingAlert(t *testing.T) {
	dir := t.TempDir()
	queueDir := filepath.Join(dir, "runtime", "queues", "alerts")
	audioDir := filepath.Join(dir, "runtime", "audio", "alerts")
	if err := os.MkdirAll(queueDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(audioDir, 0o755); err != nil {
		t.Fatal(err)
	}
	id := safeID("000_CAP-IT-ALL_urn:test_same")
	manifestPath := filepath.Join(queueDir, id+".json")
	audioPath := filepath.Join(audioDir, id+".pcm16le")
	if err := os.WriteFile(manifestPath, []byte(`{"id":"`+id+`","status":"pending"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(audioPath, []byte{0, 0, 0, 0}, 0o644); err != nil {
		t.Fatal(err)
	}

	cleanupSupersededAlertQueueParts(dir, "CAP-IT-ALL", "urn:test", "")

	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("pending manifest still exists, err=%v", err)
	}
	if _, err := os.Stat(audioPath); !os.IsNotExist(err) {
		t.Fatalf("pending audio still exists, err=%v", err)
	}
}

func TestCAPRegistryUpdateInvalidatesQueuedRoutineAlerts(t *testing.T) {
	dir := t.TempDir()
	planner := &feedPlanner{
		cfg:  loadedConfig{BaseDir: dir},
		feed: feedXML{ID: "sk-0001"},
		queue: []playlistItem{{
			QueueID:   "alerts-queued",
			FeedID:    "sk-0001",
			PackageID: "alerts",
			Title:     "Alerts",
		}, {
			QueueID:   "forecast-queued",
			FeedID:    "sk-0001",
			PackageID: "forecast",
			Title:     "Forecast",
		}},
	}
	service := &Service{feeds: map[string]*feedPlanner{"sk-0001": planner}}

	service.handleEvent(context.Background(), map[string]any{
		"type":    "cap.alert.registry.updated",
		"feed_id": "sk-0001",
		"data":    map[string]any{"feed_id": "sk-0001", "alert_id": "urn:test"},
	})

	if len(planner.queue) != 1 || planner.queue[0].PackageID != "forecast" {
		t.Fatalf("queue after invalidation = %#v", planner.queue)
	}
	if planner.current != nil {
		t.Fatalf("current was unexpectedly changed: %#v", planner.current)
	}
}

func TestCAPCancellationStopsCurrentRoutineAlert(t *testing.T) {
	dir := t.TempDir()
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	messages := make(chan map[string]any, 2)
	go func() {
		decoder := json.NewDecoder(serverConn)
		for {
			var message map[string]any
			if err := decoder.Decode(&message); err != nil {
				close(messages)
				return
			}
			messages <- message
		}
	}()

	planner := &feedPlanner{
		cfg:    loadedConfig{BaseDir: dir},
		bridge: &bridgeClient{conn: clientConn},
		feed:   feedXML{ID: "sk-0001"},
		current: &playlistItem{
			QueueID:   "alerts-current",
			FeedID:    "sk-0001",
			PackageID: "alerts",
			Title:     "Alerts",
		},
		queue: []playlistItem{{
			QueueID:   "station-id-queued",
			FeedID:    "sk-0001",
			PackageID: "station_id",
			Title:     "Station Identification",
		}},
	}
	service := &Service{feeds: map[string]*feedPlanner{"sk-0001": planner}}

	service.handleEvent(context.Background(), map[string]any{
		"type":    "cap.alert.cancelled",
		"feed_id": "sk-0001",
		"data":    map[string]any{"feed_id": "sk-0001", "alert_id": "urn:test"},
	})

	if planner.current != nil {
		t.Fatalf("current alert was not cleared: %#v", planner.current)
	}
	if len(planner.queue) != 1 || planner.queue[0].PackageID != "station_id" {
		t.Fatalf("queue after cancellation = %#v", planner.queue)
	}
	control := readPublishedTestMessage(t, messages)
	if action := actionFromPublishedControl(control); action != "flush_restart" {
		t.Fatalf("control action = %q, want flush_restart", action)
	}
}

func TestBuildProductAudioItemUsesRoutineQueueAudioPath(t *testing.T) {
	dir := t.TempDir()
	audioRel := filepath.Join("managed", "audio", "bulletins", "test.wav")
	audioPath := filepath.Join(dir, audioRel)
	writeTestWAV(t, audioPath, 48000, 1, 4800)
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir:   dir,
			OutputDir: filepath.Join(dir, "runtime", "audio", "playout"),
			Root: rootConfig{
				Playout: playoutConfig{SampleRate: 48000, Channels: 1},
			},
		},
		feed: feedXML{ID: "sk-0001"},
	}
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	item, ok, err := planner.buildProductAudioItem(context.Background(), renderedProduct{
		PackageID: "user_bulletin",
		Title:     "Operator Bulletin",
		Metadata: map[string]string{
			"content_type": "audio",
			"audio_path":   audioRel,
		},
	}, "user_bulletin", "routine", "", now, now, "routine-audio-test")

	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("audio product metadata was not recognized")
	}
	if item.Kind != "audio" || item.PackageID != "user_bulletin" || item.Source != "routine" {
		t.Fatalf("audio item = %#v", item)
	}
	if item.AudioPath != audioPath {
		t.Fatalf("audio path = %q, want %q", item.AudioPath, audioPath)
	}
	if item.DurationMS <= 0 {
		t.Fatalf("duration = %d", item.DurationMS)
	}
}

func TestBuildSegmentedProductItemIncludesAudioOpener(t *testing.T) {
	dir := t.TempDir()
	audioRel := filepath.Join("audio", "vocal", "marine.wav")
	audioPath := filepath.Join(dir, audioRel)
	openerPCM := []byte{1, 0, 2, 0, 3, 0, 4, 0}
	ttsPCM := []byte{9, 0, 8, 0, 7, 0, 6, 0}
	if err := writePCM16WAV(audioPath, openerPCM, 48000, 1); err != nil {
		t.Fatal(err)
	}
	clientConn, serverConn := net.Pipe()
	bridge := &bridgeClient{
		conn:            clientConn,
		events:          make(chan map[string]any, 16),
		pendingProducts: map[string]chan productResult{},
		pendingSynth:    map[string]chan synthResult{},
	}
	go bridge.readLoop()
	defer bridge.Close()
	go serveSegmentSynthBridge(t, serverConn, ttsPCM)
	outputDir := filepath.Join(dir, "runtime", "audio", "playlist")
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir:   dir,
			OutputDir: outputDir,
			Root: rootConfig{
				Playout: playoutConfig{SampleRate: 48000, Channels: 1},
			},
		},
		bridge: bridge,
		feed:   feedXML{ID: "cwxr-on01", Timezone: "America/Toronto"},
	}
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)

	item, ok, err := planner.buildSegmentedProductItem(context.Background(), renderedProduct{
		PackageID: "marine_reports",
		Title:     "Marine Reports",
		ReaderID:  "00",
		Language:  "en-CA",
		Segments: []renderedSegment{
			{Kind: "audio", Label: "opener", AudioPath: audioRel},
			{Kind: "package", Label: "report", Text: "The marine report for Toronto Island."},
		},
	}, "marine_reports", "routine", "", now, now, "marine-segment-test")

	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("segmented product was not recognized")
	}
	if item.Kind != "product" || item.PackageID != "marine_reports" {
		t.Fatalf("item = %#v", item)
	}
	raw, err := os.ReadFile(item.AudioPath)
	if err != nil {
		t.Fatal(err)
	}
	pcm, _, err := wavPCM16(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append(append([]byte{}, openerPCM...), silencePCM(48000, 1, 400*time.Millisecond)...), ttsPCM...)
	if !bytes.Equal(pcm, want) {
		t.Fatalf("combined PCM = %#v, want %#v", pcm, want)
	}
}

func TestBuildSegmentedProductItemPacesTextOnlySegments(t *testing.T) {
	dir := t.TempDir()
	ttsPCM := []byte{9, 0, 8, 0, 7, 0, 6, 0}
	clientConn, serverConn := net.Pipe()
	bridge := &bridgeClient{
		conn:            clientConn,
		events:          make(chan map[string]any, 16),
		pendingProducts: map[string]chan productResult{},
		pendingSynth:    map[string]chan synthResult{},
	}
	go bridge.readLoop()
	defer bridge.Close()
	go serveSegmentSynthBridge(t, serverConn, ttsPCM)
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir:   dir,
			OutputDir: filepath.Join(dir, "runtime", "audio", "playlist"),
			Root: rootConfig{
				Playout: playoutConfig{SampleRate: 48000, Channels: 1},
			},
		},
		bridge: bridge,
		feed:   feedXML{ID: "cwxr-on01", Timezone: "America/Toronto"},
	}
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)

	item, ok, err := planner.buildSegmentedProductItem(context.Background(), renderedProduct{
		PackageID: "current_conditions",
		Title:     "Current Conditions",
		ReaderID:  "00",
		Language:  "en-CA",
		Segments: []renderedSegment{
			{Kind: "opener", Label: "opener", Text: "The current weather conditions."},
			{Kind: "package", Label: "temperature", Text: "The temperature was 20 degrees."},
		},
	}, "current_conditions", "routine", "", now, now, "text-segment-test")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("text-only segmented product was not recognized")
	}
	raw, err := os.ReadFile(item.AudioPath)
	if err != nil {
		t.Fatal(err)
	}
	pcm, _, err := wavPCM16(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append(append([]byte{}, ttsPCM...), silencePCM(48000, 1, 400*time.Millisecond)...), ttsPCM...)
	if !bytes.Equal(pcm, want) {
		t.Fatalf("combined text PCM length = %d, want %d", len(pcm), len(want))
	}
}

func TestBuildProductUsesCachedCompleteAudioWhenSegmentSynthesisFails(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "runtime", "audio", "playlist")
	cachedPath := filepath.Join(outputDir, "cwxr-on01", "forecast-en-ca-cached.wav")
	if err := writeTestWAVFile(cachedPath, 48000, 1, 4800); err != nil {
		t.Fatal(err)
	}
	clientConn, serverConn := net.Pipe()
	bridge := &bridgeClient{
		conn:            clientConn,
		events:          make(chan map[string]any, 16),
		pendingProducts: map[string]chan productResult{},
		pendingSynth:    map[string]chan synthResult{},
	}
	go bridge.readLoop()
	defer bridge.Close()
	defer serverConn.Close()
	go func() {
		decoder := json.NewDecoder(serverConn)
		encoder := json.NewEncoder(serverConn)
		for {
			var message map[string]any
			if decoder.Decode(&message) != nil {
				return
			}
			data := mapAt(message, "data")
			switch message["type"] {
			case "product.render.request":
				requestID := firstText(message, data, "request_id", "subject")
				_ = encoder.Encode(map[string]any{
					"type": "product.rendered",
					"data": map[string]any{
						"request_id": requestID,
						"product": map[string]any{
							"package_id": "forecast",
							"title":      "Forecast",
							"language":   "en-CA",
							"reader_id":  "00",
							"segments": []map[string]any{
								{"kind": "opener", "text": "The forecast."},
								{"kind": "package", "text": "Cloudy today."},
							},
						},
					},
				})
			case "tts.synthesize":
				jobID := firstText(message, data, "job_id", "subject")
				_ = encoder.Encode(map[string]any{
					"type": "tts.failed",
					"data": map[string]any{"job_id": jobID, "error": "remote voice unavailable"},
				})
			}
		}
	}()
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir:   dir,
			OutputDir: outputDir,
			Root:      rootConfig{Playout: playoutConfig{SampleRate: 48000, Channels: 1}},
		},
		bridge: bridge,
		feed:   feedXML{ID: "cwxr-on01", Timezone: "America/Toronto"},
	}
	now := time.Now()

	item, err := planner.buildProduct(context.Background(), "forecast", "routine", "", now, now)
	if err != nil {
		t.Fatal(err)
	}
	if item.AudioPath != cachedPath {
		t.Fatalf("audio path = %q, want cached %q", item.AudioPath, cachedPath)
	}
	if !strings.Contains(planner.lastError, "segmented synthesis failed") {
		t.Fatalf("last error = %q", planner.lastError)
	}
}

func TestProductSegmentScratchNamesAreNotCacheCandidates(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{name: "forecast-en-ca-123-text-00.wav", want: true},
		{name: "marine-en-ca-123-audio-12.WAV", want: true},
		{name: "forecast-en-ca-complete.wav", want: false},
		{name: "text-forecast-en-ca.wav", want: false},
		{name: "forecast-en-ca-123-text-a0.wav", want: false},
	}
	for _, test := range tests {
		if got := isProductSegmentScratchName(test.name); got != test.want {
			t.Errorf("isProductSegmentScratchName(%q) = %v, want %v", test.name, got, test.want)
		}
	}
}

func TestMinuteTargetFindsNextWallClockSecond(t *testing.T) {
	from := time.Date(2026, 6, 15, 12, 4, 59, 0, time.UTC)
	until := from.Add(2 * time.Minute)

	got, ok := minuteTarget(from, until, 5)

	if !ok || got.Minute() != 5 || got.Second() != 0 {
		t.Fatalf("target = %s ok=%v", got, ok)
	}
}

func TestNextFixedEventSkipsAlreadyScheduledTarget(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	enabled := true
	disabled := false
	cfg := loadedConfig{}
	cfg.Root.Playout.StationIDSchedule.Enabled = &enabled
	cfg.Root.Playout.StationIDSchedule.Minutes = minuteList{0}
	cfg.Root.Playout.DateTimeSchedule.Enabled = &disabled

	planner := &feedPlanner{
		cfg:       cfg,
		feed:      feedXML{ID: "sk-0001", Timezone: "UTC"},
		lastFixed: map[string]time.Time{},
	}

	event, ok := planner.nextFixedEvent(now, now)
	if !ok || event.Kind != "station_id" || !event.Target.Equal(now) {
		t.Fatalf("fixed event = %#v ok=%v", event, ok)
	}
	planner.lastFixed[fixedEventKey(event)] = event.Target

	if next, ok := planner.nextFixedEvent(now, now); ok {
		t.Fatalf("duplicate fixed event = %#v", next)
	}
}

func TestNextRoutinePackageSkipsAlertsWithoutCAPState(t *testing.T) {
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir: t.TempDir(),
			Root: rootConfig{
				Playout: playoutConfig{PlaylistOrder: []string{"alerts", "forecast"}},
			},
		},
		feed: feedXML{ID: "sk-0001"},
	}

	if got := planner.nextRoutinePackage(); got != "forecast" {
		t.Fatalf("routine package = %q", got)
	}
}

func TestNextRoutinePackageIncludesAlertsWithRenderableCAPState(t *testing.T) {
	dir := t.TempDir()
	store, err := datastore.OpenSQLite(context.Background(), datastore.SQLiteConfig{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.StoreCAPArchive(context.Background(), datastore.CAPArchiveRecord{
		AlertID:      "urn:test:playlist-alert",
		FeedID:       "sk-0001",
		Bucket:       "accepted",
		Status:       "accepted",
		SentAtRaw:    "2026-06-15T15:58:00-06:00",
		UpdatedAtRaw: time.Now().UTC().Format(time.RFC3339Nano),
		ExpiresAtRaw: "2099-06-15T21:30:00-06:00",
		Event:        "thunderstorm",
		Headline:     "yellow warning - severe thunderstorm - in effect",
		RawXML:       playlistCAP(),
	}); err != nil {
		t.Fatal(err)
	}
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir: dir,
			Store:   store,
			Root: rootConfig{
				Playout: playoutConfig{PlaylistOrder: []string{"alerts", "forecast"}},
			},
		},
		feed: feedXML{ID: "sk-0001"},
	}

	if got := planner.nextRoutinePackage(); got != "alerts" {
		t.Fatalf("routine package = %q", got)
	}
}

func TestNextRoutinePackagePrunesExpiredCAPState(t *testing.T) {
	dir := t.TempDir()
	store, err := datastore.OpenSQLite(context.Background(), datastore.SQLiteConfig{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	expiredXML := strings.Replace(playlistCAP(), "2099-06-15T21:30:00-06:00", time.Now().UTC().Add(-11*time.Minute).Format(time.RFC3339), 1)
	if err := store.StoreCAPArchive(context.Background(), datastore.CAPArchiveRecord{
		AlertID:      "urn:test:playlist-alert",
		FeedID:       "sk-0001",
		Bucket:       "accepted",
		Status:       "accepted",
		SentAtRaw:    "2026-06-15T15:58:00-06:00",
		UpdatedAtRaw: time.Now().UTC().Format(time.RFC3339Nano),
		ExpiresAtRaw: time.Now().UTC().Add(-11 * time.Minute).Format(time.RFC3339),
		Event:        "thunderstorm",
		Headline:     "yellow warning - severe thunderstorm - in effect",
		RawXML:       expiredXML,
	}); err != nil {
		t.Fatal(err)
	}
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir: dir,
			Store:   store,
			Root: rootConfig{
				Playout: playoutConfig{PlaylistOrder: []string{"alerts", "forecast"}},
			},
		},
		feed: feedXML{ID: "sk-0001"},
	}

	if got := planner.nextRoutinePackage(); got != "forecast" {
		t.Fatalf("routine package = %q", got)
	}
	accepted, err := store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 0 {
		t.Fatalf("expired alert remained accepted: %#v", accepted)
	}
	expired, err := store.ListCAPArchives(context.Background(), "expired", time.Now().UTC().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].AlertID != "urn:test:playlist-alert" {
		t.Fatalf("expired alert archive rows = %#v", expired)
	}
}

func TestRoutineItemWouldCrowdFixedEvent(t *testing.T) {
	target := time.Date(2026, 6, 15, 12, 5, 0, 0, time.UTC)
	item := playlistItem{
		PredictedFinishAt: target.Add(45 * time.Second).Format(time.RFC3339Nano),
	}
	event := fixedEvent{Target: target}

	if !routineItemWouldCrowdFixed(item, event, 4) {
		t.Fatal("long routine item should defer to the fixed event")
	}

	item.PredictedFinishAt = target.Add(-time.Second).Format(time.RFC3339Nano)
	if routineItemWouldCrowdFixed(item, event, 4) {
		t.Fatal("routine item ending before the fixed event should remain schedulable")
	}
}

func TestShouldFrontLoadFixedOnlyWhenImminent(t *testing.T) {
	target := time.Date(2026, 6, 15, 12, 5, 0, 0, time.UTC)
	event := fixedEvent{Target: target}

	if shouldFrontLoadFixed(target.Add(-30*time.Second), event, 4) {
		t.Fatal("fixed event should not be front-loaded when there is still routine room")
	}

	if !shouldFrontLoadFixed(target.Add(-3*time.Second), event, 4) {
		t.Fatal("fixed event should be front-loaded inside the tolerance window")
	}

	if !shouldFrontLoadFixed(target, event, 4) {
		t.Fatal("fixed event should be front-loaded at its target time")
	}
}

func TestStaticProductFallbacks(t *testing.T) {
	planner := &feedPlanner{
		feed: feedXML{
			ID:       "sk-0001",
			Timezone: "America/Regina",
			Transmitter: struct {
				Transmitters []transmitterXML `xml:"transmitter"`
			}{
				Transmitters: []transmitterXML{
					{
						SiteName:     "Saskatoon",
						Relationship: "primary",
						FrequencyMHz: transmitterFrequencyXML{Value: "162.550"},
					},
					{
						SiteName:     "Saskatoon",
						Callsign:     "XLF322",
						Relationship: "replaces",
						Network: transmitterNetworkXML{
							Name:          "Weatheradio Canada",
							Pronunciation: "weather radio canada",
						},
						FrequencyMHz: transmitterFrequencyXML{Value: "162.550"},
					},
				},
			},
		},
	}
	planner.cfg.Root.Operator.OnAirName = []any{map[string]any{
		"text":          "Canada RadioMET",
		"pronunciation": "all hazards, canada radio met",
	}}

	stationID, err := planner.staticProduct("station_id", "", time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if stationID.Text == "" || stationID.Title != "Station Identification" || stationID.ReaderID == "" {
		t.Fatalf("station ID fallback = %#v", stationID)
	}
	for _, wanted := range []string{
		"You are listening to all hazards, canada radio met.",
		"Broadcasting from Saskatoon on a frequency of 162.550 megahertz.",
		"This station replaces former weather radio canada station XLF322 in Saskatoon.",
	} {
		if !strings.Contains(stationID.Text, wanted) {
			t.Fatalf("station ID fallback missing %q: %s", wanted, stationID.Text)
		}
	}

	dateTime, err := planner.staticProduct("date_time", "", time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if dateTime.Text == "" || dateTime.Title != "Date and Time" {
		t.Fatalf("date/time fallback = %#v", dateTime)
	}
	if !strings.Contains(dateTime.Text, "Good morning. The current time is six A.M., Central Standard Time.") {
		t.Fatalf("date/time fallback did not use legacy wording: %s", dateTime.Text)
	}
}

func TestStartupPrimerQueuesStaticItemsAndSkipsUnavailableCurrentConditions(t *testing.T) {
	dir := t.TempDir()
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	client := &bridgeClient{
		conn:            clientConn,
		events:          make(chan map[string]any, 16),
		pendingProducts: map[string]chan productResult{},
		pendingSynth:    map[string]chan synthResult{},
	}
	go client.readLoop()

	ready := make(chan map[string]any, 4)
	go serveStartupPrimerBridge(t, serverConn, ready)

	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir:   dir,
			OutputDir: filepath.Join(dir, "playlist"),
			Root: rootConfig{
				Playout: playoutConfig{SampleRate: 48000, Channels: 1},
			},
		},
		bridge:               client,
		feed:                 feedXML{ID: "sk-0001", Timezone: "UTC"},
		mode:                 "running",
		startupPrimerAt:      now.Add(startupPrimerDelay),
		startupPrimerPending: true,
		lastFixed:            map[string]time.Time{},
	}

	planner.tick(context.Background(), now, time.Minute)
	if len(planner.queue) != 0 || !planner.startupPrimerPending {
		t.Fatalf("primer queued early: queue=%#v pending=%v", planner.queue, planner.startupPrimerPending)
	}

	planner.tick(context.Background(), now.Add(startupPrimerDelay), time.Minute)
	if planner.startupPrimerPending {
		t.Fatal("startup primer remained pending")
	}
	if len(planner.queue) != 2 {
		t.Fatalf("startup queue = %#v", planner.queue)
	}
	if planner.queue[0].PackageID != "station_id" || planner.queue[1].PackageID != "date_time" {
		t.Fatalf("startup package order = %s, %s", planner.queue[0].PackageID, planner.queue[1].PackageID)
	}
	if planner.queue[0].Source != "startup" || planner.queue[1].Source != "startup" {
		t.Fatalf("startup sources = %s, %s", planner.queue[0].Source, planner.queue[1].Source)
	}

	first := readPublishedTestMessage(t, ready)
	second := readPublishedTestMessage(t, ready)
	if packageIDFromReady(first) != "station_id" || packageIDFromReady(second) != "date_time" {
		t.Fatalf("ready package order = %#v then %#v", first, second)
	}
	select {
	case extra := <-ready:
		t.Fatalf("unexpected current conditions ready item = %#v", extra)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestStartupPrimerCursorSkipsCurrentConditions(t *testing.T) {
	planner := &feedPlanner{
		cfg: loadedConfig{
			Root: rootConfig{
				Playout: playoutConfig{
					PlaylistOrder: []string{"current_conditions", "forecast", "air_quality"},
				},
			},
		},
	}

	planner.advanceRoutineCursorPast("current_conditions")

	if got := planner.nextRoutinePackage(); got != "forecast" {
		t.Fatalf("next routine package = %q, want forecast", got)
	}
}

func TestStartupProductCanUseCachedAudioWithoutTTSBridge(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "runtime", "audio", "playlist")
	cachedPath := filepath.Join(outputDir, "sk-0001", "station_id-cached.wav")
	if err := writeTestWAVFile(cachedPath, 48000, 1, 4800); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	planner := &feedPlanner{
		cfg: loadedConfig{
			BaseDir:   dir,
			OutputDir: outputDir,
			Root: rootConfig{
				Playout: playoutConfig{SampleRate: 48000, Channels: 1},
			},
		},
		feed: feedXML{ID: "sk-0001", Timezone: "UTC"},
	}

	item, err := planner.buildProduct(context.Background(), "station_id", "startup", "", now, now)
	if err != nil {
		t.Fatal(err)
	}
	if item.AudioPath != cachedPath {
		t.Fatalf("audio path = %q, want cached %q", item.AudioPath, cachedPath)
	}
	if item.PackageID != "station_id" || item.Source != "startup" {
		t.Fatalf("cached startup item = %#v", item)
	}
}

func TestPrepareLocalAlertAudioCopiesPreparedPCM(t *testing.T) {
	dir := t.TempDir()
	sourceRel := filepath.Join("runtime", "audio", "alerts", "uploads", "manual.pcm16le")
	sourcePath := filepath.Join(dir, sourceRel)
	raw := []byte{0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00}
	if err := os.MkdirAll(filepath.Dir(sourcePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourcePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(dir, "runtime", "audio", "alerts", "out.pcm16le")
	planner := &feedPlanner{
		cfg: loadedConfig{BaseDir: dir},
	}
	err := planner.prepareLocalAlertAudio(context.Background(), filepath.ToSlash(sourceRel), outputPath, 48000, 1, map[string]any{
		"audio_format":      "pcm_s16le",
		"audio_sample_rate": float64(48000),
		"audio_channels":    float64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("copied PCM = %v, want %v", got, raw)
	}
	if _, err := planner.resolveAlertAudioPath("../secret.pcm16le"); err == nil {
		t.Fatal("expected path traversal to be rejected")
	}
}

func serveStartupPrimerBridge(t *testing.T, conn net.Conn, ready chan<- map[string]any) {
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	for {
		var message map[string]any
		if err := decoder.Decode(&message); err != nil {
			close(ready)
			return
		}
		data, _ := message["data"].(map[string]any)
		switch message["type"] {
		case "tts.synthesize":
			jobID := firstText(message, data, "job_id", "subject")
			outputPath := firstText(message, data, "output_path")
			if err := writeTestWAVFile(outputPath, 48000, 1, 4800); err != nil {
				t.Errorf("write synthesized test WAV: %v", err)
				close(ready)
				return
			}
			if err := encoder.Encode(map[string]any{
				"type": "tts.synthesized",
				"data": map[string]any{"job_id": jobID, "output_path": outputPath},
			}); err != nil {
				close(ready)
				return
			}
		case "product.render.request":
			requestID := firstText(message, data, "request_id", "subject")
			if err := encoder.Encode(map[string]any{
				"type": "product.render.failed",
				"data": map[string]any{"request_id": requestID, "error": "current conditions unavailable"},
			}); err != nil {
				close(ready)
				return
			}
		case "playlist.item.ready":
			ready <- message
		}
	}
}

func serveSegmentSynthBridge(t *testing.T, conn net.Conn, pcm []byte) {
	t.Helper()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	for {
		var message map[string]any
		if err := decoder.Decode(&message); err != nil {
			return
		}
		data, _ := message["data"].(map[string]any)
		if message["type"] != "tts.synthesize" {
			continue
		}
		jobID := firstText(message, data, "job_id", "subject")
		outputPath := firstText(message, data, "output_path")
		if err := writePCM16WAV(outputPath, pcm, 48000, 1); err != nil {
			t.Errorf("write synthesized segment WAV: %v", err)
			return
		}
		if err := encoder.Encode(map[string]any{
			"type":    "tts.synthesized",
			"subject": jobID,
			"data": map[string]any{
				"job_id":      jobID,
				"output_path": outputPath,
			},
		}); err != nil {
			return
		}
	}
}

func packageIDFromReady(message map[string]any) string {
	data, _ := message["data"].(map[string]any)
	return firstText(message, data, "package_id")
}

func writeTestWAVFile(path string, sampleRate int, channels int, frames int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	dataBytes := frames * channels * 2
	raw := make([]byte, 44+dataBytes)
	copy(raw[0:4], "RIFF")
	binary.LittleEndian.PutUint32(raw[4:8], uint32(36+dataBytes))
	copy(raw[8:12], "WAVE")
	copy(raw[12:16], "fmt ")
	binary.LittleEndian.PutUint32(raw[16:20], 16)
	binary.LittleEndian.PutUint16(raw[20:22], 1)
	binary.LittleEndian.PutUint16(raw[22:24], uint16(channels))
	binary.LittleEndian.PutUint32(raw[24:28], uint32(sampleRate))
	binary.LittleEndian.PutUint32(raw[28:32], uint32(sampleRate*channels*2))
	binary.LittleEndian.PutUint16(raw[32:34], uint16(channels*2))
	binary.LittleEndian.PutUint16(raw[34:36], 16)
	copy(raw[36:40], "data")
	binary.LittleEndian.PutUint32(raw[40:44], uint32(dataBytes))
	return os.WriteFile(path, raw, 0o600)
}

func TestInterruptedItemRemainsCurrentUntilRestartAndCompletion(t *testing.T) {
	planner := &feedPlanner{
		queue: []playlistItem{{
			QueueID:   "routine-1",
			PackageID: "forecast",
			Title:     "Forecast",
			Status:    "queued",
		}},
	}

	planner.markStarted("routine-1")
	if planner.current == nil || planner.current.Status != "playing" {
		t.Fatalf("current after start = %#v", planner.current)
	}

	planner.markInterrupted("routine-1")
	if planner.current == nil || planner.current.Status != "interrupted" {
		t.Fatalf("current after interrupt = %#v", planner.current)
	}

	planner.markStarted("routine-1")
	if planner.current == nil || planner.current.Status != "playing" {
		t.Fatalf("current after restart = %#v", planner.current)
	}

	planner.markCompleted("routine-1")
	if planner.current != nil {
		t.Fatalf("current after completion = %#v", planner.current)
	}
}

func TestPriorityWindowPublishesRoutinePauseAndResume(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	messages := make(chan map[string]any, 4)
	go func() {
		decoder := json.NewDecoder(serverConn)
		for {
			var message map[string]any
			if err := decoder.Decode(&message); err != nil {
				close(messages)
				return
			}
			messages <- message
		}
	}()

	planner := &feedPlanner{
		bridge: &bridgeClient{conn: clientConn},
		feed:   feedXML{ID: "sk-0001"},
		mode:   "running",
	}

	planner.markPriorityStarted()
	planner.markPriorityStarted()
	planner.markPriorityCompleted()
	planner.markPriorityCompleted()

	first := readPublishedTestMessage(t, messages)
	second := readPublishedTestMessage(t, messages)
	if actionFromPublishedControl(first) != "pause" {
		t.Fatalf("first control = %#v", first)
	}
	if actionFromPublishedControl(second) != "resume" {
		t.Fatalf("second control = %#v", second)
	}
	select {
	case extra := <-messages:
		t.Fatalf("unexpected extra control = %#v", extra)
	case <-time.After(25 * time.Millisecond):
	}
}

func readPublishedTestMessage(t *testing.T, messages <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case message, ok := <-messages:
		if !ok {
			t.Fatal("published message stream closed")
		}
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published message")
	}
	return nil
}

func actionFromPublishedControl(message map[string]any) string {
	data, _ := message["data"].(map[string]any)
	return firstText(message, data, "action")
}

func TestIsPlayoutReadyEvent(t *testing.T) {
	if !isPlayoutReadyEvent(map[string]any{
		"type":   "service.ready",
		"source": "haze-playout",
	}) {
		t.Fatal("playout ready event by source should be recognized")
	}

	if !isPlayoutReadyEvent(map[string]any{
		"type": "service.ready",
		"data": map[string]any{"service": "haze-playout"},
	}) {
		t.Fatal("playout ready event by service name should be recognized")
	}

	if isPlayoutReadyEvent(map[string]any{
		"type":   "service.ready",
		"source": "haze-playlist",
	}) {
		t.Fatal("non-playout service should not trigger queue replay")
	}
}

func TestPlaylistLifecycleEventClassification(t *testing.T) {
	if !playlistLifecycleEvent("playout.started") || !playlistLifecycleEvent("alert.playout.completed") {
		t.Fatal("playout lifecycle events must be retained under bridge backpressure")
	}
	if playlistLifecycleEvent("cap.alert.received") {
		t.Fatal("ordinary CAP traffic should not be classified as playlist lifecycle")
	}
}

func TestPendingItemsReplayAfterInterval(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	messages := make(chan map[string]any, 4)
	go func() {
		decoder := json.NewDecoder(serverConn)
		for {
			var message map[string]any
			if err := decoder.Decode(&message); err != nil {
				close(messages)
				return
			}
			messages <- message
		}
	}()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	planner := &feedPlanner{
		bridge: &bridgeClient{conn: clientConn},
		feed:   feedXML{ID: "sk-0001"},
		queue: []playlistItem{{
			QueueID:   "routine-1",
			FeedID:    "sk-0001",
			PackageID: "forecast",
			Title:     "Forecast",
		}},
		lastPendingReplayAt: now.Add(-pendingReplayInterval),
	}

	planner.replayPendingItemsIfDue(now)
	first := readPublishedTestMessage(t, messages)
	if packageIDFromReady(first) != "forecast" {
		t.Fatalf("replayed package = %#v", first)
	}

	planner.replayPendingItemsIfDue(time.Now())
	select {
	case extra := <-messages:
		t.Fatalf("unexpected replay before interval = %#v", extra)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestPendingReplayHeartbeatSkipsCurrentItem(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	messages := make(chan map[string]any, 4)
	go func() {
		decoder := json.NewDecoder(serverConn)
		for {
			var message map[string]any
			if err := decoder.Decode(&message); err != nil {
				close(messages)
				return
			}
			messages <- message
		}
	}()

	now := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	planner := &feedPlanner{
		bridge: &bridgeClient{conn: clientConn},
		feed:   feedXML{ID: "sk-0001"},
		current: &playlistItem{
			QueueID:   "current-1",
			FeedID:    "sk-0001",
			PackageID: "station_id",
			Title:     "Station Identification",
		},
		queue: []playlistItem{{
			QueueID:   "queued-1",
			FeedID:    "sk-0001",
			PackageID: "forecast",
			Title:     "Forecast",
		}},
		lastPendingReplayAt: now.Add(-pendingReplayInterval),
	}

	planner.replayPendingItemsIfDue(now)
	first := readPublishedTestMessage(t, messages)
	if packageIDFromReady(first) != "forecast" {
		t.Fatalf("heartbeat replayed wrong item = %#v", first)
	}
	select {
	case extra := <-messages:
		t.Fatalf("heartbeat replayed current item = %#v", extra)
	case <-time.After(25 * time.Millisecond):
	}
	if got := planner.queuedCount(); got != 2 {
		t.Fatalf("queued count = %d, want current plus queued", got)
	}
}

func TestAlertTextFromDataPrefersRenderedCAPScript(t *testing.T) {
	text := alertTextFromData(map[string]any{
		"title":       "Severe Thunderstorm Warning",
		"alert_text":  "Environment Canada has issued a Severe Thunderstorm Warning.",
		"description": "Fallback description.",
	})

	if text != "Environment Canada has issued a Severe Thunderstorm Warning." {
		t.Fatalf("alert text = %q", text)
	}
}

func TestAlertTextFromDataFallsBackToDescriptionAndInstruction(t *testing.T) {
	text := alertTextFromData(map[string]any{
		"title":       "Severe Thunderstorm Warning",
		"description": "Large hail is possible.",
		"instruction": "Take shelter immediately.",
	})

	for _, wanted := range []string{"Severe Thunderstorm Warning", "Large hail is possible.", "Take shelter immediately."} {
		if !strings.Contains(text, wanted) {
			t.Fatalf("alert text missing %q in %q", wanted, text)
		}
	}
}

func TestNWSAlertTextUsesSameTranslationWithCAPProse(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "managed", "csv"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "managed", "sameMapping.json"), []byte(`{"eas":{"SPS":"Special Weather Statement"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "managed", "csv", "NWS_ZONE_COUNTY_CORRELATION.csv"), []byte("STATE|ZONE_CODE|CWA_ID|ZONE_NAME|STATE+ZONE|COUNTY_NAME|FIPS/SAME|TIMEZONE|FE_AREA|LAT|LON\nAL|001|BMX|Autauga|ALC001|Autauga|001001|C|se|32.5364|-86.6445\nAL|003|MOB|Baldwin|ALC003|Baldwin|001003|C|se|30.6592|-87.7461\nAL|005|BMX|Barbour|ALC005|Barbour|001005|C|se|31.8707|-85.4055\nAL|007|BMX|Bibb|ALC007|Bibb|001007|C|se|33.0159|-87.1271\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	planner := &feedPlanner{cfg: loadedConfig{BaseDir: dir}, feed: feedXML{ID: "CAP-IT-ALL"}}
	data := map[string]any{
		"cap_source":           "nws",
		"same_event":           "SPS",
		"same_event_name":      "Special Weather Statement",
		"same_originator":      "WXR",
		"same_weather_service": "The National Weather Service",
		"same_locations":       []any{"001001", "001003", "001005", "001007"},
		"same_callsign":        "CAP-IT-ALL",
		"same_translation":     "The National Weather Service has issued a Special Weather Statement for the following areas: Unknown Location (001001). (CAP-IT-ALL).",
		"same_begins_at":       "2026-06-22T15:01:00-05:00",
		"same_expires_at":      "2026-06-22T16:01:00-05:00",
		"alert_text":           "CAP text that should not be spoken.",
		"description":          "At 500 PM EDT, National Weather Service meteorologists were tracking a strong thunderstorm.",
		"instruction":          "Seek shelter in a safe building until the storm passes.",
		"mimic_endec":          "SAGE",
	}
	text := planner.alertTextFromData(data)

	want := "The National Weather Service has issued a Special Weather Statement for the following areas: Autauga, AL; Baldwin, AL; Barbour, AL; and Bibb, AL. Beginning at 3:01 PM and ending at 4:01 PM on June 22nd, 2026. At 500 PM EDT, National Weather Service meteorologists were tracking a strong thunderstorm. Seek shelter in a safe building until the storm passes."
	if text != want {
		t.Fatalf("alert text = %q, want %q", text, want)
	}
	if strings.Contains(text, "CAP text") {
		t.Fatalf("NWS alert text included raw CAP fallback text: %q", text)
	}
	if strings.Contains(text, "CAP-IT-ALL") {
		t.Fatalf("speech alert text included source label: %q", text)
	}

	bannerText := planner.bannerTextFromData(data, text)
	if !strings.Contains(bannerText, "(CAP-IT-ALL).") {
		t.Fatalf("banner text omitted source label: %q", bannerText)
	}
}

func TestNWSAlertPacketUsesCanonicalBodyInsteadOfGeneratedText(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "managed", "csv"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "managed", "sameMapping.json"), []byte(`{"eas":{"SVR":"Severe Thunderstorm Warning"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "managed", "csv", "NWS_ZONE_COUNTY_CORRELATION.csv"), []byte("STATE|ZONE_CODE|CWA_ID|ZONE_NAME|STATE+ZONE|COUNTY_NAME|FIPS/SAME|TIMEZONE|FE_AREA|LAT|LON\nCO|075|BOU|Logan|COC075|Logan|008075|M|nw|40.72|-103.09\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	planner := &feedPlanner{cfg: loadedConfig{BaseDir: dir}, feed: feedXML{ID: "CAP-IT-ALL"}}
	packet := alertmodel.Packet{
		ID:          "urn:test:nws",
		Source:      "nws",
		FeedID:      "CAP-IT-ALL",
		MessageType: "Alert",
		Content: alertmodel.Content{
			Headline:    "Severe Thunderstorm Warning",
			Event:       "Severe Thunderstorm Warning",
			Severity:    "Severe",
			Description: "At 400 PM MDT, a severe thunderstorm was located east of Sterling.\n\nHAZARD...Two inch hail.",
			Instruction: "Anyone outdoors should move to shelter inside a well-built structure.",
		},
		Timing: alertmodel.Timing{
			SentAt:    "2026-06-22T22:00:00Z",
			ExpiresAt: "2026-06-22T22:30:00Z",
		},
		SAME: &alertmodel.SAME{
			Include:        true,
			Event:          "SVR",
			EventName:      "Severe Thunderstorm Warning",
			Originator:     "WXR",
			WeatherService: "The National Weather Service",
			Locations:      []string{"008075"},
			BeginsAt:       "2026-06-22T22:00:00Z",
			ExpiresAt:      "2026-06-22T22:30:00Z",
			Callsign:       "CAP-IT-ALL",
		},
		Presentation: alertmodel.Presentation{
			SpeechText: "SVSBOU The National Weather Service in Denver has issued a generated wrapper that should not be spoken.",
		},
	}
	text := planner.alertTextFromData(map[string]any{"alert_packet": packet})

	if strings.Contains(text, "SVSBOU") || strings.Contains(text, "generated wrapper") {
		t.Fatalf("NWS alert packet used generated text instead of canonical body: %q", text)
	}
	for _, wanted := range []string{
		"The National Weather Service has issued a Severe Thunderstorm Warning",
		"Logan, CO",
		"At 400 PM MDT, a severe thunderstorm was located east of Sterling.",
		"HAZARD. Two inch hail.",
		"Anyone outdoors should move to shelter",
	} {
		if !strings.Contains(text, wanted) {
			t.Fatalf("NWS alert packet text missing %q: %q", wanted, text)
		}
	}
}

func TestNWSAlertTextStripsDuplicatedProductPreamble(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "managed", "csv"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "managed", "sameMapping.json"), []byte(`{"eas":{"SVR":"Severe Thunderstorm Warning"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "managed", "csv", "NWS_ZONE_COUNTY_CORRELATION.csv"), []byte("STATE|ZONE_CODE|CWA_ID|ZONE_NAME|STATE+ZONE|COUNTY_NAME|FIPS/SAME|TIMEZONE|FE_AREA|LAT|LON\nVA|085|AKQ|Hanover|VAC085|Hanover|051085|E|se|37.7600|-77.4900\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	planner := &feedPlanner{cfg: loadedConfig{BaseDir: dir}, feed: feedXML{ID: "CAP-IT-ALL"}}
	text := planner.alertTextFromData(map[string]any{
		"cap_source":           "nws",
		"same_event":           "SVR",
		"same_event_name":      "Severe Thunderstorm Warning",
		"same_originator":      "WXR",
		"same_weather_service": "The National Weather Service",
		"same_locations":       []any{"051085"},
		"same_callsign":        "CAP-IT-ALL",
		"same_begins_at":       "2026-06-22T21:58:00Z",
		"same_expires_at":      "2026-06-22T22:45:00Z",
		"description": `SVRAKQ The National Weather Service in Wakefield has issued a

* Severe Thunderstorm Warning for...
Hanover County in central Virginia...

* Until 645 PM EDT.

* At 557 PM EDT, a severe thunderstorm was located near Gum Spring, moving east at 40 mph.

HAZARD...60 mph wind gusts.

SOURCE...Radar indicated.

IMPACT...Expect damage to trees and powerlines.

* Locations impacted include...
Montpelier and Ashland.`,
		"instruction": "For your protection move to an interior room on the lowest floor of a building. Please report severe weather by calling 757-899-2415.",
		"mimic_endec": "SAGE",
	})

	for _, forbidden := range []string{"SVRAKQ", "has issued a * Severe Thunderstorm Warning", "*", "...", "Please report severe weather", "CAP-IT-ALL"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("NWS alert text still contains %q: %q", forbidden, text)
		}
	}
	for _, wanted := range []string{
		"The National Weather Service has issued a Severe Thunderstorm Warning for the following areas: Hanover, VA.",
		"At 557 PM EDT, a severe thunderstorm was located near Gum Spring",
		"HAZARD. 60 mph wind gusts.",
		"SOURCE. Radar indicated.",
		"IMPACT. Expect damage to trees and powerlines.",
		"Locations impacted include. Montpelier and Ashland.",
		"For your protection move to an interior room",
	} {
		if !strings.Contains(text, wanted) {
			t.Fatalf("NWS alert text missing %q: %q", wanted, text)
		}
	}
}

func TestBannerTextFromDataUsesSameIntroEvenWhenSameAudioDisabled(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "managed", "csv"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "managed", "sameMapping.json"), []byte(`{"eas":{"DMO":"Practice/demo Warning"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "managed", "csv", "NWS_ZONE_COUNTY_CORRELATION.csv"), []byte("FIPS|COUNTYNAME\n001217|Talladega, AL\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	planner := &feedPlanner{cfg: loadedConfig{BaseDir: dir}, feed: feedXML{ID: "CAP-IT-ALL"}}
	text := planner.bannerTextFromData(map[string]any{
		"include_same":    false,
		"same_event":      "DMO",
		"same_originator": "WXR",
		"same_locations":  []any{"001217"},
		"same_duration":   "0015",
		"same_callsign":   "meowraii",
		"alert_text":      "Custom text.",
		"mimic_endec":     "SAGE",
	}, "Custom text.")

	if !strings.Contains(text, "Environment Canada has issued a Practice/demo Warning") || !strings.Contains(text, "Custom text.") {
		t.Fatalf("banner text = %q", text)
	}
}

func TestIncludeSameAlertRequiresExplicitOptIn(t *testing.T) {
	if includeSameAlert(map[string]any{}) {
		t.Fatal("missing include_same should not default to SAME tones")
	}
	if includeSameAlert(map[string]any{"include_same": "false"}) {
		t.Fatal("include_same=false should disable SAME tones")
	}
	if !includeSameAlert(map[string]any{"include_same": true}) {
		t.Fatal("include_same=true should enable SAME tones")
	}
}

func TestAlertAttentionToneEnabledIsIndependentOfSame(t *testing.T) {
	if !alertAttentionToneEnabled(map[string]any{"same_tone": "WXR", "include_same": false}) {
		t.Fatal("selected attention tone should be enabled even when SAME is disabled")
	}
	if alertAttentionToneEnabled(map[string]any{"same_tone": "NONE", "include_same": false}) {
		t.Fatal("NONE tone should disable attention tone")
	}
}

func TestPriorityAlertRequestStaleAllowsForcedArchiveBroadcasts(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	expired := map[string]any{
		"title":            "Severe Thunderstorm Warning",
		"same_event":       "SVR",
		"alert_sent_at":    now.Add(-2 * time.Hour).Format(time.RFC3339),
		"alert_expires_at": now.Add(-time.Hour).Format(time.RFC3339),
	}
	if !priorityAlertRequestStale(expired, now) {
		t.Fatal("ordinary expired alert should be stale")
	}
	forced := map[string]any{
		"title":            "Severe Thunderstorm Warning",
		"same_event":       "SVR",
		"alert_sent_at":    now.Add(-2 * time.Hour).Format(time.RFC3339),
		"alert_expires_at": now.Add(-time.Hour).Format(time.RFC3339),
		"force_broadcast":  true,
	}
	if priorityAlertRequestStale(forced, now) {
		t.Fatal("forced archive broadcast should bypass sent/expiry staleness")
	}
	cancel := map[string]any{
		"title":           "Severe Thunderstorm Warning",
		"message_type":    "Cancel",
		"force_broadcast": true,
	}
	if !priorityAlertRequestStale(cancel, now) {
		t.Fatal("forced cancellation should still be stale")
	}
}

func TestCombineAttentionAlertAudioPrependsToneAndVoice(t *testing.T) {
	dir := t.TempDir()
	voicePath := filepath.Join(dir, "voice.pcm16le")
	outputPath := filepath.Join(dir, "alert.pcm16le")
	voice := []byte{1, 2, 3, 4}
	tone := []byte{5, 6}
	if err := os.WriteFile(voicePath, voice, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := combineAttentionAlertAudio(outputPath, tone, voicePath, 48000, 1); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	expectedLen := len(tone) + len(silencePCM(48000, 1, time.Second)) + len(voice)
	if len(raw) != expectedLen {
		t.Fatalf("combined len = %d, want %d", len(raw), expectedLen)
	}
	if !bytes.Equal(raw[:len(tone)], tone) || !bytes.Equal(raw[len(raw)-len(voice):], voice) {
		t.Fatalf("combined audio did not preserve tone and voice: %#v", raw)
	}
}

func TestCombineConfiguredLeadAlertAudioPlacesLeadsAroundSAME(t *testing.T) {
	dir := t.TempDir()
	voicePath := filepath.Join(dir, "voice.pcm16le")
	outputPath := filepath.Join(dir, "alert.pcm16le")
	preRoll := []byte{1, 0}
	header := []byte{2, 0}
	voice := []byte{3, 0}
	eom := []byte{4, 0}
	postRoll := []byte{5, 0}
	if err := os.WriteFile(voicePath, voice, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := combineConfiguredLeadAlertAudio(outputPath, preRoll, header, voicePath, eom, postRoll, 1, 1); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	silence := []byte{0, 0}
	want := append([]byte{}, preRoll...)
	want = append(want, silence...)
	want = append(want, header...)
	want = append(want, silence...)
	want = append(want, voice...)
	want = append(want, eom...)
	want = append(want, silence...)
	want = append(want, postRoll...)
	if !bytes.Equal(raw, want) {
		t.Fatalf("configured lead sequence = %#v, want %#v", raw, want)
	}
}

func TestPriorityAlertRequestStaleUsesAlertSentTime(t *testing.T) {
	now := time.Date(2026, 6, 16, 23, 40, 0, 0, time.UTC)
	if priorityAlertRequestStale(map[string]any{
		"alert_sent_at": "2026-06-16T23:11:00Z",
		"same_event":    "SVR",
		"title":         "orange warning - severe thunderstorm - in effect",
	}, now) {
		t.Fatal("fresh SVR should not be stale")
	}
	if !priorityAlertRequestStale(map[string]any{
		"alert_sent_at": "2026-06-16T23:09:00Z",
		"same_event":    "SVR",
		"title":         "orange warning - severe thunderstorm - in effect",
	}, now) {
		t.Fatal("old SVR should be stale")
	}
	if priorityAlertRequestStale(map[string]any{
		"alert_sent_at": "2026-06-16T22:41:00Z",
		"same_event":    "SVA",
	}, now) {
		t.Fatal("fresh non-SVR/TOR should not be stale before 60 minutes")
	}
	if !priorityAlertRequestStale(map[string]any{
		"alert_sent_at": "2026-06-16T22:39:00Z",
		"same_event":    "SVA",
	}, now) {
		t.Fatal("old non-SVR/TOR should be stale after 60 minutes")
	}
}

func TestPriorityAlertRequestStaleSuppressesCancellations(t *testing.T) {
	now := time.Date(2026, 6, 16, 23, 40, 0, 0, time.UTC)
	if !priorityAlertRequestStale(map[string]any{"message_type": "Cancel"}, now) {
		t.Fatal("CAP cancellations should be stale for priority queueing")
	}
	if !priorityAlertRequestStale(map[string]any{"title": "yellow warning - severe thunderstorm - ended"}, now) {
		t.Fatal("ended headlines should be stale for priority queueing")
	}
}

func TestMixedActiveAndEndedCAPIsRenderableForRoutine(t *testing.T) {
	activeRaw := playlistCAP()
	endedRaw := strings.ReplaceAll(activeRaw, "<responseType>Monitor</responseType>", "<responseType>AllClear</responseType>")
	endedRaw = strings.ReplaceAll(endedRaw, "yellow warning - severe thunderstorm - in effect", "yellow warning - severe thunderstorm - ended")
	infoStart := strings.Index(endedRaw, "<info>")
	infoEnd := strings.LastIndex(endedRaw, "</info>")
	if infoStart < 0 || infoEnd < 0 {
		t.Fatal("ended fixture info block not found")
	}
	mixedRaw := strings.Replace(activeRaw, "</alert>", endedRaw[infoStart:infoEnd+len("</info>")]+"\n</alert>", 1)
	alert, err := capmodel.ParseCAP([]byte(mixedRaw))
	if err != nil {
		t.Fatal(err)
	}

	if isExplicitCAPEnd(alert) {
		t.Fatal("mixed active and ended CAP update was treated as a global cancellation")
	}
	if !isRenderableCAPEntry(capRegistryEntry{UpdatedAt: time.Now().UTC(), Alert: alert}, time.Now().UTC()) {
		t.Fatal("mixed active and ended CAP update should remain routine-renderable")
	}
}

func TestCancelPriorityAlertsRemovesPendingQueueParts(t *testing.T) {
	dir := t.TempDir()
	queueDir := filepath.Join(dir, "runtime", "queues", "alerts")
	audioDir := filepath.Join(dir, "runtime", "audio", "alerts")
	if err := os.MkdirAll(queueDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(audioDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{
		"000_sk-0001_urnoid2.49.0.1.124.TEST.2026_same",
		"001_sk-0001_urnoid2.49.0.1.124.TEST.2026_cap",
	} {
		if err := os.WriteFile(filepath.Join(queueDir, id+".json"), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(audioDir, id+".pcm16le"), []byte("audio"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	planner := &feedPlanner{cfg: loadedConfig{BaseDir: dir}, feed: feedXML{ID: "sk-0001"}}

	planner.cancelPriorityAlerts(map[string]any{
		"alert_ids": []any{"urn:oid:2.49.0.1.124.TEST.2026"},
	})

	entries, err := os.ReadDir(queueDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("queue entries remain: %#v", entries)
	}
	audioEntries, err := os.ReadDir(audioDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(audioEntries) != 0 {
		t.Fatalf("audio entries remain: %#v", audioEntries)
	}
}

func playlistCAP() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>urn:test:playlist-alert</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-06-15T15:58:00-06:00</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>thunderstorm</event>
    <responseType>Monitor</responseType>
    <urgency>Immediate</urgency>
    <severity>Moderate</severity>
    <certainty>Likely</certainty>
    <effective>2026-06-15T15:58:00-06:00</effective>
    <expires>2099-06-15T21:30:00-06:00</expires>
    <senderName>Environment Canada</senderName>
    <headline>yellow warning - severe thunderstorm - in effect</headline>
    <description>Test alert.</description>
  </info>
</alert>`
}

func writeTestWAV(t *testing.T, path string, sampleRate int, channels int, frames int) {
	t.Helper()
	if err := writeTestWAVFile(path, sampleRate, channels, frames); err != nil {
		t.Fatal(err)
	}
}
