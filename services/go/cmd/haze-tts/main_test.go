package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/tts"
)

type fakeProvider struct {
	id                  string
	audio               tts.Audio
	err                 error
	listErr             error
	listWait            <-chan struct{}
	listStarted         chan<- struct{}
	waitForCancellation bool
	mu                  sync.Mutex
	listRequests        int
	requests            []tts.Request
}

func (p *fakeProvider) ID() string { return p.id }

func (p *fakeProvider) ListVoices(ctx context.Context) ([]tts.Voice, error) {
	p.mu.Lock()
	p.listRequests++
	err := p.listErr
	wait := p.listWait
	started := p.listStarted
	p.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if wait != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait:
		}
	}
	return nil, err
}

func (p *fakeProvider) Synthesize(ctx context.Context, req tts.Request) (tts.Audio, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	if p.waitForCancellation {
		<-ctx.Done()
		return tts.Audio{}, ctx.Err()
	}
	return p.audio, p.err
}

func (p *fakeProvider) requestAt(index int) tts.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.requests[index]
}

func (p *fakeProvider) requestCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.requests)
}

func (p *fakeProvider) listRequestCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.listRequests
}

func (p *fakeProvider) setListError(err error) {
	p.mu.Lock()
	p.listErr = err
	p.mu.Unlock()
}

func TestHandleSynthesisJobDefaultsToWAV(t *testing.T) {
	provider := &fakeProvider{id: "piper", audio: tts.Audio{Format: tts.FormatWAV, Data: []byte("wav")}}
	state := &serviceState{
		cfg:          serviceConfig{Timeout: time.Second},
		providers:    map[string]tts.Provider{"piper": provider},
		dictionaries: map[string]dictionaryResult{},
	}
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	outputPath := filepath.Join(t.TempDir(), "out.wav")

	go handleSynthesisJob(context.Background(), conn, state, map[string]any{
		"type": "tts.synthesize",
		"data": map[string]any{
			"job_id":      "job-1",
			"provider":    "piper",
			"text":        "hello",
			"output_path": outputPath,
		},
	})

	var event map[string]any
	if err := json.NewDecoder(peer).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event["type"] != "tts.synthesized" {
		t.Fatalf("event type = %v", event["type"])
	}
	data := event["data"].(map[string]any)
	if data["format"] != string(tts.FormatWAV) {
		t.Fatalf("format = %v", data["format"])
	}
	if got := provider.requestAt(0).OutputFormat; got != tts.FormatWAV {
		t.Fatalf("request output format = %q", got)
	}
	if raw, err := os.ReadFile(outputPath); err != nil || string(raw) != "wav" {
		t.Fatalf("output = %q err=%v", raw, err)
	}
}

func TestHandleSynthesisJobPublishesPCMMetadata(t *testing.T) {
	provider := &fakeProvider{id: "piper", audio: tts.Audio{
		Format:     tts.FormatPCM16LE,
		SampleRate: 22050,
		Channels:   1,
		Data:       []byte{0, 0, 1, 0},
	}}
	state := &serviceState{
		cfg:          serviceConfig{Timeout: time.Second},
		providers:    map[string]tts.Provider{"piper": provider},
		dictionaries: map[string]dictionaryResult{},
	}
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	outputPath := filepath.Join(t.TempDir(), "out.pcm16")

	go handleSynthesisJob(context.Background(), conn, state, map[string]any{
		"type": "tts.synthesize",
		"data": map[string]any{
			"job_id":        "job-2",
			"provider":      "piper",
			"text":          "hello",
			"output_path":   outputPath,
			"output_format": "pcm_s16le",
		},
	})

	var event map[string]any
	if err := json.NewDecoder(peer).Decode(&event); err != nil {
		t.Fatal(err)
	}
	data := event["data"].(map[string]any)
	if data["format"] != string(tts.FormatPCM16LE) || int(data["sample_rate"].(float64)) != 22050 || int(data["channels"].(float64)) != 1 {
		t.Fatalf("pcm metadata = %#v", data)
	}
	if got := provider.requestAt(0).OutputFormat; got != tts.FormatPCM16LE {
		t.Fatalf("request output format = %q", got)
	}
	if raw, err := os.ReadFile(outputPath); err != nil || len(raw) != 4 {
		t.Fatalf("output bytes = %d err=%v", len(raw), err)
	}
}

func TestHandleSynthesisJobDropsExpiredQueuedRequest(t *testing.T) {
	provider := &fakeProvider{id: "piper", audio: tts.Audio{Format: tts.FormatWAV, Data: []byte("wav")}}
	state := &serviceState{
		cfg:          serviceConfig{Timeout: time.Second},
		providers:    map[string]tts.Provider{"piper": provider},
		dictionaries: map[string]dictionaryResult{},
	}
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	outputPath := filepath.Join(t.TempDir(), "expired.wav")

	go handleSynthesisJob(context.Background(), conn, state, map[string]any{
		"type": "tts.synthesize",
		"data": map[string]any{
			"job_id":      "expired-job",
			"provider":    "piper",
			"text":        "stale weather",
			"output_path": outputPath,
			"deadline_at": time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano),
		},
	})

	var event map[string]any
	if err := json.NewDecoder(peer).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event["type"] != "tts.failed" {
		t.Fatalf("event type = %v, want tts.failed", event["type"])
	}
	data := event["data"].(map[string]any)
	if !strings.Contains(data["error"].(string), "expired") {
		t.Fatalf("error = %v", data["error"])
	}
	if got := provider.requestCount(); got != 0 {
		t.Fatalf("expired request reached provider %d time(s)", got)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("expired request wrote output, err=%v", err)
	}
}

func TestSynthesizeWithReaderBackupUsesConfiguredVoice(t *testing.T) {
	primary := &fakeProvider{id: "speakyapi", err: tts.ErrProviderUnavailable}
	backup := &fakeProvider{id: "piper", audio: tts.Audio{Format: tts.FormatWAV, Data: []byte("backup")}}
	audio, provider, voiceID, backupUsed, err := synthesizeWithReaderBackup(
		context.Background(),
		map[string]tts.Provider{"speakyapi": primary, "piper": backup},
		"speakyapi",
		tts.Request{Text: "weather", VoiceID: "RadioMET Tom", Language: "en-US"},
		&tts.ReaderBackup{Provider: "piper", VoiceID: "en_US-hfc_male-medium"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if provider.ID() != "piper" || !backupUsed || voiceID != "en_US-hfc_male-medium" || string(audio.Data) != "backup" {
		t.Fatalf("fallback result provider=%v used=%v voice=%q audio=%q", provider.ID(), backupUsed, voiceID, audio.Data)
	}
	if got := backup.requestAt(0).VoiceID; got != "en_US-hfc_male-medium" {
		t.Fatalf("backup voice = %q", got)
	}
}

func TestSynthesizeWithReaderBackupDoesNotReplaceSlowHealthyPrimary(t *testing.T) {
	primary := &fakeProvider{id: "speakyapi", waitForCancellation: true}
	backup := &fakeProvider{id: "piper", audio: tts.Audio{Format: tts.FormatWAV, Data: []byte("backup")}}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, provider, _, backupUsed, err := synthesizeWithReaderBackup(
		ctx,
		map[string]tts.Provider{"speakyapi": primary, "piper": backup},
		"speakyapi",
		tts.Request{Text: "weather", VoiceID: "RadioMET Tom", Language: "en-US"},
		&tts.ReaderBackup{ReaderID: "000", Provider: "piper", VoiceID: "en_US-hfc_male-medium"},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want deadline exceeded", err)
	}
	if provider != nil || backupUsed {
		t.Fatalf("provider=%v backup_used=%v", provider, backupUsed)
	}
	if got := backup.requestCount(); got != 0 {
		t.Fatalf("slow primary triggered backup %d time(s)", got)
	}
}

func TestSynthesizeWithoutBackupRetainsParentDeadline(t *testing.T) {
	primary := &fakeProvider{id: "speakyapi", waitForCancellation: true}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, _, _, backupUsed, err := synthesizeWithReaderBackup(
		ctx,
		map[string]tts.Provider{"speakyapi": primary},
		"speakyapi",
		tts.Request{Text: "weather"},
		nil,
	)
	if err == nil || backupUsed {
		t.Fatalf("err=%v backup_used=%v", err, backupUsed)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("ordinary reader did not retain its parent deadline: %v", ctx.Err())
	}
}

func TestReaderBackupEligibility(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "provider unavailable", err: tts.ErrProviderUnavailable, want: true},
		{name: "ordinary synthesis failure", err: errors.New("invalid request"), want: false},
		{name: "provider deadline", err: fmt.Errorf("%w: %w", tts.ErrProviderUnavailable, context.DeadlineExceeded), want: false},
		{name: "provider cancellation", err: fmt.Errorf("%w: %w", tts.ErrProviderUnavailable, context.Canceled), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := readerBackupEligible(context.Background(), test.err); got != test.want {
				t.Fatalf("readerBackupEligible()=%v, want %v", got, test.want)
			}
		})
	}

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if readerBackupEligible(expired, tts.ErrProviderUnavailable) {
		t.Fatal("cancelled request allowed reader backup")
	}
}

func TestResolveVoiceRoutePinsFallbackWhenSpeakyAPIIsUnavailable(t *testing.T) {
	primary := &fakeProvider{id: "speakyapi", listErr: tts.ErrProviderUnavailable}
	state := &serviceState{
		ctx:       context.Background(),
		providers: map[string]tts.Provider{"speakyapi": primary},
	}
	backup := &tts.ReaderBackup{Provider: "piper", VoiceID: "en_US-hfc_male-medium"}
	decision, locked, err := state.resolveVoiceRoute(context.Background(), "product-1", "speakyapi", "RadioMET Tom", backup)
	if err != nil {
		t.Fatal(err)
	}
	if !locked || !decision.backupUsed || decision.providerID != "piper" || decision.voiceID != "en_US-hfc_male-medium" {
		t.Fatalf("route = %#v locked=%v", decision, locked)
	}

	primary.setListError(nil)
	second, locked, err := state.resolveVoiceRoute(context.Background(), "product-1", "speakyapi", "RadioMET Tom", backup)
	if err != nil {
		t.Fatal(err)
	}
	if !locked || second != decision {
		t.Fatalf("pinned route changed from %#v to %#v", decision, second)
	}
	if got := primary.listRequestCount(); got != 1 {
		t.Fatalf("provider health probes = %d, want 1", got)
	}
}

func TestHandleSynthesisJobRoutesUnavailableSpeakyAPIToFallbackBeforeSynthesis(t *testing.T) {
	primary := &fakeProvider{id: "speakyapi", listErr: tts.ErrProviderUnavailable}
	backup := &fakeProvider{id: "piper", audio: tts.Audio{Format: tts.FormatWAV, Data: []byte("fallback")}}
	state := &serviceState{
		ctx:       context.Background(),
		cfg:       serviceConfig{Timeout: time.Second},
		providers: map[string]tts.Provider{"speakyapi": primary, "piper": backup},
		readers: []tts.Reader{{
			ID:       "00",
			Provider: "speakyapi",
			VoiceID:  "RadioMET Tom",
			Backup:   &tts.ReaderBackup{Provider: "piper", VoiceID: "en_US-hfc_male-medium"},
		}},
		dictionaries:   map[string]dictionaryResult{},
		synthesisCache: map[[32]byte]synthesisCacheEntry{},
	}
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	outputPath := filepath.Join(t.TempDir(), "fallback.wav")

	go handleSynthesisJob(context.Background(), conn, state, map[string]any{
		"type": "tts.synthesize",
		"data": map[string]any{
			"job_id":          "fallback-job",
			"voice_route_key": "product-1",
			"reader_id":       "00",
			"text":            "weather",
			"output_path":     outputPath,
		},
	})

	var event map[string]any
	if err := json.NewDecoder(peer).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event["type"] != "tts.synthesized" {
		t.Fatalf("event type = %v", event["type"])
	}
	data := event["data"].(map[string]any)
	if data["provider"] != "piper" || data["requested_provider"] != "speakyapi" || data["backup_used"] != true {
		t.Fatalf("fallback metadata = %#v", data)
	}
	if primary.requestCount() != 0 || backup.requestCount() != 1 {
		t.Fatalf("primary requests=%d fallback requests=%d", primary.requestCount(), backup.requestCount())
	}
	if got := backup.requestAt(0).VoiceID; got != "en_US-hfc_male-medium" {
		t.Fatalf("fallback voice = %q", got)
	}
}

func TestHandleSynthesisJobFallsBackWhenHealthySpeakyAPIFailsDuringSynthesis(t *testing.T) {
	primary := &fakeProvider{id: "speakyapi", err: tts.ErrProviderUnavailable}
	backup := &fakeProvider{id: "piper", audio: tts.Audio{Format: tts.FormatWAV, Data: []byte("fallback")}}
	state := &serviceState{
		ctx:       context.Background(),
		cfg:       serviceConfig{Timeout: time.Second},
		providers: map[string]tts.Provider{"speakyapi": primary, "piper": backup},
		readers: []tts.Reader{{
			ID:       "00",
			Provider: "speakyapi",
			VoiceID:  "Vocalizer Tom - Claro",
			Backup:   &tts.ReaderBackup{Provider: "piper", VoiceID: "en_US-hfc_male-medium"},
		}},
		dictionaries:   map[string]dictionaryResult{},
		synthesisCache: map[[32]byte]synthesisCacheEntry{},
	}
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	outputPath := filepath.Join(t.TempDir(), "fallback.wav")

	go handleSynthesisJob(context.Background(), conn, state, map[string]any{
		"type": "tts.synthesize",
		"data": map[string]any{
			"job_id":          "healthy-probe-fallback-job",
			"voice_route_key": "current-conditions",
			"reader_id":       "00",
			"text":            "weather",
			"output_path":     outputPath,
		},
	})

	var event map[string]any
	if err := json.NewDecoder(peer).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event["type"] != "tts.synthesized" {
		t.Fatalf("event type = %v", event["type"])
	}
	data := event["data"].(map[string]any)
	if data["provider"] != "piper" || data["backup_used"] != true {
		t.Fatalf("fallback metadata = %#v", data)
	}
	if primary.requestCount() != 1 || backup.requestCount() != 1 {
		t.Fatalf("primary requests=%d fallback requests=%d", primary.requestCount(), backup.requestCount())
	}
}

func TestResolveVoiceRouteSharesProviderHealthProbe(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	primary := &fakeProvider{id: "speakyapi", listWait: release, listStarted: started}
	state := &serviceState{
		ctx:       context.Background(),
		providers: map[string]tts.Provider{"speakyapi": primary},
	}
	backup := &tts.ReaderBackup{Provider: "piper", VoiceID: "en_US-hfc_male-medium"}
	const requests = 8
	results := make(chan voiceRouteDecision, requests)
	errors := make(chan error, requests)
	for index := range requests {
		go func() {
			decision, _, err := state.resolveVoiceRoute(context.Background(), fmt.Sprintf("product-%d", index), "speakyapi", "RadioMET Tom", backup)
			results <- decision
			errors <- err
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider health probe did not start")
	}
	if got := primary.listRequestCount(); got != 1 {
		t.Fatalf("concurrent provider health probes = %d, want 1", got)
	}
	close(release)
	for range requests {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
		decision := <-results
		if decision.backupUsed || decision.providerID != "speakyapi" || decision.voiceID != "RadioMET Tom" {
			t.Fatalf("healthy route = %#v", decision)
		}
	}
	if got := primary.listRequestCount(); got != 1 {
		t.Fatalf("provider health probes = %d, want 1", got)
	}
}

func TestHandleSynthesisJobReusesIdenticalCompletedAudio(t *testing.T) {
	provider := &fakeProvider{id: "piper", audio: tts.Audio{
		Format:     tts.FormatWAV,
		SampleRate: 22050,
		Channels:   1,
		Data:       []byte("cached wav"),
	}}
	state := &serviceState{
		cfg:          serviceConfig{Timeout: time.Second},
		providers:    map[string]tts.Provider{"piper": provider},
		dictionaries: map[string]dictionaryResult{},
	}
	dir := t.TempDir()

	run := func(jobID string, outputPath string) map[string]any {
		conn, peer := net.Pipe()
		defer conn.Close()
		defer peer.Close()
		go handleSynthesisJob(context.Background(), conn, state, map[string]any{
			"type": "tts.synthesize",
			"data": map[string]any{
				"job_id":      jobID,
				"provider":    "piper",
				"text":        "unchanged forecast",
				"voice_id":    "voice-a",
				"language":    "en-CA",
				"output_path": outputPath,
			},
		})
		var event map[string]any
		if err := json.NewDecoder(peer).Decode(&event); err != nil {
			t.Fatal(err)
		}
		return event["data"].(map[string]any)
	}

	firstPath := filepath.Join(dir, "first.wav")
	secondPath := filepath.Join(dir, "second.wav")
	first := run("job-1", firstPath)
	second := run("job-2", secondPath)

	if provider.requestCount() != 1 {
		t.Fatalf("provider requests = %d, want 1", provider.requestCount())
	}
	if first["cache_hit"] != false || second["cache_hit"] != true {
		t.Fatalf("cache metadata first=%v second=%v", first["cache_hit"], second["cache_hit"])
	}
	if raw, err := os.ReadFile(secondPath); err != nil || string(raw) != "cached wav" {
		t.Fatalf("cached output = %q err=%v", raw, err)
	}
}

func TestHandleSynthesisJobReusesPersistentCacheAfterRestart(t *testing.T) {
	provider := &fakeProvider{id: "piper", audio: tts.Audio{
		Format:     tts.FormatWAV,
		SampleRate: 22050,
		Channels:   1,
		Data:       []byte("persistent cached wav"),
	}}
	dir := t.TempDir()
	cacheDir := filepath.Join(dir, "cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatal(err)
	}

	run := func(state *serviceState, jobID string, outputPath string) map[string]any {
		conn, peer := net.Pipe()
		defer conn.Close()
		defer peer.Close()
		go handleSynthesisJob(context.Background(), conn, state, map[string]any{
			"type": "tts.synthesize",
			"data": map[string]any{
				"job_id":      jobID,
				"provider":    "piper",
				"text":        "unchanged forecast",
				"voice_id":    "voice-a",
				"language":    "en-CA",
				"output_path": outputPath,
			},
		})
		var event map[string]any
		if err := json.NewDecoder(peer).Decode(&event); err != nil {
			t.Fatal(err)
		}
		return event["data"].(map[string]any)
	}

	newState := func() *serviceState {
		return &serviceState{
			cfg: serviceConfig{
				Timeout:         time.Second,
				CacheDir:        cacheDir,
				CacheMaxBytes:   1 << 20,
				CacheMaxEntries: 8,
			},
			providers:    map[string]tts.Provider{"piper": provider},
			dictionaries: map[string]dictionaryResult{},
		}
	}

	first := run(newState(), "job-1", filepath.Join(dir, "first.wav"))
	secondPath := filepath.Join(dir, "second.wav")
	second := run(newState(), "job-2", secondPath)

	if provider.requestCount() != 1 {
		t.Fatalf("provider requests = %d, want 1", provider.requestCount())
	}
	if first["cache_hit"] != false || second["cache_hit"] != true {
		t.Fatalf("cache metadata first=%v second=%v", first["cache_hit"], second["cache_hit"])
	}
	if raw, err := os.ReadFile(secondPath); err != nil || string(raw) != "persistent cached wav" {
		t.Fatalf("persistent cached output = %q err=%v", raw, err)
	}
}

func TestSynthesisQueuePrioritizesRealtimeJobs(t *testing.T) {
	queue := &synthesisQueue{
		high:   make(chan map[string]any, 3),
		normal: make(chan map[string]any, 3),
		low:    make(chan map[string]any, 3),
	}
	if !queue.Enqueue(context.Background(), map[string]any{"data": map[string]any{"job_id": "low", "priority": "low"}}) {
		t.Fatal("low enqueue failed")
	}
	if !queue.Enqueue(context.Background(), map[string]any{"data": map[string]any{"job_id": "normal"}}) {
		t.Fatal("normal enqueue failed")
	}
	if !queue.Enqueue(context.Background(), map[string]any{"data": map[string]any{"job_id": "high", "priority": "high"}}) {
		t.Fatal("high enqueue failed")
	}
	for _, want := range []string{"high", "normal", "low"} {
		message, ok := queue.next(context.Background())
		if !ok {
			t.Fatalf("next returned false for %s", want)
		}
		if got := firstText(message, objectValue(message, "data"), "job_id"); got != want {
			t.Fatalf("next job = %q, want %q", got, want)
		}
	}
}

func TestProviderCandidatesIncludesSpeakyAPIAsAutoFallback(t *testing.T) {
	providers := map[string]tts.Provider{
		"piper":     &fakeProvider{id: "piper"},
		"speakyapi": &fakeProvider{id: "speakyapi"},
	}
	candidates, err := providerCandidates(providers, "auto")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 || candidates[0].ID() != "piper" || candidates[1].ID() != "speakyapi" {
		t.Fatalf("candidates = %#v", candidates)
	}
}

func TestProviderCandidatesSelectsSpeakyAPIExplicitly(t *testing.T) {
	providers := map[string]tts.Provider{
		"piper":     &fakeProvider{id: "piper"},
		"speakyapi": &fakeProvider{id: "speakyapi"},
	}
	candidates, err := providerCandidates(providers, "speaky-api")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].ID() != "speakyapi" {
		t.Fatalf("candidates = %#v", candidates)
	}
}
