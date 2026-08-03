package asr

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type capturePublisher struct {
	mu     sync.Mutex
	events []map[string]any
}

func (p *capturePublisher) Publish(event map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *capturePublisher) find(eventType string, requestID string) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, event := range p.events {
		if stringAt(event, "type") != eventType {
			continue
		}
		data := mapAt(event, "data")
		if requestID == "" || firstText(event, data, "request_id", "subject") == requestID {
			return event
		}
	}
	return nil
}

type fakeTranscriber struct {
	text    string
	err     error
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *fakeTranscriber) Transcribe(ctx context.Context, _ string, _ string, _ string) (string, error) {
	if f.started != nil {
		f.once.Do(func() { close(f.started) })
	}
	if f.release != nil {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-f.release:
		}
	}
	return f.text, f.err
}

func TestServicePublishesTranscriptAndDeletesSpool(t *testing.T) {
	t.Parallel()
	spool := t.TempDir()
	requestID := "request-success"
	path := filepath.Join(spool, requestID+".wav")
	writeTestWave(t, path, 2*time.Second)
	publisher := &capturePublisher{}
	service := NewService(testConfig(spool), &fakeTranscriber{text: "  Moose Jaw  "}, publisher, true)
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan map[string]any, 1)
	done := make(chan error, 1)
	go func() { done <- service.Serve(ctx, events) }()
	events <- asrEvent(requestID)
	waitForEvent(t, publisher, "asr.transcribed", requestID)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spool audio was not deleted: %v", err)
	}
	cancel()
	<-done
}

func TestServiceRejectsTraversalAndCleansUnavailableAudio(t *testing.T) {
	t.Parallel()
	spool := t.TempDir()
	publisher := &capturePublisher{}
	service := NewService(testConfig(spool), nil, publisher, false)

	service.submit(Request{
		RequestID: "request-badpath", AudioFile: "../request-badpath.wav", SampleRate: 16000, Channels: 1, Audience: "telephone",
	})
	failed := publisher.find("asr.failed", "request-badpath")
	if stringAt(mapAt(failed, "data"), "code") != "invalid_request" {
		t.Fatalf("path traversal did not produce invalid_request: %#v", failed)
	}

	requestID := "request-offline"
	path := filepath.Join(spool, requestID+".wav")
	writeTestWave(t, path, 2*time.Second)
	service.submit(Request{RequestID: requestID, AudioFile: requestID + ".wav", SampleRate: 16000, Channels: 1, Audience: "telephone"})
	failed = publisher.find("asr.failed", requestID)
	if stringAt(mapAt(failed, "data"), "code") != "provider_unavailable" {
		t.Fatalf("degraded service did not report provider_unavailable: %#v", failed)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("degraded service did not delete spool audio: %v", err)
	}
}

func TestServiceQueueSaturationIsBoundedAndCleansRejectedAudio(t *testing.T) {
	spool := t.TempDir()
	publisher := &capturePublisher{}
	transcriber := &fakeTranscriber{started: make(chan struct{}), release: make(chan struct{})}
	cfg := testConfig(spool)
	cfg.Workers = 1
	cfg.QueueSize = 1
	service := NewService(cfg, transcriber, publisher, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan map[string]any, 3)
	done := make(chan error, 1)
	go func() { done <- service.Serve(ctx, events) }()

	for _, requestID := range []string{"request-first", "request-second", "request-third"} {
		writeTestWave(t, filepath.Join(spool, requestID+".wav"), 2*time.Second)
	}
	events <- asrEvent("request-first")
	select {
	case <-transcriber.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start first transcription")
	}
	events <- asrEvent("request-second")
	events <- asrEvent("request-third")
	waitForEvent(t, publisher, "asr.failed", "request-third")
	failed := publisher.find("asr.failed", "request-third")
	if stringAt(mapAt(failed, "data"), "code") != "busy" || !boolValue(mapAt(failed, "data")["retryable"], false) {
		t.Fatalf("queue rejection did not publish retryable busy: %#v", failed)
	}
	if _, err := os.Stat(filepath.Join(spool, "request-third.wav")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected audio was not deleted: %v", err)
	}
	service.publishMetrics()
	metric := publisher.find("service.metrics", "")
	if metric == nil || firstInt(metric, mapAt(metric, "data"), "queue_rejected") != 1 {
		t.Fatalf("queue rejection metric = %#v", metric)
	}
	close(transcriber.release)
	cancel()
	<-done
	if _, err := os.Stat(filepath.Join(spool, "request-second.wav")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("queued audio was not cleaned during shutdown: %v", err)
	}
}

func TestServiceTimeoutAndEmptyTranscriptUseStableErrors(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		fake     *fakeTranscriber
		timeout  time.Duration
		wantCode string
	}{
		{name: "timeout", fake: &fakeTranscriber{release: make(chan struct{})}, timeout: 10 * time.Millisecond, wantCode: "timeout"},
		{name: "empty transcript", fake: &fakeTranscriber{text: "   "}, timeout: time.Second, wantCode: "provider_error"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spool := t.TempDir()
			requestID := "request-stable-error"
			path := filepath.Join(spool, requestID+".wav")
			writeTestWave(t, path, 2*time.Second)
			publisher := &capturePublisher{}
			cfg := testConfig(spool)
			cfg.Timeout = test.timeout
			service := NewService(cfg, test.fake, publisher, true)
			ctx, cancel := context.WithCancel(context.Background())
			events := make(chan map[string]any, 1)
			done := make(chan error, 1)
			go func() { done <- service.Serve(ctx, events) }()
			events <- asrEvent(requestID)
			waitForEvent(t, publisher, "asr.failed", requestID)
			failed := publisher.find("asr.failed", requestID)
			if stringAt(mapAt(failed, "data"), "code") != test.wantCode {
				t.Fatalf("stable error = %#v", failed)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed request left spool audio: %v", err)
			}
			cancel()
			<-done
		})
	}
}

func TestServiceRejectsSpoolSymlinkWithoutDeletingTarget(t *testing.T) {
	t.Parallel()
	spool := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.wav")
	writeTestWave(t, outside, 2*time.Second)
	requestID := "request-symlink"
	link := filepath.Join(spool, requestID+".wav")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}
	publisher := &capturePublisher{}
	service := NewService(testConfig(spool), &fakeTranscriber{text: "unused"}, publisher, true)
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan map[string]any, 1)
	done := make(chan error, 1)
	go func() { done <- service.Serve(ctx, events) }()
	events <- asrEvent(requestID)
	waitForEvent(t, publisher, "asr.failed", requestID)
	if stringAt(mapAt(publisher.find("asr.failed", requestID), "data"), "code") != "invalid_request" {
		t.Fatal("spool symlink did not produce invalid_request")
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("rejecting spool symlink removed its target: %v", err)
	}
	cancel()
	<-done
}

func TestServiceRejectsMalformedAndOutOfRangeWAV(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		duration time.Duration
		mutate   func([]byte)
	}{
		{name: "too short", duration: time.Second},
		{name: "too long", duration: 9 * time.Second},
		{name: "wrong sample rate", duration: 2 * time.Second, mutate: func(raw []byte) { putUint32(raw[24:28], 8000) }},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spool := t.TempDir()
			requestID := "request-invalid"
			path := filepath.Join(spool, requestID+".wav")
			writeTestWave(t, path, test.duration)
			if test.mutate != nil {
				raw, _ := os.ReadFile(path)
				test.mutate(raw)
				if err := os.WriteFile(path, raw, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			publisher := &capturePublisher{}
			service := NewService(testConfig(spool), &fakeTranscriber{text: "unused"}, publisher, true)
			ctx, cancel := context.WithCancel(context.Background())
			events := make(chan map[string]any, 1)
			done := make(chan error, 1)
			go func() { done <- service.Serve(ctx, events) }()
			events <- asrEvent(requestID)
			waitForEvent(t, publisher, "asr.failed", requestID)
			failed := publisher.find("asr.failed", requestID)
			if stringAt(mapAt(failed, "data"), "code") != "invalid_request" {
				t.Fatalf("malformed audio result: %#v", failed)
			}
			cancel()
			<-done
		})
	}
}

func testConfig(spool string) Config {
	return Config{
		Enabled: true, Provider: defaultProvider, Model: defaultModel, SpoolDir: spool,
		Workers: 1, QueueSize: 4, Timeout: time.Second, MaxAudioBytes: defaultMaxAudioBytes,
	}
}

func asrEvent(requestID string) map[string]any {
	return map[string]any{
		"type": "asr.transcribe",
		"data": map[string]any{
			"request_id": requestID, "audio_file": requestID + ".wav", "language": "en-CA",
			"prompt": "A place in Saskatchewan", "sample_rate": 16000, "channels": 1, "audience": "telephone",
		},
	}
}

func waitForEvent(t *testing.T, publisher *capturePublisher, eventType string, requestID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if publisher.find(eventType, requestID) != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s for %s", eventType, requestID)
}
