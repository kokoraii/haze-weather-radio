package webgateway

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/events"
)

func TestOperatorBreakInPublishesLiveEvents(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)
	configPath := filepath.Join(dir, "config.yaml")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("HAZE_HOST_BRIDGE_ADDR", listener.Addr().String())
	received := make(chan map[string]any, 4)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			var event map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &event); err == nil {
				received <- event
			}
		}
	}()
	manager := NewOperatorBreakInManager()

	started, err := manager.start(configPath, []string{"sk-0001"}, "Desk Mic", 48000, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	sessionID, _ := started["session_id"].(string)
	if sessionID == "" {
		t.Fatal("session id was not returned")
	}
	if _, err := manager.appendChunk(sessionID, []byte{0x00, 0x00, 0x10, 0x00}); err != nil {
		t.Fatal(err)
	}
	result, err := manager.finish(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if live, _ := result["live"].(bool); !live {
		t.Fatalf("finish result did not report live mode: %#v", result)
	}
	registration := <-received
	if registration["type"] != "bridge.client" {
		t.Fatalf("publisher registration type = %v", registration["type"])
	}
	registrationData, _ := registration["data"].(map[string]any)
	if registrationData["receive_events"] != false {
		t.Fatalf("publisher registration receives events: %#v", registration)
	}
	wantTypes := []string{"operator.breakin.start", "operator.breakin.chunk", "operator.breakin.finish"}
	for _, want := range wantTypes {
		event := <-received
		if event["type"] != want {
			t.Fatalf("event type = %v, want %s", event["type"], want)
		}
	}
}

func TestOperatorBreakInRejectsUnsafePrerollPath(t *testing.T) {
	if _, err := operatorBreakInWAVFromRelPath(filepath.Join(t.TempDir(), "config.yaml"), "../secret.wav"); err == nil {
		t.Fatal("expected unsafe preroll path to be rejected")
	}
}

func TestOperatorBreakInStreamURLValidation(t *testing.T) {
	for _, raw := range []string{"ftp://example.com/live.mp3", "file:///tmp/audio.wav", "http://"} {
		if err := validateOperatorBreakInStreamURL(raw); err == nil {
			t.Fatalf("expected %q to be rejected", raw)
		}
	}
	for _, raw := range []string{
		"http://example.com/live.mp3",
		"https://example.com/live.m3u8",
		"rtmp://example.com/live/stream",
		"rtsp://example.com/live.sdp",
		"srt://example.com:9000?mode=caller",
		"udp://239.0.0.1:5000",
	} {
		if err := validateOperatorBreakInStreamURL(raw); err != nil {
			t.Fatalf("expected %q to be accepted: %v", raw, err)
		}
	}
}

func TestOperatorBreakInReaperKeepsLiveSessions(t *testing.T) {
	manager := NewOperatorBreakInManager()
	cancelled := false
	manager.sessions["stream"] = &operatorBreakInSession{
		ID:        "stream",
		StreamURL: "https://example.com/live.mp3",
		StartedAt: time.Now().Add(-10 * time.Minute),
		Cancel: func() {
			cancelled = true
		},
	}
	manager.sessions["mic"] = &operatorBreakInSession{
		ID:        "mic",
		StartedAt: time.Now().Add(-10 * time.Minute),
		Cancel: func() {
			cancelled = true
		},
	}
	manager.sessions["broken"] = nil

	manager.reapStale()

	if manager.sessions["stream"] == nil {
		t.Fatal("live URL stream session was reaped")
	}
	if manager.sessions["mic"] == nil {
		t.Fatal("live mic session was reaped")
	}
	if _, ok := manager.sessions["broken"]; ok {
		t.Fatal("nil session was not pruned")
	}
	if cancelled {
		t.Fatal("live session was cancelled by stale reaper")
	}
}

func TestOperatorBreakInStreamChunksAreUnlimitedUntilStopped(t *testing.T) {
	manager := NewOperatorBreakInManager()
	manager.sessions["stream"] = &operatorBreakInSession{
		ID:         "stream",
		FeedIDs:    []string{"sk-0001"},
		Title:      "Stream",
		SampleRate: 48000,
		Channels:   1,
		Publisher:  nil,
		StartedAt:  time.Now(),
	}

	if _, err := manager.appendChunk("stream", []byte{0, 0, 1, 0}); err == nil {
		t.Fatal("inactive publisher should still be rejected")
	}

	manager.sessions["stream"].Publisher = eventsTestPublisher(t)
	defer manager.sessions["stream"].Publisher.Close()
	manager.sessions["stream"].Bytes = 48_000 * 2 * 60 * 60
	if _, err := manager.appendChunk("stream", []byte{0, 0, 1, 0}); err != nil {
		t.Fatalf("stream chunk should not be capped by mic duration: %v", err)
	}
}

func eventsTestPublisher(t *testing.T) *events.HostBridgePublisher {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()
	return events.NewHostBridgePublisher(listener.Addr().String())
}
