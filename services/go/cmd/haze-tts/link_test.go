package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/tts"
)

const testLinkToken = "test-token"

// startTestLink serves one link over a pipe and returns the daemon's end.
func startTestLink(t *testing.T, ctx context.Context, provider tts.Provider, links *linkSlot) (net.Conn, *bufio.Reader, <-chan struct{}) {
	t.Helper()
	service, daemon := net.Pipe()
	t.Cleanup(func() { _ = daemon.Close() })
	done := make(chan struct{})
	cfg := serviceConfig{Timeout: time.Second, Workers: 1}
	go func() {
		defer close(done)
		serveLink(ctx, service, cfg, map[string]tts.Provider{"piper": provider}, testLinkToken, links)
	}()
	return daemon, bufio.NewReader(daemon), done
}

func sendLinkLine(t *testing.T, conn net.Conn, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		t.Fatalf("write to link: %v", err)
	}
}

func readLinkEvent(t *testing.T, conn net.Conn, reader *bufio.Reader) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read from link: %v", err)
	}
	var event map[string]any
	if err := json.Unmarshal(line, &event); err != nil {
		t.Fatalf("decode %q: %v", line, err)
	}
	return event
}

func testHello(token string, readersXML string) map[string]any {
	return map[string]any{
		"type": "link.hello",
		"data": map[string]any{
			"protocol": linkProtocol,
			"token":    token,
			"settings": map[string]any{
				"readers_xml":     readersXML,
				"dictionary_json": `{"*":{"WX":"weather"}}`,
				"language":        "en-CA",
				"timeout":         "5s",
			},
		},
	}
}

func TestServeLinkRefusesAWrongToken(t *testing.T) {
	provider := &fakeProvider{id: "piper"}
	daemon, reader, done := startTestLink(t, context.Background(), provider, &linkSlot{})

	sendLinkLine(t, daemon, testHello("wrong", "<Readers/>"))
	event := readLinkEvent(t, daemon, reader)
	if event["type"] != "link.refused" {
		t.Fatalf("event = %#v, want link.refused", event)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a refused link stayed open")
	}
}

func TestServeLinkSynthesizesInlineWithPushedReaders(t *testing.T) {
	provider := &fakeProvider{id: "piper", audio: tts.Audio{Format: tts.FormatWAV, Data: []byte("wav")}}
	daemon, reader, _ := startTestLink(t, context.Background(), provider, &linkSlot{})

	readers := `<Readers><reader id="r1" provider="piper"><language>en-CA</language><voice_id>voice-1</voice_id></reader></Readers>`
	sendLinkLine(t, daemon, testHello(testLinkToken, readers))
	if event := readLinkEvent(t, daemon, reader); event["type"] != "link.ready" {
		t.Fatalf("event = %#v, want link.ready", event)
	}
	registration := readLinkEvent(t, daemon, reader)
	if registration["type"] != "bridge.client" {
		t.Fatalf("event = %#v, want the bridge.client registration", registration)
	}
	if event := readLinkEvent(t, daemon, reader); event["type"] != "service.ready" {
		t.Fatalf("event = %#v, want service.ready", event)
	}

	// The path is on the daemon's machine. The service must not write it.
	outputPath := filepath.Join(t.TempDir(), "daemon-side.wav")
	sendLinkLine(t, daemon, map[string]any{
		"type": "tts.synthesize",
		"data": map[string]any{"job_id": "remote-1", "reader_id": "r1", "text": "WX update", "output_path": outputPath},
	})
	event := readLinkEvent(t, daemon, reader)
	if event["type"] != "tts.synthesized" {
		t.Fatalf("event = %#v, want tts.synthesized", event)
	}
	data := event["data"].(map[string]any)
	if data["output_path"] != outputPath || data["output_b64"] != base64.StdEncoding.EncodeToString([]byte("wav")) {
		t.Fatalf("synthesized data = %#v", data)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("remote mode wrote the daemon's output path: %v", err)
	}
	request := provider.requestAt(0)
	if request.VoiceID != "voice-1" || request.Text != "weather update" {
		t.Fatalf("request = %#v, want the pushed reader's voice and dictionary", request)
	}
}

func TestServeLinkReplacesTheActiveLink(t *testing.T) {
	links := &linkSlot{}
	first, firstReader, firstDone := startTestLink(t, context.Background(), &fakeProvider{id: "piper"}, links)
	sendLinkLine(t, first, testHello(testLinkToken, "<Readers/>"))
	if event := readLinkEvent(t, first, firstReader); event["type"] != "link.ready" {
		t.Fatalf("first link event = %#v", event)
	}
	// Drain the registration and service.ready so the first link is idle.
	readLinkEvent(t, first, firstReader)
	readLinkEvent(t, first, firstReader)

	second, secondReader, _ := startTestLink(t, context.Background(), &fakeProvider{id: "piper"}, links)
	sendLinkLine(t, second, testHello(testLinkToken, "<Readers/>"))
	if event := readLinkEvent(t, second, secondReader); event["type"] != "link.ready" {
		t.Fatalf("second link event = %#v", event)
	}
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("the first link kept running after a newer link connected")
	}
}
