package webgateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// TestMediaControlTransportsAgreeOnARealHazeMedia runs the real haze-media
// binary and checks that the HTTP and gRPC control clients see the same thing.
// It is skipped unless HAZE_MEDIA_BIN names a built haze-media executable:
//
//	cargo build -p haze-media
//	HAZE_MEDIA_BIN=../../target/debug/haze-media go test ./internal/webgateway -run RealHazeMedia -v
//
// A binary built with --features gstreamer-backend can be run with
// HAZE_MEDIA_BACKEND=auto to compare the encoded audio and WebRTC rules too.
func TestMediaControlTransportsAgreeOnARealHazeMedia(t *testing.T) {
	binary := os.Getenv("HAZE_MEDIA_BIN")
	if binary == "" {
		t.Skip("HAZE_MEDIA_BIN is not set")
	}
	httpAddr, grpcAddr := freeLoopbackAddr(t), freeLoopbackAddr(t)
	dir := t.TempDir()
	configDir := filepath.Join(dir, "managed", "configs")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "output.xml"),
		[]byte(`<outputs><feed id="sk-0001"><webrtc enabled="true"/></feed></outputs>`), 0o644); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "config.yaml")
	backend := os.Getenv("HAZE_MEDIA_BACKEND")
	if backend == "" {
		backend = "legacy"
	}
	config := fmt.Sprintf("services:\n  rust:\n    media:\n      enabled: true\n      addr: %q\n      grpc_addr: %q\n      backend: %s\n", httpAddr, grpcAddr, backend)
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}

	process := exec.Command(binary, "--config", configPath, "--bridge", "127.0.0.1:1")
	process.Stdout, process.Stderr = os.Stderr, os.Stderr
	if err := process.Start(); err != nil {
		t.Fatalf("start haze-media: %v", err)
	}
	t.Cleanup(func() {
		_ = process.Process.Kill()
		_ = process.Wait()
	})

	httpControl := httpMediaControl{baseURL: "http://" + httpAddr}
	grpcControl, err := newGRPCMediaControl(grpcAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = grpcControl.conn.Close() })
	waitForMediaControl(t, httpControl)
	waitForMediaControl(t, grpcControl)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	t.Run("health", func(t *testing.T) {
		fromHTTP, okHTTP := httpControl.Health(ctx)
		fromGRPC, okGRPC := grpcControl.Health(ctx)
		if !okHTTP || !okGRPC {
			t.Fatalf("health available over http=%v grpc=%v", okHTTP, okGRPC)
		}
		for _, key := range []string{"ok", "service", "backend", "gstreamer_available", "capabilities"} {
			if fmt.Sprint(fromHTTP[key]) != fmt.Sprint(fromGRPC[key]) {
				t.Errorf("health %q differs: http=%v grpc=%v", key, fromHTTP[key], fromGRPC[key])
			}
		}
	})

	t.Run("audio streams match", func(t *testing.T) {
		for _, codec := range []string{"pcm16", "raw"} {
			fromHTTP := readAudioPrefix(t, httpControl, "sk-0001", codec, 44+1920*3)
			fromGRPC := readAudioPrefix(t, grpcControl, "sk-0001", codec, 44+1920*3)
			if fromHTTP.contentType != fromGRPC.contentType {
				t.Errorf("%s content type: http=%q grpc=%q", codec, fromHTTP.contentType, fromGRPC.contentType)
			}
			head := 1920
			if codec == "pcm16" {
				head = 44 + 1920
			}
			if !bytes.Equal(fromHTTP.body[:head], fromGRPC.body[:head]) {
				t.Errorf("%s: the opening header and silence frame differ between transports", codec)
			}
			if len(fromGRPC.body) < head+1920 {
				t.Errorf("%s: gRPC delivered only %d bytes, no paced frames followed", codec, len(fromGRPC.body))
			}
		}
	})

	health, _ := httpControl.Health(ctx)
	capabilities, _ := health["capabilities"].(map[string]any)
	encoderAvailable, _ := capabilities["encoded_http_audio"].(bool)

	t.Run("audio refusals match", func(t *testing.T) {
		cases := []struct{ feed, codec string }{{"missing", "pcm16"}, {"sk-0001", "flac"}, {"bad feed", "pcm16"}}
		if !encoderAvailable {
			cases = append(cases, struct{ feed, codec string }{"sk-0001", "opus"})
		}
		for _, tc := range cases {
			streamHTTP, errHTTP := httpControl.FeedAudio(ctx, tc.feed, tc.codec)
			if errHTTP == nil {
				_ = streamHTTP.Body.Close()
			}
			streamGRPC, errGRPC := grpcControl.FeedAudio(ctx, tc.feed, tc.codec)
			if errGRPC == nil {
				_ = streamGRPC.Body.Close()
			}
			if errHTTP == nil || errGRPC == nil {
				t.Errorf("%+v: refused over http=%v grpc=%v", tc, errHTTP != nil, errGRPC != nil)
			}
		}
	})

	t.Run("offers match", func(t *testing.T) {
		offer := map[string]any{"feed_id": "sk-0001", "sdp": "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n"}
		fromHTTP, fromGRPC := httpControl.WebRTCOffer(ctx, offer), grpcControl.WebRTCOffer(ctx, offer)
		if fromHTTP.StatusCode != http.StatusServiceUnavailable || fromGRPC.StatusCode != http.StatusServiceUnavailable ||
			fromHTTP.Terminal || fromGRPC.Terminal || fromHTTP.OK() || fromGRPC.OK() {
			t.Fatalf("without a WebRTC backend both transports must fall back: http=%#v grpc=%#v", fromHTTP, fromGRPC)
		}
	})

	if encoderAvailable {
		t.Run("encoded audio matches", func(t *testing.T) {
			for codec, magic := range map[string]string{"opus": "OggS", "aac": "\xff"} {
				fromHTTP := readAudioPrefix(t, httpControl, "sk-0001", codec, 4)
				fromGRPC := readAudioPrefix(t, grpcControl, "sk-0001", codec, 4)
				if fromHTTP.contentType != fromGRPC.contentType {
					t.Errorf("%s content type: http=%q grpc=%q", codec, fromHTTP.contentType, fromGRPC.contentType)
				}
				if !bytes.HasPrefix(fromHTTP.body, []byte(magic)) || !bytes.HasPrefix(fromGRPC.body, []byte(magic)) {
					t.Errorf("%s: unexpected stream start http=%q grpc=%q", codec, fromHTTP.body, fromGRPC.body)
				}
			}
		})
	}

	if webrtc, _ := capabilities["webrtc"].(bool); webrtc {
		t.Run("a real browser offer is answered the same way", func(t *testing.T) {
			offerSDP := browserOfferSDP(t)
			answer := func(control mediaControl, clientIP string) mediaServiceWebRTCAnswerResult {
				return control.WebRTCOffer(ctx, map[string]any{"feed_id": "sk-0001", "sdp": offerSDP, "client_ip": clientIP})
			}
			// Each transport uses its own listener address, because haze-media allows one
			// WebRTC listener per address and feed.
			fromHTTP, fromGRPC := answer(httpControl, "198.51.100.1"), answer(grpcControl, "198.51.100.2")
			if !fromHTTP.OK() || !fromGRPC.OK() {
				t.Fatalf("answered over http=%v grpc=%v (http %#v, grpc %#v)", fromHTTP.OK(), fromGRPC.OK(), fromHTTP, fromGRPC)
			}
			for _, key := range []string{"feed_id", "sdp_type", "codec", "payload_type", "media_recent"} {
				if fmt.Sprint(fromHTTP.Answer[key]) != fmt.Sprint(fromGRPC.Answer[key]) {
					t.Errorf("answer %q differs: http=%v grpc=%v", key, fromHTTP.Answer[key], fromGRPC.Answer[key])
				}
			}
			if sdp, _ := fromGRPC.Answer["sdp"].(string); !strings.HasPrefix(sdp, "v=0") {
				t.Errorf("gRPC answer SDP = %q", sdp)
			}
			// A second offer from the same address is refused identically.
			again := []mediaServiceWebRTCAnswerResult{answer(httpControl, "198.51.100.1"), answer(grpcControl, "198.51.100.2")}
			for index, result := range again {
				if result.StatusCode != http.StatusTooManyRequests || !result.Terminal || result.Detail != "listener already active for this IP and feed" {
					t.Errorf("duplicate offer %d = %#v", index, result)
				}
			}
		})

		t.Run("offer refusals match", func(t *testing.T) {
			offer := map[string]any{"feed_id": "sk-0001", "sdp": "v=0"}
			fromHTTP, fromGRPC := httpControl.WebRTCOffer(ctx, offer), grpcControl.WebRTCOffer(ctx, offer)
			if fromHTTP.StatusCode != fromGRPC.StatusCode || fromHTTP.Terminal != fromGRPC.Terminal || !fromGRPC.Terminal {
				t.Fatalf("http=%#v grpc=%#v", fromHTTP, fromGRPC)
			}
			if fromHTTP.Detail != fromGRPC.Detail {
				t.Errorf("detail differs: http=%q grpc=%q", fromHTTP.Detail, fromGRPC.Detail)
			}
		})
	}

	t.Run("listeners are released", func(t *testing.T) {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			fromHTTP, _ := httpControl.Health(ctx)
			if fmt.Sprint(fromHTTP["http_client_count"]) == "0" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("audio listeners were not unregistered after the streams were closed")
	})
}

type audioPrefix struct {
	contentType string
	body        []byte
}

func readAudioPrefix(t *testing.T, control mediaControl, feed string, codec string, want int) audioPrefix {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := control.FeedAudio(ctx, feed, codec)
	if err != nil {
		t.Fatalf("%T FeedAudio(%s, %s): %v", control, feed, codec, err)
	}
	defer stream.Body.Close()
	body := make([]byte, want)
	if codec == "raw" {
		body = make([]byte, 1920*4)
	}
	if _, err := io.ReadFull(stream.Body, body); err != nil {
		t.Fatalf("%T read %s: %v", control, codec, err)
	}
	return audioPrefix{contentType: stream.Header.Get("Content-Type"), body: body}
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func waitForMediaControl(t *testing.T, control mediaControl) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := control.Health(context.Background()); ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%T never became healthy", control)
}

// browserOfferSDP builds the kind of offer a browser listener sends: one
// receive-only audio transceiver.
func browserOfferSDP(t *testing.T) string {
	t.Helper()
	connection, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		t.Fatal(err)
	}
	offer, err := connection.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	return offer.SDP
}
