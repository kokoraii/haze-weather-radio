package webgateway

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	mediav1 "github.com/meowraii/haze-weather-radio/services/go/internal/gen/media/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeMediaControlServer stands in for haze-media's MediaControlService.
type fakeMediaControlServer struct {
	mediav1.UnimplementedMediaControlServiceServer
	health func() (*mediav1.HealthResponse, error)
	offer  func(*mediav1.WebRtcOfferRequest) (*mediav1.WebRtcOfferResponse, error)
	audio  func(*mediav1.StreamFeedAudioRequest, grpc.ServerStreamingServer[mediav1.StreamFeedAudioResponse]) error
}

func (f *fakeMediaControlServer) Health(context.Context, *mediav1.HealthRequest) (*mediav1.HealthResponse, error) {
	return f.health()
}

func (f *fakeMediaControlServer) WebRtcOffer(_ context.Context, request *mediav1.WebRtcOfferRequest) (*mediav1.WebRtcOfferResponse, error) {
	return f.offer(request)
}

func (f *fakeMediaControlServer) StreamFeedAudio(request *mediav1.StreamFeedAudioRequest, stream grpc.ServerStreamingServer[mediav1.StreamFeedAudioResponse]) error {
	return f.audio(request, stream)
}

func startFakeMediaControl(t *testing.T, fake *fakeMediaControlServer) (control *grpcMediaControl, stop func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	mediav1.RegisterMediaControlServiceServer(server, fake)
	go func() { _ = server.Serve(listener) }()
	control, err = newGRPCMediaControl(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = control.conn.Close()
		server.Stop()
	})
	return control, server.Stop
}

func TestGRPCMediaControlHealthDecodesTheStatusDocument(t *testing.T) {
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		health: func() (*mediav1.HealthResponse, error) {
			return &mediav1.HealthResponse{StatusJson: []byte(`{"ok":true,"service":"haze-media","capabilities":{"http_audio":true,"webrtc":false}}`)}, nil
		},
	})
	health, ok := control.Health(context.Background())
	if !ok {
		t.Fatal("health was not available")
	}
	capabilities, _ := health["capabilities"].(map[string]any)
	if health["service"] != "haze-media" || capabilities["http_audio"] != true {
		t.Fatalf("health = %#v", health)
	}
}

func TestGRPCMediaControlHealthReportsUnavailableForErrorsAndBadJSON(t *testing.T) {
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		health: func() (*mediav1.HealthResponse, error) {
			return &mediav1.HealthResponse{StatusJson: []byte(`not json`)}, nil
		},
	})
	if _, ok := control.Health(context.Background()); ok {
		t.Fatal("malformed health document was accepted")
	}
	failing, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		health: func() (*mediav1.HealthResponse, error) { return nil, status.Error(codes.Internal, "boom") },
	})
	if _, ok := failing.Health(context.Background()); ok {
		t.Fatal("a failing health call was reported as available")
	}
}

func TestGRPCMediaControlOfferAnswerCarriesTheRequestFieldsAndAnswer(t *testing.T) {
	var seen *mediav1.WebRtcOfferRequest
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		offer: func(request *mediav1.WebRtcOfferRequest) (*mediav1.WebRtcOfferResponse, error) {
			seen = request
			return &mediav1.WebRtcOfferResponse{
				FeedId: request.GetFeedId(), Sdp: "v=0 answer", SdpType: "answer", Codec: "opus", PayloadType: 111, MediaRecent: true,
			}, nil
		},
	})
	result := control.WebRTCOffer(context.Background(), map[string]any{
		"feed_id": "sk-0001", "sdp": "v=0 offer", "disable_g722": true, "require_opus": true,
		"preferred_codec": "opus", "client_ip": "203.0.113.9", "remote_addr": "203.0.113.9:5000",
	})
	if !result.OK() || result.StatusCode != http.StatusOK || result.Terminal {
		t.Fatalf("result = %#v", result)
	}
	if result.Answer["sdp"] != "v=0 answer" || result.Answer["codec"] != "opus" || result.Answer["media_recent"] != true {
		t.Fatalf("answer = %#v", result.Answer)
	}
	if got, ok := result.Answer["payload_type"].(uint32); !ok || got != 111 {
		t.Fatalf("payload_type = %#v", result.Answer["payload_type"])
	}
	if seen.GetFeedId() != "sk-0001" || seen.GetSdp() != "v=0 offer" || !seen.GetDisableG722() || !seen.GetRequireOpus() ||
		seen.GetPreferredCodec() != "opus" || seen.GetClientIp() != "203.0.113.9" || seen.GetRemoteAddr() != "203.0.113.9:5000" {
		t.Fatalf("request = %v", seen)
	}
}

func TestGRPCMediaControlOfferErrorsFollowTheHTTPRules(t *testing.T) {
	cases := []struct {
		name         string
		err          error
		wantStatus   int
		wantTerminal bool
		wantDetail   string
	}{
		{"unavailable falls back", status.Error(codes.Unavailable, "str0m WebRTC backend is unavailable"), http.StatusServiceUnavailable, false, ""},
		{"not found falls back", status.Error(codes.NotFound, "feed is not configured"), http.StatusNotFound, false, ""},
		{"bad request is final", status.Error(codes.InvalidArgument, "sdp is required"), http.StatusBadRequest, true, "sdp is required"},
		{"no usable codec is final", status.Error(codes.Unimplemented, "no codec"), http.StatusUnsupportedMediaType, true, "no codec"},
		{"duplicate listener is final", status.Error(codes.ResourceExhausted, "listener already active"), http.StatusTooManyRequests, true, "listener already active"},
		{"internal error is final", status.Error(codes.Internal, "boom"), http.StatusInternalServerError, true, "boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
				offer: func(*mediav1.WebRtcOfferRequest) (*mediav1.WebRtcOfferResponse, error) { return nil, tc.err },
			})
			result := control.WebRTCOffer(context.Background(), map[string]any{"feed_id": "sk-0001", "sdp": "v=0"})
			if result.OK() || result.StatusCode != tc.wantStatus || result.Terminal != tc.wantTerminal || result.Detail != tc.wantDetail {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestGRPCMediaControlOfferTransportFailureIsNotTerminal(t *testing.T) {
	control, stop := startFakeMediaControl(t, &fakeMediaControlServer{
		offer: func(*mediav1.WebRtcOfferRequest) (*mediav1.WebRtcOfferResponse, error) {
			return nil, status.Error(codes.Internal, "unreachable")
		},
	})
	stop()
	started := time.Now()
	result := control.WebRTCOffer(context.Background(), map[string]any{"feed_id": "sk-0001", "sdp": "v=0"})
	if result.OK() || result.Terminal {
		t.Fatalf("result = %#v, want an unavailable fallback", result)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("the unreachable service held the offer for %s", elapsed)
	}
}

func TestGRPCMediaControlOfferWithoutSDPIsNotAnAnswer(t *testing.T) {
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		offer: func(*mediav1.WebRtcOfferRequest) (*mediav1.WebRtcOfferResponse, error) {
			return &mediav1.WebRtcOfferResponse{Sdp: "  "}, nil
		},
	})
	if result := control.WebRTCOffer(context.Background(), map[string]any{"feed_id": "sk-0001", "sdp": "v=0"}); result.OK() {
		t.Fatalf("an empty SDP was accepted: %#v", result)
	}
}

func TestGRPCMediaControlFeedAudioStreamsBytesAndForwardsHeaders(t *testing.T) {
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		audio: func(request *mediav1.StreamFeedAudioRequest, stream grpc.ServerStreamingServer[mediav1.StreamFeedAudioResponse]) error {
			if request.GetFeedId() != "sk-0001" || request.GetCodec() != "raw" {
				return status.Error(codes.InvalidArgument, "unexpected request")
			}
			for _, chunk := range []string{"ab", "", "cde", "f"} {
				message := &mediav1.StreamFeedAudioResponse{Data: []byte(chunk)}
				if chunk == "ab" {
					message.ContentType = "audio/L16; rate=48000; channels=1"
				}
				if err := stream.Send(message); err != nil {
					return err
				}
			}
			return nil
		},
	})
	audio, err := control.FeedAudio(context.Background(), "sk-0001", "raw")
	if err != nil {
		t.Fatal(err)
	}
	defer audio.Body.Close()
	if audio.StatusCode != http.StatusOK || audio.Header.Get("Content-Type") != "audio/L16; rate=48000; channels=1" ||
		audio.Header.Get("Cache-Control") != "no-store" || audio.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("status %d headers %v", audio.StatusCode, audio.Header)
	}
	got, err := io.ReadAll(audio.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abcdef" {
		t.Fatalf("body = %q", got)
	}
}

func TestGRPCMediaControlFeedAudioRefusalIsAnError(t *testing.T) {
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		audio: func(*mediav1.StreamFeedAudioRequest, grpc.ServerStreamingServer[mediav1.StreamFeedAudioResponse]) error {
			return status.Error(codes.NotFound, "feed is not configured")
		},
	})
	audio, err := control.FeedAudio(context.Background(), "missing", "pcm16")
	if err == nil {
		_ = audio.Body.Close()
		t.Fatal("a refused stream was reported as open")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("error = %v", err)
	}
}

func TestGRPCMediaControlClosingTheBodyCancelsTheServerStream(t *testing.T) {
	cancelled := make(chan struct{})
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		audio: func(_ *mediav1.StreamFeedAudioRequest, stream grpc.ServerStreamingServer[mediav1.StreamFeedAudioResponse]) error {
			if err := stream.Send(&mediav1.StreamFeedAudioResponse{ContentType: "audio/wav", Data: []byte("x")}); err != nil {
				return err
			}
			<-stream.Context().Done()
			close(cancelled)
			return stream.Context().Err()
		},
	})
	audio, err := control.FeedAudio(context.Background(), "sk-0001", "pcm16")
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	if _, err := io.ReadFull(audio.Body, buffer); err != nil {
		t.Fatal(err)
	}
	_ = audio.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("the server stream kept running after the listener left")
	}
}

// A listener hanging up cancels the request context. The proxy tells that apart
// from a real failure with errors.Is(err, context.Canceled), as it does for the
// HTTP body, so the gRPC body must report the context error and not a status.
func TestGRPCMediaControlListenerHangUpReadsAsContextCanceled(t *testing.T) {
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		audio: func(_ *mediav1.StreamFeedAudioRequest, stream grpc.ServerStreamingServer[mediav1.StreamFeedAudioResponse]) error {
			if err := stream.Send(&mediav1.StreamFeedAudioResponse{ContentType: "audio/wav", Data: []byte("x")}); err != nil {
				return err
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		},
	})
	ctx, hangUp := context.WithCancel(context.Background())
	audio, err := control.FeedAudio(ctx, "sk-0001", "pcm16")
	if err != nil {
		t.Fatal(err)
	}
	defer audio.Body.Close()
	buffer := make([]byte, 1)
	if _, err := io.ReadFull(audio.Body, buffer); err != nil {
		t.Fatal(err)
	}

	readErr := make(chan error, 1)
	go func() {
		_, err := audio.Body.Read(buffer)
		readErr <- err
	}()
	hangUp()
	select {
	case err := <-readErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("read error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the read did not end when the listener left")
	}
}

func TestProxyMediaServiceAudioStreamsOverGRPC(t *testing.T) {
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		audio: func(request *mediav1.StreamFeedAudioRequest, stream grpc.ServerStreamingServer[mediav1.StreamFeedAudioResponse]) error {
			_ = stream.Send(&mediav1.StreamFeedAudioResponse{ContentType: "audio/wav", Data: []byte("RIFF")})
			return stream.Send(&mediav1.StreamFeedAudioResponse{Data: []byte("data")})
		},
	})
	grpcMediaControls.Store("127.0.0.1:18098", mediaControl(control))
	t.Cleanup(func() { grpcMediaControls.Delete("127.0.0.1:18098") })

	var config Config
	config.Services.Rust.Media.Enabled = true
	config.Services.Rust.Media.ControlTransport = "grpc"
	config.Services.Rust.Media.GRPCAddr = "127.0.0.1:18098"
	server := &Server{config: config}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/public/v1/feed/audio?feed=sk-0001", nil)
	if !server.proxyMediaServiceAudio(recorder, request, "sk-0001", "pcm16") {
		t.Fatal("the gRPC audio stream was not proxied")
	}
	result := recorder.Result()
	if result.StatusCode != http.StatusOK || result.Header.Get("Content-Type") != "audio/wav" ||
		result.Header.Get("X-Haze-Media-Backend") != "haze-media" {
		t.Fatalf("status %d headers %v", result.StatusCode, result.Header)
	}
	body, _ := io.ReadAll(result.Body)
	if string(body) != "RIFFdata" {
		t.Fatalf("body = %q", body)
	}

	refused, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		audio: func(*mediav1.StreamFeedAudioRequest, grpc.ServerStreamingServer[mediav1.StreamFeedAudioResponse]) error {
			return status.Error(codes.Unavailable, "no encoder")
		},
	})
	grpcMediaControls.Store("127.0.0.1:18098", mediaControl(refused))
	recorder = httptest.NewRecorder()
	if server.proxyMediaServiceAudio(recorder, request, "sk-0001", "opus") {
		t.Fatal("a refused stream was reported as proxied")
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("a refused stream wrote %q", recorder.Body.String())
	}
}

func TestHTTPMediaControlFeedAudioForwardsSafeHeadersAndRejectsErrors(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/feed/audio", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("feed") == "missing" {
			http.Error(writer, "feed is not configured", http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "audio/wav")
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("Set-Cookie", "must-not-forward=1")
		_, _ = writer.Write([]byte("RIFF"))
	})
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	control := httpMediaControl{baseURL: httpServer.URL}

	audio, err := control.FeedAudio(context.Background(), "sk-0001", "pcm16")
	if err != nil {
		t.Fatal(err)
	}
	defer audio.Body.Close()
	if audio.Header.Get("Content-Type") != "audio/wav" || audio.Header.Get("Cache-Control") != "no-store" || audio.Header.Get("Set-Cookie") != "" {
		t.Fatalf("headers = %v", audio.Header)
	}
	if body, _ := io.ReadAll(audio.Body); string(body) != "RIFF" {
		t.Fatalf("body = %q", body)
	}
	if _, err := control.FeedAudio(context.Background(), "missing", "pcm16"); err == nil {
		t.Fatal("a 404 was reported as an open stream")
	}
}

func TestMediaServiceControlSelectsTheConfiguredTransport(t *testing.T) {
	var config Config
	if mediaServiceControl(config) != nil {
		t.Fatal("a disabled media service produced a client")
	}
	config.Services.Rust.Media.Enabled = true
	config.Services.Rust.Media.Addr = "127.0.0.1:8097"

	for _, transport := range []string{"", "http", " HTTP ", "websocket"} {
		config.Services.Rust.Media.ControlTransport = transport
		control, ok := mediaServiceControl(config).(httpMediaControl)
		if !ok || control.baseURL != "http://127.0.0.1:8097" {
			t.Fatalf("transport %q selected %#v", transport, mediaServiceControl(config))
		}
	}

	config.Services.Rust.Media.ControlTransport = "gRPC"
	if got := mediaServiceGRPCAddr(config); got != defaultMediaGRPCAddr {
		t.Fatalf("default gRPC address = %q", got)
	}
	first := mediaServiceControl(config)
	if _, ok := first.(*grpcMediaControl); !ok {
		t.Fatalf("grpc transport selected %#v", first)
	}
	if second := mediaServiceControl(config); first != second {
		t.Fatal("the gRPC client was rebuilt instead of reused")
	}
	config.Services.Rust.Media.GRPCAddr = "http://127.0.0.1:19999"
	if got := mediaServiceGRPCAddr(config); got != "127.0.0.1:19999" {
		t.Fatalf("configured gRPC address = %q", got)
	}
}

func TestMediaHubSourcesPacedPCMOverGRPC(t *testing.T) {
	frame := make([]byte, opusFrameSamples*webrtcChannels*2)
	for index := range frame {
		frame[index] = byte(index%250) + 1
	}
	var streams atomic.Int32
	control, _ := startFakeMediaControl(t, &fakeMediaControlServer{
		audio: func(request *mediav1.StreamFeedAudioRequest, stream grpc.ServerStreamingServer[mediav1.StreamFeedAudioResponse]) error {
			if request.GetCodec() != "raw" {
				return status.Error(codes.InvalidArgument, "the hub asks for raw PCM")
			}
			streams.Add(1)
			if err := stream.Send(&mediav1.StreamFeedAudioResponse{ContentType: "audio/L16", Data: frame}); err != nil {
				return err
			}
			<-stream.Context().Done()
			return nil
		},
	})
	hub := NewMediaHub("")
	hub.SetAudioSource(control)
	if !hub.Available() {
		t.Fatal("a hub with an audio source reports itself unavailable")
	}
	subscription, cancel := hub.Subscribe("sk-0001")
	defer cancel()
	select {
	case chunk := <-subscription:
		if len(chunk.Data) != len(frame) || chunk.FeedID != "sk-0001" {
			t.Fatalf("chunk = feed %q, %d bytes", chunk.FeedID, len(chunk.Data))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no PCM arrived from the gRPC audio source")
	}
	hub.SetAudioSource(nil)
	if hub.Available() {
		t.Fatal("clearing the audio source left the hub available")
	}
}
