package webgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	mediav1 "github.com/meowraii/haze-weather-radio/services/go/internal/gen/media/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// haze-media control transports. Both listeners stay up in haze-media, and
// config.yaml (services.rust.media.control_transport) picks the one haze-web
// uses. See docs/grpc-migration-plan.md, Phase 1.
const (
	mediaControlTransportHTTP = "http"
	mediaControlTransportGRPC = "grpc"

	defaultMediaGRPCAddr     = "127.0.0.1:8098"
	mediaHealthTimeout       = 750 * time.Millisecond
	mediaGRPCKeepaliveTime   = 30 * time.Second
	mediaGRPCKeepaliveWait   = 10 * time.Second
	mediaGRPCReconnectBase   = 200 * time.Millisecond
	mediaGRPCReconnectMax    = 2 * time.Second
	mediaGRPCMaxMessageBytes = 4 * 1024 * 1024
)

// mediaControl is how haze-web calls the haze-media control surface. The HTTP
// and gRPC implementations behave the same way, so callers do not care which
// transport is configured.
type mediaControl interface {
	// Health returns haze-media's status document, or false when it is unreachable.
	Health(ctx context.Context) (map[string]any, bool)
	// WebRTCOffer asks haze-media to answer a listener's SDP offer.
	WebRTCOffer(ctx context.Context, payload map[string]any) mediaServiceWebRTCAnswerResult
	// FeedAudio opens a live audio stream. The caller must close the body.
	FeedAudio(ctx context.Context, feedID string, codec string) (*mediaAudioStream, error)
}

// mediaAudioStream is a live audio response from haze-media.
type mediaAudioStream struct {
	StatusCode int
	// Header carries the response headers that are safe to forward to a listener.
	Header http.Header
	Body   io.ReadCloser
}

// mediaServiceControl returns the configured control client, or nil when the
// Rust media service is disabled.
func mediaServiceControl(config Config) mediaControl {
	if !config.Services.Rust.Media.Enabled {
		return nil
	}
	if mediaServiceControlTransport(config) == mediaControlTransportGRPC {
		return sharedGRPCMediaControl(mediaServiceGRPCAddr(config))
	}
	return httpMediaControl{baseURL: mediaServiceBaseURL(config)}
}

// mediaServiceControlTransport reports the configured transport. Anything but
// "grpc" selects HTTP, so a typo can never leave haze-web without media.
func mediaServiceControlTransport(config Config) string {
	value := strings.ToLower(strings.TrimSpace(config.Services.Rust.Media.ControlTransport))
	switch value {
	case "", mediaControlTransportHTTP:
		return mediaControlTransportHTTP
	case mediaControlTransportGRPC:
		return mediaControlTransportGRPC
	default:
		warnUnknownMediaTransport(value)
		return mediaControlTransportHTTP
	}
}

var warnedMediaTransports sync.Map

func warnUnknownMediaTransport(value string) {
	if _, seen := warnedMediaTransports.LoadOrStore(value, struct{}{}); !seen {
		log.Printf("services.rust.media.control_transport %q is not http or grpc; using http", value)
	}
}

func mediaServiceGRPCAddr(config Config) string {
	addr := strings.TrimSpace(config.Services.Rust.Media.GRPCAddr)
	if addr == "" {
		return defaultMediaGRPCAddr
	}
	return strings.TrimPrefix(strings.TrimPrefix(addr, "http://"), "https://")
}

// httpMediaControl calls the haze-media HTTP routes.
type httpMediaControl struct {
	baseURL string
}

func (c httpMediaControl) Health(ctx context.Context) (map[string]any, bool) {
	if c.baseURL == "" {
		return nil, false
	}
	return mediaServiceHealthFromBaseURL(ctx, c.baseURL)
}

func (c httpMediaControl) WebRTCOffer(ctx context.Context, payload map[string]any) mediaServiceWebRTCAnswerResult {
	return mediaServiceWebRTCAnswerResultFromBase(ctx, c.baseURL, payload)
}

func (c httpMediaControl) FeedAudio(ctx context.Context, feedID string, codec string) (*mediaAudioStream, error) {
	endpoint, err := url.Parse(c.baseURL + "/api/v1/feed/audio")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("feed", feedID)
	query.Set("codec", codec)
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = response.Body.Close()
		return nil, fmt.Errorf("haze-media returned %s", response.Status)
	}
	header := http.Header{}
	for key, values := range response.Header {
		if !proxyAudioResponseHeader(key) {
			continue
		}
		for _, value := range values {
			header.Add(key, value)
		}
	}
	return &mediaAudioStream{StatusCode: response.StatusCode, Header: header, Body: response.Body}, nil
}

// unavailableMediaControl stands in when the gRPC client cannot even be built,
// which only happens for a malformed address. Every call reports the media
// service as unreachable.
type unavailableMediaControl struct {
	err error
}

func (c unavailableMediaControl) Health(context.Context) (map[string]any, bool) { return nil, false }

func (c unavailableMediaControl) WebRTCOffer(context.Context, map[string]any) mediaServiceWebRTCAnswerResult {
	return mediaServiceWebRTCAnswerResult{}
}

func (c unavailableMediaControl) FeedAudio(context.Context, string, string) (*mediaAudioStream, error) {
	return nil, c.err
}

var grpcMediaControls sync.Map

// sharedGRPCMediaControl returns the one client per address. A gRPC client owns
// a long-lived connection, so it is created once and reused by every request.
func sharedGRPCMediaControl(addr string) mediaControl {
	if existing, ok := grpcMediaControls.Load(addr); ok {
		return existing.(mediaControl)
	}
	control, err := newGRPCMediaControl(addr)
	var created mediaControl
	if err != nil {
		log.Printf("haze-media gRPC control client for %s could not be created: %v", addr, err)
		created = unavailableMediaControl{err: err}
	} else {
		created = control
	}
	actual, _ := grpcMediaControls.LoadOrStore(addr, created)
	return actual.(mediaControl)
}

// grpcMediaControl calls haze-media's MediaControlService.
type grpcMediaControl struct {
	conn   *grpc.ClientConn
	client mediav1.MediaControlServiceClient
}

func newGRPCMediaControl(addr string) (*grpcMediaControl, error) {
	conn, err := grpc.NewClient(
		"passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                mediaGRPCKeepaliveTime,
			Timeout:             mediaGRPCKeepaliveWait,
			PermitWithoutStream: true,
		}),
		// Recover quickly when haze-media restarts instead of backing off for minutes.
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  mediaGRPCReconnectBase,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   mediaGRPCReconnectMax,
			},
			MinConnectTimeout: time.Second,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(mediaGRPCMaxMessageBytes)),
	)
	if err != nil {
		return nil, err
	}
	return &grpcMediaControl{conn: conn, client: mediav1.NewMediaControlServiceClient(conn)}, nil
}

func (c *grpcMediaControl) Health(ctx context.Context) (map[string]any, bool) {
	checkCtx, cancel := context.WithTimeout(ctx, mediaHealthTimeout)
	defer cancel()
	response, err := c.client.Health(checkCtx, &mediav1.HealthRequest{})
	if err != nil {
		return nil, false
	}
	var health map[string]any
	if err := json.Unmarshal(response.GetStatusJson(), &health); err != nil {
		return nil, false
	}
	return health, true
}

func (c *grpcMediaControl) WebRTCOffer(ctx context.Context, payload map[string]any) mediaServiceWebRTCAnswerResult {
	offerCtx, cancel := context.WithTimeout(ctx, mediaServiceWebRTCOfferTimeout)
	defer cancel()
	response, err := c.client.WebRtcOffer(offerCtx, &mediav1.WebRtcOfferRequest{
		FeedId:         stringValue(payload, "feed_id"),
		Sdp:            stringValue(payload, "sdp"),
		SdpType:        stringValue(payload, "sdp_type"),
		PreferredCodec: stringValue(payload, "preferred_codec"),
		Codec:          stringValue(payload, "codec"),
		RequireOpus:    boolValue(payload, "require_opus"),
		DisableG722:    boolValue(payload, "disable_g722"),
		ClientIp:       stringValue(payload, "client_ip"),
		RemoteAddr:     stringValue(payload, "remote_addr"),
	})
	if err != nil {
		return mediaServiceWebRTCAnswerFromStatus(err)
	}
	if strings.TrimSpace(response.GetSdp()) == "" {
		return mediaServiceWebRTCAnswerResult{StatusCode: http.StatusOK}
	}
	return mediaServiceWebRTCAnswerResult{
		StatusCode: http.StatusOK,
		Answer: map[string]any{
			"feed_id":      response.GetFeedId(),
			"sdp":          response.GetSdp(),
			"sdp_type":     firstNonBlank(response.GetSdpType(), "answer"),
			"codec":        response.GetCodec(),
			"payload_type": response.GetPayloadType(),
			"media_recent": response.GetMediaRecent(),
		},
	}
}

// mediaServiceWebRTCAnswerFromStatus applies the same rules as the HTTP route
// to a gRPC error. Not found and unavailable mean haze-media cannot serve the
// offer, so the caller may fall back. Anything else is a refusal of this offer.
func mediaServiceWebRTCAnswerFromStatus(err error) mediaServiceWebRTCAnswerResult {
	st, ok := status.FromError(err)
	if !ok {
		log.Printf("haze-media WebRTC unavailable: %v", err)
		return mediaServiceWebRTCAnswerResult{}
	}
	switch st.Code() {
	case codes.NotFound:
		return mediaServiceWebRTCAnswerResult{StatusCode: http.StatusNotFound}
	case codes.Unavailable:
		// The transport failing and haze-media declining both arrive as Unavailable.
		return mediaServiceWebRTCAnswerResult{StatusCode: http.StatusServiceUnavailable}
	case codes.DeadlineExceeded, codes.Canceled:
		log.Printf("haze-media WebRTC unavailable: %v", err)
		return mediaServiceWebRTCAnswerResult{}
	}
	log.Printf("haze-media WebRTC returned %s", st.Code())
	return mediaServiceWebRTCAnswerResult{
		StatusCode: httpStatusForCode(st.Code()),
		Detail:     firstNonBlank(st.Message(), st.Code().String()),
		Terminal:   true,
	}
}

// httpStatusForCode is the inverse of the status mapping in haze-media's gRPC
// listener and in contracts/media/v1/control.proto.
func httpStatusForCode(code codes.Code) int {
	switch code {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.Unimplemented:
		return http.StatusUnsupportedMediaType
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func (c *grpcMediaControl) FeedAudio(ctx context.Context, feedID string, codec string) (*mediaAudioStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.client.StreamFeedAudio(streamCtx, &mediav1.StreamFeedAudioRequest{FeedId: feedID, Codec: codec})
	if err != nil {
		cancel()
		return nil, err
	}
	// The first message carries the content type, and a refusal arrives here too.
	first, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, err
	}
	header := http.Header{}
	header.Set("Content-Type", firstNonBlank(first.GetContentType(), "application/octet-stream"))
	header.Set("Cache-Control", "no-store")
	header.Set("X-Accel-Buffering", "no")
	return &mediaAudioStream{
		StatusCode: http.StatusOK,
		Header:     header,
		Body:       &grpcAudioBody{stream: stream, cancel: cancel, pending: first.GetData()},
	}, nil
}

// grpcAudioBody presents a gRPC audio stream as a byte stream.
type grpcAudioBody struct {
	stream  grpc.ServerStreamingClient[mediav1.StreamFeedAudioResponse]
	cancel  context.CancelFunc
	pending []byte
}

func (b *grpcAudioBody) Read(p []byte) (int, error) {
	for len(b.pending) == 0 {
		message, err := b.stream.Recv()
		if err != nil {
			// A listener leaving cancels the stream and gRPC reports that as a
			// status. Return the context error, as an HTTP body does, so callers
			// can tell a hang-up from a failure.
			if ctxErr := b.stream.Context().Err(); ctxErr != nil && !errors.Is(err, io.EOF) {
				return 0, ctxErr
			}
			return 0, err
		}
		b.pending = message.GetData()
	}
	n := copy(p, b.pending)
	b.pending = b.pending[n:]
	return n, nil
}

func (b *grpcAudioBody) Close() error {
	b.cancel()
	return nil
}
