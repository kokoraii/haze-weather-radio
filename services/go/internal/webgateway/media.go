package webgateway

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotranspile/g722"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/meowraii/haze-weather-radio/services/go/internal/bridgeclient"
)

const (
	opusSampleRate             = 48000
	opusFrameSamples           = opusSampleRate / 50
	opusBitrateBPS             = 32000
	g722SampleRate             = 16000
	webrtcRTPClockRate         = 8000
	pcmuSampleRate             = 8000
	pcmuFrameSamples           = pcmuSampleRate / 50
	webrtcChannels             = 1
	opusRTPChannels            = 2
	opusEncoderChannels        = opusRTPChannels
	webrtcFrameDuration        = 20 * time.Millisecond
	webrtcMaxQueuedFrames      = 10
	webrtcPeerFrameMailbox     = 3
	webrtcResumeQueuedFrames   = 2
	webrtcConcealmentFrames    = 5
	feedIngressCapacity        = 4
	g722FrameSamples           = g722SampleRate / 50
	bridgeReconnectDelay       = 750 * time.Millisecond
	webrtcDisconnectGrace      = 15 * time.Second
	webrtcDiagnosticsInterval  = 30 * time.Second
	webrtcWriteTimeout         = 3 * time.Second
	webrtcMediaStartFallback   = 750 * time.Millisecond
	webrtcFrameSourceIdleGrace = 15 * time.Second
	webrtcLateWriteThreshold   = 2 * webrtcFrameDuration
	webrtcPeerSourceWait       = 0
	webrtcFrameSourceResetGap  = 100 * time.Millisecond
	webrtcPeerPacingResetGap   = 100 * time.Millisecond
)

type webRTCAudioCodec int

const (
	webRTCAudioOpus webRTCAudioCodec = iota
	webRTCAudioG722
	webRTCAudioPCMU
	webRTCAudioPCMA
)

func (c webRTCAudioCodec) String() string {
	switch c {
	case webRTCAudioOpus:
		return "opus"
	case webRTCAudioG722:
		return "g722"
	case webRTCAudioPCMU:
		return "pcmu"
	case webRTCAudioPCMA:
		return "pcma"
	default:
		return "unknown"
	}
}

func defaultWebRTCAudioCodec() webRTCAudioCodec {
	if codec, ok := parseWebRTCAudioCodec(os.Getenv("HAZE_WEBRTC_DEFAULT_CODEC")); ok {
		return codec
	}
	return webRTCAudioOpus
}

func WebRTCAudioCapabilities() map[string]any {
	return map[string]any{
		"webrtc_opus":          opusBackendAvailable(),
		"webrtc_default_codec": defaultWebRTCAudioCodec().String(),
		"webrtc_codecs":        []string{"auto", "opus", "g722", "pcmu", "pcma"},
	}
}

type WebRTCAnswerOptions struct {
	DisableG722    bool
	RequireOpus    bool
	PreferredCodec string
	OnClose        func()
}

type WebRTCAnswer struct {
	SDP         string
	Codec       webRTCAudioCodec
	PayloadType uint8
	MediaRecent bool
}

type webRTCPeerSnapshot struct {
	PeerID               string
	FeedID               string
	Codec                string
	PayloadType          uint8
	StartedAt            time.Time
	UpdatedAt            time.Time
	Written              uint64
	FillerFrames         uint64
	ConcealedFrames      uint64
	SkippedFrames        uint64
	SourceGapFrames      uint64
	LateWrites           uint64
	WriteErrors          uint64
	TotalWritten         uint64
	TotalFillerFrames    uint64
	TotalConcealedFrames uint64
	TotalSkippedFrames   uint64
	TotalSourceGapFrames uint64
	TotalLateWrites      uint64
	TotalWriteErrors     uint64
	MaxWriteGapMS        int64
	SequenceNumber       uint16
	Timestamp            uint32
	LastPayloadBytes     int
	LastWriteAt          time.Time
}

type webRTCFrameSourceSnapshot struct {
	FeedID         string
	Codec          string
	StartedAt      time.Time
	UpdatedAt      time.Time
	Subscribers    int
	QueuedFrames   int
	Produced       uint64
	Real           uint64
	Concealed      uint64
	Idle           uint64
	Dropped        uint64
	TotalProduced  uint64
	TotalReal      uint64
	TotalConcealed uint64
	TotalIdle      uint64
	TotalDropped   uint64
	LastKind       string
	LastPayload    int
	LastFrameAt    time.Time
}

type opusFrameEncoder interface {
	Encode([]int16) ([]byte, error)
}

type PCMChunk struct {
	FeedID     string
	SampleRate int
	Channels   int
	Duration   time.Duration
	Data       []byte
}

type MediaHub struct {
	addr          string
	sourceControl mediaControl
	sourceFeeds   map[string]context.CancelFunc
	mu            sync.Mutex
	peerStatsMu   sync.Mutex
	subscribers   map[string]map[chan PCMChunk]struct{}
	peers         map[string]*webrtc.PeerConnection
	peerStats     map[string]webRTCPeerSnapshot
	ingress       map[string]chan PCMChunk
	frameSources  map[webRTCFrameSourceKey]*webRTCFrameSource
	last          map[string]PCMChunk
	lastAt        map[string]time.Time
	seenLogged    map[string]bool
}

func NewMediaHub(addr string) *MediaHub {
	hub := &MediaHub{
		addr:         strings.TrimSpace(addr),
		subscribers:  map[string]map[chan PCMChunk]struct{}{},
		peers:        map[string]*webrtc.PeerConnection{},
		peerStats:    map[string]webRTCPeerSnapshot{},
		sourceFeeds:  map[string]context.CancelFunc{},
		ingress:      map[string]chan PCMChunk{},
		frameSources: map[webRTCFrameSourceKey]*webRTCFrameSource{},
		last:         map[string]PCMChunk{},
		lastAt:       map[string]time.Time{},
		seenLogged:   map[string]bool{},
	}
	if hub.addr != "" {
		go hub.run(context.Background())
	}
	return hub
}

func (h *MediaHub) Available() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.addr != "" || h.sourceControl != nil
}

// SetHTTPSource sources paced PCM from haze-media's HTTP audio route. An empty
// URL clears the source.
func (h *MediaHub) SetHTTPSource(baseURL string) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		h.SetAudioSource(nil)
		return
	}
	h.SetAudioSource(httpMediaControl{baseURL: baseURL})
}

// SetAudioSource sources paced PCM from haze-media over the given control
// client. Nil clears the source.
func (h *MediaHub) SetAudioSource(control mediaControl) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.sourceControl = control
	if h.sourceFeeds == nil {
		h.sourceFeeds = map[string]context.CancelFunc{}
	}
	h.mu.Unlock()
}

func (h *MediaHub) ensureHTTPSource(feedID string) {
	if h == nil {
		return
	}
	if strings.TrimSpace(h.addr) != "" {
		return
	}
	feedID = strings.TrimSpace(feedID)
	if feedID == "" {
		return
	}
	h.mu.Lock()
	control := h.sourceControl
	if control == nil {
		h.mu.Unlock()
		return
	}
	if h.sourceFeeds == nil {
		h.sourceFeeds = map[string]context.CancelFunc{}
	}
	if h.sourceFeeds[feedID] != nil {
		h.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.sourceFeeds[feedID] = cancel
	h.mu.Unlock()
	go h.runPCMSource(ctx, control, feedID)
}

func (h *MediaHub) runPCMSource(ctx context.Context, control mediaControl, feedID string) {
	frameBytes := opusFrameSamples * webrtcChannels * 2
	if frameBytes <= 0 {
		frameBytes = 1920
	}
	for ctx.Err() == nil {
		response, err := control.FeedAudio(ctx, feedID, "raw")
		if err != nil {
			sleepMedia(ctx, bridgeReconnectDelay)
			continue
		}
		log.Printf("media hub sourcing paced PCM for feed %s from haze-media", feedID)
		buffer := make([]byte, frameBytes)
		for ctx.Err() == nil {
			n, readErr := io.ReadFull(response.Body, buffer)
			if n == frameBytes {
				data := append([]byte(nil), buffer[:n]...)
				h.publish(PCMChunk{
					FeedID:     feedID,
					SampleRate: opusSampleRate,
					Channels:   webrtcChannels,
					Duration:   webrtcFrameDuration,
					Data:       data,
				})
			}
			if readErr != nil {
				break
			}
		}
		_ = response.Body.Close()
		sleepMedia(ctx, bridgeReconnectDelay)
	}
}

func (h *MediaHub) WebRTCPeerSnapshots() []map[string]any {
	if h == nil {
		return nil
	}
	h.peerStatsMu.Lock()
	defer h.peerStatsMu.Unlock()
	out := make([]map[string]any, 0, len(h.peerStats))
	for _, snapshot := range h.peerStats {
		lastWriteAgeMS := int64(-1)
		if !snapshot.LastWriteAt.IsZero() {
			lastWriteAgeMS = time.Since(snapshot.LastWriteAt).Milliseconds()
		}
		out = append(out, map[string]any{
			"peer_id":                    snapshot.PeerID,
			"feed_id":                    snapshot.FeedID,
			"codec":                      snapshot.Codec,
			"payload_type":               snapshot.PayloadType,
			"started_at":                 snapshot.StartedAt,
			"updated_at":                 snapshot.UpdatedAt,
			"interval_written":           snapshot.Written,
			"interval_filler_frames":     snapshot.FillerFrames,
			"interval_concealed_frames":  snapshot.ConcealedFrames,
			"interval_skipped_frames":    snapshot.SkippedFrames,
			"interval_source_gap_frames": snapshot.SourceGapFrames,
			"interval_late_writes":       snapshot.LateWrites,
			"interval_write_errors":      snapshot.WriteErrors,
			"total_written":              snapshot.TotalWritten,
			"total_filler_frames":        snapshot.TotalFillerFrames,
			"total_concealed_frames":     snapshot.TotalConcealedFrames,
			"total_skipped_frames":       snapshot.TotalSkippedFrames,
			"total_source_gap_frames":    snapshot.TotalSourceGapFrames,
			"total_late_writes":          snapshot.TotalLateWrites,
			"total_write_errors":         snapshot.TotalWriteErrors,
			"max_write_gap_ms":           snapshot.MaxWriteGapMS,
			"next_seq":                   snapshot.SequenceNumber,
			"next_ts":                    snapshot.Timestamp,
			"last_payload_bytes":         snapshot.LastPayloadBytes,
			"last_write_at":              snapshot.LastWriteAt,
			"last_write_age_ms":          lastWriteAgeMS,
		})
	}
	return out
}

func (h *MediaHub) WebRTCFrameSourceSnapshots() []map[string]any {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	sources := make([]*webRTCFrameSource, 0, len(h.frameSources))
	for _, source := range h.frameSources {
		sources = append(sources, source)
	}
	h.mu.Unlock()
	out := make([]map[string]any, 0, len(sources))
	for _, source := range sources {
		snapshot := source.snapshot()
		lastFrameAgeMS := int64(-1)
		if !snapshot.LastFrameAt.IsZero() {
			lastFrameAgeMS = time.Since(snapshot.LastFrameAt).Milliseconds()
		}
		out = append(out, map[string]any{
			"feed_id":            snapshot.FeedID,
			"codec":              snapshot.Codec,
			"started_at":         snapshot.StartedAt,
			"updated_at":         snapshot.UpdatedAt,
			"subscribers":        snapshot.Subscribers,
			"queued_frames":      snapshot.QueuedFrames,
			"interval_produced":  snapshot.Produced,
			"interval_real":      snapshot.Real,
			"interval_concealed": snapshot.Concealed,
			"interval_idle":      snapshot.Idle,
			"interval_dropped":   snapshot.Dropped,
			"total_produced":     snapshot.TotalProduced,
			"total_real":         snapshot.TotalReal,
			"total_concealed":    snapshot.TotalConcealed,
			"total_idle":         snapshot.TotalIdle,
			"total_dropped":      snapshot.TotalDropped,
			"last_kind":          snapshot.LastKind,
			"last_payload_bytes": snapshot.LastPayload,
			"last_frame_at":      snapshot.LastFrameAt,
			"last_frame_age_ms":  lastFrameAgeMS,
		})
	}
	return out
}

func (h *MediaHub) Answer(ctx context.Context, feedID string, offerSDP string) (string, error) {
	answer, err := h.AnswerWithOptions(ctx, feedID, offerSDP, WebRTCAnswerOptions{})
	if err != nil {
		return "", err
	}
	return answer.SDP, nil
}

func (h *MediaHub) AnswerWithOptions(ctx context.Context, feedID string, offerSDP string, options WebRTCAnswerOptions) (WebRTCAnswer, error) {
	feedID = strings.TrimSpace(feedID)
	if !h.Available() {
		return WebRTCAnswer{}, errors.New("media bridge is not available")
	}
	if feedID == "" {
		return WebRTCAnswer{}, errors.New("feed_id is required")
	}
	if strings.TrimSpace(offerSDP) == "" {
		return WebRTCAnswer{}, errors.New("sdp is required")
	}
	h.ensureHTTPSource(feedID)

	peerConnection, err := newWebRTCPeerConnection(webrtc.Configuration{})
	if err != nil {
		return WebRTCAnswer{}, err
	}
	mediaRecent := h.HasRecentPCM(feedID, 5*time.Second)
	if !mediaRecent {
		log.Printf("media bridge has no recent PCM for feed %s; WebRTC peer will receive silence until playout publishes audio", feedID)
	}
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	peerID := fmt.Sprintf("%s-%d", mediaSafeID(feedID), time.Now().UnixNano())
	h.mu.Lock()
	h.peers[peerID] = peerConnection
	h.mu.Unlock()
	var cleanupOnce sync.Once
	var disconnectMu sync.Mutex
	var disconnectTimer *time.Timer
	var cleanup func()
	mediaReady := make(chan struct{})
	var mediaReadyOnce sync.Once
	markMediaReady := func() {
		mediaReadyOnce.Do(func() {
			close(mediaReady)
		})
	}
	stopDisconnectTimer := func() {
		disconnectMu.Lock()
		defer disconnectMu.Unlock()
		if disconnectTimer != nil {
			disconnectTimer.Stop()
			disconnectTimer = nil
		}
	}
	scheduleDisconnectCleanup := func() {
		disconnectMu.Lock()
		defer disconnectMu.Unlock()
		if disconnectTimer != nil {
			return
		}
		disconnectTimer = time.AfterFunc(webrtcDisconnectGrace, func() {
			if h.shouldCleanupDisconnectedWebRTCPeer(peerID, peerConnection.ConnectionState(), peerConnection.ICEConnectionState(), time.Now()) {
				cleanup()
			}
			disconnectMu.Lock()
			disconnectTimer = nil
			disconnectMu.Unlock()
		})
	}
	cleanup = func() {
		cleanupOnce.Do(func() {
			if options.OnClose != nil {
				options.OnClose()
			}
			stopDisconnectTimer()
			cancelPeer()
			h.mu.Lock()
			delete(h.peers, peerID)
			h.mu.Unlock()
			h.removeWebRTCPeerSnapshot(peerID)
			_ = peerConnection.Close()
		})
	}

	codec, err := preferredWebRTCAudioCodec(offerSDP, options)
	if err != nil {
		cleanup()
		return WebRTCAnswer{}, err
	}
	payloadType := offeredAudioPayloadType(offerSDP, codec)
	capability := webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: opusSampleRate, Channels: opusRTPChannels}
	if codec == webRTCAudioG722 {
		capability = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeG722, ClockRate: webrtcRTPClockRate, Channels: webrtcChannels}
	} else if codec == webRTCAudioPCMU {
		capability = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: pcmuSampleRate, Channels: webrtcChannels}
	} else if codec == webRTCAudioPCMA {
		capability = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: pcmuSampleRate, Channels: webrtcChannels}
	}
	track, err := webrtc.NewTrackLocalStaticRTP(capability, "haze-"+mediaSafeID(feedID), "haze-"+mediaSafeID(feedID))
	if err != nil {
		cleanup()
		return WebRTCAnswer{}, err
	}
	transceiver, err := peerConnection.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		cleanup()
		return WebRTCAnswer{}, err
	}
	if err := transceiver.SetCodecPreferences(codecPreferences(codec, payloadType)); err != nil {
		cleanup()
		return WebRTCAnswer{}, err
	}
	sender := transceiver.Sender()
	go drainRTCP(sender)

	peerConnection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if shouldCleanupWebRTCPeer(state) {
			cleanup()
			return
		}
		if shouldStartWebRTCMedia(state, peerConnection.ICEConnectionState()) {
			markMediaReady()
		}
		switch state {
		case webrtc.PeerConnectionStateConnected:
			stopDisconnectTimer()
		case webrtc.PeerConnectionStateDisconnected:
			scheduleDisconnectCleanup()
		}
	})
	peerConnection.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		if shouldCleanupWebRTCICE(state) {
			cleanup()
			return
		}
		if shouldStartWebRTCMedia(peerConnection.ConnectionState(), state) {
			markMediaReady()
		}
		switch state {
		case webrtc.ICEConnectionStateConnected, webrtc.ICEConnectionStateCompleted:
			stopDisconnectTimer()
		case webrtc.ICEConnectionStateDisconnected:
			scheduleDisconnectCleanup()
		}
	})

	if err := peerConnection.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	}); err != nil {
		cleanup()
		return WebRTCAnswer{}, err
	}
	answer, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		cleanup()
		return WebRTCAnswer{}, err
	}
	gatheringComplete := webrtc.GatheringCompletePromise(peerConnection)
	if err := peerConnection.SetLocalDescription(answer); err != nil {
		cleanup()
		return WebRTCAnswer{}, err
	}
	select {
	case <-gatheringComplete:
	case <-ctx.Done():
		cleanup()
		return WebRTCAnswer{}, ctx.Err()
	case <-time.After(5 * time.Second):
	}
	localDescription := peerConnection.LocalDescription()
	if localDescription == nil {
		cleanup()
		return WebRTCAnswer{}, errors.New("could not create local WebRTC description")
	}

	frames, unsubscribeFrames, err := h.SubscribeWebRTCFrames(feedID, codec)
	if err != nil {
		cleanup()
		return WebRTCAnswer{}, err
	}
	h.recordWebRTCPeerSnapshot(peerID, webRTCPeerSnapshot{
		PeerID:      peerID,
		FeedID:      feedID,
		Codec:       codec.String(),
		PayloadType: payloadType,
		StartedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	})
	go h.streamWebRTCFrames(peerCtx, peerID, feedID, codec, payloadType, track, frames, unsubscribeFrames, mediaReady, cleanup)
	go markWebRTCMediaReadyWhenConnectedAfter(peerCtx, webrtcMediaStartFallback, webrtcMediaStartFallback/3, func() bool {
		return shouldStartWebRTCMedia(peerConnection.ConnectionState(), peerConnection.ICEConnectionState())
	}, markMediaReady)
	return WebRTCAnswer{SDP: localDescription.SDP, Codec: codec, PayloadType: payloadType, MediaRecent: mediaRecent}, nil
}

func shouldCleanupWebRTCPeer(state webrtc.PeerConnectionState) bool {
	return state == webrtc.PeerConnectionStateClosed || state == webrtc.PeerConnectionStateFailed
}

func shouldCleanupWebRTCICE(state webrtc.ICEConnectionState) bool {
	return state == webrtc.ICEConnectionStateClosed || state == webrtc.ICEConnectionStateFailed
}

func shouldCleanupDisconnectedWebRTC(peerState webrtc.PeerConnectionState, iceState webrtc.ICEConnectionState) bool {
	return peerState == webrtc.PeerConnectionStateDisconnected || iceState == webrtc.ICEConnectionStateDisconnected
}

func (h *MediaHub) shouldCleanupDisconnectedWebRTCPeer(peerID string, peerState webrtc.PeerConnectionState, iceState webrtc.ICEConnectionState, now time.Time) bool {
	if !shouldCleanupDisconnectedWebRTC(peerState, iceState) {
		return false
	}
	return !h.webRTCPeerHasRecentWrite(peerID, now, webrtcDisconnectGrace)
}

func shouldStartWebRTCMedia(peerState webrtc.PeerConnectionState, iceState webrtc.ICEConnectionState) bool {
	return peerState == webrtc.PeerConnectionStateConnected ||
		iceState == webrtc.ICEConnectionStateConnected ||
		iceState == webrtc.ICEConnectionStateCompleted
}

func markWebRTCMediaReadyAfter(ctx context.Context, delay time.Duration, mark func()) {
	markWebRTCMediaReadyWhenConnectedAfter(ctx, delay, delay, func() bool { return true }, mark)
}

func markWebRTCMediaReadyWhenConnectedAfter(ctx context.Context, delay time.Duration, retryEvery time.Duration, ready func() bool, mark func()) {
	if mark == nil {
		return
	}
	if delay <= 0 {
		if ready == nil || ready() {
			mark()
		}
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if ready == nil || ready() {
				mark()
				return
			}
			if retryEvery <= 0 {
				retryEvery = delay
			}
			timer.Reset(retryEvery)
		}
	}
}

func newWebRTCPeerConnection(configuration webrtc.Configuration) (*webrtc.PeerConnection, error) {
	var mediaEngine webrtc.MediaEngine
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	var registry interceptor.Registry
	if err := webrtc.RegisterDefaultInterceptors(&mediaEngine, &registry); err != nil {
		return nil, err
	}
	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(&mediaEngine),
		webrtc.WithInterceptorRegistry(&registry),
	)
	return api.NewPeerConnection(configuration)
}

func preferredWebRTCAudioCodec(offerSDP string, options WebRTCAnswerOptions) (webRTCAudioCodec, error) {
	upper := strings.ToUpper(offerSDP)
	if preferred, ok := parseWebRTCAudioCodec(options.PreferredCodec); ok {
		return requiredWebRTCAudioCodec(upper, preferred)
	}
	if options.RequireOpus {
		return requiredWebRTCAudioCodec(upper, webRTCAudioOpus)
	}
	if codec, err := requiredWebRTCAudioCodec(upper, defaultWebRTCAudioCodec()); err == nil {
		return codec, nil
	}
	if !options.DisableG722 && (strings.Contains(upper, "G722/8000") || strings.Contains(upper, " G722")) {
		return webRTCAudioG722, nil
	}
	if strings.Contains(upper, "PCMU/8000") {
		return webRTCAudioPCMU, nil
	}
	if strings.Contains(upper, "PCMA/8000") {
		return webRTCAudioPCMA, nil
	}
	if opusBackendAvailable() && strings.Contains(upper, "OPUS/48000") {
		return webRTCAudioOpus, nil
	}
	return webRTCAudioPCMU, nil
}

func parseWebRTCAudioCodec(value string) (webRTCAudioCodec, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return webRTCAudioOpus, false
	case "opus":
		return webRTCAudioOpus, true
	case "g722", "g.722":
		return webRTCAudioG722, true
	case "pcmu", "mulaw", "ulaw", "u-law":
		return webRTCAudioPCMU, true
	case "pcma", "alaw", "a-law":
		return webRTCAudioPCMA, true
	default:
		return webRTCAudioOpus, false
	}
}

func requiredWebRTCAudioCodec(upperOfferSDP string, codec webRTCAudioCodec) (webRTCAudioCodec, error) {
	switch codec {
	case webRTCAudioOpus:
		if !strings.Contains(upperOfferSDP, "OPUS/48000") {
			return webRTCAudioOpus, errors.New("receiver requires Opus but the offer did not include Opus")
		}
		if !opusBackendAvailable() {
			return webRTCAudioOpus, errors.New("receiver requires Opus but this gateway was built without an Opus encoder")
		}
		return webRTCAudioOpus, nil
	case webRTCAudioG722:
		if !strings.Contains(upperOfferSDP, "G722/8000") && !strings.Contains(upperOfferSDP, " G722") {
			return webRTCAudioG722, errors.New("receiver requires G.722 but the offer did not include G.722")
		}
		return webRTCAudioG722, nil
	case webRTCAudioPCMU:
		if !strings.Contains(upperOfferSDP, "PCMU/8000") {
			return webRTCAudioPCMU, errors.New("receiver requires PCMU but the offer did not include PCMU")
		}
		return webRTCAudioPCMU, nil
	case webRTCAudioPCMA:
		if !strings.Contains(upperOfferSDP, "PCMA/8000") {
			return webRTCAudioPCMA, errors.New("receiver requires PCMA but the offer did not include PCMA")
		}
		return webRTCAudioPCMA, nil
	default:
		return webRTCAudioOpus, errors.New("unsupported requested WebRTC audio codec")
	}
}

func offeredAudioPayloadType(offerSDP string, codec webRTCAudioCodec) uint8 {
	target := ""
	fallback := uint8(0)
	switch codec {
	case webRTCAudioOpus:
		target = "OPUS/48000"
		fallback = 111
	case webRTCAudioG722:
		target = "G722/8000"
		fallback = 9
	case webRTCAudioPCMU:
		target = "PCMU/8000"
		fallback = 0
	case webRTCAudioPCMA:
		target = "PCMA/8000"
		fallback = 8
	default:
		target = "PCMU/8000"
		fallback = 0
	}
	for _, line := range strings.Split(offerSDP, "\n") {
		line = strings.TrimSpace(line)
		upper := strings.ToUpper(line)
		if !strings.HasPrefix(upper, "A=RTPMAP:") || !strings.Contains(upper, target) {
			continue
		}
		start := len("a=rtpmap:")
		end := strings.Index(line[start:], " ")
		if end < 0 {
			continue
		}
		var parsed uint64
		if _, err := fmt.Sscanf(line[start:start+end], "%d", &parsed); err == nil && parsed <= 127 {
			return uint8(parsed)
		}
	}
	return fallback
}

func codecPreferences(codec webRTCAudioCodec, payloadType uint8) []webrtc.RTPCodecParameters {
	switch codec {
	case webRTCAudioOpus:
		if payloadType == 0 {
			payloadType = 111
		}
		return []webrtc.RTPCodecParameters{{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    webrtc.MimeTypeOpus,
				ClockRate:   opusSampleRate,
				Channels:    opusRTPChannels,
				SDPFmtpLine: "minptime=10;useinbandfec=1",
			},
			PayloadType: webrtc.PayloadType(payloadType),
		}}
	case webRTCAudioG722:
		if payloadType == 0 {
			payloadType = 9
		}
		return []webrtc.RTPCodecParameters{{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeG722,
				ClockRate: webrtcRTPClockRate,
				Channels:  webrtcChannels,
			},
			PayloadType: webrtc.PayloadType(payloadType),
		}}
	case webRTCAudioPCMU:
		return []webrtc.RTPCodecParameters{{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypePCMU,
				ClockRate: pcmuSampleRate,
				Channels:  webrtcChannels,
			},
			PayloadType: webrtc.PayloadType(payloadType),
		}}
	case webRTCAudioPCMA:
		if payloadType == 0 {
			payloadType = 8
		}
		return []webrtc.RTPCodecParameters{{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypePCMA,
				ClockRate: pcmuSampleRate,
				Channels:  webrtcChannels,
			},
			PayloadType: webrtc.PayloadType(payloadType),
		}}
	default:
		return nil
	}
}

func (h *MediaHub) run(ctx context.Context) {
	bridgeclient.Reconnect(ctx, bridgeclient.Options{Addr: h.addr}, bridgeReconnectDelay, h.readBridge)
}

func (h *MediaHub) readBridge(ctx context.Context, conn *bridgeclient.Conn) {
	_ = conn.Scan(func(line []byte) bool {
		if ctx.Err() != nil {
			return false
		}
		if chunk, ok := decodePCMBridgeEvent(line); ok {
			h.publish(chunk)
		}
		return true
	})
}

func decodePCMBridgeEvent(raw []byte) (PCMChunk, bool) {
	var event struct {
		Type   string `json:"type"`
		FeedID string `json:"feed_id"`
		Data   struct {
			FeedID     string `json:"feed_id"`
			SampleRate int    `json:"sample_rate"`
			Channels   int    `json:"channels"`
			DurationMS int    `json:"duration_ms"`
			PCM        string `json:"pcm"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &event); err != nil || event.Type != "playout.pcm" {
		return PCMChunk{}, false
	}
	feedID := strings.TrimSpace(event.Data.FeedID)
	if feedID == "" {
		feedID = strings.TrimSpace(event.FeedID)
	}
	if feedID == "" || event.Data.PCM == "" {
		return PCMChunk{}, false
	}
	pcm, err := base64.StdEncoding.DecodeString(event.Data.PCM)
	if err != nil {
		return PCMChunk{}, false
	}
	sampleRate := event.Data.SampleRate
	if sampleRate <= 0 {
		sampleRate = opusSampleRate
	}
	channels := event.Data.Channels
	if channels <= 0 {
		channels = 1
	}
	duration := time.Duration(event.Data.DurationMS) * time.Millisecond
	if duration <= 0 {
		duration = webrtcFrameDuration
	}
	chunk := PCMChunk{
		FeedID:     feedID,
		SampleRate: sampleRate,
		Channels:   channels,
		Duration:   duration,
		Data:       pcm,
	}
	chunk, ok, _ := validatePCMChunk(chunk)
	return chunk, ok
}

func (h *MediaHub) publish(chunk PCMChunk) {
	chunk, ok, reason := validatePCMChunk(chunk)
	if !ok {
		if reason != "" {
			log.Printf("media bridge rejected PCM for feed %s: %s", chunk.FeedID, reason)
		}
		return
	}
	ingress := h.feedIngress(chunk.FeedID)
	select {
	case ingress <- chunk:
	default:
		select {
		case <-ingress:
		default:
		}
		select {
		case ingress <- chunk:
		default:
		}
	}
}

func (h *MediaHub) feedIngress(feedID string) chan PCMChunk {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ingress == nil {
		h.ingress = map[string]chan PCMChunk{}
	}
	ingress := h.ingress[feedID]
	if ingress == nil {
		ingress = make(chan PCMChunk, feedIngressCapacity)
		h.ingress[feedID] = ingress
		go h.runFeedIngress(feedID, ingress)
	}
	return ingress
}

func (h *MediaHub) runFeedIngress(_ string, ingress <-chan PCMChunk) {
	for chunk := range ingress {
		h.publishReady(chunk)
	}
}

func (h *MediaHub) publishReady(chunk PCMChunk) {
	now := time.Now()
	var subscribers []chan PCMChunk
	h.mu.Lock()
	if h.last == nil {
		h.last = map[string]PCMChunk{}
	}
	if h.lastAt == nil {
		h.lastAt = map[string]time.Time{}
	}
	if h.seenLogged == nil {
		h.seenLogged = map[string]bool{}
	}
	h.last[chunk.FeedID] = chunk
	h.lastAt[chunk.FeedID] = now
	if !h.seenLogged[chunk.FeedID] {
		h.seenLogged[chunk.FeedID] = true
		log.Printf("media bridge receiving PCM for feed %s (%d Hz, %d channel)", chunk.FeedID, chunk.SampleRate, chunk.Channels)
	}
	for subscriber := range h.subscribers[chunk.FeedID] {
		subscribers = append(subscribers, subscriber)
	}
	h.mu.Unlock()

	for _, subscriber := range subscribers {
		deliverPCMToSubscriber(subscriber, chunk)
	}
}

func deliverPCMToSubscriber(subscriber chan PCMChunk, chunk PCMChunk) {
	select {
	case subscriber <- chunk:
	default:
		select {
		case <-subscriber:
		default:
		}
		select {
		case subscriber <- chunk:
		default:
		}
	}
}

func validatePCMChunk(chunk PCMChunk) (PCMChunk, bool, string) {
	chunk.FeedID = strings.TrimSpace(chunk.FeedID)
	if chunk.FeedID == "" {
		return chunk, false, "missing feed_id"
	}
	if len(chunk.Data) == 0 {
		return chunk, false, "empty PCM payload"
	}
	if chunk.SampleRate <= 0 {
		chunk.SampleRate = opusSampleRate
	}
	if chunk.SampleRate < 8000 || chunk.SampleRate > 96000 {
		return chunk, false, fmt.Sprintf("unsupported sample rate %d", chunk.SampleRate)
	}
	if chunk.Channels <= 0 {
		chunk.Channels = 1
	}
	if chunk.Channels > 8 {
		return chunk, false, fmt.Sprintf("unsupported channel count %d", chunk.Channels)
	}
	bytesPerFrame := chunk.Channels * 2
	if len(chunk.Data) < bytesPerFrame {
		return chunk, false, "PCM payload is shorter than one frame"
	}
	if remainder := len(chunk.Data) % bytesPerFrame; remainder != 0 {
		chunk.Data = chunk.Data[:len(chunk.Data)-remainder]
	}
	if len(chunk.Data) == 0 {
		return chunk, false, "PCM payload has no aligned frames"
	}
	maxBytes := chunk.SampleRate * chunk.Channels * 2 * 2
	if len(chunk.Data) > maxBytes {
		return chunk, false, fmt.Sprintf("PCM payload is too large (%d bytes)", len(chunk.Data))
	}
	if chunk.Duration <= 0 {
		chunk.Duration = webrtcFrameDuration
	}
	if chunk.Duration > 2*time.Second {
		return chunk, false, fmt.Sprintf("PCM duration is too large (%s)", chunk.Duration)
	}
	return chunk, true, ""
}

func (h *MediaHub) HasRecentPCM(feedID string, maxAge time.Duration) bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	lastAt, ok := h.lastAt[strings.TrimSpace(feedID)]
	return ok && time.Since(lastAt) <= maxAge
}

func (h *MediaHub) Subscribe(feedID string) (<-chan PCMChunk, func()) {
	feedID = strings.TrimSpace(feedID)
	h.ensureHTTPSource(feedID)
	ch := make(chan PCMChunk, 8)
	h.mu.Lock()
	if h.subscribers[feedID] == nil {
		h.subscribers[feedID] = map[chan PCMChunk]struct{}{}
	}
	h.subscribers[feedID][ch] = struct{}{}
	if last, ok := h.last[feedID]; ok {
		ch <- last
	}
	h.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			if h.subscribers[feedID] != nil {
				delete(h.subscribers[feedID], ch)
				if len(h.subscribers[feedID]) == 0 {
					delete(h.subscribers, feedID)
				}
			}
			h.mu.Unlock()
		})
	}
}

type webRTCFrameSourceKey struct {
	feedID string
	codec  webRTCAudioCodec
}

type webRTCFrameSource struct {
	hub            *MediaHub
	key            webRTCFrameSourceKey
	encoder        opusFrameEncoder
	mu             sync.Mutex
	subs           map[chan webRTCFrame]struct{}
	sourceSnapshot webRTCFrameSourceSnapshot
	lastFrame      webRTCFrame
	sequence       uint64
	stopCh         chan struct{}
	stopOnce       sync.Once
	closed         bool
	idleEpoch      uint64
}

type webRTCFrame struct {
	sequence uint64
	payload  []byte
}

type webRTCFrameKind int

const (
	webRTCFrameReal webRTCFrameKind = iota
	webRTCFrameConcealed
	webRTCFrameIdle
)

type webRTCFrameSourceStats struct {
	produced       uint64
	real           uint64
	concealed      uint64
	idle           uint64
	dropped        uint64
	totalProduced  uint64
	totalReal      uint64
	totalConcealed uint64
	totalIdle      uint64
	totalDropped   uint64
	lastKind       webRTCFrameKind
	lastPayload    int
	lastFrameAt    time.Time
	startedAt      time.Time
	lastReport     time.Time
}

func (h *MediaHub) SubscribeWebRTCFrames(feedID string, codec webRTCAudioCodec) (<-chan webRTCFrame, func(), error) {
	for {
		source, err := h.webRTCFrameSource(feedID, codec)
		if err != nil {
			return nil, nil, err
		}
		frames, unsubscribe, ok := source.subscribe()
		if ok {
			return frames, unsubscribe, nil
		}
		h.removeWebRTCFrameSource(source)
	}
}

func (h *MediaHub) webRTCFrameSource(feedID string, codec webRTCAudioCodec) (*webRTCFrameSource, error) {
	key := webRTCFrameSourceKey{feedID: strings.TrimSpace(feedID), codec: codec}
	h.mu.Lock()
	if h.frameSources == nil {
		h.frameSources = map[webRTCFrameSourceKey]*webRTCFrameSource{}
	}
	source := h.frameSources[key]
	h.mu.Unlock()
	if source != nil {
		return source, nil
	}

	var encoder opusFrameEncoder
	var err error
	if codec == webRTCAudioOpus {
		encoder, err = newOpusFrameEncoder(opusSampleRate, opusEncoderChannels)
		if err != nil {
			return nil, err
		}
	}
	source = &webRTCFrameSource{
		hub:     h,
		key:     key,
		encoder: encoder,
		subs:    map[chan webRTCFrame]struct{}{},
		sourceSnapshot: webRTCFrameSourceSnapshot{
			FeedID:    key.feedID,
			Codec:     key.codec.String(),
			StartedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		stopCh: make(chan struct{}),
	}

	h.mu.Lock()
	if existing := h.frameSources[key]; existing != nil {
		h.mu.Unlock()
		source.stop()
		return existing, nil
	}
	h.frameSources[key] = source
	h.mu.Unlock()

	go source.run()
	return source, nil
}

func (s *webRTCFrameSource) subscribe() (<-chan webRTCFrame, func(), bool) {
	ch := make(chan webRTCFrame, webrtcPeerFrameMailbox)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, nil, false
	}
	s.idleEpoch++
	s.subs[ch] = struct{}{}
	if len(s.lastFrame.payload) > 0 {
		ch <- cloneWebRTCFrame(s.lastFrame)
	}
	s.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			var idleEpoch uint64
			existed := false
			s.mu.Lock()
			if s.subs != nil {
				_, existed = s.subs[ch]
				delete(s.subs, ch)
			}
			empty := len(s.subs) == 0
			if empty {
				s.idleEpoch++
				idleEpoch = s.idleEpoch
			}
			s.mu.Unlock()
			if existed {
				close(ch)
			}
			if empty {
				s.hub.removeWebRTCFrameSourceAfter(s, webrtcFrameSourceIdleGrace, idleEpoch)
			}
		})
	}, true
}

func (h *MediaHub) removeWebRTCFrameSourceAfter(source *webRTCFrameSource, delay time.Duration, idleEpoch uint64) {
	if h == nil || source == nil {
		return
	}
	if delay <= 0 {
		h.removeWebRTCFrameSourceIfIdle(source, idleEpoch)
		return
	}
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()
		select {
		case <-source.stopCh:
			return
		case <-timer.C:
			h.removeWebRTCFrameSourceIfIdle(source, idleEpoch)
		}
	}()
}

func (h *MediaHub) removeWebRTCFrameSource(source *webRTCFrameSource) {
	if h == nil || source == nil {
		return
	}
	h.removeWebRTCFrameSourceIfIdle(source, 0)
}

func (h *MediaHub) removeWebRTCFrameSourceIfIdle(source *webRTCFrameSource, idleEpoch uint64) {
	if h == nil || source == nil {
		return
	}
	shouldStop := false
	h.mu.Lock()
	source.mu.Lock()
	epochMatches := idleEpoch == 0 || source.idleEpoch == idleEpoch
	if epochMatches && h.frameSources[source.key] == source && len(source.subs) == 0 {
		delete(h.frameSources, source.key)
		source.closed = true
		shouldStop = true
	}
	source.mu.Unlock()
	h.mu.Unlock()
	if shouldStop {
		source.stop()
	}
}

func (s *webRTCFrameSource) stop() {
	var subscribers []chan webRTCFrame
	s.mu.Lock()
	if !s.closed {
		s.closed = true
	}
	for subscriber := range s.subs {
		subscribers = append(subscribers, subscriber)
		delete(s.subs, subscriber)
	}
	s.mu.Unlock()
	s.stopOnce.Do(func() {
		for _, subscriber := range subscribers {
			close(subscriber)
		}
		close(s.stopCh)
	})
}

func (s *webRTCFrameSource) run() {
	updates, unsubscribe := s.hub.Subscribe(s.key.feedID)
	defer unsubscribe()
	ticker := time.NewTicker(webrtcFrameDuration)
	defer ticker.Stop()

	frameQueue := make([][]byte, 0, 32)
	frameHead := 0
	concealer := frameConcealer{}
	g722Encoder := g722.NewEncoder(g722.Rate64000, 0)
	g722Idle := g722IdleFrameSamples()
	opusIdle := opusIdleFrameSamples()
	idleFrameIndex := 0
	loggedFirstFrame := false
	stats := webRTCFrameSourceStats{startedAt: time.Now(), lastReport: time.Now()}
	lastTickAt := time.Now()
	lastStallResetLog := time.Time{}

	appendChunk := func(chunk PCMChunk) {
		compactQueuedFrames(&frameQueue, &frameHead)
		frameQueue = s.appendFrames(frameQueue, g722Encoder, chunk)
	}
	drainUpdates := func() bool {
		for drained := 0; drained < cap(updates); drained++ {
			select {
			case chunk, ok := <-updates:
				if !ok {
					return false
				}
				appendChunk(chunk)
			default:
				return true
			}
		}
		return true
	}

	emitFrame := func(frame []byte, kind webRTCFrameKind) bool {
		if len(frame) == 0 {
			return true
		}
		dropped, subscribers := s.broadcast(frame)
		stats.record(kind, dropped, len(frame))
		queuedFrames := queuedFrameCount(frameQueue, frameHead)
		s.recordDiagnosticsSnapshot(&stats, subscribers, queuedFrames)
		s.maybeLogDiagnostics(&stats, subscribers, queuedFrames)
		if !loggedFirstFrame {
			log.Printf("media bridge WebRTC frame source started for feed %s codec=%s (%d bytes)", s.key.feedID, s.key.codec, len(frame))
			loggedFirstFrame = true
		}
		return true
	}

	emitNextFrame := func() bool {
		now := time.Now()
		if gap := now.Sub(lastTickAt); gap >= webrtcFrameSourceResetGap {
			frameHead = 0
			frameQueue = frameQueue[:0]
			concealer.reset()
			if lastStallResetLog.IsZero() || now.Sub(lastStallResetLog) >= 10*time.Second {
				log.Printf("media bridge WebRTC frame source reset after scheduler stall feed=%s codec=%s gap_ms=%d", s.key.feedID, s.key.codec, gap.Milliseconds())
				lastStallResetLog = now
			}
		}
		lastTickAt = now
		if !drainUpdates() {
			return false
		}
		frame, kind := concealer.nextWithKind(&frameQueue, &frameHead, func() []byte {
			frame := s.idleFrame(g722Encoder, g722Idle, opusIdle, idleFrameIndex)
			idleFrameIndex++
			return frame
		})
		return emitFrame(frame, kind)
	}

	initialFrame := s.idleFrame(g722Encoder, g722Idle, opusIdle, idleFrameIndex)
	idleFrameIndex++
	if !emitFrame(initialFrame, webRTCFrameIdle) {
		return
	}
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if !emitNextFrame() {
				return
			}
		}
	}
}

func (s *webRTCFrameSource) appendFrames(queue [][]byte, g722Encoder *g722.Encoder, chunk PCMChunk) [][]byte {
	switch s.key.codec {
	case webRTCAudioOpus:
		return appendOpusFrames(queue, s.encoder, chunk)
	case webRTCAudioPCMU:
		return appendPCMUFrames(queue, chunk)
	case webRTCAudioPCMA:
		return appendPCMAFrames(queue, chunk)
	default:
		return appendG722Frames(queue, g722Encoder, chunk)
	}
}

func (s *webRTCFrameSource) idleFrame(g722Encoder *g722.Encoder, g722Idle []int16, opusIdle []int16, phase int) []byte {
	switch s.key.codec {
	case webRTCAudioOpus:
		encoded, err := s.encoder.Encode(opusIdle)
		if err != nil {
			return nil
		}
		return encoded
	case webRTCAudioPCMU:
		return pcmuIdleFrameWithPhase(phase)
	case webRTCAudioPCMA:
		return pcmaIdleFrame()
	default:
		return encodeG722Frame(g722Encoder, g722Idle)
	}
}

func (s *webRTCFrameSource) maybeLogDiagnostics(stats *webRTCFrameSourceStats, subscribers int, queuedFrames int) {
	now := time.Now()
	if now.Sub(stats.lastReport) < webrtcDiagnosticsInterval {
		return
	}
	if stats.dropped == 0 && stats.concealed == 0 && stats.idle == 0 {
		stats.lastReport = now
		stats.resetInterval()
		return
	}
	log.Printf("media bridge WebRTC diagnostics feed=%s codec=%s frames=%d real=%d concealed=%d idle=%d subscriber_drops=%d subscribers=%d queue_frames=%d",
		s.key.feedID,
		s.key.codec,
		stats.produced,
		stats.real,
		stats.concealed,
		stats.idle,
		stats.dropped,
		subscribers,
		queuedFrames,
	)
	stats.lastReport = now
	stats.resetInterval()
}

func (s *webRTCFrameSource) recordDiagnosticsSnapshot(stats *webRTCFrameSourceStats, subscribers int, queuedFrames int) {
	now := time.Now()
	s.mu.Lock()
	if s.sourceSnapshot.StartedAt.IsZero() {
		s.sourceSnapshot.StartedAt = stats.startedAt
	}
	s.sourceSnapshot.UpdatedAt = now
	s.sourceSnapshot.Subscribers = subscribers
	s.sourceSnapshot.QueuedFrames = queuedFrames
	s.sourceSnapshot.Produced = stats.produced
	s.sourceSnapshot.Real = stats.real
	s.sourceSnapshot.Concealed = stats.concealed
	s.sourceSnapshot.Idle = stats.idle
	s.sourceSnapshot.Dropped = stats.dropped
	s.sourceSnapshot.TotalProduced = stats.totalProduced
	s.sourceSnapshot.TotalReal = stats.totalReal
	s.sourceSnapshot.TotalConcealed = stats.totalConcealed
	s.sourceSnapshot.TotalIdle = stats.totalIdle
	s.sourceSnapshot.TotalDropped = stats.totalDropped
	s.sourceSnapshot.LastKind = stats.lastKind.String()
	s.sourceSnapshot.LastPayload = stats.lastPayload
	s.sourceSnapshot.LastFrameAt = stats.lastFrameAt
	s.mu.Unlock()
}

func (s *webRTCFrameSource) snapshot() webRTCFrameSourceSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.sourceSnapshot
	snapshot.Subscribers = len(s.subs)
	return snapshot
}

func (s *webRTCFrameSourceStats) record(kind webRTCFrameKind, dropped int, payloadBytes int) {
	s.produced++
	s.totalProduced++
	s.lastKind = kind
	s.lastPayload = payloadBytes
	s.lastFrameAt = time.Now()
	switch kind {
	case webRTCFrameConcealed:
		s.concealed++
		s.totalConcealed++
	case webRTCFrameIdle:
		s.idle++
		s.totalIdle++
	default:
		s.real++
		s.totalReal++
	}
	if dropped > 0 {
		value := uint64(dropped)
		s.dropped += value
		s.totalDropped += value
	}
}

func (k webRTCFrameKind) String() string {
	switch k {
	case webRTCFrameReal:
		return "real"
	case webRTCFrameConcealed:
		return "concealed"
	case webRTCFrameIdle:
		return "idle"
	default:
		return "unknown"
	}
}

func (s *webRTCFrameSourceStats) resetInterval() {
	s.produced = 0
	s.real = 0
	s.concealed = 0
	s.idle = 0
	s.dropped = 0
}

func (s *webRTCFrameSource) broadcast(frame []byte) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequence++
	frameCopy := webRTCFrame{sequence: s.sequence, payload: append([]byte(nil), frame...)}
	s.lastFrame = cloneWebRTCFrame(frameCopy)
	dropped := 0
	for subscriber := range s.subs {
		select {
		case subscriber <- cloneWebRTCFrame(frameCopy):
		default:
			select {
			case <-subscriber:
				dropped++
			default:
			}
			select {
			case subscriber <- cloneWebRTCFrame(frameCopy):
			default:
				dropped++
			}
		}
	}
	return dropped, len(s.subs)
}

func cloneWebRTCFrame(frame webRTCFrame) webRTCFrame {
	if len(frame.payload) > 0 {
		frame.payload = append([]byte(nil), frame.payload...)
	}
	return frame
}

func (h *MediaHub) streamWebRTCFrames(ctx context.Context, peerID string, feedID string, codec webRTCAudioCodec, payloadType uint8, track *webrtc.TrackLocalStaticRTP, frames <-chan webRTCFrame, unsubscribe func(), ready <-chan struct{}, onWriteStall func()) {
	var unsubscribeOnce sync.Once
	unsubscribePeer := func() {
		if unsubscribe != nil {
			unsubscribeOnce.Do(unsubscribe)
		}
	}
	defer unsubscribePeer()
	if ready != nil {
		select {
		case <-ctx.Done():
			return
		case <-ready:
		}
	}
	loggedWrite := false
	stats := newWebRTCPeerStreamStats(time.Now())
	var writeInFlight atomic.Bool
	var writeStartedAt atomic.Int64
	var stallOnce sync.Once
	failPeer := func(reason string) {
		stallOnce.Do(func() {
			log.Printf("media bridge WebRTC stream ending feed=%s codec=%s reason=%s", feedID, codec, reason)
			unsubscribePeer()
			if onWriteStall != nil {
				onWriteStall()
			}
		})
	}
	watchdogCtx, stopWatchdog := context.WithCancel(ctx)
	defer stopWatchdog()
	go watchWebRTCSampleWrites(watchdogCtx, feedID, codec, &writeInFlight, &writeStartedAt, webrtcWriteTimeout, func() {
		failPeer("write_stall")
	})
	writeFrame := func(frame webRTCFrame, skipped int, sourceGap int, filler bool, concealed bool) bool {
		if len(frame.payload) == 0 {
			return true
		}
		stats.recordSkipped(skipped)
		stats.recordSourceGap(sourceGap)
		stats.recordFiller(filler)
		stats.recordConcealed(concealed)
		writeStartedAt.Store(time.Now().UnixNano())
		writeInFlight.Store(true)
		packetTimestamp := rtpTimestampForFrame(stats.timestamp)
		err := track.WriteRTP(&rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         !loggedWrite,
				PayloadType:    payloadType,
				SequenceNumber: stats.sequenceNumber,
				Timestamp:      packetTimestamp,
			},
			Payload: append([]byte(nil), frame.payload...),
		})
		writeInFlight.Store(false)
		if err != nil {
			stats.writeErrors++
			stats.totalWriteErrors++
			log.Printf("media bridge WebRTC stream write failed feed=%s codec=%s skipped_frames=%d write_errors=%d: %v", feedID, codec, stats.skippedFrames, stats.writeErrors, err)
			failPeer("write_error")
			return false
		}
		stats.written++
		stats.totalWritten++
		writeCompletedAt := time.Now()
		stats.recordWriteCadence(writeCompletedAt)
		stats.sequenceNumber++
		stats.timestamp = rtpTimestampAfterFrame(codec, packetTimestamp)
		stats.lastWriteAt = writeCompletedAt
		stats.lastPayloadBytes = len(frame.payload)
		h.recordWebRTCPeerSnapshot(peerID, webRTCPeerSnapshot{
			PeerID:               peerID,
			FeedID:               feedID,
			Codec:                codec.String(),
			PayloadType:          payloadType,
			StartedAt:            stats.startedAt,
			UpdatedAt:            writeCompletedAt,
			Written:              stats.written,
			FillerFrames:         stats.fillerFrames,
			ConcealedFrames:      stats.concealedFrames,
			SkippedFrames:        stats.skippedFrames,
			SourceGapFrames:      stats.sourceGapFrames,
			LateWrites:           stats.lateWrites,
			WriteErrors:          stats.writeErrors,
			TotalWritten:         stats.totalWritten,
			TotalFillerFrames:    stats.totalFillerFrames,
			TotalConcealedFrames: stats.totalConcealedFrames,
			TotalSkippedFrames:   stats.totalSkippedFrames,
			TotalSourceGapFrames: stats.totalSourceGapFrames,
			TotalLateWrites:      stats.totalLateWrites,
			TotalWriteErrors:     stats.totalWriteErrors,
			MaxWriteGapMS:        stats.maxWriteGapMS,
			SequenceNumber:       stats.sequenceNumber,
			Timestamp:            stats.timestamp,
			LastPayloadBytes:     stats.lastPayloadBytes,
			LastWriteAt:          stats.lastWriteAt,
		})
		maybeLogWebRTCPeerDiagnostics(feedID, codec, &stats)
		if !loggedWrite {
			log.Printf("media bridge WebRTC stream wrote first frame for feed %s codec=%s (%d bytes)", feedID, codec, len(frame.payload))
			loggedWrite = true
		}
		return true
	}
	var pendingSkipped int
	var lastSourceSequence uint64
	writeSourceFrame := func(frame webRTCFrame, drainSkipped int) bool {
		if len(frame.payload) == 0 {
			return true
		}
		sourceSkipped := webRTCTimestampSkippedFrames(lastSourceSequence, frame.sequence)
		pendingSkipped += webRTCDiagnosticSkippedFrames(lastSourceSequence, sourceSkipped, drainSkipped)
		lastSourceSequence = frame.sequence
		skipped := pendingSkipped
		pendingSkipped = 0
		return writeFrame(frame, skipped, sourceSkipped, false, false)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-frames:
			if !ok {
				failPeer("frame_source_closed")
				return
			}
			if !writeSourceFrame(frame, 0) {
				return
			}
		}
	}
}

func webRTCFillerFrame(codec webRTCAudioCodec) []byte {
	return webRTCFillerFrameWithPhase(codec, 0)
}

func webRTCFillerFrameWithPhase(codec webRTCAudioCodec, phase int) []byte {
	return newWebRTCFillerGenerator(codec).next(phase)
}

type webRTCFillerGenerator struct {
	codec      webRTCAudioCodec
	opus       opusFrameEncoder
	g722       *g722.Encoder
	opusIdle   []int16
	g722Idle   []int16
	pcmuSilent []byte
	pcmaSilent []byte
}

func newWebRTCFillerGenerator(codec webRTCAudioCodec) *webRTCFillerGenerator {
	generator := &webRTCFillerGenerator{codec: codec}
	switch codec {
	case webRTCAudioOpus:
		generator.opus, _ = newOpusFrameEncoder(opusSampleRate, opusEncoderChannels)
		generator.opusIdle = opusIdleFrameSamples()
	case webRTCAudioG722:
		generator.g722 = g722.NewEncoder(g722.Rate64000, 0)
		generator.g722Idle = g722IdleFrameSamples()
	case webRTCAudioPCMU:
		generator.pcmuSilent = pcmuIdleFrame()
	case webRTCAudioPCMA:
		generator.pcmaSilent = pcmaIdleFrame()
	}
	return generator
}

func (g *webRTCFillerGenerator) next(phase int) []byte {
	if g == nil {
		return nil
	}
	switch g.codec {
	case webRTCAudioPCMU:
		if len(g.pcmuSilent) == 0 {
			g.pcmuSilent = pcmuIdleFrameWithPhase(phase)
		}
		return append([]byte(nil), g.pcmuSilent...)
	case webRTCAudioPCMA:
		if len(g.pcmaSilent) == 0 {
			g.pcmaSilent = pcmaIdleFrame()
		}
		return append([]byte(nil), g.pcmaSilent...)
	case webRTCAudioG722:
		if g.g722 == nil {
			g.g722 = g722.NewEncoder(g722.Rate64000, 0)
		}
		if len(g.g722Idle) == 0 {
			g.g722Idle = g722IdleFrameSamples()
		}
		return encodeG722Frame(g.g722, g.g722Idle)
	case webRTCAudioOpus:
		if g.opus == nil {
			return nil
		}
		if len(g.opusIdle) == 0 {
			g.opusIdle = opusIdleFrameSamples()
		}
		encoded, err := g.opus.Encode(g.opusIdle)
		if err != nil {
			return nil
		}
		return encoded
	default:
		return nil
	}
}

func initialWebRTCFrame(codec webRTCAudioCodec) []byte {
	return initialWebRTCFrameWithPhase(codec, 0)
}

func initialWebRTCFrameWithPhase(codec webRTCAudioCodec, phase int) []byte {
	switch codec {
	case webRTCAudioPCMU:
		return pcmuIdleFrameWithPhase(phase)
	case webRTCAudioPCMA:
		return pcmaIdleFrame()
	case webRTCAudioG722:
		return encodeG722Frame(g722.NewEncoder(g722.Rate64000, 0), g722IdleFrameSamples())
	case webRTCAudioOpus:
		encoder, err := newOpusFrameEncoder(opusSampleRate, opusEncoderChannels)
		if err != nil {
			return nil
		}
		encoded, err := encoder.Encode(opusIdleFrameSamples())
		if err != nil {
			return nil
		}
		return encoded
	default:
		return nil
	}
}

func rtpTimestampStep(codec webRTCAudioCodec) uint32 {
	if codec == webRTCAudioOpus {
		return uint32(opusSampleRate / 50)
	}
	return uint32(webrtcRTPClockRate / 50)
}

func shouldSendWebRTCFiller(lastWriteAt time.Time, now time.Time) bool {
	if lastWriteAt.IsZero() {
		return true
	}
	return now.Sub(lastWriteAt).Truncate(time.Millisecond) >= webrtcFrameDuration
}

func shouldPreferWebRTCFrameOverFiller(frame webRTCFrame, ok bool) bool {
	return ok && len(frame.payload) > 0
}

func rtpTimestampForFrame(nextTimestamp uint32) uint32 {
	return nextTimestamp
}

func rtpTimestampAfterFrame(codec webRTCAudioCodec, packetTimestamp uint32) uint32 {
	return packetTimestamp + rtpTimestampStep(codec)
}

func webRTCTimestampSkippedFrames(lastSequence uint64, currentSequence uint64) int {
	if lastSequence == 0 || currentSequence <= lastSequence+1 {
		return 0
	}
	return int(currentSequence - lastSequence - 1)
}

func webRTCSourceGapFramesAfterFiller(sourceSkipped int, fillerFrames int) int {
	remaining := sourceSkipped - fillerFrames
	if remaining < 0 {
		return 0
	}
	return remaining
}

func webRTCDiagnosticSkippedFrames(lastSequence uint64, sourceSkipped int, drainedSkipped int) int {
	if lastSequence == 0 {
		return drainedSkipped
	}
	if sourceSkipped > drainedSkipped {
		return sourceSkipped
	}
	return drainedSkipped
}

func watchWebRTCSampleWrites(ctx context.Context, feedID string, codec webRTCAudioCodec, inFlight *atomic.Bool, startedAt *atomic.Int64, timeout time.Duration, onTimeout func()) {
	if timeout <= 0 {
		timeout = webrtcWriteTimeout
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !inFlight.Load() {
				continue
			}
			started := startedAt.Load()
			if started <= 0 {
				continue
			}
			elapsed := time.Since(time.Unix(0, started))
			if elapsed < timeout {
				continue
			}
			log.Printf("media bridge WebRTC stream write stalled feed=%s codec=%s elapsed_ms=%d", feedID, codec, elapsed.Milliseconds())
			if onTimeout != nil {
				onTimeout()
			}
			return
		}
	}
}

type webRTCPeerStreamStats struct {
	written              uint64
	fillerFrames         uint64
	concealedFrames      uint64
	skippedFrames        uint64
	sourceGapFrames      uint64
	lateWrites           uint64
	writeErrors          uint64
	totalWritten         uint64
	totalFillerFrames    uint64
	totalConcealedFrames uint64
	totalSkippedFrames   uint64
	totalSourceGapFrames uint64
	totalLateWrites      uint64
	totalWriteErrors     uint64
	startedAt            time.Time
	lastReport           time.Time
	sequenceNumber       uint16
	timestamp            uint32
	lastWriteAt          time.Time
	maxWriteGapMS        int64
	lastPayloadBytes     int
}

func newWebRTCPeerStreamStats(now time.Time) webRTCPeerStreamStats {
	var seed [6]byte
	if _, err := cryptorand.Read(seed[:]); err != nil {
		nanos := uint64(now.UnixNano())
		binary.BigEndian.PutUint16(seed[0:2], uint16(nanos))
		binary.BigEndian.PutUint32(seed[2:6], uint32(nanos>>16))
	}
	return webRTCPeerStreamStats{
		startedAt:      now,
		lastReport:     now,
		sequenceNumber: binary.BigEndian.Uint16(seed[0:2]),
		timestamp:      binary.BigEndian.Uint32(seed[2:6]),
	}
}

func (h *MediaHub) recordWebRTCPeerSnapshot(peerID string, snapshot webRTCPeerSnapshot) {
	if h == nil || strings.TrimSpace(peerID) == "" {
		return
	}
	if snapshot.StartedAt.IsZero() {
		snapshot.StartedAt = time.Now()
	}
	if snapshot.UpdatedAt.IsZero() {
		snapshot.UpdatedAt = time.Now()
	}
	h.peerStatsMu.Lock()
	if h.peerStats == nil {
		h.peerStats = map[string]webRTCPeerSnapshot{}
	}
	if existing, ok := h.peerStats[peerID]; ok && !existing.StartedAt.IsZero() {
		snapshot.StartedAt = existing.StartedAt
	}
	h.peerStats[peerID] = snapshot
	h.peerStatsMu.Unlock()
}

func (h *MediaHub) removeWebRTCPeerSnapshot(peerID string) {
	if h == nil || strings.TrimSpace(peerID) == "" {
		return
	}
	h.peerStatsMu.Lock()
	delete(h.peerStats, peerID)
	h.peerStatsMu.Unlock()
}

func (h *MediaHub) webRTCPeerHasRecentWrite(peerID string, now time.Time, maxAge time.Duration) bool {
	if h == nil || strings.TrimSpace(peerID) == "" || maxAge <= 0 {
		return false
	}
	h.peerStatsMu.Lock()
	snapshot, ok := h.peerStats[peerID]
	h.peerStatsMu.Unlock()
	return ok && !snapshot.LastWriteAt.IsZero() && now.Sub(snapshot.LastWriteAt) <= maxAge
}

func (s *webRTCPeerStreamStats) recordSkipped(skipped int) {
	if skipped > 0 {
		value := uint64(skipped)
		s.skippedFrames += value
		s.totalSkippedFrames += value
	}
}

func (s *webRTCPeerStreamStats) recordSourceGap(frames int) {
	if frames > 0 {
		value := uint64(frames)
		s.sourceGapFrames += value
		s.totalSourceGapFrames += value
	}
}

func (s *webRTCPeerStreamStats) recordFiller(filler bool) {
	if filler {
		s.fillerFrames++
		s.totalFillerFrames++
	}
}

func (s *webRTCPeerStreamStats) recordConcealed(concealed bool) {
	if concealed {
		s.concealedFrames++
		s.totalConcealedFrames++
	}
}

func (s *webRTCPeerStreamStats) recordWriteCadence(now time.Time) {
	if s.lastWriteAt.IsZero() {
		return
	}
	gap := now.Sub(s.lastWriteAt)
	gapMS := gap.Milliseconds()
	if gapMS > s.maxWriteGapMS {
		s.maxWriteGapMS = gapMS
	}
	if gap >= webrtcLateWriteThreshold {
		s.lateWrites++
		s.totalLateWrites++
	}
}

func (s *webRTCPeerStreamStats) resetInterval() {
	s.written = 0
	s.fillerFrames = 0
	s.concealedFrames = 0
	s.skippedFrames = 0
	s.sourceGapFrames = 0
	s.lateWrites = 0
	s.writeErrors = 0
	s.maxWriteGapMS = 0
}

func maybeLogWebRTCPeerDiagnostics(feedID string, codec webRTCAudioCodec, stats *webRTCPeerStreamStats) {
	now := time.Now()
	if now.Sub(stats.lastReport) < webrtcDiagnosticsInterval {
		return
	}
	lastWriteAgeMS := int64(-1)
	if !stats.lastWriteAt.IsZero() {
		lastWriteAgeMS = now.Sub(stats.lastWriteAt).Milliseconds()
	}
	log.Printf("media bridge WebRTC peer diagnostics feed=%s codec=%s written=%d filler_frames=%d concealed_frames=%d skipped_stale_frames=%d source_gap_frames=%d late_writes=%d max_write_gap_ms=%d write_errors=%d next_seq=%d next_ts=%d last_payload_bytes=%d last_write_age_ms=%d",
		feedID,
		codec,
		stats.written,
		stats.fillerFrames,
		stats.concealedFrames,
		stats.skippedFrames,
		stats.sourceGapFrames,
		stats.lateWrites,
		stats.maxWriteGapMS,
		stats.writeErrors,
		stats.sequenceNumber,
		stats.timestamp,
		stats.lastPayloadBytes,
		lastWriteAgeMS,
	)
	stats.lastReport = now
	stats.resetInterval()
}

func latestWebRTCFrame(current webRTCFrame, frames <-chan webRTCFrame) (webRTCFrame, int, bool) {
	latest := current
	collected := 0
	for {
		select {
		case frame, ok := <-frames:
			if !ok {
				return latest, skippedCollectedFrames(current, collected), false
			}
			if len(frame.payload) == 0 {
				continue
			}
			latest = frame
			collected++
		default:
			return latest, skippedCollectedFrames(current, collected), true
		}
	}
}

func drainLatestWebRTCFrame(frames <-chan webRTCFrame) (webRTCFrame, int, bool, bool) {
	select {
	case frame, ok := <-frames:
		if !ok {
			return webRTCFrame{}, 0, false, false
		}
		latest, skipped, ok := latestWebRTCFrame(frame, frames)
		if !ok {
			return webRTCFrame{}, skipped, false, false
		}
		if len(latest.payload) == 0 {
			return webRTCFrame{}, skipped, true, false
		}
		return latest, skipped, true, true
	default:
		return webRTCFrame{}, 0, true, false
	}
}

func drainLatestWebRTCFrameWithWait(frames <-chan webRTCFrame, wait time.Duration) (webRTCFrame, int, bool, bool) {
	frame, skipped, ok, hasFrame := drainLatestWebRTCFrame(frames)
	if !ok || hasFrame || wait <= 0 {
		return frame, skipped, ok, hasFrame
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case frame, ok := <-frames:
		if !ok {
			return webRTCFrame{}, 0, false, false
		}
		latest, skipped, ok := latestWebRTCFrame(frame, frames)
		if !ok {
			return webRTCFrame{}, skipped, false, false
		}
		if len(latest.payload) == 0 {
			return webRTCFrame{}, skipped, true, false
		}
		return latest, skipped, true, true
	case <-timer.C:
		return webRTCFrame{}, 0, true, false
	}
}

func skippedCollectedFrames(current webRTCFrame, collected int) int {
	if collected <= 0 {
		return 0
	}
	if len(current.payload) == 0 {
		return collected - 1
	}
	return collected
}

type frameConcealer struct {
	last       []byte
	repeated   int
	needsPrime bool
}

func (c *frameConcealer) reset() {
	c.last = nil
	c.repeated = 0
	c.needsPrime = false
}

func (c *frameConcealer) next(queue *[][]byte, head *int, silence func() []byte) []byte {
	frame, _ := c.nextWithKind(queue, head, silence)
	return frame
}

func (c *frameConcealer) nextWithKind(queue *[][]byte, head *int, silence func() []byte) ([]byte, webRTCFrameKind) {
	if c.needsPrime && queuedFrameCount(*queue, *head) < webrtcResumeQueuedFrames {
		return c.fallback(silence)
	}
	if frame, ok := popQueuedFrame(queue, head); ok {
		c.last = append([]byte(nil), frame...)
		c.repeated = 0
		c.needsPrime = false
		return frame, webRTCFrameReal
	}
	c.needsPrime = true
	return c.fallback(silence)
}

func (c *frameConcealer) fallback(silence func() []byte) ([]byte, webRTCFrameKind) {
	if len(c.last) > 0 && c.repeated < webrtcConcealmentFrames {
		c.repeated++
		return append([]byte(nil), c.last...), webRTCFrameConcealed
	}
	return silence(), webRTCFrameIdle
}

func queuedFrameCount(queue [][]byte, head int) int {
	if head < 0 || head >= len(queue) {
		return 0
	}
	return len(queue) - head
}

func popQueuedFrame(queue *[][]byte, head *int) ([]byte, bool) {
	if *head >= len(*queue) {
		*head = 0
		*queue = (*queue)[:0]
		return nil, false
	}
	frame := (*queue)[*head]
	(*queue)[*head] = nil
	*head = *head + 1
	if *head > 32 && *head*2 >= len(*queue) {
		copy(*queue, (*queue)[*head:])
		*queue = (*queue)[:len(*queue)-*head]
		*head = 0
	}
	return frame, true
}

func compactQueuedFrames(queue *[][]byte, head *int) {
	if *head <= 0 {
		return
	}
	if *head >= len(*queue) {
		*head = 0
		*queue = (*queue)[:0]
		return
	}
	copy(*queue, (*queue)[*head:])
	*queue = (*queue)[:len(*queue)-*head]
	*head = 0
}

func opusIdleFrameSamples() []int16 {
	return make([]int16, opusFrameSamples*opusEncoderChannels)
}

func g722IdleFrameSamples() []int16 {
	return make([]int16, g722FrameSamples)
}

func idleFrameSamplesWithPhase(base []int16, phase int) []int16 {
	return append([]int16(nil), base...)
}

func appendG722Frames(queue [][]byte, encoder *g722.Encoder, chunk PCMChunk) [][]byte {
	frames := pcm16ToG722Frames(encoder, chunk)
	if len(frames) == 0 {
		return queue
	}
	queue = append(queue, frames...)
	if len(queue) > webrtcMaxQueuedFrames {
		queue = queue[len(queue)-webrtcMaxQueuedFrames:]
	}
	return queue
}

func appendOpusFrames(queue [][]byte, encoder opusFrameEncoder, chunk PCMChunk) [][]byte {
	frames := pcm16ToOpusFrames(encoder, chunk)
	if len(frames) == 0 {
		return queue
	}
	queue = append(queue, frames...)
	if len(queue) > webrtcMaxQueuedFrames {
		queue = queue[len(queue)-webrtcMaxQueuedFrames:]
	}
	return queue
}

func pcm16ToOpusFrames(encoder opusFrameEncoder, chunk PCMChunk) [][]byte {
	samples := resamplePCM16ToMono(chunk, opusSampleRate)
	if len(samples) == 0 {
		return nil
	}
	frameCount := (len(samples) + opusFrameSamples - 1) / opusFrameSamples
	frames := make([][]byte, 0, frameCount)
	for frameIndex := 0; frameIndex < frameCount; frameIndex++ {
		start := frameIndex * opusFrameSamples
		end := start + opusFrameSamples
		frameSamples := make([]int16, opusFrameSamples*opusEncoderChannels)
		if start < len(samples) {
			monoEnd := min(end, len(samples))
			for sourceIndex, sample := range samples[start:monoEnd] {
				out := sourceIndex * opusEncoderChannels
				for channel := 0; channel < opusEncoderChannels; channel++ {
					frameSamples[out+channel] = sample
				}
			}
		}
		encoded, err := encoder.Encode(frameSamples)
		if err != nil || len(encoded) == 0 {
			continue
		}
		frames = append(frames, encoded)
	}
	return frames
}

func pcm16ToG722(chunk PCMChunk) []byte {
	encoder := g722.NewEncoder(g722.Rate64000, 0)
	frames := pcm16ToG722Frames(encoder, chunk)
	if len(frames) == 0 {
		return encodeG722Frame(encoder, make([]int16, g722FrameSamples))
	}
	return frames[0]
}

func pcm16ToG722Frames(encoder *g722.Encoder, chunk PCMChunk) [][]byte {
	samples := resamplePCM16ToMono(chunk, g722SampleRate)
	if len(samples) == 0 {
		return nil
	}
	frameCount := (len(samples) + g722FrameSamples - 1) / g722FrameSamples
	frames := make([][]byte, 0, frameCount)
	for frameIndex := 0; frameIndex < frameCount; frameIndex++ {
		start := frameIndex * g722FrameSamples
		end := start + g722FrameSamples
		frameSamples := make([]int16, g722FrameSamples)
		if start < len(samples) {
			copy(frameSamples, samples[start:min(end, len(samples))])
		}
		frames = append(frames, encodeG722Frame(encoder, frameSamples))
	}
	return frames
}

func encodeG722Frame(encoder *g722.Encoder, samples []int16) []byte {
	out := make([]byte, len(samples))
	n := encoder.Encode(out, samples)
	return out[:n]
}

func appendPCMUFrames(queue [][]byte, chunk PCMChunk) [][]byte {
	frames := pcm16ToPCMUFrames(chunk)
	if len(frames) == 0 {
		return queue
	}
	queue = append(queue, frames...)
	if len(queue) > webrtcMaxQueuedFrames {
		queue = queue[len(queue)-webrtcMaxQueuedFrames:]
	}
	return queue
}

func pcm16ToPCMU(chunk PCMChunk) []byte {
	frames := pcm16ToPCMUFrames(chunk)
	if len(frames) == 0 {
		return pcmuIdleFrame()
	}
	return frames[0]
}

func pcm16ToPCMUFrames(chunk PCMChunk) [][]byte {
	samples := resamplePCM16ToMono(chunk, pcmuSampleRate)
	if len(samples) == 0 {
		return nil
	}
	frameCount := (len(samples) + pcmuFrameSamples - 1) / pcmuFrameSamples
	frames := make([][]byte, 0, frameCount)
	for frameIndex := 0; frameIndex < frameCount; frameIndex++ {
		start := frameIndex * pcmuFrameSamples
		end := min(start+pcmuFrameSamples, len(samples))
		out := make([]byte, pcmuFrameSamples)
		for i := range out {
			sampleIndex := start + i
			if sampleIndex >= end {
				out[i] = 0xff
			} else {
				out[i] = linearToMuLaw(samples[sampleIndex])
			}
		}
		frames = append(frames, out)
	}
	return frames
}

func resamplePCM16ToMono(chunk PCMChunk, outputSampleRate int) []int16 {
	if len(chunk.Data) == 0 {
		return nil
	}
	if outputSampleRate <= 0 {
		outputSampleRate = opusSampleRate
	}
	sampleRate := chunk.SampleRate
	if sampleRate <= 0 {
		sampleRate = opusSampleRate
	}
	channels := chunk.Channels
	if channels <= 0 {
		channels = 1
	}
	bytesPerFrame := channels * 2
	sourceFrames := len(chunk.Data) / bytesPerFrame
	if sourceFrames <= 0 {
		return nil
	}
	outputSamples := int((int64(sourceFrames)*int64(outputSampleRate) + int64(sampleRate/2)) / int64(sampleRate))
	if outputSamples <= 0 {
		return nil
	}
	out := make([]int16, outputSamples)
	for outputIndex := range out {
		sourceNumerator := int64(outputIndex) * int64(sampleRate)
		left := int(sourceNumerator / int64(outputSampleRate))
		if left >= sourceFrames {
			left = sourceFrames - 1
		}
		right := min(left+1, sourceFrames-1)
		fraction := float64(sourceNumerator%int64(outputSampleRate)) / float64(outputSampleRate)
		leftSample := monoSampleAt(chunk.Data, left, bytesPerFrame, channels)
		rightSample := monoSampleAt(chunk.Data, right, bytesPerFrame, channels)
		value := float64(leftSample) + (float64(rightSample)-float64(leftSample))*fraction
		out[outputIndex] = int16(clampPCMInt(int(value), int(i16Min), int(i16Max)))
	}
	return out
}

func linearToMuLaw(sample int16) byte {
	const bias = 0x84
	const clip = 32635
	pcm := int(sample)
	sign := 0
	if pcm < 0 {
		sign = 0x80
		pcm = -pcm
	}
	if pcm > clip {
		pcm = clip
	}
	pcm += bias
	exponent := 7
	for mask := 0x4000; exponent > 0 && pcm&mask == 0; exponent-- {
		mask >>= 1
	}
	mantissa := (pcm >> (exponent + 3)) & 0x0f
	return ^byte(sign | (exponent << 4) | mantissa)
}

func pcmuIdleFrame() []byte {
	return pcmuIdleFrameWithPhase(0)
}

func pcmuIdleFrameWithPhase(phase int) []byte {
	frame := make([]byte, pcmuFrameSamples)
	for i := range frame {
		frame[i] = 0xff
	}
	return frame
}

func appendPCMAFrames(queue [][]byte, chunk PCMChunk) [][]byte {
	frames := pcm16ToPCMAFrames(chunk)
	if len(frames) == 0 {
		return queue
	}
	queue = append(queue, frames...)
	if len(queue) > webrtcMaxQueuedFrames {
		queue = queue[len(queue)-webrtcMaxQueuedFrames:]
	}
	return queue
}

func pcm16ToPCMA(chunk PCMChunk) []byte {
	frames := pcm16ToPCMAFrames(chunk)
	if len(frames) == 0 {
		return pcmaIdleFrame()
	}
	return frames[0]
}

func pcm16ToPCMAFrames(chunk PCMChunk) [][]byte {
	samples := resamplePCM16ToMono(chunk, pcmuSampleRate)
	if len(samples) == 0 {
		return nil
	}
	frameCount := (len(samples) + pcmuFrameSamples - 1) / pcmuFrameSamples
	frames := make([][]byte, 0, frameCount)
	for frameIndex := 0; frameIndex < frameCount; frameIndex++ {
		start := frameIndex * pcmuFrameSamples
		end := min(start+pcmuFrameSamples, len(samples))
		out := make([]byte, pcmuFrameSamples)
		for i := range out {
			sampleIndex := start + i
			if sampleIndex >= end {
				out[i] = 0xd5
			} else {
				out[i] = linearToALaw(samples[sampleIndex])
			}
		}
		frames = append(frames, out)
	}
	return frames
}

func linearToALaw(sample int16) byte {
	pcm := int(sample)
	mask := byte(0xd5)
	if pcm < 0 {
		pcm = -pcm - 1
		mask = 0x55
	}
	if pcm > 32635 {
		pcm = 32635
	}
	pcm >>= 3
	var encoded byte
	if pcm >= 256 {
		exponent := 7
		for expMask := 0x4000 >> 3; exponent > 0 && pcm&expMask == 0; exponent-- {
			expMask >>= 1
		}
		mantissa := (pcm >> (exponent + 3)) & 0x0f
		encoded = byte((exponent << 4) | mantissa)
	} else {
		encoded = byte(pcm >> 4)
	}
	return encoded ^ mask
}

func pcmaIdleFrame() []byte {
	frame := make([]byte, pcmuFrameSamples)
	for i := range frame {
		frame[i] = 0xd5
	}
	return frame
}

func monoSampleAt(data []byte, frame int, bytesPerFrame int, channels int) int {
	offset := frame * bytesPerFrame
	sum := 0
	for channel := 0; channel < channels; channel++ {
		sampleOffset := offset + channel*2
		sum += int(int16(binary.LittleEndian.Uint16(data[sampleOffset : sampleOffset+2])))
	}
	return sum / channels
}

func clampPCMInt(value int, minValue int, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

const (
	i16Min = -32768
	i16Max = 32767
)

func drainRTCP(sender *webrtc.RTPSender) {
	buffer := make([]byte, 1500)
	for {
		if _, _, err := sender.Read(buffer); err != nil {
			return
		}
	}
}

func mediaSafeID(value string) string {
	var builder strings.Builder
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' {
			builder.WriteRune(ch)
		}
	}
	if builder.Len() == 0 {
		return "feed"
	}
	return builder.String()
}

func sleepMedia(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
