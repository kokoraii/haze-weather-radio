package ivr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type twilioStreamMessage struct {
	Event          string `json:"event"`
	SequenceNumber string `json:"sequenceNumber"`
	StreamSID      string `json:"streamSid"`
	Protocol       string `json:"protocol"`
	Version        string `json:"version"`
	Start          struct {
		AccountSID       string            `json:"accountSid"`
		CallSID          string            `json:"callSid"`
		StreamSID        string            `json:"streamSid"`
		Tracks           []string          `json:"tracks"`
		CustomParameters map[string]string `json:"customParameters"`
		MediaFormat      struct {
			Encoding   string `json:"encoding"`
			SampleRate int    `json:"sampleRate"`
			Channels   int    `json:"channels"`
		} `json:"mediaFormat"`
	} `json:"start"`
	Media struct {
		Track     string `json:"track"`
		Chunk     string `json:"chunk"`
		Timestamp string `json:"timestamp"`
		Payload   string `json:"payload"`
	} `json:"media"`
	DTMF struct {
		Track string `json:"track"`
		Digit string `json:"digit"`
	} `json:"dtmf"`
	Mark struct {
		Name string `json:"name"`
	} `json:"mark"`
}

type twilioMediaConnection struct {
	service    *Service
	ws         *websocket.Conn
	streamSID  string
	startedAt  time.Time
	frames     chan pcmFrame
	digits     chan string
	marks      chan string
	writeMu    sync.Mutex
	promptPCMU func(context.Context, string, string, string) ([]byte, error)
	textPCMU   func(context.Context, string, string, string) ([]byte, error)
}

func (s *Service) handleTwilioMedia(writer http.ResponseWriter, request *http.Request) {
	if s == nil || s.twilio == nil || s.searchIndex == nil || !s.cfg.IVR.Search.enabled() {
		http.Error(writer, "Twilio location search is unavailable", http.StatusServiceUnavailable)
		return
	}
	if !s.twilio.validateWebSocket(request) {
		http.Error(writer, "invalid Twilio signature", http.StatusForbidden)
		return
	}
	select {
	case s.twilio.streams <- struct{}{}:
		defer func() { <-s.twilio.streams }()
	default:
		http.Error(writer, "too many Twilio streams", http.StatusServiceUnavailable)
		return
	}
	connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	connection.SetReadLimit(s.cfg.IVR.Twilio.MaxMessageBytes)
	defer connection.Close(websocket.StatusNormalClosure, "search complete")

	ctx := context.Background()
	if s.ctx != nil {
		ctx = s.ctx
	}
	maxCall := time.Duration(s.cfg.IVR.MaxCallSeconds) * time.Second
	if maxCall <= 0 {
		maxCall = 11 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, maxCall)
	defer cancel()
	startCtx, cancelStart := context.WithTimeout(ctx, 5*time.Second)
	start, session, err := s.readTwilioStreamStart(startCtx, connection)
	cancelStart()
	if err != nil {
		_ = connection.Close(websocket.StatusPolicyViolation, "invalid stream start")
		return
	}
	media := &twilioMediaConnection{
		service: s, ws: connection, streamSID: start.StreamSID, startedAt: time.Now(),
		frames: make(chan pcmFrame, 64), digits: make(chan string, 16), marks: make(chan string, 16),
	}
	readerCtx, stopReader := context.WithCancel(ctx)
	defer stopReader()
	readerDone := make(chan error, 1)
	go func() { readerDone <- media.readLoop(readerCtx, start.SequenceNumber) }()

	location, ok := media.searchLocation(ctx, session.Language, session.Region, session.callerProvince)
	if !ok {
		stopReader()
		return
	}
	resultToken, err := s.twilio.storeResult(session, location)
	if err != nil {
		return
	}
	redirectCtx, cancelRedirect := context.WithTimeout(ctx, 15*time.Second)
	err = s.twilio.redirector.Redirect(redirectCtx, session.AccountSID, session.CallSID, s.twilioResultURL(resultToken))
	cancelRedirect()
	if err != nil {
		return
	}
	stopReader()
	select {
	case <-readerDone:
	case <-time.After(100 * time.Millisecond):
	}
}

func (s *Service) readTwilioStreamStart(ctx context.Context, connection *websocket.Conn) (twilioStreamMessage, *twilioPendingSession, error) {
	message, err := readTwilioMessage(ctx, connection)
	if err != nil || message.Event != "connected" || message.Protocol != "Call" {
		return twilioStreamMessage{}, nil, fmt.Errorf("missing connected event")
	}
	message, err = readTwilioMessage(ctx, connection)
	if err != nil || message.Event != "start" {
		return twilioStreamMessage{}, nil, fmt.Errorf("missing start event")
	}
	start := message.Start
	if message.StreamSID == "" || start.StreamSID != message.StreamSID || !validTwilioSID(start.StreamSID, "MZ") || start.AccountSID != s.twilio.accountSID || !validTwilioSID(start.CallSID, "CA") {
		return twilioStreamMessage{}, nil, fmt.Errorf("invalid stream identity")
	}
	if !strings.EqualFold(start.MediaFormat.Encoding, "audio/x-mulaw") || start.MediaFormat.SampleRate != 8000 || start.MediaFormat.Channels != 1 {
		return twilioStreamMessage{}, nil, fmt.Errorf("unsupported Twilio media format")
	}
	nonce := strings.TrimSpace(start.CustomParameters["nonce"])
	session, ok := s.twilio.consumePending(nonce, start.AccountSID, start.CallSID)
	if !ok {
		return twilioStreamMessage{}, nil, fmt.Errorf("invalid or replayed search nonce")
	}
	return message, session, nil
}

func readTwilioMessage(ctx context.Context, connection *websocket.Conn) (twilioStreamMessage, error) {
	typeCode, raw, err := connection.Read(ctx)
	if err != nil {
		return twilioStreamMessage{}, err
	}
	if typeCode != websocket.MessageText {
		return twilioStreamMessage{}, fmt.Errorf("Twilio message must be text")
	}
	var message twilioStreamMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return twilioStreamMessage{}, err
	}
	return message, nil
}

func (connection *twilioMediaConnection) readLoop(ctx context.Context, initialSequence string) error {
	lastSequence, _ := strconv.ParseUint(initialSequence, 10, 64)
	malformed := 0
	for {
		message, err := readTwilioMessage(ctx, connection.ws)
		if err != nil {
			return err
		}
		sequence, err := strconv.ParseUint(message.SequenceNumber, 10, 64)
		if err != nil || sequence <= lastSequence {
			malformed++
			if malformed >= 3 {
				return fmt.Errorf("invalid Twilio message sequence")
			}
			continue
		}
		lastSequence = sequence
		switch message.Event {
		case "media":
			if !connection.handleInboundMedia(message) {
				malformed++
			}
		case "dtmf":
			if message.DTMF.Track == "inbound_track" && validDTMFDigit(message.DTMF.Digit) {
				pushDropOldest(connection.digits, message.DTMF.Digit)
			}
		case "mark":
			if name := strings.TrimSpace(message.Mark.Name); name != "" && len(name) <= 128 {
				pushDropOldest(connection.marks, name)
			}
		case "stop":
			return io.EOF
		default:
			malformed++
		}
		if malformed >= 3 {
			return fmt.Errorf("too many malformed Twilio messages")
		}
	}
}

func (connection *twilioMediaConnection) handleInboundMedia(message twilioStreamMessage) bool {
	if message.Media.Track != "inbound" || len(message.Media.Payload) > 48*1024 {
		return false
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(message.Media.Payload)
	if err != nil || len(raw) == 0 || len(raw) > 32*1024 {
		return false
	}
	timestampMS, err := strconv.ParseInt(message.Media.Timestamp, 10, 64)
	if err != nil || timestampMS < 0 {
		return false
	}
	samples := make([]int16, len(raw)*2)
	for index, value := range raw {
		sample := uLawToLinear(value)
		samples[index*2] = sample
		samples[index*2+1] = sample
	}
	frameAt := connection.startedAt.Add(time.Duration(timestampMS) * time.Millisecond)
	for offset := 0; offset < len(samples); offset += sipG722FrameSamples {
		end := minInt(offset+sipG722FrameSamples, len(samples))
		frame := pcmFrame{Samples: append([]int16(nil), samples[offset:end]...), At: frameAt.Add(time.Duration(offset) * time.Second / searchPCMRate)}
		pushDropOldest(connection.frames, frame)
	}
	return true
}

func pushDropOldest[T any](channel chan T, value T) {
	select {
	case channel <- value:
		return
	default:
	}
	select {
	case <-channel:
	default:
	}
	select {
	case channel <- value:
	default:
	}
}

func validDTMFDigit(digit string) bool {
	return len(digit) == 1 && strings.Contains("0123456789*#", digit)
}

func (connection *twilioMediaConnection) searchLocation(ctx context.Context, language string, region string, callerProvince ...string) (ResolvedLocation, bool) {
	provinceHint := ""
	if len(callerProvince) > 0 {
		provinceHint = callerProvince[0]
	}
	hint := twilioLocationSearchContext(language, region, provinceHint, connection.service.cfg.IVR.Search.CallerHintEnabled)
	for attempt := 0; attempt < connection.service.cfg.IVR.Search.MaxAttempts && ctx.Err() == nil; attempt++ {
		decision, voiceFailed := connection.runSearchAttempt(ctx, language, hint, connection.service.cfg.IVR.Search.voiceEnabled())
		if voiceFailed {
			_ = connection.playConfiguredPrompt(ctx, "location_search", "voice_unavailable", language)
			decision, _ = connection.runSearchAttempt(ctx, language, hint, false)
		}
		if location, ok := connection.confirmDecision(ctx, language, decision); ok {
			return location, true
		}
		if decision.Kind == locationSearchRefine {
			_ = connection.playConfiguredPrompt(ctx, "location_search", "more_detail", language)
		} else if decision.Kind == locationSearchNoMatch {
			_ = connection.playConfiguredPrompt(ctx, "location_search", "no_match", language)
		}
	}
	_ = connection.playConfiguredPrompt(ctx, "location_search", "fallback_numeric", language)
	return ResolvedLocation{}, false
}

func twilioLocationSearchContext(language string, regionalSelector string, callerProvince string, callerHintEnabled bool) locationSearchContext {
	hint := regionalLocationSearchContext(language, regionalSelector)
	if !hint.ExplicitRegion && callerHintEnabled {
		hint.Region = searchRegionFromSelector(callerProvince)
	}
	return hint
}

func (connection *twilioMediaConnection) runSearchAttempt(ctx context.Context, language string, hint locationSearchContext, allowVoice bool) (locationSearchDecision, bool) {
	session := newLocationSearchSession(connection.service.searchIndex, hint, connection.service.cfg.IVR.Search.MultitapWindow)
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	connection.drainFrames()
	var capture voiceCapture
	if allowVoice {
		capture = startAdaptiveVoiceCapture(attemptCtx, connection.frames, connection.service.cfg.IVR.Search)
	}
	input := connection.playSearchPrompt(attemptCtx, language, capture.Onset)
	if input.voice {
		session.LockVoice(input.onset)
	} else if input.digit != "" {
		session.FeedDigit(input.digit, time.Now())
	}
	if session.Method() == "" {
		input = connection.waitModality(attemptCtx, capture.Onset, 10*time.Second)
		if input.voice {
			session.LockVoice(input.onset)
		} else if input.digit != "" {
			session.FeedDigit(input.digit, time.Now())
		}
	}
	switch session.Method() {
	case locationSearchVoice:
		connection.service.metrics.SearchVoice.Add(1)
		captured, ok := waitCapturedVoice(attemptCtx, capture.Done)
		if !ok {
			return locationSearchDecision{Kind: locationSearchNoMatch}, true
		}
		transcript, err := connection.service.transcribeSearchAudio(attemptCtx, captured, language, hint.Region)
		if err != nil {
			connection.service.metrics.ASRFailures.Add(1)
			return locationSearchDecision{Kind: locationSearchNoMatch}, true
		}
		return session.VoiceDecision(transcript), false
	case locationSearchT9, locationSearchMultitap:
		cancel()
		decision := connection.collectKeypad(ctx, session)
		if session.Method() == locationSearchMultitap {
			connection.service.metrics.SearchMultitap.Add(1)
		} else {
			connection.service.metrics.SearchT9.Add(1)
		}
		return decision, false
	default:
		return locationSearchDecision{Kind: locationSearchNoMatch}, false
	}
}

func (connection *twilioMediaConnection) playSearchPrompt(ctx context.Context, language string, onset <-chan time.Time) sipSearchInput {
	mark, err := connection.sendConfiguredPrompt(ctx, "location_search", "main", language)
	if err != nil {
		return sipSearchInput{}
	}
	for {
		select {
		case <-ctx.Done():
			return sipSearchInput{}
		case at := <-onset:
			if !at.IsZero() {
				_ = connection.clear(ctx)
				return sipSearchInput{voice: true, onset: at}
			}
		case digit := <-connection.digits:
			if len(digit) == 1 && digit[0] >= '2' && digit[0] <= '9' {
				_ = connection.clear(ctx)
				return sipSearchInput{digit: digit}
			}
		case played := <-connection.marks:
			if played == mark {
				return sipSearchInput{}
			}
		}
	}
}

func (connection *twilioMediaConnection) waitModality(ctx context.Context, onset <-chan time.Time, timeout time.Duration) sipSearchInput {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return sipSearchInput{}
		case at := <-onset:
			if !at.IsZero() {
				return sipSearchInput{voice: true, onset: at}
			}
		case digit := <-connection.digits:
			if len(digit) == 1 && digit[0] >= '2' && digit[0] <= '9' {
				return sipSearchInput{digit: digit}
			}
		case <-timer.C:
			return sipSearchInput{}
		}
	}
}

func (connection *twilioMediaConnection) collectKeypad(ctx context.Context, session *locationSearchSession) locationSearchDecision {
	idle := time.NewTimer(connection.service.cfg.IVR.Search.IdleSubmit)
	defer idle.Stop()
	for {
		select {
		case <-ctx.Done():
			return locationSearchDecision{Kind: locationSearchNoMatch}
		case digit := <-connection.digits:
			if digit == "#" {
				return session.KeypadDecision()
			}
			if session.FeedDigit(digit, time.Now()) {
				resetTimer(idle, connection.service.cfg.IVR.Search.IdleSubmit)
			}
		case <-idle.C:
			return session.KeypadDecision()
		}
	}
}

func (connection *twilioMediaConnection) confirmDecision(ctx context.Context, language string, decision locationSearchDecision) (ResolvedLocation, bool) {
	switch decision.Kind {
	case locationSearchAccept:
		connection.service.metrics.SearchAccepted.Add(1)
		return localizedSearchLocation(decision.Matches[0], language), true
	case locationSearchConfirm:
		connection.service.metrics.SearchConfirmations.Add(1)
		match := decision.Matches[0]
		digit, ok := connection.playTextAndWaitDigit(ctx, "search-confirm", localizedConfirmPrompt(language, match.DisplayName, match.Target.Location.Province), language, 8*time.Second)
		if ok && digit == "1" {
			connection.service.metrics.SearchAccepted.Add(1)
			return localizedSearchLocation(match, language), true
		}
	case locationSearchChoices:
		connection.service.metrics.SearchAmbiguities.Add(1)
		if len(decision.Matches) == 0 {
			return ResolvedLocation{}, false
		}
		current := 0
		for ctx.Err() == nil {
			digit, ok := connection.playTextAndWaitDigit(ctx, "search-choices", localizedAmbiguityPrompt(language, decision.Matches[current]), language, 10*time.Second)
			if !ok {
				return ResolvedLocation{}, false
			}
			var action locationSearchChoiceAction
			current, action = advanceLocationSearchChoice(current, len(decision.Matches), digit)
			switch action {
			case locationSearchChoiceBack:
				return ResolvedLocation{}, false
			case locationSearchChoiceAccept:
				connection.service.metrics.SearchAccepted.Add(1)
				return localizedSearchLocation(decision.Matches[current], language), true
			}
		}
	case locationSearchRefine:
		connection.service.metrics.SearchAmbiguities.Add(1)
	default:
		connection.service.metrics.SearchNoMatch.Add(1)
	}
	return ResolvedLocation{}, false
}

func (connection *twilioMediaConnection) playConfiguredPrompt(ctx context.Context, menuID string, lineKey string, language string) error {
	mark, err := connection.sendConfiguredPrompt(ctx, menuID, lineKey, language)
	if err != nil {
		return err
	}
	return connection.waitMark(ctx, mark)
}

func (connection *twilioMediaConnection) sendConfiguredPrompt(ctx context.Context, menuID string, lineKey string, language string) (string, error) {
	if connection.promptPCMU != nil {
		raw, err := connection.promptPCMU(ctx, menuID, lineKey, language)
		if err != nil {
			return "", err
		}
		return connection.sendPCMU(ctx, raw)
	}
	values := connection.service.promptValues(map[string]string{"lang": language})
	audio, ok := connection.service.staticPromptAudio(menuID, lineKey, values)
	if !ok {
		var err error
		audio, err = connection.service.cache.GetPromptWithPolicy(ctx, menuID, lineKey, values, connection.service.defaultPlaybackPolicy(), false)
		if err != nil {
			return "", err
		}
	}
	raw, err := os.ReadFile(audio.PCMUPath)
	if err != nil {
		return "", err
	}
	return connection.sendPCMU(ctx, raw)
}

func (connection *twilioMediaConnection) playTextAndWaitDigit(ctx context.Context, lineKey string, text string, language string, timeout time.Duration) (string, bool) {
	var raw []byte
	var err error
	if connection.textPCMU != nil {
		raw, err = connection.textPCMU(ctx, lineKey, text, language)
	} else {
		var audio CachedAudio
		audio, err = connection.service.textPromptAudioForLanguage(ctx, lineKey, text, language)
		if err == nil {
			raw, err = os.ReadFile(audio.PCMUPath)
		}
	}
	if err != nil {
		return "", false
	}
	mark, err := connection.sendPCMU(ctx, raw)
	if err != nil {
		return "", false
	}
	// The response window starts after Twilio confirms prompt playback. Keep a
	// separate bounded playback deadline so longer ambiguity prompts are not
	// timed out while they are still queued at Twilio.
	timer := time.NewTimer(maxDuration(timeout, 30*time.Second))
	defer timer.Stop()
	played := false
	for {
		select {
		case <-ctx.Done():
			return "", false
		case digit := <-connection.digits:
			_ = connection.clear(ctx)
			return digit, true
		case name := <-connection.marks:
			if name == mark {
				played = true
				resetTimer(timer, timeout)
			}
		case <-timer.C:
			return "", played
		}
	}
}

func (connection *twilioMediaConnection) sendPCMU(ctx context.Context, raw []byte) (string, error) {
	if len(raw) == 0 {
		return "", fmt.Errorf("prompt audio is empty")
	}
	markID, err := newSearchRequestID()
	if err != nil {
		return "", err
	}
	for offset := 0; offset < len(raw); offset += 3200 {
		end := minInt(offset+3200, len(raw))
		message := map[string]any{
			"event": "media", "streamSid": connection.streamSID,
			"media": map[string]string{"payload": base64.StdEncoding.EncodeToString(raw[offset:end])},
		}
		if err := connection.writeJSON(ctx, message); err != nil {
			return "", err
		}
	}
	if err := connection.writeJSON(ctx, map[string]any{
		"event": "mark", "streamSid": connection.streamSID, "mark": map[string]string{"name": markID},
	}); err != nil {
		return "", err
	}
	return markID, nil
}

func (connection *twilioMediaConnection) clear(ctx context.Context) error {
	return connection.writeJSON(ctx, map[string]any{"event": "clear", "streamSid": connection.streamSID})
}

func (connection *twilioMediaConnection) writeJSON(ctx context.Context, message map[string]any) error {
	raw, err := json.Marshal(message)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	connection.writeMu.Lock()
	defer connection.writeMu.Unlock()
	return connection.ws.Write(writeCtx, websocket.MessageText, raw)
}

func (connection *twilioMediaConnection) waitMark(ctx context.Context, mark string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case current := <-connection.marks:
			if current == mark {
				return nil
			}
		}
	}
}

func (connection *twilioMediaConnection) drainFrames() {
	for {
		select {
		case <-connection.frames:
		default:
			return
		}
	}
}

func (s *Service) textPromptAudioForLanguage(ctx context.Context, lineKey string, text string, language string) (CachedAudio, error) {
	if s == nil || s.cache == nil {
		return CachedAudio{}, errors.New("IVR prompt cache is unavailable")
	}
	policy := s.staticPromptPolicy()
	policy.Language = fallbackText(language, policy.Language)
	return s.cache.GetTextPromptWithPolicy(ctx, "dynamic", safeID(lineKey), text, policy, false)
}
