package ivr

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestReadTwilioStreamStartValidatesFormatIdentityAndReplay(t *testing.T) {
	t.Parallel()
	runtime := testTwilioRuntime(t)
	service := &Service{twilio: runtime}
	session, err := runtime.newPending(testTwilioAccountSID, testTwilioCallSID, "en-CA", "SK")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			result <- err
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _, err = service.readTwilioStreamStart(ctx, connection)
		result <- err
	}))
	defer server.Close()

	sendTwilioStart(t, server.URL, session.Token, testTwilioAccountSID, testTwilioCallSID, testTwilioStreamSID)
	if err := <-result; err != nil {
		t.Fatalf("valid stream start failed: %v", err)
	}
	sendTwilioStart(t, server.URL, session.Token, testTwilioAccountSID, testTwilioCallSID, testTwilioStreamSID)
	if err := <-result; err == nil || !strings.Contains(err.Error(), "replayed") {
		t.Fatalf("replayed stream nonce was accepted: %v", err)
	}
}

func TestReadTwilioStreamStartRejectsForgedAccountAndMediaFormat(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		accountSID string
		sampleRate int
	}{
		{name: "forged account", accountSID: "ACffffffffffffffffffffffffffffffff", sampleRate: 8000},
		{name: "wrong sample rate", accountSID: testTwilioAccountSID, sampleRate: 16000},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			runtime := testTwilioRuntime(t)
			service := &Service{twilio: runtime}
			session, err := runtime.newPending(testTwilioAccountSID, testTwilioCallSID, "en-CA", "SK")
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err != nil {
					result <- err
					return
				}
				defer connection.Close(websocket.StatusNormalClosure, "done")
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, _, err = service.readTwilioStreamStart(ctx, connection)
				result <- err
			}))
			defer server.Close()
			sendTwilioStartWithRate(t, server.URL, session.Token, test.accountSID, testTwilioCallSID, testTwilioStreamSID, test.sampleRate)
			if err := <-result; err == nil {
				t.Fatal("invalid Twilio stream start was accepted")
			}
		})
	}
}

func TestTwilioInboundMediaDecodesPCMUAndRejectsOversizedPayload(t *testing.T) {
	t.Parallel()
	connection := &twilioMediaConnection{startedAt: time.Unix(100, 0), frames: make(chan pcmFrame, 4)}
	payload := make([]byte, 160)
	for index := range payload {
		payload[index] = linearToULaw(2000)
	}
	message := twilioStreamMessage{}
	message.Media.Track = "inbound"
	message.Media.Timestamp = "20"
	message.Media.Payload = base64.StdEncoding.EncodeToString(payload)
	if !connection.handleInboundMedia(message) {
		t.Fatal("valid PCMU media was rejected")
	}
	frame := <-connection.frames
	if len(frame.Samples) != 320 || frame.At != connection.startedAt.Add(20*time.Millisecond) {
		t.Fatalf("unexpected decoded frame: samples=%d at=%s", len(frame.Samples), frame.At)
	}
	message.Media.Payload = strings.Repeat("A", 49*1024)
	if connection.handleInboundMedia(message) {
		t.Fatal("oversized Twilio media payload was accepted")
	}
}

func TestTwilioWebSocketReadLimitRejectsOversizedMessage(t *testing.T) {
	t.Parallel()
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			result <- err
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")
		connection.SetReadLimit(128)
		_, err = readTwilioMessage(context.Background(), connection)
		result <- err
	}))
	defer server.Close()
	connection, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Write(context.Background(), websocket.MessageText, []byte(strings.Repeat("A", 1024))); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err == nil {
		t.Fatal("oversized WebSocket message was accepted")
	}
	_ = connection.Close(websocket.StatusNormalClosure, "done")
}

func TestTwilioKeypadSearchUsesSharedSessionAndBargesPrompt(t *testing.T) {
	index := testLocationSearchIndex(t)
	enabled := true
	disabled := false
	service := &Service{
		searchIndex: index,
		cfg: loadedConfig{IVR: Config{
			Search: searchConfig{
				Enabled: &enabled, VoiceEnabled: &disabled, MultitapWindow: 700 * time.Millisecond,
				IdleSubmit: 2500 * time.Millisecond, MaxAttempts: 2,
			},
		}},
	}
	selected := make(chan ResolvedLocation, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ws, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer ws.Close(websocket.StatusNormalClosure, "done")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		media := &twilioMediaConnection{
			service: service, ws: ws, streamSID: testTwilioStreamSID, startedAt: time.Now(),
			frames: make(chan pcmFrame, 64), digits: make(chan string, 16), marks: make(chan string, 16),
			promptPCMU: func(context.Context, string, string, string) ([]byte, error) {
				return make([]byte, 160), nil
			},
			textPCMU: func(context.Context, string, string, string) ([]byte, error) {
				return make([]byte, 160), nil
			},
		}
		go media.readLoop(ctx, "0")
		location, ok := media.searchLocation(ctx, "en-CA", "SK")
		if ok {
			selected <- location
		}
	}))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(websocket.StatusNormalClosure, "done")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		messageType, raw, err := client.Read(ctx)
		if err != nil {
			t.Fatalf("read search prompt: %v", err)
		}
		if messageType != websocket.MessageText {
			continue
		}
		var outbound map[string]any
		if json.Unmarshal(raw, &outbound) == nil && outbound["event"] == "media" {
			break
		}
	}
	digits := []string{"7", "2", "7", "5", "2", "8", "6", "6", "6", "#"}
	for index, digit := range digits {
		message := map[string]any{
			"event": "dtmf", "streamSid": testTwilioStreamSID, "sequenceNumber": fmt.Sprint(index + 1),
			"dtmf": map[string]string{"track": "inbound_track", "digit": digit},
		}
		raw, _ := json.Marshal(message)
		if err := client.Write(ctx, websocket.MessageText, raw); err != nil {
			t.Fatalf("send DTMF: %v", err)
		}
	}
	select {
	case location := <-selected:
		if location.Code != "SK-40" || location.Source != "eccc_forecast" {
			t.Fatalf("unexpected selected location: %#v", location)
		}
	case <-ctx.Done():
		t.Fatal("Twilio keypad search did not select Saskatoon")
	}
}

func TestTwilioLocationSearchContextAppliesCallerHintSoftly(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		selector      string
		caller        string
		callerEnabled bool
		wantRegion    string
		wantExplicit  bool
	}{
		{
			name: "nationwide caller hint", selector: "CA", caller: "SK", callerEnabled: true,
			wantRegion: "SK", wantExplicit: false,
		},
		{
			name: "regional line wins", selector: "ON", caller: "SK", callerEnabled: true,
			wantRegion: "ON", wantExplicit: true,
		},
		{
			name: "hint disabled", selector: "CA", caller: "SK", callerEnabled: false,
			wantRegion: "", wantExplicit: false,
		},
		{
			name: "ambiguous caller", selector: "CA", caller: "", callerEnabled: true,
			wantRegion: "", wantExplicit: false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			hint := twilioLocationSearchContext("en-CA", test.selector, test.caller, test.callerEnabled)
			if hint.Region != test.wantRegion || hint.ExplicitRegion != test.wantExplicit {
				t.Fatalf("context = %#v, want region=%q explicit=%t", hint, test.wantRegion, test.wantExplicit)
			}
		})
	}
}

func TestTwilioAmbiguityMenuPlaysOneMatchAtATime(t *testing.T) {
	t.Parallel()
	type result struct {
		location ResolvedLocation
		prompts  []string
		ok       bool
	}
	results := make(chan result, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ws, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			results <- result{}
			return
		}
		defer ws.Close(websocket.StatusNormalClosure, "done")
		prompts := make([]string, 0, 2)
		media := &twilioMediaConnection{
			service: &Service{}, ws: ws, streamSID: testTwilioStreamSID,
			digits: make(chan string, 4), marks: make(chan string, 4),
			textPCMU: func(_ context.Context, _ string, text string, _ string) ([]byte, error) {
				prompts = append(prompts, text)
				return make([]byte, 160), nil
			},
		}
		media.digits <- "3"
		media.digits <- "2"
		decision := locationSearchDecision{Kind: locationSearchChoices, Matches: []locationSearchMatch{
			{DisplayName: "Springfield", Target: locationSearchTarget{Location: ResolvedLocation{Source: "test", Code: "first", Province: "ON"}}},
			{DisplayName: "Saskatoon", Target: locationSearchTarget{Location: ResolvedLocation{Source: "test", Code: "second", Province: "SK"}}},
		}}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		location, ok := media.confirmDecision(ctx, "en-CA", decision)
		results <- result{location: location, prompts: prompts, ok: ok}
	}))
	defer server.Close()
	client, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(websocket.StatusNormalClosure, "done")
	select {
	case got := <-results:
		if !got.ok || got.location.Code != "second" {
			t.Fatalf("sequential ambiguity selected %#v, ok=%t", got.location, got.ok)
		}
		if len(got.prompts) != 2 {
			t.Fatalf("prompt count = %d, want 2: %#v", len(got.prompts), got.prompts)
		}
		if !strings.Contains(got.prompts[0], "Springfield") || strings.Contains(got.prompts[0], "Saskatoon") {
			t.Fatalf("first prompt did not contain exactly the current match: %q", got.prompts[0])
		}
		if !strings.Contains(got.prompts[1], "Saskatoon") || !strings.Contains(got.prompts[1], "Press 2") || !strings.Contains(got.prompts[1], "3 for the next") || !strings.Contains(got.prompts[1], "1 to go back") {
			t.Fatalf("second prompt did not contain localized navigation: %q", got.prompts[1])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Twilio ambiguity menu did not complete")
	}
}

func TestTwilioVoiceSearchUsesVADBrokeredASRAndSharedMatcher(t *testing.T) {
	index := testLocationSearchIndex(t)
	enabled := true
	spoolDir := filepath.Join(t.TempDir(), "asr")
	bridge := mockIVRASRBridge(t, "Saskatoon")
	service := &Service{
		searchIndex: index,
		bridge:      bridge,
		cfg: loadedConfig{BaseDir: filepath.Dir(spoolDir), IVR: Config{
			DefaultLanguage: "en-CA",
			Search: searchConfig{
				Enabled: &enabled, VoiceEnabled: &enabled, SpoolDir: spoolDir,
				MultitapWindow: 700 * time.Millisecond, IdleSubmit: 2500 * time.Millisecond,
				ASRTimeout: 2 * time.Second, MaxAttempts: 2, VADPrerollMS: 300,
				VADTrailingMS: 60, MinVoiceMS: 2000, MaxVoiceMS: 8000,
			},
		}},
	}
	selected := make(chan ResolvedLocation, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ws, err := websocket.Accept(writer, request, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer ws.Close(websocket.StatusNormalClosure, "done")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		media := &twilioMediaConnection{
			service: service, ws: ws, streamSID: testTwilioStreamSID, startedAt: time.Now(),
			frames: make(chan pcmFrame, 64), digits: make(chan string, 16), marks: make(chan string, 16),
			promptPCMU: func(context.Context, string, string, string) ([]byte, error) {
				return make([]byte, 160), nil
			},
		}
		go media.readLoop(ctx, "0")
		location, ok := media.searchLocation(ctx, "en-CA", "SK")
		if ok {
			selected <- location
		}
	}))
	defer server.Close()
	client, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(websocket.StatusNormalClosure, "done")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		messageType, raw, err := client.Read(ctx)
		if err != nil {
			t.Fatalf("read search prompt: %v", err)
		}
		if messageType != websocket.MessageText {
			continue
		}
		var outbound map[string]any
		if json.Unmarshal(raw, &outbound) == nil && outbound["event"] == "media" {
			break
		}
	}
	speech := make([]byte, 160)
	for index := range speech {
		speech[index] = linearToULaw(5000)
	}
	silence := make([]byte, 160)
	for index := range silence {
		silence[index] = 0xff
	}
	sequence := 1
	for frameIndex, payload := range [][]byte{speech, speech, speech, silence, silence, silence} {
		writeTwilioTestMessage(t, ctx, client, map[string]any{
			"event": "media", "streamSid": testTwilioStreamSID, "sequenceNumber": fmt.Sprint(sequence),
			"media": map[string]string{
				"track": "inbound", "timestamp": fmt.Sprint(frameIndex * 20),
				"payload": base64.StdEncoding.EncodeToString(payload),
			},
		})
		sequence++
	}
	select {
	case location := <-selected:
		if location.Code != "SK-40" || location.Source != "eccc_forecast" {
			t.Fatalf("unexpected selected location: %#v", location)
		}
	case <-ctx.Done():
		t.Fatal("Twilio voice search did not select Saskatoon")
	}
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		t.Fatalf("read ASR spool: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("IVR retained voice spool files: %#v", entries)
	}
}

func mockIVRASRBridge(t *testing.T, transcript string) *bridgeClient {
	t.Helper()
	ivrSide, brokerSide := net.Pipe()
	client := &bridgeClient{
		conn: ivrSide, events: make(chan map[string]any, 16),
		pendingProducts: map[string]chan productResult{}, pendingWx: map[string]chan wxResult{},
		pendingSynth: map[string]chan synthResult{}, pendingASR: map[string]chan asrResult{},
	}
	go client.readLoop()
	go func() {
		decoder := json.NewDecoder(brokerSide)
		encoder := json.NewEncoder(brokerSide)
		for {
			var event map[string]any
			if decoder.Decode(&event) != nil {
				return
			}
			if event["type"] != "asr.transcribe" {
				continue
			}
			data, _ := event["data"].(map[string]any)
			requestID, _ := data["request_id"].(string)
			_ = encoder.Encode(map[string]any{
				"type": "asr.transcribed", "source": "haze-asr", "subject": requestID,
				"data": map[string]any{
					"request_id": requestID, "text": transcript, "provider": "local_whisper",
					"model": "base-q5_1", "latency_ms": 25,
				},
			})
		}
	}()
	t.Cleanup(func() {
		_ = ivrSide.Close()
		_ = brokerSide.Close()
	})
	return client
}

func sendTwilioStart(t *testing.T, serverURL string, nonce string, accountSID string, callSID string, streamSID string) {
	t.Helper()
	sendTwilioStartWithRate(t, serverURL, nonce, accountSID, callSID, streamSID, 8000)
}

func sendTwilioStartWithRate(t *testing.T, serverURL string, nonce string, accountSID string, callSID string, streamSID string, sampleRate int) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(serverURL, "http")
	connection, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "done")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	writeTwilioTestMessage(t, ctx, connection, map[string]any{"event": "connected", "protocol": "Call", "version": "1.0.0"})
	writeTwilioTestMessage(t, ctx, connection, map[string]any{
		"event": "start", "sequenceNumber": "1", "streamSid": streamSID,
		"start": map[string]any{
			"accountSid": accountSID, "callSid": callSID, "streamSid": streamSID,
			"tracks":           []string{"inbound"},
			"customParameters": map[string]string{"nonce": nonce},
			"mediaFormat":      map[string]any{"encoding": "audio/x-mulaw", "sampleRate": sampleRate, "channels": 1},
		},
	})
}

func writeTwilioTestMessage(t *testing.T, ctx context.Context, connection *websocket.Conn, message map[string]any) {
	t.Helper()
	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatal(err)
	}
}
