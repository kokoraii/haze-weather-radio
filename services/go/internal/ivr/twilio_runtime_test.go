package ivr

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1" // #nosec G505 -- Twilio webhook signatures are defined with HMAC-SHA1.
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	twilioclient "github.com/twilio/twilio-go/client"
)

const (
	testTwilioAccountSID = "AC00000000000000000000000000000000"
	testTwilioCallSID    = "CA11111111111111111111111111111111"
	testTwilioStreamSID  = "MZ22222222222222222222222222222222"
	testTwilioAuthToken  = "test-twilio-auth-token"
)

func testTwilioRuntime(t *testing.T) *twilioRuntime {
	t.Helper()
	base, err := url.Parse("https://weather.example.test/public")
	if err != nil {
		t.Fatal(err)
	}
	return &twilioRuntime{
		cfg: twilioConfig{
			SessionTTL: 2 * time.Minute, MaxConcurrentStreams: 4, MaxMessageBytes: 64 * 1024,
		},
		accountSID: testTwilioAccountSID,
		baseURL:    base,
		validator:  twilioclient.NewRequestValidator(testTwilioAuthToken),
		signingKey: []byte("0123456789abcdef0123456789abcdef"),
		streams:    make(chan struct{}, 4),
		pending:    map[string]*twilioPendingSession{},
		results:    map[string]*twilioLocationResult{},
		media:      map[string]*twilioMediaGrant{},
		byCallID:   map[string]string{},
	}
}

func TestTwilioSearchNonceBindsIdentityAndRejectsReplay(t *testing.T) {
	t.Parallel()
	runtime := testTwilioRuntime(t)
	session, err := runtime.newPending(testTwilioAccountSID, testTwilioCallSID, "fr-CA", "QC")
	if err != nil {
		t.Fatalf("new pending session: %v", err)
	}
	if _, ok := runtime.consumePending(session.Token, testTwilioAccountSID, "CA33333333333333333333333333333333"); ok {
		t.Fatal("nonce was accepted for a different call")
	}
	consumed, ok := runtime.consumePending(session.Token, testTwilioAccountSID, testTwilioCallSID)
	if !ok || consumed.Language != "fr-CA" || consumed.Region != "QC" {
		t.Fatalf("bound session was not recovered: %#v", consumed)
	}
	if _, ok := runtime.consumePending(session.Token, testTwilioAccountSID, testTwilioCallSID); ok {
		t.Fatal("one-use nonce was replayed")
	}
	forged := session.Token[:len(session.Token)-1] + "A"
	if _, ok := runtime.consumePending(forged, testTwilioAccountSID, testTwilioCallSID); ok {
		t.Fatal("forged nonce was accepted")
	}
}

func TestTwilioRecoveryConsumesUnexpectedlyEndedStreamOnce(t *testing.T) {
	t.Parallel()
	runtime := testTwilioRuntime(t)
	session, err := runtime.newPending(testTwilioAccountSID, testTwilioCallSID, "en-CA", "SK")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := runtime.consumePending(session.Token, testTwilioAccountSID, testTwilioCallSID); !ok {
		t.Fatal("stream could not consume nonce")
	}
	recovered, ok := runtime.recoverySession(session.Token, testTwilioAccountSID, testTwilioCallSID)
	if !ok || recovered.Region != "SK" {
		t.Fatalf("unexpected stream termination was not recoverable: %#v", recovered)
	}
	if _, ok := runtime.recoverySession(session.Token, testTwilioAccountSID, testTwilioCallSID); ok {
		t.Fatal("recovery action was replayed")
	}
}

func TestTwilioCallbackSignatureUsesConfiguredExternalURL(t *testing.T) {
	t.Parallel()
	runtime := testTwilioRuntime(t)
	service := &Service{twilio: runtime}
	body := url.Values{"AccountSid": {testTwilioAccountSID}, "CallSid": {testTwilioCallSID}}.Encode()
	publicURL := runtime.publicHTTPURL("/ivr/v1/twiml", "state=location_number")
	request := httptest.NewRequest(http.MethodPost, "http://forged-host.invalid/ivr/v1/twiml?state=location_number", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("X-Forwarded-Host", "also-forged.invalid")
	request.Header.Set("X-Twilio-Signature", signTwilioRequest(publicURL, parseSingleValues(t, body), testTwilioAuthToken))
	called := false
	handler := service.requireTwilioCallback(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		called = true
		if request.FormValue("CallSid") != testTwilioCallSID {
			t.Errorf("callback body was not restored")
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || !called {
		t.Fatalf("configured external URL signature was rejected: status=%d", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "http://forged-host.invalid/ivr/v1/twiml?state=location_number", strings.NewReader(body))
	request.Header.Set("X-Twilio-Signature", signTwilioRequest("http://forged-host.invalid/ivr/v1/twiml?state=location_number", parseSingleValues(t, body), testTwilioAuthToken))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("forged host signature status = %d", response.Code)
	}
}

func TestTwilioInitialCallbackCreatesCallBoundProtectedMediaURL(t *testing.T) {
	t.Parallel()
	runtime := testTwilioRuntime(t)
	service := &Service{twilio: runtime}
	body := url.Values{"AccountSid": {testTwilioAccountSID}, "CallSid": {testTwilioCallSID}}.Encode()
	publicURL := runtime.publicHTTPURL("/ivr/v1/twiml", "state=location_number")
	request := httptest.NewRequest(http.MethodPost, "http://forged-host.invalid/ivr/v1/twiml?state=location_number", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("X-Forwarded-Host", "also-forged.invalid")
	request.Header.Set("X-Twilio-Signature", signTwilioRequest(publicURL, parseSingleValues(t, body), testTwilioAuthToken))

	response := httptest.NewRecorder()
	service.requireTwilioCallback(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(twimlURL(request, "/ivr/v1/prompt", map[string]string{"line": "welcome"})))
	})).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("callback status = %d", response.Code)
	}
	protectedURL, err := url.Parse(response.Body.String())
	if err != nil {
		t.Fatal(err)
	}
	if protectedURL.Scheme != "https" || protectedURL.Host != "weather.example.test" || protectedURL.Path != "/public/ivr/v1/twilio/prompt" {
		t.Fatalf("protected prompt did not use the configured external URL: %q", protectedURL.String())
	}
	mediaToken := protectedURL.Query().Get("twilio_token")
	grant, ok := runtime.mediaGrant(mediaToken)
	if !ok || grant.AccountSID != testTwilioAccountSID || grant.CallSID != testTwilioCallSID {
		t.Fatalf("initial media token was not bound to the callback identity: %#v", grant)
	}
}

func TestTwilioCallbackRejectsOversizedBodyBeforeValidation(t *testing.T) {
	t.Parallel()
	service := &Service{twilio: testTwilioRuntime(t)}
	request := httptest.NewRequest(http.MethodPost, "http://internal/ivr/v1/twiml", strings.NewReader(strings.Repeat("A", maxTwilioCallbackBytes+1)))
	response := httptest.NewRecorder()
	service.requireTwilioCallback(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized callback reached handler")
	})).ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized callback status = %d", response.Code)
	}
}

func TestTwilioConnectUsesCustomNonceWithoutWebSocketQuery(t *testing.T) {
	t.Parallel()
	enabled := true
	service := &Service{
		cfg:         loadedConfig{IVR: Config{DefaultLanguage: "en-CA", Search: searchConfig{Enabled: &enabled}}},
		searchIndex: &locationSearchIndex{}, twilio: testTwilioRuntime(t),
	}
	form := url.Values{"AccountSid": {testTwilioAccountSID}, "CallSid": {testTwilioCallSID}}
	request := httptest.NewRequest(http.MethodPost, "http://internal/ivr/v1/twiml", bytes.NewBufferString(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	service.writeTwilioSearchConnect(response, request, "en-CA", "SK")
	body := response.Body.String()
	if !strings.Contains(body, `<Stream url="wss://weather.example.test/public/ivr/v1/twilio/media"`) {
		t.Fatalf("TwiML did not use configured WSS URL: %s", body)
	}
	if strings.Contains(body, `/twilio/media?`) || !strings.Contains(body, `<Parameter name="nonce" value="`) {
		t.Fatalf("nonce was not passed as a custom parameter: %s", body)
	}
	if !strings.Contains(body, `action="https://weather.example.test/public/ivr/v1/twilio/recover/`) {
		t.Fatalf("Connect recovery action missing: %s", body)
	}
}

func TestTwilioResultAndMediaTokensAreOpaqueAndLocationBound(t *testing.T) {
	t.Parallel()
	runtime := testTwilioRuntime(t)
	session, err := runtime.newPending(testTwilioAccountSID, testTwilioCallSID, "en-CA", "SK")
	if err != nil {
		t.Fatal(err)
	}
	location := ResolvedLocation{Source: "hello_weather", Code: "06040", Name: "Saskatoon", Province: "SK", Country: "CA"}
	token, err := runtime.storeResult(session, location)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(token, location.Name) || strings.Contains(token, location.Code) {
		t.Fatalf("result token exposes location: %q", token)
	}
	result, ok := runtime.consumeResult(token, testTwilioAccountSID, testTwilioCallSID)
	if !ok || result.Location.Code != location.Code {
		t.Fatalf("result could not be consumed: %#v", result)
	}
	if _, ok := runtime.consumeResult(token, testTwilioAccountSID, testTwilioCallSID); ok {
		t.Fatal("result URL was replayed")
	}
	mediaToken, err := runtime.createMediaGrant(result)
	if err != nil {
		t.Fatal(err)
	}
	grant, ok := runtime.mediaGrant(mediaToken)
	if !ok || grant.Location.Source != location.Source || grant.Location.Code != location.Code {
		t.Fatalf("media grant lost source-qualified location: %#v", grant)
	}
	if _, ok := runtime.recoverySession(session.Token, testTwilioAccountSID, testTwilioCallSID); ok {
		t.Fatal("completed search still allowed the recovery action to replace its result")
	}
}

func TestTwilioCallbackCarriesCallBoundMediaGrant(t *testing.T) {
	t.Parallel()
	runtime := testTwilioRuntime(t)
	service := &Service{twilio: runtime}
	session, err := runtime.newPending(testTwilioAccountSID, testTwilioCallSID, "en-CA", "SK")
	if err != nil {
		t.Fatal(err)
	}
	resultToken, err := runtime.storeResult(session, ResolvedLocation{Source: "hello_weather", Code: "06040"})
	if err != nil {
		t.Fatal(err)
	}
	result, ok := runtime.consumeResult(resultToken, testTwilioAccountSID, testTwilioCallSID)
	if !ok {
		t.Fatal("consume result")
	}
	mediaToken, err := runtime.createMediaGrant(result)
	if err != nil {
		t.Fatal(err)
	}

	serve := func(callSID string) *httptest.ResponseRecorder {
		body := url.Values{"AccountSid": {testTwilioAccountSID}, "CallSid": {callSID}}.Encode()
		rawQuery := url.Values{"state": {"location_option"}, "twilio_token": {mediaToken}}.Encode()
		publicURL := runtime.publicHTTPURL("/ivr/v1/twiml", rawQuery)
		request := httptest.NewRequest(http.MethodPost, "http://internal/ivr/v1/twiml?"+rawQuery, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		request.Header.Set("X-Twilio-Signature", signTwilioRequest(publicURL, parseSingleValues(t, body), testTwilioAuthToken))
		response := httptest.NewRecorder()
		service.requireTwilioCallback(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			generated := twimlURL(request, "/ivr/v1/audio", map[string]string{"source": "hello_weather", "code": "06040"})
			_, _ = writer.Write([]byte(generated))
		})).ServeHTTP(response, request)
		return response
	}

	response := serve(testTwilioCallSID)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "/ivr/v1/twilio/audio") || !strings.Contains(response.Body.String(), "twilio_token=") {
		t.Fatalf("media grant was not propagated: status=%d body=%q", response.Code, response.Body.String())
	}
	response = serve("CA33333333333333333333333333333333")
	if response.Code != http.StatusForbidden {
		t.Fatalf("media grant accepted for a different call: %d", response.Code)
	}
}

func TestTwilioResolvedNumericLocationUpgradesPromptOnlyGrant(t *testing.T) {
	t.Parallel()
	runtime := testTwilioRuntime(t)
	service := &Service{twilio: runtime}
	promptToken, err := runtime.createCallMediaGrant(testTwilioAccountSID, testTwilioCallSID)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://internal/ivr/v1/twiml", nil)
	request = request.WithContext(context.WithValue(request.Context(), twilioRequestContextKey{}, twilioRequestContext{
		MediaToken: promptToken,
		BaseURL:    strings.TrimRight(runtime.baseURL.String(), "/"),
	}))
	location := ResolvedLocation{Source: "hello_weather", Code: "06040", Name: "Saskatoon"}
	request = service.twilioLocationRequest(request, location)
	protectedURL, err := url.Parse(twimlURL(request, "/ivr/v1/audio", locationTwiMLParams(location)))
	if err != nil {
		t.Fatal(err)
	}
	upgradedToken := protectedURL.Query().Get("twilio_token")
	if upgradedToken == "" || upgradedToken == promptToken {
		t.Fatalf("numeric location did not receive a location-bound token: %q", protectedURL.String())
	}
	grant, ok := runtime.mediaGrant(upgradedToken)
	if !ok || grant.Location.Source != location.Source || grant.Location.Code != location.Code {
		t.Fatalf("upgraded token is not bound to the canonical location: %#v", grant)
	}
}

func TestTwilioWebSocketSignatureIgnoresForwardedHost(t *testing.T) {
	t.Parallel()
	runtime := testTwilioRuntime(t)
	request := httptest.NewRequest(http.MethodGet, "http://forged.invalid/ivr/v1/twilio/media", nil)
	request.Header.Set("X-Forwarded-Host", "forged.invalid")
	configured := runtime.publicWSURL("/ivr/v1/twilio/media")
	request.Header.Set("x-twilio-signature", signTwilioRequest(configured, nil, testTwilioAuthToken))
	if !runtime.validateWebSocket(request) {
		t.Fatal("configured WSS signature was rejected")
	}
	request.Header.Set("x-twilio-signature", signTwilioRequest("ws://forged.invalid/ivr/v1/twilio/media", nil, testTwilioAuthToken))
	if runtime.validateWebSocket(request) {
		t.Fatal("forged host WSS signature was accepted")
	}
}

func parseSingleValues(t *testing.T, encoded string) map[string]string {
	t.Helper()
	values, err := url.ParseQuery(encoded)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for key, value := range values {
		out[key] = value[0]
	}
	return out
}

func signTwilioRequest(target string, params map[string]string, token string) string {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var message strings.Builder
	message.WriteString(target)
	for _, key := range keys {
		message.WriteString(key)
		message.WriteString(params[key])
	}
	mac := hmac.New(sha1.New, []byte(token)) // #nosec G401 -- Twilio mandates HMAC-SHA1.
	_, _ = mac.Write([]byte(message.String()))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

type fakeRedirector struct {
	mu      sync.Mutex
	targets []string
	err     error
}

func (redirector *fakeRedirector) Redirect(_ context.Context, accountSID string, callSID string, targetURL string) error {
	if accountSID != testTwilioAccountSID || callSID != testTwilioCallSID {
		return fmt.Errorf("unexpected call identity")
	}
	redirector.mu.Lock()
	redirector.targets = append(redirector.targets, targetURL)
	redirector.mu.Unlock()
	return redirector.err
}
