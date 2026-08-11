package ivr

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	twilio "github.com/twilio/twilio-go"
	twilioclient "github.com/twilio/twilio-go/client"
	openapi "github.com/twilio/twilio-go/rest/api/v2010"
)

const maxTwilioCallbackBytes = 128 * 1024

type twilioRequestContextKey struct{}

type twilioRequestContext struct {
	MediaToken string
	BaseURL    string
}

type twilioPendingSession struct {
	Token      string
	AccountSID string
	CallSID    string
	Language   string
	Region     string
	ExpiresAt  time.Time
	Used       bool

	callerProvince string
}

type twilioLocationResult struct {
	Token      string
	AccountSID string
	CallSID    string
	Location   ResolvedLocation
	ExpiresAt  time.Time
	Used       bool
}

type twilioMediaGrant struct {
	Token      string
	AccountSID string
	CallSID    string
	Location   ResolvedLocation
	ExpiresAt  time.Time
}

type twilioCallRedirector interface {
	Redirect(ctx context.Context, accountSID string, callSID string, targetURL string) error
}

type twilioSDKRedirector struct {
	client *twilio.RestClient
}

func (redirector twilioSDKRedirector) Redirect(ctx context.Context, accountSID string, callSID string, targetURL string) error {
	if redirector.client == nil {
		return fmt.Errorf("Twilio client is unavailable")
	}
	done := make(chan error, 1)
	go func() {
		params := (&openapi.UpdateCallParams{}).
			SetPathAccountSid(accountSID).
			SetUrl(targetURL).
			SetMethod(http.MethodPost)
		_, err := redirector.client.Api.UpdateCall(callSID, params)
		done <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

type twilioRuntime struct {
	cfg        twilioConfig
	accountSID string
	baseURL    *url.URL
	validator  twilioclient.RequestValidator
	signingKey []byte
	redirector twilioCallRedirector
	streams    chan struct{}

	mu       sync.Mutex
	pending  map[string]*twilioPendingSession
	results  map[string]*twilioLocationResult
	media    map[string]*twilioMediaGrant
	byCallID map[string]string
}

func newTwilioRuntime(cfg loadedConfig) (*twilioRuntime, error) {
	twilioCfg := cfg.IVR.Twilio
	externalURL := firstNonBlank(twilioCfg.ExternalURL, os.Getenv("HAZE_PUBLIC_BASE_URL"))
	parsed, err := url.Parse(strings.TrimSpace(externalURL))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("Twilio external_url must be an absolute HTTPS URL without credentials, query, or fragment")
	}
	accountSID := strings.TrimSpace(os.Getenv(twilioCfg.AccountSIDEnv))
	authToken := strings.TrimSpace(os.Getenv(twilioCfg.AuthTokenEnv))
	signingKey := []byte(os.Getenv(twilioCfg.SigningKeyEnv))
	if !validTwilioSID(accountSID, "AC") || authToken == "" {
		return nil, fmt.Errorf("Twilio account credentials are not configured")
	}
	if len(signingKey) < 32 {
		return nil, fmt.Errorf("IVR signing key must contain at least 32 bytes")
	}
	client := twilio.NewRestClientWithParams(twilio.ClientParams{
		Username: accountSID, Password: authToken, AccountSid: accountSID,
	})
	client.SetTimeout(15 * time.Second)
	return &twilioRuntime{
		cfg:        twilioCfg,
		accountSID: accountSID,
		baseURL:    parsed,
		validator:  twilioclient.NewRequestValidator(authToken),
		signingKey: append([]byte(nil), signingKey...),
		redirector: twilioSDKRedirector{client: client},
		streams:    make(chan struct{}, twilioCfg.MaxConcurrentStreams),
		pending:    map[string]*twilioPendingSession{},
		results:    map[string]*twilioLocationResult{},
		media:      map[string]*twilioMediaGrant{},
		byCallID:   map[string]string{},
	}, nil
}

func (runtime *twilioRuntime) newPending(accountSID string, callSID string, language string, region string, caller ...string) (*twilioPendingSession, error) {
	if runtime == nil || accountSID != runtime.accountSID || !validTwilioSID(callSID, "CA") {
		return nil, fmt.Errorf("invalid Twilio call identity")
	}
	callerProvince := ""
	if len(caller) > 0 {
		callerProvince = callerProvinceHint(caller[0])
	}
	expires := time.Now().UTC().Add(runtime.cfg.SessionTTL)
	token, err := runtime.newBoundToken("search", accountSID, callSID, language, region, expires)
	if err != nil {
		return nil, err
	}
	session := &twilioPendingSession{
		Token: token, AccountSID: accountSID, CallSID: callSID, Language: language,
		Region: region, ExpiresAt: expires, callerProvince: callerProvince,
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.cleanupLocked(time.Now().UTC())
	if previous := runtime.byCallID[callSID]; previous != "" {
		delete(runtime.pending, previous)
	}
	runtime.pending[token] = session
	runtime.byCallID[callSID] = token
	clone := *session
	return &clone, nil
}

func (runtime *twilioRuntime) consumePending(token string, accountSID string, callSID string) (*twilioPendingSession, bool) {
	if runtime == nil {
		return nil, false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	now := time.Now().UTC()
	runtime.cleanupLocked(now)
	session := runtime.pending[token]
	if session == nil || session.Used || now.After(session.ExpiresAt) || session.AccountSID != accountSID || session.CallSID != callSID || !runtime.verifyToken("search", token, session.AccountSID, session.CallSID, session.Language, session.Region, session.ExpiresAt) {
		return nil, false
	}
	session.Used = true
	clone := *session
	return &clone, true
}

func (runtime *twilioRuntime) recoverySession(token string, accountSID string, callSID string) (*twilioPendingSession, bool) {
	if runtime == nil {
		return nil, false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.cleanupLocked(time.Now().UTC())
	session := runtime.pending[token]
	if session == nil || session.AccountSID != accountSID || session.CallSID != callSID || !runtime.verifyToken("search", token, session.AccountSID, session.CallSID, session.Language, session.Region, session.ExpiresAt) {
		return nil, false
	}
	clone := *session
	delete(runtime.pending, token)
	if runtime.byCallID[session.CallSID] == token {
		delete(runtime.byCallID, session.CallSID)
	}
	return &clone, true
}

func (runtime *twilioRuntime) storeResult(session *twilioPendingSession, location ResolvedLocation) (string, error) {
	if runtime == nil || session == nil {
		return "", fmt.Errorf("Twilio search session is unavailable")
	}
	expires := time.Now().UTC().Add(runtime.cfg.SessionTTL)
	token, err := runtime.newBoundToken("result", session.AccountSID, session.CallSID, location.Source, location.Code, expires)
	if err != nil {
		return "", err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	delete(runtime.pending, session.Token)
	if runtime.byCallID[session.CallSID] == session.Token {
		delete(runtime.byCallID, session.CallSID)
	}
	runtime.results[token] = &twilioLocationResult{
		Token: token, AccountSID: session.AccountSID, CallSID: session.CallSID,
		Location: location, ExpiresAt: expires,
	}
	return token, nil
}

func (runtime *twilioRuntime) consumeResult(token string, accountSID string, callSID string) (*twilioLocationResult, bool) {
	if runtime == nil {
		return nil, false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	now := time.Now().UTC()
	runtime.cleanupLocked(now)
	result := runtime.results[token]
	if result == nil || result.Used || result.AccountSID != accountSID || result.CallSID != callSID || !runtime.verifyToken("result", token, result.AccountSID, result.CallSID, result.Location.Source, result.Location.Code, result.ExpiresAt) {
		return nil, false
	}
	result.Used = true
	clone := *result
	return &clone, true
}

func (runtime *twilioRuntime) createMediaGrant(result *twilioLocationResult) (string, error) {
	if runtime == nil || result == nil {
		return "", fmt.Errorf("Twilio location result is unavailable")
	}
	return runtime.createBoundMediaGrant(result.AccountSID, result.CallSID, result.Location)
}

func (runtime *twilioRuntime) createCallMediaGrant(accountSID string, callSID string) (string, error) {
	if runtime == nil || accountSID != runtime.accountSID || !validTwilioSID(callSID, "CA") {
		return "", fmt.Errorf("invalid Twilio call identity")
	}
	return runtime.createBoundMediaGrant(accountSID, callSID, ResolvedLocation{})
}

func (runtime *twilioRuntime) createBoundMediaGrant(accountSID string, callSID string, location ResolvedLocation) (string, error) {
	expires := time.Now().UTC().Add(maxDuration(runtime.cfg.SessionTTL, 15*time.Minute))
	token, err := runtime.newBoundToken("media", accountSID, callSID, location.Source, location.Code, expires)
	if err != nil {
		return "", err
	}
	runtime.mu.Lock()
	runtime.media[token] = &twilioMediaGrant{
		Token: token, AccountSID: accountSID, CallSID: callSID,
		Location: location, ExpiresAt: expires,
	}
	runtime.mu.Unlock()
	return token, nil
}

func (runtime *twilioRuntime) mediaGrant(token string) (*twilioMediaGrant, bool) {
	if runtime == nil {
		return nil, false
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.cleanupLocked(time.Now().UTC())
	grant := runtime.media[token]
	if grant == nil || !runtime.verifyToken("media", token, grant.AccountSID, grant.CallSID, grant.Location.Source, grant.Location.Code, grant.ExpiresAt) {
		return nil, false
	}
	clone := *grant
	return &clone, true
}

func (runtime *twilioRuntime) newBoundToken(purpose string, fields ...any) (string, error) {
	random := make([]byte, 24)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	randomText := base64.RawURLEncoding.EncodeToString(random)
	mac := hmac.New(sha256.New, runtime.signingKey)
	_, _ = mac.Write([]byte(runtime.tokenMessage(purpose, randomText, fields...)))
	return randomText + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (runtime *twilioRuntime) verifyToken(purpose string, token string, fields ...any) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return false
	}
	provided, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, runtime.signingKey)
	_, _ = mac.Write([]byte(runtime.tokenMessage(purpose, parts[0], fields...)))
	return subtle.ConstantTimeCompare(provided, mac.Sum(nil)) == 1
}

func (runtime *twilioRuntime) tokenMessage(purpose string, randomText string, fields ...any) string {
	parts := []string{purpose, randomText}
	for _, field := range fields {
		switch value := field.(type) {
		case time.Time:
			parts = append(parts, value.UTC().Format(time.RFC3339Nano))
		default:
			parts = append(parts, fmt.Sprint(value))
		}
	}
	return strings.Join(parts, "\x00")
}

func (runtime *twilioRuntime) cleanupLocked(now time.Time) {
	for token, session := range runtime.pending {
		if now.After(session.ExpiresAt) {
			delete(runtime.pending, token)
			if runtime.byCallID[session.CallSID] == token {
				delete(runtime.byCallID, session.CallSID)
			}
		}
	}
	for token, result := range runtime.results {
		if now.After(result.ExpiresAt) {
			delete(runtime.results, token)
		}
	}
	for token, grant := range runtime.media {
		if now.After(grant.ExpiresAt) {
			delete(runtime.media, token)
		}
	}
}

func (runtime *twilioRuntime) publicHTTPURL(route string, rawQuery string) string {
	copyURL := *runtime.baseURL
	copyURL.Path = joinPublicURLPath(copyURL.Path, route)
	copyURL.RawPath = ""
	copyURL.RawQuery = rawQuery
	return copyURL.String()
}

func (runtime *twilioRuntime) publicWSURL(route string) string {
	copyURL := *runtime.baseURL
	copyURL.Scheme = "wss"
	copyURL.Path = joinPublicURLPath(copyURL.Path, route)
	copyURL.RawPath = ""
	copyURL.RawQuery = ""
	return copyURL.String()
}

func joinPublicURLPath(base string, route string) string {
	base = strings.TrimSuffix(base, "/")
	return path.Clean("/" + strings.TrimPrefix(base+"/"+strings.TrimPrefix(route, "/"), "/"))
}

func (runtime *twilioRuntime) validateHTTP(request *http.Request, body []byte) bool {
	if runtime == nil || request == nil {
		return false
	}
	publicURL := runtime.publicHTTPURL(request.URL.EscapedPath(), request.URL.RawQuery)
	signature := request.Header.Get("X-Twilio-Signature")
	if signature == "" || !runtime.validator.ValidateBody(publicURL, body, signature) {
		return false
	}
	values, _ := url.ParseQuery(string(body))
	if accountSID := values.Get("AccountSid"); accountSID != "" && accountSID != runtime.accountSID {
		return false
	}
	return true
}

func (runtime *twilioRuntime) validateWebSocket(request *http.Request) bool {
	if runtime == nil || request == nil {
		return false
	}
	signature := request.Header.Get("x-twilio-signature")
	if signature == "" {
		return false
	}
	wssURL := runtime.publicWSURL("/ivr/v1/twilio/media")
	if runtime.validator.Validate(wssURL, map[string]string{}, signature) {
		return true
	}
	httpsURL := strings.Replace(wssURL, "wss://", "https://", 1)
	return runtime.validator.Validate(httpsURL, map[string]string{}, signature)
}

func (s *Service) requireTwilioCallback(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if s == nil || s.twilio == nil {
			http.Error(writer, "Twilio IVR is unavailable", http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(io.LimitReader(request.Body, maxTwilioCallbackBytes+1))
		if err != nil || len(body) > maxTwilioCallbackBytes {
			http.Error(writer, "request too large", http.StatusRequestEntityTooLarge)
			return
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		if !s.twilio.validateHTTP(request, body) {
			http.Error(writer, "invalid Twilio signature", http.StatusForbidden)
			return
		}
		token := strings.TrimSpace(request.URL.Query().Get("twilio_token"))
		if token != "" {
			grant, ok := s.twilio.mediaGrant(token)
			if !ok || grant.AccountSID != request.FormValue("AccountSid") || grant.CallSID != request.FormValue("CallSid") {
				http.Error(writer, "Twilio media session does not match callback", http.StatusForbidden)
				return
			}
		} else {
			var err error
			token, err = s.twilio.createCallMediaGrant(request.FormValue("AccountSid"), request.FormValue("CallSid"))
			if err != nil {
				http.Error(writer, "invalid Twilio call identity", http.StatusForbidden)
				return
			}
		}
		request = request.WithContext(context.WithValue(request.Context(), twilioRequestContextKey{}, twilioRequestContext{
			MediaToken: token,
			BaseURL:    strings.TrimRight(s.twilio.baseURL.String(), "/"),
		}))
		next.ServeHTTP(writer, request)
	})
}

func (s *Service) requireTwilioMedia(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		token := strings.TrimSpace(request.URL.Query().Get("twilio_token"))
		grant, ok := s.twilio.mediaGrant(token)
		if !ok {
			http.Error(writer, "invalid or expired media token", http.StatusForbidden)
			return
		}
		if strings.HasSuffix(request.URL.Path, "/audio") || strings.HasSuffix(request.URL.Path, "/alert_audio") {
			if source := request.URL.Query().Get("source"); source != "" && !strings.EqualFold(source, grant.Location.Source) {
				http.Error(writer, "media token does not match location", http.StatusForbidden)
				return
			}
			if code := request.URL.Query().Get("code"); code != "" && !strings.EqualFold(code, grant.Location.Code) {
				http.Error(writer, "media token does not match location", http.StatusForbidden)
				return
			}
		}
		next(writer, request)
	}
}

func validTwilioSID(value string, prefix string) bool {
	if len(value) != 34 || !strings.HasPrefix(value, prefix) {
		return false
	}
	for _, char := range value[2:] {
		if char < '0' || char > '9' && char < 'A' || char > 'F' && char < 'a' || char > 'f' {
			return false
		}
	}
	return true
}

// normalizeNANPCaller accepts the common Twilio forms of a Canadian or US
// NANP number. It returns an E.164 number or an empty string for anonymous,
// international, malformed, or extension-bearing values.
func normalizeNANPCaller(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	hasCountryPrefix := strings.HasPrefix(value, "+")
	var digits strings.Builder
	for index, char := range value {
		switch {
		case char >= '0' && char <= '9':
			digits.WriteRune(char)
		case char == '+' && index == 0:
		case char == ' ' || char == '-' || char == '.' || char == '(' || char == ')':
		default:
			return ""
		}
	}
	number := digits.String()
	if hasCountryPrefix && (len(number) != 11 || number[0] != '1') {
		return ""
	}
	if len(number) == 11 && number[0] == '1' {
		number = number[1:]
	}
	if len(number) != 10 || number[0] < '2' || number[0] > '9' || number[3] < '2' || number[3] > '9' {
		return ""
	}
	return "+1" + number
}

// callerProvinceHint derives a conservative Canadian province hint from a
// NANP area code. Shared area codes and non-geographic codes deliberately
// return no hint.
func callerProvinceHint(caller string) string {
	caller = normalizeNANPCaller(caller)
	if len(caller) != 12 {
		return ""
	}
	return canadianAreaCodeProvinces[caller[2:5]]
}

// The 902/782 codes shared by Nova Scotia and Prince Edward Island, and 867
// shared by all three territories, are intentionally omitted.
var canadianAreaCodeProvinces = map[string]string{
	"368": "AB", "403": "AB", "568": "AB", "587": "AB", "780": "AB", "825": "AB",
	"236": "BC", "250": "BC", "257": "BC", "604": "BC", "672": "BC", "778": "BC",
	"204": "MB", "431": "MB", "584": "MB",
	"428": "NB", "506": "NB",
	"709": "NL", "879": "NL",
	"226": "ON", "249": "ON", "289": "ON", "343": "ON", "365": "ON", "382": "ON",
	"416": "ON", "437": "ON", "519": "ON", "548": "ON", "613": "ON", "647": "ON",
	"683": "ON", "705": "ON", "742": "ON", "753": "ON", "807": "ON", "905": "ON", "942": "ON",
	"263": "QC", "354": "QC", "367": "QC", "418": "QC", "438": "QC", "450": "QC",
	"468": "QC", "514": "QC", "579": "QC", "581": "QC", "819": "QC", "873": "QC",
	"306": "SK", "474": "SK", "639": "SK",
}

func maxDuration(left time.Duration, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}
