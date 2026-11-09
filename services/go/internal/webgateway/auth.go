package webgateway

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/argon2"
)

const sessionCookieName = "haze_admin_session"

// AuthManager owns in-memory admin sessions for the Go web gateway.
type AuthManager struct {
	enabled      bool
	secureCookie bool
	mode         string
	accounts     *accountAuth
	password     []byte
	passwordHash string
	ttl          time.Duration
	mu           sync.Mutex
	sessions     map[string]time.Time
	revoked      map[string]time.Time
}

// NewAuthManager creates an authentication manager from config and environment.
func NewAuthManager(config Config) *AuthManager {
	return NewAuthManagerWithPath(config, "config.yaml")
}

// NewAuthManagerWithPath creates an authentication manager using the configured runtime directory.
func NewAuthManagerWithPath(config Config, configPath string) *AuthManager {
	ttlSeconds := config.Webpanel.Authentication.SessionTTLSeconds
	if ttlSeconds <= 0 {
		ttlSeconds = 12 * 60 * 60
	}
	enabled := true
	if config.Webpanel.Authentication.Enabled != nil {
		enabled = *config.Webpanel.Authentication.Enabled
	}
	manager := &AuthManager{
		enabled:      enabled,
		secureCookie: config.Webpanel.Authentication.SecureCookies,
		mode:         strings.ToLower(strings.TrimSpace(config.Webpanel.Authentication.Mode)),
		password:     []byte(os.Getenv("ADMIN_PASSWD")),
		passwordHash: strings.TrimSpace(os.Getenv("ADMIN_PASSWD_HASH")),
		ttl:          time.Duration(ttlSeconds) * time.Second,
		sessions:     make(map[string]time.Time),
		revoked:      make(map[string]time.Time),
	}
	if manager.mode == "accounts" {
		// Account sessions are always Secure cookies. Account login also
		// rejects requests that are not HTTPS or from a trusted HTTPS proxy.
		manager.secureCookie = true
		manager.accounts = newAccountAuth(config, configPath)
		manager.password = nil
		manager.passwordHash = ""
	}
	return manager
}

func (a *AuthManager) Enabled() bool {
	return a != nil && a.enabled
}

func (a *AuthManager) Configured() bool {
	if a == nil || !a.enabled {
		return true
	}
	if a.accounts != nil {
		return a.accounts.configured
	}
	return len(a.password) > 0 || a.passwordHash != ""
}

func (a *AuthManager) Login(password string) (string, error) {
	if a == nil || !a.enabled {
		return "", nil
	}
	if a.accounts != nil {
		return "", errors.New("username and request context are required for account login")
	}
	if a.passwordHash != "" {
		ok, err := verifyArgon2IDPHC(a.passwordHash, password)
		if err != nil {
			return "", errors.New("operator password hash is invalid")
		}
		if !ok {
			return "", errors.New("incorrect password")
		}
	} else if len(a.password) == 0 {
		return "", errors.New("operator password is not configured")
	} else if subtle.ConstantTimeCompare([]byte(password), a.password) != 1 {
		return "", errors.New("incorrect password")
	}
	token, err := randomToken()
	if err != nil {
		return "", err
	}
	token, err = a.signSessionToken(token, time.Now())
	if err != nil {
		return "", err
	}
	a.mu.Lock()
	now := time.Now()
	a.cleanupSessionsLocked(now)
	if _, signed := a.sessionTokenExpiry(token); !signed {
		a.sessions[token] = now.Add(a.ttl)
	}
	a.mu.Unlock()
	return token, nil
}

func (a *AuthManager) Logout(token string) {
	if a == nil || token == "" {
		return
	}
	a.mu.Lock()
	delete(a.sessions, token)
	if expires, ok := a.sessionTokenExpiry(token); ok && expires.After(time.Now()) {
		a.revoked[tokenDigest(token)] = expires
	}
	a.cleanupRevokedLocked(time.Now())
	a.mu.Unlock()
}

func (a *AuthManager) Authenticated(request *http.Request) bool {
	if a == nil || !a.enabled {
		return true
	}
	if a.accounts != nil {
		_, err := a.accounts.Authenticate(request)
		return err == nil
	}
	return a.ValidToken(tokenFromRequest(request))
}

// FullyAuthenticated excludes sessions that are restricted to changing an
// expired password. The admin shell and profile password command deliberately
// use Authenticated so an operator can complete that recovery flow.
func (a *AuthManager) FullyAuthenticated(request *http.Request) bool {
	if a == nil || !a.enabled {
		return true
	}
	if a.accounts != nil {
		identity, err := a.accounts.Authenticate(request)
		return err == nil && !identity.PasswordChangeRequired
	}
	return a.ValidToken(tokenFromRequest(request))
}

func (a *AuthManager) ValidToken(token string) bool {
	if a == nil || !a.enabled {
		return true
	}
	if a.accounts != nil {
		return false
	}
	if token == "" {
		return false
	}
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cleanupRevokedLocked(now)
	a.cleanupSessionsLocked(now)
	if _, revoked := a.revoked[tokenDigest(token)]; revoked {
		return false
	}
	if expires, ok := a.sessionTokenExpiry(token); ok {
		return expires.After(now)
	}
	expires, ok := a.sessions[token]
	if !ok {
		return false
	}
	if !expires.After(now) {
		delete(a.sessions, token)
		return false
	}
	return true
}

func (a *AuthManager) SetCookie(writer http.ResponseWriter, token string) {
	a.SetCookieForRequest(writer, nil, token)
}

func (a *AuthManager) SetCookieForRequest(writer http.ResponseWriter, request *http.Request, token string) {
	if a == nil || !a.enabled || token == "" {
		return
	}
	http.SetCookie(writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(a.ttl.Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   a.cookieSecureForRequest(request),
	})
}

// LoginWithRequest authenticates an account and creates a PASETO session in account mode.
func (a *AuthManager) LoginWithRequest(ctx context.Context, input LoginInput) (LoginResult, error) {
	if a == nil || !a.enabled {
		return LoginResult{}, nil
	}
	if a.accounts == nil {
		token, err := a.Login(input.Password)
		return LoginResult{Token: token, Persistent: true}, err
	}
	return a.accounts.Login(ctx, input)
}

// Identity returns the authenticated account principal for a request.
func (a *AuthManager) Identity(request *http.Request) (Identity, error) {
	if a == nil || !a.enabled {
		return Identity{Account: Account{Username: "admin", IsAdmin: true, AllowOrigination: true, AllowedOriginators: []string{"CIV", "EAS", "PEP", "WXR"}}}, nil
	}
	if a.accounts == nil {
		if !a.ValidToken(tokenFromRequest(request)) {
			return Identity{}, &AuthError{Code: "unauthorized", Detail: "Authentication is required.", HTTPStatus: http.StatusUnauthorized}
		}
		return Identity{Account: Account{Username: "admin", IsAdmin: true, AllowOrigination: true, AllowedOriginators: []string{"CIV", "EAS", "PEP", "WXR"}}}, nil
	}
	return a.accounts.Authenticate(request)
}

// LogoutRequest revokes the request's active session.
func (a *AuthManager) LogoutRequest(request *http.Request) error {
	if a == nil {
		return nil
	}
	if a.accounts != nil {
		return a.accounts.Logout(request)
	}
	a.Logout(tokenFromRequest(request))
	return nil
}

// SetLoginCookie writes a browser-close or persistent session cookie.
func (a *AuthManager) SetLoginCookie(writer http.ResponseWriter, request *http.Request, result LoginResult) {
	if a == nil || !a.enabled || result.Token == "" {
		return
	}
	if a.accounts == nil {
		a.SetCookieForRequest(writer, request, result.Token)
		return
	}
	cookie := &http.Cookie{
		Name:     sessionCookieName,
		Value:    result.Token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   a.cookieSecureForRequest(request),
	}
	if result.Persistent {
		cookie.MaxAge = int(time.Until(result.Identity.Session.ExpiresAt).Seconds())
		cookie.Expires = result.Identity.Session.ExpiresAt
	}
	http.SetCookie(writer, cookie)
}

// AccountMode reports whether account-mode authentication is selected.
func (a *AuthManager) AccountMode() bool {
	return a != nil && a.accounts != nil
}

// Close releases account database and session registry resources.
func (a *AuthManager) Close() {
	if a != nil && a.accounts != nil {
		a.accounts.Close()
	}
}

// RecoverLockedAccount performs a local, audited unlock for disaster recovery.
// It is intended for the haze-web CLI running with the service's private keys.
func (a *AuthManager) RecoverLockedAccount(ctx context.Context, username string) error {
	if a == nil || a.accounts == nil {
		return fmt.Errorf("account recovery is unavailable in the current sign-in mode")
	}
	if !a.accounts.configured {
		return fmt.Errorf("account sign-in is unavailable: %w", a.accounts.initializationError)
	}
	account, err := a.accounts.store.ByUsername(ctx, strings.TrimSpace(username))
	if err != nil {
		return err
	}
	event := AuditEvent{
		Event: "LOCAL_ACCOUNT_UNLOCK_REQUESTED", ActorID: account.ID, ActorUsername: account.Username,
		Severity: "critical", Details: map[string]any{"source": "haze-web --unlock-account"},
	}
	if err := a.accounts.audit.Append("webpanel", event); err != nil {
		return fmt.Errorf("write recovery audit request: %w", err)
	}
	if err := a.accounts.store.Unlock(ctx, account.ID); err != nil {
		return err
	}
	a.accounts.ResetLoginLimits(ctx, account.Username, account.LastIP)
	if err := a.accounts.RevokeUser(ctx, account.ID); err != nil {
		return err
	}
	event.Event = "LOCAL_ACCOUNT_UNLOCK_COMPLETED"
	if err := a.accounts.audit.Append("webpanel", event); err != nil {
		return fmt.Errorf("write recovery audit completion: %w", err)
	}
	return nil
}

// PasswordReset reports one account whose password was replaced locally.
type PasswordReset struct {
	Username string
	Password string
}

const generatedPasswordLength = 24

// generatedPasswordAlphabet omits look-alike characters (0/O, 1/I/l) and
// symbols so a printed password survives copying from a terminal.
const generatedPasswordAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"

// ResetAccountPasswords performs a local, audited password reset for every
// account whose username matches pattern (case-insensitive, with * and ?
// wildcards). Each match gets a new random password that must be changed at
// the next sign-in, its lockout state is cleared, and its sessions are revoked. Resets completed before an error are
// still returned so the caller can report them. It is intended for the
// haze-web CLI running with the service's private keys.
func (a *AuthManager) ResetAccountPasswords(ctx context.Context, pattern string) ([]PasswordReset, error) {
	if a == nil || a.accounts == nil {
		return nil, fmt.Errorf("account recovery is unavailable in the current sign-in mode")
	}
	if !a.accounts.configured {
		return nil, fmt.Errorf("account sign-in is unavailable: %w", a.accounts.initializationError)
	}
	pattern = strings.TrimSpace(pattern)
	matcher, err := compileUsernamePattern(pattern)
	if err != nil {
		return nil, err
	}
	accounts, err := a.accounts.store.List(ctx)
	if err != nil {
		return nil, err
	}
	resets := []PasswordReset{}
	matched := 0
	for _, account := range accounts {
		if !matcher.MatchString(account.Username) {
			continue
		}
		matched++
		password, err := generatePassword()
		if err != nil {
			return resets, err
		}
		hash, err := a.accounts.hashPassword(password)
		if err != nil {
			return resets, err
		}
		event := AuditEvent{
			Event: "LOCAL_PASSWORD_RESET_REQUESTED", ActorID: account.ID, ActorUsername: account.Username,
			Severity: "critical", Details: map[string]any{"source": "haze-web --reset-password", "pattern": pattern},
		}
		if err := a.accounts.audit.Append("webpanel", event); err != nil {
			return resets, fmt.Errorf("write password reset audit request: %w", err)
		}
		// The printed password is single use: the account must replace it at the
		// next sign-in. Accounts barred from self-service changes keep it, since
		// forcing a change they cannot make would lock them out.
		if err := a.accounts.store.SetPassword(ctx, account.ID, hash, account.AllowUserPasswordChange); err != nil {
			return resets, fmt.Errorf("reset password for %q: %w", account.Username, err)
		}
		a.accounts.ResetLoginLimits(ctx, account.Username, account.LastIP)
		if err := a.accounts.RevokeUser(ctx, account.ID); err != nil {
			return resets, fmt.Errorf("revoke sessions for %q: %w", account.Username, err)
		}
		resets = append(resets, PasswordReset{Username: account.Username, Password: password})
		event.Event = "LOCAL_PASSWORD_RESET_COMPLETED"
		if err := a.accounts.audit.Append("webpanel", event); err != nil {
			return resets, fmt.Errorf("write password reset audit completion: %w", err)
		}
	}
	if matched == 0 {
		return nil, fmt.Errorf("no accounts match %q", pattern)
	}
	return resets, nil
}

// compileUsernamePattern turns a case-insensitive * and ? wildcard pattern
// into an anchored matcher. Every other character matches literally.
func compileUsernamePattern(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, fmt.Errorf("an account name or wildcard pattern is required")
	}
	var expression strings.Builder
	expression.WriteString("(?i)^")
	for _, char := range pattern {
		switch char {
		case '*':
			expression.WriteString(".*")
		case '?':
			expression.WriteString(".")
		default:
			expression.WriteString(regexp.QuoteMeta(string(char)))
		}
	}
	expression.WriteString("$")
	return regexp.Compile(expression.String())
}

func generatePassword() (string, error) {
	const alphabetSize = len(generatedPasswordAlphabet)
	// Discard bytes past the last whole multiple of the alphabet size so every
	// character is equally likely.
	const limit = 256 - 256%alphabetSize
	password := make([]byte, 0, generatedPasswordLength)
	var raw [generatedPasswordLength]byte
	for len(password) < generatedPasswordLength {
		if _, err := rand.Read(raw[:]); err != nil {
			return "", fmt.Errorf("generate password: %w", err)
		}
		for _, value := range raw {
			if int(value) >= limit {
				continue
			}
			password = append(password, generatedPasswordAlphabet[int(value)%alphabetSize])
			if len(password) == generatedPasswordLength {
				break
			}
		}
	}
	return string(password), nil
}

func (a *AuthManager) cookieSecureForRequest(request *http.Request) bool {
	if a == nil || !a.secureCookie {
		return false
	}
	if a.accounts != nil {
		return true
	}
	return !requestUsesLocalHost(request)
}

type adminSessionClaims struct {
	Subject    string `json:"sub"`
	IssuedAt   int64  `json:"iat"`
	ExpiresAt  int64  `json:"exp"`
	SessionID  string `json:"sid"`
	TokenNonce string `json:"nonce"`
}

func (a *AuthManager) signSessionToken(sessionID string, issuedAt time.Time) (string, error) {
	nonce, err := randomToken()
	if err != nil {
		return "", err
	}
	claims := adminSessionClaims{
		Subject:    "admin",
		IssuedAt:   issuedAt.Unix(),
		ExpiresAt:  issuedAt.Add(a.ttl).Unix(),
		SessionID:  sessionID,
		TokenNonce: nonce,
	}
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(raw)
	signature := a.signTokenPayload(encodedPayload)
	return "haze1." + encodedPayload + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (a *AuthManager) sessionTokenExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "haze1" {
		return time.Time{}, false
	}
	actual, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return time.Time{}, false
	}
	expected := a.signTokenPayload(parts[1])
	if subtle.ConstantTimeCompare(actual, expected) != 1 {
		return time.Time{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims adminSessionClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return time.Time{}, false
	}
	if claims.Subject != "admin" || claims.ExpiresAt <= 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.ExpiresAt, 0), true
}

func (a *AuthManager) signTokenPayload(encodedPayload string) []byte {
	mac := hmac.New(sha256.New, a.sessionSigningKey())
	_, _ = mac.Write([]byte(encodedPayload))
	return mac.Sum(nil)
}

func (a *AuthManager) sessionSigningKey() []byte {
	if secret := strings.TrimSpace(os.Getenv("ADMIN_SESSION_SECRET")); secret != "" {
		sum := sha256.Sum256([]byte("haze-admin-session-secret:" + secret))
		return sum[:]
	}
	if a.passwordHash != "" {
		sum := sha256.Sum256([]byte("haze-admin-session-hash:" + a.passwordHash))
		return sum[:]
	}
	sum := sha256.Sum256(append([]byte("haze-admin-session-password:"), a.password...))
	return sum[:]
}

func tokenDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (a *AuthManager) cleanupRevokedLocked(now time.Time) {
	for digest, expires := range a.revoked {
		if !expires.After(now) {
			delete(a.revoked, digest)
		}
	}
}

func (a *AuthManager) cleanupSessionsLocked(now time.Time) {
	for token, expires := range a.sessions {
		if !expires.After(now) || strings.HasPrefix(token, "haze1.") {
			delete(a.sessions, token)
		}
	}
}

func tokenFromRequest(request *http.Request) string {
	if token := strings.TrimSpace(request.URL.Query().Get("token")); token != "" {
		return token
	}
	if header := strings.TrimSpace(request.Header.Get("Authorization")); header != "" {
		const prefix = "Bearer "
		if strings.HasPrefix(strings.ToLower(header), strings.ToLower(prefix)) {
			return strings.TrimSpace(header[len(prefix):])
		}
	}
	if cookie, err := request.Cookie(sessionCookieName); err == nil {
		return strings.TrimSpace(cookie.Value)
	}
	return ""
}

func randomToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func verifyArgon2IDPHC(encoded string, password string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, fmt.Errorf("unsupported password hash")
	}
	params, err := parseArgon2Params(parts[3])
	if err != nil {
		return false, err
	}
	salt, err := decodePHCBase64(parts[4])
	if err != nil {
		return false, err
	}
	expected, err := decodePHCBase64(parts[5])
	if err != nil {
		return false, err
	}
	actual := argon2.IDKey(
		[]byte(password),
		salt,
		params.time,
		params.memory,
		uint8(params.parallelism),
		uint32(len(expected)),
	)
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

type argon2Params struct {
	memory      uint32
	time        uint32
	parallelism uint32
}

func parseArgon2Params(raw string) (argon2Params, error) {
	params := argon2Params{}
	for _, field := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		parsed, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return params, err
		}
		switch key {
		case "m":
			params.memory = uint32(parsed)
		case "t":
			params.time = uint32(parsed)
		case "p":
			params.parallelism = uint32(parsed)
		}
	}
	if params.memory == 0 || params.time == 0 || params.parallelism == 0 || params.parallelism > 255 {
		return params, fmt.Errorf("invalid argon2id parameters")
	}
	return params, nil
}

func decodePHCBase64(value string) ([]byte, error) {
	return base64.RawStdEncoding.DecodeString(value)
}
