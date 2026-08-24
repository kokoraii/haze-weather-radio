package webgateway

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	paseto "aidanwoods.dev/go-paseto"
	"github.com/pquerna/otp"
	"github.com/pquerna/otp/totp"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
)

const (
	pasetoIssuer                   = "haze-weather-radio"
	pasetoAudience                 = "haze-webpanel"
	pasetoImplicitAssertion        = "haze-webpanel-session:v1"
	argonMemoryKiB          uint32 = 65536
	argonIterations         uint32 = 3
	argonParallelism        uint8  = 4
	argonSaltLength                = 16
	argonOutputLength              = 32
	maxPasswordBytes               = 1024
)

// Identity is the immutable authenticated request principal.
type Identity struct {
	Account                Account       `json:"account"`
	Session                ActiveSession `json:"session"`
	PasswordChangeRequired bool          `json:"password_change_required"`
	RawSessionID           string        `json:"-"`
}

// LoginInput contains all factors and client attributes used to issue a session.
type LoginInput struct {
	Username   string
	Password   string
	TOTP       string
	Persistent bool
	Request    *http.Request
}

// LoginResult describes a successful login or MFA enrollment challenge.
type LoginResult struct {
	Token                  string
	Identity               Identity
	Persistent             bool
	MFAEnrollmentRequired  bool
	MFAEnrollmentSecret    string
	MFAEnrollmentURI       string
	PasswordChangeRequired bool
}

// AuthError is a safe authentication error suitable for an API response.
type AuthError struct {
	Code       string
	Detail     string
	HTTPStatus int
}

func (e *AuthError) Error() string { return e.Detail }

type accountAuth struct {
	store               *accountStore
	sessions            sessionRegistry
	audit               *auditLogger
	pasetoKey           paseto.V4SymmetricKey
	pepper              []byte
	mfaKey              []byte
	enforceMFA          bool
	redisRequired       bool
	sessionTTL          time.Duration
	persistentTTL       time.Duration
	idleTimeout         time.Duration
	loginLimit          int
	loginWindow         time.Duration
	accountLockDuration time.Duration
	originationRate     int
	trustedProxyCIDRs   []*net.IPNet
	loginCIDRAllowlist  []*net.IPNet
	loginLimiter        *attemptLimiter
	loginIPLimiter      *attemptLimiter
	originationLimiter  *attemptLimiter
	expiryAuditLimiter  *attemptLimiter
	preauthAuditLimiter *attemptLimiter
	accountMutationMu   sync.Mutex
	lockRepairMu        sync.Mutex
	lockRepairCancel    context.CancelFunc
	lockRepairDone      chan struct{}
	argonSlots          chan struct{}
	dummyPasswordHash   string
	configured          bool
	initializationError error
}

type attemptWindow struct {
	timestamps []time.Time
}

type attemptLimiter struct {
	mu      sync.Mutex
	windows map[string]attemptWindow
}

const maxAttemptLimiterKeys = 4096

const (
	minimumAccountLockRepairInterval = 5 * time.Second
	maximumAccountLockRepairInterval = 15 * time.Second
	accountLockRepairTimeout         = 5 * time.Second
)

func newAttemptLimiter() *attemptLimiter {
	return &attemptLimiter{windows: map[string]attemptWindow{}}
}

func (l *attemptLimiter) Allow(key string, limit int, window time.Duration, now time.Time) bool {
	if limit <= 0 || window <= 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked(window, now)
	entry := l.windows[key]
	if len(entry.timestamps) >= limit {
		return false
	}
	l.admitKeyLocked(key)
	entry.timestamps = append(entry.timestamps, now)
	l.windows[key] = entry
	return true
}

func (l *attemptLimiter) pruneLocked(window time.Duration, now time.Time) {
	cutoff := now.Add(-window)
	for key, entry := range l.windows {
		kept := entry.timestamps[:0]
		for _, timestamp := range entry.timestamps {
			if timestamp.After(cutoff) {
				kept = append(kept, timestamp)
			}
		}
		if len(kept) == 0 {
			delete(l.windows, key)
			continue
		}
		entry.timestamps = kept
		l.windows[key] = entry
	}
}

func (l *attemptLimiter) admitKeyLocked(key string) {
	if _, ok := l.windows[key]; ok || len(l.windows) < maxAttemptLimiterKeys {
		return
	}
	oldestKey := ""
	var oldest time.Time
	for candidate, entry := range l.windows {
		if len(entry.timestamps) == 0 {
			oldestKey = candidate
			break
		}
		latest := entry.timestamps[len(entry.timestamps)-1]
		if oldestKey == "" || latest.Before(oldest) {
			oldestKey = candidate
			oldest = latest
		}
	}
	delete(l.windows, oldestKey)
}

func (l *attemptLimiter) Reset(key string) {
	l.mu.Lock()
	delete(l.windows, key)
	l.mu.Unlock()
}

func (l *attemptLimiter) ResetPrefix(prefix string) {
	l.mu.Lock()
	for key := range l.windows {
		if strings.HasPrefix(key, prefix) {
			delete(l.windows, key)
		}
	}
	l.mu.Unlock()
}

func newAccountAuth(config Config, configPath string) *accountAuth {
	authConfig := config.Webpanel.Authentication
	h := &accountAuth{
		enforceMFA:          authConfig.EnforceMFA,
		redisRequired:       authConfig.RedisRequired,
		sessionTTL:          durationSeconds(authConfig.SessionTTLSeconds, 12*time.Hour),
		persistentTTL:       durationSeconds(authConfig.PersistentSessionTTLSeconds, 30*24*time.Hour),
		idleTimeout:         durationSeconds(authConfig.IdleTimeoutSeconds, 15*time.Minute),
		loginLimit:          positiveOr(authConfig.LoginRateLimit, 5),
		loginWindow:         durationSeconds(authConfig.LoginRateWindowSeconds, 2*time.Minute),
		accountLockDuration: durationSeconds(authConfig.AccountLockDurationSeconds, 2*time.Minute),
		originationRate:     positiveOr(authConfig.OriginationRatePerSecond, 2),
		loginLimiter:        newAttemptLimiter(),
		loginIPLimiter:      newAttemptLimiter(),
		originationLimiter:  newAttemptLimiter(),
		expiryAuditLimiter:  newAttemptLimiter(),
		preauthAuditLimiter: newAttemptLimiter(),
		argonSlots:          make(chan struct{}, 2),
	}
	var err error
	h.trustedProxyCIDRs, err = parseCIDRList(authConfig.TrustedProxyCIDRs)
	if err != nil {
		h.initializationError = fmt.Errorf("trusted proxy configuration: %w", err)
		return h
	}
	h.loginCIDRAllowlist, err = parseCIDRList(authConfig.LoginCIDRAllowlist)
	if err != nil {
		h.initializationError = fmt.Errorf("login CIDR configuration: %w", err)
		return h
	}
	pasetoKeyBytes, err := requiredEnvironmentKeyExact(firstNonBlank(authConfig.PasetoKeyEnv, "HAZE_PASETO_V4_LOCAL_KEY"), 32)
	if err != nil {
		h.initializationError = err
		return h
	}
	h.pasetoKey, err = paseto.V4SymmetricKeyFromBytes(pasetoKeyBytes)
	if err != nil {
		h.initializationError = fmt.Errorf("load PASETO v4.local key: %w", err)
		return h
	}
	h.pepper, err = requiredEnvironmentKey(firstNonBlank(authConfig.PasswordPepperEnv, "HAZE_PASSWORD_PEPPER"), 32)
	if err != nil {
		h.initializationError = err
		return h
	}
	h.mfaKey, err = requiredEnvironmentKeyExact(firstNonBlank(authConfig.MFAKeyEnv, "HAZE_MFA_ENCRYPTION_KEY"), chacha20poly1305.KeySize)
	if err != nil {
		h.initializationError = err
		return h
	}
	auditKey, err := requiredEnvironmentKey(firstNonBlank(authConfig.AuditKeyEnv, "HAZE_AUDIT_HMAC_KEY"), 32)
	if err != nil {
		h.initializationError = err
		return h
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	h.store, err = openAccountStore(ctx, config, configPath)
	if err != nil {
		h.initializationError = err
		return h
	}
	auditDir := strings.TrimSpace(authConfig.AuditDir)
	if auditDir == "" {
		auditDir = "logs"
	}
	if !filepath.IsAbs(auditDir) {
		auditDir = filepath.Join(filepath.Dir(filepath.Clean(configPath)), auditDir)
	}
	h.audit, err = newAuditLogger(auditDir, auditKey)
	if err != nil {
		h.initializationError = err
		h.store.Close()
		return h
	}
	redisEnv := firstNonBlank(authConfig.RedisURLEnv, "HAZE_REDIS_URL")
	redisURL := strings.TrimSpace(os.Getenv(redisEnv))
	if redisURL != "" {
		h.sessions, err = newRedisSessionRegistry(ctx, redisURL, authConfig.RedisKeyPrefix)
		if err != nil {
			h.initializationError = err
			h.store.Close()
			return h
		}
	} else if authConfig.RedisRequired {
		h.initializationError = fmt.Errorf("%s is required for account session revocation", redisEnv)
		h.store.Close()
		return h
	} else {
		h.sessions = newMemorySessionRegistry()
	}
	if err := h.bootstrapFirstAdmin(ctx, authConfig.BootstrapUsernameEnv, authConfig.BootstrapPasswordEnv); err != nil {
		h.initializationError = err
		_ = h.sessions.Close()
		h.store.Close()
		return h
	}
	if err := h.repairAccountLocks(ctx, time.Now().UTC()); err != nil {
		h.initializationError = err
		_ = h.sessions.Close()
		h.store.Close()
		return h
	}
	dummy, err := h.hashPassword("haze-unknown-account-dummy-password")
	if err != nil {
		h.initializationError = err
		_ = h.sessions.Close()
		h.store.Close()
		return h
	}
	h.dummyPasswordHash = dummy
	h.configured = true
	h.startAccountLockRepair()
	return h
}

func (h *accountAuth) Close() {
	if h == nil {
		return
	}
	h.stopAccountLockRepair()
	if h.sessions != nil {
		_ = h.sessions.Close()
	}
	if h.store != nil {
		h.store.Close()
	}
}

// startAccountLockRepair makes temporary account locks truly time-bounded.
// Login and startup still repair expired locks immediately, while this bounded
// background pass covers idle accounts that do not make another request.
func (h *accountAuth) startAccountLockRepair() {
	if h == nil || h.store == nil || h.initializationError != nil {
		return
	}
	h.lockRepairMu.Lock()
	if h.lockRepairCancel != nil {
		h.lockRepairMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	h.lockRepairCancel = cancel
	h.lockRepairDone = done
	interval := accountLockRepairInterval(h.accountLockDuration)
	h.lockRepairMu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				repairCtx, repairCancel := context.WithTimeout(ctx, accountLockRepairTimeout)
				err := h.repairAccountLocks(repairCtx, time.Now().UTC())
				repairCancel()
				if err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("web account lock repair failed")
				}
			}
		}
	}()
}

func (h *accountAuth) stopAccountLockRepair() {
	if h == nil {
		return
	}
	h.lockRepairMu.Lock()
	cancel := h.lockRepairCancel
	done := h.lockRepairDone
	h.lockRepairCancel = nil
	h.lockRepairDone = nil
	h.lockRepairMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func accountLockRepairInterval(lockDuration time.Duration) time.Duration {
	interval := lockDuration / 4
	if interval < minimumAccountLockRepairInterval {
		return minimumAccountLockRepairInterval
	}
	if interval > maximumAccountLockRepairInterval {
		return maximumAccountLockRepairInterval
	}
	return interval
}

func (h *accountAuth) bootstrapFirstAdmin(ctx context.Context, usernameEnv string, passwordEnv string) error {
	count, err := h.store.Count(ctx)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	usernameEnv = firstNonBlank(usernameEnv, "HAZE_BOOTSTRAP_ADMIN_USERNAME")
	passwordEnv = firstNonBlank(passwordEnv, "HAZE_BOOTSTRAP_ADMIN_PASSWORD")
	username := firstNonBlank(os.Getenv(usernameEnv), "admin")
	password := os.Getenv(passwordEnv)
	if password == "" {
		// ADMIN_PASSWD is accepted only once to migrate an existing deployment.
		password = os.Getenv("ADMIN_PASSWD")
	}
	if password == "" {
		return fmt.Errorf("account database is empty and %s is not configured", passwordEnv)
	}
	passwordHash, err := h.hashPassword(password)
	if err != nil {
		return fmt.Errorf("hash bootstrap administrator password: %w", err)
	}
	account := Account{
		Username:                username,
		AllowOrigination:        true,
		AllowedOriginators:      []string{"CIV", "EAS", "PEP", "WXR"},
		CanViewLogs:             true,
		AllowPersistentSessions: false,
		PasswordExpiryDays:      90,
		AllowUserPasswordChange: true,
		LoggingEnabled:          true,
		IsAdmin:                 true,
	}
	if err := h.store.Create(ctx, account, passwordHash); err != nil {
		return fmt.Errorf("create bootstrap administrator: %w", err)
	}
	return nil
}

func temporaryAccountLockError() *AuthError {
	return &AuthError{
		Code:       "account_locked",
		Detail:     "This account is temporarily locked. Try again after the lock period.",
		HTTPStatus: http.StatusLocked,
	}
}

func (h *accountAuth) repairAccountLocks(ctx context.Context, now time.Time) error {
	accounts, err := h.store.List(ctx)
	if err != nil {
		return fmt.Errorf("load account locks: %w", err)
	}
	for _, account := range accounts {
		if !account.AccountLocked {
			continue
		}
		if _, err := h.applyAccountLockPolicy(ctx, account, now); err != nil {
			return err
		}
	}
	return nil
}

func (h *accountAuth) applyAccountLockPolicy(ctx context.Context, account Account, now time.Time) (Account, error) {
	if !account.AccountLocked {
		return account, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	reason := ""
	if account.IsAdmin {
		reason = "administrator_exempt"
	} else {
		lockedAt := account.failedLoginWindowStartedAt
		if lockedAt.IsZero() {
			lockedAt = account.UpdatedAt
		}
		if lockedAt.IsZero() || now.Before(lockedAt) || now.Sub(lockedAt) >= h.accountLockDuration {
			reason = "duration_elapsed"
		}
	}
	if reason == "" {
		return account, nil
	}
	unlocked, err := h.store.UnlockLockedIfAuthVersion(ctx, account.ID, account.authVersion, now)
	if err != nil {
		return Account{}, fmt.Errorf("apply account lock policy: %w", err)
	}
	if !unlocked {
		return h.store.ByID(ctx, account.ID)
	}
	h.ResetLoginLimits(ctx, account.Username, "")
	h.purgeUserSessionLeases(ctx, account.ID, account)
	if err := h.audit.Append("access", AuditEvent{
		Timestamp: now, Event: "ACCOUNT_LOCK_AUTO_CLEARED", ActorID: account.ID, ActorUsername: account.Username,
		Severity: "warning", Details: map[string]any{"reason": reason},
	}); err != nil {
		return Account{}, fmt.Errorf("audit automatic account unlock: %w", err)
	}
	return h.store.ByID(ctx, account.ID)
}

func (h *accountAuth) Login(ctx context.Context, input LoginInput) (LoginResult, error) {
	if h == nil || !h.configured {
		return LoginResult{}, h.unavailableError()
	}
	username := strings.TrimSpace(input.Username)
	ip := h.clientIP(input.Request)
	userAgent := cleanUserAgent(input.Request)
	if !h.secureRequest(input.Request) {
		h.auditPreauthenticationFailure("LOGIN_INSECURE_TRANSPORT_DENIED", username, ip, userAgent, "critical")
		return LoginResult{}, &AuthError{Code: "https_required", Detail: "Sign in requires HTTPS.", HTTPStatus: http.StatusUpgradeRequired}
	}
	if !validUsername.MatchString(username) || len(input.Password) == 0 || len(input.Password) > maxPasswordBytes {
		return LoginResult{}, invalidCredentials()
	}
	if !ipAllowed(ip, h.loginCIDRAllowlist) {
		h.auditPreauthenticationFailure("LOGIN_CIDR_DENIED", username, ip, userAgent, "critical")
		return LoginResult{}, &AuthError{Code: "login_not_allowed", Detail: "Sign in is not allowed from this network.", HTTPStatus: http.StatusForbidden}
	}
	limiterKey := strings.ToLower(username) + "|" + ip
	now := time.Now().UTC()
	ipLimit := max(h.loginLimit*5, 20)
	pairAllowed, ipRateAllowed, limitErr := h.admitLoginAttempt(ctx, strings.ToLower(username), limiterKey, ip, ipLimit, now)
	if limitErr != nil {
		return LoginResult{}, &AuthError{Code: "login_rate_unavailable", Detail: "The sign-in rate limiter is unavailable.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if !pairAllowed || !ipRateAllowed {
		h.auditPreauthenticationFailure("LOGIN_RATE_LIMITED", username, ip, userAgent, "critical")
		if !pairAllowed {
			if account, lookupErr := h.store.ByUsername(ctx, username); lookupErr == nil {
				account, policyErr := h.applyAccountLockPolicy(ctx, account, now)
				if policyErr != nil {
					return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "Sign in is temporarily unavailable.", HTTPStatus: http.StatusServiceUnavailable}
				}
				if account.AccountLocked {
					return LoginResult{}, temporaryAccountLockError()
				}
			}
		}
		return LoginResult{}, &AuthError{Code: "login_rate_limited", Detail: "Too many sign-in attempts. Try again later.", HTTPStatus: http.StatusTooManyRequests}
	}
	account, err := h.store.ByUsername(ctx, username)
	if errors.Is(err, errAccountNotFound) {
		_, _ = h.verifyPassword(ctx, h.dummyPasswordHash, input.Password)
		return LoginResult{}, invalidCredentials()
	}
	if err != nil {
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "Sign in is temporarily unavailable.", HTTPStatus: http.StatusServiceUnavailable}
	}
	account, err = h.applyAccountLockPolicy(ctx, account, now)
	if err != nil {
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "Sign in is temporarily unavailable.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if account.AccountLocked {
		return LoginResult{}, temporaryAccountLockError()
	}
	accountCIDRs, err := parseCIDRList(account.CIDRWhitelist)
	if err != nil {
		_ = h.audit.Append("access", AuditEvent{
			Event: "LOGIN_ACCOUNT_CIDR_POLICY_INVALID", ActorID: account.ID, ActorUsername: account.Username,
			IP: ip, UserAgent: userAgent, Severity: "critical",
		})
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "The account network policy is invalid.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if !ipAllowed(ip, accountCIDRs) {
		_ = h.audit.Append("access", AuditEvent{
			Event: "LOGIN_ACCOUNT_CIDR_DENIED", ActorID: account.ID, ActorUsername: account.Username,
			IP: ip, UserAgent: userAgent, Severity: "critical",
		})
		return LoginResult{}, &AuthError{Code: "login_not_allowed", Detail: "Sign in is not allowed from this network.", HTTPStatus: http.StatusForbidden}
	}
	passwordOK, verifyErr := h.verifyPassword(ctx, account.passwordHash, input.Password)
	if verifyErr != nil {
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "Sign in is temporarily unavailable.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if !passwordOK {
		updated, recordErr := h.store.RecordLoginFailureIfAuthVersion(ctx, account.ID, account.authVersion, h.loginLimit, h.loginWindow, now)
		if errors.Is(recordErr, errAccountVersionChanged) {
			return LoginResult{}, invalidCredentials()
		}
		if recordErr != nil {
			_ = h.audit.Append("access", AuditEvent{
				Event: "LOGIN_FAILURE_STATE_ERROR", ActorID: account.ID, ActorUsername: account.Username,
				IP: ip, UserAgent: userAgent, Severity: "critical",
			})
			return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "Sign in failure state could not be recorded.", HTTPStatus: http.StatusServiceUnavailable}
		}
		_ = h.audit.Append("access", AuditEvent{
			Event: "LOGIN_FAILURE", ActorID: account.ID, ActorUsername: account.Username, IP: ip, UserAgent: userAgent,
			Details: map[string]any{"failed_attempts": updated.FailedLoginAttempts},
		})
		if updated.AccountLocked {
			_ = h.sessions.DeleteUser(ctx, account.ID)
			return LoginResult{}, temporaryAccountLockError()
		}
		return LoginResult{}, invalidCredentials()
	}
	account, err = h.refreshLoginAccount(ctx, account)
	if err != nil {
		return LoginResult{}, err
	}
	// A stored but unconfirmed enrollment secret is not an authentication
	// factor. Only the global policy or an already-enabled account may require
	// MFA. This lets an operator disable mandatory enrollment without a stale
	// pending secret continuing to block password-only sign-in.
	if h.enforceMFA || account.MFAEnabled {
		if !account.MFAConfigured {
			return h.startMFAEnrollment(ctx, account, ip, userAgent)
		}
		if strings.TrimSpace(input.TOTP) == "" {
			if !account.MFAEnabled {
				return h.pendingMFAEnrollment(account)
			}
			return LoginResult{}, &AuthError{Code: "mfa_required", Detail: "Enter the current authentication code.", HTTPStatus: http.StatusUnauthorized}
		}
		if err := h.validateMFA(ctx, account, input.TOTP, now); err != nil {
			updated, recordErr := h.store.RecordLoginFailureIfAuthVersion(ctx, account.ID, account.authVersion, h.loginLimit, h.loginWindow, now)
			if errors.Is(recordErr, errAccountVersionChanged) {
				return LoginResult{}, invalidCredentials()
			}
			if recordErr != nil {
				_ = h.audit.Append("access", AuditEvent{
					Event: "MFA_FAILURE_STATE_ERROR", ActorID: account.ID, ActorUsername: account.Username,
					IP: ip, UserAgent: userAgent, Severity: "critical",
				})
				return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "MFA failure state could not be recorded.", HTTPStatus: http.StatusServiceUnavailable}
			}
			_ = h.audit.Append("access", AuditEvent{
				Event: "MFA_FAILURE", ActorID: account.ID, ActorUsername: account.Username, IP: ip, UserAgent: userAgent,
				Details: map[string]any{"failed_attempts": updated.FailedLoginAttempts},
			})
			if updated.AccountLocked {
				_ = h.sessions.DeleteUser(ctx, account.ID)
				return LoginResult{}, temporaryAccountLockError()
			}
			return LoginResult{}, &AuthError{Code: "invalid_mfa", Detail: "The authentication code is invalid or was already used.", HTTPStatus: http.StatusUnauthorized}
		}
		if !account.MFAEnabled {
			if err := h.store.EnableMFAIfAuthVersion(ctx, account.ID, account.authVersion); err != nil {
				if errors.Is(err, errAccountVersionChanged) {
					return LoginResult{}, &AuthError{Code: "login_state_changed", Detail: "The account changed while sign in was in progress. Try again.", HTTPStatus: http.StatusConflict}
				}
				return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "MFA enrollment could not be completed.", HTTPStatus: http.StatusServiceUnavailable}
			}
			account.MFAEnabled = true
		}
	}
	account, err = h.refreshLoginAccount(ctx, account)
	if err != nil {
		return LoginResult{}, err
	}
	persistent := input.Persistent && account.AllowPersistentSessions
	expiresAt := now.Add(h.sessionTTL)
	if persistent {
		expiresAt = now.Add(h.persistentTTL)
	}
	passwordChangeRequired := passwordExpired(account, now)
	if passwordChangeRequired && !account.AllowUserPasswordChange {
		_ = h.audit.Append("access", AuditEvent{
			Event: "PASSWORD_RESET_REQUIRED", ActorID: account.ID, ActorUsername: account.Username,
			IP: ip, UserAgent: userAgent, Severity: "warning",
		})
		return LoginResult{}, &AuthError{
			Code: "password_reset_required", Detail: "This password has expired and must be reset by an administrator.", HTTPStatus: http.StatusForbidden,
		}
	}
	rawSessionID, err := randomToken()
	if err != nil {
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "A secure session could not be created.", HTTPStatus: http.StatusServiceUnavailable}
	}
	token, err := h.issueToken(account, rawSessionID, ip, now, expiresAt, passwordChangeRequired)
	if err != nil {
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "A secure session could not be created.", HTTPStatus: http.StatusServiceUnavailable}
	}
	session := ActiveSession{
		UserID: account.ID, Username: account.Username, IP: ip, UserAgent: userAgent, Persistent: persistent,
		AuthVersion: account.authVersion,
		CreatedAt:   now, LastSeenAt: now, ExpiresAt: expiresAt,
	}
	if passwordChangeRequired {
		session.UserAgent = userAgent + " [password-change-required]"
	}
	if err := h.sessions.Put(ctx, rawSessionID, session); err != nil {
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "The session registry is unavailable.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if err := h.store.RecordLoginSuccess(ctx, account.ID, ip, account.authVersion); err != nil {
		_ = h.sessions.Delete(ctx, rawSessionID)
		if errors.Is(err, errAccountVersionChanged) {
			return LoginResult{}, &AuthError{Code: "login_state_changed", Detail: "The account changed while sign in was in progress. Try again.", HTTPStatus: http.StatusConflict}
		}
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "The account state could not be updated.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if account.LoggingEnabled {
		if err := h.audit.Append("access", AuditEvent{
			Event: "LOGIN_SUCCESS", ActorID: account.ID, ActorUsername: account.Username,
			SessionID: sessionIDDigest(rawSessionID), IP: ip, UserAgent: userAgent,
			Details: map[string]any{"persistent_session": persistent, "password_change_required": passwordChangeRequired},
		}); err != nil {
			_ = h.sessions.Delete(ctx, rawSessionID)
			return LoginResult{}, &AuthError{Code: "audit_unavailable", Detail: "Audit integrity could not be established.", HTTPStatus: http.StatusServiceUnavailable}
		}
	}
	h.resetLoginPair(ctx, limiterKey)
	account.LastIP = ip
	account.LastLoginAt = now
	identity := Identity{
		Account: account, Session: session, RawSessionID: rawSessionID,
		PasswordChangeRequired: passwordChangeRequired,
	}
	return LoginResult{
		Token: token, Identity: identity, Persistent: persistent,
		PasswordChangeRequired: passwordChangeRequired,
	}, nil
}

func (h *accountAuth) auditPreauthenticationFailure(event string, attemptedUsername string, ip string, userAgent string, severity string) {
	if h == nil || h.audit == nil || h.preauthAuditLimiter == nil {
		return
	}
	now := time.Now().UTC()
	key := strings.ToUpper(strings.TrimSpace(event)) + "|" + canonicalIP(ip)
	if !h.preauthAuditLimiter.Allow(key, 10, h.loginWindow, now) {
		return
	}
	usernameDetail := strings.TrimSpace(attemptedUsername)
	if !validUsername.MatchString(usernameDetail) {
		digest := sha256.Sum256([]byte(usernameDetail))
		usernameDetail = "invalid:" + hex.EncodeToString(digest[:8])
	}
	_ = h.audit.Append("access", AuditEvent{
		Timestamp: now, Event: event, ActorUsername: "unknown", IP: ip, UserAgent: userAgent, Severity: severity,
		Details: map[string]any{"attempted_username": usernameDetail},
	})
}

func (h *accountAuth) refreshLoginAccount(ctx context.Context, previous Account) (Account, error) {
	account, err := h.store.ByID(ctx, previous.ID)
	if errors.Is(err, errAccountNotFound) {
		return Account{}, invalidCredentials()
	}
	if err != nil {
		return Account{}, &AuthError{Code: "auth_unavailable", Detail: "Sign in is temporarily unavailable.", HTTPStatus: http.StatusServiceUnavailable}
	}
	account, err = h.applyAccountLockPolicy(ctx, account, time.Now().UTC())
	if err != nil {
		return Account{}, &AuthError{Code: "auth_unavailable", Detail: "Sign in is temporarily unavailable.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if account.AccountLocked {
		return Account{}, temporaryAccountLockError()
	}
	if account.authVersion != previous.authVersion {
		return Account{}, &AuthError{Code: "login_state_changed", Detail: "The account changed while sign in was in progress. Try again.", HTTPStatus: http.StatusConflict}
	}
	return account, nil
}

func (h *accountAuth) startMFAEnrollment(ctx context.Context, account Account, ip string, userAgent string) (LoginResult, error) {
	key, err := totp.Generate(totp.GenerateOpts{
		Issuer: pasetoIssuer, AccountName: account.Username, Period: 30, SecretSize: 32,
		Digits: otp.DigitsSix, Algorithm: otp.AlgorithmSHA1, Rand: rand.Reader,
	})
	if err != nil {
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "MFA enrollment could not be created.", HTTPStatus: http.StatusServiceUnavailable}
	}
	encrypted, err := h.encryptMFASecret(account.ID, key.Secret())
	if err != nil {
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "MFA enrollment could not be protected.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if err := h.store.SetMFAIfAuthVersion(ctx, account.ID, encrypted, account.authVersion); err != nil {
		if errors.Is(err, errAccountVersionChanged) {
			return LoginResult{}, &AuthError{Code: "login_state_changed", Detail: "The account changed while sign in was in progress. Try again.", HTTPStatus: http.StatusConflict}
		}
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "MFA enrollment could not be saved.", HTTPStatus: http.StatusServiceUnavailable}
	}
	_ = h.audit.Append("access", AuditEvent{
		Event: "MFA_ENROLLMENT_STARTED", ActorID: account.ID, ActorUsername: account.Username, IP: ip, UserAgent: userAgent,
	})
	return LoginResult{
		MFAEnrollmentRequired: true, MFAEnrollmentSecret: key.Secret(), MFAEnrollmentURI: key.URL(),
	}, mfaEnrollmentRequiredError()
}

func (h *accountAuth) pendingMFAEnrollment(account Account) (LoginResult, error) {
	secret, err := h.decryptMFASecret(account.ID, account.mfaSecret)
	if err != nil {
		return LoginResult{}, &AuthError{Code: "auth_unavailable", Detail: "MFA enrollment could not be recovered.", HTTPStatus: http.StatusServiceUnavailable}
	}
	label := url.QueryEscape(pasetoIssuer + ":" + account.Username)
	uri := "otpauth://totp/" + label + "?secret=" + url.QueryEscape(secret) + "&issuer=" + url.QueryEscape(pasetoIssuer) + "&period=30&digits=6&algorithm=SHA1"
	return LoginResult{
		MFAEnrollmentRequired: true, MFAEnrollmentSecret: secret, MFAEnrollmentURI: uri,
	}, mfaEnrollmentRequiredError()
}

func mfaEnrollmentRequiredError() error {
	return &AuthError{Code: "mfa_enrollment_required", Detail: "Add the MFA secret to an authenticator, then enter its current code.", HTTPStatus: http.StatusUnauthorized}
}

func (h *accountAuth) Authenticate(request *http.Request) (Identity, error) {
	if h == nil || !h.configured {
		return Identity{}, h.unavailableError()
	}
	if !h.secureRequest(request) {
		return Identity{}, &AuthError{Code: "https_required", Detail: "This page requires HTTPS.", HTTPStatus: http.StatusUpgradeRequired}
	}
	rawToken := accountTokenFromRequest(request)
	if rawToken == "" {
		return Identity{}, &AuthError{Code: "unauthorized", Detail: "Authentication is required.", HTTPStatus: http.StatusUnauthorized}
	}
	parser := paseto.NewParserForValidNow()
	parser.AddRule(paseto.IssuedBy(pasetoIssuer), paseto.ForAudience(pasetoAudience))
	token, err := parser.ParseV4Local(h.pasetoKey, rawToken, []byte(pasetoImplicitAssertion))
	if err != nil {
		h.auditExpiredSession(request, rawToken)
		return Identity{}, &AuthError{Code: "invalid_session", Detail: "The session is invalid or expired.", HTTPStatus: http.StatusUnauthorized}
	}
	rawSessionID, err := token.GetJti()
	if err != nil || rawSessionID == "" {
		return Identity{}, &AuthError{Code: "invalid_session", Detail: "The session is invalid.", HTTPStatus: http.StatusUnauthorized}
	}
	userID, err := token.GetSubject()
	if err != nil || userID == "" {
		return Identity{}, &AuthError{Code: "invalid_session", Detail: "The session is invalid.", HTTPStatus: http.StatusUnauthorized}
	}
	var tokenIP string
	if err := token.Get("ip", &tokenIP); err != nil {
		return Identity{}, &AuthError{Code: "invalid_session", Detail: "The session is invalid.", HTTPStatus: http.StatusUnauthorized}
	}
	var passwordChangeRequired bool
	_ = token.Get("password_change_required", &passwordChangeRequired)
	var tokenAuthVersion int64
	if err := token.Get("auth_version", &tokenAuthVersion); err != nil || tokenAuthVersion < 1 {
		return Identity{}, &AuthError{Code: "invalid_session", Detail: "The session is invalid.", HTTPStatus: http.StatusUnauthorized}
	}
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	session, err := h.sessions.Get(ctx, rawSessionID)
	if errors.Is(err, errSessionNotFound) {
		return Identity{}, &AuthError{Code: "invalid_session", Detail: "The session was revoked or is unavailable.", HTTPStatus: http.StatusUnauthorized}
	}
	if err != nil {
		return Identity{}, &AuthError{Code: "session_unavailable", Detail: "The session registry is unavailable.", HTTPStatus: http.StatusServiceUnavailable}
	}
	now := time.Now().UTC()
	requestIP := h.clientIP(request)
	if session.UserID != userID || session.AuthVersion != tokenAuthVersion || !sameCanonicalIP(tokenIP, requestIP) || !sameCanonicalIP(session.IP, requestIP) {
		_ = h.sessions.Delete(ctx, rawSessionID)
		_ = h.audit.Append("access", AuditEvent{
			Event: "SESSION_IP_MISMATCH", ActorID: session.UserID, ActorUsername: session.Username,
			SessionID: session.ID, IP: requestIP, UserAgent: cleanUserAgent(request), Severity: "critical",
			Details: map[string]any{"issued_ip": session.IP},
		})
		return Identity{}, &AuthError{Code: "session_ip_mismatch", Detail: "The session was revoked because the client address changed.", HTTPStatus: http.StatusUnauthorized}
	}
	if h.idleTimeout > 0 && !session.Persistent && now.Sub(session.LastSeenAt) > h.idleTimeout {
		_ = h.sessions.Delete(ctx, rawSessionID)
		_ = h.audit.Append("access", AuditEvent{
			Event: "SESSION_IDLE_EXPIRED", ActorID: session.UserID, ActorUsername: session.Username,
			SessionID: session.ID, IP: requestIP, UserAgent: cleanUserAgent(request),
		})
		return Identity{}, &AuthError{Code: "session_expired", Detail: "The session expired due to inactivity.", HTTPStatus: http.StatusUnauthorized}
	}
	account, err := h.store.ByID(ctx, userID)
	if errors.Is(err, errAccountNotFound) {
		_ = h.sessions.Delete(ctx, rawSessionID)
		return Identity{}, &AuthError{Code: "session_revoked", Detail: "The account session is no longer active.", HTTPStatus: http.StatusUnauthorized}
	}
	if err != nil {
		return Identity{}, &AuthError{Code: "session_unavailable", Detail: "The account store is unavailable.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if account.AccountLocked {
		_ = h.sessions.Delete(ctx, rawSessionID)
		return Identity{}, &AuthError{Code: "session_revoked", Detail: "The account session is no longer active.", HTTPStatus: http.StatusUnauthorized}
	}
	if account.authVersion != tokenAuthVersion {
		_ = h.sessions.Delete(ctx, rawSessionID)
		return Identity{}, &AuthError{Code: "session_revoked", Detail: "The account credentials or session policy changed.", HTTPStatus: http.StatusUnauthorized}
	}
	passwordChangeRequired = passwordExpired(account, now)
	if now.Sub(session.LastSeenAt) >= 30*time.Second {
		if err := h.sessions.Touch(ctx, rawSessionID, now); err != nil {
			return Identity{}, &AuthError{Code: "session_unavailable", Detail: "The session registry is unavailable.", HTTPStatus: http.StatusServiceUnavailable}
		}
		session.LastSeenAt = now
	}
	return Identity{
		Account: account, Session: session, RawSessionID: rawSessionID,
		PasswordChangeRequired: passwordChangeRequired,
	}, nil
}

func (h *accountAuth) auditExpiredSession(request *http.Request, rawToken string) {
	parser := paseto.NewParserWithoutExpiryCheck()
	parser.AddRule(paseto.IssuedBy(pasetoIssuer), paseto.ForAudience(pasetoAudience))
	token, err := parser.ParseV4Local(h.pasetoKey, rawToken, []byte(pasetoImplicitAssertion))
	if err != nil {
		return
	}
	expiresAt, err := token.GetExpiration()
	if err != nil || expiresAt.After(time.Now().UTC()) {
		return
	}
	rawSessionID, err := token.GetJti()
	if err != nil || rawSessionID == "" {
		return
	}
	digest := sessionIDDigest(rawSessionID)
	if !h.expiryAuditLimiter.Allow(digest, 1, 24*time.Hour, time.Now().UTC()) {
		return
	}
	userID, err := token.GetSubject()
	if err != nil || userID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
	defer cancel()
	account, err := h.store.ByID(ctx, userID)
	if err != nil || !account.LoggingEnabled {
		return
	}
	_ = h.audit.Append("access", AuditEvent{
		Event: "SESSION_EXPIRED", ActorID: account.ID, ActorUsername: account.Username,
		SessionID: digest, IP: h.clientIP(request), UserAgent: cleanUserAgent(request),
		Details: map[string]any{"expired_at": expiresAt.UTC().Format(time.RFC3339Nano)},
	})
}

func (h *accountAuth) Logout(request *http.Request) error {
	if h == nil || h.sessions == nil || request == nil {
		return nil
	}
	identity, err := h.Authenticate(request)
	if err != nil {
		var authErr *AuthError
		if errors.As(err, &authErr) && authErr.HTTPStatus == http.StatusUnauthorized {
			return nil
		}
		return err
	}
	ctx, cancel := context.WithTimeout(request.Context(), 3*time.Second)
	defer cancel()
	if err := h.sessions.Delete(ctx, identity.RawSessionID); err != nil {
		return &AuthError{Code: "logout_unavailable", Detail: "The session could not be revoked.", HTTPStatus: http.StatusServiceUnavailable}
	}
	if identity.Account.LoggingEnabled {
		if err := h.audit.Append("access", AuditEvent{
			Event: "LOGOUT", ActorID: identity.Account.ID, ActorUsername: identity.Account.Username,
			SessionID: identity.Session.ID, IP: identity.Session.IP, UserAgent: identity.Session.UserAgent,
		}); err != nil {
			return &AuthError{Code: "audit_unavailable", Detail: "The session was revoked, but its audit record could not be written.", HTTPStatus: http.StatusServiceUnavailable}
		}
	}
	return nil
}

func (h *accountAuth) ListAccounts(ctx context.Context) ([]Account, error) {
	accounts, err := h.store.List(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	for index, account := range accounts {
		if !account.AccountLocked {
			continue
		}
		accounts[index], err = h.applyAccountLockPolicy(ctx, account, now)
		if err != nil {
			return nil, err
		}
	}
	return accounts, nil
}

func (h *accountAuth) ListSessions(ctx context.Context, userID string) ([]ActiveSession, error) {
	return h.sessions.ListUser(ctx, userID)
}

func (h *accountAuth) RevokeSession(ctx context.Context, digest string) error {
	return h.sessions.DeleteDigest(ctx, digest)
}

func (h *accountAuth) RevokeUser(ctx context.Context, userID string) error {
	account, _ := h.store.ByID(ctx, userID)
	if err := h.store.BumpAuthVersion(ctx, userID); err != nil {
		return err
	}
	h.purgeUserSessionLeases(ctx, userID, account)
	return nil
}

func (h *accountAuth) PurgeUserSessionLeases(ctx context.Context, userID string) {
	account, _ := h.store.ByID(ctx, userID)
	h.purgeUserSessionLeases(ctx, userID, account)
}

func (h *accountAuth) purgeUserSessionLeases(ctx context.Context, userID string, account Account) {
	if err := h.sessions.DeleteUser(ctx, userID); err != nil {
		username := account.Username
		if username == "" {
			username = "unknown"
		}
		_ = h.audit.Append("access", AuditEvent{
			Event: "SESSION_REGISTRY_CLEANUP_FAILED", ActorID: userID, ActorUsername: username,
			Severity: "critical", Details: map[string]any{"sessions_invalidated_by_auth_version": true},
		})
		// The authentication-version bump is authoritative. Existing tokens are
		// already unusable even if stale Redis lease cleanup must happen later.
	}
}

func (h *accountAuth) ResetLoginLimits(ctx context.Context, username string, ip string) {
	username = strings.ToLower(strings.TrimSpace(username))
	if username != "" {
		h.loginLimiter.ResetPrefix(username + "|")
	}
	ip = canonicalIP(ip)
	if ip != "" {
		h.loginIPLimiter.Reset(ip)
	}
	if registry, ok := h.sessions.(*redisSessionRegistry); ok {
		rateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if username != "" {
			_ = registry.ResetRateGroup(rateCtx, "login_pair", username)
		}
		if ip != "" {
			_ = registry.ResetRate(rateCtx, "login_ip", ip)
		}
	}
}

func (h *accountAuth) admitLoginAttempt(ctx context.Context, username string, pairKey string, ip string, ipLimit int, now time.Time) (bool, bool, error) {
	if registry, ok := h.sessions.(*redisSessionRegistry); ok {
		pairMember, err := randomUUID()
		if err != nil {
			return false, false, err
		}
		ipMember, err := randomUUID()
		if err != nil {
			return false, false, err
		}
		rateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		pairAllowed, err := registry.AllowGroupedRate(rateCtx, "login_pair", pairKey, username, pairMember, h.loginLimit, h.loginWindow, now)
		if err != nil {
			return false, false, err
		}
		if !pairAllowed {
			return false, true, nil
		}
		ipAllowed, err := registry.AllowRate(rateCtx, "login_ip", ip, ipMember, ipLimit, h.loginWindow, now)
		if err != nil {
			return false, false, err
		}
		return pairAllowed, ipAllowed, nil
	}
	pairAllowed := h.loginLimiter.Allow(pairKey, h.loginLimit, h.loginWindow, now)
	if !pairAllowed {
		return false, true, nil
	}
	return true, h.loginIPLimiter.Allow(ip, ipLimit, h.loginWindow, now), nil
}

func (h *accountAuth) resetLoginPair(ctx context.Context, pairKey string) {
	h.loginLimiter.Reset(pairKey)
	if registry, ok := h.sessions.(*redisSessionRegistry); ok {
		rateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_ = registry.ResetRate(rateCtx, "login_pair", pairKey)
	}
}

func (h *accountAuth) AllowOrigination(ctx context.Context, identity Identity) error {
	if !identity.Account.AllowOrigination {
		return &AuthError{Code: "origination_forbidden", Detail: "This account is not allowed to originate alerts.", HTTPStatus: http.StatusForbidden}
	}
	now := time.Now().UTC()
	key := identity.Account.ID
	allowed := false
	if registry, ok := h.sessions.(*redisSessionRegistry); ok {
		member, err := randomUUID()
		if err != nil {
			return &AuthError{Code: "origination_rate_unavailable", Detail: "The alert origination rate limiter is unavailable.", HTTPStatus: http.StatusServiceUnavailable}
		}
		rateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		allowed, err = registry.AllowRate(rateCtx, "origination", key, member, h.originationRate, time.Second, now)
		if err != nil {
			return &AuthError{Code: "origination_rate_unavailable", Detail: "The alert origination rate limiter is unavailable.", HTTPStatus: http.StatusServiceUnavailable}
		}
	} else {
		allowed = h.originationLimiter.Allow(key, h.originationRate, time.Second, now)
	}
	if !allowed {
		return &AuthError{Code: "origination_rate_limited", Detail: fmt.Sprintf("Alert origination is limited to %d requests per second.", h.originationRate), HTTPStatus: http.StatusTooManyRequests}
	}
	return nil
}

func (h *accountAuth) hashPassword(password string) (string, error) {
	if !utf8.ValidString(password) || utf8.RuneCountInString(password) < 12 {
		return "", fmt.Errorf("password must contain at least 12 characters")
	}
	if len(password) > maxPasswordBytes {
		return "", fmt.Errorf("password is too long")
	}
	peppered := pepperPassword(h.pepper, password)
	salt := make([]byte, argonSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	h.argonSlots <- struct{}{}
	output := argon2.IDKey(peppered, salt, argonIterations, argonMemoryKiB, argonParallelism, argonOutputLength)
	<-h.argonSlots
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		argonMemoryKiB, argonIterations, argonParallelism,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(output)), nil
}

func (h *accountAuth) verifyPassword(ctx context.Context, encoded string, password string) (bool, error) {
	params, salt, expected, err := parseSecureArgon2Hash(encoded)
	if err != nil {
		return false, err
	}
	peppered := pepperPassword(h.pepper, password)
	select {
	case h.argonSlots <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	actual := argon2.IDKey(peppered, salt, params.time, params.memory, uint8(params.parallelism), uint32(len(expected)))
	<-h.argonSlots
	return subtle.ConstantTimeCompare(actual, expected) == 1, nil
}

func parseSecureArgon2Hash(encoded string) (argon2Params, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != "v=19" {
		return argon2Params{}, nil, nil, fmt.Errorf("unsupported password hash")
	}
	params, err := parseArgon2Params(parts[3])
	if err != nil {
		return argon2Params{}, nil, nil, err
	}
	if params.memory != argonMemoryKiB || params.time != argonIterations || params.parallelism != uint32(argonParallelism) {
		return argon2Params{}, nil, nil, fmt.Errorf("password hash does not use the required Argon2id profile")
	}
	salt, err := decodePHCBase64(parts[4])
	if err != nil || len(salt) < argonSaltLength {
		return argon2Params{}, nil, nil, fmt.Errorf("password hash salt is invalid")
	}
	expected, err := decodePHCBase64(parts[5])
	if err != nil || len(expected) < argonOutputLength || len(expected) > 64 {
		return argon2Params{}, nil, nil, fmt.Errorf("password hash output is invalid")
	}
	return params, salt, expected, nil
}

func pepperPassword(pepper []byte, password string) []byte {
	mac := hmac.New(sha256.New, pepper)
	_, _ = mac.Write([]byte(password))
	return mac.Sum(nil)
}

func (h *accountAuth) issueToken(account Account, sessionID string, ip string, issuedAt time.Time, expiresAt time.Time, passwordChangeRequired bool) (string, error) {
	token := paseto.NewToken()
	token.SetIssuer(pasetoIssuer)
	token.SetAudience(pasetoAudience)
	token.SetSubject(account.ID)
	token.SetJti(sessionID)
	token.SetIssuedAt(issuedAt)
	token.SetNotBefore(issuedAt.Add(-5 * time.Second))
	token.SetExpiration(expiresAt)
	token.SetString("username", account.Username)
	token.SetString("ip", ip)
	if err := token.Set("admin", account.IsAdmin); err != nil {
		return "", err
	}
	if err := token.Set("password_change_required", passwordChangeRequired); err != nil {
		return "", err
	}
	if err := token.Set("auth_version", account.authVersion); err != nil {
		return "", err
	}
	return token.V4Encrypt(h.pasetoKey, []byte(pasetoImplicitAssertion)), nil
}

func (h *accountAuth) encryptMFASecret(accountID string, secret string) (string, error) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return "", fmt.Errorf("account ID is required to encrypt an MFA secret")
	}
	aead, err := chacha20poly1305.NewX(h.mfaKey)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := aead.Seal(nil, nonce, []byte(secret), []byte("haze-mfa-secret:v2:"+accountID))
	return "mfa2." + base64.RawURLEncoding.EncodeToString(append(nonce, sealed...)), nil
}

func (h *accountAuth) decryptMFASecret(accountID string, encrypted string) (string, error) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return "", fmt.Errorf("account ID is required to decrypt an MFA secret")
	}
	if !strings.HasPrefix(encrypted, "mfa2.") {
		return "", fmt.Errorf("unsupported MFA secret format")
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(encrypted, "mfa2."))
	if err != nil {
		return "", err
	}
	aead, err := chacha20poly1305.NewX(h.mfaKey)
	if err != nil {
		return "", err
	}
	if len(raw) <= aead.NonceSize() {
		return "", fmt.Errorf("invalid MFA secret ciphertext")
	}
	opened, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte("haze-mfa-secret:v2:"+accountID))
	if err != nil {
		return "", err
	}
	return string(opened), nil
}

func (h *accountAuth) validateMFA(ctx context.Context, account Account, passcode string, now time.Time) error {
	secret, err := h.decryptMFASecret(account.ID, account.mfaSecret)
	if err != nil {
		return err
	}
	passcode = strings.TrimSpace(passcode)
	if len(passcode) != 6 {
		return fmt.Errorf("invalid MFA code")
	}
	options := totp.ValidateOpts{Period: 30, Skew: 0, Digits: otp.DigitsSix, Algorithm: otp.AlgorithmSHA1}
	acceptedStep := int64(0)
	for offset := -1; offset <= 1; offset++ {
		candidateTime := now.Add(time.Duration(offset) * 30 * time.Second)
		ok, validateErr := totp.ValidateCustom(passcode, secret, candidateTime, options)
		if validateErr != nil {
			return validateErr
		}
		if ok {
			acceptedStep = candidateTime.Unix() / 30
			break
		}
	}
	if acceptedStep == 0 || acceptedStep <= account.mfaLastStep {
		return fmt.Errorf("invalid or replayed MFA code")
	}
	return h.store.AcceptMFAStep(ctx, account.ID, account.authVersion, acceptedStep)
}

func (h *accountAuth) clientIP(request *http.Request) string {
	if request == nil {
		return "0.0.0.0"
	}
	remote := canonicalIP(hostOnly(request.RemoteAddr))
	if remote == "" {
		remote = "0.0.0.0"
	}
	if !ipAllowed(remote, h.trustedProxyCIDRs) || len(h.trustedProxyCIDRs) == 0 {
		return remote
	}
	forwarded := strings.Split(request.Header.Get("X-Forwarded-For"), ",")
	for index := len(forwarded) - 1; index >= 0; index-- {
		candidate := canonicalIP(strings.TrimSpace(forwarded[index]))
		if candidate == "" {
			continue
		}
		if !ipAllowed(candidate, h.trustedProxyCIDRs) {
			return candidate
		}
		remote = candidate
	}
	if realIP := canonicalIP(request.Header.Get("X-Real-IP")); realIP != "" {
		return realIP
	}
	return remote
}

func (h *accountAuth) secureRequest(request *http.Request) bool {
	if request == nil {
		return false
	}
	if request.TLS != nil {
		return true
	}
	remote := canonicalIP(hostOnly(request.RemoteAddr))
	if remote == "" || len(h.trustedProxyCIDRs) == 0 || !ipAllowed(remote, h.trustedProxyCIDRs) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(request.Header.Get("X-Forwarded-Proto")), "https")
}

func accountTokenFromRequest(request *http.Request) string {
	if request == nil {
		return ""
	}
	if cookie, err := request.Cookie(sessionCookieName); err == nil {
		return strings.TrimSpace(cookie.Value)
	}
	if header := strings.TrimSpace(request.Header.Get("Authorization")); strings.HasPrefix(strings.ToLower(header), "bearer ") {
		return strings.TrimSpace(header[len("Bearer "):])
	}
	return ""
}

func requiredEnvironmentKey(envName string, minimum int) ([]byte, error) {
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		return nil, fmt.Errorf("%s is required", envName)
	}
	decoders := []func(string) ([]byte, error){
		hex.DecodeString,
		base64.RawURLEncoding.DecodeString,
		base64.StdEncoding.DecodeString,
	}
	for _, decode := range decoders {
		if raw, err := decode(value); err == nil && len(raw) >= minimum {
			return raw, nil
		}
	}
	return nil, fmt.Errorf("%s must be an encoded random key of at least %d bytes", envName, minimum)
}

func requiredEnvironmentKeyExact(envName string, length int) ([]byte, error) {
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		return nil, fmt.Errorf("%s is required", envName)
	}
	decoders := []func(string) ([]byte, error){
		hex.DecodeString,
		base64.RawURLEncoding.DecodeString,
		base64.StdEncoding.DecodeString,
	}
	for _, decode := range decoders {
		if raw, err := decode(value); err == nil && len(raw) == length {
			return raw, nil
		}
	}
	return nil, fmt.Errorf("%s must be an encoded random key of exactly %d bytes", envName, length)
}

func randomUUID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate UUID: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(raw[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func durationSeconds(seconds int, fallback time.Duration) time.Duration {
	if seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func positiveOr(value int, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func parseCIDRList(values []string) ([]*net.IPNet, error) {
	out := []*net.IPNet{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if ip := net.ParseIP(value); ip != nil {
			bits := 128
			if ip.To4() != nil {
				bits = 32
			}
			value += "/" + strconv.Itoa(bits)
		}
		_, network, err := net.ParseCIDR(value)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR %q", value)
		}
		out = append(out, network)
	}
	return out, nil
}

func validCIDR(value string) bool {
	if strings.TrimSpace(value) == "" {
		return false
	}
	_, err := parseCIDRList([]string{value})
	return err == nil
}

func ipAllowed(rawIP string, networks []*net.IPNet) bool {
	if len(networks) == 0 {
		return true
	}
	ip := net.ParseIP(canonicalIP(rawIP))
	if ip == nil {
		return false
	}
	for _, network := range networks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func sameCanonicalIP(left string, right string) bool {
	leftIP := net.ParseIP(canonicalIP(left))
	rightIP := net.ParseIP(canonicalIP(right))
	return leftIP != nil && rightIP != nil && leftIP.Equal(rightIP)
}

func canonicalIP(value string) string {
	ip := net.ParseIP(strings.TrimSpace(value))
	if ip == nil {
		return ""
	}
	return ip.String()
}

func hostOnly(value string) string {
	value = strings.TrimSpace(value)
	if host, _, err := net.SplitHostPort(value); err == nil {
		return host
	}
	return strings.Trim(value, "[]")
}

func cleanUserAgent(request *http.Request) string {
	if request == nil {
		return ""
	}
	value := strings.TrimSpace(request.UserAgent())
	if len(value) > 512 {
		value = value[:512]
	}
	return value
}

func passwordExpired(account Account, now time.Time) bool {
	if account.PasswordExpiryDays <= 0 || account.PasswordChangedAt.IsZero() {
		return false
	}
	return !account.PasswordChangedAt.Add(time.Duration(account.PasswordExpiryDays) * 24 * time.Hour).After(now)
}

func invalidCredentials() error {
	return &AuthError{Code: "invalid_credentials", Detail: "The username, password, or authentication code is incorrect.", HTTPStatus: http.StatusUnauthorized}
}

func (h *accountAuth) unavailableError() error {
	detail := "Sign-in is not configured."
	if h != nil && h.initializationError != nil {
		detail = "Sign-in is unavailable. Check the server configuration."
	}
	return &AuthError{Code: "auth_unavailable", Detail: detail, HTTPStatus: http.StatusServiceUnavailable}
}
