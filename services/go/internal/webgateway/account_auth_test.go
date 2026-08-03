package webgateway

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pquerna/otp/totp"
)

func TestAccountLoginIssuesPASETOAndPinsClientIP(t *testing.T) {
	manager, configPath := newTestAccountAuthManager(t, false)
	defer manager.Close()
	request := testLoginRequest("24.120.53.11:41000")
	result, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: request,
	})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if !strings.HasPrefix(result.Token, "v4.local.") {
		t.Fatalf("token is not PASETO v4.local: %q", result.Token)
	}
	authRequest := httptest.NewRequest(http.MethodGet, "https://example.test/admin", nil)
	authRequest.RemoteAddr = request.RemoteAddr
	authRequest.AddCookie(&http.Cookie{Name: sessionCookieName, Value: result.Token})
	identity, err := manager.Identity(authRequest)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if identity.Account.Username != "admin" || !identity.Account.IsAdmin {
		t.Fatalf("identity = %#v", identity)
	}
	if identity.Session.IP != "24.120.53.11" {
		t.Fatalf("session IP = %q", identity.Session.IP)
	}

	mismatch := httptest.NewRequest(http.MethodGet, "https://example.test/admin", nil)
	mismatch.RemoteAddr = "24.120.53.12:41001"
	mismatch.AddCookie(&http.Cookie{Name: sessionCookieName, Value: result.Token})
	if _, err := manager.Identity(mismatch); err == nil {
		t.Fatal("IP-mismatched session was accepted")
	}
	if _, err := manager.Identity(authRequest); err == nil {
		t.Fatal("IP-mismatched session was not revoked")
	}

	if _, err := os.Stat(filepath.Join(filepath.Dir(configPath), "logs", "access", "admin.log")); err != nil {
		t.Fatalf("access audit log: %v", err)
	}
}

func TestAccountBrowserCloseCookieHasNoPersistentExpiry(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	request := testLoginRequest("192.0.2.20:5000")
	result, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	manager.SetLoginCookie(recorder, request, result)
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies = %#v", cookies)
	}
	if cookies[0].MaxAge != 0 || !cookies[0].Expires.IsZero() {
		t.Fatalf("browser-close cookie unexpectedly persisted: %#v", cookies[0])
	}
	if !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("cookie is missing required protections: %#v", cookies[0])
	}
	if !cookies[0].Secure {
		t.Fatalf("account cookie is not Secure: %#v", cookies[0])
	}
}

func TestAccountLoginRejectsPlainHTTP(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	request := httptest.NewRequest(http.MethodPost, "http://example.test/api/v1/auth/login", nil)
	request.RemoteAddr = "192.0.2.44:5000"
	_, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: request,
	})
	var authErr *AuthError
	if !errors.As(err, &authErr) || authErr.Code != "https_required" {
		t.Fatalf("plain HTTP login error = %#v", err)
	}
	if authErr.Detail != "Sign in requires HTTPS." {
		t.Fatalf("unexpected user-facing detail: %q", authErr.Detail)
	}
}

func TestUnavailableAuthErrorsUsePlainUserFacingLanguage(t *testing.T) {
	tests := []struct {
		name string
		auth *accountAuth
		want string
	}{
		{name: "not configured", auth: &accountAuth{}, want: "Sign-in is not configured."},
		{
			name: "initialization failed",
			auth: &accountAuth{initializationError: errors.New("private initialization detail")},
			want: "Sign-in is unavailable. Check the server configuration.",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var authErr *AuthError
			if err := test.auth.unavailableError(); !errors.As(err, &authErr) {
				t.Fatalf("expected AuthError, got %T", err)
			}
			if authErr.Detail != test.want {
				t.Fatalf("detail = %q, want %q", authErr.Detail, test.want)
			}
		})
	}
}

func TestAccountPasswordHashUsesRequiredPepperedProfile(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	hash, err := manager.accounts.hashPassword("another correct battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(hash, "another correct battery staple") {
		t.Fatal("password appeared in encoded hash")
	}
	if !strings.Contains(hash, "$m=65536,t=3,p=4$") {
		t.Fatalf("hash profile = %q", hash)
	}
	ok, err := manager.accounts.verifyPassword(context.Background(), hash, "another correct battery staple")
	if err != nil || !ok {
		t.Fatalf("verify correct password: ok=%v err=%v", ok, err)
	}
	ok, err = manager.accounts.verifyPassword(context.Background(), hash, "wrong password")
	if err != nil || ok {
		t.Fatalf("verify wrong password: ok=%v err=%v", ok, err)
	}
}

func TestAccountLoginLocksAccountAfterFiveFailures(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	for attempt := 0; attempt < 5; attempt++ {
		request := testLoginRequest("198.51.100.9:44000")
		_, _ = manager.LoginWithRequest(context.Background(), LoginInput{
			Username: "admin", Password: "incorrect password value", Request: request,
		})
	}
	account, err := manager.accounts.store.ByUsername(context.Background(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	if !account.AccountLocked || account.FailedLoginAttempts != 5 {
		t.Fatalf("account lock state = locked:%v attempts:%d", account.AccountLocked, account.FailedLoginAttempts)
	}
	_, err = manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: testLoginRequest("198.51.100.9:44001"),
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "locked") {
		t.Fatalf("locked account login error = %v", err)
	}
}

func TestAccountMFAEnrollmentAndReplayProtection(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, true)
	defer manager.Close()
	request := testLoginRequest("203.0.113.20:55000")
	challenge, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: request,
	})
	if err == nil || !challenge.MFAEnrollmentRequired || challenge.MFAEnrollmentSecret == "" {
		t.Fatalf("MFA challenge = %#v, err=%v", challenge, err)
	}
	code, err := totp.GenerateCode(challenge.MFAEnrollmentSecret, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", TOTP: code, Request: request,
	})
	if err != nil || result.Token == "" {
		t.Fatalf("MFA login: result=%#v err=%v", result, err)
	}
	manager.accounts.Logout(authenticatedRequest(result.Token, request.RemoteAddr))
	if _, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", TOTP: code, Request: request,
	}); err == nil {
		t.Fatal("replayed MFA code was accepted")
	}
}

func TestOptionalMFADoesNotEnforcePendingEnrollment(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	ctx := context.Background()
	account, err := manager.accounts.store.ByUsername(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := manager.accounts.encryptMFASecret(account.ID, "JBSWY3DPEHPK3PXP")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.accounts.store.SetMFA(ctx, account.ID, encrypted, false); err != nil {
		t.Fatal(err)
	}

	result, err := manager.LoginWithRequest(ctx, LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: testLoginRequest("203.0.113.21:55001"),
	})
	if err != nil || result.Token == "" {
		t.Fatalf("optional MFA login: result=%#v err=%v", result, err)
	}
	stored, err := manager.accounts.store.ByUsername(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if !stored.MFAConfigured || stored.MFAEnabled {
		t.Fatalf("pending MFA state changed unexpectedly: configured=%v enabled=%v", stored.MFAConfigured, stored.MFAEnabled)
	}
}

func TestAuditLoggerDetectsModifiedEntry(t *testing.T) {
	logger, err := newAuditLogger(t.TempDir(), []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if err := logger.Append("access", AuditEvent{Event: "LOGIN_SUCCESS", ActorUsername: "admin"}); err != nil {
		t.Fatal(err)
	}
	path := logger.logPath("access", "admin")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(raw), "LOGIN_SUCCESS", "LOGIN_FAILURE", 1)
	if err := os.WriteFile(path, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := logger.VerifyAll(); err == nil {
		t.Fatal("tampered audit entry passed integrity verification")
	}
}

func TestAuditLoggerDetectsModifiedCheckpoint(t *testing.T) {
	dir := t.TempDir()
	logger, err := newAuditLogger(dir, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if err := logger.Append("access", AuditEvent{Event: "LOGIN_SUCCESS", ActorUsername: "admin"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "access", "integrity.sig")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(raw), `"index": 1`, `"index": 0`, 1)
	if err := os.WriteFile(path, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := logger.VerifyAll(); err == nil {
		t.Fatal("tampered audit checkpoint passed integrity verification")
	}
}

func TestAccountAuthVersionRevokesRacingSession(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	loginRequest := testLoginRequest("203.0.113.31:51000")
	result, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: loginRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.accounts.store.BumpAuthVersion(context.Background(), result.Identity.Account.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Identity(authenticatedRequest(result.Token, loginRequest.RemoteAddr)); err == nil {
		t.Fatal("session remained valid after authentication version changed")
	}
}

func TestConditionalSelfPasswordChangeCannotOverwriteAdminReset(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	account, err := manager.accounts.store.ByUsername(context.Background(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	adminHash, err := manager.accounts.hashPassword("administrator replacement password")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.accounts.store.SetPassword(context.Background(), account.ID, adminHash); err != nil {
		t.Fatal(err)
	}
	staleUserHash, err := manager.accounts.hashPassword("stale user replacement password")
	if err != nil {
		t.Fatal(err)
	}
	err = manager.accounts.store.SetPasswordIfAuthVersion(context.Background(), account.ID, staleUserHash, account.authVersion)
	if !errors.Is(err, errAccountVersionChanged) {
		t.Fatalf("stale password update error = %v", err)
	}
	stored, err := manager.accounts.store.ByID(context.Background(), account.ID)
	if err != nil {
		t.Fatal(err)
	}
	adminOK, err := manager.accounts.verifyPassword(context.Background(), stored.passwordHash, "administrator replacement password")
	if err != nil || !adminOK {
		t.Fatalf("administrator password was not preserved: ok=%v err=%v", adminOK, err)
	}
	staleOK, err := manager.accounts.verifyPassword(context.Background(), stored.passwordHash, "stale user replacement password")
	if err != nil || staleOK {
		t.Fatalf("stale user password replaced administrator password: ok=%v err=%v", staleOK, err)
	}
}

func TestConditionalMFAEnrollmentCannotSurviveCredentialReset(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	account, err := manager.accounts.store.ByUsername(context.Background(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	replacementHash, err := manager.accounts.hashPassword("administrator replacement password")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.accounts.store.SetPassword(context.Background(), account.ID, replacementHash); err != nil {
		t.Fatal(err)
	}
	encrypted, err := manager.accounts.encryptMFASecret(account.ID, "JBSWY3DPEHPK3PXP")
	if err != nil {
		t.Fatal(err)
	}
	err = manager.accounts.store.SetMFAIfAuthVersion(context.Background(), account.ID, encrypted, account.authVersion)
	if !errors.Is(err, errAccountVersionChanged) {
		t.Fatalf("stale MFA enrollment error = %v", err)
	}
	stored, err := manager.accounts.store.ByID(context.Background(), account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.MFAConfigured {
		t.Fatal("stale MFA enrollment was stored after a credential reset")
	}
}

func TestAccountPolicySaveRevokesExistingSessionByVersion(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	request := testLoginRequest("203.0.113.48:51000")
	result, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	account, err := manager.accounts.store.ByID(context.Background(), result.Identity.Account.ID)
	if err != nil {
		t.Fatal(err)
	}
	account.CanViewLogs = !account.CanViewLogs
	account.BlockedEventCodes = []string{"EAN", "NPT"}
	if err := manager.accounts.store.Save(context.Background(), account); err != nil {
		t.Fatal(err)
	}
	stored, err := manager.accounts.store.ByID(context.Background(), account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.BlockedEventCodes) != 2 || stored.BlockedEventCodes[0] != "EAN" || stored.BlockedEventCodes[1] != "NPT" {
		t.Fatalf("stored blocked event codes = %v", stored.BlockedEventCodes)
	}
	if _, err := manager.Identity(authenticatedRequest(result.Token, request.RemoteAddr)); err == nil {
		t.Fatal("session remained valid after account policy changed")
	}
}

func TestAccountStoreOutageDoesNotDeleteSessionLease(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	loginRequest := testLoginRequest("203.0.113.47:51000")
	result, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: loginRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.accounts.store.db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = manager.Identity(authenticatedRequest(result.Token, loginRequest.RemoteAddr))
	var authErr *AuthError
	if !errors.As(err, &authErr) || authErr.Code != "session_unavailable" {
		t.Fatalf("database outage authentication error = %#v", err)
	}
	if _, err := manager.accounts.sessions.Get(context.Background(), result.Identity.RawSessionID); err != nil {
		t.Fatalf("database outage revoked the session lease: %v", err)
	}
}

func TestLoginAttemptAdmissionIsAtomicInMemory(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	const attempts = 40
	var allowed atomic.Int32
	var wait sync.WaitGroup
	wait.Add(attempts)
	now := time.Now().UTC()
	for index := 0; index < attempts; index++ {
		go func() {
			defer wait.Done()
			pairAllowed, ipAllowed, err := manager.accounts.admitLoginAttempt(context.Background(), "admin", "admin|198.51.100.77", "198.51.100.77", 100, now)
			if err != nil {
				t.Errorf("admit login attempt: %v", err)
				return
			}
			if pairAllowed && ipAllowed {
				allowed.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := allowed.Load(); got != 5 {
		t.Fatalf("concurrent allowed attempts = %d, want 5", got)
	}
}

func TestMFASecretCiphertextIsBoundToAccount(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	encrypted, err := manager.accounts.encryptMFASecret("account-one", "JBSWY3DPEHPK3PXP")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := manager.accounts.decryptMFASecret("account-one", encrypted)
	if err != nil || secret != "JBSWY3DPEHPK3PXP" {
		t.Fatalf("decrypt bound MFA secret: secret=%q err=%v", secret, err)
	}
	if _, err := manager.accounts.decryptMFASecret("account-two", encrypted); err == nil {
		t.Fatal("MFA secret ciphertext was accepted for a different account")
	}
}

func TestPasswordExpiryIsRecomputedForExistingSession(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	loginRequest := testLoginRequest("203.0.113.32:51000")
	result, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: loginRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-91 * 24 * time.Hour).Format(time.RFC3339Nano)
	if _, err := manager.accounts.store.db.Exec(`UPDATE users SET password_changed_at=? WHERE id=?`, old, result.Identity.Account.ID); err != nil {
		t.Fatal(err)
	}
	authRequest := authenticatedRequest(result.Token, loginRequest.RemoteAddr)
	identity, err := manager.Identity(authRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !identity.PasswordChangeRequired {
		t.Fatal("existing session did not pick up newly expired password")
	}
	if !manager.Authenticated(authRequest) {
		t.Fatal("expired-password session cannot reach its password-change flow")
	}
	if manager.FullyAuthenticated(authRequest) {
		t.Fatal("expired-password session passed full authorization")
	}
}

func TestLoginFailureWindowExpires(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	account, err := manager.accounts.store.ByUsername(context.Background(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for attempt := 0; attempt < 4; attempt++ {
		if _, err := manager.accounts.store.RecordLoginFailure(context.Background(), account.ID, 5, 15*time.Minute, now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	updated, err := manager.accounts.store.RecordLoginFailure(context.Background(), account.ID, 5, 15*time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if updated.AccountLocked || updated.FailedLoginAttempts != 1 {
		t.Fatalf("expired failure window = locked:%v attempts:%d", updated.AccountLocked, updated.FailedLoginAttempts)
	}
}

func TestCommandErrorResponsePreservesAuthorizationStatus(t *testing.T) {
	status, payload := commandErrorResponse(&AuthError{Code: "origination_forbidden", Detail: "forbidden", HTTPStatus: http.StatusForbidden})
	if status != http.StatusForbidden || payload["code"] != "origination_forbidden" {
		t.Fatalf("command error response = status:%d payload:%#v", status, payload)
	}
}

func TestAccountOriginationPolicyOverridesSenderAndName(t *testing.T) {
	identity := Identity{
		Account: Account{
			ID: "user-id", Username: "operator", AllowOrigination: true,
			AllowedOriginators: []string{"CIV"}, ForceSenderID: true, SenderID: "ABCD1234",
			ForceOriginatorName: true, OriginatorNameText: "Emergency Management", IncludeIPInBrackets: true,
		},
		Session: ActiveSession{ID: "session-digest", IP: "203.0.113.8"},
	}
	payload, err := applyAccountOriginationPolicy(map[string]any{"originator": "CIV"}, identity)
	if err != nil {
		t.Fatal(err)
	}
	if payload["sender_id"] != "ABCD1234" || payload["originator_name"] != "Emergency Management [203.0.113.8]" {
		t.Fatalf("policy payload = %#v", payload)
	}
	if _, err := applyAccountOriginationPolicy(map[string]any{"originator": "EAS"}, identity); err == nil {
		t.Fatal("disallowed originator was accepted")
	}
}

func TestAccountOriginationPolicyBlocksConfiguredEventCodes(t *testing.T) {
	identity := Identity{Account: Account{
		AllowedOriginators: []string{"EAS"},
		BlockedEventCodes:  []string{"EAN", "NPT"},
	}}
	if _, err := applyAccountOriginationPolicy(map[string]any{"originator": "EAS", "same_event": "EAN"}, identity); err == nil {
		t.Fatal("blocked national alert event was accepted")
	} else {
		var authErr *AuthError
		if !errors.As(err, &authErr) || authErr.Code != "event_code_forbidden" || authErr.HTTPStatus != http.StatusForbidden {
			t.Fatalf("blocked event error = %#v", err)
		}
	}
	if _, err := applyAccountOriginationPolicy(map[string]any{"originator": "EAS", "event_code": "RWT"}, identity); err != nil {
		t.Fatalf("allowed event was rejected: %v", err)
	}
}

func TestTemplateOriginatorIsResolvedBeforeAccountPolicy(t *testing.T) {
	payload, err := prepareAccountOriginationPolicyPayload("config.yaml", "same.test", map[string]any{
		"template": map[string]any{"same": map[string]any{"originator": "EAS", "event": "NPT"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := stringValue(payload, "originator"); got != "EAS" {
		t.Fatalf("resolved template originator = %q, want EAS", got)
	}
	if got := stringValue(payload, "same_event"); got != "NPT" {
		t.Fatalf("resolved template event = %q, want NPT", got)
	}
	identity := Identity{Account: Account{AllowedOriginators: []string{"EAS"}, BlockedEventCodes: []string{"NPT"}}}
	if _, err := applyAccountOriginationPolicy(payload, identity); err == nil {
		t.Fatal("blocked template event was accepted")
	}
	identity.Account.BlockedEventCodes = nil
	if _, err := applyAccountOriginationPolicy(payload, identity); err != nil {
		t.Fatalf("EAS-only account was denied its template originator: %v", err)
	}
}

func TestNonAdminArchiveRBACAllowsOnlyOriginationActions(t *testing.T) {
	account := Account{CanViewLogs: true, AllowOrigination: true}
	for _, action := range []string{"rebroadcast", "force_broadcast_without_same", "preview_same"} {
		if !nonAdminCommandAllowed("alerts.archive.action", map[string]any{"action": action}, account) {
			t.Fatalf("safe archive action %q was denied", action)
		}
	}
	for _, action := range []string{"delete", "clear_all", "expire_all"} {
		if nonAdminCommandAllowed("alerts.archive.action", map[string]any{"action": action}, account) {
			t.Fatalf("destructive archive action %q was allowed", action)
		}
	}
	account.CanViewLogs = false
	if nonAdminCommandAllowed("alerts.archive.action", map[string]any{"action": "rebroadcast"}, account) {
		t.Fatal("archive rebroadcast was allowed without archive visibility")
	}
}

func TestExpiredPasswordWithoutSelfChangeRequiresAdminReset(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	account, err := manager.accounts.store.ByUsername(context.Background(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	changedAt := time.Now().UTC().Add(-2 * 24 * time.Hour).Format(time.RFC3339Nano)
	if _, err := manager.accounts.store.db.Exec(`UPDATE users SET allow_user_pw_change=false, password_expiry_days=1, password_changed_at=? WHERE id=?`, changedAt, account.ID); err != nil {
		t.Fatal(err)
	}
	_, err = manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: testLoginRequest("203.0.113.91:51000"),
	})
	var authErr *AuthError
	if !errors.As(err, &authErr) || authErr.Code != "password_reset_required" {
		t.Fatalf("expired non-self-service password error = %#v", err)
	}
}

func TestAccountStorePreventsDemotingLastUnlockedAdministrator(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	account, err := manager.accounts.store.ByUsername(context.Background(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	account.IsAdmin = false
	if err := manager.accounts.store.Save(context.Background(), account); err == nil {
		t.Fatal("last unlocked administrator was demoted")
	}
	stored, err := manager.accounts.store.ByID(context.Background(), account.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.IsAdmin {
		t.Fatal("failed last-admin demotion changed the database")
	}
}

func TestAuditLoggerRenamePreflightDoesNotPartiallyMoveCategories(t *testing.T) {
	logger, err := newAuditLogger(t.TempDir(), []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if err := logger.Append("access", AuditEvent{Event: "LOGIN_SUCCESS", ActorUsername: "oldname"}); err != nil {
		t.Fatal(err)
	}
	if err := logger.Append("webpanel", AuditEvent{Event: "ACCOUNT_CREATED", ActorUsername: "newname"}); err != nil {
		t.Fatal(err)
	}
	if err := logger.RenameAccount("oldname", "newname"); err == nil {
		t.Fatal("rename conflict was accepted")
	}
	if _, err := os.Stat(logger.logPath("access", "oldname")); err != nil {
		t.Fatalf("source access log was moved during failed preflight: %v", err)
	}
	if _, err := os.Stat(logger.logPath("access", "newname")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination access log exists after failed preflight: %v", err)
	}
	if err := logger.VerifyAll(); err != nil {
		t.Fatalf("audit tree failed verification after rejected rename: %v", err)
	}
}

func newTestAccountAuthManager(t *testing.T, enforceMFA bool) (*AuthManager, string) {
	t.Helper()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	config := authEnabledConfig()
	config.Webpanel.Authentication.Mode = "accounts"
	config.Webpanel.Authentication.EnforceMFA = enforceMFA
	config.Webpanel.Authentication.RedisRequired = false
	config.Webpanel.Authentication.AuditDir = "logs"
	config.Webpanel.Authentication.SecureCookies = false
	config.Webpanel.Authentication.LoginRateLimit = 5
	config.Webpanel.Authentication.LoginRateWindowSeconds = 900
	config.Storage.SQLite.Path = "runtime/state/haze.db"
	t.Setenv("HAZE_PASETO_V4_LOCAL_KEY", encodedTestKey(1))
	t.Setenv("HAZE_PASSWORD_PEPPER", encodedTestKey(2))
	t.Setenv("HAZE_MFA_ENCRYPTION_KEY", encodedTestKey(3))
	t.Setenv("HAZE_AUDIT_HMAC_KEY", encodedTestKey(4))
	t.Setenv("HAZE_REDIS_URL", "")
	t.Setenv("HAZE_BOOTSTRAP_ADMIN_USERNAME", "admin")
	t.Setenv("HAZE_BOOTSTRAP_ADMIN_PASSWORD", "correct horse battery staple")
	t.Setenv("ADMIN_PASSWD", "")
	manager := NewAuthManagerWithPath(config, configPath)
	if !manager.Configured() {
		t.Fatalf("account authentication was not configured: %v", manager.accounts.initializationError)
	}
	return manager, configPath
}

func encodedTestKey(value byte) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat(string([]byte{value}), 32)))
}

func testLoginRequest(remoteAddr string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "https://example.test/api/v1/auth/login", nil)
	request.RemoteAddr = remoteAddr
	request.Header.Set("User-Agent", "Haze Test Client")
	return request
}

func authenticatedRequest(token string, remoteAddr string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "https://example.test/api/v1/auth/logout", nil)
	request.RemoteAddr = remoteAddr
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	return request
}

func TestPasswordExpiry(t *testing.T) {
	account := Account{PasswordExpiryDays: 90, PasswordChangedAt: time.Now().UTC().Add(-91 * 24 * time.Hour)}
	if !passwordExpired(account, time.Now().UTC()) {
		t.Fatal("expired password was not detected")
	}
}
