package webgateway

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

func (s *wsSession) accountCommand(command string, payload map[string]any) (any, error) {
	if s == nil || s.auth == nil || s.auth.accounts == nil {
		return nil, fmt.Errorf("account management is unavailable in the current sign-in mode")
	}
	if serializedAccountMutation(command) {
		s.auth.accounts.accountMutationMu.Lock()
		defer s.auth.accounts.accountMutationMu.Unlock()
	}
	// Re-authenticate after waiting for the mutation lock. A password reset,
	// session revocation, or role change that completed while this request was
	// queued must invalidate the queued mutation before it reads account state.
	actor, err := s.auth.Identity(s.request)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.request.Context(), 10*time.Second)
	defer cancel()
	switch command {
	case "accounts.list":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		accounts, err := s.auth.accounts.ListAccounts(ctx)
		if err != nil {
			return nil, err
		}
		security := map[string]any{
			"enforce_mfa":                    s.config.Webpanel.Authentication.EnforceMFA,
			"session_ttl_seconds":            s.config.Webpanel.Authentication.SessionTTLSeconds,
			"idle_timeout_seconds":           s.config.Webpanel.Authentication.IdleTimeoutSeconds,
			"persistent_session_ttl_seconds": s.config.Webpanel.Authentication.PersistentSessionTTLSeconds,
			"login_rate_limit":               s.config.Webpanel.Authentication.LoginRateLimit,
			"login_rate_window_seconds":      s.config.Webpanel.Authentication.LoginRateWindowSeconds,
			"account_lock_duration_seconds":  int(s.auth.accounts.accountLockDuration.Seconds()),
			"administrator_lock_exempt":      true,
			"origination_rate_per_second":    s.config.Webpanel.Authentication.OriginationRatePerSecond,
			"redis_required":                 s.config.Webpanel.Authentication.RedisRequired,
			"login_cidr_allowlist":           s.config.Webpanel.Authentication.LoginCIDRAllowlist,
		}
		return map[string]any{"accounts": accounts, "security": security}, nil
	case "accounts.get":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		account, err := s.auth.accounts.store.ByID(ctx, stringValue(payload, "id"))
		if err != nil {
			return nil, err
		}
		sessions, err := s.auth.accounts.ListSessions(ctx, account.ID)
		if err != nil {
			return nil, err
		}
		return map[string]any{"account": account, "sessions": sessions}, nil
	case "accounts.create":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		account := accountPolicyFromPayload(Account{
			PasswordExpiryDays: 90, AllowUserPasswordChange: true, LoggingEnabled: true,
		}, payload)
		password := stringValue(payload, "password")
		hash, err := s.auth.accounts.hashPassword(password)
		if err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "ACCOUNT_CREATE_REQUESTED", account, map[string]any{"is_admin": account.IsAdmin}); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.store.Create(ctx, account, hash); err != nil {
			return nil, err
		}
		created, err := s.auth.accounts.store.ByUsername(ctx, account.Username)
		if err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "ACCOUNT_CREATED", created, map[string]any{"is_admin": created.IsAdmin}); err != nil {
			return accountMutationAuditWarning(map[string]any{"account": created}), nil
		}
		return map[string]any{"account": created}, nil
	case "accounts.update":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		account, err := s.auth.accounts.store.ByID(ctx, stringValue(payload, "id"))
		if err != nil {
			return nil, err
		}
		originalAccount := account
		oldUsername := account.Username
		oldAdmin := account.IsAdmin
		account = accountPolicyFromPayload(account, payload)
		if err := validateAccount(&account); err != nil {
			return nil, err
		}
		auditDetails := accountPolicyAuditDetails(originalAccount, account)
		if err := s.auditWebpanel(actor, "ACCOUNT_POLICY_UPDATE_REQUESTED", account, auditDetails); err != nil {
			return nil, err
		}
		if oldAdmin && !account.IsAdmin {
			if err := s.requireAnotherAdministrator(ctx, account.ID); err != nil {
				return nil, err
			}
		}
		renamed := oldUsername != account.Username
		if renamed {
			if err := s.auth.accounts.audit.RenameAccount(oldUsername, account.Username); err != nil {
				return nil, err
			}
		}
		if err := s.auth.accounts.store.Save(ctx, account); err != nil {
			if renamed {
				_ = s.auth.accounts.audit.RenameAccount(account.Username, oldUsername)
			}
			return nil, err
		}
		// Save advances auth_version for every policy update. Purge the stale
		// leases as cleanup; the version change remains authoritative if Redis
		// is temporarily unavailable.
		s.auth.accounts.PurgeUserSessionLeases(ctx, account.ID)
		if actor.Account.ID == account.ID {
			actor.Account.Username = account.Username
		}
		if err := s.auditWebpanel(actor, "ACCOUNT_POLICY_UPDATED", account, auditDetails); err != nil {
			return accountMutationAuditWarning(map[string]any{"account": account}), nil
		}
		return map[string]any{"account": account}, nil
	case "accounts.password.reset":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		account, err := s.auth.accounts.store.ByID(ctx, stringValue(payload, "id"))
		if err != nil {
			return nil, err
		}
		hash, err := s.auth.accounts.hashPassword(stringValue(payload, "password"))
		if err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "PASSWORD_RESET_REQUESTED_BY_ADMIN", account, nil); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.store.SetPassword(ctx, account.ID, hash); err != nil {
			return nil, err
		}
		s.auth.accounts.ResetLoginLimits(ctx, account.Username, account.LastIP)
		if err := s.auth.accounts.RevokeUser(ctx, account.ID); err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "PASSWORD_RESET_BY_ADMIN", account, nil); err != nil {
			return accountMutationAuditWarning(map[string]any{"updated": true, "sessions_revoked": true}), nil
		}
		return map[string]any{"updated": true, "sessions_revoked": true}, nil
	case "accounts.mfa.reset":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		account, err := s.auth.accounts.store.ByID(ctx, stringValue(payload, "id"))
		if err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "MFA_RESET_REQUESTED_BY_ADMIN", account, nil); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.store.SetMFA(ctx, account.ID, "", false); err != nil {
			return nil, err
		}
		s.auth.accounts.ResetLoginLimits(ctx, account.Username, account.LastIP)
		if err := s.auth.accounts.RevokeUser(ctx, account.ID); err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "MFA_RESET_BY_ADMIN", account, nil); err != nil {
			return accountMutationAuditWarning(map[string]any{"updated": true, "sessions_revoked": true}), nil
		}
		return map[string]any{"updated": true, "sessions_revoked": true}, nil
	case "accounts.unlock":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		account, err := s.auth.accounts.store.ByID(ctx, stringValue(payload, "id"))
		if err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "ACCOUNT_UNLOCK_REQUESTED", account, nil); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.store.Unlock(ctx, account.ID); err != nil {
			return nil, err
		}
		s.auth.accounts.ResetLoginLimits(ctx, account.Username, account.LastIP)
		if err := s.auth.accounts.RevokeUser(ctx, account.ID); err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "ACCOUNT_UNLOCKED", account, nil); err != nil {
			return accountMutationAuditWarning(map[string]any{"updated": true, "sessions_revoked": true}), nil
		}
		return map[string]any{"updated": true, "sessions_revoked": true}, nil
	case "accounts.sessions.revoke":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		account, err := s.auth.accounts.store.ByID(ctx, stringValue(payload, "id"))
		if err != nil {
			return nil, err
		}
		sessionID := stringValue(payload, "session_id")
		sessions, err := s.auth.accounts.ListSessions(ctx, account.ID)
		if err != nil {
			return nil, err
		}
		owned := false
		for _, session := range sessions {
			if session.ID == sessionID {
				owned = true
				break
			}
		}
		if !owned {
			return nil, fmt.Errorf("session was not found for the selected account")
		}
		if err := s.auditWebpanel(actor, "SESSION_REVOCATION_REQUESTED_BY_ADMIN", account, map[string]any{"session_id": sessionID}); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.RevokeSession(ctx, sessionID); err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "SESSION_REVOKED_BY_ADMIN", account, map[string]any{"session_id": sessionID}); err != nil {
			return accountMutationAuditWarning(map[string]any{"revoked": true}), nil
		}
		return map[string]any{"revoked": true}, nil
	case "accounts.sessions.revoke_all":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		account, err := s.auth.accounts.store.ByID(ctx, stringValue(payload, "id"))
		if err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "ALL_SESSIONS_REVOCATION_REQUESTED_BY_ADMIN", account, nil); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.RevokeUser(ctx, account.ID); err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "ALL_SESSIONS_REVOKED_BY_ADMIN", account, nil); err != nil {
			return accountMutationAuditWarning(map[string]any{"revoked": true}), nil
		}
		return map[string]any{"revoked": true}, nil
	case "accounts.delete":
		if !actor.Account.IsAdmin {
			return nil, fmt.Errorf("administrator permission is required")
		}
		if subtle.ConstantTimeCompare([]byte(stringValue(payload, "confirmation")), []byte("DELETE")) != 1 {
			return nil, fmt.Errorf("type DELETE to confirm account deletion")
		}
		account, err := s.auth.accounts.store.ByID(ctx, stringValue(payload, "id"))
		if err != nil {
			return nil, err
		}
		if account.ID == actor.Account.ID {
			return nil, fmt.Errorf("the active administrator account cannot delete itself")
		}
		if account.IsAdmin {
			if err := s.requireAnotherAdministrator(ctx, account.ID); err != nil {
				return nil, err
			}
		}
		if err := s.auditWebpanel(actor, "ACCOUNT_DELETE_REQUESTED", account, nil); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.RevokeUser(ctx, account.ID); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.store.Delete(ctx, account.ID); err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "ACCOUNT_DELETED", account, nil); err != nil {
			return accountMutationAuditWarning(map[string]any{"deleted": true}), nil
		}
		return map[string]any{"deleted": true}, nil
	case "profile.get":
		return map[string]any{"account": actor.Account, "session": actor.Session, "password_change_required": actor.PasswordChangeRequired}, nil
	case "profile.sessions":
		sessions, err := s.auth.accounts.ListSessions(ctx, actor.Account.ID)
		if err != nil {
			return nil, err
		}
		return map[string]any{"sessions": sessions}, nil
	case "profile.session.revoke":
		sessionID := stringValue(payload, "session_id")
		sessions, err := s.auth.accounts.ListSessions(ctx, actor.Account.ID)
		if err != nil {
			return nil, err
		}
		owned := false
		for _, session := range sessions {
			if session.ID == sessionID {
				owned = true
				break
			}
		}
		if !owned {
			return nil, fmt.Errorf("session was not found")
		}
		if err := s.auditWebpanel(actor, "SESSION_REVOCATION_REQUESTED_BY_USER", actor.Account, map[string]any{"session_id": sessionID}); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.RevokeSession(ctx, sessionID); err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "SESSION_REVOKED_BY_USER", actor.Account, map[string]any{"session_id": sessionID}); err != nil {
			return accountMutationAuditWarning(map[string]any{"revoked": true}), nil
		}
		return map[string]any{"revoked": true}, nil
	case "profile.password.change":
		if !actor.Account.AllowUserPasswordChange {
			return nil, &AuthError{Code: "password_change_forbidden", Detail: "This account is not allowed to change its password.", HTTPStatus: http.StatusForbidden}
		}
		ok, err := s.auth.accounts.verifyPassword(ctx, actor.Account.passwordHash, stringValue(payload, "current_password"))
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("current password is incorrect")
		}
		hash, err := s.auth.accounts.hashPassword(stringValue(payload, "new_password"))
		if err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "PASSWORD_CHANGE_REQUESTED_BY_USER", actor.Account, nil); err != nil {
			return nil, err
		}
		if err := s.auth.accounts.store.SetPasswordIfAuthVersion(ctx, actor.Account.ID, hash, actor.Account.authVersion); err != nil {
			if errors.Is(err, errAccountVersionChanged) {
				return nil, &AuthError{Code: "session_changed", Detail: "The account changed while the password update was in progress. Sign in again.", HTTPStatus: http.StatusConflict}
			}
			return nil, err
		}
		s.auth.accounts.ResetLoginLimits(ctx, actor.Account.Username, actor.Session.IP)
		if err := s.auth.accounts.RevokeUser(ctx, actor.Account.ID); err != nil {
			return nil, err
		}
		if err := s.auditWebpanel(actor, "PASSWORD_CHANGED_BY_USER", actor.Account, nil); err != nil {
			return accountMutationAuditWarning(map[string]any{"updated": true, "sessions_revoked": true}), nil
		}
		return map[string]any{"updated": true, "sessions_revoked": true}, nil
	default:
		return nil, fmt.Errorf("unsupported account command %q", command)
	}
}

func serializedAccountMutation(command string) bool {
	switch command {
	case "accounts.create", "accounts.update", "accounts.password.reset", "accounts.mfa.reset",
		"accounts.unlock", "accounts.sessions.revoke", "accounts.sessions.revoke_all", "accounts.delete",
		"profile.password.change":
		return true
	default:
		return false
	}
}

func accountMutationAuditWarning(result map[string]any) map[string]any {
	if result == nil {
		result = map[string]any{}
	}
	result["audit_status"] = "completion_record_failed"
	return result
}

func accountPolicyAuditDetails(before Account, after Account) map[string]any {
	previous := accountPolicyAuditView(before)
	updated := accountPolicyAuditView(after)
	changed := make([]string, 0, len(updated))
	for key, value := range updated {
		if fmt.Sprint(previous[key]) != fmt.Sprint(value) {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return map[string]any{
		"renamed_from":    before.Username,
		"changed_fields":  changed,
		"previous_policy": previous,
		"updated_policy":  updated,
	}
}

func accountPolicyAuditView(account Account) map[string]any {
	return map[string]any{
		"username":                  account.Username,
		"sender_id":                 account.SenderID,
		"force_sender_id":           account.ForceSenderID,
		"allow_origination":         account.AllowOrigination,
		"allowed_originators":       account.AllowedOriginators,
		"blocked_event_codes":       account.BlockedEventCodes,
		"force_originator_name":     account.ForceOriginatorName,
		"originator_name_text":      account.OriginatorNameText,
		"include_ip_in_brackets":    account.IncludeIPInBrackets,
		"can_view_logs":             account.CanViewLogs,
		"allow_persistent_sessions": account.AllowPersistentSessions,
		"password_expiry_days":      account.PasswordExpiryDays,
		"allow_user_pw_change":      account.AllowUserPasswordChange,
		"logging_enabled":           account.LoggingEnabled,
		"account_locked":            account.AccountLocked,
		"mfa_enabled":               account.MFAEnabled,
		"is_admin":                  account.IsAdmin,
		"cidr_allowlist":            account.CIDRWhitelist,
	}
}

func accountPolicyFromPayload(account Account, payload map[string]any) Account {
	if value, ok := payload["username"]; ok {
		account.Username = strings.TrimSpace(fmt.Sprint(value))
	}
	if value, ok := payload["sender_id"]; ok {
		account.SenderID = strings.TrimSpace(fmt.Sprint(value))
	}
	account.ForceSenderID = boolPayload(payload, "force_sender_id", account.ForceSenderID)
	account.AllowOrigination = boolPayload(payload, "allow_origination", account.AllowOrigination)
	if value, ok := payload["allowed_originators"]; ok {
		account.AllowedOriginators = stringListAny(value)
	}
	if value, ok := payload["blocked_event_codes"]; ok {
		account.BlockedEventCodes = stringListAny(value)
	}
	account.ForceOriginatorName = boolPayload(payload, "force_originator_name", account.ForceOriginatorName)
	if value, ok := payload["originator_name_text"]; ok {
		account.OriginatorNameText = strings.TrimSpace(fmt.Sprint(value))
	}
	account.IncludeIPInBrackets = boolPayload(payload, "include_ip_in_brackets", account.IncludeIPInBrackets)
	account.CanViewLogs = boolPayload(payload, "can_view_logs", account.CanViewLogs)
	account.AllowPersistentSessions = boolPayload(payload, "allow_persistent_sessions", account.AllowPersistentSessions)
	account.PasswordExpiryDays = intPayload(payload, "password_expiry_days", account.PasswordExpiryDays)
	account.AllowUserPasswordChange = boolPayload(payload, "allow_user_pw_change", account.AllowUserPasswordChange)
	account.LoggingEnabled = boolPayload(payload, "logging_enabled", account.LoggingEnabled)
	account.IsAdmin = boolPayload(payload, "is_admin", account.IsAdmin)
	if value, ok := payload["cidr_allowlist"]; ok {
		account.CIDRWhitelist = stringListAny(value)
	} else if value, ok := payload["cidr_whitelist"]; ok {
		account.CIDRWhitelist = stringListAny(value)
	}
	return account
}

func (s *wsSession) requireAnotherAdministrator(ctx context.Context, excludedID string) error {
	accounts, err := s.auth.accounts.ListAccounts(ctx)
	if err != nil {
		return err
	}
	for _, account := range accounts {
		if account.ID != excludedID && account.IsAdmin && !account.AccountLocked {
			return nil
		}
	}
	return fmt.Errorf("at least one other unlocked administrator account is required")
}

func (s *wsSession) auditWebpanel(actor Identity, action string, target Account, details map[string]any) error {
	if s.auth.accounts.audit == nil || !actor.Account.LoggingEnabled {
		return nil
	}
	if details == nil {
		details = map[string]any{}
	}
	details["target_account_id"] = target.ID
	details["target_username"] = target.Username
	if err := s.auth.accounts.audit.Append("webpanel", AuditEvent{
		Event: action, ActorID: actor.Account.ID, ActorUsername: actor.Account.Username,
		SessionID: actor.Session.ID, IP: actor.Session.IP, UserAgent: actor.Session.UserAgent,
		Details: details,
	}); err != nil {
		return fmt.Errorf("web panel audit integrity failure: %w", err)
	}
	return nil
}

func accountCommandName(command string) bool {
	return strings.HasPrefix(command, "accounts.") || strings.HasPrefix(command, "profile.")
}

func accountCommandNotFound(err error) bool {
	return errors.Is(err, errAccountNotFound)
}
