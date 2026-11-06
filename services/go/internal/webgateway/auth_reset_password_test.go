package webgateway

import (
	"context"
	"strings"
	"testing"
)

func TestResetAccountPasswordsMatchesWildcardAndRevokesOldPassword(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	for _, username := range []string{"ops.one", "ops.two", "viewer"} {
		createTestAccount(t, manager, Account{
			Username: username, PasswordExpiryDays: 90, AllowUserPasswordChange: true, LoggingEnabled: true,
		}, "original password 123")
	}

	resets, err := manager.ResetAccountPasswords(context.Background(), "OPS.*")
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if len(resets) != 2 {
		t.Fatalf("resets = %#v", resets)
	}
	seen := map[string]bool{}
	for _, reset := range resets {
		if len(reset.Password) != generatedPasswordLength {
			t.Fatalf("password length for %s = %d", reset.Username, len(reset.Password))
		}
		if strings.Trim(reset.Password, generatedPasswordAlphabet) != "" {
			t.Fatalf("password for %s has characters outside the alphabet", reset.Username)
		}
		seen[reset.Username] = true
		if _, err := manager.LoginWithRequest(context.Background(), LoginInput{
			Username: reset.Username, Password: reset.Password, Request: testLoginRequest("192.0.2.30:5000"),
		}); err != nil {
			t.Fatalf("login with reset password for %s: %v", reset.Username, err)
		}
		if _, err := manager.LoginWithRequest(context.Background(), LoginInput{
			Username: reset.Username, Password: "original password 123", Request: testLoginRequest("192.0.2.31:5000"),
		}); err == nil {
			t.Fatalf("old password still works for %s", reset.Username)
		}
	}
	if !seen["ops.one"] || !seen["ops.two"] || seen["viewer"] {
		t.Fatalf("unexpected accounts reset: %#v", seen)
	}
	if resets[0].Password == resets[1].Password {
		t.Fatal("accounts shared a generated password")
	}
	if _, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "viewer", Password: "original password 123", Request: testLoginRequest("192.0.2.32:5000"),
	}); err != nil {
		t.Fatalf("unmatched account lost its password: %v", err)
	}
}

func TestResetAccountPasswordsForceChangeAtNextSignIn(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	createTestAccount(t, manager, Account{
		Username: "ops", PasswordExpiryDays: 90, AllowUserPasswordChange: true, LoggingEnabled: true,
	}, "original password 123")
	createTestAccount(t, manager, Account{
		Username: "kiosk", PasswordExpiryDays: 90, AllowUserPasswordChange: false, LoggingEnabled: true,
	}, "original password 123")

	resets, err := manager.ResetAccountPasswords(context.Background(), "*o*")
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	passwords := map[string]string{}
	for _, reset := range resets {
		passwords[reset.Username] = reset.Password
	}
	login := func(username, password, addr string) LoginResult {
		t.Helper()
		result, err := manager.LoginWithRequest(context.Background(), LoginInput{
			Username: username, Password: password, Request: testLoginRequest(addr),
		})
		if err != nil {
			t.Fatalf("login %s: %v", username, err)
		}
		return result
	}
	if !login("ops", passwords["ops"], "192.0.2.40:5000").PasswordChangeRequired {
		t.Fatal("reset password did not require a change")
	}
	if login("kiosk", passwords["kiosk"], "192.0.2.41:5000").PasswordChangeRequired {
		t.Fatal("account barred from self-service changes was forced to change")
	}

	account, err := manager.accounts.store.ByUsername(context.Background(), "ops")
	if err != nil {
		t.Fatalf("load account: %v", err)
	}
	hash, err := manager.accounts.hashPassword("chosen password 456")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if err := manager.accounts.store.SetPasswordIfAuthVersion(context.Background(), account.ID, hash, account.authVersion); err != nil {
		t.Fatalf("self-service change: %v", err)
	}
	if login("ops", "chosen password 456", "192.0.2.42:5000").PasswordChangeRequired {
		t.Fatal("self-chosen password still required a change")
	}
}

func TestResetAccountPasswordsWildcardCoversEveryAccount(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	createTestAccount(t, manager, Account{Username: "viewer", PasswordExpiryDays: 90, LoggingEnabled: true}, "original password 123")

	resets, err := manager.ResetAccountPasswords(context.Background(), "*")
	if err != nil {
		t.Fatalf("reset: %v", err)
	}
	if len(resets) != 2 {
		t.Fatalf("resets = %#v", resets)
	}
}

func TestResetAccountPasswordsRejectsUnmatchedAndEmptyPatterns(t *testing.T) {
	manager, _ := newTestAccountAuthManager(t, false)
	defer manager.Close()
	if _, err := manager.ResetAccountPasswords(context.Background(), "nobody*"); err == nil {
		t.Fatal("pattern with no matches succeeded")
	}
	if _, err := manager.ResetAccountPasswords(context.Background(), "  "); err == nil {
		t.Fatal("empty pattern succeeded")
	}
	if _, err := manager.LoginWithRequest(context.Background(), LoginInput{
		Username: "admin", Password: "correct horse battery staple", Request: testLoginRequest("192.0.2.33:5000"),
	}); err != nil {
		t.Fatalf("rejected reset changed the administrator password: %v", err)
	}
}

func TestUsernamePatternTreatsOnlyStarAndQuestionMarkAsWildcards(t *testing.T) {
	cases := []struct {
		pattern  string
		username string
		want     bool
	}{
		{"ops.one", "ops.one", true},
		{"ops.one", "opsXone", false},
		{"ops?one", "ops.one", true},
		{"OPS*", "ops.two", true},
		{"ops", "ops.two", false},
		{"[ab]", "a", false},
	}
	for _, tc := range cases {
		matcher, err := compileUsernamePattern(tc.pattern)
		if err != nil {
			t.Fatalf("compile %q: %v", tc.pattern, err)
		}
		if got := matcher.MatchString(tc.username); got != tc.want {
			t.Errorf("pattern %q against %q = %v, want %v", tc.pattern, tc.username, got, tc.want)
		}
	}
}
