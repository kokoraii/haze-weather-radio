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
