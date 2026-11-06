package webgateway

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"golang.org/x/crypto/argon2"
)

func TestArgon2IDPasswordHashLogin(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "")
	t.Setenv("ADMIN_PASSWD_HASH", testPHCHash("secret"))
	manager := NewAuthManager(authEnabledConfig())

	token, err := manager.Login("secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if token == "" {
		t.Fatal("token was empty")
	}
	if _, err := manager.Login("wrong"); err == nil {
		t.Fatal("wrong password was accepted")
	}
}

func TestSignedSessionTokenSurvivesAuthManagerRestart(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	t.Setenv("ADMIN_PASSWD_HASH", "")
	config := authEnabledConfig()

	first := NewAuthManager(config)
	token, err := first.Login("secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if !first.ValidToken(token) {
		t.Fatal("fresh token was not valid")
	}

	restarted := NewAuthManager(config)
	if !restarted.ValidToken(token) {
		t.Fatal("signed token did not survive auth manager restart")
	}
}

func TestLogoutRevokesSignedSessionTokenForCurrentProcess(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	t.Setenv("ADMIN_PASSWD_HASH", "")
	manager := NewAuthManager(authEnabledConfig())
	token, err := manager.Login("secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	manager.Logout(token)
	if manager.ValidToken(token) {
		t.Fatal("logged out token remained valid")
	}
}

func TestSignedSessionLoginDoesNotRetainSessionMapEntry(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	t.Setenv("ADMIN_PASSWD_HASH", "")
	manager := NewAuthManager(authEnabledConfig())
	token, err := manager.Login("secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if token == "" {
		t.Fatal("token was empty")
	}
	if len(manager.sessions) != 0 {
		t.Fatalf("signed token was retained in legacy session map: %d", len(manager.sessions))
	}
	manager.sessions["legacy"] = time.Now().Add(-time.Second)
	if !manager.ValidToken(token) {
		t.Fatal("signed token was not valid")
	}
	if len(manager.sessions) != 0 {
		t.Fatalf("expired legacy sessions were not pruned: %d", len(manager.sessions))
	}
}

func testPHCHash(password string) string {
	salt := []byte("1234567890abcdef")
	hash := argon2.IDKey([]byte(password), salt, 1, 64, 1, 32)
	return fmt.Sprintf(
		"$argon2id$v=19$m=64,t=1,p=1$%s$%s",
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	)
}
