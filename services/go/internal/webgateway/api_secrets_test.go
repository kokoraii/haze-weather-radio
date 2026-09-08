package webgateway

import (
	"bytes"
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestAPISecretLifecycleDoesNotRetainPlaintext(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	store, err := openAccountStore(context.Background(), Config{}, configPath)
	if err != nil {
		t.Fatalf("open account store: %v", err)
	}
	defer store.Close()
	auth := &accountAuth{store: store, pepper: bytes.Repeat([]byte{7}, 32), apiSecretLimiter: newAttemptLimiter()}
	created, plain, err := auth.CreateAPISecret(context.Background(), "Weather dashboard", "operator-id", []string{"weather", "audio"}, time.Time{})
	if err != nil {
		t.Fatalf("create API secret: %v", err)
	}
	if created.secretHash == plain || created.secretHash == "" {
		t.Fatal("plaintext API secret was retained")
	}
	request := httptest.NewRequest("POST", "https://example.test/api/v1/wx-on-demand/generate", nil)
	request.Header.Set("Authorization", "Bearer "+plain)
	authenticated, err := auth.AuthenticateAPISecret(context.Background(), request)
	if err != nil {
		t.Fatalf("authenticate API secret: %v", err)
	}
	if authenticated.ID != created.ID || len(authenticated.Scopes) != 2 {
		t.Fatalf("unexpected authenticated API secret: %#v", authenticated)
	}
	if err := store.RevokeAPISecret(context.Background(), created.ID, time.Now().UTC()); err != nil {
		t.Fatalf("revoke API secret: %v", err)
	}
	if _, err := auth.AuthenticateAPISecret(context.Background(), request); err == nil {
		t.Fatal("revoked API secret was accepted")
	}
}

func TestAPISecretScopesRequireWeather(t *testing.T) {
	if _, err := normalizeAPISecretScopes([]string{"audio"}); err == nil {
		t.Fatal("audio-only API secret was accepted")
	}
}

func TestParseAPISecretExportRequiresMatchingKeyAndUniqueRecords(t *testing.T) {
	now := time.Now().UTC()
	backup := map[string]any{
		"schema_version": apiSecretExportSchemaVersion,
		"key_id":         "haze-wx-test",
		"exported_at":    now.Format(time.RFC3339Nano),
		"secrets": []any{map[string]any{
			"name":        "Weather dashboard",
			"secret_hash": "abcdefghijklmnoabcdefghijklmnoabcdefgh",
			"prefix":      "haze_wx_example",
			"scopes":      []any{"weather"},
			"created_at":  now.Format(time.RFC3339Nano),
			"updated_at":  now.Format(time.RFC3339Nano),
		}},
	}
	parsed, err := parseAPISecretExport(backup, "haze-wx-test")
	if err != nil || len(parsed.Secrets) != 1 {
		t.Fatalf("parse API secret export: %#v, %v", parsed, err)
	}
	if _, err := parseAPISecretExport(backup, "haze-wx-other"); err == nil {
		t.Fatal("backup with another key ID was accepted")
	}
}
