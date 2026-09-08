package webgateway

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	apiSecretWeatherScope = "weather"
	apiSecretAudioScope   = "audio"
)

const apiSecretExportSchemaVersion = 1

var validAPISecretName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 _.-]{0,99}$`)

// APISecret is the safe management representation. Its secret material is never returned.
type APISecret struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Prefix     string    `json:"prefix"`
	Scopes     []string  `json:"scopes"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	LastUsedAt time.Time `json:"last_used_at,omitempty"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`
	RevokedAt  time.Time `json:"revoked_at,omitempty"`

	secretHash string
}

type apiSecretExport struct {
	SchemaVersion int                     `json:"schema_version"`
	KeyID         string                  `json:"key_id"`
	ExportedAt    time.Time               `json:"exported_at"`
	Secrets       []apiSecretExportRecord `json:"secrets"`
}

// apiSecretExportRecord deliberately contains only the keyed verifier, never a usable secret.
type apiSecretExportRecord struct {
	Name       string    `json:"name"`
	SecretHash string    `json:"secret_hash"`
	Prefix     string    `json:"prefix"`
	Scopes     []string  `json:"scopes"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	ExpiresAt  time.Time `json:"expires_at,omitempty"`
}

type apiSecretImportChoice struct {
	Index   int    `json:"index"`
	Action  string `json:"action"`
	NewName string `json:"new_name,omitempty"`
	OldName string `json:"old_name,omitempty"`
}

func normalizeAPISecretScopes(values []string) ([]string, error) {
	seen := map[string]bool{}
	for _, value := range values {
		scope := strings.ToLower(strings.TrimSpace(value))
		if scope != apiSecretWeatherScope && scope != apiSecretAudioScope {
			return nil, fmt.Errorf("unsupported API secret scope %q", value)
		}
		seen[scope] = true
	}
	if !seen[apiSecretWeatherScope] {
		return nil, fmt.Errorf("weather scope is required")
	}
	out := make([]string, 0, len(seen))
	for scope := range seen {
		out = append(out, scope)
	}
	sort.Strings(out)
	return out, nil
}

func parseAPISecretExpiry(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	expiresAt, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("expiry must be an RFC3339 timestamp")
	}
	if !expiresAt.After(time.Now().UTC()) {
		return time.Time{}, fmt.Errorf("expiry must be in the future")
	}
	return expiresAt.UTC(), nil
}

func apiSecretFromRow(row interface{ Scan(...any) error }) (APISecret, error) {
	var secret APISecret
	var scopesJSON, createdAt, updatedAt, lastUsedAt, expiresAt, revokedAt string
	if err := row.Scan(&secret.ID, &secret.Name, &secret.secretHash, &secret.Prefix, &scopesJSON, &createdAt, &updatedAt, &lastUsedAt, &expiresAt, &revokedAt); err != nil {
		return APISecret{}, err
	}
	if err := json.Unmarshal([]byte(scopesJSON), &secret.Scopes); err != nil {
		return APISecret{}, fmt.Errorf("decode API secret scopes: %w", err)
	}
	var err error
	if secret.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt); err != nil {
		return APISecret{}, err
	}
	if secret.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt); err != nil {
		return APISecret{}, err
	}
	for _, item := range []struct {
		raw    string
		target *time.Time
	}{{lastUsedAt, &secret.LastUsedAt}, {expiresAt, &secret.ExpiresAt}, {revokedAt, &secret.RevokedAt}} {
		if item.raw != "" {
			if *item.target, err = time.Parse(time.RFC3339Nano, item.raw); err != nil {
				return APISecret{}, err
			}
		}
	}
	return secret, nil
}

func (s *accountStore) ListAPISecrets(ctx context.Context) ([]APISecret, error) {
	rows, err := s.db.QueryContext(ctx, s.bind(`SELECT id, name, secret_hash, secret_prefix, scopes, created_at, updated_at, COALESCE(last_used_at, ''), COALESCE(expires_at, ''), COALESCE(revoked_at, '') FROM api_secrets ORDER BY created_at DESC`))
	if err != nil {
		return nil, fmt.Errorf("list API secrets: %w", err)
	}
	defer rows.Close()
	secrets := []APISecret{}
	for rows.Next() {
		secret, err := apiSecretFromRow(rows)
		if err != nil {
			return nil, fmt.Errorf("read API secret: %w", err)
		}
		secrets = append(secrets, secret)
	}
	return secrets, rows.Err()
}

func (s *accountStore) CreateAPISecret(ctx context.Context, secret APISecret, actorID string) error {
	scopes, err := json.Marshal(secret.Scopes)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, s.bind(`INSERT INTO api_secrets (id, name, secret_hash, secret_prefix, scopes, created_by_user_id, created_at, updated_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`), secret.ID, secret.Name, secret.secretHash, secret.Prefix, string(scopes), actorID, secret.CreatedAt.Format(time.RFC3339Nano), secret.UpdatedAt.Format(time.RFC3339Nano), nullableTime(secret.ExpiresAt))
	if err != nil {
		return fmt.Errorf("create API secret: %w", err)
	}
	return nil
}

func (s *accountStore) UpdateAPISecret(ctx context.Context, secret APISecret) error {
	scopes, err := json.Marshal(secret.Scopes)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, s.bind(`UPDATE api_secrets SET name = ?, scopes = ?, updated_at = ?, expires_at = ? WHERE id = ? AND revoked_at IS NULL`), secret.Name, string(scopes), secret.UpdatedAt.Format(time.RFC3339Nano), nullableTime(secret.ExpiresAt), secret.ID)
	if err != nil {
		return fmt.Errorf("update API secret: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errAccountNotFound
	}
	return nil
}

func (s *accountStore) RevokeAPISecret(ctx context.Context, id string, now time.Time) error {
	result, err := s.db.ExecContext(ctx, s.bind(`UPDATE api_secrets SET revoked_at = ?, updated_at = ? WHERE id = ? AND revoked_at IS NULL`), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), id)
	if err != nil {
		return fmt.Errorf("revoke API secret: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errAccountNotFound
	}
	return nil
}

func (s *accountStore) APISecretByHash(ctx context.Context, digest string) (APISecret, error) {
	row := s.db.QueryRowContext(ctx, s.bind(`SELECT id, name, secret_hash, secret_prefix, scopes, created_at, updated_at, COALESCE(last_used_at, ''), COALESCE(expires_at, ''), COALESCE(revoked_at, '') FROM api_secrets WHERE secret_hash = ?`), digest)
	secret, err := apiSecretFromRow(row)
	if err != nil {
		return APISecret{}, err
	}
	return secret, nil
}

func (s *accountStore) MarkAPISecretUsed(ctx context.Context, id string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, s.bind(`UPDATE api_secrets SET last_used_at = ? WHERE id = ?`), now.Format(time.RFC3339Nano), id)
	return err
}

func (s *accountStore) importAPISecret(ctx context.Context, secret apiSecretExportRecord, actorID string) error {
	scopes, err := json.Marshal(secret.Scopes)
	if err != nil {
		return err
	}
	id, err := randomUUID()
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, s.bind(`INSERT INTO api_secrets (id, name, secret_hash, secret_prefix, scopes, created_by_user_id, created_at, updated_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`), id, secret.Name, secret.SecretHash, secret.Prefix, string(scopes), actorID, secret.CreatedAt.Format(time.RFC3339Nano), secret.UpdatedAt.Format(time.RFC3339Nano), nullableTime(secret.ExpiresAt))
	if err != nil {
		return fmt.Errorf("import API secret: %w", err)
	}
	return nil
}

func (s *accountStore) replaceAPISecret(ctx context.Context, id string, secret apiSecretExportRecord) error {
	scopes, err := json.Marshal(secret.Scopes)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, s.bind(`UPDATE api_secrets SET name = ?, secret_hash = ?, secret_prefix = ?, scopes = ?, updated_at = ?, expires_at = ?, revoked_at = NULL WHERE id = ?`), secret.Name, secret.SecretHash, secret.Prefix, string(scopes), secret.UpdatedAt.Format(time.RFC3339Nano), nullableTime(secret.ExpiresAt), id)
	if err != nil {
		return fmt.Errorf("overwrite API secret: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errAccountNotFound
	}
	return nil
}

func (s *accountStore) renameAPISecret(ctx context.Context, id string, name string) error {
	_, err := s.db.ExecContext(ctx, s.bind(`UPDATE api_secrets SET name = ?, updated_at = ? WHERE id = ?`), name, time.Now().UTC().Format(time.RFC3339Nano), id)
	return err
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.Format(time.RFC3339Nano)
}

func (h *accountAuth) apiSecretHash(secret string) string {
	mac := hmac.New(sha256.New, h.pepper)
	_, _ = mac.Write([]byte(secret))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (h *accountAuth) apiSecretExportKeyID() string {
	sum := sha256.Sum256(h.pepper)
	return "haze-wx-" + hex.EncodeToString(sum[:8])
}

func (h *accountAuth) CreateAPISecret(ctx context.Context, actorID string, name string, scopes []string, expiresAt time.Time) (APISecret, string, error) {
	name = strings.TrimSpace(name)
	if !validAPISecretName.MatchString(name) {
		return APISecret{}, "", fmt.Errorf("API secret name must be 1 to 100 letters, numbers, spaces, dots, underscores, or hyphens")
	}
	scopes, err := normalizeAPISecretScopes(scopes)
	if err != nil {
		return APISecret{}, "", err
	}
	id, err := randomUUID()
	if err != nil {
		return APISecret{}, "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return APISecret{}, "", fmt.Errorf("generate API secret: %w", err)
	}
	plain := "haze_wx_" + base64.RawURLEncoding.EncodeToString(raw)
	now := time.Now().UTC()
	secret := APISecret{ID: id, Name: name, Prefix: plain[:16], Scopes: scopes, CreatedAt: now, UpdatedAt: now, ExpiresAt: expiresAt, secretHash: h.apiSecretHash(plain)}
	if err := h.store.CreateAPISecret(ctx, secret, actorID); err != nil {
		return APISecret{}, "", err
	}
	return secret, plain, nil
}

func (h *accountAuth) AuthenticateAPISecret(ctx context.Context, request *http.Request) (APISecret, error) {
	if !h.secureRequest(request) {
		return APISecret{}, &AuthError{Code: "https_required", Detail: "This endpoint requires HTTPS.", HTTPStatus: http.StatusUpgradeRequired}
	}
	header := strings.TrimSpace(request.Header.Get("Authorization"))
	if !strings.HasPrefix(strings.ToLower(header), "bearer ") {
		return APISecret{}, &AuthError{Code: "unauthorized", Detail: "Authentication is required.", HTTPStatus: http.StatusUnauthorized}
	}
	plain := strings.TrimSpace(header[len("Bearer "):])
	if !strings.HasPrefix(plain, "haze_wx_") {
		return APISecret{}, &AuthError{Code: "unauthorized", Detail: "Authentication is required.", HTTPStatus: http.StatusUnauthorized}
	}
	secret, err := h.store.APISecretByHash(ctx, h.apiSecretHash(plain))
	if err != nil {
		return APISecret{}, &AuthError{Code: "unauthorized", Detail: "Authentication is required.", HTTPStatus: http.StatusUnauthorized}
	}
	if secret.RevokedAt.IsZero() == false || (!secret.ExpiresAt.IsZero() && !secret.ExpiresAt.After(time.Now().UTC())) {
		return APISecret{}, &AuthError{Code: "unauthorized", Detail: "Authentication is required.", HTTPStatus: http.StatusUnauthorized}
	}
	if hmac.Equal([]byte(secret.secretHash), []byte(h.apiSecretHash(plain))) == false {
		return APISecret{}, &AuthError{Code: "unauthorized", Detail: "Authentication is required.", HTTPStatus: http.StatusUnauthorized}
	}
	if !h.apiSecretLimiter.Allow(secret.ID, 2, time.Second, time.Now().UTC()) {
		return APISecret{}, &AuthError{Code: "api_rate_limited", Detail: "This API secret is limited to two requests per second.", HTTPStatus: http.StatusTooManyRequests}
	}
	_ = h.store.MarkAPISecretUsed(ctx, secret.ID, time.Now().UTC())
	return secret, nil
}

func (s *wsSession) wxAPISecretsActor() (Identity, error) {
	if s == nil || s.auth == nil || s.auth.accounts == nil {
		return Identity{}, fmt.Errorf("API secret management requires account authentication")
	}
	identity, err := s.auth.Identity(s.request)
	if err != nil {
		return Identity{}, err
	}
	if !identity.Account.IsAdmin {
		return Identity{}, &AuthError{Code: "administrator_required", Detail: "Administrator permission is required.", HTTPStatus: http.StatusForbidden}
	}
	return identity, nil
}

func (s *wsSession) wxAPISecretsList(_ map[string]any) (any, error) {
	if _, err := s.wxAPISecretsActor(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.request.Context(), 5*time.Second)
	defer cancel()
	secrets, err := s.auth.accounts.store.ListAPISecrets(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"secrets": secrets}, nil
}

func (s *wsSession) wxAPISecretsCreate(payload map[string]any) (any, error) {
	actor, err := s.wxAPISecretsActor()
	if err != nil {
		return nil, err
	}
	expiresAt, err := parseAPISecretExpiry(stringValue(payload, "expires_at"))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.request.Context(), 5*time.Second)
	defer cancel()
	secret, plain, err := s.auth.accounts.CreateAPISecret(ctx, actor.Account.ID, stringValue(payload, "name"), stringListAny(payload["scopes"]), expiresAt)
	if err != nil {
		return nil, err
	}
	if err := s.auditWebpanel(actor, "WX_API_SECRET_CREATED", actor.Account, map[string]any{"secret_id": secret.ID, "name": secret.Name, "scopes": secret.Scopes, "expires_at": nullableTime(secret.ExpiresAt)}); err != nil {
		return nil, err
	}
	return map[string]any{"secret": secret, "value": plain}, nil
}

func (s *wsSession) wxAPISecretsUpdate(payload map[string]any) (any, error) {
	actor, err := s.wxAPISecretsActor()
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(stringValue(payload, "name"))
	if !validAPISecretName.MatchString(name) {
		return nil, fmt.Errorf("API secret name must be 1 to 100 letters, numbers, spaces, dots, underscores, or hyphens")
	}
	scopes, err := normalizeAPISecretScopes(stringListAny(payload["scopes"]))
	if err != nil {
		return nil, err
	}
	expiresAt, err := parseAPISecretExpiry(stringValue(payload, "expires_at"))
	if err != nil {
		return nil, err
	}
	secret := APISecret{ID: strings.TrimSpace(stringValue(payload, "id")), Name: name, Scopes: scopes, UpdatedAt: time.Now().UTC(), ExpiresAt: expiresAt}
	if secret.ID == "" {
		return nil, fmt.Errorf("API secret ID is required")
	}
	ctx, cancel := context.WithTimeout(s.request.Context(), 5*time.Second)
	defer cancel()
	if err := s.auth.accounts.store.UpdateAPISecret(ctx, secret); err != nil {
		return nil, err
	}
	if err := s.auditWebpanel(actor, "WX_API_SECRET_UPDATED", actor.Account, map[string]any{"secret_id": secret.ID, "name": secret.Name, "scopes": secret.Scopes, "expires_at": nullableTime(secret.ExpiresAt)}); err != nil {
		return nil, err
	}
	return map[string]any{"updated": true}, nil
}

func (s *wsSession) wxAPISecretsRevoke(payload map[string]any) (any, error) {
	actor, err := s.wxAPISecretsActor()
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(stringValue(payload, "id"))
	if id == "" {
		return nil, fmt.Errorf("API secret ID is required")
	}
	ctx, cancel := context.WithTimeout(s.request.Context(), 5*time.Second)
	defer cancel()
	if err := s.auth.accounts.store.RevokeAPISecret(ctx, id, time.Now().UTC()); err != nil {
		return nil, err
	}
	if err := s.auditWebpanel(actor, "WX_API_SECRET_REVOKED", actor.Account, map[string]any{"secret_id": id}); err != nil {
		return nil, err
	}
	return map[string]any{"revoked": true}, nil
}

func (s *wsSession) wxAPISecretsExport(_ map[string]any) (any, error) {
	if _, err := s.wxAPISecretsActor(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.request.Context(), 5*time.Second)
	defer cancel()
	secrets, err := s.auth.accounts.store.ListAPISecrets(ctx)
	if err != nil {
		return nil, err
	}
	document := apiSecretExport{SchemaVersion: apiSecretExportSchemaVersion, KeyID: s.auth.accounts.apiSecretExportKeyID(), ExportedAt: time.Now().UTC(), Secrets: make([]apiSecretExportRecord, 0, len(secrets))}
	for _, secret := range secrets {
		if !secret.RevokedAt.IsZero() {
			continue
		}
		document.Secrets = append(document.Secrets, apiSecretExportRecord{Name: secret.Name, SecretHash: secret.secretHash, Prefix: secret.Prefix, Scopes: secret.Scopes, CreatedAt: secret.CreatedAt, UpdatedAt: secret.UpdatedAt, ExpiresAt: secret.ExpiresAt})
	}
	return map[string]any{"backup": document}, nil
}

func parseAPISecretExport(raw any, keyID string) (apiSecretExport, error) {
	encoded, err := json.Marshal(raw)
	if err != nil {
		return apiSecretExport{}, fmt.Errorf("encode API secret backup: %w", err)
	}
	if len(encoded) > 512*1024 {
		return apiSecretExport{}, fmt.Errorf("API secret backup is too large")
	}
	var document apiSecretExport
	if err := json.Unmarshal(encoded, &document); err != nil {
		return apiSecretExport{}, fmt.Errorf("invalid API secret backup")
	}
	if document.SchemaVersion != apiSecretExportSchemaVersion {
		return apiSecretExport{}, fmt.Errorf("unsupported API secret backup version")
	}
	if document.KeyID != keyID {
		return apiSecretExport{}, fmt.Errorf("this backup was created for a different Haze secret key")
	}
	if len(document.Secrets) > 1000 {
		return apiSecretExport{}, fmt.Errorf("API secret backup contains too many records")
	}
	seenNames := map[string]bool{}
	seenHashes := map[string]bool{}
	for index := range document.Secrets {
		secret := &document.Secrets[index]
		secret.Name = strings.TrimSpace(secret.Name)
		if !validAPISecretName.MatchString(secret.Name) || len(secret.SecretHash) < 32 || !strings.HasPrefix(secret.Prefix, "haze_wx_") {
			return apiSecretExport{}, fmt.Errorf("API secret backup record %d is invalid", index+1)
		}
		var err error
		if secret.Scopes, err = normalizeAPISecretScopes(secret.Scopes); err != nil {
			return apiSecretExport{}, err
		}
		if secret.CreatedAt.IsZero() || secret.UpdatedAt.IsZero() {
			return apiSecretExport{}, fmt.Errorf("API secret backup record %d is missing timestamps", index+1)
		}
		if seenNames[strings.ToLower(secret.Name)] || seenHashes[secret.SecretHash] {
			return apiSecretExport{}, fmt.Errorf("API secret backup contains duplicate records")
		}
		seenNames[strings.ToLower(secret.Name)] = true
		seenHashes[secret.SecretHash] = true
	}
	return document, nil
}

func apiSecretConflict(secret apiSecretExportRecord, existing []APISecret) (APISecret, string) {
	for _, candidate := range existing {
		if strings.EqualFold(candidate.Name, secret.Name) {
			return candidate, "name"
		}
		if candidate.secretHash == secret.SecretHash {
			return candidate, "secret"
		}
	}
	return APISecret{}, ""
}

func (s *wsSession) wxAPISecretsImportPreview(payload map[string]any) (any, error) {
	if _, err := s.wxAPISecretsActor(); err != nil {
		return nil, err
	}
	document, err := parseAPISecretExport(payload["backup"], s.auth.accounts.apiSecretExportKeyID())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(s.request.Context(), 5*time.Second)
	defer cancel()
	existing, err := s.auth.accounts.store.ListAPISecrets(ctx)
	if err != nil {
		return nil, err
	}
	entries := make([]map[string]any, 0, len(document.Secrets))
	for index, incoming := range document.Secrets {
		conflict, kind := apiSecretConflict(incoming, existing)
		entry := map[string]any{"index": index, "name": incoming.Name, "scopes": incoming.Scopes, "conflict": kind != "", "conflict_kind": kind}
		if kind != "" {
			entry["existing"] = map[string]any{"id": conflict.ID, "name": conflict.Name, "prefix": conflict.Prefix}
		}
		entries = append(entries, entry)
	}
	return map[string]any{"entries": entries}, nil
}

func (s *wsSession) wxAPISecretsImport(payload map[string]any) (any, error) {
	actor, err := s.wxAPISecretsActor()
	if err != nil {
		return nil, err
	}
	document, err := parseAPISecretExport(payload["backup"], s.auth.accounts.apiSecretExportKeyID())
	if err != nil {
		return nil, err
	}
	choices := map[int]apiSecretImportChoice{}
	choicesRaw, _ := payload["choices"].([]any)
	for _, raw := range choicesRaw {
		encoded, _ := json.Marshal(raw)
		var choice apiSecretImportChoice
		if json.Unmarshal(encoded, &choice) == nil {
			choices[choice.Index] = choice
		}
	}
	ctx, cancel := context.WithTimeout(s.request.Context(), 10*time.Second)
	defer cancel()
	existing, err := s.auth.accounts.store.ListAPISecrets(ctx)
	if err != nil {
		return nil, err
	}
	imported, skipped := 0, 0
	for index, incoming := range document.Secrets {
		conflict, kind := apiSecretConflict(incoming, existing)
		if kind == "" {
			if err := s.auth.accounts.store.importAPISecret(ctx, incoming, actor.Account.ID); err != nil {
				return nil, err
			}
			imported++
			continue
		}
		choice, ok := choices[index]
		if !ok {
			return nil, fmt.Errorf("choose how to resolve the conflict for %q", incoming.Name)
		}
		switch choice.Action {
		case "keep":
			skipped++
		case "overwrite":
			if err := s.auth.accounts.store.replaceAPISecret(ctx, conflict.ID, incoming); err != nil {
				return nil, err
			}
			imported++
		case "rename_new":
			if kind == "secret" {
				return nil, fmt.Errorf("a duplicate secret cannot be kept twice")
			}
			incoming.Name = strings.TrimSpace(choice.NewName)
			if !validAPISecretName.MatchString(incoming.Name) {
				return nil, fmt.Errorf("new API secret name is invalid")
			}
			if _, conflictKind := apiSecretConflict(incoming, existing); conflictKind != "" {
				return nil, fmt.Errorf("new API secret name conflicts with an existing secret")
			}
			if err := s.auth.accounts.store.importAPISecret(ctx, incoming, actor.Account.ID); err != nil {
				return nil, err
			}
			imported++
		case "rename_old":
			if kind == "secret" {
				return nil, fmt.Errorf("a duplicate secret cannot be kept twice")
			}
			oldName := strings.TrimSpace(choice.OldName)
			if !validAPISecretName.MatchString(oldName) {
				return nil, fmt.Errorf("existing API secret name is invalid")
			}
			if err := s.auth.accounts.store.renameAPISecret(ctx, conflict.ID, oldName); err != nil {
				return nil, err
			}
			if err := s.auth.accounts.store.importAPISecret(ctx, incoming, actor.Account.ID); err != nil {
				return nil, err
			}
			imported++
		default:
			return nil, fmt.Errorf("invalid conflict action")
		}
	}
	if err := s.auditWebpanel(actor, "WX_API_SECRETS_IMPORTED", actor.Account, map[string]any{"imported": imported, "skipped": skipped}); err != nil {
		return nil, err
	}
	return map[string]any{"imported": imported, "skipped": skipped}, nil
}
