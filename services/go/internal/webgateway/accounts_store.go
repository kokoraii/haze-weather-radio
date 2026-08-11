package webgateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

var (
	errAccountNotFound       = errors.New("account not found")
	errAccountVersionChanged = errors.New("account authentication state changed")
	validUsername            = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,50}$`)
	validSenderID            = regexp.MustCompile(`^[A-Z0-9/]{8}$`)
	validEventCode           = regexp.MustCompile(`^[A-Z0-9]{3}$`)
)

var allowedOriginatorCodes = map[string]struct{}{
	"CIV": {},
	"EAS": {},
	"PEP": {},
	"WXR": {},
}

// Account is an authenticated Haze operator and its origination policy.
type Account struct {
	ID                      string    `json:"id"`
	Username                string    `json:"username"`
	SenderID                string    `json:"sender_id,omitempty"`
	ForceSenderID           bool      `json:"force_sender_id"`
	LastIP                  string    `json:"last_ip,omitempty"`
	LastLoginAt             time.Time `json:"last_login_at,omitempty"`
	PasswordChangedAt       time.Time `json:"password_changed_at"`
	AllowOrigination        bool      `json:"allow_origination"`
	AllowedOriginators      []string  `json:"allowed_originators"`
	BlockedEventCodes       []string  `json:"blocked_event_codes"`
	ForceOriginatorName     bool      `json:"force_originator_name"`
	OriginatorNameText      string    `json:"originator_name_text,omitempty"`
	IncludeIPInBrackets     bool      `json:"include_ip_in_brackets"`
	CanViewLogs             bool      `json:"can_view_logs"`
	AllowPersistentSessions bool      `json:"allow_persistent_sessions"`
	PasswordExpiryDays      int       `json:"password_expiry_days"`
	AllowUserPasswordChange bool      `json:"allow_user_pw_change"`
	LoggingEnabled          bool      `json:"logging_enabled"`
	AccountLocked           bool      `json:"account_locked"`
	FailedLoginAttempts     int       `json:"failed_login_attempts"`
	MFAEnabled              bool      `json:"mfa_enabled"`
	MFAConfigured           bool      `json:"mfa_configured"`
	IsAdmin                 bool      `json:"is_admin"`
	CIDRWhitelist           []string  `json:"cidr_allowlist"`
	CreatedAt               time.Time `json:"created_at"`
	UpdatedAt               time.Time `json:"updated_at"`

	passwordHash               string
	mfaSecret                  string
	mfaLastStep                int64
	authVersion                int64
	failedLoginWindowStartedAt time.Time
}

type accountStore struct {
	db      *sql.DB
	dialect string
}

const sqliteAccountsSchema = `
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE COLLATE NOCASE,
    password_hash TEXT NOT NULL,
    sender_id TEXT,
    force_sender_id INTEGER NOT NULL DEFAULT 0,
    last_ip TEXT,
    last_login_at TEXT,
    password_changed_at TEXT NOT NULL,
    allow_origination INTEGER NOT NULL DEFAULT 0,
    allowed_originators TEXT NOT NULL DEFAULT '[]',
    blocked_event_codes TEXT NOT NULL DEFAULT '[]',
    force_originator_name INTEGER NOT NULL DEFAULT 0,
    originator_name_text TEXT,
    include_ip_in_brackets INTEGER NOT NULL DEFAULT 0,
    can_view_logs INTEGER NOT NULL DEFAULT 0,
    allow_persistent_sessions INTEGER NOT NULL DEFAULT 0,
    password_expiry_days INTEGER NOT NULL DEFAULT 90,
    allow_user_pw_change INTEGER NOT NULL DEFAULT 1,
    logging_enabled INTEGER NOT NULL DEFAULT 1,
    account_locked INTEGER NOT NULL DEFAULT 0,
    failed_login_attempts INTEGER NOT NULL DEFAULT 0,
	failed_login_window_started_at TEXT,
	mfa_secret TEXT,
	mfa_enabled INTEGER NOT NULL DEFAULT 0,
	mfa_last_used_step INTEGER NOT NULL DEFAULT 0,
	auth_version INTEGER NOT NULL DEFAULT 1,
	is_admin INTEGER NOT NULL DEFAULT 0,
    cidr_whitelist TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_ci ON users(lower(username));
`

const postgresAccountsSchema = `
CREATE TABLE IF NOT EXISTS users (
    id uuid PRIMARY KEY,
    username varchar(50) NOT NULL UNIQUE,
    password_hash varchar(255) NOT NULL,
    sender_id varchar(8),
    force_sender_id boolean NOT NULL DEFAULT false,
    last_ip varchar(45),
    last_login_at text,
    password_changed_at text NOT NULL,
    allow_origination boolean NOT NULL DEFAULT false,
    allowed_originators jsonb NOT NULL DEFAULT '[]'::jsonb,
    blocked_event_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
    force_originator_name boolean NOT NULL DEFAULT false,
    originator_name_text varchar(100),
    include_ip_in_brackets boolean NOT NULL DEFAULT false,
    can_view_logs boolean NOT NULL DEFAULT false,
    allow_persistent_sessions boolean NOT NULL DEFAULT false,
    password_expiry_days integer NOT NULL DEFAULT 90,
    allow_user_pw_change boolean NOT NULL DEFAULT true,
    logging_enabled boolean NOT NULL DEFAULT true,
    account_locked boolean NOT NULL DEFAULT false,
    failed_login_attempts integer NOT NULL DEFAULT 0,
	failed_login_window_started_at text,
	mfa_secret text,
	mfa_enabled boolean NOT NULL DEFAULT false,
	mfa_last_used_step bigint NOT NULL DEFAULT 0,
	auth_version bigint NOT NULL DEFAULT 1,
	is_admin boolean NOT NULL DEFAULT false,
    cidr_whitelist jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at text NOT NULL,
    updated_at text NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_ci ON users(lower(username));
`

func openAccountStore(ctx context.Context, config Config, configPath string) (*accountStore, error) {
	baseDir := filepath.Dir(filepath.Clean(configPath))
	var (
		db          *sql.DB
		dialect     string
		err         error
		maxOpen     = 4
		autoMigrate = true
	)
	if config.Storage.Postgres.Enabled {
		envName := strings.TrimSpace(config.Storage.Postgres.DSNEnv)
		if envName == "" {
			envName = "HAZE_POSTGRES_DSN"
		}
		dsn := strings.TrimSpace(config.Storage.Postgres.DSN)
		if dsn == "" {
			dsn = strings.TrimSpace(os.Getenv(envName))
		}
		if dsn == "" {
			return nil, fmt.Errorf("account store PostgreSQL DSN is not configured")
		}
		db, err = sql.Open("pgx", dsn)
		dialect = "postgres"
		if config.Storage.Postgres.MaxConns > 0 {
			maxOpen = int(config.Storage.Postgres.MaxConns)
		}
		autoMigrate = config.Storage.Postgres.AutoMigrate == nil || *config.Storage.Postgres.AutoMigrate
	} else {
		path := strings.TrimSpace(config.Storage.SQLite.Path)
		if path == "" {
			path = filepath.Join("runtime", "state", "haze.db")
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(baseDir, path)
		}
		path = filepath.Clean(path)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, fmt.Errorf("create account store directory: %w", err)
		}
		busyTimeout := 30 * time.Second
		if parsed, parseErr := time.ParseDuration(strings.TrimSpace(config.Storage.SQLite.BusyTimeout)); parseErr == nil && parsed > 0 {
			busyTimeout = parsed
		}
		values := url.Values{}
		values.Add("_pragma", fmt.Sprintf("busy_timeout=%d", busyTimeout.Milliseconds()))
		values.Add("_pragma", "journal_mode(WAL)")
		values.Add("_pragma", "synchronous(FULL)")
		values.Add("_pragma", "foreign_keys(ON)")
		db, err = sql.Open("sqlite", path+"?"+values.Encode())
		dialect = "sqlite"
		if config.Storage.SQLite.MaxOpenConns > 0 {
			maxOpen = config.Storage.SQLite.MaxOpenConns
		}
		autoMigrate = config.Storage.SQLite.AutoMigrate == nil || *config.Storage.SQLite.AutoMigrate
	}
	if err != nil {
		return nil, fmt.Errorf("open account store: %w", err)
	}
	db.SetMaxOpenConns(maxOpen)
	maxIdle := maxOpen / 2
	if maxIdle < 1 {
		maxIdle = 1
	}
	db.SetMaxIdleConns(maxIdle)
	openCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(openCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping account store: %w", err)
	}
	store := &accountStore{db: db, dialect: dialect}
	if autoMigrate {
		schema := sqliteAccountsSchema
		if dialect == "postgres" {
			schema = postgresAccountsSchema
		}
		if _, err := db.ExecContext(openCtx, schema); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("migrate account store: %w", err)
		}
		if err := ensureAccountSecurityColumns(openCtx, db, dialect); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return store, nil
}

func ensureAccountSecurityColumns(ctx context.Context, db *sql.DB, dialect string) error {
	if dialect == "postgres" {
		if _, err := db.ExecContext(ctx, `
ALTER TABLE users ADD COLUMN IF NOT EXISTS auth_version bigint NOT NULL DEFAULT 1;
ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_login_window_started_at text;
ALTER TABLE users ADD COLUMN IF NOT EXISTS blocked_event_codes jsonb NOT NULL DEFAULT '[]'::jsonb;`); err != nil {
			return fmt.Errorf("migrate account security columns: %w", err)
		}
		return nil
	}
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(users)`)
	if err != nil {
		return fmt.Errorf("inspect account schema: %w", err)
	}
	found := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull int
		var defaultValue any
		var primaryKey int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return fmt.Errorf("inspect account schema row: %w", err)
		}
		found[strings.ToLower(name)] = true
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close account schema inspection: %w", err)
	}
	if !found["auth_version"] {
		if _, err := db.ExecContext(ctx, `ALTER TABLE users ADD COLUMN auth_version INTEGER NOT NULL DEFAULT 1`); err != nil {
			return fmt.Errorf("migrate account auth version: %w", err)
		}
	}
	if !found["failed_login_window_started_at"] {
		if _, err := db.ExecContext(ctx, `ALTER TABLE users ADD COLUMN failed_login_window_started_at TEXT`); err != nil {
			return fmt.Errorf("migrate account login window: %w", err)
		}
	}
	if !found["blocked_event_codes"] {
		if _, err := db.ExecContext(ctx, `ALTER TABLE users ADD COLUMN blocked_event_codes TEXT NOT NULL DEFAULT '[]'`); err != nil {
			return fmt.Errorf("migrate account blocked event codes: %w", err)
		}
	}
	return nil
}

func (s *accountStore) Close() {
	if s != nil && s.db != nil {
		_ = s.db.Close()
	}
}

func (s *accountStore) bind(query string) string {
	if s == nil || s.dialect != "postgres" {
		return query
	}
	var out strings.Builder
	parameter := 1
	for _, char := range query {
		if char == '?' {
			fmt.Fprintf(&out, "$%d", parameter)
			parameter++
			continue
		}
		out.WriteRune(char)
	}
	return out.String()
}

func (s *accountStore) Count(ctx context.Context) (int, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("account store is unavailable")
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count accounts: %w", err)
	}
	return count, nil
}

func (s *accountStore) Create(ctx context.Context, account Account, passwordHash string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("account store is unavailable")
	}
	if err := validateAccount(&account); err != nil {
		return err
	}
	if strings.TrimSpace(passwordHash) == "" {
		return fmt.Errorf("password hash is required")
	}
	now := time.Now().UTC()
	if account.ID == "" {
		id, err := randomUUID()
		if err != nil {
			return err
		}
		account.ID = id
	}
	if account.PasswordChangedAt.IsZero() {
		account.PasswordChangedAt = now
	}
	account.CreatedAt = now
	account.UpdatedAt = now
	originators, _ := json.Marshal(account.AllowedOriginators)
	blockedEvents, _ := json.Marshal(account.BlockedEventCodes)
	cidrs, _ := json.Marshal(account.CIDRWhitelist)
	_, err := s.db.ExecContext(ctx, s.bind(`
INSERT INTO users (
    id, username, password_hash, sender_id, force_sender_id, last_ip, last_login_at,
    password_changed_at, allow_origination, allowed_originators, blocked_event_codes, force_originator_name,
    originator_name_text, include_ip_in_brackets, can_view_logs, allow_persistent_sessions,
    password_expiry_days, allow_user_pw_change, logging_enabled, account_locked,
	failed_login_attempts, failed_login_window_started_at, mfa_secret, mfa_enabled, mfa_last_used_step, auth_version, is_admin, cidr_whitelist, created_at, updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`),
		account.ID, account.Username, passwordHash, nullText(account.SenderID), account.ForceSenderID,
		nullText(account.LastIP), nullTimeText(account.LastLoginAt), account.PasswordChangedAt.Format(time.RFC3339Nano),
		account.AllowOrigination, string(originators), string(blockedEvents), account.ForceOriginatorName, nullText(account.OriginatorNameText),
		account.IncludeIPInBrackets, account.CanViewLogs, account.AllowPersistentSessions,
		account.PasswordExpiryDays, account.AllowUserPasswordChange, account.LoggingEnabled,
		account.AccountLocked, account.FailedLoginAttempts, nullTimeText(account.failedLoginWindowStartedAt), nullText(account.mfaSecret), account.MFAEnabled, account.mfaLastStep, maxInt64(account.authVersion, 1),
		account.IsAdmin, string(cidrs), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("create account: %w", err)
	}
	return nil
}

func (s *accountStore) ByUsername(ctx context.Context, username string) (Account, error) {
	return s.queryOne(ctx, `SELECT `+accountColumns+` FROM users WHERE lower(username) = lower(?)`, strings.TrimSpace(username))
}

func (s *accountStore) ByID(ctx context.Context, id string) (Account, error) {
	return s.queryOne(ctx, `SELECT `+accountColumns+` FROM users WHERE id = ?`, strings.TrimSpace(id))
}

func (s *accountStore) List(ctx context.Context) ([]Account, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("account store is unavailable")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+accountColumns+` FROM users ORDER BY lower(username)`)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	defer rows.Close()
	accounts := []Account{}
	for rows.Next() {
		account, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	return accounts, nil
}

func (s *accountStore) queryOne(ctx context.Context, query string, args ...any) (Account, error) {
	if s == nil || s.db == nil {
		return Account{}, fmt.Errorf("account store is unavailable")
	}
	account, err := scanAccount(s.db.QueryRowContext(ctx, s.bind(query), args...))
	if errors.Is(err, sql.ErrNoRows) {
		return Account{}, errAccountNotFound
	}
	if err != nil {
		return Account{}, err
	}
	return account, nil
}

const accountColumns = `
id, username, password_hash, sender_id, force_sender_id, last_ip, last_login_at,
password_changed_at, allow_origination, allowed_originators, blocked_event_codes, force_originator_name,
originator_name_text, include_ip_in_brackets, can_view_logs, allow_persistent_sessions,
password_expiry_days, allow_user_pw_change, logging_enabled, account_locked,
failed_login_attempts, failed_login_window_started_at, mfa_secret, mfa_enabled, mfa_last_used_step, auth_version, is_admin, cidr_whitelist, created_at, updated_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanAccount(row rowScanner) (Account, error) {
	var (
		account                                       Account
		passwordHash, senderID, lastIP                sql.NullString
		lastLogin, originatorName                     sql.NullString
		failedWindow                                  sql.NullString
		mfaSecret                                     sql.NullString
		passwordChanged, created, updated             string
		originatorsJSON, blockedEventsJSON, cidrsJSON []byte
	)
	err := row.Scan(
		&account.ID, &account.Username, &passwordHash, &senderID, &account.ForceSenderID,
		&lastIP, &lastLogin, &passwordChanged, &account.AllowOrigination, &originatorsJSON, &blockedEventsJSON,
		&account.ForceOriginatorName, &originatorName, &account.IncludeIPInBrackets,
		&account.CanViewLogs, &account.AllowPersistentSessions, &account.PasswordExpiryDays,
		&account.AllowUserPasswordChange, &account.LoggingEnabled, &account.AccountLocked,
		&account.FailedLoginAttempts, &failedWindow, &mfaSecret, &account.MFAEnabled, &account.mfaLastStep, &account.authVersion, &account.IsAdmin,
		&cidrsJSON, &created, &updated,
	)
	if err != nil {
		return Account{}, err
	}
	account.passwordHash = passwordHash.String
	account.SenderID = senderID.String
	account.LastIP = lastIP.String
	account.OriginatorNameText = originatorName.String
	account.mfaSecret = mfaSecret.String
	account.MFAConfigured = strings.TrimSpace(account.mfaSecret) != ""
	var timeErr error
	if account.LastLoginAt, timeErr = parseStoredTime(lastLogin.String, false); timeErr != nil {
		return Account{}, fmt.Errorf("decode account last login timestamp: %w", timeErr)
	}
	if account.PasswordChangedAt, timeErr = parseStoredTime(passwordChanged, true); timeErr != nil {
		return Account{}, fmt.Errorf("decode account password timestamp: %w", timeErr)
	}
	if account.CreatedAt, timeErr = parseStoredTime(created, true); timeErr != nil {
		return Account{}, fmt.Errorf("decode account creation timestamp: %w", timeErr)
	}
	if account.UpdatedAt, timeErr = parseStoredTime(updated, true); timeErr != nil {
		return Account{}, fmt.Errorf("decode account update timestamp: %w", timeErr)
	}
	if account.failedLoginWindowStartedAt, timeErr = parseStoredTime(failedWindow.String, false); timeErr != nil {
		return Account{}, fmt.Errorf("decode account login failure window: %w", timeErr)
	}
	if err := json.Unmarshal(originatorsJSON, &account.AllowedOriginators); err != nil {
		return Account{}, fmt.Errorf("decode account originator policy: %w", err)
	}
	if err := json.Unmarshal(blockedEventsJSON, &account.BlockedEventCodes); err != nil {
		return Account{}, fmt.Errorf("decode account blocked event policy: %w", err)
	}
	if err := json.Unmarshal(cidrsJSON, &account.CIDRWhitelist); err != nil {
		return Account{}, fmt.Errorf("decode account CIDR policy: %w", err)
	}
	for _, code := range account.AllowedOriginators {
		if _, ok := allowedOriginatorCodes[strings.ToUpper(strings.TrimSpace(code))]; !ok {
			return Account{}, fmt.Errorf("account contains unsupported originator policy %q", code)
		}
	}
	for _, cidr := range account.CIDRWhitelist {
		if !validCIDR(cidr) {
			return Account{}, fmt.Errorf("account contains invalid CIDR policy %q", cidr)
		}
	}
	account.AllowedOriginators = normalizeOriginators(account.AllowedOriginators)
	account.BlockedEventCodes = normalizeEventCodes(account.BlockedEventCodes)
	account.CIDRWhitelist = normalizeCIDRs(account.CIDRWhitelist)
	return account, nil
}

func (s *accountStore) Save(ctx context.Context, account Account) error {
	if err := validateAccount(&account); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin account policy update: %w", err)
	}
	defer tx.Rollback()
	if err := s.lockAdministratorMutation(ctx, tx, account.ID); err != nil {
		return err
	}
	var wasAdmin bool
	if err := tx.QueryRowContext(ctx, s.bind(`SELECT is_admin FROM users WHERE id=?`), account.ID).Scan(&wasAdmin); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errAccountNotFound
		}
		return fmt.Errorf("load account role before update: %w", err)
	}
	if wasAdmin && (!account.IsAdmin || account.AccountLocked) {
		if err := s.requireOtherUnlockedAdministrator(ctx, tx, account.ID); err != nil {
			return err
		}
	}
	originators, _ := json.Marshal(account.AllowedOriginators)
	blockedEvents, _ := json.Marshal(account.BlockedEventCodes)
	cidrs, _ := json.Marshal(account.CIDRWhitelist)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := tx.ExecContext(ctx, s.bind(`
UPDATE users SET
    username=?, sender_id=?, force_sender_id=?, allow_origination=?, allowed_originators=?, blocked_event_codes=?,
	force_originator_name=?, originator_name_text=?, include_ip_in_brackets=?, can_view_logs=?,
	allow_persistent_sessions=?, password_expiry_days=?, allow_user_pw_change=?, logging_enabled=?,
	account_locked=?, mfa_enabled=?, is_admin=?, cidr_whitelist=?, auth_version=auth_version+1, updated_at=?
WHERE id=?`), account.Username, nullText(account.SenderID), account.ForceSenderID,
		account.AllowOrigination, string(originators), string(blockedEvents), account.ForceOriginatorName,
		nullText(account.OriginatorNameText), account.IncludeIPInBrackets, account.CanViewLogs,
		account.AllowPersistentSessions, account.PasswordExpiryDays, account.AllowUserPasswordChange,
		account.LoggingEnabled, account.AccountLocked, account.MFAEnabled, account.IsAdmin,
		string(cidrs), now, account.ID)
	if err != nil {
		return fmt.Errorf("save account: %w", err)
	}
	if err := requireAffected(result); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit account policy update: %w", err)
	}
	return nil
}

func (s *accountStore) SetPassword(ctx context.Context, accountID string, passwordHash string) error {
	result, err := s.db.ExecContext(ctx, s.bind(`
UPDATE users SET password_hash=?, password_changed_at=?, failed_login_attempts=0,
	failed_login_window_started_at=NULL, account_locked=false, auth_version=auth_version+1, updated_at=? WHERE id=?`), passwordHash, time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano), accountID)
	if err != nil {
		return fmt.Errorf("set account password: %w", err)
	}
	return requireAffected(result)
}

func (s *accountStore) SetPasswordIfAuthVersion(ctx context.Context, accountID string, passwordHash string, expectedVersion int64) error {
	if expectedVersion < 1 {
		return fmt.Errorf("expected account authentication version is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, s.bind(`
UPDATE users SET password_hash=?, password_changed_at=?, failed_login_attempts=0,
	failed_login_window_started_at=NULL, account_locked=false, auth_version=auth_version+1, updated_at=?
WHERE id=? AND auth_version=? AND account_locked=false AND allow_user_pw_change=true`), passwordHash, now, now, accountID, expectedVersion)
	if err != nil {
		return fmt.Errorf("set account password conditionally: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errAccountVersionChanged
	}
	return nil
}

func (s *accountStore) SetMFA(ctx context.Context, accountID string, encryptedSecret string, enabled bool) error {
	result, err := s.db.ExecContext(ctx, s.bind(`UPDATE users SET mfa_secret=?, mfa_enabled=?, mfa_last_used_step=0, auth_version=auth_version+1, updated_at=? WHERE id=?`),
		nullText(encryptedSecret), enabled, time.Now().UTC().Format(time.RFC3339Nano), accountID)
	if err != nil {
		return fmt.Errorf("set account MFA: %w", err)
	}
	return requireAffected(result)
}

func (s *accountStore) SetMFAIfAuthVersion(ctx context.Context, accountID string, encryptedSecret string, expectedVersion int64) error {
	if expectedVersion < 1 {
		return fmt.Errorf("expected account authentication version is required")
	}
	result, err := s.db.ExecContext(ctx, s.bind(`
UPDATE users SET mfa_secret=?, mfa_enabled=false, mfa_last_used_step=0, auth_version=auth_version+1, updated_at=?
WHERE id=? AND auth_version=? AND account_locked=false AND (mfa_secret IS NULL OR mfa_secret='')`),
		nullText(encryptedSecret), time.Now().UTC().Format(time.RFC3339Nano), accountID, expectedVersion)
	if err != nil {
		return fmt.Errorf("set account MFA conditionally: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errAccountVersionChanged
	}
	return nil
}

func (s *accountStore) EnableMFA(ctx context.Context, accountID string) error {
	result, err := s.db.ExecContext(ctx, s.bind(`UPDATE users SET mfa_enabled=true, updated_at=? WHERE id=?`),
		time.Now().UTC().Format(time.RFC3339Nano), accountID)
	if err != nil {
		return fmt.Errorf("enable account MFA: %w", err)
	}
	return requireAffected(result)
}

func (s *accountStore) EnableMFAIfAuthVersion(ctx context.Context, accountID string, expectedVersion int64) error {
	if expectedVersion < 1 {
		return fmt.Errorf("expected account authentication version is required")
	}
	result, err := s.db.ExecContext(ctx, s.bind(`
UPDATE users SET mfa_enabled=true, updated_at=?
WHERE id=? AND auth_version=? AND account_locked=false AND mfa_secret IS NOT NULL AND mfa_secret<>''`),
		time.Now().UTC().Format(time.RFC3339Nano), accountID, expectedVersion)
	if err != nil {
		return fmt.Errorf("enable account MFA conditionally: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errAccountVersionChanged
	}
	return nil
}

func (s *accountStore) AcceptMFAStep(ctx context.Context, accountID string, expectedVersion int64, step int64) error {
	if expectedVersion < 1 {
		return fmt.Errorf("expected account authentication version is required")
	}
	result, err := s.db.ExecContext(ctx, s.bind(`
UPDATE users SET mfa_last_used_step=?, updated_at=?
WHERE id=? AND auth_version=? AND account_locked=false AND mfa_last_used_step < ?`),
		step, time.Now().UTC().Format(time.RFC3339Nano), accountID, expectedVersion, step)
	if err != nil {
		return fmt.Errorf("record MFA step: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("MFA code was already used")
	}
	return nil
}

func (s *accountStore) RecordLoginFailure(ctx context.Context, accountID string, lockAt int, window time.Duration, now time.Time) (Account, error) {
	return s.recordLoginFailure(ctx, accountID, 0, lockAt, window, now)
}

func (s *accountStore) RecordLoginFailureIfAuthVersion(ctx context.Context, accountID string, expectedVersion int64, lockAt int, window time.Duration, now time.Time) (Account, error) {
	if expectedVersion < 1 {
		return Account{}, fmt.Errorf("expected account authentication version is required")
	}
	return s.recordLoginFailure(ctx, accountID, expectedVersion, lockAt, window, now)
}

func (s *accountStore) recordLoginFailure(ctx context.Context, accountID string, expectedVersion int64, lockAt int, window time.Duration, now time.Time) (Account, error) {
	if lockAt <= 0 {
		lockAt = 5
	}
	if window <= 0 {
		window = 15 * time.Minute
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return Account{}, fmt.Errorf("begin login failure update: %w", err)
	}
	defer tx.Rollback()
	query := `SELECT failed_login_attempts, failed_login_window_started_at, account_locked, auth_version, is_admin FROM users WHERE id=?`
	if s.dialect == "postgres" {
		query += ` FOR UPDATE`
	}
	var attempts int
	var started sql.NullString
	var locked bool
	var currentVersion int64
	var isAdmin bool
	if err := tx.QueryRowContext(ctx, s.bind(query), accountID).Scan(&attempts, &started, &locked, &currentVersion, &isAdmin); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Account{}, errAccountNotFound
		}
		return Account{}, fmt.Errorf("load login failure window: %w", err)
	}
	if expectedVersion > 0 && currentVersion != expectedVersion {
		return Account{}, errAccountVersionChanged
	}
	windowStarted, err := parseStoredTime(started.String, false)
	if err != nil {
		return Account{}, fmt.Errorf("decode login failure window: %w", err)
	}
	if windowStarted.IsZero() || now.Before(windowStarted) || now.Sub(windowStarted) >= window {
		attempts = 0
		windowStarted = now
	}
	attempts++
	newlyLocked := !isAdmin && !locked && attempts >= lockAt
	if newlyLocked {
		locked = true
		windowStarted = now
	}
	versionIncrement := 0
	if newlyLocked {
		versionIncrement = 1
	}
	updateQuery := `
UPDATE users SET failed_login_attempts=?, failed_login_window_started_at=?, account_locked=?,
	auth_version=auth_version+?, updated_at=? WHERE id=?`
	args := []any{attempts, windowStarted.Format(time.RFC3339Nano), locked, versionIncrement, now.Format(time.RFC3339Nano), accountID}
	if expectedVersion > 0 {
		updateQuery += ` AND auth_version=?`
		args = append(args, expectedVersion)
	}
	result, err := tx.ExecContext(ctx, s.bind(updateQuery), args...)
	if err != nil {
		return Account{}, fmt.Errorf("record login failure: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return Account{}, err
	}
	if count != 1 {
		if expectedVersion > 0 {
			return Account{}, errAccountVersionChanged
		}
		return Account{}, errAccountNotFound
	}
	if err := tx.Commit(); err != nil {
		return Account{}, fmt.Errorf("commit login failure: %w", err)
	}
	return s.ByID(ctx, accountID)
}

func (s *accountStore) RecordLoginSuccess(ctx context.Context, accountID string, ip string, expectedVersion int64) error {
	if expectedVersion < 1 {
		return fmt.Errorf("expected account authentication version is required")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, s.bind(`
UPDATE users SET last_ip=?, last_login_at=?, failed_login_attempts=0, failed_login_window_started_at=NULL, updated_at=?
WHERE id=? AND auth_version=? AND account_locked=false`), nullText(ip), now, now, accountID, expectedVersion)
	if err != nil {
		return fmt.Errorf("record login success: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errAccountVersionChanged
	}
	return nil
}

func (s *accountStore) Unlock(ctx context.Context, accountID string) error {
	result, err := s.db.ExecContext(ctx, s.bind(`UPDATE users SET account_locked=false, failed_login_attempts=0, failed_login_window_started_at=NULL, auth_version=auth_version+1, updated_at=? WHERE id=?`),
		time.Now().UTC().Format(time.RFC3339Nano), accountID)
	if err != nil {
		return fmt.Errorf("unlock account: %w", err)
	}
	return requireAffected(result)
}

func (s *accountStore) UnlockLockedIfAuthVersion(ctx context.Context, accountID string, expectedVersion int64, now time.Time) (bool, error) {
	if expectedVersion < 1 {
		return false, fmt.Errorf("expected account authentication version is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	result, err := s.db.ExecContext(ctx, s.bind(`
UPDATE users SET account_locked=false, failed_login_attempts=0, failed_login_window_started_at=NULL,
	auth_version=auth_version+1, updated_at=?
WHERE id=? AND auth_version=? AND account_locked=true`), now.Format(time.RFC3339Nano), accountID, expectedVersion)
	if err != nil {
		return false, fmt.Errorf("automatically unlock account: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return count == 1, nil
}

func (s *accountStore) BumpAuthVersion(ctx context.Context, accountID string) error {
	result, err := s.db.ExecContext(ctx, s.bind(`UPDATE users SET auth_version=auth_version+1, updated_at=? WHERE id=?`),
		time.Now().UTC().Format(time.RFC3339Nano), accountID)
	if err != nil {
		return fmt.Errorf("bump account authentication version: %w", err)
	}
	return requireAffected(result)
}

func maxInt64(value int64, minimum int64) int64 {
	if value < minimum {
		return minimum
	}
	return value
}

func (s *accountStore) Delete(ctx context.Context, accountID string) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("begin account deletion: %w", err)
	}
	defer tx.Rollback()
	if err := s.lockAdministratorMutation(ctx, tx, accountID); err != nil {
		return err
	}
	var isAdmin bool
	if err := tx.QueryRowContext(ctx, s.bind(`SELECT is_admin FROM users WHERE id=?`), accountID).Scan(&isAdmin); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errAccountNotFound
		}
		return fmt.Errorf("load account role before deletion: %w", err)
	}
	if isAdmin {
		if err := s.requireOtherUnlockedAdministrator(ctx, tx, accountID); err != nil {
			return err
		}
	}
	result, err := tx.ExecContext(ctx, s.bind(`DELETE FROM users WHERE id=?`), accountID)
	if err != nil {
		return fmt.Errorf("delete account: %w", err)
	}
	if err := requireAffected(result); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit account deletion: %w", err)
	}
	return nil
}

func (s *accountStore) lockAdministratorMutation(ctx context.Context, tx *sql.Tx, accountID string) error {
	if s.dialect == "postgres" {
		if _, err := tx.ExecContext(ctx, `LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return fmt.Errorf("lock administrator account set: %w", err)
		}
		return nil
	}
	result, err := tx.ExecContext(ctx, s.bind(`UPDATE users SET updated_at=updated_at WHERE id=?`), accountID)
	if err != nil {
		return fmt.Errorf("lock administrator account set: %w", err)
	}
	return requireAffected(result)
}

func (s *accountStore) requireOtherUnlockedAdministrator(ctx context.Context, tx *sql.Tx, excludedID string) error {
	var count int
	if err := tx.QueryRowContext(ctx, s.bind(`SELECT COUNT(*) FROM users WHERE id<>? AND is_admin=true AND account_locked=false`), excludedID).Scan(&count); err != nil {
		return fmt.Errorf("count remaining administrators: %w", err)
	}
	if count < 1 {
		return fmt.Errorf("at least one other unlocked administrator account is required")
	}
	return nil
}

func requireAffected(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return errAccountNotFound
	}
	return nil
}

func validateAccount(account *Account) error {
	if account == nil {
		return fmt.Errorf("account is required")
	}
	account.Username = strings.TrimSpace(account.Username)
	if !validUsername.MatchString(account.Username) {
		return fmt.Errorf("username must be 1 to 50 letters, numbers, periods, underscores, or hyphens")
	}
	if account.IsAdmin {
		account.AccountLocked = false
	}
	account.SenderID = strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(account.SenderID), "-", "/"))
	if account.ForceSenderID && !validSenderID.MatchString(account.SenderID) {
		return fmt.Errorf("forced SAME sender ID must be exactly 8 ASCII letters, numbers, or slashes")
	}
	if !account.ForceSenderID {
		account.SenderID = ""
	}
	account.OriginatorNameText = strings.TrimSpace(account.OriginatorNameText)
	if len(account.OriginatorNameText) > 100 || strings.ContainsAny(account.OriginatorNameText, "\r\n\x00") {
		return fmt.Errorf("originator name must be at most 100 characters without control characters")
	}
	if account.ForceOriginatorName && account.OriginatorNameText == "" {
		return fmt.Errorf("originator name is required when the override is enabled")
	}
	if !account.ForceOriginatorName {
		account.OriginatorNameText = ""
		account.IncludeIPInBrackets = false
	}
	if account.PasswordExpiryDays < 0 || account.PasswordExpiryDays > 3650 {
		return fmt.Errorf("password expiry must be between 0 and 3650 days")
	}
	for _, value := range account.AllowedOriginators {
		code := strings.ToUpper(strings.TrimSpace(value))
		if code == "" {
			continue
		}
		if _, ok := allowedOriginatorCodes[code]; !ok {
			return fmt.Errorf("unsupported originator %q", code)
		}
	}
	account.AllowedOriginators = normalizeOriginators(account.AllowedOriginators)
	if account.AllowOrigination && len(account.AllowedOriginators) == 0 {
		return fmt.Errorf("at least one originator must be allowed")
	}
	for _, value := range account.BlockedEventCodes {
		code := strings.ToUpper(strings.TrimSpace(value))
		if !validEventCode.MatchString(code) {
			return fmt.Errorf("invalid blocked SAME event code %q", value)
		}
	}
	account.BlockedEventCodes = normalizeEventCodes(account.BlockedEventCodes)
	for _, cidr := range account.CIDRWhitelist {
		if !validCIDR(cidr) {
			return fmt.Errorf("invalid CIDR %q", cidr)
		}
	}
	account.CIDRWhitelist = normalizeCIDRs(account.CIDRWhitelist)
	return nil
}

func normalizeOriginators(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		code := strings.ToUpper(strings.TrimSpace(value))
		if _, ok := allowedOriginatorCodes[code]; !ok {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

func normalizeEventCodes(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		code := strings.ToUpper(strings.TrimSpace(value))
		if !validEventCode.MatchString(code) {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

func normalizeCIDRs(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func nullText(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func nullTimeText(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func parseStoredTime(value string, required bool) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		if required {
			return time.Time{}, fmt.Errorf("timestamp is required")
		}
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}
