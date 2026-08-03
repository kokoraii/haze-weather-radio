package datastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
)

var ErrNotConfigured = errors.New("datastore is not configured")

type StorageConfig struct {
	SQLite   SQLiteConfig   `yaml:"sqlite"`
	Postgres PostgresConfig `yaml:"postgres"`
}

type Store interface {
	Close()
	UpsertLocation(ctx context.Context, record LocationRecord) error
	UpsertObservation(ctx context.Context, record ObservationRecord) error
	UpsertForecast(ctx context.Context, record ForecastRecord) error
	ObservationPayload(ctx context.Context, source string, locationID string, target any) (bool, error)
	ForecastPayload(ctx context.Context, source string, forecastID string, target any) (bool, error)
	StoreProductPayload(ctx context.Context, record ProductPayloadRecord) error
	ProductPayload(ctx context.Context, kind string, source string, id string, target any) (bool, error)
	StoreTextProduct(ctx context.Context, record TextProductRecord) error
	TextProduct(ctx context.Context, source string, id string) (string, bool, error)
	StoreCAPArchive(ctx context.Context, record CAPArchiveRecord) error
	ListCAPArchives(ctx context.Context, bucket string, since time.Time) ([]StoredCAPArchive, error)
	DeleteCAPArchive(ctx context.Context, alertID string, feedID string) error
	DeleteCAPArchiveBucketItem(ctx context.Context, alertID string, feedID string, bucket string) error
	ClearCAPArchiveBucket(ctx context.Context, bucket string) error
	ClearAllCAPArchives(ctx context.Context) error
	ExpireNonCriticalCAPArchives(ctx context.Context) error
}

type SQLiteConfig struct {
	Enabled      *bool  `yaml:"enabled"`
	Path         string `yaml:"path"`
	AutoMigrate  *bool  `yaml:"auto_migrate"`
	BusyTimeout  string `yaml:"busy_timeout"`
	MaxOpenConns int    `yaml:"max_open_conns"`
}

type PostgresConfig struct {
	Enabled     bool   `yaml:"enabled"`
	DSN         string `yaml:"dsn"`
	DSNEnv      string `yaml:"dsn_env"`
	AutoMigrate *bool  `yaml:"auto_migrate"`
	MaxConns    int32  `yaml:"max_conns"`
	Timeout     string `yaml:"timeout"`
	AppName     string `yaml:"app_name"`
}

type PostgresStore struct {
	pool *pgxpool.Pool
}

type LocationRecord struct {
	CanonicalID string
	Source      string
	LocationID  string
	Kind        string
	NameEN      string
	NameFR      string
	StationID   string
	CityPageID  string
	CLC         string
	Latitude    *float64
	Longitude   *float64
	Metadata    map[string]any
}

type ObservationRecord struct {
	Source        string
	LocationID    string
	StationID     string
	ObservedAtRaw string
	Payload       any
}

type ForecastRecord struct {
	Source       string
	ForecastID   string
	RegionID     string
	IssuedAtRaw  string
	UpdatedAtRaw string
	Payload      any
}

type ProductPayloadRecord struct {
	Kind      string
	Source    string
	ID        string
	Payload   any
	UpdatedAt time.Time
}

type TextProductRecord struct {
	Source    string
	ID        string
	Text      string
	Metadata  map[string]any
	UpdatedAt time.Time
}

type CAPArchiveRecord struct {
	AlertID      string
	FeedID       string
	Bucket       string
	Status       string
	Reason       string
	Sender       string
	Source       string
	SentAtRaw    string
	UpdatedAtRaw string
	ExpiresAtRaw string
	Event        string
	Headline     string
	RawXML       string
	Metadata     map[string]any
}

type StoredCAPArchive struct {
	AlertID   string
	FeedID    string
	Bucket    string
	Status    string
	Reason    string
	Sender    string
	Source    string
	SentAt    time.Time
	UpdatedAt time.Time
	ExpiresAt time.Time
	Event     string
	Headline  string
	RawXML    string
	StoredAt  time.Time
	Metadata  map[string]any
}

type CAPXMLArchive struct {
	Compressed []byte
	SHA256Hex  string
	RawBytes   int
	ZstdBytes  int
}

func Open(ctx context.Context, cfg StorageConfig, baseDir string) (Store, error) {
	if cfg.Postgres.Enabled {
		return OpenPostgres(ctx, cfg.Postgres)
	}
	sqliteEnabled := cfg.SQLite.Enabled == nil || *cfg.SQLite.Enabled
	if sqliteEnabled {
		return OpenSQLite(ctx, cfg.SQLite, baseDir)
	}
	return nil, ErrNotConfigured
}

func OpenPostgres(ctx context.Context, cfg PostgresConfig) (*PostgresStore, error) {
	if !cfg.Enabled {
		return nil, ErrNotConfigured
	}
	dsn := cfg.resolvedDSN()
	if dsn == "" {
		return nil, ErrNotConfigured
	}
	timeout := durationOr(cfg.Timeout, 5*time.Second)
	openCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pgxCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		pgxCfg.MaxConns = cfg.MaxConns
	}
	if cfg.AppName != "" {
		pgxCfg.ConnConfig.RuntimeParams["application_name"] = cfg.AppName
	}
	pool, err := pgxpool.NewWithConfig(openCtx, pgxCfg)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}
	if err := pool.Ping(openCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	store := &PostgresStore{pool: pool}
	if cfg.AutoMigrate == nil || *cfg.AutoMigrate {
		if err := store.Migrate(openCtx); err != nil {
			pool.Close()
			return nil, err
		}
	}
	return store, nil
}

func (cfg PostgresConfig) resolvedDSN() string {
	if value := strings.TrimSpace(cfg.DSN); value != "" {
		return value
	}
	envName := strings.TrimSpace(cfg.DSNEnv)
	if envName == "" {
		envName = "HAZE_POSTGRES_DSN"
	}
	return strings.TrimSpace(os.Getenv(envName))
}

func (s *PostgresStore) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *PostgresStore) Migrate(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return ErrNotConfigured
	}
	if _, err := s.pool.Exec(ctx, postgresSchemaSQL); err != nil {
		return fmt.Errorf("migrate postgres datastore: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `ALTER TABLE locations.locations ADD COLUMN IF NOT EXISTS canonical_id text`); err != nil {
		return fmt.Errorf("add canonical location ID column: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpsertLocation(ctx context.Context, record LocationRecord) error {
	if s == nil || s.pool == nil {
		return nil
	}
	source := clean(record.Source, "unknown")
	locationID := clean(record.LocationID, "")
	if locationID == "" {
		return nil
	}
	metadata, err := jsonText(record.Metadata)
	if err != nil {
		return fmt.Errorf("marshal location metadata: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO locations.locations (
    canonical_id, source, location_id, kind, name_en, name_fr, station_id, citypage_id, clc,
    latitude, longitude, metadata, last_seen
) VALUES (NULLIF($1,''),$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb, now())
ON CONFLICT (source, location_id) DO UPDATE SET
    canonical_id = COALESCE(EXCLUDED.canonical_id, locations.locations.canonical_id),
    kind = COALESCE(NULLIF(EXCLUDED.kind, ''), locations.locations.kind),
    name_en = COALESCE(NULLIF(EXCLUDED.name_en, ''), locations.locations.name_en),
    name_fr = COALESCE(NULLIF(EXCLUDED.name_fr, ''), locations.locations.name_fr),
    station_id = COALESCE(NULLIF(EXCLUDED.station_id, ''), locations.locations.station_id),
    citypage_id = COALESCE(NULLIF(EXCLUDED.citypage_id, ''), locations.locations.citypage_id),
    clc = COALESCE(NULLIF(EXCLUDED.clc, ''), locations.locations.clc),
    latitude = COALESCE(EXCLUDED.latitude, locations.locations.latitude),
    longitude = COALESCE(EXCLUDED.longitude, locations.locations.longitude),
    metadata = locations.locations.metadata || EXCLUDED.metadata,
    last_seen = now()`,
		strings.TrimSpace(record.CanonicalID),
		source,
		locationID,
		clean(record.Kind, "unknown"),
		strings.TrimSpace(record.NameEN),
		strings.TrimSpace(record.NameFR),
		strings.TrimSpace(record.StationID),
		strings.TrimSpace(record.CityPageID),
		strings.TrimSpace(record.CLC),
		record.Latitude,
		record.Longitude,
		metadata,
	)
	if err != nil {
		return fmt.Errorf("upsert location %s/%s: %w", source, locationID, err)
	}
	return nil
}

func (s *PostgresStore) UpsertObservation(ctx context.Context, record ObservationRecord) error {
	if s == nil || s.pool == nil {
		return nil
	}
	source := clean(record.Source, "unknown")
	locationID := clean(record.LocationID, "")
	if locationID == "" {
		return nil
	}
	payload, err := jsonText(record.Payload)
	if err != nil {
		return fmt.Errorf("marshal observation payload: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO observations.current (
    source, location_id, station_id, observed_at, payload, updated_at
) VALUES ($1,$2,$3,$4,$5::jsonb, now())
ON CONFLICT (source, location_id) DO UPDATE SET
    station_id = COALESCE(NULLIF(EXCLUDED.station_id, ''), observations.current.station_id),
    observed_at = COALESCE(EXCLUDED.observed_at, observations.current.observed_at),
    payload = EXCLUDED.payload,
    updated_at = now()`,
		source,
		locationID,
		strings.TrimSpace(record.StationID),
		parseStoreTime(record.ObservedAtRaw),
		payload,
	)
	if err != nil {
		return fmt.Errorf("upsert observation %s/%s: %w", source, locationID, err)
	}
	return nil
}

func (s *PostgresStore) UpsertForecast(ctx context.Context, record ForecastRecord) error {
	if s == nil || s.pool == nil {
		return nil
	}
	source := clean(record.Source, "unknown")
	forecastID := clean(record.ForecastID, "")
	if forecastID == "" {
		return nil
	}
	payload, err := jsonText(record.Payload)
	if err != nil {
		return fmt.Errorf("marshal forecast payload: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO forecasts.current (
    source, forecast_id, region_id, issued_at, source_updated_at, payload, updated_at
) VALUES ($1,$2,$3,$4,$5,$6::jsonb, now())
ON CONFLICT (source, forecast_id) DO UPDATE SET
    region_id = COALESCE(NULLIF(EXCLUDED.region_id, ''), forecasts.current.region_id),
    issued_at = COALESCE(EXCLUDED.issued_at, forecasts.current.issued_at),
    source_updated_at = COALESCE(EXCLUDED.source_updated_at, forecasts.current.source_updated_at),
    payload = EXCLUDED.payload,
    updated_at = now()`,
		source,
		forecastID,
		strings.TrimSpace(record.RegionID),
		parseStoreTime(record.IssuedAtRaw),
		parseStoreTime(record.UpdatedAtRaw),
		payload,
	)
	if err != nil {
		return fmt.Errorf("upsert forecast %s/%s: %w", source, forecastID, err)
	}
	return nil
}

func (s *PostgresStore) ObservationPayload(ctx context.Context, source string, locationID string, target any) (bool, error) {
	return s.loadJSONPayload(ctx, `SELECT payload FROM observations.current WHERE source = $1 AND location_id = $2`, source, locationID, target)
}

func (s *PostgresStore) ForecastPayload(ctx context.Context, source string, forecastID string, target any) (bool, error) {
	if s == nil || s.pool == nil {
		return false, nil
	}
	source = clean(source, "unknown")
	forecastID = clean(forecastID, "")
	if forecastID == "" {
		return false, nil
	}
	var raw []byte
	var issuedAt pgtype.Timestamptz
	var updatedAt pgtype.Timestamptz
	if err := s.pool.QueryRow(ctx, `SELECT payload, issued_at, source_updated_at FROM forecasts.current WHERE source = $1 AND forecast_id = $2`, source, forecastID).Scan(&raw, &issuedAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if err := mergeForecastPayloadTimes(raw, pgTimestampText(issuedAt), pgTimestampText(updatedAt), target); err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) StoreProductPayload(ctx context.Context, record ProductPayloadRecord) error {
	if s == nil || s.pool == nil {
		return nil
	}
	return s.storeProductPayload(ctx, record)
}

func (s *PostgresStore) ProductPayload(ctx context.Context, kind string, source string, id string, target any) (bool, error) {
	if s == nil || s.pool == nil {
		return false, nil
	}
	kind = clean(kind, "unknown")
	source = clean(source, "unknown")
	id = clean(id, "")
	if id == "" {
		return false, nil
	}
	var raw []byte
	if err := s.pool.QueryRow(ctx, `SELECT payload FROM products.payloads WHERE kind = $1 AND source = $2 AND item_id = $3`, kind, source, id).Scan(&raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) StoreTextProduct(ctx context.Context, record TextProductRecord) error {
	if s == nil || s.pool == nil {
		return nil
	}
	source := clean(record.Source, "unknown")
	id := clean(record.ID, "")
	if id == "" || strings.TrimSpace(record.Text) == "" {
		return nil
	}
	metadata, err := jsonText(record.Metadata)
	if err != nil {
		return fmt.Errorf("marshal text product metadata: %w", err)
	}
	updatedAt := record.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO products.text_products (source, item_id, body, metadata, updated_at)
VALUES ($1,$2,$3,$4::jsonb,$5)
ON CONFLICT (source, item_id) DO UPDATE SET
    body = EXCLUDED.body,
    metadata = products.text_products.metadata || EXCLUDED.metadata,
    updated_at = EXCLUDED.updated_at`,
		source,
		id,
		strings.TrimSpace(record.Text),
		metadata,
		updatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("store text product %s/%s: %w", source, id, err)
	}
	return nil
}

func (s *PostgresStore) TextProduct(ctx context.Context, source string, id string) (string, bool, error) {
	if s == nil || s.pool == nil {
		return "", false, nil
	}
	source = clean(source, "unknown")
	id = clean(id, "")
	if id == "" {
		return "", false, nil
	}
	var body string
	if err := s.pool.QueryRow(ctx, `SELECT body FROM products.text_products WHERE source = $1 AND item_id = $2`, source, id).Scan(&body); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return body, true, nil
}

func (s *PostgresStore) storeProductPayload(ctx context.Context, record ProductPayloadRecord) error {
	kind := clean(record.Kind, "unknown")
	source := clean(record.Source, "unknown")
	id := clean(record.ID, "")
	if id == "" {
		return nil
	}
	payload, err := jsonText(record.Payload)
	if err != nil {
		return fmt.Errorf("marshal product payload: %w", err)
	}
	updatedAt := record.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO products.payloads (kind, source, item_id, payload, updated_at)
VALUES ($1,$2,$3,$4::jsonb,$5)
ON CONFLICT (kind, source, item_id) DO UPDATE SET
    payload = EXCLUDED.payload,
    updated_at = EXCLUDED.updated_at`,
		kind,
		source,
		id,
		payload,
		updatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("store product payload %s/%s/%s: %w", kind, source, id, err)
	}
	return nil
}

func (s *PostgresStore) loadJSONPayload(ctx context.Context, query string, source string, id string, target any) (bool, error) {
	if s == nil || s.pool == nil {
		return false, nil
	}
	source = clean(source, "unknown")
	id = clean(id, "")
	if id == "" {
		return false, nil
	}
	var raw []byte
	if err := s.pool.QueryRow(ctx, query, source, id).Scan(&raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) StoreCAPArchive(ctx context.Context, record CAPArchiveRecord) error {
	if s == nil || s.pool == nil {
		return nil
	}
	alertID := clean(record.AlertID, "")
	rawXML := strings.TrimSpace(record.RawXML)
	if alertID == "" || rawXML == "" {
		return nil
	}
	archive, err := EncodeCAPXMLArchive([]byte(rawXML))
	if err != nil {
		return err
	}
	metadata, err := jsonText(record.Metadata)
	if err != nil {
		return fmt.Errorf("marshal cap archive metadata: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO archive.cap_xml (
    alert_id, feed_id, bucket, status, reason, sender, source, sent_at, updated_at,
    expires_at, event, headline, cap_xml_zstd, cap_xml_sha256, original_bytes,
    compressed_bytes, metadata, stored_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb, now())
ON CONFLICT (alert_id, feed_id, bucket) DO UPDATE SET
    status = EXCLUDED.status,
    reason = EXCLUDED.reason,
    sender = EXCLUDED.sender,
    source = EXCLUDED.source,
    sent_at = COALESCE(EXCLUDED.sent_at, archive.cap_xml.sent_at),
    updated_at = COALESCE(EXCLUDED.updated_at, archive.cap_xml.updated_at),
    expires_at = COALESCE(EXCLUDED.expires_at, archive.cap_xml.expires_at),
    event = COALESCE(NULLIF(EXCLUDED.event, ''), archive.cap_xml.event),
    headline = COALESCE(NULLIF(EXCLUDED.headline, ''), archive.cap_xml.headline),
    cap_xml_zstd = EXCLUDED.cap_xml_zstd,
    cap_xml_sha256 = EXCLUDED.cap_xml_sha256,
    original_bytes = EXCLUDED.original_bytes,
    compressed_bytes = EXCLUDED.compressed_bytes,
    metadata = archive.cap_xml.metadata || EXCLUDED.metadata,
    stored_at = now()`,
		alertID,
		strings.TrimSpace(record.FeedID),
		clean(record.Bucket, "accepted"),
		clean(record.Status, record.Bucket),
		strings.TrimSpace(record.Reason),
		strings.TrimSpace(record.Sender),
		strings.TrimSpace(record.Source),
		parseStoreTime(record.SentAtRaw),
		parseStoreTime(record.UpdatedAtRaw),
		parseStoreTime(record.ExpiresAtRaw),
		strings.TrimSpace(record.Event),
		strings.TrimSpace(record.Headline),
		archive.Compressed,
		archive.SHA256Hex,
		archive.RawBytes,
		archive.ZstdBytes,
		metadata,
	)
	if err != nil {
		return fmt.Errorf("store CAP archive %s: %w", alertID, err)
	}
	return nil
}

func (s *PostgresStore) ListCAPArchives(ctx context.Context, bucket string, since time.Time) ([]StoredCAPArchive, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	bucket = clean(bucket, "")
	if bucket == "" {
		return nil, nil
	}
	var sinceArg any
	if !since.IsZero() {
		sinceArg = since.UTC()
	}
	rows, err := s.pool.Query(ctx, `
SELECT alert_id, feed_id, bucket, status, reason, sender, source, sent_at, updated_at,
       expires_at, event, headline, cap_xml_zstd, stored_at, metadata
FROM archive.cap_xml
WHERE bucket = $1 AND ($2::timestamptz IS NULL OR stored_at >= $2)
ORDER BY stored_at DESC
LIMIT 500`, bucket, sinceArg)
	if err != nil {
		return nil, fmt.Errorf("list cap archive %s: %w", bucket, err)
	}
	defer rows.Close()

	out := []StoredCAPArchive{}
	for rows.Next() {
		var record StoredCAPArchive
		var feedID pgtype.Text
		var reason pgtype.Text
		var sender pgtype.Text
		var source pgtype.Text
		var event pgtype.Text
		var headline pgtype.Text
		var sentAt pgtype.Timestamptz
		var updatedAt pgtype.Timestamptz
		var expiresAt pgtype.Timestamptz
		var compressed []byte
		var metadataRaw []byte
		if err := rows.Scan(
			&record.AlertID,
			&feedID,
			&record.Bucket,
			&record.Status,
			&reason,
			&sender,
			&source,
			&sentAt,
			&updatedAt,
			&expiresAt,
			&event,
			&headline,
			&compressed,
			&record.StoredAt,
			&metadataRaw,
		); err != nil {
			return nil, err
		}
		raw, err := DecodeCAPXMLArchive(compressed)
		if err != nil {
			return nil, err
		}
		record.RawXML = string(raw)
		record.FeedID = pgText(feedID)
		record.Reason = pgText(reason)
		record.Sender = pgText(sender)
		record.Source = pgText(source)
		record.Event = pgText(event)
		record.Headline = pgText(headline)
		record.SentAt = pgTime(sentAt)
		record.UpdatedAt = pgTime(updatedAt)
		record.ExpiresAt = pgTime(expiresAt)
		if len(metadataRaw) > 0 {
			_ = json.Unmarshal(metadataRaw, &record.Metadata)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PostgresStore) DeleteCAPArchive(ctx context.Context, alertID string, feedID string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	alertID = clean(alertID, "")
	if alertID == "" {
		return nil
	}
	if strings.TrimSpace(feedID) == "" {
		_, err := s.pool.Exec(ctx, `DELETE FROM archive.cap_xml WHERE alert_id = $1`, alertID)
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM archive.cap_xml WHERE alert_id = $1 AND feed_id = $2`, alertID, strings.TrimSpace(feedID))
	return err
}

func (s *PostgresStore) DeleteCAPArchiveBucketItem(ctx context.Context, alertID string, feedID string, bucket string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	alertID = clean(alertID, "")
	bucket = clean(bucket, "")
	if alertID == "" || bucket == "" {
		return nil
	}
	if strings.TrimSpace(feedID) == "" {
		_, err := s.pool.Exec(ctx, `DELETE FROM archive.cap_xml WHERE alert_id = $1 AND bucket = $2`, alertID, bucket)
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM archive.cap_xml WHERE alert_id = $1 AND feed_id = $2 AND bucket = $3`, alertID, strings.TrimSpace(feedID), bucket)
	return err
}

func (s *PostgresStore) ClearCAPArchiveBucket(ctx context.Context, bucket string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	bucket = clean(bucket, "")
	if bucket == "" {
		return nil
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM archive.cap_xml WHERE bucket = $1`, bucket)
	return err
}

func (s *PostgresStore) ClearAllCAPArchives(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM archive.cap_xml`)
	return err
}

func (s *PostgresStore) ExpireNonCriticalCAPArchives(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
DELETE FROM archive.cap_xml expired
USING archive.cap_xml accepted
WHERE expired.bucket = 'expired'
  AND accepted.bucket = 'accepted'
  AND expired.alert_id = accepted.alert_id
  AND expired.feed_id = accepted.feed_id
  AND upper(coalesce(accepted.event, '')) NOT IN ('TOR', 'SVR');

UPDATE archive.cap_xml
SET bucket = 'expired',
    status = 'expired',
    reason = 'expired by operator',
    stored_at = now()
WHERE bucket = 'accepted'
  AND upper(coalesce(event, '')) NOT IN ('TOR', 'SVR')`)
	return err
}

func EncodeCAPXMLArchive(raw []byte) (CAPXMLArchive, error) {
	if len(raw) == 0 {
		return CAPXMLArchive{}, nil
	}
	hash := sha256.Sum256(raw)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	if err != nil {
		return CAPXMLArchive{}, fmt.Errorf("create zstd encoder: %w", err)
	}
	compressed := encoder.EncodeAll(raw, nil)
	return CAPXMLArchive{
		Compressed: compressed,
		SHA256Hex:  hex.EncodeToString(hash[:]),
		RawBytes:   len(raw),
		ZstdBytes:  len(compressed),
	}, nil
}

func DecodeCAPXMLArchive(compressed []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	defer decoder.Close()
	return decoder.DecodeAll(compressed, nil)
}

func pgTime(value pgtype.Timestamptz) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return value.Time.UTC()
}

func pgText(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func pgTimestampText(value pgtype.Timestamptz) string {
	if !value.Valid {
		return ""
	}
	return value.Time.UTC().Format(time.RFC3339Nano)
}

func jsonText(value any) (string, error) {
	if value == nil {
		return "{}", nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return "{}", nil
	}
	return string(raw), nil
}

func parseStoreTime(raw string) *time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05-0700",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			utc := parsed.UTC()
			return &utc
		}
	}
	return nil
}

func clean(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return strings.TrimSpace(fallback)
	}
	return value
}

func durationOr(raw string, fallback time.Duration) time.Duration {
	if parsed, err := time.ParseDuration(strings.TrimSpace(raw)); err == nil && parsed > 0 {
		return parsed
	}
	return fallback
}
