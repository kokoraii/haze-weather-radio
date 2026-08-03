package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLite(ctx context.Context, cfg SQLiteConfig, baseDir string) (*SQLiteStore, error) {
	path := strings.TrimSpace(cfg.Path)
	if path == "" {
		path = filepath.Join("runtime", "state", "haze.db")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(baseDir, path)
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	timeout := durationOr(cfg.BusyTimeout, 30*time.Second)
	db, err := sql.Open("sqlite", sqliteDSN(path, timeout))
	if err != nil {
		return nil, fmt.Errorf("open sqlite datastore: %w", err)
	}
	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 1
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	openCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	store := &SQLiteStore{db: db}
	if err := store.configure(openCtx, timeout); err != nil {
		db.Close()
		return nil, err
	}
	if cfg.AutoMigrate == nil || *cfg.AutoMigrate {
		if err := store.Migrate(openCtx); err != nil {
			db.Close()
			return nil, err
		}
	}
	return store, nil
}

func sqliteDSN(path string, busyTimeout time.Duration) string {
	values := url.Values{}
	values.Add("_pragma", fmt.Sprintf("busy_timeout=%d", int(busyTimeout.Milliseconds())))
	values.Add("_pragma", "journal_mode(WAL)")
	values.Add("_pragma", "synchronous(NORMAL)")
	values.Add("_pragma", "foreign_keys(ON)")
	values.Add("_pragma", "temp_store(MEMORY)")
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + values.Encode()
}

func (s *SQLiteStore) configure(ctx context.Context, busyTimeout time.Duration) error {
	if s == nil || s.db == nil {
		return ErrNotConfigured
	}
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		fmt.Sprintf("PRAGMA busy_timeout=%d", int(busyTimeout.Milliseconds())),
		"PRAGMA foreign_keys=ON",
		"PRAGMA temp_store=MEMORY",
	}
	for _, statement := range pragmas {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure sqlite datastore: %w", err)
		}
	}
	return nil
}

func (s *SQLiteStore) Close() {
	if s != nil && s.db != nil {
		_ = s.db.Close()
	}
}

func (s *SQLiteStore) Migrate(ctx context.Context) error {
	if s == nil || s.db == nil {
		return ErrNotConfigured
	}
	if _, err := s.db.ExecContext(ctx, sqliteSchemaSQL); err != nil {
		return fmt.Errorf("migrate sqlite datastore: %w", err)
	}
	hasCanonicalID := false
	rows, err := s.db.QueryContext(ctx, "PRAGMA table_info(locations_locations)")
	if err != nil {
		return fmt.Errorf("inspect sqlite location schema: %w", err)
	}
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		if name == "canonical_id" {
			hasCanonicalID = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !hasCanonicalID {
		if _, err := s.db.ExecContext(ctx, "ALTER TABLE locations_locations ADD COLUMN canonical_id TEXT"); err != nil {
			return fmt.Errorf("add canonical location ID column: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, "CREATE INDEX IF NOT EXISTS idx_locations_canonical_id ON locations_locations(canonical_id) WHERE canonical_id IS NOT NULL AND canonical_id <> ''"); err != nil {
		return fmt.Errorf("index canonical location IDs: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpsertLocation(ctx context.Context, record LocationRecord) error {
	if s == nil || s.db == nil {
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
	_, err = s.db.ExecContext(ctx, `
INSERT INTO locations_locations (
    canonical_id, source, location_id, kind, name_en, name_fr, station_id, citypage_id, clc,
    latitude, longitude, metadata, last_seen
) VALUES (NULLIF(?,''),?,?,?,?,?,?,?,?,?,?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
ON CONFLICT(source, location_id) DO UPDATE SET
    canonical_id = COALESCE(excluded.canonical_id, locations_locations.canonical_id),
    kind = COALESCE(NULLIF(excluded.kind, ''), locations_locations.kind),
    name_en = COALESCE(NULLIF(excluded.name_en, ''), locations_locations.name_en),
    name_fr = COALESCE(NULLIF(excluded.name_fr, ''), locations_locations.name_fr),
    station_id = COALESCE(NULLIF(excluded.station_id, ''), locations_locations.station_id),
    citypage_id = COALESCE(NULLIF(excluded.citypage_id, ''), locations_locations.citypage_id),
    clc = COALESCE(NULLIF(excluded.clc, ''), locations_locations.clc),
    latitude = COALESCE(excluded.latitude, locations_locations.latitude),
    longitude = COALESCE(excluded.longitude, locations_locations.longitude),
    metadata = excluded.metadata,
    last_seen = strftime('%Y-%m-%dT%H:%M:%fZ','now')`,
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
		return fmt.Errorf("upsert sqlite location %s/%s: %w", source, locationID, err)
	}
	return nil
}

func (s *SQLiteStore) UpsertObservation(ctx context.Context, record ObservationRecord) error {
	if s == nil || s.db == nil {
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
	_, err = s.db.ExecContext(ctx, `
INSERT INTO observations_current (
    source, location_id, station_id, observed_at, payload, updated_at
) VALUES (?,?,?,?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
ON CONFLICT(source, location_id) DO UPDATE SET
    station_id = COALESCE(NULLIF(excluded.station_id, ''), observations_current.station_id),
    observed_at = COALESCE(excluded.observed_at, observations_current.observed_at),
    payload = excluded.payload,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')`,
		source,
		locationID,
		strings.TrimSpace(record.StationID),
		storeTimeText(parseStoreTime(record.ObservedAtRaw)),
		payload,
	)
	if err != nil {
		return fmt.Errorf("upsert sqlite observation %s/%s: %w", source, locationID, err)
	}
	return nil
}

func (s *SQLiteStore) UpsertForecast(ctx context.Context, record ForecastRecord) error {
	if s == nil || s.db == nil {
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
	_, err = s.db.ExecContext(ctx, `
INSERT INTO forecasts_current (
    source, forecast_id, region_id, issued_at, source_updated_at, payload, updated_at
) VALUES (?,?,?,?,?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))
ON CONFLICT(source, forecast_id) DO UPDATE SET
    region_id = COALESCE(NULLIF(excluded.region_id, ''), forecasts_current.region_id),
    issued_at = COALESCE(excluded.issued_at, forecasts_current.issued_at),
    source_updated_at = COALESCE(excluded.source_updated_at, forecasts_current.source_updated_at),
    payload = excluded.payload,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')`,
		source,
		forecastID,
		strings.TrimSpace(record.RegionID),
		storeTimeText(parseStoreTime(record.IssuedAtRaw)),
		storeTimeText(parseStoreTime(record.UpdatedAtRaw)),
		payload,
	)
	if err != nil {
		return fmt.Errorf("upsert sqlite forecast %s/%s: %w", source, forecastID, err)
	}
	return nil
}

func (s *SQLiteStore) ObservationPayload(ctx context.Context, source string, locationID string, target any) (bool, error) {
	return s.loadJSONPayload(ctx, `SELECT payload FROM observations_current WHERE source = ? AND location_id = ?`, source, locationID, target)
}

func (s *SQLiteStore) ForecastPayload(ctx context.Context, source string, forecastID string, target any) (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	source = clean(source, "unknown")
	forecastID = clean(forecastID, "")
	if forecastID == "" {
		return false, nil
	}
	var raw string
	var issuedAt sql.NullString
	var updatedAt sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT payload, issued_at, source_updated_at FROM forecasts_current WHERE source = ? AND forecast_id = ?`, source, forecastID).Scan(&raw, &issuedAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if err := mergeForecastPayloadTimes([]byte(raw), issuedAt.String, updatedAt.String, target); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLiteStore) StoreProductPayload(ctx context.Context, record ProductPayloadRecord) error {
	if s == nil || s.db == nil {
		return nil
	}
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
	_, err = s.db.ExecContext(ctx, `
INSERT INTO products_payloads (kind, source, item_id, payload, updated_at)
VALUES (?,?,?,?,?)
ON CONFLICT(kind, source, item_id) DO UPDATE SET
    payload = excluded.payload,
    updated_at = excluded.updated_at`,
		kind,
		source,
		id,
		payload,
		updatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("store sqlite product payload %s/%s/%s: %w", kind, source, id, err)
	}
	return nil
}

func (s *SQLiteStore) ProductPayload(ctx context.Context, kind string, source string, id string, target any) (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	kind = clean(kind, "unknown")
	source = clean(source, "unknown")
	id = clean(id, "")
	if id == "" {
		return false, nil
	}
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT payload FROM products_payloads WHERE kind = ? AND source = ? AND item_id = ?`, kind, source, id).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLiteStore) StoreTextProduct(ctx context.Context, record TextProductRecord) error {
	if s == nil || s.db == nil {
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
	_, err = s.db.ExecContext(ctx, `
INSERT INTO products_text (source, item_id, body, metadata, updated_at)
VALUES (?,?,?,?,?)
ON CONFLICT(source, item_id) DO UPDATE SET
    body = excluded.body,
    metadata = excluded.metadata,
    updated_at = excluded.updated_at`,
		source,
		id,
		strings.TrimSpace(record.Text),
		metadata,
		updatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("store sqlite text product %s/%s: %w", source, id, err)
	}
	return nil
}

func (s *SQLiteStore) TextProduct(ctx context.Context, source string, id string) (string, bool, error) {
	if s == nil || s.db == nil {
		return "", false, nil
	}
	source = clean(source, "unknown")
	id = clean(id, "")
	if id == "" {
		return "", false, nil
	}
	var body string
	if err := s.db.QueryRowContext(ctx, `SELECT body FROM products_text WHERE source = ? AND item_id = ?`, source, id).Scan(&body); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return body, true, nil
}

func (s *SQLiteStore) loadJSONPayload(ctx context.Context, query string, source string, id string, target any) (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	source = clean(source, "unknown")
	id = clean(id, "")
	if id == "" {
		return false, nil
	}
	var raw string
	if err := s.db.QueryRowContext(ctx, query, source, id).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLiteStore) StoreCAPArchive(ctx context.Context, record CAPArchiveRecord) error {
	if s == nil || s.db == nil {
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.db.ExecContext(ctx, `
INSERT INTO archive_cap_xml (
    alert_id, feed_id, bucket, status, reason, sender, source, sent_at, updated_at,
    expires_at, event, headline, cap_xml_zstd, cap_xml_sha256, original_bytes,
    compressed_bytes, metadata, stored_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(alert_id, feed_id, bucket) DO UPDATE SET
    status = excluded.status,
    reason = excluded.reason,
    sender = excluded.sender,
    source = excluded.source,
    sent_at = COALESCE(excluded.sent_at, archive_cap_xml.sent_at),
    updated_at = COALESCE(excluded.updated_at, archive_cap_xml.updated_at),
    expires_at = COALESCE(excluded.expires_at, archive_cap_xml.expires_at),
    event = COALESCE(NULLIF(excluded.event, ''), archive_cap_xml.event),
    headline = COALESCE(NULLIF(excluded.headline, ''), archive_cap_xml.headline),
    cap_xml_zstd = excluded.cap_xml_zstd,
    cap_xml_sha256 = excluded.cap_xml_sha256,
    original_bytes = excluded.original_bytes,
    compressed_bytes = excluded.compressed_bytes,
    metadata = excluded.metadata,
    stored_at = excluded.stored_at`,
		alertID,
		strings.TrimSpace(record.FeedID),
		clean(record.Bucket, "accepted"),
		clean(record.Status, record.Bucket),
		strings.TrimSpace(record.Reason),
		strings.TrimSpace(record.Sender),
		strings.TrimSpace(record.Source),
		storeTimeText(parseStoreTime(record.SentAtRaw)),
		storeTimeText(parseStoreTime(record.UpdatedAtRaw)),
		storeTimeText(parseStoreTime(record.ExpiresAtRaw)),
		strings.TrimSpace(record.Event),
		strings.TrimSpace(record.Headline),
		archive.Compressed,
		archive.SHA256Hex,
		archive.RawBytes,
		archive.ZstdBytes,
		metadata,
		now,
	)
	if err != nil {
		return fmt.Errorf("store sqlite cap archive %s: %w", alertID, err)
	}
	return nil
}

func (s *SQLiteStore) ListCAPArchives(ctx context.Context, bucket string, since time.Time) ([]StoredCAPArchive, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	bucket = clean(bucket, "")
	if bucket == "" {
		return nil, nil
	}
	query := `
SELECT alert_id, feed_id, bucket, status, reason, sender, source, sent_at, updated_at,
       expires_at, event, headline, cap_xml_zstd, stored_at, metadata
FROM archive_cap_xml
WHERE bucket = ?`
	args := []any{bucket}
	if !since.IsZero() {
		query += " AND stored_at >= ?"
		args = append(args, since.UTC().Format(time.RFC3339Nano))
	}
	query += " ORDER BY stored_at DESC LIMIT 500"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite cap archive %s: %w", bucket, err)
	}
	defer rows.Close()
	out := []StoredCAPArchive{}
	for rows.Next() {
		var record StoredCAPArchive
		var status, reason, sender, source, event, headline sql.NullString
		var sentAt, updatedAt, expiresAt, storedAt, metadataRaw sql.NullString
		var compressed []byte
		if err := rows.Scan(
			&record.AlertID,
			&record.FeedID,
			&record.Bucket,
			&status,
			&reason,
			&sender,
			&source,
			&sentAt,
			&updatedAt,
			&expiresAt,
			&event,
			&headline,
			&compressed,
			&storedAt,
			&metadataRaw,
		); err != nil {
			return nil, err
		}
		record.Status = status.String
		record.Reason = reason.String
		record.Sender = sender.String
		record.Source = source.String
		record.Event = event.String
		record.Headline = headline.String
		raw, err := DecodeCAPXMLArchive(compressed)
		if err != nil {
			return nil, err
		}
		record.RawXML = string(raw)
		record.SentAt = parseStoredTime(sentAt.String)
		record.UpdatedAt = parseStoredTime(updatedAt.String)
		record.ExpiresAt = parseStoredTime(expiresAt.String)
		record.StoredAt = parseStoredTime(storedAt.String)
		if strings.TrimSpace(metadataRaw.String) != "" {
			_ = json.Unmarshal([]byte(metadataRaw.String), &record.Metadata)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLiteStore) DeleteCAPArchive(ctx context.Context, alertID string, feedID string) error {
	if s == nil || s.db == nil {
		return nil
	}
	alertID = clean(alertID, "")
	if alertID == "" {
		return nil
	}
	if strings.TrimSpace(feedID) == "" {
		_, err := s.db.ExecContext(ctx, `DELETE FROM archive_cap_xml WHERE alert_id = ?`, alertID)
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM archive_cap_xml WHERE alert_id = ? AND feed_id = ?`, alertID, strings.TrimSpace(feedID))
	return err
}

func (s *SQLiteStore) DeleteCAPArchiveBucketItem(ctx context.Context, alertID string, feedID string, bucket string) error {
	if s == nil || s.db == nil {
		return nil
	}
	alertID = clean(alertID, "")
	bucket = clean(bucket, "")
	if alertID == "" || bucket == "" {
		return nil
	}
	if strings.TrimSpace(feedID) == "" {
		_, err := s.db.ExecContext(ctx, `DELETE FROM archive_cap_xml WHERE alert_id = ? AND bucket = ?`, alertID, bucket)
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM archive_cap_xml WHERE alert_id = ? AND feed_id = ? AND bucket = ?`, alertID, strings.TrimSpace(feedID), bucket)
	return err
}

func (s *SQLiteStore) ClearCAPArchiveBucket(ctx context.Context, bucket string) error {
	if s == nil || s.db == nil {
		return nil
	}
	bucket = clean(bucket, "")
	if bucket == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM archive_cap_xml WHERE bucket = ?`, bucket)
	return err
}

func (s *SQLiteStore) ClearAllCAPArchives(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM archive_cap_xml`)
	return err
}

func (s *SQLiteStore) ExpireNonCriticalCAPArchives(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
DELETE FROM archive_cap_xml
WHERE bucket = 'expired'
  AND EXISTS (
      SELECT 1 FROM archive_cap_xml accepted
      WHERE accepted.bucket = 'accepted'
        AND accepted.alert_id = archive_cap_xml.alert_id
        AND accepted.feed_id = archive_cap_xml.feed_id
        AND upper(coalesce(accepted.event, '')) NOT IN ('TOR', 'SVR')
  )`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE archive_cap_xml
SET bucket = 'expired',
    status = 'expired',
    reason = 'expired by operator',
    stored_at = ?
WHERE bucket = 'accepted'
  AND upper(coalesce(event, '')) NOT IN ('TOR', 'SVR')`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return tx.Commit()
}

func storeTimeText(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func parseStoredTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}
