package datastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEncodeCAPXMLArchiveRoundTrips(t *testing.T) {
	raw := []byte(`<alert><identifier>urn:test</identifier><info><event>thunderstorm</event></info></alert>`)

	archive, err := EncodeCAPXMLArchive(raw)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCAPXMLArchive(archive.Compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("decoded archive mismatch: %q", decoded)
	}
	hash := sha256.Sum256(raw)
	if archive.SHA256Hex != hex.EncodeToString(hash[:]) {
		t.Fatalf("sha256 = %q", archive.SHA256Hex)
	}
	if archive.RawBytes != len(raw) || archive.ZstdBytes != len(archive.Compressed) {
		t.Fatalf("sizes = %#v", archive)
	}
}

func TestPostgresConfigResolvesDSNEnv(t *testing.T) {
	t.Setenv("HAZE_TEST_POSTGRES_DSN", "postgres://haze:test@localhost/haze")
	cfg := PostgresConfig{Enabled: true, DSNEnv: "HAZE_TEST_POSTGRES_DSN"}

	if got := cfg.resolvedDSN(); got != os.Getenv("HAZE_TEST_POSTGRES_DSN") {
		t.Fatalf("dsn = %q", got)
	}
}

func TestSQLiteDSNAppliesStartupPragmas(t *testing.T) {
	dsn := sqliteDSN("runtime/state/haze.db", 30*time.Second)
	for _, want := range []string{
		"_pragma=busy_timeout%3D30000",
		"_pragma=journal_mode%28WAL%29",
		"_pragma=synchronous%28NORMAL%29",
		"_pragma=foreign_keys%28ON%29",
		"_pragma=temp_store%28MEMORY%29",
	} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("dsn %q missing %q", dsn, want)
		}
	}

	withExistingQuery := sqliteDSN("runtime/state/haze.db?cache=shared", time.Second)
	if !strings.Contains(withExistingQuery, "?cache=shared&") {
		t.Fatalf("existing query was not preserved: %q", withExistingQuery)
	}
}

func TestSQLiteMigrateConcurrentCanonicalIDUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "haze.db")
	db, err := sql.Open("sqlite", sqliteDSN(path, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE locations_locations (
		source TEXT NOT NULL,
		location_id TEXT NOT NULL,
		kind TEXT NOT NULL DEFAULT 'unknown',
		name_en TEXT,
		name_fr TEXT,
		station_id TEXT,
		citypage_id TEXT,
		clc TEXT,
		latitude REAL,
		longitude REAL,
		metadata TEXT NOT NULL DEFAULT '{}',
		first_seen TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
		last_seen TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
		PRIMARY KEY (source, location_id)
	)`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	start := make(chan struct{})
	errors := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			store, err := OpenSQLite(ctx, SQLiteConfig{
				Path:        path,
				BusyTimeout: "5s",
			}, ".")
			if err == nil {
				store.Close()
			}
			errors <- err
		}()
	}
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent migration: %v", err)
		}
	}

	store, err := OpenSQLite(context.Background(), SQLiteConfig{Path: path}, ".")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	hasCanonicalID, err := store.hasLocationCanonicalID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !hasCanonicalID {
		t.Fatal("canonical_id column is missing after concurrent migration")
	}
}

func TestParseStoreTime(t *testing.T) {
	parsed := parseStoreTime("2026-06-16T15:04:05-06:00")
	if parsed == nil {
		t.Fatal("time did not parse")
	}
	if parsed.UTC().Format(time.RFC3339) != "2026-06-16T21:04:05Z" {
		t.Fatalf("parsed = %s", parsed.UTC().Format(time.RFC3339))
	}
	if parseStoreTime("not a time") != nil {
		t.Fatal("bad time parsed")
	}
}

func TestSQLiteStoreRoundTripsPayloadsAndArchive(t *testing.T) {
	ctx := context.Background()
	store, err := OpenSQLite(ctx, SQLiteConfig{Path: filepath.Join("runtime", "state", "haze.db")}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	observation := map[string]any{"station": map[string]any{"en": "Saskatoon"}, "value": 24}
	if err := store.UpsertObservation(ctx, ObservationRecord{Source: "eccc", LocationID: "sk-40", StationID: "CYXE", ObservedAtRaw: "2026-06-16T21:04:05Z", Payload: observation}); err != nil {
		t.Fatalf("UpsertObservation: %v", err)
	}
	var loadedObservation map[string]any
	ok, err := store.ObservationPayload(ctx, "eccc", "sk-40", &loadedObservation)
	if err != nil || !ok {
		t.Fatalf("ObservationPayload ok=%v err=%v", ok, err)
	}
	if loadedObservation["value"].(float64) != 24 {
		t.Fatalf("loaded observation = %#v", loadedObservation)
	}

	forecast := map[string]any{
		"name":     map[string]any{"en": "Saskatoon"},
		"forecast": []any{map[string]any{"period": map[string]any{"en": "Tonight"}, "textSummary": map[string]any{"en": "Clear."}}},
	}
	if err := store.UpsertForecast(ctx, ForecastRecord{Source: "eccc", ForecastID: "sk-40", RegionID: "06040", IssuedAtRaw: "2026-06-16T21:04:05Z", UpdatedAtRaw: "2026-06-16T22:04:05Z", Payload: forecast}); err != nil {
		t.Fatalf("UpsertForecast: %v", err)
	}
	var loadedForecast map[string]any
	ok, err = store.ForecastPayload(ctx, "eccc", "sk-40", &loadedForecast)
	if err != nil || !ok {
		t.Fatalf("ForecastPayload ok=%v err=%v", ok, err)
	}
	if loadedForecast["issued_at"] != "2026-06-16T21:04:05Z" || loadedForecast["updated_at"] != "2026-06-16T22:04:05Z" {
		t.Fatalf("loaded forecast timestamps = %#v", loadedForecast)
	}

	aqhi := map[string]any{"aqhi": 3, "location": map[string]any{"en": "Saskatoon"}}
	if err := store.StoreProductPayload(ctx, ProductPayloadRecord{Kind: "air_quality", Source: "eccc", ID: "sk-40", Payload: aqhi}); err != nil {
		t.Fatalf("StoreProductPayload: %v", err)
	}
	var loadedAQHI map[string]any
	ok, err = store.ProductPayload(ctx, "air_quality", "eccc", "sk-40", &loadedAQHI)
	if err != nil || !ok {
		t.Fatalf("ProductPayload ok=%v err=%v", ok, err)
	}
	if loadedAQHI["aqhi"].(float64) != 3 {
		t.Fatalf("loaded aqhi = %#v", loadedAQHI)
	}

	if err := store.StoreTextProduct(ctx, TextProductRecord{Source: "nws", ID: "wwv", Text: "Space weather quiet."}); err != nil {
		t.Fatalf("StoreTextProduct: %v", err)
	}
	text, ok, err := store.TextProduct(ctx, "nws", "wwv")
	if err != nil || !ok || text != "Space weather quiet." {
		t.Fatalf("TextProduct text=%q ok=%v err=%v", text, ok, err)
	}

	rawCAP := `<alert><identifier>urn:test:sqlite</identifier><sender>test</sender><sent>2026-06-16T21:04:05Z</sent><status>Actual</status><msgType>Alert</msgType><scope>Public</scope><info><event>thunderstorm</event><headline>test alert</headline></info></alert>`
	if err := store.StoreCAPArchive(ctx, CAPArchiveRecord{AlertID: "urn:test:sqlite", FeedID: "sk-0001", Bucket: "accepted", Status: "accepted", Event: "SVR", RawXML: rawCAP}); err != nil {
		t.Fatalf("StoreCAPArchive: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE archive_cap_xml SET reason = NULL, sender = NULL, source = NULL WHERE alert_id = ?`, "urn:test:sqlite"); err != nil {
		t.Fatalf("force nullable archive fields: %v", err)
	}
	rows, err := store.ListCAPArchives(ctx, "accepted", time.Time{})
	if err != nil {
		t.Fatalf("ListCAPArchives: %v", err)
	}
	if len(rows) != 1 || rows[0].RawXML != rawCAP {
		t.Fatalf("rows = %#v", rows)
	}
}
