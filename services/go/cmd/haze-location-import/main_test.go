package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationimport"
)

func TestRunLegacySplitRollsBackGeometryWhenCoreActivationFails(t *testing.T) {
	for _, hadPreviousGeometry := range []bool{true, false} {
		name := "first_install"
		if hadPreviousGeometry {
			name = "previous_generation"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			legacyPath := filepath.Join(root, "alert_location_map.sqlite")
			coreCandidate := filepath.Join(root, "core-candidate.sqlite")
			geometryCandidate := filepath.Join(root, "geometry-candidate.sqlite")
			geometryActive := filepath.Join(root, "legacy-weather-geometry.sqlite")
			backupDir := filepath.Join(root, "backups")
			writeCombinedLegacyFixture(t, legacyPath)
			legacyBefore, err := os.ReadFile(legacyPath)
			if err != nil {
				t.Fatal(err)
			}
			if hadPreviousGeometry {
				writeMarkerDatabase(t, geometryActive, "previous")
			}

			originalActivate := activateLocationDatabase
			originalRollback := rollbackLocationDatabase
			defer func() {
				activateLocationDatabase = originalActivate
				rollbackLocationDatabase = originalRollback
			}()
			activationCount := 0
			activateLocationDatabase = func(ctx context.Context, activePath, candidatePath, backupPath, generation string, now time.Time) (string, error) {
				activationCount++
				if activationCount == 2 {
					return "", errors.New("injected core activation failure")
				}
				return locationimport.ActivateDatabase(ctx, activePath, candidatePath, backupPath, generation, now)
			}
			rollbackCalled := false
			rollbackLocationDatabase = func(ctx context.Context, activePath, backupPath, backupName string) error {
				rollbackCalled = true
				return locationimport.RollbackDatabaseActivation(ctx, activePath, backupPath, backupName)
			}

			err = runLegacySplit(
				context.Background(), legacyPath, "", coreCandidate, geometryCandidate,
				geometryActive, backupDir, true, time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC),
			)
			if err == nil || !strings.Contains(err.Error(), "injected core activation failure") {
				t.Fatalf("split activation error = %v", err)
			}
			if activationCount != 2 || !rollbackCalled {
				t.Fatalf("activation count = %d, rollback called = %t", activationCount, rollbackCalled)
			}
			legacyAfter, err := os.ReadFile(legacyPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(legacyBefore, legacyAfter) {
				t.Fatal("active combined core changed despite the injected core activation failure")
			}
			if hadPreviousGeometry {
				if got := readMarkerDatabase(t, geometryActive); got != "previous" {
					t.Fatalf("restored geometry marker = %q", got)
				}
			} else if _, err := os.Stat(geometryActive); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("first geometry installation remained after rollback: %v", err)
			}
		})
	}
}

func writeCombinedLegacyFixture(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
		CREATE TABLE places(source TEXT, code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT, kind TEXT, lat REAL, lon REAL, attrs_json TEXT, PRIMARY KEY(source, code));
		CREATE TABLE links(link_type TEXT, from_source TEXT, from_code TEXT, to_source TEXT, to_code TEXT, score REAL, confidence TEXT, distance_km REAL, method TEXT, components_json TEXT);
		CREATE TABLE postal_links(postal TEXT);
		CREATE TABLE station_links(area_source TEXT, area_code TEXT, station_id TEXT, station_name TEXT, distance_km REAL);
		CREATE TABLE area_geometries(
			source TEXT, code TEXT, same_code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT,
			latitude REAL, longitude REAL, min_lon REAL, min_lat REAL, max_lon REAL, max_lat REAL,
			geometry_json TEXT, provider_version TEXT, source_url TEXT, updated_at TEXT,
			PRIMARY KEY(source, code)
		);
		INSERT INTO metadata VALUES('area_geometry_schema', '1');
		INSERT INTO places VALUES('clc','065100','City of Saskatoon','Ville de Saskatoon','SK','CA','land',52.1,-106.6,'{}');
		INSERT INTO area_geometries VALUES(
			'clc','065100','065100','City of Saskatoon','Ville de Saskatoon','SK','CA',
			52.1,-106.6,-107,52,-106,53,
			'{"type":"Polygon","coordinates":[[[-107,52],[-106,52],[-106,53],[-107,52]]]}',
			'6.15.0','https://example.invalid/clc','2026-08-02T00:00:00Z'
		);
	`); err != nil {
		t.Fatal(err)
	}
}

func writeMarkerDatabase(t *testing.T, path, value string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE marker(value TEXT); INSERT INTO marker(value) VALUES(?)", value); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func readMarkerDatabase(t *testing.T, path string) string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value string
	if err := db.QueryRow("SELECT value FROM marker").Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}
