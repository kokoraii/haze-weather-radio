package locationimport

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGeometryDatabaseStoresExactWKBAndRTree(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-weather-geometry.sqlite")
	if err := InitializeGeometryDatabase(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	geometry := multiPolygon{
		{{{0, 0}, {2, 0}, {2, 2}, {0, 0}}},
	}
	wkb, err := encodeMultiPolygonWKB(geometry)
	if err != nil {
		t.Fatal(err)
	}
	record := areaRecord{
		Source: "clc", Code: "065100", SameCode: "065100", GeometryType: "multipolygon", GeometryWKB: wkb,
		Latitude: 1, Longitude: 1, MinLon: 0, MaxLon: 2, MinLat: 0, MaxLat: 2,
		ProviderVersion: "6.15.0", SourceURL: "https://example.invalid/clc",
	}
	if _, err := applyRecords(context.Background(), path, "clc", "6.15.0", []areaRecord{record}, time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var canonicalID, geometryType string
	var stored []byte
	if err := db.QueryRow("SELECT canonical_id, geometry_type, geometry_wkb FROM area_geometries WHERE source = 'clc' AND code = '065100'").Scan(&canonicalID, &geometryType, &stored); err != nil {
		t.Fatal(err)
	}
	if canonicalID != "urn:haze:location:eba0b464-9160-5ca7-93ef-2f8cd6969b8d" {
		t.Fatalf("canonical ID = %q", canonicalID)
	}
	if geometryType != "multipolygon" || !bytes.Equal(stored, wkb) {
		t.Fatalf("stored geometry = %q %x", geometryType, stored)
	}
	if prefix := hex.EncodeToString(stored[:minIntForTest(len(stored), 18)]); prefix != "010600000001000000010300000001000000" {
		t.Fatalf("WKB prefix = %s", prefix)
	}
	var indexed int
	if err := db.QueryRow("SELECT COUNT(*) FROM area_geometry_rtree").Scan(&indexed); err != nil || indexed != 1 {
		t.Fatalf("RTree rows = %d, err = %v", indexed, err)
	}
	var placesTable int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'places'").Scan(&placesTable); err != nil {
		t.Fatal(err)
	}
	if placesTable != 0 {
		t.Fatal("geometry-only database unexpectedly contains places")
	}
	for key, want := range map[string]string{
		"pack_id":                               "legacy-weather-geometry",
		"core_pack_id":                          "legacy-alert-locations",
		"pack_kind":                             "geometry",
		"schema_version":                        "1",
		"count.geometries":                      "1",
		"count.source.legacy-area-geometry:clc": "1",
	} {
		var got string
		if err := db.QueryRow("SELECT value FROM catalog_metadata WHERE key = ?", key).Scan(&got); err != nil {
			t.Fatalf("metadata %s: %v", key, err)
		}
		if got != want {
			t.Fatalf("metadata %s = %q, want %q", key, got, want)
		}
	}

	replacement := record
	replacement.Code, replacement.SameCode = "065101", "065101"
	if _, err := applyRecords(context.Background(), path, "clc", "6.15.1", []areaRecord{replacement}, time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	var total int
	if err := db.QueryRow("SELECT COUNT(*) FROM area_geometries WHERE source = 'clc'").Scan(&total); err != nil || total != 1 {
		t.Fatalf("replacement rows = %d, err = %v", total, err)
	}
}

func TestSplitLegacyDatabaseCreatesIndependentCoreAndGeometry(t *testing.T) {
	root := t.TempDir()
	legacyPath := filepath.Join(root, "combined.sqlite")
	corePath := filepath.Join(root, "core.sqlite")
	geometryPath := filepath.Join(root, "geometry.sqlite")
	db, err := sql.Open("sqlite", legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
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
		INSERT INTO metadata VALUES('core_value', 'preserve');
		INSERT INTO places VALUES('clc','065100','City of Saskatoon','Ville de Saskatoon','SK','CA','land',52.1,-106.6,'{}');
		INSERT INTO links VALUES('fixture','clc','065100','forecast','sk-40',1,'exact',0,'fixture','{}');
		INSERT INTO postal_links VALUES('S7K');
		INSERT INTO station_links VALUES('clc','065100','CYXE','Saskatoon Airport',5);
		INSERT INTO area_geometries VALUES(
			'clc','065100','065100','City of Saskatoon','Ville de Saskatoon','SK','CA',
			52.1,-106.6,-107,52,-106,53,
			'{"type":"Polygon","coordinates":[[[-107,52],[-106,52],[-106,53],[-107,52]]]}',
			'6.15.0','https://example.invalid/clc','2026-08-02T00:00:00Z'
		);
	`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	result, err := SplitLegacyDatabase(context.Background(), legacyPath, corePath, geometryPath, time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if result.GeometryCount != 1 || result.SourceCounts["clc"] != 1 {
		t.Fatalf("split result = %#v", result)
	}
	after, err := os.ReadFile(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("legacy input changed during non-destructive split")
	}
	core, err := sql.Open("sqlite", corePath)
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	var areaTable, placeCount int
	if err := core.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'area_geometries'").Scan(&areaTable); err != nil {
		t.Fatal(err)
	}
	if err := core.QueryRow("SELECT COUNT(*) FROM places").Scan(&placeCount); err != nil {
		t.Fatal(err)
	}
	if areaTable != 0 || placeCount != 1 {
		t.Fatalf("stripped core area table = %d, places = %d", areaTable, placeCount)
	}
	geometryDB, err := sql.Open("sqlite", geometryPath)
	if err != nil {
		t.Fatal(err)
	}
	defer geometryDB.Close()
	var geometryType string
	var wkb []byte
	if err := geometryDB.QueryRow("SELECT geometry_type, geometry_wkb FROM area_geometries").Scan(&geometryType, &wkb); err != nil {
		t.Fatal(err)
	}
	if geometryType != "polygon" {
		t.Fatalf("split geometry type = %q", geometryType)
	}
	if parsed, err := validateAreaWKB(wkb); err != nil || parsed != "polygon" {
		t.Fatalf("split WKB = %q, err = %v", parsed, err)
	}
	var coreChecksum string
	if err := geometryDB.QueryRow("SELECT value FROM catalog_metadata WHERE key = 'core_sha256'").Scan(&coreChecksum); err != nil {
		t.Fatal(err)
	}
	wantChecksum, err := databaseSHA256(corePath)
	if err != nil {
		t.Fatal(err)
	}
	if coreChecksum != wantChecksum {
		t.Fatalf("split core checksum = %q, want %q", coreChecksum, wantChecksum)
	}
	if _, err := SplitLegacyDatabase(context.Background(), legacyPath, corePath, filepath.Join(root, "other-geometry.sqlite"), time.Now()); err == nil || !strings.Contains(err.Error(), "core output already exists") {
		t.Fatalf("existing output was not rejected: %v", err)
	}
}

func TestValidateGeometryAgainstLegacyCoreRejectsIncompatibleIdentities(t *testing.T) {
	root := t.TempDir()
	corePath := filepath.Join(root, "core.sqlite")
	geometryPath := filepath.Join(root, "geometry.sqlite")
	core, err := sql.Open("sqlite", corePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.Exec(`
		CREATE TABLE places(source TEXT NOT NULL, code TEXT NOT NULL, PRIMARY KEY(source, code));
		INSERT INTO places VALUES('clc', '065100');
	`); err != nil {
		t.Fatal(err)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitializeGeometryDatabase(context.Background(), geometryPath); err != nil {
		t.Fatal(err)
	}
	wkb, err := encodeMultiPolygonWKB(multiPolygon{{{{-107, 52}, {-106, 52}, {-106, 53}, {-107, 52}}}})
	if err != nil {
		t.Fatal(err)
	}
	record := areaRecord{
		Source: "clc", Code: "065100", SameCode: "065100", GeometryType: "multipolygon", GeometryWKB: wkb,
		Latitude: 52.1, Longitude: -106.6, MinLon: -107, MaxLon: -106, MinLat: 52, MaxLat: 53,
		ProviderVersion: "6.15.0", SourceURL: "https://example.invalid/clc",
	}
	if _, err := applyRecords(context.Background(), geometryPath, "clc", "6.15.0", []areaRecord{record}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := BindGeometryToLegacyCore(context.Background(), geometryPath, corePath); err != nil {
		t.Fatalf("matching core could not be bound: %v", err)
	}
	if err := ValidateGeometryAgainstLegacyCore(context.Background(), geometryPath, corePath); err != nil {
		t.Fatalf("matching core was rejected: %v", err)
	}
	core, err = sql.Open("sqlite", corePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.Exec("CREATE TABLE generation_marker(value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGeometryAgainstLegacyCore(context.Background(), geometryPath, corePath); err == nil || !strings.Contains(err.Error(), "different paired core file generation") {
		t.Fatalf("stale core checksum error = %v", err)
	}
	if err := BindGeometryToLegacyCore(context.Background(), geometryPath, corePath); err != nil {
		t.Fatalf("rebinding unchanged identities failed: %v", err)
	}
	core, err = sql.Open("sqlite", corePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.Exec("DELETE FROM places; INSERT INTO places VALUES('clc', '065101')"); err != nil {
		t.Fatal(err)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
	if err := BindGeometryToLegacyCore(context.Background(), geometryPath, corePath); err == nil || !strings.Contains(err.Error(), "absent") {
		t.Fatalf("orphan geometry error = %v", err)
	}
	activePath := filepath.Join(root, "active-geometry.sqlite")
	writeGenerationDatabase(t, activePath, "previous")
	if _, err := ActivateGeometryDatabase(
		context.Background(), activePath, geometryPath, corePath, filepath.Join(root, "backups"), "fixture", time.Now(),
	); err == nil || !strings.Contains(err.Error(), "absent") {
		t.Fatalf("orphan geometry activation error = %v", err)
	}
	if got := readGenerationDatabase(t, activePath); got != "previous" {
		t.Fatalf("active geometry changed after rejected activation: %q", got)
	}
	if _, err := os.Stat(geometryPath); err != nil {
		t.Fatalf("rejected geometry candidate was consumed: %v", err)
	}
	geometry, err := sql.Open("sqlite", geometryPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := geometry.Exec("UPDATE area_geometries SET canonical_id = 'urn:haze:location:wrong'"); err != nil {
		t.Fatal(err)
	}
	if err := geometry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGeometryAgainstLegacyCore(context.Background(), geometryPath, corePath); err == nil || !strings.Contains(err.Error(), "canonical ID") {
		t.Fatalf("mismatched canonical ID error = %v", err)
	}
}

func TestMergeLegacyGeometryPlacesPreservesCoreBaseAndAddsOnlyMissingRows(t *testing.T) {
	root := t.TempDir()
	corePath := filepath.Join(root, "core-base.sqlite")
	combinedPath := filepath.Join(root, "combined.sqlite")
	core, err := sql.Open("sqlite", corePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.Exec(`
		CREATE TABLE places(source TEXT, code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT, kind TEXT, lat REAL, lon REAL, attrs_json TEXT, PRIMARY KEY(source, code));
		INSERT INTO places VALUES('clc','065100','Live Saskatoon','Saskatoon direct','SK','CA','land',52.2,-106.7,'{"live":true}');
		INSERT INTO places VALUES('live','only','Live only',NULL,'SK','CA','land',50,-105,'{}');
	`); err != nil {
		t.Fatal(err)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
	combined, err := sql.Open("sqlite", combinedPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := combined.Exec(`
		CREATE TABLE places(source TEXT, code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT, kind TEXT, lat REAL, lon REAL, attrs_json TEXT, PRIMARY KEY(source, code));
		CREATE TABLE area_geometries(source TEXT, code TEXT);
		INSERT INTO places VALUES('clc','065100','Bundled Saskatoon',NULL,'SK','CA','land',52.1,-106.6,'{}');
		INSERT INTO places VALUES('clc','065101','Missing geometry place',NULL,'SK','CA','land',51,-106,'{}');
		INSERT INTO places VALUES('text','only','Not geometry backed',NULL,'SK','CA','land',49,-104,'{}');
		INSERT INTO area_geometries VALUES('clc','065100');
		INSERT INTO area_geometries VALUES('clc','065101');
	`); err != nil {
		t.Fatal(err)
	}
	if err := combined.Close(); err != nil {
		t.Fatal(err)
	}
	added, err := mergeLegacyGeometryPlaces(context.Background(), corePath, combinedPath)
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 {
		t.Fatalf("added places = %d, want 1", added)
	}
	core, err = sql.Open("sqlite", corePath)
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	var existingName, addedName string
	if err := core.QueryRow("SELECT name FROM places WHERE source='clc' AND code='065100'").Scan(&existingName); err != nil {
		t.Fatal(err)
	}
	if err := core.QueryRow("SELECT name FROM places WHERE source='clc' AND code='065101'").Scan(&addedName); err != nil {
		t.Fatal(err)
	}
	if existingName != "Live Saskatoon" || addedName != "Missing geometry place" {
		t.Fatalf("merged names = existing %q, added %q", existingName, addedName)
	}
	var textOnly int
	if err := core.QueryRow("SELECT COUNT(*) FROM places WHERE source='text' AND code='only'").Scan(&textOnly); err != nil {
		t.Fatal(err)
	}
	if textOnly != 0 {
		t.Fatal("non-geometry place was copied from the combined database")
	}
}

func minIntForTest(left int, right int) int {
	if left < right {
		return left
	}
	return right
}

func TestActivateDatabaseRetainsPriorGenerationAndRemovesDetachedSidecars(t *testing.T) {
	root := t.TempDir()
	active := filepath.Join(root, "alert_location_map.sqlite")
	candidate := filepath.Join(root, "candidate.sqlite")
	for path, value := range map[string]string{active: "old", candidate: "new"} {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("CREATE TABLE generation(value TEXT); INSERT INTO generation VALUES(?)", value); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(active+"-wal", []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(active+"-shm", []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	backupName, err := ActivateDatabase(context.Background(), active, candidate, filepath.Join(root, "backups"), "6.15.0", time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(active + suffix); !os.IsNotExist(err) {
			t.Fatalf("active sidecar %s was retained", suffix)
		}
	}
	readGeneration := func(path string) string {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var value string
		if err := db.QueryRow("SELECT value FROM generation").Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if value := readGeneration(active); value != "new" {
		t.Fatalf("active generation = %q", value)
	}
	if value := readGeneration(filepath.Join(root, "backups", backupName)); value != "old" {
		t.Fatalf("backup generation = %q", value)
	}
}

func TestActivateDatabaseSupportsFirstGeometryInstallation(t *testing.T) {
	root := t.TempDir()
	active := filepath.Join(root, "legacy-weather-geometry.sqlite")
	candidate := filepath.Join(root, "candidate.sqlite")
	db, err := sql.Open("sqlite", candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE generation(value TEXT); INSERT INTO generation VALUES('first')"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	backupName, err := ActivateDatabase(
		context.Background(), active, candidate, filepath.Join(root, "backups"),
		"first", time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if backupName != "" {
		t.Fatalf("first installation backup = %q", backupName)
	}
	if _, err := os.Stat(active); err != nil {
		t.Fatalf("active database was not installed: %v", err)
	}
	if _, err := os.Stat(candidate); !os.IsNotExist(err) {
		t.Fatalf("candidate still exists after activation: %v", err)
	}
}

func TestRollbackDatabaseActivationRestoresPreviousOrRemovesFirstInstall(t *testing.T) {
	for _, hadPrevious := range []bool{true, false} {
		t.Run(fmt.Sprintf("previous_%t", hadPrevious), func(t *testing.T) {
			root := t.TempDir()
			active := filepath.Join(root, "geometry.sqlite")
			candidate := filepath.Join(root, "candidate.sqlite")
			writeGenerationDatabase(t, candidate, "new")
			if hadPrevious {
				writeGenerationDatabase(t, active, "old")
			}
			backupDir := filepath.Join(root, "backups")
			backupName, err := ActivateDatabase(context.Background(), active, candidate, backupDir, "test", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if err := RollbackDatabaseActivation(context.Background(), active, backupDir, backupName); err != nil {
				t.Fatal(err)
			}
			if !hadPrevious {
				if _, err := os.Stat(active); !os.IsNotExist(err) {
					t.Fatalf("first installation remained after rollback: %v", err)
				}
				return
			}
			if got := readGenerationDatabase(t, active); got != "old" {
				t.Fatalf("restored generation = %q", got)
			}
		})
	}
}

func writeGenerationDatabase(t *testing.T, path string, value string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE generation(value TEXT); INSERT INTO generation VALUES(?)", value); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func readGenerationDatabase(t *testing.T, path string) string {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value string
	if err := db.QueryRow("SELECT value FROM generation").Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestValidateArchiveRejectsTraversalAndSymlink(t *testing.T) {
	for name, configure := range map[string]func(*zip.Writer) error{
		"traversal": func(writer *zip.Writer) error {
			file, err := writer.Create("../outside.shp")
			if err != nil {
				return err
			}
			_, err = file.Write([]byte("shape"))
			return err
		},
		"symlink": func(writer *zip.Writer) error {
			header := &zip.FileHeader{Name: "link.shp"}
			header.SetMode(os.ModeSymlink | 0o777)
			file, err := writer.CreateHeader(header)
			if err != nil {
				return err
			}
			_, err = file.Write([]byte("target"))
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			archivePath := filepath.Join(t.TempDir(), "MSC_Geography_Pkg_V6_15_0_Land_Unproj.zip")
			file, err := os.Create(archivePath)
			if err != nil {
				t.Fatal(err)
			}
			writer := zip.NewWriter(file)
			if err := configure(writer); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := validateArchive(archivePath, filepath.Base(archivePath), true); err == nil {
				t.Fatal("unsafe archive was accepted")
			}
		})
	}
}

func TestNWSVersionSupportsOperationalFilename(t *testing.T) {
	version, err := nwsVersion("c_16ap26.zip")
	if err != nil {
		t.Fatal(err)
	}
	if version != "2026-04-16" {
		t.Fatalf("version = %q", version)
	}
}

func TestOrganizeRingsAssignsHoleWithoutTrustingWinding(t *testing.T) {
	exterior := [][]float64{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}
	hole := [][]float64{{2, 2}, {2, 4}, {4, 4}, {4, 2}, {2, 2}}
	polygons := organizeRings([][][]float64{hole, exterior})
	if len(polygons) != 1 || len(polygons[0]) != 2 {
		t.Fatalf("polygons = %#v", polygons)
	}
	if ringArea(polygons[0][0]) <= 0 {
		t.Fatal("exterior ring is not counter-clockwise")
	}
	if ringArea(polygons[0][1]) >= 0 {
		t.Fatal("hole ring is not clockwise")
	}
}
