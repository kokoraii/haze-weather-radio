package locationdb

import (
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestLoadCoordinatesByIdentifier(t *testing.T) {
	baseDir := t.TempDir()
	coreDir := filepath.Join(baseDir, "managed", "locations")
	if err := os.MkdirAll(coreDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(coreDir, "ca-weather.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE entities (
			entity_pk INTEGER PRIMARY KEY
		);
		CREATE TABLE identifiers (
			identifier_pk INTEGER PRIMARY KEY,
			entity_pk INTEGER NOT NULL,
			scheme TEXT NOT NULL,
			value TEXT NOT NULL
		);
		CREATE TABLE geometries (
			geometry_pk INTEGER PRIMARY KEY,
			entity_pk INTEGER NOT NULL,
			latitude REAL,
			longitude REAL,
			valid_from TEXT,
			is_current INTEGER NOT NULL
		);
		INSERT INTO entities VALUES (1), (2), (3), (4);
		INSERT INTO identifiers VALUES
			(1, 1, 'icao', ' YXE '),
			(2, 2, 'ICAO', 'YQR'),
			(3, 3, 'wmo', '71866'),
			(4, 4, 'icao', 'INVALID');
		INSERT INTO geometries VALUES
			(1, 1, 50.0, -105.0, '2024-01-01', 0),
			(2, 1, 51.0, -106.0, '2020-01-01', 1),
			(3, 1, 52.0, -107.0, '2025-01-01', 1),
			(4, 2, 49.0, -104.0, '2025-01-01', 1),
			(5, 2, 50.0, -105.0, '2025-01-01', 1),
			(6, 3, 53.0, -108.0, '2025-01-01', 1),
			(7, 4, 91.0, -106.0, '2025-01-01', 1);
	`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	coordinates, ok := LoadCoordinatesByIdentifier(baseDir, "icao")
	if !ok {
		t.Fatal("LoadCoordinatesByIdentifier returned !ok")
	}
	want := map[string]Coordinate{
		"yxe": {Latitude: 52, Longitude: -107},
		"yqr": {Latitude: 50, Longitude: -105},
	}
	if len(coordinates) != len(want) {
		t.Fatalf("coordinates = %#v, want %#v", coordinates, want)
	}
	for key, expected := range want {
		if coordinates[key] != expected {
			t.Errorf("coordinate %q = %#v, want %#v", key, coordinates[key], expected)
		}
	}
	if _, exists := coordinates["71866"]; exists {
		t.Fatal("coordinate from a different identifier scheme was loaded")
	}
	if _, exists := coordinates["invalid"]; exists {
		t.Fatal("out-of-range coordinate was loaded")
	}
	coordinates, ok = LoadCoordinatesByIdentifier(baseDir, "icao' OR 1=1 --")
	if !ok || len(coordinates) != 0 {
		t.Fatalf("parameterized scheme query returned %#v, ok=%t", coordinates, ok)
	}
}

func TestLoadCoordinatesByIdentifierUnavailable(t *testing.T) {
	geometryOnlyBase := t.TempDir()
	geometryDir := filepath.Join(geometryOnlyBase, "managed", "locations")
	if err := os.MkdirAll(geometryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(geometryDir, "ca-weather.geometry.sqlite"), []byte("geometry pack must not be opened"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		baseDir string
		scheme  string
	}{
		{name: "missing core pack", baseDir: t.TempDir(), scheme: "icao"},
		{name: "geometry pack is not a fallback", baseDir: geometryOnlyBase, scheme: "icao"},
		{name: "blank scheme", baseDir: t.TempDir(), scheme: "   "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinates, ok := LoadCoordinatesByIdentifier(test.baseDir, test.scheme)
			if ok || coordinates != nil {
				t.Fatalf("LoadCoordinatesByIdentifier() = (%#v, %t), want (nil, false)", coordinates, ok)
			}
		})
	}
}

func TestValidLoadedCoordinate(t *testing.T) {
	tests := []struct {
		name       string
		coordinate Coordinate
		want       bool
	}{
		{name: "origin", coordinate: Coordinate{}, want: true},
		{name: "inclusive bounds", coordinate: Coordinate{Latitude: -90, Longitude: 180}, want: true},
		{name: "latitude too high", coordinate: Coordinate{Latitude: 90.0001}, want: false},
		{name: "latitude too low", coordinate: Coordinate{Latitude: -90.0001}, want: false},
		{name: "longitude too high", coordinate: Coordinate{Longitude: 180.0001}, want: false},
		{name: "longitude too low", coordinate: Coordinate{Longitude: -180.0001}, want: false},
		{name: "NaN latitude", coordinate: Coordinate{Latitude: math.NaN()}, want: false},
		{name: "infinite longitude", coordinate: Coordinate{Longitude: math.Inf(1)}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validLoadedCoordinate(test.coordinate); got != test.want {
				t.Fatalf("validLoadedCoordinate(%#v) = %t, want %t", test.coordinate, got, test.want)
			}
		})
	}
}
