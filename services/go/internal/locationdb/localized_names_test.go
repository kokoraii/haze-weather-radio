package locationdb

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestLoadLocalizedNamesByIdentifier(t *testing.T) {
	t.Parallel()
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
			entity_pk INTEGER PRIMARY KEY,
			lifecycle_status TEXT
		);
		CREATE TABLE identifiers (
			entity_pk INTEGER NOT NULL,
			scheme TEXT NOT NULL,
			value TEXT NOT NULL
		);
		CREATE TABLE names (
			name_pk INTEGER PRIMARY KEY,
			entity_pk INTEGER NOT NULL,
			locale TEXT,
			name TEXT NOT NULL,
			name_kind TEXT NOT NULL,
			is_primary INTEGER NOT NULL
		);
		INSERT INTO entities VALUES (1, 'active'), (2, 'inactive');
		INSERT INTO identifiers VALUES
			(1, 'eccc_citypage', 'nb-17'),
			(2, 'eccc_citypage', 'on-1');
		INSERT INTO names VALUES
			(1, 1, 'en-CA', 'Shediac', 'canonical', 1),
			(2, 1, 'fr-CA', 'Shédiac', 'canonical', 0),
			(3, 1, 'fr', 'Shediac alternatif', 'alternate', 0),
			(4, 2, 'fr-CA', 'Parc Algonquin', 'canonical', 1);
	`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	names, ok := LoadLocalizedNamesByIdentifier(baseDir, "ECCC_CITYPAGE", "fr")
	if !ok {
		t.Fatal("localized name catalog did not load")
	}
	if names["nb-17"] != "Shédiac" {
		t.Fatalf("French name = %q, want Shédiac", names["nb-17"])
	}
	if _, exists := names["on-1"]; exists {
		t.Fatal("inactive entity was included")
	}
}
