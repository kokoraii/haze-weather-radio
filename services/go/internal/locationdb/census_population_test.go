package locationdb

import (
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestLoadCensusPopulationByCanonicalID(t *testing.T) {
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
			canonical_id TEXT NOT NULL,
			kind TEXT NOT NULL DEFAULT 'administrative_area',
			country TEXT,
			region TEXT NOT NULL DEFAULT 'SK',
			lifecycle_status TEXT,
			attributes_json TEXT
		);
		CREATE TABLE identifiers (
			identifier_pk INTEGER PRIMARY KEY,
			entity_pk INTEGER NOT NULL,
			authority TEXT NOT NULL,
			scheme TEXT NOT NULL,
			value TEXT NOT NULL,
			is_primary INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE names (
			name_pk INTEGER PRIMARY KEY,
			entity_pk INTEGER NOT NULL,
			name TEXT NOT NULL,
			is_primary INTEGER NOT NULL DEFAULT 0
		);
		INSERT INTO entities(entity_pk, canonical_id, country, lifecycle_status, attributes_json) VALUES
			(1, 'urn:test:saskatoon', 'CA', 'active',
			 '{"population":999999,"census_population":266141,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(2, 'urn:test:inactive', 'CA', 'inactive',
			 '{"census_population":1000,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(3, 'urn:test:retired', 'CA', 'retired',
			 '{"census_population":1000,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(4, 'urn:test:decimal', 'CA', 'active',
			 '{"census_population":12.5,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(5, 'urn:test:zero', 'CA', 'active',
			 '{"census_population":0,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(6, 'urn:test:unofficial', 'CA', 'active',
			 '{"census_population":1000,"census_year":2021,"population_source":"other-source"}'),
			(7, 'urn:test:malformed', 'CA', 'active',
			 '{"census_population":1000,"population_source":"statcan-csd-population-2021"'),
			(8, 'urn:test:no-year', 'CA', 'active',
			 '{"census_population":"1234","census_year":"not-a-year","population_source":"STATCAN-CSD-POPULATION-2021"}'),
			(9, 'urn:test:ambiguous', 'CA', 'active',
			 '{"census_population":100,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(10, 'urn:test:ambiguous', 'CA', 'active',
			 '{"census_population":200,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(11, 'urn:test:foreign', 'US', 'active',
			 '{"census_population":1000,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(12, 'urn:test:citypage-saskatoon', 'CA', 'active', '{}'),
			(13, 'urn:test:lakeview-one', 'CA', 'active',
			 '{"census_population":100,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(14, 'urn:test:lakeview-two', 'CA', 'active',
			 '{"census_population":200,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(15, 'urn:test:citypage-lakeview', 'CA', 'active', '{}'),
			(16, 'urn:test:citypage-inactive', 'CA', 'inactive', '{}'),
			(17, 'urn:test:citypage-shared-one', 'CA', 'active', '{}'),
			(18, 'urn:test:citypage-shared-two', 'CA', 'active', '{}'),
			(19, 'urn:test:citypage-wrong-province', 'CA', 'active', '{}'),
			(20, 'urn:test:copied-attrs', 'CA', 'active',
			 '{"census_population":777,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(21, 'urn:test:citypage-nonprimary', 'CA', 'active', '{}'),
			(22, 'urn:test:not-admin', 'CA', 'active',
			 '{"census_population":888,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(23, 'urn:test:alternate-only', 'CA', 'active',
			 '{"census_population":333,"census_year":2021,"population_source":"statcan-csd-population-2021"}'),
			(24, 'urn:test:citypage-alternate-only', 'CA', 'active', '{}'),
			(25, 'urn:test:citypage-alternate-name', 'CA', 'active', '{}');
		UPDATE entities
		SET kind = 'forecast_location'
		WHERE entity_pk IN (12, 15, 16, 17, 18, 19, 21, 24, 25);
		UPDATE entities SET kind = 'air_quality_station' WHERE entity_pk = 22;
		INSERT INTO identifiers(identifier_pk, entity_pk, authority, scheme, value, is_primary) VALUES
			(1, 12, 'eccc', 'eccc_citypage', 'sk-40', 1),
			(2, 15, 'eccc', 'eccc_citypage', 'sk-41', 1),
			(3, 16, 'eccc', 'eccc_citypage', 'sk-42', 1),
			(4, 17, 'eccc', 'eccc_citypage', 'sk-43', 1),
			(5, 18, 'eccc', 'eccc_citypage', 'sk-43', 1),
			(6, 19, 'eccc', 'eccc_citypage', 'on-40', 1),
			(7, 1, 'statcan', 'sgc', '4711066', 1),
			(8, 8, 'statcan', 'sgc_dguid', 'DGUID-NO-YEAR', 1),
			(9, 9, 'statcan', 'sgc', '9999001', 1),
			(10, 10, 'statcan', 'sgc', '9999002', 1),
			(11, 13, 'statcan', 'sgc', '9999003', 1),
			(12, 14, 'statcan', 'sgc', '9999004', 1),
			(13, 21, 'eccc', 'eccc_citypage', 'sk-44', 0),
			(14, 4, 'statcan', 'sgc', '9999005', 1),
			(15, 5, 'statcan', 'sgc', '9999006', 1),
			(16, 7, 'statcan', 'sgc', '9999007', 1),
			(17, 22, 'statcan', 'sgc', '9999008', 1),
			(18, 23, 'statcan', 'sgc', '9999009', 1),
			(19, 24, 'eccc', 'eccc_citypage', 'sk-45', 1),
			(20, 25, 'eccc', 'eccc_citypage', 'sk-46', 1);
		INSERT INTO names(name_pk, entity_pk, name, is_primary) VALUES
			(1, 1, 'City of Saskatoon', 1),
			(2, 8, 'No Year', 1),
			(3, 9, 'Ambiguous Canonical One', 1),
			(4, 10, 'Ambiguous Canonical Two', 1),
			(5, 13, 'Lakeview', 1),
			(6, 14, 'Lakeview', 1),
			(7, 12, 'Saskatoon', 1),
			(8, 15, 'Lakeview', 1),
			(9, 16, 'Saskatoon', 1),
			(10, 17, 'Saskatoon', 1),
			(11, 18, 'Saskatoon', 1),
			(12, 19, 'Saskatoon', 1),
			(13, 21, 'Saskatoon', 1),
			(14, 23, 'Different Place', 1),
			(15, 23, 'Altville', 0),
			(16, 24, 'Altville', 1),
			(17, 25, 'Different Citypage', 1),
			(18, 25, 'Saskatoon', 0);
	`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	catalog, ok := LoadCensusPopulationCatalog(baseDir)
	if !ok {
		t.Fatal("LoadCensusPopulationCatalog returned !ok")
	}
	want := map[string]CensusPopulation{
		"urn:test:saskatoon":      {Population: 266_141, CensusYear: 2021},
		"urn:test:no-year":        {Population: 1_234},
		"urn:test:lakeview-one":   {Population: 100, CensusYear: 2021},
		"urn:test:lakeview-two":   {Population: 200, CensusYear: 2021},
		"urn:test:alternate-only": {Population: 333, CensusYear: 2021},
	}
	if !reflect.DeepEqual(catalog.ByCanonicalID, want) {
		t.Fatalf("canonical populations = %#v, want %#v", catalog.ByCanonicalID, want)
	}
	wantCityPages := map[string]CensusPopulation{
		"sk-40": {Population: 266_141, CensusYear: 2021},
	}
	if !reflect.DeepEqual(catalog.ByCityPageID, wantCityPages) {
		t.Fatalf("city-page populations = %#v, want %#v", catalog.ByCityPageID, wantCityPages)
	}
	if populations, ok := LoadCensusPopulationByCanonicalID(baseDir); !ok || !reflect.DeepEqual(populations, want) {
		t.Fatalf("canonical view = (%#v, %t), want (%#v, true)", populations, ok, want)
	}
	if populations, ok := LoadCensusPopulationByCityPageID(baseDir); !ok || !reflect.DeepEqual(populations, wantCityPages) {
		t.Fatalf("city-page view = (%#v, %t), want (%#v, true)", populations, ok, wantCityPages)
	}
}

func TestLoadCensusPopulationByCanonicalIDUnavailable(t *testing.T) {
	geometryOnlyBase := t.TempDir()
	geometryDir := filepath.Join(geometryOnlyBase, "managed", "locations")
	if err := os.MkdirAll(geometryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	geometryPath := filepath.Join(geometryDir, "ca-weather.geometry.sqlite")
	if err := os.WriteFile(geometryPath, []byte("geometry pack must not be opened"), 0o600); err != nil {
		t.Fatal(err)
	}

	catalog, ok := LoadCensusPopulationCatalog(geometryOnlyBase)
	if ok || catalog.ByCanonicalID != nil || catalog.ByCityPageID != nil {
		t.Fatalf("geometry-only result = (%#v, %t), want (zero, false)", catalog, ok)
	}
	if _, err := os.Stat(geometryPath); err != nil {
		t.Fatalf("geometry pack changed: %v", err)
	}
}

func TestParseStatsCanCensusPopulation(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       CensusPopulation
		ok         bool
	}{
		{
			name:       "valid integer",
			attributes: `{"census_population":42,"census_year":2021,"population_source":"statcan-csd-population-2021"}`,
			want:       CensusPopulation{Population: 42, CensusYear: 2021},
			ok:         true,
		},
		{
			name:       "fractional population",
			attributes: `{"census_population":42.5,"population_source":"statcan-csd-population-2021"}`,
		},
		{
			name:       "scientific notation is not an integer encoding",
			attributes: `{"census_population":1e3,"population_source":"statcan-csd-population-2021"}`,
		},
		{
			name:       "missing official source",
			attributes: `{"census_population":42}`,
		},
		{
			name:       "malformed JSON",
			attributes: `{"census_population":42`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := parseStatsCanCensusPopulation(test.attributes)
			if ok != test.ok || got != test.want {
				t.Fatalf("parseStatsCanCensusPopulation() = (%#v, %t), want (%#v, %t)", got, ok, test.want, test.ok)
			}
		})
	}

	overlong := strings.Repeat("x", maxCensusPopulationAttributeSize+1)
	if _, ok := parseStatsCanCensusPopulation(overlong); ok {
		t.Fatal("oversized attributes were accepted")
	}
}
