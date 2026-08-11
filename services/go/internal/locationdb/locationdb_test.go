package locationdb

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestPlaceCensusPopulation(t *testing.T) {
	tests := []struct {
		name           string
		attrs          map[string]any
		wantPopulation int64
		wantYear       int
	}{
		{name: "JSON number", attrs: map[string]any{"population": float64(12_345), "census_year": float64(2021)}, wantPopulation: 12_345, wantYear: 2021},
		{name: "largest available estimate", attrs: map[string]any{"population": 400, "population_centre_population": "1,250", "census_population": 900}, wantPopulation: 1_250},
		{name: "invalid values ignored", attrs: map[string]any{"population": -1, "census_population": "unknown", "census_year": 2021}, wantYear: 2021},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			population, year := (Place{Attrs: test.attrs}).CensusPopulation()
			if population != test.wantPopulation || year != test.wantYear {
				t.Fatalf("CensusPopulation() = (%d, %d), want (%d, %d)", population, year, test.wantPopulation, test.wantYear)
			}
		})
	}
}

func TestEnrichPopulationFromCorePack(t *testing.T) {
	baseDir := t.TempDir()
	coreDir := filepath.Join(baseDir, "managed", "locations")
	if err := os.MkdirAll(coreDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(coreDir, "ca-weather.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE entities (
			entity_pk INTEGER PRIMARY KEY,
			country TEXT,
			region TEXT,
			attributes_json TEXT NOT NULL
		);
		CREATE TABLE identifiers (
			entity_pk INTEGER NOT NULL,
			authority TEXT NOT NULL,
			scheme TEXT NOT NULL,
			normalized_value TEXT NOT NULL
		);
		CREATE TABLE names (
			entity_pk INTEGER NOT NULL,
			name TEXT NOT NULL,
			normalized_name TEXT NOT NULL
		);
		INSERT INTO entities VALUES
			(1, 'CA', 'SK', '{"population":266141,"census_year":2021,"census_dguid":"2021A00054711066"}'),
			(2, 'CA', 'SK', '{"census_population":1000,"census_year":2021,"census_dguid":"DGUID-LAKEVIEW-1"}'),
			(3, 'CA', 'SK', '{"census_population":500,"census_year":2021,"census_dguid":"DGUID-LAKEVIEW-2"}');
		INSERT INTO identifiers VALUES
			(1, 'statcan', 'sgc_dguid', '2021A00054711066'),
			(1, 'statcan', 'sgc', '4711066'),
			(2, 'statcan', 'sgc_dguid', 'DGUID-LAKEVIEW-1'),
			(3, 'statcan', 'sgc_dguid', 'DGUID-LAKEVIEW-2');
		INSERT INTO names VALUES
			(1, 'Saskatoon', 'saskatoon'),
			(2, 'Lakeview', 'lakeview'),
			(3, 'Lakeview', 'lakeview');
	`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	snapshot := Snapshot{Places: []Place{
		{Source: "forecast", Code: "SK-1", Name: "City of Saskatoon", Region: "SK", Country: "CA"},
		{Source: "forecast", Code: "SK-2", Name: "Lakeview", Region: "SK", Country: "CA", Attrs: map[string]any{"census_dguid": "DGUID-LAKEVIEW-1"}},
		{Source: "hello_weather", Code: "00001", Name: "Lakeview", Region: "SK", Country: "CA"},
		{Source: "sgc", Code: "4711066", Name: "Alternate Saskatoon label", Region: "SK", Country: "CA"},
	}}
	enriched, ok := EnrichPopulationFromCorePack(baseDir, snapshot)
	if !ok {
		t.Fatal("EnrichPopulationFromCorePack did not load the optional core pack")
	}
	tests := []struct {
		name           string
		position       int
		wantPopulation int64
	}{
		{name: "unique normalized name and province", position: 0, wantPopulation: 266_141},
		{name: "exact DGUID overrides ambiguous name", position: 1, wantPopulation: 1_000},
		{name: "ambiguous name and province is not guessed", position: 2, wantPopulation: 0},
		{name: "exact SGC identifier", position: 3, wantPopulation: 266_141},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			population, _ := enriched.Places[test.position].CensusPopulation()
			if population != test.wantPopulation {
				t.Fatalf("population = %d, want %d", population, test.wantPopulation)
			}
		})
	}
}

func TestLoadPathReadsPlacesAndLinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alert_location_map.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE places (
			source TEXT NOT NULL,
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			name_fr TEXT NOT NULL,
			region TEXT NOT NULL,
			country TEXT NOT NULL,
			kind TEXT NOT NULL,
			lat REAL,
			lon REAL,
			attrs_json TEXT NOT NULL,
			PRIMARY KEY (source, code)
		);
		CREATE TABLE links (
			link_type TEXT NOT NULL,
			from_source TEXT NOT NULL,
			from_code TEXT NOT NULL,
			to_source TEXT NOT NULL,
			to_code TEXT NOT NULL,
			score REAL NOT NULL,
			confidence TEXT NOT NULL,
			distance_km REAL,
			method TEXT NOT NULL,
			components_json TEXT NOT NULL
		);
		CREATE TABLE station_links (
			area_source TEXT NOT NULL,
			area_code TEXT NOT NULL,
			station_id TEXT NOT NULL,
			station_name TEXT NOT NULL,
			distance_km REAL NOT NULL,
			PRIMARY KEY (area_source, area_code)
		);
		INSERT INTO places VALUES
			('nws_marine_same', '073531', 'Chesapeake Bay from Pooles Island to Sandy Point MD', '', 'AN', 'US', 'NWS marine SAME zone', 39.1806, -76.3446, '{"zone_ugc":"ANZ531"}'),
			('nws_marine_zone', 'ANZ531', 'Chesapeake Bay from Pooles Island to Sandy Point MD', '', 'AN', 'US', 'NWS marine forecast zone', 39.1806, -76.3446, '{"same":"073531"}');
		INSERT INTO links VALUES
			('nws_marine_same_to_zone', 'nws_marine_same', '073531', 'nws_marine_zone', 'ANZ531', 1.0, 'exact', 0.0, 'fixture', '{}');
		INSERT INTO station_links VALUES
			('nws_marine_zone', 'ANZ531', 'KNAK', 'Annapolis Naval Academy', 18.5);
	`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	snap, ok := LoadPath(path)
	if !ok {
		t.Fatal("LoadPath returned !ok")
	}
	place, ok := snap.Place("nws_marine_same", "073531")
	if !ok || place.Name != "Chesapeake Bay from Pooles Island to Sandy Point MD" {
		t.Fatalf("place = %#v, ok=%v", place, ok)
	}
	if labels := snap.Labels(); labels["ANZ531"] != "Chesapeake Bay from Pooles Island to Sandy Point MD" {
		t.Fatalf("labels = %#v", labels)
	}
	if len(snap.Links) != 1 || snap.Links[0].FromCode != "073531" || snap.Links[0].ToCode != "ANZ531" {
		t.Fatalf("links = %#v", snap.Links)
	}
	if len(snap.StationLinks) != 1 || snap.StationLinks[0].StationID != "KNAK" {
		t.Fatalf("station links = %#v", snap.StationLinks)
	}
}

func TestBundledHelloWeatherDirectoryIsComplete(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "bundle", "managed", "alert_location_map.sqlite")
	snap, ok := LoadPath(path)
	if !ok {
		t.Fatalf("could not load bundled location database at %s", path)
	}
	locations := snap.PlacesBySource("hello_weather")
	if len(locations) != 844 {
		t.Fatalf("hello_weather location count = %d, want 844", len(locations))
	}
	tests := map[string]struct {
		name     string
		province string
	}{
		"08074": {name: "Vancouver", province: "BC"},
		"07052": {name: "Calgary", province: "AB"},
		"06040": {name: "Saskatoon", province: "SK"},
		"05038": {name: "Winnipeg", province: "MB"},
		"04143": {name: "Toronto", province: "ON"},
		"01723": {name: "Saint John", province: "NB"},
		"09524": {name: "Yellowknife", province: "NT"},
		"09821": {name: "Iqaluit", province: "NU"},
	}
	for code, want := range tests {
		place, found := snap.Place("hello_weather", code)
		if !found || place.Name != want.name || place.Region != want.province {
			t.Fatalf("hello_weather %s = %#v, found=%v", code, place, found)
		}
	}
	shared, _ := snap.Place("hello_weather", "02024")
	aliases, ok := shared.Attrs["aliases"].([]any)
	if !ok || len(aliases) == 0 || aliases[0] != "St. John’s" {
		t.Fatalf("shared code aliases = %#v", shared.Attrs["aliases"])
	}
	if got := shared.Aliases(); len(got) == 0 || got[0] != "St. John’s" {
		t.Fatalf("shared code aliases helper = %#v", got)
	}
}
