package locationdb

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestLoadHelloWeatherCityPageIDsUsesUniqueNormalizedNameAndProvince(t *testing.T) {
	baseDir := createHelloCityPageCatalogs(t)
	legacyPath := Path(baseDir)
	corePath := filepath.Join(baseDir, populationCoreRelPath)
	oversizedName := strings.Repeat("x", maxHelloBridgeNameLen+1)
	oversizedRegion := strings.Repeat("O", maxHelloBridgeRegionLen+1)
	oversizedIdentifier := "on-" + strings.Repeat("1", maxHelloBridgeCodeLen)

	legacyDB, err := sql.Open("sqlite", legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacyDB.Exec(`
		INSERT INTO places(source, code, name, region, country) VALUES
			('hello_weather', '03001', 'City of Montréal', 'QC', 'CA'),
			('hello_weather', '03009', 'Ville de Québec', 'qc, Canada', 'CA'),
			('hello_weather', '04001', 'London', 'ON', 'CA'),
			('hello_weather', '03002', 'London', 'QC', 'CA'),
			('hello_weather', '01001', 'Springfield', 'NS', 'CA'),
			('hello_weather', '04002', 'Riverview', 'ON', 'CA'),
			('hello_weather', '04003', 'City of Riverview', 'ON', 'CA'),
			('hello_weather', '04004', 'Inactive Place', 'ON', 'CA'),
			('hello_weather', '04005', 'Duplicate Identifier One', 'ON', 'CA'),
			('hello_weather', '04006', 'Duplicate Identifier Two', 'ON', 'CA'),
			('hello_weather', '04007', 'Retired Place', 'ON', 'CA'),
			('hello_weather', 'US001', 'London', 'ON', 'US');
	`)
	if err != nil {
		legacyDB.Close()
		t.Fatal(err)
	}
	if _, err := legacyDB.Exec(`
		INSERT INTO places(source, code, name, region, country) VALUES
			('hello_weather', '04008', ?, 'ON', 'CA'),
			('hello_weather', '04009', 'Oversized Region', ?, 'CA'),
			('hello_weather', '04010', 'Oversized Identifier', 'ON', 'CA')
	`, oversizedName, oversizedRegion); err != nil {
		legacyDB.Close()
		t.Fatal(err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatal(err)
	}

	coreDB, err := sql.Open("sqlite", corePath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = coreDB.Exec(`
		INSERT INTO entities(entity_pk, canonical_id, kind, country, region, lifecycle_status) VALUES
			(1, 'urn:test:1', 'forecast_location', 'CA', 'Greater Montréal forecast region', 'unknown'),
			(2, 'urn:test:2', 'forecast_location', 'CA', 'Québec forecast region', 'unknown'),
			(3, 'urn:test:3', 'forecast_location', 'CA', 'London - Middlesex County', 'unknown'),
			(4, 'urn:test:4', 'forecast_location', 'CA', 'Québec west', 'unknown'),
			(5, 'urn:test:5', 'forecast_location', 'CA', 'Mainland Nova Scotia', 'unknown'),
			(6, 'urn:test:6', 'forecast_location', 'CA', 'Cape Breton', 'unknown'),
			(7, 'urn:test:7', 'forecast_location', 'CA', 'Ontario north', 'inactive'),
			(8, 'urn:test:8', 'forecast_location', 'CA', 'Ontario east', 'unknown'),
			(9, 'urn:test:9', 'forecast_location', 'CA', 'Ontario west', 'unknown'),
			(10, 'urn:test:10', 'forecast_location', 'US', 'Ontario County', 'unknown'),
			(11, 'urn:test:11', 'forecast_location', 'CA', 'Ontario retired', 'retired'),
			(12, 'urn:test:12', 'forecast_location', 'CA', 'Ontario oversized name', 'unknown'),
			(13, 'urn:test:13', 'forecast_location', 'CA', 'Ontario oversized region', 'unknown'),
			(14, 'urn:test:14', 'forecast_location', 'CA', 'Ontario oversized identifier', 'unknown');
		INSERT INTO identifiers(entity_pk, authority, scheme, value, normalized_value) VALUES
			(1, 'eccc', 'eccc_citypage', 'qc-1', 'QC-1'),
			(2, 'eccc', 'eccc_citypage', 'qc-9', 'QC-9'),
			(3, 'eccc', 'eccc_citypage', 'on-1', 'ON-1'),
			(4, 'eccc', 'eccc_citypage', 'qc-2', 'QC-2'),
			(5, 'eccc', 'eccc_citypage', 'ns-1', 'NS-1'),
			(6, 'eccc', 'eccc_citypage', 'ns-2', 'NS-2'),
			(7, 'eccc', 'eccc_citypage', 'on-4', 'ON-4'),
			(8, 'eccc', 'eccc_citypage', 'on-duplicate', 'ON-DUPLICATE'),
			(9, 'eccc', 'eccc_citypage', 'on-duplicate', 'ON-DUPLICATE'),
			(10, 'eccc', 'eccc_citypage', 'on-us', 'ON-US'),
			(11, 'eccc', 'eccc_citypage', 'on-7', 'ON-7'),
			(12, 'eccc', 'eccc_citypage', 'on-8', 'ON-8'),
			(13, 'eccc', 'eccc_citypage', 'on-9', 'ON-9');
		INSERT INTO names(entity_pk, name, normalized_name) VALUES
			(1, 'Montreal', 'montreal'),
			(1, 'Montréal', 'montreal'),
			(2, 'Québec', 'quebec'),
			(3, 'City of London', 'city of london'),
			(4, 'London', 'london'),
			(5, 'Springfield', 'springfield'),
			(6, 'Springfield', 'springfield'),
			(7, 'Inactive Place', 'inactive place'),
			(8, 'Duplicate Identifier One', 'duplicate identifier one'),
			(9, 'Duplicate Identifier Two', 'duplicate identifier two'),
			(10, 'London', 'london'),
			(11, 'Retired Place', 'retired place'),
			(13, 'Oversized Region', 'oversized region'),
			(14, 'Oversized Identifier', 'oversized identifier');
	`)
	if err != nil {
		coreDB.Close()
		t.Fatal(err)
	}
	if _, err := coreDB.Exec(`
		INSERT INTO identifiers(entity_pk, authority, scheme, value, normalized_value)
		VALUES(14, 'eccc', 'eccc_citypage', ?, ?);
		INSERT INTO names(entity_pk, name, normalized_name) VALUES(12, ?, ?)
	`, oversizedIdentifier, strings.ToUpper(oversizedIdentifier), oversizedName, oversizedName); err != nil {
		coreDB.Close()
		t.Fatal(err)
	}
	if err := coreDB.Close(); err != nil {
		t.Fatal(err)
	}

	got, ok := LoadHelloWeatherCityPageIDs(baseDir)
	if !ok {
		t.Fatal("LoadHelloWeatherCityPageIDs returned !ok")
	}
	want := map[string]string{
		"03001": "qc-1",
		"03009": "qc-9",
		"04001": "on-1",
		"03002": "qc-2",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("city-page bridge = %#v, want %#v", got, want)
	}
	for _, code := range []string{"01001", "04002", "04003", "04004", "04005", "04006", "04007", "04008", "04009", "04010", "US001"} {
		if _, exists := got[code]; exists {
			t.Fatalf("unsafe or out-of-scope Hello Weather code %s was mapped", code)
		}
	}
}

func TestLoadHelloWeatherCityPageIDsMissingPackFailsReadOnly(t *testing.T) {
	tests := []struct {
		name         string
		createLegacy bool
		createCore   bool
	}{
		{name: "both missing"},
		{name: "legacy only", createLegacy: true},
		{name: "core only", createCore: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseDir := t.TempDir()
			legacyPath := Path(baseDir)
			corePath := filepath.Join(baseDir, populationCoreRelPath)
			if test.createLegacy {
				createHelloBridgeLegacyCatalog(t, legacyPath)
			}
			if test.createCore {
				createHelloBridgeCoreCatalog(t, corePath)
			}

			got, ok := LoadHelloWeatherCityPageIDs(baseDir)
			if ok || got != nil {
				t.Fatalf("missing pack result = (%#v, %t), want (nil, false)", got, ok)
			}
			if !test.createLegacy {
				if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
					t.Fatalf("read-only load created missing legacy catalog: %v", err)
				}
			}
			if !test.createCore {
				if _, err := os.Stat(corePath); !os.IsNotExist(err) {
					t.Fatalf("read-only load created missing core catalog: %v", err)
				}
			}
		})
	}
}

func TestLoadHelloWeatherCityPageIDsRejectsRowOverflow(t *testing.T) {
	t.Run("legacy rows", func(t *testing.T) {
		baseDir := createHelloCityPageCatalogs(t)
		db, err := sql.Open("sqlite", Path(baseDir))
		if err != nil {
			t.Fatal(err)
		}
		transaction, err := db.Begin()
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		statement, err := transaction.Prepare(`INSERT INTO places(source, code, name, region, country) VALUES(?, ?, ?, ?, ?)`)
		if err != nil {
			transaction.Rollback()
			db.Close()
			t.Fatal(err)
		}
		for index := 0; index <= maxHelloWeatherBridgeRows; index++ {
			if _, err := statement.Exec("hello_weather", fmt.Sprintf("%05d", index), fmt.Sprintf("Place %d", index), "ON", "CA"); err != nil {
				statement.Close()
				transaction.Rollback()
				db.Close()
				t.Fatal(err)
			}
		}
		if err := statement.Close(); err != nil {
			transaction.Rollback()
			db.Close()
			t.Fatal(err)
		}
		if err := transaction.Commit(); err != nil {
			db.Close()
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		if got, ok := LoadHelloWeatherCityPageIDs(baseDir); ok || got != nil {
			t.Fatalf("legacy overflow = (%#v, %t), want (nil, false)", got, ok)
		}
	})

	t.Run("core name rows", func(t *testing.T) {
		baseDir := createHelloCityPageCatalogs(t)
		legacyDB, err := sql.Open("sqlite", Path(baseDir))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := legacyDB.Exec(`INSERT INTO places(source, code, name, region, country) VALUES('hello_weather', '04001', 'Place 0', 'ON', 'CA')`); err != nil {
			legacyDB.Close()
			t.Fatal(err)
		}
		if err := legacyDB.Close(); err != nil {
			t.Fatal(err)
		}

		coreDB, err := sql.Open("sqlite", filepath.Join(baseDir, populationCoreRelPath))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := coreDB.Exec(`
			INSERT INTO entities(entity_pk, canonical_id, kind, country, region, lifecycle_status)
			VALUES(1, 'urn:test:overflow', 'forecast_location', 'CA', 'Ontario', 'unknown');
			INSERT INTO identifiers(entity_pk, authority, scheme, value, normalized_value)
			VALUES(1, 'eccc', 'eccc_citypage', 'on-1', 'ON-1')
		`); err != nil {
			coreDB.Close()
			t.Fatal(err)
		}
		transaction, err := coreDB.Begin()
		if err != nil {
			coreDB.Close()
			t.Fatal(err)
		}
		statement, err := transaction.Prepare(`INSERT INTO names(entity_pk, name, normalized_name) VALUES(?, ?, ?)`)
		if err != nil {
			transaction.Rollback()
			coreDB.Close()
			t.Fatal(err)
		}
		for index := 0; index <= maxCityPageNameRows; index++ {
			name := fmt.Sprintf("Place %d", index)
			if _, err := statement.Exec(1, name, name); err != nil {
				statement.Close()
				transaction.Rollback()
				coreDB.Close()
				t.Fatal(err)
			}
		}
		if err := statement.Close(); err != nil {
			transaction.Rollback()
			coreDB.Close()
			t.Fatal(err)
		}
		if err := transaction.Commit(); err != nil {
			coreDB.Close()
			t.Fatal(err)
		}
		if err := coreDB.Close(); err != nil {
			t.Fatal(err)
		}

		if got, ok := LoadHelloWeatherCityPageIDs(baseDir); ok || got != nil {
			t.Fatalf("core overflow = (%#v, %t), want (nil, false)", got, ok)
		}
	})
}

func createHelloCityPageCatalogs(t *testing.T) string {
	t.Helper()
	baseDir := t.TempDir()
	createHelloBridgeLegacyCatalog(t, Path(baseDir))
	createHelloBridgeCoreCatalog(t, filepath.Join(baseDir, populationCoreRelPath))
	return baseDir
}

func createHelloBridgeLegacyCatalog(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE places (
			source TEXT NOT NULL,
			code TEXT NOT NULL,
			name TEXT NOT NULL,
			region TEXT NOT NULL,
			country TEXT NOT NULL
		);
	`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func createHelloBridgeCoreCatalog(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	schemaPath := filepath.Join("..", "..", "..", "..", "crates", "haze-location", "schema", "v1.sql")
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	_, err = db.Exec(string(schema))
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
