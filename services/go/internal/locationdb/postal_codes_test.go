package locationdb

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	_ "modernc.org/sqlite"
)

func TestPostalCodesByHelloWeatherCodeMatchesExactUniqueCityAndProvince(t *testing.T) {
	path := createPostalCatalogFixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO places(source, code, name, region, country) VALUES
			('hello_weather', '06040', 'City of Saskatoon', 'SK', 'CA'),
			('hello_weather', '03147', 'Montréal', 'QC', 'CA'),
			('hello_weather', '08001', 'Portland', 'BC', 'CA'),
			('hello_weather', '04001', 'Portland', 'ON', 'CA'),
			('hello_weather', '04002', 'Springfield', 'ON', 'CA'),
			('hello_weather', '04003', 'City of Springfield', 'ON', 'CA'),
			('hello_weather', 'US001', 'Saskatoon', 'SK', 'US');
		INSERT INTO postal_links(country, postal_code, city, region) VALUES
			('CA', 'S7K 3J8', 'Saskatoon', 'SK'),
			('CA', 's7k 9z9', 'Saskatoon', 'SK'),
			('CA', 'S0K 1A0', 'Saskatoon', 'SK'),
			('CA', 'S7V', 'Saskatoon', 'SK'),
			('CA', 'SO7 1A1', 'Saskatoon', 'SK'),
			('CA', 'H2X 1Y4', 'Montreal', 'QC'),
			('CA', 'V8V 1A1', 'Portland', 'BC'),
			('CA', 'K1A 0B1', 'Portland', 'ON'),
			('CA', 'K2A 0A1', 'Springfield', 'ON'),
			('US', '90210', 'Saskatoon', 'SK');
	`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	got, ok := postalCodesByHelloWeatherPath(path, 2)
	if !ok {
		t.Fatal("postalCodesByHelloWeatherPath returned !ok")
	}
	want := map[string]PostalCodeSet{
		"06040": {PostalCodes: []string{"S0K", "S7K"}, Truncated: true},
		"03147": {PostalCodes: []string{"H2X"}},
		"08001": {PostalCodes: []string{"V8V"}},
		"04001": {PostalCodes: []string{"K1A"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("postal sets = %#v, want %#v", got, want)
	}
	if _, exists := got["04002"]; exists {
		t.Fatal("ambiguous normalized Hello Weather name was mapped")
	}
	if _, exists := got["04003"]; exists {
		t.Fatal("ambiguous normalized Hello Weather alias was mapped")
	}
}

func TestPostalCodesByHelloWeatherCodeClampsAndSortsOutput(t *testing.T) {
	path := createPostalCatalogFixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO places(source, code, name, region, country) VALUES('hello_weather', '03147', 'Montreal', 'QC', 'CA')`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	statement, err := db.Prepare(`INSERT INTO postal_links(country, postal_code, city, region) VALUES(?, ?, ?, ?)`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	letters := "ABCEGHJKLMNPRSTVWXYZ"
	for index := 0; index < MaxPostalCodesPerLocation+1; index++ {
		fsa := fmt.Sprintf("H%d%c", index/len(letters), letters[index%len(letters)])
		if _, err := statement.Exec("CA", fsa+" 1A1", "Montreal", "QC"); err != nil {
			statement.Close()
			db.Close()
			t.Fatal(err)
		}
	}
	if err := statement.Close(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for _, limit := range []int{0, MaxPostalCodesPerLocation + 100} {
		got, ok := postalCodesByHelloWeatherPath(path, limit)
		if !ok {
			t.Fatalf("postalCodesByHelloWeatherPath(limit=%d) returned !ok", limit)
		}
		set := got["03147"]
		if len(set.PostalCodes) != MaxPostalCodesPerLocation || !set.Truncated {
			t.Fatalf("limit=%d set = %#v, want %d sorted codes and truncation", limit, set, MaxPostalCodesPerLocation)
		}
		for index := 1; index < len(set.PostalCodes); index++ {
			if set.PostalCodes[index-1] >= set.PostalCodes[index] {
				t.Fatalf("postal codes are not strictly sorted: %#v", set.PostalCodes)
			}
		}
	}
}

func TestCanadianPostalFSAValidation(t *testing.T) {
	tests := []struct {
		input string
		want  string
		ok    bool
	}{
		{input: "S7K 3J8", want: "S7K", ok: true},
		{input: " h2x\t1y4 ", want: "H2X", ok: true},
		{input: "V8V", want: "V8V", ok: true},
		{input: "D0A 1K0"},
		{input: "AOE 2V0"},
		{input: "S7K-3J8"},
		{input: "S7K 3J"},
		{input: "90210"},
		{input: ""},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, ok := canadianPostalFSA(test.input)
			if got != test.want || ok != test.ok {
				t.Fatalf("canadianPostalFSA(%q) = (%q, %t), want (%q, %t)", test.input, got, ok, test.want, test.ok)
			}
		})
	}
}

func TestPostalCodesByHelloWeatherCodeMissingCatalogFailsReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.sqlite")
	if got, ok := postalCodesByHelloWeatherPath(path, 10); ok || got != nil {
		t.Fatalf("missing catalog = (%#v, %t), want (nil, false)", got, ok)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("read-only load created the missing catalog: %v", err)
	}
}

func createPostalCatalogFixture(t *testing.T) string {
	t.Helper()
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
			region TEXT NOT NULL,
			country TEXT NOT NULL
		);
		CREATE TABLE postal_links (
			country TEXT NOT NULL,
			postal_code TEXT NOT NULL,
			city TEXT NOT NULL,
			region TEXT NOT NULL
		);
	`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
