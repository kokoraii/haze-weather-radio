package ivr

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

func TestLocationCodesReturnsSortedPaginatedHelloWeatherDirectory(t *testing.T) {
	t.Parallel()
	service := locationCodeTestService()
	request := httptest.NewRequest(http.MethodGet, "/ivr/v1/location-codes?limit=2", nil)
	response := httptest.NewRecorder()

	service.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "public, max-age=300" {
		t.Fatalf("Cache-Control = %q", got)
	}
	var page telephoneLocationCodePage
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Source != "hello_weather" || page.Total != 3 || len(page.Locations) != 2 {
		t.Fatalf("unexpected page: %#v", page)
	}
	if page.Locations[0].Code != "03010" || page.Locations[0].DialCode != "310" || page.Locations[0].RegionalID != "10" {
		t.Fatalf("first location = %#v", page.Locations[0])
	}
	if page.Locations[1].Code != "04143" || page.Locations[1].DialCode != "4143" || page.Locations[1].RegionalID != "143" {
		t.Fatalf("second location = %#v", page.Locations[1])
	}
	if page.Locations[0].NameFR != "Ville de Québec" || page.Locations[1].NameFR != "Toronto" {
		t.Fatalf("French names or fallback names are missing: %#v", page.Locations)
	}
	if page.Locations[1].Population != 2_794_356 || page.Locations[1].CensusYear != 2021 {
		t.Fatalf("Toronto census data = (%d, %d)", page.Locations[1].Population, page.Locations[1].CensusYear)
	}
	if page.NextOffset == nil || *page.NextOffset != 2 {
		t.Fatalf("next offset = %#v", page.NextOffset)
	}
}

func TestLocationCodesCORSAllowsWebsiteAndLocalDevelopmentOrigins(t *testing.T) {
	tests := []struct {
		name          string
		method        string
		origin        string
		wantStatus    int
		wantAllow     string
		wantMethods   string
		wantAllowHead string
	}{
		{
			name:          "website get",
			method:        http.MethodGet,
			origin:        "https://teleweather.ca",
			wantStatus:    http.StatusOK,
			wantAllow:     "https://teleweather.ca",
			wantMethods:   http.MethodGet,
			wantAllowHead: "Accept",
		},
		{
			name:          "local preflight",
			method:        http.MethodOptions,
			origin:        "http://localhost:4321",
			wantStatus:    http.StatusNoContent,
			wantAllow:     "http://localhost:4321",
			wantMethods:   http.MethodGet,
			wantAllowHead: "Accept",
		},
		{
			name:       "unknown origin",
			method:     http.MethodGet,
			origin:     "https://example.com",
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown preflight",
			method:     http.MethodOptions,
			origin:     "https://example.com",
			wantStatus: http.StatusForbidden,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := locationCodeTestService()
			request := httptest.NewRequest(test.method, "/ivr/v1/location-codes", nil)
			request.Header.Set("Origin", test.origin)
			response := httptest.NewRecorder()

			service.routes().ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.wantStatus, response.Body.String())
			}
			if got := response.Header().Get("Access-Control-Allow-Origin"); got != test.wantAllow {
				t.Fatalf("allow origin = %q, want %q", got, test.wantAllow)
			}
			if got := response.Header().Get("Access-Control-Allow-Methods"); got != test.wantMethods {
				t.Fatalf("allow methods = %q, want %q", got, test.wantMethods)
			}
			if got := response.Header().Get("Access-Control-Allow-Headers"); got != test.wantAllowHead {
				t.Fatalf("allow headers = %q, want %q", got, test.wantAllowHead)
			}
		})
	}
}

func TestLocationCodesReturnEntireDirectoryByDefault(t *testing.T) {
	t.Parallel()
	service := locationCodeTestService()
	page := locationCodePageForTest(t, service, "/ivr/v1/location-codes")
	if page.Total != 3 || len(page.Locations) != 3 || page.Limit != 0 || page.NextOffset != nil {
		t.Fatalf("default response was unexpectedly paginated: %#v", page)
	}
}

func TestLocationCodesOnlyExposeDirectFeedCoverage(t *testing.T) {
	t.Parallel()
	service := locationCodeTestService()

	covered := locationCodePageForTest(t, service, "/ivr/v1/location-codes?q=saskatoon")
	if covered.Total != 1 || len(covered.Locations) != 1 {
		t.Fatalf("unexpected Saskatoon page: %#v", covered)
	}
	saskatoon := covered.Locations[0]
	if saskatoon.FeedID != "sk-0001" || !saskatoon.Covered || saskatoon.RegionalID != "40" || saskatoon.DialCode != "640" {
		t.Fatalf("covered location = %#v", saskatoon)
	}

	uncovered := locationCodePageForTest(t, service, "/ivr/v1/location-codes?q=toronto")
	if uncovered.Total != 1 || len(uncovered.Locations) != 1 {
		t.Fatalf("unexpected Toronto page: %#v", uncovered)
	}
	toronto := uncovered.Locations[0]
	if toronto.FeedID != "" || toronto.Covered || toronto.Language != "" || toronto.Timezone != "" {
		t.Fatalf("uncovered location inherited a rendering feed: %#v", toronto)
	}
}

func TestLocationCodesExposeValidatedNumericCoordinates(t *testing.T) {
	t.Parallel()
	service := locationCodeTestService()

	saskatoon := locationCodePageForTest(t, service, "/ivr/v1/location-codes?q=saskatoon").Locations[0]
	if saskatoon.Latitude == nil || *saskatoon.Latitude != 52.1332 || saskatoon.Longitude == nil || *saskatoon.Longitude != -106.67 {
		t.Fatalf("Saskatoon coordinates = %#v, %#v", saskatoon.Latitude, saskatoon.Longitude)
	}

	toronto := locationCodePageForTest(t, service, "/ivr/v1/location-codes?q=toronto").Locations[0]
	if toronto.Latitude != nil || toronto.Longitude != nil {
		t.Fatalf("invalid Toronto coordinates were exposed: %#v, %#v", toronto.Latitude, toronto.Longitude)
	}

	request := httptest.NewRequest(http.MethodGet, "/ivr/v1/location-codes?q=saskatoon", nil)
	response := httptest.NewRecorder()
	service.routes().ServeHTTP(response, request)
	var payload struct {
		Locations []map[string]any `json:"locations"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload.Locations[0]["latitude"].(float64); !ok {
		t.Fatalf("latitude JSON value is not numeric: %#v", payload.Locations[0]["latitude"])
	}
	if _, ok := payload.Locations[0]["longitude"].(float64); !ok {
		t.Fatalf("longitude JSON value is not numeric: %#v", payload.Locations[0]["longitude"])
	}

	request = httptest.NewRequest(http.MethodGet, "/ivr/v1/location-codes?q=toronto", nil)
	response = httptest.NewRecorder()
	service.routes().ServeHTTP(response, request)
	payload.Locations = nil
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if _, exists := payload.Locations[0]["latitude"]; exists {
		t.Fatalf("invalid latitude was serialized: %#v", payload.Locations[0])
	}
	if _, exists := payload.Locations[0]["longitude"]; exists {
		t.Fatalf("invalid longitude was serialized: %#v", payload.Locations[0])
	}
}

func TestLocationCodesExposeCensusPopulationOnlyWhenAvailable(t *testing.T) {
	t.Parallel()
	service := locationCodeTestService()

	request := httptest.NewRequest(http.MethodGet, "/ivr/v1/location-codes?q=saskatoon", nil)
	response := httptest.NewRecorder()
	service.routes().ServeHTTP(response, request)
	var populationPayload struct {
		Locations []map[string]any `json:"locations"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &populationPayload); err != nil {
		t.Fatal(err)
	}
	if len(populationPayload.Locations) != 1 {
		t.Fatalf("population locations = %#v", populationPayload.Locations)
	}
	if populationPayload.Locations[0]["population"] != float64(266_141) || populationPayload.Locations[0]["census_year"] != float64(2021) {
		t.Fatalf("Saskatoon census JSON = %#v", populationPayload.Locations[0])
	}

	request = httptest.NewRequest(http.MethodGet, "/ivr/v1/location-codes?q=quebec", nil)
	response = httptest.NewRecorder()
	service.routes().ServeHTTP(response, request)
	populationPayload.Locations = nil
	if err := json.Unmarshal(response.Body.Bytes(), &populationPayload); err != nil {
		t.Fatal(err)
	}
	if len(populationPayload.Locations) != 1 {
		t.Fatalf("missing-population locations = %#v", populationPayload.Locations)
	}
	if _, exists := populationPayload.Locations[0]["population"]; exists {
		t.Fatalf("missing population was serialized: %#v", populationPayload.Locations[0])
	}
	if _, exists := populationPayload.Locations[0]["census_year"]; exists {
		t.Fatalf("missing census year was serialized: %#v", populationPayload.Locations[0])
	}
}

func TestCensusPopulationForCityPageUsesOnlyCatalogMatches(t *testing.T) {
	t.Parallel()
	catalog := locationdb.CensusPopulationCatalog{ByCityPageID: map[string]locationdb.CensusPopulation{
		"sk-40": {Population: 266_141, CensusYear: 2021},
	}}
	population, year := censusPopulationForCityPage(catalog, " SK-40 ")
	if population != 266_141 || year != 2021 {
		t.Fatalf("city-page census data = (%d, %d)", population, year)
	}
	population, year = censusPopulationForCityPage(catalog, "sk-unknown")
	if population != 0 || year != 0 {
		t.Fatalf("unknown city-page census data = (%d, %d)", population, year)
	}
}

func TestLocationCodesExposeAndSearchBoundedPostalFSAs(t *testing.T) {
	t.Parallel()
	service := locationCodeTestService()

	page := locationCodePageForTest(t, service, "/ivr/v1/location-codes?q=s7k")
	if page.Total != 1 || len(page.Locations) != 1 {
		t.Fatalf("postal search page = %#v", page)
	}
	location := page.Locations[0]
	if location.Code != "06040" || location.PostalCodeFormat != "fsa" || location.PostalCodesTruncated {
		t.Fatalf("postal metadata = %#v", location)
	}
	want := []string{"S7H", "S7J", "S7K"}
	if len(location.PostalCodes) != len(want) {
		t.Fatalf("postal codes = %#v, want %#v", location.PostalCodes, want)
	}
	for index := range want {
		if location.PostalCodes[index] != want[index] {
			t.Fatalf("postal codes = %#v, want %#v", location.PostalCodes, want)
		}
	}

	toronto := locationCodePageForTest(t, service, "/ivr/v1/location-codes?q=toronto").Locations[0]
	if !toronto.PostalCodesTruncated || toronto.PostalCodeFormat != "fsa" {
		t.Fatalf("Toronto postal metadata = %#v", toronto)
	}
}

func TestLocationCodeCoordinateRejectsNonFiniteAndOutOfRangeValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		minimum float64
		maximum float64
		want    *float64
	}{
		{name: "valid", raw: " 52.1332 ", minimum: -90, maximum: 90, want: floatPointerForTest(52.1332)},
		{name: "lower boundary", raw: "-180", minimum: -180, maximum: 180, want: floatPointerForTest(-180)},
		{name: "upper boundary", raw: "180", minimum: -180, maximum: 180, want: floatPointerForTest(180)},
		{name: "empty", raw: "", minimum: -90, maximum: 90},
		{name: "malformed", raw: "north", minimum: -90, maximum: 90},
		{name: "nan", raw: "NaN", minimum: -90, maximum: 90},
		{name: "positive infinity", raw: "+Inf", minimum: -90, maximum: 90},
		{name: "negative infinity", raw: "-Inf", minimum: -90, maximum: 90},
		{name: "latitude out of range", raw: "90.1", minimum: -90, maximum: 90},
		{name: "longitude out of range", raw: "-180.1", minimum: -180, maximum: 180},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := locationCodeCoordinate(test.raw, test.minimum, test.maximum)
			if test.want == nil {
				if got != nil {
					t.Fatalf("coordinate = %v, want nil", *got)
				}
				return
			}
			if got == nil || *got != *test.want {
				t.Fatalf("coordinate = %#v, want %v", got, *test.want)
			}
		})
	}
}

func floatPointerForTest(value float64) *float64 {
	return &value
}

func TestLocationCodesFiltersProvinceAndAccentInsensitiveQuery(t *testing.T) {
	t.Parallel()
	service := locationCodeTestService()
	request := httptest.NewRequest(http.MethodGet, "/ivr/v1/location-codes?province=Quebec&q=ville%20de%20quebec", nil)
	response := httptest.NewRecorder()

	service.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var page telephoneLocationCodePage
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || len(page.Locations) != 1 || page.Locations[0].NameFR != "Ville de Québec" {
		t.Fatalf("unexpected filtered page: %#v", page)
	}
}

func TestLocationCodesRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
		url    string
		status int
	}{
		{name: "method", method: http.MethodPost, url: "/ivr/v1/location-codes", status: http.StatusMethodNotAllowed},
		{name: "source", method: http.MethodGet, url: "/ivr/v1/location-codes?source=census", status: http.StatusBadRequest},
		{name: "province", method: http.MethodGet, url: "/ivr/v1/location-codes?province=ZZ", status: http.StatusBadRequest},
		{name: "limit", method: http.MethodGet, url: "/ivr/v1/location-codes?limit=501", status: http.StatusBadRequest},
		{name: "offset", method: http.MethodGet, url: "/ivr/v1/location-codes?offset=-1", status: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := locationCodeTestService()
			request := httptest.NewRequest(test.method, test.url, nil)
			response := httptest.NewRecorder()
			service.routes().ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d, body = %s", response.Code, test.status, response.Body.String())
			}
		})
	}
}

func TestHelloWeatherDialCode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code string
		want string
	}{
		{code: "06040", want: "640"},
		{code: "04143", want: "4143"},
		{code: "01723", want: "1723"},
		{code: "invalid", want: ""},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			if got := helloWeatherDialCode(test.code); got != test.want {
				t.Fatalf("helloWeatherDialCode(%q) = %q, want %q", test.code, got, test.want)
			}
		})
	}
}

func TestHelloWeatherRegionalID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code string
		want string
	}{
		{code: "06040", want: "40"},
		{code: "04143", want: "143"},
		{code: "01001", want: "1"},
		{code: "09000", want: "0"},
		{code: "invalid", want: ""},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			if got := helloWeatherRegionalID(test.code); got != test.want {
				t.Fatalf("helloWeatherRegionalID(%q) = %q, want %q", test.code, got, test.want)
			}
		})
	}
}

func locationCodePageForTest(t *testing.T, service *Service, target string) telephoneLocationCodePage {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, target, nil)
	response := httptest.NewRecorder()
	service.routes().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var page telephoneLocationCodePage
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func locationCodeTestService() *Service {
	feed := feedXML{ID: "sk-0001", EnabledRaw: "true", Timezone: "America/Regina"}
	feed.Locations.Coverage.Regions = []coverageRegionXML{{
		ID:             "06040",
		Name:           "Saskatoon",
		DeriveForecast: "sk-40",
	}}
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		HelloWeather: map[string]locationRecord{
			"06040": {Name: "Saskatoon", Province: "SK", Forecast: "sk-40", Latitude: "52.1332", Longitude: "-106.67", Population: 266_141, CensusYear: 2021},
			"04143": {Name: "Toronto", Province: "ON", Forecast: "on-143", Latitude: "NaN", Longitude: "181", Population: 2_794_356, CensusYear: 2021},
			"03010": {Name: "Quebec City", NameFR: "Ville de Québec", Province: "QC", Forecast: "qc-10"},
			"bad":   {Name: "Not a telephone code", Province: "SK"},
		},
		HelloPostalCodes: map[string]locationdb.PostalCodeSet{
			"06040": {PostalCodes: []string{"S7H", "S7J", "S7K"}},
			"04143": {PostalCodes: []string{"M4C", "M5A"}, Truncated: true},
		},
		Feeds: []feedXML{feed},
	}
	return &Service{cfg: cfg, resolver: NewResolver(cfg)}
}
