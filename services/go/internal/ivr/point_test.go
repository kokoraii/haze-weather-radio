package ivr

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

func TestPointRoutesReturnBoundedSanitizedCanonicalResults(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"/point", "/ivr/v1/point"} {
		t.Run(path, func(t *testing.T) {
			queryCalls := 0
			service := &Service{
				pointSlots: make(chan struct{}, 1),
				pointQuery: func(ctx context.Context, request locationclient.Request) (locationclient.Response, error) {
					queryCalls++
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > pointQueryTimeout {
						t.Fatalf("query deadline = %v, present = %t", deadline, ok)
					}
					if request.Operation != "search" || request.Input == nil || request.Input.Kind != "name" || request.Input.Text != "Saskatoon" {
						t.Fatalf("query input = %#v", request)
					}
					if request.Filters.Country != "CA" || request.Filters.Region != "SK" || request.Options.Locale != "fr-CA" || request.Options.Limit != 2 || request.Options.IncludeAreaGeometry {
						t.Fatalf("query controls = %#v", request)
					}
					return locationclient.Response{
						Status: "ambiguous", Ambiguous: true, CatalogGeneration: "generation-1",
						Results: []locationclient.Candidate{pointFixtureCandidate("one"), pointFixtureCandidate("two"), pointFixtureCandidate("three")},
					}, nil
				},
			}
			request := httptest.NewRequest(http.MethodGet, path+"?name=Saskatoon&country=ca&region=sk&locale=fr-CA&limit=2", nil)
			request.Header.Set("Origin", "https://teleweather.ca")
			response := httptest.NewRecorder()

			service.routes().ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if queryCalls != 1 {
				t.Fatalf("query calls = %d", queryCalls)
			}
			if got := response.Header().Get("Access-Control-Allow-Origin"); got != "https://teleweather.ca" {
				t.Fatalf("allow origin = %q", got)
			}
			body := response.Body.String()
			for _, forbidden := range []string{`"attributes"`, `"deployments"`, `"bbox"`, `"area_geometry"`, `"evidence"`, "secret-wkb"} {
				if strings.Contains(body, forbidden) {
					t.Fatalf("response exposed %q: %s", forbidden, body)
				}
			}
			var payload pointResponse
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if !payload.Ambiguous || !payload.Truncated || len(payload.Results) != 2 || payload.CatalogGeneration != "generation-1" {
				t.Fatalf("response metadata = %#v", payload)
			}
			entity := payload.Results[0].Entity
			if len(entity.Identifiers) != 2 || len(entity.Names) != 2 || entity.Geometry == nil || entity.Geometry.Latitude != 52.13 || entity.Geometry.Longitude != -106.67 {
				t.Fatalf("public entity = %#v", entity)
			}
		})
	}
}

func TestPointRequestSelectorsAndAliases(t *testing.T) {
	t.Parallel()
	helloWeather := map[string]locationRecord{"09999": {Forecast: "nu-special"}}
	tests := []struct {
		name          string
		target        string
		operation     string
		kind          string
		scheme        string
		authority     string
		value         string
		selectorKey   string
		selectorValue string
	}{
		{name: "icao", target: "/point?icao=cyxe", operation: "resolve", kind: "identifier", scheme: "icao", authority: "icao", value: "cyxe", selectorKey: "icao", selectorValue: "cyxe"},
		{name: "iata", target: "/point?iata=yxe", operation: "resolve", kind: "identifier", scheme: "iata", authority: "iata", value: "yxe", selectorKey: "iata", selectorValue: "yxe"},
		{name: "citypage", target: "/point?citypage=sk-40", operation: "resolve", kind: "identifier", scheme: "eccc_citypage", authority: "eccc", value: "sk-40", selectorKey: "citypage", selectorValue: "sk-40"},
		{name: "postal", target: "/point?postal=S7L%202V7", operation: "resolve", kind: "identifier", scheme: "postal", value: "S7L 2V7", selectorKey: "postal", selectorValue: "S7L 2V7"},
		{name: "generic", target: "/point?scheme=custom-source_1&authority=agency.example&value=ABC-123", operation: "resolve", kind: "identifier", scheme: "custom-source_1", authority: "agency.example", value: "ABC-123", selectorKey: "scheme", selectorValue: "ABC-123"},
		{name: "canonical entity", target: "/point?id=urn:haze:location:fixture", operation: "resolve", kind: "entity", value: "urn:haze:location:fixture", selectorKey: "id", selectorValue: "urn:haze:location:fixture"},
		{name: "configured hello weather", target: "/point?helloweather=09999", operation: "resolve", kind: "identifier", scheme: "eccc_citypage", authority: "eccc", value: "nu-special", selectorKey: "helloweather", selectorValue: "09999"},
		{name: "derived hello weather", target: "/point?helloweather=06040", operation: "resolve", kind: "identifier", scheme: "eccc_citypage", authority: "eccc", value: "sk-40", selectorKey: "helloweather", selectorValue: "06040"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := pointRequestFromHTTP(httptest.NewRequest(http.MethodGet, test.target, nil), helloWeather)
			if err != nil {
				t.Fatal(err)
			}
			input := plan.Request.Input
			if plan.Request.Operation != test.operation || input == nil || input.Kind != test.kind || input.Scheme != test.scheme || input.Authority != test.authority {
				t.Fatalf("request = %#v", plan.Request)
			}
			actualValue := input.Value
			if input.Kind == "entity" {
				actualValue = input.ID
			}
			if actualValue != test.value || plan.Selector.Key != test.selectorKey || plan.Selector.Value != test.selectorValue {
				t.Fatalf("request value = %q, selector = %#v", actualValue, plan.Selector)
			}
		})
	}

	plan, err := pointRequestFromHTTP(httptest.NewRequest(http.MethodGet, "/point?geocode=52.13,-106.67", nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Request.Operation != "point_facets" || plan.Request.Input == nil || plan.Request.Input.Latitude == nil || *plan.Request.Input.Latitude != 52.13 || plan.Request.Input.Longitude == nil || *plan.Request.Input.Longitude != -106.67 {
		t.Fatalf("point request = %#v", plan.Request)
	}
}

func TestPointIdentifierFallbacks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		target         string
		primaryScheme  string
		fallbackScheme string
		fallbackValue  string
	}{
		{name: "Canadian postal FSA", target: "/point?postal=S7L%202V7", primaryScheme: "postal", fallbackScheme: "postal", fallbackValue: "S7L"},
		{name: "IATA station", target: "/point?iata=yxe", primaryScheme: "iata", fallbackScheme: "eccc_station", fallbackValue: "YXE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := []locationclient.Request{}
			service := &Service{pointSlots: make(chan struct{}, 1)}
			service.pointQuery = func(_ context.Context, request locationclient.Request) (locationclient.Response, error) {
				calls = append(calls, request)
				if len(calls) == 1 {
					return locationclient.Response{Status: "not_found", Results: []locationclient.Candidate{}}, nil
				}
				return locationclient.Response{Status: "resolved", Results: []locationclient.Candidate{pointFixtureCandidate("fallback")}}, nil
			}
			response := httptest.NewRecorder()
			service.routes().ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.target, nil))
			if response.Code != http.StatusOK || len(calls) != 2 {
				t.Fatalf("status = %d, calls = %#v, body = %s", response.Code, calls, response.Body.String())
			}
			if calls[0].Input.Scheme != test.primaryScheme || calls[1].Input.Scheme != test.fallbackScheme || calls[1].Input.Value != test.fallbackValue {
				t.Fatalf("fallback requests = %#v", calls)
			}
			var payload pointResponse
			if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Selector.Scheme != test.fallbackScheme {
				t.Fatalf("resolved selector = %#v", payload.Selector)
			}
		})
	}
}

func TestPointCensusPopulationOnlyAttachesToCensusAndForecastEntities(t *testing.T) {
	censusEntityID := "urn:haze:location:census-subdivision"
	catalog := locationdb.CensusPopulationCatalog{
		ByCanonicalID: map[string]locationdb.CensusPopulation{
			censusEntityID: {Population: 266_141, CensusYear: 2021},
		},
		ByCityPageID: map[string]locationdb.CensusPopulation{
			"sk-40": {Population: 266_141, CensusYear: 2021},
		},
	}

	censusSubdivision := pointFixtureCandidate("census-subdivision")
	censusSubdivision.Entity.ID = censusEntityID
	censusSubdivision.Entity.Kind = "administrative_area"
	censusSubdivision.Entity.Identifiers = []locationclient.Identifier{{Authority: "statcan", Scheme: "sgc", Value: "4711066", Primary: true}}

	forecastLocation := pointFixtureCandidate("forecast-location")
	forecastLocation.Entity.Kind = "forecast_location"
	forecastLocation.Entity.Identifiers = []locationclient.Identifier{{Authority: "eccc", Scheme: "eccc_citypage", Value: "SK-40", Primary: true}}

	weatherStation := pointFixtureCandidate("weather-station")
	weatherStation.Entity.Kind = "weather_station"
	weatherStation.Entity.Identifiers = []locationclient.Identifier{{Authority: "eccc", Scheme: "eccc_citypage", Value: "sk-40", Primary: true}}

	publicForecast := pointFixtureCandidate("public-forecast")
	publicForecast.Entity.Kind = "public_forecast_location"
	publicForecast.Entity.Identifiers = []locationclient.Identifier{{Authority: "eccc", Scheme: "eccc_citypage", Value: "sk-40", Primary: true}}

	unqualifiedAdministrativeArea := pointFixtureCandidate("unqualified-administrative")
	unqualifiedAdministrativeArea.Entity.ID = censusEntityID
	unqualifiedAdministrativeArea.Entity.Kind = "administrative_area"
	unqualifiedAdministrativeArea.Entity.Identifiers = []locationclient.Identifier{{Authority: "eccc", Scheme: "station", Value: "CYXE", Primary: true}}

	service := &Service{
		cfg:        loadedConfig{CensusPopulation: catalog},
		pointSlots: make(chan struct{}, 1),
		pointQuery: func(context.Context, locationclient.Request) (locationclient.Response, error) {
			return locationclient.Response{Status: "resolved", Results: []locationclient.Candidate{
				censusSubdivision, forecastLocation, weatherStation, publicForecast, unqualifiedAdministrativeArea,
			}}, nil
		},
	}
	response := httptest.NewRecorder()
	service.routes().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/point?name=Saskatoon&limit=5", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var payload pointResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Results) != 5 {
		t.Fatalf("results = %#v", payload.Results)
	}
	for _, index := range []int{0, 1} {
		entity := payload.Results[index].Entity
		if entity.Population != 266_141 || entity.CensusYear != 2021 {
			t.Fatalf("result %d census data = (%d, %d), want (266141, 2021)", index, entity.Population, entity.CensusYear)
		}
	}
	for _, index := range []int{2, 3, 4} {
		entity := payload.Results[index].Entity
		if entity.Population != 0 || entity.CensusYear != 0 {
			t.Fatalf("result %d received unrelated census data: %#v", index, entity)
		}
	}
	if strings.Contains(response.Body.String(), `"attributes"`) {
		t.Fatalf("response exposed internal attributes: %s", response.Body.String())
	}
}

func TestPointPostalFallbackRejectsInvalidCanadianLetterPositions(t *testing.T) {
	t.Parallel()
	plan, err := pointRequestFromHTTP(httptest.NewRequest(http.MethodGet, "/point?postal=D7L2V7", nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Fallback != nil {
		t.Fatalf("invalid Canadian postal code received an FSA fallback: %#v", plan.Fallback)
	}
}

func TestPointRejectsInvalidRequests(t *testing.T) {
	t.Parallel()
	tests := []string{
		"/point",
		"/point?name=Saskatoon&icao=CYXE",
		"/point?scheme=icao",
		"/point?value=CYXE",
		"/point?name=Saskatoon&unknown=yes",
		"/point?name=one&name=two",
		"/point?geocode=NaN,-106",
		"/point?geocode=91,-106",
		"/point?scheme=bad%20scheme&value=x",
		"/point?scheme=icao&authority=bad%20authority&value=CYXE",
		"/point?icao=CYXE&authority=icao",
		"/point?id=fixture",
		"/point?icao=CYXE&limit=11",
		"/point?name=" + strings.Repeat("x", pointMaximumName+1),
		"/point?helloweather=99999",
	}
	for _, target := range tests {
		t.Run(target, func(t *testing.T) {
			_, err := pointRequestFromHTTP(httptest.NewRequest(http.MethodGet, target, nil), nil)
			if err == nil {
				t.Fatal("invalid request was accepted")
			}
		})
	}
}

func TestPointHTTPStatusesConcurrencyAndCORS(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		service := &Service{pointSlots: make(chan struct{}, 1), pointQuery: func(context.Context, locationclient.Request) (locationclient.Response, error) {
			return locationclient.Response{Status: "not_found"}, nil
		}}
		response := httptest.NewRecorder()
		service.routes().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/point?icao=NONE", nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
	})

	for name, queryError := range map[string]error{"unavailable": errors.New("offline"), "timeout": context.DeadlineExceeded} {
		t.Run(name, func(t *testing.T) {
			service := &Service{pointSlots: make(chan struct{}, 1), pointQuery: func(context.Context, locationclient.Request) (locationclient.Response, error) {
				return locationclient.Response{}, queryError
			}}
			response := httptest.NewRecorder()
			service.routes().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/point?icao=CYXE", nil))
			want := http.StatusServiceUnavailable
			if errors.Is(queryError, context.DeadlineExceeded) {
				want = http.StatusGatewayTimeout
			}
			if response.Code != want {
				t.Fatalf("status = %d, want %d", response.Code, want)
			}
		})
	}

	t.Run("busy", func(t *testing.T) {
		slots := make(chan struct{}, 1)
		slots <- struct{}{}
		called := false
		service := &Service{pointSlots: slots, pointQuery: func(context.Context, locationclient.Request) (locationclient.Response, error) {
			called = true
			return locationclient.Response{}, nil
		}}
		response := httptest.NewRecorder()
		service.routes().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/point?icao=CYXE", nil))
		if response.Code != http.StatusTooManyRequests || called || response.Header().Get("Retry-After") != "1" {
			t.Fatalf("busy response = %d, called = %t", response.Code, called)
		}
	})

	for _, path := range []string{"/point", "/ivr/v1/point"} {
		t.Run("CORS "+path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodOptions, path, nil)
			request.Header.Set("Origin", "http://localhost:4321")
			response := httptest.NewRecorder()
			(&Service{}).routes().ServeHTTP(response, request)
			if response.Code != http.StatusNoContent || response.Header().Get("Access-Control-Allow-Origin") != "http://localhost:4321" {
				t.Fatalf("CORS response = %d, headers = %#v", response.Code, response.Header())
			}
		})
	}

	t.Run("method", func(t *testing.T) {
		response := httptest.NewRecorder()
		(&Service{}).routes().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/point?icao=CYXE", nil))
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d", response.Code)
		}
	})
}

func pointFixtureCandidate(suffix string) locationclient.Candidate {
	latitude, longitude, accuracy, distance := 52.13, -106.67, 25.0, 0.0
	return locationclient.Candidate{
		Entity: locationclient.Entity{
			ID: "urn:haze:location:" + suffix, Kind: "place", Capabilities: []string{"forecast"},
			Country: "CA", Region: "SK", LifecycleStatus: "active", ReportingStatus: "not_applicable", SourceQuality: 1,
			Identifiers: []locationclient.Identifier{
				{Authority: "icao", Scheme: "icao", Value: "CYXE", NormalizedValue: "CYXE", Primary: true, Confidence: "exact"},
				{Authority: "iata", Scheme: "iata", Value: "YXE", NormalizedValue: "YXE", Confidence: "exact"},
			},
			Names: []locationclient.Name{
				{Locale: "en-CA", Value: "Saskatoon", NormalizedValue: "saskatoon", Kind: "canonical", Primary: true},
				{Locale: "fr-CA", Value: "Saskatoon", NormalizedValue: "saskatoon", Kind: "localized"},
			},
			Geometry:    &locationclient.Geometry{Type: "point", Latitude: &latitude, Longitude: &longitude, BBox: [4]float64{-107, 52, -106, 53}, AccuracyM: &accuracy},
			Deployments: []locationclient.Deployment{{ProviderDeploymentID: "secret-deployment", Attributes: map[string]any{"secret": true}}},
			Attributes:  map[string]any{"area_geometry": "secret-wkb", "private": true},
		},
		Match: locationclient.Match{Score: 1, Confidence: "exact", Method: "fixture", Algorithm: "test", Evidence: map[string]any{"secret": true}},
		Facet: "forecast", Distance: &distance,
	}
}
