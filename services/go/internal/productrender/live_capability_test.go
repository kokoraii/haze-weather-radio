package productrender

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOnDemandCapabilityProviderFallbacks(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		var body string
		switch {
		case strings.Contains(request.URL.Path, "aqhi-observations"):
			body = `{"features":[{"properties":{"location_name_en":"Saskatoon","location_name_fr":"Saskatoon","observation_datetime":"2026-08-07T18:00:00Z","aqhi":3}}]}`
		case strings.Contains(request.URL.Path, "aqhi-forecasts"):
			body = `{"features":[{"properties":{"publication_datetime":"2026-08-07T18:00:00Z","forecast_period":{"period_1":{"forecast_period_en":"Tonight","forecast_period_fr":"Ce soir","aqhi":4}}}}]}`
		case strings.Contains(request.URL.Path, "hydrometric-realtime"):
			body = `{"features":[{"properties":{"IDENTIFIER":"05HG001","STATION_NUMBER":"05HG001","STATION_NAME":"South Saskatchewan River","DATETIME":"2026-08-07T18:00:00Z","LEVEL":3.2,"DISCHARGE":125}}]}`
		case strings.Contains(request.URL.Path, "marineweather-realtime"):
			body = `{"id":"088800","properties":{"area":{"value":{"en":"Lake Diefenbaker","fr":"lac Diefenbaker"}},"regularForecast":{"issuedDatetimeUTC":"2026-08-07T18:00:00Z","locations":[{"name":{"en":"Lake Diefenbaker"},"weatherCondition":{"periodOfCoverage":{"en":"Tonight"},"wind":{"en":"Wind west 15 knots"}}}]}}}`
		default:
			http.NotFound(writer, request)
			return
		}
		if _, err := writer.Write([]byte(body)); err != nil {
			t.Errorf("write fixture: %v", err)
		}
	}))
	defer server.Close()

	renderer := newRenderer(loadedConfig{})
	var aqhi liveAirQualityFile
	if input, ok := renderer.fetchLiveAQHIFromBase(locationXML{ID: "AQSASK", Source: "eccc"}, &aqhi, server.URL); !ok || input != "eccc:aqhi/AQSASK" {
		t.Fatalf("AQHI fallback input=%q ok=%t", input, ok)
	}
	if value, ok := numberFromAny(aqhi.AQHI); !ok || value != 3 || len(aqhi.Forecast.Periods) != 1 {
		t.Fatalf("AQHI payload = %#v", aqhi)
	}

	feed := feedXML{OnDemand: true}
	feed.Locations.HydrometricLocations.Locations = []locationXML{{ID: "05HG001", Source: "eccc"}}
	var hydrometric liveSpecialtyProductFile
	if input, ok := renderer.fetchLiveHydrometricFromBase(feed, &hydrometric, server.URL); !ok || input != "eccc:hydrometric/05HG001" {
		t.Fatalf("hydrometric fallback input=%q ok=%t", input, ok)
	}
	if len(hydrometric.Items) != 1 || hydrometric.Items[0]["station_id"] != "05HG001" {
		t.Fatalf("hydrometric payload = %#v", hydrometric)
	}

	var marine liveMarineForecastFile
	if input, ok := renderer.fetchLiveMarineForecastFromBase(locationXML{ID: "088800", Source: "eccc"}, &marine, server.URL); !ok || input != "eccc:marine/088800" {
		t.Fatalf("marine fallback input=%q ok=%t", input, ok)
	}
	if len(marine.Properties.RegularForecast.Locations) != 1 {
		t.Fatalf("marine payload = %#v", marine)
	}
}
