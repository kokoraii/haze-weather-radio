package ivr

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

func testCapabilityService() *Service {
	return &Service{capabilities: locationdb.NewCapabilityCatalog([]locationdb.CapabilityLocation{
		{Kind: locationdb.CapabilityObservation, ID: "CYA", Name: "Alpha Airport", NameFR: "Aéroport Alpha", Region: "SK", Country: "CA", Latitude: 52.05, Longitude: -106},
		{Kind: locationdb.CapabilityObservation, ID: "CYB", Name: "Bravo Station", Region: "SK", Country: "CA", Latitude: 52.12, Longitude: -106},
		{Kind: locationdb.CapabilityObservation, ID: "CYC", Name: "Charlie Station", Region: "SK", Country: "CA", Latitude: 53.50, Longitude: -106},
		{Kind: locationdb.CapabilityForecast, ID: "SK-40", Name: "Saskatoon", Region: "SK", Country: "CA", Latitude: 52.13, Longitude: -106.67},
		{Kind: locationdb.CapabilityAirQuality, ID: "AQSASK", Name: "Saskatoon AQHI", Region: "SK", Country: "CA", Latitude: 52.14, Longitude: -106.65},
		{Kind: locationdb.CapabilityHydrometric, ID: "05HG001", Name: "South Saskatchewan River", Region: "SK", Country: "CA", Latitude: 52.10, Longitude: -106.70},
		{Kind: locationdb.CapabilityMarineForecast, ID: "088800", Name: "Lake Diefenbaker", Region: "SK", Country: "CA", Latitude: 51.00, Longitude: -106.80},
	})}
}

func TestObservationStationChoicesOffersOnlyVeryCloseStations(t *testing.T) {
	t.Parallel()
	service := testCapabilityService()
	location := ResolvedLocation{Province: "SK", Latitude: "52", Longitude: "-106", Language: "en-CA"}

	choices := service.observationStationChoices(location)
	if len(choices) != 2 {
		t.Fatalf("choices = %#v, want two close stations", choices)
	}
	if choices[0].Location.ID != "CYA" || choices[1].Location.ID != "CYB" {
		t.Fatalf("choice order = %#v", choices)
	}
	prompt := localizedObservationChoicesPrompt(location.Language, choices)
	for _, wanted := range []string{"Press 1 for Alpha Airport", "Press 2 for Bravo Station", "Press pound to go back"} {
		if !strings.Contains(prompt, wanted) {
			t.Fatalf("prompt %q missing %q", prompt, wanted)
		}
	}
}

func TestMapProductLocationUsesCapabilitySpecificTargets(t *testing.T) {
	t.Parallel()
	service := testCapabilityService()
	location := ResolvedLocation{Province: "SK", Latitude: "52.1", Longitude: "-106.6"}
	packages := []string{"current_conditions", "forecast", "air_quality", "hydrometric", "marine_forecast"}

	mapped := service.mapProductLocation(location, packages, nil)
	if mapped.StationID != "CYB" {
		t.Fatalf("station = %q", mapped.StationID)
	}
	if mapped.Forecast != "SK-40" || mapped.AirQualityID != "AQSASK" || mapped.HydrometricID != "05HG001" || mapped.MarineForecastID != "088800" {
		t.Fatalf("mapped capabilities = %#v", mapped)
	}
}

func TestLocalizedObservationChoicesPromptFrench(t *testing.T) {
	t.Parallel()
	choices := []locationdb.CapabilityMatch{{Location: locationdb.CapabilityLocation{ID: "CYA", Name: "Alpha Airport", NameFR: "Aéroport Alpha"}}}
	prompt := localizedObservationChoicesPrompt("fr-CA", choices)
	if !strings.Contains(prompt, "Appuyez sur 1 pour Aéroport Alpha") || !strings.Contains(prompt, "Appuyez sur le carré") {
		t.Fatalf("prompt = %q", prompt)
	}
}

func TestApplyRequestedCapabilityTargetsValidatesProviderIDs(t *testing.T) {
	t.Parallel()
	service := testCapabilityService()
	request := httptest.NewRequest("GET", "/ivr/v1/audio?station_id=CYB&air_quality_id=AQSASK&hydrometric_id=05HG001&marine_forecast_id=088800", nil)
	location := ResolvedLocation{StationID: "ORIGINAL", Latitude: "52", Longitude: "-106"}

	service.applyRequestedCapabilityTargets(request, &location)
	if location.StationID != "CYB" || location.AirQualityID != "AQSASK" || location.HydrometricID != "05HG001" || location.MarineForecastID != "088800" {
		t.Fatalf("location = %#v", location)
	}
	if location.Latitude != "52.12000000" || location.Longitude != "-106.00000000" {
		t.Fatalf("station coordinates = %q,%q", location.Latitude, location.Longitude)
	}

	forged := httptest.NewRequest("GET", "/ivr/v1/audio?station_id=FORGED", nil)
	service.applyRequestedCapabilityTargets(forged, &location)
	if location.StationID != "CYB" {
		t.Fatalf("forged station ID was accepted: %#v", location)
	}
}

func TestTwilioCurrentConditionsOffersAndAppliesStationChoice(t *testing.T) {
	t.Parallel()
	service := testCapabilityService()
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{{ID: "sk-0001", EnabledRaw: "true"}},
		Prompts: defaultPromptConfig(),
	}
	service.cfg = cfg
	service.cache = NewProductCache(cfg, nil)
	service.resolver = resolverWithHelloWeather(cfg, locationRecord{
		Code: "06040", Source: "hello_weather", Name: "Test Municipality", Province: "SK",
		Latitude: "52", Longitude: "-106",
	})

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=location_option&code=06040&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"1"}})
	response := httptest.NewRecorder()
	service.handleLocationOptionTwiML(response, request)
	if body := response.Body.String(); !strings.Contains(body, "state=observation_choice") || !strings.Contains(body, "kind=observation_choices") {
		t.Fatalf("station choice TwiML = %s", body)
	}

	choiceRequest := formRequest("http://ivr.test/ivr/v1/twiml?state=observation_choice&code=06040&feed_id=sk-0001&lang=en-CA&packages=current_conditions", url.Values{"Digits": {"2"}})
	choiceResponse := httptest.NewRecorder()
	service.handleObservationChoiceTwiML(choiceResponse, choiceRequest)
	if body := choiceResponse.Body.String(); !strings.Contains(body, "station_id=CYB") || !strings.Contains(body, "packages=current_conditions") {
		t.Fatalf("selected station TwiML = %s", body)
	}
}
