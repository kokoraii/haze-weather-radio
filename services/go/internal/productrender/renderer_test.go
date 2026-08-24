package productrender

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
)

func TestCurrentConditionsProductUsesOpenerPackageAndRepeatSegments(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeObservationJSON(t, cfg.Store, "CYXE", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Saskatoon Diefenbaker Int'l Airport"},
  "station_id": "CYXE",
  "properties": {
    "condition": {"en": "Mostly Cloudy"},
    "temp": 24,
    "dewpoint": 8,
    "humidity": 67,
    "wind": {"direction": "NW", "speed": 38, "gust": 42},
    "visibility": 24,
    "pressure": {"value": 101.4, "tendency": {"en": "falling"}}
  }
}`)
	storeObservationJSON(t, cfg.Store, "OUTLOOK", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Outlook"},
  "station_id": "OUTLOOK",
  "properties": {
    "temp": 24,
    "wind": {"direction": "NW", "speed": 17}
  }
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "current_conditions"})
	if err != nil {
		t.Fatal(err)
	}

	if product.Title != "Current Conditions" {
		t.Fatalf("title = %q", product.Title)
	}
	for _, wanted := range []string{
		"The current weather conditions. Issued by Environment and Climate Change Canada at 8 PM Central Standard Time",
		"The weather at Saskatoon Diefenbaker Int'l Airport was Mostly Cloudy",
		"Outlook",
		"24 degrees",
		"winds were north west at 17 kilometres per hour.",
		"Again, at Saskatoon Diefenbaker Int'l Airport",
	} {
		if !strings.Contains(product.Text, wanted) {
			t.Fatalf("product text missing %q:\n%s", wanted, product.Text)
		}
	}
	if strings.Contains(product.Text, "at 10 17 kilometres per hour") || strings.Count(product.Text, "winds were north west at 17 kilometres per hour") != 1 {
		t.Fatalf("current conditions wind pacing or wording wrong:\n%s", product.Text)
	}
	if len(product.Segments) < 10 {
		t.Fatalf("segments = %#v", product.Segments)
	}
}

func TestConfiguredCurrentConditionsRendersUnavailableAnnouncementWhenDataIsMissing(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "current_conditions"})
	if err != nil {
		t.Fatal(err)
	}
	if product.Title != "Current Conditions" || !strings.Contains(product.Text, "temporarily unavailable") {
		t.Fatalf("product = %#v", product)
	}
	if len(product.Segments) != 1 || product.Segments[0].Label != "unavailable" {
		t.Fatalf("segments = %#v", product.Segments)
	}
}

func TestCurrentConditionsDeduplicatesNearbyStationNames(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	cfg.Feeds[0].Locations.ObservationLocations.Locations = []locationXML{
		{ID: "CYXE", Source: "eccc"},
		{ID: "BRIDGE1", Source: "eccc"},
		{ID: "BRIDGE2", Source: "eccc"},
		{ID: "OUTLOOK", Source: "eccc"},
	}
	storeObservationJSON(t, cfg.Store, "CYXE", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Saskatoon Diefenbaker Int'l Airport"},
  "station_id": "CYXE",
  "properties": {"temp": 24, "wind": {"speed": 10}}
}`)
	storeObservationJSON(t, cfg.Store, "BRIDGE1", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Burlington Lift Bridge"},
  "station_id": "BRIDGE1",
  "properties": {"temp": 22, "wind": {"speed": 3}}
}`)
	storeObservationJSON(t, cfg.Store, "BRIDGE2", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Burlington Lift Bridge"},
  "station_id": "BRIDGE2",
  "properties": {"temp": 22, "wind": {"speed": 3}}
}`)
	storeObservationJSON(t, cfg.Store, "OUTLOOK", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Outlook"},
  "station_id": "OUTLOOK",
  "properties": {"temp": 24, "wind": {"speed": 17}}
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "current_conditions"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(product.Text, "Burlington Lift Bridge"); got != 1 {
		t.Fatalf("Burlington Lift Bridge count = %d:\n%s", got, product.Text)
	}
	if !strings.Contains(product.Text, "Outlook") {
		t.Fatalf("dedupe removed distinct nearby station:\n%s", product.Text)
	}
}

func TestMarineAndAviationReportsUseSharedObservationWithKnots(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	cfg.Packages["marine_reports"] = packageProfile{Enabled: true, ReaderID: "00"}
	cfg.Packages["aviation_reports"] = packageProfile{Enabled: true, ReaderID: "00"}
	cfg.Feeds[0].Locations.MarineConditions.Locations = []locationXML{{ID: "CYXE", Source: "eccc"}}
	cfg.Feeds[0].Locations.AviationReportLocations.Locations = []locationXML{{ID: "CYXE", Source: "eccc"}}
	cfg.ProductText["marine_reports"] = map[string]map[string]string{
		"report.text.1":       {"en-ca": "The marine report for {location},"},
		"report.text.2":       {"en-ca": "the weather was {weather}."},
		"report.text.3":       {"en-ca": "Air temperature, {ctemp} degrees."},
		"report.winds.text.1": {"en-ca": "Winds were {dir_text} at {spd_maritime}"},
		"report.winds.text.2": {"en-ca": "The wind was calm."},
	}
	cfg.ProductText["aviation_reports"] = map[string]map[string]string{
		"report.text.1":       {"en-ca": "The aviation weather report for {location},"},
		"report.text.2":       {"en-ca": "visibility, {visibility}."},
		"report.text.3":       {"en-ca": "altimeter setting, {altimeter}."},
		"report.winds.text.1": {"en-ca": "Winds were {dir_text} at {spd_aviation}"},
		"report.winds.text.2": {"en-ca": "The wind was calm."},
	}
	storeObservationJSON(t, cfg.Store, "CYXE", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Saskatoon Diefenbaker Int'l Airport"},
  "station_id": "CYXE",
  "properties": {
    "condition": {"en": "Mostly Cloudy"},
    "temp": 24,
    "wind": {"direction": "NW", "speed": 38},
    "visibility": 24,
    "pressure": {"value": 101.4}
  }
}`)

	marine, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "marine_reports"})
	if err != nil {
		t.Fatal(err)
	}
	aviation, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "aviation_reports"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(marine.Text, "Winds were north west at 21 knots") || strings.Contains(marine.Text, "at 10 21 knots") {
		t.Fatalf("marine report wind wording wrong:\n%s", marine.Text)
	}
	if !strings.Contains(marine.Text, "Airport,\n\nthe weather was Mostly Cloudy.") {
		t.Fatalf("marine report collapsed punctuation or line pacing:\n%s", marine.Text)
	}
	for _, wanted := range []string{
		"The aviation weather report for Saskatoon Diefenbaker Int'l Airport",
		"visibility, 15 statute miles.",
		"altimeter setting, 29.94 inches.",
		"Winds were north west at 21 knots",
	} {
		if !strings.Contains(aviation.Text, wanted) {
			t.Fatalf("aviation report missing %q:\n%s", wanted, aviation.Text)
		}
	}
	if !strings.Contains(aviation.Text, "Airport,\n\nvisibility, 15 statute miles.\n\naltimeter setting, 29.94 inches.") {
		t.Fatalf("aviation report collapsed punctuation or line pacing:\n%s", aviation.Text)
	}
}

func TestObservationReportUsesExistingAudioOpenerSegment(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	audioPath := filepath.Join(dir, "audio", "vocal", "marine.wav")
	if err := os.MkdirAll(filepath.Dir(audioPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(audioPath, []byte("placeholder"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := loadFixtureConfig(t, dir)
	cfg.Packages["marine_reports"] = packageProfile{Enabled: true, ReaderID: "00"}
	cfg.Feeds[0].Locations.MarineConditions.Locations = []locationXML{{ID: "CYXE", Source: "eccc"}}
	cfg.ProductAudio = map[string]map[string]map[string]string{
		"marine_reports": {"opener": {"en-ca": "./audio/vocal/marine.wav"}},
	}
	cfg.ProductText["marine_reports"] = map[string]map[string]string{
		"opener":              {"en-ca": "The text opener should be replaced."},
		"report.text.1":       {"en-ca": "The marine report for {location}."},
		"report.winds.text.1": {"en-ca": "Winds were {dir_text} at {spd_maritime}."},
		"report.winds.text.2": {"en-ca": "The wind was calm."},
	}
	storeObservationJSON(t, cfg.Store, "CYXE", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Saskatoon Diefenbaker Int'l Airport"},
  "station_id": "CYXE",
  "properties": {
    "temp": 24,
    "wind": {"direction": "NW", "speed": 18}
  }
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "marine_reports"})
	if err != nil {
		t.Fatal(err)
	}
	if len(product.Segments) < 2 || product.Segments[0].AudioPath != "./audio/vocal/marine.wav" {
		t.Fatalf("segments = %#v", product.Segments)
	}
	if strings.Contains(product.Text, "text opener") {
		t.Fatalf("audio opener leaked fallback text:\n%s", product.Text)
	}
	if !strings.Contains(product.Text, "The marine report for Saskatoon Diefenbaker Int'l Airport.") {
		t.Fatalf("report text missing:\n%s", product.Text)
	}
}

func TestObservationReportProductsRequireDedicatedLocationBlocks(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	cfg.Packages["marine_reports"] = packageProfile{Enabled: true, ReaderID: "00"}
	cfg.Packages["aviation_reports"] = packageProfile{Enabled: true, ReaderID: "00"}
	cfg.ProductText["marine_reports"] = map[string]map[string]string{
		"report.text.1": {"en-ca": "The marine report for {location}."},
	}
	cfg.ProductText["aviation_reports"] = map[string]map[string]string{
		"report.text.1": {"en-ca": "The aviation weather report for {location}."},
	}
	cfg.Feeds[0].Locations.AviationReportLocations.Locations = nil
	cfg.Feeds[0].Locations.MarineConditions.Locations = []locationXML{{ID: "CXTO", Source: "eccc"}}
	storeObservationJSON(t, cfg.Store, "sk-40", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Saskatoon"},
  "station_id": "sk-40",
  "properties": {"temp": 24}
}`)
	storeObservationJSON(t, cfg.Store, "CXTO", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Toronto Island"},
  "station_id": "CXTO",
  "properties": {"temp": 18}
}`)

	marine, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "marine_reports"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(marine.Text, "The marine report for Toronto Island.") {
		t.Fatalf("marine report used wrong station:\n%s", marine.Text)
	}
	if _, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "aviation_reports"}); err == nil {
		t.Fatalf("aviation report should not fall back to generic observation locations")
	}
}

func TestRenderTemplateTextPreservesPunctuationAndLines(t *testing.T) {
	got := renderTemplateText("Wait...  {thing},\nthen continue.", map[string]string{"thing": "now"})
	want := "Wait... now,\nthen continue."
	if got != want {
		t.Fatalf("renderTemplateText = %q, want %q", got, want)
	}
}

func TestMarineForecastProductRendersRealtimePayload(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	cfg.Packages["marine_forecast"] = packageProfile{Enabled: true, ReaderID: "01"}
	cfg.Feeds[0].Locations.MarineForecastLocations.Subregions = []locationXML{{ID: "m0000144", Source: "eccc"}}
	cfg.ProductText["marine_forecast"] = map[string]map[string]string{
		"opener":                     {"en-ca": "The {source} marine forecast for {area}, issued at {issue_hour} {issue_minute} {issue_ampm} {issue_timezone}."},
		"forecast.regular.location":  {"en-ca": "For {location}. {period} {wind}"},
		"forecast.waves.location":    {"en-ca": "Waves for {location}. {period} {waves}"},
		"forecast.extended.opener":   {"en-ca": "The extended marine outlook."},
		"forecast.extended.period":   {"en-ca": "For {period}. {forecast}"},
		"forecast.warnings.location": {"en-ca": "Marine warning for {location}. {warning}"},
	}
	storeProductPayloadJSON(t, cfg.Store, "marine_forecast", "eccc", "m0000144", `{
  "id": "m0000144",
  "properties": {
    "lastUpdated": "2026-07-07T14:30:51Z",
    "area": {"value": {"en": "Lake Ontario"}},
    "regularForecast": {
      "issuedDatetimeLocal": "2026-07-07T10:30:00-04:00",
      "locations": [
        {"name": "Lake Ontario East", "weatherCondition": {"periodOfCoverage": {"en": "Today Tonight and Wednesday."}, "wind": {"en": "Wind east 10 knots veering to southeast 10 this evening."}}},
        {"weatherCondition": {"periodOfCoverage": {"en": "Thursday."}, "wind": {"en": "Wind light."}}}
      ]
    },
    "waveForecast": {
      "locations": [
        {"name": "Lake Ontario East", "weatherCondition": {"periodOfCoverage": {"en": "Today Tonight and Wednesday."}, "textSummary": {"en": "Waves 0.5 metres or less."}}}
      ]
    },
    "extendedForecast": {
      "locations": [
        {"weatherCondition": {"forecastPeriods": [
          {"name": {"en": "Thursday"}, "value": {"en": "Wind light becoming southwest 15 knots in the afternoon."}}
        ]}}
      ]
    },
    "warnings": {"locations": [
      {"name": {"en": "Eastern Lake Ontario", "fr": "Est du lac Ontario"}, "weatherCondition": {"textSummary": {"en": "Strong wind warning in effect."}}}
    ]}
  }
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "marine_forecast"})
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{
		"The Environment Canada marine forecast for Lake Ontario, issued at 8 30 AM Central Standard Time.",
		"Marine warning for Eastern Lake Ontario. Strong wind warning in effect.",
		"For Lake Ontario East. Today Tonight and Wednesday. Wind east 10 knots veering to southeast 10 this evening.",
		"For Lake Ontario. Thursday. Wind light.",
		"Waves for Lake Ontario East. Today Tonight and Wednesday. Waves 0.5 metres or less.",
		"The extended marine outlook.",
		"For Thursday. Wind light becoming southwest 15 knots in the afternoon.",
	} {
		if !strings.Contains(product.Text, wanted) {
			t.Fatalf("marine forecast missing %q:\n%s", wanted, product.Text)
		}
	}
}

func TestReportTimeConvertsUTCProductTimestampsToFeedTimezone(t *testing.T) {
	for _, raw := range []string{"2026 Jul 05 1700 UTC", "2026-07-05T17:00:00Z"} {
		if got := reportTime(raw, "America/Regina"); got != "11 AM Central Standard Time" {
			t.Fatalf("reportTime(%q) = %q", raw, got)
		}
	}
}

func TestForecastRegionTitleDoesNotDoubleRegion(t *testing.T) {
	if got := normalizeRegionTitle("Outlook, Watrous, Hanley, Imperial, and Dinsmore region"); got != "Outlook, Watrous, Hanley, Imperial, and Dinsmore region" {
		t.Fatalf("region = %q", got)
	}
	if got := normalizeRegionTitle("Outlook, Watrous Area"); got != "Outlook, Watrous" {
		t.Fatalf("area = %q", got)
	}
	if got := normalizeRegionTitle("Outlook - Watrous - Hanley - Imperial - Dinsmore"); got != "Outlook, Watrous, Hanley, Imperial, and Dinsmore region" {
		t.Fatalf("hyphenated region = %q", got)
	}
	if got := normalizeRegionTitle("City of Saskatoon"); got != "City of Saskatoon" {
		t.Fatalf("city = %q", got)
	}
}

func TestStationIDAndDateTimeUseLegacyShape(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)

	stationID, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "station_id"})
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{
		"You are listening to all hazards, canada radio met.",
		"Callsign X L F 3 2 2.",
		"Broadcasting from Saskatoon on a frequency of 162.550 megahertz.",
	} {
		if !strings.Contains(stationID.Text, wanted) {
			t.Fatalf("station ID missing %q:\n%s", wanted, stationID.Text)
		}
	}

	fixedTime := time.Date(2026, 6, 15, 18, 5, 0, 0, time.FixedZone("CST", -6*60*60))
	if got := dateTimeAnnouncement(fixedTime, "en-CA"); got != "Good evening. The current time is six oh five P.M., Central Standard Time." {
		t.Fatalf("date/time announcement = %q", got)
	}
}

func TestForecastProductSaysRegionOncePerRegion(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeForecastJSON(t, cfg.Store, "065500", "065500", `{
  "issued_at": "2026-06-15T20:00:00-06:00",
  "name": {"en": "Outlook - Watrous - Hanley - Imperial and Dinsmore region"},
  "forecast": [
    {"period": {"en": "Tonight"}, "textSummary": {"en": "Cloudy with showers."}},
    {"period": {"en": "Tuesday"}, "textSummary": {"en": "Sunny."}}
  ]
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "forecast"})
	if err != nil {
		t.Fatal(err)
	}

	region := "For the Outlook, Watrous, Hanley, Imperial, and Dinsmore region."
	if strings.Count(product.Text, region) != 1 {
		t.Fatalf("region opener count mismatch:\n%s", product.Text)
	}
	if strings.Contains(product.Text, "Outlook, Watrous, Hanley, Imperial, and Dinsmore region, Tuesday") {
		t.Fatalf("period line repeated region name:\n%s", product.Text)
	}
	if !strings.Contains(product.Text, "Tonight. Cloudy with showers.") || !strings.Contains(product.Text, "Tuesday. Sunny.") {
		t.Fatalf("period text missing:\n%s", product.Text)
	}
}

func TestForecastProductUsesNestedProductTemplates(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	_ = os.Remove(filepath.Join(dir, "managed", "configs", "packages.xml"))
	mustWrite(t, filepath.Join(dir, "managed", "configs", "products.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<ProductText version="1.1">
  <product id="forecast" enabled="true">
    <lang iso="en-CA" readerid="01">
      <text key="opener">Now for your official {source} forecast, Issued at {issue_hour} {issue_ampm} {issue_timezone}.</text>
      <region>
        <shortterm>
          <text key="region">For areas in and around {region}.</text>
          <text key="region_plain">For the {region}.</text>
          <text key="today">For today. {fc_text}</text>
          <text key="tonight">For tonight. {fc_text}</text>
        </shortterm>
        <extended>
          <text key="opener">The extended outlook.</text>
          <text key="period">For {period}. {fc_text}</text>
        </extended>
      </region>
    </lang>
  </product>
</ProductText>`)
	cfg := loadFixtureConfig(t, dir)
	storeForecastJSON(t, cfg.Store, "065500", "065500", `{
  "issued_at": "2026-06-15T20:00:00-06:00",
  "name": {"en": "Outlook - Watrous - Hanley - Imperial and Dinsmore region"},
  "forecast": [
    {"period": {"en": "Tonight"}, "textSummary": {"en": "Cloudy with showers"}},
    {"period": {"en": "Wednesday"}, "textSummary": {"en": "Sunny"}},
    {"period": {"en": "Wednesday Night"}, "textSummary": {"en": "Partly cloudy"}},
    {"period": {"en": "Thursday"}, "textSummary": {"en": "Sunny"}},
    {"period": {"en": "Thursday Night"}, "textSummary": {"en": "Clear"}}
  ]
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "forecast"})
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{
		"Now for your official Environment Canada forecast, Issued at 8 PM Central Standard Time.",
		"For tonight. Cloudy with showers.",
		"For Wednesday. Sunny.",
		"For Wednesday Night. Partly cloudy.",
		"The extended outlook.",
		"For Thursday. Sunny.",
		"For Thursday Night. Clear.",
	} {
		if !strings.Contains(product.Text, wanted) {
			t.Fatalf("forecast missing %q:\n%s", wanted, product.Text)
		}
	}
	if strings.Index(product.Text, "For Wednesday Night. Partly cloudy.") > strings.Index(product.Text, "The extended outlook.") {
		t.Fatalf("Wednesday night should be before extended outlook:\n%s", product.Text)
	}
	if strings.Index(product.Text, "The extended outlook.") > strings.Index(product.Text, "For Thursday. Sunny.") {
		t.Fatalf("Thursday should be after extended outlook:\n%s", product.Text)
	}
	var extendedSegments int
	for _, segment := range product.Segments {
		if strings.Contains(segment.Text, "The extended outlook.") {
			extendedSegments++
			if strings.Contains(segment.Text, "For Wednesday Night.") || !strings.Contains(segment.Text, "For Thursday.") || !strings.Contains(segment.Text, "For Thursday Night.") {
				t.Fatalf("extended segment had wrong periods:\n%#v", segment)
			}
		}
	}
	if strings.Contains(product.Text, "Today. Sunny") {
		t.Fatalf("forecast used legacy period wording:\n%s", product.Text)
	}
	if extendedSegments != 1 {
		t.Fatalf("extended forecast segment count = %d, segments=%#v", extendedSegments, product.Segments)
	}
}

func TestWxOnDemandUsesRequestedLocationInsteadOfFeedLocations(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	if cfg.ProductText["forecast"] == nil {
		cfg.ProductText["forecast"] = map[string]map[string]string{}
	}
	cfg.ProductText["forecast"]["region"] = map[string]string{"en-CA": "For the {region}."}
	storeObservationJSON(t, cfg.Store, "sk-99", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Regina"},
  "station_id": "sk-99",
  "properties": {
    "condition": {"en": "Clear"},
    "temp": 21,
    "wind": {"direction": "SE", "speed": 14},
    "pressure": {"value": 101.1}
  }
}`)
	storeForecastJSON(t, cfg.Store, "sk-99", "06099", `{
  "issued_at": "2026-06-15T20:00:00-06:00",
  "name": {"en": "Regina"},
  "forecast": [
    {"period": {"en": "Tonight"}, "textSummary": {"en": "Clear."}},
    {"period": {"en": "Tuesday"}, "textSummary": {"en": "Sunny."}}
  ]
}`)

	product, err := newRenderer(cfg).RenderWxOnDemand(wxOnDemandRequest{
		RequestID:    "wx-regina",
		FeedID:       "sk-0001",
		Code:         "06099",
		Source:       "hello_weather",
		LocationName: "Regina",
		ForecastID:   "sk-99",
		StationID:    "sk-99",
		Packages:     []string{"current_conditions", "forecast"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{
		"The weather at Regina was Clear",
		"For Regina.",
		"Tonight. Clear.",
	} {
		if !strings.Contains(product.Text, wanted) {
			t.Fatalf("on-demand product missing %q:\n%s", wanted, product.Text)
		}
	}
	if strings.Contains(product.Text, "Saskatoon Diefenbaker") {
		t.Fatalf("on-demand product leaked feed observation:\n%s", product.Text)
	}
}

func TestWxOnDemandDoesNotRequireFeed(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeForecastJSON(t, cfg.Store, "sk-99", "06099", `{
  "issued_at": "2026-06-15T20:00:00-06:00",
  "name": {"en": "Regina"},
  "forecast": [
    {"period": {"en": "Tonight"}, "textSummary": {"en": "Clear."}}
  ]
}`)

	product, err := newRenderer(cfg).RenderWxOnDemand(wxOnDemandRequest{
		RequestID:    "wx-feedless",
		Code:         "06099",
		Source:       "hello_weather",
		LocationName: "Regina",
		ForecastID:   "sk-99",
		StationID:    "sk-99",
		Packages:     []string{"forecast"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if product.FeedID != "wx-on-demand" {
		t.Fatalf("FeedID = %q", product.FeedID)
	}
	if !strings.Contains(product.Text, "Tonight. Clear.") {
		t.Fatalf("on-demand forecast missing requested location text:\n%s", product.Text)
	}
}

func TestWxOnDemandUsesProviderStationWhenLocationNameIsID(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeObservationJSON(t, cfg.Store, "sk-99", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Regina International Airport"},
  "station_id": "sk-99",
  "properties": {
    "condition": {"en": "Clear"},
    "temp": 21,
    "wind": {"direction": "SE", "speed": 14},
    "pressure": {"value": 101.1}
  }
}`)

	product, err := newRenderer(cfg).RenderWxOnDemand(wxOnDemandRequest{
		RequestID:    "wx-regina-id-name",
		FeedID:       "sk-0001",
		Code:         "06099",
		Source:       "hello_weather",
		LocationName: "sk-99",
		ForecastID:   "sk-99",
		StationID:    "sk-99",
		Packages:     []string{"current_conditions"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "The weather at Regina International Airport was Clear") {
		t.Fatalf("on-demand current conditions did not use provider station:\n%s", product.Text)
	}
	if strings.Contains(product.Text, "The weather at sk-99") {
		t.Fatalf("on-demand current conditions leaked ID override:\n%s", product.Text)
	}
}

func TestTelephoneWxOnDemandUsesTelephoneForecastAndSkipsConditionRepeat(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	cfg.Feeds[0].Playout.Routine = "false"
	storeObservationJSON(t, cfg.Store, "sk-99", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Regina International Airport"},
  "station_id": "sk-99",
  "properties": {
    "condition": {"en": "Clear"},
    "temp": 21,
    "wind": {"direction": "SE", "speed": 14},
    "pressure": {"value": 101.1}
  }
}`)
	storeForecastJSON(t, cfg.Store, "sk-99", "06099", `{
  "issued_at": "2026-06-15T23:00:00-06:00",
  "name": {"en": "Regina"},
  "forecast": [
    {"period": {"en": "Tonight"}, "textSummary": {"en": "Clear."}},
    {"period": {"en": "Tuesday"}, "textSummary": {"en": "Sunny."}}
  ]
}`)

	product, err := newRenderer(cfg).RenderWxOnDemand(wxOnDemandRequest{
		RequestID:    "wx-regina-telephone",
		FeedID:       "sk-0001",
		Code:         "06099",
		Source:       "hello_weather",
		LocationName: "Regina",
		ForecastID:   "sk-99",
		StationID:    "sk-99",
		Packages:     []string{"current_conditions", "forecast"},
		Telephone:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{
		"The weather at Regina International Airport was Clear",
		"The official Environment Canada forecast for the Regina area issued at 11 PM Central Standard Time.",
		"Tonight. Clear.",
	} {
		if !strings.Contains(product.Text, wanted) {
			t.Fatalf("telephone product missing %q:\n%s", wanted, product.Text)
		}
	}
	for _, unwanted := range []string{
		"Again, at Regina International Airport",
		"For Regina.",
		"XLF322",
		"listening area",
	} {
		if strings.Contains(product.Text, unwanted) {
			t.Fatalf("telephone product contained %q:\n%s", unwanted, product.Text)
		}
	}
}

func TestWxOnDemandIsIndependentOfFeedEnablementAndRoutinePlayout(t *testing.T) {
	tests := []struct {
		name           string
		feedID         string
		disableFeed    bool
		disableRoutine bool
	}{
		{name: "disabled configured feed", feedID: "sk-0001", disableFeed: true, disableRoutine: true},
		{name: "routine disabled configured feed", feedID: "sk-0001", disableRoutine: true},
		{name: "unknown feed identifier", feedID: "telephone-only"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFixture(t, dir)
			cfg := loadFixtureConfig(t, dir)
			if test.disableFeed {
				cfg.Feeds[0].EnabledRaw = "false"
			}
			if test.disableRoutine {
				cfg.Feeds[0].Playout.Routine = "false"
			}
			storeObservationJSON(t, cfg.Store, "sk-99", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Regina International Airport"},
  "station_id": "sk-99",
  "properties": {
    "condition": {"en": "Clear"},
    "temp": 21,
    "wind": {"direction": "SE", "speed": 14}
  }
}`)

			product, err := newRenderer(cfg).RenderWxOnDemand(wxOnDemandRequest{
				RequestID:    "wx-independent-" + test.name,
				FeedID:       test.feedID,
				Code:         "06099",
				Source:       "hello_weather",
				LocationName: "sk-99",
				StationID:    "sk-99",
				Packages:     []string{"current_conditions"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(product.Text, "Regina International Airport") {
				t.Fatalf("on-demand product did not render requested observation: %s", product.Text)
			}
		})
	}
}

func TestTelephoneWxOnDemandUsesStoredForecastIssueTime(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeObservationJSON(t, cfg.Store, "sk-99", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Regina International Airport"},
  "station_id": "sk-99",
  "properties": {
    "condition": {"en": "Clear"},
    "temp": 21,
    "wind": {"direction": "SE", "speed": 14},
    "pressure": {"value": 101.1}
  }
}`)
	if err := cfg.Store.UpsertForecast(context.Background(), datastore.ForecastRecord{
		Source:      "eccc",
		ForecastID:  "sk-99",
		RegionID:    "06099",
		IssuedAtRaw: "2026-06-15T23:00:00-06:00",
		Payload: mustJSONMap(t, `{
  "name": {"en": "Regina"},
  "forecast": [
    {"period": {"en": "Tonight"}, "textSummary": {"en": "Clear."}},
    {"period": {"en": "Tuesday"}, "textSummary": {"en": "Sunny."}}
  ]
}`),
	}); err != nil {
		t.Fatal(err)
	}

	product, err := newRenderer(cfg).RenderWxOnDemand(wxOnDemandRequest{
		RequestID:    "wx-regina-telephone-stored-time",
		FeedID:       "sk-0001",
		Code:         "06099",
		Source:       "hello_weather",
		LocationName: "Regina",
		ForecastID:   "sk-99",
		StationID:    "sk-99",
		Packages:     []string{"forecast"},
		Telephone:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "issued at 11 PM Central Standard Time.") {
		t.Fatalf("telephone product missing stored issue time:\n%s", product.Text)
	}
	if strings.Contains(product.Text, "latest issue time") {
		t.Fatalf("telephone product used fallback issue time:\n%s", product.Text)
	}
}

func TestTelephoneWxOnDemandOmitsIssuePhraseWhenTimeMissing(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeForecastJSON(t, cfg.Store, "sk-99", "06099", `{
  "name": {"en": "Regina"},
  "forecast": [
    {"period": {"en": "Tonight"}, "textSummary": {"en": "Clear."}}
  ]
}`)

	product, err := newRenderer(cfg).RenderWxOnDemand(wxOnDemandRequest{
		RequestID:    "wx-regina-telephone-no-time",
		FeedID:       "sk-0001",
		Code:         "06099",
		Source:       "hello_weather",
		LocationName: "Regina",
		ForecastID:   "sk-99",
		Packages:     []string{"forecast"},
		Telephone:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "The official Environment Canada forecast for the Regina area.") {
		t.Fatalf("telephone product missing no-time opener:\n%s", product.Text)
	}
	for _, unwanted := range []string{"latest issue time", "issued at ."} {
		if strings.Contains(product.Text, unwanted) {
			t.Fatalf("telephone product contained %q:\n%s", unwanted, product.Text)
		}
	}
}

func TestTelephoneWxOnDemandUsesForecastIDForCurrentConditionsWhenStationMissing(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeObservationJSON(t, cfg.Store, "sk-37", `{
  "source": "eccc",
  "observed_at": "2026-06-15T20:00:00-06:00",
  "station": {"en": "Last Mountain"},
  "station_id": "sk-37",
  "properties": {
    "condition": {"en": "Not observed"},
    "temp": 16.2,
    "wind": {"direction": "NW", "speed": 18},
    "pressure": {"value": 101.3}
  }
}`)

	product, err := newRenderer(cfg).RenderWxOnDemand(wxOnDemandRequest{
		RequestID:    "wx-imperial-telephone",
		FeedID:       "sk-0001",
		Code:         "4711008",
		Source:       "capcp_geocode",
		LocationName: "Imperial",
		ForecastID:   "sk-37",
		Packages:     []string{"current_conditions"},
		Telephone:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "The weather at Last Mountain was Not observed") {
		t.Fatalf("telephone current conditions did not use requested forecast station:\n%s", product.Text)
	}
	if strings.Contains(product.Text, "Saskatoon") || strings.Contains(product.Text, "Diefenbaker") {
		t.Fatalf("telephone current conditions leaked feed default observation:\n%s", product.Text)
	}
}

func TestBuildECCCPointConditionsUsesNearestObservationStation(t *testing.T) {
	obs, err := extractECCCPointObservation(`<script>window.__INITIAL_STATE__={"weather":{"obs":{}},"location":{"location":{"51.347--105.434":{"obs":{"observedAt":"Last Mountain","provinceCode":"sk","climateId":"4014156","tcid":"wxg","timeStamp":"2026-06-18T18:00:00.000Z","timeStampText":"12:00 PM CST Thursday 18 June 2026","iconCode":"29","condition":"","temperature":{"metric":"18","metricUnrounded":"18.3"},"dewpoint":{"metric":"7","metricUnrounded":"7.0"},"pressure":{"metric":"101.2"},"tendency":"falling","humidity":"47","windSpeed":{"metric":"9"},"windGust":{"metric":""},"windDirection":"NW"}}}}};</script>`)
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := buildECCCPointConditions(obs)
	if !ok {
		t.Fatal("point conditions did not build")
	}
	decoded, ok := decodeLiveObservation(payload)
	if !ok {
		t.Fatal("point conditions did not decode")
	}
	rendered := observationFromLiveFile(locationXML{ID: "sk-37", Source: "eccc"}, "en-CA", decoded)
	if rendered.LocationName != "Last Mountain" {
		t.Fatalf("station = %q", rendered.LocationName)
	}
	if rendered.Condition != "Not observed" {
		t.Fatalf("condition = %q", rendered.Condition)
	}
	if rendered.TemperatureC == nil || *rendered.TemperatureC != 18.3 {
		t.Fatalf("temperature = %#v", rendered.TemperatureC)
	}
}

func TestLoadStoreForecastIgnoresDecodedEmptyPayload(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeForecastJSON(t, cfg.Store, "sk-32", "06032", `{
  "forecastGroup": {"forecasts": []}
}`)

	var raw liveForecastFile
	input, ok := newRenderer(cfg).loadStoreForecast(coverageRegionXML{
		ID:     "06032",
		Source: "eccc",
	}, "sk-32", &raw)
	if ok || input != "" {
		t.Fatalf("empty decoded store forecast should be ignored, input=%q forecast=%#v", input, raw.Forecast)
	}
}

func TestBuildECCCForecastUsesCitypageNameFallback(t *testing.T) {
	payload, ok := buildECCCForecast(map[string]any{
		"properties": map[string]any{
			"lastUpdated": "2026-06-18T05:07:12Z",
			"name":        map[string]any{"en": "Regina", "fr": "Regina"},
			"forecastGroup": map[string]any{
				"timestamp": map[string]any{"en": "2026-06-17T22:00:00Z", "fr": "2026-06-17T22:00:00Z"},
				"forecasts": []any{
					map[string]any{
						"period":      map[string]any{"textForecastName": map[string]any{"en": "Tonight", "fr": "Ce soir"}},
						"cloudPrecip": map[string]any{"en": "Clear.", "fr": "Dégagé."},
					},
				},
			},
		},
	}, coverageRegionXML{ID: "06032", Source: "eccc", Name: "sk-32"}, map[string]forecastRegionName{})
	if !ok {
		t.Fatal("forecast payload did not build")
	}
	name, ok := payload["name"].(map[string]any)
	if !ok {
		t.Fatalf("name block = %#v", payload["name"])
	}
	if name["en"] != "Regina" || name["fr"] != "Regina" {
		t.Fatalf("name block = %#v", name)
	}
	if payload["issued_at"] != "2026-06-17T22:00:00Z" || payload["updated_at"] != "2026-06-18T05:07:12Z" {
		t.Fatalf("timestamps = issued:%#v updated:%#v", payload["issued_at"], payload["updated_at"])
	}
}

func TestAirQualityProductUsesLegacyAQHINarrative(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeProductPayloadJSON(t, cfg.Store, "air_quality", "eccc", "HAHJJ", `{
  "source": "eccc",
  "location": {"en": "Saskatoon", "fr": "Saskatoon"},
  "observed_at": "2026-06-15T21:00:00Z",
  "aqhi": 4,
  "special_notes": {"en": "Smoke may cause fluctuating air quality conditions.", "fr": ""},
  "forecast": {
    "published_at": "2026-06-15T22:00:00Z",
    "periods": [
      {"period": {"en": "Tonight"}, "aqhi": 4, "aqhi_insmoke": 7},
      {"period": {"en": "Tuesday"}, "aqhi": 6},
      {"period": {"en": "Tuesday night"}, "aqhi": 3},
      {"period": {"en": "Wednesday"}, "aqhi": 2}
    ]
  }
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "air_quality"})
	if err != nil {
		t.Fatal(err)
	}

	for _, wanted := range []string{
		"The air quality health index was observed at Saskatoon and reported a value of 4 at 3 PM Central Standard Time.",
		"This is acceptable air quality for outdoor activities for most people.",
		"Smoke may cause fluctuating air quality conditions.",
		"The air quality health index forecast for Saskatoon is 4 for Tonight and is considered Moderate.",
		"The air quality health index is expected to be 7 in smoke.",
		"For Tuesday, the maximum air quality health index is forecast to be 6, or Moderate.",
		"3 on Tuesday night, and lastly, 2 on Wednesday.",
	} {
		if !strings.Contains(product.Text, wanted) {
			t.Fatalf("air quality product missing %q:\n%s", wanted, product.Text)
		}
	}
	if strings.Contains(product.Text, "Air quality for Saskatoon.") {
		t.Fatalf("generic air quality opener leaked into legacy narrative:\n%s", product.Text)
	}
}

func TestClimateSummaryRendersNOAAStyleDailyPackage(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeProductPayloadJSON(t, cfg.Store, "climate_summary", "eccc", "4057165", `{
  "source": "eccc",
  "name": {"en": "Saskatoon"},
  "last_updated": "2026-06-18T12:00:00Z",
  "observations": {
    "station": {"en": "Saskatoon RCS"},
    "date": "2026-06-17 00:00:00",
    "high": 20.3,
    "low": 8.5,
    "mean": 14.4,
    "precipitation": 4,
    "max_gust_speed": 47,
    "max_gust_direction": "NNW",
    "heating_degree_days": 3.6,
    "cooling_degree_days": 0,
    "min_humidity": 48
  },
  "normals": {
    "station": {"en": "Saskatoon Diefenbaker Int'l A"},
    "month": 6,
    "period": "1981 to 2010",
    "temperature": {"high": 22.4, "low": 9.2, "mean": 15.8},
    "precipitation": 65.8
  },
  "records": {
    "high_temperature": {"value": 34.6, "year": 1988},
    "low_temperature": {"value": 0, "year": 1949},
    "precipitation": {"value": 86.4, "year": 2007}
  },
  "astronomy": {
    "sunrise": "2026-06-17T10:45:00Z",
    "sunset": "2026-06-18T03:31:00Z",
    "timezone": "America/Regina"
  }
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "climate_summary"})
	if err != nil {
		t.Fatal(err)
	}

	for _, wanted := range []string{
		"The Environment Canada climate summary.",
		"At Saskatoon RCS on Wednesday, June 17, the high was 20.3 degrees, the low was 8.5 degrees, and the mean temperature was 14.4 degrees.",
		"Precipitation totalled 4.0 millimetres.",
		"The strongest wind gust was 47.0 kilometres per hour from the north north west.",
		"The minimum relative humidity was 48 percent.",
		"Heating degree days were 3.6, and cooling degree days were 0.0.",
		"For June, the monthly averages from 1981 to 2010 at Saskatoon Diefenbaker Int'l A are high 22.4 degrees, low 9.2 degrees, mean 15.8 degrees, and total precipitation 65.8 millimetres.",
		"For June 17, the record high was 34.6 degrees in 1988, the record low was 0 degrees in 1949, and the greatest precipitation was 86.4 millimetres in 2007.",
		"Sunrise is at 4:45 AM, and sunset is at 9:31 PM Central Standard Time.",
	} {
		if !strings.Contains(product.Text, wanted) {
			t.Fatalf("climate summary missing %q:\n%s", wanted, product.Text)
		}
	}
}

func TestClimateSummaryOmitsMissingPrecipitation(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeProductPayloadJSON(t, cfg.Store, "climate_summary", "eccc", "4057165", `{
  "source": "eccc",
  "name": {"en": "Saskatoon"},
  "observations": {
    "station": {"en": "Saskatoon RCS"},
    "date": "2026-06-17 00:00:00",
    "high": 20.3,
    "low": 8.5
  }
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "climate_summary"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(product.Text), "precipitation") {
		t.Fatalf("missing precipitation should not be announced:\n%s", product.Text)
	}
}

func TestSpecialtyProductsRenderStoredPayloads(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	cfg.ProductText["hydrometric"] = map[string]map[string]string{
		"opener":              {"en-ca": "Hydrometric reports courtesy of Environment and Climate Change Canada."},
		"primary.placeholder": {"en-ca": "No river condition report is available for {site}."},
		"primary.gauge.text.1": {
			"en-ca": "{water_station} reported a water level of {level}.",
		},
		"primary.gauge.text.2": {
			"en-ca": "The discharge rate was {discharge}.",
		},
		"nearby.upstream": {"en-ca": "Upstream."},
		"nearby.downstream": {
			"en-ca": "Downstream.",
		},
		"nearby.and": {"en-ca": "and,"},
		"nearby.gauge.text.1": {
			"en-ca": "{water_station}, water level {level}.",
		},
		"nearby.gauge.text.2": {
			"en-ca": "Discharge, {discharge}.",
		},
	}
	tests := []struct {
		name    string
		kind    string
		payload string
		want    []string
	}{
		{
			name: "thunderstorm outlook",
			kind: "thunderstorm_outlook",
			payload: `{
  "source": "eccc",
  "title": "Thunderstorm Outlook",
  "updated_at": "2026-06-18T18:00:00Z",
  "items": [{
    "area": "Prairies",
    "thunderstorm": "isolated",
    "risk": 1,
    "confidence": 3,
    "impact": 1,
    "tornado": true,
    "rain_mm": 30,
    "hail_cm": 1,
    "gust_kmh": 70,
    "valid_at": "2026-06-18T18:00:00Z",
    "expires_at": "2099-06-18T23:00:00Z"
  }]
}`,
			want: []string{"Environment Canada Thunderstorm Outlook covering the City of Saskatoon area.", "a minor convective risk is anticipated.", "A tornado risk is also indicated.", "Wind gusts up to 70 kilometers per hour, 1 centimeter of hail, and 30 millimeters of rain are associated with this convective risk."},
		},
		{
			name: "hydrometric",
			kind: "hydrometric",
			payload: `{
  "source": "eccc",
  "title": "River Conditions",
  "updated_at": "2026-06-18T18:00:00Z",
  "items": [{
    "station_id": "05HG001",
    "station": "South Saskatchewan River at Saskatoon",
    "relation": "primary",
    "order": 0,
    "observed_at": "2026-06-18T17:00:00-06:00",
    "level_m": 2.424,
    "discharge": 288
  }, {
    "station_id": "05JG006",
    "station": "Red Deer River near Bindloss",
    "relation": "upstream",
    "order": 1,
    "observed_at": "2026-06-18T17:00:00-06:00",
    "level_m": 5.91,
    "discharge": 111
  }, {
    "station_id": "05KD007",
    "station": "Saskatchewan River below the Forks",
    "relation": "downstream",
    "order": 3,
    "observed_at": "2026-06-18T17:00:00-06:00",
    "level_m": 378.266,
    "discharge": 787
  }]
}`,
			want: []string{
				"Hydrometric reports courtesy of Environment and Climate Change Canada.",
				"South Saskatchewan River at Saskatoon reported a water level of 2.4 metres.",
				"The discharge rate was 288 cubic metres per second.",
				"Upstream.",
				"Red Deer River near Bindloss, water level 5.9 metres.",
				"and,",
				"Downstream.",
				"Saskatchewan River below the Forks, water level 378.3 metres.",
				"Discharge, 787 cubic metres per second.",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeProductPayloadJSON(t, cfg.Store, tt.kind, "eccc", "sk-0001", tt.payload)
			product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: tt.kind})
			if err != nil {
				t.Fatal(err)
			}
			for _, wanted := range tt.want {
				if !strings.Contains(product.Text, wanted) {
					t.Fatalf("%s product missing %q:\n%s", tt.kind, wanted, product.Text)
				}
			}
		})
	}
}

func TestThunderstormPeriodLabelUsesNaturalBroadcastWording(t *testing.T) {
	loc, err := time.LoadLocation("America/Regina")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 19, 10, 0, 0, 0, loc)
	tests := []struct {
		name  string
		start string
		end   string
		want  string
	}{
		{
			name:  "same day noon outlook",
			start: "2026-06-19T18:00:00Z",
			end:   "2026-06-20T06:00:00Z",
			want:  "This afternoon",
		},
		{
			name:  "overnight outlook",
			start: "2026-06-20T06:00:00Z",
			end:   "2026-06-20T18:00:00Z",
			want:  "Overnight",
		},
		{
			name:  "tomorrow noon outlook",
			start: "2026-06-20T18:00:00Z",
			end:   "2026-06-21T06:00:00Z",
			want:  "For Saturday afternoon",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := thunderstormPeriodLabel(test.start, test.end, "America/Regina", now)
			if got != test.want {
				t.Fatalf("period label = %q, want %q", got, test.want)
			}
		})
	}
}

func TestThunderstormRiskLabelUsesEnvironmentCanadaCategories(t *testing.T) {
	tests := []struct {
		value any
		want  string
	}{
		{value: 0, want: "level 0"},
		{value: 1, want: "minor"},
		{value: 2, want: "moderate"},
		{value: 3, want: "high"},
		{value: 4, want: "extreme"},
		{value: "moderate", want: "moderate"},
	}
	for _, test := range tests {
		got := thunderstormRiskLabel(test.value)
		if got != test.want {
			t.Fatalf("thunderstormRiskLabel(%v) = %q, want %q", test.value, got, test.want)
		}
	}
}

func TestThunderstormOutlookSkipsNearbyPolygonRows(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeProductPayloadJSON(t, cfg.Store, "thunderstorm_outlook", "eccc", "sk-0001", `{
  "source": "eccc",
  "title": "Thunderstorm Outlook",
  "updated_at": "2026-06-20T18:00:00Z",
  "items": [{
    "area": "Prairies",
    "thunderstorm": "isolated",
    "risk": 1,
    "gust_kmh": 80,
    "hail_cm": 1,
    "valid_at": "2026-06-20T18:00:00Z",
    "expires_at": "2099-06-21T06:00:00Z",
    "distance_km": 90,
    "direction": "south west"
  }, {
    "area": "Prairies",
    "thunderstorm": "none",
    "risk": 0,
    "valid_at": "2026-06-20T18:00:00Z",
    "expires_at": "2099-06-21T06:00:00Z",
    "distance_km": 0
  }]
}`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "thunderstorm_outlook"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "no hazardous weather is expected") {
		t.Fatalf("product did not render covered no-hazard row:\n%s", product.Text)
	}
	for _, unwanted := range []string{"close proximity", "south west", "80 kilometers", "1 centimeter of hail"} {
		if strings.Contains(product.Text, unwanted) {
			t.Fatalf("product rendered nearby row text %q:\n%s", unwanted, product.Text)
		}
	}
}

func TestConfiguredMissingProductDataRendersUnavailableAnnouncement(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "forecast"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "forecast is temporarily unavailable") {
		t.Fatalf("product text = %q", product.Text)
	}
}

func TestGeophysicalAlertRemovesWWVBoilerplate(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	storeTextProduct(t, cfg.Store, "nws", "wwv", `:Product: Geophysical Alert Message wwv.txt
:Issued: 2026 Jun 16 0305 UTC
# Prepared by the US Dept. of Commerce, NOAA, Space Weather Prediction Center
#
#          Geophysical Alert Message
#
Solar-terrestrial indices for 15 June follow.
The planetary A index was 5.`)

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "geophysical_alert"})
	if err != nil {
		t.Fatal(err)
	}

	for _, unwanted := range []string{":Product:", ":Issued:", "Prepared by", "Geophysical Alert Message"} {
		if strings.Contains(product.Text, unwanted) {
			t.Fatalf("WWV boilerplate was not removed:\n%s", product.Text)
		}
	}
	if !strings.Contains(product.Text, "Solar-terrestrial indices") {
		t.Fatalf("WWV body missing:\n%s", product.Text)
	}
}

func TestUserBulletinXMLAudioBecomesProductMetadata(t *testing.T) {
	dir := t.TempDir()
	configDir := filepath.Join(dir, "managed", "configs")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatal(err)
	}
	xmlBody := `<?xml version="1.0" encoding="UTF-8"?>
<userBulletins>
  <bulletin id="audio-1" enabled="true">
    <title>Road Closure</title>
    <active />
    <schedule mode="always" end_of_cycle="true" />
    <target><feed id="sk-0001" /></target>
    <content type="audio">
      <audio file="managed/audio/bulletins/road.wav" />
    </content>
  </bulletin>
</userBulletins>`
	if err := os.WriteFile(filepath.Join(configDir, "userBulletins.xml"), []byte(xmlBody), 0o600); err != nil {
		t.Fatal(err)
	}
	r := renderer{cfg: loadedConfig{BaseDir: dir}}

	product, err := r.userBulletinProduct(productBase(loadedConfig{}, feedXML{ID: "sk-0001"}, "user_bulletin", "", false), feedXML{ID: "sk-0001"})

	if err != nil {
		t.Fatal(err)
	}
	if product.Text != "" || len(product.Segments) != 0 {
		t.Fatalf("audio bulletin should not render TTS text: %#v", product)
	}
	if product.Metadata["content_type"] != "audio" || product.Metadata["audio_path"] != "managed/audio/bulletins/road.wav" {
		t.Fatalf("metadata = %#v", product.Metadata)
	}
	if product.Title != "Road Closure" {
		t.Fatalf("title = %q", product.Title)
	}
}

func TestAlertsProductUsesNativeCAPRegistry(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	alert, err := capmodel.ParseCAP([]byte(testCAP("urn:test:alert:1", "Update", "active", "2099-06-15T21:30:00-06:00", false)))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: cfg}
	if _, err := service.recordCAPAlert(alert, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}

	for _, wanted := range []string{
		"Environment Canada has updated a Yellow Warning - Severe Thunderstorm",
		"for City of Saskatoon; R.M. of Corman Park",
		"Forecast confidence is high with moderate impact expected",
		"capable of producing very strong wind gusts",
	} {
		if !strings.Contains(product.Text, wanted) {
			t.Fatalf("alert product missing %q:\n%s", wanted, product.Text)
		}
	}
	rows, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !strings.Contains(rows[0].RawXML, `<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">`) {
		t.Fatalf("archive is not storing native CAP payloads: %#v", rows)
	}
}

func TestAlertsProductRendersMixedActiveAndEndedCAPUpdate(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	activeRaw := testCAP("urn:test:alert:mixed-render", "Update", "active", "2099-06-15T21:30:00-06:00", false)
	endedRaw := testCAP("urn:test:alert:mixed-ended-info", "Update", "ended", "2099-06-15T21:30:00-06:00", true)
	endedRaw = strings.ReplaceAll(endedRaw, "065522", "066999")
	endedRaw = strings.ReplaceAll(endedRaw, "City of Saskatoon", "Off Feed County")
	infoStart := strings.Index(endedRaw, "<info>")
	infoEnd := strings.LastIndex(endedRaw, "</info>")
	if infoStart < 0 || infoEnd < 0 {
		t.Fatal("ended fixture info block not found")
	}
	mixedRaw := strings.Replace(activeRaw, "</alert>", endedRaw[infoStart:infoEnd+len("</info>")]+"\n</alert>", 1)
	alert, err := capmodel.ParseCAP([]byte(mixedRaw))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: cfg}
	if _, err := service.recordCAPAlert(alert, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "Yellow Warning - Severe Thunderstorm") {
		t.Fatalf("mixed active/ended alert did not render active info:\n%s", product.Text)
	}
	if strings.Contains(product.Text, "Off Feed County") || strings.Contains(product.Text, "066999") {
		t.Fatalf("mixed active/ended alert leaked ended area text:\n%s", product.Text)
	}
}

func TestOperatorExpiredCAPReplayIsSuppressedUntilAuthorityUpdate(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	now := time.Now().UTC()
	raw := testCAP("urn:test:operator-suppressed", "Alert", "active", "2099-06-15T21:30:00-06:00", false)
	alert, err := capmodel.ParseCAP([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	storeCAPArchiveRecord(cfg.Store, "expired", capArchiveRecord{
		ID:        alert.Identifier,
		FeedID:    "sk-0001",
		Status:    "expired",
		Reason:    "expired by operator",
		UpdatedAt: now,
		Alert:     alert,
		RawXML:    alert.RawXML,
	})
	storeCAPArchiveRecord(cfg.Store, "accepted", capArchiveRecord{
		ID:        alert.Identifier,
		FeedID:    "sk-0001",
		Status:    "accepted",
		UpdatedAt: now,
		Alert:     alert,
		RawXML:    alert.RawXML,
	})
	service := &Service{cfg: cfg}

	updates, err := service.recordCAPAlert(alert, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].Broadcast {
		t.Fatalf("operator-suppressed replay did not invalidate the accepted copy: %#v", updates)
	}
	accepted, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 0 {
		t.Fatalf("operator-suppressed replay entered accepted archive: %#v", accepted)
	}

	updatedRaw := strings.Replace(raw, "nickel size hail", "quarter size hail", 1)
	updatedAlert, err := capmodel.ParseCAP([]byte(updatedRaw))
	if err != nil {
		t.Fatal(err)
	}
	updates, err = service.recordCAPAlert(updatedAlert, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || !updates[0].Renderable {
		t.Fatalf("authority-updated alert was not accepted: %#v", updates)
	}
	accepted, err = cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 1 || accepted[0].AlertID != alert.Identifier {
		t.Fatalf("accepted archive rows after authority update = %#v", accepted)
	}
}

func TestMinorCAPAlertBypassesSAMEFilterForRoutinePackage(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	cfg.Feeds[0].Alerts.CapCP.Filter.Blocklist.Severities = []string{"Minor"}
	raw := strings.Replace(testCAP("urn:test:alert:minor-statement", "Alert", "active", "2099-06-15T21:30:00-06:00", false), "<severity>Moderate</severity>", "<severity>Minor</severity>", 1)
	raw = strings.Replace(raw, "<event>thunderstorm</event>", "<event>weather</event>", 1)
	raw = strings.Replace(raw, "yellow warning - severe thunderstorm", "special weather statement", -1)
	alert, err := capmodel.ParseCAP([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: cfg}
	updates, err := service.recordCAPAlert(alert, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || !updates[0].Renderable || updates[0].Broadcast {
		t.Fatalf("updates = %#v", updates)
	}

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "Special Weather Statement") {
		t.Fatalf("routine alert package missing minor alert:\n%s", product.Text)
	}
	rows, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].AlertID != alert.Identifier {
		t.Fatalf("accepted archive rows = %#v", rows)
	}
}

func TestNonRoutineCatchallRejectsMinorAndUnknownCAPAlerts(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	var feed feedXML
	feed.ID = "CAP-IT-ALL"
	feed.EnabledRaw = "true"
	feed.Playout.Routine = "false"
	feed.Alerts.CapCP.EnabledRaw = "true"
	feed.Alerts.CapCP.Filter.UseFeedLocations = "false"
	feed.Alerts.CapCP.Filter.Allowlist.Severities = []string{"Moderate", "Severe", "Extreme"}
	cfg.Feeds = []feedXML{feed}
	service := &Service{cfg: cfg}

	for _, severity := range []string{"Minor", "Unknown"} {
		raw := strings.Replace(testCAP("urn:test:catchall:"+strings.ToLower(severity), "Alert", "active", "2099-06-15T21:30:00-06:00", false), "<severity>Moderate</severity>", "<severity>"+severity+"</severity>", 1)
		alert, err := capmodel.ParseCAP([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		updates, err := service.recordCAPAlert(alert, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if len(updates) != 0 {
			t.Fatalf("%s alert should not publish catchall update: %#v", severity, updates)
		}
	}

	accepted, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 0 {
		t.Fatalf("minor/unknown catchall alerts entered accepted archive: %#v", accepted)
	}
	rejected, err := cfg.Store.ListCAPArchives(context.Background(), "rejected", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rejected) != 2 {
		t.Fatalf("rejected rows = %#v", rejected)
	}
	for _, row := range rejected {
		if row.FeedID != "CAP-IT-ALL" || row.Reason != "below feed alert threshold" {
			t.Fatalf("unexpected rejected row: %#v", row)
		}
	}
}

func TestNonRoutineCatchallRetainsModerateWatchWithoutPriorityBroadcast(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	var feed feedXML
	feed.ID = "CAP-IT-ALL"
	feed.EnabledRaw = "true"
	feed.Playout.Routine = "false"
	feed.Alerts.CapCP.EnabledRaw = "true"
	feed.Alerts.CapCP.Filter.UseFeedLocations = "false"
	feed.Alerts.CapCP.Filter.Allowlist.Severities = []string{"Moderate", "Severe", "Extreme"}
	feed.Alerts.CapCP.Filter.Allowlist.Urgencies = []string{"Immediate"}
	feed.Alerts.CapCP.Filter.Allowlist.Certainties = []string{"Observed", "Likely"}
	cfg.Feeds = []feedXML{feed}
	service := &Service{cfg: cfg}

	raw := testCAP("urn:test:catchall:sva-watch", "Alert", "active", "2099-06-15T21:30:00-06:00", false)
	raw = strings.Replace(raw, "<urgency>Immediate</urgency>", "<urgency>Expected</urgency>", 1)
	raw = strings.Replace(raw, "yellow warning - severe thunderstorm", "yellow watch - severe thunderstorm", -1)
	alert, err := capmodel.ParseCAP([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	updates, err := service.recordCAPAlert(alert, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].FeedID != "CAP-IT-ALL" || !updates[0].Renderable || updates[0].Broadcast {
		t.Fatalf("updates = %#v", updates)
	}

	accepted, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 1 || accepted[0].AlertID != alert.Identifier || accepted[0].FeedID != "CAP-IT-ALL" {
		t.Fatalf("accepted archive rows = %#v", accepted)
	}
	rejected, err := cfg.Store.ListCAPArchives(context.Background(), "rejected", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rejected) != 0 {
		t.Fatalf("moderate watch should not be rejected: %#v", rejected)
	}
}

func TestTestCAPAlertDoesNotEnterRoutinePackage(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	raw := strings.Replace(testCAP("urn:test:alert:test-message", "Alert", "active", "2099-06-15T21:30:00-06:00", false), "<status>Actual</status>", "<status>Test</status>", 1)
	alert, err := capmodel.ParseCAP([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: cfg}
	updates, err := service.recordCAPAlert(alert, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 0 {
		t.Fatalf("test alert should not publish routine update: %#v", updates)
	}
	rows, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("test alert entered accepted archive: %#v", rows)
	}
	rejected, err := cfg.Store.ListCAPArchives(context.Background(), "rejected", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rejected) != 1 || rejected[0].Reason != "test alert" {
		t.Fatalf("rejected rows = %#v", rejected)
	}
}

func TestCAPEndedUpdateRemovesReferencedActiveAlert(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	service := &Service{cfg: cfg}
	now := time.Now().UTC()
	active, err := capmodel.ParseCAP([]byte(testCAP("urn:test:alert:original", "Alert", "active", "2099-06-15T21:30:00-06:00", false)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.recordCAPAlert(active, now); err != nil {
		t.Fatal(err)
	}
	endedRaw := capWithReferences(
		testCAP("urn:test:alert:ended-update", "Update", "ended", "2099-06-15T21:30:00-06:00", true),
		"cap-pac@canada.ca,urn:test:alert:original,2026-06-15T15:58:00-06:00",
	)
	ended, err := capmodel.ParseCAP([]byte(endedRaw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.recordCAPAlert(ended, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	rows, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.AlertID == active.Identifier {
			t.Fatalf("referenced active alert remained in accepted archive: %#v", rows)
		}
	}
	if len(rows) != 0 {
		t.Fatalf("accepted archive rows = %#v", rows)
	}
	expired, err := cfg.Store.ListCAPArchives(context.Background(), "expired", time.Now().UTC().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) == 0 || expired[0].AlertID != ended.Identifier {
		t.Fatalf("ended update was not archived as expired: %#v", expired)
	}
}

func TestCAPEndedUpdateDoesNotRemoveReferencedAlertOutsideLocation(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	service := &Service{cfg: cfg}
	now := time.Now().UTC()
	active, err := capmodel.ParseCAP([]byte(testCAP("urn:test:alert:local-original", "Alert", "active", "2099-06-15T21:30:00-06:00", false)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.recordCAPAlert(active, now); err != nil {
		t.Fatal(err)
	}
	endedRaw := testCAP("urn:test:alert:off-feed-ended-update", "Update", "ended", "2099-06-15T21:30:00-06:00", true)
	endedRaw = strings.ReplaceAll(endedRaw, "065522", "066999")
	endedRaw = strings.ReplaceAll(endedRaw, "City of Saskatoon", "Off Feed County")
	endedRaw = capWithReferences(endedRaw, "cap-pac@canada.ca,urn:test:alert:local-original,2026-06-15T15:58:00-06:00")
	ended, err := capmodel.ParseCAP([]byte(endedRaw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.recordCAPAlert(ended, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	rows, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.AlertID == active.Identifier {
			found = true
		}
	}
	if !found {
		t.Fatalf("off-feed ended update removed active local alert: %#v", rows)
	}
}

func TestCAPEndedWarningDoesNotRemoveReferencedWatch(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	service := &Service{cfg: cfg}
	now := time.Now().UTC()
	activeRaw := strings.ReplaceAll(
		testCAP("urn:test:alert:watch-original", "Alert", "active", "2099-06-15T21:30:00-06:00", false),
		"yellow warning - severe thunderstorm",
		"yellow watch - severe thunderstorm",
	)
	activeRaw = strings.ReplaceAll(activeRaw, "<value>warning</value>", "<value>watch</value>")
	active, err := capmodel.ParseCAP([]byte(activeRaw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.recordCAPAlert(active, now); err != nil {
		t.Fatal(err)
	}
	endedRaw := capWithReferences(
		testCAP("urn:test:alert:warning-ended", "Update", "ended", "2099-06-15T21:30:00-06:00", true),
		"cap-pac@canada.ca,urn:test:alert:watch-original,2026-06-15T15:58:00-06:00",
	)
	ended, err := capmodel.ParseCAP([]byte(endedRaw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.recordCAPAlert(ended, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	rows, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.AlertID == active.Identifier {
			found = true
		}
	}
	if !found {
		t.Fatalf("ended warning removed active watch: %#v", rows)
	}
}

func TestCAPEndedUpdateOverridesActiveAlertByEventAndLocation(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	service := &Service{cfg: cfg}
	now := time.Now().UTC()
	active, err := capmodel.ParseCAP([]byte(testCAP("urn:test:alert:active-no-reference", "Alert", "active", "2099-06-15T21:30:00-06:00", false)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.recordCAPAlert(active, now); err != nil {
		t.Fatal(err)
	}
	ended, err := capmodel.ParseCAP([]byte(testCAP("urn:test:alert:ended-no-reference", "Update", "ended", "2099-06-15T21:30:00-06:00", true)))
	if err != nil {
		t.Fatal(err)
	}
	updates, err := service.recordCAPAlert(ended, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) == 0 || !updates[0].Cancelled {
		t.Fatalf("ended update did not emit cancellation update: %#v", updates)
	}
	if !containsString(updates[0].CancelledIDs, active.Identifier) {
		t.Fatalf("ended update did not override active alert: %#v", updates[0].CancelledIDs)
	}
	rows, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.AlertID == active.Identifier {
			t.Fatalf("overridden active alert remained accepted: %#v", rows)
		}
	}
}

func TestAlertBroadcastAudioPrefersBroadcastMP3Resource(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "cap", "example_civilEmerg_AlertReady_2026_04_10T19_08_12_03_00IA99FB9E1_6FB6_4951_B234_863F1341C4C1.xml"))
	if err != nil {
		t.Fatal(err)
	}
	alert, err := capmodel.ParseCAP(raw)
	if err != nil {
		t.Fatal(err)
	}

	audio := alertBroadcastAudio(alert, "en-CA")

	if audio.URL == "" || !strings.Contains(audio.URL, "naadstts.s3.amazonaws.com") {
		t.Fatalf("broadcast audio URL = %#v", audio)
	}
	if audio.MimeType != "audio/mpeg" {
		t.Fatalf("mime type = %q", audio.MimeType)
	}
}

func TestEndedAlertsArePrunedAfterGrace(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	alert, err := capmodel.ParseCAP([]byte(testCAP("urn:test:alert:ended", "Update", "ended", "2099-06-15T21:30:00-06:00", true)))
	if err != nil {
		t.Fatal(err)
	}
	storeCAPArchiveRecord(cfg.Store, "accepted", capArchiveRecord{ID: alert.Identifier, FeedID: "sk-0001", UpdatedAt: time.Now().UTC().Add(-11 * time.Minute), Alert: alert, RawXML: alert.RawXML})

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "There are no weather alerts currently in effect") {
		t.Fatalf("ended alert should have been pruned:\n%s", product.Text)
	}
	accepted, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 0 {
		t.Fatalf("ended alert remained accepted after grace: %#v", accepted)
	}
	expired, err := cfg.Store.ListCAPArchives(context.Background(), "expired", time.Now().UTC().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].AlertID != alert.Identifier {
		t.Fatalf("ended alert was not archived as expired: %#v", expired)
	}

	_ = cfg.Store.DeleteCAPArchive(context.Background(), alert.Identifier, "sk-0001")
	recentRaw := strings.Replace(
		testCAP("urn:test:alert:recent-ended", "Update", "ended", "2099-06-15T21:30:00-06:00", true),
		"2026-06-15T15:58:00-06:00",
		time.Now().UTC().Format(time.RFC3339),
		1,
	)
	recentEnded, err := capmodel.ParseCAP([]byte(recentRaw))
	if err != nil {
		t.Fatal(err)
	}
	storeCAPArchiveRecord(cfg.Store, "accepted", capArchiveRecord{ID: recentEnded.Identifier, FeedID: "sk-0001", UpdatedAt: time.Now().UTC(), Alert: recentEnded, RawXML: recentEnded.RawXML})
	product, err = newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "Environment Canada has ended a Yellow Warning - Severe Thunderstorm") {
		t.Fatalf("recent ended alert was not rendered:\n%s", product.Text)
	}
}

func TestExpiredAlertsArePrunedAfterExpiryGrace(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	expires := time.Now().UTC().Add(-11 * time.Minute).Format(time.RFC3339)
	alert, err := capmodel.ParseCAP([]byte(testCAP("urn:test:alert:expired", "Alert", "active", expires, false)))
	if err != nil {
		t.Fatal(err)
	}
	storeCAPArchiveRecord(cfg.Store, "accepted", capArchiveRecord{ID: alert.Identifier, FeedID: "sk-0001", UpdatedAt: time.Now().UTC(), Alert: alert, RawXML: alert.RawXML})

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(product.Text, "There are no weather alerts currently in effect") {
		t.Fatalf("expired alert should have been pruned:\n%s", product.Text)
	}
	accepted, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 0 {
		t.Fatalf("expired alert remained accepted after expiry grace: %#v", accepted)
	}
}

func TestBroadAlertsUseConfiguredCoverageRegionNames(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	alert, err := capmodel.ParseCAP([]byte(testBroadCAP()))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: cfg}
	if _, err := service.recordCAPAlert(alert, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}

	wanted := "for areas in and around Martensville, Warman, Rosthern, Delisle, and Wakaw; and areas in and around Outlook, Watrous, Hanley, Imperial, and Dinsmore"
	if !strings.Contains(product.Text, wanted) {
		t.Fatalf("broad alert region text missing %q:\n%s", wanted, product.Text)
	}
	if strings.Contains(product.Text, "R.M. of Corman Park") || strings.Contains(product.Text, "R.M. of Rudy") {
		t.Fatalf("broad alert should not list subregion area names:\n%s", product.Text)
	}
}

func TestBroadAlertCollapsesCompleteFeedRegionsWhenAlertExtendsBeyondFeed(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	outside := `
    <area>
      <areaDesc>R.M. outside the configured listening area</areaDesc>
      <geocode><valueName>layer:EC-MSC-SMC:1.0:CLC</valueName><value>099999</value></geocode>
    </area>`
	alertXML := strings.Replace(testBroadCAP(), "  </info>", outside+"\n  </info>", 1)
	alert, err := capmodel.ParseCAP([]byte(alertXML))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: cfg}
	if _, err := service.recordCAPAlert(alert, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}
	wanted := "for areas in and around Martensville, Warman, Rosthern, Delisle, and Wakaw; and areas in and around Outlook, Watrous, Hanley, Imperial, and Dinsmore"
	if !strings.Contains(product.Text, wanted) {
		t.Fatalf("complete feed regions did not collapse when alert extended beyond the feed:\n%s", product.Text)
	}
	if strings.Contains(product.Text, "R.M. outside the configured listening area") {
		t.Fatalf("out-of-feed area leaked into rendered radio text:\n%s", product.Text)
	}
}

func TestWatchAlertsUseForecastRegionNamesWhenComplete(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	cfg.Feeds[0].Locations.Coverage.Regions = []coverageRegionXML{{
		ID:     "065500",
		Source: "eccc",
		Name:   "Outlook - Watrous - Hanley - Imperial and Dinsmore region",
		Subregions: []coverageSubregionXML{
			{ID: "065514"},
			{ID: "065522"},
		},
	}}
	alert, err := capmodel.ParseCAP([]byte(testWatchCAP("urn:test:watch:complete", "065514,065522")))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: cfg}
	if _, err := service.recordCAPAlert(alert, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}

	wanted := "for areas in and around Outlook, Watrous, Hanley, Imperial, and Dinsmore"
	if !strings.Contains(product.Text, wanted) {
		t.Fatalf("watch alert region text missing %q:\n%s", wanted, product.Text)
	}
	if strings.Contains(product.Text, "R.M. of Fertile Valley") || strings.Contains(product.Text, "R.M. of Rudy") {
		t.Fatalf("complete watch should not list subregion area names:\n%s", product.Text)
	}
}

func TestWatchAlertsKeepSubregionsWhenForecastRegionIncomplete(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	cfg.Feeds[0].Locations.Coverage.Regions = []coverageRegionXML{{
		ID:     "065500",
		Source: "eccc",
		Name:   "Outlook - Watrous - Hanley - Imperial and Dinsmore region",
		Subregions: []coverageSubregionXML{
			{ID: "065514"},
			{ID: "065522"},
		},
	}}
	alert, err := capmodel.ParseCAP([]byte(testWatchCAP("urn:test:watch:partial", "065522")))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: cfg}
	if _, err := service.recordCAPAlert(alert, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(product.Text, "areas in and around Outlook, Watrous, Hanley, Imperial, and Dinsmore") {
		t.Fatalf("partial watch should not compact to the full forecast region:\n%s", product.Text)
	}
	if !strings.Contains(product.Text, "R.M. of Rudy including Outlook and Glenside") {
		t.Fatalf("partial watch should list the covered CLC area:\n%s", product.Text)
	}
}

func TestCAPAlertMatchesExpandedCoverageRegion(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	alert, err := capmodel.ParseCAP([]byte(testOutlookCAP()))
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: cfg}
	updates, err := service.recordCAPAlert(alert, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || !updates[0].Renderable {
		t.Fatalf("updates = %#v", updates)
	}

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: "sk-0001", PackageID: "alerts"})
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{
		"Environment Canada has updated a Yellow Warning - Severe Thunderstorm",
		"R.M. of Fertile Valley including Conquest Macrorie and Bounty",
		"Outlook",
	} {
		if !strings.Contains(product.Text, wanted) {
			t.Fatalf("alert product missing %q:\n%s", wanted, product.Text)
		}
	}
}

func writeFixture(t *testing.T, dir string) {
	t.Helper()
	mustWrite(t, filepath.Join(dir, "config.yaml"), `version: test
feeds_file: managed/configs/feeds.xml
operator:
  on_air_name:
    - text: Canada RadioMET
    - pronunciation: all hazards, canada radio met
services:
  go:
    product_render:
      enabled: true
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="sk-0001" enabled="true" timezone="America/Regina">
    <playout routine="true"/>
    <languages><lang code="en-CA"/></languages>
    <description><lang code="en-CA" text="Automated weather radio." suffix=""/></description>
    <alerts><cap_cp enabled="true"/></alerts>
    <locations>
      <observationLocations>
        <location id="CYXE" source="eccc"/>
        <location id="OUTLOOK" source="eccc" name_override="Outlook"/>
      </observationLocations>
      <coverage>
        <region id="065400" source="eccc" name="Martensville - Warman - Rosthern - Delisle and Wakaw region">
          <subregion id="065435"/>
        </region>
        <region id="065500" source="eccc" name="Outlook - Watrous - Hanley - Imperial and Dinsmore region">
          <subregion id="065522"/>
        </region>
      </coverage>
      <airQualityLocations>
        <location id="HAHJJ" source="eccc"/>
      </airQualityLocations>
      <climateLocations>
        <location id="4057165" source="eccc" name_override="Saskatoon" normal_id="4057120"/>
      </climateLocations>
      <hydrometricLocations>
        <location id="05HG001" source="eccc"/>
        <upstream>
          <location id="05JG006" source="eccc"/>
          <location id="05HD039" source="eccc"/>
        </upstream>
        <downstream>
          <location id="05KD007" source="eccc"/>
        </downstream>
      </hydrometricLocations>
    </locations>
    <transmitter_metadata>
      <transmitter>
        <site_name>Saskatoon</site_name>
        <callsign>XLF322</callsign>
        <frequency_mhz>162.550</frequency_mhz>
      </transmitter>
    </transmitter_metadata>
  </feed>
</feeds>
`)
	mustWrite(t, filepath.Join(dir, "managed", "csv", "FORECAST_LOCATIONS.csv"), `List of all the Forecast Locations by Program / Liste de tous les emplacements de prévisions par programme,,,,,,
CODE,NAME,NOM,PROGRAMS,PROGRAMMES,PROVINCE/WATERBODY 2 PROVINCE/PLAN D'EAU 2,PROVINCE/WATERBODY MOD. PROVINCE/PLAN D'EAU MOD.
065400,Martensville - Warman - Rosthern - Delisle - Wakaw,Martensville - Warman - Rosthern - Delisle - Wakaw,Public,Public,SK,SK
065500,Outlook - Watrous - Hanley - Imperial - Dinsmore,Outlook - Watrous - Hanley - Imperial - Dinsmore,Public,Public,SK,SK
`)
	mustWrite(t, filepath.Join(dir, "managed", "csv", "CLC_Base_Zone.csv"), `CLC,UUID,English,French
065435,fixture,R.M. of Corman Park northeast of the Yellowhead Highway incl. Martensville Warman and Langham,m.r. de Corman Park
065514,fixture,R.M. of Fertile Valley including Conquest Macrorie and Bounty,m.r. de Fertile Valley incluant Conquest Macrorie et Bounty
065522,fixture,R.M. of Rudy including Outlook and Glenside,m.r. de Rudy incluant Outlook et Glenside
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "packages.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<Packages>
  <defaults><enabled>true</enabled><reader_id>00</reader_id></defaults>
  <package id="current_conditions" enabled="true"/>
  <package id="forecast" enabled="true"/>
  <package id="air_quality" enabled="true"/>
  <package id="alerts" enabled="true"/>
  <package id="geophysical_alert" enabled="true"/>
</Packages>
`)
}

func testCAP(identifier string, msgType string, locationStatus string, expires string, ended bool) string {
	response := "Monitor"
	headlineState := "in effect"
	description := "Environment Canada meteorologists are tracking a severe thunderstorm capable of producing very strong wind gusts, nickel size hail and heavy rain."
	if ended {
		response = "AllClear"
		headlineState = "ended"
		description = "The severe thunderstorm warning has ended for the area."
	}
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>` + identifier + `</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-06-15T15:58:00-06:00</sent>
  <status>Actual</status>
  <msgType>` + msgType + `</msgType>
  <scope>Public</scope>
  <references/>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>thunderstorm</event>
    <responseType>` + response + `</responseType>
    <urgency>Immediate</urgency>
    <severity>Moderate</severity>
    <certainty>Likely</certainty>
    <effective>2026-06-15T15:58:00-06:00</effective>
    <onset>2026-06-15T16:00:00-06:00</onset>
    <expires>` + expires + `</expires>
    <senderName>Environment Canada</senderName>
    <headline>yellow warning - severe thunderstorm - ` + headlineState + `</headline>
    <description>` + description + `</description>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Location_Status</valueName><value>` + locationStatus + `</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Name</valueName><value>yellow warning - severe thunderstorm</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Coverage</valueName><value>Saskatchewan</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:MSC_Impact</valueName><value>moderate</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:MSC_Confidence</valueName><value>high</value></parameter>
    <area>
      <areaDesc>City of Saskatoon</areaDesc>
      <geocode><valueName>profile:CAP-CP:Location:0.3</valueName><value>065522</value></geocode>
    </area>
    <area>
      <areaDesc>R.M of Corman Park</areaDesc>
      <geocode><valueName>profile:CAP-CP:Location:0.3</valueName><value>065522</value></geocode>
    </area>
  </info>
</alert>`
}

func testNWSCAP(identifier string, msgType string, sent string, expires string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>` + identifier + `</identifier>
  <sender>w-nws.webmaster@noaa.gov</sender>
  <sent>` + sent + `</sent>
  <status>Actual</status>
  <msgType>` + msgType + `</msgType>
  <scope>Public</scope>
  <code>IPAWSv1.0</code>
  <info>
    <language>en-US</language>
    <category>Met</category>
    <event>Severe Thunderstorm Warning</event>
    <responseType>Shelter</responseType>
    <urgency>Immediate</urgency>
    <severity>Severe</severity>
    <certainty>Observed</certainty>
    <eventCode><valueName>SAME</valueName><value>SVR</value></eventCode>
    <eventCode><valueName>NationalWeatherService</valueName><value>SVW</value></eventCode>
    <effective>` + sent + `</effective>
    <onset>` + sent + `</onset>
    <expires>` + expires + `</expires>
    <senderName>NWS Jackson MS</senderName>
    <headline>Severe Thunderstorm Warning issued June 22 at 12:13PM CDT until June 22 at 12:45PM CDT by NWS Jackson MS</headline>
    <description>At 1212 PM CDT, a severe thunderstorm was located near Fannin.</description>
    <instruction>For your protection move to an interior room on the lowest floor of a building.</instruction>
    <parameter><valueName>EAS-ORG</valueName><value>WXR</value></parameter>
    <area>
      <areaDesc>Rankin, MS</areaDesc>
      <geocode><valueName>SAME</valueName><value>028121</value></geocode>
      <geocode><valueName>UGC</valueName><value>MSC121</value></geocode>
    </area>
  </info>
</alert>`
}

func capWithReferences(raw string, references string) string {
	return strings.Replace(raw, "<references/>", "<references>"+references+"</references>", 1)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func testBroadCAP() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>urn:test:broad-alert</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-06-15T15:58:00-06:00</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <references/>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>snowfall</event>
    <responseType>Monitor</responseType>
    <urgency>Future</urgency>
    <severity>Moderate</severity>
    <certainty>Likely</certainty>
    <effective>2026-06-15T15:58:00-06:00</effective>
    <onset>2026-06-16T06:00:00-06:00</onset>
    <expires>2099-06-15T21:30:00-06:00</expires>
    <senderName>Environment Canada</senderName>
    <headline>yellow warning - snowfall - in effect</headline>
    <description>Snowfall with total amounts of 15 to 25 centimetres is expected.</description>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Location_Status</valueName><value>active</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Name</valueName><value>yellow warning - snowfall</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:Newly_Active_Areas</valueName><value>065435,065514,065522</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:MSC_Impact</valueName><value>moderate</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:MSC_Confidence</valueName><value>high</value></parameter>
    <area>
      <areaDesc>R.M. of Corman Park northeast of the Yellowhead Highway incl. Martensville Warman and Langham</areaDesc>
      <geocode><valueName>layer:EC-MSC-SMC:1.0:CLC</valueName><value>065435</value></geocode>
    </area>
    <area>
      <areaDesc>R.M. of Fertile Valley including Conquest Macrorie and Bounty</areaDesc>
      <geocode><valueName>layer:EC-MSC-SMC:1.0:CLC</valueName><value>065514</value></geocode>
    </area>
    <area>
      <areaDesc>R.M. of Rudy including Outlook and Glenside</areaDesc>
      <geocode><valueName>layer:EC-MSC-SMC:1.0:CLC</valueName><value>065522</value></geocode>
    </area>
  </info>
</alert>`
}

func testWatchCAP(identifier string, newlyActiveAreas string) string {
	areas := map[string]string{
		"065514": "R.M. of Fertile Valley including Conquest Macrorie and Bounty",
		"065522": "R.M. of Rudy including Outlook and Glenside",
	}
	areaXML := strings.Builder{}
	for _, code := range strings.Split(newlyActiveAreas, ",") {
		code = strings.TrimSpace(code)
		if code == "" {
			continue
		}
		areaXML.WriteString(`
    <area>
      <areaDesc>` + areas[code] + `</areaDesc>
      <geocode><valueName>layer:EC-MSC-SMC:1.0:CLC</valueName><value>` + code + `</value></geocode>
    </area>`)
	}
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>` + identifier + `</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-06-15T15:58:00-06:00</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <references/>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>thunderstorm</event>
    <responseType>Monitor</responseType>
    <urgency>Future</urgency>
    <severity>Moderate</severity>
    <certainty>Likely</certainty>
    <effective>2026-06-15T15:58:00-06:00</effective>
    <onset>2026-06-15T16:00:00-06:00</onset>
    <expires>2099-06-15T21:30:00-06:00</expires>
    <senderName>Environment Canada</senderName>
    <headline>yellow watch - severe thunderstorm - in effect</headline>
    <description>Conditions are favourable for severe thunderstorms.</description>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Location_Status</valueName><value>active</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Name</valueName><value>yellow watch - severe thunderstorm</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:Newly_Active_Areas</valueName><value>` + newlyActiveAreas + `</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:MSC_Impact</valueName><value>moderate</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:MSC_Confidence</valueName><value>high</value></parameter>` + areaXML.String() + `
  </info>
</alert>`
}

func testOutlookCAP() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>urn:test:outlook-alert</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-06-16T15:49:00-06:00</sent>
  <status>Actual</status>
  <msgType>Update</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>thunderstorm</event>
    <responseType>Monitor</responseType>
    <urgency>Immediate</urgency>
    <severity>Moderate</severity>
    <certainty>Likely</certainty>
    <effective>2026-06-16T15:49:00-06:00</effective>
    <expires>2099-06-16T18:48:40-06:00</expires>
    <senderName>Environment Canada</senderName>
    <headline>yellow warning - severe thunderstorm - in effect</headline>
    <description>Thunderstorm Location: Near Conquest. Locations in the Path: Conquest Outlook Macrorie.</description>
    <eventCode><valueName>profile:CAP-CP:Event:0.4</valueName><value>thunderstorm</value></eventCode>
    <eventCode><valueName>SAME</valueName><value>SVR</value></eventCode>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Location_Status</valueName><value>active</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Name</valueName><value>yellow warning - severe thunderstorm</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Coverage</valueName><value>Saskatchewan</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:MSC_Impact</valueName><value>moderate</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:MSC_Confidence</valueName><value>high</value></parameter>
    <area>
      <areaDesc>R.M. of Fertile Valley including Conquest Macrorie and Bounty</areaDesc>
      <geocode><valueName>layer:EC-MSC-SMC:1.0:CLC</valueName><value>065514</value></geocode>
      <geocode><valueName>profile:CAP-CP:Location:0.3</valueName><value>4711027</value></geocode>
      <geocode><valueName>profile:CAP-CP:Location:0.3</valueName><value>4712019</value></geocode>
      <geocode><valueName>profile:CAP-CP:Location:0.3</valueName><value>4712022</value></geocode>
    </area>
  </info>
</alert>`
}

func mustWrite(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func loadFixtureConfig(t *testing.T, dir string) loadedConfig {
	t.Helper()
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := datastore.OpenSQLite(context.Background(), datastore.SQLiteConfig{}, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	cfg.Store = store
	return cfg
}

func storeObservationJSON(t *testing.T, store datastore.Store, id string, raw string) {
	t.Helper()
	payload := mustJSONMap(t, raw)
	observedAt, _ := payload["observed_at"].(string)
	if err := store.UpsertObservation(context.Background(), datastore.ObservationRecord{
		Source:        "eccc",
		LocationID:    id,
		StationID:     id,
		ObservedAtRaw: observedAt,
		Payload:       payload,
	}); err != nil {
		t.Fatal(err)
	}
}

func storeForecastJSON(t *testing.T, store datastore.Store, id string, regionID string, raw string) {
	t.Helper()
	payload := mustJSONMap(t, raw)
	issuedAt, _ := payload["issued_at"].(string)
	if err := store.UpsertForecast(context.Background(), datastore.ForecastRecord{
		Source:      "eccc",
		ForecastID:  id,
		RegionID:    regionID,
		IssuedAtRaw: issuedAt,
		Payload:     payload,
	}); err != nil {
		t.Fatal(err)
	}
}

func storeProductPayloadJSON(t *testing.T, store datastore.Store, kind string, source string, id string, raw string) {
	t.Helper()
	if err := store.StoreProductPayload(context.Background(), datastore.ProductPayloadRecord{
		Kind:    kind,
		Source:  source,
		ID:      id,
		Payload: mustJSONMap(t, raw),
	}); err != nil {
		t.Fatal(err)
	}
}

func storeTextProduct(t *testing.T, store datastore.Store, source string, id string, text string) {
	t.Helper()
	if err := store.StoreTextProduct(context.Background(), datastore.TextProductRecord{
		Source: source,
		ID:     id,
		Text:   text,
	}); err != nil {
		t.Fatal(err)
	}
}

func mustJSONMap(t *testing.T, raw string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestOnDemandFeedOverridesCapabilitySpecificLocations(t *testing.T) {
	t.Parallel()
	request := wxOnDemandRequest{
		LocationName:     "Test Municipality",
		ForecastID:       "SK-40",
		StationID:        "CYXE",
		AirQualityID:     "AQSASK",
		ClimateID:        "4057120",
		HydrometricID:    "05HG001",
		MarineForecastID: "088800",
		Latitude:         "52.17",
		Longitude:        "-106.70",
		Language:         "en-CA",
		Telephone:        true,
	}
	feed, err := newRenderer(loadedConfig{}).onDemandFeed(request)
	if err != nil {
		t.Fatal(err)
	}
	if got := feed.Locations.Coverage.Regions[0].DeriveForecast; got != "SK-40" {
		t.Fatalf("forecast = %q", got)
	}
	if got := feed.Locations.ObservationLocations.Locations[0].ID; got != "CYXE" {
		t.Fatalf("observation = %q", got)
	}
	if got := feed.Locations.AirQualityLocations.Locations[0].ID; got != "AQSASK" {
		t.Fatalf("air quality = %q", got)
	}
	if got := feed.Locations.ClimateLocations.Locations[0].ID; got != "4057120" {
		t.Fatalf("climate = %q", got)
	}
	if got := feed.Locations.HydrometricLocations.Locations[0].ID; got != "05HG001" {
		t.Fatalf("hydrometric = %q", got)
	}
	if got := feed.Locations.MarineForecastLocations.Locations[0].ID; got != "088800" {
		t.Fatalf("marine forecast = %q", got)
	}
}

func TestOnDemandFeedDoesNotUseConfiguredClimateOrRiverForUnresolvedCallerLocation(t *testing.T) {
	configured := feedXML{ID: "sk-0001", EnabledRaw: "true"}
	configured.Locations.ClimateLocations.Locations = []locationXML{{ID: "4057165", Source: "eccc"}}
	configured.Locations.HydrometricLocations.Locations = []locationXML{{ID: "05HG001", Source: "eccc"}}
	configured.Locations.HydrometricLocations.Upstream.Locations = []locationXML{{ID: "05HG002", Source: "eccc"}}

	feed, err := newRenderer(loadedConfig{Feeds: []feedXML{configured}}).onDemandFeed(wxOnDemandRequest{
		FeedID:   "sk-0001",
		Code:     "06040",
		Packages: []string{"climate_summary", "hydrometric"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := feed.Locations.ClimateLocations.Locations; len(got) != 1 || got[0] != onDemandUnavailableLocation() {
		t.Fatalf("climate locations = %#v", got)
	}
	if got := feed.Locations.HydrometricLocations.Locations; len(got) != 1 || got[0] != onDemandUnavailableLocation() {
		t.Fatalf("hydrometric locations = %#v", got)
	}
	if len(feed.Locations.HydrometricLocations.Upstream.Locations) != 0 || len(feed.Locations.HydrometricLocations.Downstream.Locations) != 0 {
		t.Fatalf("related hydrometric locations = %#v", feed.Locations.HydrometricLocations)
	}
}
