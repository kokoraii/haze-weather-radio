package ivr

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
)

func TestLocationMenuKeepsProductOptionsVisibleWhenAlertsAreActive(t *testing.T) {
	service, closeStore := ivrAlertTestService(t)
	defer closeStore()
	storeIVRTestCAP(t, service.store, "urn:test:ivr:tor", "sk-0001", "Tornado Warning", "Tornado Warning", "Extreme", "Immediate", "Observed", "high")

	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/twiml", nil)
	response := httptest.NewRecorder()
	service.writeLocationMenu(response, request, ivrTestLocation())

	body := response.Body.String()
	if !strings.Contains(body, "/ivr/v1/alert_audio") || !strings.Contains(body, "kind=location_menu") || !strings.Contains(body, "state=location_option") {
		t.Fatalf("location menu did not include the alert-aware normal menu: %s", body)
	}
	if strings.Contains(body, "kind=menu") || strings.Contains(body, "state=alert_option") {
		t.Fatalf("location menu opened the alert submenu without a star press: %s", body)
	}

	request = formRequest("http://ivr.test/ivr/v1/twiml?state=location_option&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"*"}})
	response = httptest.NewRecorder()
	service.handleLocationOptionTwiML(response, request)

	body = response.Body.String()
	if !strings.Contains(body, "state=alert_option") || !strings.Contains(body, "kind=menu") {
		t.Fatalf("star did not enter the alert submenu: %s", body)
	}

	request = formRequest("http://ivr.test/ivr/v1/twiml?state=alert_option&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"#"}})
	response = httptest.NewRecorder()
	service.handleAlertOptionTwiML(response, request)

	body = response.Body.String()
	if !strings.Contains(body, "state=location_option") || !strings.Contains(body, "kind=location_menu") {
		t.Fatalf("pound did not return from alerts to the normal location menu: %s", body)
	}
}

func TestLocationMenuStarIsUnavailableWithoutActiveAlerts(t *testing.T) {
	service, closeStore := ivrAlertTestService(t)
	defer closeStore()

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=location_option&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"*"}})
	response := httptest.NewRecorder()
	service.handleLocationOptionTwiML(response, request)

	body := response.Body.String()
	if strings.Contains(body, "state=alert_option") || strings.Contains(body, "/ivr/v1/alert_audio") {
		t.Fatalf("star should not expose alert submenu when no alerts are active: %s", body)
	}
}

func TestAlertReadoutCanBeInterruptedWithPound(t *testing.T) {
	service, closeStore := ivrAlertTestService(t)
	defer closeStore()
	storeIVRTestCAP(t, service.store, "urn:test:ivr:interrupt", "sk-0001", "Tornado Warning", "Tornado Warning", "Extreme", "Immediate", "Observed", "high")

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=alert_option&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"1"}})
	response := httptest.NewRecorder()
	service.handleAlertOptionTwiML(response, request)
	body := response.Body.String()
	if !strings.Contains(body, "<Gather") || !strings.Contains(body, `numDigits="1"`) || !strings.Contains(body, "state=alert_readout_option") {
		t.Fatalf("alert readout is not interruptible: %s", body)
	}

	request = formRequest("http://ivr.test/ivr/v1/twiml?state=alert_readout_option&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"#"}})
	response = httptest.NewRecorder()
	service.handleAlertReadoutOptionTwiML(response, request)
	body = response.Body.String()
	if !strings.Contains(body, "state=location_option") || strings.Contains(body, "state=alert_option") {
		t.Fatalf("pound did not exit the alert readout: %s", body)
	}
}

func TestIVRAlertMenuSortsMostCriticalAlertsFirst(t *testing.T) {
	now := time.Now().UTC()
	alerts := []ivrActiveAlert{
		{
			ID:    "statement",
			Title: "Special Weather Statement",
			Alert: capmodel.Alert{Identifier: "statement"},
			Info: capmodel.AlertInfo{
				Event:     "Special Weather Statement",
				Headline:  "Special Weather Statement",
				Severity:  "Minor",
				Urgency:   "Future",
				Certainty: "Possible",
			},
			Score: ivrAlertPriority(capmodel.Alert{}, capmodel.AlertInfo{Event: "Special Weather Statement", Headline: "Special Weather Statement", Severity: "Minor", Urgency: "Future", Certainty: "Possible"}, now.Add(-2*time.Minute)),
		},
		{
			ID:    "watch",
			Title: "Severe Thunderstorm Watch",
			Alert: capmodel.Alert{Identifier: "watch"},
			Info: capmodel.AlertInfo{
				Event:     "Severe Thunderstorm Watch",
				Headline:  "Severe Thunderstorm Watch",
				Severity:  "Moderate",
				Urgency:   "Expected",
				Certainty: "Likely",
			},
			Score: ivrAlertPriority(capmodel.Alert{}, capmodel.AlertInfo{Event: "Severe Thunderstorm Watch", Headline: "Severe Thunderstorm Watch", Severity: "Moderate", Urgency: "Expected", Certainty: "Likely"}, now.Add(-time.Minute)),
		},
		{
			ID:    "tornado",
			Title: "Tornado Warning",
			Alert: capmodel.Alert{Identifier: "tornado"},
			Info: capmodel.AlertInfo{
				Event:     "Tornado Warning",
				Headline:  "Tornado Warning",
				Severity:  "Extreme",
				Urgency:   "Immediate",
				Certainty: "Observed",
			},
			Score: ivrAlertPriority(capmodel.Alert{}, capmodel.AlertInfo{Event: "Tornado Warning", Headline: "Tornado Warning", Severity: "Extreme", Urgency: "Immediate", Certainty: "Observed"}, now.Add(-3*time.Minute)),
		},
	}

	sortIVRAlerts(alerts)

	if alerts[0].ID != "tornado" || alerts[1].ID != "watch" || alerts[2].ID != "statement" {
		t.Fatalf("alerts sorted in wrong criticality order: %#v", []string{alerts[0].ID, alerts[1].ID, alerts[2].ID})
	}
}

func TestIVRAlertReadoutCollapsesVeryLargeAreaLists(t *testing.T) {
	service := &Service{cfg: loadedConfig{Prompts: defaultPromptConfig()}}
	info := capmodel.AlertInfo{
		Language:    "en-CA",
		Event:       "thunderstorm",
		Headline:    "yellow watch - severe thunderstorm - in effect",
		Severity:    "Moderate",
		Urgency:     "Expected",
		Certainty:   "Likely",
		SenderName:  "Environment Canada",
		Description: "Conditions are favourable for severe thunderstorms.",
		Instruction: "Monitor alerts and forecasts.",
		Expires:     "2099-07-02T23:00:00-06:00",
	}
	for i := 0; i < 12; i++ {
		info.Areas = append(info.Areas, capmodel.AlertArea{Description: fmt.Sprintf("Very Long Broad Watch Area %02d", i+1)})
	}
	text := service.alertReadoutText(ivrTestLocation(), ivrActiveAlert{
		ID:    "watch",
		Title: "Yellow Watch - Severe Thunderstorm",
		Alert: capmodel.Alert{
			Identifier:  "watch",
			Sender:      "cap-pac@canada.ca",
			Sent:        "2026-07-02T12:00:00-06:00",
			MessageType: "Alert",
			Infos:       []capmodel.AlertInfo{info},
		},
		Info: info,
	})

	if !strings.Contains(text, "for Saskatoon area") {
		t.Fatalf("large area list was not collapsed to location area:\n%s", text)
	}
	if strings.Count(text, "Very Long Broad Watch Area") > 1 {
		t.Fatalf("large area list leaked into IVR readout:\n%s", text)
	}
}

func TestIVRAlertReadoutCollapsesCompleteForecastRegions(t *testing.T) {
	service := ivrForecastCollapseTestService()
	info := ivrForecastCollapseTestInfo("Severe Thunderstorm Watch")
	got := service.ivrForecastRegionAreaText(ivrTestLocation(), info)
	want := "areas in and around Outlook, Watrous, Hanley, Imperial, and Dinsmore"
	if got != want {
		t.Fatalf("collapsed forecast region = %q, want %q", got, want)
	}
}

func TestIVRAlertReadoutKeepsIncompleteForecastRegions(t *testing.T) {
	service := ivrForecastCollapseTestService()
	info := ivrForecastCollapseTestInfo("Severe Thunderstorm Watch")
	info.Areas = info.Areas[:1]
	if got := service.ivrForecastRegionAreaText(ivrTestLocation(), info); got != "" {
		t.Fatalf("partial forecast region collapsed to %q", got)
	}
}

func TestIVRConvectiveWarningsBypassForecastRegionCollapse(t *testing.T) {
	service := ivrForecastCollapseTestService()
	for _, event := range []string{"Severe Thunderstorm Warning", "Tornado Warning"} {
		info := ivrForecastCollapseTestInfo(event)
		text := service.alertReadoutText(ivrTestLocation(), ivrActiveAlert{
			ID:    event,
			Title: event,
			Alert: capmodel.Alert{Identifier: event, Sender: "cap-pac@canada.ca", Sent: "2026-07-12T12:00:00-06:00", MessageType: "Alert", Infos: []capmodel.AlertInfo{info}},
			Info:  info,
		})
		if strings.Contains(text, "areas in and around Outlook") {
			t.Fatalf("%s used forecast-region collapse:\n%s", event, text)
		}
		if !strings.Contains(text, "R.M. of Fertile Valley") || !strings.Contains(text, "R.M. of Rudy") {
			t.Fatalf("%s did not preserve raw alert locations:\n%s", event, text)
		}
	}
}

func ivrForecastCollapseTestService() *Service {
	feed := feedXML{ID: "sk-0001", EnabledRaw: "true", Timezone: "America/Regina"}
	feed.Locations.Coverage.Regions = []coverageRegionXML{{
		ID:     "065500",
		Source: "eccc",
		Name:   "Outlook - Watrous - Hanley - Imperial - Dinsmore",
		Subregions: []coverageSubregionXML{
			{ID: "065514"},
			{ID: "065522"},
		},
	}}
	return &Service{cfg: loadedConfig{Feeds: []feedXML{feed}, Prompts: defaultPromptConfig()}}
}

func ivrForecastCollapseTestInfo(event string) capmodel.AlertInfo {
	return capmodel.AlertInfo{
		Language:    "en-CA",
		Event:       event,
		Headline:    event + " - in effect",
		Severity:    "Moderate",
		Urgency:     "Immediate",
		Certainty:   "Likely",
		SenderName:  "Environment Canada",
		Description: "Hazardous weather is occurring.",
		Instruction: "Take appropriate precautions.",
		Expires:     "2099-07-12T23:00:00-06:00",
		Areas: []capmodel.AlertArea{
			{Description: "R.M. of Fertile Valley including Conquest Macrorie and Bounty", Geocodes: []capmodel.NameValue{{Name: "CLC", Value: "065514"}}},
			{Description: "R.M. of Rudy including Outlook and Glenside", Geocodes: []capmodel.NameValue{{Name: "CLC", Value: "065522"}}},
		},
	}
}

func ivrAlertTestService(t *testing.T) (*Service, func()) {
	t.Helper()
	dir := t.TempDir()
	store, err := datastore.OpenSQLite(context.Background(), datastore.SQLiteConfig{Path: "haze.db"}, dir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	cfg := loadedConfig{
		BaseDir: dir,
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Feeds:   []feedXML{{ID: "sk-0001", EnabledRaw: "true", Timezone: "America/Regina"}},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{
		cfg:       cfg,
		cache:     NewProductCache(cfg, nil),
		broadcast: recentBroadcastHub("sk-0001"),
		resolver:  NewResolver(cfg),
		store:     store,
	}
	return service, store.Close
}

func ivrTestLocation() ResolvedLocation {
	return ResolvedLocation{
		Code:     "06040",
		Source:   "hello_weather",
		Name:     "Saskatoon",
		Province: "SK",
		FeedID:   "sk-0001",
		Language: "en-CA",
		Timezone: "America/Regina",
		Covered:  true,
	}
}

func storeIVRTestCAP(t *testing.T, store datastore.Store, id string, feedID string, event string, headline string, severity string, urgency string, certainty string, impact string) {
	t.Helper()
	raw := ivrTestCAPXML(id, event, headline, severity, urgency, certainty, impact)
	if err := store.StoreCAPArchive(context.Background(), datastore.CAPArchiveRecord{
		AlertID:      id,
		FeedID:       feedID,
		Bucket:       "accepted",
		Status:       "accepted",
		Event:        event,
		Headline:     headline,
		SentAtRaw:    "2026-06-22T12:00:00-06:00",
		UpdatedAtRaw: "2026-06-22T12:05:00-06:00",
		ExpiresAtRaw: "2099-06-22T13:00:00-06:00",
		RawXML:       raw,
	}); err != nil {
		t.Fatalf("StoreCAPArchive: %v", err)
	}
}

func TestECCCAugust2026ThreatAreasDoNotAlterIVRLocations(t *testing.T) {
	info := capmodel.AlertInfo{
		Areas: []capmodel.AlertArea{
			{Description: "City of Saskatoon", Geocodes: []capmodel.NameValue{{Name: "layer:EC-MSC-SMC:1.0:CLC", Value: "065100"}}},
			{Description: "new active threat area", ThreatStatus: "issued", Geocodes: []capmodel.NameValue{{Name: "profile:CAP-CP:Location:0.3", Value: "4711066"}}},
		},
		Parameters: []capmodel.NameValue{{Name: "layer:EC-MSC-SMC:1.1:Reference_Location_Points", Value: "Saskatoon, Warman"}},
	}
	codes := ivrAlertCoverageCodes(info)
	if len(codes) != 1 {
		t.Fatalf("IVR coverage codes = %#v", codes)
	}
	if _, ok := codes["065100"]; !ok {
		t.Fatalf("IVR coverage codes = %#v, missing zone code", codes)
	}
	names := ivrAlertAreaNames(info)
	if len(names) != 1 || names[0] != "City of Saskatoon" {
		t.Fatalf("IVR area names = %#v", names)
	}
}

func ivrTestCAPXML(id string, event string, headline string, severity string, urgency string, certainty string, impact string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>` + id + `</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-06-22T12:00:00-06:00</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>` + event + `</event>
    <urgency>` + urgency + `</urgency>
    <severity>` + severity + `</severity>
    <certainty>` + certainty + `</certainty>
    <effective>2026-06-22T12:00:00-06:00</effective>
    <expires>2099-06-22T13:00:00-06:00</expires>
    <senderName>Environment Canada</senderName>
    <headline>` + headline + `</headline>
    <description>Test alert description.</description>
    <instruction>Take shelter if threatening weather approaches.</instruction>
    <parameter>
      <valueName>layer:EC-MSC-SMC:1.1:MSC_Impact</valueName>
      <value>` + impact + `</value>
    </parameter>
    <area>
      <areaDesc>City of Saskatoon</areaDesc>
      <geocode>
        <valueName>profile:CAP-CP:Location:0.4</valueName>
        <value>470602</value>
      </geocode>
    </area>
  </info>
</alert>`
}
