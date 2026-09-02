package productrender

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/lead"
)

func TestCAPSAMEPayloadSuppressesCancellations(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	feed, ok := cfg.feedByID("sk-0001")
	if !ok {
		t.Fatal("fixture feed not found")
	}
	alert := parseTestAlert(t, testCAP("urn:test:cancel", "Cancel", "ended", "2099-06-15T21:30:00-06:00", true))

	payload := capSAMEPayload(alert, feed, cfg.BaseDir, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC))

	if payload["include_same"] != false {
		t.Fatalf("include_same = %#v, want false", payload["include_same"])
	}
	if payload["same_suppressed_reason"] != "cancellation" {
		t.Fatalf("same_suppressed_reason = %#v", payload["same_suppressed_reason"])
	}
}

func TestCAPSAMEPayloadDerivesWeatherOriginatorBeforeFeedDefault(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	feed, ok := cfg.feedByID("sk-0001")
	if !ok {
		t.Fatal("fixture feed not found")
	}
	feed.Playout.SAMEOriginator = "EAS"
	alert := parseTestAlert(t, testCAP("urn:test:eccc-originator", "Alert", "active", "2099-06-15T21:30:00-06:00", false))

	payload := capSAMEPayload(alert, feed, cfg.BaseDir, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC))

	if payload["same_originator"] != "WXR" {
		t.Fatalf("same_originator = %#v, want WXR", payload["same_originator"])
	}
	if payload["same_weather_service"] != "Environment Canada" {
		t.Fatalf("same_weather_service = %#v, want Environment Canada", payload["same_weather_service"])
	}
	if payload["same_originator_name"] != "Environment Canada" {
		t.Fatalf("same_originator_name = %#v, want Environment Canada", payload["same_originator_name"])
	}
	if payload["same_event_name"] != "Yellow Warning - Severe Thunderstorm" {
		t.Fatalf("same_event_name = %#v, want Yellow Warning - Severe Thunderstorm", payload["same_event_name"])
	}
}

func TestCAPSAMEPayloadPrefersExplicitCAPSAMEEventForECCCWatch(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	mustWrite(t, filepath.Join(dir, "managed", "sameMapping.json"), `{"naadsToEas":{"tornado":"TOR"}}`)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	feed, ok := cfg.feedByID("sk-0001")
	if !ok {
		t.Fatal("fixture feed not found")
	}
	alert := parseTestAlert(t, testECCCWatchCAP("urn:test:eccc-watch-explicit", `
    <eventCode><valueName>SAME</valueName><value>TOA</value></eventCode>
    <eventCode><valueName>profile:CAP-CP:Event:0.4</valueName><value>tornado</value></eventCode>`))

	payload := capSAMEPayload(alert, feed, cfg.BaseDir, time.Date(2026, 6, 22, 17, 0, 0, 0, time.UTC))

	if payload["same_event"] != "TOA" {
		t.Fatalf("same_event = %#v, want TOA (%#v)", payload["same_event"], payload)
	}
	if payload["same_event_source"] != "cap_event_code" {
		t.Fatalf("same_event_source = %#v, want cap_event_code", payload["same_event_source"])
	}
	if payload["same_event_confidence"] != "high" {
		t.Fatalf("same_event_confidence = %#v, want high", payload["same_event_confidence"])
	}
}

func TestCAPSAMEPayloadUsesWatchMetadataBeforeRawNAADSEvent(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	mustWrite(t, filepath.Join(dir, "managed", "sameMapping.json"), `{"naadsToEas":{"tornado":"TOR"}}`)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	feed, ok := cfg.feedByID("sk-0001")
	if !ok {
		t.Fatal("fixture feed not found")
	}
	alert := parseTestAlert(t, testECCCWatchCAP("urn:test:eccc-watch-classified", `
    <eventCode><valueName>profile:CAP-CP:Event:0.4</valueName><value>tornado</value></eventCode>`))

	payload := capSAMEPayload(alert, feed, cfg.BaseDir, time.Date(2026, 6, 22, 17, 0, 0, 0, time.UTC))

	if payload["same_event"] != "TOA" {
		t.Fatalf("same_event = %#v, want TOA (%#v)", payload["same_event"], payload)
	}
	if payload["same_event_source"] != "cap_alert_class" {
		t.Fatalf("same_event_source = %#v, want cap_alert_class", payload["same_event_source"])
	}
	if payload["same_alert_class"] != "watch" {
		t.Fatalf("same_alert_class = %#v, want watch", payload["same_alert_class"])
	}
	if payload["same_event_phenomenon"] != "tornado" {
		t.Fatalf("same_event_phenomenon = %#v, want tornado", payload["same_event_phenomenon"])
	}
}

func TestCAPSAMEPayloadKeepsConfiguredToneForAlertReadyBroadcastImmediate(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "sameMapping.json"), `{"eas":{"CEM":"Civil Emergency Message"},"naadsToEas":{"civilEmerg":"CEM"}}`)
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "cap", "example_civilEmerg_AlertReady_2026_04_10T19_08_12_03_00IA99FB9E1_6FB6_4951_B234_863F1341C4C1.xml"))
	if err != nil {
		t.Fatal(err)
	}
	alert := parseTestAlert(t, string(raw))
	var feed feedXML
	feed.Playout.SAME = "true"
	feed.Playout.SAMEAttentionTone = "EAS"

	payload := capSAMEPayload(alert, feed, dir, time.Date(2026, 4, 11, 0, 30, 0, 0, time.UTC))

	if payload["include_same"] != true {
		t.Fatalf("include_same = %#v, want true (%#v)", payload["include_same"], payload)
	}
	if payload["same_event"] != "CEM" {
		t.Fatalf("same_event = %#v, want CEM", payload["same_event"])
	}
	if payload["same_originator"] != "CIV" {
		t.Fatalf("same_originator = %#v, want CIV", payload["same_originator"])
	}
	if payload["same_tone"] != "EAS" {
		t.Fatalf("same_tone = %#v, want configured EAS", payload["same_tone"])
	}
	locations, ok := payload["same_locations"].([]string)
	if !ok || len(locations) == 0 || locations[0] != "000000" {
		t.Fatalf("same_locations = %#v, want national code first", payload["same_locations"])
	}
}

func TestCAPLeadPayloadUsesConfiguredCAPParameterRule(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "cap", "example_civilEmerg_AlertReady_2026_04_10T19_08_12_03_00IA99FB9E1_6FB6_4951_B234_863F1341C4C1.xml"))
	if err != nil {
		t.Fatal(err)
	}
	alert := parseTestAlert(t, string(raw))
	info := chooseAlertInfo(alert, "en-CA")
	if info == nil {
		t.Fatal("Alert Ready fixture has no usable info")
	}
	document := lead.Document{Statements: []lead.Statement{{
		Enabled: true,
		Name:    "Alert Ready lead",
		Conditions: []lead.Condition{{
			Type: "if", Key: "layer:SOREM:1.0:Broadcast_Immediately", Equals: "yes",
		}},
		LeadIn:  "audio/NPAS_Preroll.wav",
		LeadOut: "audio/Canadian_Alerting_Attention_Signal.wav",
	}}}
	document, err = lead.Normalize(document)
	if err != nil {
		t.Fatal(err)
	}
	payload := capLeadPayload(document, alert, *info, feedXML{ID: "CAP-IT-ALL"}, []string{"000000"})
	if payload["name"] != "Alert Ready lead" || payload["lead_in"] != "audio/NPAS_Preroll.wav" {
		t.Fatalf("lead payload = %#v", payload)
	}

	nonMatching := document
	nonMatching.Statements[0].Conditions[0].Equals = "no"
	if payload := capLeadPayload(nonMatching, alert, *info, feedXML{}, nil); payload != nil {
		t.Fatalf("nonmatching lead was selected: %#v", payload)
	}
}

func TestNWSCAPWarningGeneratesPrioritySAMEForCatchall(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "sameMapping.json"), `{"eas":{"SVR":"Severe Thunderstorm Warning"}}`)
	var feed feedXML
	feed.ID = "CAP-IT-ALL"
	feed.Playout.SAME = "true"
	feed.Playout.SAMEOriginator = "EAS"
	feed.Alerts.NWSCAP.EnabledRaw = "true"
	feed.Alerts.NWSCAP.Filter.UseFeedLocations = "false"
	feed.Alerts.NWSCAP.Filter.Allowlist.Severities = []string{"Moderate", "Severe", "Extreme"}
	alert := parseTestAlert(t, testNWSCAP("urn:test:nws:svr", "Alert", "2026-06-22T12:13:00-05:00", "2026-06-22T12:45:00-05:00"))
	now := time.Date(2026, 6, 22, 17, 20, 0, 0, time.UTC)

	if !feedAcceptsCAPSource(feed, alert) {
		t.Fatal("catchall feed should accept NWS CAP source")
	}
	if !feedAllowsCAPAlert(feed, alert) {
		t.Fatal("catchall feed should allow severe NWS CAP alert")
	}
	if !capPriorityBroadcastAllowed(alert, feed, dir, now) {
		t.Fatal("fresh NWS severe thunderstorm warning should priority broadcast")
	}
	payload := capSAMEPayload(alert, feed, dir, now)
	if payload["include_same"] != true {
		t.Fatalf("include_same = %#v, want true (%#v)", payload["include_same"], payload)
	}
	if payload["same_event"] != "SVR" {
		t.Fatalf("same_event = %#v, want SVR", payload["same_event"])
	}
	if payload["same_originator"] != "WXR" {
		t.Fatalf("same_originator = %#v, want WXR", payload["same_originator"])
	}
	if payload["same_originator_name"] != "" {
		t.Fatalf("same_originator_name = %#v, want empty so NWS uses weather service label", payload["same_originator_name"])
	}
	if payload["same_weather_service"] != "The National Weather Service" {
		t.Fatalf("same_weather_service = %#v, want The National Weather Service", payload["same_weather_service"])
	}
	if payload["cap_source"] != "nws" {
		t.Fatalf("cap_source = %#v, want nws", payload["cap_source"])
	}
	wantLocations := []string{"028121"}
	if got := payload["same_locations"]; !reflect.DeepEqual(got, wantLocations) {
		t.Fatalf("same_locations = %#v, want %#v", got, wantLocations)
	}
	if payload["same_event_name"] != "Severe Thunderstorm Warning" {
		t.Fatalf("same_event_name = %#v, want Severe Thunderstorm Warning", payload["same_event_name"])
	}
	if payload["same_callsign"] != "CAP-IT-ALL" {
		t.Fatalf("same_callsign = %#v, want CAP-IT-ALL", payload["same_callsign"])
	}
	if payload["same_begins_at"] == "" || payload["same_expires_at"] == "" || payload["same_sent_at"] == "" {
		t.Fatalf("same timing fields missing in payload %#v", payload)
	}
}

func TestCoverageMatchesLinkedSGCToCLC(t *testing.T) {
	db := alertGeoDB{
		CAPCPToCLC: map[string][]string{
			"4711066": []string{"065100"},
		},
	}
	coverage := map[string]struct{}{
		"065100": {},
	}

	if !coverageMatchesAlertCode(db, coverage, "4711066") {
		t.Fatal("expected SGC linked to CLC coverage to match")
	}
}

func TestCoverageMatchesProvinceWildcardPrefix(t *testing.T) {
	db := alertGeoDB{
		CAPCPToCLC: map[string][]string{
			"4711066": []string{"065100"},
		},
	}
	coverage := map[string]struct{}{
		"06*": {},
		"07*": {},
	}

	if !coverageMatchesAlertCode(db, coverage, "065100") {
		t.Fatal("expected direct Saskatchewan CLC coverage to match 06*")
	}
	if !coverageMatchesAlertCode(db, coverage, "4711066") {
		t.Fatal("expected linked Saskatchewan SGC coverage to match 06*")
	}
	if coverageMatchesAlertCode(db, coverage, "081100") {
		t.Fatal("did not expect Manitoba CLC coverage to match 06* or 07*")
	}
}

func TestNWSCAPUsesEASEventNameInsteadOfHeadline(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "sameMapping.json"), `{"eas":{"SPS":"Special Weather Statement"}}`)
	var feed feedXML
	feed.ID = "CAP-IT-ALL"
	feed.Playout.SAME = "true"
	feed.Alerts.NWSCAP.EnabledRaw = "true"
	feed.Alerts.NWSCAP.Filter.UseFeedLocations = "false"
	feed.Alerts.NWSCAP.Filter.Allowlist.Severities = []string{"Moderate", "Severe", "Extreme"}
	raw := testNWSCAP("urn:test:nws:sps", "Alert", "2026-06-22T17:00:00-04:00", "2026-06-22T17:45:00-04:00")
	raw = strings.ReplaceAll(raw, "<event>Severe Thunderstorm Warning</event>", "<event>Special Weather Statement</event>")
	raw = strings.ReplaceAll(raw, "<severity>Severe</severity>", "<severity>Moderate</severity>")
	raw = strings.ReplaceAll(raw, "<eventCode><valueName>SAME</valueName><value>SVR</value></eventCode>", "<eventCode><valueName>SAME</valueName><value>SPS</value></eventCode>")
	raw = strings.ReplaceAll(raw, "Severe Thunderstorm Warning issued June 22 at 12:13PM CDT until June 22 at 12:45PM CDT by NWS Jackson MS", "Special Weather Statement issued June 22 at 5:00PM EDT by NWS Miami FL")
	alert := parseTestAlert(t, raw)

	payload := capSAMEPayload(alert, feed, dir, time.Date(2026, 6, 22, 21, 5, 0, 0, time.UTC))

	if payload["same_event"] != "SPS" {
		t.Fatalf("same_event = %#v, want SPS", payload["same_event"])
	}
	if payload["same_event_name"] != "Special Weather Statement" {
		t.Fatalf("same_event_name = %#v, want Special Weather Statement", payload["same_event_name"])
	}
}

func TestCAPSAMEPayloadKeepsSAMEAfterFreshnessWindow(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	feed, ok := cfg.feedByID("sk-0001")
	if !ok {
		t.Fatal("fixture feed not found")
	}
	alert := parseTestAlert(t, testCAP("urn:test:stale-render", "Alert", "active", "2099-06-15T21:30:00-06:00", false))

	payload := capSAMEPayload(alert, feed, cfg.BaseDir, time.Date(2026, 6, 15, 23, 30, 0, 0, time.UTC))

	if payload["include_same"] != true {
		t.Fatalf("include_same = %#v, want true (%#v)", payload["include_same"], payload)
	}
	if payload["same_suppressed_reason"] != nil {
		t.Fatalf("same_suppressed_reason = %#v, want none", payload["same_suppressed_reason"])
	}
	if payload["same_event"] == "" {
		t.Fatalf("same_event missing in payload %#v", payload)
	}
}

func TestSameOriginatorForCAPDerivesNWSAndCivilAuthorities(t *testing.T) {
	tests := []struct {
		name           string
		alert          capmodel.Alert
		wantOriginator string
		wantService    string
	}{
		{
			name: "nws",
			alert: capmodel.Alert{
				Sender: "alerts.weather.gov",
				Infos: []capmodel.AlertInfo{{
					SenderName: "National Weather Service",
					Event:      "Severe Thunderstorm Warning",
				}},
			},
			wantOriginator: "WXR",
			wantService:    "The National Weather Service",
		},
		{
			name: "civil authority",
			alert: capmodel.Alert{
				Sender: "county-emergency-management@example.gov",
				Infos: []capmodel.AlertInfo{{
					SenderName: "County Emergency Management",
					Event:      "Civil Emergency Message",
				}},
			},
			wantOriginator: "CIV",
			wantService:    "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if len(test.alert.Infos) == 0 {
				t.Fatal("test alert missing info")
			}
			if got := sameOriginatorForCAP(test.alert, test.alert.Infos[0]); got != test.wantOriginator {
				t.Fatalf("same originator = %q, want %q", got, test.wantOriginator)
			}
			if got := sameWeatherServiceForCAP(test.alert); got != test.wantService {
				t.Fatalf("same weather service = %q, want %q", got, test.wantService)
			}
		})
	}
}

func TestCAPPriorityBroadcastUsesFreshnessWindow(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	alert := parseTestAlert(t, testCAP("urn:test:freshness", "Alert", "active", "2099-06-15T21:30:00-06:00", false))
	service := &Service{cfg: cfg}

	freshUpdates, err := service.recordCAPAlert(alert, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(freshUpdates) != 1 || !freshUpdates[0].Broadcast {
		t.Fatalf("fresh updates = %#v", freshUpdates)
	}

	staleUpdates, err := service.recordCAPAlert(alert, time.Date(2026, 6, 15, 23, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(staleUpdates) != 1 {
		t.Fatalf("stale updates = %#v", staleUpdates)
	}
	if staleUpdates[0].Broadcast {
		t.Fatalf("stale alert should not priority broadcast: %#v", staleUpdates[0])
	}
	if !staleUpdates[0].Renderable {
		t.Fatalf("stale active alert should remain routine-renderable until ended/expired: %#v", staleUpdates[0])
	}
}

func TestCAPUpdateBroadcastRequiresFeedRelevantNewlyActiveArea(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.feedByID("sk-0001"); !ok {
		t.Fatal("fixture feed not found")
	}
	cfg.Feeds[0].Locations.Coverage.Regions = []coverageRegionXML{{ID: "065522", Source: "eccc"}}
	service := &Service{cfg: cfg}

	alert := parseTestAlert(t, capAsUpdate(testWatchCAP("urn:test:update-new-feed-area", "065522")))
	updates, err := service.recordCAPAlert(alert, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || !updates[0].Broadcast {
		t.Fatalf("feed-relevant newly active update should priority broadcast once: %#v", updates)
	}

	repeatUpdates, err := service.recordCAPAlert(alert, time.Date(2026, 6, 15, 22, 12, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(repeatUpdates) != 1 || repeatUpdates[0].Broadcast {
		t.Fatalf("same CAP update identifier should not rebroadcast SAME: %#v", repeatUpdates)
	}

	offFeedAlert := parseTestAlert(t, capAsUpdate(testWatchCAP("urn:test:update-off-feed-area", "065514")))
	offFeedUpdates, err := service.recordCAPAlert(offFeedAlert, time.Date(2026, 6, 15, 22, 14, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(offFeedUpdates) != 0 {
		t.Fatalf("off-feed newly active update should not produce feed update: %#v", offFeedUpdates)
	}

	noNewAreaRaw := strings.Replace(testWatchCAP("urn:test:update-no-new-area", "065522"), "<parameter><valueName>layer:EC-MSC-SMC:1.1:Newly_Active_Areas</valueName><value>065522</value></parameter>", "", 1)
	noNewAreaAlert := parseTestAlert(t, capAsUpdate(noNewAreaRaw))
	noNewAreaUpdates, err := service.recordCAPAlert(noNewAreaAlert, time.Date(2026, 6, 15, 22, 16, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(noNewAreaUpdates) != 1 || !noNewAreaUpdates[0].Broadcast {
		t.Fatalf("fresh first-seen CAP update entering feed coverage should priority broadcast: %#v", noNewAreaUpdates)
	}
}

func TestReferencedDLCUpdateBroadcastUsesAuthoritativeLocationDelta(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
		Source: "clc", Code: "065435", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 3, 3)},
	})
	cfg := loadFixtureConfig(t, dir)
	feed := cfg.Feeds[0]
	feed.Locations.Coverage.Regions = []coverageRegionXML{{ID: "065435", Source: "eccc"}}
	feed.Alerts.CapCP.Filter.UseFeedLocations = "true"
	feed.Alerts.CapCP.Filter.CoverageMode = capCoverageModePolygonFirst
	cfg.Feeds = []feedXML{feed}
	service := &Service{cfg: cfg}

	initial := parseTestAlert(t, testDLCGridCAP(
		"urn:test:dlc:initial",
		"Alert",
		"",
		"2026-08-13T03:00:00Z",
		[]dlcGridArea{{Status: "issued", Polygon: capCoverageSquareText(2.2, 0.2, 2.8, 0.8)}},
	))
	initialUpdates, err := service.recordCAPAlert(initial, time.Date(2026, 8, 13, 3, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(initialUpdates) != 1 || !initialUpdates[0].Broadcast {
		t.Fatalf("initial DLC alert = %#v", initialUpdates)
	}
	assertCAPLocations(t, initialUpdates[0].AddedLocations, "065435")

	shifted := parseTestAlert(t, testDLCGridCAP(
		"urn:test:dlc:shifted",
		"Update",
		"cap-pac@canada.ca,urn:test:dlc:initial,2026-08-13T03:00:00Z",
		"2026-08-13T03:02:00Z",
		[]dlcGridArea{
			{Status: "continued", Polygon: capCoverageSquareText(2.2, 0.2, 2.8, 0.8)},
			{Status: "issued", Polygon: capCoverageSquareText(2.2, 2.2, 2.8, 2.8)},
		},
	))
	shiftedUpdates, err := service.recordCAPAlert(shifted, time.Date(2026, 8, 13, 3, 3, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(shiftedUpdates) != 1 || shiftedUpdates[0].Broadcast {
		t.Fatalf("referenced DLC expansion within one full county must not rebroadcast: %#v", shiftedUpdates)
	}
	if len(shiftedUpdates[0].AddedLocations) != 0 {
		t.Fatalf("same county update unexpectedly added locations: %#v", shiftedUpdates[0].AddedLocations)
	}
	assertCAPLocations(t, shiftedUpdates[0].RetainedLocations, "065435")

	continued := parseTestAlert(t, testDLCGridCAP(
		"urn:test:dlc:continued",
		"Update",
		"cap-pac@canada.ca,urn:test:dlc:shifted,2026-08-13T03:02:00Z",
		"2026-08-13T03:04:00Z",
		[]dlcGridArea{
			{Status: "continued", Polygon: capCoverageSquareText(2.2, 0.2, 2.8, 0.8)},
			{Status: "continued", Polygon: capCoverageSquareText(2.2, 2.2, 2.8, 2.8)},
		},
	))
	continuedUpdates, err := service.recordCAPAlert(continued, time.Date(2026, 8, 13, 3, 5, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(continuedUpdates) != 1 || continuedUpdates[0].Broadcast {
		t.Fatalf("continued-only DLC update must not rebroadcast unchanged assignments: %#v", continuedUpdates)
	}
	assertCAPLocations(t, continuedUpdates[0].RetainedLocations, "065435")
}

func TestReferencedDLCUpdateContractionDoesNotBroadcast(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
		Source: "clc", Code: "065435", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 3, 3)},
	})
	cfg := loadFixtureConfig(t, dir)
	feed := cfg.Feeds[0]
	feed.Locations.Coverage.Regions = []coverageRegionXML{{ID: "065435", Source: "eccc"}}
	feed.Alerts.CapCP.Filter.UseFeedLocations = "true"
	feed.Alerts.CapCP.Filter.CoverageMode = capCoverageModePolygonFirst
	cfg.Feeds = []feedXML{feed}
	service := &Service{cfg: cfg}

	initial := parseTestAlert(t, testDLCGridCAP(
		"urn:test:dlc:whole",
		"Alert",
		"",
		"2026-08-13T03:00:00Z",
		[]dlcGridArea{{Status: "issued", Polygon: capCoverageSquareText(-1, -1, 4, 4)}},
	))
	initialUpdates, err := service.recordCAPAlert(initial, time.Date(2026, 8, 13, 3, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(initialUpdates) != 1 || !initialUpdates[0].Broadcast {
		t.Fatalf("whole-area initial DLC alert = %#v", initialUpdates)
	}
	assertCAPLocations(t, initialUpdates[0].AddedLocations, "065435")

	contracted := parseTestAlert(t, testDLCGridCAP(
		"urn:test:dlc:contracted",
		"Update",
		"cap-pac@canada.ca,urn:test:dlc:whole,2026-08-13T03:00:00Z",
		"2026-08-13T03:02:00Z",
		[]dlcGridArea{{Status: "continued", Polygon: capCoverageSquareText(2.2, 0.2, 2.8, 0.8)}},
	))
	contractedUpdates, err := service.recordCAPAlert(contracted, time.Date(2026, 8, 13, 3, 3, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(contractedUpdates) != 1 || contractedUpdates[0].Broadcast {
		t.Fatalf("DLC contraction must not broadcast: %#v", contractedUpdates)
	}
	assertCAPLocations(t, contractedUpdates[0].RetainedLocations, "065435")
}

func TestDLCGridLocationDeltaIsDirectional(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)

	tests := []struct {
		name         string
		previous     []string
		current      []string
		wantAdded    []string
		wantRetained []string
		wantRemoved  []string
	}{
		{
			name:         "whole parent contracts to child",
			previous:     []string{"065435"},
			current:      []string{"165435"},
			wantRetained: []string{"165435"},
			wantRemoved:  []string{"065435"},
		},
		{
			name:         "child expands to whole parent",
			previous:     []string{"165435"},
			current:      []string{"065435"},
			wantAdded:    []string{"065435"},
			wantRetained: nil,
		},
		{
			name:        "different children remain distinct",
			previous:    []string{"165435"},
			current:     []string{"365435"},
			wantAdded:   []string{"365435"},
			wantRemoved: []string{"165435"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			added, retained, removed := capLocationDeltaForBase(test.previous, test.current, dir)
			if !reflect.DeepEqual(added, test.wantAdded) || !reflect.DeepEqual(retained, test.wantRetained) || !reflect.DeepEqual(removed, test.wantRemoved) {
				t.Fatalf("delta = added %#v retained %#v removed %#v, want added %#v retained %#v removed %#v",
					added, retained, removed, test.wantAdded, test.wantRetained, test.wantRemoved)
			}
		})
	}
}

func TestCAPBroadcastTextOnlyNamesFeedMatchedLocations(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Feeds[0].Locations.Coverage.Regions = []coverageRegionXML{{ID: "065522", Source: "eccc"}}
	service := &Service{cfg: cfg}
	alert := parseTestAlert(t, testWatchCAP("urn:test:feed-scoped-broadcast-text", "065514,065522"))

	updates, err := service.recordCAPAlert(alert, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || !updates[0].Broadcast {
		t.Fatalf("updates = %#v", updates)
	}
	if !strings.Contains(updates[0].AlertText, "R.M. of Rudy") {
		t.Fatalf("feed-matched location missing from broadcast text: %q", updates[0].AlertText)
	}
	if strings.Contains(updates[0].AlertText, "R.M. of Fertile Valley") {
		t.Fatalf("off-feed location leaked into broadcast text: %q", updates[0].AlertText)
	}
}

func TestCatchallFirstSeenCAPUpdateBroadcastsWithoutNewlyActiveAreas(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var feed feedXML
	feed.ID = "CAP-IT-ALL"
	feed.EnabledRaw = "true"
	feed.Playout.Routine = "false"
	feed.Alerts.CapCP.EnabledRaw = "true"
	feed.Alerts.CapCP.Filter.UseFeedLocations = "false"
	feed.Alerts.CapCP.Filter.Allowlist.Severities = []string{"Moderate", "Severe", "Extreme"}
	feed.Alerts.CapCP.Filter.Allowlist.Certainties = []string{"Observed", "Likely"}
	cfg.Feeds = []feedXML{feed}
	service := &Service{cfg: cfg}

	raw := strings.Replace(testWatchCAP("urn:test:catchall:first-seen-update", "065522"), "<parameter><valueName>layer:EC-MSC-SMC:1.1:Newly_Active_Areas</valueName><value>065522</value></parameter>", "", 1)
	alert := parseTestAlert(t, capAsUpdate(raw))
	updates, err := service.recordCAPAlert(alert, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || !updates[0].Broadcast {
		t.Fatalf("first-seen catchall update should broadcast: %#v", updates)
	}

	repeatUpdates, err := service.recordCAPAlert(alert, time.Date(2026, 6, 15, 22, 12, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(repeatUpdates) != 1 || repeatUpdates[0].Broadcast {
		t.Fatalf("repeat catchall update should not rebroadcast: %#v", repeatUpdates)
	}
}

func TestCatchallFirstSeenCAPUpdateBroadcastsWithUnrelatedPriorAlerts(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	var feed feedXML
	feed.ID = "CAP-IT-ALL"
	feed.EnabledRaw = "true"
	feed.Playout.Routine = "false"
	feed.Alerts.CapCP.EnabledRaw = "true"
	feed.Alerts.CapCP.Filter.UseFeedLocations = "false"
	raw := strings.Replace(testWatchCAP("urn:test:catchall:first-seen-with-prior", "065522"), "<parameter><valueName>layer:EC-MSC-SMC:1.1:Newly_Active_Areas</valueName><value>065522</value></parameter>", "", 1)
	alert := parseTestAlert(t, capAsUpdate(raw))
	prior := []capRegistryEntry{{
		ID:        "urn:test:other-active-alert",
		UpdatedAt: time.Date(2026, 6, 15, 22, 0, 0, 0, time.UTC),
	}}

	if !capPriorityBroadcastAllowedWithPrior(alert, feed, dir, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC), prior) {
		t.Fatal("first-seen catchall update should broadcast even when another alert is already active")
	}
}

func TestCAPPriorityQueueContainsMatchesAlertAndFeed(t *testing.T) {
	dir := t.TempDir()
	queueDir := filepath.Join(dir, "runtime", "queues", "alerts")
	if err := os.MkdirAll(queueDir, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(queueDir, "000_CAP-IT-ALL_urn_test_cap_same.json"), `{
  "id": "000_CAP-IT-ALL_urn_test_cap_same",
  "alert_id": "urn:test:cap",
  "feed_ids": ["CAP-IT-ALL"],
  "status": "played"
}`)

	if !capPriorityQueueContains(dir, "CAP-IT-ALL", "urn:test:cap") {
		t.Fatal("queue manifest should count as already relayed")
	}
	if capPriorityQueueContains(dir, "sk-0001", "urn:test:cap") {
		t.Fatal("queue manifest should not match a different feed")
	}
	if capPriorityQueueContains(dir, "CAP-IT-ALL", "urn:test:other") {
		t.Fatal("queue manifest should not match a different alert")
	}
}

func TestCAPPriorityBroadcastSuppressesEndedAlerts(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	alert := parseTestAlert(t, testCAP("urn:test:ended", "Update", "ended", "2099-06-15T21:30:00-06:00", true))
	service := &Service{cfg: cfg}

	updates, err := service.recordCAPAlert(alert, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 {
		t.Fatalf("updates = %#v", updates)
	}
	if updates[0].Broadcast {
		t.Fatalf("ended alert should not priority broadcast: %#v", updates[0])
	}
	if !updates[0].Cancelled {
		t.Fatalf("ended alert should emit cancellation cleanup metadata: %#v", updates[0])
	}
}

func TestCAPMixedActiveAndEndedInfosDoNotCancelWholeAlert(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)
	cfg := loadFixtureConfig(t, dir)
	activeRaw := testCAP("urn:test:mixed-active-ended", "Update", "active", "2099-06-15T21:30:00-06:00", false)
	endedRaw := testCAP("urn:test:mixed-ended-info", "Update", "ended", "2099-06-15T21:30:00-06:00", true)
	endedRaw = strings.ReplaceAll(endedRaw, "065522", "066999")
	endedRaw = strings.ReplaceAll(endedRaw, "City of Saskatoon", "Off Feed County")
	infoStart := strings.Index(endedRaw, "<info>")
	infoEnd := strings.LastIndex(endedRaw, "</info>")
	if infoStart < 0 || infoEnd < 0 {
		t.Fatal("ended fixture info block not found")
	}
	endedInfo := endedRaw[infoStart : infoEnd+len("</info>")]
	mixedRaw := strings.Replace(activeRaw, "</alert>", endedInfo+"\n</alert>", 1)
	alert := parseTestAlert(t, mixedRaw)

	if isExplicitCAPEnd(alert) {
		t.Fatal("mixed active and ended CAP update was treated as a global cancellation")
	}
	if containsString(alertCoverageCodes(alert), "066999") {
		t.Fatal("mixed active and ended CAP update used ended area codes for active coverage")
	}
	service := &Service{cfg: cfg}
	updates, err := service.recordCAPAlert(alert, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 {
		t.Fatalf("updates = %#v", updates)
	}
	if updates[0].Cancelled {
		t.Fatalf("mixed active and ended CAP update emitted cancellation metadata: %#v", updates[0])
	}
	rows, err := cfg.Store.ListCAPArchives(context.Background(), "accepted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].AlertID != alert.Identifier {
		t.Fatalf("mixed active and ended CAP update was not retained as accepted: %#v", rows)
	}
}

func TestSameAlertFreshForToneUsesShortWindowForSVRAndTOR(t *testing.T) {
	alert := parseTestAlert(t, testCAP("urn:test:svr", "Alert", "active", "2099-06-15T21:30:00-06:00", false))
	info := chooseAlertInfo(alert, "en-CA")
	if info == nil {
		t.Fatal("alert info not found")
	}
	sentPlus29 := time.Date(2026, 6, 15, 22, 27, 0, 0, time.UTC)
	sentPlus31 := time.Date(2026, 6, 15, 22, 29, 0, 0, time.UTC)

	if !sameAlertFreshForTone(alert, *info, "SVR", sentPlus29) {
		t.Fatal("SVR should allow SAME tones before 30 minutes")
	}
	if sameAlertFreshForTone(alert, *info, "SVR", sentPlus31) {
		t.Fatal("SVR should suppress SAME tones after 30 minutes")
	}
	if !sameAlertFreshForTone(alert, *info, "TOR", sentPlus29) {
		t.Fatal("TOR should allow SAME tones before 30 minutes")
	}
	if sameAlertFreshForTone(alert, *info, "TOR", sentPlus31) {
		t.Fatal("TOR should suppress SAME tones after 30 minutes")
	}
}

func TestSameAlertFreshForToneUsesLongWindowForOtherAlerts(t *testing.T) {
	alert := parseTestAlert(t, testCAP("urn:test:watch", "Alert", "active", "2099-06-15T21:30:00-06:00", false))
	info := chooseAlertInfo(alert, "en-CA")
	if info == nil {
		t.Fatal("alert info not found")
	}
	sentPlus59 := time.Date(2026, 6, 15, 22, 57, 0, 0, time.UTC)
	sentPlus61 := time.Date(2026, 6, 15, 22, 59, 0, 0, time.UTC)

	if !sameAlertFreshForTone(alert, *info, "SVA", sentPlus59) {
		t.Fatal("SVA should allow SAME tones before 60 minutes")
	}
	if sameAlertFreshForTone(alert, *info, "SVA", sentPlus61) {
		t.Fatal("SVA should suppress SAME tones after 60 minutes")
	}
	if !sameAlertFreshForTone(alert, *info, "ADR", sentPlus59) {
		t.Fatal("other SAME events should allow tones before 60 minutes")
	}
	if sameAlertFreshForTone(alert, *info, "ADR", sentPlus61) {
		t.Fatal("other SAME events should suppress tones after 60 minutes")
	}
}

func TestCAPFeedFilterAllowsModerateAndUp(t *testing.T) {
	var feed feedXML
	feed.Alerts.CapCP.EnabledRaw = "true"
	feed.Alerts.CapCP.Filter.Allowlist.Severities = []string{"Moderate", "Severe", "Extreme"}
	alert := parseTestAlert(t, testCAP("urn:test:filter-moderate", "Alert", "active", "2099-06-15T21:30:00-06:00", false))

	if !feedAllowsCAPAlert(feed, alert) {
		t.Fatal("moderate alert should pass moderate-and-up feed filter")
	}

	alert.Infos[0].Severity = "Minor"
	if feedAllowsCAPAlert(feed, alert) {
		t.Fatal("minor alert should be rejected by moderate-and-up feed filter")
	}
}

func TestCAPFeedFilterRequiresStrictCatchallImpact(t *testing.T) {
	var feed feedXML
	feed.Alerts.CapCP.EnabledRaw = "true"
	feed.Alerts.CapCP.Filter.Allowlist.Severities = []string{"Severe", "Extreme"}
	feed.Alerts.CapCP.Filter.Allowlist.Urgencies = []string{"Immediate"}
	feed.Alerts.CapCP.Filter.Allowlist.Certainties = []string{"Observed", "Likely"}
	alert := parseTestAlert(t, testCAP("urn:test:filter-strict-catchall", "Alert", "active", "2099-06-15T21:30:00-06:00", false))
	alert.Infos[0].Severity = "Severe"
	alert.Infos[0].Urgency = "Immediate"
	alert.Infos[0].Certainty = "Likely"

	if !feedAllowsCAPAlert(feed, alert) {
		t.Fatal("severe immediate likely alert should pass strict catchall feed filter")
	}

	for _, tc := range []struct {
		name      string
		severity  string
		urgency   string
		certainty string
	}{
		{name: "moderate", severity: "Moderate", urgency: "Immediate", certainty: "Likely"},
		{name: "expected", severity: "Severe", urgency: "Expected", certainty: "Likely"},
		{name: "possible", severity: "Severe", urgency: "Immediate", certainty: "Possible"},
	} {
		filtered := alert
		filtered.Infos = append([]capmodel.AlertInfo(nil), alert.Infos...)
		filtered.Infos[0].Severity = tc.severity
		filtered.Infos[0].Urgency = tc.urgency
		filtered.Infos[0].Certainty = tc.certainty
		if feedAllowsCAPAlert(feed, filtered) {
			t.Fatalf("%s alert should be rejected by strict catchall feed filter", tc.name)
		}
	}
}

func TestCAPFeedFilterCanDisableCoverageMatching(t *testing.T) {
	var feed feedXML
	feed.Alerts.CapCP.EnabledRaw = "true"
	feed.Alerts.CapCP.Filter.UseFeedLocations = "false"
	alert := parseTestAlert(t, testCAP("urn:test:all-locations", "Alert", "active", "2099-06-15T21:30:00-06:00", false))

	if !alertMatchesFeed(alert, feed, t.TempDir()) {
		t.Fatal("feed with use_feed_locations=false should match every alert location")
	}
}

func TestSameLocationsForCAPUsesNWSFIPSAndZones(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "csv", "NWS_ZONE_COUNTY_CORRELATION.csv"), `STATE|ZONE_CODE|CWA_ID|ZONE_NAME|STATE+ZONE|COUNTY_NAME|FIPS/SAME|TIMEZONE|FE_AREA|LAT|LON
GA|033|FFC|North Fulton|GA033|Fulton|13121|E|nc|33.9350|-84.3557
`)
	var feed feedXML
	info := capmodel.AlertInfo{
		Areas: []capmodel.AlertArea{{
			Geocodes: []capmodel.NameValue{{Name: "UGC", Value: "GAZ033"}},
		}},
	}

	got := sameLocationsForCAP(info, feed, dir)
	want := []string{"013121"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("same locations = %#v, want %#v", got, want)
	}
}

func TestSameLocationCodesForCAPUsesNWSMarineZones(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "csv", "NWS_MARINE_ZONES.csv"), `region,zone_ugc,same_code,name,lon,lat,operational,source_url
AN,ANZ531,073531,Chesapeake Bay from Pooles Island to Sandy Point MD,-76.3446,39.1806,true,https://www.weather.gov/source/gis/Shapefiles/WSOM/mareas20fe25.txt
`)
	db := loadAlertGeoDB(dir)

	if got := sameLocationCodesForAlertCode(db, "ANZ531"); strings.Join(got, ",") != "073531" {
		t.Fatalf("marine same codes = %#v, want 073531", got)
	}
	if got := alertRegionName(db, "073531", "en-CA"); got != "Chesapeake Bay from Pooles Island to Sandy Point MD" {
		t.Fatalf("marine region name = %q", got)
	}
}

func TestSameLocationsForCAPTranslatesSGCToNearestCLC(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "csv", "CAP-CP_Geocodes.csv"), `NAME,NOM,CAPCPGCODE,LAT_DD,LON_DD,CGNDBKEY,PROVINCE_C,COUNTRY_C
Wabamun,,4811045,53.56186389990,-114.47830913600,,AB,CA
`)
	mustWrite(t, filepath.Join(dir, "managed", "csv", "CLC_Base_Zone.csv"), `CLC,UUID,English,French,X1,X2,LAT_DD,LON_DD,X3,X4,X5,PROVINCE_C,COUNTRY_C
076232,fixture,Parkland Co. near Wabamun Carvel and Keephills,Parkland, , ,53.48353858000,-114.38112697000, , , ,AB,CA
031419,fixture,The City of Calgary,Calgary, , ,51.05000000000,-114.06666600000, , , ,AB,CA
`)
	info := capmodel.AlertInfo{
		Areas: []capmodel.AlertArea{{
			Geocodes: []capmodel.NameValue{{Name: "profile:CAP-CP:Location:0.3", Value: "4811045"}},
		}},
	}

	got := sameLocationsForCAP(info, feedXML{}, dir)
	want := []string{"076232"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("same locations = %#v, want %#v", got, want)
	}
}

func TestSameLocationsForCAPFallsBackToNationalWhenNoCodesResolve(t *testing.T) {
	got := sameLocationsForCAP(capmodel.AlertInfo{}, feedXML{}, t.TempDir())
	want := []string{"000000"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("same locations = %#v, want %#v", got, want)
	}
}

func TestSameLocationsForCAPMatchesCoverageAfterSGCTranslation(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "csv", "CAP-CP_Geocodes.csv"), `NAME,NOM,CAPCPGCODE,LAT_DD,LON_DD,CGNDBKEY,PROVINCE_C,COUNTRY_C
Wabamun,,4811045,53.56186389990,-114.47830913600,,AB,CA
`)
	mustWrite(t, filepath.Join(dir, "managed", "csv", "CLC_Base_Zone.csv"), `CLC,UUID,English,French,X1,X2,LAT_DD,LON_DD,X3,X4,X5,PROVINCE_C,COUNTRY_C
076232,fixture,Parkland Co. near Wabamun Carvel and Keephills,Parkland, , ,53.48353858000,-114.38112697000, , , ,AB,CA
`)
	var feed feedXML
	feed.Locations.Coverage.Regions = []coverageRegionXML{{ID: "076232", Source: "eccc"}}
	info := capmodel.AlertInfo{
		Areas: []capmodel.AlertArea{{
			Geocodes: []capmodel.NameValue{{Name: "profile:CAP-CP:Location:0.3", Value: "4811045"}},
		}},
	}

	got := sameLocationsForCAP(info, feed, dir)
	want := []string{"076232"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("same locations = %#v, want %#v", got, want)
	}
}

func TestExpandedNWSCoverageMatchesFIPSAndSAMECodes(t *testing.T) {
	db := alertGeoDB{
		NWS: map[string]nwsZone{
			"GA033": {Code: "GA033", Name: "North Fulton", CountyName: "Fulton", FIPS: "13121"},
		},
		FIPS: map[string]nwsZone{
			"13121":  {Code: "GA033", Name: "North Fulton", CountyName: "Fulton", FIPS: "13121"},
			"013121": {Code: "GA033", Name: "North Fulton", CountyName: "Fulton", FIPS: "13121"},
		},
	}
	coverage := map[string]struct{}{}
	for _, code := range expandNWSRegion(db, "GAZ033") {
		addCoverageCode(coverage, code)
	}

	for _, code := range []string{"GAZ033", "GA033", "13121", "013121"} {
		if !coverageCodeMatches(coverage, code) {
			t.Fatalf("coverage should match %q with expanded NWS zone/FIPS map: %#v", code, coverage)
		}
	}
}

func testECCCWatchCAP(identifier string, eventCodes string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>` + identifier + `</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-06-22T10:48:00-06:00</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>tornado</event>
    ` + eventCodes + `
    <responseType>Monitor</responseType>
    <urgency>Future</urgency>
    <severity>Severe</severity>
    <certainty>Possible</certainty>
    <effective>2026-06-22T10:48:00-06:00</effective>
    <onset>2026-06-22T11:00:00-06:00</onset>
    <expires>2099-06-22T18:00:00-06:00</expires>
    <senderName>Environment Canada</senderName>
    <headline>yellow watch - tornado - in effect</headline>
    <description>Conditions are favourable for the development of tornadoes.</description>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Type</valueName><value>watch</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Name</valueName><value>yellow watch - tornado</value></parameter>
    <area>
      <areaDesc>City of Saskatoon</areaDesc>
      <geocode><valueName>profile:CAP-CP:Location:0.3</valueName><value>065522</value></geocode>
    </area>
  </info>
</alert>`
}

func capAsUpdate(raw string) string {
	return strings.Replace(raw, "<msgType>Alert</msgType>", "<msgType>Update</msgType>", 1)
}

type dlcGridArea struct {
	Status  string
	Polygon string
}

func testDLCGridCAP(identifier string, messageType string, references string, sent string, threatAreas []dlcGridArea) string {
	var areas strings.Builder
	for _, area := range threatAreas {
		areas.WriteString(`
    <area>
      <areaDesc>` + area.Status + ` threat area</areaDesc>
      <polygon>` + area.Polygon + `</polygon>
      <geocode><valueName>layer:EC-MSC-SMC:DLC:1.1</valueName><value>` + area.Status + `</value></geocode>
    </area>`)
	}
	referenceXML := "<references/>"
	if strings.TrimSpace(references) != "" {
		referenceXML = "<references>" + references + "</references>"
	}
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>` + identifier + `</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>` + sent + `</sent>
  <status>Actual</status>
  <msgType>` + messageType + `</msgType>
  <scope>Public</scope>
  ` + referenceXML + `
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>thunderstorm</event>
    <responseType>Shelter</responseType>
    <urgency>Immediate</urgency>
    <severity>Severe</severity>
    <certainty>Observed</certainty>
    <effective>` + sent + `</effective>
    <onset>` + sent + `</onset>
    <expires>2099-08-13T04:00:00Z</expires>
    <senderName>Environment Canada</senderName>
    <headline>yellow warning - severe thunderstorm - in effect</headline>
    <description>A dangerous severe thunderstorm is moving through the warned area.</description>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Location_Status</valueName><value>active</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.0:Alert_Name</valueName><value>yellow warning - severe thunderstorm</value></parameter>
    <area>
      <areaDesc>Test CLC</areaDesc>
      <polygon>` + capCoverageSquareText(0, 0, 3, 3) + `</polygon>
      <geocode><valueName>layer:EC-MSC-SMC:1.0:CLC</valueName><value>065435</value></geocode>
    </area>` + areas.String() + `
  </info>
</alert>`
}
