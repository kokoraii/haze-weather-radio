package webgateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
)

func TestWithArchiveStoreUsesSQLiteDefault(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`storage:
  sqlite:
    enabled: true
    path: runtime/state/haze.db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	called := false

	err := withArchiveStore(configPath, func(context.Context, datastore.Store) error {
		called = true
		return nil
	})

	if err != nil {
		t.Fatalf("withArchiveStore returned %v", err)
	}
	if !called {
		t.Fatal("archive callback was not called")
	}
}

func TestArchiveSAMEAllowedSuppressesCancellationsAndStaleWarnings(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	cancel := parseArchiveTestAlert(t, archiveTestCAP("urn:test:cancel", "Cancel", "yellow warning - severe thunderstorm - ended", "2099-06-15T21:30:00-06:00", true))
	if archiveSAMEAllowed(configPath, cancel, time.Date(2026, 6, 15, 22, 10, 0, 0, time.UTC)) {
		t.Fatal("cancellation should not be eligible for SAME rebroadcast")
	}

	warning := parseArchiveTestAlert(t, archiveTestCAP("urn:test:svr", "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false))
	if !archiveSAMEAllowed(configPath, warning, time.Date(2026, 6, 15, 22, 27, 0, 0, time.UTC)) {
		t.Fatal("fresh severe thunderstorm warning should be eligible for SAME")
	}
	if archiveSAMEAllowed(configPath, warning, time.Date(2026, 6, 15, 22, 29, 0, 0, time.UTC)) {
		t.Fatal("stale severe thunderstorm warning should not be eligible for SAME")
	}
}

func TestArchiveRecordOriginatorUsesCAPPolicyMetadata(t *testing.T) {
	record := archiveCAPRecord{Alert: capmodel.Alert{
		Sender: "alerts.example.test",
		Infos:  []capmodel.AlertInfo{{Parameters: []capmodel.NameValue{{Name: "eas-org", Value: "EAS"}}}},
	}}
	if got := archiveRecordOriginator(record); got != "EAS" {
		t.Fatalf("archive originator = %q, want EAS", got)
	}
	record.Alert.Infos[0].Parameters = nil
	record.Alert.Sender = "cap-pac@canada.ca"
	if got := archiveRecordOriginator(record); got != "WXR" {
		t.Fatalf("weather archive originator = %q, want WXR", got)
	}
}

func TestArchiveRebroadcastEventDataCarriesCompletePriorityAlertPayload(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	alert := parseArchiveTestAlert(t, archiveTestCAP("urn:test:force-svr", "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false))
	record := archiveCAPRecord{
		ID:         "urn:test:force-svr",
		FeedID:     "CAP-IT-ALL",
		Status:     "rejected",
		Alert:      alert,
		BannerText: "Custom archive banner text",
	}

	data, withSAME, audio := archiveRebroadcastEventData(configPath, record, true, true, time.Date(2026, 6, 15, 22, 0, 0, 0, time.UTC))

	if !withSAME {
		t.Fatal("forced archive broadcast should keep SAME enabled when the alert can be mapped")
	}
	if audio.URL != "" {
		t.Fatalf("test CAP has no audio resource, got %q", audio.URL)
	}
	if got := stringPayload(data, "feed_id", ""); got != "CAP-IT-ALL" {
		t.Fatalf("feed_id = %q, want CAP-IT-ALL", got)
	}
	if got := stringPayload(data, "same_event", ""); got == "" {
		t.Fatalf("same_event was not populated: %#v", data)
	}
	if got := stringSlicePayload(data, "same_locations"); len(got) != 1 || got[0] != "065522" {
		t.Fatalf("same_locations = %#v, want [065522]", got)
	}
	if includeSAME, _ := data["include_same"].(bool); !includeSAME {
		t.Fatalf("include_same = %#v, want true", data["include_same"])
	}
	if force, _ := data["force_broadcast"].(bool); !force {
		t.Fatalf("force_broadcast = %#v, want true", data["force_broadcast"])
	}
	if got := stringPayload(data, "banner_text", ""); got != "Custom archive banner text" {
		t.Fatalf("banner_text = %q", got)
	}
	if _, ok := data["alert_packet"]; !ok {
		t.Fatalf("alert_packet was not attached: %#v", data)
	}
}

func TestArchiveRecordPayloadIncludesCAPXMLURLAndSAMEPreviewFlag(t *testing.T) {
	baseDir := t.TempDir()
	alert := parseArchiveTestAlert(t, archiveTestCAP("urn:test:svr", "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false))
	payload := archiveRecordPayload(archiveCAPRecord{
		ID:     "urn:test:svr",
		FeedID: "sk-0001",
		Status: "accepted",
		Alert:  alert,
	}, "accepted", baseDir)

	if got := strings.TrimSpace(payload["cap_xml_url"].(string)); !strings.Contains(got, "/api/v1/alerts/archive/cap.xml?") || !strings.Contains(got, "feed_id=sk-0001") {
		t.Fatalf("cap_xml_url was not populated correctly: %q", got)
	}
	if available, _ := payload["same_preview_available"].(bool); !available {
		t.Fatal("SAME preview should be available for a mappable warning")
	}
	if relayed, _ := payload["relayed"].(bool); relayed {
		t.Fatal("archive record without queued material should not be marked relayed")
	}
	if got := payload["broadcast_action_label"]; got != "Force Broadcast" {
		t.Fatalf("broadcast label = %v, want Force Broadcast", got)
	}
}

func TestArchiveBroadcastAudioOnlyAllowsWebURLs(t *testing.T) {
	alert := capmodel.Alert{Infos: []capmodel.AlertInfo{{
		Resources: []capmodel.Resource{
			{MimeType: "audio/mpeg", URI: "javascript:alert(1)"},
			{MimeType: "audio/mpeg", URI: "file:///C:/Windows/win.ini"},
			{Description: "Audio clip", DerefURI: "https://alerts.example.test/audio.mp3"},
		},
	}}}

	audio := archiveBroadcastAudio(alert)
	if audio.URL != "https://alerts.example.test/audio.mp3" {
		t.Fatalf("audio URL = %q, want allowed https resource", audio.URL)
	}
}

func TestArchiveBroadcastAudioRejectsNonWebURLs(t *testing.T) {
	alert := capmodel.Alert{Infos: []capmodel.AlertInfo{{
		Resources: []capmodel.Resource{
			{MimeType: "audio/wav", URI: "data:audio/wav;base64,UklGRg=="},
			{MimeType: "audio/wav", URI: "//alerts.example.test/audio.wav"},
			{Description: "Audio clip", DerefURI: "ftp://alerts.example.test/audio.mp3"},
		},
	}}}

	if audio := archiveBroadcastAudio(alert); audio.URL != "" {
		t.Fatalf("non-web CAP audio URL should be rejected, got %q", audio.URL)
	}
}

func TestPublicArchivePayloadUsesPublicCAPXMLURL(t *testing.T) {
	publicPayload := publicArchiveCAPXMLLinks([]map[string]any{{
		"id":          "urn:test:xml",
		"feed_id":     "sk-0001",
		"cap_xml_url": "https://example.invalid/api/v1/alerts/archive/cap.xml?id=urn%3Atest%3Axml&feed_id=sk-0001&token=secret&download=1",
	}, {
		"id":          "urn:test:already-public",
		"feed_id":     "CAP-IT-ALL",
		"cap_xml_url": "/api/public/v1/alerts/archive/cap.xml?id=urn%3Atest%3Aalready-public&feed_id=CAP-IT-ALL&token=secret",
	}, {
		"id":          "urn:test:unsafe",
		"feed_id":     "sk-0001",
		"cap_xml_url": "javascript:alert(1)",
	}}).([]map[string]any)

	got := strings.TrimSpace(fmt.Sprint(publicPayload[0]["cap_xml_url"]))
	if got != "/api/public/v1/alerts/archive/cap.xml?id=urn%3Atest%3Axml&feed_id=sk-0001" {
		t.Fatalf("public cap_xml_url = %q", got)
	}
	got = strings.TrimSpace(fmt.Sprint(publicPayload[1]["cap_xml_url"]))
	if got != "/api/public/v1/alerts/archive/cap.xml?id=urn%3Atest%3Aalready-public&feed_id=CAP-IT-ALL" {
		t.Fatalf("already-public cap_xml_url = %q", got)
	}
	if got := strings.TrimSpace(fmt.Sprint(publicPayload[2]["cap_xml_url"])); got != "" {
		t.Fatalf("unsafe public cap_xml_url should be stripped, got %q", got)
	}
}

func TestPublicArchiveRecordMapsFlattensLegacyObjectBuckets(t *testing.T) {
	longDescription := strings.Repeat("severe weather details ", 400) + strings.Repeat("🌩️", 20)
	areas := make([]string, 0, publicArchiveAreaLimit+5)
	for i := 0; i < publicArchiveAreaLimit+5; i++ {
		areas = append(areas, fmt.Sprintf("Area %02d with a public-facing weather location name", i+1))
	}
	records := publicArchiveRecordMaps(map[string]any{
		"first": map[string]any{
			"id":          "urn:test:first",
			"feed_id":     "sk-0001",
			"description": longDescription,
			"areas":       areas,
			"area_text":   strings.Join(areas, "; "),
			"same_event":  "SVR",
			"audio_url":   "javascript:alert(1)",
			"cap_xml_url": "/api/v1/alerts/archive/cap.xml?id=urn%3Atest%3Afirst",
		},
		"group": map[string]any{
			"second": map[string]any{
				"id":        "urn:test:second",
				"feed_id":   "sk-0001",
				"audio_url": "https://alerts.example.test/audio.mp3",
			},
		},
	})

	if len(records) != 2 {
		t.Fatalf("records len = %d, want 2: %#v", len(records), records)
	}
	if records[0]["id"] == "" || records[1]["id"] == "" {
		t.Fatalf("flattened records lost ids: %#v", records)
	}
	var first map[string]any
	for _, record := range records {
		if record["id"] == "urn:test:first" {
			first = record
			break
		}
	}
	if first == nil {
		t.Fatalf("first record not found: %#v", records)
	}
	if _, ok := first["same_event"]; ok {
		t.Fatalf("public archive record leaked admin SAME fields: %#v", first)
	}
	if _, ok := first["audio_url"]; ok {
		t.Fatalf("public archive record should strip unsafe audio_url: %#v", first)
	}
	if got := fmt.Sprint(first["description"]); len(got) > publicArchiveTextFieldLimit+3 || !strings.HasSuffix(got, "...") {
		t.Fatalf("description was not capped for public payload: len=%d suffix=%q", len(got), got[len(got)-min(len(got), 3):])
	}
	if !utf8.ValidString(fmt.Sprint(first["description"])) {
		t.Fatalf("description truncation produced invalid UTF-8")
	}
	if _, ok := first["area_text"]; ok {
		t.Fatalf("area_text should be omitted when compact public areas are available: %#v", first)
	}
	publicAreas, _ := first["areas"].([]string)
	if got := len(publicAreas); got != publicArchiveAreaLimit+1 {
		t.Fatalf("public areas len = %d, want %d: %#v", got, publicArchiveAreaLimit+1, publicAreas)
	}
	if !strings.HasPrefix(publicAreas[len(publicAreas)-1], "and 5 more areas") {
		t.Fatalf("public areas did not include truncation marker: %#v", publicAreas[len(publicAreas)-1])
	}
	var second map[string]any
	for _, record := range records {
		if record["id"] == "urn:test:second" {
			second = record
			break
		}
	}
	if second == nil || second["audio_url"] != "https://alerts.example.test/audio.mp3" {
		t.Fatalf("public archive record should preserve safe HTTPS audio_url: %#v", second)
	}
}

func TestPublicAlertsArchivePayloadUsesPublicCAPXMLURL(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`storage:
  sqlite:
    enabled: true
    path: runtime/state/haze.db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	rawCAP := archiveTestCAP("urn:test:payload-xml", "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false)
	if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
		return store.StoreCAPArchive(ctx, datastore.CAPArchiveRecord{
			AlertID: "urn:test:payload-xml",
			FeedID:  "sk-0001",
			Status:  "accepted",
			RawXML:  rawCAP,
		})
	}); err != nil {
		t.Fatal(err)
	}

	payload, err := publicAlertsArchivePayload(configPath)
	if err != nil {
		t.Fatal(err)
	}
	byFeed := payload["by_feed"].(map[string][]map[string]any)
	records := byFeed["sk-0001"]
	record := records[0]
	got := strings.TrimSpace(fmt.Sprint(record["cap_xml_url"]))
	if !strings.Contains(got, "/api/public/v1/alerts/archive/cap.xml?") || strings.Contains(got, "/api/v1/alerts/archive/cap.xml?") {
		t.Fatalf("public payload cap_xml_url = %q", got)
	}
}

func TestPublicArchiveCAPXMLRejectsInvalidFeedID(t *testing.T) {
	dir := t.TempDir()
	config := Config{}
	config.Webpanel.Public.AlertsArchive.Access = "public"
	server := NewServerWithConfigPath(config, filepath.Join(dir, "config.yaml"), dir)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/public/v1/alerts/archive/cap.xml?id=urn:test&feed_id=..%2Fconfig.yaml", nil)

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestPublicAlertsArchivePayloadIsBounded(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`storage:
  sqlite:
    enabled: true
    path: runtime/state/haze.db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
		for i := 0; i < publicArchiveAcceptedPerFeedLimit+5; i++ {
			rawCAP := archiveTestCAP(fmt.Sprintf("urn:test:accepted:%03d", i), "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false)
			if err := store.StoreCAPArchive(ctx, datastore.CAPArchiveRecord{
				AlertID: fmt.Sprintf("urn:test:accepted:%03d", i),
				FeedID:  "sk-0001",
				Bucket:  "accepted",
				Status:  "accepted",
				RawXML:  rawCAP,
			}); err != nil {
				return err
			}
		}
		for i := 0; i < publicArchiveBucketLimit+5; i++ {
			rawCAP := archiveTestCAP(fmt.Sprintf("urn:test:rejected:%03d", i), "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false)
			if err := store.StoreCAPArchive(ctx, datastore.CAPArchiveRecord{
				AlertID: fmt.Sprintf("urn:test:rejected:%03d", i),
				FeedID:  "sk-0001",
				Bucket:  "rejected",
				Status:  "rejected",
				RawXML:  rawCAP,
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	payload, err := publicAlertsArchivePayload(configPath)
	if err != nil {
		t.Fatal(err)
	}
	byFeed := payload["by_feed"].(map[string][]map[string]any)
	if got := len(byFeed["sk-0001"]); got != publicArchiveAcceptedPerFeedLimit {
		t.Fatalf("accepted records = %d, want %d", got, publicArchiveAcceptedPerFeedLimit)
	}
	rejected := payload["rejected"].([]map[string]any)
	if got := len(rejected); got != publicArchiveBucketLimit {
		t.Fatalf("rejected records = %d, want %d", got, publicArchiveBucketLimit)
	}
	truncated := payload["truncated"].(map[string]any)
	if truncated["accepted"] != true || truncated["rejected"] != true {
		t.Fatalf("truncated flags = %#v", truncated)
	}
}

func TestPublicStateOnlyIncludesAlertsArchiveWhenRequested(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`feeds_file: managed/configs/feeds.xml
outputs_file: managed/configs/output.xml
webpanel:
  public:
    alerts_archive:
      access: public
storage:
  sqlite:
    enabled: true
    path: runtime/state/haze.db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="sk-0001" enabled="true">
    <transmitter_metadata><transmitter><site_name>Saskatoon</site_name></transmitter></transmitter_metadata>
  </feed>
</feeds>
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "output.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<outputs><feed id="sk-0001"><webrtc enabled="true"/></feed></outputs>
`)
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	rawCAP := archiveTestCAP("urn:test:scoped-archive", "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false)
	if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
		return store.StoreCAPArchive(ctx, datastore.CAPArchiveRecord{
			AlertID: "urn:test:scoped-archive",
			FeedID:  "sk-0001",
			Status:  "accepted",
			RawXML:  rawCAP,
		})
	}); err != nil {
		t.Fatal(err)
	}

	home, err := publicStatePayload(config, configPath, time.Now().UTC(), httptest.NewRequest(http.MethodGet, "/api/public/v1/panel/ws", nil), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	homeSummary := home["summary"].(map[string]any)
	if _, ok := homeSummary["alerts"]; ok {
		t.Fatalf("homepage public state should not include alert records: %#v", homeSummary["alerts"])
	}
	if homeSummary["alerts_archive"] != "public" {
		t.Fatalf("homepage public state should retain alert capability: %#v", homeSummary)
	}

	alerts, err := publicStatePayload(config, configPath, time.Now().UTC(), httptest.NewRequest(http.MethodGet, "/api/public/v1/panel/ws?alerts=1", nil), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	alertSummary := alerts["summary"].(map[string]any)
	if _, ok := alertSummary["alerts"]; !ok {
		t.Fatalf("alerts public state should include alert records: %#v", alertSummary)
	}
	if _, ok := alertSummary["alerts_archive_data"]; ok {
		t.Fatalf("alerts public state should not duplicate archive data under legacy key")
	}
}

func TestPublicAlertStateCachesArchiveWithinTTL(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`feeds_file: managed/configs/feeds.xml
outputs_file: managed/configs/output.xml
webpanel:
  public:
    alerts_archive:
      access: public
storage:
  sqlite:
    enabled: true
    path: runtime/state/haze.db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="sk-0001" enabled="true">
    <transmitter_metadata><transmitter><site_name>Saskatoon</site_name></transmitter></transmitter_metadata>
  </feed>
</feeds>
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "output.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<outputs><feed id="sk-0001"><webrtc enabled="true"/></feed></outputs>
`)
	storeArchive := func(id string) {
		t.Helper()
		rawCAP := archiveTestCAP(id, "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false)
		if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
			return store.StoreCAPArchive(ctx, datastore.CAPArchiveRecord{
				AlertID: id,
				FeedID:  "sk-0001",
				Status:  "accepted",
				RawXML:  rawCAP,
			})
		}); err != nil {
			t.Fatal(err)
		}
	}
	storeArchive("urn:test:cached-one")
	session := &wsSession{
		config:     config,
		configPath: configPath,
		startedAt:  time.Now().UTC(),
		request:    httptest.NewRequest(http.MethodGet, "/api/public/v1/panel/ws?alerts=1", nil),
		auth:       NewAuthManager(config),
		media:      NewMediaHub(""),
	}
	first, err := session.publicState()
	if err != nil {
		t.Fatal(err)
	}
	if got := publicAcceptedArchiveCount(first); got != 1 {
		t.Fatalf("initial accepted public archive count = %d, want 1", got)
	}

	storeArchive("urn:test:cached-two")
	cached, err := session.publicState()
	if err != nil {
		t.Fatal(err)
	}
	if got := publicAcceptedArchiveCount(cached); got != 1 {
		t.Fatalf("cached accepted public archive count = %d, want 1", got)
	}

	session.publicStateCacheAt = time.Now().Add(-publicAlertStateCacheTTL - time.Second)
	refreshed, err := session.publicState()
	if err != nil {
		t.Fatal(err)
	}
	if got := publicAcceptedArchiveCount(refreshed); got != 2 {
		t.Fatalf("refreshed accepted public archive count = %d, want 2", got)
	}
}

func publicAcceptedArchiveCount(state map[string]any) int {
	summary, _ := state["summary"].(map[string]any)
	alerts, _ := summary["alerts"].(map[string]any)
	byFeed, _ := alerts["by_feed"].(map[string][]map[string]any)
	total := 0
	for _, records := range byFeed {
		total += len(records)
	}
	return total
}

func TestArchiveRecordPayloadMarksQueuedAlertsRelayed(t *testing.T) {
	baseDir := t.TempDir()
	queueDir := filepath.Join(baseDir, "runtime", "queues", "alerts")
	if err := os.MkdirAll(queueDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(queueDir, "001_sk-0001_urn_test_svr_cap.json"), []byte(`{
  "id": "001_sk-0001_urn_test_svr_cap",
  "alert_id": "urn:test:svr",
  "feed_ids": ["sk-0001"],
  "status": "played"
}`), 0o644); err != nil {
		t.Fatal(err)
	}
	alert := parseArchiveTestAlert(t, archiveTestCAP("urn:test:svr", "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false))
	payload := archiveRecordPayload(archiveCAPRecord{
		ID:     "urn:test:svr",
		FeedID: "sk-0001",
		Status: "accepted",
		Alert:  alert,
	}, "accepted", baseDir)

	if relayed, _ := payload["relayed"].(bool); !relayed {
		t.Fatal("queued archive record should be marked relayed")
	}
	if got := payload["broadcast_action_label"]; got != "Rebroadcast" {
		t.Fatalf("broadcast label = %v, want Rebroadcast", got)
	}
}

func TestAlertsArchiveCAPXMLRequiresAuthAndServesStoredXML(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`webpanel:
  authentication:
    enabled: true
storage:
  sqlite:
    enabled: true
    path: runtime/state/haze.db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(Config{}, configPath, dir)
	authEnabled := true
	authConfig := Config{}
	authConfig.Webpanel.Authentication.Enabled = &authEnabled
	server.auth = NewAuthManager(authConfig)
	server.auth.password = []byte("secret")
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	rawCAP := archiveTestCAP("urn:test:xml", "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false)
	if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
		return store.StoreCAPArchive(ctx, datastore.CAPArchiveRecord{
			AlertID: "urn:test:xml",
			FeedID:  "sk-0001",
			Status:  "accepted",
			RawXML:  rawCAP,
		})
	}); err != nil {
		t.Fatal(err)
	}

	unauthorized := httptest.NewRecorder()
	server.alertsArchiveCAPXML(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/alerts/archive/cap.xml?id=urn:test:xml&feed_id=sk-0001", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized status, got %d", unauthorized.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/alerts/archive/cap.xml?id=urn:test:xml&feed_id=sk-0001&token="+token, nil)
	response := httptest.NewRecorder()
	server.alertsArchiveCAPXML(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); !strings.Contains(got, "application/cap+xml") {
		t.Fatalf("unexpected content type %q", got)
	}
	if !strings.Contains(response.Body.String(), "<identifier>urn:test:xml</identifier>") {
		t.Fatal("served CAP XML did not contain the archived alert")
	}
}

func TestPublicAlertsArchiveCAPXMLIsGatedByPublicArchiveAccess(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`webpanel:
  public:
    alerts_archive:
      access: public
storage:
  sqlite:
    enabled: true
    path: runtime/state/haze.db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, dir)
	rawCAP := archiveTestCAP("urn:test:public-xml", "Alert", "yellow warning - severe thunderstorm - in effect", "2099-06-15T21:30:00-06:00", false)
	if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
		return store.StoreCAPArchive(ctx, datastore.CAPArchiveRecord{
			AlertID: "urn:test:public-xml",
			FeedID:  "sk-0001",
			Status:  "accepted",
			RawXML:  rawCAP,
		})
	}); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/public/v1/alerts/archive/cap.xml?id=urn:test:public-xml&feed_id=sk-0001", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("expected public CAP XML 200, got %d: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("public CAP XML Cache-Control = %q, want no-store", got)
	}
	if got := response.Header().Get("Content-Disposition"); !strings.Contains(got, `inline; filename="cap-urn_test_public-xml.xml"`) {
		t.Fatalf("public CAP XML Content-Disposition = %q", got)
	}
	if !strings.Contains(response.Body.String(), "<identifier>urn:test:public-xml</identifier>") {
		t.Fatal("public CAP XML did not contain the archived alert")
	}

	config.Webpanel.Public.AlertsArchive.Access = "disabled"
	disabledServer := NewServerWithConfigPath(config, configPath, dir)
	disabled := httptest.NewRecorder()
	disabledServer.Handler().ServeHTTP(disabled, httptest.NewRequest(http.MethodGet, "/api/public/v1/alerts/archive/cap.xml?id=urn:test:public-xml&feed_id=sk-0001", nil))
	if disabled.Code != http.StatusNotFound {
		t.Fatalf("disabled public archive should not serve CAP XML, got %d", disabled.Code)
	}
}

func TestPublicAlertsArchiveCAPXMLRejectsOverlongLookup(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`webpanel:
  public:
    alerts_archive:
      access: public
storage:
  sqlite:
    enabled: true
    path: runtime/state/haze.db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, dir)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/public/v1/alerts/archive/cap.xml?id="+url.QueryEscape(strings.Repeat("x", archiveCAPXMLMaxIDLength+1)), nil)
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestPublicAlertsArchiveCAPXMLRejectsWrongMethodWithAllowHeader(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(`webpanel:
  public:
    alerts_archive:
      access: public
storage:
  sqlite:
    enabled: true
    path: runtime/state/haze.db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, dir)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/public/v1/alerts/archive/cap.xml?id=urn:test:method", nil)
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
	if got := response.Header().Get("Allow"); got != "GET, HEAD" {
		t.Fatalf("Allow = %q, want GET, HEAD", got)
	}
}

func TestECCCAugust2026ThreatAreasDoNotAlterArchiveLocations(t *testing.T) {
	info := capmodel.AlertInfo{Areas: []capmodel.AlertArea{
		{Description: "City of Saskatoon", Geocodes: []capmodel.NameValue{{Name: "layer:EC-MSC-SMC:1.0:CLC", Value: "065100"}}},
		{Description: "new active threat area", ThreatStatus: "issued", Geocodes: []capmodel.NameValue{{Name: "profile:CAP-CP:Location:0.3", Value: "4711066"}}},
	}}
	areas := archiveAreaNames(info)
	if len(areas) != 1 || areas[0] != "City of Saskatoon" {
		t.Fatalf("archive area names = %#v", areas)
	}
	locations := sameLocationsFromCAP(info)
	if len(locations) != 1 || locations[0] != "065100" {
		t.Fatalf("archive SAME locations = %#v", locations)
	}
}

func parseArchiveTestAlert(t *testing.T, raw string) capmodel.Alert {
	t.Helper()
	alert, err := capmodel.ParseCAP([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return alert
}

func archiveTestCAP(identifier string, msgType string, headline string, expires string, ended bool) string {
	response := "Monitor"
	if ended {
		response = "AllClear"
	}
	return `<?xml version="1.0" encoding="UTF-8"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>` + identifier + `</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-06-15T15:58:00-06:00</sent>
  <status>Actual</status>
  <msgType>` + msgType + `</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>thunderstorm</event>
    <responseType>` + response + `</responseType>
    <urgency>Immediate</urgency>
    <severity>Moderate</severity>
    <certainty>Likely</certainty>
    <effective>2026-06-15T15:58:00-06:00</effective>
    <expires>` + expires + `</expires>
    <headline>` + headline + `</headline>
    <area>
      <areaDesc>R.M of Rudy including Outlook and Glenside</areaDesc>
      <geocode><valueName>profile:CAP-CP:Location:0.3</valueName><value>065522</value></geocode>
    </area>
  </info>
</alert>`
}
