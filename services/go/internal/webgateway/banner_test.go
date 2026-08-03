package webgateway

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
)

func TestBannerCanonicalLocationNamesRequireActionableMatches(t *testing.T) {
	names := bannerCanonicalLocationNames([]alertmodel.LocationReference{
		{CanonicalID: "urn:haze:location:exact", Name: "Saskatoon", MatchConfidence: "exact", Actionable: true},
		{CandidateID: "urn:haze:location:shadow", Name: "Shadow Candidate", MatchConfidence: "high"},
		{CanonicalID: "urn:haze:location:ambiguous", Name: "Ambiguous Candidate", MatchConfidence: "high", Actionable: true, Ambiguous: true},
	})
	if len(names) != 1 || names[0] != "Saskatoon" {
		t.Fatalf("canonical names = %#v", names)
	}
}

func TestBannerPayloadSerializesAcceptedActiveAlerts(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	rawCAP := `<alert>
  <identifier>urn:test:banner:svr</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2099-06-17T01:00:00Z</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <event>Severe Thunderstorm Warning</event>
    <urgency>Immediate</urgency>
    <severity>Severe</severity>
    <certainty>Likely</certainty>
    <effective>2099-06-17T01:00:00Z</effective>
    <expires>2099-06-17T03:00:00Z</expires>
    <senderName>Environment Canada</senderName>
    <headline>Severe Thunderstorm Warning - in effect</headline>
    <description>Nickel size hail is possible.</description>
    <instruction>Take shelter if threatening weather approaches.</instruction>
    <area>
      <areaDesc>City of Saskatoon</areaDesc>
      <geocode><valueName>SAME</valueName><value>065100</value></geocode>
    </area>
  </info>
</alert>`
	storeBannerCAP(t, configPath, rawCAP, "sk-0001", "accepted")

	payload := bannerPayloadForTest(configPath, "sk-0001", time.Date(2026, 6, 17, 2, 0, 0, 0, time.UTC))
	if !payload.Active {
		t.Fatal("banner should be active")
	}
	if payload.Signature == "" {
		t.Fatal("missing signature")
	}
	if len(payload.Alerts) != 1 {
		t.Fatalf("alerts = %#v", payload.Alerts)
	}
	alert := payload.Alerts[0]
	if alert.Headline != "Severe Thunderstorm Warning" {
		t.Fatalf("headline = %q", alert.Headline)
	}
	if alert.BackgroundColor != "#931102" {
		t.Fatalf("background = %q", alert.BackgroundColor)
	}
	if !strings.Contains(alert.Message, "Environment Canada has issued") || !strings.Contains(alert.Message, "City of Saskatoon") {
		t.Fatalf("message = %q", alert.Message)
	}
}

func TestBannerPayloadFiltersExpiredAndFeed(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	rawCAP := `<alert>
  <identifier>urn:test:banner:expired</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-06-17T00:00:00Z</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <event>Thunderstorm Watch</event>
    <severity>Moderate</severity>
    <effective>2026-06-17T00:00:00Z</effective>
    <expires>2026-06-17T01:00:00Z</expires>
    <headline>Thunderstorm Watch - in effect</headline>
  </info>
</alert>`
	storeBannerCAP(t, configPath, rawCAP, "sk-0002", "accepted")

	payload := bannerPayloadForTest(configPath, "sk-0001", time.Date(2026, 6, 17, 2, 0, 0, 0, time.UTC))
	if payload.Active || len(payload.Alerts) != 0 {
		t.Fatalf("payload should be idle: %#v", payload)
	}
}

func TestBannerHubActivatesFromPlayoutEvent(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	rawCAP := `<alert>
  <identifier>urn:test:banner:onair</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2099-06-17T01:00:00Z</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <event>Severe Thunderstorm Warning</event>
    <severity>Severe</severity>
    <effective>2099-06-17T01:00:00Z</effective>
    <expires>2099-06-17T03:00:00Z</expires>
    <senderName>Environment Canada</senderName>
    <headline>Severe Thunderstorm Warning - in effect</headline>
    <area><areaDesc>City of Saskatoon</areaDesc></area>
  </info>
</alert>`
	storeBannerCAP(t, configPath, rawCAP, "sk-0001", "accepted")
	queueID := "000_sk-0001_urn_test_banner_onair_same_header"
	queue := sameQueueItem{
		ID:        queueID,
		AlertID:   "urn:test:banner:onair",
		Type:      "same_header",
		Status:    "claimed",
		FeedIDs:   []string{"sk-0001"},
		Header:    "ZCZC-WXR-SVR-065100+0030-1680100-XLF322  -",
		Event:     "SVR",
		CreatedAt: time.Date(2026, 6, 17, 1, 0, 0, 0, time.UTC),
	}
	rawQueue, err := json.Marshal(queue)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, dir+"/runtime/queues/alerts/"+queueID+".json", string(rawQueue))

	hub := NewBannerHub(configPath, "")
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["sk-0001"],"queue_id":"`+queueID+`","event":"SVR"}`), time.Now().UTC())
	payload := buildBannerPayload(configPath, "sk-0001", hub)

	if !payload.Active || len(payload.Alerts) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.Alerts[0].Identifier != "urn:test:banner:onair" {
		t.Fatalf("alert = %#v", payload.Alerts[0])
	}
}

func TestBannerHubUsesArchiveAcrossCatchallFeed(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	rawCAP := `<alert>
  <identifier>urn:test:banner:catchall</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2099-06-17T01:00:00Z</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <event>Tornado Warning</event>
    <severity>Extreme</severity>
    <effective>2099-06-17T01:00:00Z</effective>
    <expires>2099-06-17T03:00:00Z</expires>
    <headline>Tornado Warning - in effect</headline>
    <area><areaDesc>City of Saskatoon</areaDesc></area>
  </info>
</alert>`
	storeBannerCAP(t, configPath, rawCAP, "sk-0001", "accepted")
	hub := NewBannerHub(configPath, "")
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["CAP-IT-ALL"],"queue_id":"001_CAP-IT-ALL_urn_test_banner_catchall_cap","data":{"alert_id":"urn:test:banner:catchall","event":"TOR","header":"Tornado Warning"}}`), time.Now().UTC())

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", hub)

	if !payload.Active || len(payload.Alerts) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.Alerts[0].FeedID != "CAP-IT-ALL" || payload.Alerts[0].Identifier != "urn:test:banner:catchall" {
		t.Fatalf("alert = %#v", payload.Alerts[0])
	}
}

func TestBannerPayloadPrefersPlayoutAlertText(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	rawCAP := `<alert>
  <identifier>urn:test:banner:spoken</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2099-06-17T01:00:00Z</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <event>Tornado Warning</event>
    <severity>Extreme</severity>
    <effective>2099-06-17T01:00:00Z</effective>
    <expires>2099-06-17T03:00:00Z</expires>
    <headline>Tornado Warning - in effect</headline>
    <area><areaDesc>City of Saskatoon</areaDesc></area>
  </info>
</alert>`
	storeBannerCAP(t, configPath, rawCAP, "CAP-IT-ALL", "accepted")
	spoken := "This is the exact text that was fed into alert audio."
	hub := NewBannerHub(configPath, "")
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["CAP-IT-ALL"],"queue_id":"spoken-queue-1","data":{"alert_id":"urn:test:banner:spoken","event":"TOR","title":"Tornado Warning","alert_text":"`+spoken+`"}}`), time.Now().UTC())

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", hub)

	if !payload.Active || len(payload.Alerts) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.Alerts[0].Message != spoken {
		t.Fatalf("message = %q, want %q", payload.Alerts[0].Message, spoken)
	}
}

func TestBannerPayloadUsesBannerTextAndSameColorForManualAlert(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	bannerText := "Environment Canada has issued a Practice/demo Warning for Talladega, AL ending at 5:15 pm (meowraii). Custom text."
	hub := NewBannerHub(configPath, "")
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["CAP-IT-ALL"],"queue_id":"manual-warning-1","data":{"alert_id":"manual-warning","event":"DMO","title":"DMO - Practice/demo Warning","alert_text":"tts script should not be the crawl","banner_text":"`+bannerText+`"}}`), time.Now().UTC())

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", hub)

	if !payload.Active || len(payload.Alerts) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.PrimaryColor != "#019310" {
		t.Fatalf("primary color = %q", payload.PrimaryColor)
	}
	if payload.Alerts[0].BackgroundColor != "#019310" {
		t.Fatalf("alert background color = %q", payload.Alerts[0].BackgroundColor)
	}
	if payload.Alerts[0].Message != bannerText {
		t.Fatalf("message = %q, want %q", payload.Alerts[0].Message, bannerText)
	}
}

func TestBannerPayloadUsesBroadcastImmediateFlagForRedColor(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	hub := NewBannerHub(configPath, "")
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["CAP-IT-ALL"],"queue_id":"broadcast-immediate-1","data":{"alert_id":"broadcast-immediate","event":"civilEmerg","title":"Broadcast Immediate","broadcast_immediate":true,"banner_text":"Broadcast immediate message."}}`), time.Now().UTC())

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", hub)

	if !payload.Active || len(payload.Alerts) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.PrimaryColor != "#931102" || payload.Alerts[0].BackgroundColor != "#931102" {
		t.Fatalf("colors = primary %q alert %q, want red", payload.PrimaryColor, payload.Alerts[0].BackgroundColor)
	}
}

func TestBannerHubFallsBackToOnAirMetadataWithoutArchive(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	hub := NewBannerHub(configPath, "")
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["CAP-IT-ALL"],"queue_id":"manual-1","event":"RWT","header":"Required Weekly Test"}`), time.Now().UTC())

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", hub)

	if !payload.Active || len(payload.Alerts) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.Alerts[0].Headline != "Required Weekly Test" || payload.Alerts[0].FeedID != "CAP-IT-ALL" {
		t.Fatalf("alert = %#v", payload.Alerts[0])
	}
	if payload.Alerts[0].Message != "Required Weekly Test" {
		t.Fatalf("message = %q", payload.Alerts[0].Message)
	}
}

func TestBannerPayloadUsesPlayingQueueItemWhenHubMissedEvent(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	spoken := "Persisted queue text should be the crawl text."
	queue := sameQueueItem{
		ID:        "manual-queue-1",
		AlertID:   "manual-alert-1",
		Type:      "cap_alert",
		Status:    "playing",
		FeedIDs:   []string{"CAP-IT-ALL"},
		Header:    "Required Weekly Test",
		Event:     "RWT",
		AlertText: spoken,
		CreatedAt: time.Now().UTC().Add(-time.Second),
	}
	rawQueue, err := json.Marshal(queue)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, dir+"/runtime/queues/alerts/manual-queue-1.json", string(rawQueue))

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", NewBannerHub(configPath, ""))

	if !payload.Active || len(payload.Alerts) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.Alerts[0].Headline != "Required Weekly Test" || payload.Alerts[0].FeedID != "CAP-IT-ALL" {
		t.Fatalf("alert = %#v", payload.Alerts[0])
	}
	if payload.Alerts[0].Message != spoken {
		t.Fatalf("message = %q, want %q", payload.Alerts[0].Message, spoken)
	}
}

func TestBannerPayloadIgnoresPendingQueueItemUntilPlayoutStarts(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	queue := sameQueueItem{
		ID:        "pending-queue-1",
		AlertID:   "pending-alert-1",
		Type:      "cap_alert",
		Status:    "pending",
		FeedIDs:   []string{"CAP-IT-ALL"},
		Header:    "Tornado Warning",
		Event:     "TOR",
		CreatedAt: time.Now().UTC().Add(-time.Second),
	}
	rawQueue, err := json.Marshal(queue)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, dir+"/runtime/queues/alerts/pending-queue-1.json", string(rawQueue))

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", NewBannerHub(configPath, ""))

	if payload.Active || len(payload.Alerts) != 0 {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestBannerPayloadUsesSingularFeedIDQueueItem(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	queue := sameQueueItem{
		ID:        "single-feed-queue-1",
		AlertID:   "single-feed-alert-1",
		Type:      "cap_alert",
		Status:    "playing",
		FeedID:    "CAP-IT-ALL",
		Header:    "Required Weekly Test",
		Event:     "RWT",
		CreatedAt: time.Now().UTC().Add(-time.Second),
	}
	rawQueue, err := json.Marshal(queue)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, dir+"/runtime/queues/alerts/single-feed-queue-1.json", string(rawQueue))

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", NewBannerHub(configPath, ""))

	if !payload.Active || len(payload.Alerts) != 1 {
		t.Fatalf("payload = %#v", payload)
	}
	if payload.Alerts[0].Headline != "Required Weekly Test" || payload.Alerts[0].FeedID != "CAP-IT-ALL" {
		t.Fatalf("alert = %#v", payload.Alerts[0])
	}
}

func TestBannerHubIgnoresAudioReadyUntilPlayoutStarts(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	hub := NewBannerHub(configPath, "")
	hub.handleEvent([]byte(`{"type":"cap.alert.audio.ready","feed_ids":["CAP-IT-ALL"],"queue_id":"audio-ready-1","data":{"alert_id":"audio-ready-alert","event":"RWT","title":"Required Weekly Test"}}`), time.Now().UTC())

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", hub)

	if payload.Active || len(payload.Alerts) != 0 {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestBannerHubDoesNotReplaceActiveAlertUntilCompletion(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	hub := NewBannerHub(configPath, "")
	now := time.Now().UTC()
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["CAP-IT-ALL"],"queue_id":"tor-1","data":{"alert_id":"tor-alert","event":"TOR","title":"Tornado Warning","banner_text":"Tornado warning crawl."}}`), now)
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["CAP-IT-ALL"],"queue_id":"svr-1","data":{"alert_id":"svr-alert","event":"SVR","title":"Severe Thunderstorm Warning","banner_text":"Severe thunderstorm crawl."}}`), now.Add(time.Second))

	active := hub.Active("CAP-IT-ALL", now.Add(2*time.Second))
	if len(active) != 1 {
		t.Fatalf("active = %#v", active)
	}
	if active[0].QueueID != "tor-1" || active[0].BannerText != "Tornado warning crawl." {
		t.Fatalf("active = %#v", active[0])
	}

	hub.handleEvent([]byte(`{"type":"alert.playout.completed","feed_ids":["CAP-IT-ALL"],"queue_id":"tor-1"}`), now.Add(3*time.Second))
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["CAP-IT-ALL"],"queue_id":"svr-1","data":{"alert_id":"svr-alert","event":"SVR","title":"Severe Thunderstorm Warning","banner_text":"Severe thunderstorm crawl."}}`), now.Add(4*time.Second))
	active = hub.Active("CAP-IT-ALL", now.Add(5*time.Second))
	if len(active) != 1 || active[0].QueueID != "svr-1" {
		t.Fatalf("active after completion = %#v", active)
	}
}

func TestBannerHubCompletionClearsWildcardOnAirAlert(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	hub := NewBannerHub(configPath, "")
	now := time.Now().UTC()
	hub.handleEvent([]byte(`{"type":"alert.playout.started","feed_ids":["*"],"queue_id":"wildcard-start","data":{"alert_id":"wildcard-alert","event":"RWT","title":"Required Weekly Test","banner_text":"Station-wide crawl."}}`), now)
	if active := hub.Active("CAP-IT-ALL", now); len(active) != 1 {
		t.Fatalf("active = %#v", active)
	}

	hub.handleEvent([]byte(`{"type":"alert.playout.completed","feed_ids":["CAP-IT-ALL"],"queue_id":"wildcard-same_full"}`), now.Add(time.Second))
	payload := buildBannerPayload(configPath, "CAP-IT-ALL", hub)
	if payload.Active || len(payload.Alerts) != 0 {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestBannerPayloadIgnoresStaleQueuedQueueItem(t *testing.T) {
	dir := t.TempDir()
	configPath := testBannerConfig(t, dir)
	queue := sameQueueItem{
		ID:        "old-queue-1",
		AlertID:   "old-alert-1",
		Type:      "cap_alert",
		Status:    "queued",
		FeedIDs:   []string{"CAP-IT-ALL"},
		Header:    "Old Required Weekly Test",
		Event:     "RWT",
		CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
	}
	rawQueue, err := json.Marshal(queue)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, dir+"/runtime/queues/alerts/old-queue-1.json", string(rawQueue))

	payload := buildBannerPayload(configPath, "CAP-IT-ALL", NewBannerHub(configPath, ""))

	if payload.Active || len(payload.Alerts) != 0 {
		t.Fatalf("payload = %#v", payload)
	}
}

func testBannerConfig(t *testing.T, dir string) string {
	t.Helper()
	configPath := dir + "/config.yaml"
	mustWrite(t, configPath, "storage:\n  sqlite:\n    enabled: true\n    path: runtime/state/haze.db\n")
	return configPath
}

func storeBannerCAP(t *testing.T, configPath string, rawCAP string, feedID string, bucket string) {
	t.Helper()
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err := datastore.Open(context.Background(), cfg.Storage, dirOf(configPath))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.StoreCAPArchive(context.Background(), datastore.CAPArchiveRecord{
		AlertID: "test-" + feedID + "-" + bucket,
		FeedID:  feedID,
		Bucket:  bucket,
		Status:  bucket,
		Event:   "SVR",
		RawXML:  rawCAP,
	}); err != nil {
		t.Fatal(err)
	}
}

func dirOf(path string) string {
	index := strings.LastIndexAny(path, `/\`)
	if index < 0 {
		return "."
	}
	return path[:index]
}
