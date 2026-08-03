package webgateway

import (
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alerttext"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

type feedsXML struct {
	Feeds []feedXML `xml:"feed"`
}

type feedXML struct {
	ID         string `xml:"id,attr"`
	EnabledRaw string `xml:"enabled,attr"`
	Timezone   string `xml:"timezone,attr"`
	Playout    struct {
		Routine string `xml:"routine,attr"`
		SAME    string `xml:"same,attr"`
	} `xml:"playout"`
	Alerts struct {
		CapCP struct {
			EnabledRaw string `xml:"enabled,attr"`
		} `xml:"cap_cp"`
		NWSCAP struct {
			EnabledRaw string `xml:"enabled,attr"`
		} `xml:"nws_cap"`
	} `xml:"alerts"`
	Languages struct {
		Langs []struct {
			Code string `xml:"code,attr"`
		} `xml:"lang"`
	} `xml:"languages"`
	Locations struct {
		Coverage struct {
			Regions []coverageRegionXML `xml:"region"`
		} `xml:"coverage"`
		ObservationLocations struct {
			Locations []locationXML `xml:"location"`
		} `xml:"observationLocations"`
		AviationReportLocations struct {
			Locations []locationXML `xml:"location"`
		} `xml:"aviationReportLocations"`
		AirQualityLocations struct {
			Locations []locationXML `xml:"location"`
		} `xml:"airQualityLocations"`
		ClimateLocations struct {
			Locations []locationXML `xml:"location"`
		} `xml:"climateLocations"`
		MarineForecastLocations struct {
			Locations  []locationXML `xml:"location"`
			Subregions []locationXML `xml:"subregion"`
		} `xml:"marineForecastLocations"`
		MarineConditions struct {
			Locations []locationXML `xml:"location"`
		} `xml:"marineConditions"`
		HydrometricLocations struct {
			Locations []locationXML `xml:"location"`
			Upstream  struct {
				Locations []locationXML `xml:"location"`
			} `xml:"upstream"`
			Downstream struct {
				Locations []locationXML `xml:"location"`
			} `xml:"downstream"`
		} `xml:"hydrometricLocations"`
	} `xml:"locations"`
	Transmitter struct {
		Transmitters []transmitterXML `xml:"transmitter"`
	} `xml:"transmitter_metadata"`
}

type coverageRegionXML struct {
	ID                  string                 `xml:"id,attr"`
	Source              string                 `xml:"source,attr"`
	Name                string                 `xml:"name,attr"`
	DeriveForecast      string                 `xml:"derive_forecast,attr"`
	CanonicalID         string                 `xml:"canonical_id,attr"`
	ForecastCanonicalID string                 `xml:"forecast_canonical_id,attr"`
	Subregions          []coverageSubregionXML `xml:"subregion"`
}

type coverageSubregionXML struct {
	ID          string `xml:"id,attr"`
	CanonicalID string `xml:"canonical_id,attr"`
}

type locationXML struct {
	ID           string `xml:"id,attr"`
	Source       string `xml:"source,attr"`
	NameOverride string `xml:"name_override,attr"`
	NormalID     string `xml:"normal_id,attr"`
	Latitude     string `xml:"latitude,attr"`
	Longitude    string `xml:"longitude,attr"`
	CanonicalID  string `xml:"canonical_id,attr"`
}

type transmitterXML struct {
	Network      transmitterNetworkXML   `xml:"network"`
	HostName     string                  `xml:"host_name"`
	SiteName     string                  `xml:"site_name"`
	Callsign     string                  `xml:"callsign"`
	Relationship string                  `xml:"relationship"`
	FrequencyMHz transmitterFrequencyXML `xml:"frequency_mhz"`
	RDS          transmitterRDSXML       `xml:"rds"`
}

type transmitterNetworkXML struct {
	Name           string `xml:"name"`
	Pronounciation string `xml:"pronounciation"`
	Pronunciation  string `xml:"pronunciation"`
}

type transmitterFrequencyXML struct {
	GPCLK string `xml:"gpclk,attr"`
	GPIO  string `xml:"gpio,attr"`
	Value string `xml:",chardata"`
}

type transmitterRDSXML struct {
	EnabledRaw string   `xml:"enabled,attr"`
	PI         string   `xml:"pi"`
	PS         string   `xml:"ps"`
	RT         string   `xml:"rt"`
	PTY        string   `xml:"pty"`
	TP         string   `xml:"tp"`
	AF         []string `xml:"af"`
}

type outputsXML struct {
	Feeds []outputFeedXML `xml:"feed"`
}

// BuildGitCommit is populated by release/dev build scripts with -ldflags.
var BuildGitCommit = "unknown"

type outputFeedXML struct {
	ID          string        `xml:"id,attr"`
	Stream      outputNodeXML `xml:"stream"`
	Icecast     outputNodeXML `xml:"icecast"`
	UDP         outputNodeXML `xml:"udp"`
	RTP         outputNodeXML `xml:"rtp"`
	RTMP        outputNodeXML `xml:"rtmp"`
	SRT         outputNodeXML `xml:"srt"`
	RTSP        outputNodeXML `xml:"rtsp"`
	AudioDevice outputNodeXML `xml:"audio_device"`
	File        outputNodeXML `xml:"file"`
	WebRTC      outputNodeXML `xml:"webrtc"`
}

type outputXML = outputFeedXML

type outputNodeXML struct {
	EnabledRaw string `xml:"enabled,attr"`
	Type       string `xml:"type"`
	Host       string `xml:"host"`
	IP         string `xml:"ip"`
	Port       string `xml:"port"`
	URL        string `xml:"url"`
	Path       string `xml:"path"`
	Format     string `xml:"format"`
	Acodec     string `xml:"acodec"`
}

type packageCatalogXML struct {
	Packages []struct {
		ID         string `xml:"id,attr"`
		EnabledRaw string `xml:"enabled,attr"`
	} `xml:"package"`
	Products []struct {
		ID         string `xml:"id,attr"`
		EnabledRaw string `xml:"enabled,attr"`
	} `xml:"product"`
}

func panelStatePayload(config Config, configPath string, startedAt time.Time, request *http.Request, mediaAvailable bool) (map[string]any, error) {
	summary, err := summaryPayload(config, configPath, startedAt, request, mediaAvailable, true)
	if err != nil {
		return nil, err
	}
	root, _ := loadYAMLMap(configPath)
	return map[string]any{
		"summary":        summary,
		"events":         runtimeEvents(configPath),
		"logs":           logsPayload(configPath, request),
		"datapool":       datapoolPayload(configPath),
		"config":         adminConfigPayload(config, root),
		"last_connected": lastConnectedPayload(request),
	}, nil
}

func publicStatePayload(config Config, configPath string, startedAt time.Time, request *http.Request, auth *AuthManager, mediaAvailable bool, listenerStats ...map[string]FeedListenerStats) (map[string]any, error) {
	feedAccess := publicFeedAccess(config)
	includeFeeds := publicRequestWantsFeeds(request) && (feedAccess == "public" || (feedAccess == "auth_required" && auth != nil && auth.FullyAuthenticated(request)))
	var summary map[string]any
	var err error
	if includeFeeds {
		summary, err = publicFeedsSummaryPayload(config, configPath, startedAt, request, mediaAvailable, listenerStats...)
	} else {
		summary, err = publicSummaryPayload(config, configPath, startedAt, request, mediaAvailable)
	}
	if err != nil {
		return nil, err
	}
	if !includeFeeds {
		summary["feeds"] = []any{}
	}
	if publicRequestWantsAlerts(request) && publicAlertsArchiveAccess(config) == "public" {
		if archive, err := publicAlertsArchivePayload(configPath); err == nil {
			summary["alerts"] = archive
		}
	}
	return map[string]any{"summary": summary}, nil
}

func publicSummaryPayload(config Config, configPath string, startedAt time.Time, request *http.Request, mediaAvailable bool) (map[string]any, error) {
	feeds, err := loadBasicFeedSummaries(configPath)
	if err != nil {
		return nil, err
	}
	enabled := 0
	for _, feed := range feeds {
		if value, _ := feed["enabled"].(bool); value {
			enabled++
		}
	}
	webrtcEnabled := publicWebRTCAvailable(config, feeds)
	summary := baseSummaryPayload(config, startedAt, request, mediaAvailable, len(feeds), enabled, webrtcEnabled)
	summary["feeds"] = []any{}
	return summary, nil
}

func publicFeedsSummaryPayload(config Config, configPath string, startedAt time.Time, request *http.Request, mediaAvailable bool, listenerStats ...map[string]FeedListenerStats) (map[string]any, error) {
	feeds, err := loadPublicFeedSummaries(configPath, listenerStats...)
	if err != nil {
		return nil, err
	}
	enabled := 0
	for _, feed := range feeds {
		if value, _ := feed["enabled"].(bool); value {
			enabled++
		}
	}
	webrtcEnabled := publicWebRTCAvailable(config, feeds)
	summary := baseSummaryPayload(config, startedAt, request, mediaAvailable, len(feeds), enabled, webrtcEnabled)
	summary["feeds"] = feeds
	return summary, nil
}

func publicRequestWantsFeeds(request *http.Request) bool {
	return requestBoolQuery(request, "feeds")
}

func publicRequestWantsAlerts(request *http.Request) bool {
	return requestBoolQuery(request, "alerts")
}

func requestBoolQuery(request *http.Request, key string) bool {
	if request == nil || request.URL == nil {
		return false
	}
	value := strings.ToLower(strings.TrimSpace(request.URL.Query().Get(key)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func summaryPayload(config Config, configPath string, startedAt time.Time, request *http.Request, mediaAvailable bool, admin bool) (map[string]any, error) {
	feeds, err := loadFeedSummaries(configPath)
	if err != nil {
		return nil, err
	}
	enabled := 0
	for _, feed := range feeds {
		if value, _ := feed["enabled"].(bool); value {
			enabled++
		}
	}
	webrtcEnabled := publicWebRTCAvailable(config, feeds)
	summary := baseSummaryPayload(config, startedAt, request, mediaAvailable, len(feeds), enabled, webrtcEnabled)
	summary["feeds"] = feeds
	if admin {
		summary["data_pool_key_count"] = len(readJSONMap(resolveConfigPath(configPath, "runtime/state/dataPool.json")))
		return summary, nil
	}
	summary["feeds"] = publicFeedSummaries(feeds)
	return summary, nil
}

func baseSummaryPayload(config Config, startedAt time.Time, request *http.Request, mediaAvailable bool, feedCount int, enabledFeedCount int, webrtcEnabled bool) map[string]any {
	summary := map[string]any{
		"name":               siteName(config),
		"hostname":           hostname(),
		"ip_address":         serverIP(request),
		"operator":           displayText(config.Operator.OperatorName),
		"on_air_name":        displayText(config.Operator.OnAirName),
		"version":            fallbackText(config.Version, "dev"),
		"git_commit":         gitCommit(),
		"os":                 runtime.GOOS,
		"architecture":       runtime.GOARCH,
		"feed_count":         feedCount,
		"enabled_feed_count": enabledFeedCount,
		"uptime_seconds":     time.Since(startedAt).Seconds(),
		"feeds_access":       publicFeedAccess(config),
		"webrtc_enabled":     webrtcEnabled,
		"media_available":    mediaAvailable,
		"alerts_archive":     publicAlertsArchiveAccess(config),
		"admin_url":          adminURL(config, request),
		"public_base_url":    publicBaseURL(config, request),
		"tls":                tlsStatePayload(config, request),
		"capabilities": map[string]any{
			"public_alerts": publicAlertsArchiveAccess(config) == "public",
			"webrtc":        webrtcEnabled,
		},
	}
	for key, value := range WebRTCAudioCapabilities() {
		summary["capabilities"].(map[string]any)[key] = value
	}
	return summary
}

func loadFeedSummaries(configPath string) ([]map[string]any, error) {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return nil, err
	}
	parsed, err := loadFeedsXML(configPath, root)
	if err != nil {
		return nil, err
	}
	outputs, _ := loadOutputsXML(configPath, root)
	forecastNames := loadForecastRegionNames(resolveConfigPath(configPath, "managed/csv/FORECAST_LOCATIONS.csv"))
	clcNames := loadCLCNames(resolveConfigPath(configPath, "managed/csv/CLC_Base_Zone.csv"))
	nwsFIPSNames := loadNWSFIPSNames(resolveConfigPath(configPath, "managed/csv/NWS_ZONE_COUNTY_CORRELATION.csv"))
	queueItems, _ := loadAlertQueueItems(configPath)
	out := make([]map[string]any, 0, len(parsed.Feeds))
	for _, feed := range parsed.Feeds {
		if strings.TrimSpace(feed.ID) == "" {
			continue
		}
		out = append(out, feedSummary(configPath, feed, outputForFeed(outputs, feed.ID), forecastNames, clcNames, nwsFIPSNames, queueItems))
	}
	sort.SliceStable(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["id"]) < fmt.Sprint(out[j]["id"])
	})
	return out, nil
}

func loadBasicFeedSummaries(configPath string) ([]map[string]any, error) {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return nil, err
	}
	parsed, err := loadFeedsXML(configPath, root)
	if err != nil {
		return nil, err
	}
	outputs, _ := loadOutputsXML(configPath, root)
	out := make([]map[string]any, 0, len(parsed.Feeds))
	for _, feed := range parsed.Feeds {
		if strings.TrimSpace(feed.ID) == "" {
			continue
		}
		output := outputForFeed(outputs, feed.ID)
		httpStreamEnabled := outputHTTPStreamEnabled(output)
		out = append(out, map[string]any{
			"id":                  strings.TrimSpace(feed.ID),
			"enabled":             xmlBool(feed.EnabledRaw, true),
			"webrtc_enabled":      xmlBool(output.WebRTC.EnabledRaw, false),
			"http_stream_enabled": httpStreamEnabled,
		})
	}
	return out, nil
}

func loadPublicFeedSummaries(configPath string, listenerStats ...map[string]FeedListenerStats) ([]map[string]any, error) {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return nil, err
	}
	parsed, err := loadFeedsXML(configPath, root)
	if err != nil {
		return nil, err
	}
	outputs, _ := loadOutputsXML(configPath, root)
	stats := map[string]FeedListenerStats{}
	if len(listenerStats) > 0 && listenerStats[0] != nil {
		stats = listenerStats[0]
	}
	out := make([]map[string]any, 0, len(parsed.Feeds))
	for _, feed := range parsed.Feeds {
		id := strings.TrimSpace(feed.ID)
		if id == "" {
			continue
		}
		station := stationTransmitter(feed)
		output := outputForFeed(outputs, id)
		webrtcEnabled := xmlBool(output.WebRTC.EnabledRaw, false)
		httpStreamEnabled := outputHTTPStreamEnabled(output)
		out = append(out, map[string]any{
			"id":                  id,
			"name":                fallbackText(station.SiteName, id),
			"enabled":             xmlBool(feed.EnabledRaw, true),
			"runtime":             feedRuntimePayload(configPath, id),
			"listeners":           listenerStatsPayload(listenerStatsForFeed(stats, id)),
			"transmitter":         publicTransmitterPayloadFromXML(station, feed),
			"transmitters":        publicTransmitterPayloadsFromXML(feed),
			"webrtc_enabled":      webrtcEnabled,
			"http_stream_enabled": httpStreamEnabled,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["id"]) < fmt.Sprint(out[j]["id"])
	})
	return out, nil
}

func feedRuntimePayload(configPath string, feedID string) map[string]any {
	runtime := map[string]any{
		"now_playing": "Idle",
	}
	id := safeID(feedID)
	if id == "" {
		return runtime
	}
	path := filepath.Join(filepath.Dir(filepath.Clean(configPath)), "runtime", "feeds", id+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtime
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return runtime
	}
	for _, key := range []string{
		"feed_id",
		"now_playing",
		"on_air_now_playing",
		"station_now_playing",
		"public_stream_now_playing",
		"on_air_last_played_at",
		"public_stream_started_at",
		"current_started_at",
		"current_ends_at",
		"current_duration_ms",
		"updated_at",
	} {
		value, ok := payload[key]
		if !ok || value == nil {
			continue
		}
		switch value.(type) {
		case string, float64, bool:
			runtime[key] = value
		}
	}
	runtimeText := func(key string) string {
		text, _ := runtime[key].(string)
		return text
	}
	if nowPlaying := fallbackString(
		runtimeText("public_stream_now_playing"),
		runtimeText("on_air_now_playing"),
		runtimeText("now_playing"),
	); nowPlaying != "" {
		runtime["now_playing"] = nowPlaying
	}
	return runtime
}

func feedSummary(configPath string, feed feedXML, outputs outputXML, forecastNames map[string]string, clcNames map[string]string, nwsFIPSNames map[string]string, queueItems []sameQueueItem) map[string]any {
	station := stationTransmitter(feed)
	regions := coverageRegionPayloads(feed, forecastNames, clcNames)
	clcCodes := feedCoverageCodes(feed, clcNames)
	allLocations := feedCoversAllLocations(feed)
	sameLocations := feedSameLocations(feed, clcNames, nwsFIPSNames)
	outputLabels := outputLabels(outputs)
	webrtcEnabled := xmlBool(outputs.WebRTC.EnabledRaw, false)
	httpStreamEnabled := outputHTTPStreamEnabled(outputs)
	queueDepth, recentQueue, latestQueue := alertQueueState(queueItems, feed.ID)
	runtime := feedRuntimePayload(configPath, feed.ID)
	if latestQueue != nil {
		runtime["last_alert_event"] = latestQueue.Event
		runtime["last_alert_severity"] = queueSeverity(latestQueue.Status)
		runtime["last_alert_status"] = latestQueue.Status
		runtime["last_alert_header"] = latestQueue.Header
	}
	return map[string]any{
		"id":                  strings.TrimSpace(feed.ID),
		"name":                fallbackText(station.SiteName, feed.ID),
		"enabled":             xmlBool(feed.EnabledRaw, true),
		"timezone":            fallbackText(feed.Timezone, "Local"),
		"languages":           feedLanguages(feed),
		"location_count":      feedLocationCount(feed),
		"canonical_locations": feedCanonicalLocationIDs(feed),
		"clc_codes":           sortedKeys(clcCodes),
		"same_locations":      sameLocations,
		"same_all_locations":  allLocations,
		"coverage_regions":    regions,
		"transmitter":         transmitterPayload(station, feed),
		"transmitters":        transmitterPayloads(feed),
		"outputs":             outputLabels,
		"webrtc_enabled":      webrtcEnabled,
		"http_stream_enabled": httpStreamEnabled,
		"alert_queue_depth":   queueDepth,
		"recent_alerts":       recentQueue,
		"playlist_items":      []any{},
		"runtime":             runtime,
	}
}

func publicFeedSummaries(feeds []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(feeds))
	for _, feed := range feeds {
		transmitter, _ := feed["transmitter"].(map[string]any)
		out = append(out, map[string]any{
			"id":                  feed["id"],
			"name":                feed["name"],
			"enabled":             feed["enabled"],
			"runtime":             feed["runtime"],
			"listeners":           feed["listeners"],
			"transmitter":         publicTransmitterPayload(transmitter),
			"transmitters":        publicTransmitters(feed["transmitters"]),
			"webrtc_enabled":      feed["webrtc_enabled"],
			"http_stream_enabled": feed["http_stream_enabled"],
		})
	}
	return out
}

func publicTransmitterPayload(transmitter map[string]any) map[string]any {
	if transmitter == nil {
		return map[string]any{}
	}
	out := map[string]any{
		"site_name":  transmitter["site_name"],
		"site_names": transmitter["site_names"],
	}
	if callsign := strings.TrimSpace(fmt.Sprint(transmitter["callsign"])); callsign != "" {
		out["callsign"] = callsign
	}
	return out
}

func publicTransmitterPayloadFromXML(transmitter transmitterXML, feed feedXML) map[string]any {
	out := map[string]any{
		"site_name":  fallbackText(transmitter.SiteName, feed.ID),
		"site_names": siteNamesForFeed(feed),
	}
	if callsign := strings.TrimSpace(transmitter.Callsign); callsign != "" {
		out["callsign"] = callsign
	}
	return out
}

func publicTransmitterPayloadsFromXML(feed feedXML) []map[string]any {
	transmitters := transmitterList(feed)
	out := make([]map[string]any, 0, len(transmitters))
	for _, transmitter := range transmitters {
		out = append(out, publicTransmitterPayloadFromXML(transmitter, feed))
	}
	return out
}

func publicTransmitters(value any) []map[string]any {
	items, _ := value.([]map[string]any)
	if items == nil {
		if raw, ok := value.([]any); ok {
			items = make([]map[string]any, 0, len(raw))
			for _, item := range raw {
				if mapped, ok := item.(map[string]any); ok {
					items = append(items, mapped)
				}
			}
		}
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, publicTransmitterPayload(item))
	}
	return out
}

func loadFeedsXML(configPath string, root map[string]any) (feedsXML, error) {
	path := textAt(root, []string{"feeds_file"}, "managed/configs/feeds.xml", 240)
	raw, err := os.ReadFile(resolveConfigPath(configPath, path))
	if err != nil {
		return feedsXML{}, err
	}
	var parsed feedsXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return feedsXML{}, fmt.Errorf("failed to parse feeds XML %s: %w", resolveConfigPath(configPath, path), err)
	}
	return parsed, nil
}

func loadOutputsXML(configPath string, root map[string]any) (map[string]outputXML, error) {
	path := textAt(root, []string{"outputs_file"}, "managed/configs/output.xml", 240)
	raw, err := os.ReadFile(resolveConfigPath(configPath, path))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]outputXML{}, nil
		}
		return nil, err
	}
	var parsed outputsXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse outputs XML %s: %w", resolveConfigPath(configPath, path), err)
	}
	out := map[string]outputXML{}
	for _, feed := range parsed.Feeds {
		id := strings.TrimSpace(feed.ID)
		if id != "" {
			out[id] = outputXML(feed)
		}
	}
	return out, nil
}

func outputForFeed(outputs map[string]outputXML, feedID string) outputXML {
	feedID = strings.TrimSpace(feedID)
	if feedID == "" {
		return outputXML{}
	}
	if output, ok := outputs[feedID]; ok {
		return output
	}
	patterns := make([]string, 0)
	for pattern := range outputs {
		if !strings.Contains(pattern, "*") {
			continue
		}
		if ok, err := path.Match(pattern, feedID); err == nil && ok {
			patterns = append(patterns, pattern)
		}
	}
	sort.Strings(patterns)
	for _, pattern := range patterns {
		return outputs[pattern]
	}
	return outputXML{}
}

func stationTransmitter(feed feedXML) transmitterXML {
	transmitters := transmitterList(feed)
	for _, transmitter := range transmitters {
		if transmitter.isRelationship("primary") {
			return transmitter
		}
	}
	for _, transmitter := range transmitters {
		if !transmitter.isRelationship("replaces") && !transmitter.isRelationship("ip") &&
			(strings.TrimSpace(transmitter.Callsign) != "" || strings.TrimSpace(transmitter.SiteName) != "") {
			return transmitter
		}
	}
	for _, transmitter := range transmitters {
		if strings.TrimSpace(transmitter.Callsign) != "" || strings.TrimSpace(transmitter.SiteName) != "" {
			return transmitter
		}
	}
	if len(transmitters) > 0 {
		return transmitters[0]
	}
	return transmitterXML{}
}

func receiverPreferredTransmitter(feed feedXML) transmitterXML {
	for _, transmitter := range transmitterList(feed) {
		if transmitter.isRelationship("fm") && transmitter.frequencyText() != "" {
			return transmitter
		}
	}
	return stationTransmitter(feed)
}

func transmitterList(feed feedXML) []transmitterXML {
	out := make([]transmitterXML, 0, len(feed.Transmitter.Transmitters))
	for _, transmitter := range feed.Transmitter.Transmitters {
		if transmitter.empty() {
			continue
		}
		out = append(out, transmitter)
	}
	return out
}

func transmitterPayloads(feed feedXML) []map[string]any {
	transmitters := transmitterList(feed)
	out := make([]map[string]any, 0, len(transmitters))
	siteNames := siteNamesForFeed(feed)
	for _, transmitter := range transmitters {
		payload := transmitterPayload(transmitter, feed)
		payload["site_names"] = siteNames
		out = append(out, payload)
	}
	return out
}

func transmitterPayload(transmitter transmitterXML, feed feedXML) map[string]any {
	frequency, _ := strconv.ParseFloat(transmitter.frequencyText(), 64)
	payload := map[string]any{
		"site_name":     fallbackText(transmitter.SiteName, feed.ID),
		"site_names":    siteNamesForFeed(feed),
		"callsign":      strings.TrimSpace(transmitter.Callsign),
		"relationship":  transmitter.relationship(),
		"host_name":     strings.TrimSpace(transmitter.HostName),
		"frequency_mhz": frequency,
		"gpclk":         transmitter.gpclk(),
		"gpio":          transmitter.gpio(),
		"rds":           transmitter.rdsPayload(feed),
	}
	if transmitter.frequencyText() == "" {
		delete(payload, "frequency_mhz")
	}
	return payload
}

func siteNamesForFeed(feed feedXML) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, transmitter := range transmitterList(feed) {
		if transmitter.isRelationship("ip") {
			continue
		}
		name := strings.TrimSpace(transmitter.SiteName)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	if len(out) == 0 {
		out = append(out, feed.ID)
	}
	return out
}

func (t transmitterXML) empty() bool {
	return strings.TrimSpace(t.SiteName) == "" &&
		strings.TrimSpace(t.Callsign) == "" &&
		strings.TrimSpace(t.Relationship) == "" &&
		strings.TrimSpace(t.HostName) == "" &&
		t.frequencyText() == ""
}

func (t transmitterXML) relationship() string {
	relationship := strings.ToLower(strings.TrimSpace(t.Relationship))
	if relationship == "secondary/repeater" {
		return "secondary"
	}
	if relationship == "" {
		return "unknown"
	}
	return relationship
}

func (t transmitterXML) isRelationship(relationship string) bool {
	current := t.relationship()
	wanted := strings.ToLower(strings.TrimSpace(relationship))
	return current == wanted || (wanted == "repeater" && current == "secondary")
}

func (t transmitterXML) frequencyText() string {
	return strings.TrimSpace(t.FrequencyMHz.Value)
}

func (t transmitterXML) gpclk() string {
	configured := strings.TrimSpace(t.FrequencyMHz.GPCLK)
	if configured != "" {
		return configured
	}
	if t.isRelationship("fm") {
		return "2"
	}
	return "0"
}

func (t transmitterXML) gpio() string {
	if configured := strings.TrimSpace(t.FrequencyMHz.GPIO); configured != "" {
		return configured
	}
	switch t.gpclk() {
	case "0":
		return "4"
	case "1":
		return "5"
	case "2":
		return "6"
	default:
		return ""
	}
}

func (t transmitterXML) rdsPayload(feed feedXML) map[string]any {
	enabled := xmlBool(t.RDS.EnabledRaw, t.isRelationship("fm"))
	pi := strings.ToUpper(strings.TrimSpace(t.RDS.PI))
	if pi == "" {
		pi = defaultRDSPI(feed.ID)
	}
	ps := shortRDSText(t.RDS.PS, "HAZE WX")
	site := fallbackText(t.SiteName, feed.ID)
	frequency := strings.TrimSpace(t.frequencyText())
	rt := strings.TrimSpace(t.RDS.RT)
	if rt == "" {
		parts := []string{"Haze Weather Radio", site}
		if callsign := strings.TrimSpace(t.Callsign); callsign != "" {
			parts = append(parts, callsign)
		}
		if frequency != "" {
			parts = append(parts, frequency+" MHz")
		}
		rt = strings.Join(parts, " - ")
	}
	return map[string]any{
		"enabled": enabled,
		"pi":      truncate(pi, 4),
		"ps":      ps,
		"rt":      truncate(rt, 64),
		"pty":     fallbackText(t.RDS.PTY, "3"),
		"tp":      fallbackText(t.RDS.TP, "0"),
		"af":      normalizedRDSAF(t.RDS.AF),
	}
}

func defaultRDSPI(feedID string) string {
	sum := sha1.Sum([]byte(fallbackText(feedID, "haze")))
	pi := strings.ToUpper(hex.EncodeToString(sum[:2]))
	if pi == "0000" {
		return "48A5"
	}
	return pi
}

func shortRDSText(value string, fallback string) string {
	text := strings.ToUpper(fallbackText(value, fallback))
	builder := strings.Builder{}
	for _, r := range text {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == ' ' {
			builder.WriteRune(r)
		}
	}
	return truncate(strings.TrimSpace(builder.String()), 8)
}

func normalizedRDSAF(values []string) []string {
	out := []string{}
	for _, value := range values {
		text := strings.TrimSpace(value)
		if text == "" {
			continue
		}
		frequency, err := strconv.ParseFloat(text, 64)
		if err != nil || frequency < 87.6 || frequency > 107.9 {
			continue
		}
		out = append(out, strconv.FormatFloat(frequency, 'f', 1, 64))
	}
	return uniqueStrings(out)
}

func publicWebRTCAvailable(config Config, feeds []map[string]any) bool {
	if !config.Webpanel.Public.Feeds.WebRTC.Enabled {
		return false
	}
	if publicFeedAccess(config) == "disabled" {
		return false
	}
	for _, feed := range feeds {
		if enabled, _ := feed["enabled"].(bool); !enabled {
			continue
		}
		if webrtc, _ := feed["webrtc_enabled"].(bool); webrtc {
			return true
		}
	}
	return false
}

func publicFeedAccess(config Config) string {
	access := strings.ToLower(strings.TrimSpace(config.Webpanel.Public.Feeds.Access))
	switch access {
	case "public", "auth_required":
		return access
	default:
		return "disabled"
	}
}

func publicAlertsArchiveAccess(config Config) string {
	access := strings.ToLower(strings.TrimSpace(config.Webpanel.Public.AlertsArchive.Access))
	if access == "public" {
		return "public"
	}
	return "disabled"
}

func outputLabels(output outputXML) []string {
	items := []string{}
	addOutput := func(name string, node outputNodeXML) {
		if xmlBool(node.EnabledRaw, false) {
			items = append(items, name)
		}
	}
	addOutput("webrtc", output.WebRTC)
	addOutput("udp", output.UDP)
	addOutput("rtp", output.RTP)
	addOutput("icecast", output.Icecast)
	streamLabel := strings.TrimSpace(output.Stream.Type)
	if streamLabel == "" {
		streamLabel = "stream"
	}
	addOutput(streamLabel, output.Stream)
	addOutput("rtmp", output.RTMP)
	addOutput("srt", output.SRT)
	addOutput("rtsp", output.RTSP)
	addOutput("audio_device", output.AudioDevice)
	addOutput("file", output.File)
	return items
}

func outputHTTPStreamEnabled(output outputXML) bool {
	return xmlBool(output.Stream.EnabledRaw, false) || xmlBool(output.Icecast.EnabledRaw, false)
}

func coverageRegionPayloads(feed feedXML, forecastNames map[string]string, clcNames map[string]string) []map[string]any {
	out := make([]map[string]any, 0, len(feed.Locations.Coverage.Regions))
	for _, region := range feed.Locations.Coverage.Regions {
		id := strings.TrimSpace(region.ID)
		if id == "" {
			continue
		}
		name := fallbackText(region.Name, fallbackText(forecastNames[id], fallbackText(clcNames[id], id)))
		subregionIDs := expandedCoverageSubregionIDs(region, clcNames)
		subregions := make([]map[string]any, 0, len(subregionIDs))
		for _, subID := range subregionIDs {
			if subID == "" {
				continue
			}
			subregions = append(subregions, map[string]any{
				"id":   subID,
				"name": fallbackText(clcNames[subID], subID),
			})
		}
		out = append(out, map[string]any{
			"id":                    id,
			"name":                  name,
			"source":                strings.TrimSpace(region.Source),
			"derive_forecast":       strings.TrimSpace(region.DeriveForecast),
			"canonical_id":          strings.TrimSpace(region.CanonicalID),
			"forecast_canonical_id": strings.TrimSpace(region.ForecastCanonicalID),
			"subregions":            subregions,
		})
	}
	return out
}

func feedCoverageCodes(feed feedXML, clcNames map[string]string) map[string]struct{} {
	codes := map[string]struct{}{}
	for _, region := range feed.Locations.Coverage.Regions {
		for _, code := range expandedCoverageRegionIDs(region.ID, clcNames) {
			addCode(codes, code)
		}
		for _, subregion := range expandedCoverageSubregionIDs(region, clcNames) {
			addCode(codes, subregion)
		}
	}
	return codes
}

func feedSameLocations(feed feedXML, clcNames map[string]string, nwsFIPSNames map[string]string) []string {
	if feedCoversAllLocations(feed) {
		return allCanadaUSSameLocations(clcNames, nwsFIPSNames)
	}
	codes := map[string]struct{}{}
	for _, region := range feed.Locations.Coverage.Regions {
		for _, code := range expandedCoverageRegionIDs(region.ID, clcNames) {
			addCode(codes, code)
		}
		for _, subregion := range expandedCoverageSubregionIDs(region, clcNames) {
			addCode(codes, subregion)
		}
	}
	return sortedKeys(codes)
}

func feedCoversAllLocations(feed feedXML) bool {
	if len(feed.Locations.Coverage.Regions) > 0 {
		return false
	}
	return xmlBool(feed.Alerts.CapCP.EnabledRaw, true) || xmlBool(feed.Alerts.NWSCAP.EnabledRaw, false)
}

func allCanadaUSSameLocations(clcNames map[string]string, nwsFIPSNames map[string]string) []string {
	codes := map[string]struct{}{"000000": {}}
	for code := range clcNames {
		addCode(codes, cleanLocationCode(code))
	}
	for code := range nwsFIPSNames {
		addCode(codes, cleanLocationCode(code))
	}
	out := sortedKeys(codes)
	if len(out) == 0 || out[0] == "000000" {
		return out
	}
	return append([]string{"000000"}, removeString(out, "000000")...)
}

func removeString(values []string, unwanted string) []string {
	out := values[:0]
	for _, value := range values {
		if value != unwanted {
			out = append(out, value)
		}
	}
	return out
}

func expandedCoverageSubregionIDs(region coverageRegionXML, clcNames map[string]string) []string {
	codes := map[string]struct{}{}
	for _, subregion := range region.Subregions {
		addCode(codes, subregion.ID)
	}
	for _, subregion := range alertWildcardSubregions(region.ID, clcNames) {
		addCode(codes, subregion)
	}
	return sortedKeys(codes)
}

func expandedCoverageRegionIDs(raw string, clcNames map[string]string) []string {
	if wildcardLocationPrefix(raw) != "" {
		return alertWildcardCoverageIDs(raw, clcNames)
	}
	code := cleanLocationCode(raw)
	if code == "" {
		return nil
	}
	return []string{code}
}

func alertWildcardCoverageIDs(raw string, clcNames map[string]string) []string {
	prefix := wildcardLocationPrefix(raw)
	if prefix == "" {
		return nil
	}
	out := []string{}
	for candidate := range clcNames {
		candidateCode := cleanLocationCode(candidate)
		if candidateCode == "" {
			continue
		}
		if strings.HasPrefix(candidateCode, prefix) {
			out = append(out, candidateCode)
		}
	}
	sort.Strings(out)
	return out
}

func wildcardLocationPrefix(raw string) string {
	text := strings.ToUpper(strings.TrimSpace(raw))
	star := strings.IndexRune(text, '*')
	if star < 0 {
		return ""
	}
	text = text[:star]
	builder := strings.Builder{}
	for _, r := range text {
		if r >= '0' && r <= '9' {
			builder.WriteRune(r)
		}
	}
	prefix := builder.String()
	if len(prefix) >= 1 && len(prefix) < 6 {
		return prefix
	}
	return ""
}

func alertWildcardSubregions(raw string, clcNames map[string]string) []string {
	code := cleanLocationCode(raw)
	if len(code) != 6 || !strings.HasSuffix(code, "00") {
		return nil
	}
	prefix := code[:4]
	out := []string{}
	for candidate := range clcNames {
		candidateCode := cleanLocationCode(candidate)
		if candidateCode == "" || candidateCode == code {
			continue
		}
		if strings.HasPrefix(candidateCode, prefix) {
			out = append(out, candidateCode)
		}
	}
	sort.Strings(out)
	return out
}

func addCode(codes map[string]struct{}, raw string) {
	code := strings.TrimSpace(raw)
	if code != "" {
		codes[code] = struct{}{}
	}
}

func feedLanguages(feed feedXML) []string {
	out := []string{}
	for _, lang := range feed.Languages.Langs {
		if code := strings.TrimSpace(lang.Code); code != "" {
			out = append(out, code)
		}
	}
	if len(out) == 0 {
		out = append(out, "en-CA")
	}
	return uniqueStrings(out)
}

func feedLocationCount(feed feedXML) int {
	return len(feed.Locations.ObservationLocations.Locations) +
		len(feed.Locations.AviationReportLocations.Locations) +
		len(feed.Locations.AirQualityLocations.Locations) +
		len(feed.Locations.ClimateLocations.Locations) +
		len(feed.Locations.MarineForecastLocations.Locations) +
		len(feed.Locations.MarineForecastLocations.Subregions) +
		len(feed.Locations.MarineConditions.Locations) +
		len(feed.Locations.HydrometricLocations.Locations) +
		len(feed.Locations.HydrometricLocations.Upstream.Locations) +
		len(feed.Locations.HydrometricLocations.Downstream.Locations)
}

func feedCanonicalLocationIDs(feed feedXML) []string {
	ids := []string{}
	for _, region := range feed.Locations.Coverage.Regions {
		ids = append(ids, region.CanonicalID, region.ForecastCanonicalID)
		for _, subregion := range region.Subregions {
			ids = append(ids, subregion.CanonicalID)
		}
	}
	groups := [][]locationXML{
		feed.Locations.ObservationLocations.Locations,
		feed.Locations.AviationReportLocations.Locations,
		feed.Locations.AirQualityLocations.Locations,
		feed.Locations.ClimateLocations.Locations,
		feed.Locations.MarineForecastLocations.Locations,
		feed.Locations.MarineForecastLocations.Subregions,
		feed.Locations.MarineConditions.Locations,
		feed.Locations.HydrometricLocations.Locations,
		feed.Locations.HydrometricLocations.Upstream.Locations,
		feed.Locations.HydrometricLocations.Downstream.Locations,
	}
	for _, group := range groups {
		for _, location := range group {
			ids = append(ids, location.CanonicalID)
		}
	}
	return uniqueStrings(ids)
}

func loadForecastRegionNames(path string) map[string]string {
	if snap, ok := loadLocationSnapshotFromCSVPath(path); ok {
		out := map[string]string{}
		for _, place := range snap.PlacesBySource("forecast") {
			out[place.Code] = place.Name
		}
		if len(out) > 0 {
			return out
		}
	}
	return loadCSVNames(path, 0, 1)
}

func loadCLCNames(path string) map[string]string {
	if snap, ok := loadLocationSnapshotFromCSVPath(path); ok {
		out := map[string]string{}
		for _, place := range snap.PlacesBySource("clc") {
			out[place.Code] = place.Name
		}
		if len(out) > 0 {
			return out
		}
	}
	return loadCSVNames(path, 0, 2)
}

func loadNWSFIPSNames(path string) map[string]string {
	if snap, ok := loadLocationSnapshotFromCSVPath(path); ok {
		out := map[string]string{}
		for _, source := range []string{"nws_same", "nws_zone", "nws_marine_same", "nws_marine_zone"} {
			for _, place := range snap.PlacesBySource(source) {
				out[place.Code] = place.Name
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return map[string]string{}
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, "|")
		if len(parts) < 7 || strings.EqualFold(strings.TrimSpace(parts[0]), "STATE") {
			continue
		}
		code := cleanLocationCode(parts[6])
		if code == "" {
			continue
		}
		name := strings.TrimSpace(parts[5])
		if name == "" {
			name = strings.TrimSpace(parts[3])
		}
		if state := strings.TrimSpace(parts[0]); state != "" && name != "" {
			name = name + ", " + strings.ToUpper(state)
		}
		if name != "" {
			out[code] = name
		}
	}
	return out
}

func loadNWSMarineNames(path string) map[string]string {
	if snap, ok := loadLocationSnapshotFromCSVPath(path); ok {
		out := map[string]string{}
		for _, source := range []string{"nws_marine_same", "nws_marine_zone"} {
			for _, place := range snap.PlacesBySource(source) {
				out[place.Code] = place.Name
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return map[string]string{}
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil || len(rows) == 0 {
		return map[string]string{}
	}
	header := map[string]int{}
	for i, value := range rows[0] {
		header[strings.ToLower(strings.TrimSpace(value))] = i
	}
	sameIndex := csvHeaderIndex(header, "same_code", "same", "ssnum")
	zoneIndex := csvHeaderIndex(header, "zone_ugc", "zone", "ugc")
	nameIndex := csvHeaderIndex(header, "name", "zonename", "zone_name")
	if sameIndex < 0 || nameIndex < 0 {
		return map[string]string{}
	}
	out := map[string]string{}
	for _, row := range rows[1:] {
		name := csvCell(row, nameIndex)
		if name == "" {
			continue
		}
		for _, raw := range []string{csvCell(row, sameIndex), csvCell(row, zoneIndex)} {
			code := cleanLocationCode(raw)
			if code == "" {
				code = strings.ToUpper(strings.TrimSpace(raw))
			}
			if code != "" {
				out[code] = name
			}
		}
	}
	return out
}

func loadLocationSnapshotFromCSVPath(path string) (locationdb.Snapshot, bool) {
	path = filepath.Clean(path)
	baseDir := filepath.Dir(filepath.Dir(filepath.Dir(path)))
	if strings.TrimSpace(baseDir) == "." || strings.TrimSpace(baseDir) == "" {
		return locationdb.Snapshot{}, false
	}
	return locationdb.Load(baseDir)
}

func csvHeaderIndex(header map[string]int, keys ...string) int {
	for _, key := range keys {
		if index, ok := header[strings.ToLower(strings.TrimSpace(key))]; ok {
			return index
		}
	}
	return -1
}

func csvCell(row []string, index int) string {
	if index < 0 || index >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[index])
}

func loadCSVNames(path string, codeIndex int, nameIndex int) map[string]string {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return map[string]string{}
	}
	lines := strings.Split(string(raw), "\n")
	out := map[string]string{}
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := splitCSVLine(line)
		if i == 0 && len(parts) > codeIndex && strings.EqualFold(strings.TrimSpace(parts[codeIndex]), "code") {
			continue
		}
		if len(parts) <= codeIndex || len(parts) <= nameIndex {
			continue
		}
		code := strings.TrimSpace(parts[codeIndex])
		name := strings.TrimSpace(parts[nameIndex])
		if code != "" && name != "" {
			out[code] = name
		}
	}
	return out
}

func splitCSVLine(line string) []string {
	parts := []string{}
	var builder strings.Builder
	quoted := false
	for _, r := range line {
		switch r {
		case '"':
			quoted = !quoted
		case ',':
			if !quoted {
				parts = append(parts, builder.String())
				builder.Reset()
				continue
			}
			builder.WriteRune(r)
		default:
			builder.WriteRune(r)
		}
	}
	parts = append(parts, builder.String())
	return parts
}

func runtimeEvents(configPath string) []map[string]any {
	lines := logLines(resolveConfigPath(configPath, "logs/haze.log"), 12)
	out := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		out = append(out, map[string]any{
			"kind":      "log",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
			"message":   line,
		})
	}
	return out
}

func logsPayload(configPath string, request *http.Request) map[string]any {
	source := "app"
	if request != nil {
		source = fallbackText(request.URL.Query().Get("source"), "app")
	}
	path := resolveConfigPath(configPath, "logs/haze.log")
	return map[string]any{"source": source, "lines": logLines(path, 120)}
}

func logLines(path string, limit int) []string {
	raw, err := readLogTail(filepath.Clean(path), limit)
	if err != nil {
		return []string{}
	}
	lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if limit > 0 && len(lines) > limit {
		lines = lines[len(lines)-limit:]
	}
	return lines
}

func readLogTail(path string, limit int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() <= 0 {
		return []byte{}, nil
	}
	readBytes := int64(256 * 1024)
	if limit > 0 {
		readBytes = int64(maxInt(32*1024, limit*2048))
	}
	if readBytes > info.Size() {
		readBytes = info.Size()
	}
	if _, err := file.Seek(info.Size()-readBytes, io.SeekStart); err != nil {
		return nil, err
	}
	raw := make([]byte, readBytes)
	n, err := io.ReadFull(file, raw)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	raw = raw[:n]
	if info.Size() > readBytes {
		if idx := bytesIndexByte(raw, '\n'); idx >= 0 && idx+1 < len(raw) {
			raw = raw[idx+1:]
		}
	}
	return raw, nil
}

func bytesIndexByte(raw []byte, needle byte) int {
	for i, value := range raw {
		if value == needle {
			return i
		}
	}
	return -1
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}

func datapoolPayload(configPath string) map[string]any {
	for _, rel := range []string{"runtime/state/dataPool.json", "managed/runtime/dataPool.json"} {
		if payload := readJSONMap(resolveConfigPath(configPath, rel)); len(payload) > 0 {
			return payload
		}
	}
	return map[string]any{}
}

func adminConfigPayload(config Config, root map[string]any) map[string]any {
	return map[string]any{
		"version":  config.Version,
		"site":     siteName(config),
		"webpanel": daemonSettingsView(root)["webpanel"],
		"services": daemonSettingsView(root)["services"],
		"same": map[string]any{
			"sender": strings.TrimSpace(os.Getenv("SAME_ID")),
		},
	}
}

func lastConnectedPayload(request *http.Request) map[string]any {
	ip := "unknown"
	if request != nil {
		ip = strings.TrimSpace(request.Header.Get("X-Forwarded-For"))
		if strings.Contains(ip, ",") {
			ip = strings.TrimSpace(strings.Split(ip, ",")[0])
		}
		if ip == "" {
			ip = strings.TrimSpace(request.RemoteAddr)
			if host, _, err := strings.Cut(ip, ":"); err {
				ip = host
			}
		}
	}
	return map[string]any{"ip": fallbackText(ip, "unknown"), "at": time.Now().UTC().Format(time.RFC3339)}
}

func tlsStatePayload(config Config, request *http.Request) map[string]any {
	host := ""
	if request != nil {
		host = request.Host
	}
	https := request != nil && requestIsHTTPS(request)
	actualDomain := isActualDomain(host)
	enabled := config.Webpanel.TLS.Enabled
	needsSetup := actualDomain && !https && !enabled
	message := "Panel is using local HTTP."
	if https {
		message = "HTTPS is active."
	} else if needsSetup {
		message = "This looks like a real domain; HTTPS can be enabled from TLS settings."
	}
	return map[string]any{
		"enabled":            enabled,
		"https":              https,
		"host":               host,
		"actual_domain":      actualDomain,
		"needs_setup":        needsSetup,
		"configured_domains": config.Webpanel.TLS.Domains,
		"message":            message,
	}
}

func adminURL(config Config, request *http.Request) string {
	if requestUsesLocalHost(request) {
		adminPort := config.Webpanel.Admin.Port.Int()
		if adminPort <= 0 ||
			(config.Webpanel.Port > 0 && config.Webpanel.Admin.Port == config.Webpanel.Port) ||
			requestHostPort(request) == adminPort {
			return "/admin"
		}
		hostPart := requestHostName(request)
		if hostPart == "" {
			hostPart = "localhost"
		}
		scheme := "http"
		if requestIsHTTPS(request) {
			scheme = "https"
		}
		return scheme + "://" + net.JoinHostPort(hostPart, strconv.Itoa(config.Webpanel.Admin.Port.Int())) + "/admin"
	}
	if base := configuredPublicBaseURL(config, request); base != "" {
		return base + "/admin"
	}
	if config.Webpanel.Admin.Port <= 0 || (config.Webpanel.Port > 0 && config.Webpanel.Admin.Port == config.Webpanel.Port) {
		return "/admin"
	}
	hostPart := normalizeDomain(config.Webpanel.Admin.Host)
	if hostPart == "" || hostPart == "0.0.0.0" || hostPart == "::" {
		hostPart = requestHostName(request)
		if hostPart == "" {
			hostPart = "localhost"
		}
	}
	scheme := "http"
	if requestIsHTTPS(request) {
		scheme = "https"
	}
	if config.Webpanel.Admin.Port > 0 {
		return scheme + "://" + net.JoinHostPort(hostPart, strconv.Itoa(config.Webpanel.Admin.Port.Int())) + "/admin"
	}
	return "/admin"
}

func publicBaseURL(config Config, request *http.Request) string {
	if requestUsesLocalHost(request) {
		return requestBaseURL(request)
	}
	if base := configuredPublicBaseURL(config, request); base != "" {
		return base
	}
	return requestBaseURL(request)
}

func requestBaseURL(request *http.Request) string {
	if request == nil || strings.TrimSpace(request.Host) == "" {
		return ""
	}
	scheme := "http"
	if requestIsHTTPS(request) {
		scheme = "https"
	}
	return scheme + "://" + strings.TrimRight(request.Host, "/")
}

func requestHostPort(request *http.Request) int {
	if request == nil {
		return 0
	}
	host := strings.TrimSpace(request.Host)
	if host == "" && request.URL != nil {
		host = strings.TrimSpace(request.URL.Host)
	}
	if host == "" {
		return 0
	}
	_, rawPort, err := net.SplitHostPort(host)
	if err != nil || rawPort == "" {
		return 0
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		return 0
	}
	return port
}

func configuredPublicBaseURL(config Config, request *http.Request) string {
	for _, raw := range []string{
		os.Getenv("HAZE_PUBLIC_BASE_URL"),
		os.Getenv("WEB_BASE_URL"),
		os.Getenv("WEB_HOSTNAME"),
		firstNonBlank(config.Webpanel.TLS.Domains...),
	} {
		if base := normalizePublicBaseURL(raw, request); base != "" {
			return base
		}
	}
	return ""
}

func normalizePublicBaseURL(raw string, request *http.Request) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return ""
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ""
	}
	if parsed.Scheme == "http" && requestIsHTTPS(request) {
		parsed.Scheme = "https"
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/")
}

func publicAlertsArchivePayload(configPath string) (map[string]any, error) {
	now := time.Now().UTC()
	active, rejected, expired := archiveStoreRecordBuckets(configPath, now)

	byFeed := map[string][]map[string]any{}
	for _, record := range active {
		feedID := fallbackText(record.FeedID, "unknown")
		byFeed[feedID] = append(byFeed[feedID], publicArchivePayloadFromRecord(record, "accepted"))
	}
	acceptedTruncated := false
	for feedID, records := range byFeed {
		limited, truncated := limitPublicArchiveRecords(records, publicArchiveAcceptedPerFeedLimit)
		byFeed[feedID] = limited
		acceptedTruncated = acceptedTruncated || truncated
	}
	rejectedRecords, rejectedTruncated := limitPublicArchiveRecords(publicArchivePayloadsFromRecords(rejected, "rejected"), publicArchiveBucketLimit)
	expiredRecords, expiredTruncated := limitPublicArchiveRecords(publicArchivePayloadsFromRecords(expiredWithin(expired, now, 30*24*time.Hour), "expired"), publicArchiveBucketLimit)
	return map[string]any{
		"by_feed":  byFeed,
		"rejected": rejectedRecords,
		"expired":  expiredRecords,
		"limits": map[string]any{
			"accepted_per_feed": publicArchiveAcceptedPerFeedLimit,
			"bucket":            publicArchiveBucketLimit,
		},
		"truncated": map[string]any{
			"accepted": acceptedTruncated,
			"rejected": rejectedTruncated,
			"expired":  expiredTruncated,
		},
	}, nil
}

func publicArchivePayloadsFromRecords(records []archiveCAPRecord, bucket string) []map[string]any {
	out := make([]map[string]any, 0, len(records))
	for _, record := range records {
		out = append(out, publicArchivePayloadFromRecord(record, bucket))
	}
	return out
}

func publicArchivePayloadFromRecord(record archiveCAPRecord, bucket string) map[string]any {
	info := chooseArchiveInfo(record.Alert)
	areas := archiveAreaNames(info)
	audio := archiveBroadcastAudio(record.Alert)
	return publicArchiveRecordPayload(map[string]any{
		"id":              fallbackString(record.ID, record.Alert.Identifier),
		"feed_id":         record.FeedID,
		"bucket":          bucket,
		"status":          fallbackString(record.Status, bucket),
		"reason":          record.Reason,
		"updated_at":      record.UpdatedAt,
		"sender":          record.Alert.Sender,
		"sent":            record.Alert.Sent,
		"message_type":    record.Alert.MessageType,
		"headline":        alerttext.NormalizeHeadline(fallbackString(info.Headline, info.Event)),
		"event":           info.Event,
		"severity":        info.Severity,
		"urgency":         info.Urgency,
		"certainty":       info.Certainty,
		"effective":       info.Effective,
		"onset":           info.Onset,
		"expires":         info.Expires,
		"description":     info.Description,
		"instruction":     info.Instruction,
		"areas":           areas,
		"area_text":       alerttext.JoinParts(areas),
		"audio_url":       audio.URL,
		"audio_mime_type": audio.MimeType,
		"cap_xml_url":     archivePublicCAPXMLURL(record),
	})
}

func archivePublicCAPXMLURL(record archiveCAPRecord) string {
	return publicCAPXMLURLFromAdminURL(archiveCAPXMLURL(record))
}

const (
	publicArchiveAcceptedPerFeedLimit = 25
	publicArchiveBucketLimit          = 25
	publicArchiveTextFieldLimit       = 520
	publicArchiveAreaLimit            = 24
	publicArchiveAreaTextLimit        = 120
)

func limitPublicArchiveRecords(records []map[string]any, limit int) ([]map[string]any, bool) {
	if limit <= 0 || len(records) <= limit {
		return records, false
	}
	return records[:limit], true
}

func publicArchiveRecordMaps(value any) []map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, publicArchiveRecordPayload(item))
		}
		return out
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, publicArchiveRecordMaps(item)...)
		}
		return out
	case map[string]any:
		if looksLikeArchiveRecordPayload(typed) {
			return []map[string]any{publicArchiveRecordPayload(typed)}
		}
		out := []map[string]any{}
		for _, item := range typed {
			out = append(out, publicArchiveRecordMaps(item)...)
		}
		return out
	default:
		return nil
	}
}

func publicArchiveRecordPayload(record map[string]any) map[string]any {
	out := map[string]any{}
	areaList := publicArchiveAreasPayload(record["areas"])
	hasAreaList := len(areaList) > 0
	for _, key := range []string{
		"id",
		"feed_id",
		"bucket",
		"status",
		"reason",
		"sender",
		"sent",
		"message_type",
		"headline",
		"event",
		"severity",
		"urgency",
		"certainty",
		"effective",
		"onset",
		"expires",
		"description",
		"instruction",
		"areas",
		"area_text",
		"audio_url",
		"audio_mime_type",
		"cap_xml_url",
		"updated_at",
	} {
		value, ok := record[key]
		if !ok {
			continue
		}
		if key == "area_text" && hasAreaList {
			continue
		}
		if key == "description" || key == "instruction" || key == "area_text" || key == "reason" {
			value = limitPublicArchiveText(fmt.Sprint(value), publicArchiveTextFieldLimit)
		}
		if key == "audio_url" && !archiveAudioURLAllowed(fmt.Sprint(value)) {
			continue
		}
		if key == "areas" {
			value = areaList
		}
		out[key] = value
	}
	return out
}

func publicArchiveAreasPayload(value any) []string {
	values := []string{}
	switch typed := value.(type) {
	case []string:
		values = typed
	case []any:
		values = make([]string, 0, len(typed))
		for _, item := range typed {
			text := strings.TrimSpace(fmt.Sprint(item))
			if text != "" {
				values = append(values, text)
			}
		}
	default:
		return nil
	}
	out := make([]string, 0, min(len(values), publicArchiveAreaLimit+1))
	for _, value := range values {
		if len(out) >= publicArchiveAreaLimit {
			break
		}
		out = append(out, limitPublicArchiveText(value, publicArchiveAreaTextLimit))
	}
	if remaining := len(values) - len(out); remaining > 0 {
		out = append(out, fmt.Sprintf("and %d more areas", remaining))
	}
	return out
}

func limitPublicArchiveText(value string, limit int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if limit <= 0 || len(runes) <= limit {
		return value
	}
	return strings.TrimSpace(string(runes[:limit])) + "..."
}

func looksLikeArchiveRecordPayload(record map[string]any) bool {
	for _, key := range []string{"id", "feed_id", "event", "headline", "sent", "expires", "description", "instruction", "cap_xml_url"} {
		if _, ok := record[key]; ok {
			return true
		}
	}
	return false
}

func publicArchiveCAPXMLLinks(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = publicArchiveCAPXMLLinks(item)
		}
		if rawURL := strings.TrimSpace(fmt.Sprint(typed["cap_xml_url"])); rawURL != "" {
			out["cap_xml_url"] = publicCAPXMLURLFromAdminURL(rawURL)
		}
		return out
	case map[string][]map[string]any:
		out := make(map[string][]map[string]any, len(typed))
		for key, records := range typed {
			out[key] = publicArchiveCAPXMLRecordLinks(records)
		}
		return out
	case []map[string]any:
		return publicArchiveCAPXMLRecordLinks(typed)
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, publicArchiveCAPXMLLinks(item))
		}
		return out
	default:
		return value
	}
}

func publicArchiveCAPXMLRecordLinks(records []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(records))
	for _, record := range records {
		copied := make(map[string]any, len(record))
		for key, value := range record {
			copied[key] = value
		}
		if rawURL := strings.TrimSpace(fmt.Sprint(record["cap_xml_url"])); rawURL != "" {
			copied["cap_xml_url"] = publicCAPXMLURLFromAdminURL(rawURL)
		}
		out = append(out, copied)
	}
	return out
}

func publicCAPXMLURLFromAdminURL(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	if parsed.IsAbs() && parsed.Scheme != "http" && parsed.Scheme != "https" {
		return ""
	}
	if parsed.Path != "/api/v1/alerts/archive/cap.xml" && parsed.Path != "/api/public/v1/alerts/archive/cap.xml" {
		return ""
	}
	query := publicCAPXMLQuery(parsed.Query())
	if query == "" {
		return "/api/public/v1/alerts/archive/cap.xml"
	}
	return "/api/public/v1/alerts/archive/cap.xml?" + query
}

func publicCAPXMLQuery(values url.Values) string {
	parts := make([]string, 0, 2)
	if id := strings.TrimSpace(values.Get("id")); id != "" {
		parts = append(parts, "id="+url.QueryEscape(id))
	}
	if feedID := strings.TrimSpace(values.Get("feed_id")); feedID != "" {
		parts = append(parts, "feed_id="+url.QueryEscape(feedID))
	}
	return strings.Join(parts, "&")
}

func loadPackageIDs(configPath string) ([]string, error) {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return nil, err
	}
	path := textAt(root, []string{"products_file"}, "managed/configs/products.xml", 240)
	raw, err := os.ReadFile(resolveConfigPath(configPath, path))
	if err != nil && os.IsNotExist(err) {
		path = textAt(root, []string{"packages_file"}, "managed/configs/packages.xml", 240)
		raw, err = os.ReadFile(resolveConfigPath(configPath, path))
	}
	if err != nil {
		return nil, err
	}
	var parsed packageCatalogXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse package catalog XML: %w", err)
	}
	out := []string{}
	for _, item := range append(parsed.Products, parsed.Packages...) {
		id := strings.TrimSpace(item.ID)
		if id == "" || !xmlBool(item.EnabledRaw, true) {
			continue
		}
		out = append(out, id)
	}
	return uniqueStrings(out), nil
}

func loadSAMEMapping(configPath string) (map[string]any, error) {
	for _, rel := range []string{"managed/sameMapping.json", "managed/same_mapping.json"} {
		path := resolveConfigPath(configPath, rel)
		raw, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		var payload map[string]any
		if err := jsonUnmarshalMap(raw, &payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
	return map[string]any{"eas": map[string]any{}, "naadsToEas": map[string]any{}}, nil
}

func loadLocationNames(configPath string) (map[string]any, error) {
	names := map[string]any{}
	if snap, ok := locationdb.Load(filepath.Dir(filepath.Clean(configPath))); ok {
		for code, name := range snap.Labels() {
			names[code] = name
		}
		names["000000"] = "All areas"
		return names, nil
	}
	for code, name := range loadForecastRegionNames(resolveConfigPath(configPath, "managed/csv/FORECAST_LOCATIONS.csv")) {
		names[code] = name
	}
	for code, name := range loadCLCNames(resolveConfigPath(configPath, "managed/csv/CLC_Base_Zone.csv")) {
		if clean := cleanLocationCode(code); clean != "" {
			names[clean] = name
		}
	}
	for code, name := range loadNWSFIPSNames(resolveConfigPath(configPath, "managed/csv/NWS_ZONE_COUNTY_CORRELATION.csv")) {
		names[code] = name
	}
	for code, name := range loadNWSMarineNames(resolveConfigPath(configPath, "managed/csv/NWS_MARINE_ZONES.csv")) {
		names[code] = name
	}
	names["000000"] = "All areas"
	return names, nil
}

func cleanLocationCode(raw string) string {
	text := strings.ToUpper(strings.TrimSpace(raw))
	text = strings.Trim(text, "\"' ")
	builder := strings.Builder{}
	for _, r := range text {
		if r >= '0' && r <= '9' {
			builder.WriteRune(r)
		}
	}
	cleaned := builder.String()
	if len(cleaned) == 5 {
		return "0" + cleaned
	}
	if len(cleaned) != 6 {
		return ""
	}
	return cleaned
}

func resolveConfigPath(configPath string, configured string) string {
	configured = strings.TrimSpace(configured)
	if configured == "" {
		configured = "."
	}
	if filepath.IsAbs(configured) {
		return filepath.Clean(configured)
	}
	base := filepath.Dir(filepath.Clean(configPath))
	return filepath.Clean(filepath.Join(base, configured))
}

func fallbackText(value string, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return strings.TrimSpace(fallback)
}

func xmlBool(raw string, fallback bool) bool {
	text := strings.ToLower(strings.TrimSpace(raw))
	switch text {
	case "1", "true", "yes", "on", "enabled":
		return true
	case "0", "false", "no", "off", "disabled":
		return false
	default:
		return fallback
	}
}

func parseIntText(raw string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

var (
	hostnameOnce  sync.Once
	hostnameValue string
)

func hostname() string {
	hostnameOnce.Do(func() {
		hostnameValue = lookupHostname()
	})
	return hostnameValue
}

func lookupHostname() string {
	name, err := os.Hostname()
	if err != nil || strings.TrimSpace(name) == "" {
		return "unknown"
	}
	return name
}

func requestIP(request *http.Request) string {
	ip := "unknown"
	if request != nil {
		ip = strings.TrimSpace(request.Header.Get("X-Forwarded-For"))
		if strings.Contains(ip, ",") {
			ip = strings.TrimSpace(strings.Split(ip, ",")[0])
		}
		if ip == "" {
			ip = strings.TrimSpace(request.Header.Get("X-Real-IP"))
		}
		if ip == "" {
			ip = strings.TrimSpace(request.RemoteAddr)
			if host, _, found := strings.Cut(ip, ":"); found {
				ip = host
			}
		}
	}
	return fallbackText(ip, "unknown")
}

func serverIP(request *http.Request) string {
	if request != nil {
		if addr, ok := request.Context().Value(http.LocalAddrContextKey).(net.Addr); ok && addr != nil {
			if ip := addrIP(addr.String()); usableServerIP(ip) {
				return ip
			}
		}
		if ip := addrIP(request.Host); usableServerIP(ip) {
			return ip
		}
	}
	if ip := interfaceServerIP(); ip != "" {
		return ip
	}
	return "unknown"
}

func addrIP(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(value); err == nil {
		value = host
	} else if strings.Count(value, ":") == 1 {
		if host, _, found := strings.Cut(value, ":"); found {
			value = host
		}
	}
	return strings.Trim(value, "[]")
}

func usableServerIP(value string) bool {
	ip := net.ParseIP(strings.TrimSpace(value))
	if ip == nil {
		return false
	}
	return !ip.IsLoopback() && !ip.IsUnspecified() && !ip.IsMulticast()
}

func interfaceServerIP() string {
	return cachedInterfaceServerIP()
}

var (
	interfaceServerIPMu       sync.Mutex
	interfaceServerIPCached   string
	interfaceServerIPCachedAt time.Time
)

const interfaceServerIPCacheTTL = 30 * time.Second

func cachedInterfaceServerIP() string {
	interfaceServerIPMu.Lock()
	defer interfaceServerIPMu.Unlock()
	if time.Since(interfaceServerIPCachedAt) < interfaceServerIPCacheTTL {
		return interfaceServerIPCached
	}
	interfaceServerIPCached = scanInterfaceServerIP()
	interfaceServerIPCachedAt = time.Now()
	return interfaceServerIPCached
}

func scanInterfaceServerIP() string {
	type candidate struct {
		ip    string
		score int
	}
	candidates := []candidate{}
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip := interfaceAddrIP(addr)
			if ip == nil || !usableServerIP(ip.String()) || ip.IsLinkLocalUnicast() {
				continue
			}
			score := 1
			if ip4 := ip.To4(); ip4 != nil {
				score = 3
				if ip4.IsPrivate() {
					score = 4
				}
			} else if ip.IsPrivate() {
				score = 2
			}
			candidates = append(candidates, candidate{ip: ip.String(), score: score})
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0].ip
}

func interfaceAddrIP(addr net.Addr) net.IP {
	switch value := addr.(type) {
	case *net.IPNet:
		return value.IP
	case *net.IPAddr:
		return value.IP
	default:
		return net.ParseIP(addrIP(value.String()))
	}
}

var (
	gitCommitOnce  sync.Once
	gitCommitValue string
)

func gitCommit() string {
	gitCommitOnce.Do(func() {
		gitCommitValue = lookupGitCommit()
	})
	return gitCommitValue
}

func lookupGitCommit() string {
	for _, name := range []string{"HAZE_GIT_COMMIT", "GIT_COMMIT"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	if strings.TrimSpace(BuildGitCommit) != "" {
		return strings.TrimSpace(BuildGitCommit)
	}
	return "unknown"
}

func truncate(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit]
}

func queueSeverity(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return "Pending"
	}
	return strings.ToUpper(status[:1]) + strings.ToLower(status[1:])
}

func appendAny(value any, item any) []any {
	switch typed := value.(type) {
	case []any:
		return append(typed, item)
	case []map[string]any:
		out := make([]any, 0, len(typed)+1)
		for _, record := range typed {
			out = append(out, record)
		}
		return append(out, item)
	default:
		return []any{item}
	}
}

func jsonUnmarshalMap(raw []byte, target *map[string]any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}
	if *target == nil {
		*target = map[string]any{}
	}
	return nil
}
