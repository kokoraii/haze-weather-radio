package webgateway

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const defaultFeedsFile = "managed/feeds"

var feedIDCleaner = regexp.MustCompile(`[^a-zA-Z0-9_.:-]+`)

type adminFeedsXML struct {
	XMLName xml.Name       `xml:"feeds"`
	Feeds   []adminFeedXML `xml:"feed"`
}

type adminFeedXML struct {
	ID        string                    `xml:"id,attr"`
	Enabled   string                    `xml:"enabled,attr,omitempty"`
	Timezone  string                    `xml:"timezone,attr,omitempty"`
	Playout   adminFeedPlayoutXML       `xml:"playout"`
	Alerts    adminFeedAlertsXML        `xml:"alerts"`
	Languages adminFeedLanguagesXML     `xml:"languages"`
	Desc      adminFeedDescriptionXML   `xml:"description"`
	Locations adminFeedLocationsXML     `xml:"locations"`
	Tx        adminFeedTransmitterBlock `xml:"transmitter_metadata"`
}

type adminFeedPlayoutXML struct {
	Routine           string `xml:"routine,attr,omitempty"`
	SAME              string `xml:"same,attr,omitempty"`
	SAMEOriginator    string `xml:"same_originator,attr,omitempty"`
	SAMEAttentionTone string `xml:"same_attention_tone,attr,omitempty"`
}

type adminFeedAlertsXML struct {
	CapCP  adminFeedAlertSourceXML `xml:"cap_cp"`
	NWSCAP adminFeedAlertSourceXML `xml:"nws_cap"`
}

type adminFeedAlertSourceXML struct {
	Enabled string             `xml:"enabled,attr,omitempty"`
	Filter  adminFeedFilterXML `xml:"filter,omitempty"`
}

type adminFeedFilterXML struct {
	UseFeedLocations string               `xml:"use_feed_locations,attr,omitempty"`
	Allowlist        adminFeedRuleListXML `xml:"allowlist,omitempty"`
	Blocklist        adminFeedRuleListXML `xml:"blocklist,omitempty"`
}

type adminFeedRuleListXML struct {
	Severities  []string             `xml:"severity,omitempty"`
	Urgencies   []string             `xml:"urgency,omitempty"`
	Certainties []string             `xml:"certainty,omitempty"`
	NAADSEvents []string             `xml:"naads_event,omitempty"`
	Other       []adminFeedOtherRule `xml:"other,omitempty"`
}

type adminFeedOtherRule struct {
	ValueName string `xml:"value_name,attr,omitempty"`
	Value     string `xml:"value,attr,omitempty"`
}

type adminFeedLanguagesXML struct {
	Langs []adminFeedLangXML `xml:"lang"`
}

type adminFeedLangXML struct {
	Code     string `xml:"code,attr"`
	Interval string `xml:"interval,attr,omitempty"`
}

type adminFeedDescriptionXML struct {
	Langs []adminFeedDescriptionLangXML `xml:"lang"`
}

type adminFeedDescriptionLangXML struct {
	Code   string `xml:"code,attr"`
	Text   string `xml:"text,attr,omitempty"`
	Suffix string `xml:"suffix,attr,omitempty"`
}

type adminFeedLocationsXML struct {
	Coverage                adminFeedCoverageXML          `xml:"coverage"`
	ObservationLocations    adminFeedLocationList         `xml:"observationLocations,omitempty"`
	AviationReportLocations adminFeedLocationList         `xml:"aviationReportLocations,omitempty"`
	AirQualityLocations     adminFeedLocationList         `xml:"airQualityLocations,omitempty"`
	ClimateLocations        adminFeedLocationList         `xml:"climateLocations,omitempty"`
	MarineForecastLocations adminFeedMarineForecastXML    `xml:"marineForecastLocations,omitempty"`
	MarineConditions        adminFeedLocationList         `xml:"marineConditions,omitempty"`
	HydrometricLocations    adminFeedHydrometricLocations `xml:"hydrometricLocations,omitempty"`
}

type adminFeedCoverageXML struct {
	Regions []adminFeedCoverageRegionXML `xml:"region,omitempty"`
}

type adminFeedCoverageRegionXML struct {
	ID                  string                  `xml:"id,attr"`
	Source              string                  `xml:"source,attr,omitempty"`
	Name                string                  `xml:"name,attr,omitempty"`
	DeriveForecast      string                  `xml:"derive_forecast,attr,omitempty"`
	CanonicalID         string                  `xml:"canonical_id,attr,omitempty"`
	ForecastCanonicalID string                  `xml:"forecast_canonical_id,attr,omitempty"`
	Subregions          []adminFeedSubregionXML `xml:"subregion,omitempty"`
}

type adminFeedSubregionXML struct {
	ID          string `xml:"id,attr"`
	Source      string `xml:"source,attr,omitempty"`
	Name        string `xml:"name,attr,omitempty"`
	CanonicalID string `xml:"canonical_id,attr,omitempty"`
}

type adminFeedLocationList struct {
	Locations []adminFeedLocationXML `xml:"location,omitempty"`
}

type adminFeedLocationXML struct {
	ID           string `xml:"id,attr"`
	Source       string `xml:"source,attr,omitempty"`
	NameOverride string `xml:"name_override,attr,omitempty"`
	NormalID     string `xml:"normal_id,attr,omitempty"`
	Latitude     string `xml:"latitude,attr,omitempty"`
	Longitude    string `xml:"longitude,attr,omitempty"`
	CanonicalID  string `xml:"canonical_id,attr,omitempty"`
}

type adminFeedMarineForecastXML struct {
	Locations  []adminFeedLocationXML `xml:"location,omitempty"`
	Subregions []adminFeedLocationXML `xml:"subregion,omitempty"`
}

type adminFeedHydrometricLocations struct {
	Locations  []adminFeedLocationXML `xml:"location,omitempty"`
	Upstream   adminFeedLocationList  `xml:"upstream,omitempty"`
	Downstream adminFeedLocationList  `xml:"downstream,omitempty"`
}

type adminFeedTransmitterBlock struct {
	Transmitters []adminFeedTransmitterXML `xml:"transmitter"`
}

type adminFeedTransmitterXML struct {
	SiteName     string                `xml:"site_name,omitempty"`
	Callsign     string                `xml:"callsign,omitempty"`
	Relationship string                `xml:"relationship,omitempty"`
	HostName     string                `xml:"host_name,omitempty"`
	FrequencyMHz adminFeedFrequencyXML `xml:"frequency_mhz,omitempty"`
	Network      transmitterNetworkXML `xml:"network,omitempty"`
	RDS          transmitterRDSXML     `xml:"rds,omitempty"`
}

type adminFeedFrequencyXML struct {
	GPCLK string `xml:"gpclk,attr,omitempty"`
	GPIO  string `xml:"gpio,attr,omitempty"`
	Value string `xml:",chardata"`
}

func loadFeedsControlPayload(configPath string) (map[string]any, error) {
	path, rel, err := feedsControlPath(configPath)
	if err != nil {
		return nil, err
	}
	config, err := readAdminFeedsXML(path)
	if err != nil {
		return nil, err
	}
	playlist, _ := playlistStatePayload(configPath)
	return feedsControlPayload(path, rel, config.Feeds, playlist), nil
}

func saveFeedsControlPayload(configPath string, payload map[string]any) (map[string]any, error) {
	rawItems, ok := payload["feeds"].([]any)
	if !ok {
		return nil, fmt.Errorf("feeds payload is required")
	}
	feeds := make([]adminFeedXML, 0, len(rawItems))
	for _, raw := range rawItems {
		feed, err := adminFeedFromMap(raw)
		if err != nil {
			return nil, err
		}
		feeds = append(feeds, feed)
	}
	path, _, err := feedsControlPath(configPath)
	if err != nil {
		return nil, err
	}
	if err := writeAdminFeedsXML(path, adminFeedsXML{Feeds: feeds}); err != nil {
		return nil, err
	}
	return loadFeedsControlPayload(configPath)
}

func feedsControlPath(configPath string) (string, string, error) {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return "", "", err
	}
	rel := textAt(root, []string{"feeds_file"}, defaultFeedsFile, 240)
	return resolveConfigPath(configPath, rel), rel, nil
}

func readAdminFeedsXML(path string) (adminFeedsXML, error) {
	if info, err := os.Stat(filepath.Clean(path)); err == nil && info.IsDir() {
		entries, err := os.ReadDir(filepath.Clean(path)); if err != nil { return adminFeedsXML{}, err }
		parsed := adminFeedsXML{}
		for _, entry := range entries {
			if entry.IsDir() || strings.ToLower(filepath.Ext(entry.Name())) != ".xml" { continue }
			raw, err := os.ReadFile(filepath.Join(path, entry.Name())); if err != nil { return adminFeedsXML{}, err }
			var feed adminFeedXML; if err := xml.Unmarshal(raw, &feed); err != nil { return adminFeedsXML{}, err }
			if strings.TrimSpace(feed.ID) == "" { feed.ID = strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name())) }
			parsed.Feeds = append(parsed.Feeds, feed)
		}
		feeds, err := normalizeAdminFeeds(parsed.Feeds); if err != nil { return adminFeedsXML{}, err }
		parsed.Feeds = feeds
		return parsed, nil
	}
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return adminFeedsXML{}, nil
		}
		return adminFeedsXML{}, err
	}
	var parsed adminFeedsXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return adminFeedsXML{}, fmt.Errorf("feeds XML is invalid: %w", err)
	}
	feeds, err := normalizeAdminFeeds(parsed.Feeds)
	if err != nil {
		return adminFeedsXML{}, err
	}
	parsed.Feeds = feeds
	return parsed, nil
}

func writeAdminFeedsXML(path string, config adminFeedsXML) error {
	feeds, err := normalizeAdminFeeds(config.Feeds)
	if err != nil {
		return err
	}
	config.Feeds = feeds
	if info, err := os.Stat(filepath.Clean(path)); err == nil && info.IsDir() {
		for _, feed := range feeds {
			name := cleanFeedControlID(feed.ID) + ".xml"
			raw, err := xml.MarshalIndent(feed, "", "  "); if err != nil { return err }
			if err := os.WriteFile(filepath.Join(path, name), []byte(xml.Header+string(raw)+"\n"), 0o600); err != nil { return err }
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := xml.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(xml.Header+string(raw)+"\n"), 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func normalizeAdminFeeds(feeds []adminFeedXML) ([]adminFeedXML, error) {
	out := make([]adminFeedXML, 0, len(feeds))
	seen := map[string]struct{}{}
	for _, feed := range feeds {
		feed.ID = cleanFeedControlID(feed.ID)
		if feed.ID == "" {
			return nil, fmt.Errorf("feed id is required")
		}
		if _, ok := seen[feed.ID]; ok {
			return nil, fmt.Errorf("duplicate feed id %q", feed.ID)
		}
		seen[feed.ID] = struct{}{}
		feed.Enabled = boolText(xmlBool(feed.Enabled, true))
		feed.Timezone = fallbackText(feed.Timezone, "Local")
		feed.Playout.Routine = boolText(xmlBool(feed.Playout.Routine, true))
		feed.Playout.SAME = boolText(xmlBool(feed.Playout.SAME, true))
		feed.Playout.SAMEOriginator = strings.ToUpper(fallbackText(feed.Playout.SAMEOriginator, "EAS"))
		feed.Playout.SAMEAttentionTone = strings.ToUpper(strings.TrimSpace(feed.Playout.SAMEAttentionTone))
		feed.Alerts.CapCP.Enabled = boolText(xmlBool(feed.Alerts.CapCP.Enabled, true))
		feed.Alerts.NWSCAP.Enabled = boolText(xmlBool(feed.Alerts.NWSCAP.Enabled, false))
		feed.Alerts.CapCP.Filter.UseFeedLocations = boolText(xmlBool(feed.Alerts.CapCP.Filter.UseFeedLocations, true))
		feed.Alerts.NWSCAP.Filter.UseFeedLocations = boolText(xmlBool(feed.Alerts.NWSCAP.Filter.UseFeedLocations, true))
		feed.Languages.Langs = normalizeFeedLangs(feed.Languages.Langs)
		if len(feed.Languages.Langs) == 0 {
			feed.Languages.Langs = []adminFeedLangXML{{Code: "en-CA", Interval: "0"}}
		}
		feed.Desc.Langs = normalizeDescriptionLangs(feed.Desc.Langs)
		feed.Locations = normalizeAdminFeedLocations(feed.Locations)
		feed.Tx.Transmitters = normalizeAdminTransmitters(feed.Tx.Transmitters, feed.ID)
		out = append(out, feed)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func normalizeFeedLangs(langs []adminFeedLangXML) []adminFeedLangXML {
	out := make([]adminFeedLangXML, 0, len(langs))
	seen := map[string]bool{}
	for _, lang := range langs {
		code := fallbackText(lang.Code, "en-CA")
		if seen[strings.ToLower(code)] {
			continue
		}
		seen[strings.ToLower(code)] = true
		lang.Code = code
		lang.Interval = fallbackText(lang.Interval, "0")
		out = append(out, lang)
	}
	return out
}

func normalizeDescriptionLangs(langs []adminFeedDescriptionLangXML) []adminFeedDescriptionLangXML {
	out := make([]adminFeedDescriptionLangXML, 0, len(langs))
	for _, lang := range langs {
		lang.Code = fallbackText(lang.Code, "en-CA")
		lang.Text = strings.TrimSpace(lang.Text)
		lang.Suffix = strings.TrimSpace(lang.Suffix)
		out = append(out, lang)
	}
	return out
}

func normalizeAdminFeedLocations(loc adminFeedLocationsXML) adminFeedLocationsXML {
	loc.Coverage.Regions = normalizeCoverageRegions(loc.Coverage.Regions)
	loc.ObservationLocations.Locations = normalizeAdminLocations(loc.ObservationLocations.Locations)
	loc.AviationReportLocations.Locations = normalizeAdminLocations(loc.AviationReportLocations.Locations)
	loc.AirQualityLocations.Locations = normalizeAdminLocations(loc.AirQualityLocations.Locations)
	loc.ClimateLocations.Locations = normalizeAdminLocations(loc.ClimateLocations.Locations)
	loc.HydrometricLocations.Locations = normalizeAdminLocations(loc.HydrometricLocations.Locations)
	loc.HydrometricLocations.Upstream.Locations = normalizeAdminLocations(loc.HydrometricLocations.Upstream.Locations)
	loc.HydrometricLocations.Downstream.Locations = normalizeAdminLocations(loc.HydrometricLocations.Downstream.Locations)
	loc.MarineForecastLocations.Locations = normalizeAdminLocations(loc.MarineForecastLocations.Locations)
	loc.MarineForecastLocations.Subregions = normalizeAdminLocations(loc.MarineForecastLocations.Subregions)
	loc.MarineConditions.Locations = normalizeAdminLocations(loc.MarineConditions.Locations)
	return loc
}

func normalizeCoverageRegions(regions []adminFeedCoverageRegionXML) []adminFeedCoverageRegionXML {
	out := make([]adminFeedCoverageRegionXML, 0, len(regions))
	for _, region := range regions {
		region.ID = strings.TrimSpace(region.ID)
		if region.ID == "" {
			continue
		}
		region.Source = fallbackText(region.Source, "eccc")
		region.Name = strings.TrimSpace(region.Name)
		region.DeriveForecast = strings.TrimSpace(region.DeriveForecast)
		region.CanonicalID = strings.TrimSpace(region.CanonicalID)
		region.ForecastCanonicalID = strings.TrimSpace(region.ForecastCanonicalID)
		for index := range region.Subregions {
			region.Subregions[index].ID = strings.TrimSpace(region.Subregions[index].ID)
			region.Subregions[index].Source = strings.TrimSpace(region.Subregions[index].Source)
			region.Subregions[index].Name = strings.TrimSpace(region.Subregions[index].Name)
			region.Subregions[index].CanonicalID = strings.TrimSpace(region.Subregions[index].CanonicalID)
		}
		out = append(out, region)
	}
	return out
}

func normalizeAdminLocations(items []adminFeedLocationXML) []adminFeedLocationXML {
	out := make([]adminFeedLocationXML, 0, len(items))
	for _, item := range items {
		item.ID = strings.TrimSpace(item.ID)
		if item.ID == "" {
			continue
		}
		item.Source = fallbackText(item.Source, "eccc")
		item.NameOverride = strings.TrimSpace(item.NameOverride)
		item.NormalID = strings.TrimSpace(item.NormalID)
		item.Latitude = strings.TrimSpace(item.Latitude)
		item.Longitude = strings.TrimSpace(item.Longitude)
		item.CanonicalID = strings.TrimSpace(item.CanonicalID)
		out = append(out, item)
	}
	return out
}

func normalizeAdminTransmitters(items []adminFeedTransmitterXML, feedID string) []adminFeedTransmitterXML {
	out := make([]adminFeedTransmitterXML, 0, len(items))
	for _, item := range items {
		item.SiteName = fallbackText(item.SiteName, feedID)
		item.Callsign = strings.ToUpper(strings.TrimSpace(item.Callsign))
		item.Relationship = fallbackText(item.Relationship, "primary")
		item.HostName = strings.TrimSpace(item.HostName)
		item.FrequencyMHz.Value = strings.TrimSpace(item.FrequencyMHz.Value)
		out = append(out, item)
	}
	if len(out) == 0 {
		out = append(out, adminFeedTransmitterXML{SiteName: feedID, Relationship: "primary"})
	}
	return out
}

func adminFeedFromMap(raw any) (adminFeedXML, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return adminFeedXML{}, fmt.Errorf("feed must be an object")
	}
	feed := adminFeedXML{
		ID:        stringPayload(m, "id", ""),
		Enabled:   boolText(boolPayload(m, "enabled", true)),
		Timezone:  stringPayload(m, "timezone", "Local"),
		Languages: adminFeedLanguagesXML{Langs: langListFromText(stringPayload(m, "languages", "en-CA"))},
		Desc: adminFeedDescriptionXML{Langs: []adminFeedDescriptionLangXML{{
			Code:   stringPayload(m, "description_lang", "en-CA"),
			Text:   stringPayload(m, "description_text", ""),
			Suffix: stringPayload(m, "description_suffix", ""),
		}}},
	}
	feed.Playout = adminFeedPlayoutXML{
		Routine:           boolText(boolPayload(m, "routine", true)),
		SAME:              boolText(boolPayload(m, "same", true)),
		SAMEOriginator:    stringPayload(m, "same_originator", "EAS"),
		SAMEAttentionTone: stringPayload(m, "same_attention_tone", ""),
	}
	feed.Alerts.CapCP = adminAlertSourceFromMap(m, "cap_cp")
	feed.Alerts.NWSCAP = adminAlertSourceFromMap(m, "nws_cap")
	feed.Locations.Coverage.Regions = coverageRegionsFromText(stringPayload(m, "coverage_regions", ""))
	feed.Locations.ObservationLocations.Locations = locationsFromText(stringPayload(m, "observation_locations", ""))
	feed.Locations.AviationReportLocations.Locations = locationsFromText(stringPayload(m, "aviation_report_locations", ""))
	feed.Locations.AirQualityLocations.Locations = locationsFromText(stringPayload(m, "air_quality_locations", ""))
	feed.Locations.ClimateLocations.Locations = locationsFromText(stringPayload(m, "climate_locations", ""))
	feed.Locations.HydrometricLocations.Locations = locationsFromText(stringPayload(m, "hydrometric_locations", ""))
	feed.Locations.HydrometricLocations.Upstream.Locations = locationsFromText(stringPayload(m, "hydrometric_upstream_locations", ""))
	feed.Locations.HydrometricLocations.Downstream.Locations = locationsFromText(stringPayload(m, "hydrometric_downstream_locations", ""))
	feed.Locations.MarineForecastLocations.Locations = locationsFromText(stringPayload(m, "marine_forecast_locations", ""))
	feed.Locations.MarineForecastLocations.Subregions = locationsFromText(stringPayload(m, "marine_forecast_subregions", ""))
	feed.Locations.MarineConditions.Locations = locationsFromText(stringPayload(m, "marine_condition_locations", ""))
	feed.Tx.Transmitters = []adminFeedTransmitterXML{{
		SiteName:     stringPayload(m, "site_name", feed.ID),
		Callsign:     stringPayload(m, "callsign", ""),
		Relationship: stringPayload(m, "relationship", "primary"),
		FrequencyMHz: adminFeedFrequencyXML{Value: stringPayload(m, "frequency_mhz", "")},
	}}
	return feed, nil
}

func adminAlertSourceFromMap(m map[string]any, prefix string) adminFeedAlertSourceXML {
	source := adminFeedAlertSourceXML{
		Enabled: boolText(boolPayload(m, prefix+"_enabled", prefix == "cap_cp")),
		Filter: adminFeedFilterXML{
			UseFeedLocations: boolText(boolPayload(m, prefix+"_use_feed_locations", true)),
			Allowlist:        ruleListFromText(stringPayload(m, prefix+"_allowlist", "")),
			Blocklist:        ruleListFromText(stringPayload(m, prefix+"_blocklist", "")),
		},
	}
	return source
}

func langListFromText(text string) []adminFeedLangXML {
	out := []adminFeedLangXML{}
	for _, part := range splitControlLines(text) {
		chunks := strings.SplitN(part, ":", 2)
		lang := adminFeedLangXML{Code: chunks[0], Interval: "0"}
		if len(chunks) == 2 {
			lang.Interval = strings.TrimSpace(chunks[1])
		}
		out = append(out, lang)
	}
	return out
}

func coverageRegionsFromText(text string) []adminFeedCoverageRegionXML {
	out := []adminFeedCoverageRegionXML{}
	for _, line := range splitControlLines(text) {
		parts := strings.Split(line, "|")
		region := adminFeedCoverageRegionXML{ID: strings.TrimSpace(parts[0]), Source: "eccc"}
		if len(parts) > 1 {
			region.Source = strings.TrimSpace(parts[1])
		}
		if len(parts) > 2 {
			region.DeriveForecast = strings.TrimSpace(parts[2])
		}
		if len(parts) > 3 {
			region.Name = strings.TrimSpace(parts[3])
		}
		if len(parts) > 4 {
			region.CanonicalID = strings.TrimSpace(parts[4])
		}
		if len(parts) > 5 {
			region.ForecastCanonicalID = strings.TrimSpace(parts[5])
		}
		if len(parts) > 6 {
			for _, rawSubregion := range strings.Split(parts[6], "+") {
				if id := strings.TrimSpace(rawSubregion); id != "" {
					region.Subregions = append(region.Subregions, adminFeedSubregionXML{ID: id})
				}
			}
		}
		out = append(out, region)
	}
	return out
}

func locationsFromText(text string) []adminFeedLocationXML {
	out := []adminFeedLocationXML{}
	for _, line := range splitControlLines(text) {
		parts := strings.Split(line, "|")
		item := adminFeedLocationXML{ID: strings.TrimSpace(parts[0]), Source: "eccc"}
		if len(parts) > 1 {
			item.Source = strings.TrimSpace(parts[1])
		}
		if len(parts) > 2 {
			item.NameOverride = strings.TrimSpace(parts[2])
		}
		if len(parts) > 3 {
			item.NormalID = strings.TrimSpace(parts[3])
		}
		if len(parts) > 4 {
			item.Latitude = strings.TrimSpace(parts[4])
		}
		if len(parts) > 5 {
			item.Longitude = strings.TrimSpace(parts[5])
		}
		if len(parts) > 6 {
			item.CanonicalID = strings.TrimSpace(parts[6])
		}
		out = append(out, item)
	}
	return out
}

func ruleListFromText(text string) adminFeedRuleListXML {
	out := adminFeedRuleListXML{}
	for _, line := range splitControlLines(text) {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			key, value = "severity", line
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		switch key {
		case "severity":
			out.Severities = append(out.Severities, value)
		case "urgency":
			out.Urgencies = append(out.Urgencies, value)
		case "certainty":
			out.Certainties = append(out.Certainties, value)
		case "naads_event", "event":
			out.NAADSEvents = append(out.NAADSEvents, value)
		default:
			out.Other = append(out.Other, adminFeedOtherRule{ValueName: key, Value: value})
		}
	}
	return out
}

func splitControlLines(text string) []string {
	parts := strings.FieldsFunc(text, func(r rune) bool { return r == '\n' || r == ',' || r == ';' })
	out := []string{}
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func cleanFeedControlID(value string) string {
	cleaned := feedIDCleaner.ReplaceAllString(strings.TrimSpace(value), "-")
	return strings.Trim(cleaned, "-")
}

func feedsControlPayload(path string, rel string, feeds []adminFeedXML, playlist map[string]any) map[string]any {
	out := make([]map[string]any, 0, len(feeds))
	for _, feed := range feeds {
		out = append(out, adminFeedPayload(feed, playlistFeedState(playlist, feed.ID)))
	}
	return map[string]any{
		"path":       filepath.ToSlash(path),
		"configured": rel,
		"feeds":      out,
		"playlist":   playlist,
	}
}

func adminFeedPayload(feed adminFeedXML, runtime map[string]any) map[string]any {
	tx := adminFeedTransmitterXML{}
	if len(feed.Tx.Transmitters) > 0 {
		tx = feed.Tx.Transmitters[0]
	}
	desc := adminFeedDescriptionLangXML{}
	if len(feed.Desc.Langs) > 0 {
		desc = feed.Desc.Langs[0]
	}
	return map[string]any{
		"id":                               feed.ID,
		"enabled":                          xmlBool(feed.Enabled, true),
		"timezone":                         feed.Timezone,
		"routine":                          xmlBool(feed.Playout.Routine, true),
		"same":                             xmlBool(feed.Playout.SAME, true),
		"same_originator":                  feed.Playout.SAMEOriginator,
		"same_attention_tone":              feed.Playout.SAMEAttentionTone,
		"cap_cp_enabled":                   xmlBool(feed.Alerts.CapCP.Enabled, true),
		"cap_cp_use_feed_locations":        xmlBool(feed.Alerts.CapCP.Filter.UseFeedLocations, true),
		"cap_cp_allowlist":                 ruleListText(feed.Alerts.CapCP.Filter.Allowlist),
		"cap_cp_blocklist":                 ruleListText(feed.Alerts.CapCP.Filter.Blocklist),
		"nws_cap_enabled":                  xmlBool(feed.Alerts.NWSCAP.Enabled, false),
		"nws_cap_use_feed_locations":       xmlBool(feed.Alerts.NWSCAP.Filter.UseFeedLocations, true),
		"nws_cap_allowlist":                ruleListText(feed.Alerts.NWSCAP.Filter.Allowlist),
		"nws_cap_blocklist":                ruleListText(feed.Alerts.NWSCAP.Filter.Blocklist),
		"languages":                        langListText(feed.Languages.Langs),
		"description_lang":                 fallbackText(desc.Code, "en-CA"),
		"description_text":                 desc.Text,
		"description_suffix":               desc.Suffix,
		"coverage_regions":                 coverageRegionsText(feed.Locations.Coverage.Regions),
		"observation_locations":            locationsText(feed.Locations.ObservationLocations.Locations),
		"aviation_report_locations":        locationsText(feed.Locations.AviationReportLocations.Locations),
		"air_quality_locations":            locationsText(feed.Locations.AirQualityLocations.Locations),
		"climate_locations":                locationsText(feed.Locations.ClimateLocations.Locations),
		"hydrometric_locations":            locationsText(feed.Locations.HydrometricLocations.Locations),
		"hydrometric_upstream_locations":   locationsText(feed.Locations.HydrometricLocations.Upstream.Locations),
		"hydrometric_downstream_locations": locationsText(feed.Locations.HydrometricLocations.Downstream.Locations),
		"marine_forecast_locations":        locationsText(feed.Locations.MarineForecastLocations.Locations),
		"marine_forecast_subregions":       locationsText(feed.Locations.MarineForecastLocations.Subregions),
		"marine_condition_locations":       locationsText(feed.Locations.MarineConditions.Locations),
		"site_name":                        tx.SiteName,
		"callsign":                         tx.Callsign,
		"relationship":                     tx.Relationship,
		"frequency_mhz":                    tx.FrequencyMHz.Value,
		"runtime":                          runtime,
	}
}

func langListText(langs []adminFeedLangXML) string {
	lines := []string{}
	for _, lang := range langs {
		lines = append(lines, strings.TrimSpace(lang.Code)+":"+fallbackText(lang.Interval, "0"))
	}
	return strings.Join(lines, "\n")
}

func coverageRegionsText(regions []adminFeedCoverageRegionXML) string {
	lines := []string{}
	for _, region := range regions {
		subregions := make([]string, 0, len(region.Subregions))
		for _, subregion := range region.Subregions {
			subregions = append(subregions, subregion.ID)
		}
		lines = append(lines, strings.Join([]string{
			region.ID,
			region.Source,
			region.DeriveForecast,
			region.Name,
			region.CanonicalID,
			region.ForecastCanonicalID,
			strings.Join(subregions, "+"),
		}, "|"))
	}
	return strings.Join(lines, "\n")
}

func locationsText(items []adminFeedLocationXML) string {
	lines := []string{}
	for _, item := range items {
		lines = append(lines, strings.Join([]string{
			item.ID,
			item.Source,
			item.NameOverride,
			item.NormalID,
			item.Latitude,
			item.Longitude,
			item.CanonicalID,
		}, "|"))
	}
	return strings.Join(lines, "\n")
}

func ruleListText(list adminFeedRuleListXML) string {
	lines := []string{}
	for _, value := range list.Severities {
		lines = append(lines, "severity:"+value)
	}
	for _, value := range list.Urgencies {
		lines = append(lines, "urgency:"+value)
	}
	for _, value := range list.Certainties {
		lines = append(lines, "certainty:"+value)
	}
	for _, value := range list.NAADSEvents {
		lines = append(lines, "naads_event:"+value)
	}
	for _, value := range list.Other {
		if strings.TrimSpace(value.ValueName) != "" && strings.TrimSpace(value.Value) != "" {
			lines = append(lines, value.ValueName+":"+value.Value)
		}
	}
	return strings.Join(lines, "\n")
}
