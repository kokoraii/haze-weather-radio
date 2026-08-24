package playlist

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
	"gopkg.in/yaml.v3"
)

type Options struct {
	ConfigPath string
	BridgeAddr string
	Tick       time.Duration
	Lookahead  time.Duration
	OutDir     string
}

type rootConfig struct {
	Version   string                  `yaml:"version"`
	FeedsFile string                  `yaml:"feeds_file"`
	Storage   datastore.StorageConfig `yaml:"storage"`
	Operator  struct {
		OnAirName any `yaml:"on_air_name"`
	} `yaml:"operator"`
	Services struct {
		Go struct {
			Playlist playlistConfig `yaml:"playlist"`
		} `yaml:"go"`
	} `yaml:"services"`
	Playout  playoutConfig        `yaml:"playout"`
	Daemon   daemonConfig         `yaml:"daemon"`
}

type playlistConfig struct {
	Enabled          bool   `yaml:"enabled"`
	Tick             string `yaml:"tick"`
	Lookahead        string `yaml:"lookahead"`
	MaxQueued        int    `yaml:"max_queued"`
	OutputDir        string `yaml:"out_dir"`
	FixedToleranceS  int    `yaml:"fixed_tolerance_s"`
	RoutineEstimateS int    `yaml:"routine_estimate_s"`
}

type playoutConfig struct {
	SampleRate    int      `yaml:"sample_rate"`
	Channels      int      `yaml:"channels"`
	PlaylistOrder []string `yaml:"playlist_order"`
	Pacing        struct {
		PackageGapS float64 `yaml:"package_gap_s"`
	} `yaml:"pacing"`
	StationIDSchedule minuteScheduleConfig `yaml:"station_id_schedule"`
	DateTimeSchedule  minuteScheduleConfig `yaml:"date_time_schedule"`
	Chimes            struct {
		Enabled  bool `yaml:"enabled"`
		HalfHour struct {
			Enabled bool `yaml:"enabled"`
		} `yaml:"half_hour"`
		TopOfHour struct {
			Enabled bool `yaml:"enabled"`
		} `yaml:"top_of_hour"`
	} `yaml:"chimes"`
}

type minuteScheduleConfig struct {
	Enabled *bool      `yaml:"enabled"`
	Minutes minuteList `yaml:"minutes"`
}

type minuteList []int

type daemonConfig struct {
	AlertUpdateTonePolicy string `yaml:"alert_update_tone_policy"`
}

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
	Languages struct {
		Langs []feedLangXML `xml:"lang"`
	} `xml:"languages"`
	Locations struct {
		Coverage struct {
			Regions []coverageRegionXML `xml:"region"`
		} `xml:"coverage"`
	} `xml:"locations"`
	Transmitter struct {
		Transmitters []transmitterXML `xml:"transmitter"`
	} `xml:"transmitter_metadata"`
}

type coverageRegionXML struct {
	ID         string `xml:"id,attr"`
	Subregions []struct {
		ID string `xml:"id,attr"`
	} `xml:"subregion"`
}

type feedLangXML struct {
	Code     string `xml:"code,attr"`
	Interval string `xml:"interval,attr"`
}

type transmitterXML struct {
	Network      transmitterNetworkXML   `xml:"network"`
	HostName     string                  `xml:"host_name"`
	SiteName     string                  `xml:"site_name"`
	Callsign     string                  `xml:"callsign"`
	Relationship string                  `xml:"relationship"`
	FrequencyMHz transmitterFrequencyXML `xml:"frequency_mhz"`
}

type transmitterNetworkXML struct {
	Name           string `xml:"name"`
	Pronounciation string `xml:"pronounciation"`
	Pronunciation  string `xml:"pronunciation"`
}

type transmitterFrequencyXML struct {
	GPCLK string `xml:"gpclk,attr"`
	Value string `xml:",chardata"`
}

type loadedConfig struct {
	Root      rootConfig
	Feeds     []feedXML
	BaseDir   string
	OutputDir string
	Store     datastore.Store
	Daemon    struct {
		AlertUpdateTonePolicy string `yaml:"alert_update_tone_policy"`
	}
}

func loadConfig(configPath string, outDir string) (loadedConfig, error) {
	raw, err := os.ReadFile(filepath.Clean(configPath))
	if err != nil {
		return loadedConfig{}, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var root rootConfig
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return loadedConfig{}, err
	}
	if root.Playout.SampleRate <= 0 {
		root.Playout.SampleRate = 48000
	}
	if root.Playout.Channels <= 0 {
		root.Playout.Channels = 1
	}
	if root.Services.Go.Playlist.MaxQueued <= 0 {
		root.Services.Go.Playlist.MaxQueued = 3
	}
	if root.Services.Go.Playlist.FixedToleranceS <= 0 {
		root.Services.Go.Playlist.FixedToleranceS = 4
	}
	if root.Services.Go.Playlist.RoutineEstimateS <= 0 {
		root.Services.Go.Playlist.RoutineEstimateS = 35
	}
	if root.Daemon.AlertUpdateTonePolicy == "" {
		root.Daemon.AlertUpdateTonePolicy = "new_locations"
	}
	if root.Playout.Pacing.PackageGapS <= 0 {
		root.Playout.Pacing.PackageGapS = 1
	}
	if len(root.Playout.PlaylistOrder) == 0 {
		root.Playout.PlaylistOrder = []string{"current_conditions", "aviation_reports", "marine_reports", "marine_forecast", "air_quality", "forecast", "climate_summary", "thunderstorm_outlook", "hydrometric", "geophysical_alert", "user_bulletin"}
	}
	baseDir := filepath.Dir(filepath.Clean(configPath))
	feeds, err := loadFeeds(resolvePath(baseDir, fallbackText(root.FeedsFile, "managed/configs/feeds.xml")))
	if err != nil {
		return loadedConfig{}, err
	}
	outputDir := fallbackText(outDir, root.Services.Go.Playlist.OutputDir)
	if outputDir == "" {
		outputDir = filepath.Join("runtime", "audio", "playlist")
	}
	return loadedConfig{
		Root:      root,
		Feeds:     feeds,
		BaseDir:   baseDir,
		OutputDir: resolvePath(baseDir, outputDir),
	}, nil
}

func loadFeeds(path string) ([]feedXML, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var parsed feedsXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse feeds XML: %w", err)
	}
	return parsed.Feeds, nil
}

func (cfg loadedConfig) enabledFeeds() []feedXML {
	feeds := make([]feedXML, 0, len(cfg.Feeds))
	for _, feed := range cfg.Feeds {
		if strings.TrimSpace(feed.ID) == "" || !xmlBool(feed.EnabledRaw, true) {
			continue
		}
		if !feedRoutineEnabled(feed) && !feedSAMEEnabled(feed) {
			continue
		}
		feeds = append(feeds, feed)
	}
	return feeds
}

func feedRoutineEnabled(feed feedXML) bool {
	return xmlBool(feed.Playout.Routine, true)
}

func feedSAMEEnabled(feed feedXML) bool {
	return xmlBool(feed.Playout.SAME, true)
}

func (m *minuteList) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.SequenceNode:
		out := make([]int, 0, len(value.Content))
		for _, child := range value.Content {
			parsed, err := strconv.Atoi(strings.TrimSpace(child.Value))
			if err == nil && parsed >= 0 && parsed <= 59 {
				out = append(out, parsed)
			}
		}
		*m = out
	case yaml.ScalarNode:
		if value.Value == "" {
			*m = nil
			return nil
		}
		parts := strings.Split(value.Value, ",")
		out := make([]int, 0, len(parts))
		for _, part := range parts {
			parsed, err := strconv.Atoi(strings.TrimSpace(part))
			if err == nil && parsed >= 0 && parsed <= 59 {
				out = append(out, parsed)
			}
		}
		*m = out
	}
	return nil
}

func (s minuteScheduleConfig) enabled(fallback bool) bool {
	if s.Enabled == nil {
		return fallback
	}
	return *s.Enabled
}

func (s minuteScheduleConfig) minutesOr(defaults []int) []int {
	if len(s.Minutes) == 0 {
		return defaults
	}
	return []int(s.Minutes)
}

func feedName(feed feedXML) string {
	name := strings.TrimSpace(stationTransmitter(feed).SiteName)
	if name == "" {
		name = strings.TrimSpace(feed.ID)
	}
	return name
}

func feedCallsign(feed feedXML) string {
	return strings.TrimSpace(stationTransmitter(feed).Callsign)
}

func feedFrequencyMHz(feed feedXML) string {
	return stationTransmitter(feed).frequencyText()
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

func replacementTransmitter(feed feedXML) (transmitterXML, bool) {
	for _, transmitter := range transmitterList(feed) {
		if transmitter.isRelationship("replaces") {
			return transmitter, true
		}
	}
	return transmitterXML{}, false
}

func feedLanguage(feed feedXML) string {
	languages := feedLanguages(feed)
	if len(languages) > 0 {
		return languages[0].Code
	}
	return "en-US"
}

func feedLanguages(feed feedXML) []feedLangXML {
	out := make([]feedLangXML, 0, len(feed.Languages.Langs))
	seen := map[string]struct{}{}
	for _, lang := range feed.Languages.Langs {
		code := strings.TrimSpace(lang.Code)
		if code == "" {
			continue
		}
		key := normalizeLangKey(code)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, feedLangXML{Code: code, Interval: strings.TrimSpace(lang.Interval)})
	}
	return out
}

func normalizeLangKey(lang string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(lang), "_", "-"))
}

func feedLocation(feed feedXML) *time.Location {
	if loc, err := time.LoadLocation(fallbackText(feed.Timezone, "Local")); err == nil {
		return loc
	}
	return time.Local
}

func feedTimezone(feed feedXML) string {
	return fallbackText(feed.Timezone, "Local")
}

func xmlBool(raw string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return fallback
	case "1", "true", "yes", "on", "enabled":
		return true
	case "0", "false", "no", "off", "disabled":
		return false
	default:
		return fallback
	}
}

func resolvePath(base string, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return filepath.Clean(base)
	}
	if filepath.IsAbs(value) {
		return filepath.Clean(value)
	}
	return filepath.Clean(filepath.Join(base, value))
}

func fallbackText(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return strings.TrimSpace(fallback)
	}
	return value
}

func displayText(value any) string {
	return spokenText(value)
}

func spokenText(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []any:
		merged := map[string]any{}
		for _, item := range typed {
			if child, ok := item.(map[string]any); ok {
				for key, value := range child {
					merged[key] = value
				}
			}
		}
		for _, key := range []string{"pronunciation", "pronounciation", "text", "name"} {
			if text := spokenText(merged[key]); text != "" {
				return text
			}
		}
	case map[string]any:
		for _, key := range []string{"pronunciation", "pronounciation", "text", "name"} {
			if text := spokenText(typed[key]); text != "" {
				return text
			}
		}
	case nil:
		return ""
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
	return ""
}

func spokenNetworkName(network transmitterNetworkXML) string {
	for _, value := range []string{network.Pronunciation, network.Pronounciation, network.Name} {
		if text := strings.TrimSpace(value); text != "" {
			return text
		}
	}
	return ""
}

func dateTimeAnnouncement(now time.Time, lang string) string {
	short := strings.ToLower(strings.TrimSpace(lang))
	if idx := strings.Index(short, "-"); idx > 0 {
		short = short[:idx]
	}
	prefix := timeGreeting(now, short)
	timeText := spokenClockTime(now)
	switch short {
	case "fr":
		return strings.TrimSpace(prefix + " Il est actuellement " + timeText + ".")
	case "es":
		return strings.TrimSpace(prefix + " La hora actual es " + timeText + ".")
	default:
		return strings.TrimSpace(prefix + " The current time is " + timeText + ".")
	}
}

func timeGreeting(now time.Time, lang string) string {
	hour := now.Hour()
	period := "night"
	switch {
	case hour >= 5 && hour < 12:
		period = "morning"
	case hour >= 12 && hour < 17:
		period = "afternoon"
	case hour >= 17 && hour < 22:
		period = "evening"
	}
	switch lang {
	case "fr":
		switch period {
		case "morning":
			return "Bonjour."
		case "afternoon":
			return "Bon après-midi."
		case "evening":
			return "Bonsoir."
		default:
			return "Bonne nuit."
		}
	case "es":
		switch period {
		case "morning":
			return "Buenos días."
		case "afternoon":
			return "Buenas tardes."
		default:
			return "Buenas noches."
		}
	default:
		switch period {
		case "morning":
			return "Good morning."
		case "afternoon":
			return "Good afternoon."
		case "evening":
			return "Good evening."
		default:
			return "Good night."
		}
	}
}

func spokenClockTime(now time.Time) string {
	hour := numberToWords(now.Hour()%12, true)
	if now.Hour()%12 == 0 {
		hour = "twelve"
	}
	minute := now.Minute()
	ampm := "A.M."
	if now.Hour() >= 12 {
		ampm = "P.M."
	}
	tz := timezoneName(now.Format("MST"))
	if minute == 0 {
		return strings.TrimSpace(fmt.Sprintf("%s %s, %s", hour, ampm, tz))
	}
	minuteText := numberToWords(minute, false)
	if minute < 10 {
		minuteText = "oh " + minuteText
	}
	return strings.TrimSpace(fmt.Sprintf("%s %s %s, %s", hour, minuteText, ampm, tz))
}

func numberToWords(value int, hour bool) string {
	if hour && value == 0 {
		value = 12
	}
	ones := []string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"}
	if value >= 0 && value < len(ones) {
		return ones[value]
	}
	tens := []string{"", "", "twenty", "thirty", "forty", "fifty"}
	if value >= 20 && value < 60 {
		ten := value / 10
		one := value % 10
		if one == 0 {
			return tens[ten]
		}
		return strings.TrimSpace(tens[ten] + " " + ones[one])
	}
	return fmt.Sprintf("%d", value)
}

func timezoneName(abbrev string) string {
	switch abbrev {
	case "CST":
		return "Central Standard Time"
	case "CDT":
		return "Central Daylight Time"
	case "MST":
		return "Mountain Standard Time"
	case "MDT":
		return "Mountain Daylight Time"
	case "EST":
		return "Eastern Standard Time"
	case "EDT":
		return "Eastern Daylight Time"
	case "PST":
		return "Pacific Standard Time"
	case "PDT":
		return "Pacific Daylight Time"
	case "UTC":
		return "Coordinated Universal Time"
	default:
		return strings.TrimSpace(abbrev)
	}
}

func spokenCallsign(callsign string) string {
	callsign = strings.TrimSpace(callsign)
	if callsign == "" {
		return ""
	}
	parts := make([]string, 0, len(callsign))
	for _, ch := range callsign {
		if ch == '-' || ch == ' ' || ch == '_' {
			continue
		}
		parts = append(parts, strings.ToUpper(string(ch)))
	}
	return strings.Join(parts, " ")
}
