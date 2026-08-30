package ivr

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var requiredMenus = []string{
	"entry",
	"language_select",
	"location_code",
	"location_number",
	"location_search",
	"location_menu",
	"weather_product",
	"broadcast_menu",
	"geophysical_alert",
	"operator",
	"error",
}

var requiredMenuLines = map[string][]string{
	"entry":             {"main", "main_single_language"},
	"location_code":     {"main"},
	"location_number":   {"main", "search_unavailable"},
	"location_search":   {"main", "voice_unavailable", "no_match", "more_detail", "fallback_numeric"},
	"location_menu":     {"main"},
	"weather_product":   {"unavailable"},
	"broadcast_menu":    {"main"},
	"geophysical_alert": {"main"},
	"operator":          {"main"},
	"error":             {"invalid_code", "timeout"},
}

type promptConfigXML struct {
	XMLName  xml.Name       `xml:"ivr"`
	Defaults promptDefaults `xml:"defaults"`
	Menus    []promptMenu   `xml:"menu"`
}

type promptDefaults struct {
	TTSProfile
	Lines []promptLine `xml:"line"`
}

type promptMenu struct {
	ID          string        `xml:"id,attr"`
	TTSProfile                // menu-level TTS overrides
	TimeoutRaw  string        `xml:"timeout,attr"`
	Timeout     time.Duration `xml:"-"`
	RetriesRaw  string        `xml:"retries,attr"`
	Retries     int           `xml:"-"`
	TransferURI string        `xml:"transfer_uri,attr"`
	Lines       []promptLine  `xml:"line"`
	Options     []menuOption  `xml:"option"`
}

type promptLine struct {
	Key      string `xml:"key,attr"`
	Language string `xml:"language,attr"`
	Text     string `xml:",chardata"`
}

type menuOption struct {
	Digit    string `xml:"digit,attr"`
	Action   string `xml:"action,attr"`
	Next     string `xml:"next,attr"`
	Language string `xml:"language,attr"`
	Packages string `xml:"packages,attr"`
}

// TTSProfile describes menu-specific synthesis overrides.
type TTSProfile struct {
	ReaderID           string        `xml:"reader_id,attr"`
	Provider           string        `xml:"provider,attr"`
	VoiceID            string        `xml:"voice_id,attr"`
	Language           string        `xml:"language,attr"`
	ExplicitLanguage   bool          `xml:"-"`
	RateRaw            string        `xml:"rate,attr"`
	Rate               int           `xml:"-"`
	VolumeRaw          string        `xml:"volume,attr"`
	Volume             int           `xml:"-"`
	SentenceSilenceRaw string        `xml:"sentence_silence,attr"`
	SentenceSilence    float64       `xml:"-"`
	CacheTTLRaw        string        `xml:"cache_ttl,attr"`
	CacheTTL           time.Duration `xml:"-"`
	Priority           string        `xml:"-"`
}

type PromptConfig struct {
	Defaults       TTSProfile
	Lines          map[string]string
	LocalizedLines map[string][]promptLine
	Menus          map[string]promptMenu
}

type staticPromptLine struct {
	MenuID  string
	LineKey string
	Values  map[string]string
}

func loadPromptConfig(path string) (PromptConfig, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return defaultPromptConfig(), nil
		}
		return PromptConfig{}, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var parsed promptConfigXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return PromptConfig{}, fmt.Errorf("parse IVR XML: %w", err)
	}
	return normalizePromptConfig(parsed)
}

func normalizePromptConfig(parsed promptConfigXML) (PromptConfig, error) {
	defaults := normalizeTTSProfile(parsed.Defaults.TTSProfile, TTSProfile{
		Provider: "fast",
		Language: "en-US",
		Volume:   100,
		CacheTTL: 24 * time.Hour,
	})
	cfg := PromptConfig{
		Defaults:       defaults,
		Lines:          map[string]string{},
		LocalizedLines: map[string][]promptLine{},
		Menus:          map[string]promptMenu{},
	}
	for _, line := range parsed.Defaults.Lines {
		if key := strings.ToLower(strings.TrimSpace(line.Key)); key != "" {
			line.Key = key
			line.Language = normalizePromptLanguage(line.Language)
			line.Text = cleanPromptText(line.Text)
			cfg.LocalizedLines[key] = append(cfg.LocalizedLines[key], line)
			if line.Language == "" {
				cfg.Lines[key] = line.Text
			}
		}
	}
	for _, menu := range parsed.Menus {
		menu.ID = strings.ToLower(strings.TrimSpace(menu.ID))
		if menu.ID == "" {
			continue
		}
		menu.TTSProfile = normalizeTTSProfile(menu.TTSProfile, defaults)
		menu.Timeout = parseDuration(menu.TimeoutRaw, 8*time.Second)
		menu.Retries = parseInt(menu.RetriesRaw, 2)
		for index := range menu.Lines {
			menu.Lines[index].Key = strings.ToLower(strings.TrimSpace(menu.Lines[index].Key))
			menu.Lines[index].Language = normalizePromptLanguage(menu.Lines[index].Language)
			menu.Lines[index].Text = cleanPromptText(menu.Lines[index].Text)
		}
		for index := range menu.Options {
			menu.Options[index].Digit = strings.TrimSpace(menu.Options[index].Digit)
			menu.Options[index].Action = strings.ToLower(strings.TrimSpace(menu.Options[index].Action))
			menu.Options[index].Next = strings.ToLower(strings.TrimSpace(menu.Options[index].Next))
			menu.Options[index].Language = strings.TrimSpace(menu.Options[index].Language)
			menu.Options[index].Packages = strings.TrimSpace(menu.Options[index].Packages)
		}
		cfg.Menus[menu.ID] = menu
	}
	for _, id := range requiredMenus {
		if _, ok := cfg.Menus[id]; !ok {
			return PromptConfig{}, fmt.Errorf("IVR XML missing required menu %q", id)
		}
	}
	for menuID, lines := range requiredMenuLines {
		for _, lineKey := range lines {
			if cfg.MenuLine(menuID, lineKey, nil) == "" {
				return PromptConfig{}, fmt.Errorf("IVR XML missing required line %q in menu %q", lineKey, menuID)
			}
		}
	}
	for menuID, menu := range cfg.Menus {
		for _, option := range menu.Options {
			if option.Action != "menu" {
				continue
			}
			if strings.TrimSpace(option.Next) == "" {
				return PromptConfig{}, fmt.Errorf("IVR XML menu %q option %q has menu action without next menu", menuID, option.Digit)
			}
			if _, ok := cfg.Menus[option.Next]; !ok {
				return PromptConfig{}, fmt.Errorf("IVR XML menu %q option %q references missing menu %q", menuID, option.Digit, option.Next)
			}
		}
	}
	return cfg, nil
}

func defaultPromptConfig() PromptConfig {
	raw := promptConfigXML{
		Defaults: promptDefaults{
			TTSProfile: TTSProfile{Provider: "fast", Language: "en-US", VolumeRaw: "100", CacheTTLRaw: "24h"},
			Lines: []promptLine{
				{Key: "one_moment", Text: "One moment."},
				{Key: "enter_code", Text: "Enter your province, or enter a former Hello Weather location code."},
				{Key: "no_entry", Text: "No entry was received. Goodbye."},
				{Key: "goodbye", Text: "Goodbye."},
			},
		},
		Menus: []promptMenu{
			{ID: "entry", Lines: []promptLine{
				{Key: "main", Text: "Enter your province, or enter a former Hello Weather location code, or press star to search for a location. For your NOAA Geophysical Alert Message, press 0."},
				{Key: "main_single_language", Text: "Enter your province, or enter a former Hello Weather location code, or press star to search for a location. For your NOAA Geophysical Alert Message, press 0."},
			}, Options: []menuOption{
				{Digit: "0", Action: "product", Packages: "geophysical_alert"},
				{Digit: "*", Action: "search"},
			}},
			{ID: "language_select", Lines: []promptLine{
				{Key: "en-us", Text: "1 for English."},
				{Key: "fr-ca", Language: "fr-CA", Text: "deux pour le français."},
				{Key: "es", Language: "es", Text: "tres para español."},
			}, Options: []menuOption{
				{Digit: "1", Action: "language", Language: "en-US", Next: "entry"},
				{Digit: "2", Action: "language", Language: "fr-CA", Next: "entry"},
				{Digit: "3", Action: "language", Language: "es", Next: "entry"},
			}},
			{ID: "location_code", Lines: []promptLine{{Key: "main", Text: "Enter your province, or enter a former Hello Weather location code, or press star to search for a location. For your NOAA Geophysical Alert Message, press 0."}}},
			{ID: "location_number", Lines: []promptLine{
				{Key: "main", Text: "Enter your location number. Press star to search for a location."},
				{Key: "search_unavailable", Text: "Location search is not available yet."},
			}},
			{ID: "location_search", Lines: []promptLine{
				{Key: "main", Text: "Say a location name, or spell it with the keypad. Press pound when finished."},
				{Key: "voice_unavailable", Text: "Voice search is unavailable. Please use the keypad."},
				{Key: "no_match", Text: "No matching location was found. Try again."},
				{Key: "more_detail", Text: "Several locations match. Enter more of the name, or include a province or state."},
				{Key: "fallback_numeric", Text: "Returning to numeric location entry."},
			}},
			{ID: "location_menu", Lines: []promptLine{
				{Key: "main", Text: "You have reached {location}. 1 for regional observations, 2 for your 7 day outlook, 3 for air quality indices, 4 for the climate summary, 5 for the thunderstorm outlook, 6 for specialty products, or 0 to listen to a corresponding, 10 minute {radio_service_name} broadcast."},
				{Key: "main_no_broadcast", Text: "You have reached {location}. 1 for regional observations, 2 for your 7 day outlook, 3 for air quality indices, 4 for the climate summary, 5 for the thunderstorm outlook, or 6 for specialty products."},
				{Key: "main_alerts", Text: "{location} has {alert_count_phrase} in effect. Please press the asterisk key, or 1 for regional observations, 2 for your 7 day outlook, 3 for air quality indices, 4 for the climate summary, 5 for the thunderstorm outlook, 6 for specialty products, or 0 to listen to a corresponding, 10 minute {radio_service_name} broadcast."},
				{Key: "main_no_broadcast_alerts", Text: "{location} has {alert_count_phrase} in effect. Please press the asterisk key, or 1 for regional observations, 2 for your 7 day outlook, 3 for air quality indices, 4 for the climate summary, 5 for the thunderstorm outlook, or 6 for specialty products."},
			}, Options: []menuOption{
				{Digit: "1", Action: "product", Packages: "current_conditions"},
				{Digit: "2", Action: "product", Packages: "forecast"},
				{Digit: "3", Action: "product", Packages: "air_quality"},
				{Digit: "4", Action: "product", Packages: "climate_summary"},
				{Digit: "5", Action: "product", Packages: "thunderstorm_outlook"},
				{Digit: "6", Action: "menu", Next: "specialty_menu"},
				{Digit: "0", Action: "broadcast", Packages: "alerts,current_conditions,air_quality,forecast,geophysical_alert", Next: "broadcast_menu"},
			}},
			{ID: "specialty_menu", Lines: []promptLine{
				{Key: "main", Text: "Specialty products. 1 for river conditions. Press pound to return to the previous menu."},
			}, Options: []menuOption{
				{Digit: "1", Action: "product", Packages: "hydrometric"},
			}},
			{ID: "weather_product", Lines: []promptLine{{Key: "unavailable", Text: "Weather is unavailable for that code."}}},
			{ID: "broadcast_menu", Lines: []promptLine{{Key: "main", Text: "{radio_service_name} broadcast. Press pound to return to the previous menu."}}},
			{ID: "geophysical_alert", Lines: []promptLine{{Key: "main", Text: "NOAA Geophysical Alert Message."}}},
			{ID: "operator", Lines: []promptLine{{Key: "main", Text: "Operator transfer is not configured."}}},
			{ID: "error", Lines: []promptLine{{Key: "invalid_code", Text: "No match. Try again."}, {Key: "timeout", Text: "No entry. Goodbye."}}},
		},
	}
	cfg, _ := normalizePromptConfig(raw)
	return cfg
}

func normalizeTTSProfile(profile TTSProfile, fallback TTSProfile) TTSProfile {
	out := profile
	out.ExplicitLanguage = strings.TrimSpace(profile.Language) != ""
	if strings.TrimSpace(out.ReaderID) == "" {
		out.ReaderID = fallback.ReaderID
	}
	if strings.TrimSpace(out.Provider) == "" {
		out.Provider = fallback.Provider
	}
	if strings.TrimSpace(out.VoiceID) == "" {
		out.VoiceID = fallback.VoiceID
	}
	if strings.TrimSpace(out.Language) == "" {
		out.Language = fallback.Language
	}
	out.Rate = parseIntWithFallback(out.RateRaw, out.Rate, fallback.Rate)
	out.Volume = parseIntWithFallback(out.VolumeRaw, out.Volume, fallback.Volume)
	out.SentenceSilence = parseFloatWithFallback(out.SentenceSilenceRaw, out.SentenceSilence, fallback.SentenceSilence)
	out.CacheTTL = parseDuration(out.CacheTTLRaw, fallback.CacheTTL)
	if out.CacheTTL <= 0 {
		out.CacheTTL = 24 * time.Hour
	}
	return out
}

func (cfg PromptConfig) Menu(id string) (promptMenu, bool) {
	menu, ok := cfg.Menus[strings.ToLower(strings.TrimSpace(id))]
	return menu, ok
}

func (cfg PromptConfig) MenuLine(menuID string, key string, values map[string]string) string {
	language := firstNonBlank(values["language"], values["lang"])
	if language == "" {
		language = cfg.TTSForMenu(menuID).Language
	}
	if text, ok := cfg.LineForLanguage(menuID, key, language); ok {
		return renderPromptText(text, values)
	}
	if text, ok := selectPromptLine(cfg.LocalizedLines[strings.ToLower(strings.TrimSpace(key))], language); ok {
		return renderPromptText(text, values)
	}
	return ""
}

func (cfg PromptConfig) Line(menuID string, key string) (string, bool) {
	return cfg.LineForLanguage(menuID, key, "")
}

func (cfg PromptConfig) LineForLanguage(menuID string, key string, language string) (string, bool) {
	menu, ok := cfg.Menu(menuID)
	if !ok {
		return "", false
	}
	key = strings.ToLower(strings.TrimSpace(key))
	lines := make([]promptLine, 0, len(menu.Lines))
	for _, line := range menu.Lines {
		if line.Key == key {
			lines = append(lines, line)
		}
	}
	return selectPromptLine(lines, language)
}

func selectPromptLine(lines []promptLine, language string) (string, bool) {
	want := normalizePromptLanguage(language)
	if want != "" {
		for _, line := range lines {
			if line.Language == want && line.Text != "" {
				return line.Text, true
			}
		}
		primary := primaryPromptLanguage(want)
		for _, line := range lines {
			if primary != "" && primaryPromptLanguage(line.Language) == primary && line.Text != "" {
				return line.Text, true
			}
		}
	}
	for _, line := range lines {
		if line.Language == "" && line.Text != "" {
			return line.Text, true
		}
	}
	return "", false
}

func normalizePromptLanguage(language string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(language), "_", "-"))
}

func primaryPromptLanguage(language string) string {
	language = normalizePromptLanguage(language)
	if index := strings.IndexByte(language, '-'); index > 0 {
		return language[:index]
	}
	return language
}

func (cfg PromptConfig) Option(menuID string, digit string) (menuOption, bool) {
	menu, ok := cfg.Menu(menuID)
	if !ok {
		return menuOption{}, false
	}
	digit = strings.TrimSpace(digit)
	for _, option := range menu.Options {
		if option.Digit == digit {
			return option, true
		}
	}
	return menuOption{}, false
}

func (cfg PromptConfig) TTSForMenu(menuID string) TTSProfile {
	if menu, ok := cfg.Menu(menuID); ok {
		return menu.TTSProfile
	}
	return cfg.Defaults
}

func (cfg PromptConfig) StaticPromptLines() []staticPromptLine {
	seen := map[string]struct{}{}
	out := []staticPromptLine{}
	add := func(menuID string, lineKey string, text string) {
		menuID = strings.ToLower(strings.TrimSpace(menuID))
		lineKey = strings.ToLower(strings.TrimSpace(lineKey))
		if lineKey == "" || strings.TrimSpace(text) == "" {
			return
		}
		if promptLineGeneratesOnDemand(text) {
			return
		}
		key := menuID + "/" + lineKey
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, staticPromptLine{
			MenuID:  menuID,
			LineKey: lineKey,
			Values:  staticPromptValues(text),
		})
	}
	defaultKeys := make([]string, 0, len(cfg.Lines))
	for key := range cfg.Lines {
		defaultKeys = append(defaultKeys, key)
	}
	sort.Strings(defaultKeys)
	for _, key := range defaultKeys {
		add("", key, cfg.Lines[key])
	}
	menuIDs := make([]string, 0, len(cfg.Menus))
	for menuID := range cfg.Menus {
		menuIDs = append(menuIDs, menuID)
	}
	sort.Strings(menuIDs)
	for _, menuID := range menuIDs {
		menu := cfg.Menus[menuID]
		for _, line := range menu.Lines {
			add(menuID, line.Key, line.Text)
		}
	}
	return out
}

func promptLineGeneratesOnDemand(text string) bool {
	return strings.Contains(text, "{location}") || strings.Contains(text, "{province}") || strings.Contains(text, "{alert_count")
}

func renderPromptText(text string, values map[string]string) string {
	text = cleanPromptText(text)
	for key, value := range values {
		text = strings.ReplaceAll(text, "{"+key+"}", strings.TrimSpace(value))
	}
	return strings.Join(strings.Fields(text), " ")
}

func staticPromptValues(text string) map[string]string {
	values := map[string]string{}
	if strings.Contains(text, "{location}") {
		values["location"] = "your selected location"
	}
	if strings.Contains(text, "{province}") {
		values["province"] = "your province"
	}
	if strings.Contains(text, "{code}") {
		values["code"] = "your weather code"
	}
	if strings.Contains(text, "{feed_id}") {
		values["feed_id"] = "your feed"
	}
	if strings.Contains(text, "{lang}") {
		values["lang"] = "your language"
	}
	if strings.Contains(text, "{language_options}") {
		values["language_options"] = "1 for service in English"
	}
	if strings.Contains(text, "{telephone_service_name}") {
		values["telephone_service_name"] = "Haze Weather Telephone"
	}
	if strings.Contains(text, "{radio_service_name}") {
		values["radio_service_name"] = "Haze Weather Radio"
	}
	return values
}

func cleanPromptText(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func parseDuration(raw string, fallback time.Duration) time.Duration {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value
}

func parseInt(raw string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value
}

func parseIntWithFallback(raw string, value int, fallback int) int {
	if strings.TrimSpace(raw) != "" {
		return parseInt(raw, fallback)
	}
	if value != 0 {
		return value
	}
	return fallback
}

func parseFloatWithFallback(raw string, value float64, fallback float64) float64 {
	if strings.TrimSpace(raw) != "" {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err == nil {
			return parsed
		}
		return fallback
	}
	if value != 0 {
		return value
	}
	return fallback
}
