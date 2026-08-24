package productrender

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
	"github.com/meowraii/haze-weather-radio/services/go/internal/lead"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
	"gopkg.in/yaml.v3"
)

type Options struct {
	ConfigPath string
	BridgeAddr string
	Refresh    time.Duration
}

type rootConfig struct {
	Version   string                  `yaml:"version"`
	FeedsFile string                  `yaml:"feeds_file"`
	Operator  operatorConfig          `yaml:"operator"`
	Storage   datastore.StorageConfig `yaml:"storage"`
	Services  struct {
		Go struct {
			ProductRender productRenderConfig `yaml:"product_render"`
		} `yaml:"go"`
		Rust struct {
			Location struct {
				Mode string `yaml:"mode"`
			} `yaml:"location"`
		} `yaml:"rust"`
	} `yaml:"services"`
}

type productRenderConfig struct {
	Enabled bool          `yaml:"enabled"`
	Cleanup cleanupConfig `yaml:"cleanup"`
}

type cleanupConfig struct {
	Enabled *bool `yaml:"enabled"`
	Hour    *int  `yaml:"hour"`
	Minute  *int  `yaml:"minute"`
}

type operatorConfig struct {
	OnAirName any `yaml:"on_air_name"`
}

type feedsXML struct {
	Feeds []feedXML `xml:"feed"`
}

type feedXML struct {
	OnDemand   bool   `xml:"-"`
	ID         string `xml:"id,attr"`
	EnabledRaw string `xml:"enabled,attr"`
	Timezone   string `xml:"timezone,attr"`
	Playout    struct {
		Routine           string `xml:"routine,attr"`
		SAME              string `xml:"same,attr"`
		SAMEOriginator    string `xml:"same_originator,attr"`
		SAMEAttentionTone string `xml:"same_attention_tone,attr"`
	} `xml:"playout"`
	Alerts struct {
		CapCP  feedAlertSourceXML `xml:"cap_cp"`
		NWSCAP feedAlertSourceXML `xml:"nws_cap"`
	} `xml:"alerts"`
	Languages struct {
		Langs []feedLangXML `xml:"lang"`
	} `xml:"languages"`
	Description struct {
		Langs []struct {
			Code   string `xml:"code,attr"`
			Text   string `xml:"text,attr"`
			Suffix string `xml:"suffix,attr"`
		} `xml:"lang"`
	} `xml:"description"`
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
		HydrometricLocations struct {
			Locations  []locationXML               `xml:"location"`
			Upstream   hydrometricLocationGroupXML `xml:"upstream"`
			Downstream hydrometricLocationGroupXML `xml:"downstream"`
		} `xml:"hydrometricLocations"`
		MarineConditions struct {
			Locations []locationXML `xml:"location"`
		} `xml:"marineConditions"`
	} `xml:"locations"`
	Transmitter struct {
		Transmitters []transmitterXML `xml:"transmitter"`
	} `xml:"transmitter_metadata"`
}

type feedAlertSourceXML struct {
	EnabledRaw string         `xml:"enabled,attr"`
	Filter     alertFilterXML `xml:"filter"`
}

type feedLangXML struct {
	Code     string `xml:"code,attr"`
	Interval string `xml:"interval,attr"`
}

type alertFilterXML struct {
	UseFeedLocations string             `xml:"use_feed_locations,attr"`
	CoverageMode     string             `xml:"coverage_mode,attr"`
	Allowlist        alertFilterListXML `xml:"allowlist"`
	Blocklist        alertFilterListXML `xml:"blocklist"`
}

type alertFilterListXML struct {
	Severities   []string              `xml:"severity"`
	Urgencies    []string              `xml:"urgency"`
	Certainties  []string              `xml:"certainty"`
	MessageTypes []string              `xml:"message_type"`
	Events       []string              `xml:"event"`
	NAADSEvents  []string              `xml:"naads_event"`
	Others       []alertFilterOtherXML `xml:"other"`
}

type alertFilterOtherXML struct {
	ValueName string `xml:"value_name,attr"`
	Value     string `xml:"value,attr"`
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

type hydrometricLocationGroupXML struct {
	Locations []locationXML `xml:"location"`
}

type coverageSubregionXML struct {
	ID          string `xml:"id,attr"`
	CanonicalID string `xml:"canonical_id,attr"`
}

type locationXML struct {
	ID           string `xml:"id,attr"`
	Source       string `xml:"source,attr"`
	NameOverride string `xml:"name_override,attr"`
	Latitude     string `xml:"latitude,attr"`
	Longitude    string `xml:"longitude,attr"`
	NormalID     string `xml:"normal_id,attr"`
	CanonicalID  string `xml:"canonical_id,attr"`
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

type packagesXML struct {
	Defaults packageDefaultsXML `xml:"defaults"`
	Packages []packageXML       `xml:"package"`
}

type productTextXML struct {
	Packages []productTextPackageXML `xml:"package"`
}

type productsXML struct {
	Defaults packageDefaultsXML `xml:"defaults"`
	Products []productXML       `xml:"product"`
	Packages []productXML       `xml:"package"`
}

type productXML struct {
	ID         string                `xml:"id,attr"`
	EnabledRaw string                `xml:"enabled,attr"`
	ReaderID   string                `xml:"reader_id,attr"`
	ReaderID2  string                `xml:"readerid,attr"`
	Locations  packageLocationsXML   `xml:"locations"`
	Langs      []productLangXML      `xml:"lang"`
	Texts      []productTextEntryXML `xml:"text"`
	InnerXML   string                `xml:",innerxml"`
}

type productLangXML struct {
	ISO       string                `xml:"iso,attr"`
	Code      string                `xml:"code,attr"`
	Lang      string                `xml:"lang,attr"`
	ReaderID  string                `xml:"reader_id,attr"`
	ReaderID2 string                `xml:"readerid,attr"`
	Texts     []productTextEntryXML `xml:"text"`
	InnerXML  string                `xml:",innerxml"`
}

type productTextPackageXML struct {
	ID    string                `xml:"id,attr"`
	Texts []productTextEntryXML `xml:"text"`
}

type productTextEntryXML struct {
	Key  string `xml:"key,attr"`
	Lang string `xml:"lang,attr"`
	Text string `xml:",chardata"`
}

type packageDefaultsXML struct {
	Enabled  string `xml:"enabled"`
	ReaderID string `xml:"reader_id"`
}

type packageXML struct {
	ID         string              `xml:"id,attr"`
	EnabledRaw string              `xml:"enabled,attr"`
	ReaderID   string              `xml:"reader_id"`
	Locations  packageLocationsXML `xml:"locations"`
}

type packageProfile struct {
	Enabled      bool
	ReaderID     string
	ReaderByLang map[string]string
	Languages    []string
	Locations    packageLocations
}

type packageLocationsXML struct {
	StateProv string   `xml:"stateProv,attr"`
	Mentions  []string `xml:"mention"`
}

type packageLocations struct {
	StateProv string
	Mentions  []string
}

type loadedConfig struct {
	Root          rootConfig
	Feeds         []feedXML
	Packages      map[string]packageProfile
	ProductText   map[string]map[string]map[string]string
	ProductAudio  map[string]map[string]map[string]string
	ForecastNames map[string]forecastRegionName
	Leads         lead.Document
	BaseDir       string
	Store         datastore.Store
}

type forecastRegionName struct {
	English string
	French  string
}

func loadConfig(configPath string) (loadedConfig, error) {
	raw, err := os.ReadFile(filepath.Clean(configPath))
	if err != nil {
		return loadedConfig{}, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var root rootConfig
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return loadedConfig{}, err
	}
	baseDir := filepath.Dir(filepath.Clean(configPath))
	feeds, err := loadFeeds(resolvePath(baseDir, fallbackText(root.FeedsFile, "managed/configs/feeds.xml")))
	if err != nil {
		return loadedConfig{}, err
	}
	packages, productText, productAudio, err := loadProductsConfig(baseDir)
	if err != nil {
		return loadedConfig{}, err
	}
	forecastNames := loadForecastRegionNamesFromSQLite(baseDir)
	if len(forecastNames) == 0 {
		forecastNames = loadForecastRegionNames(resolvePath(baseDir, "managed/csv/FORECAST_LOCATIONS.csv"))
	}
	leads, err := lead.Load(resolvePath(baseDir, "managed/configs/lead.xml"))
	if err != nil {
		return loadedConfig{}, err
	}
	return loadedConfig{
		Root:          root,
		Feeds:         feeds,
		Packages:      packages,
		ProductText:   productText,
		ProductAudio:  productAudio,
		ForecastNames: forecastNames,
		Leads:         leads,
		BaseDir:       baseDir,
	}, nil
}

func loadProductsConfig(baseDir string) (map[string]packageProfile, map[string]map[string]map[string]string, map[string]map[string]map[string]string, error) {
	combinedPath := resolvePath(baseDir, "managed/configs/products.xml")
	packages, productText, productAudio, err := loadCombinedProducts(combinedPath)
	if err == nil {
		return packages, productText, productAudio, nil
	}
	if !os.IsNotExist(err) {
		return nil, nil, nil, err
	}
	packages, err = loadPackages(resolvePath(baseDir, "managed/configs/packages.xml"))
	if err != nil {
		return nil, nil, nil, err
	}
	productText, err = loadProductText(resolvePath(baseDir, "managed/configs/product_text.xml"))
	if err != nil {
		return nil, nil, nil, err
	}
	return packages, productText, map[string]map[string]map[string]string{}, nil
}

func loadForecastRegionNamesFromSQLite(baseDir string) map[string]forecastRegionName {
	snap, ok := locationdb.Load(baseDir)
	if !ok {
		return map[string]forecastRegionName{}
	}
	out := map[string]forecastRegionName{}
	for _, place := range snap.PlacesBySource("forecast") {
		if strings.TrimSpace(place.Code) == "" {
			continue
		}
		out[place.Code] = forecastRegionName{English: place.Name, French: place.NameFR}
	}
	return out
}

func loadForecastRegionNames(path string) map[string]forecastRegionName {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return map[string]forecastRegionName{}
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	_, _ = reader.Read()
	header, err := reader.Read()
	if err != nil {
		return map[string]forecastRegionName{}
	}
	codeIndex := csvHeaderIndex(header, "CODE")
	nameIndex := csvHeaderIndex(header, "NAME")
	nomIndex := csvHeaderIndex(header, "NOM")
	if codeIndex < 0 || nameIndex < 0 || nomIndex < 0 {
		return map[string]forecastRegionName{}
	}
	names := map[string]forecastRegionName{}
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		maxIndex := maxInt(codeIndex, nameIndex, nomIndex)
		if len(row) <= maxIndex {
			continue
		}
		code := strings.TrimSpace(strings.Trim(row[codeIndex], `"`))
		if code == "" {
			continue
		}
		if _, exists := names[code]; exists {
			continue
		}
		names[code] = forecastRegionName{
			English: strings.TrimSpace(strings.Trim(row[nameIndex], `"`)),
			French:  strings.TrimSpace(strings.Trim(row[nomIndex], `"`)),
		}
	}
	return names
}

func csvHeaderIndex(header []string, name string) int {
	for index, value := range header {
		if strings.EqualFold(strings.TrimSpace(value), name) {
			return index
		}
	}
	return -1
}

func maxInt(values ...int) int {
	maxValue := values[0]
	for _, value := range values[1:] {
		if value > maxValue {
			maxValue = value
		}
	}
	return maxValue
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

func loadPackages(path string) (map[string]packageProfile, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]packageProfile{}, nil
		}
		return nil, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var parsed packagesXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse packages XML: %w", err)
	}
	defaultEnabled := xmlBool(parsed.Defaults.Enabled, true)
	defaultReader := strings.TrimSpace(parsed.Defaults.ReaderID)
	profiles := map[string]packageProfile{}
	for _, item := range parsed.Packages {
		id := strings.ToLower(strings.TrimSpace(item.ID))
		if id == "" {
			continue
		}
		reader := strings.TrimSpace(item.ReaderID)
		if reader == "" {
			reader = defaultReader
		}
		profiles[id] = packageProfile{
			Enabled:   xmlBool(item.EnabledRaw, defaultEnabled),
			ReaderID:  reader,
			Locations: normalizePackageLocations(item.Locations),
		}
	}
	return profiles, nil
}

func loadCombinedProducts(path string) (map[string]packageProfile, map[string]map[string]map[string]string, map[string]map[string]map[string]string, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, nil, nil, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var parsed productsXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, nil, nil, fmt.Errorf("parse products XML: %w", err)
	}
	defaultEnabled := xmlBool(parsed.Defaults.Enabled, true)
	defaultReader := strings.TrimSpace(parsed.Defaults.ReaderID)
	packages := map[string]packageProfile{}
	productText := map[string]map[string]map[string]string{}
	productAudio := map[string]map[string]map[string]string{}
	for _, item := range append(parsed.Products, parsed.Packages...) {
		id := strings.ToLower(strings.TrimSpace(item.ID))
		if id == "" {
			continue
		}
		reader := firstNonBlank(item.ReaderID, item.ReaderID2)
		if reader == "" {
			for _, lang := range item.Langs {
				reader = firstNonBlank(lang.ReaderID, lang.ReaderID2)
				if reader != "" {
					break
				}
			}
		}
		if reader == "" {
			reader = defaultReader
		}
		readerByLang := map[string]string{}
		languages := []string{}
		seenLangs := map[string]struct{}{}
		for _, lang := range item.Langs {
			code := productLangCode(lang)
			if normalized := normalizeLangKey(code); normalized != "" && normalized != "*" {
				if _, exists := seenLangs[normalized]; !exists {
					seenLangs[normalized] = struct{}{}
					languages = append(languages, normalized)
				}
			}
			langReader := firstNonBlank(lang.ReaderID, lang.ReaderID2)
			if code != "" && langReader != "" {
				readerByLang[normalizeLangKey(code)] = langReader
			}
		}
		packages[id] = packageProfile{
			Enabled:      xmlBool(item.EnabledRaw, defaultEnabled),
			ReaderID:     reader,
			ReaderByLang: readerByLang,
			Languages:    languages,
			Locations:    normalizePackageLocations(item.Locations),
		}
		addProductTextEntries(productText, id, "", item.Texts)
		for _, lang := range item.Langs {
			addProductScriptEntries(productText, productAudio, id, productLangCode(lang), lang.InnerXML)
			addProductTextEntries(productText, id, productLangCode(lang), lang.Texts)
		}
	}
	return packages, productText, productAudio, nil
}

func productLangCode(lang productLangXML) string {
	return firstNonBlank(lang.Lang, lang.ISO, lang.Code)
}

func addProductTextEntries(out map[string]map[string]map[string]string, pkgID string, inheritedLang string, entries []productTextEntryXML) {
	if out[pkgID] == nil {
		out[pkgID] = map[string]map[string]string{}
	}
	for _, entry := range entries {
		key := strings.ToLower(strings.TrimSpace(entry.Key))
		if key == "" {
			continue
		}
		lang := normalizeLangKey(firstNonBlank(entry.Lang, inheritedLang))
		addProductTextEntry(out, pkgID, lang, key, entry.Text, true)
	}
}

func addProductTextEntry(out map[string]map[string]map[string]string, pkgID string, lang string, key string, text string, overwrite bool) {
	if out[pkgID] == nil {
		out[pkgID] = map[string]map[string]string{}
	}
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return
	}
	lang = normalizeLangKey(lang)
	if out[pkgID][key] == nil {
		out[pkgID][key] = map[string]string{}
	}
	if _, exists := out[pkgID][key][lang]; exists && !overwrite {
		return
	}
	out[pkgID][key][lang] = strings.TrimSpace(text)
}

func addProductAudioEntry(out map[string]map[string]map[string]string, pkgID string, lang string, key string, path string, overwrite bool) {
	if out[pkgID] == nil {
		out[pkgID] = map[string]map[string]string{}
	}
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" {
		return
	}
	lang = normalizeLangKey(lang)
	if out[pkgID][key] == nil {
		out[pkgID][key] = map[string]string{}
	}
	if _, exists := out[pkgID][key][lang]; exists && !overwrite {
		return
	}
	if path = strings.TrimSpace(path); path != "" {
		out[pkgID][key][lang] = path
	}
}

func addProductScriptEntries(textOut map[string]map[string]map[string]string, audioOut map[string]map[string]map[string]string, pkgID string, inheritedLang string, innerXML string) {
	decoder := xml.NewDecoder(strings.NewReader("<root>" + innerXML + "</root>"))
	var stack []string
	ordinals := map[string]int{}
	for {
		token, err := decoder.Token()
		if err != nil {
			return
		}
		switch typed := token.(type) {
		case xml.StartElement:
			name := strings.ToLower(strings.TrimSpace(typed.Name.Local))
			switch name {
			case "text", "placeholder":
				var body string
				if err := decoder.DecodeElement(&body, &typed); err != nil {
					return
				}
				key := attrValue(typed.Attr, "key")
				if key == "" {
					key = implicitProductTextKey(name, stack, ordinals)
					lang := firstNonBlank(attrValue(typed.Attr, "lang"), inheritedLang)
					addProductTextEntry(textOut, pkgID, lang, key, body, true)
					continue
				}
				lang := firstNonBlank(attrValue(typed.Attr, "lang"), inheritedLang)
				if prefix := strings.Join(stack, "."); prefix != "" {
					addProductTextEntry(textOut, pkgID, lang, prefix+"."+key, body, true)
					if len(stack) > 1 && stack[0] == "region" {
						addProductTextEntry(textOut, pkgID, lang, strings.Join(stack[1:], ".")+"."+key, body, true)
					}
					addProductTextEntry(textOut, pkgID, lang, key, body, false)
					continue
				}
				addProductTextEntry(textOut, pkgID, lang, key, body, true)
			case "audio":
				var body string
				if err := decoder.DecodeElement(&body, &typed); err != nil {
					return
				}
				key := attrValue(typed.Attr, "key")
				if key == "" {
					key = implicitProductTextKey("text", stack, ordinals)
				}
				lang := firstNonBlank(attrValue(typed.Attr, "lang"), inheritedLang)
				if prefix := strings.Join(stack, "."); prefix != "" {
					addProductAudioEntry(audioOut, pkgID, lang, prefix+"."+key, body, true)
					if len(stack) > 1 && stack[0] == "region" {
						addProductAudioEntry(audioOut, pkgID, lang, strings.Join(stack[1:], ".")+"."+key, body, true)
					}
					addProductAudioEntry(audioOut, pkgID, lang, key, body, false)
					continue
				}
				addProductAudioEntry(audioOut, pkgID, lang, key, body, true)
			case "root", "lang":
			default:
				stack = append(stack, name)
			}
		case xml.EndElement:
			name := strings.ToLower(strings.TrimSpace(typed.Name.Local))
			if len(stack) > 0 && stack[len(stack)-1] == name {
				stack = stack[:len(stack)-1]
			}
		}
	}
}

func implicitProductTextKey(kind string, stack []string, ordinals map[string]int) string {
	prefix := strings.Join(stack, ".")
	if kind == "placeholder" {
		if prefix == "" {
			return "placeholder"
		}
		return prefix + ".placeholder"
	}
	ordinals[prefix]++
	if prefix == "" {
		return fmt.Sprintf("text.%d", ordinals[prefix])
	}
	return fmt.Sprintf("%s.text.%d", prefix, ordinals[prefix])
}

func attrValue(attrs []xml.Attr, name string) string {
	for _, attr := range attrs {
		if strings.EqualFold(attr.Name.Local, name) {
			return strings.TrimSpace(attr.Value)
		}
	}
	return ""
}

func normalizePackageLocations(raw packageLocationsXML) packageLocations {
	stateProv := strings.ToUpper(strings.TrimSpace(raw.StateProv))
	seen := map[string]struct{}{}
	mentions := make([]string, 0, len(raw.Mentions)+4)
	addMention := func(value string) {
		value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
		if value == "" {
			return
		}
		key := strings.ToUpper(value)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		mentions = append(mentions, value)
	}
	for _, mention := range raw.Mentions {
		addMention(mention)
	}
	switch stateProv {
	case "SK":
		addMention("SK")
		addMention("Saskatchewan")
		addMention("Southern SK")
		addMention("Southern Saskatchewan")
	}
	return packageLocations{StateProv: stateProv, Mentions: mentions}
}

func loadProductText(path string) (map[string]map[string]map[string]string, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]map[string]map[string]string{}, nil
		}
		return nil, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var parsed productTextXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse product text XML: %w", err)
	}
	out := map[string]map[string]map[string]string{}
	for _, pkg := range parsed.Packages {
		pkgID := strings.ToLower(strings.TrimSpace(pkg.ID))
		if pkgID == "" {
			continue
		}
		addProductTextEntries(out, pkgID, "", pkg.Texts)
	}
	return out, nil
}

func (cfg loadedConfig) feedByID(feedID string) (feedXML, bool) {
	for _, feed := range cfg.Feeds {
		if strings.EqualFold(strings.TrimSpace(feed.ID), strings.TrimSpace(feedID)) {
			return feed, true
		}
	}
	return feedXML{}, false
}

func normalizeLangKey(lang string) string {
	lang = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(lang), "_", "-"))
	if lang == "" || lang == "*" {
		return "*"
	}
	return lang
}

func shortLangKey(lang string) string {
	lang = normalizeLangKey(lang)
	if lang == "*" {
		return ""
	}
	if index := strings.Index(lang, "-"); index > 0 {
		return lang[:index]
	}
	return lang
}

func (cfg loadedConfig) readerID(pkgID string) string {
	if profile, ok := cfg.Packages[pkgID]; ok {
		return profile.ReaderID
	}
	return ""
}

func (cfg loadedConfig) readerIDForLanguage(pkgID string, lang string) string {
	profile, ok := cfg.Packages[strings.ToLower(strings.TrimSpace(pkgID))]
	if !ok {
		return ""
	}
	for _, key := range []string{normalizeLangKey(lang), shortLangKey(lang), "en-ca", "en", "*"} {
		if reader := strings.TrimSpace(profile.ReaderByLang[key]); reader != "" {
			return reader
		}
	}
	return profile.ReaderID
}

func (cfg loadedConfig) packageRenderLanguage(pkgID string, lang string) string {
	profile, ok := cfg.Packages[strings.ToLower(strings.TrimSpace(pkgID))]
	if !ok || len(profile.Languages) == 0 {
		return strings.TrimSpace(lang)
	}
	langKey := normalizeLangKey(lang)
	short := shortLangKey(langKey)
	for _, allowed := range profile.Languages {
		if langKey == allowed || short != "" && short == shortLangKey(allowed) {
			return strings.TrimSpace(lang)
		}
	}
	return profile.Languages[0]
}

func (cfg loadedConfig) packageEnabled(pkgID string) bool {
	profile, ok := cfg.Packages[pkgID]
	return !ok || profile.Enabled
}

func (cfg loadedConfig) packageLocations(pkgID string) packageLocations {
	if profile, ok := cfg.Packages[strings.ToLower(strings.TrimSpace(pkgID))]; ok {
		return profile.Locations
	}
	return packageLocations{}
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

func feedRenderLanguage(feed feedXML, requested string) string {
	languages := feedLanguages(feed)
	if len(languages) == 0 {
		return "en-US"
	}
	primary := languages[0].Code
	if len(languages) == 1 {
		return primary
	}
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return primary
	}
	requestedKey := normalizeLangKey(requested)
	requestedShort := shortLangKey(requestedKey)
	for _, lang := range languages {
		codeKey := normalizeLangKey(lang.Code)
		if requestedKey == codeKey || requestedShort != "" && requestedShort == shortLangKey(codeKey) {
			return lang.Code
		}
	}
	return primary
}

func feedSiteName(feed feedXML) string {
	return fallbackText(stationTransmitter(feed).SiteName, feed.ID)
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

func feedCoverageCodes(feed feedXML) map[string]struct{} {
	codes := map[string]struct{}{}
	for _, region := range feed.Locations.Coverage.Regions {
		addCoverageCode(codes, region.ID)
		for _, subregion := range region.Subregions {
			addCoverageCode(codes, subregion.ID)
		}
	}
	return codes
}

func addCoverageCode(codes map[string]struct{}, raw string) {
	code := strings.TrimSpace(raw)
	if code == "" {
		return
	}
	codes[code] = struct{}{}
}

func displayText(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case []any:
		for _, item := range typed {
			if text := displayText(item); text != "" {
				return text
			}
		}
	case map[string]any:
		for _, key := range []string{"text", "name", "value", "address"} {
			if text := displayText(typed[key]); text != "" {
				return text
			}
		}
		for _, child := range typed {
			if text := displayText(child); text != "" {
				return text
			}
		}
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func spokenText(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case []any:
		merged := map[string]any{}
		for _, item := range typed {
			if child, ok := item.(map[string]any); ok {
				for key, value := range child {
					merged[key] = value
				}
			} else if text := spokenText(item); text != "" {
				return text
			}
		}
		for _, key := range []string{"pronunciation", "pronounciation", "text", "name", "value", "address"} {
			if text := spokenText(merged[key]); text != "" {
				return text
			}
		}
	case map[string]any:
		for _, key := range []string{"pronunciation", "pronounciation", "text", "name", "value", "address"} {
			if text := spokenText(typed[key]); text != "" {
				return text
			}
		}
		for _, child := range typed {
			if text := spokenText(child); text != "" {
				return text
			}
		}
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func spokenNetworkName(network transmitterNetworkXML) string {
	for _, value := range []string{network.Pronunciation, network.Pronounciation, network.Name} {
		if text := strings.TrimSpace(value); text != "" {
			return text
		}
	}
	return ""
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

func intText(raw string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value
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
