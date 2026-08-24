package ivr

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

// ResolvedLocation is the canonical IVR lookup result for caller-entered digits.
type ResolvedLocation struct {
	CanonicalID      string `json:"canonical_id,omitempty"`
	Input            string `json:"input"`
	Code             string `json:"code"`
	Source           string `json:"source"`
	Name             string `json:"name"`
	Province         string `json:"province,omitempty"`
	Country          string `json:"country,omitempty"`
	Kind             string `json:"kind,omitempty"`
	FeedID           string `json:"feed_id"`
	Covered          bool   `json:"covered_by_feed"`
	Language         string `json:"language"`
	Timezone         string `json:"timezone,omitempty"`
	Forecast         string `json:"forecast_id,omitempty"`
	StationID        string `json:"station_id,omitempty"`
	AirQualityID     string `json:"air_quality_id,omitempty"`
	ClimateID        string `json:"climate_id,omitempty"`
	HydrometricID    string `json:"hydrometric_id,omitempty"`
	MarineForecastID string `json:"marine_forecast_id,omitempty"`
	Latitude         string `json:"latitude,omitempty"`
	Longitude        string `json:"longitude,omitempty"`
}

// ResolvePlace resolves one source-qualified location database record without
// falling back to another record that happens to share the same code.
func (r *Resolver) ResolvePlace(snapshot locationdb.Snapshot, place locationdb.Place) (ResolvedLocation, error) {
	record, err := r.resolvePlaceRecord(snapshot, place)
	if err != nil {
		return ResolvedLocation{}, err
	}
	record = r.attachFeed(record)
	covered := record.FeedID != ""
	if record.FeedID == "" {
		record.FeedID = r.defaultFeedID()
		if record.FeedID == "" {
			return ResolvedLocation{}, fmt.Errorf("%s is known, but no enabled Haze feed is available as an IVR rendering profile", record.Name)
		}
	}
	return ResolvedLocation{
		Input:     place.Source + ":" + place.Code,
		Code:      record.Code,
		Source:    record.Source,
		Name:      record.Name,
		Province:  record.Province,
		Country:   record.Country,
		Kind:      record.Kind,
		FeedID:    record.FeedID,
		Covered:   covered,
		Language:  r.feedLanguage(record.FeedID),
		Timezone:  r.feedTimezone(record.FeedID),
		Forecast:  record.Forecast,
		StationID: record.StationID,
		Latitude:  record.Latitude,
		Longitude: record.Longitude,
	}, nil
}

func (r *Resolver) resolvePlaceRecord(snapshot locationdb.Snapshot, place locationdb.Place) (locationRecord, error) {
	if r == nil {
		return locationRecord{}, fmt.Errorf("location resolver is unavailable")
	}
	place.Source = strings.ToLower(strings.TrimSpace(place.Source))
	place.Code = strings.ToUpper(strings.TrimSpace(place.Code))
	if place.Source == "" || place.Code == "" {
		return locationRecord{}, fmt.Errorf("location source and code are required")
	}
	record := locationRecord{
		Code:      place.Code,
		Source:    place.Source,
		Name:      cleanLocationName(place.Name),
		Province:  provinceCode(place.Region),
		Country:   strings.ToUpper(strings.TrimSpace(place.Country)),
		Kind:      strings.TrimSpace(place.Kind),
		Latitude:  floatText(place.Lat),
		Longitude: floatText(place.Lon),
	}
	switch place.Source {
	case "hello_weather":
		hello, ok := r.cfg.HelloWeather[place.Code]
		if !ok {
			hello, ok = deriveHelloWeatherRecord(place.Code)
		}
		if ok {
			hello = r.enrichHelloWeatherRecord(hello)
			record.Source = hello.Source
			record.Forecast = hello.Forecast
			record.StationID = hello.StationID
			record.Latitude = firstNonBlank(record.Latitude, hello.Latitude)
			record.Longitude = firstNonBlank(record.Longitude, hello.Longitude)
		}
	case "forecast":
		record.Source = "eccc_forecast"
		record.Forecast = place.Code
		if clcCode := linkedCode(snapshot, "forecast", place.Code, "clc"); clcCode != "" {
			record.StationID = stationForArea(snapshot, "clc", clcCode)
		}
	case "clc":
		if record.Country == "US" {
			record.Source = "nws"
		}
		record.Forecast = reverseLinkedCode(snapshot, "clc", place.Code, "forecast")
		record.StationID = stationForArea(snapshot, "clc", place.Code)
	case "sgc":
		record.Source = "capcp_geocode"
		clcCode := linkedCode(snapshot, "sgc", place.Code, "clc")
		if clcCode != "" {
			record.Forecast = reverseLinkedCode(snapshot, "clc", clcCode, "forecast")
			record.StationID = stationForArea(snapshot, "clc", clcCode)
		}
	case "nws_zone", "nws_marine_zone":
		record.Forecast = place.Code
		record.StationID = stationForArea(snapshot, place.Source, place.Code)
	case "nws_same", "nws_marine_same":
		zoneSource := "nws_zone"
		if place.Source == "nws_marine_same" {
			zoneSource = "nws_marine_zone"
		}
		record.Forecast = linkedCode(snapshot, place.Source, place.Code, zoneSource)
		record.StationID = firstNonBlank(
			stationForArea(snapshot, place.Source, place.Code),
			stationForArea(snapshot, zoneSource, record.Forecast),
		)
	case "station":
		record.StationID = place.Code
		record.Forecast = forecastForStation(snapshot, place.Code)
	default:
		return locationRecord{}, fmt.Errorf("unsupported location source %q", place.Source)
	}
	if record.Name == "" {
		record.Name = place.Code
	}
	return record, nil
}

func linkedCode(snapshot locationdb.Snapshot, fromSource string, fromCode string, toSource string) string {
	return snapshot.LinkedCode(fromSource, fromCode, toSource)
}

func reverseLinkedCode(snapshot locationdb.Snapshot, toSource string, toCode string, fromSource string) string {
	return snapshot.ReverseLinkedCode(toSource, toCode, fromSource)
}

func stationForArea(snapshot locationdb.Snapshot, source string, code string) string {
	return snapshot.StationForArea(source, code)
}

func forecastForStation(snapshot locationdb.Snapshot, stationID string) string {
	stationID = strings.ToUpper(strings.TrimSpace(stationID))
	bestDistance := math.MaxFloat64
	bestForecast := ""
	for _, stationLink := range snapshot.StationAreas(stationID) {
		forecast := ""
		switch stationLink.AreaSource {
		case "clc":
			forecast = reverseLinkedCode(snapshot, "clc", stationLink.AreaCode, "forecast")
		case "nws_zone", "nws_marine_zone":
			forecast = stationLink.AreaCode
		}
		if forecast == "" {
			continue
		}
		if stationLink.DistanceKM < bestDistance || stationLink.DistanceKM == bestDistance && forecast < bestForecast {
			bestDistance = stationLink.DistanceKM
			bestForecast = forecast
		}
	}
	return bestForecast
}

type Resolver struct {
	cfg                loadedConfig
	providerNameMu     sync.Mutex
	providerNames      map[string]providerNameResult
	lookupProviderName providerNameLookup
	helloWeatherMu     sync.Mutex
	helloWeatherCodes  map[string]locationRecord
	helloWeatherLoaded bool
	helloWeatherReady  bool
	lookupHelloWeather helloWeatherLookup
	telephoneCodesOnce sync.Once
	telephoneCodes     []telephoneLocationCode
	geocodeIndexOnce   sync.Once
	geocodeNameIndex   map[string]locationRecord
	feedIndexOnce      sync.Once
	feedIndex          resolverFeedIndex
}

type resolverFeedAttachment struct {
	FeedID     string
	Forecast   string
	StationID  string
	RegionName string
	FeedOrder  int
	Phase      int
	ItemOrder  int
}

type resolverFeedIndex struct {
	RegionCodes  map[string]resolverFeedAttachment
	Forecasts    map[string]resolverFeedAttachment
	Subregions   map[string]resolverFeedAttachment
	Observations map[string]resolverFeedAttachment
	Names        map[string]resolverFeedAttachment
	Languages    map[string]string
	Timezones    map[string]string
	DefaultFeed  string
}

type providerNameLookup func(context.Context, string) (string, bool)
type helloWeatherLookup func(context.Context) map[string]locationRecord

type providerNameResult struct {
	Name string
	OK   bool
}

const helloWeatherCodesURL = "https://www.canada.ca/en/environment-climate-change/services/weather-general-tools-resources/telephone-services/recorded-observations-forecasts.html"

var (
	helloWeatherLinePattern = regexp.MustCompile(`(?i)^(.*?)\s+1-833-[0-9-]+\s*\([^)]*\)\s*Code:\s*([0-9](?:\s*[0-9]){4})`)
	htmlHeadingPattern      = regexp.MustCompile(`(?is)<(?:h2|summary)\b[^>]*>(.*?)</(?:h2|summary)>`)
	htmlRowPattern          = regexp.MustCompile(`(?is)<tr\b[^>]*>(.*?)</tr>`)
	htmlCellPattern         = regexp.MustCompile(`(?is)<td\b[^>]*>(.*?)</td>`)
	htmlTagPattern          = regexp.MustCompile(`(?s)<[^>]+>`)
	htmlScriptStylePattern  = regexp.MustCompile(`(?is)<script\b[^>]*>.*?</script>|<style\b[^>]*>.*?</style>`)
	resolverHTTPClient      = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        16,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     30 * time.Second,
		},
	}
)

func NewResolver(cfg loadedConfig) *Resolver {
	return &Resolver{
		cfg:                cfg,
		providerNames:      map[string]providerNameResult{},
		lookupProviderName: fetchECCCCitypageName,
		lookupHelloWeather: fetchHelloWeatherCodes,
	}
}

func (r *Resolver) Resolve(input string) (ResolvedLocation, error) {
	mode := locationclient.ParseMode(r.cfg.Root.Services.Rust.Location.Mode)
	if mode == locationclient.ModeLegacy {
		return r.resolveLegacy(input)
	}
	started := time.Now()
	canonical, canonicalErr := r.resolveCanonical(input)
	if mode == locationclient.ModeAuthoritative {
		return canonical, canonicalErr
	}
	legacy, legacyErr := r.resolveLegacy(input)
	log.Printf(
		"location shadow resolve input=%q legacy=%s canonical=%s canonical_id=%s ambiguous=%t latency_ms=%d error=%v",
		input,
		locationIdentity(legacy),
		locationIdentity(canonical),
		canonical.CanonicalID,
		canonicalErr != nil && strings.Contains(strings.ToLower(canonicalErr.Error()), "ambiguous"),
		time.Since(started).Milliseconds(),
		canonicalErr,
	)
	return legacy, legacyErr
}

func (r *Resolver) resolveLegacy(input string) (ResolvedLocation, error) {
	code := normalizeCallerCode(input)
	if code == "" {
		return ResolvedLocation{}, fmt.Errorf("enter a location code followed by pound")
	}
	code = r.canonicalCallerLocationCode(code)
	candidates := r.candidates(code)
	if len(candidates) == 0 {
		return ResolvedLocation{}, fmt.Errorf("no weather location matched %q", input)
	}
	sort.SliceStable(candidates, func(left, right int) bool {
		if candidates[left].FeedID != "" && candidates[right].FeedID == "" {
			return true
		}
		if candidates[left].FeedID == "" && candidates[right].FeedID != "" {
			return false
		}
		if leftPriority, rightPriority := candidatePriority(candidates[left]), candidatePriority(candidates[right]); leftPriority != rightPriority {
			return leftPriority < rightPriority
		}
		return candidates[left].Name < candidates[right].Name
	})
	location := candidates[0]
	attachedToFeed := location.FeedID != ""
	if location.FeedID == "" {
		location.FeedID = r.defaultFeedID()
		if location.FeedID == "" {
			return ResolvedLocation{}, fmt.Errorf("%s is known, but no enabled Haze feed is available as an IVR rendering profile", location.Name)
		}
		location.Language = r.feedLanguage(location.FeedID)
		location.Timezone = r.feedTimezone(location.FeedID)
	}
	location = r.withProviderDisplayName(location, attachedToFeed)
	return location, nil
}

func (r *Resolver) resolveCanonical(input string) (ResolvedLocation, error) {
	code := r.canonicalCallerLocationCode(normalizeCallerCode(input))
	if code == "" {
		return ResolvedLocation{}, fmt.Errorf("enter a location code followed by pound")
	}
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	client := locationclient.New(bridgeAddr, "haze-ivr-location")
	client.Timeout = 4 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
	defer cancel()
	response, err := client.Query(ctx, locationclient.Request{
		Operation: "resolve",
		Input:     &locationclient.Input{Kind: "auto", Text: code},
		Options: locationclient.Options{
			Limit:                  4,
			MinimumConfidence:      "high",
			StationModeRequirement: "any",
		},
	})
	if err != nil {
		return ResolvedLocation{}, err
	}
	if response.Ambiguous || len(response.Results) > 1 && response.Results[0].Match.Score-response.Results[1].Match.Score < 0.08 {
		return ResolvedLocation{}, fmt.Errorf("location %q is ambiguous and requires confirmation", input)
	}
	if len(response.Results) == 0 {
		return ResolvedLocation{}, fmt.Errorf("no weather location matched %q", input)
	}
	candidate := response.Results[0]
	entity := candidate.Entity
	record := locationRecord{
		Code:      code,
		Name:      entity.DisplayName(),
		Province:  provinceCode(entity.Region),
		Country:   strings.ToUpper(strings.TrimSpace(entity.Country)),
		Kind:      entity.Kind,
		Forecast:  entity.Identifier("eccc_citypage", "nws_zone", "nws_marine_zone"),
		StationID: entity.Identifier("eccc_station", "icao", "msc", "wmo", "ndbc"),
	}
	if entity.Geometry != nil {
		if entity.Geometry.Latitude != nil {
			record.Latitude = floatText(*entity.Geometry.Latitude)
		}
		if entity.Geometry.Longitude != nil {
			record.Longitude = floatText(*entity.Geometry.Longitude)
		}
	}
	record.Source = canonicalProviderSource(entity)
	if record.Forecast != "" {
		record.Code = record.Forecast
	} else if record.StationID != "" {
		record.Code = record.StationID
	}
	record = r.attachFeed(record)
	covered := record.FeedID != ""
	if record.FeedID == "" {
		record.FeedID = r.defaultFeedID()
	}
	if record.FeedID == "" {
		return ResolvedLocation{}, fmt.Errorf("%s is known, but no enabled Haze feed is available as an IVR rendering profile", record.Name)
	}
	return ResolvedLocation{
		CanonicalID: entity.ID,
		Input:       input,
		Code:        record.Code,
		Source:      record.Source,
		Name:        record.Name,
		Province:    record.Province,
		Country:     record.Country,
		Kind:        record.Kind,
		FeedID:      record.FeedID,
		Covered:     covered,
		Language:    r.feedLanguage(record.FeedID),
		Timezone:    r.feedTimezone(record.FeedID),
		Forecast:    record.Forecast,
		StationID:   record.StationID,
		Latitude:    record.Latitude,
		Longitude:   record.Longitude,
	}, nil
}

func canonicalProviderSource(entity locationclient.Entity) string {
	for _, identifier := range entity.Identifiers {
		switch strings.ToLower(identifier.Scheme) {
		case "nws_zone", "nws_marine_zone", "nws_ugc_county", "fips", "same":
			return "nws"
		case "eccc_citypage", "clc", "eccc_station", "msc", "wmo", "icao":
			return "eccc"
		case "ndbc":
			return "ndbc"
		}
	}
	return "canonical"
}

func locationIdentity(location ResolvedLocation) string {
	if location.Code == "" {
		return ""
	}
	return location.Source + ":" + location.Code
}

func (r *Resolver) withProviderDisplayName(location ResolvedLocation, attachedToFeed bool) ResolvedLocation {
	forecastID := strings.TrimSpace(location.Forecast)
	if strings.EqualFold(location.Source, "hello_weather") && !attachedToFeed && looksLikeProviderID(forecastID) {
		if name, ok := r.providerDisplayName(forecastID); ok {
			location.Name = name
			return location
		}
	}
	name := strings.TrimSpace(location.Name)
	if strings.EqualFold(location.Source, "hello_weather") && looksLikeProviderID(forecastID) && shouldPreferHelloWeatherProviderName(name, location.Code) {
		if providerName, ok := r.providerDisplayName(forecastID); ok {
			location.Name = providerName
			return location
		}
	}
	if name != "" && !strings.EqualFold(name, strings.TrimSpace(location.Code)) && !looksLikeProviderID(name) {
		return location
	}
	if !looksLikeProviderID(forecastID) {
		return location
	}
	if name, ok := r.providerDisplayName(forecastID); ok {
		location.Name = name
	}
	return location
}

func shouldPreferHelloWeatherProviderName(name string, code string) bool {
	name = strings.TrimSpace(name)
	if name == "" || strings.EqualFold(name, strings.TrimSpace(code)) || looksLikeProviderID(name) {
		return true
	}
	lower := strings.ToLower(name)
	return strings.HasPrefix(lower, "city of ") || strings.HasPrefix(lower, "town of ") || strings.HasPrefix(lower, "village of ")
}

func (r *Resolver) providerDisplayName(forecastID string) (string, bool) {
	forecastID = strings.TrimSpace(forecastID)
	if forecastID == "" || r.lookupProviderName == nil {
		return "", false
	}
	r.providerNameMu.Lock()
	if cached, ok := r.providerNames[strings.ToLower(forecastID)]; ok {
		r.providerNameMu.Unlock()
		return cached.Name, cached.OK
	}
	r.providerNameMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	name, ok := r.lookupProviderName(ctx, forecastID)
	name = cleanLocationName(name)

	r.providerNameMu.Lock()
	r.providerNames[strings.ToLower(forecastID)] = providerNameResult{Name: name, OK: ok && name != ""}
	r.providerNameMu.Unlock()
	return name, ok && name != ""
}

func (r *Resolver) candidates(code string) []ResolvedLocation {
	seen := map[string]struct{}{}
	var out []ResolvedLocation
	add := func(record locationRecord) {
		if record.Code == "" {
			return
		}
		record = r.attachFeed(record)
		key := strings.Join([]string{record.Code, record.Source, record.FeedID, record.Forecast, record.StationID}, "|")
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, ResolvedLocation{
			Input:     code,
			Code:      record.Code,
			Source:    record.Source,
			Name:      fallbackText(record.Name, record.Code),
			Province:  record.Province,
			Country:   record.Country,
			Kind:      record.Kind,
			FeedID:    record.FeedID,
			Covered:   record.FeedID != "",
			Language:  r.feedLanguage(record.FeedID),
			Timezone:  r.feedTimezone(record.FeedID),
			Forecast:  record.Forecast,
			StationID: record.StationID,
			Latitude:  record.Latitude,
			Longitude: record.Longitude,
		})
	}

	if record, ok := r.helloWeather(code); ok {
		add(record)
	}
	if record, ok := r.cfg.ForecastLocations[leftPad6(code)]; ok {
		add(record)
	}
	if record, ok := r.cfg.CLCs[leftPad6(code)]; ok {
		add(record)
	}
	if record, ok := r.cfg.Geocodes[code]; ok {
		add(record)
	}
	if record, ok := r.cfg.NWS[strings.ToUpper(code)]; ok {
		add(record)
	}
	if record, ok := r.cfg.NWS[leftPad6(code)]; ok {
		add(record)
	}
	for _, feed := range r.cfg.Feeds {
		if !xmlBool(feed.EnabledRaw, true) {
			continue
		}
		for _, loc := range feed.Locations.ObservationLocations.Locations {
			if strings.EqualFold(loc.ID, code) || strings.EqualFold(t9(loc.ID), code) {
				add(locationRecord{Code: loc.ID, Source: "station", Name: fallbackText(loc.NameOverride, loc.ID), FeedID: feed.ID, StationID: loc.ID})
			}
		}
	}
	return out
}

func (r *Resolver) helloWeather(code string) (locationRecord, bool) {
	if len(code) != 5 {
		return locationRecord{}, false
	}
	codes := r.helloWeatherDirectory()
	record, ok := codes[code]
	if !ok {
		if !r.helloWeatherDirectoryReady() {
			if derived, derivedOK := deriveHelloWeatherRecord(code); derivedOK {
				return r.enrichHelloWeatherRecord(derived), true
			}
		}
		return locationRecord{}, false
	}
	return r.enrichHelloWeatherRecord(record), true
}

func (r *Resolver) helloWeatherDirectory() map[string]locationRecord {
	r.helloWeatherMu.Lock()
	defer r.helloWeatherMu.Unlock()
	if r.helloWeatherLoaded {
		return r.helloWeatherCodes
	}
	lookup := r.lookupHelloWeather

	codes := cloneLocationRecords(r.cfg.HelloWeather)
	if len(codes) == 0 && lookup != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		codes = lookup(ctx)
		cancel()
	}
	if codes == nil {
		codes = map[string]locationRecord{}
	}
	r.helloWeatherCodes = codes
	r.helloWeatherReady = len(codes) > 0
	r.helloWeatherLoaded = true
	return codes
}

func (r *Resolver) helloWeatherDirectoryReady() bool {
	if r == nil {
		return false
	}
	_ = r.helloWeatherDirectory()
	r.helloWeatherMu.Lock()
	defer r.helloWeatherMu.Unlock()
	return r.helloWeatherReady
}

func cloneLocationRecords(records map[string]locationRecord) map[string]locationRecord {
	out := make(map[string]locationRecord, len(records))
	for code, record := range records {
		out[code] = record
	}
	return out
}

func (r *Resolver) enrichHelloWeatherRecord(record locationRecord) locationRecord {
	if strings.TrimSpace(record.Forecast) == "" {
		if derived, ok := deriveHelloWeatherRecord(record.Code); ok {
			record.Forecast = derived.Forecast
			if strings.TrimSpace(record.Province) == "" {
				record.Province = derived.Province
			}
		}
	}
	key := geocodeNameKey(record.Province, record.Name)
	if key == "" {
		return record
	}
	index := r.geocodeIndex()
	candidate, ok := index[key]
	if !ok {
		return record
	}
	record.Latitude = firstNonBlank(record.Latitude, candidate.Latitude)
	record.Longitude = firstNonBlank(record.Longitude, candidate.Longitude)
	return record
}

func (r *Resolver) geocodeIndex() map[string]locationRecord {
	r.geocodeIndexOnce.Do(func() {
		index := make(map[string]locationRecord, len(r.cfg.Geocodes))
		for _, candidate := range r.cfg.Geocodes {
			key := geocodeNameKey(candidate.Province, candidate.Name)
			if key == "" {
				continue
			}
			if _, exists := index[key]; !exists {
				index[key] = candidate
			}
		}
		r.geocodeNameIndex = index
	})
	return r.geocodeNameIndex
}

func geocodeNameKey(province string, name string) string {
	province = provinceCode(province)
	name = normalizedLocationName(name)
	if province == "" || name == "" {
		return ""
	}
	return province + "|" + name
}

func (r *Resolver) forecastDisplayName(forecast string) string {
	forecast = strings.TrimSpace(forecast)
	if forecast == "" {
		return ""
	}
	for _, record := range r.cfg.ForecastLocations {
		if strings.EqualFold(record.Forecast, forecast) {
			if name := cleanLocationName(record.Name); name != "" {
				return name
			}
		}
	}
	for _, feed := range r.cfg.Feeds {
		for _, region := range feed.Locations.Coverage.Regions {
			if !strings.EqualFold(strings.TrimSpace(region.DeriveForecast), forecast) {
				continue
			}
			if name := r.locationRecordName(region.ID); name != "" {
				return name
			}
			if name := cleanLocationName(region.Name); name != "" {
				return name
			}
		}
		for _, loc := range feed.Locations.ObservationLocations.Locations {
			if !strings.EqualFold(strings.TrimSpace(loc.ID), forecast) {
				continue
			}
			if name := cleanLocationName(loc.NameOverride); name != "" {
				return name
			}
			if name := cleanLocationName(feedDisplayName(feed)); name != "" {
				return name
			}
		}
	}
	return ""
}

func (r *Resolver) locationRecordName(code string) string {
	code = strings.TrimSpace(code)
	if code == "" {
		return ""
	}
	for _, key := range []string{code, leftPad6(code), strings.ToUpper(code)} {
		if record, ok := r.cfg.ForecastLocations[key]; ok {
			if name := cleanLocationName(record.Name); name != "" {
				return name
			}
		}
		if record, ok := r.cfg.CLCs[key]; ok {
			if name := cleanLocationName(record.Name); name != "" {
				return name
			}
		}
		if record, ok := r.cfg.Geocodes[key]; ok {
			if name := cleanLocationName(record.Name); name != "" {
				return name
			}
		}
		if record, ok := r.cfg.NWS[key]; ok {
			if name := cleanLocationName(record.Name); name != "" {
				return name
			}
		}
	}
	return ""
}

func cleanLocationName(name string) string {
	name = strings.TrimSpace(name)
	if looksLikeProviderID(name) {
		return ""
	}
	return name
}

func (r *Resolver) attachFeed(record locationRecord) locationRecord {
	if record.FeedID != "" {
		return record
	}
	index := r.resolverFeedIndex()
	candidates := make([]resolverFeedAttachment, 0, 8)
	appendCandidate := func(values map[string]resolverFeedAttachment, key string) {
		if candidate, ok := values[resolverCodeKey(key)]; ok {
			candidates = append(candidates, candidate)
		}
	}
	appendCandidate(index.RegionCodes, record.Code)
	appendCandidate(index.Forecasts, record.Forecast)
	appendCandidate(index.Forecasts, record.Code)
	if nameKey := strings.Join(normalizedNameTokens(record.Name), " "); nameKey != "" {
		if candidate, ok := index.Names[resolverCodeKey(nameKey)]; ok {
			candidates = append(candidates, candidate)
		}
	}
	appendCandidate(index.Subregions, record.Code)
	appendCandidate(index.Observations, record.Forecast)
	appendCandidate(index.Observations, record.StationID)
	appendCandidate(index.Observations, record.Code)
	if len(candidates) == 0 {
		return record
	}
	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if resolverFeedAttachmentBefore(candidate, best) {
			best = candidate
		}
	}
	record.FeedID = best.FeedID
	if best.Forecast != "" {
		record.Forecast = best.Forecast
	}
	if best.StationID != "" && record.StationID == "" {
		record.StationID = best.StationID
	}
	if best.RegionName != "" && !locationNameMentioned(best.RegionName, record.Name) {
		record.Name = best.RegionName
	}
	return record
}

func (r *Resolver) resolverFeedIndex() resolverFeedIndex {
	r.feedIndexOnce.Do(func() {
		index := resolverFeedIndex{
			RegionCodes:  make(map[string]resolverFeedAttachment),
			Forecasts:    make(map[string]resolverFeedAttachment),
			Subregions:   make(map[string]resolverFeedAttachment),
			Observations: make(map[string]resolverFeedAttachment),
			Names:        make(map[string]resolverFeedAttachment),
			Languages:    make(map[string]string),
			Timezones:    make(map[string]string),
		}
		for feedOrder, feed := range r.cfg.Feeds {
			feedID := strings.TrimSpace(feed.ID)
			feedKey := strings.ToLower(feedID)
			if feedKey != "" {
				if _, exists := index.Languages[feedKey]; !exists {
					for _, lang := range feed.Languages.Langs {
						if code := strings.TrimSpace(lang.Code); code != "" {
							index.Languages[feedKey] = code
							break
						}
					}
				}
				if _, exists := index.Timezones[feedKey]; !exists {
					index.Timezones[feedKey] = strings.TrimSpace(feed.Timezone)
				}
			}
			if !xmlBool(feed.EnabledRaw, true) {
				continue
			}
			if index.DefaultFeed == "" && feedID != "" {
				index.DefaultFeed = feedID
			}
			coverageOrder := 0
			for _, region := range feed.Locations.Coverage.Regions {
				attachment := resolverFeedAttachment{
					FeedID:     feed.ID,
					Forecast:   strings.TrimSpace(region.DeriveForecast),
					RegionName: r.coverageRegionDisplayName(region),
					FeedOrder:  feedOrder,
					Phase:      0,
					ItemOrder:  coverageOrder,
				}
				coverageOrder++
				resolverAddBestAttachment(index.RegionCodes, region.ID, attachment)
				resolverAddBestAttachment(index.Forecasts, region.DeriveForecast, attachment)
				for _, candidateName := range []string{
					region.Name,
					r.locationRecordName(region.ID),
					r.forecastDisplayName(region.DeriveForecast),
				} {
					for _, key := range resolverNameSubsequenceKeys(candidateName) {
						resolverAddBestAttachment(index.Names, key, attachment)
					}
				}
				for _, subregion := range region.Subregions {
					subregionAttachment := attachment
					subregionAttachment.ItemOrder = coverageOrder
					coverageOrder++
					resolverAddBestAttachment(index.Subregions, subregion.ID, subregionAttachment)
				}
			}
			for observationOrder, loc := range feed.Locations.ObservationLocations.Locations {
				resolverAddBestAttachment(index.Observations, loc.ID, resolverFeedAttachment{
					FeedID:    feed.ID,
					StationID: loc.ID,
					FeedOrder: feedOrder,
					Phase:     1,
					ItemOrder: observationOrder,
				})
			}
		}
		r.feedIndex = index
	})
	return r.feedIndex
}

func resolverAddBestAttachment(index map[string]resolverFeedAttachment, key string, candidate resolverFeedAttachment) {
	key = resolverCodeKey(key)
	if key == "" {
		return
	}
	if current, exists := index[key]; !exists || resolverFeedAttachmentBefore(candidate, current) {
		index[key] = candidate
	}
}

func resolverFeedAttachmentBefore(left resolverFeedAttachment, right resolverFeedAttachment) bool {
	if left.FeedOrder != right.FeedOrder {
		return left.FeedOrder < right.FeedOrder
	}
	if left.Phase != right.Phase {
		return left.Phase < right.Phase
	}
	return left.ItemOrder < right.ItemOrder
}

func resolverCodeKey(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func resolverNameSubsequenceKeys(value string) []string {
	tokens := normalizedNameTokens(value)
	if len(tokens) == 0 {
		return nil
	}
	keys := make([]string, 0, len(tokens)*(len(tokens)+1)/2)
	for start := range tokens {
		for end := start + 1; end <= len(tokens); end++ {
			keys = append(keys, strings.Join(tokens[start:end], " "))
		}
	}
	return keys
}

func (r *Resolver) coverageRegionDisplayName(region coverageRegionXML) string {
	if name := r.locationRecordName(region.ID); name != "" {
		return name
	}
	return cleanLocationName(region.Name)
}

func (r *Resolver) regionNameMatchesRecord(region coverageRegionXML, record locationRecord) bool {
	name := strings.TrimSpace(record.Name)
	if name == "" {
		return false
	}
	for _, candidate := range []string{
		region.Name,
		r.locationRecordName(region.ID),
		r.forecastDisplayName(region.DeriveForecast),
	} {
		if locationNameMentioned(candidate, name) {
			return true
		}
	}
	return false
}

func locationNameMentioned(haystack string, needle string) bool {
	haystackParts := normalizedNameTokens(haystack)
	needleParts := normalizedNameTokens(needle)
	if len(haystackParts) == 0 || len(needleParts) == 0 {
		return false
	}
	for start := 0; start+len(needleParts) <= len(haystackParts); start++ {
		matched := true
		for offset, part := range needleParts {
			if haystackParts[start+offset] != part {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func normalizedNameTokens(value string) []string {
	fields := strings.FieldsFunc(strings.ToLower(strings.TrimSpace(value)), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	})
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func regionMatchesRecord(region coverageRegionXML, record locationRecord) bool {
	return sameCode(region.ID, record.Code) ||
		sameCode(region.DeriveForecast, record.Forecast) ||
		sameCode(region.DeriveForecast, record.Code)
}

func sameCode(left string, right string) bool {
	return strings.EqualFold(strings.TrimSpace(left), strings.TrimSpace(right)) && strings.TrimSpace(left) != ""
}

func candidatePriority(location ResolvedLocation) int {
	source := strings.ToLower(strings.TrimSpace(location.Source))
	if sameCode(location.Code, location.Input) {
		switch source {
		case "station":
			return 0
		case "capcp_geocode":
			return 1
		case "eccc_forecast", "clc":
			return 2
		case "hello_weather":
			return 2
		case "nws_same", "nws_zone":
			return 4
		default:
			return 5
		}
	}
	switch source {
	case "station":
		return 0
	case "capcp_geocode":
		return 1
	case "hello_weather":
		return 2
	case "eccc_forecast", "clc":
		return 3
	case "nws_same", "nws_zone":
		return 4
	default:
		return 5
	}
}

func (r *Resolver) feedLanguage(feedID string) string {
	if language := r.resolverFeedIndex().Languages[strings.ToLower(strings.TrimSpace(feedID))]; language != "" {
		return language
	}
	return r.cfg.IVR.DefaultLanguage
}

func (r *Resolver) feedTimezone(feedID string) string {
	return r.resolverFeedIndex().Timezones[strings.ToLower(strings.TrimSpace(feedID))]
}

func (r *Resolver) defaultFeedID() string {
	return r.resolverFeedIndex().DefaultFeed
}

func normalizeCallerCode(input string) string {
	input = strings.TrimSpace(input)
	var builder strings.Builder
	for _, char := range input {
		if char >= '0' && char <= '9' {
			builder.WriteRune(char)
			continue
		}
		if char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' {
			builder.WriteRune(char)
		}
	}
	return builder.String()
}

func leftPad6(code string) string {
	code = strings.TrimSpace(code)
	if len(code) >= 6 {
		return code
	}
	return strings.Repeat("0", 6-len(code)) + code
}

func (r *Resolver) canonicalCallerLocationCode(code string) string {
	if expanded, ok := r.helloWeatherShortCode(code); ok {
		return expanded
	}
	return code
}

func (r *Resolver) helloWeatherShortCode(code string) (string, bool) {
	code = strings.TrimSpace(code)
	if len(code) < 3 || len(code) > 4 {
		return "", false
	}
	province := code[:1]
	city := code[1:]
	return r.helloWeatherCodeForProvinceNumber(province, city)
}

func (r *Resolver) helloWeatherCodeForProvinceNumber(province string, number string) (string, bool) {
	if r == nil {
		return "", false
	}
	provinceCodes := helloWeatherProvinceCodes(province)
	if len(provinceCodes) == 0 {
		return "", false
	}
	number = digitsOnly(number)
	if number == "" {
		return "", false
	}
	allowed := make(map[string]struct{}, len(provinceCodes))
	for _, code := range provinceCodes {
		allowed[code] = struct{}{}
	}
	directory := r.helloWeatherDirectory()
	if len(number) == 5 {
		if record, ok := directory[number]; ok {
			_, provinceOK := allowed[provinceCode(record.Province)]
			return number, provinceOK
		}
		return "", false
	}
	wanted := strings.TrimLeft(number, "0")
	if wanted == "" {
		return "", false
	}
	matches := make([]string, 0, 1)
	for code, record := range directory {
		if len(code) != 5 {
			continue
		}
		if _, ok := allowed[provinceCode(record.Province)]; !ok {
			continue
		}
		if strings.TrimLeft(code[2:], "0") == wanted {
			matches = append(matches, code)
		}
	}
	if len(matches) == 0 {
		return "", false
	}
	sort.Strings(matches)
	if len(matches) != 1 {
		return "", false
	}
	return matches[0], true
}

// helloWeatherShortCodeHasLongerMatch reports whether a typed legacy shortcut
// is a prefix of another real legacy shortcut. It prevents an IVR auto-submit
// from selecting a different place while the caller is still entering digits.
func (r *Resolver) helloWeatherShortCodeHasLongerMatch(input string) bool {
	input = digitsOnly(input)
	if len(input) < 2 || len(input) > 3 {
		return false
	}
	provinceCodes := helloWeatherProvinceCodes(input[:1])
	if len(provinceCodes) == 0 {
		return false
	}
	allowed := make(map[string]struct{}, len(provinceCodes))
	for _, province := range provinceCodes {
		allowed[province] = struct{}{}
	}
	for code, record := range r.helloWeatherDirectory() {
		if len(code) != 5 {
			continue
		}
		if _, ok := allowed[provinceCode(record.Province)]; !ok {
			continue
		}
		local := strings.TrimLeft(code[2:], "0")
		if local == "" {
			continue
		}
		short := input[:1] + local
		if len(short) > len(input) && strings.HasPrefix(short, input) {
			return true
		}
	}
	return false
}

// CanAutoSubmitCallerCode is intentionally stricter than Resolve. A valid
// shorter legacy code can also be the prefix of a longer legacy code, so it
// must wait for another digit, a pound key, or the regular digit timeout.
func (r *Resolver) CanAutoSubmitCallerCode(input string) bool {
	if r == nil {
		return false
	}
	code := normalizeCallerCode(input)
	if code == "" || r.helloWeatherShortCodeHasLongerMatch(code) {
		return false
	}
	_, err := r.Resolve(code)
	return err == nil
}

func (r *Resolver) IsFullHelloWeatherCode(input string) bool {
	if r == nil {
		return false
	}
	code := digitsOnly(input)
	if len(code) != 5 {
		return false
	}
	_, ok := r.helloWeatherDirectory()[code]
	return ok
}

func helloWeatherProvinceCodes(selector string) []string {
	selector = strings.TrimSpace(selector)
	switch selector {
	case "1":
		return []string{"NS", "NB", "PE"}
	case "2":
		return []string{"NL"}
	case "3":
		return []string{"QC"}
	case "4":
		return []string{"ON"}
	case "5":
		return []string{"MB"}
	case "6":
		return []string{"SK"}
	case "7":
		return []string{"AB"}
	case "8":
		return []string{"BC"}
	case "9":
		return []string{"YT", "NT", "NU"}
	}
	code := normalizeProvinceCode(selector)
	if validProvinceCode(code) && code != "CA" {
		return []string{code}
	}
	return nil
}

func deriveHelloWeatherRecord(code string) (locationRecord, bool) {
	code = strings.TrimSpace(code)
	if len(code) != 5 || code[0] != '0' {
		return locationRecord{}, false
	}
	meta, ok := helloWeatherProvinceForCode(code)
	if !ok {
		return locationRecord{}, false
	}
	cityDigits := code[2:]
	switch code[:3] {
	case "010", "011", "015", "017", "018", "091", "095", "098":
		cityDigits = code[3:]
	}
	for _, digit := range cityDigits {
		if digit < '0' || digit > '9' {
			return locationRecord{}, false
		}
	}
	city := strings.TrimLeft(cityDigits, "0")
	if city == "" {
		return locationRecord{}, false
	}
	return locationRecord{
		Code:     code,
		Source:   "hello_weather",
		Name:     code,
		Province: meta.Province,
		Forecast: meta.ProviderPrefix + "-" + city,
	}, true
}

func helloWeatherProvinceForCode(code string) (struct {
	Province       string
	ProviderPrefix string
}, bool) {
	code = strings.TrimSpace(code)
	if len(code) != 5 || code[0] != '0' {
		return struct {
			Province       string
			ProviderPrefix string
		}{}, false
	}
	switch code[:3] {
	case "010", "011":
		return helloWeatherProvinceMeta("NS", "ns"), true
	case "015", "017":
		return helloWeatherProvinceMeta("NB", "nb"), true
	case "018":
		return helloWeatherProvinceMeta("PE", "pe"), true
	case "091":
		return helloWeatherProvinceMeta("YT", "yt"), true
	case "095":
		return helloWeatherProvinceMeta("NT", "nt"), true
	case "098":
		return helloWeatherProvinceMeta("NU", "nu"), true
	}
	return helloWeatherProvinceDigit(code[1:2])
}

func helloWeatherProvinceMeta(province string, providerPrefix string) struct {
	Province       string
	ProviderPrefix string
} {
	return struct {
		Province       string
		ProviderPrefix string
	}{Province: province, ProviderPrefix: providerPrefix}
}

func helloWeatherProvinceDigit(digit string) (struct {
	Province       string
	ProviderPrefix string
}, bool) {
	switch strings.TrimSpace(digit) {
	case "1":
		return helloWeatherProvinceMeta("NS", "ns"), true
	case "2":
		return helloWeatherProvinceMeta("NL", "nl"), true
	case "3":
		return helloWeatherProvinceMeta("QC", "qc"), true
	case "4":
		return helloWeatherProvinceMeta("ON", "on"), true
	case "5":
		return helloWeatherProvinceMeta("MB", "mb"), true
	case "6":
		return helloWeatherProvinceMeta("SK", "sk"), true
	case "7":
		return helloWeatherProvinceMeta("AB", "ab"), true
	case "8":
		return helloWeatherProvinceMeta("BC", "bc"), true
	default:
		return struct {
			Province       string
			ProviderPrefix string
		}{}, false
	}
}

func isProvinceDigit(code string) bool {
	code = strings.TrimSpace(code)
	return len(code) == 1 && code[0] >= '1' && code[0] <= '9'
}

func provinceDigitDisplayName(province string) string {
	switch strings.TrimSpace(province) {
	case "1":
		return "the Atlantic provinces"
	case "2":
		return "Newfoundland and Labrador"
	case "3":
		return "Quebec"
	case "4":
		return "Ontario"
	case "5":
		return "Manitoba"
	case "6":
		return "Saskatchewan"
	case "7":
		return "Alberta"
	case "8":
		return "British Columbia"
	case "9":
		return "the territories"
	}
	if code := normalizeProvinceCode(province); validProvinceCode(code) && code != "CA" {
		return provinceDisplayName(code)
	}
	return "your province or territory"
}

func t9(value string) string {
	var builder strings.Builder
	for _, char := range strings.ToUpper(value) {
		switch {
		case char >= '0' && char <= '9':
			builder.WriteRune(char)
		case strings.ContainsRune("ABC", char):
			builder.WriteByte('2')
		case strings.ContainsRune("DEF", char):
			builder.WriteByte('3')
		case strings.ContainsRune("GHI", char):
			builder.WriteByte('4')
		case strings.ContainsRune("JKL", char):
			builder.WriteByte('5')
		case strings.ContainsRune("MNO", char):
			builder.WriteByte('6')
		case strings.ContainsRune("PQRS", char):
			builder.WriteByte('7')
		case strings.ContainsRune("TUV", char):
			builder.WriteByte('8')
		case strings.ContainsRune("WXYZ", char):
			builder.WriteByte('9')
		}
	}
	return builder.String()
}

func fetchECCCCitypageName(ctx context.Context, forecastID string) (string, bool) {
	forecastID = strings.TrimSpace(forecastID)
	if !looksLikeProviderID(forecastID) {
		return "", false
	}
	url := fmt.Sprintf("https://api.weather.gc.ca/collections/citypageweather-realtime/items/%s?f=json", forecastID)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", false
	}
	request.Header.Set("User-Agent", "HazeWeatherRadio/26.06 ivr")
	response, err := resolverHTTPClient.Do(request)
	if err != nil {
		return "", false
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", false
	}
	var payload map[string]any
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return "", false
	}
	name := providerLocalizedText(mapAt(mapAt(payload, "properties"), "name"))
	return name, name != ""
}

func providerLocalizedText(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case map[string]string:
		for _, key := range []string{"en-CA", "en", "fr-CA", "fr", "name", "text", "value"} {
			if text := strings.TrimSpace(typed[key]); text != "" {
				return text
			}
		}
	case map[string]any:
		for _, key := range []string{"en-CA", "en", "fr-CA", "fr", "name", "text", "value"} {
			if text := providerLocalizedText(typed[key]); text != "" {
				return text
			}
		}
	case []any:
		for _, item := range typed {
			if text := providerLocalizedText(item); text != "" {
				return text
			}
		}
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
	return ""
}

func fetchHelloWeatherCodes(ctx context.Context) map[string]locationRecord {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, helloWeatherCodesURL, nil)
	if err != nil {
		return map[string]locationRecord{}
	}
	request.Header.Set("User-Agent", "HazeWeatherRadio/26.06 ivr")
	response, err := resolverHTTPClient.Do(request)
	if err != nil {
		return map[string]locationRecord{}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return map[string]locationRecord{}
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return map[string]locationRecord{}
	}
	return parseHelloWeatherCodes(string(body))
}

func parseHelloWeatherCodes(page string) map[string]locationRecord {
	out := map[string]locationRecord{}
	headings := htmlHeadingPattern.FindAllStringSubmatchIndex(page, -1)
	for index, heading := range headings {
		headingText := cleanHTMLText(page[heading[2]:heading[3]])
		province, ok := provinceCodeFromHeading(headingText)
		if !ok {
			continue
		}
		sectionEnd := len(page)
		if index+1 < len(headings) {
			sectionEnd = headings[index+1][0]
		}
		for _, row := range htmlRowPattern.FindAllStringSubmatch(page[heading[1]:sectionEnd], -1) {
			cells := htmlCellPattern.FindAllStringSubmatch(row[1], -1)
			if len(cells) < 2 {
				continue
			}
			name := cleanHTMLText(cells[0][1])
			match := helloWeatherLinePattern.FindStringSubmatch(name + " " + cleanHTMLText(cells[1][1]))
			if len(match) != 3 {
				continue
			}
			code := digitsOnly(match[2])
			if name == "" || len(code) != 5 {
				continue
			}
			if _, exists := out[code]; !exists {
				out[code] = locationRecord{Code: code, Source: "hello_weather", Name: name, Province: province}
			}
		}
	}

	// Keep a text-only fallback for fixtures and simplified mirrors of the page.
	province := ""
	for _, line := range htmlTextLines(page) {
		if code, ok := provinceCodeFromHeading(line); ok {
			province = code
			continue
		}
		match := helloWeatherLinePattern.FindStringSubmatch(line)
		if len(match) != 3 || province == "" {
			continue
		}
		name := strings.TrimSpace(match[1])
		code := digitsOnly(match[2])
		if name == "" || code == "" {
			continue
		}
		if _, exists := out[code]; exists {
			continue
		}
		out[code] = locationRecord{
			Code:     code,
			Source:   "hello_weather",
			Name:     name,
			Province: province,
		}
	}
	return out
}

func cleanHTMLText(value string) string {
	return collapseSpace(html.UnescapeString(htmlTagPattern.ReplaceAllString(value, " ")))
}

func digitsOnly(value string) string {
	var builder strings.Builder
	for _, char := range value {
		if char >= '0' && char <= '9' {
			builder.WriteRune(char)
		}
	}
	return builder.String()
}

func htmlTextLines(page string) []string {
	page = htmlScriptStylePattern.ReplaceAllString(page, " ")
	page = strings.ReplaceAll(page, "<", "\n<")
	page = htmlTagPattern.ReplaceAllString(page, "\n")
	page = html.UnescapeString(page)
	lines := strings.Split(page, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = collapseSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func collapseSpace(value string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(value, "\u00a0", " ")), " ")
}

func provinceCodeFromHeading(line string) (string, bool) {
	for _, province := range helloWeatherProvinces() {
		if strings.EqualFold(line, province.Name) || strings.HasPrefix(strings.ToLower(line), strings.ToLower(province.Name+" City and vicinity")) {
			return province.Code, true
		}
	}
	return "", false
}

func helloWeatherProvinces() []struct {
	Name string
	Code string
} {
	return []struct {
		Name string
		Code string
	}{
		{Name: "British Columbia", Code: "BC"},
		{Name: "Alberta", Code: "AB"},
		{Name: "Saskatchewan", Code: "SK"},
		{Name: "Manitoba", Code: "MB"},
		{Name: "Ontario", Code: "ON"},
		{Name: "Quebec", Code: "QC"},
		{Name: "New Brunswick", Code: "NB"},
		{Name: "Nova Scotia", Code: "NS"},
		{Name: "Prince Edward Island", Code: "PE"},
		{Name: "Newfoundland and Labrador", Code: "NL"},
		{Name: "Yukon", Code: "YT"},
		{Name: "Northwest Territories", Code: "NT"},
		{Name: "Nunavut", Code: "NU"},
	}
}

func sameProvince(left string, right string) bool {
	left = provinceCode(left)
	right = provinceCode(right)
	return left != "" && right != "" && left == right
}

func provinceCode(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.TrimSpace(strings.Split(value, ",")[0])
	upper := strings.ToUpper(value)
	if len(upper) == 2 || len(upper) == 3 {
		return upper
	}
	for _, province := range helloWeatherProvinces() {
		if strings.EqualFold(value, province.Name) {
			return province.Code
		}
	}
	return upper
}

func normalizedLocationName(value string) string {
	tokens := normalizedNameTokens(value)
	out := tokens[:0]
	for _, token := range tokens {
		switch token {
		case "city", "town", "village", "rural", "municipality", "of":
			continue
		default:
			out = append(out, token)
		}
	}
	return strings.Join(out, " ")
}
