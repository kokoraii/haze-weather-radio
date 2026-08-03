package dataingest

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
	"github.com/meowraii/haze-weather-radio/services/go/internal/events"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
	"gopkg.in/yaml.v3"
)

const serviceID = "haze-data-ingest"
const thunderstormOutlookCoverageToleranceKM = 0.5
const maxDataIngestCycleTimeout = 10 * time.Minute
const swobPartnerBaseURL = "https://dd.weather.gc.ca/today/observations/swob-ml/partners/"

type Options struct {
	ConfigPath string
	Interval   time.Duration
	Timeout    time.Duration
	Once       bool
}

type rootConfig struct {
	FeedsFile string                  `yaml:"feeds_file"`
	Storage   datastore.StorageConfig `yaml:"storage"`
	Services  struct {
		Go struct {
			DataIngest struct {
				Interval string `yaml:"interval"`
				Timeout  string `yaml:"timeout"`
			} `yaml:"data_ingest"`
		} `yaml:"go"`
		Rust struct {
			Location struct {
				Mode string `yaml:"mode"`
			} `yaml:"location"`
		} `yaml:"rust"`
	} `yaml:"services"`
}

type feedsXML struct {
	Feeds []feedXML `xml:"feed"`
}

type feedXML struct {
	ID         string `xml:"id,attr"`
	EnabledRaw string `xml:"enabled,attr"`
	Timezone   string `xml:"timezone,attr"`
	Locations  struct {
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
			Locations  []locationXML               `xml:"location"`
			Upstream   hydrometricLocationGroupXML `xml:"upstream"`
			Downstream hydrometricLocationGroupXML `xml:"downstream"`
		} `xml:"hydrometricLocations"`
	} `xml:"locations"`
}

type coverageRegionXML struct {
	ID                  string `xml:"id,attr"`
	Source              string `xml:"source,attr"`
	Name                string `xml:"name,attr"`
	DeriveForecast      string `xml:"derive_forecast,attr"`
	CanonicalID         string `xml:"canonical_id,attr"`
	ForecastCanonicalID string `xml:"forecast_canonical_id,attr"`
}

type hydrometricLocationGroupXML struct {
	Locations []locationXML `xml:"location"`
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

type loadedConfig struct {
	Root          rootConfig
	Feeds         []feedXML
	ForecastNames map[string]forecastRegionName
	BaseDir       string
}

type forecastRegionName struct {
	English string
	French  string
}

func Run(ctx context.Context, options Options) error {
	if strings.TrimSpace(options.ConfigPath) == "" {
		options.ConfigPath = "config.yaml"
	}
	if options.Interval <= 0 {
		options.Interval = 45 * time.Minute
	}
	if options.Timeout <= 0 {
		options.Timeout = 20 * time.Second
	}
	loadDotEnv(filepath.Join(filepath.Dir(filepath.Clean(options.ConfigPath)), ".env"))
	loadDotEnv(".env")
	cfg, err := loadConfig(options.ConfigPath)
	if err != nil {
		return err
	}
	interval := configDuration(cfg.Root.Services.Go.DataIngest.Interval, options.Interval)
	timeout := configDuration(cfg.Root.Services.Go.DataIngest.Timeout, options.Timeout)
	publisher := events.Publisher(events.NewJSONLPublisher(os.Stdout))
	if bridgeAddr := os.Getenv("HAZE_HOST_BRIDGE_ADDR"); bridgeAddr != "" {
		publisher = events.NewHostBridgePublisher(bridgeAddr)
	}
	_ = publisher.Publish(events.Event{
		Type:   "service.ready",
		Source: serviceID,
		Data: map[string]any{
			"service": serviceID,
			"feeds":   len(cfg.enabledFeeds()),
		},
	})

	store, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer store.Close()

	client := dataIngestHTTPClient(timeout)
	cycleTimeout := dataIngestCycleTimeout(interval, timeout)
	for {
		cfg = hydrateConfiguredLocations(ctx, cfg, strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR")))
		cycleCtx, cancel := context.WithTimeout(ctx, cycleTimeout)
		err := fetchOnce(cycleCtx, cfg, client, publisher, store)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				log.Printf("data ingest cycle timed out after %s", cycleTimeout)
			} else {
				log.Printf("data ingest cycle failed: %v", err)
			}
		}
		if options.Once {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func dataIngestCycleTimeout(interval time.Duration, requestTimeout time.Duration) time.Duration {
	if interval <= 0 {
		interval = 45 * time.Minute
	}
	if requestTimeout <= 0 {
		requestTimeout = 20 * time.Second
	}
	minimum := requestTimeout * 3
	if minimum < 2*time.Minute {
		minimum = 2 * time.Minute
	}
	timeout := interval
	if timeout > maxDataIngestCycleTimeout {
		timeout = maxDataIngestCycleTimeout
	}
	if timeout < minimum {
		return minimum
	}
	return timeout
}

func dataIngestHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          128,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}
}

func fetchOnce(ctx context.Context, cfg loadedConfig, client *http.Client, publisher events.Publisher, store datastore.Store) error {
	ecccCache := map[string]map[string]any{}
	for _, feed := range cfg.enabledFeeds() {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, loc := range feed.Locations.ObservationLocations.Locations {
			if err := ctx.Err(); err != nil {
				return err
			}
			if strings.TrimSpace(loc.ID) == "" {
				continue
			}
			payload, err := fetchObservation(ctx, client, loc)
			if err != nil {
				log.Printf("observation fetch failed for %s/%s: %v", sourceKind(loc.Source), loc.ID, err)
				continue
			}
			setDefaultStationID(payload, loc.ID)
			if sourceKind(loc.Source) == "eccc" {
				if raw, ok := payload["_raw_citypage"].(map[string]any); ok {
					ecccCache[loc.ID] = raw
				}
				delete(payload, "_raw_citypage")
			}
			if err := persistObservation(ctx, store, loc, payload); err != nil {
				log.Printf("observation store failed for %s: %v", loc.ID, err)
				continue
			}
			publishLocationDiscovery(publisher, loc, payload, "weather_station", "surface_observation")
			publishDataReady(publisher, feed.ID, "current_conditions", loc.ID, loc.CanonicalID)
		}
		for _, loc := range feed.Locations.AviationReportLocations.Locations {
			if err := ctx.Err(); err != nil {
				return err
			}
			if strings.TrimSpace(loc.ID) == "" {
				continue
			}
			payload, err := fetchAviationObservation(ctx, client, loc)
			if err != nil {
				log.Printf("aviation observation fetch failed for %s/%s: %v", sourceKind(loc.Source), loc.ID, err)
				continue
			}
			setDefaultStationID(payload, loc.ID)
			if raw, ok := payload["_raw_citypage"].(map[string]any); ok {
				ecccCache[loc.ID] = raw
			}
			delete(payload, "_raw_citypage")
			if err := persistObservation(ctx, store, loc, payload); err != nil {
				log.Printf("aviation observation store failed for %s: %v", loc.ID, err)
				continue
			}
			publishLocationDiscovery(publisher, loc, payload, "airport", "aviation_observation")
			publishDataReady(publisher, feed.ID, "aviation_reports", loc.ID, loc.CanonicalID)
		}
		for _, loc := range feed.Locations.MarineConditions.Locations {
			if err := ctx.Err(); err != nil {
				return err
			}
			if strings.TrimSpace(loc.ID) == "" {
				continue
			}
			payload, err := fetchAviationObservation(ctx, client, loc)
			if err != nil {
				log.Printf("marine observation fetch failed for %s/%s: %v", sourceKind(loc.Source), loc.ID, err)
				continue
			}
			setDefaultStationID(payload, loc.ID)
			delete(payload, "_raw_citypage")
			if err := persistObservation(ctx, store, loc, payload); err != nil {
				log.Printf("marine observation store failed for %s: %v", loc.ID, err)
				continue
			}
			publishLocationDiscovery(publisher, loc, payload, "marine_station", "marine_observation")
			publishDataReady(publisher, feed.ID, "marine_reports", loc.ID, loc.CanonicalID)
		}
		for _, region := range feed.Locations.Coverage.Regions {
			if err := ctx.Err(); err != nil {
				return err
			}
			if sourceKind(region.Source) != "eccc" {
				continue
			}
			forecastID := forecastRegionFetchID(region)
			if forecastID == "" {
				continue
			}
			raw := ecccCache[forecastID]
			if raw == nil {
				var err error
				raw, err = fetchECCCCitypage(ctx, client, forecastID)
				if err != nil {
					log.Printf("ECCC forecast fetch failed for %s: %v", forecastID, err)
					continue
				}
				ecccCache[forecastID] = raw
			}
			payload, ok := buildECCCForecast(raw, region, cfg.ForecastNames)
			if !ok {
				continue
			}
			if err := persistForecast(ctx, store, region, forecastID, payload); err != nil {
				log.Printf("forecast store failed for %s: %v", forecastID, err)
				continue
			}
			publishForecastDiscovery(publisher, region, forecastID, payload)
			publishDataReady(publisher, feed.ID, "forecast", forecastID, region.ForecastCanonicalID)
		}
		for _, loc := range feed.Locations.AirQualityLocations.Locations {
			if err := ctx.Err(); err != nil {
				return err
			}
			if sourceKind(loc.Source) != "eccc" || strings.TrimSpace(loc.ID) == "" {
				continue
			}
			payload, err := fetchAQHI(ctx, client, loc.ID)
			if err != nil {
				log.Printf("ECCC AQHI fetch failed for %s: %v", loc.ID, err)
				continue
			}
			if err := persistAirQuality(ctx, store, loc, payload); err != nil {
				log.Printf("air quality store failed for %s: %v", loc.ID, err)
				continue
			}
			publishLocationDiscovery(publisher, loc, payload, "air_quality_station", "air_quality")
			publishDataReady(publisher, feed.ID, "air_quality", loc.ID, loc.CanonicalID)
		}
		for _, loc := range feed.Locations.ClimateLocations.Locations {
			if err := ctx.Err(); err != nil {
				return err
			}
			if sourceKind(loc.Source) != "eccc" || strings.TrimSpace(loc.ID) == "" {
				continue
			}
			payload, err := fetchClimateSummary(ctx, client, loc, feed.Timezone, time.Now().UTC())
			if err != nil {
				log.Printf("ECCC climate summary fetch failed for %s: %v", loc.ID, err)
				continue
			}
			if err := persistClimateSummary(ctx, store, loc, payload); err != nil {
				log.Printf("climate summary store failed for %s: %v", loc.ID, err)
				continue
			}
			publishLocationDiscovery(publisher, loc, payload, "climate_station", "climate")
			publishDataReady(publisher, feed.ID, "climate_summary", loc.ID, loc.CanonicalID)
		}
		for _, loc := range feedMarineForecastLocations(feed) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if sourceKind(loc.Source) != "eccc" || strings.TrimSpace(loc.ID) == "" {
				continue
			}
			payload, err := fetchMarineForecast(ctx, client, loc.ID)
			if err != nil {
				log.Printf("ECCC marine forecast fetch failed for %s: %v", loc.ID, err)
				continue
			}
			if err := persistMarineForecast(ctx, store, loc, payload); err != nil {
				log.Printf("marine forecast store failed for %s: %v", loc.ID, err)
				continue
			}
			publishDataReady(publisher, feed.ID, "marine_forecast", loc.ID, loc.CanonicalID)
		}
		fetchFeedSpecialtyProducts(ctx, client, publisher, store, feed, ecccCache)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if text, err := fetchText(ctx, client, "https://services.swpc.noaa.gov/text/wwv.txt"); err == nil && strings.TrimSpace(text) != "" {
		if err := store.StoreTextProduct(ctx, datastore.TextProductRecord{Source: "nws", ID: "wwv", Text: text, Metadata: map[string]any{"source_url": "https://services.swpc.noaa.gov/text/wwv.txt"}}); err == nil {
			publishDataReady(publisher, "", "geophysical_alert", "wwv")
		} else {
			log.Printf("geophysical alert store failed: %v", err)
		}
	}
	return nil
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
	feedsPath := resolvePath(baseDir, fallbackText(root.FeedsFile, "managed/configs/feeds.xml"))
	feedsRaw, err := os.ReadFile(filepath.Clean(feedsPath))
	if err != nil {
		return loadedConfig{}, err
	}
	feedsRaw = []byte(os.ExpandEnv(string(feedsRaw)))
	var feeds feedsXML
	if err := xml.Unmarshal(feedsRaw, &feeds); err != nil {
		return loadedConfig{}, err
	}
	return loadedConfig{
		Root:          root,
		Feeds:         feeds.Feeds,
		ForecastNames: loadForecastRegionNamesForBase(baseDir),
		BaseDir:       baseDir,
	}, nil
}

func loadForecastRegionNamesForBase(baseDir string) map[string]forecastRegionName {
	names := loadForecastRegionNamesFromSQLite(baseDir)
	if len(names) > 0 {
		return names
	}
	return loadForecastRegionNames(resolvePath(baseDir, "managed/csv/FORECAST_LOCATIONS.csv"))
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

func (cfg loadedConfig) enabledFeeds() []feedXML {
	out := make([]feedXML, 0, len(cfg.Feeds))
	for _, feed := range cfg.Feeds {
		if strings.TrimSpace(feed.ID) != "" && xmlBool(feed.EnabledRaw, true) {
			out = append(out, feed)
		}
	}
	return out
}

func fetchECCCCitypage(ctx context.Context, client *http.Client, locationID string) (map[string]any, error) {
	url := fmt.Sprintf("https://api.weather.gc.ca/collections/citypageweather-realtime/items/%s?f=json", locationID)
	var raw map[string]any
	return raw, fetchJSON(ctx, client, url, &raw)
}

func fetchObservation(ctx context.Context, client *http.Client, loc locationXML) (map[string]any, error) {
	switch sourceKind(loc.Source) {
	case "eccc":
		switch ecccObservationIDKind(loc.ID) {
		case "swob", "point":
			raw, err := fetchECCCSWOBObservation(ctx, client, loc.ID)
			if err != nil {
				return nil, err
			}
			return buildECCCSWOBConditions(raw, loc), nil
		default:
			raw, err := fetchECCCCitypage(ctx, client, loc.ID)
			if err != nil {
				return nil, err
			}
			payload := buildECCCConditions(raw)
			payload["_raw_citypage"] = raw
			return payload, nil
		}
	case "nws":
		if raw, err := fetchTWCObservationWithMetadata(ctx, client, loc.ID); err == nil {
			payload := buildTWCConditions(raw, loc)
			payload["source"] = "nws"
			payload["_source_api"] = "api.weather.com"
			return payload, nil
		}
		raw, err := fetchNWSObservation(ctx, client, loc.ID)
		if err != nil {
			return nil, err
		}
		return buildNWSConditions(raw, loc), nil
	case "twc":
		raw, err := fetchTWCObservationWithMetadata(ctx, client, loc.ID)
		if err != nil {
			return nil, err
		}
		return buildTWCConditions(raw, loc), nil
	default:
		return nil, fmt.Errorf("unsupported observation source %q", loc.Source)
	}
}

func fetchAviationObservation(ctx context.Context, client *http.Client, loc locationXML) (map[string]any, error) {
	if sourceKind(loc.Source) != "eccc" {
		return fetchObservation(ctx, client, loc)
	}
	raw, err := fetchECCCSWOBObservation(ctx, client, loc.ID)
	if err != nil {
		return nil, err
	}
	return buildECCCSWOBConditions(raw, loc), nil
}

func swobStationIDFromCode(code string) string {
	cleaned := strings.ToUpper(strings.TrimSpace(code))
	cleaned = strings.NewReplacer(" ", "", "-", "", "_", "").Replace(cleaned)
	if cleaned == "" {
		return ""
	}
	if len(cleaned) == 4 {
		return cleaned
	}
	if len(cleaned) == 3 {
		return "C" + cleaned
	}
	return cleaned
}

func ecccObservationIDKind(id string) string {
	cleaned := strings.TrimSpace(id)
	if cleaned == "" {
		return "citypage"
	}
	if _, _, ok := parseECCCCoordinateID(cleaned); ok {
		return "point"
	}
	if isECCCCitypageID(cleaned) || isECCCCLCID(cleaned) {
		return "citypage"
	}
	return "swob"
}

func isECCCCitypageID(id string) bool {
	parts := strings.Split(strings.TrimSpace(id), "-")
	if len(parts) != 2 || len(parts[0]) != 2 || parts[1] == "" {
		return false
	}
	for _, char := range parts[0] {
		if char < 'A' || char > 'Z' {
			if char < 'a' || char > 'z' {
				return false
			}
		}
	}
	for _, char := range parts[1] {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func isECCCCLCID(id string) bool {
	cleaned := strings.TrimSpace(id)
	if len(cleaned) != 6 {
		return false
	}
	for _, char := range cleaned {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func parseECCCCoordinateID(id string) (float64, float64, bool) {
	cleaned := strings.TrimSpace(id)
	cleaned = strings.NewReplacer(";", ",", " ", ",").Replace(cleaned)
	parts := strings.Split(cleaned, ",")
	values := []string{}
	for _, part := range parts {
		if text := strings.TrimSpace(part); text != "" {
			values = append(values, text)
		}
	}
	if len(values) != 2 {
		return 0, 0, false
	}
	first, err1 := strconv.ParseFloat(values[0], 64)
	second, err2 := strconv.ParseFloat(values[1], 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	if math.Abs(first) <= 90 && math.Abs(second) <= 180 {
		return first, second, true
	}
	if math.Abs(first) <= 180 && math.Abs(second) <= 90 {
		return second, first, true
	}
	return 0, 0, false
}

func buildECCCConditions(raw map[string]any) map[string]any {
	props := mapAt(raw, "properties")
	cc := mapAt(props, "currentConditions")
	wind := mapAt(cc, "wind")
	return map[string]any{
		"source":      "eccc",
		"observed_at": ev(cc, "timestamp"),
		"station":     bilingual(cc, "station", "value"),
		"properties": map[string]any{
			"temp":       ev(cc, "temperature", "value"),
			"condition":  bilingual(cc, "condition"),
			"wind":       map[string]any{"speed": ev(wind, "speed", "value"), "direction": ev(wind, "direction", "value"), "gust": ev(wind, "gust", "value")},
			"humidity":   ev(cc, "relativeHumidity", "value"),
			"dewpoint":   ev(cc, "dewpoint", "value"),
			"visibility": ev(cc, "visibility", "value"),
			"pressure":   map[string]any{"value": ev(cc, "pressure", "value"), "tendency": bilingual(cc, "pressure", "tendency")},
			"windChill":  ev(cc, "windChill", "value"),
			"humidex":    ev(cc, "humidex", "value"),
			"heatIndex":  nil,
		},
	}
}

type swobCollection struct {
	Members []swobMember `xml:"member"`
}

type swobMember struct {
	Observation swobObservation `xml:"Observation"`
}

type swobObservation struct {
	Metadata struct {
		Set struct {
			Identification struct {
				Elements []swobElement `xml:"element"`
			} `xml:"identification-elements"`
		} `xml:"set"`
	} `xml:"metadata"`
	SamplingTime struct {
		TimeInstant struct {
			TimePosition string `xml:"timePosition"`
		} `xml:"TimeInstant"`
	} `xml:"samplingTime"`
	Result struct {
		Elements struct {
			Elements []swobElement `xml:"element"`
		} `xml:"elements"`
	} `xml:"result"`
}

type swobElement struct {
	Name       string          `xml:"name,attr"`
	UOM        string          `xml:"uom,attr"`
	Value      string          `xml:"value,attr"`
	CodeSource string          `xml:"code-src,attr"`
	CodeType   string          `xml:"code-type,attr"`
	Qualifiers []swobQualifier `xml:"qualifier"`
}

type swobQualifier struct {
	Name  string `xml:"name,attr"`
	UOM   string `xml:"uom,attr"`
	Value string `xml:"value,attr"`
}

func fetchECCCSWOBLatest(ctx context.Context, client *http.Client, stationID string) (map[string]any, error) {
	stationID = strings.ToUpper(strings.TrimSpace(stationID))
	if stationID == "" {
		return nil, fmt.Errorf("missing SWOB station id")
	}
	baseURL := "https://dd.weather.gc.ca/today/observations/swob-ml/latest/"
	name, err := latestSWOBFilename(ctx, client, baseURL, stationID)
	if err != nil {
		return nil, err
	}
	raw, err := fetchText(ctx, client, baseURL+url.PathEscape(name))
	if err != nil {
		return nil, err
	}
	var parsed swobCollection
	if err := xml.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, err
	}
	return map[string]any{"_swob": parsed}, nil
}

func fetchECCCSWOBObservation(ctx context.Context, client *http.Client, id string) (map[string]any, error) {
	stationID, err := resolveECCCSWOBStationID(ctx, client, id)
	if err != nil {
		return nil, err
	}
	raw, err := fetchECCCSWOBLatest(ctx, client, stationID)
	if err == nil {
		return raw, nil
	}
	realtimeRaw, realtimeErr := fetchECCCSWOBRealtime(ctx, client, id, stationID)
	if realtimeErr == nil {
		return realtimeRaw, nil
	}
	partnerRaw, partnerErr := fetchECCCSWOBPartner(ctx, client, id, stationID)
	if partnerErr == nil {
		return partnerRaw, nil
	}
	return nil, errors.Join(
		fmt.Errorf("latest SWOB: %w", err),
		fmt.Errorf("realtime SWOB: %w", realtimeErr),
		fmt.Errorf("partner SWOB: %w", partnerErr),
	)
}

type swobPartnerDataset struct {
	ID              string
	NestedByStation bool
}

func fetchECCCSWOBPartner(ctx context.Context, client *http.Client, id string, stationID string) (map[string]any, error) {
	station, err := resolveECCCSWOBPartnerStation(ctx, client, id, stationID)
	if err != nil {
		return nil, err
	}
	dataset, err := swobPartnerDatasetForStation(station)
	if err != nil {
		return nil, err
	}
	return fetchECCCSWOBPartnerFile(ctx, client, swobPartnerBaseURL, station, dataset, time.Now().UTC())
}

func resolveECCCSWOBPartnerStation(ctx context.Context, client *http.Client, id string, stationID string) (map[string]any, error) {
	candidates := uniqueNonBlankStrings(id, strings.ToUpper(strings.TrimSpace(id)), stationID)
	for _, candidate := range candidates {
		requestURL := fmt.Sprintf(
			"https://api.weather.gc.ca/collections/swob-partner-stations/items/%s?f=json",
			url.PathEscape(candidate),
		)
		var feature map[string]any
		if err := fetchJSON(ctx, client, requestURL, &feature); err == nil && len(mapAt(feature, "properties")) > 0 {
			return feature, nil
		}
	}

	for _, field := range []string{"msc_id", "iata_id", "wmo_id"} {
		for _, candidate := range candidates {
			if field == "wmo_id" && !isDigits(candidate) {
				continue
			}
			query := url.Values{}
			query.Set("f", "json")
			query.Set("lang", "en")
			query.Set("limit", "5")
			query.Set(field, candidate)
			requestURL := "https://api.weather.gc.ca/collections/swob-partner-stations/items?" + query.Encode()
			var collection map[string]any
			if err := fetchJSON(ctx, client, requestURL, &collection); err != nil {
				continue
			}
			features := anySlice(collection["features"])
			if len(features) > 0 {
				return mapValue(features[0]), nil
			}
		}
	}
	return nil, fmt.Errorf("no SWOB partner station found for %s", strings.TrimSpace(id))
}

func swobPartnerDatasetForStation(station map[string]any) (swobPartnerDataset, error) {
	props := mapAt(station, "properties")
	mscID := strings.ToUpper(textValue(props["msc_id"]))
	provider := strings.ToUpper(strings.Join([]string{
		textValue(props["data_provider_en"]),
		textValue(props["data_provider_fr"]),
		textValue(props["data_attribution_notice_en"]),
		textValue(props["data_attribution_notice_fr"]),
	}, " "))

	switch {
	case strings.HasPrefix(mscID, "AB-MAI_") ||
		(strings.Contains(provider, "ALBERTA") && strings.Contains(provider, "AGRICULTURE")):
		return swobPartnerDataset{ID: "ab_agriculture", NestedByStation: true}, nil
	case strings.Contains(provider, "CANADIAN COAST GUARD") ||
		strings.Contains(provider, "NATIONAL DEFENCE") ||
		strings.Contains(provider, "DÉFENSE NATIONALE"):
		return swobPartnerDataset{ID: "ccg_lighthouse", NestedByStation: true}, nil
	case strings.HasPrefix(mscID, "YT-DE-WRB_") ||
		(strings.Contains(provider, "YUKON") && strings.Contains(provider, "WATER RESOURCES")):
		return swobPartnerDataset{ID: "yt_water"}, nil
	default:
		return swobPartnerDataset{}, fmt.Errorf(
			"unsupported SWOB partner dataset for %s",
			firstNonBlank(textValue(props["msc_id"]), textValue(station["id"])),
		)
	}
}

func swobPartnerDatasetDirectories(datasetID string, now time.Time) []string {
	switch datasetID {
	case "ab_agriculture":
		return []string{"ab_agriculture"}
	case "ccg_lighthouse":
		cutover := time.Date(2026, time.August, 11, 0, 0, 0, 0, time.UTC)
		if now.UTC().Before(cutover) {
			return []string{"dfo-ccg-lighthouse", "dnd-ccg-lighthouse"}
		}
		return []string{"dnd-ccg-lighthouse", "dfo-ccg-lighthouse"}
	case "yt_water":
		return []string{"yt-water", "yt_water"}
	default:
		return nil
	}
}

func fetchECCCSWOBPartnerFile(
	ctx context.Context,
	client *http.Client,
	rootURL string,
	station map[string]any,
	dataset swobPartnerDataset,
	now time.Time,
) (map[string]any, error) {
	aliases := swobPartnerStationAliases(station)
	if len(aliases) == 0 {
		return nil, fmt.Errorf("SWOB partner station has no usable identifiers")
	}
	directories := swobPartnerDatasetDirectories(dataset.ID, now)
	if len(directories) == 0 {
		return nil, fmt.Errorf("SWOB partner dataset %q has no directory mapping", dataset.ID)
	}
	rootURL = strings.TrimRight(rootURL, "/") + "/"
	for _, observedDay := range []time.Time{now.UTC(), now.UTC().AddDate(0, 0, -1)} {
		date := observedDay.Format("20060102")
		for _, directory := range directories {
			baseURL := rootURL + url.PathEscape(directory) + "/" + date + "/"
			if dataset.NestedByStation {
				listing, err := fetchText(ctx, client, baseURL)
				if err != nil {
					continue
				}
				stationDirectory, ok := swobPartnerStationDirectoryFromListing(listing, aliases)
				if !ok {
					continue
				}
				baseURL += url.PathEscape(stationDirectory) + "/"
			}

			listing, err := fetchText(ctx, client, baseURL)
			if err != nil {
				continue
			}
			name, ok := latestSWOBPartnerFilenameFromListing(listing, aliases)
			if !ok {
				continue
			}
			raw, err := fetchText(ctx, client, baseURL+url.PathEscape(name))
			if err != nil {
				continue
			}
			var parsed swobCollection
			if err := xml.Unmarshal([]byte(raw), &parsed); err != nil {
				return nil, fmt.Errorf("parse SWOB partner file %s: %w", name, err)
			}
			return map[string]any{"_swob": parsed}, nil
		}
	}
	return nil, fmt.Errorf(
		"no current SWOB partner file found for %s in %s",
		aliases[0],
		strings.Join(directories, ", "),
	)
}

func swobPartnerStationAliases(station map[string]any) []string {
	props := mapAt(station, "properties")
	return uniqueNonBlankStrings(
		textValue(props["iata_id"]),
		textValue(props["msc_id"]),
		textValue(props["wmo_id"]),
		textValue(station["id"]),
	)
}

func swobPartnerStationDirectoryFromListing(listing string, aliases []string) (string, bool) {
	for _, token := range strings.Split(listing, "\"") {
		href := strings.TrimSpace(token)
		pathPart := strings.SplitN(href, "?", 2)[0]
		if !strings.HasSuffix(pathPart, "/") {
			continue
		}
		name := swobListingEntryName(pathPart)
		if name == "" {
			continue
		}
		nameKey := swobIdentifierKey(name)
		for _, alias := range aliases {
			if nameKey != "" && nameKey == swobIdentifierKey(alias) {
				return name, true
			}
		}
	}
	return "", false
}

func latestSWOBPartnerFilenameFromListing(listing string, aliases []string) (string, bool) {
	candidates := []string{}
	for _, token := range strings.Split(listing, "\"") {
		name := swobListingEntryName(strings.TrimSpace(token))
		upper := strings.ToUpper(name)
		if name == "" || !strings.HasSuffix(upper, ".XML") || !strings.Contains(upper, "SWOB") {
			continue
		}
		nameKey := swobIdentifierKey(name)
		for _, alias := range aliases {
			aliasKey := swobIdentifierKey(alias)
			if aliasKey != "" && strings.Contains(nameKey, aliasKey) {
				candidates = append(candidates, name)
				break
			}
		}
	}
	if len(candidates) == 0 {
		return "", false
	}
	sort.Slice(candidates, func(i int, j int) bool {
		return strings.ToUpper(candidates[i]) < strings.ToUpper(candidates[j])
	})
	return candidates[len(candidates)-1], true
}

func swobListingEntryName(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	pathPart := strings.TrimSuffix(parsed.Path, "/")
	if pathPart == "" {
		return ""
	}
	if index := strings.LastIndex(pathPart, "/"); index >= 0 {
		pathPart = pathPart[index+1:]
	}
	name, err := url.PathUnescape(pathPart)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(name)
}

func swobIdentifierKey(raw string) string {
	var builder strings.Builder
	for _, char := range strings.ToUpper(strings.TrimSpace(raw)) {
		if (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') {
			builder.WriteRune(char)
		}
	}
	return builder.String()
}

func uniqueNonBlankStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToUpper(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func resolveECCCSWOBStationID(ctx context.Context, client *http.Client, id string) (string, error) {
	cleaned := strings.TrimSpace(id)
	if cleaned == "" {
		return "", fmt.Errorf("missing SWOB station id")
	}
	if lat, lon, ok := parseECCCCoordinateID(cleaned); ok {
		return resolveECCCSWOBStationByPoint(ctx, client, lat, lon)
	}
	if isDigits(cleaned) {
		if len(cleaned) == 7 {
			if stationID, err := resolveECCCSWOBStationItem(ctx, client, cleaned); err == nil && stationID != "" {
				return stationID, nil
			}
			if stationID, err := resolveECCCSWOBMarineStationItem(ctx, client, cleaned); err == nil && stationID != "" {
				return stationID, nil
			}
		}
		if len(cleaned) == 5 {
			if stationID, err := resolveECCCSWOBStationByWMO(ctx, client, cleaned); err == nil && stationID != "" {
				return stationID, nil
			}
		}
	}
	return swobStationIDFromCode(cleaned), nil
}

func resolveECCCSWOBStationItem(ctx context.Context, client *http.Client, id string) (string, error) {
	return resolveECCCSWOBStationItemFromCollection(ctx, client, "swob-stations", id)
}

func resolveECCCSWOBMarineStationItem(ctx context.Context, client *http.Client, id string) (string, error) {
	return resolveECCCSWOBStationItemFromCollection(ctx, client, "swob-marine-stations", id)
}

func resolveECCCSWOBStationItemFromCollection(ctx context.Context, client *http.Client, collection string, id string) (string, error) {
	requestURL := fmt.Sprintf("https://api.weather.gc.ca/collections/%s/items/%s?f=json", url.PathEscape(strings.TrimSpace(collection)), url.PathEscape(strings.TrimSpace(id)))
	var raw map[string]any
	if err := fetchJSON(ctx, client, requestURL, &raw); err != nil {
		return "", err
	}
	return swobStationIDFromStationFeature(raw), nil
}

func resolveECCCSWOBStationByWMO(ctx context.Context, client *http.Client, wmoID string) (string, error) {
	query := url.Values{}
	query.Set("f", "json")
	query.Set("lang", "en")
	query.Set("limit", "5")
	query.Set("wmo_id", strings.TrimSpace(wmoID))
	requestURL := "https://api.weather.gc.ca/collections/swob-stations/items?" + query.Encode()
	var raw map[string]any
	if err := fetchJSON(ctx, client, requestURL, &raw); err != nil {
		return "", err
	}
	for _, item := range anySlice(raw["features"]) {
		if stationID := swobStationIDFromStationFeature(mapValue(item)); stationID != "" {
			return stationID, nil
		}
	}
	return "", fmt.Errorf("no SWOB station found for WMO %s", strings.TrimSpace(wmoID))
}

func resolveECCCSWOBStationByPoint(ctx context.Context, client *http.Client, lat float64, lon float64) (string, error) {
	const searchRadius = 0.35
	query := url.Values{}
	query.Set("f", "json")
	query.Set("lang", "en")
	query.Set("limit", "100")
	query.Set("bbox", fmt.Sprintf("%.5f,%.5f,%.5f,%.5f", lon-searchRadius, lat-searchRadius, lon+searchRadius, lat+searchRadius))
	requestURL := "https://api.weather.gc.ca/collections/swob-stations/items?" + query.Encode()
	var raw map[string]any
	if err := fetchJSON(ctx, client, requestURL, &raw); err != nil {
		return "", err
	}
	bestID := ""
	bestDistance := math.MaxFloat64
	for _, item := range anySlice(raw["features"]) {
		feature := mapValue(item)
		stationID := swobStationIDFromStationFeature(feature)
		if stationID == "" {
			continue
		}
		geometry := mapAt(feature, "geometry")
		coords := anySlice(geometry["coordinates"])
		if len(coords) < 2 {
			continue
		}
		stationLon, lonOK := numberValue(coords[0])
		stationLat, latOK := numberValue(coords[1])
		if !lonOK || !latOK {
			continue
		}
		distance := haversineKM(lon, lat, stationLon, stationLat)
		if distance < bestDistance {
			bestDistance = distance
			bestID = stationID
		}
	}
	if bestID == "" {
		return "", fmt.Errorf("no SWOB station found near %.4f,%.4f", lat, lon)
	}
	return bestID, nil
}

func fetchECCCSWOBRealtime(ctx context.Context, client *http.Client, id string, stationID string) (map[string]any, error) {
	for _, query := range swobRealtimeQueries(id, stationID) {
		requestURL := "https://api.weather.gc.ca/collections/swob-realtime/items?" + query.Encode()
		var raw map[string]any
		if err := fetchJSON(ctx, client, requestURL, &raw); err != nil {
			continue
		}
		features := anySlice(raw["features"])
		if len(features) == 0 {
			continue
		}
		return map[string]any{"_swob": swobCollectionFromRealtimeFeature(mapValue(features[0]))}, nil
	}
	return nil, fmt.Errorf("no SWOB realtime row found for %s", strings.TrimSpace(id))
}

func swobRealtimeQueries(id string, stationID string) []url.Values {
	candidates := []struct {
		field string
		value string
	}{}
	add := func(field string, value string) {
		value = strings.TrimSpace(value)
		if field != "" && value != "" {
			candidates = append(candidates, struct {
				field string
				value string
			}{field: field, value: value})
		}
	}
	cleanedID := strings.TrimSpace(id)
	cleanedStationID := strings.ToUpper(strings.TrimSpace(stationID))
	if isDigits(cleanedID) {
		switch len(cleanedID) {
		case 7:
			add("msc_id-value", cleanedID)
		case 5:
			add("wmo_synop_id-value", cleanedID)
			add("wmo_id-value", cleanedID)
		}
	}
	if cleanedStationID != "" {
		add("icao_stn_id-value", cleanedStationID)
		add("icao_id-value", cleanedStationID)
		add("msc_id-value", cleanedStationID)
		if len(cleanedStationID) == 4 && strings.HasPrefix(cleanedStationID, "C") {
			add("tc_id-value", strings.TrimPrefix(cleanedStationID, "C"))
		}
	}
	queries := []url.Values{}
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		key := candidate.field + "=" + candidate.value
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		query := url.Values{}
		query.Set("f", "json")
		query.Set("lang", "en")
		query.Set("limit", "1")
		query.Set("sortby", "-date_tm-value")
		query.Set(candidate.field, candidate.value)
		queries = append(queries, query)
	}
	return queries
}

func swobCollectionFromRealtimeFeature(feature map[string]any) swobCollection {
	props := mapAt(feature, "properties")
	elements := []swobElement{}
	seen := map[string]struct{}{}
	for key, value := range props {
		name := strings.TrimSuffix(key, "-value")
		if strings.Contains(name, "-") || strings.TrimSpace(name) == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		elementValue := textValue(value)
		if withSuffix := textValue(props[name+"-value"]); withSuffix != "" {
			elementValue = withSuffix
		}
		if elementValue == "" {
			continue
		}
		seen[name] = struct{}{}
		elements = append(elements, swobElement{
			Name:  name,
			UOM:   textValue(props[name+"-uom"]),
			Value: elementValue,
		})
	}
	return swobCollection{Members: []swobMember{{
		Observation: swobObservation{
			Metadata: struct {
				Set struct {
					Identification struct {
						Elements []swobElement `xml:"element"`
					} `xml:"identification-elements"`
				} `xml:"set"`
			}{Set: struct {
				Identification struct {
					Elements []swobElement `xml:"element"`
				} `xml:"identification-elements"`
			}{Identification: struct {
				Elements []swobElement `xml:"element"`
			}{Elements: elements}}},
			Result: struct {
				Elements struct {
					Elements []swobElement `xml:"element"`
				} `xml:"elements"`
			}{Elements: struct {
				Elements []swobElement `xml:"element"`
			}{Elements: elements}},
		},
	}}}
}

func swobStationIDFromStationFeature(feature map[string]any) string {
	props := mapAt(feature, "properties")
	return swobStationIDFromCode(firstNonBlank(
		textValue(props["icao_id"]),
		textValue(props["iata_id"]),
		textValue(props["tc_id"]),
		textValue(props["msc_id"]),
		textValue(props["wmo_id"]),
		textValue(feature["id"]),
	))
}

func latestSWOBFilename(ctx context.Context, client *http.Client, baseURL string, stationID string) (string, error) {
	listing, err := fetchText(ctx, client, baseURL)
	if err != nil {
		return "", err
	}
	candidates := []string{}
	for _, token := range strings.Split(listing, "\"") {
		name := strings.TrimSpace(token)
		upper := strings.ToUpper(name)
		if strings.HasPrefix(upper, stationID+"-") && strings.HasSuffix(upper, "-SWOB.XML") {
			candidates = append(candidates, name)
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no latest SWOB file found for %s", stationID)
	}
	for _, name := range candidates {
		if strings.Contains(strings.ToUpper(name), "-MAN-SWOB.XML") {
			return name, nil
		}
	}
	for _, name := range candidates {
		if strings.Contains(strings.ToUpper(name), "-AUTO-SWOB.XML") {
			return name, nil
		}
	}
	return candidates[0], nil
}

func buildECCCSWOBConditions(raw map[string]any, loc locationXML) map[string]any {
	collection, _ := raw["_swob"].(swobCollection)
	if len(collection.Members) == 0 {
		return map[string]any{"source": "eccc", "station_id": loc.ID}
	}
	obs := collection.Members[0].Observation
	ident := swobElementMap(obs.Metadata.Set.Identification.Elements)
	values := swobElementMap(obs.Result.Elements.Elements)
	stationName := firstNonBlank(loc.NameOverride, swobString(ident, "stn_nam"), loc.ID)
	observedAt := firstNonBlank(swobString(ident, "date_tm"), obs.SamplingTime.TimeInstant.TimePosition)
	windDirection := ""
	if degrees, ok := swobFirstFloat(values,
		"avg_wnd_dir_10m_pst2mts",
		"avg_wnd_dir_10m_pst10mts",
		"avg_wnd_dir_10m_pst1mt",
		"avg_wnd_dir_10m_pst1hr",
		"avg_wnd_dir_10m_mt50-60",
	); ok {
		windDirection, _ = degreesToCardinal(degrees, true).(string)
	}
	pressureKPA := any(nil)
	if hpa, ok := swobFloat(values, "mslp"); ok {
		pressureKPA = math.Round((hpa/10)*100) / 100
	} else if hpa, ok := swobFloat(values, "stn_pres"); ok {
		pressureKPA = math.Round((hpa/10)*100) / 100
	}
	condition := swobECCCCondition(values)
	return map[string]any{
		"source":      "eccc",
		"station_id":  swobStationIdentifier(ident, loc.ID),
		"observed_at": observedAt,
		"station":     map[string]any{"en": stationName},
		"properties": map[string]any{
			"temp":          swobNullable(values, "air_temp"),
			"condition":     map[string]any{"en": condition},
			"sky_condition": map[string]any{"en": swobSkyCondition(values)},
			"altimeter":     swobAltimeter(values),
			"wind": map[string]any{
				"speed": swobFirstNullable(values,
					"avg_wnd_spd_10m_pst2mts",
					"avg_wnd_spd_10m_pst10mts",
					"avg_wnd_spd_10m_pst1mt",
					"avg_wnd_spd_10m_pst1hr",
					"avg_wnd_spd_10m_mt58-60",
					"avg_wnd_spd_10m_mt50-60",
				),
				"direction": windDirection,
				"gust": swobFirstNullable(values,
					"max_wnd_gst_spd_10m_pst10mts",
					"max_wnd_gst_spd_10m_mt50-60",
				),
			},
			"humidity":   swobNullable(values, "rel_hum"),
			"dewpoint":   swobNullable(values, "dwpt_temp"),
			"visibility": swobFirstNullable(values, "vis", "avg_vis_pst10mts", "min_vis", "max_vis"),
			"pressure":   map[string]any{"value": pressureKPA, "tendency": map[string]any{"en": swobPressureTendency(values)}},
			"windChill":  nil,
			"humidex":    nil,
			"heatIndex":  nil,
		},
	}
}

func swobElementMap(elements []swobElement) map[string]swobElement {
	out := make(map[string]swobElement, len(elements))
	for _, element := range elements {
		name := strings.TrimSpace(element.Name)
		if name != "" {
			out[name] = element
			out[strings.ToLower(name)] = element
		}
	}
	return out
}

func swobString(elements map[string]swobElement, name string) string {
	value := strings.TrimSpace(elements[name].Value)
	if value == "" || strings.EqualFold(value, "MSNG") {
		return ""
	}
	return value
}

func swobFloat(elements map[string]swobElement, name string) (float64, bool) {
	value := swobString(elements, name)
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func swobFirstFloat(elements map[string]swobElement, names ...string) (float64, bool) {
	for _, name := range names {
		if value, ok := swobFloat(elements, name); ok {
			return value, true
		}
	}
	return 0, false
}

func swobNullable(elements map[string]swobElement, name string) any {
	value, ok := swobFloat(elements, name)
	if !ok {
		return nil
	}
	return value
}

func swobFirstNullable(elements map[string]swobElement, names ...string) any {
	if value, ok := swobFirstFloat(elements, names...); ok {
		return value
	}
	return nil
}

func swobStationIdentifier(elements map[string]swobElement, fallback string) string {
	return firstNonBlank(
		swobString(elements, "icao_stn_id"),
		swobString(elements, "tc_id"),
		swobString(elements, "msc_id"),
		swobString(elements, "clim_id"),
		swobString(elements, "wmo_synop_id"),
		fallback,
	)
}

func swobAltimeter(elements map[string]swobElement) string {
	value, ok := swobFloat(elements, "altmetr_setng")
	if !ok {
		return ""
	}
	return fmt.Sprintf("%.2f inches", value)
}

func swobECCCCondition(elements map[string]swobElement) string {
	if condition := swobPresentWeather(elements); condition != "" {
		return condition
	}
	return swobCloudCondition(elements)
}

func swobPresentWeather(elements map[string]swobElement) string {
	for _, name := range swobPresentWeatherFields() {
		raw := swobString(elements, name)
		if raw == "" {
			continue
		}
		text, ok := swobPresentWeatherText(raw)
		if ok && text != "" {
			return text
		}
	}
	return ""
}

func swobCloudCondition(elements map[string]swobElement) string {
	for index := 1; index <= 8; index++ {
		if text := swobCloudConditionText(swobString(elements, fmt.Sprintf("cld_amt_code_%d", index))); text != "" {
			return text
		}
	}
	return swobCloudConditionText(firstNonBlank(
		swobString(elements, "tot_cld_amt"),
		swobString(elements, "total_cloud_amount"),
		swobString(elements, "cld_amt_code"),
	))
}

func swobCloudConditionText(raw string) string {
	code, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	switch code {
	case 0, 42, 45, 50:
		return "Clear"
	case 1, 10, 32, 33, 43, 48, 51, 52, 54:
		return "Mainly Sunny"
	case 2, 34, 35, 55:
		return "Partly Cloudy"
	case 3, 36, 37, 38, 56:
		return "Mostly Cloudy"
	case 4, 6, 7, 8, 9, 11, 12, 13, 14, 15, 39, 40, 44, 46, 47, 49, 57:
		return "Cloudy"
	default:
		return ""
	}
}

func swobSkyCondition(elements map[string]swobElement) string {
	parts := []string{}
	for index := 1; index <= 8; index++ {
		amount := swobString(elements, fmt.Sprintf("cld_amt_code_%d", index))
		height, ok := swobFloat(elements, fmt.Sprintf("cld_bas_hgt_%d", index))
		amountText, amountOK := swobCloudAmountText(amount)
		if amountOK {
			if amountText == "" {
				continue
			}
			if ok {
				feet := swobMetersToRoundedFeet(height)
				if feet > 0 && swobCloudAmountUsesHeight(amount) {
					amountText = fmt.Sprintf("%s at %d feet", amountText, feet)
				}
			}
			parts = appendUniqueString(parts, amountText)
			continue
		}
		if !ok {
			continue
		}
		feet := swobMetersToRoundedFeet(height)
		if feet <= 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("cloud layer at %d feet", feet))
	}
	if len(parts) == 0 {
		if amountText, ok := swobCloudAmountText(firstNonBlank(swobString(elements, "tot_cld_amt"), swobString(elements, "total_cloud_amount"), swobString(elements, "cld_amt_code"))); ok && amountText != "" {
			return amountText
		}
		if vertical, ok := swobFloat(elements, "vert_vis"); ok {
			feet := swobMetersToRoundedFeet(vertical)
			if feet > 0 {
				return fmt.Sprintf("vertical visibility %d feet", feet)
			}
		}
		return ""
	}
	return strings.Join(parts, ", ")
}

func swobPresentWeatherFields() []string {
	fields := []string{"prsnt_wx"}
	for index := 1; index <= 8; index++ {
		fields = append(fields, fmt.Sprintf("prsnt_wx_%d", index))
	}
	return fields
}

func swobPresentWeatherText(raw string) (string, bool) {
	code, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimSpace(raw), strings.TrimSpace(raw) != ""
	}
	if text, ok := swobECCCConditionCodeText[code]; ok {
		return text, true
	}
	switch code {
	case 0, 125, 300, 409, 808, 839:
		return "", true
	case 809, 810, 811, 414:
		return "", false
	default:
		return "", false
	}
}

var swobECCCConditionCodeText = map[int]string{
	4:   "Smoke",
	5:   "Haze",
	10:  "Mist",
	13:  "Thunderstorm",
	17:  "Thunderstorm",
	18:  "Squalls",
	19:  "Funnel Cloud",
	20:  "Drizzle",
	21:  "Rain",
	22:  "Snow",
	23:  "Rain and Snow",
	24:  "Freezing Rain",
	25:  "Rain Shower",
	26:  "Flurries",
	27:  "Hail",
	28:  "Fog",
	29:  "Thunderstorm",
	36:  "Drifting Snow",
	37:  "Drifting Snow",
	38:  "Blowing Snow",
	39:  "Blowing Snow",
	40:  "Fog",
	41:  "Fog Patches",
	42:  "Fog",
	43:  "Fog",
	44:  "Fog",
	45:  "Fog",
	46:  "Fog",
	47:  "Fog",
	48:  "Fog",
	49:  "Fog",
	50:  "Light Drizzle",
	51:  "Light Drizzle",
	52:  "Drizzle",
	53:  "Heavy Drizzle",
	54:  "Light Drizzle",
	55:  "Drizzle",
	56:  "Heavy Drizzle",
	57:  "Light Freezing Drizzle",
	58:  "Light Freezing Drizzle",
	59:  "Freezing Drizzle",
	60:  "Heavy Freezing Drizzle",
	61:  "Freezing Drizzle",
	62:  "Light Rain and Drizzle",
	63:  "Heavy Rain and Drizzle",
	64:  "Light Rain",
	65:  "Light Rain",
	66:  "Rain",
	67:  "Heavy Rain",
	68:  "Light Rain",
	69:  "Rain",
	70:  "Heavy Rain",
	71:  "Light Freezing Rain",
	72:  "Light Freezing Rain",
	73:  "Freezing Rain",
	74:  "Heavy Freezing Rain",
	75:  "Freezing Rain",
	76:  "Light Rain and Snow",
	77:  "Heavy Rain and Snow",
	78:  "Light Snow",
	79:  "Light Snow",
	80:  "Snow",
	81:  "Heavy Snow",
	82:  "Light Snow",
	83:  "Snow",
	84:  "Heavy Snow",
	85:  "Ice Crystals",
	86:  "Snow Grains",
	87:  "Snow Grains",
	88:  "Snow Grains",
	89:  "Snow Grains",
	90:  "Snow Grains",
	91:  "Ice Crystals",
	92:  "Ice Pellets",
	93:  "Ice Pellets",
	94:  "Ice Pellets",
	95:  "Ice Pellets",
	96:  "Ice Pellets",
	97:  "Light Rain Shower",
	98:  "Light Rain Shower",
	99:  "Rain Shower",
	100: "Heavy Rain Shower",
	101: "Rain Shower",
	102: "Light Rain Shower and Flurries",
	103: "Heavy Rain Shower and Flurries",
	104: "Light Flurries",
	105: "Light Flurries",
	106: "Flurries",
	107: "Heavy Flurries",
	108: "Flurries",
	109: "Snow Pellets",
	110: "Snow Pellets",
	111: "Hail",
	112: "Hail",
	113: "Hail",
	114: "Hail",
	115: "Hail",
	116: "Thunderstorm with Light Rain",
	117: "Thunderstorm with Heavy Rain",
	118: "Thunderstorm with Rain",
	119: "Thunderstorm with Rain",
	120: "Thunderstorm with Hail",
	121: "Heavy Thunderstorm with Hail",
	122: "Thunderstorm with Dust Storm",
	123: "Thunderstorm with Light Rain",
	124: "Thunderstorm with Heavy Rain",
	126: "Blowing Dust",
	127: "Sandstorm",
	128: "Blowing Snow",
	129: "Dust Storm",
	130: "Sandstorm",
	131: "Funnel Cloud",
	132: "Tornado",
	133: "Waterspout",
	134: "Drifting Dust",
	135: "Sandstorm",
	136: "Drifting Snow",
	137: "Precipitation",
	138: "Fog",
	139: "Ice Fog",
	140: "Shallow Fog",
	141: "Ice Fog",
	142: "Fog Patches",
	143: "Fog",
	144: "Smoke",
	145: "Thunderstorm",
	146: "Heavy Thunderstorm",
	147: "Dust Devils",
	148: "Snow Pellets",
	149: "Snow Pellets",
	150: "Snow Pellets",
	151: "Snow Pellets",
	152: "Ice Pellets",
	153: "Ice Pellets",
	154: "Ice Pellets",
	155: "Ice Pellets",
	156: "Dust Storm",
	157: "Dust Storm",
	158: "Dust Storm",
	159: "Volcanic Ash",
	160: "Haze",
	161: "Rain",
	162: "Rain Shower",
	163: "Fog",
	164: "Ice Fog",
	165: "Thunderstorm",
	166: "Dust Storm",
	167: "Sandstorm",
	168: "Funnel Cloud",
	169: "Tornado",
	170: "Waterspout",
	171: "Rain",
	172: "Drizzle",
	173: "Snow",
	174: "Snow Grains",
	175: "Ice Crystals",
	176: "Ice Pellets",
	177: "Hail",
	178: "Snow Pellets",
	179: "Freezing Rain",
	180: "Freezing Drizzle",
	181: "Thunderstorm",
	182: "Blowing Snow",
	183: "Volcanic Ash",
	184: "Dust Storm",
	304: "Haze",
	305: "Haze",
	310: "Mist",
	311: "Ice Crystals",
	312: "Thunderstorm",
	318: "Squalls",
	320: "Fog",
	321: "Precipitation",
	322: "Drizzle",
	323: "Rain",
	324: "Snow",
	325: "Freezing Rain",
	326: "Thunderstorm",
	327: "Blowing Snow",
	328: "Blowing Snow",
	329: "Blowing Snow",
	330: "Fog",
	331: "Fog Patches",
	332: "Fog",
	333: "Fog",
	334: "Fog",
	335: "Ice Fog",
	340: "Precipitation",
	341: "Light Precipitation",
	342: "Heavy Precipitation",
	343: "Light Precipitation",
	344: "Heavy Precipitation",
	345: "Light Snow",
	346: "Heavy Snow",
	347: "Light Freezing Rain",
	348: "Heavy Freezing Rain",
	350: "Drizzle",
	351: "Light Drizzle",
	352: "Light Drizzle",
	353: "Drizzle",
	354: "Heavy Drizzle",
	355: "Light Freezing Drizzle",
	356: "Light Freezing Drizzle",
	357: "Freezing Drizzle",
	358: "Heavy Freezing Drizzle",
	359: "Light Rain and Drizzle",
	360: "Heavy Rain and Drizzle",
	362: "Rain",
	363: "Light Rain",
	364: "Light Rain",
	365: "Rain",
	366: "Heavy Rain",
	367: "Light Freezing Rain",
	368: "Light Freezing Rain",
	369: "Freezing Rain",
	370: "Heavy Freezing Rain",
	371: "Light Rain and Snow",
	372: "Heavy Rain and Snow",
	374: "Snow",
	375: "Light Snow",
	376: "Light Snow",
	377: "Snow",
	378: "Heavy Snow",
	379: "Ice Pellets",
	380: "Ice Pellets",
	381: "Ice Pellets",
	382: "Snow Grains",
	383: "Ice Crystals",
	385: "Precipitation",
	386: "Light Rain Shower",
	387: "Light Rain Shower",
	388: "Rain Shower",
	389: "Heavy Rain Shower",
	390: "Light Flurries",
	391: "Flurries",
	392: "Heavy Flurries",
	394: "Hail",
	395: "Hail",
	396: "Hail",
	397: "Hail",
	398: "Hail",
	399: "Thunderstorm",
	400: "Thunderstorm with Rain",
	401: "Thunderstorm with Heavy Rain",
	402: "Thunderstorm",
	403: "Thunderstorm with Light Rain",
	404: "Thunderstorm with Heavy Rain",
	405: "Heavy Thunderstorm",
	408: "Tornado",
	410: "Precipitation",
	411: "Light Precipitation",
	412: "Precipitation",
	413: "Heavy Precipitation",
	415: "Snow",
	416: "Light Snow",
	417: "Snow",
	418: "Heavy Snow",
}

var swobPresentWeatherCodeText = map[int]string{
	4:   "smoke",
	5:   "haze",
	10:  "mist",
	13:  "lightning visible, no thunder heard",
	17:  "thunderstorm without precipitation",
	18:  "squalls",
	19:  "funnel cloud",
	20:  "drizzle or snow grains",
	21:  "rain",
	22:  "snow",
	23:  "rain and snow or ice pellets",
	24:  "freezing drizzle or freezing rain",
	25:  "showers of rain",
	26:  "showers of snow or rain and snow",
	27:  "showers of hail or rain and hail",
	28:  "fog or ice fog",
	29:  "thunderstorm",
	36:  "slight drifting snow",
	37:  "moderate or heavy drifting snow",
	38:  "slight blowing snow",
	39:  "moderate or heavy blowing snow",
	40:  "fog at a distance",
	41:  "fog patches",
	42:  "fog has become thinner",
	43:  "fog, no appreciable change",
	44:  "fog has begun or become thicker",
	45:  "fog depositing rime",
	46:  "fog or ice fog, sky visible",
	47:  "fog or ice fog, sky not visible",
	48:  "fog depositing rime, sky visible",
	49:  "fog depositing rime, sky not visible",
	50:  "very light drizzle",
	51:  "light drizzle",
	52:  "moderate drizzle",
	53:  "heavy drizzle",
	54:  "light intermittent drizzle",
	55:  "moderate intermittent drizzle",
	56:  "heavy intermittent drizzle",
	57:  "very light freezing drizzle",
	58:  "light freezing drizzle",
	59:  "moderate freezing drizzle",
	60:  "heavy freezing drizzle",
	61:  "moderate or heavy freezing drizzle",
	62:  "light drizzle and rain",
	63:  "moderate or heavy drizzle and rain",
	64:  "very light rain",
	65:  "light rain",
	66:  "moderate rain",
	67:  "heavy rain",
	68:  "light intermittent rain",
	69:  "moderate intermittent rain",
	70:  "heavy intermittent rain",
	71:  "very light freezing rain",
	72:  "light freezing rain",
	73:  "moderate freezing rain",
	74:  "heavy freezing rain",
	75:  "moderate or heavy freezing rain",
	76:  "light rain or drizzle and snow",
	77:  "moderate or heavy rain or drizzle and snow",
	78:  "very light snow",
	79:  "light snow",
	80:  "moderate snow",
	81:  "heavy snow",
	82:  "light intermittent snow",
	83:  "moderate intermittent snow",
	84:  "heavy intermittent snow",
	85:  "ice crystals",
	86:  "snow grains",
	87:  "very light snow grains",
	88:  "light snow grains",
	89:  "moderate snow grains",
	90:  "heavy snow grains",
	91:  "snow crystals",
	92:  "ice pellets",
	93:  "very light ice pellets",
	94:  "light ice pellets",
	95:  "moderate ice pellets",
	96:  "heavy ice pellets",
	97:  "very light rain showers",
	98:  "light rain showers",
	99:  "moderate rain showers",
	100: "heavy rain showers",
	101: "moderate or heavy rain showers",
	102: "light mixed rain and snow showers",
	103: "moderate or heavy mixed rain and snow showers",
	104: "very light snow showers",
	105: "light snow showers",
	106: "moderate snow showers",
	107: "heavy snow showers",
	108: "moderate or heavy snow showers",
	109: "light showers of snow pellets or small hail",
	110: "moderate or heavy showers of snow pellets or small hail",
	111: "very light hail",
	112: "light showers of hail",
	113: "moderate hail",
	114: "heavy hail",
	115: "moderate or heavy showers of hail",
	116: "thunderstorm with slight rain",
	117: "thunderstorm with moderate or heavy rain",
	118: "thunderstorm with slight snow or rain and snow",
	119: "thunderstorm with moderate or heavy snow or rain and snow",
	120: "thunderstorm with slight hail",
	121: "thunderstorm with moderate or heavy hail",
	122: "thunderstorm with duststorm or sandstorm",
	123: "thunderstorm with slight freezing rain",
	124: "thunderstorm with moderate or heavy freezing rain",
	126: "blowing dust",
	127: "blowing sand",
	128: "blowing snow",
	129: "dust storm",
	130: "sand storm",
	131: "funnel cloud",
	132: "tornado",
	133: "waterspout",
	134: "low drifting dust",
	135: "low drifting sand",
	136: "low drifting snow",
	137: "blowing spray",
	138: "fog",
	139: "freezing fog",
	140: "shallow fog",
	141: "ice fog",
	142: "patchy fog",
	143: "fog covering part of the aerodrome",
	144: "smoke",
	145: "thunderstorm",
	146: "heavy thunderstorm",
	147: "dust whirl or sand whirl",
	148: "very light showers of snow pellets",
	149: "light showers of snow pellets",
	150: "moderate showers of snow pellets",
	151: "heavy showers of snow pellets",
	152: "very light showers of ice pellets",
	153: "light showers of ice pellets",
	154: "moderate showers of ice pellets",
	155: "heavy showers of ice pellets",
	156: "light sandstorm or duststorm",
	157: "moderate sandstorm or duststorm",
	158: "heavy sandstorm or duststorm",
	159: "volcanic ash",
	160: "dust in suspension",
	161: "rain in the vicinity",
	162: "showers in the vicinity",
	163: "fog in the vicinity",
	164: "freezing fog in the vicinity",
	165: "thunderstorm in the vicinity",
	166: "duststorm in the vicinity",
	167: "sandstorm in the vicinity",
	168: "funnel cloud in the vicinity",
	169: "tornado in the vicinity",
	170: "waterspout in the vicinity",
	171: "recent rain",
	172: "recent drizzle",
	173: "recent snow",
	174: "recent snow grains",
	175: "recent ice crystals",
	176: "recent ice pellets",
	177: "recent hail",
	178: "recent snow pellets",
	179: "recent freezing rain",
	180: "recent freezing drizzle",
	181: "recent thunderstorm",
	182: "recent blowing snow",
	183: "recent volcanic ash",
	184: "recent sandstorm or duststorm",
	301: "clouds decreasing",
	302: "clouds unchanged",
	303: "clouds increasing",
	304: "haze, smoke, or dust",
	305: "haze, smoke, or dust with visibility less than one kilometre",
	310: "mist",
	311: "ice crystals",
	312: "distant lightning",
	318: "squalls",
	320: "fog",
	321: "precipitation during the preceding hour but not at the time of observation",
	322: "drizzle or snow grains",
	323: "rain",
	324: "snow",
	325: "freezing drizzle or freezing rain",
	326: "thunderstorm",
	327: "blowing or drifting snow or sand",
	328: "blowing or drifting snow or sand",
	329: "blowing or drifting snow or sand with visibility less than one kilometre",
	330: "fog",
	331: "fog or ice fog patches",
	332: "fog or ice fog thinning",
	333: "fog or ice fog unchanged",
	334: "fog or ice fog thickening",
	335: "freezing fog",
	340: "precipitation",
	341: "light or moderate precipitation",
	342: "heavy precipitation",
	343: "light or moderate liquid precipitation",
	344: "heavy liquid precipitation",
	345: "light or moderate solid precipitation",
	346: "heavy solid precipitation",
	347: "light or moderate freezing precipitation",
	348: "heavy freezing precipitation",
	350: "drizzle",
	351: "very light drizzle",
	352: "light drizzle",
	353: "moderate drizzle",
	354: "heavy drizzle",
	355: "very light freezing drizzle",
	356: "light freezing drizzle",
	357: "moderate freezing drizzle",
	358: "heavy freezing drizzle",
	359: "light drizzle and rain",
	360: "moderate or heavy drizzle and rain",
	362: "rain",
	363: "very light rain",
	364: "light rain",
	365: "moderate rain",
	366: "heavy rain",
	367: "very light freezing rain",
	368: "light freezing rain",
	369: "moderate freezing rain",
	370: "heavy freezing rain",
	371: "light rain or drizzle and snow",
	372: "moderate or heavy rain or drizzle and snow",
	374: "snow",
	375: "very light snow",
	376: "light snow",
	377: "moderate snow",
	378: "heavy snow",
	379: "ice pellets",
	380: "light ice pellets",
	381: "moderate or heavy ice pellets",
	382: "snow grains",
	383: "ice crystals",
	385: "showers or intermittent precipitation",
	386: "very light rain showers",
	387: "light rain showers",
	388: "moderate rain showers",
	389: "heavy rain showers",
	390: "light snow showers",
	391: "moderate snow showers",
	392: "heavy snow showers",
	394: "hail",
	395: "very light hail",
	396: "light hail",
	397: "moderate hail",
	398: "heavy hail",
	399: "thunderstorm",
	400: "thunderstorm with light or moderate precipitation",
	401: "thunderstorm with heavy precipitation",
	402: "thunderstorm without precipitation",
	403: "thunderstorm with light or moderate rain showers",
	404: "thunderstorm with heavy rain showers",
	405: "heavy thunderstorm",
	408: "tornado",
	410: "unclassified precipitation",
	411: "light unclassified precipitation",
	412: "moderate unclassified precipitation",
	413: "heavy unclassified precipitation",
	415: "frozen precipitation",
	416: "light frozen precipitation",
	417: "moderate frozen precipitation",
	418: "heavy frozen precipitation",
}

func swobCloudAmountText(raw string) (string, bool) {
	code, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return "", false
	}
	switch code {
	case 0, 42, 45, 50:
		return "Clear", true
	case 1, 10, 32, 33:
		return "few clouds", true
	case 2, 34, 35:
		return "scattered clouds", true
	case 3, 36, 37, 38:
		return "broken clouds", true
	case 4, 39:
		return "overcast", true
	case 6:
		return "scattered to broken clouds", true
	case 7:
		return "broken to overcast clouds", true
	case 8:
		return "isolated cumulonimbus clouds", true
	case 9:
		return "isolated embedded cumulonimbus clouds", true
	case 11:
		return "occasional embedded cumulonimbus clouds", true
	case 12:
		return "frequent cumulonimbus clouds", true
	case 13:
		return "dense cloud", true
	case 14:
		return "multiple cloud layers", true
	case 15, 40, 44, 46:
		return "obscured", true
	case 41:
		return "", false
	case 43:
		return "no significant cloud", true
	case 47, 49:
		return "partially obscured", true
	case 48:
		return "no clouds detected below 10000 feet", true
	case 51:
		return "no clouds detected below 25000 feet", true
	case 52:
		return "ceiling and visibility OK", true
	case 53, 58:
		return "", true
	case 54:
		return "thin few clouds", true
	case 55:
		return "thin scattered clouds", true
	case 56:
		return "thin broken clouds", true
	case 57:
		return "thin overcast", true
	default:
		return "", false
	}
}

func swobCloudAmountUsesHeight(raw string) bool {
	code, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return true
	}
	switch code {
	case 0, 15, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 58:
		return false
	default:
		return true
	}
}

func swobMetersToRoundedFeet(meters float64) int {
	return int(math.Round(meters*3.28084/100.0) * 100)
}

func swobPressureTendency(elements map[string]swobElement) string {
	codeRaw := swobString(elements, "pres_tend_char_pst3hrs")
	if codeRaw == "" {
		return ""
	}
	code, err := strconv.Atoi(codeRaw)
	if err != nil {
		return ""
	}
	switch code {
	case 1, 2, 3:
		return "rising"
	case 4:
		return "steady"
	case 5, 6, 7, 8:
		return "falling"
	case 16:
		return "rising rapidly"
	case 17:
		return "falling rapidly"
	default:
		return ""
	}
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if strings.EqualFold(existing, value) {
			return values
		}
	}
	return append(values, value)
}

func fetchNWSObservation(ctx context.Context, client *http.Client, stationID string) (map[string]any, error) {
	url := fmt.Sprintf("https://api.weather.gov/stations/%s/observations/latest", stationID)
	var raw map[string]any
	return raw, fetchJSON(ctx, client, url, &raw)
}

func buildNWSConditions(raw map[string]any, loc locationXML) map[string]any {
	props := mapAt(raw, "properties")
	windDirection, hasWindDirection := numberAt(mapAt(props, "windDirection"), "value")
	station := fallbackText(loc.NameOverride, firstNonBlank(textAt(props, "name"), textAt(props, "stationIdentifier"), loc.ID))
	return map[string]any{
		"source":      "nws",
		"observed_at": props["timestamp"],
		"station":     map[string]any{"en": station},
		"properties": map[string]any{
			"temp":      roundedNullable(unitValue(props, "temperature")),
			"condition": map[string]any{"en": textAt(props, "textDescription")},
			"wind": map[string]any{
				"speed":     roundedNullable(speedKPH(props, "windSpeed")),
				"direction": degreesToCardinal(windDirection, hasWindDirection),
				"gust":      roundedNullable(speedKPH(props, "windGust")),
			},
			"humidity":   roundedNullable(unitValue(props, "relativeHumidity")),
			"dewpoint":   roundedNullable(unitValue(props, "dewpoint")),
			"visibility": roundedNullable(visibilityKM(props)),
			"pressure":   map[string]any{"value": roundedNullable(pressureKPA(props)), "tendency": nil},
			"windChill":  roundedNullable(unitValue(props, "windChill")),
			"humidex":    nil,
			"heatIndex":  roundedNullable(unitValue(props, "heatIndex")),
		},
	}
}

func fetchTWCObservation(ctx context.Context, client *http.Client, stationID string) (map[string]any, error) {
	apiKey := strings.TrimSpace(os.Getenv("TWC_API_KEY"))
	if apiKey == "" {
		return nil, fmt.Errorf("TWC_API_KEY is not configured")
	}
	url := fmt.Sprintf("https://api.weather.com/v3/wx/observations/current?icaoCode=%s&units=m&language=en-CA&format=json&apiKey=%s", url.QueryEscape(stationID), url.QueryEscape(apiKey))
	var raw map[string]any
	return raw, fetchJSON(ctx, client, url, &raw)
}

func fetchTWCObservationWithMetadata(ctx context.Context, client *http.Client, stationID string) (map[string]any, error) {
	raw, err := fetchTWCObservation(ctx, client, stationID)
	if err != nil {
		return nil, err
	}
	if name, nameErr := fetchTWCLocationName(ctx, client, stationID); nameErr == nil && strings.TrimSpace(name) != "" {
		raw["_twc_location_name"] = strings.TrimSpace(name)
	}
	return raw, nil
}

func fetchTWCLocationName(ctx context.Context, client *http.Client, stationID string) (string, error) {
	apiKey := strings.TrimSpace(os.Getenv("TWC_API_KEY"))
	if apiKey == "" {
		return "", fmt.Errorf("TWC_API_KEY is not configured")
	}
	url := fmt.Sprintf("https://api.weather.com/v3/location/point?icaoCode=%s&language=en-CA&format=json&apiKey=%s", url.QueryEscape(stationID), url.QueryEscape(apiKey))
	var raw map[string]any
	if err := fetchJSON(ctx, client, url, &raw); err != nil {
		return "", err
	}
	location := mapAt(raw, "location")
	return firstNonBlank(
		textAt(location, "displayName"),
		textAt(location, "city"),
		textAt(location, "neighborhood"),
		textAt(location, "postalCode"),
	), nil
}

func buildTWCConditions(raw map[string]any, loc locationXML) map[string]any {
	pressureHPA, _ := numberAt(raw, "pressureMeanSeaLevel")
	var pressureKPA any
	if pressureHPA > 0 {
		pressureKPA = math.Round(pressureHPA) / 10
	}
	return map[string]any{
		"source":      "twc",
		"observed_at": raw["validTimeLocal"],
		"station":     map[string]any{"en": twcStationName(raw, loc)},
		"properties": map[string]any{
			"temp":       raw["temperature"],
			"condition":  map[string]any{"en": firstNonBlank(textAt(raw, "wxPhraseLong"), textAt(raw, "wxPhraseMedium"))},
			"wind":       map[string]any{"speed": raw["windSpeed"], "direction": raw["windDirectionCardinal"], "gust": raw["windGust"]},
			"humidity":   raw["relativeHumidity"],
			"dewpoint":   raw["temperatureDewPoint"],
			"visibility": raw["visibility"],
			"pressure": map[string]any{
				"value":    pressureKPA,
				"tendency": raw["pressureTendencyTrend"],
			},
			"windChill": raw["temperatureWindChill"],
			"humidex":   nil,
			"heatIndex": raw["temperatureHeatIndex"],
		},
	}
}

func twcStationName(raw map[string]any, loc locationXML) string {
	location := mapAt(raw, "location")
	return firstNonBlank(
		loc.NameOverride,
		textAt(raw, "obsName"),
		textAt(raw, "stationName"),
		textAt(raw, "station_name"),
		textAt(raw, "displayName"),
		textAt(raw, "display_name"),
		textAt(raw, "name"),
		textAt(raw, "_twc_location_name"),
		textAt(location, "displayName"),
		textAt(location, "city"),
		textAt(location, "neighborhood"),
		loc.ID,
	)
}

func buildECCCForecast(raw map[string]any, region coverageRegionXML, forecastNames map[string]forecastRegionName) (map[string]any, bool) {
	props := mapAt(raw, "properties")
	group := mapAt(props, "forecastGroup")
	rawForecasts, _ := group["forecasts"].([]any)
	periods := make([]map[string]any, 0, len(rawForecasts))
	for _, item := range rawForecasts {
		forecast, ok := item.(map[string]any)
		if !ok {
			continue
		}
		enParts := []string{}
		frParts := []string{}
		if text := textLang(mapAt(forecast, "cloudPrecip"), "en"); text != "" {
			enParts = append(enParts, text)
		}
		if text := textLang(mapAt(forecast, "cloudPrecip"), "fr"); text != "" {
			frParts = append(frParts, text)
		}
		for _, key := range []string{"temperatures", "windChill"} {
			summary := mapAt(mapAt(forecast, key), "textSummary")
			if text := textLang(summary, "en"); text != "" {
				enParts = append(enParts, text)
			}
			if text := textLang(summary, "fr"); text != "" {
				frParts = append(frParts, text)
			}
		}
		periods = append(periods, map[string]any{
			"period":      bilingual(forecast, "period", "textForecastName"),
			"textSummary": map[string]any{"en": strings.Join(enParts, " "), "fr": strings.Join(frParts, " ")},
		})
	}
	if len(periods) == 0 {
		return nil, false
	}
	return map[string]any{
		"source":          "eccc",
		"issued_at":       textValue(ev(group, "timestamp")),
		"updated_at":      props["lastUpdated"],
		"forecast":        periods,
		"forecast_region": region.ID,
		"name":            forecastLocationNameBlock(region, forecastNames),
	}, true
}

func forecastLocationNameBlock(region coverageRegionXML, forecastNames map[string]forecastRegionName) map[string]any {
	if names, ok := forecastNames[forecastRegionBaseCode(region.ID)]; ok && (names.English != "" || names.French != "") {
		fallback := firstNonBlank(names.English, names.French, region.ID)
		return map[string]any{
			"en": pauseForecastRegionName(firstNonBlank(names.English, fallback), "en"),
			"fr": pauseForecastRegionName(firstNonBlank(names.French, fallback), "fr"),
		}
	}
	fallback := fallbackText(region.Name, region.ID)
	return map[string]any{"en": pauseForecastRegionName(fallback, "en"), "fr": pauseForecastRegionName(fallback, "fr")}
}

func loadForecastRegionNames(path string) map[string]forecastRegionName {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return map[string]forecastRegionName{}
	}
	defer func() {
		_ = file.Close()
	}()
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

func pauseForecastRegionName(value string, language string) string {
	cleaned := strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	if cleaned == "" {
		return ""
	}
	parts := strings.Split(cleaned, " - ")
	if len(parts) <= 1 {
		return cleaned
	}
	cleanParts := make([]string, 0, len(parts))
	for index, part := range parts {
		part = strings.TrimSpace(strings.Trim(part, " ,."))
		part = strings.TrimSpace(strings.TrimSuffix(part, " region"))
		part = strings.TrimSpace(strings.TrimSuffix(part, " Region"))
		part = strings.TrimSpace(strings.TrimPrefix(part, "and "))
		if index == len(parts)-1 && strings.Contains(part, " and ") {
			for _, child := range strings.Split(part, " and ") {
				child = strings.TrimSpace(strings.Trim(child, " ,."))
				if child != "" {
					cleanParts = append(cleanParts, child)
				}
			}
			continue
		}
		if part != "" {
			cleanParts = append(cleanParts, part)
		}
	}
	if len(cleanParts) <= 1 {
		return cleaned
	}
	if language == "fr" {
		return strings.Join(cleanParts, ". ")
	}
	if len(cleanParts) == 2 {
		return cleanParts[0] + " and " + cleanParts[1] + " region"
	}
	return strings.Join(cleanParts[:len(cleanParts)-1], ", ") + ", and " + cleanParts[len(cleanParts)-1] + " region"
}

func forecastRegionBaseCode(region string) string {
	region = strings.TrimSpace(region)
	if before, _, ok := strings.Cut(region, "-"); ok {
		return strings.TrimSpace(before)
	}
	return region
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

func fetchAQHI(ctx context.Context, client *http.Client, stationID string) (map[string]any, error) {
	obsURL := fmt.Sprintf("https://api.weather.gc.ca/collections/aqhi-observations-realtime/items?offset=0&limit=1000&sortby=-latest&location_id=%s&f=json", stationID)
	fcstURL := fmt.Sprintf("https://api.weather.gc.ca/collections/aqhi-forecasts-realtime/items?limit=1000&offset=0&f=json&sortby=-publication_datetime&aqhi_type=-Period&location_id=%s", stationID)
	var obsRaw, fcstRaw map[string]any
	_ = fetchJSON(ctx, client, obsURL, &obsRaw)
	_ = fetchJSON(ctx, client, fcstURL, &fcstRaw)
	payload := map[string]any{"source": "eccc"}
	if props := firstFeatureProperties(obsRaw); props != nil {
		payload["location"] = map[string]any{"en": textAt(props, "location_name_en"), "fr": textAt(props, "location_name_fr")}
		payload["observed_at"] = props["observation_datetime"]
		payload["aqhi"] = props["aqhi"]
		payload["special_notes"] = map[string]any{"en": textAt(props, "special_notes_en"), "fr": textAt(props, "special_notes_fr")}
	}
	if props := firstFeatureProperties(fcstRaw); props != nil {
		payload["forecast"] = map[string]any{"published_at": props["publication_datetime"], "periods": aqhiPeriods(props)}
	}
	if len(payload) <= 1 {
		return nil, fmt.Errorf("no AQHI data for %s", stationID)
	}
	return payload, nil
}

func fetchMarineForecast(ctx context.Context, client *http.Client, id string) (map[string]any, error) {
	url := fmt.Sprintf("https://api.weather.gc.ca/collections/marineweather-realtime/items/%s?lang=en", url.QueryEscape(strings.TrimSpace(id)))
	var raw map[string]any
	if err := fetchJSON(ctx, client, url, &raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func feedMarineForecastLocations(feed feedXML) []locationXML {
	out := make([]locationXML, 0, len(feed.Locations.MarineForecastLocations.Locations)+len(feed.Locations.MarineForecastLocations.Subregions))
	out = append(out, feed.Locations.MarineForecastLocations.Locations...)
	out = append(out, feed.Locations.MarineForecastLocations.Subregions...)
	return out
}

func fetchClimateSummary(ctx context.Context, client *http.Client, loc locationXML, timezone string, now time.Time) (map[string]any, error) {
	daily, timestamp, err := fetchLatestClimateDaily(ctx, client, loc.ID, now)
	if err != nil {
		return nil, err
	}
	props := daily.Properties
	month := intValue(props["LOCAL_MONTH"])
	day := intValue(props["LOCAL_DAY"])
	var climateDate time.Time
	if month == 0 {
		if parsed, parseErr := parseClimateDate(textValue(props["LOCAL_DATE"])); parseErr == nil {
			month = int(parsed.Month())
			day = parsed.Day()
			climateDate = parsed
		}
	}
	if climateDate.IsZero() {
		if parsed, parseErr := parseClimateDate(textValue(props["LOCAL_DATE"])); parseErr == nil {
			climateDate = parsed
		}
	}
	normalID := firstNonBlank(loc.NormalID, textValue(props["CLIMATE_IDENTIFIER"]), loc.ID)
	normals := map[string]any{}
	if month > 0 {
		if fetched, normalErr := fetchClimateNormals(ctx, client, normalID, month); normalErr == nil {
			normals = fetched
		} else {
			log.Printf("ECCC climate normals fetch failed for %s: %v", normalID, normalErr)
		}
	}
	stationName := firstNonBlank(loc.NameOverride, titleText(textValue(props["STATION_NAME"])), loc.ID)
	obs := map[string]any{
		"station":             map[string]any{"en": stationName, "fr": stationName},
		"date":                textValue(props["LOCAL_DATE"]),
		"high":                climateNumber(props, "MAX_TEMPERATURE"),
		"low":                 climateNumber(props, "MIN_TEMPERATURE"),
		"mean":                climateNumber(props, "MEAN_TEMPERATURE"),
		"precipitation":       climateNumber(props, "TOTAL_PRECIPITATION"),
		"precipitation_trace": climateTrace(props, "TOTAL_PRECIPITATION"),
		"rain":                climateNumber(props, "TOTAL_RAIN"),
		"rain_trace":          climateTrace(props, "TOTAL_RAIN"),
		"snowfall":            climateNumber(props, "TOTAL_SNOW"),
		"snowfall_trace":      climateTrace(props, "TOTAL_SNOW"),
		"snow_on_ground":      climateNumber(props, "SNOW_ON_GROUND"),
		"max_gust_speed":      climateNumber(props, "SPEED_MAX_GUST"),
		"max_gust_direction":  climateGustDirection(props),
		"heating_degree_days": climateNumber(props, "HEATING_DEGREE_DAYS"),
		"cooling_degree_days": climateNumber(props, "COOLING_DEGREE_DAYS"),
		"min_humidity":        climateNumber(props, "MIN_REL_HUMIDITY"),
	}
	lon, lat, hasPoint := configuredPoint(loc)
	if !hasPoint {
		lon, lat, hasPoint = featurePoint(daily)
	}
	records := map[string]any{}
	if hasPoint && month > 0 && day > 0 {
		if fetched, recordsErr := fetchClimateRecords(ctx, client, month, day, lon, lat); recordsErr == nil {
			records = fetched
		} else {
			log.Printf("ECCC climate records fetch failed for %s: %v", loc.ID, recordsErr)
		}
	}
	astronomy := map[string]any{}
	if hasPoint && !climateDate.IsZero() {
		if sun, ok := climateSunriseSunset(climateDate, lon, lat, timezone); ok {
			astronomy = sun
		}
	}
	return map[string]any{
		"source":       "eccc",
		"name":         map[string]any{"en": stationName, "fr": stationName},
		"last_updated": firstNonBlank(timestamp, now.Format(time.RFC3339)),
		"observations": obs,
		"normals":      normals,
		"records":      records,
		"astronomy":    astronomy,
		"metadata": map[string]any{
			"collection":          "climate-daily",
			"station_id":          textValue(props["STN_ID"]),
			"climate_identifier":  firstNonBlank(textValue(props["CLIMATE_IDENTIFIER"]), loc.ID),
			"normal_identifier":   normalID,
			"acceptable_fields":   climateAcceptableFieldList(),
			"accepted_flag_notes": "Only blank-value flags and trace precipitation flags are rendered.",
			"records_collection":  "ltce-temperature, ltce-precipitation, ltce-snowfall",
			"astronomy_source":    "computed from the climate station coordinates",
		},
	}, nil
}

func fetchLatestClimateDaily(ctx context.Context, client *http.Client, stationID string, now time.Time) (geoFeature, string, error) {
	query := "limit=14&sortby=-LOCAL_DATE&CLIMATE_IDENTIFIER=" + url.QueryEscape(strings.TrimSpace(stationID))
	features, timestamp, err := fetchCollectionFeatures(ctx, client, "climate-daily", query)
	if err != nil {
		return geoFeature{}, "", err
	}
	for _, feature := range features {
		if climateDailyUsable(feature.Properties, now) {
			return feature, timestamp, nil
		}
	}
	return geoFeature{}, "", fmt.Errorf("no usable climate daily rows for %s", stationID)
}

func climateDailyUsable(props map[string]any, now time.Time) bool {
	if props == nil {
		return false
	}
	if parsed, err := parseClimateDate(textValue(props["LOCAL_DATE"])); err == nil && parsed.After(now.Add(24*time.Hour)) {
		return false
	}
	for _, field := range climateAcceptableFieldList() {
		if climateNumber(props, field) != nil || climateTrace(props, field) {
			return true
		}
	}
	return false
}

func climateAcceptableFieldList() []string {
	return []string{
		"MAX_TEMPERATURE",
		"MIN_TEMPERATURE",
		"MEAN_TEMPERATURE",
		"TOTAL_PRECIPITATION",
		"TOTAL_RAIN",
		"TOTAL_SNOW",
		"SNOW_ON_GROUND",
		"SPEED_MAX_GUST",
		"DIRECTION_MAX_GUST",
		"HEATING_DEGREE_DAYS",
		"COOLING_DEGREE_DAYS",
		"MIN_REL_HUMIDITY",
	}
}

func fetchClimateNormals(ctx context.Context, client *http.Client, stationID string, month int) (map[string]any, error) {
	query := fmt.Sprintf("limit=120&CLIMATE_IDENTIFIER=%s&MONTH=%d", url.QueryEscape(strings.TrimSpace(stationID)), month)
	features, _, err := fetchCollectionFeatures(ctx, client, "climate-normals", query)
	if err != nil {
		return nil, err
	}
	values := map[int]float64{}
	station := ""
	periodBegin := 0
	periodEnd := 0
	for _, feature := range features {
		props := feature.Properties
		id := intValue(props["NORMAL_ID"])
		if id == 0 || !climateNormalUsable(props) {
			continue
		}
		value, ok := numberValue(props["VALUE"])
		if !ok {
			continue
		}
		values[id] = math.Round(value*10) / 10
		if station == "" {
			station = titleText(textValue(props["STATION_NAME"]))
		}
		if periodBegin == 0 {
			periodBegin = intValue(props["PERIOD_BEGIN"])
		}
		if periodEnd == 0 {
			periodEnd = intValue(props["PERIOD_END"])
		}
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("no usable climate normals for %s month %d", stationID, month)
	}
	temperature := map[string]any{}
	if value, ok := values[5]; ok {
		temperature["high"] = value
	}
	if value, ok := values[8]; ok {
		temperature["low"] = value
	}
	if value, ok := values[1]; ok {
		temperature["mean"] = value
	}
	normals := map[string]any{
		"station":     map[string]any{"en": station, "fr": station},
		"month":       month,
		"temperature": temperature,
	}
	if periodBegin > 0 && periodEnd > 0 {
		normals["period"] = fmt.Sprintf("%d to %d", periodBegin, periodEnd)
	}
	if value, ok := values[56]; ok {
		normals["precipitation"] = value
	}
	if value, ok := values[52]; ok {
		normals["rainfall"] = value
	}
	if value, ok := values[54]; ok {
		normals["snowfall"] = value
	}
	if value, ok := values[90]; ok {
		normals["wind_speed"] = value
	}
	return normals, nil
}

func fetchClimateRecords(ctx context.Context, client *http.Client, month int, day int, lon float64, lat float64) (map[string]any, error) {
	records := map[string]any{}
	var firstErr error
	if feature, ok, err := fetchClosestClimateRecordFeature(ctx, client, "ltce-temperature", month, day, lon, lat); err != nil {
		firstErr = err
	} else if ok {
		props := feature.Properties
		addClimateRecord(records, "high_temperature", props, "RECORD_HIGH_MAX_TEMP", "RECORD_HIGH_MAX_TEMP_YR", true)
		addClimateRecord(records, "low_temperature", props, "RECORD_LOW_MIN_TEMP", "RECORD_LOW_MIN_TEMP_YR", true)
	}
	if feature, ok, err := fetchClosestClimateRecordFeature(ctx, client, "ltce-precipitation", month, day, lon, lat); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	} else if ok {
		addClimateRecord(records, "precipitation", feature.Properties, "RECORD_PRECIPITATION", "RECORD_PRECIPITATION_YR", false)
	}
	if feature, ok, err := fetchClosestClimateRecordFeature(ctx, client, "ltce-snowfall", month, day, lon, lat); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	} else if ok {
		addClimateRecord(records, "snowfall", feature.Properties, "RECORD_SNOWFALL", "RECORD_SNOWFALL_YR", false)
	}
	if len(records) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return records, nil
}

func fetchClosestClimateRecordFeature(ctx context.Context, client *http.Client, collection string, month int, day int, lon float64, lat float64) (geoFeature, bool, error) {
	for _, radius := range []float64{0.75, 1.5} {
		query := fmt.Sprintf(
			"limit=50&LOCAL_MONTH=%d&LOCAL_DAY=%d&bbox=%.5f,%.5f,%.5f,%.5f",
			month,
			day,
			lon-radius,
			lat-radius,
			lon+radius,
			lat+radius,
		)
		features, _, err := fetchCollectionFeatures(ctx, client, collection, query)
		if err != nil {
			return geoFeature{}, false, err
		}
		if feature, ok := closestFeature(features, lon, lat); ok {
			return feature, true, nil
		}
	}
	return geoFeature{}, false, nil
}

func closestFeature(features []geoFeature, lon float64, lat float64) (geoFeature, bool) {
	var best geoFeature
	bestDistance := math.Inf(1)
	for _, feature := range features {
		featureLon, featureLat, ok := featurePoint(feature)
		if !ok {
			continue
		}
		dLon := featureLon - lon
		dLat := featureLat - lat
		distance := dLon*dLon + dLat*dLat
		if distance < bestDistance {
			best = feature
			bestDistance = distance
		}
	}
	return best, !math.IsInf(bestDistance, 1)
}

func featurePoint(feature geoFeature) (float64, float64, bool) {
	coordinates, _ := feature.Geometry["coordinates"].([]any)
	if len(coordinates) < 2 {
		return 0, 0, false
	}
	lon, lonOK := numberValue(coordinates[0])
	lat, latOK := numberValue(coordinates[1])
	if !lonOK || !latOK {
		return 0, 0, false
	}
	return lon, lat, true
}

func geoFeatureDistanceToPointKM(feature geoFeature, lon float64, lat float64) (float64, bool) {
	return geoGeometryDistanceToPointKM(feature.Geometry, lon, lat)
}

func geoFeatureDirectionFromPoint(feature geoFeature, lon float64, lat float64) (string, bool) {
	targetLon, targetLat, ok := geoGeometryCentroid(feature.Geometry)
	if !ok {
		return "", false
	}
	return cardinalDirection(lon, lat, targetLon, targetLat), true
}

func geoGeometryDistanceToPointKM(geometry map[string]any, lon float64, lat float64) (float64, bool) {
	geometryType := strings.ToLower(strings.TrimSpace(textValue(geometry["type"])))
	coordinates, _ := geometry["coordinates"].([]any)
	switch geometryType {
	case "point":
		if len(coordinates) < 2 {
			return 0, false
		}
		pointLon, lonOK := numberValue(coordinates[0])
		pointLat, latOK := numberValue(coordinates[1])
		if !lonOK || !latOK {
			return 0, false
		}
		return haversineKM(lon, lat, pointLon, pointLat), true
	case "linestring":
		return lineStringDistanceToPointKM(coordinates, lon, lat)
	case "polygon":
		return polygonDistanceToPointKM(coordinates, lon, lat)
	case "multipolygon":
		best := math.Inf(1)
		for _, polygonRaw := range coordinates {
			polygon, _ := polygonRaw.([]any)
			if distance, ok := polygonDistanceToPointKM(polygon, lon, lat); ok && distance < best {
				best = distance
			}
		}
		return best, !math.IsInf(best, 1)
	default:
		return 0, false
	}
}

func geoGeometryCentroid(geometry map[string]any) (float64, float64, bool) {
	geometryType := strings.ToLower(strings.TrimSpace(textValue(geometry["type"])))
	coordinates, _ := geometry["coordinates"].([]any)
	switch geometryType {
	case "point":
		if len(coordinates) < 2 {
			return 0, 0, false
		}
		lon, lonOK := numberValue(coordinates[0])
		lat, latOK := numberValue(coordinates[1])
		return lon, lat, lonOK && latOK
	case "linestring":
		return coordinateListCentroid(coordinates)
	case "polygon":
		if len(coordinates) == 0 {
			return 0, 0, false
		}
		outer, _ := coordinates[0].([]any)
		return coordinateListCentroid(outer)
	case "multipolygon":
		lons := []float64{}
		lats := []float64{}
		for _, polygonRaw := range coordinates {
			polygon, _ := polygonRaw.([]any)
			if len(polygon) == 0 {
				continue
			}
			outer, _ := polygon[0].([]any)
			if lon, lat, ok := coordinateListCentroid(outer); ok {
				lons = append(lons, lon)
				lats = append(lats, lat)
			}
		}
		return averageLonLat(lons, lats)
	default:
		return 0, 0, false
	}
}

func coordinateListCentroid(points []any) (float64, float64, bool) {
	lons := []float64{}
	lats := []float64{}
	for index, raw := range points {
		if index == len(points)-1 && len(points) > 1 {
			if sameCoordinate(points[0], raw) {
				continue
			}
		}
		point, _ := raw.([]any)
		if len(point) < 2 {
			continue
		}
		lon, lonOK := numberValue(point[0])
		lat, latOK := numberValue(point[1])
		if lonOK && latOK {
			lons = append(lons, lon)
			lats = append(lats, lat)
		}
	}
	return averageLonLat(lons, lats)
}

func sameCoordinate(left any, right any) bool {
	leftPoint, _ := left.([]any)
	rightPoint, _ := right.([]any)
	if len(leftPoint) < 2 || len(rightPoint) < 2 {
		return false
	}
	leftLon, leftLonOK := numberValue(leftPoint[0])
	leftLat, leftLatOK := numberValue(leftPoint[1])
	rightLon, rightLonOK := numberValue(rightPoint[0])
	rightLat, rightLatOK := numberValue(rightPoint[1])
	return leftLonOK && leftLatOK && rightLonOK && rightLatOK && math.Abs(leftLon-rightLon) < 1e-9 && math.Abs(leftLat-rightLat) < 1e-9
}

func averageLonLat(lons []float64, lats []float64) (float64, float64, bool) {
	if len(lons) == 0 || len(lons) != len(lats) {
		return 0, 0, false
	}
	lonTotal := 0.0
	latTotal := 0.0
	for index := range lons {
		lonTotal += lons[index]
		latTotal += lats[index]
	}
	count := float64(len(lons))
	return lonTotal / count, latTotal / count, true
}

func polygonDistanceToPointKM(rings []any, lon float64, lat float64) (float64, bool) {
	if len(rings) == 0 {
		return 0, false
	}
	outer, ok := rings[0].([]any)
	if !ok || len(outer) < 4 {
		return 0, false
	}
	if pointInRing(lon, lat, outer) {
		inHole := false
		for _, holeRaw := range rings[1:] {
			hole, _ := holeRaw.([]any)
			if len(hole) >= 4 && pointInRing(lon, lat, hole) {
				inHole = true
				break
			}
		}
		if !inHole {
			return 0, true
		}
	}
	return lineStringDistanceToPointKM(outer, lon, lat)
}

func lineStringDistanceToPointKM(points []any, lon float64, lat float64) (float64, bool) {
	if len(points) == 0 {
		return 0, false
	}
	best := math.Inf(1)
	var prevLon, prevLat float64
	hasPrev := false
	for _, raw := range points {
		point, _ := raw.([]any)
		if len(point) < 2 {
			continue
		}
		pointLon, lonOK := numberValue(point[0])
		pointLat, latOK := numberValue(point[1])
		if !lonOK || !latOK {
			continue
		}
		if !hasPrev {
			distance := haversineKM(lon, lat, pointLon, pointLat)
			if distance < best {
				best = distance
			}
			prevLon, prevLat = pointLon, pointLat
			hasPrev = true
			continue
		}
		distance := segmentDistanceToPointKM(prevLon, prevLat, pointLon, pointLat, lon, lat)
		if distance < best {
			best = distance
		}
		prevLon, prevLat = pointLon, pointLat
	}
	return best, !math.IsInf(best, 1)
}

func pointInRing(lon float64, lat float64, ring []any) bool {
	inside := false
	j := len(ring) - 1
	for i := 0; i < len(ring); i++ {
		left, _ := ring[i].([]any)
		right, _ := ring[j].([]any)
		if len(left) < 2 || len(right) < 2 {
			j = i
			continue
		}
		leftLon, leftLonOK := numberValue(left[0])
		leftLat, leftLatOK := numberValue(left[1])
		rightLon, rightLonOK := numberValue(right[0])
		rightLat, rightLatOK := numberValue(right[1])
		if !leftLonOK || !leftLatOK || !rightLonOK || !rightLatOK {
			j = i
			continue
		}
		if (leftLat > lat) != (rightLat > lat) {
			crossLon := (rightLon-leftLon)*(lat-leftLat)/(rightLat-leftLat) + leftLon
			if lon < crossLon {
				inside = !inside
			}
		}
		j = i
	}
	return inside
}

func segmentDistanceToPointKM(lon1 float64, lat1 float64, lon2 float64, lat2 float64, lon float64, lat float64) float64 {
	const kmPerDegreeLat = 111.32
	cosLat := math.Cos(degreesToRadians(lat))
	x1 := lon1 * cosLat * kmPerDegreeLat
	y1 := lat1 * kmPerDegreeLat
	x2 := lon2 * cosLat * kmPerDegreeLat
	y2 := lat2 * kmPerDegreeLat
	px := lon * cosLat * kmPerDegreeLat
	py := lat * kmPerDegreeLat
	dx := x2 - x1
	dy := y2 - y1
	if math.Abs(dx) < 1e-9 && math.Abs(dy) < 1e-9 {
		return math.Hypot(px-x1, py-y1)
	}
	t := ((px-x1)*dx + (py-y1)*dy) / (dx*dx + dy*dy)
	t = math.Max(0, math.Min(1, t))
	closestX := x1 + t*dx
	closestY := y1 + t*dy
	return math.Hypot(px-closestX, py-closestY)
}

func haversineKM(lon1 float64, lat1 float64, lon2 float64, lat2 float64) float64 {
	const earthRadiusKM = 6371.0
	dLat := degreesToRadians(lat2 - lat1)
	dLon := degreesToRadians(lon2 - lon1)
	lat1Rad := degreesToRadians(lat1)
	lat2Rad := degreesToRadians(lat2)
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusKM * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func cardinalDirection(fromLon float64, fromLat float64, toLon float64, toLat float64) string {
	dx := (toLon - fromLon) * math.Cos(degreesToRadians(fromLat))
	dy := toLat - fromLat
	if math.Abs(dx) < 1e-9 && math.Abs(dy) < 1e-9 {
		return ""
	}
	degrees := math.Mod(radiansToDegrees(math.Atan2(dx, dy))+360, 360)
	directions := []string{"north", "north east", "east", "south east", "south", "south west", "west", "north west"}
	index := int(math.Floor((degrees+22.5)/45.0)) % len(directions)
	return directions[index]
}

func addClimateRecord(records map[string]any, key string, props map[string]any, valueField string, yearField string, includeZero bool) {
	value, valueOK := numberValue(props[valueField])
	year := intValue(props[yearField])
	if !valueOK || year <= 0 {
		return
	}
	if !includeZero && math.Abs(value) < 0.05 {
		return
	}
	records[key] = map[string]any{
		"value": math.Round(value*10) / 10,
		"year":  year,
	}
}

func climateSunriseSunset(date time.Time, lon float64, lat float64, timezone string) (map[string]any, bool) {
	loc, err := time.LoadLocation(fallbackText(timezone, "UTC"))
	if err != nil {
		loc = time.UTC
	}
	localDate := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, loc)
	sunrise, sunriseOK := solarEvent(localDate, lat, lon, true, loc)
	sunset, sunsetOK := solarEvent(localDate, lat, lon, false, loc)
	if !sunriseOK || !sunsetOK {
		return nil, false
	}
	return map[string]any{
		"sunrise":  sunrise.Format(time.RFC3339),
		"sunset":   sunset.Format(time.RFC3339),
		"timezone": timezone,
	}, true
}

func solarEvent(localDate time.Time, lat float64, lon float64, sunrise bool, loc *time.Location) (time.Time, bool) {
	day := float64(localDate.YearDay())
	longitudeHour := lon / 15
	targetHour := 18.0
	if sunrise {
		targetHour = 6
	}
	t := day + ((targetHour - longitudeHour) / 24)
	meanAnomaly := normalizeDegrees((0.9856 * t) - 3.289)
	trueLongitude := normalizeDegrees(meanAnomaly + (1.916 * sinDegrees(meanAnomaly)) + (0.020 * sinDegrees(2*meanAnomaly)) + 282.634)
	rightAscension := normalizeDegrees(radiansToDegrees(math.Atan(0.91764 * tanDegrees(trueLongitude))))
	rightAscension += math.Floor(trueLongitude/90)*90 - math.Floor(rightAscension/90)*90
	rightAscension /= 15
	sinDeclination := 0.39782 * sinDegrees(trueLongitude)
	cosDeclination := math.Cos(math.Asin(sinDeclination))
	cosHour := (cosDegrees(90.833) - (sinDeclination * sinDegrees(lat))) / (cosDeclination * cosDegrees(lat))
	if cosHour > 1 || cosHour < -1 {
		return time.Time{}, false
	}
	hourAngle := radiansToDegrees(math.Acos(cosHour))
	if sunrise {
		hourAngle = 360 - hourAngle
	}
	hourAngle /= 15
	localMeanTime := hourAngle + rightAscension - (0.06571 * t) - 6.622
	utcHour := normalizeHours(localMeanTime - longitudeHour)
	event := time.Date(localDate.Year(), localDate.Month(), localDate.Day(), 0, 0, 0, 0, time.UTC).
		Add(time.Duration(math.Round(utcHour*3600)) * time.Second)
	return alignSolarEventToLocalDate(event, localDate, loc), true
}

func alignSolarEventToLocalDate(event time.Time, localDate time.Time, loc *time.Location) time.Time {
	target := dateOnly(localDate, loc)
	for i := 0; i < 3; i++ {
		current := dateOnly(event.In(loc), loc)
		switch {
		case current.Equal(target):
			return event
		case current.Before(target):
			event = event.Add(24 * time.Hour)
		default:
			event = event.Add(-24 * time.Hour)
		}
	}
	return event
}

func dateOnly(value time.Time, loc *time.Location) time.Time {
	year, month, day := value.In(loc).Date()
	return time.Date(year, month, day, 0, 0, 0, 0, loc)
}

func normalizeDegrees(value float64) float64 {
	out := math.Mod(value, 360)
	if out < 0 {
		out += 360
	}
	return out
}

func normalizeHours(value float64) float64 {
	out := math.Mod(value, 24)
	if out < 0 {
		out += 24
	}
	return out
}

func sinDegrees(value float64) float64 {
	return math.Sin(degreesToRadians(value))
}

func cosDegrees(value float64) float64 {
	return math.Cos(degreesToRadians(value))
}

func tanDegrees(value float64) float64 {
	return math.Tan(degreesToRadians(value))
}

func degreesToRadians(value float64) float64 {
	return value * math.Pi / 180
}

func radiansToDegrees(value float64) float64 {
	return value * 180 / math.Pi
}

func climateNormalUsable(props map[string]any) bool {
	if strings.EqualFold(strings.TrimSpace(textValue(props["CURRENT_FLAG"])), "N") {
		return false
	}
	period := strings.ToUpper(strings.TrimSpace(textValue(props["PERIOD"])))
	if period != "" && period != "NORM" {
		return false
	}
	if years := intValue(props["YEAR_COUNT_NORMAL_PERIOD"]); years > 0 && years < 15 {
		return false
	}
	if percent, ok := numberValue(props["PERCENT_OF_POSSIBLE_OBS"]); ok && percent < 80 {
		return false
	}
	return true
}

func climateNumber(props map[string]any, field string) any {
	if !climateFlagOK(textValue(props[field+"_FLAG"]), field) {
		return nil
	}
	value, ok := numberValue(props[field])
	if !ok {
		return nil
	}
	return math.Round(value*10) / 10
}

func climateTrace(props map[string]any, field string) bool {
	if !climateTraceCapable(field) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(textValue(props[field+"_FLAG"])), "T")
}

func climateFlagOK(flag string, field string) bool {
	flag = strings.ToUpper(strings.TrimSpace(flag))
	if flag == "" {
		return true
	}
	return flag == "T" && climateTraceCapable(field)
}

func climateTraceCapable(field string) bool {
	switch field {
	case "TOTAL_PRECIPITATION", "TOTAL_RAIN", "TOTAL_SNOW", "SNOW_ON_GROUND":
		return true
	default:
		return false
	}
}

func climateGustDirection(props map[string]any) any {
	raw, ok := numberValue(climateNumber(props, "DIRECTION_MAX_GUST"))
	if !ok {
		return nil
	}
	if raw > 0 && raw <= 36 {
		raw *= 10
	}
	return degreesToCardinal(raw, true)
}

func parseClimateDate(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02"} {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid climate date %q", raw)
}

func aqhiPeriods(props map[string]any) []map[string]any {
	group := mapAt(props, "forecast_period")
	keys := []string{"period_1", "period1", "period_2", "period2", "period_3", "period3", "period_4", "period4", "period_5", "period5", "period_6", "period6"}
	out := []map[string]any{}
	seen := map[string]struct{}{}
	for _, key := range keys {
		period := mapAt(group, key)
		if len(period) == 0 {
			continue
		}
		normalizedKey := strings.ReplaceAll(key, "_", "")
		if _, ok := seen[normalizedKey]; ok {
			continue
		}
		seen[normalizedKey] = struct{}{}
		out = append(out, map[string]any{
			"period":       map[string]any{"en": textAt(period, "forecast_period_en"), "fr": textAt(period, "forecast_period_fr")},
			"aqhi":         period["aqhi"],
			"aqhi_insmoke": period["aqhi_insmoke"],
		})
	}
	return out
}

type geoFeatureCollection struct {
	Features       []geoFeature `json:"features"`
	NumberMatched  int          `json:"numberMatched"`
	NumberReturned int          `json:"numberReturned"`
	TimeStamp      string       `json:"timeStamp"`
}

type geoFeature struct {
	ID         string         `json:"id"`
	Geometry   map[string]any `json:"geometry"`
	Properties map[string]any `json:"properties"`
}

func fetchFeedSpecialtyProducts(ctx context.Context, client *http.Client, publisher events.Publisher, store datastore.Store, feed feedXML, ecccCache map[string]map[string]any) {
	fetchers := []struct {
		kind string
		fn   func(context.Context, *http.Client, feedXML, map[string]map[string]any) (map[string]any, error)
	}{
		{"thunderstorm_outlook", fetchThunderstormOutlookProduct},
		{"hydrometric", fetchHydrometricProduct},
	}
	for _, fetcher := range fetchers {
		if ctx.Err() != nil {
			return
		}
		payload, err := fetcher.fn(ctx, client, feed, ecccCache)
		if err != nil {
			log.Printf("%s fetch failed for feed %s: %v", fetcher.kind, feed.ID, err)
			continue
		}
		if len(payload) == 0 {
			continue
		}
		canonicalIDs := feedCanonicalLocationIDs(feed)
		if len(canonicalIDs) > 0 {
			payload["_canonical_location_ids"] = canonicalIDs
		}
		if err := store.StoreProductPayload(ctx, datastore.ProductPayloadRecord{
			Kind:    fetcher.kind,
			Source:  "eccc",
			ID:      feed.ID,
			Payload: payload,
		}); err != nil {
			log.Printf("%s store failed for feed %s: %v", fetcher.kind, feed.ID, err)
			continue
		}
		publishDataReady(publisher, feed.ID, fetcher.kind, feed.ID, canonicalIDs...)
	}
}

func fetchThunderstormOutlookProduct(ctx context.Context, client *http.Client, feed feedXML, ecccCache map[string]map[string]any) (map[string]any, error) {
	features, timestamp, err := fetchCollectionFeatures(ctx, client, "thunderstorm_outlook", "limit=1000&sortby=-validity_datetime")
	if err != nil {
		return nil, err
	}
	if ecccCache == nil {
		ecccCache = map[string]map[string]any{}
	}
	refLon, refLat, _, hasReference := feedReferencePoint(ctx, client, feed, ecccCache)
	subtypes := feedSpecialtySubtypes(feed)
	now := time.Now().UTC()
	items := []map[string]any{}
	for _, feature := range features {
		props := feature.Properties
		if !featureCurrent(props, now) || !featureSubtypeMatchesFeed(props, subtypes) {
			continue
		}
		distanceKM := 0.0
		if hasReference {
			var coversPoint bool
			distanceKM, coversPoint = geoFeatureDistanceToPointKM(feature, refLon, refLat)
			if !coversPoint || distanceKM > thunderstormOutlookCoverageToleranceKM {
				continue
			}
		}
		item := map[string]any{
			"id":           firstNonBlank(feature.ID, textValue(props["id"])),
			"area":         textValue(props["product_sub_type"]),
			"thunderstorm": textValue(props["metobject.thunderstorm.value"]),
			"risk":         props["metobject.risk_swo.value"],
			"confidence":   props["metobject.confidence.value"],
			"impact":       props["metobject.impact.value"],
			"tornado":      props["metobject.tornado_risk.value"],
			"hail_cm":      props["metobject.hail.value"],
			"hail_unit":    props["metobject.hail.unit"],
			"gust_kmh":     props["metobject.gust.value"],
			"gust_unit":    props["metobject.gust.unit"],
			"rain_mm":      props["metobject.rain.value"],
			"rain_unit":    props["metobject.rain.unit"],
			"published_at": textValue(props["publication_datetime"]),
			"valid_at":     textValue(props["validity_datetime"]),
			"expires_at":   textValue(props["expiration_datetime"]),
		}
		if hasReference {
			item["distance_km"] = math.Round(distanceKM)
		}
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool {
		left, _ := numberValue(items[i]["risk"])
		right, _ := numberValue(items[j]["risk"])
		return left > right
	})
	return specialtyPayload("thunderstorm_outlook", "Thunderstorm Outlook", timestamp, items), nil
}

func fetchHydrometricProduct(ctx context.Context, client *http.Client, feed feedXML, _ map[string]map[string]any) (map[string]any, error) {
	items := []map[string]any{}
	for index, entry := range feedHydrometricLocations(feed) {
		loc := entry.Location
		if sourceKind(loc.Source) != "eccc" || strings.TrimSpace(loc.ID) == "" {
			continue
		}
		query := "limit=1&sortby=-DATETIME&STATION_NUMBER=" + url.QueryEscape(strings.TrimSpace(loc.ID))
		features, _, err := fetchCollectionFeatures(ctx, client, "hydrometric-realtime", query)
		if err != nil {
			log.Printf("hydrometric fetch failed for %s: %v", loc.ID, err)
			continue
		}
		if len(features) == 0 {
			continue
		}
		props := features[0].Properties
		items = append(items, map[string]any{
			"id":           firstNonBlank(features[0].ID, textValue(props["IDENTIFIER"])),
			"station_id":   firstNonBlank(textValue(props["STATION_NUMBER"]), loc.ID),
			"canonical_id": loc.CanonicalID,
			"raw_source":   loc.Source,
			"raw_id":       loc.ID,
			"station":      firstNonBlank(loc.NameOverride, titleText(textValue(props["STATION_NAME"]))),
			"relation":     entry.Relation,
			"order":        index,
			"observed_at":  firstNonBlank(textValue(props["DATETIME_LST"]), textValue(props["DATETIME"])),
			"level_m":      props["LEVEL"],
			"discharge":    props["DISCHARGE"],
			"level_note":   textValue(props["LEVEL_SYMBOL_EN"]),
			"flow_note":    textValue(props["DISCHARGE_SYMBOL_EN"]),
			"published_at": firstNonBlank(textValue(props["DATETIME_LST"]), textValue(props["DATETIME"])),
		})
	}
	return specialtyPayload("hydrometric", "River Conditions", time.Now().UTC().Format(time.RFC3339), items), nil
}

type hydrometricLocationEntry struct {
	Location locationXML
	Relation string
}

func feedHydrometricLocations(feed feedXML) []hydrometricLocationEntry {
	out := make([]hydrometricLocationEntry, 0, len(feed.Locations.HydrometricLocations.Locations)+len(feed.Locations.HydrometricLocations.Upstream.Locations)+len(feed.Locations.HydrometricLocations.Downstream.Locations))
	appendAll := func(relation string, locations []locationXML) {
		for _, loc := range locations {
			out = append(out, hydrometricLocationEntry{Location: loc, Relation: relation})
		}
	}
	appendAll("primary", feed.Locations.HydrometricLocations.Locations)
	appendAll("upstream", feed.Locations.HydrometricLocations.Upstream.Locations)
	appendAll("downstream", feed.Locations.HydrometricLocations.Downstream.Locations)
	return out
}

func feedCanonicalLocationIDs(feed feedXML) []string {
	ids := []string{}
	for _, region := range feed.Locations.Coverage.Regions {
		ids = append(ids, region.CanonicalID, region.ForecastCanonicalID)
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
	return uniqueNonBlankStrings(ids...)
}

func fetchCollectionFeatures(ctx context.Context, client *http.Client, collection string, query string) ([]geoFeature, string, error) {
	base := "https://api.weather.gc.ca/collections/" + collection + "/items?f=json"
	if strings.TrimSpace(query) != "" {
		base += "&" + strings.TrimLeft(query, "&?")
	}
	var raw geoFeatureCollection
	if err := fetchJSON(ctx, client, base, &raw); err != nil {
		return nil, "", err
	}
	return raw.Features, raw.TimeStamp, nil
}

func specialtyPayload(collection string, title string, timestamp string, items []map[string]any) map[string]any {
	if items == nil {
		items = []map[string]any{}
	}
	return map[string]any{
		"source":     "eccc",
		"collection": collection,
		"title":      title,
		"updated_at": firstNonBlank(timestamp, time.Now().UTC().Format(time.RFC3339)),
		"items":      items,
	}
}

func featureCurrent(props map[string]any, now time.Time) bool {
	if props == nil {
		return false
	}
	for _, key := range []string{"expiration_datetime", "end_datetime"} {
		raw := textValue(props[key])
		if raw == "" {
			continue
		}
		if expires, err := parseAPITime(raw); err == nil && expires.Before(now.Add(-5*time.Minute)) {
			return false
		}
	}
	status := strings.ToLower(textValue(props["status"]))
	return status == "" || status == "final" || status == "updated" || status == "actual"
}

func parseAPITime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05Z", "2006-01-02T15:04:05-0700"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported API time %q", raw)
}

func featureSubtypeMatchesFeed(props map[string]any, subtypes map[string]struct{}) bool {
	if len(subtypes) == 0 {
		return true
	}
	subtype := strings.ToUpper(strings.TrimSpace(textValue(props["product_sub_type"])))
	if subtype == "" {
		return true
	}
	_, ok := subtypes[subtype]
	return ok
}

func feedSpecialtySubtypes(feed feedXML) map[string]struct{} {
	out := map[string]struct{}{}
	for province := range feedProvinceCodes(feed) {
		switch province {
		case "AB", "MB", "SK":
			out["PRAIRIES"] = struct{}{}
		case "BC", "YT":
			out["BC-YT"] = struct{}{}
		case "NB", "NL", "NS", "PE":
			out["ATLANTIC"] = struct{}{}
		case "ON", "QC":
			out[province] = struct{}{}
		}
	}
	return out
}

func feedProvinceCodes(feed feedXML) map[string]struct{} {
	out := map[string]struct{}{}
	addFromID := func(raw string) {
		value := strings.ToUpper(strings.TrimSpace(raw))
		if before, _, ok := strings.Cut(value, "-"); ok && len(before) == 2 && before[0] >= 'A' && before[0] <= 'Z' && before[1] >= 'A' && before[1] <= 'Z' {
			out[before] = struct{}{}
		}
	}
	for _, loc := range feed.Locations.ObservationLocations.Locations {
		addFromID(loc.ID)
	}
	for _, loc := range feed.Locations.AirQualityLocations.Locations {
		addFromID(loc.ID)
	}
	for _, region := range feed.Locations.Coverage.Regions {
		addFromID(region.DeriveForecast)
		addFromID(region.ID)
	}
	return out
}

func feedReferencePoint(ctx context.Context, client *http.Client, feed feedXML, ecccCache map[string]map[string]any) (float64, float64, string, bool) {
	for _, loc := range feed.Locations.ObservationLocations.Locations {
		if sourceKind(loc.Source) != "eccc" {
			continue
		}
		if lon, lat, ok := configuredPoint(loc); ok {
			return lon, lat, firstNonBlank(loc.NameOverride, loc.ID), true
		}
		raw := ecccCache[loc.ID]
		if raw == nil {
			fetched, err := fetchECCCCitypage(ctx, client, loc.ID)
			if err != nil {
				continue
			}
			raw = fetched
			ecccCache[loc.ID] = fetched
		}
		if lon, lat, name, ok := citypagePoint(raw); ok {
			return lon, lat, firstNonBlank(loc.NameOverride, name, loc.ID), true
		}
	}
	for _, region := range feed.Locations.Coverage.Regions {
		forecastID := forecastRegionFetchID(region)
		if forecastID == "" {
			continue
		}
		raw := ecccCache[forecastID]
		if raw == nil {
			fetched, err := fetchECCCCitypage(ctx, client, forecastID)
			if err != nil {
				continue
			}
			raw = fetched
			ecccCache[forecastID] = fetched
		}
		if lon, lat, name, ok := citypagePoint(raw); ok {
			return lon, lat, firstNonBlank(region.Name, name, forecastID), true
		}
	}
	return 0, 0, "", false
}

func forecastRegionFetchID(region coverageRegionXML) string {
	forecastID := strings.TrimSpace(fallbackText(region.DeriveForecast, region.ID))
	if forecastID == "" || strings.ContainsAny(forecastID, "*?") {
		return ""
	}
	return forecastID
}

func configuredPoint(loc locationXML) (float64, float64, bool) {
	lat, latErr := strconv.ParseFloat(strings.TrimSpace(loc.Latitude), 64)
	lon, lonErr := strconv.ParseFloat(strings.TrimSpace(loc.Longitude), 64)
	if latErr != nil || lonErr != nil {
		return 0, 0, false
	}
	return lon, lat, true
}

func citypagePoint(raw map[string]any) (float64, float64, string, bool) {
	geometry := mapAt(raw, "geometry")
	coordinates, _ := geometry["coordinates"].([]any)
	if len(coordinates) < 2 {
		return 0, 0, "", false
	}
	lon, lonOK := numberValue(coordinates[0])
	lat, latOK := numberValue(coordinates[1])
	if !lonOK || !latOK {
		return 0, 0, "", false
	}
	props := mapAt(raw, "properties")
	name := localizedText(props["name"], "en")
	return lon, lat, name, true
}

func numberValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func intValue(value any) int {
	parsed, ok := numberValue(value)
	if !ok {
		return 0
	}
	return int(math.Round(parsed))
}

func titleText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	words := strings.Fields(strings.ToLower(value))
	for i, word := range words {
		if word == "" {
			continue
		}
		words[i] = strings.ToUpper(word[:1]) + word[1:]
	}
	return strings.Join(words, " ")
}

func fetchJSON(ctx context.Context, client *http.Client, url string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "HazeWeatherRadio/26.06 data-ingest")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s returned %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

func fetchText(ctx context.Context, client *http.Client, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "HazeWeatherRadio/26.06 data-ingest")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("%s returned %s", url, resp.Status)
	}
	raw, err := io.ReadAll(resp.Body)
	return string(raw), err
}

func firstFeatureProperties(raw map[string]any) map[string]any {
	features, _ := raw["features"].([]any)
	if len(features) == 0 {
		return nil
	}
	feature, _ := features[0].(map[string]any)
	return mapAt(feature, "properties")
}

func unitValue(props map[string]any, key string) (float64, bool) {
	return numberAt(mapAt(props, key), "value")
}

func speedKPH(props map[string]any, key string) (float64, bool) {
	item := mapAt(props, key)
	value, ok := numberAt(item, "value")
	if !ok {
		return 0, false
	}
	unit := strings.ToLower(textAt(item, "unitCode"))
	if strings.Contains(unit, "m_s-1") || strings.Contains(unit, "m/s") {
		value *= 3.6
	}
	return value, true
}

func visibilityKM(props map[string]any) (float64, bool) {
	item := mapAt(props, "visibility")
	value, ok := numberAt(item, "value")
	if !ok {
		return 0, false
	}
	unit := strings.ToLower(textAt(item, "unitCode"))
	if strings.Contains(unit, "m") && !strings.Contains(unit, "km") {
		value /= 1000
	}
	return value, true
}

func pressureKPA(props map[string]any) (float64, bool) {
	item := mapAt(props, "barometricPressure")
	value, ok := numberAt(item, "value")
	if !ok {
		return 0, false
	}
	unit := strings.ToLower(textAt(item, "unitCode"))
	if strings.Contains(unit, "pa") {
		value /= 1000
	}
	return value, true
}

func numberAt(source map[string]any, key string) (float64, bool) {
	if source == nil {
		return 0, false
	}
	switch value := source[key].(type) {
	case float64:
		return value, true
	case int:
		return float64(value), true
	case json.Number:
		parsed, err := value.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func roundedNullable(value float64, ok bool) any {
	if !ok {
		return nil
	}
	return math.Round(value*10) / 10
}

func degreesToCardinal(value float64, ok bool) any {
	if !ok {
		return nil
	}
	dirs := []string{"N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"}
	return dirs[int(math.Round(value/22.5))%len(dirs)]
}

func loadDotEnv(path string) {
	path = filepath.Clean(path)
	raw, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) || filepath.Base(path) != ".env" {
			return
		}
		examplePath := filepath.Join(filepath.Dir(path), ".env.example")
		exampleRaw, readErr := os.ReadFile(examplePath)
		if readErr != nil {
			log.Printf("WARN .env file not found and no .env.example is available: %s", path)
			return
		}
		if writeErr := os.WriteFile(path, exampleRaw, 0o600); writeErr != nil {
			log.Printf("WARN .env file not found and could not create %s: %v", path, writeErr)
			return
		}
		log.Printf("WARN .env file not found: created %s from %s", path, examplePath)
		raw = exampleRaw
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" || os.Getenv(key) != "" {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		_ = os.Setenv(key, value)
	}
}

func publishDataReady(publisher events.Publisher, feedID string, kind string, subject string, canonicalIDs ...string) {
	data := map[string]any{"feed_id": feedID, "kind": kind, "id": subject}
	canonicalIDs = uniqueNonBlankStrings(canonicalIDs...)
	if len(canonicalIDs) > 0 {
		data["canonical_id"] = canonicalIDs[0]
		data["canonical_location_ids"] = canonicalIDs
	}
	if err := publisher.Publish(events.Event{
		Type:    "data.ready",
		Source:  serviceID,
		Subject: subject,
		Data:    data,
	}); err != nil {
		log.Printf("data ready publish failed for %s/%s: %v", kind, subject, err)
	}
}

func publishLocationDiscovery(publisher events.Publisher, loc locationXML, payload map[string]any, kind string, capability string) {
	purpose := ""
	switch capability {
	case "surface_observation":
		purpose = "observation"
	case "aviation_observation":
		purpose = "aviation"
	case "marine_observation":
		purpose = "marine_observation"
	case "air_quality":
		purpose = "air_quality"
	case "climate":
		purpose = "climate"
	}
	configuredInput, configured := locationclient.ConfiguredIdentifier(loc.Source, purpose, loc.ID)
	identifierScheme, identifierAuthority := configuredInput.Scheme, configuredInput.Authority
	if !configured {
		identifierScheme = "provider"
		identifierAuthority = sourceKind(loc.Source)
	}
	name := firstNonBlank(
		loc.NameOverride,
		localizedText(payload["station"], "en"),
		localizedText(payload["location"], "en"),
		localizedText(payload["name"], "en"),
		loc.ID,
	)
	identifiers := []map[string]any{{
		"authority": identifierAuthority, "scheme": identifierScheme,
		"value": loc.ID, "normalized_value": strings.ToUpper(strings.TrimSpace(loc.ID)),
		"primary": true, "confidence": "exact", "source_id": serviceID,
	}}
	stationID := strings.TrimSpace(textValue(payload["station_id"]))
	if stationID != "" && !strings.EqualFold(stationID, loc.ID) {
		identifiers = append(identifiers, map[string]any{
			"authority": sourceKind(loc.Source), "scheme": "eccc_station",
			"value": stationID, "normalized_value": strings.ToUpper(stationID),
			"primary": false, "confidence": "high", "source_id": serviceID,
		})
	}
	entity := map[string]any{
		"id": "", "kind": kind, "capabilities": []string{capability},
		"country": "CA", "lifecycle_status": "unknown", "reporting_status": "recently_reporting",
		"source_quality": 0.8, "identifiers": identifiers,
		"names": []map[string]any{{
			"locale": "en-CA", "value": name, "normalized_value": strings.ToLower(name),
			"name_kind": "canonical", "primary": true, "source_id": serviceID,
		}},
		"attributes": map[string]any{"discovered_from": "validated_data_ingest"},
	}
	if geometry := discoveryPoint(loc, payload); geometry != nil {
		entity["geometry"] = geometry
	}
	publishOverlayUpsert(publisher, loc.ID, entity)
}

func publishForecastDiscovery(publisher events.Publisher, region coverageRegionXML, forecastID string, payload map[string]any) {
	name := firstNonBlank(
		localizedText(payload["name"], "en"),
		region.Name,
		region.ID,
	)
	entity := map[string]any{
		"id": "", "kind": "public_forecast_location", "capabilities": []string{"forecast"},
		"country": "CA", "lifecycle_status": "unknown", "reporting_status": "not_applicable",
		"source_quality": 0.85,
		"identifiers": []map[string]any{
			{"authority": "eccc", "scheme": "clc", "value": region.ID, "normalized_value": strings.ToUpper(region.ID), "primary": true, "confidence": "exact", "source_id": serviceID},
			{"authority": "eccc", "scheme": "eccc_citypage", "value": forecastID, "normalized_value": strings.ToUpper(forecastID), "primary": false, "confidence": "exact", "source_id": serviceID},
		},
		"names": []map[string]any{{
			"locale": "en-CA", "value": name, "normalized_value": strings.ToLower(name),
			"name_kind": "canonical", "primary": true, "source_id": serviceID,
		}},
		"attributes": map[string]any{"discovered_from": "validated_data_ingest"},
	}
	publishOverlayUpsert(publisher, region.ID, entity)
}

func publishOverlayUpsert(publisher events.Publisher, subject string, entity map[string]any) {
	requestID := fmt.Sprintf("data-location-%d", time.Now().UnixNano())
	if err := publisher.Publish(events.Event{
		Type:    "location.overlay.upsert.request",
		Source:  serviceID,
		Target:  "haze-location",
		Subject: requestID,
		Data: map[string]any{
			"api_version": 1,
			"request_id":  requestID,
			"source_id":   serviceID,
			"expires_at":  time.Now().UTC().Add(30 * 24 * time.Hour).Format(time.RFC3339),
			"entity":      entity,
			"raw_subject": subject,
		},
	}); err != nil {
		log.Printf("location discovery publish failed for %s: %v", subject, err)
	}
}

func discoveryPoint(loc locationXML, payload map[string]any) map[string]any {
	latitude, latOK := numberValue(payload["latitude"])
	longitude, lonOK := numberValue(payload["longitude"])
	if value, err := strconv.ParseFloat(strings.TrimSpace(loc.Latitude), 64); err == nil {
		latitude, latOK = value, true
	}
	if value, err := strconv.ParseFloat(strings.TrimSpace(loc.Longitude), 64); err == nil {
		longitude, lonOK = value, true
	}
	if !latOK || !lonOK || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 {
		return nil
	}
	return map[string]any{
		"geometry_type": "point", "latitude": latitude, "longitude": longitude,
		"bbox": []float64{longitude, latitude, longitude, latitude}, "source_id": serviceID,
	}
}

func openStore(ctx context.Context, cfg loadedConfig) (datastore.Store, error) {
	store, err := datastore.Open(ctx, cfg.Root.Storage, cfg.BaseDir)
	if err != nil {
		return nil, err
	}
	log.Printf("datastore connected")
	return store, nil
}

func persistObservation(ctx context.Context, store datastore.Store, loc locationXML, payload map[string]any) error {
	if store == nil {
		return datastore.ErrNotConfigured
	}
	source := sourceKind(loc.Source)
	name := firstNonBlank(loc.NameOverride, localizedText(payload["station"], "en"), loc.ID)
	stationID := fallbackText(textValue(payload["station_id"]), loc.ID)
	latitude, longitude := configuredLocationPoint(loc)
	attachCanonicalLocation(payload, loc.CanonicalID, loc.Source, loc.ID)
	if err := store.UpsertLocation(ctx, datastore.LocationRecord{
		CanonicalID: loc.CanonicalID,
		Source:      source,
		LocationID:  loc.ID,
		Kind:        "observation_station",
		NameEN:      name,
		NameFR:      firstNonBlank(localizedText(payload["station"], "fr"), name),
		StationID:   stationID,
		CityPageID:  cityPageID(source, loc.ID),
		Latitude:    latitude,
		Longitude:   longitude,
		Metadata: map[string]any{
			"configured_source": loc.Source,
			"name_override":     loc.NameOverride,
		},
	}); err != nil {
		return err
	}
	return store.UpsertObservation(ctx, datastore.ObservationRecord{
		Source:        source,
		LocationID:    loc.ID,
		StationID:     stationID,
		ObservedAtRaw: textValue(payload["observed_at"]),
		Payload:       payload,
	})
}

func persistForecast(ctx context.Context, store datastore.Store, region coverageRegionXML, forecastID string, payload map[string]any) error {
	if store == nil {
		return datastore.ErrNotConfigured
	}
	source := sourceKind(region.Source)
	nameBlock, _ := payload["name"].(map[string]any)
	nameEN := firstNonBlank(localizedText(nameBlock, "en"), region.Name, region.ID)
	attachCanonicalLocation(payload, region.CanonicalID, region.Source, region.ID)
	if err := store.UpsertLocation(ctx, datastore.LocationRecord{
		CanonicalID: region.CanonicalID,
		Source:      source,
		LocationID:  region.ID,
		Kind:        "forecast_region",
		NameEN:      nameEN,
		NameFR:      firstNonBlank(localizedText(nameBlock, "fr"), nameEN),
		CityPageID:  forecastID,
		CLC:         region.ID,
		Metadata: map[string]any{
			"derive_forecast": forecastID,
			"configured_name": region.Name,
		},
	}); err != nil {
		return err
	}
	return store.UpsertForecast(ctx, datastore.ForecastRecord{
		Source:       source,
		ForecastID:   forecastID,
		RegionID:     region.ID,
		IssuedAtRaw:  firstNonBlank(textValue(payload["issued_at"]), textValue(payload["published_at"])),
		UpdatedAtRaw: firstNonBlank(textValue(payload["updated_at"]), textValue(payload["last_updated"])),
		Payload:      payload,
	})
}

func persistAirQuality(ctx context.Context, store datastore.Store, loc locationXML, payload map[string]any) error {
	if store == nil {
		return datastore.ErrNotConfigured
	}
	source := sourceKind(loc.Source)
	name := firstNonBlank(loc.NameOverride, localizedText(payload["location"], "en"), loc.ID)
	latitude, longitude := configuredLocationPoint(loc)
	attachCanonicalLocation(payload, loc.CanonicalID, loc.Source, loc.ID)
	if err := store.UpsertLocation(ctx, datastore.LocationRecord{
		CanonicalID: loc.CanonicalID,
		Source:      source,
		LocationID:  loc.ID,
		Kind:        "air_quality_location",
		NameEN:      name,
		NameFR:      firstNonBlank(localizedText(payload["location"], "fr"), name),
		Latitude:    latitude,
		Longitude:   longitude,
		Metadata: map[string]any{
			"configured_source": loc.Source,
			"name_override":     loc.NameOverride,
		},
	}); err != nil {
		return err
	}
	return store.StoreProductPayload(ctx, datastore.ProductPayloadRecord{
		Kind:    "air_quality",
		Source:  source,
		ID:      loc.ID,
		Payload: payload,
	})
}

func persistClimateSummary(ctx context.Context, store datastore.Store, loc locationXML, payload map[string]any) error {
	if store == nil {
		return datastore.ErrNotConfigured
	}
	source := sourceKind(loc.Source)
	name := firstNonBlank(loc.NameOverride, localizedText(payload["name"], "en"), loc.ID)
	latitude, longitude := configuredLocationPoint(loc)
	attachCanonicalLocation(payload, loc.CanonicalID, loc.Source, loc.ID)
	if err := store.UpsertLocation(ctx, datastore.LocationRecord{
		CanonicalID: loc.CanonicalID,
		Source:      source,
		LocationID:  loc.ID,
		Kind:        "climate_location",
		NameEN:      name,
		NameFR:      firstNonBlank(localizedText(payload["name"], "fr"), name),
		Latitude:    latitude,
		Longitude:   longitude,
		Metadata: map[string]any{
			"configured_source": loc.Source,
			"name_override":     loc.NameOverride,
			"normal_id":         loc.NormalID,
		},
	}); err != nil {
		return err
	}
	return store.StoreProductPayload(ctx, datastore.ProductPayloadRecord{
		Kind:    "climate_summary",
		Source:  source,
		ID:      loc.ID,
		Payload: payload,
	})
}

func persistMarineForecast(ctx context.Context, store datastore.Store, loc locationXML, payload map[string]any) error {
	if store == nil {
		return datastore.ErrNotConfigured
	}
	source := sourceKind(loc.Source)
	props := mapAt(payload, "properties")
	area := mapAt(props, "area")
	name := firstNonBlank(loc.NameOverride, localizedText(mapAt(area, "value"), "en"), loc.ID)
	latitude, longitude := configuredLocationPoint(loc)
	attachCanonicalLocation(payload, loc.CanonicalID, loc.Source, loc.ID)
	if err := store.UpsertLocation(ctx, datastore.LocationRecord{
		CanonicalID: loc.CanonicalID,
		Source:      source,
		LocationID:  loc.ID,
		Kind:        "marine_forecast_area",
		NameEN:      name,
		NameFR:      firstNonBlank(localizedText(mapAt(area, "value"), "fr"), name),
		Latitude:    latitude,
		Longitude:   longitude,
		Metadata: map[string]any{
			"configured_source": loc.Source,
			"name_override":     loc.NameOverride,
		},
	}); err != nil {
		return err
	}
	return store.StoreProductPayload(ctx, datastore.ProductPayloadRecord{
		Kind:    "marine_forecast",
		Source:  source,
		ID:      loc.ID,
		Payload: payload,
	})
}

func configuredLocationPoint(loc locationXML) (*float64, *float64) {
	latitude, longitude, ok := configuredPoint(loc)
	if !ok {
		return nil, nil
	}
	return &latitude, &longitude
}

func attachCanonicalLocation(payload map[string]any, canonicalID string, source string, rawID string) {
	canonicalID = strings.TrimSpace(canonicalID)
	if payload == nil || canonicalID == "" {
		return
	}
	payload["_location"] = map[string]any{
		"canonical_id": canonicalID,
		"raw_source":   strings.TrimSpace(source),
		"raw_id":       strings.TrimSpace(rawID),
	}
}

func cityPageID(source string, id string) string {
	if source == "eccc" {
		return strings.TrimSpace(id)
	}
	return ""
}

func localizedText(value any, lang string) string {
	switch typed := value.(type) {
	case map[string]any:
		return textValue(typed[lang])
	case map[string]string:
		return strings.TrimSpace(typed[lang])
	default:
		return textValue(value)
	}
}

func textValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case nil:
		return ""
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func anySlice(value any) []any {
	typed, _ := value.([]any)
	return typed
}

func mapValue(value any) map[string]any {
	typed, _ := value.(map[string]any)
	return typed
}

func mapAt(source map[string]any, key string) map[string]any {
	if source == nil {
		return nil
	}
	value, _ := source[key].(map[string]any)
	return value
}

func bilingual(source map[string]any, keys ...string) map[string]any {
	current := source
	for _, key := range keys {
		current = mapAt(current, key)
	}
	out := map[string]any{}
	for _, lang := range []string{"en", "fr"} {
		if value, ok := current[lang]; ok {
			out[lang] = value
		}
	}
	return out
}

func ev(source map[string]any, keys ...string) any {
	current := source
	for _, key := range keys {
		current = mapAt(current, key)
	}
	if value, ok := current["en"]; ok {
		return value
	}
	return nil
}

func textLang(source map[string]any, lang string) string {
	if source == nil {
		return ""
	}
	value, _ := source[lang].(string)
	return strings.TrimSpace(value)
}

func textAt(source map[string]any, key string) string {
	value, _ := source[key].(string)
	return strings.TrimSpace(value)
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			return text
		}
	}
	return ""
}

func isDigits(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func setDefaultStationID(payload map[string]any, stationID string) {
	if payload == nil || strings.TrimSpace(textValue(payload["station_id"])) != "" {
		return
	}
	payload["station_id"] = strings.TrimSpace(stationID)
}

func configDuration(raw string, fallback time.Duration) time.Duration {
	if parsed, err := time.ParseDuration(strings.TrimSpace(raw)); err == nil && parsed > 0 {
		return parsed
	}
	return fallback
}

func sourceKind(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return "eccc"
	}
	if value == "swob" || value == "eccc-swob" || value == "eccc_swob" || value == "msc-swob" || value == "msc_swob" {
		return "eccc"
	}
	if value == "weather.com" || value == "weatherdotcom" {
		return "twc"
	}
	return value
}

func resolvePath(base string, value string) string {
	if filepath.IsAbs(value) {
		return filepath.Clean(value)
	}
	return filepath.Clean(filepath.Join(base, value))
}

func fallbackText(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return strings.TrimSpace(fallback)
	}
	return strings.TrimSpace(value)
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
