package productrender

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/ecccclimate"
)

const liveClimateAPIBase = "https://api.weather.gc.ca"

type liveClimateFeature struct {
	Properties map[string]any `json:"properties"`
}

type liveClimateFeatureCollection struct {
	Features  []liveClimateFeature `json:"features"`
	TimeStamp string               `json:"timeStamp"`
}

// fetchLiveClimateSnapshot retrieves a current daily climate record only for
// on-demand rendering. Routine feeds retain the data-ingest snapshot path.
func fetchLiveClimateSnapshot(loc locationXML, timezone string) (string, liveClimateFile, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	return fetchLiveClimateSnapshotFromBase(ctx, loc, timezone, liveClimateAPIBase)
}

func fetchLiveClimateSnapshotFromBase(ctx context.Context, loc locationXML, timezone string, apiBase string) (string, liveClimateFile, bool) {
	requestedID := strings.ToUpper(strings.TrimSpace(loc.ID))
	if canonicalSource(loc.Source) != "eccc" || requestedID == "" {
		return "", liveClimateFile{}, false
	}
	apiBase = strings.TrimRight(strings.TrimSpace(apiBase), "/")
	if apiBase == "" {
		apiBase = liveClimateAPIBase
	}
	dailyID := requestedID
	virtual, virtualFound, virtualErr := ecccclimate.ResolveVirtualStationFromBase(ctx, nil, apiBase, requestedID)
	if virtualErr != nil {
		return "", liveClimateFile{}, false
	}
	if ecccclimate.IsVirtualClimateID(requestedID) {
		if !virtualFound || strings.TrimSpace(virtual.CurrentClimateID) == "" {
			return "", liveClimateFile{}, false
		}
		dailyID = virtual.CurrentClimateID
	}

	query := url.Values{}
	query.Set("f", "json")
	query.Set("limit", "14")
	query.Set("sortby", "-LOCAL_DATE")
	query.Set("CLIMATE_IDENTIFIER", dailyID)
	endpoint := apiBase + "/collections/climate-daily/items?" + query.Encode()
	var collection liveClimateFeatureCollection
	if err := fetchJSON(ctx, endpoint, &collection); err != nil {
		return "", liveClimateFile{}, false
	}
	for _, feature := range collection.Features {
		if !liveClimateDailyUsable(feature.Properties, time.Now().UTC()) {
			continue
		}
		stationName := liveFirstNonBlank(loc.NameOverride, virtual.Name, liveClimateText(feature.Properties["STATION_NAME"]), requestedID)
		raw, ok := buildLiveClimateFile(feature.Properties, stationName, collection.TimeStamp)
		if !ok {
			continue
		}
		input := "eccc:climate/" + dailyID
		if virtualFound {
			input += ";eccc:ltce/" + virtual.ID
		}
		return input, raw, true
	}
	return "", liveClimateFile{}, false
}

func buildLiveClimateFile(properties map[string]any, stationName string, timestamp string) (liveClimateFile, bool) {
	if len(properties) == 0 {
		return liveClimateFile{}, false
	}
	payload := map[string]any{
		"source":       "eccc",
		"name":         map[string]any{"en": stationName, "fr": stationName},
		"last_updated": liveFirstNonBlank(timestamp, time.Now().UTC().Format(time.RFC3339)),
		"observations": map[string]any{
			"station":             map[string]any{"en": stationName, "fr": stationName},
			"date":                liveClimateText(properties["LOCAL_DATE"]),
			"high":                liveClimateNumber(properties, "MAX_TEMPERATURE"),
			"low":                 liveClimateNumber(properties, "MIN_TEMPERATURE"),
			"mean":                liveClimateNumber(properties, "MEAN_TEMPERATURE"),
			"precipitation":       liveClimateNumber(properties, "TOTAL_PRECIPITATION"),
			"precipitation_trace": liveClimateTrace(properties, "TOTAL_PRECIPITATION"),
			"rain":                liveClimateNumber(properties, "TOTAL_RAIN"),
			"rain_trace":          liveClimateTrace(properties, "TOTAL_RAIN"),
			"snowfall":            liveClimateNumber(properties, "TOTAL_SNOW"),
			"snowfall_trace":      liveClimateTrace(properties, "TOTAL_SNOW"),
			"snow_on_ground":      liveClimateNumber(properties, "SNOW_ON_GROUND"),
			"max_gust_speed":      liveClimateNumber(properties, "SPEED_MAX_GUST"),
			"max_gust_direction":  liveClimateGustDirection(properties),
			"heating_degree_days": liveClimateNumber(properties, "HEATING_DEGREE_DAYS"),
			"cooling_degree_days": liveClimateNumber(properties, "COOLING_DEGREE_DAYS"),
			"min_humidity":        liveClimateNumber(properties, "MIN_REL_HUMIDITY"),
		},
	}
	var raw liveClimateFile
	if !decodeLiveValue(payload, &raw) || len(climateLines(raw, "en", "UTC")) == 0 {
		return liveClimateFile{}, false
	}
	return raw, true
}

func liveClimateDailyUsable(properties map[string]any, now time.Time) bool {
	if len(properties) == 0 {
		return false
	}
	if date, ok := liveClimateDate(liveClimateText(properties["LOCAL_DATE"])); ok && date.After(now.Add(24*time.Hour)) {
		return false
	}
	for _, field := range liveClimateDailyFields() {
		if liveClimateNumber(properties, field) != nil || liveClimateTrace(properties, field) {
			return true
		}
	}
	return false
}

func liveClimateDailyFields() []string {
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

func liveClimateNumber(properties map[string]any, field string) any {
	if !liveClimateFlagOK(liveClimateText(properties[field+"_FLAG"]), field) {
		return nil
	}
	value, ok := liveClimateFloat(properties[field])
	if !ok {
		return nil
	}
	return math.Round(value*10) / 10
}

func liveClimateTrace(properties map[string]any, field string) bool {
	return liveClimateTraceCapable(field) && strings.EqualFold(liveClimateText(properties[field+"_FLAG"]), "T")
}

func liveClimateFlagOK(flag string, field string) bool {
	flag = strings.ToUpper(strings.TrimSpace(flag))
	return flag == "" || flag == "T" && liveClimateTraceCapable(field)
}

func liveClimateTraceCapable(field string) bool {
	switch field {
	case "TOTAL_PRECIPITATION", "TOTAL_RAIN", "TOTAL_SNOW", "SNOW_ON_GROUND":
		return true
	default:
		return false
	}
}

func liveClimateGustDirection(properties map[string]any) any {
	value, ok := liveClimateFloat(liveClimateNumber(properties, "DIRECTION_MAX_GUST"))
	if !ok {
		return nil
	}
	if value > 0 && value <= 36 {
		value *= 10
	}
	directions := []string{"N", "NE", "E", "SE", "S", "SW", "W", "NW"}
	index := int(math.Floor(math.Mod(value+22.5, 360)/45)) % len(directions)
	return directions[index]
}

func liveClimateFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
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

func liveClimateDate(raw string) (time.Time, bool) {
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02", time.RFC3339} {
		if parsed, err := time.Parse(layout, strings.TrimSpace(raw)); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func liveClimateText(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}
