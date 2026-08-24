// Package ecccclimate contains small, provider-specific helpers shared by
// services that read ECCC climate data.
package ecccclimate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultAPIBase = "https://api.weather.gc.ca"

const virtualStationCacheTTL = 6 * time.Hour

// VirtualStation identifies the current physical climate station that ECCC
// uses as the active member of a long-term virtual climate record.
//
// Virtual stations are valuable for historical extremes, but they cannot be
// queried from climate-daily directly. Callers must use CurrentClimateID for
// current daily observations.
type VirtualStation struct {
	ID               string
	Name             string
	NameFR           string
	CitypageID       string
	CurrentClimateID string
}

type virtualClimateFeature struct {
	Properties map[string]any `json:"properties"`
}

type virtualClimateCollection struct {
	Features []virtualClimateFeature `json:"features"`
}

type virtualCacheEntry struct {
	station   VirtualStation
	found     bool
	expiresAt time.Time
}

var virtualStationCache = struct {
	sync.Mutex
	entries map[string]virtualCacheEntry
}{entries: make(map[string]virtualCacheEntry)}

// IsVirtualClimateID reports whether value has ECCC's virtual-climate ID
// shape. Provider lookup still verifies that the ID exists before it is used.
func IsVirtualClimateID(value string) bool {
	value = normalizeVirtualID(value)
	if len(value) < 6 || !strings.HasPrefix(value, "VS") || !strings.HasSuffix(value, "V") {
		return false
	}
	for _, character := range value[2 : len(value)-1] {
		if character < 'A' || character > 'Z' {
			if character < '0' || character > '9' {
				return false
			}
		}
	}
	return true
}

// ResolveVirtualStation resolves an ECCC virtual climate ID using the
// production GeoMet endpoint. Non-virtual IDs return found=false.
func ResolveVirtualStation(ctx context.Context, client *http.Client, value string) (station VirtualStation, found bool, err error) {
	return ResolveVirtualStationFromBase(ctx, client, defaultAPIBase, value)
}

// ResolveVirtualStationFromBase is ResolveVirtualStation with an injectable
// API base for focused integration tests.
func ResolveVirtualStationFromBase(ctx context.Context, client *http.Client, apiBase string, value string) (station VirtualStation, found bool, err error) {
	identifier := normalizeVirtualID(value)
	if !IsVirtualClimateID(identifier) {
		return VirtualStation{}, false, nil
	}
	apiBase = strings.TrimRight(strings.TrimSpace(apiBase), "/")
	if apiBase == "" {
		apiBase = defaultAPIBase
	}
	cacheKey := apiBase + "\x00" + identifier
	now := time.Now()
	virtualStationCache.Lock()
	if cached, ok := virtualStationCache.entries[cacheKey]; ok && cached.expiresAt.After(now) {
		virtualStationCache.Unlock()
		return cached.station, cached.found, nil
	}
	virtualStationCache.Unlock()

	query := url.Values{}
	query.Set("f", "json")
	query.Set("limit", "1000")
	query.Set("VIRTUAL_CLIMATE_ID", identifier)
	requestURL := apiBase + "/collections/ltce-stations/items?" + query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return VirtualStation{}, false, err
	}
	req.Header.Set("User-Agent", "HazeWeatherRadio/26.08 climate resolver")
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second}
	}
	response, err := client.Do(req)
	if err != nil {
		return VirtualStation{}, false, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return VirtualStation{}, false, fmt.Errorf("ECCC virtual climate lookup returned %s", response.Status)
	}
	var collection virtualClimateCollection
	if err := json.NewDecoder(response.Body).Decode(&collection); err != nil {
		return VirtualStation{}, false, err
	}

	station = VirtualStation{ID: identifier}
	currentCounts := make(map[string]int)
	matched := false
	for _, feature := range collection.Features {
		properties := feature.Properties
		if normalizeVirtualID(textValue(properties["VIRTUAL_CLIMATE_ID"])) != identifier {
			continue
		}
		matched = true
		station.Name = firstNonBlank(station.Name, textValue(properties["VIRTUAL_STATION_NAME_E"]))
		station.NameFR = firstNonBlank(station.NameFR, textValue(properties["VIRTUAL_STATION_NAME_F"]))
		station.CitypageID = firstNonBlank(station.CitypageID, textValue(properties["WXO_CITY_CODE"]))
		physicalID := strings.ToUpper(strings.TrimSpace(textValue(properties["CLIMATE_IDENTIFIER"])))
		if physicalID != "" && strings.TrimSpace(textValue(properties["END_DATE"])) == "" {
			currentCounts[physicalID]++
		}
	}
	if matched {
		station.CurrentClimateID = preferredCurrentID(currentCounts)
	}
	virtualStationCache.Lock()
	virtualStationCache.entries[cacheKey] = virtualCacheEntry{
		station:   station,
		found:     matched,
		expiresAt: now.Add(virtualStationCacheTTL),
	}
	virtualStationCache.Unlock()
	return station, matched, nil
}

func normalizeVirtualID(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	return strings.Map(func(character rune) rune {
		if character == ' ' || character == '\t' || character == '\r' || character == '\n' {
			return -1
		}
		return character
	}, value)
}

func preferredCurrentID(counts map[string]int) string {
	ids := make([]string, 0, len(counts))
	for identifier := range counts {
		ids = append(ids, identifier)
	}
	sort.Slice(ids, func(left int, right int) bool {
		if counts[ids[left]] != counts[ids[right]] {
			return counts[ids[left]] > counts[ids[right]]
		}
		return ids[left] < ids[right]
	})
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func textValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return fmt.Sprintf("%g", typed)
	default:
		return ""
	}
}
