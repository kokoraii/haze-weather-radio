package webgateway

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
)

const (
	publicFeedMapMaxAreas         = 5000
	publicFeedMapMaxAlertFeatures = 2000
	// Request a small number of equally exact candidates so an installed
	// lower-priority geometry pack can supply a polygon when the preferred core
	// catalog's geometry sidecar is unavailable.
	publicFeedMapCandidateLimit = 8
	// Area responses can contain large exact WKB-derived polygons. Keep each
	// broker response comfortably below the location client's bounded scanner.
	publicFeedMapBatchSize          = 1
	publicFeedMapResolveConcurrency = 16
	publicFeedMapTimeout            = 20 * time.Second
)

type feedAreaKey struct {
	Source string
	Code   string
}

type publicMapBounds struct {
	West  float64
	South float64
	East  float64
	North float64
}

type publicMapLocation struct {
	Key            string   `json:"key"`
	Source         string   `json:"source"`
	Name           string   `json:"name"`
	Country        string   `json:"country,omitempty"`
	SameCode       string   `json:"same_code"`
	SameCodes      []string `json:"same_codes,omitempty"`
	SGCCodes       []string `json:"sgc_codes,omitempty"`
	FIPSCodes      []string `json:"fips_codes,omitempty"`
	NWSCountyCodes []string `json:"nws_county_codes,omitempty"`
	NWSZoneCodes   []string `json:"nws_zone_codes,omitempty"`
	Latitude       *float64 `json:"latitude"`
	Longitude      *float64 `json:"longitude"`
}

type feedAreaResolveResult struct {
	batchIndex int
	response   locationclient.Response
	err        error
}

func (s *Server) publicFeedMap(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	access := publicFeedAccess(s.config)
	if access == "disabled" {
		http.NotFound(writer, request)
		return
	}
	if access == "auth_required" && !s.auth.FullyAuthenticated(request) {
		writeJSONStatus(writer, http.StatusUnauthorized, map[string]any{"detail": "not authenticated"})
		return
	}
	feedID := strings.TrimSpace(request.URL.Query().Get("feed"))
	if feedID == "" || len(feedID) > httpAudioMaxFeedID || !validPublicAudioFeedID(feedID) {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"detail": "feed is invalid"})
		return
	}
	alertsRequested := queryBool(request.URL.Query().Get("alerts"))
	alertBounds := publicMapBounds{}
	if alertsRequested {
		var ok bool
		alertBounds, ok = parsePublicMapBounds(request.URL.Query().Get("bbox"))
		if !ok {
			writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"detail": "map bounds are invalid"})
			return
		}
	}
	root, err := loadYAMLMap(s.configPath)
	if err != nil {
		writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]any{"detail": "feed configuration is unavailable"})
		return
	}
	feeds, err := loadFeedsXML(s.configPath, root)
	if err != nil {
		writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]any{"detail": "feed configuration is unavailable"})
		return
	}
	var feed feedXML
	found := false
	for _, candidate := range feeds.Feeds {
		if strings.EqualFold(strings.TrimSpace(candidate.ID), feedID) && xmlBool(candidate.EnabledRaw, true) {
			feed, found = candidate, true
			break
		}
	}
	if !found {
		http.NotFound(writer, request)
		return
	}
	clcNames := loadCLCNames(resolveConfigPath(s.configPath, "managed/csv/CLC_Base_Zone.csv"))
	nwsNames := loadNWSFIPSNames(resolveConfigPath(s.configPath, "managed/csv/NWS_ZONE_COUNTY_CORRELATION.csv"))
	keys := feedAreaKeys(feed, clcNames, nwsNames)
	if len(keys) > publicFeedMapMaxAreas {
		writeJSONStatus(writer, http.StatusUnprocessableEntity, map[string]any{"detail": "feed coverage exceeds the public map area limit"})
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), publicFeedMapTimeout)
	defer cancel()
	locations := []publicMapLocation{}
	features := []map[string]any{}
	generation := ""
	packs := []string{}
	if len(keys) > 0 {
		locations, features, generation, packs, err = s.resolveFeedAreaFeatures(ctx, keys)
		if err != nil {
			writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]any{"detail": "location map data is unavailable"})
			return
		}
	}
	alerts := geoJSONFeatureCollection(nil)
	alertTruncated := false
	if alertsRequested {
		alerts, alertTruncated = s.activeMapAlertFeatures(ctx, alertBounds, clcNames, nwsNames)
	}
	writeJSON(writer, map[string]any{
		"feed_id":            feedID,
		"catalog_generation": generation,
		"catalog_packs":      packs,
		"locations":          locations,
		"coverage":           geoJSONFeatureCollection(features),
		"alerts":             alerts,
		"alert_count":        len(alerts["features"].([]map[string]any)),
		"alert_truncated":    alertTruncated,
	})
}

func emptyPublicFeedMap(feedID string) map[string]any {
	return map[string]any{
		"feed_id": feedID, "catalog_generation": "", "catalog_packs": []string{},
		"locations": []publicMapLocation{}, "coverage": geoJSONFeatureCollection(nil),
		"alerts": geoJSONFeatureCollection(nil), "alert_count": 0,
	}
}

func queryBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func parsePublicMapBounds(raw string) (publicMapBounds, bool) {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	if len(parts) != 4 {
		return publicMapBounds{}, false
	}
	values := [4]float64{}
	for index, part := range parts {
		value, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return publicMapBounds{}, false
		}
		values[index] = value
	}
	bounds := publicMapBounds{West: values[0], South: values[1], East: values[2], North: values[3]}
	if bounds.West < -180 || bounds.East > 180 || bounds.South < -90 || bounds.North > 90 || bounds.West >= bounds.East || bounds.South >= bounds.North {
		return publicMapBounds{}, false
	}
	return bounds, true
}

func feedAreaKeys(feed feedXML, clcNames map[string]string, nwsNames map[string]string) []feedAreaKey {
	seen := map[string]feedAreaKey{}
	add := func(source string, rawCode string) {
		code := cleanLocationCode(rawCode)
		if code == "" || code == "000000" {
			return
		}
		source = normalizeCoverageSource(source)
		if source == "" {
			_, inCLC := clcNames[code]
			_, inNWS := nwsNames[code]
			if inCLC {
				addKey := feedAreaKey{Source: "clc", Code: code}
				seen[addKey.Source+"\x00"+code] = addKey
			}
			if inNWS {
				addKey := feedAreaKey{Source: "nws_same", Code: code}
				seen[addKey.Source+"\x00"+code] = addKey
			}
			return
		}
		key := feedAreaKey{Source: source, Code: code}
		seen[source+"\x00"+code] = key
	}
	if feedCoversAllLocations(feed) {
		for code := range clcNames {
			add("clc", code)
		}
		for code := range nwsNames {
			add("nws_same", code)
		}
	} else {
		for _, region := range feed.Locations.Coverage.Regions {
			for _, code := range expandedCoverageRegionIDs(region.ID, clcNames) {
				add(region.Source, code)
			}
			for _, code := range expandedCoverageSubregionIDs(region, clcNames) {
				add(region.Source, code)
			}
		}
	}
	out := make([]feedAreaKey, 0, len(seen))
	for _, key := range seen {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Source == out[j].Source {
			return out[i].Code < out[j].Code
		}
		return out[i].Source < out[j].Source
	})
	return out
}

func normalizeCoverageSource(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "eccc", "ec", "msc", "clc", "cap_cp", "cap-cp":
		return "clc"
	case "nws", "nws_same", "same", "fips", "nws_cap", "nws-cap":
		return "nws_same"
	default:
		return ""
	}
}

func (s *Server) resolveFeedAreaFeatures(ctx context.Context, keys []feedAreaKey) ([]publicMapLocation, []map[string]any, string, []string, error) {
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return nil, nil, "", nil, fmt.Errorf("location bridge is unavailable")
	}
	if len(keys) == 0 {
		return []publicMapLocation{}, []map[string]any{}, "", []string{}, nil
	}

	batchCount := (len(keys) + publicFeedMapBatchSize - 1) / publicFeedMapBatchSize
	workerCount := min(batchCount, publicFeedMapResolveConcurrency)
	resolveContext, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	results := make(chan feedAreaResolveResult, workerCount)
	var workers sync.WaitGroup
	for workerIndex := 0; workerIndex < workerCount; workerIndex++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-resolveContext.Done():
					return
				case batchIndex, ok := <-jobs:
					if !ok {
						return
					}
					start := batchIndex * publicFeedMapBatchSize
					end := min(start+publicFeedMapBatchSize, len(keys))
					release, err := s.acquireFeedMapResolveSlot(resolveContext)
					response := locationclient.Response{}
					if err == nil {
						client := locationclient.New(bridgeAddr, "haze-web-public-feed-map")
						client.Timeout = publicFeedMapTimeout
						response, err = client.Query(resolveContext, feedAreaLocationRequest(keys[start:end]))
						release()
					}
					select {
					case results <- feedAreaResolveResult{batchIndex: batchIndex, response: response, err: err}:
						if err != nil {
							cancel()
							return
						}
					case <-resolveContext.Done():
						return
					}
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
			select {
			case jobs <- batchIndex:
			case <-resolveContext.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	responses := make([]locationclient.Response, batchCount)
	completed := make([]bool, batchCount)
	var resolveErr error
	for result := range results {
		if result.err != nil {
			if resolveErr == nil {
				resolveErr = fmt.Errorf("resolve feed map location batch %d: %w", result.batchIndex, result.err)
				cancel()
			}
			continue
		}
		responses[result.batchIndex] = result.response
		completed[result.batchIndex] = true
	}
	if resolveErr != nil {
		return nil, nil, "", nil, resolveErr
	}
	if err := resolveContext.Err(); err != nil {
		return nil, nil, "", nil, err
	}

	locations := make([]publicMapLocation, 0, len(keys))
	features := make([]map[string]any, 0, len(keys))
	generation := ""
	packs := []string{}
	catalogIdentitySet := false
	for batchIndex, response := range responses {
		if !completed[batchIndex] {
			return nil, nil, "", nil, fmt.Errorf("location map batch %d did not complete", batchIndex)
		}
		start := batchIndex * publicFeedMapBatchSize
		end := min(start+publicFeedMapBatchSize, len(keys))
		batchKeys := keys[start:end]
		if !catalogIdentitySet {
			generation = response.CatalogGeneration
			packs = append([]string(nil), response.CatalogPacks...)
			catalogIdentitySet = true
		} else if generation != response.CatalogGeneration || !sameCatalogPacks(packs, response.CatalogPacks) {
			return nil, nil, "", nil, fmt.Errorf("location catalog changed while the feed map was being resolved")
		}
		for _, batch := range response.Batches {
			if batch.InputIndex < 0 || batch.InputIndex >= len(batchKeys) || len(batch.Results) == 0 {
				continue
			}
			key := batchKeys[batch.InputIndex]
			var entity locationclient.Entity
			var geometry map[string]any
			for _, candidate := range batch.Results {
				candidateGeometry, ok := candidate.Entity.Attributes["area_geometry"].(map[string]any)
				if ok && strings.TrimSpace(fmt.Sprint(candidateGeometry["type"])) != "" {
					entity, geometry = candidate.Entity, candidateGeometry
					break
				}
			}
			if geometry == nil {
				continue
			}
			sameCode, sameCodes, sgcCodes, fipsCodes, countyCodes, zoneCodes := publicMapLocationCodes(entity, key.Code)
			name := entity.DisplayName()
			locationKey := key.Source + ":" + key.Code
			latitude, longitude := (*float64)(nil), (*float64)(nil)
			if entity.Geometry != nil {
				latitude, longitude = entity.Geometry.Latitude, entity.Geometry.Longitude
			}
			locations = append(locations, publicMapLocation{
				Key: locationKey, Source: key.Source, Name: name, Country: strings.ToUpper(strings.TrimSpace(entity.Country)),
				SameCode: sameCode, SameCodes: sameCodes, SGCCodes: sgcCodes, FIPSCodes: fipsCodes,
				NWSCountyCodes: countyCodes, NWSZoneCodes: zoneCodes, Latitude: latitude, Longitude: longitude,
			})
			properties := map[string]any{
				"key": locationKey, "source": key.Source, "code": key.Code, "country": strings.ToUpper(strings.TrimSpace(entity.Country)),
				"same_code": sameCode, "same_codes": strings.Join(sameCodes, ", "), "sgc_codes": strings.Join(sgcCodes, ", "),
				"fips_codes": strings.Join(fipsCodes, ", "), "nws_county_codes": strings.Join(countyCodes, ", "),
				"nws_zone_codes": strings.Join(zoneCodes, ", "), "location_name": name,
				"provider_version": strings.TrimSpace(fmt.Sprint(entity.Attributes["provider_version"])),
			}
			if latitude != nil {
				properties["latitude"] = *latitude
			}
			if longitude != nil {
				properties["longitude"] = *longitude
			}
			features = append(features, map[string]any{
				"type": "Feature", "id": locationKey, "geometry": geometry,
				"properties": properties,
			})
		}
	}
	sort.SliceStable(locations, func(i, j int) bool {
		if locations[i].Name == locations[j].Name {
			return locations[i].SameCode < locations[j].SameCode
		}
		return locations[i].Name < locations[j].Name
	})
	return locations, features, generation, packs, nil
}

func (s *Server) acquireFeedMapResolveSlot(ctx context.Context) (func(), error) {
	if s == nil || s.feedMapResolveSlots == nil {
		return func() {}, nil
	}
	select {
	case s.feedMapResolveSlots <- struct{}{}:
		return func() { <-s.feedMapResolveSlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func feedAreaLocationRequest(keys []feedAreaKey) locationclient.Request {
	inputs := make([]locationclient.Input, 0, len(keys))
	for _, key := range keys {
		scheme, authority := "clc", "eccc"
		if key.Source == "nws_same" {
			scheme, authority = "same", "nws"
		}
		inputs = append(inputs, locationclient.Input{Kind: "identifier", Scheme: scheme, Authority: authority, Value: key.Code})
	}
	return locationclient.Request{
		APIVersion: locationclient.APIVersion, Operation: "batch_resolve", Inputs: inputs,
		Filters: locationclient.Filters{}, Options: locationclient.Options{
			Limit: publicFeedMapCandidateLimit, MinimumConfidence: "exact", IncludeInactive: true, IncludeAreaGeometry: true,
		},
	}
}

func sameCatalogPacks(left []string, right []string) bool {
	normalize := func(values []string) []string {
		seen := map[string]struct{}{}
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value != "" {
				seen[value] = struct{}{}
			}
		}
		out := make([]string, 0, len(seen))
		for value := range seen {
			out = append(out, value)
		}
		sort.Strings(out)
		return out
	}
	return reflect.DeepEqual(normalize(left), normalize(right))
}

func publicMapLocationCodes(entity locationclient.Entity, fallbackSame string) (string, []string, []string, []string, []string, []string) {
	attributes := entity.Attributes
	sameCode := ""
	if primary := mapLocationCodeValues(attributes["same_code"]); len(primary) > 0 {
		sameCode = primary[0]
	}
	sameCodes := mapLocationCodeValues(attributes["same_codes"], sameCode)
	if sameCode == "" && len(sameCodes) > 0 {
		sameCode = sameCodes[0]
	}
	if sameCode == "" {
		sameCode = strings.TrimSpace(fallbackSame)
		sameCodes = mapLocationCodeValues(sameCodes, sameCode)
	}

	switch strings.ToUpper(strings.TrimSpace(entity.Country)) {
	case "CA":
		return sameCode, sameCodes, mapLocationCodeValues(attributes["sgc_codes"], attributes["sgc_code"]), nil, nil, nil
	case "US":
		return sameCode, sameCodes,
			nil,
			mapLocationCodeValues(attributes["fips_codes"], attributes["fips"]),
			mapLocationCodeValues(attributes["nws_county_codes"], attributes["nws_county_code"]),
			mapLocationCodeValues(attributes["nws_zone_codes"], attributes["zones"], attributes["nws_zone_code"])
	default:
		return sameCode, sameCodes, nil, nil, nil, nil
	}
}

func mapLocationCodeValues(values ...any) []string {
	seen := map[string]struct{}{}
	var add func(any)
	add = func(value any) {
		switch typed := value.(type) {
		case string:
			code := strings.TrimSpace(typed)
			if code != "" {
				seen[code] = struct{}{}
			}
		case []string:
			for _, item := range typed {
				add(item)
			}
		case []any:
			for _, item := range typed {
				add(item)
			}
		}
	}
	for _, value := range values {
		add(value)
	}
	result := make([]string, 0, len(seen))
	for code := range seen {
		result = append(result, code)
	}
	sort.Strings(result)
	return result
}

func geoJSONFeatureCollection(features []map[string]any) map[string]any {
	if features == nil {
		features = []map[string]any{}
	}
	return map[string]any{"type": "FeatureCollection", "features": features}
}

func (s *Server) activeMapAlertFeatures(ctx context.Context, bounds publicMapBounds, clcNames map[string]string, nwsNames map[string]string) (map[string]any, bool) {
	now := time.Now().UTC()
	records, _, _ := archiveStoreRecordBuckets(s.configPath, now)
	keysByID := map[string]feedAreaKey{}
	for _, record := range records {
		if !archiveAlertInEffect(record.Alert, now) {
			continue
		}
		for _, info := range record.Alert.Infos {
			for _, area := range info.Areas {
				if capmodel.IsECCCThreatArea(area) {
					continue
				}
				if alertAreaHasExplicitGeometry(area) {
					continue
				}
				for _, key := range alertAreaKeys(area, clcNames, nwsNames) {
					keysByID[key.Source+"\x00"+key.Code] = key
				}
			}
		}
	}
	keys := make([]feedAreaKey, 0, len(keysByID))
	for _, key := range keysByID {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Source == keys[j].Source {
			return keys[i].Code < keys[j].Code
		}
		return keys[i].Source < keys[j].Source
	})
	if len(keys) > publicFeedMapMaxAreas {
		keys = keys[:publicFeedMapMaxAreas]
	}
	coverage := []map[string]any{}
	if len(keys) > 0 {
		_, resolved, _, _, err := s.resolveFeedAreaFeatures(ctx, keys)
		if err == nil {
			coverage = resolved
		}
	}
	return activeAlertFeaturesInBounds(records, coverage, bounds, clcNames, nwsNames, now)
}

func activeAlertFeaturesInBounds(records []archiveCAPRecord, coverage []map[string]any, bounds publicMapBounds, clcNames map[string]string, nwsNames map[string]string, now time.Time) (map[string]any, bool) {
	coverageByKey := map[string]map[string]any{}
	for _, feature := range coverage {
		properties, _ := feature["properties"].(map[string]any)
		source := normalizeCoverageSource(fmt.Sprint(properties["source"]))
		code := cleanLocationCode(fmt.Sprint(properties["code"]))
		if source != "" && code != "" {
			coverageByKey[source+"\x00"+code] = feature
		}
	}
	features := []map[string]any{}
	seen := map[string]struct{}{}
	truncated := false
	appendFeature := func(feature map[string]any) bool {
		if len(features) >= publicFeedMapMaxAlertFeatures {
			truncated = true
			return false
		}
		features = append(features, feature)
		return true
	}

recordsLoop:
	for _, record := range records {
		if !archiveAlertInEffect(record.Alert, now) {
			continue
		}
		for infoIndex, info := range record.Alert.Infos {
			hasThreatAreas := false
			for _, area := range info.Areas {
				if capmodel.IsECCCThreatArea(area) {
					hasThreatAreas = true
					break
				}
			}
			for areaIndex, area := range info.Areas {
				threatArea := capmodel.IsECCCThreatArea(area)
				if hasThreatAreas && !threatArea {
					continue
				}
				if threatArea && !capmodel.IsECCCActiveThreatArea(area) {
					continue
				}
				base := map[string]any{
					"alert_id": record.ID, "event": info.Event, "headline": info.Headline,
					"severity": strings.ToLower(info.Severity), "urgency": info.Urgency, "certainty": info.Certainty,
					"status": record.Alert.Status, "message_type": record.Alert.MessageType, "sender": record.Alert.Sender,
					"sender_name": info.SenderName, "effective": info.Effective, "onset": info.Onset, "expires": info.Expires,
					"area_description": area.Description, "category": strings.Join(info.Category, ", "), "response": strings.Join(info.Response, ", "),
				}
				if threatArea {
					base["threat_area"] = true
					base["threat_status"] = capmodel.ECCCThreatAreaStatus(area)
				}
				appendStormMapProperties(base, info.Storm)
				explicit := false
				for polygonIndex, raw := range area.Polygons {
					geometry, ok := capPolygonGeometry(raw)
					if !ok {
						continue
					}
					explicit = true
					id := fmt.Sprintf("%s:%d:%d:p%d", record.ID, infoIndex, areaIndex, polygonIndex)
					if _, ok := seen[id]; ok {
						continue
					}
					seen[id] = struct{}{}
					properties := cloneAnyMap(base)
					properties["geometry_method"] = "cap_polygon"
					if threatArea {
						properties["geometry_method"] = "cap_threat_polygon"
					}
					if geoJSONGeometryIntersectsBounds(geometry, bounds) && !appendFeature(map[string]any{"type": "Feature", "id": id, "geometry": geometry, "properties": properties}) {
						break recordsLoop
					}
				}
				for circleIndex, raw := range area.Circles {
					geometry, ok := capCircleGeometry(raw)
					if !ok {
						continue
					}
					explicit = true
					id := fmt.Sprintf("%s:%d:%d:c%d", record.ID, infoIndex, areaIndex, circleIndex)
					if _, ok := seen[id]; ok {
						continue
					}
					seen[id] = struct{}{}
					properties := cloneAnyMap(base)
					properties["geometry_method"] = "cap_circle"
					if threatArea {
						properties["geometry_method"] = "cap_threat_circle"
					}
					if geoJSONGeometryIntersectsBounds(geometry, bounds) && !appendFeature(map[string]any{"type": "Feature", "id": id, "geometry": geometry, "properties": properties}) {
						break recordsLoop
					}
				}
				if explicit {
					continue
				}
				if hasThreatAreas {
					continue
				}
				for _, key := range alertAreaKeys(area, clcNames, nwsNames) {
					coverageFeature := coverageByKey[key.Source+"\x00"+key.Code]
					if coverageFeature == nil {
						continue
					}
					id := fmt.Sprintf("%s:%d:%d:g%s:%s", record.ID, infoIndex, areaIndex, key.Source, key.Code)
					if _, ok := seen[id]; ok {
						continue
					}
					seen[id] = struct{}{}
					properties := cloneAnyMap(base)
					properties["geometry_method"] = "area_geocode"
					properties["same_code"] = key.Code
					properties["source"] = key.Source
					geometry, _ := coverageFeature["geometry"].(map[string]any)
					if geoJSONGeometryIntersectsBounds(geometry, bounds) && !appendFeature(map[string]any{"type": "Feature", "id": id, "geometry": geometry, "properties": properties}) {
						break recordsLoop
					}
				}
			}
		}
	}
	return geoJSONFeatureCollection(features), truncated
}

func archiveAlertInEffect(alert capmodel.Alert, now time.Time) bool {
	if archiveAlertExpired(alert, now) {
		return false
	}
	info := chooseArchiveInfo(alert)
	start := parseArchiveTime(info.Onset)
	if start.IsZero() {
		start = parseArchiveTime(info.Effective)
	}
	return start.IsZero() || !now.Before(start)
}

func alertAreaHasExplicitGeometry(area capmodel.AlertArea) bool {
	for _, raw := range area.Polygons {
		if _, ok := capPolygonGeometry(raw); ok {
			return true
		}
	}
	for _, raw := range area.Circles {
		if _, ok := capCircleGeometry(raw); ok {
			return true
		}
	}
	return false
}

func alertAreaKeys(area capmodel.AlertArea, clcNames map[string]string, nwsNames map[string]string) []feedAreaKey {
	seen := map[string]feedAreaKey{}
	for _, geocode := range area.Geocodes {
		code := capAreaSameCode(geocode)
		if code == "" {
			continue
		}
		_, isCLC := clcNames[code]
		_, isNWS := nwsNames[code]
		name := strings.ToLower(strings.TrimSpace(geocode.Name))
		if isCLC || (!isNWS && (strings.Contains(name, "clc") || strings.Contains(name, "location"))) {
			key := feedAreaKey{Source: "clc", Code: code}
			seen[key.Source+"\x00"+key.Code] = key
		}
		if isNWS || (!isCLC && (strings.Contains(name, "same") || strings.Contains(name, "fips") || strings.Contains(name, "nws"))) {
			key := feedAreaKey{Source: "nws_same", Code: code}
			seen[key.Source+"\x00"+key.Code] = key
		}
	}
	out := make([]feedAreaKey, 0, len(seen))
	for _, key := range seen {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Source == out[j].Source {
			return out[i].Code < out[j].Code
		}
		return out[i].Source < out[j].Source
	})
	return out
}

func geoJSONGeometryIntersectsBounds(geometry map[string]any, bounds publicMapBounds) bool {
	if geometry == nil {
		return false
	}
	minLon, minLat := math.Inf(1), math.Inf(1)
	maxLon, maxLat := math.Inf(-1), math.Inf(-1)
	var visit func(any)
	visit = func(value any) {
		reflected := reflect.ValueOf(value)
		if !reflected.IsValid() || (reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array) {
			return
		}
		if reflected.Len() >= 2 {
			lon, lonOK := numericCoordinate(reflected.Index(0))
			lat, latOK := numericCoordinate(reflected.Index(1))
			if lonOK && latOK {
				minLon, maxLon = math.Min(minLon, lon), math.Max(maxLon, lon)
				minLat, maxLat = math.Min(minLat, lat), math.Max(maxLat, lat)
				return
			}
		}
		for index := 0; index < reflected.Len(); index++ {
			visit(reflected.Index(index).Interface())
		}
	}
	visit(geometry["coordinates"])
	if math.IsInf(minLon, 1) {
		return false
	}
	return maxLon >= bounds.West && minLon <= bounds.East && maxLat >= bounds.South && minLat <= bounds.North
}

func numericCoordinate(value reflect.Value) (float64, bool) {
	for value.IsValid() && (value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer) {
		if value.IsNil() {
			return 0, false
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return 0, false
	}
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		result := value.Float()
		return result, !math.IsNaN(result) && !math.IsInf(result, 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(value.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(value.Uint()), true
	default:
		return 0, false
	}
}

func cloneAnyMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func appendStormMapProperties(properties map[string]any, storm *capmodel.StormInfo) {
	if properties == nil || storm == nil {
		return
	}
	if value := strings.TrimSpace(storm.Speed); value != "" {
		properties["storm_speed"] = value
	}
	if storm.DirectionDegrees != nil {
		properties["storm_direction_degrees"] = *storm.DirectionDegrees
	}
	if value := strings.TrimSpace(storm.GeometryType); value != "" {
		properties["storm_geometry_type"] = value
	}
	if len(storm.Points) > 0 {
		points := make([]string, 0, len(storm.Points))
		for _, point := range storm.Points {
			points = append(points, strconv.FormatFloat(point.Latitude, 'f', -1, 64)+","+strconv.FormatFloat(point.Longitude, 'f', -1, 64))
		}
		properties["storm_points"] = strings.Join(points, " ")
	}
	if value := strings.TrimSpace(storm.Time); value != "" {
		properties["storm_time"] = value
	}
	if value := strings.TrimSpace(storm.MotionDescription); value != "" {
		properties["motion_description"] = value
	}
	if value := strings.TrimSpace(storm.PositionDescription); value != "" {
		properties["storm_position_description"] = value
	}
	if value := strings.TrimSpace(storm.ReferenceLocationPoints); value != "" {
		properties["reference_location_points"] = value
	}
}

func capPolygonGeometry(raw string) (map[string]any, bool) {
	ring := [][]float64{}
	for _, pair := range strings.Fields(strings.TrimSpace(raw)) {
		parts := strings.Split(pair, ",")
		if len(parts) != 2 {
			return nil, false
		}
		latitude, errLat := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		longitude, errLon := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if errLat != nil || errLon != nil || !validCAPPoint(latitude, longitude) {
			return nil, false
		}
		ring = append(ring, []float64{longitude, latitude})
	}
	if len(ring) < 3 {
		return nil, false
	}
	if ring[0][0] != ring[len(ring)-1][0] || ring[0][1] != ring[len(ring)-1][1] {
		ring = append(ring, []float64{ring[0][0], ring[0][1]})
	}
	return map[string]any{"type": "Polygon", "coordinates": []any{ring}}, true
}

func capCircleGeometry(raw string) (map[string]any, bool) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) != 2 {
		return nil, false
	}
	center := strings.Split(parts[0], ",")
	if len(center) != 2 {
		return nil, false
	}
	latitude, errLat := strconv.ParseFloat(strings.TrimSpace(center[0]), 64)
	longitude, errLon := strconv.ParseFloat(strings.TrimSpace(center[1]), 64)
	radiusKM, errRadius := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if errLat != nil || errLon != nil || errRadius != nil || !validCAPPoint(latitude, longitude) || radiusKM <= 0 || radiusKM > 5000 {
		return nil, false
	}
	angularDistance := radiusKM / 6371.0088
	centerLat := latitude * math.Pi / 180
	centerLon := longitude * math.Pi / 180
	ring := make([][]float64, 0, 65)
	for index := 0; index <= 64; index++ {
		bearing := float64(index) * 2 * math.Pi / 64
		lat := math.Asin(math.Sin(centerLat)*math.Cos(angularDistance) + math.Cos(centerLat)*math.Sin(angularDistance)*math.Cos(bearing))
		lon := centerLon + math.Atan2(math.Sin(bearing)*math.Sin(angularDistance)*math.Cos(centerLat), math.Cos(angularDistance)-math.Sin(centerLat)*math.Sin(lat))
		ring = append(ring, []float64{lon * 180 / math.Pi, lat * 180 / math.Pi})
	}
	return map[string]any{"type": "Polygon", "coordinates": []any{ring}}, true
}

func validCAPPoint(latitude float64, longitude float64) bool {
	return !math.IsNaN(latitude) && !math.IsInf(latitude, 0) && !math.IsNaN(longitude) && !math.IsInf(longitude, 0) && latitude >= -90 && latitude <= 90 && longitude >= -180 && longitude <= 180
}

func capAreaSameCode(geocode capmodel.NameValue) string {
	name := strings.ToLower(strings.TrimSpace(geocode.Name))
	if !strings.Contains(name, "same") && !strings.Contains(name, "location") && !strings.Contains(name, "fips") && !strings.Contains(name, "clc") {
		return ""
	}
	return cleanLocationCode(geocode.Value)
}
