package webgateway

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
)

func TestResolveFeedAreaFeaturesRequestsSplitPackGeometry(t *testing.T) {
	address, requests, done := startFeedMapLocationBridge(t, []string{"generation-a"}, [][]string{{"legacy-alert-locations", "legacy-weather-geometry"}}, true)
	t.Setenv("HAZE_HOST_BRIDGE_ADDR", address)
	locations, features, generation, packs, err := (&Server{}).resolveFeedAreaFeatures(
		context.Background(), []feedAreaKey{{Source: "clc", Code: "065100"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	request := <-requests
	data := request["data"].(map[string]any)
	options := data["options"].(map[string]any)
	if options["include_area_geometry"] != true {
		t.Fatalf("map request options = %#v", options)
	}
	if options["limit"] != float64(publicFeedMapCandidateLimit) {
		t.Fatalf("map request result limit = %#v", options["limit"])
	}
	if inputs, ok := data["inputs"].([]any); !ok || len(inputs) != 1 {
		t.Fatalf("map request inputs = %#v", data["inputs"])
	}
	if len(locations) != 1 || len(features) != 1 || generation != "generation-a" || !sameCatalogPacks(packs, []string{"legacy-alert-locations", "legacy-weather-geometry"}) {
		t.Fatalf("resolved map = locations %d, features %d, generation %q, packs %#v", len(locations), len(features), generation, packs)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestResolveFeedAreaFeaturesRejectsMixedCatalogIdentity(t *testing.T) {
	tests := []struct {
		name        string
		generations []string
		packs       [][]string
	}{
		{name: "generation", generations: []string{"generation-a", "generation-b"}, packs: [][]string{{"core", "geometry"}, {"core", "geometry"}}},
		{name: "packs", generations: []string{"generation-a", "generation-a"}, packs: [][]string{{"core", "geometry-a"}, {"core", "geometry-b"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			address, _, done := startFeedMapLocationBridge(t, test.generations, test.packs, false)
			t.Setenv("HAZE_HOST_BRIDGE_ADDR", address)
			keys := make([]feedAreaKey, publicFeedMapBatchSize+1)
			for index := range keys {
				keys[index] = feedAreaKey{Source: "clc", Code: fmt.Sprintf("%06d", index+1)}
			}
			_, _, _, _, err := (&Server{}).resolveFeedAreaFeatures(context.Background(), keys)
			if err == nil || !strings.Contains(err.Error(), "changed") {
				t.Fatalf("mixed catalog identity error = %v", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResolveFeedAreaFeaturesUsesBoundedConcurrencyAndDeterministicOrder(t *testing.T) {
	requestCount := publicFeedMapResolveConcurrency*2 + 3
	address, started, completed, release, serverDone := startConcurrentFeedMapLocationBridge(t, requestCount)
	t.Setenv("HAZE_HOST_BRIDGE_ADDR", address)
	keys := make([]feedAreaKey, requestCount)
	for index := range keys {
		keys[index] = feedAreaKey{Source: "clc", Code: fmt.Sprintf("%06d", index+1)}
	}
	type resolveResult struct {
		locations  []publicMapLocation
		features   []map[string]any
		generation string
		packs      []string
		err        error
	}
	resolved := make(chan resolveResult, 1)
	server := &Server{feedMapResolveSlots: make(chan struct{}, publicFeedMapResolveConcurrency)}
	go func() {
		locations, features, generation, packs, err := server.resolveFeedAreaFeatures(context.Background(), keys)
		resolved <- resolveResult{locations: locations, features: features, generation: generation, packs: packs, err: err}
	}()
	for index := 0; index < publicFeedMapResolveConcurrency; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d location requests started concurrently", index, publicFeedMapResolveConcurrency)
		}
	}
	select {
	case <-started:
		t.Fatalf("more than %d location requests ran concurrently", publicFeedMapResolveConcurrency)
	case <-time.After(50 * time.Millisecond):
	}
	release()

	var result resolveResult
	select {
	case result = <-resolved:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent feed map resolution did not finish")
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	completionOrder := make([]string, 0, requestCount)
	for code := range completed {
		completionOrder = append(completionOrder, code)
	}
	if codesMatchKeyOrder(completionOrder, keys) {
		t.Fatalf("test bridge unexpectedly completed requests in input order: %#v", completionOrder)
	}
	if len(result.locations) != requestCount || len(result.features) != requestCount {
		t.Fatalf("resolved locations = %d, features = %d, want %d", len(result.locations), len(result.features), requestCount)
	}
	if result.generation != "generation-a" || !sameCatalogPacks(result.packs, []string{"core", "geometry"}) {
		t.Fatalf("catalog identity = %q %#v", result.generation, result.packs)
	}
	for index, feature := range result.features {
		want := "clc:" + keys[index].Code
		if feature["id"] != want {
			t.Fatalf("feature %d id = %v, want %q", index, feature["id"], want)
		}
	}
}

func TestResolveFeedAreaFeaturesHonorsCancellation(t *testing.T) {
	address, started := startBlockingFeedMapLocationBridge(t)
	t.Setenv("HAZE_HOST_BRIDGE_ADDR", address)
	keys := make([]feedAreaKey, publicFeedMapResolveConcurrency*2)
	for index := range keys {
		keys[index] = feedAreaKey{Source: "clc", Code: fmt.Sprintf("%06d", index+1)}
	}
	ctx, cancel := context.WithCancel(context.Background())
	resolved := make(chan error, 1)
	server := &Server{feedMapResolveSlots: make(chan struct{}, publicFeedMapResolveConcurrency)}
	go func() {
		_, _, _, _, err := server.resolveFeedAreaFeatures(ctx, keys)
		resolved <- err
	}()
	for index := 0; index < publicFeedMapResolveConcurrency; index++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d location requests started before cancellation", index, publicFeedMapResolveConcurrency)
		}
	}
	cancel()
	select {
	case err := <-resolved:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled feed map resolution error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled feed map resolution did not return promptly")
	}
	select {
	case <-started:
		t.Fatal("feed map resolution started more work after cancellation")
	case <-time.After(50 * time.Millisecond):
	}
}

func codesMatchKeyOrder(completed []string, keys []feedAreaKey) bool {
	if len(completed) != len(keys) {
		return false
	}
	for index := range completed {
		if completed[index] != keys[index].Code {
			return false
		}
	}
	return true
}

func startFeedMapLocationBridge(t *testing.T, generations []string, packs [][]string, includeGeometry bool) (string, <-chan map[string]any, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	requests := make(chan map[string]any, len(generations))
	done := make(chan error, 1)
	go func() {
		defer close(requests)
		for index, generation := range generations {
			connection, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			scanner := bufio.NewScanner(connection)
			if !scanner.Scan() || !scanner.Scan() {
				scanErr := scanner.Err()
				if scanErr == nil {
					scanErr = fmt.Errorf("location bridge request ended early")
				}
				_ = connection.Close()
				done <- scanErr
				return
			}
			var request map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
				_ = connection.Close()
				done <- err
				return
			}
			requests <- request
			packValues := []string{}
			if index < len(packs) {
				packValues = packs[index]
			}
			response, _, err := feedMapLocationFixtureResponse(request, generation, packValues, includeGeometry)
			if err != nil {
				_ = connection.Close()
				done <- err
				return
			}
			encodeErr := json.NewEncoder(connection).Encode(response)
			closeErr := connection.Close()
			if encodeErr != nil {
				done <- encodeErr
				return
			}
			if closeErr != nil {
				done <- closeErr
				return
			}
		}
		done <- nil
	}()
	return listener.Addr().String(), requests, done
}

func startConcurrentFeedMapLocationBridge(t *testing.T, requestCount int) (string, <-chan struct{}, <-chan string, func(), <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, requestCount)
	completed := make(chan string, requestCount)
	releaseRequests := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRequests) }) }
	t.Cleanup(func() {
		release()
		_ = listener.Close()
	})
	done := make(chan error, 1)
	go func() {
		var handlers sync.WaitGroup
		handlerErrors := make(chan error, requestCount)
		for index := 0; index < requestCount; index++ {
			connection, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			handlers.Add(1)
			go func(connection net.Conn) {
				defer handlers.Done()
				defer connection.Close()
				scanner := bufio.NewScanner(connection)
				if !scanner.Scan() || !scanner.Scan() {
					scanErr := scanner.Err()
					if scanErr == nil {
						scanErr = fmt.Errorf("location bridge request ended early")
					}
					handlerErrors <- scanErr
					return
				}
				var request map[string]any
				if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
					handlerErrors <- err
					return
				}
				response, code, err := feedMapLocationFixtureResponse(request, "generation-a", []string{"core", "geometry"}, true)
				if err != nil {
					handlerErrors <- err
					return
				}
				started <- struct{}{}
				<-releaseRequests
				codeIndex, _ := strconv.Atoi(code)
				if delay := requestCount - codeIndex; delay > 0 {
					time.Sleep(time.Duration(delay) * 2 * time.Millisecond)
				}
				if err := json.NewEncoder(connection).Encode(response); err != nil {
					handlerErrors <- err
					return
				}
				completed <- code
				handlerErrors <- nil
			}(connection)
		}
		handlers.Wait()
		close(handlerErrors)
		close(completed)
		for err := range handlerErrors {
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	return listener.Addr().String(), started, completed, release, done
}

func startBlockingFeedMapLocationBridge(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, publicFeedMapResolveConcurrency+1)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func(connection net.Conn) {
				defer connection.Close()
				scanner := bufio.NewScanner(connection)
				if !scanner.Scan() || !scanner.Scan() {
					return
				}
				started <- struct{}{}
				_ = scanner.Scan()
			}(connection)
		}
	}()
	return listener.Addr().String(), started
}

func feedMapLocationFixtureResponse(request map[string]any, generation string, packs []string, includeGeometry bool) (map[string]any, string, error) {
	data, ok := request["data"].(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("location bridge request data is missing")
	}
	inputs, ok := data["inputs"].([]any)
	if !ok || len(inputs) == 0 {
		return nil, "", fmt.Errorf("location bridge request inputs are missing")
	}
	resultLimit := 0
	if options, ok := data["options"].(map[string]any); ok {
		if value, ok := options["limit"].(float64); ok {
			resultLimit = int(value)
		}
	}
	firstCode := ""
	batches := make([]map[string]any, 0, len(inputs))
	for inputIndex, rawInput := range inputs {
		input, _ := rawInput.(map[string]any)
		code := strings.TrimSpace(fmt.Sprint(input["value"]))
		if code == "" {
			code = "065100"
		}
		if firstCode == "" {
			firstCode = code
		}
		results := []map[string]any{}
		if includeGeometry {
			name := "Location " + code
			if code == "065100" {
				name = "City of Saskatoon"
			}
			results = append(results, map[string]any{
				"entity": map[string]any{
					"id": "urn:haze:location:preferred:" + code, "kind": "forecast_zone",
					"country": "CA", "lifecycle_status": "active", "reporting_status": "not_applicable", "source_quality": 1,
					"names":      []map[string]any{{"value": "Preferred " + name, "normalized_value": "preferred " + strings.ToLower(name), "name_kind": "canonical", "primary": true}},
					"attributes": map[string]any{"same_code": code, "provider_version": "core-without-geometry"},
				},
				"match": map[string]any{"score": 1, "confidence": "exact", "method": "exact_identifier", "algorithm": "fixture"},
			}, map[string]any{
				"entity": map[string]any{
					"id": "urn:haze:location:fixture:" + code, "kind": "forecast_zone",
					"country": "CA", "lifecycle_status": "active", "reporting_status": "not_applicable", "source_quality": 1,
					"names": []map[string]any{{"value": name, "normalized_value": strings.ToLower(name), "name_kind": "canonical", "primary": true}},
					"attributes": map[string]any{
						"same_code": code, "provider_version": "6.15.0",
						"area_geometry": map[string]any{"type": "Polygon", "coordinates": []any{[]any{[]float64{-107, 52}, []float64{-106, 52}, []float64{-106, 53}, []float64{-107, 52}}}},
					},
				},
				"match": map[string]any{"score": 1, "confidence": "exact", "method": "exact_identifier", "algorithm": "fixture"},
			})
			if resultLimit > 0 && len(results) > resultLimit {
				results = results[:resultLimit]
			}
		}
		batches = append(batches, map[string]any{"input_index": inputIndex, "status": "resolved", "results": results})
	}
	return map[string]any{
		"type": "location.query.completed", "subject": data["request_id"],
		"data": map[string]any{
			"api_version": 1, "request_id": data["request_id"], "operation": "batch_resolve",
			"status": "resolved", "ambiguous": false, "catalog_generation": generation,
			"catalog_packs": packs, "truncated": false, "batches": batches,
		},
	}, firstCode, nil
}

func TestFeedAreaKeysPreserveQualifiedCoverageSources(t *testing.T) {
	feed := feedXML{}
	feed.Alerts.CapCP.EnabledRaw = "false"
	feed.Locations.Coverage.Regions = []coverageRegionXML{
		{ID: "065100", Source: "eccc"},
		{ID: "019001", Source: "nws"},
	}
	keys := feedAreaKeys(feed, map[string]string{"065100": "Saskatoon"}, map[string]string{"019001": "Fairfield, CT"})
	if len(keys) != 2 {
		t.Fatalf("keys = %#v", keys)
	}
	if keys[0] != (feedAreaKey{Source: "clc", Code: "065100"}) || keys[1] != (feedAreaKey{Source: "nws_same", Code: "019001"}) {
		t.Fatalf("keys = %#v", keys)
	}
}

func TestCAPPolygonConvertsLatitudeLongitudeOrder(t *testing.T) {
	geometry, ok := capPolygonGeometry("52,-107 52,-106 53,-106 52,-107")
	if !ok {
		t.Fatal("CAP polygon was rejected")
	}
	coordinates := geometry["coordinates"].([]any)[0].([][]float64)
	if coordinates[0][0] != -107 || coordinates[0][1] != 52 {
		t.Fatalf("coordinate = %#v", coordinates[0])
	}
}

func TestCAPCircleRejectsUnboundedRadius(t *testing.T) {
	if _, ok := capCircleGeometry("52,-106 9000"); ok {
		t.Fatal("unbounded CAP circle radius was accepted")
	}
}

func TestPublicMapLocationCodesUseCanadianSGCCodes(t *testing.T) {
	entity := locationclient.Entity{
		Country: "CA",
		Attributes: map[string]any{
			"same_code": "065100",
			"sgc_codes": []any{"4711066"},
		},
	}
	same, sameCodes, sgc, fips, counties, zones := publicMapLocationCodes(entity, "065100")
	if same != "065100" || !reflect.DeepEqual(sameCodes, []string{"065100"}) || !reflect.DeepEqual(sgc, []string{"4711066"}) {
		t.Fatalf("Canada codes = same %q, all %#v, SGC %#v", same, sameCodes, sgc)
	}
	if fips != nil || counties != nil || zones != nil {
		t.Fatalf("Canada exposed US codes: fips %#v, counties %#v, zones %#v", fips, counties, zones)
	}
}

func TestPublicMapLocationCodesUseUSFIPSAndNWSCodes(t *testing.T) {
	entity := locationclient.Entity{
		Country: "US",
		Attributes: map[string]any{
			"same_codes":       []any{"020001"},
			"fips_codes":       []any{"20001"},
			"nws_county_codes": []any{"KSC001"},
			"nws_zone_codes":   []any{"KSZ072"},
		},
	}
	same, sameCodes, sgc, fips, counties, zones := publicMapLocationCodes(entity, "020001")
	if same != "020001" || !reflect.DeepEqual(sameCodes, []string{"020001"}) {
		t.Fatalf("SAME codes = %q %#v", same, sameCodes)
	}
	if sgc != nil || !reflect.DeepEqual(fips, []string{"20001"}) || !reflect.DeepEqual(counties, []string{"KSC001"}) || !reflect.DeepEqual(zones, []string{"KSZ072"}) {
		t.Fatalf("US codes = SGC %#v, FIPS %#v, counties %#v, zones %#v", sgc, fips, counties, zones)
	}
}

func TestPublicMapLocationCodesKeepOtherCountriesToSAMEOnly(t *testing.T) {
	entity := locationclient.Entity{
		Country: "MX",
		Attributes: map[string]any{
			"same_code":        "123456",
			"sgc_codes":        []any{"not-shown"},
			"fips_codes":       []any{"not-shown"},
			"nws_zone_codes":   []any{"not-shown"},
			"nws_county_codes": []any{"not-shown"},
		},
	}
	same, _, sgc, fips, counties, zones := publicMapLocationCodes(entity, "123456")
	if same != "123456" || sgc != nil || fips != nil || counties != nil || zones != nil {
		t.Fatalf("other-country codes = same %q, SGC %#v, FIPS %#v, counties %#v, zones %#v", same, sgc, fips, counties, zones)
	}
}

func TestParsePublicMapBoundsRequiresFiniteOrderedViewport(t *testing.T) {
	bounds, ok := parsePublicMapBounds("-110,49,-100,55")
	if !ok || bounds.West != -110 || bounds.North != 55 {
		t.Fatalf("bounds = %#v, ok = %v", bounds, ok)
	}
	for _, raw := range []string{"", "-110,49,-100", "-181,49,-100,55", "-110,55,-100,49", "NaN,49,-100,55"} {
		if _, ok := parsePublicMapBounds(raw); ok {
			t.Fatalf("invalid bounds %q were accepted", raw)
		}
	}
}

func TestGeoJSONGeometryIntersectsViewport(t *testing.T) {
	geometry, ok := capPolygonGeometry("52,-107 52,-106 53,-106 52,-107")
	if !ok {
		t.Fatal("test polygon was rejected")
	}
	if !geoJSONGeometryIntersectsBounds(geometry, publicMapBounds{West: -106.5, South: 51, East: -105, North: 54}) {
		t.Fatal("visible polygon was excluded")
	}
	if geoJSONGeometryIntersectsBounds(geometry, publicMapBounds{West: -90, South: 40, East: -80, North: 45}) {
		t.Fatal("offscreen polygon was included")
	}
}

func TestActiveAlertFeaturesIncludeEveryFeedButOnlyCurrentViewport(t *testing.T) {
	now := time.Date(2026, 8, 2, 18, 0, 0, 0, time.UTC)
	record := func(id, feedID, polygon, onset string) archiveCAPRecord {
		return archiveCAPRecord{
			ID: id, FeedID: feedID,
			Alert: capmodel.Alert{Identifier: id, Status: "Actual", MessageType: "Alert", Infos: []capmodel.AlertInfo{{
				Event: "Severe Thunderstorm Warning", Severity: "Severe", Onset: onset, Expires: "2026-08-02T20:00:00Z",
				Areas: []capmodel.AlertArea{{Description: id, Polygons: []string{polygon}}},
			}}},
		}
	}
	records := []archiveCAPRecord{
		record("feed-a-visible", "feed-a", "52,-107 52,-106 53,-106 52,-107", "2026-08-02T17:00:00Z"),
		record("feed-b-visible", "feed-b", "51,-105 51,-104 52,-104 51,-105", "2026-08-02T17:00:00Z"),
		record("future", "feed-c", "52,-107 52,-106 53,-106 52,-107", "2026-08-02T19:00:00Z"),
		record("offscreen", "feed-d", "40,-90 40,-89 41,-89 40,-90", "2026-08-02T17:00:00Z"),
	}
	collection, truncated := activeAlertFeaturesInBounds(records, nil, publicMapBounds{West: -110, South: 49, East: -100, North: 55}, nil, nil, now)
	features := collection["features"].([]map[string]any)
	if truncated || len(features) != 2 {
		t.Fatalf("features = %d, truncated = %v, want two active areas from separate feeds", len(features), truncated)
	}
}

func TestActiveAlertFeaturesPreferCurrentECCCThreatGeometry(t *testing.T) {
	now := time.Date(2026, 8, 11, 15, 45, 0, 0, time.UTC)
	speed := 40.0
	direction := 90.12841
	record := archiveCAPRecord{
		ID: "eccc-threat", FeedID: "cwxr-sk01",
		Alert: capmodel.Alert{Identifier: "eccc-threat", Status: "Actual", MessageType: "Update", Infos: []capmodel.AlertInfo{{
			Event: "Severe Thunderstorm Warning", Headline: "Severe thunderstorm warning - updated", Severity: "Severe",
			Onset: "2026-08-11T15:30:00Z", Expires: "2026-08-11T16:15:00Z",
			Storm: &capmodel.StormInfo{
				Speed: "40 km/h", SpeedValue: &speed, DirectionDegrees: &direction, GeometryType: "isolated_cell",
				Points: []capmodel.GeoPoint{{Latitude: 52.1433, Longitude: -106.6732}}, MotionDescription: "east at 40 km/h",
			},
			Areas: []capmodel.AlertArea{
				{Description: "City of Saskatoon", Polygons: []string{"52.0,-106.9 52.0,-106.4 52.4,-106.4 52.0,-106.9"}},
				{Description: "new active threat area", ThreatStatus: "issued", Polygons: []string{"52.10,-106.75 52.11,-106.62 52.20,-106.60 52.10,-106.75"}},
				{Description: "continued active threat area", ThreatStatus: "continued", Polygons: []string{"52.13,-106.62 52.14,-106.50 52.22,-106.48 52.13,-106.62"}},
				{Description: "ended threat area", ThreatStatus: "ended", Polygons: []string{"52.08,-106.90 52.09,-106.78 52.16,-106.76 52.08,-106.90"}},
				{Description: "cancelled threat area", ThreatStatus: "cancelled", Polygons: []string{"52.00,-106.90 52.01,-106.78 52.07,-106.76 52.00,-106.90"}},
			},
		}}},
	}
	collection, truncated := activeAlertFeaturesInBounds(
		[]archiveCAPRecord{record}, nil,
		publicMapBounds{West: -110, South: 49, East: -100, North: 55}, nil, nil, now,
	)
	features := collection["features"].([]map[string]any)
	if truncated || len(features) != 2 {
		t.Fatalf("features = %#v, truncated = %v", features, truncated)
	}
	statuses := map[string]bool{}
	for _, feature := range features {
		properties := feature["properties"].(map[string]any)
		statuses[fmt.Sprint(properties["threat_status"])] = true
		if properties["geometry_method"] != "cap_threat_polygon" || properties["storm_speed"] != "40 km/h" || properties["motion_description"] != "east at 40 km/h" {
			t.Fatalf("properties = %#v", properties)
		}
	}
	if !statuses["issued"] || !statuses["continued"] || statuses["ended"] || statuses["cancelled"] {
		t.Fatalf("statuses = %#v", statuses)
	}
}
