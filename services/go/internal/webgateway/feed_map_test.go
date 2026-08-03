package webgateway

import (
	"reflect"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
)

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
