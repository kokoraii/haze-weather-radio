package productrender

import (
	"database/sql"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

func TestPolygonFirstCoverageUsesPositiveAreaOverlap(t *testing.T) {
	dir := t.TempDir()
	writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
		Source: "clc", Code: "065400", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 2, 2)},
	})
	feed := polygonFirstCoverageFeed("065400")

	overlap := polygonCoverageTestAlert("999999", capCoverageSquareText(0.1, 0.1, 0.4, 0.4), nil)
	if !alertMatchesFeed(overlap, feed, dir) {
		t.Fatal("polygon overlap should route even when its CLC does not match")
	}
	if got, want := capFeedLocationAssignments(overlap, feed, dir), []string{"765400"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("polygon assignments = %#v, want %#v", got, want)
	}

	nonOverlap := polygonCoverageTestAlert("065400", capCoverageSquareText(3, 3, 4, 4), nil)
	if alertMatchesFeed(nonOverlap, feed, dir) {
		t.Fatal("a valid non-overlapping polygon must not broaden through its matching CLC")
	}
	if got := capFeedLocationAssignments(nonOverlap, feed, dir); len(got) != 0 {
		t.Fatalf("non-overlap assignments = %#v, want none", got)
	}

	tangent := polygonCoverageTestAlert("065400", capCoverageSquareText(2, 0, 4, 2), nil)
	if alertMatchesFeed(tangent, feed, dir) {
		t.Fatal("boundary-only contact must not count as polygon coverage")
	}
}

func TestPolygonFirstCoverageFallsBackToCodesForUnavailableOrInvalidGeometry(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, dir string)
		alert capmodel.Alert
	}{
		{
			name:  "missing sidecar",
			setup: func(t *testing.T, dir string) {},
			alert: polygonCoverageTestAlert("065400", capCoverageSquareText(3, 3, 4, 4), nil),
		},
		{
			name: "self crossing CAP polygon",
			setup: func(t *testing.T, dir string) {
				writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
					Source: "clc", Code: "065400", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 2, 2)},
				})
			},
			alert: polygonCoverageTestAlert("065400", "0,0 2,2 0,2 2,0 0,0", nil),
		},
		{
			name: "invalid coverage hole topology",
			setup: func(t *testing.T, dir string) {
				writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
					Source: "clc", Code: "065400", Rings: [][]capCoveragePoint{
						capCoverageSquare(0, 0, 4, 4),
						capCoverageSquare(5, 5, 6, 6),
					},
				})
			},
			alert: polygonCoverageTestAlert("065400", capCoverageSquareText(10, 10, 11, 11), nil),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			test.setup(t, dir)
			if !alertMatchesFeed(test.alert, polygonFirstCoverageFeed("065400"), dir) {
				t.Fatal("unavailable or invalid geometry should retain code-based coverage routing")
			}
		})
	}
}

func TestPolygonFirstCoverageExpandsRegionalCLCToLeafGeometries(t *testing.T) {
	dir := t.TempDir()
	writeCAPCoverageGeometryFixture(t, dir,
		capCoverageGeometryFixture{Source: "clc", Code: "065401", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 2, 2)}},
		capCoverageGeometryFixture{Source: "clc", Code: "065435", Rings: [][]capCoveragePoint{capCoverageSquare(2, 0, 4, 2)}},
	)
	alert := polygonCoverageTestAlert("999999", capCoverageSquareText(0.5, 0.5, 1.5, 1.5), nil)
	if !alertMatchesFeed(alert, polygonFirstCoverageFeed("065400"), dir) {
		t.Fatal("a regional CLC without a direct shape should route through its leaf CLC geometries")
	}
}

func TestPolygonFirstCoveragePrefersActiveDLCPolygons(t *testing.T) {
	dir := t.TempDir()
	writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
		Source: "clc", Code: "065400", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 2, 2)},
	})
	feed := polygonFirstCoverageFeed("065400")
	normal := capCoverageSquareText(0.25, 0.25, 1.75, 1.75)
	outsideThreat := capmodel.AlertArea{Polygons: []string{capCoverageSquareText(5, 5, 6, 6)}, Geocodes: []capmodel.NameValue{{Name: "layer:EC-MSC-SMC:DLC:1.1", Value: "issued"}}}
	if alertMatchesFeed(polygonCoverageTestAlert("065400", normal, []capmodel.AlertArea{outsideThreat}), feed, dir) {
		t.Fatal("an active DLC polygon must supersede a broader normal CAP area polygon")
	}

	endedThreat := capmodel.AlertArea{Polygons: []string{capCoverageSquareText(5, 5, 6, 6)}, Geocodes: []capmodel.NameValue{{Name: "layer:EC-MSC-SMC:DLC:1.1", Value: "ended"}}}
	if !alertMatchesFeed(polygonCoverageTestAlert("065400", normal, []capmodel.AlertArea{endedThreat}), feed, dir) {
		t.Fatal("an ended DLC polygon must not suppress an eligible normal CAP area polygon")
	}
}

func TestCAPUpdateUsesIssuedDLCGeometryForNewFeedCoverage(t *testing.T) {
	dir := t.TempDir()
	writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
		Source: "clc", Code: "065400", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 3, 3)},
	})
	feed := polygonFirstCoverageFeed("065400")
	issuedInside := capmodel.AlertArea{
		Polygons: []string{capCoverageSquareText(0.2, 0.2, 0.8, 0.8)},
		Geocodes: []capmodel.NameValue{{Name: capmodel.ECCCThreatAreaGeocodeName, Value: "issued"}},
	}
	continuedInside := capmodel.AlertArea{
		Polygons: []string{capCoverageSquareText(1.2, 1.2, 1.8, 1.8)},
		Geocodes: []capmodel.NameValue{{Name: capmodel.ECCCThreatAreaGeocodeName, Value: "continued"}},
	}
	issuedOutside := capmodel.AlertArea{
		Polygons: []string{capCoverageSquareText(5, 5, 6, 6)},
		Geocodes: []capmodel.NameValue{{Name: capmodel.ECCCThreatAreaGeocodeName, Value: "issued"}},
	}

	tests := []struct {
		name  string
		areas []capmodel.AlertArea
		want  bool
	}{
		{name: "issued polygon enters feed", areas: []capmodel.AlertArea{issuedInside, continuedInside}, want: true},
		{name: "continued polygon alone does not retone", areas: []capmodel.AlertArea{continuedInside}, want: false},
		{name: "off-feed issued polygon does not borrow continued coverage", areas: []capmodel.AlertArea{issuedOutside, continuedInside}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := capmodel.AlertInfo{Areas: test.areas}
			alert := capmodel.Alert{Sender: "cap-pac@canada.ca", MessageType: "Update", Infos: []capmodel.AlertInfo{info}}
			if got := capUpdateAddsFeedLocations(alert, info, feed, dir); got != test.want {
				t.Fatalf("capUpdateAddsFeedLocations() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPolygonFirstCoverageLeavesNormalModeUntouched(t *testing.T) {
	dir := t.TempDir()
	writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
		Source: "clc", Code: "065400", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 2, 2)},
	})
	feed := polygonFirstCoverageFeed("065400")
	feed.Alerts.CapCP.Filter.CoverageMode = ""
	alert := polygonCoverageTestAlert("065400", capCoverageSquareText(3, 3, 4, 4), nil)
	if !alertMatchesFeed(alert, feed, dir) {
		t.Fatal("normal coverage mode should continue using the configured CLC")
	}
}

func TestPolygonFirstCoverageModeIsScopedToItsCAPSource(t *testing.T) {
	var feeds feedsXML
	if err := xml.Unmarshal([]byte(`
<feeds>
  <feed id="test">
    <alerts>
      <cap_cp enabled="true"><filter coverage_mode="polygon_first"/></cap_cp>
      <nws_cap enabled="true"><filter/></nws_cap>
    </alerts>
  </feed>
</feeds>`), &feeds); err != nil {
		t.Fatal(err)
	}
	if len(feeds.Feeds) != 1 {
		t.Fatalf("feeds = %#v", feeds.Feeds)
	}
	feed := feeds.Feeds[0]
	eccc := capmodel.Alert{Sender: "cap-pac@canada.ca"}
	nws := capmodel.Alert{Sender: "alerts@weather.gov"}
	if !feedUsesPolygonFirstCoverage(feed, eccc) {
		t.Fatal("cap_cp coverage_mode was not loaded")
	}
	if feedUsesPolygonFirstCoverage(feed, nws) {
		t.Fatal("cap_cp coverage_mode must not affect nws_cap routing")
	}
}

func TestPolygonFirstCoverageEmitsPartialAndWholeCLCGridCodes(t *testing.T) {
	dir := t.TempDir()
	writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
		Source: "clc", Code: "065435", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 3, 3)},
	})
	feed := polygonFirstCoverageFeed("065435")

	partial := polygonCoverageTestAlert("999999", capCoverageSquareText(2.2, 0.2, 2.8, 0.8), nil)
	assignments := capFeedLocationAssignments(partial, feed, dir)
	if want := []string{"165435"}; !reflect.DeepEqual(assignments, want) {
		t.Fatalf("partial assignments = %#v, want %#v", assignments, want)
	}
	if got, want := sameLocationsForAssignments(partial.Infos[0], feed, dir, assignments), []string{"165435"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("partial SAME locations = %#v, want %#v", got, want)
	}

	whole := polygonCoverageTestAlert("999999", capCoverageSquareText(-1, -1, 4, 4), nil)
	if got, want := capFeedLocationAssignments(whole, feed, dir), []string{"065435"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("whole assignments = %#v, want %#v", got, want)
	}
}

func TestPolygonFirstRegionalCoverageUsesLeafGridIdentity(t *testing.T) {
	dir := t.TempDir()
	writeCAPCoverageGeometryFixture(t, dir, capCoverageGeometryFixture{
		Source: "clc", Code: "065401", Rings: [][]capCoveragePoint{capCoverageSquare(0, 0, 2, 2)},
	})
	alert := polygonCoverageTestAlert("999999", capCoverageSquareText(0.1, 0.1, 0.4, 0.4), nil)
	if got, want := capFeedLocationAssignments(alert, polygonFirstCoverageFeed("065400"), dir), []string{"765401"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("regional leaf assignments = %#v, want %#v", got, want)
	}
}

func TestCLCGridScopeKeepsChildrenDistinctAndLetsParentsCancel(t *testing.T) {
	db := alertGeoDB{CLC: map[string]clcBaseZone{"065435": {Code: "065435"}}}
	if !capLocationScopeOverlaps(db, "eccc", "065435", "165435") {
		t.Fatal("whole CLC parent should scope its northwest child")
	}
	if capLocationScopeOverlaps(db, "eccc", "165435", "365435") {
		t.Fatal("different CLC grid children must remain distinct")
	}
	if !capLocationScopeOverlaps(db, "eccc", "165435", "165435") {
		t.Fatal("the same CLC grid child should overlap itself")
	}
	if !capLocationScopeOverlaps(db, "eccc", "065400", "065435") {
		t.Fatal("existing ECCC regional parent scope should keep covering its leaf")
	}
	if got, want := capGridAreaPhrase(db, []string{"165435", "365435"}, "065435", "Saskatoon area"), "northwest and northeast portions of Saskatoon area"; got != want {
		t.Fatalf("grid area phrase = %q, want %q", got, want)
	}
}

func TestCoverageGeometryCandidatesUseOnlyTrustedForecastCLCLinks(t *testing.T) {
	db := alertGeoDB{ForecastToCLC: map[string][]string{"004430": {"032760"}}}
	region := coverageRegionXML{ID: "004430", Source: "eccc"}
	if got, want := capCoverageGeometryCandidates(region, db), []capCoverageGeometryIdentity{
		{Source: "clc", Code: "004430"},
		{Source: "clc", Code: "032760"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("geometry candidates = %#v, want %#v", got, want)
	}

	if !trustedForecastToCLCLink(locationdb.Link{Confidence: "high", Score: 0.92}) {
		t.Fatal("high-confidence link should be trusted")
	}
	if trustedForecastToCLCLink(locationdb.Link{Confidence: "review", Score: 0.99}) {
		t.Fatal("review link must not be trusted")
	}
	if trustedForecastToCLCLink(locationdb.Link{Confidence: "high", Score: 0.89}) {
		t.Fatal("low-score high-confidence link must not be trusted")
	}
}

func TestPolygonFirstCoverageFallsBackToTrustedForecastCLCCode(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Clean(dir)
	alertGeoCache.Lock()
	alertGeoCache.byBase[key] = alertGeoDB{
		CLC:           map[string]clcBaseZone{"032760": {Code: "032760"}},
		ForecastToCLC: map[string][]string{"004430": {"032760"}},
	}
	alertGeoCache.Unlock()
	t.Cleanup(func() {
		alertGeoCache.Lock()
		delete(alertGeoCache.byBase, key)
		alertGeoCache.Unlock()
	})

	feed := polygonFirstCoverageFeed("004430")
	alert := polygonCoverageTestAlert("032760", capCoverageSquareText(50, -100, 51, -99), nil)
	if !alertMatchesFeed(alert, feed, dir) {
		t.Fatal("trusted forecast-to-CLC code should route when the geometry sidecar is missing")
	}
	if got, want := capFeedLocationAssignments(alert, feed, dir), []string{"032760"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback assignments = %#v, want %#v", got, want)
	}

	coverage := feedCoverageModel(dir, feed, nil)
	if _, ok := coverage.Codes["032760"]; !ok {
		t.Fatalf("coverage codes omit trusted linked CLC: %#v", coverage.Codes)
	}
	if len(coverage.Regions) != 1 {
		t.Fatalf("coverage regions = %#v", coverage.Regions)
	}
	if _, ok := coverage.Regions[0].Subregions["032760"]; !ok {
		t.Fatalf("coverage item omits trusted linked CLC: %#v", coverage.Regions[0])
	}
	if _, ok := coverage.Regions[0].RequiredSubregions["032760"]; !ok {
		t.Fatalf("coverage item requirements omit trusted linked CLC: %#v", coverage.Regions[0])
	}
}

func polygonFirstCoverageFeed(regionID string) feedXML {
	var feed feedXML
	feed.Alerts.CapCP.EnabledRaw = "true"
	feed.Alerts.CapCP.Filter.UseFeedLocations = "true"
	feed.Alerts.CapCP.Filter.CoverageMode = capCoverageModePolygonFirst
	feed.Locations.Coverage.Regions = []coverageRegionXML{{ID: regionID, Source: "eccc"}}
	return feed
}

func polygonCoverageTestAlert(code string, normalPolygon string, threatAreas []capmodel.AlertArea) capmodel.Alert {
	areas := append([]capmodel.AlertArea{}, threatAreas...)
	if normalPolygon != "" {
		areas = append([]capmodel.AlertArea{{
			Polygons: []string{normalPolygon},
			Geocodes: []capmodel.NameValue{{Name: "layer:EC-MSC-SMC:1.0:CLC", Value: code}},
		}}, areas...)
	}
	return capmodel.Alert{
		Sender: "cap-pac@canada.ca",
		Infos:  []capmodel.AlertInfo{{Areas: areas}},
	}
}

func capCoverageSquare(minLatitude float64, minLongitude float64, maxLatitude float64, maxLongitude float64) []capCoveragePoint {
	return []capCoveragePoint{
		{Latitude: minLatitude, Longitude: minLongitude},
		{Latitude: minLatitude, Longitude: maxLongitude},
		{Latitude: maxLatitude, Longitude: maxLongitude},
		{Latitude: maxLatitude, Longitude: minLongitude},
		{Latitude: minLatitude, Longitude: minLongitude},
	}
}

func capCoverageSquareText(minLatitude float64, minLongitude float64, maxLatitude float64, maxLongitude float64) string {
	return fmt.Sprintf("%g,%g %g,%g %g,%g %g,%g %g,%g",
		minLatitude, minLongitude,
		minLatitude, maxLongitude,
		maxLatitude, maxLongitude,
		maxLatitude, minLongitude,
		minLatitude, minLongitude,
	)
}

type capCoverageGeometryFixture struct {
	Source string
	Code   string
	Rings  [][]capCoveragePoint
}

func writeCAPCoverageGeometryFixture(t *testing.T, baseDir string, fixtures ...capCoverageGeometryFixture) {
	t.Helper()
	path := filepath.Join(baseDir, filepath.FromSlash(capCoverageGeometryRelPath))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.Exec(`
		CREATE TABLE area_geometries (
			source TEXT NOT NULL,
			code TEXT NOT NULL,
			geometry_type TEXT NOT NULL,
			geometry_wkb BLOB NOT NULL,
			is_current INTEGER NOT NULL
		)
	`); err != nil {
		t.Fatal(err)
	}
	statement, err := database.Prepare(`
		INSERT INTO area_geometries(source, code, geometry_type, geometry_wkb, is_current)
		VALUES (?, ?, 'polygon', ?, 1)
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Close()
	for _, fixture := range fixtures {
		if _, err := statement.Exec(fixture.Source, fixture.Code, capCoveragePolygonWKB(fixture.Rings)); err != nil {
			t.Fatal(err)
		}
	}
}

func capCoveragePolygonWKB(rings [][]capCoveragePoint) []byte {
	data := make([]byte, 0, 9+4+len(rings)*4)
	data = append(data, 1)
	data = capCoverageAppendUint32(data, 3)
	data = capCoverageAppendUint32(data, uint32(len(rings)))
	for _, ring := range rings {
		data = capCoverageAppendUint32(data, uint32(len(ring)))
		for _, point := range ring {
			data = capCoverageAppendFloat64(data, point.Longitude)
			data = capCoverageAppendFloat64(data, point.Latitude)
		}
	}
	return data
}

func capCoverageAppendUint32(data []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(data, encoded[:]...)
}

func capCoverageAppendFloat64(data []byte, value float64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))
	return append(data, encoded[:]...)
}
