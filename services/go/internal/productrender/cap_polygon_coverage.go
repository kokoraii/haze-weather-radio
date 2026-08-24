package productrender

import (
	"context"
	"database/sql"
	"encoding/binary"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertgeo"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	_ "modernc.org/sqlite"
)

const (
	capCoverageModePolygonFirst = "polygon_first"

	// CAP coverage geometry is intentionally separate from the compact text
	// catalog. Product rendering only reads a configured coverage shape on
	// demand, so a large geometry sidecar cannot inflate normal alert routing.
	capCoverageGeometryRelPath = "managed/locations/legacy-weather-geometry.sqlite"

	capCoverageGeometryQueryTimeout      = 250 * time.Millisecond
	capCoverageGeometryMaxWKBBytes       = 128 << 10
	capCoverageGeometryMaxPolygons       = 512
	capCoverageGeometryMaxRings          = 4096
	capCoverageGeometryMaxPoints         = 16_384
	capCoverageGeometryMaxFeedPoints     = 32_768
	capCoverageGeometryMaxAlertPolygons  = 64
	capCoverageGeometryMaxAlertPoints    = 16_384
	capCoverageGeometryMaxPolygonRawSize = 256 << 10
	capCoverageGeometryMaxEdgeChecks     = 1_000_000
	capCoverageGeometryMaxTopologyChecks = 1_000_000
)

type capCoveragePoint struct {
	Latitude  float64
	Longitude float64
}

type capCoverageBounds struct {
	MinLatitude  float64
	MaxLatitude  float64
	MinLongitude float64
	MaxLongitude float64
	Valid        bool
}

type capCoveragePolygon struct {
	Outer  []capCoveragePoint
	Holes  [][]capCoveragePoint
	Bounds capCoverageBounds
}

type capCoverageShape struct {
	Polygons   []capCoveragePolygon
	Bounds     capCoverageBounds
	PointCount int
}

type capCoverageGeometryIdentity struct {
	Source string
	Code   string
}

type capCoverageTopologyBudget struct {
	Remaining int
}

func (budget *capCoverageTopologyBudget) take() bool {
	if budget == nil || budget.Remaining <= 0 {
		return false
	}
	budget.Remaining--
	return true
}

type capCoverageGeometryState uint8

const (
	capCoverageGeometryUnavailable capCoverageGeometryState = iota
	capCoverageGeometryValid
	capCoverageGeometryInvalid
)

type capCoverageGeometryCacheValue struct {
	Shape capCoverageShape
	State capCoverageGeometryState
}

type capCoverageGeometryLeafCacheValue struct {
	Identities []capCoverageGeometryIdentity
	State      capCoverageGeometryState
}

type capCoverageGeometryCatalog struct {
	Modified time.Time
	Size     int64
	Values   map[capCoverageGeometryIdentity]capCoverageGeometryCacheValue
	Leaves   map[capCoverageGeometryIdentity]capCoverageGeometryLeafCacheValue
}

var capCoverageGeometryCache = struct {
	sync.Mutex
	ByPath map[string]capCoverageGeometryCatalog
}{ByPath: map[string]capCoverageGeometryCatalog{}}

func feedUsesPolygonFirstCoverage(feed feedXML, alert capmodel.Alert) bool {
	_, sourceConfig := feedCAPSourceConfig(feed, alert)
	return strings.EqualFold(strings.TrimSpace(sourceConfig.Filter.CoverageMode), capCoverageModePolygonFirst)
}

// capAlertCoveragePolygons selects the exact polygon set that is authoritative
// for an opt-in CAP route. Active DLC geometry supersedes broad ECCC area
// geometry, while ordinary CAP polygons remain eligible when DLC is absent.
func capAlertCoveragePolygons(alert capmodel.Alert) []string {
	activeThreatPolygons := []string{}
	normalPolygons := []string{}
	for _, info := range capActiveOrAllInfos(alert.Infos) {
		for _, area := range info.Areas {
			switch {
			case capDLCThreatAreaActive(area):
				activeThreatPolygons = append(activeThreatPolygons, area.Polygons...)
			case capDLCThreatAreaStatus(area) != "":
				// Ended and cancelled DLC polygons are historical, not coverage.
				continue
			default:
				normalPolygons = append(normalPolygons, area.Polygons...)
			}
		}
	}
	if len(activeThreatPolygons) > 0 {
		return activeThreatPolygons
	}
	return normalPolygons
}

// capDLCThreatAreaStatus is deliberately local to product rendering. CWRS
// still has a compatible CAP model snapshot from before the optional
// ThreatStatus field was added, but the DLC discriminator itself is stable
// CAP input and can be recognized from its source geocode on every supported
// build.
func capDLCThreatAreaStatus(area capmodel.AlertArea) string {
	for _, geocode := range area.Geocodes {
		if strings.EqualFold(strings.TrimSpace(geocode.Name), "layer:EC-MSC-SMC:DLC:1.1") {
			return strings.ToLower(strings.TrimSpace(geocode.Value))
		}
	}
	return ""
}

func capDLCThreatAreaActive(area capmodel.AlertArea) bool {
	switch capDLCThreatAreaStatus(area) {
	case "issued", "continued":
		return true
	default:
		return false
	}
}

func capAlertCoverageShape(alert capmodel.Alert) (capCoverageShape, bool) {
	return capCoverageShapeFromCAPPolygons(capAlertCoveragePolygons(alert))
}

func capCoverageShapeFromCAPPolygons(rawPolygons []string) (capCoverageShape, bool) {
	if len(rawPolygons) == 0 || len(rawPolygons) > capCoverageGeometryMaxAlertPolygons {
		return capCoverageShape{}, false
	}
	shape := capCoverageShape{}
	seen := map[string]struct{}{}
	topologyBudget := capCoverageTopologyBudget{Remaining: capCoverageGeometryMaxTopologyChecks}
	for _, raw := range rawPolygons {
		raw = strings.TrimSpace(raw)
		if raw == "" || len(raw) > capCoverageGeometryMaxPolygonRawSize {
			return capCoverageShape{}, false
		}
		if _, exists := seen[raw]; exists {
			continue
		}
		seen[raw] = struct{}{}
		polygon, ok := parseCAPCoveragePolygon(raw, &topologyBudget)
		if !ok || shape.PointCount+len(polygon.Outer) > capCoverageGeometryMaxAlertPoints {
			return capCoverageShape{}, false
		}
		shape.Polygons = append(shape.Polygons, polygon)
		shape.PointCount += len(polygon.Outer)
		shape.Bounds = capCoverageBoundsUnion(shape.Bounds, polygon.Bounds)
	}
	if len(shape.Polygons) == 0 || !shape.Bounds.Valid {
		return capCoverageShape{}, false
	}
	return shape, true
}

func parseCAPCoveragePolygon(raw string, topologyBudget *capCoverageTopologyBudget) (capCoveragePolygon, bool) {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) < 4 || len(fields) > capCoverageGeometryMaxAlertPoints {
		return capCoveragePolygon{}, false
	}
	ring := make([]capCoveragePoint, 0, len(fields))
	for _, field := range fields {
		parts := strings.Split(field, ",")
		if len(parts) != 2 {
			return capCoveragePolygon{}, false
		}
		latitude, latitudeErr := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		longitude, longitudeErr := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if latitudeErr != nil || longitudeErr != nil || !capCoveragePointValid(latitude, longitude) {
			return capCoveragePolygon{}, false
		}
		if len(ring) > 0 && math.Abs(ring[len(ring)-1].Longitude-longitude) > 180 {
			return capCoveragePolygon{}, false
		}
		ring = append(ring, capCoveragePoint{Latitude: latitude, Longitude: longitude})
	}
	if ring[0] != ring[len(ring)-1] || math.Abs(capCoverageRingArea(ring)) <= 1e-12 {
		return capCoveragePolygon{}, false
	}
	bounds := capCoverageBoundsForRing(ring)
	if !bounds.Valid || bounds.MaxLongitude-bounds.MinLongitude > 180 {
		return capCoveragePolygon{}, false
	}
	polygon := capCoveragePolygon{Outer: ring, Bounds: bounds}
	if !capCoveragePolygonTopologyValid(polygon, topologyBudget) {
		return capCoveragePolygon{}, false
	}
	return polygon, true
}

func capFeedCoverageShape(baseDir string, feed feedXML, db alertGeoDB) (capCoverageShape, bool) {
	if len(feed.Locations.Coverage.Regions) == 0 {
		return capCoverageShape{}, false
	}
	shape := capCoverageShape{}
	loaded := map[capCoverageGeometryIdentity]capCoverageShape{}
	for _, region := range feed.Locations.Coverage.Regions {
		candidates := capCoverageGeometryCandidates(region, db)
		if len(candidates) == 0 {
			return capCoverageShape{}, false
		}
		var regionShape capCoverageShape
		found := false
		for _, candidate := range candidates {
			if candidateShape, cached := loaded[candidate]; cached {
				regionShape = candidateShape
				found = true
				break
			}
			candidateShape, state := loadCAPCoverageGeometry(baseDir, candidate)
			switch state {
			case capCoverageGeometryValid:
				loaded[candidate] = candidateShape
				regionShape = candidateShape
				found = true
			case capCoverageGeometryInvalid:
				return capCoverageShape{}, false
			case capCoverageGeometryUnavailable:
				if !capCoverageGeometryCanExpandLeaves(candidate) {
					continue
				}
				leafIdentity := capCoverageGeometryIdentity{Source: candidate.Source, Code: candidate.Code + "/*"}
				if leafShape, cached := loaded[leafIdentity]; cached {
					regionShape = leafShape
					found = true
					break
				}
				leafShape, leafState := loadCAPCoverageGeometry(baseDir, leafIdentity)
				if leafState == capCoverageGeometryInvalid {
					return capCoverageShape{}, false
				}
				if leafState != capCoverageGeometryValid {
					continue
				}
				loaded[leafIdentity] = leafShape
				loaded[candidate] = leafShape
				regionShape = leafShape
				found = true
			}
			if found {
				break
			}
		}
		if !found || len(regionShape.Polygons) == 0 {
			return capCoverageShape{}, false
		}
		if shape.PointCount+regionShape.PointCount > capCoverageGeometryMaxFeedPoints {
			return capCoverageShape{}, false
		}
		shape.Polygons = append(shape.Polygons, regionShape.Polygons...)
		shape.PointCount += regionShape.PointCount
		shape.Bounds = capCoverageBoundsUnion(shape.Bounds, regionShape.Bounds)
	}
	if len(shape.Polygons) == 0 || !shape.Bounds.Valid {
		return capCoverageShape{}, false
	}
	return shape, true
}

type capCoverageFeature struct {
	Identity capCoverageGeometryIdentity
	Shape    capCoverageShape
}

// capPolygonFirstLocationAssignments returns synthesized, source-qualified CLC
// SAME scopes for an opt-in polygon-first route. A true usable value with no
// assignments is a valid negative overlap. A false usable value leaves the
// caller on the existing code-routing path.
func capPolygonFirstLocationAssignments(alert capmodel.Alert, feed feedXML, baseDir string, db alertGeoDB) (assignments []string, usable bool) {
	if !feedUsesPolygonFirstCoverage(feed, alert) {
		return nil, false
	}
	rawPolygons := capAlertCoveragePolygons(alert)
	capPolygons, ok := capGridAlertPolygons(rawPolygons)
	if !ok {
		return nil, false
	}
	features, ok := capFeedCoverageFeatures(baseDir, feed, db)
	if !ok {
		return nil, false
	}
	for _, feature := range features {
		grid, err := capCoverageGrid(feature.Identity, feature.Shape)
		if err != nil {
			return nil, false
		}
		parts, err := grid.MatchPolygons(capPolygons)
		if err != nil {
			return nil, false
		}
		for _, part := range parts {
			if code := sameLocationCode(part.Code); code != "" {
				assignments = append(assignments, code)
			}
		}
	}
	return capCollapseGridAssignments(assignments), true
}

func capGridAlertPolygons(rawPolygons []string) ([]alertgeo.Polygon, bool) {
	if len(rawPolygons) == 0 || len(rawPolygons) > 16 {
		return nil, false
	}
	polygons := make([]alertgeo.Polygon, 0, len(rawPolygons))
	seen := map[string]struct{}{}
	for _, raw := range rawPolygons {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, false
		}
		if _, exists := seen[raw]; exists {
			continue
		}
		seen[raw] = struct{}{}
		polygon, err := alertgeo.ParseCAPPolygon(raw)
		if err != nil {
			return nil, false
		}
		polygons = append(polygons, polygon)
	}
	return polygons, len(polygons) > 0
}

func capCoverageGrid(identity capCoverageGeometryIdentity, shape capCoverageShape) (alertgeo.Grid, error) {
	if identity.Source != "clc" || len(identity.Code) != 6 || identity.Code[0] != '0' {
		return alertgeo.Grid{}, alertgeo.ErrInvalidBaseCode
	}
	geometry := alertgeo.Geometry{Polygons: make([]alertgeo.Polygon, 0, len(shape.Polygons))}
	for _, polygon := range shape.Polygons {
		converted := alertgeo.Polygon{
			Exterior: capCoverageAlertGeoRing(polygon.Outer),
			Holes:    make([]alertgeo.Ring, 0, len(polygon.Holes)),
		}
		for _, hole := range polygon.Holes {
			converted.Holes = append(converted.Holes, capCoverageAlertGeoRing(hole))
		}
		geometry.Polygons = append(geometry.Polygons, converted)
	}
	return alertgeo.NewGrid(identity.Code, geometry)
}

func capCoverageAlertGeoRing(points []capCoveragePoint) alertgeo.Ring {
	ring := make(alertgeo.Ring, 0, len(points))
	for _, point := range points {
		ring = append(ring, alertgeo.Point{Latitude: point.Latitude, Longitude: point.Longitude})
	}
	return ring
}

// capCollapseGridAssignments preserves selected cells, but represents a full
// nine-cell feature by its P=0 parent. It never turns a partial group into a
// whole parent merely to shorten a header.
func capCollapseGridAssignments(values []string) []string {
	set := map[string]struct{}{}
	children := map[string]map[byte]struct{}{}
	for _, value := range values {
		code := sameLocationCode(value)
		if code == "" {
			continue
		}
		set[code] = struct{}{}
		if code[0] >= '1' && code[0] <= '9' {
			parent := "0" + code[1:]
			if children[parent] == nil {
				children[parent] = map[byte]struct{}{}
			}
			children[parent][code[0]] = struct{}{}
		}
	}
	for parent, parts := range children {
		if len(parts) != 9 {
			continue
		}
		for part := byte('1'); part <= '9'; part++ {
			delete(set, string(part)+parent[1:])
		}
		set[parent] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for code := range set {
		result = append(result, code)
	}
	sort.Strings(result)
	return result
}

// capFeedCoverageFeatures resolves each configured feed scope to the exact
// CLC geometry feature that owns it. A regional CLC with no parent geometry is
// expanded to its source-qualified leaf CLC features. Any unresolved or bad
// region makes the complete polygon attempt unavailable, not negatively
// matched, so legacy code routing remains the safe fallback.
func capFeedCoverageFeatures(baseDir string, feed feedXML, db alertGeoDB) ([]capCoverageFeature, bool) {
	if len(feed.Locations.Coverage.Regions) == 0 {
		return nil, false
	}
	features := make([]capCoverageFeature, 0, len(feed.Locations.Coverage.Regions))
	seen := map[capCoverageGeometryIdentity]struct{}{}
	pointCount := 0
	add := func(identity capCoverageGeometryIdentity, shape capCoverageShape) bool {
		if identity.Source != "clc" || len(identity.Code) != 6 || identity.Code[0] != '0' || len(shape.Polygons) == 0 {
			return false
		}
		if _, exists := seen[identity]; exists {
			return true
		}
		if pointCount+shape.PointCount > capCoverageGeometryMaxFeedPoints {
			return false
		}
		seen[identity] = struct{}{}
		features = append(features, capCoverageFeature{Identity: identity, Shape: shape})
		pointCount += shape.PointCount
		return true
	}
	for _, region := range feed.Locations.Coverage.Regions {
		candidates := capCoverageGeometryCandidates(region, db)
		if len(candidates) == 0 {
			return nil, false
		}
		found := false
		for _, candidate := range candidates {
			shape, state := loadCAPCoverageGeometry(baseDir, candidate)
			switch state {
			case capCoverageGeometryValid:
				if !add(candidate, shape) {
					return nil, false
				}
				found = true
			case capCoverageGeometryInvalid:
				return nil, false
			case capCoverageGeometryUnavailable:
				if !capCoverageGeometryCanExpandLeaves(candidate) {
					continue
				}
				leaves, leafState := capCoverageLeafGeometryIdentities(baseDir, candidate)
				if leafState != capCoverageGeometryValid || len(leaves) == 0 {
					continue
				}
				for _, leaf := range leaves {
					leafShape, leafGeometryState := loadCAPCoverageGeometry(baseDir, leaf)
					if leafGeometryState != capCoverageGeometryValid || !add(leaf, leafShape) {
						return nil, false
					}
				}
				found = true
			}
			if found {
				break
			}
		}
		if !found {
			return nil, false
		}
	}
	return features, len(features) > 0
}

func capCoverageGeometryCanExpandLeaves(identity capCoverageGeometryIdentity) bool {
	return identity.Source == "clc" && len(identity.Code) >= 4 && strings.HasSuffix(identity.Code, "00")
}

func capCoverageGeometryCandidates(region coverageRegionXML, db alertGeoDB) []capCoverageGeometryIdentity {
	code := canonicalCAPLocation(region.ID)
	if code == "" {
		return nil
	}
	source := strings.ToLower(strings.TrimSpace(region.Source))
	add := func(values []capCoverageGeometryIdentity, source string, code string) []capCoverageGeometryIdentity {
		candidate := capCoverageGeometryIdentity{Source: strings.ToLower(strings.TrimSpace(source)), Code: canonicalCAPLocation(code)}
		if candidate.Source == "" || candidate.Code == "" {
			return values
		}
		for _, value := range values {
			if value == candidate {
				return values
			}
		}
		return append(values, candidate)
	}
	values := []capCoverageGeometryIdentity{}
	switch source {
	case "", "clc":
		return add(values, "clc", code)
	case "eccc":
		values = add(values, "clc", code)
		for _, linked := range db.ForecastToCLC[code] {
			values = add(values, "clc", linked)
		}
		return values
	case "nws", "nws_same", "nws_zone", "nws_marine", "nws_marine_same", "nws_marine_zone":
		values = add(values, "nws_same", sameLocationCode(code))
		for _, sameCode := range sameLocationCodesForAlertCode(db, code) {
			values = add(values, "nws_same", sameCode)
		}
		return values
	default:
		return nil
	}
}

func loadCAPCoverageGeometry(baseDir string, identity capCoverageGeometryIdentity) (capCoverageShape, capCoverageGeometryState) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		baseDir = "."
	}
	path := filepath.Clean(filepath.Join(baseDir, filepath.FromSlash(capCoverageGeometryRelPath)))
	stat, err := os.Stat(path)
	if err != nil || stat.IsDir() {
		return capCoverageShape{}, capCoverageGeometryUnavailable
	}

	capCoverageGeometryCache.Lock()
	defer capCoverageGeometryCache.Unlock()
	catalog, exists := capCoverageGeometryCache.ByPath[path]
	if !exists || !catalog.Modified.Equal(stat.ModTime()) || catalog.Size != stat.Size() {
		catalog = capCoverageGeometryCatalog{
			Modified: stat.ModTime(),
			Size:     stat.Size(),
			Values:   map[capCoverageGeometryIdentity]capCoverageGeometryCacheValue{},
			Leaves:   map[capCoverageGeometryIdentity]capCoverageGeometryLeafCacheValue{},
		}
	}
	if cached, ok := catalog.Values[identity]; ok {
		capCoverageGeometryCache.ByPath[path] = catalog
		return cached.Shape, cached.State
	}

	shape, state := readCAPCoverageGeometry(path, identity)
	catalog.Values[identity] = capCoverageGeometryCacheValue{Shape: shape, State: state}
	capCoverageGeometryCache.ByPath[path] = catalog
	return shape, state
}

func capCoverageLeafGeometryIdentities(baseDir string, parent capCoverageGeometryIdentity) ([]capCoverageGeometryIdentity, capCoverageGeometryState) {
	if !capCoverageGeometryCanExpandLeaves(parent) {
		return nil, capCoverageGeometryUnavailable
	}
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		baseDir = "."
	}
	path := filepath.Clean(filepath.Join(baseDir, filepath.FromSlash(capCoverageGeometryRelPath)))
	stat, err := os.Stat(path)
	if err != nil || stat.IsDir() {
		return nil, capCoverageGeometryUnavailable
	}

	capCoverageGeometryCache.Lock()
	defer capCoverageGeometryCache.Unlock()
	catalog, exists := capCoverageGeometryCache.ByPath[path]
	if !exists || !catalog.Modified.Equal(stat.ModTime()) || catalog.Size != stat.Size() {
		catalog = capCoverageGeometryCatalog{
			Modified: stat.ModTime(),
			Size:     stat.Size(),
			Values:   map[capCoverageGeometryIdentity]capCoverageGeometryCacheValue{},
			Leaves:   map[capCoverageGeometryIdentity]capCoverageGeometryLeafCacheValue{},
		}
	}
	if cached, ok := catalog.Leaves[parent]; ok {
		capCoverageGeometryCache.ByPath[path] = catalog
		return append([]capCoverageGeometryIdentity(nil), cached.Identities...), cached.State
	}
	identities, state := readCAPCoverageLeafGeometryIdentities(path, parent)
	if catalog.Leaves == nil {
		catalog.Leaves = map[capCoverageGeometryIdentity]capCoverageGeometryLeafCacheValue{}
	}
	catalog.Leaves[parent] = capCoverageGeometryLeafCacheValue{
		Identities: append([]capCoverageGeometryIdentity(nil), identities...),
		State:      state,
	}
	capCoverageGeometryCache.ByPath[path] = catalog
	return identities, state
}

func readCAPCoverageLeafGeometryIdentities(path string, parent capCoverageGeometryIdentity) ([]capCoverageGeometryIdentity, capCoverageGeometryState) {
	ctx, cancel := context.WithTimeout(context.Background(), capCoverageGeometryQueryTimeout)
	defer cancel()
	database, err := sql.Open("sqlite", capCoverageGeometrySQLiteDSN(path))
	if err != nil {
		return nil, capCoverageGeometryUnavailable
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)

	rows, err := database.QueryContext(ctx, `
		SELECT code
		FROM area_geometries
		WHERE source = ? AND code LIKE ? AND code <> ? AND length(code) = 6 AND is_current = 1
		  AND geometry_type IN ('polygon', 'multipolygon')
		ORDER BY code ASC
	`, parent.Source, parent.Code[:4]+"%", parent.Code)
	if err != nil {
		return nil, capCoverageGeometryInvalid
	}
	defer rows.Close()
	identities := make([]capCoverageGeometryIdentity, 0)
	for rows.Next() {
		if len(identities) >= capCoverageGeometryMaxPolygons {
			return nil, capCoverageGeometryInvalid
		}
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, capCoverageGeometryInvalid
		}
		code = sameLocationCode(code)
		if len(code) != 6 || code[0] != '0' {
			return nil, capCoverageGeometryInvalid
		}
		identities = append(identities, capCoverageGeometryIdentity{Source: parent.Source, Code: code})
	}
	if err := rows.Err(); err != nil {
		return nil, capCoverageGeometryInvalid
	}
	if len(identities) == 0 {
		return nil, capCoverageGeometryUnavailable
	}
	return identities, capCoverageGeometryValid
}

func readCAPCoverageGeometry(path string, identity capCoverageGeometryIdentity) (capCoverageShape, capCoverageGeometryState) {
	if strings.HasSuffix(identity.Code, "/*") {
		return readCAPCoverageLeafGeometries(path, identity)
	}
	ctx, cancel := context.WithTimeout(context.Background(), capCoverageGeometryQueryTimeout)
	defer cancel()
	database, err := sql.Open("sqlite", capCoverageGeometrySQLiteDSN(path))
	if err != nil {
		return capCoverageShape{}, capCoverageGeometryUnavailable
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)

	var raw []byte
	err = database.QueryRowContext(ctx, `
		SELECT geometry_wkb
		FROM area_geometries
		WHERE source = ? AND code = ? AND is_current = 1
		  AND geometry_type IN ('polygon', 'multipolygon')
		LIMIT 1
	`, identity.Source, identity.Code).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return capCoverageShape{}, capCoverageGeometryUnavailable
		}
		return capCoverageShape{}, capCoverageGeometryInvalid
	}
	shape, ok := parseCAPCoverageWKB(raw)
	if !ok {
		return capCoverageShape{}, capCoverageGeometryInvalid
	}
	return shape, capCoverageGeometryValid
}

func readCAPCoverageLeafGeometries(path string, identity capCoverageGeometryIdentity) (capCoverageShape, capCoverageGeometryState) {
	parentCode := strings.TrimSuffix(identity.Code, "/*")
	if !capCoverageGeometryCanExpandLeaves(capCoverageGeometryIdentity{Source: identity.Source, Code: parentCode}) {
		return capCoverageShape{}, capCoverageGeometryUnavailable
	}
	ctx, cancel := context.WithTimeout(context.Background(), capCoverageGeometryQueryTimeout)
	defer cancel()
	database, err := sql.Open("sqlite", capCoverageGeometrySQLiteDSN(path))
	if err != nil {
		return capCoverageShape{}, capCoverageGeometryUnavailable
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)

	rows, err := database.QueryContext(ctx, `
		SELECT geometry_wkb
		FROM area_geometries
		WHERE source = ? AND code LIKE ? AND code <> ? AND is_current = 1
		  AND geometry_type IN ('polygon', 'multipolygon')
		ORDER BY code ASC
	`, identity.Source, parentCode[:4]+"%", parentCode)
	if err != nil {
		return capCoverageShape{}, capCoverageGeometryInvalid
	}
	defer rows.Close()

	shape := capCoverageShape{}
	count := 0
	for rows.Next() {
		count++
		if count > capCoverageGeometryMaxPolygons {
			return capCoverageShape{}, capCoverageGeometryInvalid
		}
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return capCoverageShape{}, capCoverageGeometryInvalid
		}
		leafShape, ok := parseCAPCoverageWKB(raw)
		if !ok || shape.PointCount+leafShape.PointCount > capCoverageGeometryMaxFeedPoints {
			return capCoverageShape{}, capCoverageGeometryInvalid
		}
		shape.Polygons = append(shape.Polygons, leafShape.Polygons...)
		shape.PointCount += leafShape.PointCount
		shape.Bounds = capCoverageBoundsUnion(shape.Bounds, leafShape.Bounds)
	}
	if err := rows.Err(); err != nil {
		return capCoverageShape{}, capCoverageGeometryInvalid
	}
	if count == 0 || len(shape.Polygons) == 0 || !shape.Bounds.Valid {
		return capCoverageShape{}, capCoverageGeometryUnavailable
	}
	return shape, capCoverageGeometryValid
}

func capCoverageGeometrySQLiteDSN(path string) string {
	values := url.Values{}
	values.Set("mode", "ro")
	values.Add("_pragma", "query_only(ON)")
	values.Add("_pragma", "temp_store(MEMORY)")
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + values.Encode()
}

type capCoverageWKBReader struct {
	Data           []byte
	Offset         int
	Polygons       int
	Rings          int
	Points         int
	TopologyBudget capCoverageTopologyBudget
}

func parseCAPCoverageWKB(raw []byte) (capCoverageShape, bool) {
	if len(raw) == 0 || len(raw) > capCoverageGeometryMaxWKBBytes {
		return capCoverageShape{}, false
	}
	reader := capCoverageWKBReader{
		Data:           raw,
		TopologyBudget: capCoverageTopologyBudget{Remaining: capCoverageGeometryMaxTopologyChecks},
	}
	shape, ok := reader.geometry(0)
	if !ok || reader.Offset != len(raw) || len(shape.Polygons) == 0 || !shape.Bounds.Valid {
		return capCoverageShape{}, false
	}
	return shape, true
}

func (reader *capCoverageWKBReader) geometry(depth int) (capCoverageShape, bool) {
	if reader == nil || depth > 1 {
		return capCoverageShape{}, false
	}
	order, ok := reader.byteOrder()
	if !ok {
		return capCoverageShape{}, false
	}
	geometryType, ok := reader.uint32(order)
	if !ok {
		return capCoverageShape{}, false
	}
	switch geometryType {
	case 3:
		polygon, ok := reader.polygon(order)
		if !ok {
			return capCoverageShape{}, false
		}
		return capCoverageShape{Polygons: []capCoveragePolygon{polygon}, Bounds: polygon.Bounds, PointCount: len(polygon.Outer) + capCoverageHolePointCount(polygon.Holes)}, true
	case 6:
		count, ok := reader.uint32(order)
		if !ok || count == 0 || count > capCoverageGeometryMaxPolygons {
			return capCoverageShape{}, false
		}
		shape := capCoverageShape{}
		for index := uint32(0); index < count; index++ {
			child, ok := reader.geometry(depth + 1)
			if !ok || len(child.Polygons) != 1 || shape.PointCount+child.PointCount > capCoverageGeometryMaxPoints {
				return capCoverageShape{}, false
			}
			shape.Polygons = append(shape.Polygons, child.Polygons[0])
			shape.PointCount += child.PointCount
			shape.Bounds = capCoverageBoundsUnion(shape.Bounds, child.Bounds)
		}
		return shape, len(shape.Polygons) > 0 && shape.Bounds.Valid
	default:
		return capCoverageShape{}, false
	}
}

func (reader *capCoverageWKBReader) polygon(order binary.ByteOrder) (capCoveragePolygon, bool) {
	if reader == nil || reader.Polygons >= capCoverageGeometryMaxPolygons {
		return capCoveragePolygon{}, false
	}
	reader.Polygons++
	ringCount, ok := reader.uint32(order)
	if !ok || ringCount == 0 || ringCount > capCoverageGeometryMaxRings {
		return capCoveragePolygon{}, false
	}
	polygon := capCoveragePolygon{}
	for ringIndex := uint32(0); ringIndex < ringCount; ringIndex++ {
		if reader.Rings >= capCoverageGeometryMaxRings {
			return capCoveragePolygon{}, false
		}
		reader.Rings++
		pointCount, ok := reader.uint32(order)
		if !ok || pointCount < 4 || pointCount > capCoverageGeometryMaxPoints || uint64(pointCount) > uint64((len(reader.Data)-reader.Offset)/16) || reader.Points+int(pointCount) > capCoverageGeometryMaxPoints {
			return capCoveragePolygon{}, false
		}
		ring := make([]capCoveragePoint, 0, pointCount)
		for pointIndex := uint32(0); pointIndex < pointCount; pointIndex++ {
			longitude, ok := reader.float64(order)
			if !ok {
				return capCoveragePolygon{}, false
			}
			latitude, ok := reader.float64(order)
			if !ok || !capCoveragePointValid(latitude, longitude) {
				return capCoveragePolygon{}, false
			}
			if len(ring) > 0 && math.Abs(ring[len(ring)-1].Longitude-longitude) > 180 {
				return capCoveragePolygon{}, false
			}
			ring = append(ring, capCoveragePoint{Latitude: latitude, Longitude: longitude})
		}
		reader.Points += len(ring)
		if ring[0] != ring[len(ring)-1] || math.Abs(capCoverageRingArea(ring)) <= 1e-12 {
			return capCoveragePolygon{}, false
		}
		if ringIndex == 0 {
			polygon.Outer = ring
			polygon.Bounds = capCoverageBoundsForRing(ring)
			if !polygon.Bounds.Valid || polygon.Bounds.MaxLongitude-polygon.Bounds.MinLongitude > 180 {
				return capCoveragePolygon{}, false
			}
		} else {
			polygon.Holes = append(polygon.Holes, ring)
		}
	}
	if len(polygon.Outer) == 0 || !polygon.Bounds.Valid {
		return capCoveragePolygon{}, false
	}
	if !capCoveragePolygonTopologyValid(polygon, &reader.TopologyBudget) {
		return capCoveragePolygon{}, false
	}
	return polygon, true
}

func (reader *capCoverageWKBReader) byteOrder() (binary.ByteOrder, bool) {
	if reader == nil || reader.Offset >= len(reader.Data) {
		return nil, false
	}
	value := reader.Data[reader.Offset]
	reader.Offset++
	switch value {
	case 0:
		return binary.BigEndian, true
	case 1:
		return binary.LittleEndian, true
	default:
		return nil, false
	}
}

func (reader *capCoverageWKBReader) uint32(order binary.ByteOrder) (uint32, bool) {
	if reader == nil || order == nil || reader.Offset+4 > len(reader.Data) {
		return 0, false
	}
	value := order.Uint32(reader.Data[reader.Offset : reader.Offset+4])
	reader.Offset += 4
	return value, true
}

func (reader *capCoverageWKBReader) float64(order binary.ByteOrder) (float64, bool) {
	if reader == nil || order == nil || reader.Offset+8 > len(reader.Data) {
		return 0, false
	}
	value := math.Float64frombits(order.Uint64(reader.Data[reader.Offset : reader.Offset+8]))
	reader.Offset += 8
	return value, true
}

func capCoverageHolePointCount(holes [][]capCoveragePoint) int {
	count := 0
	for _, hole := range holes {
		count += len(hole)
	}
	return count
}

func capCoveragePointValid(latitude float64, longitude float64) bool {
	return !math.IsNaN(latitude) && !math.IsNaN(longitude) &&
		!math.IsInf(latitude, 0) && !math.IsInf(longitude, 0) &&
		latitude >= -90 && latitude <= 90 && longitude >= -180 && longitude <= 180
}

func capCoverageRingArea(ring []capCoveragePoint) float64 {
	area := 0.0
	for index := 1; index < len(ring); index++ {
		previous := ring[index-1]
		current := ring[index]
		area += previous.Longitude*current.Latitude - current.Longitude*previous.Latitude
	}
	return area / 2
}

func capCoverageBoundsForRing(ring []capCoveragePoint) capCoverageBounds {
	bounds := capCoverageBounds{}
	for _, point := range ring {
		if !bounds.Valid {
			bounds = capCoverageBounds{MinLatitude: point.Latitude, MaxLatitude: point.Latitude, MinLongitude: point.Longitude, MaxLongitude: point.Longitude, Valid: true}
			continue
		}
		bounds.MinLatitude = math.Min(bounds.MinLatitude, point.Latitude)
		bounds.MaxLatitude = math.Max(bounds.MaxLatitude, point.Latitude)
		bounds.MinLongitude = math.Min(bounds.MinLongitude, point.Longitude)
		bounds.MaxLongitude = math.Max(bounds.MaxLongitude, point.Longitude)
	}
	return bounds
}

func capCoverageBoundsUnion(left capCoverageBounds, right capCoverageBounds) capCoverageBounds {
	if !left.Valid {
		return right
	}
	if !right.Valid {
		return left
	}
	return capCoverageBounds{
		MinLatitude:  math.Min(left.MinLatitude, right.MinLatitude),
		MaxLatitude:  math.Max(left.MaxLatitude, right.MaxLatitude),
		MinLongitude: math.Min(left.MinLongitude, right.MinLongitude),
		MaxLongitude: math.Max(left.MaxLongitude, right.MaxLongitude),
		Valid:        true,
	}
}

// capCoveragePolygonTopologyValid rejects self-crossing rings, holes that
// escape or touch their outer ring, and intersecting or nested holes. Those
// conditions make planar overlap ambiguous, so the caller must use code
// routing instead of guessing.
func capCoveragePolygonTopologyValid(polygon capCoveragePolygon, budget *capCoverageTopologyBudget) bool {
	if len(polygon.Outer) < 4 || !capCoverageRingSimple(polygon.Outer, budget) {
		return false
	}
	for holeIndex, hole := range polygon.Holes {
		if len(hole) < 4 || !capCoverageRingSimple(hole, budget) {
			return false
		}
		inside, ok := capCoveragePointInRingStrict(hole[0], polygon.Outer, budget)
		touches, complete := capCoverageRingsTouchOrCross(hole, polygon.Outer, budget)
		if !ok || !complete || !inside || touches {
			return false
		}
		for otherIndex := 0; otherIndex < holeIndex; otherIndex++ {
			other := polygon.Holes[otherIndex]
			touches, complete := capCoverageRingsTouchOrCross(hole, other, budget)
			if !complete || touches {
				return false
			}
			inside, ok := capCoveragePointInRingStrict(hole[0], other, budget)
			if !ok || inside {
				return false
			}
			inside, ok = capCoveragePointInRingStrict(other[0], hole, budget)
			if !ok || inside {
				return false
			}
		}
	}
	return true
}

func capCoverageRingSimple(ring []capCoveragePoint, budget *capCoverageTopologyBudget) bool {
	segmentCount := len(ring) - 1
	if segmentCount < 3 {
		return false
	}
	for firstIndex := 0; firstIndex < segmentCount; firstIndex++ {
		firstStart := ring[firstIndex]
		firstEnd := ring[firstIndex+1]
		if firstStart == firstEnd {
			return false
		}
		firstBounds := capCoverageBoundsForRing([]capCoveragePoint{firstStart, firstEnd})
		for secondIndex := firstIndex + 1; secondIndex < segmentCount; secondIndex++ {
			if secondIndex == firstIndex+1 || (firstIndex == 0 && secondIndex == segmentCount-1) {
				continue
			}
			if !budget.take() {
				return false
			}
			secondStart := ring[secondIndex]
			secondEnd := ring[secondIndex+1]
			if !capCoverageBoundsOverlap(firstBounds, capCoverageBoundsForRing([]capCoveragePoint{secondStart, secondEnd})) {
				continue
			}
			if capCoverageSegmentsTouchOrCross(firstStart, firstEnd, secondStart, secondEnd) {
				return false
			}
		}
	}
	return true
}

func capCoverageShapesOverlap(left capCoverageShape, right capCoverageShape) (bool, bool) {
	if !left.Bounds.Valid || !right.Bounds.Valid {
		return false, false
	}
	if !capCoverageBoundsOverlap(left.Bounds, right.Bounds) {
		return false, true
	}
	budget := capCoverageEdgeBudget{Remaining: capCoverageGeometryMaxEdgeChecks}
	for _, leftPolygon := range left.Polygons {
		for _, rightPolygon := range right.Polygons {
			if !capCoverageBoundsOverlap(leftPolygon.Bounds, rightPolygon.Bounds) {
				continue
			}
			overlaps, ok := capCoveragePolygonsOverlap(leftPolygon, rightPolygon, &budget)
			if !ok {
				return false, false
			}
			if overlaps {
				return true, true
			}
		}
	}
	return false, true
}

type capCoverageEdgeBudget struct {
	Remaining int
}

func (budget *capCoverageEdgeBudget) take() bool {
	if budget == nil || budget.Remaining <= 0 {
		return false
	}
	budget.Remaining--
	return true
}

func capCoveragePolygonsOverlap(left capCoveragePolygon, right capCoveragePolygon, budget *capCoverageEdgeBudget) (bool, bool) {
	for _, leftRing := range capCoveragePolygonRings(left) {
		for _, rightRing := range capCoveragePolygonRings(right) {
			overlaps, ok := capCoverageRingsProperlyCross(leftRing, rightRing, budget)
			if !ok {
				return false, false
			}
			if overlaps {
				return true, true
			}
		}
	}
	inside, ok := capCoveragePointInPolygon(left.Outer[0], right, budget)
	if !ok {
		return false, false
	}
	if inside {
		return true, true
	}
	inside, ok = capCoveragePointInPolygon(right.Outer[0], left, budget)
	if !ok {
		return false, false
	}
	return inside, true
}

func capCoveragePolygonRings(polygon capCoveragePolygon) [][]capCoveragePoint {
	rings := make([][]capCoveragePoint, 0, len(polygon.Holes)+1)
	if len(polygon.Outer) > 0 {
		rings = append(rings, polygon.Outer)
	}
	rings = append(rings, polygon.Holes...)
	return rings
}

type capCoverageBudget interface {
	take() bool
}

func capCoverageRingsProperlyCross(left []capCoveragePoint, right []capCoveragePoint, budget *capCoverageEdgeBudget) (bool, bool) {
	if len(left) < 2 || len(right) < 2 {
		return false, false
	}
	for leftIndex := 1; leftIndex < len(left); leftIndex++ {
		leftStart := left[leftIndex-1]
		leftEnd := left[leftIndex]
		leftBounds := capCoverageBoundsForRing([]capCoveragePoint{leftStart, leftEnd})
		for rightIndex := 1; rightIndex < len(right); rightIndex++ {
			if !budget.take() {
				return false, false
			}
			rightStart := right[rightIndex-1]
			rightEnd := right[rightIndex]
			if !capCoverageBoundsOverlap(leftBounds, capCoverageBoundsForRing([]capCoveragePoint{rightStart, rightEnd})) {
				continue
			}
			if capCoverageSegmentsProperlyCross(leftStart, leftEnd, rightStart, rightEnd) {
				return true, true
			}
		}
	}
	return false, true
}

func capCoverageRingsTouchOrCross(left []capCoveragePoint, right []capCoveragePoint, budget *capCoverageTopologyBudget) (bool, bool) {
	if len(left) < 2 || len(right) < 2 {
		return false, false
	}
	for leftIndex := 1; leftIndex < len(left); leftIndex++ {
		leftStart := left[leftIndex-1]
		leftEnd := left[leftIndex]
		leftBounds := capCoverageBoundsForRing([]capCoveragePoint{leftStart, leftEnd})
		for rightIndex := 1; rightIndex < len(right); rightIndex++ {
			if !budget.take() {
				return false, false
			}
			rightStart := right[rightIndex-1]
			rightEnd := right[rightIndex]
			if !capCoverageBoundsOverlap(leftBounds, capCoverageBoundsForRing([]capCoveragePoint{rightStart, rightEnd})) {
				continue
			}
			if capCoverageSegmentsTouchOrCross(leftStart, leftEnd, rightStart, rightEnd) {
				return true, true
			}
		}
	}
	return false, true
}

func capCoverageSegmentsProperlyCross(firstStart capCoveragePoint, firstEnd capCoveragePoint, secondStart capCoveragePoint, secondEnd capCoveragePoint) bool {
	first := capCoverageOrientation(firstStart, firstEnd, secondStart)
	second := capCoverageOrientation(firstStart, firstEnd, secondEnd)
	third := capCoverageOrientation(secondStart, secondEnd, firstStart)
	fourth := capCoverageOrientation(secondStart, secondEnd, firstEnd)
	return first*second < 0 && third*fourth < 0
}

func capCoverageSegmentsTouchOrCross(firstStart capCoveragePoint, firstEnd capCoveragePoint, secondStart capCoveragePoint, secondEnd capCoveragePoint) bool {
	if capCoverageSegmentsProperlyCross(firstStart, firstEnd, secondStart, secondEnd) {
		return true
	}
	first := capCoverageOrientation(firstStart, firstEnd, secondStart)
	second := capCoverageOrientation(firstStart, firstEnd, secondEnd)
	third := capCoverageOrientation(secondStart, secondEnd, firstStart)
	fourth := capCoverageOrientation(secondStart, secondEnd, firstEnd)
	return (first == 0 && capCoveragePointOnSegment(secondStart, firstStart, firstEnd)) ||
		(second == 0 && capCoveragePointOnSegment(secondEnd, firstStart, firstEnd)) ||
		(third == 0 && capCoveragePointOnSegment(firstStart, secondStart, secondEnd)) ||
		(fourth == 0 && capCoveragePointOnSegment(firstEnd, secondStart, secondEnd))
}

func capCoverageOrientation(first capCoveragePoint, second capCoveragePoint, third capCoveragePoint) int {
	value := (second.Longitude-first.Longitude)*(third.Latitude-first.Latitude) - (second.Latitude-first.Latitude)*(third.Longitude-first.Longitude)
	const epsilon = 1e-12
	switch {
	case value > epsilon:
		return 1
	case value < -epsilon:
		return -1
	default:
		return 0
	}
}

func capCoveragePointOnSegment(point capCoveragePoint, start capCoveragePoint, end capCoveragePoint) bool {
	const epsilon = 1e-12
	return point.Longitude >= math.Min(start.Longitude, end.Longitude)-epsilon &&
		point.Longitude <= math.Max(start.Longitude, end.Longitude)+epsilon &&
		point.Latitude >= math.Min(start.Latitude, end.Latitude)-epsilon &&
		point.Latitude <= math.Max(start.Latitude, end.Latitude)+epsilon
}

func capCoveragePointInPolygon(point capCoveragePoint, polygon capCoveragePolygon, budget *capCoverageEdgeBudget) (bool, bool) {
	inside, ok := capCoveragePointInRingStrict(point, polygon.Outer, budget)
	if !ok || !inside {
		return inside, ok
	}
	for _, hole := range polygon.Holes {
		inHole, ok := capCoveragePointInRingStrict(point, hole, budget)
		if !ok {
			return false, false
		}
		if inHole {
			return false, true
		}
	}
	return true, true
}

func capCoveragePointInRingStrict(point capCoveragePoint, ring []capCoveragePoint, budget capCoverageBudget) (bool, bool) {
	if len(ring) < 4 {
		return false, false
	}
	inside := false
	for index := 1; index < len(ring); index++ {
		if !budget.take() {
			return false, false
		}
		previous := ring[index-1]
		current := ring[index]
		if capCoverageOrientation(previous, current, point) == 0 && capCoveragePointOnSegment(point, previous, current) {
			return false, true
		}
		crosses := (previous.Latitude > point.Latitude) != (current.Latitude > point.Latitude)
		if crosses && point.Longitude < (current.Longitude-previous.Longitude)*(point.Latitude-previous.Latitude)/(current.Latitude-previous.Latitude)+previous.Longitude {
			inside = !inside
		}
	}
	return inside, true
}

func capCoverageBoundsOverlap(left capCoverageBounds, right capCoverageBounds) bool {
	if !left.Valid || !right.Valid {
		return false
	}
	const epsilon = 1e-12
	return left.MinLongitude <= right.MaxLongitude+epsilon && left.MaxLongitude+epsilon >= right.MinLongitude &&
		left.MinLatitude <= right.MaxLatitude+epsilon && left.MaxLatitude+epsilon >= right.MinLatitude
}
