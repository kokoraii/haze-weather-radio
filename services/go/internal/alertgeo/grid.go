// Package alertgeo provides bounded, fail-closed helpers for geographic alert
// targeting. It is intentionally independent of feed configuration and CAP
// ingestion so callers can keep source-qualified coverage policy at their own
// service boundary.
package alertgeo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	gridColumns = 3
	gridRows    = 3

	maxWKBBytes           = 1 << 20
	maxGeometryPolygons   = 128
	maxGeometryRings      = 256
	maxGeometryVertices   = 16_384
	maxRingVertices       = 16_384
	maxCAPPolygons        = 16
	maxCAPVertices        = 2048
	maxCAPRingVertices    = 1024
	maxCAPPolygonBytes    = 64 << 10
	maxTopologyChecks     = 2_000_000
	maxIntersectionChecks = 1_000_000
	minimumAreaDegrees    = 1e-12
	maximumLongitudeSpan  = 180.0
)

var (
	// ErrInvalidBaseCode indicates that a parent SAME/CLC code cannot be split
	// into an NWS-style partial-location code.
	ErrInvalidBaseCode = errors.New("alert grid base code must be a six-digit P=0 location code")
	// ErrInvalidGeometry indicates malformed WGS84 polygon geometry.
	ErrInvalidGeometry = errors.New("alert grid geometry is invalid")
	// ErrGeometryTooLarge indicates input that exceeds the fixed memory bounds.
	ErrGeometryTooLarge = errors.New("alert grid geometry exceeds limits")
	// ErrGeometryTooComplex indicates input that would exceed the fixed CPU bound.
	ErrGeometryTooComplex = errors.New("alert grid geometry exceeds complexity limit")
	// ErrInvalidCAPPolygon indicates a malformed CAP polygon string or shape.
	ErrInvalidCAPPolygon = errors.New("CAP polygon is invalid")
	// ErrCAPPolygonTooLarge indicates CAP input exceeding the fixed memory bounds.
	ErrCAPPolygonTooLarge = errors.New("CAP polygon exceeds limits")
)

// Point is a WGS84 coordinate. CAP supplies latitude,longitude pairs. WKB
// supplies longitude,latitude values which GeometryFromWKB converts here.
type Point struct {
	Latitude  float64
	Longitude float64
}

// Ring is a closed simple linear ring. Its first and final points must match.
type Ring []Point

// Polygon is one exterior ring and zero or more interior-hole rings.
type Polygon struct {
	Exterior Ring
	Holes    []Ring
}

// Geometry is a polygon or multipolygon feature in WGS84 coordinates.
type Geometry struct {
	Polygons []Polygon
}

// Bounds is a non-wrapping WGS84 extent. Dateline-wrapping geometry is
// deliberately rejected because a three-by-three grid would otherwise be
// ambiguous.
type Bounds struct {
	West  float64
	South float64
	East  float64
	North float64
}

// Valid reports whether bounds can be used safely for a local three-by-three
// partition grid.
func (bounds Bounds) Valid() bool {
	return validBounds(bounds) == nil
}

// Partition is a delivery scope within a Grid. Number follows the NWS partial
// county convention: 1 northwest, 2 north, 3 northeast, 4 west, 5 centre,
// 6 east, 7 southwest, 8 south, and 9 southeast. Number zero denotes the
// complete parent feature and is returned only after all nine parts match.
// Bounds is the geographic envelope of the projected cell corners. Matching
// always uses the precise local-projection cell, never this display envelope.
type Partition struct {
	Number     int
	Code       string
	Identifier string
	Bounds     Bounds
}

// Grid is an immutable feature geometry and its NWS-style three-by-three
// delivery partitions. Construct it with NewGrid, NewGridForBounds, or
// GridFromWKB.
type Grid struct {
	baseCode            string
	bounds              Bounds
	projection          laeaProjection
	polygons            []Polygon
	featureBounds       []Bounds
	projectedPartitions [gridColumns * gridRows]Bounds
	partitions          [gridColumns * gridRows]Partition
}

// BaseCode returns the P=0 parent SAME/CLC code used to construct the grid.
func (grid Grid) BaseCode() string {
	return grid.baseCode
}

// Bounds returns the feature extent used to derive the three-by-three grid.
func (grid Grid) Bounds() Bounds {
	return grid.bounds
}

// Partitions returns the nine partial P=1 through P=9 partitions in NWS
// order. It never includes the whole-parent P=0 partition.
func (grid Grid) Partitions() []Partition {
	if grid.baseCode == "" {
		return nil
	}
	result := make([]Partition, len(grid.partitions))
	copy(result, grid.partitions[:])
	return result
}

// WholePartition returns the P=0 delivery scope for the complete feature.
func (grid Grid) WholePartition() Partition {
	if grid.baseCode == "" {
		return Partition{}
	}
	return Partition{
		Number:     0,
		Code:       grid.baseCode,
		Identifier: grid.baseCode,
		Bounds:     grid.bounds,
	}
}

// PartitionCode derives an NWS-style partial-location code. For example,
// PartitionCode("065435", 1) returns "165435".
func PartitionCode(baseCode string, partition int) (string, error) {
	baseCode, err := normalizedBaseCode(baseCode)
	if err != nil {
		return "", err
	}
	if partition < 1 || partition > gridColumns*gridRows {
		return "", fmt.Errorf("%w: partition must be in 1..9", ErrInvalidBaseCode)
	}
	return strconv.Itoa(partition) + baseCode[1:], nil
}

// PartitionIdentifier returns a parent-child identifier suitable for storage
// and lifecycle matching. For example, it returns "065435-165435" for the
// northwest portion of CLC 065435.
func PartitionIdentifier(baseCode string, partition int) (string, error) {
	baseCode, err := normalizedBaseCode(baseCode)
	if err != nil {
		return "", err
	}
	code, err := PartitionCode(baseCode, partition)
	if err != nil {
		return "", err
	}
	return baseCode + "-" + code, nil
}

// NewGrid constructs a grid from valid WGS84 feature geometry. Geometry is
// copied, so later caller mutation cannot change alert targeting.
func NewGrid(baseCode string, geometry Geometry) (Grid, error) {
	baseCode, err := normalizedBaseCode(baseCode)
	if err != nil {
		return Grid{}, err
	}
	polygons, bounds, err := validateAndCopyGeometry(geometry)
	if err != nil {
		return Grid{}, err
	}
	return newGrid(baseCode, bounds, polygons)
}

// NewGridForBounds constructs a grid for a feature that has only a valid
// bounding area. Its geometry is the bounds rectangle itself.
func NewGridForBounds(baseCode string, bounds Bounds) (Grid, error) {
	if err := validBounds(bounds); err != nil {
		return Grid{}, err
	}
	geometry := Geometry{Polygons: []Polygon{{Exterior: Ring{
		{Latitude: bounds.South, Longitude: bounds.West},
		{Latitude: bounds.South, Longitude: bounds.East},
		{Latitude: bounds.North, Longitude: bounds.East},
		{Latitude: bounds.North, Longitude: bounds.West},
		{Latitude: bounds.South, Longitude: bounds.West},
	}}}}
	return NewGrid(baseCode, geometry)
}

// GridFromWKB decodes a bounded standard WKB Polygon or MultiPolygon feature,
// then builds a three-by-three grid. It accepts both little- and big-endian
// WKB and rejects EWKB extensions, trailing bytes, malformed rings, and input
// over the fixed resource limits.
func GridFromWKB(baseCode string, data []byte) (Grid, error) {
	geometry, err := GeometryFromWKB(data)
	if err != nil {
		return Grid{}, err
	}
	return NewGrid(baseCode, geometry)
}

// GeometryFromWKB decodes a bounded standard WKB Polygon or MultiPolygon into
// the package's WGS84 geometry representation.
func GeometryFromWKB(data []byte) (Geometry, error) {
	if len(data) == 0 {
		return Geometry{}, fmt.Errorf("%w: WKB is empty", ErrInvalidGeometry)
	}
	if len(data) > maxWKBBytes {
		return Geometry{}, fmt.Errorf("%w: WKB is larger than %d bytes", ErrGeometryTooLarge, maxWKBBytes)
	}
	reader := wkbReader{data: data}
	geometry, err := reader.geometry(0)
	if err != nil {
		return Geometry{}, err
	}
	if reader.offset != len(reader.data) {
		return Geometry{}, fmt.Errorf("%w: WKB has trailing bytes", ErrInvalidGeometry)
	}
	return geometry, nil
}

// ParseCAPPolygon parses one CAP polygon in its standard "lat,lon lat,lon"
// representation. An omitted final closure coordinate is added. CAP circles
// are intentionally not accepted here because callers must make their chosen
// circle approximation explicit before grid targeting.
func ParseCAPPolygon(raw string) (Polygon, error) {
	if len(raw) == 0 {
		return Polygon{}, fmt.Errorf("%w: polygon is empty", ErrInvalidCAPPolygon)
	}
	if len(raw) > maxCAPPolygonBytes {
		return Polygon{}, fmt.Errorf("%w: polygon is larger than %d bytes", ErrCAPPolygonTooLarge, maxCAPPolygonBytes)
	}
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) < 3 {
		return Polygon{}, fmt.Errorf("%w: polygon has fewer than three points", ErrInvalidCAPPolygon)
	}
	if len(fields) > maxCAPRingVertices {
		return Polygon{}, fmt.Errorf("%w: polygon has too many points", ErrCAPPolygonTooLarge)
	}
	ring := make(Ring, 0, len(fields)+1)
	for _, field := range fields {
		parts := strings.Split(field, ",")
		if len(parts) != 2 {
			return Polygon{}, fmt.Errorf("%w: point %q is not latitude,longitude", ErrInvalidCAPPolygon, field)
		}
		latitude, latitudeErr := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
		longitude, longitudeErr := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if latitudeErr != nil || longitudeErr != nil || !validPoint(Point{Latitude: latitude, Longitude: longitude}) {
			return Polygon{}, fmt.Errorf("%w: point %q is outside WGS84", ErrInvalidCAPPolygon, field)
		}
		if len(ring) > 0 && samePoint(ring[len(ring)-1], Point{Latitude: latitude, Longitude: longitude}) {
			return Polygon{}, fmt.Errorf("%w: polygon has a duplicate adjacent point", ErrInvalidCAPPolygon)
		}
		ring = append(ring, Point{Latitude: latitude, Longitude: longitude})
	}
	if !samePoint(ring[0], ring[len(ring)-1]) {
		ring = append(ring, ring[0])
	}
	polygon := Polygon{Exterior: ring}
	if _, _, err := validateCAPPolygon(polygon, newBudget(maxTopologyChecks)); err != nil {
		return Polygon{}, err
	}
	return polygon, nil
}

// MatchCAPPolygons parses CAP polygon strings and returns the partial
// partitions with positive-area overlap. If all nine partitions overlap, it
// returns only the P=0 whole-parent partition. If any input is malformed or
// exceeds a resource limit, it returns no partitions and an error.
func (grid Grid) MatchCAPPolygons(rawPolygons []string) ([]Partition, error) {
	if len(rawPolygons) == 0 {
		return nil, nil
	}
	if len(rawPolygons) > maxCAPPolygons {
		return nil, fmt.Errorf("%w: alert contains more than %d polygons", ErrCAPPolygonTooLarge, maxCAPPolygons)
	}
	polygons := make([]Polygon, 0, len(rawPolygons))
	vertices := 0
	for _, raw := range rawPolygons {
		polygon, err := ParseCAPPolygon(raw)
		if err != nil {
			return nil, err
		}
		vertices += len(polygon.Exterior) - 1
		if vertices > maxCAPVertices {
			return nil, fmt.Errorf("%w: alert has more than %d vertices", ErrCAPPolygonTooLarge, maxCAPVertices)
		}
		polygons = append(polygons, polygon)
	}
	return grid.MatchPolygons(polygons)
}

// MatchPolygons returns the partial partitions with positive-area overlap
// against already parsed CAP polygons. CAP polygons must not contain holes. If
// all nine partitions overlap, it returns only the P=0 whole-parent partition.
func (grid Grid) MatchPolygons(capPolygons []Polygon) ([]Partition, error) {
	if !grid.valid() {
		return nil, fmt.Errorf("%w: grid was not constructed by a valid constructor", ErrInvalidGeometry)
	}
	if len(capPolygons) == 0 {
		return nil, nil
	}
	if len(capPolygons) > maxCAPPolygons {
		return nil, fmt.Errorf("%w: alert contains more than %d polygons", ErrCAPPolygonTooLarge, maxCAPPolygons)
	}
	topology := newBudget(maxTopologyChecks)
	vertices := 0
	validated := make([]Polygon, 0, len(capPolygons))
	for _, polygon := range capPolygons {
		copyPolygon, bounds, err := validateCAPPolygon(polygon, topology)
		if err != nil {
			return nil, err
		}
		vertices += len(copyPolygon.Exterior) - 1
		if vertices > maxCAPVertices {
			return nil, fmt.Errorf("%w: alert has more than %d vertices", ErrCAPPolygonTooLarge, maxCAPVertices)
		}
		if !boundsOverlapPositive(grid.bounds, bounds) {
			continue
		}
		projected, err := projectPolygon(copyPolygon, grid.projection)
		if err != nil {
			return nil, err
		}
		validated = append(validated, projected)
	}
	if len(validated) == 0 {
		return nil, nil
	}

	matched := [gridColumns * gridRows]bool{}
	work := newBudget(maxIntersectionChecks)
	for _, capPolygon := range validated {
		for index := range grid.partitions {
			if matched[index] {
				continue
			}
			clipped := clipRingToBounds(capPolygon.Exterior, grid.projectedPartitions[index])
			if len(clipped) < 4 || math.Abs(ringArea(clipped)) <= minimumAreaDegrees {
				continue
			}
			clippedBounds, ok := planarRingBounds(clipped)
			if !ok {
				return nil, fmt.Errorf("%w: clipped CAP polygon has invalid coordinates", ErrInvalidCAPPolygon)
			}
			for featureIndex, featurePolygon := range grid.polygons {
				if !boundsOverlapPositive(grid.featureBounds[featureIndex], clippedBounds) {
					continue
				}
				overlaps, err := polygonOverlapsCAP(featurePolygon, clipped, work)
				if err != nil {
					return nil, err
				}
				if overlaps {
					matched[index] = true
					break
				}
			}
		}
	}

	result := make([]Partition, 0, len(grid.partitions))
	for index, partition := range grid.partitions {
		if matched[index] {
			result = append(result, partition)
		}
	}
	if len(result) == len(grid.partitions) {
		return []Partition{grid.WholePartition()}, nil
	}
	return result, nil
}

func newGrid(baseCode string, bounds Bounds, polygons []Polygon) (Grid, error) {
	projection, err := newLAEAProjection(bounds)
	if err != nil {
		return Grid{}, err
	}
	projectedPolygons, featureBounds, projectedBounds, err := projectGeometry(polygons, projection)
	if err != nil {
		return Grid{}, err
	}
	grid := Grid{
		baseCode:      baseCode,
		bounds:        bounds,
		projection:    projection,
		polygons:      projectedPolygons,
		featureBounds: featureBounds,
	}
	width := (projectedBounds.East - projectedBounds.West) / gridColumns
	height := (projectedBounds.North - projectedBounds.South) / gridRows
	for row := 0; row < gridRows; row++ {
		for column := 0; column < gridColumns; column++ {
			number := row*gridColumns + column + 1
			code, _ := PartitionCode(baseCode, number)
			identifier, _ := PartitionIdentifier(baseCode, number)
			north := projectedBounds.North - float64(row)*height
			south := projectedBounds.North - float64(row+1)*height
			west := projectedBounds.West + float64(column)*width
			east := projectedBounds.West + float64(column+1)*width
			projectedPartition := Bounds{West: west, South: south, East: east, North: north}
			partitionBounds, err := projection.boundsForProjectedCell(projectedPartition)
			if err != nil {
				return Grid{}, err
			}
			grid.projectedPartitions[number-1] = projectedPartition
			grid.partitions[number-1] = Partition{
				Number:     number,
				Code:       code,
				Identifier: identifier,
				Bounds:     partitionBounds,
			}
		}
	}
	return grid, nil
}

func (grid Grid) valid() bool {
	if _, err := normalizedBaseCode(grid.baseCode); err != nil || validBounds(grid.bounds) != nil || len(grid.polygons) == 0 || len(grid.featureBounds) != len(grid.polygons) || !grid.projection.valid() {
		return false
	}
	return true
}

func normalizedBaseCode(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) != 6 || value[0] != '0' {
		return "", ErrInvalidBaseCode
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return "", ErrInvalidBaseCode
		}
	}
	return value, nil
}

func validateAndCopyGeometry(geometry Geometry) ([]Polygon, Bounds, error) {
	if len(geometry.Polygons) == 0 {
		return nil, Bounds{}, fmt.Errorf("%w: geometry has no polygons", ErrInvalidGeometry)
	}
	if len(geometry.Polygons) > maxGeometryPolygons {
		return nil, Bounds{}, fmt.Errorf("%w: geometry has more than %d polygons", ErrGeometryTooLarge, maxGeometryPolygons)
	}
	polygons := make([]Polygon, 0, len(geometry.Polygons))
	vertices := 0
	rings := 0
	firstBounds := true
	var bounds Bounds
	topology := newBudget(maxTopologyChecks)
	for _, polygon := range geometry.Polygons {
		copyPolygon, polygonBounds, count, ringCount, err := validateFeaturePolygon(polygon, topology)
		if err != nil {
			return nil, Bounds{}, err
		}
		vertices += count
		rings += ringCount
		if vertices > maxGeometryVertices || rings > maxGeometryRings {
			return nil, Bounds{}, fmt.Errorf("%w: geometry exceeds vertex or ring limits", ErrGeometryTooLarge)
		}
		if firstBounds {
			bounds = polygonBounds
			firstBounds = false
		} else {
			bounds = mergeBounds(bounds, polygonBounds)
		}
		polygons = append(polygons, copyPolygon)
	}
	if err := validBounds(bounds); err != nil {
		return nil, Bounds{}, err
	}
	return polygons, bounds, nil
}

func validateFeaturePolygon(polygon Polygon, topology *budget) (Polygon, Bounds, int, int, error) {
	exterior, bounds, count, err := validateRing(polygon.Exterior, ErrInvalidGeometry, ErrGeometryTooLarge, topology)
	if err != nil {
		return Polygon{}, Bounds{}, 0, 0, err
	}
	copyPolygon := Polygon{Exterior: exterior}
	for _, hole := range polygon.Holes {
		copyHole, _, holeCount, err := validateRing(hole, ErrInvalidGeometry, ErrGeometryTooLarge, topology)
		if err != nil {
			return Polygon{}, Bounds{}, 0, 0, err
		}
		inside, err := pointStrictlyInsideRing(copyHole[0], exterior, topology)
		if err != nil || !inside {
			if err != nil {
				return Polygon{}, Bounds{}, 0, 0, err
			}
			return Polygon{}, Bounds{}, 0, 0, fmt.Errorf("%w: hole is not inside its exterior", ErrInvalidGeometry)
		}
		crosses, err := ringsIntersect(copyHole, exterior, topology)
		if err != nil {
			return Polygon{}, Bounds{}, 0, 0, err
		}
		if crosses {
			return Polygon{}, Bounds{}, 0, 0, fmt.Errorf("%w: hole crosses its exterior", ErrInvalidGeometry)
		}
		for _, existing := range copyPolygon.Holes {
			crosses, err = ringsIntersect(copyHole, existing, topology)
			if err != nil {
				return Polygon{}, Bounds{}, 0, 0, err
			}
			if crosses {
				return Polygon{}, Bounds{}, 0, 0, fmt.Errorf("%w: holes overlap", ErrInvalidGeometry)
			}
			insideExisting, err := pointStrictlyInsideRing(copyHole[0], existing, topology)
			if err != nil {
				return Polygon{}, Bounds{}, 0, 0, err
			}
			insideNew, err := pointStrictlyInsideRing(existing[0], copyHole, topology)
			if err != nil {
				return Polygon{}, Bounds{}, 0, 0, err
			}
			if insideExisting || insideNew {
				return Polygon{}, Bounds{}, 0, 0, fmt.Errorf("%w: holes overlap", ErrInvalidGeometry)
			}
		}
		copyPolygon.Holes = append(copyPolygon.Holes, copyHole)
		count += holeCount
	}
	return copyPolygon, bounds, count, len(copyPolygon.Holes) + 1, nil
}

func validateCAPPolygon(polygon Polygon, topology *budget) (Polygon, Bounds, error) {
	if len(polygon.Holes) != 0 {
		return Polygon{}, Bounds{}, fmt.Errorf("%w: CAP polygons cannot contain holes", ErrInvalidCAPPolygon)
	}
	if len(polygon.Exterior) < 4 || len(polygon.Exterior)-1 > maxCAPRingVertices {
		return Polygon{}, Bounds{}, fmt.Errorf("%w: CAP polygon has too many vertices", ErrCAPPolygonTooLarge)
	}
	exterior, bounds, _, err := validateRing(polygon.Exterior, ErrInvalidCAPPolygon, ErrCAPPolygonTooLarge, topology)
	if err != nil {
		return Polygon{}, Bounds{}, err
	}
	return Polygon{Exterior: exterior}, bounds, nil
}

func validateRing(ring Ring, invalidErr error, tooLargeErr error, topology *budget) (Ring, Bounds, int, error) {
	if len(ring) < 4 {
		return nil, Bounds{}, 0, fmt.Errorf("%w: ring has fewer than four closed points", invalidErr)
	}
	if len(ring)-1 > maxRingVertices {
		return nil, Bounds{}, 0, fmt.Errorf("%w: ring has more than %d vertices", tooLargeErr, maxRingVertices)
	}
	if !samePoint(ring[0], ring[len(ring)-1]) {
		return nil, Bounds{}, 0, fmt.Errorf("%w: ring is not closed", invalidErr)
	}
	copyRing := append(Ring(nil), ring...)
	for index, point := range copyRing {
		if !validPoint(point) {
			return nil, Bounds{}, 0, fmt.Errorf("%w: ring point %d is outside WGS84", invalidErr, index)
		}
		if index > 0 && samePoint(point, copyRing[index-1]) {
			return nil, Bounds{}, 0, fmt.Errorf("%w: ring has a duplicate adjacent point", invalidErr)
		}
	}
	bounds, err := ringBounds(copyRing)
	if err != nil {
		return nil, Bounds{}, 0, fmt.Errorf("%w: %v", invalidErr, err)
	}
	if math.Abs(ringArea(copyRing)) <= minimumAreaDegrees {
		return nil, Bounds{}, 0, fmt.Errorf("%w: ring has zero area", invalidErr)
	}
	if len(copyRing)-1 <= maxCAPRingVertices {
		if err := validateSimpleRing(copyRing, topology, invalidErr); err != nil {
			return nil, Bounds{}, 0, err
		}
	}
	return copyRing, bounds, len(copyRing) - 1, nil
}

func validBounds(bounds Bounds) error {
	for _, value := range []float64{bounds.West, bounds.South, bounds.East, bounds.North} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("%w: bounds contain a non-finite coordinate", ErrInvalidGeometry)
		}
	}
	if bounds.West < -180 || bounds.East > 180 || bounds.South < -90 || bounds.North > 90 {
		return fmt.Errorf("%w: bounds are outside WGS84", ErrInvalidGeometry)
	}
	if bounds.West >= bounds.East || bounds.South >= bounds.North {
		return fmt.Errorf("%w: bounds have no positive area", ErrInvalidGeometry)
	}
	if bounds.East-bounds.West >= maximumLongitudeSpan {
		return fmt.Errorf("%w: dateline-wrapping bounds are unsupported", ErrInvalidGeometry)
	}
	return nil
}

func validPoint(point Point) bool {
	return !math.IsNaN(point.Latitude) && !math.IsInf(point.Latitude, 0) &&
		!math.IsNaN(point.Longitude) && !math.IsInf(point.Longitude, 0) &&
		point.Latitude >= -90 && point.Latitude <= 90 && point.Longitude >= -180 && point.Longitude <= 180
}

func samePoint(left Point, right Point) bool {
	return left.Latitude == right.Latitude && left.Longitude == right.Longitude
}

func ringBounds(ring Ring) (Bounds, error) {
	bounds, ok := planarRingBounds(ring)
	if !ok {
		return Bounds{}, errors.New("ring is empty or has non-finite coordinates")
	}
	return bounds, validBounds(bounds)
}

func planarRingBounds(ring Ring) (Bounds, bool) {
	if len(ring) == 0 {
		return Bounds{}, false
	}
	bounds := Bounds{West: ring[0].Longitude, East: ring[0].Longitude, South: ring[0].Latitude, North: ring[0].Latitude}
	if !finiteCoordinate(bounds.West) || !finiteCoordinate(bounds.South) {
		return Bounds{}, false
	}
	for _, point := range ring[1:] {
		if !finiteCoordinate(point.Longitude) || !finiteCoordinate(point.Latitude) {
			return Bounds{}, false
		}
		if point.Longitude < bounds.West {
			bounds.West = point.Longitude
		}
		if point.Longitude > bounds.East {
			bounds.East = point.Longitude
		}
		if point.Latitude < bounds.South {
			bounds.South = point.Latitude
		}
		if point.Latitude > bounds.North {
			bounds.North = point.Latitude
		}
	}
	if bounds.West >= bounds.East || bounds.South >= bounds.North {
		return Bounds{}, false
	}
	return bounds, true
}

func finiteCoordinate(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func mergeBounds(left Bounds, right Bounds) Bounds {
	return Bounds{
		West:  math.Min(left.West, right.West),
		South: math.Min(left.South, right.South),
		East:  math.Max(left.East, right.East),
		North: math.Max(left.North, right.North),
	}
}

func boundsOverlapPositive(left Bounds, right Bounds) bool {
	return left.West < right.East && left.East > right.West && left.South < right.North && left.North > right.South
}

func ringArea(ring Ring) float64 {
	var area float64
	for index := 0; index+1 < len(ring); index++ {
		area += ring[index].Longitude*ring[index+1].Latitude - ring[index+1].Longitude*ring[index].Latitude
	}
	return area / 2
}

type budget struct {
	remaining int
}

func newBudget(limit int) *budget {
	return &budget{remaining: limit}
}

func (budget *budget) spend() error {
	if budget == nil {
		return nil
	}
	if budget.remaining <= 0 {
		return ErrGeometryTooComplex
	}
	budget.remaining--
	return nil
}

func validateSimpleRing(ring Ring, budget *budget, invalidErr error) error {
	edges := len(ring) - 1
	for left := 0; left < edges; left++ {
		for right := left + 1; right < edges; right++ {
			if right == left+1 || (left == 0 && right == edges-1) {
				continue
			}
			if err := budget.spend(); err != nil {
				return err
			}
			if segmentsIntersect(ring[left], ring[left+1], ring[right], ring[right+1]) {
				return fmt.Errorf("%w: ring self-intersects", invalidErr)
			}
		}
	}
	return nil
}

func ringsIntersect(left Ring, right Ring, budget *budget) (bool, error) {
	for leftIndex := 0; leftIndex+1 < len(left); leftIndex++ {
		for rightIndex := 0; rightIndex+1 < len(right); rightIndex++ {
			if err := budget.spend(); err != nil {
				return false, err
			}
			if segmentsIntersect(left[leftIndex], left[leftIndex+1], right[rightIndex], right[rightIndex+1]) {
				return true, nil
			}
		}
	}
	return false, nil
}

func polygonOverlapsCAP(feature Polygon, cap Ring, budget *budget) (bool, error) {
	outerOverlaps, err := ringsOverlapArea(feature.Exterior, cap, budget)
	if err != nil || !outerOverlaps {
		return false, err
	}
	if len(feature.Holes) == 0 {
		return true, nil
	}
	for index := 0; index+1 < len(cap); index++ {
		inside, err := pointStrictlyInsidePolygon(cap[index], feature, budget)
		if err != nil {
			return false, err
		}
		if inside {
			return true, nil
		}
	}
	for index := 0; index+1 < len(feature.Exterior); index++ {
		inside, err := pointStrictlyInsideRing(feature.Exterior[index], cap, budget)
		if err != nil {
			return false, err
		}
		if inside {
			return true, nil
		}
	}
	// There is an exterior overlap, but all available witnesses are inside a
	// hole or on a boundary. Fail closed rather than selecting a false grid.
	return false, nil
}

func ringsOverlapArea(left Ring, right Ring, budget *budget) (bool, error) {
	leftBounds, leftOK := planarRingBounds(left)
	rightBounds, rightOK := planarRingBounds(right)
	if !leftOK || !rightOK {
		return false, fmt.Errorf("%w: polygon intersection received invalid coordinates", ErrInvalidGeometry)
	}
	if !boundsOverlapPositive(leftBounds, rightBounds) {
		return false, nil
	}
	for leftIndex := 0; leftIndex+1 < len(left); leftIndex++ {
		for rightIndex := 0; rightIndex+1 < len(right); rightIndex++ {
			if err := budget.spend(); err != nil {
				return false, err
			}
			if properSegmentsIntersect(left[leftIndex], left[leftIndex+1], right[rightIndex], right[rightIndex+1]) {
				return true, nil
			}
		}
	}
	for index := 0; index+1 < len(left); index++ {
		inside, err := pointStrictlyInsideRing(left[index], right, budget)
		if err != nil {
			return false, err
		}
		if inside {
			return true, nil
		}
	}
	for index := 0; index+1 < len(right); index++ {
		inside, err := pointStrictlyInsideRing(right[index], left, budget)
		if err != nil {
			return false, err
		}
		if inside {
			return true, nil
		}
	}
	return false, nil
}

func pointStrictlyInsidePolygon(point Point, polygon Polygon, budget *budget) (bool, error) {
	inside, err := pointStrictlyInsideRing(point, polygon.Exterior, budget)
	if err != nil || !inside {
		return false, err
	}
	for _, hole := range polygon.Holes {
		insideHole, err := pointInOrOnRing(point, hole, budget)
		if err != nil {
			return false, err
		}
		if insideHole {
			return false, nil
		}
	}
	return true, nil
}

func pointStrictlyInsideRing(point Point, ring Ring, budget *budget) (bool, error) {
	if len(ring) < 4 {
		return false, fmt.Errorf("%w: point-in-ring received an invalid ring", ErrInvalidGeometry)
	}
	inside := false
	for current, previous := 0, len(ring)-1; current < len(ring); previous, current = current, current+1 {
		if err := budget.spend(); err != nil {
			return false, err
		}
		left := ring[previous]
		right := ring[current]
		if pointOnSegment(point, left, right) {
			return false, nil
		}
		intersects := (left.Latitude > point.Latitude) != (right.Latitude > point.Latitude) &&
			point.Longitude < (right.Longitude-left.Longitude)*(point.Latitude-left.Latitude)/(right.Latitude-left.Latitude)+left.Longitude
		if intersects {
			inside = !inside
		}
	}
	return inside, nil
}

func pointInOrOnRing(point Point, ring Ring, budget *budget) (bool, error) {
	if len(ring) < 4 {
		return false, fmt.Errorf("%w: point-in-ring received an invalid ring", ErrInvalidGeometry)
	}
	inside := false
	for current, previous := 0, len(ring)-1; current < len(ring); previous, current = current, current+1 {
		if err := budget.spend(); err != nil {
			return false, err
		}
		left := ring[previous]
		right := ring[current]
		if pointOnSegment(point, left, right) {
			return true, nil
		}
		intersects := (left.Latitude > point.Latitude) != (right.Latitude > point.Latitude) &&
			point.Longitude < (right.Longitude-left.Longitude)*(point.Latitude-left.Latitude)/(right.Latitude-left.Latitude)+left.Longitude
		if intersects {
			inside = !inside
		}
	}
	return inside, nil
}

func orientation(first Point, second Point, third Point) float64 {
	return (second.Longitude-first.Longitude)*(third.Latitude-first.Latitude) - (second.Latitude-first.Latitude)*(third.Longitude-first.Longitude)
}

func pointOnSegment(point Point, first Point, second Point) bool {
	if math.Abs(orientation(first, second, point)) > 1e-12 {
		return false
	}
	return point.Longitude >= math.Min(first.Longitude, second.Longitude)-1e-12 &&
		point.Longitude <= math.Max(first.Longitude, second.Longitude)+1e-12 &&
		point.Latitude >= math.Min(first.Latitude, second.Latitude)-1e-12 &&
		point.Latitude <= math.Max(first.Latitude, second.Latitude)+1e-12
}

func segmentsIntersect(firstStart Point, firstEnd Point, secondStart Point, secondEnd Point) bool {
	first := orientation(firstStart, firstEnd, secondStart)
	second := orientation(firstStart, firstEnd, secondEnd)
	third := orientation(secondStart, secondEnd, firstStart)
	fourth := orientation(secondStart, secondEnd, firstEnd)
	if oppositeSigns(first, second) && oppositeSigns(third, fourth) {
		return true
	}
	return (math.Abs(first) <= 1e-12 && pointOnSegment(secondStart, firstStart, firstEnd)) ||
		(math.Abs(second) <= 1e-12 && pointOnSegment(secondEnd, firstStart, firstEnd)) ||
		(math.Abs(third) <= 1e-12 && pointOnSegment(firstStart, secondStart, secondEnd)) ||
		(math.Abs(fourth) <= 1e-12 && pointOnSegment(firstEnd, secondStart, secondEnd))
}

func properSegmentsIntersect(firstStart Point, firstEnd Point, secondStart Point, secondEnd Point) bool {
	return oppositeSigns(orientation(firstStart, firstEnd, secondStart), orientation(firstStart, firstEnd, secondEnd)) &&
		oppositeSigns(orientation(secondStart, secondEnd, firstStart), orientation(secondStart, secondEnd, firstEnd))
}

func oppositeSigns(left float64, right float64) bool {
	return (left > 1e-12 && right < -1e-12) || (left < -1e-12 && right > 1e-12)
}

func clipRingToBounds(ring Ring, bounds Bounds) Ring {
	if len(ring) < 4 {
		return nil
	}
	points := append([]Point(nil), ring[:len(ring)-1]...)
	clip := func(input []Point, inside func(Point) bool, intersection func(Point, Point) Point) []Point {
		if len(input) == 0 {
			return nil
		}
		output := make([]Point, 0, len(input)+1)
		previous := input[len(input)-1]
		previousInside := inside(previous)
		for _, current := range input {
			currentInside := inside(current)
			if currentInside != previousInside {
				output = append(output, intersection(previous, current))
			}
			if currentInside {
				output = append(output, current)
			}
			previous = current
			previousInside = currentInside
		}
		return output
	}
	points = clip(points, func(point Point) bool { return point.Longitude >= bounds.West }, func(first Point, second Point) Point {
		return intersectAtLongitude(first, second, bounds.West)
	})
	points = clip(points, func(point Point) bool { return point.Longitude <= bounds.East }, func(first Point, second Point) Point {
		return intersectAtLongitude(first, second, bounds.East)
	})
	points = clip(points, func(point Point) bool { return point.Latitude >= bounds.South }, func(first Point, second Point) Point {
		return intersectAtLatitude(first, second, bounds.South)
	})
	points = clip(points, func(point Point) bool { return point.Latitude <= bounds.North }, func(first Point, second Point) Point {
		return intersectAtLatitude(first, second, bounds.North)
	})
	if len(points) < 3 {
		return nil
	}
	if !samePoint(points[0], points[len(points)-1]) {
		points = append(points, points[0])
	}
	return Ring(points)
}

func intersectAtLongitude(first Point, second Point, longitude float64) Point {
	if first.Longitude == second.Longitude {
		return Point{Latitude: first.Latitude, Longitude: longitude}
	}
	ratio := (longitude - first.Longitude) / (second.Longitude - first.Longitude)
	return Point{Latitude: first.Latitude + ratio*(second.Latitude-first.Latitude), Longitude: longitude}
}

func intersectAtLatitude(first Point, second Point, latitude float64) Point {
	if first.Latitude == second.Latitude {
		return Point{Latitude: latitude, Longitude: first.Longitude}
	}
	ratio := (latitude - first.Latitude) / (second.Latitude - first.Latitude)
	return Point{Latitude: latitude, Longitude: first.Longitude + ratio*(second.Longitude-first.Longitude)}
}

const earthRadiusMeters = 6_371_008.8

// laeaProjection is a local Lambert azimuthal equal-area projection. It keeps
// the grid physically meaningful at high latitude and for wide Canadian
// features, unlike direct longitude and latitude subdivision.
type laeaProjection struct {
	latitude0    float64
	longitude0   float64
	sinLatitude0 float64
	cosLatitude0 float64
}

func newLAEAProjection(bounds Bounds) (laeaProjection, error) {
	if err := validBounds(bounds); err != nil {
		return laeaProjection{}, err
	}
	latitude := ((bounds.South + bounds.North) / 2) * math.Pi / 180
	longitude := ((bounds.West + bounds.East) / 2) * math.Pi / 180
	projection := laeaProjection{
		latitude0:    latitude,
		longitude0:   longitude,
		sinLatitude0: math.Sin(latitude),
		cosLatitude0: math.Cos(latitude),
	}
	if !projection.valid() {
		return laeaProjection{}, fmt.Errorf("%w: local projection is invalid", ErrInvalidGeometry)
	}
	return projection, nil
}

func (projection laeaProjection) valid() bool {
	return finiteCoordinate(projection.latitude0) && finiteCoordinate(projection.longitude0) &&
		finiteCoordinate(projection.sinLatitude0) && finiteCoordinate(projection.cosLatitude0) &&
		projection.latitude0 >= -math.Pi/2 && projection.latitude0 <= math.Pi/2 &&
		projection.longitude0 >= -math.Pi && projection.longitude0 <= math.Pi
}

func (projection laeaProjection) project(point Point) (Point, error) {
	if !projection.valid() || !validPoint(point) {
		return Point{}, fmt.Errorf("%w: cannot project invalid WGS84 point", ErrInvalidGeometry)
	}
	latitude := point.Latitude * math.Pi / 180
	longitude := point.Longitude * math.Pi / 180
	deltaLongitude := normalizeRadians(longitude - projection.longitude0)
	sineLatitude := math.Sin(latitude)
	cosineLatitude := math.Cos(latitude)
	denominator := 1 + projection.sinLatitude0*sineLatitude + projection.cosLatitude0*cosineLatitude*math.Cos(deltaLongitude)
	if denominator <= 1e-15 {
		return Point{}, fmt.Errorf("%w: geometry is outside the local projection", ErrInvalidGeometry)
	}
	factor := math.Sqrt(2 / denominator)
	x := earthRadiusMeters * factor * cosineLatitude * math.Sin(deltaLongitude)
	y := earthRadiusMeters * factor * (projection.cosLatitude0*sineLatitude - projection.sinLatitude0*cosineLatitude*math.Cos(deltaLongitude))
	if !finiteCoordinate(x) || !finiteCoordinate(y) {
		return Point{}, fmt.Errorf("%w: projected coordinate is non-finite", ErrInvalidGeometry)
	}
	return Point{Latitude: y, Longitude: x}, nil
}

func (projection laeaProjection) unproject(point Point) (Point, error) {
	if !projection.valid() || !finiteCoordinate(point.Longitude) || !finiteCoordinate(point.Latitude) {
		return Point{}, fmt.Errorf("%w: cannot unproject invalid point", ErrInvalidGeometry)
	}
	radius := math.Hypot(point.Longitude, point.Latitude)
	if radius == 0 {
		return Point{Latitude: projection.latitude0 * 180 / math.Pi, Longitude: projection.longitude0 * 180 / math.Pi}, nil
	}
	if radius > 2*earthRadiusMeters+1e-6 {
		return Point{}, fmt.Errorf("%w: projected point is outside the earth disk", ErrInvalidGeometry)
	}
	centralAngle := 2 * math.Asin(math.Min(1, radius/(2*earthRadiusMeters)))
	sineAngle := math.Sin(centralAngle)
	cosineAngle := math.Cos(centralAngle)
	latitude := math.Asin(cosineAngle*projection.sinLatitude0 + point.Latitude*sineAngle*projection.cosLatitude0/radius)
	longitude := projection.longitude0 + math.Atan2(point.Longitude*sineAngle, radius*projection.cosLatitude0*cosineAngle-point.Latitude*projection.sinLatitude0*sineAngle)
	result := Point{Latitude: latitude * 180 / math.Pi, Longitude: normalizeDegrees(longitude * 180 / math.Pi)}
	if !validPoint(result) {
		return Point{}, fmt.Errorf("%w: inverse projection is outside WGS84", ErrInvalidGeometry)
	}
	return result, nil
}

func (projection laeaProjection) boundsForProjectedCell(cell Bounds) (Bounds, error) {
	if cell.West >= cell.East || cell.South >= cell.North {
		return Bounds{}, fmt.Errorf("%w: projected grid cell has no area", ErrInvalidGeometry)
	}
	points := []Point{
		{Latitude: cell.South, Longitude: cell.West},
		{Latitude: cell.South, Longitude: cell.East},
		{Latitude: cell.North, Longitude: cell.East},
		{Latitude: cell.North, Longitude: cell.West},
		{Latitude: (cell.South + cell.North) / 2, Longitude: cell.West},
		{Latitude: (cell.South + cell.North) / 2, Longitude: cell.East},
		{Latitude: cell.South, Longitude: (cell.West + cell.East) / 2},
		{Latitude: cell.North, Longitude: (cell.West + cell.East) / 2},
	}
	var bounds Bounds
	for index, point := range points {
		geographic, err := projection.unproject(point)
		if err != nil {
			return Bounds{}, err
		}
		if index == 0 {
			bounds = Bounds{West: geographic.Longitude, East: geographic.Longitude, South: geographic.Latitude, North: geographic.Latitude}
			continue
		}
		bounds = mergeBounds(bounds, Bounds{West: geographic.Longitude, East: geographic.Longitude, South: geographic.Latitude, North: geographic.Latitude})
	}
	if err := validBounds(bounds); err != nil {
		return Bounds{}, err
	}
	return bounds, nil
}

func projectGeometry(polygons []Polygon, projection laeaProjection) ([]Polygon, []Bounds, Bounds, error) {
	projected := make([]Polygon, 0, len(polygons))
	featureBounds := make([]Bounds, 0, len(polygons))
	var combined Bounds
	for index, polygon := range polygons {
		copyPolygon, err := projectPolygon(polygon, projection)
		if err != nil {
			return nil, nil, Bounds{}, err
		}
		bounds, ok := planarRingBounds(copyPolygon.Exterior)
		if !ok {
			return nil, nil, Bounds{}, fmt.Errorf("%w: projected feature polygon is invalid", ErrInvalidGeometry)
		}
		if index == 0 {
			combined = bounds
		} else {
			combined = mergeBounds(combined, bounds)
		}
		projected = append(projected, copyPolygon)
		featureBounds = append(featureBounds, bounds)
	}
	if combined.West >= combined.East || combined.South >= combined.North {
		return nil, nil, Bounds{}, fmt.Errorf("%w: projected feature has no area", ErrInvalidGeometry)
	}
	return projected, featureBounds, combined, nil
}

func projectPolygon(polygon Polygon, projection laeaProjection) (Polygon, error) {
	exterior, err := projectRing(polygon.Exterior, projection)
	if err != nil {
		return Polygon{}, err
	}
	result := Polygon{Exterior: exterior, Holes: make([]Ring, 0, len(polygon.Holes))}
	for _, hole := range polygon.Holes {
		copyHole, err := projectRing(hole, projection)
		if err != nil {
			return Polygon{}, err
		}
		result.Holes = append(result.Holes, copyHole)
	}
	return result, nil
}

func projectRing(ring Ring, projection laeaProjection) (Ring, error) {
	result := make(Ring, 0, len(ring))
	for _, point := range ring {
		projected, err := projection.project(point)
		if err != nil {
			return nil, err
		}
		result = append(result, projected)
	}
	return result, nil
}

func normalizeRadians(value float64) float64 {
	for value > math.Pi {
		value -= 2 * math.Pi
	}
	for value < -math.Pi {
		value += 2 * math.Pi
	}
	return value
}

func normalizeDegrees(value float64) float64 {
	for value > 180 {
		value -= 360
	}
	for value < -180 {
		value += 360
	}
	return value
}

type wkbReader struct {
	data     []byte
	offset   int
	polygons int
	rings    int
	vertices int
}

func (reader *wkbReader) geometry(depth int) (Geometry, error) {
	if reader == nil || depth > 1 {
		return Geometry{}, fmt.Errorf("%w: WKB nesting is invalid", ErrInvalidGeometry)
	}
	order, err := reader.byteOrder()
	if err != nil {
		return Geometry{}, err
	}
	geometryType, err := reader.uint32(order)
	if err != nil {
		return Geometry{}, err
	}
	switch geometryType {
	case 3:
		polygon, err := reader.polygon(order)
		if err != nil {
			return Geometry{}, err
		}
		return Geometry{Polygons: []Polygon{polygon}}, nil
	case 6:
		count, err := reader.uint32(order)
		if err != nil {
			return Geometry{}, err
		}
		if count == 0 || count > maxGeometryPolygons {
			return Geometry{}, fmt.Errorf("%w: WKB multipolygon count is invalid", ErrGeometryTooLarge)
		}
		geometry := Geometry{Polygons: make([]Polygon, 0, count)}
		for index := uint32(0); index < count; index++ {
			child, err := reader.geometry(depth + 1)
			if err != nil {
				return Geometry{}, err
			}
			if len(child.Polygons) != 1 {
				return Geometry{}, fmt.Errorf("%w: WKB multipolygon contains a non-polygon member", ErrInvalidGeometry)
			}
			geometry.Polygons = append(geometry.Polygons, child.Polygons[0])
		}
		return geometry, nil
	default:
		return Geometry{}, fmt.Errorf("%w: unsupported WKB geometry type %d", ErrInvalidGeometry, geometryType)
	}
}

func (reader *wkbReader) polygon(order binary.ByteOrder) (Polygon, error) {
	count, err := reader.uint32(order)
	if err != nil {
		return Polygon{}, err
	}
	if count == 0 || count > maxGeometryRings || int(count)+reader.rings > maxGeometryRings {
		return Polygon{}, fmt.Errorf("%w: WKB ring count is invalid", ErrGeometryTooLarge)
	}
	reader.polygons++
	if reader.polygons > maxGeometryPolygons {
		return Polygon{}, fmt.Errorf("%w: WKB polygon count is invalid", ErrGeometryTooLarge)
	}
	polygon := Polygon{Holes: make([]Ring, 0, count-1)}
	for index := uint32(0); index < count; index++ {
		pointCount, err := reader.uint32(order)
		if err != nil {
			return Polygon{}, err
		}
		if pointCount < 4 || pointCount-1 > maxRingVertices || int(pointCount-1)+reader.vertices > maxGeometryVertices {
			return Polygon{}, fmt.Errorf("%w: WKB ring point count is invalid", ErrGeometryTooLarge)
		}
		ring := make(Ring, 0, pointCount)
		for pointIndex := uint32(0); pointIndex < pointCount; pointIndex++ {
			longitude, err := reader.float64(order)
			if err != nil {
				return Polygon{}, err
			}
			latitude, err := reader.float64(order)
			if err != nil {
				return Polygon{}, err
			}
			ring = append(ring, Point{Latitude: latitude, Longitude: longitude})
		}
		reader.rings++
		reader.vertices += len(ring) - 1
		if index == 0 {
			polygon.Exterior = ring
		} else {
			polygon.Holes = append(polygon.Holes, ring)
		}
	}
	return polygon, nil
}

func (reader *wkbReader) byteOrder() (binary.ByteOrder, error) {
	if reader == nil || reader.offset >= len(reader.data) {
		return nil, fmt.Errorf("%w: WKB is truncated", ErrInvalidGeometry)
	}
	value := reader.data[reader.offset]
	reader.offset++
	switch value {
	case 0:
		return binary.BigEndian, nil
	case 1:
		return binary.LittleEndian, nil
	default:
		return nil, fmt.Errorf("%w: WKB byte order is invalid", ErrInvalidGeometry)
	}
}

func (reader *wkbReader) uint32(order binary.ByteOrder) (uint32, error) {
	if reader == nil || reader.offset+4 > len(reader.data) {
		return 0, fmt.Errorf("%w: WKB is truncated", ErrInvalidGeometry)
	}
	value := order.Uint32(reader.data[reader.offset : reader.offset+4])
	reader.offset += 4
	return value, nil
}

func (reader *wkbReader) float64(order binary.ByteOrder) (float64, error) {
	if reader == nil || reader.offset+8 > len(reader.data) {
		return 0, fmt.Errorf("%w: WKB is truncated", ErrInvalidGeometry)
	}
	value := math.Float64frombits(order.Uint64(reader.data[reader.offset : reader.offset+8]))
	reader.offset += 8
	return value, nil
}
