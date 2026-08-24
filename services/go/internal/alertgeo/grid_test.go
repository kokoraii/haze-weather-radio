package alertgeo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestGridPartitionsFollowNWSPartialLocationOrder(t *testing.T) {
	grid, err := NewGridForBounds("065435", Bounds{West: -107, South: 52, East: -106, North: 53})
	if err != nil {
		t.Fatalf("NewGridForBounds() error = %v", err)
	}
	parts := grid.Partitions()
	if len(parts) != 9 {
		t.Fatalf("partition count = %d, want 9", len(parts))
	}
	want := []string{"165435", "265435", "365435", "465435", "565435", "665435", "765435", "865435", "965435"}
	for index, part := range parts {
		if part.Number != index+1 || part.Code != want[index] {
			t.Fatalf("partition %d = %#v, want number %d code %q", index, part, index+1, want[index])
		}
		if !part.Bounds.Valid() {
			t.Fatalf("partition %d has invalid geographic envelope: %#v", part.Number, part.Bounds)
		}
	}
	if got := parts[0].Identifier; got != "065435-165435" {
		t.Fatalf("northwest identifier = %q, want 065435-165435", got)
	}
	if whole := grid.WholePartition(); whole.Number != 0 || whole.Code != "065435" || whole.Identifier != "065435" {
		t.Fatalf("whole partition = %#v", whole)
	}
}

func TestGridMatchesOnlyTheOverlappedPartialCode(t *testing.T) {
	grid := testSquareGrid(t)
	raw := capSquareInsidePartition(t, grid, 1, 0.18)
	parts, err := grid.MatchCAPPolygons([]string{raw})
	if err != nil {
		t.Fatalf("MatchCAPPolygons() error = %v", err)
	}
	if len(parts) != 1 || parts[0].Number != 1 || parts[0].Code != "165435" {
		t.Fatalf("matched partitions = %#v, want only northwest 165435", parts)
	}
}

func TestGridCollapsesAllNineToWholeParent(t *testing.T) {
	grid := testSquareGrid(t)
	minimum, maximum := projectedGridExtent(grid)
	raw := capSquareForProjectedBounds(t, grid, Bounds{
		West:  minimum.Longitude - 2_000,
		South: minimum.Latitude - 2_000,
		East:  maximum.Longitude + 2_000,
		North: maximum.Latitude + 2_000,
	})
	parts, err := grid.MatchCAPPolygons([]string{raw})
	if err != nil {
		t.Fatalf("MatchCAPPolygons() error = %v", err)
	}
	if len(parts) != 1 || parts[0].Number != 0 || parts[0].Code != "065435" {
		t.Fatalf("matched partitions = %#v, want whole parent only", parts)
	}
}

func TestGridUsesActualFeatureShapeRatherThanOnlyItsBoundingBox(t *testing.T) {
	geometry := Geometry{Polygons: []Polygon{{Exterior: Ring{
		{Latitude: 52.0, Longitude: -107.0},
		{Latitude: 52.0, Longitude: -106.6},
		{Latitude: 52.6, Longitude: -106.6},
		{Latitude: 52.6, Longitude: -106.0},
		{Latitude: 53.0, Longitude: -106.0},
		{Latitude: 53.0, Longitude: -107.0},
		{Latitude: 52.0, Longitude: -107.0},
	}}}}
	grid, err := NewGrid("065435", geometry)
	if err != nil {
		t.Fatalf("NewGrid() error = %v", err)
	}
	parts, err := grid.MatchCAPPolygons([]string{capSquareInsidePartition(t, grid, 9, 0.16)})
	if err != nil {
		t.Fatalf("MatchCAPPolygons() error = %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("bbox-only false positive = %#v", parts)
	}
}

func TestGridHonoursFeatureHoles(t *testing.T) {
	geometry := Geometry{Polygons: []Polygon{{
		Exterior: rectangleRing(52.0, -107.0, 53.0, -106.0),
		Holes:    []Ring{rectangleRing(52.35, -106.65, 52.65, -106.35)},
	}}}
	grid, err := NewGrid("065435", geometry)
	if err != nil {
		t.Fatalf("NewGrid() error = %v", err)
	}
	parts, err := grid.MatchCAPPolygons([]string{capSquareInsidePartition(t, grid, 5, 0.10)})
	if err != nil {
		t.Fatalf("MatchCAPPolygons() error = %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("hole was selected as alert coverage: %#v", parts)
	}
}

func TestGridUsesLocalEqualAreaProjectionAtHighLatitude(t *testing.T) {
	grid, err := NewGridForBounds("065435", Bounds{West: -130, South: 82.0, East: -110, North: 83.7})
	if err != nil {
		t.Fatalf("NewGridForBounds() error = %v", err)
	}
	first := grid.projectedPartitions[0]
	wantArea := (first.East - first.West) * (first.North - first.South)
	if wantArea <= 0 {
		t.Fatalf("first projected cell area = %f", wantArea)
	}
	for index, cell := range grid.projectedPartitions {
		area := (cell.East - cell.West) * (cell.North - cell.South)
		if math.Abs(area-wantArea) > math.Max(1, wantArea)*1e-12 {
			t.Fatalf("projected cell %d area = %.9f, want %.9f", index+1, area, wantArea)
		}
	}
	parts, err := grid.MatchCAPPolygons([]string{capSquareInsidePartition(t, grid, 1, 0.15)})
	if err != nil {
		t.Fatalf("high-latitude MatchCAPPolygons() error = %v", err)
	}
	if len(parts) != 1 || parts[0].Number != 1 {
		t.Fatalf("high-latitude match = %#v, want northwest only", parts)
	}
}

func TestGridFromWKBSupportsLargeBoundedFeature(t *testing.T) {
	const pointCount = 5_270
	ring := make(Ring, 0, pointCount)
	for index := 0; index < pointCount-1; index++ {
		angle := 2 * math.Pi * float64(index) / float64(pointCount-1)
		ring = append(ring, Point{Latitude: 52.5 + 0.2*math.Sin(angle), Longitude: -106.5 + 0.25*math.Cos(angle)})
	}
	ring = append(ring, ring[0])
	grid, err := GridFromWKB("065435", encodePolygonWKB(ring))
	if err != nil {
		t.Fatalf("GridFromWKB() rejected a 5,270-point feature: %v", err)
	}
	if len(grid.Partitions()) != 9 {
		t.Fatalf("partition count = %d, want 9", len(grid.Partitions()))
	}
}

func TestGridFromWKBRejectsMalformedAndOversizedInput(t *testing.T) {
	if _, err := GridFromWKB("065435", []byte{1, 3}); !errors.Is(err, ErrInvalidGeometry) {
		t.Fatalf("truncated WKB error = %v, want ErrInvalidGeometry", err)
	}
	data := make([]byte, maxWKBBytes+1)
	if _, err := GridFromWKB("065435", data); !errors.Is(err, ErrGeometryTooLarge) {
		t.Fatalf("oversized WKB error = %v, want ErrGeometryTooLarge", err)
	}
}

func TestCAPParseAndMatchFailClosed(t *testing.T) {
	grid := testSquareGrid(t)
	good := capSquareInsidePartition(t, grid, 1, 0.15)
	parts, err := grid.MatchCAPPolygons([]string{good, "not-a-polygon"})
	if !errors.Is(err, ErrInvalidCAPPolygon) || parts != nil {
		t.Fatalf("mixed valid and malformed CAP result = (%#v, %v), want (nil, ErrInvalidCAPPolygon)", parts, err)
	}
	polygons := make([]string, maxCAPPolygons+1)
	for index := range polygons {
		polygons[index] = good
	}
	parts, err = grid.MatchCAPPolygons(polygons)
	if !errors.Is(err, ErrCAPPolygonTooLarge) || parts != nil {
		t.Fatalf("too-many CAP polygons result = (%#v, %v), want (nil, ErrCAPPolygonTooLarge)", parts, err)
	}
}

func TestTangentPolygonsHaveNoPositiveAreaOverlap(t *testing.T) {
	left := rectangleRing(0, 0, 1, 1)
	right := rectangleRing(0, 1, 1, 2)
	overlaps, err := ringsOverlapArea(left, right, newBudget(maxIntersectionChecks))
	if err != nil {
		t.Fatalf("ringsOverlapArea() error = %v", err)
	}
	if overlaps {
		t.Fatal("tangent rings were treated as positive-area overlap")
	}
}

func TestInvalidBaseCodeAndSelfIntersectingCAPAreRejected(t *testing.T) {
	if _, err := PartitionCode("65435", 1); !errors.Is(err, ErrInvalidBaseCode) {
		t.Fatalf("short parent code error = %v, want ErrInvalidBaseCode", err)
	}
	if _, err := ParseCAPPolygon("52,-107 53,-106 52,-106 53,-107"); !errors.Is(err, ErrInvalidCAPPolygon) {
		t.Fatalf("bow-tie CAP error = %v, want ErrInvalidCAPPolygon", err)
	}
}

func testSquareGrid(t *testing.T) Grid {
	t.Helper()
	grid, err := NewGridForBounds("065435", Bounds{West: -107, South: 52, East: -106, North: 53})
	if err != nil {
		t.Fatalf("NewGridForBounds() error = %v", err)
	}
	return grid
}

func rectangleRing(south float64, west float64, north float64, east float64) Ring {
	return Ring{
		{Latitude: south, Longitude: west},
		{Latitude: south, Longitude: east},
		{Latitude: north, Longitude: east},
		{Latitude: north, Longitude: west},
		{Latitude: south, Longitude: west},
	}
}

func capSquareInsidePartition(t *testing.T, grid Grid, number int, scale float64) string {
	t.Helper()
	cell := grid.projectedPartitions[number-1]
	centre := Point{Latitude: (cell.South + cell.North) / 2, Longitude: (cell.West + cell.East) / 2}
	halfWidth := (cell.East - cell.West) * scale / 2
	halfHeight := (cell.North - cell.South) * scale / 2
	return capSquareForProjectedBounds(t, grid, Bounds{
		West:  centre.Longitude - halfWidth,
		South: centre.Latitude - halfHeight,
		East:  centre.Longitude + halfWidth,
		North: centre.Latitude + halfHeight,
	})
}

func capSquareForProjectedBounds(t *testing.T, grid Grid, bounds Bounds) string {
	t.Helper()
	points := []Point{
		{Latitude: bounds.South, Longitude: bounds.West},
		{Latitude: bounds.South, Longitude: bounds.East},
		{Latitude: bounds.North, Longitude: bounds.East},
		{Latitude: bounds.North, Longitude: bounds.West},
		{Latitude: bounds.South, Longitude: bounds.West},
	}
	values := make([]string, 0, len(points))
	for _, point := range points {
		geographic, err := grid.projection.unproject(point)
		if err != nil {
			t.Fatalf("unproject(%#v) error = %v", point, err)
		}
		values = append(values, fmt.Sprintf("%.12f,%.12f", geographic.Latitude, geographic.Longitude))
	}
	return strings.Join(values, " ")
}

func projectedGridExtent(grid Grid) (Point, Point) {
	minimum := Point{Latitude: math.Inf(1), Longitude: math.Inf(1)}
	maximum := Point{Latitude: math.Inf(-1), Longitude: math.Inf(-1)}
	for _, cell := range grid.projectedPartitions {
		minimum.Latitude = math.Min(minimum.Latitude, cell.South)
		minimum.Longitude = math.Min(minimum.Longitude, cell.West)
		maximum.Latitude = math.Max(maximum.Latitude, cell.North)
		maximum.Longitude = math.Max(maximum.Longitude, cell.East)
	}
	return minimum, maximum
}

func encodePolygonWKB(ring Ring) []byte {
	data := []byte{1}
	var uint32Data [4]byte
	binary.LittleEndian.PutUint32(uint32Data[:], 3)
	data = append(data, uint32Data[:]...)
	binary.LittleEndian.PutUint32(uint32Data[:], 1)
	data = append(data, uint32Data[:]...)
	binary.LittleEndian.PutUint32(uint32Data[:], uint32(len(ring)))
	data = append(data, uint32Data[:]...)
	for _, point := range ring {
		var floatData [8]byte
		binary.LittleEndian.PutUint64(floatData[:], math.Float64bits(point.Longitude))
		data = append(data, floatData[:]...)
		binary.LittleEndian.PutUint64(floatData[:], math.Float64bits(point.Latitude))
		data = append(data, floatData[:]...)
	}
	return data
}
