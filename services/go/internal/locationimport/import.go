// Package locationimport validates and imports managed alert-area geometry packs.
package locationimport

import (
	"archive/zip"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	shp "github.com/jonas-p/go-shp"
	_ "modernc.org/sqlite"
)

const (
	MaxArchiveEntries          = 512
	MaxArchiveUncompressedSize = int64(512 << 20)
	MaxArchiveEntrySize        = int64(128 << 20)
	MinimumECCCRecordCount     = 1000
	MaximumECCCRecordCount     = 2500
	MinimumNWSRecordCount      = 3000
	MaximumNWSRecordCount      = 4000
)

var (
	sixDigitCode  = regexp.MustCompile(`^[0-9]{6}$`)
	fiveDigitFIPS = regexp.MustCompile(`^[0-9]{5}$`)
	versionRE     = regexp.MustCompile(`(?i)(?:version|v)[_-]?(\d+)[_.-](\d+)[_.-](\d+)`)
	nwsVersionRE  = regexp.MustCompile(`(?i)^c_(\d{2})([a-z]{2,3})(\d{2})\.zip$`)
)

// Result summarizes an imported provider generation without exposing local paths.
type Result struct {
	Source          string `json:"source"`
	ProviderVersion string `json:"provider_version"`
	RecordCount     int    `json:"record_count"`
	ImportedAt      string `json:"imported_at"`
}

// SplitResult summarizes a non-destructive legacy database split.
type SplitResult struct {
	GeometryCount   int            `json:"geometry_count"`
	SourceCounts    map[string]int `json:"source_counts"`
	CorePlacesAdded int            `json:"core_places_added"`
}

type areaRecord struct {
	Source          string
	Code            string
	SameCode        string
	Name            string
	NameFR          string
	Region          string
	Country         string
	Latitude        float64
	Longitude       float64
	MinLon          float64
	MinLat          float64
	MaxLon          float64
	MaxLat          float64
	GeometryType    string
	GeometryWKB     []byte
	ProviderVersion string
	SourceURL       string
	SourceID        string
	UpdatedAt       string
}

type multiPolygon [][][][]float64

// CloneDatabase produces a transactionally consistent candidate from an existing SQLite database.
func CloneDatabase(ctx context.Context, source string, destination string) error {
	source = filepath.Clean(source)
	destination = filepath.Clean(destination)
	if source == destination {
		return errors.New("source and destination databases must be different")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	if err := os.Remove(destination); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	db, err := sql.Open("sqlite", source)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	quotedDestination := strings.ReplaceAll(destination, "'", "''")
	if _, err := db.ExecContext(ctx, "VACUUM INTO '"+quotedDestination+"'"); err != nil {
		_ = os.Remove(destination)
		return fmt.Errorf("clone location database: %w", err)
	}
	return nil
}

// ImportECCCArchive replaces only ECCC CLC geometry in a candidate database.
func ImportECCCArchive(ctx context.Context, databasePath string, archivePath string, originalName string, sourceURL string, now time.Time) (Result, error) {
	version, err := validateArchive(archivePath, originalName, true)
	if err != nil {
		return Result{}, err
	}
	records, err := readECCCRecords(archivePath, version, sourceURL)
	if err != nil {
		return Result{}, err
	}
	if len(records) < MinimumECCCRecordCount || len(records) > MaximumECCCRecordCount {
		return Result{}, fmt.Errorf("ECCC CLC record count %d is outside the expected range", len(records))
	}
	return applyRecords(ctx, databasePath, "clc", version, records, now)
}

// ImportNWSArchive replaces only NWS county geometry in a candidate database.
func ImportNWSArchive(ctx context.Context, databasePath string, archivePath string, originalName string, sourceURL string, now time.Time) (Result, error) {
	if _, err := validateArchive(archivePath, originalName, false); err != nil {
		return Result{}, err
	}
	version, err := nwsVersion(originalName)
	if err != nil {
		return Result{}, err
	}
	records, err := readNWSRecords(archivePath, version, sourceURL)
	if err != nil {
		return Result{}, err
	}
	if len(records) < MinimumNWSRecordCount || len(records) > MaximumNWSRecordCount {
		return Result{}, fmt.Errorf("NWS county record count %d is outside the expected range", len(records))
	}
	return applyRecords(ctx, databasePath, "nws_same", version, records, now)
}

func validateArchive(archivePath string, originalName string, requireECCCVersion bool) (string, error) {
	if !strings.EqualFold(filepath.Ext(strings.TrimSpace(originalName)), ".zip") {
		return "", errors.New("location catalog upload must be a ZIP archive")
	}
	file, err := os.Open(filepath.Clean(archivePath))
	if err != nil {
		return "", err
	}
	var signature [4]byte
	_, readErr := io.ReadFull(file, signature[:])
	_ = file.Close()
	if readErr != nil || string(signature[:]) != "PK\x03\x04" {
		return "", errors.New("location catalog upload is not a valid ZIP archive")
	}
	reader, err := zip.OpenReader(filepath.Clean(archivePath))
	if err != nil {
		return "", errors.New("location catalog ZIP could not be opened")
	}
	defer reader.Close()
	if len(reader.File) == 0 || len(reader.File) > MaxArchiveEntries {
		return "", fmt.Errorf("location catalog ZIP contains an invalid number of entries")
	}
	var total int64
	for _, entry := range reader.File {
		clean := path.Clean(strings.ReplaceAll(entry.Name, "\\", "/"))
		if clean == "." || strings.HasPrefix(clean, "/") || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(entry.Name, "\\") {
			return "", errors.New("location catalog ZIP contains an unsafe entry path")
		}
		if entry.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("location catalog ZIP contains a symbolic link")
		}
		if entry.FileInfo().IsDir() {
			continue
		}
		if entry.UncompressedSize64 > uint64(MaxArchiveEntrySize) {
			return "", errors.New("location catalog ZIP contains an oversized entry")
		}
		if entry.UncompressedSize64 > math.MaxInt64 || total > MaxArchiveUncompressedSize-int64(entry.UncompressedSize64) {
			return "", errors.New("location catalog ZIP expands beyond the allowed size")
		}
		total += int64(entry.UncompressedSize64)
	}
	if !requireECCCVersion {
		return "", nil
	}
	version := semanticVersion(originalName)
	if version == "" {
		return "", errors.New("ECCC archive filename must include its provider version")
	}
	return version, nil
}

func semanticVersion(value string) string {
	match := versionRE.FindStringSubmatch(filepath.Base(value))
	if len(match) != 4 {
		return ""
	}
	return strings.Join(match[1:], ".")
}

func nwsVersion(value string) (string, error) {
	match := nwsVersionRE.FindStringSubmatch(strings.ToLower(filepath.Base(value)))
	if len(match) != 4 {
		return "", errors.New("NWS county archive must use the official c_DDmmmYY.zip filename")
	}
	monthNames := map[string]time.Month{
		"ja": time.January, "jan": time.January, "fe": time.February, "feb": time.February,
		"mr": time.March, "mar": time.March, "ap": time.April, "apr": time.April,
		"my": time.May, "may": time.May, "jn": time.June, "jun": time.June,
		"jl": time.July, "jul": time.July, "au": time.August, "aug": time.August,
		"se": time.September, "sep": time.September, "oc": time.October, "oct": time.October,
		"no": time.November, "nov": time.November, "de": time.December, "dec": time.December,
	}
	month, ok := monthNames[strings.ToLower(match[2])]
	if !ok {
		return "", errors.New("NWS county archive filename contains an invalid month")
	}
	day, _ := strconv.Atoi(match[1])
	year, _ := strconv.Atoi(match[3])
	year += 2000
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if date.Day() != day {
		return "", errors.New("NWS county archive filename contains an invalid date")
	}
	return date.Format("2006-01-02"), nil
}

func readECCCRecords(archivePath string, version string, sourceURL string) ([]areaRecord, error) {
	shapeName, err := findShapeName(archivePath, func(name string) bool {
		return strings.EqualFold(filepath.Base(name), "land_CLCBaseZone_hybrid_unproj.shp")
	})
	if err != nil {
		return nil, errors.New("ECCC archive is missing land_CLCBaseZone_hybrid_unproj.shp")
	}
	reader, err := shp.OpenShapeFromZip(filepath.Clean(archivePath), shapeName)
	if err != nil {
		return nil, fmt.Errorf("open ECCC shapefile: %w", err)
	}
	defer reader.Close()
	fields := fieldIndexes(reader.Fields())
	for _, required := range []string{"CLC", "NAME", "NOM", "LAT_DD", "LON_DD", "PROVINCE_C", "COUNTRY_C"} {
		if _, ok := fields[required]; !ok {
			return nil, fmt.Errorf("ECCC shapefile is missing required field %s", required)
		}
	}
	byCode := map[string]*areaRecord{}
	geometries := map[string]multiPolygon{}
	for reader.Next() {
		_, rawShape := reader.Shape()
		polygon, ok := rawShape.(*shp.Polygon)
		if !ok {
			return nil, fmt.Errorf("ECCC shapefile contains unsupported geometry %T", rawShape)
		}
		code := normalizeSixDigit(reader.Attribute(fields["CLC"]))
		if !sixDigitCode.MatchString(code) {
			return nil, fmt.Errorf("ECCC shapefile contains invalid CLC code %q", code)
		}
		parts, bbox, err := shapePolygon(polygon)
		if err != nil {
			return nil, fmt.Errorf("ECCC CLC %s: %w", code, err)
		}
		latitude, err := parseCoordinate(reader.Attribute(fields["LAT_DD"]), -90, 90)
		if err != nil {
			return nil, fmt.Errorf("ECCC CLC %s latitude: %w", code, err)
		}
		longitude, err := parseCoordinate(reader.Attribute(fields["LON_DD"]), -180, 180)
		if err != nil {
			return nil, fmt.Errorf("ECCC CLC %s longitude: %w", code, err)
		}
		if existing := byCode[code]; existing != nil {
			geometries[code] = append(geometries[code], parts...)
			existing.MinLon = math.Min(existing.MinLon, bbox[0])
			existing.MinLat = math.Min(existing.MinLat, bbox[1])
			existing.MaxLon = math.Max(existing.MaxLon, bbox[2])
			existing.MaxLat = math.Max(existing.MaxLat, bbox[3])
			continue
		}
		byCode[code] = &areaRecord{
			Source: "clc", Code: code, SameCode: code,
			Name: strings.TrimSpace(reader.Attribute(fields["NAME"])), NameFR: strings.TrimSpace(reader.Attribute(fields["NOM"])),
			Region: strings.ToUpper(strings.TrimSpace(reader.Attribute(fields["PROVINCE_C"]))), Country: strings.ToUpper(strings.TrimSpace(reader.Attribute(fields["COUNTRY_C"]))),
			Latitude: latitude, Longitude: longitude, MinLon: bbox[0], MinLat: bbox[1], MaxLon: bbox[2], MaxLat: bbox[3],
			ProviderVersion: version, SourceURL: sourceURL,
		}
		geometries[code] = parts
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("read ECCC shapefile: %w", err)
	}
	return finalizeRecords(byCode, geometries)
}

func readNWSRecords(archivePath string, version string, sourceURL string) ([]areaRecord, error) {
	shapeName, err := findShapeName(archivePath, func(name string) bool {
		base := strings.ToLower(filepath.Base(name))
		return strings.HasPrefix(base, "c_") && strings.HasSuffix(base, ".shp")
	})
	if err != nil {
		return nil, errors.New("NWS archive is missing the official county shapefile")
	}
	reader, err := shp.OpenShapeFromZip(filepath.Clean(archivePath), shapeName)
	if err != nil {
		return nil, fmt.Errorf("open NWS shapefile: %w", err)
	}
	defer reader.Close()
	fields := fieldIndexes(reader.Fields())
	for _, required := range []string{"STATE", "COUNTYNAME", "FIPS", "LAT", "LON"} {
		if _, ok := fields[required]; !ok {
			return nil, fmt.Errorf("NWS shapefile is missing required field %s", required)
		}
	}
	byCode := map[string]*areaRecord{}
	geometries := map[string]multiPolygon{}
	for reader.Next() {
		_, rawShape := reader.Shape()
		polygon, ok := rawShape.(*shp.Polygon)
		if !ok {
			return nil, fmt.Errorf("NWS shapefile contains unsupported geometry %T", rawShape)
		}
		fips := strings.TrimSpace(reader.Attribute(fields["FIPS"]))
		if !fiveDigitFIPS.MatchString(fips) {
			return nil, fmt.Errorf("NWS shapefile contains invalid county FIPS %q", fips)
		}
		code := "0" + fips
		parts, bbox, err := shapePolygon(polygon)
		if err != nil {
			return nil, fmt.Errorf("NWS county %s: %w", code, err)
		}
		latitude, err := parseCoordinate(reader.Attribute(fields["LAT"]), -90, 90)
		if err != nil {
			return nil, fmt.Errorf("NWS county %s latitude: %w", code, err)
		}
		longitude, err := parseCoordinate(reader.Attribute(fields["LON"]), -180, 180)
		if err != nil {
			return nil, fmt.Errorf("NWS county %s longitude: %w", code, err)
		}
		name := strings.TrimSpace(reader.Attribute(fields["COUNTYNAME"]))
		state := strings.ToUpper(strings.TrimSpace(reader.Attribute(fields["STATE"])))
		if state != "" {
			name += ", " + state
		}
		if existing := byCode[code]; existing != nil {
			geometries[code] = append(geometries[code], parts...)
			existing.MinLon = math.Min(existing.MinLon, bbox[0])
			existing.MinLat = math.Min(existing.MinLat, bbox[1])
			existing.MaxLon = math.Max(existing.MaxLon, bbox[2])
			existing.MaxLat = math.Max(existing.MaxLat, bbox[3])
			continue
		}
		byCode[code] = &areaRecord{
			Source: "nws_same", Code: code, SameCode: code, Name: name, Region: state, Country: "US",
			Latitude: latitude, Longitude: longitude, MinLon: bbox[0], MinLat: bbox[1], MaxLon: bbox[2], MaxLat: bbox[3],
			ProviderVersion: version, SourceURL: sourceURL,
		}
		geometries[code] = parts
	}
	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("read NWS shapefile: %w", err)
	}
	return finalizeRecords(byCode, geometries)
}

func findShapeName(archivePath string, matches func(string) bool) (string, error) {
	reader, err := zip.OpenReader(filepath.Clean(archivePath))
	if err != nil {
		return "", err
	}
	defer reader.Close()
	for _, entry := range reader.File {
		if matches(entry.Name) {
			return entry.Name, nil
		}
	}
	return "", os.ErrNotExist
}

func fieldIndexes(fields []shp.Field) map[string]int {
	out := make(map[string]int, len(fields))
	for index, field := range fields {
		out[strings.ToUpper(strings.TrimSpace(field.String()))] = index
	}
	return out
}

func normalizeSixDigit(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < 6 {
		value = strings.Repeat("0", 6-len(value)) + value
	}
	return value
}

func parseCoordinate(raw string, minimum float64, maximum float64) (float64, error) {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < minimum || value > maximum {
		return 0, fmt.Errorf("%q is outside WGS84 bounds", strings.TrimSpace(raw))
	}
	return value, nil
}

func shapePolygon(shape *shp.Polygon) (multiPolygon, [4]float64, error) {
	if shape == nil || len(shape.Points) < 4 || len(shape.Parts) == 0 {
		return nil, [4]float64{}, errors.New("polygon is empty")
	}
	rings := make([][][]float64, 0, len(shape.Parts))
	for index, startValue := range shape.Parts {
		start := int(startValue)
		end := len(shape.Points)
		if index+1 < len(shape.Parts) {
			end = int(shape.Parts[index+1])
		}
		if start < 0 || end > len(shape.Points) || start >= end {
			return nil, [4]float64{}, errors.New("polygon part offsets are invalid")
		}
		ring := make([][]float64, 0, end-start+1)
		for _, point := range shape.Points[start:end] {
			if !validPoint(point.Y, point.X) {
				return nil, [4]float64{}, errors.New("polygon contains coordinates outside WGS84 bounds")
			}
			coordinate := []float64{point.X, point.Y}
			if len(ring) == 0 || coordinate[0] != ring[len(ring)-1][0] || coordinate[1] != ring[len(ring)-1][1] {
				ring = append(ring, coordinate)
			}
		}
		if len(ring) < 3 {
			continue
		}
		if ring[0][0] != ring[len(ring)-1][0] || ring[0][1] != ring[len(ring)-1][1] {
			ring = append(ring, []float64{ring[0][0], ring[0][1]})
		}
		if len(ring) >= 4 && math.Abs(ringArea(ring)) > 1e-12 {
			rings = append(rings, ring)
		}
	}
	if len(rings) == 0 {
		return nil, [4]float64{}, errors.New("polygon has no valid rings")
	}
	polygons := organizeRings(rings)
	if len(polygons) == 0 {
		return nil, [4]float64{}, errors.New("polygon ring topology is invalid")
	}
	bbox := [4]float64{shape.Box.MinX, shape.Box.MinY, shape.Box.MaxX, shape.Box.MaxY}
	if !validPoint(bbox[1], bbox[0]) || !validPoint(bbox[3], bbox[2]) || bbox[0] > bbox[2] || bbox[1] > bbox[3] {
		return nil, [4]float64{}, errors.New("polygon bounding box is invalid")
	}
	return polygons, bbox, nil
}

func organizeRings(rings [][][]float64) multiPolygon {
	type ringNode struct {
		ring   [][]float64
		area   float64
		parent int
		depth  int
	}
	nodes := make([]ringNode, len(rings))
	for index, ring := range rings {
		nodes[index] = ringNode{ring: ring, area: math.Abs(ringArea(ring)), parent: -1}
	}
	for index := range nodes {
		bestArea := math.Inf(1)
		point := nodes[index].ring[0]
		for candidate := range nodes {
			if index == candidate || nodes[candidate].area <= nodes[index].area || nodes[candidate].area >= bestArea {
				continue
			}
			if pointInRing(point, nodes[candidate].ring) {
				nodes[index].parent = candidate
				bestArea = nodes[candidate].area
			}
		}
	}
	var depth func(int) int
	depth = func(index int) int {
		if nodes[index].parent < 0 {
			return 0
		}
		if nodes[index].depth > 0 {
			return nodes[index].depth
		}
		nodes[index].depth = depth(nodes[index].parent) + 1
		return nodes[index].depth
	}
	polygonIndex := map[int]int{}
	out := multiPolygon{}
	for index := range nodes {
		if depth(index)%2 != 0 {
			continue
		}
		ring := orientRing(nodes[index].ring, true)
		polygonIndex[index] = len(out)
		out = append(out, [][][]float64{ring})
	}
	for index := range nodes {
		if depth(index)%2 == 0 {
			continue
		}
		exterior := nodes[index].parent
		for exterior >= 0 && depth(exterior)%2 != 0 {
			exterior = nodes[exterior].parent
		}
		if target, ok := polygonIndex[exterior]; ok {
			out[target] = append(out[target], orientRing(nodes[index].ring, false))
		}
	}
	return out
}

func ringArea(ring [][]float64) float64 {
	var area float64
	for index := 0; index+1 < len(ring); index++ {
		area += ring[index][0]*ring[index+1][1] - ring[index+1][0]*ring[index][1]
	}
	return area / 2
}

func orientRing(ring [][]float64, counterClockwise bool) [][]float64 {
	copyRing := append([][]float64(nil), ring...)
	if (ringArea(copyRing) > 0) != counterClockwise {
		for left, right := 0, len(copyRing)-1; left < right; left, right = left+1, right-1 {
			copyRing[left], copyRing[right] = copyRing[right], copyRing[left]
		}
	}
	return copyRing
}

func pointInRing(point []float64, ring [][]float64) bool {
	inside := false
	for current, previous := 0, len(ring)-1; current < len(ring); previous, current = current, current+1 {
		xi, yi := ring[current][0], ring[current][1]
		xj, yj := ring[previous][0], ring[previous][1]
		intersects := (yi > point[1]) != (yj > point[1]) && point[0] < (xj-xi)*(point[1]-yi)/(yj-yi)+xi
		if intersects {
			inside = !inside
		}
	}
	return inside
}

func validPoint(latitude float64, longitude float64) bool {
	return !math.IsNaN(latitude) && !math.IsInf(latitude, 0) && !math.IsNaN(longitude) && !math.IsInf(longitude, 0) && latitude >= -90 && latitude <= 90 && longitude >= -180 && longitude <= 180
}

func finalizeRecords(byCode map[string]*areaRecord, geometries map[string]multiPolygon) ([]areaRecord, error) {
	codes := make([]string, 0, len(byCode))
	for code := range byCode {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	out := make([]areaRecord, 0, len(codes))
	for _, code := range codes {
		record := *byCode[code]
		raw, err := encodeMultiPolygonWKB(geometries[code])
		if err != nil {
			return nil, fmt.Errorf("encode %s:%s geometry: %w", record.Source, record.Code, err)
		}
		record.GeometryType = "multipolygon"
		record.GeometryWKB = raw
		out = append(out, record)
	}
	return out, nil
}

func encodeMultiPolygonWKB(polygons multiPolygon) ([]byte, error) {
	if len(polygons) == 0 || uint64(len(polygons)) > uint64(^uint32(0)) {
		return nil, errors.New("multipolygon contains an invalid polygon count")
	}
	out := []byte{1}
	out = appendWKBUint32(out, 6)
	out = appendWKBUint32(out, uint32(len(polygons)))
	for _, polygon := range polygons {
		encoded, err := encodePolygonWKB(polygon)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded...)
	}
	return out, nil
}

func encodePolygonWKB(polygon [][][]float64) ([]byte, error) {
	if len(polygon) == 0 || uint64(len(polygon)) > uint64(^uint32(0)) {
		return nil, errors.New("polygon contains an invalid ring count")
	}
	out := []byte{1}
	out = appendWKBUint32(out, 3)
	out = appendWKBUint32(out, uint32(len(polygon)))
	for _, ring := range polygon {
		if len(ring) < 4 || uint64(len(ring)) > uint64(^uint32(0)) {
			return nil, errors.New("polygon ring contains an invalid point count")
		}
		first, last := ring[0], ring[len(ring)-1]
		if len(first) < 2 || len(last) < 2 || first[0] != last[0] || first[1] != last[1] {
			return nil, errors.New("polygon ring is not closed")
		}
		if math.Abs(ringArea(ring)) <= 1e-12 {
			return nil, errors.New("polygon ring has zero area")
		}
		out = appendWKBUint32(out, uint32(len(ring)))
		for _, point := range ring {
			if len(point) < 2 || !validPoint(point[1], point[0]) {
				return nil, errors.New("polygon contains coordinates outside WGS84 bounds")
			}
			out = appendWKBFloat64(out, point[0])
			out = appendWKBFloat64(out, point[1])
		}
	}
	return out, nil
}

func appendWKBUint32(destination []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendWKBFloat64(destination []byte, value float64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))
	return append(destination, encoded[:]...)
}

type areaWKBReader struct {
	data   []byte
	offset int
}

func validateAreaWKB(data []byte) (string, error) {
	reader := areaWKBReader{data: data}
	geometryType, err := reader.geometry(0)
	if err != nil {
		return "", err
	}
	if reader.offset != len(reader.data) {
		return "", errors.New("WKB contains trailing data")
	}
	return geometryType, nil
}

func (reader *areaWKBReader) geometry(depth int) (string, error) {
	if reader == nil || depth > 1 {
		return "", errors.New("WKB geometry nesting is invalid")
	}
	byteOrder, err := reader.byteOrder()
	if err != nil {
		return "", err
	}
	geometryType, err := reader.uint32(byteOrder)
	if err != nil {
		return "", err
	}
	switch geometryType {
	case 3:
		if err := reader.polygon(byteOrder); err != nil {
			return "", err
		}
		return "polygon", nil
	case 6:
		count, err := reader.uint32(byteOrder)
		if err != nil {
			return "", err
		}
		if count == 0 || uint64(count) > uint64(len(reader.data)-reader.offset) {
			return "", errors.New("WKB multipolygon contains an invalid polygon count")
		}
		for index := uint32(0); index < count; index++ {
			childType, err := reader.geometry(depth + 1)
			if err != nil {
				return "", err
			}
			if childType != "polygon" {
				return "", errors.New("WKB multipolygon contains a non-polygon member")
			}
		}
		return "multipolygon", nil
	default:
		return "", fmt.Errorf("unsupported WKB geometry type %d", geometryType)
	}
}

func (reader *areaWKBReader) polygon(byteOrder binary.ByteOrder) error {
	ringCount, err := reader.uint32(byteOrder)
	if err != nil {
		return err
	}
	if ringCount == 0 || uint64(ringCount) > uint64(len(reader.data)-reader.offset) {
		return errors.New("WKB polygon contains an invalid ring count")
	}
	for ringIndex := uint32(0); ringIndex < ringCount; ringIndex++ {
		pointCount, err := reader.uint32(byteOrder)
		if err != nil {
			return err
		}
		if pointCount < 4 || uint64(pointCount) > uint64((len(reader.data)-reader.offset)/16) {
			return errors.New("WKB polygon ring contains an invalid point count")
		}
		var firstX, firstY, previousX, previousY float64
		var signedArea float64
		for pointIndex := uint32(0); pointIndex < pointCount; pointIndex++ {
			x, err := reader.float64(byteOrder)
			if err != nil {
				return err
			}
			y, err := reader.float64(byteOrder)
			if err != nil {
				return err
			}
			if !validPoint(y, x) {
				return errors.New("WKB polygon contains coordinates outside WGS84 bounds")
			}
			if pointIndex == 0 {
				firstX, firstY = x, y
			} else {
				signedArea += previousX*y - x*previousY
			}
			previousX, previousY = x, y
		}
		if previousX != firstX || previousY != firstY {
			return errors.New("WKB polygon ring is not closed")
		}
		if math.Abs(signedArea/2) <= 1e-12 {
			return errors.New("WKB polygon ring has zero area")
		}
	}
	return nil
}

func (reader *areaWKBReader) byteOrder() (binary.ByteOrder, error) {
	if reader == nil || reader.offset >= len(reader.data) {
		return nil, errors.New("WKB is truncated")
	}
	value := reader.data[reader.offset]
	reader.offset++
	switch value {
	case 0:
		return binary.BigEndian, nil
	case 1:
		return binary.LittleEndian, nil
	default:
		return nil, errors.New("WKB contains an invalid byte order")
	}
}

func (reader *areaWKBReader) uint32(byteOrder binary.ByteOrder) (uint32, error) {
	if reader == nil || reader.offset+4 > len(reader.data) {
		return 0, errors.New("WKB is truncated")
	}
	value := byteOrder.Uint32(reader.data[reader.offset : reader.offset+4])
	reader.offset += 4
	return value, nil
}

func (reader *areaWKBReader) float64(byteOrder binary.ByteOrder) (float64, error) {
	if reader == nil || reader.offset+8 > len(reader.data) {
		return 0, errors.New("WKB is truncated")
	}
	value := math.Float64frombits(byteOrder.Uint64(reader.data[reader.offset : reader.offset+8]))
	reader.offset += 8
	return value, nil
}

func applyRecords(ctx context.Context, databasePath string, source string, version string, records []areaRecord, now time.Time) (Result, error) {
	if len(records) == 0 {
		return Result{}, fmt.Errorf("%s geometry import contains no records", source)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	importedAt := now.UTC().Format(time.RFC3339)
	db, err := sql.Open("sqlite", filepath.Clean(databasePath))
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := ensureGeometrySchema(ctx, db); err != nil {
		return Result{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM area_geometries WHERE source = ?", source); err != nil {
		return Result{}, err
	}
	statement, err := tx.PrepareContext(ctx, `
		INSERT INTO area_geometries(
			canonical_id, source, code, same_code, geometry_type, geometry_wkb,
			latitude, longitude, min_lon, max_lon, min_lat, max_lat, accuracy_m,
			valid_from, valid_to, is_current, source_id, provider_version, source_url,
			updated_at, attributes_json
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return Result{}, err
	}
	defer statement.Close()
	for _, record := range records {
		updatedAt := strings.TrimSpace(record.UpdatedAt)
		if updatedAt == "" {
			updatedAt = importedAt
		}
		sourceID := strings.TrimSpace(record.SourceID)
		if sourceID == "" {
			sourceID = areaGeometrySourceID(record.Source)
		}
		if _, err := statement.ExecContext(ctx,
			legacyCanonicalID(record.Source, record.Code), record.Source, record.Code, record.SameCode,
			record.GeometryType, record.GeometryWKB, record.Latitude, record.Longitude,
			record.MinLon, record.MaxLon, record.MinLat, record.MaxLat, nil, nil, nil, 1,
			sourceID, record.ProviderVersion, record.SourceURL, updatedAt, "{}",
		); err != nil {
			return Result{}, err
		}
	}
	sourceID := areaGeometrySourceID(source)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO sources(source_id, title, source_version, retrieved_at, attributes_json)
		VALUES(?, ?, ?, ?, '{}')
		ON CONFLICT(source_id) DO UPDATE SET
			title = excluded.title, source_version = excluded.source_version,
			retrieved_at = excluded.retrieved_at
	`, sourceID, areaGeometrySourceTitle(source), version, importedAt); err != nil {
		return Result{}, err
	}
	metadataPrefix := map[string]string{"clc": "eccc_clc", "nws_same": "nws_county"}[source]
	if metadataPrefix == "" {
		metadataPrefix = source
	}
	var geometryCount int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM area_geometries").Scan(&geometryCount); err != nil {
		return Result{}, err
	}
	for key, value := range map[string]string{
		"schema_version":                 "1",
		"pack_id":                        "legacy-weather-geometry",
		"core_pack_id":                   "legacy-alert-locations",
		"pack_kind":                      "geometry",
		"count.geometries":               strconv.Itoa(geometryCount),
		"count.source." + sourceID:       strconv.Itoa(len(records)),
		"updated_at":                     importedAt,
		metadataPrefix + "_version":      version,
		metadataPrefix + "_record_count": strconv.Itoa(len(records)),
		metadataPrefix + "_source_url":   records[0].SourceURL,
		metadataPrefix + "_imported_at":  importedAt,
	} {
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO catalog_metadata(key, value) VALUES(?, ?)", key, value); err != nil {
			return Result{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	if err := validateGeometryDatabase(ctx, databasePath, source, len(records)); err != nil {
		return Result{}, err
	}
	return Result{Source: source, ProviderVersion: version, RecordCount: len(records), ImportedAt: importedAt}, nil
}

func ensureGeometrySchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		PRAGMA foreign_keys = ON;
		PRAGMA user_version = 1;
		CREATE TABLE IF NOT EXISTS catalog_metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS sources (
			source_id TEXT PRIMARY KEY,
			title TEXT NOT NULL,
			source_version TEXT,
			retrieved_at TEXT,
			valid_from TEXT,
			valid_to TEXT,
			licence TEXT,
			attribution TEXT,
			source_sha256 TEXT,
			attributes_json TEXT NOT NULL DEFAULT '{}'
		);
		CREATE TABLE IF NOT EXISTS area_geometries (
			geometry_pk INTEGER PRIMARY KEY,
			canonical_id TEXT NOT NULL,
			source TEXT,
			code TEXT,
			same_code TEXT,
			geometry_type TEXT NOT NULL,
			geometry_wkb BLOB NOT NULL,
			latitude REAL,
			longitude REAL,
			min_lon REAL NOT NULL,
			max_lon REAL NOT NULL,
			min_lat REAL NOT NULL,
			max_lat REAL NOT NULL,
			accuracy_m REAL,
			valid_from TEXT,
			valid_to TEXT,
			is_current INTEGER NOT NULL DEFAULT 1 CHECK (is_current IN (0, 1)),
			source_id TEXT,
			provider_version TEXT,
			source_url TEXT,
			updated_at TEXT,
			attributes_json TEXT NOT NULL DEFAULT '{}'
		);
		CREATE INDEX IF NOT EXISTS idx_area_geometries_canonical
			ON area_geometries(canonical_id, is_current);
		CREATE INDEX IF NOT EXISTS idx_area_geometries_source_code
			ON area_geometries(source, code, is_current);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_area_geometries_legacy_identity
			ON area_geometries(source, code)
			WHERE source IS NOT NULL AND code IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_area_geometries_same
			ON area_geometries(source, same_code);
		CREATE VIRTUAL TABLE IF NOT EXISTS area_geometry_rtree USING rtree(
			geometry_pk,
			min_lon,
			max_lon,
			min_lat,
			max_lat
		);
		CREATE TRIGGER IF NOT EXISTS area_geometry_rtree_insert
		AFTER INSERT ON area_geometries BEGIN
			INSERT OR REPLACE INTO area_geometry_rtree(
				geometry_pk, min_lon, max_lon, min_lat, max_lat
			) VALUES (new.geometry_pk, new.min_lon, new.max_lon, new.min_lat, new.max_lat);
		END;
		CREATE TRIGGER IF NOT EXISTS area_geometry_rtree_update
		AFTER UPDATE OF min_lon, max_lon, min_lat, max_lat ON area_geometries BEGIN
			DELETE FROM area_geometry_rtree WHERE geometry_pk = old.geometry_pk;
			INSERT OR REPLACE INTO area_geometry_rtree(
				geometry_pk, min_lon, max_lon, min_lat, max_lat
			) VALUES (new.geometry_pk, new.min_lon, new.max_lon, new.min_lat, new.max_lat);
		END;
		CREATE TRIGGER IF NOT EXISTS area_geometry_rtree_delete
		AFTER DELETE ON area_geometries BEGIN
			DELETE FROM area_geometry_rtree WHERE geometry_pk = old.geometry_pk;
		END;
	`)
	if err != nil {
		return fmt.Errorf("create geometry catalog schema: %w", err)
	}
	return nil
}

// InitializeGeometryDatabase creates an empty geometry-only catalog.
func InitializeGeometryDatabase(ctx context.Context, databasePath string) error {
	databasePath = filepath.Clean(databasePath)
	if err := os.MkdirAll(filepath.Dir(databasePath), 0o755); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := ensureGeometrySchema(ctx, db); err != nil {
		return err
	}
	for key, value := range map[string]string{
		"schema_version":   "1",
		"pack_id":          "legacy-weather-geometry",
		"core_pack_id":     "legacy-alert-locations",
		"pack_kind":        "geometry",
		"count.geometries": "0",
	} {
		if _, err := db.ExecContext(ctx, "INSERT OR REPLACE INTO catalog_metadata(key, value) VALUES(?, ?)", key, value); err != nil {
			return err
		}
	}
	return nil
}

// PrepareGeometryCandidate clones an active geometry catalog or initializes an
// empty candidate when the active catalog has not been installed yet.
func PrepareGeometryCandidate(ctx context.Context, activePath string, candidatePath string) error {
	activePath = filepath.Clean(activePath)
	candidatePath = filepath.Clean(candidatePath)
	if activePath == candidatePath {
		return errors.New("active and candidate geometry databases must be different")
	}
	if stat, err := os.Stat(activePath); err == nil {
		if stat.IsDir() {
			return errors.New("active geometry catalog is a directory")
		}
		return CloneDatabase(ctx, activePath, candidatePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(candidatePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return InitializeGeometryDatabase(ctx, candidatePath)
}

// SplitLegacyDatabase creates a stripped core candidate and a geometry-only
// candidate from a combined legacy location database. Existing inputs and
// outputs are never replaced by this function.
func SplitLegacyDatabase(ctx context.Context, legacyPath string, coreOutputPath string, geometryOutputPath string, now time.Time) (SplitResult, error) {
	return SplitLegacyDatabaseWithCoreBase(ctx, legacyPath, "", coreOutputPath, geometryOutputPath, now)
}

// SplitLegacyDatabaseWithCoreBase creates geometry from a combined legacy
// database while optionally seeding the core candidate from a newer compact
// core. Existing core rows win, and only missing places referenced by an
// inline geometry are copied from the combined input.
func SplitLegacyDatabaseWithCoreBase(ctx context.Context, legacyPath string, coreBasePath string, coreOutputPath string, geometryOutputPath string, now time.Time) (SplitResult, error) {
	legacyPath = filepath.Clean(legacyPath)
	coreBasePath = strings.TrimSpace(coreBasePath)
	if coreBasePath != "" {
		coreBasePath = filepath.Clean(coreBasePath)
	}
	coreOutputPath = filepath.Clean(coreOutputPath)
	geometryOutputPath = filepath.Clean(geometryOutputPath)
	if samePath(legacyPath, coreOutputPath) || samePath(legacyPath, geometryOutputPath) || samePath(coreOutputPath, geometryOutputPath) || (coreBasePath != "" && (samePath(coreBasePath, coreOutputPath) || samePath(coreBasePath, geometryOutputPath))) {
		return SplitResult{}, errors.New("legacy input, core output, and geometry output paths must be different")
	}
	for label, candidate := range map[string]string{"core output": coreOutputPath, "geometry output": geometryOutputPath} {
		if _, err := os.Lstat(candidate); err == nil {
			return SplitResult{}, fmt.Errorf("%s already exists", label)
		} else if !errors.Is(err, os.ErrNotExist) {
			return SplitResult{}, err
		}
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	coreSeedPath := legacyPath
	if coreBasePath != "" {
		coreSeedPath = coreBasePath
	}
	coreCounts, err := legacyCoreTableCounts(ctx, coreSeedPath)
	if err != nil {
		return SplitResult{}, err
	}
	recordsBySource, versions, err := readLegacyGeometryRecords(ctx, legacyPath)
	if err != nil {
		return SplitResult{}, err
	}
	if len(recordsBySource) == 0 {
		return SplitResult{}, errors.New("legacy database contains no inline area geometries")
	}
	succeeded := false
	defer func() {
		if !succeeded {
			_ = os.Remove(coreOutputPath)
			_ = os.Remove(geometryOutputPath)
		}
	}()
	if err := CloneDatabase(ctx, coreSeedPath, coreOutputPath); err != nil {
		return SplitResult{}, fmt.Errorf("clone legacy core candidate: %w", err)
	}
	if err := InitializeGeometryDatabase(ctx, geometryOutputPath); err != nil {
		return SplitResult{}, fmt.Errorf("initialize geometry candidate: %w", err)
	}
	result := SplitResult{SourceCounts: map[string]int{}}
	if coreBasePath != "" {
		added, err := mergeLegacyGeometryPlaces(ctx, coreOutputPath, legacyPath)
		if err != nil {
			return SplitResult{}, fmt.Errorf("merge missing geometry places into core base: %w", err)
		}
		result.CorePlacesAdded = added
		coreCounts["places"] += added
	}
	for _, source := range sortedRecordSources(recordsBySource) {
		records := recordsBySource[source]
		if _, err := applyRecords(ctx, geometryOutputPath, source, versions[source], records, now); err != nil {
			return SplitResult{}, fmt.Errorf("split %s geometry: %w", source, err)
		}
		result.SourceCounts[source] = len(records)
		result.GeometryCount += len(records)
	}
	if err := stripLegacyCoreGeometry(ctx, coreOutputPath, coreBasePath == ""); err != nil {
		return SplitResult{}, err
	}
	if err := validateSplitCore(ctx, coreOutputPath, coreCounts); err != nil {
		return SplitResult{}, err
	}
	if err := BindGeometryToLegacyCore(ctx, geometryOutputPath, coreOutputPath); err != nil {
		return SplitResult{}, fmt.Errorf("bind split geometry to stripped core: %w", err)
	}
	succeeded = true
	return result, nil
}

type sqliteColumn struct {
	Name       string
	NotNull    bool
	HasDefault bool
}

func mergeLegacyGeometryPlaces(ctx context.Context, corePath string, combinedPath string) (int, error) {
	db, err := sql.Open("sqlite", filepath.Clean(corePath))
	if err != nil {
		return 0, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "ATTACH DATABASE ? AS combined", filepath.Clean(combinedPath)); err != nil {
		return 0, err
	}
	defer db.ExecContext(context.Background(), "DETACH DATABASE combined")
	coreColumns, err := sqliteTableColumns(ctx, db, "main", "places")
	if err != nil {
		return 0, err
	}
	combinedColumns, err := sqliteTableColumns(ctx, db, "combined", "places")
	if err != nil {
		return 0, err
	}
	combinedNames := make(map[string]struct{}, len(combinedColumns))
	for _, column := range combinedColumns {
		combinedNames[column.Name] = struct{}{}
	}
	selected := make([]string, 0, len(coreColumns))
	for _, column := range coreColumns {
		if _, ok := combinedNames[column.Name]; ok {
			selected = append(selected, column.Name)
			continue
		}
		if column.NotNull && !column.HasDefault {
			return 0, fmt.Errorf("core places column %q is required but absent from the combined catalog", column.Name)
		}
	}
	if !containsString(selected, "source") || !containsString(selected, "code") {
		return 0, errors.New("core and combined places tables must contain source and code")
	}
	var before int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM main.places").Scan(&before); err != nil {
		return 0, err
	}
	quoted := make([]string, len(selected))
	qualified := make([]string, len(selected))
	for index, name := range selected {
		quoted[index] = quoteSQLiteIdentifier(name)
		qualified[index] = "place." + quoteSQLiteIdentifier(name)
	}
	statement := `INSERT OR IGNORE INTO main.places (` + strings.Join(quoted, ",") + `)
		SELECT DISTINCT ` + strings.Join(qualified, ",") + `
		FROM combined.places AS place
		JOIN combined.area_geometries AS geometry
		  ON geometry.source = place.source AND geometry.code = place.code
		WHERE NOT EXISTS (
			SELECT 1 FROM main.places AS existing
			WHERE existing.source = place.source AND existing.code = place.code
		)`
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return 0, err
	}
	var after int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM main.places").Scan(&after); err != nil {
		return 0, err
	}
	return after - before, nil
}

func sqliteTableColumns(ctx context.Context, db *sql.DB, schema string, table string) ([]sqliteColumn, error) {
	if schema != "main" && schema != "combined" {
		return nil, errors.New("invalid SQLite schema name")
	}
	if table != "places" {
		return nil, errors.New("invalid SQLite table name")
	}
	rows, err := db.QueryContext(ctx, "PRAGMA "+schema+".table_info("+table+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := []sqliteColumn{}
	for rows.Next() {
		var sequence, notNull, primaryKey int
		var name, dataType string
		var defaultValue sql.NullString
		if err := rows.Scan(&sequence, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, err
		}
		columns = append(columns, sqliteColumn{Name: name, NotNull: notNull != 0, HasDefault: defaultValue.Valid})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("%s.%s does not exist", schema, table)
	}
	return columns, nil
}

func quoteSQLiteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func readLegacyGeometryRecords(ctx context.Context, databasePath string) (map[string][]areaRecord, map[string]string, error) {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(databasePath))
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	rows, err := db.QueryContext(ctx, `
		SELECT source, code, same_code, latitude, longitude,
		       min_lon, min_lat, max_lon, max_lat, geometry_json,
		       provider_version, source_url, updated_at
		FROM area_geometries
		ORDER BY source, code
	`)
	if err != nil {
		return nil, nil, fmt.Errorf("read legacy inline geometry: %w", err)
	}
	defer rows.Close()
	recordsBySource := map[string][]areaRecord{}
	versions := map[string]string{}
	for rows.Next() {
		var record areaRecord
		var geometryJSON string
		if err := rows.Scan(
			&record.Source, &record.Code, &record.SameCode, &record.Latitude, &record.Longitude,
			&record.MinLon, &record.MinLat, &record.MaxLon, &record.MaxLat, &geometryJSON,
			&record.ProviderVersion, &record.SourceURL, &record.UpdatedAt,
		); err != nil {
			return nil, nil, err
		}
		record.Source = strings.TrimSpace(record.Source)
		record.Code = strings.TrimSpace(record.Code)
		record.SameCode = strings.TrimSpace(record.SameCode)
		if record.Source == "" || record.Code == "" {
			return nil, nil, errors.New("legacy inline geometry has an empty source or code")
		}
		record.GeometryType, record.GeometryWKB, err = encodeLegacyGeoJSONWKB(geometryJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("legacy area geometry %s:%s: %w", record.Source, record.Code, err)
		}
		recordsBySource[record.Source] = append(recordsBySource[record.Source], record)
		if versions[record.Source] == "" {
			versions[record.Source] = record.ProviderVersion
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return recordsBySource, versions, nil
}

func encodeLegacyGeoJSONWKB(raw string) (string, []byte, error) {
	var geometry struct {
		Type        string          `json:"type"`
		Coordinates json.RawMessage `json:"coordinates"`
	}
	if err := json.Unmarshal([]byte(raw), &geometry); err != nil {
		return "", nil, fmt.Errorf("invalid GeoJSON: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(geometry.Type)) {
	case "polygon":
		var polygon [][][]float64
		if err := json.Unmarshal(geometry.Coordinates, &polygon); err != nil {
			return "", nil, fmt.Errorf("invalid Polygon coordinates: %w", err)
		}
		encoded, err := encodePolygonWKB(polygon)
		return "polygon", encoded, err
	case "multipolygon":
		var polygons multiPolygon
		if err := json.Unmarshal(geometry.Coordinates, &polygons); err != nil {
			return "", nil, fmt.Errorf("invalid MultiPolygon coordinates: %w", err)
		}
		encoded, err := encodeMultiPolygonWKB(polygons)
		return "multipolygon", encoded, err
	default:
		return "", nil, fmt.Errorf("unsupported GeoJSON geometry type %q", geometry.Type)
	}
}

func sortedRecordSources(records map[string][]areaRecord) []string {
	sources := make([]string, 0, len(records))
	for source := range records {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	return sources
}

func stripLegacyCoreGeometry(ctx context.Context, databasePath string, removeGeometryMetadata bool) error {
	db, err := sql.Open("sqlite", filepath.Clean(databasePath))
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	statement := "DROP TABLE IF EXISTS area_geometries;"
	if removeGeometryMetadata {
		statement += `
			DELETE FROM metadata
			WHERE key = 'area_geometry_schema'
			   OR key LIKE 'eccc_clc_%'
			   OR key LIKE 'nws_county_%';`
	}
	if _, err := db.ExecContext(ctx, statement); err != nil {
		_ = db.Close()
		return fmt.Errorf("strip inline geometry from core candidate: %w", err)
	}
	if _, err := db.ExecContext(ctx, "VACUUM"); err != nil {
		_ = db.Close()
		return fmt.Errorf("compact stripped core candidate: %w", err)
	}
	if err := db.Close(); err != nil {
		return err
	}
	return nil
}

func validateSplitCore(ctx context.Context, databasePath string, expected map[string]int) error {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(databasePath))
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var integrity string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		return fmt.Errorf("stripped core database failed integrity validation: %s", integrity)
	}
	var geometryTable int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'area_geometries'").Scan(&geometryTable); err != nil {
		return err
	}
	if geometryTable != 0 {
		return errors.New("stripped core database still contains area_geometries")
	}
	actual, err := legacyCoreTableCountsFromDB(ctx, db)
	if err != nil {
		return err
	}
	for table, count := range expected {
		if actual[table] != count {
			return fmt.Errorf("stripped core table %s contains %d rows, expected %d", table, actual[table], count)
		}
	}
	return nil
}

// BindGeometryToLegacyCore validates source-qualified membership and records
// the exact finalized core file checksum in a writable geometry candidate.
// Geometry imports deliberately do not update core names or centroids.
// Operators must refresh the core catalog first when a provider adds an ID.
func BindGeometryToLegacyCore(ctx context.Context, geometryPath string, corePath string) error {
	if err := validateGeometryLegacyIdentities(ctx, geometryPath, corePath); err != nil {
		return err
	}
	checksum, err := databaseSHA256(corePath)
	if err != nil {
		return fmt.Errorf("checksum paired legacy core: %w", err)
	}
	db, err := sql.Open("sqlite", filepath.Clean(geometryPath))
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "INSERT OR REPLACE INTO catalog_metadata(key, value) VALUES('core_sha256', ?)", checksum); err != nil {
		return fmt.Errorf("bind geometry candidate to paired legacy core: %w", err)
	}
	return nil
}

// ValidateGeometryAgainstLegacyCore proves membership and verifies the
// geometry pack is bound to the exact paired core file generation.
func ValidateGeometryAgainstLegacyCore(ctx context.Context, geometryPath string, corePath string) error {
	if err := validateGeometryLegacyIdentities(ctx, geometryPath, corePath); err != nil {
		return err
	}
	expected, err := databaseSHA256(corePath)
	if err != nil {
		return fmt.Errorf("checksum paired legacy core: %w", err)
	}
	geometryDB, err := sql.Open("sqlite", sqliteReadOnlyDSN(geometryPath))
	if err != nil {
		return err
	}
	defer geometryDB.Close()
	geometryDB.SetMaxOpenConns(1)
	var actual string
	if err := geometryDB.QueryRowContext(ctx, "SELECT value FROM catalog_metadata WHERE key = 'core_sha256'").Scan(&actual); err != nil {
		return fmt.Errorf("read geometry core checksum binding: %w", err)
	}
	decoded, err := hex.DecodeString(actual)
	if err != nil || len(decoded) != sha256.Size {
		return errors.New("geometry core checksum binding is malformed")
	}
	if !strings.EqualFold(actual, expected) {
		return errors.New("geometry pack is bound to a different paired core file generation")
	}
	return nil
}

func validateGeometryLegacyIdentities(ctx context.Context, geometryPath string, corePath string) error {
	geometryDB, err := sql.Open("sqlite", sqliteReadOnlyDSN(geometryPath))
	if err != nil {
		return err
	}
	defer geometryDB.Close()
	geometryDB.SetMaxOpenConns(1)
	var pairedCoreID string
	if err := geometryDB.QueryRowContext(ctx, "SELECT value FROM catalog_metadata WHERE key = 'core_pack_id'").Scan(&pairedCoreID); err != nil {
		return fmt.Errorf("read geometry core pack identity: %w", err)
	}
	if pairedCoreID != "legacy-alert-locations" {
		return fmt.Errorf("geometry pack declares core %q, expected legacy-alert-locations", pairedCoreID)
	}
	coreDB, err := sql.Open("sqlite", sqliteReadOnlyDSN(corePath))
	if err != nil {
		return err
	}
	defer coreDB.Close()
	coreDB.SetMaxOpenConns(1)
	validIDs := map[string]struct{}{}
	coreRows, err := coreDB.QueryContext(ctx, "SELECT source, code FROM places ORDER BY source, code")
	if err != nil {
		return fmt.Errorf("read paired legacy core identities: %w", err)
	}
	for coreRows.Next() {
		var source, code string
		if err := coreRows.Scan(&source, &code); err != nil {
			_ = coreRows.Close()
			return err
		}
		validIDs[legacyCanonicalID(source, code)] = struct{}{}
	}
	if err := coreRows.Err(); err != nil {
		_ = coreRows.Close()
		return err
	}
	if err := coreRows.Close(); err != nil {
		return err
	}
	if len(validIDs) == 0 {
		return errors.New("paired legacy core contains no source-qualified places")
	}
	geometryRows, err := geometryDB.QueryContext(ctx, "SELECT canonical_id, source, code FROM area_geometries ORDER BY geometry_pk")
	if err != nil {
		return fmt.Errorf("read geometry identities: %w", err)
	}
	defer geometryRows.Close()
	geometryCount := 0
	for geometryRows.Next() {
		var canonicalID string
		var source, code sql.NullString
		if err := geometryRows.Scan(&canonicalID, &source, &code); err != nil {
			return err
		}
		geometryCount++
		if !source.Valid || strings.TrimSpace(source.String) == "" || !code.Valid || strings.TrimSpace(code.String) == "" {
			return fmt.Errorf("geometry %q is missing its legacy source-qualified identity", canonicalID)
		}
		expectedID := legacyCanonicalID(source.String, code.String)
		if canonicalID != expectedID {
			return fmt.Errorf("geometry %s:%s has canonical ID %q, expected %q", source.String, code.String, canonicalID, expectedID)
		}
		if _, ok := validIDs[canonicalID]; !ok {
			return fmt.Errorf("geometry %s:%s is absent from the paired legacy core", source.String, code.String)
		}
	}
	if err := geometryRows.Err(); err != nil {
		return err
	}
	if geometryCount == 0 {
		return errors.New("geometry pack contains no records")
	}
	return nil
}

func databaseSHA256(path string) (string, error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func legacyCoreTableCounts(ctx context.Context, databasePath string) (map[string]int, error) {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(databasePath))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	return legacyCoreTableCountsFromDB(ctx, db)
}

func legacyCoreTableCountsFromDB(ctx context.Context, db *sql.DB) (map[string]int, error) {
	counts := map[string]int{}
	for _, table := range []string{"places", "links", "postal_links", "station_links"} {
		var exists int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&exists); err != nil {
			return nil, err
		}
		if exists == 0 {
			continue
		}
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			return nil, err
		}
		counts[table] = count
	}
	return counts, nil
}

func samePath(left string, right string) bool {
	leftAbsolute, leftErr := filepath.Abs(filepath.Clean(left))
	rightAbsolute, rightErr := filepath.Abs(filepath.Clean(right))
	if leftErr != nil || rightErr != nil {
		return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
	}
	return strings.EqualFold(leftAbsolute, rightAbsolute)
}

func sqliteReadOnlyDSN(path string) string {
	return "file:" + filepath.ToSlash(filepath.Clean(path)) + "?mode=ro&_pragma=query_only(ON)"
}

func validateGeometryDatabase(ctx context.Context, databasePath string, source string, expected int) error {
	db, err := sql.Open("sqlite", filepath.Clean(databasePath))
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var integrity string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		return fmt.Errorf("candidate geometry database failed integrity validation: %s", integrity)
	}
	var actual, indexed int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM area_geometries
		WHERE source = ? AND same_code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
		  AND min_lon >= -180 AND max_lon <= 180 AND min_lat >= -90 AND max_lat <= 90
		  AND min_lon <= max_lon AND min_lat <= max_lat
		  AND geometry_type IN ('polygon', 'multipolygon') AND length(geometry_wkb) > 9
		  AND is_current = 1 AND canonical_id LIKE 'urn:haze:location:%'
	`, source).Scan(&actual); err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("candidate geometry database contains %d valid %s geometries, expected %d", actual, source, expected)
	}
	rows, err := db.QueryContext(ctx, "SELECT geometry_type, geometry_wkb FROM area_geometries WHERE source = ?", source)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var geometryType string
		var wkb []byte
		if err := rows.Scan(&geometryType, &wkb); err != nil {
			return err
		}
		parsedType, err := validateAreaWKB(wkb)
		if err != nil {
			return fmt.Errorf("candidate geometry database contains invalid WKB: %w", err)
		}
		if parsedType != geometryType {
			return fmt.Errorf("candidate geometry type %q does not match WKB type %q", geometryType, parsedType)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM area_geometry_rtree r
		JOIN area_geometries g ON g.geometry_pk = r.geometry_pk
		WHERE g.source = ?
	`, source).Scan(&indexed); err != nil {
		return err
	}
	if indexed != expected {
		return fmt.Errorf("candidate geometry database indexes %d %s geometries, expected %d", indexed, source, expected)
	}
	var rtreeIntegrity string
	if err := db.QueryRowContext(ctx, "SELECT rtreecheck('area_geometry_rtree')").Scan(&rtreeIntegrity); err != nil {
		return err
	}
	if rtreeIntegrity != "ok" {
		return fmt.Errorf("candidate geometry RTree failed integrity validation: %s", rtreeIntegrity)
	}
	return nil
}

func areaGeometrySourceID(source string) string {
	return "legacy-area-geometry:" + strings.ToLower(strings.TrimSpace(source))
}

func areaGeometrySourceTitle(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "clc":
		return "Environment and Climate Change Canada CLC geometry"
	case "nws_same":
		return "National Weather Service county geometry"
	default:
		return strings.TrimSpace(source) + " area geometry"
	}
}

func legacyCanonicalID(source string, code string) string {
	namespace := [16]byte{0xf9, 0x87, 0xbc, 0x4e, 0x56, 0xfe, 0x5f, 0x41, 0x8b, 0xcf, 0xfd, 0xcf, 0x8c, 0x5a, 0x8e, 0x3e}
	key := []byte("legacy:" + strings.TrimSpace(source) + ":" + strings.TrimSpace(code))
	payload := make([]byte, 0, len(namespace)+len(key))
	payload = append(payload, namespace[:]...)
	payload = append(payload, key...)
	digest := sha1.Sum(payload)
	sum := digest[:]
	sum[6] = (sum[6] & 0x0f) | 0x50
	sum[8] = (sum[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"urn:haze:location:%x-%x-%x-%x-%x",
		sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16],
	)
}
