// Package locationimport validates and imports managed alert-area geometry packs.
package locationimport

import (
	"archive/zip"
	"context"
	"database/sql"
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
	GeometryJSON    string
	ProviderVersion string
	SourceURL       string
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
		geometry := map[string]any{"type": "MultiPolygon", "coordinates": geometries[code]}
		raw, err := json.Marshal(geometry)
		if err != nil {
			return nil, err
		}
		record.GeometryJSON = string(raw)
		out = append(out, record)
	}
	return out, nil
}

func applyRecords(ctx context.Context, databasePath string, source string, version string, records []areaRecord, now time.Time) (Result, error) {
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
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS area_geometries (
			source TEXT NOT NULL, code TEXT NOT NULL, same_code TEXT NOT NULL,
			name TEXT NOT NULL, name_fr TEXT NOT NULL DEFAULT '', region TEXT NOT NULL DEFAULT '', country TEXT NOT NULL DEFAULT '',
			latitude REAL NOT NULL, longitude REAL NOT NULL,
			min_lon REAL NOT NULL, min_lat REAL NOT NULL, max_lon REAL NOT NULL, max_lat REAL NOT NULL,
			geometry_json TEXT NOT NULL, provider_version TEXT NOT NULL, source_url TEXT NOT NULL, updated_at TEXT NOT NULL,
			PRIMARY KEY (source, code)
		);
		CREATE INDEX IF NOT EXISTS idx_area_geometries_same ON area_geometries(source, same_code);
	`); err != nil {
		return Result{}, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM area_geometries WHERE source = ?", source); err != nil {
		return Result{}, err
	}
	statement, err := tx.PrepareContext(ctx, `
		INSERT INTO area_geometries(
			source, code, same_code, name, name_fr, region, country, latitude, longitude,
			min_lon, min_lat, max_lon, max_lat, geometry_json, provider_version, source_url, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return Result{}, err
	}
	defer statement.Close()
	for _, record := range records {
		if _, err := statement.ExecContext(ctx,
			record.Source, record.Code, record.SameCode, record.Name, record.NameFR, record.Region, record.Country,
			record.Latitude, record.Longitude, record.MinLon, record.MinLat, record.MaxLon, record.MaxLat,
			record.GeometryJSON, record.ProviderVersion, record.SourceURL, importedAt,
		); err != nil {
			return Result{}, err
		}
		kind := "county"
		if source == "clc" {
			kind = "land"
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO places(source, code, name, name_fr, region, country, kind, lat, lon, attrs_json)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')
			ON CONFLICT(source, code) DO UPDATE SET
				name = excluded.name, name_fr = excluded.name_fr, region = excluded.region,
				country = excluded.country, kind = excluded.kind, lat = excluded.lat, lon = excluded.lon
		`, source, record.Code, record.Name, record.NameFR, record.Region, record.Country, kind, record.Latitude, record.Longitude); err != nil {
			return Result{}, err
		}
	}
	metadataPrefix := map[string]string{"clc": "eccc_clc", "nws_same": "nws_county"}[source]
	if metadataPrefix == "" {
		metadataPrefix = source
	}
	for key, value := range map[string]string{
		"area_geometry_schema":           "1",
		metadataPrefix + "_version":      version,
		metadataPrefix + "_record_count": strconv.Itoa(len(records)),
		metadataPrefix + "_source_url":   records[0].SourceURL,
		metadataPrefix + "_imported_at":  importedAt,
	} {
		if _, err := tx.ExecContext(ctx, "INSERT OR REPLACE INTO metadata(key, value) VALUES(?, ?)", key, value); err != nil {
			return Result{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	if err := validateDatabase(ctx, databasePath, source, len(records)); err != nil {
		return Result{}, err
	}
	return Result{Source: source, ProviderVersion: version, RecordCount: len(records), ImportedAt: importedAt}, nil
}

func validateDatabase(ctx context.Context, databasePath string, source string, expected int) error {
	db, err := sql.Open("sqlite", filepath.Clean(databasePath))
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var integrity string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		return fmt.Errorf("candidate location database failed integrity validation: %s", integrity)
	}
	var actual int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM area_geometries
		WHERE source = ? AND same_code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
		  AND min_lon >= -180 AND max_lon <= 180 AND min_lat >= -90 AND max_lat <= 90
		  AND min_lon <= max_lon AND min_lat <= max_lat AND json_valid(geometry_json)
	`, source).Scan(&actual); err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("candidate location database contains %d valid %s geometries, expected %d", actual, source, expected)
	}
	return nil
}
