package locationdb

import (
	"database/sql"
	"math"
	"path/filepath"
	"sort"
	"strings"
)

const CapabilityForecast = "forecast_location"
const CapabilityObservation = "weather_station"
const CapabilityAirQuality = "air_quality_station"
const CapabilityHydrometric = "hydrometric_station"
const CapabilityMarineForecast = "marine_forecast_zone"
const CapabilityMarineObservation = "marine_station"
const CapabilityClimate = "climate_station"

// CapabilityLocation is one provider target that can render a specific class
// of weather product for a nearby caller-selected place.
type CapabilityLocation struct {
	CanonicalID string
	Kind        string
	ID          string
	Name        string
	NameFR      string
	Region      string
	Country     string
	Latitude    float64
	Longitude   float64
}

// CapabilityMatch pairs a provider target with its great-circle distance from
// the caller-selected place.
type CapabilityMatch struct {
	Location   CapabilityLocation
	DistanceKM float64
}

// CapabilityCatalog is an immutable in-memory view of product-capable points
// from the compact location core. Geometry blobs are never opened by the IVR.
type CapabilityCatalog struct {
	byKind map[string][]CapabilityLocation
}

// NewCapabilityCatalog builds an immutable capability catalog from records.
func NewCapabilityCatalog(records []CapabilityLocation) *CapabilityCatalog {
	catalog := &CapabilityCatalog{byKind: make(map[string][]CapabilityLocation)}
	for _, record := range records {
		record.Kind = strings.ToLower(strings.TrimSpace(record.Kind))
		record.ID = strings.TrimSpace(record.ID)
		if record.Kind == "" || record.ID == "" || !validCoordinate(record.Latitude, record.Longitude) {
			continue
		}
		record.Region = normalizeCapabilityRegion(record.Region)
		record.Country = strings.ToUpper(strings.TrimSpace(record.Country))
		catalog.byKind[record.Kind] = append(catalog.byKind[record.Kind], record)
	}
	for kind := range catalog.byKind {
		sort.SliceStable(catalog.byKind[kind], func(i int, j int) bool {
			left := catalog.byKind[kind][i]
			right := catalog.byKind[kind][j]
			if left.ID != right.ID {
				return left.ID < right.ID
			}
			return left.CanonicalID < right.CanonicalID
		})
	}
	return catalog
}

// LoadCapabilityCatalog loads product-capable points from the optional compact
// Canadian core pack. It returns false when the pack is unavailable or invalid.
func LoadCapabilityCatalog(baseDir string) (*CapabilityCatalog, bool) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		baseDir = "."
	}
	path := filepath.Clean(filepath.Join(baseDir, populationCoreRelPath))
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return nil, false
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	records, err := readCapabilityLocations(db)
	if err != nil || len(records) == 0 {
		return nil, false
	}
	return NewCapabilityCatalog(records), true
}

// Count returns the number of indexed targets for one capability kind.
func (c *CapabilityCatalog) Count(kind string) int {
	if c == nil {
		return 0
	}
	return len(c.byKind[strings.ToLower(strings.TrimSpace(kind))])
}

// Find returns one source-qualified capability target by its provider ID.
func (c *CapabilityCatalog) Find(kind string, id string) (CapabilityLocation, bool) {
	if c == nil {
		return CapabilityLocation{}, false
	}
	id = strings.TrimSpace(id)
	for _, candidate := range c.byKind[strings.ToLower(strings.TrimSpace(kind))] {
		if strings.EqualFold(candidate.ID, id) {
			return candidate, true
		}
	}
	return CapabilityLocation{}, false
}

// Nearest returns up to limit nearby targets. Same-province targets receive a
// modest ordering preference, while physical distance remains authoritative.
func (c *CapabilityCatalog) Nearest(kind string, latitude float64, longitude float64, region string, limit int, maxDistanceKM float64) []CapabilityMatch {
	if c == nil || limit <= 0 || !validCoordinate(latitude, longitude) {
		return nil
	}
	region = normalizeCapabilityRegion(region)
	type rankedMatch struct {
		match CapabilityMatch
		score float64
	}
	ranked := make([]rankedMatch, 0, minCapabilityInt(limit*4, 64))
	for _, candidate := range c.byKind[strings.ToLower(strings.TrimSpace(kind))] {
		distance := capabilityDistanceKM(latitude, longitude, candidate.Latitude, candidate.Longitude)
		if maxDistanceKM > 0 && distance > maxDistanceKM {
			continue
		}
		score := distance
		if region != "" && candidate.Region != "" && candidate.Region != region {
			score += 40
		}
		ranked = append(ranked, rankedMatch{
			match: CapabilityMatch{Location: candidate, DistanceKM: distance},
			score: score,
		})
	}
	sort.SliceStable(ranked, func(i int, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score < ranked[j].score
		}
		if ranked[i].match.DistanceKM != ranked[j].match.DistanceKM {
			return ranked[i].match.DistanceKM < ranked[j].match.DistanceKM
		}
		return ranked[i].match.Location.ID < ranked[j].match.Location.ID
	})
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	out := make([]CapabilityMatch, len(ranked))
	for index := range ranked {
		out[index] = ranked[index].match
	}
	return out
}

func readCapabilityLocations(db *sql.DB) ([]CapabilityLocation, error) {
	rows, err := db.Query(`
		SELECT e.entity_pk, e.canonical_id, e.kind, COALESCE(e.country, ''),
		       COALESCE(e.region, ''), COALESCE(e.lifecycle_status, ''),
		       g.latitude, g.longitude
		FROM entities e
		JOIN geometries g ON g.entity_pk = e.entity_pk AND g.is_current = 1
		WHERE e.kind IN (?, ?, ?, ?, ?, ?, ?)
		  AND LOWER(COALESCE(e.lifecycle_status, '')) <> 'inactive'
		  AND (e.kind <> 'climate_station'
		       OR SUBSTR(COALESCE(json_extract(e.attributes_json, '$.LAST_DATE'), ''), 1, 10) >= date('now', '-730 days'))
		  AND g.latitude IS NOT NULL AND g.longitude IS NOT NULL
	`, CapabilityForecast, CapabilityObservation, CapabilityAirQuality,
		CapabilityHydrometric, CapabilityMarineForecast, CapabilityMarineObservation, CapabilityClimate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type capabilityRow struct {
		location CapabilityLocation
		idScore  int
	}
	byEntity := make(map[int64]*capabilityRow)
	order := make([]int64, 0)
	for rows.Next() {
		var entityPK int64
		var lifecycle string
		row := &capabilityRow{idScore: math.MaxInt}
		if err := rows.Scan(&entityPK, &row.location.CanonicalID, &row.location.Kind,
			&row.location.Country, &row.location.Region, &lifecycle,
			&row.location.Latitude, &row.location.Longitude); err != nil {
			return nil, err
		}
		if _, exists := byEntity[entityPK]; exists {
			continue
		}
		byEntity[entityPK] = row
		order = append(order, entityPK)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	nameRows, err := db.Query(`
		SELECT entity_pk, COALESCE(locale, ''), name, is_primary
		FROM names
		ORDER BY entity_pk, is_primary DESC, name_pk
	`)
	if err != nil {
		return nil, err
	}
	for nameRows.Next() {
		var entityPK int64
		var locale string
		var name string
		var primary int
		if err := nameRows.Scan(&entityPK, &locale, &name, &primary); err != nil {
			nameRows.Close()
			return nil, err
		}
		row := byEntity[entityPK]
		if row == nil {
			continue
		}
		locale = strings.ToLower(strings.TrimSpace(locale))
		if strings.HasPrefix(locale, "fr") {
			if row.location.NameFR == "" || primary != 0 {
				row.location.NameFR = strings.TrimSpace(name)
			}
			continue
		}
		if row.location.Name == "" || primary != 0 {
			row.location.Name = strings.TrimSpace(name)
		}
	}
	if err := nameRows.Close(); err != nil {
		return nil, err
	}
	if err := nameRows.Err(); err != nil {
		return nil, err
	}

	identifierRows, err := db.Query(`
		SELECT entity_pk, LOWER(scheme), value, is_primary
		FROM identifiers
		ORDER BY entity_pk, is_primary DESC, identifier_pk
	`)
	if err != nil {
		return nil, err
	}
	for identifierRows.Next() {
		var entityPK int64
		var scheme string
		var value string
		var primary int
		if err := identifierRows.Scan(&entityPK, &scheme, &value, &primary); err != nil {
			identifierRows.Close()
			return nil, err
		}
		row := byEntity[entityPK]
		if row == nil {
			continue
		}
		score := capabilityIdentifierPriority(row.location.Kind, scheme, primary != 0)
		if score < row.idScore {
			row.location.ID = strings.TrimSpace(value)
			row.idScore = score
		}
	}
	if err := identifierRows.Close(); err != nil {
		return nil, err
	}
	if err := identifierRows.Err(); err != nil {
		return nil, err
	}

	out := make([]CapabilityLocation, 0, len(order))
	for _, entityPK := range order {
		row := byEntity[entityPK]
		if row == nil || row.location.ID == "" {
			continue
		}
		if row.location.Kind == CapabilityForecast {
			if prefix, _, ok := strings.Cut(strings.ToUpper(row.location.ID), "-"); ok && len(prefix) == 2 {
				row.location.Region = prefix
			}
		}
		if row.location.Name == "" {
			row.location.Name = row.location.NameFR
		}
		if row.location.Name == "" {
			row.location.Name = row.location.ID
		}
		out = append(out, row.location)
	}
	return out, nil
}

func capabilityIdentifierPriority(kind string, scheme string, primary bool) int {
	scheme = strings.ToLower(strings.TrimSpace(scheme))
	priorities := map[string][]string{
		CapabilityForecast:          {"eccc_citypage"},
		CapabilityObservation:       {"eccc_station", "msc", "wmo"},
		CapabilityAirQuality:        {"aqhi"},
		CapabilityHydrometric:       {"hydrometric"},
		CapabilityMarineForecast:    {"marine", "clc"},
		CapabilityMarineObservation: {"eccc_station", "msc", "wmo"},
		CapabilityClimate:           {"climate"},
	}
	for index, candidate := range priorities[strings.ToLower(strings.TrimSpace(kind))] {
		if scheme == candidate {
			return index
		}
	}
	if primary {
		return 100
	}
	return math.MaxInt
}

func normalizeCapabilityRegion(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	if len(value) == 2 {
		return value
	}
	return ""
}

func validCoordinate(latitude float64, longitude float64) bool {
	return !math.IsNaN(latitude) && !math.IsNaN(longitude) &&
		latitude >= -90 && latitude <= 90 && longitude >= -180 && longitude <= 180 &&
		(latitude != 0 || longitude != 0)
}

func capabilityDistanceKM(lat1 float64, lon1 float64, lat2 float64, lon2 float64) float64 {
	const earthRadiusKM = 6371.0088
	toRadians := math.Pi / 180
	lat1 *= toRadians
	lon1 *= toRadians
	lat2 *= toRadians
	lon2 *= toRadians
	dLat := lat2 - lat1
	dLon := lon2 - lon1
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusKM * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func minCapabilityInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}
