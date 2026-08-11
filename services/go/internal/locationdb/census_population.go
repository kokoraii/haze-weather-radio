package locationdb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	maxCensusPopulationRows          = 10_000
	maxCensusPopulationNameRows      = 20_000
	maxCensusPopulationCityPageRows  = 10_000
	maxCensusPopulationCanonicalID   = 512
	maxCensusPopulationAttributeSize = 64 * 1024
	maxCensusPopulationNameLength    = 512
	maxCensusPopulationIdentifierLen = 128
	maxCensusPopulationRegionLength  = 32
	censusPopulationLoadTimeout      = 5 * time.Second
)

const statsCanCensusPopulationSourcePattern = "statcan-csd-population-%"

// CensusPopulation is a validated Statistics Canada census population for one
// canonical location. CensusYear is zero when a valid population record does
// not provide a usable census year.
type CensusPopulation struct {
	Population int64
	CensusYear int
}

// CensusPopulationCatalog contains official Statistics Canada census data
// indexed both by the census entity's canonical ID and, where a unique match
// exists, by an ECCC city-page identifier.
type CensusPopulationCatalog struct {
	ByCanonicalID map[string]CensusPopulation
	ByCityPageID  map[string]CensusPopulation
}

type censusPopulationRecord struct {
	EntityPK    int64
	CanonicalID string
	Population  CensusPopulation
}

type censusPopulationNameKey struct {
	name     string
	province string
}

type censusCityPageNameRecord struct {
	entityPK   int64
	identifier string
	key        censusPopulationNameKey
}

// LoadCensusPopulationCatalog loads official Statistics Canada census
// subdivision populations from the compact Canadian core pack. It only admits
// active Canadian administrative areas with a source-qualified Statistics
// Canada SGC or DGUID identifier, a statcan-csd-population source, and a
// positive integral census_population value. City-page mappings require an
// active primary ECCC forecast_location identifier and one unique normalized
// name-and-province match, with the province derived from that identifier. The
// geometry pack is never opened.
//
// Missing, unreadable, oversized, or malformed core data returns a zero
// catalog and false. Individual malformed rows are skipped so that one bad
// catalog record does not hide otherwise valid census data. Conflicting
// canonical IDs, names, and city-page IDs are omitted rather than selected
// arbitrarily.
func LoadCensusPopulationCatalog(baseDir string) (CensusPopulationCatalog, bool) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		baseDir = "."
	}
	path := filepath.Clean(filepath.Join(baseDir, populationCoreRelPath))
	stat, err := os.Stat(path)
	if err != nil || stat.IsDir() {
		return CensusPopulationCatalog{}, false
	}
	catalog, err := readCensusPopulationCatalog(path)
	if err != nil {
		return CensusPopulationCatalog{}, false
	}
	return catalog, true
}

// LoadCensusPopulationByCanonicalID loads official Statistics Canada census
// population data keyed by canonical location ID. It is a convenience view of
// LoadCensusPopulationCatalog for callers that do not need city-page matches.
func LoadCensusPopulationByCanonicalID(baseDir string) (map[string]CensusPopulation, bool) {
	catalog, ok := LoadCensusPopulationCatalog(baseDir)
	if !ok {
		return nil, false
	}
	return catalog.ByCanonicalID, true
}

// LoadCensusPopulationByCityPageID loads official Statistics Canada census
// population data keyed by normalized ECCC city-page ID. Only unambiguous
// name-and-province matches are included.
func LoadCensusPopulationByCityPageID(baseDir string) (map[string]CensusPopulation, bool) {
	catalog, ok := LoadCensusPopulationCatalog(baseDir)
	if !ok {
		return nil, false
	}
	return catalog.ByCityPageID, true
}

func readCensusPopulationCatalog(path string) (_ CensusPopulationCatalog, resultErr error) {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return CensusPopulationCatalog{}, err
	}
	defer func() {
		if err := db.Close(); resultErr == nil && err != nil {
			resultErr = err
		}
	}()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), censusPopulationLoadTimeout)
	defer cancel()
	records, byCanonicalID, err := readCensusPopulationRecords(ctx, db)
	if err != nil {
		return CensusPopulationCatalog{}, err
	}
	byName, err := readCensusPopulationNameIndex(ctx, db, records)
	if err != nil {
		return CensusPopulationCatalog{}, err
	}
	byCityPageID, err := readCensusPopulationCityPageIndex(ctx, db, byName)
	if err != nil {
		return CensusPopulationCatalog{}, err
	}
	return CensusPopulationCatalog{
		ByCanonicalID: byCanonicalID,
		ByCityPageID:  byCityPageID,
	}, nil
}

func readCensusPopulationRecords(ctx context.Context, db *sql.DB) (map[int64]censusPopulationRecord, map[string]CensusPopulation, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT e.entity_pk, e.canonical_id, e.attributes_json
		FROM entities AS e
		WHERE e.kind = ?
		  AND UPPER(COALESCE(e.country, '')) = ?
		  AND LOWER(COALESCE(e.lifecycle_status, '')) NOT IN (?, ?)
		  AND LOWER(COALESCE(e.attributes_json, '')) LIKE ?
		  AND length(CAST(e.canonical_id AS BLOB)) <= ?
		  AND length(CAST(e.attributes_json AS BLOB)) <= ?
		  AND EXISTS (
			  SELECT 1
			  FROM identifiers AS statcan_identifier
			  WHERE statcan_identifier.entity_pk = e.entity_pk
			    AND LOWER(statcan_identifier.authority) = ?
			    AND LOWER(statcan_identifier.scheme) IN (?, ?)
			    AND length(TRIM(statcan_identifier.value)) > 0
		  )
		ORDER BY canonical_id ASC, entity_pk ASC
		LIMIT ?
	`, "administrative_area", "CA", "inactive", "retired", `%"population_source"%`+statsCanCensusPopulationSourcePattern,
		maxCensusPopulationCanonicalID, maxCensusPopulationAttributeSize,
		"statcan", "sgc", "sgc_dguid", maxCensusPopulationRows+1)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	byEntity := make(map[int64]censusPopulationRecord)
	byCanonicalID := make(map[string]CensusPopulation)
	canonicalOwners := make(map[string]int64)
	ambiguousCanonicalIDs := make(map[string]struct{})
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount > maxCensusPopulationRows {
			return nil, nil, errors.New("census population catalog exceeds row limit")
		}
		var entityPK int64
		var canonicalID string
		var attributes string
		if err := rows.Scan(&entityPK, &canonicalID, &attributes); err != nil {
			return nil, nil, err
		}
		canonicalID = strings.TrimSpace(canonicalID)
		if entityPK <= 0 || canonicalID == "" || len(canonicalID) > maxCensusPopulationCanonicalID {
			continue
		}
		population, ok := parseStatsCanCensusPopulation(attributes)
		if !ok {
			continue
		}
		if _, duplicate := ambiguousCanonicalIDs[canonicalID]; duplicate {
			continue
		}
		if previousEntityPK, exists := canonicalOwners[canonicalID]; exists && previousEntityPK != entityPK {
			delete(byEntity, previousEntityPK)
			delete(byCanonicalID, canonicalID)
			ambiguousCanonicalIDs[canonicalID] = struct{}{}
			continue
		}
		canonicalOwners[canonicalID] = entityPK
		byEntity[entityPK] = censusPopulationRecord{
			EntityPK: entityPK, CanonicalID: canonicalID, Population: population,
		}
		byCanonicalID[canonicalID] = population
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return byEntity, byCanonicalID, nil
}

func readCensusPopulationNameIndex(ctx context.Context, db *sql.DB, byEntity map[int64]censusPopulationRecord) (map[censusPopulationNameKey]censusPopulationRecord, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT e.entity_pk, e.region, n.name
		FROM entities AS e
		JOIN names AS n ON n.entity_pk = e.entity_pk
		WHERE e.kind = ?
		  AND UPPER(COALESCE(e.country, '')) = ?
		  AND LOWER(COALESCE(e.lifecycle_status, '')) NOT IN (?, ?)
		  AND LOWER(COALESCE(e.attributes_json, '')) LIKE ?
		  AND n.is_primary = ?
		  AND length(CAST(e.region AS BLOB)) <= ?
		  AND length(CAST(n.name AS BLOB)) <= ?
		  AND EXISTS (
			  SELECT 1
			  FROM identifiers AS statcan_identifier
			  WHERE statcan_identifier.entity_pk = e.entity_pk
			    AND LOWER(statcan_identifier.authority) = ?
			    AND LOWER(statcan_identifier.scheme) IN (?, ?)
			    AND length(TRIM(statcan_identifier.value)) > 0
		  )
		ORDER BY e.entity_pk ASC, n.name_pk ASC
		LIMIT ?
	`, "administrative_area", "CA", "inactive", "retired", `%"population_source"%`+statsCanCensusPopulationSourcePattern,
		1, maxCensusPopulationRegionLength, maxCensusPopulationNameLength,
		"statcan", "sgc", "sgc_dguid", maxCensusPopulationNameRows+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byName := make(map[censusPopulationNameKey]censusPopulationRecord)
	ambiguousNames := make(map[censusPopulationNameKey]struct{})
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount > maxCensusPopulationNameRows {
			return nil, errors.New("census population name catalog exceeds row limit")
		}
		var entityPK int64
		var region string
		var name string
		if err := rows.Scan(&entityPK, &region, &name); err != nil {
			return nil, err
		}
		candidate, exists := byEntity[entityPK]
		if !exists {
			continue
		}
		key := newCensusPopulationNameKey(name, region)
		if key.name == "" || key.province == "" {
			continue
		}
		if _, ambiguous := ambiguousNames[key]; ambiguous {
			continue
		}
		if previous, exists := byName[key]; exists && previous.EntityPK != candidate.EntityPK {
			delete(byName, key)
			ambiguousNames[key] = struct{}{}
			continue
		}
		byName[key] = candidate
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return byName, nil
}

func readCensusPopulationCityPageIndex(ctx context.Context, db *sql.DB, byName map[censusPopulationNameKey]censusPopulationRecord) (map[string]CensusPopulation, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT e.entity_pk, i.value, n.name
		FROM identifiers AS i
		JOIN entities AS e ON e.entity_pk = i.entity_pk
		JOIN names AS n ON n.entity_pk = e.entity_pk
		WHERE LOWER(i.authority) = ?
		  AND LOWER(i.scheme) = ?
		  AND i.is_primary = ?
		  AND n.is_primary = ?
		  AND e.kind = ?
		  AND UPPER(COALESCE(e.country, '')) = ?
		  AND LOWER(COALESCE(e.lifecycle_status, '')) NOT IN (?, ?)
		  AND length(CAST(i.value AS BLOB)) <= ?
		  AND length(CAST(n.name AS BLOB)) <= ?
		ORDER BY LOWER(i.value) ASC, e.entity_pk ASC, n.name_pk ASC
		LIMIT ?
	`, "eccc", "eccc_citypage", 1, 1, "forecast_location", "CA", "inactive", "retired",
		maxCensusPopulationIdentifierLen, maxCensusPopulationNameLength, maxCensusPopulationCityPageRows+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]censusCityPageNameRecord, 0)
	identifierOwners := make(map[string]int64)
	ambiguousIdentifiers := make(map[string]struct{})
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount > maxCensusPopulationCityPageRows {
			return nil, errors.New("census population city-page catalog exceeds row limit")
		}
		var record censusCityPageNameRecord
		var name string
		if err := rows.Scan(&record.entityPK, &record.identifier, &name); err != nil {
			return nil, err
		}
		record.identifier = strings.ToLower(strings.TrimSpace(record.identifier))
		if record.entityPK <= 0 || record.identifier == "" || len(record.identifier) > maxCensusPopulationIdentifierLen {
			continue
		}
		if previousEntityPK, exists := identifierOwners[record.identifier]; exists && previousEntityPK != record.entityPK {
			ambiguousIdentifiers[record.identifier] = struct{}{}
		} else {
			identifierOwners[record.identifier] = record.entityPK
		}
		record.key = newCensusPopulationNameKey(name, cityPageIdentifierProvince(record.identifier))
		if record.key.name == "" || record.key.province == "" {
			continue
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	byCityPageID := make(map[string]CensusPopulation)
	matchedEntities := make(map[string]int64)
	ambiguousMatches := make(map[string]struct{})
	for _, record := range records {
		if _, ambiguous := ambiguousIdentifiers[record.identifier]; ambiguous {
			continue
		}
		candidate, exists := byName[record.key]
		if !exists {
			continue
		}
		if _, ambiguous := ambiguousMatches[record.identifier]; ambiguous {
			continue
		}
		if previousEntityPK, exists := matchedEntities[record.identifier]; exists && previousEntityPK != candidate.EntityPK {
			delete(byCityPageID, record.identifier)
			ambiguousMatches[record.identifier] = struct{}{}
			continue
		}
		matchedEntities[record.identifier] = candidate.EntityPK
		byCityPageID[record.identifier] = candidate.Population
	}
	return byCityPageID, nil
}

func newCensusPopulationNameKey(name string, province string) censusPopulationNameKey {
	if len(name) > maxCensusPopulationNameLength || len(province) > maxCensusPopulationRegionLength {
		return censusPopulationNameKey{}
	}
	return censusPopulationNameKey{
		name:     normalizePopulationName(name),
		province: normalizedCanadianProvince(province),
	}
}

func parseStatsCanCensusPopulation(attributes string) (CensusPopulation, bool) {
	if len(attributes) == 0 || len(attributes) > maxCensusPopulationAttributeSize {
		return CensusPopulation{}, false
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal([]byte(attributes), &values); err != nil {
		return CensusPopulation{}, false
	}
	source, ok := jsonRawString(values["population_source"])
	if !ok || !strings.HasPrefix(strings.ToLower(source), "statcan-csd-population-") {
		return CensusPopulation{}, false
	}
	population, ok := jsonRawPositiveInt64(values["census_population"])
	if !ok {
		return CensusPopulation{}, false
	}
	result := CensusPopulation{Population: population}
	if year, ok := jsonRawPositiveInt64(values["census_year"]); ok && year <= math.MaxInt32 {
		result.CensusYear = int(year)
	}
	return result, true
}

func jsonRawString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	value = strings.TrimSpace(value)
	return value, value != ""
}

func jsonRawPositiveInt64(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var rawValue any
	if err := decoder.Decode(&rawValue); err != nil {
		return 0, false
	}
	var text string
	switch value := rawValue.(type) {
	case json.Number:
		text = value.String()
	case string:
		text = strings.TrimSpace(value)
	default:
		return 0, false
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	return parsed, err == nil && parsed > 0
}
