package locationdb

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

const (
	maxHelloWeatherBridgeRows = 10_000
	maxCityPageNameRows       = 10_000
	helloCityPageLoadTimeout  = 10 * time.Second
	maxHelloBridgeCodeLen     = 32
	maxHelloBridgeNameLen     = 512
	maxHelloBridgeRegionLen   = 32
)

type helloCityPageKey struct {
	name     string
	province string
}

type cityPageNameRecord struct {
	entityPK   int64
	identifier string
	key        helloCityPageKey
}

// LoadHelloWeatherCityPageIDs maps legacy Hello Weather codes to compact-core
// ECCC city-page identifiers using only unique normalized name-and-province
// matches. It never infers a match from coordinates or geometry.
func LoadHelloWeatherCityPageIDs(baseDir string) (map[string]string, bool) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		baseDir = "."
	}
	legacyPath := Path(baseDir)
	corePath := filepath.Clean(filepath.Join(baseDir, populationCoreRelPath))
	return loadHelloWeatherCityPageIDsPaths(legacyPath, corePath)
}

func loadHelloWeatherCityPageIDsPaths(legacyPath string, corePath string) (map[string]string, bool) {
	legacyDB, err := openHelloBridgeDatabase(legacyPath)
	if err != nil {
		return nil, false
	}
	defer legacyDB.Close()

	coreDB, err := openHelloBridgeDatabase(corePath)
	if err != nil {
		return nil, false
	}
	defer coreDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), helloCityPageLoadTimeout)
	defer cancel()
	if err := legacyDB.PingContext(ctx); err != nil {
		return nil, false
	}
	if err := coreDB.PingContext(ctx); err != nil {
		return nil, false
	}

	helloCodes, ok := readUniqueHelloWeatherNameKeys(ctx, legacyDB)
	if !ok {
		return nil, false
	}
	cityPages, ok := readUniqueCityPageNameKeys(ctx, coreDB)
	if !ok {
		return nil, false
	}

	result := make(map[string]string)
	for key, code := range helloCodes {
		if identifier, exists := cityPages[key]; exists {
			result[code] = identifier
		}
	}
	return result, true
}

func openHelloBridgeDatabase(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", helloBridgeReadOnlyDSN(filepath.Clean(path)))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func helloBridgeReadOnlyDSN(path string) string {
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	uriPath := filepath.ToSlash(path)
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	values := url.Values{}
	values.Set("mode", "ro")
	values.Add("_pragma", "query_only(ON)")
	values.Add("_pragma", "temp_store(MEMORY)")
	uri := url.URL{
		Scheme:   "file",
		Path:     uriPath,
		RawQuery: values.Encode(),
	}
	return uri.String()
}

func readUniqueHelloWeatherNameKeys(ctx context.Context, db *sql.DB) (map[helloCityPageKey]string, bool) {
	rows, err := db.QueryContext(ctx, `
		SELECT code, name, region
		FROM places
		WHERE source = ? AND country = ?
		  AND length(CAST(code AS BLOB)) <= ?
		  AND length(CAST(name AS BLOB)) <= ?
		  AND length(CAST(region AS BLOB)) <= ?
		LIMIT ?
	`, "hello_weather", "CA", maxHelloBridgeCodeLen, maxHelloBridgeNameLen,
		maxHelloBridgeRegionLen, maxHelloWeatherBridgeRows+1)
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	unique := make(map[helloCityPageKey]string)
	ambiguous := make(map[helloCityPageKey]struct{})
	count := 0
	for rows.Next() {
		count++
		if count > maxHelloWeatherBridgeRows {
			return nil, false
		}
		var code string
		var name string
		var region string
		if err := rows.Scan(&code, &name, &region); err != nil {
			return nil, false
		}
		if len(code) > maxHelloBridgeCodeLen {
			continue
		}
		code = strings.ToUpper(strings.TrimSpace(code))
		key := normalizedHelloCityPageKey(name, region)
		if code == "" || key.name == "" || key.province == "" {
			continue
		}
		if _, exists := ambiguous[key]; exists {
			continue
		}
		if previous, exists := unique[key]; exists && previous != code {
			delete(unique, key)
			ambiguous[key] = struct{}{}
			continue
		}
		unique[key] = code
	}
	if err := rows.Err(); err != nil {
		return nil, false
	}
	return unique, true
}

func readUniqueCityPageNameKeys(ctx context.Context, db *sql.DB) (map[helloCityPageKey]string, bool) {
	rows, err := db.QueryContext(ctx, `
		SELECT e.entity_pk, i.value, n.name
		FROM identifiers i
		JOIN entities e ON e.entity_pk = i.entity_pk
		JOIN names n ON n.entity_pk = e.entity_pk
		WHERE i.authority = ?
		  AND i.scheme = ?
		  AND COALESCE(e.country, '') = ?
		  AND COALESCE(e.lifecycle_status, '') NOT IN (?, ?)
		  AND length(CAST(i.value AS BLOB)) <= ?
		  AND length(CAST(n.name AS BLOB)) <= ?
		LIMIT ?
	`, "eccc", "eccc_citypage", "CA", "inactive", "retired",
		maxHelloBridgeCodeLen, maxHelloBridgeNameLen, maxCityPageNameRows+1)
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	records := make([]cityPageNameRecord, 0)
	identifierOwners := make(map[string]int64)
	ambiguousIdentifiers := make(map[string]struct{})
	entityIdentifiers := make(map[int64]string)
	ambiguousEntities := make(map[int64]struct{})
	count := 0
	for rows.Next() {
		count++
		if count > maxCityPageNameRows {
			return nil, false
		}
		var record cityPageNameRecord
		var name string
		if err := rows.Scan(&record.entityPK, &record.identifier, &name); err != nil {
			return nil, false
		}
		if len(record.identifier) > maxHelloBridgeCodeLen {
			continue
		}
		record.identifier = strings.ToLower(strings.TrimSpace(record.identifier))
		province := cityPageIdentifierProvince(record.identifier)
		record.key = normalizedHelloCityPageKey(name, province)
		if record.identifier == "" || record.key.name == "" || record.key.province == "" {
			continue
		}
		if previous, exists := identifierOwners[record.identifier]; exists && previous != record.entityPK {
			ambiguousIdentifiers[record.identifier] = struct{}{}
		} else {
			identifierOwners[record.identifier] = record.entityPK
		}
		if previous, exists := entityIdentifiers[record.entityPK]; exists && previous != record.identifier {
			ambiguousEntities[record.entityPK] = struct{}{}
		} else {
			entityIdentifiers[record.entityPK] = record.identifier
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, false
	}

	unique := make(map[helloCityPageKey]cityPageNameRecord)
	ambiguousKeys := make(map[helloCityPageKey]struct{})
	for _, record := range records {
		if _, ambiguous := ambiguousIdentifiers[record.identifier]; ambiguous {
			continue
		}
		if _, ambiguous := ambiguousEntities[record.entityPK]; ambiguous {
			continue
		}
		if _, ambiguous := ambiguousKeys[record.key]; ambiguous {
			continue
		}
		if previous, exists := unique[record.key]; exists && previous.entityPK != record.entityPK {
			delete(unique, record.key)
			ambiguousKeys[record.key] = struct{}{}
			continue
		}
		unique[record.key] = record
	}

	result := make(map[helloCityPageKey]string, len(unique))
	for key, record := range unique {
		result[key] = record.identifier
	}
	return result, true
}

func normalizedHelloCityPageKey(name string, region string) helloCityPageKey {
	if len(name) > maxHelloBridgeNameLen || len(region) > maxHelloBridgeRegionLen {
		return helloCityPageKey{}
	}
	return helloCityPageKey{
		name:     normalizePopulationName(name),
		province: normalizedCanadianProvince(region),
	}
}

func cityPageIdentifierProvince(identifier string) string {
	parts := strings.SplitN(strings.TrimSpace(identifier), "-", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
		return ""
	}
	return normalizedCanadianProvince(parts[0])
}

func normalizedCanadianProvince(value string) string {
	value = normalizePopulationRegion(value)
	switch value {
	case "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT":
		return value
	default:
		return ""
	}
}
