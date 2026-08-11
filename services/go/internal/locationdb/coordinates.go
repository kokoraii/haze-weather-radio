package locationdb

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Coordinate is a validated WGS84 latitude and longitude pair.
type Coordinate struct {
	Latitude  float64
	Longitude float64
}

// LoadCoordinatesByIdentifier loads one deterministic coordinate for each
// identifier in the requested scheme from the compact Canadian core pack.
// The geometry pack is deliberately not opened. Missing or unreadable optional
// core data returns false without modifying the catalog.
func LoadCoordinatesByIdentifier(baseDir string, scheme string) (map[string]Coordinate, bool) {
	scheme = strings.TrimSpace(scheme)
	if scheme == "" {
		return nil, false
	}
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		baseDir = "."
	}
	path := filepath.Clean(filepath.Join(baseDir, "managed", "locations", "ca-weather.sqlite"))
	stat, err := os.Stat(path)
	if err != nil || stat.IsDir() {
		return nil, false
	}
	coordinates, err := readCoordinatesByIdentifier(path, scheme)
	if err != nil {
		return nil, false
	}
	return coordinates, true
}

func readCoordinatesByIdentifier(path string, scheme string) (_ map[string]Coordinate, resultErr error) {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := db.Close(); resultErr == nil && err != nil {
			resultErr = err
		}
	}()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `
		SELECT i.value, g.latitude, g.longitude
		FROM identifiers AS i
		JOIN entities AS e ON e.entity_pk = i.entity_pk
		JOIN geometries AS g ON g.entity_pk = e.entity_pk
		WHERE LOWER(i.scheme) = LOWER(?)
		  AND g.latitude IS NOT NULL
		  AND g.longitude IS NOT NULL
		ORDER BY LOWER(TRIM(i.value)) ASC,
		         CASE WHEN g.is_current = 1 THEN 1 ELSE 0 END DESC,
		         COALESCE(g.valid_from, '') DESC,
		         g.geometry_pk DESC,
		         e.entity_pk ASC,
		         i.identifier_pk ASC
	`, scheme)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); resultErr == nil && err != nil {
			resultErr = err
		}
	}()

	coordinates := map[string]Coordinate{}
	for rows.Next() {
		var identifier string
		var coordinate Coordinate
		if err := rows.Scan(&identifier, &coordinate.Latitude, &coordinate.Longitude); err != nil {
			return nil, err
		}
		key := strings.ToLower(strings.TrimSpace(identifier))
		if key == "" || !validLoadedCoordinate(coordinate) {
			continue
		}
		if _, exists := coordinates[key]; exists {
			continue
		}
		coordinates[key] = coordinate
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return coordinates, nil
}

func validLoadedCoordinate(coordinate Coordinate) bool {
	return !math.IsNaN(coordinate.Latitude) &&
		!math.IsNaN(coordinate.Longitude) &&
		!math.IsInf(coordinate.Latitude, 0) &&
		!math.IsInf(coordinate.Longitude, 0) &&
		coordinate.Latitude >= -90 && coordinate.Latitude <= 90 &&
		coordinate.Longitude >= -180 && coordinate.Longitude <= 180
}
