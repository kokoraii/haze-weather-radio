package locationdb

import (
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const DefaultRelPath = "managed/alert_location_map.sqlite"

type Place struct {
	Source  string
	Code    string
	Name    string
	NameFR  string
	Region  string
	Country string
	Kind    string
	Lat     float64
	Lon     float64
	Attrs   map[string]any
}

type Link struct {
	Type       string
	FromSource string
	FromCode   string
	ToSource   string
	ToCode     string
	Score      float64
	Confidence string
	Method     string
	Components map[string]any
}

// StationLink associates a weather area with the observation station selected
// for that area by the managed location database.
type StationLink struct {
	AreaSource  string
	AreaCode    string
	StationID   string
	StationName string
	DistanceKM  float64
}

type Snapshot struct {
	Places       []Place
	Links        []Link
	StationLinks []StationLink

	bySourceCode   map[string]map[string]Place
	linksFrom      map[string][]Link
	linksTo        map[string][]Link
	stationsByArea map[string]StationLink
	stationsByID   map[string][]StationLink
}

// Aliases returns the caller-facing aliases stored in attrs_json.
func (p Place) Aliases() []string {
	raw, ok := p.Attrs["aliases"]
	if !ok {
		return nil
	}
	seen := map[string]struct{}{}
	out := []string{}
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		key := strings.ToLower(value)
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	switch values := raw.(type) {
	case []any:
		for _, value := range values {
			if text, ok := value.(string); ok {
				add(text)
			}
		}
	case []string:
		for _, value := range values {
			add(value)
		}
	case string:
		add(values)
	}
	return out
}

type cachedSnapshot struct {
	mtime time.Time
	size  int64
	snap  Snapshot
}

var cache = struct {
	sync.Mutex
	byPath map[string]cachedSnapshot
}{byPath: map[string]cachedSnapshot{}}

func Path(baseDir string) string {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		baseDir = "."
	}
	return filepath.Clean(filepath.Join(baseDir, DefaultRelPath))
}

func Load(baseDir string) (Snapshot, bool) {
	return LoadPath(Path(baseDir))
}

func LoadPath(path string) (Snapshot, bool) {
	path = filepath.Clean(path)
	stat, err := os.Stat(path)
	if err != nil || stat.IsDir() {
		return Snapshot{}, false
	}
	cache.Lock()
	if cached, ok := cache.byPath[path]; ok && cached.mtime.Equal(stat.ModTime()) && cached.size == stat.Size() {
		cache.Unlock()
		return cached.snap, true
	}
	cache.Unlock()

	snap, err := readSnapshot(path)
	if err != nil {
		return Snapshot{}, false
	}
	cache.Lock()
	cache.byPath[path] = cachedSnapshot{mtime: stat.ModTime(), size: stat.Size(), snap: snap}
	cache.Unlock()
	return snap, true
}

func (s Snapshot) Place(source string, code string) (Place, bool) {
	if len(s.bySourceCode) == 0 {
		s.bySourceCode = indexPlaces(s.Places)
	}
	source = strings.ToLower(strings.TrimSpace(source))
	code = strings.ToUpper(strings.TrimSpace(code))
	if byCode := s.bySourceCode[source]; byCode != nil {
		place, ok := byCode[code]
		return place, ok
	}
	return Place{}, false
}

func (s Snapshot) PlacesBySource(source string) []Place {
	source = strings.ToLower(strings.TrimSpace(source))
	out := []Place{}
	for _, place := range s.Places {
		if strings.EqualFold(place.Source, source) {
			out = append(out, place)
		}
	}
	return out
}

// LinkedCode returns the highest-quality source-qualified outgoing link.
func (s Snapshot) LinkedCode(fromSource string, fromCode string, toSource string) string {
	s.ensureRelationshipIndexes()
	key := relationshipKey(fromSource, fromCode)
	toSource = strings.ToLower(strings.TrimSpace(toSource))
	bestCode := ""
	bestScore := -1.0
	for _, link := range s.linksFrom[key] {
		if link.ToSource == toSource && (link.Score > bestScore || link.Score == bestScore && link.ToCode < bestCode) {
			bestCode, bestScore = link.ToCode, link.Score
		}
	}
	return bestCode
}

// ReverseLinkedCode returns the highest-quality incoming link from a source.
func (s Snapshot) ReverseLinkedCode(toSource string, toCode string, fromSource string) string {
	s.ensureRelationshipIndexes()
	key := relationshipKey(toSource, toCode)
	fromSource = strings.ToLower(strings.TrimSpace(fromSource))
	bestCode := ""
	bestScore := -1.0
	for _, link := range s.linksTo[key] {
		if link.FromSource == fromSource && (link.Score > bestScore || link.Score == bestScore && link.FromCode < bestCode) {
			bestCode, bestScore = link.FromCode, link.Score
		}
	}
	return bestCode
}

// StationForArea returns the observation station linked to one qualified area.
func (s Snapshot) StationForArea(source string, code string) string {
	s.ensureRelationshipIndexes()
	return s.stationsByArea[relationshipKey(source, code)].StationID
}

// StationAreas returns the small set of areas served by a station.
func (s Snapshot) StationAreas(stationID string) []StationLink {
	s.ensureRelationshipIndexes()
	return s.stationsByID[strings.ToUpper(strings.TrimSpace(stationID))]
}

func (s *Snapshot) ensureRelationshipIndexes() {
	if s.linksFrom != nil && s.linksTo != nil && s.stationsByArea != nil && s.stationsByID != nil {
		return
	}
	s.linksFrom = map[string][]Link{}
	s.linksTo = map[string][]Link{}
	for _, link := range s.Links {
		s.linksFrom[relationshipKey(link.FromSource, link.FromCode)] = append(s.linksFrom[relationshipKey(link.FromSource, link.FromCode)], link)
		s.linksTo[relationshipKey(link.ToSource, link.ToCode)] = append(s.linksTo[relationshipKey(link.ToSource, link.ToCode)], link)
	}
	s.stationsByArea = map[string]StationLink{}
	s.stationsByID = map[string][]StationLink{}
	for _, link := range s.StationLinks {
		s.stationsByArea[relationshipKey(link.AreaSource, link.AreaCode)] = link
		s.stationsByID[link.StationID] = append(s.stationsByID[link.StationID], link)
	}
}

func relationshipKey(source string, code string) string {
	return strings.ToLower(strings.TrimSpace(source)) + "\x00" + strings.ToUpper(strings.TrimSpace(code))
}

func (s Snapshot) Labels() map[string]string {
	out := map[string]string{}
	for _, source := range []string{"forecast", "clc", "sgc", "hello_weather", "nws_same", "nws_zone", "nws_marine_same", "nws_marine_zone"} {
		for _, place := range s.PlacesBySource(source) {
			if place.Code != "" && place.Name != "" {
				out[place.Code] = place.Name
			}
		}
	}
	return out
}

func readSnapshot(path string) (Snapshot, error) {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return Snapshot{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	_, _ = db.Exec("PRAGMA query_only=ON")

	places, err := readPlaces(db)
	if err != nil {
		return Snapshot{}, err
	}
	links, err := readLinks(db)
	if err != nil {
		return Snapshot{}, err
	}
	stationLinks, err := readStationLinks(db)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{
		Places:       places,
		Links:        links,
		StationLinks: stationLinks,
		bySourceCode: indexPlaces(places),
	}
	snapshot.ensureRelationshipIndexes()
	return snapshot, nil
}

func sqliteReadOnlyDSN(path string) string {
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

func readPlaces(db *sql.DB) ([]Place, error) {
	rows, err := db.Query(`
		SELECT source, code, name, name_fr, region, country, kind,
		       COALESCE(lat, 0), COALESCE(lon, 0), attrs_json
		FROM places
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Place{}
	for rows.Next() {
		var place Place
		var attrsRaw string
		if err := rows.Scan(&place.Source, &place.Code, &place.Name, &place.NameFR, &place.Region, &place.Country, &place.Kind, &place.Lat, &place.Lon, &attrsRaw); err != nil {
			return nil, err
		}
		place.Source = strings.ToLower(strings.TrimSpace(place.Source))
		place.Code = strings.ToUpper(strings.TrimSpace(place.Code))
		place.Attrs = map[string]any{}
		_ = json.Unmarshal([]byte(attrsRaw), &place.Attrs)
		out = append(out, place)
	}
	return out, rows.Err()
}

func readLinks(db *sql.DB) ([]Link, error) {
	rows, err := db.Query(`
		SELECT link_type, from_source, from_code, to_source, to_code,
		       score, confidence, method, components_json
		FROM links
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Link{}
	for rows.Next() {
		var link Link
		var componentsRaw string
		if err := rows.Scan(&link.Type, &link.FromSource, &link.FromCode, &link.ToSource, &link.ToCode, &link.Score, &link.Confidence, &link.Method, &componentsRaw); err != nil {
			return nil, err
		}
		link.Type = strings.ToLower(strings.TrimSpace(link.Type))
		link.FromSource = strings.ToLower(strings.TrimSpace(link.FromSource))
		link.ToSource = strings.ToLower(strings.TrimSpace(link.ToSource))
		link.FromCode = strings.ToUpper(strings.TrimSpace(link.FromCode))
		link.ToCode = strings.ToUpper(strings.TrimSpace(link.ToCode))
		link.Components = map[string]any{}
		_ = json.Unmarshal([]byte(componentsRaw), &link.Components)
		out = append(out, link)
	}
	return out, rows.Err()
}

func readStationLinks(db *sql.DB) ([]StationLink, error) {
	rows, err := db.Query(`
		SELECT area_source, area_code, station_id, station_name, distance_km
		FROM station_links
	`)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table") {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	out := []StationLink{}
	for rows.Next() {
		var link StationLink
		if err := rows.Scan(&link.AreaSource, &link.AreaCode, &link.StationID, &link.StationName, &link.DistanceKM); err != nil {
			return nil, err
		}
		link.AreaSource = strings.ToLower(strings.TrimSpace(link.AreaSource))
		link.AreaCode = strings.ToUpper(strings.TrimSpace(link.AreaCode))
		link.StationID = strings.ToUpper(strings.TrimSpace(link.StationID))
		link.StationName = strings.TrimSpace(link.StationName)
		out = append(out, link)
	}
	return out, rows.Err()
}

func indexPlaces(places []Place) map[string]map[string]Place {
	out := map[string]map[string]Place{}
	for _, place := range places {
		source := strings.ToLower(strings.TrimSpace(place.Source))
		code := strings.ToUpper(strings.TrimSpace(place.Code))
		if source == "" || code == "" {
			continue
		}
		if out[source] == nil {
			out[source] = map[string]Place{}
		}
		out[source][code] = place
	}
	return out
}
