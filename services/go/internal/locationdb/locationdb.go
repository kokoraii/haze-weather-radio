package locationdb

import (
	"database/sql"
	"encoding/json"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"
	_ "modernc.org/sqlite"
)

const DefaultRelPath = "managed/alert_location_map.sqlite"

const populationCoreRelPath = "managed/locations/ca-weather.sqlite"

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

// CensusPopulation returns the largest positive population estimate attached
// to a place and the optional Census year that describes it. Location catalog
// builders may use different field names, so the reader accepts the compact
// set used by the managed Canadian catalogs.
func (p Place) CensusPopulation() (int64, int) {
	population := int64(0)
	for _, key := range []string{"population", "population_centre_population", "census_population"} {
		if value, ok := positiveAttributeInt64(p.Attrs[key]); ok && value > population {
			population = value
		}
	}
	year := 0
	if value, ok := positiveAttributeInt64(p.Attrs["census_year"]); ok && value <= math.MaxInt32 {
		year = int(value)
	}
	return population, year
}

func positiveAttributeInt64(raw any) (int64, bool) {
	var value int64
	switch typed := raw.(type) {
	case int:
		value = int64(typed)
	case int32:
		value = int64(typed)
	case int64:
		value = typed
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, false
		}
		value = int64(typed)
	case uint32:
		value = int64(typed)
	case uint64:
		if typed > math.MaxInt64 {
			return 0, false
		}
		value = int64(typed)
	case float32:
		floatValue := float64(typed)
		if math.IsNaN(floatValue) || math.IsInf(floatValue, 0) || floatValue > math.MaxInt64 {
			return 0, false
		}
		value = int64(floatValue)
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || typed > math.MaxInt64 {
			return 0, false
		}
		value = int64(typed)
	case json.Number:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed.String()), 10, 64)
		if err != nil {
			return 0, false
		}
		value = parsed
	case string:
		cleaned := strings.NewReplacer(",", "", " ", "").Replace(strings.TrimSpace(typed))
		parsed, err := strconv.ParseInt(cleaned, 10, 64)
		if err != nil {
			return 0, false
		}
		value = parsed
	default:
		return 0, false
	}
	return value, value > 0
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

// EnrichPopulationFromCorePack copies population metadata from the optional
// Canadian core catalog into a legacy snapshot. Exact Statistics Canada DGUIDs
// are preferred. A normalized name and province fallback is used only when it
// identifies exactly one core entity, preserving same-province homonyms.
func EnrichPopulationFromCorePack(baseDir string, snapshot Snapshot) (Snapshot, bool) {
	baseDir = strings.TrimSpace(baseDir)
	if baseDir == "" {
		baseDir = "."
	}
	path := filepath.Clean(filepath.Join(baseDir, populationCoreRelPath))
	stat, err := os.Stat(path)
	if err != nil || stat.IsDir() {
		return snapshot, false
	}
	index, err := readPopulationCoreIndex(path)
	if err != nil {
		return snapshot, false
	}

	places := make([]Place, len(snapshot.Places))
	copy(places, snapshot.Places)
	changed := false
	for position := range places {
		candidate, ok := index.match(places[position])
		if !ok {
			continue
		}
		currentPopulation, _ := places[position].CensusPopulation()
		if currentPopulation >= candidate.Population {
			continue
		}
		attrs := make(map[string]any, len(places[position].Attrs)+4)
		for key, value := range places[position].Attrs {
			attrs[key] = value
		}
		attrs["census_population"] = candidate.Population
		if candidate.CensusYear > 0 {
			attrs["census_year"] = candidate.CensusYear
		}
		if candidate.DGUID != "" {
			attrs["census_dguid"] = candidate.DGUID
		}
		attrs["population_source"] = "statistics_canada"
		places[position].Attrs = attrs
		changed = true
	}
	if !changed {
		return snapshot, true
	}
	snapshot.Places = places
	snapshot.bySourceCode = indexPlaces(places)
	return snapshot, true
}

type populationCoreCandidate struct {
	EntityPK   int64
	Population int64
	CensusYear int
	DGUID      string
	SGC        string
	Region     string
}

type populationCoreIndex struct {
	byDGUID           map[string]populationCoreCandidate
	bySGC             map[string]populationCoreCandidate
	byNameRegion      map[string]populationCoreCandidate
	ambiguousNameKeys map[string]struct{}
}

func (index populationCoreIndex) match(place Place) (populationCoreCandidate, bool) {
	if dguid := placePopulationDGUID(place); dguid != "" {
		if candidate, ok := index.byDGUID[dguid]; ok {
			return candidate, true
		}
	}
	if strings.EqualFold(strings.TrimSpace(place.Source), "sgc") {
		if candidate, ok := index.bySGC[normalizedSGC(place.Code)]; ok {
			return candidate, true
		}
	}
	region := normalizePopulationRegion(place.Region)
	if region == "" {
		return populationCoreCandidate{}, false
	}
	var matched populationCoreCandidate
	found := false
	for _, name := range append([]string{place.Name, place.NameFR}, place.Aliases()...) {
		key := populationNameRegionKey(name, region)
		candidate, ok := index.byNameRegion[key]
		if !ok {
			continue
		}
		if found && candidate.EntityPK != matched.EntityPK {
			return populationCoreCandidate{}, false
		}
		matched = candidate
		found = true
	}
	return matched, found
}

func readPopulationCoreIndex(path string) (populationCoreIndex, error) {
	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(path))
	if err != nil {
		return populationCoreIndex{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	index := populationCoreIndex{
		byDGUID:           map[string]populationCoreCandidate{},
		bySGC:             map[string]populationCoreCandidate{},
		byNameRegion:      map[string]populationCoreCandidate{},
		ambiguousNameKeys: map[string]struct{}{},
	}
	byEntity := map[int64]populationCoreCandidate{}
	rows, err := db.Query(`
		SELECT entity_pk, COALESCE(region, ''), attributes_json
		FROM entities
		WHERE UPPER(COALESCE(country, '')) = 'CA'
	`)
	if err != nil {
		return populationCoreIndex{}, err
	}
	for rows.Next() {
		var entityPK int64
		var region string
		var attrsRaw string
		if err := rows.Scan(&entityPK, &region, &attrsRaw); err != nil {
			rows.Close()
			return populationCoreIndex{}, err
		}
		attrs := map[string]any{}
		if err := json.Unmarshal([]byte(attrsRaw), &attrs); err != nil {
			continue
		}
		population, censusYear := (Place{Attrs: attrs}).CensusPopulation()
		if population <= 0 {
			continue
		}
		candidate := populationCoreCandidate{
			EntityPK:   entityPK,
			Population: population,
			CensusYear: censusYear,
			DGUID:      normalizedDGUID(attributeString(attrs, "census_dguid")),
			Region:     normalizePopulationRegion(region),
		}
		byEntity[entityPK] = candidate
		if candidate.DGUID != "" {
			index.byDGUID[candidate.DGUID] = candidate
		}
	}
	if err := rows.Close(); err != nil {
		return populationCoreIndex{}, err
	}
	if err := rows.Err(); err != nil {
		return populationCoreIndex{}, err
	}

	identifierRows, err := db.Query(`
		SELECT entity_pk, LOWER(scheme), normalized_value
		FROM identifiers
		WHERE LOWER(authority) = 'statcan' AND LOWER(scheme) IN ('sgc_dguid', 'sgc')
	`)
	if err != nil {
		return populationCoreIndex{}, err
	}
	for identifierRows.Next() {
		var entityPK int64
		var scheme string
		var value string
		if err := identifierRows.Scan(&entityPK, &scheme, &value); err != nil {
			identifierRows.Close()
			return populationCoreIndex{}, err
		}
		candidate, ok := byEntity[entityPK]
		if !ok {
			continue
		}
		if scheme == "sgc_dguid" {
			candidate.DGUID = normalizedDGUID(value)
		} else {
			candidate.SGC = normalizedSGC(value)
		}
		byEntity[entityPK] = candidate
		if candidate.DGUID != "" {
			index.byDGUID[candidate.DGUID] = candidate
		}
		if candidate.SGC != "" {
			index.bySGC[candidate.SGC] = candidate
		}
	}
	if err := identifierRows.Close(); err != nil {
		return populationCoreIndex{}, err
	}
	if err := identifierRows.Err(); err != nil {
		return populationCoreIndex{}, err
	}

	nameRows, err := db.Query(`
		SELECT entity_pk, name, normalized_name
		FROM names
	`)
	if err != nil {
		return populationCoreIndex{}, err
	}
	for nameRows.Next() {
		var entityPK int64
		var name string
		var normalizedName string
		if err := nameRows.Scan(&entityPK, &name, &normalizedName); err != nil {
			nameRows.Close()
			return populationCoreIndex{}, err
		}
		candidate, ok := byEntity[entityPK]
		if !ok || candidate.Region == "" {
			continue
		}
		for _, value := range []string{name, normalizedName} {
			index.addName(populationNameRegionKey(value, candidate.Region), candidate)
		}
	}
	if err := nameRows.Close(); err != nil {
		return populationCoreIndex{}, err
	}
	if err := nameRows.Err(); err != nil {
		return populationCoreIndex{}, err
	}
	return index, nil
}

func (index populationCoreIndex) addName(key string, candidate populationCoreCandidate) {
	if key == "" {
		return
	}
	if _, ambiguous := index.ambiguousNameKeys[key]; ambiguous {
		return
	}
	if current, exists := index.byNameRegion[key]; exists && current.EntityPK != candidate.EntityPK {
		delete(index.byNameRegion, key)
		index.ambiguousNameKeys[key] = struct{}{}
		return
	}
	index.byNameRegion[key] = candidate
}

func populationNameRegionKey(name string, region string) string {
	normalizedName := normalizePopulationName(name)
	normalizedRegion := normalizePopulationRegion(region)
	if normalizedName == "" || normalizedRegion == "" {
		return ""
	}
	return normalizedName + "\x00" + normalizedRegion
}

func normalizePopulationName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	decomposed := norm.NFD.String(value)
	var builder strings.Builder
	builder.Grow(len(decomposed))
	for _, current := range decomposed {
		switch {
		case unicode.Is(unicode.Mn, current):
			continue
		case unicode.IsLetter(current) || unicode.IsDigit(current):
			builder.WriteRune(current)
		default:
			builder.WriteByte(' ')
		}
	}
	normalized := strings.Join(strings.Fields(builder.String()), " ")
	for _, prefix := range []string{
		"rural municipality of ", "regional municipality of ", "municipality of ",
		"municipalite de ", "municipalite d ", "city of ", "town of ", "village of ",
		"ville de ", "village de ",
	} {
		if strings.HasPrefix(normalized, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(normalized, prefix))
		}
	}
	return normalized
}

func normalizePopulationRegion(value string) string {
	return strings.ToUpper(strings.TrimSpace(strings.Split(value, ",")[0]))
}

func placePopulationDGUID(place Place) string {
	for _, key := range []string{"census_dguid", "statcan_dguid", "csd_dguid", "dguid"} {
		if value := normalizedDGUID(attributeString(place.Attrs, key)); value != "" {
			return value
		}
	}
	return ""
}

func normalizedDGUID(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func normalizedSGC(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func attributeString(attrs map[string]any, key string) string {
	if attrs == nil {
		return ""
	}
	value, _ := attrs[key].(string)
	return strings.TrimSpace(value)
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
