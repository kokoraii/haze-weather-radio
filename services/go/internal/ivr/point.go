package ivr

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

const (
	pointQueryTimeout  = 4 * time.Second
	pointDefaultLimit  = 5
	pointMaximumLimit  = 10
	pointMaximumQuery  = 2048
	pointMaximumName   = 160
	pointMaximumValue  = 128
	pointMaximumID     = 256
	pointMaximumLocale = 35
	pointMaximumRegion = 32
	pointQueryCapacity = 16
)

var (
	defaultPointQuerySlots = make(chan struct{}, pointQueryCapacity)
	pointSchemePattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)
	pointLocalePattern     = regexp.MustCompile(`^[A-Za-z]{2,8}(?:[-_][A-Za-z0-9]{1,8})*$`)
	pointRegionPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$`)
	pointCanonicalPattern  = regexp.MustCompile(`^urn:haze:location:[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	canadianPostalPattern  = regexp.MustCompile(`^[ABCEGHJKLMNPRSTVXY][0-9][ABCEGHJKLMNPRSTVWXYZ][0-9][ABCEGHJKLMNPRSTVWXYZ][0-9]$`)
)

type pointQueryFunc func(context.Context, locationclient.Request) (locationclient.Response, error)

type pointQueryPlan struct {
	Request           locationclient.Request
	Fallback          *locationclient.Request
	FallbackScheme    string
	FallbackAuthority string
	Selector          pointSelector
	Limit             int
}

type pointSelector struct {
	Type      string `json:"type"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Scheme    string `json:"scheme,omitempty"`
	Authority string `json:"authority,omitempty"`
}

type pointResponse struct {
	Status            string           `json:"status"`
	Ambiguous         bool             `json:"ambiguous"`
	Truncated         bool             `json:"truncated"`
	CatalogGeneration string           `json:"catalog_generation,omitempty"`
	Selector          pointSelector    `json:"selector"`
	Results           []pointCandidate `json:"results"`
}

type pointCandidate struct {
	Entity    pointEntity `json:"entity"`
	Match     pointMatch  `json:"match"`
	Facet     string      `json:"facet,omitempty"`
	DistanceM *float64    `json:"distance_m,omitempty"`
}

type pointEntity struct {
	ID              string                      `json:"id"`
	Kind            string                      `json:"kind"`
	Capabilities    []string                    `json:"capabilities,omitempty"`
	Country         string                      `json:"country,omitempty"`
	Region          string                      `json:"region,omitempty"`
	LifecycleStatus string                      `json:"lifecycle_status,omitempty"`
	ReportingStatus string                      `json:"reporting_status,omitempty"`
	SourceQuality   *float64                    `json:"source_quality,omitempty"`
	Identifiers     []locationclient.Identifier `json:"identifiers,omitempty"`
	Names           []locationclient.Name       `json:"names,omitempty"`
	Population      int64                       `json:"population,omitempty"`
	CensusYear      int                         `json:"census_year,omitempty"`
	Geometry        *pointGeometry              `json:"geometry,omitempty"`
}

type pointGeometry struct {
	Type      string   `json:"geometry_type"`
	Latitude  float64  `json:"latitude"`
	Longitude float64  `json:"longitude"`
	AccuracyM *float64 `json:"accuracy_m,omitempty"`
	SourceID  string   `json:"source_id,omitempty"`
}

type pointMatch struct {
	Score      *float64 `json:"score,omitempty"`
	Confidence string   `json:"confidence,omitempty"`
	Method     string   `json:"method,omitempty"`
	Algorithm  string   `json:"algorithm,omitempty"`
}

func (s *Service) handlePoint(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet+", "+http.MethodOptions)
		writePointError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	var helloWeather map[string]locationRecord
	if s != nil {
		helloWeather = s.cfg.HelloWeather
	}
	plan, err := pointRequestFromHTTP(request, helloWeather)
	if err != nil {
		writePointError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	slots := defaultPointQuerySlots
	if s != nil && s.pointSlots != nil {
		slots = s.pointSlots
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		writer.Header().Set("Retry-After", "1")
		writePointError(writer, http.StatusTooManyRequests, "busy", "location point service is busy")
		return
	}

	ctx, cancel := context.WithTimeout(request.Context(), pointQueryTimeout)
	defer cancel()
	response, err := s.executePointQuery(ctx, plan.Request)
	if err == nil && len(response.Results) == 0 && plan.Fallback != nil {
		response, err = s.executePointQuery(ctx, *plan.Fallback)
		if err == nil {
			plan.Selector.Scheme = plan.FallbackScheme
			plan.Selector.Authority = plan.FallbackAuthority
		}
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			writePointError(writer, http.StatusGatewayTimeout, "timeout", "location query timed out")
			return
		}
		writePointError(writer, http.StatusServiceUnavailable, "unavailable", "location service is unavailable")
		return
	}

	originalCount := len(response.Results)
	if originalCount > plan.Limit {
		response.Results = response.Results[:plan.Limit]
		response.Truncated = true
	}
	result := pointResponse{
		Status:            response.Status,
		Ambiguous:         response.Ambiguous,
		Truncated:         response.Truncated,
		CatalogGeneration: response.CatalogGeneration,
		Selector:          plan.Selector,
		Results:           make([]pointCandidate, 0, len(response.Results)),
	}
	for _, candidate := range response.Results {
		result.Results = append(result.Results, publicPointCandidate(candidate, s.cfg.CensusPopulation))
	}
	if len(result.Results) == 0 {
		result.Status = "not_found"
		writePointStatus(writer, http.StatusNotFound, result)
		return
	}
	if result.Status == "" || result.Status == "not_found" {
		result.Status = "resolved"
	}
	writer.Header().Set("Cache-Control", "public, max-age=60")
	writePointStatus(writer, http.StatusOK, result)
}

func (s *Service) executePointQuery(ctx context.Context, request locationclient.Request) (locationclient.Response, error) {
	if s != nil && s.pointQuery != nil {
		return s.pointQuery(ctx, request)
	}
	bridgeAddress := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddress == "" {
		return locationclient.Response{}, errors.New("location service bridge is unavailable")
	}
	client := locationclient.New(bridgeAddress, "haze-ivr-point")
	client.Timeout = pointQueryTimeout
	return client.Query(ctx, request)
}

func pointRequestFromHTTP(request *http.Request, helloWeather map[string]locationRecord) (pointQueryPlan, error) {
	if request == nil || request.URL == nil {
		return pointQueryPlan{}, errors.New("location selector is required")
	}
	if len(request.URL.RawQuery) > pointMaximumQuery {
		return pointQueryPlan{}, errors.New("query string is too long")
	}
	values := request.URL.Query()
	allowed := map[string]bool{
		"geocode": true, "name": true, "icao": true, "iata": true,
		"citypage": true, "helloweather": true, "postal": true,
		"scheme": true, "value": true, "authority": true, "id": true,
		"locale": true, "country": true, "region": true, "limit": true,
	}
	for key, items := range values {
		if !allowed[key] {
			return pointQueryPlan{}, errors.New("unknown query parameter " + strconv.Quote(key))
		}
		if len(items) != 1 {
			return pointQueryPlan{}, errors.New("query parameter " + strconv.Quote(key) + " must appear once")
		}
	}

	limit := pointDefaultLimit
	if values.Has("limit") {
		parsed, err := strconv.Atoi(strings.TrimSpace(values.Get("limit")))
		if err != nil || parsed < 1 || parsed > pointMaximumLimit {
			return pointQueryPlan{}, errors.New("limit must be between 1 and 10")
		}
		limit = parsed
	}
	filters := locationclient.Filters{}
	if values.Has("country") {
		country := strings.ToUpper(strings.TrimSpace(values.Get("country")))
		if len(country) != 2 || country[0] < 'A' || country[0] > 'Z' || country[1] < 'A' || country[1] > 'Z' {
			return pointQueryPlan{}, errors.New("country must be a two-letter code")
		}
		filters.Country = country
	}
	if values.Has("region") {
		region := strings.TrimSpace(values.Get("region"))
		if utf8.RuneCountInString(region) > pointMaximumRegion || !pointRegionPattern.MatchString(region) {
			return pointQueryPlan{}, errors.New("region is invalid")
		}
		filters.Region = strings.ToUpper(region)
	}
	locale := ""
	if values.Has("locale") {
		locale = strings.TrimSpace(values.Get("locale"))
		if utf8.RuneCountInString(locale) > pointMaximumLocale || !pointLocalePattern.MatchString(locale) {
			return pointQueryPlan{}, errors.New("locale is invalid")
		}
		locale = strings.ReplaceAll(locale, "_", "-")
	}
	base := locationclient.Request{
		APIVersion: locationclient.APIVersion,
		Filters:    filters,
		Options: locationclient.Options{
			Limit: limit, Locale: locale, IncludeAreaGeometry: false,
		},
	}

	selectorKeys := []string{"geocode", "name", "icao", "iata", "citypage", "helloweather", "postal", "id"}
	selected := ""
	selectorCount := 0
	for _, key := range selectorKeys {
		if values.Has(key) {
			selected = key
			selectorCount++
		}
	}
	generic := values.Has("scheme") || values.Has("value")
	if generic {
		if !values.Has("scheme") || !values.Has("value") {
			return pointQueryPlan{}, errors.New("scheme and value must be provided together")
		}
		selectorCount++
		selected = "scheme"
	}
	if values.Has("authority") && !generic {
		return pointQueryPlan{}, errors.New("authority is only valid with scheme and value")
	}
	if selectorCount != 1 {
		return pointQueryPlan{}, errors.New("exactly one location selector is required")
	}

	plan := pointQueryPlan{Request: base, Limit: limit}
	switch selected {
	case "geocode":
		raw := strings.TrimSpace(values.Get("geocode"))
		latitude, longitude, err := parsePointGeocode(raw)
		if err != nil {
			return pointQueryPlan{}, err
		}
		plan.Request.Operation = "point_facets"
		plan.Request.Input = &locationclient.Input{Kind: "point", Latitude: &latitude, Longitude: &longitude}
		plan.Selector = pointSelector{Type: "point", Key: "geocode", Value: raw}
	case "name":
		text, err := validatedPointText(values.Get("name"), pointMaximumName, "name")
		if err != nil {
			return pointQueryPlan{}, err
		}
		plan.Request.Operation = "search"
		plan.Request.Input = &locationclient.Input{Kind: "name", Text: text}
		plan.Selector = pointSelector{Type: "name", Key: "name", Value: text}
	case "id":
		id, err := validatedPointText(values.Get("id"), pointMaximumID, "id")
		if err != nil || !pointCanonicalPattern.MatchString(id) {
			return pointQueryPlan{}, errors.New("id must be a canonical Haze location URN")
		}
		plan.Request.Operation = "resolve"
		plan.Request.Input = &locationclient.Input{Kind: "entity", ID: id}
		plan.Selector = pointSelector{Type: "entity", Key: "id", Value: id}
	case "scheme":
		scheme := strings.ToLower(strings.TrimSpace(values.Get("scheme")))
		if !pointSchemePattern.MatchString(scheme) {
			return pointQueryPlan{}, errors.New("scheme is invalid")
		}
		value, err := validatedPointText(values.Get("value"), pointMaximumValue, "value")
		if err != nil {
			return pointQueryPlan{}, err
		}
		authority := ""
		if values.Has("authority") {
			authority = strings.ToLower(strings.TrimSpace(values.Get("authority")))
			if !pointSchemePattern.MatchString(authority) {
				return pointQueryPlan{}, errors.New("authority is invalid")
			}
		}
		plan.Request.Operation = "resolve"
		plan.Request.Input = &locationclient.Input{Kind: "identifier", Scheme: scheme, Authority: authority, Value: value}
		plan.Selector = pointSelector{Type: "identifier", Key: "scheme", Value: value, Scheme: scheme, Authority: authority}
	default:
		value, err := validatedPointText(values.Get(selected), pointMaximumValue, selected)
		if err != nil {
			return pointQueryPlan{}, err
		}
		scheme, authority := pointIdentifierAlias(selected)
		if selected == "helloweather" {
			code := strings.TrimSpace(value)
			if !validPointHelloWeatherCode(code) {
				return pointQueryPlan{}, errors.New("helloweather must be a valid five-digit telephone code")
			}
			forecast := ""
			if configured, ok := helloWeather[code]; ok {
				forecast = strings.TrimSpace(configured.Forecast)
			}
			if forecast == "" {
				if derived, ok := deriveHelloWeatherRecord(code); ok {
					forecast = strings.TrimSpace(derived.Forecast)
				}
			}
			if forecast == "" {
				return pointQueryPlan{}, errors.New("helloweather code has no forecast mapping")
			}
			scheme, authority, value = "eccc_citypage", "eccc", forecast
		}
		plan.Request.Operation = "resolve"
		plan.Request.Input = &locationclient.Input{Kind: "identifier", Scheme: scheme, Authority: authority, Value: value}
		plan.Selector = pointSelector{Type: "identifier", Key: selected, Value: strings.TrimSpace(values.Get(selected)), Scheme: scheme, Authority: authority}
		if selected == "iata" {
			fallback := base
			fallback.Operation = "resolve"
			fallback.Input = &locationclient.Input{Kind: "identifier", Scheme: "eccc_station", Authority: "eccc", Value: strings.ToUpper(value)}
			plan.Fallback = &fallback
			plan.FallbackScheme = "eccc_station"
			plan.FallbackAuthority = "eccc"
		}
		if selected == "postal" {
			postal := strings.ToUpper(strings.ReplaceAll(value, " ", ""))
			if canadianPostalPattern.MatchString(postal) && (filters.Country == "" || filters.Country == "CA") {
				fallback := base
				fallback.Operation = "resolve"
				fallback.Input = &locationclient.Input{Kind: "identifier", Scheme: "postal", Value: postal[:3]}
				plan.Fallback = &fallback
				plan.FallbackScheme = "postal"
			}
		}
	}
	return plan, nil
}

func validPointHelloWeatherCode(value string) bool {
	if len(value) != 5 || value[0] != '0' {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func pointIdentifierAlias(key string) (string, string) {
	switch key {
	case "icao":
		return "icao", "icao"
	case "iata":
		return "iata", "iata"
	case "citypage":
		return "eccc_citypage", "eccc"
	case "helloweather":
		return "hello_weather", "eccc"
	case "postal":
		return "postal", ""
	default:
		return "", ""
	}
}

func parsePointGeocode(raw string) (float64, float64, error) {
	parts := strings.Split(raw, ",")
	if len(parts) != 2 {
		return 0, 0, errors.New("geocode must be latitude,longitude")
	}
	latitude, latitudeErr := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	longitude, longitudeErr := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if latitudeErr != nil || longitudeErr != nil || !finitePointNumber(latitude) || !finitePointNumber(longitude) || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 {
		return 0, 0, errors.New("geocode must contain valid WGS84 coordinates")
	}
	return latitude, longitude, nil
}

func validatedPointText(raw string, maximum int, field string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", errors.New(field + " is required")
	}
	if utf8.RuneCountInString(value) > maximum {
		return "", errors.New(field + " is too long")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return "", errors.New(field + " contains control characters")
		}
	}
	return value, nil
}

func publicPointCandidate(candidate locationclient.Candidate, censusCatalog locationdb.CensusPopulationCatalog) pointCandidate {
	entity := candidate.Entity
	censusPopulation := pointCensusPopulation(entity, censusCatalog)
	result := pointCandidate{
		Entity: pointEntity{
			ID: entity.ID, Kind: entity.Kind,
			Capabilities: append([]string(nil), entity.Capabilities...),
			Country:      entity.Country, Region: entity.Region,
			LifecycleStatus: entity.LifecycleStatus, ReportingStatus: entity.ReportingStatus,
			SourceQuality: finitePointPointer(entity.SourceQuality),
			Identifiers:   append([]locationclient.Identifier(nil), entity.Identifiers...),
			Names:         append([]locationclient.Name(nil), entity.Names...),
			Population:    censusPopulation.Population,
			CensusYear:    censusPopulation.CensusYear,
			Geometry:      publicPointGeometry(entity.Geometry),
		},
		Match: pointMatch{
			Score: finitePointPointer(candidate.Match.Score), Confidence: candidate.Match.Confidence,
			Method: candidate.Match.Method, Algorithm: candidate.Match.Algorithm,
		},
		Facet: candidate.Facet,
	}
	if candidate.Distance != nil && finitePointNumber(*candidate.Distance) && *candidate.Distance >= 0 {
		distance := *candidate.Distance
		result.DistanceM = &distance
	}
	return result
}

func pointCensusPopulation(entity locationclient.Entity, catalog locationdb.CensusPopulationCatalog) locationdb.CensusPopulation {
	if !strings.EqualFold(strings.TrimSpace(entity.Country), "CA") {
		return locationdb.CensusPopulation{}
	}
	switch strings.ToLower(strings.TrimSpace(entity.Kind)) {
	case "administrative_area":
		if !pointHasCensusSubdivisionIdentifier(entity.Identifiers) {
			return locationdb.CensusPopulation{}
		}
		return catalog.ByCanonicalID[strings.TrimSpace(entity.ID)]
	case "forecast_location":
		cityPageID := pointPrimaryCityPageID(entity.Identifiers)
		if cityPageID == "" {
			return locationdb.CensusPopulation{}
		}
		return catalog.ByCityPageID[cityPageID]
	default:
		return locationdb.CensusPopulation{}
	}
}

func pointHasCensusSubdivisionIdentifier(identifiers []locationclient.Identifier) bool {
	for _, identifier := range identifiers {
		if strings.TrimSpace(identifier.Value) == "" {
			continue
		}
		authority := strings.ToLower(strings.TrimSpace(identifier.Authority))
		scheme := strings.ToLower(strings.TrimSpace(identifier.Scheme))
		if authority == "statcan" && (scheme == "sgc" || scheme == "sgc_dguid") {
			return true
		}
		if authority == "eccc" && scheme == "sgc" {
			return true
		}
	}
	return false
}

func pointPrimaryCityPageID(identifiers []locationclient.Identifier) string {
	for _, identifier := range identifiers {
		if !identifier.Primary || !strings.EqualFold(strings.TrimSpace(identifier.Authority), "eccc") || !strings.EqualFold(strings.TrimSpace(identifier.Scheme), "eccc_citypage") {
			continue
		}
		if value := strings.ToLower(strings.TrimSpace(firstNonBlank(identifier.NormalizedValue, identifier.Value))); value != "" {
			return value
		}
	}
	return ""
}

func publicPointGeometry(geometry *locationclient.Geometry) *pointGeometry {
	if geometry == nil || geometry.Latitude == nil || geometry.Longitude == nil {
		return nil
	}
	latitude, longitude := *geometry.Latitude, *geometry.Longitude
	if !finitePointNumber(latitude) || !finitePointNumber(longitude) || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 {
		return nil
	}
	result := &pointGeometry{Type: geometry.Type, Latitude: latitude, Longitude: longitude, SourceID: geometry.SourceID}
	if geometry.AccuracyM != nil && finitePointNumber(*geometry.AccuracyM) && *geometry.AccuracyM >= 0 {
		accuracy := *geometry.AccuracyM
		result.AccuracyM = &accuracy
	}
	return result
}

func finitePointPointer(value float64) *float64 {
	if !finitePointNumber(value) {
		return nil
	}
	return &value
}

func finitePointNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func writePointError(writer http.ResponseWriter, status int, code string, message string) {
	writer.Header().Set("Cache-Control", "no-store")
	writePointStatus(writer, status, map[string]string{"code": code, "error": message})
}

func writePointStatus(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return
	}
}
