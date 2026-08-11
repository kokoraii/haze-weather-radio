package ivr

import (
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

const (
	maximumLocationCodeLimit = 500
	maximumLocationCodeQuery = 100
)

type telephoneLocationCode struct {
	Code                 string   `json:"code"`
	DialCode             string   `json:"dial_code,omitempty"`
	RegionalID           string   `json:"regional_id,omitempty"`
	Source               string   `json:"source"`
	Name                 string   `json:"name"`
	NameFR               string   `json:"name_fr"`
	Province             string   `json:"province,omitempty"`
	Country              string   `json:"country,omitempty"`
	FeedID               string   `json:"feed_id,omitempty"`
	Covered              bool     `json:"covered_by_feed"`
	Language             string   `json:"language,omitempty"`
	Timezone             string   `json:"timezone,omitempty"`
	ForecastID           string   `json:"forecast_id,omitempty"`
	StationID            string   `json:"station_id,omitempty"`
	Latitude             *float64 `json:"latitude,omitempty"`
	Longitude            *float64 `json:"longitude,omitempty"`
	Population           int64    `json:"population,omitempty"`
	CensusYear           int      `json:"census_year,omitempty"`
	PostalCodes          []string `json:"postal_codes,omitempty"`
	PostalCodeFormat     string   `json:"postal_code_format,omitempty"`
	PostalCodesTruncated bool     `json:"postal_codes_truncated,omitempty"`
}

type telephoneLocationCodePage struct {
	Source     string                  `json:"source"`
	Total      int                     `json:"total"`
	Offset     int                     `json:"offset"`
	Limit      int                     `json:"limit"`
	NextOffset *int                    `json:"next_offset,omitempty"`
	Locations  []telephoneLocationCode `json:"locations"`
}

func (s *Service) handleLocationCodes(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s == nil || s.resolver == nil {
		http.Error(writer, "location catalog unavailable", http.StatusServiceUnavailable)
		return
	}

	source := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("source")))
	if source == "" {
		source = "hello_weather"
	}
	if source != "hello_weather" {
		http.Error(writer, "unsupported location-code source", http.StatusBadRequest)
		return
	}

	province := strings.TrimSpace(request.URL.Query().Get("province"))
	if province != "" {
		province = normalizeProvinceCode(province)
		if !validProvinceCode(province) || province == "CA" {
			http.Error(writer, "invalid province or territory", http.StatusBadRequest)
			return
		}
	}

	query := strings.TrimSpace(request.URL.Query().Get("q"))
	if len([]rune(query)) > maximumLocationCodeQuery {
		http.Error(writer, "location query is too long", http.StatusBadRequest)
		return
	}
	query = normalizeLocationSearchText(query, false)

	limit, ok := locationCodePageInteger(request.URL.Query().Get("limit"), 0, 1, maximumLocationCodeLimit)
	if !ok {
		http.Error(writer, "limit must be between 1 and 500", http.StatusBadRequest)
		return
	}
	offset, ok := locationCodePageInteger(request.URL.Query().Get("offset"), 0, 0, 1_000_000)
	if !ok {
		http.Error(writer, "offset must be between 0 and 1000000", http.StatusBadRequest)
		return
	}

	all := s.resolver.telephoneLocationCodes()
	filtered := make([]telephoneLocationCode, 0, len(all))
	for _, location := range all {
		if province != "" && !strings.EqualFold(location.Province, province) {
			continue
		}
		if query != "" {
			searchable := normalizeLocationSearchText(strings.Join([]string{
				location.Code,
				location.DialCode,
				location.RegionalID,
				location.Name,
				location.NameFR,
				location.Province,
				strings.Join(location.PostalCodes, " "),
			}, " "), false)
			if !strings.Contains(searchable, query) {
				continue
			}
		}
		filtered = append(filtered, location)
	}

	start := minInt(offset, len(filtered))
	end := len(filtered)
	if limit > 0 {
		end = minInt(start+limit, len(filtered))
	}
	page := telephoneLocationCodePage{
		Source:    source,
		Total:     len(filtered),
		Offset:    offset,
		Limit:     limit,
		Locations: filtered[start:end],
	}
	if limit > 0 && end < len(filtered) {
		next := end
		page.NextOffset = &next
	}
	writer.Header().Set("Cache-Control", "public, max-age=300")
	writeJSON(writer, page)
}

func locationCodePageInteger(raw string, fallback int, minimum int, maximum int) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, false
	}
	return value, true
}

func (r *Resolver) telephoneLocationCodes() []telephoneLocationCode {
	if r == nil {
		return nil
	}
	r.telephoneCodesOnce.Do(func() {
		codes := r.helloWeatherDirectory()
		locations := make([]telephoneLocationCode, 0, len(codes))
		for code, candidate := range codes {
			code = strings.TrimSpace(code)
			if !isHelloWeatherDirectoryCode(code) {
				continue
			}
			candidate.Code = code
			candidate.Source = "hello_weather"
			candidate.Country = "CA"
			candidate.Kind = "telephone"
			candidate = r.enrichHelloWeatherRecord(candidate)
			name := fallbackText(cleanLocationName(candidate.Name), candidate.Code)
			nameFR := fallbackText(cleanLocationName(candidate.NameFR), name)
			candidate = r.attachFeed(candidate)
			covered := candidate.FeedID != ""
			language := ""
			timezone := ""
			if covered {
				language = r.feedLanguage(candidate.FeedID)
				timezone = r.feedTimezone(candidate.FeedID)
			}
			postalSet := r.cfg.HelloPostalCodes[code]
			postalCodeFormat := ""
			if len(postalSet.PostalCodes) > 0 {
				postalCodeFormat = "fsa"
			}
			locations = append(locations, telephoneLocationCode{
				Code:                 candidate.Code,
				DialCode:             helloWeatherDialCode(candidate.Code),
				RegionalID:           helloWeatherRegionalID(candidate.Code),
				Source:               candidate.Source,
				Name:                 name,
				NameFR:               nameFR,
				Province:             provinceCode(candidate.Province),
				Country:              candidate.Country,
				FeedID:               candidate.FeedID,
				Covered:              covered,
				Language:             language,
				Timezone:             timezone,
				ForecastID:           candidate.Forecast,
				StationID:            candidate.StationID,
				Latitude:             locationCodeCoordinate(candidate.Latitude, -90, 90),
				Longitude:            locationCodeCoordinate(candidate.Longitude, -180, 180),
				Population:           candidate.Population,
				CensusYear:           candidate.CensusYear,
				PostalCodes:          append([]string(nil), postalSet.PostalCodes...),
				PostalCodeFormat:     postalCodeFormat,
				PostalCodesTruncated: postalSet.Truncated,
			})
		}
		sort.Slice(locations, func(left int, right int) bool {
			return locations[left].Code < locations[right].Code
		})
		r.telephoneCodes = locations
	})
	return r.telephoneCodes
}

func locationCodeCoordinate(raw string, minimum float64, maximum float64) *float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < minimum || value > maximum {
		return nil
	}
	return &value
}

func isHelloWeatherDirectoryCode(code string) bool {
	if len(code) != 5 || code[0] != '0' {
		return false
	}
	for _, char := range code {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func helloWeatherDialCode(code string) string {
	if !isHelloWeatherDirectoryCode(code) {
		return ""
	}
	return code[1:2] + helloWeatherRegionalID(code)
}

func helloWeatherRegionalID(code string) string {
	if !isHelloWeatherDirectoryCode(code) {
		return ""
	}
	regionalID := strings.TrimLeft(code[2:], "0")
	if regionalID == "" {
		return "0"
	}
	return regionalID
}
