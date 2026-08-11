package locationdb

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
)

const MaxPostalCodesPerLocation = 64

const (
	maxHelloWeatherPostalRows = 10_000
	maxPostalLinkRows         = 250_000
	maxPostalLocationNameLen  = 512
	maxPostalRegionLen        = 32
	maxPostalIdentifierLen    = 32
	postalCodeLoadTimeout     = 10 * time.Second
)

// PostalCodeSet contains the bounded FSA prefixes that exactly match one
// Hello Weather location. Truncated reports that additional valid FSAs exist.
type PostalCodeSet struct {
	PostalCodes []string `json:"postal_codes"`
	Truncated   bool     `json:"truncated"`
}

type postalLocationKey struct {
	name   string
	region string
}

// PostalCodesByHelloWeatherCode loads exact city-and-province postal matches
// from the managed legacy catalog. It never infers matches from coordinates.
// maxPerLocation is clamped to MaxPostalCodesPerLocation, and non-positive
// values use that maximum.
func PostalCodesByHelloWeatherCode(baseDir string, maxPerLocation int) (map[string]PostalCodeSet, bool) {
	return postalCodesByHelloWeatherPath(Path(baseDir), maxPerLocation)
}

func postalCodesByHelloWeatherPath(path string, maxPerLocation int) (map[string]PostalCodeSet, bool) {
	path = filepath.Clean(path)
	maxPerLocation = boundedPostalCodeLimit(maxPerLocation)

	db, err := sql.Open("sqlite", postalReadOnlyDSN(path))
	if err != nil {
		return nil, false
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), postalCodeLoadTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, false
	}

	helloCodes, ok := readUniqueHelloWeatherPostalKeys(ctx, db)
	if !ok {
		return nil, false
	}
	postalCodes, ok := readPostalFSAsByLocation(ctx, db, helloCodes)
	if !ok {
		return nil, false
	}

	result := make(map[string]PostalCodeSet, len(postalCodes))
	for key, codes := range postalCodes {
		helloCode, exists := helloCodes[key]
		if !exists || len(codes) == 0 {
			continue
		}
		ordered := make([]string, 0, len(codes))
		for code := range codes {
			ordered = append(ordered, code)
		}
		sort.Strings(ordered)
		truncated := len(ordered) > maxPerLocation
		if truncated {
			ordered = ordered[:maxPerLocation]
		}
		result[helloCode] = PostalCodeSet{
			PostalCodes: ordered,
			Truncated:   truncated,
		}
	}
	return result, true
}

func postalReadOnlyDSN(path string) string {
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

func boundedPostalCodeLimit(value int) int {
	if value <= 0 || value > MaxPostalCodesPerLocation {
		return MaxPostalCodesPerLocation
	}
	return value
}

func readUniqueHelloWeatherPostalKeys(ctx context.Context, db *sql.DB) (map[postalLocationKey]string, bool) {
	rows, err := db.QueryContext(ctx, `
		SELECT code, name, region
		FROM places
		WHERE source = ? AND country = ?
		ORDER BY code
		LIMIT ?
	`, "hello_weather", "CA", maxHelloWeatherPostalRows+1)
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	unique := make(map[postalLocationKey]string)
	ambiguous := make(map[postalLocationKey]struct{})
	count := 0
	for rows.Next() {
		count++
		if count > maxHelloWeatherPostalRows {
			return nil, false
		}
		var code string
		var name string
		var region string
		if err := rows.Scan(&code, &name, &region); err != nil {
			return nil, false
		}
		if len(code) > maxPostalIdentifierLen {
			continue
		}
		code = strings.ToUpper(strings.TrimSpace(code))
		key := normalizedPostalLocationKey(name, region)
		if code == "" || key.name == "" || key.region == "" {
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

func readPostalFSAsByLocation(ctx context.Context, db *sql.DB, helloCodes map[postalLocationKey]string) (map[postalLocationKey]map[string]struct{}, bool) {
	rows, err := db.QueryContext(ctx, `
		SELECT postal_code, city, region
		FROM postal_links
		WHERE country = ?
		LIMIT ?
	`, "CA", maxPostalLinkRows+1)
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	result := make(map[postalLocationKey]map[string]struct{})
	count := 0
	for rows.Next() {
		count++
		if count > maxPostalLinkRows {
			return nil, false
		}
		var postalCode string
		var city string
		var region string
		if err := rows.Scan(&postalCode, &city, &region); err != nil {
			return nil, false
		}
		key := normalizedPostalLocationKey(city, region)
		if _, exists := helloCodes[key]; !exists {
			continue
		}
		fsa, valid := canadianPostalFSA(postalCode)
		if !valid {
			continue
		}
		if result[key] == nil {
			result[key] = make(map[string]struct{})
		}
		result[key][fsa] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, false
	}
	return result, true
}

func normalizedPostalLocationKey(name string, region string) postalLocationKey {
	if len(name) > maxPostalLocationNameLen || len(region) > maxPostalRegionLen {
		return postalLocationKey{}
	}
	return postalLocationKey{
		name:   normalizePopulationName(name),
		region: normalizePopulationRegion(region),
	}
}

func canadianPostalFSA(value string) (string, bool) {
	if len(value) > maxPostalIdentifierLen {
		return "", false
	}
	compact := strings.Map(func(current rune) rune {
		if unicode.IsSpace(current) {
			return -1
		}
		return current
	}, strings.ToUpper(strings.TrimSpace(value)))
	if len(compact) != 3 && len(compact) != 6 {
		return "", false
	}
	if !strings.ContainsRune("ABCEGHJKLMNPRSTVXY", rune(compact[0])) ||
		compact[1] < '0' || compact[1] > '9' ||
		!strings.ContainsRune("ABCEGHJKLMNPRSTVWXYZ", rune(compact[2])) {
		return "", false
	}
	if len(compact) == 6 && (compact[3] < '0' || compact[3] > '9' ||
		!strings.ContainsRune("ABCEGHJKLMNPRSTVWXYZ", rune(compact[4])) ||
		compact[5] < '0' || compact[5] > '9') {
		return "", false
	}
	return compact[:3], true
}
