package ivr

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

func TestResolvePlaceKeepsOverlappingCodesSourceQualified(t *testing.T) {
	t.Parallel()
	snapshot := locationdb.Snapshot{Places: []locationdb.Place{
		{Source: "forecast", Code: "SAME1", Name: "Canadian Place", Region: "SK", Country: "CA"},
		{Source: "nws_zone", Code: "SAME1", Name: "American Place", Region: "MT", Country: "US"},
	}}
	resolver := NewResolver(loadedConfig{Feeds: []feedXML{{ID: "render", EnabledRaw: "true"}}})
	canadian, err := resolver.ResolvePlace(snapshot, snapshot.Places[0])
	if err != nil {
		t.Fatal(err)
	}
	american, err := resolver.ResolvePlace(snapshot, snapshot.Places[1])
	if err != nil {
		t.Fatal(err)
	}
	if canadian.Source != "eccc_forecast" || canadian.Name != "Canadian Place" || american.Source != "nws_zone" || american.Name != "American Place" {
		t.Fatalf("source-qualified results crossed providers: Canadian=%#v American=%#v", canadian, american)
	}
}

func TestResolverFeedIndexPreservesLegacyPrecedence(t *testing.T) {
	first := feedXML{ID: "first", EnabledRaw: "true"}
	first.Locations.Coverage.Regions = []coverageRegionXML{
		{ID: "EARLY", Name: "Greater Alpha District", DeriveForecast: "forecast-early", Subregions: []coverageSubregionXML{{ID: "SHARED"}}},
		{ID: "SHARED", Name: "Later direct region", DeriveForecast: "forecast-late"},
	}
	first.Locations.ObservationLocations.Locations = []feedLocationXML{{ID: "OBS"}}
	second := feedXML{ID: "second", EnabledRaw: "true"}
	second.Locations.Coverage.Regions = []coverageRegionXML{
		{ID: "OBS", Name: "Second feed coverage", DeriveForecast: "forecast-second"},
	}

	resolver := NewResolver(loadedConfig{Feeds: []feedXML{first, second}})
	for _, record := range []locationRecord{
		{Code: "SHARED", Name: "Unrelated"},
		{Code: "OTHER", Name: "Alpha"},
		{Code: "OTHER", Name: "Unrelated", Forecast: "OBS"},
	} {
		got := resolver.attachFeed(record)
		want := legacyAttachFeedForTest(resolver, record)
		if got != want {
			t.Fatalf("indexed feed attachment changed legacy precedence for %#v: got %#v, want %#v", record, got, want)
		}
	}
}

func legacyAttachFeedForTest(resolver *Resolver, record locationRecord) locationRecord {
	if record.FeedID != "" {
		return record
	}
	for _, feed := range resolver.cfg.Feeds {
		if !xmlBool(feed.EnabledRaw, true) {
			continue
		}
		for _, region := range feed.Locations.Coverage.Regions {
			if regionMatchesRecord(region, record) || resolver.regionNameMatchesRecord(region, record) {
				record.FeedID = feed.ID
				if strings.TrimSpace(region.DeriveForecast) != "" {
					record.Forecast = strings.TrimSpace(region.DeriveForecast)
				}
				if regionName := resolver.coverageRegionDisplayName(region); regionName != "" && !locationNameMentioned(regionName, record.Name) {
					record.Name = regionName
				}
				return record
			}
			for _, subregion := range region.Subregions {
				if sameCode(subregion.ID, record.Code) {
					record.FeedID = feed.ID
					if strings.TrimSpace(region.DeriveForecast) != "" {
						record.Forecast = strings.TrimSpace(region.DeriveForecast)
					}
					if regionName := resolver.coverageRegionDisplayName(region); regionName != "" && !locationNameMentioned(regionName, record.Name) {
						record.Name = regionName
					}
					return record
				}
			}
		}
		for _, loc := range feed.Locations.ObservationLocations.Locations {
			if sameCode(loc.ID, record.Forecast) || sameCode(loc.ID, record.StationID) || sameCode(loc.ID, record.Code) {
				record.FeedID = feed.ID
				if record.StationID == "" {
					record.StationID = loc.ID
				}
				return record
			}
		}
	}
	return record
}

func TestResolverMapsHelloWeatherCodeToCoveredFeed(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		ForecastLocations: map[string]locationRecord{
			"000040": {Code: "000040", Source: "eccc_forecast", Name: "Saskatoon", Province: "SK", Forecast: "sk-40"},
		},
		Feeds: []feedXML{
			{
				ID:         "sk-0001",
				EnabledRaw: "true",
				Timezone:   "America/Regina",
				Locations: struct {
					Coverage struct {
						Regions []coverageRegionXML "xml:\"region\""
					} "xml:\"coverage\""
					ObservationLocations struct {
						Locations []feedLocationXML "xml:\"location\""
					} "xml:\"observationLocations\""
				}{
					Coverage: struct {
						Regions []coverageRegionXML "xml:\"region\""
					}{
						Regions: []coverageRegionXML{{ID: "065100", Source: "eccc", DeriveForecast: "sk-40"}},
					},
				},
			},
		},
	}

	location, err := resolverWithHelloWeather(cfg, locationRecord{Code: "06040", Source: "hello_weather", Name: "Saskatoon", Province: "SK"}).Resolve("06040")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.FeedID != "sk-0001" {
		t.Fatalf("FeedID = %q", location.FeedID)
	}
	if location.Forecast != "sk-40" {
		t.Fatalf("Forecast = %q", location.Forecast)
	}
	if location.Name != "Saskatoon" {
		t.Fatalf("Name = %q", location.Name)
	}
}

func TestResolverMarksNearbyHelloWeatherCodeOutsideFeedCoverage(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		ForecastLocations: map[string]locationRecord{
			"065100": {Code: "065100", Source: "eccc_forecast", Name: "City of Saskatoon", Province: "SK", Forecast: "sk-40"},
		},
		Feeds: []feedXML{{
			ID:         "sk-0001",
			EnabledRaw: "true",
			Timezone:   "America/Regina",
			Locations: struct {
				Coverage struct {
					Regions []coverageRegionXML "xml:\"region\""
				} "xml:\"coverage\""
				ObservationLocations struct {
					Locations []feedLocationXML "xml:\"location\""
				} "xml:\"observationLocations\""
			}{
				Coverage: struct {
					Regions []coverageRegionXML "xml:\"region\""
				}{
					Regions: []coverageRegionXML{{ID: "065100", Source: "eccc", DeriveForecast: "sk-40"}},
				},
			},
		}},
	}
	resolver := resolverWithHelloWeather(cfg, locationRecord{Code: "06041", Source: "hello_weather", Name: "Nearby test location", Province: "SK", Forecast: "sk-41"})

	location, err := resolver.Resolve("06041")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.FeedID != "sk-0001" {
		t.Fatalf("FeedID = %q", location.FeedID)
	}
	if location.Covered {
		t.Fatalf("nearby non-covered location was marked covered: %#v", location)
	}
	if location.Forecast != "sk-41" {
		t.Fatalf("Forecast = %q", location.Forecast)
	}
}

func TestResolverNamesHelloWeatherCodeFromFeedCoverageRegion(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		ForecastLocations: map[string]locationRecord{
			"065100": {Code: "065100", Source: "eccc_forecast", Name: "City of Saskatoon", Province: "SK"},
		},
		Feeds: []feedXML{
			{
				ID:         "sk-0001",
				EnabledRaw: "true",
				Timezone:   "America/Regina",
				Locations: struct {
					Coverage struct {
						Regions []coverageRegionXML "xml:\"region\""
					} "xml:\"coverage\""
					ObservationLocations struct {
						Locations []feedLocationXML "xml:\"location\""
					} "xml:\"observationLocations\""
				}{
					Coverage: struct {
						Regions []coverageRegionXML "xml:\"region\""
					}{
						Regions: []coverageRegionXML{{ID: "065100", Source: "eccc", DeriveForecast: "sk-40"}},
					},
				},
			},
		},
	}

	location, err := resolverWithHelloWeather(cfg, locationRecord{Code: "06040", Source: "hello_weather", Name: "Saskatoon", Province: "SK"}).Resolve("06040")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.Name != "Saskatoon" {
		t.Fatalf("Name = %q", location.Name)
	}
	if spokenLocationName(location) != "Saskatoon" {
		t.Fatalf("spoken location = %q", spokenLocationName(location))
	}
}

func TestResolverUsesDefaultFeedForKnownUncoveredLocation(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		CLCs: map[string]locationRecord{
			"065522": {Code: "065522", Source: "clc", Name: "Outlook", Province: "SK"},
		},
		Feeds: []feedXML{{ID: "sk-0001", EnabledRaw: "true", Timezone: "America/Regina"}},
	}

	location, err := NewResolver(cfg).Resolve("065522")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.FeedID != "sk-0001" {
		t.Fatalf("FeedID = %q", location.FeedID)
	}
	if location.Name != "Outlook" {
		t.Fatalf("Name = %q", location.Name)
	}
	if location.Timezone != "America/Regina" {
		t.Fatalf("Timezone = %q", location.Timezone)
	}
}

func TestResolverUsesCoverageDerivedForecastForRegionCode(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		ForecastLocations: map[string]locationRecord{
			"065500": {Code: "065500", Source: "eccc_forecast", Name: "Outlook - Watrous - Hanley - Imperial - Dinsmore", Province: "SK", Forecast: "sk-65500"},
		},
		Feeds: []feedXML{{
			ID:         "sk-0001",
			EnabledRaw: "true",
			Timezone:   "America/Regina",
			Locations: struct {
				Coverage struct {
					Regions []coverageRegionXML "xml:\"region\""
				} "xml:\"coverage\""
				ObservationLocations struct {
					Locations []feedLocationXML "xml:\"location\""
				} "xml:\"observationLocations\""
			}{
				Coverage: struct {
					Regions []coverageRegionXML "xml:\"region\""
				}{
					Regions: []coverageRegionXML{{ID: "065500", Source: "eccc", DeriveForecast: "sk-37"}},
				},
			},
		}},
	}

	location, err := NewResolver(cfg).Resolve("065500")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.Forecast != "sk-37" {
		t.Fatalf("Forecast = %q, want sk-37", location.Forecast)
	}
}

func TestResolverMapsNamedGeocodeToCoverageDerivedForecast(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		Geocodes: map[string]locationRecord{
			"4711008": {Code: "4711008", Source: "capcp_geocode", Name: "Imperial", Province: "SK", Latitude: "51.34708551950", Longitude: "-105.43980930200"},
		},
		ForecastLocations: map[string]locationRecord{
			"065500": {Code: "065500", Source: "eccc_forecast", Name: "Outlook - Watrous - Hanley - Imperial - Dinsmore", Province: "SK"},
		},
		Feeds: []feedXML{{
			ID:         "sk-0001",
			EnabledRaw: "true",
			Timezone:   "America/Regina",
			Locations: struct {
				Coverage struct {
					Regions []coverageRegionXML "xml:\"region\""
				} "xml:\"coverage\""
				ObservationLocations struct {
					Locations []feedLocationXML "xml:\"location\""
				} "xml:\"observationLocations\""
			}{
				Coverage: struct {
					Regions []coverageRegionXML "xml:\"region\""
				}{
					Regions: []coverageRegionXML{{ID: "065500", Source: "eccc", DeriveForecast: "sk-37"}},
				},
			},
		}},
	}

	location, err := NewResolver(cfg).Resolve("4711008")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.FeedID != "sk-0001" || location.Forecast != "sk-37" {
		t.Fatalf("location = %#v, want sk-0001/sk-37", location)
	}
	if location.Name != "Imperial" {
		t.Fatalf("Name = %q", location.Name)
	}
	if location.Latitude != "51.34708551950" || location.Longitude != "-105.43980930200" {
		t.Fatalf("coordinates = %q,%q", location.Latitude, location.Longitude)
	}
}

func TestParseHelloWeatherCodesUsesOfficialProvinceSections(t *testing.T) {
	codes := parseHelloWeatherCodes(`
		<h2>Manitoba</h2>
		<p>Winnipeg 1-833-794-3556 (79HELLO) Code: 05038</p>
		<h2>Quebec</h2>
		<p>Val-des-Sources 1-833-794-3556 (79HELLO) Code: 03038</p>
		<h2>Saskatchewan</h2>
		<p>Saskatoon 1-833-794-3556 (79HELLO) Code: 06040</p>
	`)
	tests := map[string]locationRecord{
		"05038": {Name: "Winnipeg", Province: "MB"},
		"03038": {Name: "Val-des-Sources", Province: "QC"},
		"06040": {Name: "Saskatoon", Province: "SK"},
	}
	for code, want := range tests {
		got, ok := codes[code]
		if !ok {
			t.Fatalf("missing code %s", code)
		}
		if got.Name != want.Name || got.Province != want.Province || got.Source != "hello_weather" {
			t.Fatalf("code %s = %#v", code, got)
		}
	}
}

func TestParseHelloWeatherCodesHandlesOfficialTableMarkup(t *testing.T) {
	codes := parseHelloWeatherCodes(`
		<details><summary>Nova Scotia</summary><table><tbody>
		<tr><td>Yarmouth</td><td>1-833-794-3556 (79HELLO) Code: 0102<span>9</span></td></tr>
		</tbody></table></details>
		<h2>New Brunswick</h2><table><tbody>
		<tr><td>Woodstock</td><td>1-833-794-3556 (79HELLO) Code: 0172<span>1</span></td></tr>
		</tbody></table>
		<h2>Yukon</h2><table><tbody>
		<tr><td>Dempster (Highway)</td><td>1-833-794-3556 (79HELLO) Code: 0910<span>4</span></td></tr>
		</tbody></table>
	`)
	tests := map[string]locationRecord{
		"01029": {Name: "Yarmouth", Province: "NS"},
		"01721": {Name: "Woodstock", Province: "NB"},
		"09104": {Name: "Dempster (Highway)", Province: "YT"},
	}
	for code, want := range tests {
		got, ok := codes[code]
		if !ok || got.Name != want.Name || got.Province != want.Province {
			t.Fatalf("code %s = %#v, ok=%v", code, got, ok)
		}
	}
}

func TestHelloWeatherProvinceNumberLookupCoversSharedCodeFamilies(t *testing.T) {
	cfg := loadedConfig{IVR: Config{DefaultLanguage: "en-CA"}}
	resolver := resolverWithHelloWeather(cfg,
		locationRecord{Code: "06040", Source: "hello_weather", Name: "Saskatoon", Province: "SK"},
		locationRecord{Code: "04143", Source: "hello_weather", Name: "Toronto", Province: "ON"},
		locationRecord{Code: "01723", Source: "hello_weather", Name: "Saint John", Province: "NB"},
		locationRecord{Code: "01805", Source: "hello_weather", Name: "Charlottetown", Province: "PE"},
		locationRecord{Code: "09524", Source: "hello_weather", Name: "Yellowknife", Province: "NT"},
	)
	tests := []struct {
		province string
		number   string
		want     string
	}{
		{province: "SK", number: "40", want: "06040"},
		{province: "ON", number: "143", want: "04143"},
		{province: "1", number: "723", want: "01723"},
		{province: "PE", number: "805", want: "01805"},
		{province: "9", number: "524", want: "09524"},
		{province: "NT", number: "09524", want: "09524"},
	}
	for _, test := range tests {
		got, ok := resolver.helloWeatherCodeForProvinceNumber(test.province, test.number)
		if !ok || got != test.want {
			t.Fatalf("province %s number %s = %q, ok=%v, want %s", test.province, test.number, got, ok, test.want)
		}
	}
}

func TestResolverUsesOfficialHelloWeatherDirectory(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		Geocodes: map[string]locationRecord{
			"4611040": {Code: "4611040", Source: "capcp_geocode", Name: "Winnipeg", Province: "MB", Latitude: "49.895", Longitude: "-97.138"},
		},
		Feeds: []feedXML{{ID: "mb-0001", EnabledRaw: "true", Timezone: "America/Winnipeg"}},
	}
	resolver := resolverWithHelloWeather(cfg, locationRecord{Code: "05038", Source: "hello_weather", Name: "Winnipeg", Province: "MB"})

	location, err := resolver.Resolve("05038")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.Name != "Winnipeg" || location.Province != "MB" {
		t.Fatalf("location = %#v", location)
	}
	if location.Latitude != "49.895" || location.Longitude != "-97.138" {
		t.Fatalf("coordinates = %q,%q", location.Latitude, location.Longitude)
	}
}

func TestResolverUsesLocalHelloWeatherDirectoryWithoutNetworkLookup(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		HelloWeather: map[string]locationRecord{
			"05038": {Code: "05038", Source: "hello_weather", Name: "Winnipeg", Province: "MB"},
		},
		Feeds: []feedXML{{ID: "mb-0001", EnabledRaw: "true", Timezone: "America/Winnipeg"}},
	}
	resolver := NewResolver(cfg)
	lookupCalls := 0
	resolver.lookupHelloWeather = func(context.Context) map[string]locationRecord {
		lookupCalls++
		return nil
	}

	location, err := resolver.Resolve("05038")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.Name != "Winnipeg" || location.Province != "MB" {
		t.Fatalf("location = %#v", location)
	}
	if lookupCalls != 0 {
		t.Fatalf("network directory lookup calls = %d, want 0", lookupCalls)
	}
}

func TestHelloWeatherDirectorySerializesInitialFallbackLoad(t *testing.T) {
	resolver := NewResolver(loadedConfig{})
	started := make(chan struct{})
	release := make(chan struct{})
	resolver.lookupHelloWeather = func(context.Context) map[string]locationRecord {
		close(started)
		<-release
		return map[string]locationRecord{
			"06040": {Code: "06040", Source: "hello_weather", Name: "Saskatoon", Province: "SK"},
		}
	}
	first := make(chan map[string]locationRecord, 1)
	second := make(chan map[string]locationRecord, 1)
	go func() { first <- resolver.helloWeatherDirectory() }()
	<-started
	go func() { second <- resolver.helloWeatherDirectory() }()
	select {
	case result := <-second:
		t.Fatalf("second lookup returned before initial load completed: %#v", result)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	for index, result := range []map[string]locationRecord{<-first, <-second} {
		if result["06040"].Name != "Saskatoon" {
			t.Fatalf("result %d = %#v", index, result)
		}
	}
}

func TestResolverDerivesHelloWeatherProviderWhenDirectoryUnavailable(t *testing.T) {
	cfg := loadedConfig{
		IVR:   Config{DefaultLanguage: "en-CA"},
		Feeds: []feedXML{{ID: "mb-0001", EnabledRaw: "true", Timezone: "America/Winnipeg"}},
	}
	resolver := resolverWithHelloWeather(cfg)
	calls := 0
	resolver.lookupProviderName = func(_ context.Context, forecastID string) (string, bool) {
		calls++
		if forecastID != "mb-38" {
			t.Fatalf("forecastID = %q", forecastID)
		}
		return "Winnipeg", true
	}

	location, err := resolver.Resolve("05038")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.Name != "Winnipeg" || location.Province != "MB" || location.Forecast != "mb-38" {
		t.Fatalf("location = %#v", location)
	}
	if calls != 1 {
		t.Fatalf("lookup calls = %d", calls)
	}
}

func TestResolverDerivesThreeDigitHelloWeatherProviderIDs(t *testing.T) {
	cfg := loadedConfig{
		IVR:   Config{DefaultLanguage: "en-CA"},
		Feeds: []feedXML{{ID: "default", EnabledRaw: "true", Timezone: "America/Toronto"}},
	}
	tests := map[string]struct {
		Forecast string
		Province string
		Name     string
	}{
		"08074": {Forecast: "bc-74", Province: "BC", Name: "Vancouver"},
		"04143": {Forecast: "on-143", Province: "ON", Name: "Toronto"},
		"04137": {Forecast: "on-137", Province: "ON", Name: "London"},
		"04118": {Forecast: "on-118", Province: "ON", Name: "Ottawa"},
		"03133": {Forecast: "qc-133", Province: "QC", Name: "Quebec"},
	}
	for code, want := range tests {
		t.Run(code, func(t *testing.T) {
			resolver := resolverWithHelloWeather(cfg)
			resolver.lookupProviderName = func(_ context.Context, forecastID string) (string, bool) {
				if forecastID != want.Forecast {
					t.Fatalf("forecastID = %q, want %q", forecastID, want.Forecast)
				}
				return want.Name, true
			}

			location, err := resolver.Resolve(code)
			if err != nil {
				t.Fatalf("Resolve returned error: %v", err)
			}
			if location.Forecast != want.Forecast || location.Province != want.Province || location.Name != want.Name {
				t.Fatalf("location = %#v", location)
			}
		})
	}
}

func TestResolverAcceptsProvinceAndLocationShorthand(t *testing.T) {
	cfg := loadedConfig{
		IVR:   Config{DefaultLanguage: "en-CA"},
		Feeds: []feedXML{{ID: "sk-0001", EnabledRaw: "true", Timezone: "America/Regina"}},
	}
	resolver := resolverWithHelloWeather(cfg, locationRecord{Code: "06040", Source: "hello_weather", Name: "Saskatoon", Province: "SK"})

	location, err := resolver.Resolve("640")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.Code != "06040" || location.Name != "Saskatoon" {
		t.Fatalf("location = %#v", location)
	}
}

func TestResolverPrefersHelloWeatherOverLeftPadCollision(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		ForecastLocations: map[string]locationRecord{
			"006005": {Code: "006005", Source: "eccc_forecast", Name: "AC000005", Province: "SK", Forecast: "sk-5"},
		},
		Feeds: []feedXML{{ID: "sk-0001", EnabledRaw: "true", Timezone: "America/Regina"}},
	}
	resolver := resolverWithHelloWeather(cfg, locationRecord{Code: "06005", Source: "hello_weather", Name: "Fort Qu'Appelle", Province: "SK"})

	location, err := resolver.Resolve("06005")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.Source != "hello_weather" {
		t.Fatalf("Source = %q", location.Source)
	}
	if location.Name != "Fort Qu'Appelle" {
		t.Fatalf("Name = %q", location.Name)
	}
}

func TestResolverPrefersExactForecastLocationOverHelloWeatherGuess(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		ForecastLocations: map[string]locationRecord{
			"050001": {Code: "050001", Source: "eccc_forecast", Name: "Winnipeg", Province: "MB"},
		},
		Feeds: []feedXML{{ID: "mb-0001", EnabledRaw: "true", Timezone: "America/Winnipeg"}},
	}
	resolver := resolverWithHelloWeather(cfg)
	resolver.lookupProviderName = func(context.Context, string) (string, bool) {
		t.Fatal("exact forecast location should not need provider fetch")
		return "", false
	}

	location, err := resolver.Resolve("050001")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.Source != "eccc_forecast" {
		t.Fatalf("Source = %q", location.Source)
	}
	if location.Name != "Winnipeg" || location.Province != "MB" {
		t.Fatalf("location = %#v", location)
	}
}

func TestResolverUsesCoverageRegionNameWhenLocalForecastNameIsStale(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		ForecastLocations: map[string]locationRecord{
			"000001": {Code: "000001", Source: "eccc_forecast", Name: "Saint Pierre and Miquelon", Forecast: "sk-1"},
			"065400": {Code: "065400", Source: "eccc_forecast", Name: "Martensville - Warman - Rosthern - Delisle - Wakaw", Province: "SK"},
		},
		Feeds: []feedXML{{
			ID:         "sk-0001",
			EnabledRaw: "true",
			Timezone:   "America/Regina",
			Locations: struct {
				Coverage struct {
					Regions []coverageRegionXML "xml:\"region\""
				} "xml:\"coverage\""
				ObservationLocations struct {
					Locations []feedLocationXML "xml:\"location\""
				} "xml:\"observationLocations\""
			}{
				Coverage: struct {
					Regions []coverageRegionXML "xml:\"region\""
				}{
					Regions: []coverageRegionXML{{ID: "065400", Source: "eccc", DeriveForecast: "sk-1"}},
				},
			},
		}},
	}
	resolver := resolverWithHelloWeather(cfg, locationRecord{Code: "06001", Source: "hello_weather", Name: "Martensville", Province: "SK"})
	resolver.lookupProviderName = func(context.Context, string) (string, bool) {
		t.Fatal("covered region should not need provider fetch")
		return "", false
	}

	location, err := resolver.Resolve("06001")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.Name != "Martensville" {
		t.Fatalf("Name = %q", location.Name)
	}
	if strings.Contains(location.Name, "Saint Pierre") {
		t.Fatalf("stale name leaked: %#v", location)
	}
}

func resolverWithHelloWeather(cfg loadedConfig, records ...locationRecord) *Resolver {
	resolver := NewResolver(cfg)
	codes := map[string]locationRecord{}
	for _, record := range records {
		codes[record.Code] = record
	}
	resolver.lookupHelloWeather = func(context.Context) map[string]locationRecord {
		return codes
	}
	return resolver
}

func TestResolverRejectsKnownLocationWhenNoFeedProfileExists(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		CLCs: map[string]locationRecord{
			"065522": {Code: "065522", Source: "clc", Name: "Outlook", Province: "SK"},
		},
	}

	_, err := NewResolver(cfg).Resolve("065522")
	if err == nil {
		t.Fatal("Resolve succeeded without an enabled feed profile")
	}
}

func TestT9StationLookup(t *testing.T) {
	cfg := loadedConfig{
		IVR: Config{DefaultLanguage: "en-CA"},
		Feeds: []feedXML{
			{
				ID:         "sk-0001",
				EnabledRaw: "true",
				Locations: struct {
					Coverage struct {
						Regions []coverageRegionXML "xml:\"region\""
					} "xml:\"coverage\""
					ObservationLocations struct {
						Locations []feedLocationXML "xml:\"location\""
					} "xml:\"observationLocations\""
				}{
					ObservationLocations: struct {
						Locations []feedLocationXML "xml:\"location\""
					}{
						Locations: []feedLocationXML{{ID: "CYXE", Source: "eccc", NameOverride: "Saskatoon Airport"}},
					},
				},
			},
		},
	}

	location, err := NewResolver(cfg).Resolve("2993")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}
	if location.StationID != "CYXE" {
		t.Fatalf("StationID = %q", location.StationID)
	}
}
