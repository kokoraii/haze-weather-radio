package ivr

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMediaServiceURLForBroadcastPrefersPacedMediaService(t *testing.T) {
	cfg := loadedConfig{}
	cfg.Root.Services.Rust.Media.Enabled = true
	cfg.Root.Services.Rust.Media.Addr = "127.0.0.1:8097"

	service := &Service{cfg: cfg}
	if got := service.mediaServiceURLForBroadcast(); got != "http://127.0.0.1:8097" {
		t.Fatalf("media service broadcast URL = %q, want HTTP fallback", got)
	}

	service.mediaBridge = &bridgeClient{}
	if got := service.mediaServiceURLForBroadcast(); got != "http://127.0.0.1:8097" {
		t.Fatalf("media service broadcast URL with media bridge = %q, want paced HTTP source", got)
	}
}

func TestWriteProductTwiMLDoesNotPreRenderOnCacheMiss(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
			DefaultPackages: []string{"current_conditions"},
		},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")

	request := httptest.NewRequest("POST", "http://ivr.test/ivr/v1/twiml", nil)
	response := httptest.NewRecorder()
	service.writeProductTwiML(response, request, ResolvedLocation{
		Code:     "06040",
		FeedID:   "sk-0001",
		Language: "en-CA",
		Name:     "Saskatoon",
	}, []string{"forecast"}, "http://ivr.test/next")

	body := response.Body.String()
	if strings.Contains(body, "unavailable") {
		t.Fatalf("product TwiML should not fail before the audio URL can render: %s", body)
	}
	if !strings.Contains(body, "/ivr/v1/prompt?line=one_moment") {
		t.Fatalf("cache miss should play one_moment before audio URL: %s", body)
	}
	if !strings.Contains(body, "/ivr/v1/audio") || !strings.Contains(body, "packages=forecast") {
		t.Fatalf("product TwiML did not include audio URL with package: %s", body)
	}
	if !strings.Contains(body, "<Redirect>http://ivr.test/next</Redirect>") {
		t.Fatalf("product TwiML did not preserve redirect: %s", body)
	}
}

func TestProductCacheKeyUsesCallerLanguageWhenMenuDoesNotOverride(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
			DefaultPackages: []string{"forecast"},
		},
		Prompts: defaultPromptConfig(),
	}
	cache := NewProductCache(cfg, nil)
	location := ResolvedLocation{Code: "06040", FeedID: "sk-0001", Language: "en-CA"}
	enKey, _, enLang, _, _ := cache.productCacheKey(location, []string{"forecast"})
	location.Language = "fr-CA"
	frKey, _, frLang, _, _ := cache.productCacheKey(location, []string{"forecast"})

	if enLang != "en-CA" || frLang != "fr-CA" {
		t.Fatalf("languages = %q/%q", enLang, frLang)
	}
	if enKey == frKey {
		t.Fatalf("language-specific IVR products must not share a cache key")
	}
}

func TestProductCacheKeyIncludesResolvedForecastAndStation(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
			DefaultPackages: []string{"current_conditions"},
		},
		Prompts: defaultPromptConfig(),
	}
	cache := NewProductCache(cfg, nil)
	location := ResolvedLocation{Code: "065500", Source: "eccc_forecast", FeedID: "sk-0001", Language: "en-CA", Forecast: "sk-37"}
	firstKey, _, _, _, _ := cache.productCacheKey(location, []string{"current_conditions"})
	location.Forecast = "sk-40"
	secondKey, _, _, _, _ := cache.productCacheKey(location, []string{"current_conditions"})
	if firstKey == secondKey {
		t.Fatalf("different forecast/station targets must not share IVR cache keys")
	}
}

func TestEntryMenuDoesNotAnnounceBroadcastWhenRadioFeedIsAvailable(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage:   "en-CA",
			DefaultPackages:   []string{"current_conditions"},
			BroadcastPackages: []string{"alerts", "forecast"},
		},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA", "fr-CA", "es")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")

	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/twiml", nil)
	response := httptest.NewRecorder()
	service.writeEntryTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "line=main") || strings.Contains(body, "main_no_broadcast") {
		t.Fatalf("entry menu did not use the main non-broadcast line: %s", body)
	}
	text := service.cfg.Prompts.MenuLine("entry", "main", service.promptValues(nil))
	if strings.Contains(strings.ToLower(text), "broadcast") || strings.Contains(strings.ToLower(text), "radiomet") {
		t.Fatalf("entry menu still announces a broadcast option: %q", text)
	}
}

func TestEntryMenuSkipsLanguageWhenOnlyOneLanguageConfigured(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()

	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/twiml", nil)
	response := httptest.NewRecorder()
	service.writeEntryTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "line=main_single_language") {
		t.Fatalf("single-language entry should use direct location prompt: %s", body)
	}
	if strings.Contains(body, "numDigits=\"1\"") {
		t.Fatalf("single-language entry should collect a location code, not one language digit: %s", body)
	}
}

func TestProvinceExtensionSkipsCanadaProvincePrompt(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
			Extensions:      []extensionConfig{{Extension: "600", Name: "Saskatchewan", NamePosition: "before", Province: "SK"}},
		},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA")},
		Prompts: defaultPromptConfig(),
	}
	cfg.Root.Operator.TelephoneName = "TeleWeather"
	service := &Service{cfg: cfg}
	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/twiml?extension=600", nil)
	response := httptest.NewRecorder()

	service.writeEntryTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "state=location_number") || !strings.Contains(body, "province=SK") {
		t.Fatalf("province line did not route directly to its location numbers: %s", body)
	}
	if strings.Contains(body, "state=location_code") || strings.Contains(body, "main_single_language") {
		t.Fatalf("province line fell through to the Canada province prompt: %s", body)
	}
	if !strings.Contains(body, "line=greeting") || !strings.Contains(body, "extension_telephone_service_name=Saskatchewan+TeleWeather") {
		t.Fatalf("province line greeting missing or incorrect: %s", body)
	}
}

func TestDisabledProvinceExtensionHangsUp(t *testing.T) {
	disabled := false
	cfg := loadedConfig{
		IVR: Config{Extensions: []extensionConfig{
			{Extension: "haze", Province: "CA"},
			{Extension: "800", Province: "BC", Enabled: &disabled},
		}},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/twiml?extension=800", nil)
	response := httptest.NewRecorder()

	service.writeEntryTwiML(response, request)

	if body := response.Body.String(); !strings.Contains(body, "<Hangup/>") || strings.Contains(body, "<Gather") {
		t.Fatalf("disabled province line response = %s", body)
	}
}

func TestEntryDigitRejectsUnconfiguredLanguage(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA", "fr-CA")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=entry", url.Values{"Digits": {"3"}})
	response := httptest.NewRecorder()
	service.handleEntryDigit(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "line=invalid_code") {
		t.Fatalf("Spanish should be rejected when only English/French are configured: %s", body)
	}
}

func TestEntryStarStartsGeophysicalAlert(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA", "fr-CA")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=entry", url.Values{"Digits": {"*"}})
	response := httptest.NewRecorder()
	service.handleEntryDigit(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "/ivr/v1/audio") || !strings.Contains(body, "packages=geophysical_alert") {
		t.Fatalf("entry star did not start geophysical alert product: %s", body)
	}
}

func TestSingleLanguageEntryDigitTreatsDigitsAsLocationCode(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()
	service.resolver = resolverWithHelloWeather(cfg, locationRecord{Code: "06040", Source: "hello_weather", Name: "Saskatoon", Province: "SK"})

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=entry", url.Values{"Digits": {"06040"}})
	response := httptest.NewRecorder()
	service.handleEntryDigit(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "state=location_option") || !strings.Contains(body, "lang=en-CA") {
		t.Fatalf("single-language entry did not resolve location code: %s", body)
	}
}

func TestSingleLanguageEntryDigitPromptsForLocationNumberAfterProvince(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()
	service.resolver = resolverWithHelloWeather(cfg, locationRecord{Code: "06040", Source: "hello_weather", Name: "Saskatoon", Province: "SK"})

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=entry", url.Values{"Digits": {"6"}})
	response := httptest.NewRecorder()
	service.handleEntryDigit(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "state=location_number") || !strings.Contains(body, "province=6") {
		t.Fatalf("province digit did not route to location number prompt: %s", body)
	}
}

func TestLocationNumberCombinesWithProvince(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()
	service.resolver = resolverWithHelloWeather(cfg, locationRecord{Code: "06040", Source: "hello_weather", Name: "Saskatoon", Province: "SK"})

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=location_number&province=6&lang=en-CA", url.Values{"Digits": {"40"}})
	response := httptest.NewRecorder()
	service.handleLocationNumberTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "state=location_option") || !strings.Contains(body, "code=06040") {
		t.Fatalf("location number did not resolve official code: %s", body)
	}
}

func TestLocationCodeKeepsIncompleteHelloWeatherShortCodeOpen(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("qc-0001", "en-CA")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()
	service.resolver = resolverWithHelloWeather(cfg,
		locationRecord{Code: "03013", Source: "hello_weather", Name: "Saint-Jérôme", Province: "QC", Forecast: "qc-13"},
		locationRecord{Code: "03133", Source: "hello_weather", Name: "Quebec", Province: "QC", Forecast: "qc-133"},
	)

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=location_code&lang=en-CA", url.Values{"Digits": {"313"}})
	response := httptest.NewRecorder()
	service.handleLocationCodeTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "state=location_code") || !strings.Contains(body, "prefix=313") || strings.Contains(body, "state=location_option") {
		t.Fatalf("incomplete short code was resolved early: %s", body)
	}
}

func TestProvinceLineAcceptsThreeDigitLocationNumber(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("on-0001", "en-CA")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()
	service.resolver = resolverWithHelloWeather(cfg, locationRecord{Code: "04143", Source: "hello_weather", Name: "Toronto", Province: "ON"})

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=location_number&province=ON&lang=en-CA", url.Values{"Digits": {"143"}})
	response := httptest.NewRecorder()
	service.handleLocationNumberTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "state=location_option") || !strings.Contains(body, "code=04143") {
		t.Fatalf("three-digit province location did not resolve: %s", body)
	}
}

func TestLocationNumberStarReturnsSearchPlaceholder(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=location_number&province=6&lang=en-CA", url.Values{"Digits": {"*"}})
	response := httptest.NewRecorder()
	service.handleLocationNumberTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "search_unavailable") || !strings.Contains(body, "state=location_code") {
		t.Fatalf("star did not play search placeholder and return to location prompt: %s", body)
	}
}

func TestEntryDigitFourIsInvalid(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA", "fr-CA", "es")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=entry", url.Values{"Digits": {"4"}})
	response := httptest.NewRecorder()
	service.handleEntryDigit(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "line=invalid_code") {
		t.Fatalf("entry digit 4 should be invalid after removing main-menu broadcast: %s", body)
	}
	if strings.Contains(body, "packages=") {
		t.Fatalf("entry digit 4 should not start a product or broadcast flow: %s", body)
	}
}

func TestEntryDigitFiveIsInvalid(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA", "fr-CA", "es")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=entry", url.Values{"Digits": {"5"}})
	response := httptest.NewRecorder()
	service.handleEntryDigit(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "line=invalid_code") {
		t.Fatalf("entry digit 5 should be invalid after moving geophysical alert to 0: %s", body)
	}
	if strings.Contains(body, "packages=") {
		t.Fatalf("entry digit 5 should not start a product flow: %s", body)
	}
}

func TestEntryDigitZeroStartsGeophysicalAlert(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR:     Config{DefaultLanguage: "en-CA"},
		Feeds:   []feedXML{testFeedWithLanguages("sk-0001", "en-CA", "fr-CA", "es")},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=entry", url.Values{"Digits": {"0"}})
	response := httptest.NewRecorder()
	service.handleEntryDigit(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "/ivr/v1/audio") || !strings.Contains(body, "packages=geophysical_alert") {
		t.Fatalf("entry digit 0 did not start geophysical alert product: %s", body)
	}
	if strings.Contains(body, "operator") {
		t.Fatalf("entry digit 0 should no longer route to operator: %s", body)
	}
}

func TestSpokenLocationNameSuppressesProviderIDs(t *testing.T) {
	location := ResolvedLocation{Name: "sk-32", Forecast: "sk-32", Code: "06032"}
	if got := spokenLocationName(location); got != "the selected area" {
		t.Fatalf("provider ID location = %q", got)
	}
	location.Name = "Regina"
	if got := spokenLocationName(location); got != "Regina" {
		t.Fatalf("friendly location = %q", got)
	}
}

func TestLocationOptionPoundReturnsToEntryMenu(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Feeds:   []feedXML{{ID: "sk-0001", EnabledRaw: "true"}},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = newBroadcastHub()
	service.resolver = NewResolver(cfg)

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=location_option&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"#"}})
	response := httptest.NewRecorder()
	service.handleLocationOptionTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "state=entry") {
		t.Fatalf("pound did not return to entry menu: %s", body)
	}
	if strings.Contains(body, "location_option") {
		t.Fatalf("pound should not stay in location menu: %s", body)
	}
}

func TestLocationMenuHidesBroadcastForUncoveredDefaultRenderedLocation(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Feeds:   []feedXML{{ID: "sk-0001", EnabledRaw: "true"}},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")

	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/twiml", nil)
	response := httptest.NewRecorder()
	service.writeLocationMenu(response, request, ResolvedLocation{
		Code:     "05038",
		Source:   "hello_weather",
		Name:     "Winnipeg",
		Province: "MB",
		FeedID:   "sk-0001",
		Language: "en-CA",
		Covered:  false,
	})

	body := response.Body.String()
	if !strings.Contains(body, "main_no_broadcast") {
		t.Fatalf("uncovered location should use no-broadcast menu line: %s", body)
	}
}

func TestLocationMenuHidesBroadcastForNearbyLocationOutsideCoverage(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
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
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")
	service.resolver = resolverWithHelloWeather(cfg, locationRecord{Code: "06041", Source: "hello_weather", Name: "Nearby test location", Province: "SK", Forecast: "sk-41"})

	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/twiml?state=location_menu&code=06041&lang=en-CA", nil)
	response := httptest.NewRecorder()
	service.writeLocationMenuTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "main_no_broadcast") {
		t.Fatalf("nearby outside-coverage location should use no-broadcast menu line: %s", body)
	}
	if strings.Contains(body, "line=main&") {
		t.Fatalf("nearby outside-coverage location should not use broadcast-capable menu line: %s", body)
	}
}

func TestLocationMenuAllowsBroadcastForCoveredLocation(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Feeds:   []feedXML{{ID: "sk-0001", EnabledRaw: "true"}},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")

	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/twiml", nil)
	response := httptest.NewRecorder()
	service.writeLocationMenu(response, request, ResolvedLocation{
		Code:     "06040",
		Source:   "hello_weather",
		Name:     "Saskatoon",
		Province: "SK",
		FeedID:   "sk-0001",
		Language: "en-CA",
		Covered:  true,
	})

	body := response.Body.String()
	if !strings.Contains(body, "line=main") || strings.Contains(body, "main_no_broadcast") {
		t.Fatalf("covered location should use broadcast-capable menu line: %s", body)
	}
}

func TestLocationMenuDigitZeroStartsBroadcast(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Feeds:   []feedXML{{ID: "sk-0001", EnabledRaw: "true"}},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")
	service.resolver = NewResolver(cfg)

	request := formRequest("http://ivr.test/ivr/v1/twiml?state=location_option&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"0"}})
	response := httptest.NewRecorder()
	service.handleLocationOptionTwiML(response, request)

	body := response.Body.String()
	if !strings.Contains(body, "/ivr/v1/audio") || !strings.Contains(body, "packages=alerts%2Ccurrent_conditions%2Cair_quality%2Cforecast%2Cgeophysical_alert") {
		t.Fatalf("digit 0 did not start the broadcast fallback package: %s", body)
	}
	if strings.Contains(body, "operator") {
		t.Fatalf("digit 0 should not route to operator: %s", body)
	}
}

func TestLocationMenuNewProductDigits(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Feeds:   []feedXML{{ID: "sk-0001", EnabledRaw: "true"}},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")
	service.resolver = NewResolver(cfg)

	tests := []struct {
		digit   string
		pkg     string
		state   string
		wantURL string
	}{
		{digit: "4", pkg: "climate_summary", wantURL: "/ivr/v1/audio"},
		{digit: "5", pkg: "thunderstorm_outlook", wantURL: "/ivr/v1/audio"},
		{digit: "6", state: "ivr_menu", wantURL: "/ivr/v1/prompt"},
	}
	for _, tt := range tests {
		t.Run(tt.digit, func(t *testing.T) {
			request := formRequest("http://ivr.test/ivr/v1/twiml?state=location_option&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {tt.digit}})
			response := httptest.NewRecorder()
			service.handleLocationOptionTwiML(response, request)

			body := response.Body.String()
			if !strings.Contains(body, tt.wantURL) {
				t.Fatalf("digit %s response missing %s: %s", tt.digit, tt.wantURL, body)
			}
			if tt.pkg != "" && !strings.Contains(body, "packages="+tt.pkg) {
				t.Fatalf("digit %s response missing package %s: %s", tt.digit, tt.pkg, body)
			}
			if tt.state != "" && !strings.Contains(body, "state="+tt.state) {
				t.Fatalf("digit %s response missing state %s: %s", tt.digit, tt.state, body)
			}
		})
	}
}

func TestSpecialtyMenuProductAndPound(t *testing.T) {
	cfg := loadedConfig{
		BaseDir: t.TempDir(),
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Feeds:   []feedXML{{ID: "sk-0001", EnabledRaw: "true"}},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	service.cache = NewProductCache(cfg, nil)
	service.broadcast = recentBroadcastHub("sk-0001")
	service.resolver = NewResolver(cfg)

	productRequest := formRequest("http://ivr.test/ivr/v1/twiml?state=ivr_menu_option&menu=specialty_menu&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"1"}})
	productResponse := httptest.NewRecorder()
	service.handleConfiguredMenuOptionTwiML(productResponse, productRequest)

	productBody := productResponse.Body.String()
	if !strings.Contains(productBody, "/ivr/v1/audio") || !strings.Contains(productBody, "packages=hydrometric") {
		t.Fatalf("specialty menu digit 1 did not start hydrometric: %s", productBody)
	}
	if !strings.Contains(productBody, "state=ivr_menu") || !strings.Contains(productBody, "menu=specialty_menu") {
		t.Fatalf("specialty menu product should return to specialty menu: %s", productBody)
	}

	backRequest := formRequest("http://ivr.test/ivr/v1/twiml?state=ivr_menu_option&menu=specialty_menu&feed_id=sk-0001&lang=en-CA", url.Values{"Digits": {"#"}})
	backResponse := httptest.NewRecorder()
	service.handleConfiguredMenuOptionTwiML(backResponse, backRequest)

	backBody := backResponse.Body.String()
	if !strings.Contains(backBody, "state=location_option") || strings.Contains(backBody, "menu=specialty_menu") {
		t.Fatalf("pound should return from specialty menu to location menu: %s", backBody)
	}
}

func TestServePromptAudioPreservesFallbackStatus(t *testing.T) {
	service := staticPromptTestService(t, "error__invalid_code", "No match. Try again.")

	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/audio", nil)
	response := httptest.NewRecorder()
	service.servePromptAudio(response, request, "error", "invalid_code", nil, http.StatusBadGateway)

	if response.Code != http.StatusBadGateway {
		t.Fatalf("fallback status = %d, want %d", response.Code, http.StatusBadGateway)
	}
	if response.Body.String() != "wav" {
		t.Fatalf("fallback body = %q", response.Body.String())
	}
}

func TestStaticPromptPolicyUsesConfiguredDefaultReader(t *testing.T) {
	service := &Service{cfg: loadedConfig{
		IVR: Config{
			DefaultLanguage: "en-US",
			DefaultReaderID: "some-sapi-reader",
		},
		Prompts: defaultPromptConfig(),
	}}
	policy := service.staticPromptPolicy()
	if policy.ReaderID != "some-sapi-reader" || policy.Provider != "fast" || policy.Language != "en-US" {
		t.Fatalf("static policy = %+v", policy)
	}
	if policy.SentenceSilence != 0 {
		t.Fatalf("static prompts must not pass sentence silence unless configured, got %f", policy.SentenceSilence)
	}
}

func TestPromptValuesConfiguredServiceNamesOverrideStaticDefaults(t *testing.T) {
	service := &Service{cfg: loadedConfig{}}
	service.cfg.Root.Operator.OnAirName = map[string]any{"text": "Configured Radio"}
	service.cfg.Root.Operator.TelephoneName = map[string]any{"text": "Configured Telephone"}
	values := service.promptValues(map[string]string{
		"telephone_service_name": "Haze Weather Telephone",
		"radio_service_name":     "Haze Weather Radio",
	})
	if values["telephone_service_name"] != "Configured Telephone" || values["radio_service_name"] != "Configured Radio" {
		t.Fatalf("service names = %#v", values)
	}
}

func TestStaticPromptFingerprintChangesWithIVRXML(t *testing.T) {
	dir := t.TempDir()
	promptsPath := filepath.Join(dir, "ivr.xml")
	if err := os.WriteFile(promptsPath, []byte(validPromptXML()), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := loadedConfig{
		BaseDir:     dir,
		PromptsPath: promptsPath,
		IVR:         Config{DefaultLanguage: "en-CA"},
		Prompts:     defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	policy := service.staticPromptPolicy()
	first, err := service.staticPromptFingerprint(cfg.Prompts.StaticPromptLines(), policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(promptsPath, []byte(strings.Replace(validPromptXML(), "Entry.", "Entry changed.", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := service.staticPromptFingerprint(cfg.Prompts.StaticPromptLines(), policy)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("fingerprint did not change after ivr.xml changed: %s", first)
	}
}

func TestStaticPromptFingerprintChangesWithReaderVoiceSettings(t *testing.T) {
	dir := t.TempDir()
	readersPath := filepath.Join(dir, "readers.xml")
	if err := os.WriteFile(readersPath, []byte(`<Readers><reader id="00" provider="kokoro"><gender>male</gender><language>en-US</language><voice_id>5</voice_id></reader></Readers>`), 0o644); err != nil {
		t.Fatal(err)
	}
	var root rootConfig
	root.Services.Go.TTS.Readers = "readers.xml"
	cfg := loadedConfig{
		Root:    root,
		BaseDir: dir,
		IVR: Config{
			DefaultLanguage: "en-US",
			DefaultReaderID: "00",
		},
		Prompts: defaultPromptConfig(),
	}
	service := &Service{cfg: cfg}
	policy := service.staticPromptPolicy()
	first, err := service.staticPromptFingerprint(cfg.Prompts.StaticPromptLines(), policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readersPath, []byte(`<Readers><reader id="00" provider="kokoro"><gender>male</gender><language>en-US</language><voice_id>7</voice_id></reader></Readers>`), 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := service.staticPromptFingerprint(cfg.Prompts.StaticPromptLines(), policy)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("fingerprint did not change after reader voice changed: %s", first)
	}
}

func TestStaticPromptManifestCurrentRequiresMatchingFingerprintAndFiles(t *testing.T) {
	dir := t.TempDir()
	wav := filepath.Join(dir, "entry__main.wav")
	pcmu := filepath.Join(dir, "entry__main.pcmu")
	for _, path := range []string{wav, pcmu} {
		if err := os.WriteFile(path, []byte("audio"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := writeStaticPromptManifest(manifestPath, staticPromptManifest{
		Version:     staticPromptManifestVersion,
		Fingerprint: "abc",
		Files: map[string]staticPromptFile{
			"entry__main": {WAV: filepath.ToSlash(wav), PCMU: filepath.ToSlash(pcmu)},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if !staticPromptManifestCurrent(manifestPath, "abc") {
		t.Fatal("manifest should be current with matching fingerprint and files")
	}
	if staticPromptManifestCurrent(manifestPath, "def") {
		t.Fatal("manifest should not be current with a different fingerprint")
	}
	if err := os.Remove(pcmu); err != nil {
		t.Fatal(err)
	}
	if staticPromptManifestCurrent(manifestPath, "abc") {
		t.Fatal("manifest should not be current after a listed file is removed")
	}
}

func TestStaticPromptAudioUsesManifestWhenTextMatches(t *testing.T) {
	service := staticPromptTestService(t, "default__one_moment", "One moment.")
	audio, ok := service.staticPromptAudio("", "one_moment", service.promptValues(nil))
	if !ok {
		t.Fatal("static prompt was not found")
	}
	if audio.PCMUPath == "" || audio.WAVPath == "" {
		t.Fatalf("audio paths = %#v", audio)
	}
}

func TestStaticPromptAudioRejectsStaleManifestText(t *testing.T) {
	service := staticPromptTestService(t, "default__one_moment", "Old text.")
	if _, ok := service.staticPromptAudio("", "one_moment", service.promptValues(nil)); ok {
		t.Fatal("stale static prompt should not be used")
	}
}

func TestHandlePromptServesStaticPromptBeforeTTSCache(t *testing.T) {
	service := staticPromptTestService(t, "default__one_moment", "One moment.")
	request := httptest.NewRequest(http.MethodGet, "http://ivr.test/ivr/v1/prompt?line=one_moment&format=pcmu", nil)
	response := httptest.NewRecorder()

	service.handlePrompt(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%q", response.Code, response.Body.String())
	}
	if response.Body.String() != "pcmu" {
		t.Fatalf("body = %q", response.Body.String())
	}
}

func formRequest(target string, values url.Values) *http.Request {
	request := httptest.NewRequest(http.MethodPost, target, strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return request
}

func recentBroadcastHub(feedID string) *broadcastHub {
	hub := newBroadcastHub()
	hub.publish(broadcastPCMChunk{
		FeedID:     feedID,
		SampleRate: 8000,
		Channels:   1,
		Data:       make([]byte, 320),
	})
	return hub
}

func staticPromptTestService(t *testing.T, key string, text string) *Service {
	t.Helper()
	dir := t.TempDir()
	service := &Service{cfg: loadedConfig{
		BaseDir: dir,
		IVR: Config{
			DefaultLanguage: "en-CA",
		},
		Prompts: defaultPromptConfig(),
	}}
	policy := service.staticPromptPolicy()
	fingerprint, err := service.staticPromptFingerprint(service.cfg.Prompts.StaticPromptLines(), policy)
	if err != nil {
		t.Fatal(err)
	}
	staticDir := filepath.Join(dir, "audio", "ivr")
	if err := os.MkdirAll(staticDir, 0o755); err != nil {
		t.Fatal(err)
	}
	wav := filepath.Join(staticDir, key+".wav")
	pcmu := filepath.Join(staticDir, key+".pcmu")
	g722 := filepath.Join(staticDir, key+".g722")
	for path, body := range map[string]string{wav: "wav", pcmu: "pcmu", g722: "g722"} {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := writeStaticPromptManifest(filepath.Join(staticDir, "manifest.json"), staticPromptManifest{
		Version:     staticPromptManifestVersion,
		Fingerprint: fingerprint,
		Provider:    policy.Provider,
		ReaderID:    policy.ReaderID,
		VoiceID:     policy.VoiceID,
		Language:    policy.Language,
		GeneratedAt: time.Now().UTC(),
		Files: map[string]staticPromptFile{
			key: {
				Text: text,
				WAV:  filepath.ToSlash(wav),
				PCMU: filepath.ToSlash(pcmu),
				G722: filepath.ToSlash(g722),
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	return service
}

func testFeedWithLanguages(id string, languages ...string) feedXML {
	feed := feedXML{ID: id, EnabledRaw: "true"}
	for _, language := range languages {
		feed.Languages.Langs = append(feed.Languages.Langs, struct {
			Code string `xml:"code,attr"`
		}{Code: language})
	}
	return feed
}
