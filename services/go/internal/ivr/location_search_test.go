package ivr

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

func testLocationSearchIndex(t *testing.T) *locationSearchIndex {
	t.Helper()
	snapshot := locationdb.Snapshot{
		Places: []locationdb.Place{
			{Source: "forecast", Code: "SK-40", Name: "City of Saskatoon", NameFR: "Saskatoon", Region: "SK", Country: "CA", Kind: "forecast"},
			{Source: "hello_weather", Code: "06040", Name: "Saskatoon", Region: "SK", Country: "CA", Kind: "telephone", Attrs: map[string]any{"aliases": []any{"YXE"}}},
			{Source: "forecast", Code: "QC-1", Name: "Québec", NameFR: "Québec", Region: "QC", Country: "CA", Kind: "forecast"},
			{Source: "forecast", Code: "MB-1", Name: "The Pas", Region: "MB", Country: "CA", Kind: "forecast"},
			{Source: "nws_zone", Code: "ILZ001", Name: "Springfield", Region: "IL", Country: "US", Kind: "zone", Lat: 39.8, Lon: -89.6},
			{Source: "nws_zone", Code: "MOZ001", Name: "Springfield", Region: "MO", Country: "US", Kind: "zone", Lat: 37.2, Lon: -93.3},
			{Source: "clc", Code: "000002", Name: "MN000002", Region: "MN", Country: "US", Kind: "zone", Lat: 45, Lon: -94},
		},
		Links:        []locationdb.Link{{FromSource: "forecast", FromCode: "SK-40", ToSource: "clc", ToCode: "065400", Score: 1}},
		StationLinks: []locationdb.StationLink{{AreaSource: "forecast", AreaCode: "SK-40", StationID: "CYXE", StationName: "Saskatoon Diefenbaker"}},
	}
	cfg := loadedConfig{IVR: Config{DefaultLanguage: "en-US"}, Feeds: []feedXML{{ID: "test", EnabledRaw: "true", Timezone: "UTC"}}}
	index, err := newLocationSearchIndex(snapshot, NewResolver(cfg))
	if err != nil {
		t.Fatalf("newLocationSearchIndex: %v", err)
	}
	return index
}

func TestLocationSearchVoiceNormalizesNamesAndRegions(t *testing.T) {
	index := testLocationSearchIndex(t)
	tests := []struct {
		name   string
		query  string
		region string
		code   string
	}{
		{name: "municipal prefix", query: "weather for Saskatoon Saskatchewan", code: "06040"},
		{name: "accent insensitive", query: "Quebec", code: "QC-1"},
		{name: "explicit state", query: "Springfield Missouri", region: "MO", code: "MOZ001"},
		{name: "alias", query: "YXE", code: "06040"},
		{name: "station link name", query: "Saskatoon Diefenbaker", code: "06040"},
		{name: "name article preserved", query: "The Pas", code: "MB-1"},
		{name: "fuzzy typo", query: "Saskaton", code: "06040"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matches := index.SearchVoice(test.query, locationSearchContext{Language: "en-US"})
			if len(matches) == 0 || matches[0].Target.Location.Code != test.code {
				t.Fatalf("SearchVoice(%q) = %#v", test.query, matches)
			}
			if test.region != "" && matches[0].Target.Location.Province != test.region {
				t.Fatalf("SearchVoice(%q) region = %q", test.query, matches[0].Target.Location.Province)
			}
		})
	}
}

func TestT9RegionQualifierUsesWordBoundary(t *testing.T) {
	query, context := extractT9SearchRegion("77746434353 64776874", locationSearchContext{})
	if query != "77746434353" || context.Region != "MO" || !context.ExplicitRegion {
		t.Fatalf("query = %q context = %#v", query, context)
	}
}

func TestLocationSearchDoesNotAutoAcceptHomonym(t *testing.T) {
	index := testLocationSearchIndex(t)
	matches := index.SearchVoice("Springfield", locationSearchContext{Language: "en-US"})
	if len(matches) < 2 {
		t.Fatalf("matches = %#v", matches)
	}
	if shouldAutoAcceptLocation(matches, locationSearchContext{}) {
		t.Fatal("homonymous location was auto-accepted")
	}
	context := locationSearchContext{Region: "MO", ExplicitRegion: true, Language: "en-US"}
	matches = index.SearchVoice("Springfield Missouri", context)
	if !shouldAutoAcceptLocation(matches, context) {
		t.Fatal("unique region-qualified exact location was not auto-accepted")
	}
}

func TestLocationSearchOpaqueLabelsAreExactOnly(t *testing.T) {
	index := testLocationSearchIndex(t)
	if matches := index.SearchVoice("MN000003", locationSearchContext{}); len(matches) != 0 {
		t.Fatalf("fuzzy code label matches = %#v", matches)
	}
	if matches := index.SearchVoice("MN000002", locationSearchContext{}); len(matches) == 0 || matches[0].Target.Location.Code != "000002" {
		t.Fatalf("exact code label matches = %#v", matches)
	}
}

func TestLocationSearchKeypadInfersT9AndMultitap(t *testing.T) {
	index := testLocationSearchIndex(t)
	base := time.Unix(0, 0)
	t9Input := newLocationSearchKeypad(700 * time.Millisecond)
	for offset, digit := range "727528666" {
		t9Input.Feed(string(digit), base.Add(time.Duration(offset)*100*time.Millisecond))
	}
	t9Matches := index.SearchKeypad(t9Input, locationSearchContext{Region: "SK"})
	if len(t9Matches) == 0 || t9Matches[0].Target.Location.Code != "06040" || t9Matches[0].Method != locationSearchT9 {
		t.Fatalf("T9 matches = %#v", t9Matches)
	}

	multiInput := newLocationSearchKeypad(700 * time.Millisecond)
	sequence := []struct {
		digit string
		delay time.Duration
	}{
		{"7", 0}, {"7", 100 * time.Millisecond}, {"7", 200 * time.Millisecond}, {"7", 300 * time.Millisecond},
		{"2", time.Second}, {"7", 2 * time.Second}, {"7", 2100 * time.Millisecond}, {"7", 2200 * time.Millisecond}, {"7", 2300 * time.Millisecond},
	}
	for _, item := range sequence {
		multiInput.Feed(item.digit, base.Add(item.delay))
	}
	if got := multiInput.Multitap(); got != "sas" {
		t.Fatalf("multitap = %q", got)
	}
	if multiInput.MethodHint() != locationSearchMultitap {
		t.Fatalf("method hint = %q", multiInput.MethodHint())
	}
	if !multiInput.Edit() || multiInput.Multitap() != "sa" {
		t.Fatalf("multitap after edit = %q", multiInput.Multitap())
	}
}

func TestLocationSearchMultitapSpellsCompleteLocation(t *testing.T) {
	index := testLocationSearchIndex(t)
	input := newLocationSearchKeypad(700 * time.Millisecond)
	groups := []string{"7777", "2", "7777", "55", "2", "8", "666", "666", "66"}
	at := time.Unix(0, 0)
	previousKey := byte(0)
	for _, group := range groups {
		if previousKey == group[0] {
			at = at.Add(800 * time.Millisecond)
		} else if previousKey != 0 {
			at = at.Add(100 * time.Millisecond)
		}
		for index, digit := range group {
			if index > 0 {
				at = at.Add(100 * time.Millisecond)
			}
			input.Feed(string(digit), at)
		}
		previousKey = group[0]
	}
	if got := input.Multitap(); got != "saskatoon" {
		t.Fatalf("multitap = %q", got)
	}
	matches := index.SearchKeypad(input, locationSearchContext{Region: "SK"})
	if len(matches) == 0 || matches[0].Target.Location.Code != "06040" || matches[0].Method != locationSearchMultitap {
		t.Fatalf("multitap location matches = %#v", matches)
	}
}

func TestLocationSearchFrenchDisplayName(t *testing.T) {
	index := testLocationSearchIndex(t)
	matches := index.SearchVoice("Quebec", locationSearchContext{Language: "fr-CA"})
	if len(matches) == 0 || matches[0].DisplayName != "Québec" {
		t.Fatalf("French matches = %#v", matches)
	}
}

func TestLocationSearchSessionLocksFirstSubstantiveModality(t *testing.T) {
	index := testLocationSearchIndex(t)
	context := locationSearchContext{Region: "SK", Language: "en-CA"}
	session := newLocationSearchSession(index, context, 700*time.Millisecond)
	start := time.Unix(100, 0)
	if !session.FeedDigit("7", start) {
		t.Fatal("first keypad letter was not accepted")
	}
	if session.LockVoice(start.Add(time.Millisecond)) {
		t.Fatal("later voice onset replaced keypad lock")
	}
	if session.Method() != locationSearchT9 {
		t.Fatalf("unexpected locked method %q", session.Method())
	}

	voiceFirst := newLocationSearchSession(index, context, 700*time.Millisecond)
	if !voiceFirst.LockVoice(start) {
		t.Fatal("voice onset did not lock attempt")
	}
	if voiceFirst.FeedDigit("7", start.Add(time.Millisecond)) {
		t.Fatal("later keypad input replaced voice lock")
	}
}

func TestDecideLocationSearchNeverGuessesLargeAmbiguity(t *testing.T) {
	matches := []locationSearchMatch{
		{Target: locationSearchTarget{Key: "1"}, Score: 0.90},
		{Target: locationSearchTarget{Key: "2"}, Score: 0.89},
		{Target: locationSearchTarget{Key: "3"}, Score: 0.88},
		{Target: locationSearchTarget{Key: "4"}, Score: 0.87},
	}
	decision := decideLocationSearch(matches, locationSearchContext{})
	if decision.Kind != locationSearchRefine {
		t.Fatalf("large collision set produced %q", decision.Kind)
	}
}

func TestLocalizedRegionHintsDistinguishCanadaAndUnitedStates(t *testing.T) {
	t.Parallel()
	if got := localizedRegionHint("en-US", "SK"); got != "A place name in Saskatchewan, Canada." {
		t.Fatalf("Canadian hint = %q", got)
	}
	if got := localizedRegionHint("en-US", "IL"); got != "A place name in Illinois, United States." {
		t.Fatalf("United States hint = %q", got)
	}
	if got := localizedConfirmPrompt("en-US", "Springfield", "IL"); !strings.Contains(got, "Springfield, Illinois") {
		t.Fatalf("confirmation did not localize the state name: %q", got)
	}
}

func TestBundledLocationCatalogProperties(t *testing.T) {
	baseDir, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "bundle"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := locationdb.Load(baseDir)
	if !ok {
		t.Fatalf("could not load bundled location database from %s", baseDir)
	}
	if len(snapshot.Places) < 15000 {
		t.Fatalf("bundled location records = %d, want at least 15000", len(snapshot.Places))
	}
	feeds, err := loadFeeds(filepath.Join(baseDir, "managed", "configs", "feeds.xml"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := loadedConfig{
		BaseDir:           baseDir,
		Feeds:             feeds,
		ForecastLocations: loadForecastLocationsForBase(baseDir),
		HelloWeather:      loadHelloWeatherForBase(baseDir),
		CLCs:              loadCLCsForBase(baseDir),
		Geocodes:          loadGeocodesForBase(baseDir),
		NWS:               loadNWSForBase(baseDir),
	}
	normalizeIVRConfig(&cfg.IVR)
	resolver := NewResolver(cfg)
	index, err := newLocationSearchIndex(snapshot, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if len(index.targets) < 5000 {
		t.Fatalf("canonical searchable targets = %d, want at least 5000", len(index.targets))
	}

	targets := make(map[string]locationSearchTarget, len(index.targets))
	nameCollisions := map[string]map[string]struct{}{}
	signatureCollisions := map[string]map[string]struct{}{}
	indexedNames := 0
	for _, target := range index.targets {
		targets[target.Key] = target
		for _, name := range target.Names {
			indexedNames++
			if name.Normalized == "" || name.T9 == "" {
				t.Fatalf("target %q contains an unsearchable name %#v", target.Key, name)
			}
			addCollisionTarget(nameCollisions, name.Normalized, target.Key)
			addCollisionTarget(signatureCollisions, name.T9, target.Key)
		}
	}
	if indexedNames < 10000 {
		t.Fatalf("indexed names = %d, want at least 10000", indexedNames)
	}
	assertCollisionSetsNeedDisambiguation(t, nameCollisions, targets)
	assertCollisionSetsNeedDisambiguation(t, signatureCollisions, targets)

	parityStep := maxInt(1, len(snapshot.Places)/512)
	for position := 0; position < len(snapshot.Places); position += parityStep {
		record, resolveErr := resolver.resolvePlaceRecord(snapshot, snapshot.Places[position])
		if resolveErr != nil {
			continue
		}
		got := resolver.attachFeed(record)
		want := legacyAttachFeedForTest(resolver, record)
		if got != want {
			t.Fatalf("bundled feed index changed legacy resolution for %#v: got %#v, want %#v", snapshot.Places[position], got, want)
		}
	}

	step := maxInt(1, len(index.targets)/256)
	for position := 0; position < len(index.targets); position += step {
		target := index.targets[position]
		matches := index.searchText(target.Names[0].Normalized, locationSearchContext{}, locationSearchVoice)
		found := false
		for _, match := range matches {
			if match.Target.Key == target.Key {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("sampled indexed name %q did not find canonical target %q", target.Names[0].Text, target.Key)
		}
	}
	t.Logf("verified %d named records into %d canonical targets and %d indexed names", len(snapshot.Places), len(index.targets), indexedNames)
}

func addCollisionTarget(groups map[string]map[string]struct{}, value string, target string) {
	if groups[value] == nil {
		groups[value] = map[string]struct{}{}
	}
	groups[value][target] = struct{}{}
}

func assertCollisionSetsNeedDisambiguation(t *testing.T, groups map[string]map[string]struct{}, targets map[string]locationSearchTarget) {
	t.Helper()
	for value, keys := range groups {
		if len(keys) < 2 {
			continue
		}
		matches := make([]locationSearchMatch, 0, len(keys))
		for key := range keys {
			matches = append(matches, locationSearchMatch{Target: targets[key], DisplayName: value, Score: 1, Exact: true})
		}
		sortLocationSearchMatches(matches)
		if shouldAutoAcceptLocation(matches, locationSearchContext{}) {
			t.Fatalf("collision %q auto-selected one of %d canonical targets", value, len(keys))
		}
	}
}
