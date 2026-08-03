package ivr

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
	"golang.org/x/text/unicode/norm"
)

type locationSearchMethod string

const (
	locationSearchVoice    locationSearchMethod = "voice"
	locationSearchT9       locationSearchMethod = "t9"
	locationSearchMultitap locationSearchMethod = "multitap"
)

type locationSearchContext struct {
	Region         string
	ExplicitRegion bool
	Language       string
}

type locationSearchName struct {
	Text       string
	Normalized string
	T9         string
	Language   string
	ExactOnly  bool
}

type locationSearchTarget struct {
	Key      string
	Location ResolvedLocation
	Names    []locationSearchName
}

type locationSearchMatch struct {
	Target      locationSearchTarget
	DisplayName string
	Method      locationSearchMethod
	Score       float64
	Exact       bool
}

type locationSearchIndex struct {
	targets []locationSearchTarget
}

func locationSearchTargetCount(index *locationSearchIndex) int {
	if index == nil {
		return 0
	}
	return len(index.targets)
}

type locationSearchDecisionKind string

const (
	locationSearchNoMatch locationSearchDecisionKind = "no_match"
	locationSearchAccept  locationSearchDecisionKind = "accept"
	locationSearchConfirm locationSearchDecisionKind = "confirm"
	locationSearchChoices locationSearchDecisionKind = "choices"
	locationSearchRefine  locationSearchDecisionKind = "refine"
)

type locationSearchDecision struct {
	Kind    locationSearchDecisionKind
	Matches []locationSearchMatch
}

// locationSearchSession owns one transport-neutral voice or keypad attempt.
// SIP and Twilio adapters feed timestamped modality events into the same rules.
type locationSearchSession struct {
	index    *locationSearchIndex
	context  locationSearchContext
	keypad   *locationSearchKeypad
	method   locationSearchMethod
	lockedAt time.Time
}

func newLocationSearchSession(index *locationSearchIndex, context locationSearchContext, multitapWindow time.Duration) *locationSearchSession {
	return &locationSearchSession{
		index:   index,
		context: context,
		keypad:  newLocationSearchKeypad(multitapWindow),
	}
}

func (session *locationSearchSession) LockVoice(onset time.Time) bool {
	if session == nil {
		return false
	}
	return session.lock(locationSearchVoice, onset)
}

func (session *locationSearchSession) FeedDigit(digit string, at time.Time) bool {
	if session == nil || len(digit) != 1 {
		return false
	}
	switch digit {
	case "#":
		return session.method == locationSearchT9 || session.method == locationSearchMultitap
	case "*":
		if session.method == locationSearchVoice {
			return false
		}
		return session.keypad.Edit()
	case "0":
		if session.method == locationSearchVoice {
			return false
		}
		return session.keypad.Feed(digit, at)
	}
	if digit[0] < '2' || digit[0] > '9' {
		return false
	}
	method := session.keypad.MethodHint()
	if !session.lock(method, at) || session.method == locationSearchVoice {
		return false
	}
	accepted := session.keypad.Feed(digit, at)
	if session.keypad.MethodHint() == locationSearchMultitap {
		session.method = locationSearchMultitap
	}
	return accepted
}

func (session *locationSearchSession) lock(method locationSearchMethod, at time.Time) bool {
	if at.IsZero() {
		at = time.Now()
	}
	if session.method == "" || at.Before(session.lockedAt) {
		session.method = method
		session.lockedAt = at
	}
	return session.method == method || method != locationSearchVoice && (session.method == locationSearchT9 || session.method == locationSearchMultitap)
}

func (session *locationSearchSession) VoiceDecision(text string) locationSearchDecision {
	if session == nil || session.index == nil || session.method != locationSearchVoice {
		return locationSearchDecision{Kind: locationSearchNoMatch}
	}
	return decideLocationSearch(session.index.SearchVoice(text, session.context), session.context)
}

func (session *locationSearchSession) KeypadDecision() locationSearchDecision {
	if session == nil || session.index == nil || session.method == "" || session.method == locationSearchVoice {
		return locationSearchDecision{Kind: locationSearchNoMatch}
	}
	return decideLocationSearch(session.index.SearchKeypad(session.keypad, session.context), session.context)
}

func (session *locationSearchSession) Method() locationSearchMethod {
	if session == nil {
		return ""
	}
	return session.method
}

func decideLocationSearch(matches []locationSearchMatch, context locationSearchContext) locationSearchDecision {
	if len(matches) == 0 {
		return locationSearchDecision{Kind: locationSearchNoMatch}
	}
	if shouldAutoAcceptLocation(matches, context) {
		return locationSearchDecision{Kind: locationSearchAccept, Matches: matches[:1]}
	}
	closeMatches := make([]locationSearchMatch, 0, len(matches))
	threshold := math.Max(0.62, matches[0].Score-0.15)
	for _, match := range matches {
		if match.Score < threshold {
			break
		}
		closeMatches = append(closeMatches, match)
	}
	if len(closeMatches) == 1 {
		return locationSearchDecision{Kind: locationSearchConfirm, Matches: closeMatches}
	}
	if len(closeMatches) <= 3 {
		return locationSearchDecision{Kind: locationSearchChoices, Matches: closeMatches}
	}
	return locationSearchDecision{Kind: locationSearchRefine, Matches: closeMatches[:minInt(4, len(closeMatches))]}
}

func loadLocationSearchIndex(cfg loadedConfig, resolver *Resolver) (*locationSearchIndex, error) {
	snapshot, ok := locationdb.Load(cfg.BaseDir)
	if !ok {
		return nil, fmt.Errorf("managed location database is unavailable")
	}
	return newLocationSearchIndex(snapshot, resolver)
}

func newLocationSearchIndex(snapshot locationdb.Snapshot, resolver *Resolver) (*locationSearchIndex, error) {
	grouped := map[string]*locationSearchTarget{}
	for _, place := range snapshot.Places {
		if strings.TrimSpace(place.Name) == "" && strings.TrimSpace(place.NameFR) == "" && len(place.Aliases()) == 0 {
			continue
		}
		location, err := resolver.ResolvePlace(snapshot, place)
		if err != nil {
			continue
		}
		key := canonicalSearchTargetKey(location)
		target := grouped[key]
		if target == nil {
			target = &locationSearchTarget{Key: key, Location: location}
			grouped[key] = target
		} else if preferSearchLocation(location, target.Location) {
			target.Location = location
		}
		addSearchName(target, place.Name, "en", place.Code)
		addSearchName(target, place.NameFR, "fr", place.Code)
		for _, alias := range place.Aliases() {
			addSearchName(target, alias, "", place.Code)
		}
	}
	for _, stationLink := range snapshot.StationLinks {
		if strings.TrimSpace(stationLink.StationName) == "" {
			continue
		}
		place, ok := snapshot.Place("station", stationLink.StationID)
		if !ok {
			place, ok = snapshot.Place(stationLink.AreaSource, stationLink.AreaCode)
		}
		if !ok {
			continue
		}
		location, err := resolver.ResolvePlace(snapshot, place)
		if err != nil {
			continue
		}
		key := canonicalSearchTargetKey(location)
		target := grouped[key]
		if target == nil {
			target = &locationSearchTarget{Key: key, Location: location}
			grouped[key] = target
		}
		addSearchName(target, stationLink.StationName, "", stationLink.StationID)
	}
	if len(grouped) == 0 {
		return nil, fmt.Errorf("managed location database contains no resolvable caller-facing names")
	}
	index := &locationSearchIndex{targets: make([]locationSearchTarget, 0, len(grouped))}
	for _, target := range grouped {
		if len(target.Names) == 0 {
			continue
		}
		index.targets = append(index.targets, *target)
	}
	sort.Slice(index.targets, func(i, j int) bool { return index.targets[i].Key < index.targets[j].Key })
	return index, nil
}

func canonicalSearchTargetKey(location ResolvedLocation) string {
	provider := "eccc"
	if strings.HasPrefix(strings.ToLower(location.Source), "nws") || strings.EqualFold(location.Source, "nws") {
		provider = "nws"
	}
	if value := strings.ToUpper(strings.TrimSpace(location.Forecast)); value != "" {
		return provider + "|forecast|" + value
	}
	if value := strings.ToUpper(strings.TrimSpace(location.StationID)); value != "" {
		return provider + "|station|" + value
	}
	return strings.ToLower(strings.TrimSpace(location.Source)) + "|code|" + strings.ToUpper(strings.TrimSpace(location.Code))
}

func preferSearchLocation(candidate ResolvedLocation, current ResolvedLocation) bool {
	if candidate.Covered != current.Covered {
		return candidate.Covered
	}
	left := searchSourcePriority(candidate.Source)
	right := searchSourcePriority(current.Source)
	if left != right {
		return left < right
	}
	return candidate.Code < current.Code
}

func searchSourcePriority(source string) int {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "hello_weather":
		return 0
	case "eccc_forecast", "nws_zone", "nws_marine_zone":
		return 1
	case "station":
		return 2
	case "nws_same", "nws_marine_same":
		return 3
	case "capcp_geocode", "clc", "nws":
		return 4
	default:
		return 5
	}
}

func addSearchName(target *locationSearchTarget, text string, language string, code string) {
	text = strings.TrimSpace(text)
	if target == nil || text == "" {
		return
	}
	normalized := normalizeLocationSearchText(text, false)
	if normalized == "" {
		return
	}
	name := locationSearchName{
		Text:       text,
		Normalized: normalized,
		T9:         t9(normalized),
		Language:   strings.ToLower(strings.TrimSpace(language)),
		ExactOnly:  opaqueLocationLabel(text, code),
	}
	for _, existing := range target.Names {
		if existing.Normalized == name.Normalized && existing.Language == name.Language {
			return
		}
	}
	target.Names = append(target.Names, name)
	stripped := stripMunicipalPrefix(normalized)
	if stripped == "" || stripped == normalized || name.ExactOnly {
		return
	}
	for _, existing := range target.Names {
		if existing.Normalized == stripped && existing.Language == name.Language {
			return
		}
	}
	target.Names = append(target.Names, locationSearchName{
		Text:       text,
		Normalized: stripped,
		T9:         t9(stripped),
		Language:   name.Language,
	})
}

func opaqueLocationLabel(text string, code string) bool {
	compact := strings.ReplaceAll(normalizeLocationSearchText(text, false), " ", "")
	code = strings.ToLower(strings.TrimSpace(code))
	if compact == "" || compact == code {
		return true
	}
	letters := 0
	digits := 0
	for _, char := range compact {
		if unicode.IsLetter(char) {
			letters++
		}
		if unicode.IsDigit(char) {
			digits++
		}
	}
	return !strings.Contains(strings.TrimSpace(text), " ") && letters > 0 && digits >= 3
}

func stripMunicipalPrefix(value string) string {
	parts := strings.Fields(value)
	prefixes := [][]string{
		{"city", "of"}, {"town", "of"}, {"village", "of"}, {"municipality", "of"},
		{"rural", "municipality", "of"}, {"ville", "de"}, {"municipalite", "de"},
	}
	for _, prefix := range prefixes {
		if len(parts) <= len(prefix) {
			continue
		}
		matched := true
		for index := range prefix {
			if parts[index] != prefix[index] {
				matched = false
				break
			}
		}
		if matched {
			return strings.Join(parts[len(prefix):], " ")
		}
	}
	return value
}

func normalizeLocationSearchText(value string, removeFiller bool) string {
	value = norm.NFD.String(strings.ToLower(strings.TrimSpace(value)))
	var builder strings.Builder
	space := true
	for _, char := range value {
		if unicode.Is(unicode.Mn, char) {
			continue
		}
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			builder.WriteRune(char)
			space = false
			continue
		}
		if !space {
			builder.WriteByte(' ')
			space = true
		}
	}
	parts := strings.Fields(builder.String())
	if removeFiller {
		return stripLocationSpeechFiller(strings.Join(parts, " "))
	}
	return strings.Join(parts, " ")
}

func stripLocationSpeechFiller(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	prefixes := []string{
		"please tell me the weather for ", "please give me the weather for ", "tell me the weather for ",
		"give me the weather for ", "show me the weather for ", "i want the weather for ",
		"weather conditions for ", "current conditions for ", "weather forecast for ",
		"weather for ", "forecast for ", "conditions for ", "weather in ", "forecast in ",
		"weather at ", "location ", "please ",
		"s il vous plait donnez moi la meteo pour ", "donnez moi la meteo pour ",
		"je veux la meteo pour ", "la meteo pour ", "meteo pour ", "previsions pour ",
		"prevision pour ", "temps pour ", "meteo a ",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) {
			value = strings.TrimSpace(strings.TrimPrefix(value, prefix))
			break
		}
	}
	for _, suffix := range []string{" weather", " forecast", " conditions", " meteo", " previsions"} {
		if strings.HasSuffix(value, suffix) {
			value = strings.TrimSpace(strings.TrimSuffix(value, suffix))
			break
		}
	}
	return value
}

func (index *locationSearchIndex) SearchVoice(text string, context locationSearchContext) []locationSearchMatch {
	query := normalizeLocationSearchText(text, true)
	query, explicitRegion := extractSearchRegion(query)
	if explicitRegion != "" {
		context.Region = explicitRegion
		context.ExplicitRegion = true
	}
	return index.searchText(query, context, locationSearchVoice)
}

func (index *locationSearchIndex) searchText(query string, context locationSearchContext, method locationSearchMethod) []locationSearchMatch {
	query = normalizeLocationSearchText(query, method == locationSearchVoice)
	if index == nil || query == "" {
		return nil
	}
	matches := make([]locationSearchMatch, 0, 16)
	for _, target := range index.targets {
		bestScore := 0.0
		exact := false
		languageMatch := false
		for _, name := range target.Names {
			score, nameExact := scoreLocationText(query, name)
			nameLanguageMatch := primaryPromptLanguage(name.Language) != "" && primaryPromptLanguage(name.Language) == primaryPromptLanguage(context.Language)
			if score > bestScore || score == bestScore && nameLanguageMatch && !languageMatch {
				bestScore = score
				exact = nameExact
				languageMatch = nameLanguageMatch
			}
		}
		if bestScore < 0.62 {
			continue
		}
		if languageMatch && bestScore < 1 {
			bestScore += 0.005
		}
		bestScore = applyLocationContext(bestScore, target.Location, context)
		matches = append(matches, locationSearchMatch{
			Target:      target,
			DisplayName: target.displayName(context.Language),
			Method:      method,
			Score:       bestScore,
			Exact:       exact,
		})
	}
	sortLocationSearchMatches(matches)
	return trimLocationSearchMatches(matches, 10)
}

func (index *locationSearchIndex) searchT9(digits string, context locationSearchContext) []locationSearchMatch {
	digits = strings.ReplaceAll(strings.TrimSpace(digits), " ", "")
	if index == nil || len(digits) < 2 {
		return nil
	}
	matches := make([]locationSearchMatch, 0, 16)
	for _, target := range index.targets {
		bestScore := 0.0
		exact := false
		for _, name := range target.Names {
			candidate := strings.ReplaceAll(name.T9, " ", "")
			if candidate == "" {
				continue
			}
			score := 0.0
			nameExact := false
			switch {
			case candidate == digits:
				score = 1
				nameExact = true
			case !name.ExactOnly && len(digits) >= 4 && strings.HasPrefix(candidate, digits):
				score = 0.86 + 0.08*float64(len(digits))/float64(len(candidate))
			}
			if score > bestScore {
				bestScore = score
				exact = nameExact
			}
		}
		if bestScore == 0 {
			continue
		}
		bestScore = applyLocationContext(bestScore, target.Location, context)
		matches = append(matches, locationSearchMatch{
			Target:      target,
			DisplayName: target.displayName(context.Language),
			Method:      locationSearchT9,
			Score:       bestScore,
			Exact:       exact,
		})
	}
	sortLocationSearchMatches(matches)
	return trimLocationSearchMatches(matches, 10)
}

func (index *locationSearchIndex) SearchKeypad(input *locationSearchKeypad, context locationSearchContext) []locationSearchMatch {
	if input == nil {
		return nil
	}
	t9Query, t9Context := extractT9SearchRegion(input.T9(), context)
	t9Matches := index.searchT9(t9Query, t9Context)
	multitapQuery, explicitRegion := extractSearchRegion(input.Multitap())
	multitapContext := context
	if explicitRegion != "" {
		multitapContext.Region = explicitRegion
		multitapContext.ExplicitRegion = true
	}
	multiMatches := index.searchText(multitapQuery, multitapContext, locationSearchMultitap)
	byTarget := map[string]locationSearchMatch{}
	for _, match := range append(t9Matches, multiMatches...) {
		current, ok := byTarget[match.Target.Key]
		if !ok || match.Score > current.Score || match.Score == current.Score && match.Method == input.MethodHint() {
			byTarget[match.Target.Key] = match
		}
	}
	out := make([]locationSearchMatch, 0, len(byTarget))
	for _, match := range byTarget {
		out = append(out, match)
	}
	sortLocationSearchMatches(out)
	return trimLocationSearchMatches(out, 10)
}

func extractT9SearchRegion(query string, context locationSearchContext) (string, locationSearchContext) {
	parts := strings.Fields(query)
	if len(parts) < 2 {
		return query, context
	}
	for start := 1; start < len(parts); start++ {
		candidate := strings.Join(parts[start:], "")
		matchedRegion := ""
		for alias, region := range searchRegionAliases {
			if candidate == t9(alias) {
				if matchedRegion != "" && matchedRegion != region {
					matchedRegion = ""
					break
				}
				matchedRegion = region
			}
		}
		if matchedRegion != "" {
			context.Region = matchedRegion
			context.ExplicitRegion = true
			return strings.Join(parts[:start], " "), context
		}
	}
	return query, context
}

func scoreLocationText(query string, name locationSearchName) (float64, bool) {
	if query == name.Normalized {
		return 1, true
	}
	if name.ExactOnly {
		return 0, false
	}
	if len([]rune(query)) >= 4 && strings.HasPrefix(name.Normalized, query) {
		return 0.86 + 0.08*float64(len([]rune(query)))/float64(len([]rune(name.Normalized))), false
	}
	tokenScore := tokenSetScore(query, name.Normalized)
	editScore := normalizedEditScore(query, name.Normalized)
	return math.Max(editScore, 0.65*tokenScore+0.35*editScore), false
}

func tokenSetScore(left string, right string) float64 {
	leftSet := map[string]struct{}{}
	rightSet := map[string]struct{}{}
	for _, token := range strings.Fields(left) {
		leftSet[token] = struct{}{}
	}
	for _, token := range strings.Fields(right) {
		rightSet[token] = struct{}{}
	}
	if len(leftSet) == 0 || len(rightSet) == 0 {
		return 0
	}
	intersection := 0
	for token := range leftSet {
		if _, ok := rightSet[token]; ok {
			intersection++
		}
	}
	union := len(leftSet) + len(rightSet) - intersection
	return float64(intersection) / float64(union)
}

func normalizedEditScore(left string, right string) float64 {
	a := []rune(left)
	b := []rune(right)
	maxLen := maxInt(len(a), len(b))
	if maxLen == 0 {
		return 1
	}
	distance := levenshteinDistance(a, b)
	return math.Max(0, 1-float64(distance)/float64(maxLen))
}

func levenshteinDistance(left []rune, right []rune) int {
	previous := make([]int, len(right)+1)
	current := make([]int, len(right)+1)
	for index := range previous {
		previous[index] = index
	}
	for i, leftRune := range left {
		current[0] = i + 1
		for j, rightRune := range right {
			cost := 1
			if leftRune == rightRune {
				cost = 0
			}
			current[j+1] = minInt(current[j]+1, minInt(previous[j+1]+1, previous[j]+cost))
		}
		previous, current = current, previous
	}
	return previous[len(right)]
}

func applyLocationContext(score float64, location ResolvedLocation, context locationSearchContext) float64 {
	region := strings.ToUpper(strings.TrimSpace(context.Region))
	if region != "" {
		if strings.EqualFold(region, location.Province) {
			if context.ExplicitRegion {
				score += 0.04
			} else {
				score += 0.02
			}
		} else if context.ExplicitRegion {
			score -= 0.25
		}
	}
	if location.Covered {
		score += 0.01
	}
	return math.Max(0, math.Min(1, score))
}

func sortLocationSearchMatches(matches []locationSearchMatch) {
	sort.SliceStable(matches, func(i, j int) bool {
		if matches[i].Score != matches[j].Score {
			return matches[i].Score > matches[j].Score
		}
		if matches[i].Exact != matches[j].Exact {
			return matches[i].Exact
		}
		if left, right := searchSourcePriority(matches[i].Target.Location.Source), searchSourcePriority(matches[j].Target.Location.Source); left != right {
			return left < right
		}
		return matches[i].Target.Key < matches[j].Target.Key
	})
}

func trimLocationSearchMatches(matches []locationSearchMatch, limit int) []locationSearchMatch {
	if len(matches) <= limit {
		return matches
	}
	return matches[:limit]
}

func (target locationSearchTarget) displayName(language string) string {
	want := strings.ToLower(strings.TrimSpace(language))
	if len(want) > 2 {
		want = want[:2]
	}
	for _, name := range target.Names {
		if name.Language == want && name.Text != "" {
			return name.Text
		}
	}
	for _, name := range target.Names {
		if name.Language == "en" && name.Text != "" {
			return name.Text
		}
	}
	if len(target.Names) > 0 {
		return target.Names[0].Text
	}
	return fallbackText(target.Location.Name, target.Location.Code)
}

func shouldAutoAcceptLocation(matches []locationSearchMatch, context locationSearchContext) bool {
	if len(matches) == 0 {
		return false
	}
	top := matches[0]
	if top.Exact {
		exactPeers := 0
		for _, match := range matches {
			if !match.Exact {
				continue
			}
			if context.Region == "" || strings.EqualFold(match.Target.Location.Province, context.Region) {
				exactPeers++
			}
		}
		if exactPeers == 1 && (context.Region == "" || strings.EqualFold(top.Target.Location.Province, context.Region)) {
			return true
		}
	}
	margin := top.Score
	if len(matches) > 1 {
		margin -= matches[1].Score
	}
	if top.Score < 0.94 || margin < 0.15 {
		return false
	}
	topName := normalizeLocationSearchText(top.DisplayName, false)
	for _, match := range matches[1:] {
		if normalizeLocationSearchText(match.DisplayName, false) == topName && match.Target.Key != top.Target.Key {
			return false
		}
	}
	return true
}

type multitapUnit struct {
	key   byte
	count int
	at    time.Time
}

type locationSearchKeypad struct {
	gap      time.Duration
	t9Digits []byte
	multitap []multitapUnit
	repeated int
}

func newLocationSearchKeypad(gap time.Duration) *locationSearchKeypad {
	if gap <= 0 {
		gap = 700 * time.Millisecond
	}
	return &locationSearchKeypad{gap: gap}
}

func (input *locationSearchKeypad) Feed(digit string, at time.Time) bool {
	if input == nil || len(digit) != 1 {
		return false
	}
	key := digit[0]
	if key == '0' {
		if len(input.t9Digits) > 0 && input.t9Digits[len(input.t9Digits)-1] != ' ' {
			input.t9Digits = append(input.t9Digits, ' ')
		}
		if len(input.multitap) > 0 && input.multitap[len(input.multitap)-1].key != '0' {
			input.multitap = append(input.multitap, multitapUnit{key: '0', count: 1, at: at})
		}
		return true
	}
	if key < '2' || key > '9' {
		return false
	}
	input.t9Digits = append(input.t9Digits, key)
	if len(input.multitap) > 0 {
		last := &input.multitap[len(input.multitap)-1]
		if last.key == key && !last.at.IsZero() && at.Sub(last.at) >= 0 && at.Sub(last.at) <= input.gap {
			last.count++
			last.at = at
			input.repeated++
			return true
		}
	}
	input.multitap = append(input.multitap, multitapUnit{key: key, count: 1, at: at})
	return true
}

func (input *locationSearchKeypad) Edit() bool {
	if input == nil || len(input.t9Digits) == 0 && len(input.multitap) == 0 {
		return false
	}
	if len(input.t9Digits) > 0 {
		input.t9Digits = input.t9Digits[:len(input.t9Digits)-1]
	}
	if len(input.multitap) > 0 {
		last := input.multitap[len(input.multitap)-1]
		if last.count > 1 {
			input.repeated -= last.count - 1
		}
		input.multitap = input.multitap[:len(input.multitap)-1]
	}
	if input.repeated < 0 {
		input.repeated = 0
	}
	return true
}

func (input *locationSearchKeypad) T9() string {
	if input == nil {
		return ""
	}
	return strings.TrimSpace(string(input.t9Digits))
}

func (input *locationSearchKeypad) Multitap() string {
	if input == nil {
		return ""
	}
	var builder strings.Builder
	for _, unit := range input.multitap {
		if unit.key == '0' {
			builder.WriteByte(' ')
			continue
		}
		letters := multitapLetters[unit.key]
		if letters == "" {
			continue
		}
		builder.WriteByte(letters[(unit.count-1)%len(letters)])
	}
	return strings.TrimSpace(builder.String())
}

func (input *locationSearchKeypad) MethodHint() locationSearchMethod {
	if input != nil && input.repeated > 0 {
		return locationSearchMultitap
	}
	return locationSearchT9
}

var multitapLetters = map[byte]string{
	'2': "abc", '3': "def", '4': "ghi", '5': "jkl",
	'6': "mno", '7': "pqrs", '8': "tuv", '9': "wxyz",
}

func searchRegionFromSelector(selector string) string {
	codes := helloWeatherProvinceCodes(selector)
	if len(codes) == 1 {
		return codes[0]
	}
	return ""
}

func extractSearchRegion(query string) (string, string) {
	query = normalizeLocationSearchText(query, false)
	if query == "" {
		return "", ""
	}
	padded := " " + query + " "
	aliases := make([]string, 0, len(searchRegionAliases))
	for alias := range searchRegionAliases {
		aliases = append(aliases, alias)
	}
	sort.Slice(aliases, func(i, j int) bool { return len(aliases[i]) > len(aliases[j]) })
	for _, alias := range aliases {
		needle := " " + alias + " "
		if !strings.Contains(padded, needle) {
			continue
		}
		remaining := strings.Join(strings.Fields(strings.Replace(padded, needle, " ", 1)), " ")
		for _, suffix := range []string{" in", " near", " at", " for", " en", " a", " pres de"} {
			remaining = strings.TrimSpace(strings.TrimSuffix(" "+remaining, suffix))
		}
		// A bare region name may also be the location itself, such as Quebec.
		// Treat it as a qualifier only when another substantive token remains.
		if remaining == "" {
			return query, ""
		}
		return remaining, searchRegionAliases[alias]
	}
	return query, ""
}

var searchRegionAliases = map[string]string{
	"alberta": "AB", "british columbia": "BC", "manitoba": "MB", "new brunswick": "NB",
	"newfoundland and labrador": "NL", "newfoundland": "NL", "nova scotia": "NS",
	"northwest territories": "NT", "nunavut": "NU", "ontario": "ON", "prince edward island": "PE",
	"quebec": "QC", "saskatchewan": "SK", "yukon": "YT",
	"alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
	"colorado": "CO", "connecticut": "CT", "delaware": "DE", "district of columbia": "DC",
	"florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID", "illinois": "IL",
	"indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
	"maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
	"mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
	"new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
	"north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
	"pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
	"tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA",
	"washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}
