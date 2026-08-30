package ivr

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type sipSearchInput struct {
	digit string
	onset time.Time
	voice bool
}

type locationSearchChoiceAction uint8

const (
	locationSearchChoiceWait locationSearchChoiceAction = iota
	locationSearchChoiceBack
	locationSearchChoiceAccept
)

func (c *sipCall) searchLocation(language string, region string) (ResolvedLocation, bool) {
	if c == nil || c.service == nil || c.service.searchIndex == nil || !c.service.cfg.IVR.Search.enabled() {
		c.playPrompt("location_number", "search_unavailable", map[string]string{"lang": language})
		return ResolvedLocation{}, false
	}
	contextHint := sipLocationSearchContext(
		language,
		region,
		c.callerProvince,
		c.service.cfg.IVR.Search.CallerHintEnabled,
	)
	for attempt := 0; attempt < c.service.cfg.IVR.Search.MaxAttempts && c.ctx.Err() == nil; attempt++ {
		decision, voiceFailed := c.runSearchAttempt(language, contextHint, c.service.cfg.IVR.Search.voiceEnabled())
		if voiceFailed {
			c.playPrompt("location_search", "voice_unavailable", map[string]string{"lang": language})
			decision, _ = c.runSearchAttempt(language, contextHint, false)
		}
		if location, ok := c.confirmSearchDecision(language, decision); ok {
			return location, true
		}
		if decision.Kind == locationSearchRefine {
			c.playPrompt("location_search", "more_detail", map[string]string{"lang": language})
		} else if decision.Kind == locationSearchNoMatch {
			c.playPrompt("location_search", "no_match", map[string]string{"lang": language})
		}
	}
	c.playPrompt("location_search", "fallback_numeric", map[string]string{"lang": language})
	return ResolvedLocation{}, false
}

func (c *sipCall) runSearchAttempt(language string, contextHint locationSearchContext, allowVoice bool) (locationSearchDecision, bool) {
	session := newLocationSearchSession(c.service.searchIndex, contextHint, c.service.cfg.IVR.Search.MultitapWindow)
	attemptCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	var capture voiceCapture
	if allowVoice {
		capture = startAdaptiveVoiceCapture(attemptCtx, c.beginVoiceCapture(), c.service.cfg.IVR.Search)
		defer c.endVoiceCapture()
	}
	input := c.playSearchPrompt("location_search", "main", map[string]string{"lang": language}, capture.Onset)
	if input.voice {
		session.LockVoice(input.onset)
	} else if input.digit != "" {
		session.FeedDigit(input.digit, time.Now())
	}
	if session.Method() == "" {
		input = c.waitSearchModality(capture.Onset, 10*time.Second)
		if input.voice {
			session.LockVoice(input.onset)
		} else if input.digit != "" {
			session.FeedDigit(input.digit, time.Now())
		}
	}
	switch session.Method() {
	case locationSearchVoice:
		c.service.metrics.SearchVoice.Add(1)
		captured, ok := waitCapturedVoice(attemptCtx, capture.Done)
		if !ok {
			return locationSearchDecision{Kind: locationSearchNoMatch}, true
		}
		transcript, err := c.service.transcribeSearchAudio(attemptCtx, captured, language, contextHint.Region)
		if err != nil {
			c.service.metrics.ASRFailures.Add(1)
			return locationSearchDecision{Kind: locationSearchNoMatch}, true
		}
		return session.VoiceDecision(transcript), false
	case locationSearchT9, locationSearchMultitap:
		cancel()
		c.endVoiceCapture()
		decision := c.collectSearchKeypad(session)
		if session.Method() == locationSearchMultitap {
			c.service.metrics.SearchMultitap.Add(1)
		} else {
			c.service.metrics.SearchT9.Add(1)
		}
		return decision, false
	default:
		return locationSearchDecision{Kind: locationSearchNoMatch}, false
	}
}

func (c *sipCall) waitSearchModality(onset <-chan time.Time, timeout time.Duration) sipSearchInput {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return sipSearchInput{}
		case at := <-onset:
			if !at.IsZero() {
				return sipSearchInput{onset: at, voice: true}
			}
		case digit := <-c.digits:
			if len(digit) == 1 && digit[0] >= '2' && digit[0] <= '9' {
				return sipSearchInput{digit: digit}
			}
		case <-timer.C:
			return sipSearchInput{}
		}
	}
}

func (c *sipCall) collectSearchKeypad(session *locationSearchSession) locationSearchDecision {
	idle := time.NewTimer(c.service.cfg.IVR.Search.IdleSubmit)
	defer idle.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return locationSearchDecision{Kind: locationSearchNoMatch}
		case digit := <-c.digits:
			if digit == "#" {
				return session.KeypadDecision()
			}
			if session.FeedDigit(digit, time.Now()) {
				resetTimer(idle, c.service.cfg.IVR.Search.IdleSubmit)
			}
		case <-idle.C:
			return session.KeypadDecision()
		}
	}
}

func (c *sipCall) confirmSearchDecision(language string, decision locationSearchDecision) (ResolvedLocation, bool) {
	switch decision.Kind {
	case locationSearchAccept:
		c.service.metrics.SearchAccepted.Add(1)
		return localizedSearchLocation(decision.Matches[0], language), true
	case locationSearchConfirm:
		c.service.metrics.SearchConfirmations.Add(1)
		match := decision.Matches[0]
		text := localizedConfirmPrompt(language, match.DisplayName, match.Target.Location.Province)
		digit, ok := c.promptTextAndWaitDigit("location_search_confirm", text, 8*time.Second)
		if ok && digit == "1" {
			c.service.metrics.SearchAccepted.Add(1)
			return localizedSearchLocation(match, language), true
		}
		return ResolvedLocation{}, false
	case locationSearchChoices:
		c.service.metrics.SearchAmbiguities.Add(1)
		if len(decision.Matches) == 0 {
			return ResolvedLocation{}, false
		}
		current := 0
		for c.ctx.Err() == nil {
			text := localizedAmbiguityPrompt(language, decision.Matches[current])
			digit, ok := c.promptTextAndWaitDigit("location_search_choices", text, 10*time.Second)
			if !ok {
				return ResolvedLocation{}, false
			}
			var action locationSearchChoiceAction
			current, action = advanceLocationSearchChoice(current, len(decision.Matches), digit)
			switch action {
			case locationSearchChoiceBack:
				return ResolvedLocation{}, false
			case locationSearchChoiceAccept:
				c.service.metrics.SearchAccepted.Add(1)
				return localizedSearchLocation(decision.Matches[current], language), true
			}
		}
		return ResolvedLocation{}, false
	case locationSearchRefine:
		c.service.metrics.SearchAmbiguities.Add(1)
	default:
		c.service.metrics.SearchNoMatch.Add(1)
	}
	return ResolvedLocation{}, false
}

func (c *sipCall) playSearchPrompt(menuID string, lineKey string, values map[string]string, onset <-chan time.Time) sipSearchInput {
	promptValues := c.service.promptValues(values)
	audio, ok := c.service.staticPromptAudio(menuID, lineKey, promptValues)
	if !ok {
		var err error
		audio, err = c.service.cache.GetPromptWithPolicy(c.ctx, menuID, lineKey, promptValues, c.service.defaultPlaybackPolicy(), false)
		if err != nil {
			log.Printf("IVR SIP search prompt unavailable: %v", err)
			return sipSearchInput{}
		}
	}
	raw, err := os.ReadFile(c.cachedAudioPath(audio))
	if err != nil {
		return sipSearchInput{}
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for offset := 0; offset < len(raw) && c.ctx.Err() == nil; offset += sipPacketSamples {
		end := minInt(offset+sipPacketSamples, len(raw))
		frame := c.audioCodec.silenceFrame()
		copy(frame, raw[offset:end])
		c.sendRTP(frame)
		select {
		case <-c.ctx.Done():
			return sipSearchInput{}
		case at := <-onset:
			if !at.IsZero() {
				return sipSearchInput{voice: true, onset: at}
			}
		case digit := <-c.digits:
			if len(digit) == 1 && digit[0] >= '2' && digit[0] <= '9' {
				return sipSearchInput{digit: digit}
			}
		case <-ticker.C:
		}
	}
	return sipSearchInput{}
}

func waitCapturedVoice(ctx context.Context, done <-chan capturedVoice) (capturedVoice, bool) {
	select {
	case <-ctx.Done():
		return capturedVoice{}, false
	case captured, ok := <-done:
		return captured, ok && len(captured.Samples) > 0
	}
}

func (s *Service) transcribeSearchAudio(parent context.Context, captured capturedVoice, language string, region string) (string, error) {
	requestID, err := newSearchRequestID()
	if err != nil {
		return "", err
	}
	basename, err := writePrivateSearchWAV(s.cfg.searchSpoolDir(), requestID, captured.Samples)
	if err != nil {
		return "", err
	}
	path := filepath.Join(s.cfg.searchSpoolDir(), basename)
	defer func() { _ = os.Remove(path) }()
	ctx, cancel := context.WithTimeout(parent, s.cfg.IVR.Search.ASRTimeout)
	defer cancel()
	result, err := s.bridge.Transcribe(ctx, requestID, basename, language, localizedRegionHint(language, region))
	if err != nil {
		s.recordASRFailure(result.Code)
		return "", err
	}
	if result.LatencyMS >= 0 {
		s.metrics.ASRLatencySamples.Add(1)
		s.metrics.ASRLatencyMS.Add(uint64(result.LatencyMS))
	}
	return result.Text, nil
}

func (s *Service) recordASRFailure(code string) {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "busy":
		s.metrics.ASRBusy.Add(1)
	case "timeout":
		s.metrics.ASRTimeouts.Add(1)
	case "provider_unavailable":
		s.metrics.ASRUnavailable.Add(1)
	}
}

func localizedSearchLocation(match locationSearchMatch, language string) ResolvedLocation {
	location := match.Target.Location
	location.Name = match.DisplayName
	location.Language = fallbackText(language, location.Language)
	return location
}

func localizedRegionHint(language string, region string) string {
	regionName := searchRegionDisplayName(region)
	country := searchRegionCountry(region)
	if strings.HasPrefix(strings.ToLower(language), "fr") {
		if regionName == "" {
			return "Un nom de lieu météorologique au Canada ou aux États-Unis."
		}
		if country == "United States" {
			return fmt.Sprintf("Un nom de lieu en %s, aux États-Unis.", regionName)
		}
		return fmt.Sprintf("Un nom de lieu en %s, au Canada.", regionName)
	}
	if regionName == "" {
		return "A weather place name in Canada or the United States."
	}
	return fmt.Sprintf("A place name in %s, %s.", regionName, country)
}

func localizedConfirmPrompt(language string, name string, region string) string {
	location := localizedSearchDisplay(name, region)
	if strings.HasPrefix(strings.ToLower(language), "fr") {
		return fmt.Sprintf("Avez-vous dit %s? Appuyez sur 1 pour oui, ou 2 pour non.", location)
	}
	return fmt.Sprintf("Did you mean %s? Press 1 for yes, or 2 for no.", location)
}

func regionalLocationSearchContext(language string, selector string) locationSearchContext {
	region := searchRegionFromSelector(selector)
	return locationSearchContext{
		Region:         region,
		ExplicitRegion: region != "",
		Language:       language,
	}
}

func sipLocationSearchContext(language string, selector string, callerProvince string, callerHintEnabled bool) locationSearchContext {
	hint := regionalLocationSearchContext(language, selector)
	if !hint.ExplicitRegion && callerHintEnabled {
		hint.Region = searchRegionFromSelector(callerProvince)
	}
	return hint
}

func advanceLocationSearchChoice(current int, count int, digit string) (int, locationSearchChoiceAction) {
	if count <= 0 {
		return 0, locationSearchChoiceBack
	}
	if current < 0 || current >= count {
		current = 0
	}
	switch digit {
	case "1":
		return current, locationSearchChoiceBack
	case "2":
		return current, locationSearchChoiceAccept
	case "3":
		return (current + 1) % count, locationSearchChoiceWait
	default:
		return current, locationSearchChoiceWait
	}
}

func localizedAmbiguityPrompt(language string, match locationSearchMatch) string {
	location := localizedAmbiguityDisplay(language, match.DisplayName, match.Target.Location.Province)
	if strings.HasPrefix(strings.ToLower(language), "fr") {
		return fmt.Sprintf("%s. Appuyez sur 2 pour choisir ce lieu, sur 3 pour le lieu suivant, ou sur 1 pour revenir en arrière.", location)
	}
	return fmt.Sprintf("%s. Press 2 to choose this location, 3 for the next match, or 1 to go back.", location)
}

func localizedChoicePrompt(language string, matches []locationSearchMatch) string {
	if len(matches) == 0 {
		return ""
	}
	return localizedAmbiguityPrompt(language, matches[0])
}

func localizedAmbiguityDisplay(language string, name string, region string) string {
	name = strings.TrimSpace(name)
	region = localizedSearchRegionDisplayName(language, region)
	if region == "" {
		return name
	}
	return name + ", " + region
}

func localizedSearchRegionDisplayName(language string, region string) string {
	if !strings.HasPrefix(strings.ToLower(language), "fr") {
		return searchRegionDisplayName(region)
	}
	if name := frenchCanadianRegionDisplayNames[strings.ToUpper(strings.TrimSpace(region))]; name != "" {
		return name
	}
	return searchRegionDisplayName(region)
}

var frenchCanadianRegionDisplayNames = map[string]string{
	"AB": "Alberta",
	"BC": "Colombie-Britannique",
	"MB": "Manitoba",
	"NB": "Nouveau-Brunswick",
	"NL": "Terre-Neuve-et-Labrador",
	"NS": "Nouvelle-Écosse",
	"NT": "Territoires du Nord-Ouest",
	"NU": "Nunavut",
	"ON": "Ontario",
	"PE": "Île-du-Prince-Édouard",
	"QC": "Québec",
	"SK": "Saskatchewan",
	"YT": "Yukon",
}

func localizedSearchDisplay(name string, region string) string {
	name = strings.TrimSpace(name)
	region = searchRegionDisplayName(region)
	if region == "" {
		return name
	}
	return name + ", " + region
}

func searchRegionDisplayName(region string) string {
	region = strings.ToUpper(strings.TrimSpace(region))
	if region == "" {
		return ""
	}
	if name := provinceDisplayName(region); name != "" && !strings.EqualFold(name, region) {
		return name
	}
	best := ""
	for alias, code := range searchRegionAliases {
		if code == region && len(alias) > len(best) {
			best = alias
		}
	}
	if best == "" {
		return region
	}
	words := strings.Fields(best)
	for index, word := range words {
		if index > 0 && (word == "and" || word == "of") {
			continue
		}
		runes := []rune(word)
		runes[0] = []rune(strings.ToUpper(string(runes[0])))[0]
		words[index] = string(runes)
	}
	return strings.Join(words, " ")
}

func searchRegionCountry(region string) string {
	region = strings.ToUpper(strings.TrimSpace(region))
	for _, province := range helloWeatherProvinces() {
		if province.Code == region {
			return "Canada"
		}
	}
	return "United States"
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}
