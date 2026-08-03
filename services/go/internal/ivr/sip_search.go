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

func (c *sipCall) searchLocation(language string, region string) (ResolvedLocation, bool) {
	if c == nil || c.service == nil || c.service.searchIndex == nil || !c.service.cfg.IVR.Search.enabled() {
		c.playPrompt("location_number", "search_unavailable", map[string]string{"lang": language})
		return ResolvedLocation{}, false
	}
	contextHint := locationSearchContext{Region: searchRegionFromSelector(region), Language: language}
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
		text := localizedChoicePrompt(language, decision.Matches)
		digit, ok := c.promptTextAndWaitDigit("location_search_choices", text, 10*time.Second)
		if !ok || len(digit) != 1 || digit[0] < '1' || int(digit[0]-'1') >= len(decision.Matches) {
			return ResolvedLocation{}, false
		}
		c.service.metrics.SearchAccepted.Add(1)
		return localizedSearchLocation(decision.Matches[int(digit[0]-'1')], language), true
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
		audio, err = c.service.cache.GetPromptWithPolicy(c.ctx, menuID, lineKey, promptValues, c.service.staticPromptPolicy(), false)
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

func localizedChoicePrompt(language string, matches []locationSearchMatch) string {
	parts := make([]string, 0, len(matches)+1)
	if strings.HasPrefix(strings.ToLower(language), "fr") {
		parts = append(parts, "Plusieurs lieux correspondent.")
		for index, match := range matches {
			parts = append(parts, fmt.Sprintf("Pour %s, appuyez sur %d.", localizedSearchDisplay(match.DisplayName, match.Target.Location.Province), index+1))
		}
		return strings.Join(parts, " ")
	}
	parts = append(parts, "Several locations match.")
	for index, match := range matches {
		parts = append(parts, fmt.Sprintf("For %s, press %d.", localizedSearchDisplay(match.DisplayName, match.Target.Location.Province), index+1))
	}
	return strings.Join(parts, " ")
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
