package webgateway

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/events"
)

var broadcastOriginatorNamePattern = regexp.MustCompile(`(?i)\bA Broadcast Station or Cable System\b`)

func (s *wsSession) broadcastAlert(payload map[string]any) (map[string]any, error) {
	targets, err := alertTargetFeedIDs(s.configPath, payload)
	if err != nil {
		return nil, err
	}
	includeSame := boolPayload(payload, "include_same", true)
	allFeedLocations := boolPayload(payload, "all_feed_locations", false)
	alertID := safeID(firstNonBlank(
		stringPayload(payload, "alert_id", ""),
		fmt.Sprintf("manual-%d", time.Now().UTC().UnixNano()),
	))
	payload, err = s.prepareBroadcastAlertAudioPayload(payload, alertID)
	if err != nil {
		return nil, err
	}
	scheduleAt := parseOptionalTime(stringPayload(payload, "schedule_at", ""))
	dataByFeed, err := s.broadcastAlertDataByFeed(payload, targets, alertID, includeSame, allFeedLocations)
	if err != nil {
		return nil, err
	}
	data := dataByFeed[targets[0]]

	if !scheduleAt.IsZero() && scheduleAt.After(time.Now()) {
		delay := time.Until(scheduleAt)
		configPath := s.configPath
		go func() {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			<-timer.C
			_ = publishAlertBroadcast(configPath, targets, dataByFeed)
		}()
		return map[string]any{
			"scheduled":    true,
			"schedule_at":  scheduleAt.UTC().Format(time.RFC3339Nano),
			"alert_id":     alertID,
			"feed_ids":     targets,
			"include_same": includeSame,
			"intro":        data["same_intro"],
			"message":      "Alert scheduled",
		}, nil
	}

	if err := publishAlertBroadcast(s.configPath, targets, dataByFeed); err != nil {
		return nil, err
	}
	return map[string]any{
		"queued":       true,
		"alert_id":     alertID,
		"feed_ids":     targets,
		"include_same": includeSame,
		"intro":        data["same_intro"],
		"message":      "Alert broadcast requested",
	}, nil
}

// broadcastAlertDataByFeed creates a distinct event payload for every target feed.
// With all_feed_locations, each payload carries only that feed's configured SAME area.
func (s *wsSession) broadcastAlertDataByFeed(payload map[string]any, targets []string, alertID string, includeSame bool, allFeedLocations bool) (map[string]map[string]any, error) {
	locationsByFeed, err := alertLocationsByFeed(s.configPath, payload, targets, allFeedLocations)
	if err != nil {
		return nil, err
	}
	dataByFeed := make(map[string]map[string]any, len(targets))
	for _, feedID := range targets {
		dataByFeed[feedID] = s.broadcastAlertDataForFeed(payload, feedID, locationsByFeed[feedID], alertID, includeSame, allFeedLocations)
	}
	return dataByFeed, nil
}

func alertLocationsByFeed(configPath string, payload map[string]any, targets []string, allFeedLocations bool) (map[string][]string, error) {
	locationsByFeed := make(map[string][]string, len(targets))
	if !allFeedLocations {
		locations := expandSameLocationsForFeeds(configPath, targets, stringSlicePayload(payload, "locations"))
		for _, feedID := range targets {
			locationsByFeed[feedID] = locations
		}
		return locationsByFeed, nil
	}

	feeds, err := loadFeedSummaries(configPath)
	if err != nil {
		return nil, err
	}
	configured := make(map[string][]string, len(feeds))
	for _, feed := range feeds {
		feedID := strings.TrimSpace(fmt.Sprint(feed["id"]))
		if feedID != "" {
			configured[feedID] = normalizeAlertLocations(stringListAny(feed["same_locations"]))
		}
	}
	for _, feedID := range targets {
		locations := configured[feedID]
		if len(locations) == 0 {
			return nil, fmt.Errorf("feed %q has no configured SAME locations", feedID)
		}
		locationsByFeed[feedID] = locations
	}
	return locationsByFeed, nil
}

func normalizeAlertLocations(locations []string) []string {
	seen := map[string]struct{}{}
	for _, raw := range locations {
		code := cleanLocationCode(raw)
		if code == "000000" {
			return []string{"000000"}
		}
		if code != "" {
			seen[code] = struct{}{}
		}
	}
	return sortedKeys(seen)
}

func (s *wsSession) broadcastAlertDataForFeed(payload map[string]any, feedID string, sameLocations []string, alertID string, includeSame bool, allFeedLocations bool) map[string]any {
	introPayload := withFeedFallback(payload, feedID)
	introPayload["feed_id"] = feedID
	introPayload["feed_ids"] = []string{feedID}
	if allFeedLocations {
		delete(introPayload, "area_names")
	}
	introPayload["locations"] = sameLocations
	introRequest := alertIntroRequestFromPayload(s.configPath, introPayload)
	intro := buildSAMEToTextIntro(introRequest)
	event := strings.ToUpper(firstNonBlank(stringPayload(payload, "same_event", ""), stringPayload(payload, "event", "ADR")))
	title := strings.TrimSpace(firstNonBlank(
		stringPayload(payload, "title", ""),
		fmt.Sprintf("%s - %s", event, introRequest.EventName),
	))
	customText := strings.TrimSpace(firstNonBlank(
		stringPayload(payload, "alert_text", ""),
		stringPayload(payload, "voice_message", ""),
		stringPayload(payload, "text", ""),
	))
	description := stringPayload(payload, "description", "")
	instruction := stringPayload(payload, "instruction", "")
	originatorName := strings.TrimSpace(firstNonBlank(
		stringPayload(payload, "same_originator_name", ""),
		stringPayload(payload, "originator_name", ""),
	))
	if originatorName != "" {
		customText = replaceBroadcastOriginatorName(customText, originatorName)
		description = replaceBroadcastOriginatorName(description, originatorName)
		instruction = replaceBroadcastOriginatorName(instruction, originatorName)
	}
	prependIntro := boolPayload(payload, "prepend_same_translation", false)
	baseSpeech := manualAlertSpeechText(customText, description, instruction, title, introRequest.EventName)
	alertText := baseSpeech
	if prependIntro {
		alertText = strings.TrimSpace(strings.Join(nonEmptyManualAlertParts(intro, baseSpeech), " "))
	}
	bannerText := bannerTextFromManualAlert(intro, customText, description, instruction)
	audioMode := normalizeBroadcastAudioMode(stringPayload(payload, "audio_mode", "tts"))
	data := map[string]any{
		"feed_id":            feedID,
		"feed_ids":           []string{feedID},
		"alert_id":           alertID,
		"message_type":       "Alert",
		"title":              title,
		"event":              event,
		"alert_text":         alertText,
		"banner_text":        bannerText,
		"description":        description,
		"instruction":        instruction,
		"include_same":       includeSame,
		"same_intro":         intro,
		"same_translation":   intro,
		"same_event":         event,
		"same_originator":    strings.ToUpper(stringPayload(payload, "originator", "EAS")),
		"same_locations":     sameLocations,
		"all_feed_locations": allFeedLocations,
		"area_names":         introRequest.AreaNames,
		"same_duration":      sameDuration(payload),
		"same_tone":          strings.ToUpper(stringPayload(payload, "tone_type", "WXR")),
		"same_callsign": firstNonBlank(
			stringPayload(payload, "sender_id", ""),
			stringPayload(payload, "same_callsign", ""),
			sameCallsignFromConfig(s.configPath, feedID),
		),
		"alert_sent_at":            time.Now().UTC().Format(time.RFC3339Nano),
		"source":                   "webpanel",
		"audio_mode":               audioMode,
		"prepend_same_translation": prependIntro,
	}
	if originatorName != "" {
		data["originator_name"] = originatorName
		data["same_originator_name"] = originatorName
	}
	for _, key := range []string{"originated_by_user_id", "originated_by_username", "originated_by_session_id", "originated_from_ip"} {
		if value, ok := payload[key]; ok {
			data[key] = value
		}
	}
	if readerID := cleanReaderID(firstNonBlank(stringPayload(payload, "reader_id", ""), stringPayload(payload, "tts_reader_id", ""))); readerID != "" {
		data["reader_id"] = readerID
		data["tts_reader_id"] = readerID
	}
	if audioPath := strings.TrimSpace(stringPayload(payload, "audio_path", "")); audioPath != "" {
		data["audio_path"] = filepath.ToSlash(audioPath)
		data["audio_format"] = firstNonBlank(stringPayload(payload, "audio_format", ""), "pcm_s16le")
		data["audio_sample_rate"] = intPayload(payload, "audio_sample_rate", 48000)
		data["audio_channels"] = intPayload(payload, "audio_channels", 1)
	}
	if audioURL := strings.TrimSpace(firstNonBlank(stringPayload(payload, "audio_url", ""), stringPayload(payload, "authoritative_url", ""))); audioURL != "" {
		data["audio_url"] = audioURL
		data["authoritative_url"] = audioURL
	}
	if scheduleAt := parseOptionalTime(stringPayload(payload, "schedule_at", "")); !scheduleAt.IsZero() {
		data["scheduled_for"] = scheduleAt.UTC().Format(time.RFC3339Nano)
	}
	data["canonical_locations"] = s.canonicalSAMELocationReferences(sameLocations)
	packet, _ := alertmodel.FromMap(data)
	packet.Meta = map[string]any{
		"originated_by_user_id":    data["originated_by_user_id"],
		"originated_by_username":   data["originated_by_username"],
		"originated_by_session_id": data["originated_by_session_id"],
		"originated_from_ip":       data["originated_from_ip"],
	}
	return alertmodel.WithLegacyFields(packet, data)
}

func replaceBroadcastOriginatorName(text string, replacement string) string {
	replacement = strings.TrimSpace(replacement)
	if replacement == "" {
		return text
	}
	return broadcastOriginatorNamePattern.ReplaceAllStringFunc(text, func(string) string { return replacement })
}

func manualAlertSpeechText(customText string, description string, instruction string, fallbackValues ...string) string {
	if text := strings.TrimSpace(customText); text != "" {
		return text
	}
	parts := []string{}
	for _, value := range []string{description, instruction} {
		if clean := strings.TrimSpace(value); clean != "" {
			parts = append(parts, clean)
		}
	}
	if len(parts) > 0 {
		return strings.TrimSpace(strings.Join(parts, " "))
	}
	for _, value := range fallbackValues {
		if clean := strings.TrimSpace(value); clean != "" {
			return clean
		}
	}
	return ""
}

func nonEmptyManualAlertParts(values ...string) []string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		if clean := strings.TrimSpace(value); clean != "" {
			parts = append(parts, clean)
		}
	}
	return parts
}

func bannerTextFromManualAlert(intro string, customText string, description string, instruction string) string {
	parts := []string{}
	for _, value := range []string{intro, customText, description, instruction} {
		clean := strings.TrimSpace(value)
		if clean != "" {
			parts = append(parts, clean)
		}
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func withFeedFallback(payload map[string]any, feedID string) map[string]any {
	out := map[string]any{}
	for key, value := range payload {
		out[key] = value
	}
	if strings.TrimSpace(fmt.Sprint(out["feed_id"])) == "" {
		out["feed_id"] = feedID
	}
	return out
}

func publishAlertBroadcast(configPath string, targets []string, dataByFeed map[string]map[string]any) error {
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return fmt.Errorf("event bridge is not available")
	}
	for _, feedID := range targets {
		eventData := cloneBroadcastMap(dataByFeed[feedID])
		eventData["feed_id"] = feedID
		delete(eventData, "feed_ids")
		publisher := events.NewHostBridgePublisher(bridgeAddr)
		err := publisher.Publish(events.Event{
			Type:    "cap.alert.broadcast.requested",
			Source:  "haze-web",
			Subject: strings.TrimSpace(fmt.Sprint(eventData["alert_id"])),
			Data:    eventData,
		})
		_ = publisher.Close()
		if err != nil {
			return err
		}
	}
	_ = configPath
	return nil
}

func cloneBroadcastMap(source map[string]any) map[string]any {
	out := make(map[string]any, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}
