package webgateway

import (
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alerttext"
	"github.com/meowraii/haze-weather-radio/services/go/internal/alertvars"
)

// alertVarsByFeed gathers each target feed's values for {variables}, so one
// broadcast can name every feed's own callsign, coverage, and time.
func (s *wsSession) alertVarsByFeed(payload map[string]any, targets []string) (map[string]alertvars.Feed, error) {
	root, err := loadYAMLMap(s.configPath)
	if err != nil {
		return nil, err
	}
	parsed, err := loadFeedsXML(s.configPath, root)
	if err != nil {
		return nil, err
	}
	feeds := make(map[string]feedXML, len(parsed.Feeds))
	for _, feed := range parsed.Feeds {
		feeds[feed.ID] = feed
	}
	at := parseOptionalTime(stringPayload(payload, "schedule_at", ""))
	if at.IsZero() {
		at = time.Now()
	}
	event := strings.ToUpper(firstNonBlank(stringPayload(payload, "same_event", ""), stringPayload(payload, "event", "ADR")))
	eventName := alerttext.EventName(s.configPath, event)
	operator := s.server.config.Operator
	out := make(map[string]alertvars.Feed, len(targets))
	for _, feedID := range targets {
		feed := feeds[feedID]
		location, err := time.LoadLocation(fallbackText(feed.Timezone, "Local"))
		if err != nil {
			location = time.Local
		}
		primary, calls := orderedTransmitterCalls(feed)
		out[feedID] = alertvars.Feed{
			ID:              feedID,
			EventName:       eventName,
			PrimaryCoverage: fallbackText(s.primaryCoverageName(feedID), primary.SiteName),
			NetworkName:     fallbackText(networkSpokenName(primary.Network), alertvars.Spoken(operator.OnAirName)),
			OperatorName:    alertvars.Spoken(operator.OperatorName),
			PhoneNumber:     alertvars.Spoken(operator.PhoneNumber),
			Transmitters:    calls,
			At:              at.In(location),
		}
	}
	return out, nil
}

// payloadWithVariables copies an operator payload with its free-text fields
// expanded for one feed. Expanding before the alert packet is built keeps the
// packet, speech, banner, and webhook text in agreement.
func payloadWithVariables(payload map[string]any, vars alertvars.Feed) map[string]any {
	out := cloneBroadcastMap(payload)
	for _, key := range []string{"title", "alert_text", "voice_message", "text", "description", "instruction"} {
		if text, ok := out[key].(string); ok {
			out[key] = alertvars.Expand(text, vars)
		}
	}
	return out
}

func (s *wsSession) primaryCoverageName(feedID string) string {
	feed, ok := s.server.feedConfig.Feed(feedID)
	if !ok {
		return ""
	}
	for _, entry := range feed.Entries {
		if name := strings.TrimSpace(entry.Name); name != "" {
			return name
		}
	}
	return ""
}

// orderedTransmitterCalls returns the primary transmitter and every callsign,
// primary first, skipping transmitters this feed replaces.
func orderedTransmitterCalls(feed feedXML) (transmitterXML, []string) {
	var primary transmitterXML
	for _, transmitter := range feed.Transmitter.Transmitters {
		if strings.EqualFold(strings.TrimSpace(transmitter.Relationship), "primary") {
			primary = transmitter
			break
		}
	}
	if strings.TrimSpace(primary.Callsign) == "" && len(feed.Transmitter.Transmitters) > 0 {
		primary = feed.Transmitter.Transmitters[0]
	}
	calls := []string{}
	if call := strings.TrimSpace(primary.Callsign); call != "" {
		calls = append(calls, call)
	}
	for _, transmitter := range feed.Transmitter.Transmitters {
		call := strings.TrimSpace(transmitter.Callsign)
		if call == "" || strings.EqualFold(strings.TrimSpace(transmitter.Relationship), "replaces") || (len(calls) > 0 && call == calls[0]) {
			continue
		}
		calls = append(calls, call)
	}
	return primary, calls
}

func networkSpokenName(network transmitterNetworkXML) string {
	return strings.TrimSpace(fallbackText(fallbackText(network.Pronunciation, network.Pronounciation), network.Name))
}
