// Package alertvars expands {variable} placeholders in operator-written alert
// text (automations and web panel broadcasts). Callers gather the feed facts
// from their own config; this package only formats and substitutes them.
package alertvars

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Feed holds the per-feed facts a placeholder can name. Values should already
// be in spoken form where the config has a pronunciation.
type Feed struct {
	ID              string
	EventName       string
	PrimaryCoverage string
	NetworkName     string
	OperatorName    string
	PhoneNumber     string
	// Transmitters are callsigns with the primary transmitter first.
	Transmitters []string
	// At is the time the alert is for (scheduled fire time or now), in the
	// feed's local zone.
	At time.Time
}

var pattern = regexp.MustCompile(`\{([A-Za-z][A-Za-z-]*)(?:\[(\d+)\])?\}`)

// Expand replaces known placeholders. Unknown names stay as written so typos
// are audible rather than silently dropped. An out-of-range
// {transmitterCall[n]} becomes empty.
func Expand(text string, feed Feed) string {
	if !strings.Contains(text, "{") {
		return text
	}
	hour := feed.At.Hour() % 12
	if hour == 0 {
		hour = 12
	}
	scalars := map[string]string{
		"feed-id":         feed.ID,
		"eventType":       feed.EventName,
		"primaryCoverage": feed.PrimaryCoverage,
		"networkName":     feed.NetworkName,
		"operatorName":    feed.OperatorName,
		"phoneNumber":     feed.PhoneNumber,
		"autoHour":        strconv.Itoa(hour),
		"autoMin":         fmt.Sprintf("%02d", feed.At.Minute()),
		"autoAmPm":        feed.At.Format("PM"),
		"autoDay":         feed.At.Weekday().String(),
		"timeZone":        TimeZoneName(feed.At),
	}
	return pattern.ReplaceAllStringFunc(text, func(match string) string {
		parts := pattern.FindStringSubmatch(match)
		name, indexText := parts[1], parts[2]
		if name == "transmitterCall" {
			index, _ := strconv.Atoi(indexText)
			if index < len(feed.Transmitters) {
				return feed.Transmitters[index]
			}
			return ""
		}
		if value, ok := scalars[name]; ok && indexText == "" {
			return value
		}
		return match
	})
}

var zonePrefixes = map[string]string{
	"N": "Newfoundland", "A": "Atlantic", "E": "Eastern", "C": "Central",
	"M": "Mountain", "P": "Pacific", "AK": "Alaska", "H": "Hawaii",
}

// TimeZoneName gives a spoken zone name such as "Eastern Daylight Time".
// Zones without daylight saving read as "Central Time" (Saskatchewan style).
func TimeZoneName(at time.Time) string {
	abbr := at.Format("MST")
	if len(abbr) < 3 || !strings.HasSuffix(abbr, "T") {
		return abbr
	}
	region, ok := zonePrefixes[abbr[:len(abbr)-2]]
	if !ok {
		return abbr
	}
	_, january := time.Date(at.Year(), time.January, 1, 12, 0, 0, 0, at.Location()).Zone()
	_, july := time.Date(at.Year(), time.July, 1, 12, 0, 0, 0, at.Location()).Zone()
	if january == july {
		return region + " Time"
	}
	switch abbr[len(abbr)-2] {
	case 'D':
		return region + " Daylight Time"
	case 'S':
		return region + " Standard Time"
	}
	return region + " Time"
}

// Spoken reads a config.yaml operator value (a string, or a list/map with
// text and pronunciation) and prefers its pronunciation.
func Spoken(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case []any:
		merged := map[string]any{}
		for _, item := range typed {
			if child, ok := item.(map[string]any); ok {
				for key, inner := range child {
					merged[key] = inner
				}
			}
		}
		return Spoken(merged)
	case map[string]any:
		for _, key := range []string{"pronunciation", "pronounciation", "text", "name"} {
			if text := Spoken(typed[key]); text != "" {
				return text
			}
		}
	}
	return ""
}
