// Package alerttext builds operator-facing and broadcast-facing alert text.
package alerttext

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

var compactSpaceRE = regexp.MustCompile(`\s+`)
var headlineTrailRE = regexp.MustCompile(`(?i)\s*-\s*(in effect|ended|updated|cancelled|canceled|statement)\s*$`)
var nwsBodyStartRE = regexp.MustCompile(`(?im)(^|[\n*]\s*)At\s+\d{3,4}\s+[AP]M\b`)
var ellipsisRE = regexp.MustCompile(`\.{3,}`)

var alertBoilerplateMarkers = []string{
	"Please continue to monitor alerts and forecasts issued by Environment Canada.",
	"To report severe weather",
	"Please report severe weather",
}

var severityPriority = []string{"Extreme", "Severe", "Moderate", "Minor", "Unknown"}

var severityHexDefault = map[string]string{
	"Extreme":  "#B91C1C",
	"Severe":   "#B91C1C",
	"Moderate": "#B45309",
	"Minor":    "#0B3810",
	"Unknown":  "#0B3810",
}

var easCrawlColors = map[string][]string{
	"warning":  {"#931102", "#370c16"},
	"watch":    {"#929301", "#37380b"},
	"advisory": {"#019310", "#0b3810"},
}

var endecModeAliases = map[string]string{
	"":             "NONE",
	"NONE":         "NONE",
	"TFT":          "TFT",
	"SAGE":         "SAGE EAS",
	"SAGE EAS":     "SAGE EAS",
	"SAGE DIGITAL": "SAGE DIGITAL",
	"DIGITAL":      "SAGE DIGITAL",
	"TRILITHIC":    "TRILITHIC",
	"VIAVI":        "TRILITHIC",
	"EASY":         "TRILITHIC",
	"BURK":         "BURK",
	"DAS":          "DASDEC",
	"DASDEC":       "DASDEC",
	"MONROE":       "DASDEC",
}

var endecOriginators = map[string]map[string]string{
	"NONE": {
		"EAS": "An EAS Participant",
		"CIV": "A Civil Authority",
		"PEP": "A Primary Entry Point System",
	},
	"TFT": {
		"EAS": "An EAS Participant",
		"CIV": "A Civil Authority",
		"PEP": "A Primary Entry Point System",
	},
	"SAGE EAS": {
		"EAS": "A Broadcast station or cable system",
		"CIV": "The Civil Authorities",
		"PEP": "A Primary Entry Point System",
	},
	"SAGE DIGITAL": {
		"EAS": "An EAS Participant",
		"CIV": "The Civil Authorities",
		"PEP": "A Primary Entry Point System",
	},
	"TRILITHIC": {
		"EAS": "An EAS Participant",
		"CIV": "Civil Authorities",
		"PEP": "A Primary Entry Point System",
	},
	"BURK": {
		"EAS": "A Broadcast station or cable system",
		"CIV": "Civil Authorities",
		"PEP": "A Primary Entry Point System",
	},
	"DASDEC": {
		"EAS": "An EAS Participant",
		"CIV": "A Civil Authority",
		"PEP": "A Primary Entry Point System",
	},
}

var originatorLabels = map[string]string{
	"EAS": "An EAS Participant",
	"CIV": "A Civil Authority",
	"PEP": "A Primary Entry Point System",
}

var sameCategoryTerminal = map[string]string{
	"warning":   "warning",
	"watch":     "watch",
	"advisory":  "advisory",
	"statement": "advisory",
	"outlook":   "advisory",
}

var weatherServicesByRegion = map[string]string{
	"AU": "The Australian Bureau of Meteorology",
	"BR": "The National Institute of Meteorology of Brazil",
	"CA": "Environment Canada",
	"DE": "Deutscher Wetterdienst",
	"ES": "Agencia Estatal de Meteorologia",
	"FR": "Meteo-France",
	"GB": "The Met Office",
	"HK": "The Hong Kong Observatory",
	"IE": "Met Eireann",
	"IN": "The India Meteorological Department",
	"IT": "The Italian Meteorological Service",
	"JP": "The Japan Meteorological Agency",
	"KR": "The Korea Meteorological Administration",
	"MX": "Servicio Meteorologico Nacional",
	"NZ": "MetService New Zealand",
	"PH": "PAGASA",
	"SG": "The Meteorological Service Singapore",
	"US": "The National Weather Service",
	"ZA": "The South African Weather Service",
}

// SAMERequest describes the SAME fields needed to build a spoken translation.
type SAMERequest struct {
	Originator     string
	OriginatorName string
	Event          string
	EventName      string
	Locations      []string
	AreaNames      []string
	Callsign       string
	WeatherService string
	SentAt         time.Time
	ExpiresAt      time.Time
	BeginsAt       time.Time
	MimicENDEC     string
	Description    string
	Instruction    string
}

// CAPMessageRequest describes a CAP alert speech request.
type CAPMessageRequest struct {
	Alert          capmodel.Alert
	Info           capmodel.AlertInfo
	AreaText       string
	Sender         string
	EventName      string
	Timezone       string
	Now            time.Time
	UpdatedAt      time.Time
	WeatherService string
}

// SerializedAlert is a banner/archive friendly alert payload.
type SerializedAlert struct {
	Identifier         string                         `json:"identifier"`
	FeedID             string                         `json:"feed_id,omitempty"`
	DisplayID          string                         `json:"display_id,omitempty"`
	Headline           string                         `json:"headline"`
	Issuer             string                         `json:"issuer"`
	Event              string                         `json:"event"`
	Severity           string                         `json:"severity"`
	Urgency            string                         `json:"urgency,omitempty"`
	Certainty          string                         `json:"certainty,omitempty"`
	Areas              []string                       `json:"areas"`
	AreaText           string                         `json:"area_text"`
	CanonicalLocations []alertmodel.LocationReference `json:"canonical_locations,omitempty"`
	ReceivedAt         string                         `json:"received_at,omitempty"`
	EffectiveAt        string                         `json:"effective_at,omitempty"`
	OnsetAt            string                         `json:"onset_at,omitempty"`
	ExpiresAt          string                         `json:"expires_at,omitempty"`
	OnsetDisplay       string                         `json:"onset_display,omitempty"`
	ExpiresDisplay     string                         `json:"expires_display,omitempty"`
	Description        string                         `json:"description,omitempty"`
	Instruction        string                         `json:"instruction,omitempty"`
	Message            string                         `json:"message"`
	SourceKind         string                         `json:"source_kind,omitempty"`
	BackgroundColor    string                         `json:"background_color"`
	BackgroundGradient []string                       `json:"background_gradient"`
}

// CleanFragment compacts whitespace and trims a value for spoken text.
func CleanFragment(value any) string {
	return compactSpaceRE.ReplaceAllString(strings.TrimSpace(fmt.Sprint(value)), " ")
}

// ResolveENDECMode normalizes a configured ENDEC mimic value.
func ResolveENDECMode(raw string) string {
	normalized := strings.ToUpper(strings.TrimSpace(strings.NewReplacer("_", " ", "-", " ").Replace(raw)))
	normalized = compactSpaceRE.ReplaceAllString(normalized, " ")
	if mode, ok := endecModeAliases[normalized]; ok {
		return mode
	}
	return "NONE"
}

// BuildSAMETranslation builds the old banner-style SAME-to-text lead.
func BuildSAMETranslation(request SAMERequest) string {
	mode := ResolveENDECMode(request.MimicENDEC)
	if mode == "NONE" && strings.TrimSpace(request.MimicENDEC) == "" {
		mode = "SAGE EAS"
	}
	request.MimicENDEC = mode
	subject := sameSubject(request)
	eventName := request.EventName
	if eventName == "" {
		eventName = strings.ToUpper(strings.TrimSpace(request.Event))
	}
	eventPhrase := sameEventPhrase(eventName, mode)
	areas := formatENDECAreas(request.AreaNames, mode)
	sourceLabel := CleanFragment(request.Callsign)
	begins := FormatENDECTime(request.BeginsAt, mode, "start", request.ExpiresAt)
	expires := FormatENDECTime(request.ExpiresAt, mode, "end", request.BeginsAt)

	switch mode {
	case "SAGE EAS", "SAGE DIGITAL":
		beginsAt := request.BeginsAt
		if beginsAt.IsZero() {
			beginsAt = request.SentAt
		}
		leadAreas := formatSAMELeadAreas(request.AreaNames)
		if leadAreas == "" {
			leadAreas = areas
		}
		lead := fmt.Sprintf("%s %s issued %s", subject, sameVerb(request, mode), eventPhrase)
		if leadAreas != "" {
			lead += " for the following areas: " + leadAreas
		}
		parts := []string{CleanSentence(lead)}
		if timing := sameLeadTimingSentence(beginsAt, request.ExpiresAt); timing != "" {
			parts = append(parts, timing)
		}
		if sourceLabel != "" {
			parts = append(parts, fmt.Sprintf("(%s).", sourceLabel))
		}
		return CleanFragment(strings.Join(parts, " "))
	case "TRILITHIC":
		areaClause := "for"
		if areas != "" {
			areaClause = "for the following counties: " + areas
		}
		lead := strings.TrimSpace(fmt.Sprintf("%s %s issued %s %s", subject, sameVerb(request, mode), eventPhrase, areaClause))
		if expires != "" {
			lead += ". Effective Until " + expires
		}
		if sourceLabel != "" {
			lead += fmt.Sprintf(". (%s)", sourceLabel)
		}
		return CleanSentence(lead)
	case "BURK":
		lead := fmt.Sprintf("%s has issued %s", subject, eventPhrase)
		if areas != "" {
			lead += " for the following counties/areas: " + areas
		}
		if begins != "" && expires != "" {
			lead += fmt.Sprintf(" on %s effective until %s", begins, expires)
		} else if expires != "" {
			lead += " effective until " + expires
		}
		return CleanSentence(lead)
	case "DASDEC":
		lead := fmt.Sprintf("%s HAS ISSUED %s", strings.ToUpper(subject), eventPhrase)
		if areas != "" {
			lead += " FOR THE FOLLOWING COUNTIES/AREAS: " + areas
		}
		if begins != "" && expires != "" {
			lead += fmt.Sprintf(" AT %s EFFECTIVE UNTIL %s", begins, expires)
		} else if expires != "" {
			lead += " EFFECTIVE UNTIL " + expires
		}
		if sourceLabel != "" {
			lead += ". MESSAGE FROM " + strings.ToUpper(sourceLabel)
		}
		return CleanSentence(lead)
	case "TFT":
		eventCode := strings.ToUpper(strings.TrimSpace(request.Event))
		lead := ""
		if sameOriginatorCode(request) == "EAS" || eventCode == "NPT" || eventCode == "EAN" {
			lead = eventPhrase + " has been issued"
		} else {
			lead = fmt.Sprintf("%s has issued %s", subject, eventPhrase)
		}
		if areas != "" {
			lead += " for the following counties/areas: " + areas
		}
		if begins != "" && expires != "" {
			lead += fmt.Sprintf(" at %s effective until %s", begins, expires)
		} else if expires != "" {
			lead += " effective until " + expires
		}
		if sourceLabel != "" {
			lead += ". message from " + sourceLabel
		}
		return strings.ToUpper(CleanSentence(lead))
	default:
		lead := fmt.Sprintf("%s has issued %s", subject, eventPhrase)
		if areas != "" {
			lead += " for " + areas
		}
		if begins != "" && expires != "" {
			lead += fmt.Sprintf(" beginning at %s and ending at %s", begins, expires)
		} else if expires != "" {
			lead += " effective until " + expires
		}
		if sourceLabel != "" {
			lead += ". Message from " + sourceLabel
		}
		return CleanSentence(lead)
	}
}

// BuildAlertMessage appends description and instruction to the SAME translation.
func BuildAlertMessage(request SAMERequest) string {
	parts := []string{BuildSAMETranslation(request)}
	if description := CleanFragment(request.Description); description != "" {
		parts = append(parts, description)
	}
	if instruction := CleanFragment(request.Instruction); instruction != "" {
		parts = append(parts, instruction)
	}
	return strings.Join(parts, " ")
}

// BuildCAPAlertText builds the spoken CAP alert text used by playout and archive rebroadcasts.
func BuildCAPAlertText(request CAPMessageRequest) string {
	alert := request.Alert
	info := request.Info
	now := request.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	sender := fallbackText(request.Sender, info.SenderName, CAPSenderName(alert, request.WeatherService))
	subject := fallbackText(request.EventName, AlertSubject(info))
	areas := fallbackText(request.AreaText, CAPParam(info, "layer:EC-MSC-SMC:1.0:Alert_Coverage"), "the listening area")
	if areas == "the listening area" && DetectCAPSource(alert) == "nws" {
		for _, area := range info.Areas {
			if text := CleanFragment(area.Description); text != "" {
				areas = text
				break
			}
		}
	}
	issuedAt := ReportTime(fallbackText(alert.Sent, info.Effective), request.Timezone)
	ended := IsCAPEnded(alert, now)
	locationStatus := strings.ToLower(fallbackText(
		CAPParam(info, "layer:EC-MSC-SMC:1.1:Alert_Location_Status"),
		CAPParam(info, "layer:EC-MSC-SMC:1.0:Alert_Location_Status"),
	))

	parts := []string{}
	switch {
	case locationStatus == "ended" || ended:
		parts = append(parts, fmt.Sprintf("%s has ended a %s for %s.", sender, subject, areas))
	case strings.EqualFold(alert.MessageType, "Cancel"):
		parts = append(parts, fmt.Sprintf("%s has cancelled the %s for %s.", sender, subject, areas))
	case strings.EqualFold(alert.MessageType, "Alert"):
		if issuedAt != "" {
			parts = append(parts, fmt.Sprintf("%s has issued a %s at %s for %s.", sender, subject, issuedAt, areas))
		} else {
			parts = append(parts, fmt.Sprintf("%s has issued a %s for %s.", sender, subject, areas))
		}
	case strings.EqualFold(alert.MessageType, "Update") && !request.UpdatedAt.IsZero() && now.After(request.UpdatedAt.Add(10*time.Minute)):
		parts = append(parts, fmt.Sprintf("%s continues a %s for %s.", sender, subject, areas))
	case strings.EqualFold(alert.MessageType, "Update"):
		if issuedAt != "" {
			parts = append(parts, fmt.Sprintf("%s has updated a %s at %s for %s.", sender, subject, issuedAt, areas))
		} else {
			parts = append(parts, fmt.Sprintf("%s has updated a %s for %s.", sender, subject, areas))
		}
	default:
		parts = append(parts, fmt.Sprintf("%s has issued a %s for %s.", sender, subject, areas))
	}

	if !ended {
		onset := ParseCAPTime(fallbackText(info.Onset, info.Effective))
		expires := ParseCAPTime(info.Expires)
		if !onset.IsZero() && now.Before(onset) && !expires.IsZero() {
			parts = append(parts, fmt.Sprintf("In effect from %s through %s.", ReportTime(onset.Format(time.RFC3339), request.Timezone), ReportTime(expires.Format(time.RFC3339), request.Timezone)))
		} else if !expires.IsZero() {
			parts = append(parts, fmt.Sprintf("In effect until %s.", ReportTime(expires.Format(time.RFC3339), request.Timezone)))
		} else if !onset.IsZero() && now.Before(onset) {
			parts = append(parts, fmt.Sprintf("Beginning %s.", ReportTime(onset.Format(time.RFC3339), request.Timezone)))
		}
		confidence := strings.ToLower(CAPParam(info, "layer:EC-MSC-SMC:1.1:MSC_Confidence"))
		impact := strings.ToLower(CAPParam(info, "layer:EC-MSC-SMC:1.1:MSC_Impact"))
		if confidence != "" && impact != "" {
			parts = append(parts, fmt.Sprintf("Forecast confidence is %s with %s impact expected.", confidence, impact))
		}
	}
	if description := CleanAlertText(info.Description); description != "" {
		parts = append(parts, description)
	}
	if instruction := CleanAlertText(info.Instruction); instruction != "" {
		parts = append(parts, instruction)
	}
	return strings.Join(parts, " ")
}

// SpeechFromData returns alert speech from an event data map.
func SpeechFromData(data map[string]any) string {
	prependSame, prependSameSet := mapBoolValue(data, "prepend_same_translation")
	if text := firstMapText(data, "alert_text", "tts_text", "text", "message"); text != "" {
		if prependSameSet && !prependSame {
			if stripped := stripLeadingSameTranslation(data, text); stripped != "" {
				return stripped
			}
			if sameTranslationOnly(data, text) {
				return fallbackSpeechFromData(data, false)
			}
		}
		return text
	}
	return fallbackSpeechFromData(data, !prependSameSet || prependSame)
}

func fallbackSpeechFromData(data map[string]any, includeSameTranslation bool) string {
	parts := []string{}
	keys := []string{"title", "header", "description", "instruction"}
	if includeSameTranslation {
		keys = append([]string{"same_translation", "same_intro"}, keys...)
	}
	for _, key := range keys {
		if value := firstMapText(data, key); value != "" {
			parts = append(parts, value)
		}
	}
	return strings.Join(parts, " ")
}

func stripLeadingSameTranslation(data map[string]any, text string) string {
	for _, key := range []string{"same_translation", "same_intro"} {
		intro := firstMapText(data, key)
		if intro == "" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(text), strings.ToLower(intro)) {
			return CleanFragment(strings.TrimLeft(strings.TrimSpace(text[len(intro):]), ".:-; "))
		}
	}
	return text
}

func sameTranslationOnly(data map[string]any, text string) bool {
	cleanText := strings.ToLower(CleanFragment(text))
	for _, key := range []string{"same_translation", "same_intro"} {
		if intro := firstMapText(data, key); intro != "" && cleanText == strings.ToLower(CleanFragment(intro)) {
			return true
		}
	}
	return false
}

// EventName resolves a SAME/EAS event code into a spoken event label.
func EventName(configPath string, event string) string {
	labels := LoadEventLabels(configPath)
	if label := labels[strings.ToUpper(strings.TrimSpace(event))]; label != "" {
		return NormalizeHeadline(label)
	}
	return strings.ToUpper(strings.TrimSpace(event))
}

// ResolveAreaNames resolves provided names or SAME location codes into area names.
func ResolveAreaNames(configPath string, provided []string, locations []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	add := func(value string) {
		clean := CleanFragment(value)
		if clean == "" {
			return
		}
		key := strings.ToLower(clean)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, clean)
	}
	for _, name := range provided {
		if len(locations) > 0 && isUnknownLocationName(name) {
			continue
		}
		add(name)
	}
	if len(out) > 0 {
		return out
	}
	locationNames := LoadLocationLabels(configPath)
	for _, code := range locations {
		cleanCode := CleanLocationCode(code)
		if cleanCode == "" {
			continue
		}
		name := sameLocationLabel(locationNames, cleanCode)
		if name == "" {
			name = "Unknown Location (" + cleanCode + ")"
		}
		add(name)
	}
	return out
}

// sameLocationLabel returns a configured label for a whole or NWS-style
// partial SAME location code. A nonzero first digit denotes one of nine
// parent-area partitions, so the parent label remains useful for speech.
func sameLocationLabel(labels map[string]string, code string) string {
	if name := strings.TrimSpace(labels[code]); name != "" {
		return name
	}
	if len(code) != 6 || code[0] < '1' || code[0] > '9' {
		return ""
	}
	parent := "0" + code[1:]
	name := strings.TrimSpace(labels[parent])
	if name == "" {
		return ""
	}
	direction := map[byte]string{
		'1': "northwest",
		'2': "north",
		'3': "northeast",
		'4': "west",
		'5': "central",
		'6': "east",
		'7': "southwest",
		'8': "south",
		'9': "southeast",
	}[code[0]]
	if direction == "" {
		return ""
	}
	return direction + " portion of " + name
}

func isUnknownLocationName(value string) bool {
	return strings.Contains(strings.ToLower(CleanFragment(value)), "unknown location (")
}

// LoadEventLabels loads SAME event labels from managed/sameMapping.json.
func LoadEventLabels(configPath string) map[string]string {
	out := map[string]string{}
	for _, rel := range []string{"managed/sameMapping.json", "managed/same_mapping.json"} {
		raw, err := os.ReadFile(resolveConfigPath(configPath, rel))
		if err != nil {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			continue
		}
		eas, _ := payload["eas"].(map[string]any)
		for code, label := range eas {
			if text := CleanFragment(label); text != "" {
				out[strings.ToUpper(strings.TrimSpace(code))] = text
			}
		}
		return out
	}
	return out
}

// LoadLocationLabels loads forecast, CLC, CAP-CP, and NWS location labels when available.
func LoadLocationLabels(configPath string) map[string]string {
	labels := map[string]string{}
	if snap, ok := locationdb.Load(filepath.Dir(filepath.Clean(configPath))); ok {
		for code, name := range snap.Labels() {
			if strings.TrimSpace(code) != "" && strings.TrimSpace(name) != "" {
				labels[code] = name
			}
		}
		return labels
	}
	loadCSVNames(resolveConfigPath(configPath, "managed/csv/FORECAST_LOCATIONS.csv"), labels, []string{"CODE"}, []string{"NAME", "NOM"})
	loadCSVNames(resolveConfigPath(configPath, "managed/csv/CLC_Base_Zone.csv"), labels, []string{"CLC", "CODE", "Geocode", "geocode"}, []string{"NAME", "Name", "English", "EN", "name_en"})
	loadCSVNames(resolveConfigPath(configPath, "managed/csv/CAP-CP_Geocodes.csv"), labels, []string{"CODE", "Geocode", "geocode", "value"}, []string{"NAME", "Name", "English", "EN", "name_en"})
	loadNWSLocationLabels(resolveConfigPath(configPath, "managed/csv/NWS_ZONE_COUNTY_CORRELATION.csv"), labels)
	loadNWSMarineLocationLabels(resolveConfigPath(configPath, "managed/csv/NWS_MARINE_ZONES.csv"), labels)
	return labels
}

// SerializeCAPAlert builds a Python banner-style public alert payload.
func SerializeCAPAlert(alert capmodel.Alert, info capmodel.AlertInfo, feedID string, areaNames []string, timezone string, sourceKind string, now time.Time) SerializedAlert {
	headline := NormalizeHeadline(fallbackText(info.Headline, info.Event, "Alert"))
	issuer := fallbackText(info.SenderName, CAPSenderName(alert, ""))
	message := BuildCAPAlertText(CAPMessageRequest{
		Alert:     alert,
		Info:      info,
		AreaText:  JoinParts(areaNames),
		Timezone:  timezone,
		Now:       now,
		EventName: AlertSubject(info),
	})
	onset := fallbackText(info.Onset, info.Effective)
	visualEvent := strings.Join(nonEmpty([]string{info.Event, info.Headline, AlertSubject(info), message}), " ")
	gradient := PickBannerGradient([]AlertVisualInput{{
		Severity:           info.Severity,
		Event:              visualEvent,
		BroadcastImmediate: IsBroadcastImmediateInfo(info),
	}})
	return SerializedAlert{
		Identifier:         CleanFragment(alert.Identifier),
		FeedID:             CleanFragment(feedID),
		DisplayID:          MessageID(alert),
		Headline:           headline,
		Issuer:             issuer,
		Event:              fallbackText(info.Event, headline),
		Severity:           fallbackText(strings.Title(strings.ToLower(CleanFragment(info.Severity))), "Unknown"),
		Urgency:            strings.Title(strings.ToLower(CleanFragment(info.Urgency))),
		Certainty:          strings.Title(strings.ToLower(CleanFragment(info.Certainty))),
		Areas:              unique(areaNames),
		AreaText:           JoinParts(areaNames),
		ReceivedAt:         CleanFragment(alert.Sent),
		EffectiveAt:        CleanFragment(info.Effective),
		OnsetAt:            CleanFragment(onset),
		ExpiresAt:          CleanFragment(info.Expires),
		OnsetDisplay:       FormatDisplayTime(onset, timezone),
		ExpiresDisplay:     FormatDisplayTime(info.Expires, timezone),
		Description:        CleanFragment(info.Description),
		Instruction:        CleanFragment(info.Instruction),
		Message:            message,
		SourceKind:         CleanFragment(sourceKind),
		BackgroundColor:    gradient[0],
		BackgroundGradient: gradient,
	}
}

// AlertVisualInput is the minimal shape needed to choose banner colors.
type AlertVisualInput struct {
	Severity           string
	Event              string
	BroadcastImmediate bool
}

// PickBannerGradient returns the EAS crawl gradient for a group of alerts.
func PickBannerGradient(alerts []AlertVisualInput) []string {
	for _, entry := range alerts {
		if entry.BroadcastImmediate {
			return append([]string{}, easCrawlColors["warning"]...)
		}
	}
	for _, severity := range severityPriority {
		for _, entry := range alerts {
			entrySeverity := strings.Title(strings.ToLower(CleanFragment(entry.Severity)))
			if entrySeverity == "" {
				entrySeverity = "Unknown"
			}
			if entrySeverity != severity {
				continue
			}
			if gradient := easCrawlColors[deriveSameCategory(entry.Event)]; len(gradient) > 0 {
				return append([]string{}, gradient...)
			}
			fallback := severityHexDefault[severity]
			return []string{fallback, fallback}
		}
	}
	for _, entry := range alerts {
		if gradient := easCrawlColors[deriveSameCategory(entry.Event)]; len(gradient) > 0 {
			return append([]string{}, gradient...)
		}
	}
	return append([]string{}, easCrawlColors["advisory"]...)
}

// PickBannerColor returns the first banner color for a group of alerts.
func PickBannerColor(alerts []AlertVisualInput) string {
	return PickBannerGradient(alerts)[0]
}

// IsBroadcastImmediateInfo reports whether a CAP info block requests immediate broadcast handling.
func IsBroadcastImmediateInfo(info capmodel.AlertInfo) bool {
	for _, param := range info.Parameters {
		name := strings.ToLower(strings.TrimSpace(param.Name))
		if !strings.Contains(name, "broadcast_immediately") && !strings.Contains(name, "wirelessimmediate") {
			continue
		}
		value := strings.ToLower(strings.TrimSpace(param.Value))
		return value == "yes" || value == "true" || value == "1" || value == "broadcast immediate"
	}
	return false
}

// NormalizeHeadline mirrors the Python banner headline normalizer.
func NormalizeHeadline(value string) string {
	headline := strings.Trim(headlineTrailRE.ReplaceAllString(CleanFragment(value), ""), " -")
	if headline == "" {
		return "Alert"
	}
	parts := strings.Split(headline, " - ")
	for i, part := range parts {
		clean := CleanFragment(part)
		parts[i] = TitleWords(clean)
	}
	return strings.Join(parts, " - ")
}

// AlertSubject resolves the public alert subject from CAP fields.
func AlertSubject(info capmodel.AlertInfo) string {
	name := CAPParam(info, "layer:EC-MSC-SMC:1.0:Alert_Name")
	if name == "" {
		name = stripAlertHeadlineState(info.Headline)
	}
	if name == "" {
		name = info.Event
	}
	name = strings.ReplaceAll(name, "_", " ")
	parts := strings.Split(name, " - ")
	for i := range parts {
		parts[i] = TitleWords(parts[i])
	}
	return strings.Join(parts, " - ")
}

// CleanAlertText trims CAP description/instruction boilerplate for speech.
func CleanAlertText(raw string) string {
	return cleanAlertSpeech(raw)
}

// CleanNWSAlertText trims leaked NWS product headers while preserving CAP description/instruction body text.
func CleanNWSAlertText(raw string) string {
	return cleanAlertSpeech(stripNWSProductPreamble(raw))
}

func cleanAlertSpeech(raw string) string {
	value := normalizeAlertSpeech(raw)
	if value == "" {
		return ""
	}
	for _, marker := range alertBoilerplateMarkers {
		if idx := strings.Index(value, marker); idx >= 0 {
			value = strings.TrimSpace(value[:idx])
		}
	}
	fields := strings.Fields(value)
	if len(fields) > 120 {
		fields = fields[:120]
	}
	return CleanSentence(strings.Join(fields, " "))
}

func normalizeAlertSpeech(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	value = strings.ReplaceAll(value, "###", " ")
	value = strings.ReplaceAll(value, "*", " ")
	value = ellipsisRE.ReplaceAllString(value, ". ")
	return strings.TrimSpace(value)
}

func stripNWSProductPreamble(raw string) string {
	value := strings.TrimSpace(raw)
	if !startsWithAWIPSID(value) || !strings.Contains(strings.ToLower(value), "the national weather service") {
		return value
	}
	loc := nwsBodyStartRE.FindStringIndex(value)
	if loc == nil {
		return value
	}
	return strings.TrimLeft(value[loc[0]:], " \t\r\n*")
}

func startsWithAWIPSID(value string) bool {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return false
	}
	token := strings.Trim(fields[0], ".,:;")
	if len(token) < 6 || len(token) > 7 {
		return false
	}
	for i, r := range token {
		if i < 3 && (r < 'A' || r > 'Z') {
			return false
		}
		if !((r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

// CAPParam returns a CAP parameter by case-insensitive name.
func CAPParam(info capmodel.AlertInfo, name string) string {
	for _, param := range info.Parameters {
		if strings.EqualFold(strings.TrimSpace(param.Name), name) {
			return strings.TrimSpace(param.Value)
		}
	}
	return ""
}

// IsCAPEnded returns true for explicit or time-expired CAP end states.
func IsCAPEnded(alert capmodel.Alert, now time.Time) bool {
	if strings.EqualFold(alert.MessageType, "Cancel") {
		return true
	}
	if len(alert.Infos) == 0 {
		return false
	}
	allExplicitlyEnded := true
	allExpired := !now.IsZero()
	for _, info := range alert.Infos {
		explicitlyEnded := capInfoExplicitlyEnded(info)
		if !explicitlyEnded {
			allExplicitlyEnded = false
		}
		expired := false
		if expires := ParseCAPTime(info.Expires); !expires.IsZero() && !now.IsZero() {
			expired = now.After(expires)
		}
		if !expired {
			allExpired = false
		}
		if !explicitlyEnded && !expired {
			return false
		}
	}
	if allExplicitlyEnded || allExpired {
		return true
	}
	return false
}

func capInfoExplicitlyEnded(info capmodel.AlertInfo) bool {
	for _, response := range info.Response {
		if strings.EqualFold(response, "AllClear") {
			return true
		}
	}
	status := strings.ToLower(fallbackText(
		CAPParam(info, "layer:EC-MSC-SMC:1.0:Alert_Location_Status"),
		CAPParam(info, "layer:EC-MSC-SMC:1.1:Alert_Location_Status"),
	))
	return status == "ended" || strings.Contains(strings.ToLower(info.Headline), "ended")
}

// AlertExpiresAt returns the first CAP info expiry timestamp.
func AlertExpiresAt(alert capmodel.Alert) time.Time {
	for _, info := range alert.Infos {
		if expires := ParseCAPTime(info.Expires); !expires.IsZero() {
			return expires
		}
	}
	return time.Time{}
}

// FirstCAPTime returns the best event anchor time.
func FirstCAPTime(alert capmodel.Alert, info capmodel.AlertInfo) time.Time {
	for _, raw := range []string{alert.Sent, info.Effective, info.Onset, info.Expires} {
		if parsed := ParseCAPTime(raw); !parsed.IsZero() {
			return parsed
		}
	}
	return time.Time{}
}

// ParseCAPTime parses common CAP timestamps.
func ParseCAPTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z", "2006-01-02T15:04:05Z07:00"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

// DetectCAPSource identifies the likely CAP source family.
func DetectCAPSource(alert capmodel.Alert) string {
	for _, info := range alert.Infos {
		for _, param := range info.Parameters {
			if strings.HasPrefix(param.Name, "layer:EC-MSC-SMC") || strings.Contains(strings.ToLower(param.Name), "cap-cp") {
				return "eccc"
			}
		}
	}
	sender := strings.ToLower(alert.Sender)
	if strings.Contains(sender, "canada") || strings.Contains(sender, "cap-pac") {
		return "eccc"
	}
	if strings.Contains(sender, "weather.gov") || strings.Contains(sender, "nws") || strings.Contains(sender, "noaa") {
		return "nws"
	}
	return "generic"
}

// CAPSenderName returns a default sender for a CAP alert.
func CAPSenderName(alert capmodel.Alert, weatherService string) string {
	if DetectCAPSource(alert) == "eccc" {
		return fallbackText(weatherService, "Environment Canada")
	}
	return fallbackText(alert.Sender, "The alerting authority")
}

// ReportTime formats an alert time for speech.
func ReportTime(raw string, timezone string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parsed := ParseCAPTime(raw)
	if parsed.IsZero() {
		for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
			if candidate, err := time.Parse(layout, strings.TrimSpace(raw)); err == nil {
				parsed = candidate
				break
			}
		}
	}
	if parsed.IsZero() {
		return strings.TrimSpace(raw)
	}
	if loc, err := time.LoadLocation(fallbackText(timezone, "Local")); err == nil {
		parsed = parsed.In(loc)
	}
	return strings.TrimSpace(parsed.Format("3:04 PM ") + timezoneName(parsed.Format("MST")))
}

// FormatDisplayTime formats a CAP timestamp for archive/display payloads.
func FormatDisplayTime(raw string, timezone string) string {
	parsed := ParseCAPTime(raw)
	if parsed.IsZero() {
		return CleanFragment(raw)
	}
	if loc, err := time.LoadLocation(fallbackText(timezone, "Local")); err == nil {
		parsed = parsed.In(loc)
	}
	day := parsed.Day()
	return fmt.Sprintf("%d:%02d %s on %s %d%s, %d", hour12(parsed), parsed.Minute(), ampm(parsed), parsed.Format("January"), day, ordinalSuffix(day), parsed.Year())
}

// FormatENDECTime formats a timestamp in the configured ENDEC mimic style.
func FormatENDECTime(value time.Time, mode string, role string, other time.Time) string {
	if value.IsZero() {
		return ""
	}
	local := value.Local()
	switch ResolveENDECMode(mode) {
	case "SAGE EAS", "SAGE DIGITAL":
		text := strings.ToLower(local.Format("03:04 PM"))
		if !other.IsZero() && (local.Year() != other.Local().Year() || local.YearDay() != other.Local().YearDay()) {
			text += strings.ToLower(local.Format(" Mon Jan 02"))
			if local.Year() != other.Local().Year() {
				text += local.Format(", 2006")
			}
		}
		return strings.TrimLeft(text, "0")
	case "TRILITHIC":
		zone := local.Format("MST")
		if zone == "" {
			zone = "UTC"
		}
		return local.Format("01/02/06 15:04:00 ") + zone
	case "BURK":
		if role == "start" {
			return local.Format("January 02, 2006 at 03:04 PM")
		}
		return local.Format("03:04 PM, January 02, 2006")
	case "DASDEC":
		if role == "start" {
			return strings.ToUpper(local.Format("03:04 PM ON Jan 02, 2006"))
		}
		return strings.ToUpper(local.Format("03:04 PM Jan 02, 2006"))
	case "TFT":
		if role == "end" && !other.IsZero() && sameLocalDate(local, other.Local()) {
			return strings.ToUpper(local.Format("03:04 PM"))
		}
		return strings.ToUpper(local.Format("03:04 PM ON Jan 02, 2006"))
	default:
		if !other.IsZero() && sameLocalDate(local, other.Local()) {
			return local.Format("03:04 PM")
		}
		if !other.IsZero() && local.Year() == other.Local().Year() {
			return local.Format("03:04 PM January 02")
		}
		return local.Format("03:04 PM January 02, 2006")
	}
}

// JoinParts joins spoken list parts with an Oxford comma.
func JoinParts(parts []string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		if clean := CleanFragment(part); clean != "" {
			cleaned = append(cleaned, clean)
		}
	}
	switch len(cleaned) {
	case 0:
		return ""
	case 1:
		return cleaned[0]
	case 2:
		return cleaned[0] + " and " + cleaned[1]
	default:
		return strings.Join(cleaned[:len(cleaned)-1], ", ") + ", and " + cleaned[len(cleaned)-1]
	}
}

// BypassForecastRegionCollapse keeps the warning path free of region-database work.
func BypassForecastRegionCollapse(info capmodel.AlertInfo) bool {
	haystack := strings.ToLower(strings.Join([]string{
		AlertSubject(info),
		info.Event,
		info.Headline,
		CAPParam(info, "layer:EC-MSC-SMC:1.0:Alert_Type"),
	}, " "))
	if !strings.Contains(haystack, "warning") {
		return false
	}
	return strings.Contains(haystack, "tornado") || strings.Contains(haystack, "severe thunderstorm")
}

// ForecastRegionAreaPhrase formats one or more forecast-region names for speech.
func ForecastRegionAreaPhrase(names []string) string {
	phrases := make([]string, 0, len(names))
	for _, name := range names {
		localities := ForecastRegionLocalities(name)
		if localities != "" {
			phrases = append(phrases, "areas in and around "+localities)
		}
	}
	return strings.Join(phrases, "; and ")
}

// ForecastRegionLocalities turns ECCC's dash-separated forecast name into a spoken list.
func ForecastRegionLocalities(raw string) string {
	name := strings.TrimSpace(raw)
	if strings.HasSuffix(strings.ToLower(name), " region") {
		name = strings.TrimSpace(name[:len(name)-len(" region")])
	}
	parts := strings.Split(name, " - ")
	if len(parts) == 1 {
		return name
	}
	return JoinParts(parts)
}

// CleanSentence trims a fragment and ensures one final period.
func CleanSentence(value string) string {
	text := strings.TrimSpace(value)
	text = strings.TrimRight(text, ". ")
	if text == "" {
		return "Alert."
	}
	return text + "."
}

// TitleWords title-cases a simple alert phrase.
func TitleWords(value string) string {
	words := strings.Fields(strings.ToLower(strings.TrimSpace(value)))
	for i, word := range words {
		if word == "" {
			continue
		}
		words[i] = strings.ToUpper(word[:1]) + word[1:]
	}
	return strings.Join(words, " ")
}

// CleanLocationCode returns a six-digit SAME location code.
func CleanLocationCode(raw string) string {
	text := strings.Trim(strings.ToUpper(strings.TrimSpace(raw)), "\"' ")
	builder := strings.Builder{}
	for _, r := range text {
		if r >= '0' && r <= '9' {
			builder.WriteRune(r)
		}
	}
	cleaned := builder.String()
	if len(cleaned) != 6 {
		return ""
	}
	return cleaned
}

// MessageID mirrors the old banner short message id heuristic.
func MessageID(alert capmodel.Alert) string {
	if sent := ParseCAPTime(alert.Sent); !sent.IsZero() {
		return "MSG" + sent.UTC().Format("150405")
	}
	id := CleanFragment(alert.Identifier)
	for _, prefix := range []string{"manual_", "test_"} {
		if strings.HasPrefix(id, prefix) {
			suffix := id[strings.LastIndex(id, "_")+1:]
			if len(suffix) > 6 {
				suffix = suffix[len(suffix)-6:]
			}
			return "MSG" + suffix
		}
	}
	return ""
}

func sameSubject(request SAMERequest) string {
	if name := CleanFragment(request.OriginatorName); name != "" {
		return name
	}
	originator := sameOriginatorCode(request)
	if originator == "WXR" {
		weatherLabel := CleanFragment(request.WeatherService)
		if weatherLabel == "" {
			weatherLabel = "Environment Canada"
		}
		return weatherLabel
	}
	labels := endecOriginators[ResolveENDECMode(request.MimicENDEC)]
	if label := labels[originator]; label != "" {
		return label
	}
	if label := originatorLabels[originator]; label != "" {
		return label
	}
	if originator != "" {
		return "Unknown Originator " + originator
	}
	return originatorLabels["EAS"]
}

func sameOriginatorCode(request SAMERequest) string {
	originator := strings.ToUpper(strings.TrimSpace(request.Originator))
	if len(originator) > 3 {
		originator = originator[:3]
	}
	if originator == "" {
		return "EAS"
	}
	return originator
}

func sameEventPhrase(eventName string, mode string) string {
	phrase := withIndefiniteArticle(eventName)
	switch ResolveENDECMode(mode) {
	case "BURK":
		return strings.ToUpper(stripLeadingArticle(phrase, false))
	case "DASDEC":
		return strings.ToUpper(phrase)
	default:
		return phrase
	}
}

func sameVerb(request SAMERequest, mode string) string {
	if ResolveENDECMode(mode) != "SAGE EAS" && ResolveENDECMode(mode) != "SAGE DIGITAL" && ResolveENDECMode(mode) != "TRILITHIC" {
		return "has"
	}
	if sameOriginatorCode(request) == "CIV" {
		return "have"
	}
	return "has"
}

func withIndefiniteArticle(value string) string {
	text := CleanFragment(value)
	if text == "" {
		return "an Alert"
	}
	lower := strings.ToLower(text)
	if strings.HasPrefix(lower, "a ") || strings.HasPrefix(lower, "an ") {
		return text
	}
	first := strings.ToLower(text[:1])
	if strings.Contains("aeiou", first) {
		return "an " + text
	}
	return "a " + text
}

func stripLeadingArticle(value string, includeThe bool) string {
	words := strings.Fields(value)
	if len(words) == 0 {
		return ""
	}
	first := strings.ToLower(words[0])
	if first == "a" || first == "an" || (includeThe && first == "the") {
		return strings.Join(words[1:], " ")
	}
	return strings.Join(words, " ")
}

func formatENDECAreas(areaNames []string, mode string) string {
	switch ResolveENDECMode(mode) {
	case "TRILITHIC":
		return strings.Join(uniqueClean(areaNames), " - ")
	case "BURK", "DASDEC":
		return joinSemicolonParts(areaNames)
	default:
		return JoinParts(areaNames)
	}
}

func joinSemicolonParts(parts []string) string {
	cleaned := uniqueClean(parts)
	if len(cleaned) == 0 {
		return ""
	}
	if len(cleaned) > 1 {
		cleaned[len(cleaned)-1] = "and " + cleaned[len(cleaned)-1]
	}
	return strings.Join(cleaned, "; ") + ";"
}

func formatSAMELeadAreas(areaNames []string) string {
	cleaned := uniqueClean(areaNames)
	switch len(cleaned) {
	case 0:
		return ""
	case 1:
		return cleaned[0]
	case 2:
		return cleaned[0] + "; and " + cleaned[1]
	default:
		return strings.Join(cleaned[:len(cleaned)-1], "; ") + "; and " + cleaned[len(cleaned)-1]
	}
}

func sameLeadTimingSentence(begins time.Time, expires time.Time) string {
	if begins.IsZero() && expires.IsZero() {
		return ""
	}
	switch {
	case !begins.IsZero() && !expires.IsZero():
		if sameLocalDate(begins, expires) {
			return fmt.Sprintf(
				"Beginning at %s and ending at %s on %s.",
				formatSAMELeadClock(begins),
				formatSAMELeadClock(expires),
				formatSAMELeadDate(expires),
			)
		}
		return fmt.Sprintf(
			"Beginning at %s on %s and ending at %s on %s.",
			formatSAMELeadClock(begins),
			formatSAMELeadDate(begins),
			formatSAMELeadClock(expires),
			formatSAMELeadDate(expires),
		)
	case !begins.IsZero():
		return fmt.Sprintf("Beginning at %s on %s.", formatSAMELeadClock(begins), formatSAMELeadDate(begins))
	default:
		return fmt.Sprintf("Ending at %s on %s.", formatSAMELeadClock(expires), formatSAMELeadDate(expires))
	}
}

func formatSAMELeadClock(value time.Time) string {
	return strings.TrimLeft(value.Format("3:04 PM"), "0")
}

func formatSAMELeadDate(value time.Time) string {
	day := value.Day()
	return fmt.Sprintf("%s %d%s, %d", value.Format("January"), day, ordinalSuffix(day), value.Year())
}

func uniqueClean(values []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		clean := CleanFragment(value)
		if clean == "" {
			continue
		}
		key := strings.ToLower(clean)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, clean)
	}
	return out
}

func stripAlertHeadlineState(headline string) string {
	value := strings.TrimSpace(headline)
	for _, suffix := range []string{" - in effect", " - ended", " - updated", " - cancelled", " - canceled"} {
		if strings.HasSuffix(strings.ToLower(value), suffix) {
			return strings.TrimSpace(value[:len(value)-len(suffix)])
		}
	}
	return value
}

func deriveSameCategory(event string) string {
	raw := strings.TrimSpace(event)
	code := strings.ToUpper(raw)
	if category := sameEventCategory(code); category != "" {
		return category
	}
	if fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ' ' || r == '-' || r == ':' || r == '/' || r == '\t' || r == '\n' || r == '\r'
	}); len(fields) > 0 {
		if category := sameEventCategory(strings.ToUpper(strings.TrimSpace(fields[0]))); category != "" {
			return category
		}
	}
	lower := strings.ToLower(raw)
	switch {
	case strings.Contains(lower, "warning"):
		return "warning"
	case strings.Contains(lower, "watch"):
		return "watch"
	case strings.Contains(lower, "advisory"), strings.Contains(lower, "statement"), strings.Contains(lower, "outlook"):
		return "advisory"
	}
	words := strings.Fields(lower)
	if len(words) > 0 {
		if category := sameCategoryTerminal[words[len(words)-1]]; category != "" {
			return category
		}
	}
	if len(raw) == 3 {
		switch strings.ToLower(raw[2:]) {
		case "w":
			return "warning"
		case "a":
			return "watch"
		case "s", "y", "e":
			return "advisory"
		}
	}
	return "advisory"
}

func sameEventCategory(code string) string {
	switch code {
	case "AVW", "BHW", "BZW", "CDW", "CEM", "CFW", "DSW", "EQW", "EVI", "EWW", "FCW", "FFW", "FLW", "FRW", "FSW", "HMW", "HUW", "HWW", "LEW", "NUW", "RHW", "SPW", "SQW", "SSW", "SVR", "TOR", "TRW", "TSW", "VOW", "WSW":
		return "warning"
	case "FFA", "FLA", "HUA", "HWA", "SVA", "TOA", "TRA", "TSA", "WSA":
		return "watch"
	case "ADR", "BWW", "CAE", "DMO", "EAT", "EAN", "LAE", "NMN", "NPT", "RMT", "RWT", "TXB", "TXF", "TXO", "TXP":
		return "advisory"
	default:
		return ""
	}
}

func loadCSVNames(path string, labels map[string]string, codeColumns []string, nameColumns []string) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return
	}
	defer file.Close()
	probe := make([]byte, 4096)
	n, _ := file.Read(probe)
	_, _ = file.Seek(0, 0)
	reader := csv.NewReader(file)
	reader.Comma = detectCSVDelimiter(string(probe[:n]))
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil || len(rows) == 0 {
		return
	}
	headerIndex := map[string]int{}
	for i, name := range rows[0] {
		headerIndex[strings.ToLower(strings.TrimSpace(name))] = i
	}
	codeIndex := firstHeaderIndex(headerIndex, codeColumns)
	nameIndex := firstHeaderIndex(headerIndex, nameColumns)
	start := 1
	if codeIndex < 0 || nameIndex < 0 {
		codeIndex = 0
		nameIndex = 1
		start = 0
	}
	for _, row := range rows[start:] {
		if len(row) <= codeIndex || len(row) <= nameIndex {
			continue
		}
		code := CleanLocationCode(row[codeIndex])
		name := CleanFragment(row[nameIndex])
		if code != "" && name != "" {
			if _, exists := labels[code]; !exists {
				labels[code] = name
			}
		}
	}
}

func loadNWSLocationLabels(path string, labels map[string]string) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return
	}
	defer file.Close()
	probe := make([]byte, 4096)
	n, _ := file.Read(probe)
	_, _ = file.Seek(0, 0)
	reader := csv.NewReader(file)
	reader.Comma = detectCSVDelimiter(string(probe[:n]))
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil || len(rows) == 0 {
		return
	}
	headerIndex := map[string]int{}
	for i, name := range rows[0] {
		headerIndex[strings.ToLower(strings.TrimSpace(name))] = i
	}
	fipsIndex := firstHeaderIndex(headerIndex, []string{"FIPS/SAME", "FIPS", "COUNTY_FIPS"})
	stateZoneIndex := firstHeaderIndex(headerIndex, []string{"STATE+ZONE", "UGC", "ZONE"})
	countyIndex := firstHeaderIndex(headerIndex, []string{"COUNTY_NAME", "COUNTYNAME", "CountyName", "county_name"})
	zoneNameIndex := firstHeaderIndex(headerIndex, []string{"ZONE_NAME", "NAME", "Name"})
	stateIndex := firstHeaderIndex(headerIndex, []string{"STATE", "state"})
	if fipsIndex < 0 && stateZoneIndex < 0 {
		return
	}
	for _, row := range rows[1:] {
		state := rowValue(row, stateIndex)
		countyName := rowValue(row, countyIndex)
		zoneName := rowValue(row, zoneNameIndex)
		label := nwsLocationLabel(countyName, zoneName, state)
		if label == "" {
			continue
		}
		for _, rawCode := range []string{rowValue(row, fipsIndex), rowValue(row, stateZoneIndex)} {
			code := CleanLocationCode(rawCode)
			if code == "" {
				continue
			}
			if _, exists := labels[code]; !exists {
				labels[code] = label
			}
		}
	}
}

func loadNWSMarineLocationLabels(path string, labels map[string]string) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil || len(rows) == 0 {
		return
	}
	headerIndex := map[string]int{}
	for i, name := range rows[0] {
		headerIndex[strings.ToLower(strings.TrimSpace(name))] = i
	}
	sameIndex := firstHeaderIndex(headerIndex, []string{"SAME_CODE", "SAME", "SSNUM"})
	zoneIndex := firstHeaderIndex(headerIndex, []string{"ZONE_UGC", "ZONE", "UGC"})
	nameIndex := firstHeaderIndex(headerIndex, []string{"NAME", "ZONENAME", "ZONE_NAME"})
	if sameIndex < 0 || nameIndex < 0 {
		return
	}
	for _, row := range rows[1:] {
		name := CleanFragment(rowValue(row, nameIndex))
		if name == "" {
			continue
		}
		for _, rawCode := range []string{rowValue(row, sameIndex), rowValue(row, zoneIndex)} {
			code := CleanLocationCode(rawCode)
			if code == "" {
				code = strings.ToUpper(strings.TrimSpace(rawCode))
			}
			if code == "" {
				continue
			}
			if _, exists := labels[code]; !exists {
				labels[code] = name
			}
		}
	}
}

func nwsLocationLabel(countyName string, zoneName string, state string) string {
	name := CleanFragment(fallbackText(countyName, zoneName))
	state = strings.ToUpper(strings.TrimSpace(state))
	if name == "" {
		return ""
	}
	if state == "" || strings.Contains(name, ",") {
		return name
	}
	return name + ", " + state
}

func rowValue(row []string, index int) string {
	if index < 0 || index >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[index])
}

func detectCSVDelimiter(sample string) rune {
	for _, line := range strings.Split(sample, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.Count(line, "|") > strings.Count(line, ",") {
			return '|'
		}
		return ','
	}
	return ','
}

func firstHeaderIndex(header map[string]int, candidates []string) int {
	for _, candidate := range candidates {
		if index, ok := header[strings.ToLower(strings.TrimSpace(candidate))]; ok {
			return index
		}
	}
	return -1
}

func firstMapText(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if data == nil {
			continue
		}
		if value, ok := data[key]; ok {
			if text := CleanFragment(value); text != "" && text != "<nil>" {
				return text
			}
		}
	}
	return ""
}

func mapBoolValue(data map[string]any, key string) (bool, bool) {
	if data == nil {
		return false, false
	}
	value, ok := data[key]
	if !ok {
		return false, false
	}
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes", "on", "enabled":
			return true, true
		case "false", "0", "no", "off", "disabled":
			return false, true
		default:
			return false, true
		}
	case float64:
		return typed != 0, true
	case int:
		return typed != 0, true
	default:
		text := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
		return text == "true" || text == "1" || text == "yes" || text == "on" || text == "enabled", true
	}
}

func fallbackText(values ...string) string {
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			return text
		}
	}
	return ""
}

func nonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if text := CleanFragment(value); text != "" {
			out = append(out, text)
		}
	}
	return out
}

func unique(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = CleanFragment(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func resolveConfigPath(configPath string, configured string) string {
	configured = strings.TrimSpace(configured)
	if configured == "" {
		configured = "."
	}
	if filepath.IsAbs(configured) {
		return filepath.Clean(configured)
	}
	base := filepath.Dir(filepath.Clean(configPath))
	return filepath.Clean(filepath.Join(base, configured))
}

func timezoneName(abbrev string) string {
	switch abbrev {
	case "CST":
		return "Central Standard Time"
	case "CDT":
		return "Central Daylight Time"
	case "MST":
		return "Mountain Standard Time"
	case "MDT":
		return "Mountain Daylight Time"
	case "EST":
		return "Eastern Standard Time"
	case "EDT":
		return "Eastern Daylight Time"
	case "PST":
		return "Pacific Standard Time"
	case "PDT":
		return "Pacific Daylight Time"
	default:
		return abbrev
	}
}

func ordinalSuffix(day int) string {
	if day%100 >= 10 && day%100 <= 20 {
		return "th"
	}
	switch day % 10 {
	case 1:
		return "st"
	case 2:
		return "nd"
	case 3:
		return "rd"
	default:
		return "th"
	}
}

func hour12(value time.Time) int {
	hour := value.Hour() % 12
	if hour == 0 {
		return 12
	}
	return hour
}

func ampm(value time.Time) string {
	if value.Hour() < 12 {
		return "A.M."
	}
	return "P.M."
}

func sameLocalDate(left time.Time, right time.Time) bool {
	return left.Year() == right.Year() && left.YearDay() == right.YearDay()
}
