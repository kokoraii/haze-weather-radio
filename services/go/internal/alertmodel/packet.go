// Package alertmodel defines the canonical alert payload shared between Haze services.
package alertmodel

import (
	"encoding/json"
	"fmt"
	"strings"
)

const packetVersion = 1

// Packet is the single source payload for an alert moving across services.
type Packet struct {
	Version      int            `json:"version"`
	ID           string         `json:"id,omitempty"`
	Source       string         `json:"source,omitempty"`
	FeedID       string         `json:"feed_id,omitempty"`
	FeedIDs      []string       `json:"feed_ids,omitempty"`
	MessageType  string         `json:"message_type,omitempty"`
	Content      Content        `json:"content,omitempty"`
	Timing       Timing         `json:"timing,omitempty"`
	Areas        Areas          `json:"areas,omitempty"`
	Storm        *StormInfo     `json:"storm,omitempty"`
	SAME         *SAME          `json:"same,omitempty"`
	Audio        *Audio         `json:"audio,omitempty"`
	Options      Options        `json:"options,omitempty"`
	Presentation Presentation   `json:"presentation,omitempty"`
	Meta         map[string]any `json:"meta,omitempty"`
}

// Content contains public alert text and classification facts from CAP or operator input.
type Content struct {
	Title           string `json:"title,omitempty"`
	Headline        string `json:"headline,omitempty"`
	Event           string `json:"event,omitempty"`
	EventName       string `json:"event_name,omitempty"`
	Severity        string `json:"severity,omitempty"`
	Urgency         string `json:"urgency,omitempty"`
	Certainty       string `json:"certainty,omitempty"`
	Description     string `json:"description,omitempty"`
	Instruction     string `json:"instruction,omitempty"`
	CustomText      string `json:"custom_text,omitempty"`
	BackgroundColor string `json:"background_color,omitempty"`
}

// Timing contains alert lifecycle timestamps as RFC3339-compatible strings.
type Timing struct {
	SentAt       string `json:"sent_at,omitempty"`
	EffectiveAt  string `json:"effective_at,omitempty"`
	OnsetAt      string `json:"onset_at,omitempty"`
	ExpiresAt    string `json:"expires_at,omitempty"`
	ScheduledFor string `json:"scheduled_for,omitempty"`
}

// Areas contains resolved names and raw location codes targeted by the alert.
type Areas struct {
	Names       []string            `json:"names,omitempty"`
	Codes       []string            `json:"codes,omitempty"`
	Locations   []LocationReference `json:"locations,omitempty"`
	ThreatAreas []ThreatArea        `json:"threat_areas,omitempty"`
}

// ThreatArea is an additive free-form hazard geometry. Zone identifiers remain
// authoritative for relay and SAME decisions.
type ThreatArea struct {
	Status         string   `json:"status"`
	Description    string   `json:"description,omitempty"`
	Polygons       []string `json:"polygons,omitempty"`
	CAPCPLocations []string `json:"cap_cp_locations,omitempty"`
}

// StormInfo carries ECCC storm-specific facts without replacing the original
// CAP parameters retained in the archived XML.
type StormInfo struct {
	Speed                   string     `json:"speed,omitempty"`
	SpeedValue              *float64   `json:"speed_value,omitempty"`
	SpeedUnit               string     `json:"speed_unit,omitempty"`
	DirectionDegrees        *float64   `json:"direction_degrees,omitempty"`
	GeometryType            string     `json:"geometry_type,omitempty"`
	Points                  []GeoPoint `json:"points,omitempty"`
	Time                    string     `json:"time,omitempty"`
	MotionDescription       string     `json:"motion_description,omitempty"`
	PositionDescription     string     `json:"position_description,omitempty"`
	ReferenceLocationPoints string     `json:"reference_location_points,omitempty"`
}

// GeoPoint is a CAP latitude/longitude coordinate.
type GeoPoint struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// LocationReference keeps an alert's raw targeting identifier beside the
// confidence-bearing canonical location returned by haze-location.
type LocationReference struct {
	CanonicalID          string               `json:"canonical_id,omitempty"`
	CandidateID          string               `json:"candidate_id,omitempty"`
	Name                 string               `json:"name,omitempty"`
	Kind                 string               `json:"kind,omitempty"`
	Country              string               `json:"country,omitempty"`
	Region               string               `json:"region,omitempty"`
	RawIdentifiers       []LocationIdentifier `json:"raw_identifiers,omitempty"`
	CanonicalIdentifiers []LocationIdentifier `json:"canonical_identifiers,omitempty"`
	MatchScore           float64              `json:"match_score,omitempty"`
	MatchConfidence      string               `json:"match_confidence,omitempty"`
	MatchMethod          string               `json:"match_method,omitempty"`
	SourceQuality        float64              `json:"source_quality,omitempty"`
	CatalogGeneration    string               `json:"catalog_generation,omitempty"`
	Actionable           bool                 `json:"actionable"`
	Ambiguous            bool                 `json:"ambiguous,omitempty"`
}

// LocationIdentifier is an original source-qualified code. Leading zeros and
// source spelling are preserved for exact fallback comparisons.
type LocationIdentifier struct {
	Authority string `json:"authority,omitempty"`
	Scheme    string `json:"scheme"`
	Value     string `json:"value"`
}

// SAME contains SAME/EAS generation facts.
type SAME struct {
	Include          bool     `json:"include"`
	SuppressedReason string   `json:"suppressed_reason,omitempty"`
	Event            string   `json:"event,omitempty"`
	EventName        string   `json:"event_name,omitempty"`
	EventSource      string   `json:"event_source,omitempty"`
	EventReason      string   `json:"event_reason,omitempty"`
	EventConfidence  string   `json:"event_confidence,omitempty"`
	AlertClass       string   `json:"alert_class,omitempty"`
	Phenomenon       string   `json:"phenomenon,omitempty"`
	Originator       string   `json:"originator,omitempty"`
	OriginatorName   string   `json:"originator_name,omitempty"`
	WeatherService   string   `json:"weather_service,omitempty"`
	Locations        []string `json:"locations,omitempty"`
	Duration         string   `json:"duration,omitempty"`
	SentAt           string   `json:"sent_at,omitempty"`
	BeginsAt         string   `json:"begins_at,omitempty"`
	ExpiresAt        string   `json:"expires_at,omitempty"`
	Tone             string   `json:"tone,omitempty"`
	Callsign         string   `json:"callsign,omitempty"`
	Header           string   `json:"header,omitempty"`
	Translation      string   `json:"translation,omitempty"`
}

// Audio points to authoritative CAP audio or generated alert audio.
type Audio struct {
	URL           string `json:"url,omitempty"`
	MimeType      string `json:"mime_type,omitempty"`
	Language      string `json:"language,omitempty"`
	Description   string `json:"description,omitempty"`
	Authoritative bool   `json:"authoritative,omitempty"`
	Path          string `json:"path,omitempty"`
	Format        string `json:"format,omitempty"`
	SampleRate    int    `json:"sample_rate,omitempty"`
	Channels      int    `json:"channels,omitempty"`
	Bytes         int    `json:"bytes,omitempty"`
	Source        string `json:"source,omitempty"`
}

// Options contains operator and feed policy that affects alert presentation.
type Options struct {
	BroadcastImmediate     bool `json:"broadcast_immediate,omitempty"`
	PrependSAMETranslation bool `json:"prepend_same_translation,omitempty"`
}

// Presentation contains rendered, cacheable views derived from the packet.
type Presentation struct {
	SpeechText string `json:"speech_text,omitempty"`
	BannerText string `json:"banner_text,omitempty"`
}

// Normalize returns a copy with stable defaults and compact repeated values.
func (p Packet) Normalize() Packet {
	if p.Version == 0 {
		p.Version = packetVersion
	}
	p.ID = clean(p.ID)
	p.Source = clean(p.Source)
	p.FeedID = clean(p.FeedID)
	p.MessageType = clean(p.MessageType)
	p.FeedIDs = uniqueClean(p.FeedIDs)
	p.Areas.Names = uniqueClean(p.Areas.Names)
	p.Areas.Codes = uniqueClean(p.Areas.Codes)
	p.Areas.Locations = normalizeLocationReferences(p.Areas.Locations)
	p.Areas.ThreatAreas = normalizeThreatAreas(p.Areas.ThreatAreas)
	if p.Storm != nil {
		p.Storm.Speed = clean(p.Storm.Speed)
		p.Storm.SpeedUnit = strings.ToLower(clean(p.Storm.SpeedUnit))
		p.Storm.GeometryType = strings.ToLower(clean(p.Storm.GeometryType))
		p.Storm.Time = clean(p.Storm.Time)
		p.Storm.MotionDescription = clean(p.Storm.MotionDescription)
		p.Storm.PositionDescription = clean(p.Storm.PositionDescription)
		p.Storm.ReferenceLocationPoints = clean(p.Storm.ReferenceLocationPoints)
	}
	if p.SAME != nil {
		p.SAME.Locations = uniqueClean(p.SAME.Locations)
	}
	return p
}

// FromMap returns an alert packet from a transport map, preferring data.alert_packet.
func FromMap(data map[string]any) (Packet, bool) {
	if data == nil {
		return Packet{}, false
	}
	if raw, ok := data["alert_packet"]; ok {
		if packet, ok := packetFromAny(raw); ok {
			return packet.Normalize(), true
		}
	}
	packet := LegacyFromMap(data).Normalize()
	return packet, packet.ID != "" || packet.Content.Event != "" || packet.Content.Description != "" || packet.Content.CustomText != ""
}

// LegacyFromMap builds a packet from the older flat alert event shape.
func LegacyFromMap(data map[string]any) Packet {
	if data == nil {
		return Packet{}
	}
	packet := Packet{
		Version:     packetVersion,
		ID:          firstText(data, "alert_id", "id", "identifier", "subject"),
		Source:      firstText(data, "cap_source", "source"),
		FeedID:      firstText(data, "feed_id"),
		FeedIDs:     stringList(firstValue(data, "feed_ids")),
		MessageType: firstText(data, "message_type", "msg_type"),
		Content: Content{
			Title:           firstText(data, "title", "header"),
			Headline:        firstText(data, "headline"),
			Event:           firstText(data, "event"),
			EventName:       firstText(data, "same_event_name", "event_name"),
			Severity:        firstText(data, "severity"),
			Urgency:         firstText(data, "urgency"),
			Certainty:       firstText(data, "certainty"),
			Description:     firstText(data, "description"),
			Instruction:     firstText(data, "instruction"),
			CustomText:      firstText(data, "custom_text", "voice_message", "text", "message"),
			BackgroundColor: firstText(data, "background_color"),
		},
		Timing: Timing{
			SentAt:       firstText(data, "alert_sent_at", "sent"),
			EffectiveAt:  firstText(data, "effective", "effective_at"),
			OnsetAt:      firstText(data, "onset", "onset_at"),
			ExpiresAt:    firstText(data, "alert_expires_at", "expires", "expires_at"),
			ScheduledFor: firstText(data, "scheduled_for", "schedule_at"),
		},
		Options: Options{
			BroadcastImmediate:     boolValue(firstValue(data, "broadcast_immediate")),
			PrependSAMETranslation: boolValue(firstValue(data, "prepend_same_translation")),
		},
		Presentation: Presentation{
			SpeechText: firstText(data, "alert_text", "tts_text"),
			BannerText: firstText(data, "banner_text"),
		},
	}
	if packet.Content.CustomText == "" && packet.Presentation.SpeechText != "" {
		packet.Content.CustomText = packet.Presentation.SpeechText
	}
	if names := stringList(firstValue(data, "area_names", "areas")); len(names) > 0 {
		packet.Areas.Names = names
	}
	if codes := stringList(firstValue(data, "same_locations", "locations")); len(codes) > 0 {
		packet.Areas.Codes = codes
	}
	packet.Areas.Locations = locationReferences(firstValue(data, "canonical_locations"))
	packet.Areas.ThreatAreas = threatAreas(firstValue(data, "threat_areas"))
	packet.Storm = stormInfo(firstValue(data, "storm", "storm_characteristics"))
	if hasSAMEFields(data) {
		packet.SAME = &SAME{
			Include:          boolValueDefault(firstValue(data, "include_same"), true),
			SuppressedReason: firstText(data, "same_suppressed_reason"),
			Event:            firstText(data, "same_event"),
			EventName:        firstText(data, "same_event_name"),
			EventSource:      firstText(data, "same_event_source"),
			EventReason:      firstText(data, "same_event_reason"),
			EventConfidence:  firstText(data, "same_event_confidence"),
			AlertClass:       firstText(data, "same_alert_class"),
			Phenomenon:       firstText(data, "same_event_phenomenon"),
			Originator:       firstText(data, "same_originator"),
			OriginatorName:   firstText(data, "same_originator_name"),
			WeatherService:   firstText(data, "same_weather_service"),
			Locations:        stringList(firstValue(data, "same_locations", "locations")),
			Duration:         firstText(data, "same_duration"),
			SentAt:           firstText(data, "same_sent_at"),
			BeginsAt:         firstText(data, "same_begins_at"),
			ExpiresAt:        firstText(data, "same_expires_at"),
			Tone:             firstText(data, "same_tone"),
			Callsign:         firstText(data, "same_callsign"),
			Header:           firstText(data, "same_header"),
			Translation:      firstText(data, "same_translation", "same_intro"),
		}
	}
	if hasAudioFields(data) {
		packet.Audio = &Audio{
			URL:           firstText(data, "audio_url", "authoritative_url"),
			MimeType:      firstText(data, "audio_mime_type"),
			Language:      firstText(data, "audio_language"),
			Description:   firstText(data, "audio_description"),
			Authoritative: boolValue(firstValue(data, "audio_authoritative")),
			Path:          firstText(data, "audio_path"),
			Format:        firstText(data, "audio_format", "format"),
			SampleRate:    intValue(firstValue(data, "sample_rate")),
			Channels:      intValue(firstValue(data, "channels")),
			Bytes:         intValue(firstValue(data, "audio_bytes")),
			Source:        firstText(data, "source"),
		}
	}
	return packet
}

// LegacyFields returns the flattened fields needed by older alert consumers.
func LegacyFields(packet Packet) map[string]any {
	packet = packet.Normalize()
	out := map[string]any{
		"alert_id":     packet.ID,
		"message_type": packet.MessageType,
		"source":       packet.Source,
		"alert_source": packet.Source,
		"cap_source":   packet.Source,
	}
	setText(out, "feed_id", packet.FeedID)
	if len(packet.FeedIDs) > 0 {
		out["feed_ids"] = packet.FeedIDs
	}
	setText(out, "title", firstNonBlank(packet.Content.Headline, packet.Content.Title))
	setText(out, "headline", packet.Content.Headline)
	setText(out, "event", firstNonBlank(packet.Content.Event, packet.Content.EventName))
	setText(out, "severity", packet.Content.Severity)
	setText(out, "urgency", packet.Content.Urgency)
	setText(out, "certainty", packet.Content.Certainty)
	setText(out, "description", packet.Content.Description)
	setText(out, "instruction", packet.Content.Instruction)
	setText(out, "background_color", packet.Content.BackgroundColor)
	setText(out, "alert_sent_at", packet.Timing.SentAt)
	setText(out, "effective", packet.Timing.EffectiveAt)
	setText(out, "onset", packet.Timing.OnsetAt)
	setText(out, "alert_expires_at", packet.Timing.ExpiresAt)
	setText(out, "scheduled_for", packet.Timing.ScheduledFor)
	if len(packet.Areas.Names) > 0 {
		out["area_names"] = packet.Areas.Names
	}
	if len(packet.Areas.Codes) > 0 {
		out["locations"] = packet.Areas.Codes
	}
	if len(packet.Areas.Locations) > 0 {
		out["canonical_locations"] = packet.Areas.Locations
	}
	if len(packet.Areas.ThreatAreas) > 0 {
		out["threat_areas"] = packet.Areas.ThreatAreas
	}
	if packet.Storm != nil {
		out["storm"] = packet.Storm
	}
	if packet.Presentation.SpeechText != "" {
		out["alert_text"] = packet.Presentation.SpeechText
	}
	if packet.Presentation.BannerText != "" {
		out["banner_text"] = packet.Presentation.BannerText
	}
	if packet.Options.BroadcastImmediate {
		out["broadcast_immediate"] = true
	}
	if packet.Options.PrependSAMETranslation {
		out["prepend_same_translation"] = true
	}
	if packet.SAME != nil {
		out["include_same"] = packet.SAME.Include
		setText(out, "same_suppressed_reason", packet.SAME.SuppressedReason)
		setText(out, "same_event", packet.SAME.Event)
		setText(out, "same_event_name", packet.SAME.EventName)
		setText(out, "same_event_source", packet.SAME.EventSource)
		setText(out, "same_event_reason", packet.SAME.EventReason)
		setText(out, "same_event_confidence", packet.SAME.EventConfidence)
		setText(out, "same_alert_class", packet.SAME.AlertClass)
		setText(out, "same_event_phenomenon", packet.SAME.Phenomenon)
		setText(out, "same_originator", packet.SAME.Originator)
		setText(out, "same_originator_name", packet.SAME.OriginatorName)
		setText(out, "same_weather_service", packet.SAME.WeatherService)
		if len(packet.SAME.Locations) > 0 {
			out["same_locations"] = packet.SAME.Locations
		}
		setText(out, "same_duration", packet.SAME.Duration)
		setText(out, "same_sent_at", packet.SAME.SentAt)
		setText(out, "same_begins_at", packet.SAME.BeginsAt)
		setText(out, "same_expires_at", packet.SAME.ExpiresAt)
		setText(out, "same_tone", packet.SAME.Tone)
		setText(out, "same_callsign", packet.SAME.Callsign)
		setText(out, "same_header", packet.SAME.Header)
		setText(out, "same_intro", packet.SAME.Translation)
		setText(out, "same_translation", packet.SAME.Translation)
	}
	if packet.Audio != nil {
		setText(out, "audio_url", packet.Audio.URL)
		setText(out, "authoritative_url", packet.Audio.URL)
		setText(out, "audio_mime_type", packet.Audio.MimeType)
		setText(out, "audio_language", packet.Audio.Language)
		setText(out, "audio_description", packet.Audio.Description)
		if packet.Audio.Authoritative {
			out["audio_authoritative"] = true
		}
		setText(out, "audio_path", packet.Audio.Path)
		setText(out, "audio_format", packet.Audio.Format)
		if packet.Audio.SampleRate > 0 {
			out["sample_rate"] = packet.Audio.SampleRate
		}
		if packet.Audio.Channels > 0 {
			out["channels"] = packet.Audio.Channels
		}
		if packet.Audio.Bytes > 0 {
			out["audio_bytes"] = packet.Audio.Bytes
		}
	}
	return out
}

// WithLegacyFields returns a map that contains alert_packet plus flattened compatibility fields.
func WithLegacyFields(packet Packet, extra map[string]any) map[string]any {
	out := LegacyFields(packet)
	for key, value := range extra {
		out[key] = value
	}
	out["alert_packet"] = packet.Normalize()
	return out
}

// MergePacketFields overlays packet-derived legacy fields without discarding explicit transport fields.
func MergePacketFields(data map[string]any) map[string]any {
	packet, ok := FromMap(data)
	if !ok {
		return data
	}
	out := LegacyFields(packet)
	for key, value := range data {
		out[key] = value
	}
	out["alert_packet"] = packet.Normalize()
	return out
}

// BodyText returns the authoritative alert body text from packet content.
func BodyText(packet Packet) string {
	parts := []string{}
	if text := clean(packet.Content.CustomText); text != "" {
		return text
	}
	for _, value := range []string{packet.Content.Description, packet.Content.Instruction} {
		if text := clean(value); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, " ")
}

func packetFromAny(value any) (Packet, bool) {
	switch typed := value.(type) {
	case Packet:
		return typed, true
	case *Packet:
		if typed == nil {
			return Packet{}, false
		}
		return *typed, true
	case map[string]any:
		var packet Packet
		if decodeMap(typed, &packet) == nil {
			return packet, true
		}
	case string:
		var packet Packet
		if err := json.Unmarshal([]byte(typed), &packet); err == nil {
			return packet, true
		}
	}
	return Packet{}, false
}

func decodeMap(source map[string]any, target any) error {
	raw, err := json.Marshal(source)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode alert packet: %w", err)
	}
	return nil
}

func firstValue(data map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := data[key]; ok {
			return value
		}
	}
	return nil
}

func firstText(data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := data[key]; ok {
			if text := clean(fmt.Sprint(value)); text != "" && text != "<nil>" {
				return text
			}
		}
	}
	return ""
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if text := clean(value); text != "" {
			return text
		}
	}
	return ""
}

func stringList(value any) []string {
	out := []string{}
	switch typed := value.(type) {
	case []string:
		out = append(out, typed...)
	case []any:
		for _, item := range typed {
			if text := clean(fmt.Sprint(item)); text != "" && text != "<nil>" {
				out = append(out, text)
			}
		}
	case string:
		if text := clean(typed); text != "" {
			for _, part := range strings.Split(text, ",") {
				if cleanPart := clean(part); cleanPart != "" {
					out = append(out, cleanPart)
				}
			}
		}
	}
	return uniqueClean(out)
}

func locationReferences(value any) []LocationReference {
	if value == nil {
		return nil
	}
	if typed, ok := value.([]LocationReference); ok {
		return normalizeLocationReferences(typed)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var references []LocationReference
	if err := json.Unmarshal(raw, &references); err != nil {
		return nil
	}
	return normalizeLocationReferences(references)
}

func threatAreas(value any) []ThreatArea {
	if value == nil {
		return nil
	}
	if typed, ok := value.([]ThreatArea); ok {
		return normalizeThreatAreas(typed)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var areas []ThreatArea
	if err := json.Unmarshal(raw, &areas); err != nil {
		return nil
	}
	return normalizeThreatAreas(areas)
}

func normalizeThreatAreas(values []ThreatArea) []ThreatArea {
	out := make([]ThreatArea, 0, len(values))
	seen := map[string]struct{}{}
	for _, area := range values {
		area.Status = strings.ToLower(clean(area.Status))
		area.Description = clean(area.Description)
		area.Polygons = uniqueClean(area.Polygons)
		area.CAPCPLocations = uniqueClean(area.CAPCPLocations)
		if area.Status == "" || len(area.Polygons) == 0 {
			continue
		}
		key := area.Status + "\x00" + strings.Join(area.Polygons, "\x00")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, area)
	}
	return out
}

func stormInfo(value any) *StormInfo {
	if value == nil {
		return nil
	}
	if typed, ok := value.(*StormInfo); ok {
		return typed
	}
	if typed, ok := value.(StormInfo); ok {
		return &typed
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var storm StormInfo
	if err := json.Unmarshal(raw, &storm); err != nil {
		return nil
	}
	return &storm
}

func normalizeLocationReferences(values []LocationReference) []LocationReference {
	out := make([]LocationReference, 0, len(values))
	seen := map[string]struct{}{}
	for _, reference := range values {
		reference.CanonicalID = clean(reference.CanonicalID)
		reference.CandidateID = clean(reference.CandidateID)
		reference.Name = clean(reference.Name)
		reference.Kind = clean(reference.Kind)
		reference.Country = clean(reference.Country)
		reference.Region = clean(reference.Region)
		reference.MatchConfidence = strings.ToLower(clean(reference.MatchConfidence))
		reference.MatchMethod = clean(reference.MatchMethod)
		reference.CatalogGeneration = clean(reference.CatalogGeneration)
		rawIdentifiers := make([]LocationIdentifier, 0, len(reference.RawIdentifiers))
		identifierSeen := map[string]struct{}{}
		for _, identifier := range reference.RawIdentifiers {
			identifier.Authority = clean(identifier.Authority)
			identifier.Scheme = strings.ToLower(clean(identifier.Scheme))
			identifier.Value = clean(identifier.Value)
			if identifier.Scheme == "" || identifier.Value == "" {
				continue
			}
			key := strings.ToLower(identifier.Authority) + "\x00" + identifier.Scheme + "\x00" + identifier.Value
			if _, exists := identifierSeen[key]; exists {
				continue
			}
			identifierSeen[key] = struct{}{}
			rawIdentifiers = append(rawIdentifiers, identifier)
		}
		reference.RawIdentifiers = rawIdentifiers
		reference.CanonicalIdentifiers = normalizeLocationIdentifiers(reference.CanonicalIdentifiers)
		if reference.Ambiguous || (reference.MatchConfidence != "exact" && reference.MatchConfidence != "high") {
			reference.Actionable = false
			if reference.CandidateID == "" {
				reference.CandidateID = reference.CanonicalID
			}
			reference.CanonicalID = ""
		}
		if reference.CanonicalID == "" {
			reference.Actionable = false
		}
		key := strings.ToLower(reference.CanonicalID)
		if key == "" && len(reference.RawIdentifiers) > 0 {
			identifier := reference.RawIdentifiers[0]
			key = strings.ToLower(identifier.Authority) + "\x00" + identifier.Scheme + "\x00" + identifier.Value
		}
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, reference)
	}
	return out
}

func normalizeLocationIdentifiers(values []LocationIdentifier) []LocationIdentifier {
	out := make([]LocationIdentifier, 0, len(values))
	seen := map[string]struct{}{}
	for _, identifier := range values {
		identifier.Authority = clean(identifier.Authority)
		identifier.Scheme = strings.ToLower(clean(identifier.Scheme))
		identifier.Value = clean(identifier.Value)
		if identifier.Scheme == "" || identifier.Value == "" {
			continue
		}
		key := strings.ToLower(identifier.Authority) + "\x00" + identifier.Scheme + "\x00" + identifier.Value
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, identifier)
	}
	return out
}

func uniqueClean(values []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		text := clean(value)
		if text == "" {
			continue
		}
		key := strings.ToLower(text)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, text)
	}
	return out
}

func boolValue(value any) bool {
	return boolValueDefault(value, false)
}

func boolValueDefault(value any, fallback bool) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(clean(typed)) {
		case "true", "1", "yes", "on", "enabled":
			return true
		case "false", "0", "no", "off", "disabled":
			return false
		}
	case float64:
		return typed != 0
	case int:
		return typed != 0
	}
	return fallback
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case json.Number:
		if n, err := typed.Int64(); err == nil {
			return int(n)
		}
	case string:
		var parsed int
		if _, err := fmt.Sscanf(clean(typed), "%d", &parsed); err == nil {
			return parsed
		}
	}
	return 0
}

func hasSAMEFields(data map[string]any) bool {
	for _, key := range []string{"include_same", "same_event", "same_originator", "same_locations", "same_translation", "same_intro"} {
		if _, ok := data[key]; ok {
			return true
		}
	}
	return false
}

func hasAudioFields(data map[string]any) bool {
	for _, key := range []string{"audio_url", "authoritative_url", "audio_path", "audio_format", "sample_rate"} {
		if _, ok := data[key]; ok {
			return true
		}
	}
	return false
}

func setText(out map[string]any, key string, value string) {
	if text := clean(value); text != "" {
		out[key] = text
	}
}

func clean(value string) string {
	return strings.TrimSpace(value)
}
