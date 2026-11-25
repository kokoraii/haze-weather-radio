package webgateway

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type alertTemplatesXML struct {
	XMLName     xml.Name
	Templates   []alertTemplateXML `xml:"template"`
	Automations []alertTemplateXML `xml:"automation"`
}

type alertTemplateXML struct {
	ID          string               `xml:"id,attr,omitempty"`
	Name        string               `xml:"name"`
	Description string               `xml:"description"`
	Automated   automatedTemplateXML `xml:"automated"`
	Schedule    automationTimingXML  `xml:"schedule"`
	SAME        sameTemplateXML      `xml:"same"`
	Content     contentTemplateXML   `xml:"content"`
	Target      automationTargetXML  `xml:"target"`
}

type automatedTemplateXML struct {
	Enabled string              `xml:"enabled"`
	Timing  automationTimingXML `xml:"timing"`
}

type automationTimingXML struct {
	Months   string             `xml:"months"`
	Days     string             `xml:"days"`
	Weekdays string             `xml:"weekdays"`
	Hours    string             `xml:"hours"`
	Minutes  string             `xml:"minutes"`
	Seconds  string             `xml:"seconds"`
	Weeks    automationWeeksXML `xml:"weeks"`
}

type automationWeeksXML struct {
	Weeks []automationWeekXML `xml:"week"`
}

type automationWeekXML struct {
	EventOverride string `xml:"event_override,attr,omitempty"`
	Value         string `xml:",chardata"`
}

type automationTargetXML struct {
	Feeds []automationFeedXML `xml:"feed"`
}

type automationFeedXML struct {
	ID string `xml:"id,attr"`
}

type sameTemplateXML struct {
	Enabled    string               `xml:"enabled"`
	Originator string               `xml:"originator"`
	Event      string               `xml:"event"`
	Locations  templateLocationsXML `xml:"locations"`
	Duration   durationAttrsXML     `xml:"duration"`
	SenderID   string               `xml:"sender_id"`
	Tone       string               `xml:"tone,omitempty"`
}

type templateLocationsXML struct {
	Locations []templateLocationXML `xml:"location"`
}

type templateLocationXML struct {
	ID     string `xml:"id,attr"`
	Source string `xml:"source,attr,omitempty"`
}

type durationAttrsXML struct {
	Hours   string `xml:"hr,attr"`
	Minutes string `xml:"min,attr"`
}

type contentTemplateXML struct {
	AttentionTone string            `xml:"attention_tone,attr,omitempty"`
	ReaderID      string            `xml:"reader_id,attr,omitempty"`
	Langs         []langTemplateXML `xml:"lang"`
}

type langTemplateXML struct {
	Code     string `xml:"code,attr"`
	ReaderID string `xml:"reader_id,attr,omitempty"`
	Text string `xml:"text"`
	File string `xml:"file"`
}

func loadAlertTemplates(configPath string) (map[string]any, error) {
	path := resolveConfigPath(configPath, "managed/configs/automations.xml")
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			path = resolveConfigPath(configPath, "managed/configs/alertTemplates.xml")
			raw, err = os.ReadFile(path)
			if err != nil {
				if os.IsNotExist(err) {
					return map[string]any{}, nil
				}
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	var parsed alertTemplatesXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse automations XML: %w", err)
	}
	items := append([]alertTemplateXML{}, parsed.Templates...)
	items = append(items, parsed.Automations...)
	result := map[string]any{}
	for i, template := range items {
		code := strings.ToUpper(strings.TrimSpace(template.SAME.Event))
		if code == "" {
			code = fmt.Sprintf("TEMPLATE%d", i+1)
		}
		hours := parseIntText(template.SAME.Duration.Hours, 0)
		minutes := parseIntText(template.SAME.Duration.Minutes, 15)
		msg := map[string]any{}
		files := map[string]any{}
		readers := map[string]any{}
		for _, lang := range template.Content.Langs {
			key := strings.TrimSpace(lang.Code)
			if key == "" {
				continue
			}
			msg[key] = strings.TrimSpace(lang.Text)
			files[key] = strings.TrimSpace(lang.File)
			readers[key] = strings.TrimSpace(lang.ReaderID)
		}
		result[code] = map[string]any{
			"id":          strings.TrimSpace(template.ID),
			"name":        fallbackText(template.Name, code),
			"description": strings.TrimSpace(template.Description),
			"automated": map[string]any{
				"enabled":  xmlBool(template.Automated.Enabled, false),
				"schedule": automationScheduleMap(firstAutomationSchedule(template)),
				"target":   automationTargetMap(template.Target),
			},
			"same": map[string]any{
				"enabled":    xmlBool(template.SAME.Enabled, true),
				"originator": strings.TrimSpace(template.SAME.Originator),
				"event":      code,
				"sender_id":  strings.TrimSpace(template.SAME.SenderID),
				"tone":       strings.TrimSpace(template.SAME.Tone),
				"locations":  templateLocations(template.SAME.Locations.Locations),
				"duration": map[string]any{
					"hr":  hours,
					"min": minutes,
				},
				"content": map[string]any{
					"attention_tone": strings.TrimSpace(template.Content.AttentionTone),
					"reader_id":      strings.TrimSpace(template.Content.ReaderID),
					"lang":           msg,
					"file":           files,
				},
			},
			"sameEvent":  code,
			"sameExpire": fmt.Sprintf("%02d%02d", clampInt(hours, 0, 99), clampInt(minutes, 0, 59)),
			"msg":        msg,
			"files":      files,
			"readers":    readers,
		}
	}
	return result, nil
}

func writeAlertTemplates(configPath string, content string) (map[string]any, error) {
	var payload map[string]any
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		return nil, fmt.Errorf("template content must be JSON: %w", err)
	}
	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	allowed, err := configuredLanguages(configPath)
	if err != nil {
		return nil, err
	}
	for _, code := range keys {
		template, _ := payload[code].(map[string]any)
		for lang := range mapFromAny(template["msg"]) {
			if !languageAllowed(lang, allowed) {
				return nil, fmt.Errorf("automation %s uses language %q, which no feed has enabled (configured: %s)", code, lang, strings.Join(allowed, ", "))
			}
		}
	}
	out := alertTemplatesXML{
		XMLName:     xml.Name{Local: "automations"},
		Automations: make([]alertTemplateXML, 0, len(keys)),
	}
	for _, code := range keys {
		template, _ := payload[code].(map[string]any)
		if template == nil {
			continue
		}
		out.Automations = append(out.Automations, templateToXML(code, template))
	}
	raw, err := xml.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, err
	}
	path := resolveConfigPath(configPath, "managed/configs/automations.xml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	body := append([]byte(xml.Header), raw...)
	body = append(body, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	return loadAlertTemplates(configPath)
}

func templateToXML(code string, source map[string]any) alertTemplateXML {
	same, _ := source["same"].(map[string]any)
	content, _ := same["content"].(map[string]any)
	duration, _ := same["duration"].(map[string]any)
	event := strings.ToUpper(fallbackText(stringFromMap(source, "sameEvent"), code))
	if explicit := strings.ToUpper(stringFromMap(same, "event")); explicit != "" {
		event = explicit
	}
	hours := clampInt(intFromMap(duration, "hr", 0), 0, 99)
	minutes := clampInt(intFromMap(duration, "min", 15), 0, 59)
	if expire := stringFromMap(source, "sameExpire"); len(expire) >= 4 {
		hours = clampInt(parseIntText(expire[:2], hours), 0, 99)
		minutes = clampInt(parseIntText(expire[2:4], minutes), 0, 59)
	}
	msg := mapFromAny(source["msg"])
	if len(msg) == 0 {
		msg = mapFromAny(content["lang"])
	}
	readers := mapFromAny(source["readers"])
	files := mapFromAny(source["files"])
	if len(files) == 0 {
		files = mapFromAny(content["file"])
	}
	locations := locationXMLFromAny(same["locations"])
	langs := make([]langTemplateXML, 0, len(msg))
	langKeys := make([]string, 0, len(msg))
	for lang := range msg {
		langKeys = append(langKeys, lang)
	}
	sort.Strings(langKeys)
	for _, lang := range langKeys {
		langs = append(langs, langTemplateXML{
			Code:     lang,
			ReaderID: stringFromMap(readers, lang),
			Text:     fmt.Sprint(msg[lang]),
			File:     stringFromMap(files, lang),
		})
	}
	automated, _ := source["automated"].(map[string]any)
	schedule := automationScheduleXMLFromAny(automated["schedule"])
	target := automationTargetXMLFromAny(automated["target"])
	return alertTemplateXML{
		ID:          stringFromMap(source, "id"),
		Name:        fallbackText(stringFromMap(source, "name"), code),
		Description: stringFromMap(source, "description"),
		Automated: automatedTemplateXML{
			Enabled: boolText(boolFromMap(automated, "enabled", false)),
			Timing:  schedule,
		},
		Schedule: schedule,
		SAME: sameTemplateXML{
			Enabled:    boolText(boolFromMap(same, "enabled", true)),
			Originator: stringFromMap(same, "originator"),
			Event:      event,
			Locations: templateLocationsXML{
				Locations: locations,
			},
			Duration: durationAttrsXML{Hours: strconvText(hours), Minutes: strconvText(minutes)},
			SenderID: stringFromMap(same, "sender_id"),
			Tone:     stringFromMap(same, "tone"),
		},
		Content: contentTemplateXML{
			AttentionTone: stringFromMap(content, "attention_tone"),
			ReaderID:      stringFromMap(content, "reader_id"),
			Langs:         langs,
		},
		Target: target,
	}
}

func firstAutomationSchedule(template alertTemplateXML) automationTimingXML {
	if hasAutomationSchedule(template.Schedule) {
		return template.Schedule
	}
	return template.Automated.Timing
}

func hasAutomationSchedule(schedule automationTimingXML) bool {
	return strings.TrimSpace(schedule.Months) != "" ||
		strings.TrimSpace(schedule.Days) != "" ||
		strings.TrimSpace(schedule.Weekdays) != "" ||
		strings.TrimSpace(schedule.Hours) != "" ||
		strings.TrimSpace(schedule.Minutes) != "" ||
		strings.TrimSpace(schedule.Seconds) != "" ||
		len(schedule.Weeks.Weeks) > 0
}

func automationScheduleMap(schedule automationTimingXML) map[string]any {
	weeks := make([]map[string]string, 0, len(schedule.Weeks.Weeks))
	for _, week := range schedule.Weeks.Weeks {
		value := strings.TrimSpace(week.Value)
		if value == "" {
			continue
		}
		weeks = append(weeks, map[string]string{
			"week":           value,
			"event_override": strings.TrimSpace(week.EventOverride),
		})
	}
	return map[string]any{
		"months":   strings.TrimSpace(schedule.Months),
		"days":     strings.TrimSpace(schedule.Days),
		"weekdays": strings.TrimSpace(schedule.Weekdays),
		"hours":    strings.TrimSpace(schedule.Hours),
		"minutes":  strings.TrimSpace(schedule.Minutes),
		"seconds":  strings.TrimSpace(schedule.Seconds),
		"weeks":    weeks,
	}
}

func automationTargetMap(target automationTargetXML) map[string]any {
	feeds := []string{}
	for _, feed := range target.Feeds {
		if id := strings.TrimSpace(feed.ID); id != "" {
			feeds = append(feeds, id)
		}
	}
	return map[string]any{"feed_ids": feeds}
}

func automationScheduleXMLFromAny(value any) automationTimingXML {
	source := mapFromAny(value)
	weeks := []automationWeekXML{}
	for _, item := range anySlice(source["weeks"]) {
		itemMap, _ := item.(map[string]any)
		if itemMap == nil {
			if week := strings.TrimSpace(fmt.Sprint(item)); week != "" {
				weeks = append(weeks, automationWeekXML{Value: week})
			}
			continue
		}
		week := strings.TrimSpace(fmt.Sprint(itemMap["week"]))
		if week == "" || week == "<nil>" {
			continue
		}
		override := strings.TrimSpace(fmt.Sprint(itemMap["event_override"]))
		if override == "<nil>" {
			override = ""
		}
		weeks = append(weeks, automationWeekXML{Value: week, EventOverride: override})
	}
	return automationTimingXML{
		Months:   stringFromMap(source, "months"),
		Days:     stringFromMap(source, "days"),
		Weekdays: stringFromMap(source, "weekdays"),
		Hours:    stringFromMap(source, "hours"),
		Minutes:  stringFromMap(source, "minutes"),
		Seconds:  stringFromMap(source, "seconds"),
		Weeks:    automationWeeksXML{Weeks: weeks},
	}
}

func automationTargetXMLFromAny(value any) automationTargetXML {
	source := mapFromAny(value)
	feeds := []automationFeedXML{}
	for _, feedID := range stringListAny(source["feed_ids"]) {
		if id := strings.TrimSpace(feedID); id != "" {
			feeds = append(feeds, automationFeedXML{ID: id})
		}
	}
	return automationTargetXML{Feeds: feeds}
}

func templateLocations(locations []templateLocationXML) []map[string]string {
	out := make([]map[string]string, 0, len(locations))
	for _, location := range locations {
		id := strings.TrimSpace(location.ID)
		if id == "" {
			continue
		}
		out = append(out, map[string]string{
			"id":     id,
			"source": strings.TrimSpace(location.Source),
		})
	}
	return out
}

func locationXMLFromAny(value any) []templateLocationXML {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]templateLocationXML, 0, len(items))
	for _, item := range items {
		switch typed := item.(type) {
		case string:
			if id := strings.TrimSpace(typed); id != "" {
				out = append(out, templateLocationXML{ID: id})
			}
		case map[string]any:
			id := strings.TrimSpace(fmt.Sprint(typed["id"]))
			if id == "" || id == "<nil>" {
				continue
			}
			source := strings.TrimSpace(fmt.Sprint(typed["source"]))
			if source == "<nil>" {
				source = ""
			}
			out = append(out, templateLocationXML{ID: id, Source: source})
		}
	}
	return out
}

func mapFromAny(value any) map[string]any {
	result := map[string]any{}
	source, _ := value.(map[string]any)
	for key, item := range source {
		result[key] = item
	}
	return result
}

func stringFromMap(source map[string]any, key string) string {
	if source == nil {
		return ""
	}
	value, ok := source[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func intFromMap(source map[string]any, key string, fallback int) int {
	if source == nil {
		return fallback
	}
	switch typed := source[key].(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case string:
		return parseIntText(typed, fallback)
	default:
		return fallback
	}
}

func boolFromMap(source map[string]any, key string, fallback bool) bool {
	if source == nil {
		return fallback
	}
	switch typed := source[key].(type) {
	case bool:
		return typed
	case string:
		return xmlBool(typed, fallback)
	default:
		return fallback
	}
}

func boolText(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func strconvText(value int) string {
	return fmt.Sprintf("%02d", value)
}

func clampInt(value int, minValue int, maxValue int) int {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

// configuredLanguages lists every language code enabled on at least one feed.
// Automation text may only be written in these languages.
func configuredLanguages(configPath string) ([]string, error) {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return nil, err
	}
	parsed, err := loadFeedsXML(configPath, root)
	if err != nil {
		return nil, err
	}
	codes := []string{}
	for _, feed := range parsed.Feeds {
		codes = append(codes, feedLanguages(feed)...)
	}
	codes = uniqueStrings(codes)
	sort.Strings(codes)
	return codes, nil
}

// languageAllowed accepts an exact configured code, or a bare base language
// ("en") when some feed carries a regional form of it ("en-US").
func languageAllowed(code string, allowed []string) bool {
	code = strings.ToLower(strings.TrimSpace(code))
	for _, candidate := range allowed {
		candidate = strings.ToLower(candidate)
		if candidate == code || (!strings.Contains(code, "-") && strings.HasPrefix(candidate, code+"-")) {
			return true
		}
	}
	return false
}
