package capmodel

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type capXML struct {
	XMLName    xml.Name  `xml:"alert"`
	Identifier string    `xml:"identifier"`
	Sender     string    `xml:"sender"`
	Sent       string    `xml:"sent"`
	Status     string    `xml:"status"`
	MsgType    string    `xml:"msgType"`
	Scope      string    `xml:"scope"`
	Note       string    `xml:"note"`
	Code       []string  `xml:"code"`
	References string    `xml:"references"`
	Incidents  string    `xml:"incidents"`
	Infos      []infoXML `xml:"info"`
}

type infoXML struct {
	Language    string        `xml:"language"`
	Category    []string      `xml:"category"`
	Event       string        `xml:"event"`
	Response    []string      `xml:"responseType"`
	Urgency     string        `xml:"urgency"`
	Severity    string        `xml:"severity"`
	Certainty   string        `xml:"certainty"`
	Audience    string        `xml:"audience"`
	Effective   string        `xml:"effective"`
	Onset       string        `xml:"onset"`
	Expires     string        `xml:"expires"`
	SenderName  string        `xml:"senderName"`
	Headline    string        `xml:"headline"`
	Description string        `xml:"description"`
	Instruction string        `xml:"instruction"`
	Web         string        `xml:"web"`
	EventCodes  []pairXML     `xml:"eventCode"`
	Areas       []areaXML     `xml:"area"`
	Parameters  []pairXML     `xml:"parameter"`
	Resources   []resourceXML `xml:"resource"`
}

type areaXML struct {
	Description string    `xml:"areaDesc"`
	Polygons    []string  `xml:"polygon"`
	Circles     []string  `xml:"circle"`
	Geocodes    []pairXML `xml:"geocode"`
}

type pairXML struct {
	Name  string `xml:"valueName"`
	Value string `xml:"value"`
}

type resourceXML struct {
	Description string `xml:"resourceDesc"`
	MimeType    string `xml:"mimeType"`
	URI         string `xml:"uri"`
	DerefURI    string `xml:"derefUri"`
}

// ParseCAP parses a CAP XML document into a normalized alert.
func ParseCAP(raw []byte) (Alert, error) {
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	decoder.Strict = true
	var parsed capXML
	if err := decoder.Decode(&parsed); err != nil {
		return Alert{}, err
	}
	if parsed.XMLName.Local != "alert" {
		return Alert{}, fmt.Errorf("CAP root element is %q, expected alert", parsed.XMLName.Local)
	}

	alert := Alert{
		Identifier:  clean(parsed.Identifier),
		Sender:      clean(parsed.Sender),
		Sent:        clean(parsed.Sent),
		Status:      clean(parsed.Status),
		MessageType: clean(parsed.MsgType),
		Scope:       clean(parsed.Scope),
		Note:        clean(parsed.Note),
		Code:        cleanSlice(parsed.Code),
		References:  clean(parsed.References),
		Incidents:   clean(parsed.Incidents),
		RawXML:      string(raw),
	}
	for _, info := range parsed.Infos {
		alert.Infos = append(alert.Infos, normalizeInfo(info))
	}
	alert.Warnings = validateCAP(alert)
	if len(alert.Warnings) > 0 && hasFatalCAPWarning(alert.Warnings) {
		return Alert{}, fmt.Errorf("invalid CAP alert %s: %s", alert.Identifier, strings.Join(alert.Warnings, "; "))
	}
	return alert, nil
}

func validateCAP(alert Alert) []string {
	var warnings []string
	required := []struct {
		name  string
		value string
	}{
		{"identifier", alert.Identifier},
		{"sender", alert.Sender},
		{"sent", alert.Sent},
		{"status", alert.Status},
		{"msgType", alert.MessageType},
		{"scope", alert.Scope},
	}
	for _, item := range required {
		if strings.TrimSpace(item.value) == "" {
			warnings = append(warnings, "fatal: missing "+item.name)
		}
	}
	if alert.Sent != "" && parseCAPTime(alert.Sent).IsZero() {
		warnings = append(warnings, "fatal: invalid sent timestamp")
	}
	warnings = appendEnumWarning(warnings, "status", alert.Status, []string{"Actual", "Exercise", "System", "Test", "Draft"}, true)
	warnings = appendEnumWarning(warnings, "msgType", alert.MessageType, []string{"Alert", "Update", "Cancel", "Ack", "Error"}, true)
	warnings = appendEnumWarning(warnings, "scope", alert.Scope, []string{"Public", "Restricted", "Private"}, true)
	if len(alert.Infos) == 0 &&
		!strings.EqualFold(alert.MessageType, "Cancel") &&
		!strings.EqualFold(alert.Status, "System") {
		warnings = append(warnings, "fatal: non-cancel alert has no info block")
	}
	for index, info := range alert.Infos {
		prefix := fmt.Sprintf("info[%d]", index)
		if info.Event == "" {
			warnings = append(warnings, prefix+": missing event")
		}
		warnings = appendEnumWarning(warnings, prefix+".urgency", info.Urgency, []string{"Immediate", "Expected", "Future", "Past", "Unknown"}, false)
		warnings = appendEnumWarning(warnings, prefix+".severity", info.Severity, []string{"Extreme", "Severe", "Moderate", "Minor", "Unknown"}, false)
		warnings = appendEnumWarning(warnings, prefix+".certainty", info.Certainty, []string{"Observed", "Likely", "Possible", "Unlikely", "Unknown"}, false)
		for _, raw := range []struct {
			name  string
			value string
		}{
			{"effective", info.Effective},
			{"onset", info.Onset},
			{"expires", info.Expires},
		} {
			if raw.value != "" && parseCAPTime(raw.value).IsZero() {
				warnings = append(warnings, prefix+": invalid "+raw.name+" timestamp")
			}
		}
		for _, resource := range info.Resources {
			if resource.URI != "" {
				if parsed, err := url.Parse(resource.URI); err != nil || parsed.Scheme == "" && !strings.HasPrefix(resource.URI, "cid:") {
					warnings = append(warnings, prefix+": resource URI is not absolute")
				}
			}
		}
		warnings = append(warnings, validateECCC2026Info(info, prefix)...)
	}
	return warnings
}

func appendEnumWarning(warnings []string, name string, value string, allowed []string, fatal bool) []string {
	if strings.TrimSpace(value) == "" {
		return warnings
	}
	for _, item := range allowed {
		if strings.EqualFold(value, item) {
			return warnings
		}
	}
	prefix := ""
	if fatal {
		prefix = "fatal: "
	}
	return append(warnings, prefix+"invalid "+name+" "+value)
}

func hasFatalCAPWarning(warnings []string) bool {
	for _, warning := range warnings {
		if strings.HasPrefix(warning, "fatal:") {
			return true
		}
	}
	return false
}

func parseCAPTime(raw string) time.Time {
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw)); err == nil {
		return parsed
	}
	return time.Time{}
}

func normalizeInfo(info infoXML) AlertInfo {
	result := AlertInfo{
		Language:    clean(info.Language),
		Category:    cleanSlice(info.Category),
		Event:       clean(info.Event),
		Response:    cleanSlice(info.Response),
		Urgency:     clean(info.Urgency),
		Severity:    clean(info.Severity),
		Certainty:   clean(info.Certainty),
		Audience:    clean(info.Audience),
		Effective:   clean(info.Effective),
		Onset:       clean(info.Onset),
		Expires:     clean(info.Expires),
		SenderName:  clean(info.SenderName),
		Headline:    clean(info.Headline),
		Description: clean(info.Description),
		Instruction: clean(info.Instruction),
		Web:         clean(info.Web),
		EventCodes:  normalizePairs(info.EventCodes),
	}
	for _, area := range info.Areas {
		result.Areas = append(result.Areas, AlertArea{
			Description: clean(area.Description),
			Polygons:    cleanSlice(area.Polygons),
			Circles:     cleanSlice(area.Circles),
			Geocodes:    normalizePairs(area.Geocodes),
		})
	}
	result.Parameters = normalizePairs(info.Parameters)
	for _, resource := range info.Resources {
		result.Resources = append(result.Resources, Resource{
			Description: clean(resource.Description),
			MimeType:    clean(resource.MimeType),
			URI:         clean(resource.URI),
			DerefURI:    clean(resource.DerefURI),
		})
	}
	normalizeECCC2026Info(&result)
	return result
}

func normalizePairs(values []pairXML) []NameValue {
	pairs := make([]NameValue, 0, len(values))
	for _, value := range values {
		name := clean(value.Name)
		item := clean(value.Value)
		if name == "" && item == "" {
			continue
		}
		pairs = append(pairs, NameValue{Name: name, Value: item})
	}
	return pairs
}

func cleanSlice(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		cleaned := clean(value)
		if cleaned != "" {
			result = append(result, cleaned)
		}
	}
	return result
}

func clean(value string) string {
	return strings.TrimSpace(value)
}
