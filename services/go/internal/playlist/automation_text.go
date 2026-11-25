package playlist

import (
	"maps"
	"path/filepath"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alerttext"
	"github.com/meowraii/haze-weather-radio/services/go/internal/alertvars"
)

// automationLang is one <lang> block of an automation, as the scheduler sends
// it in the alert_langs field of cap.alert.broadcast.requested.
type automationLang struct {
	Code     string
	Text     string
	ReaderID string
}

// languageReader asks haze-tts for the first male reader of the request's
// language. An empty reader would skip reader selection and speak in the
// engine's default voice (Microsoft David on SAPI).
const languageReader = "male"

// automationContent resolves an automation request's text for this feed and
// returns a new map; data is shared across feeds and is not modified. It must
// run before the request is normalized into an alert packet, because the
// packet's speech text is taken from alert_text.
//
// It keeps only the language blocks the feed has enabled (in the feed's
// language order), expands variables in each, and stores the first block as
// alert_text and the rest as alert_extra_langs for renderAlertTTSAsPCM.
func (p *feedPlanner) automationContent(data map[string]any) map[string]any {
	out := maps.Clone(data)
	vars := p.automationFeedVars(data)
	langs := selectAutomationLangs(feedLanguages(p.feed), automationLangsFromData(data))
	if len(langs) == 0 {
		out["alert_text"] = alertvars.Expand(firstText(nil, data, "alert_text"), vars)
		return out
	}
	for index := range langs {
		langs[index].Text = alertvars.Expand(langs[index].Text, vars)
		if langs[index].ReaderID == "" {
			langs[index].ReaderID = languageReader
		}
	}
	primary := langs[0]
	out["alert_text"] = primary.Text
	out["alert_language"] = primary.Code
	out["reader_id"] = primary.ReaderID
	out["alert_extra_langs"] = langs[1:]
	return out
}

func automationLangsFromData(data map[string]any) []automationLang {
	out := []automationLang{}
	for _, item := range sliceOfAny(data["alert_langs"]) {
		entry, _ := item.(map[string]any)
		lang := automationLang{
			Code:     strings.TrimSpace(firstText(nil, entry, "code")),
			Text:     strings.TrimSpace(firstText(nil, entry, "text")),
			ReaderID: strings.TrimSpace(firstText(nil, entry, "reader_id")),
		}
		if lang.Code != "" && lang.Text != "" {
			out = append(out, lang)
		}
	}
	return out
}

// selectAutomationLangs returns the automation blocks the feed carries, in the
// feed's language order, each tagged with the feed's own language code (en
// becomes en-US) so TTS voice and dictionary lookups match exactly. A feed
// that carries none of them still airs the first block, so an automation never
// goes silent on a feed.
func selectAutomationLangs(feedLangs []feedLangXML, langs []automationLang) []automationLang {
	if len(langs) == 0 {
		return nil
	}
	selected := []automationLang{}
	used := map[int]bool{}
	for _, feedLang := range feedLangs {
		if index := matchAutomationLang(feedLang.Code, langs, used); index >= 0 {
			used[index] = true
			lang := langs[index]
			lang.Code = feedLang.Code
			selected = append(selected, lang)
		}
	}
	if len(selected) == 0 {
		return langs[:1]
	}
	return selected
}

// matchAutomationLang prefers an exact code match (en-US), then a base
// language match (en matches en-US and en-CA).
func matchAutomationLang(feedCode string, langs []automationLang, used map[int]bool) int {
	want := normalizeLangKey(feedCode)
	for index, lang := range langs {
		if !used[index] && normalizeLangKey(lang.Code) == want {
			return index
		}
	}
	for index, lang := range langs {
		if !used[index] && baseLang(lang.Code) == baseLang(want) {
			return index
		}
	}
	return -1
}

func baseLang(code string) string {
	code = normalizeLangKey(code)
	if index := strings.Index(code, "-"); index > 0 {
		return code[:index]
	}
	return code
}

// automationFeedVars gathers this feed's values for alertvars placeholders.
func (p *feedPlanner) automationFeedVars(data map[string]any) alertvars.Feed {
	at := dataTimeValue(data, "scheduled_for")
	if at.IsZero() {
		at = time.Now()
	}
	event := strings.TrimSpace(firstText(nil, data, "same_event", "event"))
	eventName := alerttext.EventName(filepath.Join(p.cfg.BaseDir, "config.yaml"), event)
	if event == "" {
		eventName = firstText(nil, data, "title")
	}
	primary := stationTransmitter(p.feed)
	return alertvars.Feed{
		ID:              p.feed.ID,
		EventName:       eventName,
		PrimaryCoverage: p.primaryCoverageName(primary),
		NetworkName:     fallbackText(spokenNetworkName(primary.Network), spokenText(p.cfg.Root.Operator.OnAirName)),
		OperatorName:    spokenText(p.cfg.Root.Operator.OperatorName),
		PhoneNumber:     spokenText(p.cfg.Root.Operator.PhoneNumber),
		Transmitters:    automationTransmitterCalls(p.feed, primary),
		At:              at.In(feedLocation(p.feed)),
	}
}

// primaryCoverageName is the name of the feed's first coverage region, falling
// back to the primary transmitter's site.
func (p *feedPlanner) primaryCoverageName(primary transmitterXML) string {
	if p.cfg.FeedConfig != nil {
		if feed, ok := p.cfg.FeedConfig.Feed(p.feed.ID); ok {
			for _, entry := range feed.Entries {
				if name := strings.TrimSpace(entry.Name); name != "" {
					return name
				}
			}
		}
	}
	return strings.TrimSpace(primary.SiteName)
}

// automationTransmitterCalls lists callsigns with the primary transmitter at
// index 0 and the rest in feed order.
func automationTransmitterCalls(feed feedXML, primary transmitterXML) []string {
	calls := []string{}
	if call := strings.TrimSpace(primary.Callsign); call != "" {
		calls = append(calls, call)
	}
	for _, transmitter := range transmitterList(feed) {
		call := strings.TrimSpace(transmitter.Callsign)
		if call == "" || transmitter.isRelationship("replaces") || (len(calls) > 0 && call == calls[0]) {
			continue
		}
		calls = append(calls, call)
	}
	return calls
}

// sliceOfAny reads a JSON array field; anything else is treated as empty.
func sliceOfAny(value any) []any {
	items, _ := value.([]any)
	return items
}
