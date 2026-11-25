package playlist

import (
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

// applyAutomationContent resolves an automation request's text for this feed.
// It keeps only the language blocks the feed has enabled (in the feed's
// language order), expands variables in each, and stores the first block as
// alert_text and the rest as alert_extra_langs for renderAlertTTSAsPCM.
func (p *feedPlanner) applyAutomationContent(data map[string]any) {
	vars := p.automationFeedVars(data)
	langs := selectAutomationLangs(feedLanguages(p.feed), automationLangsFromData(data))
	if len(langs) == 0 {
		data["alert_text"] = alertvars.Expand(firstText(nil, data, "alert_text"), vars)
		return
	}
	for index := range langs {
		langs[index].Text = alertvars.Expand(langs[index].Text, vars)
	}
	primary := langs[0]
	data["alert_text"] = primary.Text
	data["alert_language"] = primary.Code
	// Empty lets TTS choose a voice for the block's language.
	data["reader_id"] = primary.ReaderID
	data["alert_extra_langs"] = langs[1:]
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
// feed's language order. A feed that carries none of them still airs the first
// block, so an automation never goes silent on a feed.
func selectAutomationLangs(feedLangs []feedLangXML, langs []automationLang) []automationLang {
	if len(langs) == 0 {
		return nil
	}
	selected := []automationLang{}
	used := map[int]bool{}
	for _, feedLang := range feedLangs {
		if index := matchAutomationLang(feedLang.Code, langs, used); index >= 0 {
			used[index] = true
			selected = append(selected, langs[index])
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
