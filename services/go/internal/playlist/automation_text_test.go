package playlist

import "testing"

func TestSelectAutomationLangs(t *testing.T) {
	langs := []automationLang{{Code: "en", Text: "hello"}, {Code: "fr-CA", Text: "bonjour"}, {Code: "es-MX", Text: "hola"}}
	cases := []struct {
		name  string
		feed  []feedLangXML
		codes []string
	}{
		{"feed order with feed's own codes", []feedLangXML{{Code: "fr-CA"}, {Code: "en-US"}}, []string{"fr-CA", "en-US"}},
		{"extra blocks only where enabled", []feedLangXML{{Code: "en-CA"}}, []string{"en-CA"}},
		{"no match falls back to default block", []feedLangXML{{Code: "de-DE"}}, []string{"en"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := selectAutomationLangs(tc.feed, langs)
			if len(got) != len(tc.codes) {
				t.Fatalf("got %d langs, want %v", len(got), tc.codes)
			}
			for i, code := range tc.codes {
				if got[i].Code != code {
					t.Fatalf("lang %d = %s, want %s", i, got[i].Code, code)
				}
			}
		})
	}
}

// The expanded text must be what gets spoken: the alert packet built during
// normalization takes its speech text from alert_text.
func TestAutomationContentSurvivesPacketNormalization(t *testing.T) {
	planner := &feedPlanner{feed: feedXML{ID: "cwxr-sk01", Timezone: "America/Regina"}}
	planner.feed.Languages.Langs = []feedLangXML{{Code: "en-US"}}
	request := map[string]any{
		"alert_id":      "automation-x",
		"automation_id": "x",
		"event":         "RWT",
		"same_event":    "RWT",
		"message_type":  "Alert",
		"include_same":  true,
		"alert_text":    "Test on {feed-id}.",
		"alert_langs":   []any{map[string]any{"code": "en", "text": "Test on {feed-id}."}},
	}
	data, _ := normalizePriorityAlertRequest(planner.automationContent(request))
	if got := planner.alertTextFromData(data); got != "Test on cwxr-sk01." {
		t.Fatalf("spoken text = %q", got)
	}
	if data["reader_id"] != languageReader || data["alert_language"] != "en-US" {
		t.Fatalf("reader = %v, language = %v", data["reader_id"], data["alert_language"])
	}
	if request["alert_text"] != "Test on {feed-id}." {
		t.Fatal("shared request data was modified")
	}
}
