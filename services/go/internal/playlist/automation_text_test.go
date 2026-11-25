package playlist

import "testing"

func TestSelectAutomationLangs(t *testing.T) {
	langs := []automationLang{{Code: "en", Text: "hello"}, {Code: "fr-CA", Text: "bonjour"}, {Code: "es-MX", Text: "hola"}}
	cases := []struct {
		name  string
		feed  []feedLangXML
		codes []string
	}{
		{"feed order with base match", []feedLangXML{{Code: "fr-CA"}, {Code: "en-US"}}, []string{"fr-CA", "en"}},
		{"extra blocks only where enabled", []feedLangXML{{Code: "en-CA"}}, []string{"en"}},
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
