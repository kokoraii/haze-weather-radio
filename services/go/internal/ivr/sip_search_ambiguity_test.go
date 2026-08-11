package ivr

import (
	"strings"
	"testing"
)

func TestAdvanceLocationSearchChoice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		current    int
		count      int
		digit      string
		wantIndex  int
		wantAction locationSearchChoiceAction
	}{
		{name: "next", current: 0, count: 3, digit: "3", wantIndex: 1, wantAction: locationSearchChoiceWait},
		{name: "next wraps", current: 2, count: 3, digit: "3", wantIndex: 0, wantAction: locationSearchChoiceWait},
		{name: "accept current", current: 1, count: 3, digit: "2", wantIndex: 1, wantAction: locationSearchChoiceAccept},
		{name: "back", current: 2, count: 3, digit: "1", wantIndex: 2, wantAction: locationSearchChoiceBack},
		{name: "invalid digit stays", current: 1, count: 3, digit: "9", wantIndex: 1, wantAction: locationSearchChoiceWait},
		{name: "empty choices exits", current: 0, count: 0, digit: "2", wantIndex: 0, wantAction: locationSearchChoiceBack},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			gotIndex, gotAction := advanceLocationSearchChoice(test.current, test.count, test.digit)
			if gotIndex != test.wantIndex || gotAction != test.wantAction {
				t.Fatalf("advanceLocationSearchChoice(%d, %d, %q) = (%d, %d), want (%d, %d)", test.current, test.count, test.digit, gotIndex, gotAction, test.wantIndex, test.wantAction)
			}
		})
	}
}

func TestLocalizedAmbiguityPrompt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		language string
		match    locationSearchMatch
		contains []string
	}{
		{
			name:     "English",
			language: "en-CA",
			match: locationSearchMatch{
				DisplayName: "Saskatoon",
				Target:      locationSearchTarget{Location: ResolvedLocation{Province: "SK"}},
			},
			contains: []string{"Saskatoon, Saskatchewan", "Press 2 to choose", "3 for the next match", "1 to go back"},
		},
		{
			name:     "French",
			language: "fr-CA",
			match: locationSearchMatch{
				DisplayName: "Moncton",
				Target:      locationSearchTarget{Location: ResolvedLocation{Province: "NB"}},
			},
			contains: []string{"Moncton, Nouveau-Brunswick", "Appuyez sur 2", "sur 3 pour le lieu suivant", "sur 1 pour revenir en arrière"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := localizedAmbiguityPrompt(test.language, test.match)
			for _, want := range test.contains {
				if !strings.Contains(got, want) {
					t.Fatalf("localizedAmbiguityPrompt() = %q, want substring %q", got, want)
				}
			}
		})
	}
}

func TestLocalizedChoicePromptReadsOnlyCurrentCandidate(t *testing.T) {
	t.Parallel()

	matches := []locationSearchMatch{
		{DisplayName: "Saskatoon", Target: locationSearchTarget{Location: ResolvedLocation{Province: "SK"}}},
		{DisplayName: "Saskatoon Mountain", Target: locationSearchTarget{Location: ResolvedLocation{Province: "BC"}}},
	}
	got := localizedChoicePrompt("en-CA", matches)
	if !strings.Contains(got, "Saskatoon, Saskatchewan") {
		t.Fatalf("localizedChoicePrompt() = %q, want current candidate", got)
	}
	if strings.Contains(got, "Saskatoon Mountain") {
		t.Fatalf("localizedChoicePrompt() read more than the current candidate: %q", got)
	}
}

func TestRegionalLocationSearchContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		selector     string
		wantRegion   string
		wantExplicit bool
	}{
		{name: "regional line", selector: "SK", wantRegion: "SK", wantExplicit: true},
		{name: "nationwide line", selector: "", wantRegion: "", wantExplicit: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := regionalLocationSearchContext("fr-CA", test.selector)
			if got.Region != test.wantRegion || got.ExplicitRegion != test.wantExplicit {
				t.Fatalf("regionalLocationSearchContext(%q) = region %q explicit %t, want %q %t", test.selector, got.Region, got.ExplicitRegion, test.wantRegion, test.wantExplicit)
			}
			if got.Language != "fr-CA" {
				t.Fatalf("regionalLocationSearchContext(%q) language = %q, want fr-CA", test.selector, got.Language)
			}
		})
	}
}

func TestSIPLocationSearchContextUsesCallerOnlyAsSoftHint(t *testing.T) {
	tests := []struct {
		name           string
		selector       string
		callerProvince string
		enabled        bool
		wantRegion     string
		wantExplicit   bool
	}{
		{name: "nationwide caller hint", callerProvince: "ON", enabled: true, wantRegion: "ON"},
		{name: "disabled caller hint", callerProvince: "ON", enabled: false},
		{name: "regional line wins", selector: "SK", callerProvince: "ON", enabled: true, wantRegion: "SK", wantExplicit: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hint := sipLocationSearchContext("en-CA", test.selector, test.callerProvince, test.enabled)
			if hint.Region != test.wantRegion || hint.ExplicitRegion != test.wantExplicit {
				t.Fatalf("context = %#v, want region %q explicit=%v", hint, test.wantRegion, test.wantExplicit)
			}
		})
	}
}
