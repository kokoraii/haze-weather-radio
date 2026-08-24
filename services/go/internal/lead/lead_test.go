package lead

import (
	"strings"
	"testing"
)

func TestParseScaffoldStatementMatchesCAPParameter(t *testing.T) {
	raw := []byte(`<?xml version="1.0" encoding="utf-8"?>
<leads>
  <lead enabled="true">
    <name>NPAS preroll</name>
    <conditions>
      <if key="layer:SOREM:1.0:Broadcast_Immediately" equals="no" matchcase="false" matchwhole="false" useregex="true" />
      <and />
    </conditions>
    <audio><lead_in>./audio/NPAS_Preroll.wav</lead_in><lead_out></lead_out></audio>
  </lead>
</leads>`)
	document, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(document.Statements) != 1 {
		t.Fatalf("statements = %d", len(document.Statements))
	}
	statement := document.Statements[0]
	if statement.LeadIn != "audio/NPAS_Preroll.wav" {
		t.Fatalf("lead_in = %q", statement.LeadIn)
	}
	context := Context{Values: map[string][]string{
		"layer:SOREM:1.0:Broadcast_Immediately": {"no"},
	}}
	if selected, ok := document.Select(context); !ok || selected.Name != "NPAS preroll" {
		t.Fatalf("selected = %#v, %v", selected, ok)
	}
	context.Values["layer:SOREM:1.0:Broadcast_Immediately"] = []string{"yes"}
	if _, ok := document.Select(context); ok {
		t.Fatal("lead matched the opposite Broadcast_Immediately value")
	}
}

func TestSelectUsesFirstMatchingStatementAndLocation(t *testing.T) {
	document := Document{Statements: []Statement{
		{
			Enabled: true,
			Name:    "Area lead",
			Conditions: []Condition{{
				Type: "if", Location: "065100", Equals: "065100",
			}},
			LeadIn: "audio/area.wav",
		},
		{
			Enabled: true,
			Name:    "Fallback",
			LeadIn:  "audio/fallback.wav",
		},
	}}
	normalized, err := Normalize(document)
	if err != nil {
		t.Fatal(err)
	}
	selected, ok := normalized.Select(Context{Locations: []string{"065100", "065200"}})
	if !ok || selected.Name != "Area lead" {
		t.Fatalf("selected = %#v, %v", selected, ok)
	}
	selected, ok = normalized.Select(Context{Locations: []string{"065200"}})
	if !ok || selected.Name != "Fallback" {
		t.Fatalf("fallback = %#v, %v", selected, ok)
	}
}

func TestNormalizeRejectsUnsafeAndInvalidLeadAudio(t *testing.T) {
	for _, raw := range []string{"../.env", "/audio/lead.wav", "audio/lead.txt", "C:/audio/lead.wav"} {
		if _, err := NormalizeAudioPath(raw); err == nil {
			t.Fatalf("NormalizeAudioPath(%q) accepted an unsafe path", raw)
		}
	}
	if _, err := Normalize(Document{Statements: []Statement{{Enabled: true, Name: "Missing audio"}}}); err == nil {
		t.Fatal("enabled statement without audio was accepted")
	}
}

func TestEncodeRoundTripsConditions(t *testing.T) {
	document := Document{Statements: []Statement{{
		Enabled: true,
		Name:    "Priority lead",
		Conditions: []Condition{
			{Type: "if", Key: "severity", Equals: "Severe"},
			{Type: "or"},
			{Type: "if", Location: "065100", Includes: "065"},
		},
		LeadIn:  "audio/lead.wav",
		LeadOut: "audio/out.wav",
	}}}
	raw, err := Encode(document)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "<lead") || !strings.Contains(string(raw), "<or") {
		t.Fatalf("encoded XML did not retain lead conditions: %s", raw)
	}
	parsed, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Statements) != 1 || len(parsed.Statements[0].Conditions) != 3 {
		t.Fatalf("round trip = %#v", parsed)
	}
}
