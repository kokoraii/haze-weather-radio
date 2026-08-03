package ivr

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadPromptConfigParsesMenusAndOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ivr.xml")
	if err := os.WriteFile(path, []byte(validPromptXML()), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := loadPromptConfig(path)
	if err != nil {
		t.Fatalf("loadPromptConfig: %v", err)
	}

	text := cfg.MenuLine("location_menu", "main", map[string]string{"location": "Saskatoon"})
	if text != "You have reached Saskatoon." {
		t.Fatalf("location prompt = %q", text)
	}
	entry, ok := cfg.Option("entry", "1")
	if !ok || entry.Action != "language" || entry.Language != "en-US" {
		t.Fatalf("entry option = %+v ok=%v", entry, ok)
	}
	weather := cfg.TTSForMenu("weather_product")
	if weather.Provider != "piper" || weather.CacheTTL != 5*time.Minute {
		t.Fatalf("weather policy = %+v", weather)
	}
	locationPolicy := cfg.TTSForMenu("location_menu")
	if locationPolicy.Provider != "sapi5" || locationPolicy.Volume != 80 {
		t.Fatalf("location policy did not inherit defaults: %+v", locationPolicy)
	}
}

func TestLoadPromptConfigRejectsMissingRequiredMenu(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ivr.xml")
	raw := strings.Replace(validPromptXML(), `<menu id="weather_product" provider="piper" cache_ttl="5m">
    <line key="unavailable">Weather is unavailable.</line>
  </menu>`, "", 1)
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadPromptConfig(path)
	if err == nil || !strings.Contains(err.Error(), `weather_product`) {
		t.Fatalf("expected missing weather_product error, got %v", err)
	}
}

func TestLoadPromptConfigRejectsMissingRequiredLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ivr.xml")
	raw := strings.Replace(validPromptXML(), `<line key="timeout">No entry.</line>`, "", 1)
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := loadPromptConfig(path)
	if err == nil || !strings.Contains(err.Error(), `timeout`) {
		t.Fatalf("expected missing timeout line error, got %v", err)
	}
}

func TestStaticPromptLinesExcludeDynamicPrompts(t *testing.T) {
	cfg := defaultPromptConfig()
	lines := cfg.StaticPromptLines()
	for _, line := range lines {
		if line.MenuID == "location_menu" && line.LineKey == "main" {
			t.Fatal("location menu should be generated on demand, not at startup")
		}
	}
}

func TestPromptLineLanguageFallbackOrder(t *testing.T) {
	raw := strings.Replace(validPromptXML(), `<line key="main">Search.</line>`, `<line key="main">Search default.</line>
    <line key="main" language="en-US">Search English.</line>
    <line key="main" language="fr">Recherche française.</line>`, 1)
	var parsed promptConfigXML
	if err := xml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatal(err)
	}
	cfg, err := normalizePromptConfig(parsed)
	if err != nil {
		t.Fatalf("normalize prompt config: %v", err)
	}
	for _, test := range []struct {
		language string
		want     string
	}{
		{language: "fr-CA", want: "Recherche française."},
		{language: "en-CA", want: "Search English."},
		{language: "de-DE", want: "Search default."},
	} {
		if got := cfg.MenuLine("location_search", "main", map[string]string{"lang": test.language}); got != test.want {
			t.Errorf("language %s selected %q, want %q", test.language, got, test.want)
		}
	}
}

func validPromptXML() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<ivr>
  <defaults provider="sapi5" language="en-US" volume="80" cache_ttl="24h">
    <line key="one_moment">One moment.</line>
  </defaults>
  <menu id="entry">
    <line key="main">Entry.</line>
    <line key="main_single_language">Entry single language.</line>
    <option digit="1" action="language" language="en-US" next="location_code"/>
  </menu>
  <menu id="language_select">
    <line key="main">Language.</line>
  </menu>
  <menu id="location_code">
    <line key="main">Enter code.</line>
  </menu>
  <menu id="location_number">
    <line key="main">Enter location number for {province}.</line>
    <line key="search_unavailable">Search unavailable.</line>
  </menu>
  <menu id="location_search">
    <line key="main">Search.</line>
    <line key="voice_unavailable">Voice unavailable.</line>
    <line key="no_match">No match.</line>
    <line key="more_detail">More detail.</line>
    <line key="fallback_numeric">Numeric fallback.</line>
  </menu>
  <menu id="location_menu">
    <line key="main">You have reached {location}.</line>
    <option digit="1" action="product" packages="current_conditions"/>
  </menu>
  <menu id="weather_product" provider="piper" cache_ttl="5m">
    <line key="unavailable">Weather is unavailable.</line>
  </menu>
  <menu id="broadcast_menu">
    <line key="main">Broadcast.</line>
  </menu>
  <menu id="geophysical_alert">
    <line key="main">Geophysical.</line>
  </menu>
  <menu id="operator">
    <line key="main">Operator unavailable.</line>
  </menu>
  <menu id="error">
    <line key="invalid_code">Invalid.</line>
    <line key="timeout">No entry.</line>
  </menu>
</ivr>`
}
