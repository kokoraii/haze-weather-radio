package productrender

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCombinedProductsBuildsProfilesAndText(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "products.xml")
	raw := `<?xml version="1.0" encoding="UTF-8"?>
<ProductText version="1.1">
  <defaults>
    <enabled>true</enabled>
    <reader_id>00</reader_id>
  </defaults>
  <product id="forecast" enabled="true">
    <lang iso="en-US" readerid="01">
      <text key="opener">Forecast for {site}.</text>
      <region>
        <shortterm>
          <text key="today">For today. {fc_text}</text>
        </shortterm>
        <extended>
          <text key="opener">The extended outlook.</text>
          <text key="period">For {period}. {fc_text}</text>
        </extended>
      </region>
    </lang>
    <lang iso="fr-CA" readerid="02">
      <text key="opener">Previsions pour {site}.</text>
    </lang>
  </product>
  <product id="current_conditions" enabled="true">
    <lang iso="en" readerid="03">
      <audio key="opener">./audio/vocal/current.wav</audio>
      <primary>
        <placeholder>The report at {location} was not available.</placeholder>
        <text>At {location},</text>
        <text>The temperature was {ctemp} degrees.</text>
      </primary>
    </lang>
  </product>
  <product id="alerts" enabled="false" />
</ProductText>`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	packages, productText, productAudio, err := loadCombinedProducts(path)
	if err != nil {
		t.Fatal(err)
	}
	if !packages["forecast"].Enabled {
		t.Fatalf("forecast should be enabled")
	}
	if packages["forecast"].ReaderID != "01" {
		t.Fatalf("forecast reader = %q, want 01", packages["forecast"].ReaderID)
	}
	if packages["forecast"].ReaderByLang["fr-ca"] != "02" {
		t.Fatalf("forecast fr reader = %q, want 02", packages["forecast"].ReaderByLang["fr-ca"])
	}
	if packages["current_conditions"].ReaderID != "03" {
		t.Fatalf("current_conditions reader = %q, want 03", packages["current_conditions"].ReaderID)
	}
	if packages["alerts"].Enabled {
		t.Fatalf("alerts should be disabled")
	}
	if packages["alerts"].ReaderID != "00" {
		t.Fatalf("alerts reader = %q, want default 00", packages["alerts"].ReaderID)
	}
	if got := productText["forecast"]["opener"]["en-us"]; got != "Forecast for {site}." {
		t.Fatalf("en opener = %q", got)
	}
	if got := productText["forecast"]["opener"]["fr-ca"]; got != "Previsions pour {site}." {
		t.Fatalf("fr opener = %q", got)
	}
	if got := productText["forecast"]["today"]["en-us"]; got != "For today. {fc_text}" {
		t.Fatalf("forecast today = %q", got)
	}
	if got := productText["forecast"]["shortterm.today"]["en-us"]; got != "For today. {fc_text}" {
		t.Fatalf("forecast shortterm today = %q", got)
	}
	if got := productText["forecast"]["opener"]["en-us"]; got != "Forecast for {site}." {
		t.Fatalf("nested extended opener should not replace main opener, got %q", got)
	}
	if got := productText["forecast"]["extended.opener"]["en-us"]; got != "The extended outlook." {
		t.Fatalf("forecast extended opener = %q", got)
	}
	if got := productText["forecast"]["extended.period"]["en-us"]; got != "For {period}. {fc_text}" {
		t.Fatalf("forecast extended period = %q", got)
	}
	if got := productText["current_conditions"]["primary.placeholder"]["en"]; got != "The report at {location} was not available." {
		t.Fatalf("primary placeholder = %q", got)
	}
	if got := productText["current_conditions"]["primary.text.1"]["en"]; got != "At {location}," {
		t.Fatalf("primary text 1 = %q", got)
	}
	if got := productText["current_conditions"]["primary.text.2"]["en"]; got != "The temperature was {ctemp} degrees." {
		t.Fatalf("primary text 2 = %q", got)
	}
	if got := productAudio["current_conditions"]["opener"]["en"]; got != "./audio/vocal/current.wav" {
		t.Fatalf("current conditions opener audio = %q", got)
	}
}

func TestFeedRenderLanguageRequiresConfiguredBilingualFeed(t *testing.T) {
	englishOnly := feedXML{ID: "mono"}
	englishOnly.Languages.Langs = []feedLangXML{{Code: "en-US"}}

	if got := feedRenderLanguage(englishOnly, "fr-CA"); got != "en-US" {
		t.Fatalf("single language feed rendered %q, want en-US", got)
	}

	bilingual := feedXML{ID: "bi"}
	bilingual.Languages.Langs = []feedLangXML{{Code: "en-US"}, {Code: "fr-CA", Interval: "1"}}

	if got := feedRenderLanguage(bilingual, "fr"); got != "fr-CA" {
		t.Fatalf("bilingual feed rendered %q, want fr-CA", got)
	}
	if got := feedRenderLanguage(bilingual, "es"); got != "en-US" {
		t.Fatalf("unconfigured requested language rendered %q, want en-US", got)
	}
}

func TestLoadFeedsReadsSourceScopedPolygonFirstCoverageMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "feeds.xml")
	raw := `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="cwxr-test" enabled="true">
    <alerts>
      <cap_cp enabled="true"><filter use_feed_locations="true" coverage_mode="polygon_first"/></cap_cp>
      <nws_cap enabled="true"><filter use_feed_locations="true"/></nws_cap>
    </alerts>
  </feed>
</feeds>`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	feeds, err := loadFeeds(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(feeds) != 1 || feeds[0].Alerts.CapCP.Filter.CoverageMode != capCoverageModePolygonFirst {
		t.Fatalf("CAP-CP coverage mode = %#v", feeds)
	}
	if feeds[0].Alerts.NWSCAP.Filter.CoverageMode != "" {
		t.Fatalf("NWS CAP coverage mode = %q, want unset", feeds[0].Alerts.NWSCAP.Filter.CoverageMode)
	}
}

func TestRendererUsesRequestedBilingualProductLanguage(t *testing.T) {
	feed := feedXML{ID: "cwxr-test", EnabledRaw: "true"}
	feed.Languages.Langs = []feedLangXML{{Code: "en-US"}, {Code: "fr-CA", Interval: "1"}}

	cfg := loadedConfig{
		Feeds: []feedXML{feed},
		Packages: map[string]packageProfile{
			"station_id": {Enabled: true, ReaderID: "00", ReaderByLang: map[string]string{"fr-ca": "02"}},
		},
		ProductText: map[string]map[string]map[string]string{
			"station_id": {
				"text.1": {
					"en":    "You are listening to {on_air_name}.",
					"fr-ca": "Vous écoutez {on_air_name}.",
				},
			},
		},
	}
	cfg.Root.Operator.OnAirName = "Haze"

	product, err := newRenderer(cfg).Render(renderRequest{FeedID: feed.ID, PackageID: "station_id", Language: "fr-CA"})
	if err != nil {
		t.Fatal(err)
	}
	if product.Language != "fr-CA" {
		t.Fatalf("language = %q, want fr-CA", product.Language)
	}
	if product.ReaderID != "02" {
		t.Fatalf("reader = %q, want 02", product.ReaderID)
	}
	if product.Text != "Vous écoutez Haze." {
		t.Fatalf("text = %q", product.Text)
	}
}

func TestPackageLanguageAllowListCanForcePrimaryLanguage(t *testing.T) {
	feed := feedXML{ID: "cwxr-test", EnabledRaw: "true"}
	feed.Languages.Langs = []feedLangXML{{Code: "en-US"}, {Code: "fr-CA", Interval: "1"}}

	cfg := loadedConfig{
		Feeds: []feedXML{feed},
		Packages: map[string]packageProfile{
			"alerts": {Enabled: true, ReaderID: "00", Languages: []string{"en"}},
		},
		ProductText: map[string]map[string]map[string]string{},
	}

	product := productBase(cfg, feed, "alerts", "fr-CA", false)
	if product.Language != "en" {
		t.Fatalf("language = %q, want en", product.Language)
	}
}

func TestFrequencyMHzSpeechIncludesUnit(t *testing.T) {
	if got := frequencyMHzSpeech("162.550"); got != "162.550 megahertz" {
		t.Fatalf("frequency text = %q", got)
	}
	if got := frequencyMHzSpeech("162.550 megahertz"); got != "162.550 megahertz" {
		t.Fatalf("frequency text with unit = %q", got)
	}
}
