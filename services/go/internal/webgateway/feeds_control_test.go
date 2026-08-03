package webgateway

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFeedsControlRoundTripPreservesPracticalFields(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, `feeds_file: managed/configs/feeds.xml
`)

	payload, err := saveFeedsControlPayload(configPath, map[string]any{
		"feeds": []any{
			map[string]any{
				"id":                               "sk-0001",
				"enabled":                          true,
				"timezone":                         "America/Regina",
				"routine":                          true,
				"same":                             true,
				"same_originator":                  "EAS",
				"same_attention_tone":              "EAS",
				"cap_cp_enabled":                   true,
				"cap_cp_use_feed_locations":        true,
				"cap_cp_blocklist":                 "severity:Minor\ncertainty:Unknown",
				"nws_cap_enabled":                  false,
				"nws_cap_use_feed_locations":       false,
				"nws_cap_allowlist":                "severity:Extreme",
				"languages":                        "en-CA:0\nfr-CA:10",
				"description_text":                 "Saskatoon weather radio.",
				"description_suffix":               "Unofficial service.",
				"coverage_regions":                 "065100|eccc|sk-40\n065500|eccc|sk-37",
				"observation_locations":            "sk-40|eccc|||52.1|-106.6|urn:haze:location:observation",
				"aviation_report_locations":        "CYXE|eccc|Saskatoon Airport||||urn:haze:location:airport",
				"air_quality_locations":            "HAHJJ|eccc||",
				"climate_locations":                "4057165|eccc|Saskatoon|4057120",
				"hydrometric_locations":            "05HG001|eccc|South Saskatchewan River at Saskatoon|",
				"hydrometric_upstream_locations":   "05JG006|eccc|||||urn:haze:location:upstream",
				"hydrometric_downstream_locations": "05KD007|eccc|||||urn:haze:location:downstream",
				"marine_forecast_subregions":       "m0000109|eccc|||||urn:haze:location:marine-zone",
				"marine_condition_locations":       "9401177|eccc|||||urn:haze:location:buoy",
				"site_name":                        "Saskatoon",
				"callsign":                         "XLF322",
				"relationship":                     "primary",
				"frequency_mhz":                    "162.550",
			},
		},
	})
	if err != nil {
		t.Fatalf("saveFeedsControlPayload: %v", err)
	}
	feeds, ok := payload["feeds"].([]map[string]any)
	if !ok || len(feeds) != 1 {
		t.Fatalf("feeds payload = %#v", payload["feeds"])
	}
	if feeds[0]["same_originator"] != "EAS" || feeds[0]["site_name"] != "Saskatoon" {
		t.Fatalf("feed payload = %#v", feeds[0])
	}

	raw, err := os.ReadFile(filepath.Join(dir, "managed", "configs", "feeds.xml"))
	if err != nil {
		t.Fatalf("read feeds.xml: %v", err)
	}
	text := string(raw)
	for _, needle := range []string{
		`<feed id="sk-0001" enabled="true" timezone="America/Regina">`,
		`<playout routine="true" same="true" same_originator="EAS" same_attention_tone="EAS"></playout>`,
		`<severity>Minor</severity>`,
		`<region id="065100" source="eccc" derive_forecast="sk-40"></region>`,
		`<location id="4057165" source="eccc" name_override="Saskatoon" normal_id="4057120"></location>`,
		`<location id="sk-40" source="eccc" latitude="52.1" longitude="-106.6" canonical_id="urn:haze:location:observation"></location>`,
		`<location id="CYXE" source="eccc" name_override="Saskatoon Airport" canonical_id="urn:haze:location:airport"></location>`,
		`<location id="05JG006" source="eccc" canonical_id="urn:haze:location:upstream"></location>`,
		`<subregion id="m0000109" source="eccc" canonical_id="urn:haze:location:marine-zone"></subregion>`,
		`<location id="9401177" source="eccc" canonical_id="urn:haze:location:buoy"></location>`,
		`<frequency_mhz>162.550</frequency_mhz>`,
	} {
		if !strings.Contains(text, needle) {
			t.Fatalf("missing %q in XML:\n%s", needle, text)
		}
	}
}

func TestFeedsControlRejectsDuplicateFeedIDs(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, `feeds_file: managed/configs/feeds.xml
`)
	_, err := saveFeedsControlPayload(configPath, map[string]any{
		"feeds": []any{
			map[string]any{"id": "sk-0001"},
			map[string]any{"id": "sk-0001"},
		},
	})
	if err == nil {
		t.Fatal("expected duplicate feed id error")
	}
}
