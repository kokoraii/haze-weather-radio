package alerttext

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
)

func TestBuildSAMETranslationMatchesBannerStyleLead(t *testing.T) {
	text := BuildSAMETranslation(SAMERequest{
		Originator: "WXR",
		Event:      "RWT",
		EventName:  "Required Weekly Test",
		AreaNames:  []string{"Saskatoon"},
		Callsign:   "XLF322",
		SentAt:     time.Date(2026, 6, 17, 1, 0, 0, 0, time.UTC),
		ExpiresAt:  time.Date(2026, 6, 17, 1, 15, 0, 0, time.UTC),
		MimicENDEC: "SAGE",
	})

	if !strings.Contains(text, "Environment Canada has issued a Required Weekly Test for the following areas: Saskatoon") {
		t.Fatalf("text = %q", text)
	}
	if !strings.Contains(text, "(XLF322)") {
		t.Fatalf("text = %q", text)
	}
}

func TestBuildSAMETranslationUsesWeatherServiceForWXR(t *testing.T) {
	text := BuildSAMETranslation(SAMERequest{
		Originator:     "WXR",
		Event:          "SVR",
		EventName:      "Severe Thunderstorm Warning",
		AreaNames:      []string{"Talladega, AL"},
		Callsign:       "NWS",
		WeatherService: "The National Weather Service",
		SentAt:         time.Date(2026, 6, 17, 1, 0, 0, 0, time.UTC),
		ExpiresAt:      time.Date(2026, 6, 17, 1, 30, 0, 0, time.UTC),
		MimicENDEC:     "SAGE",
	})

	if !strings.Contains(text, "The National Weather Service has issued a Severe Thunderstorm Warning") {
		t.Fatalf("text = %q", text)
	}
}

func TestBuildSAMETranslationUsesNaturalAreaAndTimingLead(t *testing.T) {
	text := BuildSAMETranslation(SAMERequest{
		Originator:     "WXR",
		Event:          "SVR",
		EventName:      "Severe Thunderstorm Warning",
		AreaNames:      []string{"Escambia, AL", "Elmore, AL", "DeKalb, AL", "Dallas, AL"},
		Callsign:       "meowraii",
		WeatherService: "The National Weather Service",
		BeginsAt:       time.Date(2026, 6, 22, 12, 38, 0, 0, time.UTC),
		ExpiresAt:      time.Date(2026, 6, 22, 12, 39, 0, 0, time.UTC),
		MimicENDEC:     "SAGE",
	})

	want := "The National Weather Service has issued a Severe Thunderstorm Warning for the following areas: Escambia, AL; Elmore, AL; DeKalb, AL; and Dallas, AL. Beginning at 12:38 PM and ending at 12:39 PM on June 22nd, 2026. (meowraii)."
	if text != want {
		t.Fatalf("text = %q, want %q", text, want)
	}
}

func TestBuildSAMETranslationUsesCAPOriginatorAndEventNames(t *testing.T) {
	text := BuildSAMETranslation(SAMERequest{
		Originator:     "WXR",
		OriginatorName: "Environment Canada",
		Event:          "SVR",
		EventName:      "Yellow Warning - Severe Thunderstorm",
		AreaNames:      []string{"City of Saskatoon"},
		Callsign:       "XLF322",
		SentAt:         time.Date(2026, 6, 17, 1, 0, 0, 0, time.UTC),
		ExpiresAt:      time.Date(2026, 6, 17, 1, 30, 0, 0, time.UTC),
		MimicENDEC:     "SAGE",
	})

	if !strings.Contains(text, "Environment Canada has issued a Yellow Warning - Severe Thunderstorm") {
		t.Fatalf("text = %q", text)
	}
}

func TestLoadEventAndAreaLabels(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "sameMapping.json"), `{"eas":{"SVR":"Severe Thunderstorm Warning"}}`)
	mustWrite(t, filepath.Join(dir, "managed", "csv", "FORECAST_LOCATIONS.csv"), "skip\nCODE,NAME,NOM\n065522,Saskatoon,Saskatoon\n")
	configPath := filepath.Join(dir, "config.yaml")

	if name := EventName(configPath, "SVR"); name != "Severe Thunderstorm Warning" {
		t.Fatalf("event name = %q", name)
	}
	areas := ResolveAreaNames(configPath, nil, []string{"065522"})
	if len(areas) != 1 || areas[0] != "Saskatoon" {
		t.Fatalf("areas = %#v", areas)
	}
}

func TestResolveAreaNamesUsesNWSFIPSNamesAndUnknownFallback(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "csv", "NWS_ZONE_COUNTY_CORRELATION.csv"), `STATE|ZONE_CODE|CWA_ID|ZONE_NAME|STATE+ZONE|COUNTY_NAME|FIPS/SAME|TIMEZONE|FE_AREA|LAT|LON
AL|001|BMX|Autauga|ALC001|Autauga|001001|C|se|32.5364|-86.6445
AL|003|MOB|Baldwin|ALC003|Baldwin|001003|C|se|30.6592|-87.7461
`)
	configPath := filepath.Join(dir, "config.yaml")

	areas := ResolveAreaNames(configPath, nil, []string{"001001", "001003", "012011"})
	want := []string{"Autauga, AL", "Baldwin, AL", "Unknown Location (012011)"}
	if strings.Join(areas, "|") != strings.Join(want, "|") {
		t.Fatalf("areas = %#v, want %#v", areas, want)
	}
}

func TestResolveAreaNamesUsesParentLabelForPartialSAMECode(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "csv", "CLC_Base_Zone.csv"), "CLC,NAME\n065435,Saskatoon area\n")
	configPath := filepath.Join(dir, "config.yaml")

	areas := ResolveAreaNames(configPath, nil, []string{"165435", "965435"})
	want := []string{"northwest portion of Saskatoon area", "southeast portion of Saskatoon area"}
	if strings.Join(areas, "|") != strings.Join(want, "|") {
		t.Fatalf("areas = %#v, want %#v", areas, want)
	}
}

func TestResolveAreaNamesUsesNWSMarineNames(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "managed", "csv", "NWS_MARINE_ZONES.csv"), `region,zone_ugc,same_code,name,lon,lat,operational,source_url
AN,ANZ531,073531,Chesapeake Bay from Pooles Island to Sandy Point MD,-76.3446,39.1806,true,https://www.weather.gov/source/gis/Shapefiles/WSOM/mareas20fe25.txt
AN,ANZ532,073532,Chesapeake Bay from Sandy Point to North Beach MD,-76.4244,38.8506,true,https://www.weather.gov/source/gis/Shapefiles/WSOM/mareas20fe25.txt
AN,ANZ539,073539,Chester River to Queenstown MD,-76.2538,39.0433,true,https://www.weather.gov/source/gis/Shapefiles/WSOM/mareas20fe25.txt
AN,ANZ540,073540,Eastern Bay,-76.273,38.8694,true,https://www.weather.gov/source/gis/Shapefiles/WSOM/mareas20fe25.txt
AN,ANZ541,073541,Choptank River to Cambridge MD and the Little Choptank River,-76.2539,38.6262,true,https://www.weather.gov/source/gis/Shapefiles/WSOM/mareas20fe25.txt
`)
	configPath := filepath.Join(dir, "config.yaml")

	areas := ResolveAreaNames(configPath, []string{"Unknown Location (073531)"}, []string{"073531", "073532", "073539", "073540", "073541"})
	want := []string{
		"Chesapeake Bay from Pooles Island to Sandy Point MD",
		"Chesapeake Bay from Sandy Point to North Beach MD",
		"Chester River to Queenstown MD",
		"Eastern Bay",
		"Choptank River to Cambridge MD and the Little Choptank River",
	}
	if strings.Join(areas, "|") != strings.Join(want, "|") {
		t.Fatalf("areas = %#v, want %#v", areas, want)
	}
}

func TestBuildCAPAlertTextUsesSharedWeatherSpeech(t *testing.T) {
	now := time.Date(2026, 6, 17, 3, 0, 0, 0, time.UTC)
	alert := capmodel.Alert{
		Identifier:  "cap-1",
		Sender:      "cap-pac@canada.ca",
		Sent:        "2026-06-17T02:30:00Z",
		MessageType: "Alert",
		Infos: []capmodel.AlertInfo{{
			Event:       "Severe Thunderstorm Warning",
			Headline:    "Severe Thunderstorm Warning - in effect",
			SenderName:  "Environment Canada",
			Effective:   "2026-06-17T02:30:00Z",
			Expires:     "2026-06-17T04:00:00Z",
			Description: "Nickel size hail is possible.",
			Instruction: "Take shelter if threatening weather approaches.",
		}},
	}
	text := BuildCAPAlertText(CAPMessageRequest{
		Alert:     alert,
		Info:      alert.Infos[0],
		AreaText:  "City of Saskatoon",
		Timezone:  "America/Regina",
		Now:       now,
		EventName: AlertSubject(alert.Infos[0]),
	})

	if !strings.Contains(text, "Environment Canada has issued a Severe Thunderstorm Warning") {
		t.Fatalf("text = %q", text)
	}
	if !strings.Contains(text, "City of Saskatoon") || !strings.Contains(text, "Nickel size hail is possible.") {
		t.Fatalf("text = %q", text)
	}
}

func TestCleanNWSAlertTextStripsProductPreambleBulletsAndEllipses(t *testing.T) {
	raw := `SVRAKQ The National Weather Service in Wakefield has issued a

* Severe Thunderstorm Warning for...
Hanover County in central Virginia...

* Until 645 PM EDT.

* At 557 PM EDT, a severe thunderstorm was located near Gum Spring,
moving east at 40 mph.

HAZARD...60 mph wind gusts.

SOURCE...Radar indicated.

IMPACT...Expect damage to trees and powerlines.

* Locations impacted include...
Montpelier and Ashland.

Please report severe weather by calling 757-899-2415.`

	text := CleanNWSAlertText(raw)

	for _, forbidden := range []string{"SVRAKQ", "has issued", "*", "...", "Please report severe weather"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("cleaned NWS text still contains %q: %q", forbidden, text)
		}
	}
	for _, wanted := range []string{"At 557 PM EDT", "HAZARD. 60 mph wind gusts.", "SOURCE. Radar indicated.", "IMPACT. Expect damage", "Locations impacted include. Montpelier"} {
		if !strings.Contains(text, wanted) {
			t.Fatalf("cleaned NWS text missing %q: %q", wanted, text)
		}
	}
}

func TestPickBannerGradientUsesWarningWatchAdvisoryWords(t *testing.T) {
	cases := []struct {
		name  string
		event string
		want  string
	}{
		{name: "same advisory code with warning title", event: "DMO - Practice/demo Warning", want: "#019310"},
		{name: "same warning code", event: "SVR", want: "#931102"},
		{name: "canadian yellow warning", event: "Yellow Warning - Severe Thunderstorm", want: "#931102"},
		{name: "watch", event: "Severe Thunderstorm Watch", want: "#929301"},
		{name: "advisory", event: "Yellow Advisory - Fog", want: "#019310"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PickBannerColor([]AlertVisualInput{{Severity: "Unknown", Event: tc.event}})
			if got != tc.want {
				t.Fatalf("color = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPickBannerGradientForcesBroadcastImmediateRed(t *testing.T) {
	got := PickBannerColor([]AlertVisualInput{{
		Severity:           "Minor",
		Event:              "civilEmerg",
		BroadcastImmediate: true,
	}})

	if got != "#931102" {
		t.Fatalf("color = %q, want red broadcast-immediate color", got)
	}
}

func TestSerializeCAPAlertUsesHeadlineForBackgroundColor(t *testing.T) {
	alert := capmodel.Alert{
		Identifier:  "cap-yellow-warning",
		Sender:      "cap-pac@canada.ca",
		Sent:        "2026-06-17T02:30:00Z",
		MessageType: "Alert",
		Infos: []capmodel.AlertInfo{{
			Event:      "thunderstorm",
			Headline:   "yellow warning - severe thunderstorm - in effect",
			SenderName: "Environment Canada",
			Severity:   "Moderate",
		}},
	}

	serialized := SerializeCAPAlert(alert, alert.Infos[0], "sk-0001", []string{"City of Saskatoon"}, "America/Regina", "cap", time.Now().UTC())

	if serialized.BackgroundColor != "#931102" {
		t.Fatalf("background color = %q", serialized.BackgroundColor)
	}
}

func TestIsCAPEndedRequiresEveryInfoToBeEndedOrExpired(t *testing.T) {
	now := time.Date(2026, 7, 3, 21, 0, 0, 0, time.UTC)
	alert := capmodel.Alert{
		Identifier:  "mixed-active-ended",
		MessageType: "Update",
		Infos: []capmodel.AlertInfo{
			{
				Headline: "yellow watch - severe thunderstorm - in effect",
				Expires:  now.Add(2 * time.Hour).Format(time.RFC3339),
				Parameters: []capmodel.NameValue{{
					Name:  "layer:EC-MSC-SMC:1.0:Alert_Location_Status",
					Value: "Active",
				}},
			},
			{
				Headline: "yellow watch - severe thunderstorm - ended",
				Expires:  now.Add(-1 * time.Minute).Format(time.RFC3339),
				Parameters: []capmodel.NameValue{{
					Name:  "layer:EC-MSC-SMC:1.0:Alert_Location_Status",
					Value: "Ended",
				}},
			},
		},
	}

	if IsCAPEnded(alert, now) {
		t.Fatal("mixed active and ended info blocks should not end the whole alert")
	}

	alert.Infos[0].Headline = "yellow watch - severe thunderstorm - ended"
	alert.Infos[0].Parameters[0].Value = "Ended"
	if !IsCAPEnded(alert, now) {
		t.Fatal("all explicitly ended info blocks should end the alert")
	}
}

func TestSpeechFromDataRespectsDisabledSameIntro(t *testing.T) {
	intro := "Environment Canada has issued a Practice/demo Warning for Saskatoon."
	cases := []struct {
		name string
		data map[string]any
		want string
	}{
		{
			name: "strips stored intro from alert text",
			data: map[string]any{
				"prepend_same_translation": false,
				"same_translation":         intro,
				"alert_text":               intro + " Custom text.",
			},
			want: "Custom text.",
		},
		{
			name: "falls back without same translation",
			data: map[string]any{
				"prepend_same_translation": false,
				"same_translation":         intro,
				"title":                    "Practice/demo Warning",
				"description":              "Custom text.",
			},
			want: "Practice/demo Warning Custom text.",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := SpeechFromData(tc.data); got != tc.want {
				t.Fatalf("speech = %q, want %q", got, tc.want)
			}
		})
	}
}

func mustWrite(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
