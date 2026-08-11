package capmodel

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseCAP(t *testing.T) {
	raw := []byte(`<?xml version="1.0"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>ABC-123</identifier>
  <sender>sender@example.test</sender>
  <sent>2026-06-14T12:00:00Z</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>Severe Thunderstorm Warning</event>
    <urgency>Immediate</urgency>
    <severity>Severe</severity>
    <certainty>Likely</certainty>
    <headline>Storm warning</headline>
    <eventCode><valueName>SAME</valueName><value>SVR</value></eventCode>
    <area>
      <areaDesc>Test Region</areaDesc>
      <geocode><valueName>profile:CAP-CP:Location:0.4</valueName><value>4611045</value></geocode>
    </area>
  </info>
</alert>`)

	alert, err := ParseCAP(raw)
	if err != nil {
		t.Fatalf("ParseCAP returned error: %v", err)
	}
	if alert.Identifier != "ABC-123" {
		t.Fatalf("identifier = %q", alert.Identifier)
	}
	if len(alert.Infos) != 1 {
		t.Fatalf("infos = %d", len(alert.Infos))
	}
	if alert.Infos[0].Areas[0].Geocodes[0].Value != "4611045" {
		t.Fatalf("geocode = %q", alert.Infos[0].Areas[0].Geocodes[0].Value)
	}
	if len(alert.Infos[0].EventCodes) != 1 || alert.Infos[0].EventCodes[0].Value != "SVR" {
		t.Fatalf("event codes = %#v", alert.Infos[0].EventCodes)
	}
}

func TestParseCAPReferenceFixtures(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join("..", "..", "testdata", "cap", "example_*.xml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("no CAP reference fixtures found")
	}
	for _, path := range matches {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			alert, err := ParseCAP(raw)
			if err != nil {
				t.Fatalf("ParseCAP returned error: %v", err)
			}
			if alert.Identifier == "" {
				t.Fatal("identifier is empty")
			}
			if len(alert.Infos) == 0 {
				t.Fatal("infos are empty")
			}
		})
	}
}

func TestParseECCCAugust2026ThreatAreasAndStormInfo(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "cap", "eccc_2026_true_threat_area.xml"))
	if err != nil {
		t.Fatal(err)
	}
	alert, err := ParseCAP(raw)
	if err != nil {
		t.Fatalf("ParseCAP returned error: %v", err)
	}
	if len(alert.Infos) != 1 {
		t.Fatalf("infos = %d", len(alert.Infos))
	}
	info := alert.Infos[0]
	if len(info.Areas) != 5 {
		t.Fatalf("areas = %d", len(info.Areas))
	}
	statuses := []string{"", "issued", "continued", "ended", "cancelled"}
	for index, status := range statuses {
		if info.Areas[index].ThreatStatus != status {
			t.Fatalf("area %d threat status = %q, want %q", index, info.Areas[index].ThreatStatus, status)
		}
	}
	if IsECCCThreatArea(info.Areas[0]) || !IsECCCActiveThreatArea(info.Areas[1]) || !IsECCCActiveThreatArea(info.Areas[2]) || IsECCCActiveThreatArea(info.Areas[3]) || IsECCCActiveThreatArea(info.Areas[4]) {
		t.Fatalf("unexpected threat area classification: %#v", info.Areas)
	}
	if info.Storm == nil || info.Storm.SpeedValue == nil || *info.Storm.SpeedValue != 40 || info.Storm.SpeedUnit != "km/h" {
		t.Fatalf("storm speed = %#v", info.Storm)
	}
	if info.Storm.DirectionDegrees == nil || *info.Storm.DirectionDegrees != 90.12841 || info.Storm.GeometryType != "isolated_cell" {
		t.Fatalf("storm direction or geometry = %#v", info.Storm)
	}
	if len(info.Storm.Points) != 1 || info.Storm.Points[0].Latitude != 52.1433 || info.Storm.Points[0].Longitude != -106.6732 {
		t.Fatalf("storm points = %#v", info.Storm.Points)
	}
	if info.Storm.Time != "20260811153000000" || info.Storm.MotionDescription != "east at 40 km/h" || info.Storm.ReferenceLocationPoints != "Saskatoon, Warman" {
		t.Fatalf("storm text fields = %#v", info.Storm)
	}
	if len(alert.Warnings) != 0 {
		t.Fatalf("warnings = %#v", alert.Warnings)
	}
}

func TestParseECCCAugust2026WarnsOnInvalidTypedValues(t *testing.T) {
	raw := []byte(`<?xml version="1.0"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>invalid-eccc-2026</identifier><sender>cap-pac@canada.ca</sender>
  <sent>2026-08-11T15:30:00Z</sent><status>Actual</status><msgType>Alert</msgType><scope>Public</scope>
  <info><language>en-CA</language><category>Met</category><event>tornado warning</event>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:Storm_Direction</valueName><value>720</value></parameter>
    <area><areaDesc>new active threat area</areaDesc>
      <geocode><valueName>layer:EC-MSC-SMC:DLC:1.1</valueName><value>unexpected</value></geocode>
    </area>
  </info>
</alert>`)
	alert, err := ParseCAP(raw)
	if err != nil {
		t.Fatalf("non-fatal ECCC validation rejected CAP: %v", err)
	}
	joined := strings.Join(alert.Warnings, " | ")
	for _, expected := range []string{"invalid ECCC threat status", "ECCC threat area has no polygon", "invalid ECCC storm direction"} {
		if !strings.Contains(joined, expected) {
			t.Fatalf("warnings = %q, missing %q", joined, expected)
		}
	}
}

func TestECCCAugust2026RejectsNonFiniteStormNumbers(t *testing.T) {
	for _, test := range []struct {
		name      string
		parameter string
		value     string
		warning   string
	}{
		{name: "speed", parameter: ecccStormSpeedName, value: "NaN km/h", warning: "invalid ECCC storm speed"},
		{name: "direction", parameter: ecccStormDirectionName, value: "NaN", warning: "invalid ECCC storm direction"},
		{name: "point", parameter: ecccStormPointName, value: "NaN,-106.67", warning: "invalid ECCC storm point"},
	} {
		t.Run(test.name, func(t *testing.T) {
			info := AlertInfo{Parameters: []NameValue{{Name: test.parameter, Value: test.value}}}
			normalizeECCC2026Info(&info)
			warnings := validateECCC2026Info(info, "info[0]")
			if !strings.Contains(strings.Join(warnings, " | "), test.warning) {
				t.Fatalf("warnings = %#v, missing %q", warnings, test.warning)
			}
		})
	}
}

func TestParseCAPRejectsMissingRequiredHeader(t *testing.T) {
	raw := []byte(`<?xml version="1.0"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>ABC-123</identifier>
  <sender>sender@example.test</sender>
  <sent>not-a-time</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
</alert>`)

	if _, err := ParseCAP(raw); err == nil {
		t.Fatal("ParseCAP accepted invalid CAP header")
	}
}

func TestParseCAPAcceptsNAADSHeartbeatWithoutInfo(t *testing.T) {
	raw := []byte(`<?xml version="1.0"?>
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>heartbeat-1</identifier>
  <sender>NAADS-Heartbeat</sender>
  <sent>2026-06-14T12:00:00Z</sent>
  <status>System</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <references>sender,abc,2026-06-14T11:59:00Z</references>
</alert>`)

	alert, err := ParseCAP(raw)
	if err != nil {
		t.Fatalf("ParseCAP returned error: %v", err)
	}
	if alert.Status != "System" || len(alert.Infos) != 0 {
		t.Fatalf("heartbeat parse = %#v", alert)
	}
}
