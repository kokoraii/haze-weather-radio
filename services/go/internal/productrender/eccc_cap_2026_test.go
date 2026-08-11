package productrender

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
)

func loadECCCAugust2026TestInfo(t *testing.T) capmodel.AlertInfo {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "cap", "eccc_2026_true_threat_area.xml"))
	if err != nil {
		t.Fatal(err)
	}
	alert, err := capmodel.ParseCAP(raw)
	if err != nil {
		t.Fatal(err)
	}
	return alert.Infos[0]
}

func TestECCCAugust2026ThreatAreasDoNotBroadenRelayCoverage(t *testing.T) {
	info := loadECCCAugust2026TestInfo(t)
	keys := capAlertInfoLocationKeys(info)
	if !reflect.DeepEqual(keys, []string{"065100", "4711066"}) {
		t.Fatalf("zone-based relay keys = %#v", keys)
	}
	locations := sameLocationsForCAP(info, feedXML{}, t.TempDir())
	if !reflect.DeepEqual(locations, []string{"065100"}) {
		t.Fatalf("SAME locations = %#v", locations)
	}
}

func TestECCCAugust2026ThreatAndStormPacketMetadata(t *testing.T) {
	info := loadECCCAugust2026TestInfo(t)
	areas := capPacketThreatAreas(info)
	if len(areas) != 4 {
		t.Fatalf("threat areas = %#v", areas)
	}
	if areas[0].Status != "issued" || areas[1].Status != "continued" || areas[2].Status != "ended" || areas[3].Status != "cancelled" {
		t.Fatalf("threat statuses = %#v", areas)
	}
	if !reflect.DeepEqual(areas[0].CAPCPLocations, []string{"4711066"}) {
		t.Fatalf("CAP-CP locations = %#v", areas[0].CAPCPLocations)
	}
	storm := capPacketStormInfo(info)
	if storm == nil || storm.SpeedValue == nil || *storm.SpeedValue != 40 || storm.DirectionDegrees == nil || *storm.DirectionDegrees != 90.12841 {
		t.Fatalf("storm = %#v", storm)
	}
}

func TestECCCAugust2026ThreatDescriptionsDoNotBecomeSpokenAreas(t *testing.T) {
	info := loadECCCAugust2026TestInfo(t)
	areas := alertAreas(info, feedXML{}, "en-CA", t.TempDir(), nil, []string{"065100", "4711066"})
	if areas != "City of Saskatoon" {
		t.Fatalf("spoken areas = %q", areas)
	}
	fastAreas := fastAlertAreas(info, nil)
	if fastAreas != "City of Saskatoon" {
		t.Fatalf("fast spoken areas = %q", fastAreas)
	}
}
