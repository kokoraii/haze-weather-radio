package alertmodel

import "testing"

func TestFromMapPrefersAlertPacket(t *testing.T) {
	packet, ok := FromMap(map[string]any{
		"alert_id":    "flat-id",
		"description": "flat description",
		"alert_packet": map[string]any{
			"id":      "packet-id",
			"source":  "nws",
			"feed_id": "CAP-IT-ALL",
			"content": map[string]any{
				"event":       "SVR",
				"description": "packet description",
				"instruction": "packet instruction",
			},
			"same": map[string]any{
				"include":   true,
				"event":     "SVR",
				"locations": []any{"008075", "008075"},
			},
		},
	})
	if !ok {
		t.Fatal("packet was not resolved")
	}
	if packet.ID != "packet-id" || packet.Source != "nws" || packet.Content.Description != "packet description" {
		t.Fatalf("packet = %#v", packet)
	}
	if packet.SAME == nil || len(packet.SAME.Locations) != 1 || packet.SAME.Locations[0] != "008075" {
		t.Fatalf("same = %#v", packet.SAME)
	}
}

func TestWithLegacyFieldsMirrorsPacket(t *testing.T) {
	packet := Packet{
		ID:          "alert-1",
		Source:      "nws",
		FeedID:      "CAP-IT-ALL",
		MessageType: "Alert",
		Content: Content{
			Headline:    "Severe Thunderstorm Warning",
			Event:       "Severe Thunderstorm Warning",
			Severity:    "Severe",
			Description: "At 400 PM MDT, a severe thunderstorm was located east of Sterling.",
			Instruction: "Move indoors.",
		},
		SAME: &SAME{
			Include:     true,
			Event:       "SVR",
			EventName:   "Severe Thunderstorm Warning",
			Originator:  "WXR",
			Locations:   []string{"008075"},
			Translation: "The National Weather Service has issued a Severe Thunderstorm Warning.",
		},
	}
	fields := WithLegacyFields(packet, map[string]any{"queue_id": "queue-1"})
	if fields["alert_id"] != "alert-1" || fields["cap_source"] != "nws" || fields["same_event"] != "SVR" {
		t.Fatalf("fields = %#v", fields)
	}
	if fields["description"] != packet.Content.Description || fields["instruction"] != packet.Content.Instruction {
		t.Fatalf("body fields were not mirrored: %#v", fields)
	}
	if _, ok := fields["alert_packet"].(Packet); !ok {
		t.Fatalf("alert_packet missing or wrong type: %#v", fields["alert_packet"])
	}
}

func TestLocationReferencesKeepRawIdentifiersButRejectWeakAuthority(t *testing.T) {
	packet := (Packet{
		Areas: Areas{
			Locations: []LocationReference{{
				CanonicalID:     "urn:haze:location:weak",
				Name:            "Possible Place",
				MatchConfidence: "medium",
				Actionable:      true,
				RawIdentifiers: []LocationIdentifier{{
					Authority: "nws",
					Scheme:    "same",
					Value:     "001001",
				}},
			}},
		},
	}).Normalize()
	if len(packet.Areas.Locations) != 1 {
		t.Fatalf("locations = %#v", packet.Areas.Locations)
	}
	location := packet.Areas.Locations[0]
	if location.Actionable || location.CanonicalID != "" || location.CandidateID != "urn:haze:location:weak" {
		t.Fatalf("weak candidate became authoritative: %#v", location)
	}
	if len(location.RawIdentifiers) != 1 || location.RawIdentifiers[0].Value != "001001" {
		t.Fatalf("raw identifier was not preserved: %#v", location.RawIdentifiers)
	}
}

func TestThreatAreasAndStormInfoSurviveCompatibilityFields(t *testing.T) {
	speed := 40.0
	direction := 90.12841
	packet := Packet{
		ID: "eccc-threat-1",
		Areas: Areas{ThreatAreas: []ThreatArea{{
			Status:         "ISSUED",
			Description:    "new active threat area",
			Polygons:       []string{"52.1,-106.7 52.2,-106.6 52.1,-106.7"},
			CAPCPLocations: []string{"4711066", "4711066"},
		}}},
		Storm: &StormInfo{
			Speed:                   "40 km/h",
			SpeedValue:              &speed,
			SpeedUnit:               "KM/H",
			DirectionDegrees:        &direction,
			GeometryType:            "ISOLATED_CELL",
			Points:                  []GeoPoint{{Latitude: 52.1433, Longitude: -106.6732}},
			Time:                    "20260811153000000",
			MotionDescription:       "east at 40 km/h",
			ReferenceLocationPoints: "Saskatoon, Warman",
		},
	}
	fields := WithLegacyFields(packet, nil)
	resolved, ok := FromMap(fields)
	if !ok {
		t.Fatal("packet was not resolved")
	}
	if len(resolved.Areas.ThreatAreas) != 1 || resolved.Areas.ThreatAreas[0].Status != "issued" || len(resolved.Areas.ThreatAreas[0].CAPCPLocations) != 1 {
		t.Fatalf("threat areas = %#v", resolved.Areas.ThreatAreas)
	}
	if resolved.Storm == nil || resolved.Storm.SpeedUnit != "km/h" || resolved.Storm.GeometryType != "isolated_cell" || resolved.Storm.DirectionDegrees == nil || *resolved.Storm.DirectionDegrees != direction {
		t.Fatalf("storm = %#v", resolved.Storm)
	}
	if _, ok := fields["threat_areas"]; !ok {
		t.Fatalf("legacy threat_areas missing: %#v", fields)
	}
	if _, ok := fields["storm"]; !ok {
		t.Fatalf("legacy storm missing: %#v", fields)
	}
}
