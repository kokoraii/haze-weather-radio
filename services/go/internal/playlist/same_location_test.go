package playlist

import (
	"testing"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
)

func TestSameLocationsPreferActionableCanonicalIdentifiers(t *testing.T) {
	packet := alertmodel.Packet{
		ID: "alert-1",
		Areas: alertmodel.Areas{Locations: []alertmodel.LocationReference{{
			CanonicalID:     "urn:haze:location:county",
			MatchConfidence: "exact",
			Actionable:      true,
			CanonicalIdentifiers: []alertmodel.LocationIdentifier{{
				Authority: "nws",
				Scheme:    "same",
				Value:     "001001",
			}},
		}}},
	}
	data := alertmodel.WithLegacyFields(packet, map[string]any{"same_locations": []string{"001003"}})
	locations := sameLocationsFromData(data)
	if len(locations) != 2 || locations[0] != "001001" || locations[1] != "001003" {
		t.Fatalf("locations = %#v", locations)
	}
}

func TestSameLocationsIgnoreAmbiguousCanonicalCandidate(t *testing.T) {
	packet := alertmodel.Packet{
		ID: "alert-1",
		Areas: alertmodel.Areas{Locations: []alertmodel.LocationReference{{
			CandidateID:     "urn:haze:location:ambiguous",
			MatchConfidence: "high",
			Ambiguous:       true,
			CanonicalIdentifiers: []alertmodel.LocationIdentifier{{
				Authority: "nws",
				Scheme:    "same",
				Value:     "001001",
			}},
		}}},
	}
	data := alertmodel.WithLegacyFields(packet, map[string]any{"same_locations": []string{"001003"}})
	locations := sameLocationsFromData(data)
	if len(locations) != 1 || locations[0] != "001003" {
		t.Fatalf("locations = %#v", locations)
	}
}
