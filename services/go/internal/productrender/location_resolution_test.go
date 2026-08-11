package productrender

import (
	"testing"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
)

func TestCAPLocationReferencesPreserveQualifiedRawCodes(t *testing.T) {
	alert := capmodel.Alert{Infos: []capmodel.AlertInfo{{Areas: []capmodel.AlertArea{{Geocodes: []capmodel.NameValue{
		{Name: "SAME", Value: "001001"},
		{Name: "UGC", Value: "ALC001"},
		{Name: "layer:EC-MSC-SMC:1.0:CLC", Value: "065400"},
	}}}}}}
	references, queries := capLocationReferences(alert)
	if len(references) != 3 || len(queries) != 3 {
		t.Fatalf("references=%#v queries=%#v", references, queries)
	}
	if queries[0].input.Scheme != "same" || queries[1].input.Scheme != "nws_ugc_county" || queries[2].input.Scheme != "clc" {
		t.Fatalf("qualified queries = %#v", queries)
	}
	if references[0].RawIdentifiers[0].Value != "001001" {
		t.Fatalf("leading zero was lost: %#v", references[0])
	}
}

func TestCAPLocationReferencesIgnoreECCCThreatAreaMapping(t *testing.T) {
	alert := capmodel.Alert{Infos: []capmodel.AlertInfo{{Areas: []capmodel.AlertArea{
		{Geocodes: []capmodel.NameValue{{Name: "layer:EC-MSC-SMC:1.0:CLC", Value: "065100"}}},
		{ThreatStatus: "issued", Geocodes: []capmodel.NameValue{
			{Name: capmodel.ECCCThreatAreaGeocodeName, Value: "issued"},
			{Name: "profile:CAP-CP:Location:0.3", Value: "4711075"},
		}},
	}}}}
	references, queries := capLocationReferences(alert)
	if len(references) != 1 || len(queries) != 1 || references[0].RawIdentifiers[0].Value != "065100" {
		t.Fatalf("references=%#v queries=%#v", references, queries)
	}
}

func TestCanonicalSAMELocationsRejectsNonActionableCandidates(t *testing.T) {
	locations := canonicalSAMELocations([]alertmodel.LocationReference{
		{
			CanonicalID:     "urn:haze:location:exact",
			MatchConfidence: "exact",
			Actionable:      true,
			CanonicalIdentifiers: []alertmodel.LocationIdentifier{{
				Scheme: "same",
				Value:  "001001",
			}},
		},
		{
			CandidateID:     "urn:haze:location:shadow",
			MatchConfidence: "high",
			CanonicalIdentifiers: []alertmodel.LocationIdentifier{{
				Scheme: "same",
				Value:  "001003",
			}},
		},
	})
	if len(locations) != 1 || locations[0] != "001001" {
		t.Fatalf("locations = %#v", locations)
	}
}
