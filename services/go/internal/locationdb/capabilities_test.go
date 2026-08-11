package locationdb

import (
	"math"
	"testing"
)

func TestCapabilityCatalogNearestPrefersNearbySameProvince(t *testing.T) {
	t.Parallel()
	catalog := NewCapabilityCatalog([]CapabilityLocation{
		{Kind: CapabilityObservation, ID: "NEAR", Name: "Nearby", Region: "AB", Country: "CA", Latitude: 52.01, Longitude: -106.00},
		{Kind: CapabilityObservation, ID: "SAME", Name: "Same province", Region: "SK", Country: "CA", Latitude: 52.20, Longitude: -106.00},
		{Kind: CapabilityObservation, ID: "FAR", Name: "Far away", Region: "SK", Country: "CA", Latitude: 55.00, Longitude: -106.00},
	})

	matches := catalog.Nearest(CapabilityObservation, 52, -106, "SK", 3, 250)
	if len(matches) != 2 {
		t.Fatalf("matches = %#v", matches)
	}
	if matches[0].Location.ID != "SAME" || matches[1].Location.ID != "NEAR" {
		t.Fatalf("same-province preference not applied: %#v", matches)
	}
	if matches[0].DistanceKM <= matches[1].DistanceKM {
		t.Fatalf("fixture should prove the province preference is bounded ordering, distances = %#v", matches)
	}
}

func TestCapabilityCatalogNearestHonorsLimitAndDistance(t *testing.T) {
	t.Parallel()
	catalog := NewCapabilityCatalog([]CapabilityLocation{
		{Kind: CapabilityForecast, ID: "SK-1", Latitude: 52.00, Longitude: -106.00},
		{Kind: CapabilityForecast, ID: "SK-2", Latitude: 52.10, Longitude: -106.00},
		{Kind: CapabilityForecast, ID: "SK-3", Latitude: 55.00, Longitude: -106.00},
	})

	matches := catalog.Nearest(CapabilityForecast, 52, -106, "SK", 1, 50)
	if len(matches) != 1 || matches[0].Location.ID != "SK-1" {
		t.Fatalf("matches = %#v", matches)
	}
	if math.Abs(matches[0].DistanceKM) > 0.001 {
		t.Fatalf("distance = %f", matches[0].DistanceKM)
	}
}

func TestCapabilityCatalogRejectsIncompleteRecords(t *testing.T) {
	t.Parallel()
	catalog := NewCapabilityCatalog([]CapabilityLocation{
		{Kind: CapabilityObservation, ID: "", Latitude: 52, Longitude: -106},
		{Kind: CapabilityObservation, ID: "ZERO", Latitude: 0, Longitude: 0},
		{Kind: CapabilityObservation, ID: "VALID", Latitude: 52, Longitude: -106},
	})
	if got := catalog.Count(CapabilityObservation); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
}
