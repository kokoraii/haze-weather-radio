package webgateway

import (
	"testing"
)

func TestLocationUtilityRequestSearch(t *testing.T) {
	request, err := locationUtilityRequest(map[string]any{
		"operation": " SEARCH ",
		"input": map[string]any{
			"kind": "name",
			"text": "  Saskatoon  ",
		},
		"filters": map[string]any{"region": "SK"},
		"options": map[string]any{
			"limit":                    20,
			"dedupe_mode":              "similar_name",
			"station_mode_preference":  "AUTO",
			"station_mode_requirement": "any",
		},
	})
	if err != nil {
		t.Fatalf("locationUtilityRequest: %v", err)
	}
	if request.APIVersion != 1 || request.Operation != "search" {
		t.Fatalf("request metadata = %#v", request)
	}
	if request.Input == nil || request.Input.Text != "Saskatoon" {
		t.Fatalf("input = %#v", request.Input)
	}
	if request.Options.StationModePreference == nil || *request.Options.StationModePreference != "auto" {
		t.Fatalf("station preference = %#v", request.Options.StationModePreference)
	}
}

func TestLocationUtilityRequestRejectsWrongInputKind(t *testing.T) {
	_, err := locationUtilityRequest(map[string]any{
		"operation": "nearest",
		"input":     map[string]any{"kind": "name", "text": "Saskatoon"},
	})
	if err == nil {
		t.Fatal("expected nearest name input to fail")
	}
}

func TestLocationUtilityRequestAcceptsZeroZeroPoint(t *testing.T) {
	request, err := locationUtilityRequest(map[string]any{
		"operation": "point_facets",
		"input":     map[string]any{"kind": "point", "latitude": 0, "longitude": 0},
	})
	if err != nil {
		t.Fatalf("zero point rejected: %v", err)
	}
	if request.Input == nil || request.Input.Latitude == nil || request.Input.Longitude == nil {
		t.Fatalf("point input = %#v", request.Input)
	}
}

func TestLocationUtilityRequestRejectsOversizedBatch(t *testing.T) {
	inputs := make([]map[string]any, 101)
	for index := range inputs {
		inputs[index] = map[string]any{"kind": "auto", "text": "CYXE"}
	}
	_, err := locationUtilityRequest(map[string]any{
		"operation": "batch_resolve",
		"inputs":    inputs,
	})
	if err == nil {
		t.Fatal("expected oversized batch to fail")
	}
}
