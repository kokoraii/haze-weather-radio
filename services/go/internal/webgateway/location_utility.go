package webgateway

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
)

const (
	locationUtilityTimeout       = 8 * time.Second
	locationUtilityMaxTextLength = 512
)

func (s *wsSession) queryLocationUtility(payload map[string]any) (locationclient.Response, error) {
	request, err := locationUtilityRequest(payload)
	if err != nil {
		return locationclient.Response{}, err
	}
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return locationclient.Response{}, fmt.Errorf("location service bridge is unavailable")
	}
	client := locationclient.New(bridgeAddr, "haze-web-location-utility")
	client.Timeout = locationUtilityTimeout
	ctx := context.Background()
	if s != nil && s.request != nil {
		ctx = s.request.Context()
	}
	queryCtx, cancel := context.WithTimeout(ctx, locationUtilityTimeout)
	defer cancel()
	return client.Query(queryCtx, request)
}

func locationUtilityRequest(payload map[string]any) (locationclient.Request, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return locationclient.Request{}, fmt.Errorf("location query is not valid JSON: %w", err)
	}
	var request locationclient.Request
	if err := json.Unmarshal(raw, &request); err != nil {
		return locationclient.Request{}, fmt.Errorf("location query is invalid: %w", err)
	}
	request.APIVersion = locationclient.APIVersion
	request.RequestID = ""
	request.Operation = strings.ToLower(strings.TrimSpace(request.Operation))
	if !locationUtilityOneOf(request.Operation, "resolve", "batch_resolve", "search", "point_facets", "nearest", "traverse") {
		return locationclient.Request{}, fmt.Errorf("unsupported location operation %q", request.Operation)
	}
	if request.Options.Limit < 0 || request.Options.Limit > 100 {
		return locationclient.Request{}, fmt.Errorf("location result limit must be between 1 and 100")
	}
	if request.Options.MaxDistanceKM != nil && (!locationUtilityFinite(*request.Options.MaxDistanceKM) || *request.Options.MaxDistanceKM < 0) {
		return locationclient.Request{}, fmt.Errorf("maximum distance must be a non-negative number")
	}
	if request.Options.MaxDepth < 0 || request.Options.MaxDepth > 5 {
		return locationclient.Request{}, fmt.Errorf("graph depth must be between 1 and 5")
	}
	if request.Options.MaxVisited < 0 || request.Options.MaxVisited > 10_000 {
		return locationclient.Request{}, fmt.Errorf("visited-node limit must be between 1 and 10000")
	}
	if value := strings.ToLower(strings.TrimSpace(request.Options.MinimumConfidence)); value != "" && !locationUtilityOneOf(value, "low", "medium", "high", "exact") {
		return locationclient.Request{}, fmt.Errorf("minimum confidence must be low, medium, high, or exact")
	} else {
		request.Options.MinimumConfidence = value
	}
	if value := strings.ToLower(strings.TrimSpace(request.Options.DedupeMode)); value != "" && !locationUtilityOneOf(value, "none", "similar_name") {
		return locationclient.Request{}, fmt.Errorf("deduplication mode must be none or similar_name")
	} else {
		request.Options.DedupeMode = value
	}
	if request.Options.StationModePreference != nil {
		value := strings.ToLower(strings.TrimSpace(*request.Options.StationModePreference))
		if value == "" {
			request.Options.StationModePreference = nil
		} else if !locationUtilityOneOf(value, "auto", "manual") {
			return locationclient.Request{}, fmt.Errorf("station mode preference must be auto or manual")
		} else {
			request.Options.StationModePreference = &value
		}
	}
	if value := strings.ToLower(strings.TrimSpace(request.Options.StationModeRequirement)); value != "" && !locationUtilityOneOf(value, "any", "auto", "manual") {
		return locationclient.Request{}, fmt.Errorf("station mode requirement must be any, auto, or manual")
	} else {
		request.Options.StationModeRequirement = value
	}
	if request.Options.GeographicBias != nil {
		if err := validateLocationUtilityPoint(request.Options.GeographicBias.Latitude, request.Options.GeographicBias.Longitude); err != nil {
			return locationclient.Request{}, fmt.Errorf("geographic bias %w", err)
		}
	}
	if err := validateLocationUtilityFilters(request.Filters); err != nil {
		return locationclient.Request{}, err
	}
	if request.Operation == "batch_resolve" {
		if len(request.Inputs) == 0 || len(request.Inputs) > 100 {
			return locationclient.Request{}, fmt.Errorf("batch resolve requires between 1 and 100 inputs")
		}
		for index := range request.Inputs {
			if err := validateLocationUtilityInput(&request.Inputs[index]); err != nil {
				return locationclient.Request{}, fmt.Errorf("batch input %d: %w", index+1, err)
			}
		}
		request.Input = nil
		return request, nil
	}
	if request.Input == nil {
		return locationclient.Request{}, fmt.Errorf("location input is required")
	}
	if err := validateLocationUtilityInput(request.Input); err != nil {
		return locationclient.Request{}, err
	}
	expectedKind := map[string]string{
		"search":       "name",
		"point_facets": "point",
		"nearest":      "point",
		"traverse":     "entity",
	}[request.Operation]
	if expectedKind != "" && request.Input.Kind != expectedKind {
		return locationclient.Request{}, fmt.Errorf("%s requires a %s input", request.Operation, expectedKind)
	}
	if request.Operation == "resolve" && request.Input.Kind != "identifier" && request.Input.Kind != "auto" {
		return locationclient.Request{}, fmt.Errorf("resolve requires an identifier or auto input")
	}
	request.Inputs = nil
	return request, nil
}

func validateLocationUtilityInput(input *locationclient.Input) error {
	input.Kind = strings.ToLower(strings.TrimSpace(input.Kind))
	input.Scheme = strings.ToLower(strings.TrimSpace(input.Scheme))
	input.Authority = strings.ToLower(strings.TrimSpace(input.Authority))
	input.Value = strings.TrimSpace(input.Value)
	input.Text = strings.TrimSpace(input.Text)
	input.ID = strings.TrimSpace(input.ID)
	if !locationUtilityOneOf(input.Kind, "identifier", "name", "auto", "point", "entity") {
		return fmt.Errorf("unsupported input kind %q", input.Kind)
	}
	for label, value := range map[string]string{
		"scheme": input.Scheme, "authority": input.Authority, "value": input.Value,
		"text": input.Text, "entity ID": input.ID,
	} {
		if len(value) > locationUtilityMaxTextLength {
			return fmt.Errorf("%s is too long", label)
		}
	}
	switch input.Kind {
	case "identifier":
		if input.Scheme == "" || input.Value == "" {
			return fmt.Errorf("identifier scheme and value are required")
		}
	case "name", "auto":
		if input.Text == "" {
			return fmt.Errorf("location text is required")
		}
	case "point":
		if input.Latitude == nil || input.Longitude == nil {
			return fmt.Errorf("latitude and longitude are required")
		}
		if err := validateLocationUtilityPoint(*input.Latitude, *input.Longitude); err != nil {
			return err
		}
	case "entity":
		if input.ID == "" || !strings.HasPrefix(strings.ToLower(input.ID), "urn:haze:location:") {
			return fmt.Errorf("a canonical urn:haze:location entity ID is required")
		}
	}
	return nil
}

func validateLocationUtilityFilters(filters locationclient.Filters) error {
	for label, values := range map[string][]string{
		"kinds": filters.Kinds, "capabilities": filters.Capabilities, "roles": filters.Roles,
		"relationship types": filters.RelationshipTypes,
	} {
		if len(values) > 64 {
			return fmt.Errorf("too many %s filters", label)
		}
		for _, value := range values {
			if len(strings.TrimSpace(value)) > 128 {
				return fmt.Errorf("%s filter is too long", label)
			}
		}
	}
	if len(strings.TrimSpace(filters.Country)) > 16 || len(strings.TrimSpace(filters.Region)) > 64 {
		return fmt.Errorf("country or region filter is too long")
	}
	return nil
}

func validateLocationUtilityPoint(latitude float64, longitude float64) error {
	if !locationUtilityFinite(latitude) || latitude < -90 || latitude > 90 {
		return fmt.Errorf("latitude must be between -90 and 90")
	}
	if !locationUtilityFinite(longitude) || longitude < -180 || longitude > 180 {
		return fmt.Errorf("longitude must be between -180 and 180")
	}
	return nil
}

func locationUtilityFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func locationUtilityOneOf(value string, choices ...string) bool {
	for _, choice := range choices {
		if value == choice {
			return true
		}
	}
	return false
}
