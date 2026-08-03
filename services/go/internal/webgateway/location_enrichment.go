package webgateway

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
)

const webLocationEnrichmentTimeout = 4 * time.Second

func (s *wsSession) canonicalSAMELocationReferences(codes []string) []alertmodel.LocationReference {
	references := make([]alertmodel.LocationReference, 0, len(codes))
	inputs := make([]locationclient.Input, 0, len(codes))
	for _, raw := range codes {
		code := strings.TrimSpace(raw)
		if code == "" {
			continue
		}
		references = append(references, alertmodel.LocationReference{
			RawIdentifiers: []alertmodel.LocationIdentifier{{Authority: "same", Scheme: "same", Value: code}},
		})
		inputs = append(inputs, locationclient.Input{Kind: "identifier", Authority: "same", Scheme: "same", Value: code})
	}
	mode := webLocationRolloutMode(s.configPath)
	if mode == locationclient.ModeLegacy || len(inputs) == 0 {
		return references
	}
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return references
	}
	client := locationclient.New(bridgeAddr, "haze-web-alert-location")
	client.Timeout = webLocationEnrichmentTimeout
	ctx := context.Background()
	if s != nil && s.request != nil {
		ctx = s.request.Context()
	}
	queryCtx, cancel := context.WithTimeout(ctx, webLocationEnrichmentTimeout)
	defer cancel()
	response, err := client.Query(queryCtx, locationclient.Request{
		Operation: "batch_resolve",
		Inputs:    inputs,
		Options: locationclient.Options{
			Limit:                  4,
			MinimumConfidence:      "low",
			StationModeRequirement: "any",
		},
	})
	if err != nil {
		log.Printf("web alert location enrichment failed: %v", err)
		return references
	}
	for _, batch := range response.Batches {
		if batch.InputIndex < 0 || batch.InputIndex >= len(references) || len(batch.Results) == 0 {
			continue
		}
		reference := &references[batch.InputIndex]
		candidate := batch.Results[0]
		reference.CandidateID = candidate.Entity.ID
		reference.Name = candidate.Entity.DisplayName()
		reference.Kind = candidate.Entity.Kind
		reference.Country = candidate.Entity.Country
		reference.Region = candidate.Entity.Region
		reference.MatchScore = candidate.Match.Score
		reference.MatchConfidence = strings.ToLower(strings.TrimSpace(candidate.Match.Confidence))
		reference.MatchMethod = candidate.Match.Method
		reference.SourceQuality = candidate.Entity.SourceQuality
		reference.CatalogGeneration = response.CatalogGeneration
		reference.Ambiguous = batch.Status == "ambiguous" || len(batch.Results) > 1
		for _, identifier := range candidate.Entity.Identifiers {
			reference.CanonicalIdentifiers = append(reference.CanonicalIdentifiers, alertmodel.LocationIdentifier{
				Authority: identifier.Authority,
				Scheme:    identifier.Scheme,
				Value:     identifier.Value,
			})
		}
		if mode == locationclient.ModeAuthoritative && !reference.Ambiguous && (reference.MatchConfidence == "exact" || reference.MatchConfidence == "high") {
			reference.CanonicalID = candidate.Entity.ID
			reference.Actionable = true
		}
	}
	return references
}

func webLocationRolloutMode(configPath string) locationclient.Mode {
	if root, err := loadYAMLMap(configPath); err == nil {
		return locationclient.ParseMode(textAt(root, []string{"services", "rust", "location", "mode"}, string(locationclient.RolloutMode()), 32))
	}
	return locationclient.RolloutMode()
}
