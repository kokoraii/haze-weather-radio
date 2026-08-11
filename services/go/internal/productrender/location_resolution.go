package productrender

import (
	"context"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
)

type capLocationQuery struct {
	referenceIndex int
	input          locationclient.Input
}

func (s *Service) canonicalCAPLocations(alert capmodel.Alert) []alertmodel.LocationReference {
	references, queries := capLocationReferences(alert)
	mode := locationclient.ParseMode(s.cfg.Root.Services.Rust.Location.Mode)
	if mode == locationclient.ModeLegacy || len(queries) == 0 {
		return references
	}
	bridgeAddr := strings.TrimSpace(s.options.BridgeAddr)
	if bridgeAddr == "" {
		bridgeAddr = strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	}
	client := locationclient.New(bridgeAddr, "haze-product-render-location")
	client.Timeout = 4 * time.Second
	started := time.Now()
	resolved := 0
	ambiguous := 0
	for start := 0; start < len(queries); start += 100 {
		end := start + 100
		if end > len(queries) {
			end = len(queries)
		}
		inputs := make([]locationclient.Input, 0, end-start)
		for _, query := range queries[start:end] {
			inputs = append(inputs, query.input)
		}
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		response, err := client.Query(ctx, locationclient.Request{
			Operation: "batch_resolve",
			Inputs:    inputs,
			Options: locationclient.Options{
				Limit:                  4,
				MinimumConfidence:      "low",
				StationModeRequirement: "any",
			},
		})
		cancel()
		if err != nil {
			log.Printf("location CAP enrichment failed for alert %s: %v", alert.Identifier, err)
			continue
		}
		for _, batch := range response.Batches {
			queryIndex := start + batch.InputIndex
			if queryIndex < start || queryIndex >= end {
				continue
			}
			referenceIndex := queries[queryIndex].referenceIndex
			if referenceIndex < 0 || referenceIndex >= len(references) {
				continue
			}
			reference := &references[referenceIndex]
			reference.CatalogGeneration = response.CatalogGeneration
			if batch.Status == "ambiguous" || len(batch.Results) > 1 {
				reference.Ambiguous = true
				ambiguous++
			}
			if len(batch.Results) == 0 {
				continue
			}
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
				resolved++
			}
		}
	}
	if mode == locationclient.ModeShadow {
		log.Printf(
			"location shadow CAP enrichment alert_id=%s raw=%d candidates=%d ambiguous=%d latency_ms=%d",
			alert.Identifier,
			len(references),
			countLocationCandidates(references),
			ambiguous,
			time.Since(started).Milliseconds(),
		)
	} else if resolved > 0 {
		log.Printf("location CAP enrichment alert_id=%s resolved=%d raw=%d", alert.Identifier, resolved, len(references))
	}
	return references
}

func capLocationReferences(alert capmodel.Alert) ([]alertmodel.LocationReference, []capLocationQuery) {
	references := []alertmodel.LocationReference{}
	queries := []capLocationQuery{}
	seen := map[string]struct{}{}
	for _, info := range alert.Infos {
		for _, area := range info.Areas {
			if capmodel.IsECCCThreatArea(area) {
				continue
			}
			for _, geocode := range area.Geocodes {
				value := strings.TrimSpace(geocode.Value)
				if value == "" {
					continue
				}
				scheme, authority, queryable := capLocationIdentifier(geocode.Name, value)
				key := strings.ToLower(authority) + "\x00" + scheme + "\x00" + value
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				referenceIndex := len(references)
				references = append(references, alertmodel.LocationReference{
					RawIdentifiers: []alertmodel.LocationIdentifier{{
						Authority: authority,
						Scheme:    scheme,
						Value:     value,
					}},
				})
				if queryable {
					queries = append(queries, capLocationQuery{
						referenceIndex: referenceIndex,
						input: locationclient.Input{
							Kind:      "identifier",
							Scheme:    scheme,
							Authority: authority,
							Value:     value,
						},
					})
				}
			}
		}
	}
	return references, queries
}

func capLocationIdentifier(name string, value string) (scheme string, authority string, queryable bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	upperValue := strings.ToUpper(strings.TrimSpace(value))
	switch {
	case strings.Contains(name, "cap-cp:location"):
		return "sgc", "cap-cp", true
	case strings.HasSuffix(name, ":clc") || name == "clc":
		return "clc", "eccc", true
	case name == "same" || strings.Contains(name, "same"):
		return "same", "same", true
	case name == "ugc" || strings.Contains(name, "ugc"):
		if len(upperValue) >= 3 && upperValue[2] == 'C' {
			return "nws_ugc_county", "nws", true
		}
		return "nws_zone", "nws", true
	case strings.Contains(name, "fips"):
		return "fips", "nws", true
	case strings.Contains(name, "wmo"):
		return "wmo", "wmo", true
	default:
		return "provider", strings.TrimSpace(name), false
	}
}

func countLocationCandidates(references []alertmodel.LocationReference) int {
	count := 0
	for _, reference := range references {
		if reference.CandidateID != "" {
			count++
		}
	}
	return count
}

func canonicalSAMELocations(references []alertmodel.LocationReference) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, reference := range references {
		if !reference.Actionable || reference.CanonicalID == "" || reference.Ambiguous {
			continue
		}
		for _, identifier := range reference.CanonicalIdentifiers {
			scheme := strings.ToLower(strings.TrimSpace(identifier.Scheme))
			if scheme != "same" && scheme != "fips" && scheme != "clc" {
				continue
			}
			code := sameLocationCode(identifier.Value)
			if code == "" {
				continue
			}
			if _, exists := seen[code]; exists {
				continue
			}
			seen[code] = struct{}{}
			out = append(out, code)
		}
	}
	sort.Strings(out)
	if len(out) > 31 {
		out = out[:31]
	}
	return out
}
