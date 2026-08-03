package dataingest

import (
	"context"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
)

type configuredLocationBinding struct {
	input   locationclient.Input
	targets []*string
}

func hydrateConfiguredLocations(ctx context.Context, cfg loadedConfig, bridgeAddr string) loadedConfig {
	mode := locationclient.ParseMode(cfg.Root.Services.Rust.Location.Mode)
	if mode == locationclient.ModeLegacy {
		return cfg
	}
	if strings.TrimSpace(bridgeAddr) == "" {
		log.Printf("location feed binding hydration skipped: location service bridge is unavailable")
		return cfg
	}
	bindings := map[string]*configuredLocationBinding{}
	add := func(target *string, source string, purpose string, value string) {
		if target == nil || strings.HasPrefix(strings.ToLower(strings.TrimSpace(*target)), "urn:haze:location:") {
			return
		}
		value = strings.TrimSpace(value)
		input, ok := locationclient.ConfiguredIdentifier(source, purpose, value)
		if !ok {
			return
		}
		key := strings.ToLower(input.Authority) + "\x00" + input.Scheme + "\x00" + strings.ToUpper(value)
		binding := bindings[key]
		if binding == nil {
			binding = &configuredLocationBinding{input: input}
			bindings[key] = binding
		}
		binding.targets = append(binding.targets, target)
	}
	for feedIndex := range cfg.Feeds {
		feed := &cfg.Feeds[feedIndex]
		for index := range feed.Locations.Coverage.Regions {
			region := &feed.Locations.Coverage.Regions[index]
			add(&region.CanonicalID, region.Source, "coverage", region.ID)
			add(&region.ForecastCanonicalID, region.Source, "forecast", region.DeriveForecast)
		}
		addLocations := func(locations []locationXML, purpose string) {
			for index := range locations {
				add(&locations[index].CanonicalID, locations[index].Source, purpose, locations[index].ID)
			}
		}
		addLocations(feed.Locations.ObservationLocations.Locations, "observation")
		addLocations(feed.Locations.AviationReportLocations.Locations, "aviation")
		addLocations(feed.Locations.AirQualityLocations.Locations, "air_quality")
		addLocations(feed.Locations.ClimateLocations.Locations, "climate")
		addLocations(feed.Locations.MarineForecastLocations.Locations, "marine_forecast")
		addLocations(feed.Locations.MarineForecastLocations.Subregions, "marine_forecast")
		addLocations(feed.Locations.MarineConditions.Locations, "marine_observation")
		addLocations(feed.Locations.HydrometricLocations.Locations, "hydrometric")
		addLocations(feed.Locations.HydrometricLocations.Upstream.Locations, "hydrometric")
		addLocations(feed.Locations.HydrometricLocations.Downstream.Locations, "hydrometric")
	}
	keys := make([]string, 0, len(bindings))
	for key := range bindings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	ordered := make([]*configuredLocationBinding, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, bindings[key])
	}
	client := locationclient.New(bridgeAddr, "haze-data-ingest-location")
	client.Timeout = 5 * time.Second
	resolved := 0
	ambiguous := 0
	started := time.Now()
	for start := 0; start < len(ordered); start += 100 {
		end := start + 100
		if end > len(ordered) {
			end = len(ordered)
		}
		inputs := make([]locationclient.Input, 0, end-start)
		for _, binding := range ordered[start:end] {
			inputs = append(inputs, binding.input)
		}
		queryCtx, cancel := context.WithTimeout(ctx, client.Timeout)
		response, err := client.Query(queryCtx, locationclient.Request{
			Operation: "batch_resolve",
			Inputs:    inputs,
			Options: locationclient.Options{
				Limit:                  4,
				MinimumConfidence:      "high",
				StationModeRequirement: "any",
			},
		})
		cancel()
		if err != nil {
			log.Printf("location feed binding hydration failed: %v", err)
			continue
		}
		for _, batch := range response.Batches {
			bindingIndex := start + batch.InputIndex
			if bindingIndex < start || bindingIndex >= end {
				continue
			}
			if batch.Status == "ambiguous" || len(batch.Results) != 1 {
				if batch.Status == "ambiguous" || len(batch.Results) > 1 {
					ambiguous++
				}
				continue
			}
			candidate := batch.Results[0]
			confidence := strings.ToLower(strings.TrimSpace(candidate.Match.Confidence))
			if confidence != "exact" && confidence != "high" {
				continue
			}
			if mode == locationclient.ModeAuthoritative {
				for _, target := range ordered[bindingIndex].targets {
					*target = candidate.Entity.ID
				}
				resolved++
			}
		}
	}
	log.Printf(
		"location feed binding hydration mode=%s bindings=%d resolved=%d ambiguous=%d latency_ms=%d",
		mode,
		len(ordered),
		resolved,
		ambiguous,
		time.Since(started).Milliseconds(),
	)
	return cfg
}
