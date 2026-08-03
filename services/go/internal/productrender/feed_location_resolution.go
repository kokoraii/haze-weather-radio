package productrender

import (
	"context"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
)

type renderFeedLocationBinding struct {
	input   locationclient.Input
	targets []*string
}

func hydrateRenderFeedLocations(ctx context.Context, cfg loadedConfig, bridgeAddr string) loadedConfig {
	mode := locationclient.ParseMode(cfg.Root.Services.Rust.Location.Mode)
	if mode == locationclient.ModeLegacy {
		return cfg
	}
	bridgeAddr = strings.TrimSpace(bridgeAddr)
	if bridgeAddr == "" {
		bridgeAddr = strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	}
	if bridgeAddr == "" {
		return cfg
	}
	bindings := map[string]*renderFeedLocationBinding{}
	add := func(target *string, source string, purpose string, value string) {
		if target == nil || strings.HasPrefix(strings.ToLower(strings.TrimSpace(*target)), "urn:haze:location:") {
			return
		}
		input, ok := locationclient.ConfiguredIdentifier(source, purpose, value)
		if !ok {
			return
		}
		key := strings.ToLower(input.Authority) + "\x00" + input.Scheme + "\x00" + strings.ToUpper(input.Value)
		binding := bindings[key]
		if binding == nil {
			binding = &renderFeedLocationBinding{input: input}
			bindings[key] = binding
		}
		binding.targets = append(binding.targets, target)
	}
	for feedIndex := range cfg.Feeds {
		feed := &cfg.Feeds[feedIndex]
		for regionIndex := range feed.Locations.Coverage.Regions {
			region := &feed.Locations.Coverage.Regions[regionIndex]
			add(&region.CanonicalID, region.Source, "coverage", region.ID)
			add(&region.ForecastCanonicalID, region.Source, "forecast", region.DeriveForecast)
			for subregionIndex := range region.Subregions {
				add(&region.Subregions[subregionIndex].CanonicalID, region.Source, "coverage", region.Subregions[subregionIndex].ID)
			}
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
	ordered := make([]*renderFeedLocationBinding, 0, len(keys))
	for _, key := range keys {
		ordered = append(ordered, bindings[key])
	}
	client := locationclient.New(bridgeAddr, "haze-product-render-feed-location")
	client.Timeout = 5 * time.Second
	resolved := 0
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
			log.Printf("product render feed location hydration failed: %v", err)
			continue
		}
		for _, batch := range response.Batches {
			index := start + batch.InputIndex
			if index < start || index >= end || batch.Status == "ambiguous" || len(batch.Results) != 1 {
				continue
			}
			candidate := batch.Results[0]
			confidence := strings.ToLower(strings.TrimSpace(candidate.Match.Confidence))
			if confidence != "exact" && confidence != "high" {
				continue
			}
			if mode == locationclient.ModeAuthoritative {
				for _, target := range ordered[index].targets {
					*target = candidate.Entity.ID
				}
				resolved++
			}
		}
	}
	log.Printf("product render feed location hydration mode=%s bindings=%d resolved=%d", mode, len(ordered), resolved)
	return cfg
}

func renderFeedCanonicalLocationIDs(feed feedXML) []string {
	ids := []string{}
	for _, region := range feed.Locations.Coverage.Regions {
		ids = append(ids, region.CanonicalID, region.ForecastCanonicalID)
		for _, subregion := range region.Subregions {
			ids = append(ids, subregion.CanonicalID)
		}
	}
	groups := [][]locationXML{
		feed.Locations.ObservationLocations.Locations,
		feed.Locations.AviationReportLocations.Locations,
		feed.Locations.AirQualityLocations.Locations,
		feed.Locations.ClimateLocations.Locations,
		feed.Locations.MarineForecastLocations.Locations,
		feed.Locations.MarineForecastLocations.Subregions,
		feed.Locations.MarineConditions.Locations,
		feed.Locations.HydrometricLocations.Locations,
		feed.Locations.HydrometricLocations.Upstream.Locations,
		feed.Locations.HydrometricLocations.Downstream.Locations,
	}
	for _, group := range groups {
		for _, location := range group {
			ids = append(ids, location.CanonicalID)
		}
	}
	return uniqueStrings(ids)
}

func inheritRenderFeedCanonicalIDs(current loadedConfig, previous loadedConfig) loadedConfig {
	type canonicalPair struct {
		entity   string
		forecast string
	}
	previousRegions := map[string]canonicalPair{}
	previousCoverageSubregions := map[string]string{}
	previousLocations := map[string]string{}
	for _, feed := range previous.Feeds {
		for _, region := range feed.Locations.Coverage.Regions {
			key := strings.ToLower(strings.TrimSpace(feed.ID)) + "\x00" + strings.ToLower(strings.TrimSpace(region.Source)) + "\x00" + strings.TrimSpace(region.ID)
			previousRegions[key] = canonicalPair{entity: region.CanonicalID, forecast: region.ForecastCanonicalID}
			for _, subregion := range region.Subregions {
				subregionKey := key + "\x00" + strings.TrimSpace(subregion.ID)
				previousCoverageSubregions[subregionKey] = subregion.CanonicalID
			}
		}
		collect := func(locations []locationXML, purpose string) {
			for _, location := range locations {
				key := strings.ToLower(strings.TrimSpace(feed.ID)) + "\x00" + purpose + "\x00" + strings.ToLower(strings.TrimSpace(location.Source)) + "\x00" + strings.TrimSpace(location.ID)
				previousLocations[key] = location.CanonicalID
			}
		}
		collect(feed.Locations.ObservationLocations.Locations, "observation")
		collect(feed.Locations.AviationReportLocations.Locations, "aviation")
		collect(feed.Locations.AirQualityLocations.Locations, "air_quality")
		collect(feed.Locations.ClimateLocations.Locations, "climate")
		collect(feed.Locations.MarineForecastLocations.Locations, "marine_forecast")
		collect(feed.Locations.MarineForecastLocations.Subregions, "marine_forecast_subregion")
		collect(feed.Locations.MarineConditions.Locations, "marine_observation")
		collect(feed.Locations.HydrometricLocations.Locations, "hydrometric")
		collect(feed.Locations.HydrometricLocations.Upstream.Locations, "hydrometric_upstream")
		collect(feed.Locations.HydrometricLocations.Downstream.Locations, "hydrometric_downstream")
	}
	for feedIndex := range current.Feeds {
		feed := &current.Feeds[feedIndex]
		for regionIndex := range feed.Locations.Coverage.Regions {
			region := &feed.Locations.Coverage.Regions[regionIndex]
			key := strings.ToLower(strings.TrimSpace(feed.ID)) + "\x00" + strings.ToLower(strings.TrimSpace(region.Source)) + "\x00" + strings.TrimSpace(region.ID)
			if previous, ok := previousRegions[key]; ok {
				if region.CanonicalID == "" {
					region.CanonicalID = previous.entity
				}
				if region.ForecastCanonicalID == "" {
					region.ForecastCanonicalID = previous.forecast
				}
			}
			for subregionIndex := range region.Subregions {
				subregionKey := key + "\x00" + strings.TrimSpace(region.Subregions[subregionIndex].ID)
				if region.Subregions[subregionIndex].CanonicalID == "" {
					region.Subregions[subregionIndex].CanonicalID = previousCoverageSubregions[subregionKey]
				}
			}
		}
		inherit := func(locations []locationXML, purpose string) {
			for index := range locations {
				key := strings.ToLower(strings.TrimSpace(feed.ID)) + "\x00" + purpose + "\x00" + strings.ToLower(strings.TrimSpace(locations[index].Source)) + "\x00" + strings.TrimSpace(locations[index].ID)
				if locations[index].CanonicalID == "" {
					locations[index].CanonicalID = previousLocations[key]
				}
			}
		}
		inherit(feed.Locations.ObservationLocations.Locations, "observation")
		inherit(feed.Locations.AviationReportLocations.Locations, "aviation")
		inherit(feed.Locations.AirQualityLocations.Locations, "air_quality")
		inherit(feed.Locations.ClimateLocations.Locations, "climate")
		inherit(feed.Locations.MarineForecastLocations.Locations, "marine_forecast")
		inherit(feed.Locations.MarineForecastLocations.Subregions, "marine_forecast_subregion")
		inherit(feed.Locations.MarineConditions.Locations, "marine_observation")
		inherit(feed.Locations.HydrometricLocations.Locations, "hydrometric")
		inherit(feed.Locations.HydrometricLocations.Upstream.Locations, "hydrometric_upstream")
		inherit(feed.Locations.HydrometricLocations.Downstream.Locations, "hydrometric_downstream")
	}
	return current
}
