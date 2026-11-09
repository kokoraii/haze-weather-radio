package productrender

import (
	"log"

	"github.com/meowraii/haze-weather-radio/services/go/internal/feedconfig"
)

// applyFeedConfig replaces each feed's coverage with the daemon's validated
// coverage, so product render evaluates the same regions as every other
// service. A feed the snapshot does not list is left out until it does:
// empty coverage would mean "every location", which a missing feed must not
// get by accident.
//
// The rest of product render still reads coverage through
// feedXML.Locations.Coverage; only its source changed.
func applyFeedConfig(cfg loadedConfig, snapshot *feedconfig.Snapshot) loadedConfig {
	feeds := make([]feedXML, 0, len(cfg.Feeds))
	for _, feed := range cfg.Feeds {
		coverage, ok := snapshot.Feed(feed.ID)
		if !ok {
			log.Printf("product render: feed %s is not in the daemon's feed config yet; skipping it", feed.ID)
			continue
		}
		feed.Locations.Coverage.Regions = coverageRegions(coverage)
		feeds = append(feeds, feed)
	}
	cfg.Feeds = feeds
	cfg.FeedConfigGeneration = snapshot.Generation
	return cfg
}

func coverageRegions(feed feedconfig.Feed) []coverageRegionXML {
	regions := make([]coverageRegionXML, 0, len(feed.Entries))
	for _, entry := range feed.Entries {
		region := coverageRegionXML{
			// A wildcard with a region code (0655*) reads as its region (065500),
			// which product render already treats as covering 0655xx. A bare
			// pattern (06551*) stays a pattern, which coverage matching accepts.
			ID:             entry.ID,
			Source:         entry.Source,
			Name:           entry.Name,
			DeriveForecast: entry.DeriveForecast,
		}
		if entry.RegionCode != "" {
			region.ID = entry.RegionCode
		}
		for _, sub := range entry.Subregions {
			id := sub.Value
			if sub.Kind == feedconfig.MatchPrefix {
				id += "*"
			}
			region.Subregions = append(region.Subregions, coverageSubregionXML{ID: id})
		}
		regions = append(regions, region)
	}
	return regions
}
