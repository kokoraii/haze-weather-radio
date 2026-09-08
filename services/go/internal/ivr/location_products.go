package ivr

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

// These are the on-demand products that can be selected for a location in the
// public directory. The catalog still controls whether each product is enabled.
var publicLocationProducts = map[string]struct{}{
	"alerts":               {},
	"air_quality":          {},
	"climate_summary":      {},
	"current_conditions":   {},
	"forecast":             {},
	"hydrometric":          {},
	"thunderstorm_outlook": {},
}

type locationProductCatalogXML struct {
	Defaults struct {
		Enabled string `xml:"enabled"`
	} `xml:"defaults"`
	Products []locationProductCatalogEntry `xml:"product"`
	Packages []locationProductCatalogEntry `xml:"package"`
}

type locationProductCatalogEntry struct {
	ID         string `xml:"id,attr"`
	EnabledRaw string `xml:"enabled,attr"`
}

// locationCodesWithProducts builds the public directory once per service
// connection. The enabled-product catalog and capability index only change on
// configuration reload, which already requires a service restart.
func (s *Service) locationCodesWithProducts() []telephoneLocationCode {
	if s == nil || s.resolver == nil {
		return nil
	}
	s.locationCodesOnce.Do(func() {
		base := s.resolver.telephoneLocationCodes()
		locations := make([]telephoneLocationCode, len(base))
		copy(locations, base)
		enabled := s.onDemandLocationProductIDs()
		for index := range locations {
			locations[index].AvailableProducts = s.availableProductsForLocation(locations[index], enabled)
		}
		s.locationCodes = locations
	})
	return s.locationCodes
}

func (s *Service) onDemandLocationProductIDs() []string {
	productsPath := strings.TrimSpace(s.cfg.Root.ProductsFile)
	if productsPath == "" {
		productsPath = "managed/configs/products.xml"
	}
	ids, err := locationProductIDsFromFile(resolvePath(s.cfg.BaseDir, productsPath))
	if err == nil {
		return filterPublicLocationProducts(ids)
	}
	if !os.IsNotExist(err) {
		return nil
	}

	packagesPath := strings.TrimSpace(s.cfg.Root.PackagesFile)
	if packagesPath == "" {
		packagesPath = "managed/configs/packages.xml"
	}
	ids, err = locationProductIDsFromFile(resolvePath(s.cfg.BaseDir, packagesPath))
	if err != nil {
		return nil
	}
	return filterPublicLocationProducts(ids)
}

func locationProductIDsFromFile(path string) ([]string, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var catalog locationProductCatalogXML
	if err := xml.Unmarshal(raw, &catalog); err != nil {
		return nil, err
	}
	defaultEnabled := xmlBool(catalog.Defaults.Enabled, true)
	seen := make(map[string]struct{})
	ids := make([]string, 0, len(catalog.Products)+len(catalog.Packages))
	for _, entry := range append(catalog.Products, catalog.Packages...) {
		id := strings.ToLower(strings.TrimSpace(entry.ID))
		if id == "" || !xmlBool(entry.EnabledRaw, defaultEnabled) {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}

func filterPublicLocationProducts(ids []string) []string {
	products := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, supported := publicLocationProducts[id]; supported {
			products = append(products, id)
		}
	}
	return products
}

func (s *Service) availableProductsForLocation(location telephoneLocationCode, enabled []string) []string {
	if len(enabled) == 0 {
		return []string{}
	}
	if s.capabilities == nil {
		return append([]string(nil), enabled...)
	}
	if location.Latitude == nil || location.Longitude == nil {
		return []string{}
	}
	latitude, longitude := *location.Latitude, *location.Longitude
	has := func(kind string, radiusKM float64) bool {
		return len(s.capabilities.Nearest(kind, latitude, longitude, location.Province, 1, radiusKM)) > 0
	}
	forecast := strings.TrimSpace(location.ForecastID) != "" || has(locationdb.CapabilityForecast, forecastSearchRadiusKM)

	products := make([]string, 0, len(enabled))
	for _, id := range enabled {
		switch id {
		case "current_conditions":
			if has(locationdb.CapabilityObservation, observationSearchRadiusKM) {
				products = append(products, id)
			}
		case "forecast", "alerts", "thunderstorm_outlook":
			if forecast {
				products = append(products, id)
			}
		case "air_quality":
			if has(locationdb.CapabilityAirQuality, airQualitySearchRadiusKM) {
				products = append(products, id)
			}
		case "climate_summary":
			if has(locationdb.CapabilityClimate, climateSearchRadiusKM) {
				products = append(products, id)
			}
		case "hydrometric":
			if has(locationdb.CapabilityHydrometric, hydrometricSearchRadiusKM) {
				products = append(products, id)
			}
		}
	}
	return products
}
