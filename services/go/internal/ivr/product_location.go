package ivr

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

const (
	observationSearchRadiusKM = 200.0
	observationChoiceRadiusKM = 60.0
	observationChoiceLeadKM   = 20.0
	forecastSearchRadiusKM    = 300.0
	airQualitySearchRadiusKM  = 350.0
	hydrometricSearchRadiusKM = 200.0
	marineSearchRadiusKM      = 300.0
	climateSearchRadiusKM     = 250.0
	maxObservationChoices     = 9
)

func (s *Service) observationStationChoices(location ResolvedLocation) []locationdb.CapabilityMatch {
	latitude, longitude, ok := resolvedCoordinates(location)
	if !ok || s == nil || s.capabilities == nil {
		return nil
	}
	matches := s.capabilities.Nearest(locationdb.CapabilityObservation, latitude, longitude, location.Province, maxObservationChoices, observationSearchRadiusKM)
	if len(matches) <= 1 {
		return matches
	}
	cutoff := matches[0].DistanceKM + observationChoiceLeadKM
	if cutoff > observationChoiceRadiusKM {
		cutoff = observationChoiceRadiusKM
	}
	choices := make([]locationdb.CapabilityMatch, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		key := strings.ToUpper(strings.TrimSpace(match.Location.ID))
		if key == "" || match.DistanceKM > cutoff {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		choices = append(choices, match)
	}
	if len(choices) == 0 {
		return matches[:1]
	}
	return choices
}

func (s *Service) mapProductLocation(location ResolvedLocation, packages []string, observation *locationdb.CapabilityMatch) ResolvedLocation {
	if s == nil || s.capabilities == nil {
		return location
	}
	latitude, longitude, ok := resolvedCoordinates(location)
	if !ok {
		return location
	}
	requested := make(map[string]struct{})
	for _, packageID := range packages {
		requested[strings.ToLower(strings.TrimSpace(packageID))] = struct{}{}
	}
	has := func(packageID string) bool {
		_, exists := requested[packageID]
		return exists
	}

	if has("current_conditions") || has("aviation_reports") {
		match := observation
		if match == nil {
			choices := s.observationStationChoices(location)
			if len(choices) > 0 {
				match = &choices[0]
			}
		}
		if match != nil {
			location.StationID = match.Location.ID
			location.Latitude = floatText(match.Location.Latitude)
			location.Longitude = floatText(match.Location.Longitude)
		}
	}

	if has("forecast") || has("thunderstorm_outlook") {
		if matches := s.capabilities.Nearest(locationdb.CapabilityForecast, latitude, longitude, location.Province, 1, forecastSearchRadiusKM); len(matches) > 0 {
			location.Forecast = matches[0].Location.ID
		}
	}
	if has("air_quality") {
		if matches := s.capabilities.Nearest(locationdb.CapabilityAirQuality, latitude, longitude, location.Province, 1, airQualitySearchRadiusKM); len(matches) > 0 {
			location.AirQualityID = matches[0].Location.ID
		}
	}
	if has("hydrometric") {
		if matches := s.capabilities.Nearest(locationdb.CapabilityHydrometric, latitude, longitude, location.Province, 1, hydrometricSearchRadiusKM); len(matches) > 0 {
			location.HydrometricID = matches[0].Location.ID
		}
	}
	if has("marine_forecast") {
		if matches := s.capabilities.Nearest(locationdb.CapabilityMarineForecast, latitude, longitude, location.Province, 1, marineSearchRadiusKM); len(matches) > 0 {
			location.MarineForecastID = matches[0].Location.ID
		}
	}
	return location
}

func applyObservationChoice(location ResolvedLocation, match locationdb.CapabilityMatch) ResolvedLocation {
	location.StationID = match.Location.ID
	location.Latitude = floatText(match.Location.Latitude)
	location.Longitude = floatText(match.Location.Longitude)
	return location
}

func localizedObservationChoicesPrompt(language string, choices []locationdb.CapabilityMatch) string {
	french := strings.HasPrefix(strings.ToLower(strings.TrimSpace(language)), "fr")
	parts := make([]string, 0, len(choices)+1)
	for index, choice := range choices {
		name := strings.TrimSpace(choice.Location.Name)
		if french && strings.TrimSpace(choice.Location.NameFR) != "" {
			name = strings.TrimSpace(choice.Location.NameFR)
		}
		if name == "" {
			name = choice.Location.ID
		}
		if french {
			parts = append(parts, fmt.Sprintf("Appuyez sur %d pour %s.", index+1, name))
		} else {
			parts = append(parts, fmt.Sprintf("Press %d for %s.", index+1, name))
		}
	}
	if french {
		parts = append(parts, "Appuyez sur le carré pour revenir.")
	} else {
		parts = append(parts, "Press pound to go back.")
	}
	return strings.Join(parts, " ")
}

func resolvedCoordinates(location ResolvedLocation) (float64, float64, bool) {
	latitude, latErr := strconv.ParseFloat(strings.TrimSpace(location.Latitude), 64)
	longitude, lonErr := strconv.ParseFloat(strings.TrimSpace(location.Longitude), 64)
	if latErr != nil || lonErr != nil || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 || latitude == 0 && longitude == 0 {
		return 0, 0, false
	}
	return latitude, longitude, true
}

func packagesInclude(packages []string, wanted string) bool {
	wanted = strings.ToLower(strings.TrimSpace(wanted))
	for _, packageID := range packages {
		if strings.ToLower(strings.TrimSpace(packageID)) == wanted {
			return true
		}
	}
	return false
}

func (s *Service) writeObservationChoiceTwiML(writer http.ResponseWriter, request *http.Request, location ResolvedLocation, packages []string) {
	choices := s.observationStationChoices(location)
	if len(choices) <= 1 {
		s.writeProductTwiML(writer, request, s.mapProductLocation(location, packages, nil), packages, "")
		return
	}
	menu, _ := s.cfg.Prompts.Menu("location_menu")
	actionParams := locationTwiMLParams(location)
	actionParams["state"] = "observation_choice"
	actionParams["packages"] = strings.Join(normalizePackages(packages, s.cfg.IVR.DefaultPackages), ",")
	audioParams := locationTwiMLParams(location)
	audioParams["kind"] = "observation_choices"
	returnParams := locationTwiMLParams(location)
	returnParams["state"] = "location_menu"
	body := twimlGather(
		twimlURL(request, "/ivr/v1/twiml", actionParams),
		"1",
		"#",
		menu.Timeout,
		[]string{twimlURL(request, "/ivr/v1/alert_audio", audioParams)},
		[]string{twimlRedirect(twimlURL(request, "/ivr/v1/twiml", returnParams))},
	)
	writeTwiML(writer, body)
}

func (s *Service) handleObservationChoiceTwiML(writer http.ResponseWriter, request *http.Request) {
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	digit := strings.TrimSpace(request.FormValue("Digits"))
	if digit == "" || digit == "#" {
		s.writeLocationMenuWithAlertAuto(writer, request, location, false)
		return
	}
	choices := s.observationStationChoices(location)
	index, parseErr := strconv.Atoi(digit)
	if parseErr != nil || index < 1 || index > len(choices) {
		s.writeObservationChoiceTwiML(writer, request, location, packagesFromRequest(request))
		return
	}
	packages := packagesFromRequest(request)
	selected := choices[index-1]
	mapped := s.mapProductLocation(location, packages, &selected)
	returnParams := locationTwiMLParams(location)
	returnParams["state"] = "location_menu"
	returnParams["alert_auto"] = "0"
	s.writeProductTwiML(writer, request, mapped, packages, twimlURL(request, "/ivr/v1/twiml", returnParams))
}

func (c *sipCall) playMappedProduct(location ResolvedLocation, packages []string, allowObservationChoice bool) bool {
	if c == nil || c.service == nil {
		return false
	}
	if allowObservationChoice && packagesInclude(packages, "current_conditions") {
		choices := c.service.observationStationChoices(location)
		if len(choices) > 1 {
			menu, _ := c.service.cfg.Prompts.Menu("location_menu")
			digit, ok := c.promptTextAndWaitDigit(
				"observation_choices_"+firstNonBlank(location.Code, location.FeedID, "default"),
				localizedObservationChoicesPrompt(location.Language, choices),
				menu.Timeout,
			)
			if !ok || digit == "#" {
				return false
			}
			index, err := strconv.Atoi(digit)
			if err != nil || index < 1 || index > len(choices) {
				c.playPrompt("error", "invalid_code", nil)
				return false
			}
			selected := choices[index-1]
			location = c.service.mapProductLocation(location, packages, &selected)
			return c.playProduct(location, packages)
		}
	}
	location = c.service.mapProductLocation(location, packages, nil)
	return c.playProduct(location, packages)
}
