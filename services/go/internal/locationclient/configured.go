package locationclient

import (
	"regexp"
	"strings"
)

var configuredCitypagePattern = regexp.MustCompile(`(?i)^[a-z]{2}-\d+$`)
var configuredICAOPattern = regexp.MustCompile(`(?i)^[a-z]{4}$`)
var configuredIATAPattern = regexp.MustCompile(`(?i)^[a-z]{3}$`)
var configuredMarineMSCIDPattern = regexp.MustCompile(`^\d{5,8}$`)
var configuredVirtualClimatePattern = regexp.MustCompile(`(?i)^vs[a-z]{2}[a-z0-9]*v$`)

// ConfiguredIdentifier translates a feed location purpose into the same
// qualified namespace used by haze-location. It deliberately does not use a
// bare auto lookup, because configured feed identifiers are unattended input.
func ConfiguredIdentifier(source string, purpose string, value string) (Input, bool) {
	source = strings.ToLower(strings.TrimSpace(source))
	purpose = strings.ToLower(strings.TrimSpace(purpose))
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, "*") {
		return Input{}, false
	}
	scheme := ""
	authority := source
	switch {
	case purpose == "coverage":
		if source == "nws" {
			if len(value) >= 3 && strings.ToUpper(value)[2] == 'C' {
				scheme = "nws_ugc_county"
			} else {
				scheme = "nws_zone"
			}
			authority = "nws"
		} else {
			scheme, authority = "clc", "eccc"
		}
	case purpose == "forecast":
		if source == "nws" {
			scheme, authority = "nws_zone", "nws"
		} else {
			scheme, authority = "eccc_citypage", "eccc"
		}
	case purpose == "aviation":
		switch {
		case configuredICAOPattern.MatchString(value):
			scheme, authority = "icao", "icao"
		case configuredIATAPattern.MatchString(value):
			scheme, authority = "iata", "iata"
		default:
			scheme, authority = "eccc_station", "eccc"
		}
	case purpose == "air_quality":
		if source == "nws" || source == "epa" {
			scheme, authority = "epa_aqs", "epa"
		} else {
			scheme, authority = "aqhi", "eccc"
		}
	case purpose == "climate":
		if configuredVirtualClimatePattern.MatchString(value) {
			scheme, authority = "virtual_climate", "eccc"
		} else {
			scheme = "climate"
		}
	case purpose == "marine_forecast":
		if source == "nws" {
			scheme, authority = "nws_marine_zone", "nws"
		} else {
			scheme, authority = "marine", "eccc"
		}
	case purpose == "marine_observation":
		switch {
		case source == "ndbc" || source == "nws":
			scheme, authority = "ndbc", "ndbc"
		case configuredICAOPattern.MatchString(value):
			scheme, authority = "icao", "icao"
		case configuredMarineMSCIDPattern.MatchString(value):
			scheme, authority = "msc", "eccc"
		default:
			scheme, authority = "eccc_station", "eccc"
		}
	case purpose == "hydrometric":
		scheme = "hydrometric"
	case purpose == "observation" && source == "eccc" && configuredCitypagePattern.MatchString(value):
		scheme, authority = "eccc_citypage", "eccc"
	case purpose == "observation" && configuredICAOPattern.MatchString(value):
		scheme, authority = "icao", "icao"
	case purpose == "observation":
		scheme = "eccc_station"
	}
	if scheme == "" {
		return Input{}, false
	}
	return Input{Kind: "identifier", Scheme: scheme, Authority: authority, Value: value}, true
}
