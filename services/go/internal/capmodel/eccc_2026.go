package capmodel

import (
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	ECCCThreatAreaGeocodeName = "layer:EC-MSC-SMC:DLC:1.1"

	ecccStormSpeedName             = "layer:EC-MSC-SMC:1.1:Storm_Speed"
	ecccStormDirectionName         = "layer:EC-MSC-SMC:1.1:Storm_Direction"
	ecccStormGeometryTypeName      = "layer:EC-MSC-SMC:1.1:Storm_Geometry_Type"
	ecccStormPointName             = "layer:EC-MSC-SMC:1.1:Storm_Point"
	ecccStormTimeName              = "layer:EC-MSC-SMC:1.1:Storm_Time"
	ecccMotionDescriptionName      = "layer:EC-MSC-SMC:1.1:Motion_Description"
	ecccStormPositionName          = "layer:EC-MSC-SMC:1.1:Storm_Position_Description"
	ecccReferenceLocationPointName = "layer:EC-MSC-SMC:1.1:Reference_Location_Points"
)

var ecccThreatStatuses = map[string]struct{}{
	"issued":    {},
	"continued": {},
	"ended":     {},
	"cancelled": {},
}

// IsECCCStormParameter reports whether a parameter belongs to the additive
// August 2026 storm-characteristics set. These values are descriptive metadata,
// not alert routing identifiers.
func IsECCCStormParameter(name string) bool {
	name = strings.TrimSpace(name)
	for _, expected := range []string{
		ecccStormSpeedName,
		ecccStormDirectionName,
		ecccStormGeometryTypeName,
		ecccStormPointName,
		ecccStormTimeName,
		ecccMotionDescriptionName,
		ecccStormPositionName,
		ecccReferenceLocationPointName,
	} {
		if strings.EqualFold(name, expected) {
			return true
		}
	}
	return false
}

// IsECCCThreatAreaGeocode reports whether a CAP geocode is the ECCC free-form
// threat-area discriminator introduced in August 2026.
func IsECCCThreatAreaGeocode(value NameValue) bool {
	return strings.EqualFold(strings.TrimSpace(value.Name), ECCCThreatAreaGeocodeName)
}

// ECCCThreatAreaStatus returns the normalized DLC status. Unknown values are
// retained so callers can fail closed while the raw geocode remains available.
func ECCCThreatAreaStatus(area AlertArea) string {
	if status := strings.ToLower(strings.TrimSpace(area.ThreatStatus)); status != "" {
		return status
	}
	for _, geocode := range area.Geocodes {
		if IsECCCThreatAreaGeocode(geocode) {
			return strings.ToLower(strings.TrimSpace(geocode.Value))
		}
	}
	return ""
}

// IsECCCThreatArea reports whether an area is a DLC free-form threat area.
func IsECCCThreatArea(area AlertArea) bool {
	return ECCCThreatAreaStatus(area) != ""
}

// IsECCCActiveThreatArea reports whether the threat geometry is currently
// issued or continued. Ended and cancelled polygons are historical geometry.
func IsECCCActiveThreatArea(area AlertArea) bool {
	switch ECCCThreatAreaStatus(area) {
	case "issued", "continued":
		return true
	default:
		return false
	}
}

func normalizeECCC2026Info(info *AlertInfo) {
	if info == nil {
		return
	}
	for index := range info.Areas {
		info.Areas[index].ThreatStatus = ECCCThreatAreaStatus(info.Areas[index])
	}
	info.Storm = stormInfoFromParameters(info.Parameters)
}

func stormInfoFromParameters(parameters []NameValue) *StormInfo {
	storm := StormInfo{}
	found := false
	for _, parameter := range parameters {
		name := strings.TrimSpace(parameter.Name)
		value := strings.TrimSpace(parameter.Value)
		switch {
		case strings.EqualFold(name, ecccStormSpeedName):
			found = true
			storm.Speed = value
			if number, unit, ok := parseECCCStormSpeed(value); ok {
				storm.SpeedValue = &number
				storm.SpeedUnit = unit
			}
		case strings.EqualFold(name, ecccStormDirectionName):
			found = true
			if number, err := strconv.ParseFloat(value, 64); err == nil && isFinite(number) && number >= 0 && number <= 360 {
				storm.DirectionDegrees = &number
			}
		case strings.EqualFold(name, ecccStormGeometryTypeName):
			found = true
			storm.GeometryType = strings.ToLower(value)
		case strings.EqualFold(name, ecccStormPointName):
			found = true
			for _, rawPoint := range strings.Fields(value) {
				if point, ok := parseECCCStormPoint(rawPoint); ok {
					storm.Points = append(storm.Points, point)
				}
			}
		case strings.EqualFold(name, ecccStormTimeName):
			found = true
			storm.Time = value
		case strings.EqualFold(name, ecccMotionDescriptionName):
			found = true
			storm.MotionDescription = value
		case strings.EqualFold(name, ecccStormPositionName):
			found = true
			storm.PositionDescription = value
		case strings.EqualFold(name, ecccReferenceLocationPointName):
			found = true
			storm.ReferenceLocationPoints = value
		}
	}
	if !found {
		return nil
	}
	return &storm
}

func validateECCC2026Info(info AlertInfo, prefix string) []string {
	warnings := []string{}
	for index, area := range info.Areas {
		status := ECCCThreatAreaStatus(area)
		if status == "" {
			continue
		}
		if _, ok := ecccThreatStatuses[status]; !ok {
			warnings = append(warnings, prefix+".area["+strconv.Itoa(index)+"]: invalid ECCC threat status "+status)
		}
		if len(area.Polygons) == 0 {
			warnings = append(warnings, prefix+".area["+strconv.Itoa(index)+"]: ECCC threat area has no polygon")
		}
	}
	for _, parameter := range info.Parameters {
		name := strings.TrimSpace(parameter.Name)
		value := strings.TrimSpace(parameter.Value)
		switch {
		case strings.EqualFold(name, ecccStormSpeedName):
			if _, _, ok := parseECCCStormSpeed(value); !ok {
				warnings = append(warnings, prefix+": invalid ECCC storm speed")
			}
		case strings.EqualFold(name, ecccStormDirectionName):
			direction, err := strconv.ParseFloat(value, 64)
			if err != nil || !isFinite(direction) || direction < 0 || direction > 360 {
				warnings = append(warnings, prefix+": invalid ECCC storm direction")
			}
		case strings.EqualFold(name, ecccStormGeometryTypeName):
			switch strings.ToLower(value) {
			case "isolated_cell", "area", "line":
			default:
				warnings = append(warnings, prefix+": invalid ECCC storm geometry type")
			}
		case strings.EqualFold(name, ecccStormPointName):
			points := strings.Fields(value)
			if len(points) == 0 {
				warnings = append(warnings, prefix+": invalid ECCC storm point")
				continue
			}
			for _, rawPoint := range points {
				if _, ok := parseECCCStormPoint(rawPoint); !ok {
					warnings = append(warnings, prefix+": invalid ECCC storm point")
					break
				}
			}
		case strings.EqualFold(name, ecccStormTimeName):
			if !validECCCStormTime(value) {
				warnings = append(warnings, prefix+": invalid ECCC storm time")
			}
		}
	}
	return warnings
}

func parseECCCStormSpeed(raw string) (float64, string, bool) {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) != 2 {
		return 0, "", false
	}
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || !isFinite(value) || value < 0 {
		return 0, "", false
	}
	unit := strings.ToLower(strings.TrimSpace(fields[1]))
	if unit != "km/h" && unit != "knots" {
		return 0, "", false
	}
	return value, unit, true
}

func parseECCCStormPoint(raw string) (GeoPoint, bool) {
	parts := strings.Split(strings.TrimSpace(raw), ",")
	if len(parts) != 2 {
		return GeoPoint{}, false
	}
	latitude, errLatitude := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	longitude, errLongitude := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if errLatitude != nil || errLongitude != nil || !isFinite(latitude) || !isFinite(longitude) || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180 {
		return GeoPoint{}, false
	}
	return GeoPoint{Latitude: latitude, Longitude: longitude}, true
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validECCCStormTime(raw string) bool {
	raw = strings.TrimSpace(raw)
	if len(raw) != 17 {
		return false
	}
	for _, character := range raw {
		if character < '0' || character > '9' {
			return false
		}
	}
	if _, err := time.Parse("20060102150405", raw[:14]); err != nil {
		return false
	}
	milliseconds, err := strconv.Atoi(raw[14:])
	return err == nil && milliseconds >= 0 && milliseconds <= 999
}
