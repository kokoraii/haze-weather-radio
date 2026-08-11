package productrender

import (
	"strings"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
)

func capPacketThreatAreas(info capmodel.AlertInfo) []alertmodel.ThreatArea {
	areas := []alertmodel.ThreatArea{}
	for _, area := range info.Areas {
		status := capmodel.ECCCThreatAreaStatus(area)
		if status == "" || len(area.Polygons) == 0 {
			continue
		}
		locations := []string{}
		for _, geocode := range area.Geocodes {
			if strings.Contains(strings.ToLower(strings.TrimSpace(geocode.Name)), "profile:cap-cp:location:") {
				locations = append(locations, strings.TrimSpace(geocode.Value))
			}
		}
		areas = append(areas, alertmodel.ThreatArea{
			Status:         status,
			Description:    strings.TrimSpace(area.Description),
			Polygons:       append([]string(nil), area.Polygons...),
			CAPCPLocations: uniqueStrings(locations),
		})
	}
	return areas
}

func capPacketStormInfo(info capmodel.AlertInfo) *alertmodel.StormInfo {
	if info.Storm == nil {
		return nil
	}
	storm := &alertmodel.StormInfo{
		Speed:                   info.Storm.Speed,
		SpeedUnit:               info.Storm.SpeedUnit,
		GeometryType:            info.Storm.GeometryType,
		Time:                    info.Storm.Time,
		MotionDescription:       info.Storm.MotionDescription,
		PositionDescription:     info.Storm.PositionDescription,
		ReferenceLocationPoints: info.Storm.ReferenceLocationPoints,
	}
	if info.Storm.SpeedValue != nil {
		value := *info.Storm.SpeedValue
		storm.SpeedValue = &value
	}
	if info.Storm.DirectionDegrees != nil {
		value := *info.Storm.DirectionDegrees
		storm.DirectionDegrees = &value
	}
	for _, point := range info.Storm.Points {
		storm.Points = append(storm.Points, alertmodel.GeoPoint{
			Latitude:  point.Latitude,
			Longitude: point.Longitude,
		})
	}
	return storm
}
