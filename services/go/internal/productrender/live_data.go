package productrender

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type liveObservationFile struct {
	Source     string `json:"source"`
	ObservedAt string `json:"observed_at"`
	Station    any    `json:"station"`
	StationID  string `json:"station_id"`
	Properties struct {
		Temp         *float64 `json:"temp"`
		Condition    any      `json:"condition"`
		SkyCondition any      `json:"sky_condition"`
		Altimeter    string   `json:"altimeter"`
		Wind         struct {
			Speed     *float64 `json:"speed"`
			Direction string   `json:"direction"`
			Gust      *float64 `json:"gust"`
		} `json:"wind"`
		Humidity   *float64 `json:"humidity"`
		Dewpoint   *float64 `json:"dewpoint"`
		Visibility *float64 `json:"visibility"`
		Pressure   struct {
			Value    *float64 `json:"value"`
			Tendency any      `json:"tendency"`
		} `json:"pressure"`
	} `json:"properties"`
}

type liveForecastFile struct {
	Source         string `json:"source"`
	ForecastRegion string `json:"forecast_region"`
	Name           any    `json:"name"`
	IssuedAt       string `json:"issued_at"`
	UpdatedAt      string `json:"updated_at"`
	PublishedAt    string `json:"published_at"`
	LastUpdated    string `json:"last_updated"`
	Forecast       []struct {
		Period      any `json:"period"`
		TextSummary any `json:"textSummary"`
	} `json:"forecast"`
}

type liveAirQualityFile struct {
	Source     string `json:"source"`
	Location   any    `json:"location"`
	ObservedAt string `json:"observed_at"`
	AQHI       any    `json:"aqhi"`
	Forecast   struct {
		PublishedAt string `json:"published_at"`
		Periods     []struct {
			Period      any `json:"period"`
			AQHI        any `json:"aqhi"`
			AQHIInSmoke any `json:"aqhi_insmoke"`
		} `json:"periods"`
	} `json:"forecast"`
	SpecialNotes any `json:"special_notes"`
}

type liveClimateFile struct {
	Source       string `json:"source"`
	Name         any    `json:"name"`
	LastUpdated  string `json:"last_updated"`
	Observations struct {
		Station            any      `json:"station"`
		Date               string   `json:"date"`
		High               *float64 `json:"high"`
		Low                *float64 `json:"low"`
		Mean               *float64 `json:"mean"`
		Precipitation      *float64 `json:"precipitation"`
		PrecipitationTrace bool     `json:"precipitation_trace"`
		Rain               *float64 `json:"rain"`
		RainTrace          bool     `json:"rain_trace"`
		Snowfall           *float64 `json:"snowfall"`
		SnowfallTrace      bool     `json:"snowfall_trace"`
		SnowOnGround       *float64 `json:"snow_on_ground"`
		MaxGustSpeed       *float64 `json:"max_gust_speed"`
		MaxGustDirection   string   `json:"max_gust_direction"`
		HeatingDegreeDays  *float64 `json:"heating_degree_days"`
		CoolingDegreeDays  *float64 `json:"cooling_degree_days"`
		MinHumidity        *float64 `json:"min_humidity"`
	} `json:"observations"`
	Normals struct {
		TextSummary any    `json:"textSummary"`
		Station     any    `json:"station"`
		Month       int    `json:"month"`
		Period      string `json:"period"`
		Temperature struct {
			High *float64 `json:"high"`
			Low  *float64 `json:"low"`
			Mean *float64 `json:"mean"`
		} `json:"temperature"`
		Precipitation *float64 `json:"precipitation"`
		Rainfall      *float64 `json:"rainfall"`
		Snowfall      *float64 `json:"snowfall"`
		WindSpeed     *float64 `json:"wind_speed"`
	} `json:"normals"`
	Records map[string]struct {
		Value *float64 `json:"value"`
		Year  any      `json:"year"`
	} `json:"records"`
	Astronomy struct {
		Sunrise  string `json:"sunrise"`
		Sunset   string `json:"sunset"`
		Timezone string `json:"timezone"`
	} `json:"astronomy"`
}

type liveMarineForecastFile struct {
	ID         string `json:"id"`
	Properties struct {
		LastUpdated string `json:"lastUpdated"`
		Area        struct {
			Value     any `json:"value"`
			Region    any `json:"region"`
			SubRegion any `json:"subRegion"`
		} `json:"area"`
		RegularForecast struct {
			Locations           []liveMarineForecastLocation `json:"locations"`
			IssuedDatetimeUTC   string                       `json:"issuedDatetimeUTC"`
			IssuedDatetimeLocal string                       `json:"issuedDatetimeLocal"`
		} `json:"regularForecast"`
		ExtendedForecast struct {
			Locations           []liveMarineExtendedLocation `json:"locations"`
			IssuedDatetimeUTC   string                       `json:"issuedDatetimeUTC"`
			IssuedDatetimeLocal string                       `json:"issuedDatetimeLocal"`
		} `json:"extendedForecast"`
		WaveForecast struct {
			Locations           []liveMarineForecastLocation `json:"locations"`
			IssuedDatetimeUTC   string                       `json:"issuedDatetimeUTC"`
			IssuedDatetimeLocal string                       `json:"issuedDatetimeLocal"`
		} `json:"waveForecast"`
		Warnings struct {
			Locations []liveMarineForecastLocation `json:"locations"`
		} `json:"warnings"`
	} `json:"properties"`
}

type liveMarineForecastLocation struct {
	Name             any `json:"name"`
	WeatherCondition struct {
		PeriodOfCoverage any `json:"periodOfCoverage"`
		Wind             any `json:"wind"`
		TextSummary      any `json:"textSummary"`
		Value            any `json:"value"`
	} `json:"weatherCondition"`
}

type liveMarineExtendedLocation struct {
	WeatherCondition struct {
		ForecastPeriods []struct {
			Name  any `json:"name"`
			Value any `json:"value"`
		} `json:"forecastPeriods"`
	} `json:"weatherCondition"`
}

type liveSpecialtyProductFile struct {
	Source     string           `json:"source"`
	Collection string           `json:"collection"`
	Title      string           `json:"title"`
	UpdatedAt  string           `json:"updated_at"`
	Items      []map[string]any `json:"items"`
}

func (r renderer) loadLiveObservationSnapshot(feed feedXML, lang string, snapshot *observationSnapshot) (string, bool) {
	return r.loadObservationSnapshotFromLocations(feed.Locations.ObservationLocations.Locations, lang, snapshot)
}

func (r renderer) loadLiveObservationReportSnapshot(feed feedXML, pkgID string, lang string, snapshot *observationSnapshot) (string, bool) {
	var locations []locationXML
	switch strings.ToLower(strings.TrimSpace(pkgID)) {
	case "aviation_reports":
		locations = feed.Locations.AviationReportLocations.Locations
	case "marine_reports":
		locations = feed.Locations.MarineConditions.Locations
	default:
		locations = feed.Locations.ObservationLocations.Locations
	}
	if len(locations) == 0 {
		return "", false
	}
	return r.loadObservationSnapshotFromLocations(locations, lang, snapshot)
}

func (r renderer) loadObservationSnapshotFromLocations(locations []locationXML, lang string, snapshot *observationSnapshot) (string, bool) {
	var inputs []string
	var observations []observation
	for _, loc := range locations {
		obs, path, ok := r.liveObservation(loc, lang)
		if !ok {
			continue
		}
		inputs = append(inputs, path)
		observations = append(observations, obs)
	}
	if len(observations) == 0 {
		return "", false
	}
	*snapshot = observationSnapshot{
		ReportedAt:       newestObservationTime(observations),
		Primary:          observations[0],
		Observations:     observations,
		AreaObservations: observations[1:],
	}
	return strings.Join(inputs, ";"), true
}

func (r renderer) liveObservation(loc locationXML, lang string) (observation, string, bool) {
	var raw liveObservationFile
	if input, ok := r.fetchECCCPointObservation(loc, &raw); ok {
		return observationFromLiveFile(loc, lang, raw), input, true
	}
	if input, ok := r.loadStoreObservation(loc, &raw); ok {
		return observationFromLiveFile(loc, lang, raw), input, true
	}
	if input, ok := r.fetchECCCObservation(loc, &raw); ok {
		return observationFromLiveFile(loc, lang, raw), input, true
	}
	return observation{}, "", false
}

func observationFromLiveFile(loc locationXML, lang string, raw liveObservationFile) observation {
	name := fallbackText(loc.NameOverride, localizedString(raw.Station, lang))
	if name == "" {
		name = strings.TrimSpace(loc.ID)
	}
	id := fallbackText(raw.StationID, loc.ID)
	return observation{
		ID:               id,
		Source:           fallbackText(raw.Source, loc.Source),
		LocationName:     name,
		Condition:        localizedString(raw.Properties.Condition, lang),
		SkyCondition:     localizedString(raw.Properties.SkyCondition, lang),
		TemperatureC:     raw.Properties.Temp,
		DewpointC:        raw.Properties.Dewpoint,
		HumidityPercent:  raw.Properties.Humidity,
		WindDirection:    raw.Properties.Wind.Direction,
		WindSpeedKMH:     raw.Properties.Wind.Speed,
		WindGustKMH:      raw.Properties.Wind.Gust,
		VisibilityKM:     raw.Properties.Visibility,
		PressureKPA:      raw.Properties.Pressure.Value,
		PressureTendency: localizedString(raw.Properties.Pressure.Tendency, lang),
		Altimeter:        raw.Properties.Altimeter,
		ObservedAt:       raw.ObservedAt,
	}
}

func (r renderer) loadLiveForecastSnapshot(feed feedXML, lang string, snapshot *forecastSnapshot) (string, bool) {
	var paths []string
	var regions []forecastRegion
	var issuedAt string
	for _, region := range feed.Locations.Coverage.Regions {
		forecastID := fallbackText(region.DeriveForecast, region.ID)
		raw, input, _ := r.liveForecast(region, forecastID)
		if input == "" {
			continue
		}
		periods := make([]forecastPeriod, 0, len(raw.Forecast))
		for _, item := range raw.Forecast {
			name := localizedString(item.Period, lang)
			text := localizedString(item.TextSummary, lang)
			if name == "" && text == "" {
				continue
			}
			periods = append(periods, forecastPeriod{Name: name, Text: text})
		}
		if len(periods) == 0 {
			continue
		}
		name := r.forecastRegionDisplayName(region, raw.Name, lang)
		paths = append(paths, input)
		regions = append(regions, forecastRegion{Name: name, Periods: periods})
		if issuedAt == "" {
			issuedAt = firstNonBlank(raw.IssuedAt, raw.UpdatedAt, raw.PublishedAt, raw.LastUpdated)
		}
	}
	if len(regions) == 0 {
		return "", false
	}
	*snapshot = forecastSnapshot{IssuedAt: issuedAt, Regions: regions}
	return strings.Join(paths, ";"), true
}

func (r renderer) liveForecast(region coverageRegionXML, forecastID string) (liveForecastFile, string, bool) {
	var raw liveForecastFile
	if input, ok := r.loadStoreForecast(region, forecastID, &raw); ok {
		return raw, input, true
	}
	if input, ok := r.fetchECCCForecast(region, forecastID, &raw); ok {
		return raw, input, true
	}
	return liveForecastFile{}, "", false
}

func (r renderer) forecastRegionDisplayName(region coverageRegionXML, rawName any, lang string) string {
	langShort := strings.ToLower(strings.TrimSpace(lang))
	if len(langShort) > 2 {
		langShort = langShort[:2]
	}
	if names, ok := r.cfg.ForecastNames[forecastRegionBaseCode(region.ID)]; ok {
		if langShort == "fr" {
			if names.French != "" {
				return pauseForecastRegionName(names.French, langShort)
			}
		}
		if names.English != "" {
			return pauseForecastRegionName(names.English, "en")
		}
	}
	if name := localizedString(rawName, lang); name != "" {
		return pauseForecastRegionName(name, langShort)
	}
	return pauseForecastRegionName(fallbackText(region.Name, region.ID), langShort)
}

func (r renderer) loadLiveAirQualitySnapshot(feed feedXML, lang string, snapshot *airQualitySnapshot) (string, bool) {
	for _, loc := range feed.Locations.AirQualityLocations.Locations {
		var raw liveAirQualityFile
		input, ok := r.loadStoreProductPayload("air_quality", loc.Source, loc.ID, &raw)
		if !ok {
			input, ok = r.fetchLiveAQHI(loc, &raw)
		}
		if !ok {
			continue
		}
		aqhi, ok := numberFromAny(raw.AQHI)
		value := aqhiValueText(raw.AQHI, lang)
		periods := make([]airQualityPeriodSnapshot, 0, len(raw.Forecast.Periods))
		for _, period := range raw.Forecast.Periods {
			name := localizedString(period.Period, lang)
			periodAQHI := aqhiValueText(period.AQHI, lang)
			if name == "" || periodAQHI == "" {
				continue
			}
			periodRisk := ""
			if numeric, hasNumeric := numberFromAny(period.AQHI); hasNumeric {
				periodRisk = aqhiRisk(numeric)
			}
			periods = append(periods, airQualityPeriodSnapshot{
				Name:        name,
				AQHI:        periodAQHI,
				AQHIInSmoke: aqhiValueText(period.AQHIInSmoke, lang),
				Risk:        periodRisk,
			})
		}
		specialNotes := localizedString(raw.SpecialNotes, lang)
		if !ok && value == "" && len(periods) == 0 && specialNotes == "" {
			continue
		}
		location := fallbackText(loc.NameOverride, localizedString(raw.Location, lang))
		if location == "" {
			location = loc.ID
		}
		*snapshot = airQualitySnapshot{
			ReportedAt:   raw.ObservedAt,
			Location:     location,
			AQHI:         value,
			Risk:         aqhiRisk(aqhi),
			Forecast:     forecastFromAQHIPeriods(periods),
			Periods:      periods,
			SpecialNotes: specialNotes,
		}
		return input, true
	}
	return "", false
}

func (r renderer) loadLiveClimateSnapshot(feed feedXML, lang string, snapshot *climateSnapshot) (string, bool) {
	for _, loc := range feed.Locations.ClimateLocations.Locations {
		var raw liveClimateFile
		input, ok := r.loadStoreProductPayload("climate_summary", loc.Source, loc.ID, &raw)
		if !ok {
			continue
		}
		location := fallbackText(loc.NameOverride, localizedString(raw.Name, lang))
		if location == "" {
			location = loc.ID
		}
		lines := climateLines(raw, lang, feed.Timezone)
		if len(lines) == 0 {
			continue
		}
		*snapshot = climateSnapshot{
			ReportedAt: raw.LastUpdated,
			Location:   titleText(location),
			Summary:    lines,
		}
		return input, true
	}
	return "", false
}

func (r renderer) loadLiveMarineForecastSnapshot(feed feedXML, lang string, snapshot *marineForecastSnapshot) (string, bool) {
	for _, loc := range feedMarineForecastLocations(feed) {
		var raw liveMarineForecastFile
		input, ok := r.loadStoreProductPayload("marine_forecast", loc.Source, loc.ID, &raw)
		if !ok {
			input, ok = r.fetchLiveMarineForecast(loc, &raw)
		}
		if !ok {
			continue
		}
		area := firstNonBlank(loc.NameOverride, localizedString(raw.Properties.Area.Value, lang), localizedString(raw.Properties.Area.SubRegion, lang), loc.ID)
		regular := marineForecastLocations(raw.Properties.RegularForecast.Locations, lang, "wind", area)
		waves := marineForecastLocations(raw.Properties.WaveForecast.Locations, lang, "wave", area)
		extended := marineExtendedForecastPeriods(raw.Properties.ExtendedForecast.Locations, lang)
		warnings := marineForecastLocations(raw.Properties.Warnings.Locations, lang, "warning", area)
		if len(regular) == 0 && len(waves) == 0 && len(extended) == 0 && len(warnings) == 0 {
			continue
		}
		*snapshot = marineForecastSnapshot{
			IssuedAt:  firstNonBlank(raw.Properties.RegularForecast.IssuedDatetimeLocal, raw.Properties.RegularForecast.IssuedDatetimeUTC, raw.Properties.ExtendedForecast.IssuedDatetimeLocal, raw.Properties.ExtendedForecast.IssuedDatetimeUTC),
			UpdatedAt: raw.Properties.LastUpdated,
			Area:      area,
			Regular:   regular,
			Waves:     waves,
			Extended:  extended,
			Warnings:  warnings,
		}
		return input, true
	}
	return "", false
}

func feedMarineForecastLocations(feed feedXML) []locationXML {
	out := make([]locationXML, 0, len(feed.Locations.MarineForecastLocations.Locations)+len(feed.Locations.MarineForecastLocations.Subregions))
	out = append(out, feed.Locations.MarineForecastLocations.Locations...)
	out = append(out, feed.Locations.MarineForecastLocations.Subregions...)
	return out
}

func marineForecastLocations(locations []liveMarineForecastLocation, lang string, field string, area string) []marineForecastLocation {
	out := make([]marineForecastLocation, 0, len(locations))
	for _, loc := range locations {
		text := ""
		switch field {
		case "wave":
			text = localizedString(loc.WeatherCondition.TextSummary, lang)
		case "warning":
			text = firstNonBlank(localizedString(loc.WeatherCondition.TextSummary, lang), localizedString(loc.WeatherCondition.Value, lang), localizedString(loc.WeatherCondition.Wind, lang))
		default:
			text = localizedString(loc.WeatherCondition.Wind, lang)
		}
		text = cleanMarineForecastText(text)
		if text == "" {
			continue
		}
		out = append(out, marineForecastLocation{
			Name:   firstNonBlank(localizedString(loc.Name, lang), area, "marine area"),
			Period: cleanMarineForecastText(localizedString(loc.WeatherCondition.PeriodOfCoverage, lang)),
			Text:   text,
		})
	}
	return out
}

func marineExtendedForecastPeriods(locations []liveMarineExtendedLocation, lang string) []marineForecastPeriod {
	var out []marineForecastPeriod
	for _, loc := range locations {
		for _, period := range loc.WeatherCondition.ForecastPeriods {
			name := cleanMarineForecastText(localizedString(period.Name, lang))
			text := cleanMarineForecastText(localizedString(period.Value, lang))
			if name == "" || text == "" {
				continue
			}
			out = append(out, marineForecastPeriod{Name: name, Text: text})
		}
	}
	return out
}

func cleanMarineForecastText(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func (r renderer) loadLiveBulletinSnapshot(feed feedXML, lang string, snapshot *bulletinSnapshot) (string, bool) {
	if input, ok := r.loadXMLBulletinSnapshot(feed, lang, snapshot); ok {
		return input, true
	}
	path := resolvePath(r.cfg.BaseDir, filepath.Join("managed", "userbulletins.json"))
	var raw []struct {
		Enabled   bool                         `json:"enabled"`
		DateStart *string                      `json:"dateStart"`
		DateEnd   *string                      `json:"dateEnd"`
		Text      map[string]map[string]string `json:"text"`
	}
	if !readManagedJSON(path, &raw) {
		return "", false
	}
	now := time.Now()
	lines := []string{}
	for _, bulletin := range raw {
		if !bulletin.Enabled || !bulletinActive(bulletin.DateStart, bulletin.DateEnd, now) {
			continue
		}
		block := localizedBulletinBlock(bulletin.Text, lang)
		for _, key := range []string{"header", "body", "footer"} {
			if text := strings.TrimSpace(block[key]); text != "" {
				lines = append(lines, text)
			}
		}
	}
	if len(lines) == 0 {
		return "", false
	}
	*snapshot = bulletinSnapshot{Title: "User Bulletin", Lines: lines}
	return path, true
}

type userBulletinsXML struct {
	Bulletins []userBulletinXML `xml:"bulletin"`
}

type userBulletinXML struct {
	ID       string                  `xml:"id,attr"`
	Enabled  string                  `xml:"enabled,attr"`
	Title    string                  `xml:"title"`
	Active   userBulletinActiveXML   `xml:"active"`
	Schedule userBulletinScheduleXML `xml:"schedule"`
	Target   userBulletinTargetXML   `xml:"target"`
	Content  userBulletinContentXML  `xml:"content"`
}

type userBulletinActiveXML struct {
	Start  string `xml:"start,attr"`
	Expire string `xml:"expire,attr"`
}

type userBulletinScheduleXML struct {
	Mode    string   `xml:"mode,attr"`
	EndEach string   `xml:"end_of_cycle,attr"`
	Hours   []string `xml:"hours>hour"`
	Days    []string `xml:"days>day"`
}

type userBulletinTargetXML struct {
	Feeds []struct {
		ID string `xml:"id,attr"`
	} `xml:"feed"`
}

type userBulletinContentXML struct {
	Type  string               `xml:"type,attr"`
	Audio userBulletinAudioXML `xml:"audio"`
	Langs []struct {
		Code string `xml:"code,attr"`
		Text string `xml:",chardata"`
	} `xml:"lang"`
}

type userBulletinAudioXML struct {
	File string `xml:"file,attr"`
	URL  string `xml:"url,attr"`
}

func (r renderer) loadXMLBulletinSnapshot(feed feedXML, lang string, snapshot *bulletinSnapshot) (string, bool) {
	path := resolvePath(r.cfg.BaseDir, filepath.Join("managed", "configs", "userBulletins.xml"))
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var parsed userBulletinsXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return "", false
	}
	now := time.Now()
	lines := []string{}
	title := "User Bulletin"
	for _, bulletin := range parsed.Bulletins {
		if !xmlBoolText(bulletin.Enabled, true) || !bulletinTargetsFeed(bulletin, feed.ID) || !xmlBulletinActive(bulletin, now) {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(bulletin.Content.Type), "audio") {
			audioPath := strings.TrimSpace(bulletin.Content.Audio.File)
			audioURL := strings.TrimSpace(bulletin.Content.Audio.URL)
			if audioPath == "" && audioURL == "" {
				continue
			}
			title := fallbackText(strings.TrimSpace(bulletin.Title), "User Bulletin")
			*snapshot = bulletinSnapshot{
				Title:       title,
				ContentType: "audio",
				AudioPath:   audioPath,
				AudioURL:    audioURL,
			}
			return path, true
		}
		text := xmlBulletinText(bulletin, lang)
		if text == "" {
			continue
		}
		if strings.TrimSpace(bulletin.Title) != "" {
			title = strings.TrimSpace(bulletin.Title)
		}
		lines = append(lines, text)
	}
	if len(lines) == 0 {
		return "", false
	}
	*snapshot = bulletinSnapshot{Title: title, Lines: lines}
	return path, true
}

func xmlBulletinActive(bulletin userBulletinXML, now time.Time) bool {
	if !bulletinActive(stringPtrOrNil(bulletin.Active.Start), stringPtrOrNil(bulletin.Active.Expire), now) {
		return false
	}
	mode := strings.ToLower(strings.TrimSpace(bulletin.Schedule.Mode))
	switch mode {
	case "hours":
		hour := fmt.Sprintf("%02d", now.Hour())
		return stringListContains(bulletin.Schedule.Hours, hour) || stringListContains(bulletin.Schedule.Hours, fmt.Sprint(now.Hour()))
	case "days":
		day := strings.ToLower(now.Weekday().String()[:3])
		return stringListContainsFold(bulletin.Schedule.Days, day)
	default:
		return true
	}
}

func xmlBulletinText(bulletin userBulletinXML, lang string) string {
	fallback := ""
	for _, item := range bulletin.Content.Langs {
		text := strings.TrimSpace(item.Text)
		if text == "" {
			continue
		}
		code := strings.TrimSpace(item.Code)
		if strings.EqualFold(code, lang) {
			return text
		}
		if fallback == "" && strings.HasPrefix(strings.ToLower(code), "en") {
			fallback = text
		}
		if fallback == "" {
			fallback = text
		}
	}
	return fallback
}

func bulletinTargetsFeed(bulletin userBulletinXML, feedID string) bool {
	if len(bulletin.Target.Feeds) == 0 {
		return true
	}
	for _, feed := range bulletin.Target.Feeds {
		if strings.EqualFold(strings.TrimSpace(feed.ID), feedID) {
			return true
		}
	}
	return false
}

func stringPtrOrNil(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func stringListContains(values []string, needle string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == needle {
			return true
		}
	}
	return false
}

func stringListContainsFold(values []string, needle string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), needle) {
			return true
		}
	}
	return false
}

func xmlBoolText(raw string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on", "enabled":
		return true
	case "0", "false", "no", "off", "disabled":
		return false
	default:
		return fallback
	}
}

func (r renderer) loadStoreObservation(loc locationXML, target *liveObservationFile) (string, bool) {
	if r.cfg.Store == nil {
		return "", false
	}
	source := canonicalSource(loc.Source)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ok, err := r.cfg.Store.ObservationPayload(ctx, source, loc.ID, target)
	if err != nil || !ok {
		return "", false
	}
	return fmt.Sprintf("store:observations.current/%s/%s", source, loc.ID), true
}

func (r renderer) fetchECCCObservation(loc locationXML, target *liveObservationFile) (string, bool) {
	if canonicalSource(loc.Source) != "eccc" || strings.TrimSpace(loc.ID) == "" {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	raw, err := fetchECCCCitypage(ctx, strings.TrimSpace(loc.ID))
	if err != nil {
		return "", false
	}
	payload := buildECCCConditions(raw)
	payload["_raw_citypage"] = raw
	decoded, ok := decodeLiveObservation(payload)
	if !ok {
		return "", false
	}
	*target = decoded
	return fmt.Sprintf("eccc:citypage/%s/current", loc.ID), true
}

func (r renderer) fetchECCCPointObservation(loc locationXML, target *liveObservationFile) (string, bool) {
	latitude := strings.TrimSpace(loc.Latitude)
	longitude := strings.TrimSpace(loc.Longitude)
	if canonicalSource(loc.Source) != "eccc" || latitude == "" || longitude == "" {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	raw, err := fetchECCCPointPage(ctx, latitude, longitude)
	if err != nil {
		return "", false
	}
	payload, ok := buildECCCPointConditions(raw)
	if !ok {
		return "", false
	}
	decoded, ok := decodeLiveObservation(payload)
	if !ok {
		return "", false
	}
	*target = decoded
	return fmt.Sprintf("eccc:point/%s,%s/current", latitude, longitude), true
}

func (r renderer) loadStoreForecast(region coverageRegionXML, forecastID string, target *liveForecastFile) (string, bool) {
	if r.cfg.Store == nil {
		return "", false
	}
	source := canonicalSource(region.Source)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ok, err := r.cfg.Store.ForecastPayload(ctx, source, forecastID, target)
	if err != nil || !ok {
		return "", false
	}
	if len(target.Forecast) == 0 {
		return "", false
	}
	return fmt.Sprintf("store:forecasts.current/%s/%s", source, forecastID), true
}

func (r renderer) fetchECCCForecast(region coverageRegionXML, forecastID string, target *liveForecastFile) (string, bool) {
	if canonicalSource(region.Source) != "eccc" || strings.TrimSpace(forecastID) == "" {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	raw, err := fetchECCCCitypage(ctx, strings.TrimSpace(forecastID))
	if err != nil {
		return "", false
	}
	payload, ok := buildECCCForecast(raw, region, r.cfg.ForecastNames)
	if !ok {
		return "", false
	}
	decoded, ok := decodeLiveForecast(payload)
	if !ok {
		return "", false
	}
	*target = decoded
	return fmt.Sprintf("eccc:citypage/%s/forecast", forecastID), true
}

func fetchECCCCitypage(ctx context.Context, locationID string) (map[string]any, error) {
	url := fmt.Sprintf("https://api.weather.gc.ca/collections/citypageweather-realtime/items/%s?f=json", strings.TrimSpace(locationID))
	var raw map[string]any
	return raw, fetchJSON(ctx, url, &raw)
}

func fetchECCCPointPage(ctx context.Context, latitude string, longitude string) (map[string]any, error) {
	coords := strings.TrimSpace(latitude) + "," + strings.TrimSpace(longitude)
	pageURL := "https://weather.gc.ca/en/location/index.html?coords=" + url.QueryEscape(coords)
	body, err := fetchText(ctx, pageURL)
	if err != nil {
		return nil, err
	}
	return extractECCCPointObservation(body)
}

func fetchJSON(ctx context.Context, url string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "HazeWeatherRadio/26.06 product-render")
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s returned %s", url, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

func fetchText(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "HazeWeatherRadio/26.06 product-render")
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("%s returned %s", url, resp.Status)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func buildECCCConditions(raw map[string]any) map[string]any {
	props := mapAt(raw, "properties")
	cc := mapAt(props, "currentConditions")
	wind := mapAt(cc, "wind")
	return map[string]any{
		"source":      "eccc",
		"observed_at": ecccValue(cc, "timestamp"),
		"station":     ecccBilingual(cc, "station", "value"),
		"properties": map[string]any{
			"temp":       ecccValue(cc, "temperature", "value"),
			"condition":  ecccBilingual(cc, "condition"),
			"wind":       map[string]any{"speed": ecccValue(wind, "speed", "value"), "direction": ecccValue(wind, "direction", "value"), "gust": ecccValue(wind, "gust", "value")},
			"humidity":   ecccValue(cc, "relativeHumidity", "value"),
			"dewpoint":   ecccValue(cc, "dewpoint", "value"),
			"visibility": ecccValue(cc, "visibility", "value"),
			"pressure":   map[string]any{"value": ecccValue(cc, "pressure", "value"), "tendency": ecccBilingual(cc, "pressure", "tendency")},
			"windChill":  ecccValue(cc, "windChill", "value"),
			"humidex":    ecccValue(cc, "humidex", "value"),
			"heatIndex":  nil,
		},
	}
}

func extractECCCPointObservation(page string) (map[string]any, error) {
	offset := 0
	for {
		index := strings.Index(page[offset:], `"obs":`)
		if index < 0 {
			break
		}
		index += offset
		raw, ok := extractJSONObjectAt(page, index+len(`"obs":`))
		if !ok {
			offset = index + len(`"obs":`)
			continue
		}
		var obs map[string]any
		if err := json.Unmarshal([]byte(raw), &obs); err == nil && localizedString(obs["observedAt"], "en") != "" {
			return obs, nil
		}
		offset = index + len(`"obs":`) + len(raw)
	}
	return nil, fmt.Errorf("ECCC point page did not include observation payload")
}

func extractJSONObjectAt(text string, start int) (string, bool) {
	for start < len(text) && (text[start] == ' ' || text[start] == '\n' || text[start] == '\r' || text[start] == '\t') {
		start++
	}
	if start >= len(text) || text[start] != '{' {
		return "", false
	}
	depth := 0
	inString := false
	escaped := false
	for index := start; index < len(text); index++ {
		char := text[index]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if char == '\\' {
				escaped = true
				continue
			}
			if char == '"' {
				inString = false
			}
			continue
		}
		switch char {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return text[start : index+1], true
			}
		}
	}
	return "", false
}

func buildECCCPointConditions(obs map[string]any) (map[string]any, bool) {
	station := strings.TrimSpace(localizedString(obs["observedAt"], "en"))
	if station == "" {
		return nil, false
	}
	condition := strings.TrimSpace(localizedString(obs["condition"], "en"))
	if condition == "" && strings.TrimSpace(localizedString(obs["iconCode"], "en")) == "29" {
		condition = "Not observed"
	}
	payload := map[string]any{
		"source":      "eccc",
		"observed_at": firstNonBlank(localizedString(obs["timeStamp"], "en"), localizedString(obs["timeStampText"], "en")),
		"station":     map[string]any{"en": station, "fr": station},
		"station_id":  firstNonBlank(localizedString(obs["climateId"], "en"), localizedString(obs["tcid"], "en")),
		"properties": map[string]any{
			"temp":       pointMetricNumber(mapAt(obs, "temperature"), "metricUnrounded", "metric"),
			"condition":  map[string]any{"en": condition, "fr": condition},
			"wind":       map[string]any{"speed": pointMetricNumber(mapAt(obs, "windSpeed"), "metric"), "direction": localizedString(obs["windDirection"], "en"), "gust": pointMetricNumber(mapAt(obs, "windGust"), "metric")},
			"humidity":   pointNumber(obs["humidity"]),
			"dewpoint":   pointMetricNumber(mapAt(obs, "dewpoint"), "metricUnrounded", "metric"),
			"visibility": pointMetricNumber(mapAt(obs, "visibility"), "metric"),
			"pressure":   map[string]any{"value": pointMetricNumber(mapAt(obs, "pressure"), "metric"), "tendency": map[string]any{"en": localizedString(obs["tendency"], "en"), "fr": localizedString(obs["tendency"], "en")}},
			"windChill":  pointMetricNumber(mapAt(obs, "windChill"), "metric"),
			"humidex":    pointMetricNumber(mapAt(obs, "humidex"), "metric"),
			"heatIndex":  nil,
		},
	}
	return payload, true
}

func pointMetricNumber(source map[string]any, keys ...string) any {
	for _, key := range keys {
		if value := pointNumber(source[key]); value != nil {
			return value
		}
	}
	return nil
}

func pointNumber(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case float64:
		return typed
	case int:
		return float64(typed)
	case json.Number:
		parsed, err := typed.Float64()
		if err == nil {
			return parsed
		}
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			return nil
		}
		parsed, err := strconv.ParseFloat(text, 64)
		if err == nil {
			return parsed
		}
	}
	return nil
}

func buildECCCForecast(raw map[string]any, region coverageRegionXML, forecastNames map[string]forecastRegionName) (map[string]any, bool) {
	props := mapAt(raw, "properties")
	group := mapAt(props, "forecastGroup")
	rawForecasts, _ := group["forecasts"].([]any)
	periods := make([]map[string]any, 0, len(rawForecasts))
	for _, item := range rawForecasts {
		forecast, ok := item.(map[string]any)
		if !ok {
			continue
		}
		enParts := []string{}
		frParts := []string{}
		if text := ecccTextLang(mapAt(forecast, "cloudPrecip"), "en"); text != "" {
			enParts = append(enParts, text)
		}
		if text := ecccTextLang(mapAt(forecast, "cloudPrecip"), "fr"); text != "" {
			frParts = append(frParts, text)
		}
		for _, key := range []string{"temperatures", "windChill"} {
			summary := mapAt(mapAt(forecast, key), "textSummary")
			if text := ecccTextLang(summary, "en"); text != "" {
				enParts = append(enParts, text)
			}
			if text := ecccTextLang(summary, "fr"); text != "" {
				frParts = append(frParts, text)
			}
		}
		periods = append(periods, map[string]any{
			"period":      ecccBilingual(forecast, "period", "textForecastName"),
			"textSummary": map[string]any{"en": strings.Join(enParts, " "), "fr": strings.Join(frParts, " ")},
		})
	}
	if len(periods) == 0 {
		return nil, false
	}
	return map[string]any{
		"source":          "eccc",
		"issued_at":       ecccValue(group, "timestamp"),
		"updated_at":      props["lastUpdated"],
		"forecast":        periods,
		"forecast_region": region.ID,
		"name":            ecccForecastLocationNameBlock(region, props["name"], forecastNames),
	}, true
}

func ecccForecastLocationNameBlock(region coverageRegionXML, rawName any, forecastNames map[string]forecastRegionName) map[string]any {
	if names, ok := forecastNames[forecastRegionBaseCode(region.ID)]; ok && (names.English != "" || names.French != "") {
		fallback := firstNonBlank(names.English, names.French, region.ID)
		return map[string]any{
			"en": pauseForecastRegionName(firstNonBlank(names.English, fallback), "en"),
			"fr": pauseForecastRegionName(firstNonBlank(names.French, fallback), "fr"),
		}
	}
	if name := localizedString(rawName, "en"); name != "" {
		return map[string]any{
			"en": pauseForecastRegionName(name, "en"),
			"fr": pauseForecastRegionName(fallbackText(localizedString(rawName, "fr"), name), "fr"),
		}
	}
	fallback := fallbackText(region.Name, region.ID)
	return map[string]any{"en": pauseForecastRegionName(fallback, "en"), "fr": pauseForecastRegionName(fallback, "fr")}
}

func decodeLiveObservation(payload map[string]any) (liveObservationFile, bool) {
	var decoded liveObservationFile
	raw, err := json.Marshal(payload)
	if err != nil {
		return liveObservationFile{}, false
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return liveObservationFile{}, false
	}
	return decoded, true
}

func decodeLiveForecast(payload map[string]any) (liveForecastFile, bool) {
	var decoded liveForecastFile
	raw, err := json.Marshal(payload)
	if err != nil {
		return liveForecastFile{}, false
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return liveForecastFile{}, false
	}
	return decoded, true
}

func ecccBilingual(source map[string]any, keys ...string) map[string]any {
	current := source
	for _, key := range keys {
		current = mapAt(current, key)
	}
	out := map[string]any{}
	for _, lang := range []string{"en", "fr"} {
		if value, ok := current[lang]; ok {
			out[lang] = value
		}
	}
	return out
}

func ecccValue(source map[string]any, keys ...string) any {
	current := source
	for _, key := range keys {
		current = mapAt(current, key)
	}
	if value, ok := current["en"]; ok {
		return value
	}
	return nil
}

func ecccTextLang(source map[string]any, lang string) string {
	if source == nil {
		return ""
	}
	value, _ := source[lang].(string)
	return strings.TrimSpace(value)
}

func (r renderer) loadStoreProductPayload(kind string, sourceRaw string, id string, target any) (string, bool) {
	if r.cfg.Store == nil {
		return "", false
	}
	source := canonicalSource(sourceRaw)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ok, err := r.cfg.Store.ProductPayload(ctx, kind, source, id, target)
	if err != nil || !ok {
		return "", false
	}
	return fmt.Sprintf("store:products.payloads/%s/%s/%s", kind, source, id), true
}

func (r renderer) loadSpecialtyProductSnapshot(kind string, feed feedXML, snapshot *liveSpecialtyProductFile) (string, bool) {
	if feed.OnDemand && strings.EqualFold(strings.TrimSpace(kind), "hydrometric") {
		if input, ok := r.fetchLiveHydrometric(feed, snapshot); ok {
			return input, true
		}
	}
	input, ok := r.loadStoreProductPayload(kind, "eccc", feed.ID, snapshot)
	if !ok {
		return "", false
	}
	if len(snapshot.Items) == 0 {
		return "", false
	}
	return input, true
}

func (r renderer) fetchLiveAQHI(loc locationXML, target *liveAirQualityFile) (string, bool) {
	return r.fetchLiveAQHIFromBase(loc, target, "https://api.weather.gc.ca")
}

func (r renderer) fetchLiveAQHIFromBase(loc locationXML, target *liveAirQualityFile, apiBase string) (string, bool) {
	id := strings.TrimSpace(loc.ID)
	if canonicalSource(loc.Source) != "eccc" || id == "" || target == nil {
		return "", false
	}
	apiBase = strings.TrimRight(strings.TrimSpace(apiBase), "/")
	if apiBase == "" {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	observationURL := fmt.Sprintf("%s/collections/aqhi-observations-realtime/items?offset=0&limit=1000&sortby=-latest&location_id=%s&f=json", apiBase, url.QueryEscape(id))
	forecastURL := fmt.Sprintf("%s/collections/aqhi-forecasts-realtime/items?limit=1000&offset=0&f=json&sortby=-publication_datetime&aqhi_type=-Period&location_id=%s", apiBase, url.QueryEscape(id))
	var observationRaw map[string]any
	var forecastRaw map[string]any
	observationErr := fetchJSON(ctx, observationURL, &observationRaw)
	forecastErr := fetchJSON(ctx, forecastURL, &forecastRaw)
	if observationErr != nil && forecastErr != nil {
		return "", false
	}
	payload := map[string]any{"source": "eccc"}
	if properties := liveFirstFeatureProperties(observationRaw); properties != nil {
		payload["location"] = map[string]any{"en": liveText(properties["location_name_en"]), "fr": liveText(properties["location_name_fr"])}
		payload["observed_at"] = properties["observation_datetime"]
		payload["aqhi"] = properties["aqhi"]
		payload["special_notes"] = map[string]any{"en": liveText(properties["special_notes_en"]), "fr": liveText(properties["special_notes_fr"])}
	}
	if properties := liveFirstFeatureProperties(forecastRaw); properties != nil {
		payload["forecast"] = map[string]any{
			"published_at": properties["publication_datetime"],
			"periods":      liveAQHIPeriods(properties),
		}
	}
	if len(payload) <= 1 || !decodeLiveValue(payload, target) {
		return "", false
	}
	return "eccc:aqhi/" + id, true
}

func (r renderer) fetchLiveMarineForecast(loc locationXML, target *liveMarineForecastFile) (string, bool) {
	return r.fetchLiveMarineForecastFromBase(loc, target, "https://api.weather.gc.ca")
}

func (r renderer) fetchLiveMarineForecastFromBase(loc locationXML, target *liveMarineForecastFile, apiBase string) (string, bool) {
	id := strings.TrimSpace(loc.ID)
	if canonicalSource(loc.Source) != "eccc" || id == "" || target == nil {
		return "", false
	}
	apiBase = strings.TrimRight(strings.TrimSpace(apiBase), "/")
	if apiBase == "" {
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	endpoint := fmt.Sprintf("%s/collections/marineweather-realtime/items/%s?lang=en", apiBase, url.QueryEscape(id))
	if err := fetchJSON(ctx, endpoint, target); err != nil {
		return "", false
	}
	return "eccc:marine/" + id, true
}

func (r renderer) fetchLiveHydrometric(feed feedXML, target *liveSpecialtyProductFile) (string, bool) {
	return r.fetchLiveHydrometricFromBase(feed, target, "https://api.weather.gc.ca")
}

func (r renderer) fetchLiveHydrometricFromBase(feed feedXML, target *liveSpecialtyProductFile, apiBase string) (string, bool) {
	if target == nil {
		return "", false
	}
	apiBase = strings.TrimRight(strings.TrimSpace(apiBase), "/")
	if apiBase == "" {
		return "", false
	}
	items := make([]map[string]any, 0)
	inputs := make([]string, 0)
	appendLocations := func(relation string, locations []locationXML) {
		for _, loc := range locations {
			id := strings.TrimSpace(loc.ID)
			if canonicalSource(loc.Source) != "eccc" || id == "" {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			endpoint := apiBase + "/collections/hydrometric-realtime/items?limit=1&sortby=-DATETIME&STATION_NUMBER=" + url.QueryEscape(id) + "&f=json"
			var raw map[string]any
			err := fetchJSON(ctx, endpoint, &raw)
			cancel()
			if err != nil {
				continue
			}
			properties := liveFirstFeatureProperties(raw)
			if properties == nil {
				continue
			}
			items = append(items, map[string]any{
				"id":           liveFirstNonBlank(liveText(properties["IDENTIFIER"]), id),
				"station_id":   liveFirstNonBlank(liveText(properties["STATION_NUMBER"]), id),
				"canonical_id": loc.CanonicalID,
				"raw_source":   loc.Source,
				"raw_id":       id,
				"station":      liveFirstNonBlank(loc.NameOverride, liveText(properties["STATION_NAME"]), id),
				"relation":     relation,
				"order":        len(items),
				"observed_at":  liveFirstNonBlank(liveText(properties["DATETIME_LST"]), liveText(properties["DATETIME"])),
				"level_m":      properties["LEVEL"],
				"discharge":    properties["DISCHARGE"],
				"level_note":   liveText(properties["LEVEL_SYMBOL_EN"]),
				"flow_note":    liveText(properties["DISCHARGE_SYMBOL_EN"]),
				"published_at": liveFirstNonBlank(liveText(properties["DATETIME_LST"]), liveText(properties["DATETIME"])),
			})
			inputs = append(inputs, "eccc:hydrometric/"+id)
		}
	}
	appendLocations("primary", feed.Locations.HydrometricLocations.Locations)
	appendLocations("upstream", feed.Locations.HydrometricLocations.Upstream.Locations)
	appendLocations("downstream", feed.Locations.HydrometricLocations.Downstream.Locations)
	if len(items) == 0 {
		return "", false
	}
	*target = liveSpecialtyProductFile{
		Source:     "eccc",
		Collection: "hydrometric-realtime",
		Title:      "River Conditions",
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
		Items:      items,
	}
	return strings.Join(inputs, ";"), true
}

func liveFirstFeatureProperties(raw map[string]any) map[string]any {
	features, _ := raw["features"].([]any)
	if len(features) == 0 {
		return nil
	}
	feature, _ := features[0].(map[string]any)
	return mapAt(feature, "properties")
}

func liveAQHIPeriods(properties map[string]any) []map[string]any {
	group := mapAt(properties, "forecast_period")
	keys := []string{"period_1", "period1", "period_2", "period2", "period_3", "period3", "period_4", "period4", "period_5", "period5", "period_6", "period6"}
	out := make([]map[string]any, 0, 6)
	seen := make(map[string]struct{})
	for _, key := range keys {
		period := mapAt(group, key)
		if len(period) == 0 {
			continue
		}
		normalized := strings.ReplaceAll(key, "_", "")
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, map[string]any{
			"period":       map[string]any{"en": liveText(period["forecast_period_en"]), "fr": liveText(period["forecast_period_fr"])},
			"aqhi":         period["aqhi"],
			"aqhi_insmoke": period["aqhi_insmoke"],
		})
	}
	return out
}

func decodeLiveValue(source any, target any) bool {
	raw, err := json.Marshal(source)
	if err != nil {
		return false
	}
	return json.Unmarshal(raw, target) == nil
}

func liveText(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func liveFirstNonBlank(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func canonicalSource(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return "eccc"
	}
	if value == "swob" || value == "eccc-swob" || value == "eccc_swob" || value == "msc-swob" || value == "msc_swob" {
		return "eccc"
	}
	if value == "weather.com" || value == "weatherdotcom" {
		return "twc"
	}
	return value
}

func readManagedJSON(path string, target any) bool {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return false
	}
	return json.Unmarshal(raw, target) == nil
}

func newestObservationTime(observations []observation) string {
	var newest time.Time
	var rawNewest string
	for _, obs := range observations {
		raw := strings.TrimSpace(obs.ObservedAt)
		if raw == "" {
			continue
		}
		parsed, err := parseLooseTime(raw)
		if err != nil {
			if rawNewest == "" {
				rawNewest = raw
			}
			continue
		}
		if newest.IsZero() || parsed.After(newest) {
			newest = parsed
			rawNewest = raw
		}
	}
	return rawNewest
}

func parseLooseTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05-0700",
		"2006-01-02 15:04:05 MST",
		"2006-01-02 15:04 MST",
		"2006 Jan 02 1504 MST",
		"2006 Jan 02 15:04 MST",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time %q", raw)
}

func localizedString(value any, lang string) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case map[string]string:
		return localizedStringMap(typed, lang)
	case map[string]any:
		values := make(map[string]string, len(typed))
		for key, item := range typed {
			values[key] = localizedString(item, lang)
		}
		return localizedStringMap(values, lang)
	case float64:
		return rounded(typed)
	case int:
		return strconv.Itoa(typed)
	case json.Number:
		return typed.String()
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func localizedStringMap(values map[string]string, lang string) string {
	lang = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(lang), "_", "-"))
	short := lang
	if idx := strings.Index(short, "-"); idx > 0 {
		short = short[:idx]
	}
	for _, key := range []string{lang, short, "en-ca", "en", "fr-ca", "fr"} {
		if text := strings.TrimSpace(values[key]); text != "" {
			return text
		}
		for rawKey, text := range values {
			if strings.EqualFold(rawKey, key) && strings.TrimSpace(text) != "" {
				return strings.TrimSpace(text)
			}
		}
	}
	for _, text := range values {
		if strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func numberFromAny(value any) (float64, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func aqhiValueText(value any, lang string) string {
	if numeric, ok := numberFromAny(value); ok {
		return rounded(numeric)
	}
	return localizedString(value, lang)
}

func aqhiRisk(value float64) string {
	switch {
	case value <= 0:
		return ""
	case value <= 3:
		return "low"
	case value <= 6:
		return "moderate"
	case value <= 10:
		return "high"
	default:
		return "very high"
	}
}

func forecastFromAQHIPeriods(periods []airQualityPeriodSnapshot) string {
	if len(periods) == 0 {
		return ""
	}
	parts := make([]string, 0, len(periods))
	for _, period := range periods {
		if period.Name == "" || period.AQHI == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s, %s", period.Name, period.AQHI))
	}
	return forecastSentence(parts)
}

func forecastSentence(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	return "The forecast air quality health index is " + strings.Join(parts, "; ") + "."
}

func climateLines(raw liveClimateFile, lang string, timezone string) []string {
	lines := []string{}
	if text := climateTemperatureLine(raw, lang, timezone); text != "" {
		lines = append(lines, text)
	}
	if text := climatePrecipitationLine(raw); text != "" {
		lines = append(lines, text)
	}
	if text := climateWindLine(raw); text != "" {
		lines = append(lines, text)
	}
	if text := climateHumidityLine(raw); text != "" {
		lines = append(lines, text)
	}
	if text := climateDegreeDayLine(raw); text != "" {
		lines = append(lines, text)
	}
	if text := climateNormalsLine(raw, lang); text != "" {
		lines = append(lines, text)
	}
	if text := climateRecordsLine(raw); text != "" {
		lines = append(lines, text)
	}
	if text := climateAstronomyLine(raw, timezone); text != "" {
		lines = append(lines, text)
	}
	return lines
}

func climateTemperatureLine(raw liveClimateFile, lang string, timezone string) string {
	facts := []string{}
	if raw.Observations.High != nil {
		facts = append(facts, "the high was "+degrees(*raw.Observations.High))
	}
	if raw.Observations.Low != nil {
		facts = append(facts, "the low was "+degrees(*raw.Observations.Low))
	}
	if raw.Observations.Mean != nil {
		facts = append(facts, "the mean temperature was "+degrees(*raw.Observations.Mean))
	}
	if len(facts) == 0 {
		return ""
	}
	prefix := climateStationDatePrefix(raw, lang, timezone)
	if prefix == "" {
		return sentence(climateJoin(facts))
	}
	return sentence(prefix + ", " + climateJoin(facts))
}

func climatePrecipitationLine(raw liveClimateFile) string {
	if raw.Observations.PrecipitationTrace {
		return "Only a trace of precipitation was recorded."
	}
	if raw.Observations.Precipitation != nil {
		if nearZero(*raw.Observations.Precipitation) {
			return "No measurable precipitation was recorded."
		}
		return "Precipitation totalled " + oneDecimal(*raw.Observations.Precipitation) + " millimetres."
	}
	parts := []string{}
	if raw.Observations.RainTrace {
		parts = append(parts, "a trace of rain")
	} else if raw.Observations.Rain != nil {
		parts = append(parts, oneDecimal(*raw.Observations.Rain)+" millimetres of rain")
	}
	if raw.Observations.SnowfallTrace {
		parts = append(parts, "a trace of snow")
	} else if raw.Observations.Snowfall != nil {
		parts = append(parts, oneDecimal(*raw.Observations.Snowfall)+" centimetres of snow")
	}
	if len(parts) > 0 {
		return sentence("The station recorded " + climateJoin(parts))
	}
	if raw.Observations.SnowOnGround != nil {
		if nearZero(*raw.Observations.SnowOnGround) {
			return "No snow was on the ground at observation time."
		}
		return "Snow on the ground was " + oneDecimal(*raw.Observations.SnowOnGround) + " centimetres."
	}
	return ""
}

func climateWindLine(raw liveClimateFile) string {
	if raw.Observations.MaxGustSpeed == nil {
		return ""
	}
	text := "The strongest wind gust was " + oneDecimal(*raw.Observations.MaxGustSpeed) + " kilometres per hour"
	if direction := readableDirection(raw.Observations.MaxGustDirection); direction != "" {
		text += " from the " + direction
	}
	return sentence(text)
}

func climateHumidityLine(raw liveClimateFile) string {
	if raw.Observations.MinHumidity == nil {
		return ""
	}
	return "The minimum relative humidity was " + rounded(*raw.Observations.MinHumidity) + " percent."
}

func climateDegreeDayLine(raw liveClimateFile) string {
	parts := []string{}
	if raw.Observations.HeatingDegreeDays != nil {
		parts = append(parts, "Heating degree days were "+oneDecimal(*raw.Observations.HeatingDegreeDays))
	}
	if raw.Observations.CoolingDegreeDays != nil {
		parts = append(parts, "cooling degree days were "+oneDecimal(*raw.Observations.CoolingDegreeDays))
	}
	if len(parts) == 0 {
		return ""
	}
	return sentence(climateJoin(parts))
}

func climateNormalsLine(raw liveClimateFile, lang string) string {
	facts := []string{}
	if raw.Normals.Temperature.High != nil {
		facts = append(facts, "high "+degrees(*raw.Normals.Temperature.High))
	}
	if raw.Normals.Temperature.Low != nil {
		facts = append(facts, "low "+degrees(*raw.Normals.Temperature.Low))
	}
	if raw.Normals.Temperature.Mean != nil {
		facts = append(facts, "mean "+degrees(*raw.Normals.Temperature.Mean))
	}
	if raw.Normals.Precipitation != nil {
		facts = append(facts, "total precipitation "+oneDecimal(*raw.Normals.Precipitation)+" millimetres")
	}
	if len(facts) == 0 {
		if normal := localizedString(raw.Normals.TextSummary, lang); normal != "" {
			return "The normal temperatures are " + strings.TrimSuffix(normal, ".") + "."
		}
		return ""
	}
	month := time.Month(raw.Normals.Month).String()
	if raw.Normals.Month <= 0 || raw.Normals.Month > 12 {
		month = "the month"
	}
	period := fallbackText(raw.Normals.Period, "1981 to 2010")
	station := titleText(localizedString(raw.Normals.Station, lang))
	prefix := "For " + month + ", the monthly averages from " + period
	if station != "" {
		prefix += " at " + station
	}
	return sentence(prefix + " are " + climateJoin(facts))
}

func climateRecordsLine(raw liveClimateFile) string {
	parts := []string{}
	if value, year, ok := climateRecord(raw, "high_temperature"); ok {
		parts = append(parts, "the record high was "+degrees(value)+" in "+year)
	}
	if value, year, ok := climateRecord(raw, "low_temperature"); ok {
		parts = append(parts, "the record low was "+degrees(value)+" in "+year)
	}
	if value, year, ok := climateRecord(raw, "precipitation"); ok {
		parts = append(parts, "the greatest precipitation was "+oneDecimal(value)+" millimetres in "+year)
	}
	if value, year, ok := climateRecord(raw, "snowfall"); ok && !nearZero(value) {
		parts = append(parts, "the greatest snowfall was "+oneDecimal(value)+" centimetres in "+year)
	}
	if len(parts) == 0 {
		return ""
	}
	date := climateRecordDate(raw.Observations.Date)
	if date == "" {
		return sentence("Climate records are " + climateJoin(parts))
	}
	return sentence("For " + date + ", " + climateJoin(parts))
}

func climateRecord(raw liveClimateFile, key string) (float64, string, bool) {
	record, ok := raw.Records[key]
	if !ok || record.Value == nil {
		return 0, "", false
	}
	year := climateRecordYear(record.Year)
	if year == "" {
		return 0, "", false
	}
	return *record.Value, year, true
}

func climateRecordYear(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case float64:
		if typed <= 0 {
			return ""
		}
		return strconv.Itoa(int(typed + 0.5))
	case int:
		if typed <= 0 {
			return ""
		}
		return strconv.Itoa(typed)
	case json.Number:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed.String()), 64)
		if err != nil || parsed <= 0 {
			return ""
		}
		return strconv.Itoa(int(parsed + 0.5))
	default:
		text := strings.TrimSpace(fmt.Sprint(typed))
		if text == "" {
			return ""
		}
		parsed, err := strconv.ParseFloat(text, 64)
		if err == nil {
			if parsed <= 0 {
				return ""
			}
			return strconv.Itoa(int(parsed + 0.5))
		}
		return text
	}
}

func climateRecordDate(raw string) string {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02", time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.Format("January 2")
		}
	}
	return ""
}

func climateAstronomyLine(raw liveClimateFile, timezone string) string {
	if strings.TrimSpace(raw.Astronomy.Sunrise) == "" || strings.TrimSpace(raw.Astronomy.Sunset) == "" {
		return ""
	}
	sunrise, sunriseErr := parseLooseTime(raw.Astronomy.Sunrise)
	sunset, sunsetErr := parseLooseTime(raw.Astronomy.Sunset)
	if sunriseErr != nil || sunsetErr != nil {
		return ""
	}
	if loc, locErr := time.LoadLocation(fallbackText(raw.Astronomy.Timezone, timezone)); locErr == nil {
		sunrise = sunrise.In(loc)
		sunset = sunset.In(loc)
	}
	return sentence("Sunrise is at " + compactClock(sunrise) + ", and sunset is at " + compactClock(sunset) + " " + timezoneName(sunset.Format("MST")))
}

func climateStationDatePrefix(raw liveClimateFile, lang string, timezone string) string {
	station := titleText(localizedString(raw.Observations.Station, lang))
	date := climateDateLabel(raw.Observations.Date, timezone)
	switch {
	case station != "" && date != "":
		return "At " + station + " on " + date
	case station != "":
		return "At " + station
	case date != "":
		return "For " + date
	default:
		return ""
	}
}

func climateDateLabel(raw string, timezone string) string {
	raw = strings.TrimSpace(raw)
	var parsed time.Time
	var err error
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02", time.RFC3339} {
		parsed, err = time.Parse(layout, raw)
		if err == nil {
			break
		}
	}
	if err != nil {
		return ""
	}
	if loc, locErr := time.LoadLocation(fallbackText(timezone, "Local")); locErr == nil {
		parsed = time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 12, 0, 0, 0, loc)
	}
	return parsed.Format("Monday, January 2")
}

func climateJoin(parts []string) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	case 2:
		return parts[0] + ", and " + parts[1]
	default:
		return strings.Join(parts[:len(parts)-1], ", ") + ", and " + parts[len(parts)-1]
	}
}

func nearZero(value float64) bool {
	return value > -0.05 && value < 0.05
}

func bulletinActive(start *string, end *string, now time.Time) bool {
	if start != nil && strings.TrimSpace(*start) != "" {
		if parsed, err := parseLooseTime(*start); err == nil && now.Before(parsed) {
			return false
		}
	}
	if end != nil && strings.TrimSpace(*end) != "" {
		if parsed, err := parseLooseTime(*end); err == nil && now.After(parsed) {
			return false
		}
	}
	return true
}

func localizedBulletinBlock(blocks map[string]map[string]string, lang string) map[string]string {
	if len(blocks) == 0 {
		return nil
	}
	lang = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(lang), "_", "-"))
	short := lang
	if idx := strings.Index(short, "-"); idx > 0 {
		short = short[:idx]
	}
	for _, key := range []string{lang, short, "en-ca", "en", "fr-ca", "fr"} {
		for rawKey, block := range blocks {
			if strings.EqualFold(rawKey, key) {
				return block
			}
		}
	}
	for _, block := range blocks {
		return block
	}
	return nil
}

func cleanPlaintextProduct(raw string) string {
	lines := strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n")
	paragraphs := []string{}
	current := []string{}
	flush := func() {
		if len(current) == 0 {
			return
		}
		paragraphs = append(paragraphs, strings.Join(current, " "))
		current = nil
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, ":") {
			line = strings.Trim(line, ":")
			line = strings.ReplaceAll(line, ":", ". ")
		}
		current = append(current, line)
	}
	flush()
	return strings.Join(paragraphs, "\n\n")
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			return text
		}
	}
	return ""
}

func titleText(value string) string {
	value = strings.TrimSpace(value)
	if value == strings.ToUpper(value) {
		words := strings.Fields(strings.ToLower(value))
		for i, word := range words {
			if word == "" {
				continue
			}
			words[i] = strings.ToUpper(word[:1]) + word[1:]
		}
		return strings.Join(words, " ")
	}
	return value
}
