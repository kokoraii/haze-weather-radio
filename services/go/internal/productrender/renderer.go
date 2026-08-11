package productrender

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type renderer struct {
	cfg loadedConfig
}

var templateVariableRE = regexp.MustCompile(`\{([A-Za-z0-9_]+)\}`)

func newRenderer(cfg loadedConfig) renderer {
	return renderer{cfg: cfg}
}

func (r renderer) RenderWxOnDemand(request wxOnDemandRequest) (Product, error) {
	feed, err := r.onDemandFeed(request)
	if err != nil {
		return Product{}, err
	}
	packages := cleanStringList(request.Packages)
	if len(packages) == 0 {
		packages = []string{"current_conditions", "forecast"}
	}
	var products []Product
	var texts []string
	var titles []string
	var inputs []InputRef
	readerID := strings.TrimSpace(request.ReaderID)
	language := strings.TrimSpace(request.Language)
	for _, packageID := range packages {
		product, err := r.Render(renderRequest{
			RequestID:    request.RequestID + "-" + safeID(packageID),
			FeedID:       feed.ID,
			PackageID:    packageID,
			Language:     request.Language,
			Force:        request.Force,
			FeedOverride: &feed,
			Telephone:    request.Telephone,
		})
		if err != nil {
			return Product{}, err
		}
		products = append(products, product)
		if text := strings.TrimSpace(product.Text); text != "" {
			texts = append(texts, text)
		}
		if title := strings.TrimSpace(product.Title); title != "" {
			titles = append(titles, title)
		}
		inputs = append(inputs, product.Inputs...)
		if readerID == "" {
			readerID = product.ReaderID
		}
		if language == "" {
			language = product.Language
		}
	}
	if len(texts) == 0 {
		return Product{}, fmt.Errorf("no weather product text was generated")
	}
	now := time.Now().UTC()
	locationName := firstNonBlank(request.LocationName, request.Code, request.ForecastID, request.StationID)
	title := strings.Join(titles, " / ")
	if title == "" {
		title = "Weather On Demand"
	}
	segments := make([]Segment, 0, len(products))
	for _, product := range products {
		if len(product.Segments) == 0 {
			segments = append(segments, Segment{
				Kind:  "product",
				Label: product.PackageID,
				Text:  product.Text,
			})
			continue
		}
		for _, segment := range product.Segments {
			if strings.TrimSpace(segment.Text) == "" && strings.TrimSpace(segment.AudioPath) == "" {
				continue
			}
			if segment.Kind == "" {
				segment.Kind = "product"
			}
			segment.Label = segmentLabel(product.PackageID, segment.Label)
			segments = append(segments, segment)
		}
	}
	return Product{
		ID:          safeID(fmt.Sprintf("wx-%s-%s-%d", feed.ID, request.Code, now.UnixNano())),
		FeedID:      feed.ID,
		PackageID:   "wx_on_demand",
		Title:       title,
		Text:        strings.Join(texts, "\n\n"),
		ReaderID:    readerID,
		Language:    fallbackText(language, "en-US"),
		Segments:    segments,
		Inputs:      inputs,
		GeneratedAt: now,
		Metadata: map[string]string{
			"canonical_id":       request.CanonicalID,
			"location_code":      request.Code,
			"location_name":      locationName,
			"source":             request.Source,
			"forecast_id":        request.ForecastID,
			"station_id":         request.StationID,
			"air_quality_id":     request.AirQualityID,
			"climate_id":         request.ClimateID,
			"hydrometric_id":     request.HydrometricID,
			"marine_forecast_id": request.MarineForecastID,
			"packages":           strings.Join(packages, ","),
		},
	}, nil
}

func (r renderer) Render(request renderRequest) (Product, error) {
	feed, ok := r.cfg.feedByID(request.FeedID)
	if request.FeedOverride != nil {
		feed = *request.FeedOverride
		ok = true
	}
	if !ok {
		return Product{}, fmt.Errorf("feed %q is not configured", request.FeedID)
	}
	if strings.TrimSpace(feed.ID) == "" || !xmlBool(feed.EnabledRaw, true) {
		return Product{}, fmt.Errorf("feed %q is disabled", request.FeedID)
	}
	if !xmlBool(feed.Playout.Routine, true) {
		return Product{}, fmt.Errorf("routine playout is disabled for feed %q", request.FeedID)
	}
	if !r.cfg.packageEnabled(request.PackageID) {
		return Product{}, fmt.Errorf("package %q is disabled", request.PackageID)
	}

	base := productBase(r.cfg, feed, request.PackageID, request.Language, request.Telephone)
	var product Product
	var err error
	switch strings.ToLower(strings.TrimSpace(request.PackageID)) {
	case "station_id":
		product = r.stationIDProduct(base, feed)
	case "date_time":
		product = r.dateTimeProduct(base, feed)
	case "current_conditions":
		product, err = r.currentConditionsProduct(base, feed)
	case "marine_reports", "aviation_reports":
		product, err = r.observationReportProduct(base, feed)
	case "marine_forecast":
		product, err = r.marineForecastProduct(base, feed)
	case "forecast":
		product, err = r.forecastProduct(base, feed)
	case "air_quality":
		product, err = r.airQualityProduct(base, feed)
	case "climate_summary":
		product, err = r.climateProduct(base, feed)
	case "thunderstorm_outlook":
		product, err = r.thunderstormOutlookProduct(base, feed)
	case "hydrometric":
		product, err = r.hydrometricProduct(base, feed)
	case "user_bulletin":
		product, err = r.userBulletinProduct(base, feed)
	case "alerts":
		product, err = r.alertsProduct(base, feed)
	case "geophysical_alert":
		product, err = r.textStoreProduct(base, "Geophysical Alert", "nws", "wwv", cleanWWVProduct)
	default:
		return Product{}, fmt.Errorf("package %q is not supported by product-render yet", request.PackageID)
	}
	if err != nil {
		return Product{}, err
	}
	product.Text = flattenSegments(product.Segments)
	product.GeneratedAt = time.Now().UTC()
	if product.ID == "" {
		product.ID = safeID(fmt.Sprintf("%s-%s-%d", product.FeedID, product.PackageID, product.GeneratedAt.UnixNano()))
	}
	return product, nil
}

func (r renderer) onDemandFeed(request wxOnDemandRequest) (feedXML, error) {
	feedID := strings.TrimSpace(request.FeedID)
	feed := feedXML{
		ID:         "wx-on-demand",
		EnabledRaw: "true",
		Timezone:   firstNonBlank(request.Timezone, "UTC"),
	}
	if language := strings.TrimSpace(request.Language); language != "" {
		feed.Languages.Langs = []feedLangXML{{Code: language}}
	}
	if feedID != "" {
		configured, ok := r.cfg.feedByID(feedID)
		if !ok {
			return feedXML{}, fmt.Errorf("feed %q is not configured", request.FeedID)
		}
		if strings.TrimSpace(configured.ID) == "" || !xmlBool(configured.EnabledRaw, true) {
			return feedXML{}, fmt.Errorf("feed %q is disabled", request.FeedID)
		}
		feed = configured
	}
	feed.OnDemand = true

	locationName := firstNonBlank(request.LocationName, request.Code, request.ForecastID, request.StationID)
	nameOverride := onDemandNameOverride(locationName, request)
	if request.Telephone {
		nameOverride = ""
	}
	source := onDemandSource(request)
	forecastID := strings.TrimSpace(request.ForecastID)
	latitude := strings.TrimSpace(request.Latitude)
	longitude := strings.TrimSpace(request.Longitude)
	stationID := firstNonBlank(request.StationID, forecastID)
	if forecastID != "" {
		regionID := firstNonBlank(request.Code, forecastID)
		feed.Locations.Coverage.Regions = []coverageRegionXML{{
			ID:             regionID,
			Source:         source,
			Name:           locationName,
			DeriveForecast: forecastID,
		}}
	}
	if stationID != "" || latitude != "" || longitude != "" {
		feed.Locations.ObservationLocations.Locations = []locationXML{{
			ID:           firstNonBlank(stationID, request.Code),
			Source:       source,
			NameOverride: nameOverride,
			Latitude:     latitude,
			Longitude:    longitude,
		}}
	}
	if id := strings.TrimSpace(request.AirQualityID); id != "" {
		feed.Locations.AirQualityLocations.Locations = []locationXML{{ID: id, Source: "eccc"}}
	}
	if id := strings.TrimSpace(request.ClimateID); id != "" {
		feed.Locations.ClimateLocations.Locations = []locationXML{{ID: id, Source: "eccc"}}
	}
	if id := strings.TrimSpace(request.HydrometricID); id != "" {
		feed.Locations.HydrometricLocations.Locations = []locationXML{{ID: id, Source: "eccc"}}
		feed.Locations.HydrometricLocations.Upstream.Locations = nil
		feed.Locations.HydrometricLocations.Downstream.Locations = nil
	}
	if id := strings.TrimSpace(request.MarineForecastID); id != "" {
		feed.Locations.MarineForecastLocations.Locations = []locationXML{{ID: id, Source: "eccc"}}
		feed.Locations.MarineForecastLocations.Subregions = nil
	}
	if strings.TrimSpace(request.Timezone) != "" {
		feed.Timezone = strings.TrimSpace(request.Timezone)
	}
	if request.Telephone {
		feed.Transmitter.Transmitters = []transmitterXML{telephoneTransmitter(feed, locationName)}
	}
	return feed, nil
}

func telephoneTransmitter(feed feedXML, locationName string) transmitterXML {
	transmitter := stationTransmitter(feed)
	transmitter.SiteName = fallbackText(locationName, transmitter.SiteName)
	transmitter.Callsign = ""
	transmitter.Relationship = "primary"
	transmitter.FrequencyMHz = transmitterFrequencyXML{}
	return transmitter
}

func onDemandNameOverride(locationName string, request wxOnDemandRequest) string {
	value := strings.TrimSpace(locationName)
	for _, id := range []string{request.Code, request.ForecastID, request.StationID} {
		if value != "" && strings.EqualFold(value, strings.TrimSpace(id)) {
			return ""
		}
	}
	return value
}

func onDemandSource(request wxOnDemandRequest) string {
	source := strings.ToLower(strings.TrimSpace(request.Source))
	switch source {
	case "", "hello_weather", "eccc_forecast", "clc", "capcp_geocode", "eccc", "msc", "envcan":
		return "eccc"
	case "station":
		stationID := strings.ToUpper(strings.TrimSpace(request.StationID))
		if strings.HasPrefix(stationID, "K") {
			return "nws"
		}
		return "eccc"
	default:
		return canonicalSource(source)
	}
}

func productBase(cfg loadedConfig, feed feedXML, pkgID string, language string, telephone bool) Product {
	renderLanguage := cfg.packageRenderLanguage(pkgID, feedRenderLanguage(feed, language))
	product := Product{
		FeedID:    feed.ID,
		PackageID: pkgID,
		Title:     titleForPackage(pkgID),
		ReaderID:  cfg.readerIDForLanguage(pkgID, renderLanguage),
		Language:  renderLanguage,
		Metadata: map[string]string{
			"site_name": feedSiteName(feed),
			"callsign":  feedCallsign(feed),
		},
	}
	if telephone {
		product.Metadata["audience"] = "telephone"
	}
	if canonicalIDs := renderFeedCanonicalLocationIDs(feed); len(canonicalIDs) > 0 {
		product.Metadata["canonical_location_ids"] = strings.Join(canonicalIDs, ",")
	}
	return product
}

func (r renderer) stationIDProduct(base Product, feed feedXML) Product {
	onAirName := spokenText(r.cfg.Root.Operator.OnAirName)
	if onAirName == "" {
		onAirName = "Haze Weather Radio"
	}
	site := feedSiteName(feed)
	callsign := feedCallsign(feed)
	frequency := feedFrequencyMHz(feed)
	frequencyMHz := frequencyMHzSpeech(frequency)
	values := map[string]string{
		"on_air_name":    onAirName,
		"site":           site,
		"service_area":   fallbackText(site, feed.ID),
		"service_region": fallbackText(feedDescription(feed, base.Language), site),
		"callsign":       callsign,
		"frequency":      frequency,
		"freq_mhz":       frequencyMHz,
	}
	parts := r.packageTextSequence(base.PackageID, "", base.Language, values)
	if len(parts) == 0 {
		parts = []string{r.packageText(base.PackageID, "opener", base.Language, "You are listening to {on_air_name}.", values)}
		if callsignSpoken := spokenCallsign(callsign); callsignSpoken != "" {
			parts = append(parts, fmt.Sprintf("Callsign %s.", callsignSpoken))
		}
		if frequency != "" {
			location := fallbackText(site, feed.ID)
			parts = append(parts, fmt.Sprintf("Broadcasting from %s on a frequency of %s megahertz.", location, frequency))
		} else if site != "" {
			parts = append(parts, fmt.Sprintf("Serving %s.", site))
		}
	}
	if desc := feedDescription(feed, base.Language); desc != "" {
		parts = append(parts, desc)
	}
	if replacement := replacementStationStatement(feed); replacement != "" {
		parts = append(parts, replacement)
	}
	base.Title = "Station Identification"
	base.Segments = appendTextSegments(nil, "static", "station_id", parts)
	return base
}

func frequencyMHzSpeech(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	lower := strings.ToLower(value)
	if strings.Contains(lower, "megahertz") || strings.HasSuffix(lower, "mhz") {
		return value
	}
	return value + " megahertz"
}

func replacementStationStatement(feed feedXML) string {
	transmitter, ok := replacementTransmitter(feed)
	if !ok {
		return ""
	}
	callsign := strings.TrimSpace(transmitter.Callsign)
	site := strings.TrimSpace(transmitter.SiteName)
	if callsign == "" && site == "" {
		return ""
	}
	network := strings.TrimSpace(spokenNetworkName(transmitter.Network))
	if network == "" {
		network = "Weatheradio Canada"
	}
	parts := []string{"This station replaces former", network, "station"}
	if callsign != "" {
		parts = append(parts, callsign)
	}
	if site != "" {
		parts = append(parts, "in", site)
	}
	return strings.Join(parts, " ") + "."
}

func (r renderer) dateTimeProduct(base Product, feed feedXML) Product {
	now := time.Now()
	if loc, err := time.LoadLocation(fallbackText(feed.Timezone, "Local")); err == nil {
		now = now.In(loc)
	}
	base.Title = "Date and Time"
	base.Segments = []Segment{{Kind: "static", Label: "date_time", Text: dateTimeAnnouncement(now, base.Language)}}
	return base
}

func (r renderer) currentConditionsProduct(base Product, feed feedXML) (Product, error) {
	var snapshot observationSnapshot
	inputPath, ok := r.loadLiveObservationSnapshot(feed, base.Language, &snapshot)
	if !ok {
		return Product{}, fmt.Errorf("current weather observations are unavailable for feed %s", feed.ID)
	}

	base.Title = "Current Conditions"
	base.Inputs = append(base.Inputs, InputRef{Type: inputTypeForPath(inputPath), ID: inputPath})
	callsign := fallbackText(feedCallsign(feed), feedSiteName(feed))
	reported := reportTime(snapshot.ReportedAt, feed.Timezone)
	source := spokenWeatherSource(firstNonBlank(snapshot.Primary.Source, sourceFromObservations(snapshot.Observations), sourceFromObservations(snapshot.AreaObservations)))
	values := issueTimeTemplateValues(snapshot.ReportedAt, feed.Timezone, map[string]string{
		"source":   fallbackText(source, "Environment and Climate Change Canada"),
		"time":     fallbackText(reported, "the latest report time"),
		"callsign": callsign,
		"site":     feedSiteName(feed),
	})
	segments := []Segment{}
	if text := r.packageText(base.PackageID, "opener", base.Language, "The current weather conditions. Issued by {source} at {time}.", values); text != "" {
		segments = append(segments, Segment{Kind: "opener", Label: "main_opener", Text: text})
	}
	if text := r.packageText(base.PackageID, "report_time", base.Language, "", map[string]string{
		"time":     reported,
		"callsign": callsign,
		"site":     feedSiteName(feed),
	}); text != "" {
		segments = append(segments, Segment{Kind: "opener", Label: "report_time", Text: text})
	}
	if strings.TrimSpace(snapshot.Primary.LocationName) != "" {
		segments = append(segments, r.currentObservationSegments(base, snapshot.Primary, "primary", true)...)
	}

	area := uniqueAreaObservations(snapshot.AreaObservations, snapshot.Primary)
	if len(area) == 0 {
		area = uniqueAreaObservations(snapshot.Observations, snapshot.Primary)
	}
	addedAreaOpener := false
	for _, obs := range area {
		if obs.LocationName == "" || sameObservation(obs, snapshot.Primary) {
			continue
		}
		if !addedAreaOpener {
			segments = append(segments, Segment{
				Kind:  "opener",
				Label: "area_observations",
				Text: r.packageText(base.PackageID, "area_opener", base.Language, "Elsewhere around the {callsign} listening area:", map[string]string{
					"callsign": callsign,
					"site":     feedSiteName(feed),
				}),
			})
			addedAreaOpener = true
		}
		segments = append(segments, r.currentObservationSegments(base, obs, "area", false)...)
	}
	if repeat := repeatObservationText(snapshot.Primary); repeat != "" && !isTelephoneProduct(base) {
		segments = append(segments, Segment{Kind: "closure", Label: "repeat_primary", Text: repeat})
	}
	base.Segments = segments
	return base, nil
}

func (r renderer) currentObservationSegments(base Product, obs observation, labelPrefix string, primary bool) []Segment {
	values := observationTemplateValues(obs)
	templatePrefix := "nearby"
	fallbackParts := []string{}
	if primary {
		templatePrefix = "primary"
		fallbackParts = fullObservationBodyParts(obs)
	} else {
		fallbackParts = shortObservationBodyParts(obs)
	}
	parts := r.packageTextSequenceFiltered(base.PackageID, templatePrefix, base.Language, values)
	if len(parts) == 0 {
		parts = fallbackParts
	}
	if wind := r.currentObservationWindText(base, templatePrefix, obs, values, primary); wind != "" {
		parts = append(parts, wind)
	}
	return appendTextSegments(nil, "package", labelPrefix+"_observation", parts)
}

func (r renderer) currentObservationWindText(base Product, prefix string, obs observation, values map[string]string, primary bool) string {
	keysPrefix := strings.Trim(strings.ToLower(strings.TrimSpace(prefix)), ".")
	if values["wind_calm"] == "true" {
		return r.packageTextFirst(base.PackageID, []string{keysPrefix + ".winds.text.2", "winds.text.2"}, base.Language, "The wind was calm.", values)
	}
	if obs.WindDirection == "" && obs.WindSpeedKMH == nil {
		return r.packageTextFirst(base.PackageID, []string{keysPrefix + ".winds.placeholder", "winds.placeholder"}, base.Language, "", values)
	}
	raw := r.packageTextRaw(base.PackageID, keysPrefix+".winds.text.1", base.Language)
	if currentWindTemplateUsable(raw, values) {
		return renderTemplateText(raw, values)
	}
	return sentence(windText(obs, primary))
}

func currentWindTemplateUsable(raw string, values map[string]string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" || templateHasMissingValue(raw, values) {
		return false
	}
	normalized := strings.Join(strings.Fields(strings.ToLower(raw)), " ")
	for _, bad := range []string{" 10 {spd_metric}", " at 10 {spd_metric}", " à 10 {spd_metric}", " a 10 {spd_metric}"} {
		if strings.Contains(normalized, strings.TrimSpace(bad)) {
			return false
		}
	}
	return true
}

func (r renderer) observationReportProduct(base Product, feed feedXML) (Product, error) {
	var snapshot observationSnapshot
	inputPath, ok := r.loadLiveObservationReportSnapshot(feed, base.PackageID, base.Language, &snapshot)
	if !ok {
		return Product{}, fmt.Errorf("%s observations are unavailable for feed %s", strings.ToLower(titleForPackage(base.PackageID)), feed.ID)
	}
	base.Title = titleForPackage(base.PackageID)
	base.Inputs = append(base.Inputs, InputRef{Type: inputTypeForPath(inputPath), ID: inputPath})

	segments := []Segment{}
	if audioPath := r.packageAudio(base.PackageID, "opener", base.Language); audioPath != "" {
		segments = append(segments, Segment{Kind: "audio", Label: "opener", AudioPath: audioPath})
	} else if opener := r.packageText(base.PackageID, "opener", base.Language, "", nil); opener != "" {
		segments = append(segments, Segment{Kind: "opener", Label: "opener", Text: opener})
	}
	for index, obs := range append([]observation{snapshot.Primary}, snapshot.Observations...) {
		if strings.TrimSpace(obs.LocationName) == "" || (index > 0 && sameObservation(obs, snapshot.Primary)) {
			continue
		}
		reportSegments := r.renderObservationReportSegments(base, obs)
		if len(reportSegments) > 0 {
			label := "report"
			if index == 0 {
				label = "primary_report"
			}
			for _, segment := range reportSegments {
				segment.Label = segmentLabel(label, segment.Label)
				segments = append(segments, segment)
			}
		}
	}
	if len(segments) == 0 {
		return Product{}, fmt.Errorf("%s observations are unavailable for feed %s", strings.ToLower(titleForPackage(base.PackageID)), feed.ID)
	}
	base.Segments = segments
	return base, nil
}

func (r renderer) marineForecastProduct(base Product, feed feedXML) (Product, error) {
	var snapshot marineForecastSnapshot
	inputPath, ok := r.loadLiveMarineForecastSnapshot(feed, base.Language, &snapshot)
	if !ok {
		return Product{}, fmt.Errorf("marine forecast is unavailable for feed %s", feed.ID)
	}
	base.Title = "Marine Forecast"
	base.Inputs = append(base.Inputs, InputRef{Type: inputTypeForPath(inputPath), ID: inputPath})
	values := issueTimeTemplateValues(snapshot.IssuedAt, feed.Timezone, map[string]string{
		"source": "Environment Canada",
		"area":   snapshot.Area,
	})
	segments := []Segment{{
		Kind:  "opener",
		Label: "opener",
		Text:  r.packageText(base.PackageID, "opener", base.Language, "The {source} marine forecast for {area}, issued at {time}.", values),
	}}
	for _, item := range snapshot.Warnings {
		segments = append(segments, Segment{Kind: "warning", Label: segmentLabel(item.Name, "warning"), Text: r.packageTextFirst(base.PackageID, []string{"forecast.warnings.location", "warnings.location"}, base.Language, "For {location}. {warning}", map[string]string{
			"location": item.Name,
			"period":   item.Period,
			"warning":  sentence(item.Text),
		})})
	}
	for _, item := range snapshot.Regular {
		segments = append(segments, Segment{Kind: "package", Label: segmentLabel(item.Name, "regular"), Text: r.packageTextFirst(base.PackageID, []string{"forecast.regular.location", "regular.location"}, base.Language, "For {location}. {period} {wind}", map[string]string{
			"location": item.Name,
			"period":   sentence(item.Period),
			"wind":     sentence(item.Text),
		})})
	}
	for _, item := range snapshot.Waves {
		segments = append(segments, Segment{Kind: "package", Label: segmentLabel(item.Name, "waves"), Text: r.packageTextFirst(base.PackageID, []string{"forecast.waves.location", "waves.location"}, base.Language, "Waves for {location}. {period} {waves}", map[string]string{
			"location": item.Name,
			"period":   sentence(item.Period),
			"waves":    sentence(item.Text),
		})})
	}
	if len(snapshot.Extended) > 0 {
		if opener := r.packageTextFirst(base.PackageID, []string{"forecast.extended.opener", "extended.opener"}, base.Language, "The extended marine outlook.", nil); opener != "" {
			segments = append(segments, Segment{Kind: "opener", Label: "extended_opener", Text: sentence(opener)})
		}
		for _, item := range snapshot.Extended {
			segments = append(segments, Segment{Kind: "package", Label: segmentLabel("extended", item.Name), Text: r.packageTextFirst(base.PackageID, []string{"forecast.extended.period", "extended.period"}, base.Language, "For {period}. {forecast}", map[string]string{
				"period":   item.Name,
				"forecast": sentence(item.Text),
			})})
		}
	}
	base.Segments = segments
	return base, nil
}

func (r renderer) renderObservationReportSegments(base Product, obs observation) []Segment {
	values := observationTemplateValues(obs)
	parts := r.packageTextSequenceFiltered(base.PackageID, "report", base.Language, values)
	if len(parts) == 0 {
		parts = fullObservationBodyParts(obs)
	}
	if wind := r.renderObservationWindBlock(base, values); wind != "" {
		parts = append(parts, wind)
	}
	return appendTextSegments(nil, "package", "line", finishReportParts(parts))
}

func (r renderer) renderObservationWindBlock(base Product, values map[string]string) string {
	if values["wind_calm"] == "true" {
		return r.packageTextFirst(base.PackageID, []string{"report.winds.text.2", "winds.text.2"}, base.Language, "The wind was calm.", values)
	}
	if values["dir_text"] == "" || values["spd_metric"] == "" {
		return r.packageTextFirst(base.PackageID, []string{"report.winds.placeholder", "winds.placeholder"}, base.Language, "", values)
	}
	return r.packageTextFirst(base.PackageID, []string{"report.winds.text.1", "winds.text.1"}, base.Language, "Winds were {dir_text} at {spd_metric}", values)
}

func (r renderer) forecastProduct(base Product, feed feedXML) (Product, error) {
	var snapshot forecastSnapshot
	inputPath, ok := r.loadLiveForecastSnapshot(feed, base.Language, &snapshot)
	if !ok {
		return Product{}, fmt.Errorf("forecast information is unavailable for feed %s (%s)", feed.ID, forecastCoverageDebug(feed))
	}
	base.Title = "Forecast"
	base.Inputs = append(base.Inputs, InputRef{Type: inputTypeForPath(inputPath), ID: inputPath})
	callsign := fallbackText(feedCallsign(feed), feedSiteName(feed))
	segments := []Segment{}
	openerConfigured := r.packageTextConfigured(base.PackageID, "opener", base.Language)
	openerValues := issueTimeTemplateValues(snapshot.IssuedAt, feed.Timezone, map[string]string{
		"callsign": callsign,
		"site":     feedSiteName(feed),
		"source":   forecastSourceName(feed),
	})
	if isTelephoneProduct(base) {
		segments = append(segments, Segment{Kind: "opener", Label: "telephone_opener", Text: r.telephoneForecastOpener(base, feed, snapshot)})
	} else {
		segments = append(segments, Segment{Kind: "opener", Text: r.packageText(base.PackageID, "opener", base.Language, "Forecast for the {callsign} listening area:", openerValues)})
	}
	if issued := reportTime(snapshot.IssuedAt, feed.Timezone); issued != "" && !isTelephoneProduct(base) && (!openerConfigured || r.packageTextConfigured(base.PackageID, "issued_at", base.Language)) {
		segments = append(segments, Segment{Kind: "opener", Label: "issued_at", Text: r.packageText(base.PackageID, "issued_at", base.Language, "Issued at {time}.", map[string]string{
			"time":     issued,
			"callsign": callsign,
			"site":     feedSiteName(feed),
		})})
	}
	for _, region := range snapshot.Regions {
		name := normalizeRegionTitle(region.Name)
		if name != "" && !isTelephoneProduct(base) {
			segments = append(segments, Segment{Kind: "opener", Label: "forecast_region", Text: r.packageTextFirst(base.PackageID, forecastRegionTextKeys(name), base.Language, forecastRegionIntroFallback(name), map[string]string{
				"region": name,
			})})
		}
		segments = r.appendForecastPeriodSegments(segments, base, name, region.Periods)
	}
	segments = r.appendForecastPeriodSegments(segments, base, "", snapshot.Periods)
	if len(segments) == 1 {
		segments = append(segments, Segment{Kind: "package", Text: "No active forecast periods are available."})
	}
	base.Segments = segments
	return base, nil
}

func (r renderer) forecastPeriodText(base Product, period forecastPeriod) string {
	key := forecastPeriodTextKey(period.Name)
	keys := forecastPeriodTextKeys(key)
	if key == "" || !r.packageAnyTextConfigured(base.PackageID, keys, base.Language) {
		return periodForecastText(period)
	}
	periodValue, dayValue, nightValue := forecastPeriodTemplateValues(period.Name)
	rendered := r.packageTextFirst(base.PackageID, keys, base.Language, "", map[string]string{
		"period":      periodValue,
		"period_name": periodValue,
		"day":         dayValue,
		"night":       nightValue,
		"fc_text":     sentence(period.Text),
	})
	return sentence(rendered)
}

func (r renderer) appendForecastPeriodSegments(segments []Segment, base Product, regionName string, periods []forecastPeriod) []Segment {
	combined := []string{}
	combinedLabels := []string{}
	shortTermDay := ""
	for _, period := range periods {
		if period.Text == "" {
			continue
		}
		period = normalizeForecastPeriodName(period, shortTermDay)
		label := strings.TrimSpace(period.Name)
		text := r.forecastPeriodText(base, period)
		if text == "" {
			continue
		}
		if forecastPeriodIsShortTerm(period.Name, &shortTermDay) {
			segments = append(segments, Segment{Kind: "package", Label: forecastPeriodSegmentLabel(regionName, label), Text: text})
			continue
		}
		combined = append(combined, text)
		if label != "" {
			combinedLabels = append(combinedLabels, label)
		}
	}
	if len(combined) > 0 {
		label := "extended forecast"
		if len(combinedLabels) > 0 {
			label = strings.Join(combinedLabels, ", ")
		}
		if opener := r.packageTextFirst(base.PackageID, []string{"extended.opener"}, base.Language, "", nil); opener != "" {
			combined = append([]string{sentence(opener)}, combined...)
		}
		segments = append(segments, Segment{Kind: "package", Label: forecastPeriodSegmentLabel(regionName, label), Text: strings.Join(combined, " ")})
	}
	return segments
}

func normalizeForecastPeriodName(period forecastPeriod, previousDay string) forecastPeriod {
	name := strings.Join(strings.Fields(strings.TrimSpace(period.Name)), " ")
	if name == "" {
		return period
	}
	lower := strings.ToLower(strings.Trim(name, " .,:;-"))
	if (lower == "night" || lower == "overnight") && previousDay != "" {
		period.Name = previousDay + " " + name
	}
	return period
}

func forecastPeriodIsShortTerm(name string, shortTermDay *string) bool {
	key := forecastPeriodTextKey(name)
	if key == "today" || key == "tonight" {
		return true
	}
	day := forecastPeriodDayName(name)
	if day == "" {
		return false
	}
	if *shortTermDay == "" {
		*shortTermDay = day
		return true
	}
	return strings.EqualFold(*shortTermDay, day)
}

func forecastPeriodSegmentLabel(regionName string, periodLabel string) string {
	regionName = strings.TrimSpace(regionName)
	periodLabel = strings.TrimSpace(periodLabel)
	if regionName == "" {
		return periodLabel
	}
	if periodLabel == "" {
		return regionName
	}
	return regionName + ", " + periodLabel
}

func (r renderer) telephoneForecastOpener(base Product, feed feedXML, snapshot forecastSnapshot) string {
	name := telephoneForecastAreaName(feed, snapshot)
	issuedAt := reportTime(snapshot.IssuedAt, feed.Timezone)
	if issuedAt == "" {
		return r.packageText(base.PackageID, "telephone_opener_no_time", base.Language, "The official {source} forecast for the {name} area.", map[string]string{
			"source": forecastSourceName(feed),
			"name":   name,
		})
	}
	return r.packageText(base.PackageID, "telephone_opener", base.Language, "The official {source} forecast for the {name} area issued at {time}.", map[string]string{
		"source": forecastSourceName(feed),
		"name":   name,
		"time":   issuedAt,
	})
}

func telephoneForecastAreaName(feed feedXML, snapshot forecastSnapshot) string {
	if len(snapshot.Regions) == 1 {
		if name := normalizeRegionTitle(snapshot.Regions[0].Name); name != "" {
			return name
		}
	}
	return fallbackText(feedSiteName(feed), "your selected location")
}

func forecastSourceName(feed feedXML) string {
	for _, region := range feed.Locations.Coverage.Regions {
		switch canonicalSource(region.Source) {
		case "eccc":
			return "Environment Canada"
		case "nws":
			return "National Weather Service"
		}
	}
	return "weather services"
}

func isTelephoneProduct(product Product) bool {
	return strings.EqualFold(strings.TrimSpace(product.Metadata["audience"]), "telephone")
}

func forecastCoverageDebug(feed feedXML) string {
	if len(feed.Locations.Coverage.Regions) == 0 {
		return "no coverage regions"
	}
	capacity := len(feed.Locations.Coverage.Regions)
	if capacity > 3 {
		capacity = 3
	}
	parts := make([]string, 0, capacity)
	for index, region := range feed.Locations.Coverage.Regions {
		if index >= 3 {
			parts = append(parts, "...")
			break
		}
		parts = append(parts, fmt.Sprintf("region=%s source=%s forecast=%s", strings.TrimSpace(region.ID), canonicalSource(region.Source), fallbackText(region.DeriveForecast, region.ID)))
	}
	return strings.Join(parts, "; ")
}

func (r renderer) airQualityProduct(base Product, feed feedXML) (Product, error) {
	var snapshot airQualitySnapshot
	inputPath, ok := r.loadLiveAirQualitySnapshot(feed, base.Language, &snapshot)
	if !ok {
		return Product{}, fmt.Errorf("air quality information is unavailable for feed %s", feed.ID)
	}
	base.Title = "Air Quality"
	base.Inputs = append(base.Inputs, InputRef{Type: inputTypeForPath(inputPath), ID: inputPath})
	location := fallbackText(fallbackText(snapshot.Location, feedSiteName(feed)), "this area")
	segments := []Segment{}
	if snapshot.AQHI != "" {
		segments = append(segments, Segment{Kind: "package", Label: "observed", Text: r.packageText(base.PackageID, "now_eccc", base.Language, "The air quality health index was observed at {name} and reported a value of {val} at {time}.", map[string]string{
			"name": location,
			"val":  snapshot.AQHI,
			"time": fallbackText(reportTime(snapshot.ReportedAt, feed.Timezone), "the latest report time"),
		})})
	}
	if narrative := r.airQualityRiskNarrative(base.Language, snapshot.AQHI, snapshot.Risk); narrative != "" {
		segments = append(segments, Segment{Kind: "package", Label: "risk", Text: narrative})
	}
	if note := sentence(snapshot.SpecialNotes); note != "" {
		segments = append(segments, Segment{Kind: "package", Label: "special_notes", Text: note})
	}
	segments = append(segments, r.airQualityForecastSegments(base, location, snapshot.Periods)...)
	if len(snapshot.Periods) == 0 && snapshot.Forecast != "" {
		segments = append(segments, Segment{Kind: "package", Label: "forecast", Text: sentence(snapshot.Forecast)})
	}
	if len(segments) == 0 {
		segments = append(segments, Segment{Kind: "package", Label: "unavailable", Text: r.packageText(base.PackageID, "unavailable_report", base.Language, "The air quality information for {name} was unavailable.", map[string]string{
			"name": location,
		})})
	}
	base.Segments = segments
	return base, nil
}

func (r renderer) airQualityForecastSegments(base Product, location string, periods []airQualityPeriodSnapshot) []Segment {
	segments := []Segment{}
	for index := 0; index < len(periods) && index < 2; index++ {
		period := periods[index]
		if period.Name == "" || period.AQHI == "" {
			continue
		}
		key := "forecast_eccc"
		fallback := "For {period_name}, the maximum air quality health index is forecast to be {val}, or {risk}."
		label := "forecast_period"
		if index == 0 {
			key = "forecast_opener_eccc"
			fallback = "The air quality health index forecast for {name} is {val} for {period_name} and is considered {risk}."
			label = "forecast_opener"
		}
		segments = append(segments, Segment{Kind: "package", Label: label, Text: r.packageText(base.PackageID, key, base.Language, fallback, map[string]string{
			"name":        location,
			"val":         period.AQHI,
			"period_name": period.Name,
			"risk":        airQualityRiskLabel(base.Language, period.AQHI, period.Risk),
		})})
		if period.AQHIInSmoke != "" {
			segments = append(segments, Segment{Kind: "package", Label: "forecast_smoke", Text: r.packageText(base.PackageID, "forecast_insmoke_eccc", base.Language, "The air quality health index is expected to be {val} in smoke.", map[string]string{
				"val": period.AQHIInSmoke,
			})})
		}
	}
	if len(periods) >= 4 && periods[2].AQHI != "" && periods[3].AQHI != "" {
		segments = append(segments, Segment{Kind: "package", Label: "forecast_trailing", Text: r.packageText(base.PackageID, "forecast_trailing_eccc", base.Language, "{period2_val} on {period2_name}, and lastly, {period3_val} on {period3_name}.", map[string]string{
			"period2_val":  periods[2].AQHI,
			"period2_name": periods[2].Name,
			"period3_val":  periods[3].AQHI,
			"period3_name": periods[3].Name,
		})})
	} else if len(periods) >= 3 && periods[2].AQHI != "" {
		period := periods[2]
		segments = append(segments, Segment{Kind: "package", Label: "forecast_period", Text: r.packageText(base.PackageID, "forecast_eccc", base.Language, "For {period_name}, the maximum air quality health index is forecast to be {val}, or {risk}.", map[string]string{
			"period_name": period.Name,
			"val":         period.AQHI,
			"risk":        airQualityRiskLabel(base.Language, period.AQHI, period.Risk),
		})})
		if period.AQHIInSmoke != "" {
			segments = append(segments, Segment{Kind: "package", Label: "forecast_smoke", Text: r.packageText(base.PackageID, "forecast_insmoke_eccc", base.Language, "The air quality health index is expected to be {val} in smoke.", map[string]string{
				"val": period.AQHIInSmoke,
			})})
		}
	}
	return segments
}

func (r renderer) airQualityRiskNarrative(lang string, aqhi string, risk string) string {
	key := airQualityRiskKey(aqhi, risk)
	if key == "" {
		return ""
	}
	fallbacks := map[string]string{
		"low":       "This is ideal air quality for outdoor activities for both at-risk and general populations.",
		"moderate":  "This is acceptable air quality for outdoor activities for most people. However, at-risk individuals should consider reducing or rescheduling strenuous outdoor activities if symptoms occur.",
		"high":      "This is unhealthy air quality for at-risk individuals, which include children, seniors, and those with pre-existing respiratory or heart conditions. At-risk individuals should reduce or reschedule strenuous outdoor activities. The general population is not likely to be affected.",
		"very_high": "This is hazardous air quality for most people. At-risk individuals should avoid strenuous outdoor activities. Everyone else should also reduce or reschedule strenuous outdoor activities.",
	}
	return r.packageText("air_quality", key+"_eccc", lang, fallbacks[key], nil)
}

func (r renderer) climateProduct(base Product, feed feedXML) (Product, error) {
	var snapshot climateSnapshot
	inputPath, ok := r.loadLiveClimateSnapshot(feed, base.Language, &snapshot)
	if !ok {
		return Product{}, fmt.Errorf("climate summary information is unavailable for feed %s", feed.ID)
	}
	base.Title = "Climate Summary"
	base.Inputs = append(base.Inputs, InputRef{Type: inputTypeForPath(inputPath), ID: inputPath})
	location := fallbackText(snapshot.Location, feedSiteName(feed))
	segments := []Segment{{Kind: "opener", Text: r.packageText(base.PackageID, "opener", base.Language, "The Environment Canada climate summary.", map[string]string{
		"location": location,
		"site":     feedSiteName(feed),
		"source":   "Environment Canada",
	})}}
	for _, line := range snapshot.Summary {
		if text := sentence(strings.TrimSpace(line)); text != "" {
			segments = append(segments, Segment{Kind: "package", Text: text})
		}
	}
	base.Segments = segments
	return base, nil
}

func (r renderer) thunderstormOutlookProduct(base Product, feed feedXML) (Product, error) {
	var snapshot liveSpecialtyProductFile
	inputPath, ok := r.loadSpecialtyProductSnapshot("thunderstorm_outlook", feed, &snapshot)
	if !ok {
		return Product{}, fmt.Errorf("thunderstorm outlook information is unavailable for feed %s", feed.ID)
	}
	base.Title = "Thunderstorm Outlook"
	base.Inputs = append(base.Inputs, InputRef{Type: inputTypeForPath(inputPath), ID: inputPath})
	now := time.Now()
	site := fallbackText(feedSiteName(feed), "the listening")
	segments := []Segment{{
		Kind:  "opener",
		Label: "opener",
		Text: r.packageText(base.PackageID, "opener", base.Language, thunderstormOutlookOpener(snapshot, site, feed.Timezone), map[string]string{
			"site": site,
		}),
	}}
	for _, item := range sortedThunderstormItems(snapshot.Items) {
		if !specialtyItemCurrent(item, now) || specialtyString(item, "thunderstorm", base.Language) == "" {
			continue
		}
		if !thunderstormItemCoversSite(item) {
			continue
		}
		if hazard := thunderstormHazardText(item, site, base.Language, feed.Timezone, now); hazard != "" {
			segments = append(segments, Segment{Kind: "package", Label: "convective_hazard", Text: hazard})
		}
		if len(segments) >= 6 {
			break
		}
	}
	if len(segments) <= 1 {
		return Product{}, fmt.Errorf("thunderstorm outlook information is unavailable for feed %s", feed.ID)
	}
	base.Segments = segments
	return base, nil
}

func (r renderer) hydrometricProduct(base Product, feed feedXML) (Product, error) {
	var snapshot liveSpecialtyProductFile
	inputPath, ok := r.loadSpecialtyProductSnapshot("hydrometric", feed, &snapshot)
	if !ok {
		return Product{}, fmt.Errorf("river conditions are unavailable for feed %s", feed.ID)
	}
	base.Title = "River Conditions"
	base.Inputs = append(base.Inputs, InputRef{Type: inputTypeForPath(inputPath), ID: inputPath})
	segments := []Segment{}
	if opener := r.packageText(base.PackageID, "opener", base.Language, "River conditions for the {site} area.", map[string]string{"site": feedSiteName(feed)}); opener != "" {
		segments = append(segments, Segment{Kind: "opener", Label: "opener", Text: opener})
	}
	groups := hydrometricGroups(snapshot.Items)
	primarySegments := []Segment{}
	for _, item := range groups["primary"] {
		primarySegments = append(primarySegments, r.hydrometricGaugeSegments(base, "primary.gauge", "primary_gauge", item, base.Language, feed.Timezone)...)
	}
	if len(primarySegments) == 0 {
		if placeholder := r.packageText(base.PackageID, "primary.placeholder", base.Language, "", map[string]string{"site": feedSiteName(feed)}); placeholder != "" {
			primarySegments = append(primarySegments, Segment{Kind: "package", Label: "primary_placeholder", Text: placeholder})
		}
	}
	segments = append(segments, primarySegments...)
	nearbySegments := []Segment{}
	if len(groups["upstream"]) > 0 {
		if text := r.packageText(base.PackageID, "nearby.upstream", base.Language, "Upstream.", nil); text != "" {
			nearbySegments = append(nearbySegments, Segment{Kind: "package", Label: "nearby_upstream", Text: sentence(text)})
		}
		for _, item := range groups["upstream"] {
			nearbySegments = append(nearbySegments, r.hydrometricGaugeSegments(base, "nearby.gauge", "nearby_gauge", item, base.Language, feed.Timezone)...)
		}
	}
	if len(groups["upstream"]) > 0 && len(groups["downstream"]) > 0 {
		if text := r.packageText(base.PackageID, "nearby.and", base.Language, "and,", nil); text != "" {
			nearbySegments = append(nearbySegments, Segment{Kind: "package", Label: "nearby_and", Text: strings.TrimSpace(text)})
		}
	}
	if len(groups["downstream"]) > 0 {
		if text := r.packageText(base.PackageID, "nearby.downstream", base.Language, "Downstream.", nil); text != "" {
			nearbySegments = append(nearbySegments, Segment{Kind: "package", Label: "nearby_downstream", Text: sentence(text)})
		}
		for _, item := range groups["downstream"] {
			nearbySegments = append(nearbySegments, r.hydrometricGaugeSegments(base, "nearby.gauge", "nearby_gauge", item, base.Language, feed.Timezone)...)
		}
	}
	segments = append(segments, nearbySegments...)
	if len(segments) <= 1 || (len(primarySegments) == 0 && len(nearbySegments) == 0) {
		return Product{}, fmt.Errorf("river conditions are unavailable for feed %s", feed.ID)
	}
	base.Segments = segments
	return base, nil
}

func (r renderer) thunderstormOutlookText(item map[string]any, lang string, timezone string) string {
	area := fallbackText(specialtyString(item, "area", lang), "the region")
	thunderstorm := fallbackText(specialtyString(item, "thunderstorm", lang), "thunderstorms")
	parts := []string{fmt.Sprintf("For %s, the thunderstorm outlook calls for %s thunderstorms", area, strings.ToLower(thunderstorm))}
	if risk := specialtyLevel(item, "risk"); risk != "" {
		parts = append(parts, "risk level "+risk)
	}
	if confidence := specialtyLevel(item, "confidence"); confidence != "" {
		parts = append(parts, "confidence level "+confidence)
	}
	if impact := specialtyLevel(item, "impact"); impact != "" {
		parts = append(parts, "impact level "+impact)
	}
	if specialtyBool(item, "tornado") {
		parts = append(parts, "a tornado risk is indicated")
	}
	if hail := specialtyPositiveNumber(item, "hail_cm"); hail != "" {
		parts = append(parts, "hail up to "+hail+" centimetres")
	}
	if gust := specialtyPositiveNumber(item, "gust_kmh"); gust != "" {
		parts = append(parts, "gusts up to "+gust+" kilometres per hour")
	}
	if rain := specialtyPositiveNumber(item, "rain_mm"); rain != "" {
		parts = append(parts, "rainfall near "+rain+" millimetres")
	}
	if valid := reportTime(specialtyString(item, "valid_at", lang), timezone); valid != "" {
		parts = append(parts, "valid around "+valid)
	}
	return sentence(strings.Join(parts, "; "))
}

func sortedThunderstormItems(items []map[string]any) []map[string]any {
	out := append([]map[string]any(nil), items...)
	sort.SliceStable(out, func(i, j int) bool {
		leftValid := specialtyString(out[i], "valid_at", "en")
		rightValid := specialtyString(out[j], "valid_at", "en")
		if leftValid != rightValid {
			return leftValid < rightValid
		}
		leftRisk, _ := numberFromAny(out[i]["risk"])
		rightRisk, _ := numberFromAny(out[j]["risk"])
		if leftRisk != rightRisk {
			return leftRisk > rightRisk
		}
		leftImpact, _ := numberFromAny(out[i]["impact"])
		rightImpact, _ := numberFromAny(out[j]["impact"])
		if leftImpact != rightImpact {
			return leftImpact > rightImpact
		}
		return specialtyString(out[i], "id", "en") < specialtyString(out[j], "id", "en")
	})
	return out
}

func thunderstormOutlookHeader(item map[string]any, timezone string, now time.Time) string {
	published := specialtyString(item, "published_at", "en")
	validAt := specialtyString(item, "valid_at", "en")
	expiresAt := specialtyString(item, "expires_at", "en")
	day := thunderstormDayLabel(validAt, timezone, now)
	title := "Environment Canada thunderstorm outlook"
	if day != "" {
		title += " for " + day
	}
	parts := []string{sentence(title)}
	if publishedText := thunderstormPublishedText(published, timezone); publishedText != "" {
		parts = append(parts, sentence("Published "+publishedText))
	}
	if validText := thunderstormValidityText(validAt, expiresAt, timezone); validText != "" {
		parts = append(parts, sentence("Valid "+validText))
	}
	return strings.Join(parts, " ")
}

func thunderstormOutlookOpener(_ liveSpecialtyProductFile, site string, _ string) string {
	return fmt.Sprintf("The Environment Canada Thunderstorm Outlook covering %s.", thunderstormAreaPhrase(site))
}

func thunderstormHazardText(item map[string]any, site string, lang string, timezone string, now time.Time) string {
	period := thunderstormPeriodIntro(item, timezone, now)
	if thunderstormNoHazardExpected(item, lang) {
		return sentence(period + " no hazardous weather is expected.")
	}
	risk := fallbackText(thunderstormRiskLabel(item["risk"]), "minor")
	text := fmt.Sprintf("%s a %s convective risk is anticipated.", period, risk)
	if specialtyBool(item, "tornado") {
		text += " A tornado risk is also indicated."
	}
	if hazards := thunderstormAssociatedHazards(item); hazards != "" {
		text += " " + thunderstormHazardsSentence(hazards)
	}
	return sentence(text)
}

func thunderstormAreaPhrase(site string) string {
	site = strings.TrimSpace(site)
	if site == "" {
		return "the listening area"
	}
	lower := strings.ToLower(site)
	if lower == "saskatoon" {
		return "the City of Saskatoon area"
	}
	if strings.HasPrefix(lower, "the ") || strings.HasSuffix(lower, " area") || strings.HasSuffix(lower, " region") {
		return site
	}
	return "the " + site + " area"
}

func thunderstormItemCoversSite(item map[string]any) bool {
	distance, ok := numberFromAny(item["distance_km"])
	return !ok || distance <= 0.5
}

func thunderstormNoHazardExpected(item map[string]any, lang string) bool {
	if risk, ok := numberFromAny(item["risk"]); ok && risk <= 0 {
		return true
	}
	thunderstorm := readableSpecialtyToken(specialtyString(item, "thunderstorm", lang))
	switch thunderstorm {
	case "none", "no", "nil", "no thunderstorms", "not expected":
		return true
	default:
		return false
	}
}

func thunderstormPeriodIntro(item map[string]any, timezone string, now time.Time) string {
	label := thunderstormPeriodLabel(
		specialtyString(item, "valid_at", "en"),
		specialtyString(item, "expires_at", "en"),
		timezone,
		now,
	)
	if label == "" {
		return "For this outlook period,"
	}
	return label + ","
}

func thunderstormPeriodLabel(startRaw string, endRaw string, timezone string, now time.Time) string {
	start, err := parseLooseTime(startRaw)
	if err != nil {
		return ""
	}
	end, _ := parseLooseTime(endRaw)
	loc := now.Location()
	if loaded, locErr := time.LoadLocation(fallbackText(timezone, "Local")); locErr == nil {
		loc = loaded
	}
	start = start.In(loc)
	if !end.IsZero() {
		end = end.In(loc)
	}
	now = now.In(loc)
	period := thunderstormPeriodName(start, end)
	if period == "" {
		return thunderstormClockPeriodLabel(start, now)
	}
	switch {
	case sameDate(start, now):
		if period == "overnight" {
			return "Tonight"
		}
		return "This " + period
	default:
		if period == "overnight" && sameDate(start, now.AddDate(0, 0, 1)) {
			return "Overnight"
		}
		return "For " + start.Format("Monday") + " " + period
	}
}

func thunderstormPeriodName(start time.Time, end time.Time) string {
	hour := start.Hour()
	if hour >= 0 && hour < 6 {
		return "overnight"
	}
	if hour >= 6 && hour < 12 {
		return "morning"
	}
	if hour >= 12 && hour < 17 {
		return "afternoon"
	}
	if hour >= 17 && hour < 22 {
		return "evening"
	}
	return "overnight"
}

func thunderstormClockPeriodLabel(start time.Time, now time.Time) string {
	switch {
	case sameDate(start, now):
		return "Around " + strings.ToLower(compactClock(start)) + " today"
	case sameDate(start, now.AddDate(0, 0, 1)):
		return "Around " + strings.ToLower(compactClock(start)) + " tomorrow"
	default:
		return "Around " + strings.ToLower(compactClock(start)) + " on " + start.Format("January 2")
	}
}

func thunderstormAssociatedHazards(item map[string]any) string {
	parts := []string{}
	if gust := thunderstormAmount(item["gust_kmh"], "kilometers per hour"); gust != "" {
		parts = append(parts, "wind gusts up to "+gust)
	}
	if hail := thunderstormAmount(item["hail_cm"], "centimeters of hail"); hail != "" {
		parts = append(parts, hail)
	}
	if rain := thunderstormAmount(item["rain_mm"], "millimeters of rain"); rain != "" {
		parts = append(parts, rain)
	}
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

func thunderstormHazardsSentence(hazards string) string {
	verb := "are"
	if !strings.Contains(hazards, ",") && !strings.Contains(hazards, " and ") {
		verb = "is"
	}
	return sentence(capitalizeFirst(hazards) + " " + verb + " associated with this convective risk")
}

func capitalizeFirst(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.ToUpper(value[:1]) + value[1:]
}

func thunderstormAmount(value any, unit string) string {
	if values, ok := value.([]any); ok {
		numbers := []string{}
		for _, item := range values {
			if numeric, ok := numberFromAny(item); ok && numeric > 0 {
				numbers = append(numbers, rounded(numeric))
			}
		}
		if len(numbers) == 0 {
			return ""
		}
		return strings.Join(numbers, " to ") + " " + unit
	}
	numeric, ok := numberFromAny(value)
	if !ok || numeric <= 0 {
		return ""
	}
	return rounded(numeric) + " " + thunderstormUnitForValue(numeric, unit)
}

func thunderstormUnitForValue(value float64, unit string) string {
	if math.Abs(value-1) > 0.05 {
		return unit
	}
	replacements := map[string]string{
		"millimeters": "millimeter",
		"centimeters": "centimeter",
		"kilometers":  "kilometer",
	}
	for plural, singular := range replacements {
		if strings.Contains(unit, plural) {
			return strings.Replace(unit, plural, singular, 1)
		}
	}
	return unit
}

func thunderstormRiskLabel(value any) string {
	numeric, ok := numberFromAny(value)
	if !ok {
		return readableSpecialtyToken(localizedString(value, "en"))
	}
	switch int(math.Round(numeric)) {
	case 1:
		return "minor"
	case 2:
		return "moderate"
	case 3:
		return "high"
	case 4:
		return "extreme"
	default:
		return "level " + rounded(numeric)
	}
}

func thunderstormDayLabel(raw string, timezone string, now time.Time) string {
	parsed, err := parseLooseTime(raw)
	if err != nil {
		return "today"
	}
	loc := now.Location()
	if loaded, locErr := time.LoadLocation(fallbackText(timezone, "Local")); locErr == nil {
		loc = loaded
	}
	validDate := parsed.In(loc)
	nowDate := now.In(loc)
	switch {
	case sameDate(validDate, nowDate):
		return "today"
	case sameDate(validDate, nowDate.AddDate(0, 0, 1)):
		return "tomorrow"
	default:
		return validDate.Format("January 2")
	}
}

func thunderstormPublishedText(raw string, timezone string) string {
	parsed, err := parseLooseTime(raw)
	if err != nil {
		return ""
	}
	if loc, locErr := time.LoadLocation(fallbackText(timezone, "Local")); locErr == nil {
		parsed = parsed.In(loc)
	}
	return "at " + compactClock(parsed) + " on " + parsed.Format("January 2")
}

func thunderstormValidityText(startRaw string, endRaw string, timezone string) string {
	start, startErr := parseLooseTime(startRaw)
	end, endErr := parseLooseTime(endRaw)
	if startErr != nil || endErr != nil {
		return ""
	}
	if loc, locErr := time.LoadLocation(fallbackText(timezone, "Local")); locErr == nil {
		start = start.In(loc)
		end = end.In(loc)
	}
	return "from " + compactClock(start) + " on " + start.Format("January 2") + " through " + compactClock(end) + " " + timezoneName(end.Format("MST")) + " on " + end.Format("January 2, 2006")
}

func compactClock(value time.Time) string {
	if value.Minute() == 0 {
		return value.Format("3 PM")
	}
	return value.Format("3:04 PM")
}

func sameDate(left time.Time, right time.Time) bool {
	ly, lm, ld := left.Date()
	ry, rm, rd := right.Date()
	return ly == ry && lm == rm && ld == rd
}

func (r renderer) hydrometricText(item map[string]any, lang string, timezone string) string {
	station := fallbackText(specialtyString(item, "station", lang), "the river gauge")
	parts := []string{"At " + station}
	if level := hydrometricLevelText(item, lang); level != "" {
		parts = append(parts, "the water level was "+level)
	}
	if discharge := hydrometricDischargeText(item); discharge != "" {
		parts = append(parts, "the discharge was "+discharge)
	}
	if observed := reportTime(specialtyString(item, "observed_at", lang), timezone); observed != "" {
		parts = append(parts, "reported at "+observed)
	}
	return sentence(strings.Join(parts, ", "))
}

func (r renderer) hydrometricGaugeText(base Product, prefix string, item map[string]any, lang string, timezone string) string {
	return strings.Join(r.hydrometricGaugeParts(base, prefix, item, lang, timezone), " ")
}

func (r renderer) hydrometricGaugeSegments(base Product, prefix string, label string, item map[string]any, lang string, timezone string) []Segment {
	return appendTextSegments(nil, "package", label, r.hydrometricGaugeParts(base, prefix, item, lang, timezone))
}

func (r renderer) hydrometricGaugeParts(base Product, prefix string, item map[string]any, lang string, timezone string) []string {
	values := hydrometricTemplateValues(item, lang, timezone)
	parts := r.packageTextSequenceFiltered(base.PackageID, prefix, lang, values)
	if len(parts) == 0 {
		return []string{r.hydrometricText(item, lang, timezone)}
	}
	return finishReportParts(parts)
}

func hydrometricTemplateValues(item map[string]any, lang string, timezone string) map[string]string {
	station := firstNonBlank(specialtyString(item, "station", lang), specialtyString(item, "station_id", lang), "the river gauge")
	values := map[string]string{
		"water_station": station,
		"station":       station,
		"level":         hydrometricLevelText(item, lang),
		"discharge":     hydrometricDischargeText(item),
	}
	if observed := reportTime(specialtyString(item, "observed_at", lang), timezone); observed != "" {
		values["observed_at"] = observed
	}
	return values
}

func hydrometricLevelText(item map[string]any, lang string) string {
	level := specialtyNumber(item, "level_m")
	if level == "" {
		return ""
	}
	text := level + " metres"
	if note := specialtyString(item, "level_note", lang); note != "" {
		text += " " + note
	}
	return text
}

func hydrometricDischargeText(item map[string]any) string {
	discharge := specialtyNumber(item, "discharge")
	if discharge == "" {
		return ""
	}
	return discharge + " cubic metres per second"
}

func hydrometricGroups(items []map[string]any) map[string][]map[string]any {
	out := map[string][]map[string]any{
		"primary":    {},
		"upstream":   {},
		"downstream": {},
	}
	sorted := append([]map[string]any(nil), items...)
	sort.SliceStable(sorted, func(i, j int) bool {
		left, _ := numberFromAny(sorted[i]["order"])
		right, _ := numberFromAny(sorted[j]["order"])
		if left != right {
			return left < right
		}
		return specialtyString(sorted[i], "station_id", "en") < specialtyString(sorted[j], "station_id", "en")
	})
	for _, item := range sorted {
		relation := strings.ToLower(strings.TrimSpace(specialtyString(item, "relation", "en")))
		switch relation {
		case "upstream", "downstream":
			out[relation] = append(out[relation], item)
		default:
			out["primary"] = append(out["primary"], item)
		}
	}
	return out
}

func specialtyItemCurrent(item map[string]any, now time.Time) bool {
	expires := specialtyString(item, "expires_at", "en")
	if expires == "" {
		return true
	}
	parsed, err := parseLooseTime(expires)
	if err != nil {
		return true
	}
	return parsed.After(now.Add(-5 * time.Minute))
}

func specialtyString(item map[string]any, key string, lang string) string {
	if item == nil {
		return ""
	}
	return localizedString(item[key], lang)
}

func specialtyNumber(item map[string]any, key string) string {
	value, ok := numberFromAny(item[key])
	if !ok {
		return ""
	}
	return rounded(value)
}

func specialtyPositiveNumber(item map[string]any, key string) string {
	value, ok := numberFromAny(item[key])
	if !ok || value <= 0 {
		return ""
	}
	return rounded(value)
}

func specialtyLevel(item map[string]any, key string) string {
	if value := specialtyString(item, key, "en"); value != "" {
		return readableSpecialtyToken(value)
	}
	return specialtyNumber(item, key)
}

func specialtyBool(item map[string]any, key string) bool {
	switch value := item[key].(type) {
	case bool:
		return value
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "y":
			return true
		}
	}
	return false
}

func readableSpecialtyToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, "_", " ")
	value = strings.ReplaceAll(value, "-", " ")
	return strings.Join(strings.Fields(strings.ToLower(value)), " ")
}

func (r renderer) userBulletinProduct(base Product, feed feedXML) (Product, error) {
	var snapshot bulletinSnapshot
	inputPath, ok := r.loadLiveBulletinSnapshot(feed, base.Language, &snapshot)
	if !ok {
		return Product{}, fmt.Errorf("no active user bulletins are available for feed %s", feed.ID)
	}
	base.Title = fallbackText(snapshot.Title, "User Bulletin")
	base.Inputs = append(base.Inputs, InputRef{Type: inputTypeForPath(inputPath), ID: inputPath})
	if strings.EqualFold(strings.TrimSpace(snapshot.ContentType), "audio") {
		if base.Metadata == nil {
			base.Metadata = map[string]string{}
		}
		base.Metadata["content_type"] = "audio"
		if strings.TrimSpace(snapshot.AudioPath) != "" {
			base.Metadata["audio_path"] = strings.TrimSpace(snapshot.AudioPath)
		}
		if strings.TrimSpace(snapshot.AudioURL) != "" {
			base.Metadata["audio_url"] = strings.TrimSpace(snapshot.AudioURL)
		}
		return base, nil
	}
	for _, line := range snapshot.Lines {
		if text := sentence(strings.TrimSpace(line)); text != "" {
			base.Segments = append(base.Segments, Segment{Kind: "package", Text: text})
		}
	}
	if len(base.Segments) == 0 {
		base.Segments = []Segment{{Kind: "package", Text: "There are no user bulletins at this time."}}
	}
	return base, nil
}

func (r renderer) textStoreProduct(base Product, title string, source string, id string, clean func(string) string) (Product, error) {
	if r.cfg.Store == nil {
		return Product{}, fmt.Errorf("%s is unavailable", strings.ToLower(title))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	body, ok, err := r.cfg.Store.TextProduct(ctx, source, id)
	if err != nil || !ok {
		return Product{}, fmt.Errorf("%s is unavailable", strings.ToLower(title))
	}
	if clean != nil {
		body = clean(body)
	}
	text := cleanPlaintextProduct(body)
	if text == "" {
		return Product{}, fmt.Errorf("%s is empty", strings.ToLower(title))
	}
	base.Title = title
	base.Inputs = append(base.Inputs, InputRef{Type: "store", ID: fmt.Sprintf("products.text/%s/%s", source, id)})
	base.Segments = appendTextSegments(nil, "package", "text", splitPlaintextProductSegments(text))
	return base, nil
}

func cleanWWVProduct(raw string) string {
	lines := strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n")
	out := make([]string, 0, len(lines))
	seenBody := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !seenBody {
			lower := strings.ToLower(trimmed)
			if trimmed == "" ||
				strings.HasPrefix(trimmed, ":") ||
				strings.HasPrefix(trimmed, "#") ||
				strings.Contains(lower, "geophysical alert message") ||
				strings.Contains(lower, "prepared by the us dept. of commerce") {
				continue
			}
		}
		if trimmed != "" {
			seenBody = true
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func unavailableProduct(base Product, title string, text string) Product {
	base.Title = title
	base.Segments = []Segment{{Kind: "package", Label: "unavailable", Text: text}}
	return base
}

func (r renderer) packageText(pkgID string, key string, lang string, fallback string, values map[string]string) string {
	text := fallback
	pkgID = strings.ToLower(strings.TrimSpace(pkgID))
	key = strings.ToLower(strings.TrimSpace(key))
	if byKey, ok := r.cfg.ProductText[pkgID]; ok {
		if byLang, ok := byKey[key]; ok {
			if configured := localizedTextEntry(byLang, lang); configured != "" {
				text = configured
			}
		}
	}
	return renderTemplateText(text, values)
}

func (r renderer) packageAudio(pkgID string, key string, lang string) string {
	pkgID = strings.ToLower(strings.TrimSpace(pkgID))
	key = strings.ToLower(strings.TrimSpace(key))
	if byKey, ok := r.cfg.ProductAudio[pkgID]; ok {
		if byLang, ok := byKey[key]; ok {
			if configured := localizedTextEntry(byLang, lang); configured != "" && r.audioFileExists(configured) {
				return configured
			}
		}
	}
	return ""
}

func (r renderer) audioFileExists(path string) bool {
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}
	resolved := resolvePath(r.cfg.BaseDir, path)
	info, err := os.Stat(filepath.Clean(resolved))
	return err == nil && !info.IsDir()
}

func (r renderer) packageTextFirst(pkgID string, keys []string, lang string, fallback string, values map[string]string) string {
	for _, key := range keys {
		if r.packageTextConfigured(pkgID, key, lang) {
			return r.packageText(pkgID, key, lang, fallback, values)
		}
	}
	return renderTemplateText(fallback, values)
}

func (r renderer) packageTextConfigured(pkgID string, key string, lang string) bool {
	pkgID = strings.ToLower(strings.TrimSpace(pkgID))
	key = strings.ToLower(strings.TrimSpace(key))
	if byKey, ok := r.cfg.ProductText[pkgID]; ok {
		if byLang, ok := byKey[key]; ok {
			return localizedTextEntry(byLang, lang) != ""
		}
	}
	return false
}

func (r renderer) packageAnyTextConfigured(pkgID string, keys []string, lang string) bool {
	for _, key := range keys {
		if r.packageTextConfigured(pkgID, key, lang) {
			return true
		}
	}
	return false
}

func (r renderer) packageTextSequence(pkgID string, prefix string, lang string, values map[string]string) []string {
	var out []string
	prefix = strings.Trim(strings.ToLower(strings.TrimSpace(prefix)), ".")
	for index := 1; index < 100; index++ {
		key := fmt.Sprintf("text.%d", index)
		if prefix != "" {
			key = prefix + "." + key
		}
		if !r.packageTextConfigured(pkgID, key, lang) {
			break
		}
		if text := r.packageText(pkgID, key, lang, "", values); text != "" {
			out = append(out, text)
		}
	}
	return out
}

func (r renderer) packageTextSequenceFiltered(pkgID string, prefix string, lang string, values map[string]string) []string {
	var out []string
	prefix = strings.Trim(strings.ToLower(strings.TrimSpace(prefix)), ".")
	for index := 1; index < 100; index++ {
		key := fmt.Sprintf("text.%d", index)
		if prefix != "" {
			key = prefix + "." + key
		}
		if !r.packageTextConfigured(pkgID, key, lang) {
			break
		}
		raw := r.packageTextRaw(pkgID, key, lang)
		if templateHasMissingValue(raw, values) {
			continue
		}
		if text := renderTemplateText(raw, values); text != "" {
			out = append(out, text)
		}
	}
	return out
}

func (r renderer) packageTextRaw(pkgID string, key string, lang string) string {
	pkgID = strings.ToLower(strings.TrimSpace(pkgID))
	key = strings.ToLower(strings.TrimSpace(key))
	if byKey, ok := r.cfg.ProductText[pkgID]; ok {
		if byLang, ok := byKey[key]; ok {
			return localizedTextEntry(byLang, lang)
		}
	}
	return ""
}

func templateHasMissingValue(template string, values map[string]string) bool {
	for _, name := range templateVariableNames(template) {
		if strings.TrimSpace(values[name]) == "" {
			return true
		}
	}
	return false
}

func templateVariableNames(template string) []string {
	matches := templateVariableRE.FindAllStringSubmatch(template, -1)
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			out = append(out, match[1])
		}
	}
	return out
}

func localizedTextEntry(values map[string]string, lang string) string {
	if len(values) == 0 {
		return ""
	}
	lang = normalizeLangKey(lang)
	short := lang
	if idx := strings.Index(short, "-"); idx > 0 {
		short = short[:idx]
	}
	for _, key := range []string{lang, short, "en-ca", "en", "*"} {
		if text := strings.TrimSpace(values[key]); text != "" {
			return text
		}
		for rawKey, text := range values {
			if strings.EqualFold(rawKey, key) && strings.TrimSpace(text) != "" {
				return strings.TrimSpace(text)
			}
		}
	}
	return ""
}

func renderTemplateText(text string, values map[string]string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	for key, value := range values {
		text = strings.ReplaceAll(text, "{"+key+"}", strings.TrimSpace(value))
	}
	return normalizeRenderedTemplateText(text)
}

func normalizeRenderedTemplateText(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	lines := strings.Split(text, "\n")
	for index := range lines {
		lines[index] = strings.Join(strings.Fields(lines[index]), " ")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func reportText(parts []string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = normalizeRenderedTemplateText(part)
		if part != "" {
			clean = append(clean, part)
		}
	}
	if len(clean) == 0 {
		return ""
	}
	return finishReportText(strings.Join(clean, "\n"))
}

func finishReportParts(parts []string) []string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = normalizeRenderedTemplateText(part)
		if part != "" {
			clean = append(clean, part)
		}
	}
	if len(clean) == 0 {
		return nil
	}
	clean[len(clean)-1] = finishReportText(clean[len(clean)-1])
	return clean
}

func finishReportText(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	last := text[len(text)-1]
	switch last {
	case '.', '!', '?':
		return text
	case ',', ';', ':':
		return strings.TrimRight(text, " ,;:") + "."
	default:
		return text + "."
	}
}

func appendTextSegments(segments []Segment, kind string, labelPrefix string, parts []string) []Segment {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		if text := normalizeRenderedTemplateText(part); text != "" {
			clean = append(clean, text)
		}
	}
	for index, text := range clean {
		label := labelPrefix
		if len(clean) > 1 {
			label = fmt.Sprintf("%s_%d", labelPrefix, index+1)
		}
		segments = append(segments, Segment{Kind: kind, Label: label, Text: text})
	}
	return segments
}

func segmentLabel(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, "_")
}

func splitPlaintextProductSegments(text string) []string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	rawParts := strings.Split(text, "\n")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.TrimSpace(part)
		if part != "" {
			parts = append(parts, part)
		}
	}
	if len(parts) == 0 && strings.TrimSpace(text) != "" {
		parts = append(parts, strings.TrimSpace(text))
	}
	return parts
}

func inputTypeForPath(path string) string {
	if strings.HasPrefix(path, "store:") {
		return "store"
	}
	if strings.Contains(filepath.ToSlash(path), "/managed/") {
		return "managed"
	}
	return "input"
}

func fullObservationText(obs observation) string {
	return sentence(strings.Join(fullObservationParts(obs), ". "))
}

func fullObservationParts(obs observation) []string {
	return fullObservationPartsWithWind(obs, true)
}

func fullObservationBodyParts(obs observation) []string {
	return fullObservationPartsWithWind(obs, false)
}

func fullObservationPartsWithWind(obs observation, includeWind bool) []string {
	location := fallbackText(obs.LocationName, "the reporting station")
	parts := []string{fmt.Sprintf("The weather at %s", location)}
	if obs.Condition != "" {
		parts[0] += " was " + obs.Condition
	}
	if obs.TemperatureC != nil {
		parts = append(parts, fmt.Sprintf("The temperature was %s", degrees(*obs.TemperatureC)))
	}
	if obs.DewpointC != nil {
		parts = append(parts, fmt.Sprintf("dewpoint %s", degreesBare(*obs.DewpointC)))
	}
	if obs.HumidityPercent != nil {
		parts = append(parts, fmt.Sprintf("and the relative humidity was %s percent", rounded(*obs.HumidityPercent)))
	}
	if includeWind {
		if wind := windText(obs, true); wind != "" {
			parts = append(parts, wind)
		}
	}
	if obs.VisibilityKM != nil {
		parts = append(parts, fmt.Sprintf("Visibility was up to %s kilometres", rounded(*obs.VisibilityKM)))
	}
	if obs.PressureKPA != nil {
		pressure := fmt.Sprintf("The pressure was %s kilopascals", oneDecimal(*obs.PressureKPA))
		if obs.PressureTendency != "" {
			pressure += " and " + strings.ToLower(obs.PressureTendency)
		}
		parts = append(parts, pressure)
	}
	return parts
}

func shortObservationText(obs observation) string {
	return sentence(strings.Join(shortObservationParts(obs), ", "))
}

func shortObservationParts(obs observation) []string {
	return shortObservationPartsWithWind(obs, true)
}

func shortObservationBodyParts(obs observation) []string {
	return shortObservationPartsWithWind(obs, false)
}

func shortObservationPartsWithWind(obs observation, includeWind bool) []string {
	if obs.LocationName == "" {
		return nil
	}
	parts := []string{obs.LocationName}
	if obs.TemperatureC != nil {
		parts = append(parts, degrees(*obs.TemperatureC))
	}
	if includeWind {
		if wind := windText(obs, false); wind != "" {
			parts = append(parts, wind)
		}
	}
	return parts
}

func repeatObservationText(obs observation) string {
	if obs.LocationName == "" {
		return ""
	}
	parts := []string{fmt.Sprintf("Again, at %s", obs.LocationName)}
	if obs.Condition != "" {
		parts = append(parts, "it was "+obs.Condition)
	}
	if obs.TemperatureC != nil {
		parts = append(parts, "with a temperature of "+degrees(*obs.TemperatureC))
	}
	return sentence(strings.Join(parts, " "))
}

func windText(obs observation, sentenceCase bool) string {
	if obs.WindDirection == "" && obs.WindSpeedKMH == nil {
		return ""
	}
	prefix := "winds were"
	if sentenceCase {
		prefix = "Winds were"
	}
	direction := readableDirection(obs.WindDirection)
	if obs.WindSpeedKMH == nil {
		return strings.TrimSpace(prefix + " " + direction)
	}
	text := strings.TrimSpace(fmt.Sprintf("%s %s at %s kilometres per hour", prefix, direction, rounded(*obs.WindSpeedKMH)))
	if obs.WindGustKMH != nil && *obs.WindGustKMH > *obs.WindSpeedKMH {
		text += fmt.Sprintf(" with gusts up to %s kilometres per hour", rounded(*obs.WindGustKMH))
	}
	return text
}

func observationTemplateValues(obs observation) map[string]string {
	weather := fallbackText(obs.Condition, obs.SkyCondition)
	values := map[string]string{
		"location":      fallbackText(obs.LocationName, obs.ID),
		"weather":       weather,
		"dir_text":      readableDirection(obs.WindDirection),
		"sky_condition": obs.SkyCondition,
	}
	if obs.TemperatureC != nil {
		values["ctemp"] = rounded(*obs.TemperatureC)
	}
	if obs.DewpointC != nil {
		values["c_dewp"] = rounded(*obs.DewpointC)
	}
	if obs.HumidityPercent != nil {
		values["hum"] = rounded(*obs.HumidityPercent)
	}
	if obs.VisibilityKM != nil {
		values["visibility"] = aviationVisibility(*obs.VisibilityKM)
	}
	if obs.PressureKPA != nil {
		values["pressure"] = oneDecimal(*obs.PressureKPA) + " kilopascals"
		values["altimeter"] = altimeterInHg(*obs.PressureKPA)
	}
	if strings.TrimSpace(obs.Altimeter) != "" {
		values["altimeter"] = strings.TrimSpace(obs.Altimeter)
	}
	if obs.WindSpeedKMH != nil {
		values["spd_metric"] = rounded(*obs.WindSpeedKMH) + " kilometres per hour"
		values["spd_maritime"] = wholeKnots(*obs.WindSpeedKMH)
		values["spd_aviation"] = wholeKnots(*obs.WindSpeedKMH)
		if *obs.WindSpeedKMH < 1 {
			values["wind_calm"] = "true"
		}
	}
	if obs.WindGustKMH != nil {
		values["gust_metric"] = rounded(*obs.WindGustKMH) + " kilometres per hour"
		values["gust_aviation"] = wholeKnots(*obs.WindGustKMH)
	}
	return values
}

func wholeKnots(kmh float64) string {
	return strconv.Itoa(int(math.Round(kmhToKnots(kmh)))) + " knots"
}

func kmhToKnots(value float64) float64 {
	return value / 1.852
}

func altimeterInHg(pressureKPA float64) string {
	return fmt.Sprintf("%.2f inches", pressureKPA*0.295299830714)
}

func aviationVisibility(visibilityKM float64) string {
	miles := visibilityKM * 0.621371
	if miles >= 10 {
		return strconv.Itoa(int(math.Round(miles))) + " statute miles"
	}
	if math.Abs(miles-math.Round(miles)) < 0.05 {
		return rounded(miles) + " statute miles"
	}
	return oneDecimal(miles) + " statute miles"
}

func sameObservation(left observation, right observation) bool {
	if left.ID != "" && strings.EqualFold(left.ID, right.ID) {
		return true
	}
	return left.LocationName != "" && strings.EqualFold(left.LocationName, right.LocationName)
}

func uniqueAreaObservations(observations []observation, primary observation) []observation {
	seen := map[string]struct{}{}
	remember := func(obs observation) {
		for _, key := range observationDedupeKeys(obs) {
			seen[key] = struct{}{}
		}
	}
	remember(primary)
	out := make([]observation, 0, len(observations))
	for _, obs := range observations {
		keys := observationDedupeKeys(obs)
		duplicate := false
		for _, key := range keys {
			if _, ok := seen[key]; ok {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		out = append(out, obs)
		for _, key := range keys {
			seen[key] = struct{}{}
		}
	}
	return out
}

func observationDedupeKeys(obs observation) []string {
	keys := []string{}
	if id := strings.ToLower(strings.TrimSpace(obs.ID)); id != "" {
		keys = append(keys, "id:"+id)
	}
	if name := strings.ToLower(strings.TrimSpace(obs.LocationName)); name != "" {
		keys = append(keys, "name:"+name)
	}
	return keys
}

func sourceFromObservations(observations []observation) string {
	for _, obs := range observations {
		if text := strings.TrimSpace(obs.Source); text != "" {
			return text
		}
	}
	return ""
}

func airQualityRiskKey(aqhi string, risk string) string {
	if numeric, ok := numberFromAny(strings.TrimSpace(aqhi)); ok {
		switch {
		case numeric <= 0:
			return ""
		case numeric <= 3:
			return "low"
		case numeric <= 6:
			return "moderate"
		case numeric <= 10:
			return "high"
		default:
			return "very_high"
		}
	}
	normalized := strings.ToLower(strings.TrimSpace(risk))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, " ", "_")
	switch normalized {
	case "low", "moderate", "high", "very_high":
		return normalized
	default:
		return ""
	}
}

func airQualityRiskLabel(lang string, aqhi string, risk string) string {
	key := airQualityRiskKey(aqhi, risk)
	labels := map[string]map[string]string{
		"en": {
			"low":       "Low",
			"moderate":  "Moderate",
			"high":      "High",
			"very_high": "Very High",
		},
		"fr": {
			"low":       "Faible",
			"moderate":  "Modere",
			"high":      "Eleve",
			"very_high": "Tres eleve",
		},
		"es": {
			"low":       "Bajo",
			"moderate":  "Moderado",
			"high":      "Alto",
			"very_high": "Muy alto",
		},
	}
	short := strings.ToLower(strings.TrimSpace(lang))
	if idx := strings.Index(short, "-"); idx > 0 {
		short = short[:idx]
	}
	if localized := labels[short][key]; localized != "" {
		return localized
	}
	if localized := labels["en"][key]; localized != "" {
		return localized
	}
	return strings.TrimSpace(risk)
}

func spokenWeatherSource(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "eccc", "msc", "envcan", "environment canada":
		return "Environment and Climate Change Canada"
	case "noaa":
		return "the National Oceanic and Atmospheric Administration"
	case "nws":
		return "the National Weather Service"
	case "twc", "weather.com", "weatherdotcom":
		return "The Weather Channel"
	default:
		return strings.TrimSpace(source)
	}
}

func feedDescription(feed feedXML, lang string) string {
	lang = strings.ToLower(strings.TrimSpace(lang))
	short := lang
	if idx := strings.Index(short, "-"); idx > 0 {
		short = short[:idx]
	}
	for _, entry := range feed.Description.Langs {
		code := strings.ToLower(strings.TrimSpace(entry.Code))
		if code == lang || code == short || (code == "en-ca" && short == "en") {
			return strings.TrimSpace(strings.Join([]string{entry.Text, entry.Suffix}, " "))
		}
	}
	for _, entry := range feed.Description.Langs {
		if text := strings.TrimSpace(strings.Join([]string{entry.Text, entry.Suffix}, " ")); text != "" {
			return text
		}
	}
	return ""
}

func reportTime(raw string, timezone string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parsed, err := parseLooseTime(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimSpace(raw)
	}
	if loc, locErr := time.LoadLocation(fallbackText(timezone, "Local")); locErr == nil {
		parsed = parsed.In(loc)
	}
	return strings.TrimSpace(compactClock(parsed) + " " + timezoneName(parsed.Format("MST")))
}

func issueTimeTemplateValues(raw string, timezone string, values map[string]string) map[string]string {
	if values == nil {
		values = map[string]string{}
	}
	reported := reportTime(raw, timezone)
	if reported != "" {
		values["time"] = reported
	}
	parsed, err := parseLooseTime(strings.TrimSpace(raw))
	if err != nil {
		return values
	}
	if loc, locErr := time.LoadLocation(fallbackText(timezone, "Local")); locErr == nil {
		parsed = parsed.In(loc)
	}
	hour := parsed.Hour()
	ampm := "AM"
	if hour >= 12 {
		ampm = "PM"
	}
	hour12 := hour % 12
	if hour12 == 0 {
		hour12 = 12
	}
	values["issue_hour"] = strconv.Itoa(hour12)
	values["issue_minute"] = fmt.Sprintf("%02d", parsed.Minute())
	values["issue_ampm"] = ampm
	values["issue_timezone"] = timezoneName(parsed.Format("MST"))
	return values
}

func timezoneName(abbrev string) string {
	switch abbrev {
	case "CST":
		return "Central Standard Time"
	case "CDT":
		return "Central Daylight Time"
	case "MST":
		return "Mountain Standard Time"
	case "MDT":
		return "Mountain Daylight Time"
	case "EST":
		return "Eastern Standard Time"
	case "EDT":
		return "Eastern Daylight Time"
	case "PST":
		return "Pacific Standard Time"
	case "PDT":
		return "Pacific Daylight Time"
	default:
		return abbrev
	}
}

func dateTimeAnnouncement(now time.Time, lang string) string {
	short := strings.ToLower(strings.TrimSpace(lang))
	if idx := strings.Index(short, "-"); idx > 0 {
		short = short[:idx]
	}
	prefix := timeGreeting(now, short)
	timeText := spokenClockTime(now)
	switch short {
	case "fr":
		return strings.TrimSpace(prefix + " Il est actuellement " + timeText + ".")
	case "es":
		return strings.TrimSpace(prefix + " La hora actual es " + timeText + ".")
	default:
		return strings.TrimSpace(prefix + " The current time is " + timeText + ".")
	}
}

func timeGreeting(now time.Time, lang string) string {
	hour := now.Hour()
	period := "night"
	switch {
	case hour >= 5 && hour < 12:
		period = "morning"
	case hour >= 12 && hour < 17:
		period = "afternoon"
	case hour >= 17 && hour < 22:
		period = "evening"
	}
	switch lang {
	case "fr":
		switch period {
		case "morning":
			return "Bonjour."
		case "afternoon":
			return "Bon après-midi."
		case "evening":
			return "Bonsoir."
		default:
			return "Bonne nuit."
		}
	case "es":
		switch period {
		case "morning":
			return "Buenos días."
		case "afternoon":
			return "Buenas tardes."
		default:
			return "Buenas noches."
		}
	default:
		switch period {
		case "morning":
			return "Good morning."
		case "afternoon":
			return "Good afternoon."
		case "evening":
			return "Good evening."
		default:
			return "Good night."
		}
	}
}

func spokenClockTime(now time.Time) string {
	hour := numberToWords(now.Hour()%12, true)
	if now.Hour()%12 == 0 {
		hour = "twelve"
	}
	minute := now.Minute()
	ampm := "A.M."
	if now.Hour() >= 12 {
		ampm = "P.M."
	}
	tz := timezoneName(now.Format("MST"))
	if minute == 0 {
		return strings.TrimSpace(fmt.Sprintf("%s %s, %s", hour, ampm, tz))
	}
	minuteText := numberToWords(minute, false)
	if minute < 10 {
		minuteText = "oh " + minuteText
	}
	return strings.TrimSpace(fmt.Sprintf("%s %s %s, %s", hour, minuteText, ampm, tz))
}

func numberToWords(value int, hour bool) string {
	if hour && value == 0 {
		value = 12
	}
	ones := []string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"}
	if value >= 0 && value < len(ones) {
		return ones[value]
	}
	tens := []string{"", "", "twenty", "thirty", "forty", "fifty"}
	if value >= 20 && value < 60 {
		ten := value / 10
		one := value % 10
		if one == 0 {
			return tens[ten]
		}
		return strings.TrimSpace(tens[ten] + " " + ones[one])
	}
	return fmt.Sprintf("%d", value)
}

func spokenCallsign(callsign string) string {
	callsign = strings.TrimSpace(callsign)
	if callsign == "" {
		return ""
	}
	parts := make([]string, 0, len(callsign))
	for _, ch := range callsign {
		if ch == '-' || ch == ' ' || ch == '_' {
			continue
		}
		parts = append(parts, strings.ToUpper(string(ch)))
	}
	return strings.Join(parts, " ")
}

func normalizeRegionTitle(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.TrimSuffix(value, " Area")
	value = strings.TrimSuffix(value, " area")
	if value == "" {
		return ""
	}
	if strings.Contains(value, " - ") {
		return pauseForecastRegionName(value, "en")
	}
	if strings.Contains(strings.ToLower(value), "region") {
		return value
	}
	return value
}

func forecastRegionIntroFallback(region string) string {
	lower := strings.ToLower(strings.TrimSpace(region))
	if strings.Contains(lower, "region") || strings.Contains(lower, "area") || strings.Contains(region, ",") {
		return "For the {region}."
	}
	return "For {region}."
}

func forecastRegionTextKey(region string) string {
	if forecastRegionIntroFallback(region) == "For {region}." {
		return "region_plain"
	}
	return "region"
}

func forecastRegionTextKeys(region string) []string {
	key := forecastRegionTextKey(region)
	return []string{"shortterm." + key, key}
}

func pauseForecastRegionName(value string, language string) string {
	cleaned := strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	if cleaned == "" {
		return ""
	}
	parts := strings.Split(cleaned, " - ")
	if len(parts) <= 1 {
		return cleaned
	}
	cleanParts := make([]string, 0, len(parts))
	for index, part := range parts {
		part = strings.TrimSpace(strings.Trim(part, " ,."))
		part = strings.TrimSpace(strings.TrimSuffix(part, " region"))
		part = strings.TrimSpace(strings.TrimSuffix(part, " Region"))
		part = strings.TrimSpace(strings.TrimPrefix(part, "and "))
		if index == len(parts)-1 && strings.Contains(part, " and ") {
			for _, child := range strings.Split(part, " and ") {
				child = strings.TrimSpace(strings.Trim(child, " ,."))
				if child != "" {
					cleanParts = append(cleanParts, child)
				}
			}
			continue
		}
		if part != "" {
			cleanParts = append(cleanParts, part)
		}
	}
	if len(cleanParts) <= 1 {
		return cleaned
	}
	if language == "fr" {
		return strings.Join(cleanParts, ". ")
	}
	if len(cleanParts) == 2 {
		return cleanParts[0] + " and " + cleanParts[1] + " region"
	}
	return strings.Join(cleanParts[:len(cleanParts)-1], ", ") + ", and " + cleanParts[len(cleanParts)-1] + " region"
}

func forecastRegionBaseCode(region string) string {
	region = strings.TrimSpace(region)
	if before, _, ok := strings.Cut(region, "-"); ok {
		return strings.TrimSpace(before)
	}
	return region
}

func readableDirection(raw string) string {
	normalized := strings.ToUpper(strings.TrimSpace(raw))
	replacements := map[string]string{
		"N":   "north",
		"NE":  "north east",
		"E":   "east",
		"SE":  "south east",
		"S":   "south",
		"SW":  "south west",
		"W":   "west",
		"NW":  "north west",
		"NNE": "north north east",
		"ENE": "east north east",
		"ESE": "east south east",
		"SSE": "south south east",
		"SSW": "south south west",
		"WSW": "west south west",
		"WNW": "west north west",
		"NNW": "north north west",
	}
	if text, ok := replacements[normalized]; ok {
		return text
	}
	return strings.ToLower(strings.ReplaceAll(raw, "_", " "))
}

func degrees(value float64) string {
	return rounded(value) + " degrees"
}

func degreesBare(value float64) string {
	return rounded(value)
}

func rounded(value float64) string {
	if math.Abs(value-math.Round(value)) < 0.05 {
		return strconv.Itoa(int(math.Round(value)))
	}
	return oneDecimal(value)
}

func oneDecimal(value float64) string {
	return strconv.FormatFloat(math.Round(value*10)/10, 'f', 1, 64)
}

func periodForecastText(period forecastPeriod) string {
	name := strings.TrimSpace(period.Name)
	text := strings.TrimSpace(period.Text)
	if name == "" {
		return sentence(text)
	}
	if text == "" {
		return sentence(name)
	}
	return sentence(strings.TrimSpace(name + ". " + text))
}

func forecastPeriodTextKey(name string) string {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(name)), " "))
	normalized = strings.Trim(normalized, " .,:;-")
	switch normalized {
	case "today", "this morning", "this afternoon", "this evening":
		return "today"
	case "tonight":
		return "tonight"
	case "monday":
		return "mon"
	case "tuesday":
		return "tue"
	case "wednesday":
		return "wed"
	case "thursday":
		return "thu"
	case "friday":
		return "fri"
	case "saturday":
		return "sat"
	case "sunday":
		return "sun"
	default:
		if strings.HasPrefix(normalized, "monday ") {
			return "mon"
		}
		if strings.HasPrefix(normalized, "tuesday ") {
			return "tue"
		}
		if strings.HasPrefix(normalized, "wednesday ") {
			return "wed"
		}
		if strings.HasPrefix(normalized, "thursday ") {
			return "thu"
		}
		if strings.HasPrefix(normalized, "friday ") {
			return "fri"
		}
		if strings.HasPrefix(normalized, "saturday ") {
			return "sat"
		}
		if strings.HasPrefix(normalized, "sunday ") {
			return "sun"
		}
		return ""
	}
}

func forecastPeriodDayName(name string) string {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(name)), " "))
	normalized = strings.Trim(normalized, " .,:;-")
	normalized = strings.TrimSuffix(normalized, " night")
	normalized = strings.TrimSuffix(normalized, " overnight")
	normalized = strings.TrimSpace(strings.Trim(normalized, " .,:;-"))
	words := strings.Fields(normalized)
	if len(words) == 0 {
		return ""
	}
	switch words[0] {
	case "mon", "monday":
		return "monday"
	case "tue", "tues", "tuesday":
		return "tuesday"
	case "wed", "weds", "wednesday":
		return "wednesday"
	case "thu", "thur", "thurs", "thursday":
		return "thursday"
	case "fri", "friday":
		return "friday"
	case "sat", "saturday":
		return "saturday"
	case "sun", "sunday":
		return "sunday"
	default:
		return ""
	}
}

func forecastPeriodTextKeys(key string) []string {
	switch key {
	case "today", "tonight":
		return []string{"shortterm." + key, key}
	case "":
		return nil
	default:
		return []string{"extended.period", key}
	}
}

func forecastPeriodTemplateValues(name string) (period string, day string, night string) {
	period = strings.Join(strings.Fields(strings.TrimSpace(name)), " ")
	if period == "" {
		return "", "", ""
	}
	words := strings.Fields(period)
	if len(words) == 0 {
		return period, "", ""
	}
	last := strings.ToLower(strings.Trim(words[len(words)-1], " .,:;-"))
	if last == "night" || last == "overnight" {
		day = strings.Join(words[:len(words)-1], " ")
		night = words[len(words)-1]
		return period, day, night
	}
	day = period
	return period, day, ""
}

func sentence(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	last := text[len(text)-1]
	if last == '.' || last == '!' || last == '?' {
		return text
	}
	return text + "."
}

func flattenSegments(segments []Segment) string {
	parts := make([]string, 0, len(segments))
	for _, segment := range segments {
		if text := strings.TrimSpace(segment.Text); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n\n")
}

func titleForPackage(pkgID string) string {
	switch strings.ToLower(strings.TrimSpace(pkgID)) {
	case "date_time":
		return "Date and Time"
	case "station_id":
		return "Station Identification"
	case "current_conditions":
		return "Current Conditions"
	case "forecast":
		return "Forecast"
	case "air_quality":
		return "Air Quality"
	case "climate_summary":
		return "Climate Summary"
	case "marine_forecast":
		return "Marine Forecast"
	case "marine_reports":
		return "Marine Reports"
	case "aviation_reports":
		return "Aviation Weather Reports"
	case "thunderstorm_outlook":
		return "Thunderstorm Outlook"
	case "hydrometric":
		return "River Conditions"
	case "geophysical_alert":
		return "Geophysical Alert"
	case "alerts":
		return "Alerts"
	case "user_bulletin":
		return "User Bulletin"
	default:
		words := strings.Fields(strings.ReplaceAll(pkgID, "_", " "))
		for i := range words {
			words[i] = strings.ToUpper(words[i][:1]) + strings.ToLower(words[i][1:])
		}
		return strings.Join(words, " ")
	}
}

func safeID(value string) string {
	var builder strings.Builder
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.' {
			builder.WriteRune(ch)
		}
	}
	if builder.Len() == 0 {
		return "item"
	}
	return builder.String()
}
