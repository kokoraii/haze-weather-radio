package webgateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationclient"
	"github.com/meowraii/haze-weather-radio/services/go/internal/tts"
)

const wxGenerateTimeout = 90 * time.Second

var wxOnDemandExcludedPackages = map[string]bool{
	"station_id":    true,
	"user_bulletin": true,
}

type wxGeneratePayload struct {
	CanonicalID  string
	FeedID       string
	Code         string
	Source       string
	LocationName string
	Province     string
	Country      string
	ForecastID   string
	StationID    string
	Latitude     string
	Longitude    string
	Timezone     string
	Language     string
	ReaderID     string
	Format       string
	Packages     []string
	Force        bool
}

type wxRenderedProduct struct {
	ID          string              `json:"id"`
	FeedID      string              `json:"feed_id"`
	PackageID   string              `json:"package_id"`
	Title       string              `json:"title"`
	Text        string              `json:"text"`
	ReaderID    string              `json:"reader_id"`
	Language    string              `json:"language"`
	Segments    []wxRenderedSegment `json:"segments,omitempty"`
	Inputs      []any               `json:"inputs,omitempty"`
	GeneratedAt time.Time           `json:"generated_at"`
	Metadata    map[string]string   `json:"metadata,omitempty"`
}

type wxRenderedSegment struct {
	Kind  string `json:"kind"`
	Label string `json:"label,omitempty"`
	Text  string `json:"text"`
}

type wxBridgeResult struct {
	Product wxRenderedProduct
	Err     error
}

type wxSynthResult struct {
	Path       string
	Format     string
	SampleRate int
	Channels   int
	Provider   string
	VoiceID    string
	Err        error
}

type wxBridgeClient struct {
	conn         net.Conn
	pendingWx    map[string]chan wxBridgeResult
	pendingSynth map[string]chan wxSynthResult
	mu           sync.Mutex
}

func (s *wsSession) generateWx(payload map[string]any) (any, error) {
	return generateWxOnDemand(s.configPath, payload)
}

func (s *Server) wxOnDemandPackages(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, ok := s.requireAdminRequest(writer, request); !ok {
		return
	}
	packages, err := loadWxOnDemandPackageIDs(s.configPath)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(writer, map[string]any{"packages": packages})
}

func (s *Server) wxOnDemandReaders(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, ok := s.requireAdminRequest(writer, request); !ok {
		return
	}
	readers, err := loadReaderCatalog(s.configPath)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(writer, map[string]any{"readers": readers})
}

func (s *Server) wxOnDemandGenerate(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, ok := s.requireAdminRequest(writer, request); !ok {
		return
	}
	defer request.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(io.LimitReader(request.Body, 256*1024)).Decode(&payload); err != nil {
		http.Error(writer, "invalid JSON", http.StatusBadRequest)
		return
	}
	result, err := generateWxOnDemand(s.configPath, payload)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadGateway)
		return
	}
	writeWxHTTPResult(writer, result)
}

func writeWxHTTPResult(writer http.ResponseWriter, result any) {
	payload, _ := result.(map[string]any)
	if text, ok := payload["text"].(string); ok {
		contentType, _ := payload["content_type"].(string)
		if contentType == "" {
			contentType = "text/plain; charset=utf-8"
		}
		writer.Header().Set("Content-Type", contentType)
		writer.Header().Set("X-Format", strings.TrimSpace(fmt.Sprint(payload["format"])))
		writer.Header().Set("X-Packages", strings.TrimSpace(fmt.Sprint(payload["packages"])))
		_, _ = writer.Write([]byte(text))
		return
	}
	audio, _ := payload["audio_base64"].(string)
	bytes, err := base64.StdEncoding.DecodeString(audio)
	if err != nil {
		http.Error(writer, "invalid generated audio", http.StatusInternalServerError)
		return
	}
	format := strings.TrimSpace(fmt.Sprint(payload["format"]))
	contentType := strings.TrimSpace(fmt.Sprint(payload["content_type"]))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	writer.Header().Set("Content-Type", contentType)
	writer.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="haze-wx.%s"`, wxExtension(format)))
	writer.Header().Set("X-Format", format)
	writer.Header().Set("X-Audio-Sample-Rate", strings.TrimSpace(fmt.Sprint(payload["sample_rate"])))
	writer.Header().Set("X-Audio-Channels", strings.TrimSpace(fmt.Sprint(payload["channels"])))
	writer.Header().Set("X-Packages", strings.TrimSpace(fmt.Sprint(payload["packages"])))
	_, _ = writer.Write(bytes)
}

func generateWxOnDemand(configPath string, raw map[string]any) (map[string]any, error) {
	request, err := parseWxGeneratePayload(configPath, raw)
	if err != nil {
		return nil, err
	}
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return nil, fmt.Errorf("event bridge is not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), wxGenerateTimeout)
	defer cancel()
	bridge, err := connectWxBridge(ctx, bridgeAddr)
	if err != nil {
		return nil, fmt.Errorf("event bridge connect failed: %w", err)
	}
	defer bridge.Close()
	if err := applyWxCanonicalLocation(ctx, configPath, bridgeAddr, &request); err != nil {
		return nil, err
	}

	requestID := safeID(fmt.Sprintf("wx-web-%d", time.Now().UnixNano()))
	product, err := bridge.WxOnDemand(ctx, requestID, request)
	if err != nil {
		return nil, err
	}
	if wxTextFormat(request.Format) {
		return wxTextResult(product, request), nil
	}
	return wxAudioResult(ctx, configPath, bridge, product, request)
}

func parseWxGeneratePayload(configPath string, raw map[string]any) (wxGeneratePayload, error) {
	packages, err := parseWxPackages(configPath, raw["packages"])
	if err != nil {
		return wxGeneratePayload{}, err
	}
	format := strings.ToLower(strings.TrimSpace(firstNonBlank(stringAny(raw["format"]), "wav")))
	format = strings.ReplaceAll(format, "-", "_")
	if format == "pcm16" || format == "pcm_s16le" || format == "s16le" {
		format = "raw"
	}
	if !wxTextFormat(format) {
		if _, ok := wxAudioFormatByID(format); !ok {
			return wxGeneratePayload{}, fmt.Errorf("unsupported output format %q", format)
		}
	}
	feedID := strings.TrimSpace(firstNonBlank(stringAny(raw["feed_id"]), stringAny(raw["feed"])))
	code := firstLocation(raw["locations"])
	if code == "" {
		code = firstLocation(raw["location"])
	}
	request := wxGeneratePayload{
		FeedID:   feedID,
		Code:     code,
		Source:   strings.TrimSpace(firstNonBlank(stringAny(raw["source"]), stringAny(raw["weather_source"]))),
		Language: strings.TrimSpace(firstNonBlank(stringAny(raw["language"]), stringAny(raw["lang"]), "en-US")),
		ReaderID: strings.TrimSpace(firstNonBlank(stringAny(raw["reader_id"]), stringAny(raw["voice"]))),
		Format:   format,
		Packages: packages,
		Force:    boolAny(raw["force"]),
	}
	request.applyLocationHints(configPath)
	return request, nil
}

func (request *wxGeneratePayload) applyLocationHints(configPath string) {
	code := strings.TrimSpace(request.Code)
	if code == "" {
		return
	}
	if left, right, ok := strings.Cut(code, ","); ok {
		request.Latitude = strings.TrimSpace(left)
		request.Longitude = strings.TrimSpace(right)
		request.Source = firstNonBlank(request.Source, "twc")
		request.LocationName = firstNonBlank(request.LocationName, code)
		return
	}
	canonical := wxCanonicalLocationCode(code)
	if canonical != "" {
		request.Code = canonical
		code = canonical
	}
	if request.applyConfiguredLocation(configPath, code) {
		return
	}
	if forecastID, province, ok := wxDeriveHelloWeatherForecast(code); ok {
		request.ForecastID = forecastID
		request.StationID = firstNonBlank(request.StationID, forecastID)
		request.Province = province
		request.Source = firstNonBlank(request.Source, "hello_weather")
		request.LocationName = firstNonBlank(request.LocationName, code)
		return
	}
	if wxLooksLikeProviderID(code) {
		request.ForecastID = code
		request.StationID = firstNonBlank(request.StationID, code)
		request.Source = firstNonBlank(request.Source, wxProviderSource(code))
		request.LocationName = firstNonBlank(request.LocationName, code)
		return
	}
	if wxLooksLikeStationID(code) {
		request.StationID = code
		request.Source = firstNonBlank(request.Source, wxProviderSource(code))
		request.LocationName = firstNonBlank(request.LocationName, code)
	}
}

func (request *wxGeneratePayload) applyConfiguredLocation(configPath string, code string) bool {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return false
	}
	feeds, err := loadFeedsXML(configPath, root)
	if err != nil {
		return false
	}
	forecastNames := loadForecastRegionNames(resolveConfigPath(configPath, "managed/csv/FORECAST_LOCATIONS.csv"))
	clcNames := loadCLCNames(resolveConfigPath(configPath, "managed/csv/CLC_Base_Zone.csv"))
	for _, feed := range feeds.Feeds {
		if request.FeedID != "" && !strings.EqualFold(feed.ID, request.FeedID) {
			continue
		}
		if request.Timezone == "" {
			request.Timezone = strings.TrimSpace(feed.Timezone)
		}
		for _, loc := range feed.Locations.ObservationLocations.Locations {
			if !wxSameCode(loc.ID, code) {
				continue
			}
			request.StationID = loc.ID
			request.ForecastID = firstNonBlank(request.ForecastID, loc.ID)
			request.Source = firstNonBlank(request.Source, loc.Source, wxProviderSource(loc.ID))
			request.LocationName = firstNonBlank(request.LocationName, loc.NameOverride, loc.ID)
			return true
		}
		for _, region := range feed.Locations.Coverage.Regions {
			if wxSameCode(region.ID, code) || wxSameCode(region.DeriveForecast, code) {
				request.ForecastID = firstNonBlank(region.DeriveForecast, region.ID)
				request.StationID = firstNonBlank(request.StationID, request.ForecastID)
				request.Source = firstNonBlank(request.Source, region.Source, wxProviderSource(request.ForecastID))
				request.LocationName = firstNonBlank(request.LocationName, forecastNames[region.ID], clcNames[region.ID], region.Name, region.ID)
				return true
			}
			for _, subregion := range region.Subregions {
				if !wxSameCode(subregion.ID, code) {
					continue
				}
				request.ForecastID = firstNonBlank(region.DeriveForecast, region.ID)
				request.StationID = firstNonBlank(request.StationID, request.ForecastID)
				request.Source = firstNonBlank(request.Source, region.Source, wxProviderSource(request.ForecastID))
				request.LocationName = firstNonBlank(request.LocationName, clcNames[subregion.ID], forecastNames[region.ID], region.Name, subregion.ID)
				return true
			}
		}
	}
	return false
}

func parseWxPackages(configPath string, value any) ([]string, error) {
	available, err := loadWxOnDemandPackageIDs(configPath)
	if err != nil {
		return nil, err
	}
	if len(available) == 0 {
		return nil, fmt.Errorf("no enabled packages are configured")
	}
	if strings.EqualFold(strings.TrimSpace(stringAny(value)), "all") {
		return available, nil
	}
	requested := stringSliceAny(value)
	if len(requested) == 0 {
		defaults := []string{"date_time", "current_conditions", "forecast"}
		for _, pkg := range defaults {
			if wxContainsString(available, pkg) {
				requested = append(requested, pkg)
			}
		}
		if len(requested) == 0 {
			requested = available[:1]
		}
	}
	allowed := map[string]bool{}
	for _, pkg := range available {
		allowed[pkg] = true
	}
	out := make([]string, 0, len(requested))
	for _, pkg := range requested {
		pkg = strings.TrimSpace(pkg)
		if pkg == "" {
			continue
		}
		if !allowed[pkg] {
			return nil, fmt.Errorf("package %q is not enabled", pkg)
		}
		out = append(out, pkg)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one package is required")
	}
	return uniqueStrings(out), nil
}

func loadWxOnDemandPackageIDs(configPath string) ([]string, error) {
	packageIDs, err := loadPackageIDs(configPath)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(packageIDs))
	for _, id := range packageIDs {
		if wxOnDemandExcludedPackages[strings.TrimSpace(id)] {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

func firstEnabledFeedID(configPath string) string {
	feeds, err := loadFeedSummaries(configPath)
	if err != nil {
		return ""
	}
	for _, feed := range feeds {
		if enabled, _ := feed["enabled"].(bool); enabled {
			return strings.TrimSpace(fmt.Sprint(feed["id"]))
		}
	}
	if len(feeds) > 0 {
		return strings.TrimSpace(fmt.Sprint(feeds[0]["id"]))
	}
	return ""
}

func loadReaderCatalog(configPath string) ([]map[string]any, error) {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return nil, err
	}
	path := textAt(root, []string{"services", "go", "tts", "readers"}, "managed/configs/readers.xml", 240)
	readers, err := tts.LoadReaders(resolveConfigPath(configPath, path))
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(readers))
	for _, reader := range readers {
		backupProvider := ""
		backupVoiceID := ""
		backupReaderID := ""
		if reader.Backup != nil {
			backupProvider = reader.Backup.Provider
			backupVoiceID = reader.Backup.VoiceID
			backupReaderID = reader.Backup.ReaderID
		}
		labelParts := []string{reader.ID}
		for _, part := range []string{reader.Provider, reader.Gender, reader.Language, reader.VoiceID} {
			if strings.TrimSpace(part) != "" {
				labelParts = append(labelParts, part)
			}
		}
		out = append(out, map[string]any{
			"id":                reader.ID,
			"provider":          reader.Provider,
			"gender":            reader.Gender,
			"language":          reader.Language,
			"voice_id":          reader.VoiceID,
			"backup_reader_id":  backupReaderID,
			"backup_provider":   backupProvider,
			"backup_voice_id":   backupVoiceID,
			"backup_reader_ids": append([]string(nil), reader.BackupFor...),
			"label":             strings.Join(labelParts, " · "),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["id"]) < fmt.Sprint(out[j]["id"])
	})
	return out, nil
}

func connectWxBridge(ctx context.Context, addr string) (*wxBridgeClient, error) {
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	client := &wxBridgeClient{
		conn:         conn,
		pendingWx:    map[string]chan wxBridgeResult{},
		pendingSynth: map[string]chan wxSynthResult{},
	}
	go client.readLoop()
	return client, nil
}

func (c *wxBridgeClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *wxBridgeClient) Publish(message map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := message["timestamp"]; !ok {
		message["timestamp"] = time.Now().UTC()
	}
	return json.NewEncoder(c.conn).Encode(message)
}

func (c *wxBridgeClient) WxOnDemand(ctx context.Context, requestID string, request wxGeneratePayload) (wxRenderedProduct, error) {
	ch := make(chan wxBridgeResult, 1)
	c.mu.Lock()
	c.pendingWx[requestID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pendingWx, requestID)
		c.mu.Unlock()
	}()
	if err := c.Publish(map[string]any{
		"type":    "wx.on_demand.request",
		"source":  "haze-web",
		"subject": requestID,
		"data": map[string]any{
			"request_id":    requestID,
			"canonical_id":  request.CanonicalID,
			"feed_id":       request.FeedID,
			"code":          request.Code,
			"source":        request.Source,
			"location_name": request.LocationName,
			"province":      request.Province,
			"country":       request.Country,
			"forecast_id":   request.ForecastID,
			"station_id":    request.StationID,
			"latitude":      request.Latitude,
			"longitude":     request.Longitude,
			"timezone":      request.Timezone,
			"language":      request.Language,
			"reader_id":     request.ReaderID,
			"packages":      request.Packages,
			"force":         request.Force,
			"telephone":     false,
		},
	}); err != nil {
		return wxRenderedProduct{}, err
	}
	select {
	case <-ctx.Done():
		return wxRenderedProduct{}, ctx.Err()
	case result := <-ch:
		return result.Product, result.Err
	}
}

func applyWxCanonicalLocation(ctx context.Context, configPath string, bridgeAddr string, request *wxGeneratePayload) error {
	mode := locationclient.RolloutMode()
	if root, err := loadYAMLMap(configPath); err == nil {
		mode = locationclient.ParseMode(textAt(root, []string{"services", "rust", "location", "mode"}, "legacy", 32))
	}
	if mode == locationclient.ModeLegacy || request == nil || strings.TrimSpace(request.Code) == "" {
		return nil
	}
	started := time.Now()
	client := locationclient.New(bridgeAddr, "haze-web-location")
	client.Timeout = 4 * time.Second
	response, err := client.Query(ctx, locationclient.Request{
		Operation: "resolve",
		Input:     &locationclient.Input{Kind: "auto", Text: request.Code},
		Filters: locationclient.Filters{
			Region: request.Province,
		},
		Options: locationclient.Options{
			Limit:                  4,
			MinimumConfidence:      "medium",
			StationModeRequirement: "any",
		},
	})
	if mode == locationclient.ModeShadow {
		canonicalID := ""
		ambiguous := false
		if err == nil {
			ambiguous = response.Ambiguous
			if len(response.Results) > 0 {
				canonicalID = response.Results[0].Entity.ID
			}
		}
		log.Printf(
			"location shadow web resolve input=%q legacy=%s:%s canonical_id=%s ambiguous=%t latency_ms=%d error=%v",
			request.Code,
			request.Source,
			firstNonBlank(request.ForecastID, request.StationID, request.Code),
			canonicalID,
			ambiguous,
			time.Since(started).Milliseconds(),
			err,
		)
		return nil
	}
	if err != nil {
		return fmt.Errorf("location resolution failed: %w", err)
	}
	if response.Ambiguous {
		return fmt.Errorf("location %q is ambiguous and requires a qualified identifier", request.Code)
	}
	if len(response.Results) == 0 {
		return fmt.Errorf("location %q was not found", request.Code)
	}
	entity := response.Results[0].Entity
	request.CanonicalID = entity.ID
	request.LocationName = firstNonBlank(entity.DisplayName(), request.LocationName)
	request.Province = firstNonBlank(entity.Region, request.Province)
	request.Country = entity.Country
	request.Source = firstNonBlank(wxCanonicalProvider(entity), request.Source)
	request.ForecastID = firstNonBlank(
		entity.Identifier("eccc_citypage", "nws_zone", "nws_marine_zone"),
		request.ForecastID,
	)
	request.StationID = firstNonBlank(
		entity.Identifier("eccc_station", "icao", "msc", "wmo", "ndbc"),
		request.StationID,
	)
	if entity.Geometry != nil {
		if entity.Geometry.Latitude != nil {
			request.Latitude = fmt.Sprintf("%.7f", *entity.Geometry.Latitude)
		}
		if entity.Geometry.Longitude != nil {
			request.Longitude = fmt.Sprintf("%.7f", *entity.Geometry.Longitude)
		}
	}
	return nil
}

func wxCanonicalProvider(entity locationclient.Entity) string {
	for _, identifier := range entity.Identifiers {
		switch strings.ToLower(identifier.Scheme) {
		case "nws_zone", "nws_marine_zone", "nws_ugc_county", "fips", "same":
			return "nws"
		case "eccc_citypage", "clc", "eccc_station", "msc", "wmo", "icao":
			return "eccc"
		case "ndbc":
			return "ndbc"
		}
	}
	return ""
}

func (c *wxBridgeClient) Synthesize(ctx context.Context, jobID string, product wxRenderedProduct, outputPath string, outputFormat string) (wxSynthResult, error) {
	ch := make(chan wxSynthResult, 1)
	c.mu.Lock()
	c.pendingSynth[jobID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pendingSynth, jobID)
		c.mu.Unlock()
	}()
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return wxSynthResult{}, err
	}
	if err := c.Publish(map[string]any{
		"type":    "tts.synthesize",
		"source":  "haze-web",
		"subject": jobID,
		"data": map[string]any{
			"job_id":        jobID,
			"text":          product.Text,
			"reader_id":     product.ReaderID,
			"language":      product.Language,
			"output_path":   outputPath,
			"output_format": outputFormat,
		},
	}); err != nil {
		return wxSynthResult{}, err
	}
	select {
	case <-ctx.Done():
		return wxSynthResult{}, ctx.Err()
	case result := <-ch:
		return result, result.Err
	}
}

func (c *wxBridgeClient) readLoop() {
	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var message map[string]any
		if err := json.Unmarshal([]byte(line), &message); err != nil {
			continue
		}
		if c.handleWxResult(message) || c.handleSynthResult(message) {
			continue
		}
	}
	c.failPending(fmt.Errorf("host event bridge closed"))
}

func (c *wxBridgeClient) failPending(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.pendingWx {
		ch <- wxBridgeResult{Err: fmt.Errorf("%w while waiting for %s", err, id)}
		delete(c.pendingWx, id)
	}
	for id, ch := range c.pendingSynth {
		ch <- wxSynthResult{Err: fmt.Errorf("%w while waiting for %s", err, id)}
		delete(c.pendingSynth, id)
	}
}

func (c *wxBridgeClient) handleWxResult(message map[string]any) bool {
	msgType := wxStringAt(message, "type")
	if msgType != "wx.on_demand.rendered" && msgType != "wx.on_demand.failed" {
		return false
	}
	data := wxMapAt(message, "data")
	requestID := firstNonBlank(wxStringAt(message, "subject"), wxStringAt(data, "request_id"))
	if requestID == "" {
		return true
	}
	c.mu.Lock()
	ch := c.pendingWx[requestID]
	c.mu.Unlock()
	if ch == nil {
		return true
	}
	if msgType == "wx.on_demand.failed" {
		ch <- wxBridgeResult{Err: fmt.Errorf("wx on-demand failed: %s", wxStringAt(data, "error"))}
		return true
	}
	raw, err := json.Marshal(data["product"])
	if err != nil {
		ch <- wxBridgeResult{Err: err}
		return true
	}
	var product wxRenderedProduct
	if err := json.Unmarshal(raw, &product); err != nil {
		ch <- wxBridgeResult{Err: err}
		return true
	}
	ch <- wxBridgeResult{Product: product}
	return true
}

func (c *wxBridgeClient) handleSynthResult(message map[string]any) bool {
	msgType := wxStringAt(message, "type")
	if msgType != "tts.synthesized" && msgType != "tts.failed" {
		return false
	}
	data := wxMapAt(message, "data")
	jobID := firstNonBlank(wxStringAt(message, "subject"), wxStringAt(data, "job_id"))
	if jobID == "" {
		return true
	}
	c.mu.Lock()
	ch := c.pendingSynth[jobID]
	c.mu.Unlock()
	if ch == nil {
		return true
	}
	if msgType == "tts.failed" {
		ch <- wxSynthResult{Err: fmt.Errorf("TTS failed: %s", wxStringAt(data, "error"))}
		return true
	}
	ch <- wxSynthResult{
		Path:       wxStringAt(data, "output_path"),
		Format:     wxStringAt(data, "format"),
		SampleRate: wxIntAt(data, "sample_rate"),
		Channels:   wxIntAt(data, "channels"),
		Provider:   wxStringAt(data, "provider"),
		VoiceID:    wxStringAt(data, "voice_id"),
	}
	return true
}

func wxAudioResult(ctx context.Context, configPath string, bridge *wxBridgeClient, product wxRenderedProduct, request wxGeneratePayload) (map[string]any, error) {
	audioFormat, _ := wxAudioFormatByID(request.Format)
	outputFormat := "wav"
	extension := "wav"
	if request.Format == "raw" {
		outputFormat = "pcm_s16le"
		extension = "s16le"
	}
	jobID := safeID(fmt.Sprintf("wx-tts-%d", time.Now().UnixNano()))
	outputPath := resolveConfigPath(configPath, filepath.Join("runtime", "audio", "wx-on-demand", jobID+"."+extension))
	synth, err := bridge.Synthesize(ctx, jobID, product, outputPath, outputFormat)
	if err != nil {
		return nil, err
	}
	path := firstNonBlank(synth.Path, outputPath)
	resultFormat := firstNonBlank(synth.Format, request.Format)
	bytes, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	sampleRate := synth.SampleRate
	if sampleRate == 0 {
		sampleRate = 48000
	}
	channels := synth.Channels
	if channels == 0 {
		channels = 1
	}
	contentType := "audio/wav"
	if request.Format == "raw" {
		contentType = "audio/L16; rate=48000; channels=1"
		resultFormat = "raw"
	}
	if request.Format != "wav" && request.Format != "raw" {
		bytes, err = transcodeWxAudio(ctx, bytes, audioFormat)
		if err != nil {
			return nil, err
		}
		resultFormat = audioFormat.ID
		contentType = audioFormat.ContentType
	}
	return map[string]any{
		"audio_base64": base64.StdEncoding.EncodeToString(bytes),
		"format":       resultFormat,
		"content_type": contentType,
		"sample_rate":  sampleRate,
		"channels":     channels,
		"packages":     strings.Join(request.Packages, ","),
		"feed_id":      product.FeedID,
		"title":        product.Title,
		"reader_id":    product.ReaderID,
		"language":     product.Language,
	}, nil
}

func transcodeWxAudio(ctx context.Context, wavBytes []byte, format httpAudioFormat) ([]byte, error) {
	if format.FFmpegFormat == "" {
		return wavBytes, nil
	}
	ffmpeg, err := resolveFFmpegExecutable()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg is required for %s output: %w", format.ID, err)
	}
	if format.Channels <= 0 {
		format.Channels = 1
	}
	if format.SampleRate <= 0 {
		format.SampleRate = 48000
	}
	args := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-nostdin",
		"-i", "pipe:0",
		"-vn",
		"-sn",
		"-dn",
		"-ar", fmt.Sprintf("%d", format.SampleRate),
		"-ac", fmt.Sprintf("%d", format.Channels),
		"-c:a", format.FFmpegCodec,
	}
	if format.Bitrate != "" {
		args = append(args, "-b:a", format.Bitrate)
	}
	args = append(args, format.ExtraOutputArg...)
	args = append(args, "-f", format.FFmpegFormat, "pipe:1")
	cmd := exec.CommandContext(ctx, ffmpeg, args...)
	cmd.Stdin = bytes.NewReader(wavBytes)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg %s conversion failed: %w: %s", format.ID, err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func wxTextResult(product wxRenderedProduct, request wxGeneratePayload) map[string]any {
	contentType := wxTextContentType(request.Format)
	text := serializeWxProduct(product, request.Format)
	return map[string]any{
		"text":         text,
		"format":       request.Format,
		"content_type": contentType,
		"packages":     strings.Join(request.Packages, ","),
		"feed_id":      product.FeedID,
		"title":        product.Title,
		"reader_id":    product.ReaderID,
		"language":     product.Language,
	}
}

func serializeWxProduct(product wxRenderedProduct, format string) string {
	switch format {
	case "json":
		raw, _ := json.MarshalIndent(product, "", "  ")
		return string(raw)
	case "xml":
		type xmlSegment struct {
			XMLName xml.Name `xml:"segment"`
			Kind    string   `xml:"kind,attr,omitempty"`
			Label   string   `xml:"label,attr,omitempty"`
			Text    string   `xml:",chardata"`
		}
		type xmlProduct struct {
			XMLName  xml.Name     `xml:"product"`
			ID       string       `xml:"id,attr,omitempty"`
			FeedID   string       `xml:"feed_id,attr,omitempty"`
			Title    string       `xml:"title"`
			Language string       `xml:"language"`
			ReaderID string       `xml:"reader_id,omitempty"`
			Text     string       `xml:"text"`
			Segments []xmlSegment `xml:"segments>segment,omitempty"`
		}
		segments := make([]xmlSegment, 0, len(product.Segments))
		for _, segment := range product.Segments {
			segments = append(segments, xmlSegment{Kind: segment.Kind, Label: segment.Label, Text: segment.Text})
		}
		raw, _ := xml.MarshalIndent(xmlProduct{
			ID:       product.ID,
			FeedID:   product.FeedID,
			Title:    product.Title,
			Language: product.Language,
			ReaderID: product.ReaderID,
			Text:     product.Text,
			Segments: segments,
		}, "", "  ")
		return xml.Header + string(raw)
	case "html":
		return "<!doctype html>\n<html><head><meta charset=\"utf-8\"><title>" + html.EscapeString(product.Title) + "</title></head><body><h1>" + html.EscapeString(product.Title) + "</h1><p>" + strings.ReplaceAll(html.EscapeString(product.Text), "\n", "<br>") + "</p></body></html>\n"
	case "ssml":
		return "<speak>" + html.EscapeString(product.Text) + "</speak>\n"
	case "latex":
		return "\\section*{" + latexEscape(product.Title) + "}\n\n" + latexEscape(product.Text) + "\n"
	default:
		title := strings.TrimSpace(product.Title)
		if title == "" {
			return strings.TrimSpace(product.Text) + "\n"
		}
		return "# " + title + "\n\n" + strings.TrimSpace(product.Text) + "\n"
	}
}

func wxTextContentType(format string) string {
	switch format {
	case "json":
		return "application/json; charset=utf-8"
	case "xml":
		return "application/xml; charset=utf-8"
	case "html":
		return "text/html; charset=utf-8"
	case "ssml":
		return "application/ssml+xml; charset=utf-8"
	case "latex":
		return "application/x-latex; charset=utf-8"
	default:
		return "text/markdown; charset=utf-8"
	}
}

func wxTextFormat(format string) bool {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json", "xml", "ssml", "html", "markdown", "latex":
		return true
	default:
		return false
	}
}

func wxAudioFormatByID(format string) (httpAudioFormat, bool) {
	if format == "wav" {
		return httpAudioFormat{ID: "wav", ContentType: "audio/wav", Extension: "wav"}, true
	}
	if format == "raw" {
		return httpAudioFormat{ID: "raw", ContentType: "audio/L16; rate=48000; channels=1", Extension: "s16le"}, true
	}
	if format == "ogg" {
		format = "vorbis"
	}
	return httpAudioFormatByID(format)
}

func wxExtension(format string) string {
	if audioFormat, ok := wxAudioFormatByID(format); ok && audioFormat.Extension != "" {
		return audioFormat.Extension
	}
	switch format {
	case "json", "xml", "html", "ssml", "latex":
		return format
	case "markdown":
		return "md"
	default:
		return "txt"
	}
}

func latexEscape(value string) string {
	replacer := strings.NewReplacer(
		"\\", "\\textbackslash{}",
		"&", "\\&",
		"%", "\\%",
		"$", "\\$",
		"#", "\\#",
		"_", "\\_",
		"{", "\\{",
		"}", "\\}",
	)
	return replacer.Replace(value)
}

func firstLocation(value any) string {
	switch typed := value.(type) {
	case string:
		for _, part := range strings.FieldsFunc(typed, func(ch rune) bool { return ch == ',' || ch == '\n' || ch == '\r' || ch == '\t' }) {
			if text := strings.TrimSpace(part); text != "" {
				return text
			}
		}
	case []any:
		for _, item := range typed {
			if text := strings.TrimSpace(fmt.Sprint(item)); text != "" {
				return text
			}
		}
	case []string:
		for _, item := range typed {
			if text := strings.TrimSpace(item); text != "" {
				return text
			}
		}
	default:
		text := strings.TrimSpace(fmt.Sprint(value))
		if text != "" && text != "<nil>" {
			return text
		}
	}
	return ""
}

func wxCanonicalLocationCode(code string) string {
	code = strings.TrimSpace(code)
	if len(code) == 3 && code[0] >= '1' && code[0] <= '9' {
		return "0" + code[:1] + "0" + code[1:]
	}
	return code
}

func wxDeriveHelloWeatherForecast(code string) (string, string, bool) {
	code = strings.TrimSpace(code)
	if len(code) != 5 || code[0] != '0' || code[2] != '0' {
		return "", "", false
	}
	prefix, province, ok := wxHelloWeatherProvince(code[1:2])
	if !ok {
		return "", "", false
	}
	city := strings.TrimLeft(code[3:], "0")
	if city == "" {
		return "", "", false
	}
	return prefix + "-" + city, province, true
}

func wxHelloWeatherProvince(digit string) (string, string, bool) {
	switch strings.TrimSpace(digit) {
	case "1":
		return "ns", "NS", true
	case "2":
		return "nl", "NL", true
	case "3":
		return "qc", "QC", true
	case "4":
		return "on", "ON", true
	case "5":
		return "mb", "MB", true
	case "6":
		return "sk", "SK", true
	case "7":
		return "ab", "AB", true
	case "8":
		return "bc", "BC", true
	default:
		return "", "", false
	}
}

func wxLooksLikeProviderID(code string) bool {
	code = strings.TrimSpace(code)
	if code == "" {
		return false
	}
	parts := strings.Split(code, "-")
	return len(parts) == 2 && parts[0] != "" && parts[1] != "" && wxHasLetter(parts[0]) && wxHasDigit(parts[1])
}

func wxLooksLikeStationID(code string) bool {
	code = strings.ToUpper(strings.TrimSpace(code))
	if len(code) == 4 && wxHasLetter(code) {
		return true
	}
	return len(code) >= 3 && len(code) <= 6 && wxHasLetter(code) && wxHasDigit(code)
}

func wxProviderSource(code string) string {
	code = strings.ToUpper(strings.TrimSpace(code))
	if strings.HasPrefix(code, "K") || strings.Contains(code, "Z") || strings.Contains(code, "C") && wxHasDigit(code) {
		if strings.HasPrefix(code, "K") {
			return "nws"
		}
	}
	if strings.Contains(strings.ToLower(code), "-") {
		return "eccc"
	}
	return ""
}

func wxHasLetter(value string) bool {
	for _, ch := range value {
		if ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' {
			return true
		}
	}
	return false
}

func wxHasDigit(value string) bool {
	for _, ch := range value {
		if ch >= '0' && ch <= '9' {
			return true
		}
	}
	return false
}

func wxSameCode(left string, right string) bool {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return left != "" && right != "" && strings.EqualFold(left, right)
}

func stringAny(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func stringSliceAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		return cleanStringSlice(typed)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			out = append(out, fmt.Sprint(item))
		}
		return cleanStringSlice(out)
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return cleanStringSlice(strings.Split(typed, ","))
	default:
		return nil
	}
}

func cleanStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func boolAny(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on", "enabled":
			return true
		}
	case float64:
		return typed != 0
	case int:
		return typed != 0
	}
	return false
}

func wxContainsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func wxStringAt(source map[string]any, key string) string {
	if source == nil {
		return ""
	}
	value, _ := source[key].(string)
	return strings.TrimSpace(value)
}

func wxMapAt(source map[string]any, key string) map[string]any {
	if source == nil {
		return nil
	}
	value, _ := source[key].(map[string]any)
	return value
}

func wxIntAt(source map[string]any, key string) int {
	if source == nil {
		return 0
	}
	switch value := source[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		parsed, _ := value.Int64()
		return int(parsed)
	default:
		return 0
	}
}
