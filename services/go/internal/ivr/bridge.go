package ivr

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type bridgeClient struct {
	conn            net.Conn
	events          chan map[string]any
	pendingProducts map[string]chan productResult
	pendingWx       map[string]chan wxResult
	pendingSynth    map[string]chan synthResult
	pendingASR      map[string]chan asrResult
	mu              sync.Mutex
}

type productResult struct {
	Product renderedProduct
	Err     error
}

type synthResult struct {
	Path       string
	Format     string
	SampleRate int
	Channels   int
	Err        error
}

type wxResult struct {
	Product renderedProduct
	Err     error
}

type asrResult struct {
	Text      string
	Code      string
	Retryable bool
	LatencyMS int
	Err       error
}

type renderedProduct struct {
	ID        string `json:"id"`
	FeedID    string `json:"feed_id"`
	PackageID string `json:"package_id"`
	Title     string `json:"title"`
	Text      string `json:"text"`
	ReaderID  string `json:"reader_id"`
	Language  string `json:"language"`
}

type synthRequest struct {
	ID              string
	Text            string
	ReaderID        string
	Provider        string
	VoiceID         string
	Language        string
	Timezone        string
	Rate            int
	Volume          int
	SentenceSilence float64
	OutputPath      string
	OutputFormat    string
	Priority        string
}

func connectBridge(ctx context.Context, addr string) (*bridgeClient, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("missing host event bridge address")
	}
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	client := &bridgeClient{
		conn:            conn,
		events:          make(chan map[string]any, 256),
		pendingProducts: map[string]chan productResult{},
		pendingWx:       map[string]chan wxResult{},
		pendingSynth:    map[string]chan synthResult{},
		pendingASR:      map[string]chan asrResult{},
	}
	go client.readLoop()
	return client, nil
}

func (c *bridgeClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *bridgeClient) Events() <-chan map[string]any {
	return c.events
}

func (c *bridgeClient) Publish(message map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := message["timestamp"]; !ok {
		message["timestamp"] = time.Now().UTC()
	}
	return json.NewEncoder(c.conn).Encode(message)
}

func (c *bridgeClient) WxOnDemand(ctx context.Context, requestID string, location ResolvedLocation, packages []string, language string, readerID string) (renderedProduct, error) {
	ch := make(chan wxResult, 1)
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
		"source":  serviceID,
		"subject": requestID,
		"data": map[string]any{
			"request_id":         requestID,
			"canonical_id":       location.CanonicalID,
			"feed_id":            location.FeedID,
			"covered_by_feed":    location.Covered,
			"code":               location.Code,
			"source":             location.Source,
			"location_identity":  location.Source + ":" + location.Code,
			"location_name":      location.Name,
			"province":           location.Province,
			"country":            location.Country,
			"forecast_id":        location.Forecast,
			"station_id":         location.StationID,
			"air_quality_id":     location.AirQualityID,
			"climate_id":         location.ClimateID,
			"hydrometric_id":     location.HydrometricID,
			"marine_forecast_id": location.MarineForecastID,
			"latitude":           location.Latitude,
			"longitude":          location.Longitude,
			"timezone":           location.Timezone,
			"language":           language,
			"reader_id":          readerID,
			"packages":           packages,
			"audience":           "telephone",
			"telephone":          true,
		},
	}); err != nil {
		return renderedProduct{}, err
	}
	select {
	case <-ctx.Done():
		return renderedProduct{}, ctx.Err()
	case result := <-ch:
		return result.Product, result.Err
	}
}

func (c *bridgeClient) Transcribe(ctx context.Context, requestID string, audioFile string, language string, prompt string) (asrResult, error) {
	ch := make(chan asrResult, 1)
	c.mu.Lock()
	c.pendingASR[requestID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pendingASR, requestID)
		c.mu.Unlock()
	}()
	if err := c.Publish(map[string]any{
		"type":    "asr.transcribe",
		"source":  serviceID,
		"subject": requestID,
		"data": map[string]any{
			"request_id":  requestID,
			"audio_file":  audioFile,
			"language":    language,
			"prompt":      prompt,
			"sample_rate": 16000,
			"channels":    1,
			"audience":    "telephone",
		},
	}); err != nil {
		return asrResult{}, err
	}
	select {
	case <-ctx.Done():
		return asrResult{}, ctx.Err()
	case result := <-ch:
		return result, result.Err
	}
}

func (c *bridgeClient) RenderProduct(ctx context.Context, requestID string, feedID string, packageID string) (renderedProduct, error) {
	ch := make(chan productResult, 1)
	c.mu.Lock()
	c.pendingProducts[requestID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pendingProducts, requestID)
		c.mu.Unlock()
	}()
	if err := c.Publish(map[string]any{
		"type":    "product.render.request",
		"source":  serviceID,
		"subject": requestID,
		"data": map[string]any{
			"request_id": requestID,
			"feed_id":    feedID,
			"package_id": packageID,
			"pkg_id":     packageID,
		},
	}); err != nil {
		return renderedProduct{}, err
	}
	select {
	case <-ctx.Done():
		return renderedProduct{}, ctx.Err()
	case result := <-ch:
		return result.Product, result.Err
	}
}

func (c *bridgeClient) Synthesize(ctx context.Context, job synthRequest) (synthResult, error) {
	ch := make(chan synthResult, 1)
	c.mu.Lock()
	c.pendingSynth[job.ID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pendingSynth, job.ID)
		c.mu.Unlock()
	}()
	if err := os.MkdirAll(filepath.Dir(job.OutputPath), 0o755); err != nil {
		return synthResult{}, err
	}
	if err := c.Publish(map[string]any{
		"type":    "tts.synthesize",
		"source":  serviceID,
		"subject": job.ID,
		"data": map[string]any{
			"job_id":           job.ID,
			"text":             job.Text,
			"reader_id":        job.ReaderID,
			"provider":         job.Provider,
			"voice_id":         job.VoiceID,
			"language":         job.Language,
			"timezone":         job.Timezone,
			"rate":             job.Rate,
			"volume":           job.Volume,
			"sentence_silence": job.SentenceSilence,
			"output_path":      job.OutputPath,
			"output_format":    job.OutputFormat,
			"priority":         job.Priority,
		},
	}); err != nil {
		return synthResult{}, err
	}
	select {
	case <-ctx.Done():
		return synthResult{}, ctx.Err()
	case result := <-ch:
		return result, result.Err
	}
}

func (c *bridgeClient) readLoop() {
	defer close(c.events)
	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var message map[string]any
		if err := json.Unmarshal([]byte(line), &message); err != nil {
			continue
		}
		if c.handleProductResult(message) || c.handleWxResult(message) || c.handleSynthResult(message) || c.handleASRResult(message) {
			continue
		}
		select {
		case c.events <- message:
		default:
		}
	}
	c.failPending(fmt.Errorf("host event bridge closed"))
}

func (c *bridgeClient) failPending(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.pendingProducts {
		ch <- productResult{Err: fmt.Errorf("%w while waiting for %s", err, id)}
		delete(c.pendingProducts, id)
	}
	for id, ch := range c.pendingWx {
		ch <- wxResult{Err: fmt.Errorf("%w while waiting for %s", err, id)}
		delete(c.pendingWx, id)
	}
	for id, ch := range c.pendingSynth {
		ch <- synthResult{Err: fmt.Errorf("%w while waiting for %s", err, id)}
		delete(c.pendingSynth, id)
	}
	for id, ch := range c.pendingASR {
		ch <- asrResult{Err: fmt.Errorf("%w while waiting for %s", err, id)}
		delete(c.pendingASR, id)
	}
}

func (c *bridgeClient) handleASRResult(message map[string]any) bool {
	msgType := stringAt(message, "type")
	if msgType != "asr.transcribed" && msgType != "asr.failed" {
		return false
	}
	data := mapAt(message, "data")
	requestID := firstText(message, data, "request_id", "subject")
	if requestID == "" {
		return true
	}
	c.mu.Lock()
	ch := c.pendingASR[requestID]
	c.mu.Unlock()
	if ch == nil {
		return true
	}
	if msgType == "asr.failed" {
		code := stringAt(data, "code")
		ch <- asrResult{Code: code, Retryable: boolAt(data, "retryable", false), Err: fmt.Errorf("ASR failed: %s", code)}
		return true
	}
	ch <- asrResult{Text: stringAt(data, "text"), LatencyMS: intAt(data, "latency_ms")}
	return true
}

func (c *bridgeClient) handleProductResult(message map[string]any) bool {
	msgType := stringAt(message, "type")
	if msgType != "product.rendered" && msgType != "product.render.failed" {
		return false
	}
	data := mapAt(message, "data")
	requestID := firstText(message, data, "request_id", "subject")
	if requestID == "" {
		return true
	}
	c.mu.Lock()
	ch := c.pendingProducts[requestID]
	c.mu.Unlock()
	if ch == nil {
		return true
	}
	if msgType == "product.render.failed" {
		ch <- productResult{Err: fmt.Errorf("product render failed: %s", stringAt(data, "error"))}
		return true
	}
	raw, err := json.Marshal(data["product"])
	if err != nil {
		ch <- productResult{Err: err}
		return true
	}
	var product renderedProduct
	if err := json.Unmarshal(raw, &product); err != nil {
		ch <- productResult{Err: err}
		return true
	}
	ch <- productResult{Product: product}
	return true
}

func (c *bridgeClient) handleWxResult(message map[string]any) bool {
	msgType := stringAt(message, "type")
	if msgType != "wx.on_demand.rendered" && msgType != "wx.on_demand.failed" {
		return false
	}
	data := mapAt(message, "data")
	requestID := firstText(message, data, "request_id", "subject")
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
		ch <- wxResult{Err: fmt.Errorf("wx on-demand failed: %s", stringAt(data, "error"))}
		return true
	}
	raw, err := json.Marshal(data["product"])
	if err != nil {
		ch <- wxResult{Err: err}
		return true
	}
	var product renderedProduct
	if err := json.Unmarshal(raw, &product); err != nil {
		ch <- wxResult{Err: err}
		return true
	}
	ch <- wxResult{Product: product}
	return true
}

func (c *bridgeClient) handleSynthResult(message map[string]any) bool {
	msgType := stringAt(message, "type")
	if msgType != "tts.synthesized" && msgType != "tts.failed" {
		return false
	}
	data := mapAt(message, "data")
	jobID := firstText(message, data, "job_id", "subject")
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
		ch <- synthResult{Err: fmt.Errorf("TTS failed: %s", stringAt(data, "error"))}
		return true
	}
	ch <- synthResult{
		Path:       stringAt(data, "output_path"),
		Format:     stringAt(data, "format"),
		SampleRate: intAt(data, "sample_rate"),
		Channels:   intAt(data, "channels"),
	}
	return true
}

func stringAt(source map[string]any, key string) string {
	if source == nil {
		return ""
	}
	value, _ := source[key].(string)
	return strings.TrimSpace(value)
}

func intAt(source map[string]any, key string) int {
	if source == nil {
		return 0
	}
	switch value := source[key].(type) {
	case float64:
		return int(value)
	case int:
		return value
	case json.Number:
		parsed, _ := value.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func boolAt(source map[string]any, key string, fallback bool) bool {
	if source == nil {
		return fallback
	}
	switch value := source[key].(type) {
	case bool:
		return value
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return fallback
}

func mapAt(source map[string]any, key string) map[string]any {
	if source == nil {
		return nil
	}
	value, _ := source[key].(map[string]any)
	return value
}

func firstText(message map[string]any, data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringAt(message, key); value != "" {
			return value
		}
		if value := stringAt(data, key); value != "" {
			return value
		}
	}
	return ""
}
