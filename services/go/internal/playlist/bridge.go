package playlist

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	bridgeEventBuffer         = 8192
	bridgeCriticalEventBuffer = 256
)

type bridgeClient struct {
	conn            net.Conn
	events          chan map[string]any
	criticalEvents  chan map[string]any
	pendingProducts map[string]chan productResult
	pendingSynth    map[string]chan synthResult
	mu              sync.Mutex
	stop            chan struct{}
	stopOnce        sync.Once
}

type productResult struct {
	Product renderedProduct
	Err     error
}

type synthResult struct {
	Path string
	Err  error
}

type renderedProduct struct {
	ID        string            `json:"id"`
	FeedID    string            `json:"feed_id"`
	PackageID string            `json:"package_id"`
	Title     string            `json:"title"`
	Text      string            `json:"text"`
	ReaderID  string            `json:"reader_id"`
	Language  string            `json:"language"`
	Segments  []renderedSegment `json:"segments"`
	Metadata  map[string]string `json:"metadata"`
}

type renderedSegment struct {
	Kind      string `json:"kind"`
	Label     string `json:"label"`
	Text      string `json:"text"`
	AudioPath string `json:"audio_path"`
}

type synthJob struct {
	ID               string
	VoiceRouteKey    string
	Text             string
	ReaderID         string
	Language         string
	Timezone         string
	OutputPath       string
	TargetSampleRate int
	TargetChannels   int
	Priority         string
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
		events:          make(chan map[string]any, bridgeEventBuffer),
		criticalEvents:  make(chan map[string]any, bridgeCriticalEventBuffer),
		pendingProducts: map[string]chan productResult{},
		pendingSynth:    map[string]chan synthResult{},
		stop:            make(chan struct{}),
	}
	go client.readLoop()
	return client, nil
}

func (c *bridgeClient) Close() error {
	if c == nil {
		return nil
	}
	c.stopOnce.Do(func() {
		if c.stop != nil {
			close(c.stop)
		}
	})
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *bridgeClient) Events() <-chan map[string]any {
	return c.events
}

func (c *bridgeClient) CriticalEvents() <-chan map[string]any {
	return c.criticalEvents
}

func (c *bridgeClient) Publish(message map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := message["timestamp"]; !ok {
		message["timestamp"] = time.Now().UTC()
	}
	return json.NewEncoder(c.conn).Encode(message)
}

func (c *bridgeClient) RenderProduct(ctx context.Context, requestID string, feedID string, packageID string, language string) (renderedProduct, error) {
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
			"language":   language,
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

func (c *bridgeClient) Synthesize(ctx context.Context, job synthJob) (string, error) {
	if strings.TrimSpace(job.ID) == "" {
		job.ID = fmt.Sprintf("tts-%d", time.Now().UnixNano())
	}
	ch := make(chan synthResult, 1)
	c.mu.Lock()
	c.pendingSynth[job.ID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pendingSynth, job.ID)
		c.mu.Unlock()
	}()
	if job.OutputPath != "" {
		if err := os.MkdirAll(filepath.Dir(job.OutputPath), 0o755); err != nil {
			return "", err
		}
	}
	priority := normalizeSynthesisPriority(job.Priority)
	voiceRouteKey := strings.TrimSpace(job.VoiceRouteKey)
	if voiceRouteKey == "" {
		voiceRouteKey = job.ID
	}
	data := map[string]any{
		"job_id":             job.ID,
		"voice_route_key":    voiceRouteKey,
		"text":               job.Text,
		"reader_id":          job.ReaderID,
		"language":           job.Language,
		"timezone":           job.Timezone,
		"output_path":        job.OutputPath,
		"target_sample_rate": job.TargetSampleRate,
		"target_channels":    job.TargetChannels,
		"priority":           priority,
	}
	if deadline, ok := ctx.Deadline(); ok {
		data["deadline_at"] = deadline.UTC().Format(time.RFC3339Nano)
	}
	if err := c.Publish(map[string]any{
		"type":    "tts.synthesize",
		"source":  serviceID,
		"subject": job.ID,
		"data":    data,
	}); err != nil {
		return "", err
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case result := <-ch:
		return result.Path, result.Err
	}
}

func normalizeSynthesisPriority(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "high", "realtime", "urgent", "radio", "playout":
		return "high"
	case "low", "batch", "background":
		return "low"
	default:
		return "normal"
	}
}

func (c *bridgeClient) readLoop() {
	defer func() {
		if c.events != nil {
			close(c.events)
		}
		if c.criticalEvents != nil {
			close(c.criticalEvents)
		}
	}()
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
		if c.handleProductResult(message) || c.handleSynthResult(message) {
			continue
		}
		c.enqueueEvent(message)
	}
	c.mu.Lock()
	for id, ch := range c.pendingProducts {
		ch <- productResult{Err: fmt.Errorf("event bridge closed while waiting for %s", id)}
		delete(c.pendingProducts, id)
	}
	for id, ch := range c.pendingSynth {
		ch <- synthResult{Err: fmt.Errorf("event bridge closed while waiting for %s", id)}
		delete(c.pendingSynth, id)
	}
	c.mu.Unlock()
}

func (c *bridgeClient) enqueueEvent(message map[string]any) {
	if playlistCriticalEvent(stringAt(message, "type")) {
		c.enqueueCriticalEvent(message)
		return
	}
	select {
	case c.events <- message:
		return
	default:
	}
	msgType := stringAt(message, "type")
	if !playlistLifecycleEvent(msgType) {
		log.Printf("playlist bridge event buffer full; dropped %s", msgType)
		return
	}
	select {
	case <-c.events:
	default:
	}
	select {
	case c.events <- message:
	default:
		log.Printf("playlist bridge event buffer full; dropped %s", msgType)
	}
}

func (c *bridgeClient) enqueueCriticalEvent(message map[string]any) {
	if c.criticalEvents != nil {
		select {
		case c.criticalEvents <- message:
		case <-c.stop:
			log.Printf("playlist bridge critical event dropped during shutdown: %s", stringAt(message, "type"))
		}
		return
	}
	if c.events == nil {
		log.Printf("playlist bridge critical event dropped because no event buffer exists: %s", stringAt(message, "type"))
		return
	}
	select {
	case c.events <- message:
	case <-c.stop:
		log.Printf("playlist bridge critical event dropped during shutdown: %s", stringAt(message, "type"))
	}
}

func playlistCriticalEvent(msgType string) bool {
	switch strings.TrimSpace(msgType) {
	case "cap.alert.broadcast.requested", "cap.alert.cancelled":
		return true
	default:
		return false
	}
}

func playlistLifecycleEvent(msgType string) bool {
	switch strings.TrimSpace(msgType) {
	case "playout.accepted", "playout.started", "playout.interrupted", "playout.completed",
		"alert.playout.started", "alert.playout.completed", "service.ready", "system.shutdown":
		return true
	default:
		return false
	}
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
	ch <- synthResult{Path: stringAt(data, "output_path")}
	return true
}

func stringAt(source map[string]any, key string) string {
	if source == nil {
		return ""
	}
	switch value := source[key].(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func mapAt(source map[string]any, key string) map[string]any {
	if source == nil {
		return nil
	}
	if value, ok := source[key].(map[string]any); ok {
		return value
	}
	return nil
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

func firstValue(message map[string]any, data map[string]any, keys ...string) any {
	for _, key := range keys {
		if message != nil {
			if value, ok := message[key]; ok && value != nil {
				return value
			}
		}
		if data != nil {
			if value, ok := data[key]; ok && value != nil {
				return value
			}
		}
	}
	return nil
}

func boolAny(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "y", "on", "broadcast immediate":
			return true
		}
	}
	return false
}

func stringListAny(value any) []string {
	switch typed := value.(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text := strings.TrimSpace(fmt.Sprint(item))
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text := strings.TrimSpace(item)
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	case string:
		if text := strings.TrimSpace(typed); text != "" {
			return []string{text}
		}
	}
	return nil
}
