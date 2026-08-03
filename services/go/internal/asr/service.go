package asr

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const serviceID = "haze-asr"

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{7,127}$`)

// Request is the broker contract for one private-spool transcription.
type Request struct {
	RequestID  string
	AudioFile  string
	Language   string
	Prompt     string
	SampleRate int
	Channels   int
	Audience   string
}

type job struct {
	request Request
	path    string
}

type Service struct {
	cfg         Config
	transcriber Transcriber
	publisher   eventPublisher
	available   bool
	jobs        chan job

	accepted       atomic.Uint64
	completed      atomic.Uint64
	failed         atomic.Uint64
	rejected       atomic.Uint64
	timedOut       atomic.Uint64
	unavailable    atomic.Uint64
	latencySamples atomic.Uint64
	latencyMS      atomic.Uint64
}

func Run(ctx context.Context, options Options) error {
	if strings.TrimSpace(options.ConfigPath) == "" {
		options.ConfigPath = "config.yaml"
	}
	cfg, err := loadConfig(options.ConfigPath)
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		log.Printf("ASR service disabled")
		<-ctx.Done()
		return nil
	}
	if err := ensurePrivateSpool(cfg.SpoolDir); err != nil {
		return err
	}
	cleanupStaleSpool(cfg.SpoolDir, 5*time.Minute)
	localTranscriber, runtimeErr := newLocalWhisperTranscriber(ctx, cfg)
	available := runtimeErr == nil
	var transcriber Transcriber = localTranscriber
	if runtimeErr != nil {
		log.Printf("ASR starting in degraded keypad-only mode: local Whisper is unavailable: %v", runtimeErr)
	} else {
		defer localTranscriber.Close()
		log.Printf("ASR local Whisper ready: provider=%s model=%s", cfg.Provider, cfg.Model)
	}
	for {
		bridge, err := connectBridge(ctx, options.BridgeAddr)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("ASR waiting for event bridge: %v", err)
			if !sleepOrDone(ctx, time.Second) {
				return nil
			}
			continue
		}
		service := NewService(cfg, transcriber, bridge, available)
		err = service.Serve(ctx, bridge.Events())
		_ = bridge.Close()
		if errors.Is(err, errSystemShutdown) || ctx.Err() != nil {
			return nil
		}
		log.Printf("ASR event bridge disconnected")
		if !sleepOrDone(ctx, time.Second) {
			return nil
		}
	}
}

var errSystemShutdown = errors.New("system shutdown requested")

func NewService(cfg Config, transcriber Transcriber, publisher eventPublisher, available bool) *Service {
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.MaxAudioBytes <= 0 {
		cfg.MaxAudioBytes = defaultMaxAudioBytes
	}
	return &Service{
		cfg:         cfg,
		transcriber: transcriber,
		publisher:   publisher,
		available:   available && transcriber != nil,
		jobs:        make(chan job, cfg.QueueSize),
	}
}

func (s *Service) Serve(ctx context.Context, events <-chan map[string]any) error {
	workerCtx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	for index := 0; index < s.cfg.Workers; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			s.worker(workerCtx)
		}()
	}
	defer func() {
		cancel()
		workers.Wait()
		s.cleanupQueued()
	}()
	_ = s.publisher.Publish(map[string]any{
		"type":   "service.ready",
		"source": serviceID,
		"data": map[string]any{
			"service":    serviceID,
			"provider":   s.cfg.Provider,
			"model":      s.cfg.Model,
			"workers":    s.cfg.Workers,
			"queue_size": s.cfg.QueueSize,
			"degraded":   !s.available,
		},
	})
	metricsTicker := time.NewTicker(30 * time.Second)
	defer metricsTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-metricsTicker.C:
			s.publishMetrics()
		case event, ok := <-events:
			if !ok {
				return io.EOF
			}
			switch stringAt(event, "type") {
			case "system.shutdown":
				return errSystemShutdown
			case "asr.transcribe":
				s.submit(requestFromEvent(event))
			}
		}
	}
}

func (s *Service) submit(request Request) {
	path, err := secureAudioPath(s.cfg.SpoolDir, request)
	if err != nil {
		s.publishFailed(request.RequestID, "invalid_request", false)
		return
	}
	if !s.available {
		removeSpoolFile(path)
		s.publishFailed(request.RequestID, "provider_unavailable", false)
		return
	}
	select {
	case s.jobs <- job{request: request, path: path}:
		s.accepted.Add(1)
	default:
		removeSpoolFile(path)
		s.publishFailed(request.RequestID, "busy", true)
	}
}

func (s *Service) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case next := <-s.jobs:
			s.process(ctx, next)
		}
	}
}

func (s *Service) process(parent context.Context, next job) {
	cleaned := false
	cleanup := func() {
		if !cleaned {
			removeSpoolFile(next.path)
			cleaned = true
		}
	}
	defer cleanup()
	if err := validateWaveFile(next.path, next.request, s.cfg.MaxAudioBytes); err != nil {
		cleanup()
		s.publishFailed(next.request.RequestID, "invalid_request", false)
		return
	}
	ctx, cancel := context.WithTimeout(parent, s.cfg.Timeout)
	defer cancel()
	started := time.Now()
	text, err := s.transcriber.Transcribe(ctx, next.path, next.request.Language, next.request.Prompt)
	latency := time.Since(started)
	s.latencySamples.Add(1)
	s.latencyMS.Add(uint64(latency.Milliseconds()))
	if err != nil {
		code, retryable := classifyProviderError(err)
		cleanup()
		s.publishFailed(next.request.RequestID, code, retryable)
		return
	}
	text = strings.TrimSpace(text)
	if text == "" || len(text) > 4096 {
		cleanup()
		s.publishFailed(next.request.RequestID, "provider_error", false)
		return
	}
	s.completed.Add(1)
	cleanup()
	_ = s.publisher.Publish(map[string]any{
		"type":    "asr.transcribed",
		"source":  serviceID,
		"subject": next.request.RequestID,
		"data": map[string]any{
			"request_id": next.request.RequestID,
			"text":       text,
			"provider":   s.cfg.Provider,
			"model":      s.cfg.Model,
			"latency_ms": latency.Milliseconds(),
		},
	})
}

func (s *Service) publishFailed(requestID string, code string, retryable bool) {
	s.failed.Add(1)
	switch code {
	case "busy":
		s.rejected.Add(1)
	case "timeout":
		s.timedOut.Add(1)
	case "provider_unavailable":
		s.unavailable.Add(1)
	}
	_ = s.publisher.Publish(failedEvent(requestID, code, retryable))
}

func (s *Service) publishMetrics() {
	_ = s.publisher.Publish(map[string]any{
		"type":   "service.metrics",
		"source": serviceID,
		"data": map[string]any{
			"accepted":             s.accepted.Load(),
			"completed":            s.completed.Load(),
			"failed":               s.failed.Load(),
			"queue_rejected":       s.rejected.Load(),
			"timeouts":             s.timedOut.Load(),
			"provider_unavailable": s.unavailable.Load(),
			"latency_samples":      s.latencySamples.Load(),
			"latency_ms_total":     s.latencyMS.Load(),
		},
	})
}

func failedEvent(requestID string, code string, retryable bool) map[string]any {
	return map[string]any{
		"type":    "asr.failed",
		"source":  serviceID,
		"subject": requestID,
		"data": map[string]any{
			"request_id": requestID,
			"code":       code,
			"retryable":  retryable,
		},
	}
}

func (s *Service) cleanupQueued() {
	for {
		select {
		case pending := <-s.jobs:
			removeSpoolFile(pending.path)
		default:
			return
		}
	}
}

func requestFromEvent(event map[string]any) Request {
	data := mapAt(event, "data")
	return Request{
		RequestID:  firstText(event, data, "request_id", "subject"),
		AudioFile:  firstText(event, data, "audio_file"),
		Language:   firstText(event, data, "language"),
		Prompt:     firstText(event, data, "prompt"),
		SampleRate: firstInt(event, data, "sample_rate"),
		Channels:   firstInt(event, data, "channels"),
		Audience:   firstText(event, data, "audience"),
	}
}

func secureAudioPath(spoolDir string, request Request) (string, error) {
	if !requestIDPattern.MatchString(request.RequestID) {
		return "", fmt.Errorf("invalid request ID")
	}
	if request.SampleRate != 16000 || request.Channels != 1 {
		return "", fmt.Errorf("unsupported audio format")
	}
	if request.Audience != "" && !strings.EqualFold(request.Audience, "telephone") {
		return "", fmt.Errorf("unsupported audience")
	}
	if len(request.Prompt) > 256 {
		return "", fmt.Errorf("prompt is too long")
	}
	expected := request.RequestID + ".wav"
	if request.AudioFile != filepath.Base(request.AudioFile) || request.AudioFile != expected {
		return "", fmt.Errorf("audio filename does not match request")
	}
	root, err := filepath.Abs(filepath.Clean(spoolDir))
	if err != nil {
		return "", err
	}
	path, err := filepath.Abs(filepath.Join(root, request.AudioFile))
	if err != nil || filepath.Dir(path) != root {
		return "", fmt.Errorf("audio path escapes spool")
	}
	return path, nil
}

func validateWaveFile(path string, request Request, maxBytes int64) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < 44 || info.Size() > maxBytes {
		return fmt.Errorf("invalid audio file size or type")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if filepath.Clean(resolved) != filepath.Clean(path) {
		return fmt.Errorf("audio file must not be a link")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	header := make([]byte, 44)
	if _, err := io.ReadFull(file, header); err != nil {
		return err
	}
	if string(header[0:4]) != "RIFF" || string(header[8:12]) != "WAVE" || string(header[12:16]) != "fmt " || string(header[36:40]) != "data" {
		return fmt.Errorf("unsupported WAV layout")
	}
	if binary.LittleEndian.Uint16(header[20:22]) != 1 || binary.LittleEndian.Uint16(header[22:24]) != uint16(request.Channels) || binary.LittleEndian.Uint32(header[24:28]) != uint32(request.SampleRate) || binary.LittleEndian.Uint16(header[34:36]) != 16 {
		return fmt.Errorf("WAV must be mono 16 kHz PCM16")
	}
	dataBytes := int64(binary.LittleEndian.Uint32(header[40:44]))
	if dataBytes != info.Size()-44 {
		return fmt.Errorf("WAV data length mismatch")
	}
	minimum := int64(request.SampleRate * request.Channels * 2 * 2)
	maximum := int64(request.SampleRate * request.Channels * 2 * 8)
	if dataBytes < minimum || dataBytes > maximum {
		return fmt.Errorf("WAV duration must be between two and eight seconds")
	}
	return nil
}

func ensurePrivateSpool(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create ASR spool: %w", err)
	}
	if err := os.Chmod(path, 0o700); err != nil && !errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("protect ASR spool: %w", err)
	}
	return nil
}

func cleanupStaleSpool(dir string, age time.Duration) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-age)
	for _, entry := range entries {
		stem := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		if entry.IsDir() || strings.ToLower(filepath.Ext(entry.Name())) != ".wav" || !requestIDPattern.MatchString(stem) {
			continue
		}
		info, err := entry.Info()
		if err == nil && info.ModTime().Before(cutoff) {
			removeSpoolFile(filepath.Join(dir, entry.Name()))
		}
	}
}

func removeSpoolFile(path string) {
	if strings.TrimSpace(path) != "" {
		_ = os.Remove(path)
	}
}

func stringAt(source map[string]any, key string) string {
	if source == nil {
		return ""
	}
	value, _ := source[key].(string)
	return strings.TrimSpace(value)
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

func firstInt(message map[string]any, data map[string]any, key string) int {
	for _, source := range []map[string]any{message, data} {
		switch value := source[key].(type) {
		case int:
			return value
		case int64:
			return int(value)
		case uint64:
			return int(value)
		case float64:
			return int(value)
		case json.Number:
			parsed, _ := value.Int64()
			return int(parsed)
		}
	}
	return 0
}

func sleepOrDone(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
