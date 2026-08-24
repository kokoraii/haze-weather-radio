package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/events"
	"github.com/meowraii/haze-weather-radio/services/go/internal/processguard"
	"github.com/meowraii/haze-weather-radio/services/go/internal/tts"
)

const serviceID = "haze-tts"

const (
	synthesisHighQueueSize   = 64
	synthesisNormalQueueSize = 64
	synthesisLowQueueSize    = 16
	synthesisCacheMaxEntries = 256
	synthesisCacheMaxIdle    = 30 * time.Minute
	defaultSynthesisWorkers  = 4
	providerHealthTimeout    = 5 * time.Second
	providerHealthyTTL       = 30 * time.Second
	providerUnavailableTTL   = 15 * time.Second
	voiceRouteTTL            = 5 * time.Minute
	voiceRouteMaxEntries     = 1024
)

var errSystemShutdown = errors.New("system shutdown requested")

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Fatalf("haze-tts: %v", err)
	}
}

func run() error {
	readersPath := flag.String("readers", filepath.Join("managed", "configs", "readers.xml"), "readers.xml path")
	dictionaryPath := flag.String("dictionary", envOrDefault("HAZE_TTS_DICTIONARY", filepath.Join("managed", "dictionary.json")), "dictionary.json path")
	providerID := flag.String("provider", "auto", "provider to use: auto, piper, kokoro, sapi5, espeak, f5tts, chatterbox, or speakyapi")
	readerID := flag.String("reader-id", "", "reader id from readers.xml")
	lang := flag.String("lang", "en-US", "requested language")
	timezone := flag.String("timezone", envOrDefault("HAZE_TTS_TIMEZONE", "Local"), "timezone for spoken timestamps")
	_ = flag.String("piper-exe", "", "deprecated; Piper uses the native sherpa-onnx runtime")
	piperVoicesDir := flag.String("piper-voices-dir", envOrDefault("HAZE_PIPER_VOICES_DIR", filepath.Join("managed", "voices", "piper")), "Piper voice model directory")
	_ = flag.String("piper-mode", "", "deprecated; Piper is native-only")
	_ = flag.Int("piper-workers", 0, "deprecated; Piper is native-only")
	_ = flag.Bool("piper-cuda", false, "deprecated; use HAZE_PIPER_PROVIDER or HAZE_KOKORO_PROVIDER")
	kokoroModelDir := flag.String("kokoro-model-dir", envOrDefault("HAZE_KOKORO_MODEL_DIR", filepath.Join("managed", "voices", "kokoro-multi-lang-v1_0")), "Kokoro model directory")
	kokoroRuntimeProvider := flag.String("kokoro-runtime-provider", envOrDefault("HAZE_KOKORO_PROVIDER", "cpu"), "Kokoro sherpa-onnx provider: cpu, cuda, or coreml")
	kokoroThreads := flag.Int("kokoro-threads", envIntOrDefault("HAZE_KOKORO_THREADS", 0), "Kokoro neural network worker threads")
	kokoroSpeed := flag.Float64("kokoro-speed", envFloatOrDefault("HAZE_KOKORO_SPEED", 1.0), "Kokoro default generation speed")
	kokoroLengthScale := flag.Float64("kokoro-length-scale", envFloatOrDefault("HAZE_KOKORO_LENGTH_SCALE", 1.0), "Kokoro model length scale")
	speakyAPIURL := flag.String("speakyapi-url", envOrDefault("HAZE_SPEAKYAPI_URL", ""), "SpeakyAPI server base URL, for example http://127.0.0.1:5000")
	runtimeIdleTimeout := flag.Duration("runtime-idle-timeout", envDurationOrDefault("HAZE_TTS_RUNTIME_IDLE", 30*time.Second), "idle time before native TTS model runtimes are unloaded; 0 disables unloading")
	text := flag.String("text", "", "text to synthesize")
	out := flag.String("out", "", "output WAV path")
	listVoices := flag.Bool("list-voices", false, "list provider voices as JSON")
	service := flag.Bool("service", false, "run as a host-bridge TTS service")
	bridge := flag.String("bridge", os.Getenv("HAZE_HOST_BRIDGE_ADDR"), "host bridge address")
	outDir := flag.String("out-dir", filepath.Join("managed", "audio", "tts"), "default service output directory")
	cacheDir := flag.String("cache-dir", "", "persistent synthesis cache directory; empty disables the persistent cache")
	cacheMaxBytes := flag.Int64("cache-max-bytes", 0, "maximum persistent synthesis cache size in bytes; 0 is unlimited")
	cacheMaxEntries := flag.Int("cache-max-entries", synthesisCacheMaxEntries, "maximum persistent synthesis cache entries; 0 is unlimited")
	timeout := flag.Duration("timeout", 60*time.Second, "synthesis timeout")
	workers := flag.Int("workers", envIntOrDefault("HAZE_TTS_WORKERS", defaultSynthesisWorkers), "maximum concurrent synthesis jobs")
	flag.Parse()
	setTTSRuntimeEnv(ttsRuntimeEnv{
		PiperVoicesDir:        *piperVoicesDir,
		KokoroModelDir:        *kokoroModelDir,
		KokoroLang:            kokoroRuntimeLang(*lang),
		KokoroRuntimeProvider: *kokoroRuntimeProvider,
		KokoroThreads:         *kokoroThreads,
		KokoroSpeed:           *kokoroSpeed,
		KokoroLengthScale:     *kokoroLengthScale,
	})

	if *service {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		ctx = processguard.WithParent(ctx)
		return runService(ctx, serviceConfig{
			BridgeAddr:      *bridge,
			Readers:         *readersPath,
			Dictionary:      *dictionaryPath,
			Provider:        *providerID,
			Language:        *lang,
			Timezone:        *timezone,
			OutDir:          *outDir,
			CacheDir:        *cacheDir,
			CacheMaxBytes:   *cacheMaxBytes,
			CacheMaxEntries: *cacheMaxEntries,
			Timeout:         *timeout,
			Workers:         maxInt(1, *workers),
			SpeakyAPIURL:    *speakyAPIURL,
			RuntimeIdle:     *runtimeIdleTimeout,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	providers := tts.DefaultProviders()
	configureSpeakyAPIProvider(providers, *speakyAPIURL)
	if *listVoices {
		voices, err := listVoicesForProvider(ctx, providers, *providerID)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(voices)
	}

	if *text == "" {
		return errors.New("missing --text")
	}
	if *out == "" {
		return errors.New("missing --out")
	}

	reader, err := resolveReader(*readersPath, *readerID, *lang)
	if err != nil {
		return err
	}
	providerName := *providerID
	voiceID := ""
	if reader.ID != "" {
		providerName = reader.Provider
		voiceID = reader.VoiceID
		if reader.Language != "" {
			*lang = reader.Language
		}
	}
	dictionary, err := tts.LoadDictionary(*dictionaryPath, *lang)
	if err != nil {
		return err
	}
	audio, _, _, _, err := synthesizeWithReaderBackup(ctx, providers, providerName, tts.Request{
		Text:     tts.NormalizeText(*text, dictionary, *timezone),
		VoiceID:  voiceID,
		Language: *lang,
		Volume:   100,
	}, reader.Backup)
	if err != nil {
		return err
	}
	if audio.Format != tts.FormatWAV {
		return fmt.Errorf("provider returned unsupported format %q", audio.Format)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		return err
	}
	return writeFileAtomic(*out, audio.Data, 0o644)
}

type serviceConfig struct {
	BridgeAddr      string
	Readers         string
	Dictionary      string
	Provider        string
	Language        string
	Timezone        string
	OutDir          string
	CacheDir        string
	CacheMaxBytes   int64
	CacheMaxEntries int
	Timeout         time.Duration
	Workers         int
	SpeakyAPIURL    string
	RuntimeIdle     time.Duration
}

type serviceState struct {
	ctx            context.Context
	cfg            serviceConfig
	providers      map[string]tts.Provider
	readers        []tts.Reader
	readersErr     error
	dictionaries   map[string]dictionaryResult
	synthesisCache map[[sha256.Size]byte]synthesisCacheEntry
	providerHealth map[string]*providerHealthStatus
	voiceRoutes    map[[sha256.Size]byte]voiceRouteDecision
	mu             sync.Mutex
	routeMu        sync.Mutex
	publishMu      sync.Mutex
}

type providerHealthStatus struct {
	available bool
	expiresAt time.Time
	ready     chan struct{}
}

type voiceRouteDecision struct {
	providerID string
	voiceID    string
	backupUsed bool
	expiresAt  time.Time
}

type dictionaryResult struct {
	Dictionary tts.Dictionary
	Err        error
}

type synthesisCacheEntry struct {
	OutputPath string
	Format     tts.AudioFormat
	SampleRate int
	Channels   int
	Bytes      int64
	ProviderID string
	ReaderID   string
	VoiceID    string
	Language   string
	LastUsed   time.Time
}

type persistentSynthesisEntry struct {
	Format     tts.AudioFormat `json:"format"`
	SampleRate int             `json:"sample_rate"`
	Channels   int             `json:"channels"`
	Bytes      int64           `json:"bytes"`
	ProviderID string          `json:"provider_id"`
	ReaderID   string          `json:"reader_id"`
	VoiceID    string          `json:"voice_id"`
	Language   string          `json:"language"`
	LastUsed   time.Time       `json:"last_used"`
}

func runService(ctx context.Context, cfg serviceConfig) error {
	if strings.TrimSpace(cfg.BridgeAddr) == "" {
		return errors.New("missing host bridge address")
	}
	if strings.TrimSpace(cfg.Language) == "" {
		cfg.Language = "en-US"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 60 * time.Second
	}
	if strings.TrimSpace(cfg.Timezone) == "" {
		cfg.Timezone = "Local"
	}
	if cfg.CacheMaxBytes < 0 {
		return errors.New("cache max bytes cannot be negative")
	}
	if cfg.CacheMaxEntries < 0 {
		return errors.New("cache max entries cannot be negative")
	}
	if strings.TrimSpace(cfg.CacheDir) != "" {
		cfg.CacheDir = filepath.Clean(cfg.CacheDir)
		if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
			return fmt.Errorf("create synthesis cache directory: %w", err)
		}
	}
	state, err := newServiceState(ctx, cfg)
	if err != nil {
		return err
	}
	stopPruner := startRuntimePruner(ctx, state.providers, cfg.RuntimeIdle)
	defer stopPruner()

	for ctx.Err() == nil {
		conn, err := net.DialTimeout("tcp", cfg.BridgeAddr, 3*time.Second)
		if err != nil {
			sleepOrDone(ctx, time.Second)
			continue
		}
		log.Printf("connected to host bridge at %s", cfg.BridgeAddr)
		_ = publishServiceEvent(conn, "service.ready", "", map[string]any{
			"service":   serviceID,
			"providers": providerIDs(state.providers),
			"readers":   cfg.Readers,
		})

		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				_ = conn.Close()
			case <-done:
			}
		}()
		err = runServiceConnection(ctx, conn, state)
		close(done)
		_ = conn.Close()
		if errors.Is(err, errSystemShutdown) {
			return nil
		}
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			log.Printf("host bridge connection closed: %v", err)
		}
		sleepOrDone(ctx, time.Second)
	}
	return nil
}

func startRuntimePruner(ctx context.Context, providers map[string]tts.Provider, maxIdle time.Duration) func() {
	if maxIdle <= 0 {
		return func() {}
	}
	pruners := make(map[string]tts.RuntimePruner)
	for id, provider := range providers {
		if pruner, ok := provider.(tts.RuntimePruner); ok {
			pruners[id] = pruner
		}
	}
	if len(pruners) == 0 {
		return func() {}
	}
	pruneCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval := maxIdle / 4
		if interval < 5*time.Second {
			interval = 5 * time.Second
		}
		if interval > time.Minute {
			interval = time.Minute
		}
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-pruneCtx.Done():
				return
			case <-timer.C:
				total := 0
				for id, pruner := range pruners {
					removed := pruner.PruneIdleRuntime(maxIdle)
					if removed > 0 {
						log.Printf("pruned %d idle %s TTS runtime(s)", removed, id)
						total += removed
					}
				}
				if total > 0 {
					runtime.GC()
					debug.FreeOSMemory()
				}
				timer.Reset(interval)
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func runServiceConnection(ctx context.Context, conn net.Conn, state *serviceState) error {
	queue := newSynthesisQueue(ctx, conn, state, maxInt(1, state.cfg.Workers))
	defer queue.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := strings.TrimPrefix(strings.TrimSpace(scanner.Text()), "\ufeff")
		if line == "" {
			continue
		}
		var message map[string]any
		if err := json.Unmarshal([]byte(line), &message); err != nil {
			continue
		}
		if stringValue(message, "type") == "system.shutdown" {
			return errSystemShutdown
		}
		if stringValue(message, "type") != "tts.synthesize" {
			continue
		}
		if !queue.Enqueue(ctx, message) {
			jobID := firstText(message, objectValue(message, "data"), "job_id", "id", "subject")
			if jobID == "" {
				jobID = "tts"
			}
			state.publishTTSError(conn, jobID, "tts queue is full")
		}
	}
	return scanner.Err()
}

type synthesisQueue struct {
	conn   net.Conn
	state  *serviceState
	high   chan map[string]any
	normal chan map[string]any
	low    chan map[string]any
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newSynthesisQueue(ctx context.Context, conn net.Conn, state *serviceState, workers int) *synthesisQueue {
	queueCtx, cancel := context.WithCancel(ctx)
	q := &synthesisQueue{
		conn:   conn,
		state:  state,
		high:   make(chan map[string]any, synthesisHighQueueSize),
		normal: make(chan map[string]any, synthesisNormalQueueSize),
		low:    make(chan map[string]any, synthesisLowQueueSize),
		cancel: cancel,
	}
	for range maxInt(1, workers) {
		q.wg.Add(1)
		go q.worker(queueCtx)
	}
	return q
}

func (q *synthesisQueue) Enqueue(ctx context.Context, message map[string]any) bool {
	var target chan map[string]any
	switch synthesisPriority(message) {
	case "high":
		target = q.high
	case "low":
		target = q.low
	default:
		target = q.normal
	}
	select {
	case target <- message:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

func (q *synthesisQueue) Close() {
	q.cancel()
	q.wg.Wait()
}

func (q *synthesisQueue) worker(ctx context.Context) {
	defer q.wg.Done()
	for {
		message, ok := q.next(ctx)
		if !ok {
			return
		}
		handleSynthesisJob(ctx, q.conn, q.state, message)
	}
}

func (q *synthesisQueue) next(ctx context.Context) (map[string]any, bool) {
	select {
	case message := <-q.high:
		return message, true
	default:
	}
	select {
	case message := <-q.high:
		return message, true
	case message := <-q.normal:
		return message, true
	default:
	}
	select {
	case message := <-q.high:
		return message, true
	case message := <-q.normal:
		return message, true
	case message := <-q.low:
		return message, true
	case <-ctx.Done():
		return nil, false
	}
}

func synthesisPriority(message map[string]any) string {
	data := objectValue(message, "data")
	switch strings.ToLower(firstText(message, data, "priority", "queue_priority")) {
	case "realtime", "urgent", "high", "radio", "playout":
		return "high"
	case "batch", "background", "low":
		return "low"
	default:
		return "normal"
	}
}

func newServiceState(ctx context.Context, cfg serviceConfig) (*serviceState, error) {
	providers := tts.DefaultProviders()
	configureSpeakyAPIProvider(providers, cfg.SpeakyAPIURL)
	readers, err := tts.LoadReaders(cfg.Readers)
	if err != nil {
		readers = nil
	}
	state := &serviceState{
		ctx:            ctx,
		cfg:            cfg,
		providers:      providers,
		readers:        readers,
		readersErr:     err,
		dictionaries:   map[string]dictionaryResult{},
		synthesisCache: map[[sha256.Size]byte]synthesisCacheEntry{},
		providerHealth: map[string]*providerHealthStatus{},
		voiceRoutes:    map[[sha256.Size]byte]voiceRouteDecision{},
	}
	return state, nil
}

func configureSpeakyAPIProvider(providers map[string]tts.Provider, baseURL string) {
	if strings.TrimSpace(baseURL) == "" {
		return
	}
	providers["speakyapi"] = tts.NewSpeakyAPIProvider(baseURL)
}

func (s *serviceState) serviceReader(readerID string, language string) (tts.Reader, bool, error) {
	if strings.TrimSpace(readerID) == "" {
		return tts.Reader{}, false, nil
	}
	if s.readersErr != nil {
		return tts.Reader{}, false, s.readersErr
	}
	reader, ok := tts.SelectReader(s.readers, readerID, language, "")
	if !ok {
		return tts.Reader{}, false, fmt.Errorf("reader %q not found in %s", readerID, s.cfg.Readers)
	}
	return reader, true, nil
}

func (s *serviceState) dictionary(language string) (tts.Dictionary, error) {
	key := tts.NormalizeLanguage(language)
	if key == "" {
		key = tts.NormalizeLanguage(s.cfg.Language)
	}
	s.mu.Lock()
	if cached, ok := s.dictionaries[key]; ok {
		s.mu.Unlock()
		return cached.Dictionary, cached.Err
	}
	s.mu.Unlock()

	dictionary, err := tts.LoadDictionary(s.cfg.Dictionary, language)
	s.mu.Lock()
	s.dictionaries[key] = dictionaryResult{Dictionary: dictionary, Err: err}
	s.mu.Unlock()
	return dictionary, err
}

func (s *serviceState) cachedSynthesis(key [sha256.Size]byte) (synthesisCacheEntry, bool) {
	s.mu.Lock()
	entry, ok := s.synthesisCache[key]
	if !ok || time.Since(entry.LastUsed) > synthesisCacheMaxIdle {
		if ok {
			delete(s.synthesisCache, key)
		}
		s.mu.Unlock()
		entry, ok = s.loadPersistentSynthesis(key)
		if !ok {
			return synthesisCacheEntry{}, false
		}
		s.storeSynthesis(key, entry)
	} else {
		s.mu.Unlock()
	}

	info, err := os.Stat(entry.OutputPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() != entry.Bytes {
		s.mu.Lock()
		if current, exists := s.synthesisCache[key]; exists && current.OutputPath == entry.OutputPath {
			delete(s.synthesisCache, key)
		}
		s.mu.Unlock()
		return synthesisCacheEntry{}, false
	}

	entry.LastUsed = time.Now()
	if strings.TrimSpace(s.cfg.CacheDir) != "" {
		if err := s.writePersistentMetadata(key, entry); err != nil {
			log.Printf("synthesis cache metadata update failed: %v", err)
		}
	}
	s.mu.Lock()
	if s.synthesisCache == nil {
		s.synthesisCache = make(map[[sha256.Size]byte]synthesisCacheEntry)
	}
	s.synthesisCache[key] = entry
	s.mu.Unlock()
	return entry, true
}

func (s *serviceState) storeSynthesis(key [sha256.Size]byte, entry synthesisCacheEntry) {
	entry.LastUsed = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.synthesisCache == nil {
		s.synthesisCache = make(map[[sha256.Size]byte]synthesisCacheEntry)
	}
	s.synthesisCache[key] = entry
	maxEntries := s.cfg.CacheMaxEntries
	if maxEntries == 0 && strings.TrimSpace(s.cfg.CacheDir) == "" {
		maxEntries = synthesisCacheMaxEntries
	}
	for maxEntries > 0 && len(s.synthesisCache) > maxEntries {
		var oldestKey [sha256.Size]byte
		oldestAt := time.Now()
		for candidateKey, candidate := range s.synthesisCache {
			if candidate.LastUsed.Before(oldestAt) {
				oldestKey = candidateKey
				oldestAt = candidate.LastUsed
			}
		}
		delete(s.synthesisCache, oldestKey)
	}
}

func (s *serviceState) persistSynthesis(key [sha256.Size]byte, entry synthesisCacheEntry, data []byte) (synthesisCacheEntry, error) {
	if strings.TrimSpace(s.cfg.CacheDir) == "" {
		return entry, nil
	}
	audioPath, _ := s.persistentCachePaths(key)
	if err := writeFileAtomic(audioPath, data, 0o644); err != nil {
		return synthesisCacheEntry{}, err
	}
	entry.OutputPath = audioPath
	entry.LastUsed = time.Now()
	if err := s.writePersistentMetadata(key, entry); err != nil {
		_ = os.Remove(audioPath)
		return synthesisCacheEntry{}, err
	}
	s.prunePersistentCache()
	return entry, nil
}

func (s *serviceState) loadPersistentSynthesis(key [sha256.Size]byte) (synthesisCacheEntry, bool) {
	if strings.TrimSpace(s.cfg.CacheDir) == "" {
		return synthesisCacheEntry{}, false
	}
	audioPath, metadataPath := s.persistentCachePaths(key)
	raw, err := os.ReadFile(metadataPath)
	if err != nil {
		return synthesisCacheEntry{}, false
	}
	var record persistentSynthesisEntry
	if err := json.Unmarshal(raw, &record); err != nil {
		_ = os.Remove(metadataPath)
		return synthesisCacheEntry{}, false
	}
	info, err := os.Stat(audioPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() != record.Bytes {
		_ = os.Remove(audioPath)
		_ = os.Remove(metadataPath)
		return synthesisCacheEntry{}, false
	}
	return synthesisCacheEntry{
		OutputPath: audioPath,
		Format:     record.Format,
		SampleRate: record.SampleRate,
		Channels:   record.Channels,
		Bytes:      record.Bytes,
		ProviderID: record.ProviderID,
		ReaderID:   record.ReaderID,
		VoiceID:    record.VoiceID,
		Language:   record.Language,
		LastUsed:   record.LastUsed,
	}, true
}

func (s *serviceState) writePersistentMetadata(key [sha256.Size]byte, entry synthesisCacheEntry) error {
	_, metadataPath := s.persistentCachePaths(key)
	record := persistentSynthesisEntry{
		Format:     entry.Format,
		SampleRate: entry.SampleRate,
		Channels:   entry.Channels,
		Bytes:      entry.Bytes,
		ProviderID: entry.ProviderID,
		ReaderID:   entry.ReaderID,
		VoiceID:    entry.VoiceID,
		Language:   entry.Language,
		LastUsed:   entry.LastUsed,
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return writeFileAtomic(metadataPath, raw, 0o644)
}

func (s *serviceState) persistentCachePaths(key [sha256.Size]byte) (string, string) {
	stem := hex.EncodeToString(key[:])
	return filepath.Join(s.cfg.CacheDir, stem+".audio"), filepath.Join(s.cfg.CacheDir, stem+".json")
}

type persistentCacheFile struct {
	audioPath    string
	metadataPath string
	bytes        int64
	lastUsed     time.Time
}

func (s *serviceState) prunePersistentCache() {
	if strings.TrimSpace(s.cfg.CacheDir) == "" || (s.cfg.CacheMaxEntries == 0 && s.cfg.CacheMaxBytes == 0) {
		return
	}
	metadataFiles, err := filepath.Glob(filepath.Join(s.cfg.CacheDir, "*.json"))
	if err != nil {
		return
	}
	entries := make([]persistentCacheFile, 0, len(metadataFiles))
	var totalBytes int64
	for _, metadataPath := range metadataFiles {
		raw, readErr := os.ReadFile(metadataPath)
		var record persistentSynthesisEntry
		if readErr != nil || json.Unmarshal(raw, &record) != nil || record.Bytes < 0 {
			_ = os.Remove(metadataPath)
			continue
		}
		stem := strings.TrimSuffix(filepath.Base(metadataPath), filepath.Ext(metadataPath))
		audioPath := filepath.Join(s.cfg.CacheDir, stem+".audio")
		info, statErr := os.Stat(audioPath)
		if statErr != nil || !info.Mode().IsRegular() || info.Size() != record.Bytes {
			_ = os.Remove(audioPath)
			_ = os.Remove(metadataPath)
			continue
		}
		entries = append(entries, persistentCacheFile{
			audioPath:    audioPath,
			metadataPath: metadataPath,
			bytes:        record.Bytes,
			lastUsed:     record.LastUsed,
		})
		totalBytes += record.Bytes
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].lastUsed.Before(entries[j].lastUsed) })
	for len(entries) > 0 && ((s.cfg.CacheMaxEntries > 0 && len(entries) > s.cfg.CacheMaxEntries) ||
		(s.cfg.CacheMaxBytes > 0 && totalBytes > s.cfg.CacheMaxBytes)) {
		oldest := entries[0]
		entries = entries[1:]
		_ = os.Remove(oldest.audioPath)
		_ = os.Remove(oldest.metadataPath)
		totalBytes -= oldest.bytes
	}
}

func synthesisKey(providerID string, readerID string, targetSampleRate int, targetChannels int, req tts.Request) [sha256.Size]byte {
	hash := sha256.New()
	writePart := func(value string) {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	writePart(strings.ToLower(strings.TrimSpace(providerID)))
	writePart(strings.TrimSpace(readerID))
	writePart(req.Text)
	writePart(req.VoiceID)
	writePart(tts.NormalizeLanguage(req.Language))
	writePart(string(req.OutputFormat))
	writePart(strconv.Itoa(req.Rate))
	writePart(strconv.Itoa(req.Volume))
	writePart(strconv.FormatFloat(req.SentenceSilence, 'g', -1, 64))
	writePart(strconv.Itoa(targetSampleRate))
	writePart(strconv.Itoa(targetChannels))
	var key [sha256.Size]byte
	copy(key[:], hash.Sum(nil))
	return key
}

func handleSynthesisJob(ctx context.Context, conn net.Conn, state *serviceState, message map[string]any) {
	cfg := state.cfg
	data := objectValue(message, "data")
	jobID := firstText(message, data, "job_id", "id", "subject")
	if jobID == "" {
		jobID = fmt.Sprintf("tts-%d", time.Now().UnixNano())
	}

	text := firstText(message, data, "text")
	if strings.TrimSpace(text) == "" {
		state.publishTTSError(conn, jobID, "empty synthesis text")
		return
	}

	jobCtx, cancel, err := synthesisJobContext(ctx, cfg.Timeout, firstText(message, data, "deadline_at", "expires_at"))
	if err != nil {
		state.publishTTSError(conn, jobID, err.Error())
		return
	}
	defer cancel()
	if err := jobCtx.Err(); err != nil {
		state.publishTTSError(conn, jobID, "synthesis request expired before processing")
		return
	}
	reader, hasReader, err := state.serviceReader(firstText(message, data, "reader_id"), firstText(message, data, "language"))
	if err != nil {
		state.publishTTSError(conn, jobID, err.Error())
		return
	}

	providerID := firstText(message, data, "provider")
	voiceID := firstText(message, data, "voice_id")
	language := firstText(message, data, "language")
	if language == "" {
		language = cfg.Language
	}
	timezone := firstText(message, data, "timezone")
	if timezone == "" {
		timezone = cfg.Timezone
	}
	if hasReader {
		providerID = reader.Provider
		voiceID = reader.VoiceID
		if reader.Language != "" {
			language = reader.Language
		}
	}
	if providerID == "" {
		providerID = cfg.Provider
	}
	if providerID == "" {
		providerID = "auto"
	}
	dictionary, err := state.dictionary(language)
	if err != nil {
		state.publishTTSError(conn, jobID, err.Error())
		return
	}
	outputFormat := normalizeOutputFormat(firstText(message, data, "output_format", "format"))
	request := tts.Request{
		Text:            tts.NormalizeText(text, dictionary, timezone),
		VoiceID:         voiceID,
		Language:        language,
		OutputFormat:    outputFormat,
		Volume:          intValue(message, data, "volume", 100),
		Rate:            intValue(message, data, "rate", 0),
		SentenceSilence: floatValue(message, data, "sentence_silence", 0),
	}
	requestedProviderID := providerID
	backupUsed := false
	readerBackup := reader.Backup
	voiceRouteLocked := false
	if hasReader {
		decision, locked, routeErr := state.resolveVoiceRoute(
			jobCtx,
			firstText(message, data, "voice_route_key"),
			providerID,
			request.VoiceID,
			reader.Backup,
		)
		if routeErr != nil {
			state.publishTTSError(conn, jobID, routeErr.Error())
			return
		}
		if locked {
			providerID = decision.providerID
			request.VoiceID = decision.voiceID
			backupUsed = decision.backupUsed
			// Keep the configured backup when the primary health probe passed.
			// SpeakyAPI can report healthy voices and still fail a subsequent
			// synthesis request (for example when Maki rejects one voice). The
			// synthesis path must then be able to fall back to Piper. A route
			// already pinned to the backup has no further reader fallback.
			if decision.backupUsed {
				readerBackup = nil
			}
			voiceRouteLocked = true
		}
	}
	targetSampleRate := intValue(message, data, "target_sample_rate", 0)
	targetChannels := intValue(message, data, "target_channels", 0)
	outputPath := firstText(message, data, "output_path", "out")
	if outputPath == "" {
		outputPath = filepath.Join(cfg.OutDir, sanitizeFileName(jobID)+".wav")
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		state.publishTTSError(conn, jobID, err.Error())
		return
	}
	cacheKey := synthesisKey(providerID, reader.ID, targetSampleRate, targetChannels, request)
	if cached, ok := state.cachedSynthesis(cacheKey); ok {
		if err := materializeCachedAudio(cached.OutputPath, outputPath); err == nil {
			state.storeSynthesis(cacheKey, cached)
			_ = state.publishServiceEvent(conn, "tts.synthesized", jobID, map[string]any{
				"job_id":             jobID,
				"output_path":        outputPath,
				"bytes":              cached.Bytes,
				"format":             cached.Format,
				"sample_rate":        cached.SampleRate,
				"channels":           cached.Channels,
				"provider":           cached.ProviderID,
				"reader_id":          cached.ReaderID,
				"voice_id":           cached.VoiceID,
				"language":           cached.Language,
				"cache_hit":          true,
				"backup_used":        backupUsed,
				"requested_provider": requestedProviderID,
			})
			return
		}
	}

	audio, provider, activeVoiceID, attemptBackupUsed, err := synthesizeWithReaderBackup(jobCtx, state.providers, providerID, request, readerBackup)
	if err != nil {
		state.publishTTSError(conn, jobID, err.Error())
		return
	}
	backupUsed = backupUsed || attemptBackupUsed
	if err := jobCtx.Err(); err != nil {
		state.publishTTSError(conn, jobID, "synthesis request expired before completion")
		return
	}
	if audio.Format != tts.FormatWAV && audio.Format != tts.FormatPCM16LE {
		state.publishTTSError(conn, jobID, fmt.Sprintf("unsupported audio format %q", audio.Format))
		return
	}
	audio, err = tts.NormalizeAudio(audio, targetSampleRate, targetChannels)
	if err != nil {
		state.publishTTSError(conn, jobID, err.Error())
		return
	}
	if err := jobCtx.Err(); err != nil {
		state.publishTTSError(conn, jobID, "synthesis request expired before output was written")
		return
	}
	if err := writeFileAtomic(outputPath, audio.Data, 0o644); err != nil {
		state.publishTTSError(conn, jobID, err.Error())
		return
	}
	cacheEntry := synthesisCacheEntry{
		OutputPath: outputPath,
		Format:     audio.Format,
		SampleRate: audio.SampleRate,
		Channels:   audio.Channels,
		Bytes:      int64(len(audio.Data)),
		ProviderID: provider.ID(),
		ReaderID:   reader.ID,
		VoiceID:    activeVoiceID,
		Language:   language,
		LastUsed:   time.Now(),
	}
	if !backupUsed || voiceRouteLocked {
		if persisted, persistErr := state.persistSynthesis(cacheKey, cacheEntry, audio.Data); persistErr != nil {
			log.Printf("synthesis cache write failed: %v", persistErr)
		} else {
			cacheEntry = persisted
		}
		state.storeSynthesis(cacheKey, cacheEntry)
	}
	_ = state.publishServiceEvent(conn, "tts.synthesized", jobID, map[string]any{
		"job_id":             jobID,
		"output_path":        outputPath,
		"bytes":              len(audio.Data),
		"format":             audio.Format,
		"sample_rate":        audio.SampleRate,
		"channels":           audio.Channels,
		"provider":           provider.ID(),
		"reader_id":          reader.ID,
		"voice_id":           activeVoiceID,
		"language":           language,
		"cache_hit":          false,
		"backup_used":        backupUsed,
		"requested_provider": requestedProviderID,
	})
}

func (s *serviceState) resolveVoiceRoute(ctx context.Context, routeKey string, providerID string, voiceID string, backup *tts.ReaderBackup) (voiceRouteDecision, bool, error) {
	providerID = tts.NormalizeProvider(providerID)
	if backup == nil || providerID != "speakyapi" {
		return voiceRouteDecision{}, false, nil
	}
	backupProviderID := tts.NormalizeProvider(backup.Provider)
	backupVoiceID := strings.TrimSpace(backup.VoiceID)
	if backupProviderID == "" || backupVoiceID == "" {
		return voiceRouteDecision{}, false, nil
	}
	if strings.TrimSpace(routeKey) == "" {
		routeKey = fmt.Sprintf("request-%d", time.Now().UnixNano())
	}
	key := sha256.Sum256([]byte(strings.Join([]string{routeKey, providerID, voiceID, backupProviderID, backupVoiceID}, "\x00")))
	now := time.Now()
	s.routeMu.Lock()
	if decision, ok := s.voiceRoutes[key]; ok && now.Before(decision.expiresAt) {
		s.routeMu.Unlock()
		return decision, true, nil
	}
	s.routeMu.Unlock()

	available, err := s.providerAvailable(ctx, providerID)
	if err != nil {
		return voiceRouteDecision{}, false, err
	}
	decision := voiceRouteDecision{
		providerID: providerID,
		voiceID:    voiceID,
		expiresAt:  now.Add(voiceRouteTTL),
	}
	if !available {
		decision.providerID = backupProviderID
		decision.voiceID = backupVoiceID
		decision.backupUsed = true
	}

	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	if existing, ok := s.voiceRoutes[key]; ok && time.Now().Before(existing.expiresAt) {
		return existing, true, nil
	}
	s.pruneVoiceRoutesLocked(time.Now())
	s.voiceRoutes[key] = decision
	return decision, true, nil
}

func (s *serviceState) providerAvailable(ctx context.Context, providerID string) (bool, error) {
	for {
		now := time.Now()
		s.routeMu.Lock()
		if s.providerHealth == nil {
			s.providerHealth = map[string]*providerHealthStatus{}
		}
		status := s.providerHealth[providerID]
		if status != nil && status.ready == nil && now.Before(status.expiresAt) {
			available := status.available
			s.routeMu.Unlock()
			return available, nil
		}
		if status == nil || (status.ready == nil && !now.Before(status.expiresAt)) {
			status = &providerHealthStatus{ready: make(chan struct{})}
			s.providerHealth[providerID] = status
			probeCtx := s.ctx
			if probeCtx == nil {
				probeCtx = context.Background()
			}
			go s.probeProvider(probeCtx, providerID, status)
		}
		ready := status.ready
		s.routeMu.Unlock()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-ready:
		}
	}
}

func (s *serviceState) probeProvider(ctx context.Context, providerID string, status *providerHealthStatus) {
	probeCtx, cancel := context.WithTimeout(ctx, providerHealthTimeout)
	defer cancel()
	_, err := listVoicesForProvider(probeCtx, s.providers, providerID)
	available := err == nil
	ttl := providerHealthyTTL
	if !available {
		ttl = providerUnavailableTTL
	}
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	if current := s.providerHealth[providerID]; current != status {
		return
	}
	status.available = available
	status.expiresAt = time.Now().Add(ttl)
	close(status.ready)
	status.ready = nil
}

func (s *serviceState) pruneVoiceRoutesLocked(now time.Time) {
	if s.voiceRoutes == nil {
		s.voiceRoutes = map[[sha256.Size]byte]voiceRouteDecision{}
	}
	for key, decision := range s.voiceRoutes {
		if !now.Before(decision.expiresAt) {
			delete(s.voiceRoutes, key)
		}
	}
	for len(s.voiceRoutes) >= voiceRouteMaxEntries {
		var oldestKey [sha256.Size]byte
		var oldestExpiry time.Time
		found := false
		for key, decision := range s.voiceRoutes {
			if !found || decision.expiresAt.Before(oldestExpiry) {
				oldestKey = key
				oldestExpiry = decision.expiresAt
				found = true
			}
		}
		if !found {
			break
		}
		delete(s.voiceRoutes, oldestKey)
	}
}

func synthesisJobContext(parent context.Context, timeout time.Duration, deadlineRaw string) (context.Context, context.CancelFunc, error) {
	deadlineRaw = strings.TrimSpace(deadlineRaw)
	if deadlineRaw == "" {
		ctx, cancel := context.WithTimeout(parent, timeout)
		return ctx, cancel, nil
	}
	deadline, err := time.Parse(time.RFC3339Nano, deadlineRaw)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid synthesis deadline %q", deadlineRaw)
	}
	if timeout > 0 {
		serviceDeadline := time.Now().Add(timeout)
		if serviceDeadline.Before(deadline) {
			deadline = serviceDeadline
		}
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	return ctx, cancel, nil
}

func serviceReader(path string, readerID string, language string) (tts.Reader, bool, error) {
	if strings.TrimSpace(readerID) == "" {
		return tts.Reader{}, false, nil
	}
	readers, err := tts.LoadReaders(path)
	if err != nil {
		return tts.Reader{}, false, err
	}
	reader, ok := tts.SelectReader(readers, readerID, language, "")
	if !ok {
		return tts.Reader{}, false, fmt.Errorf("reader %q not found in %s", readerID, path)
	}
	return reader, true, nil
}

func (s *serviceState) publishTTSError(conn net.Conn, jobID string, detail string) {
	_ = s.publishServiceEvent(conn, "tts.failed", jobID, map[string]any{
		"job_id": jobID,
		"error":  detail,
	})
}

func (s *serviceState) publishServiceEvent(conn net.Conn, eventType string, subject string, data map[string]any) error {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	return publishServiceEvent(conn, eventType, subject, data)
}

func publishServiceEvent(conn net.Conn, eventType string, subject string, data map[string]any) error {
	return json.NewEncoder(conn).Encode(events.Event{
		Type:      eventType,
		Source:    serviceID,
		Timestamp: time.Now().UTC(),
		Subject:   subject,
		Data:      data,
	})
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	tmp := fmt.Sprintf("%s.%d.tmp", path, time.Now().UnixNano())
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func materializeCachedAudio(source string, target string) error {
	if filepath.Clean(source) == filepath.Clean(target) {
		return nil
	}
	tmp := fmt.Sprintf("%s.%d.tmp", target, time.Now().UnixNano())
	if err := os.Link(source, tmp); err == nil {
		if err := os.Rename(tmp, target); err == nil {
			return nil
		}
		_ = os.Remove(tmp)
	}

	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func providerIDs(providers map[string]tts.Provider) []string {
	ids := make([]string, 0, len(providers))
	for id := range providers {
		ids = append(ids, id)
	}
	return ids
}

func objectValue(message map[string]any, key string) map[string]any {
	if value, ok := message[key].(map[string]any); ok {
		return value
	}
	return nil
}

func firstText(message map[string]any, data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringValue(data, key); value != "" {
			return value
		}
		if value := stringValue(message, key); value != "" {
			return value
		}
	}
	return ""
}

func stringValue(message map[string]any, key string) string {
	if message == nil {
		return ""
	}
	switch value := message[key].(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func intValue(message map[string]any, data map[string]any, key string, fallback int) int {
	for _, source := range []map[string]any{data, message} {
		switch value := source[key].(type) {
		case float64:
			return int(value)
		case int:
			return value
		case string:
			var parsed int
			if _, err := fmt.Sscanf(value, "%d", &parsed); err == nil {
				return parsed
			}
		}
	}
	return fallback
}

func floatValue(message map[string]any, data map[string]any, key string, fallback float64) float64 {
	for _, source := range []map[string]any{data, message} {
		switch value := source[key].(type) {
		case float64:
			return value
		case int:
			return float64(value)
		case string:
			var parsed float64
			if _, err := fmt.Sscanf(value, "%f", &parsed); err == nil {
				return parsed
			}
		}
	}
	return fallback
}

func normalizeOutputFormat(format string) tts.AudioFormat {
	switch tts.AudioFormat(strings.ToLower(strings.TrimSpace(format))) {
	case tts.FormatPCM16LE:
		return tts.FormatPCM16LE
	default:
		return tts.FormatWAV
	}
}

func sanitizeFileName(value string) string {
	var builder strings.Builder
	for _, char := range value {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' {
			builder.WriteRune(char)
		}
	}
	if builder.Len() == 0 {
		return "tts"
	}
	return builder.String()
}

func sleepOrDone(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func envOrDefault(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envIntOrDefault(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envFloatOrDefault(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	var parsed float64
	if _, err := fmt.Sscanf(value, "%f", &parsed); err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func envBoolOrDefault(key string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "":
		return fallback
	case "1", "true", "yes", "on", "enabled":
		return true
	case "0", "false", "no", "off", "disabled":
		return false
	default:
		return fallback
	}
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}

type ttsRuntimeEnv struct {
	PiperVoicesDir        string
	KokoroModelDir        string
	KokoroLang            string
	KokoroRuntimeProvider string
	KokoroThreads         int
	KokoroSpeed           float64
	KokoroLengthScale     float64
}

func setTTSRuntimeEnv(options ttsRuntimeEnv) {
	if strings.TrimSpace(options.PiperVoicesDir) != "" {
		_ = os.Setenv("HAZE_PIPER_VOICES_DIR", strings.TrimSpace(options.PiperVoicesDir))
	}
	if strings.TrimSpace(options.KokoroModelDir) != "" {
		_ = os.Setenv("HAZE_KOKORO_MODEL_DIR", strings.TrimSpace(options.KokoroModelDir))
	}
	if strings.TrimSpace(options.KokoroLang) != "" {
		_ = os.Setenv("HAZE_KOKORO_LANG", strings.TrimSpace(options.KokoroLang))
	}
	if strings.TrimSpace(options.KokoroRuntimeProvider) != "" {
		_ = os.Setenv("HAZE_KOKORO_PROVIDER", strings.TrimSpace(options.KokoroRuntimeProvider))
	}
	if options.KokoroThreads > 0 {
		_ = os.Setenv("HAZE_KOKORO_THREADS", fmt.Sprintf("%d", options.KokoroThreads))
	}
	if options.KokoroSpeed > 0 {
		_ = os.Setenv("HAZE_KOKORO_SPEED", fmt.Sprintf("%g", options.KokoroSpeed))
	}
	if options.KokoroLengthScale > 0 {
		_ = os.Setenv("HAZE_KOKORO_LENGTH_SCALE", fmt.Sprintf("%g", options.KokoroLengthScale))
	}
}

func kokoroRuntimeLang(language string) string {
	normalized := tts.NormalizeLanguage(language)
	if strings.HasPrefix(normalized, "en") || normalized == "" {
		return "en-us"
	}
	if strings.HasPrefix(normalized, "zh") || strings.HasPrefix(normalized, "cmn") || strings.HasPrefix(normalized, "yue") {
		return "zh"
	}
	return normalized
}

func resolveReader(path string, readerID string, lang string) (tts.Reader, error) {
	if readerID == "" {
		return tts.Reader{}, nil
	}
	readers, err := tts.LoadReaders(path)
	if err != nil {
		return tts.Reader{}, err
	}
	reader, ok := tts.SelectReader(readers, readerID, lang, "")
	if !ok {
		return tts.Reader{}, fmt.Errorf("reader %q not found in %s", readerID, path)
	}
	return reader, nil
}

func listVoicesForProvider(ctx context.Context, providers map[string]tts.Provider, providerID string) ([]tts.Voice, error) {
	candidates, err := providerCandidates(providers, providerID)
	if err != nil {
		return nil, err
	}
	var voices []tts.Voice
	var failures []error
	for _, provider := range candidates {
		providerVoices, err := provider.ListVoices(ctx)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", provider.ID(), err))
			continue
		}
		voices = append(voices, providerVoices...)
	}
	if len(voices) > 0 {
		return voices, nil
	}
	return nil, errors.Join(failures...)
}

func synthesizeWithProvider(ctx context.Context, providers map[string]tts.Provider, providerID string, req tts.Request) (tts.Audio, tts.Provider, error) {
	candidates, err := providerCandidates(providers, providerID)
	if err != nil {
		return tts.Audio{}, nil, err
	}
	var failures []error
	for _, provider := range candidates {
		audio, err := provider.Synthesize(ctx, req)
		if err == nil {
			return audio, provider, nil
		}
		failures = append(failures, fmt.Errorf("%s: %w", provider.ID(), err))
	}
	return tts.Audio{}, nil, errors.Join(failures...)
}

func synthesizeWithReaderBackup(ctx context.Context, providers map[string]tts.Provider, providerID string, req tts.Request, backup *tts.ReaderBackup) (tts.Audio, tts.Provider, string, bool, error) {
	audio, provider, primaryErr := synthesizeWithProvider(ctx, providers, providerID, req)
	if primaryErr == nil {
		return audio, provider, req.VoiceID, false, nil
	}
	if backup == nil || !readerBackupEligible(ctx, primaryErr) {
		return tts.Audio{}, nil, "", false, primaryErr
	}
	backupProviderID := tts.NormalizeProvider(backup.Provider)
	backupVoiceID := strings.TrimSpace(backup.VoiceID)
	if backupProviderID == tts.NormalizeProvider(providerID) && backupVoiceID == strings.TrimSpace(req.VoiceID) {
		return tts.Audio{}, nil, "", false, primaryErr
	}
	backupRequest := req
	backupRequest.VoiceID = backupVoiceID
	audio, provider, backupErr := synthesizeWithProvider(ctx, providers, backupProviderID, backupRequest)
	if backupErr != nil {
		return tts.Audio{}, nil, "", false, errors.Join(
			fmt.Errorf("primary %s: %w", providerID, primaryErr),
			fmt.Errorf("backup %s: %w", backupProviderID, backupErr),
		)
	}
	return audio, provider, backupVoiceID, true, nil
}

func readerBackupEligible(ctx context.Context, primaryErr error) bool {
	return ctx.Err() == nil &&
		errors.Is(primaryErr, tts.ErrProviderUnavailable) &&
		!errors.Is(primaryErr, context.Canceled) &&
		!errors.Is(primaryErr, context.DeadlineExceeded)
}

func providerCandidates(providers map[string]tts.Provider, providerID string) ([]tts.Provider, error) {
	normalized := tts.NormalizeProvider(providerID)
	if normalized == "fast" {
		candidates := make([]tts.Provider, 0, 3)
		for _, id := range []string{"sapi5", "espeak", "piper", "speakyapi"} {
			if provider := providers[id]; provider != nil {
				candidates = append(candidates, provider)
			}
		}
		if len(candidates) == 0 {
			return nil, fmt.Errorf("%w: fast", tts.ErrProviderUnavailable)
		}
		return candidates, nil
	}
	if normalized == "" || normalized == "auto" {
		candidates := make([]tts.Provider, 0, 4)
		for _, id := range []string{"piper", "kokoro", "sapi5", "espeak", "speakyapi"} {
			if provider := providers[id]; provider != nil {
				candidates = append(candidates, provider)
			}
		}
		if len(candidates) == 0 {
			return nil, fmt.Errorf("%w: auto", tts.ErrProviderUnavailable)
		}
		return candidates, nil
	}
	provider := providers[normalized]
	if provider == nil {
		return nil, fmt.Errorf("%w: %s", tts.ErrProviderUnavailable, providerID)
	}
	return []tts.Provider{provider}, nil
}
