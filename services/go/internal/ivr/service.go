package ivr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

const serviceID = "haze-ivr"
const staticPromptManifestVersion = 1

var locationCodeCORSOrigins = map[string]struct{}{
	"https://teleweather.ca":     {},
	"https://www.teleweather.ca": {},
	"http://localhost:4321":      {},
	"http://127.0.0.1:4321":      {},
}

type staticPromptManifest struct {
	Version     int                         `json:"version"`
	Fingerprint string                      `json:"fingerprint"`
	Provider    string                      `json:"provider"`
	ReaderID    string                      `json:"reader_id,omitempty"`
	VoiceID     string                      `json:"voice_id,omitempty"`
	Language    string                      `json:"language,omitempty"`
	GeneratedAt time.Time                   `json:"generated_at"`
	Files       map[string]staticPromptFile `json:"files"`
}

type staticPromptFile struct {
	Text string `json:"text"`
	WAV  string `json:"wav"`
	PCMU string `json:"pcmu"`
	G722 string `json:"g722,omitempty"`
}

type Service struct {
	ctx            context.Context
	cfg            loadedConfig
	resolver       *Resolver
	searchIndex    *locationSearchIndex
	capabilities   *locationdb.CapabilityCatalog
	pointQuery     pointQueryFunc
	pointSlots     chan struct{}
	cache          *ProductCache
	bridge         *bridgeClient
	mediaBridge    *bridgeClient
	broadcast      *broadcastHub
	store          datastore.Store
	metrics        metrics
	twilio         *twilioRuntime
	twilioRequired bool
}

type metrics struct {
	Lookups             atomic.Uint64 `json:"-"`
	CacheRequests       atomic.Uint64 `json:"-"`
	CacheErrors         atomic.Uint64 `json:"-"`
	SIPMessages         atomic.Uint64 `json:"-"`
	HTTPRequests        atomic.Uint64 `json:"-"`
	SearchVoice         atomic.Uint64 `json:"-"`
	SearchT9            atomic.Uint64 `json:"-"`
	SearchMultitap      atomic.Uint64 `json:"-"`
	SearchAccepted      atomic.Uint64 `json:"-"`
	SearchConfirmations atomic.Uint64 `json:"-"`
	SearchAmbiguities   atomic.Uint64 `json:"-"`
	SearchNoMatch       atomic.Uint64 `json:"-"`
	ASRFailures         atomic.Uint64 `json:"-"`
	ASRBusy             atomic.Uint64 `json:"-"`
	ASRTimeouts         atomic.Uint64 `json:"-"`
	ASRUnavailable      atomic.Uint64 `json:"-"`
	ASRLatencySamples   atomic.Uint64 `json:"-"`
	ASRLatencyMS        atomic.Uint64 `json:"-"`
}

func Run(ctx context.Context, options Options) error {
	loadDotEnv(filepath.Join(filepath.Dir(filepath.Clean(options.ConfigPath)), ".env"))
	loadDotEnv(".env")
	cfg, err := loadConfig(options.ConfigPath, options)
	if err != nil {
		return err
	}
	if !cfg.enabled() {
		log.Printf("IVR service disabled")
		<-ctx.Done()
		return nil
	}
	if err := os.MkdirAll(cfg.cacheDir(), 0o755); err != nil {
		return err
	}
	var store datastore.Store
	storeCtx, cancelStoreOpen := context.WithTimeout(ctx, 5*time.Second)
	if opened, openErr := datastore.Open(storeCtx, cfg.Root.Storage, cfg.BaseDir); openErr == nil {
		store = opened
	} else if !errors.Is(openErr, datastore.ErrNotConfigured) {
		log.Printf("IVR alert archive unavailable; active alert menu disabled until restart: %v", openErr)
	}
	cancelStoreOpen()
	if store != nil {
		defer store.Close()
	}
	for ctx.Err() == nil {
		bridge, err := connectBridge(ctx, options.BridgeAddr)
		if err != nil {
			log.Printf("IVR waiting for event bridge: %v", err)
			sleepOrDone(ctx, time.Second)
			continue
		}
		mediaBridgeAddr := firstNonBlank(options.MediaBridgeAddr, options.BridgeAddr)
		var mediaBridge *bridgeClient
		if strings.TrimSpace(mediaBridgeAddr) != "" && strings.TrimSpace(mediaBridgeAddr) != strings.TrimSpace(options.BridgeAddr) {
			mediaBridge, err = connectBridge(ctx, mediaBridgeAddr)
			if err != nil {
				log.Printf("IVR media bridge unavailable; live broadcast monitoring disabled until reconnect: %v", err)
			}
		}
		service := &Service{
			cfg:         cfg,
			bridge:      bridge,
			mediaBridge: mediaBridge,
			store:       store,
		}
		service.resolver = NewResolver(cfg)
		if capabilities, loaded := locationdb.LoadCapabilityCatalog(cfg.BaseDir); loaded {
			service.capabilities = capabilities
			log.Printf("IVR capability targets indexed forecasts=%d observations=%d air_quality=%d hydrometric=%d marine=%d",
				capabilities.Count(locationdb.CapabilityForecast),
				capabilities.Count(locationdb.CapabilityObservation),
				capabilities.Count(locationdb.CapabilityAirQuality),
				capabilities.Count(locationdb.CapabilityHydrometric),
				capabilities.Count(locationdb.CapabilityMarineForecast))
		}
		if cfg.IVR.Search.enabled() {
			searchIndex, searchErr := loadLocationSearchIndex(cfg, service.resolver)
			if searchErr != nil {
				log.Printf("IVR location search unavailable, numeric entry remains active: %v", searchErr)
			} else {
				service.searchIndex = searchIndex
				log.Printf("IVR location search indexed %d canonical targets", len(searchIndex.targets))
			}
		}
		service.cache = NewProductCache(cfg, bridge)
		service.twilioRequired = cfg.IVR.Twilio.Enabled
		if cfg.IVR.Twilio.Enabled {
			service.twilio, err = newTwilioRuntime(cfg)
			if err != nil {
				log.Printf("IVR Twilio adapter unavailable, SIP remains active: %v", err)
			}
		}
		err = service.runConnected(ctx)
		_ = bridge.Close()
		if mediaBridge != nil {
			_ = mediaBridge.Close()
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("IVR bridge disconnected: %v", err)
		sleepOrDone(ctx, time.Second)
	}
	return nil
}

func loadDotEnv(path string) {
	path = filepath.Clean(path)
	raw, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) || filepath.Base(path) != ".env" {
			return
		}
		examplePath := filepath.Join(filepath.Dir(path), ".env.example")
		exampleRaw, readErr := os.ReadFile(examplePath)
		if readErr != nil {
			log.Printf("WARN .env file not found and no .env.example is available: %s", path)
			return
		}
		if writeErr := os.WriteFile(path, exampleRaw, 0o600); writeErr != nil {
			log.Printf("WARN .env file not found and could not create %s: %v", path, writeErr)
			return
		}
		log.Printf("WARN .env file not found: created %s from %s", path, examplePath)
		raw = exampleRaw
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" || os.Getenv(key) != "" {
			continue
		}
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		_ = os.Setenv(key, value)
	}
}

func drainBridgeEvents(ctx context.Context, events <-chan map[string]any) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-events:
			if !ok {
				return
			}
		}
	}
}

func (s *Service) runConnected(ctx context.Context) error {
	s.ctx = ctx
	if err := s.bridge.Publish(map[string]any{
		"type":   "service.ready",
		"source": serviceID,
		"data": map[string]any{
			"service":             serviceID,
			"mode":                s.cfg.IVR.Mode,
			"http_addr":           s.cfg.IVR.HTTP.Addr,
			"sip_addr":            strings.Join(s.cfg.IVR.SIP.listenAddrs(), ","),
			"sip_addrs":           s.cfg.IVR.SIP.listenAddrs(),
			"cache_dir":           s.cfg.cacheDir(),
			"max_render_inflight": s.cfg.IVR.MaxRenderInflight,
			"search_enabled":      s.searchIndex != nil,
			"search_targets":      locationSearchTargetCount(s.searchIndex),
		},
	}); err != nil {
		return err
	}
	s.broadcast = newBroadcastHub()
	if mediaServiceURL := s.mediaServiceURLForBroadcast(); mediaServiceURL != "" {
		s.broadcast.SetHTTPSource(mediaServiceURL)
		go drainBridgeEvents(ctx, s.bridge.Events())
		if s.mediaBridge != nil {
			go drainBridgeEvents(ctx, s.mediaBridge.Events())
		}
	} else if s.mediaBridge != nil {
		go drainBridgeEvents(ctx, s.bridge.Events())
		go s.broadcast.run(ctx, s.mediaBridge.Events())
	} else {
		broadcastEvents := s.bridge.Events()
		go s.broadcast.run(ctx, broadcastEvents)
	}

	errCh := make(chan error, 2)
	if s.cfg.IVR.HTTP.Enabled {
		server := &http.Server{
			Addr:              s.cfg.IVR.HTTP.Addr,
			Handler:           s.routes(),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		}()
		go func() {
			log.Printf("IVR provider webhook listening on %s", s.cfg.IVR.HTTP.Addr)
			err := server.ListenAndServe()
			if !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}
	if s.cfg.IVR.SIP.Enabled {
		go func() {
			if err := s.runSIP(ctx); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
	}
	if s.cfg.IVR.Cache.StaticOnStartup {
		go s.generateStaticPrompts(ctx)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (s *Service) mediaServiceURLForBroadcast() string {
	if s == nil {
		return ""
	}
	return s.cfg.mediaServiceBaseURL()
}

func (s *Service) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ivr/v1/health", s.handleHealth)
	mux.HandleFunc("/ivr/v1/lookup", s.handleLookup)
	mux.HandleFunc("/ivr/v1/location-codes", s.handleLocationCodes)
	mux.HandleFunc("/ivr/v1/point", s.handlePoint)
	mux.HandleFunc("/point", s.handlePoint)
	mux.HandleFunc("/ivr/v1/prompt", s.handlePrompt)
	mux.HandleFunc("/ivr/v1/audio", s.handleAudio)
	mux.HandleFunc("/ivr/v1/alert_audio", s.handleAlertAudio)
	twimlHandler := http.Handler(http.HandlerFunc(s.handleTwiML))
	if s.twilioRequired {
		twimlHandler = s.requireTwilioCallback(twimlHandler)
	}
	mux.Handle("/ivr/v1/twiml", twimlHandler)
	mux.HandleFunc("/ivr/v1/twilio/media", s.handleTwilioMedia)
	mux.Handle("/ivr/v1/twilio/recover/", s.requireTwilioCallback(http.HandlerFunc(s.handleTwilioRecovery)))
	mux.Handle("/ivr/v1/twilio/result/", s.requireTwilioCallback(http.HandlerFunc(s.handleTwilioResult)))
	mux.HandleFunc("/ivr/v1/twilio/prompt", s.requireTwilioMedia(s.handlePrompt))
	mux.HandleFunc("/ivr/v1/twilio/audio", s.requireTwilioMedia(s.handleAudio))
	mux.HandleFunc("/ivr/v1/twilio/alert_audio", s.requireTwilioMedia(s.handleAlertAudio))
	mux.HandleFunc("/ivr/v1/metrics", s.handleMetrics)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		s.metrics.HTTPRequests.Add(1)
		if request.URL.Path == "/ivr/v1/location-codes" || request.URL.Path == "/ivr/v1/point" || request.URL.Path == "/point" {
			if origin := locationCodeCORSOrigin(request.Header.Get("Origin")); origin != "" {
				writer.Header().Set("Access-Control-Allow-Origin", origin)
				writer.Header().Set("Access-Control-Allow-Methods", http.MethodGet)
				writer.Header().Set("Access-Control-Allow-Headers", "Accept")
				writer.Header().Set("Access-Control-Max-Age", "600")
			}
			writer.Header().Add("Vary", "Origin")
			if request.Method == http.MethodOptions {
				if writer.Header().Get("Access-Control-Allow-Origin") == "" {
					http.Error(writer, "origin not allowed", http.StatusForbidden)
					return
				}
				writer.WriteHeader(http.StatusNoContent)
				return
			}
		}
		mux.ServeHTTP(writer, request)
	})
}

func locationCodeCORSOrigin(origin string) string {
	origin = strings.TrimSpace(origin)
	if _, ok := locationCodeCORSOrigins[origin]; ok {
		return origin
	}
	return ""
}

func (s *Service) handleHealth(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, map[string]any{
		"ok":      true,
		"service": serviceID,
		"mode":    s.cfg.IVR.Mode,
	})
}

func (s *Service) handleLookup(writer http.ResponseWriter, request *http.Request) {
	code := request.URL.Query().Get("code")
	location, err := s.resolveLocation(code)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusNotFound)
		return
	}
	s.metrics.Lookups.Add(1)
	writeJSON(writer, location)
}

func (s *Service) handlePrompt(writer http.ResponseWriter, request *http.Request) {
	menuID := request.URL.Query().Get("menu")
	lineKey := fallbackText(request.URL.Query().Get("line"), "main")
	format := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("format")))
	force := request.URL.Query().Get("force") == "1"
	values := s.promptValues(promptValuesFromRequest(request))
	if !force {
		if audio, ok := s.staticPromptAudio(menuID, lineKey, values); ok {
			s.serveCachedAudio(writer, request, audio, format, http.StatusOK)
			return
		}
	}
	audio, err := s.cache.GetPromptWithPolicy(request.Context(), menuID, lineKey, values, s.staticPromptPolicy(), force)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadGateway)
		return
	}
	s.serveCachedAudio(writer, request, audio, format, http.StatusOK)
}

func (s *Service) handleAudio(writer http.ResponseWriter, request *http.Request) {
	format := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("format")))
	force := request.URL.Query().Get("force") == "1"
	packages := packagesFromRequest(request)
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.servePromptAudio(writer, request, "error", "invalid_code", nil, http.StatusNotFound)
		return
	}
	product, err := s.productForLocation(request.Context(), location, packages, force)
	if err != nil {
		log.Printf("IVR product audio unavailable for %s packages=%s: %v", firstNonBlank(location.Code, location.FeedID), strings.Join(normalizePackages(packages, s.cfg.IVR.DefaultPackages), ","), err)
		s.servePromptAudio(writer, request, "weather_product", "unavailable", nil, http.StatusBadGateway)
		return
	}
	if format == "pcmu" {
		writer.Header().Set("Content-Type", "audio/basic")
		http.ServeFile(writer, request, product.PCMUPath)
		return
	}
	writer.Header().Set("Content-Type", "audio/wav")
	http.ServeFile(writer, request, product.WAVPath)
}

func (s *Service) handleAlertAudio(writer http.ResponseWriter, request *http.Request) {
	format := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("format")))
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.servePromptAudio(writer, request, "error", "invalid_code", nil, http.StatusNotFound)
		return
	}
	alerts := s.activeIVRAlerts(request.Context(), location)
	kind := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("kind")))
	text := ""
	lineKey := ""
	switch kind {
	case "menu":
		text = s.alertMenuText(location, alerts)
		lineKey = "alert_menu_" + firstNonBlank(location.FeedID, location.Code, "default")
	case "location_menu":
		if len(alerts) == 0 {
			text = s.cfg.Prompts.MenuLine("location_menu", s.locationMenuMainLine(location), s.promptValues(map[string]string{"location": spokenLocationName(location), "feed_id": location.FeedID}))
		} else {
			text = s.locationMenuAlertText(location, len(alerts))
		}
		lineKey = "location_menu_" + firstNonBlank(location.FeedID, location.Code, "default")
	case "observation_choices":
		choices := s.observationStationChoices(location)
		if len(choices) == 0 {
			text = "No nearby observation station is available."
		} else {
			text = localizedObservationChoicesPrompt(location.Language, choices)
		}
		lineKey = "observation_choices_" + firstNonBlank(location.Code, location.FeedID, "default")
	default:
		alert, ok := ivrAlertByDigit(alerts, request.URL.Query().Get("index"))
		if !ok {
			s.serveTextPromptAudio(writer, request, "alert_unavailable", "That alert is no longer active.", http.StatusOK)
			return
		}
		text = s.alertReadoutText(location, alert)
		lineKey = "alert_readout_" + firstNonBlank(alert.ID, fmt.Sprint(time.Now().UnixNano()))
	}
	s.serveTextPromptAudioWithFormat(writer, request, lineKey, text, format, http.StatusOK)
}

func (s *Service) handleTwiML(writer http.ResponseWriter, request *http.Request) {
	state := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("state")))
	switch state {
	case "", "entry":
		if state == "" || strings.TrimSpace(request.FormValue("Digits")) == "" {
			s.writeEntryTwiML(writer, request)
			return
		}
		s.handleEntryDigit(writer, request)
	case "location_code":
		s.handleLocationCodeTwiML(writer, request)
	case "location_number":
		s.handleLocationNumberTwiML(writer, request)
	case "location_menu", "location":
		s.writeLocationMenuTwiML(writer, request)
	case "location_option":
		s.handleLocationOptionTwiML(writer, request)
	case "observation_choice":
		s.handleObservationChoiceTwiML(writer, request)
	case "alert_menu":
		s.writeAlertMenuTwiML(writer, request)
	case "alert_option":
		s.handleAlertOptionTwiML(writer, request)
	case "alert_readout_option":
		s.handleAlertReadoutOptionTwiML(writer, request)
	case "ivr_menu":
		s.writeConfiguredMenuTwiML(writer, request)
	case "ivr_menu_option":
		s.handleConfiguredMenuOptionTwiML(writer, request)
	default:
		s.writeEntryTwiML(writer, request)
	}
}

func ivrExtensionFromRequest(request *http.Request) string {
	if request == nil {
		return ""
	}
	for _, value := range []string{
		request.URL.Query().Get("extension"),
		request.URL.Query().Get("line"),
		request.FormValue("extension"),
		request.FormValue("Called"),
		request.FormValue("To"),
	} {
		if extension := sipURIUser(value); extension != "" {
			return extension
		}
	}
	return ""
}

func (s *Service) writeEntryTwiML(writer http.ResponseWriter, request *http.Request) {
	line, matched := s.cfg.IVR.extensionLine(ivrExtensionFromRequest(request))
	if !line.enabled() {
		if matched {
			writer.Header().Set("X-Haze-IVR-Line", line.Extension)
		}
		writeTwiML(writer, twimlHangup())
		return
	}
	directProvince := line.directProvince()
	if directProvince != "" {
		if language, single := s.singleConfiguredLanguage(); single {
			s.writeLocationNumberPromptWithPlays(writer, request, language, directProvince, []string{
				promptURL(request, "entry", "greeting", s.extensionGreetingValues(line)),
			})
			return
		}
	}
	entry, _ := s.cfg.Prompts.Menu("entry")
	lineKey := s.menuMainLine("entry", "")
	numDigits := "1"
	timeout := entry.Timeout
	if _, single := s.singleConfiguredLanguage(); single {
		numDigits = ""
		timeout = locationCodeAutoSubmitTimeout()
	}
	params := map[string]string{"state": "entry"}
	if directProvince != "" {
		params["province"] = directProvince
	}
	body := twimlGather(twimlURL(request, "/ivr/v1/twiml", params), numDigits, "#", timeout, []string{
		promptURL(request, "entry", "greeting", s.extensionGreetingValues(line)),
		promptURL(request, "entry", lineKey, s.promptValues(nil)),
	}, []string{
		twimlTimeoutHangup(request, s.promptValues(nil)),
	})
	writeTwiML(writer, body)
}

func (s *Service) handleEntryDigit(writer http.ResponseWriter, request *http.Request) {
	digit := strings.TrimSpace(request.FormValue("Digits"))
	directProvince := normalizeProvinceCode(request.URL.Query().Get("province"))
	if directProvince == "CA" || !validProvinceCode(directProvince) {
		directProvince = ""
	}
	if language, single := s.singleConfiguredLanguage(); single {
		if directProvince != "" {
			s.writeLocationNumberPrompt(writer, request, language, directProvince)
			return
		}
		s.handleLocationCodeWithLanguageTwiML(writer, request, language, digit)
		return
	}
	option, ok := s.cfg.Prompts.Option("entry", digit)
	if !ok {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	switch option.Action {
	case "language":
		if !s.languageConfigured(option.Language) {
			s.writeEntryErrorTwiML(writer, request)
			return
		}
		locationMenu, _ := s.cfg.Prompts.Menu("location_code")
		lang := fallbackText(option.Language, s.cfg.IVR.DefaultLanguage)
		if directProvince != "" {
			s.writeLocationNumberPrompt(writer, request, lang, directProvince)
			return
		}
		body := twimlGather(twimlURL(request, "/ivr/v1/twiml", map[string]string{
			"state": "location_code",
			"lang":  lang,
		}), "", "#", locationCodeAutoSubmitTimeoutForMenu(locationMenu.Timeout), []string{
			promptURL(request, "location_code", "main", nil),
		}, []string{
			twimlTimeoutHangup(request, nil),
		})
		writeTwiML(writer, body)
	case "product":
		location, err := s.defaultFeedLocation()
		if err != nil {
			s.writeUnavailableTwiML(writer, request)
			return
		}
		if option.Language != "" {
			location.Language = option.Language
		}
		packages := splitCSV(option.Packages)
		s.writeProductTwiML(writer, request, s.mapProductLocation(location, packages, nil), packages, "")
	case "broadcast":
		location, err := s.defaultFeedLocation()
		if err != nil || !s.broadcastAvailable(location.FeedID) {
			s.writeEntryErrorTwiML(writer, request)
			return
		}
		packages := s.broadcastPackages(option)
		s.writeProductTwiML(writer, request, s.mapProductLocation(location, packages, nil), packages, "")
	case "operator":
		s.writeOperatorTwiML(writer, request)
	default:
		s.writeEntryErrorTwiML(writer, request)
	}
}

func (s *Service) handleLocationCodeTwiML(writer http.ResponseWriter, request *http.Request) {
	code := firstNonBlank(request.FormValue("Digits"), request.URL.Query().Get("code"))
	if strings.TrimSpace(code) == "" {
		s.writeLocationCodePrompt(writer, request, request.URL.Query().Get("lang"))
		return
	}
	s.handleLocationCodeWithLanguageTwiML(writer, request, request.URL.Query().Get("lang"), code)
}

func (s *Service) writeLocationCodePrompt(writer http.ResponseWriter, request *http.Request, language string) {
	locationMenu, _ := s.cfg.Prompts.Menu("location_code")
	body := twimlGather(twimlURL(request, "/ivr/v1/twiml", map[string]string{
		"state": "location_code",
		"lang":  language,
	}), "", "#", locationCodeAutoSubmitTimeoutForMenu(locationMenu.Timeout), []string{
		promptURL(request, "location_code", "main", nil),
	}, []string{
		twimlTimeoutHangup(request, nil),
	})
	writeTwiML(writer, body)
}

func (s *Service) handleLocationCodeWithLanguageTwiML(writer http.ResponseWriter, request *http.Request, language string, code string) {
	if code == "" {
		writeTwiML(writer, twimlTimeoutHangup(request, nil))
		return
	}
	code = strings.TrimSuffix(strings.TrimSpace(code), "#")
	if code == "*" {
		location, err := s.defaultFeedLocation()
		if err != nil {
			s.writeUnavailableTwiML(writer, request)
			return
		}
		location.Language = fallbackText(language, location.Language)
		s.writeProductTwiML(writer, request, location, []string{"geophysical_alert"}, "")
		return
	}
	if isProvinceDigit(code) {
		s.writeLocationNumberPrompt(writer, request, language, code)
		return
	}
	location, err := s.resolveLocation(code)
	if err != nil {
		body := twimlPlay(promptURL(request, "error", "invalid_code", nil)) +
			twimlRedirect(twimlURL(request, "/ivr/v1/twiml", nil))
		writeTwiML(writer, body)
		return
	}
	if lang := strings.TrimSpace(language); lang != "" {
		location.Language = lang
	}
	s.writeLocationMenu(writer, request, location)
}

func (s *Service) writeLocationNumberPrompt(writer http.ResponseWriter, request *http.Request, language string, province string) {
	s.writeLocationNumberPromptWithPlays(writer, request, language, province, nil)
}

func (s *Service) writeLocationNumberPromptWithPlays(writer http.ResponseWriter, request *http.Request, language string, province string, plays []string) {
	menu, _ := s.cfg.Prompts.Menu("location_number")
	plays = append(plays, promptURL(request, "location_number", "main", s.promptValues(map[string]string{"province": provinceDigitDisplayName(province)})))
	body := twimlGather(twimlURL(request, "/ivr/v1/twiml", map[string]string{
		"state":    "location_number",
		"lang":     language,
		"province": province,
	}), "", "#", locationCodeAutoSubmitTimeoutForMenu(menu.Timeout), plays, []string{
		twimlTimeoutHangup(request, nil),
	})
	writeTwiML(writer, body)
}

func (s *Service) handleLocationNumberTwiML(writer http.ResponseWriter, request *http.Request) {
	province := firstNonBlank(request.URL.Query().Get("province"), request.FormValue("province"))
	number := strings.TrimSuffix(strings.TrimSpace(firstNonBlank(request.FormValue("Digits"), request.URL.Query().Get("number"))), "#")
	if number == "" {
		writeTwiML(writer, twimlTimeoutHangup(request, nil))
		return
	}
	if number == "*" {
		if s.twilio != nil && s.cfg.IVR.Search.enabled() && s.searchIndex != nil {
			s.writeTwilioSearchConnect(writer, request, request.URL.Query().Get("lang"), province)
			return
		}
		body := twimlPlay(promptURL(request, "location_number", "search_unavailable", nil)) +
			twimlRedirect(twimlURL(request, "/ivr/v1/twiml", map[string]string{
				"state": "location_code",
				"lang":  request.URL.Query().Get("lang"),
			}))
		writeTwiML(writer, body)
		return
	}
	code, ok := s.resolver.helloWeatherCodeForProvinceNumber(province, number)
	if !ok {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	s.handleLocationCodeWithLanguageTwiML(writer, request, request.URL.Query().Get("lang"), code)
}

func (s *Service) writeLocationMenuTwiML(writer http.ResponseWriter, request *http.Request) {
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	s.writeLocationMenuWithAlertAuto(writer, request, location, false)
}

func (s *Service) writeLocationMenu(writer http.ResponseWriter, request *http.Request, location ResolvedLocation) {
	s.writeLocationMenuWithAlertAuto(writer, request, location, false)
}

func (s *Service) writeLocationMenuWithAlertAuto(writer http.ResponseWriter, request *http.Request, location ResolvedLocation, autoAlertMenu bool) {
	request = s.twilioLocationRequest(request, location)
	menu, _ := s.cfg.Prompts.Menu("location_menu")
	alerts := s.activeIVRAlerts(request.Context(), location)
	params := locationTwiMLParams(location)
	params["state"] = "location_option"
	plays := []string{}
	if len(alerts) > 0 {
		audioParams := locationTwiMLParams(location)
		audioParams["kind"] = "location_menu"
		plays = append(plays, twimlURL(request, "/ivr/v1/alert_audio", audioParams))
	} else {
		lineKey := s.locationMenuMainLine(location)
		plays = append(plays, promptURL(request, "location_menu", lineKey, s.promptValues(map[string]string{"location": spokenLocationName(location), "feed_id": location.FeedID})))
	}
	body := twimlGather(twimlURL(request, "/ivr/v1/twiml", params), "1", "", menu.Timeout, plays, []string{
		twimlTimeoutHangup(request, s.promptValues(nil)),
	})
	writeTwiML(writer, body)
}

func twimlAlertAutoEnabled(request *http.Request) bool {
	return strings.TrimSpace(request.URL.Query().Get("alert_auto")) != "0"
}

func (s *Service) handleLocationOptionTwiML(writer http.ResponseWriter, request *http.Request) {
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	digit := strings.TrimSpace(request.FormValue("Digits"))
	if digit == "#" {
		s.writeEntryTwiML(writer, request)
		return
	}
	if digit == "*" {
		if len(s.activeIVRAlerts(request.Context(), location)) > 0 {
			s.writeAlertMenuTwiML(writer, request)
			return
		}
		s.writeLocationMenuWithAlertAuto(writer, request, location, false)
		return
	}
	option, ok := s.cfg.Prompts.Option("location_menu", digit)
	if !ok {
		s.writeLocationMenuWithAlertAuto(writer, request, location, false)
		return
	}
	switch option.Action {
	case "product":
		packages := splitCSV(option.Packages)
		if packagesInclude(packages, "current_conditions") && len(s.observationStationChoices(location)) > 1 {
			s.writeObservationChoiceTwiML(writer, request, location, packages)
			return
		}
		params := locationTwiMLParams(location)
		params["state"] = "location_menu"
		params["alert_auto"] = "0"
		s.writeProductTwiML(writer, request, s.mapProductLocation(location, packages, nil), packages, twimlURL(request, "/ivr/v1/twiml", params))
	case "menu":
		s.writeConfiguredMenu(writer, request, location, option.Next)
	case "broadcast":
		if !s.locationBroadcastAvailable(location) {
			s.writeLocationMenuWithAlertAuto(writer, request, location, false)
			return
		}
		params := locationTwiMLParams(location)
		params["state"] = "location_menu"
		params["alert_auto"] = "0"
		packages := s.broadcastPackages(option)
		s.writeProductTwiML(writer, request, s.mapProductLocation(location, packages, nil), packages, twimlURL(request, "/ivr/v1/twiml", params))
	default:
		s.writeLocationMenuWithAlertAuto(writer, request, location, false)
	}
}

func (s *Service) writeAlertMenuTwiML(writer http.ResponseWriter, request *http.Request) {
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	s.writeAlertMenu(writer, request, location)
}

func (s *Service) writeAlertMenu(writer http.ResponseWriter, request *http.Request, location ResolvedLocation) {
	alerts := s.activeIVRAlerts(request.Context(), location)
	if len(alerts) == 0 {
		s.writeLocationMenuWithAlertAuto(writer, request, location, false)
		return
	}
	params := locationTwiMLParams(location)
	params["state"] = "alert_option"
	audioParams := locationTwiMLParams(location)
	audioParams["kind"] = "menu"
	menu, _ := s.cfg.Prompts.Menu("location_menu")
	body := twimlGather(twimlURL(request, "/ivr/v1/twiml", params), "1", "", menu.Timeout, []string{
		twimlURL(request, "/ivr/v1/alert_audio", audioParams),
	}, []string{
		twimlTimeoutHangup(request, s.promptValues(nil)),
	})
	writeTwiML(writer, body)
}

func (s *Service) handleAlertOptionTwiML(writer http.ResponseWriter, request *http.Request) {
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	digit := strings.TrimSpace(request.FormValue("Digits"))
	if digit == "#" {
		s.writeLocationMenuWithAlertAuto(writer, request, location, false)
		return
	}
	alerts := s.activeIVRAlerts(request.Context(), location)
	if _, ok := ivrAlertByDigit(alerts, digit); !ok {
		s.writeAlertMenuTwiML(writer, request)
		return
	}
	audioParams := locationTwiMLParams(location)
	audioParams["index"] = digit
	afterParams := locationTwiMLParams(location)
	afterParams["state"] = "alert_readout_option"
	fallbackParams := locationTwiMLParams(location)
	fallbackParams["state"] = "alert_menu"
	menu, _ := s.cfg.Prompts.Menu("location_menu")
	body := twimlGather(twimlURL(request, "/ivr/v1/twiml", afterParams), "1", "", menu.Timeout, []string{
		twimlURL(request, "/ivr/v1/alert_audio", audioParams),
	}, []string{
		twimlRedirect(twimlURL(request, "/ivr/v1/twiml", fallbackParams)),
	})
	writeTwiML(writer, body)
}

func (s *Service) handleAlertReadoutOptionTwiML(writer http.ResponseWriter, request *http.Request) {
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	if strings.TrimSpace(request.FormValue("Digits")) == "#" {
		s.writeLocationMenuWithAlertAuto(writer, request, location, false)
		return
	}
	s.writeAlertMenuTwiML(writer, request)
}

func (s *Service) writeConfiguredMenuTwiML(writer http.ResponseWriter, request *http.Request) {
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	s.writeConfiguredMenu(writer, request, location, request.URL.Query().Get("menu"))
}

func (s *Service) writeConfiguredMenu(writer http.ResponseWriter, request *http.Request, location ResolvedLocation, menuID string) {
	menuID = strings.ToLower(strings.TrimSpace(menuID))
	menu, ok := s.cfg.Prompts.Menu(menuID)
	if !ok {
		s.writeLocationMenuWithAlertAuto(writer, request, location, false)
		return
	}
	params := locationTwiMLParams(location)
	params["state"] = "ivr_menu_option"
	params["menu"] = menuID
	body := twimlGather(twimlURL(request, "/ivr/v1/twiml", params), "1", "", menu.Timeout, []string{
		promptURL(request, menuID, "main", s.promptValues(map[string]string{"location": spokenLocationName(location), "feed_id": location.FeedID})),
	}, []string{
		twimlTimeoutHangup(request, s.promptValues(nil)),
	})
	writeTwiML(writer, body)
}

func (s *Service) handleConfiguredMenuOptionTwiML(writer http.ResponseWriter, request *http.Request) {
	location, err := s.locationFromRequest(request)
	if err != nil {
		s.writeEntryErrorTwiML(writer, request)
		return
	}
	menuID := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("menu")))
	digit := strings.TrimSpace(request.FormValue("Digits"))
	if digit == "#" {
		s.writeLocationMenuWithAlertAuto(writer, request, location, false)
		return
	}
	option, ok := s.cfg.Prompts.Option(menuID, digit)
	if !ok {
		s.writeConfiguredMenu(writer, request, location, menuID)
		return
	}
	switch option.Action {
	case "product":
		packages := splitCSV(option.Packages)
		if packagesInclude(packages, "current_conditions") && len(s.observationStationChoices(location)) > 1 {
			s.writeObservationChoiceTwiML(writer, request, location, packages)
			return
		}
		params := locationTwiMLParams(location)
		params["state"] = "ivr_menu"
		params["menu"] = menuID
		s.writeProductTwiML(writer, request, s.mapProductLocation(location, packages, nil), packages, twimlURL(request, "/ivr/v1/twiml", params))
	case "menu":
		s.writeConfiguredMenu(writer, request, location, option.Next)
	case "broadcast":
		if !s.locationBroadcastAvailable(location) {
			s.writeConfiguredMenu(writer, request, location, menuID)
			return
		}
		params := locationTwiMLParams(location)
		params["state"] = "ivr_menu"
		params["menu"] = menuID
		packages := s.broadcastPackages(option)
		s.writeProductTwiML(writer, request, s.mapProductLocation(location, packages, nil), packages, twimlURL(request, "/ivr/v1/twiml", params))
	case "operator":
		s.writeOperatorTwiML(writer, request)
	default:
		s.writeConfiguredMenu(writer, request, location, menuID)
	}
}

func locationTwiMLParams(location ResolvedLocation) map[string]string {
	params := map[string]string{
		"code":    location.Code,
		"feed_id": location.FeedID,
		"lang":    location.Language,
	}
	if strings.TrimSpace(location.Code) == "" {
		delete(params, "code")
	}
	return params
}

func (s *Service) writeProductTwiML(writer http.ResponseWriter, request *http.Request, location ResolvedLocation, packages []string, afterURL string) {
	queryLocation := location.Code
	packages = normalizePackages(packages, s.cfg.IVR.DefaultPackages)
	params := map[string]string{
		"code":               queryLocation,
		"feed_id":            location.FeedID,
		"lang":               location.Language,
		"packages":           strings.Join(packages, ","),
		"forecast_id":        location.Forecast,
		"station_id":         location.StationID,
		"air_quality_id":     location.AirQualityID,
		"climate_id":         location.ClimateID,
		"hydrometric_id":     location.HydrometricID,
		"marine_forecast_id": location.MarineForecastID,
	}
	if queryLocation == "" {
		delete(params, "code")
	}
	body := ""
	if _, ok := s.cache.Fresh(location, packages); !ok {
		body += twimlPlay(promptURL(request, "", "one_moment", nil))
	}
	body += twimlPlay(twimlURL(request, "/ivr/v1/audio", params))
	if afterURL != "" {
		body += twimlRedirect(afterURL)
	}
	writeTwiML(writer, body)
}

func (s *Service) broadcastPackages(option menuOption) []string {
	if packages := splitCSV(option.Packages); len(packages) > 0 {
		return packages
	}
	return append([]string(nil), s.cfg.IVR.BroadcastPackages...)
}

func (s *Service) broadcastAvailable(feedID string) bool {
	if s == nil || s.broadcast == nil || strings.TrimSpace(feedID) == "" {
		return false
	}
	return s.broadcast.HasRecent(feedID, 5*time.Second) || s.broadcast.HasHTTPSource()
}

func (s *Service) locationBroadcastAvailable(location ResolvedLocation) bool {
	return location.Covered && s.broadcastAvailable(location.FeedID)
}

func (s *Service) locationMenuMainLine(location ResolvedLocation) string {
	if s.locationBroadcastAvailable(location) {
		return "main"
	}
	if _, ok := s.cfg.Prompts.Line("location_menu", "main_no_broadcast"); ok {
		return "main_no_broadcast"
	}
	return "main"
}

func (s *Service) menuMainLine(menuID string, feedID string) string {
	if menuID == "entry" {
		if _, single := s.singleConfiguredLanguage(); single {
			if _, ok := s.cfg.Prompts.Line(menuID, "main_single_language"); ok {
				return "main_single_language"
			}
		}
	}
	if strings.TrimSpace(feedID) == "" {
		if location, err := s.defaultFeedLocation(); err == nil {
			feedID = location.FeedID
		}
	}
	if s.broadcastAvailable(feedID) {
		return "main"
	}
	if _, ok := s.cfg.Prompts.Line(menuID, "main_no_broadcast"); ok {
		return "main_no_broadcast"
	}
	return "main"
}

func (s *Service) promptValues(values map[string]string) map[string]string {
	feedID := ""
	if values != nil {
		feedID = values["feed_id"]
	}
	out := map[string]string{}
	for key, value := range values {
		if strings.TrimSpace(value) != "" {
			out[key] = value
		}
	}
	out["telephone_service_name"] = s.telephoneServiceName()
	out["radio_service_name"] = s.radioServiceName(feedID)
	out["language_options"] = s.languageOptionsPrompt()
	return out
}

func (s *Service) singleConfiguredLanguage() (string, bool) {
	languages := s.configuredLanguages()
	if len(languages) != 1 {
		return "", false
	}
	return languages[0], true
}

func (s *Service) configuredLanguages() []string {
	seen := map[string]struct{}{}
	out := []string{}
	add := func(language string) {
		language = strings.TrimSpace(language)
		if language == "" {
			return
		}
		key := strings.ToLower(language)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, language)
	}
	for _, feed := range s.cfg.Feeds {
		if !xmlBool(feed.EnabledRaw, true) {
			continue
		}
		for _, language := range feed.Languages.Langs {
			add(language.Code)
		}
	}
	if len(out) == 0 {
		add(s.cfg.IVR.DefaultLanguage)
	}
	return out
}

func (s *Service) languageConfigured(language string) bool {
	language = strings.TrimSpace(language)
	if language == "" {
		return false
	}
	for _, configured := range s.configuredLanguages() {
		if strings.EqualFold(configured, language) {
			return true
		}
	}
	return false
}

func (s *Service) configuredEntryLanguageOptions() []menuOption {
	menu, ok := s.cfg.Prompts.Menu("entry")
	if !ok {
		return nil
	}
	options := []menuOption{}
	seen := map[string]struct{}{}
	for _, option := range menu.Options {
		if option.Action != "language" || !s.languageConfigured(option.Language) {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(option.Language))
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		options = append(options, option)
	}
	return options
}

func (s *Service) languageOptionsPrompt() string {
	options := s.configuredEntryLanguageOptions()
	if len(options) == 0 {
		return "1 for service in " + languageDisplayName(s.cfg.IVR.DefaultLanguage)
	}
	parts := make([]string, 0, len(options))
	for _, option := range options {
		parts = append(parts, option.Digit+" for service in "+languageDisplayName(option.Language))
	}
	return strings.Join(parts, ", ")
}

func languageDisplayName(language string) string {
	switch strings.ToLower(strings.TrimSpace(language)) {
	case "en", "en-ca", "en-us", "en-gb":
		return "English"
	case "fr", "fr-ca", "fr-fr":
		return "French"
	case "es", "es-us", "es-mx":
		return "Spanish"
	default:
		return strings.TrimSpace(language)
	}
}

func locationCodeAutoSubmitTimeout() time.Duration {
	return time.Second
}

func locationCodeAutoSubmitTimeoutForMenu(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return locationCodeAutoSubmitTimeout()
	}
	if timeout < locationCodeAutoSubmitTimeout() {
		return timeout
	}
	return locationCodeAutoSubmitTimeout()
}

func spokenLocationName(location ResolvedLocation) string {
	name := strings.TrimSpace(location.Name)
	if name != "" {
		for _, id := range []string{location.Code, location.Forecast, location.StationID, location.FeedID} {
			if strings.EqualFold(name, strings.TrimSpace(id)) {
				name = ""
				break
			}
		}
		if name != "" && !looksLikeProviderID(name) {
			return name
		}
	}
	return "the selected area"
}

func looksLikeProviderID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if matched, _ := regexp.MatchString(`(?i)^[a-z]{2}-\d+$`, value); matched {
		return true
	}
	if matched, _ := regexp.MatchString(`^[A-Z]{1,4}\d{1,4}$`, value); matched {
		return true
	}
	return false
}

func (s *Service) telephoneServiceName() string {
	if s == nil {
		return "Haze Weather Telephone"
	}
	return fallbackText(displayText(s.cfg.Root.Operator.TelephoneName), "Haze Weather Telephone")
}

func (s *Service) extensionTelephoneServiceName(line extensionConfig) string {
	telephoneName := s.telephoneServiceName()
	extensionName := displayText(line.Name)
	if extensionName == "" {
		return telephoneName
	}
	if strings.EqualFold(line.NamePosition, "after") {
		return strings.TrimSpace(telephoneName + " " + extensionName)
	}
	return strings.TrimSpace(extensionName + " " + telephoneName)
}

func (s *Service) extensionCallerIDName(line extensionConfig) string {
	if configured := strings.TrimSpace(line.CallerIDName); configured != "" {
		return configured
	}
	telephoneName := "Haze Weather Telephone"
	if s != nil {
		telephoneName = fallbackText(writtenDisplayText(s.cfg.Root.Operator.TelephoneName), telephoneName)
	}
	extensionName := writtenDisplayText(line.Name)
	if extensionName == "" {
		return telephoneName
	}
	if strings.EqualFold(line.NamePosition, "after") {
		return strings.TrimSpace(telephoneName + " " + extensionName)
	}
	return strings.TrimSpace(extensionName + " " + telephoneName)
}

func (s *Service) extensionGreetingValues(line extensionConfig) map[string]string {
	return s.promptValues(map[string]string{
		"extension_telephone_service_name": s.extensionTelephoneServiceName(line),
	})
}

func (s *Service) radioServiceName(feedID string) string {
	if s == nil {
		return "Haze Weather Radio"
	}
	if name := displayText(s.cfg.Root.Operator.OnAirName); name != "" {
		return name
	}
	for _, feed := range s.cfg.Feeds {
		if strings.TrimSpace(feedID) != "" && !strings.EqualFold(feed.ID, feedID) {
			continue
		}
		transmitter := stationTransmitter(feed)
		if name := firstNonBlank(transmitter.Network.Pronunciation, transmitter.Network.Pronounciation, transmitter.Network.Name); name != "" {
			return name
		}
		if site := strings.TrimSpace(transmitter.SiteName); site != "" {
			return site
		}
	}
	return "Haze Weather Radio"
}

func (s *Service) servePromptAudio(writer http.ResponseWriter, request *http.Request, menuID string, lineKey string, values map[string]string, status int) {
	promptValues := s.promptValues(values)
	format := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("format")))
	if audio, ok := s.staticPromptAudio(menuID, lineKey, promptValues); ok {
		s.serveCachedAudio(writer, request, audio, format, status)
		return
	}
	audio, err := s.cache.GetPromptWithPolicy(request.Context(), menuID, lineKey, promptValues, s.staticPromptPolicy(), false)
	if err != nil {
		http.Error(writer, err.Error(), status)
		return
	}
	s.serveCachedAudio(writer, request, audio, format, status)
}

func (s *Service) serveTextPromptAudio(writer http.ResponseWriter, request *http.Request, lineKey string, text string, status int) {
	format := strings.ToLower(strings.TrimSpace(request.URL.Query().Get("format")))
	s.serveTextPromptAudioWithFormat(writer, request, lineKey, text, format, status)
}

func (s *Service) serveTextPromptAudioWithFormat(writer http.ResponseWriter, request *http.Request, lineKey string, text string, format string, status int) {
	audio, err := s.textPromptAudio(request.Context(), lineKey, text)
	if err != nil {
		log.Printf("IVR dynamic prompt %s failed: %v", lineKey, err)
		http.Error(writer, err.Error(), http.StatusBadGateway)
		return
	}
	s.serveCachedAudio(writer, request, audio, format, status)
}

func (s *Service) serveCachedAudio(writer http.ResponseWriter, request *http.Request, audio CachedAudio, format string, status int) {
	if format == "pcmu" {
		writer.Header().Set("Content-Type", "audio/basic")
		if status != http.StatusOK {
			writer.WriteHeader(status)
		}
		http.ServeFile(writer, request, audio.PCMUPath)
		return
	}
	writer.Header().Set("Content-Type", "audio/wav")
	if status != http.StatusOK {
		writer.WriteHeader(status)
	}
	http.ServeFile(writer, request, audio.WAVPath)
}

func (s *Service) staticPromptAudio(menuID string, lineKey string, values map[string]string) (CachedAudio, bool) {
	manifest, ok := s.currentStaticPromptManifest()
	if !ok {
		return CachedAudio{}, false
	}
	rendered := s.cfg.Prompts.MenuLine(menuID, lineKey, values)
	if strings.TrimSpace(rendered) == "" {
		return CachedAudio{}, false
	}
	prefix := safeID(fallbackText(menuID, "default")) + "__" + safeID(lineKey)
	file, ok := manifest.Files[prefix]
	if !ok || strings.TrimSpace(file.Text) != strings.TrimSpace(rendered) {
		return CachedAudio{}, false
	}
	if strings.TrimSpace(file.WAV) == "" || strings.TrimSpace(file.PCMU) == "" {
		return CachedAudio{}, false
	}
	audio := CachedAudio{
		Key:         manifest.Fingerprint + ":" + prefix,
		Title:       fallbackText(menuID, "default") + "/" + lineKey,
		Text:        file.Text,
		WAVPath:     filepath.Clean(filepath.FromSlash(file.WAV)),
		PCMUPath:    filepath.Clean(filepath.FromSlash(file.PCMU)),
		G722Path:    filepath.Clean(filepath.FromSlash(file.G722)),
		GeneratedAt: manifest.GeneratedAt,
		ExpiresAt:   time.Now().UTC().Add(24 * time.Hour),
	}
	for _, path := range []string{audio.WAVPath, audio.PCMUPath, audio.G722Path} {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			return CachedAudio{}, false
		}
	}
	return audio, true
}

func (s *Service) currentStaticPromptManifest() (staticPromptManifest, bool) {
	targetDir := filepath.Join(s.cfg.BaseDir, "audio", "ivr")
	manifestPath := filepath.Join(targetDir, "manifest.json")
	raw, err := os.ReadFile(filepath.Clean(manifestPath))
	if err != nil {
		return staticPromptManifest{}, false
	}
	var manifest staticPromptManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return staticPromptManifest{}, false
	}
	if manifest.Version != staticPromptManifestVersion || len(manifest.Files) == 0 {
		return staticPromptManifest{}, false
	}
	fingerprint, err := s.staticPromptFingerprint(s.cfg.Prompts.StaticPromptLines(), s.staticPromptPolicy())
	if err != nil || manifest.Fingerprint != fingerprint {
		return staticPromptManifest{}, false
	}
	return manifest, true
}

func (s *Service) writeOperatorTwiML(writer http.ResponseWriter, request *http.Request) {
	menu, _ := s.cfg.Prompts.Menu("operator")
	if strings.TrimSpace(menu.TransferURI) != "" {
		writeTwiML(writer, "<Dial>"+html.EscapeString(strings.TrimSpace(menu.TransferURI))+"</Dial>")
		return
	}
	writeTwiML(writer, twimlPlay(promptURL(request, "operator", "main", s.promptValues(nil)))+twimlRedirect(twimlURL(request, "/ivr/v1/twiml", nil)))
}

func (s *Service) writeEntryErrorTwiML(writer http.ResponseWriter, request *http.Request) {
	writeTwiML(writer, twimlPlay(promptURL(request, "error", "invalid_code", s.promptValues(nil)))+twimlRedirect(twimlURL(request, "/ivr/v1/twiml", nil)))
}

func (s *Service) writeUnavailableTwiML(writer http.ResponseWriter, request *http.Request) {
	writeTwiML(writer, twimlPlay(promptURL(request, "error", "unavailable", s.promptValues(nil)))+twimlRedirect(twimlURL(request, "/ivr/v1/twiml", nil)))
}

func (s *Service) handleMetrics(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, map[string]any{
		"lookups":              s.metrics.Lookups.Load(),
		"cache_requests":       s.metrics.CacheRequests.Load(),
		"cache_errors":         s.metrics.CacheErrors.Load(),
		"sip_messages":         s.metrics.SIPMessages.Load(),
		"http_requests":        s.metrics.HTTPRequests.Load(),
		"search_voice":         s.metrics.SearchVoice.Load(),
		"search_t9":            s.metrics.SearchT9.Load(),
		"search_multitap":      s.metrics.SearchMultitap.Load(),
		"search_accepted":      s.metrics.SearchAccepted.Load(),
		"search_confirmations": s.metrics.SearchConfirmations.Load(),
		"search_ambiguities":   s.metrics.SearchAmbiguities.Load(),
		"search_no_match":      s.metrics.SearchNoMatch.Load(),
		"asr_failures":         s.metrics.ASRFailures.Load(),
		"asr_busy":             s.metrics.ASRBusy.Load(),
		"asr_timeouts":         s.metrics.ASRTimeouts.Load(),
		"asr_unavailable":      s.metrics.ASRUnavailable.Load(),
		"asr_latency_samples":  s.metrics.ASRLatencySamples.Load(),
		"asr_latency_ms_total": s.metrics.ASRLatencyMS.Load(),
	})
}

func (s *Service) productForCode(ctx context.Context, code string, packages []string, force bool) (CachedProduct, error) {
	location, err := s.resolveLocation(code)
	if err != nil {
		return CachedProduct{}, err
	}
	return s.productForLocation(ctx, location, packages, force)
}

func (s *Service) productForLocation(ctx context.Context, location ResolvedLocation, packages []string, force bool) (CachedProduct, error) {
	s.metrics.CacheRequests.Add(1)
	product, err := s.cache.Get(ctx, location, packages, force)
	if err != nil {
		s.metrics.CacheErrors.Add(1)
		return CachedProduct{}, err
	}
	return product, nil
}

func (s *Service) locationFromRequest(request *http.Request) (ResolvedLocation, error) {
	code := request.URL.Query().Get("code")
	if code == "" {
		code = request.FormValue("code")
	}
	var location ResolvedLocation
	var err error
	if strings.TrimSpace(code) != "" {
		location, err = s.resolveLocation(code)
	} else {
		location, err = s.defaultFeedLocationForID(firstNonBlank(request.URL.Query().Get("feed_id"), request.FormValue("feed_id")))
	}
	if err != nil {
		return ResolvedLocation{}, err
	}
	if lang := firstNonBlank(request.URL.Query().Get("lang"), request.FormValue("lang")); lang != "" {
		location.Language = lang
	}
	s.applyRequestedCapabilityTargets(request, &location)
	return location, nil
}

func (s *Service) applyRequestedCapabilityTargets(request *http.Request, location *ResolvedLocation) {
	if s == nil || s.capabilities == nil || request == nil || location == nil {
		return
	}
	value := func(key string) string {
		return firstNonBlank(request.URL.Query().Get(key), request.FormValue(key))
	}
	if target, ok := s.capabilities.Find(locationdb.CapabilityForecast, value("forecast_id")); ok {
		location.Forecast = target.ID
	}
	if target, ok := s.capabilities.Find(locationdb.CapabilityObservation, value("station_id")); ok {
		location.StationID = target.ID
		location.Latitude = floatText(target.Latitude)
		location.Longitude = floatText(target.Longitude)
	}
	if target, ok := s.capabilities.Find(locationdb.CapabilityAirQuality, value("air_quality_id")); ok {
		location.AirQualityID = target.ID
	}
	if target, ok := s.capabilities.Find(locationdb.CapabilityClimate, value("climate_id")); ok {
		location.ClimateID = target.ID
	}
	if target, ok := s.capabilities.Find(locationdb.CapabilityHydrometric, value("hydrometric_id")); ok {
		location.HydrometricID = target.ID
	}
	if target, ok := s.capabilities.Find(locationdb.CapabilityMarineForecast, value("marine_forecast_id")); ok {
		location.MarineForecastID = target.ID
	}
}

func (s *Service) resolveLocation(code string) (ResolvedLocation, error) {
	resolver := s.resolver
	if resolver == nil {
		resolver = NewResolver(s.cfg)
	}
	return resolver.Resolve(code)
}

func (s *Service) defaultFeedLocation() (ResolvedLocation, error) {
	return s.defaultFeedLocationForID("")
}

func (s *Service) defaultFeedLocationForID(feedID string) (ResolvedLocation, error) {
	resolver := s.resolver
	if resolver == nil {
		resolver = NewResolver(s.cfg)
	}
	for _, feed := range s.cfg.Feeds {
		if strings.TrimSpace(feedID) != "" && !strings.EqualFold(feed.ID, feedID) {
			continue
		}
		if !xmlBool(feed.EnabledRaw, true) {
			continue
		}
		return ResolvedLocation{
			Code:     "",
			Source:   "feed",
			Name:     fallbackText(feedDisplayName(feed), feed.ID),
			FeedID:   feed.ID,
			Covered:  true,
			Language: resolver.feedLanguage(feed.ID),
			Timezone: strings.TrimSpace(feed.Timezone),
		}, nil
	}
	if strings.TrimSpace(feedID) != "" {
		return ResolvedLocation{}, fmt.Errorf("feed %q is not configured or enabled", feedID)
	}
	return ResolvedLocation{}, fmt.Errorf("no enabled IVR feed is configured")
}

func (s *Service) generateStaticPrompts(ctx context.Context) {
	if s.cache == nil {
		return
	}
	targetDir := filepath.Join(s.cfg.BaseDir, "audio", "ivr")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		log.Printf("IVR static prompt directory failed: %v", err)
		return
	}
	lines := s.cfg.Prompts.StaticPromptLines()
	policy := s.staticPromptPolicy()
	policy.Priority = "low"
	fingerprint, err := s.staticPromptFingerprint(lines, policy)
	if err != nil {
		log.Printf("IVR static prompt fingerprint failed: %v", err)
		return
	}
	manifestPath := filepath.Join(targetDir, "manifest.json")
	if staticPromptManifestCurrent(manifestPath, fingerprint) {
		log.Printf("IVR static prompts are current for %s", filepath.Base(s.cfg.PromptsPath))
		return
	}
	manifest := staticPromptManifest{
		Version:     staticPromptManifestVersion,
		Fingerprint: fingerprint,
		Provider:    policy.Provider,
		ReaderID:    policy.ReaderID,
		VoiceID:     policy.VoiceID,
		Language:    policy.Language,
		GeneratedAt: time.Now().UTC(),
		Files:       map[string]staticPromptFile{},
	}
	for _, line := range lines {
		if ctx.Err() != nil {
			return
		}
		values := s.promptValues(line.Values)
		text := s.cfg.Prompts.MenuLine(line.MenuID, line.LineKey, values)
		audio, err := s.cache.GetPromptWithPolicy(ctx, line.MenuID, line.LineKey, values, policy, true)
		if err != nil {
			log.Printf("IVR static prompt %s/%s failed: %v", fallbackText(line.MenuID, "default"), line.LineKey, err)
			continue
		}
		prefix := safeID(fallbackText(line.MenuID, "default")) + "__" + safeID(line.LineKey)
		wavPath := filepath.Join(targetDir, prefix+".wav")
		pcmuPath := filepath.Join(targetDir, prefix+".pcmu")
		g722Path := filepath.Join(targetDir, prefix+".g722")
		if err := copyFile(audio.WAVPath, wavPath); err != nil {
			log.Printf("IVR static WAV prompt %s failed: %v", prefix, err)
			continue
		}
		if err := copyFile(audio.PCMUPath, pcmuPath); err != nil {
			log.Printf("IVR static PCMU prompt %s failed: %v", prefix, err)
			continue
		}
		if strings.TrimSpace(audio.G722Path) != "" {
			if err := copyFile(audio.G722Path, g722Path); err != nil {
				log.Printf("IVR static G.722 prompt %s failed: %v", prefix, err)
				continue
			}
		} else {
			g722Path = ""
		}
		manifest.Files[prefix] = staticPromptFile{
			Text: text,
			WAV:  filepath.ToSlash(wavPath),
			PCMU: filepath.ToSlash(pcmuPath),
			G722: filepath.ToSlash(g722Path),
		}
	}
	if len(manifest.Files) == 0 {
		log.Printf("IVR static prompt generation produced no files")
		return
	}
	if err := writeStaticPromptManifest(manifestPath, manifest); err != nil {
		log.Printf("IVR static prompt manifest failed: %v", err)
		return
	}
	log.Printf("IVR static prompts regenerated with reader %s (%d clips)", fallbackText(policy.ReaderID, "default"), len(manifest.Files))
}

func (s *Service) staticPromptPolicy() TTSProfile {
	policy := s.cfg.Prompts.TTSForMenu("")
	if strings.TrimSpace(policy.ReaderID) == "" {
		policy.ReaderID = fallbackText(s.cfg.IVR.DefaultReaderID, "00")
	}
	if strings.TrimSpace(policy.Language) == "" {
		policy.Language = fallbackText(s.cfg.IVR.DefaultLanguage, "en-US")
	}
	if policy.Volume <= 0 {
		policy.Volume = 100
	}
	if policy.CacheTTL <= 0 {
		policy.CacheTTL = 24 * time.Hour
	}
	return policy
}

func (s *Service) staticPromptFingerprint(lines []staticPromptLine, policy TTSProfile) (string, error) {
	hash := sha256.New()
	hash.Write([]byte(fmt.Sprintf("v%d\n", staticPromptManifestVersion)))
	hash.Write([]byte("policy\n" + ttsPolicyFingerprint(policy) + "\n"))
	hash.Write([]byte("reader\n" + s.cfg.ttsReaderFingerprint(policy.ReaderID, policy.Language, "") + "\n"))
	if strings.TrimSpace(s.cfg.PromptsPath) != "" {
		raw, err := os.ReadFile(filepath.Clean(s.cfg.PromptsPath))
		if err != nil {
			return "", err
		}
		hash.Write([]byte("ivr_xml\n"))
		hash.Write(raw)
		hash.Write([]byte("\n"))
	}
	entries := make([]string, 0, len(lines))
	for _, line := range lines {
		values := s.promptValues(line.Values)
		text := s.cfg.Prompts.MenuLine(line.MenuID, line.LineKey, values)
		entries = append(entries, strings.Join([]string{line.MenuID, line.LineKey, text}, "\x00"))
	}
	sort.Strings(entries)
	for _, entry := range entries {
		hash.Write([]byte(entry))
		hash.Write([]byte("\n"))
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func staticPromptManifestCurrent(path string, fingerprint string) bool {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return false
	}
	var manifest staticPromptManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return false
	}
	if manifest.Version != staticPromptManifestVersion || manifest.Fingerprint != fingerprint || len(manifest.Files) == 0 {
		return false
	}
	for _, file := range manifest.Files {
		for _, path := range []string{file.WAV, file.PCMU, file.G722} {
			if strings.TrimSpace(path) == "" {
				continue
			}
			if _, err := os.Stat(filepath.Clean(filepath.FromSlash(path))); err != nil {
				return false
			}
		}
	}
	return true
}

func writeStaticPromptManifest(path string, manifest staticPromptManifest) error {
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Clean(path), raw, 0o644)
}

func copyFile(source string, target string) error {
	raw, err := os.ReadFile(filepath.Clean(source))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Clean(target), raw, 0o644)
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}

func packagesFromRequest(request *http.Request) []string {
	raw := request.URL.Query().Get("packages")
	if raw == "" {
		raw = request.FormValue("packages")
	}
	return splitCSV(raw)
}

func splitCSV(raw string) []string {
	fields := strings.Split(raw, ",")
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func promptValuesFromRequest(request *http.Request) map[string]string {
	values := map[string]string{}
	for _, key := range []string{"location", "code", "feed_id", "lang"} {
		if value := firstNonBlank(request.URL.Query().Get(key), request.FormValue(key)); value != "" {
			values[key] = value
		}
	}
	return values
}

func feedDisplayName(feed feedXML) string {
	transmitter := stationTransmitter(feed)
	return firstNonBlank(transmitter.SiteName, transmitter.Callsign, feed.ID)
}

func stationTransmitter(feed feedXML) transmitterXML {
	transmitters := transmitterList(feed)
	for _, transmitter := range transmitters {
		if transmitter.isRelationship("replaces") && strings.TrimSpace(transmitter.Callsign) != "" {
			return transmitter
		}
	}
	for _, transmitter := range transmitters {
		if transmitter.isRelationship("replaces") {
			return transmitter
		}
	}
	for _, transmitter := range transmitters {
		if transmitter.isRelationship("primary") {
			return transmitter
		}
	}
	if len(transmitters) > 0 {
		return transmitters[0]
	}
	return transmitterXML{}
}

func transmitterList(feed feedXML) []transmitterXML {
	out := make([]transmitterXML, 0, len(feed.Transmitter.Transmitters))
	for _, transmitter := range feed.Transmitter.Transmitters {
		if transmitter.empty() {
			continue
		}
		out = append(out, transmitter)
	}
	return out
}

func (t transmitterXML) empty() bool {
	return strings.TrimSpace(t.SiteName) == "" &&
		strings.TrimSpace(t.Callsign) == "" &&
		strings.TrimSpace(t.Relationship) == "" &&
		strings.TrimSpace(t.HostName) == "" &&
		strings.TrimSpace(t.FrequencyMHz.Value) == ""
}

func (t transmitterXML) isRelationship(relationship string) bool {
	current := strings.ToLower(strings.TrimSpace(t.Relationship))
	if current == "" {
		current = "unknown"
	}
	if current == "secondary/repeater" {
		current = "secondary"
	}
	wanted := strings.ToLower(strings.TrimSpace(relationship))
	return current == wanted || (wanted == "repeater" && current == "secondary")
}

func promptURL(request *http.Request, menuID string, lineKey string, values map[string]string) string {
	params := map[string]string{
		"line": lineKey,
	}
	if menuID != "" {
		params["menu"] = menuID
	}
	for key, value := range values {
		if strings.TrimSpace(value) != "" {
			params[key] = value
		}
	}
	return twimlURL(request, "/ivr/v1/prompt", params)
}

func twimlURL(request *http.Request, path string, params map[string]string) string {
	values := url.Values{}
	for key, value := range params {
		if strings.TrimSpace(value) != "" {
			values.Set(key, value)
		}
	}
	mediaToken := strings.TrimSpace(request.URL.Query().Get("twilio_token"))
	configuredBase := ""
	if requestContext, ok := request.Context().Value(twilioRequestContextKey{}).(twilioRequestContext); ok && requestContext.MediaToken != "" {
		mediaToken = requestContext.MediaToken
		configuredBase = strings.TrimRight(requestContext.BaseURL, "/")
	}
	if mediaToken != "" {
		values.Set("twilio_token", mediaToken)
		switch path {
		case "/ivr/v1/prompt":
			path = "/ivr/v1/twilio/prompt"
		case "/ivr/v1/audio":
			path = "/ivr/v1/twilio/audio"
		case "/ivr/v1/alert_audio":
			path = "/ivr/v1/twilio/alert_audio"
		}
	}
	base := externalBaseURL(request)
	if configuredBase != "" {
		base = configuredBase
	}
	base += path
	if encoded := values.Encode(); encoded != "" {
		base += "?" + encoded
	}
	return base
}

func externalBaseURL(request *http.Request) string {
	scheme := firstNonBlank(request.Header.Get("X-Forwarded-Proto"), request.URL.Scheme)
	if scheme == "" {
		scheme = "http"
		if request.TLS != nil {
			scheme = "https"
		}
	}
	host := firstNonBlank(request.Header.Get("X-Forwarded-Host"), request.Host)
	return strings.TrimRight(scheme+"://"+host, "/")
}

func twimlGather(action string, numDigits string, finishOnKey string, timeout time.Duration, plays []string, fallback []string) string {
	seconds := int(timeout.Seconds())
	if seconds <= 0 {
		seconds = 8
	}
	var builder strings.Builder
	builder.WriteString(`<Gather input="dtmf" action="`)
	builder.WriteString(html.EscapeString(action))
	builder.WriteString(`" timeout="`)
	builder.WriteString(fmt.Sprint(seconds))
	builder.WriteString(`"`)
	if numDigits != "" {
		builder.WriteString(` numDigits="`)
		builder.WriteString(html.EscapeString(numDigits))
		builder.WriteString(`"`)
	}
	if finishOnKey != "" {
		builder.WriteString(` finishOnKey="`)
		builder.WriteString(html.EscapeString(finishOnKey))
		builder.WriteString(`"`)
	}
	builder.WriteString(`>`)
	for _, play := range plays {
		builder.WriteString(twimlPlay(play))
	}
	builder.WriteString(`</Gather>`)
	for _, item := range fallback {
		builder.WriteString(item)
	}
	return builder.String()
}

func twimlPlay(url string) string {
	if strings.TrimSpace(url) == "" {
		return ""
	}
	return "<Play>" + html.EscapeString(strings.TrimSpace(url)) + "</Play>"
}

func twimlRedirect(url string) string {
	if strings.TrimSpace(url) == "" {
		return ""
	}
	return "<Redirect>" + html.EscapeString(strings.TrimSpace(url)) + "</Redirect>"
}

func twimlHangup() string {
	return "<Hangup/>"
}

func twimlTimeoutHangup(request *http.Request, values map[string]string) string {
	return twimlPlay(promptURL(request, "error", "timeout", values)) + twimlHangup()
}

func writeTwiML(writer http.ResponseWriter, body string) {
	writer.Header().Set("Content-Type", "text/xml; charset=utf-8")
	_, _ = writer.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Response>` + body + `</Response>`))
}

func sleepOrDone(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
