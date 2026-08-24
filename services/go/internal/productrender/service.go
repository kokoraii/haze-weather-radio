package productrender

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
)

var errSystemShutdown = errors.New("system shutdown requested")

func Run(ctx context.Context, options Options) error {
	if strings.TrimSpace(options.ConfigPath) == "" {
		options.ConfigPath = "config.yaml"
	}
	if options.Refresh <= 0 {
		options.Refresh = 5 * time.Minute
	}
	loadDotEnv(filepath.Join(filepath.Dir(filepath.Clean(options.ConfigPath)), ".env"))
	loadDotEnv(".env")

	var store datastore.Store
	defer func() {
		if store != nil {
			store.Close()
		}
	}()
	for {
		cfg, err := loadConfig(options.ConfigPath)
		if err != nil {
			return err
		}
		if store == nil {
			store, err = openStore(ctx, cfg)
			if err != nil {
				return err
			}
		}
		cfg.Store = store
		bridge, err := connectBridge(ctx, options.BridgeAddr)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("product render waiting for event bridge: %v", err)
			sleepOrDone(ctx, time.Second)
			continue
		}
		cfg = hydrateRenderFeedLocations(ctx, cfg, options.BridgeAddr)
		service := &Service{
			cfg:        cfg,
			bridge:     bridge,
			options:    options,
			lastLoaded: time.Now(),
		}
		err = service.runConnected(ctx)
		_ = bridge.Close()
		if errors.Is(err, errSystemShutdown) {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("product render event bridge disconnected: %v", err)
		sleepOrDone(ctx, time.Second)
	}
}

type Service struct {
	cfg        loadedConfig
	bridge     *bridgeClient
	options    Options
	lastLoaded time.Time
	tonedMu    sync.Mutex
	tonedCAP   map[string]struct{}
}

func (s *Service) runConnected(ctx context.Context) error {
	s.runScheduledCleanup(time.Now().UTC())

	_ = s.bridge.Publish(map[string]any{
		"type":   "service.ready",
		"source": serviceID,
		"data": map[string]any{
			"service": serviceID,
			"feeds":   len(s.cfg.Feeds),
		},
	})

	cleanupTimer := s.newCleanupTimer(time.Now())
	defer stopTimer(cleanupTimer)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-timerChannel(cleanupTimer):
			s.runScheduledCleanup(now)
			cleanupTimer = s.resetCleanupTimer(cleanupTimer, time.Now())
		case event, ok := <-s.bridge.Events():
			if !ok {
				return fmt.Errorf("bridge event stream closed")
			}
			if stringAt(event, "type") == "system.shutdown" {
				return errSystemShutdown
			}
			s.handleEvent(event)
		}
	}
}

func (s *Service) handleEvent(event map[string]any) {
	switch stringAt(event, "type") {
	case "cap.alert.received":
		s.handleCAPAlert(event)
		return
	case "lead.config.updated":
		s.refreshConfigNow()
		return
	case "wx.on_demand.request":
		s.handleWxOnDemand(event)
		return
	case "product.render.request":
	default:
		return
	}
	data := mapAt(event, "data")
	request := renderRequest{
		RequestID: firstText(event, data, "request_id", "subject", "id"),
		FeedID:    firstText(event, data, "feed_id"),
		PackageID: firstText(event, data, "pkg_id", "package_id"),
		Language:  firstText(event, data, "language"),
		Force:     boolAt(data, "force", boolAt(event, "force", false)),
	}
	if request.RequestID == "" {
		request.RequestID = fmt.Sprintf("product-%d", time.Now().UnixNano())
	}
	if request.FeedID == "" || request.PackageID == "" {
		s.publishFailed(request, "feed_id and package_id are required")
		return
	}

	s.refreshConfigIfNeeded()
	product, err := newRenderer(s.cfg).Render(request)
	if err != nil {
		s.publishFailed(request, err.Error())
		return
	}
	_ = s.bridge.Publish(map[string]any{
		"type":    "product.rendered",
		"source":  serviceID,
		"subject": request.RequestID,
		"data": map[string]any{
			"request_id": request.RequestID,
			"feed_id":    request.FeedID,
			"package_id": request.PackageID,
			"product":    product,
		},
	})
}

func (s *Service) handleWxOnDemand(event map[string]any) {
	request := wxOnDemandRequestFromEvent(event)
	if request.RequestID == "" {
		request.RequestID = fmt.Sprintf("wx-%d", time.Now().UnixNano())
	}
	if len(request.Packages) == 0 {
		request.Packages = []string{"current_conditions", "forecast"}
	}

	s.refreshConfigIfNeeded()
	product, err := newRenderer(s.cfg).RenderWxOnDemand(request)
	if err != nil {
		s.publishWxFailed(request, err.Error())
		return
	}
	_ = s.bridge.Publish(map[string]any{
		"type":    "wx.on_demand.rendered",
		"source":  serviceID,
		"subject": request.RequestID,
		"data": map[string]any{
			"request_id":   request.RequestID,
			"canonical_id": request.CanonicalID,
			"feed_id":      request.FeedID,
			"code":         request.Code,
			"packages":     request.Packages,
			"product":      product,
		},
	})
}

func wxOnDemandRequestFromEvent(event map[string]any) wxOnDemandRequest {
	data := mapAt(event, "data")
	return wxOnDemandRequest{
		RequestID:        firstText(event, data, "request_id", "subject", "id"),
		CanonicalID:      firstText(event, data, "canonical_id"),
		FeedID:           firstText(event, data, "feed_id"),
		Code:             firstText(event, data, "code", "location_code"),
		Source:           firstNonBlank(stringAt(data, "source"), stringAt(data, "weather_source")),
		Identity:         firstText(event, data, "location_identity"),
		LocationName:     firstText(event, data, "location_name", "name"),
		Province:         firstText(event, data, "province"),
		Country:          firstText(event, data, "country"),
		ForecastID:       firstText(event, data, "forecast_id", "forecast"),
		StationID:        firstText(event, data, "station_id", "station"),
		AirQualityID:     firstText(event, data, "air_quality_id", "aqhi_id"),
		ClimateID:        firstText(event, data, "climate_id"),
		HydrometricID:    firstText(event, data, "hydrometric_id"),
		MarineForecastID: firstText(event, data, "marine_forecast_id", "marine_zone_id"),
		Latitude:         firstText(event, data, "latitude", "lat"),
		Longitude:        firstText(event, data, "longitude", "lon", "lng"),
		Timezone:         firstText(event, data, "timezone"),
		Language:         firstText(event, data, "language"),
		ReaderID:         firstText(event, data, "reader_id"),
		Packages:         stringList(firstValue(event, data, "packages", "package_ids", "pkg_ids")),
		Force:            boolAt(data, "force", boolAt(event, "force", false)),
		Telephone:        boolAt(data, "telephone", strings.EqualFold(firstText(event, data, "audience"), "telephone")),
	}
}

func (s *Service) refreshConfigIfNeeded() {
	if s.options.Refresh <= 0 || time.Since(s.lastLoaded) < s.options.Refresh {
		return
	}
	s.refreshConfigNow()
}

func (s *Service) refreshConfigNow() {
	cfg, err := loadConfig(s.options.ConfigPath)
	if err != nil {
		log.Printf("product render config refresh failed: %v", err)
		s.lastLoaded = time.Now()
		return
	}
	cfg.Store = s.cfg.Store
	cfg = inheritRenderFeedCanonicalIDs(cfg, s.cfg)
	s.cfg = cfg
	s.lastLoaded = time.Now()
}

func openStore(ctx context.Context, cfg loadedConfig) (datastore.Store, error) {
	store, err := datastore.Open(ctx, cfg.Root.Storage, cfg.BaseDir)
	if err != nil {
		return nil, err
	}
	log.Printf("datastore connected")
	return store, nil
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

func (s *Service) publishFailed(request renderRequest, detail string) {
	_ = s.bridge.Publish(map[string]any{
		"type":    "product.render.failed",
		"source":  serviceID,
		"subject": request.RequestID,
		"data": map[string]any{
			"request_id": request.RequestID,
			"feed_id":    request.FeedID,
			"package_id": request.PackageID,
			"error":      detail,
		},
	})
}

func (s *Service) publishWxFailed(request wxOnDemandRequest, detail string) {
	_ = s.bridge.Publish(map[string]any{
		"type":    "wx.on_demand.failed",
		"source":  serviceID,
		"subject": request.RequestID,
		"data": map[string]any{
			"request_id":   request.RequestID,
			"canonical_id": request.CanonicalID,
			"feed_id":      request.FeedID,
			"code":         request.Code,
			"error":        detail,
		},
	})
}

func stringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return cleanStringList(typed)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			out = append(out, fmt.Sprint(item))
		}
		return cleanStringList(out)
	case string:
		return cleanStringList(strings.Split(typed, ","))
	default:
		return nil
	}
}

func cleanStringList(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func sleepOrDone(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
