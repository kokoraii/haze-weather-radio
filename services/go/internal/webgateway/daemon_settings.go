package webgateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const defaultDaemonSettingsFile = "runtime/state/daemonSettings.json"

func daemonSettingsPayload(configPath string) (map[string]any, error) {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return nil, err
	}
	baseDir := filepath.Dir(configPath)
	settingsPath := daemonSettingsPath(baseDir, root)
	savedOverlay := readJSONMap(settingsPath)
	active := daemonSettingsView(root)

	desiredSource := cloneMap(root)
	if len(savedOverlay) > 0 {
		mergeMap(desiredSource, savedOverlay)
	}
	desired := daemonSettingsView(desiredSource)
	runtime := readJSONMap(filepath.Join(baseDir, "runtime", "state", "goServiceRuntime.json"))
	if len(runtime) == 0 {
		runtime = map[string]any{"services": map[string]any{}}
	}
	payload := map[string]any{
		"active":            active,
		"effective":         desired,
		"saved_overlay":     map[string]any{},
		"has_saved_overlay": len(savedOverlay) > 0,
		"pending_restart":   !jsonEqual(active, desired),
		"settings_path":     settingsPath,
		"go_runtime":        runtime,
		"restart_required_for": []any{
			"service enable/disable changes",
			"web listener changes",
			"managed service address/source changes",
		},
	}
	if len(savedOverlay) > 0 {
		payload["saved_overlay"] = daemonSettingsView(savedOverlay)
	}
	return payload, nil
}

func writeDaemonSettings(configPath string, settings map[string]any) (map[string]any, error) {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return nil, err
	}
	active := daemonSettingsView(root)
	sanitized := daemonSettingsView(settings)
	if err := validateServicePlacements(sanitized); err != nil {
		return nil, err
	}
	overlay := settingsDelta(active, sanitized)
	settingsPath := daemonSettingsPath(filepath.Dir(configPath), root)
	if err := os.MkdirAll(filepath.Dir(settingsPath), 0o755); err != nil {
		return nil, err
	}
	if overlay == nil {
		if err := os.Remove(settingsPath); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		raw, err := json.MarshalIndent(overlay, "", "  ")
		if err != nil {
			return nil, err
		}
		tmp := settingsPath + ".tmp"
		if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
			return nil, err
		}
		if err := os.Rename(tmp, settingsPath); err != nil {
			return nil, err
		}
	}
	return daemonSettingsPayload(configPath)
}

func daemonSettingsPath(baseDir string, root map[string]any) string {
	configured := textAt(root, []string{"daemon_settings_file"}, defaultDaemonSettingsFile, 240)
	if filepath.IsAbs(configured) {
		return filepath.Clean(configured)
	}
	return filepath.Clean(filepath.Join(baseDir, configured))
}

func daemonSettingsView(source map[string]any) map[string]any {
	publicAccess := strings.ToLower(textAt(source, []string{"webpanel", "public", "feeds", "access"}, "disabled", 24))
	if publicAccess != "disabled" && publicAccess != "public" && publicAccess != "auth_required" {
		publicAccess = "disabled"
	}
	view := map[string]any{
		"services": map[string]any{
			"go": map[string]any{
				"enabled": boolAt(source, []string{"services", "go", "enabled"}, false),
				"web_gateway": map[string]any{
					"enabled": boolAt(source, []string{"services", "go", "web_gateway", "enabled"}, false),
					"addr":    textAt(source, []string{"services", "go", "web_gateway", "addr"}, "127.0.0.1:6444", 180),
				},
				"data_ingest": map[string]any{
					"enabled":  boolAt(source, []string{"services", "go", "data_ingest", "enabled"}, false),
					"interval": textAt(source, []string{"services", "go", "data_ingest", "interval"}, "45m", 24),
					"timeout":  textAt(source, []string{"services", "go", "data_ingest", "timeout"}, "20s", 24),
				},
				"tts": map[string]any{
					"enabled":  boolAt(source, []string{"services", "go", "tts", "enabled"}, false),
					"readers":  textAt(source, []string{"services", "go", "tts", "readers"}, "managed/configs/readers.xml", 200),
					"provider": textAt(source, []string{"services", "go", "tts", "provider"}, "auto", 32),
					"language": textAt(source, []string{"services", "go", "tts", "language"}, "en-CA", 16),
					"out_dir":  textAt(source, []string{"services", "go", "tts", "out_dir"}, "runtime/audio/tts", 200),
					"timeout":  textAt(source, []string{"services", "go", "tts", "timeout"}, "60s", 24),
					"piper_voices_dir": textAt(
						source,
						[]string{"services", "go", "tts", "piper_voices_dir"},
						"managed/voices/piper",
						200,
					),
					"speakyapi_url": textAt(source, []string{"services", "go", "tts", "speakyapi_url"}, "", 400),
				},
				"product_render": map[string]any{
					"enabled": boolAt(source, []string{"services", "go", "product_render", "enabled"}, false),
					"refresh": textAt(source, []string{"services", "go", "product_render", "refresh"}, "5m", 24),
				},
				"playlist": map[string]any{
					"enabled":            boolAt(source, []string{"services", "go", "playlist", "enabled"}, false),
					"tick":               textAt(source, []string{"services", "go", "playlist", "tick"}, "500ms", 24),
					"lookahead":          textAt(source, []string{"services", "go", "playlist", "lookahead"}, "2m", 24),
					"max_queued":         textAt(source, []string{"services", "go", "playlist", "max_queued"}, "3", 12),
					"out_dir":            textAt(source, []string{"services", "go", "playlist", "out_dir"}, "runtime/audio/playlist", 200),
					"fixed_tolerance_s":  textAt(source, []string{"services", "go", "playlist", "fixed_tolerance_s"}, "4", 12),
					"routine_estimate_s": textAt(source, []string{"services", "go", "playlist", "routine_estimate_s"}, "35", 12),
				},
				"ivr": map[string]any{
					"enabled": boolAt(source, []string{"services", "go", "ivr", "enabled"}, false),
					"mode":    textAt(source, []string{"services", "go", "ivr", "mode"}, "sip-edge", 32),
					"http": map[string]any{
						"enabled": boolAt(source, []string{"services", "go", "ivr", "http", "enabled"}, true),
						"addr":    textAt(source, []string{"services", "go", "ivr", "http", "addr"}, "127.0.0.1:8096", 120),
					},
					"sip": map[string]any{
						"enabled":     boolAt(source, []string{"services", "go", "ivr", "sip", "enabled"}, true),
						"listen":      textAt(source, []string{"services", "go", "ivr", "sip", "listen"}, "0.0.0.0:5060", 120),
						"public_host": textAt(source, []string{"services", "go", "ivr", "sip", "public_host"}, "", 180),
					},
					"cache": map[string]any{
						"dir":               textAt(source, []string{"services", "go", "ivr", "cache", "dir"}, "runtime/ivr/cache", 200),
						"ttl":               textAt(source, []string{"services", "go", "ivr", "cache", "ttl"}, "10m", 24),
						"phone_sample_rate": textAt(source, []string{"services", "go", "ivr", "cache", "phone_sample_rate"}, "8000", 12),
						"phone_codec":       textAt(source, []string{"services", "go", "ivr", "cache", "phone_codec"}, "pcmu", 24),
						"max_entries":       textAt(source, []string{"services", "go", "ivr", "cache", "max_entries"}, "5000", 12),
						"stampede_waiters":  textAt(source, []string{"services", "go", "ivr", "cache", "stampede_waiters"}, "64", 12),
					},
					"default_language":     textAt(source, []string{"services", "go", "ivr", "default_language"}, "en-CA", 16),
					"default_reader_id":    textAt(source, []string{"services", "go", "ivr", "default_reader_id"}, "", 120),
					"render_timeout":       textAt(source, []string{"services", "go", "ivr", "render_timeout"}, "25s", 24),
					"max_concurrent_calls": textAt(source, []string{"services", "go", "ivr", "max_concurrent_calls"}, "256", 12),
					"max_render_inflight":  textAt(source, []string{"services", "go", "ivr", "max_render_inflight"}, "8", 12),
				},
			},
			"rust": map[string]any{
				"cap_ingest": map[string]any{
					"enabled":              boolAt(source, []string{"services", "rust", "cap_ingest", "enabled"}, true),
					"shadow":               boolAt(source, []string{"services", "rust", "cap_ingest", "shadow"}, false),
					"source_id":            textAt(source, []string{"services", "rust", "cap_ingest", "source_id"}, "rust-cap", 180),
					"source":               textAt(source, []string{"services", "rust", "cap_ingest", "source"}, "naads", 32),
					"mode":                 textAt(source, []string{"services", "rust", "cap_ingest", "mode"}, "tcp", 32),
					"url":                  textAt(source, []string{"services", "rust", "cap_ingest", "url"}, "tcp://streaming1.naad-adna.pelmorex.com:8080", 400),
					"fallback_url":         textAt(source, []string{"services", "rust", "cap_ingest", "fallback_url"}, "tcp://streaming2.naad-adna.pelmorex.com:8080", 400),
					"archive_url":          textAt(source, []string{"services", "rust", "cap_ingest", "archive_url"}, "http://capcp1.naad-adna.pelmorex.com", 400),
					"fallback_archive_url": textAt(source, []string{"services", "rust", "cap_ingest", "fallback_archive_url"}, "http://capcp2.naad-adna.pelmorex.com", 400),
					"interval":             textAt(source, []string{"services", "rust", "cap_ingest", "interval"}, "5s", 24),
					"timeout":              textAt(source, []string{"services", "rust", "cap_ingest", "timeout"}, "15s", 24),
					"startup_seed":         boolAt(source, []string{"services", "rust", "cap_ingest", "startup_seed"}, true),
					"concurrency":          textAt(source, []string{"services", "rust", "cap_ingest", "concurrency"}, "8", 12),
				},
			},
			"daemon": map[string]any{
				"enabled": boolAtAny(source, [][]string{{"services", "daemon", "enabled"}, {"services", "rust", "enabled"}}, false),
				"scheduler": map[string]any{
					"enabled": boolAtAny(source, [][]string{{"services", "daemon", "scheduler", "enabled"}, {"services", "rust", "scheduler", "enabled"}}, false),
				},
				"alert_queue": map[string]any{
					"enabled":     boolAtAny(source, [][]string{{"services", "daemon", "alert_queue", "enabled"}, {"services", "rust", "alert_queue", "enabled"}}, true),
					"interval_ms": textAtAny(source, [][]string{{"services", "daemon", "alert_queue", "interval_ms"}, {"services", "rust", "alert_queue", "interval_ms"}}, "500", 12),
				},
				"playlist": map[string]any{
					"enabled":     boolAtAny(source, [][]string{{"services", "daemon", "playlist", "enabled"}, {"services", "rust", "playlist", "enabled"}}, false),
					"interval_ms": textAtAny(source, [][]string{{"services", "daemon", "playlist", "interval_ms"}, {"services", "rust", "playlist", "interval_ms"}}, "750", 12),
				},
			},
		},
		"webpanel": map[string]any{
			"public": map[string]any{
				"enabled": boolAt(source, []string{"webpanel", "public", "enabled"}, true),
				"host":    textAt(source, []string{"webpanel", "public", "host"}, "0.0.0.0", 120),
				"port":    textAt(source, []string{"webpanel", "public", "port"}, "6444", 12),
				"feeds": map[string]any{
					"access": publicAccess,
					"webrtc": map[string]any{
						"enabled": boolAt(source, []string{"webpanel", "public", "feeds", "webrtc", "enabled"}, true),
					},
				},
			},
			"admin": map[string]any{
				"enabled": boolAt(source, []string{"webpanel", "admin", "enabled"}, true),
				"host":    textAt(source, []string{"webpanel", "admin", "host"}, "0.0.0.0", 120),
				"port":    textAt(source, []string{"webpanel", "admin", "port"}, "6444", 12),
			},
			"receiver": map[string]any{
				"enabled": boolAt(source, []string{"webpanel", "receiver", "enabled"}, false),
			},
		},
		"cap": map[string]any{
			"cap_cp":  map[string]any{"enabled": boolAt(source, []string{"cap", "cap_cp", "enabled"}, true)},
			"nws_cap": map[string]any{"enabled": boolAt(source, []string{"cap", "nws_cap", "enabled"}, false)},
		},
		"wx_on_demand": map[string]any{
			"enabled": boolAt(source, []string{"wx_on_demand", "enabled"}, false),
		},
		"playout": map[string]any{
			"station_id_schedule": map[string]any{"enabled": boolAt(source, []string{"playout", "station_id_schedule", "enabled"}, true)},
			"date_time_schedule":  map[string]any{"enabled": boolAt(source, []string{"playout", "date_time_schedule", "enabled"}, true)},
			"chimes":              map[string]any{"enabled": boolAt(source, []string{"playout", "chimes", "enabled"}, true)},
		},
	}
	addPlacementView(view, source)
	return view
}

func loadYAMLMap(path string) (map[string]any, error) {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var value map[string]any
	if err := yaml.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if value == nil {
		value = map[string]any{}
	}
	return value, nil
}

func readJSONMap(path string) map[string]any {
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return map[string]any{}
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return map[string]any{}
	}
	return value
}

func valueAt(source map[string]any, path []string) (any, bool) {
	var current any = source
	for _, part := range path {
		next, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = next[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func boolAt(source map[string]any, path []string, fallback bool) bool {
	value, ok := valueAt(source, path)
	if !ok {
		return fallback
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case int:
		return typed != 0
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on", "enabled":
			return true
		case "0", "false", "no", "off", "disabled":
			return false
		}
	}
	return fallback
}

func boolAtAny(source map[string]any, paths [][]string, fallback bool) bool {
	for _, path := range paths {
		if _, ok := valueAt(source, path); ok {
			return boolAt(source, path, fallback)
		}
	}
	return fallback
}

func textAt(source map[string]any, path []string, fallback string, maxLen int) string {
	value, ok := valueAt(source, path)
	if !ok || value == nil {
		return fallback
	}
	text := strings.TrimSpace(toString(value))
	if text == "" {
		text = fallback
	}
	if maxLen > 0 && len(text) > maxLen {
		return text[:maxLen]
	}
	return text
}

func textAtAny(source map[string]any, paths [][]string, fallback string, maxLen int) string {
	for _, path := range paths {
		if _, ok := valueAt(source, path); ok {
			return textAt(source, path, fallback, maxLen)
		}
	}
	return fallback
}

func toString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		raw, _ := json.Marshal(typed)
		return strings.Trim(string(raw), `"`)
	}
}

func cloneMap(value map[string]any) map[string]any {
	raw, _ := json.Marshal(value)
	var cloned map[string]any
	_ = json.Unmarshal(raw, &cloned)
	if cloned == nil {
		return map[string]any{}
	}
	return cloned
}

func mergeMap(target map[string]any, patch map[string]any) {
	for key, value := range patch {
		if childPatch, ok := value.(map[string]any); ok {
			if childTarget, ok := target[key].(map[string]any); ok {
				mergeMap(childTarget, childPatch)
				continue
			}
		}
		target[key] = value
	}
}

func settingsDelta(base any, desired any) any {
	baseMap, baseOK := base.(map[string]any)
	desiredMap, desiredOK := desired.(map[string]any)
	if baseOK && desiredOK {
		delta := map[string]any{}
		for key, desiredValue := range desiredMap {
			child := settingsDelta(baseMap[key], desiredValue)
			if child != nil {
				delta[key] = child
			}
		}
		if len(delta) == 0 {
			return nil
		}
		return delta
	}
	if jsonEqual(base, desired) {
		return nil
	}
	return desired
}

func jsonEqual(left any, right any) bool {
	leftRaw, _ := json.Marshal(left)
	rightRaw, _ := json.Marshal(right)
	return string(leftRaw) == string(rightRaw)
}
