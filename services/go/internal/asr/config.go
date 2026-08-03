package asr

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultWorkers               = 4
	defaultQueueSize             = 32
	defaultTimeout               = 15 * time.Second
	defaultMaxAudioBytes         = 600 * 1024
	defaultProvider              = "local_whisper"
	defaultModel                 = "base-q5_1"
	defaultModelPath             = "runtime/models/whisper/ggml-base-q5_1.bin"
	defaultModelSHA256           = "422f1ae452ade6f30a004d7e5c6a43195e4433bc370bf23fac9cc591f01a8898"
	defaultRuntimeExecutable     = "bin/whisper-server"
	defaultRuntimeThreads        = 4
	defaultRuntimeStartupTimeout = 60 * time.Second
	maxRuntimeThreads            = 16
)

// Options controls the managed ASR process.
type Options struct {
	ConfigPath string
	BridgeAddr string
}

type rootConfig struct {
	Services struct {
		Go struct {
			ASR Config `yaml:"asr"`
		} `yaml:"go"`
	} `yaml:"services"`
}

// Config contains bounded worker and private spool settings.
type Config struct {
	Enabled                  bool   `yaml:"enabled"`
	Provider                 string `yaml:"provider"`
	Model                    string `yaml:"model"`
	ModelPath                string `yaml:"model_path"`
	ModelSHA256              string `yaml:"model_sha256"`
	RuntimeExecutable        string `yaml:"runtime_executable"`
	RuntimeThreads           int    `yaml:"runtime_threads"`
	RuntimeUseGPU            bool   `yaml:"runtime_use_gpu"`
	RuntimeStartupTimeoutRaw string `yaml:"runtime_startup_timeout"`
	SpoolDir                 string `yaml:"spool_dir"`
	Workers                  int    `yaml:"workers"`
	QueueSize                int    `yaml:"queue_size"`
	TimeoutRaw               string `yaml:"timeout"`
	MaxAudioBytes            int64  `yaml:"max_audio_bytes"`

	Timeout               time.Duration `yaml:"-"`
	RuntimeStartupTimeout time.Duration `yaml:"-"`
}

func loadConfig(path string) (Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = "config.yaml"
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read configuration: %w", err)
	}
	var root rootConfig
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return Config{}, fmt.Errorf("parse configuration: %w", err)
	}
	cfg := root.Services.Go.ASR
	cfg.Provider = normalizeLocalProvider(cfg.Provider)
	if cfg.Provider == "" {
		return Config{}, fmt.Errorf("ASR provider must be local_whisper; hosted providers are not supported")
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = defaultModel
	}
	if !validModelLabel(cfg.Model) {
		return Config{}, fmt.Errorf("invalid ASR model label %q", cfg.Model)
	}
	configDir := filepath.Dir(filepath.Clean(path))
	if strings.TrimSpace(cfg.ModelPath) == "" {
		cfg.ModelPath = defaultModelPath
	}
	cfg.ModelPath = resolveConfigRelativePath(configDir, cfg.ModelPath)
	if strings.TrimSpace(cfg.ModelSHA256) == "" && cfg.Model == defaultModel {
		cfg.ModelSHA256 = defaultModelSHA256
	}
	cfg.ModelSHA256 = strings.ToLower(strings.TrimSpace(cfg.ModelSHA256))
	if cfg.ModelSHA256 != "" {
		decoded, decodeErr := hex.DecodeString(cfg.ModelSHA256)
		if decodeErr != nil || len(decoded) != 32 {
			return Config{}, fmt.Errorf("ASR model_sha256 must contain exactly 64 hexadecimal characters")
		}
	}
	if strings.TrimSpace(cfg.RuntimeExecutable) == "" {
		cfg.RuntimeExecutable = defaultRuntimeExecutable
	}
	if runtime.GOOS == "windows" && filepath.Ext(cfg.RuntimeExecutable) == "" {
		cfg.RuntimeExecutable += ".exe"
	}
	cfg.RuntimeExecutable = resolveConfigRelativePath(configDir, cfg.RuntimeExecutable)
	if cfg.RuntimeThreads <= 0 {
		cfg.RuntimeThreads = defaultRuntimeThreads
	}
	if cfg.RuntimeThreads > maxRuntimeThreads {
		return Config{}, fmt.Errorf("local Whisper is limited to %d inference threads", maxRuntimeThreads)
	}
	cfg.RuntimeStartupTimeout = defaultRuntimeStartupTimeout
	if value := strings.TrimSpace(cfg.RuntimeStartupTimeoutRaw); value != "" {
		parsed, parseErr := time.ParseDuration(value)
		if parseErr != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("invalid local Whisper startup timeout %q", value)
		}
		cfg.RuntimeStartupTimeout = parsed
	}
	if cfg.RuntimeStartupTimeout > 2*time.Minute {
		cfg.RuntimeStartupTimeout = 2 * time.Minute
	}
	if strings.TrimSpace(cfg.SpoolDir) == "" {
		cfg.SpoolDir = "runtime/ivr/asr"
	}
	cfg.SpoolDir = resolveConfigRelativePath(configDir, cfg.SpoolDir)
	if err := validateASRSpoolPath(cfg.SpoolDir, configDir); err != nil {
		return Config{}, err
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.Workers > defaultWorkers || cfg.QueueSize > defaultQueueSize {
		return Config{}, fmt.Errorf("ASR is limited to %d workers and %d queued requests", defaultWorkers, defaultQueueSize)
	}
	cfg.Timeout = defaultTimeout
	if value := strings.TrimSpace(cfg.TimeoutRaw); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("invalid ASR timeout %q", value)
		}
		cfg.Timeout = parsed
	}
	if cfg.Timeout > defaultTimeout {
		cfg.Timeout = defaultTimeout
	}
	if cfg.MaxAudioBytes <= 0 {
		cfg.MaxAudioBytes = defaultMaxAudioBytes
	} else if cfg.MaxAudioBytes > 1024*1024 {
		cfg.MaxAudioBytes = 1024 * 1024
	}
	return cfg, nil
}

func normalizeLocalProvider(provider string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	provider = strings.NewReplacer("-", "_", ".", "_").Replace(provider)
	switch provider {
	case "", "local", "local_whisper", "whisper_cpp":
		return defaultProvider
	default:
		return ""
	}
}

func validModelLabel(model string) bool {
	model = strings.TrimSpace(model)
	if model == "" || len(model) > 64 {
		return false
	}
	for _, ch := range model {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || strings.ContainsRune("._-", ch) {
			continue
		}
		return false
	}
	return true
}

func resolveConfigRelativePath(configDir string, value string) string {
	value = filepath.Clean(strings.TrimSpace(value))
	if !filepath.IsAbs(value) {
		value = filepath.Join(configDir, value)
	}
	abs, err := filepath.Abs(value)
	if err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(value)
}

func validateASRSpoolPath(spoolDir string, configDir string) error {
	abs, err := filepath.Abs(filepath.Clean(spoolDir))
	if err != nil {
		return fmt.Errorf("resolve ASR spool: %w", err)
	}
	volumeRoot := filepath.Clean(filepath.VolumeName(abs) + string(filepath.Separator))
	configAbs, _ := filepath.Abs(filepath.Clean(configDir))
	tempAbs, _ := filepath.Abs(filepath.Clean(os.TempDir()))
	name := strings.ToLower(filepath.Base(abs))
	if abs == volumeRoot || abs == configAbs || abs == tempAbs || !strings.Contains(name, "asr") {
		return fmt.Errorf("ASR spool must be a dedicated directory whose name contains asr")
	}
	return nil
}

func boolValue(value any, fallback bool) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		if err == nil {
			return parsed
		}
	}
	return fallback
}
