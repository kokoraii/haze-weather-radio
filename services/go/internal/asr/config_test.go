package asr

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigAppliesBoundedDefaults(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	raw := []byte(`services:
  go:
    asr:
      enabled: true
      spool_dir: runtime/ivr/asr
      workers: 4
      queue_size: 32
      timeout: 30s
`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Workers != 4 || cfg.QueueSize != 32 || cfg.Timeout != 15*time.Second {
		t.Fatalf("bounded ASR config = %#v", cfg)
	}
	if cfg.Provider != defaultProvider || cfg.Model != defaultModel || cfg.ModelSHA256 != defaultModelSHA256 {
		t.Fatalf("local Whisper defaults = %#v", cfg)
	}
	if cfg.RuntimeThreads != defaultRuntimeThreads || cfg.RuntimeStartupTimeout != defaultRuntimeStartupTimeout {
		t.Fatalf("local Whisper runtime defaults = %#v", cfg)
	}
	if !filepath.IsAbs(cfg.ModelPath) || !filepath.IsAbs(cfg.RuntimeExecutable) {
		t.Fatalf("local Whisper paths were not resolved: model=%q runtime=%q", cfg.ModelPath, cfg.RuntimeExecutable)
	}
	if filepath.Base(cfg.SpoolDir) != "asr" || !filepath.IsAbs(cfg.SpoolDir) {
		t.Fatalf("private spool path = %q", cfg.SpoolDir)
	}
}

func TestLoadConfigRejectsBroadSpoolAndExcessConcurrency(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		"services:\n  go:\n    asr:\n      enabled: true\n      spool_dir: .\n",
		"services:\n  go:\n    asr:\n      enabled: true\n      workers: 5\n      queue_size: 32\n",
		"services:\n  go:\n    asr:\n      enabled: true\n      provider: openai\n",
		"services:\n  go:\n    asr:\n      enabled: true\n      runtime_threads: 17\n",
		"services:\n  go:\n    asr:\n      enabled: true\n      model_sha256: xyz\n",
		"services:\n  go:\n    asr:\n      enabled: true\n      model: '../small'\n",
	} {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(path); err == nil {
			t.Fatalf("unsafe ASR config was accepted: %s", raw)
		}
	}
}
