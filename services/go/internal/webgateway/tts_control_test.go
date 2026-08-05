package webgateway

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTTSPayloadRoundTripReadersXML(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, `services:
  go:
    tts:
      readers: managed/configs/readers.xml
`)

	payload, err := saveTTSPayload(configPath, map[string]any{
		"readers": []any{
			map[string]any{
				"id":              "00",
				"provider":        "sapi",
				"gender":          "female",
				"language":        "en_CA",
				"voice_id":        "Microsoft Linda",
				"backup_provider": "piper",
				"backup_voice_id": "en_US-lessac-low",
			},
			map[string]any{
				"id":       "01",
				"provider": "piper",
				"gender":   "male",
				"language": "en-US",
				"voice_id": "en_US-hfc_male-medium",
			},
		},
	})
	if err != nil {
		t.Fatalf("saveTTSPayload: %v", err)
	}
	if payload["configured"] != "managed/configs/readers.xml" {
		t.Fatalf("configured path = %#v", payload["configured"])
	}

	readers, ok := payload["readers"].([]map[string]any)
	if !ok || len(readers) != 2 {
		t.Fatalf("readers payload = %#v", payload["readers"])
	}
	if readers[0]["provider"] != "sapi5" || readers[0]["language"] != "en-ca" {
		t.Fatalf("reader normalization failed: %#v", readers[0])
	}
	if readers[0]["backup_provider"] != "piper" || readers[0]["backup_voice_id"] != "en_US-lessac-low" {
		t.Fatalf("reader backup normalization failed: %#v", readers[0])
	}

	raw, err := os.ReadFile(filepath.Join(dir, "managed", "configs", "readers.xml"))
	if err != nil {
		t.Fatalf("read readers.xml: %v", err)
	}
	text := string(raw)
	if !containsAll(text, "<Readers>", `provider="sapi5"`, "<voice_id>Microsoft Linda</voice_id>", `<backup provider="piper">`, "<voice_id>en_US-lessac-low</voice_id>") {
		t.Fatalf("unexpected XML:\n%s", text)
	}
}

func TestTTSSaveRejectsDuplicateReaders(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, `services:
  go:
    tts:
      readers: managed/configs/readers.xml
`)
	_, err := saveTTSPayload(configPath, map[string]any{
		"readers": []any{
			map[string]any{"id": "00", "provider": "piper"},
			map[string]any{"id": "00", "provider": "kokoro"},
		},
	})
	if err == nil {
		t.Fatal("expected duplicate reader error")
	}
}

func TestTTSPayloadRoundTripsBackupReaderTargets(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, `services:
  go:
    tts:
      readers: managed/configs/readers.xml
`)

	payload, err := saveTTSPayload(configPath, map[string]any{
		"readers": []any{
			map[string]any{"id": "00", "provider": "speakyapi", "voice_id": "RadioMET Tom"},
			map[string]any{"id": "04", "provider": "speakyapi", "voice_id": "RadioMET Paul"},
			map[string]any{
				"id":                "000",
				"provider":          "piper",
				"voice_id":          "en_US-hfc_male-medium",
				"backup_reader_ids": []any{"00", "04", "00"},
			},
		},
	})
	if err != nil {
		t.Fatalf("saveTTSPayload: %v", err)
	}
	readers := payload["readers"].([]map[string]any)
	var backupReader map[string]any
	for _, reader := range readers {
		if reader["id"] == "000" {
			backupReader = reader
			break
		}
	}
	backupIDs, ok := backupReader["backup_reader_ids"].([]string)
	if !ok || len(backupIDs) != 2 || backupIDs[0] != "00" || backupIDs[1] != "04" {
		t.Fatalf("backup reader IDs = %#v", backupReader["backup_reader_ids"])
	}

	raw, err := os.ReadFile(filepath.Join(dir, "managed", "configs", "readers.xml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !containsAll(text, `<reader id="000" provider="piper">`, "<backup>", "<reader>00</reader>", "<reader>04</reader>") {
		t.Fatalf("unexpected XML:\n%s", text)
	}
	if strings.Contains(text, `<backup provider="`) {
		t.Fatalf("reverse backup was rewritten as an inline provider:\n%s", text)
	}
}

func containsAll(text string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(text, needle) {
			return false
		}
	}
	return true
}
