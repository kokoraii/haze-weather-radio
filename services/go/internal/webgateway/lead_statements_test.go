package webgateway

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/meowraii/haze-weather-radio/services/go/internal/lead"
)

func TestLeadStatementCommandsPersistManagedXML(t *testing.T) {
	t.Setenv("HAZE_HOST_BRIDGE_ADDR", "")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, "version: test\n")
	mustWrite(t, filepath.Join(dir, "audio", "lead.wav"), "test audio")
	session := &wsSession{configPath: configPath}

	resultAny, err := session.handleCommand("lead_statements.save", map[string]any{
		"statements": []any{
			map[string]any{
				"enabled":  true,
				"name":     "Immediate alert lead",
				"lead_in":  "audio/lead.wav",
				"lead_out": "",
				"conditions": []any{
					map[string]any{
						"type":   "if",
						"key":    "layer:SOREM:1.0:Broadcast_Immediately",
						"equals": "Yes",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("save command: %v", err)
	}
	result, ok := resultAny.(map[string]any)
	if !ok || result["saved"] != true {
		t.Fatalf("save result = %#v", resultAny)
	}

	document, err := lead.Load(filepath.Join(dir, "managed", "configs", "lead.xml"))
	if err != nil {
		t.Fatalf("load persisted lead.xml: %v", err)
	}
	if len(document.Statements) != 1 || document.Statements[0].LeadIn != "audio/lead.wav" {
		t.Fatalf("persisted document = %#v", document)
	}

	loadedAny, err := session.handleCommand("lead_statements.get", map[string]any{})
	if err != nil {
		t.Fatalf("get command: %v", err)
	}
	loaded, ok := loadedAny.(map[string]any)
	if !ok || len(loaded["statements"].([]lead.Statement)) != 1 {
		t.Fatalf("get result = %#v", loadedAny)
	}
	files, ok := loaded["audio_files"].([]string)
	if !ok || len(files) != 1 || files[0] != "audio/lead.wav" {
		t.Fatalf("audio files = %#v", loaded["audio_files"])
	}
}

func TestLeadStatementsRejectUnknownFieldsAndUnsafeAudio(t *testing.T) {
	_, err := leadDocumentFromPayload(map[string]any{
		"statements": []any{map[string]any{
			"enabled":  true,
			"name":     "Unexpected field",
			"lead_in":  "audio/lead.wav",
			"surprise": true,
		}},
	})
	if err == nil {
		t.Fatal("expected unknown field rejection")
	}

	_, err = leadDocumentFromPayload(map[string]any{
		"statements": []any{map[string]any{
			"enabled": true,
			"name":    "Unsafe path",
			"lead_in": "../secrets.wav",
		}},
	})
	if err == nil {
		t.Fatal("expected unsafe audio path rejection")
	}
}

func TestLeadStatementAudioValidationRejectsOversizedFile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, "version: test\n")
	path := filepath.Join(dir, "audio", "large.wav")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(leadStatementAudioMaxBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	err = validateLeadStatementAudioFiles(configPath, lead.Document{Statements: []lead.Statement{{
		Enabled: true, Name: "Oversized", LeadIn: "audio/large.wav",
	}}})
	if err == nil {
		t.Fatal("expected oversized audio file rejection")
	}
}

func TestLeadStatementCommandsRemainAdminOnly(t *testing.T) {
	for _, command := range []string{"lead_statements.get", "lead_statements.save", "lead_statements.preview"} {
		if nonAdminCommandAllowed(command, map[string]any{}, Account{}) {
			t.Fatalf("%s must not be available to non-admin accounts", command)
		}
	}
}

func TestLeadStatementPreviewUsesBundledWAV(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, "version: test\n")
	source := filepath.Join("..", "..", "..", "..", "bundle", "audio", "NPAS_Preroll.wav")
	raw, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read bundled lead audio: %v", err)
	}
	mustWrite(t, filepath.Join(dir, "audio", "NPAS_Preroll.wav"), string(raw))
	session := &wsSession{configPath: configPath}
	result, err := session.previewLeadStatement(map[string]any{
		"statement": map[string]any{
			"enabled": true,
			"name":    "Bundled lead",
			"lead_in": "audio/NPAS_Preroll.wav",
		},
		"include_same": false,
	})
	if err != nil {
		t.Fatalf("preview bundled lead: %v", err)
	}
	if result["format"] != "wav" || result["audio_base64"] == "" || result["include_same"] != false {
		t.Fatalf("preview result = %#v", result)
	}
}
