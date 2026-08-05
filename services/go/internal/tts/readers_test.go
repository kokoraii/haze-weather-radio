package tts

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadReadersAcceptsVoiceIDAndLegacyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "readers.xml")
	raw := `<Readers>
  <reader id="tom" provider="sapi5">
    <gender>male</gender>
    <language>en-US</language>
    <voice_id>Nuance Tom</voice_id>
  </reader>
  <reader id="ava" provider="sapi5">
    <gender>female</gender>
    <language>en_US</language>
    <path>Nuance Ava</path>
  </reader>
</Readers>`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	readers, err := LoadReaders(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(readers) != 2 {
		t.Fatalf("len = %d", len(readers))
	}
	if readers[0].Provider != "sapi5" || readers[0].VoiceID != "Nuance Tom" {
		t.Fatalf("unexpected reader: %+v", readers[0])
	}
	if readers[1].Provider != "sapi5" || readers[1].VoiceID != "Nuance Ava" || readers[1].Language != "en-us" {
		t.Fatalf("unexpected reader: %+v", readers[1])
	}
}

func TestLoadReadersAcceptsAutoReaderWithoutVoiceID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "readers.xml")
	raw := `<Readers>
  <reader id="00" provider="auto">
    <gender>male</gender>
    <language>en-US</language>
  </reader>
</Readers>`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	readers, err := LoadReaders(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(readers) != 1 {
		t.Fatalf("len = %d", len(readers))
	}
	if readers[0].Provider != "auto" || readers[0].VoiceID != "" {
		t.Fatalf("unexpected reader: %+v", readers[0])
	}
}

func TestLoadReadersParsesExplicitBackupVoice(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "readers.xml")
	raw := `<Readers>
  <reader id="00" provider="speakyapi">
    <gender>male</gender>
    <language>en-US</language>
    <voice_id>RadioMET Tom</voice_id>
    <backup provider="piper">
      <voice_id>en_US-hfc_male-medium</voice_id>
    </backup>
  </reader>
</Readers>`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	readers, err := LoadReaders(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(readers) != 1 || readers[0].Backup == nil {
		t.Fatalf("readers = %#v", readers)
	}
	if readers[0].Backup.Provider != "piper" || readers[0].Backup.VoiceID != "en_US-hfc_male-medium" {
		t.Fatalf("backup = %#v", readers[0].Backup)
	}
}

func TestLoadReadersResolvesBackupReaderTargets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "readers.xml")
	raw := `<Readers>
  <reader id="00" provider="speakyapi">
    <gender>male</gender>
    <language>en-US</language>
    <voice_id>RadioMET Tom</voice_id>
  </reader>
  <reader id="04" provider="speakyapi">
    <gender>male</gender>
    <language>en-US</language>
    <voice_id>RadioMET Paul</voice_id>
  </reader>
  <reader id="000" provider="piper">
    <gender>male</gender>
    <language>en-US</language>
    <voice_id>en_US-hfc_male-medium</voice_id>
    <backup>
      <reader>00</reader>
      <reader>04</reader>
    </backup>
  </reader>
</Readers>`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	readers, err := LoadReaders(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(readers) != 3 {
		t.Fatalf("readers = %#v", readers)
	}
	for _, index := range []int{0, 1} {
		backup := readers[index].Backup
		if backup == nil || backup.ReaderID != "000" || backup.Provider != "piper" || backup.VoiceID != "en_US-hfc_male-medium" {
			t.Fatalf("reader %q backup = %#v", readers[index].ID, backup)
		}
	}
	if got := readers[2].BackupFor; len(got) != 2 || got[0] != "00" || got[1] != "04" {
		t.Fatalf("backup targets = %#v", got)
	}
}

func TestLoadReadersRejectsConflictingBackups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "readers.xml")
	raw := `<Readers>
  <reader id="00" provider="speakyapi"><voice_id>RadioMET Tom</voice_id></reader>
  <reader id="000" provider="piper"><voice_id>en_US-hfc_male-medium</voice_id><backup><reader>00</reader></backup></reader>
  <reader id="001" provider="piper"><voice_id>en_US-lessac-low</voice_id><backup><reader>00</reader></backup></reader>
</Readers>`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadReaders(path); err == nil {
		t.Fatal("expected conflicting backup error")
	}
}

func TestCWXRReadersResolveLocalBackups(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "bundle", "managed", "configs", "cwxr-readers.xml")
	readers, err := LoadReaders(path)
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[string]Reader, len(readers))
	for _, reader := range readers {
		byID[reader.ID] = reader
	}
	for primaryID, backupID := range map[string]string{
		"00": "000",
		"01": "001",
		"02": "002",
		"03": "003",
	} {
		backup := byID[primaryID].Backup
		if backup == nil || backup.ReaderID != backupID || backup.Provider != "piper" {
			t.Fatalf("reader %q backup = %#v", primaryID, backup)
		}
	}
}

func TestNormalizeProviderFastAliases(t *testing.T) {
	for _, provider := range []string{"fast", "ivr-fast", "low_latency"} {
		if got := NormalizeProvider(provider); got != "fast" {
			t.Fatalf("NormalizeProvider(%q) = %q", provider, got)
		}
	}
}

func TestNormalizeProviderKokoroAliases(t *testing.T) {
	for _, provider := range []string{"kokoro", "kokoro-tts", "sherpa", "sherpa-onnx"} {
		if got := NormalizeProvider(provider); got != "kokoro" {
			t.Fatalf("NormalizeProvider(%q) = %q", provider, got)
		}
	}
}

func TestNormalizeProviderSpeakyAPIAliases(t *testing.T) {
	for _, provider := range []string{"speaky", "speaky-api", "speakyapi"} {
		if got := NormalizeProvider(provider); got != "speakyapi" {
			t.Fatalf("NormalizeProvider(%q) = %q", provider, got)
		}
	}
}

func TestSelectReaderPrefersExplicitReaderID(t *testing.T) {
	readers := []Reader{
		{ID: "00", Provider: "auto", Gender: "male", Language: "en-us"},
		{ID: "wxr_tom", Provider: "sapi5", Gender: "male", Language: "en-us", VoiceID: "Nuance Tom"},
	}

	reader, ok := SelectReader(readers, "wxr_tom", "en-US", "")
	if !ok {
		t.Fatal("reader not found")
	}
	if reader.Provider != "sapi5" || reader.VoiceID != "Nuance Tom" {
		t.Fatalf("reader = %+v", reader)
	}
}

func TestSelectReaderFallsBackByLanguageAndGender(t *testing.T) {
	readers := []Reader{
		{ID: "male", Provider: "auto", Gender: "male", Language: "en-us"},
		{ID: "female", Provider: "auto", Gender: "female", Language: "en"},
	}

	reader, ok := SelectReader(readers, "female", "en-US", "")
	if !ok {
		t.Fatal("reader not found")
	}
	if reader.ID != "female" {
		t.Fatalf("reader = %+v", reader)
	}
}
