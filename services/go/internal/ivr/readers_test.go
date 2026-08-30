package ivr

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveIVRReaderIDMatchesLanguage(t *testing.T) {
	tmp := t.TempDir()
	readersXML := `<Readers>
  <reader id="00" provider="speakyapi"><gender>male</gender><language>en-US</language><voice_id>Tom</voice_id></reader>
  <reader id="01" provider="speakyapi"><gender>female</gender><language>en-US</language><voice_id>Ava</voice_id></reader>
  <reader id="02" provider="speakyapi"><gender>male</gender><language>fr-CA</language><voice_id>Nicolas</voice_id></reader>
  <reader id="03" provider="speakyapi"><gender>female</gender><language>fr-CA</language><voice_id>Chantal</voice_id></reader>
  <reader id="04" provider="piper"><gender>male</gender><language>es-MX</language><voice_id>es_MX-ald-medium</voice_id></reader>
</Readers>`
	if err := os.WriteFile(filepath.Join(tmp, "readers.xml"), []byte(readersXML), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := loadedConfig{BaseDir: tmp}
	cfg.IVR.DefaultReaderID = "00"
	cfg.Root.Services.Go.TTS.Readers = "readers.xml"

	cases := []struct {
		requested string
		language  string
		want      string
	}{
		{"", "en-US", "00"},
		{"", "en-us", "00"},
		{"", "fr-CA", "02"},
		{"", "fr-ca", "02"},
		{"", "es", "04"},
		{"", "es-MX", "04"},
		{"07", "es", "07"},
		{"", "de-DE", "00"},
	}
	for _, tc := range cases {
		if got := cfg.resolveIVRReaderID(tc.requested, tc.language); got != tc.want {
			t.Fatalf("resolveIVRReaderID(%q,%q) = %q, want %q", tc.requested, tc.language, got, tc.want)
		}
	}
}

func TestProductCacheKeyUsesLanguageMatchedReader(t *testing.T) {
	tmp := t.TempDir()
	readersXML := `<Readers>
  <reader id="00" provider="speakyapi"><gender>male</gender><language>en-US</language><voice_id>Tom</voice_id></reader>
  <reader id="02" provider="speakyapi"><gender>male</gender><language>fr-CA</language><voice_id>Nicolas</voice_id></reader>
  <reader id="04" provider="piper"><gender>male</gender><language>es-MX</language><voice_id>es_MX-ald-medium</voice_id></reader>
</Readers>`
	if err := os.WriteFile(filepath.Join(tmp, "readers.xml"), []byte(readersXML), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := loadedConfig{BaseDir: tmp}
	cfg.IVR.DefaultReaderID = "00"
	cfg.Root.Services.Go.TTS.Readers = "readers.xml"

	pc := &ProductCache{cfg: cfg}
	location := ResolvedLocation{Code: "test", Language: "fr-ca"}
	_, _, language, readerID, _ := pc.productCacheKey(location, []string{"current_conditions"})
	if language != "fr-ca" {
		t.Fatalf("product language = %q, want fr-ca", language)
	}
	if readerID != "02" {
		t.Fatalf("product reader = %q, want 02", readerID)
	}

	spanish := ResolvedLocation{Code: "test", Language: "es"}
	_, _, _, spanishReader, _ := pc.productCacheKey(spanish, []string{"current_conditions"})
	if spanishReader != "04" {
		t.Fatalf("spanish product reader = %q, want 04", spanishReader)
	}
}
