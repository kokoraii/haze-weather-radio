package dotenv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCreatesMissingFileFromExample(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")
	examplePath := filepath.Join(dir, ".env.example")
	if err := os.WriteFile(examplePath, []byte("SITE_NAME=Example Site\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldValue, hadValue := os.LookupEnv("SITE_NAME")
	if err := os.Unsetenv("SITE_NAME"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if hadValue {
			_ = os.Setenv("SITE_NAME", oldValue)
		} else {
			_ = os.Unsetenv("SITE_NAME")
		}
	}()

	if err := Load(envPath); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "SITE_NAME=Example Site\n" {
		t.Fatalf(".env = %q", raw)
	}
	if got := os.Getenv("SITE_NAME"); got != "Example Site" {
		t.Fatalf("SITE_NAME = %q", got)
	}
}
