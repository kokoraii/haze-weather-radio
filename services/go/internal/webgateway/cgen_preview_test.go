package webgateway

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadChangedCgenPreviewSkipsUnchangedFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "preview.jpg")
	if err := os.WriteFile(path, []byte("frame-one"), 0o644); err != nil {
		t.Fatal(err)
	}
	previous, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if raw, _, err := readChangedCgenPreview(path, previous); err != nil || raw != nil {
		t.Fatalf("unchanged frame was read: raw=%q err=%v", raw, err)
	}
	if err := os.WriteFile(path, []byte("frame-two"), 0o644); err != nil {
		t.Fatal(err)
	}
	changedAt := previous.ModTime().Add(time.Second)
	if err := os.Chtimes(path, changedAt, changedAt); err != nil {
		t.Fatal(err)
	}
	raw, current, err := readChangedCgenPreview(path, previous)
	if err != nil || string(raw) != "frame-two" {
		t.Fatalf("changed frame was not read: raw=%q err=%v", raw, err)
	}
	if raw, _, err := readChangedCgenPreview(path, current); err != nil || raw != nil {
		t.Fatalf("unchanged replacement frame was read: raw=%q err=%v", raw, err)
	}
}
