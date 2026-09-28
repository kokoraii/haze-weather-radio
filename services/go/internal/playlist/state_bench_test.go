package playlist

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWritePlaylistStateSkipsUnchangedTicksAndPublishesChanges(t *testing.T) {
	baseDir := t.TempDir()
	planner := &feedPlanner{cfg: loadedConfig{BaseDir: baseDir}, feed: feedXML{ID: "test"}, mode: "running"}
	path := filepath.Join(baseDir, "runtime", "playlists", "test.json")
	read := func() map[string]any {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var state map[string]any
		if err := json.Unmarshal(raw, &state); err != nil {
			t.Fatal(err)
		}
		return state
	}

	planner.writeState()
	first := read()
	firstWriteAt := planner.lastStateWriteAt
	planner.writeState()
	if !planner.lastStateWriteAt.Equal(firstWriteAt) || read()["updated_at"] != first["updated_at"] {
		t.Fatal("unchanged tick rewrote playlist state")
	}

	planner.mode = "paused"
	planner.writeState()
	changed := read()
	if changed["mode"] != "paused" || changed["updated_at"] == first["updated_at"] {
		t.Fatal("state change was not published immediately")
	}
	planner.applyControl("pause")
	afterControl := read()["updated_at"]
	if afterControl == changed["updated_at"] {
		t.Fatal("repeated control did not acknowledge with a fresh state timestamp")
	}

	planner.lastStateWriteAt = time.Now().Add(-playlistStateHeartbeat)
	planner.writeState()
	if read()["updated_at"] == afterControl {
		t.Fatal("playlist state heartbeat was not refreshed")
	}
}

func BenchmarkWriteUnchangedPlaylistState(b *testing.B) {
	planner := &feedPlanner{cfg: loadedConfig{BaseDir: b.TempDir()}, feed: feedXML{ID: "benchmark"}, mode: "running"}
	planner.writeState()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		planner.writeState()
	}
}
