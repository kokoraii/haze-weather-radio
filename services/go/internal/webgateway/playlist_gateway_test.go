package webgateway

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPlaylistStatePayloadReadsRuntimePlaylistDirectory(t *testing.T) {
	dir := t.TempDir()
	writePlaylistGatewayFixture(t, dir)
	mustWrite(t, filepath.Join(dir, "runtime", "playlists", "sk-0001.json"), `{
  "feed_id": "sk-0001",
  "feed_name": "Saskatoon",
  "mode": "running",
  "current": null,
  "next": null,
  "queue": [],
  "updated_at": "2026-06-17T01:02:03Z"
}`)

	payload, err := playlistStatePayload(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	feed := playlistFeedState(payload, "sk-0001")
	if feed == nil {
		t.Fatal("missing feed state")
	}
	if mode, _ := feed["mode"].(string); mode != "running" {
		t.Fatalf("mode = %q, want running", mode)
	}
	if updated := playlistFeedUpdatedAt(payload, "sk-0001"); updated != "2026-06-17T01:02:03Z" {
		t.Fatalf("updated_at = %q", updated)
	}
}

func TestPlaylistStatePayloadIgnoresManagedRuntimePlaylistDirectory(t *testing.T) {
	dir := t.TempDir()
	writePlaylistGatewayFixture(t, dir)
	mustWrite(t, filepath.Join(dir, "managed", "runtime", "playlists", "sk-0001.json"), `{
  "feed_id": "sk-0001",
  "mode": "stale",
  "queue": [],
  "updated_at": "2026-06-17T00:00:00Z"
}`)

	payload, err := playlistStatePayload(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	feed := playlistFeedState(payload, "sk-0001")
	if feed == nil {
		t.Fatal("missing feed state")
	}
	if mode, _ := feed["mode"].(string); mode != "unknown" {
		t.Fatalf("mode = %q, want default unknown", mode)
	}
	if updated := playlistFeedUpdatedAt(payload, "sk-0001"); updated != "" {
		t.Fatalf("updated_at = %q, want empty", updated)
	}
}

func TestWaitForPlaylistStateChangeReadsTargetFeedState(t *testing.T) {
	dir := t.TempDir()
	writePlaylistGatewayFixture(t, dir)
	configPath := filepath.Join(dir, "config.yaml")
	statePath := filepath.Join(dir, "runtime", "playlists", "sk-0001.json")
	mustWrite(t, statePath, `{"feed_id":"sk-0001","mode":"running","updated_at":"before"}`)
	if got := playlistFeedRuntimeUpdatedAt(configPath, "sk-0001"); got != "before" {
		t.Fatalf("initial target timestamp = %q", got)
	}
	writeDone := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		writeDone <- os.WriteFile(statePath, []byte(`{"feed_id":"sk-0001","mode":"paused","updated_at":"after"}`), 0o644)
	}()
	state, settled := waitForPlaylistStateChange(configPath, "sk-0001", "before", time.Second)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if !settled || playlistFeedUpdatedAt(state, "sk-0001") != "after" {
		t.Fatalf("playlist wait did not observe target state: settled=%v state=%#v", settled, state)
	}
	if mode := playlistFeedState(state, "sk-0001")["mode"]; mode != "paused" {
		t.Fatalf("target mode = %v", mode)
	}
}

func TestPlaylistFeedRuntimeUpdatedAtIgnoresOtherFeed(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, filepath.Join(dir, "runtime", "playlists", "sk-0001.json"), `{"feed_id":"other","updated_at":"new"}`)
	if got := playlistFeedRuntimeUpdatedAt(configPath, "sk-0001"); got != "" {
		t.Fatalf("timestamp from another feed = %q", got)
	}
}

func writePlaylistGatewayFixture(t *testing.T, dir string) {
	t.Helper()
	mustWrite(t, filepath.Join(dir, "config.yaml"), `feeds_file: managed/configs/feeds.xml
outputs_file: managed/configs/output.xml
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<feeds>
  <feed id="sk-0001" enabled="true" timezone="America/Regina">
    <transmitter_metadata>
      <transmitter>
        <site_name>Saskatoon</site_name>
      </transmitter>
    </transmitter_metadata>
    <playout routine="true" same="true"/>
  </feed>
</feeds>`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "output.xml"), `<outputs/>`)
}
