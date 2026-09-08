package playlist

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPlaylistAudioRetentionProtectsPlaybackAndFallback(t *testing.T) {
	dir := t.TempDir()
	feedDir := filepath.Join(dir, "test")
	if err := os.Mkdir(feedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	cases := []struct {
		name string
		age  time.Duration
		keep bool
	}{
		{"forecast-en-us-1788716889694904851-aaaa.wav", 20 * time.Minute, false},
		{"forecast-en-us-1788716889694904852-bbbb.wav", 10 * time.Minute, true},
		{"forecast-fr-ca-1788716889694904853-cccc.wav", 12 * time.Minute, true},
		{"forecast-en-us-1788716889694904854-dddd.wav", time.Minute, true},
		{"current_conditions-en-us-1788716889694904855-eeee.wav", 10 * time.Minute, true},
		{"station_id-en-us-1788716889694904856-ffff.wav", time.Hour, false},
		{"operator.wav", time.Hour, true},
		{"forecast-en-us-1788716889694904857-abcd-text-00.wav", time.Hour, true},
	}
	for _, tc := range cases {
		path := filepath.Join(feedDir, tc.name)
		if err := os.WriteFile(path, []byte("audio"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, now.Add(-tc.age), now.Add(-tc.age)); err != nil {
			t.Fatal(err)
		}
	}
	p := &feedPlanner{
		cfg: loadedConfig{OutputDir: dir}, feed: feedXML{ID: "test"},
		current: &playlistItem{AudioPath: filepath.Join(feedDir, cases[1].name)},
		queue:   []playlistItem{{AudioPath: filepath.Join(feedDir, cases[4].name)}},
	}
	p.prunePlaylistAudio(now)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := os.Stat(filepath.Join(feedDir, tc.name))
			if tc.keep && err != nil {
				t.Fatalf("protected file removed: %v", err)
			}
			if !tc.keep && !os.IsNotExist(err) {
				t.Fatalf("obsolete file retained: %v", err)
			}
		})
	}
}

func TestAlertAudioRetentionRequiresOldCompletedManifest(t *testing.T) {
	root := t.TempDir()
	audioDir := filepath.Join(root, "runtime", "audio", "alerts")
	queueDir := filepath.Join(root, "runtime", "queues", "alerts")
	for _, dir := range []string{audioDir, queueDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	cases := []struct {
		id     string
		status string
		played string
		age    time.Duration
		keep   bool
	}{
		{"completed", "played", now.Add(-time.Hour).Format(time.RFC3339), time.Hour, false},
		{"queued", "queued", "", time.Hour, true},
		{"playing", "playing", "", time.Hour, true},
		{"recently-played", "played", now.Add(-time.Minute).Format(time.RFC3339), time.Hour, true},
		{"replaced-audio", "played", now.Add(-time.Hour).Format(time.RFC3339), time.Minute, true},
		{"missing-timestamp", "played", "", time.Hour, true},
	}
	for _, tc := range cases {
		path := filepath.Join(audioDir, tc.id+".pcm16le")
		if err := os.WriteFile(path, []byte("audio"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, now.Add(-tc.age), now.Add(-tc.age)); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(map[string]string{"id": tc.id, "status": tc.status, "played_at": tc.played})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(queueDir, tc.id+".json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s := &Service{cfg: loadedConfig{BaseDir: root}}
	s.pruneCompletedAlertAudio(now)
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			_, err := os.Stat(filepath.Join(audioDir, tc.id+".pcm16le"))
			if tc.keep && err != nil {
				t.Fatalf("protected audio removed: %v", err)
			}
			if !tc.keep && !os.IsNotExist(err) {
				t.Fatalf("completed audio retained: %v", err)
			}
			if _, err := os.Stat(filepath.Join(queueDir, tc.id+".json")); err != nil {
				t.Fatalf("manifest removed: %v", err)
			}
		})
	}
}
