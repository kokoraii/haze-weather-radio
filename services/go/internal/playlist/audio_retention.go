package playlist

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

var generatedProductAudioRE = regexp.MustCompile(`^(.+)-[0-9]{16,}-[a-f0-9]+\.wav$`)

// Prune only generated, completed product copies. Current/queued audio and the
// newest usable fallback per product/language survive. The grace period also
// protects preparations that have finished writing but have not been queued.
func (p *feedPlanner) prunePlaylistAudio(now time.Time) {
	if now.Sub(p.lastAudioCleanup) < time.Minute || p.cfg.OutputDir == "" || p.feed.ID == "" {
		return
	}
	p.lastAudioCleanup = now
	dir := filepath.Join(p.cfg.OutputDir, safeID(p.feed.ID))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	protected := make(map[string]bool, len(p.queue)+1)
	protect := func(item playlistItem) {
		path, err := filepath.Abs(item.AudioPath)
		if err == nil {
			protected[path] = true
		}
	}
	if p.current != nil {
		protect(*p.current)
	}
	for _, item := range p.queue {
		protect(item)
	}
	type candidate struct {
		path     string
		group    string
		modified time.Time
	}
	files := make([]candidate, 0, len(entries))
	latest := map[string]candidate{}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		match := generatedProductAudioRE.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		path, err := filepath.Abs(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		file := candidate{path: path, group: match[1], modified: info.ModTime()}
		files = append(files, file)
		if previous, ok := latest[file.group]; !ok || file.modified.After(previous.modified) {
			latest[file.group] = file
		}
	}
	for _, file := range files {
		age := now.Sub(file.modified)
		if protected[file.path] || age < 5*time.Minute {
			continue
		}
		if latest[file.group].path == file.path && age <= cachedStartupFallbackMaxAge {
			continue
		}
		if err := os.Remove(file.path); err != nil && !os.IsNotExist(err) {
			log.Printf("playlist audio cleanup failed feed=%s: %v", p.feed.ID, err)
		}
	}
}

// Played alert audio is reproducible from the CAP archive. Keep manifests and
// never touch queued/playing alerts; allow time for all sinks to finish reading.
func (s *Service) pruneCompletedAlertAudio(now time.Time) {
	if now.Sub(s.lastAlertAudioCleanup) < time.Minute {
		return
	}
	s.lastAlertAudioCleanup = now
	dir := filepath.Join(s.cfg.BaseDir, "runtime", "queues", "alerts")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		var manifest struct {
			ID       string `json:"id"`
			Status   string `json:"status"`
			PlayedAt string `json:"played_at"`
		}
		if json.Unmarshal(raw, &manifest) != nil || manifest.Status != "played" || manifest.ID == "" || safeID(manifest.ID)+".json" != entry.Name() {
			continue
		}
		played := parseTime(manifest.PlayedAt)
		if played.IsZero() || now.Sub(played) < 15*time.Minute {
			continue
		}
		path := filepath.Join(s.cfg.BaseDir, "runtime", "audio", "alerts", safeID(manifest.ID)+".pcm16le")
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || now.Sub(info.ModTime()) < 15*time.Minute {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Printf("completed alert audio cleanup failed: %v", err)
		}
	}
}
