package webgateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func playlistStatePayload(configPath string) (map[string]any, error) {
	dir := playlistStateDir(configPath)
	feeds, _ := loadFeedSummaries(configPath)
	byID := map[string]map[string]any{}
	ids := make([]string, 0, len(feeds))
	for _, feed := range feeds {
		id, _ := feed["id"].(string)
		if strings.TrimSpace(id) == "" {
			continue
		}
		ids = append(ids, id)
		byID[id] = map[string]any{
			"feed_id":   id,
			"feed_name": feed["name"],
			"mode":      "unknown",
			"current":   nil,
			"next":      nil,
			"queue":     []any{},
		}
	}
	entries, err := os.ReadDir(dir)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".json") {
				continue
			}
			raw, readErr := os.ReadFile(filepath.Join(dir, entry.Name()))
			if readErr != nil {
				continue
			}
			var payload map[string]any
			if json.Unmarshal(raw, &payload) != nil {
				continue
			}
			id, _ := payload["feed_id"].(string)
			if id == "" {
				id = strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
			}
			byID[id] = payload
			if !containsString(ids, id) {
				ids = append(ids, id)
			}
		}
	}
	sort.Strings(ids)
	out := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		out = append(out, byID[id])
	}
	return map[string]any{
		"feeds": out,
	}, nil
}

func playlistStateDir(configPath string) string {
	baseDir := filepath.Dir(filepath.Clean(configPath))
	return filepath.Join(baseDir, "runtime", "playlists")
}

func playlistFeedUpdatedAt(state map[string]any, feedID string) string {
	feed := playlistFeedState(state, feedID)
	if feed == nil {
		return ""
	}
	updated, _ := feed["updated_at"].(string)
	return strings.TrimSpace(updated)
}

func playlistFeedState(state map[string]any, feedID string) map[string]any {
	if state == nil {
		return nil
	}
	feeds, _ := state["feeds"].([]map[string]any)
	if feeds == nil {
		if rawFeeds, ok := state["feeds"].([]any); ok {
			feeds = make([]map[string]any, 0, len(rawFeeds))
			for _, raw := range rawFeeds {
				if feed, ok := raw.(map[string]any); ok {
					feeds = append(feeds, feed)
				}
			}
		}
	}
	for _, feed := range feeds {
		id, _ := feed["feed_id"].(string)
		if id == "" {
			id, _ = feed["id"].(string)
		}
		if strings.EqualFold(strings.TrimSpace(id), strings.TrimSpace(feedID)) {
			return feed
		}
	}
	return nil
}

func waitForPlaylistStateChange(configPath string, feedID string, previousUpdatedAt string, timeout time.Duration) (map[string]any, bool) {
	deadline := time.Now().Add(timeout)
	for {
		updatedAt := playlistFeedRuntimeUpdatedAt(configPath, feedID)
		if updatedAt != "" && updatedAt != previousUpdatedAt {
			state, err := playlistStatePayload(configPath)
			if err == nil {
				assembledUpdatedAt := playlistFeedUpdatedAt(state, feedID)
				if assembledUpdatedAt != "" && assembledUpdatedAt != previousUpdatedAt {
					return state, true
				}
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	state, err := playlistStatePayload(configPath)
	if err != nil {
		return nil, false
	}
	return state, false
}

// The playout service writes one atomic state file per feed. Polling only its
// timestamp avoids reparsing every managed feed and playlist during a control.
func playlistFeedRuntimeUpdatedAt(configPath string, feedID string) string {
	var fileID strings.Builder
	for _, ch := range feedID {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.' {
			fileID.WriteRune(ch)
		}
	}
	name := fileID.String()
	if name == "" {
		name = "item"
	}
	raw, err := os.ReadFile(filepath.Join(playlistStateDir(configPath), name+".json"))
	if err != nil {
		return ""
	}
	var state struct {
		FeedID    string `json:"feed_id"`
		UpdatedAt string `json:"updated_at"`
	}
	if json.Unmarshal(raw, &state) != nil || !strings.EqualFold(strings.TrimSpace(state.FeedID), strings.TrimSpace(feedID)) {
		return ""
	}
	return strings.TrimSpace(state.UpdatedAt)
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
