package webgateway

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/alerttext"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capsame"
)

type bannerPayload struct {
	Active          bool                        `json:"active"`
	Signature       string                      `json:"signature"`
	FeedID          string                      `json:"feed_id,omitempty"`
	FeedName        string                      `json:"feed_name,omitempty"`
	GeneratedAt     time.Time                   `json:"generated_at"`
	PrimaryColor    string                      `json:"primary_color"`
	PrimaryGradient []string                    `json:"primary_gradient"`
	Alerts          []alerttext.SerializedAlert `json:"alerts"`
}

const bannerArchiveCacheTTL = 5 * time.Second
const bannerFeedMetaCacheTTL = 15 * time.Second
const bannerQueueCacheTTL = 1 * time.Second

type bannerArchiveCacheEntry struct {
	expires time.Time
	records []archiveCAPRecord
}

type bannerFeedMeta struct {
	Name     string
	Timezone string
}

type bannerFeedMetaCacheEntry struct {
	expires time.Time
	feeds   map[string]bannerFeedMeta
}

type bannerQueueCacheEntry struct {
	expires time.Time
	items   []sameQueueItem
	err     error
}

var bannerArchiveCache = struct {
	sync.Mutex
	entries map[string]bannerArchiveCacheEntry
}{
	entries: map[string]bannerArchiveCacheEntry{},
}

var bannerFeedMetaCache = struct {
	sync.Mutex
	entry map[string]bannerFeedMetaCacheEntry
}{
	entry: map[string]bannerFeedMetaCacheEntry{},
}

var bannerQueueCache = struct {
	sync.Mutex
	entry map[string]bannerQueueCacheEntry
}{
	entry: map[string]bannerQueueCacheEntry{},
}

func (s *Server) bannerStream(writer http.ResponseWriter, request *http.Request) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "streaming is not supported", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Accel-Buffering", "no")

	feedID := strings.TrimSpace(request.URL.Query().Get("feed"))
	ticker := time.NewTicker(750 * time.Millisecond)
	defer ticker.Stop()

	lastSignature := ""
	for {
		payload := buildBannerPayload(s.configPath, feedID, s.bannerHub)
		if payload.Signature != lastSignature {
			if err := writeBannerEvent(writer, payload); err != nil {
				return
			}
			flusher.Flush()
			lastSignature = payload.Signature
		}

		select {
		case <-request.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) bannerCurrent(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	feedID := strings.TrimSpace(request.URL.Query().Get("feed"))
	writeJSON(writer, buildBannerPayload(s.configPath, feedID, s.bannerHub))
}

func (s *Server) bannerAudio(writer http.ResponseWriter, request *http.Request) {
	connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
		OriginPatterns: allOriginPatterns(),
	})
	if err != nil {
		return
	}
	defer connection.CloseNow()

	for {
		if _, _, err := connection.Read(request.Context()); err != nil {
			return
		}
	}
}

func (s *Server) bannerWebRTCOffer(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	legacyMediaAvailable := s.media != nil && s.media.Available()
	if !legacyMediaAvailable && mediaServiceBaseURL(s.config) == "" {
		http.Error(writer, "media bridge is not available", http.StatusServiceUnavailable)
		return
	}
	var payload struct {
		FeedID         string `json:"feed_id"`
		SDP            string `json:"sdp"`
		DisableG722    bool   `json:"disable_g722"`
		RequireOpus    bool   `json:"require_opus"`
		Codec          string `json:"codec"`
		PreferredCodec string `json:"preferred_codec"`
	}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		http.Error(writer, "invalid JSON", http.StatusBadRequest)
		return
	}
	feedID := strings.TrimSpace(payload.FeedID)
	if feedID == "" {
		feedID = strings.TrimSpace(request.URL.Query().Get("feed"))
	}
	if feedID == "" {
		http.Error(writer, "feed_id is required", http.StatusBadRequest)
		return
	}
	if !s.feedWebRTCEnabled(feedID) {
		http.Error(writer, "feed WebRTC output is not enabled", http.StatusForbidden)
		return
	}
	mediaResult := s.mediaServiceWebRTCAnswerResult(request.Context(), map[string]any{
		"feed_id":         feedID,
		"sdp":             payload.SDP,
		"disable_g722":    payload.DisableG722,
		"require_opus":    payload.RequireOpus,
		"preferred_codec": firstNonBlank(payload.Codec, payload.PreferredCodec),
		"client_ip":       clientIPForMediaRequest(request),
		"remote_addr":     request.RemoteAddr,
	})
	if mediaResult.OK() {
		answer := mediaResult.Answer
		answer["feed_id"] = feedID
		if _, ok := answer["sdp_type"]; !ok {
			answer["sdp_type"] = "answer"
		}
		writeJSON(writer, answer)
		return
	}
	if mediaResult.Terminal {
		status := mediaResult.StatusCode
		if status < 400 {
			status = http.StatusBadGateway
		}
		http.Error(writer, firstNonBlank(mediaResult.Detail, "haze-media WebRTC offer was rejected"), status)
		return
	}
	if !legacyMediaAvailable {
		if mediaServiceBaseURL(s.config) != "" {
			http.Error(writer, "haze-media WebRTC service is unavailable and legacy media bridge is not available", http.StatusServiceUnavailable)
			return
		}
		http.Error(writer, "media bridge is not available", http.StatusServiceUnavailable)
		return
	}
	release, ok := s.acquireFeedListener(writer, request, feedID)
	if !ok {
		return
	}
	answer, err := s.media.AnswerWithOptions(request.Context(), feedID, payload.SDP, WebRTCAnswerOptions{
		DisableG722:    payload.DisableG722,
		RequireOpus:    payload.RequireOpus,
		PreferredCodec: firstNonBlank(payload.Codec, payload.PreferredCodec),
		OnClose:        release,
	})
	if err != nil {
		release()
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(writer, map[string]any{
		"feed_id":      feedID,
		"sdp":          answer.SDP,
		"sdp_type":     "answer",
		"media_recent": answer.MediaRecent,
		"codec":        answer.Codec.String(),
		"payload_type": answer.PayloadType,
	})
}

func (s *Server) feedWebRTCEnabled(feedID string) bool {
	return feedAudioOutputEnabled(s.configPath, feedID)
}

func feedAudioOutputEnabled(configPath string, feedID string) bool {
	feedID = strings.TrimSpace(feedID)
	if feedID == "" {
		return false
	}
	feeds, err := loadBasicFeedSummaries(configPath)
	if err != nil {
		return false
	}
	for _, feed := range feeds {
		if stringValue(feed, "id") != feedID {
			continue
		}
		enabled, _ := feed["enabled"].(bool)
		webrtc, _ := feed["webrtc_enabled"].(bool)
		return enabled && webrtc
	}
	return false
}

func writeBannerEvent(writer http.ResponseWriter, payload bannerPayload) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal banner payload: %w", err)
	}
	if _, err := fmt.Fprintf(writer, "event: banner\ndata: %s\n\n", raw); err != nil {
		return fmt.Errorf("write banner event: %w", err)
	}
	return nil
}

func buildBannerPayload(configPath string, feedID string, hub *BannerHub) bannerPayload {
	now := time.Now().UTC()
	records := onAirBannerRecords(configPath, feedID, hub, now)
	alerts := make([]alerttext.SerializedAlert, 0, len(records))
	visuals := make([]alerttext.AlertVisualInput, 0, len(records))
	for _, record := range records {
		info := chooseArchiveInfo(record.Alert)
		areas := archiveAreaNames(info)
		if canonicalNames := bannerCanonicalLocationNames(record.CanonicalLocations); len(canonicalNames) > 0 {
			areas = canonicalNames
		}
		serialized := alerttext.SerializeCAPAlert(
			record.Alert,
			info,
			record.FeedID,
			areas,
			bannerFeedTimezone(configPath, record.FeedID),
			"cap",
			now,
		)
		serialized.CanonicalLocations = append([]alertmodel.LocationReference(nil), record.CanonicalLocations...)
		if text := strings.TrimSpace(fallbackString(record.BannerText, record.AlertText)); text != "" {
			serialized.Message = text
		}
		visualEvent := bannerVisualEvent(info, record)
		broadcastImmediate := record.BroadcastImmediate || alerttext.IsBroadcastImmediateInfo(info)
		alertGradient := alerttext.PickBannerGradient([]alerttext.AlertVisualInput{{
			Severity:           info.Severity,
			Event:              visualEvent,
			BroadcastImmediate: broadcastImmediate,
		}})
		serialized.BackgroundColor = alertGradient[0]
		serialized.BackgroundGradient = alertGradient
		alerts = append(alerts, serialized)
		visuals = append(visuals, alerttext.AlertVisualInput{
			Severity:           info.Severity,
			Event:              visualEvent,
			BroadcastImmediate: broadcastImmediate,
		})
	}
	gradient := alerttext.PickBannerGradient(visuals)
	if len(alerts) == 0 {
		gradient = alerttext.PickBannerGradient(nil)
	}
	payload := bannerPayload{
		Active:          len(alerts) > 0,
		FeedID:          strings.TrimSpace(feedID),
		FeedName:        bannerFeedName(configPath, feedID),
		GeneratedAt:     now,
		PrimaryColor:    gradient[0],
		PrimaryGradient: gradient,
		Alerts:          alerts,
	}
	payload.Signature = bannerSignature(payload)
	return payload
}

func bannerVisualEvent(info capmodel.AlertInfo, record archiveCAPRecord) string {
	parts := []string{info.Event, info.Headline, alerttext.AlertSubject(info), record.BannerText, record.AlertText}
	out := parts[:0]
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return strings.Join(out, " ")
}

func activeBannerRecords(configPath string, feedID string, now time.Time) []archiveCAPRecord {
	rows := cachedBannerArchiveRecords(configPath, "accepted", time.Time{}, now)
	out := make([]archiveCAPRecord, 0, len(rows))
	for _, record := range rows {
		if feedID != "" && feedID != "*" && record.FeedID != feedID {
			continue
		}
		if archiveAlertExpired(record.Alert, now) {
			continue
		}
		out = append(out, record)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return bannerSortKey(out[i].Alert).less(bannerSortKey(out[j].Alert))
	})
	return out
}

func onAirBannerRecords(configPath string, feedID string, hub *BannerHub, now time.Time) []archiveCAPRecord {
	active := hub.Active(feedID, now)
	if len(active) == 0 {
		active = activeQueueBannerAlerts(configPath, feedID, now)
	}
	if len(active) == 0 {
		return nil
	}
	out := make([]archiveCAPRecord, 0, len(active))
	for _, item := range active {
		record, ok := findCachedBannerArchiveAlert(configPath, item.AlertID, item.FeedID, now)
		if !ok {
			record, ok = findCachedBannerArchiveAlert(configPath, item.AlertID, "", now)
			if ok {
				record.FeedID = item.FeedID
			}
		}
		if !ok {
			record, ok = findArchiveAlertByQueueHint(configPath, item, now)
		}
		if !ok {
			record = bannerRecordFromOnAirAlert(item, now)
		}
		if text := strings.TrimSpace(item.AlertText); text != "" {
			record.AlertText = text
		}
		if text := strings.TrimSpace(item.BannerText); text != "" {
			record.BannerText = text
		}
		if len(item.CanonicalLocations) > 0 {
			record.CanonicalLocations = append([]alertmodel.LocationReference(nil), item.CanonicalLocations...)
		}
		if archiveAlertExpired(record.Alert, now) {
			continue
		}
		out = append(out, record)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return bannerSortKey(out[i].Alert).less(bannerSortKey(out[j].Alert))
	})
	return out
}

func activeQueueBannerAlerts(configPath string, feedID string, now time.Time) []bannerOnAirAlert {
	items, err := cachedBannerQueueItems(configPath, now)
	if err != nil {
		return nil
	}
	out := []bannerOnAirAlert{}
	for _, item := range items {
		if !bannerQueueItemOnAir(item) {
			continue
		}
		if !bannerQueueItemFresh(item, now) {
			continue
		}
		for _, targetFeedID := range queueItemFeedIDs(item) {
			targetFeedID = strings.TrimSpace(targetFeedID)
			if targetFeedID == "" {
				continue
			}
			if feedID != "" && feedID != "*" && targetFeedID != feedID {
				continue
			}
			expires := now.Add(30 * time.Minute)
			if !item.CreatedAt.IsZero() {
				expires = item.CreatedAt.Add(30 * time.Minute)
				if expires.Before(now) {
					expires = now.Add(15 * time.Second)
				}
			}
			packetFields := map[string]any{}
			if item.AlertPacket != nil {
				packetFields = alertmodel.LegacyFields(item.AlertPacket.Normalize())
			}
			out = append(out, bannerOnAirAlert{
				FeedID:             targetFeedID,
				AlertID:            fallbackString(packetField(packetFields, "alert_id"), item.AlertID),
				QueueID:            item.ID,
				Event:              fallbackString(packetField(packetFields, "same_event"), packetField(packetFields, "event"), item.Event),
				Header:             fallbackString(packetField(packetFields, "headline"), packetField(packetFields, "title"), item.Header),
				AlertText:          fallbackString(packetField(packetFields, "alert_text"), item.AlertText),
				BannerText:         fallbackString(packetField(packetFields, "banner_text"), item.BannerText),
				BroadcastImmediate: packetBool(packetFields, "broadcast_immediate") || item.BroadcastImmediate,
				CanonicalLocations: bannerPacketLocations(item.AlertPacket),
				ExpiresAt:          expires,
				UpdatedAt:          now,
			})
		}
	}
	return out
}

func bannerCanonicalLocationNames(locations []alertmodel.LocationReference) []string {
	out := make([]string, 0, len(locations))
	seen := map[string]struct{}{}
	for _, location := range locations {
		if !location.Actionable || strings.TrimSpace(location.CanonicalID) == "" || location.Ambiguous {
			continue
		}
		name := strings.TrimSpace(location.Name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, name)
	}
	return out
}

func bannerQueueItemOnAir(item sameQueueItem) bool {
	switch strings.ToLower(strings.TrimSpace(item.Status)) {
	case "playing":
		return true
	default:
		return false
	}
}

func bannerQueueItemFresh(item sameQueueItem, now time.Time) bool {
	status := strings.ToLower(strings.TrimSpace(item.Status))
	anchor := parseQueueTimestamp(item.ClaimedAt)
	if anchor.IsZero() {
		anchor = item.CreatedAt
	}
	if anchor.IsZero() {
		return status == "playing"
	}
	switch status {
	case "playing":
		return now.Sub(anchor) <= 2*time.Hour
	case "claimed", "queued", "pending":
		return now.Sub(anchor) <= 30*time.Minute
	default:
		return false
	}
}

func parseQueueTimestamp(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func findArchiveAlertByQueueHint(configPath string, item bannerOnAirAlert, now time.Time) (archiveCAPRecord, bool) {
	hint := strings.ToLower(strings.Join([]string{item.AlertID, item.Event, item.Header, item.QueueID}, " "))
	for _, record := range cachedBannerArchiveRecords(configPath, "accepted", time.Time{}, now) {
		if item.FeedID != "" && item.FeedID != "*" && record.FeedID != item.FeedID {
			continue
		}
		info := chooseArchiveInfo(record.Alert)
		resolvedEvent := capsame.ResolveEvent(record.Alert, info, filepath.Dir(filepath.Clean(configPath))).Event
		haystack := strings.ToLower(strings.Join([]string{
			record.ID,
			record.Alert.Identifier,
			info.Event,
			info.Headline,
			resolvedEvent,
		}, " "))
		sameEvent := strings.ToLower(strings.TrimSpace(resolvedEvent))
		if haystack != "" && hint != "" && sameEvent != "" && strings.Contains(hint, sameEvent) {
			return record, true
		}
		if haystack != "" && hint != "" && strings.Contains(hint, strings.ToLower(safeID(record.ID))) {
			return record, true
		}
	}
	return archiveCAPRecord{}, false
}

func cachedBannerArchiveRecords(configPath string, bucket string, since time.Time, now time.Time) []archiveCAPRecord {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	key := strings.Join([]string{
		filepath.Clean(configPath),
		strings.TrimSpace(bucket),
		since.UTC().Format(time.RFC3339Nano),
	}, "\x00")

	bannerArchiveCache.Lock()
	if entry, ok := bannerArchiveCache.entries[key]; ok && now.Before(entry.expires) {
		records := cloneArchiveCAPRecords(entry.records)
		bannerArchiveCache.Unlock()
		return records
	}
	bannerArchiveCache.Unlock()

	records := archiveStoreRecords(configPath, bucket, since)
	bannerArchiveCache.Lock()
	bannerArchiveCache.entries[key] = bannerArchiveCacheEntry{
		expires: now.Add(bannerArchiveCacheTTL),
		records: cloneArchiveCAPRecords(records),
	}
	for existingKey, entry := range bannerArchiveCache.entries {
		if !now.Before(entry.expires) {
			delete(bannerArchiveCache.entries, existingKey)
		}
	}
	bannerArchiveCache.Unlock()
	return records
}

func findCachedBannerArchiveAlert(configPath string, id string, feedID string, now time.Time) (archiveCAPRecord, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return archiveCAPRecord{}, false
	}
	for _, record := range cachedBannerArchiveRecords(configPath, "accepted", time.Time{}, now) {
		if (record.ID == id || record.Alert.Identifier == id) && (feedID == "" || record.FeedID == feedID) {
			if record.FeedID == "" {
				record.FeedID = feedID
			}
			return record, true
		}
	}
	return archiveCAPRecord{}, false
}

func cloneArchiveCAPRecords(records []archiveCAPRecord) []archiveCAPRecord {
	return append([]archiveCAPRecord(nil), records...)
}

func clearBannerArchiveCache() {
	bannerArchiveCache.Lock()
	bannerArchiveCache.entries = map[string]bannerArchiveCacheEntry{}
	bannerArchiveCache.Unlock()

	bannerQueueCache.Lock()
	bannerQueueCache.entry = map[string]bannerQueueCacheEntry{}
	bannerQueueCache.Unlock()
}

func bannerRecordFromOnAirAlert(item bannerOnAirAlert, now time.Time) archiveCAPRecord {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	updated := item.UpdatedAt
	if updated.IsZero() {
		updated = now
	}
	expires := item.ExpiresAt
	if expires.IsZero() {
		expires = now.Add(30 * time.Minute)
	}
	alertID := fallbackString(item.AlertID, item.QueueID, fmt.Sprintf("on-air-%d", updated.UnixNano()))
	event := fallbackString(item.Event, "Alert")
	headline := fallbackString(item.Header, event, "Weather Alert")
	alertText := strings.TrimSpace(fallbackString(item.AlertText, headline))
	bannerText := strings.TrimSpace(fallbackString(item.BannerText, alertText))
	sent := updated.UTC().Format(time.RFC3339Nano)
	return archiveCAPRecord{
		ID:                 alertID,
		FeedID:             item.FeedID,
		Status:             "on_air",
		UpdatedAt:          updated,
		AlertText:          alertText,
		BannerText:         bannerText,
		BroadcastImmediate: item.BroadcastImmediate,
		Alert: capmodel.Alert{
			Identifier:  alertID,
			Sent:        sent,
			Status:      "Actual",
			MessageType: "Alert",
			Scope:       "Public",
			Infos: []capmodel.AlertInfo{{
				Language:    "en-CA",
				Event:       event,
				Severity:    "Unknown",
				Urgency:     "Unknown",
				Certainty:   "Unknown",
				Effective:   sent,
				Expires:     expires.UTC().Format(time.RFC3339Nano),
				SenderName:  "Haze Weather Radio",
				Headline:    headline,
				Description: headline,
			}},
		},
	}
}

type bannerSort struct {
	severityRank int
	timestamp    time.Time
	identifier   string
}

func bannerSortKey(alert capmodel.Alert) bannerSort {
	info := chooseArchiveInfo(alert)
	severity := strings.Title(strings.ToLower(strings.TrimSpace(info.Severity)))
	if severity == "" {
		severity = "Unknown"
	}
	rank := len([]string{"Extreme", "Severe", "Moderate", "Minor", "Unknown"})
	for i, value := range []string{"Extreme", "Severe", "Moderate", "Minor", "Unknown"} {
		if severity == value {
			rank = i
			break
		}
	}
	timestamp := time.Time{}
	for _, raw := range []string{info.Effective, alert.Sent, info.Onset} {
		if parsed := alerttext.ParseCAPTime(raw); !parsed.IsZero() {
			timestamp = parsed
			break
		}
	}
	return bannerSort{
		severityRank: rank,
		timestamp:    timestamp,
		identifier:   strings.TrimSpace(alert.Identifier),
	}
}

func (key bannerSort) less(other bannerSort) bool {
	if key.severityRank != other.severityRank {
		return key.severityRank < other.severityRank
	}
	if !key.timestamp.Equal(other.timestamp) {
		return key.timestamp.After(other.timestamp)
	}
	return key.identifier < other.identifier
}

func bannerFeedName(configPath string, feedID string) string {
	if feedID == "" {
		return ""
	}
	if meta, ok := cachedBannerFeedMetas(configPath, time.Now().UTC())[feedID]; ok {
		return fallbackString(meta.Name, feedID)
	}
	return feedID
}

func bannerFeedTimezone(configPath string, feedID string) string {
	if feedID == "" {
		return "Local"
	}
	if meta, ok := cachedBannerFeedMetas(configPath, time.Now().UTC())[feedID]; ok {
		return fallbackString(meta.Timezone, "Local")
	}
	return "Local"
}

func bannerFeedIDsFromConfig(configPath string, now time.Time) []string {
	metas := cachedBannerFeedMetas(configPath, now)
	ids := make([]string, 0, len(metas))
	for id := range metas {
		if strings.TrimSpace(id) != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func cachedBannerFeedMetas(configPath string, now time.Time) map[string]bannerFeedMeta {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	key := filepath.Clean(configPath)
	bannerFeedMetaCache.Lock()
	if entry, ok := bannerFeedMetaCache.entry[key]; ok && now.Before(entry.expires) {
		feeds := cloneBannerFeedMetas(entry.feeds)
		bannerFeedMetaCache.Unlock()
		return feeds
	}
	bannerFeedMetaCache.Unlock()

	feeds := loadBannerFeedMetas(configPath)
	bannerFeedMetaCache.Lock()
	bannerFeedMetaCache.entry[key] = bannerFeedMetaCacheEntry{
		expires: now.Add(bannerFeedMetaCacheTTL),
		feeds:   cloneBannerFeedMetas(feeds),
	}
	for existingKey, entry := range bannerFeedMetaCache.entry {
		if !now.Before(entry.expires) {
			delete(bannerFeedMetaCache.entry, existingKey)
		}
	}
	bannerFeedMetaCache.Unlock()
	return feeds
}

func loadBannerFeedMetas(configPath string) map[string]bannerFeedMeta {
	root, err := loadYAMLMap(configPath)
	if err != nil {
		return map[string]bannerFeedMeta{}
	}
	parsed, err := loadFeedsXML(configPath, root)
	if err != nil {
		return map[string]bannerFeedMeta{}
	}
	out := make(map[string]bannerFeedMeta, len(parsed.Feeds))
	for _, feed := range parsed.Feeds {
		id := strings.TrimSpace(feed.ID)
		if id == "" {
			continue
		}
		station := stationTransmitter(feed)
		out[id] = bannerFeedMeta{
			Name:     fallbackText(station.SiteName, id),
			Timezone: fallbackText(feed.Timezone, "Local"),
		}
	}
	return out
}

func cloneBannerFeedMetas(feeds map[string]bannerFeedMeta) map[string]bannerFeedMeta {
	out := make(map[string]bannerFeedMeta, len(feeds))
	for id, meta := range feeds {
		out[id] = meta
	}
	return out
}

func cachedBannerQueueItems(configPath string, now time.Time) ([]sameQueueItem, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	key := filepath.Clean(configPath)
	bannerQueueCache.Lock()
	if entry, ok := bannerQueueCache.entry[key]; ok && now.Before(entry.expires) {
		items := cloneSameQueueItems(entry.items)
		err := entry.err
		bannerQueueCache.Unlock()
		return items, err
	}
	bannerQueueCache.Unlock()

	items, err := loadAlertQueueItems(configPath)
	bannerQueueCache.Lock()
	bannerQueueCache.entry[key] = bannerQueueCacheEntry{
		expires: now.Add(bannerQueueCacheTTL),
		items:   cloneSameQueueItems(items),
		err:     err,
	}
	for existingKey, entry := range bannerQueueCache.entry {
		if !now.Before(entry.expires) {
			delete(bannerQueueCache.entry, existingKey)
		}
	}
	bannerQueueCache.Unlock()
	return items, err
}

func cloneSameQueueItems(items []sameQueueItem) []sameQueueItem {
	return append([]sameQueueItem(nil), items...)
}

func bannerSignature(payload bannerPayload) string {
	hash := sha1.New()
	_, _ = fmt.Fprintf(hash, "%t|%s|%s|", payload.Active, payload.FeedID, payload.FeedName)
	for _, alert := range payload.Alerts {
		_, _ = fmt.Fprintf(hash, "%s|%s|%s|%s|%s|", alert.Identifier, alert.FeedID, alert.Headline, alert.ExpiresAt, alert.Message)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func bannerPayloadForTest(configPath string, feedID string, now time.Time) bannerPayload {
	records := activeBannerRecords(configPath, feedID, now)
	alerts := make([]alerttext.SerializedAlert, 0, len(records))
	for _, record := range records {
		info := chooseArchiveInfo(record.Alert)
		alerts = append(alerts, alerttext.SerializeCAPAlert(record.Alert, info, record.FeedID, archiveAreaNames(info), "UTC", "cap", now))
	}
	payload := bannerPayload{Active: len(alerts) > 0, FeedID: feedID, GeneratedAt: now, PrimaryGradient: []string{"#019310", "#0b3810"}, PrimaryColor: "#019310", Alerts: alerts}
	payload.Signature = bannerSignature(payload)
	return payload
}
