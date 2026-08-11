package webgateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/alerttext"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capsame"
	"github.com/meowraii/haze-weather-radio/services/go/internal/datastore"
	"github.com/meowraii/haze-weather-radio/services/go/internal/events"
)

type archiveCAPRecord struct {
	ID                 string                         `json:"id"`
	FeedID             string                         `json:"feed_id,omitempty"`
	Status             string                         `json:"status"`
	Reason             string                         `json:"reason,omitempty"`
	UpdatedAt          time.Time                      `json:"updated_at"`
	Alert              capmodel.Alert                 `json:"alert"`
	RawXML             string                         `json:"raw_xml,omitempty"`
	AlertText          string                         `json:"alert_text,omitempty"`
	BannerText         string                         `json:"banner_text,omitempty"`
	BroadcastImmediate bool                           `json:"broadcast_immediate,omitempty"`
	CanonicalLocations []alertmodel.LocationReference `json:"canonical_locations,omitempty"`
}

func alertsArchivePayload(configPath string) (map[string]any, error) {
	now := time.Now().UTC()
	baseDir := filepath.Dir(filepath.Clean(configPath))
	active, rejected, expired := archiveStoreRecordBuckets(configPath, now)
	return map[string]any{
		"accepted_by_feed": archiveRecordsPayload(active, "accepted", baseDir),
		"rejected":         archiveRecordsPayload(rejected, "rejected", baseDir),
		"expired":          archiveRecordsPayload(expiredWithin(expired, now, 30*24*time.Hour), "expired", baseDir),
	}, nil
}

func archiveStoreRecords(configPath string, bucket string, since time.Time) []archiveCAPRecord {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := datastore.Open(ctx, config.Storage, filepath.Dir(filepath.Clean(configPath)))
	if err != nil {
		return nil
	}
	defer store.Close()
	return listArchiveStoreRecords(ctx, store, bucket, since)
}

func archiveStoreRecordBuckets(configPath string, now time.Time) ([]archiveCAPRecord, []archiveCAPRecord, []archiveCAPRecord) {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := datastore.Open(ctx, config.Storage, filepath.Dir(filepath.Clean(configPath)))
	if err != nil {
		return nil, nil, nil
	}
	defer store.Close()

	active := []archiveCAPRecord{}
	rejected := []archiveCAPRecord{}
	expired := []archiveCAPRecord{}
	for _, record := range listArchiveStoreRecords(ctx, store, "accepted", time.Time{}) {
		if archiveAlertExpired(record.Alert, now) {
			record.Status = "expired"
			expired = append(expired, record)
		} else {
			record.Status = "accepted"
			active = append(active, record)
		}
	}
	rejected = append(rejected, listArchiveStoreRecords(ctx, store, "rejected", time.Time{})...)
	expired = append(expired, listArchiveStoreRecords(ctx, store, "expired", now.Add(-30*24*time.Hour))...)
	active = uniqueArchiveRecords(active)
	rejected = uniqueArchiveRecords(rejected)
	expired = uniqueArchiveRecords(expired)
	sortArchiveRecords(active)
	sortArchiveRecords(rejected)
	sortArchiveRecords(expired)
	return active, rejected, expired
}

func listArchiveStoreRecords(ctx context.Context, store datastore.Store, bucket string, since time.Time) []archiveCAPRecord {
	rows, err := store.ListCAPArchives(ctx, bucket, since)
	if err != nil {
		return nil
	}
	records := make([]archiveCAPRecord, 0, len(rows))
	for _, row := range rows {
		alert, err := capmodel.ParseCAP([]byte(row.RawXML))
		if err != nil || alert.Identifier == "" {
			continue
		}
		updated := row.UpdatedAt
		if updated.IsZero() {
			updated = row.StoredAt
		}
		records = append(records, archiveCAPRecord{
			ID:                 fallbackString(row.AlertID, alert.Identifier),
			FeedID:             row.FeedID,
			Status:             fallbackString(row.Status, bucket),
			Reason:             row.Reason,
			UpdatedAt:          updated,
			Alert:              alert,
			RawXML:             row.RawXML,
			CanonicalLocations: archiveCanonicalLocations(row.Metadata),
		})
	}
	return records
}

func archiveCanonicalLocations(metadata map[string]any) []alertmodel.LocationReference {
	if metadata == nil || metadata["canonical_locations"] == nil {
		return nil
	}
	raw, err := json.Marshal(metadata["canonical_locations"])
	if err != nil {
		return nil
	}
	var locations []alertmodel.LocationReference
	if err := json.Unmarshal(raw, &locations); err != nil {
		return nil
	}
	return locations
}

func uniqueArchiveRecords(records []archiveCAPRecord) []archiveCAPRecord {
	seen := map[string]struct{}{}
	out := records[:0]
	for _, record := range records {
		key := strings.Join([]string{record.Status, record.FeedID, fallbackString(record.ID, record.Alert.Identifier)}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, record)
	}
	return out
}

func handleAlertsArchiveAction(configPath string, payload map[string]any) (map[string]any, error) {
	action := strings.ToLower(strings.TrimSpace(stringValue(payload, "action")))
	switch action {
	case "rebroadcast", "force_broadcast", "rebroadcast_without_same", "force_broadcast_without_same":
		withSAME := action == "rebroadcast" || action == "force_broadcast"
		force := strings.HasPrefix(action, "force_broadcast")
		return rebroadcastArchivedAlert(configPath, payload, withSAME, force)
	case "preview_same":
		return previewArchivedAlertSAME(configPath, payload)
	case "delete":
		id := strings.TrimSpace(stringValue(payload, "id"))
		feedID := strings.TrimSpace(stringValue(payload, "feed_id"))
		if id == "" {
			return nil, fmt.Errorf("alert id is required")
		}
		if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
			return store.DeleteCAPArchive(ctx, id, feedID)
		}); err != nil {
			return nil, err
		}
		deleteQueuedAlertMaterial(filepath.Dir(filepath.Clean(configPath)), id, feedID)
		publishAlertArchiveInvalidated(id, feedID)
		return map[string]any{"deleted": true}, nil
	case "clear_expired":
		if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
			return store.ClearCAPArchiveBucket(ctx, "expired")
		}); err != nil {
			return nil, err
		}
		publishAlertArchiveInvalidated("", "*")
		return map[string]any{"cleared": "expired"}, nil
	case "clear_all":
		if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
			return store.ClearAllCAPArchives(ctx)
		}); err != nil {
			return nil, err
		}
		clearQueuedAlertMaterial(filepath.Dir(filepath.Clean(configPath)))
		publishAlertArchiveInvalidated("", "*")
		return map[string]any{"cleared": "all"}, nil
	case "expire_all":
		if err := withArchiveStore(configPath, func(ctx context.Context, store datastore.Store) error {
			return store.ExpireNonCriticalCAPArchives(ctx)
		}); err != nil {
			return nil, err
		}
		publishAlertArchiveInvalidated("", "*")
		return map[string]any{"expired": "non-critical"}, nil
	default:
		return nil, fmt.Errorf("unsupported alert archive action %q", action)
	}
}

func publishAlertArchiveInvalidated(alertID string, feedID string) {
	clearBannerArchiveCache()
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return
	}
	feedID = strings.TrimSpace(feedID)
	if feedID == "" {
		feedID = "*"
	}
	data := map[string]any{
		"feed_id":    feedID,
		"package_id": "alerts",
		"renderable": false,
	}
	if alertID != "" {
		data["alert_id"] = alertID
		data["alert_ids"] = []string{alertID}
	}
	publisher := events.NewHostBridgePublisher(bridgeAddr)
	defer publisher.Close()
	_ = publisher.Publish(events.Event{
		Type:    "cap.alert.cancelled",
		Source:  "haze-web",
		Subject: alertID,
		Data:    data,
	})
	_ = publisher.Publish(events.Event{
		Type:    "cap.alert.registry.updated",
		Source:  "haze-web",
		Subject: alertID,
		Data:    data,
	})
}

func withArchiveStore(configPath string, fn func(context.Context, datastore.Store) error) error {
	config, err := LoadConfig(configPath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := datastore.Open(ctx, config.Storage, filepath.Dir(filepath.Clean(configPath)))
	if err != nil {
		return err
	}
	defer store.Close()
	if err := fn(ctx, store); err != nil {
		return err
	}
	return nil
}

func archiveRecordsPayload(records []archiveCAPRecord, bucket string, baseDir string) []map[string]any {
	queueItems, _ := loadAlertQueueItems(filepath.Join(baseDir, "config.yaml"))
	out := make([]map[string]any, 0, len(records))
	for _, record := range records {
		out = append(out, archiveRecordPayloadWithQueue(record, bucket, baseDir, queueItems))
	}
	return out
}

func archiveRecordPayload(record archiveCAPRecord, bucket string, baseDir string) map[string]any {
	queueItems, _ := loadAlertQueueItems(filepath.Join(baseDir, "config.yaml"))
	return archiveRecordPayloadWithQueue(record, bucket, baseDir, queueItems)
}

func archiveRecordPayloadWithQueue(record archiveCAPRecord, bucket string, baseDir string, queueItems []sameQueueItem) map[string]any {
	info := chooseArchiveInfo(record.Alert)
	audio := archiveBroadcastAudio(record.Alert)
	resolution := capsame.ResolveEvent(record.Alert, info, baseDir)
	relayed := archiveRecordRelayed(record, queueItems)
	areas := archiveAreaNames(info)
	message := alerttext.BuildCAPAlertText(alerttext.CAPMessageRequest{
		Alert:     record.Alert,
		Info:      info,
		AreaText:  alerttext.JoinParts(areas),
		EventName: alerttext.AlertSubject(info),
		Now:       time.Now().UTC(),
	})
	visual := alerttext.PickBannerGradient([]alerttext.AlertVisualInput{{
		Severity: info.Severity,
		Event:    strings.Join([]string{info.Event, info.Headline, alerttext.AlertSubject(info), message}, " "),
	}})
	return map[string]any{
		"id":                     fallbackString(record.ID, record.Alert.Identifier),
		"feed_id":                record.FeedID,
		"bucket":                 bucket,
		"status":                 fallbackString(record.Status, bucket),
		"reason":                 record.Reason,
		"updated_at":             record.UpdatedAt,
		"sender":                 record.Alert.Sender,
		"sent":                   record.Alert.Sent,
		"message_type":           record.Alert.MessageType,
		"headline":               alerttext.NormalizeHeadline(fallbackString(info.Headline, info.Event)),
		"event":                  info.Event,
		"severity":               info.Severity,
		"urgency":                info.Urgency,
		"certainty":              info.Certainty,
		"effective":              info.Effective,
		"onset":                  info.Onset,
		"expires":                info.Expires,
		"description":            info.Description,
		"instruction":            info.Instruction,
		"areas":                  areas,
		"area_text":              alerttext.JoinParts(areas),
		"audio_url":              audio.URL,
		"audio_mime_type":        audio.MimeType,
		"cap_xml_url":            archiveCAPXMLURL(record),
		"relayed":                relayed,
		"broadcast_action_label": archiveBroadcastActionLabel(relayed),
		"same_preview_available": archiveSAMEPreviewAvailable(record.Alert, baseDir),
		"same_event":             resolution.Event,
		"same_event_source":      resolution.Source,
		"same_event_reason":      resolution.Reason,
		"same_event_confidence":  resolution.Confidence,
		"same_alert_class":       resolution.AlertClass,
		"same_event_phenomenon":  resolution.Phenomenon,
		"same_event_evidence":    resolution.Evidence,
		"message":                message,
		"background_color":       visual[0],
		"background_gradient":    visual,
	}
}

func archiveRecordOriginator(record archiveCAPRecord) string {
	info := chooseArchiveInfo(record.Alert)
	candidates := append([]capmodel.AlertInfo{info}, record.Alert.Infos...)
	for _, candidate := range candidates {
		for _, parameter := range candidate.Parameters {
			if !strings.EqualFold(strings.TrimSpace(parameter.Name), "eas-org") {
				continue
			}
			code := strings.ToUpper(strings.TrimSpace(parameter.Value))
			if _, ok := allowedOriginatorCodes[code]; ok {
				return code
			}
		}
	}
	for _, candidate := range candidates {
		for _, parameter := range candidate.Parameters {
			name := strings.ToLower(strings.TrimSpace(parameter.Name))
			if strings.HasPrefix(name, "layer:ec-msc-smc") || strings.Contains(name, "cap-cp") {
				return "WXR"
			}
		}
	}
	sender := strings.ToLower(strings.TrimSpace(record.Alert.Sender))
	if strings.Contains(sender, "canada") || strings.Contains(sender, "cap-pac") ||
		strings.Contains(sender, "weather.gov") || strings.Contains(sender, "nws") || strings.Contains(sender, "noaa") {
		return "WXR"
	}
	return "CIV"
}

func archiveRecordEvent(configPath string, record archiveCAPRecord) string {
	info := chooseArchiveInfo(record.Alert)
	resolution := capsame.ResolveEvent(record.Alert, info, filepath.Dir(filepath.Clean(configPath)))
	return strings.ToUpper(strings.TrimSpace(resolution.Event))
}

func archiveBroadcastActionLabel(relayed bool) string {
	if relayed {
		return "Rebroadcast"
	}
	return "Force Broadcast"
}

func archiveRecordRelayed(record archiveCAPRecord, items []sameQueueItem) bool {
	id := fallbackString(record.ID, record.Alert.Identifier)
	if id == "" {
		return false
	}
	for _, item := range items {
		if !queueItemMatchesArchiveRecord(item, id, record.FeedID) {
			continue
		}
		return true
	}
	return false
}

func queueItemMatchesArchiveRecord(item sameQueueItem, alertID string, feedID string) bool {
	if strings.TrimSpace(feedID) != "" && !itemTargetsFeed(item, feedID) {
		return false
	}
	if strings.TrimSpace(item.AlertID) == alertID {
		return true
	}
	if item.AlertPacket != nil && strings.TrimSpace(item.AlertPacket.ID) == alertID {
		return true
	}
	safeAlertID := safeID(alertID)
	if safeAlertID == "" {
		return false
	}
	if strings.Contains(strings.TrimSpace(item.ID), safeAlertID) {
		return true
	}
	if strings.Contains(strings.TrimSpace(item.ManifestPath), safeAlertID) {
		return true
	}
	if item.AlertPacket != nil && strings.Contains(strings.TrimSpace(item.AlertPacket.ID), safeAlertID) {
		return true
	}
	return false
}

func archiveCAPXMLURL(record archiveCAPRecord) string {
	id := fallbackString(record.ID, record.Alert.Identifier)
	if id == "" {
		return ""
	}
	query := url.Values{}
	query.Set("id", id)
	if record.FeedID != "" {
		query.Set("feed_id", record.FeedID)
	}
	return "/api/v1/alerts/archive/cap.xml?" + query.Encode()
}

func rebroadcastArchivedAlert(configPath string, payload map[string]any, withSAME bool, force bool) (map[string]any, error) {
	id := strings.TrimSpace(stringValue(payload, "id"))
	feedID := strings.TrimSpace(stringValue(payload, "feed_id"))
	if id == "" || feedID == "" {
		return nil, fmt.Errorf("alert id and feed_id are required")
	}
	record, ok := findArchiveAlert(configPath, id, feedID)
	if !ok {
		return nil, fmt.Errorf("alert %s was not found", id)
	}
	data, withSAME, audio := archiveRebroadcastEventData(configPath, record, withSAME, force, time.Now().UTC())
	data = applyArchiveAccountPolicy(data, payload, withSAME)
	if withSAME {
		if strings.TrimSpace(stringPayload(data, "same_event", "")) == "" || len(stringSlicePayload(data, "same_locations")) == 0 {
			return nil, fmt.Errorf("alert cannot be mapped to SAME cleanly")
		}
	}
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return nil, fmt.Errorf("event bridge is not available")
	}
	publisher := events.NewHostBridgePublisher(bridgeAddr)
	defer publisher.Close()
	if err := publisher.Publish(events.Event{
		Type:    "cap.alert.broadcast.requested",
		Source:  "haze-web",
		Subject: id,
		Data:    data,
	}); err != nil {
		return nil, err
	}
	return map[string]any{
		"accepted": true, "same": withSAME, "audio_url": audio.URL,
		"alert_id": id, "feed_id": feedID,
		"event_type": firstNonBlank(stringValue(data, "same_event"), stringValue(data, "event")),
		"originator": firstNonBlank(stringValue(data, "same_originator"), stringValue(data, "originator")),
		"sender_id":  firstNonBlank(stringValue(data, "same_callsign"), stringValue(data, "sender_id")),
	}, nil
}

func applyArchiveAccountPolicy(data map[string]any, policy map[string]any, withSAME bool) map[string]any {
	if data == nil {
		data = map[string]any{}
	}
	delete(data, "alert_packet")
	for _, key := range []string{"originated_by_user_id", "originated_by_username", "originated_by_session_id", "originated_from_ip"} {
		if value, ok := policy[key]; ok {
			data[key] = value
		}
	}
	originator := firstNonBlank(stringValue(policy, "same_originator"), stringValue(policy, "originator"))
	if originator != "" {
		data["originator"] = originator
		if withSAME {
			data["same_originator"] = originator
		}
	}
	originatorName := firstNonBlank(stringValue(policy, "same_originator_name"), stringValue(policy, "originator_name"))
	if originatorName != "" {
		data["originator_name"] = originatorName
		if withSAME {
			data["same_originator_name"] = originatorName
		}
		for _, key := range []string{"alert_text", "description", "instruction", "banner_text"} {
			if text := stringValue(data, key); text != "" {
				data[key] = replaceBroadcastOriginatorName(text, originatorName)
			}
		}
	}
	if withSAME {
		senderID := firstNonBlank(stringValue(policy, "same_callsign"), stringValue(policy, "sender_id"))
		if senderID != "" {
			data["sender_id"] = senderID
			data["same_callsign"] = senderID
		}
	}
	packet := alertmodel.LegacyFromMap(data)
	packet.Meta = map[string]any{
		"originated_by_user_id":    data["originated_by_user_id"],
		"originated_by_username":   data["originated_by_username"],
		"originated_by_session_id": data["originated_by_session_id"],
		"originated_from_ip":       data["originated_from_ip"],
	}
	return alertmodel.WithLegacyFields(packet, data)
}

func archiveRebroadcastEventData(configPath string, record archiveCAPRecord, withSAME bool, force bool, now time.Time) (map[string]any, bool, archiveAudio) {
	id := fallbackString(record.ID, record.Alert.Identifier)
	feedID := strings.TrimSpace(record.FeedID)
	info := chooseArchiveInfo(record.Alert)
	areas := archiveAreaNames(info)
	audio := archiveBroadcastAudio(record.Alert)
	message := alerttext.BuildCAPAlertText(alerttext.CAPMessageRequest{
		Alert:     record.Alert,
		Info:      info,
		AreaText:  alerttext.JoinParts(areas),
		EventName: alerttext.AlertSubject(info),
		Now:       now,
	})
	visual := alerttext.PickBannerGradient([]alerttext.AlertVisualInput{{
		Severity: info.Severity,
		Event:    strings.Join([]string{info.Event, info.Headline, alerttext.AlertSubject(info), message}, " "),
	}})
	if withSAME && !force && !archiveSAMEAllowed(configPath, record.Alert, now) {
		withSAME = false
	}
	data := map[string]any{
		"feed_id":          feedID,
		"feed_ids":         []string{feedID},
		"alert_id":         id,
		"id":               id,
		"package_id":       "alerts",
		"title":            archiveAlertTitle(record.Alert),
		"rebroadcast":      true,
		"force":            force,
		"force_broadcast":  force,
		"include_same":     withSAME,
		"message_type":     record.Alert.MessageType,
		"sender":           record.Alert.Sender,
		"event":            info.Event,
		"headline":         info.Headline,
		"severity":         info.Severity,
		"urgency":          info.Urgency,
		"certainty":        info.Certainty,
		"description":      info.Description,
		"instruction":      info.Instruction,
		"area_names":       areas,
		"area_text":        alerttext.JoinParts(areas),
		"alert_text":       message,
		"banner_text":      archiveBannerText(record, info, message),
		"background_color": visual[0],
	}
	if record.Alert.Sent != "" {
		data["alert_sent_at"] = record.Alert.Sent
		data["same_sent_at"] = record.Alert.Sent
	}
	if begins := firstNonBlank(info.Onset, info.Effective); begins != "" {
		data["alert_begins_at"] = begins
		data["same_begins_at"] = begins
	}
	if info.Expires != "" {
		data["alert_expires_at"] = info.Expires
		data["same_expires_at"] = info.Expires
	}
	if audio.URL != "" {
		data["audio_url"] = audio.URL
		data["audio_mime_type"] = audio.MimeType
		data["audio_authoritative"] = true
	}
	if withSAME {
		baseDir := filepath.Dir(filepath.Clean(configPath))
		resolution := capsame.ResolveEvent(record.Alert, info, baseDir)
		locations := sameLocationsFromCAP(info)
		data["same_event"] = resolution.Event
		data["same_event_name"] = alerttext.EventName(configPath, resolution.Event)
		data["same_event_source"] = resolution.Source
		data["same_event_reason"] = resolution.Reason
		data["same_event_confidence"] = resolution.Confidence
		data["same_alert_class"] = resolution.AlertClass
		data["same_event_phenomenon"] = resolution.Phenomenon
		data["same_locations"] = locations
		data["locations"] = locations
		data["same_duration"] = sameDurationFromCAP(info)
		data["same_originator"] = "WXR"
		data["same_originator_name"] = "The National Weather Service or Environment Canada"
		data["same_weather_service"] = record.Alert.Sender
		data["same_callsign"] = sameCallsignFromConfig(configPath, feedID)
		data["same_tone"] = "WXR"
	}
	packet, _ := alertmodel.FromMap(data)
	return alertmodel.WithLegacyFields(packet, data), withSAME, audio
}

func archiveBannerText(record archiveCAPRecord, info capmodel.AlertInfo, fallback string) string {
	if text := strings.TrimSpace(record.BannerText); text != "" {
		return text
	}
	parts := []string{}
	if description := strings.TrimSpace(info.Description); description != "" {
		parts = append(parts, alerttext.CleanAlertText(description))
	}
	if instruction := strings.TrimSpace(info.Instruction); instruction != "" {
		parts = append(parts, alerttext.CleanAlertText(instruction))
	}
	if len(parts) == 0 {
		return strings.TrimSpace(fallback)
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

func archiveSAMEAllowed(configPath string, alert capmodel.Alert, now time.Time) bool {
	if archiveAlertExpired(alert, now) {
		return false
	}
	info := chooseArchiveInfo(alert)
	event := capsame.ResolveEvent(alert, info, filepath.Dir(filepath.Clean(configPath))).Event
	if event == "" {
		return false
	}
	anchor := firstArchiveTime(alert)
	if anchor.IsZero() || now.IsZero() || now.Before(anchor) {
		return true
	}
	limit := time.Hour
	switch strings.ToUpper(strings.TrimSpace(event)) {
	case "SVR", "TOR":
		limit = 30 * time.Minute
	}
	return now.Sub(anchor) <= limit
}

func archiveSAMEPreviewAvailable(alert capmodel.Alert, baseDir string) bool {
	if strings.EqualFold(alert.MessageType, "Cancel") {
		return false
	}
	info := chooseArchiveInfo(alert)
	return capsame.ResolveEvent(alert, info, baseDir).Event != "" && len(sameLocationsFromCAP(info)) > 0
}

func previewArchivedAlertSAME(configPath string, payload map[string]any) (map[string]any, error) {
	id := strings.TrimSpace(stringValue(payload, "id"))
	feedID := strings.TrimSpace(stringValue(payload, "feed_id"))
	if id == "" {
		return nil, fmt.Errorf("alert id is required")
	}
	record, ok := findArchiveAlert(configPath, id, feedID)
	if !ok {
		return nil, fmt.Errorf("alert %s was not found", id)
	}
	info := chooseArchiveInfo(record.Alert)
	event := capsame.ResolveEvent(record.Alert, info, filepath.Dir(filepath.Clean(configPath))).Event
	locations := sameLocationsFromCAP(info)
	if event == "" || len(locations) == 0 {
		return nil, fmt.Errorf("alert cannot be mapped to SAME cleanly")
	}
	request := sameGenerateRequest{
		Originator:     firstNonBlank(stringValue(payload, "same_originator"), stringValue(payload, "originator"), "WXR"),
		OriginatorName: firstNonBlank(stringValue(payload, "same_originator_name"), stringValue(payload, "originator_name")),
		Event:          event,
		Locations:      locations,
		Duration:       sameDurationFromCAP(info),
		Callsign:       firstNonBlank(stringValue(payload, "same_callsign"), stringValue(payload, "sender_id"), sameCallsignFromConfig(configPath, record.FeedID)),
		Tone:           "WXR",
	}
	result, err := runSameGenerator(configPath, request)
	if err != nil {
		return nil, err
	}
	audioBase64 := strings.TrimSpace(stringPayload(result, "audio_base64", ""))
	if audioBase64 == "" {
		return nil, fmt.Errorf("SAME generator returned no audio payload")
	}
	pcm, err := base64.StdEncoding.DecodeString(audioBase64)
	if err != nil {
		return nil, fmt.Errorf("decode SAME preview audio: %w", err)
	}
	sampleRate := intPayload(result, "sample_rate", 48000)
	channels := intPayload(result, "channels", 1)
	wav := wavFromPCM16(pcm, sampleRate, channels)
	capAudio := archiveBroadcastAudio(record.Alert)
	return map[string]any{
		"same_audio_wav_base64": base64.StdEncoding.EncodeToString(wav),
		"same_audio_mime_type":  "audio/wav",
		"sample_rate":           sampleRate,
		"channels":              channels,
		"header":                stringPayload(result, "header", ""),
		"event":                 event,
		"locations":             locations,
		"audio_url":             capAudio.URL,
		"audio_mime_type":       capAudio.MimeType,
	}, nil
}

func queueArchiveSAME(configPath string, record archiveCAPRecord) (sameQueueItem, error) {
	info := chooseArchiveInfo(record.Alert)
	event := capsame.ResolveEvent(record.Alert, info, filepath.Dir(filepath.Clean(configPath))).Event
	locations := sameLocationsFromCAP(info)
	if event == "" || len(locations) == 0 {
		return sameQueueItem{}, fmt.Errorf("alert cannot be mapped to SAME cleanly")
	}
	request := sameGenerateRequest{
		Originator: "WXR",
		Event:      event,
		Locations:  locations,
		Duration:   sameDurationFromCAP(info),
		Callsign:   sameCallsignFromConfig(configPath, record.FeedID),
		Tone:       "WXR",
	}
	result, err := runSameGenerator(configPath, request)
	if err != nil {
		return sameQueueItem{}, err
	}
	return persistSameQueueItemWithID(configPath, "000_"+safeID(record.FeedID+"_"+record.ID+"_same"), request, []string{record.FeedID}, result, "")
}

func findArchiveAlert(configPath string, id string, feedID string) (archiveCAPRecord, bool) {
	active, rejected, expired := archiveStoreRecordBuckets(configPath, time.Now().UTC())
	for _, bucket := range [][]archiveCAPRecord{active, expired, rejected} {
		for _, record := range bucket {
			if (record.ID == id || record.Alert.Identifier == id) && (feedID == "" || record.FeedID == feedID) {
				if record.FeedID == "" {
					record.FeedID = feedID
				}
				return record, true
			}
		}
	}
	return archiveCAPRecord{}, false
}

func deleteQueuedAlertMaterial(baseDir string, id string, feedID string) {
	queuePath := filepath.Join(baseDir, filepath.FromSlash(alertQueueDir))
	entries, err := os.ReadDir(queuePath)
	if err != nil {
		return
	}
	safeAlertID := safeID(id)
	safeFeedID := safeID(feedID)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			continue
		}
		manifestPath := filepath.Join(queuePath, entry.Name())
		raw, err := os.ReadFile(manifestPath)
		if err != nil {
			continue
		}
		var manifest map[string]any
		if err := json.Unmarshal(raw, &manifest); err != nil {
			continue
		}
		if !queuedAlertMatches(manifest, entry.Name(), id, feedID, safeAlertID, safeFeedID) {
			continue
		}
		if audioRel := strings.TrimSpace(fmt.Sprint(manifest["audio_path"])); audioRel != "" && audioRel != "<nil>" {
			_ = os.Remove(resolveConfigPath(filepath.Join(baseDir, "config.yaml"), audioRel))
		}
		_ = os.Remove(manifestPath)
	}
}

func clearQueuedAlertMaterial(baseDir string) {
	for _, dir := range []string{
		filepath.Join(baseDir, "runtime", "queues", "alerts"),
		filepath.Join(baseDir, "runtime", "audio", "alerts"),
	} {
		_ = os.RemoveAll(dir)
		_ = os.MkdirAll(dir, 0o755)
	}
}

func queuedAlertMatches(manifest map[string]any, filename string, id string, feedID string, safeAlertID string, safeFeedID string) bool {
	if feedID != "" {
		targeted := false
		for _, value := range anySlice(manifest["feed_ids"]) {
			if strings.TrimSpace(fmt.Sprint(value)) == feedID {
				targeted = true
				break
			}
		}
		if !targeted && strings.TrimSpace(fmt.Sprint(manifest["feed_id"])) != feedID {
			return false
		}
	}
	for _, key := range []string{"alert_id", "id", "subject"} {
		if strings.TrimSpace(fmt.Sprint(manifest[key])) == id {
			return true
		}
	}
	name := strings.TrimSuffix(filename, filepath.Ext(filename))
	if safeAlertID == "" {
		return false
	}
	if safeFeedID != "" && strings.Contains(name, safeFeedID) && strings.Contains(name, safeAlertID) {
		return true
	}
	return strings.Contains(name, safeAlertID)
}

func chooseArchiveInfo(alert capmodel.Alert) capmodel.AlertInfo {
	for _, info := range alert.Infos {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(info.Language)), "en") {
			return info
		}
	}
	if len(alert.Infos) > 0 {
		return alert.Infos[0]
	}
	return capmodel.AlertInfo{}
}

type archiveAudio struct {
	URL      string
	MimeType string
}

func archiveBroadcastAudio(alert capmodel.Alert) archiveAudio {
	for _, info := range alert.Infos {
		for _, resource := range info.Resources {
			mimeType := strings.ToLower(strings.TrimSpace(resource.MimeType))
			desc := strings.ToLower(strings.TrimSpace(resource.Description))
			if !strings.HasPrefix(mimeType, "audio/") && !strings.Contains(desc, "audio") {
				continue
			}
			url := strings.TrimSpace(resource.URI)
			if url == "" {
				url = strings.TrimSpace(resource.DerefURI)
			}
			if archiveAudioURLAllowed(url) {
				return archiveAudio{URL: url, MimeType: resource.MimeType}
			}
		}
	}
	return archiveAudio{}
}

func archiveAudioURLAllowed(rawURL string) bool {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https":
		return parsed.Host != ""
	default:
		return false
	}
}

func archiveAreaNames(info capmodel.AlertInfo) []string {
	out := []string{}
	for _, area := range info.Areas {
		if capmodel.IsECCCThreatArea(area) {
			continue
		}
		if text := strings.TrimSpace(area.Description); text != "" {
			out = append(out, text)
		}
	}
	return uniqueStrings(out)
}

func archiveAlertTitle(alert capmodel.Alert) string {
	info := chooseArchiveInfo(alert)
	return fallbackString(info.Headline, info.Event, "Weather Alert")
}

func sameLocationsFromCAP(info capmodel.AlertInfo) []string {
	out := []string{}
	for _, area := range info.Areas {
		if capmodel.IsECCCThreatArea(area) {
			continue
		}
		for _, geocode := range area.Geocodes {
			value := strings.TrimSpace(geocode.Value)
			if value != "" {
				out = append(out, value)
			}
		}
	}
	return uniqueStrings(out)
}

func sameDurationFromCAP(info capmodel.AlertInfo) string {
	expires := parseArchiveTime(info.Expires)
	if expires.IsZero() {
		return "0015"
	}
	minutes := int(time.Until(expires).Minutes())
	if minutes < 15 {
		minutes = 15
	}
	if minutes > 6*60 {
		minutes = 6 * 60
	}
	return fmt.Sprintf("%02d%02d", minutes/60, minutes%60)
}

func archiveAlertExpired(alert capmodel.Alert, now time.Time) bool {
	if strings.EqualFold(alert.MessageType, "Cancel") {
		return true
	}
	info := chooseArchiveInfo(alert)
	for _, response := range info.Response {
		if strings.EqualFold(response, "AllClear") {
			return true
		}
	}
	if strings.Contains(strings.ToLower(info.Headline), "ended") {
		return true
	}
	expires := parseArchiveTime(info.Expires)
	return !expires.IsZero() && now.After(expires)
}

func isRealTornadoOrSevereThunderstorm(alert capmodel.Alert) bool {
	if !strings.EqualFold(alert.Status, "Actual") {
		return false
	}
	info := chooseArchiveInfo(alert)
	haystack := strings.ToLower(strings.Join([]string{info.Event, info.Headline}, " "))
	if !strings.Contains(haystack, "warning") {
		return false
	}
	return strings.Contains(haystack, "tornado") || strings.Contains(haystack, "severe thunderstorm")
}

func expiredWithin(records []archiveCAPRecord, now time.Time, window time.Duration) []archiveCAPRecord {
	cutoff := now.Add(-window)
	out := records[:0]
	seen := map[string]struct{}{}
	for _, record := range records {
		if !record.UpdatedAt.IsZero() && record.UpdatedAt.Before(cutoff) {
			continue
		}
		key := record.FeedID + "\x00" + fallbackString(record.ID, record.Alert.Identifier)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, record)
	}
	return out
}

func sortArchiveRecords(records []archiveCAPRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].UpdatedAt.After(records[j].UpdatedAt)
	})
}

func firstArchiveTime(alert capmodel.Alert) time.Time {
	for _, raw := range []string{alert.Sent, chooseArchiveInfo(alert).Effective, chooseArchiveInfo(alert).Onset, chooseArchiveInfo(alert).Expires} {
		if parsed := parseArchiveTime(raw); !parsed.IsZero() {
			return parsed
		}
	}
	return time.Now().UTC()
}

func parseArchiveTime(raw string) time.Time {
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw)); err == nil {
		return parsed
	}
	return time.Time{}
}

func fallbackString(values ...string) string {
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			return text
		}
	}
	return ""
}

func safeID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var builder strings.Builder
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			builder.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		case ch == '-' || ch == '_' || ch == '.':
			builder.WriteRune(ch)
		default:
			builder.WriteRune('_')
		}
	}
	return strings.Trim(builder.String(), "._-")
}
