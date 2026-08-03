package webgateway

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
)

const bannerBridgeReconnectDelay = 750 * time.Millisecond

type BannerHub struct {
	configPath string
	addr       string
	mu         sync.Mutex
	onAir      map[string]bannerOnAirAlert
}

type bannerOnAirAlert struct {
	FeedID             string
	AlertID            string
	QueueID            string
	Event              string
	Header             string
	AlertText          string
	BannerText         string
	BroadcastImmediate bool
	CanonicalLocations []alertmodel.LocationReference
	ExpiresAt          time.Time
	UpdatedAt          time.Time
}

func NewBannerHub(configPath string, addr string) *BannerHub {
	hub := &BannerHub{
		configPath: configPath,
		addr:       strings.TrimSpace(addr),
		onAir:      map[string]bannerOnAirAlert{},
	}
	if hub.addr != "" {
		go hub.run(context.Background())
	}
	return hub
}

func (h *BannerHub) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := net.DialTimeout("tcp", h.addr, 3*time.Second)
		if err != nil {
			sleepBanner(ctx, bannerBridgeReconnectDelay)
			continue
		}
		h.readBridge(ctx, conn)
		_ = conn.Close()
		sleepBanner(ctx, bannerBridgeReconnectDelay)
	}
}

func (h *BannerHub) readBridge(ctx context.Context, conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}
		h.handleEvent(scanner.Bytes(), time.Now().UTC())
	}
}

func (h *BannerHub) handleEvent(raw []byte, now time.Time) {
	var event struct {
		Type    string   `json:"type"`
		FeedID  string   `json:"feed_id"`
		FeedIDs []string `json:"feed_ids"`
		QueueID string   `json:"queue_id"`
		Header  string   `json:"header"`
		Event   string   `json:"event"`
		Data    struct {
			FeedID             string             `json:"feed_id"`
			FeedIDs            []string           `json:"feed_ids"`
			QueueID            string             `json:"queue_id"`
			Header             string             `json:"header"`
			Event              string             `json:"event"`
			AlertID            string             `json:"alert_id"`
			Title              string             `json:"title"`
			AlertText          string             `json:"alert_text"`
			BannerText         string             `json:"banner_text"`
			BroadcastImmediate bool               `json:"broadcast_immediate"`
			AlertPacket        *alertmodel.Packet `json:"alert_packet"`
			TTSText            string             `json:"tts_text"`
			Text               string             `json:"text"`
			Message            string             `json:"message"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return
	}
	eventType := strings.TrimSpace(event.Type)
	if !strings.HasPrefix(eventType, "alert.playout.") && eventType != "playout.interrupted" && eventType != "cap.alert.audio.ready" {
		return
	}
	if eventType == "cap.alert.audio.ready" {
		return
	}
	feedIDs := eventFeedIDs(event.FeedID, event.FeedIDs, event.Data.FeedID, event.Data.FeedIDs)
	queueID := fallbackString(event.Data.QueueID, event.QueueID)
	if len(feedIDs) == 0 && strings.TrimSpace(event.Data.FeedID) != "" {
		feedIDs = []string{strings.TrimSpace(event.Data.FeedID)}
	}
	if len(feedIDs) == 0 {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	for _, feedID := range feedIDs {
		switch eventType {
		case "alert.playout.started":
			if active := h.onAir[feedID]; active.QueueID != "" && active.QueueID != queueID && now.Before(active.ExpiresAt) {
				continue
			}
			item := h.queueItem(queueID, feedID)
			packet := event.Data.AlertPacket
			if packet == nil {
				packet = item.AlertPacket
			}
			packetFields := map[string]any{}
			if packet != nil {
				packetCopy := packet.Normalize()
				packetFields = alertmodel.LegacyFields(packetCopy)
			}
			alertID := fallbackString(event.Data.AlertID, item.AlertID, alertIDFromQueueID(queueID, feedID))
			if alertID == "" {
				alertID = packetField(packetFields, "alert_id")
			}
			if alertID == "" {
				alertID = fallbackString(queueID, event.Data.Event, event.Event)
			}
			h.onAir[feedID] = bannerOnAirAlert{
				FeedID:             feedID,
				AlertID:            alertID,
				QueueID:            queueID,
				Event:              fallbackString(packetField(packetFields, "same_event"), packetField(packetFields, "event"), event.Data.Event, event.Event, item.Event),
				Header:             fallbackString(packetField(packetFields, "headline"), packetField(packetFields, "title"), event.Data.Header, event.Header, item.Header, event.Data.Title),
				AlertText:          fallbackString(packetField(packetFields, "alert_text"), event.Data.AlertText, event.Data.TTSText, event.Data.Text, event.Data.Message, item.AlertText),
				BannerText:         fallbackString(packetField(packetFields, "banner_text"), event.Data.BannerText, item.BannerText),
				BroadcastImmediate: packetBool(packetFields, "broadcast_immediate") || event.Data.BroadcastImmediate || item.BroadcastImmediate,
				CanonicalLocations: bannerPacketLocations(packet, item.AlertPacket),
				ExpiresAt:          now.Add(30 * time.Minute),
				UpdatedAt:          now,
			}
		case "alert.playout.completed":
			if feedID == "*" {
				h.onAir = map[string]bannerOnAirAlert{}
				continue
			}
			item := h.queueItem(queueID, feedID)
			active := h.onAir[feedID]
			if active.QueueID != "" && active.QueueID != queueID && active.AlertID != "" && item.AlertID != active.AlertID {
				continue
			}
			if active.QueueID == queueID || bannerQueueItemEndsAlert(item, queueID) {
				delete(h.onAir, feedID)
				delete(h.onAir, "*")
			} else if active.AlertID != "" {
				active.ExpiresAt = now.Add(15 * time.Second)
				active.UpdatedAt = now
				h.onAir[feedID] = active
			}
		case "playout.interrupted":
			if feedID == "*" {
				h.onAir = map[string]bannerOnAirAlert{}
			} else {
				delete(h.onAir, feedID)
				delete(h.onAir, "*")
			}
		}
	}
}

func bannerPacketLocations(packets ...*alertmodel.Packet) []alertmodel.LocationReference {
	for _, packet := range packets {
		if packet == nil {
			continue
		}
		normalized := packet.Normalize()
		if len(normalized.Areas.Locations) > 0 {
			return append([]alertmodel.LocationReference(nil), normalized.Areas.Locations...)
		}
	}
	return nil
}

func (h *BannerHub) queueItem(queueID string, feedID string) sameQueueItem {
	items, err := loadAlertQueueItems(h.configPath)
	if err != nil {
		return sameQueueItem{}
	}
	for _, item := range items {
		if strings.TrimSpace(item.ID) == strings.TrimSpace(queueID) && itemTargetsFeed(item, feedID) {
			return item
		}
	}
	return sameQueueItem{}
}

func (h *BannerHub) Active(feedID string, now time.Time) []bannerOnAirAlert {
	if h == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	out := []bannerOnAirAlert{}
	for key, active := range h.onAir {
		if !active.ExpiresAt.IsZero() && now.After(active.ExpiresAt) {
			delete(h.onAir, key)
			continue
		}
		if strings.TrimSpace(feedID) != "" && strings.TrimSpace(feedID) != "*" && key != feedID && key != "*" {
			continue
		}
		out = append(out, active)
	}
	return out
}

func packetField(fields map[string]any, key string) string {
	if fields == nil {
		return ""
	}
	value, ok := fields[key]
	if !ok {
		return ""
	}
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "<nil>" {
		return ""
	}
	return text
}

func packetBool(fields map[string]any, key string) bool {
	value, ok := fields[key]
	if !ok {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes", "on":
			return true
		}
	}
	return false
}

func eventFeedIDs(values ...any) []string {
	out := []string{}
	seen := map[string]struct{}{}
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, value := range values {
		switch typed := value.(type) {
		case string:
			add(typed)
		case []string:
			for _, item := range typed {
				add(item)
			}
		}
	}
	return out
}

func alertIDFromQueueID(queueID string, feedID string) string {
	queueID = strings.TrimSpace(queueID)
	feedID = strings.TrimSpace(feedID)
	if queueID == "" || feedID == "" {
		return ""
	}
	for _, prefix := range []string{"000_" + feedID + "_", "001_" + feedID + "_", "002_" + feedID + "_"} {
		if strings.HasPrefix(queueID, prefix) {
			value := strings.TrimPrefix(queueID, prefix)
			for _, suffix := range []string{"_same_header", "_same_eom", "_same_full", "_same", "_cap"} {
				value = strings.TrimSuffix(value, suffix)
			}
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func bannerQueueItemEndsAlert(item sameQueueItem, queueID string) bool {
	itemType := strings.ToLower(strings.TrimSpace(item.Type))
	queueID = strings.ToLower(strings.TrimSpace(queueID))
	return strings.Contains(itemType, "eom") ||
		strings.Contains(queueID, "same_eom") ||
		strings.Contains(itemType, "full") ||
		strings.Contains(queueID, "same_full")
}

func sleepBanner(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func logBannerEventDebug(message string) {
	log.Print(message)
}
