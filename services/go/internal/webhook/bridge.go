package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/meowraii/haze-weather-radio/services/go/internal/bridgeclient"
)

const alertAudioReady = "cap.alert.audio.ready"

// connectBridge subscribes to alert-audio events only. Without a subscription
// the broker sends every event on the bus (playout PCM, status).
func connectBridge(ctx context.Context, addr string) (*bridgeclient.Conn, error) {
	return bridgeclient.Dial(ctx, bridgeclient.Options{
		Addr: addr,
		Register: &bridgeclient.Registration{
			ClientID:      serviceID,
			ReceiveEvents: true,
			Subscriptions: []string{alertAudioReady},
		},
	})
}

func stringAt(source map[string]any, key string) string {
	if source == nil {
		return ""
	}
	switch value := source[key].(type) {
	case string:
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func intAt(source map[string]any, key string, fallback int) int {
	if source == nil {
		return fallback
	}
	switch value := source[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case string:
		var parsed int
		if _, err := fmt.Sscanf(strings.TrimSpace(value), "%d", &parsed); err == nil {
			return parsed
		}
	}
	return fallback
}

func boolAt(source map[string]any, key string, fallback bool) bool {
	if source == nil {
		return fallback
	}
	switch value := source[key].(type) {
	case bool:
		return value
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "on", "enabled":
			return true
		case "0", "false", "no", "off", "disabled":
			return false
		}
	}
	return fallback
}

func mapAt(source map[string]any, key string) map[string]any {
	if source == nil {
		return nil
	}
	if value, ok := source[key].(map[string]any); ok {
		return value
	}
	return nil
}

func firstText(message map[string]any, data map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringAt(message, key); value != "" {
			return value
		}
		if value := stringAt(data, key); value != "" {
			return value
		}
	}
	return ""
}
