package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/meowraii/haze-weather-radio/services/go/internal/bridgeclient"
)

type bridgeClient struct {
	bridge *bridgeclient.Conn
	events chan map[string]any
}

func connectBridge(ctx context.Context, addr string) (*bridgeClient, error) {
	bridge, err := bridgeclient.Dial(ctx, bridgeclient.Options{Addr: addr})
	if err != nil {
		return nil, err
	}
	client := &bridgeClient{
		bridge: bridge,
		events: make(chan map[string]any, 128),
	}
	go client.readLoop()
	return client, nil
}

func (c *bridgeClient) Close() error {
	if c == nil {
		return nil
	}
	return c.bridge.Close()
}

func (c *bridgeClient) Events() <-chan map[string]any {
	return c.events
}

func (c *bridgeClient) readLoop() {
	defer close(c.events)
	_ = c.bridge.ReadMessages(func(message map[string]any) bool {
		select {
		case c.events <- message:
		default:
		}
		return true
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
