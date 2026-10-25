package productrender

import (
	"context"
	"strings"
	"sync"

	"github.com/meowraii/haze-weather-radio/services/go/internal/bridgeclient"
)

type bridgeClient struct {
	bridge    *bridgeclient.Conn
	events    chan map[string]any
	done      chan struct{}
	closeOnce sync.Once
}

const bridgeEventBufferSize = 128

func connectBridge(ctx context.Context, addr string) (*bridgeClient, error) {
	bridge, err := bridgeclient.Dial(ctx, bridgeclient.Options{Addr: addr})
	if err != nil {
		return nil, err
	}
	client := &bridgeClient{
		bridge: bridge,
		events: make(chan map[string]any, bridgeEventBufferSize),
		done:   make(chan struct{}),
	}
	go client.readLoop()
	return client, nil
}

func (c *bridgeClient) Close() error {
	if c == nil {
		return nil
	}
	var err error
	c.closeOnce.Do(func() {
		if c.done != nil {
			close(c.done)
		}
		err = c.bridge.Close()
	})
	return err
}

func (c *bridgeClient) Events() <-chan map[string]any {
	return c.events
}

func (c *bridgeClient) Publish(message map[string]any) error {
	return c.bridge.Publish(message)
}

func (c *bridgeClient) readLoop() {
	defer close(c.events)
	_ = c.bridge.ReadMessages(func(message map[string]any) bool {
		switch stringAt(message, "type") {
		case "cap.alert.received", "product.render.request", "wx.on_demand.request", "lead.config.updated", "system.shutdown":
		default:
			return true
		}
		// Discard unrelated bus traffic, not requests that callers are awaiting.
		// The dispatcher applies bounded admission and explicit busy responses.
		select {
		case c.events <- message:
			return true
		case <-c.done:
			return false
		}
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

func boolAt(source map[string]any, key string, fallback bool) bool {
	if source == nil {
		return fallback
	}
	switch value := source[key].(type) {
	case bool:
		return value
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
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

func firstValue(message map[string]any, data map[string]any, keys ...string) any {
	for _, key := range keys {
		if message != nil {
			if value, ok := message[key]; ok && value != nil {
				return value
			}
		}
		if data != nil {
			if value, ok := data[key]; ok && value != nil {
				return value
			}
		}
	}
	return nil
}
