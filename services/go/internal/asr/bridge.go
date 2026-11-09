package asr

import (
	"context"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/bridgeclient"
)

type eventPublisher interface {
	Publish(map[string]any) error
}

type bridgeClient struct {
	bridge *bridgeclient.Conn
	events chan map[string]any
}

func connectBridge(ctx context.Context, address string) (*bridgeClient, error) {
	bridge, err := bridgeclient.Dial(ctx, bridgeclient.Options{Addr: address, WriteTimeout: 3 * time.Second})
	if err != nil {
		return nil, err
	}
	client := &bridgeClient{bridge: bridge, events: make(chan map[string]any, 128)}
	go client.readLoop()
	return client, nil
}

func (c *bridgeClient) Close() error {
	if c == nil {
		return nil
	}
	return c.bridge.Close()
}

func (c *bridgeClient) Events() <-chan map[string]any { return c.events }

func (c *bridgeClient) Publish(message map[string]any) error {
	return c.bridge.Publish(message)
}

func (c *bridgeClient) readLoop() {
	defer close(c.events)
	_ = c.bridge.ReadMessages(func(message map[string]any) bool {
		if stringAt(message, "type") != "asr.transcribe" && stringAt(message, "type") != "system.shutdown" {
			return true
		}
		select {
		case c.events <- message:
		default:
			data := mapAt(message, "data")
			_ = c.Publish(failedEvent(firstText(message, data, "request_id", "subject"), "busy", true))
		}
		return true
	})
}
