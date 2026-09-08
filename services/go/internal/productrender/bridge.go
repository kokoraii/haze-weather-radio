package productrender

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

type bridgeClient struct {
	conn      net.Conn
	mu        sync.Mutex
	events    chan map[string]any
	done      chan struct{}
	closeOnce sync.Once
}

const bridgeEventBufferSize = 128

func connectBridge(ctx context.Context, addr string) (*bridgeClient, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("missing host event bridge address")
	}
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	client := &bridgeClient{
		conn:   conn,
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
		if c.conn != nil {
			err = c.conn.Close()
		}
	})
	return err
}

func (c *bridgeClient) Events() <-chan map[string]any {
	return c.events
}

func (c *bridgeClient) Publish(message map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := message["timestamp"]; !ok {
		message["timestamp"] = time.Now().UTC()
	}
	return json.NewEncoder(c.conn).Encode(message)
}

func (c *bridgeClient) readLoop() {
	defer close(c.events)
	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var message map[string]any
		if err := json.Unmarshal([]byte(line), &message); err != nil {
			continue
		}
		switch stringAt(message, "type") {
		case "cap.alert.received", "product.render.request", "wx.on_demand.request", "lead.config.updated", "system.shutdown":
		default:
			continue
		}
		// Discard unrelated bus traffic, not requests that callers are awaiting.
		// The dispatcher applies bounded admission and explicit busy responses.
		select {
		case c.events <- message:
		case <-c.done:
			return
		}
	}
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
