package asr

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

type eventPublisher interface {
	Publish(map[string]any) error
}

type bridgeClient struct {
	conn   net.Conn
	events chan map[string]any
	mu     sync.Mutex
}

func connectBridge(ctx context.Context, address string) (*bridgeClient, error) {
	if strings.TrimSpace(address) == "" {
		return nil, fmt.Errorf("missing host event bridge address")
	}
	connection, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	client := &bridgeClient{conn: connection, events: make(chan map[string]any, 128)}
	go client.readLoop()
	return client, nil
}

func (c *bridgeClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *bridgeClient) Events() <-chan map[string]any { return c.events }

func (c *bridgeClient) Publish(message map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := message["timestamp"]; !exists {
		message["timestamp"] = time.Now().UTC()
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	err := json.NewEncoder(c.conn).Encode(message)
	_ = c.conn.SetWriteDeadline(time.Time{})
	return err
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
		if json.Unmarshal([]byte(line), &message) != nil {
			continue
		}
		if stringAt(message, "type") != "asr.transcribe" && stringAt(message, "type") != "system.shutdown" {
			continue
		}
		select {
		case c.events <- message:
		default:
			data := mapAt(message, "data")
			_ = c.Publish(failedEvent(firstText(message, data, "request_id", "subject"), "busy", true))
		}
	}
}
