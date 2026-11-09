package events

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/bridgeclient"
)

const hostBridgeTimeout = 3 * time.Second

// HostBridgePublisher emits events to the Haze loopback event bridge.
type HostBridgePublisher struct {
	addr         string
	dialTimeout  time.Duration
	writeTimeout time.Duration
	mu           sync.Mutex
	conn         *bridgeclient.Conn
}

// NewHostBridgePublisher creates a publisher for the Haze event bridge.
func NewHostBridgePublisher(addr string) *HostBridgePublisher {
	return &HostBridgePublisher{
		addr:         addr,
		dialTimeout:  hostBridgeTimeout,
		writeTimeout: hostBridgeTimeout,
	}
}

// Close releases the current bridge connection.
func (p *HostBridgePublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil {
		return nil
	}
	err := p.conn.Close()
	p.conn = nil
	return err
}

// Publish writes an event to the host bridge, reconnecting once if needed.
func (p *HostBridgePublisher) Publish(event Event) error {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.write(raw); err == nil {
		return nil
	}
	p.closeLocked()
	return p.write(raw)
}

func (p *HostBridgePublisher) write(raw []byte) error {
	if p.conn == nil {
		// A zero Registration is publish-only: the broker will not send this
		// connection any events.
		conn, err := bridgeclient.Dial(context.Background(), bridgeclient.Options{
			Addr:         p.addr,
			DialTimeout:  p.timeout(p.dialTimeout),
			WriteTimeout: p.timeout(p.writeTimeout),
			Register:     &bridgeclient.Registration{},
		})
		if err != nil {
			return err
		}
		p.conn = conn
		go drainBridge(conn.NetConn())
	}
	return p.conn.WriteLine(raw)
}

func (p *HostBridgePublisher) closeLocked() {
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

func (p *HostBridgePublisher) timeout(value time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return hostBridgeTimeout
}

func drainBridge(conn net.Conn) {
	_, _ = io.Copy(io.Discard, conn)
}
