package events

import (
	"encoding/json"
	"io"
	"net"
	"sync"
	"time"
)

const hostBridgeTimeout = 3 * time.Second
const publisherRegistration = "{\"type\":\"bridge.client\",\"data\":{\"receive_events\":false}}\n"

// HostBridgePublisher emits events to the Haze loopback event bridge.
type HostBridgePublisher struct {
	addr         string
	dialTimeout  time.Duration
	writeTimeout time.Duration
	mu           sync.Mutex
	conn         net.Conn
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
	raw = append(raw, '\n')

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
		conn, err := net.DialTimeout("tcp", p.addr, p.timeout(p.dialTimeout))
		if err != nil {
			return err
		}
		if err := conn.SetWriteDeadline(time.Now().Add(p.timeout(p.writeTimeout))); err != nil {
			_ = conn.Close()
			return err
		}
		n, err := io.WriteString(conn, publisherRegistration)
		if err == nil && n != len(publisherRegistration) {
			err = io.ErrShortWrite
		}
		if err != nil {
			_ = conn.Close()
			return err
		}
		p.conn = conn
		go drainBridge(conn)
	}
	if err := p.conn.SetWriteDeadline(time.Now().Add(p.timeout(p.writeTimeout))); err != nil {
		return err
	}
	n, err := p.conn.Write(raw)
	if err != nil {
		return err
	}
	if n != len(raw) {
		return io.ErrShortWrite
	}
	_ = p.conn.SetWriteDeadline(time.Time{})
	return nil
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
