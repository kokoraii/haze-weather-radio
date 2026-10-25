// Package bridgeclient is the shared Go client for the Haze host event bridge.
//
// The bridge speaks newline-delimited JSON over loopback TCP. Every Go service
// used to carry its own copy of the dial, registration, framing, and read loop
// code. This package owns that wire handling so a later transport (see
// docs/grpc-migration-plan.md) can be selected in one place. It deliberately
// keeps the existing wire behavior: callers keep their own filtering, overflow,
// and request correlation policy and only hand the transport details here.
package bridgeclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultDialTimeout bounds the TCP connect to the bridge.
	DefaultDialTimeout = 3 * time.Second
	// DefaultMaxLineBytes is the largest single event line the reader accepts.
	DefaultMaxLineBytes = 4 * 1024 * 1024

	initialLineBuffer = 64 * 1024
	registrationType  = "bridge.client"
)

// ErrMissingAddress is returned when no bridge address is configured.
var ErrMissingAddress = errors.New("missing host event bridge address")

// Registration is the optional bridge.client handshake. A connection that never
// registers is treated by the broker as a legacy client: it receives every
// event after a short grace period or after its first publish.
type Registration struct {
	// ClientID names the connection for targeted delivery (event "target").
	ClientID string
	// ReceiveEvents asks the broker to deliver events to this connection.
	ReceiveEvents bool
	// Subscriptions limits delivery to matching event types. An empty list
	// receives every event. A trailing "*" matches by prefix.
	Subscriptions []string
	// BinaryPCM opts in to compact PCM frames on the media bridge.
	BinaryPCM bool
}

type registrationMessage struct {
	Type string           `json:"type"`
	Data registrationData `json:"data"`
}

type registrationData struct {
	ClientID      string   `json:"client_id,omitempty"`
	ReceiveEvents bool     `json:"receive_events"`
	Subscriptions []string `json:"subscriptions,omitempty"`
	BinaryPCM     bool     `json:"binary_pcm,omitempty"`
}

func (r Registration) message() registrationMessage {
	return registrationMessage{
		Type: registrationType,
		Data: registrationData{
			ClientID:      strings.TrimSpace(r.ClientID),
			ReceiveEvents: r.ReceiveEvents,
			Subscriptions: r.Subscriptions,
			BinaryPCM:     r.BinaryPCM,
		},
	}
}

// Options configures Dial.
type Options struct {
	// Addr is the bridge host:port.
	Addr string
	// DialTimeout bounds the connect. Zero selects DefaultDialTimeout.
	DialTimeout time.Duration
	// WriteTimeout bounds each Publish. Zero leaves writes unbounded.
	WriteTimeout time.Duration
	// MaxLineBytes bounds one inbound line. Zero selects DefaultMaxLineBytes.
	MaxLineBytes int
	// Deadline, when set, is an absolute I/O deadline applied to the
	// connection before the registration is written.
	Deadline time.Time
	// Register sends a bridge.client handshake right after connecting. Nil
	// keeps the legacy unregistered behavior.
	Register *Registration
}

// Conn is one connection to the host event bridge. Publish is safe for
// concurrent use. Scan and ReadMessages must be driven by a single reader.
type Conn struct {
	conn         net.Conn
	writeTimeout time.Duration
	maxLine      int
	mu           sync.Mutex
}

// Dial connects to the bridge and optionally registers the client.
func Dial(ctx context.Context, opts Options) (*Conn, error) {
	addr := strings.TrimSpace(opts.Addr)
	if addr == "" {
		return nil, ErrMissingAddress
	}
	timeout := opts.DialTimeout
	if timeout <= 0 {
		timeout = DefaultDialTimeout
	}
	raw, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn := newConn(raw, opts.WriteTimeout, opts.MaxLineBytes)
	if !opts.Deadline.IsZero() {
		if err := raw.SetDeadline(opts.Deadline); err != nil {
			_ = raw.Close()
			return nil, err
		}
	}
	if opts.Register != nil {
		if err := conn.Register(*opts.Register); err != nil {
			_ = raw.Close()
			return nil, err
		}
	}
	return conn, nil
}

// Register sends the bridge.client handshake on an established connection.
func (c *Conn) Register(registration Registration) error {
	return c.PublishValue(registration.message())
}

// Wrap adopts an already established connection, such as a test pipe. Only the
// WriteTimeout and MaxLineBytes options apply.
func Wrap(raw net.Conn, opts Options) *Conn {
	return newConn(raw, opts.WriteTimeout, opts.MaxLineBytes)
}

func newConn(raw net.Conn, writeTimeout time.Duration, maxLine int) *Conn {
	if maxLine <= 0 {
		maxLine = DefaultMaxLineBytes
	}
	return &Conn{conn: raw, writeTimeout: writeTimeout, maxLine: maxLine}
}

// NetConn exposes the underlying connection for callers that still write to it
// directly. Prefer Publish so writes stay serialized.
func (c *Conn) NetConn() net.Conn {
	if c == nil {
		return nil
	}
	return c.conn
}

// Close closes the connection. It is safe on a nil or already closed Conn.
func (c *Conn) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// SetDeadline sets the read and write deadline for the connection.
func (c *Conn) SetDeadline(deadline time.Time) error {
	return c.conn.SetDeadline(deadline)
}

// Publish stamps a missing timestamp on the message and writes it as one line.
// The caller's map is updated with the timestamp, as the per-service clients
// always did.
func (c *Conn) Publish(message map[string]any) error {
	if _, ok := message["timestamp"]; !ok {
		message["timestamp"] = time.Now().UTC()
	}
	return c.PublishValue(message)
}

// PublishValue marshals any JSON value and writes it as one line.
func (c *Conn) PublishValue(value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.WriteLine(raw)
}

// WriteLine writes one pre-encoded JSON document followed by a newline. Writes
// are serialized so concurrent publishers never interleave lines.
func (c *Conn) WriteLine(raw []byte) error {
	line := make([]byte, 0, len(raw)+1)
	line = append(line, raw...)
	line = append(line, '\n')

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeTimeout > 0 {
		_ = c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()
	}
	written, err := c.conn.Write(line)
	if err == nil && written != len(line) {
		err = io.ErrShortWrite
	}
	return err
}

// Scan reads inbound lines until the connection ends, a line exceeds
// MaxLineBytes, or handle returns false. The slice passed to handle is only
// valid during the call. It returns the reader error, or nil on a clean close
// or when handle stopped the scan.
func (c *Conn) Scan(handle func(line []byte) bool) error {
	scanner := bufio.NewScanner(c.conn)
	// The scanner only enforces its limit when the buffer has to grow, so the
	// initial buffer must not start larger than the configured maximum.
	scanner.Buffer(make([]byte, min(initialLineBuffer, c.maxLine)), c.maxLine)
	for scanner.Scan() {
		if !handle(scanner.Bytes()) {
			return nil
		}
	}
	return scanner.Err()
}

// ReadMessages decodes inbound lines into JSON objects. Blank lines, invalid
// JSON, and JSON null are skipped. Scanning stops when handle returns false.
func (c *Conn) ReadMessages(handle func(message map[string]any) bool) error {
	return c.Scan(func(line []byte) bool {
		message, ok := DecodeMessage(line)
		if !ok {
			return true
		}
		return handle(message)
	})
}

// DecodeMessage decodes one inbound line into a JSON object.
func DecodeMessage(line []byte) (map[string]any, bool) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, false
	}
	var message map[string]any
	if err := json.Unmarshal(line, &message); err != nil || message == nil {
		return nil, false
	}
	return message, true
}

// Reconnect dials the bridge until ctx ends. Each live connection is handed to
// session, and delay separates attempts. The connection is closed when session
// returns.
func Reconnect(ctx context.Context, opts Options, delay time.Duration, session func(ctx context.Context, conn *Conn)) {
	for ctx.Err() == nil {
		conn, err := Dial(ctx, opts)
		if err == nil {
			session(ctx, conn)
			_ = conn.Close()
		}
		sleep(ctx, delay)
	}
}

func sleep(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
