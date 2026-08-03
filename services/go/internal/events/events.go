package events

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// Event is the shared JSON event envelope used by first-pass Go services.
type Event struct {
	ID        string         `json:"id,omitempty"`
	Type      string         `json:"type"`
	Source    string         `json:"source"`
	Timestamp time.Time      `json:"timestamp"`
	Subject   string         `json:"subject,omitempty"`
	Target    string         `json:"target,omitempty"`
	ReplyTo   string         `json:"reply_to,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
}

// Publisher writes events to an event transport.
type Publisher interface {
	Publish(event Event) error
}

// JSONLPublisher emits one event JSON document per line.
type JSONLPublisher struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

// NewJSONLPublisher creates a JSONL event publisher.
func NewJSONLPublisher(writer io.Writer) *JSONLPublisher {
	return &JSONLPublisher{encoder: json.NewEncoder(writer)}
}

// Publish writes an event with a timestamp when the caller did not provide one.
func (p *JSONLPublisher) Publish(event Event) error {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.encoder.Encode(event)
}
