package feedconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/bridgeclient"
)

const (
	requestEvent  = "config.feeds.request"
	snapshotEvent = "config.feeds.snapshot"
	updatedEvent  = "config.feeds.updated"
	// reconnectDelay separates bridge reconnect attempts.
	reconnectDelay = 2 * time.Second
)

// Store holds the latest feed config snapshot for one service. Run keeps it
// current from the bridge; readers call Current, or Wait at startup.
type Store struct {
	mu      sync.RWMutex
	current *Snapshot
	ready   chan struct{}
	once    sync.Once
}

// NewStore returns an empty store. Use Start to fill it from the bridge, or
// Set in tests.
func NewStore() *Store {
	return &Store{ready: make(chan struct{})}
}

// Start creates a store and keeps it current from the host event bridge named
// by HAZE_HOST_BRIDGE_ADDR until ctx ends. clientID must be unique per process.
func Start(ctx context.Context, clientID string) *Store {
	store := NewStore()
	go store.Run(ctx, os.Getenv("HAZE_HOST_BRIDGE_ADDR"), clientID)
	return store
}

// Current returns the latest snapshot, or false before the first one arrives.
func (s *Store) Current() (*Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current, s.current != nil
}

// Feed returns one feed's coverage from the latest snapshot.
func (s *Store) Feed(feedID string) (Feed, bool) {
	snapshot, ok := s.Current()
	if !ok {
		return Feed{}, false
	}
	return snapshot.Feed(feedID)
}

// Wait blocks until the first snapshot arrives or ctx ends.
func (s *Store) Wait(ctx context.Context) (*Snapshot, error) {
	select {
	case <-s.ready:
		snapshot, _ := s.Current()
		return snapshot, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("feed config from the daemon: %w", ctx.Err())
	}
}

// Set replaces the snapshot. Run calls it for every snapshot it receives.
func (s *Store) Set(snapshot *Snapshot) {
	s.mu.Lock()
	s.current = snapshot
	s.mu.Unlock()
	s.once.Do(func() { close(s.ready) })
}

// Run requests the snapshot on every bridge connection and applies updates
// until ctx ends.
func (s *Store) Run(ctx context.Context, bridgeAddr, clientID string) {
	if strings.TrimSpace(bridgeAddr) == "" {
		slog.Error("feed config store has no host event bridge address")
		return
	}
	opts := bridgeclient.Options{
		Addr: bridgeAddr,
		Register: &bridgeclient.Registration{
			ClientID:      clientID,
			ReceiveEvents: true,
			// The snapshot reply is targeted at clientID, so only updates need
			// a subscription.
			Subscriptions: []string{updatedEvent},
		},
	}
	bridgeclient.Reconnect(ctx, opts, reconnectDelay, func(ctx context.Context, conn *bridgeclient.Conn) {
		stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
		defer stop()
		if err := conn.Publish(map[string]any{
			"type":     requestEvent,
			"source":   clientID,
			"reply_to": clientID,
			"subject":  clientID,
		}); err != nil {
			return
		}
		_ = conn.Scan(func(line []byte) bool {
			s.apply(line)
			return true
		})
	})
}

func (s *Store) apply(line []byte) {
	var envelope struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		return
	}
	if envelope.Type != snapshotEvent && envelope.Type != updatedEvent {
		return
	}
	var snapshot Snapshot
	if err := json.Unmarshal(envelope.Data, &snapshot); err != nil {
		slog.Error("unreadable feed config snapshot", "error", err)
		return
	}
	if snapshot.Version != SnapshotVersion {
		slog.Error("ignoring a feed config snapshot of another version", "version", snapshot.Version, "expected", SnapshotVersion)
		return
	}
	if current, ok := s.Current(); ok && current.Generation == snapshot.Generation {
		return
	}
	s.Set(&snapshot)
	slog.Info("feed config applied", "generation", snapshot.Generation, "feeds", len(snapshot.Feeds))
}
