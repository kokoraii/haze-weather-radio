package productrender

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestRenderPoolsIsolateSlowWorkAndPreserveCAPOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	order := make(chan string, 2)
	alerts := startRenderWorkPool(ctx, 1, func(job renderWork) {
		id := stringAt(job.event, "id")
		if id == "update" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
		}
		order <- id
	})
	defer alerts.wg.Wait()
	defer cancel()
	alerts.trySubmit(renderWork{event: map[string]any{"id": "update"}})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("CAP work did not start")
	}
	alerts.trySubmit(renderWork{event: map[string]any{"id": "cancel"}})
	for _, lane := range []string{"routine", "interactive"} {
		t.Run(lane, func(t *testing.T) {
			done := make(chan struct{})
			pool := startRenderWorkPool(ctx, 1, func(renderWork) { close(done) })
			pool.trySubmit(renderWork{})
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("render lane blocked behind CAP processing")
			}
		})
	}
	close(release)
	for _, want := range []string{"update", "cancel"} {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("CAP order = %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("CAP work did not finish")
		}
	}
}

func TestRenderWorkPoolBoundsQueueAndCancelsPendingWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	p := startRenderWorkPool(ctx, 1, func(renderWork) {
		close(entered)
		<-ctx.Done()
	})
	defer p.wg.Wait()
	defer cancel()
	p.trySubmit(renderWork{})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	for range renderWorkBacklog {
		if !p.trySubmit(renderWork{}) {
			t.Fatal("queue filled before configured bound")
		}
	}
	if p.trySubmit(renderWork{}) {
		t.Fatal("unbounded work accepted")
	}
}

func TestRenderWorkersUseJobConfigSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server, client := net.Pipe()
	bridge := &bridgeClient{conn: client, done: make(chan struct{})}
	defer func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()
	defer func() {
		if err := bridge.Close(); err != nil {
			t.Error(err)
		}
	}()
	s := &Service{bridge: bridge}
	w := s.startWorkers(ctx)
	defer w.wait()
	defer cancel()
	response := make(chan map[string]any, 1)
	go func() {
		var event map[string]any
		if err := json.NewDecoder(server).Decode(&event); err == nil {
			response <- event
		}
	}()
	snapshot := loadedConfig{Feeds: []feedXML{{ID: "original", EnabledRaw: "true", Timezone: "UTC"}}}
	w.routine.trySubmit(renderWork{cfg: snapshot, event: map[string]any{
		"type": "product.render.request", "subject": "snapshot-request",
		"data": map[string]any{"feed_id": "original", "pkg_id": "station_id", "language": "en-US"},
	}})
	// Replacing the dispatcher's config must not invalidate an admitted job.
	s.cfg = loadedConfig{Feeds: []feedXML{{ID: "replacement"}}}
	select {
	case event := <-response:
		if stringAt(event, "type") != "product.rendered" || stringAt(event, "subject") != "snapshot-request" {
			t.Fatalf("render response = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("render did not complete from its config snapshot")
	}
}
