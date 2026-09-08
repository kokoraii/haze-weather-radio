package productrender

import (
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestBridgeRetainsCAPAlertWhenBoundedQueueIsFull(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	bridge := &bridgeClient{
		conn:   clientConn,
		done:   make(chan struct{}),
		events: make(chan map[string]any, 1),
	}
	bridge.events <- map[string]any{"type": "ordinary.event"}
	go bridge.readLoop()
	defer bridge.Close()
	defer serverConn.Close()

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- json.NewEncoder(serverConn).Encode(map[string]any{
			"type":    "cap.alert.received",
			"subject": "urn:test:critical",
		})
	}()

	select {
	case message := <-bridge.Events():
		if stringAt(message, "type") != "ordinary.event" {
			t.Fatalf("first event = %#v, want the prefilled ordinary event", message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out draining the prefilled bridge event")
	}

	select {
	case message := <-bridge.Events():
		if stringAt(message, "type") != "cap.alert.received" {
			t.Fatalf("critical event = %#v", message)
		}
		if stringAt(message, "subject") != "urn:test:critical" {
			t.Fatalf("critical subject = %q", stringAt(message, "subject"))
		}
	case <-time.After(time.Second):
		t.Fatal("cap.alert.received was lost while the bounded queue was full")
	}

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out writing the critical bridge event")
	}
}

func TestBridgeRetainsRenderRequestsAndIgnoresUnrelatedTraffic(t *testing.T) {
	for _, eventType := range []string{"product.render.request", "wx.on_demand.request", "system.shutdown"} {
		t.Run(eventType, func(t *testing.T) {
			server, client := net.Pipe()
			bridge := &bridgeClient{conn: client, done: make(chan struct{}), events: make(chan map[string]any, 1)}
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
			bridge.events <- map[string]any{"type": "prefilled"}
			go bridge.readLoop()
			written := make(chan error, 1)
			go func() {
				encoder := json.NewEncoder(server)
				if err := encoder.Encode(map[string]any{"type": "unrelated.status"}); err != nil {
					written <- err
					return
				}
				written <- encoder.Encode(map[string]any{"type": eventType})
			}()
			select {
			case err := <-written:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("bridge blocked on unrelated traffic")
			}
			<-bridge.Events()
			select {
			case got := <-bridge.Events():
				if stringAt(got, "type") != eventType {
					t.Fatalf("got %#v, want %s", got, eventType)
				}
			case <-time.After(time.Second):
				t.Fatal("request was discarded under backpressure")
			}
		})
	}
}
