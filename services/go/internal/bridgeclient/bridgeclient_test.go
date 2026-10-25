package bridgeclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func listen(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func acceptLines(t *testing.T, listener net.Listener, want int) <-chan []string {
	t.Helper()
	lines := make(chan []string, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		var got []string
		for len(got) < want {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			got = append(got, line)
		}
		lines <- got
	}()
	return lines
}

func receive(t *testing.T, lines <-chan []string) []string {
	t.Helper()
	select {
	case got := <-lines:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("bridge lines did not arrive")
		return nil
	}
}

func TestDialRejectsBlankAddress(t *testing.T) {
	for _, addr := range []string{"", "   "} {
		if _, err := Dial(context.Background(), Options{Addr: addr}); !errors.Is(err, ErrMissingAddress) {
			t.Fatalf("Dial(%q) error = %v", addr, err)
		}
	}
	if ErrMissingAddress.Error() != "missing host event bridge address" {
		t.Fatalf("error text changed: %q", ErrMissingAddress)
	}
}

func TestLegacyDialSendsNoHandshake(t *testing.T) {
	listener := listen(t)
	lines := acceptLines(t, listener, 1)
	conn, err := Dial(context.Background(), Options{Addr: listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Publish(map[string]any{"type": "service.ready", "source": "test"}); err != nil {
		t.Fatal(err)
	}
	got := receive(t, lines)
	if len(got) != 1 || !strings.Contains(got[0], `"type":"service.ready"`) {
		t.Fatalf("first line was not the published event: %q", got)
	}
}

func TestRegistrationWireFormat(t *testing.T) {
	cases := []struct {
		name         string
		registration Registration
		want         string
	}{
		{
			name:         "publish only",
			registration: Registration{},
			want:         `{"type":"bridge.client","data":{"receive_events":false}}` + "\n",
		},
		{
			name: "subscriber",
			registration: Registration{
				ClientID: "haze-test", ReceiveEvents: true, Subscriptions: []string{"a.b", "c.*"},
			},
			want: `{"type":"bridge.client","data":{"client_id":"haze-test","receive_events":true,"subscriptions":["a.b","c.*"]}}` + "\n",
		},
		{
			name:         "binary pcm",
			registration: Registration{ReceiveEvents: true, BinaryPCM: true},
			want:         `{"type":"bridge.client","data":{"receive_events":true,"binary_pcm":true}}` + "\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			listener := listen(t)
			lines := acceptLines(t, listener, 1)
			registration := tc.registration
			conn, err := Dial(context.Background(), Options{Addr: listener.Addr().String(), Register: &registration})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			got := receive(t, lines)
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("registration = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPublishStampsMissingTimestampOnly(t *testing.T) {
	listener := listen(t)
	lines := acceptLines(t, listener, 2)
	conn, err := Dial(context.Background(), Options{Addr: listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	stamped := map[string]any{"type": "a"}
	if err := conn.Publish(stamped); err != nil {
		t.Fatal(err)
	}
	if _, ok := stamped["timestamp"]; !ok {
		t.Fatal("caller map was not stamped")
	}
	if err := conn.Publish(map[string]any{"type": "b", "timestamp": "fixed"}); err != nil {
		t.Fatal(err)
	}
	got := receive(t, lines)
	if len(got) != 2 {
		t.Fatalf("lines = %q", got)
	}
	var first, second map[string]any
	if err := json.Unmarshal([]byte(got[0]), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(got[1]), &second); err != nil {
		t.Fatal(err)
	}
	if first["timestamp"] == nil || first["timestamp"] == "" {
		t.Fatalf("missing timestamp: %v", first)
	}
	if second["timestamp"] != "fixed" {
		t.Fatalf("existing timestamp was replaced: %v", second)
	}
}

func TestPublishMatchesJSONEncoderBytes(t *testing.T) {
	message := map[string]any{
		"type": "x", "timestamp": "t",
		"data": map[string]any{"text": "<b>&</b>", "n": 3},
	}
	var want bytes.Buffer
	if err := json.NewEncoder(&want).Encode(message); err != nil {
		t.Fatal(err)
	}
	listener := listen(t)
	lines := acceptLines(t, listener, 1)
	conn, err := Dial(context.Background(), Options{Addr: listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Publish(message); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, lines); len(got) != 1 || got[0] != want.String() {
		t.Fatalf("wire bytes = %q, want %q", got, want.String())
	}
}

func TestConcurrentPublishNeverInterleavesLines(t *testing.T) {
	listener := listen(t)
	const writers, perWriter = 8, 50
	lines := acceptLines(t, listener, writers*perWriter)
	conn, err := Dial(context.Background(), Options{Addr: listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	payload := strings.Repeat("x", 4096)
	var wg sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := conn.Publish(map[string]any{"type": "load", "data": map[string]any{"p": payload}}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, line := range receive(t, lines) {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("interleaved or truncated line: %v", err)
		}
	}
}

func TestWriteTimeoutBoundsBlockedConnection(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	conn := newConn(client, 25*time.Millisecond, 0)
	defer conn.Close()
	started := time.Now()
	if err := conn.Publish(map[string]any{"type": "blocked"}); err == nil {
		t.Fatal("blocked publish unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("publish blocked for %s", elapsed)
	}
}

// serveLines writes payload to the first client and then closes the connection,
// or holds it open until the client hangs up when hold is set.
func serveLines(t *testing.T, listener net.Listener, payload string, hold bool) {
	t.Helper()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte(payload))
		if hold {
			_, _ = bufio.NewReader(conn).ReadByte()
		}
	}()
}

func TestReadMessagesSkipsNoiseAndKeepsOrder(t *testing.T) {
	listener := listen(t)
	serveLines(t, listener, "\n  \r\nnot json\nnull\n[1,2]\n"+
		`{"type":"one"}`+"\r\n"+
		`  {"type":"two"}  `+"\n"+
		`{"type":"three"}`, false)
	conn, err := Dial(context.Background(), Options{Addr: listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var types []string
	if err := conn.ReadMessages(func(message map[string]any) bool {
		types = append(types, message["type"].(string))
		return true
	}); err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Join(types, ",") != "one,two,three" {
		t.Fatalf("types = %v", types)
	}
}

func TestReadMessagesStopsWhenHandlerReturnsFalse(t *testing.T) {
	listener := listen(t)
	serveLines(t, listener, `{"type":"one"}`+"\n"+`{"type":"two"}`+"\n", false)
	conn, err := Dial(context.Background(), Options{Addr: listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	calls := 0
	if err := conn.ReadMessages(func(map[string]any) bool { calls++; return false }); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d", calls)
	}
}

func TestOversizedLineEndsScanWithTooLong(t *testing.T) {
	listener := listen(t)
	serveLines(t, listener, `{"type":"`+strings.Repeat("x", 4096)+`"}`+"\n", false)
	conn, err := Dial(context.Background(), Options{Addr: listener.Addr().String(), MaxLineBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	err = conn.ReadMessages(func(map[string]any) bool { return true })
	if !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("error = %v, want bufio.ErrTooLong", err)
	}
}

func TestDeadlineOptionBoundsReads(t *testing.T) {
	listener := listen(t)
	serveLines(t, listener, "", true)
	conn, err := Dial(context.Background(), Options{
		Addr: listener.Addr().String(), Deadline: time.Now().Add(50 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	started := time.Now()
	err = conn.Scan(func([]byte) bool { return true })
	if err == nil {
		t.Fatal("scan ended without a deadline error")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("scan blocked for %s", elapsed)
	}
}

func TestReconnectRedialsUntilContextEnds(t *testing.T) {
	listener := listen(t)
	accepted := make(chan struct{}, 4)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			_ = conn.Close()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	sessions := 0
	go func() {
		defer close(done)
		Reconnect(ctx, Options{Addr: listener.Addr().String()}, 10*time.Millisecond, func(ctx context.Context, conn *Conn) {
			sessions++
			_ = conn.Scan(func([]byte) bool { return true })
		})
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-accepted:
		case <-time.After(2 * time.Second):
			t.Fatal("bridge was not redialed")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Reconnect did not stop after cancellation")
	}
	if sessions < 2 {
		t.Fatalf("sessions = %d", sessions)
	}
}

func TestNilConnIsSafeToClose(t *testing.T) {
	var conn *Conn
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if conn.NetConn() != nil {
		t.Fatal("nil Conn returned a connection")
	}
}
