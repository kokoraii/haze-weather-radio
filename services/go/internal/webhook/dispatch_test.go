package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimitWait(t *testing.T) {
	cases := []struct {
		name   string
		result postResult
		want   time.Duration
	}{
		{"429 header preferred over millisecond body", postResult{Status: 429, Detail: `{"retry_after": 1534}`, Header: http.Header{"Retry-After": {"2"}}}, 2 * time.Second},
		{"429 body when no header", postResult{Status: 429, Detail: `{"retry_after": 2.5}`, Header: http.Header{}}, 2500 * time.Millisecond},
		{"429 without hints waits a second", postResult{Status: 429, Header: http.Header{}}, time.Second},
		{"bucket exhausted", postResult{Status: 204, Header: http.Header{"X-Ratelimit-Remaining": {"0"}, "X-Ratelimit-Reset-After": {"1.25"}}}, 1250 * time.Millisecond},
		{"bucket has room", postResult{Status: 204, Header: http.Header{"X-Ratelimit-Remaining": {"3"}, "X-Ratelimit-Reset-After": {"1.25"}}}, 0},
		{"capped", postResult{Status: 429, Header: http.Header{"Retry-After": {"600"}}}, maxRateLimitWait},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rateLimitWait(tc.result); got != tc.want {
				t.Fatalf("wait = %s, want %s", got, tc.want)
			}
		})
	}
}

// recorder is a fake Discord endpoint that records post order by username.
type recorder struct {
	mu    sync.Mutex
	order []string
	done  chan string
}

func newRecorder(t *testing.T, handle func(w http.ResponseWriter, username string) bool) (*httptest.Server, *recorder) {
	rec := &recorder{done: make(chan string, 16)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload discordPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if handle != nil && !handle(w, payload.Username) {
			return
		}
		rec.mu.Lock()
		rec.order = append(rec.order, payload.Username)
		rec.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		rec.done <- payload.Username
	}))
	t.Cleanup(server.Close)
	return server, rec
}

func (r *recorder) wait(t *testing.T, n int) []string {
	t.Helper()
	for range n {
		select {
		case <-r.done:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for posts")
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.order...)
}

func testDispatcher(t *testing.T, client *http.Client) *dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return newDispatcher(ctx, client)
}

func job(url, name string) delivery {
	return delivery{feedID: name, label: name, url: url, body: discordPayload{Username: name}}
}

func TestLaneRetriesRateLimitedPost(t *testing.T) {
	var calls atomic.Int32
	server, rec := newRecorder(t, func(w http.ResponseWriter, _ string) bool {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"retry_after": 0.05}`))
			return false
		}
		return true
	})
	d := testDispatcher(t, server.Client())
	d.enqueue(job(server.URL, "a"), nil)
	if got := rec.wait(t, 1); len(got) != 1 || calls.Load() != 2 {
		t.Fatalf("posts = %v after %d calls", got, calls.Load())
	}
}

// Posts to one URL keep enqueue order even when a later alert's audio is
// ready first.
func TestLaneKeepsEnqueueOrder(t *testing.T) {
	server, rec := newRecorder(t, nil)
	d := testDispatcher(t, server.Client())
	secondReady := make(chan struct{})
	d.enqueue(job(server.URL, "first"), func() attachment {
		<-secondReady
		return attachment{}
	})
	d.enqueue(job(server.URL, "second"), func() attachment {
		close(secondReady)
		return attachment{}
	})
	if got := rec.wait(t, 2); got[0] != "first" || got[1] != "second" {
		t.Fatalf("order = %v", got)
	}
}

// A stalled webhook URL must not hold up alerts bound for another.
func TestStalledLaneDoesNotBlockOtherURLs(t *testing.T) {
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	// Unblock the handler before Close, which waits for active requests.
	t.Cleanup(slow.Close)
	t.Cleanup(func() { close(release) })
	fast, rec := newRecorder(t, nil)
	d := testDispatcher(t, http.DefaultClient)
	for range laneQueueSize + 8 {
		d.enqueue(job(slow.URL, "slow"), nil)
	}
	if !d.enqueue(job(fast.URL, "fast"), nil) {
		t.Fatal("fast lane rejected the job")
	}
	rec.wait(t, 1)
}
