package feedconfig

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type coverageCases struct {
	Cases []struct {
		Name    string          `json:"name"`
		Feed    Feed            `json:"feed"`
		Covers  map[string]bool `json:"covers"`
		Headers []struct {
			Codes   []string `json:"codes"`
			Expect  []string `json:"expect"`
			Dropped []string `json:"dropped"`
		} `json:"headers"`
		FullHeader []string `json:"full_header"`
	} `json:"cases"`
}

// The same cases crates/haze-feed-config runs, so Go and Rust cannot drift.
func TestSharedCoverageCases(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contracts", "feedconfig", "v1", "coverage-cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases coverageCases
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases.Cases) == 0 {
		t.Fatal("no coverage cases")
	}
	for _, tc := range cases.Cases {
		for code, want := range tc.Covers {
			if got := tc.Feed.Covers(code); got != want {
				t.Errorf("%s: Covers(%s) = %v, want %v", tc.Name, code, got, want)
			}
		}
		for _, header := range tc.Headers {
			got := tc.Feed.SameHeader(header.Codes)
			if !reflect.DeepEqual(got.Codes, header.Expect) || !reflect.DeepEqual(got.Dropped, header.Dropped) {
				t.Errorf("%s: SameHeader(%v) = %v dropped %v, want %v dropped %v", tc.Name, header.Codes, got.Codes, got.Dropped, header.Expect, header.Dropped)
			}
		}
		if got := tc.Feed.FullHeader().Codes; !reflect.DeepEqual(got, tc.FullHeader) {
			t.Errorf("%s: FullHeader = %v, want %v", tc.Name, got, tc.FullHeader)
		}
	}
}

func TestNormalizeCode(t *testing.T) {
	if got := NormalizeCode(" skz-012 "); got != "SKZ012" {
		t.Fatalf("NormalizeCode = %q", got)
	}
}

// A store requests the snapshot on connect, then applies broadcast updates.
func TestStoreRequestsAndAppliesUpdates(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requests := make(chan map[string]any, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		decoder := json.NewDecoder(conn)
		for {
			var message map[string]any
			if decoder.Decode(&message) != nil {
				return
			}
			if message["type"] != requestEvent {
				continue
			}
			requests <- message
			for _, reply := range []map[string]any{
				{"type": snapshotEvent, "target": message["reply_to"], "data": Snapshot{Version: 1, Generation: "a", Feeds: []Feed{{FeedID: "cwxr-sk01"}}}},
				{"type": updatedEvent, "data": Snapshot{Version: 1, Generation: "b", Feeds: []Feed{{FeedID: "cwxr-sk01"}, {FeedID: "cwxr-sk02"}}}},
			} {
				line, _ := json.Marshal(reply)
				_, _ = conn.Write(append(line, '\n'))
			}
			<-time.After(time.Second)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := NewStore()
	go store.Run(ctx, listener.Addr().String(), "haze-test-feedconfig")

	request := <-requests
	if request["reply_to"] != "haze-test-feedconfig" {
		t.Fatalf("request reply_to = %v", request["reply_to"])
	}
	if _, err := store.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if snapshot, _ := store.Current(); snapshot.Generation == "b" {
			if _, ok := store.Feed("cwxr-sk02"); !ok {
				t.Fatal("update did not add cwxr-sk02")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("update was not applied")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
