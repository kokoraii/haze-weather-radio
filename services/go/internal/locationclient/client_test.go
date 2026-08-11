package locationclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestQueryCancellationInterruptsPendingBridgeRead(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requestReceived := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer connection.Close()
		scanner := bufio.NewScanner(connection)
		if !scanner.Scan() || !scanner.Scan() {
			serverDone <- scanner.Err()
			return
		}
		close(requestReceived)
		if scanner.Scan() {
			serverDone <- &testError{"unexpected third bridge message"}
			return
		}
		serverDone <- scanner.Err()
	}()

	client := New(listener.Addr().String(), "cancel-test")
	client.Timeout = 10 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	queryDone := make(chan error, 1)
	go func() {
		_, err := client.Query(ctx, Request{
			Operation: "resolve",
			Input:     &Input{Kind: "auto", Text: "CYXE"},
		})
		queryDone <- err
	}()
	select {
	case <-requestReceived:
	case <-time.After(time.Second):
		t.Fatal("location query did not reach the bridge")
	}
	cancel()
	select {
	case err := <-queryDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled query error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled location query did not return promptly")
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled location query did not close its bridge connection")
	}
}

func TestQueryUsesTargetedBrokerContract(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer connection.Close()
		scanner := bufio.NewScanner(connection)
		if !scanner.Scan() {
			done <- scanner.Err()
			return
		}
		if !scanner.Scan() {
			done <- scanner.Err()
			return
		}
		var request map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			done <- err
			return
		}
		if request["target"] != "haze-location" {
			done <- &testError{"query was not targeted"}
			return
		}
		data := request["data"].(map[string]any)
		response := map[string]any{
			"type": "location.query.completed", "subject": data["request_id"],
			"data": map[string]any{
				"api_version": 1, "request_id": data["request_id"], "operation": "resolve",
				"status": "resolved", "ambiguous": false, "catalog_generation": "test",
				"catalog_packs": []string{"test"}, "truncated": false,
			},
		}
		done <- json.NewEncoder(connection).Encode(response)
	}()

	client := New(listener.Addr().String(), "test")
	client.Timeout = time.Second
	response, err := client.Query(context.Background(), Request{
		Operation: "resolve",
		Input:     &Input{Kind: "auto", Text: "CYXE"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.CatalogGeneration != "test" {
		t.Fatalf("unexpected response: %#v", response)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSharedV1ContractFixtures(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate contract test")
	}
	contractRoot := filepath.Join(filepath.Dir(testFile), "..", "..", "..", "..", "contracts", "location", "v1")
	queryBytes, err := os.ReadFile(filepath.Join(contractRoot, "query.json"))
	if err != nil {
		t.Fatal(err)
	}
	var request Request
	if err := json.Unmarshal(queryBytes, &request); err != nil {
		t.Fatal(err)
	}
	if request.Input == nil || request.Input.Latitude == nil || request.Input.Longitude == nil {
		t.Fatal("point coordinates were omitted")
	}
	if *request.Input.Latitude != 0 || *request.Input.Longitude != 0 {
		t.Fatalf("expected valid 0,0 point, got %v,%v", *request.Input.Latitude, *request.Input.Longitude)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"latitude":0`) || !strings.Contains(string(encoded), `"longitude":0`) {
		t.Fatalf("encoded point lost zero coordinates: %s", encoded)
	}

	responseBytes, err := os.ReadFile(filepath.Join(contractRoot, "response.json"))
	if err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.Unmarshal(responseBytes, &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || len(response.Results[0].Entity.Deployments) != 1 {
		t.Fatalf("response fixture lost nested location data: %#v", response)
	}
}

func TestOptionsSerializeAreaGeometryOnlyWhenRequested(t *testing.T) {
	withoutGeometry, err := json.Marshal(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(withoutGeometry), "include_area_geometry") {
		t.Fatalf("default options requested geometry: %s", withoutGeometry)
	}
	withGeometry, err := json.Marshal(Options{IncludeAreaGeometry: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(withGeometry), `"include_area_geometry":true`) {
		t.Fatalf("geometry option was omitted: %s", withGeometry)
	}
}

type testError struct{ message string }

func (error *testError) Error() string { return error.message }
