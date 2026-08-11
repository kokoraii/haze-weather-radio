package webgateway

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationimport"
)

func TestRequestLocationCatalogReloadWaitsForTargetedResult(t *testing.T) {
	tests := []struct {
		name       string
		response   map[string]any
		wantError  string
		generation string
	}{
		{
			name: "confirmed",
			response: map[string]any{
				"type": "location.catalog.reloaded",
				"data": map[string]any{
					"catalog_generation": "generation-615",
					"catalog_packs":      []string{"legacy-alert-locations", "legacy-weather-geometry"},
				},
			},
			generation: "generation-615",
		},
		{
			name: "rejected",
			response: map[string]any{
				"type": "location.catalog.reload_failed",
				"data": map[string]any{"code": "reload_failed", "error": "orphan geometry"},
			},
			wantError: "orphan geometry",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			serverErr := make(chan error, 1)
			go func() {
				connection, err := listener.Accept()
				if err != nil {
					serverErr <- err
					return
				}
				defer connection.Close()
				decoder := json.NewDecoder(connection)
				var handshake struct {
					Data struct {
						ClientID string `json:"client_id"`
					} `json:"data"`
				}
				if err := decoder.Decode(&handshake); err != nil {
					serverErr <- err
					return
				}
				var request map[string]any
				if err := decoder.Decode(&request); err != nil {
					serverErr <- err
					return
				}
				if handshake.Data.ClientID == "" || request["reply_to"] != handshake.Data.ClientID || request["target"] != "haze-location" {
					serverErr <- errors.New("reload request was not registered for targeted reply")
					return
				}
				encoder := json.NewEncoder(connection)
				if err := encoder.Encode(map[string]any{
					"type": "location.catalog.reloaded", "target": "another-client",
					"data": map[string]any{"catalog_generation": "wrong-client"},
				}); err != nil {
					serverErr <- err
					return
				}
				response := make(map[string]any, len(test.response)+1)
				for key, value := range test.response {
					response[key] = value
				}
				response["target"] = handshake.Data.ClientID
				if err := encoder.Encode(response); err != nil {
					serverErr <- err
					return
				}
				serverErr <- nil
			}()
			t.Setenv("HAZE_HOST_BRIDGE_ADDR", listener.Addr().String())
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := requestLocationCatalogReload(ctx, locationimport.Result{
				Source: "clc", ProviderVersion: "6.15.0", RecordCount: 1391,
			}, "operator_catalog_import")
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				if result.CatalogGeneration != test.generation {
					t.Fatalf("catalog generation = %q", result.CatalogGeneration)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("reload error = %v, want %q", err, test.wantError)
			}
			if err := <-serverErr; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRequestLocationCatalogReloadRequiresBridge(t *testing.T) {
	t.Setenv("HAZE_HOST_BRIDGE_ADDR", "")
	_, err := requestLocationCatalogReload(context.Background(), locationimport.Result{}, "operator_catalog_import")
	if err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("missing bridge error = %v", err)
	}
}
