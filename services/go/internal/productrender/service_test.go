package productrender

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestRunDoesNotPanicBeforeStoreOpens(t *testing.T) {
	dir := t.TempDir()
	writeFixture(t, dir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := Run(ctx, Options{
		ConfigPath: filepath.Join(dir, "config.yaml"),
		BridgeAddr: "",
		Refresh:    time.Second,
	})
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context cancellation", err)
	}
}

func TestWxOnDemandRequestUsesWeatherSourceFromData(t *testing.T) {
	request := wxOnDemandRequestFromEvent(map[string]any{
		"type":    "wx.on_demand.request",
		"source":  "haze-ivr",
		"subject": "wx-1",
		"data": map[string]any{
			"request_id":        "wx-1",
			"canonical_id":      "urn:haze:location:fixture",
			"feed_id":           "sk-0001",
			"code":              "06032",
			"source":            "hello_weather",
			"location_identity": "hello_weather:06032",
			"country":           "CA",
			"forecast_id":       "sk-32",
			"latitude":          "51.347",
			"longitude":         "-105.434",
			"packages":          []any{"forecast"},
		},
	})

	if request.Source != "hello_weather" {
		t.Fatalf("weather source = %q, want hello_weather", request.Source)
	}
	if request.Identity != "hello_weather:06032" || request.Country != "CA" {
		t.Fatalf("canonical identity = %q country = %q", request.Identity, request.Country)
	}
	if request.CanonicalID != "urn:haze:location:fixture" {
		t.Fatalf("canonical ID = %q", request.CanonicalID)
	}
	if request.RequestID != "wx-1" || request.FeedID != "sk-0001" || request.ForecastID != "sk-32" {
		t.Fatalf("parsed request = %#v", request)
	}
	if request.Latitude != "51.347" || request.Longitude != "-105.434" {
		t.Fatalf("coordinates = %q,%q", request.Latitude, request.Longitude)
	}
	if len(request.Packages) != 1 || request.Packages[0] != "forecast" {
		t.Fatalf("packages = %#v", request.Packages)
	}
}
