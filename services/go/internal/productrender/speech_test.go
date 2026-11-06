package productrender

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alerttext"
	"github.com/meowraii/haze-weather-radio/services/go/internal/bridgeclient"
	"github.com/meowraii/haze-weather-radio/services/go/internal/capmodel"
)

func loadFixtureCAP(t *testing.T, name string) capmodel.Alert {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "cap", name))
	if err != nil {
		t.Fatal(err)
	}
	alert, err := capmodel.ParseCAP(raw)
	if err != nil {
		t.Fatal(err)
	}
	return alert
}

func TestAlertSpeechRequestRepliesWithCAPText(t *testing.T) {
	alert := loadFixtureCAP(t, "example_SVR_ECCC_WXR_2026_04_15T20_51_47_00_00Iurn_oid_2.49.0.1.124.2486553930.2026.xml")
	server, client := net.Pipe()
	defer server.Close()
	s := &Service{bridge: &bridgeClient{bridge: bridgeclient.Wrap(client, bridgeclient.Options{}), done: make(chan struct{})}}
	defer s.bridge.Close()

	response := make(chan map[string]any, 1)
	go func() {
		var event map[string]any
		if err := json.NewDecoder(server).Decode(&event); err == nil {
			response <- event
		}
	}()
	alertValue := map[string]any{}
	raw, _ := json.Marshal(alert)
	_ = json.Unmarshal(raw, &alertValue)
	go s.handleAlertSpeech(map[string]any{
		"type": alertSpeechRequestEvent,
		"data": map[string]any{"request_id": "speech-1", "alert": alertValue, "language": "fr-CA", "timezone": "UTC"},
	})

	select {
	case event := <-response:
		data := mapAt(event, "data")
		if stringAt(event, "type") != alertSpeechCompletedEvent || stringAt(data, "request_id") != "speech-1" {
			t.Fatalf("speech reply = %#v", event)
		}
		if !strings.HasPrefix(strings.ToLower(stringAt(data, "language")), "fr") || stringAt(data, "text") == "" {
			t.Fatalf("speech reply did not voice the French info block: %#v", data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no speech reply")
	}
}

func TestCAPBannerViewCarriesFeedNameAndGradient(t *testing.T) {
	alert := loadFixtureCAP(t, "example_SVR_ECCC_WXR_2026_04_15T20_51_47_00_00Iurn_oid_2.49.0.1.124.2486553930.2026.xml")
	info := chooseAlertInfo(alert, "en")
	feed := feedXML{ID: "CWRS-TEST", Timezone: "America/Regina"}
	visual := alerttext.AlertVisualInput{Severity: info.Severity, Event: "SVR severe thunderstorm warning", BroadcastImmediate: false}

	view := capBannerView(alert, *info, feed, "Spoken alert text.", visual, nil, time.Now().UTC())

	if view.FeedID != "CWRS-TEST" || view.FeedName != "CWRS-TEST" || view.Message != "Spoken alert text." {
		t.Fatalf("banner view identity = %+v", view)
	}
	if len(view.BackgroundGradient) != 2 || view.BackgroundColor != view.BackgroundGradient[0] {
		t.Fatalf("banner view colors = %q %v", view.BackgroundColor, view.BackgroundGradient)
	}
	if len(view.Areas) == 0 || view.AreaText == "" {
		t.Fatalf("banner view areas = %v %q", view.Areas, view.AreaText)
	}
}
