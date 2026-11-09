package webgateway

import (
	"strings"
	"testing"
)

func TestValidateServicePlacements(t *testing.T) {
	cases := []struct {
		name    string
		service string
		host    string
		port    any
		wantErr string
	}{
		{name: "local default", service: "playlist", host: "127.0.0.1"},
		{name: "localhost with an unused port", service: "playlist", host: "localhost", port: 9000},
		{name: "remote TTS", service: "tts", host: "192.0.2.22", port: 41810},
		{name: "remote TTS without a port", service: "tts", host: "192.0.2.22", wantErr: "needs a port"},
		{name: "remote service without a remote mode", service: "playlist", host: "192.0.2.22", port: 41811, wantErr: "cannot run on a remote host yet"},
		{name: "port out of range", service: "tts", host: "192.0.2.22", port: "70000", wantErr: "1 to 65535"},
		{name: "host with a path", service: "tts", host: "192.0.2.22/x", port: 41810, wantErr: "not a host name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			section := map[string]any{"enabled": true, "host": tc.host}
			if tc.port != nil {
				section["port"] = tc.port
			}
			source := map[string]any{"services": map[string]any{"go": map[string]any{tc.service: section}}}
			err := validateServicePlacements(daemonSettingsView(source))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("error = %v, want none", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestDaemonSettingsViewDefaultsEveryServiceToThisMachine(t *testing.T) {
	view := daemonSettingsView(map[string]any{})
	for _, service := range servicePlacements {
		if host := textAt(view, childPath(service.path, "host"), "", 255); host != defaultServiceHost {
			t.Fatalf("%s host = %q, want %s", service.label, host, defaultServiceHost)
		}
	}
}
