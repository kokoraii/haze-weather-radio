package webgateway

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// servicePlacement is one managed service section that takes host and port.
// A host that names this machine runs the service here; any other host makes
// the daemon connect to a remote instance (crates/haze/src/service_placement.rs).
type servicePlacement struct {
	path  []string
	label string
	// remote marks services with a remote mode. The rest must stay local.
	remote bool
}

var servicePlacements = []servicePlacement{
	{path: []string{"services", "go", "web_gateway"}, label: "Web panel"},
	{path: []string{"services", "go", "data_ingest"}, label: "Weather data ingest"},
	{path: []string{"services", "go", "tts"}, label: "TTS", remote: true},
	{path: []string{"services", "go", "product_render"}, label: "Product renderer"},
	{path: []string{"services", "go", "playlist"}, label: "Dynamic playlist"},
	{path: []string{"services", "go", "webhook"}, label: "Webhooks"},
	{path: []string{"services", "go", "asr"}, label: "Speech recognition"},
	{path: []string{"services", "go", "ivr"}, label: "Phone IVR"},
	{path: []string{"services", "rust", "location"}, label: "Locations"},
	{path: []string{"services", "rust", "cap_ingest"}, label: "CAP ingest"},
	{path: []string{"services", "rust", "media"}, label: "Media"},
	{path: []string{"services", "rust", "playout"}, label: "Playout"},
	{path: []string{"services", "rust", "cgen"}, label: "CG renderer"},
	{path: []string{"services", "rust", "netalert"}, label: "Network alerts"},
}

const defaultServiceHost = "127.0.0.1"

// addPlacementView copies every service's host and port from source into view.
func addPlacementView(view map[string]any, source map[string]any) {
	for _, service := range servicePlacements {
		section := ensureSection(view, service.path)
		section["host"] = textAt(source, childPath(service.path, "host"), defaultServiceHost, 255)
		section["port"] = textAt(source, childPath(service.path, "port"), "", 12)
	}
}

// validateServicePlacements refuses hosts the daemon could not honor, so a
// saved setting never silently falls back to running the service locally.
func validateServicePlacements(view map[string]any) error {
	for _, service := range servicePlacements {
		host := textAt(view, childPath(service.path, "host"), defaultServiceHost, 255)
		port := textAt(view, childPath(service.path, "port"), "", 12)
		if !validServiceHost(host) {
			return fmt.Errorf("%s host %q is not a host name or IP address", service.label, host)
		}
		if port != "" {
			if number, err := strconv.Atoi(port); err != nil || number < 1 || number > 65535 {
				return fmt.Errorf("%s port %q must be a number from 1 to 65535", service.label, port)
			}
		}
		if isLocalHost(host) {
			continue
		}
		if !service.remote {
			return fmt.Errorf("%s cannot run on a remote host yet; set its host to %s", service.label, defaultServiceHost)
		}
		if port == "" {
			return fmt.Errorf("%s on remote host %s needs a port", service.label, host)
		}
	}
	return nil
}

func validServiceHost(host string) bool {
	if host == "" {
		return false
	}
	for _, char := range host {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune(".-:[]", char)) {
			return false
		}
	}
	return true
}

// isLocalHost matches the daemon's rule: blank, localhost, a loopback or
// unspecified address, or a name or address bound to a local interface.
func isLocalHost(host string) bool {
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if host == "" || strings.EqualFold(host, "localhost") {
		return true
	}
	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else if resolved, err := net.LookupIP(host); err == nil {
		ips = resolved
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsUnspecified() || canBindLocally(ip) {
			return true
		}
	}
	return false
}

// canBindLocally is true only for an address one of this machine's interfaces owns.
func canBindLocally(ip net.IP) bool {
	conn, err := net.ListenPacket("udp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func childPath(path []string, key string) []string {
	return append(append([]string{}, path...), key)
}

func ensureSection(root map[string]any, path []string) map[string]any {
	current := root
	for _, part := range path {
		next, ok := current[part].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[part] = next
		}
		current = next
	}
	return current
}
