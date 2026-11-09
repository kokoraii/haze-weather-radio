package webgateway

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

// The network alert WebSocket belongs to haze-netalert. haze-web only relays
// the upgrade so the banner page and network clients can reach it on the web
// panel's origin and TLS instead of a second port.
const (
	netAlertWSPath      = "/v1/netalert"
	netAlertDefaultAddr = "0.0.0.0:41802"
)

// netAlertBackend returns haze-netalert's WebSocket URL, dialing loopback when
// the service listens on every interface.
func netAlertBackend(config Config) (*url.URL, bool) {
	netAlert := config.Services.Rust.NetAlert
	if !netAlert.Enabled {
		return nil, false
	}
	host, port, err := net.SplitHostPort(firstNonBlank(strings.TrimSpace(netAlert.Addr), netAlertDefaultAddr))
	if err != nil {
		return nil, false
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return &url.URL{Scheme: "http", Host: net.JoinHostPort(host, port), Path: netAlertWSPath}, true
}

func (s *Server) netAlertProxy(writer http.ResponseWriter, request *http.Request) {
	backend, ok := netAlertBackend(s.config)
	if !ok {
		http.Error(writer, "network alerts are not enabled", http.StatusServiceUnavailable)
		return
	}
	if !strings.EqualFold(strings.TrimSpace(request.Header.Get("Upgrade")), "websocket") {
		http.Error(writer, "WebSocket upgrade required", http.StatusUpgradeRequired)
		return
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(proxied *httputil.ProxyRequest) {
			proxied.Out.URL.Scheme = backend.Scheme
			proxied.Out.URL.Host = backend.Host
			proxied.Out.URL.Path = backend.Path
			proxied.Out.URL.RawQuery = ""
			proxied.Out.Host = backend.Host
			proxied.SetXForwarded()
		},
		ErrorHandler: func(writer http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(writer, "network alert service is unavailable", http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(writer, request)
}
