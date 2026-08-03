package webgateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestServerIPUsesNonLoopbackLocalAddress(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:6444/", nil)
	request = request.WithContext(context.WithValue(request.Context(), http.LocalAddrContextKey, &net.TCPAddr{
		IP:   net.ParseIP("192.168.50.10"),
		Port: 6444,
	}))

	if got := serverIP(request); got != "192.168.50.10" {
		t.Fatalf("serverIP = %q", got)
	}
}

func TestServerIPIgnoresLoopbackLocalAddress(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "http://203.0.113.10:6444/", nil)
	request = request.WithContext(context.WithValue(request.Context(), http.LocalAddrContextKey, &net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 6444,
	}))

	if got := serverIP(request); got != "203.0.113.10" {
		t.Fatalf("serverIP = %q", got)
	}
}

func TestLoadConfigExpandsSiteNameFromEnv(t *testing.T) {
	t.Setenv("SITE_NAME", "Env Haze")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, `version: test
webpanel:
  public:
    site_name: "${SITE_NAME}"
`)
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := siteName(config); got != "Env Haze" {
		t.Fatalf("siteName = %q", got)
	}
}

func TestWebpanelAllowsAnyHost(t *testing.T) {
	var config Config
	server := NewServer(config, ".")

	request := httptest.NewRequest(http.MethodGet, "http://wrong.example/api/public/v1/health", nil)
	request.Host = "wrong.example"
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code == http.StatusMisdirectedRequest {
		t.Fatalf("expected host to pass without allowlist configuration")
	}
}

func TestLoadConfigReadsPublicPort(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, `webpanel:
  public_port:
    enabled: true
    host: "0.0.0.0"
    http_port: 80
    https_port: 443
  tls:
    domains:
      - haze.rai.blue
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !config.Webpanel.PublicPort.Enabled || config.Webpanel.PublicPort.HTTPPort != 80 || config.Webpanel.PublicPort.HTTPSPort != 443 {
		t.Fatalf("public port = %#v", config.Webpanel.PublicPort)
	}
	if len(config.Webpanel.TLS.Domains) != 1 || config.Webpanel.TLS.Domains[0] != "haze.rai.blue" {
		t.Fatalf("tls domains = %#v", config.Webpanel.TLS.Domains)
	}
}

func TestLoadConfigAcceptsQuotedExpandedPort(t *testing.T) {
	t.Setenv("PORT", "6444")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, `webpanel:
  port: "${PORT}"
  public:
    port: "${PORT}"
  admin:
    port: "${PORT}"
`)

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if config.Webpanel.Port.Int() != 6444 || config.Webpanel.Admin.Port.Int() != 6444 {
		t.Fatalf("ports = %#v", config.Webpanel)
	}
}

func TestCachedInterfaceServerIPUsesTTL(t *testing.T) {
	interfaceServerIPMu.Lock()
	previousIP := interfaceServerIPCached
	previousAt := interfaceServerIPCachedAt
	interfaceServerIPCached = "198.51.100.44"
	interfaceServerIPCachedAt = time.Now()
	interfaceServerIPMu.Unlock()
	defer func() {
		interfaceServerIPMu.Lock()
		interfaceServerIPCached = previousIP
		interfaceServerIPCachedAt = previousAt
		interfaceServerIPMu.Unlock()
	}()

	if got := cachedInterfaceServerIP(); got != "198.51.100.44" {
		t.Fatalf("cachedInterfaceServerIP = %q, want cached value", got)
	}

	interfaceServerIPMu.Lock()
	interfaceServerIPCachedAt = time.Now().Add(-interfaceServerIPCacheTTL - time.Second)
	interfaceServerIPMu.Unlock()
	_ = cachedInterfaceServerIP()
	interfaceServerIPMu.Lock()
	refreshedAt := interfaceServerIPCachedAt
	interfaceServerIPMu.Unlock()
	if !refreshedAt.After(time.Now().Add(-5 * time.Second)) {
		t.Fatalf("cache timestamp was not refreshed: %s", refreshedAt)
	}
}

func TestGitCommitIsCachedAfterFirstLookup(t *testing.T) {
	previousValue := gitCommitValue
	t.Setenv("HAZE_GIT_COMMIT", "first")
	gitCommitOnce = sync.Once{}
	gitCommitValue = ""
	defer func() {
		gitCommitOnce = sync.Once{}
		gitCommitValue = previousValue
	}()

	if got := gitCommit(); got != "first" {
		t.Fatalf("gitCommit first = %q", got)
	}
	t.Setenv("HAZE_GIT_COMMIT", "second")
	if got := gitCommit(); got != "first" {
		t.Fatalf("gitCommit cached = %q, want first", got)
	}
}

func TestPublicContentSecurityPolicyUsesLocalScripts(t *testing.T) {
	for _, path := range []string{"/", "/feeds", "/listen", "/alerts", "/alerts/archive", "/api/public/v1/health", "/assets/js/public.js", "/assets/layout.css"} {
		csp := contentSecurityPolicy(path)
		if !strings.Contains(csp, "script-src 'self'") {
			t.Fatalf("public CSP for %s does not restrict scripts to self: %s", path, csp)
		}
		if strings.Contains(csp, "unpkg.com") {
			t.Fatalf("public CSP for %s allows CDN script source: %s", path, csp)
		}
		if strings.Contains(csp, "style-src 'self' 'unsafe-inline'") {
			t.Fatalf("public CSP for %s allows inline styles: %s", path, csp)
		}
		if !strings.Contains(csp, "style-src 'self' https://fonts.googleapis.com") {
			t.Fatalf("public CSP for %s does not restrict style sources: %s", path, csp)
		}
		if !strings.Contains(csp, "worker-src 'self'") {
			t.Fatalf("public CSP for %s does not allow the bundled MapLibre worker: %s", path, csp)
		}
	}
}

func TestPublicContentSecurityPolicyAllowsOnlyTheConfiguredBasemapProvider(t *testing.T) {
	csp := contentSecurityPolicy("/listen")
	for _, directive := range []string{
		"img-src 'self' data: https://tiles.openfreemap.org https://server.arcgisonline.com",
		"connect-src 'self' https://tiles.openfreemap.org https://server.arcgisonline.com ws: wss: stun: stuns: turn: turns:",
	} {
		if !strings.Contains(csp, directive) {
			t.Fatalf("public CSP misses %q: %s", directive, csp)
		}
	}
	if strings.Contains(csp, "connect-src 'self' https: ") || strings.Contains(csp, "img-src 'self' data: https:;") {
		t.Fatalf("public CSP allows arbitrary HTTPS basemap endpoints: %s", csp)
	}
}

func TestAdminContentSecurityPolicyKeepsLucideCompatibility(t *testing.T) {
	csp := contentSecurityPolicy("/admin")
	if !strings.Contains(csp, "https://unpkg.com") {
		t.Fatalf("admin CSP should keep current lucide CDN compatibility: %s", csp)
	}
}

func TestPublicHealthRedactsWebRTCDiagnostics(t *testing.T) {
	server := NewServer(Config{}, ".")
	request := httptest.NewRequest(http.MethodGet, "/api/public/v1/health", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if cache := response.Header().Get("Cache-Control"); cache != "no-store" {
		t.Fatalf("Cache-Control = %q", cache)
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if payload["service"] != "haze-web" {
		t.Fatalf("service = %v", payload["service"])
	}
	capabilities, ok := payload["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("missing capabilities: %#v", payload)
	}
	if _, ok := capabilities["webrtc_opus"].(bool); !ok {
		t.Fatalf("missing webrtc_opus capability: %#v", capabilities)
	}
	if strings.TrimSpace(fmt.Sprint(capabilities["webrtc_default_codec"])) == "" {
		t.Fatalf("missing webrtc_default_codec capability: %#v", capabilities)
	}
	for _, key := range []string{"webrtc_peer_count", "webrtc_peers", "webrtc_source_count", "webrtc_sources"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("public health leaked %s: %#v", key, payload)
		}
	}
}

func TestPublicReadOnlyRoutesRejectStateChangingMethods(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "index.html"), "<!doctype html><title>public</title>")
	mustWrite(t, filepath.Join(dir, "layout.css"), "body{color:white}")
	config := Config{}
	config.Webpanel.Public.AlertsArchive.Access = "public"
	server := NewServerWithConfigPath(config, "config.yaml", dir)

	for _, path := range []string{"/", "/feeds", "/listen", "/alerts", "/api/public/v1/health", "/assets/layout.css"} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status = %d, want %d", path, response.Code, http.StatusMethodNotAllowed)
		}
		if allow := response.Header().Get("Allow"); allow != "GET, HEAD" {
			t.Fatalf("%s Allow = %q", path, allow)
		}
		if strings.HasPrefix(path, "/api/public/") || publicHTMLPath(path) {
			if cache := response.Header().Get("Cache-Control"); cache != "no-store" {
				t.Fatalf("%s Cache-Control = %q, want no-store", path, cache)
			}
		}
	}
}

func TestBundledPublicIndexIncludesTLSNoticeHooks(t *testing.T) {
	raw, err := os.ReadFile(repoFixturePath(t, "bundle", "webroot", "index.html"))
	if err != nil {
		t.Fatal(err)
	}
	html := string(raw)
	for _, fragment := range []string{`id="publicTlsNotice"`, `id="publicTlsNoticeText"`, `class="tls-notice public-tls-notice"`} {
		if !strings.Contains(html, fragment) {
			t.Fatalf("public index missing %s", fragment)
		}
	}
}

func TestBundledPublicIndexPlacesOldWebBannerAfterProjectStrip(t *testing.T) {
	raw, err := os.ReadFile(repoFixturePath(t, "bundle", "webroot", "index.html"))
	if err != nil {
		t.Fatal(err)
	}
	html := string(raw)
	projectIndex := strings.Index(html, `class="public-project-strip"`)
	bannerIndex := strings.Index(html, `class="public-oldweb-banner"`)
	if projectIndex < 0 || bannerIndex < 0 {
		t.Fatalf("missing project strip or old web banner")
	}
	if bannerIndex < projectIndex {
		t.Fatalf("old web banner should be after project strip")
	}
	if !strings.Contains(html, `src="/assets/haze_banner.gif"`) {
		t.Fatalf("old web banner should use bundled haze_banner.gif")
	}
}

func TestAdminURLDoesNotEchoPublicHostByDefault(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/", nil)
	request.Host = "attacker.example"

	if got := adminURL(Config{}, request); got != "/admin" {
		t.Fatalf("adminURL default = %q, want /admin", got)
	}
}

func TestAdminURLIsRelativeWhenAdminSharesPublicPort(t *testing.T) {
	config := Config{}
	config.Webpanel.Port = 6444
	config.Webpanel.Admin.Host = "0.0.0.0"
	config.Webpanel.Admin.Port = 6444
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:6444/", nil)
	request.Host = "attacker.example"

	if got := adminURL(config, request); got != "/admin" {
		t.Fatalf("adminURL shared port = %q, want /admin", got)
	}
}

func TestAdminURLUsesConfiguredSeparateAdminPort(t *testing.T) {
	config := Config{}
	config.Webpanel.Port = 6444
	config.Webpanel.Admin.Host = "0.0.0.0"
	config.Webpanel.Admin.Port = 9000
	request := httptest.NewRequest(http.MethodGet, "https://panel.example/", nil)
	request.Host = "panel.example"
	request.Header.Set("X-Forwarded-Proto", "https")

	if got := adminURL(config, request); got != "https://panel.example:9000/admin" {
		t.Fatalf("adminURL separate port = %q", got)
	}
}

func repoFixturePath(t *testing.T, parts ...string) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		candidateParts := append([]string{dir}, parts...)
		candidate := filepath.Join(candidateParts...)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find repo fixture %s", filepath.Join(parts...))
		}
		dir = parent
	}
}

func TestAdminHealthRequiresAuthAndIncludesWebRTCDiagnostics(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	config := authEnabledConfig()
	server := NewServer(config, ".")

	unauthorized := httptest.NewRecorder()
	server.Handler().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}

	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/health?token="+url.QueryEscape(token), nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if _, ok := payload["webrtc_peer_count"].(float64); !ok {
		t.Fatalf("missing webrtc_peer_count: %#v", payload)
	}
	if _, ok := payload["webrtc_peers"].([]any); !ok {
		t.Fatalf("missing webrtc_peers: %#v", payload)
	}
	if _, ok := payload["webrtc_source_count"].(float64); !ok {
		t.Fatalf("missing webrtc_source_count: %#v", payload)
	}
	if _, ok := payload["webrtc_sources"].([]any); !ok {
		t.Fatalf("missing webrtc_sources: %#v", payload)
	}
}

func TestAdminRedirectsWithoutSession(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	config := authEnabledConfig()
	server := NewServer(config, ".")
	request := httptest.NewRequest(http.MethodGet, "/admin", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusSeeOther {
		t.Fatalf("status = %d", response.Code)
	}
	if location := response.Header().Get("Location"); !strings.HasPrefix(location, "/login?next=") {
		t.Fatalf("location = %q", location)
	}
}

func TestBannerDoesNotRequireSession(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "banner.html"), "<!doctype html><title>banner</title>")
	server := NewServerWithSurface(authEnabledConfig(), "config.yaml", dir, "admin")
	request := httptest.NewRequest(http.MethodGet, "/banner?feed=CAP-IT-ALL", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "banner") {
		t.Fatalf("body = %q", response.Body.String())
	}
}

func TestBannerStreamDoesNotRequireSession(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, "")
	server := NewServerWithSurface(authEnabledConfig(), configPath, dir, "admin")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/banner/stream?feed=CAP-IT-ALL", nil).WithContext(ctx)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code == http.StatusUnauthorized || response.Code == http.StatusSeeOther {
		t.Fatalf("status = %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "event: banner") {
		t.Fatalf("body = %q", response.Body.String())
	}
}

func TestBannerCurrentDoesNotRequireSession(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, "")
	server := NewServerWithSurface(authEnabledConfig(), configPath, dir, "admin")
	request := httptest.NewRequest(http.MethodGet, "/api/v1/banner/current?feed=CAP-IT-ALL", nil)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("body was not JSON: %v body=%q", err, response.Body.String())
	}
	if payload["feed_id"] != "CAP-IT-ALL" {
		t.Fatalf("feed_id = %v body=%q", payload["feed_id"], response.Body.String())
	}
}

func TestPublicSurfaceDoesNotExposeAdminRoutes(t *testing.T) {
	server := NewServerWithSurface(Config{}, "config.yaml", ".", "public")

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/admin", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestAdminSurfaceDoesNotExposePublicHealth(t *testing.T) {
	server := NewServerWithSurface(Config{}, "config.yaml", ".", "admin")

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/public/v1/health", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestPublicSurfaceDoesNotExposeAdminHealth(t *testing.T) {
	server := NewServerWithSurface(Config{}, "config.yaml", ".", "public")

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestCombinedSurfaceServesPublicAlertsArchiveAliasWhenEnabled(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "index.html"), "<!doctype html><title>public</title>")
	config := Config{}
	config.Webpanel.Public.AlertsArchive.Access = "public"
	server := NewServerWithSurface(config, "config.yaml", dir, "combined")

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/alerts/archive", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestCombinedSurfaceHidesPublicAlertsArchiveWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "index.html"), "<!doctype html><title>public</title>")
	server := NewServerWithSurface(Config{}, "config.yaml", dir, "combined")

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/alerts/archive", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestAdminSurfaceDoesNotExposePublicFeedSocket(t *testing.T) {
	server := NewServerWithSurface(Config{}, "config.yaml", ".", "admin")

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/feeds", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestPublicListenPageServedWhenFeedsAvailable(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	mustWrite(t, filepath.Join(dir, "index.html"), "<!doctype html><title>public listener</title>")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, dir)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/listen?feed=sk-0001&codec=opus", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), "public listener") {
		t.Fatalf("body = %q", response.Body.String())
	}
}

func TestPublicListenPageServedForHTTPOnlyFeeds(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	mustWrite(t, filepath.Join(dir, "index.html"), "<!doctype html><title>http only listener</title>")
	mustWrite(t, filepath.Join(dir, "config.yaml"), `version: test
feeds_file: managed/configs/feeds.xml
outputs_file: managed/configs/output.xml
webpanel:
  public:
    site_name: Test Haze
    feeds:
      access: public
      webrtc:
        enabled: false
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "output.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<outputs>
  <feed id="sk-0001"><webrtc enabled="false"/><stream enabled="true"/></feed>
</outputs>
`)
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, dir)
	server.media = newMemoryMediaHub()

	listen := httptest.NewRecorder()
	server.Handler().ServeHTTP(listen, httptest.NewRequest(http.MethodGet, "/listen?feed=sk-0001&codec=mp3", nil))
	if listen.Code != http.StatusOK {
		t.Fatalf("HTTP-only listen status = %d", listen.Code)
	}
	feeds := httptest.NewRecorder()
	server.Handler().ServeHTTP(feeds, httptest.NewRequest(http.MethodGet, "/feeds", nil))
	if feeds.Code != http.StatusOK {
		t.Fatalf("HTTP-only feeds status = %d", feeds.Code)
	}
	audio := httptest.NewRecorder()
	server.Handler().ServeHTTP(audio, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=sk-0001&codec=pcm16", nil))
	if audio.Code != http.StatusOK {
		t.Fatalf("HTTP-only audio HEAD status = %d", audio.Code)
	}

	state, err := publicStatePayload(config, configPath, time.Now().UTC(), httptest.NewRequest(http.MethodGet, "/api/public/v1/panel/ws?feeds=1", nil), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	summary := state["summary"].(map[string]any)
	if summary["webrtc_enabled"] != false {
		t.Fatalf("summary should report WebRTC disabled: %#v", summary)
	}
	publicFeeds := summary["feeds"].([]map[string]any)
	if len(publicFeeds) != 1 {
		t.Fatalf("public feeds = %#v", publicFeeds)
	}
	if publicFeeds[0]["webrtc_enabled"] != false || publicFeeds[0]["http_stream_enabled"] != true {
		t.Fatalf("HTTP-only public feed flags = %#v", publicFeeds[0])
	}
}

func TestPublicFeedPagesRemainAvailableWithoutStreamingOutputs(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	mustWrite(t, filepath.Join(dir, "index.html"), "<!doctype html><title>feed information</title>")
	mustWrite(t, filepath.Join(dir, "managed", "configs", "output.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<outputs>
  <feed id="sk-0001"><webrtc enabled="false"/><stream enabled="false"/></feed>
</outputs>
`)
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, dir)

	for _, path := range []string{"/feeds", "/listen?feed=sk-0001"} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want %d", path, response.Code, http.StatusOK)
		}
		if !strings.Contains(response.Body.String(), "feed information") {
			t.Fatalf("%s body = %q", path, response.Body.String())
		}
	}

	feeds, err := loadBasicFeedSummaries(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !publicFeedPagesAvailable(config, feeds) {
		t.Fatal("enabled feed should remain browsable without streaming output")
	}
}

func TestPublicFeedPagesRequireAnEnabledFeed(t *testing.T) {
	config := Config{}
	config.Webpanel.Public.Feeds.Access = "public"
	feeds := []map[string]any{{
		"id":                  "sk-0001",
		"enabled":             false,
		"webrtc_enabled":      true,
		"http_stream_enabled": true,
	}}
	if publicFeedPagesAvailable(config, feeds) {
		t.Fatal("disabled feeds should not expose public feed pages")
	}
}

func TestOutputLabelsUseStreamType(t *testing.T) {
	labels := outputLabels(outputXML{
		Stream: outputNodeXML{
			EnabledRaw: "true",
			Type:       "icecast",
		},
	})
	if len(labels) != 1 || labels[0] != "icecast" {
		t.Fatalf("labels = %#v", labels)
	}
}

func TestWildcardIcecastOutputEnablesPublicHTTPAudio(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "config.yaml"), `version: test
feeds_file: managed/configs/feeds.xml
outputs_file: managed/configs/output.xml
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="cwxr-on01" enabled="true" />
  <feed id="cwxr-sk01" enabled="true" />
</feeds>
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "output.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<outputs>
  <feed id="cwxr-*">
    <webrtc enabled="false"/>
    <icecast enabled="true"><format>opus</format><acodec>libopus</acodec></icecast>
  </feed>
</outputs>
`)

	feeds, err := loadBasicFeedSummaries(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(feeds) != 2 {
		t.Fatalf("feeds = %#v", feeds)
	}
	for _, feed := range feeds {
		if feed["http_stream_enabled"] != true {
			t.Fatalf("feed did not inherit wildcard icecast output: %#v", feed)
		}
	}
	outputs, err := loadOutputsXML(filepath.Join(dir, "config.yaml"), map[string]any{"outputs_file": "managed/configs/output.xml"})
	if err != nil {
		t.Fatal(err)
	}
	labels := outputLabels(outputForFeed(outputs, "cwxr-on01"))
	if len(labels) != 1 || labels[0] != "icecast" {
		t.Fatalf("labels = %#v", labels)
	}
}

func TestPublicAboutPageIsRemoved(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "index.html"), "<!doctype html><title>about</title>")
	server := NewServerWithSurface(Config{}, "config.yaml", dir, "public")

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/about", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestAdminAllowsValidSessionCookie(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	config := authEnabledConfig()
	server := NewServer(config, ".")
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/admin", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code == http.StatusSeeOther || response.Code == http.StatusUnauthorized {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestWebSocketLoginReturnsToken(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	server := NewServer(authEnabledConfig(), ".")
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/v1/panel/ws?mode=control"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	readType(t, ctx, conn, "hello")
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"login","request_id":"r1","password":"secret"}`)); err != nil {
		t.Fatalf("write: %v", err)
	}
	payload := readType(t, ctx, conn, "auth_ok")
	if payload["reply_to"] != "r1" {
		t.Fatalf("reply_to = %v", payload["reply_to"])
	}
	if payload["token"] == "" {
		t.Fatal("token was empty")
	}
}

func TestHTTPLoginSetsCookieAndReturnsToken(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	server := NewServer(authEnabledConfig(), ".")
	response := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"http://example.test/api/v1/auth/login",
		bytes.NewBufferString(`{"password":"secret"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "http://example.test")

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload["type"] != "auth_ok" {
		t.Fatalf("type = %v", payload["type"])
	}
	if payload["token"] == "" {
		t.Fatal("token was empty")
	}
	cookies := response.Result().Cookies()
	if len(cookies) == 0 || cookies[0].Name != sessionCookieName {
		t.Fatalf("login did not set session cookie: %#v", cookies)
	}
}

func TestHTTPLoginUsesLANCompatibleCookieWhenSecureCookiesEnabled(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	config := authEnabledConfig()
	config.Webpanel.Authentication.SecureCookies = true
	server := NewServer(config, ".")
	response := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"http://172.16.1.38:6444/api/v1/auth/login",
		bytes.NewBufferString(`{"password":"secret"}`),
	)
	request.Header.Set("Content-Type", "application/json")

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	cookies := response.Result().Cookies()
	if len(cookies) == 0 || cookies[0].Name != sessionCookieName {
		t.Fatalf("login did not set session cookie: %#v", cookies)
	}
	if cookies[0].Secure {
		t.Fatalf("LAN login cookie should not be Secure over local HTTP: %#v", cookies[0])
	}
}

func TestHTTPLoginUsesSecureCookieForDomainWhenSecureCookiesEnabled(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	config := authEnabledConfig()
	config.Webpanel.Authentication.SecureCookies = true
	server := NewServer(config, ".")
	response := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"https://cwrs.rai.blue/api/v1/auth/login",
		bytes.NewBufferString(`{"password":"secret"}`),
	)
	request.Header.Set("Content-Type", "application/json")

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	cookies := response.Result().Cookies()
	if len(cookies) == 0 || cookies[0].Name != sessionCookieName {
		t.Fatalf("login did not set session cookie: %#v", cookies)
	}
	if !cookies[0].Secure {
		t.Fatalf("domain login cookie should be Secure: %#v", cookies[0])
	}
}

func TestAdminStateEndpointRequiresAuth(t *testing.T) {
	server := NewServer(authEnabledConfig(), ".")
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/panel/state", nil)

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestAdminStateEndpointReturnsPanelStateWithCookie(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	writePanelFixture(t, dir)
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, dir)
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/panel/state", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["summary"] == nil {
		t.Fatalf("missing summary: %#v", payload)
	}
}

func TestAdminCommandRejectsCookieOnlyCrossSiteStyleRequest(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	server := NewServer(authEnabledConfig(), ".")
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/panel/command", strings.NewReader(`{"command":"health"}`))
	request.Header.Set("Content-Type", "application/json")
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestAdminCommandAcceptsExplicitAdminIntent(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	server := NewServer(authEnabledConfig(), ".")
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/panel/command", strings.NewReader(`{"command":"health"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Haze-Admin-Intent", "command")
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["type"] != "command_result" {
		t.Fatalf("payload = %#v", payload)
	}
	result, _ := payload["result"].(map[string]any)
	if result["service"] != "haze-web" {
		t.Fatalf("result = %#v", result)
	}
}

func TestAdminSSESendsInitialState(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	writePanelFixture(t, dir)
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, dir)
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	request, err := http.NewRequest(http.MethodGet, httpServer.URL+"/api/v1/panel/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	scanner := bufio.NewScanner(response.Body)
	if !scanner.Scan() {
		t.Fatalf("missing SSE event line: %v", scanner.Err())
	}
	if got := scanner.Text(); got != "event: admin_state" {
		t.Fatalf("first SSE line = %q", got)
	}
}

func TestPublicStateUsesConfiguredPublicHostname(t *testing.T) {
	t.Setenv("WEB_HOSTNAME", "cwrs.rai.blue")
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://public.example/api/public/v1/panel/state", nil)
	state, err := publicStatePayload(config, configPath, time.Now().UTC(), request, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	summary := state["summary"].(map[string]any)
	if summary["public_base_url"] != "https://cwrs.rai.blue" {
		t.Fatalf("public_base_url = %v", summary["public_base_url"])
	}
	if summary["admin_url"] != "https://cwrs.rai.blue/admin" {
		t.Fatalf("admin_url = %v", summary["admin_url"])
	}
}

func TestPublicStateKeepsLANHostWhenPublicHostnameConfigured(t *testing.T) {
	t.Setenv("WEB_HOSTNAME", "cwrs.rai.blue")
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://172.16.1.38:6444/api/public/v1/panel/state", nil)
	state, err := publicStatePayload(config, configPath, time.Now().UTC(), request, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	summary := state["summary"].(map[string]any)
	if summary["public_base_url"] != "http://172.16.1.38:6444" {
		t.Fatalf("public_base_url = %v", summary["public_base_url"])
	}
	if summary["admin_url"] != "/admin" {
		t.Fatalf("admin_url = %v", summary["admin_url"])
	}
}

func TestPublicIndexUsesOnAirNameForSEO(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	mustWrite(t, filepath.Join(dir, "index.html"), `<!doctype html>
<meta name="description" content="Live weather radio feeds, alerts, and public status.">
<title>Weather Radio Live Feeds</title>
<h1 id="publicSiteTitle">Weather Radio Live Feeds</h1>`)
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, dir)
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))

	body := response.Body.String()
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, body)
	}
	if !strings.Contains(body, "<title>Canada RadioMET</title>") {
		t.Fatalf("title was not rewritten: %s", body)
	}
	if !strings.Contains(body, "Live weather radio feeds, alerts, and public status for Canada RadioMET.") {
		t.Fatalf("description was not rewritten: %s", body)
	}
}

func TestAssetsServeStaticFilesButNotHTMLEntrypoints(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "admin.html"), "<!doctype html><title>admin</title>")
	mustWrite(t, filepath.Join(dir, "styles.css"), "body{color:white}")
	mustWrite(t, filepath.Join(dir, "site.webmanifest"), `{"name":"Haze Weather Radio"}`)
	mustWrite(t, filepath.Join(dir, "haze_banner.gif"), "GIF89a")
	mustWrite(t, filepath.Join(dir, ".env"), "ADMIN_PASSWD=secret")
	mustWrite(t, filepath.Join(dir, "js", "public.js.map"), `{"sources":["public.js"]}`)
	mustWrite(t, filepath.Join(dir, "js", ".secret.js"), "console.log('secret')")
	mustWrite(t, filepath.Join(dir, "js", "public.js"), "console.log('ok')")
	server := NewServer(Config{}, dir)

	for _, item := range []struct {
		path       string
		wantStatus int
		wantType   string
		wantCache  string
	}{
		{path: "/assets/styles.css", wantStatus: http.StatusOK, wantCache: "no-cache, must-revalidate"},
		{path: "/assets/js/public.js", wantStatus: http.StatusOK, wantCache: "no-cache, must-revalidate"},
		{path: "/assets/site.webmanifest", wantStatus: http.StatusOK, wantType: "application/manifest+json", wantCache: "public, max-age=3600, must-revalidate"},
		{path: "/assets/haze_banner.gif", wantStatus: http.StatusOK, wantCache: "public, max-age=86400"},
		{path: "/assets/admin.html", wantStatus: http.StatusNotFound},
		{path: "/assets/.env", wantStatus: http.StatusNotFound},
		{path: "/assets/js/.secret.js", wantStatus: http.StatusNotFound},
		{path: "/assets/js/public.js.map", wantStatus: http.StatusNotFound},
		{path: "/assets/", wantStatus: http.StatusNotFound},
		{path: "/assets/js/", wantStatus: http.StatusNotFound},
		{path: "/assets/%2e%2e/config.yaml", wantStatus: http.StatusBadRequest},
		{path: "/assets/C:/Windows/win.ini", wantStatus: http.StatusNotFound},
	} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, item.path, nil))
		if response.Code != item.wantStatus {
			t.Fatalf("%s status = %d, want %d", item.path, response.Code, item.wantStatus)
		}
		if item.wantType != "" && !strings.Contains(response.Header().Get("Content-Type"), item.wantType) {
			t.Fatalf("%s Content-Type = %q, want %q", item.path, response.Header().Get("Content-Type"), item.wantType)
		}
		if item.wantCache != "" && response.Header().Get("Cache-Control") != item.wantCache {
			t.Fatalf("%s Cache-Control = %q, want %q", item.path, response.Header().Get("Cache-Control"), item.wantCache)
		}
	}
}

func TestCgenManagedFontAssetsAreAdminOnly(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	webroot := filepath.Join(dir, "webroot")
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, filepath.Join(webroot, "admin.html"), "<!doctype html><title>admin</title>")
	mustWrite(t, configPath, "")
	mustWrite(t, filepath.Join(dir, "managed", "fonts", "AlertSans-Bold.woff2"), "font")
	mustWrite(t, filepath.Join(dir, "managed", "fonts", "not-a-font.txt"), "secret")
	mustWrite(t, filepath.Join(dir, "managed", "fonts", ".hidden.woff2"), "hidden")
	server := NewServerWithConfigPath(authEnabledConfig(), configPath, webroot)

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/cgen/fonts/AlertSans-Bold.woff2", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", response.Code)
	}

	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		path       string
		wantStatus int
		wantType   string
		wantCache  string
	}{
		{
			path:       "/api/v1/cgen/fonts/AlertSans-Bold.woff2",
			wantStatus: http.StatusOK,
			wantType:   "font/woff2",
			wantCache:  "private, max-age=3600",
		},
		{path: "/api/v1/cgen/fonts/not-a-font.txt", wantStatus: http.StatusNotFound},
		{path: "/api/v1/cgen/fonts/.hidden.woff2", wantStatus: http.StatusNotFound},
		{path: "/api/v1/cgen/fonts/", wantStatus: http.StatusNotFound},
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, item.path, nil)
		request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
		server.Handler().ServeHTTP(response, request)
		if response.Code != item.wantStatus {
			t.Fatalf("%s status = %d, want %d", item.path, response.Code, item.wantStatus)
		}
		if item.wantType != "" && !strings.Contains(response.Header().Get("Content-Type"), item.wantType) {
			t.Fatalf("%s Content-Type = %q, want %q", item.path, response.Header().Get("Content-Type"), item.wantType)
		}
		if item.wantCache != "" && response.Header().Get("Cache-Control") != item.wantCache {
			t.Fatalf("%s Cache-Control = %q, want %q", item.path, response.Header().Get("Cache-Control"), item.wantCache)
		}
	}
}

func TestWebSocketLoginTokenOpensAdminAndAuthenticatedStream(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "admin.html"), "<!doctype html><title>admin</title>")
	mustWrite(t, filepath.Join(dir, "login.html"), "<!doctype html><title>login</title>")
	server := NewServer(authEnabledConfig(), dir)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	loginWS := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/v1/panel/ws?mode=control"
	loginConn, _, err := websocket.Dial(ctx, loginWS, nil)
	if err != nil {
		t.Fatalf("login dial: %v", err)
	}
	readType(t, ctx, loginConn, "hello")
	writeWS(t, ctx, loginConn, map[string]any{
		"type":       "login",
		"request_id": "login1",
		"password":   "secret",
	})
	loginReply := readType(t, ctx, loginConn, "auth_ok")
	loginConn.CloseNow()
	token, _ := loginReply["token"].(string)
	if token == "" {
		t.Fatal("token was empty")
	}

	client := &http.Client{
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	adminURL := httpServer.URL + "/admin?token=" + url.QueryEscape(token)
	response, err := client.Get(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusSeeOther {
		t.Fatalf("admin status = %d", response.StatusCode)
	}
	if response.Header.Get("Location") != "/admin" {
		t.Fatalf("admin location = %q", response.Header.Get("Location"))
	}
	cookies := response.Cookies()
	if len(cookies) == 0 || cookies[0].Name != sessionCookieName {
		t.Fatalf("admin did not set session cookie: %#v", response.Cookies())
	}
	if !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("admin session cookie is missing required protections: %#v", cookies[0])
	}

	adminRequest, err := http.NewRequest(http.MethodGet, httpServer.URL+"/admin", nil)
	if err != nil {
		t.Fatal(err)
	}
	adminRequest.AddCookie(cookies[0])
	adminResponse, err := http.DefaultClient.Do(adminRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer adminResponse.Body.Close()
	if adminResponse.StatusCode != http.StatusOK {
		t.Fatalf("admin follow-up status = %d", adminResponse.StatusCode)
	}

	controlWS := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/v1/panel/ws?mode=control&token=" + url.QueryEscape(token)
	controlConn, _, err := websocket.Dial(ctx, controlWS, nil)
	if err != nil {
		t.Fatalf("control dial: %v", err)
	}
	defer controlConn.CloseNow()
	readType(t, ctx, controlConn, "hello")
	writeWS(t, ctx, controlConn, map[string]any{
		"type":       "auth_check",
		"request_id": "auth1",
	})
	authReply := readType(t, ctx, controlConn, "auth_state")
	if authReply["authenticated"] != true {
		t.Fatalf("auth_state = %#v", authReply)
	}
}

func TestWebSocketDaemonSettingsCommandRoundTrip(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.MkdirAll(filepath.Join(dir, "managed"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`version: test
daemon_settings_file: runtime/state/daemonSettings.json
services:
  rust:
    cap_ingest:
      enabled: true
      source_id: rust-cap
      source: naads
      mode: tcp
      url: tcp://streaming1.naad-adna.pelmorex.com:8080
      fallback_url: tcp://streaming2.naad-adna.pelmorex.com:8080
      archive_url: http://capcp1.naad-adna.pelmorex.com
      fallback_archive_url: http://capcp2.naad-adna.pelmorex.com
      interval: 5s
      timeout: 15s
      startup_seed: true
      concurrency: 8
  go:
    enabled: true
    web_gateway:
      enabled: true
      addr: "127.0.0.1:6444"
    tts:
      enabled: true
      readers: managed/configs/readers.xml
      provider: auto
      language: en-CA
      out_dir: runtime/audio/tts
      timeout: 60s
  daemon:
    enabled: true
    scheduler:
      enabled: true
    playlist:
      enabled: true
      interval_ms: 750
webpanel:
  public:
    enabled: true
    feeds:
      access: disabled
      webrtc:
        enabled: true
  admin:
    enabled: true
  receiver:
    enabled: false
`), 0o600); err != nil {
		t.Fatal(err)
	}

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/v1/panel/ws?mode=control&token=" + token
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	readType(t, ctx, conn, "hello")

	writeWS(t, ctx, conn, map[string]any{
		"type":       "command",
		"request_id": "get1",
		"command":    "daemon.settings.get",
		"payload":    map[string]any{},
	})
	getReply := readType(t, ctx, conn, "command_result")
	getResult := getReply["result"].(map[string]any)
	effective := getResult["effective"].(map[string]any)
	services := effective["services"].(map[string]any)
	rustServices := services["rust"].(map[string]any)
	capIngest := rustServices["cap_ingest"].(map[string]any)
	capIngest["enabled"] = false

	writeWS(t, ctx, conn, map[string]any{
		"type":       "command",
		"request_id": "save1",
		"command":    "daemon.settings.save",
		"payload": map[string]any{
			"settings": effective,
		},
	})
	saveReply := readType(t, ctx, conn, "command_result")
	saveResult := saveReply["result"].(map[string]any)
	if saveResult["pending_restart"] != true {
		t.Fatalf("pending_restart = %v", saveResult["pending_restart"])
	}
	raw, err := os.ReadFile(filepath.Join(dir, "runtime", "state", "daemonSettings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"cap_ingest"`) || !strings.Contains(string(raw), `"enabled": false`) {
		t.Fatalf("unexpected overlay: %s", raw)
	}
}

func TestWebSocketStateCommandIncludesConfiguredFeeds(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	t.Setenv("SAME_ID", "WXR123")
	dir := t.TempDir()
	writePanelFixture(t, dir)

	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/v1/panel/ws?mode=control&token=" + token
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	readType(t, ctx, conn, "hello")

	writeWS(t, ctx, conn, map[string]any{
		"type":       "command",
		"request_id": "state1",
		"command":    "state",
		"payload":    map[string]any{},
	})
	reply := readType(t, ctx, conn, "command_result")
	result := reply["result"].(map[string]any)
	summary := result["summary"].(map[string]any)
	if summary["feed_count"].(float64) != 1 {
		t.Fatalf("feed_count = %v", summary["feed_count"])
	}
	feeds := summary["feeds"].([]any)
	feed := feeds[0].(map[string]any)
	if feed["id"] != "sk-0001" || feed["name"] != "Saskatoon" {
		t.Fatalf("feed = %#v", feed)
	}
	codes := feed["clc_codes"].([]any)
	if len(codes) != 2 {
		t.Fatalf("clc_codes = %#v", codes)
	}
	sameLocations := feed["same_locations"].([]any)
	if len(sameLocations) != 2 || sameLocations[0] != "065500" || sameLocations[1] != "065522" {
		t.Fatalf("same_locations = %#v", sameLocations)
	}
	regions := feed["coverage_regions"].([]any)
	if len(regions) != 1 {
		t.Fatalf("coverage_regions = %#v", regions)
	}
	region := regions[0].(map[string]any)
	if region["name"] != "Outlook - Watrous - Hanley - Imperial - Dinsmore" {
		t.Fatalf("region = %#v", region)
	}
	subregions := region["subregions"].([]any)
	if len(subregions) != 1 || subregions[0].(map[string]any)["name"] != "R.M. of Rudy including Outlook and Glenside" {
		t.Fatalf("subregions = %#v", subregions)
	}
	configView := result["config"].(map[string]any)
	same := configView["same"].(map[string]any)
	if same["sender"] != "WXR123" {
		t.Fatalf("sender = %v", same["sender"])
	}
}

func TestStateSignatureIgnoresVolatilePanelFields(t *testing.T) {
	base := map[string]any{
		"summary": map[string]any{
			"uptime_seconds": float64(1),
			"feeds": []any{
				map[string]any{
					"id": "sk-0001",
					"runtime": map[string]any{
						"now_playing": "Idle",
					},
				},
			},
		},
		"events": []any{
			map[string]any{
				"kind":      "gateway",
				"timestamp": "2026-06-15T00:00:00Z",
				"message":   "state",
			},
		},
		"last_connected": map[string]any{
			"ip": "127.0.0.1",
			"at": "2026-06-15T00:00:00Z",
		},
	}
	updatedClockOnly := cloneMap(base)
	updatedClockOnly["summary"].(map[string]any)["uptime_seconds"] = float64(90)
	updatedClockOnly["events"].([]any)[0].(map[string]any)["timestamp"] = "2026-06-15T00:01:00Z"
	updatedClockOnly["last_connected"].(map[string]any)["at"] = "2026-06-15T00:01:00Z"

	if stateSignature(base) != stateSignature(updatedClockOnly) {
		t.Fatal("clock-only changes should not force a panel state broadcast")
	}

	changedFeed := cloneMap(base)
	feeds := changedFeed["summary"].(map[string]any)["feeds"].([]any)
	feeds[0].(map[string]any)["runtime"].(map[string]any)["now_playing"] = "Date and Time"
	if stateSignature(base) == stateSignature(changedFeed) {
		t.Fatal("feed runtime changes must force a panel state broadcast")
	}
}

func TestAllLocationFeedSameLocationsIncludeNationalCanadaAndUS(t *testing.T) {
	clcNames := map[string]string{
		"065522": "City of Saskatoon",
	}
	nwsNames := map[string]string{
		"013121": "Fulton, GA",
	}
	var feed feedXML
	feed.Alerts.CapCP.EnabledRaw = "true"
	feed.Alerts.NWSCAP.EnabledRaw = "true"

	locations := feedSameLocations(feed, clcNames, nwsNames)
	if len(locations) != 3 {
		t.Fatalf("same locations = %#v", locations)
	}
	if locations[0] != "000000" {
		t.Fatalf("national SAME code should be first: %#v", locations)
	}
	if !containsString(locations, "065522") || !containsString(locations, "013121") {
		t.Fatalf("all-location feed should include Canada CLC and US SAME/FIPS codes: %#v", locations)
	}
}

func TestWildcardCoverageFeedSameLocationsExpandCLCRegions(t *testing.T) {
	clcNames := map[string]string{
		"065522": "City of Saskatoon",
		"075520": "City of Calgary",
		"085500": "Brandon",
	}
	feed := feedXML{}
	feed.Locations.Coverage.Regions = []coverageRegionXML{
		{ID: "06*", Source: "eccc"},
		{ID: "07*", Source: "eccc"},
	}

	codes := feedCoverageCodes(feed, clcNames)
	if _, ok := codes["065522"]; !ok {
		t.Fatalf("expected SK CLC in coverage codes: %#v", sortedKeys(codes))
	}
	if _, ok := codes["075520"]; !ok {
		t.Fatalf("expected AB CLC in coverage codes: %#v", sortedKeys(codes))
	}
	if _, ok := codes["085500"]; ok {
		t.Fatalf("unexpected MB CLC in coverage codes: %#v", sortedKeys(codes))
	}

	locations := feedSameLocations(feed, clcNames, nil)
	if strings.Join(locations, ",") != "065522,075520" {
		t.Fatalf("same locations = %#v", locations)
	}
}

func TestPublicWebSocketSendsPublicState(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/public/v1/panel/ws?feeds=1"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	readType(t, ctx, conn, "hello")
	state := readType(t, ctx, conn, "public_state")
	summary := state["data"].(map[string]any)["summary"].(map[string]any)
	if summary["feeds_access"] != "public" || summary["webrtc_enabled"] != true || summary["media_available"] != false {
		t.Fatalf("summary = %#v", summary)
	}
	for _, key := range []string{"ip_address", "version", "git_commit", "os", "architecture"} {
		if strings.TrimSpace(fmt.Sprint(summary[key])) == "" {
			t.Fatalf("missing public summary %s in %#v", key, summary)
		}
	}
	capabilities := summary["capabilities"].(map[string]any)
	if _, ok := capabilities["webrtc_opus"].(bool); !ok {
		t.Fatalf("missing public webrtc_opus capability: %#v", capabilities)
	}
	if strings.TrimSpace(fmt.Sprint(capabilities["webrtc_default_codec"])) == "" {
		t.Fatalf("missing public webrtc_default_codec capability: %#v", capabilities)
	}
	feeds := summary["feeds"].([]any)
	if len(feeds) != 1 {
		t.Fatalf("feeds = %#v", feeds)
	}
	feed := feeds[0].(map[string]any)
	if feed["id"] != "sk-0001" {
		t.Fatalf("feed = %#v", feed)
	}
	for _, key := range []string{
		"alert_queue_depth",
		"clc_codes",
		"coverage_regions",
		"languages",
		"location_count",
		"outputs",
		"playlist_items",
		"recent_alerts",
		"same_all_locations",
		"same_locations",
		"timezone",
	} {
		if _, ok := feed[key]; ok {
			t.Fatalf("public feed leaked admin field %s: %#v", key, feed)
		}
	}
	transmitter, _ := feed["transmitter"].(map[string]any)
	if transmitter["site_name"] != "Saskatoon" || transmitter["callsign"] != "XLF322" {
		t.Fatalf("public transmitter = %#v", transmitter)
	}
	for _, key := range []string{"gpclk", "gpio", "rds", "frequency_mhz", "relationship"} {
		if _, ok := transmitter[key]; ok {
			t.Fatalf("public feed leaked transmitter field %s: %#v", key, transmitter)
		}
	}
	transmitters, _ := feed["transmitters"].([]any)
	if len(transmitters) != 1 {
		t.Fatalf("public transmitters = %#v", feed["transmitters"])
	}
	for _, key := range []string{"gpclk", "gpio", "rds", "frequency_mhz", "relationship"} {
		if _, ok := transmitters[0].(map[string]any)[key]; ok {
			t.Fatalf("public feed leaked transmitter list field %s: %#v", key, transmitters[0])
		}
	}
}

func TestPublicWebSocketOmitsFeedDetailsUnlessRequested(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/public/v1/panel/ws"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	readType(t, ctx, conn, "hello")
	state := readType(t, ctx, conn, "public_state")
	summary := state["data"].(map[string]any)["summary"].(map[string]any)
	if summary["feed_count"] != float64(1) && summary["feed_count"] != 1 {
		t.Fatalf("summary should retain feed count: %#v", summary)
	}
	if feeds := summary["feeds"].([]any); len(feeds) != 0 {
		t.Fatalf("homepage public socket should not include feed detail payload: %#v", feeds)
	}
}

func TestPublicWebSocketHidesAuthRequiredFeedsWithoutToken(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "auth_required")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/public/v1/panel/ws?feeds=1"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	readType(t, ctx, conn, "hello")
	state := readType(t, ctx, conn, "public_state")
	summary := state["data"].(map[string]any)["summary"].(map[string]any)
	if summary["feeds_access"] != "auth_required" {
		t.Fatalf("summary = %#v", summary)
	}
	if feeds := summary["feeds"].([]any); len(feeds) != 0 {
		t.Fatalf("auth-required feeds leaked without token: %#v", feeds)
	}
}

func TestPublicWebSocketShowsAuthRequiredFeedsWithToken(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	writePublicFixture(t, dir, "auth_required")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/public/v1/panel/ws?feeds=1&token=" + url.QueryEscape(token)
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	readType(t, ctx, conn, "hello")
	state := readType(t, ctx, conn, "public_state")
	summary := state["data"].(map[string]any)["summary"].(map[string]any)
	if summary["feeds_access"] != "auth_required" {
		t.Fatalf("summary = %#v", summary)
	}
	feeds := summary["feeds"].([]any)
	if len(feeds) != 1 {
		t.Fatalf("auth-required feeds with token = %#v", feeds)
	}
	feed := feeds[0].(map[string]any)
	if feed["id"] != "sk-0001" {
		t.Fatalf("feed = %#v", feed)
	}
	if _, ok := feed["clc_codes"]; ok {
		t.Fatalf("public authenticated feed leaked admin-only field: %#v", feed)
	}
}

func TestPublicWebSocketRejectsAdminCommandsEvenWithValidToken(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	writePublicFixture(t, dir, "auth_required")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/public/v1/panel/ws?feeds=1&token=" + url.QueryEscape(token)
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	readType(t, ctx, conn, "hello")
	readType(t, ctx, conn, "public_state")

	writeWS(t, ctx, conn, map[string]any{"type": "command", "request_id": "cmd", "command": "wx.packages"})
	reply := readType(t, ctx, conn, "error")
	if reply["reply_to"] != "cmd" {
		t.Fatalf("reply_to = %v", reply["reply_to"])
	}
	if detail := fmt.Sprint(reply["detail"]); !strings.Contains(detail, "unsupported public message") {
		t.Fatalf("detail = %q", detail)
	}
}

func TestPublicWebSocketRejectsOversizedWebRTCOfferFields(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	tests := []struct {
		name    string
		message map[string]any
		want    string
	}{
		{
			name: "feed id",
			message: map[string]any{
				"type":       "webrtc_offer",
				"request_id": "long-feed",
				"feed_id":    strings.Repeat("x", webRTCOfferMaxFeedIDLength+1),
				"sdp":        "v=0",
			},
			want: "feed_id is too long",
		},
		{
			name: "sdp",
			message: map[string]any{
				"type":       "webrtc_offer",
				"request_id": "long-sdp",
				"feed_id":    "sk-0001",
				"sdp":        strings.Repeat("v", webRTCOfferMaxSDPLength+1),
			},
			want: "sdp is too long",
		},
		{
			name: "invalid feed id",
			message: map[string]any{
				"type":       "webrtc_offer",
				"request_id": "bad-feed",
				"feed_id":    "../config.yaml",
				"sdp":        "v=0",
			},
			want: "feed_id is invalid",
		},
		{
			name: "codec",
			message: map[string]any{
				"type":            "webrtc_offer",
				"request_id":      "long-codec",
				"feed_id":         "sk-0001",
				"sdp":             "v=0",
				"preferred_codec": strings.Repeat("x", webRTCOfferMaxCodecLength+1),
			},
			want: "codec is too long",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/public/v1/panel/ws?feeds=1"
			conn, _, err := websocket.Dial(ctx, wsURL, nil)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.CloseNow()
			readType(t, ctx, conn, "hello")

			writeWS(t, ctx, conn, tc.message)
			reply := readType(t, ctx, conn, "webrtc_error")
			if detail := fmt.Sprint(reply["detail"]); detail != tc.want {
				t.Fatalf("detail = %q, want %q", detail, tc.want)
			}
		})
	}
}

func TestNormalizeWebRTCOfferSDPUsesCRLFAndNestedFallbackShape(t *testing.T) {
	raw := "v=0\nm=audio 9 UDP/TLS/RTP/SAVPF 111\n  \na=rtpmap:111 opus/48000/2  \n"
	normalized := normalizeWebRTCOfferSDP(raw)
	if normalized != "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n" {
		t.Fatalf("normalized SDP = %q", normalized)
	}
	if normalizeWebRTCOfferSDP(" \r\n\t") != "" {
		t.Fatal("blank SDP should normalize to empty")
	}
}

func TestPublicFeedAudioHEADValidatesFeedAndCodec(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	config.Services.Rust.Media.Enabled = true
	mediaBackend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/health" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true,"capabilities":{"http_audio":true}}`))
	}))
	defer mediaBackend.Close()
	config.Services.Rust.Media.Listen = mediaBackend.URL
	server := NewServerWithConfigPath(config, configPath, ".")
	server.media = newMemoryMediaHub()

	okResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(okResponse, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=sk-0001&codec=pcm16", nil))
	if okResponse.Code != http.StatusOK {
		t.Fatalf("valid public audio HEAD status = %d", okResponse.Code)
	}
	if got := okResponse.Header().Get("Content-Type"); !strings.Contains(got, "audio/wav") {
		t.Fatalf("content type = %q", got)
	}
	if got := okResponse.Header().Get("X-Haze-Media-Backend"); got != "haze-media" {
		t.Fatalf("media backend header = %q", got)
	}

	missingFeed := httptest.NewRecorder()
	server.Handler().ServeHTTP(missingFeed, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=missing&codec=pcm16", nil))
	if missingFeed.Code != http.StatusForbidden {
		t.Fatalf("missing feed status = %d", missingFeed.Code)
	}

	badCodec := httptest.NewRecorder()
	server.Handler().ServeHTTP(badCodec, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=sk-0001&codec=not-real", nil))
	if badCodec.Code != http.StatusBadRequest {
		t.Fatalf("bad codec status = %d", badCodec.Code)
	}

	overlongFeed := httptest.NewRecorder()
	server.Handler().ServeHTTP(overlongFeed, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed="+strings.Repeat("x", httpAudioMaxFeedID+1)+"&codec=pcm16", nil))
	if overlongFeed.Code != http.StatusBadRequest {
		t.Fatalf("overlong feed status = %d", overlongFeed.Code)
	}

	invalidFeed := httptest.NewRecorder()
	server.Handler().ServeHTTP(invalidFeed, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=..%2Fconfig.yaml&codec=pcm16", nil))
	if invalidFeed.Code != http.StatusBadRequest {
		t.Fatalf("invalid feed status = %d", invalidFeed.Code)
	}

	overlongCodec := httptest.NewRecorder()
	server.Handler().ServeHTTP(overlongCodec, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=sk-0001&codec="+strings.Repeat("x", httpAudioMaxCodecID+1), nil))
	if overlongCodec.Code != http.StatusBadRequest {
		t.Fatalf("overlong codec status = %d", overlongCodec.Code)
	}

	wrongMethod := httptest.NewRecorder()
	server.Handler().ServeHTTP(wrongMethod, httptest.NewRequest(http.MethodPost, "/api/public/v1/feed/audio?feed=sk-0001&codec=pcm16", nil))
	if wrongMethod.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method status = %d", wrongMethod.Code)
	}
	if allow := wrongMethod.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Fatalf("Allow = %q", allow)
	}
}

func TestPublicFeedAudioRejectsWebRTCOnlyFeed(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	mustWrite(t, filepath.Join(dir, "managed", "configs", "output.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<outputs>
  <feed id="sk-0001"><webrtc enabled="true"/><stream enabled="false"/></feed>
</outputs>
`)
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	server.media = newMemoryMediaHub()

	audio := httptest.NewRecorder()
	server.Handler().ServeHTTP(audio, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=sk-0001&codec=pcm16", nil))
	if audio.Code != http.StatusForbidden {
		t.Fatalf("WebRTC-only audio HEAD status = %d", audio.Code)
	}

	state, err := publicStatePayload(config, configPath, time.Now().UTC(), httptest.NewRequest(http.MethodGet, "/api/public/v1/panel/ws?feeds=1", nil), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	summary := state["summary"].(map[string]any)
	publicFeeds := summary["feeds"].([]map[string]any)
	if len(publicFeeds) != 1 {
		t.Fatalf("public feeds = %#v", publicFeeds)
	}
	if publicFeeds[0]["webrtc_enabled"] != true || publicFeeds[0]["http_stream_enabled"] != false {
		t.Fatalf("WebRTC-only public feed flags = %#v", publicFeeds[0])
	}
}

func TestPublicFeedAudioHEADRejectsUnavailableNativeMediaService(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	config.Services.Rust.Media.Enabled = true
	config.Services.Rust.Media.Listen = "127.0.0.1:1"
	server := NewServerWithConfigPath(config, configPath, ".")
	server.media = newMemoryMediaHub()

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=sk-0001&codec=pcm16", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestPublicFeedAudioGETDoesNotSilentlyFallbackForNativeMediaServiceCodec(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	mediaBackend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "nope", http.StatusServiceUnavailable)
	}))
	defer mediaBackend.Close()
	config.Services.Rust.Media.Enabled = true
	config.Services.Rust.Media.Listen = mediaBackend.URL
	server := NewServerWithConfigPath(config, configPath, ".")
	server.media = newMemoryMediaHub()
	server.media.publish(PCMChunk{FeedID: "sk-0001", SampleRate: httpWAVSampleRate, Channels: 1, Data: make([]byte, httpWAVFrameSamples*2)})

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/public/v1/feed/audio?feed=sk-0001&codec=pcm16", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if strings.Contains(response.Body.String(), "RIFF") {
		t.Fatal("native media service failure should not fall back to legacy WAV stream")
	}
}

func TestPublicFeedAudioUsesMediaServiceOnlyForNativeCodecs(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	mediaBackend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.URL.Path != "/api/v1/feed/audio" {
			http.NotFound(writer, request)
			return
		}
		if got := request.URL.Query().Get("codec"); got != "pcm16" {
			http.Error(writer, "unsupported media codec", http.StatusUnsupportedMediaType)
			return
		}
		writer.Header().Set("Content-Type", "audio/wav")
		_, _ = writer.Write([]byte("native-pcm"))
	}))
	defer mediaBackend.Close()
	config.Services.Rust.Media.Enabled = true
	config.Services.Rust.Media.Listen = mediaBackend.URL
	server := NewServerWithConfigPath(config, configPath, ".")
	server.media = newMemoryMediaHub()

	nativeResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(nativeResponse, httptest.NewRequest(http.MethodGet, "/api/public/v1/feed/audio?feed=sk-0001&codec=pcm16", nil))
	if nativeResponse.Code != http.StatusOK {
		t.Fatalf("native codec status = %d", nativeResponse.Code)
	}
	if nativeResponse.Body.String() != "native-pcm" {
		t.Fatalf("native codec body = %q", nativeResponse.Body.String())
	}
	if got := nativeResponse.Header().Get("X-Haze-Media-Backend"); got != "haze-media" {
		t.Fatalf("native codec backend = %q", got)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("native media requests = %d, want 1", got)
	}

	t.Setenv("FFMPEG", filepath.Join(t.TempDir(), "missing-ffmpeg"))
	flacResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(flacResponse, httptest.NewRequest(http.MethodGet, "/api/public/v1/feed/audio?feed=sk-0001&codec=flac", nil))
	if flacResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("FLAC fallback status = %d, want %d", flacResponse.Code, http.StatusServiceUnavailable)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("unsupported native media requests = %d, want 1", got)
	}
}

func TestPublicFeedAudioValidationPrecedesMediaAvailability(t *testing.T) {
	dir := t.TempDir()
	writePublicFixture(t, dir, "public")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	server.media = nil

	for _, item := range []struct {
		name string
		path string
	}{
		{name: "invalid feed", path: "/api/public/v1/feed/audio?feed=..%2Fconfig.yaml&codec=pcm16"},
		{name: "bad codec", path: "/api/public/v1/feed/audio?feed=sk-0001&codec=not-real"},
	} {
		t.Run(item.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodHead, item.path, nil))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestDrainHTTPAudioUpdatesHandlesBufferedAndClosedChannels(t *testing.T) {
	pcm := make([]byte, httpWAVFrameSamples*2)
	updates := make(chan PCMChunk, 2)
	updates <- PCMChunk{FeedID: "sk-0001", SampleRate: httpWAVSampleRate, Channels: 1, Data: pcm}
	updates <- PCMChunk{FeedID: "sk-0001", SampleRate: httpWAVSampleRate, Channels: 1, Data: pcm}

	queue, ok := drainHTTPAudioUpdates(updates, nil)
	if !ok {
		t.Fatal("open update channel should drain successfully")
	}
	if got := len(queue); got != httpWAVFrameSamples*2 {
		t.Fatalf("queue samples = %d, want %d", got, httpWAVFrameSamples*2)
	}

	close(updates)
	queue, ok = drainHTTPAudioUpdates(updates, queue)
	if ok {
		t.Fatal("closed update channel should return ok=false")
	}
	if got := len(queue); got != httpWAVFrameSamples*2 {
		t.Fatalf("closed drain should preserve queued samples, got %d", got)
	}
}

func TestValidPublicAudioFeedID(t *testing.T) {
	tests := []struct {
		feed string
		want bool
	}{
		{feed: "sk-0001", want: true},
		{feed: "CAP-IT-ALL", want: true},
		{feed: "wx.feed:main_1", want: true},
		{feed: "", want: false},
		{feed: "../config.yaml", want: false},
		{feed: "feed with spaces", want: false},
		{feed: "feed\nid", want: false},
	}
	for _, test := range tests {
		if got := validPublicAudioFeedID(test.feed); got != test.want {
			t.Fatalf("validPublicAudioFeedID(%q) = %v, want %v", test.feed, got, test.want)
		}
	}
}

func TestPublicFeedAudioRequiresAuthWhenConfigured(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	writePublicFixture(t, dir, "auth_required")
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	server.media = newMemoryMediaHub()

	unauthorized := httptest.NewRecorder()
	server.Handler().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=sk-0001&codec=pcm16", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}

	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	authorized := httptest.NewRecorder()
	server.Handler().ServeHTTP(authorized, httptest.NewRequest(http.MethodHead, "/api/public/v1/feed/audio?feed=sk-0001&codec=pcm16&token="+url.QueryEscape(token), nil))
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized status = %d", authorized.Code)
	}
}

func TestWebSocketCatalogCommands(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	writePanelFixture(t, dir)

	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	ctx := context.Background()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/api/v1/panel/ws?mode=control&token=" + token
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	readType(t, ctx, conn, "hello")

	writeWS(t, ctx, conn, map[string]any{"type": "command", "request_id": "pkg", "command": "wx.packages"})
	packagesReply := readType(t, ctx, conn, "command_result")
	packages := packagesReply["result"].(map[string]any)["packages"].([]any)
	if len(packages) != 1 || packages[0] != "forecast" {
		t.Fatalf("packages = %#v", packages)
	}

	writeWS(t, ctx, conn, map[string]any{"type": "command", "request_id": "readers", "command": "wx.readers"})
	readersReply := readType(t, ctx, conn, "command_result")
	readers := readersReply["result"].(map[string]any)["readers"].([]any)
	if len(readers) != 1 || readers[0].(map[string]any)["id"] != "00" {
		t.Fatalf("readers = %#v", readers)
	}

	writeWS(t, ctx, conn, map[string]any{"type": "command", "request_id": "codes", "command": "same.event_codes"})
	codesReply := readType(t, ctx, conn, "command_result")
	eas := codesReply["result"].(map[string]any)["eas"].(map[string]any)
	if eas["RWT"] != "Required Weekly Test" {
		t.Fatalf("eas = %#v", eas)
	}

	writeWS(t, ctx, conn, map[string]any{"type": "command", "request_id": "locs", "command": "same.location_names"})
	locationsReply := readType(t, ctx, conn, "command_result")
	locations := locationsReply["result"].(map[string]any)
	if locations["065522"] != "R.M. of Rudy including Outlook and Glenside" {
		t.Fatalf("locations = %#v", locations)
	}

	writeWS(t, ctx, conn, map[string]any{"type": "command", "request_id": "tpl", "command": "same.templates.get"})
	templatesReply := readType(t, ctx, conn, "command_result")
	templates := templatesReply["result"].(map[string]any)
	rwt, ok := templates["RWT"].(map[string]any)
	if !ok {
		t.Fatalf("templates = %#v", templates)
	}
	same := rwt["same"].(map[string]any)
	templateLocations := same["locations"].([]any)
	if len(templateLocations) != 1 || templateLocations[0].(map[string]any)["id"] != "065522" {
		t.Fatalf("template locations = %#v", same["locations"])
	}
}

func TestWxGeneratePayloadResolvesLocationHints(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)
	configPath := filepath.Join(dir, "config.yaml")

	request, err := parseWxGeneratePayload(configPath, map[string]any{
		"feed_id":   "sk-0001",
		"locations": "sk-40",
		"packages":  []any{"forecast"},
		"format":    "json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.ForecastID != "sk-40" || request.StationID != "sk-40" {
		t.Fatalf("request = %#v", request)
	}

	derived, err := parseWxGeneratePayload(configPath, map[string]any{
		"feed_id":   "sk-0001",
		"locations": "640",
		"packages":  []any{"forecast"},
		"format":    "json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if derived.Code != "06040" || derived.ForecastID != "sk-40" || derived.StationID != "sk-40" || derived.Province != "SK" {
		t.Fatalf("derived = %#v", derived)
	}
}

func TestReceiverSessionAndWebSocketUseFeedID(t *testing.T) {
	dir := t.TempDir()
	writeReceiverFixture(t, dir)
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	sessionStatus, session := postReceiverJSON(t, httpServer.URL+"/api/receiver/v1/session", map[string]any{
		"feed_id":           "sk-0001",
		"receiver_id":       "rx-1",
		"receiver_hostname": "pi-one",
	})
	if sessionStatus != http.StatusOK {
		t.Fatalf("session status = %d payload=%#v", sessionStatus, session)
	}
	wsURL := stringMapValue(session, "ws_url")
	if wsURL == "" || !strings.Contains(wsURL, "feed_id=sk-0001") {
		t.Fatalf("session = %#v", session)
	}

	conn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("receiver ws dial: %v", err)
	}
	ready := readType(t, context.Background(), conn, "receiver_ready")
	if ready["feed_id"] != "sk-0001" {
		t.Fatalf("ready = %#v", ready)
	}
	transmitter := ready["transmitter"].(map[string]any)
	if transmitter["site_name"] != "Saskatoon" || transmitter["callsign"] != "XLF322" {
		t.Fatalf("transmitter = %#v", transmitter)
	}
	active := server.receiver.ActiveSnapshots()
	if len(active) != 1 {
		t.Fatalf("active receivers = %#v", active)
	}
	if active[0]["feed_id"] != "sk-0001" || active[0]["transport"] != "control" {
		t.Fatalf("active receiver snapshot = %#v", active[0])
	}
	conn.CloseNow()
}

func TestReceiverStatusSnapshotIsSanitizedAndShownInAdminHealth(t *testing.T) {
	t.Setenv("ADMIN_PASSWD", "secret")
	dir := t.TempDir()
	writeReceiverFixture(t, dir)
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	sessionStatus, session := postReceiverJSON(t, httpServer.URL+"/api/receiver/v1/session", map[string]any{
		"feed_id":           "sk-0001",
		"receiver_id":       "rx-1",
		"receiver_hostname": "pi-one",
	})
	if sessionStatus != http.StatusOK {
		t.Fatalf("session status = %d payload=%#v", sessionStatus, session)
	}
	conn, _, err := websocket.Dial(context.Background(), stringMapValue(session, "ws_url"), nil)
	if err != nil {
		t.Fatalf("receiver ws dial: %v", err)
	}
	defer func() {
		_ = conn.CloseNow()
	}()
	_ = readType(t, context.Background(), conn, "receiver_ready")

	writeWS(t, context.Background(), conn, map[string]any{
		"type":      "receiver_status",
		"transport": "webrtc",
		"status": map[string]any{
			"state":                       "ok",
			"transport":                   "webrtc",
			"reason_code":                 "pifm_output_stalled",
			"audio_format":                "pcm_s16le",
			"input_audio_seen":            true,
			"ffmpeg_output_seen":          true,
			"pifm_output_seen":            true,
			"ffmpeg_running":              true,
			"pifm_running":                true,
			"webrtc_connection_state":     "connected",
			"webrtc_ice_state":            "completed",
			"session_uptime_ms":           float64(receiverStatusMaxMillis + 1),
			"input_audio_idle_ms":         "42",
			"ffmpeg_output_idle_ms":       float64(19),
			"pifm_output_idle_ms":         float64(23),
			"ffmpeg_stdin_drain_timeouts": float64(1),
			"pifm_stdin_slow_drains":      float64(2),
			"max_pifm_stdin_drain_ms":     float64(144),
			"credential_secret":           strings.Repeat("s", 200),
			"reason":                      "https://example.invalid/audio?token=secret",
			"url":                         "https://example.invalid/audio?token=secret",
			"unsafe_text":                 "leak-me",
			"webrtc_state":                "connected;secret",
		},
	})

	snapshot := waitForReceiverSnapshot(t, server.receiver, func(snapshot map[string]any) bool {
		return snapshot["last_message_type"] == "receiver_status"
	})
	if snapshot["transport"] != "webrtc" {
		t.Fatalf("receiver transport = %#v", snapshot)
	}
	status, ok := snapshot["status"].(map[string]any)
	if !ok {
		t.Fatalf("missing status snapshot: %#v", snapshot)
	}
	if status["state"] != "ok" || status["transport"] != "webrtc" || status["reason_code"] != "pifm_output_stalled" || status["audio_format"] != "pcm_s16le" {
		t.Fatalf("safe status fields = %#v", status)
	}
	if status["input_audio_seen"] != true || status["ffmpeg_output_seen"] != true || status["pifm_output_seen"] != true {
		t.Fatalf("pipeline seen fields = %#v", status)
	}
	if status["webrtc_connection_state"] != "connected" || status["webrtc_ice_state"] != "completed" {
		t.Fatalf("webrtc status fields = %#v", status)
	}
	if status["session_uptime_ms"] != receiverStatusMaxMillis ||
		status["input_audio_idle_ms"] != int64(42) ||
		status["ffmpeg_output_idle_ms"] != int64(19) ||
		status["pifm_output_idle_ms"] != int64(23) ||
		status["ffmpeg_stdin_drain_timeouts"] != int64(1) ||
		status["pifm_stdin_slow_drains"] != int64(2) ||
		status["max_pifm_stdin_drain_ms"] != int64(144) {
		t.Fatalf("bounded status fields = %#v", status)
	}
	for _, key := range []string{"credential_secret", "reason", "url", "unsafe_text", "webrtc_state"} {
		if _, ok := status[key]; ok {
			t.Fatalf("unsafe status field %s leaked: %#v", key, status)
		}
	}

	token, err := server.auth.Login("secret")
	if err != nil {
		t.Fatal(err)
	}
	adminHealth := httptest.NewRecorder()
	adminRequest := httptest.NewRequest(http.MethodGet, "/api/v1/health?token="+url.QueryEscape(token), nil)
	server.Handler().ServeHTTP(adminHealth, adminRequest)
	if adminHealth.Code != http.StatusOK {
		t.Fatalf("admin health status = %d", adminHealth.Code)
	}
	var adminPayload map[string]any
	if err := json.Unmarshal(adminHealth.Body.Bytes(), &adminPayload); err != nil {
		t.Fatalf("admin health json: %v", err)
	}
	connections, ok := adminPayload["receiver_connections"].([]any)
	if !ok || len(connections) != 1 {
		t.Fatalf("receiver_connections = %#v", adminPayload["receiver_connections"])
	}
	adminConnection, ok := connections[0].(map[string]any)
	if !ok {
		t.Fatalf("receiver connection = %#v", connections[0])
	}
	adminStatus, ok := adminConnection["status"].(map[string]any)
	if !ok {
		t.Fatalf("admin receiver status = %#v", adminConnection)
	}
	if adminStatus["state"] != "ok" || adminStatus["session_uptime_ms"] != float64(receiverStatusMaxMillis) {
		t.Fatalf("admin receiver status fields = %#v", adminStatus)
	}
	if _, ok := adminStatus["credential_secret"]; ok {
		t.Fatalf("admin receiver status leaked credential_secret: %#v", adminStatus)
	}

	publicHealth := httptest.NewRecorder()
	server.Handler().ServeHTTP(publicHealth, httptest.NewRequest(http.MethodGet, "/api/public/v1/health", nil))
	if publicHealth.Code != http.StatusOK {
		t.Fatalf("public health status = %d", publicHealth.Code)
	}
	var publicPayload map[string]any
	if err := json.Unmarshal(publicHealth.Body.Bytes(), &publicPayload); err != nil {
		t.Fatalf("public health json: %v", err)
	}
	if _, ok := publicPayload["receiver_connections"]; ok {
		t.Fatalf("public health leaked receiver_connections: %#v", publicPayload)
	}
}

func TestReceiverRejectsUnknownFeed(t *testing.T) {
	dir := t.TempDir()
	writeReceiverFixture(t, dir)
	configPath := filepath.Join(dir, "config.yaml")
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServerWithConfigPath(config, configPath, ".")
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()

	status, _ := postReceiverJSON(t, httpServer.URL+"/api/receiver/v1/session", map[string]any{
		"feed_id":           "sk-9999",
		"receiver_id":       "rx-1",
		"receiver_hostname": "pi-one",
	})
	if status == http.StatusOK {
		t.Fatal("unknown receiver feed was accepted")
	}
}

func TestReceiverRoutesDisabledByDefault(t *testing.T) {
	server := NewServer(Config{}, ".")
	request := httptest.NewRequest(http.MethodPost, "/api/receiver/v1/session", strings.NewReader(`{}`))
	response := httptest.NewRecorder()

	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestBuildSameRequestFromPanelPayload(t *testing.T) {
	t.Setenv("SAME_ID", "WXR123")
	dir := t.TempDir()
	writePanelFixture(t, dir)

	request, err := buildSameRequest(filepath.Join(dir, "config.yaml"), map[string]any{
		"originator":       "wxr",
		"event":            "rwt",
		"locations":        []any{"065522", "bad", "065500"},
		"duration_hours":   float64(1),
		"duration_minutes": float64(5),
		"tone_type":        "npas",
		"feed_id":          "sk-0001",
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.Originator != "WXR" || request.Event != "RWT" {
		t.Fatalf("codes = %#v", request)
	}
	if request.Duration != "0105" {
		t.Fatalf("duration = %q", request.Duration)
	}
	if strings.Join(request.Locations, ",") != "065500,065522" {
		t.Fatalf("locations = %#v", request.Locations)
	}
	if request.Callsign != "WXR123" {
		t.Fatalf("callsign = %q", request.Callsign)
	}
	if request.Tone != "NPAS" {
		t.Fatalf("tone = %q", request.Tone)
	}
}

func TestBuildSameRequestExpandsForecastRegionSubregions(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="sk-0001" enabled="true" timezone="America/Regina">
    <languages><lang code="en-CA"/></languages>
    <locations>
      <coverage>
        <region id="065400"/>
      </coverage>
    </locations>
    <transmitter_metadata><transmitter><site_name>Saskatoon</site_name><callsign>XLF322</callsign></transmitter></transmitter_metadata>
  </feed>
</feeds>
`)
	mustWrite(t, filepath.Join(dir, "managed", "csv", "CLC_Base_Zone.csv"), "CLC,FEATURE_ID,NAME,NOM\n065413,fixture,R.M. of Great Bend including Radisson and Borden,\n065421,fixture,R.M. of Laird including Waldheim Hepburn and Laird,\n065432,fixture,R.M. of Biggar including Biggar,\n")

	request, err := buildSameRequest(filepath.Join(dir, "config.yaml"), map[string]any{
		"originator": "WXR",
		"event":      "RWT",
		"feed_id":    "sk-0001",
		"locations":  []any{"065400"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(request.Locations, ",") != "065400,065413,065421,065432" {
		t.Fatalf("locations = %#v", request.Locations)
	}
}

func TestBuildSameRequestRequiresLocation(t *testing.T) {
	_, err := buildSameRequest("config.yaml", map[string]any{
		"originator": "WXR",
		"event":      "RWT",
		"locations":  []any{"bad"},
	})

	if err == nil {
		t.Fatal("expected missing location error")
	}
}

func TestSameIntroUsesSAMEToTextManualLead(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)
	result, err := sameIntroPayload(filepath.Join(dir, "config.yaml"), map[string]any{
		"originator":       "WXR",
		"event":            "RWT",
		"locations":        []any{"065522"},
		"duration_hours":   float64(0),
		"duration_minutes": float64(15),
		"callsign":         "XLF322",
	})
	if err != nil {
		t.Fatal(err)
	}
	intro, _ := result["intro"].(string)
	if !strings.Contains(intro, "Environment Canada has issued") || !strings.Contains(intro, "XLF322") {
		t.Fatalf("intro = %q", intro)
	}
}

func TestBroadcastAlertDataCanDisableSame(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)
	session := wsSession{configPath: filepath.Join(dir, "config.yaml")}
	data := session.broadcastAlertData(map[string]any{
		"originator":               "WXR",
		"event":                    "RWT",
		"locations":                []any{"065522"},
		"include_same":             false,
		"prepend_same_translation": true,
		"voice_message":            "This is only a drill.",
		"duration_hours":           float64(0),
		"duration_minutes":         float64(15),
	}, []string{"sk-0001"}, "manual-test", false)

	if data["include_same"] != false {
		t.Fatalf("include_same = %#v", data["include_same"])
	}
	text, _ := data["alert_text"].(string)
	if !strings.Contains(text, "This is only a drill.") || !strings.Contains(text, "has issued") {
		t.Fatalf("alert_text = %q", text)
	}
}

func TestBroadcastAlertDataDoesNotPrependSameTranslationWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)
	session := wsSession{configPath: filepath.Join(dir, "config.yaml")}
	data := session.broadcastAlertData(map[string]any{
		"originator":               "WXR",
		"event":                    "RWT",
		"locations":                []any{"065522"},
		"include_same":             false,
		"prepend_same_translation": false,
		"voice_message":            "This is only a drill.",
		"duration_hours":           float64(0),
		"duration_minutes":         float64(15),
	}, []string{"sk-0001"}, "manual-test", false)

	text, _ := data["alert_text"].(string)
	if text != "This is only a drill." {
		t.Fatalf("alert_text = %q", text)
	}
	bannerText, _ := data["banner_text"].(string)
	if !strings.Contains(bannerText, "Environment Canada has issued") || !strings.Contains(bannerText, "This is only a drill.") {
		t.Fatalf("banner_text = %q", bannerText)
	}
}

func TestBroadcastAlertDataDoesNotUseSameIntroFallbackWhenSameEnabled(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)
	session := wsSession{configPath: filepath.Join(dir, "config.yaml")}
	data := session.broadcastAlertData(map[string]any{
		"originator":               "WXR",
		"event":                    "RWT",
		"locations":                []any{"065522"},
		"include_same":             true,
		"prepend_same_translation": false,
		"description":              "This is only a drill.",
		"instruction":              "No action is required.",
		"duration_hours":           float64(0),
		"duration_minutes":         float64(15),
	}, []string{"sk-0001"}, "manual-test", true)

	text, _ := data["alert_text"].(string)
	if strings.Contains(text, "has issued") {
		t.Fatalf("alert_text should not include SAME intro when prepend is disabled: %q", text)
	}
	for _, wanted := range []string{"This is only a drill.", "No action is required."} {
		if !strings.Contains(text, wanted) {
			t.Fatalf("alert_text missing %q in %q", wanted, text)
		}
	}
	bannerText, _ := data["banner_text"].(string)
	if !strings.Contains(bannerText, "Environment Canada has issued") {
		t.Fatalf("banner_text should still include SAME intro: %q", bannerText)
	}
}

func TestBroadcastAlertDataCarriesAudioSourceFields(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)
	session := wsSession{configPath: filepath.Join(dir, "config.yaml")}
	data := session.broadcastAlertData(map[string]any{
		"originator":        "WXR",
		"event":             "RWT",
		"locations":         []any{"065522"},
		"include_same":      true,
		"voice_message":     "This is only a drill.",
		"duration_hours":    float64(0),
		"duration_minutes":  float64(15),
		"audio_mode":        "file",
		"audio_path":        "runtime/audio/alerts/uploads/manual.pcm16le",
		"audio_format":      "pcm_s16le",
		"audio_sample_rate": float64(48000),
		"audio_channels":    float64(1),
		"reader_id":         "02",
	}, []string{"sk-0001"}, "manual-test", true)

	if data["audio_mode"] != "file" {
		t.Fatalf("audio_mode = %#v", data["audio_mode"])
	}
	if data["audio_path"] != "runtime/audio/alerts/uploads/manual.pcm16le" {
		t.Fatalf("audio_path = %#v", data["audio_path"])
	}
	if data["audio_format"] != "pcm_s16le" || data["audio_sample_rate"] != 48000 || data["audio_channels"] != 1 {
		t.Fatalf("audio format fields = %#v %#v %#v", data["audio_format"], data["audio_sample_rate"], data["audio_channels"])
	}
	if data["reader_id"] != "02" || data["tts_reader_id"] != "02" {
		t.Fatalf("reader fields = %#v %#v", data["reader_id"], data["tts_reader_id"])
	}
}

func TestPersistSameQueueItemCreatesManifestAndStateDepth(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)
	configPath := filepath.Join(dir, "config.yaml")

	item, err := persistSameQueueItem(configPath, sameGenerateRequest{
		Originator:     "WXR",
		OriginatorName: "Emergency Management",
		Event:          "RWT",
		Locations:      []string{"065522"},
		Duration:       "0015",
		Callsign:       "XLF322",
		Tone:           "WXR",
	}, []string{"sk-0001"}, map[string]any{
		"header":       "ZCZC-WXR-RWT-065522+0015-1661200-XLF322  -",
		"format":       "raw",
		"sample_rate":  float64(48000),
		"channels":     float64(1),
		"audio_base64": "AQIDBA==",
	}, "A Broadcast Station or Cable System has issued this drill.")
	if err != nil {
		t.Fatal(err)
	}
	if item.Status != "pending" || item.AudioBytes != 4 {
		t.Fatalf("item = %#v", item)
	}
	if item.BannerText != "Emergency Management has issued this drill." {
		t.Fatalf("banner text = %q", item.BannerText)
	}
	if len(item.Outputs) != 1 || item.Outputs[0].Type != "udp" || item.Outputs[0].Address != "127.0.0.1:8898" {
		t.Fatalf("outputs = %#v", item.Outputs)
	}
	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(item.AudioPath))); err != nil {
		t.Fatalf("audio missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(item.ManifestPath))); err != nil {
		t.Fatalf("manifest missing: %v", err)
	}

	feeds, err := loadFeedSummaries(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if feeds[0]["alert_queue_depth"].(int) != 1 {
		t.Fatalf("queue depth = %v", feeds[0]["alert_queue_depth"])
	}
	runtime := feeds[0]["runtime"].(map[string]any)
	if runtime["last_alert_event"] != "RWT" || runtime["last_alert_severity"] != "Pending" {
		t.Fatalf("runtime = %#v", runtime)
	}
}

func TestAlertQueueStateCountsOnlyActiveItems(t *testing.T) {
	now := time.Now().UTC()
	depth, recent, latest := alertQueueState([]sameQueueItem{
		{ID: "played", Status: "played", FeedIDs: []string{"sk-0001"}, Event: "RWT", CreatedAt: now.Add(-time.Minute)},
		{ID: "failed", Status: "failed", FeedIDs: []string{"sk-0001"}, Event: "RWT", LastError: "unsupported", CreatedAt: now},
		{ID: "pending", Status: "pending", FeedIDs: []string{"sk-0001"}, Event: "RWT", CreatedAt: now.Add(-2 * time.Minute)},
	}, "sk-0001")

	if depth != 1 {
		t.Fatalf("depth = %d", depth)
	}
	if latest == nil || latest.ID != "failed" {
		t.Fatalf("latest = %#v", latest)
	}
	if len(recent) != 3 || recent[0]["last_error"] != "unsupported" {
		t.Fatalf("recent = %#v", recent)
	}
}

func TestTargetFeedIDsRejectsUnknownFeed(t *testing.T) {
	dir := t.TempDir()
	writePanelFixture(t, dir)

	_, err := targetFeedIDs(filepath.Join(dir, "config.yaml"), map[string]any{
		"feed_ids": []any{"missing"},
	})

	if err == nil {
		t.Fatal("expected unknown feed error")
	}
}

func TestAlertTargetFeedIDsIncludesAllLocationCatchalls(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "config.yaml"), `version: test
feeds_file: managed/configs/feeds.xml
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="sk-0001" enabled="true">
    <locations>
      <coverage><region id="065500"><subregion id="065522"/></region></coverage>
    </locations>
  </feed>
  <feed id="CAP-IT-ALL" enabled="true">
    <alerts>
      <cap_cp enabled="true"/>
      <nws_cap enabled="true"/>
    </alerts>
    <locations><coverage/></locations>
  </feed>
</feeds>
`)

	targets, err := targetFeedIDs(filepath.Join(dir, "config.yaml"), map[string]any{
		"feed_ids": []any{"sk-0001"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(targets, ","); got != "sk-0001" {
		t.Fatalf("targetFeedIDs = %q", got)
	}

	targets, err = alertTargetFeedIDs(filepath.Join(dir, "config.yaml"), map[string]any{
		"feed_ids": []any{"sk-0001"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(targets, ","); got != "sk-0001,CAP-IT-ALL" {
		t.Fatalf("alertTargetFeedIDs = %q", got)
	}
}

func TestSameGeneratorIntegration(t *testing.T) {
	generator := os.Getenv("HAZE_SAME_GENERATOR")
	if generator == "" {
		t.Skip("HAZE_SAME_GENERATOR is not set")
	}
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "config.yaml"), "version: test\n")

	result, err := runSameGenerator(filepath.Join(dir, "config.yaml"), sameGenerateRequest{
		Originator: "WXR",
		Event:      "RWT",
		Locations:  []string{"065522"},
		Duration:   "0015",
		Callsign:   "XLF322",
		Tone:       "WXR",
	})
	if err != nil {
		t.Fatal(err)
	}
	header, _ := result["header"].(string)
	if !strings.HasPrefix(header, "ZCZC-WXR-RWT-065522+0015-") || !strings.HasSuffix(header, "-XLF322  -") {
		t.Fatalf("header = %v", header)
	}
	if result["format"] != "raw" || result["sample_rate"].(float64) != 48000 {
		t.Fatalf("result = %#v", result)
	}
	if len(result["audio_base64"].(string)) < 1000 {
		t.Fatalf("audio payload too small: %d", len(result["audio_base64"].(string)))
	}
}

func TestAirSameIntegrationQueuesGeneratorOutput(t *testing.T) {
	generator := os.Getenv("HAZE_SAME_GENERATOR")
	if generator == "" {
		t.Skip("HAZE_SAME_GENERATOR is not set")
	}
	dir := t.TempDir()
	writePanelFixture(t, dir)
	session := wsSession{configPath: filepath.Join(dir, "config.yaml")}

	result, err := session.airSame(map[string]any{
		"originator":       "WXR",
		"event":            "RWT",
		"locations":        []any{"065522"},
		"duration_hours":   float64(0),
		"duration_minutes": float64(15),
		"tone_type":        "WXR",
		"feed_id":          "sk-0001",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result["queued"] != true {
		t.Fatalf("result = %#v", result)
	}
	if result["feed_id"] != "sk-0001" {
		t.Fatalf("feed_id = %v", result["feed_id"])
	}
	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(result["manifest_path"].(string)))); err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(result["audio_path"].(string)))); err != nil {
		t.Fatalf("audio missing: %v", err)
	}
	feeds, err := loadFeedSummaries(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if feeds[0]["alert_queue_depth"].(int) != 1 {
		t.Fatalf("queue depth = %v", feeds[0]["alert_queue_depth"])
	}
}

func authEnabledConfig() Config {
	var config Config
	enabled := true
	config.Webpanel.Authentication.Enabled = &enabled
	config.Webpanel.Authentication.SessionTTLSeconds = 60
	return config
}

func readType(t *testing.T, ctx context.Context, conn *websocket.Conn, wanted string) map[string]any {
	t.Helper()
	for range 5 {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatalf("json: %v", err)
		}
		if payload["type"] == wanted {
			return payload
		}
	}
	t.Fatalf("did not receive %s", wanted)
	return nil
}

func writeWS(t *testing.T, ctx context.Context, conn *websocket.Conn, payload map[string]any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, raw); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func waitForReceiverSnapshot(t *testing.T, manager *ReceiverManager, predicate func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, snapshot := range manager.ActiveSnapshots() {
			if predicate(snapshot) {
				return snapshot
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("receiver snapshot did not match before timeout: %#v", manager.ActiveSnapshots())
	return nil
}

func writePanelFixture(t *testing.T, dir string) {
	t.Helper()
	mustWrite(t, filepath.Join(dir, "config.yaml"), `version: test
feeds_file: managed/configs/feeds.xml
outputs_file: managed/configs/output.xml
logging:
  file:
    main_path: logs/haze.log
webpanel:
  authentication:
    enabled: true
    session_ttl_seconds: 60
services:
  go:
    tts:
      readers: managed/configs/readers.xml
operator:
  operator_name:
    - text: "@operator"
  on_air_name:
    - text: "Canada RadioMET"
same: {}
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="sk-0001" enabled="true" timezone="America/Regina">
    <languages><lang code="en-CA"/></languages>
    <locations>
      <coverage>
        <region id="065500"><subregion id="065522"/></region>
      </coverage>
      <observationLocations><location id="sk-40"/></observationLocations>
    </locations>
    <transmitter_metadata>
      <transmitter>
        <site_name>Saskatoon</site_name>
        <callsign>XLF322</callsign>
        <frequency_mhz>162.550</frequency_mhz>
      </transmitter>
    </transmitter_metadata>
  </feed>
</feeds>
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "output.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<outputs>
  <feed id="sk-0001">
    <udp enabled="true">
      <ip>127.0.0.1</ip>
      <port>8898</port>
      <format>raw</format>
      <acodec>pcm_s16le</acodec>
    </udp>
  </feed>
</outputs>
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "packages.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<Packages>
  <defaults><enabled>true</enabled></defaults>
  <package id="forecast" enabled="true"/>
  <package id="disabled_item" enabled="false"/>
</Packages>
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "readers.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<Readers>
  <reader id="00" provider="piper">
    <gender>male</gender>
    <language>en-CA</language>
    <voice_id>en_CA-test-medium</voice_id>
  </reader>
</Readers>
`)
	mustWrite(t, filepath.Join(dir, "managed", "sameMapping.json"), `{"eas":{"RWT":"Required Weekly Test"},"naadsToEas":{"test":"RWT"}}`)
	mustWrite(t, filepath.Join(dir, "managed", "csv", "CAP-CP_Geocodes.csv"), "NAME,NOM,CAPCPGCODE\nSaskatoon,,065522\n")
	mustWrite(t, filepath.Join(dir, "managed", "csv", "CLC_Base_Zone.csv"), "CLC,FEATURE_ID,NAME,NOM\n065522,fixture,R.M. of Rudy including Outlook and Glenside,\n")
	mustWrite(t, filepath.Join(dir, "managed", "csv", "FORECAST_LOCATIONS.csv"), "CODE,NAME,NOM,PROGRAMS\n065500,Outlook - Watrous - Hanley - Imperial - Dinsmore,,Public\n")
	mustWrite(t, filepath.Join(dir, "managed", "configs", "alertTemplates.xml"), `<?xml version="1.0" encoding="utf-8"?>
<templates>
  <template>
    <name>Weekly Test</name>
    <description>Routine test</description>
    <automated><enabled>true</enabled></automated>
    <same><enabled>true</enabled><event>RWT</event><locations><location id="065522" source="eccc"/></locations><duration hr="0" min="15"/><sender_id/></same>
    <content attention_tone="WXR"><lang code="en"><text>Test text</text><file/></lang></content>
  </template>
</templates>
`)
	mustWrite(t, filepath.Join(dir, "logs", "haze.log"), "line one\nline two\n")
}

func writeReceiverFixture(t *testing.T, dir string) {
	t.Helper()
	mustWrite(t, filepath.Join(dir, "config.yaml"), `version: test
feeds_file: managed/configs/feeds.xml
playout:
  sample_rate: 48000
  channels: 1
webpanel:
  receiver:
    enabled: true
    base_path: /api/receiver/v1
    require_tls: false
    challenge_ttl_seconds: 60
    cookie_ttl_seconds: 30
    credential_ttl_seconds: 3600
    credentials_path: runtime/state/receiver_credentials.json
    transmitter_defaults:
      bandwidth_khz: 12.5
      deviation_hz: 5000
      preemphasis: none
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="sk-0001" enabled="true" timezone="America/Regina">
    <transmitter_metadata>
      <transmitter>
        <site_name>Saskatoon</site_name>
        <callsign>XLF322</callsign>
        <frequency_mhz>162.550</frequency_mhz>
      </transmitter>
    </transmitter_metadata>
  </feed>
</feeds>
`)
}

func writePublicFixture(t *testing.T, dir string, access string) {
	t.Helper()
	mustWrite(t, filepath.Join(dir, "config.yaml"), fmt.Sprintf(`version: test
feeds_file: managed/configs/feeds.xml
outputs_file: managed/configs/output.xml
webpanel:
  public:
    site_name: Test Haze
    feeds:
      access: %s
      webrtc:
        enabled: true
  admin:
    host: 127.0.0.1
    port: 6444
  authentication:
    enabled: true
    session_ttl_seconds: 60
operator:
  operator_name:
    - text: "@operator"
  on_air_name:
    - text: "Canada RadioMET"
`, access))
	mustWrite(t, filepath.Join(dir, "managed", "configs", "feeds.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<feeds>
  <feed id="sk-0001" enabled="true" timezone="America/Regina">
    <languages><lang code="en-CA"/></languages>
    <locations>
      <coverage>
        <region id="065500"><subregion id="065522"/></region>
      </coverage>
      <observationLocations><location id="sk-40"/></observationLocations>
    </locations>
    <transmitter_metadata>
      <transmitter>
        <site_name>Saskatoon</site_name>
        <callsign>XLF322</callsign>
        <frequency_mhz>162.550</frequency_mhz>
      </transmitter>
    </transmitter_metadata>
  </feed>
</feeds>
`)
	mustWrite(t, filepath.Join(dir, "managed", "configs", "output.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<outputs>
  <feed id="sk-0001"><webrtc enabled="true"/><stream enabled="true"/></feed>
</outputs>
`)
}

func receiverChallengeForTest(t *testing.T, baseURL string, feedID string, receiverID string, hostname string, nonce string) map[string]any {
	t.Helper()
	status, payload := postReceiverJSON(t, baseURL+"/api/receiver/v1/pair/challenge", map[string]any{
		"feed_id":           feedID,
		"receiver_id":       receiverID,
		"receiver_hostname": hostname,
		"nonce":             nonce,
	})
	if status != http.StatusOK {
		t.Fatalf("challenge status = %d payload=%#v", status, payload)
	}
	return payload
}

func postReceiverJSON(t *testing.T, url string, payload map[string]any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(url, "application/json", strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil && response.Body != nil {
		decoded = map[string]any{"decode_error": err.Error()}
	}
	return response.StatusCode, decoded
}

func stringMapValue(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

func mustWrite(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
