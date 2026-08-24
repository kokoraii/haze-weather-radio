package webgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/meowraii/haze-weather-radio/services/go/internal/events"
)

// Server is the first-pass Go HTTP/WebSocket gateway for Haze.
type Server struct {
	startedAt           time.Time
	config              Config
	configPath          string
	webroot             string
	surface             WebSurface
	auth                *AuthManager
	receiver            *ReceiverManager
	media               *MediaHub
	bannerHub           *BannerHub
	breakIn             *OperatorBreakInManager
	listeners           *ListenerTracker
	feedMapResolveSlots chan struct{}
}

// Close releases long-lived authentication and storage resources.
func (s *Server) Close() {
	if s != nil && s.auth != nil {
		s.auth.Close()
	}
}

// WebSurface controls which routes a gateway instance exposes.
type WebSurface string

const (
	SurfaceCombined WebSurface = "combined"
	SurfacePublic   WebSurface = "public"
	SurfaceAdmin    WebSurface = "admin"
)

const maxWebSocketMessageBytes = 1 << 20

// NewServer creates a web gateway server.
func NewServer(config Config, webroot string) *Server {
	return NewServerWithConfigPath(config, "config.yaml", webroot)
}

// NewServerWithConfigPath creates a web gateway server with a source config path.
func NewServerWithConfigPath(config Config, configPath string, webroot string) *Server {
	return NewServerWithSurface(config, configPath, webroot, string(SurfaceCombined))
}

// NewServerWithSurface creates a gateway server constrained to one HTTP surface.
func NewServerWithSurface(config Config, configPath string, webroot string, surface string) *Server {
	hostBridgeAddr := os.Getenv("HAZE_HOST_BRIDGE_ADDR")
	mediaBridgeAddr := firstNonBlank(os.Getenv("HAZE_MEDIA_BRIDGE_ADDR"), hostBridgeAddr)
	mediaServiceURL := mediaServiceBaseURL(config)
	mediaHub := NewMediaHub(mediaBridgeAddr)
	mediaHub.SetHTTPSource(mediaServiceURL)
	bannerHub := NewBannerHub(configPath, hostBridgeAddr)
	server := &Server{
		startedAt:           time.Now().UTC(),
		config:              config,
		configPath:          filepath.Clean(configPath),
		webroot:             webroot,
		surface:             normalizeSurface(surface),
		auth:                NewAuthManagerWithPath(config, configPath),
		receiver:            NewReceiverManager(config, configPath, mediaHub),
		media:               mediaHub,
		bannerHub:           bannerHub,
		breakIn:             NewOperatorBreakInManager(),
		listeners:           NewListenerTracker(),
		feedMapResolveSlots: make(chan struct{}, publicFeedMapResolveConcurrency),
	}
	if server.surface.allowsAdmin() {
		server.startBannerStatePublisher(hostBridgeAddr)
	}
	return server
}

// Handler builds the HTTP route tree.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	if s.surface.allowsPublic() {
		mux.HandleFunc("/api/public/v1/health", s.publicHealth)
		mux.HandleFunc("/", s.publicIndex)
		mux.HandleFunc("/feeds", s.publicIndex)
		mux.HandleFunc("/listen", s.publicIndex)
		mux.HandleFunc("/alerts", s.publicIndex)
		mux.HandleFunc("/alerts/archive", s.publicIndex)
		mux.HandleFunc("/api/public/v1/panel/ws", s.websocket)
		mux.HandleFunc("/api/public/v1/panel/events", s.publicPanelEvents)
		mux.HandleFunc("/api/public/v1/panel/state", s.publicPanelState)
		mux.HandleFunc("/api/public/v1/feed/audio", s.publicFeedAudio)
		mux.HandleFunc("/api/public/v1/feed/map", s.publicFeedMap)
		mux.HandleFunc("/api/public/v1/map/basemap", s.publicBasemap)
		mux.HandleFunc("/api/public/v1/feed/webrtc/offer", s.publicFeedWebRTCOffer)
		mux.HandleFunc("/api/public/v1/alerts/archive/cap.xml", s.publicAlertsArchiveCAPXML)
	}
	if s.surface.allowsAdmin() {
		if !s.surface.allowsPublic() {
			mux.HandleFunc("/", s.adminRoot)
		}
		mux.HandleFunc("/admin", s.admin)
		mux.HandleFunc("/banner", s.banner)
		mux.HandleFunc("/api/v1/banner/current", s.bannerCurrent)
		mux.HandleFunc("/api/v1/banner/stream", s.bannerStream)
		mux.HandleFunc("/api/v1/banner/audio", s.bannerAudio)
		mux.HandleFunc("/api/v1/banner/webrtc/offer", s.bannerWebRTCOffer)
		mux.HandleFunc("/api/v1/cgen/preview", s.cgenPreview)
		mux.HandleFunc("/api/v1/cgen/fonts/", s.cgenFontAsset)
		mux.HandleFunc("/api/v1/alerts/archive/cap.xml", s.alertsArchiveCAPXML)
		mux.HandleFunc("/api/v1/alert/audio", s.alertAudioUpload)
		mux.HandleFunc("/api/v1/locations/catalog/import", s.locationCatalogImport)
		mux.HandleFunc("/api/v1/wx-on-demand/generate", s.wxOnDemandGenerate)
		mux.HandleFunc("/api/v1/wx-on-demand/packages", s.wxOnDemandPackages)
		mux.HandleFunc("/api/v1/wx-on-demand/readers", s.wxOnDemandReaders)
		mux.HandleFunc("/api/v1/health", s.adminHealth)
		mux.HandleFunc("/login", s.login)
		mux.HandleFunc("/api/v1/auth/check", s.authCheckAPI)
		mux.HandleFunc("/api/v1/auth/login", s.loginAPI)
		mux.HandleFunc("/api/v1/auth/logout", s.logoutAPI)
		mux.HandleFunc("/api/v1/panel/ws", s.websocket)
		mux.HandleFunc("/api/v1/panel/events", s.adminPanelEvents)
		mux.HandleFunc("/api/v1/panel/state", s.adminPanelState)
		mux.HandleFunc("/api/v1/panel/command", s.panelCommand)
	}
	if s.surface.allowsAdmin() && s.receiver != nil && s.receiver.Enabled() {
		base := s.receiver.BasePath()
		mux.HandleFunc(base+"/pair/challenge", s.receiver.HandlePairChallenge)
		mux.HandleFunc(base+"/pair/complete", s.receiver.HandlePairComplete)
		mux.HandleFunc(base+"/session", s.receiver.HandleSession)
		mux.HandleFunc(base+"/ws", s.receiver.HandleWebSocket)
	}
	mux.HandleFunc("/assets/", s.staticAsset)
	return s.withSecurityHeaders(mux)
}

func (s *Server) publicIndex(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" && request.URL.Path != "/feeds" && request.URL.Path != "/listen" && request.URL.Path != "/alerts" && request.URL.Path != "/alerts/archive" {
		http.NotFound(writer, request)
		return
	}
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	if (request.URL.Path == "/feeds" || request.URL.Path == "/listen") && !s.publicFeedsAvailable() {
		http.NotFound(writer, request)
		return
	}
	if publicAlertsPath(request.URL.Path) && publicAlertsArchiveAccess(s.config) != "public" {
		http.NotFound(writer, request)
		return
	}
	s.servePublicIndexHTML(writer, request)
}

func publicAlertsPath(path string) bool {
	return path == "/alerts" || path == "/alerts/archive"
}

func (s *Server) adminRoot(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(writer, request)
		return
	}
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	http.Redirect(writer, request, "/admin", http.StatusSeeOther)
}

func (s *Server) admin(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	if !s.auth.AccountMode() {
		if token := strings.TrimSpace(request.URL.Query().Get("token")); token != "" && s.auth.ValidToken(token) {
			s.auth.SetCookieForRequest(writer, request, token)
			cleanURL := *request.URL
			query := cleanURL.Query()
			query.Del("token")
			cleanURL.RawQuery = query.Encode()
			http.Redirect(writer, request, cleanURL.RequestURI(), http.StatusSeeOther)
			return
		}
	}
	if !s.auth.Authenticated(request) {
		target := "/login?next=" + request.URL.EscapedPath()
		http.Redirect(writer, request, target, http.StatusSeeOther)
		return
	}
	s.serveHTML(writer, request, "admin.html")
}

func (s *Server) banner(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	s.serveHTML(writer, request, "banner.html")
}

func (s *Server) login(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	s.serveHTML(writer, request, "login.html")
}

func (s *Server) loginAPI(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", "POST")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json")
	request.Body = http.MaxBytesReader(writer, request.Body, 8*1024)
	var payload struct {
		Username   string `json:"username"`
		Password   string `json:"password"`
		TOTP       string `json:"totp"`
		Persistent bool   `json:"persistent"`
	}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		http.Error(writer, `{"type":"auth_error","detail":"invalid login request"}`, http.StatusBadRequest)
		return
	}
	result, err := s.auth.LoginWithRequest(request.Context(), LoginInput{
		Username: payload.Username, Password: payload.Password, TOTP: payload.TOTP,
		Persistent: payload.Persistent, Request: request,
	})
	if err != nil {
		status := http.StatusUnauthorized
		code := "auth_error"
		detail := "Sign in failed."
		if authErr, ok := err.(*AuthError); ok {
			status = authErr.HTTPStatus
			code = authErr.Code
			detail = authErr.Detail
		}
		response := map[string]any{
			"type":   "auth_error",
			"code":   code,
			"detail": detail,
		}
		if result.MFAEnrollmentRequired {
			response["mfa_enrollment_required"] = true
			response["mfa_enrollment_secret"] = result.MFAEnrollmentSecret
			response["mfa_enrollment_uri"] = result.MFAEnrollmentURI
		}
		writer.WriteHeader(status)
		_ = json.NewEncoder(writer).Encode(response)
		return
	}
	s.auth.SetLoginCookie(writer, request, result)
	response := map[string]any{
		"type":                     "auth_ok",
		"password_change_required": result.PasswordChangeRequired,
		"persistent":               result.Persistent,
	}
	if !s.auth.AccountMode() {
		response["token"] = result.Token
	}
	_ = json.NewEncoder(writer).Encode(response)
}

func (s *Server) authCheckAPI(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json")
	response := map[string]any{
		"type":          "auth_state",
		"authenticated": s.auth.Authenticated(request),
		"auth_enabled":  s.auth.Enabled(),
		"auth_required": s.auth.Enabled(),
		"auth_ready":    s.auth.Configured(),
		"site_name":     siteName(s.config),
		"on_air_name":   displayText(s.config.Operator.OnAirName),
		"version":       s.config.Version,
		"git_commit":    "unknown",
	}
	if identity, err := s.auth.Identity(request); err == nil {
		response["account"] = identity.Account
		response["password_change_required"] = identity.PasswordChangeRequired
	}
	_ = json.NewEncoder(writer).Encode(response)
}

func (s *Server) logoutAPI(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", "POST")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.auth.LogoutRequest(request); err != nil {
		status, response := commandErrorResponse(err)
		writeJSONStatus(writer, status, response)
		return
	}
	http.SetCookie(writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   s.auth.cookieSecureForRequest(request),
	})
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"type":       "logout_ok",
		"public_url": "/",
	})
}

func (s *Server) serveHTML(writer http.ResponseWriter, request *http.Request, name string) {
	writer.Header().Set("Cache-Control", "no-store")
	path := filepath.Join(s.webroot, filepath.Clean(name))
	http.ServeFile(writer, request, path)
}

func (s *Server) servePublicIndexHTML(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	path := filepath.Join(s.webroot, "index.html")
	raw, err := os.ReadFile(path)
	if err != nil {
		http.NotFound(writer, request)
		return
	}
	site := fallbackText(siteName(s.config), "Haze Weather Radio")
	onAir := fallbackText(displayText(s.config.Operator.OnAirName), site)
	title := onAir
	if !strings.Contains(strings.ToLower(title), "weather") && !strings.Contains(strings.ToLower(title), "radio") {
		title = title + " Weather Radio"
	}
	description := fmt.Sprintf("Live weather radio feeds, alerts, and public status for %s.", onAir)
	htmlText := string(raw)
	htmlText = strings.Replace(htmlText, `<meta name="description" content="Public status and feed access for Haze Weather Radio">`, `<meta name="description" content="`+html.EscapeString(description)+`">`, 1)
	htmlText = strings.Replace(htmlText, `<meta name="description" content="Live weather radio feeds, alerts, and public status.">`, `<meta name="description" content="`+html.EscapeString(description)+`">`, 1)
	htmlText = strings.Replace(htmlText, `<title>Haze Weather Radio</title>`, `<title>`+html.EscapeString(title)+`</title>`, 1)
	htmlText = strings.Replace(htmlText, `<title>Weather Radio Live Feeds</title>`, `<title>`+html.EscapeString(title)+`</title>`, 1)
	htmlText = strings.Replace(htmlText, `<h1 id="publicSiteTitle">Haze Weather Radio</h1>`, `<h1 id="publicSiteTitle">`+html.EscapeString(title)+`</h1>`, 1)
	htmlText = strings.Replace(htmlText, `<h1 id="publicSiteTitle">Weather Radio Live Feeds</h1>`, `<h1 id="publicSiteTitle">`+html.EscapeString(title)+`</h1>`, 1)
	_, _ = writer.Write([]byte(htmlText))
}

func (s *Server) staticAsset(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	assetPath := strings.TrimPrefix(request.URL.Path, "/assets/")
	clean := path.Clean("/" + assetPath)
	if clean == "/" || strings.HasSuffix(clean, "/") {
		http.NotFound(writer, request)
		return
	}
	ext := strings.ToLower(path.Ext(clean))
	if ext == "" || ext == ".html" || ext == ".htm" || ext == ".map" || assetPathHasHiddenSegment(clean) {
		http.NotFound(writer, request)
		return
	}
	assetFile, ok := s.staticAssetPath(clean)
	if !ok {
		http.NotFound(writer, request)
		return
	}
	if ext == ".webmanifest" {
		writer.Header().Set("Content-Type", "application/manifest+json")
	}
	writer.Header().Set("Cache-Control", staticAssetCacheControl(ext))
	http.ServeFile(writer, request, assetFile)
}

func (s *Server) cgenFontAsset(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	if _, ok := s.requireAdminRequest(writer, request); !ok {
		return
	}
	fontPath := strings.TrimPrefix(request.URL.Path, "/api/v1/cgen/fonts/")
	clean := path.Clean("/" + fontPath)
	if clean == "/" || strings.HasSuffix(clean, "/") || assetPathHasHiddenSegment(clean) || managedFontExtension(clean) == "" {
		http.NotFound(writer, request)
		return
	}
	fontFile, ok := s.managedFontAssetPath(clean)
	if !ok {
		http.NotFound(writer, request)
		return
	}
	if contentType := fontContentType(path.Ext(clean)); contentType != "" {
		writer.Header().Set("Content-Type", contentType)
	}
	writer.Header().Set("Cache-Control", "private, max-age=3600")
	http.ServeFile(writer, request, fontFile)
}

func assetPathHasHiddenSegment(cleanURLPath string) bool {
	for _, segment := range strings.Split(strings.Trim(cleanURLPath, "/"), "/") {
		if strings.HasPrefix(segment, ".") {
			return true
		}
	}
	return false
}

func staticAssetCacheControl(ext string) string {
	switch strings.ToLower(ext) {
	case ".gif", ".png", ".jpg", ".jpeg", ".webp", ".svg", ".ico", ".woff", ".woff2":
		return "public, max-age=86400"
	case ".js", ".css":
		return "no-cache, must-revalidate"
	case ".webmanifest", ".json":
		return "public, max-age=3600, must-revalidate"
	default:
		return "public, max-age=3600"
	}
}

func (s *Server) staticAssetPath(cleanURLPath string) (string, bool) {
	localPath := filepath.FromSlash(strings.TrimPrefix(cleanURLPath, "/"))
	if localPath == "" || filepath.IsAbs(localPath) || filepath.VolumeName(localPath) != "" {
		return "", false
	}
	root, err := filepath.Abs(s.webroot)
	if err != nil {
		return "", false
	}
	target, err := filepath.Abs(filepath.Join(root, localPath))
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", false
	}
	return target, true
}

func (s *Server) managedFontAssetPath(cleanURLPath string) (string, bool) {
	localPath := filepath.FromSlash(strings.TrimPrefix(cleanURLPath, "/"))
	if localPath == "" || filepath.IsAbs(localPath) || filepath.VolumeName(localPath) != "" {
		return "", false
	}
	root, err := filepath.Abs(resolveConfigPath(s.configPath, filepath.Join("managed", "fonts")))
	if err != nil {
		return "", false
	}
	target, err := filepath.Abs(filepath.Join(root, localPath))
	if err != nil {
		return "", false
	}
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", false
	}
	return target, true
}

func fontContentType(ext string) string {
	switch strings.ToLower(ext) {
	case ".woff":
		return "font/woff"
	case ".woff2":
		return "font/woff2"
	case ".ttf":
		return "font/ttf"
	case ".otf":
		return "font/otf"
	case ".ttc", ".otc":
		return "font/collection"
	default:
		return ""
	}
}

func (s *Server) publicHealth(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	payload := map[string]any{
		"ok":           true,
		"service":      "haze-web",
		"started_at":   s.startedAt,
		"capabilities": WebRTCAudioCapabilities(),
	}
	if health, ok := mediaServiceHealth(request.Context(), s.config); ok {
		payload["media_service"] = publicMediaServiceHealth(health)
	}
	writeJSON(writer, payload)
}

func (s *Server) adminHealth(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	if !s.auth.FullyAuthenticated(request) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	webrtcPeers := s.media.WebRTCPeerSnapshots()
	webrtcSources := s.media.WebRTCFrameSourceSnapshots()
	payload := map[string]any{
		"ok":                  true,
		"service":             "haze-web",
		"started_at":          s.startedAt,
		"capabilities":        WebRTCAudioCapabilities(),
		"webrtc_peers":        webrtcPeers,
		"webrtc_peer_count":   len(webrtcPeers),
		"webrtc_sources":      webrtcSources,
		"webrtc_source_count": len(webrtcSources),
	}
	if s.receiver != nil {
		receivers := s.receiver.ActiveSnapshots()
		payload["receiver_connections"] = receivers
		payload["receiver_connection_count"] = len(receivers)
	}
	if health, ok := mediaServiceHealth(request.Context(), s.config); ok {
		payload["media_service"] = health
	}
	writeJSON(writer, payload)
}

func publicMediaServiceHealth(health map[string]any) map[string]any {
	out := map[string]any{
		"ok":                  boolValue(health, "ok"),
		"service":             stringValue(health, "service"),
		"backend":             stringValue(health, "backend"),
		"gstreamer_available": boolValue(health, "gstreamer_available"),
	}
	if capabilities, _ := health["capabilities"].(map[string]any); capabilities != nil {
		out["capabilities"] = map[string]any{
			"http_audio":         boolValue(capabilities, "http_audio"),
			"encoded_http_audio": boolValue(capabilities, "encoded_http_audio"),
			"webrtc":             boolValue(capabilities, "webrtc"),
			"webrtc_reason":      stringValue(capabilities, "webrtc_reason"),
		}
	}
	feeds, _ := health["feeds"].([]any)
	allAudioOK := true
	warningCount := 0
	feedCount := 0
	for _, item := range feeds {
		feed, _ := item.(map[string]any)
		if feed == nil {
			continue
		}
		feedCount++
		if !boolValue(feed, "audio_ok") {
			allAudioOK = false
		}
		if warnings, _ := feed["audio_warnings"].([]any); warnings != nil {
			warningCount += len(warnings)
		}
	}
	out["feed_count"] = feedCount
	out["audio_ok"] = allAudioOK
	out["audio_warning_count"] = warningCount
	return out
}

func (s *Server) websocket(writer http.ResponseWriter, request *http.Request) {
	// The authenticated panel WebSocket is same-origin only. Public audio and
	// receiver sockets have separate handlers and origin policies.
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	defer connection.CloseNow()
	connection.SetReadLimit(maxWebSocketMessageBytes)

	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	session := &wsSession{
		conn:       connection,
		auth:       s.auth,
		request:    request,
		config:     s.config,
		configPath: s.configPath,
		startedAt:  s.startedAt,
		media:      s.media,
		server:     s,
		breakIns:   map[string]struct{}{},
	}
	defer session.cancelOwnedOperatorBreakIns()
	_ = session.send(ctx, "hello", map[string]any{
		"service": "haze-web",
	})
	publicSocket := strings.HasPrefix(request.URL.Path, "/api/public/")
	if publicSocket {
		if state, err := session.publicState(); err == nil {
			_ = session.send(ctx, "public_state", state)
			session.lastStateSignature = stateSignature(state)
		}
	} else if s.auth.Authenticated(request) {
		if state, err := session.panelState(); err == nil {
			_ = session.send(ctx, "admin_state", state)
			session.lastStateSignature = stateSignature(state)
		}
	}

	type readResult struct {
		data []byte
		err  error
	}
	readDone := make(chan readResult, 1)
	go func() {
		for {
			_, data, err := connection.Read(ctx)
			select {
			case readDone <- readResult{data: data, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-readDone:
			if result.err != nil {
				return
			}
			if err := session.handle(ctx, result.data); err != nil {
				return
			}
		case <-ticker.C:
			if publicSocket {
				if state, err := session.publicState(); err == nil {
					if session.shouldSendState(state) {
						_ = session.send(ctx, "public_state", state)
						continue
					}
				}
			} else if s.auth.Authenticated(request) {
				if state, err := session.panelState(); err == nil {
					if session.shouldSendState(state) {
						_ = session.send(ctx, "admin_state", state)
						continue
					}
				}
			}
			_ = session.send(ctx, "heartbeat", map[string]any{
				"timestamp": time.Now().UTC(),
			})
		}
	}
}

func (s *Server) adminPanelState(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	if !s.auth.FullyAuthenticated(request) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	session := s.newPanelHTTPSession(request)
	state, err := session.panelState()
	if err != nil {
		http.Error(writer, "state unavailable", http.StatusInternalServerError)
		return
	}
	writeJSON(writer, state)
}

func (s *Server) publicPanelState(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	session := s.newPanelHTTPSession(request)
	state, err := session.publicState()
	if err != nil {
		http.Error(writer, "state unavailable", http.StatusInternalServerError)
		return
	}
	writeJSON(writer, state)
}

func (s *Server) adminPanelEvents(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", "GET")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.auth.FullyAuthenticated(request) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.streamPanelEvents(writer, request, false)
}

func (s *Server) publicPanelEvents(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", "GET")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.streamPanelEvents(writer, request, true)
}

func (s *Server) streamPanelEvents(writer http.ResponseWriter, request *http.Request, public bool) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Accel-Buffering", "no")
	session := s.newPanelHTTPSession(request)
	stateName := "admin_state"
	loadState := session.panelState
	if public {
		stateName = "public_state"
		loadState = session.publicState
	}
	state, err := loadState()
	if err != nil {
		_ = writeSSEEvent(writer, "error", map[string]any{"detail": "state unavailable"})
		flusher.Flush()
		return
	}
	session.lastStateSignature = stateSignature(state)
	if err := writeSSEEvent(writer, stateName, state); err != nil {
		return
	}
	flusher.Flush()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case <-ticker.C:
			state, err := loadState()
			if err == nil && session.shouldSendState(state) {
				if err := writeSSEEvent(writer, stateName, state); err != nil {
					return
				}
			} else if err := writeSSEEvent(writer, "heartbeat", map[string]any{"timestamp": time.Now().UTC()}); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) panelCommand(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", "POST")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	token := explicitAdminTokenFromRequest(request)
	headerIntent := strings.EqualFold(strings.TrimSpace(request.Header.Get("X-Haze-Admin-Intent")), "command")
	if (token == "" || !s.auth.ValidToken(token)) && !(headerIntent && s.auth.Authenticated(request)) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxWebSocketMessageBytes)
	var payload struct {
		Command string         `json:"command"`
		Payload map[string]any `json:"payload"`
	}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"type": "command_error", "detail": "invalid command request"})
		return
	}
	command := strings.TrimSpace(payload.Command)
	if command == "" {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"type": "command_error", "detail": "command is required"})
		return
	}
	session := s.newPanelHTTPSession(request)
	result, err := session.handleCommand(command, payload.Payload)
	if err != nil {
		status, response := commandErrorResponse(err)
		writeJSONStatus(writer, status, response)
		return
	}
	writeJSON(writer, map[string]any{"type": "command_result", "result": result})
}

func commandErrorResponse(err error) (int, map[string]any) {
	status := http.StatusBadRequest
	code := "command_failed"
	detail := err.Error()
	var authErr *AuthError
	if errors.As(err, &authErr) {
		code = authErr.Code
		if authErr.HTTPStatus >= 400 && authErr.HTTPStatus <= 599 {
			status = authErr.HTTPStatus
		}
	} else if errors.Is(err, errAccountNotFound) {
		status = http.StatusNotFound
		code = "account_not_found"
		detail = "The requested account was not found."
	} else {
		lower := strings.ToLower(detail)
		if strings.Contains(lower, "unique constraint") || strings.Contains(lower, "duplicate key") {
			status = http.StatusConflict
			code = "account_conflict"
			detail = "An account with that username already exists."
		} else if commandInfrastructureError(lower) {
			status = http.StatusServiceUnavailable
			code = "service_unavailable"
			detail = "The command could not be completed because a required service or secure store is unavailable."
		}
	}
	return status, map[string]any{
		"type":   "command_error",
		"code":   code,
		"detail": detail,
	}
}

func commandInfrastructureError(detail string) bool {
	for _, marker := range []string{
		"audit integrity", "audit log", "account store", "session registry", "redis", "database",
		"context deadline", "temporarily unavailable", "event bridge", "location service bridge", "location bridge", "connect session",
		"open audit", "read audit", "write audit", "sync audit", "publish ",
	} {
		if strings.Contains(detail, marker) {
			return true
		}
	}
	return false
}

func (s *Server) requireRequestIdentity(writer http.ResponseWriter, request *http.Request) (Identity, bool) {
	identity, err := s.auth.Identity(request)
	if err != nil {
		status, response := commandErrorResponse(err)
		response["type"] = "auth_error"
		writeJSONStatus(writer, status, response)
		return Identity{}, false
	}
	if identity.PasswordChangeRequired {
		err := &AuthError{Code: "password_change_required", Detail: "Password change is required before using this endpoint.", HTTPStatus: http.StatusForbidden}
		status, response := commandErrorResponse(err)
		response["type"] = "auth_error"
		writeJSONStatus(writer, status, response)
		return Identity{}, false
	}
	return identity, true
}

func (s *Server) requireAdminRequest(writer http.ResponseWriter, request *http.Request) (Identity, bool) {
	identity, ok := s.requireRequestIdentity(writer, request)
	if !ok {
		return Identity{}, false
	}
	if s.auth.AccountMode() && !identity.Account.IsAdmin {
		err := &AuthError{Code: "administrator_required", Detail: "Administrator permission is required.", HTTPStatus: http.StatusForbidden}
		status, response := commandErrorResponse(err)
		response["type"] = "auth_error"
		writeJSONStatus(writer, status, response)
		return Identity{}, false
	}
	return identity, true
}

func (s *Server) requireOriginationRequest(writer http.ResponseWriter, request *http.Request) (Identity, bool) {
	identity, ok := s.requireRequestIdentity(writer, request)
	if !ok {
		return Identity{}, false
	}
	if s.auth.AccountMode() {
		if err := s.auth.accounts.AllowOrigination(request.Context(), identity); err != nil {
			status, response := commandErrorResponse(err)
			response["type"] = "auth_error"
			writeJSONStatus(writer, status, response)
			return Identity{}, false
		}
	}
	return identity, true
}

func (s *Server) publicFeedWebRTCOffer(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", "POST")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxWebSocketMessageBytes)
	var payload map[string]any
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"type": "webrtc_error", "detail": "invalid WebRTC offer request"})
		return
	}
	feedID := strings.TrimSpace(stringValue(payload, "feed_id"))
	if feedID == "" || len(feedID) > webRTCOfferMaxFeedIDLength || !validPublicAudioFeedID(feedID) {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"type": "webrtc_error", "detail": "feed_id is invalid"})
		return
	}
	offerSDP := normalizeWebRTCOfferSDP(firstNonBlank(
		stringValue(payload, "sdp"),
		stringValue(mapValue(payload, "data"), "sdp"),
	))
	if offerSDP == "" || len(offerSDP) > webRTCOfferMaxSDPLength {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"type": "webrtc_error", "detail": "sdp is required"})
		return
	}
	if !s.config.Webpanel.Public.Feeds.WebRTC.Enabled {
		writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]any{"type": "webrtc_error", "detail": "feed streaming is not available"})
		return
	}
	access := publicFeedAccess(s.config)
	if access == "disabled" {
		writeJSONStatus(writer, http.StatusForbidden, map[string]any{"type": "webrtc_error", "detail": "public feeds are disabled"})
		return
	}
	if access == "auth_required" && !s.auth.FullyAuthenticated(request) {
		writeJSONStatus(writer, http.StatusUnauthorized, map[string]any{"type": "auth_error", "detail": "not authenticated"})
		return
	}
	session := s.newPanelHTTPSession(request)
	if !session.feedWebRTCEnabled(feedID) {
		writeJSONStatus(writer, http.StatusNotFound, map[string]any{"type": "webrtc_error", "detail": "feed streaming is not configured or disabled"})
		return
	}
	preferredCodec := firstNonBlank(stringValue(payload, "codec"), stringValue(payload, "preferred_codec"))
	if len(strings.TrimSpace(preferredCodec)) > webRTCOfferMaxCodecLength {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"type": "webrtc_error", "detail": "codec is too long"})
		return
	}
	answerRequest := map[string]any{
		"feed_id":         feedID,
		"sdp":             offerSDP,
		"disable_g722":    boolValue(payload, "disable_g722"),
		"require_opus":    boolValue(payload, "require_opus"),
		"preferred_codec": preferredCodec,
		"client_ip":       clientIPForMediaRequest(request),
		"remote_addr":     request.RemoteAddr,
	}
	mediaResult := s.mediaServiceWebRTCAnswerResult(request.Context(), answerRequest)
	if mediaResult.OK() {
		answer := mediaResult.Answer
		answer["type"] = "webrtc_answer"
		answer["feed_id"] = feedID
		if _, ok := answer["sdp_type"]; !ok {
			answer["sdp_type"] = "answer"
		}
		writeJSON(writer, answer)
		return
	}
	if mediaResult.Terminal {
		status := mediaResult.StatusCode
		if status < 400 {
			status = http.StatusBadGateway
		}
		writeJSONStatus(writer, status, map[string]any{
			"type":   "webrtc_error",
			"detail": firstNonBlank(mediaResult.Detail, "haze-media WebRTC offer was rejected"),
		})
		return
	}
	legacyMediaAvailable := s.media != nil && s.media.Available()
	if !legacyMediaAvailable {
		writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]any{"type": "webrtc_error", "detail": "media bridge is not available"})
		return
	}
	release, ok := s.acquireFeedListener(writer, request, feedID)
	if !ok {
		return
	}
	answer, err := s.media.AnswerWithOptions(request.Context(), feedID, offerSDP, WebRTCAnswerOptions{
		DisableG722:    boolValue(payload, "disable_g722"),
		RequireOpus:    boolValue(payload, "require_opus"),
		PreferredCodec: preferredCodec,
		OnClose:        release,
	})
	if err != nil {
		release()
		writeJSON(writer, map[string]any{"type": "webrtc_error", "detail": err.Error()})
		return
	}
	writeJSON(writer, map[string]any{
		"type":         "webrtc_answer",
		"feed_id":      feedID,
		"sdp":          answer.SDP,
		"sdp_type":     "answer",
		"media_recent": answer.MediaRecent,
		"codec":        answer.Codec.String(),
		"payload_type": answer.PayloadType,
	})
}

func (s *Server) newPanelHTTPSession(request *http.Request) *wsSession {
	return &wsSession{
		auth:       s.auth,
		request:    request,
		config:     s.config,
		configPath: s.configPath,
		startedAt:  s.startedAt,
		media:      s.media,
		server:     s,
		breakIns:   map[string]struct{}{},
	}
}

func writeSSEEvent(writer http.ResponseWriter, event string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "event: %s\n", event); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "data: %s\n\n", raw); err != nil {
		return err
	}
	return nil
}

func explicitAdminTokenFromRequest(request *http.Request) string {
	if request == nil {
		return ""
	}
	if header := strings.TrimSpace(request.Header.Get("Authorization")); header != "" {
		const prefix = "Bearer "
		if strings.HasPrefix(strings.ToLower(header), strings.ToLower(prefix)) {
			return strings.TrimSpace(header[len(prefix):])
		}
	}
	return strings.TrimSpace(request.Header.Get("X-Haze-Admin-Token"))
}

type wsSession struct {
	mu                 sync.Mutex
	conn               *websocket.Conn
	auth               *AuthManager
	request            *http.Request
	config             Config
	configPath         string
	startedAt          time.Time
	lastStateSignature string
	media              *MediaHub
	server             *Server
	breakIns           map[string]struct{}
	publicStateCache   map[string]any
	publicStateCacheAt time.Time
}

const (
	webRTCOfferMaxFeedIDLength = 128
	webRTCOfferMaxSDPLength    = 256 * 1024
	webRTCOfferMaxCodecLength  = 32
	publicAlertStateCacheTTL   = 15 * time.Second
)

func (s *wsSession) send(ctx context.Context, messageType string, data map[string]any) error {
	return s.sendEnvelope(ctx, map[string]any{
		"type":      messageType,
		"timestamp": time.Now().UTC(),
		"data":      data,
	})
}

func (s *wsSession) sendEnvelope(ctx context.Context, payload map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return s.conn.Write(ctx, websocket.MessageText, raw)
}

func (s *wsSession) handle(ctx context.Context, raw []byte) error {
	var message map[string]any
	if err := json.Unmarshal(raw, &message); err != nil {
		return s.reply(ctx, message, "error", map[string]any{"detail": "invalid json"})
	}
	msgType := stringValue(message, "type")
	if strings.HasPrefix(s.request.URL.Path, "/api/public/") {
		switch msgType {
		case "ping":
			return s.reply(ctx, message, "pong", map[string]any{})
		case "webrtc_offer":
			return s.handleWebRTCOffer(ctx, message)
		default:
			return s.reply(ctx, message, "error", map[string]any{"detail": "unsupported public message type"})
		}
	}
	switch msgType {
	case "auth_check":
		state := map[string]any{
			"authenticated": s.auth.Authenticated(s.request),
			"auth_enabled":  s.auth.Enabled(),
			"auth_required": s.auth.Enabled(),
			"auth_ready":    s.auth.Configured(),
			"site_name":     siteName(s.config),
			"on_air_name":   displayText(s.config.Operator.OnAirName),
			"version":       s.config.Version,
			"git_commit":    "unknown",
		}
		if identity, err := s.auth.Identity(s.request); err == nil {
			state["account"] = identity.Account
			state["password_change_required"] = identity.PasswordChangeRequired
		}
		return s.reply(ctx, message, "auth_state", state)
	case "login":
		token, err := s.auth.Login(stringValue(message, "password"))
		if err != nil {
			return s.reply(ctx, message, "auth_error", map[string]any{"detail": err.Error()})
		}
		return s.reply(ctx, message, "auth_ok", map[string]any{"token": token})
	case "logout":
		if err := s.auth.LogoutRequest(s.request); err != nil {
			return s.reply(ctx, message, "auth_error", map[string]any{"detail": err.Error()})
		}
		return s.reply(ctx, message, "logout_ok", map[string]any{})
	case "ping":
		return s.reply(ctx, message, "pong", map[string]any{})
	case "webrtc_offer":
		return s.handleWebRTCOffer(ctx, message)
	case "command":
		if !s.auth.Authenticated(s.request) {
			return s.reply(ctx, message, "auth_error", map[string]any{"detail": "not authenticated"})
		}
		result, err := s.handleCommand(stringValue(message, "command"), mapValue(message, "payload"))
		if err != nil {
			return s.reply(ctx, message, "command_error", map[string]any{"detail": err.Error()})
		}
		return s.reply(ctx, message, "command_result", map[string]any{"result": result})
	default:
		if !s.auth.Authenticated(s.request) {
			return s.reply(ctx, message, "auth_error", map[string]any{"detail": "not authenticated"})
		}
		return s.reply(ctx, message, "error", map[string]any{"detail": "unsupported message type"})
	}
}

func (s *wsSession) handleWebRTCOffer(ctx context.Context, message map[string]any) error {
	feedID := strings.TrimSpace(stringValue(message, "feed_id"))
	if feedID == "" {
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "feed_id is required"})
	}
	if len(feedID) > webRTCOfferMaxFeedIDLength {
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "feed_id is too long"})
	}
	if !validPublicAudioFeedID(feedID) {
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "feed_id is invalid"})
	}
	offerSDP := normalizeWebRTCOfferSDP(firstNonBlank(
		stringValue(message, "sdp"),
		stringValue(mapValue(message, "data"), "sdp"),
	))
	if offerSDP == "" {
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "sdp is required"})
	}
	if len(offerSDP) > webRTCOfferMaxSDPLength {
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "sdp is too long"})
	}
	preferredCodec := firstNonBlank(stringValue(message, "codec"), stringValue(message, "preferred_codec"))
	if len(strings.TrimSpace(preferredCodec)) > webRTCOfferMaxCodecLength {
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "codec is too long"})
	}
	legacyMediaAvailable := s.media != nil && s.media.Available()
	mediaServiceConfigured := mediaServiceBaseURL(s.config) != ""
	if (!legacyMediaAvailable && !mediaServiceConfigured) || !s.config.Webpanel.Public.Feeds.WebRTC.Enabled {
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "feed streaming is not available"})
	}
	if strings.HasPrefix(s.request.URL.Path, "/api/public/") {
		access := publicFeedAccess(s.config)
		if access == "disabled" {
			return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "public feeds are disabled"})
		}
		if access == "auth_required" && !s.auth.FullyAuthenticated(s.request) {
			return s.reply(ctx, message, "auth_error", map[string]any{"detail": "not authenticated"})
		}
	} else if !s.auth.FullyAuthenticated(s.request) {
		return s.reply(ctx, message, "auth_error", map[string]any{"detail": "not authenticated"})
	}
	if !s.feedWebRTCEnabled(feedID) {
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "feed streaming is not configured or disabled"})
	}
	mediaResult := s.server.mediaServiceWebRTCAnswerResult(ctx, map[string]any{
		"feed_id":         feedID,
		"sdp":             offerSDP,
		"disable_g722":    boolValue(message, "disable_g722"),
		"require_opus":    boolValue(message, "require_opus"),
		"preferred_codec": preferredCodec,
		"client_ip":       clientIPForMediaRequest(s.request),
		"remote_addr":     s.request.RemoteAddr,
	})
	if mediaResult.OK() {
		answer := mediaResult.Answer
		answer["feed_id"] = feedID
		if _, ok := answer["sdp_type"]; !ok {
			answer["sdp_type"] = "answer"
		}
		return s.reply(ctx, message, "webrtc_answer", answer)
	}
	if mediaResult.Terminal {
		return s.reply(ctx, message, "webrtc_error", map[string]any{
			"detail": firstNonBlank(mediaResult.Detail, "haze-media WebRTC offer was rejected"),
		})
	}
	if !legacyMediaAvailable {
		if mediaServiceConfigured {
			return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "haze-media WebRTC service is unavailable and legacy media bridge is not available"})
		}
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "media bridge is not available"})
	}
	var release func()
	if s.server != nil && s.server.listeners != nil {
		var ok bool
		release, _, ok = s.server.listeners.TryAcquire(feedID, listenerClientID(s.request))
		if !ok {
			return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": "listener already active for this IP and feed"})
		}
	}
	answer, err := s.media.AnswerWithOptions(ctx, feedID, offerSDP, WebRTCAnswerOptions{
		DisableG722:    boolValue(message, "disable_g722"),
		RequireOpus:    boolValue(message, "require_opus"),
		PreferredCodec: preferredCodec,
		OnClose:        release,
	})
	if err != nil {
		if release != nil {
			release()
		}
		return s.reply(ctx, message, "webrtc_error", map[string]any{"detail": err.Error()})
	}
	return s.reply(ctx, message, "webrtc_answer", map[string]any{
		"feed_id":      feedID,
		"sdp":          answer.SDP,
		"sdp_type":     "answer",
		"media_recent": answer.MediaRecent,
		"codec":        answer.Codec.String(),
		"payload_type": answer.PayloadType,
	})
}

func (s *wsSession) feedWebRTCEnabled(feedID string) bool {
	return feedAudioOutputEnabled(s.configPath, feedID)
}

func normalizeWebRTCOfferSDP(sdp string) string {
	sdp = strings.TrimSpace(sdp)
	if sdp == "" {
		return ""
	}
	sdp = strings.ReplaceAll(sdp, "\r\n", "\n")
	sdp = strings.ReplaceAll(sdp, "\r", "\n")
	lines := strings.Split(sdp, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimRight(line, " \t")
		if line == "" {
			continue
		}
		out = append(out, line)
	}
	if len(out) == 0 {
		return ""
	}
	return strings.Join(out, "\r\n") + "\r\n"
}

func (s *wsSession) handleCommand(command string, payload map[string]any) (any, error) {
	identity, err := s.auth.Identity(s.request)
	if err != nil {
		return nil, err
	}
	if identity.PasswordChangeRequired && command != "profile.password.change" && command != "profile.get" {
		return nil, &AuthError{Code: "password_change_required", Detail: "Password change is required before using the panel.", HTTPStatus: http.StatusForbidden}
	}
	if s.auth.AccountMode() && !identity.Account.IsAdmin && !nonAdminCommandAllowed(command, payload, identity.Account) {
		return nil, &AuthError{Code: "command_forbidden", Detail: fmt.Sprintf("Account is not authorized for command %q.", command), HTTPStatus: http.StatusForbidden}
	}
	if usesOriginationPolicy(command, payload) {
		payload, err = prepareAccountOriginationPolicyPayload(s.configPath, command, payload)
		if err != nil {
			return nil, err
		}
		if s.auth.AccountMode() {
			if err := s.auth.accounts.AllowOrigination(s.request.Context(), identity); err != nil {
				return nil, err
			}
		}
		payload, err = applyAccountOriginationPolicy(payload, identity)
		if err != nil {
			return nil, err
		}
	}
	if s.auth.AccountMode() && isOriginationExecution(command, payload) {
		if err := s.auditOrigination(identity, command, "ALERT_ORIGINATION_REQUESTED", payload, nil); err != nil {
			return nil, err
		}
	}
	webpanelMutation := s.auth.AccountMode() && auditedWebpanelMutation(command, payload)
	if webpanelMutation {
		if err := s.auditGenericWebpanelMutation(identity, command, "REQUESTED", payload); err != nil {
			return nil, err
		}
	}
	result, err := s.executeCommand(command, payload)
	if err != nil {
		if s.auth.AccountMode() && isOriginationExecution(command, payload) {
			_ = s.auditOrigination(identity, command, "ALERT_ORIGINATION_FAILED", payload, map[string]any{"error": err.Error()})
		}
		if webpanelMutation {
			_ = s.auditGenericWebpanelMutation(identity, command, "FAILED", payload)
		}
		return nil, err
	}
	if s.auth.AccountMode() && isOriginationExecution(command, payload) {
		resultMap, _ := result.(map[string]any)
		if err := s.auditOrigination(identity, command, "ALERT_ORIGINATION_ACCEPTED", payload, resultMap); err != nil {
			if resultMap != nil {
				resultMap["audit_status"] = "completion_record_failed"
			}
			return result, nil
		}
	}
	if webpanelMutation {
		if err := s.auditGenericWebpanelMutation(identity, command, "COMPLETED", payload); err != nil {
			if resultMap, ok := result.(map[string]any); ok {
				resultMap["audit_status"] = "completion_record_failed"
			}
		}
	}
	return result, nil
}

func (s *wsSession) executeCommand(command string, payload map[string]any) (any, error) {
	if accountCommandName(command) {
		return s.accountCommand(command, payload)
	}
	switch command {
	case "daemon.settings.get":
		return daemonSettingsPayload(s.configPath)
	case "daemon.settings.save":
		settings, ok := payload["settings"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("settings payload is required")
		}
		return writeDaemonSettings(s.configPath, settings)
	case "daemon.service.control":
		return s.publishServiceControl(payload)
	case "dictionary.get":
		return loadDictionaryPayload(s.configPath)
	case "dictionary.save":
		return writeDictionaryPayload(s.configPath, payload)
	case "tts.get":
		return loadTTSPayload(s.configPath)
	case "tts.save":
		return saveTTSPayload(s.configPath, payload)
	case "tts.preview":
		return s.previewTTS(payload)
	case "feeds.get":
		return loadFeedsControlPayload(s.configPath)
	case "feeds.save":
		return saveFeedsControlPayload(s.configPath, payload)
	case "feeds.control":
		return s.publishFeedControl(payload)
	case "bulletins.get":
		return loadBulletinsPayload(s.configPath)
	case "bulletins.save":
		return saveBulletinsPayload(s.configPath, payload)
	case "bulletins.import":
		return importBulletinsPayload(s.configPath, payload)
	case "bulletins.export":
		return exportBulletinsPayload(s.configPath, payload)
	case "bulletins.upload_audio":
		return uploadBulletinAudio(s.configPath, payload)
	case "cgen.get":
		return loadCgenPayload(s.configPath)
	case "cgen.catalog":
		return cgenCatalogPayload(s.configPath)
	case "cgen.scenes.list":
		return listCgenScenesPayload(s.configPath)
	case "cgen.scenes.get":
		return getCgenScenePayload(s.configPath, payload)
	case "cgen.scenes.save":
		result, err := saveCgenScenePayload(s.configPath, payload)
		if err != nil {
			return nil, err
		}
		_ = s.publishCgenScenesUpdated(result)
		return result, nil
	case "cgen.scenes.delete":
		result, err := deleteCgenScenePayload(s.configPath, payload)
		if err != nil {
			return nil, err
		}
		_ = s.publishCgenScenesUpdated(result)
		return result, nil
	case "cgen.save":
		result, err := saveCgenPayload(s.configPath, payload)
		if err != nil {
			return nil, err
		}
		_ = s.publishCgenConfigUpdated(result)
		return result, nil
	case "cgen.action":
		result, err := cgenActionPayload(s.configPath, payload)
		if err != nil {
			return nil, err
		}
		_ = s.publishCgenControl(payload, result)
		return result, nil
	case "state":
		return s.panelState()
	case "wx.packages":
		packageIDs, err := loadWxOnDemandPackageIDs(s.configPath)
		if err != nil {
			return nil, err
		}
		return map[string]any{"packages": packageIDs}, nil
	case "wx.readers":
		readers, err := loadReaderCatalog(s.configPath)
		if err != nil {
			return nil, err
		}
		return map[string]any{"readers": readers}, nil
	case "playlist.state":
		return playlistStatePayload(s.configPath)
	case "playlist.control":
		return s.publishPlaylistCommand("playlist.control", payload)
	case "playlist.insert":
		if strings.EqualFold(stringValue(payload, "kind"), "same") {
			result, err := s.airSame(payload)
			if err != nil {
				return nil, err
			}
			return map[string]any{"accepted": true, "same": result}, nil
		}
		return s.publishPlaylistCommand("playlist.insert", payload)
	case "alerts.archive.get":
		return alertsArchivePayload(s.configPath)
	case "logs.list":
		return listLogViewerFiles(logsViewerRoot(s.configPath))
	case "logs.tail":
		return tailLogViewerFile(logsViewerRoot(s.configPath), payload)
	case "locations.query":
		return s.queryLocationUtility(payload)
	case "alerts.archive.action":
		return handleAlertsArchiveAction(s.configPath, payload)
	case "same.event_codes":
		return loadSAMEMapping(s.configPath)
	case "same.location_names":
		return loadLocationNames(s.configPath)
	case "lead_statements.get":
		return loadLeadStatements(s.configPath)
	case "lead_statements.save":
		return s.saveLeadStatements(payload)
	case "lead_statements.preview":
		return s.previewLeadStatement(payload)
	case "automations.get", "same.templates.get":
		return loadAlertTemplates(s.configPath)
	case "automations.put", "same.templates.put":
		content, _ := payload["content"].(string)
		if content == "" {
			return nil, fmt.Errorf("automation content is required")
		}
		return writeAlertTemplates(s.configPath, content)
	case "same.intro":
		return sameIntroPayload(s.configPath, payload)
	case "same.test":
		return s.generateSameTest(payload)
	case "same.generate":
		return s.generateSame(payload)
	case "same.air":
		return s.airSame(payload)
	case "alert.broadcast":
		return s.broadcastAlert(payload)
	case "alert.preview":
		return s.previewAlert(payload)
	case "operator_breakin.prerolls":
		return s.listOperatorBreakInPrerolls()
	case "operator_breakin.upload_preroll":
		return s.uploadOperatorBreakInPreroll(payload)
	case "operator_breakin.generate_tone":
		return s.generateOperatorBreakInTone(payload)
	case "operator_breakin.start":
		return s.startOperatorBreakIn(payload)
	case "operator_breakin.chunk":
		return s.appendOperatorBreakInChunk(payload)
	case "operator_breakin.finish":
		return s.finishOperatorBreakIn(payload)
	case "operator_breakin.url":
		return s.queueOperatorBreakInURL(payload)
	case "operator_breakin.cancel":
		return s.cancelOperatorBreakIn(payload)
	case "same.upload_audio":
		return nil, fmt.Errorf("SAME media upload is not available in this gateway yet")
	case "wx.generate":
		return s.generateWx(payload)
	case "health":
		return map[string]any{
			"ok":            true,
			"service":       "haze-web",
			"started_at":    s.startedAt,
			"auth_required": s.auth != nil && s.auth.Enabled(),
			"wx_base":       "/api/v1/wx-on-demand",
			"capabilities": map[string]any{
				"same_queue":     true,
				"same_broadcast": false,
				"raw_udp_output": true,
				"wx_generate":    strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR")) != "",
			},
		}, nil
	default:
		return nil, fmt.Errorf("command %q is not implemented by the Go gateway yet", command)
	}
}

func (s *wsSession) publishCgenConfigUpdated(payload map[string]any) error {
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return nil
	}
	publisher := events.NewHostBridgePublisher(bridgeAddr)
	defer publisher.Close()
	data := map[string]any{
		"revision":               payload["revision"],
		"hash":                   payload["hash"],
		"schema_version":         payload["schema_version"],
		"encoder_schema_version": payload["encoder_schema_version"],
		"enabled":                payload["enabled"],
		"summary":                payload["summary"],
	}
	return publisher.Publish(events.Event{
		Type:   "cgen.config.updated",
		Source: "haze-web",
		Data:   data,
	})
}

func (s *wsSession) publishCgenControl(request map[string]any, result map[string]any) error {
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return nil
	}
	feedID := strings.TrimSpace(firstNonBlank(stringValue(request, "feed_id"), stringValue(request, "id")))
	if feedID == "" {
		return nil
	}
	feed, ok := cgenPayloadFeed(result, feedID)
	if !ok {
		return nil
	}
	data := cloneMap(feed)
	data["feed_id"] = feedID
	data["action"] = strings.ToLower(strings.TrimSpace(stringValue(request, "action")))
	publisher := events.NewHostBridgePublisher(bridgeAddr)
	defer publisher.Close()
	return publisher.Publish(events.Event{
		Type:    "cgen.control",
		Source:  "haze-web",
		Subject: feedID,
		Data:    data,
	})
}

func cgenPayloadFeed(payload map[string]any, feedID string) (map[string]any, bool) {
	feeds, ok := payload["feeds"].([]map[string]any)
	if !ok {
		return nil, false
	}
	for _, feed := range feeds {
		if strings.EqualFold(strings.TrimSpace(stringValue(feed, "id")), feedID) {
			return feed, true
		}
	}
	return nil, false
}

func (s *wsSession) publishPlaylistCommand(eventType string, payload map[string]any) (any, error) {
	feedID := strings.TrimSpace(stringValue(payload, "feed_id"))
	if feedID == "" {
		return nil, fmt.Errorf("feed_id is required")
	}
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return nil, fmt.Errorf("event bridge is not available")
	}
	before, _ := playlistStatePayload(s.configPath)
	previousUpdatedAt := playlistFeedUpdatedAt(before, feedID)
	publisher := events.NewHostBridgePublisher(bridgeAddr)
	defer publisher.Close()
	if err := publisher.Publish(events.Event{
		Type:    eventType,
		Source:  "haze-web",
		Subject: feedID,
		Data:    payload,
	}); err != nil {
		return nil, err
	}
	playlist, settled := waitForPlaylistStateChange(s.configPath, feedID, previousUpdatedAt, 1200*time.Millisecond)
	result := map[string]any{
		"accepted": true,
		"event":    eventType,
		"settled":  settled,
	}
	if playlist != nil {
		result["playlist"] = playlist
	}
	return result, nil
}

func (s *wsSession) publishFeedControl(payload map[string]any) (any, error) {
	action := strings.ToLower(strings.TrimSpace(stringValue(payload, "action")))
	switch action {
	case "pause":
		payload["action"] = "pause"
	case "unpause", "resume":
		payload["action"] = "resume"
	case "stop":
		payload["action"] = "flush_stop"
	case "restart", "start":
		payload["action"] = "flush_restart"
	default:
		return nil, fmt.Errorf("unsupported feed action %q", action)
	}
	return s.publishPlaylistCommand("playlist.control", payload)
}

func (s *wsSession) publishServiceControl(payload map[string]any) (any, error) {
	serviceID := strings.TrimSpace(firstNonBlank(stringValue(payload, "service_id"), stringValue(payload, "id")))
	action := strings.ToLower(strings.TrimSpace(stringValue(payload, "action")))
	if serviceID == "" {
		return nil, fmt.Errorf("service_id is required")
	}
	if action != "start" && action != "stop" && action != "restart" {
		return nil, fmt.Errorf("unsupported service action %q", action)
	}
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return nil, fmt.Errorf("event bridge is not available")
	}
	publisher := events.NewHostBridgePublisher(bridgeAddr)
	defer publisher.Close()
	if err := publisher.Publish(events.Event{
		Type:    "service.control",
		Source:  "haze-web",
		Subject: serviceID,
		Data: map[string]any{
			"service_id": serviceID,
			"action":     action,
		},
	}); err != nil {
		return nil, err
	}
	return map[string]any{
		"accepted":   true,
		"event":      "service.control",
		"service_id": serviceID,
		"action":     action,
	}, nil
}

func (s *wsSession) panelState() (map[string]any, error) {
	identity, identityErr := s.auth.Identity(s.request)
	if identityErr != nil {
		return nil, identityErr
	}
	if identity.PasswordChangeRequired {
		return map[string]any{
			"account": identity.Account,
			"summary": map[string]any{"password_change_required": true},
		}, nil
	}
	state, err := panelStatePayload(s.config, s.configPath, s.startedAt, s.request, s.media.Available())
	if err != nil {
		return nil, err
	}
	state["account"] = identity.Account
	if s.auth.AccountMode() && !identity.Account.IsAdmin {
		delete(state, "config")
		delete(state, "datapool")
		delete(state, "events")
		if !identity.Account.CanViewLogs {
			delete(state, "logs")
		}
	}
	return state, nil
}

func (s *wsSession) publicState() (map[string]any, error) {
	if s.publicAlertStateCacheable() && s.publicStateCache != nil && time.Since(s.publicStateCacheAt) < publicAlertStateCacheTTL {
		return s.publicStateCache, nil
	}
	listenerStats := map[string]FeedListenerStats{}
	if s.server != nil {
		listenerStats = s.server.listenerSnapshot(s.request.Context())
	}
	state, err := publicStatePayload(s.config, s.configPath, s.startedAt, s.request, s.auth, s.media.Available(), listenerStats)
	if err != nil {
		return nil, err
	}
	if s.publicAlertStateCacheable() {
		s.publicStateCache = state
		s.publicStateCacheAt = time.Now()
	}
	return state, nil
}

func (s *wsSession) publicAlertStateCacheable() bool {
	return publicRequestWantsAlerts(s.request) && !publicRequestWantsFeeds(s.request)
}

func (s *Server) publicFeedsAvailable() bool {
	feeds, err := loadBasicFeedSummaries(s.configPath)
	if err != nil {
		return false
	}
	return publicFeedPagesAvailable(s.config, feeds)
}

func publicFeedPagesAvailable(config Config, feeds []map[string]any) bool {
	if publicFeedAccess(config) == "disabled" {
		return false
	}
	for _, feed := range feeds {
		if enabled, _ := feed["enabled"].(bool); enabled {
			return true
		}
	}
	return false
}

func (s *wsSession) shouldSendState(state map[string]any) bool {
	signature := stateSignature(state)
	if signature == "" || signature == s.lastStateSignature {
		return false
	}
	s.lastStateSignature = signature
	return true
}

func (s *wsSession) reply(ctx context.Context, request map[string]any, messageType string, fields map[string]any) error {
	payload := map[string]any{
		"type":      messageType,
		"timestamp": time.Now().UTC(),
	}
	if requestID := stringValue(request, "request_id"); requestID != "" {
		payload["reply_to"] = requestID
	}
	for key, value := range fields {
		payload[key] = value
	}
	return s.sendEnvelope(ctx, payload)
}

func writeJSON(writer http.ResponseWriter, value any) {
	writer.Header().Set("Content-Type", "application/json")
	if writer.Header().Get("Cache-Control") == "" {
		writer.Header().Set("Cache-Control", "no-store")
	}
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		http.Error(writer, "json encode failed", http.StatusInternalServerError)
	}
}

func (s *Server) withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "SAMEORIGIN")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		writer.Header().Set("Permissions-Policy", "camera=(), microphone=(self), geolocation=(), payment=()")
		writer.Header().Set("Content-Security-Policy", contentSecurityPolicy(request.URL.Path))
		if publicNoStorePath(request.URL.Path) && writer.Header().Get("Cache-Control") == "" {
			writer.Header().Set("Cache-Control", "no-store")
		}
		if s.config.Webpanel.TLS.HSTS && requestIsHTTPS(request) {
			writer.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		next.ServeHTTP(writer, request)
	})
}

func requestMethodGETOrHEAD(writer http.ResponseWriter, request *http.Request) bool {
	if request.Method == http.MethodGet || request.Method == http.MethodHead {
		return true
	}
	writer.Header().Set("Allow", "GET, HEAD")
	http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func contentSecurityPolicy(path string) string {
	scriptSrc := "script-src 'self' https://unpkg.com"
	styleSrc := "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com"
	if publicSecurityPolicyPath(path) {
		scriptSrc = "script-src 'self'"
		styleSrc = "style-src 'self' https://fonts.googleapis.com"
	} else if path == "/banner" {
		scriptSrc = "script-src 'self' 'unsafe-inline'"
	}
	return strings.Join([]string{
		"default-src 'self'",
		scriptSrc,
		styleSrc,
		"font-src 'self' https://fonts.gstatic.com",
		"img-src 'self' data: https://tiles.openfreemap.org https://server.arcgisonline.com",
		"connect-src 'self' https://tiles.openfreemap.org https://server.arcgisonline.com ws: wss: stun: stuns: turn: turns:",
		"media-src 'self' blob: http: https:",
		"worker-src 'self'",
		"object-src 'none'",
		"base-uri 'self'",
		"frame-ancestors 'self'",
		"form-action 'self'",
	}, "; ")
}

func publicHTMLPath(path string) bool {
	return path == "/" || path == "/feeds" || path == "/listen" || publicAlertsPath(path)
}

func publicSecurityPolicyPath(path string) bool {
	return publicHTMLPath(path) || strings.HasPrefix(path, "/api/public/") || strings.HasPrefix(path, "/assets/")
}

func publicNoStorePath(path string) bool {
	return publicHTMLPath(path) || strings.HasPrefix(path, "/api/public/")
}

func allOriginPatterns() []string {
	return []string{"*"}
}

func siteName(config Config) string {
	if config.Webpanel.Public.SiteName != "" {
		return config.Webpanel.Public.SiteName
	}
	return "Haze Weather Radio"
}

// NormalizeSurface converts a CLI/config surface value into a known surface.
func NormalizeSurface(surface string) WebSurface {
	return normalizeSurface(surface)
}

func normalizeSurface(surface string) WebSurface {
	switch strings.ToLower(strings.TrimSpace(surface)) {
	case string(SurfacePublic):
		return SurfacePublic
	case string(SurfaceAdmin):
		return SurfaceAdmin
	default:
		return SurfaceCombined
	}
}

func (s WebSurface) allowsPublic() bool {
	return s == SurfacePublic || s == SurfaceCombined
}

func (s WebSurface) allowsAdmin() bool {
	return s == SurfaceAdmin || s == SurfaceCombined
}

func stringValue(message map[string]any, key string) string {
	value, _ := message[key].(string)
	return value
}

func boolValue(message map[string]any, key string) bool {
	switch value := message[key].(type) {
	case bool:
		return value
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "1", "true", "yes", "on", "enabled":
			return true
		}
	}
	return false
}

func mapValue(message map[string]any, key string) map[string]any {
	value, _ := message[key].(map[string]any)
	if value == nil {
		return map[string]any{}
	}
	return value
}
