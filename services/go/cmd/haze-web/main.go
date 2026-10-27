package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/processguard"
	"github.com/meowraii/haze-weather-radio/services/go/internal/webgateway"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(0)
	if err := run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("haze-web: %v", err)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:6444", "HTTP listen address")
	webroot := flag.String("webroot", "webroot", "webroot directory")
	configPath := flag.String("config", "config.yaml", "Haze config path")
	surface := flag.String("surface", "combined", "HTTP surface: public, admin, or combined")
	checkCodecs := flag.Bool("check-codecs", false, "print WebRTC codec capabilities and exit")
	requireOpus := flag.Bool("require-opus", false, "fail --check-codecs unless native Opus is available")
	unlockAccount := flag.String("unlock-account", "", "locally unlock an account, revoke its sessions, audit the recovery, and exit")
	resetPassword := flag.String("reset-password", "", "locally reset the password of every account matching a name or wildcard (* and ?) to a random value, print the new passwords, revoke sessions, audit the reset, and exit")
	flag.Parse()

	if *checkCodecs {
		capabilities := webgateway.WebRTCAudioCapabilities()
		if *requireOpus {
			opus, _ := capabilities["webrtc_opus"].(bool)
			if !opus {
				return fmt.Errorf("native Opus is not available in this haze-web build")
			}
		}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(capabilities)
	}

	if envFile := os.Getenv("HAZE_ENV_FILE"); envFile != "" {
		if err := webgateway.LoadDotEnv(envFile); err != nil {
			return err
		}
	} else {
		if err := webgateway.LoadDotEnv(filepath.Join(filepath.Dir(*configPath), ".env")); err != nil {
			return err
		}
		if err := webgateway.LoadDotEnv(".env"); err != nil {
			return err
		}
	}
	pprofServer := startPprofServer()

	config, err := webgateway.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	if pattern := strings.TrimSpace(*resetPassword); pattern != "" {
		if strings.TrimSpace(*unlockAccount) != "" {
			return fmt.Errorf("--reset-password and --unlock-account cannot be combined")
		}
		auth := webgateway.NewAuthManagerWithPath(config, *configPath)
		defer auth.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		resets, err := auth.ResetAccountPasswords(ctx, pattern)
		for _, reset := range resets {
			fmt.Printf("haze-web: account %q new password: %s\n", reset.Username, reset.Password)
		}
		if err != nil {
			return fmt.Errorf("reset passwords matching %q: %w", pattern, err)
		}
		log.Printf("haze-web: reset %d account password(s) and revoked all sessions for them", len(resets))
		return nil
	}
	if username := strings.TrimSpace(*unlockAccount); username != "" {
		auth := webgateway.NewAuthManagerWithPath(config, *configPath)
		defer auth.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := auth.RecoverLockedAccount(ctx, username); err != nil {
			return fmt.Errorf("unlock account %q: %w", username, err)
		}
		log.Printf("haze-web: account %q unlocked and all sessions revoked", username)
		return nil
	}
	tlsRuntime, err := webgateway.NewTLSRuntime(config, *configPath)
	if err != nil {
		return err
	}

	webServer := webgateway.NewServerWithSurface(config, *configPath, *webroot, *surface)
	defer webServer.Close()
	handler := webServer.Handler()
	server := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig:         tlsRuntime.TLSConfig(),
	}
	if configureTLSProtocols(server) {
		log.Printf("haze-web TLS HTTP/2 disabled by HAZE_WEB_DISABLE_HTTP2")
	}
	normalizedSurface := webgateway.NormalizeSurface(*surface)
	var publicServers []*http.Server
	if publicPortEnabled(config, normalizedSurface) {
		publicServers = startPublicPortServers(config, handler, tlsRuntime)
	}
	publicPortServingACME := len(publicServers) > 0 && publicPortHTTPHandlesACME(config, tlsRuntime)
	var challengeServer *http.Server
	if tlsRuntime.HTTPChallengeEnabled(normalizedSurface) && !publicPortServingACME {
		challengeServer = &http.Server{
			Addr:              tlsRuntime.HTTPChallengeAddr,
			Handler:           tlsRuntime.HTTPChallengeHandler(server.Addr),
			ReadHeaderTimeout: 10 * time.Second,
		}
		go func() {
			log.Printf("haze-web acme challenge listening on %s", challengeServer.Addr)
			if err := challengeServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("acme challenge listener failed: %v", err)
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = processguard.WithParent(ctx)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown failed: %v", err)
		}
		if challengeServer != nil {
			if err := challengeServer.Shutdown(shutdownCtx); err != nil {
				log.Printf("acme challenge shutdown failed: %v", err)
			}
		}
		for _, publicServer := range publicServers {
			if err := publicServer.Shutdown(shutdownCtx); err != nil {
				log.Printf("public web listener shutdown failed: %v", err)
			}
		}
		if pprofServer != nil {
			if err := pprofServer.Shutdown(shutdownCtx); err != nil {
				log.Printf("pprof shutdown failed: %v", err)
			}
		}
	}()

	if tlsRuntime.Enabled {
		log.Printf("haze-web %s HTTPS listening on %s", *surface, *addr)
		return tlsRuntime.ListenAndServeTLS(server)
	}
	log.Printf("haze-web %s HTTP listening on %s", *surface, *addr)
	return server.ListenAndServe()
}

func publicPortEnabled(config webgateway.Config, surface webgateway.WebSurface) bool {
	return config.Webpanel.PublicPort.Enabled && surface != webgateway.SurfaceAdmin
}

func publicPortHTTPHandlesACME(config webgateway.Config, tlsRuntime *webgateway.TLSRuntime) bool {
	return config.Webpanel.PublicPort.Enabled &&
		tlsRuntime != nil &&
		tlsRuntime.Enabled &&
		tlsRuntime.Mode == "acme" &&
		tlsRuntime.Manager != nil
}

func startPublicPortServers(config webgateway.Config, handler http.Handler, tlsRuntime *webgateway.TLSRuntime) []*http.Server {
	host := strings.TrimSpace(config.Webpanel.PublicPort.Host)
	if host == "" {
		host = "0.0.0.0"
	}
	httpPort := config.Webpanel.PublicPort.HTTPPort
	if httpPort <= 0 {
		httpPort = 80
	}
	httpsPort := config.Webpanel.PublicPort.HTTPSPort
	if httpsPort <= 0 {
		httpsPort = 443
	}

	servers := []*http.Server{}
	if tlsRuntime != nil && tlsRuntime.Enabled {
		httpsAddr := net.JoinHostPort(host, strconv.Itoa(httpsPort.Int()))
		httpsServer := &http.Server{
			Addr:              httpsAddr,
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			TLSConfig:         tlsRuntime.TLSConfig(),
		}
		configureTLSProtocols(httpsServer)
		servers = append(servers, httpsServer)
		go servePublicHTTPS(httpsServer, tlsRuntime)

		if tlsRuntime.RedirectHTTP || publicPortHTTPHandlesACME(config, tlsRuntime) {
			httpServer := &http.Server{
				Addr:              net.JoinHostPort(host, strconv.Itoa(httpPort.Int())),
				Handler:           tlsRuntime.HTTPChallengeHandler(httpsAddr),
				ReadHeaderTimeout: 10 * time.Second,
			}
			servers = append(servers, httpServer)
			go servePublicHTTP(httpServer)
		}
		return servers
	}

	httpServer := &http.Server{
		Addr:              net.JoinHostPort(host, strconv.Itoa(httpPort.Int())),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	servers = append(servers, httpServer)
	go servePublicHTTP(httpServer)
	return servers
}

func configureTLSProtocols(server *http.Server) bool {
	if server == nil || !environmentFlagEnabled("HAZE_WEB_DISABLE_HTTP2") {
		return false
	}
	server.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){}
	return true
}

func environmentFlagEnabled(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func servePublicHTTP(server *http.Server) {
	log.Printf("haze-web public HTTP listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("public HTTP listener failed: %v", err)
	}
}

func servePublicHTTPS(server *http.Server, tlsRuntime *webgateway.TLSRuntime) {
	log.Printf("haze-web public HTTPS listening on %s", server.Addr)
	err := tlsRuntime.ListenAndServeTLS(server)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("public HTTPS listener failed: %v", err)
	}
}

func startPprofServer() *http.Server {
	if os.Getenv("HAZE_WEB_PPROF") == "" && os.Getenv("HAZE_WEB_PPROF_ADDR") == "" {
		return nil
	}
	addr := os.Getenv("HAZE_WEB_PPROF_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6060"
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("haze-web pprof listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("pprof listener failed: %v", err)
		}
	}()
	return server
}
