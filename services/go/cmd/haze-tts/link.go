package main

// Remote mode runs haze-tts on another machine, such as a Windows host with
// native SAPI5 voices. With --listen the service waits for the Haze daemon to
// dial in instead of dialing a local host bridge:
//
//  1. The daemon sends link.hello with HAZE_REMOTE_TOKEN and the settings it
//     would launch a local haze-tts with (readers.xml, dictionary.json,
//     provider, language, timezone, timeout).
//  2. The service answers link.ready, or link.refused and closes. It then
//     registers like a bridge client and runs the normal service loop.
//  3. Audio returns inline as output_b64 on tts.synthesized, and the daemon
//     writes it to the output_path it asked for.
//
// The hello is lockstep: the daemon sends nothing else until link.ready, so
// the hello line can be read before the service loop takes the connection.
// One daemon link is served at a time. A newer authenticated link replaces it.
// See docs/remote-services.md.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/bridgeclient"
	"github.com/meowraii/haze-weather-radio/services/go/internal/dotenv"
	"github.com/meowraii/haze-weather-radio/services/go/internal/events"
	"github.com/meowraii/haze-weather-radio/services/go/internal/tts"
)

const (
	remoteTokenEnv       = "HAZE_REMOTE_TOKEN"
	linkProtocol         = 1
	linkHandshakeTimeout = 10 * time.Second
	// linkMaxLineBytes bounds one line from the daemon. The hello carries the
	// readers and dictionary files.
	linkMaxLineBytes = 16 * 1024 * 1024
)

type linkHello struct {
	Type string        `json:"type"`
	Data linkHelloData `json:"data"`
}

type linkHelloData struct {
	Protocol int          `json:"protocol"`
	Token    string       `json:"token"`
	Settings linkSettings `json:"settings"`
}

// linkSettings mirrors the launch flags a local haze-tts gets from the daemon.
type linkSettings struct {
	ReadersXML     *string `json:"readers_xml"`
	DictionaryJSON string  `json:"dictionary_json"`
	Provider       string  `json:"provider"`
	Language       string  `json:"language"`
	Timezone       string  `json:"timezone"`
	Timeout        string  `json:"timeout"`
}

// runListener serves remote Haze daemon links on addr until ctx ends.
func runListener(ctx context.Context, cfg serviceConfig, addr string) error {
	loadListenerEnv()
	token := strings.TrimSpace(os.Getenv(remoteTokenEnv))
	if token == "" {
		return fmt.Errorf("%s is not set: add it to the .env next to haze-tts with the same value as the Haze daemon's .env", remoteTokenEnv)
	}
	cfg, err := prepareServiceConfig(cfg)
	if err != nil {
		return err
	}
	providers := tts.DefaultProviders()
	configureSpeakyAPIProvider(providers, cfg.SpeakyAPIURL)
	stopPruner := startRuntimePruner(ctx, providers, cfg.RuntimeIdle)
	defer stopPruner()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen for Haze on %s: %w", addr, err)
	}
	log.Printf("waiting for a Haze daemon on %s", listener.Addr())
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	links := &linkSlot{}
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		raw, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("accept failed: %v", err)
			sleepOrDone(ctx, time.Second)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			serveLink(ctx, raw, cfg, providers, token, links)
		}()
	}
}

// loadListenerEnv loads the .env next to the executable, where a standalone
// bundle keeps HAZE_REMOTE_TOKEN.
func loadListenerEnv() {
	exe, err := os.Executable()
	if err != nil {
		log.Printf("WARN cannot locate the haze-tts executable for .env: %v", err)
		return
	}
	if err := dotenv.Load(filepath.Join(filepath.Dir(exe), ".env")); err != nil {
		log.Printf("WARN cannot load .env: %v", err)
	}
}

func serveLink(ctx context.Context, raw net.Conn, cfg serviceConfig, providers map[string]tts.Provider, token string, links *linkSlot) {
	defer raw.Close()
	peer := raw.RemoteAddr().String()
	conn := bridgeclient.Wrap(raw, bridgeclient.Options{MaxLineBytes: linkMaxLineBytes})
	settings, err := readLinkHello(conn, raw, token)
	if err != nil {
		log.Printf("refused Haze link from %s: %v", peer, err)
		_ = publishLinkEvent(conn, "link.refused", map[string]any{"error": err.Error()})
		return
	}

	linkCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	id := links.replace(cancel)
	defer links.release(id)
	// Closing the connection ends the service loop when this link is replaced
	// or the listener shuts down.
	go func() {
		<-linkCtx.Done()
		_ = raw.Close()
	}()

	state := newRemoteServiceState(linkCtx, cfg, providers, settings)
	if err := publishLinkEvent(conn, "link.ready", map[string]any{"service": serviceID, "protocol": linkProtocol}); err != nil {
		return
	}
	if err := conn.Register(serviceRegistration); err != nil {
		return
	}
	log.Printf("Haze daemon connected from %s", peer)
	_ = publishServiceEvent(conn, "service.ready", "", map[string]any{
		"service":   serviceID,
		"providers": providerIDs(providers),
		"readers":   state.cfg.Readers,
	})
	err = runServiceConnection(linkCtx, conn, state)
	if err != nil && !errors.Is(err, errSystemShutdown) && linkCtx.Err() == nil {
		log.Printf("Haze link from %s closed: %v", peer, err)
		return
	}
	log.Printf("Haze link from %s closed", peer)
}

// readLinkHello reads and authenticates the daemon's first line.
func readLinkHello(conn *bridgeclient.Conn, raw net.Conn, token string) (linkSettings, error) {
	if err := raw.SetDeadline(time.Now().Add(linkHandshakeTimeout)); err != nil {
		return linkSettings{}, err
	}
	var line []byte
	scanErr := conn.Scan(func(raw []byte) bool {
		line = append([]byte(nil), raw...)
		return false
	})
	if err := raw.SetDeadline(time.Time{}); err != nil {
		return linkSettings{}, err
	}
	if scanErr != nil {
		return linkSettings{}, fmt.Errorf("read link.hello: %w", scanErr)
	}
	if line == nil {
		return linkSettings{}, errors.New("connection closed before link.hello")
	}
	var hello linkHello
	if err := json.Unmarshal(bytes.TrimSpace(line), &hello); err != nil || hello.Type != "link.hello" {
		return linkSettings{}, errors.New("expected link.hello")
	}
	if hello.Data.Protocol != linkProtocol {
		return linkSettings{}, fmt.Errorf("unsupported link protocol %d (want %d)", hello.Data.Protocol, linkProtocol)
	}
	if subtle.ConstantTimeCompare([]byte(hello.Data.Token), []byte(token)) != 1 {
		return linkSettings{}, errors.New("invalid token")
	}
	return hello.Data.Settings, nil
}

// newRemoteServiceState builds one link's state from the daemon's pushed
// settings. Provider runtimes are shared with the listener so models stay
// loaded across reconnects.
func newRemoteServiceState(ctx context.Context, cfg serviceConfig, providers map[string]tts.Provider, settings linkSettings) *serviceState {
	cfg.Readers = "readers.xml from Haze"
	cfg.Dictionary = ""
	if provider := strings.TrimSpace(settings.Provider); provider != "" {
		cfg.Provider = provider
	}
	if language := strings.TrimSpace(settings.Language); language != "" {
		cfg.Language = language
	}
	if timezone := strings.TrimSpace(settings.Timezone); timezone != "" {
		cfg.Timezone = timezone
	}
	if timeout, err := time.ParseDuration(strings.TrimSpace(settings.Timeout)); err == nil && timeout > 0 {
		cfg.Timeout = timeout
	}
	var readers []tts.Reader
	readersErr := errors.New("the Haze daemon sent no readers.xml")
	if settings.ReadersXML != nil {
		readers, readersErr = tts.ParseReaders([]byte(*settings.ReadersXML))
		if readersErr != nil {
			log.Printf("readers.xml from Haze is invalid: %v", readersErr)
		}
	}
	return &serviceState{
		ctx:              ctx,
		cfg:              cfg,
		providers:        providers,
		readers:          readers,
		readersErr:       readersErr,
		pushedDictionary: append([]byte{}, settings.DictionaryJSON...),
		inline:           true,
		dictionaries:     map[string]dictionaryResult{},
		synthesisCache:   map[[sha256.Size]byte]synthesisCacheEntry{},
		providerHealth:   map[string]*providerHealthStatus{},
		voiceRoutes:      map[[sha256.Size]byte]voiceRouteDecision{},
	}
}

func publishLinkEvent(conn *bridgeclient.Conn, eventType string, data map[string]any) error {
	return conn.PublishValue(events.Event{
		Type:      eventType,
		Source:    serviceID,
		Timestamp: time.Now().UTC(),
		Data:      data,
	})
}

// linkSlot tracks the one daemon link being served.
type linkSlot struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	id     uint64
}

// replace ends the active link, if any, and makes cancel the active one.
func (s *linkSlot) replace(cancel context.CancelFunc) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	s.id++
	s.cancel = cancel
	return s.id
}

func (s *linkSlot) release(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.id == id {
		s.cancel = nil
	}
}
