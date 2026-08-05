package tts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const speakyAPIProviderID = "speakyapi"

const speakyAPIConnectTimeout = 4 * time.Second

// SpeakyAPIProvider talks to an external SpeakyAPI server. SpeakyAPI exposes
// Windows SAPI/Maki voices through HTTP for hosts that cannot run SAPI locally.
type SpeakyAPIProvider struct {
	BaseURL string
	Client  *http.Client
}

// NewSpeakyAPIProvider creates a SpeakyAPI provider. If baseURL is empty,
// HAZE_SPEAKYAPI_URL is used.
func NewSpeakyAPIProvider(baseURL string) *SpeakyAPIProvider {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = os.Getenv("HAZE_SPEAKYAPI_URL")
	}
	return &SpeakyAPIProvider{
		BaseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		Client:  newSpeakyAPIHTTPClient(),
	}
}

func newSpeakyAPIHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   speakyAPIConnectTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	return &http.Client{Transport: transport, Timeout: 60 * time.Second}
}

func (p *SpeakyAPIProvider) ID() string { return speakyAPIProviderID }

func (p *SpeakyAPIProvider) ListVoices(ctx context.Context) ([]Voice, error) {
	base, err := p.endpoint("/voices")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: speakyapi voices request failed: %w", ErrProviderUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w: speakyapi voices returned HTTP %d", ErrProviderUnavailable, resp.StatusCode)
	}
	var names []string
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&names); err != nil {
		return nil, fmt.Errorf("%w: speakyapi voices response was not JSON: %w", ErrProviderUnavailable, err)
	}
	voices := make([]Voice, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		voices = append(voices, Voice{
			ID:       name,
			Name:     name,
			Provider: p.ID(),
		})
	}
	return voices, nil
}

func (p *SpeakyAPIProvider) Synthesize(ctx context.Context, req Request) (Audio, error) {
	if strings.TrimSpace(req.Text) == "" {
		return Audio{}, fmt.Errorf("empty synthesis text")
	}
	req.Text = prepareSAPIText(req.Text)
	base, err := p.endpoint("/tts")
	if err != nil {
		return Audio{}, err
	}
	body, err := json.Marshal(map[string]string{
		"text":       req.Text,
		"voice_name": strings.TrimSpace(req.VoiceID),
	})
	if err != nil {
		return Audio{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base, bytes.NewReader(body))
	if err != nil {
		return Audio{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "audio/wav, audio/x-wav, audio/wave, application/octet-stream")
	resp, err := p.httpClient().Do(httpReq)
	if err != nil {
		return Audio{}, fmt.Errorf("%w: speakyapi tts request failed: %w", ErrProviderUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Audio{}, fmt.Errorf("%w: speakyapi tts returned HTTP %d: %s", ErrProviderUnavailable, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return Audio{}, fmt.Errorf("%w: failed reading speakyapi wav: %w", ErrProviderUnavailable, err)
	}
	if len(data) < 12 || string(data[:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return Audio{}, fmt.Errorf("%w: speakyapi returned non-WAV audio", ErrProviderUnavailable)
	}
	return Audio{Format: FormatWAV, Data: data}, nil
}

func (p *SpeakyAPIProvider) endpoint(path string) (string, error) {
	if strings.TrimSpace(p.BaseURL) == "" {
		return "", fmt.Errorf("%w: speakyapi base URL is not configured", ErrProviderUnavailable)
	}
	parsed, err := url.Parse(p.BaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("%w: invalid speakyapi base URL %q", ErrProviderUnavailable, p.BaseURL)
	}
	return strings.TrimRight(parsed.String(), "/") + path, nil
}

func (p *SpeakyAPIProvider) httpClient() *http.Client {
	if p.Client != nil {
		return p.Client
	}
	return http.DefaultClient
}
