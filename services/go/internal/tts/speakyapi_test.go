package tts

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSpeakyAPIProviderUsesBoundedHTTPTransport(t *testing.T) {
	provider := NewSpeakyAPIProvider("http://127.0.0.1:5000")
	if provider.Client == nil {
		t.Fatal("SpeakyAPI HTTP client is nil")
	}
	if provider.Client.Timeout != 60*time.Second {
		t.Fatalf("HTTP timeout = %s", provider.Client.Timeout)
	}
	transport, ok := provider.Client.Transport.(*http.Transport)
	if !ok || transport.DialContext == nil {
		t.Fatalf("SpeakyAPI transport = %#v", provider.Client.Transport)
	}
	if speakyAPIConnectTimeout > 5*time.Second {
		t.Fatalf("connect timeout = %s, want no more than 5s", speakyAPIConnectTimeout)
	}
}

func TestSpeakyAPIProviderListVoices(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/voices" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode([]string{"Microsoft David Desktop", "Microsoft Zira Desktop"})
	}))
	defer server.Close()

	provider := NewSpeakyAPIProvider(server.URL)
	voices, err := provider.ListVoices(context.Background())
	if err != nil {
		t.Fatalf("ListVoices: %v", err)
	}
	if len(voices) != 2 || voices[0].Provider != "speakyapi" || voices[0].ID != "Microsoft David Desktop" {
		t.Fatalf("voices = %#v", voices)
	}
}

func TestSpeakyAPIProviderSynthesizePostsExpectedPayload(t *testing.T) {
	const wav = "RIFF\x24\x00\x00\x00WAVEfmt "
	var got map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tts" || r.Method != http.MethodPost {
			t.Fatalf("%s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("Content-Type = %q", r.Header.Get("Content-Type"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(wav))
	}))
	defer server.Close()

	provider := NewSpeakyAPIProvider(server.URL)
	audio, err := provider.Synthesize(context.Background(), Request{
		Text:    "Hello there.",
		VoiceID: "Microsoft Hazel Desktop",
	})
	if err != nil {
		t.Fatalf("Synthesize: %v", err)
	}
	if audio.Format != FormatWAV || string(audio.Data) != wav {
		t.Fatalf("audio = %#v", audio)
	}
	if got["text"] != "Hello there." || got["voice_name"] != "Microsoft Hazel Desktop" {
		t.Fatalf("request payload = %#v", got)
	}
}

func TestSpeakyAPIProviderSynthesizePreparesSAPIPronunciations(t *testing.T) {
	const wav = "RIFF\x24\x00\x00\x00WAVEfmt "
	var got map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(wav))
	}))
	defer server.Close()

	provider := NewSpeakyAPIProvider(server.URL)
	_, err := provider.Synthesize(context.Background(), Request{
		Text: "Winds increase near windows and windspeed sensors.",
	})
	if err != nil {
		t.Fatalf("Synthesize: %v", err)
	}
	if got["text"] != "windz increase near windows and windspeed sensors." {
		t.Fatalf("request text = %q", got["text"])
	}
}

func TestSpeakyAPIProviderRejectsNonWAV(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not wav"))
	}))
	defer server.Close()

	provider := NewSpeakyAPIProvider(server.URL)
	if _, err := provider.Synthesize(context.Background(), Request{Text: "Hello"}); err == nil {
		t.Fatal("expected non-WAV response to fail")
	}
}
