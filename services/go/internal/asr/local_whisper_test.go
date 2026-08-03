package asr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalWhisperTranscriberSendsOnlyAudioAndHints(t *testing.T) {
	t.Parallel()
	fields := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/private/inference" {
			t.Errorf("unexpected request path %q", request.URL.Path)
		}
		if request.Header.Get("Authorization") != "" {
			t.Error("local request unexpectedly contained authorization")
		}
		reader, err := request.MultipartReader()
		if err != nil {
			t.Errorf("multipart reader: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		for {
			part, err := reader.NextPart()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Errorf("read multipart part: %v", err)
				return
			}
			body, readErr := io.ReadAll(part)
			if readErr != nil {
				t.Errorf("read multipart body: %v", readErr)
				return
			}
			if part.FormName() == "file" {
				if len(body) < 44 || string(body[:4]) != "RIFF" {
					t.Error("file part was not WAV audio")
				}
				fields["filename"] = part.FileName()
			} else {
				fields[part.FormName()] = string(body)
			}
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"text":"Saskatoon"}`))
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "request01.wav")
	writeTestWave(t, path, 2*time.Second)
	transcriber := &localWhisperTranscriber{
		cfg:    Config{MaxAudioBytes: defaultMaxAudioBytes},
		client: server.Client(),
	}
	process := &whisperProcess{baseURL: server.URL, requestPrefix: "/private"}
	text, err := transcriber.transcribeOnce(context.Background(), process, path, "fr-CA", "Une ville en Saskatchewan")
	if err != nil {
		t.Fatalf("transcribe: %v", err)
	}
	if text != "Saskatoon" {
		t.Fatalf("unexpected transcript %q", text)
	}
	for key, want := range map[string]string{
		"language": "fr", "prompt": "Une ville en Saskatchewan", "response_format": "json",
		"no_timestamps": "true", "token_timestamps": "false",
	} {
		if fields[key] != want {
			t.Errorf("multipart field %s = %q, want %q", key, fields[key], want)
		}
	}
	if fields["filename"] != "audio.wav" {
		t.Errorf("multipart filename exposed request identity: %q", fields["filename"])
	}
	if fields["model"] != "" {
		t.Errorf("model path was sent through the request: %q", fields["model"])
	}
}

func TestLocalWhisperErrorClassification(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		status    int
		wantCode  string
		wantRetry bool
	}{
		{name: "busy", status: http.StatusTooManyRequests, wantCode: "busy", wantRetry: true},
		{name: "timeout", status: http.StatusGatewayTimeout, wantCode: "timeout", wantRetry: true},
		{name: "unavailable", status: http.StatusServiceUnavailable, wantCode: "provider_unavailable", wantRetry: true},
		{name: "malformed request", status: http.StatusBadRequest, wantCode: "provider_error"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			code, retryable := classifyProviderError(classifyWhisperHTTPStatus(test.status))
			if code != test.wantCode || retryable != test.wantRetry {
				t.Fatalf("classification = (%q, %t), want (%q, %t)", code, retryable, test.wantCode, test.wantRetry)
			}
		})
	}
}

func TestLocalWhisperTranscriberHonorsCancellation(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"text":"Regina"}`))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "request02.wav")
	writeTestWave(t, path, 2*time.Second)
	transcriber := &localWhisperTranscriber{cfg: Config{MaxAudioBytes: defaultMaxAudioBytes}, client: server.Client()}
	process := &whisperProcess{baseURL: server.URL}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := transcriber.transcribeOnce(ctx, process, path, "en", "")
	if err == nil {
		t.Fatal("expected timeout")
	}
	code, retryable := classifyProviderError(err)
	if code != "timeout" || !retryable {
		t.Fatalf("timeout classification = (%q, %t): %v", code, retryable, err)
	}
}

func TestLocalWhisperTranscriberRejectsMalformedResponseWithoutLeakingIt(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"text":"private transcript"`))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "request03.wav")
	writeTestWave(t, path, 2*time.Second)
	transcriber := &localWhisperTranscriber{cfg: Config{MaxAudioBytes: defaultMaxAudioBytes}, client: server.Client()}
	process := &whisperProcess{baseURL: server.URL}
	_, err := transcriber.transcribeOnce(context.Background(), process, path, "en-CA", "")
	if err == nil {
		t.Fatal("malformed local response was accepted")
	}
	if strings.Contains(err.Error(), "private transcript") {
		t.Fatalf("provider response leaked through error: %v", err)
	}
}

func TestWhisperRuntimeArgumentsStayPrivateAndOffline(t *testing.T) {
	t.Parallel()
	cfg := Config{ModelPath: filepath.Join("runtime", "models", "whisper.bin"), RuntimeThreads: 3}
	args := whisperRuntimeArgs(cfg, 43123, "/haze-secret", filepath.Join("runtime", "private"))
	joined := strings.Join(args, " ")
	for _, required := range []string{"--host 127.0.0.1", "--port 43123", "--request-path /haze-secret", "--no-gpu", "--no-timestamps", "--threads 3"} {
		if !strings.Contains(joined, required) {
			t.Errorf("runtime arguments missing %q: %s", required, joined)
		}
	}
	for _, forbidden := range []string{"api.openai.com", "--convert", "--no-flash-attn", "OPENAI_API_KEY"} {
		if strings.Contains(joined, forbidden) {
			t.Errorf("runtime arguments unexpectedly contain %q", forbidden)
		}
	}
}

func TestWhisperRuntimeEnvironmentExcludesHazeSecrets(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "must-not-leak")
	t.Setenv("HAZE_TWILIO_AUTH_TOKEN", "must-not-leak")
	joined := strings.Join(whisperRuntimeEnvironment(), "\n")
	if strings.Contains(joined, "must-not-leak") || strings.Contains(joined, "OPENAI_API_KEY") || strings.Contains(joined, "HAZE_TWILIO_AUTH_TOKEN") {
		t.Fatalf("private environment leaked to local runtime: %s", joined)
	}
}

func TestValidateLocalRuntimeInputsChecksPinnedModelDigest(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	executable := filepath.Join(dir, runtimeExecutableName())
	if err := os.WriteFile(executable, []byte("runtime"), 0o700); err != nil {
		t.Fatal(err)
	}
	model := filepath.Join(dir, "ggml-test.bin")
	modelBytes := make([]byte, minWhisperModelBytes)
	for index := range modelBytes {
		modelBytes[index] = byte(index)
	}
	if err := os.WriteFile(model, modelBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(modelBytes)
	cfg := Config{
		RuntimeExecutable: executable,
		ModelPath:         model,
		ModelSHA256:       hex.EncodeToString(digest[:]),
	}
	if err := validateLocalRuntimeInputs(context.Background(), cfg); err != nil {
		t.Fatalf("valid local runtime inputs failed: %v", err)
	}
	cfg.ModelSHA256 = strings.Repeat("0", 64)
	if err := validateLocalRuntimeInputs(context.Background(), cfg); err == nil {
		t.Fatal("mismatched model digest was accepted")
	}
}

func TestPrimaryLanguageRejectsNonISOHint(t *testing.T) {
	t.Parallel()
	if got := primaryLanguage("English"); got != "" {
		t.Fatalf("unexpected non-ISO language %q", got)
	}
	if got := primaryLanguage("EN_ca"); got != "en" {
		t.Fatalf("unexpected primary language %q", got)
	}
	if strings.TrimSpace(primaryLanguage("")) != "" {
		t.Fatal("blank language was not preserved")
	}
}

func writeTestWave(t *testing.T, path string, duration time.Duration) {
	t.Helper()
	samples := int(16000 * duration / time.Second)
	dataBytes := samples * 2
	header := make([]byte, 44+dataBytes)
	copy(header[0:4], "RIFF")
	putUint32(header[4:8], uint32(36+dataBytes))
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	putUint32(header[16:20], 16)
	putUint16(header[20:22], 1)
	putUint16(header[22:24], 1)
	putUint32(header[24:28], 16000)
	putUint32(header[28:32], 32000)
	putUint16(header[32:34], 2)
	putUint16(header[34:36], 16)
	copy(header[36:40], "data")
	putUint32(header[40:44], uint32(dataBytes))
	if err := os.WriteFile(path, header, 0o600); err != nil {
		t.Fatalf("write test WAV: %v", err)
	}
}

func putUint16(target []byte, value uint16) {
	target[0] = byte(value)
	target[1] = byte(value >> 8)
}

func putUint32(target []byte, value uint32) {
	target[0] = byte(value)
	target[1] = byte(value >> 8)
	target[2] = byte(value >> 16)
	target[3] = byte(value >> 24)
}
