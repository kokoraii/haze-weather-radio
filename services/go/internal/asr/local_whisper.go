package asr

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxWhisperResponseBytes = 64 * 1024
	minWhisperModelBytes    = 1024 * 1024
	maxWhisperModelBytes    = int64(10 * 1024 * 1024 * 1024)
)

type localWhisperTranscriber struct {
	parent      context.Context
	cfg         Config
	client      *http.Client
	transport   *http.Transport
	restartGate chan struct{}

	mu     sync.Mutex
	proc   *whisperProcess
	closed bool
}

type whisperProcess struct {
	baseURL       string
	requestPrefix string
	cancel        context.CancelFunc
	command       *exec.Cmd
	done          chan struct{}
	publicDir     string
	closeOnce     sync.Once
}

func newLocalWhisperTranscriber(ctx context.Context, cfg Config) (*localWhisperTranscriber, error) {
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:      false,
		DisableCompression:     true,
		MaxConnsPerHost:        1,
		MaxIdleConnsPerHost:    1,
		IdleConnTimeout:        30 * time.Second,
		ResponseHeaderTimeout:  defaultTimeout,
		MaxResponseHeaderBytes: 8 * 1024,
	}
	transcriber := &localWhisperTranscriber{
		parent:      ctx,
		cfg:         cfg,
		transport:   transport,
		restartGate: make(chan struct{}, 1),
	}
	transcriber.restartGate <- struct{}{}
	transcriber.client = &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("local Whisper redirects are disabled")
		},
	}
	startupCtx, cancel := context.WithTimeout(ctx, cfg.RuntimeStartupTimeout)
	defer cancel()
	if _, err := transcriber.ensureProcess(startupCtx); err != nil {
		transcriber.Close()
		return nil, err
	}
	return transcriber, nil
}

func (t *localWhisperTranscriber) Transcribe(ctx context.Context, audioPath string, language string, prompt string) (string, error) {
	for attempt := 0; attempt < 2; attempt++ {
		process, err := t.ensureProcess(ctx)
		if err != nil {
			return "", err
		}
		text, err := t.transcribeOnce(ctx, process, audioPath, language, prompt)
		if err == nil {
			return text, nil
		}
		var failure *providerFailure
		if !errors.As(err, &failure) || !failure.restartRuntime || ctx.Err() != nil || attempt > 0 {
			return "", err
		}
		t.discardProcess(process)
	}
	return "", &providerFailure{code: "provider_unavailable", retryable: true}
}

func (t *localWhisperTranscriber) transcribeOnce(ctx context.Context, process *whisperProcess, audioPath string, language string, prompt string) (string, error) {
	body, contentType, err := whisperMultipartBody(audioPath, language, prompt, t.cfg.MaxAudioBytes)
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, process.inferenceURL(), bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create local Whisper request: %w", err)
	}
	request.Header.Set("Content-Type", contentType)
	request.Header.Set("Accept", "application/json")
	request.ContentLength = int64(len(body))
	response, err := t.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", &providerFailure{
			code: "provider_unavailable", retryable: true, restartRuntime: true,
			cause: errors.New("local runtime connection failed"),
		}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxWhisperResponseBytes+1))
	if err != nil {
		return "", &providerFailure{code: "provider_error", cause: errors.New("read local runtime response")}
	}
	if len(raw) > maxWhisperResponseBytes {
		return "", &providerFailure{code: "provider_error", cause: errors.New("local runtime response is too large")}
	}
	if response.StatusCode != http.StatusOK {
		return "", classifyWhisperHTTPStatus(response.StatusCode)
	}
	var decoded struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return "", &providerFailure{code: "provider_error", cause: errors.New("local runtime returned malformed JSON")}
	}
	return strings.TrimSpace(decoded.Text), nil
}

func classifyWhisperHTTPStatus(status int) error {
	switch status {
	case http.StatusTooManyRequests:
		return &providerFailure{code: "busy", retryable: true}
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return &providerFailure{code: "timeout", retryable: true}
	case http.StatusServiceUnavailable, http.StatusBadGateway:
		return &providerFailure{code: "provider_unavailable", retryable: true}
	default:
		if status >= http.StatusInternalServerError {
			return &providerFailure{code: "provider_unavailable", retryable: true}
		}
		return &providerFailure{code: "provider_error"}
	}
}

func whisperMultipartBody(audioPath string, language string, prompt string, maxAudioBytes int64) ([]byte, string, error) {
	file, err := os.Open(audioPath)
	if err != nil {
		return nil, "", fmt.Errorf("open local Whisper audio: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, "", fmt.Errorf("stat local Whisper audio: %w", err)
	}
	if maxAudioBytes <= 0 {
		maxAudioBytes = defaultMaxAudioBytes
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxAudioBytes {
		return nil, "", &providerFailure{code: "provider_error", cause: errors.New("local Whisper audio is invalid")}
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "audio.wav")
	if err != nil {
		return nil, "", fmt.Errorf("create local Whisper audio part: %w", err)
	}
	if _, err := io.Copy(part, io.LimitReader(file, maxAudioBytes+1)); err != nil {
		return nil, "", fmt.Errorf("write local Whisper audio part: %w", err)
	}
	fields := []struct {
		name  string
		value string
	}{
		{name: "language", value: primaryLanguage(language)},
		{name: "prompt", value: strings.TrimSpace(prompt)},
		{name: "response_format", value: "json"},
		{name: "temperature", value: "0.0"},
		{name: "temperature_inc", value: "0.0"},
		{name: "no_timestamps", value: "true"},
		{name: "token_timestamps", value: "false"},
		{name: "suppress_nst", value: "true"},
	}
	if fields[0].value == "" {
		fields[0].value = "auto"
	}
	for _, field := range fields {
		if err := writer.WriteField(field.name, field.value); err != nil {
			return nil, "", fmt.Errorf("write local Whisper field %s: %w", field.name, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("finish local Whisper request: %w", err)
	}
	return body.Bytes(), writer.FormDataContentType(), nil
}

func (t *localWhisperTranscriber) ensureProcess(ctx context.Context) (*whisperProcess, error) {
	if process := t.liveProcess(); process != nil {
		return process, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.restartGate:
	}
	defer func() { t.restartGate <- struct{}{} }()
	if process := t.liveProcess(); process != nil {
		return process, nil
	}

	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil, &providerFailure{code: "provider_unavailable", retryable: false, cause: errors.New("local runtime is closed")}
	}
	startupCtx, cancel := context.WithTimeout(ctx, t.cfg.RuntimeStartupTimeout)
	defer cancel()
	process, err := startWhisperProcess(t.parent, startupCtx, t.cfg)
	if err != nil {
		return nil, &providerFailure{code: "provider_unavailable", retryable: true, cause: err}
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		process.close()
		return nil, &providerFailure{code: "provider_unavailable", retryable: false, cause: errors.New("local runtime is closed")}
	}
	t.proc = process
	t.mu.Unlock()
	return process, nil
}

func (t *localWhisperTranscriber) liveProcess() *whisperProcess {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.proc == nil || t.proc.exited() {
		return nil
	}
	return t.proc
}

func (t *localWhisperTranscriber) discardProcess(process *whisperProcess) {
	t.mu.Lock()
	if t.proc == process {
		t.proc = nil
	} else {
		process = nil
	}
	t.mu.Unlock()
	if process != nil {
		process.close()
	}
}

// Close terminates the private local Whisper runtime and closes idle HTTP connections.
func (t *localWhisperTranscriber) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	process := t.proc
	t.proc = nil
	t.mu.Unlock()
	if process != nil {
		process.close()
	}
	t.transport.CloseIdleConnections()
}

func startWhisperProcess(parent context.Context, startupCtx context.Context, cfg Config) (*whisperProcess, error) {
	if err := validateLocalRuntimeInputs(startupCtx, cfg); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("reserve local Whisper port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return nil, fmt.Errorf("release local Whisper port: %w", err)
	}
	randomBytes := make([]byte, 18)
	if _, err := rand.Read(randomBytes); err != nil {
		return nil, fmt.Errorf("create local Whisper request token: %w", err)
	}
	requestPrefix := "/haze-" + hex.EncodeToString(randomBytes)
	publicDir, err := os.MkdirTemp("", "haze-whisper-public-")
	if err != nil {
		return nil, fmt.Errorf("create local Whisper private directory: %w", err)
	}
	if err := os.Chmod(publicDir, 0o700); err != nil && !errors.Is(err, os.ErrPermission) {
		_ = os.RemoveAll(publicDir)
		return nil, fmt.Errorf("protect local Whisper private directory: %w", err)
	}

	processCtx, cancel := context.WithCancel(parent)
	args := whisperRuntimeArgs(cfg, port, requestPrefix, publicDir)
	command := exec.CommandContext(processCtx, cfg.RuntimeExecutable, args...)
	command.Dir = publicDir
	command.Env = whisperRuntimeEnvironment()
	command.Stdin = nil
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	configureWhisperChild(command)
	if err := command.Start(); err != nil {
		cancel()
		_ = os.RemoveAll(publicDir)
		return nil, fmt.Errorf("start local Whisper runtime: %w", err)
	}
	process := &whisperProcess{
		baseURL:       "http://127.0.0.1:" + strconv.Itoa(port),
		requestPrefix: requestPrefix,
		cancel:        cancel,
		command:       command,
		done:          make(chan struct{}),
		publicDir:     publicDir,
	}
	go func() {
		_ = command.Wait()
		_ = os.RemoveAll(publicDir)
		close(process.done)
	}()
	if err := waitForWhisperHealth(startupCtx, process); err != nil {
		process.close()
		return nil, err
	}
	return process, nil
}

func whisperRuntimeArgs(cfg Config, port int, requestPrefix string, publicDir string) []string {
	args := []string{
		"--model", cfg.ModelPath,
		"--host", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--public", publicDir,
		"--request-path", requestPrefix,
		"--inference-path", "/inference",
		"--threads", strconv.Itoa(cfg.RuntimeThreads),
		"--processors", "1",
		"--language", "auto",
		"--no-timestamps",
		"--suppress-nst",
	}
	if !cfg.RuntimeUseGPU {
		args = append(args, "--no-gpu")
	}
	return args
}

func whisperRuntimeEnvironment() []string {
	allowed := map[string]bool{
		"PATH": true, "LD_LIBRARY_PATH": true, "DYLD_LIBRARY_PATH": true,
		"SYSTEMROOT": true, "WINDIR": true, "TMP": true, "TEMP": true, "TMPDIR": true,
		"LANG": true, "CUDA_VISIBLE_DEVICES": true,
	}
	result := make([]string, 0, 12)
	for _, item := range os.Environ() {
		key, _, ok := strings.Cut(item, "=")
		upper := strings.ToUpper(strings.TrimSpace(key))
		if !ok || (!allowed[upper] && !strings.HasPrefix(upper, "LC_")) {
			continue
		}
		result = append(result, item)
	}
	return result
}

func validateLocalRuntimeInputs(ctx context.Context, cfg Config) error {
	if err := validateRuntimeFile(cfg.RuntimeExecutable, "runtime executable", 1, 1024*1024*1024, true); err != nil {
		return err
	}
	if err := validateRuntimeFile(cfg.ModelPath, "model", minWhisperModelBytes, maxWhisperModelBytes, false); err != nil {
		return err
	}
	if cfg.ModelSHA256 == "" {
		return nil
	}
	actual, err := fileSHA256(ctx, cfg.ModelPath)
	if err != nil {
		return fmt.Errorf("verify local Whisper model: %w", err)
	}
	if actual != cfg.ModelSHA256 {
		return errors.New("local Whisper model checksum does not match configuration")
	}
	return nil
}

func validateRuntimeFile(path string, label string, minBytes int64, maxBytes int64, executable bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("local Whisper %s is unavailable: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() < minBytes || info.Size() > maxBytes {
		return fmt.Errorf("local Whisper %s must be a regular file with a valid size", label)
	}
	if executable && runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		return errors.New("local Whisper runtime is not executable")
	}
	return nil
}

func fileSHA256(ctx context.Context, path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	buffer := make([]byte, 1024*1024)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		count, readErr := file.Read(buffer)
		if count > 0 {
			if _, err := hash.Write(buffer[:count]); err != nil {
				return "", err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return "", readErr
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func waitForWhisperHealth(ctx context.Context, process *whisperProcess) error {
	client := &http.Client{
		Transport: &http.Transport{
			Proxy:                  nil,
			DialContext:            (&net.Dialer{Timeout: 250 * time.Millisecond}).DialContext,
			DisableCompression:     true,
			ForceAttemptHTTP2:      false,
			MaxResponseHeaderBytes: 8 * 1024,
		},
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("local Whisper redirects are disabled")
		},
	}
	defer client.CloseIdleConnections()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("local Whisper startup: %w", ctx.Err())
		case <-process.done:
			return errors.New("local Whisper runtime exited during startup")
		default:
		}
		requestCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, process.healthURL(), nil)
		if err == nil {
			response, requestErr := client.Do(request)
			if requestErr == nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					cancel()
					return nil
				}
			}
		}
		cancel()
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("local Whisper startup: %w", ctx.Err())
		case <-process.done:
			timer.Stop()
			return errors.New("local Whisper runtime exited during startup")
		case <-timer.C:
		}
	}
}

func (p *whisperProcess) inferenceURL() string {
	return p.baseURL + p.requestPrefix + "/inference"
}

func (p *whisperProcess) healthURL() string {
	return p.baseURL + p.requestPrefix + "/health"
}

func (p *whisperProcess) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *whisperProcess) close() {
	p.closeOnce.Do(func() {
		p.cancel()
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		select {
		case <-p.done:
			return
		case <-timer.C:
			if p.command.Process != nil {
				_ = p.command.Process.Kill()
			}
			<-p.done
		}
	})
}

func runtimeExecutableName() string {
	if runtime.GOOS == "windows" {
		return "whisper-server.exe"
	}
	return "whisper-server"
}
