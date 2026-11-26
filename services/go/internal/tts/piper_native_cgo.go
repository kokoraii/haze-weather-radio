//go:build cgo && ((windows && (amd64 || 386)) || (darwin && (amd64 || arm64)) || (linux && (amd64 || arm64 || arm || 386 || mips || mips64 || mips64le || mipsle)))

package tts

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/v2/sherpa_onnx"
)

type nativePiperEngine struct {
	mu     sync.Mutex
	engine *sherpa.OfflineTts
}

func (p *PiperProvider) synthesizeWithNative(ctx context.Context, voice resolvedPiperVoice, req Request) (Audio, error) {
	if strings.TrimSpace(req.Text) == "" {
		return Audio{}, errors.New("empty synthesis text")
	}
	engine, err := p.nativeEngine(ctx, voice)
	if err != nil {
		return Audio{}, err
	}
	return engine.Synthesize(ctx, req)
}

func (p *PiperProvider) nativeEngine(ctx context.Context, voice resolvedPiperVoice) (piperNativeEngine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := strings.Join([]string{voice.ModelPath, voice.ConfigPath, piperRuntimeProvider(), fmt.Sprint(piperThreads())}, "\x00")
	now := time.Now()
	p.runtimeMu.Lock()
	if entry := p.nativeEngines[key]; entry.engine != nil {
		entry.lastUsed = now
		p.nativeEngines[key] = entry
		p.runtimeMu.Unlock()
		return entry.engine, nil
	}
	p.runtimeMu.Unlock()

	config, err := p.nativePiperConfig(ctx, voice)
	if err != nil {
		return nil, err
	}
	engine := sherpa.NewOfflineTts(&config)
	if engine == nil {
		return nil, fmt.Errorf("%w: failed to initialize native piper runtime", ErrProviderUnavailable)
	}
	handle := &nativePiperEngine{engine: engine}
	runtime.SetFinalizer(handle, func(engine *nativePiperEngine) {
		engine.Close()
	})

	p.runtimeMu.Lock()
	if existing := p.nativeEngines[key]; existing.engine != nil {
		existing.lastUsed = now
		p.nativeEngines[key] = existing
		p.runtimeMu.Unlock()
		handle.Close()
		return existing.engine, nil
	}
	p.nativeEngines[key] = piperNativeEngineEntry{engine: handle, lastUsed: now}
	p.runtimeMu.Unlock()
	return handle, nil
}

func (p *PiperProvider) nativePiperConfig(ctx context.Context, voice resolvedPiperVoice) (sherpa.OfflineTtsConfig, error) {
	voiceConfig, err := loadPiperVoiceConfig(voice.ConfigPath)
	if err != nil {
		return sherpa.OfflineTtsConfig{}, err
	}
	if err := ensurePiperModelMetadata(voice.ModelPath, voiceConfig); err != nil {
		return sherpa.OfflineTtsConfig{}, err
	}
	tokensPath, err := ensurePiperTokensFile(voice.ConfigPath, voiceConfig)
	if err != nil {
		return sherpa.OfflineTtsConfig{}, err
	}
	dataDir, err := p.ensurePiperDataDir(ctx, voice.ModelPath)
	if err != nil {
		return sherpa.OfflineTtsConfig{}, err
	}
	debug := 0
	if piperDebug() {
		debug = 1
	}
	return sherpa.OfflineTtsConfig{
		Model: sherpa.OfflineTtsModelConfig{
			Vits: sherpa.OfflineTtsVitsModelConfig{
				Model:       voice.ModelPath,
				Tokens:      tokensPath,
				DataDir:     dataDir,
				NoiseScale:  voiceConfig.Inference.NoiseScale,
				NoiseScaleW: voiceConfig.Inference.NoiseW,
				LengthScale: voiceConfig.Inference.LengthScale,
			},
			NumThreads: piperThreads(),
			Debug:      debug,
			Provider:   piperRuntimeProvider(),
		},
		MaxNumSentences: 1,
		SilenceScale:    0.2,
	}, nil
}

func (e *nativePiperEngine) Synthesize(ctx context.Context, req Request) (Audio, error) {
	if err := ctx.Err(); err != nil {
		return Audio{}, err
	}
	e.mu.Lock()
	generated := e.engine.GenerateWithConfig(req.Text, &sherpa.GenerationConfig{
		Speed:        piperSpeedForRequest(req.Rate),
		SilenceScale: 0.2,
	}, nil)
	sampleRate := 0
	if generated != nil {
		sampleRate = generated.SampleRate
	}
	if sampleRate <= 0 {
		sampleRate = e.engine.SampleRate()
	}
	e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Audio{}, err
	}
	if generated == nil || len(generated.Samples) == 0 {
		return Audio{}, fmt.Errorf("%w: native piper generated no audio", ErrProviderUnavailable)
	}
	if sampleRate <= 0 {
		return Audio{}, fmt.Errorf("%w: native piper returned invalid sample rate", ErrProviderUnavailable)
	}
	samples := kokoroApplyVolume(generated.Samples, req.Volume)
	if req.OutputFormat == FormatPCM16LE {
		return Audio{Format: FormatPCM16LE, SampleRate: sampleRate, Channels: 1, Data: kokoroPCM16LE(samples)}, nil
	}
	wave := &sherpa.GeneratedAudio{Samples: samples, SampleRate: sampleRate}
	data := wave.ToBuffer()
	if len(data) == 0 {
		return Audio{}, fmt.Errorf("%w: native piper failed to encode WAV", ErrProviderUnavailable)
	}
	return Audio{Format: FormatWAV, SampleRate: sampleRate, Channels: 1, Data: data}, nil
}

func (e *nativePiperEngine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.engine != nil {
		sherpa.DeleteOfflineTts(e.engine)
		e.engine = nil
	}
}
