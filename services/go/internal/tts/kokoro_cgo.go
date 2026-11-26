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

// KokoroProvider synthesizes speech with sherpa-onnx's native Kokoro runtime.
type KokoroProvider struct {
	options kokoroOptions

	mu       sync.Mutex
	engine   *sherpa.OfflineTts
	lastUsed time.Time
}

// NewKokoroProvider creates a native Kokoro provider using HAZE_KOKORO_* configuration.
func NewKokoroProvider() *KokoroProvider {
	return &KokoroProvider{options: defaultKokoroOptions()}
}

func (p *KokoroProvider) ID() string { return kokoroProviderID }

func (p *KokoroProvider) ListVoices(ctx context.Context) ([]Voice, error) {
	engine, err := p.ensureEngine(ctx)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	count := engine.NumSpeakers()
	p.mu.Unlock()
	if count <= 0 {
		count = 1
	}
	voices := make([]Voice, 0, count)
	for sid := range count {
		id := kokoroVoiceID(sid)
		voices = append(voices, Voice{
			ID:       id,
			Name:     fmt.Sprintf("Kokoro voice %s", id),
			Provider: p.ID(),
			Language: kokoroVoiceLanguage(p.options.Lang),
		})
	}
	return voices, nil
}

func (p *KokoroProvider) Synthesize(ctx context.Context, req Request) (Audio, error) {
	if strings.TrimSpace(req.Text) == "" {
		return Audio{}, errors.New("empty synthesis text")
	}
	sid, err := kokoroSpeakerID(req.VoiceID)
	if err != nil {
		return Audio{}, err
	}
	engine, err := p.ensureEngine(ctx)
	if err != nil {
		return Audio{}, err
	}
	if err := ctx.Err(); err != nil {
		return Audio{}, err
	}

	p.mu.Lock()
	generated := engine.GenerateWithConfig(req.Text, &sherpa.GenerationConfig{
		Sid:          sid,
		Speed:        kokoroSpeedForRequest(p.options.Speed, req.Rate),
		SilenceScale: p.options.SilenceScale,
	}, nil)
	sampleRate := 0
	if generated != nil {
		sampleRate = generated.SampleRate
	}
	if sampleRate <= 0 {
		sampleRate = engine.SampleRate()
	}
	p.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return Audio{}, err
	}
	if generated == nil || len(generated.Samples) == 0 {
		return Audio{}, fmt.Errorf("%w: kokoro generated no audio", ErrProviderUnavailable)
	}
	if sampleRate <= 0 {
		return Audio{}, fmt.Errorf("%w: kokoro returned invalid sample rate", ErrProviderUnavailable)
	}

	samples := kokoroApplyVolume(generated.Samples, req.Volume)
	if req.OutputFormat == FormatPCM16LE {
		return Audio{
			Format:     FormatPCM16LE,
			SampleRate: sampleRate,
			Channels:   1,
			Data:       kokoroPCM16LE(samples),
		}, nil
	}

	wave := &sherpa.GeneratedAudio{Samples: samples, SampleRate: sampleRate}
	data := wave.ToBuffer()
	if len(data) == 0 {
		return Audio{}, fmt.Errorf("%w: kokoro failed to encode WAV", ErrProviderUnavailable)
	}
	return Audio{
		Format:     FormatWAV,
		SampleRate: sampleRate,
		Channels:   1,
		Data:       data,
	}, nil
}

func (p *KokoroProvider) ensureEngine(ctx context.Context) (*sherpa.OfflineTts, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.engine != nil {
		p.lastUsed = time.Now()
		return p.engine, nil
	}
	if err := p.options.ensureModelFiles(ctx); err != nil {
		return nil, err
	}
	config := p.options.sherpaConfig()
	engine := sherpa.NewOfflineTts(&config)
	if engine == nil {
		return nil, fmt.Errorf("%w: failed to initialize kokoro native runtime", ErrProviderUnavailable)
	}
	p.engine = engine
	p.lastUsed = time.Now()
	runtime.SetFinalizer(p, func(provider *KokoroProvider) {
		provider.mu.Lock()
		engine := provider.engine
		provider.engine = nil
		provider.lastUsed = time.Time{}
		provider.mu.Unlock()
		if engine != nil {
			sherpa.DeleteOfflineTts(engine)
		}
	})
	return p.engine, nil
}

// PruneIdleRuntime closes the Kokoro model after it has been idle long enough.
func (p *KokoroProvider) PruneIdleRuntime(maxIdle time.Duration) int {
	if maxIdle <= 0 {
		return 0
	}
	cutoff := time.Now().Add(-maxIdle)
	p.mu.Lock()
	if p.engine == nil || (!p.lastUsed.IsZero() && p.lastUsed.After(cutoff)) {
		p.mu.Unlock()
		return 0
	}
	engine := p.engine
	p.engine = nil
	p.lastUsed = time.Time{}
	p.mu.Unlock()
	sherpa.DeleteOfflineTts(engine)
	ReleaseNativeMemory()
	return 1
}

func (o kokoroOptions) sherpaConfig() sherpa.OfflineTtsConfig {
	debug := 0
	if o.Debug {
		debug = 1
	}
	return sherpa.OfflineTtsConfig{
		Model: sherpa.OfflineTtsModelConfig{
			Kokoro: sherpa.OfflineTtsKokoroModelConfig{
				Model:       o.ModelPath,
				Voices:      o.VoicesPath,
				Tokens:      o.TokensPath,
				DataDir:     o.DataDir,
				Lexicon:     o.LexiconPath,
				Lang:        o.Lang,
				LengthScale: o.LengthScale,
			},
			NumThreads: o.Threads,
			Debug:      debug,
			Provider:   o.RuntimeProvider,
		},
		RuleFsts:        o.RuleFsts,
		RuleFars:        o.RuleFars,
		MaxNumSentences: o.MaxSentences,
		SilenceScale:    o.SilenceScale,
	}
}

func kokoroVoiceID(sid int) string {
	return fmt.Sprintf("%d", sid)
}

func kokoroVoiceLanguage(lang string) []string {
	normalized := NormalizeLanguage(lang)
	if normalized == "" {
		normalized = "en"
	}
	return []string{normalized}
}

func kokoroSpeedForRequest(base float32, rate int) float32 {
	if base <= 0 {
		base = 1
	}
	if rate <= 0 {
		return base
	}
	return kokoroClampFloat32(float32(rate)/100, 0.5, 2.0)
}

func kokoroApplyVolume(samples []float32, volume int) []float32 {
	if volume <= 0 || volume == 100 {
		return samples
	}
	gain := float32(volume) / 100
	scaled := make([]float32, len(samples))
	for i, sample := range samples {
		scaled[i] = kokoroClampFloat32(sample*gain, -1, 1)
	}
	return scaled
}
