package ivr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type ProductCache struct {
	cfg       loadedConfig
	bridge    *bridgeClient
	renderSem chan struct{}
	mu        sync.Mutex
	inflight  map[string]*cacheCall
}

type cacheCall struct {
	done   chan struct{}
	result CachedProduct
	err    error
}

type CachedProduct struct {
	Key         string           `json:"key"`
	Location    ResolvedLocation `json:"location"`
	Packages    []string         `json:"packages"`
	Title       string           `json:"title"`
	Text        string           `json:"text"`
	ReaderID    string           `json:"reader_id,omitempty"`
	Language    string           `json:"language"`
	WAVPath     string           `json:"wav_path"`
	PCMUPath    string           `json:"pcmu_path"`
	G722Path    string           `json:"g722_path,omitempty"`
	GeneratedAt time.Time        `json:"generated_at"`
	ExpiresAt   time.Time        `json:"expires_at"`
}

type CachedAudio struct {
	Key         string    `json:"key"`
	Title       string    `json:"title"`
	Text        string    `json:"text"`
	WAVPath     string    `json:"wav_path"`
	PCMUPath    string    `json:"pcmu_path"`
	G722Path    string    `json:"g722_path,omitempty"`
	GeneratedAt time.Time `json:"generated_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func NewProductCache(cfg loadedConfig, bridge *bridgeClient) *ProductCache {
	return &ProductCache{
		cfg:       cfg,
		bridge:    bridge,
		renderSem: make(chan struct{}, maxInt(1, cfg.IVR.MaxRenderInflight)),
		inflight:  map[string]*cacheCall{},
	}
}

func (c *ProductCache) Get(ctx context.Context, location ResolvedLocation, packages []string, force bool) (CachedProduct, error) {
	key, packages, _, _, _ := c.productCacheKey(location, packages)
	if !force {
		if cached, ok := c.readFresh(key); ok {
			return cached, nil
		}
	}

	c.mu.Lock()
	if call := c.inflight[key]; call != nil {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return CachedProduct{}, ctx.Err()
		case <-call.done:
			return call.result, call.err
		}
	}
	call := &cacheCall{done: make(chan struct{})}
	c.inflight[key] = call
	c.mu.Unlock()

	call.result, call.err = c.render(ctx, key, location, packages)
	close(call.done)

	c.mu.Lock()
	delete(c.inflight, key)
	c.mu.Unlock()
	return call.result, call.err
}

func (c *ProductCache) Fresh(location ResolvedLocation, packages []string) (CachedProduct, bool) {
	key, _, _, _, _ := c.productCacheKey(location, packages)
	return c.readFresh(key)
}

func (c *ProductCache) productCacheKey(location ResolvedLocation, packages []string) (string, []string, string, string, TTSProfile) {
	packages = normalizePackages(packages, c.cfg.IVR.DefaultPackages)
	policy := c.cfg.Prompts.TTSForMenu("weather_product")
	language := firstNonBlank(location.Language, c.cfg.IVR.DefaultLanguage)
	if policy.ExplicitLanguage && strings.TrimSpace(policy.Language) != "" {
		language = strings.TrimSpace(policy.Language)
	}
	readerID := c.cfg.resolveIVRReaderID(policy.ReaderID, language)
	policy.ReaderID = readerID
	readerFingerprint := c.cfg.ttsReaderFingerprint(readerID, language, "")
	return cacheKey(location, packages, language, policy, readerFingerprint), packages, language, readerID, policy
}

func (c *ProductCache) render(ctx context.Context, key string, location ResolvedLocation, packages []string) (CachedProduct, error) {
	select {
	case c.renderSem <- struct{}{}:
		defer func() { <-c.renderSem }()
	case <-ctx.Done():
		return CachedProduct{}, ctx.Err()
	}
	renderCtx, cancel := context.WithTimeout(ctx, c.cfg.IVR.RenderTimeout)
	defer cancel()

	_, packages, language, readerID, policy := c.productCacheKey(location, packages)
	requestID := fmt.Sprintf("ivr-%s-wx-%d", key[:12], time.Now().UnixNano())
	product, err := c.bridge.WxOnDemand(renderCtx, requestID, location, packages, language, readerID)
	if err != nil {
		return CachedProduct{}, err
	}
	if strings.TrimSpace(product.Text) == "" {
		return CachedProduct{}, fmt.Errorf("no IVR product text generated for %s", location.Code)
	}
	if readerID == "" {
		readerID = product.ReaderID
	}
	if language == "" {
		language = product.Language
	}

	base := filepath.Join(c.cfg.cacheDir(), key)
	if err := os.MkdirAll(base, 0o755); err != nil {
		return CachedProduct{}, err
	}
	text := strings.TrimSpace(product.Text)
	wavPath := filepath.Join(base, "audio.wav")
	pcm16Path := filepath.Join(base, "audio.pcm16")
	pcmuPath := filepath.Join(base, "audio.pcmu")
	g722Path := filepath.Join(base, "audio.g722")
	jobID := fmt.Sprintf("ivr-tts-%s-%d", key[:12], time.Now().UnixNano())
	synth, err := c.bridge.Synthesize(renderCtx, synthRequest{
		ID:              jobID,
		Text:            text,
		ReaderID:        readerID,
		Provider:        policy.Provider,
		VoiceID:         policy.VoiceID,
		Language:        language,
		Timezone:        location.Timezone,
		Rate:            policy.Rate,
		Volume:          policy.Volume,
		SentenceSilence: policy.SentenceSilence,
		OutputPath:      pcm16Path,
		OutputFormat:    "pcm_s16le",
		Priority:        "high",
	})
	if err != nil {
		return CachedProduct{}, err
	}
	if err := c.writeTelephoneAudio(synth, wavPath, pcmuPath, g722Path); err != nil {
		return CachedProduct{}, err
	}
	now := time.Now().UTC()
	result := CachedProduct{
		Key:         key,
		Location:    location,
		Packages:    packages,
		Title:       fallbackText(product.Title, location.Name),
		Text:        text,
		ReaderID:    readerID,
		Language:    language,
		WAVPath:     wavPath,
		PCMUPath:    pcmuPath,
		G722Path:    g722Path,
		GeneratedAt: now,
		ExpiresAt:   now.Add(ttlForPolicy(policy, c.cfg.cacheTTL())),
	}
	if err := c.writeMetadata(result); err != nil {
		return CachedProduct{}, err
	}
	return result, nil
}

func (c *ProductCache) GetPrompt(ctx context.Context, menuID string, lineKey string, values map[string]string, force bool) (CachedAudio, error) {
	text := c.cfg.Prompts.MenuLine(menuID, lineKey, values)
	if text == "" {
		return CachedAudio{}, fmt.Errorf("IVR prompt %s/%s is not configured", menuID, lineKey)
	}
	policy := c.cfg.Prompts.TTSForMenu(menuID)
	policy = promptPolicyLanguage(policy, values)
	return c.getPromptAudio(ctx, menuID, lineKey, text, policy, force)
}

func (c *ProductCache) GetPromptWithPolicy(ctx context.Context, menuID string, lineKey string, values map[string]string, policy TTSProfile, force bool) (CachedAudio, error) {
	text := c.cfg.Prompts.MenuLine(menuID, lineKey, values)
	if text == "" {
		return CachedAudio{}, fmt.Errorf("IVR prompt %s/%s is not configured", menuID, lineKey)
	}
	return c.getPromptAudio(ctx, menuID, lineKey, text, promptPolicyLanguage(policy, values), force)
}

func promptPolicyLanguage(policy TTSProfile, values map[string]string) TTSProfile {
	if language := firstNonBlank(values["language"], values["lang"]); language != "" {
		policy.Language = language
	}
	return policy
}

func (c *ProductCache) GetTextPromptWithPolicy(ctx context.Context, menuID string, lineKey string, text string, policy TTSProfile, force bool) (CachedAudio, error) {
	text = cleanPromptText(text)
	if text == "" {
		return CachedAudio{}, fmt.Errorf("IVR prompt %s/%s has no text", menuID, lineKey)
	}
	return c.getPromptAudio(ctx, menuID, lineKey, text, policy, force)
}

func (c *ProductCache) getPromptAudio(ctx context.Context, menuID string, lineKey string, text string, policy TTSProfile, force bool) (CachedAudio, error) {
	policy.ReaderID = c.cfg.resolveIVRReaderID(policy.ReaderID, policy.Language)
	key := promptCacheKey(menuID, lineKey, text, policy, c.cfg.ttsReaderFingerprint(policy.ReaderID, policy.Language, ""))
	if !force {
		if cached, ok := c.readFreshAudio(key); ok {
			return cached, nil
		}
	}
	renderCtx, cancel := context.WithTimeout(ctx, c.cfg.IVR.RenderTimeout)
	defer cancel()
	base := filepath.Join(c.cfg.cacheDir(), "prompts", key)
	if err := os.MkdirAll(base, 0o755); err != nil {
		return CachedAudio{}, err
	}
	wavPath := filepath.Join(base, "audio.wav")
	pcm16Path := filepath.Join(base, "audio.pcm16")
	pcmuPath := filepath.Join(base, "audio.pcmu")
	g722Path := filepath.Join(base, "audio.g722")
	jobID := fmt.Sprintf("ivr-prompt-%s-%d", key[:12], time.Now().UnixNano())
	synth, err := c.bridge.Synthesize(renderCtx, synthRequest{
		ID:              jobID,
		Text:            text,
		ReaderID:        policy.ReaderID,
		Provider:        policy.Provider,
		VoiceID:         policy.VoiceID,
		Language:        policy.Language,
		Rate:            policy.Rate,
		Volume:          policy.Volume,
		SentenceSilence: policy.SentenceSilence,
		OutputPath:      pcm16Path,
		OutputFormat:    "pcm_s16le",
		Priority:        policy.Priority,
	})
	if err != nil {
		return CachedAudio{}, err
	}
	if err := c.writeTelephoneAudio(synth, wavPath, pcmuPath, g722Path); err != nil {
		return CachedAudio{}, err
	}
	now := time.Now().UTC()
	result := CachedAudio{
		Key:         key,
		Title:       menuID + "/" + lineKey,
		Text:        text,
		WAVPath:     wavPath,
		PCMUPath:    pcmuPath,
		G722Path:    g722Path,
		GeneratedAt: now,
		ExpiresAt:   now.Add(ttlForPolicy(policy, c.cfg.cacheTTL())),
	}
	if err := c.writeAudioMetadata(result); err != nil {
		return CachedAudio{}, err
	}
	return result, nil
}

func (c *ProductCache) writeTelephoneAudio(synth synthResult, wavPath string, pcmuPath string, g722Path string) error {
	switch synth.Format {
	case "pcm_s16le":
		return writeTelephoneAudioFromPCMFile(synth.Path, synth.SampleRate, synth.Channels, wavPath, pcmuPath, g722Path, c.cfg.IVR.Cache.PhoneSampleRate)
	default:
		if strings.TrimSpace(synth.Path) != "" && filepath.Clean(synth.Path) != filepath.Clean(wavPath) {
			if err := copyFile(synth.Path, wavPath); err != nil {
				return err
			}
		}
		return writeTelephoneAudioFromWAV(wavPath, pcmuPath, g722Path, c.cfg.IVR.Cache.PhoneSampleRate)
	}
}

func (c *ProductCache) readFresh(key string) (CachedProduct, bool) {
	path := filepath.Join(c.cfg.cacheDir(), key, "product.json")
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return CachedProduct{}, false
	}
	var cached CachedProduct
	if err := json.Unmarshal(raw, &cached); err != nil {
		return CachedProduct{}, false
	}
	if time.Now().UTC().After(cached.ExpiresAt) {
		return CachedProduct{}, false
	}
	if _, err := os.Stat(cached.WAVPath); err != nil {
		return CachedProduct{}, false
	}
	if _, err := os.Stat(cached.PCMUPath); err != nil {
		return CachedProduct{}, false
	}
	if _, err := os.Stat(cached.G722Path); err != nil {
		return CachedProduct{}, false
	}
	return cached, true
}

func (c *ProductCache) readFreshAudio(key string) (CachedAudio, bool) {
	path := filepath.Join(c.cfg.cacheDir(), "prompts", key, "audio.json")
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return CachedAudio{}, false
	}
	var cached CachedAudio
	if err := json.Unmarshal(raw, &cached); err != nil {
		return CachedAudio{}, false
	}
	if time.Now().UTC().After(cached.ExpiresAt) {
		return CachedAudio{}, false
	}
	if _, err := os.Stat(cached.WAVPath); err != nil {
		return CachedAudio{}, false
	}
	if _, err := os.Stat(cached.PCMUPath); err != nil {
		return CachedAudio{}, false
	}
	if _, err := os.Stat(cached.G722Path); err != nil {
		return CachedAudio{}, false
	}
	return cached, true
}

func (c *ProductCache) writeMetadata(product CachedProduct) error {
	raw, err := json.MarshalIndent(product, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(c.cfg.cacheDir(), product.Key, "product.json"), raw, 0o644)
}

func (c *ProductCache) writeAudioMetadata(audio CachedAudio) error {
	raw, err := json.MarshalIndent(audio, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(c.cfg.cacheDir(), "prompts", audio.Key, "audio.json"), raw, 0o644)
}

func cacheKey(location ResolvedLocation, packages []string, lang string, policy TTSProfile, readerFingerprint string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		location.FeedID,
		location.Code,
		location.Source,
		location.Forecast,
		location.StationID,
		location.AirQualityID,
		location.ClimateID,
		location.HydrometricID,
		location.MarineForecastID,
		location.Latitude,
		location.Longitude,
		strings.Join(packages, ","),
		"telephone",
		lang,
		ttsPolicyFingerprint(policy),
		readerFingerprint,
	}, "|")))
	return hex.EncodeToString(sum[:])
}

func promptCacheKey(menuID string, lineKey string, text string, policy TTSProfile, readerFingerprint string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		menuID,
		lineKey,
		text,
		ttsPolicyFingerprint(policy),
		readerFingerprint,
	}, "|")))
	return hex.EncodeToString(sum[:])
}

func ttsPolicyFingerprint(policy TTSProfile) string {
	return strings.Join([]string{
		policy.ReaderID,
		policy.Provider,
		policy.VoiceID,
		policy.Language,
		fmt.Sprint(policy.Rate),
		fmt.Sprint(policy.Volume),
		fmt.Sprintf("%.3f", policy.SentenceSilence),
	}, "|")
}

func ttlForPolicy(policy TTSProfile, fallback time.Duration) time.Duration {
	if policy.CacheTTL > 0 {
		return policy.CacheTTL
	}
	return fallback
}

func normalizePackages(packages []string, fallback []string) []string {
	source := packages
	if len(source) == 0 {
		source = fallback
	}
	out := make([]string, 0, len(source))
	seen := map[string]struct{}{}
	for _, item := range source {
		item = strings.ToLower(strings.TrimSpace(item))
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	if len(out) == 0 {
		return []string{"current_conditions", "forecast"}
	}
	return out
}

func safeID(value string) string {
	var builder strings.Builder
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.' {
			builder.WriteRune(ch)
		}
	}
	if builder.Len() == 0 {
		return "item"
	}
	return builder.String()
}
