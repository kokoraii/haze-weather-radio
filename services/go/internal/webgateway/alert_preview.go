package webgateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const alertPreviewTimeout = 120 * time.Second

func (s *wsSession) previewAlert(payload map[string]any) (map[string]any, error) {
	targets, err := alertTargetFeedIDs(s.configPath, payload)
	if err != nil {
		return nil, err
	}
	includeSame := boolPayload(payload, "include_same", true)
	allFeedLocations := boolPayload(payload, "all_feed_locations", false)
	alertID := safeID(fmt.Sprintf("preview-%d", time.Now().UTC().UnixNano()))
	locationsByFeed, err := alertLocationsByFeed(s.configPath, payload, targets, allFeedLocations)
	if err != nil {
		return nil, err
	}
	scopedPayload := withFeedFallback(payload, targets[0])
	scopedPayload["feed_id"] = targets[0]
	scopedPayload["feed_ids"] = []string{targets[0]}
	if allFeedLocations {
		delete(scopedPayload, "area_names")
	}
	scopedPayload["locations"] = locationsByFeed[targets[0]]
	data := s.broadcastAlertDataForFeed(payload, targets[0], locationsByFeed[targets[0]], alertID, includeSame, allFeedLocations)
	alertText := strings.TrimSpace(stringValue(data, "alert_text"))
	if alertText == "" {
		return nil, fmt.Errorf("alert preview text is empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), alertPreviewTimeout)
	defer cancel()
	voicePCM, err := synthesizeAlertPreviewVoice(ctx, s.configPath, scopedPayload, targets[0], alertID, alertText)
	if err != nil {
		return nil, err
	}
	lead, tail, header, err := alertPreviewLeadTail(s.configPath, scopedPayload, includeSame)
	if err != nil {
		return nil, err
	}
	pcm := assembleAlertPreviewPCM(lead, voicePCM, tail, 48000, 1)
	return map[string]any{
		"audio_base64": base64.StdEncoding.EncodeToString(wavFromPCM16(pcm, 48000, 1)),
		"format":       "wav",
		"content_type": "audio/wav",
		"sample_rate":  48000,
		"channels":     1,
		"include_same": includeSame,
		"same_header":  header,
		"alert_text":   alertText,
		"feed_id":      targets[0],
		"feed_ids":     targets,
	}, nil
}

func synthesizeAlertPreviewVoice(ctx context.Context, configPath string, payload map[string]any, feedID string, alertID string, text string) ([]byte, error) {
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return nil, fmt.Errorf("event bridge is not available")
	}
	bridge, err := connectWxBridge(ctx, bridgeAddr)
	if err != nil {
		return nil, fmt.Errorf("event bridge connect failed: %w", err)
	}
	defer bridge.Close()
	jobID := safeID("alert-preview-" + alertID)
	outputPath := resolveConfigPath(configPath, filepath.Join("runtime", "audio", "previews", jobID+".pcm16le"))
	synth, err := bridge.Synthesize(ctx, jobID, wxRenderedProduct{
		FeedID:   feedID,
		Title:    "Alert Preview",
		Text:     text,
		ReaderID: strings.TrimSpace(firstNonBlank(stringPayload(payload, "reader_id", ""), "00")),
		Language: strings.TrimSpace(firstNonBlank(stringPayload(payload, "language", ""), "en-CA")),
	}, outputPath, "pcm_s16le")
	if err != nil {
		return nil, err
	}
	path := firstNonBlank(synth.Path, outputPath)
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	return normalizePreviewVoicePCM(ctx, raw, synth)
}

func normalizePreviewVoicePCM(ctx context.Context, raw []byte, synth wxSynthResult) ([]byte, error) {
	format := strings.ToLower(strings.TrimSpace(synth.Format))
	if bytes.HasPrefix(raw, []byte("RIFF")) {
		if pcm, info, err := wavPCM16Info(raw); err == nil && info.SampleRate == 48000 && info.Channels == 1 {
			return pcm, nil
		}
		outputFormat, _ := wxAudioFormatByID("raw")
		return transcodeWxAudio(ctx, raw, outputFormat)
	}
	if format == "" || format == "pcm_s16le" || format == "raw" || format == "raw_pcm16" {
		sampleRate := synth.SampleRate
		if sampleRate <= 0 {
			sampleRate = 48000
		}
		channels := synth.Channels
		if channels <= 0 {
			channels = 1
		}
		return transcodeRawPCM16ToPCM(ctx, raw, sampleRate, channels, 48000, 1)
	}
	outputFormat, _ := wxAudioFormatByID("raw")
	return transcodeWxAudio(ctx, raw, outputFormat)
}

func transcodeRawPCM16ToPCM(ctx context.Context, raw []byte, sourceRate int, sourceChannels int, targetRate int, targetChannels int) ([]byte, error) {
	if sourceRate <= 0 {
		sourceRate = 48000
	}
	if sourceChannels <= 0 {
		sourceChannels = 1
	}
	if targetRate <= 0 {
		targetRate = 48000
	}
	if targetChannels <= 0 {
		targetChannels = 1
	}
	if sourceRate == targetRate && sourceChannels == targetChannels {
		return raw, nil
	}
	ffmpeg, err := resolveFFmpegExecutable()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg is required to resample preview TTS: %w", err)
	}
	args := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-nostdin",
		"-f", "s16le",
		"-acodec", "pcm_s16le",
		"-ar", fmt.Sprintf("%d", sourceRate),
		"-ac", fmt.Sprintf("%d", sourceChannels),
		"-i", "pipe:0",
		"-vn",
		"-sn",
		"-dn",
		"-f", "s16le",
		"-acodec", "pcm_s16le",
		"-ar", fmt.Sprintf("%d", targetRate),
		"-ac", fmt.Sprintf("%d", targetChannels),
		"pipe:1",
	}
	cmd := exec.CommandContext(ctx, ffmpeg, args...)
	cmd.Stdin = bytes.NewReader(raw)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffmpeg preview PCM resample failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func alertPreviewLeadTail(configPath string, payload map[string]any, includeSame bool) ([]byte, []byte, string, error) {
	request, err := buildSameRequest(configPath, payload)
	if err != nil {
		return nil, nil, "", err
	}
	if includeSame {
		headerRequest := request
		headerRequest.Sequence = "header"
		headerResult, err := runSameGenerator(configPath, headerRequest)
		if err != nil {
			return nil, nil, "", fmt.Errorf("SAME header generation failed: %w", err)
		}
		headerAudio, header, err := alertPreviewAudioFromResult(headerResult)
		if err != nil {
			return nil, nil, "", fmt.Errorf("SAME header generation failed: %w", err)
		}
		eomRequest := request
		eomRequest.Sequence = "eom"
		eomResult, err := runSameGenerator(configPath, eomRequest)
		if err != nil {
			return nil, nil, "", fmt.Errorf("SAME EOM generation failed: %w", err)
		}
		eomAudio, _, err := alertPreviewAudioFromResult(eomResult)
		if err != nil {
			return nil, nil, "", fmt.Errorf("SAME EOM generation failed: %w", err)
		}
		return headerAudio, eomAudio, header, nil
	}
	if !alertPreviewAttentionToneEnabled(payload) {
		return nil, nil, "", nil
	}
	toneRequest := request
	toneRequest.Sequence = "tone"
	toneResult, err := runSameGenerator(configPath, toneRequest)
	if err != nil {
		return nil, nil, "", fmt.Errorf("attention tone generation failed: %w", err)
	}
	toneAudio, _, err := alertPreviewAudioFromResult(toneResult)
	if err != nil {
		return nil, nil, "", fmt.Errorf("attention tone generation failed: %w", err)
	}
	return toneAudio, nil, "", nil
}

func alertPreviewAudioFromResult(result map[string]any) ([]byte, string, error) {
	audioBase64 := strings.TrimSpace(stringPayload(result, "audio_base64", ""))
	if audioBase64 == "" {
		return nil, strings.TrimSpace(stringPayload(result, "header", "")), nil
	}
	audio, err := base64.StdEncoding.DecodeString(audioBase64)
	if err != nil {
		return nil, "", fmt.Errorf("decode generated audio: %w", err)
	}
	return audio, strings.TrimSpace(stringPayload(result, "header", "")), nil
}

func alertPreviewAttentionToneEnabled(payload map[string]any) bool {
	tone := strings.ToUpper(strings.TrimSpace(firstNonBlank(
		stringPayload(payload, "same_tone", ""),
		stringPayload(payload, "tone_type", ""),
		stringPayload(payload, "attention_tone", ""),
	)))
	switch tone {
	case "", "NONE", "NO", "OFF", "DISABLED":
		return false
	default:
		return true
	}
}

func assembleAlertPreviewPCM(lead []byte, voice []byte, tail []byte, sampleRate int, channels int) []byte {
	size := len(lead) + len(voice) + len(tail)
	gap := []byte(nil)
	if len(lead) > 0 {
		gap = silencePCMBytes(sampleRate, channels, time.Second)
		size += len(gap)
	}
	out := make([]byte, 0, size)
	out = append(out, lead...)
	out = append(out, gap...)
	out = append(out, voice...)
	out = append(out, tail...)
	return out
}

func silencePCMBytes(sampleRate int, channels int, duration time.Duration) []byte {
	if sampleRate <= 0 {
		sampleRate = 48000
	}
	if channels <= 0 {
		channels = 1
	}
	samples := int(duration.Seconds() * float64(sampleRate) * float64(channels))
	if samples <= 0 {
		return nil
	}
	return make([]byte, samples*2)
}
