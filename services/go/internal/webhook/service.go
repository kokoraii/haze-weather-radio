package webhook

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/alerttext"
)

const serviceID = "haze-webhook"

var testEventCodes = map[string]struct{}{"RWT": {}, "RMT": {}, "DMO": {}}
var adminEventCodes = map[string]struct{}{"NMN": {}, "ADR": {}, "TXP": {}, "TXF": {}, "TXO": {}}

var codecExt = map[string]string{
	"libopus":    "ogg",
	"libmp3lame": "mp3",
	"aac":        "aac",
	"libvorbis":  "ogg",
	"pcm_s16le":  "wav",
	"wav":        "wav",
}

const (
	maxDiscordAttachmentBytes = 8 * 1024 * 1024
	opusBitrate               = "32k"
)

func webhookHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          32,
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}
}

func Run(ctx context.Context, options Options) error {
	if options.Timeout <= 0 {
		options.Timeout = 15 * time.Second
	}
	if strings.TrimSpace(options.ConfigPath) == "" {
		options.ConfigPath = "config.yaml"
	}
	loadDotEnv(filepath.Join(filepath.Dir(filepath.Clean(options.ConfigPath)), ".env"))
	loadDotEnv(".env")
	dispatch := newDispatcher(ctx, webhookHTTPClient(options.Timeout))
	cache := &configCache{}
	for {
		cfg, err := loadConfig(options)
		if err != nil {
			return err
		}
		bridge, err := connectBridge(ctx, options.BridgeAddr)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("webhook waiting for event bridge: %v", err)
			sleepOrDone(ctx, time.Second)
			continue
		}
		log.Printf("Discord webhook service connected to event bridge")
		service := &Service{cfg: cfg, cache: cache, dispatch: dispatch}
		stop := context.AfterFunc(ctx, func() { _ = bridge.Close() })
		err = bridge.ReadMessages(func(event map[string]any) bool {
			service.handleEvent(event)
			return true
		})
		stop()
		_ = bridge.Close()
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("webhook event bridge disconnected: %v", err)
		sleepOrDone(ctx, time.Second)
	}
}

// Service routes alert events to the configured webhooks. The dispatcher and
// config cache outlive bridge reconnects.
type Service struct {
	cfg      loadedConfig
	cache    *configCache
	dispatch *dispatcher
}

// handleEvent never blocks: the broker disconnects clients that stop reading.
func (s *Service) handleEvent(event map[string]any) {
	if firstText(event, nil, "type") != alertAudioReady {
		return
	}
	data := mapAt(event, "data")
	feedID := firstText(event, data, "feed_id")
	if feedID == "" {
		return
	}
	configs, err := s.cache.load(s.cfg.WebhooksPath)
	if err != nil {
		log.Printf("Discord webhook config load failed: %v", err)
		return
	}
	sameEvent := strings.ToUpper(firstNonBlank(firstText(nil, data, "same_event"), firstText(nil, data, "event"), "ADR"))
	_, isTest := testEventCodes[sameEvent]
	_, isAdmin := adminEventCodes[sameEvent]
	for _, cfg := range configs {
		if !cfg.enabled() || cfg.FeedID != feedID {
			continue
		}
		if isTest && !cfg.logTestAlerts() {
			continue
		}
		if isAdmin && !cfg.logAdminAlerts() {
			continue
		}
		job := delivery{
			feedID: feedID,
			label:  fmt.Sprintf("%s (%s)", sameEvent, firstNonBlank(firstText(nil, data, "alert_id"), firstText(nil, data, "title"))),
			url:    cfg.WebhookURL,
			body:   buildPayload(cfg, feedID, sameEvent, data, s.cfg.ListenBaseURL),
		}
		if !s.dispatch.enqueue(job, s.audioTranscode(cfg, feedID, data)) {
			log.Printf("[%s] Discord webhook queue full, alert dropped: %s", feedID, job.label)
		}
	}
}

// audioTranscode returns the attachment job for an alert, or nil when the
// webhook does not embed audio.
func (s *Service) audioTranscode(cfg WebhookConfig, feedID string, data map[string]any) func() attachment {
	rel := firstText(nil, data, "audio_path")
	if !cfg.audioEnabled() || rel == "" {
		return nil
	}
	return func() attachment {
		audio, err := transcodeAlertAudio(s.dispatch.ctx, resolvePath(s.cfg.BaseDir, rel), cfg.EmbedAudio.Codec, intAt(data, "sample_rate", 48000), intAt(data, "channels", 1))
		if err != nil {
			log.Printf("[%s] Discord webhook audio attachment skipped: %v", feedID, err)
		}
		return audio
	}
}

type discordPayload struct {
	Username  string         `json:"username,omitempty"`
	AvatarURL string         `json:"avatar_url,omitempty"`
	Embeds    []discordEmbed `json:"embeds"`
}

type discordEmbed struct {
	Title       string              `json:"title,omitempty"`
	Description string              `json:"description,omitempty"`
	Color       int                 `json:"color,omitempty"`
	Fields      []discordEmbedField `json:"fields,omitempty"`
	Footer      *discordFooter      `json:"footer,omitempty"`
	Timestamp   string              `json:"timestamp,omitempty"`
}

type discordEmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type discordFooter struct {
	Text string `json:"text"`
}

func buildPayload(cfg WebhookConfig, feedID string, sameEvent string, data map[string]any, listenBaseURL string) discordPayload {
	packet, hasPacket := alertmodel.FromMap(data)
	title := firstNonBlank(firstText(nil, data, "headline"), firstText(nil, data, "title", "header"), sameEvent)
	description := firstText(nil, data, "description")
	instruction := firstText(nil, data, "instruction")
	generated := firstText(nil, data, "alert_text", "same_message", "message")
	backgroundColor := firstText(nil, data, "background_color")
	severity := firstText(nil, data, "severity")
	eventName := firstNonBlank(firstText(nil, data, "event"), sameEvent)
	expiresAt := firstText(nil, data, "expires", "alert_expires_at")
	authoritativeURL := firstText(nil, data, "authoritative_url")
	identifier := firstText(nil, data, "alert_id", "identifier")
	if hasPacket {
		identifier = firstNonBlank(packet.ID, identifier)
		title = firstNonBlank(packet.Content.Headline, packet.Content.Title, title)
		description = firstNonBlank(packet.Content.Description, description)
		instruction = firstNonBlank(packet.Content.Instruction, instruction)
		generated = firstNonBlank(packet.Presentation.SpeechText, generated)
		backgroundColor = firstNonBlank(packet.Content.BackgroundColor, backgroundColor)
		severity = firstNonBlank(packet.Content.Severity, severity)
		eventName = firstNonBlank(packet.Content.Event, packet.Content.EventName, eventName)
		expiresAt = firstNonBlank(packet.Timing.ExpiresAt, expiresAt)
		if packet.SAME != nil {
			eventName = firstNonBlank(packet.SAME.Event, eventName)
		}
		if packet.Audio != nil {
			authoritativeURL = firstNonBlank(packet.Audio.URL, authoritativeURL)
		}
	}
	if description == "" || len(description) > 4096 {
		description = generated
	}
	color := colorInt(firstNonBlank(
		backgroundColor,
		alerttext.PickBannerColor([]alerttext.AlertVisualInput{{
			Severity: severity,
			Event:    firstNonBlank(eventName, title, sameEvent),
		}}),
	))
	embed := discordEmbed{
		Title:       clipText(title, 256),
		Description: clipText(description, 4096),
		Color:       color,
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	embed.Fields = append(embed.Fields,
		discordEmbedField{Name: "Feed", Value: clipText(feedID, 1024), Inline: true},
		discordEmbedField{Name: "Event", Value: clipText(eventName, 1024), Inline: true},
		discordEmbedField{Name: "Severity", Value: clipText(firstNonBlank(severity, "Unknown"), 1024), Inline: true},
	)
	if expiresAt != "" {
		embed.Fields = append(embed.Fields, discordEmbedField{Name: "Expires", Value: clipText(expiresAt, 1024), Inline: true})
	}
	if instruction != "" {
		embed.Fields = append(embed.Fields, discordEmbedField{Name: "Instruction", Value: clipText(instruction, 1024), Inline: false})
	}
	if header := codeBlockText(firstText(nil, data, "same_header"), 1024); header != "" {
		embed.Fields = append(embed.Fields, discordEmbedField{Name: "SAME Header", Value: header, Inline: false})
	}
	if generated != "" {
		embed.Fields = append(embed.Fields, discordEmbedField{Name: "SAME Message", Value: codeBlockText(generated, 1024), Inline: false})
	}
	if listenURL := integratedListenURL(listenBaseURL, feedID); listenURL != "" {
		embed.Fields = append(embed.Fields, discordEmbedField{Name: "Listen", Value: clipText(listenURL, 1024), Inline: false})
	}
	if authoritativeURL != "" {
		embed.Fields = append(embed.Fields, discordEmbedField{Name: "CAP Audio", Value: clipText(authoritativeURL, 1024), Inline: false})
	}
	if identifier != "" {
		embed.Footer = &discordFooter{Text: clipText(identifier, 2048)}
	}
	return discordPayload{
		Username:  cfg.Username,
		AvatarURL: cfg.IconURL,
		Embeds:    []discordEmbed{embed},
	}
}

func integratedListenURL(base string, feedID string) string {
	base = strings.TrimSpace(base)
	feedID = strings.TrimSpace(feedID)
	if base == "" || feedID == "" {
		return ""
	}
	parsed, err := url.Parse(strings.TrimRight(base, "/") + "/listen")
	if err != nil {
		return ""
	}
	query := parsed.Query()
	query.Set("feed", feedID)
	query.Set("codec", "pcm16")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

type attachment struct {
	Name string
	Data []byte
}

func transcodeAlertAudio(ctx context.Context, src string, codec string, sampleRate int, channels int) (attachment, error) {
	info, err := os.Stat(src)
	if err != nil {
		return attachment{}, err
	}
	if !info.Mode().IsRegular() {
		return attachment{}, fmt.Errorf("audio path is not a regular file")
	}
	if sampleRate <= 0 {
		sampleRate = 48000
	}
	if channels <= 0 {
		channels = 1
	}
	codec = firstNonBlank(codec, "libopus")
	ext := codecExt[codec]
	if ext == "" {
		ext = "ogg"
	}
	outCodec := codec
	if codec == "wav" {
		outCodec = "pcm_s16le"
	}
	transcodeCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(
		transcodeCtx,
		"ffmpeg",
		ffmpegAudioTranscodeArgs(src, outCodec, ext, sampleRate, channels)...,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return attachment{}, fmt.Errorf("ffmpeg: %s", clipText(detail, 200))
		}
		return attachment{}, err
	}
	if stdout.Len() > maxDiscordAttachmentBytes {
		return attachment{}, fmt.Errorf("audio exceeds Discord attachment limit")
	}
	name := "alert." + ext
	if base := strings.TrimSuffix(filepath.Base(src), filepath.Ext(src)); base != "" {
		name = base + "." + ext
	}
	return attachment{Name: name, Data: stdout.Bytes()}, nil
}

func ffmpegAudioTranscodeArgs(src string, codec string, ext string, sampleRate int, channels int) []string {
	args := []string{
		"-loglevel", "error",
		"-f", "s16le",
		"-ar", strconv.Itoa(sampleRate),
		"-ac", strconv.Itoa(channels),
		"-i", src,
		"-c:a", codec,
	}
	if codec == "libopus" {
		args = append(args, "-b:a", opusBitrate)
	}
	return append(args, "-f", ext, "pipe:1")
}

func colorInt(value string) int {
	text := strings.TrimPrefix(strings.TrimSpace(value), "#")
	if text == "" {
		return 0x888888
	}
	parsed, err := strconv.ParseInt(text, 16, 32)
	if err != nil {
		return 0x888888
	}
	return int(parsed)
}

func clipText(value string, limit int) string {
	text := strings.TrimSpace(value)
	if limit <= 0 || len(text) <= limit {
		return text
	}
	if limit <= 3 {
		return text[:limit]
	}
	return strings.TrimSpace(text[:limit-3]) + "..."
}

func codeBlockText(value string, limit int) string {
	normalized := strings.ReplaceAll(strings.TrimSpace(value), "```", "'''")
	if normalized == "" {
		return ""
	}
	innerLimit := limit - 8
	if innerLimit < 1 {
		innerLimit = 1
	}
	return "```\n" + clipText(normalized, innerLimit) + "\n```"
}

func sleepOrDone(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
