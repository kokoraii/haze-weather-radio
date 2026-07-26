package webgateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultCgenFile = "managed/configs/cgen.xml"
const defaultCgenEncodersFile = "managed/configs/cgen-encoders.xml"
const maxCgenConfigBytes int64 = 4 * 1024 * 1024
const maxCgenEncodersBytes int64 = 2 * 1024 * 1024

var cgenConfigMu sync.RWMutex

type cgenXML struct {
	XMLName       xml.Name      `xml:"cgen"`
	SchemaVersion string        `xml:"schema_version,attr,omitempty"`
	Enabled       string        `xml:"enabled,attr,omitempty"`
	Feeds         []cgenFeedXML `xml:"feed"`
}

type cgenFeedXML struct {
	ID            string                `xml:"id,attr"`
	Name          string                `xml:"name,attr,omitempty"`
	Enabled       string                `xml:"enabled,attr,omitempty"`
	ProgramInput  cgenEndpointXML       `xml:"program>input"`
	PriorityInput cgenPriorityInputXML  `xml:"priority>input"`
	ProgramOutput cgenEndpointXML       `xml:"program>output"`
	Video         cgenVideoXML          `xml:"media>video"`
	Audio         cgenAudioXML          `xml:"media>audio"`
	Ladder        cgenLadderXML         `xml:"ladder"`
	Banner        cgenBannerXML         `xml:"presentation>banner"`
	Graphics      cgenGraphicsXML       `xml:"presentation>graphics"`
	Clock         cgenClockXML          `xml:"presentation>clock"`
	Text          cgenTextXML           `xml:"presentation>text"`
	State         cgenStateXML          `xml:"presentation>state"`
	Standby       cgenStandbyXML        `xml:"presentation>standby"`
	Sync          cgenSyncXML           `xml:"sync"`
	Alert         cgenAlertXML          `xml:"alert"`
	Ancillary     cgenAncillaryXML      `xml:"ancillary"`
	Compositor    cgenCompositorXML     `xml:"compositor"`
	ProgramMap    cgenProgramMappingXML `xml:"programMapping"`
	Outputs       cgenOutputsXML        `xml:"outputs"`
	UpdatedAt     string                `xml:"updated_at,attr,omitempty"`
}

type cgenEndpointXML struct {
	URL                  string `xml:"url,attr,omitempty"`
	Type                 string `xml:"type,attr,omitempty"`
	Format               string `xml:"format,attr,omitempty"`
	VCodec               string `xml:"vcodec,attr,omitempty"`
	ACodec               string `xml:"acodec,attr,omitempty"`
	VideoBitrateKbps     string `xml:"video_bitrate_kbps,attr,omitempty"`
	AudioBitrateKbps     string `xml:"audio_bitrate_kbps,attr,omitempty"`
	HardwareDecoder      string `xml:"hardware_decoder,attr,omitempty"`
	HardwareDecoderOn    string `xml:"hardware_decoder_enabled,attr,omitempty"`
	DeinterlaceAlgorithm string `xml:"deinterlace_algorithm,attr,omitempty"`
	DeinterlaceBackend   string `xml:"deinterlace_backend,attr,omitempty"`
	DeinterlaceCadence   string `xml:"deinterlace_cadence,attr,omitempty"`
	DeinterlaceParity    string `xml:"deinterlace_parity,attr,omitempty"`
	DeviceBackend        string `xml:"device_backend,attr,omitempty"`
	DeviceID             string `xml:"device_id,attr,omitempty"`
	Width                string `xml:"width,attr,omitempty"`
	Height               string `xml:"height,attr,omitempty"`
	FPS                  string `xml:"fps,attr,omitempty"`
	Interlaced           string `xml:"interlaced,attr,omitempty"`
	FieldOrder           string `xml:"field_order,attr,omitempty"`
	Background           string `xml:"background,attr,omitempty"`
	ServiceName          string `xml:"service_name,attr,omitempty"`
	ProviderName         string `xml:"provider_name,attr,omitempty"`
	ServiceID            string `xml:"service_id,attr,omitempty"`
	TransportStreamID    string `xml:"transport_stream_id,attr,omitempty"`
}

type cgenLadderXML struct {
	Videos []cgenVideoRenditionXML `xml:"video"`
	Audios []cgenAudioRenditionXML `xml:"audio"`
}

type cgenVideoRenditionXML struct {
	ID          string `xml:"id,attr"`
	Enabled     string `xml:"enabled,attr,omitempty"`
	Width       string `xml:"width,attr,omitempty"`
	Height      string `xml:"height,attr,omitempty"`
	FPS         string `xml:"fps,attr,omitempty"`
	Interlaced  string `xml:"interlaced,attr,omitempty"`
	FieldOrder  string `xml:"field_order,attr,omitempty"`
	Standard    string `xml:"standard,attr,omitempty"`
	VCodec      string `xml:"vcodec,attr,omitempty"`
	BitrateKbps string `xml:"bitrate_kbps,attr,omitempty"`
	Program     string `xml:"program,attr,omitempty"`
	VideoPID    string `xml:"video_pid,attr,omitempty"`
	PMTPID      string `xml:"pmt_pid,attr,omitempty"`
}

type cgenAudioRenditionXML struct {
	ID          string `xml:"id,attr"`
	Enabled     string `xml:"enabled,attr,omitempty"`
	Channels    string `xml:"channels,attr,omitempty"`
	ACodec      string `xml:"acodec,attr,omitempty"`
	BitrateKbps string `xml:"bitrate_kbps,attr,omitempty"`
	Language    string `xml:"language,attr,omitempty"`
	Program     string `xml:"program,attr,omitempty"`
	AudioPID    string `xml:"audio_pid,attr,omitempty"`
	PMTPID      string `xml:"pmt_pid,attr,omitempty"`
}

type cgenEncodersXML struct {
	XMLName       xml.Name               `xml:"cgenEncoders"`
	SchemaVersion string                 `xml:"schema_version,attr,omitempty"`
	UpdatedAt     string                 `xml:"updated_at,attr,omitempty"`
	Feeds         []cgenEncoderFeedXML   `xml:"feed"`
	Outputs       []cgenEncoderOutputXML `xml:"output"`
}

type cgenEncoderFeedXML struct {
	ID        string              `xml:"id,attr"`
	Video     cgenEncoderCodecXML `xml:"video"`
	Audio     cgenEncoderCodecXML `xml:"audio"`
	UpdatedAt string              `xml:"updated_at,attr,omitempty"`
}

type cgenEncoderOutputXML struct {
	ID        string              `xml:"id,attr"`
	Video     cgenEncoderCodecXML `xml:"video"`
	Audio     cgenEncoderCodecXML `xml:"audio"`
	UpdatedAt string              `xml:"updated_at,attr,omitempty"`
}

type cgenEncoderCodecXML struct {
	Codec       string                 `xml:"codec,attr,omitempty"`
	BitrateKbps string                 `xml:"bitrate_kbps,attr,omitempty"`
	GOP         string                 `xml:"gop,attr,omitempty"`
	BFrames     string                 `xml:"bframes,attr,omitempty"`
	Preset      string                 `xml:"preset,attr,omitempty"`
	Tune        string                 `xml:"tune,attr,omitempty"`
	Profile     string                 `xml:"profile,attr,omitempty"`
	Level       string                 `xml:"level,attr,omitempty"`
	Options     []cgenEncoderOptionXML `xml:"option"`
}

type cgenEncoderOptionXML struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr,omitempty"`
}

type cgenPriorityInputXML struct {
	FeedID      string `xml:"feed_id,attr,omitempty"`
	AudioSource string `xml:"audio_source,attr,omitempty"`
	Format      string `xml:"format,attr,omitempty"`
}

func (f cgenFeedXML) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name.Local = "feed"
	start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "id"}, Value: f.ID})
	if f.Name != "" {
		start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "name"}, Value: f.Name})
	}
	if f.Enabled != "" {
		start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "enabled"}, Value: f.Enabled})
	}
	if f.UpdatedAt != "" {
		start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "updated_at"}, Value: f.UpdatedAt})
	}
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	if err := e.EncodeElement(struct {
		Input  cgenEndpointXML `xml:"input"`
		Output cgenEndpointXML `xml:"output"`
	}{
		Input:  f.ProgramInput,
		Output: f.ProgramOutput,
	}, xml.StartElement{Name: xml.Name{Local: "program"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(struct {
		Input cgenPriorityInputXML `xml:"input"`
	}{
		Input: f.PriorityInput,
	}, xml.StartElement{Name: xml.Name{Local: "priority"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(struct {
		Video cgenVideoXML `xml:"video"`
		Audio cgenAudioXML `xml:"audio"`
	}{
		Video: f.Video,
		Audio: f.Audio,
	}, xml.StartElement{Name: xml.Name{Local: "media"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(f.Ladder, xml.StartElement{Name: xml.Name{Local: "ladder"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(struct {
		Banner   cgenBannerXML   `xml:"banner"`
		Graphics cgenGraphicsXML `xml:"graphics"`
		Clock    cgenClockXML    `xml:"clock"`
		Text     cgenTextXML     `xml:"text"`
		State    cgenStateXML    `xml:"state"`
		Standby  cgenStandbyXML  `xml:"standby"`
	}{
		Banner:   f.Banner,
		Graphics: f.Graphics,
		Clock:    f.Clock,
		Text:     f.Text,
		State:    f.State,
		Standby:  f.Standby,
	}, xml.StartElement{Name: xml.Name{Local: "presentation"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(f.Sync, xml.StartElement{Name: xml.Name{Local: "sync"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(f.Alert, xml.StartElement{Name: xml.Name{Local: "alert"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(f.Ancillary, xml.StartElement{Name: xml.Name{Local: "ancillary"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(f.Compositor, xml.StartElement{Name: xml.Name{Local: "compositor"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(f.ProgramMap, xml.StartElement{Name: xml.Name{Local: "programMapping"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(f.Outputs, xml.StartElement{Name: xml.Name{Local: "outputs"}}); err != nil {
		return err
	}
	return e.EncodeToken(start.End())
}

type cgenVideoXML struct {
	Width      string `xml:"width,attr,omitempty"`
	Height     string `xml:"height,attr,omitempty"`
	FPS        string `xml:"fps,attr,omitempty"`
	Interlaced string `xml:"interlaced,attr,omitempty"`
	FieldOrder string `xml:"field_order,attr,omitempty"`
	Standard   string `xml:"standard,attr,omitempty"`
}

type cgenAudioXML struct {
	Idle               string `xml:"idle,attr,omitempty"`
	AlertMode          string `xml:"alert_mode,attr,omitempty"`
	MuteStandbyRoutine string `xml:"mute_standby_routine,attr,omitempty"`
	Topology           string `xml:"topology,attr,omitempty"`
	ForceLayout        string `xml:"force_layout,attr,omitempty"`
	IdleProgramGainDB  string `xml:"idle_program_gain_db,attr,omitempty"`
	AlertProgramGainDB string `xml:"alert_program_gain_db,attr,omitempty"`
	AlertGainDB        string `xml:"alert_gain_db,attr,omitempty"`
	TransitionMS       string `xml:"transition_ms,attr,omitempty"`
}

type cgenAlertXML struct {
	FeedID string `xml:"feed_id,attr,omitempty"`
}

type cgenAncillaryXML struct {
	Captions string `xml:"captions,attr,omitempty"`
	SCTE35   string `xml:"scte35,attr,omitempty"`
	SCTE104  string `xml:"scte104,attr,omitempty"`
}

type cgenCompositorXML struct {
	AlertSceneID string `xml:"alert_scene_id,attr,omitempty"`
	Engine       string `xml:"engine,attr,omitempty"`
}

type cgenProgramMappingXML struct {
	Mode              string                   `xml:"mode,attr,omitempty"`
	TransportStreamID string                   `xml:"transport_stream_id,attr,omitempty"`
	Programs          []cgenProgramMapEntryXML `xml:"program"`
}

type cgenProgramMapEntryXML struct {
	Number       string                   `xml:"number,attr,omitempty"`
	ServiceName  string                   `xml:"service_name,attr,omitempty"`
	ProviderName string                   `xml:"provider_name,attr,omitempty"`
	PMTPID       string                   `xml:"pmt_pid,attr,omitempty"`
	VideoPID     string                   `xml:"video_pid,attr,omitempty"`
	Audio        []cgenProgramAudioMapXML `xml:"audio"`
	SCTE35       *cgenProgramSCTE35XML    `xml:"scte35,omitempty"`
}

type cgenProgramAudioMapXML struct {
	TrackID string `xml:"track_id,attr,omitempty"`
	PID     string `xml:"pid,attr,omitempty"`
}

type cgenProgramSCTE35XML struct {
	Input              string `xml:"input,attr,omitempty"`
	GeneratedAlertCues string `xml:"generated_alert_cues,attr,omitempty"`
	PID                string `xml:"pid,attr,omitempty"`
}

type cgenOutputsXML struct {
	Outputs []cgenOutputXML `xml:"output"`
}

type cgenOutputXML struct {
	ID                  string `xml:"id,attr,omitempty"`
	Enabled             string `xml:"enabled,attr,omitempty"`
	Destination         string `xml:"destination,attr,omitempty"`
	URL                 string `xml:"url,attr,omitempty"`
	VideoURL            string `xml:"video_url,attr,omitempty"`
	AudioURLs           string `xml:"audio_urls,attr,omitempty"`
	Container           string `xml:"container,attr,omitempty"`
	LatencyMS           string `xml:"latency_ms,attr,omitempty"`
	VideoCodec          string `xml:"video_codec,attr,omitempty"`
	RateControl         string `xml:"rate_control,attr,omitempty"`
	VideoBitrateKbps    string `xml:"video_bitrate_kbps,attr,omitempty"`
	VideoMaxBitrateKbps string `xml:"video_max_bitrate_kbps,attr,omitempty"`
	GOPFrames           string `xml:"gop_frames,attr,omitempty"`
	AudioCodec          string `xml:"audio_codec,attr,omitempty"`
	AudioBitrateKbps    string `xml:"audio_bitrate_kbps,attr,omitempty"`
	SampleRate          string `xml:"sample_rate,attr,omitempty"`
}

type cgenBannerXML struct {
	Mode              string `xml:"mode,attr,omitempty"`
	TickerHeight      string `xml:"ticker_height,attr,omitempty"`
	Font              string `xml:"font,attr,omitempty"`
	FontWeight        string `xml:"font_weight,attr,omitempty"`
	FontSize          string `xml:"font_size,attr,omitempty"`
	ScrollSpeed       string `xml:"scroll_speed,attr,omitempty"`
	ScrollRepeatMode  string `xml:"scroll_repeat_mode,attr,omitempty"`
	AfterEOMRepeats   string `xml:"after_eom_repeats,attr,omitempty"`
	FixedRepeats      string `xml:"fixed_repeats,attr,omitempty"`
	BackgroundEnabled string `xml:"background_enabled,attr,omitempty"`
}

type cgenBannerFontXML struct {
	Family string `xml:"family,attr,omitempty"`
	Weight string `xml:"weight,attr,omitempty"`
	Size   string `xml:"size,attr,omitempty"`
}

type cgenBannerTickerXML struct {
	Height string              `xml:"height,attr,omitempty"`
	Speed  string              `xml:"speed,attr,omitempty"`
	Repeat cgenTickerRepeatXML `xml:"repeat"`
}

type cgenTickerRepeatXML struct {
	Mode     string `xml:"mode,attr,omitempty"`
	AfterEOM string `xml:"after_eom,attr,omitempty"`
	Count    string `xml:"count,attr,omitempty"`
}

func (b cgenBannerXML) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "mode"}, Value: b.Mode})
	start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "background_enabled"}, Value: b.BackgroundEnabled})
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	if err := e.EncodeElement(cgenBannerFontXML{
		Family: b.Font,
		Weight: b.FontWeight,
		Size:   b.FontSize,
	}, xml.StartElement{Name: xml.Name{Local: "font"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(cgenBannerTickerXML{
		Height: b.TickerHeight,
		Speed:  b.ScrollSpeed,
		Repeat: cgenTickerRepeatXML{
			Mode:     b.ScrollRepeatMode,
			AfterEOM: b.AfterEOMRepeats,
			Count:    b.FixedRepeats,
		},
	}, xml.StartElement{Name: xml.Name{Local: "ticker"}}); err != nil {
		return err
	}
	return e.EncodeToken(start.End())
}

func (b *cgenBannerXML) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	for _, attr := range start.Attr {
		switch attr.Name.Local {
		case "mode":
			b.Mode = attr.Value
		case "ticker_height":
			b.TickerHeight = attr.Value
		case "font":
			b.Font = attr.Value
		case "font_weight":
			b.FontWeight = attr.Value
		case "font_size":
			b.FontSize = attr.Value
		case "scroll_speed":
			b.ScrollSpeed = attr.Value
		case "scroll_repeat_mode":
			b.ScrollRepeatMode = attr.Value
		case "after_eom_repeats":
			b.AfterEOMRepeats = attr.Value
		case "fixed_repeats":
			b.FixedRepeats = attr.Value
		case "background_enabled":
			b.BackgroundEnabled = attr.Value
		}
	}
	for {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		switch tok := tok.(type) {
		case xml.StartElement:
			switch tok.Name.Local {
			case "font":
				var font cgenBannerFontXML
				if err := d.DecodeElement(&font, &tok); err != nil {
					return err
				}
				b.Font = fallbackText(font.Family, b.Font)
				b.FontWeight = fallbackText(font.Weight, b.FontWeight)
				b.FontSize = fallbackText(font.Size, b.FontSize)
			case "ticker":
				var ticker cgenBannerTickerXML
				if err := d.DecodeElement(&ticker, &tok); err != nil {
					return err
				}
				b.TickerHeight = fallbackText(ticker.Height, b.TickerHeight)
				b.ScrollSpeed = fallbackText(ticker.Speed, b.ScrollSpeed)
				b.ScrollRepeatMode = fallbackText(ticker.Repeat.Mode, b.ScrollRepeatMode)
				b.AfterEOMRepeats = fallbackText(ticker.Repeat.AfterEOM, b.AfterEOMRepeats)
				b.FixedRepeats = fallbackText(ticker.Repeat.Count, b.FixedRepeats)
			default:
				var discard struct{}
				if err := d.DecodeElement(&discard, &tok); err != nil {
					return err
				}
			}
		case xml.EndElement:
			if tok.Name.Local == start.Name.Local {
				return nil
			}
		}
	}
}

type cgenGraphicsXML struct {
	Font         string `xml:"font,attr,omitempty"`
	FontWeight   string `xml:"font_weight,attr,omitempty"`
	FontSize     string `xml:"font_size,attr,omitempty"`
	BannerHeight string `xml:"banner_height,attr,omitempty"`
}

type cgenClockXML struct {
	Enabled  string `xml:"enabled,attr,omitempty"`
	Format   string `xml:"format,attr,omitempty"`
	X        string `xml:"x,attr,omitempty"`
	Y        string `xml:"y,attr,omitempty"`
	FontSize string `xml:"font_size,attr,omitempty"`
	Color    string `xml:"color,attr,omitempty"`
}

type cgenTextXML struct {
	Enabled  string `xml:"enabled,attr,omitempty"`
	FontSize string `xml:"font_size,attr,omitempty"`
	Color    string `xml:"color,attr,omitempty"`
	Content  string `xml:",chardata"`
}

type cgenStateXML struct {
	Mode      string `xml:"mode,attr,omitempty"`
	SMPTEBars string `xml:"smpte_bars,attr,omitempty"`
	SunnyCat  string `xml:"sunny_cat,attr,omitempty"`
	UpdatedAt string `xml:"updated_at,attr,omitempty"`
}

type cgenStandbyXML struct {
	Mode     string `xml:"mode,attr,omitempty"`
	Text     string `xml:"text,attr,omitempty"`
	FontSize string `xml:"font_size,attr,omitempty"`
	YPercent string `xml:"y_percent,attr,omitempty"`
}

type cgenSyncXML struct {
	HardResetMS            string `xml:"hard_reset_ms,attr,omitempty"`
	MaxAudioFramesPerVideo string `xml:"max_audio_frames_per_video,attr,omitempty"`
	SourceBufferMS         string `xml:"source_buffer_ms,attr,omitempty"`
	ReconnectInitialMS     string `xml:"reconnect_initial_ms,attr,omitempty"`
	ReconnectMaxMS         string `xml:"reconnect_max_ms,attr,omitempty"`
	StatusIntervalMS       string `xml:"status_interval_ms,attr,omitempty"`
}

func loadCgenPayload(configPath string) (map[string]any, error) {
	cgenConfigMu.RLock()
	defer cgenConfigMu.RUnlock()
	return loadCgenPayloadUnlocked(configPath)
}

func loadCgenPayloadUnlocked(configPath string) (map[string]any, error) {
	path := cgenPath(configPath)
	config, err := readCgenXML(path)
	if err != nil {
		return nil, err
	}
	encoderPath := cgenEncodersPath(configPath)
	encoders, err := readCgenEncodersXML(encoderPath)
	if err != nil {
		return nil, err
	}
	return cgenPayload(configPath, path, config, encoderPath, encoders), nil
}

func saveCgenPayload(configPath string, payload map[string]any) (map[string]any, error) {
	cgenConfigMu.Lock()
	defer cgenConfigMu.Unlock()

	path := cgenPath(configPath)
	encoderPath := cgenEncodersPath(configPath)
	if err := checkCgenExpectedRevision(path, encoderPath, payload); err != nil {
		return nil, err
	}
	rawFeeds, err := cgenObjectSlice(payload["feeds"], "cgen feeds")
	if err != nil || rawFeeds == nil {
		return nil, errors.New("cgen feeds payload is required and must be an array")
	}
	config := cgenXML{SchemaVersion: "2", Enabled: boolText(boolFromAny(payload["enabled"], true))}
	encoders := cgenEncodersXML{SchemaVersion: "2", UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
	for _, raw := range rawFeeds {
		feed, err := cgenFeedFromMap(raw)
		if err != nil {
			return nil, err
		}
		if normalizeCgenAudioTopology(feed.Audio.Topology) == "preserve_native_tracks" && !cgenPreserveNativeTracksAvailable {
			return nil, fmt.Errorf("cgen feed %q preserve-native audio is unavailable in the current media backend; select force_layout", feed.ID)
		}
		config.Feeds = append(config.Feeds, feed)
		encoderFeed, err := cgenEncoderFeedFromMap(raw, feed)
		if err != nil {
			return nil, err
		}
		encoders.Feeds = append(encoders.Feeds, encoderFeed)
		outputEncoders, err := cgenEncoderOutputsFromMap(raw, feed)
		if err != nil {
			return nil, err
		}
		encoders.Outputs = append(encoders.Outputs, outputEncoders...)
	}
	if err := writeCgenXML(path, config); err != nil {
		return nil, err
	}
	if err := writeCgenEncodersXML(encoderPath, encoders); err != nil {
		return nil, err
	}
	return loadCgenPayloadUnlocked(configPath)
}

func cgenActionPayload(configPath string, payload map[string]any) (map[string]any, error) {
	cgenConfigMu.Lock()
	defer cgenConfigMu.Unlock()

	feedID := strings.TrimSpace(stringValue(payload, "feed_id"))
	action := strings.ToLower(strings.TrimSpace(stringValue(payload, "action")))
	if feedID == "" {
		return nil, fmt.Errorf("feed_id is required")
	}
	if action == "" {
		return nil, fmt.Errorf("action is required")
	}
	path := cgenPath(configPath)
	config, err := readCgenXML(path)
	if err != nil {
		return nil, err
	}
	index := -1
	for i, feed := range config.Feeds {
		if feed.ID == feedID {
			index = i
			break
		}
	}
	if index < 0 {
		return nil, fmt.Errorf("cgen feed %q was not found", feedID)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	feed := config.Feeds[index]
	applyCgenRuntimeFields(&feed, payload)
	switch action {
	case "release":
		feed.State.Mode = "release"
		feed.State.SMPTEBars = "false"
		feed.State.SunnyCat = "false"
		feed.Clock.Enabled = "false"
		feed.Text.Enabled = "false"
	case "overlay":
		feed.State.Mode = "overlay"
	case "smpte_bars":
		feed.State.Mode = "overlay"
		feed.State.SMPTEBars = boolText(boolFromAny(payload["enabled"], true))
	case "clock":
		feed.Clock.Enabled = boolText(boolFromAny(payload["enabled"], true))
	case "unleash_sunny":
		feed.State.Mode = "overlay"
		feed.State.SunnyCat = "true"
	case "banish_sunny":
		feed.State.SunnyCat = "false"
	case "insert_text":
		feed.State.Mode = "overlay"
		feed.Text.Enabled = "true"
		feed.Text.Content = stringValue(payload, "text")
	case "clear_text":
		feed.Text.Enabled = "false"
		feed.Text.Content = ""
	default:
		return nil, fmt.Errorf("unsupported cgen action %q", action)
	}
	feed.State.UpdatedAt = now
	feed.UpdatedAt = now
	config.Feeds[index] = feed
	if err := writeCgenXML(path, config); err != nil {
		return nil, err
	}
	return loadCgenPayloadUnlocked(configPath)
}

func checkCgenExpectedRevision(path string, encoderPath string, payload map[string]any) error {
	_, err := os.Stat(filepath.Clean(path))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("inspect cgen config: %w", err)
	}
	if os.IsNotExist(err) {
		return nil
	}
	raw, ok := payload["expected_revision"]
	if !ok {
		return errors.New("expected_revision is required when cgen config exists")
	}
	expected, ok := raw.(string)
	if !ok {
		return errors.New("expected_revision must be a string")
	}
	expected = strings.TrimSpace(expected)
	if len(expected) != sha256.Size*2 {
		return errors.New("expected_revision is invalid")
	}
	if _, err := hex.DecodeString(expected); err != nil {
		return errors.New("expected_revision is invalid")
	}
	current, err := cgenConfigRevision(path, encoderPath)
	if err != nil {
		return err
	}
	if expected != current {
		return errors.New("cgen configuration revision conflict")
	}
	return nil
}

func cgenConfigRevision(path string, encoderPath string) (string, error) {
	hash := sha256.New()
	for _, entry := range []struct {
		label string
		path  string
	}{
		{label: "cgen", path: path},
		{label: "encoders", path: encoderPath},
	} {
		_, _ = io.WriteString(hash, entry.label)
		_, _ = hash.Write([]byte{0})
		maximum := maxCgenConfigBytes
		if entry.label == "encoders" {
			maximum = maxCgenEncodersBytes
		}
		raw, err := readCgenManagedFile(entry.path, maximum)
		if err != nil {
			if os.IsNotExist(err) {
				_, _ = hash.Write([]byte{0})
				continue
			}
			return "", fmt.Errorf("read cgen configuration revision: %w", err)
		}
		_, _ = hash.Write(raw)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func applyCgenRuntimeFields(feed *cgenFeedXML, source map[string]any) {
	feed.Banner.Mode = fallbackText(stringFromAny(source["banner_mode"]), feed.Banner.Mode)
	feed.Banner.TickerHeight = fallbackText(firstStringFromAny(source, "ticker_height", "banner_height"), feed.Banner.TickerHeight)
	feed.Banner.Font = fallbackText(stringFromAny(source["font"]), feed.Banner.Font)
	feed.Banner.FontWeight = fallbackText(stringFromAny(source["font_weight"]), feed.Banner.FontWeight)
	feed.Banner.FontSize = fallbackText(stringFromAny(source["font_size"]), feed.Banner.FontSize)
	feed.Banner.ScrollSpeed = fallbackText(stringFromAny(source["scroll_speed"]), feed.Banner.ScrollSpeed)
	feed.Banner.ScrollRepeatMode = fallbackText(stringFromAny(source["scroll_repeat_mode"]), feed.Banner.ScrollRepeatMode)
	feed.Banner.AfterEOMRepeats = fallbackText(stringFromAny(source["after_eom_repeats"]), feed.Banner.AfterEOMRepeats)
	feed.Banner.FixedRepeats = fallbackText(stringFromAny(source["fixed_repeats"]), feed.Banner.FixedRepeats)
	if _, ok := source["banner_background_enabled"]; ok {
		feed.Banner.BackgroundEnabled = boolText(boolFromAny(source["banner_background_enabled"], true))
	}
	feed.Graphics.Font = fallbackText(stringFromAny(source["font"]), feed.Graphics.Font)
	feed.Graphics.FontWeight = fallbackText(stringFromAny(source["font_weight"]), feed.Graphics.FontWeight)
	feed.Graphics.FontSize = fallbackText(stringFromAny(source["font_size"]), feed.Graphics.FontSize)
	feed.Graphics.BannerHeight = fallbackText(firstStringFromAny(source, "banner_height", "ticker_height"), feed.Graphics.BannerHeight)
	if _, ok := source["clock_enabled"]; ok {
		feed.Clock.Enabled = boolText(boolFromAny(source["clock_enabled"], false))
	}
	feed.Clock.Format = fallbackText(stringFromAny(source["clock_format"]), feed.Clock.Format)
	feed.Clock.X = fallbackText(stringFromAny(source["clock_x"]), feed.Clock.X)
	feed.Clock.Y = fallbackText(stringFromAny(source["clock_y"]), feed.Clock.Y)
	feed.Clock.FontSize = fallbackText(stringFromAny(source["clock_font_size"]), feed.Clock.FontSize)
	feed.Clock.Color = fallbackText(stringFromAny(source["clock_color"]), feed.Clock.Color)
	if _, ok := source["text_enabled"]; ok {
		feed.Text.Enabled = boolText(boolFromAny(source["text_enabled"], false))
	}
	if _, ok := source["text"]; ok {
		feed.Text.Content = stringFromAny(source["text"])
	}
	feed.Text.FontSize = fallbackText(stringFromAny(source["text_font_size"]), feed.Text.FontSize)
	feed.Text.Color = fallbackText(stringFromAny(source["text_color"]), feed.Text.Color)
	feed.State.Mode = fallbackText(stringFromAny(source["mode"]), feed.State.Mode)
	if _, ok := source["smpte_bars"]; ok {
		feed.State.SMPTEBars = boolText(boolFromAny(source["smpte_bars"], false))
	}
	if _, ok := source["sunny_cat"]; ok {
		feed.State.SunnyCat = boolText(boolFromAny(source["sunny_cat"], false))
	}
	feed.Standby.Mode = fallbackText(stringFromAny(source["standby_mode"]), feed.Standby.Mode)
	feed.Standby.Text = fallbackText(stringFromAny(source["standby_text"]), feed.Standby.Text)
	feed.Standby.FontSize = fallbackText(stringFromAny(source["standby_font_size"]), feed.Standby.FontSize)
	feed.Standby.YPercent = fallbackText(stringFromAny(source["standby_y_percent"]), feed.Standby.YPercent)
}

func cgenPath(configPath string) string {
	return resolveConfigPath(configPath, defaultCgenFile)
}

func cgenEncodersPath(configPath string) string {
	return resolveConfigPath(configPath, defaultCgenEncodersFile)
}

func readCgenXML(path string) (cgenXML, error) {
	raw, err := readCgenManagedFile(path, maxCgenConfigBytes)
	if err != nil {
		if os.IsNotExist(err) {
			return cgenXML{SchemaVersion: "2", Enabled: "true"}, nil
		}
		return cgenXML{}, err
	}
	if err := rejectLegacyCgenXML(raw, path); err != nil {
		return cgenXML{}, err
	}
	var config cgenXML
	if err := xml.Unmarshal(raw, &config); err != nil {
		return cgenXML{}, fmt.Errorf("parse cgen XML: %w", err)
	}
	return normalizeCgen(config)
}

func readCgenEncodersXML(path string) (cgenEncodersXML, error) {
	raw, err := readCgenManagedFile(path, maxCgenEncodersBytes)
	if err != nil {
		if os.IsNotExist(err) {
			return cgenEncodersXML{SchemaVersion: "2"}, nil
		}
		return cgenEncodersXML{}, err
	}
	var config cgenEncodersXML
	if err := xml.Unmarshal(raw, &config); err != nil {
		return cgenEncodersXML{}, fmt.Errorf("parse cgen encoders XML: %w", err)
	}
	return normalizeCgenEncoders(config)
}

func writeCgenEncodersXML(path string, config cgenEncodersXML) error {
	var err error
	config, err = normalizeCgenEncoders(config)
	if err != nil {
		return err
	}
	raw, err := xml.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return writeCgenManagedFileAtomic(path, []byte(xml.Header+string(raw)+"\n"), maxCgenEncodersBytes)
}

func rejectLegacyCgenXML(raw []byte, path string) error {
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	stack := []string{}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("scan cgen XML %s: %w", path, err)
		}
		switch tok := token.(type) {
		case xml.StartElement:
			name := tok.Name.Local
			parent := ""
			if len(stack) > 0 {
				parent = stack[len(stack)-1]
			}
			if legacyCgenElement(parent, name) {
				return fmt.Errorf("legacy cgen <%s> element is no longer supported in %s; use programInput, priorityInput, and programOutput", name, path)
			}
			for _, attr := range tok.Attr {
				if legacyCgenAttribute(name, attr.Name.Local) {
					return fmt.Errorf("legacy cgen %s @%s attribute is no longer supported in %s", name, attr.Name.Local, path)
				}
			}
			stack = append(stack, name)
		case xml.EndElement:
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		default:
			continue
		}
	}
}

func legacyCgenElement(parent string, element string) bool {
	return (element == "output" && parent != "program" && parent != "outputs") ||
		element == "alertOutput" ||
		(element == "input" && parent != "program" && parent != "priority")
}

func legacyCgenAttribute(element string, attr string) bool {
	switch element {
	case "cgen":
		return attr == "graphics_backend" || attr == "ffmpeg"
	case "priorityInput":
		return attr == "url"
	case "audio":
		return attr == "duck_db"
	case "banner":
		return attr == "x" || attr == "y"
	case "graphics":
		switch attr {
		case "background_color", "text_x", "text_y", "banner_x", "banner_y", "banner_width":
			return true
		}
	}
	return false
}

func writeCgenXML(path string, config cgenXML) error {
	config, err := normalizeCgen(config)
	if err != nil {
		return err
	}
	raw, err := xml.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return writeCgenManagedFileAtomic(path, []byte(xml.Header+string(raw)+"\n"), maxCgenConfigBytes)
}

func readCgenManagedFile(path string, maximum int64) ([]byte, error) {
	clean := filepath.Clean(path)
	info, err := os.Lstat(clean)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("cgen managed configuration must be a regular file")
	}
	if info.Size() > maximum {
		return nil, errors.New("cgen managed configuration exceeds its safety limit")
	}
	file, err := os.Open(clean)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maximum {
		return nil, errors.New("cgen managed configuration exceeds its safety limit")
	}
	return raw, nil
}

func writeCgenManagedFileAtomic(path string, raw []byte, maximum int64) error {
	if int64(len(raw)) > maximum {
		return errors.New("cgen managed configuration exceeds its safety limit")
	}
	clean := filepath.Clean(path)
	directory := filepath.Dir(clean)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	if info, err := os.Lstat(clean); err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("cgen managed configuration target must be a regular file")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".cgen-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	keepTemporary := true
	defer func() {
		if keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := replaceCgenFileAtomically(temporaryPath, clean); err != nil {
		return err
	}
	keepTemporary = false
	return syncCgenDirectory(directory)
}

func normalizeCgen(config cgenXML) (cgenXML, error) {
	config.SchemaVersion = strings.TrimSpace(config.SchemaVersion)
	if config.SchemaVersion == "" {
		config.SchemaVersion = "1"
	}
	if config.SchemaVersion != "1" && config.SchemaVersion != "2" {
		return cgenXML{}, fmt.Errorf("unsupported cgen schema_version %q", config.SchemaVersion)
	}
	config.Enabled = boolText(xmlBool(config.Enabled, true))
	seen := map[string]struct{}{}
	seenOutputs := map[string]string{}
	for i := range config.Feeds {
		feed := &config.Feeds[i]
		feed.ID = strings.TrimSpace(feed.ID)
		if !validCgenIdentifier(feed.ID) {
			return cgenXML{}, fmt.Errorf("cgen feed id %q is invalid", feed.ID)
		}
		feedKey := strings.ToLower(feed.ID)
		if _, ok := seen[feedKey]; ok {
			return cgenXML{}, fmt.Errorf("duplicate cgen feed id %q", feed.ID)
		}
		seen[feedKey] = struct{}{}
		feed.Name = strings.TrimSpace(feed.Name)
		if feed.Name == "" {
			feed.Name = feed.ID
		}
		feed.Enabled = boolText(xmlBool(feed.Enabled, false))
		rawDecoder := strings.TrimSpace(feed.ProgramInput.HardwareDecoder)
		rawInput := feed.ProgramInput
		if !validCgenInputTypeToken(rawInput.Type) {
			return cgenXML{}, fmt.Errorf("cgen feed %q input type is unsupported", feed.ID)
		}
		if !validCgenDeinterlaceAlgorithm(rawInput.DeinterlaceAlgorithm) {
			return cgenXML{}, fmt.Errorf("cgen feed %q deinterlace algorithm is unsupported", feed.ID)
		}
		if !validCgenDeinterlaceBackend(rawInput.DeinterlaceBackend) {
			return cgenXML{}, fmt.Errorf("cgen feed %q deinterlace backend is unsupported", feed.ID)
		}
		if !validCgenDeinterlaceCadence(rawInput.DeinterlaceCadence) {
			return cgenXML{}, fmt.Errorf("cgen feed %q deinterlace cadence is unsupported", feed.ID)
		}
		if !validCgenDeinterlaceParity(rawInput.DeinterlaceParity) {
			return cgenXML{}, fmt.Errorf("cgen feed %q deinterlace parity is unsupported", feed.ID)
		}
		feed.ProgramInput = cleanCgenEndpoint(feed.ProgramInput)
		feed.ProgramOutput = cleanCgenEndpoint(feed.ProgramOutput)
		feed.ProgramInput.DeinterlaceAlgorithm = fallbackText(feed.ProgramInput.DeinterlaceAlgorithm, "yadif")
		feed.ProgramInput.DeinterlaceBackend = fallbackText(feed.ProgramInput.DeinterlaceBackend, "software")
		feed.ProgramInput.DeinterlaceCadence = fallbackText(feed.ProgramInput.DeinterlaceCadence, "field")
		feed.ProgramInput.DeinterlaceParity = fallbackText(feed.ProgramInput.DeinterlaceParity, "auto")
		if !validCgenDeinterlaceCombination(feed.ProgramInput) {
			return cgenXML{}, fmt.Errorf(
				"cgen feed %q deinterlace algorithm, backend, and parity combination is unsupported",
				feed.ID,
			)
		}
		if rawDecoder != "" && feed.ProgramInput.HardwareDecoder == "" {
			return cgenXML{}, fmt.Errorf("cgen feed %q hardware decoder id is invalid", feed.ID)
		}
		var formatErr error
		feed.ProgramInput.Format, formatErr = normalizeCgenInputFormat(feed.ProgramInput.Format)
		if formatErr != nil {
			return cgenXML{}, fmt.Errorf("cgen feed %q: %w", feed.ID, formatErr)
		}
		if feed.ProgramInput.URL != "" && !validCgenLocation(feed.ProgramInput.URL) {
			return cgenXML{}, fmt.Errorf("cgen feed %q input location is invalid", feed.ID)
		}
		if feed.ProgramInput.DeviceID != "" && !validCgenLocation(feed.ProgramInput.DeviceID) {
			return cgenXML{}, fmt.Errorf("cgen feed %q device id is invalid", feed.ID)
		}
		inputType := normalizeCgenInputType(feed.ProgramInput.Type)
		if inputType == "dummy" {
			if strings.TrimSpace(rawInput.Width) != "" && cleanOptionalBoundedUint(rawInput.Width, 1, 16384) == "" {
				return cgenXML{}, fmt.Errorf("cgen feed %q dummy width is invalid", feed.ID)
			}
			if strings.TrimSpace(rawInput.Height) != "" && cleanOptionalBoundedUint(rawInput.Height, 1, 16384) == "" {
				return cgenXML{}, fmt.Errorf("cgen feed %q dummy height is invalid", feed.ID)
			}
			if strings.TrimSpace(rawInput.FPS) != "" && !validCgenRational(rawInput.FPS) {
				return cgenXML{}, fmt.Errorf("cgen feed %q dummy frame rate is invalid", feed.ID)
			}
			if strings.TrimSpace(rawInput.Background) != "" && !validCgenRGBA(rawInput.Background) {
				return cgenXML{}, fmt.Errorf("cgen feed %q dummy background is invalid", feed.ID)
			}
		}
		if inputType == "device" && feed.ProgramInput.DeviceID == "" {
			return cgenXML{}, fmt.Errorf("cgen feed %q device id is required", feed.ID)
		}
		if inputType == "device" && strings.TrimSpace(rawInput.DeviceBackend) != "" && feed.ProgramInput.DeviceBackend == "" {
			return cgenXML{}, fmt.Errorf("cgen feed %q device backend is unsupported", feed.ID)
		}
		if inputType == "uri_or_file" && feed.ProgramInput.URL == "" {
			return cgenXML{}, fmt.Errorf("cgen feed %q input URL or file is required", feed.ID)
		}
		feed.PriorityInput.FeedID = fallbackText(cleanCgenID(feed.PriorityInput.FeedID), feed.ID)
		feed.PriorityInput.AudioSource = normalizeCgenAudioSource(feed.PriorityInput.AudioSource)
		feed.PriorityInput.Format = fallbackText(strings.TrimSpace(feed.PriorityInput.Format), "priority-audio")
		feed.Video.Width = cleanPositive(feed.Video.Width, "1280")
		feed.Video.Height = cleanPositive(feed.Video.Height, "720")
		feed.Video.FPS = fallbackText(strings.TrimSpace(feed.Video.FPS), "source")
		feed.Video.Interlaced = boolText(xmlBool(feed.Video.Interlaced, false))
		feed.Video.FieldOrder = normalizeCgenFieldOrder(feed.Video.FieldOrder)
		feed.Video.Standard = normalizeCgenStandard(feed.Video.Standard)
		feed.Audio.Idle = fallbackText(strings.TrimSpace(feed.Audio.Idle), "source")
		feed.Audio.AlertMode = fallbackText(strings.TrimSpace(feed.Audio.AlertMode), "replace")
		feed.Audio.MuteStandbyRoutine = boolText(xmlBool(feed.Audio.MuteStandbyRoutine, true))
		if !validCgenAudioTopologyToken(feed.Audio.Topology) {
			return cgenXML{}, fmt.Errorf("cgen feed %q audio topology is invalid", feed.ID)
		}
		if !validCgenForceLayoutToken(feed.Audio.ForceLayout) {
			return cgenXML{}, fmt.Errorf("cgen feed %q forced audio layout is invalid", feed.ID)
		}
		for _, gain := range []struct {
			name  string
			value string
		}{
			{name: "idle program", value: feed.Audio.IdleProgramGainDB},
			{name: "alert program", value: feed.Audio.AlertProgramGainDB},
			{name: "alert", value: feed.Audio.AlertGainDB},
		} {
			if !validCgenGain(gain.value) {
				return cgenXML{}, fmt.Errorf("cgen feed %q %s gain is invalid", feed.ID, gain.name)
			}
		}
		feed.Audio.Topology = normalizeCgenAudioTopology(feed.Audio.Topology)
		feed.Audio.ForceLayout = normalizeCgenForceLayout(feed.Audio.ForceLayout)
		feed.Audio.IdleProgramGainDB = normalizeCgenGain(feed.Audio.IdleProgramGainDB, "0")
		feed.Audio.AlertProgramGainDB = normalizeCgenGain(feed.Audio.AlertProgramGainDB, "muted")
		feed.Audio.AlertGainDB = normalizeCgenGain(feed.Audio.AlertGainDB, "0")
		feed.Audio.TransitionMS = cleanBoundedUint(feed.Audio.TransitionMS, "20", 1, 5000)
		if feed.Audio.TransitionMS == "" {
			return cgenXML{}, fmt.Errorf("cgen feed %q audio transition is invalid", feed.ID)
		}
		normalizeCgenLadder(feed)
		if err := validateCgenLegacyPIDAssignments(feed); err != nil {
			return cgenXML{}, fmt.Errorf("cgen feed %q ladder: %w", feed.ID, err)
		}
		feed.Banner.Mode = fallbackText(strings.TrimSpace(feed.Banner.Mode), "auto")
		feed.Banner.TickerHeight = cleanPositive(feed.Banner.TickerHeight, "128")
		feed.Banner.Font = fallbackText(strings.TrimSpace(feed.Banner.Font), "Arial")
		feed.Banner.FontWeight = normalizeCgenFontWeight(feed.Banner.FontWeight)
		feed.Banner.FontSize = cleanPositive(feed.Banner.FontSize, "58")
		feed.Banner.ScrollSpeed = cleanPositive(feed.Banner.ScrollSpeed, "8")
		feed.Banner.ScrollRepeatMode = normalizeCgenScrollRepeatMode(feed.Banner.ScrollRepeatMode)
		feed.Banner.AfterEOMRepeats = cleanNonNegative(feed.Banner.AfterEOMRepeats, "0")
		feed.Banner.FixedRepeats = cleanPositive(feed.Banner.FixedRepeats, "1")
		feed.Banner.BackgroundEnabled = boolText(xmlBool(feed.Banner.BackgroundEnabled, true))
		feed.Graphics.Font = fallbackText(strings.TrimSpace(feed.Graphics.Font), feed.Banner.Font)
		feed.Graphics.FontWeight = normalizeCgenFontWeight(fallbackText(feed.Graphics.FontWeight, feed.Banner.FontWeight))
		feed.Graphics.FontSize = cleanPositive(feed.Graphics.FontSize, feed.Banner.FontSize)
		feed.Graphics.BannerHeight = cleanPositive(feed.Graphics.BannerHeight, feed.Banner.TickerHeight)
		feed.Clock.Enabled = boolText(xmlBool(feed.Clock.Enabled, false))
		feed.Clock.Format = fallbackText(strings.TrimSpace(feed.Clock.Format), "Jan 02 15:04:05")
		feed.Clock.X = cleanNumber(feed.Clock.X, "48")
		feed.Clock.Y = cleanNumber(feed.Clock.Y, "48")
		feed.Clock.FontSize = cleanPositive(feed.Clock.FontSize, "30")
		feed.Clock.Color = cleanColor(feed.Clock.Color, "#ffffff")
		feed.Text.Enabled = boolText(xmlBool(feed.Text.Enabled, false))
		feed.Text.FontSize = cleanPositive(feed.Text.FontSize, feed.Graphics.FontSize)
		feed.Text.Color = cleanColor(feed.Text.Color, "#ffffff")
		feed.Text.Content = strings.TrimSpace(feed.Text.Content)
		feed.State.Mode = normalizeCgenMode(feed.State.Mode)
		feed.State.SMPTEBars = boolText(xmlBool(feed.State.SMPTEBars, false))
		feed.State.SunnyCat = boolText(xmlBool(feed.State.SunnyCat, false))
		feed.State.UpdatedAt = strings.TrimSpace(feed.State.UpdatedAt)
		feed.Standby.Mode = normalizeCgenStandbyMode(feed.Standby.Mode)
		standbyText := "EAS Details Channel"
		if config.SchemaVersion == "2" {
			standbyText = "Emergency Alert Details Channel"
		}
		feed.Standby.Text = fallbackText(strings.TrimSpace(feed.Standby.Text), standbyText)
		feed.Standby.FontSize = cleanPositive(feed.Standby.FontSize, feed.Banner.FontSize)
		feed.Standby.YPercent = cleanPercent(feed.Standby.YPercent, "10")
		feed.Sync.HardResetMS = cleanPositive(feed.Sync.HardResetMS, "250")
		feed.Sync.MaxAudioFramesPerVideo = cleanPositive(feed.Sync.MaxAudioFramesPerVideo, "8")
		feed.Sync.SourceBufferMS = cleanPositive(feed.Sync.SourceBufferMS, "240")
		feed.Sync.ReconnectInitialMS = cleanPositive(feed.Sync.ReconnectInitialMS, "500")
		feed.Sync.ReconnectMaxMS = cleanPositive(feed.Sync.ReconnectMaxMS, "10000")
		feed.Sync.StatusIntervalMS = cleanPositive(feed.Sync.StatusIntervalMS, "750")
		feed.Alert.FeedID = strings.TrimSpace(feed.Alert.FeedID)
		if feed.Alert.FeedID != "" && feed.Alert.FeedID != "*" && !validCgenIdentifier(feed.Alert.FeedID) {
			return cgenXML{}, fmt.Errorf("cgen feed %q alert feed id is invalid", feed.ID)
		}
		if feed.Alert.FeedID == "" || feed.Alert.FeedID == "*" {
			feed.Alert.FeedID = feed.ID
		}
		for _, policy := range []struct {
			name  string
			value string
		}{
			{name: "captions", value: feed.Ancillary.Captions},
			{name: "scte35", value: feed.Ancillary.SCTE35},
			{name: "scte104", value: feed.Ancillary.SCTE104},
		} {
			if !validPassPolicyToken(policy.value) {
				return cgenXML{}, fmt.Errorf("cgen feed %q ancillary %s policy is invalid", feed.ID, policy.name)
			}
		}
		feed.Ancillary.Captions = normalizePassPolicy(feed.Ancillary.Captions)
		feed.Ancillary.SCTE35 = normalizePassPolicy(feed.Ancillary.SCTE35)
		feed.Ancillary.SCTE104 = normalizePassPolicy(feed.Ancillary.SCTE104)
		feed.Compositor.AlertSceneID = fallbackText(strings.TrimSpace(feed.Compositor.AlertSceneID), "Standard_Crawl")
		if !validCgenIdentifier(feed.Compositor.AlertSceneID) {
			return cgenXML{}, fmt.Errorf("cgen feed %q alert scene id is invalid", feed.ID)
		}
		if strings.EqualFold(feed.Compositor.AlertSceneID, "Program_Passthrough") || strings.EqualFold(feed.Compositor.AlertSceneID, "Standby") {
			return cgenXML{}, fmt.Errorf("cgen feed %q alert scene is not selectable", feed.ID)
		}
		if !validCgenCompositorEngine(feed.Compositor.Engine) {
			return cgenXML{}, fmt.Errorf("cgen feed %q compositor engine is invalid", feed.ID)
		}
		feed.Compositor.Engine = normalizeCgenCompositorEngine(feed.Compositor.Engine)
		if err := normalizeCgenProgramMapping(feed); err != nil {
			return cgenXML{}, fmt.Errorf("cgen feed %q program mapping: %w", feed.ID, err)
		}
		if err := normalizeCgenOutputs(feed); err != nil {
			return cgenXML{}, fmt.Errorf("cgen feed %q outputs: %w", feed.ID, err)
		}
		for _, output := range feed.Outputs.Outputs {
			key := strings.ToLower(output.ID)
			if otherFeed, exists := seenOutputs[key]; exists {
				return cgenXML{}, fmt.Errorf("duplicate output id %q in feeds %q and %q", output.ID, otherFeed, feed.ID)
			}
			seenOutputs[key] = feed.ID
		}
		feed.UpdatedAt = strings.TrimSpace(feed.UpdatedAt)
	}
	sort.SliceStable(config.Feeds, func(i, j int) bool { return strings.ToLower(config.Feeds[i].ID) < strings.ToLower(config.Feeds[j].ID) })
	return config, nil
}

func validateCgenLegacyPIDAssignments(feed *cgenFeedXML) error {
	type streamPID struct {
		kind    string
		program string
		pid     string
		pmtPID  string
	}
	streams := make([]streamPID, 0, len(feed.Ladder.Videos)+len(feed.Ladder.Audios))
	for _, video := range feed.Ladder.Videos {
		streams = append(streams, streamPID{kind: "video " + video.ID, program: video.Program, pid: video.VideoPID, pmtPID: video.PMTPID})
	}
	for _, audio := range feed.Ladder.Audios {
		streams = append(streams, streamPID{kind: "audio " + audio.ID, program: audio.Program, pid: audio.AudioPID, pmtPID: audio.PMTPID})
	}
	pmtByProgram := map[uint16]uint16{}
	pmtOwners := map[uint16]uint16{}
	elementaryOwners := map[uint16]string{}
	for _, stream := range streams {
		programValue, err := strconv.ParseUint(strings.TrimSpace(stream.program), 10, 16)
		if err != nil || programValue == 0 {
			return fmt.Errorf("%s program number is invalid", stream.kind)
		}
		program := uint16(programValue)
		pmt, err := parseAssignableCgenPID(stream.pmtPID)
		if err != nil {
			return fmt.Errorf("%s PMT PID is invalid", stream.kind)
		}
		if previous, exists := pmtByProgram[program]; exists && previous != pmt {
			return fmt.Errorf("program %d has conflicting PMT PIDs", program)
		}
		if owner, exists := pmtOwners[pmt]; exists && owner != program {
			return fmt.Errorf("PMT PID %d is shared by programs %d and %d", pmt, owner, program)
		}
		pmtByProgram[program] = pmt
		pmtOwners[pmt] = program
		pid, err := parseAssignableCgenPID(stream.pid)
		if err != nil {
			return fmt.Errorf("%s elementary PID is invalid", stream.kind)
		}
		if previous, exists := elementaryOwners[pid]; exists {
			return fmt.Errorf("elementary PID %d is shared by %s and %s", pid, previous, stream.kind)
		}
		elementaryOwners[pid] = stream.kind
	}
	for pid, stream := range elementaryOwners {
		if program, exists := pmtOwners[pid]; exists {
			return fmt.Errorf("PID %d collides between %s and program %d PMT", pid, stream, program)
		}
	}
	return nil
}

func parseAssignableCgenPID(value string) (uint16, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 16)
	if err != nil || parsed < 0x20 || parsed > 0x1ffe {
		return 0, errors.New("PID is outside the assignable range")
	}
	return uint16(parsed), nil
}

func normalizeCgenAudioTopology(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "preserve", "preserve_native", "preserve_native_tracks":
		return "preserve_native_tracks"
	default:
		return "force_layout"
	}
}

func validCgenAudioTopologyToken(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "force", "forced", "force_layout", "preserve", "preserve_native", "preserve_native_tracks":
		return true
	default:
		return false
	}
}

func normalizeCgenForceLayout(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "mono", "1", "1.0":
		return "mono"
	case "surround51", "surround_51", "5.1", "6":
		return "surround51"
	default:
		return "stereo"
	}
}

func validCgenForceLayoutToken(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "mono", "1", "1.0", "stereo", "2", "2.0", "surround51", "surround_51", "5.1", "6":
		return true
	default:
		return false
	}
}

func validCgenGain(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || value == "muted" || value == "mute" || value == "-inf" {
		return true
	}
	n, err := strconv.ParseFloat(value, 64)
	return err == nil && n >= -60 && n <= 12
}

func normalizeCgenGain(value string, fallback string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		value = fallback
	}
	if value == "muted" || value == "mute" || value == "-inf" {
		return "muted"
	}
	n, err := strconv.ParseFloat(value, 64)
	if err != nil || n < -60 || n > 12 {
		return fallback
	}
	return strconv.FormatFloat(n, 'f', -1, 64)
}

func normalizePassPolicy(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "pass", "preserve", "true", "on", "enabled":
		return "pass"
	default:
		return "drop"
	}
}

func validPassPolicyToken(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "pass", "preserve", "true", "on", "enabled", "drop", "false", "off", "disabled":
		return true
	default:
		return false
	}
}

func normalizeCgenCompositorEngine(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "scene_v2", "scene", "wgpu":
		return "scene_v2"
	default:
		return "legacy"
	}
}

func validCgenCompositorEngine(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "legacy", "scene_v2", "scene", "wgpu":
		return true
	default:
		return false
	}
}

func normalizeCgenProgramMapping(feed *cgenFeedXML) error {
	feed.ProgramMap.Mode = strings.ToLower(strings.TrimSpace(feed.ProgramMap.Mode))
	if feed.ProgramMap.Mode == "" {
		if len(feed.ProgramMap.Programs) > 0 {
			feed.ProgramMap.Mode = "manual"
		} else {
			feed.ProgramMap.Mode = "source"
		}
	}
	switch feed.ProgramMap.Mode {
	case "source", "auto", "manual":
	default:
		return errors.New("mode must be source, auto, or manual")
	}
	if len(feed.ProgramMap.Programs) == 0 {
		feed.ProgramMap.TransportStreamID = cleanBoundedUint(feed.ProgramMap.TransportStreamID, "1", 1, 65535)
		if feed.ProgramMap.TransportStreamID == "" {
			return errors.New("transport_stream_id must be between 1 and 65535")
		}
		return nil
	}
	feed.ProgramMap.TransportStreamID = cleanBoundedUint(feed.ProgramMap.TransportStreamID, "1", 1, 65535)
	if feed.ProgramMap.TransportStreamID == "" {
		return errors.New("transport_stream_id must be between 1 and 65535")
	}
	programs := make(map[string]struct{}, len(feed.ProgramMap.Programs))
	usedPIDs := map[uint16]string{}
	for index := range feed.ProgramMap.Programs {
		program := &feed.ProgramMap.Programs[index]
		program.Number = cleanBoundedUint(program.Number, "", 1, 65535)
		if program.Number == "" {
			return errors.New("program number must be between 1 and 65535")
		}
		if _, exists := programs[program.Number]; exists {
			return fmt.Errorf("duplicate program number %s", program.Number)
		}
		programs[program.Number] = struct{}{}
		program.ServiceName = cleanCgenText(program.ServiceName, "Haze CGEN", 128)
		program.ProviderName = cleanCgenText(program.ProviderName, "Haze", 128)
		var err error
		if program.PMTPID, err = normalizeCgenPID(program.PMTPID, "pmt_pid", usedPIDs); err != nil {
			return err
		}
		if program.VideoPID, err = normalizeCgenPID(program.VideoPID, "video_pid", usedPIDs); err != nil {
			return err
		}
		if program.VideoPID == "" && len(program.Audio) == 0 {
			return fmt.Errorf("program %s has no elementary streams", program.Number)
		}
		tracks := map[string]struct{}{}
		for audioIndex := range program.Audio {
			audio := &program.Audio[audioIndex]
			audio.TrackID = strings.TrimSpace(audio.TrackID)
			if !validCgenIdentifier(audio.TrackID) {
				return fmt.Errorf("program %s audio track id is invalid", program.Number)
			}
			key := strings.ToLower(audio.TrackID)
			if _, exists := tracks[key]; exists {
				return fmt.Errorf("program %s has duplicate audio track id %q", program.Number, audio.TrackID)
			}
			tracks[key] = struct{}{}
			if audio.PID, err = normalizeCgenPID(audio.PID, "audio_pid", usedPIDs); err != nil {
				return err
			}
		}
		if program.SCTE35 != nil {
			program.SCTE35.Input = normalizePassPolicy(program.SCTE35.Input)
			program.SCTE35.GeneratedAlertCues = boolText(xmlBool(program.SCTE35.GeneratedAlertCues, false))
			if program.SCTE35.PID, err = normalizeCgenPID(program.SCTE35.PID, "scte35_pid", usedPIDs); err != nil {
				return err
			}
		}
	}
	return nil
}

func normalizeCgenPID(value string, field string, used map[uint16]string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "auto") {
		return "auto", nil
	}
	var (
		parsed uint64
		err    error
	)
	if strings.HasPrefix(value, "0x") || strings.HasPrefix(value, "0X") {
		parsed, err = strconv.ParseUint(value[2:], 16, 16)
	} else {
		parsed, err = strconv.ParseUint(value, 10, 16)
	}
	if err != nil || parsed < 0x20 || parsed > 0x1ffe {
		return "", fmt.Errorf("%s must be auto or an assignable MPEG-TS PID", field)
	}
	pid := uint16(parsed)
	if previous, exists := used[pid]; exists {
		return "", fmt.Errorf("PID %d collides between %s and %s", pid, previous, field)
	}
	used[pid] = field
	return strconv.FormatUint(parsed, 10), nil
}

func normalizeCgenOutputs(feed *cgenFeedXML) error {
	seen := map[string]struct{}{}
	for index := range feed.Outputs.Outputs {
		output := &feed.Outputs.Outputs[index]
		output.ID = strings.TrimSpace(output.ID)
		if !validCgenIdentifier(output.ID) {
			return fmt.Errorf("output id %q is invalid", output.ID)
		}
		key := strings.ToLower(output.ID)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate output id %q", output.ID)
		}
		seen[key] = struct{}{}
		output.Enabled = boolText(xmlBool(output.Enabled, true))
		output.Destination = normalizeCgenDestination(output.Destination)
		if output.Destination == "" {
			return fmt.Errorf("output %q destination is unsupported", output.ID)
		}
		for _, location := range []struct {
			name  string
			value *string
		}{
			{name: "url", value: &output.URL},
			{name: "video_url", value: &output.VideoURL},
			{name: "audio_urls", value: &output.AudioURLs},
		} {
			*location.value = strings.TrimSpace(*location.value)
			if *location.value != "" && !validCgenLocation(*location.value) {
				return fmt.Errorf("output %q %s is invalid", output.ID, location.name)
			}
		}
		if output.Destination == "rtp" {
			if output.VideoURL == "" {
				output.VideoURL = output.URL
			}
			if output.VideoURL == "" || output.AudioURLs == "" {
				return fmt.Errorf("output %q RTP video and audio endpoints are required", output.ID)
			}
		} else if output.URL == "" {
			return fmt.Errorf("output %q URL is required", output.ID)
		}
		if err := validateCgenOutputProtocol(*output); err != nil {
			return fmt.Errorf("output %q: %w", output.ID, err)
		}
		output.Container = cleanCgenToken(output.Container)
		if output.Container == "" {
			output.Container = "mpegts"
		}
		output.LatencyMS = cleanBoundedUint(output.LatencyMS, "120", 0, 60000)
		if output.LatencyMS == "" {
			return fmt.Errorf("output %q latency_ms is invalid", output.ID)
		}
		output.VideoCodec = normalizeCgenVideoCodec(output.VideoCodec)
		if output.VideoCodec == "" {
			return fmt.Errorf("output %q video codec is unsupported", output.ID)
		}
		output.RateControl = normalizeCgenRateControl(output.RateControl)
		output.VideoBitrateKbps = cleanBoundedUint(output.VideoBitrateKbps, "8000", 1, 1000000)
		if output.VideoBitrateKbps == "" {
			return fmt.Errorf("output %q video bitrate is invalid", output.ID)
		}
		output.VideoMaxBitrateKbps = cleanBoundedUint(output.VideoMaxBitrateKbps, output.VideoBitrateKbps, 1, 1000000)
		if output.VideoMaxBitrateKbps == "" {
			return fmt.Errorf("output %q maximum video bitrate is invalid", output.ID)
		}
		if output.RateControl == "vbr" {
			target, _ := strconv.ParseUint(output.VideoBitrateKbps, 10, 64)
			maximum, _ := strconv.ParseUint(output.VideoMaxBitrateKbps, 10, 64)
			if maximum < target {
				return fmt.Errorf("output %q maximum video bitrate is below target", output.ID)
			}
		}
		output.GOPFrames = cleanBoundedUint(output.GOPFrames, "60", 1, 10000)
		if output.GOPFrames == "" {
			return fmt.Errorf("output %q GOP interval is invalid", output.ID)
		}
		output.AudioCodec = normalizeCgenAudioCodec(output.AudioCodec)
		if output.AudioCodec == "" {
			return fmt.Errorf("output %q audio codec is unsupported", output.ID)
		}
		if feed.Audio.Topology == "preserve_native_tracks" && output.AudioCodec != "match_input" {
			return fmt.Errorf("output %q must use match_input audio in preserve mode", output.ID)
		}
		if feed.Audio.Topology != "preserve_native_tracks" && output.AudioCodec == "match_input" {
			return fmt.Errorf("output %q match_input audio requires preserve mode", output.ID)
		}
		if output.Destination == "rtmp" && (output.VideoCodec != "h264" || output.AudioCodec != "aac") {
			return fmt.Errorf("output %q RTMP requires H.264 video and AAC audio", output.ID)
		}
		if output.Destination == "file" {
			switch output.Container {
			case "mpegts", "mpeg_ts", "ts", "matroska", "mkv":
			case "flv":
				if output.VideoCodec != "h264" || output.AudioCodec != "aac" {
					return fmt.Errorf("output %q FLV requires H.264 video and AAC audio", output.ID)
				}
			case "mp4", "mov":
				if (output.VideoCodec != "h264" && output.VideoCodec != "h265") || output.AudioCodec != "aac" {
					return fmt.Errorf("output %q MP4 and MOV require H.264 or H.265 video and AAC audio", output.ID)
				}
			case "mpegps", "mpeg_ps", "ps":
				if output.VideoCodec != "mpeg2" || (output.AudioCodec != "ac3" && output.AudioCodec != "mp2") {
					return fmt.Errorf("output %q MPEG program stream requires MPEG-2 video and AC3 or MP2 audio", output.ID)
				}
			default:
				return fmt.Errorf("output %q file container %q is unsupported", output.ID, output.Container)
			}
		}
		output.AudioBitrateKbps = cleanBoundedUint(output.AudioBitrateKbps, "192", 1, 10000)
		if output.AudioBitrateKbps == "" {
			return fmt.Errorf("output %q audio bitrate is invalid", output.ID)
		}
		output.SampleRate = cleanBoundedUint(output.SampleRate, "48000", 8000, 384000)
		if output.SampleRate == "" {
			return fmt.Errorf("output %q sample rate is invalid", output.ID)
		}
	}
	return nil
}

func validateCgenOutputProtocol(output cgenOutputXML) error {
	schemeAllowed := func(value string, schemes ...string) bool {
		value = strings.ToLower(strings.TrimSpace(value))
		if strings.Contains(value, "${") {
			return true
		}
		for _, scheme := range schemes {
			if strings.HasPrefix(value, scheme+"://") {
				return true
			}
		}
		return false
	}
	switch output.Destination {
	case "mpeg_ts_udp":
		if !schemeAllowed(output.URL, "udp") {
			return errors.New("MPEG-TS/UDP URL must use udp://")
		}
	case "mpeg_ts_srt":
		if !schemeAllowed(output.URL, "srt") {
			return errors.New("MPEG-TS/SRT URL must use srt://")
		}
	case "rtmp":
		if !schemeAllowed(output.URL, "rtmp", "rtmps") {
			return errors.New("RTMP URL must use rtmp:// or rtmps://")
		}
	case "rtp":
		if !schemeAllowed(output.VideoURL, "rtp", "udp") {
			return errors.New("RTP video endpoint must use rtp:// or udp://")
		}
		for _, location := range splitCgenCommaList(output.AudioURLs) {
			if !schemeAllowed(location, "rtp", "udp") {
				return errors.New("RTP audio endpoints must use rtp:// or udp://")
			}
		}
	}
	return nil
}

func normalizeCgenDestination(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "mpeg_ts_udp", "mpegts_udp", "udp", "mpegts", "mpeg-ts":
		return "mpeg_ts_udp"
	case "mpeg_ts_srt", "mpegts_srt", "srt":
		return "mpeg_ts_srt"
	case "rtp":
		return "rtp"
	case "rtmp", "flv":
		return "rtmp"
	case "file":
		return "file"
	default:
		return ""
	}
}

func normalizeCgenVideoCodec(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "h264", "h.264", "avc", "x264", "libx264":
		return "h264"
	case "h265", "h.265", "hevc", "x265", "libx265":
		return "h265"
	case "mpeg2", "mpeg-2", "mpeg2video":
		return "mpeg2"
	default:
		return ""
	}
}

func normalizeCgenAudioCodec(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "match", "match_input", "native":
		return "match_input"
	case "aac":
		return "aac"
	case "ac3", "ac-3":
		return "ac3"
	case "mp2", "mpeg-1-layer-ii", "mpeg1layer2":
		return "mp2"
	default:
		return ""
	}
}

func normalizeCgenRateControl(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "vbr") {
		return "vbr"
	}
	return "cbr"
}

func validCgenIdentifier(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return false
	}
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' {
			continue
		}
		return false
	}
	return true
}

func validCgenLocation(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 4096 {
		return false
	}
	for _, ch := range value {
		if ch == 0 || ch == '\r' || ch == '\n' || ch < 0x20 {
			return false
		}
	}
	return true
}

func cleanCgenText(value string, fallback string, max int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = fallback
	}
	if len(value) > max || !validCgenLocation(value) {
		return fallback
	}
	return value
}

func cleanCgenToken(value string) string {
	value = strings.TrimSpace(value)
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.' {
			continue
		}
		return ""
	}
	return value
}

func cleanBoundedUint(value string, fallback string, min uint64, max uint64) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = strings.TrimSpace(fallback)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed < min || parsed > max {
		return ""
	}
	return strconv.FormatUint(parsed, 10)
}

func normalizeCgenEncoders(config cgenEncodersXML) (cgenEncodersXML, error) {
	config.SchemaVersion = strings.TrimSpace(config.SchemaVersion)
	if config.SchemaVersion == "" {
		config.SchemaVersion = "1"
	}
	if config.SchemaVersion != "1" && config.SchemaVersion != "2" {
		return cgenEncodersXML{}, fmt.Errorf("unsupported cgen encoder schema_version %q", config.SchemaVersion)
	}
	config.UpdatedAt = strings.TrimSpace(config.UpdatedAt)
	seen := map[string]struct{}{}
	out := make([]cgenEncoderFeedXML, 0, len(config.Feeds))
	for _, feed := range config.Feeds {
		feed.ID = cleanCgenID(feed.ID)
		if feed.ID == "" {
			return cgenEncodersXML{}, errors.New("cgen encoder feed id is invalid")
		}
		feed.Video = normalizeCgenEncoderCodec(feed.Video, true)
		feed.Audio = normalizeCgenEncoderCodec(feed.Audio, false)
		feed.UpdatedAt = strings.TrimSpace(feed.UpdatedAt)
		key := strings.ToLower(feed.ID)
		if _, ok := seen[key]; ok {
			return cgenEncodersXML{}, fmt.Errorf("duplicate cgen encoder feed id %q", feed.ID)
		}
		seen[key] = struct{}{}
		out = append(out, feed)
	}
	sort.SliceStable(out, func(i, j int) bool { return strings.ToLower(out[i].ID) < strings.ToLower(out[j].ID) })
	config.Feeds = out
	seenOutputs := map[string]struct{}{}
	outputs := make([]cgenEncoderOutputXML, 0, len(config.Outputs))
	for _, output := range config.Outputs {
		output.ID = strings.TrimSpace(output.ID)
		if !validCgenIdentifier(output.ID) {
			return cgenEncodersXML{}, errors.New("cgen encoder output id is invalid")
		}
		output.Video = normalizeCgenEncoderCodec(output.Video, true)
		output.Audio = normalizeCgenEncoderCodec(output.Audio, false)
		output.UpdatedAt = strings.TrimSpace(output.UpdatedAt)
		key := strings.ToLower(output.ID)
		if _, ok := seenOutputs[key]; ok {
			return cgenEncodersXML{}, fmt.Errorf("duplicate cgen encoder output id %q", output.ID)
		}
		seenOutputs[key] = struct{}{}
		outputs = append(outputs, output)
	}
	sort.SliceStable(outputs, func(i, j int) bool { return strings.ToLower(outputs[i].ID) < strings.ToLower(outputs[j].ID) })
	config.Outputs = outputs
	return config, nil
}

func normalizeCgenEncoderCodec(value cgenEncoderCodecXML, video bool) cgenEncoderCodecXML {
	value.Codec = strings.TrimSpace(value.Codec)
	value.BitrateKbps = cleanOptionalPositive(value.BitrateKbps)
	value.GOP = cleanOptionalPositive(value.GOP)
	value.BFrames = cleanNonNegative(value.BFrames, "")
	value.Preset = normalizeCgenEncoderPreset(value.Preset)
	value.Tune = normalizeCgenEncoderTune(value.Tune)
	value.Profile = strings.TrimSpace(value.Profile)
	value.Level = strings.TrimSpace(value.Level)
	options := make([]cgenEncoderOptionXML, 0, len(value.Options))
	seen := map[string]int{}
	for _, option := range value.Options {
		option.Name = normalizeCgenEncoderOptionName(option.Name)
		if option.Name == "" {
			continue
		}
		option.Value = strings.TrimSpace(option.Value)
		if index, ok := seen[option.Name]; ok {
			options[index] = option
			continue
		}
		seen[option.Name] = len(options)
		options = append(options, option)
	}
	sort.SliceStable(options, func(i, j int) bool { return options[i].Name < options[j].Name })
	value.Options = options
	if !video {
		value.GOP = ""
		value.BFrames = ""
		value.Preset = ""
		value.Tune = ""
	}
	return value
}

func normalizeCgenEncoderPreset(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow":
		return value
	default:
		return ""
	}
}

func normalizeCgenEncoderTune(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "zerolatency", "film", "animation", "grain", "stillimage", "fastdecode", "psnr", "ssim":
		return value
	default:
		return ""
	}
}

func normalizeCgenEncoderOptionName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.' {
			continue
		}
		return ""
	}
	return value
}

func normalizeCgenLadder(feed *cgenFeedXML) {
	videos := map[string]cgenVideoRenditionXML{}
	for _, video := range feed.Ladder.Videos {
		id := strings.ToLower(strings.TrimSpace(video.ID))
		if id != "" {
			video.ID = id
			videos[id] = video
		}
	}
	audios := map[string]cgenAudioRenditionXML{}
	for _, audio := range feed.Ladder.Audios {
		id := strings.ToLower(strings.TrimSpace(audio.ID))
		if id != "" {
			audio.ID = id
			audios[id] = audio
		}
	}

	hd := normalizeVideoRendition(videos["hd"], cgenVideoRenditionXML{
		ID:          "hd",
		Enabled:     "auto",
		Width:       feed.Video.Width,
		Height:      feed.Video.Height,
		FPS:         feed.Video.FPS,
		Interlaced:  feed.Video.Interlaced,
		FieldOrder:  feed.Video.FieldOrder,
		Standard:    feed.Video.Standard,
		VCodec:      feed.ProgramOutput.VCodec,
		BitrateKbps: feed.ProgramOutput.VideoBitrateKbps,
		Program:     "1",
		VideoPID:    "256",
		PMTPID:      "4096",
	})
	p720 := normalizeVideoRendition(videos["p720"], cgenVideoRenditionXML{
		ID:          "p720",
		Enabled:     "false",
		Width:       "1280",
		Height:      "720",
		FPS:         fallbackText(feed.Video.FPS, "30000/1001"),
		Interlaced:  "false",
		FieldOrder:  "tff",
		Standard:    "atsc",
		VCodec:      feed.ProgramOutput.VCodec,
		BitrateKbps: "8000",
		Program:     "2",
		VideoPID:    "288",
		PMTPID:      "4097",
	})
	sd := normalizeVideoRendition(videos["sd"], cgenVideoRenditionXML{
		ID:          "sd",
		Enabled:     "false",
		Width:       "720",
		Height:      "480",
		FPS:         fallbackText(feed.Video.FPS, "30000/1001"),
		Interlaced:  "true",
		FieldOrder:  "tff",
		Standard:    "atsc",
		VCodec:      feed.ProgramOutput.VCodec,
		BitrateKbps: "5000",
		Program:     "3",
		VideoPID:    "320",
		PMTPID:      "4098",
	})
	surround := normalizeAudioRendition(audios["surround_51"], cgenAudioRenditionXML{
		ID:          "surround_51",
		Enabled:     "true",
		Channels:    "6",
		ACodec:      feed.ProgramOutput.ACodec,
		BitrateKbps: "384",
		Language:    "eng",
		Program:     "1",
		AudioPID:    "258",
		PMTPID:      "4096",
	})
	stereo := normalizeAudioRendition(audios["stereo"], cgenAudioRenditionXML{
		ID:          "stereo",
		Enabled:     "true",
		Channels:    "2",
		ACodec:      feed.ProgramOutput.ACodec,
		BitrateKbps: fallbackText(feed.ProgramOutput.AudioBitrateKbps, "192"),
		Language:    "eng",
		Program:     "1",
		AudioPID:    "257",
		PMTPID:      "4096",
	})
	feed.Ladder = cgenLadderXML{
		Videos: []cgenVideoRenditionXML{hd, p720, sd},
		Audios: []cgenAudioRenditionXML{stereo, surround},
	}
	feed.ProgramOutput.VideoBitrateKbps = hd.BitrateKbps
	feed.ProgramOutput.AudioBitrateKbps = stereo.BitrateKbps
}

func normalizeVideoRendition(value cgenVideoRenditionXML, fallback cgenVideoRenditionXML) cgenVideoRenditionXML {
	value.ID = fallbackText(strings.ToLower(strings.TrimSpace(value.ID)), fallback.ID)
	value.Enabled = normalizeCgenEnabledText(value.Enabled, fallback.Enabled)
	value.Width = cleanPositive(value.Width, fallback.Width)
	value.Height = cleanPositive(value.Height, fallback.Height)
	value.FPS = fallbackText(strings.TrimSpace(value.FPS), fallback.FPS)
	value.Interlaced = boolText(xmlBool(value.Interlaced, xmlBool(fallback.Interlaced, false)))
	value.FieldOrder = normalizeCgenFieldOrder(fallbackText(value.FieldOrder, fallback.FieldOrder))
	value.Standard = normalizeCgenStandard(fallbackText(value.Standard, fallback.Standard))
	value.VCodec = fallbackText(strings.TrimSpace(value.VCodec), fallback.VCodec)
	value.BitrateKbps = cleanPositive(value.BitrateKbps, fallback.BitrateKbps)
	value.Program = cleanPositive(value.Program, fallback.Program)
	value.VideoPID = cleanPositive(value.VideoPID, fallback.VideoPID)
	value.PMTPID = cleanPositive(value.PMTPID, fallback.PMTPID)
	return value
}

func normalizeAudioRendition(value cgenAudioRenditionXML, fallback cgenAudioRenditionXML) cgenAudioRenditionXML {
	value.ID = fallbackText(strings.ToLower(strings.TrimSpace(value.ID)), fallback.ID)
	value.Enabled = boolText(xmlBool(value.Enabled, xmlBool(fallback.Enabled, true)))
	value.Channels = cleanPositive(value.Channels, fallback.Channels)
	value.ACodec = fallbackText(strings.TrimSpace(value.ACodec), fallback.ACodec)
	value.BitrateKbps = cleanPositive(value.BitrateKbps, fallback.BitrateKbps)
	value.Language = fallbackText(strings.TrimSpace(value.Language), fallback.Language)
	value.Program = cleanPositive(value.Program, fallback.Program)
	value.AudioPID = cleanPositive(value.AudioPID, fallback.AudioPID)
	value.PMTPID = cleanPositive(value.PMTPID, fallback.PMTPID)
	return value
}

func normalizeCgenEnabledText(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "auto") {
		return "auto"
	}
	if value == "" && strings.EqualFold(strings.TrimSpace(fallback), "auto") {
		return "auto"
	}
	return boolText(xmlBool(value, xmlBool(fallback, true)))
}

func cgenMapFromAny(raw any) map[string]any {
	value, _ := raw.(map[string]any)
	return value
}

func firstNestedString(source map[string]any, nested map[string]any, flatKey string, nestedKey string) string {
	if value, exists := source[flatKey]; exists {
		return stringFromAny(value)
	}
	return stringFromAny(nested[nestedKey])
}

func firstNestedBool(source map[string]any, nested map[string]any, flatKey string, nestedKey string, fallback bool) bool {
	if value, exists := source[flatKey]; exists {
		return boolFromAny(value, fallback)
	}
	return boolFromAny(nested[nestedKey], fallback)
}

func optionalNestedBoolText(source map[string]any, nested map[string]any, flatKey string, nestedKey string) string {
	if value, exists := source[flatKey]; exists {
		return boolText(boolFromAny(value, false))
	}
	if value, exists := nested[nestedKey]; exists {
		return boolText(boolFromAny(value, false))
	}
	return ""
}

func cgenProgramMappingFromAny(raw any) (cgenProgramMappingXML, error) {
	if raw == nil {
		return cgenProgramMappingXML{}, nil
	}
	source, ok := raw.(map[string]any)
	if !ok {
		return cgenProgramMappingXML{}, errors.New("program_mapping must be an object")
	}
	result := cgenProgramMappingXML{
		Mode:              stringFromAny(source["mode"]),
		TransportStreamID: stringFromAny(source["transport_stream_id"]),
	}
	items, err := cgenObjectSlice(source["programs"], "program_mapping programs")
	if err != nil {
		return cgenProgramMappingXML{}, err
	}
	for _, program := range items {
		entry := cgenProgramMapEntryXML{
			Number:       stringFromAny(program["number"]),
			ServiceName:  stringFromAny(program["service_name"]),
			ProviderName: stringFromAny(program["provider_name"]),
			PMTPID:       stringFromAny(program["pmt_pid"]),
			VideoPID:     stringFromAny(program["video_pid"]),
		}
		audioItems, err := cgenObjectSlice(program["audio"], "program audio mappings")
		if err != nil {
			return cgenProgramMappingXML{}, err
		}
		for _, audio := range audioItems {
			entry.Audio = append(entry.Audio, cgenProgramAudioMapXML{
				TrackID: stringFromAny(audio["track_id"]),
				PID:     stringFromAny(audio["pid"]),
			})
		}
		if rawSCTE, exists := program["scte35"]; exists && rawSCTE != nil {
			scte, ok := rawSCTE.(map[string]any)
			if !ok {
				return cgenProgramMappingXML{}, errors.New("program scte35 must be an object")
			}
			entry.SCTE35 = &cgenProgramSCTE35XML{
				Input:              stringFromAny(scte["input"]),
				GeneratedAlertCues: boolText(boolFromAny(scte["generated_alert_cues"], false)),
				PID:                stringFromAny(scte["pid"]),
			}
		}
		result.Programs = append(result.Programs, entry)
	}
	return result, nil
}

func cgenOutputsFromAny(raw any) ([]cgenOutputXML, error) {
	items, err := cgenObjectSlice(raw, "outputs")
	if err != nil {
		return nil, err
	}
	outputs := make([]cgenOutputXML, 0, len(items))
	for _, source := range items {
		video := cgenMapFromAny(source["video"])
		audio := cgenMapFromAny(source["audio"])
		outputs = append(outputs, cgenOutputXML{
			ID:                  stringFromAny(source["id"]),
			Enabled:             boolText(boolFromAny(source["enabled"], true)),
			Destination:         stringFromAny(source["destination"]),
			URL:                 stringFromAny(source["url"]),
			VideoURL:            stringFromAny(source["video_url"]),
			AudioURLs:           cgenCommaListFromAny(source["audio_urls"]),
			Container:           stringFromAny(source["container"]),
			LatencyMS:           stringFromAny(source["latency_ms"]),
			VideoCodec:          fallbackText(stringFromAny(source["video_codec"]), stringFromAny(video["codec"])),
			RateControl:         fallbackText(stringFromAny(source["rate_control"]), stringFromAny(video["rate_control"])),
			VideoBitrateKbps:    fallbackText(stringFromAny(source["video_bitrate_kbps"]), stringFromAny(video["bitrate_kbps"])),
			VideoMaxBitrateKbps: fallbackText(stringFromAny(source["video_max_bitrate_kbps"]), stringFromAny(video["max_bitrate_kbps"])),
			GOPFrames:           fallbackText(stringFromAny(source["gop_frames"]), stringFromAny(video["gop_frames"])),
			AudioCodec:          fallbackText(stringFromAny(source["audio_codec"]), stringFromAny(audio["codec"])),
			AudioBitrateKbps:    fallbackText(stringFromAny(source["audio_bitrate_kbps"]), stringFromAny(audio["bitrate_kbps"])),
			SampleRate:          fallbackText(stringFromAny(source["sample_rate"]), stringFromAny(audio["sample_rate"])),
		})
	}
	return outputs, nil
}

func cgenObjectSlice(raw any, field string) ([]map[string]any, error) {
	if raw == nil {
		return nil, nil
	}
	switch values := raw.(type) {
	case []any:
		out := make([]map[string]any, 0, len(values))
		for _, value := range values {
			object, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s entries must be objects", field)
			}
			out = append(out, object)
		}
		return out, nil
	case []map[string]any:
		return values, nil
	default:
		return nil, fmt.Errorf("%s must be an array", field)
	}
}

func cgenCommaListFromAny(raw any) string {
	switch values := raw.(type) {
	case []any:
		parts := make([]string, 0, len(values))
		for _, value := range values {
			if text := stringFromAny(value); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, ",")
	case []string:
		return strings.Join(values, ",")
	default:
		return stringFromAny(raw)
	}
}

func cgenFeedFromMap(raw any) (cgenFeedXML, error) {
	source, ok := raw.(map[string]any)
	if !ok {
		return cgenFeedXML{}, fmt.Errorf("cgen feed entries must be objects")
	}
	input := cgenMapFromAny(source["program_input"])
	alert := cgenMapFromAny(source["alert"])
	ancillary := cgenMapFromAny(source["ancillary"])
	audio := cgenMapFromAny(source["audio_routing"])
	compositor := cgenMapFromAny(source["compositor"])
	rawProgramMapping := source["program_mapping"]
	if rawProgramMapping == nil {
		rawProgramMapping = source["programMapping"]
	}
	programMapping, err := cgenProgramMappingFromAny(rawProgramMapping)
	if err != nil {
		return cgenFeedXML{}, err
	}
	outputs, err := cgenOutputsFromAny(source["outputs"])
	if err != nil {
		return cgenFeedXML{}, err
	}
	feed := cgenFeedXML{
		ID:      stringFromAny(source["id"]),
		Name:    stringFromAny(source["name"]),
		Enabled: boolText(boolFromAny(source["enabled"], false)),
		ProgramInput: cgenEndpointXML{
			URL:             firstNestedString(source, input, "program_input_url", "url"),
			Type:            firstNestedString(source, input, "program_input_type", "type"),
			Format:          firstNestedString(source, input, "program_input_format", "format"),
			HardwareDecoder: firstNestedString(source, input, "hardware_decoder", "hardware_decoder"),
			HardwareDecoderOn: boolText(
				firstNestedBool(source, input, "hardware_decoder_enabled", "hardware_decoder_enabled", false),
			),
			DeinterlaceAlgorithm: firstNestedString(source, input, "deinterlace_algorithm", "deinterlace_algorithm"),
			DeinterlaceBackend:   firstNestedString(source, input, "deinterlace_backend", "deinterlace_backend"),
			DeinterlaceCadence:   firstNestedString(source, input, "deinterlace_cadence", "deinterlace_cadence"),
			DeinterlaceParity:    firstNestedString(source, input, "deinterlace_parity", "deinterlace_parity"),
			DeviceBackend:        firstNestedString(source, input, "device_backend", "device_backend"),
			DeviceID:             firstNestedString(source, input, "device_id", "device_id"),
			Width:                firstNestedString(source, input, "dummy_width", "width"),
			Height:               firstNestedString(source, input, "dummy_height", "height"),
			FPS:                  firstNestedString(source, input, "dummy_fps", "fps"),
			Interlaced:           optionalNestedBoolText(source, input, "dummy_interlaced", "interlaced"),
			FieldOrder:           firstNestedString(source, input, "dummy_field_order", "field_order"),
			Background:           firstNestedString(source, input, "dummy_background", "background"),
		},
		PriorityInput: cgenPriorityInputXML{
			FeedID:      stringFromAny(source["priority_feed_id"]),
			AudioSource: stringFromAny(source["audio_source"]),
			Format:      stringFromAny(source["priority_input_format"]),
		},
		ProgramOutput: cgenEndpointXML{
			URL:               stringFromAny(source["program_output_url"]),
			Format:            stringFromAny(source["program_output_format"]),
			VCodec:            stringFromAny(source["vcodec"]),
			ACodec:            stringFromAny(source["acodec"]),
			VideoBitrateKbps:  firstStringFromAny(source, "hd_bitrate_kbps", "video_bitrate_kbps"),
			AudioBitrateKbps:  firstStringFromAny(source, "stereo_bitrate_kbps", "audio_bitrate_kbps"),
			ServiceName:       stringFromAny(source["service_name"]),
			ProviderName:      stringFromAny(source["provider_name"]),
			ServiceID:         stringFromAny(source["service_id"]),
			TransportStreamID: stringFromAny(source["transport_stream_id"]),
		},
		Video: cgenVideoXML{
			Width:      stringFromAny(source["width"]),
			Height:     stringFromAny(source["height"]),
			FPS:        stringFromAny(source["fps"]),
			Interlaced: boolText(boolFromAny(source["interlaced"], false)),
			FieldOrder: stringFromAny(source["field_order"]),
			Standard:   stringFromAny(source["standard"]),
		},
		Audio: cgenAudioXML{
			Idle:               stringFromAny(source["audio_idle"]),
			AlertMode:          stringFromAny(source["audio_alert_mode"]),
			MuteStandbyRoutine: boolText(boolFromAny(source["mute_standby_routine"], true)),
			Topology:           firstNestedString(source, audio, "audio_topology", "topology"),
			ForceLayout:        firstNestedString(source, audio, "audio_force_layout", "force_layout"),
			IdleProgramGainDB:  firstNestedString(source, audio, "idle_program_gain_db", "idle_program_gain_db"),
			AlertProgramGainDB: firstNestedString(source, audio, "alert_program_gain_db", "alert_program_gain_db"),
			AlertGainDB:        firstNestedString(source, audio, "alert_gain_db", "alert_gain_db"),
			TransitionMS:       firstNestedString(source, audio, "audio_transition_ms", "transition_ms"),
		},
		Ladder: cgenLadderXML{
			Videos: []cgenVideoRenditionXML{
				{
					ID:          "hd",
					Enabled:     boolOrTextFromAny(source["hd_enabled"], "auto"),
					Width:       stringFromAny(source["width"]),
					Height:      stringFromAny(source["height"]),
					FPS:         stringFromAny(source["fps"]),
					Interlaced:  boolText(boolFromAny(source["interlaced"], false)),
					FieldOrder:  stringFromAny(source["field_order"]),
					Standard:    stringFromAny(source["standard"]),
					VCodec:      stringFromAny(source["vcodec"]),
					BitrateKbps: firstStringFromAny(source, "hd_bitrate_kbps", "video_bitrate_kbps"),
					Program:     fallbackText(stringFromAny(source["hd_program"]), "1"),
					VideoPID:    fallbackText(stringFromAny(source["hd_video_pid"]), "256"),
					PMTPID:      fallbackText(stringFromAny(source["hd_pmt_pid"]), "4096"),
				},
				{
					ID:          "p720",
					Enabled:     boolText(boolFromAny(source["p720_enabled"], false)),
					Width:       "1280",
					Height:      "720",
					FPS:         stringFromAny(source["fps"]),
					Interlaced:  "false",
					FieldOrder:  "tff",
					Standard:    "atsc",
					VCodec:      stringFromAny(source["vcodec"]),
					BitrateKbps: fallbackText(stringFromAny(source["p720_bitrate_kbps"]), "8000"),
					Program:     fallbackText(stringFromAny(source["p720_program"]), "2"),
					VideoPID:    fallbackText(stringFromAny(source["p720_video_pid"]), "288"),
					PMTPID:      fallbackText(stringFromAny(source["p720_pmt_pid"]), "4097"),
				},
				{
					ID:          "sd",
					Enabled:     boolText(boolFromAny(source["sd_enabled"], false)),
					Width:       "720",
					Height:      "480",
					FPS:         stringFromAny(source["fps"]),
					Interlaced:  "true",
					FieldOrder:  "tff",
					Standard:    "atsc",
					VCodec:      stringFromAny(source["vcodec"]),
					BitrateKbps: fallbackText(stringFromAny(source["sd_bitrate_kbps"]), "5000"),
					Program:     fallbackText(stringFromAny(source["sd_program"]), "3"),
					VideoPID:    fallbackText(stringFromAny(source["sd_video_pid"]), "320"),
					PMTPID:      fallbackText(stringFromAny(source["sd_pmt_pid"]), "4098"),
				},
			},
			Audios: []cgenAudioRenditionXML{
				{
					ID:          "stereo",
					Enabled:     boolText(boolFromAny(source["stereo_enabled"], true)),
					Channels:    "2",
					ACodec:      stringFromAny(source["acodec"]),
					BitrateKbps: firstStringFromAny(source, "stereo_bitrate_kbps", "audio_bitrate_kbps"),
					Language:    "eng",
					Program:     fallbackText(stringFromAny(source["stereo_program"]), "1"),
					AudioPID:    fallbackText(stringFromAny(source["stereo_audio_pid"]), "257"),
					PMTPID:      fallbackText(stringFromAny(source["stereo_pmt_pid"]), "4096"),
				},
				{
					ID:          "surround_51",
					Enabled:     boolText(boolFromAny(source["surround_enabled"], true)),
					Channels:    "6",
					ACodec:      stringFromAny(source["acodec"]),
					BitrateKbps: fallbackText(stringFromAny(source["surround_bitrate_kbps"]), "384"),
					Language:    "eng",
					Program:     fallbackText(stringFromAny(source["surround_program"]), "1"),
					AudioPID:    fallbackText(stringFromAny(source["surround_audio_pid"]), "258"),
					PMTPID:      fallbackText(stringFromAny(source["surround_pmt_pid"]), "4096"),
				},
			},
		},
		Banner: cgenBannerXML{
			Mode:              stringFromAny(source["banner_mode"]),
			TickerHeight:      stringFromAny(source["ticker_height"]),
			Font:              stringFromAny(source["font"]),
			FontWeight:        fallbackText(stringFromAny(source["font_weight"]), "regular"),
			FontSize:          stringFromAny(source["font_size"]),
			ScrollSpeed:       stringFromAny(source["scroll_speed"]),
			ScrollRepeatMode:  stringFromAny(source["scroll_repeat_mode"]),
			AfterEOMRepeats:   stringFromAny(source["after_eom_repeats"]),
			FixedRepeats:      stringFromAny(source["fixed_repeats"]),
			BackgroundEnabled: boolText(boolFromAny(source["banner_background_enabled"], true)),
		},
		Graphics: cgenGraphicsXML{
			Font:         stringFromAny(source["font"]),
			FontWeight:   fallbackText(stringFromAny(source["font_weight"]), "regular"),
			FontSize:     stringFromAny(source["font_size"]),
			BannerHeight: stringFromAny(source["ticker_height"]),
		},
		Clock: cgenClockXML{
			Enabled:  boolText(boolFromAny(source["clock_enabled"], false)),
			Format:   stringFromAny(source["clock_format"]),
			X:        stringFromAny(source["clock_x"]),
			Y:        stringFromAny(source["clock_y"]),
			FontSize: stringFromAny(source["clock_font_size"]),
			Color:    stringFromAny(source["clock_color"]),
		},
		Text: cgenTextXML{
			Enabled:  boolText(boolFromAny(source["text_enabled"], false)),
			FontSize: stringFromAny(source["text_font_size"]),
			Color:    stringFromAny(source["text_color"]),
			Content:  stringFromAny(source["text"]),
		},
		State: cgenStateXML{
			Mode:      stringFromAny(source["mode"]),
			SMPTEBars: boolText(boolFromAny(source["smpte_bars"], false)),
			SunnyCat:  boolText(boolFromAny(source["sunny_cat"], false)),
		},
		Standby: cgenStandbyXML{
			Mode:     stringFromAny(source["standby_mode"]),
			Text:     stringFromAny(source["standby_text"]),
			FontSize: stringFromAny(source["standby_font_size"]),
			YPercent: stringFromAny(source["standby_y_percent"]),
		},
		Sync: cgenSyncXML{
			HardResetMS:            stringFromAny(source["sync_hard_reset_ms"]),
			MaxAudioFramesPerVideo: stringFromAny(source["sync_max_audio_frames_per_video"]),
			SourceBufferMS:         stringFromAny(source["sync_source_buffer_ms"]),
			ReconnectInitialMS:     stringFromAny(source["sync_reconnect_initial_ms"]),
			ReconnectMaxMS:         stringFromAny(source["sync_reconnect_max_ms"]),
			StatusIntervalMS:       stringFromAny(source["sync_status_interval_ms"]),
		},
		Alert: cgenAlertXML{
			FeedID: fallbackText(firstNestedString(source, alert, "alert_feed_id", "feed_id"), stringFromAny(source["priority_feed_id"])),
		},
		Ancillary: cgenAncillaryXML{
			Captions: firstNestedString(source, ancillary, "ancillary_captions", "captions"),
			SCTE35:   firstNestedString(source, ancillary, "ancillary_scte35", "scte35"),
			SCTE104:  firstNestedString(source, ancillary, "ancillary_scte104", "scte104"),
		},
		Compositor: cgenCompositorXML{
			AlertSceneID: firstNestedString(source, compositor, "alert_scene_id", "alert_scene_id"),
			Engine:       firstNestedString(source, compositor, "compositor_engine", "engine"),
		},
		ProgramMap: programMapping,
		Outputs:    cgenOutputsXML{Outputs: outputs},
		UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	config, err := normalizeCgen(cgenXML{Enabled: "true", Feeds: []cgenFeedXML{feed}})
	if err != nil {
		return cgenFeedXML{}, err
	}
	return config.Feeds[0], nil
}

func cgenEncoderFeedFromMap(raw any, feed cgenFeedXML) (cgenEncoderFeedXML, error) {
	source, ok := raw.(map[string]any)
	if !ok {
		return cgenEncoderFeedXML{}, fmt.Errorf("cgen feed entries must be objects")
	}
	encoderFeed := cgenEncoderFeedXML{
		ID:        feed.ID,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		Video: cgenEncoderCodecXML{
			Codec:       firstStringFromAny(source, "video_encoder_codec", "vcodec"),
			BitrateKbps: firstStringFromAny(source, "video_encoder_bitrate_kbps", "hd_bitrate_kbps", "video_bitrate_kbps"),
			GOP:         stringFromAny(source["video_gop"]),
			BFrames:     stringFromAny(source["video_bframes"]),
			Preset:      stringFromAny(source["video_preset"]),
			Tune:        stringFromAny(source["video_tune"]),
			Profile:     stringFromAny(source["video_profile"]),
			Level:       stringFromAny(source["video_level"]),
			Options:     encoderOptionsFromAny(source["video_encoder_options"]),
		},
		Audio: cgenEncoderCodecXML{
			Codec:       firstStringFromAny(source, "audio_encoder_codec", "acodec"),
			BitrateKbps: firstStringFromAny(source, "audio_encoder_bitrate_kbps", "stereo_bitrate_kbps", "audio_bitrate_kbps"),
			Profile:     stringFromAny(source["audio_profile"]),
			Level:       stringFromAny(source["audio_level"]),
			Options:     encoderOptionsFromAny(source["audio_encoder_options"]),
		},
	}
	normalized, err := normalizeCgenEncoders(cgenEncodersXML{SchemaVersion: "2", Feeds: []cgenEncoderFeedXML{encoderFeed}})
	if err != nil {
		return cgenEncoderFeedXML{}, err
	}
	return normalized.Feeds[0], nil
}

func cgenEncoderOutputsFromMap(raw any, feed cgenFeedXML) ([]cgenEncoderOutputXML, error) {
	source, ok := raw.(map[string]any)
	if !ok {
		return nil, errors.New("cgen feed entries must be objects")
	}
	rawOutputs, err := cgenObjectSlice(source["outputs"], "outputs")
	if err != nil {
		return nil, err
	}
	byID := map[string]map[string]any{}
	for _, output := range rawOutputs {
		byID[strings.ToLower(strings.TrimSpace(stringFromAny(output["id"])))] = output
	}
	now := time.Now().UTC().Format(time.RFC3339)
	profiles := make([]cgenEncoderOutputXML, 0, len(feed.Outputs.Outputs))
	for _, output := range feed.Outputs.Outputs {
		rawOutput := byID[strings.ToLower(output.ID)]
		encoder := cgenMapFromAny(rawOutput["encoder"])
		video := cgenMapFromAny(encoder["video"])
		audio := cgenMapFromAny(encoder["audio"])
		profiles = append(profiles, cgenEncoderOutputXML{
			ID:        output.ID,
			UpdatedAt: now,
			Video: cgenEncoderCodecXML{
				Codec:       fallbackText(stringFromAny(video["codec"]), output.VideoCodec),
				BitrateKbps: fallbackText(stringFromAny(video["bitrate_kbps"]), output.VideoBitrateKbps),
				GOP:         fallbackText(stringFromAny(video["gop"]), output.GOPFrames),
				BFrames:     stringFromAny(video["bframes"]),
				Preset:      stringFromAny(video["preset"]),
				Tune:        stringFromAny(video["tune"]),
				Profile:     stringFromAny(video["profile"]),
				Level:       stringFromAny(video["level"]),
				Options:     encoderOptionsFromAny(video["options"]),
			},
			Audio: cgenEncoderCodecXML{
				Codec:       fallbackText(stringFromAny(audio["codec"]), output.AudioCodec),
				BitrateKbps: fallbackText(stringFromAny(audio["bitrate_kbps"]), output.AudioBitrateKbps),
				Profile:     stringFromAny(audio["profile"]),
				Level:       stringFromAny(audio["level"]),
				Options:     encoderOptionsFromAny(audio["options"]),
			},
		})
	}
	return profiles, nil
}

func encoderOptionsFromAny(raw any) []cgenEncoderOptionXML {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	options := make([]cgenEncoderOptionXML, 0, len(items))
	for _, item := range items {
		source, ok := item.(map[string]any)
		if !ok {
			continue
		}
		options = append(options, cgenEncoderOptionXML{
			Name:  stringFromAny(source["name"]),
			Value: stringFromAny(source["value"]),
		})
	}
	return options
}

func cgenPayload(configPath string, path string, config cgenXML, encoderPath string, encoders cgenEncodersXML) map[string]any {
	revision, _ := cgenConfigRevision(path, encoderPath)
	schemaVersion, _ := strconv.Atoi(config.SchemaVersion)
	encoderSchemaVersion, _ := strconv.Atoi(encoders.SchemaVersion)
	rows := make([]map[string]any, 0, len(config.Feeds))
	for _, feed := range config.Feeds {
		runtime := loadCgenRuntimeStatus(configPath, feed.ID)
		row := map[string]any{
			"id":                       feed.ID,
			"name":                     feed.Name,
			"enabled":                  xmlBool(feed.Enabled, false),
			"mode":                     feed.State.Mode,
			"smpte_bars":               xmlBool(feed.State.SMPTEBars, false),
			"sunny_cat":                xmlBool(feed.State.SunnyCat, false),
			"standby_mode":             feed.Standby.Mode,
			"standby_text":             feed.Standby.Text,
			"standby_font_size":        feed.Standby.FontSize,
			"standby_y_percent":        feed.Standby.YPercent,
			"program_input_url":        feed.ProgramInput.URL,
			"program_input_type":       normalizeCgenInputType(feed.ProgramInput.Type),
			"program_input_format":     feed.ProgramInput.Format,
			"device_backend":           feed.ProgramInput.DeviceBackend,
			"device_id":                feed.ProgramInput.DeviceID,
			"dummy_width":              feed.ProgramInput.Width,
			"dummy_height":             feed.ProgramInput.Height,
			"dummy_fps":                feed.ProgramInput.FPS,
			"dummy_interlaced":         xmlBool(feed.ProgramInput.Interlaced, false),
			"dummy_field_order":        feed.ProgramInput.FieldOrder,
			"dummy_background":         feed.ProgramInput.Background,
			"hardware_decoder_enabled": xmlBool(feed.ProgramInput.HardwareDecoderOn, false),
			"hardware_decoder":         feed.ProgramInput.HardwareDecoder,
			"deinterlace_algorithm":    feed.ProgramInput.DeinterlaceAlgorithm,
			"deinterlace_backend":      feed.ProgramInput.DeinterlaceBackend,
			"deinterlace_cadence":      feed.ProgramInput.DeinterlaceCadence,
			"deinterlace_parity":       feed.ProgramInput.DeinterlaceParity,
			"priority_feed_id":         feed.PriorityInput.FeedID,
			"audio_source":             feed.PriorityInput.AudioSource,
			"priority_input_format":    feed.PriorityInput.Format,
			"program_output_url":       feed.ProgramOutput.URL,
			"program_output_format":    feed.ProgramOutput.Format,
			"vcodec":                   feed.ProgramOutput.VCodec,
			"acodec":                   feed.ProgramOutput.ACodec,
			"video_bitrate_kbps":       feed.ProgramOutput.VideoBitrateKbps,
			"audio_bitrate_kbps":       feed.ProgramOutput.AudioBitrateKbps,
			"service_name":             feed.ProgramOutput.ServiceName,
			"provider_name":            feed.ProgramOutput.ProviderName,
			"service_id":               feed.ProgramOutput.ServiceID,
			"transport_stream_id":      feed.ProgramOutput.TransportStreamID,
			"hd_enabled":               videoRendition(feed.Ladder, "hd").Enabled,
			"hd_bitrate_kbps":          videoRendition(feed.Ladder, "hd").BitrateKbps,
			"hd_program":               videoRendition(feed.Ladder, "hd").Program,
			"hd_video_pid":             videoRendition(feed.Ladder, "hd").VideoPID,
			"hd_pmt_pid":               videoRendition(feed.Ladder, "hd").PMTPID,
			"p720_enabled":             xmlBool(videoRendition(feed.Ladder, "p720").Enabled, false),
			"p720_bitrate_kbps":        videoRendition(feed.Ladder, "p720").BitrateKbps,
			"p720_program":             videoRendition(feed.Ladder, "p720").Program,
			"p720_video_pid":           videoRendition(feed.Ladder, "p720").VideoPID,
			"p720_pmt_pid":             videoRendition(feed.Ladder, "p720").PMTPID,
			"sd_enabled":               xmlBool(videoRendition(feed.Ladder, "sd").Enabled, false),
			"sd_bitrate_kbps":          videoRendition(feed.Ladder, "sd").BitrateKbps,
			"sd_program":               videoRendition(feed.Ladder, "sd").Program,
			"sd_video_pid":             videoRendition(feed.Ladder, "sd").VideoPID,
			"sd_pmt_pid":               videoRendition(feed.Ladder, "sd").PMTPID,
			"surround_enabled":         xmlBool(audioRendition(feed.Ladder, "surround_51").Enabled, true),
			"surround_bitrate_kbps":    audioRendition(feed.Ladder, "surround_51").BitrateKbps,
			"surround_program":         audioRendition(feed.Ladder, "surround_51").Program,
			"surround_audio_pid":       audioRendition(feed.Ladder, "surround_51").AudioPID,
			"surround_pmt_pid":         audioRendition(feed.Ladder, "surround_51").PMTPID,
			"stereo_enabled":           xmlBool(audioRendition(feed.Ladder, "stereo").Enabled, true),
			"stereo_bitrate_kbps":      audioRendition(feed.Ladder, "stereo").BitrateKbps,
			"stereo_program":           audioRendition(feed.Ladder, "stereo").Program,
			"stereo_audio_pid":         audioRendition(feed.Ladder, "stereo").AudioPID,
			"stereo_pmt_pid":           audioRendition(feed.Ladder, "stereo").PMTPID,
			"width":                    feed.Video.Width,
			"height":                   feed.Video.Height,
			"fps":                      feed.Video.FPS,
			"interlaced":               xmlBool(feed.Video.Interlaced, false),
			"field_order":              feed.Video.FieldOrder,
			"standard":                 feed.Video.Standard,
			"audio_idle":               feed.Audio.Idle,
			"audio_alert_mode":         feed.Audio.AlertMode,
			"mute_standby_routine":     xmlBool(feed.Audio.MuteStandbyRoutine, true),
			"alert_feed_id":            feed.Alert.FeedID,
			"ancillary_captions":       feed.Ancillary.Captions,
			"ancillary_scte35":         feed.Ancillary.SCTE35,
			"ancillary_scte104":        feed.Ancillary.SCTE104,
			"audio_topology":           feed.Audio.Topology,
			"audio_force_layout":       feed.Audio.ForceLayout,
			"idle_program_gain_db":     feed.Audio.IdleProgramGainDB,
			"alert_program_gain_db":    feed.Audio.AlertProgramGainDB,
			"alert_gain_db":            feed.Audio.AlertGainDB,
			"audio_transition_ms":      feed.Audio.TransitionMS,
			"alert_scene_id":           feed.Compositor.AlertSceneID,
			"compositor_engine":        feed.Compositor.Engine,
			"program_input":            cgenProgramInputPayload(feed.ProgramInput),
			"alert":                    map[string]any{"feed_id": feed.Alert.FeedID},
			"ancillary": map[string]any{
				"captions": feed.Ancillary.Captions,
				"scte35":   feed.Ancillary.SCTE35,
				"scte104":  feed.Ancillary.SCTE104,
			},
			"audio_routing": map[string]any{
				"topology":              feed.Audio.Topology,
				"force_layout":          feed.Audio.ForceLayout,
				"idle_program_gain_db":  feed.Audio.IdleProgramGainDB,
				"alert_program_gain_db": feed.Audio.AlertProgramGainDB,
				"alert_gain_db":         feed.Audio.AlertGainDB,
				"transition_ms":         feed.Audio.TransitionMS,
			},
			"compositor": map[string]any{
				"alert_scene_id": feed.Compositor.AlertSceneID,
				"engine":         feed.Compositor.Engine,
			},
			"program_mapping":                 cgenProgramMappingPayload(feed.ProgramMap),
			"outputs":                         cgenOutputsPayload(feed.Outputs.Outputs, encoders),
			"banner_mode":                     feed.Banner.Mode,
			"ticker_height":                   feed.Banner.TickerHeight,
			"scroll_speed":                    feed.Banner.ScrollSpeed,
			"scroll_repeat_mode":              feed.Banner.ScrollRepeatMode,
			"after_eom_repeats":               feed.Banner.AfterEOMRepeats,
			"fixed_repeats":                   feed.Banner.FixedRepeats,
			"font":                            feed.Graphics.Font,
			"font_weight":                     fallbackText(fallbackText(feed.Graphics.FontWeight, feed.Banner.FontWeight), "regular"),
			"font_size":                       feed.Graphics.FontSize,
			"banner_background_enabled":       xmlBool(feed.Banner.BackgroundEnabled, true),
			"banner_height":                   feed.Graphics.BannerHeight,
			"text_enabled":                    xmlBool(feed.Text.Enabled, false),
			"text":                            feed.Text.Content,
			"text_font_size":                  feed.Text.FontSize,
			"text_color":                      feed.Text.Color,
			"clock_enabled":                   xmlBool(feed.Clock.Enabled, false),
			"clock_format":                    feed.Clock.Format,
			"clock_x":                         feed.Clock.X,
			"clock_y":                         feed.Clock.Y,
			"clock_font_size":                 feed.Clock.FontSize,
			"clock_color":                     feed.Clock.Color,
			"sync_hard_reset_ms":              feed.Sync.HardResetMS,
			"sync_max_audio_frames_per_video": feed.Sync.MaxAudioFramesPerVideo,
			"sync_source_buffer_ms":           feed.Sync.SourceBufferMS,
			"sync_reconnect_initial_ms":       feed.Sync.ReconnectInitialMS,
			"sync_reconnect_max_ms":           feed.Sync.ReconnectMaxMS,
			"sync_status_interval_ms":         feed.Sync.StatusIntervalMS,
			"updated_at":                      fallbackText(feed.State.UpdatedAt, feed.UpdatedAt),
			"runtime":                         runtime,
		}
		for key, value := range cgenEncoderPayload(encoders, feed.ID) {
			row[key] = value
		}
		rows = append(rows, row)
	}
	return map[string]any{
		"path":                   filepath.ToSlash(path),
		"encoder_path":           filepath.ToSlash(encoderPath),
		"schema_version":         schemaVersion,
		"encoder_schema_version": encoderSchemaVersion,
		"revision":               revision,
		"hash":                   revision,
		"enabled":                xmlBool(config.Enabled, true),
		"feeds":                  rows,
		"summary": map[string]any{
			"count": len(rows),
		},
	}
}

func cgenEncoderPayload(encoders cgenEncodersXML, feedID string) map[string]any {
	feed := cgenEncoderFeed(encoders, feedID)
	videoOptions := encoderOptionsPayload(feed.Video.Options)
	audioOptions := encoderOptionsPayload(feed.Audio.Options)
	return map[string]any{
		"video_encoder_codec":        feed.Video.Codec,
		"video_encoder_bitrate_kbps": feed.Video.BitrateKbps,
		"video_gop":                  feed.Video.GOP,
		"video_bframes":              feed.Video.BFrames,
		"video_preset":               feed.Video.Preset,
		"video_tune":                 feed.Video.Tune,
		"video_profile":              feed.Video.Profile,
		"video_level":                feed.Video.Level,
		"video_encoder_options":      videoOptions,
		"audio_encoder_codec":        feed.Audio.Codec,
		"audio_encoder_bitrate_kbps": feed.Audio.BitrateKbps,
		"audio_profile":              feed.Audio.Profile,
		"audio_level":                feed.Audio.Level,
		"audio_encoder_options":      audioOptions,
	}
}

func cgenProgramInputPayload(input cgenEndpointXML) map[string]any {
	return map[string]any{
		"type":                     normalizeCgenInputType(input.Type),
		"url":                      input.URL,
		"format":                   input.Format,
		"hardware_decoder_enabled": xmlBool(input.HardwareDecoderOn, false),
		"hardware_decoder":         input.HardwareDecoder,
		"deinterlace_algorithm":    input.DeinterlaceAlgorithm,
		"deinterlace_backend":      input.DeinterlaceBackend,
		"deinterlace_cadence":      input.DeinterlaceCadence,
		"deinterlace_parity":       input.DeinterlaceParity,
		"device_backend":           input.DeviceBackend,
		"device_id":                input.DeviceID,
		"width":                    input.Width,
		"height":                   input.Height,
		"fps":                      input.FPS,
		"interlaced":               xmlBool(input.Interlaced, false),
		"field_order":              input.FieldOrder,
		"background":               input.Background,
	}
}

func cgenProgramMappingPayload(mapping cgenProgramMappingXML) map[string]any {
	programs := make([]map[string]any, 0, len(mapping.Programs))
	for _, program := range mapping.Programs {
		audio := make([]map[string]any, 0, len(program.Audio))
		for _, stream := range program.Audio {
			audio = append(audio, map[string]any{
				"track_id": stream.TrackID,
				"pid":      stream.PID,
			})
		}
		row := map[string]any{
			"number":        program.Number,
			"service_name":  program.ServiceName,
			"provider_name": program.ProviderName,
			"pmt_pid":       program.PMTPID,
			"video_pid":     program.VideoPID,
			"audio":         audio,
		}
		if program.SCTE35 != nil {
			row["scte35"] = map[string]any{
				"input":                program.SCTE35.Input,
				"generated_alert_cues": xmlBool(program.SCTE35.GeneratedAlertCues, false),
				"pid":                  program.SCTE35.PID,
			}
		}
		programs = append(programs, row)
	}
	return map[string]any{
		"mode":                mapping.Mode,
		"transport_stream_id": mapping.TransportStreamID,
		"programs":            programs,
	}
}

func cgenOutputsPayload(outputs []cgenOutputXML, encoders cgenEncodersXML) []map[string]any {
	rows := make([]map[string]any, 0, len(outputs))
	for _, output := range outputs {
		row := map[string]any{
			"id":                     output.ID,
			"enabled":                xmlBool(output.Enabled, true),
			"destination":            output.Destination,
			"url":                    output.URL,
			"video_url":              output.VideoURL,
			"audio_urls":             splitCgenCommaList(output.AudioURLs),
			"container":              output.Container,
			"latency_ms":             output.LatencyMS,
			"video_codec":            output.VideoCodec,
			"rate_control":           output.RateControl,
			"video_bitrate_kbps":     output.VideoBitrateKbps,
			"video_max_bitrate_kbps": output.VideoMaxBitrateKbps,
			"gop_frames":             output.GOPFrames,
			"audio_codec":            output.AudioCodec,
			"audio_bitrate_kbps":     output.AudioBitrateKbps,
			"sample_rate":            output.SampleRate,
		}
		if profile, ok := cgenEncoderOutput(encoders, output.ID); ok {
			row["encoder"] = map[string]any{
				"video": cgenEncoderCodecPayload(profile.Video),
				"audio": cgenEncoderCodecPayload(profile.Audio),
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func splitCgenCommaList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func cgenEncoderOutput(encoders cgenEncodersXML, outputID string) (cgenEncoderOutputXML, bool) {
	for _, output := range encoders.Outputs {
		if strings.EqualFold(output.ID, outputID) {
			return output, true
		}
	}
	return cgenEncoderOutputXML{}, false
}

func cgenEncoderCodecPayload(codec cgenEncoderCodecXML) map[string]any {
	return map[string]any{
		"codec":        codec.Codec,
		"bitrate_kbps": codec.BitrateKbps,
		"gop":          codec.GOP,
		"bframes":      codec.BFrames,
		"preset":       codec.Preset,
		"tune":         codec.Tune,
		"profile":      codec.Profile,
		"level":        codec.Level,
		"options":      encoderOptionsPayload(codec.Options),
	}
}

func cgenEncoderFeed(encoders cgenEncodersXML, feedID string) cgenEncoderFeedXML {
	for _, feed := range encoders.Feeds {
		if strings.EqualFold(feed.ID, feedID) {
			return feed
		}
	}
	return cgenEncoderFeedXML{}
}

func encoderOptionsPayload(options []cgenEncoderOptionXML) []map[string]any {
	out := make([]map[string]any, 0, len(options))
	for _, option := range options {
		out = append(out, map[string]any{
			"name":  option.Name,
			"value": option.Value,
		})
	}
	return out
}

func loadCgenRuntimeStatus(configPath string, feedID string) map[string]any {
	path := resolveConfigPath(configPath, filepath.Join("runtime", "cgen", safeCgenRuntimeID(feedID)+".status.json"))
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func safeCgenRuntimeID(value string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, strings.TrimSpace(value))
}

func cleanCgenEndpoint(value cgenEndpointXML) cgenEndpointXML {
	value.URL = strings.TrimSpace(value.URL)
	value.Type = normalizeCgenInputType(value.Type)
	value.Format = strings.TrimSpace(value.Format)
	value.VCodec = strings.TrimSpace(value.VCodec)
	value.ACodec = strings.TrimSpace(value.ACodec)
	value.VideoBitrateKbps = cleanOptionalPositive(value.VideoBitrateKbps)
	value.AudioBitrateKbps = cleanOptionalPositive(value.AudioBitrateKbps)
	value.HardwareDecoder = cleanGstElementName(value.HardwareDecoder)
	value.HardwareDecoderOn = boolText(xmlBool(value.HardwareDecoderOn, false))
	value.DeinterlaceAlgorithm = normalizeCgenDeinterlaceAlgorithm(value.DeinterlaceAlgorithm)
	value.DeinterlaceBackend = normalizeCgenDeinterlaceBackend(value.DeinterlaceBackend)
	value.DeinterlaceCadence = normalizeCgenDeinterlaceCadence(value.DeinterlaceCadence)
	value.DeinterlaceParity = normalizeCgenDeinterlaceParity(value.DeinterlaceParity)
	value.DeviceBackend = normalizeCgenDeviceBackend(value.DeviceBackend)
	value.DeviceID = strings.TrimSpace(value.DeviceID)
	value.Width = cleanOptionalBoundedUint(value.Width, 1, 16384)
	value.Height = cleanOptionalBoundedUint(value.Height, 1, 16384)
	value.FPS = strings.TrimSpace(value.FPS)
	if strings.TrimSpace(value.Interlaced) != "" {
		value.Interlaced = boolText(xmlBool(value.Interlaced, false))
	}
	value.FieldOrder = strings.TrimSpace(value.FieldOrder)
	value.Background = strings.TrimSpace(value.Background)
	if value.Type == "dummy" {
		value.Width = fallbackText(value.Width, "720")
		value.Height = fallbackText(value.Height, "480")
		value.FPS = fallbackText(value.FPS, "30000/1001")
		value.Interlaced = fallbackText(value.Interlaced, "false")
		value.FieldOrder = normalizeCgenFieldOrder(value.FieldOrder)
		value.Background = fallbackText(value.Background, "#000000ff")
	}
	value.ServiceName = strings.TrimSpace(value.ServiceName)
	value.ProviderName = strings.TrimSpace(value.ProviderName)
	value.ServiceID = cleanOptionalPositive(value.ServiceID)
	value.TransportStreamID = cleanOptionalPositive(value.TransportStreamID)
	if value.Type == "uri_or_file" {
		value.Type = ""
		if value.HardwareDecoderOn == "false" {
			value.HardwareDecoderOn = ""
			value.HardwareDecoder = ""
		}
	}
	return value
}

func cleanOptionalBoundedUint(value string, min uint64, max uint64) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return cleanBoundedUint(value, "", min, max)
}

func cleanGstElementName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	for _, ch := range value {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-' {
			continue
		}
		return ""
	}
	return value
}

func normalizeCgenInputType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "device", "v4l2", "directshow", "dshow":
		return "device"
	case "dummy", "none", "no_input":
		return "dummy"
	default:
		return "uri_or_file"
	}
}

func validCgenInputTypeToken(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto", "stream", "uri", "uri_or_file", "url", "file", "device", "v4l2", "directshow", "dshow", "dummy", "none", "no_input":
		return true
	default:
		return false
	}
}

func normalizeCgenDeinterlaceAlgorithm(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case "yadif":
		return "yadif"
	case "bwdif":
		return "bwdif"
	case "motion_adaptive", "motion-adaptive", "adaptive":
		return "motion_adaptive"
	default:
		return ""
	}
}

func validCgenDeinterlaceAlgorithm(value string) bool {
	return strings.TrimSpace(value) == "" || normalizeCgenDeinterlaceAlgorithm(value) != ""
}

func normalizeCgenDeinterlaceBackend(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case "auto":
		return "auto"
	case "software", "cpu":
		return "software"
	case "vaapi", "va":
		return "vaapi"
	case "qsv", "quicksync", "quick_sync":
		return "quicksync"
	case "d3d11", "direct3d11":
		return "d3d11"
	case "cuda", "nvidia":
		return "cuda"
	case "vulkan":
		return "vulkan"
	case "gl", "opengl":
		return "opengl"
	default:
		return ""
	}
}

func validCgenDeinterlaceBackend(value string) bool {
	return strings.TrimSpace(value) == "" || normalizeCgenDeinterlaceBackend(value) != ""
}

func normalizeCgenDeinterlaceCadence(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case "field", "field_rate", "bob":
		return "field"
	case "frame", "frame_rate", "single":
		return "frame"
	default:
		return ""
	}
}

func validCgenDeinterlaceCadence(value string) bool {
	return strings.TrimSpace(value) == "" || normalizeCgenDeinterlaceCadence(value) != ""
}

func normalizeCgenDeinterlaceParity(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case "auto":
		return "auto"
	case "tff", "top", "top_first":
		return "tff"
	case "bff", "bottom", "bottom_first":
		return "bff"
	default:
		return ""
	}
}

func validCgenDeinterlaceParity(value string) bool {
	return strings.TrimSpace(value) == "" || normalizeCgenDeinterlaceParity(value) != ""
}

func validCgenDeinterlaceCombination(input cgenEndpointXML) bool {
	algorithm := input.DeinterlaceAlgorithm
	backend := input.DeinterlaceBackend
	parity := input.DeinterlaceParity
	if backend != "auto" && backend != "software" && parity != "auto" {
		return false
	}
	switch algorithm {
	case "yadif":
		return backend == "auto" || backend == "software" || backend == "cuda"
	case "bwdif":
		return backend == "auto" || backend == "software" || backend == "cuda" || backend == "vulkan"
	case "motion_adaptive":
		return backend == "auto" || backend == "software" || backend == "vaapi" ||
			backend == "quicksync" || backend == "d3d11" || backend == "opengl"
	default:
		return false
	}
}

func validCgenRational(value string) bool {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 2 {
		return false
	}
	numerator, errNumerator := strconv.ParseUint(parts[0], 10, 32)
	denominator, errDenominator := strconv.ParseUint(parts[1], 10, 32)
	return errNumerator == nil && errDenominator == nil && numerator > 0 && denominator > 0
}

func validCgenRGBA(value string) bool {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "#") || (len(value) != 7 && len(value) != 9) {
		return false
	}
	_, err := hex.DecodeString(value[1:])
	return err == nil
}

func normalizeCgenInputFormat(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return "auto", nil
	case "mpegts", "mpeg-ts", "mpeg_ts", "ts":
		return "mpegts", nil
	case "rtp":
		return "rtp", nil
	case "srt":
		return "srt", nil
	case "file":
		return "file", nil
	default:
		return "", fmt.Errorf("program input format %q is unsupported", value)
	}
}

func normalizeCgenDeviceBackend(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "directshow", "dshow":
		return "directshow"
	case "v4l2":
		return "v4l2"
	default:
		return ""
	}
}

func normalizeCgenFieldOrder(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "bff" || value == "bottom" || value == "bottom_first" {
		return "bff"
	}
	return "tff"
}

func normalizeCgenStandard(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "atsc", "pal", "secam":
		return value
	default:
		return ""
	}
}

func normalizeCgenAudioSource(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "routine", "feed", "program_feed":
		return "routine"
	case "both", "priority+routine", "priority_routine", "routine_priority":
		return "both"
	default:
		return "priority"
	}
}

func cleanCgenID(value string) string {
	value = strings.TrimSpace(value)
	if value == "*" {
		return value
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return -1
	}, value)
}

func normalizeCgenMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "overlay", "program", "active":
		return "overlay"
	default:
		return "release"
	}
}

func normalizeCgenStandbyMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "smpte", "bars", "color_bars", "colour_bars":
		return "smpte"
	case "black", "blank", "off", "disabled":
		return "black"
	default:
		return "banner"
	}
}

func normalizeCgenFontWeight(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "thin", "100":
		return "thin"
	case "extra-light", "extralight", "ultra-light", "ultralight", "200":
		return "extra-light"
	case "light", "300":
		return "light"
	case "medium", "500":
		return "medium"
	case "semi-bold", "semibold", "demi-bold", "demibold", "600":
		return "semibold"
	case "bold", "700":
		return "bold"
	case "extra-bold", "extrabold", "ultra-bold", "ultrabold", "800":
		return "extra-bold"
	case "black", "heavy", "900":
		return "black"
	default:
		return "regular"
	}
}

func normalizeCgenScrollRepeatMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "fixed", "fixed_repeats", "count", "count_only":
		return "fixed"
	default:
		return "until_audio_end"
	}
}

func cleanPositive(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return fallback
		}
	}
	return value
}

func cleanNonNegative(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return fallback
		}
	}
	return value
}

func cleanPercent(value string, fallback string) string {
	text := strings.TrimSpace(value)
	if text == "" {
		text = strings.TrimSpace(fallback)
	}
	n, err := strconv.Atoi(text)
	if err != nil {
		n, _ = strconv.Atoi(strings.TrimSpace(fallback))
	}
	if n < 0 {
		n = 0
	}
	if n > 100 {
		n = 100
	}
	return strconv.Itoa(n)
}

func cleanOptionalPositive(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return cleanPositive(value, "")
}

func cleanNumber(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	for i, ch := range value {
		if ch >= '0' && ch <= '9' || ch == '.' || (ch == '-' && i == 0) {
			continue
		}
		return fallback
	}
	return value
}

func cleanColor(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if len(value) == 7 && value[0] == '#' {
		for _, ch := range value[1:] {
			if ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f' || ch >= 'A' && ch <= 'F' {
				continue
			}
			return fallback
		}
		return value
	}
	return fallback
}

func firstStringFromAny(source map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := stringFromAny(source[key]); strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func boolOrTextFromAny(value any, fallback string) string {
	text := strings.TrimSpace(stringFromAny(value))
	if strings.EqualFold(text, "auto") {
		return "auto"
	}
	if text != "" {
		return boolText(boolFromAny(value, xmlBool(fallback, true)))
	}
	return fallback
}

func videoRendition(ladder cgenLadderXML, id string) cgenVideoRenditionXML {
	for _, video := range ladder.Videos {
		if strings.EqualFold(video.ID, id) {
			return video
		}
	}
	return cgenVideoRenditionXML{}
}

func audioRendition(ladder cgenLadderXML, id string) cgenAudioRenditionXML {
	for _, audio := range ladder.Audios {
		if strings.EqualFold(audio.ID, id) {
			return audio
		}
	}
	return cgenAudioRenditionXML{}
}
