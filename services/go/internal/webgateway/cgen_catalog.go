package webgateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

const cgenPreserveNativeTracksAvailable = false

type cgenCatalogEntry struct {
	ID      string `json:"id"`
	Label   string `json:"label"`
	Kind    string `json:"kind,omitempty"`
	Element string `json:"element,omitempty"`
	Source  string `json:"source,omitempty"`
	Preview string `json:"preview,omitempty"`
}

type cgenCatalogBuilder struct {
	formats       map[string]cgenCatalogEntry
	video         map[string]cgenCatalogEntry
	audio         map[string]cgenCatalogEntry
	videoDecoders map[string]cgenCatalogEntry
}

type cgenRuntimeCatalog struct {
	Formats       []cgenCatalogEntry      `json:"formats"`
	VideoCodecs   []cgenCatalogEntry      `json:"video_codecs"`
	AudioCodecs   []cgenCatalogEntry      `json:"audio_codecs"`
	VideoDecoders []cgenCatalogEntry      `json:"video_decoders"`
	Deinterlacers []cgenDeinterlacerEntry `json:"deinterlacers"`
	InputDevices  []cgenInputDevice       `json:"input_devices"`
	Capabilities  cgenRuntimeCapabilities `json:"capabilities"`
}

type cgenDeinterlacerEntry struct {
	ID            string   `json:"id"`
	Label         string   `json:"label"`
	Algorithm     string   `json:"algorithm"`
	Backend       string   `json:"backend"`
	Hardware      bool     `json:"hardware"`
	Available     bool     `json:"available"`
	Elements      []string `json:"elements"`
	Cadences      []string `json:"cadences"`
	ParityControl bool     `json:"parity_control"`
	Reason        string   `json:"reason,omitempty"`
}

type cgenRuntimeCapabilities struct {
	AudioTopologies cgenAudioTopologyCapabilities `json:"audio_topologies"`
	Ancillary       cgenAncillaryCapabilities     `json:"ancillary"`
}

type cgenAudioTopologyCapabilities struct {
	ForceLayout          bool `json:"force_layout"`
	PreserveNativeTracks bool `json:"preserve_native_tracks"`
}

type cgenAncillaryCapabilities struct {
	CaptionReinsertion bool `json:"caption_reinsertion"`
	SCTE35Passthrough  bool `json:"scte35_passthrough"`
	SCTE104Passthrough bool `json:"scte104_passthrough"`
}

type cgenInputDevice struct {
	ID      string `json:"id"`
	Label   string `json:"label"`
	Backend string `json:"backend"`
	Element string `json:"element,omitempty"`
	Class   string `json:"class,omitempty"`
	Caps    string `json:"caps,omitempty"`
}

func cgenCatalogPayload(configPath string) (map[string]any, error) {
	builder := cgenCatalogBuilder{
		formats:       map[string]cgenCatalogEntry{},
		video:         map[string]cgenCatalogEntry{},
		audio:         map[string]cgenCatalogEntry{},
		videoDecoders: map[string]cgenCatalogEntry{},
	}
	runtimeCatalog, runtimeSource := loadCgenRuntimeCatalog(configPath)
	builder.addCatalogEntries(builder.formats, runtimeCatalog.Formats)
	builder.addCatalogEntries(builder.video, runtimeCatalog.VideoCodecs)
	builder.addCatalogEntries(builder.audio, runtimeCatalog.AudioCodecs)
	builder.addCatalogEntries(builder.videoDecoders, runtimeCatalog.VideoDecoders)
	plugins := discoverGStreamerPlugins(configPath)
	inspectPath := findGstInspect(configPath)
	if inspectPath != "" {
		builder.addGstInspectFactories(inspectPath)
	}
	builder.addPluginCatalog(plugins)
	builder.addBaselineCatalog()
	return map[string]any{
		"formats":        builder.sorted(builder.formats),
		"video_codecs":   builder.sorted(builder.video),
		"audio_codecs":   builder.sorted(builder.audio),
		"video_decoders": builder.sorted(builder.videoDecoders),
		"deinterlacers":  runtimeCatalog.Deinterlacers,
		"devices":        cgenInputDevicePayload(runtimeCatalog.InputDevices),
		"fonts":          discoverFonts(configPath),
		"capabilities": map[string]any{
			"audio_topologies": map[string]bool{
				"force_layout":           true,
				"preserve_native_tracks": cgenPreserveNativeTracksAvailable && runtimeCatalog.Capabilities.AudioTopologies.PreserveNativeTracks,
			},
			"ancillary": map[string]bool{
				"caption_reinsertion": runtimeCatalog.Capabilities.Ancillary.CaptionReinsertion,
				"scte35_passthrough":  runtimeCatalog.Capabilities.Ancillary.SCTE35Passthrough,
				"scte104_passthrough": runtimeCatalog.Capabilities.Ancillary.SCTE104Passthrough,
			},
		},
		"gstreamer": map[string]any{
			"inspect":      inspectPath,
			"plugin_count": len(plugins),
			"source":       cgenCatalogSource(runtimeSource, inspectPath),
		},
	}, nil
}

func cgenCatalogSource(runtimeSource string, inspectPath string) string {
	if strings.TrimSpace(runtimeSource) != "" {
		return runtimeSource
	}
	if inspectPath != "" {
		return "gst-inspect"
	}
	return "plugin-scan"
}

func loadCgenRuntimeCatalog(configPath string) (cgenRuntimeCatalog, string) {
	executable := findCgenCatalogExecutable(configPath)
	if executable == "" {
		return cgenRuntimeCatalog{}, ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "--gst-catalog")
	binDir := filepath.Dir(executable)
	cmd.Dir = binDir
	cmd.Env = cgenCatalogCommandEnv(binDir)
	output, err := cmd.Output()
	if err != nil || len(output) == 0 {
		return cgenRuntimeCatalog{}, ""
	}
	var payload cgenRuntimeCatalog
	if err := json.Unmarshal(output, &payload); err != nil {
		return cgenRuntimeCatalog{}, ""
	}
	if len(payload.Formats)+len(payload.VideoCodecs)+len(payload.AudioCodecs)+len(payload.VideoDecoders)+len(payload.Deinterlacers)+len(payload.InputDevices) == 0 {
		return cgenRuntimeCatalog{}, ""
	}
	return payload, "haze-cgen-registry"
}

func cgenInputDevicePayload(devices []cgenInputDevice) []map[string]any {
	seen := make(map[string]struct{}, len(devices))
	result := make([]map[string]any, 0, len(devices))
	for _, device := range devices {
		id := cleanCgenText(device.ID, "", 512)
		label := cleanCgenText(device.Label, id, 512)
		backend := strings.ToLower(strings.TrimSpace(device.Backend))
		switch {
		case strings.Contains(backend, "v4l2"), strings.Contains(backend, "video4linux"):
			backend = "v4l2"
		case strings.Contains(backend, "directshow"), strings.Contains(backend, "dshow"):
			backend = "directshow"
		default:
			continue
		}
		if id == "" || label == "" {
			continue
		}
		key := backend + "\x00" + strings.ToLower(id)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		item := map[string]any{
			"id":      id,
			"label":   label,
			"backend": backend,
			"source":  "haze-cgen-registry",
		}
		if element := cleanCgenToken(device.Element); element != "" {
			item["element"] = element
		}
		if class := cleanCgenText(device.Class, "", 256); class != "" {
			item["class"] = class
		}
		if caps := cleanCgenText(device.Caps, "", 4096); caps != "" {
			item["caps"] = caps
		}
		result = append(result, item)
	}
	sort.Slice(result, func(i int, j int) bool {
		left := strings.ToLower(stringValue(result[i], "label"))
		right := strings.ToLower(stringValue(result[j], "label"))
		if left == right {
			return stringValue(result[i], "id") < stringValue(result[j], "id")
		}
		return left < right
	})
	return result
}

func prependPathEnv(dir string) string {
	name := "PATH"
	current := os.Getenv(name)
	for _, env := range os.Environ() {
		if strings.HasPrefix(strings.ToUpper(env), "PATH=") {
			name = env[:strings.IndexByte(env, '=')]
			break
		}
	}
	if current == "" {
		return name + "=" + dir
	}
	return name + "=" + dir + string(os.PathListSeparator) + current
}

func cgenCatalogCommandEnv(binDir string) []string {
	env := append([]string{}, os.Environ()...)
	env = append(env, prependPathEnv(binDir))
	pluginDir := filepath.Join(binDir, "gstreamer-1.0")
	if info, err := os.Stat(pluginDir); err == nil && info.IsDir() {
		env = append(env,
			"GST_PLUGIN_PATH="+pluginDir,
			"GST_PLUGIN_SYSTEM_PATH_1_0="+pluginDir,
			"GST_PLUGIN_PATH_1_0="+pluginDir,
		)
		scanner := filepath.Join(pluginDir, "gst-plugin-scanner.exe")
		if runtime.GOOS != "windows" {
			scanner = filepath.Join(pluginDir, "gst-plugin-scanner")
		}
		if info, err := os.Stat(scanner); err == nil && !info.IsDir() {
			env = append(env, "GST_PLUGIN_SCANNER="+scanner)
		}
	}
	return env
}

func findCgenCatalogExecutable(configPath string) string {
	names := []string{"haze-cgen"}
	if runtime.GOOS == "windows" {
		names = []string{"haze-cgen.exe", "haze-cgen"}
	}
	candidates := []string{}
	if configured := strings.TrimSpace(os.Getenv("HAZE_CGEN_EXECUTABLE")); configured != "" {
		candidates = append(candidates, configured)
	}
	base := filepath.Dir(filepath.Clean(configPath))
	for _, name := range names {
		if path, err := exec.LookPath(name); err == nil {
			candidates = append(candidates, path)
		}
		candidates = append(candidates,
			filepath.Join(base, "bin", name),
			filepath.Join(base, name),
			filepath.Join(base, "dist", "Haze_UAP-Windows-x86_64-Portable", "bin", name),
			filepath.Join(base, "dist", "Haze_UAP-Linux-x86_64-Portable", "bin", name),
		)
	}
	for _, candidate := range candidates {
		candidate = filepath.Clean(candidate)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return ""
}

func (builder *cgenCatalogBuilder) addFormat(id string, label string, kind string, source string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	builder.formats[id] = mergeCgenCatalogEntry(builder.formats[id], cgenCatalogEntry{
		ID:     id,
		Label:  fallbackText(label, id),
		Kind:   kind,
		Source: source,
	})
}

func (builder *cgenCatalogBuilder) addVideo(id string, label string, element string, source string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	builder.video[id] = mergeCgenCatalogEntry(builder.video[id], cgenCatalogEntry{
		ID:      id,
		Label:   fallbackText(label, id),
		Element: element,
		Source:  source,
	})
}

func (builder *cgenCatalogBuilder) addAudio(id string, label string, element string, source string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	builder.audio[id] = mergeCgenCatalogEntry(builder.audio[id], cgenCatalogEntry{
		ID:      id,
		Label:   fallbackText(label, id),
		Element: element,
		Source:  source,
	})
}

func (builder *cgenCatalogBuilder) addVideoDecoder(id string, label string, element string, source string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	builder.videoDecoders[id] = mergeCgenCatalogEntry(builder.videoDecoders[id], cgenCatalogEntry{
		ID:      id,
		Label:   fallbackText(label, id),
		Kind:    "video",
		Element: element,
		Source:  source,
	})
}

func (builder *cgenCatalogBuilder) addCatalogEntries(target map[string]cgenCatalogEntry, entries []cgenCatalogEntry) {
	for _, entry := range entries {
		entry.ID = strings.TrimSpace(entry.ID)
		if entry.ID == "" {
			continue
		}
		entry.Label = fallbackText(entry.Label, entry.ID)
		target[entry.ID] = mergeCgenCatalogEntry(target[entry.ID], entry)
	}
}

func mergeCgenCatalogEntry(existing cgenCatalogEntry, next cgenCatalogEntry) cgenCatalogEntry {
	if existing.ID == "" {
		return next
	}
	existing.Label = fallbackText(existing.Label, next.Label)
	existing.Kind = mergeCatalogText(existing.Kind, next.Kind)
	existing.Element = mergeCatalogText(existing.Element, next.Element)
	existing.Source = mergeCatalogText(existing.Source, next.Source)
	existing.Preview = fallbackText(existing.Preview, next.Preview)
	return existing
}

func mergeCatalogText(existing string, next string) string {
	existing = strings.TrimSpace(existing)
	next = strings.TrimSpace(next)
	if existing == "" {
		return next
	}
	if next == "" || catalogTextContains(existing, next) {
		return existing
	}
	return existing + ", " + next
}

func catalogTextContains(existing string, next string) bool {
	for _, part := range strings.Split(existing, ",") {
		if strings.EqualFold(strings.TrimSpace(part), strings.TrimSpace(next)) {
			return true
		}
	}
	return false
}

func (builder *cgenCatalogBuilder) sorted(entries map[string]cgenCatalogEntry) []map[string]any {
	values := make([]cgenCatalogEntry, 0, len(entries))
	for _, entry := range entries {
		values = append(values, entry)
	}
	sort.Slice(values, func(i int, j int) bool {
		return strings.ToLower(values[i].Label) < strings.ToLower(values[j].Label)
	})
	result := make([]map[string]any, 0, len(values))
	for _, entry := range values {
		item := map[string]any{
			"id":    entry.ID,
			"label": entry.Label,
		}
		if entry.Kind != "" {
			item["kind"] = entry.Kind
		}
		if entry.Element != "" {
			item["element"] = entry.Element
		}
		if entry.Source != "" {
			item["source"] = entry.Source
		}
		if entry.Preview != "" {
			item["preview"] = entry.Preview
		}
		result = append(result, item)
	}
	return result
}

func (builder *cgenCatalogBuilder) addBaselineCatalog() {
	builder.addFormat("mpegts", "MPEG-TS", "container", "baseline")
	builder.addFormat("rtp", "RTP", "network", "baseline")
	builder.addFormat("udp", "UDP", "network", "baseline")
	builder.addVideo("avenc_mpeg2video", "MPEG-2 Video - libav (avenc_mpeg2video)", "avenc_mpeg2video", "baseline")
	builder.addVideo("x264enc", "H.264 / AVC - x264 software (x264enc)", "x264enc", "baseline")
	builder.addAudio("avenc_ac3", "AC-3 - libav (avenc_ac3)", "avenc_ac3", "baseline")
	builder.addAudio("avenc_eac3", "E-AC-3 - libav (avenc_eac3)", "avenc_eac3", "baseline")
	builder.addAudio("avenc_aac", "AAC - libav (avenc_aac)", "avenc_aac", "baseline")
	builder.addVideoDecoder("avdec_h264", "H.264 / AVC - libav (avdec_h264)", "avdec_h264", "baseline")
	builder.addVideoDecoder("avdec_mpeg2video", "MPEG-2 Video - libav (avdec_mpeg2video)", "avdec_mpeg2video", "baseline")
}

func (builder *cgenCatalogBuilder) addPluginCatalog(plugins map[string]bool) {
	if plugins["mpegtsmux"] {
		builder.addFormat("mpegts", "MPEG-TS", "container", "libgstmpegtsmux")
	}
	if plugins["isomp4"] {
		builder.addFormat("mp4", "MPEG-4 / MP4", "container", "libgstisomp4")
		builder.addFormat("mov", "QuickTime MOV", "container", "libgstisomp4")
	}
	if plugins["matroska"] {
		builder.addFormat("matroska", "Matroska", "container", "libgstmatroska")
		builder.addFormat("webm", "WebM", "container", "libgstmatroska")
	}
	if plugins["flv"] {
		builder.addFormat("flv", "FLV", "container", "libgstflv")
	}
	if plugins["asfmux"] || plugins["asf"] {
		builder.addFormat("asf", "ASF / Windows Media", "container", "libgstasf")
	}
	if plugins["avi"] {
		builder.addFormat("avi", "AVI", "container", "libgstavi")
	}
	if plugins["mpegpsmux"] {
		builder.addFormat("mpegps", "MPEG Program Stream", "container", "libgstmpegpsmux")
	}
	if plugins["mxf"] {
		builder.addFormat("mxf", "MXF", "container", "libgstmxf")
	}
	if plugins["ogg"] {
		builder.addFormat("ogg", "Ogg", "container", "libgstogg")
	}
	if plugins["wavenc"] {
		builder.addFormat("wav", "WAV", "container", "libgstwavenc")
	}
	if plugins["rtmp"] || plugins["rtmp2"] {
		builder.addFormat("rtmp", "RTMP", "network", "libgstrtmp")
	}
	if plugins["srt"] {
		builder.addFormat("srt", "SRT", "network", "libgstsrt")
	}
	if plugins["rtp"] || plugins["rtpmanager"] {
		builder.addFormat("rtp", "RTP", "network", "libgstrtp")
	}
	if plugins["udp"] {
		builder.addFormat("udp", "UDP", "network", "libgstudp")
	}
	if plugins["tcp"] {
		builder.addFormat("tcp", "TCP", "network", "libgsttcp")
	}
	if plugins["hls"] {
		builder.addFormat("hls", "HLS", "segment", "libgsthls")
	}
	if plugins["dash"] {
		builder.addFormat("dash", "MPEG-DASH", "segment", "libgstdash")
	}

	if plugins["libav"] {
		builder.addVideo("avenc_mpeg2video", "MPEG-2 Video - libav (avenc_mpeg2video)", "avenc_mpeg2video", "libgstlibav")
		builder.addVideo("avenc_h264", "H.264 / AVC - libav (avenc_h264)", "avenc_h264", "libgstlibav")
		builder.addVideo("avenc_hevc", "H.265 / HEVC - libav (avenc_hevc)", "avenc_hevc", "libgstlibav")
		builder.addVideo("avenc_mpeg4", "MPEG-4 Part 2 - libav (avenc_mpeg4)", "avenc_mpeg4", "libgstlibav")
		builder.addAudio("avenc_ac3", "AC-3 - libav (avenc_ac3)", "avenc_ac3", "libgstlibav")
		builder.addAudio("avenc_eac3", "E-AC-3 - libav (avenc_eac3)", "avenc_eac3", "libgstlibav")
		builder.addAudio("avenc_aac", "AAC - libav (avenc_aac)", "avenc_aac", "libgstlibav")
		builder.addAudio("avenc_mp2", "MPEG Layer II Audio - libav (avenc_mp2)", "avenc_mp2", "libgstlibav")
		builder.addAudio("avenc_mp3", "MP3 - libav (avenc_mp3)", "avenc_mp3", "libgstlibav")
		builder.addAudio("avenc_flac", "FLAC - libav (avenc_flac)", "avenc_flac", "libgstlibav")
		builder.addVideoDecoder("avdec_h264", "H.264 / AVC - libav (avdec_h264)", "avdec_h264", "libgstlibav")
		builder.addVideoDecoder("avdec_hevc", "H.265 / HEVC - libav (avdec_hevc)", "avdec_hevc", "libgstlibav")
		builder.addVideoDecoder("avdec_mpeg2video", "MPEG-2 Video - libav (avdec_mpeg2video)", "avdec_mpeg2video", "libgstlibav")
	}
	if plugins["x264"] {
		builder.addVideo("x264enc", "H.264 / AVC - x264 software (x264enc)", "x264enc", "libgstx264")
	}
	if plugins["openh264"] {
		builder.addVideo("openh264enc", "H.264 / AVC - OpenH264 (openh264enc)", "openh264enc", "libgstopenh264")
	}
	if plugins["x265"] || plugins["de265"] {
		builder.addVideo("x265enc", "H.265 / HEVC - x265 software (x265enc)", "x265enc", "libgstx265")
	}
	if plugins["vpx"] {
		builder.addVideo("vp8enc", "VP8 - libvpx (vp8enc)", "vp8enc", "libgstvpx")
		builder.addVideo("vp9enc", "VP9 - libvpx (vp9enc)", "vp9enc", "libgstvpx")
	}
	if plugins["aom"] {
		builder.addVideo("av1enc", "AV1 - AOM (av1enc)", "av1enc", "libgstaom")
	}
	if plugins["svtav1"] {
		builder.addVideo("svtav1enc", "AV1 - SVT-AV1 (svtav1enc)", "svtav1enc", "libgstsvtav1")
	}
	if plugins["theora"] {
		builder.addVideo("theoraenc", "Theora (theoraenc)", "theoraenc", "libgsttheora")
	}
	if plugins["jpeg"] {
		builder.addVideo("jpegenc", "Motion JPEG (jpegenc)", "jpegenc", "libgstjpeg")
	}
	if plugins["png"] {
		builder.addVideo("pngenc", "PNG (pngenc)", "pngenc", "libgstpng")
	}
	if plugins["webp"] {
		builder.addVideo("webpenc", "WebP (webpenc)", "webpenc", "libgstwebp")
	}
	if plugins["nvcodec"] {
		builder.addVideo("nvh264enc", "H.264 / AVC - NVIDIA NVENC (nvh264enc)", "nvh264enc", "libgstnvcodec")
		builder.addVideo("nvh265enc", "H.265 / HEVC - NVIDIA NVENC (nvh265enc)", "nvh265enc", "libgstnvcodec")
		builder.addVideo("nvav1enc", "AV1 - NVIDIA NVENC (nvav1enc)", "nvav1enc", "libgstnvcodec")
		builder.addVideoDecoder("nvh264dec", "H.264 / AVC - NVIDIA NVDEC (nvh264dec)", "nvh264dec", "libgstnvcodec")
		builder.addVideoDecoder("nvh265dec", "H.265 / HEVC - NVIDIA NVDEC (nvh265dec)", "nvh265dec", "libgstnvcodec")
		builder.addVideoDecoder("nvmpeg2videodec", "MPEG-2 Video - NVIDIA NVDEC (nvmpeg2videodec)", "nvmpeg2videodec", "libgstnvcodec")
	}
	if plugins["amfcodec"] {
		builder.addVideo("amfh264enc", "H.264 / AVC - AMD AMF (amfh264enc)", "amfh264enc", "libgstamfcodec")
		builder.addVideo("amfh265enc", "H.265 / HEVC - AMD AMF (amfh265enc)", "amfh265enc", "libgstamfcodec")
	}
	if plugins["qsv"] {
		builder.addVideo("qsvh264enc", "H.264 / AVC - Intel Quick Sync (qsvh264enc)", "qsvh264enc", "libgstqsv")
		builder.addVideo("qsvh265enc", "H.265 / HEVC - Intel Quick Sync (qsvh265enc)", "qsvh265enc", "libgstqsv")
		builder.addVideo("qsvmpeg2enc", "MPEG-2 Video - Intel Quick Sync (qsvmpeg2enc)", "qsvmpeg2enc", "libgstqsv")
		builder.addVideoDecoder("qsvh264dec", "H.264 / AVC - Intel Quick Sync (qsvh264dec)", "qsvh264dec", "libgstqsv")
		builder.addVideoDecoder("qsvh265dec", "H.265 / HEVC - Intel Quick Sync (qsvh265dec)", "qsvh265dec", "libgstqsv")
		builder.addVideoDecoder("qsvmpeg2dec", "MPEG-2 Video - Intel Quick Sync (qsvmpeg2dec)", "qsvmpeg2dec", "libgstqsv")
	}
	if plugins["faac"] || plugins["fdkaac"] {
		builder.addAudio("faac", "AAC - FAAC/fdk-aac", "faac/fdkaac", "libgstfaac")
	}
	if plugins["opus"] {
		builder.addAudio("opusenc", "Opus (opusenc)", "opusenc", "libgstopus")
	}
	if plugins["vorbis"] {
		builder.addAudio("vorbisenc", "Vorbis (vorbisenc)", "vorbisenc", "libgstvorbis")
	}
	if plugins["flac"] {
		builder.addAudio("flacenc", "FLAC (flacenc)", "flacenc", "libgstflac")
	}
	if plugins["lame"] {
		builder.addAudio("lamemp3enc", "MP3 - LAME (lamemp3enc)", "lamemp3enc", "libgstlame")
	}
	if plugins["twolame"] {
		builder.addAudio("twolame", "MPEG Layer II Audio - TwoLAME (twolame)", "twolame", "libgsttwolame")
	}
	if plugins["alaw"] {
		builder.addAudio("alawenc", "A-law (alawenc)", "alawenc", "libgstalaw")
	}
	if plugins["mulaw"] {
		builder.addAudio("mulawenc", "mu-law (mulawenc)", "mulawenc", "libgstmulaw")
	}
	if plugins["speex"] {
		builder.addAudio("speexenc", "Speex (speexenc)", "speexenc", "libgstspeex")
	}
	if plugins["wavpack"] {
		builder.addAudio("wavpackenc", "WavPack (wavpackenc)", "wavpackenc", "libgstwavpack")
	}
}

func (builder *cgenCatalogBuilder) addGstInspectFactories(inspectPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, inspectPath)
	output, err := command.Output()
	if err != nil || len(output) == 0 {
		return
	}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 3 {
			continue
		}
		element := strings.TrimSpace(parts[1])
		description := strings.TrimSpace(parts[2])
		builder.addGstFactory(element, description)
	}
}

func (builder *cgenCatalogBuilder) addGstFactory(element string, description string) {
	lower := strings.ToLower(element + " " + description)
	switch {
	case strings.Contains(lower, "mpeg-ts") || strings.Contains(lower, "mpeg transport") || element == "mpegtsmux":
		builder.addFormat("mpegts", "MPEG-TS", "container", element)
	case strings.Contains(lower, "mp4") || strings.Contains(lower, "quicktime"):
		builder.addFormat("mp4", "MPEG-4 / MP4", "container", element)
	case strings.Contains(lower, "matroska"):
		builder.addFormat("matroska", "Matroska", "container", element)
	case strings.Contains(lower, "webm"):
		builder.addFormat("webm", "WebM", "container", element)
	case strings.Contains(lower, "flv"):
		builder.addFormat("flv", "FLV", "container", element)
	}
	if !isGstEncoderFactory(element, description) {
		if isGstVideoDecoderFactory(element, description) {
			switch {
			case strings.Contains(lower, "h.264") || strings.Contains(lower, "h264") || strings.Contains(lower, "avc"):
				builder.addVideoDecoder(element, gstFactoryLabel("H.264 / AVC", element), element, "gst-inspect")
			case strings.Contains(lower, "h.265") || strings.Contains(lower, "h265") || strings.Contains(lower, "hevc"):
				builder.addVideoDecoder(element, gstFactoryLabel("H.265 / HEVC", element), element, "gst-inspect")
			case strings.Contains(lower, "mpeg-2") || strings.Contains(lower, "mpeg2"):
				builder.addVideoDecoder(element, gstFactoryLabel("MPEG-2 Video", element), element, "gst-inspect")
			case strings.Contains(lower, "av1"):
				builder.addVideoDecoder(element, gstFactoryLabel("AV1", element), element, "gst-inspect")
			case strings.Contains(lower, "vp9"):
				builder.addVideoDecoder(element, gstFactoryLabel("VP9", element), element, "gst-inspect")
			case strings.Contains(lower, "vp8"):
				builder.addVideoDecoder(element, gstFactoryLabel("VP8", element), element, "gst-inspect")
			}
		}
		return
	}
	switch {
	case strings.Contains(lower, "h.264") || strings.Contains(lower, "h264") || strings.Contains(lower, "avc"):
		builder.addVideo(element, gstFactoryLabel("H.264 / AVC", element), element, "gst-inspect")
	case strings.Contains(lower, "h.265") || strings.Contains(lower, "h265") || strings.Contains(lower, "hevc"):
		builder.addVideo(element, gstFactoryLabel("H.265 / HEVC", element), element, "gst-inspect")
	case strings.Contains(lower, "mpeg-2") || strings.Contains(lower, "mpeg2"):
		builder.addVideo(element, gstFactoryLabel("MPEG-2 Video", element), element, "gst-inspect")
	case strings.Contains(lower, "av1"):
		builder.addVideo(element, gstFactoryLabel("AV1", element), element, "gst-inspect")
	case strings.Contains(lower, "vp9"):
		builder.addVideo(element, gstFactoryLabel("VP9", element), element, "gst-inspect")
	case strings.Contains(lower, "vp8"):
		builder.addVideo(element, gstFactoryLabel("VP8", element), element, "gst-inspect")
	case strings.Contains(lower, "theora"):
		builder.addVideo(element, gstFactoryLabel("Theora", element), element, "gst-inspect")
	case strings.Contains(lower, "jpeg"):
		builder.addVideo(element, gstFactoryLabel("Motion JPEG", element), element, "gst-inspect")
	case strings.Contains(lower, "webp"):
		builder.addVideo(element, gstFactoryLabel("WebP", element), element, "gst-inspect")
	case strings.Contains(lower, "e-ac-3") || strings.Contains(lower, "eac3"):
		builder.addAudio(element, gstFactoryLabel("E-AC-3", element), element, "gst-inspect")
	case strings.Contains(lower, "ac-3") || strings.Contains(lower, "ac3"):
		builder.addAudio(element, gstFactoryLabel("AC-3", element), element, "gst-inspect")
	case strings.Contains(lower, "aac"):
		builder.addAudio(element, gstFactoryLabel("AAC", element), element, "gst-inspect")
	case strings.Contains(lower, "opus"):
		builder.addAudio(element, gstFactoryLabel("Opus", element), element, "gst-inspect")
	case strings.Contains(lower, "vorbis"):
		builder.addAudio(element, gstFactoryLabel("Vorbis", element), element, "gst-inspect")
	case strings.Contains(lower, "flac"):
		builder.addAudio(element, gstFactoryLabel("FLAC", element), element, "gst-inspect")
	case strings.Contains(lower, "mp3"):
		builder.addAudio(element, gstFactoryLabel("MP3", element), element, "gst-inspect")
	case strings.Contains(lower, "mpeg layer ii") || strings.Contains(lower, "mp2"):
		builder.addAudio(element, gstFactoryLabel("MPEG Layer II Audio", element), element, "gst-inspect")
	case strings.Contains(lower, "speex"):
		builder.addAudio(element, gstFactoryLabel("Speex", element), element, "gst-inspect")
	}
}

func gstFactoryLabel(codec string, element string) string {
	implementation := gstFactoryImplementation(element)
	if implementation == "" {
		return codec + " (" + element + ")"
	}
	return codec + " - " + implementation + " (" + element + ")"
}

func gstFactoryImplementation(element string) string {
	switch strings.ToLower(strings.TrimSpace(element)) {
	case "x264enc":
		return "x264 software"
	case "x265enc":
		return "x265 software"
	case "openh264enc":
		return "OpenH264"
	case "amfh264enc", "amfh265enc", "amfav1enc":
		return "AMD AMF"
	case "nvh264enc", "nvh265enc", "nvav1enc":
		return "NVIDIA NVENC"
	case "nvh264dec", "nvh265dec", "nvav1dec", "nvmpeg2videodec":
		return "NVIDIA NVDEC"
	case "qsvh264enc", "qsvh265enc", "qsvmpeg2enc", "qsvav1enc":
		return "Intel Quick Sync"
	case "qsvh264dec", "qsvh265dec", "qsvmpeg2dec", "qsvav1dec":
		return "Intel Quick Sync"
	case "svtav1enc":
		return "SVT-AV1"
	case "av1enc", "aomav1enc":
		return "AOM"
	case "vp8enc", "vp9enc":
		return "libvpx"
	case "opusenc":
		return "Opus"
	case "vorbisenc":
		return "Vorbis"
	case "flacenc":
		return "FLAC"
	case "lamemp3enc":
		return "LAME"
	case "twolame":
		return "TwoLAME"
	default:
		if strings.HasPrefix(strings.ToLower(element), "avenc_") {
			return "libav"
		}
		if strings.HasPrefix(strings.ToLower(element), "avdec_") {
			return "libav"
		}
		return ""
	}
}

func isGstEncoderFactory(element string, description string) bool {
	element = strings.ToLower(strings.TrimSpace(element))
	description = strings.ToLower(strings.TrimSpace(description))
	if element == "" {
		return false
	}
	blocked := []string{
		"decoder",
		"demux",
		"depay",
		"depayloader",
		"parser",
		"sink",
	}
	for _, marker := range blocked {
		if strings.Contains(description, marker) {
			return false
		}
	}
	if strings.HasPrefix(element, "avdec_") ||
		strings.Contains(element, "decode") ||
		strings.Contains(element, "dec_") ||
		strings.HasSuffix(element, "dec") ||
		strings.HasSuffix(element, "parse") ||
		strings.HasSuffix(element, "demux") {
		return false
	}
	return strings.HasPrefix(element, "avenc_") ||
		strings.HasPrefix(element, "nv") && strings.HasSuffix(element, "enc") ||
		strings.HasPrefix(element, "qsv") && strings.HasSuffix(element, "enc") ||
		strings.HasPrefix(element, "amf") && strings.HasSuffix(element, "enc") ||
		strings.HasSuffix(element, "enc") ||
		strings.Contains(description, "encoder")
}

func isGstVideoDecoderFactory(element string, description string) bool {
	element = strings.ToLower(strings.TrimSpace(element))
	description = strings.ToLower(strings.TrimSpace(description))
	if element == "" {
		return false
	}
	if strings.Contains(description, "audio") {
		return false
	}
	return strings.HasPrefix(element, "avdec_") ||
		strings.HasPrefix(element, "nv") && strings.HasSuffix(element, "dec") ||
		strings.HasPrefix(element, "qsv") && strings.HasSuffix(element, "dec") ||
		strings.HasPrefix(element, "amf") && strings.HasSuffix(element, "dec") ||
		strings.HasSuffix(element, "dec") && strings.Contains(description, "decoder") ||
		strings.Contains(description, "video decoder")
}

func discoverGStreamerPlugins(configPath string) map[string]bool {
	plugins := map[string]bool{}
	for _, dir := range gstreamerPluginDirs(configPath) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := strings.ToLower(entry.Name())
			if !strings.HasPrefix(name, "libgst") {
				continue
			}
			for _, ext := range []string{".dll.a", ".dll", ".so", ".dylib", ".a"} {
				name = strings.TrimSuffix(name, ext)
			}
			name = strings.TrimPrefix(name, "libgst")
			if name != "" {
				plugins[name] = true
			}
		}
	}
	return plugins
}

func gstreamerPluginDirs(configPath string) []string {
	base := filepath.Dir(filepath.Clean(configPath))
	candidates := []string{
		filepath.Join(base, "bin", "gstreamer-1.0"),
		filepath.Join(base, "gstreamer-1.0"),
		filepath.Join(base, "dist", "Haze_UAP-Windows-x86_64-Portable", "bin", "gstreamer-1.0"),
		filepath.Join(base, "dist", "Haze_UAP-Linux-x86_64-Portable", "bin", "gstreamer-1.0"),
	}
	for _, envName := range []string{"GST_PLUGIN_PATH_1_0", "GST_PLUGIN_PATH"} {
		for _, part := range filepath.SplitList(os.Getenv(envName)) {
			if strings.TrimSpace(part) != "" {
				candidates = append(candidates, part)
			}
		}
	}
	return uniqueCleanPaths(candidates)
}

func findGstInspect(configPath string) string {
	names := []string{"gst-inspect-1.0"}
	if runtime.GOOS == "windows" {
		names = append([]string{"gst-inspect-1.0.exe"}, names...)
	}
	for _, name := range names {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	base := filepath.Dir(filepath.Clean(configPath))
	candidates := []string{
		filepath.Join(base, "bin", names[0]),
		filepath.Join(base, "dist", "Haze_UAP-Windows-x86_64-Portable", "bin", names[0]),
		filepath.Join(base, "dist", "Haze_UAP-Linux-x86_64-Portable", "bin", names[len(names)-1]),
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return ""
}

func uniqueCleanPaths(values []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		cleaned := filepath.Clean(strings.TrimSpace(value))
		if cleaned == "." || cleaned == "" || seen[strings.ToLower(cleaned)] {
			continue
		}
		seen[strings.ToLower(cleaned)] = true
		result = append(result, cleaned)
	}
	return result
}

func discoverFonts(configPath string) []map[string]any {
	managed := discoverManagedFonts(configPath)
	managedIDs := map[string]bool{}
	for _, font := range managed {
		if id := strings.TrimSpace(stringValue(font, "id")); id != "" {
			managedIDs[strings.ToLower(id)] = true
		}
	}
	families := map[string]string{}
	for _, family := range []string{"Arial", "Segoe UI", "Tahoma", "Verdana", "Consolas"} {
		families[strings.ToLower(family)] = family
	}
	for _, family := range fontFamiliesFromFcList() {
		families[strings.ToLower(family)] = family
	}
	for _, family := range fontFamiliesFromWindowsRegistry() {
		families[strings.ToLower(family)] = family
	}
	for _, dir := range systemFontDirs() {
		_ = filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry == nil || entry.IsDir() {
				return nil
			}
			if family := fontFamilyFromFilename(entry.Name()); family != "" {
				families[strings.ToLower(family)] = family
			}
			return nil
		})
	}
	names := make([]string, 0, len(families))
	for key, family := range families {
		if managedIDs[key] {
			continue
		}
		names = append(names, family)
	}
	sort.Slice(names, func(i int, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	result := make([]map[string]any, 0, len(managed)+len(names))
	result = append(result, managed...)
	for _, family := range names {
		result = append(result, map[string]any{
			"id":      family,
			"label":   family,
			"source":  "system",
			"preview": "The quick brown fox 0123456789",
		})
	}
	return result
}

func discoverManagedFonts(configPath string) []map[string]any {
	dir := resolveConfigPath(configPath, filepath.Join("managed", "fonts"))
	entries := []map[string]any{}
	_ = filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry == nil {
			return nil
		}
		if strings.HasPrefix(entry.Name(), ".") {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		ext := managedFontExtension(entry.Name())
		if ext == "" {
			return nil
		}
		family := fontFamilyFromManagedFilename(entry.Name())
		if family == "" {
			return nil
		}
		rel := ""
		if relPath, err := filepath.Rel(filepath.Dir(filepath.Clean(configPath)), path); err == nil {
			rel = filepath.ToSlash(relPath)
		}
		fontRel := ""
		if relPath, err := filepath.Rel(dir, path); err == nil {
			fontRel = filepath.ToSlash(relPath)
		}
		item := map[string]any{
			"id":        family,
			"label":     fmt.Sprintf("(*) %s (%s)", family, ext),
			"source":    "managed",
			"extension": ext,
			"path":      rel,
			"preview":   "The quick brown fox 0123456789",
		}
		if assetURL := managedFontAssetURL(fontRel); assetURL != "" {
			item["url"] = assetURL
		}
		entries = append(entries, item)
		return nil
	})
	sort.Slice(entries, func(i int, j int) bool {
		return strings.ToLower(stringValue(entries[i], "label")) < strings.ToLower(stringValue(entries[j], "label"))
	})
	seen := map[string]bool{}
	out := entries[:0]
	for _, entry := range entries {
		id := strings.ToLower(strings.TrimSpace(stringValue(entry, "id")))
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, entry)
	}
	return out
}

func fontFamiliesFromWindowsRegistry() []string {
	if runtime.GOOS != "windows" {
		return nil
	}
	keys := []string{
		`HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Fonts`,
		`HKCU\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Fonts`,
	}
	families := map[string]bool{}
	for _, key := range keys {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		output, err := exec.CommandContext(ctx, "reg", "query", key).Output()
		cancel()
		if err != nil || len(output) == 0 {
			continue
		}
		scanner := bufio.NewScanner(bytes.NewReader(output))
		for scanner.Scan() {
			if family := fontFamilyFromRegistryLine(scanner.Text()); family != "" {
				families[family] = true
			}
		}
	}
	result := make([]string, 0, len(families))
	for family := range families {
		result = append(result, family)
	}
	return result
}

func fontFamilyFromRegistryLine(line string) string {
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return ""
	}
	regIndex := -1
	for index, field := range fields {
		if strings.HasPrefix(field, "REG_") {
			regIndex = index
			break
		}
	}
	if regIndex <= 0 {
		return ""
	}
	name := strings.Join(fields[:regIndex], " ")
	name = regexp.MustCompile(`(?i)\s*\((true|open)type\)\s*$`).ReplaceAllString(name, "")
	name = fontStyleSuffixRE.ReplaceAllString(name, "")
	name = strings.Join(strings.Fields(name), " ")
	if len(name) < 2 || strings.HasPrefix(name, "HKEY_") {
		return ""
	}
	return name
}

func fontFamiliesFromFcList() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "fc-list", ":", "family").Output()
	if err != nil || len(output) == 0 {
		return nil
	}
	families := map[string]bool{}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		for _, family := range strings.Split(scanner.Text(), ",") {
			family = strings.TrimSpace(family)
			if family != "" {
				families[family] = true
			}
		}
	}
	result := make([]string, 0, len(families))
	for family := range families {
		result = append(result, family)
	}
	return result
}

func systemFontDirs() []string {
	var candidates []string
	switch runtime.GOOS {
	case "windows":
		windir := fallbackText(os.Getenv("WINDIR"), `C:\Windows`)
		candidates = append(candidates,
			filepath.Join(windir, "Fonts"),
			filepath.Join(os.Getenv("LOCALAPPDATA"), "Microsoft", "Windows", "Fonts"),
		)
	case "darwin":
		home, _ := os.UserHomeDir()
		candidates = append(candidates,
			"/System/Library/Fonts",
			"/Library/Fonts",
			filepath.Join(home, "Library", "Fonts"),
			"/usr/share/fonts",
			"/usr/local/share/fonts",
			filepath.Join(home, ".local", "share", "fonts"),
			filepath.Join(home, ".fonts"),
		)
	default:
		home, _ := os.UserHomeDir()
		candidates = append(candidates,
			"/usr/share/fonts",
			"/usr/local/share/fonts",
			"/usr/X11R6/lib/X11/fonts",
			filepath.Join(home, ".local", "share", "fonts"),
			filepath.Join(home, ".fonts"),
		)
	}
	return uniqueCleanPaths(candidates)
}

var fontStyleSuffixRE = regexp.MustCompile(`(?i)(?:\s|-|_)+(semibold(?:\s|-|_)?cond(?:ensed)?|semi(?:\s|-|_)?bold(?:\s|-|_)?cond(?:ensed)?|semi(?:\s|-|_)?cond(?:ensed)?|extra(?:\s|-|_)?cond(?:ensed)?|extra(?:\s|-|_)?bold|extra(?:\s|-|_)?light|regular|bold|italic|bolditalic|bold\s+italic|oblique|black|medium|semibold|semi-bold|light|thin|cond|condensed|narrow|roman|book|heavy|ultra|variable|[1-9][0-9]{1,2})+$`)

func fontFamilyFromFilename(name string) string {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".ttf", ".otf", ".ttc", ".otc":
	default:
		return ""
	}
	return fontFamilyFromFontBasename(strings.TrimSuffix(name, filepath.Ext(name)))
}

func fontFamilyFromManagedFilename(name string) string {
	if managedFontExtension(name) == "" {
		return ""
	}
	return fontFamilyFromFontBasename(strings.TrimSuffix(name, filepath.Ext(name)))
}

func fontFamilyFromFontBasename(base string) string {
	base = strings.ReplaceAll(base, "_", " ")
	base = strings.ReplaceAll(base, "-", " ")
	base = fontStyleSuffixRE.ReplaceAllString(base, "")
	base = strings.Join(strings.Fields(base), " ")
	if len(base) < 2 {
		return ""
	}
	return base
}

func managedFontExtension(name string) string {
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(name)), ".")
	switch ext {
	case "ttf", "ttc", "otf", "otc", "woff", "woff2":
		return ext
	default:
		return ""
	}
}

func managedFontAssetURL(rel string) string {
	rel = strings.Trim(filepath.ToSlash(rel), "/")
	if rel == "" || rel == "." || rel == ".." || strings.HasPrefix(rel, "../") {
		return ""
	}
	parts := strings.Split(rel, "/")
	escaped := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || part == "." || part == ".." || strings.HasPrefix(part, ".") {
			return ""
		}
		escaped = append(escaped, url.PathEscape(part))
	}
	return "/api/v1/cgen/fonts/" + strings.Join(escaped, "/")
}
