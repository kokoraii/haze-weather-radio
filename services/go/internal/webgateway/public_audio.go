package webgateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	httpWAVSampleRate       = 48000
	httpWAVChannels         = 1
	httpWAVBitsPerSample    = 16
	httpWAVFrameSamples     = httpWAVSampleRate / 50
	httpWAVMaxQueuedMS      = 240
	httpWAVMaxQueuedSamples = httpWAVSampleRate * httpWAVMaxQueuedMS / 1000
	httpAudioMaxFeedID      = 128
	httpAudioMaxCodecID     = 64
	httpAudioDrainLimit     = 64
	httpAudioProxyBuffer    = httpWAVFrameSamples * httpWAVChannels * (httpWAVBitsPerSample / 8) * 16
	httpOpusBitrate         = "32k"
)

type httpAudioFormat struct {
	ID             string
	ContentType    string
	Extension      string
	FFmpegFormat   string
	FFmpegCodec    string
	Bitrate        string
	Channels       int
	SampleRate     int
	ExtraOutputArg []string
}

func (s *Server) publicFeedAudio(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	access := publicFeedAccess(s.config)
	if access == "disabled" {
		http.Error(writer, "public feeds are disabled", http.StatusNotFound)
		return
	}
	if access == "auth_required" && !s.auth.FullyAuthenticated(request) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	feedID := strings.TrimSpace(request.URL.Query().Get("feed"))
	if feedID == "" {
		http.Error(writer, "feed is required", http.StatusBadRequest)
		return
	}
	if len(feedID) > httpAudioMaxFeedID {
		http.Error(writer, "feed is too long", http.StatusBadRequest)
		return
	}
	if !validPublicAudioFeedID(feedID) {
		http.Error(writer, "feed is invalid", http.StatusBadRequest)
		return
	}
	if !publicHTTPAudioFeedEnabled(s.configPath, feedID) {
		http.Error(writer, "feed audio output is not enabled", http.StatusForbidden)
		return
	}
	codec := strings.TrimSpace(request.URL.Query().Get("codec"))
	if len(codec) > httpAudioMaxCodecID {
		http.Error(writer, "codec is too long", http.StatusBadRequest)
		return
	}
	format, ok := httpAudioFormatByID(codec)
	if !ok {
		http.Error(writer, "unsupported HTTP audio codec", http.StatusBadRequest)
		return
	}
	mediaServiceConfigured := mediaServiceBaseURL(s.config) != ""
	mediaServiceNativeCodec := mediaServiceNativeHTTPCodec(format.ID)
	if request.Method == http.MethodHead {
		if mediaServiceConfigured && mediaServiceNativeCodec && !mediaServiceHTTPAudioAvailable(request.Context(), s.config) {
			http.Error(writer, "haze-media HTTP audio is unavailable", http.StatusServiceUnavailable)
			return
		}
		writeHTTPAudioHeaders(writer, format)
		if mediaServiceConfigured && mediaServiceNativeCodec {
			writer.Header().Set("X-Haze-Media-Backend", "haze-media")
		}
		writer.WriteHeader(http.StatusOK)
		return
	}
	release, ok := s.acquireFeedListener(writer, request, feedID)
	if !ok {
		return
	}
	defer release()
	if mediaServiceNativeCodec && s.proxyMediaServiceAudio(writer, request, feedID, format.ID) {
		return
	}
	if mediaServiceConfigured && mediaServiceNativeCodec {
		http.Error(writer, "haze-media HTTP audio is unavailable", http.StatusServiceUnavailable)
		return
	}
	if s.media == nil || !s.media.Available() {
		http.Error(writer, "media bridge is not available", http.StatusServiceUnavailable)
		return
	}
	ffmpeg := ""
	if format.FFmpegFormat != "" {
		var err error
		ffmpeg, err = resolveFFmpegExecutable()
		if err != nil {
			http.Error(writer, "ffmpeg is required for this HTTP audio codec", http.StatusServiceUnavailable)
			return
		}
	}

	writeHTTPAudioHeaders(writer, format)
	flushHTTPAudioResponse(writer)
	var err error
	if format.FFmpegFormat == "" {
		err = s.media.StreamWAV(request.Context(), feedID, writer)
	} else {
		err = s.media.StreamEncodedAudio(request.Context(), feedID, writer, ffmpeg, format)
	}
	if err != nil && !errors.Is(err, http.ErrHandlerTimeout) && !errors.Is(err, context.Canceled) {
		return
	}
}

func writeHTTPAudioHeaders(writer http.ResponseWriter, format httpAudioFormat) {
	writer.Header().Set("Content-Type", format.ContentType)
	writer.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="haze-feed.%s"`, format.Extension))
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.Header().Set("Connection", "keep-alive")
}

func (s *Server) proxyMediaServiceAudio(writer http.ResponseWriter, request *http.Request, feedID string, codec string) bool {
	control := mediaServiceControl(s.config)
	if control == nil {
		return false
	}
	response, err := control.FeedAudio(request.Context(), feedID, codec)
	if err != nil {
		log.Printf("haze-media audio unavailable: %v", err)
		return false
	}
	defer response.Body.Close()
	for key, values := range response.Header {
		if !proxyAudioResponseHeader(key) {
			continue
		}
		for _, value := range values {
			writer.Header().Add(key, value)
		}
	}
	writer.Header().Set("X-Haze-Media-Backend", "haze-media")
	writer.WriteHeader(response.StatusCode)
	flushHTTPAudioResponse(writer)
	copyErr := copyRealtimeHTTPAudio(writer, response.Body)
	if copyErr != nil && !errors.Is(copyErr, context.Canceled) {
		log.Printf("haze-media HTTP audio proxy ended: %v", copyErr)
	}
	return true
}

func flushHTTPAudioResponse(writer http.ResponseWriter) {
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

func copyRealtimeHTTPAudio(writer http.ResponseWriter, reader io.Reader) error {
	flusher, _ := writer.(http.Flusher)
	buffer := make([]byte, httpAudioProxyBuffer)
	for {
		n, readErr := reader.Read(buffer)
		if n > 0 {
			if _, writeErr := writer.Write(buffer[:n]); writeErr != nil {
				return writeErr
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func proxyAudioResponseHeader(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "content-type", "cache-control", "x-accel-buffering":
		return true
	default:
		return false
	}
}

func mediaServiceNativeHTTPCodec(codec string) bool {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "pcm16", "raw_pcm16", "opus", "aac":
		return true
	default:
		return false
	}
}

func mediaServiceHTTPAudioAvailable(ctx context.Context, config Config) bool {
	health, ok := mediaServiceHealth(ctx, config)
	if !ok {
		return false
	}
	capabilities, _ := health["capabilities"].(map[string]any)
	available, _ := capabilities["http_audio"].(bool)
	return available
}

func mediaServiceBaseURL(config Config) string {
	if !config.Services.Rust.Media.Enabled {
		return ""
	}
	addr := strings.TrimSpace(firstNonBlank(config.Services.Rust.Media.Addr, config.Services.Rust.Media.Listen))
	if addr == "" {
		addr = "127.0.0.1:8097"
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + addr
}

func validPublicAudioFeedID(feedID string) bool {
	if feedID == "" {
		return false
	}
	for _, r := range feedID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '-', '_', '.', ':':
			continue
		default:
			return false
		}
	}
	return true
}

func publicHTTPAudioFeedEnabled(configPath string, feedID string) bool {
	feedID = strings.TrimSpace(feedID)
	if feedID == "" {
		return false
	}
	feeds, err := loadBasicFeedSummaries(configPath)
	if err != nil {
		return false
	}
	for _, feed := range feeds {
		if stringValue(feed, "id") != feedID {
			continue
		}
		enabled, _ := feed["enabled"].(bool)
		httpStream, _ := feed["http_stream_enabled"].(bool)
		return enabled && httpStream
	}
	return false
}

func httpAudioFormatByID(raw string) (httpAudioFormat, bool) {
	id := strings.ToLower(strings.TrimSpace(raw))
	id = strings.ReplaceAll(id, "-", "_")
	if id == "" {
		id = "pcm16"
	}
	switch id {
	case "pcm16", "wav", "wav_pcm16", "pcm_s16le":
		return httpAudioFormat{ID: "pcm16", ContentType: "audio/wav", Extension: "wav"}, true
	case "mp3", "mpeg", "libmp3lame":
		return httpAudioFormat{ID: "mp3", ContentType: "audio/mpeg", Extension: "mp3", FFmpegFormat: "mp3", FFmpegCodec: "libmp3lame", Bitrate: "96k", Channels: 1, SampleRate: 48000}, true
	case "aac", "adts":
		return httpAudioFormat{ID: "aac", ContentType: "audio/aac", Extension: "aac", FFmpegFormat: "adts", FFmpegCodec: "aac", Bitrate: "96k", Channels: 1, SampleRate: 48000}, true
	case "m4a", "mp4_aac", "fmp4_aac":
		return httpAudioFormat{ID: "m4a", ContentType: "audio/mp4", Extension: "m4a", FFmpegFormat: "mp4", FFmpegCodec: "aac", Bitrate: "96k", Channels: 1, SampleRate: 48000, ExtraOutputArg: []string{"-movflags", "frag_keyframe+empty_moov+default_base_moof", "-frag_duration", "100000"}}, true
	case "opus", "ogg_opus", "opus_ogg":
		return httpAudioFormat{ID: "opus", ContentType: "audio/ogg; codecs=opus", Extension: "opus", FFmpegFormat: "ogg", FFmpegCodec: "libopus", Bitrate: httpOpusBitrate, Channels: 1, SampleRate: 48000, ExtraOutputArg: []string{"-page_duration", "20000"}}, true
	case "webm_opus", "opus_webm", "webm":
		return httpAudioFormat{ID: "webm_opus", ContentType: "audio/webm; codecs=opus", Extension: "webm", FFmpegFormat: "webm", FFmpegCodec: "libopus", Bitrate: httpOpusBitrate, Channels: 1, SampleRate: 48000}, true
	case "vorbis", "ogg_vorbis":
		return httpAudioFormat{ID: "vorbis", ContentType: "audio/ogg; codecs=vorbis", Extension: "ogg", FFmpegFormat: "ogg", FFmpegCodec: "libvorbis", Bitrate: "80k", Channels: 1, SampleRate: 48000, ExtraOutputArg: []string{"-page_duration", "20000"}}, true
	case "flac":
		return httpAudioFormat{ID: "flac", ContentType: "audio/flac", Extension: "flac", FFmpegFormat: "flac", FFmpegCodec: "flac", Channels: 1, SampleRate: 48000}, true
	case "ogg_flac", "flac_ogg":
		return httpAudioFormat{ID: "ogg_flac", ContentType: "audio/ogg; codecs=flac", Extension: "oga", FFmpegFormat: "ogg", FFmpegCodec: "flac", Channels: 1, SampleRate: 48000, ExtraOutputArg: []string{"-page_duration", "20000"}}, true
	case "ulaw", "mulaw", "pcmu", "g711u":
		return httpAudioFormat{ID: "ulaw", ContentType: "audio/basic", Extension: "ulaw", FFmpegFormat: "mulaw", FFmpegCodec: "pcm_mulaw", Channels: 1, SampleRate: 8000}, true
	case "alaw", "pcma", "g711a":
		return httpAudioFormat{ID: "alaw", ContentType: "audio/x-alaw-basic", Extension: "alaw", FFmpegFormat: "alaw", FFmpegCodec: "pcm_alaw", Channels: 1, SampleRate: 8000}, true
	case "raw", "raw_pcm16", "s16le":
		return httpAudioFormat{ID: "raw_pcm16", ContentType: "audio/L16; rate=48000; channels=1", Extension: "s16le", FFmpegFormat: "s16le", FFmpegCodec: "pcm_s16le", Channels: 1, SampleRate: 48000}, true
	default:
		return httpAudioFormat{}, false
	}
}

func resolveFFmpegExecutable() (string, error) {
	executable := strings.TrimSpace(os.Getenv("FFMPEG"))
	if executable == "" {
		executable = "ffmpeg"
	}
	if filepath.IsAbs(executable) {
		if _, err := os.Stat(executable); err != nil {
			return "", err
		}
		return executable, nil
	}
	return exec.LookPath(executable)
}

func (h *MediaHub) StreamWAV(ctx context.Context, feedID string, writer http.ResponseWriter) error {
	flusher, _ := writer.(http.Flusher)
	if err := writeWAVStreamHeader(writer, httpWAVSampleRate, httpWAVChannels, httpWAVBitsPerSample); err != nil {
		return err
	}
	if flusher != nil {
		flusher.Flush()
	}

	updates, unsubscribe := h.Subscribe(feedID)
	defer unsubscribe()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	queue := make([]int16, 0, httpWAVFrameSamples*20)
	frame := make([]int16, httpWAVFrameSamples)
	frameBytes := make([]byte, httpWAVFrameSamples*2)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var ok bool
			queue, ok = drainHTTPAudioUpdates(updates, queue)
			if !ok {
				return nil
			}
			popWAVFrameInto(&queue, frame)
			pcm16BytesInto(frameBytes, frame)
			if _, err := writer.Write(frameBytes); err != nil {
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

func (h *MediaHub) StreamEncodedAudio(ctx context.Context, feedID string, writer http.ResponseWriter, ffmpeg string, format httpAudioFormat) error {
	if format.Channels <= 0 {
		format.Channels = 1
	}
	if format.SampleRate <= 0 {
		format.SampleRate = httpWAVSampleRate
	}
	args := ffmpegHTTPAudioArgs(format)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(runCtx, ffmpeg, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return err
	}
	wait := func() error {
		_ = stdin.Close()
		return cmd.Wait()
	}

	flusher, _ := writer.(http.Flusher)
	copyDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 32*1024)
		for {
			n, readErr := stdout.Read(buffer)
			if n > 0 {
				if _, writeErr := writer.Write(buffer[:n]); writeErr != nil {
					copyDone <- writeErr
					cancel()
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					readErr = nil
				}
				copyDone <- readErr
				cancel()
				return
			}
		}
	}()

	updates, unsubscribe := h.Subscribe(feedID)
	defer unsubscribe()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	queue := make([]int16, 0, httpWAVFrameSamples*20)
	frame := make([]int16, httpWAVFrameSamples)
	frameBytes := make([]byte, httpWAVFrameSamples*2)
	for {
		select {
		case <-ctx.Done():
			cancel()
			_ = wait()
			return ctx.Err()
		case err := <-copyDone:
			cancel()
			waitErr := wait()
			if err != nil {
				return err
			}
			return waitErr
		case <-ticker.C:
			var ok bool
			queue, ok = drainHTTPAudioUpdates(updates, queue)
			if !ok {
				cancel()
				return wait()
			}
			popWAVFrameInto(&queue, frame)
			pcm16BytesInto(frameBytes, frame)
			if _, err := stdin.Write(frameBytes); err != nil {
				cancel()
				_ = wait()
				return err
			}
		}
	}
}

func ffmpegHTTPAudioArgs(format httpAudioFormat) []string {
	args := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-nostdin",
		"-fflags", "nobuffer",
		"-probesize", "32",
		"-analyzeduration", "0",
		"-f", "s16le",
		"-ar", fmt.Sprintf("%d", httpWAVSampleRate),
		"-ac", "1",
		"-i", "pipe:0",
		"-vn",
		"-sn",
		"-dn",
		"-ar", fmt.Sprintf("%d", format.SampleRate),
		"-ac", fmt.Sprintf("%d", format.Channels),
		"-c:a", format.FFmpegCodec,
	}
	if format.Bitrate != "" {
		args = append(args, "-b:a", format.Bitrate)
	}
	args = append(args, format.ExtraOutputArg...)
	args = append(args, "-flush_packets", "1", "-f", format.FFmpegFormat, "pipe:1")
	return args
}

func drainHTTPAudioUpdates(updates <-chan PCMChunk, queue []int16) ([]int16, bool) {
	limit := cap(updates)
	if limit <= 0 || limit > httpAudioDrainLimit {
		limit = httpAudioDrainLimit
	}
	for drained := 0; drained < limit; drained++ {
		select {
		case chunk, ok := <-updates:
			if !ok {
				return queue, false
			}
			queue = appendWAVSamples(queue, chunk)
		default:
			return queue, true
		}
	}
	return queue, true
}

func writeWAVStreamHeader(writer http.ResponseWriter, sampleRate int, channels int, bitsPerSample int) error {
	byteRate := sampleRate * channels * bitsPerSample / 8
	blockAlign := channels * bitsPerSample / 8
	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], 0xffffffff)
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16)
	binary.LittleEndian.PutUint16(header[20:22], 1)
	binary.LittleEndian.PutUint16(header[22:24], uint16(channels))
	binary.LittleEndian.PutUint32(header[24:28], uint32(sampleRate))
	binary.LittleEndian.PutUint32(header[28:32], uint32(byteRate))
	binary.LittleEndian.PutUint16(header[32:34], uint16(blockAlign))
	binary.LittleEndian.PutUint16(header[34:36], uint16(bitsPerSample))
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], 0xffffffff)
	_, err := writer.Write(header)
	return err
}

func appendWAVSamples(queue []int16, chunk PCMChunk) []int16 {
	samples := resamplePCM16ToMono(chunk, httpWAVSampleRate)
	if len(samples) == 0 {
		return queue
	}
	queue = append(queue, samples...)
	if len(queue) > httpWAVMaxQueuedSamples {
		queue = queue[len(queue)-httpWAVMaxQueuedSamples:]
	}
	return queue
}

func popWAVFrameInto(queue *[]int16, frame []int16) {
	clear(frame)
	if len(*queue) == 0 {
		return
	}
	copied := copy(frame, (*queue)[:min(len(frame), len(*queue))])
	*queue = (*queue)[copied:]
}

func pcm16BytesInto(out []byte, samples []int16) {
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(out[i*2:i*2+2], uint16(sample))
	}
}
