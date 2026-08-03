package playlist

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/alertmodel"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
)

type sameGenerateRequest struct {
	Originator string
	Event      string
	Locations  []string
	Duration   string
	Callsign   string
	Tone       string
	Sequence   string
}

type sameGeneratorFunc func(context.Context, string, sameGenerateRequest) (map[string]any, error)

func includeSameAlert(data map[string]any) bool {
	value, ok := data["include_same"]
	if !ok {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return xmlBool(typed, false)
	default:
		return false
	}
}

func (p *feedPlanner) queuePrioritySAME(ctx context.Context, alertID string, data map[string]any) error {
	request, result, err := p.generatePrioritySAME(ctx, data, "full")
	if err != nil {
		return err
	}
	return p.persistPrioritySAME(alertID, request, result, "000", "full")
}

type sameAudioPayload struct {
	Header     string
	Audio      []byte
	SampleRate int
	Channels   int
}

func sameAudioFromResult(result map[string]any, fallbackSampleRate int, fallbackChannels int) (sameAudioPayload, error) {
	audioBase64, _ := result["audio_base64"].(string)
	if audioBase64 == "" {
		return sameAudioPayload{}, fmt.Errorf("SAME generator returned no audio payload")
	}
	audio, err := base64.StdEncoding.DecodeString(audioBase64)
	if err != nil {
		return sameAudioPayload{}, fmt.Errorf("decode SAME audio payload: %w", err)
	}
	header, _ := result["header"].(string)
	if header == "" {
		return sameAudioPayload{}, fmt.Errorf("SAME generator returned no header")
	}
	return sameAudioPayload{
		Header:     header,
		Audio:      audio,
		SampleRate: intFromResult(result, "sample_rate", fallbackSampleRate),
		Channels:   intFromResult(result, "channels", fallbackChannels),
	}, nil
}

func silencePCM(sampleRate int, channels int, duration time.Duration) []byte {
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

func (p *feedPlanner) generatePrioritySAME(ctx context.Context, data map[string]any, sequence string) (sameGenerateRequest, map[string]any, error) {
	request := p.buildSAMERequest(data, sequence)
	if len(request.Locations) == 0 {
		return request, nil, fmt.Errorf("no SAME locations were supplied")
	}
	result, err := p.runSameGenerator(ctx, request)
	if err != nil {
		return request, nil, err
	}
	return request, result, nil
}

func (p *feedPlanner) runSameGenerator(ctx context.Context, request sameGenerateRequest) (map[string]any, error) {
	if p.sameGenerator != nil {
		return p.sameGenerator(ctx, p.cfg.BaseDir, request)
	}
	return runSameGenerator(ctx, p.cfg.BaseDir, request)
}

func (p *feedPlanner) generatePrioritySAMEPair(ctx context.Context, data map[string]any) (sameGenerateRequest, sameAudioPayload, sameAudioPayload, error) {
	headerRequest := p.buildSAMERequest(data, "header")
	if len(headerRequest.Locations) == 0 {
		return headerRequest, sameAudioPayload{}, sameAudioPayload{}, fmt.Errorf("SAME header generation failed: no SAME locations were supplied")
	}
	eomRequest := headerRequest
	eomRequest.Locations = append([]string(nil), headerRequest.Locations...)
	eomRequest.Sequence = "eom"
	// Both parts use the same immutable request data and are assembled in a fixed order later.
	type partResult struct {
		sequence string
		result   map[string]any
		err      error
	}
	results := make(chan partResult, 2)
	generate := func(request sameGenerateRequest) {
		result, err := p.runSameGenerator(ctx, request)
		results <- partResult{sequence: request.Sequence, result: result, err: err}
	}
	go generate(headerRequest)
	go generate(eomRequest)

	parts := map[string]partResult{}
	for range 2 {
		part := <-results
		parts[part.sequence] = part
	}
	headerPart := parts["header"]
	if headerPart.err != nil {
		return headerRequest, sameAudioPayload{}, sameAudioPayload{}, fmt.Errorf("SAME header generation failed: %w", headerPart.err)
	}
	header, err := sameAudioFromResult(headerPart.result, p.cfg.Root.Playout.SampleRate, p.cfg.Root.Playout.Channels)
	if err != nil {
		return headerRequest, sameAudioPayload{}, sameAudioPayload{}, fmt.Errorf("SAME header generation failed: %w", err)
	}
	eomPart := parts["eom"]
	if eomPart.err != nil {
		return headerRequest, sameAudioPayload{}, sameAudioPayload{}, fmt.Errorf("SAME EOM generation failed: %w", eomPart.err)
	}
	eom, err := sameAudioFromResult(eomPart.result, header.SampleRate, header.Channels)
	if err != nil {
		return headerRequest, sameAudioPayload{}, sameAudioPayload{}, fmt.Errorf("SAME EOM generation failed: %w", err)
	}
	return headerRequest, header, eom, nil
}

func (p *feedPlanner) buildSAMERequest(data map[string]any, sequence string) sameGenerateRequest {
	request := sameGenerateRequest{
		Originator: strings.ToUpper(fallbackText(firstText(nil, data, "same_originator", "originator"), "WXR")),
		Event:      strings.ToUpper(fallbackText(firstText(nil, data, "same_event", "event"), "ADR")),
		Locations:  p.sameLocationsForData(data),
		Duration:   normalizeSAMEDuration(fallbackText(firstText(nil, data, "same_duration", "duration"), "0015")),
		Callsign:   fallbackText(firstText(nil, data, "same_callsign", "callsign"), feedCallsign(p.feed)),
		Tone:       strings.ToUpper(fallbackText(firstText(nil, data, "same_tone", "tone_type"), "WXR")),
		Sequence:   strings.ToLower(fallbackText(sequence, "full")),
	}
	if request.Callsign == "" {
		request.Callsign = "HAZE"
	}
	return request
}

func (p *feedPlanner) sameLocationsForData(data map[string]any) []string {
	locations := sameLocationsFromData(data)
	if len(locations) == 0 {
		for _, region := range p.feed.Locations.Coverage.Regions {
			if code := sameLocationCode(region.ID); code != "" {
				locations = append(locations, code)
			}
			for _, subregion := range region.Subregions {
				if code := sameLocationCode(subregion.ID); code != "" {
					locations = append(locations, code)
				}
			}
		}
	}
	return expandWildcardSAMELocations(p.cfg.BaseDir, locations)
}

func sameLocationsFromData(data map[string]any) []string {
	locations := stringListAny(firstValue(nil, data, "same_locations", "locations"))
	out := make([]string, 0, len(locations))
	seen := map[string]struct{}{}
	add := func(raw string) {
		if len(out) >= 31 {
			return
		}
		code := sameLocationCode(raw)
		if code == "" {
			return
		}
		if _, ok := seen[code]; ok {
			return
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	if packet, ok := alertmodel.FromMap(data); ok {
		for _, location := range packet.Areas.Locations {
			if !location.Actionable || location.Ambiguous || strings.TrimSpace(location.CanonicalID) == "" {
				continue
			}
			for _, identifier := range location.CanonicalIdentifiers {
				scheme := strings.ToLower(strings.TrimSpace(identifier.Scheme))
				if scheme == "same" || scheme == "fips" || scheme == "clc" {
					add(identifier.Value)
				}
			}
		}
	}
	for _, raw := range locations {
		add(raw)
		if len(out) >= 31 {
			break
		}
	}
	return out
}

func expandWildcardSAMELocations(baseDir string, locations []string) []string {
	out := make([]string, 0, len(locations))
	seen := map[string]struct{}{}
	add := func(raw string) {
		code := sameLocationCode(raw)
		if code == "" {
			return
		}
		if _, ok := seen[code]; ok {
			return
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	wildcards := []string{}
	for _, location := range locations {
		add(location)
		code := sameLocationCode(location)
		if strings.HasSuffix(code, "00") && len(code) == 6 {
			wildcards = append(wildcards, code)
		}
	}
	if len(wildcards) > 0 {
		for _, child := range clcWildcardChildrenForBase(baseDir, wildcards) {
			add(child)
		}
	}
	if len(out) > 31 {
		out = out[:31]
	}
	return out
}

func clcWildcardChildrenForBase(baseDir string, wildcards []string) []string {
	if snap, ok := locationdb.Load(baseDir); ok {
		children := []string{}
		for _, place := range snap.PlacesBySource("clc") {
			code := sameLocationCode(place.Code)
			if code == "" || strings.HasSuffix(code, "00") {
				continue
			}
			for _, wildcard := range wildcards {
				if strings.HasPrefix(code, wildcard[:4]) {
					children = append(children, code)
					break
				}
			}
		}
		if len(children) > 0 {
			return children
		}
	}
	return clcWildcardChildren(filepath.Join(baseDir, "managed", "csv", "CLC_Base_Zone.csv"), wildcards)
}

func clcWildcardChildren(path string, wildcards []string) []string {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return nil
	}
	clcIndex := -1
	for i, column := range header {
		if strings.EqualFold(strings.TrimSpace(column), "CLC") {
			clcIndex = i
			break
		}
	}
	if clcIndex < 0 {
		return nil
	}
	children := []string{}
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		if clcIndex >= len(row) {
			continue
		}
		code := sameLocationCode(row[clcIndex])
		if code == "" || strings.HasSuffix(code, "00") {
			continue
		}
		for _, wildcard := range wildcards {
			if strings.HasPrefix(code, wildcard[:4]) {
				children = append(children, code)
				break
			}
		}
	}
	return children
}

func sameLocationCode(raw string) string {
	digits := strings.Builder{}
	for _, ch := range raw {
		if ch >= '0' && ch <= '9' {
			digits.WriteRune(ch)
		}
	}
	value := digits.String()
	if len(value) != 6 {
		return ""
	}
	return value
}

func runSameGenerator(ctx context.Context, baseDir string, request sameGenerateRequest) (map[string]any, error) {
	executable, err := findHostExecutable(baseDir)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	args := []string{
		"same", "generate", "--json",
		"--originator", request.Originator,
		"--event", request.Event,
		"--locations", strings.Join(request.Locations, ","),
		"--duration", request.Duration,
		"--callsign", request.Callsign,
		"--tone", request.Tone,
	}
	if request.Sequence != "" {
		args = append(args, "--sequence", request.Sequence)
	}
	cmd := exec.CommandContext(runCtx, executable, args...)
	cmd.Dir = baseDir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	raw, err := cmd.Output()
	if runCtx.Err() == context.DeadlineExceeded {
		return nil, fmt.Errorf("SAME generator timed out")
	}
	if err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return nil, fmt.Errorf("SAME generator failed: %s", detail)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("SAME generator returned invalid JSON: %w", err)
	}
	return result, nil
}

func findHostExecutable(baseDir string) (string, error) {
	if configured := strings.TrimSpace(os.Getenv("HAZE_SAME_GENERATOR")); configured != "" {
		return configured, nil
	}
	if configured := strings.TrimSpace(os.Getenv("HAZE_HOST_EXE")); configured != "" {
		return configured, nil
	}
	name := "haze"
	if runtime.GOOS == "windows" {
		name = "haze.exe"
	}
	candidates := []string{
		filepath.Join(baseDir, name),
		filepath.Join(baseDir, "dist", "haze", name),
		filepath.Join(baseDir, "..", name),
	}
	for _, candidate := range candidates {
		clean := filepath.Clean(candidate)
		if info, err := os.Stat(clean); err == nil && !info.IsDir() {
			return clean, nil
		}
	}
	return "", fmt.Errorf("Haze SAME generator executable was not found")
}

func (p *feedPlanner) persistPrioritySAME(alertID string, request sameGenerateRequest, result map[string]any, order string, label string) error {
	payload, err := sameAudioFromResult(result, p.cfg.Root.Playout.SampleRate, p.cfg.Root.Playout.Channels)
	if err != nil {
		return err
	}
	label = strings.ToLower(fallbackText(label, fallbackText(request.Sequence, "same")))
	queueID := safeID(order + "_" + p.feed.ID + "_" + alertID + "_same_" + label)
	audioRel := filepath.ToSlash(filepath.Join("runtime", "audio", "alerts", queueID+".pcm16le"))
	audioPath := filepath.Join(p.cfg.BaseDir, filepath.FromSlash(audioRel))
	if err := os.MkdirAll(filepath.Dir(audioPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(audioPath+".tmp", payload.Audio, 0o600); err != nil {
		return err
	}
	if err := os.Rename(audioPath+".tmp", audioPath); err != nil {
		return err
	}
	manifest := priorityAlertManifest{
		ID:         queueID,
		AlertID:    alertID,
		Type:       "same_" + label,
		Status:     "pending",
		CreatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		FeedIDs:    []string{p.feed.ID},
		Header:     payload.Header,
		Event:      request.Event,
		AudioPath:  audioRel,
		Format:     "pcm_s16le",
		SampleRate: payload.SampleRate,
		Channels:   payload.Channels,
		AudioBytes: len(payload.Audio),
		Source:     "cap-same",
		Priority:   "same_" + label,
	}
	return writePriorityAlertManifest(filepath.Join(p.cfg.BaseDir, "runtime", "queues", "alerts", queueID+".json"), manifest)
}

func normalizeSAMEDuration(raw string) string {
	digits := strings.Builder{}
	for _, ch := range raw {
		if ch >= '0' && ch <= '9' {
			digits.WriteRune(ch)
		}
	}
	value := digits.String()
	if len(value) >= 4 {
		return value[:4]
	}
	return strings.Repeat("0", 4-len(value)) + value
}

func intFromResult(source map[string]any, key string, fallback int) int {
	switch value := source[key].(type) {
	case float64:
		return int(value)
	case int:
		return value
	case json.Number:
		parsed, err := value.Int64()
		if err == nil {
			return int(parsed)
		}
	}
	return fallback
}
