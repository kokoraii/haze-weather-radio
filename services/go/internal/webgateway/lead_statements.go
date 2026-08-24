package webgateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/events"
	"github.com/meowraii/haze-weather-radio/services/go/internal/lead"
)

const leadStatementAudioMaxBytes int64 = 16 << 20
const leadPreviewPCMMaxBytes = 12 << 20
const leadPreviewDurationLimit = 90 * time.Second

func loadLeadStatements(configPath string) (map[string]any, error) {
	document, err := lead.Load(resolveConfigPath(configPath, "managed/configs/lead.xml"))
	if err != nil {
		return nil, err
	}
	files, err := listLeadStatementAudioFiles(configPath)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"path":        "managed/configs/lead.xml",
		"statements":  document.Statements,
		"audio_files": files,
	}, nil
}

func (s *wsSession) saveLeadStatements(payload map[string]any) (map[string]any, error) {
	document, err := leadDocumentFromPayload(payload)
	if err != nil {
		return nil, err
	}
	if err := validateLeadStatementAudioFiles(s.configPath, document); err != nil {
		return nil, err
	}
	raw, err := lead.Encode(document)
	if err != nil {
		return nil, err
	}
	path := resolveConfigPath(s.configPath, "managed/configs/lead.xml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := writeFileAtomic(path, raw, 0o644); err != nil {
		return nil, err
	}
	result := map[string]any{
		"saved":      true,
		"path":       "managed/configs/lead.xml",
		"statements": document.Statements,
	}
	if err := s.publishLeadStatementsUpdated(document); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *wsSession) publishLeadStatementsUpdated(document lead.Document) error {
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return nil
	}
	publisher := events.NewHostBridgePublisher(bridgeAddr)
	defer publisher.Close()
	return publisher.Publish(events.Event{
		Type:   "lead.config.updated",
		Source: "haze-web",
		Data: map[string]any{
			"statement_count": len(document.Statements),
		},
	})
}

func leadDocumentFromPayload(payload map[string]any) (lead.Document, error) {
	value, ok := payload["statements"]
	if !ok {
		return lead.Document{}, fmt.Errorf("lead statements are required")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return lead.Document{}, fmt.Errorf("lead statements are invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var statements []lead.Statement
	if err := decoder.Decode(&statements); err != nil {
		return lead.Document{}, fmt.Errorf("lead statements are invalid: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return lead.Document{}, fmt.Errorf("lead statements are invalid")
	}
	return lead.Normalize(lead.Document{Statements: statements})
}

func leadStatementFromPayload(payload map[string]any) (lead.Statement, error) {
	value, ok := payload["statement"]
	if !ok {
		return lead.Statement{}, fmt.Errorf("lead statement is required")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return lead.Statement{}, fmt.Errorf("lead statement is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var statement lead.Statement
	if err := decoder.Decode(&statement); err != nil {
		return lead.Statement{}, fmt.Errorf("lead statement is invalid: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return lead.Statement{}, fmt.Errorf("lead statement is invalid")
	}
	document, err := lead.Normalize(lead.Document{Statements: []lead.Statement{statement}})
	if err != nil {
		return lead.Statement{}, err
	}
	return document.Statements[0], nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return err
}

func validateLeadStatementAudioFiles(configPath string, document lead.Document) error {
	baseDir := filepath.Dir(filepath.Clean(configPath))
	for _, statement := range document.Statements {
		for _, item := range []struct {
			label string
			path  string
		}{
			{"lead_in", statement.LeadIn},
			{"lead_out", statement.LeadOut},
		} {
			if item.path == "" {
				continue
			}
			resolved, err := lead.ResolveAudioPath(baseDir, item.path)
			if err != nil {
				return fmt.Errorf("lead %q %s: %w", statement.Name, item.label, err)
			}
			info, err := os.Stat(resolved)
			if err != nil || !info.Mode().IsRegular() {
				return fmt.Errorf("lead %q %s audio file is not available", statement.Name, item.label)
			}
			if info.Size() <= 0 || info.Size() > leadStatementAudioMaxBytes {
				return fmt.Errorf("lead %q %s audio must be between 1 byte and %d MiB", statement.Name, item.label, leadStatementAudioMaxBytes>>20)
			}
		}
	}
	return nil
}

func listLeadStatementAudioFiles(configPath string) ([]string, error) {
	baseDir := filepath.Dir(filepath.Clean(configPath))
	root := filepath.Join(baseDir, "audio")
	entries := []string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) && path == root {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() || !entry.Type().IsRegular() {
			return nil
		}
		relative, err := filepath.Rel(baseDir, path)
		if err != nil {
			return nil
		}
		entries = append(entries, filepath.ToSlash(relative))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lead.SortedAudioPaths(entries), nil
}

func (s *wsSession) previewLeadStatement(payload map[string]any) (map[string]any, error) {
	statement, err := leadStatementFromPayload(payload)
	if err != nil {
		return nil, err
	}
	if err := validateLeadStatementAudioFiles(s.configPath, lead.Document{Statements: []lead.Statement{statement}}); err != nil {
		return nil, err
	}
	includeSame := boolPayload(payload, "include_same", false)
	ctx, cancel := context.WithTimeout(context.Background(), leadPreviewDurationLimit)
	defer cancel()
	leadIn, err := leadStatementAudioPCM(ctx, s.configPath, statement.LeadIn)
	if err != nil {
		return nil, fmt.Errorf("lead-in preview failed: %w", err)
	}
	leadOut, err := leadStatementAudioPCM(ctx, s.configPath, statement.LeadOut)
	if err != nil {
		return nil, fmt.Errorf("lead-out preview failed: %w", err)
	}
	var header []byte
	var eom []byte
	var sameHeader string
	if includeSame {
		header, eom, sameHeader, err = leadPreviewSAME(s.configPath)
		if err != nil {
			return nil, err
		}
	}
	pcm := assembleLeadStatementPreviewPCM(leadIn, header, eom, leadOut)
	if len(pcm) == 0 {
		return nil, fmt.Errorf("lead statement has no previewable audio")
	}
	if len(pcm) > leadPreviewPCMMaxBytes {
		return nil, fmt.Errorf("lead preview is too large")
	}
	return map[string]any{
		"audio_base64": base64.StdEncoding.EncodeToString(wavFromPCM16(pcm, 48000, 1)),
		"format":       "wav",
		"content_type": "audio/wav",
		"sample_rate":  48000,
		"channels":     1,
		"include_same": includeSame,
		"same_header":  sameHeader,
		"statement":    statement.Name,
	}, nil
}

func leadPreviewSAME(configPath string) ([]byte, []byte, string, error) {
	request := sameGenerateRequest{
		Originator: "WXR",
		Event:      "RWT",
		Locations:  []string{"000000"},
		Duration:   "0015",
		Callsign:   "HAZE",
		Tone:       "WXR",
	}
	headerRequest := request
	headerRequest.Sequence = "header"
	headerResult, err := runSameGenerator(configPath, headerRequest)
	if err != nil {
		return nil, nil, "", fmt.Errorf("SAME test header generation failed: %w", err)
	}
	header, headerText, err := alertPreviewAudioFromResult(headerResult)
	if err != nil {
		return nil, nil, "", err
	}
	eomRequest := request
	eomRequest.Sequence = "eom"
	eomResult, err := runSameGenerator(configPath, eomRequest)
	if err != nil {
		return nil, nil, "", fmt.Errorf("SAME test EOM generation failed: %w", err)
	}
	eom, _, err := alertPreviewAudioFromResult(eomResult)
	if err != nil {
		return nil, nil, "", err
	}
	return header, eom, headerText, nil
}

func leadStatementAudioPCM(ctx context.Context, configPath string, relativePath string) ([]byte, error) {
	if strings.TrimSpace(relativePath) == "" {
		return nil, nil
	}
	baseDir := filepath.Dir(filepath.Clean(configPath))
	path, err := lead.ResolveAudioPath(baseDir, relativePath)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("audio file is not available")
	}
	if info.Size() <= 0 || info.Size() > leadStatementAudioMaxBytes {
		return nil, fmt.Errorf("audio file is outside the allowed size")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if pcm, wavInfo, wavErr := wavPCM16Info(raw); wavErr == nil {
		if wavInfo.SampleRate == 48000 && wavInfo.Channels == 1 {
			return pcm, nil
		}
		return transcodeRawPCM16ToPCM(ctx, pcm, wavInfo.SampleRate, wavInfo.Channels, 48000, 1)
	}
	extension := strings.ToLower(filepath.Ext(path))
	if extension == ".pcm16le" || extension == ".s16le" {
		if len(raw)%2 != 0 {
			return nil, fmt.Errorf("raw PCM audio has an incomplete sample")
		}
		return raw, nil
	}
	return transcodeLeadStatementAudio(ctx, path)
}

func transcodeLeadStatementAudio(ctx context.Context, inputPath string) ([]byte, error) {
	ffmpeg, err := resolveFFmpegExecutable()
	if err != nil {
		return nil, fmt.Errorf("audio conversion backend is unavailable")
	}
	args := []string{
		"-hide_banner", "-loglevel", "error", "-nostdin", "-i", inputPath,
		"-t", fmt.Sprintf("%.3f", leadPreviewDurationLimit.Seconds()),
		"-vn", "-sn", "-dn", "-ac", "1", "-ar", "48000", "-f", "s16le", "-acodec", "pcm_s16le", "pipe:1",
	}
	command := exec.CommandContext(ctx, ffmpeg, args...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	pcm, err := command.Output()
	if err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = "conversion failed"
		}
		return nil, fmt.Errorf("audio conversion failed: %s", detail)
	}
	if len(pcm) == 0 || len(pcm) > leadPreviewPCMMaxBytes {
		return nil, fmt.Errorf("converted audio is outside the allowed size")
	}
	return pcm, nil
}

func assembleLeadStatementPreviewPCM(leadIn []byte, sameHeader []byte, sameEOM []byte, leadOut []byte) []byte {
	gap := silencePCMBytes(48000, 1, time.Second)
	result := make([]byte, 0, len(leadIn)+len(sameHeader)+len(sameEOM)+len(leadOut)+3*len(gap))
	if len(leadIn) > 0 {
		result = append(result, leadIn...)
		result = append(result, gap...)
	}
	if len(sameHeader) > 0 {
		result = append(result, sameHeader...)
		result = append(result, gap...)
	}
	if len(sameEOM) > 0 {
		result = append(result, sameEOM...)
		if len(leadOut) > 0 {
			result = append(result, gap...)
		}
	} else if len(leadOut) > 0 && len(result) > 0 {
		result = append(result, gap...)
	}
	result = append(result, leadOut...)
	return result
}
