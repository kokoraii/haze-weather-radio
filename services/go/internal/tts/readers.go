package tts

import (
	"encoding/xml"
	"fmt"
	"os"
	"strings"
)

// Reader maps a Haze reader_id to a concrete TTS provider voice.
type Reader struct {
	ID        string        `json:"id"`
	Provider  string        `json:"provider"`
	Gender    string        `json:"gender,omitempty"`
	Language  string        `json:"language,omitempty"`
	VoiceID   string        `json:"voice_id"`
	Backup    *ReaderBackup `json:"backup,omitempty"`
	BackupFor []string      `json:"backup_for,omitempty"`
}

// ReaderBackup selects a local or alternate provider when a reader's primary
// provider cannot synthesize the request.
type ReaderBackup struct {
	ReaderID string `json:"reader_id,omitempty"`
	Provider string `json:"provider"`
	VoiceID  string `json:"voice_id,omitempty"`
}

type readersXML struct {
	Readers []readerXML `xml:"reader"`
}

type readerXML struct {
	ID       string `xml:"id,attr"`
	Provider string `xml:"provider,attr"`
	Gender   string `xml:"gender"`
	Language string `xml:"language"`
	VoiceID  string `xml:"voice_id"`
	Path     string `xml:"path"`
	Backup   *struct {
		Provider string   `xml:"provider,attr"`
		VoiceID  string   `xml:"voice_id"`
		Path     string   `xml:"path"`
		Readers  []string `xml:"reader"`
	} `xml:"backup"`
}

// LoadReaders parses managed/configs/readers.xml.
func LoadReaders(path string) ([]Reader, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	raw = []byte(os.ExpandEnv(string(raw)))
	var parsed readersXML
	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}

	readers := make([]Reader, 0, len(parsed.Readers))
	readerIndexes := make(map[string]int, len(parsed.Readers))
	for _, item := range parsed.Readers {
		readerID := strings.TrimSpace(item.ID)
		if readerID == "" {
			continue
		}
		if _, exists := readerIndexes[readerID]; exists {
			return nil, fmt.Errorf("duplicate reader id %q", readerID)
		}
		provider := NormalizeProvider(item.Provider)
		if provider == "" {
			provider = "auto"
		}
		voiceID := strings.TrimSpace(item.VoiceID)
		if voiceID == "" {
			voiceID = strings.TrimSpace(item.Path)
		}
		var backup *ReaderBackup
		var backupFor []string
		if item.Backup != nil {
			backupFor = normalizeBackupReaderIDs(item.Backup.Readers)
			if len(backupFor) > 0 {
				if strings.TrimSpace(item.Backup.Provider) != "" || strings.TrimSpace(item.Backup.VoiceID) != "" || strings.TrimSpace(item.Backup.Path) != "" {
					return nil, fmt.Errorf("reader %q backup cannot mix reader targets with an inline provider or voice", readerID)
				}
			} else {
				backupProvider := NormalizeProvider(item.Backup.Provider)
				backupVoiceID := strings.TrimSpace(item.Backup.VoiceID)
				if backupVoiceID == "" {
					backupVoiceID = strings.TrimSpace(item.Backup.Path)
				}
				if backupProvider != "" && backupProvider != "auto" {
					backup = &ReaderBackup{Provider: backupProvider, VoiceID: backupVoiceID}
				}
			}
		}
		gender := strings.ToLower(strings.TrimSpace(item.Gender))
		if gender != "female" {
			gender = "male"
		}
		readerIndexes[readerID] = len(readers)
		readers = append(readers, Reader{
			ID:        readerID,
			Provider:  provider,
			Gender:    gender,
			Language:  NormalizeLanguage(item.Language),
			VoiceID:   voiceID,
			Backup:    backup,
			BackupFor: backupFor,
		})
	}
	for _, backupReader := range readers {
		for _, targetID := range backupReader.BackupFor {
			if targetID == backupReader.ID {
				return nil, fmt.Errorf("reader %q cannot back up itself", backupReader.ID)
			}
			targetIndex, ok := readerIndexes[targetID]
			if !ok {
				return nil, fmt.Errorf("reader %q backs up unknown reader %q", backupReader.ID, targetID)
			}
			candidate := &ReaderBackup{
				ReaderID: backupReader.ID,
				Provider: backupReader.Provider,
				VoiceID:  backupReader.VoiceID,
			}
			if existing := readers[targetIndex].Backup; existing != nil {
				if sameReaderBackup(existing, candidate) {
					continue
				}
				return nil, fmt.Errorf("reader %q has more than one backup", targetID)
			}
			readers[targetIndex].Backup = candidate
		}
	}
	return readers, nil
}

func normalizeBackupReaderIDs(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func sameReaderBackup(left *ReaderBackup, right *ReaderBackup) bool {
	return left != nil && right != nil &&
		left.ReaderID == right.ReaderID &&
		left.Provider == right.Provider &&
		left.VoiceID == right.VoiceID
}

// NormalizeProvider maps provider names onto the service provider IDs.
func NormalizeProvider(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "", "auto", "default":
		return "auto"
	case "fast", "ivr-fast", "ivr_fast", "prompt", "low-latency", "low_latency":
		return "fast"
	case "sapi", "sapi5":
		return "sapi5"
	case "espeak", "espeak-ng", "espeakng":
		return "espeak"
	case "piper", "piper-tts", "pipertts":
		return "piper"
	case "f5", "f5tts", "f5-tts":
		return "f5tts"
	case "chatterbox", "chatterbox-tts", "chatterboxtts":
		return "chatterbox"
	case "kokoro", "kokoro-tts", "kokorotts", "sherpa", "sherpa-onnx":
		return "kokoro"
	case "speaky", "speaky-api", "speakyapi":
		return "speakyapi"
	default:
		return strings.ToLower(strings.TrimSpace(provider))
	}
}

// NormalizeLanguage canonicalizes language tags enough for reader matching.
func NormalizeLanguage(language string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(language), "_", "-"))
}

// SelectReader resolves a requested reader ID, falling back by language/gender.
func SelectReader(readers []Reader, readerID string, language string, gender string) (Reader, bool) {
	requested := strings.TrimSpace(readerID)
	if requested != "" && strings.ToLower(requested) != "male" && strings.ToLower(requested) != "female" {
		for _, reader := range readers {
			if reader.ID == requested {
				return reader, true
			}
		}
	}

	lang := NormalizeLanguage(language)
	prefix := lang
	if idx := strings.Index(prefix, "-"); idx >= 0 {
		prefix = prefix[:idx]
	}
	slot := strings.ToLower(strings.TrimSpace(gender))
	if slot == "" || requested == "male" || requested == "female" {
		slot = strings.ToLower(strings.TrimSpace(requested))
	}
	if slot != "female" {
		slot = "male"
	}

	groups := [][]Reader{
		filterReaders(readers, func(reader Reader) bool { return reader.Language == lang }),
		filterReaders(readers, func(reader Reader) bool {
			return reader.Language != "" && strings.SplitN(reader.Language, "-", 2)[0] == prefix
		}),
		filterReaders(readers, func(reader Reader) bool { return reader.Language == "" }),
		readers,
	}
	for _, group := range groups {
		for _, reader := range group {
			if reader.Gender == slot {
				return reader, true
			}
		}
	}
	for _, group := range groups {
		if len(group) > 0 {
			return group[0], true
		}
	}
	return Reader{}, false
}

func filterReaders(readers []Reader, keep func(Reader) bool) []Reader {
	filtered := make([]Reader, 0, len(readers))
	for _, reader := range readers {
		if keep(reader) {
			filtered = append(filtered, reader)
		}
	}
	return filtered
}

// ProviderForReader resolves the provider for a reader.
func ProviderForReader(providers map[string]Provider, reader Reader) (Provider, error) {
	provider := providers[NormalizeProvider(reader.Provider)]
	if provider == nil {
		return nil, fmt.Errorf("%w: %s", ErrProviderUnavailable, reader.Provider)
	}
	return provider, nil
}
