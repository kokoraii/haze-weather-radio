//go:build windows

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/tts"
)

// defaultTimeout bounds a shim whose caller set no deadline, so a hung
// 32-bit engine cannot keep the process alive forever.
const defaultTimeout = 2 * time.Minute

type synthRequest struct {
	Text            string          `json:"text"`
	VoiceID         string          `json:"voice_id,omitempty"`
	Language        string          `json:"language,omitempty"`
	OutputFormat    tts.AudioFormat `json:"output_format,omitempty"`
	Rate            int             `json:"rate,omitempty"`
	Volume          int             `json:"volume,omitempty"`
	SentenceSilence float64         `json:"sentence_silence,omitempty"`
	TimeoutMS       int64           `json:"timeout_ms,omitempty"`
}

type synthResponse struct {
	Format     tts.AudioFormat `json:"format"`
	SampleRate int             `json:"sample_rate,omitempty"`
	Channels   int             `json:"channels,omitempty"`
	DataBase64 string          `json:"data_base64"`
}

func main() {
	os.Setenv("HAZE_SAPI5_SHIM_DISABLED", "1")
	if len(os.Args) < 2 {
		fail("usage: haze-sapi5-shim.exe list|synthesize")
	}
	provider := tts.NewSAPI5Provider()
	switch os.Args[1] {
	case "list":
		voices, err := provider.ListVoices(watchdog(defaultTimeout))
		if err != nil {
			fail(err.Error())
		}
		writeJSON(voices)
	case "synthesize":
		var payload synthRequest
		if err := json.NewDecoder(os.Stdin).Decode(&payload); err != nil {
			fail(fmt.Sprintf("invalid synthesis request: %v", err))
		}
		timeout := defaultTimeout
		if payload.TimeoutMS > 0 {
			timeout = time.Duration(payload.TimeoutMS) * time.Millisecond
		}
		audio, err := provider.Synthesize(watchdog(timeout), tts.Request{
			Text:            payload.Text,
			VoiceID:         payload.VoiceID,
			Language:        payload.Language,
			OutputFormat:    payload.OutputFormat,
			Rate:            payload.Rate,
			Volume:          payload.Volume,
			SentenceSilence: payload.SentenceSilence,
		})
		if err != nil {
			fail(err.Error())
		}
		writeJSON(synthResponse{
			Format:     audio.Format,
			SampleRate: audio.SampleRate,
			Channels:   audio.Channels,
			DataBase64: base64.StdEncoding.EncodeToString(audio.Data),
		})
	default:
		fail("unknown command")
	}
}

// watchdog exits the shim once timeout passes. A context cannot stop a
// synchronous SAPI Speak call, and an old 32-bit engine can hang in one.
func watchdog(timeout time.Duration) context.Context {
	time.AfterFunc(timeout, func() { fail("32-bit SAPI5 call timed out") })
	return context.Background()
}

func writeJSON(value any) {
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		fail(err.Error())
	}
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
