package asr

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLocalWhisperRuntimeIntegration(t *testing.T) {
	runtimePath := strings.TrimSpace(os.Getenv("HAZE_TEST_WHISPER_RUNTIME"))
	modelPath := strings.TrimSpace(os.Getenv("HAZE_TEST_WHISPER_MODEL"))
	audioPath := strings.TrimSpace(os.Getenv("HAZE_TEST_WHISPER_AUDIO"))
	if runtimePath == "" || modelPath == "" || audioPath == "" {
		t.Skip("set HAZE_TEST_WHISPER_RUNTIME, HAZE_TEST_WHISPER_MODEL, and HAZE_TEST_WHISPER_AUDIO")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	transcriber, err := newLocalWhisperTranscriber(ctx, Config{
		ModelPath:             modelPath,
		RuntimeExecutable:     runtimePath,
		RuntimeThreads:        4,
		RuntimeStartupTimeout: 60 * time.Second,
		MaxAudioBytes:         defaultMaxAudioBytes,
	})
	if err != nil {
		t.Fatalf("start local Whisper runtime: %v", err)
	}
	defer transcriber.Close()

	requestCtx, requestCancel := context.WithTimeout(ctx, defaultTimeout)
	defer requestCancel()
	text, err := transcriber.Transcribe(requestCtx, audioPath, "en", "A Canadian location")
	if err != nil {
		t.Fatalf("transcribe with local Whisper runtime: %v", err)
	}
	if strings.TrimSpace(text) == "" {
		t.Fatal("local Whisper runtime returned an empty transcript")
	}
	if expected := strings.ToLower(strings.TrimSpace(os.Getenv("HAZE_TEST_WHISPER_EXPECT"))); expected != "" && !strings.Contains(strings.ToLower(text), expected) {
		t.Fatalf("local Whisper transcript did not contain the expected synthetic phrase")
	}
}
