package ivr

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestAdaptiveVoiceCaptureIgnoresNoiseAndConfirmsSpeech(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := make(chan pcmFrame, 128)
	cfg := searchConfig{VADPrerollMS: 300, VADTrailingMS: 100, MinVoiceMS: 2000, MaxVoiceMS: 8000}
	capture := startAdaptiveVoiceCapture(ctx, frames, cfg)
	start := time.Unix(100, 0)
	for index := 0; index < 20; index++ {
		frames <- pcmFrame{Samples: repeatedSamples(320, 80), At: start.Add(time.Duration(index) * 20 * time.Millisecond)}
	}
	select {
	case <-capture.Onset:
		t.Fatal("background noise locked voice modality")
	default:
	}
	for index := 20; index < 28; index++ {
		frames <- pcmFrame{Samples: alternatingSamples(320, 4000), At: start.Add(time.Duration(index) * 20 * time.Millisecond)}
	}
	for index := 28; index < 34; index++ {
		frames <- pcmFrame{Samples: repeatedSamples(320, 30), At: start.Add(time.Duration(index) * 20 * time.Millisecond)}
	}
	select {
	case onset := <-capture.Onset:
		if onset.Before(start.Add(300*time.Millisecond)) || onset.After(start.Add(500*time.Millisecond)) {
			t.Fatalf("unexpected speech onset timestamp %s", onset)
		}
	case <-time.After(time.Second):
		t.Fatal("confirmed speech did not lock voice modality")
	}
	select {
	case result := <-capture.Done:
		if len(result.Samples) != searchPCMRate*2 {
			t.Fatalf("short capture was not padded to two seconds: %d samples", len(result.Samples))
		}
		if pcmIsSilent(result.Samples[:320]) {
			t.Fatal("capture did not retain speech preroll")
		}
	case <-time.After(time.Second):
		t.Fatal("voice capture did not finish after trailing silence")
	}
}

func TestPrivateSearchWAVIsAtomicAndPrivate(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "spool")
	requestID := "ivr-00112233445566778899aabbccddeeff"
	basename, err := writePrivateSearchWAV(dir, requestID, make([]int16, searchPCMRate*2))
	if err != nil {
		t.Fatalf("write private WAV: %v", err)
	}
	if basename != requestID+".wav" {
		t.Fatalf("unexpected basename %q", basename)
	}
	path := filepath.Join(dir, basename)
	wav, err := readWAVPCM16(path)
	if err != nil {
		t.Fatalf("read WAV: %v", err)
	}
	if wav.SampleRate != searchPCMRate || wav.Channels != 1 || len(wav.Samples) != searchPCMRate*2 {
		t.Fatalf("unexpected WAV format: %#v", wav)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != basename {
		t.Fatalf("temporary spool file remained: %#v", entries)
	}
	if info, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("audio file permissions are not private: %o", info.Mode().Perm())
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("audio file removal failed: %v", err)
	}
}

func repeatedSamples(count int, value int16) []int16 {
	out := make([]int16, count)
	for index := range out {
		out[index] = value
	}
	return out
}

func alternatingSamples(count int, amplitude int16) []int16 {
	out := make([]int16, count)
	for index := range out {
		if index%2 == 0 {
			out[index] = amplitude
		} else {
			out[index] = -amplitude
		}
	}
	return out
}

func pcmIsSilent(samples []int16) bool {
	for _, sample := range samples {
		if sample != 0 {
			return false
		}
	}
	return true
}
