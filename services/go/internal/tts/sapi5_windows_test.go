//go:build windows

package tts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSAPI5SynthesizeHonorsCanceledContextBeforeCOM(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewSAPI5Provider().Synthesize(ctx, Request{Text: "test"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestPrepareSAPITextFixesWeatherPronunciations(t *testing.T) {
	got := prepareSAPIText("Winds north at 40 kilometers per hour and 10 kilometres near Windsor.")
	want := "windz north at 40 killometers per hour and 10 killometers near Windsor."
	if got != want {
		t.Fatalf("SAPI5 text = %q, want %q", got, want)
	}
}

func TestSAPIRateAndVolumeClampToCOMRange(t *testing.T) {
	if got := sapiRate(-99); got != -10 {
		t.Fatalf("low rate = %d", got)
	}
	if got := sapiRate(99); got != 10 {
		t.Fatalf("high rate = %d", got)
	}
	if got := sapiVolume(-1); got != 0 {
		t.Fatalf("low volume = %d", got)
	}
	if got := sapiVolume(150); got != 100 {
		t.Fatalf("high volume = %d", got)
	}
}

func TestSAPILanguagesNormalizesWindowsLCIDs(t *testing.T) {
	got := sapiLanguages("0409;0x1009;c0c")
	want := []string{"en-US", "en-CA", "fr-CA"}
	if len(got) != len(want) {
		t.Fatalf("languages = %#v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("languages = %#v, want %#v", got, want)
		}
	}
}

func TestMergeSAPIVoicesDeduplicatesNativeAndShim(t *testing.T) {
	native := []Voice{
		{ID: "native-id", Name: "Microsoft Linda", Provider: "sapi5"},
	}
	shim := []Voice{
		{ID: "shim-duplicate", Name: "Microsoft Linda", Provider: "sapi5"},
		{ID: "shim-only", Name: "Old 32 Bit Voice", Provider: "sapi5"},
	}

	got := mergeSAPIVoices(native, shim)

	if len(got) != 2 {
		t.Fatalf("voices = %#v", got)
	}
	if got[1].ID != "shim-only" {
		t.Fatalf("shim voice was not preserved: %#v", got)
	}
}

func TestSAPIAttempts(t *testing.T) {
	classNotRegistered := errors.New("Class not registered")
	x64Fails := sapiAttempt{name: "x64", run: func(context.Context, Request) (Audio, error) { return Audio{}, classNotRegistered }}
	shimAudio := Audio{Format: FormatWAV, Data: []byte("RIFF")}
	x86Speaks := sapiAttempt{name: "x86", run: func(context.Context, Request) (Audio, error) { return shimAudio, nil }}
	req := Request{Text: "test", VoiceID: "VE_Old_Voice"}

	t.Run("any failure moves on to the next process", func(t *testing.T) {
		got, err := synthesizeSAPIAttempts(context.Background(), req, []sapiAttempt{x64Fails, x86Speaks})
		if err != nil || string(got.Data) != "RIFF" {
			t.Fatalf("got %v, %v", got, err)
		}
	})
	t.Run("every failure is reported with its process", func(t *testing.T) {
		notFound := errors.New(`SAPI5 voice "VE_Old_Voice" not found`)
		x86Fails := sapiAttempt{name: "x86", run: func(context.Context, Request) (Audio, error) { return Audio{}, notFound }}
		_, err := synthesizeSAPIAttempts(context.Background(), req, []sapiAttempt{x64Fails, x86Fails})
		if !errors.Is(err, classNotRegistered) || !errors.Is(err, notFound) || !strings.Contains(err.Error(), "x64: ") || !strings.Contains(err.Error(), "x86: ") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("an expired request does not retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := synthesizeSAPIAttempts(ctx, req, []sapiAttempt{x64Fails, {name: "x86", run: func(context.Context, Request) (Audio, error) {
			t.Fatal("x86 ran after the request expired")
			return Audio{}, nil
		}}})
		if !errors.Is(err, classNotRegistered) {
			t.Fatalf("err = %v", err)
		}
	})
}

// Runs the real shims when HAZE_SAPI5_SHIM_PATH and HAZE_SAPI5_SHIM_X64_PATH
// name built ones, for example those scripts/build-haze-tts-standalone.ps1
// puts in dist. Jobs run in parallel, each in its own process.
func TestSAPIShimsSynthesizeInParallelProcesses(t *testing.T) {
	for _, shim := range []sapiShim{sapiShimX64, sapiShimX86} {
		t.Run(shim.arch, func(t *testing.T) {
			if os.Getenv(shim.pathEnv) == "" {
				t.Skipf("set %s to a built %s", shim.pathEnv, shim.file)
			}
			path, ok := shim.path()
			if !ok {
				t.Fatalf("%s not found", os.Getenv(shim.pathEnv))
			}
			var voices []Voice
			if err := runSAPI5ShimJSON(context.Background(), path, "list", nil, &voices); err != nil || len(voices) == 0 {
				t.Fatalf("shim voices = %v, %v", voices, err)
			}
			if voices[0].Arch != shim.arch {
				t.Fatalf("voice arch = %q, want %s", voices[0].Arch, shim.arch)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			errs := make(chan error, 4)
			for n := range 4 {
				go func() {
					audio, err := shim.synthesizer(path)(ctx, Request{Text: fmt.Sprintf("Parallel job %d.", n), VoiceID: voices[n%len(voices)].ID})
					if err == nil {
						_, _, _, err = pcm16WAV(audio.Data)
					}
					errs <- err
				}()
			}
			for range 4 {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSAPINativeSlotAllowsOneCallAndCancelsWaiters(t *testing.T) {
	release, err := acquireSAPINative(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := acquireSAPINative(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second caller err = %v, want it to wait and time out", err)
	}
	release()
	again, err := acquireSAPINative(context.Background())
	if err != nil {
		t.Fatalf("slot not released: %v", err)
	}
	again()
}
