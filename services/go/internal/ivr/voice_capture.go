package ivr

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const searchPCMRate = 16000

type pcmFrame struct {
	Samples []int16
	At      time.Time
}

type capturedVoice struct {
	Samples []int16
	Onset   time.Time
}

type voiceCapture struct {
	Onset <-chan time.Time
	Done  <-chan capturedVoice
}

func startAdaptiveVoiceCapture(ctx context.Context, frames <-chan pcmFrame, cfg searchConfig) voiceCapture {
	onset := make(chan time.Time, 1)
	done := make(chan capturedVoice, 1)
	go func() {
		defer close(done)
		prerollFrames := maxInt(1, cfg.VADPrerollMS/20)
		trailingFrames := maxInt(1, cfg.VADTrailingMS/20)
		minSamples := searchPCMRate * cfg.MinVoiceMS / 1000
		maxSamples := searchPCMRate * cfg.MaxVoiceMS / 1000
		if maxSamples <= 0 || maxSamples > searchPCMRate*8 {
			maxSamples = searchPCMRate * 8
		}
		if minSamples <= 0 || minSamples > maxSamples {
			minSamples = minInt(searchPCMRate*2, maxSamples)
		}
		preroll := make([]pcmFrame, 0, prerollFrames)
		captured := make([]int16, 0, maxSamples)
		noiseFloor := 120.0
		candidateFrames := 0
		trailing := 0
		confirmed := false
		var onsetAt time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case frame, ok := <-frames:
				if !ok {
					return
				}
				if len(frame.Samples) == 0 {
					continue
				}
				level, peak := pcmLevel(frame.Samples)
				threshold := math.Max(420, noiseFloor*3.2+90)
				speech := level >= threshold && peak >= 900
				if !confirmed {
					preroll = append(preroll, clonePCMFrame(frame))
					if len(preroll) > prerollFrames {
						preroll = preroll[len(preroll)-prerollFrames:]
					}
					if speech {
						candidateFrames++
					} else {
						candidateFrames = 0
						noiseFloor = noiseFloor*0.96 + level*0.04
					}
					if candidateFrames < 3 {
						continue
					}
					confirmed = true
					onsetIndex := maxInt(0, len(preroll)-candidateFrames)
					onsetAt = preroll[onsetIndex].At
					if onsetAt.IsZero() {
						onsetAt = time.Now()
					}
					for _, buffered := range preroll {
						captured = appendLimitedSamples(captured, buffered.Samples, maxSamples)
					}
					select {
					case onset <- onsetAt:
					default:
					}
					if len(captured) >= maxSamples {
						done <- finalizeCapturedVoice(captured, onsetAt, minSamples, maxSamples)
						return
					}
					continue
				}

				captured = appendLimitedSamples(captured, frame.Samples, maxSamples)
				if speech {
					trailing = 0
				} else {
					trailing++
				}
				if trailing >= trailingFrames || len(captured) >= maxSamples {
					done <- finalizeCapturedVoice(captured, onsetAt, minSamples, maxSamples)
					return
				}
			}
		}
	}()
	return voiceCapture{Onset: onset, Done: done}
}

func pcmLevel(samples []int16) (float64, int) {
	if len(samples) == 0 {
		return 0, 0
	}
	var sum float64
	peak := 0
	for _, sample := range samples {
		value := int(sample)
		if value < 0 {
			value = -value
		}
		if value > peak {
			peak = value
		}
		floatSample := float64(sample)
		sum += floatSample * floatSample
	}
	return math.Sqrt(sum / float64(len(samples))), peak
}

func clonePCMFrame(frame pcmFrame) pcmFrame {
	return pcmFrame{Samples: append([]int16(nil), frame.Samples...), At: frame.At}
}

func appendLimitedSamples(target []int16, samples []int16, limit int) []int16 {
	remaining := limit - len(target)
	if remaining <= 0 {
		return target
	}
	if len(samples) > remaining {
		samples = samples[:remaining]
	}
	return append(target, samples...)
}

func finalizeCapturedVoice(samples []int16, onset time.Time, minimum int, maximum int) capturedVoice {
	if len(samples) > maximum {
		samples = samples[:maximum]
	}
	out := append([]int16(nil), samples...)
	if len(out) < minimum {
		out = append(out, make([]int16, minimum-len(out))...)
	}
	return capturedVoice{Samples: out, Onset: onset}
}

func newSearchRequestID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "ivr-" + hex.EncodeToString(raw), nil
}

func writePrivateSearchWAV(spoolDir string, requestID string, samples []int16) (string, error) {
	if strings.TrimSpace(requestID) == "" || len(samples) == 0 {
		return "", fmt.Errorf("request ID and captured audio are required")
	}
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		return "", err
	}
	_ = os.Chmod(spoolDir, 0o700)
	basename := requestID + ".wav"
	target := filepath.Join(filepath.Clean(spoolDir), basename)
	temp, err := os.CreateTemp(filepath.Clean(spoolDir), ".asr-*.tmp")
	if err != nil {
		return "", err
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return "", err
	}
	raw := pcm16WAV(samples, searchPCMRate)
	if _, err := temp.Write(raw); err != nil {
		_ = temp.Close()
		return "", err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return "", err
	}
	if err := temp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tempPath, target); err != nil {
		return "", err
	}
	return basename, nil
}

func pcm16WAV(samples []int16, sampleRate int) []byte {
	var output bytes.Buffer
	dataSize := uint32(len(samples) * 2)
	_, _ = output.WriteString("RIFF")
	_ = binary.Write(&output, binary.LittleEndian, uint32(36)+dataSize)
	_, _ = output.WriteString("WAVEfmt ")
	_ = binary.Write(&output, binary.LittleEndian, uint32(16))
	_ = binary.Write(&output, binary.LittleEndian, uint16(1))
	_ = binary.Write(&output, binary.LittleEndian, uint16(1))
	_ = binary.Write(&output, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(&output, binary.LittleEndian, uint32(sampleRate*2))
	_ = binary.Write(&output, binary.LittleEndian, uint16(2))
	_ = binary.Write(&output, binary.LittleEndian, uint16(16))
	_, _ = output.WriteString("data")
	_ = binary.Write(&output, binary.LittleEndian, dataSize)
	for _, sample := range samples {
		_ = binary.Write(&output, binary.LittleEndian, sample)
	}
	return output.Bytes()
}
