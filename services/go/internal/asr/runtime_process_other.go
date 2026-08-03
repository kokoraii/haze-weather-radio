//go:build !windows

package asr

import "os/exec"

func configureWhisperChild(_ *exec.Cmd) {}
