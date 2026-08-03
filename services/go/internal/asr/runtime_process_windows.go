//go:build windows

package asr

import (
	"os/exec"
	"syscall"
)

func configureWhisperChild(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
}
