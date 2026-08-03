package asr

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Transcriber is the provider boundary used by the bounded worker pool.
type Transcriber interface {
	Transcribe(ctx context.Context, audioPath string, language string, prompt string) (string, error)
}

type providerFailure struct {
	code           string
	retryable      bool
	restartRuntime bool
	cause          error
}

func (e *providerFailure) Error() string {
	if e.cause == nil {
		return fmt.Sprintf("local Whisper failure: %s", e.code)
	}
	return fmt.Sprintf("local Whisper failure: %s: %v", e.code, e.cause)
}

func (e *providerFailure) Unwrap() error {
	return e.cause
}

func classifyProviderError(err error) (string, bool) {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout", true
	}
	var failure *providerFailure
	if errors.As(err, &failure) {
		return failure.code, failure.retryable
	}
	return "provider_error", false
}

func primaryLanguage(language string) string {
	language = strings.ToLower(strings.TrimSpace(language))
	if language == "" {
		return ""
	}
	if index := strings.IndexAny(language, "-_"); index > 0 {
		language = language[:index]
	}
	if len(language) != 2 {
		return ""
	}
	return language
}
