package locationimport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var safeGenerationPart = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// ActivateDatabase retains the active generation and atomically installs a validated candidate.
func ActivateDatabase(ctx context.Context, activePath string, candidatePath string, backupDir string, generation string, now time.Time) (string, error) {
	activePath = filepath.Clean(activePath)
	candidatePath = filepath.Clean(candidatePath)
	backupDir = filepath.Clean(backupDir)
	if activePath == candidatePath {
		return "", fmt.Errorf("candidate and active database paths must differ")
	}
	if filepath.Dir(activePath) != filepath.Dir(candidatePath) {
		return "", fmt.Errorf("candidate database must be on the active database filesystem")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := os.MkdirAll(backupDir, 0o750); err != nil {
		return "", err
	}
	generation = strings.Trim(safeGenerationPart.ReplaceAllString(generation, "-"), "-.")
	if generation == "" {
		generation = "catalog"
	}
	backupStem := fmt.Sprintf("alert_location_map-previous-%s-%s", generation, now.UTC().Format("20060102T150405Z"))
	backupName := backupStem + ".sqlite"
	backupPath := filepath.Join(backupDir, backupName)
	for sequence := 2; ; sequence++ {
		if _, err := os.Stat(backupPath); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return "", err
		}
		backupName = fmt.Sprintf("%s-%d.sqlite", backupStem, sequence)
		backupPath = filepath.Join(backupDir, backupName)
	}
	if err := CloneDatabase(ctx, activePath, backupPath); err != nil {
		return "", fmt.Errorf("retain previous location catalog: %w", err)
	}
	type movedSidecar struct{ from, to string }
	moved := []movedSidecar{}
	for _, suffix := range []string{"-wal", "-shm"} {
		from := activePath + suffix
		to := backupPath + ".detached" + suffix
		if err := os.Rename(from, to); err == nil {
			moved = append(moved, movedSidecar{from: from, to: to})
		} else if !errors.Is(err, os.ErrNotExist) {
			for index := len(moved) - 1; index >= 0; index-- {
				_ = os.Rename(moved[index].to, moved[index].from)
			}
			return "", fmt.Errorf("detach active database sidecar: %w", err)
		}
	}
	if err := atomicReplace(candidatePath, activePath); err != nil {
		for index := len(moved) - 1; index >= 0; index-- {
			_ = os.Rename(moved[index].to, moved[index].from)
		}
		return "", fmt.Errorf("activate location catalog: %w", err)
	}
	for _, sidecar := range moved {
		_ = os.Remove(sidecar.to)
	}
	return backupName, nil
}
