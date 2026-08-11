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

// ActivateDatabase retains an existing active generation and atomically
// installs a validated candidate. A first installation has no backup.
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
	candidateStat, err := os.Stat(candidatePath)
	if err != nil {
		return "", fmt.Errorf("inspect candidate database: %w", err)
	}
	if candidateStat.IsDir() {
		return "", errors.New("candidate database is a directory")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := os.MkdirAll(backupDir, 0o750); err != nil {
		return "", err
	}
	activeStat, activeErr := os.Stat(activePath)
	activeExists := activeErr == nil
	if activeErr != nil && !errors.Is(activeErr, os.ErrNotExist) {
		return "", activeErr
	}
	if activeExists && activeStat.IsDir() {
		return "", errors.New("active database is a directory")
	}
	generation = strings.Trim(safeGenerationPart.ReplaceAllString(generation, "-"), "-.")
	if generation == "" {
		generation = "catalog"
	}
	activeStem := strings.TrimSuffix(filepath.Base(activePath), filepath.Ext(activePath))
	activeStem = strings.Trim(safeGenerationPart.ReplaceAllString(activeStem, "-"), "-.")
	if activeStem == "" {
		activeStem = "catalog"
	}
	backupStem := fmt.Sprintf("%s-previous-%s-%s", activeStem, generation, now.UTC().Format("20060102T150405Z"))
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
	if activeExists {
		if err := CloneDatabase(ctx, activePath, backupPath); err != nil {
			return "", fmt.Errorf("retain previous location catalog: %w", err)
		}
	} else {
		backupName = ""
	}
	if err := installDatabaseCandidate(candidatePath, activePath); err != nil {
		return "", fmt.Errorf("activate location catalog: %w", err)
	}
	return backupName, nil
}

// ActivateGeometryDatabase binds a geometry candidate to the exact paired
// core generation and rejects orphan source-qualified identities before any
// active geometry database is retained or replaced.
func ActivateGeometryDatabase(ctx context.Context, activePath string, candidatePath string, corePath string, backupDir string, generation string, now time.Time) (string, error) {
	if strings.TrimSpace(corePath) == "" {
		return "", errors.New("paired core database path is required for geometry activation")
	}
	if err := BindGeometryToLegacyCore(ctx, candidatePath, corePath); err != nil {
		return "", fmt.Errorf("bind geometry candidate to paired core: %w", err)
	}
	if err := ValidateGeometryAgainstLegacyCore(ctx, candidatePath, corePath); err != nil {
		return "", fmt.Errorf("validate geometry candidate against paired core: %w", err)
	}
	return ActivateDatabase(ctx, activePath, candidatePath, backupDir, generation, now)
}

func installDatabaseCandidate(candidatePath string, activePath string) error {
	type movedSidecar struct{ from, to string }
	moved := []movedSidecar{}
	for _, suffix := range []string{"-wal", "-shm"} {
		from := activePath + suffix
		to := candidatePath + ".detached" + suffix
		if err := os.Rename(from, to); err == nil {
			moved = append(moved, movedSidecar{from: from, to: to})
		} else if !errors.Is(err, os.ErrNotExist) {
			for index := len(moved) - 1; index >= 0; index-- {
				_ = os.Rename(moved[index].to, moved[index].from)
			}
			return fmt.Errorf("detach active database sidecar: %w", err)
		}
	}
	if err := atomicReplace(candidatePath, activePath); err != nil {
		for index := len(moved) - 1; index >= 0; index-- {
			_ = os.Rename(moved[index].to, moved[index].from)
		}
		return err
	}
	for _, sidecar := range moved {
		_ = os.Remove(sidecar.to)
	}
	return nil
}

// RollbackDatabaseActivation restores the backup returned by ActivateDatabase.
// An empty backup name means the activation was a first installation, so the
// newly installed active database is removed.
func RollbackDatabaseActivation(ctx context.Context, activePath string, backupDir string, backupName string) error {
	activePath = filepath.Clean(activePath)
	backupDir = filepath.Clean(backupDir)
	backupName = strings.TrimSpace(backupName)
	if backupName == "" {
		if err := os.Remove(activePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove first database installation: %w", err)
		}
		for _, suffix := range []string{"-wal", "-shm"} {
			if err := os.Remove(activePath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove first database installation sidecar: %w", err)
			}
		}
		return nil
	}
	if filepath.Base(backupName) != backupName || strings.Trim(safeGenerationPart.ReplaceAllString(backupName, "-"), "-.") != backupName {
		return errors.New("database rollback backup name is invalid")
	}
	backupPath := filepath.Join(backupDir, backupName)
	backupStat, err := os.Stat(backupPath)
	if err != nil {
		return fmt.Errorf("inspect database rollback backup: %w", err)
	}
	if backupStat.IsDir() {
		return errors.New("database rollback backup is a directory")
	}
	temporary, err := os.CreateTemp(filepath.Dir(activePath), ".database-rollback-*.sqlite")
	if err != nil {
		return fmt.Errorf("create database rollback candidate: %w", err)
	}
	rollbackCandidate := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(rollbackCandidate)
		return err
	}
	if err := os.Remove(rollbackCandidate); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	defer os.Remove(rollbackCandidate)
	if err := CloneDatabase(ctx, backupPath, rollbackCandidate); err != nil {
		return fmt.Errorf("clone database rollback backup: %w", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Remove(activePath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove database rollback sidecar: %w", err)
		}
	}
	if err := atomicReplace(rollbackCandidate, activePath); err != nil {
		return fmt.Errorf("restore database rollback backup: %w", err)
	}
	return nil
}
