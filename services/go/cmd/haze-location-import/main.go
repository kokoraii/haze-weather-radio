package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/locationimport"
)

const (
	ecccSourceURL = "https://dd.weather.gc.ca/20260803/WXO-DD/meteocode/geodata/version_6.15.0/"
	nwsSourceURL  = "https://www.weather.gov/gis/Counties"
)

var (
	activateLocationDatabase = locationimport.ActivateDatabase
	rollbackLocationDatabase = locationimport.RollbackDatabaseActivation
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "haze-location-import: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	database := flag.String("database", "", "active geometry SQLite database, or combined legacy input with --split-legacy")
	output := flag.String("output", "", "candidate geometry SQLite database to create")
	ecccArchive := flag.String("eccc-zip", "", "official ECCC Land_Unproj ZIP")
	nwsArchive := flag.String("nws-zip", "", "official NWS c_DDmmmYY.zip county archive")
	retrievedAt := flag.String("retrieved-at", "", "deterministic RFC3339 retrieval time")
	activate := flag.Bool("activate", false, "atomically replace --database after validation")
	backupDir := flag.String("backup-dir", "", "directory for retained generations when --activate is used")
	splitLegacy := flag.Bool("split-legacy", false, "split a combined legacy database into core and exact-WKB geometry candidates")
	coreOutput := flag.String("core-output", "", "stripped core candidate created by --split-legacy")
	coreBase := flag.String("core-base", "", "optional newer compact core to preserve and supplement during --split-legacy")
	geometryDatabase := flag.String("geometry-database", "", "active geometry pack installed by --split-legacy --activate")
	coreDatabase := flag.String("core-database", "", "paired legacy core used to reject orphan geometry identities before routine activation")
	flag.Parse()
	if strings.TrimSpace(*database) == "" || strings.TrimSpace(*output) == "" {
		return fmt.Errorf("--database and --output are required")
	}
	now := time.Now().UTC()
	if strings.TrimSpace(*retrievedAt) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*retrievedAt))
		if err != nil {
			return fmt.Errorf("--retrieved-at must be RFC3339: %w", err)
		}
		now = parsed.UTC()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if *splitLegacy {
		if strings.TrimSpace(*ecccArchive) != "" || strings.TrimSpace(*nwsArchive) != "" {
			return fmt.Errorf("provider archives cannot be combined with --split-legacy")
		}
		if strings.TrimSpace(*coreOutput) == "" {
			return fmt.Errorf("--core-output is required with --split-legacy")
		}
		if *activate && strings.TrimSpace(*geometryDatabase) == "" {
			return fmt.Errorf("--geometry-database is required with --split-legacy --activate")
		}
		if strings.TrimSpace(*coreDatabase) != "" {
			return fmt.Errorf("--core-database is for routine imports; --split-legacy validates against --core-output")
		}
		coreBasePath := strings.TrimSpace(*coreBase)
		if coreBasePath != "" {
			coreBasePath = filepath.Clean(coreBasePath)
		}
		return runLegacySplit(ctx, filepath.Clean(*database), coreBasePath, filepath.Clean(*coreOutput), filepath.Clean(*output), filepath.Clean(*geometryDatabase), strings.TrimSpace(*backupDir), *activate, now)
	}
	if strings.TrimSpace(*ecccArchive) == "" && strings.TrimSpace(*nwsArchive) == "" {
		return fmt.Errorf("at least one of --eccc-zip or --nws-zip is required")
	}
	if strings.TrimSpace(*coreOutput) != "" || strings.TrimSpace(*coreBase) != "" || strings.TrimSpace(*geometryDatabase) != "" {
		return fmt.Errorf("--core-output, --core-base, and --geometry-database require --split-legacy")
	}
	if *activate && strings.TrimSpace(*coreDatabase) == "" {
		return fmt.Errorf("--core-database is required with --activate so orphan geometries cannot replace the active pack")
	}
	if err := locationimport.PrepareGeometryCandidate(ctx, *database, *output); err != nil {
		return fmt.Errorf("prepare geometry candidate: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(filepath.Clean(*output))
		}
	}()
	results := []locationimport.Result{}
	if strings.TrimSpace(*ecccArchive) != "" {
		result, err := locationimport.ImportECCCArchive(ctx, *output, *ecccArchive, filepath.Base(*ecccArchive), ecccSourceURL, now)
		if err != nil {
			return fmt.Errorf("import ECCC CLC geometry: %w", err)
		}
		results = append(results, result)
	}
	if strings.TrimSpace(*nwsArchive) != "" {
		result, err := locationimport.ImportNWSArchive(ctx, *output, *nwsArchive, filepath.Base(*nwsArchive), nwsSourceURL, now)
		if err != nil {
			return fmt.Errorf("import NWS county geometry: %w", err)
		}
		results = append(results, result)
	}
	if strings.TrimSpace(*coreDatabase) != "" {
		if err := locationimport.BindGeometryToLegacyCore(ctx, *output, *coreDatabase); err != nil {
			return fmt.Errorf("geometry candidate is incompatible with --core-database; update the paired core names and identities before activation: %w", err)
		}
	}
	responseOutput := filepath.Clean(*output)
	previousGeneration := ""
	if *activate {
		if strings.TrimSpace(*backupDir) == "" {
			*backupDir = filepath.Join(filepath.Dir(filepath.Clean(*database)), "location-catalog-backups")
		}
		generation := "location-catalog"
		if len(results) > 0 {
			generation = results[0].Source + "-" + results[0].ProviderVersion
		}
		backupName, err := locationimport.ActivateGeometryDatabase(ctx, *database, *output, *coreDatabase, *backupDir, generation, now)
		if err != nil {
			return err
		}
		previousGeneration = backupName
		responseOutput = filepath.Clean(*database)
	} else {
		keep = true
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(map[string]any{"output": responseOutput, "imports": results, "previous_generation": previousGeneration}); err != nil {
		return err
	}
	return nil
}

func runLegacySplit(ctx context.Context, legacyPath string, coreBasePath string, coreOutputPath string, geometryOutputPath string, geometryDatabasePath string, backupDir string, activate bool, now time.Time) error {
	if activate {
		if filepath.Dir(legacyPath) != filepath.Dir(coreOutputPath) {
			return fmt.Errorf("--core-output must share a filesystem directory with --database when activating")
		}
		if filepath.Dir(geometryDatabasePath) != filepath.Dir(geometryOutputPath) {
			return fmt.Errorf("--output must share a filesystem directory with --geometry-database when activating")
		}
	}
	result, err := locationimport.SplitLegacyDatabaseWithCoreBase(ctx, legacyPath, coreBasePath, coreOutputPath, geometryOutputPath, now)
	if err != nil {
		return err
	}
	if err := locationimport.ValidateGeometryAgainstLegacyCore(ctx, geometryOutputPath, coreOutputPath); err != nil {
		return fmt.Errorf("split geometry candidate is incompatible with its stripped core: %w", err)
	}
	keepCandidates := !activate
	defer func() {
		if !keepCandidates {
			_ = os.Remove(coreOutputPath)
			_ = os.Remove(geometryOutputPath)
		}
	}()
	previousCore := ""
	previousGeometry := ""
	coreResultPath := coreOutputPath
	geometryResultPath := geometryOutputPath
	if activate {
		if strings.TrimSpace(backupDir) == "" {
			backupDir = filepath.Join(filepath.Dir(legacyPath), "location-catalog-backups")
		}
		previousGeometry, err = activateLocationDatabase(
			ctx, geometryDatabasePath, geometryOutputPath, backupDir, "legacy-geometry-split", now,
		)
		if err != nil {
			return fmt.Errorf("activate split geometry catalog: %w", err)
		}
		previousCore, err = activateLocationDatabase(
			ctx, legacyPath, coreOutputPath, backupDir, "legacy-core-split", now,
		)
		if err != nil {
			coreActivationError := fmt.Errorf("activate stripped core catalog after geometry installation: %w", err)
			rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer rollbackCancel()
			if rollbackErr := rollbackLocationDatabase(rollbackCtx, geometryDatabasePath, backupDir, previousGeometry); rollbackErr != nil {
				return errors.Join(coreActivationError, fmt.Errorf("rollback split geometry installation: %w", rollbackErr))
			}
			return coreActivationError
		}
		coreResultPath = filepath.Clean(legacyPath)
		geometryResultPath = filepath.Clean(geometryDatabasePath)
	}
	response := map[string]any{
		"core_output":                  coreResultPath,
		"geometry_output":              geometryResultPath,
		"geometry_count":               result.GeometryCount,
		"core_places_added":            result.CorePlacesAdded,
		"source_counts":                result.SourceCounts,
		"previous_core_generation":     previousCore,
		"previous_geometry_generation": previousGeometry,
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(response); err != nil {
		return err
	}
	keepCandidates = true
	return nil
}
