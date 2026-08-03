package main

import (
	"context"
	"encoding/json"
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

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "haze-location-import: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	database := flag.String("database", "", "existing legacy location SQLite database")
	output := flag.String("output", "", "candidate SQLite database to create")
	ecccArchive := flag.String("eccc-zip", "", "official ECCC Land_Unproj ZIP")
	nwsArchive := flag.String("nws-zip", "", "official NWS c_DDmmmYY.zip county archive")
	retrievedAt := flag.String("retrieved-at", "", "deterministic RFC3339 retrieval time")
	activate := flag.Bool("activate", false, "atomically replace --database after validation")
	backupDir := flag.String("backup-dir", "", "directory for retained generations when --activate is used")
	flag.Parse()
	if strings.TrimSpace(*database) == "" || strings.TrimSpace(*output) == "" {
		return fmt.Errorf("--database and --output are required")
	}
	if strings.TrimSpace(*ecccArchive) == "" && strings.TrimSpace(*nwsArchive) == "" {
		return fmt.Errorf("at least one of --eccc-zip or --nws-zip is required")
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
	if err := locationimport.CloneDatabase(ctx, *database, *output); err != nil {
		return fmt.Errorf("clone database: %w", err)
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
		backupName, err := locationimport.ActivateDatabase(ctx, *database, *output, *backupDir, generation, now)
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
