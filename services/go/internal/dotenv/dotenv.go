// Package dotenv loads the simple KEY=VALUE .env files Haze services share.
package dotenv

import (
	"bufio"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Load sets each KEY=VALUE entry in path that the process environment does not
// already define. A missing file named .env is created from a sibling
// .env.example when one exists, otherwise it is skipped with a warning.
func Load(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		created, createErr := createFromExample(path)
		if createErr != nil {
			return createErr
		}
		if !created {
			log.Printf("WARN .env file not found: %s", path)
			return nil
		}
		if file, err = os.Open(path); err != nil {
			return err
		}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, trimValue(value))
	}
	return scanner.Err()
}

func createFromExample(path string) (bool, error) {
	if filepath.Base(path) != ".env" {
		return false, nil
	}
	examplePath := filepath.Join(filepath.Dir(path), ".env.example")
	raw, err := os.ReadFile(examplePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("WARN .env file not found and no .env.example is available: %s", path)
			return false, nil
		}
		return false, err
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return false, err
	}
	log.Printf("WARN .env file not found: created %s from %s", path, examplePath)
	return true, nil
}

func trimValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		quote := value[0]
		if (quote == '"' || quote == '\'') && value[len(value)-1] == quote {
			return value[1 : len(value)-1]
		}
	}
	return value
}
