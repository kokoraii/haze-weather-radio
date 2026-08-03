package locationimport

import (
	"archive/zip"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestActivateDatabaseRetainsPriorGenerationAndRemovesDetachedSidecars(t *testing.T) {
	root := t.TempDir()
	active := filepath.Join(root, "alert_location_map.sqlite")
	candidate := filepath.Join(root, "candidate.sqlite")
	for path, value := range map[string]string{active: "old", candidate: "new"} {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("CREATE TABLE generation(value TEXT); INSERT INTO generation VALUES(?)", value); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(active+"-wal", []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(active+"-shm", []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	backupName, err := ActivateDatabase(context.Background(), active, candidate, filepath.Join(root, "backups"), "6.15.0", time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(active + suffix); !os.IsNotExist(err) {
			t.Fatalf("active sidecar %s was retained", suffix)
		}
	}
	readGeneration := func(path string) string {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var value string
		if err := db.QueryRow("SELECT value FROM generation").Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if value := readGeneration(active); value != "new" {
		t.Fatalf("active generation = %q", value)
	}
	if value := readGeneration(filepath.Join(root, "backups", backupName)); value != "old" {
		t.Fatalf("backup generation = %q", value)
	}
}

func TestValidateArchiveRejectsTraversalAndSymlink(t *testing.T) {
	for name, configure := range map[string]func(*zip.Writer) error{
		"traversal": func(writer *zip.Writer) error {
			file, err := writer.Create("../outside.shp")
			if err != nil {
				return err
			}
			_, err = file.Write([]byte("shape"))
			return err
		},
		"symlink": func(writer *zip.Writer) error {
			header := &zip.FileHeader{Name: "link.shp"}
			header.SetMode(os.ModeSymlink | 0o777)
			file, err := writer.CreateHeader(header)
			if err != nil {
				return err
			}
			_, err = file.Write([]byte("target"))
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			archivePath := filepath.Join(t.TempDir(), "MSC_Geography_Pkg_V6_15_0_Land_Unproj.zip")
			file, err := os.Create(archivePath)
			if err != nil {
				t.Fatal(err)
			}
			writer := zip.NewWriter(file)
			if err := configure(writer); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := validateArchive(archivePath, filepath.Base(archivePath), true); err == nil {
				t.Fatal("unsafe archive was accepted")
			}
		})
	}
}

func TestNWSVersionSupportsOperationalFilename(t *testing.T) {
	version, err := nwsVersion("c_16ap26.zip")
	if err != nil {
		t.Fatal(err)
	}
	if version != "2026-04-16" {
		t.Fatalf("version = %q", version)
	}
}

func TestOrganizeRingsAssignsHoleWithoutTrustingWinding(t *testing.T) {
	exterior := [][]float64{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}
	hole := [][]float64{{2, 2}, {2, 4}, {4, 4}, {4, 2}, {2, 2}}
	polygons := organizeRings([][][]float64{hole, exterior})
	if len(polygons) != 1 || len(polygons[0]) != 2 {
		t.Fatalf("polygons = %#v", polygons)
	}
	if ringArea(polygons[0][0]) <= 0 {
		t.Fatal("exterior ring is not counter-clockwise")
	}
	if ringArea(polygons[0][1]) >= 0 {
		t.Fatal("hole ring is not clockwise")
	}
}
