package webgateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/meowraii/haze-weather-radio/services/go/internal/events"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationdb"
	"github.com/meowraii/haze-weather-radio/services/go/internal/locationimport"
)

const (
	locationCatalogUploadMaxBytes = int64(96 << 20)
	locationCatalogImportTimeout  = 2 * time.Minute
	ecccCLCCatalogSourceURL       = "https://dd.weather.gc.ca/meteocode/geodata/"
)

var locationCatalogImportMu sync.Mutex

func (s *Server) locationCatalogImport(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", "POST")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if _, ok := s.requireAdminRequest(writer, request); !ok {
		return
	}
	if !strings.EqualFold(strings.TrimSpace(request.Header.Get("X-Haze-Admin-Intent")), "command") {
		writeJSONStatus(writer, http.StatusForbidden, map[string]any{"detail": "explicit administrator intent is required"})
		return
	}
	if !locationCatalogImportMu.TryLock() {
		writeJSONStatus(writer, http.StatusConflict, map[string]any{"detail": "another location catalog import is already running"})
		return
	}
	defer locationCatalogImportMu.Unlock()

	request.Body = http.MaxBytesReader(writer, request.Body, locationCatalogUploadMaxBytes+(2<<20))
	if err := request.ParseMultipartForm(2 << 20); err != nil {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"detail": "catalog upload is invalid or too large"})
		return
	}
	if request.MultipartForm != nil {
		defer request.MultipartForm.RemoveAll()
	}
	upload, header, err := request.FormFile("file")
	if err != nil {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"detail": "ECCC catalog ZIP is required"})
		return
	}
	defer upload.Close()
	if !strings.EqualFold(filepath.Ext(strings.TrimSpace(header.Filename)), ".zip") {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"detail": "ECCC catalog file must be a ZIP archive"})
		return
	}
	uploadFile, err := os.CreateTemp("", "haze-eccc-clc-*.zip")
	if err != nil {
		writeJSONStatus(writer, http.StatusInternalServerError, map[string]any{"detail": "catalog upload could not be staged"})
		return
	}
	uploadPath := uploadFile.Name()
	defer os.Remove(uploadPath)
	_ = uploadFile.Chmod(0o600)
	written, copyErr := io.Copy(uploadFile, io.LimitReader(upload, locationCatalogUploadMaxBytes+1))
	closeErr := uploadFile.Close()
	if copyErr != nil || closeErr != nil {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"detail": "catalog upload could not be read"})
		return
	}
	if written == 0 || written > locationCatalogUploadMaxBytes {
		writeJSONStatus(writer, http.StatusRequestEntityTooLarge, map[string]any{"detail": "catalog upload exceeds the 96 MiB limit"})
		return
	}

	baseDir := filepath.Dir(filepath.Clean(s.configPath))
	activePath := locationdb.Path(baseDir)
	if stat, err := os.Stat(activePath); err != nil || stat.IsDir() {
		writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]any{"detail": "active location catalog is unavailable"})
		return
	}
	candidate, err := os.CreateTemp(filepath.Dir(activePath), ".alert-location-candidate-*.sqlite")
	if err != nil {
		writeJSONStatus(writer, http.StatusInternalServerError, map[string]any{"detail": "catalog candidate could not be created"})
		return
	}
	candidatePath := candidate.Name()
	_ = candidate.Close()
	_ = os.Remove(candidatePath)
	defer os.Remove(candidatePath)

	ctx, cancel := context.WithTimeout(request.Context(), locationCatalogImportTimeout)
	defer cancel()
	if err := locationimport.CloneDatabase(ctx, activePath, candidatePath); err != nil {
		writeJSONStatus(writer, http.StatusInternalServerError, map[string]any{"detail": "active location catalog could not be cloned"})
		return
	}
	result, err := locationimport.ImportECCCArchive(ctx, candidatePath, uploadPath, filepath.Base(header.Filename), ecccCLCCatalogSourceURL, time.Now().UTC())
	if err != nil {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"detail": err.Error()})
		return
	}
	backupName, err := locationimport.ActivateDatabase(
		ctx, activePath, candidatePath,
		filepath.Join(filepath.Dir(activePath), "location-catalog-backups"),
		"eccc-clc-"+result.ProviderVersion, time.Now().UTC(),
	)
	if err != nil {
		writeJSONStatus(writer, http.StatusInternalServerError, map[string]any{"detail": "validated catalog could not be activated"})
		return
	}
	reloadRequested, reloadWarning := requestLocationCatalogReload(result)
	response := map[string]any{
		"accepted": true, "source": result.Source, "provider_version": result.ProviderVersion,
		"record_count": result.RecordCount, "imported_at": result.ImportedAt,
		"previous_generation": backupName, "reload_requested": reloadRequested,
	}
	if reloadWarning != "" {
		response["warning"] = reloadWarning
		writeJSONStatus(writer, http.StatusAccepted, response)
		return
	}
	writeJSON(writer, response)
}

func requestLocationCatalogReload(result locationimport.Result) (bool, string) {
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return false, "The catalog is active on disk, but the location service bridge is unavailable. Restart or reload haze-location before querying it."
	}
	publisher := events.NewHostBridgePublisher(bridgeAddr)
	defer publisher.Close()
	err := publisher.Publish(events.Event{
		Type: "location.catalog.reload.request", Source: "haze-web", Target: "haze-location",
		Subject: fmt.Sprintf("eccc-clc-%s", result.ProviderVersion),
		Data: map[string]any{
			"reason": "operator_catalog_import", "source": result.Source,
			"provider_version": result.ProviderVersion, "record_count": result.RecordCount,
		},
	})
	if err != nil {
		return false, "The catalog is active on disk, but its reload event could not be published. Restart or reload haze-location before querying it."
	}
	return true, ""
}
