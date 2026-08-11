package webgateway

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	locationCatalogReloadTimeout  = 30 * time.Second
	ecccCLCCatalogSourceURL       = "https://dd.weather.gc.ca/meteocode/geodata/"
	legacyWeatherGeometryRelPath  = "managed/locations/legacy-weather-geometry.sqlite"
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
	corePath := locationdb.Path(baseDir)
	if stat, err := os.Stat(corePath); err != nil || stat.IsDir() {
		writeJSONStatus(writer, http.StatusServiceUnavailable, map[string]any{"detail": "paired core location catalog is unavailable"})
		return
	}
	activePath := filepath.Join(baseDir, filepath.FromSlash(legacyWeatherGeometryRelPath))
	if err := os.MkdirAll(filepath.Dir(activePath), 0o750); err != nil {
		writeJSONStatus(writer, http.StatusInternalServerError, map[string]any{"detail": "geometry catalog directory is unavailable"})
		return
	}
	candidate, err := os.CreateTemp(filepath.Dir(activePath), ".legacy-weather-geometry-candidate-*.sqlite")
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
	if err := locationimport.PrepareGeometryCandidate(ctx, activePath, candidatePath); err != nil {
		writeJSONStatus(writer, http.StatusInternalServerError, map[string]any{"detail": "geometry catalog candidate could not be prepared"})
		return
	}
	result, err := locationimport.ImportECCCArchive(ctx, candidatePath, uploadPath, filepath.Base(header.Filename), ecccCLCCatalogSourceURL, time.Now().UTC())
	if err != nil {
		writeJSONStatus(writer, http.StatusBadRequest, map[string]any{"detail": err.Error()})
		return
	}
	if err := locationimport.BindGeometryToLegacyCore(ctx, candidatePath, corePath); err != nil {
		writeJSONStatus(writer, http.StatusConflict, map[string]any{
			"detail": "The geometry package contains locations absent from the paired core catalog. Refresh the core catalog before importing this geometry generation.",
		})
		return
	}
	backupDir := filepath.Join(filepath.Dir(activePath), "location-catalog-backups")
	backupName, err := locationimport.ActivateGeometryDatabase(
		ctx, activePath, candidatePath, corePath, backupDir,
		"eccc-clc-"+result.ProviderVersion, time.Now().UTC(),
	)
	if err != nil {
		writeJSONStatus(writer, http.StatusInternalServerError, map[string]any{"detail": "validated catalog could not be activated"})
		return
	}
	reload, err := requestLocationCatalogReload(ctx, result, "operator_catalog_import")
	if err != nil {
		rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), locationCatalogReloadTimeout)
		defer rollbackCancel()
		if rollbackErr := locationimport.RollbackDatabaseActivation(rollbackCtx, activePath, backupDir, backupName); rollbackErr != nil {
			writeJSONStatus(writer, http.StatusInternalServerError, map[string]any{
				"accepted": false,
				"detail":   "haze-location rejected the imported catalog, and the previous managed geometry generation could not be restored",
				"error":    errors.Join(err, rollbackErr).Error(),
			})
			return
		}
		_, restoreErr := requestLocationCatalogReload(rollbackCtx, result, "operator_catalog_import_rollback")
		response := map[string]any{
			"accepted":    false,
			"rolled_back": true,
			"detail":      "haze-location did not confirm the imported catalog, so the previous managed geometry generation was restored",
			"error":       err.Error(),
		}
		if restoreErr != nil {
			response["reload_warning"] = "The previous catalog is restored on disk, but haze-location did not confirm reloading it. Restart haze-location before continuing."
		}
		writeJSONStatus(writer, http.StatusServiceUnavailable, response)
		return
	}
	response := map[string]any{
		"accepted": true, "source": result.Source, "provider_version": result.ProviderVersion,
		"record_count": result.RecordCount, "imported_at": result.ImportedAt,
		"pack_id": "legacy-weather-geometry", "previous_generation": backupName,
		"reload_confirmed": true, "catalog_generation": reload.CatalogGeneration,
		"catalog_packs": reload.CatalogPacks,
	}
	writeJSON(writer, response)
}

type locationCatalogReloadResult struct {
	CatalogGeneration string   `json:"catalog_generation"`
	CatalogPacks      []string `json:"catalog_packs"`
}

func requestLocationCatalogReload(ctx context.Context, result locationimport.Result, reason string) (locationCatalogReloadResult, error) {
	bridgeAddr := strings.TrimSpace(os.Getenv("HAZE_HOST_BRIDGE_ADDR"))
	if bridgeAddr == "" {
		return locationCatalogReloadResult{}, errors.New("location service bridge is unavailable")
	}
	reloadCtx, cancel := context.WithTimeout(ctx, locationCatalogReloadTimeout)
	defer cancel()
	connection, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(reloadCtx, "tcp", bridgeAddr)
	if err != nil {
		return locationCatalogReloadResult{}, fmt.Errorf("connect to location service bridge: %w", err)
	}
	defer connection.Close()
	deadline := time.Now().Add(locationCatalogReloadTimeout)
	if contextDeadline, ok := reloadCtx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return locationCatalogReloadResult{}, err
	}
	clientID := fmt.Sprintf("haze-web-location-import-%d", time.Now().UnixNano())
	encoder := json.NewEncoder(connection)
	if err := encoder.Encode(map[string]any{
		"type": "bridge.client",
		"data": map[string]any{
			"client_id": clientID, "receive_events": true,
			"subscriptions": []string{"location.catalog.reloaded", "location.catalog.reload_failed"},
		},
	}); err != nil {
		return locationCatalogReloadResult{}, fmt.Errorf("register location catalog reload client: %w", err)
	}
	if err := encoder.Encode(events.Event{
		Type: "location.catalog.reload.request", Source: clientID, ReplyTo: clientID, Target: "haze-location",
		Subject: fmt.Sprintf("eccc-clc-%s", result.ProviderVersion),
		Data: map[string]any{
			"reason": reason, "source": result.Source,
			"provider_version": result.ProviderVersion, "record_count": result.RecordCount,
			"pack_id": "legacy-weather-geometry",
		},
	}); err != nil {
		return locationCatalogReloadResult{}, fmt.Errorf("publish location catalog reload request: %w", err)
	}
	scanner := bufio.NewScanner(connection)
	scanner.Buffer(make([]byte, 16*1024), 1<<20)
	for scanner.Scan() {
		var envelope struct {
			Type   string          `json:"type"`
			Target string          `json:"target"`
			Data   json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil || envelope.Target != clientID {
			continue
		}
		switch envelope.Type {
		case "location.catalog.reloaded":
			var reload locationCatalogReloadResult
			if err := json.Unmarshal(envelope.Data, &reload); err != nil {
				return locationCatalogReloadResult{}, fmt.Errorf("decode location catalog reload response: %w", err)
			}
			if strings.TrimSpace(reload.CatalogGeneration) == "" {
				return locationCatalogReloadResult{}, errors.New("location service returned an empty catalog generation")
			}
			return reload, nil
		case "location.catalog.reload_failed":
			var failure struct {
				Error string `json:"error"`
				Code  string `json:"code"`
			}
			if err := json.Unmarshal(envelope.Data, &failure); err != nil {
				return locationCatalogReloadResult{}, fmt.Errorf("decode location catalog reload failure: %w", err)
			}
			message := strings.TrimSpace(failure.Error)
			if message == "" {
				message = strings.TrimSpace(failure.Code)
			}
			if message == "" {
				message = "location service rejected the catalog reload"
			}
			return locationCatalogReloadResult{}, errors.New(message)
		}
	}
	if err := scanner.Err(); err != nil {
		if reloadCtx.Err() != nil {
			return locationCatalogReloadResult{}, fmt.Errorf("location catalog reload acknowledgement: %w", reloadCtx.Err())
		}
		return locationCatalogReloadResult{}, fmt.Errorf("read location catalog reload acknowledgement: %w", err)
	}
	return locationCatalogReloadResult{}, errors.New("location service bridge closed before acknowledging the catalog reload")
}
