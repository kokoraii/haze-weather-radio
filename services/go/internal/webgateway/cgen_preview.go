package webgateway

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const cgenPreviewBoundary = "haze-cgen-preview"

func (s *Server) cgenPreview(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	if _, ok := s.requireAdminRequest(writer, request); !ok {
		return
	}
	feedID := strings.TrimSpace(request.URL.Query().Get("feed"))
	if feedID == "" {
		http.Error(writer, "feed is required", http.StatusBadRequest)
		return
	}
	path := resolveConfigPath(s.configPath, filepath.Join("runtime", "cgen", safeCgenRuntimeID(feedID)+".preview.jpg"))
	lastInfo, err := os.Stat(path)
	if err != nil || lastInfo.Size() == 0 {
		http.NotFound(writer, request)
		return
	}
	if request.Method == http.MethodHead {
		writer.Header().Set("Content-Type", "multipart/x-mixed-replace; boundary="+cgenPreviewBoundary)
		return
	}
	initial, err := os.ReadFile(path)
	if err != nil || len(initial) == 0 {
		http.NotFound(writer, request)
		return
	}
	flusher, _ := writer.(http.Flusher)
	writer.Header().Set("Content-Type", "multipart/x-mixed-replace; boundary="+cgenPreviewBoundary)
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Accel-Buffering", "no")

	ticker := time.NewTicker(66 * time.Millisecond)
	defer ticker.Stop()
	last := initial
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	if _, err := fmt.Fprintf(writer, "--%s\r\nContent-Type: image/jpeg\r\nContent-Length: %d\r\n\r\n", cgenPreviewBoundary, len(initial)); err != nil {
		return
	}
	if _, err := writer.Write(initial); err != nil {
		return
	}
	if _, err := writer.Write([]byte("\r\n")); err != nil {
		return
	}
	if flusher != nil {
		flusher.Flush()
	}
	for {
		raw, info, err := readChangedCgenPreview(path, lastInfo)
		if err == nil {
			lastInfo = info
			if info.Size() > 0 {
				if !deadline.Stop() {
					select {
					case <-deadline.C:
					default:
					}
				}
				deadline.Reset(30 * time.Second)
			}
		}
		if err == nil && len(raw) > 0 {
			if !bytes.Equal(raw, last) {
				last = append(last[:0], raw...)
				if _, err := fmt.Fprintf(writer, "--%s\r\nContent-Type: image/jpeg\r\nContent-Length: %d\r\n\r\n", cgenPreviewBoundary, len(raw)); err != nil {
					return
				}
				if _, err := writer.Write(raw); err != nil {
					return
				}
				if _, err := writer.Write([]byte("\r\n")); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
		}
		select {
		case <-request.Context().Done():
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}

// CGEN atomically replaces preview frames. Checking file identity and metadata
// avoids decoding the same JPEG from disk on every stream tick.
func readChangedCgenPreview(path string, previous os.FileInfo) ([]byte, os.FileInfo, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, previous, err
	}
	if os.SameFile(previous, info) && previous.Size() == info.Size() && previous.ModTime().Equal(info.ModTime()) {
		return nil, info, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, previous, err
	}
	return raw, info, nil
}
