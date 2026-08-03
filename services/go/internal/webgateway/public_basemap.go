package webgateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const publicBasemapProfileMaxBytes = 128 * 1024

type publicBasemapProfile struct {
	SchemaVersion int                    `json:"schema_version"`
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	StyleURL      string                 `json:"style_url"`
	Attribution   string                 `json:"attribution"`
	Satellite     publicSatelliteProfile `json:"satellite"`
}

type publicSatelliteProfile struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	TileURL     string `json:"tile_url"`
	Attribution string `json:"attribution"`
	MaxZoom     int    `json:"max_zoom"`
}

func (s *Server) publicBasemap(writer http.ResponseWriter, request *http.Request) {
	if !requestMethodGETOrHEAD(writer, request) {
		return
	}
	profile, ok := s.loadPublicBasemapProfile()
	if !ok {
		http.NotFound(writer, request)
		return
	}
	writeJSON(writer, profile)
}

func (s *Server) loadPublicBasemapProfile() (publicBasemapProfile, bool) {
	profilePath := resolveConfigPath(s.configPath, filepath.Join("managed", "maps", "public-basemap.json"))
	info, err := os.Lstat(profilePath)
	if err != nil || !info.Mode().IsRegular() || info.Size() > publicBasemapProfileMaxBytes {
		return publicBasemapProfile{}, false
	}
	raw, err := os.ReadFile(profilePath)
	if err != nil {
		return publicBasemapProfile{}, false
	}
	var profile publicBasemapProfile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil || decoder.Decode(&struct{}{}) != io.EOF || !validPublicBasemapProfile(profile) {
		return publicBasemapProfile{}, false
	}
	return profile, true
}

func validPublicBasemapProfile(profile publicBasemapProfile) bool {
	if profile.SchemaVersion != 1 || strings.TrimSpace(profile.ID) == "" || strings.TrimSpace(profile.Name) == "" || !safePublicBasemapAttribution(profile.Attribution) || !validPublicSatelliteProfile(profile.Satellite) {
		return false
	}
	parsed, err := url.Parse(strings.TrimSpace(profile.StyleURL))
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "tiles.openfreemap.org") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	return strings.EqualFold(parsed.EscapedPath(), "/styles/dark")
}

func validPublicSatelliteProfile(profile publicSatelliteProfile) bool {
	if strings.TrimSpace(profile.ID) == "" || strings.TrimSpace(profile.Name) == "" || !safePublicBasemapAttribution(profile.Attribution) || profile.MaxZoom < 1 || profile.MaxZoom > 20 {
		return false
	}
	parsed, err := url.Parse(strings.TrimSpace(profile.TileURL))
	if err != nil || parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), "server.arcgisonline.com") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	return strings.EqualFold(parsed.Path, "/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}")
}

func safePublicBasemapAttribution(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && len(value) <= 240 && !strings.ContainsAny(value, "<>&\"'")
}
