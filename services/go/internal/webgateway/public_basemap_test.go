package webgateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestPublicBasemapReturnsManagedDetailedProfile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, "version: test\n")
	mustWrite(t, filepath.Join(dir, "managed", "maps", "public-basemap.json"), `{
  "schema_version": 1,
  "id": "haze-public-detailed-basemap",
  "name": "Haze detailed dark public basemap",
  "style_url": "https://tiles.openfreemap.org/styles/dark",
  "attribution": "Map data © OpenStreetMap contributors, basemap © OpenFreeMap",
  "satellite": {
    "id": "esri-world-imagery",
    "name": "Satellite",
    "tile_url": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
    "attribution": "Imagery © Esri and its imagery partners",
    "max_zoom": 19
  }
}`)

	server := NewServerWithSurface(Config{}, configPath, dir, string(SurfacePublic))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/public/v1/map/basemap", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var profile publicBasemapProfile
	if err := json.Unmarshal(response.Body.Bytes(), &profile); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if profile.StyleURL != "https://tiles.openfreemap.org/styles/dark" {
		t.Fatalf("style URL = %q", profile.StyleURL)
	}
	if profile.Satellite.TileURL == "" {
		t.Fatal("satellite profile is missing")
	}
}

func TestPublicBasemapRejectsUntrustedStyleProfile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, "version: test\n")
	mustWrite(t, filepath.Join(dir, "managed", "maps", "public-basemap.json"), `{
  "schema_version": 1,
  "id": "not-allowed",
  "name": "Not allowed",
  "style_url": "https://example.test/style.json",
  "attribution": "Example"
}`)

	server := NewServerWithSurface(Config{}, configPath, dir, string(SurfacePublic))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/public/v1/map/basemap", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestPublicBasemapRejectsHTMLAttribution(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	mustWrite(t, configPath, "version: test\n")
	mustWrite(t, filepath.Join(dir, "managed", "maps", "public-basemap.json"), `{
  "schema_version": 1,
  "id": "bad-attribution",
  "name": "Bad attribution",
  "style_url": "https://tiles.openfreemap.org/styles/dark",
  "attribution": "<img src=x onerror=alert(1)>"
}`)

	server := NewServerWithSurface(Config{}, configPath, dir, string(SurfacePublic))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/public/v1/map/basemap", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestPublicBasemapRejectsUntrustedSatelliteTiles(t *testing.T) {
	profile := publicBasemapProfile{
		SchemaVersion: 1, ID: "map", Name: "Map", StyleURL: "https://tiles.openfreemap.org/styles/dark",
		Attribution: "Open map", Satellite: publicSatelliteProfile{
			ID: "bad", Name: "Satellite", TileURL: "https://example.test/{z}/{y}/{x}", Attribution: "Example", MaxZoom: 19,
		},
	}
	if validPublicBasemapProfile(profile) {
		t.Fatal("untrusted satellite provider was accepted")
	}
}
