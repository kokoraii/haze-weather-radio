package productrender

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFetchLiveClimateSnapshotUsesCurrentStationDailyData(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/collections/climate-daily/items" {
			http.NotFound(writer, request)
			return
		}
		if got := request.URL.Query().Get("CLIMATE_IDENTIFIER"); got != "4016699" {
			t.Fatalf("climate identifier = %q", got)
		}
		_, _ = writer.Write([]byte(`{
          "timeStamp":"2026-08-11T18:00:00Z",
          "features":[{"properties":{
            "CLIMATE_IDENTIFIER":"4016699",
            "STATION_NAME":"REGINA RCS",
            "LOCAL_DATE":"2026-08-10",
            "MAX_TEMPERATURE":26.24,
            "MIN_TEMPERATURE":11.06,
            "TOTAL_PRECIPITATION_FLAG":"T",
            "SPEED_MAX_GUST":42,
            "DIRECTION_MAX_GUST":27
          }}]
        }`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	input, raw, ok := fetchLiveClimateSnapshotFromBase(ctx, locationXML{ID: "4016699", Source: "eccc"}, "America/Regina", server.URL)
	if !ok {
		t.Fatal("live climate snapshot was not loaded")
	}
	if input != "eccc:climate/4016699" || raw.Observations.High == nil || *raw.Observations.High != 26.2 || raw.Observations.Low == nil || *raw.Observations.Low != 11.1 {
		t.Fatalf("live climate snapshot = %#v input=%q", raw, input)
	}
	if !raw.Observations.PrecipitationTrace || raw.Observations.MaxGustDirection != "W" {
		t.Fatalf("climate flags or wind direction were lost: %#v", raw.Observations)
	}
}

func TestFetchLiveClimateSnapshotResolvesVirtualClimateStation(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/collections/ltce-stations/items":
			if got := request.URL.Query().Get("VIRTUAL_CLIMATE_ID"); got != "VSSK32V" {
				t.Fatalf("virtual climate identifier = %q", got)
			}
			_, _ = writer.Write([]byte(`{"features":[
              {"properties":{"VIRTUAL_CLIMATE_ID":"VSSK32V","VIRTUAL_STATION_NAME_E":"REGINA AREA","CLIMATE_IDENTIFIER":"4016560","END_DATE":"2007-11-30T00:00:00Z"}},
              {"properties":{"VIRTUAL_CLIMATE_ID":"VSSK32V","VIRTUAL_STATION_NAME_E":"REGINA AREA","CLIMATE_IDENTIFIER":"4016699","END_DATE":null}}
            ]}`))
		case "/collections/climate-daily/items":
			if got := request.URL.Query().Get("CLIMATE_IDENTIFIER"); got != "4016699" {
				t.Fatalf("daily climate identifier = %q", got)
			}
			_, _ = writer.Write([]byte(`{"features":[{"properties":{
              "LOCAL_DATE":"2026-08-10", "STATION_NAME":"REGINA RCS", "MAX_TEMPERATURE":24
            }}]}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	input, raw, ok := fetchLiveClimateSnapshotFromBase(
		ctx,
		locationXML{ID: "VSSK32V", Source: "eccc"},
		"America/Regina",
		server.URL,
	)
	if !ok {
		t.Fatal("virtual climate snapshot was not loaded")
	}
	if input != "eccc:climate/4016699;eccc:ltce/VSSK32V" || raw.Observations.High == nil || *raw.Observations.High != 24 {
		t.Fatalf("virtual climate snapshot = %#v input=%q", raw, input)
	}
	if localizedString(raw.Name, "en") != "REGINA AREA" {
		t.Fatalf("virtual climate station name = %#v", raw.Name)
	}
}
