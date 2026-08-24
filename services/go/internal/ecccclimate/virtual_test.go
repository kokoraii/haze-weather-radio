package ecccclimate

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResolveVirtualStationUsesCurrentPhysicalMember(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.URL.Query().Get("VIRTUAL_CLIMATE_ID"); got != "VSSK32V" {
			t.Fatalf("virtual ID = %q", got)
		}
		_, _ = writer.Write([]byte(`{
          "features": [
            {"properties":{"VIRTUAL_CLIMATE_ID":"VSSK32V","VIRTUAL_STATION_NAME_E":"REGINA AREA","VIRTUAL_STATION_NAME_F":"RÉGION DE REGINA","WXO_CITY_CODE":"SK-32","CLIMATE_IDENTIFIER":"4016560","END_DATE":"2007-11-30T00:00:00Z"}},
            {"properties":{"VIRTUAL_CLIMATE_ID":"VSSK32V","VIRTUAL_STATION_NAME_E":"REGINA AREA","VIRTUAL_STATION_NAME_F":"RÉGION DE REGINA","WXO_CITY_CODE":"SK-32","CLIMATE_IDENTIFIER":"4016699","END_DATE":null}},
            {"properties":{"VIRTUAL_CLIMATE_ID":"VSSK32V","CLIMATE_IDENTIFIER":"4016699","END_DATE":null}}
          ]
        }`))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	station, found, err := ResolveVirtualStationFromBase(ctx, server.Client(), server.URL, "vs sk32v")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("virtual station was not found")
	}
	if station.ID != "VSSK32V" || station.CurrentClimateID != "4016699" {
		t.Fatalf("station = %#v", station)
	}
	if station.Name != "REGINA AREA" || station.NameFR != "RÉGION DE REGINA" || station.CitypageID != "SK-32" {
		t.Fatalf("station metadata = %#v", station)
	}
}

func TestIsVirtualClimateID(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"VSSK32V", "vsqc33v", "VSON143V"} {
		if !IsVirtualClimateID(value) {
			t.Fatalf("%q should be a virtual climate ID", value)
		}
	}
	for _, value := range []string{"4057165", "VS123", "VS-AB22V"} {
		if IsVirtualClimateID(value) {
			t.Fatalf("%q should not be a virtual climate ID", value)
		}
	}
}
