package alertvars

import (
	"testing"
	"time"
)

func TestExpand(t *testing.T) {
	loc, err := time.LoadLocation("America/Regina")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	feed := Feed{
		ID:           "cwxr-sk01",
		Transmitters: []string{"VAD123", "VBU829"},
		At:           time.Date(2026, time.November, 25, 12, 0, 0, 0, loc),
	}
	got := Expand("{feed-id} {transmitterCall} {transmitterCall[1]} {transmitterCall[5]}| {autoDay} {autoHour}:{autoMin} {autoAmPm} {timeZone} {bogus} {feed-id[1]}", feed)
	want := "cwxr-sk01 VAD123 VBU829 | Wednesday 12:00 PM Central Time {bogus} {feed-id[1]}"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTimeZoneName(t *testing.T) {
	cases := map[string]string{
		"America/Regina":   "Central Time",
		"America/Toronto":  "Eastern Daylight Time",
		"America/Edmonton": "Mountain Daylight Time",
	}
	for zone, want := range cases {
		loc, err := time.LoadLocation(zone)
		if err != nil {
			t.Skipf("tzdata unavailable: %v", err)
		}
		if got := TimeZoneName(time.Date(2026, time.July, 1, 12, 0, 0, 0, loc)); got != want {
			t.Errorf("%s = %q, want %q", zone, got, want)
		}
	}
}
