package webgateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCgenSaveAndActionsRoundTripXML(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("version: test\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	saved, err := saveCgenPayload(configPath, map[string]any{
		"enabled": true,
		"feeds": []any{
			map[string]any{
				"id":                              "CAP-IT-ALL",
				"name":                            "CAP CGEN",
				"enabled":                         true,
				"mode":                            "release",
				"program_input_url":               "udp://239.0.0.1:9000?overrun_nonfatal=1&reuse=1",
				"program_input_format":            "mpegts",
				"deinterlace_enabled":             true,
				"deinterlace_algorithm":           "yadif",
				"deinterlace_backend":             "software",
				"deinterlace_cadence":             "field",
				"deinterlace_parity":              "tff",
				"priority_feed_id":                "CAP-IT-ALL",
				"audio_source":                    "both",
				"audio_idle":                      "routine",
				"mute_standby_routine":            false,
				"program_output_url":              "udp://239.0.0.2:9001?pkt_size=1316",
				"program_output_format":           "mpegts",
				"vcodec":                          "libx264",
				"acodec":                          "aac",
				"video_bitrate_kbps":              "12000",
				"audio_bitrate_kbps":              "192",
				"video_gop":                       "30",
				"video_bframes":                   "2",
				"video_preset":                    "veryfast",
				"video_tune":                      "zerolatency",
				"audio_encoder_bitrate_kbps":      "256",
				"service_name":                    "Haze CGEN",
				"provider_name":                   "Haze",
				"service_id":                      "7",
				"transport_stream_id":             "9",
				"hd_program":                      "11",
				"hd_video_pid":                    "300",
				"hd_pmt_pid":                      "4300",
				"width":                           "1280",
				"height":                          "720",
				"fps":                             "source",
				"interlaced":                      true,
				"field_order":                     "bff",
				"standard":                        "atsc",
				"banner_background_enabled":       true,
				"font_weight":                     "regular",
				"scroll_speed":                    "6",
				"scroll_repeat_mode":              "until_audio_end",
				"after_eom_repeats":               "2",
				"fixed_repeats":                   "3",
				"standby_mode":                    "smpte",
				"standby_text":                    "EAS Details Channel",
				"standby_font_size":               "64",
				"standby_y_percent":               "10",
				"sync_hard_reset_ms":              "500",
				"sync_max_audio_frames_per_video": "10",
				"sync_source_buffer_ms":           "400",
				"sync_reconnect_initial_ms":       "250",
				"sync_reconnect_max_ms":           "5000",
				"sync_status_interval_ms":         "750",
			},
		},
	})
	if err != nil {
		t.Fatalf("save cgen: %v", err)
	}
	feeds := saved["feeds"].([]map[string]any)
	if len(feeds) != 1 {
		t.Fatalf("feeds = %#v", saved["feeds"])
	}
	if feeds[0]["program_input_url"] != "udp://239.0.0.1:9000?overrun_nonfatal=1&reuse=1" {
		t.Fatalf("program input = %#v", feeds[0]["program_input_url"])
	}
	if feeds[0]["deinterlace_enabled"] != true ||
		feeds[0]["deinterlace_algorithm"] != "yadif" ||
		feeds[0]["deinterlace_backend"] != "software" ||
		feeds[0]["deinterlace_cadence"] != "field" ||
		feeds[0]["deinterlace_parity"] != "tff" {
		t.Fatalf("deinterlace controls = %#v", feeds[0])
	}
	if feeds[0]["audio_source"] != "both" {
		t.Fatalf("audio source = %#v", feeds[0]["audio_source"])
	}
	if feeds[0]["audio_idle"] != "routine" {
		t.Fatalf("audio idle = %#v", feeds[0]["audio_idle"])
	}
	if feeds[0]["mute_standby_routine"] != false {
		t.Fatalf("standby routine mute = %#v", feeds[0]["mute_standby_routine"])
	}
	if feeds[0]["service_name"] != "Haze CGEN" ||
		feeds[0]["provider_name"] != "Haze" ||
		feeds[0]["service_id"] != "7" ||
		feeds[0]["transport_stream_id"] != "9" ||
		feeds[0]["hd_program"] != "11" ||
		feeds[0]["hd_video_pid"] != "300" ||
		feeds[0]["hd_pmt_pid"] != "4300" {
		t.Fatalf("routing controls = %#v", feeds[0])
	}
	if feeds[0]["video_gop"] != "30" ||
		feeds[0]["video_bframes"] != "2" ||
		feeds[0]["video_preset"] != "veryfast" ||
		feeds[0]["video_tune"] != "zerolatency" ||
		feeds[0]["audio_encoder_bitrate_kbps"] != "256" {
		t.Fatalf("encoder controls = %#v", feeds[0])
	}
	if feeds[0]["interlaced"] != true || feeds[0]["field_order"] != "bff" || feeds[0]["standard"] != "atsc" {
		t.Fatalf("video flags = %#v", feeds[0])
	}
	if feeds[0]["scroll_speed"] != "6" {
		t.Fatalf("banner controls = %#v", feeds[0])
	}
	if feeds[0]["scroll_repeat_mode"] != "until_audio_end" ||
		feeds[0]["after_eom_repeats"] != "2" ||
		feeds[0]["fixed_repeats"] != "3" {
		t.Fatalf("scroll repeat controls = %#v", feeds[0])
	}
	if feeds[0]["font_weight"] != "regular" {
		t.Fatalf("font weight = %#v", feeds[0])
	}
	rawXML, err := os.ReadFile(filepath.Join(dir, defaultCgenFile))
	if err != nil {
		t.Fatalf("read cgen xml: %v", err)
	}
	for _, want := range []string{
		"<program>",
		"<input url=\"udp://239.0.0.1:9000?overrun_nonfatal=1&amp;reuse=1\" format=\"mpegts\" deinterlace_enabled=\"true\" deinterlace_algorithm=\"yadif\" deinterlace_backend=\"software\" deinterlace_cadence=\"field\" deinterlace_parity=\"tff\"></input>",
		"<output url=\"udp://239.0.0.2:9001?pkt_size=1316\" format=\"mpegts\" vcodec=\"libx264\" acodec=\"aac\"",
		"<priority>",
		"<media>",
		"<presentation>",
		"<ticker height=\"128\" speed=\"6\">",
		"<repeat mode=\"until_audio_end\" after_eom=\"2\" count=\"3\"></repeat>",
	} {
		if !strings.Contains(string(rawXML), want) {
			t.Fatalf("written XML missing %q:\n%s", want, rawXML)
		}
	}
	if strings.Count(string(rawXML), "<program>") != 1 {
		t.Fatalf("written XML must have one program section:\n%s", rawXML)
	}
	rawEncoders, err := os.ReadFile(filepath.Join(dir, defaultCgenEncodersFile))
	if err != nil {
		t.Fatalf("read cgen encoders xml: %v", err)
	}
	for _, want := range []string{
		"<cgenEncoders",
		"<video codec=\"libx264\" bitrate_kbps=\"12000\" gop=\"30\" bframes=\"2\" preset=\"veryfast\" tune=\"zerolatency\"></video>",
		"<audio codec=\"aac\" bitrate_kbps=\"256\"></audio>",
	} {
		if !strings.Contains(string(rawEncoders), want) {
			t.Fatalf("written encoders XML missing %q:\n%s", want, rawEncoders)
		}
	}
	if feeds[0]["standby_mode"] != "smpte" ||
		feeds[0]["standby_text"] != "EAS Details Channel" ||
		feeds[0]["standby_font_size"] != "64" ||
		feeds[0]["standby_y_percent"] != "10" {
		t.Fatalf("standby controls = %#v", feeds[0])
	}
	if feeds[0]["sync_hard_reset_ms"] != "500" ||
		feeds[0]["sync_max_audio_frames_per_video"] != "10" ||
		feeds[0]["sync_source_buffer_ms"] != "400" ||
		feeds[0]["sync_reconnect_initial_ms"] != "250" ||
		feeds[0]["sync_reconnect_max_ms"] != "5000" ||
		feeds[0]["sync_status_interval_ms"] != "750" {
		t.Fatalf("sync controls = %#v", feeds[0])
	}

	acted, err := cgenActionPayload(configPath, map[string]any{
		"feed_id": "CAP-IT-ALL",
		"action":  "insert_text",
		"text":    "Weather alert",
	})
	if err != nil {
		t.Fatalf("action: %v", err)
	}
	feeds = acted["feeds"].([]map[string]any)
	if feeds[0]["text"] != "Weather alert" || feeds[0]["text_enabled"] != true || feeds[0]["mode"] != "overlay" {
		t.Fatalf("action feed = %#v", feeds[0])
	}

	acted, err = cgenActionPayload(configPath, map[string]any{
		"feed_id":         "CAP-IT-ALL",
		"action":          "clock",
		"enabled":         true,
		"clock_format":    "Jan 02 15:04:05",
		"clock_x":         "96",
		"clock_y":         "112",
		"clock_font_size": "44",
		"font":            "Arial",
		"font_weight":     "regular",
	})
	if err != nil {
		t.Fatalf("clock action: %v", err)
	}
	feeds = acted["feeds"].([]map[string]any)
	if feeds[0]["clock_enabled"] != true ||
		feeds[0]["clock_format"] != "Jan 02 15:04:05" ||
		feeds[0]["clock_x"] != "96" ||
		feeds[0]["clock_y"] != "112" ||
		feeds[0]["clock_font_size"] != "44" ||
		feeds[0]["mode"] != "overlay" {
		t.Fatalf("clock action feed = %#v", feeds[0])
	}

	acted, err = cgenActionPayload(configPath, map[string]any{
		"feed_id": "CAP-IT-ALL",
		"action":  "release",
	})
	if err != nil {
		t.Fatalf("release action: %v", err)
	}
	feeds = acted["feeds"].([]map[string]any)
	if feeds[0]["clock_enabled"] != false ||
		feeds[0]["text_enabled"] != false ||
		feeds[0]["sunny_cat"] != false ||
		feeds[0]["mode"] != "release" {
		t.Fatalf("release action feed = %#v", feeds[0])
	}
}

func TestCgenDeinterlaceEnabledDefaultsAndDisabledRoundTrip(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("version: test\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	offFeed := map[string]any{
		"id": "CGEN_OFF",
		"program_input": map[string]any{
			"type":                  "dummy",
			"deinterlace_enabled":   false,
			"deinterlace_algorithm": "not-a-filter",
			"deinterlace_backend":   "not-a-backend",
		},
	}
	mapped, err := cgenFeedFromMap(offFeed)
	if err != nil {
		t.Fatalf("map cgen feed: %v", err)
	}
	if mapped.ProgramInput.DeinterlaceEnabled != "false" {
		t.Fatalf("mapped disabled deinterlacing = %#v", mapped.ProgramInput)
	}

	saved, err := saveCgenPayload(configPath, map[string]any{
		"enabled": true,
		"feeds": []any{
			offFeed,
			map[string]any{
				"id":            "CGEN_DEFAULT",
				"program_input": map[string]any{"type": "dummy"},
			},
		},
	})
	if err != nil {
		t.Fatalf("save cgen: %v", err)
	}

	feeds := saved["feeds"].([]map[string]any)
	byID := make(map[string]map[string]any, len(feeds))
	for _, feed := range feeds {
		byID[feed["id"].(string)] = feed
	}
	disabled := byID["CGEN_OFF"]
	defaults := byID["CGEN_DEFAULT"]
	if disabled["deinterlace_enabled"] != false {
		t.Fatalf("disabled deinterlacing = %#v", disabled["deinterlace_enabled"])
	}
	input := disabled["program_input"].(map[string]any)
	if input["deinterlace_enabled"] != false {
		t.Fatalf("nested disabled deinterlacing = %#v", input)
	}
	if defaults["deinterlace_enabled"] != true {
		t.Fatalf("default deinterlacing = %#v", defaults["deinterlace_enabled"])
	}

	rawXML, err := os.ReadFile(filepath.Join(dir, defaultCgenFile))
	if err != nil {
		t.Fatalf("read cgen XML: %v", err)
	}
	if !strings.Contains(string(rawXML), `deinterlace_enabled="false"`) ||
		!strings.Contains(string(rawXML), `deinterlace_enabled="true"`) {
		t.Fatalf("written XML missing explicit deinterlace states:\n%s", rawXML)
	}
}

func TestCgenPayloadIncludesRuntimeCompositorStatus(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("version: test\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	statusDir := filepath.Join(dir, "runtime", "cgen")
	if err := os.MkdirAll(statusDir, 0o700); err != nil {
		t.Fatalf("mkdir status: %v", err)
	}
	rawStatus, _ := json.Marshal(map[string]any{
		"visual_mode":          "ticker_alert",
		"overlay_text":         "The National Weather Service has issued a test.",
		"audio_video_drift_ms": 12.5,
		"output_active":        true,
	})
	if err := os.WriteFile(filepath.Join(statusDir, "CAP-IT-ALL.status.json"), rawStatus, 0o600); err != nil {
		t.Fatalf("write status: %v", err)
	}

	loaded, err := saveCgenPayload(configPath, map[string]any{
		"enabled": true,
		"feeds": []any{
			map[string]any{
				"id":                 "CAP-IT-ALL",
				"name":               "CAP CGEN",
				"enabled":            true,
				"program_input_url":  "udp://239.0.0.1:9000",
				"priority_feed_id":   "*",
				"program_output_url": "udp://239.0.0.2:9001?pkt_size=1316",
			},
		},
	})
	if err != nil {
		t.Fatalf("save cgen: %v", err)
	}
	feeds := loaded["feeds"].([]map[string]any)
	runtime := feeds[0]["runtime"].(map[string]any)
	if runtime["visual_mode"] != "ticker_alert" || runtime["output_active"] != true {
		t.Fatalf("runtime status = %#v", runtime)
	}
}

func TestCgenReadRejectsLegacyXML(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("version: test\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cgenPath := filepath.Join(dir, defaultCgenFile)
	if err := os.MkdirAll(filepath.Dir(cgenPath), 0o700); err != nil {
		t.Fatalf("mkdir cgen: %v", err)
	}
	if err := os.WriteFile(cgenPath, []byte(`<?xml version="1.0" encoding="UTF-8"?>
<cgen enabled="true">
  <feed id="CAP-IT-ALL" enabled="true">
    <input></input>
    <programInput url="udp://127.0.0.1:5000" format="mpegts"></programInput>
    <priorityInput feed_id="*" url="udp://127.0.0.1:5002"></priorityInput>
    <programOutput url="udp://127.0.0.1:5001" format="mpegts"></programOutput>
    <video width="1280" height="720"></video>
  </feed>
</cgen>`), 0o600); err != nil {
		t.Fatalf("write cgen: %v", err)
	}

	_, err := loadCgenPayload(configPath)
	if err == nil || !strings.Contains(err.Error(), "legacy cgen <input> element") {
		t.Fatalf("expected legacy XML error, got %v", err)
	}
}

func TestCgenV2RoundTripRevisionAndActionPreserveSections(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("version: test\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	feed := map[string]any{
		"id":      "CGEN_MAIN",
		"name":    "CGEN Main",
		"enabled": true,
		"program_input": map[string]any{
			"type":        "dummy",
			"width":       720,
			"height":      480,
			"fps":         "30000/1001",
			"interlaced":  false,
			"field_order": "tff",
			"background":  "#102030ff",
		},
		"alert": map[string]any{
			"feed_id":      "CAP_MAIN",
			"audio_source": "both",
		},
		"ancillary": map[string]any{
			"captions": "pass",
			"scte35":   "pass",
			"scte104":  "drop",
		},
		"audio_routing": map[string]any{
			"idle_source":           "routine",
			"mute_standby_routine":  false,
			"topology":              "force_layout",
			"force_layout":          "stereo",
			"idle_program_gain_db":  "0",
			"alert_program_gain_db": "muted",
			"alert_gain_db":         "0",
			"transition_ms":         20,
		},
		"compositor": map[string]any{
			"alert_scene_id": "Standard_Crawl",
			"engine":         "scene_v2",
		},
		"program_mapping": map[string]any{
			"mode":                "source",
			"transport_stream_id": 1,
			"programs": []any{
				map[string]any{
					"number":        1,
					"service_name":  "Haze CGEN",
					"provider_name": "Haze",
					"pmt_pid":       "0x1000",
					"video_pid":     "0x0100",
					"audio": []any{
						map[string]any{"track_id": "main", "pid": "0x0101"},
					},
					"scte35": map[string]any{
						"input":                "pass",
						"generated_alert_cues": true,
						"pid":                  "0x0102",
					},
				},
			},
		},
		"outputs": []any{
			map[string]any{
				"id":                     "primary_ts",
				"enabled":                true,
				"destination":            "mpeg_ts_udp",
				"url":                    "${CGEN_PRIMARY_URL}",
				"video_codec":            "h264",
				"rate_control":           "vbr",
				"video_bitrate_kbps":     8000,
				"video_max_bitrate_kbps": 10000,
				"gop_frames":             60,
				"interlaced":             true,
				"field_order":            "bff",
				"audio_codec":            "aac",
				"audio_bitrate_kbps":     192,
				"sample_rate":            48000,
				"encoder": map[string]any{
					"video": map[string]any{"preset": "veryfast"},
				},
			},
		},
	}
	payload := map[string]any{"enabled": true, "feeds": []any{feed}}
	audioRouting := feed["audio_routing"].(map[string]any)
	primaryOutput := feed["outputs"].([]any)[0].(map[string]any)
	audioRouting["topology"] = "preserve_native_tracks"
	primaryOutput["audio_codec"] = "match_input"
	if _, err := saveCgenPayload(configPath, payload); err == nil || !strings.Contains(err.Error(), "preserve-native audio is unavailable") {
		t.Fatalf("expected unavailable audio topology error, got %v", err)
	}
	audioRouting["topology"] = "force_layout"
	primaryOutput["audio_codec"] = "aac"
	saved, err := saveCgenPayload(configPath, payload)
	if err != nil {
		t.Fatalf("save v2 cgen: %v", err)
	}
	revision := stringValue(saved, "revision")
	if len(revision) != 64 || stringValue(saved, "hash") != revision {
		t.Fatalf("revision fields = %#v", saved)
	}
	if saved["schema_version"] != 2 {
		t.Fatalf("schema version = %#v", saved["schema_version"])
	}
	if saved["encoder_schema_version"] != 2 {
		t.Fatalf("encoder schema version = %#v", saved["encoder_schema_version"])
	}
	feeds := saved["feeds"].([]map[string]any)
	got := feeds[0]
	if got["alert_feed_id"] != "CAP_MAIN" || got["audio_source"] != "both" ||
		got["audio_idle"] != "routine" || got["mute_standby_routine"] != false ||
		got["audio_topology"] != "force_layout" || got["alert_scene_id"] != "Standard_Crawl" {
		t.Fatalf("v2 feed fields = %#v", got)
	}
	nestedAlert := got["alert"].(map[string]any)
	nestedAudio := got["audio_routing"].(map[string]any)
	if nestedAlert["audio_source"] != "both" || nestedAudio["idle_source"] != "routine" || nestedAudio["mute_standby_routine"] != false {
		t.Fatalf("nested routing fields = alert %#v audio %#v", nestedAlert, nestedAudio)
	}
	outputs := got["outputs"].([]map[string]any)
	if len(outputs) != 1 || outputs[0]["url"] != "${CGEN_PRIMARY_URL}" ||
		outputs[0]["interlaced"] != true || outputs[0]["field_order"] != "bff" {
		t.Fatalf("outputs = %#v", outputs)
	}
	if _, err := saveCgenPayload(configPath, payload); err == nil || !strings.Contains(err.Error(), "expected_revision is required") {
		t.Fatalf("expected revision requirement, got %v", err)
	}

	rawXML, err := os.ReadFile(filepath.Join(dir, defaultCgenFile))
	if err != nil {
		t.Fatalf("read v2 XML: %v", err)
	}
	for _, want := range []string{
		`schema_version="2"`,
		`<alert feed_id="CAP_MAIN"></alert>`,
		`<ancillary captions="pass" scte35="pass" scte104="drop"></ancillary>`,
		`<compositor alert_scene_id="Standard_Crawl" engine="scene_v2"></compositor>`,
		`<programMapping mode="source" transport_stream_id="1">`,
		`<output id="primary_ts" enabled="true" destination="mpeg_ts_udp" url="${CGEN_PRIMARY_URL}"`,
	} {
		if !strings.Contains(string(rawXML), want) {
			t.Fatalf("v2 XML missing %q:\n%s", want, rawXML)
		}
	}
	rawEncoders, err := os.ReadFile(filepath.Join(dir, defaultCgenEncodersFile))
	if err != nil {
		t.Fatalf("read v2 encoders: %v", err)
	}
	if !strings.Contains(string(rawEncoders), `schema_version="2"`) || !strings.Contains(string(rawEncoders), `<output id="primary_ts"`) {
		t.Fatalf("output encoder profile missing:\n%s", rawEncoders)
	}

	acted, err := cgenActionPayload(configPath, map[string]any{
		"feed_id": "CGEN_MAIN",
		"action":  "insert_text",
		"text":    "Revision test",
	})
	if err != nil {
		t.Fatalf("cgen action: %v", err)
	}
	if stringValue(acted, "revision") == revision {
		t.Fatal("action did not advance the content revision")
	}
	actedFeed := acted["feeds"].([]map[string]any)[0]
	if actedFeed["alert_feed_id"] != "CAP_MAIN" || len(actedFeed["outputs"].([]map[string]any)) != 1 {
		t.Fatalf("action dropped v2 sections: %#v", actedFeed)
	}

	payload["expected_revision"] = revision
	if _, err := saveCgenPayload(configPath, payload); err == nil || !strings.Contains(err.Error(), "revision conflict") {
		t.Fatalf("expected stale revision conflict, got %v", err)
	}
	payload["expected_revision"] = stringValue(acted, "revision")
	feed["name"] = "CGEN Main Updated"
	finalPayload, err := saveCgenPayload(configPath, payload)
	if err != nil {
		t.Fatalf("save with current revision: %v", err)
	}
	finalFeed := finalPayload["feeds"].([]map[string]any)[0]
	if finalFeed["text"] != "Revision test" || finalFeed["mode"] != "overlay" {
		t.Fatalf("canonical save dropped runtime-owned presentation state: %#v", finalFeed)
	}
}

func TestCgenManagedFilesAreBoundedAndEncoderSchemaIsVersioned(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("version: test\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	managed := filepath.Join(dir, "managed", "configs")
	if err := os.MkdirAll(managed, 0o755); err != nil {
		t.Fatalf("create managed directory: %v", err)
	}
	cgenFile := filepath.Join(managed, "cgen.xml")
	if err := os.WriteFile(cgenFile, make([]byte, maxCgenConfigBytes+1), 0o600); err != nil {
		t.Fatalf("write oversized config: %v", err)
	}
	if _, err := loadCgenPayload(configPath); err == nil || !strings.Contains(err.Error(), "safety limit") {
		t.Fatalf("expected bounded config error, got %v", err)
	}

	if err := os.WriteFile(cgenFile, []byte(`<cgen schema_version="2" enabled="true"></cgen>`), 0o600); err != nil {
		t.Fatalf("write cgen config: %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(managed, "cgen-encoders.xml"),
		[]byte(`<cgenEncoders schema_version="99"></cgenEncoders>`),
		0o600,
	); err != nil {
		t.Fatalf("write encoder config: %v", err)
	}
	if _, err := loadCgenPayload(configPath); err == nil || !strings.Contains(err.Error(), "encoder schema_version") {
		t.Fatalf("expected encoder schema error, got %v", err)
	}
}

func TestCgenV2RejectsDuplicateIDsPIDsAndUnsupportedRTMP(t *testing.T) {
	baseFeed := func() map[string]any {
		return map[string]any{
			"id":            "CGEN_MAIN",
			"program_input": map[string]any{"type": "dummy"},
			"audio_routing": map[string]any{"topology": "force_layout"},
		}
	}
	tests := []struct {
		name string
		feed map[string]any
		want string
	}{
		{
			name: "invalid feed id",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["id"] = "../CGEN_MAIN"
				return feed
			}(),
			want: "feed id",
		},
		{
			name: "unsupported deinterlace algorithm",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["program_input"] = map[string]any{
					"type":                  "dummy",
					"deinterlace_algorithm": "weave",
				}
				return feed
			}(),
			want: "deinterlace algorithm",
		},
		{
			name: "invalid deinterlace enabled value",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["program_input"] = map[string]any{
					"type":                "dummy",
					"deinterlace_enabled": "sometimes",
				}
				return feed
			}(),
			want: "deinterlace enabled value",
		},
		{
			name: "hardware parity override",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["program_input"] = map[string]any{
					"type":                  "dummy",
					"deinterlace_algorithm": "motion_adaptive",
					"deinterlace_backend":   "vaapi",
					"deinterlace_parity":    "tff",
				}
				return feed
			}(),
			want: "algorithm, backend, and parity combination",
		},
		{
			name: "duplicate output ids",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["outputs"] = []any{
					map[string]any{"id": "same", "destination": "file", "url": "one.ts", "video_codec": "h264", "audio_codec": "aac"},
					map[string]any{"id": "SAME", "destination": "file", "url": "two.ts", "video_codec": "h264", "audio_codec": "aac"},
				}
				return feed
			}(),
			want: "duplicate output id",
		},
		{
			name: "PID collision",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["program_mapping"] = map[string]any{"programs": []any{
					map[string]any{
						"number": 1, "pmt_pid": 4096, "video_pid": 256,
						"audio": []any{map[string]any{"track_id": "main", "pid": 256}},
					},
				}}
				return feed
			}(),
			want: "collides",
		},
		{
			name: "unsupported RTMP codecs",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["outputs"] = []any{
					map[string]any{"id": "rtmp", "destination": "rtmp", "url": "rtmp://example/live", "video_codec": "h265", "audio_codec": "ac3"},
				}
				return feed
			}(),
			want: "RTMP requires",
		},
		{
			name: "unsupported MP4 codecs",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["outputs"] = []any{
					map[string]any{"id": "archive", "destination": "file", "url": "archive.mp4", "container": "mp4", "video_codec": "mpeg2", "audio_codec": "ac3"},
				}
				return feed
			}(),
			want: "MP4 and MOV require",
		},
		{
			name: "unsupported file container",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["outputs"] = []any{
					map[string]any{"id": "archive", "destination": "file", "url": "archive.bin", "container": "raw", "video_codec": "h264", "audio_codec": "aac"},
				}
				return feed
			}(),
			want: "file container",
		},
		{
			name: "unsupported destination",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["outputs"] = []any{
					map[string]any{"id": "http", "destination": "http", "url": "https://example.test/live", "video_codec": "h264", "audio_codec": "aac"},
				}
				return feed
			}(),
			want: "destination is unsupported",
		},
		{
			name: "invalid output location",
			feed: func() map[string]any {
				feed := baseFeed()
				feed["outputs"] = []any{
					map[string]any{"id": "udp", "destination": "mpeg_ts_udp", "url": "udp://239.0.0.1:9000\ninvalid", "video_codec": "h264", "audio_codec": "aac"},
				}
				return feed
			}(),
			want: "url is invalid",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(configPath, []byte("version: test\n"), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}
			_, err := saveCgenPayload(configPath, map[string]any{"feeds": []any{test.feed}})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %q, got %v", test.want, err)
			}
		})
	}
}
