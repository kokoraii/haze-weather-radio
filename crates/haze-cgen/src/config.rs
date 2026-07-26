use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::num::{NonZeroU16, NonZeroU32};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use serde::Deserialize;
use serde_json::Value;

use crate::architecture::{
    AncillaryPolicy, AudioCodec, AudioCodecPolicy, AudioEncoderSpec, AudioRoutingSpec,
    AudioStreamMap, AudioTopologyMode, AudioTrackId, ChannelLayout, CompositorSpec,
    DecoderPreference, DeinterlaceAlgorithm, DeinterlaceBackend, DeinterlaceCadence,
    DeinterlaceParity, DeinterlaceSpec, DemuxHint, DeviceBackend, DeviceInput, DummyInput,
    EncoderOutputSpec, FeedId, FieldOrder, GainDb, MpegTsPid, MpegTsProgramSpec, OutputDestination,
    OutputId, PassPolicy, PidAssignment, PipelineSpec, ProgramInput, ProgramMapSpec, RateControl,
    Rational, Rgba8, ScanMode, SceneId, Scte35Map, ServiceMetadata, UriInput, VideoCodec,
    VideoEncoderSpec, VideoFormat,
};
use crate::input_program_map::ProgramMappingMode;

const MAX_CGEN_CONFIG_BYTES: u64 = 4 * 1024 * 1024;
const MAX_CGEN_ENCODERS_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename = "cgen")]
pub(crate) struct CgenConfig {
    #[serde(rename = "@schema_version", default = "default_schema_version")]
    pub(crate) schema_version: u16,
    #[serde(rename = "@enabled", default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(rename = "feed", default)]
    pub(crate) feeds: Vec<FeedConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct FeedConfig {
    #[serde(rename = "@id")]
    pub(crate) id: String,
    #[serde(rename = "@name", default)]
    pub(crate) name: String,
    #[serde(rename = "@enabled", default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(rename = "programInput", default)]
    pub(crate) program_input: EndpointConfig,
    #[serde(rename = "priorityInput", default)]
    pub(crate) priority_input: PriorityInputConfig,
    #[serde(rename = "programOutput", default)]
    pub(crate) program_output: EndpointConfig,
    #[serde(rename = "program", default)]
    pub(crate) program: Vec<ProgramSectionConfig>,
    #[serde(default)]
    pub(crate) priority: PrioritySectionConfig,
    #[serde(default)]
    pub(crate) media: MediaSectionConfig,
    #[serde(default)]
    pub(crate) presentation: PresentationSectionConfig,
    #[serde(default)]
    pub(crate) video: VideoConfig,
    #[serde(default)]
    pub(crate) audio: AudioConfig,
    #[serde(default)]
    pub(crate) ladder: LadderConfig,
    #[serde(default)]
    pub(crate) banner: BannerConfig,
    #[serde(default)]
    pub(crate) graphics: GraphicsConfig,
    #[serde(default)]
    pub(crate) clock: ClockConfig,
    #[serde(default)]
    pub(crate) text: TextConfig,
    #[serde(default)]
    pub(crate) state: StateConfig,
    #[serde(default)]
    pub(crate) standby: StandbyConfig,
    #[serde(default)]
    pub(crate) sync: SyncConfig,
    #[serde(default)]
    pub(crate) alert: AlertRouteConfig,
    #[serde(default)]
    pub(crate) ancillary: AncillaryConfig,
    #[serde(default)]
    pub(crate) compositor: CompositorConfig,
    #[serde(rename = "programMapping", default)]
    pub(crate) program_mapping: ProgramMappingConfig,
    #[serde(default)]
    pub(crate) outputs: OutputsConfig,
    #[serde(skip)]
    pub(crate) encoder: FeedEncoderSettings,
    #[serde(skip)]
    pub(crate) output_encoders: BTreeMap<String, FeedEncoderSettings>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct EndpointConfig {
    #[serde(rename = "@url", default)]
    pub(crate) url: String,
    #[serde(rename = "@type", default)]
    pub(crate) input_type: String,
    #[serde(rename = "@format", default)]
    pub(crate) format: String,
    #[serde(rename = "@vcodec", default)]
    pub(crate) vcodec: String,
    #[serde(rename = "@acodec", default)]
    pub(crate) acodec: String,
    #[serde(rename = "@video_bitrate_kbps", default)]
    pub(crate) video_bitrate_kbps: Option<u32>,
    #[serde(rename = "@audio_bitrate_kbps", default)]
    pub(crate) audio_bitrate_kbps: Option<u32>,
    #[serde(rename = "@hardware_decoder_enabled", default)]
    pub(crate) hardware_decoder_enabled: String,
    #[serde(rename = "@hardware_decoder", default)]
    pub(crate) hardware_decoder: String,
    #[serde(rename = "@deinterlace_algorithm", default)]
    pub(crate) deinterlace_algorithm: String,
    #[serde(rename = "@deinterlace_backend", default)]
    pub(crate) deinterlace_backend: String,
    #[serde(rename = "@deinterlace_cadence", default)]
    pub(crate) deinterlace_cadence: String,
    #[serde(rename = "@deinterlace_parity", default)]
    pub(crate) deinterlace_parity: String,
    #[serde(rename = "@device_backend", default)]
    pub(crate) device_backend: String,
    #[serde(rename = "@device_id", default)]
    pub(crate) device_id: String,
    #[serde(rename = "@width", default)]
    pub(crate) width: u32,
    #[serde(rename = "@height", default)]
    pub(crate) height: u32,
    #[serde(rename = "@fps", default)]
    pub(crate) fps: String,
    #[serde(rename = "@interlaced", default)]
    pub(crate) interlaced: bool,
    #[serde(rename = "@field_order", default)]
    pub(crate) field_order: String,
    #[serde(rename = "@background", default)]
    pub(crate) background: String,
    #[serde(rename = "@service_name", default)]
    pub(crate) service_name: String,
    #[serde(rename = "@provider_name", default)]
    pub(crate) provider_name: String,
    #[serde(rename = "@service_id", default)]
    pub(crate) service_id: u16,
    #[serde(rename = "@transport_stream_id", default)]
    pub(crate) transport_stream_id: u16,
}

impl EndpointConfig {
    fn has_values(&self) -> bool {
        !self.url.trim().is_empty()
            || !self.input_type.trim().is_empty()
            || !self.format.trim().is_empty()
            || !self.vcodec.trim().is_empty()
            || !self.acodec.trim().is_empty()
            || self.video_bitrate_kbps.is_some()
            || self.audio_bitrate_kbps.is_some()
            || !self.hardware_decoder_enabled.trim().is_empty()
            || !self.hardware_decoder.trim().is_empty()
            || !self.deinterlace_algorithm.trim().is_empty()
            || !self.deinterlace_backend.trim().is_empty()
            || !self.deinterlace_cadence.trim().is_empty()
            || !self.deinterlace_parity.trim().is_empty()
            || !self.device_backend.trim().is_empty()
            || !self.device_id.trim().is_empty()
            || self.width > 0
            || self.height > 0
            || !self.fps.trim().is_empty()
            || self.interlaced
            || !self.field_order.trim().is_empty()
            || !self.background.trim().is_empty()
            || !self.service_name.trim().is_empty()
            || !self.provider_name.trim().is_empty()
            || self.service_id > 0
            || self.transport_stream_id > 0
    }

    pub(crate) fn hardware_decoder(&self) -> Option<&str> {
        if !xml_bool_text(&self.hardware_decoder_enabled, false) {
            return None;
        }
        let decoder = self.hardware_decoder.trim();
        if decoder.is_empty() || !decoder.chars().all(gst_element_char) {
            return None;
        }
        Some(decoder)
    }
}

fn gst_element_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct PriorityInputConfig {
    #[serde(rename = "@feed_id", default)]
    pub(crate) feed_id: String,
    #[serde(rename = "@audio_source", default = "priority_audio_source")]
    pub(crate) audio_source: String,
    #[serde(rename = "@format", default)]
    pub(crate) format: String,
}

impl PriorityInputConfig {
    pub(crate) fn priority_audio_enabled(&self) -> bool {
        !matches!(
            self.audio_source.trim().to_ascii_lowercase().as_str(),
            "routine" | "feed" | "program_feed"
        )
    }

    pub(crate) fn routine_audio_enabled(&self) -> bool {
        matches!(
            self.audio_source.trim().to_ascii_lowercase().as_str(),
            "routine" | "feed" | "program_feed" | "both" | "all"
        )
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct VideoConfig {
    #[serde(rename = "@width", default)]
    pub(crate) width: u32,
    #[serde(rename = "@height", default)]
    pub(crate) height: u32,
    #[serde(rename = "@fps", default)]
    pub(crate) fps: String,
    #[serde(rename = "@interlaced", default)]
    pub(crate) interlaced: bool,
    #[serde(rename = "@field_order", default = "default_field_order")]
    pub(crate) field_order: String,
    #[serde(rename = "@standard", default)]
    pub(crate) standard: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AudioConfig {
    #[serde(rename = "@idle", default = "source_audio")]
    pub(crate) idle: String,
    #[serde(rename = "@alert_mode", default = "replace_audio")]
    pub(crate) alert_mode: String,
    #[serde(rename = "@mute_standby_routine", default = "default_true")]
    pub(crate) mute_standby_routine: bool,
    #[serde(rename = "@topology", default = "default_audio_topology")]
    pub(crate) topology: String,
    #[serde(rename = "@force_layout", default = "default_forced_audio_layout")]
    pub(crate) force_layout: String,
    #[serde(
        rename = "@idle_program_gain_db",
        default = "default_idle_program_gain"
    )]
    pub(crate) idle_program_gain_db: String,
    #[serde(
        rename = "@alert_program_gain_db",
        default = "default_alert_program_gain"
    )]
    pub(crate) alert_program_gain_db: String,
    #[serde(rename = "@alert_gain_db", default = "default_alert_gain")]
    pub(crate) alert_gain_db: String,
    #[serde(rename = "@transition_ms", default = "default_audio_transition_ms")]
    pub(crate) transition_ms: u16,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct AlertRouteConfig {
    #[serde(rename = "@feed_id", default)]
    pub(crate) feed_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct AncillaryConfig {
    #[serde(rename = "@captions", default)]
    pub(crate) captions: String,
    #[serde(rename = "@scte35", default)]
    pub(crate) scte35: String,
    #[serde(rename = "@scte104", default)]
    pub(crate) scte104: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CompositorConfig {
    #[serde(rename = "@alert_scene_id", default = "default_alert_scene_id")]
    pub(crate) alert_scene_id: String,
    #[serde(rename = "@engine", default = "default_compositor_engine")]
    pub(crate) engine: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ProgramMappingConfig {
    #[serde(rename = "@mode", default)]
    pub(crate) mode: String,
    #[serde(rename = "@transport_stream_id", default)]
    pub(crate) transport_stream_id: u16,
    #[serde(rename = "program", default)]
    pub(crate) programs: Vec<ProgramMapEntryConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ProgramMapEntryConfig {
    #[serde(rename = "@number", default)]
    pub(crate) number: u16,
    #[serde(rename = "@service_name", default)]
    pub(crate) service_name: String,
    #[serde(rename = "@provider_name", default)]
    pub(crate) provider_name: String,
    #[serde(rename = "@pmt_pid", default)]
    pub(crate) pmt_pid: String,
    #[serde(rename = "@video_pid", default)]
    pub(crate) video_pid: String,
    #[serde(rename = "audio", default)]
    pub(crate) audio: Vec<ProgramAudioMapConfig>,
    #[serde(default)]
    pub(crate) scte35: Option<ProgramScte35Config>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ProgramAudioMapConfig {
    #[serde(rename = "@track_id", default)]
    pub(crate) track_id: String,
    #[serde(rename = "@pid", default)]
    pub(crate) pid: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ProgramScte35Config {
    #[serde(rename = "@input", default)]
    pub(crate) input: String,
    #[serde(rename = "@generated_alert_cues", default)]
    pub(crate) generated_alert_cues: bool,
    #[serde(rename = "@pid", default)]
    pub(crate) pid: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct OutputsConfig {
    #[serde(rename = "output", default)]
    pub(crate) outputs: Vec<OutputConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct OutputConfig {
    #[serde(rename = "@id", default)]
    pub(crate) id: String,
    #[serde(rename = "@enabled", default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(rename = "@destination", default)]
    pub(crate) destination: String,
    #[serde(rename = "@url", default)]
    pub(crate) url: String,
    #[serde(rename = "@video_url", default)]
    pub(crate) video_url: String,
    #[serde(rename = "@audio_urls", default)]
    pub(crate) audio_urls: String,
    #[serde(rename = "@container", default)]
    pub(crate) container: String,
    #[serde(rename = "@latency_ms", default)]
    pub(crate) latency_ms: u32,
    #[serde(rename = "@video_codec", default)]
    pub(crate) video_codec: String,
    #[serde(rename = "@rate_control", default)]
    pub(crate) rate_control: String,
    #[serde(rename = "@video_bitrate_kbps", default)]
    pub(crate) video_bitrate_kbps: u32,
    #[serde(rename = "@video_max_bitrate_kbps", default)]
    pub(crate) video_max_bitrate_kbps: u32,
    #[serde(rename = "@gop_frames", default)]
    pub(crate) gop_frames: u32,
    #[serde(rename = "@audio_codec", default)]
    pub(crate) audio_codec: String,
    #[serde(rename = "@audio_bitrate_kbps", default)]
    pub(crate) audio_bitrate_kbps: u32,
    #[serde(rename = "@sample_rate", default)]
    pub(crate) sample_rate: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FeedEncoderSettings {
    pub(crate) video: EncoderCodecSettings,
    pub(crate) audio: EncoderCodecSettings,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct EncoderCodecSettings {
    pub(crate) codec: String,
    pub(crate) bitrate_kbps: Option<u32>,
    pub(crate) gop: Option<u32>,
    pub(crate) bframes: Option<u32>,
    pub(crate) preset: String,
    pub(crate) tune: String,
    pub(crate) profile: String,
    pub(crate) level: String,
    pub(crate) options: Vec<EncoderOptionConfig>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub(crate) struct EncoderOptionConfig {
    #[serde(rename = "@name", default)]
    pub(crate) name: String,
    #[serde(rename = "@value", default)]
    pub(crate) value: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename = "cgenEncoders")]
struct CgenEncodersConfig {
    #[serde(rename = "@schema_version", default = "default_schema_version")]
    schema_version: u16,
    #[serde(rename = "feed", default)]
    feeds: Vec<FeedEncoderConfig>,
    #[serde(rename = "output", default)]
    outputs: Vec<OutputEncoderConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FeedEncoderConfig {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(default)]
    video: EncoderCodecConfig,
    #[serde(default)]
    audio: EncoderCodecConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct OutputEncoderConfig {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(default)]
    video: EncoderCodecConfig,
    #[serde(default)]
    audio: EncoderCodecConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct EncoderCodecConfig {
    #[serde(rename = "@codec", default)]
    codec: String,
    #[serde(rename = "@bitrate_kbps", default)]
    bitrate_kbps: Option<u32>,
    #[serde(rename = "@gop", default)]
    gop: Option<u32>,
    #[serde(rename = "@bframes", default)]
    bframes: Option<u32>,
    #[serde(rename = "@preset", default)]
    preset: String,
    #[serde(rename = "@tune", default)]
    tune: String,
    #[serde(rename = "@profile", default)]
    profile: String,
    #[serde(rename = "@level", default)]
    level: String,
    #[serde(rename = "option", default)]
    options: Vec<EncoderOptionConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct LadderConfig {
    #[serde(rename = "video", default)]
    pub(crate) videos: Vec<VideoRenditionConfig>,
    #[serde(rename = "audio", default)]
    pub(crate) audios: Vec<AudioRenditionConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct VideoRenditionConfig {
    #[serde(rename = "@id")]
    pub(crate) id: String,
    #[serde(rename = "@enabled", default = "default_true_text")]
    pub(crate) enabled: String,
    #[serde(rename = "@width")]
    pub(crate) width: u32,
    #[serde(rename = "@height")]
    pub(crate) height: u32,
    #[serde(rename = "@fps", default)]
    pub(crate) fps: String,
    #[serde(rename = "@interlaced", default)]
    pub(crate) interlaced: bool,
    #[serde(rename = "@field_order", default = "default_field_order")]
    pub(crate) field_order: String,
    #[serde(rename = "@standard", default)]
    pub(crate) standard: String,
    #[serde(rename = "@vcodec", default)]
    pub(crate) vcodec: String,
    #[serde(rename = "@bitrate_kbps", default)]
    pub(crate) bitrate_kbps: Option<u32>,
    #[serde(rename = "@program", default)]
    pub(crate) program: Option<i32>,
    #[serde(rename = "@video_pid", default)]
    pub(crate) video_pid: Option<i32>,
    #[serde(rename = "@pmt_pid", default)]
    pub(crate) pmt_pid: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AudioRenditionConfig {
    #[serde(rename = "@id")]
    pub(crate) id: String,
    #[serde(rename = "@enabled", default = "default_true_text")]
    pub(crate) enabled: String,
    #[serde(rename = "@channels", default = "default_stereo_channels")]
    pub(crate) channels: u16,
    #[serde(rename = "@acodec", default)]
    pub(crate) acodec: String,
    #[serde(rename = "@bitrate_kbps", default)]
    pub(crate) bitrate_kbps: Option<u32>,
    #[serde(rename = "@language", default = "default_audio_language")]
    pub(crate) language: String,
    #[serde(rename = "@program", default)]
    pub(crate) program: Option<i32>,
    #[serde(rename = "@audio_pid", default)]
    pub(crate) audio_pid: Option<i32>,
    #[serde(rename = "@pmt_pid", default)]
    pub(crate) pmt_pid: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BannerConfig {
    #[serde(rename = "@mode", default = "auto_mode")]
    pub(crate) mode: String,
    #[serde(rename = "@ticker_height", default = "default_ticker_height")]
    pub(crate) ticker_height: u32,
    #[serde(rename = "@font", default)]
    pub(crate) font: String,
    #[serde(rename = "@font_weight", default = "regular_font_weight")]
    pub(crate) font_weight: String,
    #[serde(rename = "@font_size", default = "default_font_size")]
    pub(crate) font_size: u32,
    #[serde(rename = "@scroll_speed", default = "default_scroll_speed")]
    pub(crate) scroll_speed: u32,
    #[serde(rename = "@scroll_repeat_mode", default = "default_scroll_repeat_mode")]
    pub(crate) scroll_repeat_mode: String,
    #[serde(rename = "@after_eom_repeats", default)]
    pub(crate) after_eom_repeats: u32,
    #[serde(rename = "@fixed_repeats", default = "default_fixed_repeats")]
    pub(crate) fixed_repeats: u32,
    #[serde(rename = "@background_enabled", default = "default_true")]
    pub(crate) background_enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct GraphicsConfig {
    #[serde(rename = "@font", default)]
    pub(crate) font: String,
    #[serde(rename = "@font_weight", default = "regular_font_weight")]
    pub(crate) font_weight: String,
    #[serde(rename = "@font_size", default = "default_font_size")]
    pub(crate) font_size: u32,
    #[serde(rename = "@banner_height", default = "default_ticker_height")]
    pub(crate) banner_height: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ClockConfig {
    #[serde(rename = "@enabled", default)]
    pub(crate) enabled: bool,
    #[serde(rename = "@format", default)]
    pub(crate) format: String,
    #[serde(rename = "@x", default = "default_text_x")]
    pub(crate) x: i32,
    #[serde(rename = "@y", default = "default_clock_y")]
    pub(crate) y: i32,
    #[serde(rename = "@font_size", default = "default_clock_font_size")]
    pub(crate) font_size: u32,
    #[serde(rename = "@color", default)]
    pub(crate) color: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TextConfig {
    #[serde(rename = "@enabled", default)]
    pub(crate) enabled: bool,
    #[serde(rename = "@x", default = "default_text_x")]
    pub(crate) x: i32,
    #[serde(rename = "@y", default = "default_text_y")]
    pub(crate) y: i32,
    #[serde(rename = "@font_size", default = "default_insert_font_size")]
    pub(crate) font_size: u32,
    #[serde(rename = "@color", default)]
    pub(crate) color: String,
    #[serde(rename = "$text", default)]
    pub(crate) content: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StateConfig {
    #[serde(rename = "@mode", default = "release_mode")]
    pub(crate) mode: String,
    #[serde(rename = "@smpte_bars", default)]
    pub(crate) smpte_bars: bool,
    #[serde(rename = "@sunny_cat", default)]
    pub(crate) sunny_cat: bool,
    #[serde(rename = "@updated_at", default)]
    pub(crate) updated_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StandbyConfig {
    #[serde(rename = "@mode", default = "standby_banner_mode")]
    pub(crate) mode: String,
    #[serde(rename = "@text", default = "default_standby_text")]
    pub(crate) text: String,
    #[serde(rename = "@font_size", default)]
    pub(crate) font_size: u32,
    #[serde(rename = "@y_percent", default = "default_standby_y_percent")]
    pub(crate) y_percent: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct SyncConfig {
    #[serde(rename = "@hard_reset_ms", default = "default_hard_reset_ms")]
    pub(crate) hard_reset_ms: u32,
    #[serde(
        rename = "@max_audio_frames_per_video",
        default = "default_max_audio_frames_per_video"
    )]
    pub(crate) max_audio_frames_per_video: u32,
    #[serde(rename = "@source_buffer_ms", default = "default_source_buffer_ms")]
    pub(crate) source_buffer_ms: u32,
    #[serde(
        rename = "@reconnect_initial_ms",
        default = "default_reconnect_initial_ms"
    )]
    pub(crate) reconnect_initial_ms: u32,
    #[serde(rename = "@reconnect_max_ms", default = "default_reconnect_max_ms")]
    pub(crate) reconnect_max_ms: u32,
    #[serde(rename = "@status_interval_ms", default = "default_status_interval_ms")]
    pub(crate) status_interval_ms: u32,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ProgramSectionConfig {
    #[serde(default)]
    input: EndpointConfig,
    #[serde(default)]
    output: EndpointConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct PrioritySectionConfig {
    #[serde(default)]
    input: PriorityInputConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct MediaSectionConfig {
    #[serde(default)]
    video: Option<VideoConfig>,
    #[serde(default)]
    audio: Option<AudioConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct PresentationSectionConfig {
    #[serde(default)]
    banner: Option<PresentationBannerSectionConfig>,
    #[serde(default)]
    graphics: Option<PresentationGraphicsSectionConfig>,
    #[serde(default)]
    clock: Option<ClockConfig>,
    #[serde(default)]
    text: Option<TextConfig>,
    #[serde(default)]
    standby: Option<StandbyConfig>,
    #[serde(default)]
    state: Option<StateConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PresentationBannerSectionConfig {
    #[serde(rename = "@mode", default)]
    mode: String,
    #[serde(rename = "@background_enabled", default)]
    background_enabled: Option<bool>,
    #[serde(default)]
    font: FontSectionConfig,
    #[serde(default)]
    ticker: TickerSectionConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PresentationGraphicsSectionConfig {
    #[serde(default)]
    font: FontSectionConfig,
    #[serde(rename = "@banner_height", default)]
    banner_height: u32,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FontSectionConfig {
    #[serde(rename = "@family", default)]
    family: String,
    #[serde(rename = "@weight", default)]
    weight: String,
    #[serde(rename = "@size", default)]
    size: u32,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct TickerSectionConfig {
    #[serde(rename = "@height", default)]
    height: u32,
    #[serde(rename = "@speed", default)]
    speed: u32,
    #[serde(default)]
    repeat: TickerRepeatSectionConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct TickerRepeatSectionConfig {
    #[serde(rename = "@mode", default)]
    mode_name: String,
    #[serde(rename = "@after_eom", default)]
    after_eom: u32,
    #[serde(rename = "@count", default)]
    count: u32,
}

impl CgenConfig {
    fn apply_nested_sections(&mut self) {
        for feed in &mut self.feeds {
            feed.apply_nested_sections();
        }
    }

    fn apply_encoder_settings(&mut self, encoders: CgenEncodersConfig) -> Result<()> {
        if !matches!(encoders.schema_version, 1 | 2) {
            bail!(
                "unsupported cgen encoder schema version {}, expected 1 or 2",
                encoders.schema_version
            );
        }
        for encoder_feed in encoders.feeds {
            if encoder_feed.id.trim().is_empty() {
                continue;
            }
            if let Some(feed) = self
                .feeds
                .iter_mut()
                .find(|feed| feed.id.eq_ignore_ascii_case(encoder_feed.id.trim()))
            {
                feed.encoder = FeedEncoderSettings::from_config(encoder_feed);
            }
        }
        let mut output_ids = BTreeSet::new();
        for encoder_output in encoders.outputs {
            let output_id = encoder_output.id.trim();
            if output_id.is_empty() {
                bail!("cgen encoder output id is required");
            }
            let parsed_id = OutputId::parse(output_id)?;
            if !output_ids.insert(parsed_id.as_str().to_ascii_lowercase()) {
                bail!("duplicate cgen encoder output id {}", parsed_id);
            }
            let settings = FeedEncoderSettings::from_output_config(encoder_output);
            for feed in &mut self.feeds {
                if feed
                    .outputs
                    .outputs
                    .iter()
                    .any(|output| output.id.eq_ignore_ascii_case(parsed_id.as_str()))
                {
                    feed.output_encoders
                        .insert(parsed_id.as_str().to_ascii_lowercase(), settings.clone());
                }
            }
        }
        Ok(())
    }

    pub(crate) fn enabled_feeds(&self) -> Result<Vec<FeedConfig>> {
        if !matches!(self.schema_version, 1 | 2) {
            bail!(
                "unsupported cgen schema version {}, expected 1 or 2",
                self.schema_version
            );
        }
        if !self.enabled {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut feed_ids = BTreeSet::new();
        for feed in &self.feeds {
            if !feed.enabled {
                continue;
            }
            if !feed_ids.insert(feed.id.trim().to_ascii_lowercase()) {
                bail!("duplicate enabled cgen feed id {}", feed.id);
            }
            feed.validate()?;
            out.push(feed.clone());
        }
        Ok(out)
    }
}

impl FeedEncoderSettings {
    fn from_config(config: FeedEncoderConfig) -> Self {
        Self {
            video: EncoderCodecSettings::from_config(config.video),
            audio: EncoderCodecSettings::from_config(config.audio),
        }
    }

    fn from_output_config(config: OutputEncoderConfig) -> Self {
        Self {
            video: EncoderCodecSettings::from_config(config.video),
            audio: EncoderCodecSettings::from_config(config.audio),
        }
    }
}

impl EncoderCodecSettings {
    fn from_config(config: EncoderCodecConfig) -> Self {
        Self {
            codec: config.codec.trim().to_string(),
            bitrate_kbps: config.bitrate_kbps.filter(|value| *value > 0),
            gop: config.gop.filter(|value| *value > 0),
            bframes: config.bframes,
            preset: config.preset.trim().to_string(),
            tune: config.tune.trim().to_string(),
            profile: config.profile.trim().to_string(),
            level: config.level.trim().to_string(),
            options: config
                .options
                .into_iter()
                .filter(|option| !option.name.trim().is_empty())
                .map(|option| EncoderOptionConfig {
                    name: option.name.trim().to_string(),
                    value: option.value.trim().to_string(),
                })
                .collect(),
        }
    }

    pub(crate) fn applies_to(&self, codec: &str) -> bool {
        self.codec.trim().is_empty() || self.codec.trim().eq_ignore_ascii_case(codec.trim())
    }

    pub(crate) fn bitrate_or(&self, fallback: Option<u32>) -> Option<u32> {
        self.bitrate_kbps.or(fallback)
    }
}

impl FeedConfig {
    pub(crate) fn encoder_settings_for_output(
        &self,
        output_id: &str,
    ) -> Option<&FeedEncoderSettings> {
        self.output_encoders
            .get(&output_id.trim().to_ascii_lowercase())
    }

    fn apply_nested_sections(&mut self) {
        for program in &self.program {
            if program.input.has_values() {
                self.program_input = program.input.clone();
            }
            if program.output.has_values() {
                self.program_output = program.output.clone();
            }
        }
        if !self.priority.input.feed_id.trim().is_empty()
            || !self.priority.input.audio_source.trim().is_empty()
            || !self.priority.input.format.trim().is_empty()
        {
            self.priority_input = self.priority.input.clone();
        }
        if let Some(video) = self.media.video.as_ref() {
            self.video = video.clone();
        }
        if let Some(audio) = self.media.audio.as_ref() {
            self.audio = audio.clone();
        }
        self.apply_presentation_section();
    }

    fn apply_presentation_section(&mut self) {
        if let Some(banner) = self.presentation.banner.as_ref() {
            if !banner.mode.trim().is_empty() {
                self.banner.mode = banner.mode.clone();
            }
            if let Some(background_enabled) = banner.background_enabled {
                self.banner.background_enabled = background_enabled;
            }
            if banner.ticker.height > 0 {
                self.banner.ticker_height = banner.ticker.height;
            }
            if banner.ticker.speed > 0 {
                self.banner.scroll_speed = banner.ticker.speed;
            }
            if !banner.ticker.repeat.mode_name.trim().is_empty() {
                self.banner.scroll_repeat_mode = banner.ticker.repeat.mode_name.clone();
            }
            if banner.ticker.repeat.after_eom > 0 {
                self.banner.after_eom_repeats = banner.ticker.repeat.after_eom;
            }
            if banner.ticker.repeat.count > 0 {
                self.banner.fixed_repeats = banner.ticker.repeat.count;
            }
            if !banner.font.family.trim().is_empty() {
                self.banner.font = banner.font.family.clone();
            }
            if !banner.font.weight.trim().is_empty() {
                self.banner.font_weight = banner.font.weight.clone();
            }
            if banner.font.size > 0 {
                self.banner.font_size = banner.font.size;
            }
        }

        if let Some(graphics) = self.presentation.graphics.as_ref() {
            if !graphics.font.family.trim().is_empty() {
                self.graphics.font = graphics.font.family.clone();
            }
            if !graphics.font.weight.trim().is_empty() {
                self.graphics.font_weight = graphics.font.weight.clone();
            }
            if graphics.font.size > 0 {
                self.graphics.font_size = graphics.font.size;
            }
            if graphics.banner_height > 0 {
                self.graphics.banner_height = graphics.banner_height;
            }
        }
        if let Some(clock) = self.presentation.clock.as_ref() {
            self.clock = clock.clone();
        }
        if let Some(text) = self.presentation.text.as_ref() {
            self.text = text.clone();
        }
        if let Some(standby) = self.presentation.standby.as_ref() {
            self.standby = standby.clone();
        }
        if let Some(state) = self.presentation.state.as_ref() {
            self.state = state.clone();
        }
    }

    #[allow(dead_code)]
    pub(crate) fn matches_feed(&self, feed_id: &str) -> bool {
        self.id.trim() == "*" || self.id.trim() == feed_id.trim()
    }

    fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() {
            bail!("cgen feed id is required");
        }
        let input_type = self.program_input.input_type.trim();
        let input_requires_url = !matches!(
            input_type.to_ascii_lowercase().as_str(),
            "dummy" | "none" | "no_input" | "device" | "v4l2" | "directshow" | "dshow"
        );
        if input_requires_url && self.program_input_url().trim().is_empty() {
            bail!("cgen feed {} input url is required", self.id);
        }
        if self.program_output_url().trim().is_empty() && self.outputs.outputs.is_empty() {
            bail!("cgen feed {} output url is required", self.id);
        }
        let (source_width, source_height) = self.configured_canvas_size();
        if !allowed_video_size(source_width, source_height) {
            bail!(
                "cgen feed {} video size {}x{} is not supported",
                self.id,
                source_width,
                source_height
            );
        }
        let enabled_videos = self.enabled_video_renditions(source_width, source_height);
        if enabled_videos.is_empty() {
            bail!("cgen feed {} has no enabled video renditions", self.id);
        }
        for video in enabled_videos {
            if !allowed_video_size(video.width, video.height) {
                bail!(
                    "cgen feed {} video rendition {} size {}x{} is not supported",
                    self.id,
                    video.id,
                    video.width,
                    video.height
                );
            }
        }
        if self.enabled_audio_renditions().is_empty() {
            bail!("cgen feed {} has no enabled audio renditions", self.id);
        }
        if !matches!(
            self.audio.alert_mode.trim().to_ascii_lowercase().as_str(),
            "replace" | "mix" | "duck"
        ) {
            bail!(
                "cgen feed {} audio alert_mode must be replace, mix, or duck",
                self.id
            );
        }
        if !matches!(
            self.compositor.engine.trim().to_ascii_lowercase().as_str(),
            "" | "legacy" | "scene_v2" | "scene" | "wgpu"
        ) {
            bail!("cgen feed {} compositor engine is unsupported", self.id);
        }
        self.pipeline_spec()?.validate().map_err(|error| {
            anyhow::anyhow!(
                "cgen feed {} pipeline configuration is invalid: {error}",
                self.id
            )
        })?;
        Ok(())
    }

    pub(crate) fn configured_canvas_size(&self) -> (u32, u32) {
        let width = self.program_input.width.max(self.video.width).max(720);
        let height = self.program_input.height.max(self.video.height).max(480);
        (width, height)
    }

    pub(crate) fn pipeline_spec(&self) -> Result<PipelineSpec> {
        let feed_id = FeedId::parse(&self.id)?;
        let alert_feed_raw = non_empty(self.alert.feed_id.as_str())
            .or_else(|| non_empty(self.priority_input.feed_id.as_str()))
            .filter(|feed_id| *feed_id != "*")
            .unwrap_or(self.id.as_str());
        let alert_feed_id = FeedId::parse(alert_feed_raw)?;
        let outputs = self.output_specs()?;
        let has_transport_stream = outputs
            .iter()
            .any(|output| output.enabled && output.destination.is_mpeg_ts());
        let program_map = if has_transport_stream {
            Some(self.program_map_spec()?)
        } else {
            None
        };
        let spec = PipelineSpec {
            feed_id,
            input: self.program_input_spec()?,
            alert_feed_id,
            ancillary: AncillaryPolicy {
                captions: pass_policy(&self.ancillary.captions)?,
                scte35: pass_policy(&self.ancillary.scte35)?,
                scte104: pass_policy(&self.ancillary.scte104)?,
            },
            audio: self.audio_routing_spec()?,
            compositor: CompositorSpec {
                alert_scene_id: SceneId::new(&self.compositor.alert_scene_id)?,
            },
            program_map,
            outputs,
        };
        Ok(spec)
    }

    pub(crate) fn deinterlace_spec(&self) -> Result<DeinterlaceSpec> {
        deinterlace_spec(&self.program_input)
    }

    fn program_input_spec(&self) -> Result<ProgramInput> {
        let input_type = self.program_input.input_type.trim().to_ascii_lowercase();
        let decoder = decoder_preference(&self.program_input)?;
        let deinterlace = deinterlace_spec(&self.program_input)?;
        match input_type.as_str() {
            "device" | "v4l2" | "directshow" | "dshow" => {
                let backend = match input_type.as_str() {
                    "v4l2" => DeviceBackend::V4l2,
                    "directshow" | "dshow" => DeviceBackend::DirectShow,
                    _ => match self
                        .program_input
                        .device_backend
                        .trim()
                        .to_ascii_lowercase()
                        .as_str()
                    {
                        "v4l2" => DeviceBackend::V4l2,
                        "directshow" | "dshow" => DeviceBackend::DirectShow,
                        other => bail!("unsupported program input device backend {other:?}"),
                    },
                };
                Ok(ProgramInput::Device(DeviceInput {
                    backend,
                    persistent_id: self.program_input.device_id.trim().to_string(),
                    decoder,
                    deinterlace,
                }))
            }
            "dummy" | "none" | "no_input" => {
                let (width, height) = self.configured_canvas_size();
                let frame_rate = Rational::parse(
                    non_empty(&self.program_input.fps)
                        .unwrap_or_else(|| non_empty(&self.video.fps).unwrap_or("30000/1001")),
                )?;
                let scan = scan_mode(
                    self.program_input.interlaced || self.video.interlaced,
                    non_empty(&self.program_input.field_order).unwrap_or(&self.video.field_order),
                )?;
                let background = if self.program_input.background.trim().is_empty() {
                    Rgba8::BLACK
                } else {
                    parse_rgba(&self.program_input.background).ok_or_else(|| {
                        anyhow::anyhow!(
                            "dummy program input background must be #RRGGBB or #RRGGBBAA"
                        )
                    })?
                };
                Ok(ProgramInput::Dummy(DummyInput {
                    format: VideoFormat::new(width, height, frame_rate, scan)?,
                    background,
                }))
            }
            "" | "uri_or_file" | "stream" | "uri" | "file" => {
                Ok(ProgramInput::UriOrFile(UriInput {
                    location: self.program_input.url.trim().to_string(),
                    demux_hint: demux_hint(&self.program_input.format)?,
                    decoder,
                    deinterlace,
                }))
            }
            other => bail!("unsupported program input type {other:?}"),
        }
    }

    pub(crate) fn audio_routing_spec(&self) -> Result<AudioRoutingSpec> {
        let topology = match self.audio.topology.trim().to_ascii_lowercase().as_str() {
            "preserve" | "preserve_native" | "preserve_native_tracks" => {
                AudioTopologyMode::PreserveNativeTracks
            }
            "" | "force" | "forced" | "force_layout" => {
                AudioTopologyMode::ForceLayout(channel_layout(&self.audio.force_layout)?)
            }
            other => bail!("unsupported audio topology {other:?}"),
        };
        Ok(AudioRoutingSpec {
            topology,
            idle_program_gain: GainDb::parse(&self.audio.idle_program_gain_db)?,
            alert_program_gain: GainDb::parse(&self.audio.alert_program_gain_db)?,
            alert_gain: GainDb::parse(&self.audio.alert_gain_db)?,
            transition_ms: self.audio.transition_ms,
        })
    }

    pub(crate) fn program_mapping_mode(&self) -> Result<ProgramMappingMode> {
        match self
            .program_mapping
            .mode
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "source" | "input" | "input_derived" | "follow_input" => Ok(ProgramMappingMode::Source),
            "auto" | "automatic" => Ok(ProgramMappingMode::Auto),
            "manual" | "configured" => Ok(ProgramMappingMode::Manual),
            "" if self.program_mapping.programs.is_empty() => Ok(ProgramMappingMode::Source),
            "" => Ok(ProgramMappingMode::Manual),
            other => bail!("unsupported program mapping mode {other:?}"),
        }
    }

    fn program_map_spec(&self) -> Result<ProgramMapSpec> {
        let mapping_mode = self.program_mapping_mode()?;
        if mapping_mode == ProgramMappingMode::Manual && !self.program_mapping.programs.is_empty() {
            let programs = self
                .program_mapping
                .programs
                .iter()
                .map(program_map_entry)
                .collect::<Result<Vec<_>>>()?;
            return Ok(ProgramMapSpec {
                transport_stream_id: self.program_mapping.transport_stream_id.max(1),
                programs,
            });
        }

        let enabled_videos = self.enabled_video_renditions(
            self.configured_canvas_size().0,
            self.configured_canvas_size().1,
        );
        if enabled_videos.is_empty() {
            bail!("MPEG-TS CGEN output requires at least one enabled video rendition");
        }
        let enabled_audio = self.enabled_audio_renditions();
        let default_service_name = non_empty(&self.name).unwrap_or(self.id.as_str());
        let mut programs = Vec::with_capacity(enabled_videos.len());
        for (video_index, video) in enabled_videos.iter().enumerate() {
            let fallback_program = u16::try_from(video_index + 1).unwrap_or(u16::MAX);
            let program_number = video
                .program
                .and_then(|value| u16::try_from(value).ok())
                .filter(|value| *value > 0)
                .or_else(|| {
                    (video_index == 0 && self.program_output.service_id > 0)
                        .then_some(self.program_output.service_id)
                })
                .unwrap_or(fallback_program.max(1));
            let audio_for_program = enabled_audio
                .iter()
                .filter(|audio| audio.program == Some(i32::from(program_number)))
                .collect::<Vec<_>>();
            // Legacy ladder configurations applied all enabled audio renditions
            // to every enabled video program. Preserve that behavior only when
            // no track explicitly targets this program.
            let audio_for_program = if audio_for_program.is_empty() {
                enabled_audio.iter().collect::<Vec<_>>()
            } else {
                audio_for_program
            };
            let audio = audio_for_program
                .into_iter()
                .map(|stream| {
                    Ok(AudioStreamMap {
                        track_id: AudioTrackId::parse(&stream.id)?,
                        pid: if mapping_mode == ProgramMappingMode::Manual
                            && stream.program == Some(i32::from(program_number))
                        {
                            stream
                                .audio_pid
                                .map(pid_assignment_i32)
                                .transpose()?
                                .unwrap_or(PidAssignment::Auto)
                        } else {
                            PidAssignment::Auto
                        },
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            programs.push(MpegTsProgramSpec {
                program_number: NonZeroU16::new(program_number)
                    .ok_or_else(|| anyhow::anyhow!("program number must be non-zero"))?,
                service: ServiceMetadata {
                    service_name: fallback_text(
                        &self.program_output.service_name,
                        default_service_name,
                    ),
                    provider_name: fallback_text(&self.program_output.provider_name, "Haze"),
                },
                pmt_pid: if mapping_mode == ProgramMappingMode::Manual {
                    video
                        .pmt_pid
                        .map(pid_assignment_i32)
                        .transpose()?
                        .unwrap_or(PidAssignment::Auto)
                } else {
                    PidAssignment::Auto
                },
                video_pid: Some(if mapping_mode == ProgramMappingMode::Manual {
                    video
                        .video_pid
                        .map(pid_assignment_i32)
                        .transpose()?
                        .unwrap_or(PidAssignment::Auto)
                } else {
                    PidAssignment::Auto
                }),
                audio,
                scte35: (video_index == 0
                    && pass_policy(&self.ancillary.scte35)? == PassPolicy::Pass)
                    .then_some(Scte35Map {
                        input: PassPolicy::Pass,
                        generated_alert_cues: true,
                        pid: PidAssignment::Auto,
                    }),
            });
        }
        Ok(ProgramMapSpec {
            transport_stream_id: self
                .program_mapping
                .transport_stream_id
                .max(self.program_output.transport_stream_id)
                .max(1),
            programs,
        })
    }

    fn output_specs(&self) -> Result<Vec<EncoderOutputSpec>> {
        if !self.outputs.outputs.is_empty() {
            return self
                .outputs
                .outputs
                .iter()
                .map(|output| {
                    output_spec(output).with_context(|| {
                        format!("invalid cgen encoder output {:?}", output.id.trim())
                    })
                })
                .collect::<Result<Vec<_>>>();
        }
        Ok(vec![legacy_output_spec(self)?])
    }

    pub(crate) fn program_input_url(&self) -> &str {
        &self.program_input.url
    }

    pub(crate) fn program_output_url(&self) -> &str {
        &self.program_output.url
    }

    pub(crate) fn resolved_program_input_url(&self) -> String {
        expand_env_vars(self.program_input_url())
    }

    pub(crate) fn resolved_program_output_url(&self) -> String {
        expand_env_vars(self.program_output_url())
    }

    pub(crate) fn output(&self) -> &EndpointConfig {
        &self.program_output
    }

    /// Explicit schema-v2 destinations use the isolated output-worker path.
    /// An empty section retains the legacy single-sink pipeline for one release.
    pub(crate) fn has_explicit_outputs(&self) -> bool {
        !self.outputs.outputs.is_empty()
    }

    pub(crate) fn redacted_program_input_url(&self) -> String {
        redact_endpoint_url(&self.resolved_program_input_url())
    }

    pub(crate) fn redacted_program_output_url(&self) -> String {
        redact_endpoint_url(&self.resolved_program_output_url())
    }

    pub(crate) fn enabled_video_renditions(
        &self,
        source_width: u32,
        source_height: u32,
    ) -> Vec<VideoRenditionConfig> {
        let mut out = Vec::new();
        if self.ladder.videos.is_empty() {
            return out;
        }
        for video in &self.ladder.videos {
            if video.enabled_for_source(source_width, source_height) {
                out.push(video.clone());
            }
        }
        if out.is_empty() {
            return out;
        }
        out
    }

    pub(crate) fn enabled_audio_renditions(&self) -> Vec<AudioRenditionConfig> {
        if self.ladder.audios.is_empty() {
            return Vec::new();
        }
        let out = self
            .ladder
            .audios
            .iter()
            .filter(|audio| audio.enabled_bool())
            .cloned()
            .collect::<Vec<_>>();
        if out.is_empty() {
            return out;
        }
        out
    }
}

fn pass_policy(raw: &str) -> Result<PassPolicy> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "pass" | "true" | "1" => Ok(PassPolicy::Pass),
        "" | "drop" | "false" | "0" => Ok(PassPolicy::Drop),
        other => bail!("unsupported ancillary pass policy {other:?}"),
    }
}

fn decoder_preference(endpoint: &EndpointConfig) -> Result<DecoderPreference> {
    let raw = endpoint.hardware_decoder.trim();
    if !xml_bool_text(&endpoint.hardware_decoder_enabled, false) || raw.is_empty() {
        return Ok(DecoderPreference::Auto);
    }
    let preference = match raw.to_ascii_lowercase().as_str() {
        "auto" => DecoderPreference::Auto,
        "software" | "cpu" => DecoderPreference::Software,
        "nvdec" | "nvidia" => DecoderPreference::Nvdec,
        "qsv" | "quicksync" | "quick_sync" => DecoderPreference::QuickSync,
        "vaapi" => DecoderPreference::Vaapi,
        _ => DecoderPreference::Named(raw.to_string()),
    };
    Ok(preference)
}

fn deinterlace_spec(endpoint: &EndpointConfig) -> Result<DeinterlaceSpec> {
    let algorithm = match endpoint
        .deinterlace_algorithm
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "" | "yadif" => DeinterlaceAlgorithm::Yadif,
        "bwdif" => DeinterlaceAlgorithm::Bwdif,
        "motion_adaptive" | "motion-adaptive" | "adaptive" => DeinterlaceAlgorithm::MotionAdaptive,
        other => bail!("unsupported deinterlace algorithm {other:?}"),
    };
    let backend = match endpoint
        .deinterlace_backend
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "auto" => DeinterlaceBackend::Auto,
        "" | "software" | "cpu" => DeinterlaceBackend::Software,
        "vaapi" | "va" => DeinterlaceBackend::Vaapi,
        "qsv" | "quicksync" | "quick_sync" => DeinterlaceBackend::QuickSync,
        "d3d11" | "direct3d11" => DeinterlaceBackend::D3d11,
        "cuda" | "nvidia" => DeinterlaceBackend::Cuda,
        "vulkan" => DeinterlaceBackend::Vulkan,
        "gl" | "opengl" => DeinterlaceBackend::OpenGl,
        other => bail!("unsupported deinterlace backend {other:?}"),
    };
    let cadence = match endpoint
        .deinterlace_cadence
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "" | "field" | "field_rate" | "bob" => DeinterlaceCadence::Field,
        "frame" | "frame_rate" | "single" => DeinterlaceCadence::Frame,
        other => bail!("unsupported deinterlace cadence {other:?}"),
    };
    let parity = match endpoint
        .deinterlace_parity
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "" | "auto" => DeinterlaceParity::Auto,
        "tff" | "top" | "top_first" => DeinterlaceParity::TopFirst,
        "bff" | "bottom" | "bottom_first" => DeinterlaceParity::BottomFirst,
        other => bail!("unsupported deinterlace parity {other:?}"),
    };
    Ok(DeinterlaceSpec {
        algorithm,
        backend,
        cadence,
        parity,
    })
}

fn demux_hint(raw: &str) -> Result<DemuxHint> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "auto" => Ok(DemuxHint::Auto),
        "mpegts" | "mpeg-ts" | "ts" => Ok(DemuxHint::MpegTs),
        "rtp" => Ok(DemuxHint::Rtp),
        "srt" => Ok(DemuxHint::Srt),
        "file" => Ok(DemuxHint::File),
        other => bail!("unsupported program input format hint {other:?}"),
    }
}

fn scan_mode(interlaced: bool, field_order: &str) -> Result<ScanMode> {
    if !interlaced {
        return Ok(ScanMode::Progressive);
    }
    let field_order = match field_order.trim().to_ascii_lowercase().as_str() {
        "" | "tff" | "top" | "top_first" => FieldOrder::TopFirst,
        "bff" | "bottom" | "bottom_first" => FieldOrder::BottomFirst,
        other => bail!("unsupported interlaced field order {other:?}"),
    };
    Ok(ScanMode::Interlaced { field_order })
}

fn parse_rgba(raw: &str) -> Option<Rgba8> {
    let value = raw.trim().strip_prefix('#').unwrap_or(raw.trim());
    let parse = |range: std::ops::Range<usize>| u8::from_str_radix(value.get(range)?, 16).ok();
    match value.len() {
        6 => Some(Rgba8 {
            red: parse(0..2)?,
            green: parse(2..4)?,
            blue: parse(4..6)?,
            alpha: 255,
        }),
        8 => Some(Rgba8 {
            red: parse(0..2)?,
            green: parse(2..4)?,
            blue: parse(4..6)?,
            alpha: parse(6..8)?,
        }),
        _ => None,
    }
}

fn channel_layout(raw: &str) -> Result<ChannelLayout> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "mono" | "1" | "1.0" => Ok(ChannelLayout::Mono),
        "" | "stereo" | "2" | "2.0" => Ok(ChannelLayout::Stereo),
        "5.1" | "surround51" | "surround_51" | "surround_5_1" | "6" => {
            Ok(ChannelLayout::Surround51)
        }
        other => bail!("unsupported forced audio channel layout {other:?}"),
    }
}

fn program_map_entry(raw: &ProgramMapEntryConfig) -> Result<MpegTsProgramSpec> {
    let program_number = NonZeroU16::new(raw.number)
        .ok_or_else(|| anyhow::anyhow!("program number must be non-zero"))?;
    let audio = raw
        .audio
        .iter()
        .map(|stream| {
            Ok(AudioStreamMap {
                track_id: AudioTrackId::parse(&stream.track_id)?,
                pid: pid_assignment(&stream.pid)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let scte35 = raw
        .scte35
        .as_ref()
        .map(|map| -> Result<Scte35Map> {
            Ok(Scte35Map {
                input: pass_policy(&map.input)?,
                generated_alert_cues: map.generated_alert_cues,
                pid: pid_assignment(&map.pid)?,
            })
        })
        .transpose()?;
    Ok(MpegTsProgramSpec {
        program_number,
        service: ServiceMetadata {
            service_name: fallback_text(&raw.service_name, "Haze CGEN"),
            provider_name: fallback_text(&raw.provider_name, "Haze"),
        },
        pmt_pid: pid_assignment(&raw.pmt_pid)?,
        video_pid: Some(pid_assignment(&raw.video_pid)?),
        audio,
        scte35,
    })
}

fn pid_assignment(raw: &str) -> Result<PidAssignment> {
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("auto") {
        return Ok(PidAssignment::Auto);
    }
    let value = if let Some(hex) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) {
        u16::from_str_radix(hex, 16)
    } else {
        raw.parse::<u16>()
    }
    .map_err(|_| anyhow::anyhow!("invalid MPEG-TS PID {raw:?}"))?;
    Ok(PidAssignment::Manual(MpegTsPid::new(value)?))
}

fn pid_assignment_i32(raw: i32) -> Result<PidAssignment> {
    let value = u16::try_from(raw).map_err(|_| anyhow::anyhow!("invalid MPEG-TS PID {raw}"))?;
    Ok(PidAssignment::Manual(MpegTsPid::new(value)?))
}

fn output_spec(raw: &OutputConfig) -> Result<EncoderOutputSpec> {
    let video_bitrate = NonZeroU32::new(raw.video_bitrate_kbps)
        .ok_or_else(|| anyhow::anyhow!("video bitrate must be non-zero"))?;
    let rate_control = match raw.rate_control.trim().to_ascii_lowercase().as_str() {
        "" | "cbr" => RateControl::Cbr {
            bitrate_kbps: video_bitrate,
        },
        "vbr" => RateControl::Vbr {
            target_kbps: video_bitrate,
            max_kbps: NonZeroU32::new(raw.video_max_bitrate_kbps)
                .ok_or_else(|| anyhow::anyhow!("VBR maximum bitrate must be non-zero"))?,
        },
        other => bail!("unsupported output rate control {other:?}"),
    };
    if let RateControl::Vbr {
        target_kbps,
        max_kbps,
    } = &rate_control
    {
        if max_kbps.get() < target_kbps.get() {
            bail!("VBR maximum bitrate must be at least the target bitrate");
        }
    }
    Ok(EncoderOutputSpec {
        id: OutputId::parse(&raw.id)?,
        enabled: raw.enabled,
        destination: configured_output_destination(
            &raw.destination,
            &raw.url,
            &raw.video_url,
            &raw.audio_urls,
            &raw.container,
            raw.latency_ms,
        )?,
        video: VideoEncoderSpec {
            codec: configured_video_codec(&raw.video_codec)?,
            rate_control,
            gop_frames: NonZeroU32::new(raw.gop_frames)
                .ok_or_else(|| anyhow::anyhow!("GOP frames must be non-zero"))?,
        },
        audio: AudioEncoderSpec {
            codec: configured_audio_codec_policy(&raw.audio_codec)?,
            bitrate_kbps: NonZeroU32::new(raw.audio_bitrate_kbps)
                .ok_or_else(|| anyhow::anyhow!("audio bitrate must be non-zero"))?,
            sample_rate: NonZeroU32::new(raw.sample_rate)
                .ok_or_else(|| anyhow::anyhow!("audio sample rate must be non-zero"))?,
        },
    })
}

fn configured_output_destination(
    kind: &str,
    url: &str,
    video_url: &str,
    audio_urls: &str,
    container: &str,
    latency_ms: u32,
) -> Result<OutputDestination> {
    let destination = match kind.trim().to_ascii_lowercase().as_str() {
        "mpeg_ts_udp" | "mpegts_udp" => OutputDestination::MpegTsUdp {
            location: url.trim().to_string(),
        },
        "mpeg_ts_srt" | "mpegts_srt" => OutputDestination::MpegTsSrt {
            location: url.trim().to_string(),
            latency_ms,
        },
        "rtp" => OutputDestination::Rtp {
            video_location: fallback_text(video_url, url),
            audio_locations: audio_urls
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect(),
        },
        "rtmp" => OutputDestination::Rtmp {
            location: url.trim().to_string(),
        },
        "file" => OutputDestination::File {
            location: url.trim().to_string(),
            container: fallback_text(container, "mpegts"),
        },
        other => bail!("unsupported output destination {other:?}"),
    };
    Ok(destination)
}

fn configured_video_codec(raw: &str) -> Result<VideoCodec> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "h264" | "h.264" | "avc" | "x264" | "x264enc" | "libx264" => Ok(VideoCodec::H264),
        "h265" | "h.265" | "hevc" | "x265" | "x265enc" | "libx265" => Ok(VideoCodec::H265),
        "mpeg2" | "mpeg-2" | "mpeg2video" | "avenc_mpeg2video" => Ok(VideoCodec::Mpeg2),
        other => bail!("unsupported output video codec {other:?}"),
    }
}

fn configured_audio_codec_policy(raw: &str) -> Result<AudioCodecPolicy> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "match" | "match_input" | "native" => Ok(AudioCodecPolicy::MatchInput),
        "aac" | "avenc_aac" | "fdkaac" | "fdkaacenc" => {
            Ok(AudioCodecPolicy::Encode(AudioCodec::Aac))
        }
        "ac3" | "ac-3" | "avenc_ac3" => Ok(AudioCodecPolicy::Encode(AudioCodec::Ac3)),
        "mp2" | "mpeg2audio" | "avenc_mp2" | "twolame" => {
            Ok(AudioCodecPolicy::Encode(AudioCodec::Mp2))
        }
        other => bail!("unsupported output audio codec {other:?}"),
    }
}

fn legacy_output_spec(feed: &FeedConfig) -> Result<EncoderOutputSpec> {
    let output = feed.output();
    let video_codec_name = non_empty(&output.vcodec)
        .or_else(|| non_empty(&feed.encoder.video.codec))
        .or_else(|| {
            feed.ladder
                .videos
                .first()
                .map(|video| video.vcodec.as_str())
        })
        .unwrap_or("x264enc");
    let audio_codec_name = non_empty(&output.acodec)
        .or_else(|| non_empty(&feed.encoder.audio.codec))
        .or_else(|| {
            feed.ladder
                .audios
                .first()
                .map(|audio| audio.acodec.as_str())
        })
        .unwrap_or("avenc_aac");
    let video_bitrate = output
        .video_bitrate_kbps
        .or(feed.encoder.video.bitrate_kbps)
        .unwrap_or(8_000)
        .max(1);
    let audio_bitrate = output
        .audio_bitrate_kbps
        .or(feed.encoder.audio.bitrate_kbps)
        .unwrap_or(192)
        .max(1);
    let preserve = matches!(
        feed.audio.topology.trim().to_ascii_lowercase().as_str(),
        "preserve" | "preserve_native" | "preserve_native_tracks"
    );
    Ok(EncoderOutputSpec {
        id: OutputId::parse("primary")?,
        enabled: true,
        destination: output_destination(&output.format, &output.url, "", "", &output.format, 120),
        video: VideoEncoderSpec {
            codec: video_codec(video_codec_name),
            rate_control: RateControl::Cbr {
                bitrate_kbps: NonZeroU32::new(video_bitrate)
                    .ok_or_else(|| anyhow::anyhow!("video bitrate must be non-zero"))?,
            },
            gop_frames: NonZeroU32::new(feed.encoder.video.gop.unwrap_or(60).max(1))
                .ok_or_else(|| anyhow::anyhow!("GOP frames must be non-zero"))?,
        },
        audio: AudioEncoderSpec {
            codec: if preserve {
                AudioCodecPolicy::MatchInput
            } else {
                AudioCodecPolicy::Encode(audio_codec(audio_codec_name))
            },
            bitrate_kbps: NonZeroU32::new(audio_bitrate)
                .ok_or_else(|| anyhow::anyhow!("audio bitrate must be non-zero"))?,
            sample_rate: NonZeroU32::new(48_000)
                .ok_or_else(|| anyhow::anyhow!("audio sample rate must be non-zero"))?,
        },
    })
}

fn output_destination(
    kind: &str,
    url: &str,
    video_url: &str,
    audio_urls: &str,
    container: &str,
    latency_ms: u32,
) -> OutputDestination {
    let kind = kind.trim().to_ascii_lowercase();
    let url_lower = url.trim().to_ascii_lowercase();
    match kind.as_str() {
        "mpeg_ts_srt" | "mpegts_srt" | "srt" => OutputDestination::MpegTsSrt {
            location: url.trim().to_string(),
            latency_ms,
        },
        "mpeg_ts_udp" | "mpegts_udp" | "udp" | "mpegts" | "mpeg-ts"
            if url_lower.starts_with("srt://") =>
        {
            OutputDestination::MpegTsSrt {
                location: url.trim().to_string(),
                latency_ms,
            }
        }
        "mpeg_ts_udp" | "mpegts_udp" | "udp" | "mpegts" | "mpeg-ts" => {
            OutputDestination::MpegTsUdp {
                location: url.trim().to_string(),
            }
        }
        "rtp" => OutputDestination::Rtp {
            video_location: fallback_text(video_url, url),
            audio_locations: audio_urls
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect(),
        },
        "rtmp" | "flv" => OutputDestination::Rtmp {
            location: url.trim().to_string(),
        },
        "file" => OutputDestination::File {
            location: url.trim().to_string(),
            container: fallback_text(container, "mpegts"),
        },
        _ if url_lower.starts_with("rtmp://") || url_lower.starts_with("rtmps://") => {
            OutputDestination::Rtmp {
                location: url.trim().to_string(),
            }
        }
        _ if url_lower.starts_with("srt://") => OutputDestination::MpegTsSrt {
            location: url.trim().to_string(),
            latency_ms,
        },
        _ if url_lower.starts_with("udp://") => OutputDestination::MpegTsUdp {
            location: url.trim().to_string(),
        },
        _ => OutputDestination::File {
            location: url.trim().to_string(),
            container: fallback_text(container, "mpegts"),
        },
    }
}

fn video_codec(raw: &str) -> VideoCodec {
    let raw = raw.trim().to_ascii_lowercase();
    if raw.contains("265") || raw.contains("hevc") {
        VideoCodec::H265
    } else if raw.contains("mpeg2") || raw.contains("mpeg-2") {
        VideoCodec::Mpeg2
    } else {
        VideoCodec::H264
    }
}

fn audio_codec(raw: &str) -> AudioCodec {
    let raw = raw.trim().to_ascii_lowercase();
    if raw.contains("ac3") || raw.contains("ac-3") {
        AudioCodec::Ac3
    } else if raw.contains("mp2") || raw.contains("twolame") || raw.contains("layer2") {
        AudioCodec::Mp2
    } else {
        AudioCodec::Aac
    }
}

fn audio_codec_policy(raw: &str) -> AudioCodecPolicy {
    if matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "match" | "match_input" | "native"
    ) {
        AudioCodecPolicy::MatchInput
    } else {
        AudioCodecPolicy::Encode(audio_codec(raw))
    }
}

pub(crate) fn redact_feed_endpoint_status(feed: &FeedConfig, value: &mut Value) {
    let resolved_input = feed.resolved_program_input_url();
    let resolved_output = feed.resolved_program_output_url();
    let mut replacements = vec![
        (
            feed.program_input_url().trim().to_string(),
            redact_endpoint_url(feed.program_input_url()),
        ),
        (
            feed.program_output_url().trim().to_string(),
            redact_endpoint_url(feed.program_output_url()),
        ),
        (
            resolved_input.trim().to_string(),
            feed.redacted_program_input_url(),
        ),
        (
            resolved_output.trim().to_string(),
            feed.redacted_program_output_url(),
        ),
    ];
    for output in &feed.outputs.outputs {
        for location in [&output.url, &output.video_url, &output.audio_urls] {
            if location.trim().is_empty() {
                continue;
            }
            replacements.push((location.trim().to_string(), redact_endpoint_url(location)));
            let resolved = expand_env_vars(location);
            replacements.push((resolved.clone(), redact_endpoint_url(&resolved)));
        }
    }
    redact_status_value(value, &replacements);
}

pub(crate) fn redacted_pipeline_description(feed: &FeedConfig, text: &str) -> String {
    let mut value = Value::String(text.to_string());
    redact_feed_endpoint_status(feed, &mut value);
    value.as_str().unwrap_or(text).to_string()
}

fn redact_status_value(value: &mut Value, replacements: &[(String, String)]) {
    match value {
        Value::String(text) => {
            for (raw, redacted) in replacements {
                if !raw.is_empty() && raw != redacted {
                    *text = text.replace(raw, redacted);
                    let quoted = format!("\"{}\"", raw.replace('\\', "\\\\").replace('"', "\\\""));
                    let redacted_quoted = format!(
                        "\"{}\"",
                        redacted.replace('\\', "\\\\").replace('"', "\\\"")
                    );
                    *text = text.replace(&quoted, &redacted_quoted);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_status_value(item, replacements);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                redact_status_value(item, replacements);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn redact_endpoint_url(value: &str) -> String {
    let value = value.trim();
    let Some(scheme_end) = value.find("://") else {
        return value.to_string();
    };
    let scheme = &value[..scheme_end];
    let rest = &value[scheme_end + 3..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if authority.is_empty() {
        return format!("{scheme}://...");
    }
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("{scheme}://{host}/...")
}

impl VideoRenditionConfig {
    pub(crate) fn enabled_for_source(&self, source_width: u32, source_height: u32) -> bool {
        let enabled = self.enabled.trim();
        if enabled.eq_ignore_ascii_case("auto") {
            return self.id.eq_ignore_ascii_case("hd")
                && self.width == source_width
                && self.height == source_height;
        }
        xml_bool_text(enabled, true)
    }

    pub(crate) fn codec_name<'a>(&'a self, fallback: &'a str) -> &'a str {
        non_empty(self.vcodec.as_str()).unwrap_or(fallback)
    }

    pub(crate) fn frame_rate_text<'a>(&'a self, fallback: &'a str) -> &'a str {
        non_empty(self.fps.as_str()).unwrap_or(fallback)
    }

    pub(crate) fn program_number(&self, index: usize) -> i32 {
        self.program
            .unwrap_or_else(|| i32::try_from(index + 1).unwrap_or(1))
            .max(1)
    }

    pub(crate) fn video_pid(&self, index: usize) -> i32 {
        self.video_pid
            .unwrap_or_else(|| 0x100 + (i32::try_from(index).unwrap_or(0) * 0x20))
    }

    pub(crate) fn pmt_pid(&self, index: usize) -> i32 {
        self.pmt_pid
            .unwrap_or_else(|| 0x1000 + i32::try_from(index).unwrap_or(0))
    }
}

impl AudioRenditionConfig {
    pub(crate) fn enabled_bool(&self) -> bool {
        xml_bool_text(&self.enabled, true)
    }

    pub(crate) fn channels(&self) -> u16 {
        self.channels.clamp(1, 6)
    }

    pub(crate) fn codec_name<'a>(&'a self, fallback: &'a str) -> &'a str {
        non_empty(self.acodec.as_str()).unwrap_or(fallback)
    }

    pub(crate) fn bitrate_bps(&self) -> i64 {
        self.bitrate_kbps
            .map(|kbps| i64::from(kbps) * 1000)
            .unwrap_or_else(|| {
                if self.channels() > 2 {
                    384_000
                } else {
                    192_000
                }
            })
    }
}

pub(crate) fn load_config(path: &Path) -> Result<CgenConfig> {
    let raw = read_bounded_utf8(path, MAX_CGEN_CONFIG_BYTES, "cgen configuration")?;
    reject_legacy_cgen_xml(path, &raw)?;
    let mut config: CgenConfig = quick_xml::de::from_str(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    config.apply_nested_sections();
    if let Some(encoders) = load_encoder_settings(path)? {
        config.apply_encoder_settings(encoders)?;
    }
    Ok(config)
}

fn load_encoder_settings(cgen_path: &Path) -> Result<Option<CgenEncodersConfig>> {
    let Some(parent) = cgen_path.parent() else {
        return Ok(None);
    };
    let path = parent.join("cgen-encoders.xml");
    let raw = match read_bounded_utf8(&path, MAX_CGEN_ENCODERS_BYTES, "cgen encoder configuration")
    {
        Ok(raw) => raw,
        Err(err)
            if err
                .downcast_ref::<std::io::Error>()
                .is_some_and(|source| source.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    let encoders: CgenEncodersConfig = quick_xml::de::from_str(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some(encoders))
}

fn read_bounded_utf8(path: &Path, maximum_bytes: u64, description: &str) -> Result<String> {
    let metadata =
        fs::metadata(path).with_context(|| format!("failed to inspect {}", path.display()))?;
    if metadata.len() > maximum_bytes {
        bail!(
            "{} exceeds the {} byte safety limit: {}",
            description,
            maximum_bytes,
            path.display()
        );
    }
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum_bytes {
        bail!(
            "{} exceeds the {} byte safety limit: {}",
            description,
            maximum_bytes,
            path.display()
        );
    }
    String::from_utf8(bytes).with_context(|| format!("{} is not UTF-8", path.display()))
}

fn expand_env_vars(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = String::with_capacity(raw.len());
    let mut cursor = 0usize;
    let mut literal_start = 0usize;

    while cursor < bytes.len() {
        if bytes[cursor] != b'$' {
            cursor += 1;
            continue;
        }

        out.push_str(&raw[literal_start..cursor]);
        if bytes.get(cursor + 1) == Some(&b'{') {
            let name_start = cursor + 2;
            let Some(relative_end) = bytes[name_start..].iter().position(|byte| *byte == b'}')
            else {
                out.push_str(&raw[cursor..]);
                return out;
            };
            let name_end = name_start + relative_end;
            let name = &raw[name_start..name_end];
            if valid_env_name(name) {
                out.push_str(&std::env::var(name).unwrap_or_default());
            } else {
                out.push_str(&raw[cursor..=name_end]);
            }
            cursor = name_end + 1;
            literal_start = cursor;
            continue;
        }

        let name_start = cursor + 1;
        let mut name_end = name_start;
        while name_end < bytes.len()
            && (bytes[name_end] == b'_' || bytes[name_end].is_ascii_alphanumeric())
        {
            name_end += 1;
        }
        let name = &raw[name_start..name_end];
        if valid_env_name(name) {
            out.push_str(&std::env::var(name).unwrap_or_default());
            cursor = name_end;
        } else {
            out.push('$');
            cursor += 1;
        }
        literal_start = cursor;
    }

    out.push_str(&raw[literal_start..]);
    out
}

fn valid_env_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first == b'_' || first.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

fn reject_legacy_cgen_xml(path: &Path, raw: &str) -> Result<()> {
    let mut reader = Reader::from_str(raw);
    reader.config_mut().trim_text(true);
    let mut stack: Vec<String> = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => {
                let element_name = event.name();
                let name_bytes = element_name.as_ref();
                let name = std::str::from_utf8(name_bytes).unwrap_or_default();
                let parent = stack.last().map(String::as_str).unwrap_or_default();
                if legacy_element(parent, name) {
                    bail!(
                        "legacy cgen <{}> element is no longer supported in {}; use programInput, priorityInput, and programOutput",
                        name,
                        path.display()
                    );
                }
                reject_legacy_attrs(path, name, event.attributes())?;
                stack.push(name.to_string());
            }
            Ok(Event::Empty(event)) => {
                let element_name = event.name();
                let name_bytes = element_name.as_ref();
                let name = std::str::from_utf8(name_bytes).unwrap_or_default();
                let parent = stack.last().map(String::as_str).unwrap_or_default();
                if legacy_element(parent, name) {
                    bail!(
                        "legacy cgen <{}> element is no longer supported in {}; use programInput, priorityInput, and programOutput",
                        name,
                        path.display()
                    );
                }
                reject_legacy_attrs(path, name, event.attributes())?;
            }
            Ok(Event::End(_)) => {
                stack.pop();
            }
            Ok(Event::Eof) => return Ok(()),
            Ok(_) => {}
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to scan cgen XML {}", path.display()))
            }
        }
    }
}

fn reject_legacy_attrs<'a>(
    path: &Path,
    name: &str,
    attributes: quick_xml::events::attributes::Attributes<'a>,
) -> Result<()> {
    for attr in attributes {
        let attr = attr
            .with_context(|| format!("failed to read cgen XML attribute in {}", path.display()))?;
        let key_bytes = attr.key.as_ref();
        let key = std::str::from_utf8(key_bytes).unwrap_or_default();
        if legacy_attribute(name, key) {
            bail!(
                "legacy cgen {} @{} attribute is no longer supported in {}",
                name,
                key,
                path.display()
            );
        }
    }
    Ok(())
}

fn legacy_element(parent: &str, element: &str) -> bool {
    (element == "output" && !matches!(parent, "program" | "outputs"))
        || element == "alertOutput"
        || (element == "input" && !matches!(parent, "program" | "priority"))
}

fn legacy_attribute(element: &str, attr: &str) -> bool {
    match element {
        "cgen" => matches!(attr, "graphics_backend" | "ffmpeg"),
        "priorityInput" => attr == "url",
        "audio" => attr == "duck_db",
        "banner" => matches!(attr, "x" | "y"),
        "graphics" => matches!(
            attr,
            "background_color" | "text_x" | "text_y" | "banner_x" | "banner_y" | "banner_width"
        ),
        _ => false,
    }
}

pub(crate) fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

pub(crate) fn allowed_video_size(width: u32, height: u32) -> bool {
    matches!(
        (width, height),
        (720, 480) | (720, 540) | (720, 576) | (1280, 720) | (1920, 1080)
    )
}

fn default_true() -> bool {
    true
}

fn default_schema_version() -> u16 {
    1
}

fn default_true_text() -> String {
    "true".to_string()
}

fn default_stereo_channels() -> u16 {
    2
}

fn default_audio_language() -> String {
    "eng".to_string()
}

fn source_audio() -> String {
    "source".to_string()
}

fn priority_audio_source() -> String {
    "priority".to_string()
}

fn replace_audio() -> String {
    "replace".to_string()
}

fn default_audio_topology() -> String {
    "force_layout".to_string()
}

fn default_forced_audio_layout() -> String {
    "stereo".to_string()
}

fn default_idle_program_gain() -> String {
    "0".to_string()
}

fn default_alert_program_gain() -> String {
    "muted".to_string()
}

fn default_alert_gain() -> String {
    "0".to_string()
}

fn default_audio_transition_ms() -> u16 {
    20
}

fn default_alert_scene_id() -> String {
    "Standard_Crawl".to_string()
}

fn default_compositor_engine() -> String {
    "legacy".to_string()
}

fn auto_mode() -> String {
    "auto".to_string()
}

fn default_ticker_height() -> u32 {
    128
}

fn default_font_size() -> u32 {
    58
}

fn regular_font_weight() -> String {
    "regular".to_string()
}

fn default_scroll_speed() -> u32 {
    8
}

fn default_scroll_repeat_mode() -> String {
    "until_audio_end".to_string()
}

fn default_fixed_repeats() -> u32 {
    1
}

fn default_text_x() -> i32 {
    48
}

fn default_text_y() -> i32 {
    128
}

fn default_clock_y() -> i32 {
    48
}

fn default_clock_font_size() -> u32 {
    30
}

fn default_insert_font_size() -> u32 {
    58
}

fn default_field_order() -> String {
    "tff".to_string()
}

fn release_mode() -> String {
    "release".to_string()
}

fn standby_banner_mode() -> String {
    "banner".to_string()
}

fn default_standby_text() -> String {
    "Emergency Alert Details Channel".to_string()
}

fn default_standby_y_percent() -> u32 {
    10
}

pub(crate) fn default_hard_reset_ms() -> u32 {
    250
}

pub(crate) fn default_max_audio_frames_per_video() -> u32 {
    8
}

pub(crate) fn default_source_buffer_ms() -> u32 {
    240
}

pub(crate) fn default_reconnect_initial_ms() -> u32 {
    500
}

pub(crate) fn default_reconnect_max_ms() -> u32 {
    10_000
}

pub(crate) fn default_status_interval_ms() -> u32 {
    750
}

fn xml_bool_text(value: &str, default: bool) -> bool {
    let value = value.trim();
    if value.is_empty() {
        return default;
    }
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "enabled"
    )
}

fn non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn fallback_text(value: &str, fallback: &str) -> String {
    non_empty(value).unwrap_or(fallback).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_disabled_example_feed() {
        let xml = r#"
<cgen enabled="true">
  <feed id="*" enabled="false">
    <programInput url="udp://127.0.0.1:5000" format="mpegts"/>
    <programOutput url="udp://127.0.0.1:5001" format="mpegts" vcodec="libx264" acodec="aac"/>
    <video width="1280" height="720" fps="source" interlaced="false" field_order="tff"/>
    <audio idle="source" alert_mode="replace"/>
    <banner mode="auto" ticker_height="128" font="Arial"/>
  </feed>
</cgen>"#;
        let parsed: CgenConfig = quick_xml::de::from_str(xml).expect("parse");
        assert!(parsed.enabled);
        assert_eq!(parsed.feeds.len(), 1);
        assert_eq!(parsed.enabled_feeds().expect("feeds").len(), 0);
    }

    #[test]
    fn load_config_rejects_legacy_cgen_elements() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cgen.xml");
        std::fs::write(
            &path,
            r#"
<cgen enabled="true">
  <feed id="CAP-IT-ALL" enabled="true">
    <input></input>
    <programInput url="udp://127.0.0.1:5000" format="mpegts"/>
    <programOutput url="udp://127.0.0.1:5001" format="mpegts"/>
    <video width="1280" height="720"/>
  </feed>
</cgen>"#,
        )
        .expect("write config");

        let err = load_config(&path).expect_err("legacy config rejected");
        assert!(err.to_string().contains("legacy cgen <input> element"));
    }

    #[test]
    fn load_config_rejects_legacy_cgen_attributes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cgen.xml");
        std::fs::write(
            &path,
            r#"
<cgen enabled="true">
  <feed id="CAP-IT-ALL" enabled="true">
    <programInput url="udp://127.0.0.1:5000" format="mpegts"/>
    <priorityInput feed_id="*" url="udp://127.0.0.1:5002" format="priority-audio"/>
    <programOutput url="udp://127.0.0.1:5001" format="mpegts"/>
    <video width="1280" height="720"/>
    <audio idle="source" alert_mode="replace" duck_db="-12"/>
  </feed>
</cgen>"#,
        )
        .expect("write config");

        let err = load_config(&path).expect_err("legacy config rejected");
        assert!(err
            .to_string()
            .contains("legacy cgen priorityInput @url attribute"));
    }

    #[test]
    fn load_config_accepts_current_cgen_shape() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cgen.xml");
        std::fs::write(
            &path,
            r#"
<cgen enabled="true">
  <feed id="CAP-IT-ALL" enabled="true">
    <programInput url="udp://127.0.0.1:5000" format="mpegts"/>
    <priorityInput feed_id="*" audio_source="both" format="priority-audio"/>
    <programOutput url="udp://127.0.0.1:5001" format="mpegts" vcodec="mpeg2video" acodec="ac3"/>
    <video width="1280" height="720" fps="30000/1001"/>
    <audio idle="source" alert_mode="replace" mute_standby_routine="true"/>
    <ladder>
      <video id="p720" enabled="true" width="1280" height="720" fps="30000/1001"/>
      <audio id="stereo" enabled="true" channels="2"/>
    </ladder>
  </feed>
</cgen>"#,
        )
        .expect("write config");

        let config = load_config(&path).expect("current config");
        assert_eq!(config.enabled_feeds().expect("feeds").len(), 1);
    }

    #[test]
    fn load_config_merges_cgen_encoder_settings() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cgen.xml");
        std::fs::write(
            &path,
            r#"
<cgen enabled="true">
  <feed id="CAP" enabled="true">
    <program>
      <input url="udp://in" format="mpegts"></input>
      <output url="udp://out" format="mpegts" vcodec="x264enc" acodec="avenc_ac3"></output>
    </program>
    <media>
      <video width="1280" height="720" fps="source"></video>
      <audio idle="source" alert_mode="replace"></audio>
    </media>
    <ladder>
      <video id="hd" enabled="true" width="1280" height="720" fps="source" vcodec="x264enc" bitrate_kbps="6000"/>
      <audio id="stereo" enabled="true" channels="2" acodec="avenc_ac3" bitrate_kbps="192"/>
    </ladder>
  </feed>
</cgen>"#,
        )
        .expect("write cgen");
        std::fs::write(
            dir.path().join("cgen-encoders.xml"),
            r#"
<cgenEncoders>
  <feed id="CAP">
    <video codec="x264enc" bitrate_kbps="7000" gop="30" bframes="2" preset="veryfast" tune="zerolatency"></video>
    <audio codec="avenc_ac3" bitrate_kbps="256" profile="main"></audio>
  </feed>
</cgenEncoders>"#,
        )
        .expect("write encoders");

        let config = load_config(&path).expect("config");
        let feed = &config.feeds[0];

        assert_eq!(feed.encoder.video.codec, "x264enc");
        assert_eq!(feed.encoder.video.bitrate_kbps, Some(7000));
        assert_eq!(feed.encoder.video.gop, Some(30));
        assert_eq!(feed.encoder.video.bframes, Some(2));
        assert_eq!(feed.encoder.video.preset, "veryfast");
        assert_eq!(feed.encoder.audio.bitrate_kbps, Some(256));
    }

    #[test]
    fn load_config_indexes_versioned_encoder_profiles_by_output_id() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cgen.xml");
        std::fs::write(
            &path,
            r#"<cgen schema_version="2" enabled="true">
  <feed id="CAP" enabled="true">
    <programInput type="dummy" width="720" height="480" fps="30000/1001"/>
    <priorityInput feed_id="CAP"/>
    <programOutput url="udp://239.0.0.1:9000" format="mpegts"/>
    <video width="720" height="480" fps="30000/1001"/>
    <audio topology="force_layout" force_layout="stereo"/>
    <ladder><video id="sd" enabled="true" width="720" height="480"/><audio id="stereo" enabled="true" channels="2"/></ladder>
    <outputs><output id="Primary" enabled="true" destination="rtmp" url="rtmp://example.invalid/live" video_codec="h264" audio_codec="aac" video_bitrate_kbps="4000" audio_bitrate_kbps="192" sample_rate="48000" gop_frames="60"/></outputs>
  </feed>
</cgen>"#,
        )
        .expect("write config");
        std::fs::write(
            dir.path().join("cgen-encoders.xml"),
            r#"<cgenEncoders schema_version="2"><output id="primary"><video codec="x264enc" preset="veryfast"/><audio codec="avenc_aac"/></output></cgenEncoders>"#,
        )
        .expect("write encoders");

        let config = load_config(&path).expect("config");
        let settings = config.feeds[0]
            .encoder_settings_for_output("PRIMARY")
            .expect("output profile");
        assert_eq!(settings.video.codec, "x264enc");
        assert_eq!(settings.video.preset, "veryfast");
        assert_eq!(settings.audio.codec, "avenc_aac");
    }

    #[test]
    fn load_config_accepts_nested_cgen_shape() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cgen.xml");
        std::fs::write(
            &path,
            r#"
<cgen enabled="true">
  <feed id="CAP-IT-ALL" enabled="true">
    <program>
      <input url="udp://127.0.0.1:5000" format="mpegts"/>
      <output url="udp://127.0.0.1:5001" format="mpegts" vcodec="mpeg2video" acodec="ac3"/>
    </program>
    <priority>
      <input feed_id="*" audio_source="both" format="priority-audio"/>
    </priority>
    <media>
      <video width="1280" height="720" fps="30000/1001"/>
      <audio idle="source" alert_mode="replace" mute_standby_routine="true"/>
    </media>
    <ladder>
      <video id="p720" enabled="true" width="1280" height="720" fps="30000/1001"/>
      <audio id="stereo" enabled="true" channels="2"/>
    </ladder>
    <presentation>
      <banner mode="ticker" background_enabled="true">
        <font family="Arial" weight="regular" size="58"/>
        <ticker height="96" speed="12">
          <repeat mode="until_audio_end" after_eom="2" count="1"/>
        </ticker>
      </banner>
    </presentation>
  </feed>
</cgen>"#,
        )
        .expect("write config");

        let config = load_config(&path).expect("nested config");
        let feeds = config.enabled_feeds().expect("feeds");
        assert_eq!(feeds[0].program_input_url(), "udp://127.0.0.1:5000");
        assert_eq!(feeds[0].priority_input.feed_id, "*");
        assert_eq!(feeds[0].video.width, 1280);
        assert_eq!(feeds[0].banner.ticker_height, 96);
        assert_eq!(feeds[0].banner.scroll_speed, 12);
        assert_eq!(feeds[0].banner.scroll_repeat_mode, "until_audio_end");
        assert_eq!(feeds[0].banner.after_eom_repeats, 2);
    }

    #[test]
    fn load_config_expands_environment_variables_at_runtime_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cgen.xml");
        std::env::set_var(
            "HAZE_CGEN_TEST_OUTPUT",
            "rtmp://example.invalid/live/super-secret-key",
        );
        std::fs::write(
            &path,
            r#"
<cgen enabled="true">
  <feed id="CAP-IT-ALL" enabled="true">
    <programInput url="udp://127.0.0.1:5000" format="mpegts"/>
    <priorityInput feed_id="CAP-IT-ALL" audio_source="priority"/>
    <programOutput url="${HAZE_CGEN_TEST_OUTPUT}" format="rtmp" vcodec="h264" acodec="aac"/>
    <video width="1920" height="1080" fps="30000/1001"/>
    <audio idle="source" alert_mode="replace"/>
    <ladder>
      <video id="hd" enabled="true" width="1920" height="1080"/>
      <audio id="stereo" enabled="true" channels="2"/>
    </ladder>
  </feed>
</cgen>"#,
        )
        .expect("write config");

        let config = load_config(&path).expect("config");
        let feeds = config.enabled_feeds().expect("feeds");
        assert_eq!(feeds[0].program_output_url(), "${HAZE_CGEN_TEST_OUTPUT}");
        assert_eq!(
            feeds[0].resolved_program_output_url(),
            "rtmp://example.invalid/live/super-secret-key"
        );
        std::env::remove_var("HAZE_CGEN_TEST_OUTPUT");
    }

    #[test]
    fn malformed_environment_references_remain_literal() {
        assert_eq!(expand_env_vars("udp://${UNCLOSED"), "udp://${UNCLOSED");
        assert_eq!(expand_env_vars("udp://${}"), "udp://${}");
        assert_eq!(expand_env_vars("udp://${BAD-NAME}"), "udp://${BAD-NAME}");
        assert_eq!(expand_env_vars("udp://$9INVALID"), "udp://$9INVALID");
        assert_eq!(expand_env_vars("price-$"), "price-$");
    }

    #[test]
    fn oversized_managed_configuration_is_rejected_before_parsing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cgen.xml");
        std::fs::write(
            &path,
            vec![b' '; usize::try_from(MAX_CGEN_CONFIG_BYTES + 1).expect("test size")],
        )
        .expect("write oversized config");

        let error = load_config(&path).expect_err("oversized config must fail");
        assert!(error.to_string().contains("safety limit"));
    }

    #[test]
    fn redacts_configured_endpoint_values_from_status() {
        let config: CgenConfig = quick_xml::de::from_str(
            r#"
<cgen enabled="true">
  <feed id="CAP-IT-ALL" enabled="true">
    <programInput url="http://user:pass@example.invalid/input.ts" format="mpegts"/>
    <priorityInput feed_id="CAP-IT-ALL" audio_source="priority"/>
    <programOutput url="rtmp://publish.example.invalid/live/secret-key" format="rtmp" vcodec="h264" acodec="ac3"/>
    <video width="1920" height="1080" fps="30000/1001"/>
    <audio idle="source" alert_mode="replace"/>
    <ladder>
      <video id="hd" enabled="true" width="1920" height="1080"/>
      <audio id="stereo" enabled="true" channels="2"/>
    </ladder>
  </feed>
</cgen>"#,
        )
        .expect("parse");
        let feed = config.feeds.into_iter().next().expect("feed");
        let mut status = serde_json::json!({
            "pipeline_description": "uridecodebin uri=\"http://user:pass@example.invalid/input.ts\" ! rtmpsink location=\"rtmp://publish.example.invalid/live/secret-key\"",
            "nested": {
                "output": "rtmp://publish.example.invalid/live/secret-key"
            }
        });

        redact_feed_endpoint_status(&feed, &mut status);
        let rendered = status.to_string();
        assert!(!rendered.contains("user:pass"));
        assert!(!rendered.contains("secret-key"));
        assert!(rendered.contains("http://example.invalid/..."));
        assert!(rendered.contains("rtmp://publish.example.invalid/..."));
    }

    #[test]
    fn validates_supported_sizes() {
        assert!(allowed_video_size(720, 480));
        assert!(allowed_video_size(720, 540));
        assert!(allowed_video_size(720, 576));
        assert!(allowed_video_size(1280, 720));
        assert!(allowed_video_size(1920, 1080));
        assert!(!allowed_video_size(1024, 576));
    }

    #[test]
    fn parses_program_priority_and_output_sections() {
        let xml = r##"
<cgen enabled="true">
  <feed id="CAP-IT-ALL" name="CAP CGEN" enabled="true">
    <programInput url="udp://239.0.0.1:9000?reuse=1" format="mpegts"/>
    <priorityInput feed_id="CAP-IT-ALL" audio_source="both" format="priority-audio"/>
    <programOutput url="udp://239.0.0.2:9001?pkt_size=1316" format="mpegts" vcodec="libx264" acodec="aac"/>
    <video width="1920" height="1080" fps="30000/1001" interlaced="true" field_order="tff" standard="atsc"/>
    <audio idle="source" alert_mode="replace" mute_standby_routine="false"/>
    <ladder>
      <video id="hd" enabled="auto" width="1920" height="1080" fps="30000/1001" interlaced="true" field_order="tff" standard="atsc" vcodec="mpeg2video" bitrate_kbps="12000" program="1" video_pid="256" pmt_pid="4096"/>
      <audio id="stereo" enabled="true" channels="2" acodec="ac3" bitrate_kbps="192" language="eng"/>
    </ladder>
    <banner mode="auto" ticker_height="128" font="Arial" font_weight="regular" font_size="58" scroll_speed="5" background_enabled="true"/>
    <graphics font="Arial" font_weight="regular" font_size="58"/>
    <clock enabled="true" x="48" y="48"/>
    <text enabled="true" x="48" y="96">Hello</text>
    <state mode="overlay" smpte_bars="true"/>
    <sync hard_reset_ms="500" max_audio_frames_per_video="10" source_buffer_ms="400" reconnect_initial_ms="250" reconnect_max_ms="5000" status_interval_ms="750"/>
  </feed>
</cgen>"##;
        let parsed: CgenConfig = quick_xml::de::from_str(xml).expect("parse");
        let feeds = parsed.enabled_feeds().expect("feeds");

        assert_eq!(feeds.len(), 1);
        assert_eq!(feeds[0].program_input_url(), "udp://239.0.0.1:9000?reuse=1");
        assert_eq!(
            feeds[0].program_output_url(),
            "udp://239.0.0.2:9001?pkt_size=1316"
        );
        assert_eq!(feeds[0].priority_input.feed_id, "CAP-IT-ALL");
        assert_eq!(feeds[0].priority_input.audio_source, "both");
        assert!(feeds[0].priority_input.priority_audio_enabled());
        assert!(feeds[0].priority_input.routine_audio_enabled());
        assert!(!feeds[0].audio.mute_standby_routine);
        assert_eq!(feeds[0].video.width, 1920);
        assert!(feeds[0].video.interlaced);
        assert_eq!(feeds[0].video.field_order, "tff");
        assert_eq!(feeds[0].video.standard, "atsc");
        assert_eq!(feeds[0].banner.scroll_speed, 5);
        assert_eq!(feeds[0].banner.font_weight, "regular");
        assert_eq!(feeds[0].graphics.font_weight, "regular");
        assert!(feeds[0].clock.enabled);
        assert!(feeds[0].state.smpte_bars);
        assert_eq!(feeds[0].sync.hard_reset_ms, 500);
        assert_eq!(feeds[0].sync.max_audio_frames_per_video, 10);
        assert_eq!(feeds[0].sync.source_buffer_ms, 400);
        assert_eq!(feeds[0].sync.reconnect_initial_ms, 250);
        assert_eq!(feeds[0].sync.reconnect_max_ms, 5000);
        assert_eq!(feeds[0].sync.status_interval_ms, 750);
    }

    #[test]
    fn wildcard_feed_matches_any_feed() {
        let feed = FeedConfig {
            id: "*".to_string(),
            name: String::new(),
            enabled: true,
            program_input: EndpointConfig {
                url: "udp://in".to_string(),
                format: "mpegts".to_string(),
                vcodec: String::new(),
                acodec: String::new(),
                video_bitrate_kbps: None,
                audio_bitrate_kbps: None,
                ..Default::default()
            },
            priority_input: PriorityInputConfig::default(),
            program_output: EndpointConfig {
                url: "udp://out".to_string(),
                format: "mpegts".to_string(),
                vcodec: String::new(),
                acodec: String::new(),
                video_bitrate_kbps: None,
                audio_bitrate_kbps: None,
                ..Default::default()
            },
            program: Default::default(),
            priority: Default::default(),
            media: Default::default(),
            presentation: Default::default(),
            video: VideoConfig {
                width: 1280,
                height: 720,
                fps: "source".to_string(),
                interlaced: false,
                field_order: "tff".to_string(),
                standard: String::new(),
            },
            audio: AudioConfig {
                idle: "source".to_string(),
                alert_mode: "replace".to_string(),
                mute_standby_routine: true,
                ..Default::default()
            },
            ladder: LadderConfig {
                videos: vec![VideoRenditionConfig {
                    id: "hd".to_string(),
                    enabled: "true".to_string(),
                    width: 1280,
                    height: 720,
                    fps: "source".to_string(),
                    interlaced: false,
                    field_order: "tff".to_string(),
                    standard: String::new(),
                    vcodec: "mpeg2video".to_string(),
                    bitrate_kbps: Some(8_000),
                    program: Some(1),
                    video_pid: Some(0x100),
                    pmt_pid: Some(0x1000),
                }],
                audios: vec![AudioRenditionConfig {
                    id: "stereo".to_string(),
                    enabled: "true".to_string(),
                    channels: 2,
                    acodec: "ac3".to_string(),
                    bitrate_kbps: Some(192),
                    language: "eng".to_string(),
                    program: Some(1),
                    audio_pid: Some(0x101),
                    pmt_pid: Some(0x1000),
                }],
            },
            banner: BannerConfig::default(),
            graphics: GraphicsConfig::default(),
            clock: ClockConfig::default(),
            text: TextConfig::default(),
            state: StateConfig::default(),
            standby: StandbyConfig::default(),
            sync: SyncConfig::default(),
            alert: Default::default(),
            ancillary: Default::default(),
            compositor: Default::default(),
            program_mapping: Default::default(),
            outputs: Default::default(),
            encoder: Default::default(),
            output_encoders: Default::default(),
        };
        assert!(feed.matches_feed("sk-0001"));
        assert!(feed.matches_feed("CAP-IT-ALL"));
    }

    #[test]
    fn parses_mpts_output_ladder() {
        let xml = r#"
<cgen enabled="true">
  <feed id="CAP-IT-ALL" enabled="true">
    <programInput url="udp://239.0.0.1:9000" format="mpegts"/>
    <priorityInput feed_id="*" format="priority-audio"/>
    <programOutput url="udp://239.0.0.2:9001?pkt_size=1316" format="mpegts" vcodec="mpeg2video" acodec="ac3"/>
    <video width="1920" height="1080" fps="30000/1001" interlaced="true" field_order="tff" standard="atsc"/>
    <audio idle="source" alert_mode="replace"/>
    <ladder>
      <video id="hd" enabled="auto" width="1920" height="1080" fps="30000/1001" interlaced="true" field_order="tff" standard="atsc" bitrate_kbps="12000" program="1" video_pid="256" pmt_pid="4096"/>
      <video id="p720" enabled="false" width="1280" height="720" fps="30000/1001" bitrate_kbps="8000" program="2"/>
      <audio id="surround_51" enabled="true" channels="6" acodec="ac3" bitrate_kbps="384" language="eng"/>
      <audio id="stereo" enabled="true" channels="2" acodec="ac3" bitrate_kbps="192" language="eng"/>
    </ladder>
  </feed>
</cgen>"#;
        let parsed: CgenConfig = quick_xml::de::from_str(xml).expect("parse");
        let feeds = parsed.enabled_feeds().expect("feeds");
        let videos = feeds[0].enabled_video_renditions(1920, 1080);
        let audios = feeds[0].enabled_audio_renditions();

        assert_eq!(videos.len(), 1);
        assert_eq!(videos[0].id, "hd");
        assert_eq!(videos[0].program_number(0), 1);
        assert_eq!(videos[0].video_pid(0), 256);
        assert_eq!(audios.len(), 2);
        assert_eq!(audios[0].channels(), 6);
        assert_eq!(audios[1].channels(), 2);
    }

    #[test]
    fn source_program_mapping_uses_automatic_ids_until_input_is_detected() {
        let xml = r#"
<cgen enabled="true">
  <feed id="SOURCE-MAP" enabled="true">
    <programInput url="udp://239.0.0.1:9000" format="mpegts"/>
    <priorityInput feed_id="SOURCE-MAP" format="priority-audio"/>
    <programOutput url="udp://239.0.0.2:9001" format="mpegts" vcodec="mpeg2video" acodec="ac3"/>
    <video width="720" height="480" fps="30000/1001" interlaced="true" field_order="tff" standard="atsc"/>
    <audio idle="source" alert_mode="replace"/>
    <ladder>
      <video id="hd" enabled="true" width="720" height="480" fps="30000/1001" interlaced="true" field_order="tff" standard="atsc" program="1" video_pid="512" pmt_pid="4097"/>
      <audio id="stereo" enabled="true" channels="2" acodec="ac3" language="eng" program="1" audio_pid="513" pmt_pid="4097"/>
    </ladder>
    <programMapping mode="source" transport_stream_id="9">
      <program number="4" service_name="Ignored IDs" provider_name="Haze" pmt_pid="5000" video_pid="5001">
        <audio track_id="stereo" pid="5002"/>
      </program>
    </programMapping>
  </feed>
</cgen>"#;
        let parsed: CgenConfig = quick_xml::de::from_str(xml).expect("parse");
        let mut feeds = parsed.enabled_feeds().expect("feeds");
        let feed = feeds.remove(0);
        assert_eq!(
            feed.program_mapping_mode().expect("mapping mode"),
            ProgramMappingMode::Source
        );

        let pipeline = feed.pipeline_spec().expect("pipeline");
        let map = pipeline.program_map.expect("program map");
        assert_eq!(map.transport_stream_id, 9);
        assert_eq!(map.programs[0].program_number.get(), 1);
        assert_eq!(map.programs[0].pmt_pid, PidAssignment::Auto);
        assert_eq!(map.programs[0].video_pid, Some(PidAssignment::Auto));
        assert_eq!(map.programs[0].audio[0].pid, PidAssignment::Auto);
    }

    #[test]
    fn parses_standby_output_settings() {
        let xml = r#"
<cgen enabled="true">
  <feed id="standby" enabled="true">
    <programInput url="udp://in" format="mpegts"/>
    <programOutput url="udp://out" format="mpegts" vcodec="mpeg2video" acodec="ac3"/>
    <video width="1920" height="1080" fps="30000/1001" interlaced="true" field_order="tff" standard="atsc"/>
    <audio idle="source" alert_mode="replace"/>
    <ladder>
      <video id="hd" enabled="auto" width="1920" height="1080" fps="30000/1001" interlaced="true" field_order="tff" standard="atsc" vcodec="mpeg2video" bitrate_kbps="12000" program="1" video_pid="256" pmt_pid="4096"/>
      <audio id="stereo" enabled="true" channels="2" acodec="ac3" bitrate_kbps="192" language="eng"/>
    </ladder>
    <standby mode="smpte" text="EAS Details Channel" font_size="72" y_percent="10"/>
  </feed>
</cgen>"#;
        let parsed: CgenConfig = quick_xml::de::from_str(xml).expect("parse");
        let feeds = parsed.enabled_feeds().expect("feeds");

        assert_eq!(feeds[0].standby.mode, "smpte");
        assert_eq!(feeds[0].standby.text, "EAS Details Channel");
        assert_eq!(feeds[0].standby.font_size, 72);
        assert_eq!(feeds[0].standby.y_percent, 10);
    }

    #[test]
    fn missing_ladder_outputs_are_invalid() {
        let xml = r#"
<cgen enabled="true">
  <feed id="missing-ladder" enabled="true">
    <programInput url="udp://in" format="mpegts"/>
    <programOutput url="udp://out" format="mpegts" vcodec="mpeg2video" acodec="ac3" video_bitrate_kbps="12000" audio_bitrate_kbps="192"/>
    <video width="1920" height="1080" fps="30000/1001" interlaced="true" field_order="tff" standard="atsc"/>
    <audio idle="source" alert_mode="replace"/>
  </feed>
</cgen>"#;
        let parsed: CgenConfig = quick_xml::de::from_str(xml).expect("parse");
        let err = parsed
            .enabled_feeds()
            .expect_err("missing ladder should fail");

        assert!(err.to_string().contains("no enabled video renditions"));
    }

    #[test]
    fn explicit_outputs_reject_unsupported_codec_and_destination_tokens() {
        let mut output = valid_explicit_output();
        output.video_codec = "vp9".to_string();
        assert!(output_spec(&output)
            .expect_err("unsupported video codec")
            .to_string()
            .contains("unsupported output video codec"));

        output = valid_explicit_output();
        output.destination = "websocket".to_string();
        assert!(output_spec(&output)
            .expect_err("unsupported destination")
            .to_string()
            .contains("unsupported output destination"));
    }

    #[test]
    fn explicit_outputs_reject_invalid_rate_control_values() {
        let mut output = valid_explicit_output();
        output.rate_control = "vbr".to_string();
        output.video_max_bitrate_kbps = output.video_bitrate_kbps - 1;
        assert!(output_spec(&output)
            .expect_err("VBR maximum below target")
            .to_string()
            .contains("at least the target"));

        output = valid_explicit_output();
        output.rate_control = "constant-quality".to_string();
        assert!(output_spec(&output)
            .expect_err("unsupported rate control")
            .to_string()
            .contains("unsupported output rate control"));
    }

    #[test]
    fn domain_token_parsers_reject_unknown_values() {
        assert!(pass_policy("maybe").is_err());
        assert!(demux_hint("dash").is_err());
        assert!(scan_mode(true, "sideways").is_err());
        assert!(channel_layout("7.1").is_err());
        let invalid = EndpointConfig {
            deinterlace_algorithm: "blur-o-matic".to_string(),
            ..Default::default()
        };
        assert!(deinterlace_spec(&invalid).is_err());
    }

    #[test]
    fn deinterlace_defaults_and_explicit_tokens_are_typed() {
        assert_eq!(
            deinterlace_spec(&EndpointConfig::default()).expect("defaults"),
            DeinterlaceSpec::default()
        );
        let endpoint = EndpointConfig {
            deinterlace_algorithm: "motion_adaptive".to_string(),
            deinterlace_backend: "vaapi".to_string(),
            deinterlace_cadence: "frame".to_string(),
            deinterlace_parity: "auto".to_string(),
            ..Default::default()
        };

        assert_eq!(
            deinterlace_spec(&endpoint).expect("explicit deinterlace spec"),
            DeinterlaceSpec {
                algorithm: DeinterlaceAlgorithm::MotionAdaptive,
                backend: DeinterlaceBackend::Vaapi,
                cadence: DeinterlaceCadence::Frame,
                parity: DeinterlaceParity::Auto,
            }
        );
    }

    #[test]
    fn enabled_feed_rejects_unknown_program_input_type() {
        let xml = r#"<cgen schema_version="2" enabled="true">
  <feed id="CAP" enabled="true">
    <programInput type="mystery" url="udp://239.0.0.1:9000" format="mpegts"/>
    <priorityInput feed_id="CAP"/>
    <programOutput url="udp://239.0.0.2:9000" format="mpegts" vcodec="h264" acodec="aac"/>
    <video width="720" height="480" fps="30000/1001"/>
    <audio topology="force_layout" force_layout="stereo"/>
    <ladder>
      <video id="sd" enabled="true" width="720" height="480" bitrate_kbps="4000" program="1"/>
      <audio id="main" enabled="true" channels="2" bitrate_kbps="192" program="1"/>
    </ladder>
  </feed>
</cgen>"#;
        let parsed: CgenConfig = quick_xml::de::from_str(xml).expect("parse config");
        assert!(parsed
            .enabled_feeds()
            .expect_err("unknown program input type")
            .to_string()
            .contains("unsupported program input type"));
    }

    fn valid_explicit_output() -> OutputConfig {
        OutputConfig {
            id: "primary".to_string(),
            enabled: true,
            destination: "rtmp".to_string(),
            url: "rtmp://example.invalid/live".to_string(),
            video_codec: "h264".to_string(),
            rate_control: "cbr".to_string(),
            video_bitrate_kbps: 4_000,
            video_max_bitrate_kbps: 4_000,
            gop_frames: 60,
            audio_codec: "aac".to_string(),
            audio_bitrate_kbps: 192,
            sample_rate: 48_000,
            ..OutputConfig::default()
        }
    }
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            hard_reset_ms: default_hard_reset_ms(),
            max_audio_frames_per_video: default_max_audio_frames_per_video(),
            source_buffer_ms: default_source_buffer_ms(),
            reconnect_initial_ms: default_reconnect_initial_ms(),
            reconnect_max_ms: default_reconnect_max_ms(),
            status_interval_ms: default_status_interval_ms(),
        }
    }
}

impl Default for BannerConfig {
    fn default() -> Self {
        Self {
            mode: auto_mode(),
            ticker_height: default_ticker_height(),
            font: String::new(),
            font_weight: regular_font_weight(),
            font_size: default_font_size(),
            scroll_speed: default_scroll_speed(),
            scroll_repeat_mode: default_scroll_repeat_mode(),
            after_eom_repeats: 0,
            fixed_repeats: default_fixed_repeats(),
            background_enabled: true,
        }
    }
}

impl Default for AudioConfig {
    fn default() -> Self {
        Self {
            idle: source_audio(),
            alert_mode: replace_audio(),
            mute_standby_routine: true,
            topology: default_audio_topology(),
            force_layout: default_forced_audio_layout(),
            idle_program_gain_db: default_idle_program_gain(),
            alert_program_gain_db: default_alert_program_gain(),
            alert_gain_db: default_alert_gain(),
            transition_ms: default_audio_transition_ms(),
        }
    }
}

impl Default for CompositorConfig {
    fn default() -> Self {
        Self {
            alert_scene_id: default_alert_scene_id(),
            engine: default_compositor_engine(),
        }
    }
}

impl Default for GraphicsConfig {
    fn default() -> Self {
        Self {
            font: String::new(),
            font_weight: regular_font_weight(),
            font_size: default_font_size(),
            banner_height: default_ticker_height(),
        }
    }
}

impl Default for ClockConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            format: String::new(),
            x: default_text_x(),
            y: default_clock_y(),
            font_size: default_clock_font_size(),
            color: String::new(),
        }
    }
}

impl Default for TextConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            x: default_text_x(),
            y: default_text_y(),
            font_size: default_insert_font_size(),
            color: String::new(),
            content: String::new(),
        }
    }
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            mode: release_mode(),
            smpte_bars: false,
            sunny_cat: false,
            updated_at: String::new(),
        }
    }
}

impl Default for StandbyConfig {
    fn default() -> Self {
        Self {
            mode: standby_banner_mode(),
            text: default_standby_text(),
            font_size: 0,
            y_percent: default_standby_y_percent(),
        }
    }
}
