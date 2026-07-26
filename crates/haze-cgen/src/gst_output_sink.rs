//! Per-destination GStreamer encoder, muxer, and sink pipelines.
//!
//! This adapter is deliberately downstream-only. It accepts retained CPU BGRx
//! frames and interleaved `f32` PCM from [`crate::output_workers`], creates one
//! GStreamer pipeline per output worker, and never exposes resolved endpoint
//! locations through status or errors.

use std::any::Any;
use std::collections::BTreeSet;
use std::fmt;
use std::mem::size_of;
use std::num::{NonZeroU16, NonZeroU32};
use std::sync::Arc;
use std::time::Duration;

use gst::prelude::*;
use gstreamer as gst;
use gstreamer_app as gst_app;
use thiserror::Error;
use tracing::warn;

use crate::architecture::{
    AudioCodec, AudioCodecPolicy, EncoderOutputSpec, OutputDestination, RateControl,
    ResolvedProgramMapSpec, VideoCodec,
};
use crate::output_workers::{
    AudioPacket, AudioPayload, OutputCompatibilityValidator, OutputFailure, OutputFailureCode,
    OutputSink, OutputSinkFactory, SinkFuture, StandardOutputCompatibility, VideoFrame,
    VideoFrameHandle,
};

const VIDEO_SOURCE_NAME: &str = "video_src";
const AUDIO_SOURCE_NAME: &str = "audio_src";
const MUX_NAME: &str = "mux";
const MAX_RESOLVED_LOCATION_LEN: usize = 4096;
const OUTPUT_QUEUE_TIME_NS: u64 = 500_000_000;
const FILE_FINALIZE_GRACE: Duration = Duration::from_millis(750);

/// A retained, system-memory BGRx frame accepted by the GStreamer adapter.
#[derive(Clone)]
pub(crate) struct CpuBgrxFrameHandle {
    width: NonZeroU32,
    height: NonZeroU32,
    stride: NonZeroU32,
    pixels: Arc<[u8]>,
}

impl CpuBgrxFrameHandle {
    /// Builds a validated retained frame handle.
    pub(crate) fn new(
        width: NonZeroU32,
        height: NonZeroU32,
        stride: NonZeroU32,
        pixels: Arc<[u8]>,
    ) -> Result<Self, CpuBgrxFrameError> {
        let row_bytes = width
            .get()
            .checked_mul(4)
            .ok_or(CpuBgrxFrameError::DimensionsTooLarge)?;
        if stride.get() < row_bytes {
            return Err(CpuBgrxFrameError::StrideTooSmall);
        }
        let required = usize::try_from(stride.get())
            .ok()
            .and_then(|stride| {
                usize::try_from(height.get().saturating_sub(1))
                    .ok()
                    .and_then(|rows| stride.checked_mul(rows))
            })
            .and_then(|prefix| {
                usize::try_from(row_bytes)
                    .ok()
                    .and_then(|row| prefix.checked_add(row))
            })
            .ok_or(CpuBgrxFrameError::DimensionsTooLarge)?;
        if pixels.len() < required {
            return Err(CpuBgrxFrameError::BufferTooSmall);
        }
        Ok(Self {
            width,
            height,
            stride,
            pixels,
        })
    }

    pub(crate) fn width(&self) -> NonZeroU32 {
        self.width
    }

    pub(crate) fn height(&self) -> NonZeroU32 {
        self.height
    }

    pub(crate) fn stride(&self) -> NonZeroU32 {
        self.stride
    }

    pub(crate) fn pixels(&self) -> &[u8] {
        &self.pixels
    }

    fn tightly_packed(&self) -> Result<Vec<u8>, CpuBgrxFrameError> {
        let row_bytes = usize::try_from(self.width.get())
            .ok()
            .and_then(|width| width.checked_mul(4))
            .ok_or(CpuBgrxFrameError::DimensionsTooLarge)?;
        let height = usize::try_from(self.height.get())
            .map_err(|_| CpuBgrxFrameError::DimensionsTooLarge)?;
        let stride = usize::try_from(self.stride.get())
            .map_err(|_| CpuBgrxFrameError::DimensionsTooLarge)?;
        let output_len = row_bytes
            .checked_mul(height)
            .ok_or(CpuBgrxFrameError::DimensionsTooLarge)?;
        let mut output = vec![0_u8; output_len];
        for (source, destination) in self
            .pixels
            .chunks(stride)
            .take(height)
            .zip(output.chunks_mut(row_bytes))
        {
            destination.copy_from_slice(
                source
                    .get(..row_bytes)
                    .ok_or(CpuBgrxFrameError::BufferTooSmall)?,
            );
        }
        Ok(output)
    }
}

impl fmt::Debug for CpuBgrxFrameHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CpuBgrxFrameHandle")
            .field("width", &self.width)
            .field("height", &self.height)
            .field("stride", &self.stride)
            .field("byte_len", &self.pixels.len())
            .finish()
    }
}

impl VideoFrameHandle for CpuBgrxFrameHandle {
    fn backend(&self) -> &'static str {
        "cpu_bgrx"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CpuBgrxFrameError {
    #[error("BGRx frame dimensions exceed addressable memory")]
    DimensionsTooLarge,
    #[error("BGRx frame stride is smaller than one packed row")]
    StrideTooSmall,
    #[error("BGRx frame buffer is smaller than its dimensions and stride require")]
    BufferTooSmall,
}

/// GStreamer MPEG-TS mapping features used by this adapter.
///
/// Transport stream ID, service metadata, and SCTE-35 section events require
/// the separate GStreamer MPEG-TS section API. They remain explicit unsupported
/// capabilities until that API is added and verified on all bundle targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GstProgramMapCapabilities {
    pub(crate) elementary_pids: bool,
    pub(crate) program_numbers: bool,
    pub(crate) pmt_pids: bool,
    pub(crate) pcr_pids: bool,
    pub(crate) transport_stream_id: bool,
    pub(crate) service_metadata: bool,
    pub(crate) timestamped_scte35_events: bool,
    pub(crate) generated_scte35_cues: bool,
}

const GST_PROGRAM_MAP_CAPABILITIES: GstProgramMapCapabilities = GstProgramMapCapabilities {
    elementary_pids: true,
    program_numbers: true,
    pmt_pids: true,
    pcr_pids: true,
    transport_stream_id: false,
    service_metadata: false,
    timestamped_scte35_events: false,
    generated_scte35_cues: false,
};

/// Builds fresh GStreamer output sinks for isolated output workers.
pub(crate) struct GstOutputSinkFactory {
    program_map: Option<Arc<ResolvedProgramMapSpec>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GstOutputPreflight {
    pub(crate) output_id: String,
    pub(crate) destination_kind: &'static str,
    pub(crate) required_elements: Vec<&'static str>,
}

impl GstOutputSinkFactory {
    pub(crate) fn new(program_map: Option<ResolvedProgramMapSpec>) -> Self {
        Self {
            program_map: program_map.map(Arc::new),
        }
    }

    pub(crate) fn from_shared_program_map(
        program_map: Option<Arc<ResolvedProgramMapSpec>>,
    ) -> Self {
        Self { program_map }
    }

    pub(crate) const fn program_map_capabilities() -> GstProgramMapCapabilities {
        GST_PROGRAM_MAP_CAPABILITIES
    }

    pub(crate) fn preflight(
        &self,
        output: &EncoderOutputSpec,
    ) -> anyhow::Result<GstOutputPreflight> {
        StandardOutputCompatibility
            .validate(output)
            .map_err(|error| {
                anyhow::anyhow!(
                    "output {} ({}) is incompatible: {error}",
                    output.id,
                    output.destination.kind()
                )
            })?;
        gst::init().map_err(|_| anyhow::anyhow!("failed to initialize GStreamer"))?;
        let activated = ActivatedOutputSpec::from_output(output).map_err(|error| {
            anyhow::anyhow!(
                "output {} ({}) failed activation preflight: {}",
                output.id,
                output.destination.kind(),
                error.safe_reason()
            )
        })?;
        let plan = OutputPipelinePlan::build(
            &activated,
            output
                .destination
                .is_mpeg_ts()
                .then_some(self.program_map.as_deref())
                .flatten(),
            RtmpSinkElement::available(),
        )
        .map_err(|error| {
            anyhow::anyhow!(
                "output {} ({}) failed pipeline preflight: {}",
                output.id,
                output.destination.kind(),
                error.safe_reason()
            )
        })?;
        let mut required_elements = plan
            .required_elements
            .iter()
            .map(|element| element.name)
            .collect::<Vec<_>>();
        required_elements.sort_unstable();
        let missing = required_elements
            .iter()
            .copied()
            .filter(|element| gst::ElementFactory::find(*element).is_none())
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            anyhow::bail!(
                "output {} ({}) is missing required GStreamer element(s): {}",
                output.id,
                output.destination.kind(),
                missing.join(", ")
            );
        }
        Ok(GstOutputPreflight {
            output_id: output.id.to_string(),
            destination_kind: output.destination.kind(),
            required_elements,
        })
    }
}

impl fmt::Debug for GstOutputSinkFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GstOutputSinkFactory")
            .field("has_program_map", &self.program_map.is_some())
            .field("program_map_capabilities", &GST_PROGRAM_MAP_CAPABILITIES)
            .finish()
    }
}

impl OutputSinkFactory for GstOutputSinkFactory {
    fn create(&self, output: Arc<EncoderOutputSpec>) -> Result<Box<dyn OutputSink>, OutputFailure> {
        if StandardOutputCompatibility.validate(&output).is_err() {
            return Err(OutputFailure::terminal(OutputFailureCode::Protocol));
        }
        gst::init().map_err(|_| OutputFailure::terminal(OutputFailureCode::Factory))?;
        let activated = ActivatedOutputSpec::from_output(&output).map_err(|error| {
            warn!(
                output_id = %output.id,
                destination = output.destination.kind(),
                reason = error.safe_reason(),
                "rejecting GStreamer output activation"
            );
            error.failure()
        })?;
        let rtmp_sink = RtmpSinkElement::available();
        let plan = OutputPipelinePlan::build(
            &activated,
            output
                .destination
                .is_mpeg_ts()
                .then_some(self.program_map.as_deref())
                .flatten(),
            rtmp_sink,
        )
        .map_err(|error| {
            warn!(
                output_id = %output.id,
                destination = output.destination.kind(),
                reason = error.safe_reason(),
                "rejecting GStreamer output pipeline plan"
            );
            error.failure()
        })?;
        Ok(Box::new(GstOutputSink::new(plan)))
    }
}

#[derive(Clone)]
struct ActivatedOutputSpec {
    destination: ActivatedDestination,
    video_codec: VideoCodec,
    rate_control: RateControl,
    gop_frames: NonZeroU32,
    audio_codec: AudioCodec,
    audio_bitrate_kbps: NonZeroU32,
    audio_sample_rate: NonZeroU32,
}

#[derive(Clone)]
enum ActivatedDestination {
    MpegTsUdp {
        endpoint: UdpEndpoint,
    },
    MpegTsSrt {
        location: String,
        latency_ms: u32,
    },
    Rtp {
        video_endpoint: UdpEndpoint,
        audio_endpoints: Vec<UdpEndpoint>,
    },
    Rtmp {
        location: String,
    },
    File {
        location: String,
        container: FileMux,
    },
}

impl ActivatedOutputSpec {
    fn from_output(output: &EncoderOutputSpec) -> Result<Self, PlanError> {
        let destination = match &output.destination {
            OutputDestination::MpegTsUdp { location } => {
                let location = expand_env_vars(location);
                validate_resolved_location(&location)?;
                ActivatedDestination::MpegTsUdp {
                    endpoint: UdpEndpoint::parse(&location, &["udp"])
                        .ok_or(PlanError::InvalidUdpEndpoint)?,
                }
            }
            OutputDestination::MpegTsSrt {
                location,
                latency_ms,
            } => {
                let location = expand_env_vars(location);
                validate_resolved_location(&location)?;
                if !has_uri_scheme(&location, &["srt"]) {
                    return Err(PlanError::InvalidSrtEndpoint);
                }
                ActivatedDestination::MpegTsSrt {
                    location,
                    latency_ms: *latency_ms,
                }
            }
            OutputDestination::Rtp {
                video_location,
                audio_locations,
            } => {
                let video_location = expand_env_vars(video_location);
                validate_resolved_location(&video_location)?;
                let video_endpoint = UdpEndpoint::parse(&video_location, &["udp", "rtp"])
                    .ok_or(PlanError::InvalidRtpEndpoint)?;
                let mut audio_endpoints = Vec::with_capacity(audio_locations.len());
                for location in audio_locations {
                    let location = expand_env_vars(location);
                    validate_resolved_location(&location)?;
                    audio_endpoints.push(
                        UdpEndpoint::parse(&location, &["udp", "rtp"])
                            .ok_or(PlanError::InvalidRtpEndpoint)?,
                    );
                }
                if audio_endpoints.is_empty() {
                    return Err(PlanError::MissingRtpAudioEndpoint);
                }
                ActivatedDestination::Rtp {
                    video_endpoint,
                    audio_endpoints,
                }
            }
            OutputDestination::Rtmp { location } => {
                let location = expand_env_vars(location);
                validate_resolved_location(&location)?;
                if !has_uri_scheme(&location, &["rtmp", "rtmps"]) {
                    return Err(PlanError::InvalidRtmpEndpoint);
                }
                ActivatedDestination::Rtmp { location }
            }
            OutputDestination::File {
                location,
                container,
            } => {
                let location = expand_env_vars(location);
                validate_resolved_location(&location)?;
                ActivatedDestination::File {
                    location,
                    container: FileMux::parse(container)?,
                }
            }
        };
        let AudioCodecPolicy::Encode(audio_codec) = output.audio.codec else {
            return Err(PlanError::MatchInputRequiresEncodedHandle);
        };
        validate_rate_control(output.video.rate_control)?;
        Ok(Self {
            destination,
            video_codec: output.video.codec,
            rate_control: output.video.rate_control,
            gop_frames: output.video.gop_frames,
            audio_codec,
            audio_bitrate_kbps: output.audio.bitrate_kbps,
            audio_sample_rate: output.audio.sample_rate,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileMux {
    MpegTs,
    Matroska,
    Flv,
    Mp4,
    Mov,
    MpegPs,
}

impl FileMux {
    fn parse(raw: &str) -> Result<Self, PlanError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "mpegts" | "mpeg_ts" | "ts" => Ok(Self::MpegTs),
            "matroska" | "mkv" => Ok(Self::Matroska),
            "flv" => Ok(Self::Flv),
            "mp4" => Ok(Self::Mp4),
            "mov" => Ok(Self::Mov),
            "mpegps" | "mpeg_ps" | "ps" => Ok(Self::MpegPs),
            _ => Err(PlanError::UnsupportedFileContainer),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DestinationClass {
    Network,
    File,
}

impl DestinationClass {
    const fn connect_failure(self) -> OutputFailure {
        match self {
            Self::Network => OutputFailure::retryable(OutputFailureCode::Network),
            Self::File => OutputFailure::terminal(OutputFailureCode::Sink),
        }
    }

    const fn write_failure(self) -> OutputFailure {
        self.connect_failure()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RtmpSinkElement {
    Rtmp2,
    Legacy,
}

impl RtmpSinkElement {
    fn available() -> Self {
        if gst::ElementFactory::find("rtmp2sink").is_some() {
            Self::Rtmp2
        } else {
            Self::Legacy
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Rtmp2 => "rtmp2sink",
            Self::Legacy => "rtmpsink",
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
enum PlanError {
    #[error("resolved output location is invalid")]
    InvalidResolvedLocation,
    #[error("UDP destination endpoint is invalid")]
    InvalidUdpEndpoint,
    #[error("SRT destination endpoint is invalid")]
    InvalidSrtEndpoint,
    #[error("RTP destination endpoint is invalid")]
    InvalidRtpEndpoint,
    #[error("RTMP destination endpoint is invalid")]
    InvalidRtmpEndpoint,
    #[error("RTP output has no audio endpoint")]
    MissingRtpAudioEndpoint,
    #[error("file output container is unsupported")]
    UnsupportedFileContainer,
    #[error("Match Input cannot consume interleaved raw PCM")]
    MatchInputRequiresEncodedHandle,
    #[error("video VBR maximum is below its target")]
    InvalidRateControl,
    #[error("MPEG-TS output has no resolved program map")]
    MissingProgramMap,
    #[error("MPEG-TS program map has no video route")]
    MissingVideoRoute,
    #[error("MPEG-TS program map has no audio route")]
    MissingAudioRoute,
    #[error("MPEG-TS PCR PID does not identify a mapped stream")]
    InvalidPcrRoute,
    #[error("GStreamer pipeline description is invalid")]
    InvalidPipeline,
}

impl PlanError {
    const fn failure(self) -> OutputFailure {
        match self {
            Self::MatchInputRequiresEncodedHandle => {
                OutputFailure::terminal(OutputFailureCode::Encoder)
            }
            Self::MissingProgramMap
            | Self::MissingVideoRoute
            | Self::MissingAudioRoute
            | Self::InvalidPcrRoute => OutputFailure::terminal(OutputFailureCode::Muxer),
            Self::InvalidPipeline => OutputFailure::terminal(OutputFailureCode::Factory),
            _ => OutputFailure::terminal(OutputFailureCode::Protocol),
        }
    }

    const fn safe_reason(self) -> &'static str {
        match self {
            Self::InvalidResolvedLocation => "invalid_resolved_location",
            Self::InvalidUdpEndpoint => "invalid_udp_endpoint",
            Self::InvalidSrtEndpoint => "invalid_srt_endpoint",
            Self::InvalidRtpEndpoint => "invalid_rtp_endpoint",
            Self::InvalidRtmpEndpoint => "invalid_rtmp_endpoint",
            Self::MissingRtpAudioEndpoint => "missing_rtp_audio_endpoint",
            Self::UnsupportedFileContainer => "unsupported_file_container",
            Self::MatchInputRequiresEncodedHandle => "match_input_requires_encoded_handle",
            Self::InvalidRateControl => "invalid_rate_control",
            Self::MissingProgramMap => "missing_program_map",
            Self::MissingVideoRoute => "missing_video_route",
            Self::MissingAudioRoute => "missing_audio_route",
            Self::InvalidPcrRoute => "invalid_pcr_route",
            Self::InvalidPipeline => "invalid_pipeline",
        }
    }
}

fn validate_rate_control(rate_control: RateControl) -> Result<(), PlanError> {
    if let RateControl::Vbr {
        target_kbps,
        max_kbps,
    } = rate_control
    {
        if max_kbps < target_kbps {
            return Err(PlanError::InvalidRateControl);
        }
    }
    Ok(())
}

fn validate_resolved_location(location: &str) -> Result<(), PlanError> {
    let location = location.trim();
    if location.is_empty()
        || location.chars().count() > MAX_RESOLVED_LOCATION_LEN
        || location
            .chars()
            .any(|character| matches!(character, '\0' | '\r' | '\n'))
    {
        return Err(PlanError::InvalidResolvedLocation);
    }
    Ok(())
}

fn has_uri_scheme(location: &str, accepted: &[&str]) -> bool {
    location.split_once("://").is_some_and(|(scheme, _)| {
        accepted
            .iter()
            .any(|candidate| scheme.eq_ignore_ascii_case(candidate))
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UdpEndpoint {
    host: String,
    port: NonZeroU16,
}

impl UdpEndpoint {
    fn parse(location: &str, schemes: &[&str]) -> Option<Self> {
        let (scheme, remainder) = location.trim().split_once("://")?;
        if !schemes
            .iter()
            .any(|candidate| scheme.eq_ignore_ascii_case(candidate))
        {
            return None;
        }
        let authority = remainder.split(['/', '?', '#']).next()?;
        let (host, port) = if let Some(ipv6) = authority.strip_prefix('[') {
            let (host, port) = ipv6.split_once("]:")?;
            (host, port)
        } else {
            authority.rsplit_once(':')?
        };
        let host = host.trim();
        if host.is_empty() || host.chars().any(char::is_whitespace) {
            return None;
        }
        Some(Self {
            host: host.to_string(),
            port: NonZeroU16::new(port.parse().ok()?)?,
        })
    }
}

fn expand_env_vars(raw: &str) -> String {
    expand_env_vars_with(raw, |name| std::env::var(name).ok())
}

fn expand_env_vars_with(raw: &str, mut lookup: impl FnMut(&str) -> Option<String>) -> String {
    let bytes = raw.as_bytes();
    let mut output = String::with_capacity(raw.len());
    let mut cursor = 0_usize;
    let mut literal_start = 0_usize;
    while cursor < bytes.len() {
        if bytes[cursor] != b'$' {
            cursor += 1;
            continue;
        }
        output.push_str(&raw[literal_start..cursor]);
        if bytes.get(cursor + 1) == Some(&b'{') {
            let name_start = cursor + 2;
            let Some(relative_end) = bytes[name_start..].iter().position(|byte| *byte == b'}')
            else {
                output.push_str(&raw[cursor..]);
                return output;
            };
            let name_end = name_start + relative_end;
            let name = &raw[name_start..name_end];
            if valid_env_name(name) {
                output.push_str(lookup(name).as_deref().unwrap_or_default());
            } else {
                output.push_str(&raw[cursor..=name_end]);
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
            output.push_str(lookup(name).as_deref().unwrap_or_default());
            cursor = name_end;
        } else {
            output.push('$');
            cursor += 1;
        }
        literal_start = cursor;
    }
    output.push_str(&raw[literal_start..]);
    output
}

fn valid_env_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first == b'_' || first.is_ascii_alphabetic())
        && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct RequiredElement {
    name: &'static str,
    missing_failure: OutputFailure,
}

impl fmt::Debug for RequiredElement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequiredElement")
            .field("name", &self.name)
            .field("missing_failure", &self.missing_failure)
            .finish()
    }
}

#[derive(Default)]
struct RequiredElements {
    names: BTreeSet<&'static str>,
    elements: Vec<RequiredElement>,
}

impl RequiredElements {
    fn add(&mut self, name: &'static str, missing_failure: OutputFailure) {
        if self.names.insert(name) {
            self.elements.push(RequiredElement {
                name,
                missing_failure,
            });
        }
    }

    fn add_common(&mut self) {
        self.add(
            "appsrc",
            OutputFailure::terminal(OutputFailureCode::Factory),
        );
        self.add("queue", OutputFailure::terminal(OutputFailureCode::Factory));
        self.add(
            "videoconvert",
            OutputFailure::terminal(OutputFailureCode::Encoder),
        );
        self.add(
            "audioconvert",
            OutputFailure::terminal(OutputFailureCode::Encoder),
        );
        self.add(
            "audioresample",
            OutputFailure::terminal(OutputFailureCode::Encoder),
        );
    }

    fn finish(self) -> Vec<RequiredElement> {
        self.elements
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GstProgramMapBinding {
    structure: String,
    requested_transport_stream_id: u16,
    requested_service_count: usize,
    requested_scte35_stream_count: usize,
    capabilities: GstProgramMapCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransportStreamRoutes {
    video_pids: Vec<u16>,
    audio_pids: Vec<u16>,
    binding: GstProgramMapBinding,
}

impl TransportStreamRoutes {
    fn from_program_map(program_map: &ResolvedProgramMapSpec) -> Result<Self, PlanError> {
        let mut fields = vec!["program_map".to_string()];
        let mut video_pids = Vec::new();
        let mut audio_pids = Vec::new();
        let mut scte35_stream_count = 0_usize;
        for program in &program_map.programs {
            let program_number = program.program_number.get();
            let mut program_streams = BTreeSet::new();
            if let Some(pid) = program.video_pid {
                video_pids.push(pid.get());
                program_streams.insert(pid.get());
                fields.push(format!("sink_{}=(int){program_number}", pid.get()));
            }
            for (_, pid) in &program.audio {
                audio_pids.push(pid.get());
                program_streams.insert(pid.get());
                fields.push(format!("sink_{}=(int){program_number}", pid.get()));
            }
            if program.scte35.is_some() {
                scte35_stream_count = scte35_stream_count.saturating_add(1);
            }
            if !program_streams.contains(&program.pcr_pid.get()) {
                return Err(PlanError::InvalidPcrRoute);
            }
            fields.push(format!(
                "PMT_{program_number}=(uint){}",
                program.pmt_pid.get()
            ));
            fields.push(format!(
                "PCR_{program_number}=(int){}",
                program.pcr_pid.get()
            ));
        }
        if video_pids.is_empty() {
            return Err(PlanError::MissingVideoRoute);
        }
        if audio_pids.is_empty() {
            return Err(PlanError::MissingAudioRoute);
        }
        Ok(Self {
            video_pids,
            audio_pids,
            binding: GstProgramMapBinding {
                structure: fields.join(","),
                requested_transport_stream_id: program_map.transport_stream_id,
                requested_service_count: program_map.programs.len(),
                requested_scte35_stream_count: scte35_stream_count,
                capabilities: GST_PROGRAM_MAP_CAPABILITIES,
            },
        })
    }
}

struct OutputPipelinePlan {
    description: String,
    required_elements: Vec<RequiredElement>,
    destination_class: DestinationClass,
    program_map: Option<GstProgramMapBinding>,
}

impl fmt::Debug for OutputPipelinePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutputPipelinePlan")
            .field("description", &"<redacted>")
            .field("required_elements", &self.required_elements)
            .field("destination_class", &self.destination_class)
            .field("program_map", &self.program_map)
            .finish()
    }
}

impl OutputPipelinePlan {
    fn build(
        output: &ActivatedOutputSpec,
        program_map: Option<&ResolvedProgramMapSpec>,
        rtmp_sink: RtmpSinkElement,
    ) -> Result<Self, PlanError> {
        let mut required = RequiredElements::default();
        required.add_common();
        let video = VideoEncodingChain::new(output);
        let audio = AudioEncodingChain::new(output);
        required.add(
            video.encoder_element,
            OutputFailure::terminal(OutputFailureCode::Encoder),
        );
        required.add(
            video.parser_element,
            OutputFailure::terminal(OutputFailureCode::Encoder),
        );
        required.add(
            audio.encoder_element,
            OutputFailure::terminal(OutputFailureCode::Encoder),
        );
        required.add(
            audio.parser_element,
            OutputFailure::terminal(OutputFailureCode::Encoder),
        );

        let (description, destination_class, program_map) = match &output.destination {
            ActivatedDestination::MpegTsUdp { endpoint } => {
                let routes = TransportStreamRoutes::from_program_map(
                    program_map.ok_or(PlanError::MissingProgramMap)?,
                )?;
                required.add(
                    "mpegtsmux",
                    OutputFailure::terminal(OutputFailureCode::Muxer),
                );
                required.add("tee", OutputFailure::terminal(OutputFailureCode::Factory));
                required.add("udpsink", OutputFailure::terminal(OutputFailureCode::Sink));
                let sink = udp_sink_fragment(endpoint, "output_sink");
                (
                    mpeg_ts_description(&video, &audio, &routes, &sink),
                    DestinationClass::Network,
                    Some(routes.binding),
                )
            }
            ActivatedDestination::MpegTsSrt {
                location,
                latency_ms,
            } => {
                let routes = TransportStreamRoutes::from_program_map(
                    program_map.ok_or(PlanError::MissingProgramMap)?,
                )?;
                required.add(
                    "mpegtsmux",
                    OutputFailure::terminal(OutputFailureCode::Muxer),
                );
                required.add("tee", OutputFailure::terminal(OutputFailureCode::Factory));
                required.add("srtsink", OutputFailure::terminal(OutputFailureCode::Sink));
                let sink = format!(
                    "srtsink name=output_sink uri={} latency={} auto-reconnect=true wait-for-connection=false sync=false async=false",
                    gst_quote(location),
                    latency_ms
                );
                (
                    mpeg_ts_description(&video, &audio, &routes, &sink),
                    DestinationClass::Network,
                    Some(routes.binding),
                )
            }
            ActivatedDestination::Rtp {
                video_endpoint,
                audio_endpoints,
            } => {
                required.add(
                    video.rtp_payloader(),
                    OutputFailure::terminal(OutputFailureCode::Protocol),
                );
                required.add(
                    audio.rtp_payloader(),
                    OutputFailure::terminal(OutputFailureCode::Protocol),
                );
                required.add("tee", OutputFailure::terminal(OutputFailureCode::Factory));
                required.add("udpsink", OutputFailure::terminal(OutputFailureCode::Sink));
                (
                    rtp_description(&video, &audio, video_endpoint, audio_endpoints),
                    DestinationClass::Network,
                    None,
                )
            }
            ActivatedDestination::Rtmp { location } => {
                required.add("flvmux", OutputFailure::terminal(OutputFailureCode::Muxer));
                required.add(
                    rtmp_sink.name(),
                    OutputFailure::terminal(OutputFailureCode::Sink),
                );
                let sink = format!(
                    "{} name=output_sink location={} sync=false async=false",
                    rtmp_sink.name(),
                    gst_quote(location)
                );
                (
                    muxed_description(&video, &audio, MuxPlan::Flv, &sink),
                    DestinationClass::Network,
                    None,
                )
            }
            ActivatedDestination::File {
                location,
                container,
            } => {
                required.add("filesink", OutputFailure::terminal(OutputFailureCode::Sink));
                let sink = format!(
                    "filesink name=output_sink location={} sync=false async=false",
                    gst_quote(location)
                );
                match container {
                    FileMux::MpegTs => {
                        let routes = TransportStreamRoutes::from_program_map(
                            program_map.ok_or(PlanError::MissingProgramMap)?,
                        )?;
                        required.add(
                            "mpegtsmux",
                            OutputFailure::terminal(OutputFailureCode::Muxer),
                        );
                        required.add("tee", OutputFailure::terminal(OutputFailureCode::Factory));
                        (
                            mpeg_ts_description(&video, &audio, &routes, &sink),
                            DestinationClass::File,
                            Some(routes.binding),
                        )
                    }
                    container => {
                        let mux = MuxPlan::from_file(*container);
                        required.add(
                            mux.element(),
                            OutputFailure::terminal(OutputFailureCode::Muxer),
                        );
                        (
                            muxed_description(&video, &audio, mux, &sink),
                            DestinationClass::File,
                            None,
                        )
                    }
                }
            }
        };
        if description.trim().is_empty() {
            return Err(PlanError::InvalidPipeline);
        }
        Ok(Self {
            description,
            required_elements: required.finish(),
            destination_class,
            program_map,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MuxPlan {
    Flv,
    Matroska,
    Mp4,
    Mov,
    MpegPs,
}

impl MuxPlan {
    fn from_file(file: FileMux) -> Self {
        match file {
            FileMux::Matroska => Self::Matroska,
            FileMux::Flv => Self::Flv,
            FileMux::Mp4 => Self::Mp4,
            FileMux::Mov => Self::Mov,
            FileMux::MpegPs => Self::MpegPs,
            FileMux::MpegTs => unreachable!("MPEG-TS uses the transport stream builder"),
        }
    }

    const fn element(self) -> &'static str {
        match self {
            Self::Flv => "flvmux",
            Self::Matroska => "matroskamux",
            Self::Mp4 | Self::Mov => "mp4mux",
            Self::MpegPs => "mpegpsmux",
        }
    }

    const fn declaration(self) -> &'static str {
        match self {
            Self::Flv => "flvmux name=mux streamable=true",
            Self::Matroska => "matroskamux name=mux streamable=false",
            Self::Mp4 | Self::Mov => "mp4mux name=mux faststart=true",
            Self::MpegPs => "mpegpsmux name=mux",
        }
    }

    const fn video_pad(self) -> &'static str {
        match self {
            Self::Flv => "mux.video",
            _ => "mux.",
        }
    }

    const fn audio_pad(self) -> &'static str {
        match self {
            Self::Flv => "mux.audio",
            _ => "mux.",
        }
    }

    const fn video_caps(self, codec: VideoCodec) -> &'static str {
        match (self, codec) {
            (Self::Flv | Self::Matroska | Self::Mp4 | Self::Mov, VideoCodec::H264) => {
                "video/x-h264,stream-format=avc,alignment=au"
            }
            (Self::Matroska | Self::Mp4 | Self::Mov, VideoCodec::H265) => {
                "video/x-h265,stream-format=hvc1,alignment=au"
            }
            _ => "",
        }
    }

    const fn audio_caps(self, codec: AudioCodec) -> &'static str {
        match (self, codec) {
            (Self::Flv | Self::Matroska | Self::Mp4 | Self::Mov, AudioCodec::Aac) => {
                "audio/mpeg,mpegversion=4,stream-format=raw"
            }
            _ => "",
        }
    }
}

struct VideoEncodingChain {
    codec: VideoCodec,
    description: String,
    encoder_element: &'static str,
    parser_element: &'static str,
}

impl VideoEncodingChain {
    fn new(output: &ActivatedOutputSpec) -> Self {
        let (target_kbps, max_kbps, constant) = rate_control_values(output.rate_control);
        let gop = output.gop_frames.get();
        let (encoder_element, parser_element, encoder) = match output.video_codec {
            VideoCodec::H264 => (
                "x264enc",
                "h264parse",
                format!(
                    "x264enc name=video_encoder tune=zerolatency speed-preset=veryfast bframes=0 bitrate={target_kbps} key-int-max={gop} option-string={}",
                    gst_quote(&format!(
                        "vbv-maxrate={max_kbps}:vbv-bufsize={max_kbps}:nal-hrd={}",
                        if constant { "cbr" } else { "vbr" }
                    ))
                ),
            ),
            VideoCodec::H265 => (
                "x265enc",
                "h265parse",
                format!(
                    "x265enc name=video_encoder tune=zerolatency speed-preset=veryfast bitrate={target_kbps} key-int-max={gop} option-string={}",
                    gst_quote(&format!(
                        "vbv-maxrate={max_kbps}:vbv-bufsize={max_kbps}:bframes=0:rc-lookahead=0"
                    ))
                ),
            ),
            VideoCodec::Mpeg2 => {
                let minimum = if constant { target_kbps } else { 0 };
                (
                    "avenc_mpeg2video",
                    "mpegvideoparse",
                    format!(
                        "avenc_mpeg2video name=video_encoder bitrate={} minrate={} maxrate={} gop-size={gop}",
                        u64::from(target_kbps) * 1_000,
                        u64::from(minimum) * 1_000,
                        u64::from(max_kbps) * 1_000
                    ),
                )
            }
        };
        Self {
            codec: output.video_codec,
            description: format!(
                "{} ! {} ! videoconvert ! video/x-raw,format=I420 ! {encoder} ! {parser_element}",
                video_appsrc_fragment(),
                queue_fragment("video_input_queue", 2)
            ),
            encoder_element,
            parser_element,
        }
    }

    const fn transport_stream_caps(&self) -> &'static str {
        match self.codec {
            VideoCodec::H264 => "video/x-h264,stream-format=byte-stream,alignment=au",
            VideoCodec::H265 => "video/x-h265,stream-format=byte-stream,alignment=au",
            VideoCodec::Mpeg2 => "video/mpeg,mpegversion=2,systemstream=false,parsed=true",
        }
    }

    const fn rtp_caps(&self) -> &'static str {
        self.transport_stream_caps()
    }

    const fn rtp_payloader(&self) -> &'static str {
        match self.codec {
            VideoCodec::H264 => "rtph264pay",
            VideoCodec::H265 => "rtph265pay",
            VideoCodec::Mpeg2 => "rtpmpvpay",
        }
    }

    const fn rtp_payloader_fragment(&self) -> &'static str {
        match self.codec {
            VideoCodec::H264 => "rtph264pay name=video_payloader config-interval=-1 pt=96",
            VideoCodec::H265 => "rtph265pay name=video_payloader config-interval=-1 pt=96",
            VideoCodec::Mpeg2 => "rtpmpvpay name=video_payloader pt=96",
        }
    }
}

struct AudioEncodingChain {
    codec: AudioCodec,
    description: String,
    encoder_element: &'static str,
    parser_element: &'static str,
}

impl AudioEncodingChain {
    fn new(output: &ActivatedOutputSpec) -> Self {
        let bitrate = u64::from(output.audio_bitrate_kbps.get()) * 1_000;
        let (encoder_element, parser_element, encoder) = match output.audio_codec {
            AudioCodec::Aac => (
                "avenc_aac",
                "aacparse",
                format!("avenc_aac name=audio_encoder bitrate={bitrate}"),
            ),
            AudioCodec::Ac3 => (
                "avenc_ac3",
                "ac3parse",
                format!("avenc_ac3 name=audio_encoder bitrate={bitrate}"),
            ),
            AudioCodec::Mp2 => (
                "avenc_mp2",
                "mpegaudioparse",
                format!("avenc_mp2 name=audio_encoder bitrate={bitrate}"),
            ),
        };
        Self {
            codec: output.audio_codec,
            description: format!(
                "{} ! {} ! audioconvert ! audioresample ! audio/x-raw,rate={},layout=interleaved ! {encoder} ! {parser_element}",
                audio_appsrc_fragment(),
                queue_fragment("audio_input_queue", 32),
                output.audio_sample_rate
            ),
            encoder_element,
            parser_element,
        }
    }

    const fn transport_stream_caps(&self) -> &'static str {
        match self.codec {
            AudioCodec::Aac => "audio/mpeg,mpegversion=4,stream-format=adts",
            AudioCodec::Ac3 => "audio/x-ac3,framed=true",
            AudioCodec::Mp2 => "audio/mpeg,mpegversion=1,layer=2,parsed=true",
        }
    }

    const fn rtp_caps(&self) -> &'static str {
        match self.codec {
            AudioCodec::Aac => "audio/mpeg,mpegversion=4,stream-format=raw",
            AudioCodec::Ac3 => "audio/x-ac3,framed=true",
            AudioCodec::Mp2 => "audio/mpeg,mpegversion=1,layer=2,parsed=true",
        }
    }

    const fn rtp_payloader(&self) -> &'static str {
        match self.codec {
            AudioCodec::Aac => "rtpmp4gpay",
            AudioCodec::Ac3 => "rtpac3pay",
            AudioCodec::Mp2 => "rtpmpapay",
        }
    }

    const fn rtp_payloader_fragment(&self) -> &'static str {
        match self.codec {
            AudioCodec::Aac => "rtpmp4gpay pt=97",
            AudioCodec::Ac3 => "rtpac3pay pt=97",
            AudioCodec::Mp2 => "rtpmpapay pt=97",
        }
    }
}

fn rate_control_values(rate_control: RateControl) -> (u32, u32, bool) {
    match rate_control {
        RateControl::Cbr { bitrate_kbps } => {
            let bitrate = bitrate_kbps.get();
            (bitrate, bitrate, true)
        }
        RateControl::Vbr {
            target_kbps,
            max_kbps,
        } => (target_kbps.get(), max_kbps.get(), false),
    }
}

fn mpeg_ts_description(
    video: &VideoEncodingChain,
    audio: &AudioEncodingChain,
    routes: &TransportStreamRoutes,
    sink: &str,
) -> String {
    let video_routes = routes
        .video_pids
        .iter()
        .map(|pid| {
            format!(
                "video_encoded_tee. ! {} ! mux.sink_{pid}",
                queue_fragment(&format!("video_pid_{pid}_queue"), 4)
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    let audio_routes = routes
        .audio_pids
        .iter()
        .map(|pid| {
            format!(
                "audio_encoded_tee. ! {} ! mux.sink_{pid}",
                queue_fragment(&format!("audio_pid_{pid}_queue"), 8)
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "mpegtsmux name={MUX_NAME} alignment=7 pat-interval=4500 pmt-interval=4500 si-interval=4500 pcr-interval=1800 ! {} ! {sink} {} ! {} ! tee name=video_encoded_tee {video_routes} {} ! {} ! tee name=audio_encoded_tee {audio_routes}",
        output_queue_fragment(),
        video.description,
        video.transport_stream_caps(),
        audio.description,
        audio.transport_stream_caps(),
    )
}

fn rtp_description(
    video: &VideoEncodingChain,
    audio: &AudioEncodingChain,
    video_endpoint: &UdpEndpoint,
    audio_endpoints: &[UdpEndpoint],
) -> String {
    let video_sink = udp_sink_fragment(video_endpoint, "video_output_sink");
    let audio_sinks = audio_endpoints
        .iter()
        .enumerate()
        .map(|(index, endpoint)| {
            format!(
                "audio_encoded_tee. ! {} ! {} ! {}",
                queue_fragment(&format!("audio_rtp_{index}_queue"), 8),
                audio.rtp_payloader_fragment(),
                udp_sink_fragment(endpoint, &format!("audio_output_sink_{index}"))
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "{} ! {} ! {} ! {} {} ! {} ! tee name=audio_encoded_tee {audio_sinks}",
        video.description,
        video.rtp_caps(),
        video.rtp_payloader_fragment(),
        video_sink,
        audio.description,
        audio.rtp_caps(),
    )
}

fn muxed_description(
    video: &VideoEncodingChain,
    audio: &AudioEncodingChain,
    mux: MuxPlan,
    sink: &str,
) -> String {
    let video_caps = optional_caps_fragment(mux.video_caps(video.codec));
    let audio_caps = optional_caps_fragment(mux.audio_caps(audio.codec));
    format!(
        "{} ! {} ! {sink} {}{video_caps} ! {} {}{audio_caps} ! {}",
        mux.declaration(),
        output_queue_fragment(),
        video.description,
        mux.video_pad(),
        audio.description,
        mux.audio_pad(),
    )
}

fn optional_caps_fragment(caps: &str) -> String {
    if caps.is_empty() {
        String::new()
    } else {
        format!(" ! {caps}")
    }
}

fn video_appsrc_fragment() -> &'static str {
    "appsrc name=video_src is-live=true block=false format=time do-timestamp=false stream-type=stream max-buffers=2 leaky-type=downstream"
}

fn audio_appsrc_fragment() -> &'static str {
    "appsrc name=audio_src is-live=true block=false format=time do-timestamp=false stream-type=stream max-buffers=32 leaky-type=downstream"
}

fn queue_fragment(name: &str, maximum_buffers: usize) -> String {
    format!(
        "queue name={name} max-size-time={OUTPUT_QUEUE_TIME_NS} max-size-buffers={maximum_buffers} max-size-bytes=0 leaky=downstream flush-on-eos=true"
    )
}

fn output_queue_fragment() -> String {
    queue_fragment("output_queue", 16)
}

fn udp_sink_fragment(endpoint: &UdpEndpoint, name: &str) -> String {
    format!(
        "udpsink name={name} host={} port={} auto-multicast=true sync=false async=false qos=true",
        gst_quote(&endpoint.host),
        endpoint.port
    )
}

fn gst_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

struct GstOutputSink {
    plan: OutputPipelinePlan,
    runtime: Option<GstOutputRuntime>,
}

impl GstOutputSink {
    fn new(plan: OutputPipelinePlan) -> Self {
        Self {
            plan,
            runtime: None,
        }
    }

    fn connect_inner(&mut self) -> Result<(), OutputFailure> {
        if self.runtime.is_some() {
            return Ok(());
        }
        for required in &self.plan.required_elements {
            if gst::ElementFactory::find(required.name).is_none() {
                return Err(required.missing_failure);
            }
        }
        let element = gst::parse::launch(&self.plan.description)
            .map_err(|_| OutputFailure::terminal(OutputFailureCode::Factory))?;
        let pipeline = element
            .downcast::<gst::Pipeline>()
            .map_err(|_| OutputFailure::terminal(OutputFailureCode::Factory))?;
        if let Some(binding) = &self.plan.program_map {
            if let Err(failure) = apply_program_map(&pipeline, binding) {
                let _ = pipeline.set_state(gst::State::Null);
                return Err(failure);
            }
        }
        let video_source = match pipeline
            .by_name(VIDEO_SOURCE_NAME)
            .and_then(|source| source.downcast::<gst_app::AppSrc>().ok())
        {
            Some(source) => source,
            None => {
                let _ = pipeline.set_state(gst::State::Null);
                return Err(OutputFailure::terminal(OutputFailureCode::Factory));
            }
        };
        let audio_source = match pipeline
            .by_name(AUDIO_SOURCE_NAME)
            .and_then(|source| source.downcast::<gst_app::AppSrc>().ok())
        {
            Some(source) => source,
            None => {
                let _ = pipeline.set_state(gst::State::Null);
                return Err(OutputFailure::terminal(OutputFailureCode::Factory));
            }
        };
        let Some(bus) = pipeline.bus() else {
            let _ = pipeline.set_state(gst::State::Null);
            return Err(OutputFailure::terminal(OutputFailureCode::Factory));
        };
        if pipeline.set_state(gst::State::Playing).is_err() {
            let _ = pipeline.set_state(gst::State::Null);
            return Err(self.plan.destination_class.connect_failure());
        }
        let runtime = GstOutputRuntime {
            pipeline,
            bus,
            video_source,
            audio_source,
            destination_class: self.plan.destination_class,
            video_caps: None,
            audio_caps: None,
            video_timeline: StreamTimeline::default(),
            audio_timeline: StreamTimeline::default(),
        };
        runtime.poll_bus(false)?;
        self.runtime = Some(runtime);
        Ok(())
    }

    fn write_video_inner(&mut self, frame: Arc<VideoFrame>) -> Result<(), OutputFailure> {
        let runtime = self
            .runtime
            .as_mut()
            .ok_or_else(|| OutputFailure::retryable(OutputFailureCode::Connect))?;
        runtime.poll_bus(false)?;
        let handle = frame
            .surface
            .as_any()
            .downcast_ref::<CpuBgrxFrameHandle>()
            .ok_or_else(|| OutputFailure::terminal(OutputFailureCode::Sink))?;
        if handle.width() != frame.width || handle.height() != frame.height {
            return Err(OutputFailure::terminal(OutputFailureCode::Sink));
        }
        let width = i32::try_from(frame.width.get())
            .map_err(|_| OutputFailure::terminal(OutputFailureCode::Sink))?;
        let height = i32::try_from(frame.height.get())
            .map_err(|_| OutputFailure::terminal(OutputFailureCode::Sink))?;
        let next_caps = (frame.width.get(), frame.height.get());
        let caps_changed = runtime.video_caps != Some(next_caps);
        if caps_changed {
            let caps = gst::Caps::builder("video/x-raw")
                .field("format", "BGRx")
                .field("width", width)
                .field("height", height)
                .field("pixel-aspect-ratio", gst::Fraction::new(1, 1))
                .build();
            runtime.video_source.set_caps(Some(&caps));
            runtime.video_caps = Some(next_caps);
        }
        let (pts, discontinuity) = runtime.video_timeline.translate(
            &runtime.video_source,
            frame.pts_ns,
            frame.discontinuity || caps_changed,
        );
        let pixels = handle
            .tightly_packed()
            .map_err(|_| OutputFailure::terminal(OutputFailureCode::Sink))?;
        let mut buffer = gst::Buffer::from_mut_slice(pixels);
        {
            let buffer = buffer.make_mut();
            buffer.set_pts(pts);
            buffer.set_duration(gst::ClockTime::from_nseconds(frame.duration_ns));
            buffer.set_offset(frame.sequence);
            buffer.set_offset_end(frame.sequence.saturating_add(1));
            if discontinuity {
                buffer.set_flags(gst::BufferFlags::DISCONT);
            }
        }
        runtime
            .video_source
            .push_buffer(buffer)
            .map_err(|_| runtime.destination_class.write_failure())?;
        runtime.poll_bus(false)?;
        Ok(())
    }

    fn write_audio_inner(&mut self, packet: Arc<AudioPacket>) -> Result<(), OutputFailure> {
        let runtime = self
            .runtime
            .as_mut()
            .ok_or_else(|| OutputFailure::retryable(OutputFailureCode::Connect))?;
        runtime.poll_bus(false)?;
        let AudioPayload::InterleavedF32(samples) = &packet.payload else {
            return Err(OutputFailure::terminal(OutputFailureCode::Encoder));
        };
        let channels = usize::from(packet.channels.get());
        if samples.len() % channels != 0 {
            return Err(OutputFailure::terminal(OutputFailureCode::Encoder));
        }
        let rate = i32::try_from(packet.sample_rate.get())
            .map_err(|_| OutputFailure::terminal(OutputFailureCode::Encoder))?;
        let channels_i32 = i32::from(packet.channels.get());
        let next_caps = (packet.sample_rate.get(), packet.channels.get());
        let caps_changed = runtime.audio_caps != Some(next_caps);
        if caps_changed {
            let caps = gst::Caps::builder("audio/x-raw")
                .field("format", "F32LE")
                .field("rate", rate)
                .field("channels", channels_i32)
                .field("layout", "interleaved")
                .build();
            runtime.audio_source.set_caps(Some(&caps));
            runtime.audio_caps = Some(next_caps);
        }
        let (pts, discontinuity) = runtime.audio_timeline.translate(
            &runtime.audio_source,
            packet.pts_ns,
            packet.discontinuity || caps_changed,
        );
        let byte_capacity = samples
            .len()
            .checked_mul(size_of::<f32>())
            .ok_or_else(|| OutputFailure::terminal(OutputFailureCode::Encoder))?;
        let mut bytes = Vec::with_capacity(byte_capacity);
        for sample in samples.iter().copied() {
            bytes.extend_from_slice(&sample.to_le_bytes());
        }
        let mut buffer = gst::Buffer::from_mut_slice(bytes);
        {
            let buffer = buffer.make_mut();
            buffer.set_pts(pts);
            buffer.set_duration(gst::ClockTime::from_nseconds(packet.duration_ns));
            buffer.set_offset(packet.sequence);
            buffer.set_offset_end(packet.sequence.saturating_add(1));
            if discontinuity {
                buffer.set_flags(gst::BufferFlags::DISCONT);
            }
        }
        runtime
            .audio_source
            .push_buffer(buffer)
            .map_err(|_| runtime.destination_class.write_failure())?;
        runtime.poll_bus(false)?;
        Ok(())
    }
}

impl OutputSink for GstOutputSink {
    fn connect(&mut self) -> SinkFuture<'_, ()> {
        Box::pin(async move { self.connect_inner() })
    }

    fn write_video(&mut self, frame: Arc<VideoFrame>) -> SinkFuture<'_, ()> {
        Box::pin(async move { self.write_video_inner(frame) })
    }

    fn write_audio(&mut self, packet: Arc<AudioPacket>) -> SinkFuture<'_, ()> {
        Box::pin(async move { self.write_audio_inner(packet) })
    }

    fn close(&mut self) -> SinkFuture<'_, ()> {
        Box::pin(async move {
            let Some(runtime) = self.runtime.take() else {
                return Ok(());
            };
            let mut outcome = runtime
                .video_source
                .end_of_stream()
                .map(|_| ())
                .map_err(|_| runtime.destination_class.write_failure());
            if runtime.audio_source.end_of_stream().is_err() && outcome.is_ok() {
                outcome = Err(runtime.destination_class.write_failure());
            }
            let deadline = tokio::time::Instant::now() + FILE_FINALIZE_GRACE;
            while outcome.is_ok() && tokio::time::Instant::now() < deadline {
                match runtime.poll_bus(true) {
                    Ok(BusState::Eos) => break,
                    Ok(BusState::Running) => tokio::time::sleep(Duration::from_millis(10)).await,
                    Err(failure) => outcome = Err(failure),
                }
            }
            if runtime.pipeline.set_state(gst::State::Null).is_err() && outcome.is_ok() {
                outcome = Err(runtime.destination_class.write_failure());
            }
            outcome
        })
    }
}

struct GstOutputRuntime {
    pipeline: gst::Pipeline,
    bus: gst::Bus,
    video_source: gst_app::AppSrc,
    audio_source: gst_app::AppSrc,
    destination_class: DestinationClass,
    video_caps: Option<(u32, u32)>,
    audio_caps: Option<(u32, u16)>,
    video_timeline: StreamTimeline,
    audio_timeline: StreamTimeline,
}

impl GstOutputRuntime {
    fn poll_bus(&self, allow_eos: bool) -> Result<BusState, OutputFailure> {
        while let Some(message) = self.bus.timed_pop(gst::ClockTime::ZERO) {
            use gst::MessageView;
            match message.view() {
                MessageView::Error(error) => {
                    let source = error
                        .src()
                        .map(|source| source.path_string().to_string())
                        .unwrap_or_default();
                    return Err(classify_pipeline_failure(&source, self.destination_class));
                }
                MessageView::Eos(..) if allow_eos => return Ok(BusState::Eos),
                MessageView::Eos(..) => return Err(self.destination_class.write_failure()),
                _ => {}
            }
        }
        Ok(BusState::Running)
    }
}

impl Drop for GstOutputRuntime {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BusState {
    Running,
    Eos,
}

#[derive(Debug, Default)]
struct StreamTimeline {
    source_origin_ns: Option<u64>,
    running_origin: Option<gst::ClockTime>,
    last_source_pts_ns: Option<u64>,
}

impl StreamTimeline {
    fn translate(
        &mut self,
        source: &gst_app::AppSrc,
        source_pts_ns: u64,
        requested_discontinuity: bool,
    ) -> (gst::ClockTime, bool) {
        let discontinuity = requested_discontinuity
            || self
                .last_source_pts_ns
                .is_some_and(|previous| source_pts_ns < previous)
            || self.source_origin_ns.is_none();
        if discontinuity {
            self.source_origin_ns = Some(source_pts_ns);
            self.running_origin = source.current_running_time().or(Some(gst::ClockTime::ZERO));
        }
        let source_origin = self.source_origin_ns.unwrap_or(source_pts_ns);
        let running_origin = self.running_origin.unwrap_or(gst::ClockTime::ZERO);
        let pts = running_origin.saturating_add(gst::ClockTime::from_nseconds(
            source_pts_ns.saturating_sub(source_origin),
        ));
        self.last_source_pts_ns = Some(source_pts_ns);
        (pts, discontinuity)
    }
}

fn apply_program_map(
    pipeline: &gst::Pipeline,
    binding: &GstProgramMapBinding,
) -> Result<(), OutputFailure> {
    let mux = pipeline
        .by_name(MUX_NAME)
        .ok_or_else(|| OutputFailure::terminal(OutputFailureCode::Muxer))?;
    let structure = binding
        .structure
        .parse::<gst::Structure>()
        .map_err(|_| OutputFailure::terminal(OutputFailureCode::Muxer))?;
    mux.set_property("prog-map", &structure);
    Ok(())
}

fn classify_pipeline_failure(source_path: &str, destination: DestinationClass) -> OutputFailure {
    let source = source_path.to_ascii_lowercase();
    if source.contains("video_encoder") || source.contains("audio_encoder") {
        OutputFailure::terminal(OutputFailureCode::Encoder)
    } else if source.contains("mux") {
        OutputFailure::terminal(OutputFailureCode::Muxer)
    } else if source.contains("payloader") {
        OutputFailure::terminal(OutputFailureCode::Protocol)
    } else {
        destination.write_failure()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::architecture::{
        AudioEncoderSpec, AudioTrackId, EncoderOutputSpec, MpegTsPid, OutputId,
        ResolvedMpegTsProgramSpec, ServiceMetadata, VideoEncoderSpec,
    };

    fn program_map() -> ResolvedProgramMapSpec {
        ResolvedProgramMapSpec {
            transport_stream_id: 7,
            programs: vec![ResolvedMpegTsProgramSpec {
                program_number: NonZeroU16::new(1).expect("non-zero program"),
                service: ServiceMetadata {
                    service_name: "Haze CGEN".to_string(),
                    provider_name: "Haze".to_string(),
                },
                pmt_pid: MpegTsPid::new(0x1000).expect("valid PMT PID"),
                video_pid: Some(MpegTsPid::new(0x0100).expect("valid video PID")),
                audio: vec![(
                    AudioTrackId::parse("main").expect("valid track ID"),
                    MpegTsPid::new(0x0101).expect("valid audio PID"),
                )],
                scte35: None,
                pcr_pid: MpegTsPid::new(0x0100).expect("valid PCR PID"),
            }],
        }
    }

    fn output(destination: OutputDestination) -> EncoderOutputSpec {
        output_with_codecs(
            destination,
            VideoCodec::H264,
            AudioCodecPolicy::Encode(AudioCodec::Aac),
        )
    }

    fn output_with_codecs(
        destination: OutputDestination,
        video_codec: VideoCodec,
        audio_codec: AudioCodecPolicy,
    ) -> EncoderOutputSpec {
        EncoderOutputSpec {
            id: OutputId::parse("test-output").expect("valid output ID"),
            enabled: true,
            destination,
            video: VideoEncoderSpec {
                codec: video_codec,
                rate_control: RateControl::Vbr {
                    target_kbps: NonZeroU32::new(4_000).expect("non-zero bitrate"),
                    max_kbps: NonZeroU32::new(6_000).expect("non-zero bitrate"),
                },
                gop_frames: NonZeroU32::new(60).expect("non-zero GOP"),
            },
            audio: AudioEncoderSpec {
                codec: audio_codec,
                bitrate_kbps: NonZeroU32::new(192).expect("non-zero bitrate"),
                sample_rate: NonZeroU32::new(48_000).expect("non-zero sample rate"),
            },
        }
    }

    fn plan(
        output: &EncoderOutputSpec,
        program_map: Option<&ResolvedProgramMapSpec>,
    ) -> OutputPipelinePlan {
        let activated = ActivatedOutputSpec::from_output(output).expect("activated output");
        OutputPipelinePlan::build(&activated, program_map, RtmpSinkElement::Rtmp2)
            .expect("pipeline plan")
    }

    #[test]
    fn cpu_bgrx_handle_validates_stride_and_removes_padding() {
        let width = NonZeroU32::new(2).expect("non-zero width");
        let height = NonZeroU32::new(2).expect("non-zero height");
        let stride = NonZeroU32::new(12).expect("non-zero stride");
        let pixels = (0_u8..24).collect::<Vec<_>>();

        let handle = CpuBgrxFrameHandle::new(width, height, stride, pixels.into())
            .expect("valid retained frame");

        assert_eq!(
            handle.tightly_packed().expect("packed frame"),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(handle.backend(), "cpu_bgrx");
        assert!(handle
            .as_any()
            .downcast_ref::<CpuBgrxFrameHandle>()
            .is_some());
    }

    #[test]
    fn cpu_bgrx_handle_rejects_short_rows_and_buffers() {
        let width = NonZeroU32::new(2).expect("non-zero width");
        let height = NonZeroU32::new(2).expect("non-zero height");
        assert!(matches!(
            CpuBgrxFrameHandle::new(
                width,
                height,
                NonZeroU32::new(7).expect("non-zero stride"),
                Arc::<[u8]>::from(vec![0; 16]),
            ),
            Err(CpuBgrxFrameError::StrideTooSmall)
        ));
        assert!(matches!(
            CpuBgrxFrameHandle::new(
                width,
                height,
                NonZeroU32::new(8).expect("non-zero stride"),
                Arc::<[u8]>::from(vec![0; 15]),
            ),
            Err(CpuBgrxFrameError::BufferTooSmall)
        ));
    }

    #[test]
    fn environment_references_expand_only_when_requested() {
        let raw = "srt://${OUTPUT_HOST}:$OUTPUT_PORT?passphrase=$OUTPUT_SECRET";
        let expanded = expand_env_vars_with(raw, |name| match name {
            "OUTPUT_HOST" => Some("encoder.example".to_string()),
            "OUTPUT_PORT" => Some("9000".to_string()),
            "OUTPUT_SECRET" => Some("not-for-status".to_string()),
            _ => None,
        });

        assert!(raw.contains("$OUTPUT_SECRET"));
        assert_eq!(
            expanded,
            "srt://encoder.example:9000?passphrase=not-for-status"
        );
        assert_eq!(
            expand_env_vars_with("udp://${UNCLOSED", |_| None),
            "udp://${UNCLOSED"
        );
    }

    #[test]
    fn endpoint_parser_supports_ipv4_hostnames_and_ipv6() {
        assert_eq!(
            UdpEndpoint::parse("udp://239.0.0.1:5000?pkt_size=1316", &["udp"]),
            Some(UdpEndpoint {
                host: "239.0.0.1".to_string(),
                port: NonZeroU16::new(5000).expect("non-zero port"),
            })
        );
        assert_eq!(
            UdpEndpoint::parse("rtp://[2001:db8::1]:5002", &["udp", "rtp"]),
            Some(UdpEndpoint {
                host: "2001:db8::1".to_string(),
                port: NonZeroU16::new(5002).expect("non-zero port"),
            })
        );
        assert!(UdpEndpoint::parse("udp://missing-port", &["udp"]).is_none());
    }

    #[test]
    fn mpeg_ts_plan_binds_elementary_pmt_and_pcr_pids() {
        let map = program_map();
        let spec = output(OutputDestination::MpegTsUdp {
            location: "udp://239.10.10.10:5000".to_string(),
        });

        let plan = plan(&spec, Some(&map));
        let binding = plan.program_map.expect("program map binding");

        assert!(plan.description.contains("mpegtsmux name=mux"));
        assert!(plan.description.contains("mux.sink_256"));
        assert!(plan.description.contains("mux.sink_257"));
        assert_eq!(
            binding.structure,
            "program_map,sink_256=(int)1,sink_257=(int)1,PMT_1=(uint)4096,PCR_1=(int)256"
        );
        assert_eq!(binding.requested_transport_stream_id, 7);
        assert_eq!(binding.requested_service_count, 1);
        assert!(binding.capabilities.elementary_pids);
        assert!(binding.capabilities.pmt_pids);
        assert!(binding.capabilities.pcr_pids);
        assert!(!binding.capabilities.transport_stream_id);
        assert!(!binding.capabilities.service_metadata);
        assert!(!binding.capabilities.timestamped_scte35_events);
        assert!(!binding.capabilities.generated_scte35_cues);
    }

    #[test]
    fn transport_stream_destinations_require_resolved_mapping() {
        let spec = output(OutputDestination::MpegTsSrt {
            location: "srt://encoder.example:9000".to_string(),
            latency_ms: 120,
        });
        let activated = ActivatedOutputSpec::from_output(&spec).expect("activated output");

        assert!(matches!(
            OutputPipelinePlan::build(&activated, None, RtmpSinkElement::Rtmp2),
            Err(PlanError::MissingProgramMap)
        ));
    }

    #[test]
    fn srt_plan_applies_latency_and_auto_reconnect() {
        let map = program_map();
        let spec = output(OutputDestination::MpegTsSrt {
            location: "srt://encoder.example:9000".to_string(),
            latency_ms: 240,
        });

        let plan = plan(&spec, Some(&map));

        assert!(plan.description.contains("srtsink name=output_sink"));
        assert!(plan.description.contains("latency=240"));
        assert!(plan.description.contains("auto-reconnect=true"));
    }

    #[test]
    fn rtp_plan_uses_explicit_video_and_all_audio_endpoints() {
        let spec = output(OutputDestination::Rtp {
            video_location: "udp://239.20.0.1:5000".to_string(),
            audio_locations: vec![
                "udp://239.20.0.1:5002".to_string(),
                "rtp://239.20.0.2:5002".to_string(),
            ],
        });

        let plan = plan(&spec, None);

        assert!(plan.description.contains("rtph264pay"));
        assert!(plan.description.contains("rtpmp4gpay pt=97"));
        assert!(plan.description.contains("name=video_output_sink"));
        assert!(plan.description.contains("name=audio_output_sink_0"));
        assert!(plan.description.contains("name=audio_output_sink_1"));
        assert!(!plan.description.contains("mpegtsmux"));
    }

    #[test]
    fn rtp_payloaders_cover_all_supported_audio_and_video_codecs() {
        let destination = || OutputDestination::Rtp {
            video_location: "udp://127.0.0.1:5000".to_string(),
            audio_locations: vec!["udp://127.0.0.1:5002".to_string()],
        };
        let cases = [
            (
                VideoCodec::H264,
                AudioCodec::Aac,
                "rtph264pay",
                "rtpmp4gpay",
            ),
            (VideoCodec::H265, AudioCodec::Ac3, "rtph265pay", "rtpac3pay"),
            (VideoCodec::Mpeg2, AudioCodec::Mp2, "rtpmpvpay", "rtpmpapay"),
        ];
        for (video, audio, video_payloader, audio_payloader) in cases {
            let spec = output_with_codecs(destination(), video, AudioCodecPolicy::Encode(audio));
            let plan = plan(&spec, None);
            assert!(plan.description.contains(video_payloader));
            assert!(plan.description.contains(audio_payloader));
        }
    }

    #[test]
    fn rtmp_and_supported_file_muxers_have_isolated_plans() {
        let rtmp = output(OutputDestination::Rtmp {
            location: "rtmp://example.invalid/live/key".to_string(),
        });
        let rtmp_plan = plan(&rtmp, None);
        assert!(rtmp_plan.description.contains("flvmux name=mux"));
        assert!(rtmp_plan.description.contains("rtmp2sink name=output_sink"));

        for (container, muxer) in [
            ("mkv", "matroskamux"),
            ("flv", "flvmux"),
            ("mp4", "mp4mux"),
            ("mov", "mp4mux"),
        ] {
            let spec = output(OutputDestination::File {
                location: format!("recording.{container}"),
                container: container.to_string(),
            });
            assert!(plan(&spec, None).description.contains(muxer));
        }

        let mpeg_ps = output_with_codecs(
            OutputDestination::File {
                location: "recording.mpg".to_string(),
                container: "mpegps".to_string(),
            },
            VideoCodec::Mpeg2,
            AudioCodecPolicy::Encode(AudioCodec::Ac3),
        );
        assert!(plan(&mpeg_ps, None).description.contains("mpegpsmux"));

        let map = program_map();
        let mpeg_ts = output(OutputDestination::File {
            location: "recording.ts".to_string(),
            container: "mpegts".to_string(),
        });
        assert!(plan(&mpeg_ts, Some(&map)).description.contains("mpegtsmux"));
    }

    #[test]
    fn plan_debug_never_contains_resolved_destination() {
        let secret = "rtmp://operator:very-secret@example.invalid/live/key";
        let spec = output(OutputDestination::Rtmp {
            location: secret.to_string(),
        });
        let plan = plan(&spec, None);

        let debug = format!("{plan:?}");

        assert!(!debug.contains(secret));
        assert!(!debug.contains("very-secret"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn raw_pcm_adapter_rejects_match_input_policy() {
        let spec = output_with_codecs(
            OutputDestination::File {
                location: "native.ts".to_string(),
                container: "mpegts".to_string(),
            },
            VideoCodec::H264,
            AudioCodecPolicy::MatchInput,
        );

        assert!(matches!(
            ActivatedOutputSpec::from_output(&spec),
            Err(PlanError::MatchInputRequiresEncodedHandle)
        ));
    }

    #[test]
    fn pipeline_failures_are_classified_without_error_text() {
        assert_eq!(
            classify_pipeline_failure("pipeline/video_encoder", DestinationClass::Network),
            OutputFailure::terminal(OutputFailureCode::Encoder)
        );
        assert_eq!(
            classify_pipeline_failure("pipeline/mux", DestinationClass::Network),
            OutputFailure::terminal(OutputFailureCode::Muxer)
        );
        assert_eq!(
            classify_pipeline_failure("pipeline/output_sink", DestinationClass::Network),
            OutputFailure::retryable(OutputFailureCode::Network)
        );
        assert_eq!(
            classify_pipeline_failure("pipeline/output_sink", DestinationClass::File),
            OutputFailure::terminal(OutputFailureCode::Sink)
        );
    }
}
