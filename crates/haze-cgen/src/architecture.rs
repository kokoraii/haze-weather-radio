use std::collections::BTreeSet;
use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

use thiserror::Error;

pub(crate) use crate::scene::SceneId;

const MAX_ID_LEN: usize = 128;
const MAX_LOCATION_LEN: usize = 4096;
const MAX_SERVICE_TEXT_LEN: usize = 255;
const FIRST_AUTO_ES_PID: u16 = 0x0100;
const FIRST_AUTO_PMT_PID: u16 = 0x1000;
const LAST_ASSIGNABLE_PID: u16 = 0x1ffe;

#[derive(Debug, Error, Clone, PartialEq)]
pub(crate) enum PipelineSpecError {
    #[error("{kind} must not be empty")]
    EmptyId { kind: &'static str },
    #[error("{kind} exceeds {MAX_ID_LEN} characters")]
    IdTooLong { kind: &'static str },
    #[error("{kind} contains unsupported characters")]
    InvalidIdCharacters { kind: &'static str },
    #[error("{field} must not be empty")]
    EmptyValue { field: &'static str },
    #[error("{field} contains invalid control characters")]
    InvalidControlCharacter { field: &'static str },
    #[error("{field} exceeds {MAX_LOCATION_LEN} characters")]
    ValueTooLong { field: &'static str },
    #[error("video dimensions must be non-zero")]
    ZeroVideoDimension,
    #[error("frame rate numerator and denominator must be non-zero")]
    InvalidFrameRate,
    #[error("gain must be muted or between -60 dB and +12 dB")]
    InvalidGain,
    #[error("audio transition must be between 1 and 1000 milliseconds")]
    InvalidAudioTransition,
    #[error("mix matrix has {actual} coefficients but requires {expected}")]
    InvalidMixMatrixSize { expected: usize, actual: usize },
    #[error("MPEG-TS PID {0:#06x} is outside the assignable range")]
    InvalidPid(u16),
    #[error("MPEG-TS PID {0:#06x} is assigned more than once")]
    DuplicatePid(u16),
    #[error("MPEG-TS program number {0} is assigned more than once")]
    DuplicateProgram(u16),
    #[error("MPEG-TS program map requires at least one program")]
    MissingProgram,
    #[error("MPEG-TS transport stream ID must be non-zero")]
    InvalidTransportStreamId,
    #[error("MPEG-TS program {0} requires at least one elementary stream")]
    MissingElementaryStream(u16),
    #[error("automatic MPEG-TS PID allocation exhausted the assignable range")]
    PidAllocationExhausted,
    #[error("encoder output IDs must be unique")]
    DuplicateOutputId,
    #[error("pipeline requires at least one encoder output")]
    MissingOutput,
    #[error("program mapping is only valid for MPEG-TS destinations")]
    ProgramMapOnNonTransportStream,
    #[error("MPEG-TS destinations require a program map")]
    MissingProgramMap,
    #[error("RTMP output requires H.264 video and AAC audio")]
    InvalidRtmpCodec,
    #[error("FLV output requires H.264 video and AAC audio")]
    InvalidFlvCodec,
    #[error("MP4 and MOV output require H.264 or H.265 video and AAC audio")]
    InvalidIsoBmffCodec,
    #[error("MPEG program stream output requires MPEG-2 video and AC3 or MP2 audio")]
    InvalidMpegProgramStreamCodec,
    #[error("unsupported file output container {0}")]
    UnsupportedFileContainer(String),
    #[error("RTP output requires at least one explicit audio endpoint")]
    MissingRtpAudioEndpoint,
    #[error("{kind} output location must use one of: {allowed}")]
    InvalidOutputScheme {
        kind: &'static str,
        allowed: &'static str,
    },
    #[error("VBR maximum bitrate must be greater than or equal to target bitrate")]
    InvalidVbrRange,
    #[error("preserve-native audio outputs must use Match Input")]
    PreserveNativeRequiresMatchInput,
    #[error("forced-layout audio outputs must select AAC, AC3, or MP2")]
    ForcedLayoutRequiresEncoder,
    #[error("MPEG-TS program {program} has duplicate audio track ID {track_id}")]
    DuplicateAudioTrackId { program: u16, track_id: String },
    #[error("{field} exceeds {MAX_SERVICE_TEXT_LEN} characters")]
    ServiceMetadataTooLong { field: &'static str },
    #[error("Program_Passthrough and Standby cannot be selected as alert scenes")]
    InvalidAlertScene,
    #[error(
        "named GStreamer elements may contain only letters, numbers, underscores, and hyphens"
    )]
    InvalidElementName,
    #[error("deinterlace algorithm, backend, and parity combination is unsupported")]
    InvalidDeinterlaceCombination,
}

macro_rules! string_id {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub(crate) struct $name(Box<str>);

        impl $name {
            pub(crate) fn parse(raw: impl AsRef<str>) -> Result<Self, PipelineSpecError> {
                let value = raw.as_ref().trim();
                if value.is_empty() {
                    return Err(PipelineSpecError::EmptyId { kind: $kind });
                }
                if value.chars().count() > MAX_ID_LEN {
                    return Err(PipelineSpecError::IdTooLong { kind: $kind });
                }
                if is_reserved_identifier_component(value) {
                    return Err(PipelineSpecError::InvalidIdCharacters { kind: $kind });
                }
                if !value
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
                {
                    return Err(PipelineSpecError::InvalidIdCharacters { kind: $kind });
                }
                Ok(Self(value.into()))
            }

            pub(crate) fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

string_id!(FeedId, "feed ID");
string_id!(OutputId, "output ID");
string_id!(AudioTrackId, "audio track ID");

fn is_reserved_identifier_component(value: &str) -> bool {
    if matches!(value, "." | "..") || value.ends_with(' ') || value.ends_with('.') {
        return true;
    }
    let stem = value
        .split('.')
        .next()
        .unwrap_or(value)
        .to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$")
        || stem
            .strip_prefix("COM")
            .or_else(|| stem.strip_prefix("LPT"))
            .is_some_and(|suffix| matches!(suffix.as_bytes(), [b'1'..=b'9']))
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PipelineSpec {
    pub(crate) feed_id: FeedId,
    pub(crate) input: ProgramInput,
    pub(crate) alert_feed_id: FeedId,
    pub(crate) ancillary: AncillaryPolicy,
    pub(crate) audio: AudioRoutingSpec,
    pub(crate) compositor: CompositorSpec,
    pub(crate) program_map: Option<ProgramMapSpec>,
    pub(crate) outputs: Vec<EncoderOutputSpec>,
}

impl PipelineSpec {
    pub(crate) fn validate(&self) -> Result<(), PipelineSpecError> {
        self.input.validate()?;
        self.audio.validate()?;
        self.compositor.validate()?;
        let enabled_outputs = self.outputs.iter().filter(|output| output.enabled);
        if self.program_map.is_some()
            && !enabled_outputs
                .clone()
                .any(|output| output.destination.is_mpeg_ts())
        {
            return Err(PipelineSpecError::ProgramMapOnNonTransportStream);
        }
        if let Some(program_map) = &self.program_map {
            program_map.resolve()?;
        }
        if enabled_outputs.count() == 0 {
            return Err(PipelineSpecError::MissingOutput);
        }
        let mut ids = BTreeSet::new();
        for output in &self.outputs {
            if !ids.insert(output.id.as_str().to_ascii_lowercase()) {
                return Err(PipelineSpecError::DuplicateOutputId);
            }
            if output.enabled {
                match (self.audio.topology, output.audio.codec) {
                    (AudioTopologyMode::PreserveNativeTracks, AudioCodecPolicy::Encode(_)) => {
                        return Err(PipelineSpecError::PreserveNativeRequiresMatchInput)
                    }
                    (AudioTopologyMode::ForceLayout(_), AudioCodecPolicy::MatchInput) => {
                        return Err(PipelineSpecError::ForcedLayoutRequiresEncoder);
                    }
                    _ => {}
                }
                output.validate(self.program_map.as_ref())?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ProgramInput {
    Device(DeviceInput),
    UriOrFile(UriInput),
    Dummy(DummyInput),
}

impl ProgramInput {
    fn validate(&self) -> Result<(), PipelineSpecError> {
        match self {
            Self::Device(input) => {
                validate_text("device ID", &input.persistent_id)?;
                input.decoder.validate()?;
                input.deinterlace.validate()
            }
            Self::UriOrFile(input) => {
                validate_text("input location", &input.location)?;
                input.decoder.validate()?;
                input.deinterlace.validate()
            }
            Self::Dummy(input) => input.format.validate(),
        }
    }

    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Device(_) => "device",
            Self::UriOrFile(_) => "uri_or_file",
            Self::Dummy(_) => "dummy",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeviceBackend {
    V4l2,
    DirectShow,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DeviceInput {
    pub(crate) backend: DeviceBackend,
    pub(crate) persistent_id: String,
    pub(crate) decoder: DecoderPreference,
    pub(crate) deinterlace: DeinterlaceSpec,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UriInput {
    pub(crate) location: String,
    pub(crate) demux_hint: DemuxHint,
    pub(crate) decoder: DecoderPreference,
    pub(crate) deinterlace: DeinterlaceSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DemuxHint {
    Auto,
    MpegTs,
    Rtp,
    Srt,
    File,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecoderPreference {
    Auto,
    Software,
    Nvdec,
    QuickSync,
    Vaapi,
    Named(String),
}

impl DecoderPreference {
    fn validate(&self) -> Result<(), PipelineSpecError> {
        let Self::Named(element) = self else {
            return Ok(());
        };
        if element.is_empty()
            || !element
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
        {
            return Err(PipelineSpecError::InvalidElementName);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeinterlaceAlgorithm {
    Yadif,
    Bwdif,
    MotionAdaptive,
}

impl DeinterlaceAlgorithm {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Yadif => "yadif",
            Self::Bwdif => "bwdif",
            Self::MotionAdaptive => "motion_adaptive",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeinterlaceBackend {
    Auto,
    Software,
    Vaapi,
    QuickSync,
    D3d11,
    Cuda,
    Vulkan,
    OpenGl,
}

impl DeinterlaceBackend {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Software => "software",
            Self::Vaapi => "vaapi",
            Self::QuickSync => "quicksync",
            Self::D3d11 => "d3d11",
            Self::Cuda => "cuda",
            Self::Vulkan => "vulkan",
            Self::OpenGl => "opengl",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeinterlaceCadence {
    Field,
    Frame,
}

impl DeinterlaceCadence {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Field => "field",
            Self::Frame => "frame",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeinterlaceParity {
    Auto,
    TopFirst,
    BottomFirst,
}

impl DeinterlaceParity {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::TopFirst => "tff",
            Self::BottomFirst => "bff",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeinterlaceSpec {
    pub(crate) enabled: bool,
    pub(crate) algorithm: DeinterlaceAlgorithm,
    pub(crate) backend: DeinterlaceBackend,
    pub(crate) cadence: DeinterlaceCadence,
    pub(crate) parity: DeinterlaceParity,
}

impl Default for DeinterlaceSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithm: DeinterlaceAlgorithm::Yadif,
            backend: DeinterlaceBackend::Software,
            cadence: DeinterlaceCadence::Field,
            parity: DeinterlaceParity::Auto,
        }
    }
}

impl DeinterlaceSpec {
    fn validate(self) -> Result<(), PipelineSpecError> {
        if !self.enabled {
            return Ok(());
        }
        if self.backend != DeinterlaceBackend::Auto
            && self.backend != DeinterlaceBackend::Software
            && self.parity != DeinterlaceParity::Auto
        {
            return Err(PipelineSpecError::InvalidDeinterlaceCombination);
        }
        let supported = match self.algorithm {
            DeinterlaceAlgorithm::Yadif => matches!(
                self.backend,
                DeinterlaceBackend::Auto | DeinterlaceBackend::Software | DeinterlaceBackend::Cuda
            ),
            DeinterlaceAlgorithm::Bwdif => matches!(
                self.backend,
                DeinterlaceBackend::Auto
                    | DeinterlaceBackend::Software
                    | DeinterlaceBackend::Cuda
                    | DeinterlaceBackend::Vulkan
            ),
            DeinterlaceAlgorithm::MotionAdaptive => matches!(
                self.backend,
                DeinterlaceBackend::Auto
                    | DeinterlaceBackend::Software
                    | DeinterlaceBackend::Vaapi
                    | DeinterlaceBackend::QuickSync
                    | DeinterlaceBackend::D3d11
                    | DeinterlaceBackend::OpenGl
            ),
        };
        if supported {
            Ok(())
        } else {
            Err(PipelineSpecError::InvalidDeinterlaceCombination)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DummyInput {
    pub(crate) format: VideoFormat,
    pub(crate) background: Rgba8,
}

impl Default for DummyInput {
    fn default() -> Self {
        Self {
            format: VideoFormat::default(),
            background: Rgba8::BLACK,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Rgba8 {
    pub(crate) red: u8,
    pub(crate) green: u8,
    pub(crate) blue: u8,
    pub(crate) alpha: u8,
}

impl Rgba8 {
    pub(crate) const BLACK: Self = Self {
        red: 0,
        green: 0,
        blue: 0,
        alpha: 255,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Rational {
    pub(crate) numerator: NonZeroU32,
    pub(crate) denominator: NonZeroU32,
}

impl Rational {
    pub(crate) fn new(numerator: u32, denominator: u32) -> Result<Self, PipelineSpecError> {
        let Some(numerator) = NonZeroU32::new(numerator) else {
            return Err(PipelineSpecError::InvalidFrameRate);
        };
        let Some(denominator) = NonZeroU32::new(denominator) else {
            return Err(PipelineSpecError::InvalidFrameRate);
        };
        Ok(Self {
            numerator,
            denominator,
        })
    }

    pub(crate) fn parse(raw: &str) -> Result<Self, PipelineSpecError> {
        let raw = raw.trim();
        if let Some((numerator, denominator)) = raw.split_once('/') {
            let numerator = numerator
                .trim()
                .parse::<u32>()
                .map_err(|_| PipelineSpecError::InvalidFrameRate)?;
            let denominator = denominator
                .trim()
                .parse::<u32>()
                .map_err(|_| PipelineSpecError::InvalidFrameRate)?;
            return Self::new(numerator, denominator);
        }
        let numerator = raw
            .parse::<u32>()
            .map_err(|_| PipelineSpecError::InvalidFrameRate)?;
        Self::new(numerator, 1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldOrder {
    TopFirst,
    BottomFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanMode {
    Progressive,
    Interlaced { field_order: FieldOrder },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VideoFormat {
    pub(crate) width: NonZeroU32,
    pub(crate) height: NonZeroU32,
    pub(crate) frame_rate: Rational,
    pub(crate) scan: ScanMode,
}

impl VideoFormat {
    pub(crate) fn new(
        width: u32,
        height: u32,
        frame_rate: Rational,
        scan: ScanMode,
    ) -> Result<Self, PipelineSpecError> {
        let Some(width) = NonZeroU32::new(width) else {
            return Err(PipelineSpecError::ZeroVideoDimension);
        };
        let Some(height) = NonZeroU32::new(height) else {
            return Err(PipelineSpecError::ZeroVideoDimension);
        };
        Ok(Self {
            width,
            height,
            frame_rate,
            scan,
        })
    }

    fn validate(&self) -> Result<(), PipelineSpecError> {
        if self.width.get() == 0 || self.height.get() == 0 {
            return Err(PipelineSpecError::ZeroVideoDimension);
        }
        Ok(())
    }
}

impl Default for VideoFormat {
    fn default() -> Self {
        Self::new(
            720,
            480,
            Rational::new(30_000, 1_001).expect("default frame rate is valid"),
            ScanMode::Progressive,
        )
        .expect("default dimensions are valid")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PassPolicy {
    Pass,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AncillaryPolicy {
    pub(crate) captions: PassPolicy,
    pub(crate) scte35: PassPolicy,
    pub(crate) scte104: PassPolicy,
}

impl Default for AncillaryPolicy {
    fn default() -> Self {
        Self {
            captions: PassPolicy::Drop,
            scte35: PassPolicy::Drop,
            scte104: PassPolicy::Drop,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum GainDb {
    Muted,
    Db(f32),
}

impl GainDb {
    pub(crate) fn parse(raw: &str) -> Result<Self, PipelineSpecError> {
        let value = raw.trim();
        if value.eq_ignore_ascii_case("muted") || value.eq_ignore_ascii_case("-inf") {
            return Ok(Self::Muted);
        }
        let value = value
            .parse::<f32>()
            .map_err(|_| PipelineSpecError::InvalidGain)?;
        if !value.is_finite() || !(-60.0..=12.0).contains(&value) {
            return Err(PipelineSpecError::InvalidGain);
        }
        Ok(Self::Db(value))
    }

    pub(crate) fn linear(self) -> f32 {
        match self {
            Self::Muted => 0.0,
            Self::Db(value) => 10.0_f32.powf(value / 20.0),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChannelLayout {
    Mono,
    Stereo,
    Surround51,
}

impl ChannelLayout {
    pub(crate) fn channels(self) -> u16 {
        match self {
            Self::Mono => 1,
            Self::Stereo => 2,
            Self::Surround51 => 6,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Mono => "mono",
            Self::Stereo => "stereo",
            Self::Surround51 => "surround_5_1",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AudioTopologyMode {
    PreserveNativeTracks,
    ForceLayout(ChannelLayout),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AudioRoutingSpec {
    pub(crate) topology: AudioTopologyMode,
    pub(crate) idle_program_gain: GainDb,
    pub(crate) alert_program_gain: GainDb,
    pub(crate) alert_gain: GainDb,
    pub(crate) transition_ms: u16,
}

impl AudioRoutingSpec {
    fn validate(&self) -> Result<(), PipelineSpecError> {
        for gain in [
            self.idle_program_gain,
            self.alert_program_gain,
            self.alert_gain,
        ] {
            if let GainDb::Db(value) = gain {
                if !value.is_finite() || !(-60.0..=12.0).contains(&value) {
                    return Err(PipelineSpecError::InvalidGain);
                }
            }
        }
        if !(1..=1_000).contains(&self.transition_ms) {
            return Err(PipelineSpecError::InvalidAudioTransition);
        }
        Ok(())
    }
}

impl Default for AudioRoutingSpec {
    fn default() -> Self {
        Self {
            topology: AudioTopologyMode::PreserveNativeTracks,
            idle_program_gain: GainDb::Db(0.0),
            alert_program_gain: GainDb::Muted,
            alert_gain: GainDb::Db(0.0),
            transition_ms: 20,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MixMatrix {
    pub(crate) source_channels: u16,
    pub(crate) destination_channels: u16,
    pub(crate) coefficients: Vec<f32>,
}

impl MixMatrix {
    pub(crate) fn new(
        source_channels: u16,
        destination_channels: u16,
        coefficients: Vec<f32>,
    ) -> Result<Self, PipelineSpecError> {
        let expected = usize::from(source_channels) * usize::from(destination_channels);
        if coefficients.len() != expected {
            return Err(PipelineSpecError::InvalidMixMatrixSize {
                expected,
                actual: coefficients.len(),
            });
        }
        Ok(Self {
            source_channels,
            destination_channels,
            coefficients,
        })
    }

    pub(crate) fn for_program(
        source: ChannelLayout,
        destination: ChannelLayout,
    ) -> Result<Self, PipelineSpecError> {
        const CENTER: f32 = 0.707_106_77;
        let coefficients = match (source, destination) {
            (ChannelLayout::Mono, ChannelLayout::Mono) => vec![1.0],
            (ChannelLayout::Mono, ChannelLayout::Stereo) => vec![1.0, 1.0],
            (ChannelLayout::Mono, ChannelLayout::Surround51) => {
                vec![CENTER, CENTER, 1.0, 0.0, 0.5, 0.5]
            }
            (ChannelLayout::Stereo, ChannelLayout::Mono) => vec![0.5, 0.5],
            (ChannelLayout::Stereo, ChannelLayout::Stereo) => {
                vec![1.0, 0.0, 0.0, 1.0]
            }
            (ChannelLayout::Stereo, ChannelLayout::Surround51) => {
                vec![1.0, 0.0, 0.5, 0.0, 0.5, 0.0, 0.0, 1.0, 0.5, 0.0, 0.0, 0.5]
            }
            (ChannelLayout::Surround51, ChannelLayout::Mono) => {
                vec![
                    0.207_106_78,
                    0.207_106_78,
                    0.292_893_23,
                    0.0,
                    0.146_446_62,
                    0.146_446_62,
                ]
            }
            (ChannelLayout::Surround51, ChannelLayout::Stereo) => vec![
                0.414_213_57,
                0.0,
                0.0,
                0.414_213_57,
                0.292_893_23,
                0.292_893_23,
                0.0,
                0.0,
                0.292_893_23,
                0.0,
                0.0,
                0.292_893_23,
            ],
            (ChannelLayout::Surround51, ChannelLayout::Surround51) => {
                let mut identity = vec![0.0; 36];
                for index in 0..6 {
                    identity[index * 6 + index] = 1.0;
                }
                identity
            }
        };
        Self::new(source.channels(), destination.channels(), coefficients)
    }

    pub(crate) fn for_alert(destination: ChannelLayout) -> Result<Self, PipelineSpecError> {
        Self::new(
            1,
            destination.channels(),
            vec![1.0; usize::from(destination.channels())],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositorSpec {
    pub(crate) alert_scene_id: SceneId,
}

impl CompositorSpec {
    fn validate(&self) -> Result<(), PipelineSpecError> {
        crate::scene::validate_alert_scene(&self.alert_scene_id)
            .map_err(|_| PipelineSpecError::InvalidAlertScene)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MpegTsPid(u16);

impl MpegTsPid {
    pub(crate) fn new(value: u16) -> Result<Self, PipelineSpecError> {
        if !(0x0020..=LAST_ASSIGNABLE_PID).contains(&value) {
            return Err(PipelineSpecError::InvalidPid(value));
        }
        Ok(Self(value))
    }

    pub(crate) fn get(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PidAssignment {
    Auto,
    Manual(MpegTsPid),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudioStreamMap {
    pub(crate) track_id: AudioTrackId,
    pub(crate) pid: PidAssignment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scte35Map {
    pub(crate) input: PassPolicy,
    pub(crate) generated_alert_cues: bool,
    pub(crate) pid: PidAssignment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ServiceMetadata {
    pub(crate) service_name: String,
    pub(crate) provider_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MpegTsProgramSpec {
    pub(crate) program_number: NonZeroU16,
    pub(crate) service: ServiceMetadata,
    pub(crate) pmt_pid: PidAssignment,
    pub(crate) video_pid: Option<PidAssignment>,
    pub(crate) audio: Vec<AudioStreamMap>,
    pub(crate) scte35: Option<Scte35Map>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProgramMapSpec {
    pub(crate) transport_stream_id: u16,
    pub(crate) programs: Vec<MpegTsProgramSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedProgramMapSpec {
    pub(crate) transport_stream_id: u16,
    pub(crate) programs: Vec<ResolvedMpegTsProgramSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedMpegTsProgramSpec {
    pub(crate) program_number: NonZeroU16,
    pub(crate) service: ServiceMetadata,
    pub(crate) pmt_pid: MpegTsPid,
    pub(crate) video_pid: Option<MpegTsPid>,
    pub(crate) audio: Vec<(AudioTrackId, MpegTsPid)>,
    pub(crate) scte35: Option<(Scte35Map, MpegTsPid)>,
    pub(crate) pcr_pid: MpegTsPid,
}

impl ProgramMapSpec {
    pub(crate) fn resolve(&self) -> Result<ResolvedProgramMapSpec, PipelineSpecError> {
        if self.transport_stream_id == 0 {
            return Err(PipelineSpecError::InvalidTransportStreamId);
        }
        if self.programs.is_empty() {
            return Err(PipelineSpecError::MissingProgram);
        }
        let mut program_numbers = BTreeSet::new();
        let mut used = BTreeSet::new();
        for program in &self.programs {
            if !program_numbers.insert(program.program_number.get()) {
                return Err(PipelineSpecError::DuplicateProgram(
                    program.program_number.get(),
                ));
            }
            if program.video_pid.is_none() && program.audio.is_empty() {
                return Err(PipelineSpecError::MissingElementaryStream(
                    program.program_number.get(),
                ));
            }
            validate_service_text("service name", &program.service.service_name)?;
            validate_service_text("provider name", &program.service.provider_name)?;
            let mut track_ids = BTreeSet::new();
            for audio in &program.audio {
                if !track_ids.insert(audio.track_id.as_str().to_ascii_lowercase()) {
                    return Err(PipelineSpecError::DuplicateAudioTrackId {
                        program: program.program_number.get(),
                        track_id: audio.track_id.as_str().to_string(),
                    });
                }
            }
            collect_manual_pid(program.pmt_pid, &mut used)?;
            if let Some(pid) = program.video_pid {
                collect_manual_pid(pid, &mut used)?;
            }
            for audio in &program.audio {
                collect_manual_pid(audio.pid, &mut used)?;
            }
            if let Some(scte35) = &program.scte35 {
                collect_manual_pid(scte35.pid, &mut used)?;
            }
        }

        let mut next_pmt = FIRST_AUTO_PMT_PID;
        let mut next_es = FIRST_AUTO_ES_PID;
        let mut programs = Vec::with_capacity(self.programs.len());
        for program in &self.programs {
            let pmt_pid = resolve_pid(program.pmt_pid, &mut next_pmt, &mut used)?;
            let video_pid = program
                .video_pid
                .map(|assignment| resolve_pid(assignment, &mut next_es, &mut used))
                .transpose()?;
            let mut audio = Vec::with_capacity(program.audio.len());
            for stream in &program.audio {
                audio.push((
                    stream.track_id.clone(),
                    resolve_pid(stream.pid, &mut next_es, &mut used)?,
                ));
            }
            let scte35 = program
                .scte35
                .as_ref()
                .map(|map| {
                    resolve_pid(map.pid, &mut next_es, &mut used).map(|pid| (map.clone(), pid))
                })
                .transpose()?;
            let pcr_pid = video_pid
                .or_else(|| audio.first().map(|(_, pid)| *pid))
                .ok_or(PipelineSpecError::MissingElementaryStream(
                    program.program_number.get(),
                ))?;
            programs.push(ResolvedMpegTsProgramSpec {
                program_number: program.program_number,
                service: program.service.clone(),
                pmt_pid,
                video_pid,
                audio,
                scte35,
                pcr_pid,
            });
        }
        Ok(ResolvedProgramMapSpec {
            transport_stream_id: self.transport_stream_id,
            programs,
        })
    }
}

fn validate_service_text(field: &'static str, value: &str) -> Result<(), PipelineSpecError> {
    validate_text(field, value)?;
    if value.chars().count() > MAX_SERVICE_TEXT_LEN {
        return Err(PipelineSpecError::ServiceMetadataTooLong { field });
    }
    Ok(())
}

fn collect_manual_pid(
    assignment: PidAssignment,
    used: &mut BTreeSet<u16>,
) -> Result<(), PipelineSpecError> {
    let PidAssignment::Manual(pid) = assignment else {
        return Ok(());
    };
    if !used.insert(pid.get()) {
        return Err(PipelineSpecError::DuplicatePid(pid.get()));
    }
    Ok(())
}

fn resolve_pid(
    assignment: PidAssignment,
    next: &mut u16,
    used: &mut BTreeSet<u16>,
) -> Result<MpegTsPid, PipelineSpecError> {
    if let PidAssignment::Manual(pid) = assignment {
        return Ok(pid);
    }
    while *next <= LAST_ASSIGNABLE_PID && used.contains(next) {
        *next = next.saturating_add(1);
    }
    if *next > LAST_ASSIGNABLE_PID {
        return Err(PipelineSpecError::PidAllocationExhausted);
    }
    let pid = MpegTsPid::new(*next)?;
    used.insert(*next);
    *next = next.saturating_add(1);
    Ok(pid)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VideoCodec {
    H264,
    H265,
    Mpeg2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AudioCodec {
    Aac,
    Ac3,
    Mp2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AudioCodecPolicy {
    MatchInput,
    Encode(AudioCodec),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RateControl {
    Cbr {
        bitrate_kbps: NonZeroU32,
    },
    Vbr {
        target_kbps: NonZeroU32,
        max_kbps: NonZeroU32,
    },
}

impl RateControl {
    fn validate(self) -> Result<(), PipelineSpecError> {
        if let Self::Vbr {
            target_kbps,
            max_kbps,
        } = self
        {
            if max_kbps < target_kbps {
                return Err(PipelineSpecError::InvalidVbrRange);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VideoEncoderSpec {
    pub(crate) codec: VideoCodec,
    pub(crate) rate_control: RateControl,
    pub(crate) gop_frames: NonZeroU32,
    pub(crate) scan: ScanMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AudioEncoderSpec {
    pub(crate) codec: AudioCodecPolicy,
    pub(crate) bitrate_kbps: NonZeroU32,
    pub(crate) sample_rate: NonZeroU32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OutputDestination {
    MpegTsUdp {
        location: String,
    },
    MpegTsSrt {
        location: String,
        latency_ms: u32,
    },
    Rtp {
        video_location: String,
        audio_locations: Vec<String>,
    },
    Rtmp {
        location: String,
    },
    File {
        location: String,
        container: String,
    },
}

impl OutputDestination {
    pub(crate) fn is_mpeg_ts(&self) -> bool {
        matches!(Self::kind(self), "mpeg_ts_udp" | "mpeg_ts_srt")
            || matches!(
                self,
                Self::File { container, .. }
                    if matches!(
                        container.trim().to_ascii_lowercase().as_str(),
                        "mpegts" | "mpeg_ts" | "ts"
                    )
            )
    }

    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::MpegTsUdp { .. } => "mpeg_ts_udp",
            Self::MpegTsSrt { .. } => "mpeg_ts_srt",
            Self::Rtp { .. } => "rtp",
            Self::Rtmp { .. } => "rtmp",
            Self::File { .. } => "file",
        }
    }

    fn validate(&self) -> Result<(), PipelineSpecError> {
        match self {
            Self::MpegTsUdp { location } => {
                validate_output_scheme("MPEG-TS/UDP", location, &["udp"], "udp://")
            }
            Self::MpegTsSrt { location, .. } => {
                validate_output_scheme("MPEG-TS/SRT", location, &["srt"], "srt://")
            }
            Self::Rtmp { location } => {
                validate_output_scheme("RTMP", location, &["rtmp", "rtmps"], "rtmp://, rtmps://")
            }
            Self::Rtp {
                video_location,
                audio_locations,
            } => {
                validate_output_scheme(
                    "RTP video",
                    video_location,
                    &["rtp", "udp"],
                    "rtp://, udp://",
                )?;
                if audio_locations.is_empty() {
                    return Err(PipelineSpecError::MissingRtpAudioEndpoint);
                }
                for location in audio_locations {
                    validate_output_scheme(
                        "RTP audio",
                        location,
                        &["rtp", "udp"],
                        "rtp://, udp://",
                    )?;
                }
                Ok(())
            }
            Self::File {
                location,
                container,
            } => {
                validate_text("file output location", location)?;
                validate_text("file output container", container)
            }
        }
    }
}

fn validate_output_scheme(
    kind: &'static str,
    value: &str,
    schemes: &[&str],
    allowed: &'static str,
) -> Result<(), PipelineSpecError> {
    validate_text("output location", value)?;
    let value = value.trim();
    if value.starts_with('$') || (value.starts_with('%') && value.ends_with('%') && value.len() > 2)
    {
        return Ok(());
    }
    let scheme = value
        .split_once("://")
        .map(|(scheme, _)| scheme.to_ascii_lowercase());
    if scheme
        .as_deref()
        .is_some_and(|scheme| schemes.contains(&scheme))
    {
        Ok(())
    } else {
        Err(PipelineSpecError::InvalidOutputScheme { kind, allowed })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncoderOutputSpec {
    pub(crate) id: OutputId,
    pub(crate) enabled: bool,
    pub(crate) destination: OutputDestination,
    pub(crate) video: VideoEncoderSpec,
    pub(crate) audio: AudioEncoderSpec,
}

impl EncoderOutputSpec {
    fn validate(&self, program_map: Option<&ProgramMapSpec>) -> Result<(), PipelineSpecError> {
        self.destination.validate()?;
        self.video.rate_control.validate()?;
        if self.destination.is_mpeg_ts() && program_map.is_none() {
            return Err(PipelineSpecError::MissingProgramMap);
        }
        if matches!(self.destination, OutputDestination::Rtmp { .. })
            && (self.video.codec != VideoCodec::H264
                || self.audio.codec != AudioCodecPolicy::Encode(AudioCodec::Aac))
        {
            return Err(PipelineSpecError::InvalidRtmpCodec);
        }
        if let OutputDestination::File { container, .. } = &self.destination {
            let container = container.trim().to_ascii_lowercase();
            match container.as_str() {
                "mpegts" | "mpeg_ts" | "ts" | "matroska" | "mkv" => {}
                "flv"
                    if self.video.codec != VideoCodec::H264
                        || self.audio.codec != AudioCodecPolicy::Encode(AudioCodec::Aac) =>
                {
                    return Err(PipelineSpecError::InvalidFlvCodec);
                }
                "flv" => {}
                "mp4" | "mov"
                    if !matches!(self.video.codec, VideoCodec::H264 | VideoCodec::H265)
                        || self.audio.codec != AudioCodecPolicy::Encode(AudioCodec::Aac) =>
                {
                    return Err(PipelineSpecError::InvalidIsoBmffCodec);
                }
                "mp4" | "mov" => {}
                "mpegps" | "mpeg_ps" | "ps"
                    if self.video.codec != VideoCodec::Mpeg2
                        || !matches!(
                            self.audio.codec,
                            AudioCodecPolicy::Encode(AudioCodec::Ac3 | AudioCodec::Mp2)
                        ) =>
                {
                    return Err(PipelineSpecError::InvalidMpegProgramStreamCodec);
                }
                "mpegps" | "mpeg_ps" | "ps" => {}
                _ => {
                    return Err(PipelineSpecError::UnsupportedFileContainer(container));
                }
            }
        }
        Ok(())
    }
}

fn validate_text(field: &'static str, value: &str) -> Result<(), PipelineSpecError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(PipelineSpecError::EmptyValue { field });
    }
    if value.chars().count() > MAX_LOCATION_LEN {
        return Err(PipelineSpecError::ValueTooLong { field });
    }
    if value
        .chars()
        .any(|ch| ch == '\0' || ch == '\r' || ch == '\n')
    {
        return Err(PipelineSpecError::InvalidControlCharacter { field });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_program_map() -> ProgramMapSpec {
        ProgramMapSpec {
            transport_stream_id: 1,
            programs: vec![MpegTsProgramSpec {
                program_number: NonZeroU16::new(1).expect("non-zero"),
                service: ServiceMetadata {
                    service_name: "Haze CGEN".to_string(),
                    provider_name: "Haze".to_string(),
                },
                pmt_pid: PidAssignment::Auto,
                video_pid: Some(PidAssignment::Auto),
                audio: vec![AudioStreamMap {
                    track_id: AudioTrackId::parse("stereo").expect("valid ID"),
                    pid: PidAssignment::Auto,
                }],
                scte35: Some(Scte35Map {
                    input: PassPolicy::Pass,
                    generated_alert_cues: true,
                    pid: PidAssignment::Auto,
                }),
            }],
        }
    }

    #[test]
    fn string_ids_reject_path_characters() {
        assert!(SceneId::new("../standby").is_err());
        assert!(OutputId::parse("output/one").is_err());
    }

    #[test]
    fn dummy_input_uses_broadcast_safe_defaults() {
        let input = DummyInput::default();
        assert_eq!(input.format.width.get(), 720);
        assert_eq!(input.format.height.get(), 480);
        assert_eq!(input.format.frame_rate.numerator.get(), 30_000);
        assert_eq!(input.format.frame_rate.denominator.get(), 1_001);
        assert_eq!(input.format.scan, ScanMode::Progressive);
    }

    #[test]
    fn deinterlace_domain_rejects_invalid_backend_and_parity_combinations() {
        assert_eq!(DeinterlaceSpec::default().validate(), Ok(()));
        let invalid_backend = DeinterlaceSpec {
            algorithm: DeinterlaceAlgorithm::Yadif,
            backend: DeinterlaceBackend::Vaapi,
            ..DeinterlaceSpec::default()
        };
        assert_eq!(
            invalid_backend.validate(),
            Err(PipelineSpecError::InvalidDeinterlaceCombination)
        );
        let invalid_parity = DeinterlaceSpec {
            algorithm: DeinterlaceAlgorithm::MotionAdaptive,
            backend: DeinterlaceBackend::Vaapi,
            parity: DeinterlaceParity::TopFirst,
            ..DeinterlaceSpec::default()
        };
        assert_eq!(
            invalid_parity.validate(),
            Err(PipelineSpecError::InvalidDeinterlaceCombination)
        );
        assert_eq!(
            DeinterlaceSpec {
                enabled: false,
                ..invalid_parity
            }
            .validate(),
            Ok(())
        );
    }

    #[test]
    fn gain_parser_accepts_muted_and_rejects_excess_gain() {
        assert_eq!(GainDb::parse("-inf"), Ok(GainDb::Muted));
        assert_eq!(GainDb::parse("0"), Ok(GainDb::Db(0.0)));
        assert_eq!(GainDb::parse("12.1"), Err(PipelineSpecError::InvalidGain));
    }

    #[test]
    fn alert_matrix_replicates_pcm_to_every_surround_channel() {
        let matrix = MixMatrix::for_alert(ChannelLayout::Surround51).expect("valid matrix");
        assert_eq!(matrix.source_channels, 1);
        assert_eq!(matrix.destination_channels, 6);
        assert_eq!(matrix.coefficients, vec![1.0; 6]);
    }

    #[test]
    fn surround_downmix_omits_lfe() {
        let matrix = MixMatrix::for_program(ChannelLayout::Surround51, ChannelLayout::Stereo)
            .expect("valid matrix");
        assert_eq!(matrix.coefficients[6], 0.0);
        assert_eq!(matrix.coefficients[7], 0.0);
        for destination in 0..2 {
            let sum = (0..6)
                .map(|source| matrix.coefficients[source * 2 + destination])
                .sum::<f32>();
            assert!((sum - 1.0).abs() < 0.000_001);
        }
    }

    #[test]
    fn stereo_upmix_leaves_synthesized_lfe_silent() {
        let matrix = MixMatrix::for_program(ChannelLayout::Stereo, ChannelLayout::Surround51)
            .expect("valid matrix");
        assert_eq!(matrix.coefficients[3], 0.0);
        assert_eq!(matrix.coefficients[9], 0.0);
        assert_eq!(matrix.coefficients[0], 1.0);
        assert_eq!(matrix.coefficients[7], 1.0);
        for destination in 0..6 {
            let sum = (0..2)
                .map(|source| matrix.coefficients[source * 6 + destination])
                .sum::<f32>();
            assert!(sum <= 1.0);
        }
    }

    #[test]
    fn auto_pid_allocation_uses_existing_default_ranges() {
        let resolved = test_program_map().resolve().expect("program map resolves");
        let program = &resolved.programs[0];
        assert_eq!(program.pmt_pid.get(), 0x1000);
        assert_eq!(program.video_pid.map(MpegTsPid::get), Some(0x0100));
        assert_eq!(program.pcr_pid.get(), 0x0100);
        assert_eq!(program.audio[0].1.get(), 0x0101);
        assert_eq!(
            program.scte35.as_ref().map(|(_, pid)| pid.get()),
            Some(0x0102)
        );
    }

    #[test]
    fn zero_transport_stream_id_is_rejected() {
        let mut map = test_program_map();
        map.transport_stream_id = 0;
        assert_eq!(
            map.resolve(),
            Err(PipelineSpecError::InvalidTransportStreamId)
        );
    }

    #[test]
    fn duplicate_manual_pid_is_rejected() {
        let mut map = test_program_map();
        let pid = MpegTsPid::new(0x0100).expect("valid PID");
        map.programs[0].video_pid = Some(PidAssignment::Manual(pid));
        map.programs[0].audio[0].pid = PidAssignment::Manual(pid);
        assert_eq!(map.resolve(), Err(PipelineSpecError::DuplicatePid(0x0100)));
    }

    #[test]
    fn audio_only_program_uses_first_audio_pid_for_pcr() {
        let mut map = test_program_map();
        map.programs[0].video_pid = None;
        let resolved = map.resolve().expect("audio-only map resolves");
        assert_eq!(resolved.programs[0].pcr_pid.get(), 0x0100);
    }

    #[test]
    fn duplicate_audio_track_ids_are_case_insensitive() {
        let mut map = test_program_map();
        map.programs[0].audio.push(AudioStreamMap {
            track_id: AudioTrackId::parse("STEREO").expect("valid ID"),
            pid: PidAssignment::Auto,
        });
        assert_eq!(
            map.resolve(),
            Err(PipelineSpecError::DuplicateAudioTrackId {
                program: 1,
                track_id: "STEREO".to_string(),
            })
        );
    }

    #[test]
    fn service_metadata_is_required_and_bounded() {
        let mut map = test_program_map();
        map.programs[0].service.provider_name.clear();
        assert_eq!(
            map.resolve(),
            Err(PipelineSpecError::EmptyValue {
                field: "provider name",
            })
        );
        map.programs[0].service.provider_name = "H".repeat(MAX_SERVICE_TEXT_LEN + 1);
        assert_eq!(
            map.resolve(),
            Err(PipelineSpecError::ServiceMetadataTooLong {
                field: "provider name",
            })
        );
    }

    #[test]
    fn alert_scene_rejects_non_alert_protected_scenes() {
        for id in ["Program_Passthrough", "Standby"] {
            let spec = CompositorSpec {
                alert_scene_id: SceneId::new(id).expect("valid ID"),
            };
            assert_eq!(spec.validate(), Err(PipelineSpecError::InvalidAlertScene));
        }
    }

    #[test]
    fn rtmp_rejects_non_aac_audio() {
        let output = EncoderOutputSpec {
            id: OutputId::parse("primary").expect("valid ID"),
            enabled: true,
            destination: OutputDestination::Rtmp {
                location: "rtmp://example.test/live".to_string(),
            },
            video: VideoEncoderSpec {
                codec: VideoCodec::H264,
                rate_control: RateControl::Cbr {
                    bitrate_kbps: NonZeroU32::new(4_000).expect("non-zero"),
                },
                gop_frames: NonZeroU32::new(60).expect("non-zero"),
                scan: ScanMode::Progressive,
            },
            audio: AudioEncoderSpec {
                codec: AudioCodecPolicy::Encode(AudioCodec::Ac3),
                bitrate_kbps: NonZeroU32::new(192).expect("non-zero"),
                sample_rate: NonZeroU32::new(48_000).expect("non-zero"),
            },
        };
        assert_eq!(
            output.validate(None),
            Err(PipelineSpecError::InvalidRtmpCodec)
        );
    }

    #[test]
    fn mpeg_ts_file_requires_a_program_map() {
        let output = EncoderOutputSpec {
            id: OutputId::parse("archive").expect("valid ID"),
            enabled: true,
            destination: OutputDestination::File {
                location: "archive.ts".to_string(),
                container: "mpegts".to_string(),
            },
            video: VideoEncoderSpec {
                codec: VideoCodec::H264,
                rate_control: RateControl::Cbr {
                    bitrate_kbps: NonZeroU32::new(4_000).expect("non-zero"),
                },
                gop_frames: NonZeroU32::new(60).expect("non-zero"),
                scan: ScanMode::Progressive,
            },
            audio: AudioEncoderSpec {
                codec: AudioCodecPolicy::Encode(AudioCodec::Aac),
                bitrate_kbps: NonZeroU32::new(192).expect("non-zero"),
                sample_rate: NonZeroU32::new(48_000).expect("non-zero"),
            },
        };

        assert_eq!(
            output.validate(None),
            Err(PipelineSpecError::MissingProgramMap)
        );
        assert_eq!(output.validate(Some(&test_program_map())), Ok(()));
    }

    #[test]
    fn file_container_compatibility_is_validated_before_activation() {
        let mut output = EncoderOutputSpec {
            id: OutputId::parse("archive").expect("valid ID"),
            enabled: true,
            destination: OutputDestination::File {
                location: "archive.mp4".to_string(),
                container: "mp4".to_string(),
            },
            video: VideoEncoderSpec {
                codec: VideoCodec::Mpeg2,
                rate_control: RateControl::Cbr {
                    bitrate_kbps: NonZeroU32::new(4_000).expect("non-zero"),
                },
                gop_frames: NonZeroU32::new(60).expect("non-zero"),
                scan: ScanMode::Progressive,
            },
            audio: AudioEncoderSpec {
                codec: AudioCodecPolicy::Encode(AudioCodec::Ac3),
                bitrate_kbps: NonZeroU32::new(192).expect("non-zero"),
                sample_rate: NonZeroU32::new(48_000).expect("non-zero"),
            },
        };
        assert_eq!(
            output.validate(None),
            Err(PipelineSpecError::InvalidIsoBmffCodec)
        );

        output.destination = OutputDestination::File {
            location: "archive.bin".to_string(),
            container: "unknown".to_string(),
        };
        assert_eq!(
            output.validate(None),
            Err(PipelineSpecError::UnsupportedFileContainer(
                "unknown".to_string()
            ))
        );
    }

    #[test]
    fn rtp_requires_explicit_audio_endpoints() {
        let destination = OutputDestination::Rtp {
            video_location: "rtp://127.0.0.1:5004".to_string(),
            audio_locations: Vec::new(),
        };
        assert_eq!(
            destination.validate(),
            Err(PipelineSpecError::MissingRtpAudioEndpoint)
        );
    }

    #[test]
    fn output_protocols_reject_incompatible_literal_schemes() {
        let destination = OutputDestination::MpegTsUdp {
            location: "http://example.invalid/output".to_string(),
        };
        assert_eq!(
            destination.validate(),
            Err(PipelineSpecError::InvalidOutputScheme {
                kind: "MPEG-TS/UDP",
                allowed: "udp://",
            })
        );
    }

    #[test]
    fn output_protocols_allow_unexpanded_environment_references() {
        let destination = OutputDestination::Rtmp {
            location: "${CGEN_RTMP_URL}".to_string(),
        };
        assert_eq!(destination.validate(), Ok(()));
    }

    #[test]
    fn audio_topology_and_output_codec_policy_must_agree() {
        let output = EncoderOutputSpec {
            id: OutputId::parse("primary").expect("valid ID"),
            enabled: true,
            destination: OutputDestination::File {
                location: "output.ts".to_string(),
                container: "mpegts".to_string(),
            },
            video: VideoEncoderSpec {
                codec: VideoCodec::H264,
                rate_control: RateControl::Cbr {
                    bitrate_kbps: NonZeroU32::new(4_000).expect("non-zero"),
                },
                gop_frames: NonZeroU32::new(60).expect("non-zero"),
                scan: ScanMode::Progressive,
            },
            audio: AudioEncoderSpec {
                codec: AudioCodecPolicy::MatchInput,
                bitrate_kbps: NonZeroU32::new(192).expect("non-zero"),
                sample_rate: NonZeroU32::new(48_000).expect("non-zero"),
            },
        };
        let pipeline = PipelineSpec {
            feed_id: FeedId::parse("feed").expect("valid ID"),
            input: ProgramInput::Dummy(DummyInput::default()),
            alert_feed_id: FeedId::parse("feed").expect("valid ID"),
            ancillary: AncillaryPolicy::default(),
            audio: AudioRoutingSpec {
                topology: AudioTopologyMode::ForceLayout(ChannelLayout::Stereo),
                ..AudioRoutingSpec::default()
            },
            compositor: CompositorSpec {
                alert_scene_id: SceneId::new("Standard_Crawl").expect("valid ID"),
            },
            program_map: None,
            outputs: vec![output],
        };
        assert_eq!(
            pipeline.validate(),
            Err(PipelineSpecError::ForcedLayoutRequiresEncoder)
        );
    }
}
