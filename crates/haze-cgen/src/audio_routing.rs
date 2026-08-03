use std::collections::BTreeSet;

use thiserror::Error;

use crate::architecture::{
    AudioCodec, AudioCodecPolicy, AudioRoutingSpec, AudioTopologyMode, AudioTrackId, ChannelLayout,
    EncoderOutputSpec, GainDb, MixMatrix, OutputDestination, PipelineSpecError,
};

const MAX_LANGUAGE_LEN: usize = 35;

#[derive(Debug, Error, Clone, PartialEq)]
pub(crate) enum AudioRoutingError {
    #[error(transparent)]
    InvalidMatrix(#[from] PipelineSpecError),
    #[error("preserve-native audio requires at least one source track")]
    MissingSourceTrack,
    #[error("source audio track IDs must be unique")]
    DuplicateTrackId,
    #[error("source audio track ordering values must be unique")]
    DuplicateTrackOrder,
    #[error("audio language metadata is invalid")]
    InvalidLanguage,
    #[error("output destination cannot retain the source audio codec")]
    UnsupportedRetainedCodec,
    #[error("preserve-native output must use Match Input")]
    PreserveRequiresMatchInput,
    #[error("forced-layout output must select an encoder codec")]
    ForcedLayoutRequiresCodec,
    #[error("PCM block length is not aligned to its channel layout")]
    MisalignedPcm,
    #[error("program and alert PCM blocks describe different frame counts")]
    FrameCountMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceAudioTrack {
    pub(crate) id: AudioTrackId,
    pub(crate) codec: AudioCodec,
    pub(crate) language: Option<String>,
    pub(crate) layout: ChannelLayout,
    pub(crate) order: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AudioBranchPlan {
    pub(crate) source_track: Option<SourceAudioTrack>,
    pub(crate) codec: AudioCodec,
    pub(crate) output_layout: ChannelLayout,
    pub(crate) program_matrix: MixMatrix,
    pub(crate) alert_matrix: MixMatrix,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AudioRoutingPlan {
    pub(crate) branches: Vec<AudioBranchPlan>,
    pub(crate) preserve_native_tracks: bool,
}

impl AudioRoutingPlan {
    pub(crate) fn build(
        spec: &AudioRoutingSpec,
        source_tracks: &[SourceAudioTrack],
        forced_source_layout: ChannelLayout,
        output: &EncoderOutputSpec,
    ) -> Result<Self, AudioRoutingError> {
        validate_source_tracks(source_tracks)?;
        match spec.topology {
            AudioTopologyMode::PreserveNativeTracks => {
                if output.audio.codec != AudioCodecPolicy::MatchInput {
                    return Err(AudioRoutingError::PreserveRequiresMatchInput);
                }
                if source_tracks.is_empty() {
                    return Err(AudioRoutingError::MissingSourceTrack);
                }
                let mut tracks = source_tracks.to_vec();
                tracks.sort_by_key(|track| track.order);
                let branches = tracks
                    .into_iter()
                    .map(|track| {
                        if !destination_supports_codec(&output.destination, track.codec) {
                            return Err(AudioRoutingError::UnsupportedRetainedCodec);
                        }
                        Ok(AudioBranchPlan {
                            codec: track.codec,
                            output_layout: track.layout,
                            program_matrix: MixMatrix::for_program(track.layout, track.layout)?,
                            alert_matrix: MixMatrix::for_alert(track.layout)?,
                            source_track: Some(track),
                        })
                    })
                    .collect::<Result<Vec<_>, AudioRoutingError>>()?;
                Ok(Self {
                    branches,
                    preserve_native_tracks: true,
                })
            }
            AudioTopologyMode::ForceLayout(output_layout) => {
                let AudioCodecPolicy::Encode(codec) = output.audio.codec else {
                    return Err(AudioRoutingError::ForcedLayoutRequiresCodec);
                };
                Ok(Self {
                    branches: vec![AudioBranchPlan {
                        source_track: None,
                        codec,
                        output_layout,
                        program_matrix: MixMatrix::for_program(
                            forced_source_layout,
                            output_layout,
                        )?,
                        alert_matrix: MixMatrix::for_alert(output_layout)?,
                    }],
                    preserve_native_tracks: false,
                })
            }
        }
    }
}

fn validate_source_tracks(tracks: &[SourceAudioTrack]) -> Result<(), AudioRoutingError> {
    let mut ids = BTreeSet::new();
    let mut order = BTreeSet::new();
    for track in tracks {
        if !ids.insert(track.id.as_str().to_ascii_lowercase()) {
            return Err(AudioRoutingError::DuplicateTrackId);
        }
        if !order.insert(track.order) {
            return Err(AudioRoutingError::DuplicateTrackOrder);
        }
        if let Some(language) = &track.language {
            let language = language.trim();
            if language.is_empty()
                || language.chars().count() > MAX_LANGUAGE_LEN
                || !language
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '-')
            {
                return Err(AudioRoutingError::InvalidLanguage);
            }
        }
    }
    Ok(())
}

fn destination_supports_codec(destination: &OutputDestination, codec: AudioCodec) -> bool {
    match destination {
        OutputDestination::MpegTsUdp { .. }
        | OutputDestination::MpegTsSrt { .. }
        | OutputDestination::Rtp { .. } => true,
        OutputDestination::Rtmp { .. } => codec == AudioCodec::Aac,
        OutputDestination::File { container, .. } => {
            match container.trim().to_ascii_lowercase().as_str() {
                "mpegts" | "mpeg-ts" | "ts" => true,
                "flv" | "mp4" | "m4a" => codec == AudioCodec::Aac,
                _ => false,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct GainRamp {
    current: f32,
    target: f32,
    step: f32,
    frames_remaining: u32,
}

impl GainRamp {
    fn new(gain: GainDb) -> Self {
        let value = gain.linear();
        Self {
            current: value,
            target: value,
            step: 0.0,
            frames_remaining: 0,
        }
    }

    fn transition_to(&mut self, gain: GainDb, sample_rate: u32, transition_ms: u16) {
        let target = gain.linear();
        if (target - self.target).abs() <= f32::EPSILON {
            return;
        }
        let frames = u64::from(sample_rate)
            .saturating_mul(u64::from(transition_ms))
            .div_ceil(1_000)
            .max(1);
        self.target = target;
        self.frames_remaining = u32::try_from(frames).unwrap_or(u32::MAX);
        self.step = (self.target - self.current) / self.frames_remaining as f32;
    }

    fn next(&mut self) -> f32 {
        let value = self.current;
        if self.frames_remaining > 0 {
            self.current += self.step;
            self.frames_remaining -= 1;
            if self.frames_remaining == 0 {
                self.current = self.target;
            }
        }
        value
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AudioGainController {
    program: GainRamp,
    alert: GainRamp,
}

impl AudioGainController {
    pub(crate) fn new(spec: &AudioRoutingSpec) -> Self {
        Self {
            program: GainRamp::new(spec.idle_program_gain),
            alert: GainRamp::new(GainDb::Muted),
        }
    }

    pub(crate) fn set_alert_active(
        &mut self,
        active: bool,
        spec: &AudioRoutingSpec,
        sample_rate: u32,
    ) {
        let program = if active {
            spec.alert_program_gain
        } else {
            spec.idle_program_gain
        };
        let alert = if active {
            spec.alert_gain
        } else {
            GainDb::Muted
        };
        self.program
            .transition_to(program, sample_rate, spec.transition_ms);
        self.alert
            .transition_to(alert, sample_rate, spec.transition_ms);
    }

    pub(crate) fn set_targets(
        &mut self,
        program: GainDb,
        alert: GainDb,
        sample_rate: u32,
        transition_ms: u16,
    ) {
        self.program
            .transition_to(program, sample_rate, transition_ms);
        self.alert.transition_to(alert, sample_rate, transition_ms);
    }

    pub(crate) fn prime_alert(&mut self, gain: GainDb) {
        self.alert = GainRamp::new(gain);
    }

    pub(crate) fn next_program_gain(&mut self) -> f32 {
        self.program.next()
    }

    pub(crate) fn next_alert_gain(&mut self) -> f32 {
        self.alert.next()
    }

    pub(crate) fn status(&self) -> AudioGainStatus {
        AudioGainStatus {
            program_current: self.program.current,
            program_target: self.program.target,
            alert_current: self.alert.current,
            alert_target: self.alert.target,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct AudioGainStatus {
    pub(crate) program_current: f32,
    pub(crate) program_target: f32,
    pub(crate) alert_current: f32,
    pub(crate) alert_target: f32,
}

pub(crate) fn mix_interleaved_f32(
    program: &[f32],
    alert_mono: &[f32],
    matrix: &MixMatrix,
    gains: &mut AudioGainController,
) -> Result<Vec<f32>, AudioRoutingError> {
    let source_channels = usize::from(matrix.source_channels);
    let destination_channels = usize::from(matrix.destination_channels);
    if source_channels == 0 || program.len() % source_channels != 0 {
        return Err(AudioRoutingError::MisalignedPcm);
    }
    let frames = program.len() / source_channels;
    if !alert_mono.is_empty() && alert_mono.len() != frames {
        return Err(AudioRoutingError::FrameCountMismatch);
    }
    let mut output = vec![0.0; frames.saturating_mul(destination_channels)];
    for frame in 0..frames {
        let program_gain = gains.program.next();
        let alert_gain = gains.alert.next();
        let alert_sample = alert_mono.get(frame).copied().unwrap_or(0.0) * alert_gain;
        for destination in 0..destination_channels {
            let mut sample = alert_sample;
            for source in 0..source_channels {
                let coefficient = matrix.coefficients[source * destination_channels + destination];
                sample += program[frame * source_channels + source] * coefficient * program_gain;
            }
            output[frame * destination_channels + destination] = sample.clamp(-1.0, 1.0);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::architecture::{RateControl, VideoCodec, VideoEncoderSpec};

    fn output(destination: OutputDestination, codec: AudioCodecPolicy) -> EncoderOutputSpec {
        EncoderOutputSpec {
            id: crate::architecture::OutputId::parse("output").expect("valid ID"),
            enabled: true,
            destination,
            video: VideoEncoderSpec {
                codec: VideoCodec::H264,
                rate_control: RateControl::Cbr {
                    bitrate_kbps: NonZeroU32::new(4_000).expect("non-zero"),
                },
                gop_frames: NonZeroU32::new(60).expect("non-zero"),
                scan: crate::architecture::ScanMode::Progressive,
            },
            audio: crate::architecture::AudioEncoderSpec {
                codec,
                bitrate_kbps: NonZeroU32::new(192).expect("non-zero"),
                sample_rate: NonZeroU32::new(48_000).expect("non-zero"),
            },
        }
    }

    #[test]
    fn preserve_mode_keeps_track_order_codec_language_and_layout() {
        let tracks = vec![
            SourceAudioTrack {
                id: AudioTrackId::parse("secondary").expect("valid ID"),
                codec: AudioCodec::Ac3,
                language: Some("fra".to_string()),
                layout: ChannelLayout::Surround51,
                order: 2,
            },
            SourceAudioTrack {
                id: AudioTrackId::parse("primary").expect("valid ID"),
                codec: AudioCodec::Aac,
                language: Some("eng".to_string()),
                layout: ChannelLayout::Stereo,
                order: 1,
            },
        ];
        let plan = AudioRoutingPlan::build(
            &AudioRoutingSpec::default(),
            &tracks,
            ChannelLayout::Stereo,
            &output(
                OutputDestination::MpegTsUdp {
                    location: "udp://239.0.0.1:9000".to_string(),
                },
                AudioCodecPolicy::MatchInput,
            ),
        )
        .expect("plan");

        assert!(plan.preserve_native_tracks);
        assert_eq!(plan.branches[0].source_track.as_ref().unwrap().order, 1);
        assert_eq!(plan.branches[0].codec, AudioCodec::Aac);
        assert_eq!(
            plan.branches[1].source_track.as_ref().unwrap().language,
            Some("fra".to_string())
        );
        assert_eq!(plan.branches[1].alert_matrix.coefficients, vec![1.0; 6]);
    }

    #[test]
    fn preserve_mode_rejects_codec_the_destination_cannot_carry() {
        let tracks = vec![SourceAudioTrack {
            id: AudioTrackId::parse("primary").expect("valid ID"),
            codec: AudioCodec::Ac3,
            language: Some("eng".to_string()),
            layout: ChannelLayout::Stereo,
            order: 0,
        }];
        let error = AudioRoutingPlan::build(
            &AudioRoutingSpec::default(),
            &tracks,
            ChannelLayout::Stereo,
            &output(
                OutputDestination::Rtmp {
                    location: "rtmp://example.invalid/live".to_string(),
                },
                AudioCodecPolicy::MatchInput,
            ),
        )
        .expect_err("AC3 cannot be retained in RTMP");
        assert_eq!(error, AudioRoutingError::UnsupportedRetainedCodec);
    }

    #[test]
    fn surround_impulse_downmix_omits_lfe_and_stays_headroom_safe() {
        let spec = AudioRoutingSpec {
            topology: AudioTopologyMode::ForceLayout(ChannelLayout::Stereo),
            ..AudioRoutingSpec::default()
        };
        let matrix = MixMatrix::for_program(ChannelLayout::Surround51, ChannelLayout::Stereo)
            .expect("matrix");
        let mut gains = AudioGainController::new(&spec);
        let lfe_only = [0.0, 0.0, 0.0, 1.0, 0.0, 0.0];
        assert_eq!(
            mix_interleaved_f32(&lfe_only, &[], &matrix, &mut gains).expect("mix"),
            vec![0.0, 0.0]
        );

        let correlated = [1.0; 6];
        let mixed = mix_interleaved_f32(&correlated, &[], &matrix, &mut gains).expect("mix");
        assert!(mixed.iter().all(|sample| (*sample - 1.0).abs() < 0.000_001));
    }

    #[test]
    fn alert_gain_moves_click_free_over_twenty_milliseconds() {
        let spec = AudioRoutingSpec {
            topology: AudioTopologyMode::ForceLayout(ChannelLayout::Stereo),
            ..AudioRoutingSpec::default()
        };
        let matrix =
            MixMatrix::for_program(ChannelLayout::Mono, ChannelLayout::Stereo).expect("matrix");
        let mut gains = AudioGainController::new(&spec);
        gains.set_alert_active(true, &spec, 48_000);
        let program = vec![1.0; 961];
        let alert = vec![1.0; 961];
        let mixed = mix_interleaved_f32(&program, &alert, &matrix, &mut gains).expect("mix");
        assert!((mixed[0] - 1.0).abs() < 0.000_001);
        let last_frame = &mixed[mixed.len() - 2..];
        assert!(last_frame
            .iter()
            .all(|sample| (*sample - 1.0).abs() < 0.000_001));
        assert!(mixed
            .windows(2)
            .all(|window| (window[1] - window[0]).abs() <= 1.0));
    }

    #[test]
    fn normalized_surround_sine_downmix_remains_finite_and_bounded() {
        let spec = AudioRoutingSpec {
            topology: AudioTopologyMode::ForceLayout(ChannelLayout::Stereo),
            ..AudioRoutingSpec::default()
        };
        let matrix = MixMatrix::for_program(ChannelLayout::Surround51, ChannelLayout::Stereo)
            .expect("matrix");
        let mut program = Vec::new();
        for frame in 0..480 {
            let sample = (std::f32::consts::TAU * 1_000.0 * frame as f32 / 48_000.0).sin();
            program.extend_from_slice(&[sample, sample, sample, 1.0, sample, sample]);
        }
        let mut gains = AudioGainController::new(&spec);
        let mixed = mix_interleaved_f32(&program, &[], &matrix, &mut gains).expect("mix");
        assert!(mixed.iter().all(|sample| sample.is_finite()));
        assert!(mixed.iter().all(|sample| sample.abs() <= 1.0));
        assert!(mixed
            .chunks_exact(2)
            .all(|frame| (frame[0] - frame[1]).abs() < 0.000_001));
    }
}
