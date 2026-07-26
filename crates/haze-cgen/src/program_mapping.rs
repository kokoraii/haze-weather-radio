//! Pure MPEG-TS program-map planning and SCTE-35 section encoding.
//!
//! This module does not make GStreamer API calls or inject ancillary packets.
//! It converts already validated domain configuration into deterministic data
//! consumed by the CGEN mux configuration, while SCTE-35 insertion remains
//! capability-gated until the native section API is wired on every target.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;

use serde_json::{json, Value};
use thiserror::Error;

use crate::ancillary::{CueCommand, GeneratedScte35Cue};
use crate::architecture::{
    AudioTrackId, MpegTsPid, PassPolicy, ResolvedMpegTsProgramSpec, ResolvedProgramMapSpec,
};

const MAX_SERVICE_METADATA_CHARS: usize = 255;
const SCTE35_TABLE_ID: u8 = 0xfc;
const SPLICE_INSERT_COMMAND_TYPE: u8 = 0x05;
const SPLICE_INSERT_COMMAND_LENGTH: u16 = 15;
const SCTE35_TIER: u16 = 0x0fff;
const PTS_33_MASK: u64 = (1_u64 << 33) - 1;

/// One encoded video branch and the mux sink PID it will request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlannedVideoTrackAssociation {
    pub(crate) program_number: NonZeroU16,
    pub(crate) sink_pid: MpegTsPid,
}

/// One encoded audio branch and the configured logical track it represents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedAudioTrackAssociation {
    pub(crate) program_number: NonZeroU16,
    pub(crate) track_id: AudioTrackId,
    pub(crate) sink_pid: MpegTsPid,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PlannedTrackAssociations {
    pub(crate) video: Vec<PlannedVideoTrackAssociation>,
    pub(crate) audio: Vec<PlannedAudioTrackAssociation>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum ProgramMapError {
    #[error("transport stream ID must be non-zero")]
    InvalidTransportStreamId,
    #[error("at least one MPEG-TS program is required")]
    MissingProgram,
    #[error("program number {0} occurs more than once")]
    DuplicateProgram(u16),
    #[error("PID {0:#06x} occurs more than once in the resolved program map")]
    DuplicateConfiguredPid(u16),
    #[error("program {program} has invalid {field}")]
    InvalidServiceMetadata { program: u16, field: &'static str },
    #[error("program {program} has no valid PCR elementary-stream PID")]
    InvalidPcrPid { program: u16 },
    #[error("planned {kind} association references unknown program {program}")]
    UnknownAssociationProgram { kind: &'static str, program: u16 },
    #[error("program {program} has more than one planned video association")]
    DuplicateVideoAssociation { program: u16 },
    #[error("program {program} has no planned video association")]
    MissingVideoAssociation { program: u16 },
    #[error("program {program} does not define a video stream")]
    UnexpectedVideoAssociation { program: u16 },
    #[error(
        "program {program} video association uses PID {actual:#06x}, expected {expected:#06x}"
    )]
    VideoPidMismatch {
        program: u16,
        expected: u16,
        actual: u16,
    },
    #[error("program {program} has more than one association for audio track {track_id}")]
    DuplicateAudioAssociation { program: u16, track_id: String },
    #[error("program {program} defines audio track {track_id} more than once")]
    DuplicateConfiguredAudioTrack { program: u16, track_id: String },
    #[error("program {program} has no association for audio track {track_id}")]
    MissingAudioAssociation { program: u16, track_id: String },
    #[error("program {program} does not define audio track {track_id}")]
    UnexpectedAudioAssociation { program: u16, track_id: String },
    #[error(
        "program {program} audio track {track_id} uses PID {actual:#06x}, expected {expected:#06x}"
    )]
    AudioPidMismatch {
        program: u16,
        track_id: String,
        expected: u16,
        actual: u16,
    },
    #[error("planned mux sink PID {0:#06x} occurs more than once")]
    DuplicateSinkPid(u16),
}

/// A validated GStreamer `prog-map` structure and its redacted operator view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedGstProgramMap {
    structure: String,
    spec: ResolvedProgramMapSpec,
    sink_programs: BTreeMap<u16, u16>,
}

impl ValidatedGstProgramMap {
    pub(crate) fn structure(&self) -> &str {
        &self.structure
    }

    /// Contains only broadcast service metadata and numeric transport IDs. It
    /// never includes source locations, destination credentials, or secrets.
    pub(crate) fn redacted_status_value(&self) -> Value {
        let mut programs = self.spec.programs.iter().collect::<Vec<_>>();
        programs.sort_by_key(|program| program.program_number);
        json!({
            "redacted": true,
            "transport_stream_id": self.spec.transport_stream_id,
            "pat_pid": 0,
            "prog_map": self.structure.as_str(),
            "sink_pids": self.sink_programs.keys().copied().collect::<Vec<_>>(),
            "programs": programs.into_iter().map(program_status_value).collect::<Vec<_>>(),
        })
    }
}

/// Returns the safe operator-facing portion of a resolved MPEG-TS map before
/// it has been bound to concrete GStreamer encoder branches.
pub(crate) fn redacted_resolved_program_map_status(spec: &ResolvedProgramMapSpec) -> Value {
    let mut programs = spec.programs.iter().collect::<Vec<_>>();
    programs.sort_by_key(|program| program.program_number);
    json!({
        "redacted": true,
        "transport_stream_id": spec.transport_stream_id,
        "pat_pid": 0,
        "programs": programs.into_iter().map(program_status_value).collect::<Vec<_>>(),
    })
}

/// Validates that the concrete encoder branches exactly match the resolved
/// domain map, then builds the structure accepted by GStreamer's `prog-map`
/// property. No GStreamer API is called here.
pub(crate) fn build_gstreamer_program_map(
    spec: &ResolvedProgramMapSpec,
    associations: &PlannedTrackAssociations,
) -> Result<ValidatedGstProgramMap, ProgramMapError> {
    validate_resolved_program_map(spec)?;

    let programs = spec
        .programs
        .iter()
        .map(|program| (program.program_number.get(), program))
        .collect::<BTreeMap<_, _>>();
    let mut sink_programs = BTreeMap::<u16, u16>::new();
    let mut videos = BTreeMap::<u16, MpegTsPid>::new();
    for association in &associations.video {
        let program_number = association.program_number.get();
        let Some(program) = programs.get(&program_number) else {
            return Err(ProgramMapError::UnknownAssociationProgram {
                kind: "video",
                program: program_number,
            });
        };
        if videos
            .insert(program_number, association.sink_pid)
            .is_some()
        {
            return Err(ProgramMapError::DuplicateVideoAssociation {
                program: program_number,
            });
        }
        let Some(expected) = program.video_pid else {
            return Err(ProgramMapError::UnexpectedVideoAssociation {
                program: program_number,
            });
        };
        if association.sink_pid != expected {
            return Err(ProgramMapError::VideoPidMismatch {
                program: program_number,
                expected: expected.get(),
                actual: association.sink_pid.get(),
            });
        }
        insert_sink_program(&mut sink_programs, association.sink_pid, program_number)?;
    }

    let mut audios = BTreeMap::<(u16, String), MpegTsPid>::new();
    for association in &associations.audio {
        let program_number = association.program_number.get();
        let Some(program) = programs.get(&program_number) else {
            return Err(ProgramMapError::UnknownAssociationProgram {
                kind: "audio",
                program: program_number,
            });
        };
        let normalized_track_id = association.track_id.as_str().to_ascii_lowercase();
        let key = (program_number, normalized_track_id.clone());
        if audios.insert(key, association.sink_pid).is_some() {
            return Err(ProgramMapError::DuplicateAudioAssociation {
                program: program_number,
                track_id: association.track_id.as_str().to_string(),
            });
        }
        let expected = program
            .audio
            .iter()
            .find(|(track_id, _)| track_id.as_str().eq_ignore_ascii_case(&normalized_track_id))
            .map(|(_, pid)| *pid)
            .ok_or_else(|| ProgramMapError::UnexpectedAudioAssociation {
                program: program_number,
                track_id: association.track_id.as_str().to_string(),
            })?;
        if association.sink_pid != expected {
            return Err(ProgramMapError::AudioPidMismatch {
                program: program_number,
                track_id: association.track_id.as_str().to_string(),
                expected: expected.get(),
                actual: association.sink_pid.get(),
            });
        }
        insert_sink_program(&mut sink_programs, association.sink_pid, program_number)?;
    }

    for program in programs.values() {
        let program_number = program.program_number.get();
        if program.video_pid.is_some() && !videos.contains_key(&program_number) {
            return Err(ProgramMapError::MissingVideoAssociation {
                program: program_number,
            });
        }
        for (track_id, _) in &program.audio {
            let key = (program_number, track_id.as_str().to_ascii_lowercase());
            if !audios.contains_key(&key) {
                return Err(ProgramMapError::MissingAudioAssociation {
                    program: program_number,
                    track_id: track_id.as_str().to_string(),
                });
            }
        }
    }

    let mut fields = Vec::with_capacity(1 + sink_programs.len() + programs.len() * 2);
    fields.push("program_map".to_string());
    fields.extend(
        sink_programs
            .iter()
            .map(|(pid, program)| format!("sink_{pid}=(int){program}")),
    );
    for (program_number, program) in &programs {
        fields.push(format!(
            "PMT_{program_number}=(uint){}",
            program.pmt_pid.get()
        ));
        fields.push(format!(
            "PCR_{program_number}=(int){}",
            program.pcr_pid.get()
        ));
    }
    Ok(ValidatedGstProgramMap {
        structure: fields.join(","),
        spec: spec.clone(),
        sink_programs,
    })
}

fn validate_resolved_program_map(spec: &ResolvedProgramMapSpec) -> Result<(), ProgramMapError> {
    if spec.transport_stream_id == 0 {
        return Err(ProgramMapError::InvalidTransportStreamId);
    }
    if spec.programs.is_empty() {
        return Err(ProgramMapError::MissingProgram);
    }
    let mut programs = BTreeSet::new();
    let mut pids = BTreeSet::new();
    for program in &spec.programs {
        let program_number = program.program_number.get();
        if !programs.insert(program_number) {
            return Err(ProgramMapError::DuplicateProgram(program_number));
        }
        validate_service_metadata(
            program_number,
            "service name",
            &program.service.service_name,
        )?;
        validate_service_metadata(
            program_number,
            "provider name",
            &program.service.provider_name,
        )?;
        insert_configured_pid(&mut pids, program.pmt_pid)?;
        if let Some(pid) = program.video_pid {
            insert_configured_pid(&mut pids, pid)?;
        }
        let mut track_ids = BTreeSet::new();
        for (track_id, pid) in &program.audio {
            if !track_ids.insert(track_id.as_str().to_ascii_lowercase()) {
                return Err(ProgramMapError::DuplicateConfiguredAudioTrack {
                    program: program_number,
                    track_id: track_id.as_str().to_string(),
                });
            }
            insert_configured_pid(&mut pids, *pid)?;
        }
        if let Some((_, pid)) = &program.scte35 {
            insert_configured_pid(&mut pids, *pid)?;
        }
        let pcr_is_video = program.video_pid == Some(program.pcr_pid);
        let pcr_is_audio = program.audio.iter().any(|(_, pid)| *pid == program.pcr_pid);
        if !pcr_is_video && !pcr_is_audio {
            return Err(ProgramMapError::InvalidPcrPid {
                program: program_number,
            });
        }
    }
    Ok(())
}

fn validate_service_metadata(
    program: u16,
    field: &'static str,
    value: &str,
) -> Result<(), ProgramMapError> {
    let value = value.trim();
    if value.is_empty()
        || value.chars().count() > MAX_SERVICE_METADATA_CHARS
        || value
            .chars()
            .any(|character| matches!(character, '\0' | '\r' | '\n'))
    {
        return Err(ProgramMapError::InvalidServiceMetadata { program, field });
    }
    Ok(())
}

fn insert_configured_pid(pids: &mut BTreeSet<u16>, pid: MpegTsPid) -> Result<(), ProgramMapError> {
    if !pids.insert(pid.get()) {
        return Err(ProgramMapError::DuplicateConfiguredPid(pid.get()));
    }
    Ok(())
}

fn insert_sink_program(
    sinks: &mut BTreeMap<u16, u16>,
    pid: MpegTsPid,
    program: u16,
) -> Result<(), ProgramMapError> {
    if sinks.insert(pid.get(), program).is_some() {
        return Err(ProgramMapError::DuplicateSinkPid(pid.get()));
    }
    Ok(())
}

fn program_status_value(program: &ResolvedMpegTsProgramSpec) -> Value {
    let mut audio = program.audio.iter().collect::<Vec<_>>();
    audio.sort_by(|(left_id, left_pid), (right_id, right_pid)| {
        left_pid
            .cmp(right_pid)
            .then_with(|| left_id.as_str().cmp(right_id.as_str()))
    });
    json!({
        "program_number": program.program_number.get(),
        "service_name": program.service.service_name.as_str(),
        "provider_name": program.service.provider_name.as_str(),
        "pmt_pid": program.pmt_pid.get(),
        "pcr_pid": program.pcr_pid.get(),
        "video_pid": program.video_pid.map(MpegTsPid::get),
        "audio": audio.into_iter().map(|(track_id, pid)| json!({
            "track_id": track_id.as_str(),
            "pid": pid.get(),
        })).collect::<Vec<_>>(),
        "scte35": program.scte35.as_ref().map(|(mapping, pid)| json!({
            "pid": pid.get(),
            "input": pass_policy_name(mapping.input),
            "generated_alert_cues": mapping.generated_alert_cues,
        })),
    })
}

fn pass_policy_name(policy: PassPolicy) -> &'static str {
    match policy {
        PassPolicy::Pass => "pass",
        PassPolicy::Drop => "drop",
    }
}

/// An SCTE-35 splice_info_section carrying one program-level splice_insert.
/// It is not a transport packet and is not injected into GStreamer here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EncodedScte35Section {
    bytes: Vec<u8>,
    pts_90khz: u64,
}

impl EncodedScte35Section {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub(crate) fn pts_90khz(&self) -> u64 {
        self.pts_90khz
    }
}

/// Encodes a non-encrypted program splice with an explicit 33-bit splice time,
/// no break duration, and no descriptors. Splice-out sets
/// `out_of_network_indicator`; splice-in clears it.
pub(crate) fn encode_generated_scte35_cue(cue: &GeneratedScte35Cue) -> EncodedScte35Section {
    let pts_90khz = nanoseconds_to_90khz(cue.pts_ns);
    let mut command = Vec::with_capacity(15);
    command.extend_from_slice(&cue.event_id.to_be_bytes());
    command.push(0x7f); // splice_event_cancel_indicator = 0, reserved = all ones
    let out_of_network = matches!(cue.command, CueCommand::SpliceOut);
    command.push(if out_of_network { 0xcf } else { 0x4f });
    command.push(0xfe | u8::try_from((pts_90khz >> 32) & 1).unwrap_or(0));
    let pts_low = u32::try_from(pts_90khz & u64::from(u32::MAX)).unwrap_or(0);
    command.extend_from_slice(&pts_low.to_be_bytes());
    let unique_program_id = u16::try_from(cue.event_id & u32::from(u16::MAX)).unwrap_or(0);
    command.extend_from_slice(&unique_program_id.to_be_bytes());
    command.push(0); // avail_num
    command.push(0); // avails_expected

    debug_assert_eq!(command.len(), usize::from(SPLICE_INSERT_COMMAND_LENGTH));
    let tier_and_length = (u32::from(SCTE35_TIER) << 12) | u32::from(SPLICE_INSERT_COMMAND_LENGTH);
    let mut section_body = Vec::with_capacity(28);
    section_body.push(0); // protocol_version
    section_body.extend_from_slice(&[0; 5]); // encryption fields and pts_adjustment
    section_body.push(0); // cw_index
    section_body.extend_from_slice(&[
        u8::try_from((tier_and_length >> 16) & 0xff).unwrap_or(0),
        u8::try_from((tier_and_length >> 8) & 0xff).unwrap_or(0),
        u8::try_from(tier_and_length & 0xff).unwrap_or(0),
    ]);
    section_body.push(SPLICE_INSERT_COMMAND_TYPE);
    section_body.extend_from_slice(&command);
    section_body.extend_from_slice(&[0, 0]); // descriptor_loop_length

    let section_length = section_body.len() + 4;
    debug_assert!(section_length <= 0x0fff);
    let mut bytes = Vec::with_capacity(section_length + 3);
    bytes.push(SCTE35_TABLE_ID);
    bytes.push(0x30 | u8::try_from((section_length >> 8) & 0x0f).unwrap_or(0));
    bytes.push(u8::try_from(section_length & 0xff).unwrap_or(0));
    bytes.extend_from_slice(&section_body);
    let crc = crc32_mpeg2(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    EncodedScte35Section { bytes, pts_90khz }
}

pub(crate) fn nanoseconds_to_90khz(pts_ns: u64) -> u64 {
    let ticks = u128::from(pts_ns) * 90_000 / 1_000_000_000;
    u64::try_from(ticks).unwrap_or(u64::MAX) & PTS_33_MASK
}

/// CRC-32/MPEG-2: polynomial 0x04C11DB7, initial value 0xFFFFFFFF,
/// non-reflected input/output, and no final XOR.
pub(crate) fn crc32_mpeg2(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for byte in bytes {
        crc ^= u32::from(*byte) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04c1_1db7
            } else {
                crc << 1
            };
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::architecture::{
        AudioStreamMap, PidAssignment, ProgramMapSpec, Scte35Map, ServiceMetadata,
    };

    fn resolved_program_map() -> ResolvedProgramMapSpec {
        ProgramMapSpec {
            transport_stream_id: 42,
            programs: vec![
                crate::architecture::MpegTsProgramSpec {
                    program_number: NonZeroU16::new(2).expect("program"),
                    service: ServiceMetadata {
                        service_name: "Haze Secondary".to_string(),
                        provider_name: "Haze".to_string(),
                    },
                    pmt_pid: PidAssignment::Manual(MpegTsPid::new(0x1001).expect("PID")),
                    video_pid: Some(PidAssignment::Manual(MpegTsPid::new(0x0120).expect("PID"))),
                    audio: vec![AudioStreamMap {
                        track_id: AudioTrackId::parse("secondary").expect("track"),
                        pid: PidAssignment::Manual(MpegTsPid::new(0x0121).expect("PID")),
                    }],
                    scte35: None,
                },
                crate::architecture::MpegTsProgramSpec {
                    program_number: NonZeroU16::new(1).expect("program"),
                    service: ServiceMetadata {
                        service_name: "Haze Primary".to_string(),
                        provider_name: "Haze Weather Radio".to_string(),
                    },
                    pmt_pid: PidAssignment::Manual(MpegTsPid::new(0x1000).expect("PID")),
                    video_pid: Some(PidAssignment::Manual(MpegTsPid::new(0x0100).expect("PID"))),
                    audio: vec![
                        AudioStreamMap {
                            track_id: AudioTrackId::parse("english").expect("track"),
                            pid: PidAssignment::Manual(MpegTsPid::new(0x0101).expect("PID")),
                        },
                        AudioStreamMap {
                            track_id: AudioTrackId::parse("french").expect("track"),
                            pid: PidAssignment::Manual(MpegTsPid::new(0x0102).expect("PID")),
                        },
                    ],
                    scte35: Some(Scte35Map {
                        input: PassPolicy::Pass,
                        generated_alert_cues: true,
                        pid: PidAssignment::Manual(MpegTsPid::new(0x0103).expect("PID")),
                    }),
                },
            ],
        }
        .resolve()
        .expect("resolved program map")
    }

    fn associations() -> PlannedTrackAssociations {
        PlannedTrackAssociations {
            video: vec![
                PlannedVideoTrackAssociation {
                    program_number: NonZeroU16::new(2).expect("program"),
                    sink_pid: MpegTsPid::new(0x0120).expect("PID"),
                },
                PlannedVideoTrackAssociation {
                    program_number: NonZeroU16::new(1).expect("program"),
                    sink_pid: MpegTsPid::new(0x0100).expect("PID"),
                },
            ],
            audio: vec![
                PlannedAudioTrackAssociation {
                    program_number: NonZeroU16::new(1).expect("program"),
                    track_id: AudioTrackId::parse("french").expect("track"),
                    sink_pid: MpegTsPid::new(0x0102).expect("PID"),
                },
                PlannedAudioTrackAssociation {
                    program_number: NonZeroU16::new(2).expect("program"),
                    track_id: AudioTrackId::parse("secondary").expect("track"),
                    sink_pid: MpegTsPid::new(0x0121).expect("PID"),
                },
                PlannedAudioTrackAssociation {
                    program_number: NonZeroU16::new(1).expect("program"),
                    track_id: AudioTrackId::parse("english").expect("track"),
                    sink_pid: MpegTsPid::new(0x0101).expect("PID"),
                },
            ],
        }
    }

    #[test]
    fn prog_map_is_deterministic_and_contains_sink_pmt_and_pcr_fields() {
        let map = build_gstreamer_program_map(&resolved_program_map(), &associations())
            .expect("program map");
        assert_eq!(
            map.structure(),
            "program_map,sink_256=(int)1,sink_257=(int)1,sink_258=(int)1,sink_288=(int)2,sink_289=(int)2,PMT_1=(uint)4096,PCR_1=(int)256,PMT_2=(uint)4097,PCR_2=(int)288"
        );
        let status = map.redacted_status_value();
        assert_eq!(status["redacted"], true);
        assert_eq!(status["transport_stream_id"], 42);
        assert_eq!(status["pat_pid"], 0);
        assert_eq!(status["programs"][0]["service_name"], "Haze Primary");
        assert_eq!(status["programs"][0]["provider_name"], "Haze Weather Radio");
        assert_eq!(status["programs"][0]["pmt_pid"], 0x1000);
        assert_eq!(status["programs"][0]["pcr_pid"], 0x0100);
        assert_eq!(status["programs"][0]["video_pid"], 0x0100);
        assert_eq!(status["programs"][0]["audio"][0]["pid"], 0x0101);
        assert_eq!(status["programs"][0]["scte35"]["pid"], 0x0103);
    }

    #[test]
    fn planned_tracks_must_exactly_match_the_resolved_map() {
        let spec = resolved_program_map();
        let mut planned = associations();
        planned
            .audio
            .retain(|audio| audio.track_id.as_str() != "french");
        assert_eq!(
            build_gstreamer_program_map(&spec, &planned),
            Err(ProgramMapError::MissingAudioAssociation {
                program: 1,
                track_id: "french".to_string(),
            })
        );

        let mut planned = associations();
        planned.video[0].sink_pid = MpegTsPid::new(0x0122).expect("PID");
        assert_eq!(
            build_gstreamer_program_map(&spec, &planned),
            Err(ProgramMapError::VideoPidMismatch {
                program: 2,
                expected: 0x0120,
                actual: 0x0122,
            })
        );
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ParsedSpliceInsert {
        event_id: u32,
        out_of_network: bool,
        pts_90khz: u64,
        command_length: u16,
    }

    fn parse_test_splice_insert(bytes: &[u8]) -> ParsedSpliceInsert {
        assert_eq!(bytes.len(), 35);
        assert_eq!(bytes[0], SCTE35_TABLE_ID);
        assert_eq!(bytes[1] & 0xf0, 0x30);
        let section_length = (usize::from(bytes[1] & 0x0f) << 8) | usize::from(bytes[2]);
        assert_eq!(section_length, bytes.len() - 3);
        assert_eq!(bytes[3], 0); // protocol_version
        assert_eq!(bytes[13], SPLICE_INSERT_COMMAND_TYPE);
        let command_length = (u16::from(bytes[11] & 0x0f) << 8) | u16::from(bytes[12]);
        let event_id = u32::from_be_bytes(bytes[14..18].try_into().expect("event ID"));
        assert_eq!(bytes[18] & 0x80, 0);
        assert_ne!(bytes[19] & 0x40, 0); // program_splice_flag
        assert_eq!(bytes[19] & 0x30, 0); // duration and immediate flags
        assert_ne!(bytes[20] & 0x80, 0); // time_specified_flag
        let pts_90khz = (u64::from(bytes[20] & 1) << 32)
            | u64::from(u32::from_be_bytes(bytes[21..25].try_into().expect("PTS")));
        assert_eq!(&bytes[29..31], &[0, 0]);
        let stored_crc = u32::from_be_bytes(bytes[31..35].try_into().expect("CRC"));
        assert_eq!(stored_crc, crc32_mpeg2(&bytes[..31]));
        assert_eq!(crc32_mpeg2(bytes), 0);
        ParsedSpliceInsert {
            event_id,
            out_of_network: bytes[19] & 0x80 != 0,
            pts_90khz,
            command_length,
        }
    }

    #[test]
    fn splice_insert_round_trips_out_and_in_fields_with_valid_crc() {
        let pts_ns = 12_345_678_901_u64;
        let expected_pts = nanoseconds_to_90khz(pts_ns);
        for (command, expected_out_of_network) in
            [(CueCommand::SpliceOut, true), (CueCommand::SpliceIn, false)]
        {
            let cue = GeneratedScte35Cue {
                queue_id: "queue-42".to_string(),
                event_id: 0x8abc_def0,
                command,
                pts_ns,
            };
            let encoded = encode_generated_scte35_cue(&cue);
            assert_eq!(encoded.pts_90khz(), expected_pts);
            if matches!(command, CueCommand::SpliceOut) {
                assert_eq!(
                    encoded.as_bytes(),
                    &[
                        0xfc, 0x30, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xf0,
                        0x0f, 0x05, 0x8a, 0xbc, 0xde, 0xf0, 0x7f, 0xcf, 0xfe, 0x00, 0x10, 0xf4,
                        0x47, 0xde, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x67, 0x97, 0xde, 0x36,
                    ]
                );
            }
            let parsed = parse_test_splice_insert(encoded.as_bytes());
            assert_eq!(parsed.event_id, cue.event_id);
            assert_eq!(parsed.out_of_network, expected_out_of_network);
            assert_eq!(parsed.pts_90khz, expected_pts);
            assert_eq!(parsed.command_length, 15);
        }
    }

    #[test]
    fn crc_matches_the_mpeg2_reference_vector() {
        assert_eq!(crc32_mpeg2(b"123456789"), 0x0376_e6e7);
    }
}
