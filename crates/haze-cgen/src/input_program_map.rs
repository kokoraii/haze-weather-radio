use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;

use serde_json::{json, Value};

use crate::architecture::{MpegTsPid, PidAssignment, ProgramMapSpec, ResolvedProgramMapSpec};
use crate::program_mapping::crc32_mpeg2;

const MPEG_TS_PACKET_BYTES: usize = 188;
const MAX_PENDING_TRANSPORT_BYTES: usize = MPEG_TS_PACKET_BYTES * 64;
const MAX_PSI_SECTION_BYTES: usize = 4096;
const PAT_PID: u16 = 0x0000;
const NULL_PID: u16 = 0x1fff;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgramMappingMode {
    Source,
    Auto,
    Manual,
}

impl ProgramMappingMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Auto => "auto",
            Self::Manual => "manual",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputStreamKind {
    Video,
    Audio,
    Scte35,
    Data,
}

impl InputStreamKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Video => "video",
            Self::Audio => "audio",
            Self::Scte35 => "scte35",
            Self::Data => "data",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputElementaryStream {
    pub(crate) pid: u16,
    pub(crate) stream_type: u8,
    pub(crate) kind: InputStreamKind,
    pub(crate) codec: &'static str,
    pub(crate) language: Option<String>,
}

impl InputElementaryStream {
    fn status_value(&self) -> Value {
        json!({
            "pid": self.pid,
            "pid_hex": format!("0x{:04X}", self.pid),
            "stream_type": self.stream_type,
            "stream_type_hex": format!("0x{:02X}", self.stream_type),
            "kind": self.kind.as_str(),
            "codec": self.codec,
            "language": self.language,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputProgram {
    pub(crate) program_number: NonZeroU16,
    pub(crate) pmt_pid: u16,
    pub(crate) pcr_pid: u16,
    pub(crate) streams: Vec<InputElementaryStream>,
}

impl InputProgram {
    fn first_pid(&self, kind: InputStreamKind) -> Option<u16> {
        self.streams
            .iter()
            .find(|stream| stream.kind == kind)
            .map(|stream| stream.pid)
    }

    fn status_value(&self) -> Value {
        json!({
            "program_number": self.program_number.get(),
            "pmt_pid": self.pmt_pid,
            "pmt_pid_hex": format!("0x{:04X}", self.pmt_pid),
            "pcr_pid": self.pcr_pid,
            "pcr_pid_hex": format!("0x{:04X}", self.pcr_pid),
            "streams": self.streams.iter()
                .map(InputElementaryStream::status_value)
                .collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputProgramMapSnapshot {
    pub(crate) transport_stream_id: u16,
    pub(crate) pat_program_count: usize,
    pub(crate) selected_program: InputProgram,
}

impl InputProgramMapSnapshot {
    pub(crate) fn status_value(&self) -> Value {
        json!({
            "transport_stream_id": self.transport_stream_id,
            "pat_program_count": self.pat_program_count,
            "selection": "first_program_with_video",
            "selected_program": self.selected_program.status_value(),
        })
    }

    pub(crate) fn derive_program_map(&self, template: &ProgramMapSpec) -> ProgramMapSpec {
        let mut derived = template.clone();
        if self.transport_stream_id != 0 {
            derived.transport_stream_id = self.transport_stream_id;
        }
        let Some(primary) = derived.programs.first_mut() else {
            return derived;
        };

        primary.program_number = self.selected_program.program_number;
        primary.pmt_pid = assign_input_pid(self.selected_program.pmt_pid);
        primary.video_pid = primary.video_pid.map(|_| {
            self.selected_program
                .first_pid(InputStreamKind::Video)
                .map(assign_input_pid)
                .unwrap_or(PidAssignment::Auto)
        });
        for (index, audio) in primary.audio.iter_mut().enumerate() {
            audio.pid = if index == 0 {
                self.selected_program
                    .first_pid(InputStreamKind::Audio)
                    .map(assign_input_pid)
                    .unwrap_or(PidAssignment::Auto)
            } else {
                PidAssignment::Auto
            };
        }
        if let Some(scte35) = primary.scte35.as_mut() {
            scte35.pid = self
                .selected_program
                .first_pid(InputStreamKind::Scte35)
                .map(assign_input_pid)
                .unwrap_or(PidAssignment::Auto);
        }

        make_program_numbers_unique(&mut derived);
        derived
    }

    pub(crate) fn resolve_program_map(
        &self,
        template: &ProgramMapSpec,
    ) -> Result<ResolvedProgramMapSpec, crate::architecture::PipelineSpecError> {
        self.derive_program_map(template).resolve()
    }
}

fn assign_input_pid(pid: u16) -> PidAssignment {
    MpegTsPid::new(pid)
        .map(PidAssignment::Manual)
        .unwrap_or(PidAssignment::Auto)
}

fn make_program_numbers_unique(program_map: &mut ProgramMapSpec) {
    let mut used = BTreeSet::new();
    for program in &mut program_map.programs {
        let current = program.program_number.get();
        if used.insert(current) {
            continue;
        }
        let Some(next) = (1..=u16::MAX).find(|candidate| !used.contains(candidate)) else {
            return;
        };
        if let Some(number) = NonZeroU16::new(next) {
            program.program_number = number;
            used.insert(next);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PatSnapshot {
    transport_stream_id: u16,
    programs: Vec<(NonZeroU16, u16)>,
}

#[derive(Debug, Default)]
struct PsiSectionAssembler {
    bytes: Vec<u8>,
    expected: Option<usize>,
}

impl PsiSectionAssembler {
    fn reset(&mut self) {
        self.bytes.clear();
        self.expected = None;
    }

    fn push(&mut self, payload: &[u8], payload_unit_start: bool) -> Vec<Vec<u8>> {
        if payload_unit_start {
            let Some((&pointer, remainder)) = payload.split_first() else {
                self.reset();
                return Vec::new();
            };
            let pointer = usize::from(pointer);
            if pointer > remainder.len() {
                self.reset();
                return Vec::new();
            }
            let mut sections = Vec::new();
            if pointer > 0 && !self.bytes.is_empty() {
                sections.extend(self.consume(&remainder[..pointer], false));
            }
            self.reset();
            sections.extend(self.consume(&remainder[pointer..], true));
            sections
        } else if self.bytes.is_empty() {
            Vec::new()
        } else {
            self.consume(payload, false)
        }
    }

    fn consume(&mut self, mut bytes: &[u8], allow_new_sections: bool) -> Vec<Vec<u8>> {
        let mut sections = Vec::new();
        while !bytes.is_empty() {
            if self.bytes.is_empty() && bytes[0] == 0xff {
                break;
            }

            let needed = self
                .expected
                .map(|expected| expected.saturating_sub(self.bytes.len()))
                .unwrap_or_else(|| 3_usize.saturating_sub(self.bytes.len()));
            let take = needed.min(bytes.len());
            self.bytes.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];

            if self.expected.is_none() && self.bytes.len() >= 3 {
                let section_length =
                    (usize::from(self.bytes[1] & 0x0f) << 8) | usize::from(self.bytes[2]);
                let total = section_length.saturating_add(3);
                if !(7..=MAX_PSI_SECTION_BYTES).contains(&total) {
                    self.reset();
                    break;
                }
                self.expected = Some(total);
            }

            if self
                .expected
                .is_some_and(|expected| self.bytes.len() == expected)
            {
                sections.push(std::mem::take(&mut self.bytes));
                self.expected = None;
                if !allow_new_sections {
                    break;
                }
            }
        }
        sections
    }
}

#[derive(Debug, Default)]
pub(crate) struct MpegTsPsiParser {
    pending: Vec<u8>,
    assemblers: BTreeMap<u16, PsiSectionAssembler>,
    pat: Option<PatSnapshot>,
    programs: BTreeMap<u16, InputProgram>,
    last_snapshot: Option<InputProgramMapSnapshot>,
}

impl MpegTsPsiParser {
    pub(crate) fn push(&mut self, bytes: &[u8]) -> Option<InputProgramMapSnapshot> {
        if bytes.is_empty() {
            return None;
        }
        if self.pending.len().saturating_add(bytes.len()) > MAX_PENDING_TRANSPORT_BYTES {
            self.pending.clear();
        }
        self.pending.extend_from_slice(bytes);

        while self.pending.len() >= MPEG_TS_PACKET_BYTES {
            if self.pending[0] != 0x47
                || (self.pending.len() >= MPEG_TS_PACKET_BYTES * 2
                    && self.pending[MPEG_TS_PACKET_BYTES] != 0x47)
            {
                let Some(offset) = find_transport_sync(&self.pending) else {
                    let keep = self.pending.len().min(MPEG_TS_PACKET_BYTES - 1);
                    self.pending.drain(..self.pending.len() - keep);
                    break;
                };
                self.pending.drain(..offset);
                if self.pending.len() < MPEG_TS_PACKET_BYTES {
                    break;
                }
            }
            let packet = self.pending[..MPEG_TS_PACKET_BYTES].to_vec();
            self.pending.drain(..MPEG_TS_PACKET_BYTES);
            self.push_packet(&packet);
        }

        let snapshot = self.snapshot()?;
        if self.last_snapshot.as_ref() == Some(&snapshot) {
            return None;
        }
        self.last_snapshot = Some(snapshot.clone());
        Some(snapshot)
    }

    fn push_packet(&mut self, packet: &[u8]) {
        if packet.len() != MPEG_TS_PACKET_BYTES || packet[0] != 0x47 {
            return;
        }
        if packet[1] & 0x80 != 0 || packet[3] & 0xc0 != 0 {
            return;
        }
        let pid = (u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2]);
        let adaptation_control = (packet[3] >> 4) & 0x03;
        if adaptation_control == 0 || adaptation_control == 2 {
            return;
        }
        if pid != PAT_PID && !self.is_known_pmt_pid(pid) {
            return;
        }
        let payload_unit_start = packet[1] & 0x40 != 0;
        let mut offset = 4_usize;
        if adaptation_control == 3 {
            let Some(length) = packet.get(offset).copied() else {
                return;
            };
            offset = offset.saturating_add(usize::from(length)).saturating_add(1);
        }
        if offset >= packet.len() {
            return;
        }

        let sections = self
            .assemblers
            .entry(pid)
            .or_default()
            .push(&packet[offset..], payload_unit_start);
        for section in sections {
            if pid == PAT_PID {
                self.observe_pat(&section);
            } else {
                self.observe_pmt(pid, &section);
            }
        }
    }

    fn is_known_pmt_pid(&self, pid: u16) -> bool {
        self.pat
            .as_ref()
            .is_some_and(|pat| pat.programs.iter().any(|(_, pmt_pid)| *pmt_pid == pid))
    }

    fn observe_pat(&mut self, section: &[u8]) {
        let Some(pat) = parse_pat(section) else {
            return;
        };
        if self.pat.as_ref() != Some(&pat) {
            self.programs.clear();
            self.assemblers.retain(|pid, _| *pid == PAT_PID);
            self.pat = Some(pat);
        }
    }

    fn observe_pmt(&mut self, pmt_pid: u16, section: &[u8]) {
        let Some(program) = parse_pmt(pmt_pid, section) else {
            return;
        };
        let belongs_to_pat = self.pat.as_ref().is_some_and(|pat| {
            pat.programs
                .iter()
                .any(|(number, pid)| *number == program.program_number && *pid == program.pmt_pid)
        });
        if belongs_to_pat {
            self.programs.insert(program.program_number.get(), program);
        }
    }

    fn snapshot(&self) -> Option<InputProgramMapSnapshot> {
        let pat = self.pat.as_ref()?;
        let selected = pat
            .programs
            .iter()
            .filter_map(|(number, _)| self.programs.get(&number.get()))
            .find(|program| {
                program
                    .streams
                    .iter()
                    .any(|stream| stream.kind == InputStreamKind::Video)
            })
            .or_else(|| {
                pat.programs
                    .iter()
                    .find_map(|(number, _)| self.programs.get(&number.get()))
            })?
            .clone();
        Some(InputProgramMapSnapshot {
            transport_stream_id: pat.transport_stream_id,
            pat_program_count: pat.programs.len(),
            selected_program: selected,
        })
    }
}

fn find_transport_sync(bytes: &[u8]) -> Option<usize> {
    let limit = bytes.len().min(MPEG_TS_PACKET_BYTES);
    (0..limit).find(|offset| {
        bytes[*offset] == 0x47
            && (bytes.len() <= offset.saturating_add(MPEG_TS_PACKET_BYTES)
                || bytes[offset + MPEG_TS_PACKET_BYTES] == 0x47)
    })
}

fn parse_pat(section: &[u8]) -> Option<PatSnapshot> {
    if !valid_psi_section(section, 0x00) || section.len() < 12 {
        return None;
    }
    if section[5] & 0x01 == 0 {
        return None;
    }
    let transport_stream_id = u16::from_be_bytes([section[3], section[4]]);
    let mut programs = Vec::new();
    let mut seen = BTreeSet::new();
    let end = section.len().checked_sub(4)?;
    let mut offset = 8_usize;
    while offset.saturating_add(4) <= end {
        let number = u16::from_be_bytes([section[offset], section[offset + 1]]);
        let pid = (u16::from(section[offset + 2] & 0x1f) << 8) | u16::from(section[offset + 3]);
        offset += 4;
        let Some(number) = NonZeroU16::new(number) else {
            continue;
        };
        if pid <= 0x001f || pid == NULL_PID || !seen.insert(number.get()) {
            continue;
        }
        programs.push((number, pid));
    }
    (!programs.is_empty()).then_some(PatSnapshot {
        transport_stream_id,
        programs,
    })
}

fn parse_pmt(pmt_pid: u16, section: &[u8]) -> Option<InputProgram> {
    if !valid_psi_section(section, 0x02) || section.len() < 16 || section[5] & 0x01 == 0 {
        return None;
    }
    let program_number = NonZeroU16::new(u16::from_be_bytes([section[3], section[4]]))?;
    let pcr_pid = (u16::from(section[8] & 0x1f) << 8) | u16::from(section[9]);
    let program_info_length = (usize::from(section[10] & 0x0f) << 8) | usize::from(section[11]);
    let end = section.len().checked_sub(4)?;
    let mut offset = 12_usize.checked_add(program_info_length)?;
    if offset > end {
        return None;
    }

    let mut streams = Vec::new();
    let mut seen = BTreeSet::new();
    while offset.saturating_add(5) <= end {
        let stream_type = section[offset];
        let pid = (u16::from(section[offset + 1] & 0x1f) << 8) | u16::from(section[offset + 2]);
        let info_length =
            (usize::from(section[offset + 3] & 0x0f) << 8) | usize::from(section[offset + 4]);
        let info_start = offset + 5;
        let info_end = info_start.checked_add(info_length)?;
        if info_end > end {
            return None;
        }
        if pid > 0x001f && pid != NULL_PID && seen.insert(pid) {
            let descriptors = &section[info_start..info_end];
            let (kind, codec) = classify_stream(stream_type, descriptors);
            streams.push(InputElementaryStream {
                pid,
                stream_type,
                kind,
                codec,
                language: descriptor_language(descriptors),
            });
        }
        offset = info_end;
    }
    (!streams.is_empty()).then_some(InputProgram {
        program_number,
        pmt_pid,
        pcr_pid,
        streams,
    })
}

fn valid_psi_section(section: &[u8], table_id: u8) -> bool {
    if section.len() < 7 || section[0] != table_id || section[1] & 0x80 == 0 {
        return false;
    }
    let declared = ((usize::from(section[1] & 0x0f) << 8) | usize::from(section[2])) + 3;
    declared == section.len() && crc32_mpeg2(section) == 0
}

fn classify_stream(stream_type: u8, descriptors: &[u8]) -> (InputStreamKind, &'static str) {
    match stream_type {
        0x01 => (InputStreamKind::Video, "mpeg1video"),
        0x02 => (InputStreamKind::Video, "mpeg2video"),
        0x10 => (InputStreamKind::Video, "mpeg4video"),
        0x1b => (InputStreamKind::Video, "h264"),
        0x24 => (InputStreamKind::Video, "h265"),
        0x42 => (InputStreamKind::Video, "avs"),
        0x03 => (InputStreamKind::Audio, "mpeg1audio"),
        0x04 => (InputStreamKind::Audio, "mp2"),
        0x0f => (InputStreamKind::Audio, "aac"),
        0x11 => (InputStreamKind::Audio, "aac_latm"),
        0x81 => (InputStreamKind::Audio, "ac3"),
        0x87 => (InputStreamKind::Audio, "eac3"),
        0x86 => (InputStreamKind::Scte35, "scte35"),
        0x06 if has_registration(descriptors, b"CUEI") => (InputStreamKind::Scte35, "scte35"),
        0x06 if has_descriptor(descriptors, 0x6a) || has_registration(descriptors, b"AC-3") => {
            (InputStreamKind::Audio, "ac3")
        }
        0x06 if has_descriptor(descriptors, 0x7a) || has_registration(descriptors, b"EAC3") => {
            (InputStreamKind::Audio, "eac3")
        }
        _ => (InputStreamKind::Data, "private_or_data"),
    }
}

fn descriptor_language(descriptors: &[u8]) -> Option<String> {
    for (tag, value) in descriptors_iter(descriptors) {
        if tag == 0x0a && value.len() >= 3 && value[..3].iter().all(u8::is_ascii_alphabetic) {
            return std::str::from_utf8(&value[..3])
                .ok()
                .map(|language| language.to_ascii_lowercase());
        }
    }
    None
}

fn has_descriptor(descriptors: &[u8], wanted: u8) -> bool {
    descriptors_iter(descriptors).any(|(tag, _)| tag == wanted)
}

fn has_registration(descriptors: &[u8], wanted: &[u8; 4]) -> bool {
    descriptors_iter(descriptors)
        .any(|(tag, value)| tag == 0x05 && value.len() >= 4 && &value[..4] == wanted)
}

fn descriptors_iter(mut descriptors: &[u8]) -> impl Iterator<Item = (u8, &[u8])> {
    std::iter::from_fn(move || {
        let (&tag, remainder) = descriptors.split_first()?;
        let (&length, remainder) = remainder.split_first()?;
        let length = usize::from(length);
        if remainder.len() < length {
            descriptors = &[];
            return None;
        }
        let (value, rest) = remainder.split_at(length);
        descriptors = rest;
        Some((tag, value))
    })
}

#[cfg(test)]
mod tests {
    use crate::architecture::{
        AudioStreamMap, AudioTrackId, MpegTsProgramSpec, PassPolicy, Scte35Map, ServiceMetadata,
    };

    use super::*;

    #[test]
    fn parser_discovers_video_audio_languages_and_scte35() {
        let bytes = sample_transport_stream();
        let mut parser = MpegTsPsiParser::default();
        let snapshot = parser.push(&bytes).expect("program map");

        assert_eq!(snapshot.transport_stream_id, 7);
        assert_eq!(snapshot.selected_program.program_number.get(), 1);
        assert_eq!(snapshot.selected_program.pmt_pid, 0x1000);
        assert_eq!(snapshot.selected_program.pcr_pid, 0x0200);
        assert_eq!(
            snapshot.selected_program.first_pid(InputStreamKind::Video),
            Some(0x0200)
        );
        assert_eq!(
            snapshot.selected_program.first_pid(InputStreamKind::Audio),
            Some(0x0201)
        );
        assert_eq!(
            snapshot.selected_program.first_pid(InputStreamKind::Scte35),
            Some(0x10c0)
        );
        assert_eq!(
            snapshot.selected_program.streams[1].language.as_deref(),
            Some("eng")
        );
        assert_eq!(
            snapshot.selected_program.streams[2].language.as_deref(),
            Some("spa")
        );
    }

    #[test]
    fn parser_handles_split_buffers_and_resynchronizes() {
        let mut bytes = vec![0x00, 0x11, 0x22];
        bytes.extend(sample_transport_stream());
        let mut parser = MpegTsPsiParser::default();
        assert!(parser.push(&bytes[..91]).is_none());
        let snapshot = parser.push(&bytes[91..]).expect("program map");
        assert_eq!(snapshot.selected_program.pmt_pid, 0x1000);
    }

    #[test]
    fn invalid_crc_is_rejected() {
        let mut bytes = sample_transport_stream();
        bytes[MPEG_TS_PACKET_BYTES + 20] ^= 0x80;
        let mut parser = MpegTsPsiParser::default();
        assert!(parser.push(&bytes).is_none());
    }

    #[test]
    fn source_map_overrides_primary_ids_and_auto_allocates_extra_audio() {
        let mut parser = MpegTsPsiParser::default();
        let snapshot = parser
            .push(&sample_transport_stream())
            .expect("program map");
        let template = ProgramMapSpec {
            transport_stream_id: 1,
            programs: vec![MpegTsProgramSpec {
                program_number: NonZeroU16::new(3).expect("non-zero"),
                service: ServiceMetadata {
                    service_name: "CGEN".to_string(),
                    provider_name: "Haze".to_string(),
                },
                pmt_pid: PidAssignment::Auto,
                video_pid: Some(PidAssignment::Auto),
                audio: vec![
                    AudioStreamMap {
                        track_id: AudioTrackId::parse("stereo").expect("track"),
                        pid: PidAssignment::Auto,
                    },
                    AudioStreamMap {
                        track_id: AudioTrackId::parse("surround_51").expect("track"),
                        pid: PidAssignment::Auto,
                    },
                ],
                scte35: Some(Scte35Map {
                    input: PassPolicy::Pass,
                    generated_alert_cues: true,
                    pid: PidAssignment::Auto,
                }),
            }],
        };

        let resolved = snapshot
            .resolve_program_map(&template)
            .expect("derived map resolves");
        let program = &resolved.programs[0];
        assert_eq!(resolved.transport_stream_id, 7);
        assert_eq!(program.program_number.get(), 1);
        assert_eq!(program.pmt_pid.get(), 0x1000);
        assert_eq!(program.video_pid.map(MpegTsPid::get), Some(0x0200));
        assert_eq!(program.audio[0].1.get(), 0x0201);
        assert_eq!(program.audio[1].1.get(), 0x0100);
        assert_eq!(
            program.scte35.as_ref().map(|(_, pid)| pid.get()),
            Some(0x10c0)
        );
    }

    fn sample_transport_stream() -> Vec<u8> {
        let pat = pat_section(7, 1, 0x1000);
        let pmt = pmt_section(
            1,
            0x0200,
            &[
                (0x02, 0x0200, Vec::new()),
                (0x81, 0x0201, language_descriptor(b"eng")),
                (0x81, 0x0022, language_descriptor(b"spa")),
                (0x86, 0x10c0, Vec::new()),
            ],
        );
        [psi_packet(PAT_PID, &pat), psi_packet(0x1000, &pmt)].concat()
    }

    fn pat_section(transport_stream_id: u16, program: u16, pmt_pid: u16) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&transport_stream_id.to_be_bytes());
        body.extend_from_slice(&[0xc1, 0x00, 0x00]);
        body.extend_from_slice(&program.to_be_bytes());
        body.push(0xe0 | u8::try_from((pmt_pid >> 8) & 0x1f).expect("PID high"));
        body.push(u8::try_from(pmt_pid & 0xff).expect("PID low"));
        psi_section(0x00, body)
    }

    fn pmt_section(program: u16, pcr_pid: u16, streams: &[(u8, u16, Vec<u8>)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&program.to_be_bytes());
        body.extend_from_slice(&[0xc1, 0x00, 0x00]);
        body.push(0xe0 | u8::try_from((pcr_pid >> 8) & 0x1f).expect("PCR high"));
        body.push(u8::try_from(pcr_pid & 0xff).expect("PCR low"));
        body.extend_from_slice(&[0xf0, 0x00]);
        for (stream_type, pid, descriptors) in streams {
            body.push(*stream_type);
            body.push(0xe0 | u8::try_from((pid >> 8) & 0x1f).expect("PID high"));
            body.push(u8::try_from(pid & 0xff).expect("PID low"));
            body.push(
                0xf0 | u8::try_from((descriptors.len() >> 8) & 0x0f).expect("descriptor high"),
            );
            body.push(u8::try_from(descriptors.len() & 0xff).expect("descriptor low"));
            body.extend_from_slice(descriptors);
        }
        psi_section(0x02, body)
    }

    fn psi_section(table_id: u8, body: Vec<u8>) -> Vec<u8> {
        let section_length = body.len() + 4;
        let mut section = vec![
            table_id,
            0xb0 | u8::try_from((section_length >> 8) & 0x0f).expect("length high"),
            u8::try_from(section_length & 0xff).expect("length low"),
        ];
        section.extend(body);
        let crc = crc32_mpeg2(&section);
        section.extend_from_slice(&crc.to_be_bytes());
        section
    }

    fn psi_packet(pid: u16, section: &[u8]) -> Vec<u8> {
        assert!(section.len() <= 183);
        let mut packet = vec![0xff; MPEG_TS_PACKET_BYTES];
        packet[0] = 0x47;
        packet[1] = 0x40 | u8::try_from((pid >> 8) & 0x1f).expect("PID high");
        packet[2] = u8::try_from(pid & 0xff).expect("PID low");
        packet[3] = 0x10;
        packet[4] = 0;
        packet[5..5 + section.len()].copy_from_slice(section);
        packet
    }

    fn language_descriptor(language: &[u8; 3]) -> Vec<u8> {
        vec![0x0a, 0x04, language[0], language[1], language[2], 0x00]
    }
}
