use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::architecture::{
    AncillaryPolicy, EncoderOutputSpec, OutputDestination, PassPolicy, VideoCodec,
};

const MAX_ANCILLARY_PAYLOAD_BYTES: usize = 64 * 1024;
const GENERATED_CUE_NAMESPACE: u32 = 0x8000_0000;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum AncillaryError {
    #[error("ancillary payload exceeds the 65536 byte safety limit")]
    PayloadTooLarge,
    #[error("alert queue ID must not be empty")]
    EmptyQueueId,
    #[error("splice-in has no matching active generated cue")]
    MissingActiveCue,
    #[error("generated SCTE-35 cue namespace is exhausted")]
    CueNamespaceExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CaptionFormat {
    Eia608,
    Eia708,
}

impl CaptionFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Eia608 => "eia608",
            Self::Eia708 => "eia708",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AncillaryKind {
    Caption(CaptionFormat),
    Scte35,
    Scte104,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AncillaryPacket {
    pub(crate) pts_ns: u64,
    pub(crate) kind: AncillaryKind,
    pub(crate) payload: Vec<u8>,
}

impl AncillaryPacket {
    pub(crate) fn new(
        pts_ns: u64,
        kind: AncillaryKind,
        payload: Vec<u8>,
    ) -> Result<Self, AncillaryError> {
        if payload.len() > MAX_ANCILLARY_PAYLOAD_BYTES {
            return Err(AncillaryError::PayloadTooLarge);
        }
        Ok(Self {
            pts_ns,
            kind,
            payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectivePassPolicy {
    pub(crate) requested: PassPolicy,
    pub(crate) effective: PassPolicy,
    pub(crate) warning: Option<String>,
}

impl EffectivePassPolicy {
    fn resolve(requested: PassPolicy, supported: bool, unsupported_message: &str) -> Self {
        if requested == PassPolicy::Pass && !supported {
            Self {
                requested,
                effective: PassPolicy::Drop,
                warning: Some(unsupported_message.to_string()),
            }
        } else {
            Self {
                requested,
                effective: requested,
                warning: None,
            }
        }
    }

    fn status_value(&self) -> Value {
        json!({
            "requested": policy_name(self.requested),
            "effective": policy_name(self.effective),
            "warning": self.warning,
        })
    }

    fn is_degraded(&self) -> bool {
        self.requested != self.effective
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputAncillaryPolicy {
    pub(crate) captions: EffectivePassPolicy,
    pub(crate) scte35: EffectivePassPolicy,
    pub(crate) scte104: EffectivePassPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AncillaryBackendCapabilities {
    pub(crate) caption_reinsertion: bool,
    pub(crate) scte35_passthrough: bool,
    pub(crate) scte104_passthrough: bool,
}

impl AncillaryBackendCapabilities {
    pub(crate) const fn current_gstreamer() -> Self {
        Self {
            caption_reinsertion: false,
            scte35_passthrough: false,
            scte104_passthrough: false,
        }
    }

    pub(crate) fn status_value(self) -> Value {
        json!({
            "caption_reinsertion": self.caption_reinsertion,
            "scte35_passthrough": self.scte35_passthrough,
            "scte104_passthrough": self.scte104_passthrough,
        })
    }
}

impl OutputAncillaryPolicy {
    pub(crate) fn resolve(
        requested: AncillaryPolicy,
        output: &EncoderOutputSpec,
        caption_format: CaptionFormat,
        backend: AncillaryBackendCapabilities,
    ) -> Self {
        let container_captions_supported = output_supports_captions(output, caption_format);
        let container_scte_supported = output.destination.is_mpeg_ts();
        let captions_supported = container_captions_supported && backend.caption_reinsertion;
        let scte35_supported = container_scte_supported && backend.scte35_passthrough;
        let scte104_supported = container_scte_supported && backend.scte104_passthrough;
        let caption_warning = if container_captions_supported {
            "the active media backend does not yet reinsert timestamped captions"
        } else {
            "selected output or video encoder cannot carry the requested caption format"
        };
        let scte35_warning = if container_scte_supported {
            "the active media backend does not yet inject timestamped SCTE-35 packets"
        } else {
            "SCTE-35 is supported only for MPEG-TS outputs"
        };
        let scte104_warning = if container_scte_supported {
            "the active media backend does not support SCTE-104 opaque passthrough"
        } else {
            "SCTE-104 opaque passthrough requires a compatible MPEG-TS output"
        };
        Self {
            captions: EffectivePassPolicy::resolve(
                requested.captions,
                captions_supported,
                caption_warning,
            ),
            scte35: EffectivePassPolicy::resolve(
                requested.scte35,
                scte35_supported,
                scte35_warning,
            ),
            scte104: EffectivePassPolicy::resolve(
                requested.scte104,
                scte104_supported,
                scte104_warning,
            ),
        }
    }

    pub(crate) fn status_value(&self) -> Value {
        json!({
            "captions": self.captions.status_value(),
            "scte35": self.scte35.status_value(),
            "scte104": self.scte104.status_value(),
        })
    }

    pub(crate) fn is_degraded(&self) -> bool {
        self.captions.is_degraded() || self.scte35.is_degraded() || self.scte104.is_degraded()
    }
}

fn output_supports_captions(output: &EncoderOutputSpec, _format: CaptionFormat) -> bool {
    let codec_can_embed = matches!(
        output.video.codec,
        VideoCodec::H264 | VideoCodec::H265 | VideoCodec::Mpeg2
    );
    if !codec_can_embed {
        return false;
    }
    match &output.destination {
        OutputDestination::MpegTsUdp { .. }
        | OutputDestination::MpegTsSrt { .. }
        | OutputDestination::Rtp { .. } => true,
        OutputDestination::File { container, .. } => matches!(
            container.trim().to_ascii_lowercase().as_str(),
            "mpegts" | "mpeg-ts" | "ts"
        ),
        OutputDestination::Rtmp { .. } => false,
    }
}

fn policy_name(policy: PassPolicy) -> &'static str {
    match policy {
        PassPolicy::Pass => "pass",
        PassPolicy::Drop => "drop",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CueCommand {
    SpliceOut,
    SpliceIn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GeneratedScte35Cue {
    pub(crate) queue_id: String,
    pub(crate) event_id: u32,
    pub(crate) command: CueCommand,
    pub(crate) pts_ns: u64,
}

#[derive(Debug, Default)]
pub(crate) struct AlertCueTracker {
    active: BTreeMap<String, u32>,
    allocated: BTreeSet<u32>,
}

impl AlertCueTracker {
    pub(crate) fn splice_out(
        &mut self,
        queue_id: &str,
        pts_ns: u64,
    ) -> Result<GeneratedScte35Cue, AncillaryError> {
        let queue_id = queue_id.trim();
        if queue_id.is_empty() {
            return Err(AncillaryError::EmptyQueueId);
        }
        if let Some(event_id) = self.active.get(queue_id).copied() {
            return Ok(GeneratedScte35Cue {
                queue_id: queue_id.to_string(),
                event_id,
                command: CueCommand::SpliceOut,
                pts_ns,
            });
        }
        let mut event_id = generated_event_id(queue_id);
        let start = event_id;
        while self.allocated.contains(&event_id) {
            event_id = GENERATED_CUE_NAMESPACE | event_id.wrapping_add(1) & 0x7fff_ffff;
            if event_id == start {
                return Err(AncillaryError::CueNamespaceExhausted);
            }
        }
        self.active.insert(queue_id.to_string(), event_id);
        self.allocated.insert(event_id);
        Ok(GeneratedScte35Cue {
            queue_id: queue_id.to_string(),
            event_id,
            command: CueCommand::SpliceOut,
            pts_ns,
        })
    }

    pub(crate) fn splice_in(
        &mut self,
        queue_id: &str,
        pts_ns: u64,
    ) -> Result<GeneratedScte35Cue, AncillaryError> {
        let queue_id = queue_id.trim();
        if queue_id.is_empty() {
            return Err(AncillaryError::EmptyQueueId);
        }
        let event_id = self
            .active
            .remove(queue_id)
            .ok_or(AncillaryError::MissingActiveCue)?;
        self.allocated.remove(&event_id);
        Ok(GeneratedScte35Cue {
            queue_id: queue_id.to_string(),
            event_id,
            command: CueCommand::SpliceIn,
            pts_ns,
        })
    }
}

fn generated_event_id(queue_id: &str) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for byte in queue_id.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    GENERATED_CUE_NAMESPACE | hash & 0x7fff_ffff
}

#[derive(Debug, Clone)]
pub(crate) struct AncillarySender {
    tx: mpsc::Sender<AncillaryPacket>,
    dropped: Arc<AtomicU64>,
}

impl AncillarySender {
    pub(crate) fn try_send(&self, packet: AncillaryPacket) -> bool {
        match self.tx.try_send(packet) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) | Err(mpsc::error::TrySendError::Closed(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

pub(crate) fn ancillary_channel(
    capacity: usize,
) -> (AncillarySender, mpsc::Receiver<AncillaryPacket>) {
    let (tx, rx) = mpsc::channel(capacity.clamp(1, 1_024));
    (
        AncillarySender {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
        },
        rx,
    )
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::architecture::{
        AudioCodec, AudioCodecPolicy, AudioEncoderSpec, OutputId, RateControl, VideoEncoderSpec,
    };

    fn output(destination: OutputDestination) -> EncoderOutputSpec {
        EncoderOutputSpec {
            id: OutputId::parse("output").expect("valid ID"),
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
            audio: AudioEncoderSpec {
                codec: AudioCodecPolicy::Encode(AudioCodec::Aac),
                bitrate_kbps: NonZeroU32::new(192).expect("non-zero"),
                sample_rate: NonZeroU32::new(48_000).expect("non-zero"),
            },
        }
    }

    #[test]
    fn scte104_requested_pass_reports_effective_drop() {
        let backend = AncillaryBackendCapabilities::current_gstreamer();
        let policy = OutputAncillaryPolicy::resolve(
            AncillaryPolicy {
                captions: PassPolicy::Pass,
                scte35: PassPolicy::Pass,
                scte104: PassPolicy::Pass,
            },
            &output(OutputDestination::MpegTsUdp {
                location: "udp://239.0.0.1:9000".to_string(),
            }),
            CaptionFormat::Eia708,
            backend,
        );
        assert_eq!(policy.captions.effective, PassPolicy::Drop);
        assert_eq!(policy.scte35.effective, PassPolicy::Drop);
        assert_eq!(policy.scte104.requested, PassPolicy::Pass);
        assert_eq!(policy.scte104.effective, PassPolicy::Drop);
        assert!(policy.is_degraded());
        assert!(policy.captions.warning.is_some());
        assert!(policy.scte35.warning.is_some());
        assert!(policy.scte104.warning.is_some());
    }

    #[test]
    fn capable_backend_preserves_supported_mpeg_ts_requests() {
        let policy = OutputAncillaryPolicy::resolve(
            AncillaryPolicy {
                captions: PassPolicy::Pass,
                scte35: PassPolicy::Pass,
                scte104: PassPolicy::Pass,
            },
            &output(OutputDestination::MpegTsUdp {
                location: "udp://239.0.0.1:9000".to_string(),
            }),
            CaptionFormat::Eia708,
            AncillaryBackendCapabilities {
                caption_reinsertion: true,
                scte35_passthrough: true,
                scte104_passthrough: true,
            },
        );
        assert_eq!(policy.captions.effective, PassPolicy::Pass);
        assert_eq!(policy.scte35.effective, PassPolicy::Pass);
        assert_eq!(policy.scte104.effective, PassPolicy::Pass);
        assert!(!policy.is_degraded());
    }

    #[test]
    fn explicit_drop_is_not_degraded_when_backend_is_unavailable() {
        let policy = OutputAncillaryPolicy::resolve(
            AncillaryPolicy::default(),
            &output(OutputDestination::MpegTsUdp {
                location: "udp://239.0.0.1:9000".to_string(),
            }),
            CaptionFormat::Eia708,
            AncillaryBackendCapabilities::current_gstreamer(),
        );
        assert!(!policy.is_degraded());
        assert!(policy.captions.warning.is_none());
        assert!(policy.scte35.warning.is_none());
        assert!(policy.scte104.warning.is_none());
    }

    #[test]
    fn rtmp_rejects_caption_and_scte_passthrough() {
        let policy = OutputAncillaryPolicy::resolve(
            AncillaryPolicy {
                captions: PassPolicy::Pass,
                scte35: PassPolicy::Pass,
                scte104: PassPolicy::Drop,
            },
            &output(OutputDestination::Rtmp {
                location: "rtmp://example.invalid/live".to_string(),
            }),
            CaptionFormat::Eia608,
            AncillaryBackendCapabilities {
                caption_reinsertion: true,
                scte35_passthrough: true,
                scte104_passthrough: true,
            },
        );
        assert_eq!(policy.captions.effective, PassPolicy::Drop);
        assert_eq!(policy.scte35.effective, PassPolicy::Drop);
    }

    #[test]
    fn generated_cue_lifecycle_keeps_queue_id_event_id_and_pts() {
        let mut tracker = AlertCueTracker::default();
        let out = tracker.splice_out("queue-7", 1_000).expect("splice out");
        let duplicate = tracker
            .splice_out("queue-7", 1_500)
            .expect("idempotent splice out");
        let input = tracker.splice_in("queue-7", 2_000).expect("splice in");
        assert_eq!(out.event_id, duplicate.event_id);
        assert_eq!(out.event_id, input.event_id);
        assert_eq!(out.command, CueCommand::SpliceOut);
        assert_eq!(input.command, CueCommand::SpliceIn);
        assert_eq!(input.pts_ns, 2_000);
        assert_ne!(out.event_id & GENERATED_CUE_NAMESPACE, 0);
    }

    #[test]
    fn ancillary_queue_is_bounded_and_preserves_timestamps() {
        let (sender, mut receiver) = ancillary_channel(1);
        let first = AncillaryPacket::new(
            77,
            AncillaryKind::Caption(CaptionFormat::Eia708),
            vec![1, 2, 3],
        )
        .expect("packet");
        let second = AncillaryPacket::new(88, AncillaryKind::Scte35, vec![4]).expect("packet");
        assert!(sender.try_send(first));
        assert!(!sender.try_send(second));
        assert_eq!(sender.dropped(), 1);
        let received = receiver.try_recv().expect("queued packet");
        assert_eq!(received.pts_ns, 77);
        assert_eq!(received.payload, vec![1, 2, 3]);
    }

    #[test]
    fn oversized_ancillary_payload_is_rejected() {
        assert_eq!(
            AncillaryPacket::new(
                0,
                AncillaryKind::Scte104,
                vec![0; MAX_ANCILLARY_PAYLOAD_BYTES + 1],
            ),
            Err(AncillaryError::PayloadTooLarge)
        );
    }

    #[test]
    fn caption_format_names_are_stable() {
        assert_eq!(CaptionFormat::Eia608.as_str(), "eia608");
        assert_eq!(CaptionFormat::Eia708.as_str(), "eia708");
    }
}
