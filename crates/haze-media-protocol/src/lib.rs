//! Compact frames for PCM travelling through Haze's internal media broker.
//!
//! The public and general-purpose event broker continues to use newline-delimited
//! JSON. Continuous PCM is different: repeatedly Base64-encoding raw audio and
//! parsing the resulting JSON is expensive, so the dedicated media broker uses
//! this bounded binary frame after its normal JSON client handshake.

use std::error::Error;
use std::fmt;
use std::str;

/// Prefix used to distinguish a binary PCM frame from a JSON bridge event.
pub const PCM_FRAME_MAGIC: [u8; 4] = *b"HPCM";
/// Current binary PCM wire version.
pub const PCM_FRAME_VERSION: u8 = 1;
/// Bytes preceding the variable-length PCM frame body.
pub const PCM_FRAME_PREFIX_LEN: usize = 10;
/// Largest accepted binary frame. This remains below the legacy bridge line limit.
pub const MAX_PCM_FRAME_LEN: usize = 4 * 1024 * 1024;

const FLAG_DISCONTINUITY: u8 = 0b0000_0001;
const FIXED_BODY_LEN: usize = 35;

/// The media classification associated with a PCM frame.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum PcmMediaKind {
    Silence = 0,
    Routine = 1,
    Alert = 2,
    OperatorBreakIn = 3,
}

impl TryFrom<u8> for PcmMediaKind {
    type Error = PcmFrameError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Silence),
            1 => Ok(Self::Routine),
            2 => Ok(Self::Alert),
            3 => Ok(Self::OperatorBreakIn),
            other => Err(PcmFrameError::UnknownMediaKind(other)),
        }
    }
}

/// Borrowed PCM fields ready for binary framing.
#[derive(Debug, Clone, Copy)]
pub struct PcmFrame<'a> {
    pub feed_id: &'a str,
    pub queue_id: Option<&'a str>,
    pub sample_rate: u32,
    pub channels: u16,
    pub duration_ms: u32,
    pub sequence: u64,
    pub pts_ns: u64,
    pub discontinuity: bool,
    pub media_kind: PcmMediaKind,
    pub pcm: &'a [u8],
}

/// Borrowed fields decoded from a binary PCM frame.
#[derive(Debug, Clone, Copy)]
pub struct DecodedPcmFrame<'a> {
    pub feed_id: &'a str,
    pub queue_id: Option<&'a str>,
    pub sample_rate: u32,
    pub channels: u16,
    pub duration_ms: u32,
    pub sequence: u64,
    pub pts_ns: u64,
    pub discontinuity: bool,
    pub media_kind: PcmMediaKind,
    pub pcm: &'a [u8],
}

/// Errors returned when a binary PCM frame is malformed or too large.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PcmFrameError {
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidFlags(u8),
    InvalidLength,
    Truncated,
    FieldTooLarge,
    FrameTooLarge,
    EmptyFeedId,
    InvalidUtf8,
    UnknownMediaKind(u8),
}

impl fmt::Display for PcmFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("invalid PCM frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported PCM frame version {version}")
            }
            Self::InvalidFlags(flags) => write!(formatter, "invalid PCM frame flags {flags}"),
            Self::InvalidLength => formatter.write_str("invalid PCM frame length"),
            Self::Truncated => formatter.write_str("truncated PCM frame"),
            Self::FieldTooLarge => formatter.write_str("PCM frame field is too large"),
            Self::FrameTooLarge => formatter.write_str("PCM frame exceeds maximum length"),
            Self::EmptyFeedId => formatter.write_str("PCM frame feed ID is empty"),
            Self::InvalidUtf8 => formatter.write_str("PCM frame identifier is not UTF-8"),
            Self::UnknownMediaKind(kind) => write!(formatter, "unknown PCM media kind {kind}"),
        }
    }
}

impl Error for PcmFrameError {}

/// Encodes a PCM frame into `out`, replacing any existing contents.
///
/// The frame is deliberately fixed-endian and allocation-friendly. Its payload
/// remains raw PCM, which avoids Base64 expansion on the real-time media path.
pub fn encode_pcm_frame(out: &mut Vec<u8>, frame: PcmFrame<'_>) -> Result<(), PcmFrameError> {
    let feed_id = frame.feed_id.as_bytes();
    let queue_id = frame.queue_id.unwrap_or_default().as_bytes();
    if feed_id.is_empty() {
        return Err(PcmFrameError::EmptyFeedId);
    }
    let feed_len = u16::try_from(feed_id.len()).map_err(|_| PcmFrameError::FieldTooLarge)?;
    let queue_len = u16::try_from(queue_id.len()).map_err(|_| PcmFrameError::FieldTooLarge)?;
    let pcm_len = u32::try_from(frame.pcm.len()).map_err(|_| PcmFrameError::FrameTooLarge)?;
    let body_len = FIXED_BODY_LEN
        .checked_add(feed_id.len())
        .and_then(|length| length.checked_add(queue_id.len()))
        .and_then(|length| length.checked_add(frame.pcm.len()))
        .ok_or(PcmFrameError::FrameTooLarge)?;
    let total_len = PCM_FRAME_PREFIX_LEN
        .checked_add(body_len)
        .ok_or(PcmFrameError::FrameTooLarge)?;
    if total_len > MAX_PCM_FRAME_LEN {
        return Err(PcmFrameError::FrameTooLarge);
    }
    let body_len = u32::try_from(body_len).map_err(|_| PcmFrameError::FrameTooLarge)?;

    out.clear();
    out.reserve(total_len);
    out.extend_from_slice(&PCM_FRAME_MAGIC);
    out.push(PCM_FRAME_VERSION);
    out.push(if frame.discontinuity {
        FLAG_DISCONTINUITY
    } else {
        0
    });
    out.extend_from_slice(&body_len.to_be_bytes());
    out.extend_from_slice(&feed_len.to_be_bytes());
    out.extend_from_slice(&queue_len.to_be_bytes());
    out.extend_from_slice(&frame.sample_rate.to_be_bytes());
    out.extend_from_slice(&frame.channels.to_be_bytes());
    out.extend_from_slice(&frame.duration_ms.to_be_bytes());
    out.extend_from_slice(&frame.sequence.to_be_bytes());
    out.extend_from_slice(&frame.pts_ns.to_be_bytes());
    out.push(frame.media_kind as u8);
    out.extend_from_slice(&pcm_len.to_be_bytes());
    out.extend_from_slice(feed_id);
    out.extend_from_slice(queue_id);
    out.extend_from_slice(frame.pcm);
    Ok(())
}

/// Returns the full frame length after enough leading bytes have been read.
pub fn pcm_frame_len_from_prefix(prefix: &[u8]) -> Result<usize, PcmFrameError> {
    if prefix.len() < PCM_FRAME_PREFIX_LEN {
        return Err(PcmFrameError::Truncated);
    }
    if prefix[..PCM_FRAME_MAGIC.len()] != PCM_FRAME_MAGIC {
        return Err(PcmFrameError::InvalidMagic);
    }
    if prefix[4] != PCM_FRAME_VERSION {
        return Err(PcmFrameError::UnsupportedVersion(prefix[4]));
    }
    if prefix[5] & !FLAG_DISCONTINUITY != 0 {
        return Err(PcmFrameError::InvalidFlags(prefix[5]));
    }
    let body_len = u32::from_be_bytes(prefix[6..10].try_into().expect("PCM prefix length"));
    let total_len = PCM_FRAME_PREFIX_LEN
        .checked_add(usize::try_from(body_len).map_err(|_| PcmFrameError::FrameTooLarge)?)
        .ok_or(PcmFrameError::FrameTooLarge)?;
    if total_len > MAX_PCM_FRAME_LEN {
        return Err(PcmFrameError::FrameTooLarge);
    }
    if total_len < PCM_FRAME_PREFIX_LEN + FIXED_BODY_LEN {
        return Err(PcmFrameError::InvalidLength);
    }
    Ok(total_len)
}

/// Decodes a complete binary PCM frame without copying its identifiers or PCM.
pub fn decode_pcm_frame(raw: &[u8]) -> Result<DecodedPcmFrame<'_>, PcmFrameError> {
    let total_len = pcm_frame_len_from_prefix(raw)?;
    if raw.len() < total_len {
        return Err(PcmFrameError::Truncated);
    }
    if raw.len() != total_len {
        return Err(PcmFrameError::InvalidLength);
    }

    let flags = raw[5];
    let mut cursor = PCM_FRAME_PREFIX_LEN;
    let feed_len = take_u16(raw, &mut cursor)?;
    let queue_len = take_u16(raw, &mut cursor)?;
    let sample_rate = take_u32(raw, &mut cursor)?;
    let channels = take_u16(raw, &mut cursor)?;
    let duration_ms = take_u32(raw, &mut cursor)?;
    let sequence = take_u64(raw, &mut cursor)?;
    let pts_ns = take_u64(raw, &mut cursor)?;
    let media_kind = PcmMediaKind::try_from(take_u8(raw, &mut cursor)?)?;
    let pcm_len =
        usize::try_from(take_u32(raw, &mut cursor)?).map_err(|_| PcmFrameError::FrameTooLarge)?;
    let feed_id = take_bytes(raw, &mut cursor, usize::from(feed_len))?;
    let queue_id = take_bytes(raw, &mut cursor, usize::from(queue_len))?;
    let pcm = take_bytes(raw, &mut cursor, pcm_len)?;
    if cursor != raw.len() {
        return Err(PcmFrameError::InvalidLength);
    }
    let feed_id = str::from_utf8(feed_id).map_err(|_| PcmFrameError::InvalidUtf8)?;
    if feed_id.is_empty() {
        return Err(PcmFrameError::EmptyFeedId);
    }
    let queue_id = if queue_id.is_empty() {
        None
    } else {
        Some(str::from_utf8(queue_id).map_err(|_| PcmFrameError::InvalidUtf8)?)
    };
    Ok(DecodedPcmFrame {
        feed_id,
        queue_id,
        sample_rate,
        channels,
        duration_ms,
        sequence,
        pts_ns,
        discontinuity: flags & FLAG_DISCONTINUITY != 0,
        media_kind,
        pcm,
    })
}

fn take_u8(raw: &[u8], cursor: &mut usize) -> Result<u8, PcmFrameError> {
    let bytes = take_bytes(raw, cursor, 1)?;
    Ok(bytes[0])
}

fn take_u16(raw: &[u8], cursor: &mut usize) -> Result<u16, PcmFrameError> {
    let bytes = take_bytes(raw, cursor, 2)?;
    Ok(u16::from_be_bytes(
        bytes.try_into().expect("two PCM frame bytes"),
    ))
}

fn take_u32(raw: &[u8], cursor: &mut usize) -> Result<u32, PcmFrameError> {
    let bytes = take_bytes(raw, cursor, 4)?;
    Ok(u32::from_be_bytes(
        bytes.try_into().expect("four PCM frame bytes"),
    ))
}

fn take_u64(raw: &[u8], cursor: &mut usize) -> Result<u64, PcmFrameError> {
    let bytes = take_bytes(raw, cursor, 8)?;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("eight PCM frame bytes"),
    ))
}

fn take_bytes<'a>(
    raw: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], PcmFrameError> {
    let end = cursor
        .checked_add(len)
        .ok_or(PcmFrameError::InvalidLength)?;
    let bytes = raw.get(*cursor..end).ok_or(PcmFrameError::Truncated)?;
    *cursor = end;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_pcm_without_base64_expansion() {
        let frame = PcmFrame {
            feed_id: "cwxr-on01",
            queue_id: Some("alert-1"),
            sample_rate: 48_000,
            channels: 1,
            duration_ms: 100,
            sequence: 7,
            pts_ns: 700_000_000,
            discontinuity: true,
            media_kind: PcmMediaKind::Alert,
            pcm: &[1, 0, 2, 0],
        };
        let mut encoded = Vec::new();

        encode_pcm_frame(&mut encoded, frame).expect("encode PCM frame");
        let decoded = decode_pcm_frame(&encoded).expect("decode PCM frame");

        assert_eq!(decoded.feed_id, frame.feed_id);
        assert_eq!(decoded.queue_id, frame.queue_id);
        assert_eq!(decoded.sample_rate, frame.sample_rate);
        assert_eq!(decoded.channels, frame.channels);
        assert_eq!(decoded.duration_ms, frame.duration_ms);
        assert_eq!(decoded.sequence, frame.sequence);
        assert_eq!(decoded.pts_ns, frame.pts_ns);
        assert!(decoded.discontinuity);
        assert_eq!(decoded.media_kind, PcmMediaKind::Alert);
        assert_eq!(decoded.pcm, frame.pcm);
        assert_eq!(pcm_frame_len_from_prefix(&encoded), Ok(encoded.len()));
    }

    #[test]
    fn rejects_unknown_flags_and_trailing_bytes() {
        let mut encoded = Vec::new();
        encode_pcm_frame(
            &mut encoded,
            PcmFrame {
                feed_id: "feed",
                queue_id: None,
                sample_rate: 48_000,
                channels: 1,
                duration_ms: 20,
                sequence: 0,
                pts_ns: 0,
                discontinuity: false,
                media_kind: PcmMediaKind::Routine,
                pcm: &[0, 0],
            },
        )
        .expect("encode PCM frame");

        encoded[5] = 0b1000_0000;
        assert_eq!(
            pcm_frame_len_from_prefix(&encoded),
            Err(PcmFrameError::InvalidFlags(128))
        );
        encoded[5] = 0;
        encoded.push(0);
        assert!(matches!(
            decode_pcm_frame(&encoded),
            Err(PcmFrameError::InvalidLength)
        ));
    }
}
