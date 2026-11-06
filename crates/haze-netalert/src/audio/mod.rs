//! Alert audio for network clients: loading rendered PCM/WAV and encoding it
//! once into Ogg/Opus pages that every recipient shares.

mod ogg_opus;

use std::path::Path;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use tokio_tungstenite::tungstenite::Utf8Bytes;

pub use ogg_opus::{encode, OUTPUT_SAMPLE_RATE};

/// Interleaved signed 16-bit PCM.
#[derive(Debug, Clone, PartialEq)]
pub struct Pcm {
    pub samples: Vec<i16>,
    pub sample_rate: u32,
    pub channels: u16,
}

/// One encoded alert audio stream as it goes out on a socket: the
/// `audio.start` text frame, one binary frame per Ogg page batch, then
/// `audio.end`.
#[derive(Debug)]
pub struct AudioStream {
    pub start: Utf8Bytes,
    pub pages: Vec<Bytes>,
    pub end: Utf8Bytes,
}

/// Reads a WAV file, or raw little-endian PCM at the given rate and channel
/// count (Haze renders alert audio as `pcm_s16le` at 48 kHz mono).
pub fn load_pcm(path: &Path, raw_rate: u32, raw_channels: u16) -> Result<Pcm> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read audio {}", path.display()))?;
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
        return parse_wav(&bytes)
            .with_context(|| format!("unsupported WAV audio {}", path.display()));
    }
    if raw_channels == 0 || raw_rate == 0 {
        bail!(
            "raw PCM audio {} has no rate or channel count",
            path.display()
        );
    }
    Ok(Pcm {
        samples: pcm_samples(&bytes),
        sample_rate: raw_rate,
        channels: raw_channels,
    })
}

fn parse_wav(bytes: &[u8]) -> Result<Pcm> {
    let mut format = None;
    let mut offset = 12;
    while offset + 8 <= bytes.len() {
        let id = &bytes[offset..offset + 4];
        let len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into()?) as usize;
        let body_start = offset + 8;
        let body_end = body_start.saturating_add(len).min(bytes.len());
        let body = &bytes[body_start..body_end];
        match id {
            b"fmt " if body.len() >= 16 => {
                let tag = u16::from_le_bytes([body[0], body[1]]);
                let channels = u16::from_le_bytes([body[2], body[3]]);
                let rate = u32::from_le_bytes(body[4..8].try_into()?);
                let bits = u16::from_le_bytes([body[14], body[15]]);
                if !(tag == 1 || tag == 0xFFFE) || bits != 16 {
                    bail!("only 16-bit PCM WAV is supported (format {tag}, {bits} bits)");
                }
                format = Some((rate, channels));
            }
            b"data" => {
                let Some((sample_rate, channels)) = format else {
                    bail!("WAV data chunk precedes its fmt chunk");
                };
                return Ok(Pcm {
                    samples: pcm_samples(body),
                    sample_rate,
                    channels,
                });
            }
            _ => {}
        }
        // Chunks are word aligned.
        offset = body_start.saturating_add(len + (len & 1));
    }
    bail!("WAV has no data chunk")
}

fn pcm_samples(bytes: &[u8]) -> Vec<i16> {
    bytes
        .chunks_exact(2)
        .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_wav_and_raw_pcm() {
        let dir =
            std::env::temp_dir().join(format!("haze-netalert-pcm-{}", crate::util::unix_now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut wav = Vec::new();
        wav.extend_from_slice(b"RIFF\0\0\0\0WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&22_050u32.to_le_bytes());
        wav.extend_from_slice(&44_100u32.to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&4u32.to_le_bytes());
        wav.extend_from_slice(&[1, 0, 2, 0]);
        std::fs::write(dir.join("a.wav"), &wav).unwrap();
        std::fs::write(dir.join("a.pcm16le"), [3, 0, 4, 0]).unwrap();

        let parsed = load_pcm(&dir.join("a.wav"), 48_000, 1).unwrap();
        assert_eq!(
            (parsed.sample_rate, parsed.channels, parsed.samples),
            (22_050, 1, vec![1, 2])
        );
        let raw = load_pcm(&dir.join("a.pcm16le"), 48_000, 1).unwrap();
        assert_eq!((raw.sample_rate, raw.samples), (48_000, vec![3, 4]));
        let _ = std::fs::remove_dir_all(dir);
    }
}
