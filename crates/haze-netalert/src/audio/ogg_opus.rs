//! Ogg/Opus encoding (RFC 7845) with natively linked libopus.
//!
//! Each returned page batch becomes one binary WebSocket frame. The Ogg
//! bitstream serial number is the stream ID announced in `audio.start`, so a
//! client can demultiplex overlapping alert streams by serial alone.

use std::borrow::Cow;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use ogg::writing::{PacketWriteEndInfo, PacketWriter};
use opus::{Application, Bitrate, Channels, Encoder};

use super::Pcm;

/// Ogg Opus granule positions always count 48 kHz samples.
pub const OUTPUT_SAMPLE_RATE: u32 = 48_000;
const OPUS_INPUT_RATES: [u32; 5] = [8_000, 12_000, 16_000, 24_000, 48_000];
const FRAME_DIVISOR: u32 = 50; // 20 ms frames
const GRANULE_PER_FRAME: u64 = (OUTPUT_SAMPLE_RATE / FRAME_DIVISOR) as u64;
/// Half a second of audio per page keeps the first audible frame close to the
/// `audio.start` message without flooding clients with tiny frames.
const PACKETS_PER_PAGE: usize = 25;
const BITRATE_PER_CHANNEL: i32 = 32_000;
const MAX_PACKET_BYTES: usize = 4_000;
const VENDOR: &str = "haze-netalert";

#[derive(Debug)]
pub struct Encoded {
    /// Complete Ogg pages: headers first, last batch carries end-of-stream.
    pub pages: Vec<Bytes>,
    pub duration_ms: u64,
}

pub fn encode(pcm: &Pcm, serial: u32) -> Result<Encoded> {
    let channels = match pcm.channels {
        1 => Channels::Mono,
        2 => Channels::Stereo,
        other => bail!("Opus streams support 1 or 2 channels, not {other}"),
    };
    let channel_count = usize::from(pcm.channels);
    let frames_in = pcm.samples.len() / channel_count;
    if frames_in == 0 {
        bail!("alert audio is empty");
    }
    let (rate, samples) = if OPUS_INPUT_RATES.contains(&pcm.sample_rate) {
        (pcm.sample_rate, Cow::Borrowed(pcm.samples.as_slice()))
    } else {
        (
            OUTPUT_SAMPLE_RATE,
            Cow::Owned(resample_linear(
                &pcm.samples,
                channel_count,
                pcm.sample_rate,
                OUTPUT_SAMPLE_RATE,
            )),
        )
    };

    let mut encoder = Encoder::new(rate, channels, Application::Audio)
        .context("failed to create Opus encoder")?;
    encoder
        .set_bitrate(Bitrate::Bits(BITRATE_PER_CHANNEL * i32::from(pcm.channels)))
        .context("failed to set Opus bitrate")?;
    let lookahead = usize::try_from(
        encoder
            .get_lookahead()
            .context("failed to read Opus lookahead")?,
    )?;
    let pre_skip = lookahead as u64 * u64::from(OUTPUT_SAMPLE_RATE) / u64::from(rate);
    let total_granule =
        pre_skip + frames_in as u64 * u64::from(OUTPUT_SAMPLE_RATE) / u64::from(pcm.sample_rate);
    // The encoder delays its output by `lookahead`, so feed that much silence
    // after the audio or its last samples never leave the encoder.
    let mut samples = samples.into_owned();
    samples.resize(samples.len() + lookahead * channel_count, 0);

    let mut writer = PacketWriter::new(Vec::new());
    let mut pages = Vec::new();
    writer.write_packet(
        opus_head(pcm.channels, u16::try_from(pre_skip)?, pcm.sample_rate),
        serial,
        PacketWriteEndInfo::EndPage,
        0,
    )?;
    writer.write_packet(opus_tags(), serial, PacketWriteEndInfo::EndPage, 0)?;
    pages.push(drain(&mut writer));

    let frame_len = (rate / FRAME_DIVISOR) as usize * channel_count;
    let frame_count = samples.len().div_ceil(frame_len);
    let mut padded = vec![0i16; frame_len];
    for (index, chunk) in samples.chunks(frame_len).enumerate() {
        let input = if chunk.len() == frame_len {
            chunk
        } else {
            padded[..chunk.len()].copy_from_slice(chunk);
            padded[chunk.len()..].fill(0);
            &padded
        };
        let packet = encoder
            .encode_vec(input, MAX_PACKET_BYTES)
            .context("Opus encode failed")?;
        // Granules count decoded samples from the first packet, pre-skip
        // included. Only the last one is cut short to trim the padding.
        let last = index + 1 == frame_count;
        let granule = if last {
            total_granule
        } else {
            (index as u64 + 1) * GRANULE_PER_FRAME
        };
        let end = if last {
            PacketWriteEndInfo::EndStream
        } else if (index + 1) % PACKETS_PER_PAGE == 0 {
            PacketWriteEndInfo::EndPage
        } else {
            PacketWriteEndInfo::NormalPacket
        };
        let flush = !matches!(end, PacketWriteEndInfo::NormalPacket);
        writer.write_packet(packet, serial, end, granule)?;
        if flush {
            pages.push(drain(&mut writer));
        }
    }
    Ok(Encoded {
        pages,
        duration_ms: frames_in as u64 * 1_000 / u64::from(pcm.sample_rate),
    })
}

fn drain(writer: &mut PacketWriter<'_, Vec<u8>>) -> Bytes {
    Bytes::from(std::mem::take(writer.inner_mut()))
}

fn opus_head(channels: u16, pre_skip: u16, input_rate: u32) -> Vec<u8> {
    let mut head = Vec::with_capacity(19);
    head.extend_from_slice(b"OpusHead");
    head.push(1);
    head.push(channels as u8);
    head.extend_from_slice(&pre_skip.to_le_bytes());
    head.extend_from_slice(&input_rate.to_le_bytes());
    head.extend_from_slice(&0i16.to_le_bytes());
    head.push(0);
    head
}

fn opus_tags() -> Vec<u8> {
    let mut tags = Vec::with_capacity(16 + VENDOR.len());
    tags.extend_from_slice(b"OpusTags");
    tags.extend_from_slice(&(VENDOR.len() as u32).to_le_bytes());
    tags.extend_from_slice(VENDOR.as_bytes());
    tags.extend_from_slice(&0u32.to_le_bytes());
    tags
}

/// Linear interpolation, used only when a source rate is one Opus cannot take.
fn resample_linear(samples: &[i16], channels: usize, from: u32, to: u32) -> Vec<i16> {
    let frames_in = samples.len() / channels;
    let frames_out = (frames_in as u64 * u64::from(to) / u64::from(from)) as usize;
    let step = f64::from(from) / f64::from(to);
    let mut out = Vec::with_capacity(frames_out * channels);
    for frame in 0..frames_out {
        let position = frame as f64 * step;
        let left = (position as usize).min(frames_in - 1);
        let right = (left + 1).min(frames_in - 1);
        let fraction = position - left as f64;
        for channel in 0..channels {
            let a = f64::from(samples[left * channels + channel]);
            let b = f64::from(samples[right * channels + channel]);
            out.push((a + (b - a) * fraction).round() as i16);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone(rate: u32, seconds: f32) -> Pcm {
        let frames = (rate as f32 * seconds) as usize;
        let samples = (0..frames)
            .map(|index| {
                let phase = index as f32 / rate as f32 * 853.0 * std::f32::consts::TAU;
                (phase.sin() * f32::from(i16::MAX) * 0.3) as i16
            })
            .collect();
        Pcm {
            samples,
            sample_rate: rate,
            channels: 1,
        }
    }

    /// (serial, granule, header type, packets ending on the page) per page.
    fn pages(stream: &[u8]) -> Vec<(u32, u64, u8, usize)> {
        let mut out = Vec::new();
        let mut offset = 0;
        while offset < stream.len() {
            assert_eq!(&stream[offset..offset + 4], b"OggS");
            let header_type = stream[offset + 5];
            let granule = u64::from_le_bytes(stream[offset + 6..offset + 14].try_into().unwrap());
            let serial = u32::from_le_bytes(stream[offset + 14..offset + 18].try_into().unwrap());
            let segments = stream[offset + 26] as usize;
            let lacing = &stream[offset + 27..offset + 27 + segments];
            let body: usize = lacing.iter().map(|len| *len as usize).sum();
            let packets = lacing.iter().filter(|len| **len < 255).count();
            out.push((serial, granule, header_type, packets));
            offset += 27 + segments + body;
        }
        out
    }

    #[test]
    fn encodes_a_valid_ogg_opus_stream() {
        let encoded = encode(&tone(48_000, 1.03), 7).unwrap();
        assert_eq!(encoded.duration_ms, 1_030);
        assert!(encoded.pages[0]
            .windows(8)
            .any(|window| window == b"OpusHead"));
        let stream = encoded.pages.concat();
        let pages = pages(&stream);
        assert!(pages.iter().all(|(serial, _, _, _)| *serial == 7));
        assert_eq!(pages[0].2 & 0x02, 0x02, "first page begins the stream");
        let (_, last_granule, last_type, _) = *pages.last().unwrap();
        assert_eq!(last_type & 0x04, 0x04, "last page ends the stream");
        // Granules count 48 kHz samples after pre-skip, trimming the padded final frame.
        let pre_skip = u16::from_le_bytes([encoded.pages[0][28 + 10], encoded.pages[0][28 + 11]]);
        assert_eq!(last_granule, u64::from(pre_skip) + 49_440);
        // The first audio page starts at granule zero, so players do not see a gap.
        let (_, first_audio_granule, _, first_audio_packets) = pages[2];
        assert_eq!(
            first_audio_granule,
            first_audio_packets as u64 * GRANULE_PER_FRAME
        );
    }

    #[test]
    fn frame_aligned_audio_still_flushes_the_encoder_lookahead() {
        // 1 s is exactly 50 frames, so only the lookahead padding can supply
        // the samples the final granule promises past the delayed output.
        let encoded = encode(&tone(48_000, 1.0), 3).unwrap();
        let pages = pages(&encoded.pages.concat());
        let last_granule = pages.last().unwrap().1;
        let audio_packets = pages.iter().map(|page| page.3).sum::<usize>() - 2;
        assert!(audio_packets as u64 * GRANULE_PER_FRAME >= last_granule);
    }

    #[test]
    fn resamples_rates_opus_cannot_take() {
        let encoded = encode(&tone(22_050, 0.5), 1).unwrap();
        assert_eq!(encoded.duration_ms, 500);
        assert!(encoded.pages.len() >= 2);
    }
}
