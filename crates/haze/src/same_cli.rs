use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use chrono::{Datelike, Timelike, Utc};
use clap::{Args, Subcommand};
use serde::Serialize;

use haze_same::{
    attention_tone, eom_sequence, generate_same_header_attention_sequence,
    generate_same_header_attention_sequence_with_attention, generate_same_header_sequence,
    generate_same_header_sequence_with_attention, SameAudio, SameHeader, ToneType, SAMPLE_RATE,
};

const NPAS_ATTENTION_SIGNAL_PATH: &str = "bundle/audio/Canadian_Alerting_Attention_Signal.wav";

#[derive(Debug, Subcommand)]
pub enum SameCommand {
    /// Generate SAME header/tone/EOM audio.
    Generate(SameGenerateArgs),
}

#[derive(Debug, Args)]
pub struct SameGenerateArgs {
    /// SAME originator code.
    #[arg(long, default_value = "WXR")]
    originator: String,

    /// SAME event code.
    #[arg(long)]
    event: String,

    /// Comma-separated SAME location codes.
    #[arg(long, value_delimiter = ',')]
    locations: Vec<String>,

    /// SAME duration as HHMM.
    #[arg(long, default_value = "0015")]
    duration: String,

    /// Station callsign / sender field.
    #[arg(long, default_value = "HAZE")]
    callsign: String,

    /// SAME issue time as JJJHHMM. Defaults to current UTC.
    #[arg(long)]
    issue_time: Option<String>,

    /// Attention tone type: WXR, EAS, NPAS, EGG_TIMER, QUEBEC, or NONE.
    #[arg(long, default_value = "WXR")]
    tone: ToneArg,

    /// Audio segment to generate: full, header, eom, or tone.
    #[arg(long, default_value = "full")]
    sequence: SameSequenceArg,

    /// Emit JSON instead of a plain header string.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy)]
enum ToneArg {
    None,
    Builtin(ToneType),
    Npas,
}

impl FromStr for ToneArg {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let tone = match value.trim().to_ascii_uppercase().as_str() {
            "" | "NONE" | "NO" | "OFF" => Self::None,
            "WXR" | "WEATHER" => Self::Builtin(ToneType::Wxr),
            "EAS" => Self::Builtin(ToneType::Eas),
            "NPAS" | "ALERT_READY" | "ALERT-READY" => Self::Npas,
            "EGG_TIMER" | "EGG-TIMER" | "EGG" => Self::Builtin(ToneType::EggTimer),
            "QUEBEC" | "QC" => Self::Builtin(ToneType::Quebec),
            other => bail!("unsupported attention tone {other:?}"),
        };
        Ok(tone)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SameSequenceArg {
    Full,
    Header,
    Eom,
    Tone,
}

impl FromStr for SameSequenceArg {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "full" | "all" | "complete" => Ok(Self::Full),
            "header" | "start" | "attention" | "head" => Ok(Self::Header),
            "eom" | "end" | "tail" => Ok(Self::Eom),
            "tone" | "attention_tone" | "attention-tone" => Ok(Self::Tone),
            other => bail!("unsupported SAME sequence {other:?}"),
        }
    }
}

#[derive(Debug, Serialize)]
struct SameGenerateOutput {
    ok: bool,
    header: String,
    format: &'static str,
    sample_rate: u32,
    channels: u8,
    audio_base64: String,
    duration_seconds: f32,
}

pub(crate) fn run(command: SameCommand) -> Result<()> {
    match command {
        SameCommand::Generate(args) => run_generate(args),
    }
}

fn run_generate(args: SameGenerateArgs) -> Result<()> {
    let header = SameHeader::new(
        args.originator.trim().to_ascii_uppercase(),
        args.event.trim().to_ascii_uppercase(),
        args.locations
            .iter()
            .map(|location| location.trim().to_string())
            .filter(|location| !location.is_empty()),
        args.duration.trim().to_string(),
        args.callsign.trim().to_string(),
        args.issue_time
            .unwrap_or_else(current_same_issue_time)
            .trim()
            .to_string(),
    )
    .context("failed to build SAME header")?;
    let audio = match args.sequence {
        SameSequenceArg::Full => {
            if let Some(attention) = attention_samples(args.tone)? {
                generate_same_header_sequence_with_attention(&header, Some(&attention))
            } else {
                generate_same_header_sequence(&header, None)
            }
        }
        SameSequenceArg::Header => {
            if let Some(attention) = attention_samples(args.tone)? {
                generate_same_header_attention_sequence_with_attention(&header, Some(&attention))
            } else {
                generate_same_header_attention_sequence(&header, None)
            }
        }
        SameSequenceArg::Eom => eom_sequence(),
        SameSequenceArg::Tone => SameAudio {
            sample_rate: SAMPLE_RATE,
            samples: attention_samples(args.tone)?.unwrap_or_default(),
        },
    };
    let pcm = audio.to_pcm16le();
    let output = SameGenerateOutput {
        ok: true,
        header: header.encoded(),
        format: "raw",
        sample_rate: audio.sample_rate,
        channels: 1,
        audio_base64: encode_base64(&pcm),
        duration_seconds: audio.samples.len() as f32 / audio.sample_rate as f32,
    };
    if args.json {
        println!("{}", serde_json::to_string(&output)?);
    } else {
        println!("{}", output.header);
    }
    Ok(())
}

fn attention_samples(tone: ToneArg) -> Result<Option<Vec<f32>>> {
    match tone {
        ToneArg::None => Ok(None),
        ToneArg::Builtin(tone) => Ok(Some(attention_tone(tone, 8.0))),
        ToneArg::Npas => Ok(Some(load_npas_attention_signal()?)),
    }
}

fn load_npas_attention_signal() -> Result<Vec<f32>> {
    let path = find_npas_attention_signal_path()
        .with_context(|| format!("failed to locate {NPAS_ATTENTION_SIGNAL_PATH}"))?;
    let raw = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    read_wav_mono_f32(&raw).with_context(|| format!("failed to decode {}", path.display()))
}

fn find_npas_attention_signal_path() -> Option<PathBuf> {
    let direct = PathBuf::from(NPAS_ATTENTION_SIGNAL_PATH);
    if direct.exists() {
        return Some(direct);
    }
    if let Ok(current_dir) = std::env::current_dir() {
        for dir in current_dir.ancestors() {
            let candidate = dir.join(NPAS_ATTENTION_SIGNAL_PATH);
            if candidate.exists() {
                return Some(candidate);
            }
            let candidate = dir
                .join("audio")
                .join("Canadian_Alerting_Attention_Signal.wav");
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        for dir in exe.ancestors() {
            let candidate = dir.join(NPAS_ATTENTION_SIGNAL_PATH);
            if candidate.exists() {
                return Some(candidate);
            }
            let candidate = dir
                .join("audio")
                .join("Canadian_Alerting_Attention_Signal.wav");
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }
    None
}

fn read_wav_mono_f32(raw: &[u8]) -> Result<Vec<f32>> {
    if raw.len() < 12 || &raw[0..4] != b"RIFF" || &raw[8..12] != b"WAVE" {
        bail!("not a RIFF/WAVE file");
    }
    let mut cursor = 12usize;
    let mut format_tag = 0u16;
    let mut channels = 0u16;
    let mut sample_rate = 0u32;
    let mut bits_per_sample = 0u16;
    let mut data = &[][..];
    while cursor + 8 <= raw.len() {
        let chunk_id = &raw[cursor..cursor + 4];
        let chunk_size = u32::from_le_bytes(raw[cursor + 4..cursor + 8].try_into()?) as usize;
        let start = cursor + 8;
        let end = start.saturating_add(chunk_size).min(raw.len());
        if chunk_id == b"fmt " {
            if chunk_size < 16 || end > raw.len() {
                bail!("invalid wav fmt chunk");
            }
            format_tag = u16::from_le_bytes(raw[start..start + 2].try_into()?);
            channels = u16::from_le_bytes(raw[start + 2..start + 4].try_into()?);
            sample_rate = u32::from_le_bytes(raw[start + 4..start + 8].try_into()?);
            bits_per_sample = u16::from_le_bytes(raw[start + 14..start + 16].try_into()?);
        } else if chunk_id == b"data" {
            data = &raw[start..end];
        }
        cursor = end + (chunk_size % 2);
    }
    if channels == 0 || sample_rate == 0 || data.is_empty() {
        bail!("wav is missing audio metadata or data");
    }
    let mut samples = decode_wav_samples(data, format_tag, channels, bits_per_sample)?;
    if channels > 1 {
        samples = downmix_to_mono(&samples, channels as usize);
    }
    if sample_rate != SAMPLE_RATE {
        samples = resample_linear(&samples, sample_rate, SAMPLE_RATE);
    }
    Ok(samples)
}

fn decode_wav_samples(
    data: &[u8],
    format_tag: u16,
    channels: u16,
    bits_per_sample: u16,
) -> Result<Vec<f32>> {
    let bytes_per_sample = usize::from(bits_per_sample / 8);
    if bytes_per_sample == 0 || channels == 0 {
        bail!("invalid wav sample format");
    }
    let mut samples = Vec::with_capacity(data.len() / bytes_per_sample);
    for chunk in data.chunks_exact(bytes_per_sample) {
        let sample = match (format_tag, bits_per_sample) {
            (1, 16) => i16::from_le_bytes(chunk.try_into()?) as f32 / i16::MAX as f32,
            (1, 24) => {
                let value = i32::from_le_bytes([
                    chunk[0],
                    chunk[1],
                    chunk[2],
                    if chunk[2] & 0x80 != 0 { 0xff } else { 0x00 },
                ]);
                value as f32 / 8_388_607.0
            }
            (1, 32) => i32::from_le_bytes(chunk.try_into()?) as f32 / i32::MAX as f32,
            (3, 32) => f32::from_le_bytes(chunk.try_into()?),
            other => bail!("unsupported wav format {:?}", other),
        };
        samples.push(sample.clamp(-1.0, 1.0));
    }
    Ok(samples)
}

fn downmix_to_mono(samples: &[f32], channels: usize) -> Vec<f32> {
    samples
        .chunks_exact(channels)
        .map(|frame| frame.iter().sum::<f32>() / channels as f32)
        .collect()
}

fn resample_linear(samples: &[f32], source_rate: u32, target_rate: u32) -> Vec<f32> {
    if samples.is_empty() || source_rate == target_rate {
        return samples.to_vec();
    }
    let out_len =
        (samples.len() as u64 * u64::from(target_rate) / u64::from(source_rate)).max(1) as usize;
    let ratio = source_rate as f64 / target_rate as f64;
    let mut out = Vec::with_capacity(out_len);
    for index in 0..out_len {
        let source = index as f64 * ratio;
        let left = source.floor() as usize;
        let right = (left + 1).min(samples.len() - 1);
        let frac = (source - left as f64) as f32;
        out.push(samples[left] + ((samples[right] - samples[left]) * frac));
    }
    out
}

fn current_same_issue_time() -> String {
    let now = Utc::now();
    format!("{:03}{:02}{:02}", now.ordinal(), now.hour(), now.minute())
}

fn encode_base64(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let n = ((b0 as u32) << 16) | ((b1 as u32) << 8) | b2 as u32;
        output.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        output.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            output.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(TABLE[(n & 0x3f) as usize] as char);
        } else {
            output.push('=');
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_panel_tone_names() {
        assert!(matches!(
            "WXR".parse::<ToneArg>().unwrap(),
            ToneArg::Builtin(ToneType::Wxr)
        ));
        assert!(matches!("NPAS".parse::<ToneArg>().unwrap(), ToneArg::Npas));
        assert!(matches!(
            "EGG_TIMER".parse::<ToneArg>().unwrap(),
            ToneArg::Builtin(ToneType::EggTimer)
        ));
        assert!(matches!(
            "QUEBEC".parse::<ToneArg>().unwrap(),
            ToneArg::Builtin(ToneType::Quebec)
        ));
        assert!(matches!("NONE".parse::<ToneArg>().unwrap(), ToneArg::None));
    }

    #[test]
    fn parses_same_sequence_names() {
        assert_eq!(
            "full".parse::<SameSequenceArg>().unwrap(),
            SameSequenceArg::Full
        );
        assert_eq!(
            "header".parse::<SameSequenceArg>().unwrap(),
            SameSequenceArg::Header
        );
        assert_eq!(
            "eom".parse::<SameSequenceArg>().unwrap(),
            SameSequenceArg::Eom
        );
        assert_eq!(
            "tone".parse::<SameSequenceArg>().unwrap(),
            SameSequenceArg::Tone
        );
    }

    #[test]
    fn issue_time_has_same_shape() {
        let issue_time = current_same_issue_time();

        assert_eq!(issue_time.len(), 7);
        assert!(issue_time.bytes().all(|byte| byte.is_ascii_digit()));
    }

    #[test]
    fn encodes_base64_without_external_dependency() {
        assert_eq!(encode_base64(b""), "");
        assert_eq!(encode_base64(b"f"), "Zg==");
        assert_eq!(encode_base64(b"fo"), "Zm8=");
        assert_eq!(encode_base64(b"foo"), "Zm9v");
    }

    #[test]
    fn decodes_npas_attention_signal_wav() {
        let samples = load_npas_attention_signal().expect("bundled NPAS WAV");

        assert_eq!(samples.len(), SAMPLE_RATE as usize * 8);
        assert!(samples.iter().any(|sample| sample.abs() > 0.01));
    }
}
