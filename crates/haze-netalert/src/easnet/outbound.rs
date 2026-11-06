//! Outbound EAS NET: turning `cap.alert.audio.ready` airings into peer
//! messages, with audio uploaded (SSH peers) or offered by URL (TCP peers).

use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use tokio::fs;
use tokio::sync::mpsc;
use tracing::warn;

use super::config::{Config, PeerConfig, PeerProtocol};
use super::message::{event_matches_peer, AudioUpload, EasNetMessage};
use super::server::AudioRegistry;
use super::transport::send_to_peer;
use super::EASNET_SOURCE;
use crate::bridge::Bridge;
use crate::util::{
    make_nonce_token, nested_text, resolve_path, unix_now_ms, value_list, value_text,
};

const SAME_SAMPLE_RATE: u32 = 48_000;
const SAME_BIT_SAMPLES: usize = 92;
const SAME_PREAMBLE_BYTES: usize = 16;
const SAME_INTER_BURST_SAMPLES: usize = 48_000;
const SAME_BURST_LEAD_SAMPLES: usize = 4_800;
const SAME_PRE_ATTENTION_SAMPLES: usize = 48_000;
const SAME_ATTENTION_SAMPLES: usize = 384_000;
const SAME_EOM_LEAD_SAMPLES: usize = 48_000;
const SAME_EOM_TAIL_SAMPLES: usize = 38_400;
const PLAYLIST_POST_HEADER_SILENCE_SAMPLES: usize = 48_000;

/// Sends each airing to every matching send-capable peer, in arrival order.
pub async fn run(
    config: Config,
    bridge: Bridge,
    registry: AudioRegistry,
    mut events: mpsc::Receiver<Value>,
) -> Result<()> {
    while let Some(event) = events.recv().await {
        let data = event.get("data").cloned().unwrap_or_else(|| json!({}));
        for peer in config
            .peers
            .iter()
            .filter(|peer| peer.enabled && peer.direction.can_send())
            .filter(|peer| event_matches_peer(peer, &data))
        {
            let outcome = match outbound_message_for_event(&config, peer, &registry, &data).await {
                Ok(message) => match send_to_peer(peer, &message).await {
                    Ok(()) => json!({
                        "type": "easnet.message.sent",
                        "source": EASNET_SOURCE,
                        "subject": message.id(),
                        "data": {
                            "peer_id": peer.id,
                            "field_count": message.fields.len(),
                            "audio_url_included": message.get("EAS.AUDIO.STREAM.URL").is_some(),
                        }
                    }),
                    Err(err) => {
                        warn!(peer_id = %peer.id, alert_id = %message.id(), "EAS NET outbound send failed: {err:#}");
                        json!({
                            "type": "easnet.message.failed",
                            "source": EASNET_SOURCE,
                            "subject": message.id(),
                            "data": {"peer_id": peer.id, "error": err.to_string()}
                        })
                    }
                },
                Err(err) => {
                    warn!(peer_id = %peer.id, "EAS NET outbound message build failed: {err:#}");
                    json!({
                        "type": "easnet.message.failed",
                        "source": EASNET_SOURCE,
                        "data": {"peer_id": peer.id, "error": err.to_string()}
                    })
                }
            };
            bridge.publish(outcome).await?;
        }
    }
    Ok(())
}

async fn outbound_message_for_event(
    config: &Config,
    peer: &PeerConfig,
    registry: &AudioRegistry,
    data: &Value,
) -> Result<EasNetMessage> {
    let object = data
        .as_object()
        .ok_or_else(|| anyhow!("event data is not an object"))?;
    let mut fields = BTreeMap::new();
    let alert_id = value_text(object.get("alert_id"))
        .or_else(|| value_text(object.get("id")))
        .or_else(|| value_text(object.get("queue_id")))
        .unwrap_or_else(|| format!("haze-{}", unix_now_ms()));
    fields.insert("EAS.NET.VERSION".to_string(), "1".to_string());
    fields.insert("EAS.NET.FUNC".to_string(), "EAS".to_string());
    fields.insert("EAS.ID".to_string(), alert_id.clone());
    fields.insert(
        "EAS.TYPE".to_string(),
        value_text(object.get("same_event"))
            .or_else(|| value_text(object.get("event")))
            .unwrap_or_else(|| "ADR".to_string()),
    );
    fields.insert(
        "EAS.ORG".to_string(),
        value_text(object.get("same_originator"))
            .or_else(|| value_text(object.get("originator")))
            .unwrap_or_else(|| "WXR".to_string()),
    );
    for (index, location) in value_list(
        object
            .get("same_locations")
            .or_else(|| object.get("locations")),
    )
    .into_iter()
    .enumerate()
    {
        fields.insert(format!("EAS.FIPS_{}", index + 1), location);
    }
    for (source, target) in [
        ("same_header", "EAS.HEADER"),
        ("same_translation", "EAS.TRANSLATION"),
        ("title", "EAS.HEADLINE"),
        ("header", "EAS.HEADLINE"),
        ("description", "EAS.DESCRIPTION"),
        ("instruction", "EAS.INSTRUCTION"),
        ("alert_text", "EAS.TEXT"),
        ("severity", "EAS.SEVERITY"),
        ("urgency", "EAS.URGENCY"),
        ("certainty", "EAS.CERTAINTY"),
        ("same_callsign", "EAS.STATION_ID"),
    ] {
        if !fields.contains_key(target) {
            if let Some(value) = value_text(object.get(source)) {
                fields.insert(target.to_string(), value);
            }
        }
    }
    if let Some(sent) = value_text(
        object
            .get("alert_sent_at")
            .or_else(|| object.get("same_sent_at")),
    ) {
        fields.insert("EAS.START_TIME".to_string(), sent);
    }
    if let Some(expires) = value_text(
        object
            .get("alert_expires_at")
            .or_else(|| object.get("same_expires_at")),
    ) {
        fields.insert("EAS.END_TIME".to_string(), expires);
    }
    if let Some(duration) = value_text(object.get("same_duration")) {
        fields.insert("EAS.DURATION".to_string(), duration);
    }

    let audio_path = value_text(object.get("audio_path")).filter(|_| config.audio.enabled);
    if let Some(audio_path) = audio_path {
        let path = resolve_path(&config.base_dir, Path::new(&audio_path));
        let metadata = fs::metadata(&path)
            .await
            .with_context(|| format!("EAS NET outbound audio missing: {}", path.display()))?;
        if metadata.len() <= config.audio.max_audio_bytes {
            let duration = outbound_audio_duration(object, &path, metadata.len());
            if peer.protocol == PeerProtocol::SshStdin {
                let upload_path = prepare_outbound_audio_file(config, object, path).await?;
                let extension = upload_path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .unwrap_or("wav");
                let remote_name = easnet_audio_remote_name(&alert_id, extension);
                fields.insert("EAS.AUDIO.FILE".to_string(), remote_name.clone());
                fields.insert("EAS.AUDIO.FILE.ALERT".to_string(), remote_name.clone());
                return Ok(EasNetMessage {
                    fields,
                    audio_upload: Some(AudioUpload {
                        local_path: upload_path,
                        remote_name,
                    }),
                });
            }
            if !config.audio.public_base_url.is_empty() {
                let path = prepare_outbound_audio(config, object, path).await?;
                let token = registry
                    .register(peer, path, config.audio.token_ttl, duration)
                    .await;
                let url = format!("{}/easnet/audio/{}", config.audio.public_base_url, token);
                fields.insert("EAS.AUDIO.STREAM.URL".to_string(), url);
            }
        }
    }
    Ok(EasNetMessage {
        fields,
        audio_upload: None,
    })
}

fn source_format(object: &serde_json::Map<String, Value>) -> (u32, u16) {
    let sample_rate = value_text(object.get("sample_rate"))
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(48_000)
        .clamp(8_000, 96_000);
    let channels = value_text(object.get("channels"))
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(1)
        .clamp(1, 8);
    (sample_rate, channels)
}

fn has_extension(path: &Path, extension: &str) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case(extension))
}

/// Ogg/Vorbis for peers that stream the audio URL.
async fn prepare_outbound_audio(
    config: &Config,
    object: &serde_json::Map<String, Value>,
    source_path: PathBuf,
) -> Result<PathBuf> {
    if has_extension(&source_path, "ogg") {
        return Ok(source_path);
    }
    let (sample_rate, channels) = source_format(object);
    let out_dir = config.base_dir.join("runtime").join("audio").join("easnet");
    fs::create_dir_all(&out_dir).await?;
    let out_path = out_dir.join(format!(
        "{}.ogg",
        make_nonce_token(&source_path.to_string_lossy())
    ));
    let output = out_path.clone();
    tokio::task::spawn_blocking(move || {
        encode_pcm_s16le_to_ogg_vorbis(&source_path, &output, sample_rate, channels)
    })
    .await
    .context("EAS NET ffmpeg audio conversion task failed")??;
    Ok(out_path)
}

/// WAV, with Haze's SAME wrapper stripped, for peers that receive uploads.
async fn prepare_outbound_audio_file(
    config: &Config,
    object: &serde_json::Map<String, Value>,
    source_path: PathBuf,
) -> Result<PathBuf> {
    if has_extension(&source_path, "wav") {
        return Ok(source_path);
    }
    let (sample_rate, channels) = source_format(object);
    let out_dir = config.base_dir.join("runtime").join("audio").join("easnet");
    fs::create_dir_all(&out_dir).await?;
    let out_path = out_dir.join(format!(
        "{}.wav",
        make_nonce_token(&source_path.to_string_lossy())
    ));
    let output = out_path.clone();
    let object = object.clone();
    tokio::task::spawn_blocking(move || {
        write_easnet_alert_wav(&source_path, &output, &object, sample_rate, channels)
    })
    .await
    .context("EAS NET WAV audio preparation task failed")??;
    Ok(out_path)
}

fn write_easnet_alert_wav(
    input_path: &Path,
    output_path: &Path,
    object: &serde_json::Map<String, Value>,
    sample_rate: u32,
    channels: u16,
) -> Result<()> {
    let bytes = std::fs::read(input_path)
        .with_context(|| format!("failed to read EAS NET PCM audio {}", input_path.display()))?;
    let bytes =
        easnet_alert_body_pcm(input_path, object, &bytes, sample_rate, channels).unwrap_or(bytes);
    write_pcm_s16le_wav_bytes(&bytes, output_path, sample_rate, channels)
}

fn write_pcm_s16le_wav_bytes(
    bytes: &[u8],
    output_path: &Path,
    sample_rate: u32,
    channels: u16,
) -> Result<()> {
    let channels = channels.clamp(1, 8);
    let sample_rate = sample_rate.clamp(8_000, 96_000);
    let block_align = channels
        .checked_mul(2)
        .ok_or_else(|| anyhow!("invalid EAS NET WAV channel count"))?;
    let complete_len = bytes.len() - (bytes.len() % usize::from(block_align));
    if complete_len == 0 {
        bail!("EAS NET PCM audio is empty for {}", output_path.display());
    }
    let data_len = u32::try_from(complete_len).context("EAS NET WAV audio is too large")?;
    let riff_len = 36u32
        .checked_add(data_len)
        .ok_or_else(|| anyhow!("EAS NET WAV audio is too large"))?;
    let byte_rate = sample_rate
        .checked_mul(u32::from(block_align))
        .ok_or_else(|| anyhow!("invalid EAS NET WAV byte rate"))?;
    let mut out = Vec::with_capacity(44 + complete_len);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&riff_len.to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&channels.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    out.extend_from_slice(&bytes[..complete_len]);
    std::fs::write(output_path, out).with_context(|| {
        format!(
            "failed to write EAS NET WAV audio {}",
            output_path.display()
        )
    })
}

/// The voice body of a Haze `_same` alert render, without the SAME header,
/// attention tone, and EOM the peer generates itself.
fn easnet_alert_body_pcm(
    input_path: &Path,
    object: &serde_json::Map<String, Value>,
    bytes: &[u8],
    sample_rate: u32,
    channels: u16,
) -> Option<Vec<u8>> {
    if sample_rate != SAME_SAMPLE_RATE || channels == 0 {
        return None;
    }
    if !looks_like_haze_same_alert_audio(input_path, object) {
        return None;
    }
    let header = value_text(
        object
            .get("same_header")
            .or_else(|| object.get("EAS.HEADER"))
            .or_else(|| object.get("header")),
    )?;
    let lead_samples = same_header_attention_samples(&header, object)?
        .checked_add(PLAYLIST_POST_HEADER_SILENCE_SAMPLES)?;
    let bytes_per_sample = usize::from(channels).checked_mul(2)?;
    let lead_bytes = lead_samples.checked_mul(bytes_per_sample)?;
    let tail_bytes = same_eom_samples().checked_mul(bytes_per_sample)?;
    let end = bytes.len().checked_sub(tail_bytes)?;
    if lead_bytes >= end {
        return None;
    }
    let body = bytes[lead_bytes..end].to_vec();
    if body.len() < bytes_per_sample * usize::try_from(sample_rate / 2).ok()? {
        return None;
    }
    Some(body)
}

fn looks_like_haze_same_alert_audio(
    input_path: &Path,
    object: &serde_json::Map<String, Value>,
) -> bool {
    let source = value_text(object.get("source"))
        .or_else(|| nested_text(object.get("alert_packet"), &["audio", "source"]))
        .unwrap_or_default();
    if source.contains("cap-same") {
        return true;
    }
    input_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with("_same.pcm16le") || name.contains("_same."))
}

fn same_header_attention_samples(
    header: &str,
    object: &serde_json::Map<String, Value>,
) -> Option<usize> {
    let burst = same_burst_samples(SAME_PREAMBLE_BYTES.checked_add(header.len())?);
    let mut samples = SAME_BURST_LEAD_SAMPLES.checked_add(triple_burst_samples(burst)?)?;
    if same_attention_enabled(object) {
        samples = samples
            .checked_add(SAME_PRE_ATTENTION_SAMPLES)?
            .checked_add(SAME_ATTENTION_SAMPLES)?;
    }
    Some(samples)
}

fn same_eom_samples() -> usize {
    let burst = same_burst_samples(SAME_PREAMBLE_BYTES + 4);
    SAME_EOM_LEAD_SAMPLES + triple_burst_samples(burst).unwrap_or(0) + SAME_EOM_TAIL_SAMPLES
}

fn same_burst_samples(byte_len: usize) -> usize {
    byte_len * 8 * SAME_BIT_SAMPLES
}

fn triple_burst_samples(burst_samples: usize) -> Option<usize> {
    burst_samples
        .checked_mul(3)?
        .checked_add(SAME_INTER_BURST_SAMPLES.checked_mul(2)?)
}

fn same_attention_enabled(object: &serde_json::Map<String, Value>) -> bool {
    let tone = value_text(object.get("same_tone").or_else(|| object.get("tone_type")))
        .or_else(|| nested_text(object.get("alert_packet"), &["same", "tone"]))
        .unwrap_or_else(|| "WXR".to_string());
    !matches!(
        tone.trim().to_ascii_uppercase().as_str(),
        "" | "NONE" | "NO" | "OFF" | "DISABLED"
    )
}

fn outbound_audio_duration(
    object: &serde_json::Map<String, Value>,
    source_path: &Path,
    byte_len: u64,
) -> Option<Duration> {
    for key in ["duration_ms", "audio_duration_ms"] {
        if let Some(ms) = value_text(object.get(key)).and_then(|value| value.parse::<u64>().ok()) {
            if ms > 0 {
                return Some(Duration::from_millis(ms));
            }
        }
    }
    if !["pcm", "s16", "pcm16le"]
        .iter()
        .any(|ext| has_extension(source_path, ext))
    {
        return None;
    }
    let (sample_rate, channels) = source_format(object);
    let bytes_per_second = u64::from(sample_rate) * u64::from(channels) * 2;
    if byte_len == 0 {
        return None;
    }
    Some(Duration::from_secs_f64(
        byte_len as f64 / bytes_per_second as f64,
    ))
}

fn easnet_audio_remote_name(alert_id: &str, extension: &str) -> String {
    let mut safe = String::with_capacity(alert_id.len().min(96) + 4);
    for ch in alert_id.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            safe.push(ch);
        } else {
            safe.push('_');
        }
        if safe.len() >= 96 {
            break;
        }
    }
    let safe = safe.trim_matches('_');
    let extension = extension
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>();
    let extension = if extension.is_empty() {
        "wav"
    } else {
        extension.as_str()
    };
    if safe.is_empty() {
        format!("haze_easnet_audio.{extension}")
    } else {
        format!("{safe}.{extension}")
    }
}

fn encode_pcm_s16le_to_ogg_vorbis(
    input_path: &Path,
    output_path: &Path,
    sample_rate: u32,
    channels: u16,
) -> Result<()> {
    let channels = channels.clamp(1, 8);
    let sample_rate = sample_rate.clamp(8_000, 96_000);
    let len = std::fs::metadata(input_path)
        .with_context(|| format!("failed to stat EAS NET PCM audio {}", input_path.display()))?
        .len();
    if len < u64::from(channels) * 2 {
        bail!("EAS NET PCM audio is empty: {}", input_path.display());
    }

    let mut failures = Vec::new();
    for codec in ["libvorbis", "vorbis"] {
        match run_ffmpeg_pcm_to_ogg(input_path, output_path, sample_rate, channels, codec) {
            Ok(()) => return Ok(()),
            Err(err) => {
                failures.push(format!("{codec}: {err:#}"));
                let _ = std::fs::remove_file(output_path);
            }
        }
    }
    bail!(
        "failed to encode EAS NET Ogg/Vorbis audio {}: {}",
        output_path.display(),
        failures.join("; ")
    )
}

fn run_ffmpeg_pcm_to_ogg(
    input_path: &Path,
    output_path: &Path,
    sample_rate: u32,
    channels: u16,
    codec: &str,
) -> Result<()> {
    let ffmpeg = env::var("HAZE_FFMPEG")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "ffmpeg".to_string());
    let output = Command::new(&ffmpeg)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "s16le",
            "-ar",
        ])
        .arg(sample_rate.to_string())
        .arg("-ac")
        .arg(channels.to_string())
        .arg("-i")
        .arg(input_path)
        .args(["-vn", "-c:a", codec, "-b:a", "128k", "-f", "ogg"])
        .arg(output_path)
        .output()
        .with_context(|| format!("failed to run {ffmpeg} for EAS NET audio conversion"))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let detail = if stderr.is_empty() {
        "no stderr output".to_string()
    } else {
        stderr
    };
    bail!("{ffmpeg} exited with {}: {detail}", output.status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffmpeg_audio_encoder_writes_ogg_vorbis() {
        let dir = std::env::temp_dir().join(format!("haze-easnet-audio-{}", unix_now_ms()));
        std::fs::create_dir_all(&dir).unwrap();
        let input = dir.join("alert.pcm");
        let output = dir.join("alert.ogg");
        let mut pcm = Vec::new();
        for sample in 0..4800 {
            let phase = (sample as f32 / 48_000.0) * 440.0 * std::f32::consts::TAU;
            let value = (phase.sin() * f32::from(i16::MAX) * 0.25) as i16;
            pcm.extend_from_slice(&value.to_le_bytes());
        }
        std::fs::write(&input, pcm).unwrap();

        encode_pcm_s16le_to_ogg_vorbis(&input, &output, 48_000, 1).unwrap();
        let data = std::fs::read(&output).unwrap();
        assert!(data.starts_with(b"OggS"));
        assert!(data.len() > 64);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn dasdec_upload_strips_haze_same_wrapper() {
        let path = PathBuf::from("000_CAP-IT-ALL_alert_same.pcm16le");
        let header = "ZCZC-WXR-SVR-066311+0030-1832355-CAP/IT/A-";
        let mut object = serde_json::Map::new();
        object.insert("same_header".to_string(), Value::String(header.to_string()));
        object.insert(
            "alert_packet".to_string(),
            json!({"audio": {"source": "cap-same-tts"}}),
        );

        let lead = (same_header_attention_samples(header, &object).unwrap()
            + PLAYLIST_POST_HEADER_SILENCE_SAMPLES)
            * 2;
        let tail = same_eom_samples() * 2;
        let body = vec![0x55; 48_000 * 2];
        let mut pcm = vec![0x11; lead];
        pcm.extend_from_slice(&body);
        pcm.extend(std::iter::repeat_n(0x22, tail));

        let stripped = easnet_alert_body_pcm(&path, &object, &pcm, SAME_SAMPLE_RATE, 1).unwrap();
        assert_eq!(stripped, body);
    }
}
