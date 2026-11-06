//! Generated audio for CAP-matched alerts, which no feed has rendered.
//!
//! Product render supplies the spoken text (`alert.speech.request`) so it reads
//! exactly like playout, then haze-tts voices it as 48 kHz mono PCM. Audio is
//! rendered once per alert and language and shared by every client asking.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde_json::json;

use crate::audio::{load_pcm, Pcm, OUTPUT_SAMPLE_RATE};
use crate::bridge::{Bridge, SOURCE};
use crate::hub::CapEntry;
use crate::util::{make_nonce_token, value_text};

const SPEECH_TIMEOUT: Duration = Duration::from_secs(15);
const TTS_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct Speech {
    bridge: Bridge,
    out_dir: PathBuf,
}

impl Speech {
    pub fn new(bridge: Bridge, out_dir: PathBuf) -> Self {
        Self { bridge, out_dir }
    }

    /// Voices `cap` in `language`, an exact CAP info language.
    pub async fn render(&self, cap: &CapEntry, language: &str, timezone: &str) -> Result<Pcm> {
        let token = make_nonce_token(&format!("{}:{language}", cap.id));
        let request_id = format!("netalert-speech-{token}");
        let reply = self
            .bridge
            .request(
                &request_id,
                json!({
                    "type": "alert.speech.request",
                    "source": SOURCE,
                    "subject": request_id,
                    "data": {
                        "request_id": request_id,
                        "alert": cap.alert_with_xml(),
                        "language": language,
                        "timezone": timezone,
                    }
                }),
                SPEECH_TIMEOUT,
            )
            .await
            .context("alert speech text")?;
        let Some(text) = value_text(reply.get("text")) else {
            bail!("product render returned no alert speech text");
        };

        let job_id = format!("netalert-tts-{token}");
        tokio::fs::create_dir_all(&self.out_dir).await?;
        let output_path = self.out_dir.join(format!("{job_id}.pcm16le"));
        let reply = self
            .bridge
            .request(
                &job_id,
                json!({
                    "type": "tts.synthesize",
                    "source": SOURCE,
                    "subject": job_id,
                    "data": {
                        "job_id": job_id,
                        "voice_route_key": job_id,
                        "text": text,
                        "language": language,
                        "timezone": timezone,
                        "output_path": output_path.to_string_lossy(),
                        "output_format": "pcm_s16le",
                        "target_sample_rate": OUTPUT_SAMPLE_RATE,
                        "target_channels": 1,
                        "priority": "normal",
                        "deadline_at": (Utc::now() + TTS_TIMEOUT).to_rfc3339(),
                    }
                }),
                TTS_TIMEOUT,
            )
            .await
            .context("alert speech synthesis")?;
        let path = value_text(reply.get("output_path"))
            .map(PathBuf::from)
            .unwrap_or(output_path);
        let rate = value_text(reply.get("sample_rate"))
            .and_then(|rate| rate.parse().ok())
            .unwrap_or(OUTPUT_SAMPLE_RATE);
        let channels = value_text(reply.get("channels"))
            .and_then(|channels| channels.parse().ok())
            .unwrap_or(1);
        let pcm = tokio::task::spawn_blocking({
            let path = path.clone();
            move || load_pcm(&path, rate, channels)
        })
        .await??;
        let _ = tokio::fs::remove_file(&path).await;
        Ok(pcm)
    }
}
