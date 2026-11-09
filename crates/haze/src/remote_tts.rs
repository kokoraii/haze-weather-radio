//! The TTS side of a remote service link.
//!
//! Requesters name a local `output_path` for every synthesis. A remote
//! haze-tts cannot write there, so it returns the audio inline as
//! `output_b64`, and this adapter writes it to the path that request named
//! before the event reaches the bridge. Only paths Haze itself requested are
//! ever written, so a remote peer cannot pick where files land.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use base64::Engine as _;
use serde_json::{json, Map, Value};

use crate::remote_service::LinkAdapter;

/// How long a request waits for its audio before its path is forgotten.
const PENDING_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_PENDING: usize = 4096;

/// Settings a local haze-tts would get as launch flags.
#[derive(Clone)]
pub(crate) struct TtsLinkSettings {
    pub readers: PathBuf,
    pub dictionary: PathBuf,
    pub out_dir: PathBuf,
    pub provider: String,
    pub language: String,
    pub timezone: String,
    pub timeout: String,
}

pub(crate) struct TtsLink {
    /// Base for relative output paths, the working directory of a local haze-tts.
    runtime_dir: PathBuf,
    settings: TtsLinkSettings,
    pending: Mutex<HashMap<String, PendingOutput>>,
}

struct PendingOutput {
    path: PathBuf,
    requested_at: Instant,
}

impl TtsLink {
    pub(crate) fn new(runtime_dir: PathBuf, settings: TtsLinkSettings) -> Self {
        Self {
            runtime_dir,
            settings,
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn resolve(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.runtime_dir.join(path)
        }
    }

    fn remember(&self, job_id: String, path: PathBuf) {
        let mut pending = self.pending.lock().expect("tts pending lock");
        let now = Instant::now();
        pending.retain(|_, output| now.duration_since(output.requested_at) < PENDING_TTL);
        if pending.len() >= MAX_PENDING {
            if let Some(oldest) = pending
                .iter()
                .min_by_key(|(_, output)| output.requested_at)
                .map(|(job, _)| job.clone())
            {
                pending.remove(&oldest);
            }
        }
        pending.insert(
            job_id,
            PendingOutput {
                path,
                requested_at: now,
            },
        );
    }

    fn take(&self, job_id: &str) -> Option<PathBuf> {
        self.pending
            .lock()
            .expect("tts pending lock")
            .remove(job_id)
            .map(|output| output.path)
    }

    /// Writes inline audio for a finished job, or explains why it cannot.
    fn materialize(&self, job_id: &str, data: &mut Map<String, Value>) -> Result<(), String> {
        let encoded = data
            .remove("output_b64")
            .ok_or("remote TTS returned no inline audio")?;
        let path = self
            .take(job_id)
            .ok_or("remote TTS answered a request this link did not send")?;
        let audio = encoded
            .as_str()
            .and_then(|encoded| {
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .ok()
            })
            .ok_or("remote TTS sent invalid inline audio")?;
        write_atomic(&path, &audio).map_err(|err| format!("cannot write {}: {err}", path.display()))
    }
}

impl LinkAdapter for TtsLink {
    fn settings(&self) -> Result<Value> {
        let readers_xml = read_optional(&self.settings.readers)
            .with_context(|| format!("cannot read {}", self.settings.readers.display()))?;
        let dictionary_json = read_optional(&self.settings.dictionary)
            .with_context(|| format!("cannot read {}", self.settings.dictionary.display()))?
            .unwrap_or_default();
        Ok(json!({
            "readers_xml": readers_xml,
            "dictionary_json": dictionary_json,
            "provider": self.settings.provider,
            "language": self.settings.language,
            "timezone": self.settings.timezone,
            "timeout": self.settings.timeout,
        }))
    }

    fn outbound(&self, event: &mut Value) {
        if event.get("type").and_then(Value::as_str) != Some("tts.synthesize") {
            return;
        }
        let Some(job_id) = job_id(event) else {
            return;
        };
        let Some(data) = event.get_mut("data").and_then(Value::as_object_mut) else {
            return;
        };
        let requested = text(data, "output_path").or_else(|| text(data, "out"));
        let requested = requested.map(PathBuf::from).unwrap_or_else(|| {
            // A local haze-tts would pick this default itself. Naming it here
            // lets the requester find the file at the path in the reply.
            let path = self
                .settings
                .out_dir
                .join(format!("{}.wav", sanitize_file_name(&job_id)));
            data.insert(
                "output_path".to_string(),
                Value::String(path.to_string_lossy().into_owned()),
            );
            path
        });
        self.remember(job_id, self.resolve(&requested));
    }

    fn inbound(&self, mut event: Value) -> Value {
        match event.get("type").and_then(Value::as_str) {
            Some("tts.synthesized") => {}
            Some("tts.failed") => {
                if let Some(job_id) = job_id(&event) {
                    self.take(&job_id);
                }
                return event;
            }
            _ => return event,
        }
        let job_id = job_id(&event).unwrap_or_default();
        let Some(data) = event.get_mut("data").and_then(Value::as_object_mut) else {
            return failed(&job_id, "remote TTS reply has no data");
        };
        match self.materialize(&job_id, data) {
            Ok(()) => event,
            Err(error) => failed(&job_id, &error),
        }
    }

    fn reset(&self) {
        self.pending.lock().expect("tts pending lock").clear();
    }
}

/// The job id the way haze-tts reads it: `data` first, then the event, for
/// `job_id`, `id`, and `subject` in turn.
fn job_id(event: &Value) -> Option<String> {
    let data = event.get("data").and_then(Value::as_object);
    let top = event.as_object();
    ["job_id", "id", "subject"].into_iter().find_map(|key| {
        data.and_then(|data| text(data, key))
            .or_else(|| top.and_then(|top| text(top, key)))
    })
}

fn text(object: &Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn failed(job_id: &str, error: &str) -> Value {
    json!({
        "type": "tts.failed",
        "source": "haze-tts",
        "subject": job_id,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "data": {"job_id": job_id, "error": error},
    })
}

fn read_optional(path: &Path) -> io::Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(raw) => Ok(Some(raw)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".remote.tmp");
    let tmp = PathBuf::from(tmp);
    fs::write(&tmp, data)?;
    fs::rename(&tmp, path).inspect_err(|_| {
        let _ = fs::remove_file(&tmp);
    })
}

/// Matches haze-tts: ASCII letters, digits, `-`, and `_` only.
fn sanitize_file_name(value: &str) -> String {
    let clean: String = value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-' || *ch == '_')
        .collect();
    if clean.is_empty() {
        "tts".to_string()
    } else {
        clean
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(dir: &Path) -> TtsLink {
        TtsLink::new(
            dir.to_path_buf(),
            TtsLinkSettings {
                readers: dir.join("readers.xml"),
                dictionary: dir.join("dictionary.json"),
                out_dir: PathBuf::from("runtime/audio/tts"),
                provider: "auto".to_string(),
                language: "en-CA".to_string(),
                timezone: "America/Regina".to_string(),
                timeout: "60s".to_string(),
            },
        )
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("haze-remote-tts-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn synthesized(job_id: &str, audio: &[u8]) -> Value {
        json!({
            "type": "tts.synthesized",
            "subject": job_id,
            "data": {
                "job_id": job_id,
                "output_b64": base64::engine::general_purpose::STANDARD.encode(audio),
            },
        })
    }

    #[test]
    fn writes_inline_audio_to_the_requested_path() {
        let dir = temp_dir("write");
        let link = link(&dir);
        let mut request = json!({
            "type": "tts.synthesize",
            "data": {"job_id": "job-1", "text": "hi", "output_path": "audio/job-1.pcm16le"},
        });
        link.outbound(&mut request);

        let event = link.inbound(synthesized("job-1", b"pcm"));
        assert_eq!(event["type"], "tts.synthesized");
        assert!(
            event["data"].get("output_b64").is_none(),
            "inline audio must not reach the bridge"
        );
        assert_eq!(fs::read(dir.join("audio/job-1.pcm16le")).unwrap(), b"pcm");
    }

    #[test]
    fn names_a_default_path_when_the_request_has_none() {
        let dir = temp_dir("default");
        let link = link(&dir);
        let mut request =
            json!({"type": "tts.synthesize", "data": {"job_id": "a/b c", "text": "hi"}});
        link.outbound(&mut request);
        assert_eq!(
            request["data"]["output_path"],
            Path::new("runtime/audio/tts")
                .join("abc.wav")
                .to_string_lossy()
                .as_ref()
        );
        link.inbound(synthesized("a/b c", b"wav"));
        assert_eq!(
            fs::read(dir.join("runtime/audio/tts/abc.wav")).unwrap(),
            b"wav"
        );
    }

    #[test]
    fn refuses_audio_for_a_job_it_never_requested() {
        let dir = temp_dir("unknown");
        let link = link(&dir);
        let event = link.inbound(synthesized("stranger", b"wav"));
        assert_eq!(event["type"], "tts.failed");
        assert_eq!(event["data"]["job_id"], "stranger");
        assert_eq!(
            fs::read_dir(&dir).unwrap().count(),
            0,
            "nothing may be written"
        );
    }

    #[test]
    fn settings_carry_the_readers_and_dictionary() {
        let dir = temp_dir("settings");
        fs::write(dir.join("readers.xml"), "<Readers/>").unwrap();
        let settings = link(&dir).settings().unwrap();
        assert_eq!(settings["readers_xml"], "<Readers/>");
        assert_eq!(
            settings["dictionary_json"], "",
            "a missing dictionary is empty, like a local launch"
        );
        assert_eq!(settings["timezone"], "America/Regina");
    }
}
