//! Host bridge access: one shared writer plus request/reply correlation for
//! the events netalert waits on (alert speech text and TTS synthesis).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use haze_bridge_client::BridgeWriter;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::util::value_text;

pub const SOURCE: &str = "haze-netalert";
pub const CLIENT_ID: &str = "haze-netalert";

/// Every event type the dispatcher acts on.
pub const SUBSCRIPTIONS: &[&str] = &[
    "cap.alert.received",
    "cap.alert.audio.ready",
    "cap.alert.cancelled",
    "alert.playout.started",
    "alert.playout.completed",
    "playout.interrupted",
    "alert.speech.completed",
    "alert.speech.failed",
    "tts.synthesized",
    "tts.failed",
    "system.shutdown",
];

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;

#[derive(Clone)]
pub struct Bridge {
    writer: BridgeWriter,
    pending: Pending,
}

impl Bridge {
    pub fn new(writer: BridgeWriter) -> Self {
        Self {
            writer,
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn publish(&self, event: Value) -> Result<()> {
        self.writer.publish(event).await
    }

    /// Publishes `event` and waits up to `wait` for the reply carrying `id`
    /// (see [`reply_id`]). Returns the reply's `data` object.
    pub async fn request(&self, id: &str, event: Value, wait: Duration) -> Result<Value> {
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("bridge pending map poisoned")
            .insert(id.to_string(), tx);
        let result = async {
            self.writer.publish(event).await?;
            match tokio::time::timeout(wait, rx).await {
                Ok(Ok(Ok(data))) => Ok(data),
                Ok(Ok(Err(detail))) => bail!("{detail}"),
                Ok(Err(_)) => bail!("bridge closed while waiting for {id}"),
                Err(_) => Err(anyhow!("timed out waiting for {id}")),
            }
        }
        .await;
        self.pending
            .lock()
            .expect("bridge pending map poisoned")
            .remove(id);
        result
    }

    /// Hands a reply event to its waiting request. Returns true when the event
    /// was a reply type, whether or not anything was still waiting for it.
    pub fn resolve(&self, event: &Value) -> bool {
        let Some((id, outcome)) = reply_id(event) else {
            return false;
        };
        let waiter = self
            .pending
            .lock()
            .expect("bridge pending map poisoned")
            .remove(&id);
        if let Some(waiter) = waiter {
            let _ = waiter.send(outcome);
        }
        true
    }
}

/// The correlation ID and outcome of a reply event, or `None` for other events.
fn reply_id(event: &Value) -> Option<(String, Result<Value, String>)> {
    let kind = event.get("type")?.as_str()?;
    let data = event.get("data").cloned().unwrap_or(Value::Null);
    let (key, ok) = match kind {
        "alert.speech.completed" => ("request_id", true),
        "alert.speech.failed" => ("request_id", false),
        "tts.synthesized" => ("job_id", true),
        "tts.failed" => ("job_id", false),
        _ => return None,
    };
    let id = value_text(data.get(key)).or_else(|| value_text(event.get("subject")))?;
    let outcome = if ok {
        Ok(data)
    } else {
        Err(value_text(data.get("error")).unwrap_or_else(|| format!("{kind} without detail")))
    };
    Some((id, outcome))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reply_ids_follow_each_reply_contract() {
        let (id, outcome) = reply_id(&json!({
            "type": "tts.synthesized", "data": {"job_id": "job-1", "output_path": "a.pcm"}
        }))
        .unwrap();
        assert_eq!(id, "job-1");
        assert_eq!(outcome.unwrap()["output_path"], "a.pcm");

        let (id, outcome) = reply_id(&json!({
            "type": "alert.speech.failed", "data": {"request_id": "speech-1", "error": "no info"}
        }))
        .unwrap();
        assert_eq!(id, "speech-1");
        assert_eq!(outcome.unwrap_err(), "no info");

        assert!(reply_id(&json!({"type": "cap.alert.received"})).is_none());
    }
}
