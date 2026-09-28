use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};
use tokio::time::sleep;
use tracing::{error, warn};

const JOURNAL_DIR: &str = "runtime/state/cap-ingest-journal";
const ROUTER_ACK_EVENT: &str = "cap.alert.router.acknowledged";

#[derive(Debug, Deserialize, Serialize)]
pub struct EventEnvelope {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    pub data: Value,
}

impl EventEnvelope {
    pub fn cap_alert(source: &str, alert: Value, ingest: Value) -> Self {
        let parent_id = alert
            .get("identifier")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let raw_xml = alert
            .get("raw_xml")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let fallback_document = (!raw_xml.trim().is_empty())
            .then(|| raw_xml.as_bytes().to_vec())
            .unwrap_or_else(|| serde_json::to_vec(&alert).expect("CAP alert serializes"));
        let mut hasher = Sha256::new();
        hasher.update(source.as_bytes());
        hasher.update(b"\0");
        hasher.update(fallback_document);
        let digest = hasher.finalize();
        let delivery_id = format!("cap-{:x}", digest);
        Self {
            id: Some(delivery_id.clone()),
            event_type: "cap.alert.received".to_string(),
            source: source.to_string(),
            subject: Some(parent_id),
            delivery_id: Some(delivery_id.clone()),
            timestamp: Some(chrono::Utc::now().to_rfc3339()),
            data: json!({
                "delivery_id": delivery_id,
                "alert": alert,
                "ingest": ingest,
            }),
        }
    }

    pub fn status(source: &str, subject: &str, data: Value) -> Self {
        Self {
            id: None,
            event_type: "cap.ingest.status".to_string(),
            source: source.to_string(),
            subject: Some(subject.to_string()),
            delivery_id: None,
            timestamp: Some(chrono::Utc::now().to_rfc3339()),
            data,
        }
    }
}

#[derive(Clone)]
pub struct EventPublisher {
    addr: Option<String>,
    client_id: String,
    writer: Arc<Mutex<Option<OwnedWriteHalf>>>,
    pending_acks:
        Arc<Mutex<std::collections::HashMap<String, Vec<oneshot::Sender<Result<(), String>>>>>>,
    journal_lock: Arc<Mutex<()>>,
    journal_dir: PathBuf,
}

impl EventPublisher {
    pub fn new(addr: Option<String>, source_id: impl Into<String>) -> Self {
        Self {
            addr: addr.and_then(|value| {
                let value = value.trim().to_string();
                (!value.is_empty()).then_some(value)
            }),
            client_id: format!("haze-cap-ingest:{}", source_id.into()),
            writer: Arc::new(Mutex::new(None)),
            pending_acks: Arc::new(Mutex::new(std::collections::HashMap::new())),
            journal_lock: Arc::new(Mutex::new(())),
            journal_dir: PathBuf::from(JOURNAL_DIR),
        }
    }

    pub async fn publish(&self, event: &EventEnvelope) -> Result<()> {
        let value = serde_json::to_value(event).context("failed to serialize event")?;
        if event.event_type == "cap.alert.received" {
            return self.publish_durable_alert(value).await;
        }
        let line = serde_json::to_vec(&value).context("failed to serialize event")?;
        if self.addr.is_none() {
            println!("{}", String::from_utf8_lossy(&line));
            return Ok(());
        }

        self.write_once(&line).await
    }

    async fn publish_durable_alert(&self, event: Value) -> Result<()> {
        let delivery_id = event
            .get("delivery_id")
            .and_then(Value::as_str)
            .or_else(|| event.get("data")?.get("delivery_id")?.as_str())
            .context("CAP alert is missing delivery_id")?
            .to_string();
        let line = serde_json::to_vec(&event).context("failed to serialize CAP alert")?;
        if self.addr.is_none() {
            println!("{}", String::from_utf8_lossy(&line));
            return Ok(());
        }

        let journal_path = self.journal_path(&delivery_id);
        {
            let _guard = self.journal_lock.lock().await;
            persist_journal(&journal_path, &line).await?;
        }

        loop {
            let (ack_tx, ack_rx) = oneshot::channel();
            self.pending_acks
                .lock()
                .await
                .entry(delivery_id.clone())
                .or_default()
                .push(ack_tx);
            let mut ack_rx = Box::pin(ack_rx);
            loop {
                match self.write_once(&line).await {
                    Ok(()) => {}
                    Err(err) => warn!(%delivery_id, "CAP publish failed, retrying: {err:#}"),
                }
                tokio::select! {
                    ack = &mut ack_rx => {
                        match ack {
                            Ok(Ok(())) => {
                                tokio::fs::remove_file(&journal_path)
                                    .await
                                    .with_context(|| format!("failed to remove acknowledged CAP journal {}", journal_path.display()))?;
                                return Ok(());
                            }
                            Ok(Err(err)) => warn!(%delivery_id, "CAP router acknowledgement requested retry: {err}"),
                            Err(_) => warn!(%delivery_id, "CAP router acknowledgement channel closed, retrying"),
                        }
                        break;
                    }
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
            self.pending_acks.lock().await.remove(&delivery_id);
        }
    }

    /// Replays CAP documents left in the durable journal before polling sources.
    pub async fn replay_pending(&self) -> Result<usize> {
        let mut entries = match tokio::fs::read_dir(&self.journal_dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(err) => return Err(err).context("failed to read CAP ingest journal"),
        };
        let mut pending = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                pending.push(entry.path());
            }
        }
        pending.sort();
        let mut replayed = 0;
        for path in pending {
            let raw = tokio::fs::read(&path)
                .await
                .with_context(|| format!("failed to read CAP journal {}", path.display()))?;
            let event: Value = serde_json::from_slice(&raw)
                .with_context(|| format!("failed to parse CAP journal {}", path.display()))?;
            self.publish_durable_alert(event).await?;
            replayed += 1;
        }
        Ok(replayed)
    }

    fn journal_path(&self, delivery_id: &str) -> PathBuf {
        let name = delivery_id.strip_prefix("cap-").unwrap_or(delivery_id);
        self.journal_dir.join(format!("{name}.json"))
    }

    async fn write_once(&self, line: &[u8]) -> Result<()> {
        let mut guard = self.writer.lock().await;
        if guard.is_none() {
            let addr = self.addr.as_ref().expect("bridge addr checked");
            let stream = TcpStream::connect(addr)
                .await
                .with_context(|| format!("failed to connect to host bridge at {addr}"))?;
            let (reader, mut writer) = stream.into_split();
            let registration = publisher_registration(&self.client_id);
            writer
                .write_all(&registration)
                .await
                .context("failed to register CAP publisher with host bridge")?;
            writer
                .write_all(b"\n")
                .await
                .context("failed to terminate CAP publisher bridge registration")?;
            writer
                .flush()
                .await
                .context("failed to flush CAP publisher bridge registration")?;
            tokio::spawn(read_acknowledgements(
                reader,
                Arc::clone(&self.pending_acks),
            ));
            *guard = Some(writer);
        }
        let writer = guard.as_mut().expect("bridge writer exists");
        let write_result = async {
            writer.write_all(line).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await
        }
        .await;
        if let Err(err) = write_result {
            *guard = None;
            return Err(err).context("failed to write host bridge event");
        }
        Ok(())
    }
}

async fn persist_journal(path: &Path, line: &[u8]) -> Result<()> {
    let parent = path.parent().expect("CAP journal path has a parent");
    tokio::fs::create_dir_all(parent).await.with_context(|| {
        format!(
            "failed to create CAP journal directory {}",
            parent.display()
        )
    })?;
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let temporary = path.with_extension("json.tmp");
    let mut file = tokio::fs::File::create(&temporary)
        .await
        .with_context(|| format!("failed to create CAP journal {}", temporary.display()))?;
    file.write_all(line).await?;
    file.sync_all().await?;
    tokio::fs::rename(&temporary, path)
        .await
        .with_context(|| format!("failed to commit CAP journal {}", path.display()))
}

async fn read_acknowledgements(
    reader: OwnedReadHalf,
    pending: Arc<
        Mutex<std::collections::HashMap<String, Vec<oneshot::Sender<Result<(), String>>>>>,
    >,
) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) != Some(ROUTER_ACK_EVENT) {
            continue;
        }
        let data = value.get("data").unwrap_or(&Value::Null);
        let delivery_id = data
            .get("delivery_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if delivery_id.is_empty() {
            continue;
        }
        let status = data
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("committed");
        let result = if status == "committed" || status == "duplicate" {
            Ok(())
        } else {
            Err(data
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or(status)
                .to_string())
        };
        if let Some(waiters) = pending.lock().await.remove(delivery_id) {
            for waiter in waiters {
                let _ = waiter.send(result.clone());
            }
        }
    }
    error!("CAP ingest broker acknowledgement reader disconnected");
}

fn publisher_registration(client_id: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "type": "bridge.client",
        "source": "haze-cap-ingest",
        "data": {
            "client_id": client_id,
            "receive_events": true,
            "subscriptions": [ROUTER_ACK_EVENT],
        }
    }))
    .expect("CAP bridge registration serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_publisher_registers_as_publish_only() {
        let registration: Value =
            serde_json::from_slice(&publisher_registration("haze-cap-ingest:test"))
                .expect("registration JSON");
        assert_eq!(registration["type"], "bridge.client");
        assert_eq!(registration["data"]["client_id"], "haze-cap-ingest:test");
        assert_eq!(registration["data"]["receive_events"], true);
        assert_eq!(
            registration["data"]["subscriptions"],
            json!([ROUTER_ACK_EVENT])
        );
    }

    #[test]
    fn cap_delivery_id_is_stable_and_parent_id_remains_subject() {
        let alert = json!({"identifier":"parent-1", "raw_xml":"<alert/>"});
        let first = EventEnvelope::cap_alert("naads", alert.clone(), json!({}));
        let second = EventEnvelope::cap_alert("naads", alert, json!({}));
        assert_eq!(first.delivery_id, second.delivery_id);
        assert_eq!(first.subject.as_deref(), Some("parent-1"));
        assert_eq!(first.data["delivery_id"], first.delivery_id.unwrap());
    }

    #[test]
    fn cap_delivery_id_uses_parsed_document_when_raw_xml_is_unavailable() {
        let first = EventEnvelope::cap_alert("naads", json!({"identifier":"alert-1"}), json!({}));
        let second = EventEnvelope::cap_alert("naads", json!({"identifier":"alert-2"}), json!({}));

        assert_ne!(first.delivery_id, second.delivery_id);
    }
}
