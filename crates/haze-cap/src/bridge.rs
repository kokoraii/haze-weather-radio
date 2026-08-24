use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::warn;

#[derive(Debug, Serialize)]
pub struct EventEnvelope {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub data: Value,
}

impl EventEnvelope {
    pub fn cap_alert(source: &str, alert: Value, ingest: Value) -> Self {
        let id = alert
            .get("identifier")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        Self {
            id: Some(id.clone()),
            event_type: "cap.alert.received".to_string(),
            source: source.to_string(),
            subject: Some(id),
            data: json!({
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
            data,
        }
    }
}

pub struct EventPublisher {
    addr: Option<String>,
    writer: Mutex<Option<BufWriter<TcpStream>>>,
}

impl EventPublisher {
    pub fn new(addr: Option<String>) -> Self {
        Self {
            addr: addr.and_then(|value| {
                let value = value.trim().to_string();
                (!value.is_empty()).then_some(value)
            }),
            writer: Mutex::new(None),
        }
    }

    pub async fn publish(&self, event: &EventEnvelope) -> Result<()> {
        let line = serde_json::to_vec(event).context("failed to serialize event")?;
        if self.addr.is_none() {
            println!("{}", String::from_utf8_lossy(&line));
            return Ok(());
        }

        if let Err(err) = self.write_once(&line).await {
            warn!("host bridge publish failed, reconnecting: {err:#}");
            *self.writer.lock().await = None;
            self.write_once(&line).await?;
        }
        Ok(())
    }

    async fn write_once(&self, line: &[u8]) -> Result<()> {
        let mut guard = self.writer.lock().await;
        if guard.is_none() {
            let addr = self.addr.as_ref().expect("bridge addr checked");
            let stream = TcpStream::connect(addr)
                .await
                .with_context(|| format!("failed to connect to host bridge at {addr}"))?;
            let mut writer = BufWriter::new(stream);
            let registration = publisher_registration();
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
            *guard = Some(writer);
        }
        let writer = guard.as_mut().expect("bridge writer exists");
        writer.write_all(line).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(())
    }
}

fn publisher_registration() -> Vec<u8> {
    br#"{"type":"bridge.client","source":"haze-cap-ingest","data":{"client_id":"haze-cap-ingest","receive_events":false,"subscriptions":[]}}"#.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap_publisher_registers_as_publish_only() {
        let registration: Value =
            serde_json::from_slice(&publisher_registration()).expect("registration JSON");
        assert_eq!(registration["type"], "bridge.client");
        assert_eq!(registration["data"]["client_id"], "haze-cap-ingest");
        assert_eq!(registration["data"]["receive_events"], false);
    }
}
