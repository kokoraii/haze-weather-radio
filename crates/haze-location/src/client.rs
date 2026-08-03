//! Shared asynchronous Rust request client for the targeted broker contract.

use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use uuid::Uuid;

use crate::contract::{QueryFailure, QueryRequest, QueryResponse, API_VERSION};

pub async fn query(
    bridge_addr: &str,
    client_prefix: &str,
    mut request: QueryRequest,
    timeout: Duration,
) -> Result<QueryResponse> {
    let client_id = format!("{}-{}", client_prefix.trim(), Uuid::new_v4());
    if request.api_version == 0 {
        request.api_version = API_VERSION;
    }
    if request.request_id.trim().is_empty() {
        request.request_id = Uuid::new_v4().to_string();
    }
    let request_id = request.request_id.clone();
    tokio::time::timeout(timeout, async move {
        let stream = TcpStream::connect(bridge_addr)
            .await
            .with_context(|| format!("failed to connect to host bridge at {bridge_addr}"))?;
        let (read, mut write) = stream.into_split();
        for event in [
            json!({
                "type": "bridge.client",
                "data": {
                    "client_id": client_id.clone(),
                    "receive_events": true,
                    "subscriptions": ["location.query.completed", "location.query.failed"],
                }
            }),
            json!({
                "type": "location.query.request",
                "source": client_id.clone(),
                "reply_to": client_id,
                "target": "haze-location",
                "subject": request.request_id,
                "data": request,
            }),
        ] {
            let mut raw = serde_json::to_vec(&event)?;
            raw.push(b'\n');
            write.write_all(&raw).await?;
        }
        write.flush().await?;
        let mut lines = BufReader::new(read).lines();
        while let Some(line) = lines.next_line().await? {
            let event: Value = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if event.get("subject").and_then(Value::as_str) != Some(&request_id) {
                continue;
            }
            let data = event.get("data").cloned().unwrap_or(Value::Null);
            match event.get("type").and_then(Value::as_str) {
                Some("location.query.completed") => return Ok(serde_json::from_value(data)?),
                Some("location.query.failed") => {
                    let failure: QueryFailure = serde_json::from_value(data)?;
                    anyhow::bail!(
                        "location query failed ({}): {}",
                        failure.code,
                        failure.error
                    );
                }
                _ => {}
            }
        }
        anyhow::bail!("host bridge closed before the location reply")
    })
    .await
    .context("location query timed out")?
}
