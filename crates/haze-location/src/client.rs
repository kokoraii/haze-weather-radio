//! Shared asynchronous Rust request client for the targeted broker contract.

use std::time::Duration;

use anyhow::{Context, Result};
use haze_bridge_client::{Options, Registration};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::contract::{QueryFailure, QueryRequest, QueryResponse, API_VERSION};

/// Query replies can carry whole grouped location sets.
const MAX_BRIDGE_LINE_BYTES: usize = 16 * 1024 * 1024;

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
        let registration = Registration::subscriber(
            client_id.clone(),
            &["location.query.completed", "location.query.failed"],
        );
        let options = Options {
            max_line_bytes: MAX_BRIDGE_LINE_BYTES,
            ..Options::default()
        };
        let (mut reader, writer) =
            haze_bridge_client::connect(bridge_addr, &options, Some(&registration)).await?;
        writer
            .send(&json!({
                "type": "location.query.request",
                "source": client_id.clone(),
                "reply_to": client_id,
                "target": "haze-location",
                "subject": request.request_id,
                "data": request,
            }))
            .await?;
        while let Some(event) = reader.next_event().await? {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    fn fixture_request() -> QueryRequest {
        serde_json::from_str(include_str!("../../../contracts/location/v1/query.json"))
            .expect("query fixture")
    }

    #[tokio::test]
    async fn query_registers_targets_the_location_service_and_returns_the_reply() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake broker");
        let addr = listener.local_addr().expect("broker address").to_string();
        let broker = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let (read, mut write) = socket.into_split();
            let mut lines = BufReader::new(read).lines();
            let registration: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            let request: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            let reply = json!({
                "type": "location.query.completed",
                "target": request["reply_to"],
                "subject": request["subject"],
                "data": serde_json::from_str::<Value>(include_str!(
                    "../../../contracts/location/v1/response.json"
                ))
                .unwrap(),
            });
            let mut raw = serde_json::to_vec(&reply).unwrap();
            raw.push(b'\n');
            write.write_all(&raw).await.unwrap();
            (registration, request)
        });

        let response = query(
            &addr,
            "test-client",
            fixture_request(),
            Duration::from_secs(5),
        )
        .await
        .expect("query succeeds");
        assert_eq!(response.results.len(), 1);

        let (registration, request) = broker.await.expect("broker task");
        assert_eq!(registration["type"], "bridge.client");
        assert_eq!(registration["data"]["receive_events"], true);
        assert_eq!(
            registration["data"]["subscriptions"],
            json!(["location.query.completed", "location.query.failed"])
        );
        let client_id = registration["data"]["client_id"].as_str().unwrap();
        assert!(client_id.starts_with("test-client-"));
        assert_eq!(request["type"], "location.query.request");
        assert_eq!(request["target"], "haze-location");
        assert_eq!(request["reply_to"], client_id);
        assert_eq!(request["subject"], "contract-query-001");
    }

    #[tokio::test]
    async fn query_reports_a_failure_reply_with_its_code() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake broker");
        let addr = listener.local_addr().expect("broker address").to_string();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let (read, mut write) = socket.into_split();
            let mut lines = BufReader::new(read).lines();
            let _registration = lines.next_line().await.unwrap().unwrap();
            let request: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            let reply = json!({
                "type": "location.query.failed",
                "subject": request["subject"],
                "data": {
                    "api_version": 1, "request_id": request["subject"],
                    "code": "busy", "error": "catalog is busy", "retryable": true,
                },
            });
            let mut raw = serde_json::to_vec(&reply).unwrap();
            raw.push(b'\n');
            write.write_all(&raw).await.unwrap();
        });

        let err = query(
            &addr,
            "test-client",
            fixture_request(),
            Duration::from_secs(5),
        )
        .await
        .expect_err("failure reply is an error");
        assert!(
            err.to_string().contains("location query failed (busy)"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn query_fails_when_the_bridge_closes_before_replying() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake broker");
        let addr = listener.local_addr().expect("broker address").to_string();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            // Read the registration and the request first, so the close is a
            // clean end of stream and not a reset caused by unread data.
            let mut lines = BufReader::new(socket).lines();
            let _registration = lines.next_line().await.unwrap().unwrap();
            let _request = lines.next_line().await.unwrap().unwrap();
        });
        let err = query(
            &addr,
            "test-client",
            fixture_request(),
            Duration::from_secs(5),
        )
        .await
        .expect_err("closed bridge is an error");
        assert!(
            err.to_string().contains("closed before the location reply"),
            "{err:#}"
        );
    }
}
