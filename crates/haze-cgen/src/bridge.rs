use std::time::Duration;

use anyhow::Result;
use haze_bridge_client::{BridgeWriter, Options};
use serde_json::Value;
use tokio::sync::mpsc;

const MAX_BRIDGE_EVENT_BYTES: usize = 2 * 1024 * 1024;
const BRIDGE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const BRIDGE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const BRIDGE_RETRY_DELAY: Duration = Duration::from_secs(1);
const EVENT_BUFFER: usize = 512;

pub(crate) type BridgeClient = BridgeWriter;

pub(crate) struct BridgeConnection {
    pub(crate) client: BridgeClient,
    pub(crate) events: mpsc::Receiver<Value>,
}

fn bridge_options() -> Options {
    Options {
        connect_timeout: BRIDGE_CONNECT_TIMEOUT,
        write_timeout: Some(BRIDGE_WRITE_TIMEOUT),
        max_line_bytes: MAX_BRIDGE_EVENT_BYTES,
        max_publish_bytes: Some(MAX_BRIDGE_EVENT_BYTES),
    }
}

pub(crate) async fn connect(addr: &str) -> Result<BridgeConnection> {
    let (reader, client) = haze_bridge_client::connect(addr, &bridge_options(), None).await?;
    let (events, _reader_task) = haze_bridge_client::spawn_event_pump(reader, EVENT_BUFFER);
    Ok(BridgeConnection { client, events })
}

pub(crate) async fn connect_retry(addr: &str) -> Result<BridgeConnection> {
    let (reader, client) = haze_bridge_client::connect_retry(
        addr,
        &bridge_options(),
        None,
        BRIDGE_RETRY_DELAY,
        "host event bridge",
    )
    .await;
    let (events, _reader_task) = haze_bridge_client::spawn_event_pump(reader, EVENT_BUFFER);
    Ok(BridgeConnection { client, events })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn publish_refuses_events_over_the_cgen_line_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let connection = connect(&addr).await.unwrap();
        let oversized = json!({
            "type": "cgen.test",
            "data": "x".repeat(MAX_BRIDGE_EVENT_BYTES),
        });
        let err = connection.client.publish(oversized).await.unwrap_err();
        assert!(err.to_string().contains("byte limit"), "{err:#}");
    }

    #[tokio::test]
    async fn events_stream_in_order_and_publish_adds_a_timestamp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let broker = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let (read, mut write) = socket.into_split();
            write
                .write_all(b"{\"type\":\"one\"}\n\n{\"type\":\"two\"}\n")
                .await
                .unwrap();
            let mut lines = BufReader::new(read).lines();
            let published = lines.next_line().await.unwrap().unwrap();
            // Keep the write half open until the client has read both events.
            (published, write)
        });
        let mut connection = connect_retry(&addr).await.unwrap();
        assert_eq!(connection.events.recv().await.unwrap()["type"], "one");
        assert_eq!(connection.events.recv().await.unwrap()["type"], "two");
        connection
            .client
            .publish(json!({"type": "service.ready", "source": "haze-cgen"}))
            .await
            .unwrap();
        let (published, _write) = broker.await.unwrap();
        let published: Value = serde_json::from_str(&published).unwrap();
        assert_eq!(published["type"], "service.ready");
        assert!(published["timestamp"].is_string());
    }
}
