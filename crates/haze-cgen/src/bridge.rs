use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, timeout};

const MAX_BRIDGE_EVENT_BYTES: usize = 2 * 1024 * 1024;
const BRIDGE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const BRIDGE_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct BridgeClient {
    writer: Arc<Mutex<OwnedWriteHalf>>,
}

pub(crate) struct BridgeConnection {
    pub(crate) client: BridgeClient,
    pub(crate) events: mpsc::Receiver<Value>,
}

pub(crate) async fn connect(addr: &str) -> Result<BridgeConnection> {
    let stream = timeout(BRIDGE_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .with_context(|| format!("timed out connecting to host bridge at {addr}"))?
        .with_context(|| format!("failed to connect to host bridge at {addr}"))?;
    let (reader, writer) = stream.into_split();
    let (tx, rx) = mpsc::channel(512);
    tokio::spawn(read_loop(reader, tx));
    Ok(BridgeConnection {
        client: BridgeClient {
            writer: Arc::new(Mutex::new(writer)),
        },
        events: rx,
    })
}

pub(crate) async fn connect_retry(addr: &str) -> Result<BridgeConnection> {
    loop {
        match connect(addr).await {
            Ok(connection) => return Ok(connection),
            Err(err) => {
                tracing::warn!("waiting for host event bridge at {addr}: {err}");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

impl BridgeClient {
    pub(crate) async fn publish(&self, mut value: Value) -> Result<()> {
        if value.get("timestamp").is_none() {
            value["timestamp"] = serde_json::json!(chrono::Utc::now().to_rfc3339());
        }
        let mut raw = serde_json::to_vec(&value)?;
        if raw.len() > MAX_BRIDGE_EVENT_BYTES {
            bail!(
                "host bridge event exceeds the {} byte limit",
                MAX_BRIDGE_EVENT_BYTES
            );
        }
        raw.push(b'\n');
        let mut writer = self.writer.lock().await;
        timeout(BRIDGE_WRITE_TIMEOUT, writer.write_all(&raw))
            .await
            .context("timed out writing host bridge event")?
            .context("failed to write host bridge event")
    }
}

async fn read_loop(reader: OwnedReadHalf, tx: mpsc::Sender<Value>) {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::with_capacity(32 * 1024);
    loop {
        match read_bounded_line(&mut reader, &mut line).await {
            Ok(ReadLine::Eof) => return,
            Ok(ReadLine::Oversized) => {
                tracing::warn!(
                    limit_bytes = MAX_BRIDGE_EVENT_BYTES,
                    "ignored oversized host bridge event"
                );
            }
            Ok(ReadLine::Data) => {
                let trimmed = trim_ascii_whitespace(&line);
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_slice::<Value>(trimmed) {
                    Ok(value) => {
                        if tx.send(value).await.is_err() {
                            return;
                        }
                    }
                    Err(err) => tracing::warn!("ignored malformed bridge event: {err}"),
                }
            }
            Err(err) => {
                tracing::warn!("host bridge read failed: {err}");
                return;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadLine {
    Data,
    Oversized,
    Eof,
}

async fn read_bounded_line<R>(reader: &mut R, line: &mut Vec<u8>) -> io::Result<ReadLine>
where
    R: AsyncBufRead + Unpin,
{
    line.clear();
    let mut oversized = false;
    loop {
        let (take, found_newline, eof) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                (0, false, true)
            } else if let Some(index) = available.iter().position(|byte| *byte == b'\n') {
                (index + 1, true, false)
            } else {
                (available.len(), false, false)
            }
        };
        if eof {
            return if line.is_empty() && !oversized {
                Ok(ReadLine::Eof)
            } else if oversized {
                Ok(ReadLine::Oversized)
            } else {
                Ok(ReadLine::Data)
            };
        }
        if !oversized {
            let available = reader.fill_buf().await?;
            if line.len().saturating_add(take) > MAX_BRIDGE_EVENT_BYTES {
                oversized = true;
                line.clear();
            } else {
                line.extend_from_slice(&available[..take]);
            }
        }
        reader.consume(take);
        if found_newline {
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return if oversized {
                Ok(ReadLine::Oversized)
            } else {
                Ok(ReadLine::Data)
            };
        }
    }
}

fn trim_ascii_whitespace(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

#[cfg(test)]
mod tests {
    use super::{read_bounded_line, trim_ascii_whitespace, ReadLine, MAX_BRIDGE_EVENT_BYTES};
    use tokio::io::BufReader;

    #[tokio::test]
    async fn oversized_bridge_line_is_discarded_and_reader_recovers() {
        let mut input = vec![b'x'; MAX_BRIDGE_EVENT_BYTES + 1];
        input.extend_from_slice(b"\n{\"type\":\"service.ready\"}\n");
        let mut reader = BufReader::new(input.as_slice());
        let mut line = Vec::new();

        assert_eq!(
            read_bounded_line(&mut reader, &mut line).await.unwrap(),
            ReadLine::Oversized
        );
        assert!(line.is_empty());
        assert_eq!(
            read_bounded_line(&mut reader, &mut line).await.unwrap(),
            ReadLine::Data
        );
        assert_eq!(line, br#"{"type":"service.ready"}"#);
    }

    #[tokio::test]
    async fn bridge_reader_accepts_a_final_line_without_newline() {
        let mut reader = BufReader::new(br#"{"type":"service.ready"}"#.as_slice());
        let mut line = Vec::new();

        assert_eq!(
            read_bounded_line(&mut reader, &mut line).await.unwrap(),
            ReadLine::Data
        );
        assert_eq!(line, br#"{"type":"service.ready"}"#);
        assert_eq!(
            read_bounded_line(&mut reader, &mut line).await.unwrap(),
            ReadLine::Eof
        );
    }

    #[test]
    fn bridge_event_whitespace_is_trimmed_without_utf8_allocation() {
        assert_eq!(
            trim_ascii_whitespace(b" \r\n{\"type\":\"event\"}\t"),
            br#"{"type":"event"}"#
        );
    }
}
