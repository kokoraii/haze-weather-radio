//! Shared Rust client for the Haze host event bridge.
//!
//! The bridge speaks newline-delimited JSON over loopback TCP. Each service
//! used to carry its own copy of the connect, registration, framing, and read
//! loop code. This crate owns the wire handling so a later transport (see
//! `docs/grpc-migration-plan.md`) can be selected in one place. It keeps the
//! existing wire behavior: callers keep their own filtering, request
//! correlation, and retry policy and only hand the transport details here.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use haze_media_protocol::{pcm_frame_len_from_prefix, PCM_FRAME_MAGIC, PCM_FRAME_PREFIX_LEN};
use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

/// Largest single inbound line accepted unless the caller picks another limit.
pub const DEFAULT_MAX_LINE_BYTES: usize = 4 * 1024 * 1024;
/// Capacity of the channel fed by [`spawn_event_pump`] unless overridden.
pub const DEFAULT_EVENT_BUFFER: usize = 256;
/// Bound on the TCP connect unless overridden.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Delay between attempts in [`connect_retry`].
pub const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// The optional `bridge.client` handshake.
///
/// A connection that never registers is treated by the broker as a legacy
/// client: it receives every event after a short grace period or after its
/// first publish.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Registration {
    source: Option<String>,
    client_id: Option<String>,
    receive_events: bool,
    subscriptions: Vec<String>,
    binary_pcm: bool,
}

impl Registration {
    /// A client that publishes and is sent no events.
    pub fn publish_only() -> Self {
        Self::default()
    }

    /// A client named `client_id` that receives events matching `subscriptions`.
    ///
    /// An entry is an exact type, `*` for everything, or a prefix ending in
    /// `*`. An empty list receives every event.
    pub fn subscriber(client_id: impl Into<String>, subscriptions: &[&str]) -> Self {
        Self {
            client_id: Some(client_id.into()),
            receive_events: true,
            subscriptions: subscriptions
                .iter()
                .map(|value| value.to_string())
                .collect(),
            ..Self::default()
        }
    }

    /// A client that receives events but has no identity for targeted delivery.
    pub fn anonymous_subscriber(subscriptions: &[&str]) -> Self {
        Self {
            receive_events: true,
            subscriptions: subscriptions
                .iter()
                .map(|value| value.to_string())
                .collect(),
            ..Self::default()
        }
    }

    /// Sets the `source` member on the handshake event.
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Opts in to compact binary PCM frames on the media bridge.
    pub fn with_binary_pcm(mut self) -> Self {
        self.binary_pcm = true;
        self
    }

    /// The handshake event exactly as it is written to the bridge.
    pub fn to_value(&self) -> Value {
        let mut data = json!({
            "receive_events": self.receive_events,
            "subscriptions": self.subscriptions,
        });
        if let Some(object) = data.as_object_mut() {
            if let Some(client_id) = &self.client_id {
                object.insert("client_id".to_string(), json!(client_id));
            }
            if self.binary_pcm {
                object.insert("binary_pcm".to_string(), json!(true));
            }
        }
        let mut event = json!({ "type": "bridge.client", "data": data });
        if let Some(source) = &self.source {
            event["source"] = json!(source);
        }
        event
    }
}

/// Connection settings shared by every bridge client.
#[derive(Debug, Clone)]
pub struct Options {
    /// Bound on the TCP connect.
    pub connect_timeout: Duration,
    /// Bound on each write. `None` leaves writes unbounded.
    pub write_timeout: Option<Duration>,
    /// Largest inbound line. A longer line is discarded and reading continues.
    pub max_line_bytes: usize,
    /// When set, [`BridgeWriter::publish`] and [`BridgeWriter::send`] refuse
    /// events whose encoded form is longer than this.
    pub max_publish_bytes: Option<usize>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            write_timeout: None,
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            max_publish_bytes: None,
        }
    }
}

/// The write half of a bridge connection. Cloning shares the connection, and
/// writes are serialized so concurrent publishers never interleave lines.
#[derive(Clone)]
pub struct BridgeWriter {
    inner: Arc<Mutex<OwnedWriteHalf>>,
    write_timeout: Option<Duration>,
    max_publish_bytes: Option<usize>,
}

impl BridgeWriter {
    fn new(writer: OwnedWriteHalf, options: &Options) -> Self {
        Self {
            inner: Arc::new(Mutex::new(writer)),
            write_timeout: options.write_timeout,
            max_publish_bytes: options.max_publish_bytes,
        }
    }

    /// Sends the `bridge.client` handshake.
    pub async fn register(&self, registration: &Registration) -> Result<()> {
        self.send(&registration.to_value()).await
    }

    /// Stamps a missing `timestamp` with the current time and sends the event.
    pub async fn publish(&self, mut value: Value) -> Result<()> {
        if value.get("timestamp").is_none() {
            value["timestamp"] = json!(chrono::Utc::now().to_rfc3339());
        }
        self.send(&value).await
    }

    /// Sends the event exactly as given.
    pub async fn send(&self, value: &Value) -> Result<()> {
        let mut raw = serde_json::to_vec(value).context("failed to serialize host bridge event")?;
        if let Some(limit) = self.max_publish_bytes {
            if raw.len() > limit {
                bail!("host bridge event exceeds the {limit} byte limit");
            }
        }
        raw.push(b'\n');
        self.write_raw(&raw).await
    }

    /// Writes a complete, newline-terminated payload prepared by a hot-path
    /// producer.
    pub async fn write_raw(&self, payload: &[u8]) -> Result<()> {
        let mut writer = self.inner.lock().await;
        let write = async {
            writer.write_all(payload).await?;
            writer.flush().await
        };
        let result = match self.write_timeout {
            Some(limit) => timeout(limit, write)
                .await
                .context("timed out writing host bridge event")?,
            None => write.await,
        };
        result.context("failed to write host bridge event")
    }
}

/// What [`read_bounded_line`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadLine {
    /// A line is in the buffer, without its line terminator.
    Data,
    /// A line longer than the limit was discarded.
    Oversized,
    /// The stream ended.
    Eof,
}

/// Reads one line into `line`, discarding it if it exceeds `max_bytes`.
///
/// An oversized line is consumed to its newline so the next call resumes at the
/// following event. A final line without a trailing newline is returned as data.
pub async fn read_bounded_line<R>(
    reader: &mut R,
    line: &mut Vec<u8>,
    max_bytes: usize,
) -> io::Result<ReadLine>
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
            return Ok(if oversized {
                ReadLine::Oversized
            } else if line.is_empty() {
                ReadLine::Eof
            } else {
                ReadLine::Data
            });
        }
        if !oversized {
            let available = reader.fill_buf().await?;
            if line.len().saturating_add(take) > max_bytes {
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
            return Ok(if oversized {
                ReadLine::Oversized
            } else {
                ReadLine::Data
            });
        }
    }
}

/// What [`BridgeReader::next_payload`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Payload {
    /// A JSON line is available from [`BridgeReader::payload`].
    Line,
    /// A compact PCM frame is available from [`BridgeReader::payload`].
    Pcm,
    /// A JSON line longer than the limit was discarded.
    Oversized,
    /// The stream ended.
    Eof,
}

/// Trims ASCII whitespace from both ends without allocating.
pub fn trim_ascii_whitespace(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

/// Decodes one inbound line into a JSON value. Blank lines yield `None`.
pub fn decode_event(line: &[u8]) -> Option<std::result::Result<Value, serde_json::Error>> {
    let trimmed = trim_ascii_whitespace(line);
    if trimmed.is_empty() {
        return None;
    }
    Some(serde_json::from_slice(trimmed))
}

/// The read half of a bridge connection.
pub struct BridgeReader<R = OwnedReadHalf> {
    reader: BufReader<R>,
    line: Vec<u8>,
    max_line_bytes: usize,
}

impl<R: AsyncRead + Unpin> BridgeReader<R> {
    /// Wraps any async reader, which makes the framing testable without a socket.
    pub fn new(reader: R, max_line_bytes: usize) -> Self {
        Self {
            reader: BufReader::new(reader),
            line: Vec::with_capacity(32 * 1024),
            max_line_bytes,
        }
    }

    /// Reads the next raw line into the internal buffer, for callers that decode
    /// events themselves. Read it back with [`BridgeReader::line`].
    pub async fn next_line(&mut self) -> io::Result<ReadLine> {
        read_bounded_line(&mut self.reader, &mut self.line, self.max_line_bytes).await
    }

    /// The line most recently read by [`BridgeReader::next_line`], without its
    /// line terminator. It is only meaningful after a [`ReadLine::Data`] result.
    pub fn line(&self) -> &[u8] {
        &self.line
    }

    /// Reads either a JSON line or one compact PCM frame, for a client that
    /// registered with [`Registration::with_binary_pcm`] on the media bridge.
    ///
    /// A JSON line is bounded by the reader's line limit and a PCM frame by the
    /// protocol's frame limit. Read the bytes back with [`BridgeReader::payload`].
    /// A PCM frame with an invalid prefix, or one cut short by the end of the
    /// stream, is an error because the stream cannot be resynchronized.
    pub async fn next_payload(&mut self) -> io::Result<Payload> {
        let first = match self.reader.fill_buf().await?.first() {
            Some(byte) => *byte,
            None => return Ok(Payload::Eof),
        };
        if first != PCM_FRAME_MAGIC[0] {
            return Ok(
                match read_bounded_line(&mut self.reader, &mut self.line, self.max_line_bytes)
                    .await?
                {
                    ReadLine::Data => Payload::Line,
                    ReadLine::Oversized => Payload::Oversized,
                    ReadLine::Eof => Payload::Eof,
                },
            );
        }
        self.line.clear();
        self.line.resize(PCM_FRAME_PREFIX_LEN, 0);
        self.reader.read_exact(&mut self.line).await?;
        let length = pcm_frame_len_from_prefix(&self.line)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        self.line.resize(length, 0);
        self.reader
            .read_exact(&mut self.line[PCM_FRAME_PREFIX_LEN..])
            .await?;
        Ok(Payload::Pcm)
    }

    /// The bytes read by [`BridgeReader::next_payload`]: a JSON line without its
    /// terminator, or a whole PCM frame. Empty after [`Payload::Oversized`].
    pub fn payload(&self) -> &[u8] {
        &self.line
    }

    /// The next JSON event. Blank lines, oversized lines, and malformed JSON are
    /// skipped with a log line. `None` means the stream ended.
    pub async fn next_event(&mut self) -> io::Result<Option<Value>> {
        loop {
            match read_bounded_line(&mut self.reader, &mut self.line, self.max_line_bytes).await? {
                ReadLine::Eof => return Ok(None),
                ReadLine::Oversized => tracing::warn!(
                    limit_bytes = self.max_line_bytes,
                    "ignored oversized host bridge event"
                ),
                ReadLine::Data => match decode_event(&self.line) {
                    None => {}
                    Some(Ok(value)) => return Ok(Some(value)),
                    Some(Err(err)) => tracing::warn!("ignored malformed bridge event: {err}"),
                },
            }
        }
    }
}

/// Connects to the bridge and optionally registers the client.
pub async fn connect(
    addr: &str,
    options: &Options,
    registration: Option<&Registration>,
) -> Result<(BridgeReader, BridgeWriter)> {
    let stream = timeout(options.connect_timeout, TcpStream::connect(addr))
        .await
        .with_context(|| format!("timed out connecting to host bridge at {addr}"))?
        .with_context(|| format!("failed to connect to host bridge at {addr}"))?;
    let (read, write) = stream.into_split();
    let writer = BridgeWriter::new(write, options);
    if let Some(registration) = registration {
        writer
            .register(registration)
            .await
            .context("failed to register with host bridge")?;
    }
    Ok((BridgeReader::new(read, options.max_line_bytes), writer))
}

/// Connects, retrying every `delay` until it succeeds. Each failure is logged
/// with `what` naming the bridge being waited for.
pub async fn connect_retry(
    addr: &str,
    options: &Options,
    registration: Option<&Registration>,
    delay: Duration,
    what: &str,
) -> (BridgeReader, BridgeWriter) {
    loop {
        match connect(addr, options, registration).await {
            Ok(connection) => return connection,
            Err(err) => tracing::warn!("waiting for {what} at {addr}: {err:#}"),
        }
        sleep(delay).await;
    }
}

/// Feeds decoded events from `reader` into a bounded channel until the stream
/// ends, a read fails, or the receiver is dropped. The channel closes when the
/// task ends.
pub fn spawn_event_pump(
    mut reader: BridgeReader,
    capacity: usize,
) -> (mpsc::Receiver<Value>, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(capacity.max(1));
    let task = tokio::spawn(async move {
        loop {
            match reader.next_event().await {
                Ok(Some(value)) => {
                    if tx.send(value).await.is_err() {
                        return;
                    }
                }
                Ok(None) => return,
                Err(err) => {
                    tracing::warn!("host bridge read failed: {err}");
                    return;
                }
            }
        }
    });
    (rx, task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use haze_media_protocol::{encode_pcm_frame, PcmFrame, PcmMediaKind};
    use tokio::net::TcpListener;

    fn pcm_frame(feed_id: &str, pcm: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        encode_pcm_frame(
            &mut frame,
            PcmFrame {
                feed_id,
                queue_id: None,
                sample_rate: 48_000,
                channels: 1,
                duration_ms: 20,
                sequence: 0,
                pts_ns: 0,
                discontinuity: false,
                media_kind: PcmMediaKind::Routine,
                pcm,
            },
        )
        .expect("encode PCM frame");
        frame
    }

    #[test]
    fn publish_only_registration_has_no_identity() {
        let value = Registration::publish_only().to_value();
        assert_eq!(value["type"], "bridge.client");
        assert_eq!(value["data"]["receive_events"], false);
        assert_eq!(value["data"]["subscriptions"], json!([]));
        assert!(value["data"].get("client_id").is_none());
        assert!(value["data"].get("binary_pcm").is_none());
        assert!(value.get("source").is_none());
    }

    #[test]
    fn subscriber_registration_matches_the_existing_wire_shape() {
        let value =
            Registration::subscriber("haze-playout", &["system.shutdown", "operator.breakin.*"])
                .with_source("haze-playout")
                .to_value();
        assert_eq!(
            value,
            json!({
                "type": "bridge.client",
                "source": "haze-playout",
                "data": {
                    "receive_events": true,
                    "subscriptions": ["system.shutdown", "operator.breakin.*"],
                    "client_id": "haze-playout",
                },
            })
        );
    }

    #[test]
    fn binary_pcm_is_only_sent_when_requested() {
        let plain = Registration::anonymous_subscriber(&["playout.pcm"]).to_value();
        assert!(plain["data"].get("binary_pcm").is_none());
        assert!(plain["data"].get("client_id").is_none());
        let binary = Registration::anonymous_subscriber(&["playout.pcm"])
            .with_binary_pcm()
            .to_value();
        assert_eq!(binary["data"]["binary_pcm"], true);
    }

    async fn read_all(input: &[u8], max: usize) -> Vec<(ReadLine, Vec<u8>)> {
        let mut reader = BufReader::new(input);
        let mut line = Vec::new();
        let mut out = Vec::new();
        loop {
            let kind = read_bounded_line(&mut reader, &mut line, max)
                .await
                .unwrap();
            out.push((kind, line.clone()));
            if kind == ReadLine::Eof {
                return out;
            }
        }
    }

    #[tokio::test]
    async fn oversized_line_is_discarded_and_the_reader_recovers() {
        let mut input = vec![b'x'; 65];
        input.extend_from_slice(b"\n{\"type\":\"service.ready\"}\n");
        let lines = read_all(&input, 64).await;
        assert_eq!(lines[0], (ReadLine::Oversized, Vec::new()));
        assert_eq!(
            lines[1],
            (ReadLine::Data, br#"{"type":"service.ready"}"#.to_vec())
        );
        assert_eq!(lines[2].0, ReadLine::Eof);
    }

    #[tokio::test]
    async fn final_line_without_newline_is_data_and_crlf_is_stripped() {
        let lines = read_all(b"{\"a\":1}\r\n{\"b\":2}", 1024).await;
        assert_eq!(lines[0], (ReadLine::Data, br#"{"a":1}"#.to_vec()));
        assert_eq!(lines[1], (ReadLine::Data, br#"{"b":2}"#.to_vec()));
        assert_eq!(lines[2].0, ReadLine::Eof);
    }

    #[tokio::test]
    async fn a_line_at_the_limit_is_kept() {
        let mut input = vec![b'y'; 64];
        input.push(b'\n');
        let lines = read_all(&input, 65).await;
        assert_eq!(lines[0].0, ReadLine::Data);
        assert_eq!(lines[0].1.len(), 64);
    }

    #[tokio::test]
    async fn next_event_skips_blank_malformed_and_oversized_lines() {
        let mut input = Vec::new();
        input.extend_from_slice(b"\n   \r\nnot json\n");
        input.extend_from_slice(&vec![b'z'; 200]);
        input.extend_from_slice(b"\n  {\"type\":\"one\"}  \n{\"type\":\"two\"}");
        let mut reader = BridgeReader::new(input.as_slice(), 128);
        assert_eq!(reader.next_event().await.unwrap().unwrap()["type"], "one");
        assert_eq!(reader.next_event().await.unwrap().unwrap()["type"], "two");
        assert!(reader.next_event().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn next_line_exposes_the_raw_line_and_reports_oversized_input() {
        let mut input = vec![b'q'; 40];
        input.extend_from_slice(b"\n{\"type\":\"raw\"}\n");
        let mut reader = BridgeReader::new(input.as_slice(), 32);
        assert_eq!(reader.next_line().await.unwrap(), ReadLine::Oversized);
        assert_eq!(reader.next_line().await.unwrap(), ReadLine::Data);
        assert_eq!(reader.line(), br#"{"type":"raw"}"#);
        assert_eq!(reader.next_line().await.unwrap(), ReadLine::Eof);
    }

    #[tokio::test]
    async fn payloads_interleave_json_lines_and_compact_pcm_frames() {
        let first = pcm_frame("sk-0001", &[1, 0, 2, 0]);
        let second = pcm_frame("sk-0002", &[3, 0]);
        let mut input = Vec::new();
        input.extend_from_slice(b"{\"type\":\"playout.pcm\"}\n");
        input.extend_from_slice(&first);
        input.extend_from_slice(b"\n");
        input.extend_from_slice(&second);
        input.extend_from_slice(b"{\"type\":\"last\"}");
        let mut reader = BridgeReader::new(input.as_slice(), 1024);

        assert_eq!(reader.next_payload().await.unwrap(), Payload::Line);
        assert_eq!(reader.payload(), br#"{"type":"playout.pcm"}"#);
        assert_eq!(reader.next_payload().await.unwrap(), Payload::Pcm);
        assert_eq!(reader.payload(), first);
        // A blank line between frames is an empty JSON line, not a framing error.
        assert_eq!(reader.next_payload().await.unwrap(), Payload::Line);
        assert!(reader.payload().is_empty());
        assert_eq!(reader.next_payload().await.unwrap(), Payload::Pcm);
        assert_eq!(reader.payload(), second);
        assert_eq!(reader.next_payload().await.unwrap(), Payload::Line);
        assert_eq!(reader.payload(), br#"{"type":"last"}"#);
        assert_eq!(reader.next_payload().await.unwrap(), Payload::Eof);
    }

    #[tokio::test]
    async fn oversized_json_line_does_not_affect_the_next_pcm_frame() {
        let frame = pcm_frame("sk-0001", &[0; 64]);
        let mut input = vec![b'x'; 300];
        input.push(b'\n');
        input.extend_from_slice(&frame);
        let mut reader = BridgeReader::new(input.as_slice(), 128);

        assert_eq!(reader.next_payload().await.unwrap(), Payload::Oversized);
        assert_eq!(reader.next_payload().await.unwrap(), Payload::Pcm);
        assert_eq!(reader.payload(), frame);
        assert_eq!(reader.next_payload().await.unwrap(), Payload::Eof);
    }

    #[tokio::test]
    async fn pcm_frames_are_not_limited_by_the_json_line_limit() {
        let frame = pcm_frame("sk-0001", &vec![0; 4096]);
        let mut reader = BridgeReader::new(frame.as_slice(), 128);
        assert_eq!(reader.next_payload().await.unwrap(), Payload::Pcm);
        assert_eq!(reader.payload().len(), frame.len());
    }

    #[tokio::test]
    async fn invalid_or_truncated_pcm_frames_are_stream_errors() {
        let mut bad_version = pcm_frame("sk-0001", &[0, 0]);
        bad_version[4] = 99;
        let mut reader = BridgeReader::new(bad_version.as_slice(), 128);
        let err = reader.next_payload().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let frame = pcm_frame("sk-0001", &[0; 32]);
        let truncated = &frame[..frame.len() - 4];
        let mut reader = BridgeReader::new(truncated, 128);
        let err = reader.next_payload().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        let mut reader = BridgeReader::new(&frame[..4], 128);
        let err = reader.next_payload().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn trim_removes_ascii_whitespace_only_at_the_ends() {
        assert_eq!(
            trim_ascii_whitespace(b" \r\n{\"type\":\"a b\"}\t"),
            br#"{"type":"a b"}"#
        );
    }

    async fn fake_broker() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        (listener, addr)
    }

    #[tokio::test]
    async fn connect_registers_first_then_publishes_with_a_timestamp() {
        let (listener, addr) = fake_broker().await;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut text = String::new();
            let mut buffer = [0u8; 1024];
            while text.matches('\n').count() < 2 {
                let read = socket.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                text.push_str(&String::from_utf8_lossy(&buffer[..read]));
            }
            text
        });
        let registration = Registration::subscriber("haze-test", &["a.*"]);
        let (_reader, writer) = connect(&addr, &Options::default(), Some(&registration))
            .await
            .unwrap();
        writer
            .publish(json!({"type": "service.ready", "source": "haze-test"}))
            .await
            .unwrap();
        let text = server.await.unwrap();
        let mut lines = text.lines();
        let first: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        let second: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(first["type"], "bridge.client");
        assert_eq!(first["data"]["client_id"], "haze-test");
        assert_eq!(second["type"], "service.ready");
        assert!(second["timestamp"].is_string());
    }

    #[tokio::test]
    async fn send_keeps_the_event_unchanged_and_publish_keeps_an_existing_timestamp() {
        let (listener, addr) = fake_broker().await;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut text = String::new();
            let mut buffer = [0u8; 1024];
            while text.matches('\n').count() < 2 {
                let read = socket.read(&mut buffer).await.unwrap();
                if read == 0 {
                    break;
                }
                text.push_str(&String::from_utf8_lossy(&buffer[..read]));
            }
            text
        });
        let (_reader, writer) = connect(&addr, &Options::default(), None).await.unwrap();
        writer.send(&json!({"type": "a"})).await.unwrap();
        writer
            .publish(json!({"type": "b", "timestamp": "fixed"}))
            .await
            .unwrap();
        let text = server.await.unwrap();
        let mut lines = text.lines();
        let first: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        let second: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert!(first.get("timestamp").is_none());
        assert_eq!(second["timestamp"], "fixed");
    }

    #[tokio::test]
    async fn publish_limit_rejects_oversized_events_before_writing() {
        let (listener, addr) = fake_broker().await;
        let _server = tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let options = Options {
            max_publish_bytes: Some(32),
            ..Options::default()
        };
        let (_reader, writer) = connect(&addr, &options, None).await.unwrap();
        let big = json!({"type": "x", "data": "y".repeat(64)});
        let err = writer.send(&big).await.unwrap_err();
        assert!(err.to_string().contains("32 byte limit"), "{err:#}");
        writer.send(&json!({"type": "ok"})).await.unwrap();
    }

    #[tokio::test]
    async fn event_pump_delivers_events_and_closes_on_eof() {
        let (listener, addr) = fake_broker().await;
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket
                .write_all(b"{\"type\":\"one\"}\ngarbage\n{\"type\":\"two\"}\n")
                .await
                .unwrap();
        });
        let (reader, _writer) = connect(&addr, &Options::default(), None).await.unwrap();
        let (mut events, task) = spawn_event_pump(reader, 8);
        assert_eq!(events.recv().await.unwrap()["type"], "one");
        assert_eq!(events.recv().await.unwrap()["type"], "two");
        assert!(events.recv().await.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn connect_retry_waits_for_the_bridge_to_appear() {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let addr_text = addr.to_string();
        let server = tokio::spawn(async move {
            sleep(Duration::from_millis(150)).await;
            let listener = TcpListener::bind(addr).await.unwrap();
            listener.accept().await.unwrap();
        });
        let connected = timeout(
            Duration::from_secs(5),
            connect_retry(
                &addr_text,
                &Options::default(),
                None,
                Duration::from_millis(25),
                "test bridge",
            ),
        )
        .await;
        assert!(connected.is_ok(), "connect_retry never connected");
        server.await.unwrap();
    }
}
