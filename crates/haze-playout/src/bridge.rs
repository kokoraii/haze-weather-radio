use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use haze_bridge_client::{BridgeReader, BridgeWriter, Options, Registration};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

type BridgeResult<T> = std::result::Result<T, String>;
type PendingReply<T> = oneshot::Sender<BridgeResult<T>>;
type PendingMap<T> = Arc<Mutex<HashMap<String, PendingReply<T>>>>;
type PendingSynth = PendingMap<String>;
type PendingProducts = PendingMap<RenderedProduct>;

const SYNTH_REPLY_TIMEOUT: Duration = Duration::from_secs(90);
const PRODUCT_REPLY_TIMEOUT: Duration = Duration::from_secs(10);
/// Rendered products and synthesis results are the largest events playout reads.
const MAX_BRIDGE_LINE_BYTES: usize = 8 * 1024 * 1024;
const PLAYOUT_CLIENT_ID: &str = "haze-playout";
const PLAYOUT_SUBSCRIPTIONS: &[&str] = &[
    "system.shutdown",
    "scheduled_package",
    "playlist_refill",
    "playlist.item.ready",
    "playlist.item.group",
    "cap.alert.audio.ready",
    "cap.alert.cancelled",
    "playlist.control",
    "operator.breakin.*",
    "tts.synthesized",
    "tts.failed",
    "product.rendered",
    "product.render.failed",
];

#[derive(Clone)]
pub(crate) struct BridgeClient {
    writer: BridgeWriter,
    pending_synth: PendingSynth,
    pending_products: PendingProducts,
}

pub(crate) struct BridgeConnection {
    pub(crate) client: BridgeClient,
    pub(crate) events: mpsc::Receiver<Value>,
    pub(crate) reader_task: JoinHandle<()>,
}

#[derive(Debug, Clone)]
pub(crate) struct SynthJob {
    pub(crate) id: String,
    pub(crate) text: String,
    pub(crate) reader_id: String,
    pub(crate) language: String,
    pub(crate) output_path: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct ProductRenderRequest {
    pub(crate) request_id: String,
    pub(crate) feed_id: String,
    pub(crate) package_id: String,
    pub(crate) force: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RenderedProduct {
    #[serde(default)]
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) feed_id: String,
    #[serde(default)]
    pub(crate) package_id: String,
    #[serde(default)]
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) text: String,
    #[serde(default)]
    pub(crate) reader_id: String,
    #[serde(default)]
    pub(crate) language: String,
    #[serde(default)]
    pub(crate) metadata: HashMap<String, String>,
    #[serde(default)]
    pub(crate) segments: Vec<RenderedSegment>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RenderedSegment {
    #[serde(default)]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) label: String,
    #[serde(default)]
    pub(crate) text: String,
    #[serde(default)]
    pub(crate) audio_path: String,
}

fn bridge_options() -> Options {
    Options {
        max_line_bytes: MAX_BRIDGE_LINE_BYTES,
        ..Options::default()
    }
}

pub(crate) async fn connect(addr: &str) -> Result<BridgeConnection> {
    let (reader, writer) = haze_bridge_client::connect(addr, &bridge_options(), None).await?;
    let (tx, rx) = mpsc::channel(256);
    let pending_synth = Arc::new(Mutex::new(HashMap::new()));
    let pending_products = Arc::new(Mutex::new(HashMap::new()));
    let reader_task = tokio::spawn(read_loop(
        reader,
        tx,
        Arc::clone(&pending_synth),
        Arc::clone(&pending_products),
    ));
    Ok(BridgeConnection {
        client: BridgeClient {
            writer,
            pending_synth,
            pending_products,
        },
        events: rx,
        reader_task,
    })
}

pub(crate) async fn connect_retry(addr: &str) -> Result<BridgeConnection> {
    connect_consumer_retry(addr, PLAYOUT_CLIENT_ID, PLAYOUT_SUBSCRIPTIONS).await
}

pub(crate) async fn connect_consumer_retry(
    addr: &str,
    client_id: &str,
    subscriptions: &[&str],
) -> Result<BridgeConnection> {
    loop {
        match connect(addr).await {
            Ok(connection) => match connection
                .client
                .register_consumer(client_id, subscriptions)
                .await
            {
                Ok(()) => return Ok(connection),
                Err(err) => {
                    tracing::warn!(
                        "failed to register playout event subscriptions at {addr}: {err}"
                    );
                }
            },
            Err(err) => {
                tracing::warn!("waiting for host event bridge at {addr}: {err}");
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

pub(crate) async fn connect_publish_only_retry(addr: &str) -> Result<BridgeClient> {
    loop {
        match connect_publish_only(addr).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                tracing::warn!("waiting for host media bridge at {addr}: {err}");
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn connect_publish_only(addr: &str) -> Result<BridgeClient> {
    let registration = Registration::publish_only().with_source(PLAYOUT_CLIENT_ID);
    let (reader, writer) =
        haze_bridge_client::connect(addr, &bridge_options(), Some(&registration)).await?;
    // Nothing is delivered to a publish-only connection, so the read half is not used.
    drop(reader);
    Ok(BridgeClient {
        writer,
        pending_synth: Arc::new(Mutex::new(HashMap::new())),
        pending_products: Arc::new(Mutex::new(HashMap::new())),
    })
}

impl BridgeClient {
    pub(crate) async fn publish(&self, value: Value) -> Result<()> {
        self.writer.publish(value).await
    }

    /// Writes a complete pre-framed payload prepared by a hot-path producer.
    pub(crate) async fn write_raw(&self, payload: &[u8]) -> Result<()> {
        self.writer.write_raw(payload).await
    }

    async fn register_consumer(&self, client_id: &str, subscriptions: &[&str]) -> Result<()> {
        self.writer
            .register(
                &Registration::subscriber(client_id, subscriptions).with_source(PLAYOUT_CLIENT_ID),
            )
            .await
    }

    pub(crate) async fn service_ready(&self, feeds: usize) {
        let _ = self
            .publish(json!({
                "type": "service.ready",
                "source": "haze-playout",
                "data": {
                    "service": "haze-playout",
                    "feeds": feeds,
                }
            }))
            .await;
    }

    pub(crate) async fn synthesize(&self, job: SynthJob) -> Result<String> {
        let job_id = fallback_text(
            &job.id,
            &format!(
                "tts-{}",
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
            ),
        );
        let (tx, rx) = oneshot::channel();
        self.pending_synth.lock().await.insert(job_id.clone(), tx);
        if let Some(parent) = job.output_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let publish_result = self
            .publish(json!({
                "type": "tts.synthesize",
                "source": "haze-playout",
                "subject": job_id,
                "data": {
                    "job_id": job_id,
                    "text": job.text,
                    "reader_id": job.reader_id,
                    "language": job.language,
                    "output_path": job.output_path,
                }
            }))
            .await;
        if let Err(err) = publish_result {
            self.pending_synth.lock().await.remove(&job_id);
            return Err(err);
        }
        let result = match timeout(SYNTH_REPLY_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.pending_synth.lock().await.remove(&job_id);
                anyhow::bail!("event bridge closed while waiting for TTS");
            }
            Err(_) => {
                self.pending_synth.lock().await.remove(&job_id);
                anyhow::bail!("TTS request timed out");
            }
        };
        match result {
            Ok(path) => Ok(path),
            Err(err) => anyhow::bail!("{err}"),
        }
    }

    pub(crate) async fn render_product(
        &self,
        request: ProductRenderRequest,
    ) -> Result<RenderedProduct> {
        let request_id = fallback_text(
            &request.request_id,
            &format!(
                "product-{}-{}-{}",
                request.feed_id,
                request.package_id,
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
            ),
        );
        let (tx, rx) = oneshot::channel();
        self.pending_products
            .lock()
            .await
            .insert(request_id.clone(), tx);
        let publish_result = self
            .publish(json!({
                "type": "product.render.request",
                "source": "haze-playout",
                "subject": request_id,
                "data": {
                    "request_id": request_id,
                    "feed_id": request.feed_id,
                    "package_id": request.package_id,
                    "pkg_id": request.package_id,
                    "force": request.force,
                }
            }))
            .await;
        if let Err(err) = publish_result {
            self.pending_products.lock().await.remove(&request_id);
            return Err(err);
        }
        let result = match timeout(PRODUCT_REPLY_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.pending_products.lock().await.remove(&request_id);
                anyhow::bail!("event bridge closed while waiting for product render");
            }
            Err(_) => {
                self.pending_products.lock().await.remove(&request_id);
                anyhow::bail!("product render request timed out");
            }
        };
        match result {
            Ok(product) => Ok(product),
            Err(err) => anyhow::bail!("{err}"),
        }
    }
}

async fn read_loop(
    mut reader: BridgeReader,
    tx: mpsc::Sender<Value>,
    pending_synth: PendingSynth,
    pending_products: PendingProducts,
) {
    loop {
        let value = match reader.next_event().await {
            Ok(Some(value)) => value,
            Ok(None) => break,
            Err(err) => {
                tracing::warn!("host event bridge read failed: {err}");
                break;
            }
        };
        if handle_synth_result(&value, &pending_synth).await
            || handle_product_result(&value, &pending_products).await
        {
            continue;
        }
        if tx.send(value).await.is_err() {
            break;
        }
    }
    fail_pending(&pending_synth, "event bridge closed while waiting for TTS").await;
    fail_pending(
        &pending_products,
        "event bridge closed while waiting for product render",
    )
    .await;
}

async fn handle_synth_result(value: &Value, pending: &PendingSynth) -> bool {
    let msg_type = string_at(value, "type");
    if msg_type != "tts.synthesized" && msg_type != "tts.failed" {
        return false;
    }
    let data = data(value);
    let job_id = first_text(value, data, &["job_id", "subject"]);
    if job_id.is_empty() {
        return true;
    }
    let Some(ch) = pending.lock().await.remove(job_id) else {
        return true;
    };
    let result = if msg_type == "tts.failed" {
        Err(format!(
            "TTS failed for {job_id}: {}",
            string_at(data, "error")
        ))
    } else {
        Ok(string_at(data, "output_path").to_string())
    };
    let _ = ch.send(result);
    true
}

async fn handle_product_result(value: &Value, pending: &PendingProducts) -> bool {
    let msg_type = string_at(value, "type");
    if msg_type != "product.rendered" && msg_type != "product.render.failed" {
        return false;
    }
    let data = data(value);
    let request_id = first_text(value, data, &["request_id", "subject"]);
    if request_id.is_empty() {
        return true;
    }
    let Some(ch) = pending.lock().await.remove(request_id) else {
        return true;
    };
    let result = if msg_type == "product.render.failed" {
        Err(format!(
            "product render failed for {request_id}: {}",
            string_at(data, "error")
        ))
    } else {
        serde_json::from_value::<RenderedProduct>(
            data.get("product").cloned().unwrap_or(Value::Null),
        )
        .map_err(|err| err.to_string())
    };
    let _ = ch.send(result);
    true
}

async fn fail_pending<T>(pending: &PendingMap<T>, message: &str) {
    let mut pending = pending.lock().await;
    for (_, ch) in pending.drain() {
        let _ = ch.send(Err(message.to_string()));
    }
}

pub(crate) fn string_at<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("").trim()
}

pub(crate) fn data(value: &Value) -> &Value {
    value.get("data").unwrap_or(&Value::Null)
}

pub(crate) fn first_text<'a>(message: &'a Value, data: &'a Value, keys: &[&str]) -> &'a str {
    for key in keys {
        let from_data = string_at(data, key);
        if !from_data.is_empty() {
            return from_data;
        }
        let from_message = string_at(message, key);
        if !from_message.is_empty() {
            return from_message;
        }
    }
    ""
}

fn fallback_text(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    fn client_registration(
        receive_events: bool,
        client_id: Option<&str>,
        subscriptions: &[&str],
    ) -> Value {
        let registration = match (receive_events, client_id) {
            (false, _) => Registration::publish_only(),
            (true, Some(client_id)) => Registration::subscriber(client_id, subscriptions),
            (true, None) => Registration::anonymous_subscriber(subscriptions),
        };
        registration.with_source(PLAYOUT_CLIENT_ID).to_value()
    }

    #[tokio::test]
    async fn consumer_connection_registers_then_correlates_synthesis_replies() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake broker");
        let addr = listener.local_addr().expect("broker address").to_string();
        let broker = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept playout");
            let (read, mut write) = socket.into_split();
            let mut lines = BufReader::new(read).lines();
            let registration: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            let request: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            let reply = json!({
                "type": "tts.synthesized",
                "subject": request["subject"],
                "data": { "job_id": request["data"]["job_id"], "output_path": "out/final.wav" },
            });
            let mut raw = serde_json::to_vec(&reply).unwrap();
            raw.push(b'\n');
            write.write_all(&raw).await.unwrap();
            (registration, request)
        });

        let connection = connect_consumer_retry(&addr, PLAYOUT_CLIENT_ID, PLAYOUT_SUBSCRIPTIONS)
            .await
            .expect("connect to fake broker");
        let output = std::env::temp_dir()
            .join("haze-playout-bridge-test")
            .join("out.wav");
        let path = connection
            .client
            .synthesize(SynthJob {
                id: "job-1".to_string(),
                text: "hello".to_string(),
                reader_id: "reader".to_string(),
                language: "en-CA".to_string(),
                output_path: output,
            })
            .await
            .expect("synthesis reply is correlated by job id");
        assert_eq!(path, "out/final.wav");

        let (registration, request) = broker.await.expect("broker task");
        assert_eq!(registration["type"], "bridge.client");
        assert_eq!(registration["source"], PLAYOUT_CLIENT_ID);
        assert_eq!(registration["data"]["client_id"], PLAYOUT_CLIENT_ID);
        assert_eq!(registration["data"]["receive_events"], true);
        assert_eq!(request["type"], "tts.synthesize");
        assert_eq!(request["data"]["job_id"], "job-1");
        assert!(request["timestamp"].is_string());
    }

    #[tokio::test]
    async fn publish_only_connection_registers_without_receiving() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake broker");
        let addr = listener.local_addr().expect("broker address").to_string();
        let broker = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept playout");
            let mut lines = BufReader::new(socket).lines();
            let registration: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            let raw = lines.next_line().await.unwrap().unwrap();
            (registration, raw)
        });
        let client = connect_publish_only_retry(&addr).await.expect("connect");
        client
            .write_raw(b"HPCMdata\n")
            .await
            .expect("write pre-framed payload");
        let (registration, raw) = broker.await.expect("broker task");
        assert_eq!(registration["data"]["receive_events"], false);
        assert!(registration["data"].get("client_id").is_none());
        assert_eq!(raw, "HPCMdata");
    }

    #[test]
    fn playout_registration_uses_targeted_non_pcm_subscriptions() {
        let registration =
            client_registration(true, Some(PLAYOUT_CLIENT_ID), PLAYOUT_SUBSCRIPTIONS);
        let data = registration["data"]
            .as_object()
            .expect("registration data object");
        let subscriptions = data["subscriptions"]
            .as_array()
            .expect("subscription array");

        assert_eq!(data["client_id"], PLAYOUT_CLIENT_ID);
        assert_eq!(data["receive_events"], true);
        assert!(subscriptions.iter().any(|value| value == "system.shutdown"));
        assert!(subscriptions
            .iter()
            .any(|value| value == "playlist.item.ready"));
        assert!(!subscriptions.iter().any(|value| value == "playout.pcm"));
        assert!(!subscriptions.iter().any(|value| value == "*"));
    }

    #[test]
    fn publish_only_registration_has_no_identity_or_subscriptions() {
        let registration = client_registration(false, None, &[]);
        let data = registration["data"]
            .as_object()
            .expect("registration data object");

        assert_eq!(data["receive_events"], false);
        assert_eq!(data["subscriptions"], json!([]));
        assert!(!data.contains_key("client_id"));
    }
}
