use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use base64::Engine;
use haze_bridge_client::{BridgeReader, BridgeWriter, Options, ReadLine, Registration};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::watch;
use tokio::time::sleep;

const DEFAULT_QUEUE_CAPACITY: usize = 16;
const MAX_BRIDGE_LINE_BYTES: usize = 2 * 1024 * 1024;
const MAX_PCM_BYTES: usize = 1024 * 1024;
const MAX_CHUNK_AGE: Duration = Duration::from_secs(1);
const MAX_PCM_CHUNK_DURATION_MS: u32 = 100;
const MAX_DRAIN_DURATION_MS: u32 = 100;
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(250);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PcmMediaKind {
    Alert,
}

#[derive(Debug, Clone)]
pub(crate) struct PcmChunk {
    pub(crate) feed_id: String,
    pub(crate) queue_id: String,
    pub(crate) sequence: u64,
    pub(crate) pts_ns: u64,
    pub(crate) discontinuity: bool,
    pub(crate) sample_rate: u32,
    pub(crate) channels: u16,
    pub(crate) channel_layout: String,
    pub(crate) duration_ms: u32,
    pub(crate) media_kind: PcmMediaKind,
    pub(crate) pcm: Arc<[u8]>,
    received_at: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct MediaPcmHub {
    inner: Arc<HubInner>,
}

#[derive(Debug)]
struct HubInner {
    queue_capacity: usize,
    subscribers: Mutex<HashMap<String, Vec<Weak<SubscriptionQueue>>>>,
    stats: IngressStats,
}

#[derive(Debug, Default)]
struct IngressStats {
    connected: AtomicBool,
    connection_attempts: AtomicU64,
    reconnects: AtomicU64,
    lines_received: AtomicU64,
    accepted_chunks: AtomicU64,
    malformed_chunks: AtomicU64,
    uncorrelated_chunks: AtomicU64,
    wrong_media_kind_chunks: AtomicU64,
    oversized_lines: AtomicU64,
    chunks_without_subscribers: AtomicU64,
}

#[derive(Debug)]
struct SubscriptionQueue {
    feed_id: String,
    capacity: usize,
    chunks: Mutex<VecDeque<PcmChunk>>,
    stats: SubscriptionStats,
}

#[derive(Debug, Default)]
struct SubscriptionStats {
    routed: AtomicU64,
    overwritten: AtomicU64,
    superseded: AtomicU64,
    drained: AtomicU64,
    wrong_queue: AtomicU64,
    stale: AtomicU64,
    duplicate_or_reordered: AtomicU64,
    sequence_gaps: AtomicU64,
    discontinuities: AtomicU64,
    appsrc_errors: AtomicU64,
}

#[derive(Debug, Clone)]
pub(crate) struct MediaPcmSubscription {
    queue: Arc<SubscriptionQueue>,
    ingress: Arc<HubInner>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
enum DecodeError {
    #[error("event is not playout.pcm")]
    NotPcm,
    #[error("event JSON is malformed")]
    Malformed,
    #[error("event identities conflict")]
    ConflictingIdentity,
    #[error("event has no concrete feed or queue identity")]
    Uncorrelated,
    #[error("event is not alert media")]
    WrongMediaKind,
    #[error("event has invalid PCM metadata")]
    InvalidMetadata,
    #[error("event PCM payload is invalid")]
    InvalidPayload,
}

impl Default for MediaPcmHub {
    fn default() -> Self {
        Self::new()
    }
}

impl MediaPcmHub {
    pub(crate) fn new() -> Self {
        Self::with_capacity(DEFAULT_QUEUE_CAPACITY)
    }

    fn with_capacity(queue_capacity: usize) -> Self {
        Self {
            inner: Arc::new(HubInner {
                queue_capacity: queue_capacity.max(1),
                subscribers: Mutex::new(HashMap::new()),
                stats: IngressStats::default(),
            }),
        }
    }

    pub(crate) fn subscribe(&self, feed_id: impl Into<String>) -> MediaPcmSubscription {
        let feed_id = feed_id.into().trim().to_string();
        let queue = Arc::new(SubscriptionQueue {
            feed_id: feed_id.clone(),
            capacity: self.inner.queue_capacity,
            chunks: Mutex::new(VecDeque::with_capacity(self.inner.queue_capacity)),
            stats: SubscriptionStats::default(),
        });
        lock_unpoison(&self.inner.subscribers)
            .entry(feed_id)
            .or_default()
            .push(Arc::downgrade(&queue));
        MediaPcmSubscription {
            queue,
            ingress: Arc::clone(&self.inner),
        }
    }

    pub(crate) fn ingest_raw(&self, raw: &[u8]) {
        self.inner
            .stats
            .lines_received
            .fetch_add(1, Ordering::Relaxed);
        match decode_pcm_event(raw) {
            Ok(chunk) => {
                self.inner
                    .stats
                    .accepted_chunks
                    .fetch_add(1, Ordering::Relaxed);
                self.route(chunk);
            }
            Err(DecodeError::NotPcm) => {}
            Err(DecodeError::Uncorrelated | DecodeError::ConflictingIdentity) => {
                self.inner
                    .stats
                    .uncorrelated_chunks
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(DecodeError::WrongMediaKind) => {
                self.inner
                    .stats
                    .wrong_media_kind_chunks
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(
                DecodeError::Malformed | DecodeError::InvalidMetadata | DecodeError::InvalidPayload,
            ) => {
                self.inner
                    .stats
                    .malformed_chunks
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn route(&self, chunk: PcmChunk) {
        let mut subscribers = lock_unpoison(&self.inner.subscribers);
        let Some(feed_subscribers) = subscribers.get_mut(&chunk.feed_id) else {
            self.inner
                .stats
                .chunks_without_subscribers
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let mut delivered = false;
        feed_subscribers.retain(|weak| {
            let Some(queue) = weak.upgrade() else {
                return false;
            };
            queue.push(chunk.clone());
            delivered = true;
            true
        });
        if !delivered {
            self.inner
                .stats
                .chunks_without_subscribers
                .fetch_add(1, Ordering::Relaxed);
        }
        if feed_subscribers.is_empty() {
            subscribers.remove(&chunk.feed_id);
        }
    }

    fn set_connected(&self, connected: bool) {
        self.inner
            .stats
            .connected
            .store(connected, Ordering::Relaxed);
    }
}

impl SubscriptionQueue {
    fn push(&self, chunk: PcmChunk) {
        let mut chunks = lock_unpoison(&self.chunks);
        if chunks.len() == self.capacity {
            chunks.pop_front();
            self.stats.overwritten.fetch_add(1, Ordering::Relaxed);
        }
        chunks.push_back(chunk);
        self.stats.routed.fetch_add(1, Ordering::Relaxed);
    }
}

impl MediaPcmSubscription {
    pub(crate) fn feed_id(&self) -> &str {
        &self.queue.feed_id
    }

    pub(crate) fn drain_correlated(&self, queue_id: &str) -> Vec<PcmChunk> {
        let now = Instant::now();
        let mut pending = lock_unpoison(&self.queue.chunks);
        let mut correlated = Vec::new();
        while let Some(chunk) = pending.pop_front() {
            if chunk.queue_id != queue_id {
                self.queue.stats.wrong_queue.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if now.saturating_duration_since(chunk.received_at) > MAX_CHUNK_AGE {
                self.queue.stats.stale.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            self.queue.stats.drained.fetch_add(1, Ordering::Relaxed);
            correlated.push(chunk);
        }

        let mut keep_from = correlated.len();
        let mut retained_duration_ms = 0_u32;
        for (index, chunk) in correlated.iter().enumerate().rev() {
            let next_duration = retained_duration_ms.saturating_add(chunk.duration_ms);
            if next_duration > MAX_DRAIN_DURATION_MS {
                break;
            }
            retained_duration_ms = next_duration;
            keep_from = index;
        }
        if keep_from > 0 {
            let dropped = keep_from;
            correlated.drain(..dropped);
            self.queue
                .stats
                .superseded
                .fetch_add(dropped as u64, Ordering::Relaxed);
            if let Some(first) = correlated.first_mut() {
                first.discontinuity = true;
            }
        }

        correlated
    }

    pub(crate) fn record_duplicate_or_reordered(&self) {
        self.queue
            .stats
            .duplicate_or_reordered
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_sequence_gap(&self) {
        self.queue
            .stats
            .sequence_gaps
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_discontinuity(&self) {
        self.queue
            .stats
            .discontinuities
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_appsrc_error(&self) {
        self.queue
            .stats
            .appsrc_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn dropped_chunks(&self) -> u64 {
        self.queue.stats.overwritten.load(Ordering::Relaxed)
            + self.queue.stats.superseded.load(Ordering::Relaxed)
            + self.queue.stats.wrong_queue.load(Ordering::Relaxed)
            + self.queue.stats.stale.load(Ordering::Relaxed)
            + self
                .queue
                .stats
                .duplicate_or_reordered
                .load(Ordering::Relaxed)
    }

    pub(crate) fn status_value(&self) -> Value {
        let ingress = &self.ingress.stats;
        let queue = &self.queue.stats;
        json!({
            "connected": ingress.connected.load(Ordering::Relaxed),
            "feed_id": self.feed_id(),
            "connection_attempts": ingress.connection_attempts.load(Ordering::Relaxed),
            "reconnects": ingress.reconnects.load(Ordering::Relaxed),
            "lines_received": ingress.lines_received.load(Ordering::Relaxed),
            "accepted_chunks": ingress.accepted_chunks.load(Ordering::Relaxed),
            "malformed_chunks": ingress.malformed_chunks.load(Ordering::Relaxed),
            "uncorrelated_chunks": ingress.uncorrelated_chunks.load(Ordering::Relaxed),
            "wrong_media_kind_chunks": ingress.wrong_media_kind_chunks.load(Ordering::Relaxed),
            "oversized_lines": ingress.oversized_lines.load(Ordering::Relaxed),
            "chunks_without_subscribers": ingress.chunks_without_subscribers.load(Ordering::Relaxed),
            "routed_chunks": queue.routed.load(Ordering::Relaxed),
            "drained_chunks": queue.drained.load(Ordering::Relaxed),
            "overwritten_chunks": queue.overwritten.load(Ordering::Relaxed),
            "superseded_chunks": queue.superseded.load(Ordering::Relaxed),
            "wrong_queue_chunks": queue.wrong_queue.load(Ordering::Relaxed),
            "stale_chunks": queue.stale.load(Ordering::Relaxed),
            "duplicate_or_reordered_chunks": queue.duplicate_or_reordered.load(Ordering::Relaxed),
            "sequence_gaps": queue.sequence_gaps.load(Ordering::Relaxed),
            "discontinuities": queue.discontinuities.load(Ordering::Relaxed),
            "appsrc_errors": queue.appsrc_errors.load(Ordering::Relaxed),
            "pending_chunks": lock_unpoison(&self.queue.chunks).len(),
        })
    }
}

pub(crate) async fn run_bridge(
    addr: String,
    hub: MediaPcmHub,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let addr = addr.trim().to_string();
    if addr.is_empty() {
        tracing::warn!(
            "HAZE_MEDIA_BRIDGE_ADDR is empty; correlated CGEN alert audio is unavailable"
        );
        return;
    }
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;
    let mut connected_once = false;
    loop {
        if *shutdown_rx.borrow() {
            hub.set_connected(false);
            return;
        }
        hub.inner
            .stats
            .connection_attempts
            .fetch_add(1, Ordering::Relaxed);
        let registration = Registration::anonymous_subscriber(&[]).with_source("haze-cgen");
        let options = bridge_options();
        let connection = tokio::select! {
            result = haze_bridge_client::connect(&addr, &options, Some(&registration)) => result,
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    hub.set_connected(false);
                    return;
                }
                continue;
            }
        };
        match connection {
            Ok((reader, writer)) => {
                if connected_once {
                    hub.inner.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                }
                connected_once = true;
                hub.set_connected(true);
                tracing::info!("haze-cgen connected to the media bridge");
                let connected_at = Instant::now();
                if let Err(err) = run_connection(reader, writer, &hub, &mut shutdown_rx).await {
                    if !*shutdown_rx.borrow() {
                        tracing::warn!("haze-cgen media bridge disconnected: {err:#}");
                    }
                }
                if connected_at.elapsed() >= Duration::from_secs(10) {
                    reconnect_delay = INITIAL_RECONNECT_DELAY;
                }
                hub.set_connected(false);
            }
            Err(err) => {
                hub.set_connected(false);
                tracing::warn!("haze-cgen waiting for media bridge: {err:#}");
            }
        }
        tokio::select! {
            _ = sleep(reconnect_delay) => {}
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return;
                }
            }
        }
        reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
    }
}

fn bridge_options() -> Options {
    Options {
        max_line_bytes: MAX_BRIDGE_LINE_BYTES,
        ..Options::default()
    }
}

/// Reads PCM events until the bridge closes or shutdown is requested. The
/// writer is held for the life of the connection: dropping it would close the
/// socket's write side and the broker would drop this consumer.
async fn run_connection(
    mut reader: BridgeReader,
    _writer: BridgeWriter,
    hub: &MediaPcmHub,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<()> {
    loop {
        let result = tokio::select! {
            result = reader.next_line() => result,
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
                continue;
            }
        }?;
        match result {
            ReadLine::Eof => bail!("media bridge closed"),
            ReadLine::Oversized => {
                hub.inner
                    .stats
                    .oversized_lines
                    .fetch_add(1, Ordering::Relaxed);
            }
            ReadLine::Data => hub.ingest_raw(reader.line()),
        }
    }
}

fn decode_pcm_event(raw: &[u8]) -> std::result::Result<PcmChunk, DecodeError> {
    #[derive(Debug, Default, Deserialize)]
    struct EventData {
        #[serde(default)]
        feed_id: Option<String>,
        #[serde(default)]
        queue_id: Option<String>,
        sequence: Option<u64>,
        pts_ns: Option<u64>,
        #[serde(default)]
        discontinuity: bool,
        #[serde(default)]
        sample_rate: u32,
        #[serde(default)]
        channels: u16,
        #[serde(default)]
        channel_layout: String,
        #[serde(default)]
        duration_ms: u32,
        #[serde(default)]
        media_kind: String,
        #[serde(default)]
        pcm: String,
    }
    #[derive(Debug, Deserialize)]
    struct Event {
        #[serde(default)]
        r#type: String,
        #[serde(default)]
        feed_id: Option<String>,
        #[serde(default)]
        queue_id: Option<String>,
        #[serde(default)]
        data: EventData,
    }

    let event = serde_json::from_slice::<Event>(raw).map_err(|_| DecodeError::Malformed)?;
    if event.r#type != "playout.pcm" {
        return Err(DecodeError::NotPcm);
    }
    let feed_id = correlated_identity(
        event.data.feed_id.as_deref().unwrap_or_default(),
        event.feed_id.as_deref().unwrap_or_default(),
    )?;
    let queue_id = correlated_identity(
        event.data.queue_id.as_deref().unwrap_or_default(),
        event.queue_id.as_deref().unwrap_or_default(),
    )?;
    if feed_id == "*" || queue_id == "*" {
        return Err(DecodeError::Uncorrelated);
    }
    if event.data.media_kind.trim() != "alert" {
        return Err(DecodeError::WrongMediaKind);
    }
    let sequence = event.data.sequence.ok_or(DecodeError::InvalidMetadata)?;
    let pts_ns = event.data.pts_ns.ok_or(DecodeError::InvalidMetadata)?;
    if !(8_000..=192_000).contains(&event.data.sample_rate)
        || !(1..=8).contains(&event.data.channels)
        || !layout_matches(event.data.channel_layout.trim(), event.data.channels)
    {
        return Err(DecodeError::InvalidMetadata);
    }
    let encoded = event.data.pcm.trim();
    if encoded.is_empty() || encoded.len() > MAX_PCM_BYTES.saturating_mul(2) {
        return Err(DecodeError::InvalidPayload);
    }
    let mut pcm = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| DecodeError::InvalidPayload)?;
    let frame_bytes = usize::from(event.data.channels).saturating_mul(2);
    if pcm.is_empty() || pcm.len() > MAX_PCM_BYTES || pcm.len() % frame_bytes != 0 {
        return Err(DecodeError::InvalidPayload);
    }
    let frames = pcm.len() / frame_bytes;
    let derived_duration = ((frames as u128 * 1_000) / u128::from(event.data.sample_rate))
        .max(1)
        .min(u128::from(u32::MAX)) as u32;
    if derived_duration > MAX_PCM_CHUNK_DURATION_MS
        || (event.data.duration_ms != 0 && event.data.duration_ms.abs_diff(derived_duration) > 2)
    {
        return Err(DecodeError::InvalidMetadata);
    }
    pcm.shrink_to_fit();
    Ok(PcmChunk {
        feed_id,
        queue_id,
        sequence,
        pts_ns,
        discontinuity: event.data.discontinuity,
        sample_rate: event.data.sample_rate,
        channels: event.data.channels,
        channel_layout: event.data.channel_layout.trim().to_string(),
        duration_ms: derived_duration,
        media_kind: PcmMediaKind::Alert,
        pcm: Arc::from(pcm),
        received_at: Instant::now(),
    })
}

fn correlated_identity(primary: &str, fallback: &str) -> std::result::Result<String, DecodeError> {
    let primary = primary.trim();
    let fallback = fallback.trim();
    if !primary.is_empty() && !fallback.is_empty() && primary != fallback {
        return Err(DecodeError::ConflictingIdentity);
    }
    let identity = if primary.is_empty() {
        fallback
    } else {
        primary
    };
    if identity.is_empty() {
        Err(DecodeError::Uncorrelated)
    } else if identity.len() > 256 || identity.chars().any(char::is_control) {
        Err(DecodeError::InvalidMetadata)
    } else {
        Ok(identity.to_string())
    }
}

fn layout_matches(layout: &str, channels: u16) -> bool {
    match (layout, channels) {
        ("mono", 1) | ("stereo", 2) | ("5.1", 6) => true,
        _ => {
            layout
                .strip_suffix("ch")
                .and_then(|count| count.parse::<u16>().ok())
                == Some(channels)
        }
    }
}

fn lock_unpoison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(feed_id: &str, queue_id: Option<&str>, sequence: u64) -> Vec<u8> {
        let pcm = base64::engine::general_purpose::STANDARD.encode(vec![0u8; 1_920]);
        serde_json::to_vec(&json!({
            "type": "playout.pcm",
            "feed_id": feed_id,
            "queue_id": queue_id,
            "data": {
                "feed_id": feed_id,
                "queue_id": queue_id,
                "sequence": sequence,
                "pts_ns": sequence * 20_000_000,
                "discontinuity": sequence == 0,
                "sample_rate": 48_000,
                "channels": 1,
                "channel_layout": "mono",
                "duration_ms": 20,
                "media_kind": "alert",
                "pcm": pcm,
            }
        }))
        .expect("event JSON")
    }

    #[test]
    fn decodes_correlated_alert_pcm_metadata() {
        let chunk = decode_pcm_event(&event("feed-a", Some("queue-a"), 7)).expect("PCM chunk");
        assert_eq!(chunk.feed_id, "feed-a");
        assert_eq!(chunk.queue_id, "queue-a");
        assert_eq!(chunk.sequence, 7);
        assert_eq!(chunk.pts_ns, 140_000_000);
        assert_eq!(chunk.sample_rate, 48_000);
        assert_eq!(chunk.channel_layout, "mono");
        assert_eq!(chunk.pcm.len(), 1_920);
    }

    #[test]
    fn rejects_uncorrelated_and_non_alert_pcm() {
        assert_eq!(
            decode_pcm_event(&event("feed-a", None, 0)).unwrap_err(),
            DecodeError::Uncorrelated
        );
        let mut value: Value = serde_json::from_slice(&event("feed-a", Some("q"), 0)).unwrap();
        value["data"]["media_kind"] = json!("routine");
        assert_eq!(
            decode_pcm_event(&serde_json::to_vec(&value).unwrap()).unwrap_err(),
            DecodeError::WrongMediaKind
        );
    }

    #[test]
    fn rejects_conflicting_top_level_and_data_identities() {
        let mut value: Value = serde_json::from_slice(&event("feed-a", Some("q"), 0)).unwrap();
        value["data"]["feed_id"] = json!("feed-b");
        assert_eq!(
            decode_pcm_event(&serde_json::to_vec(&value).unwrap()).unwrap_err(),
            DecodeError::ConflictingIdentity
        );
    }

    #[test]
    fn per_feed_queue_is_bounded_and_keeps_latest_chunks() {
        let hub = MediaPcmHub::with_capacity(2);
        let subscription = hub.subscribe("feed-a");
        for sequence in 0..3 {
            hub.ingest_raw(&event("feed-a", Some("queue-a"), sequence));
        }
        let chunks = subscription.drain_correlated("queue-a");
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(subscription.status_value()["overwritten_chunks"], 1);
        assert_eq!(subscription.status_value()["superseded_chunks"], 0);
    }

    #[test]
    fn drain_drops_only_stale_prefix_when_fresh_backlog_is_too_large() {
        let hub = MediaPcmHub::with_capacity(8);
        let subscription = hub.subscribe("feed-a");
        for sequence in 0..8 {
            hub.ingest_raw(&event("feed-a", Some("queue-a"), sequence));
        }

        let chunks = subscription.drain_correlated("queue-a");
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| chunk.sequence)
                .collect::<Vec<_>>(),
            vec![3, 4, 5, 6, 7]
        );
        assert!(chunks[0].discontinuity);
        assert_eq!(subscription.status_value()["superseded_chunks"], 3);
    }

    #[test]
    fn rejects_declared_duration_that_disagrees_with_pcm() {
        let mut value: Value =
            serde_json::from_slice(&event("feed-a", Some("queue-a"), 0)).unwrap();
        value["data"]["duration_ms"] = json!(250);

        assert_eq!(
            decode_pcm_event(&serde_json::to_vec(&value).unwrap()).unwrap_err(),
            DecodeError::InvalidMetadata
        );
    }

    #[test]
    fn drain_drops_chunks_for_a_different_queue() {
        let hub = MediaPcmHub::new();
        let subscription = hub.subscribe("feed-a");
        hub.ingest_raw(&event("feed-a", Some("old-queue"), 0));
        hub.ingest_raw(&event("feed-a", Some("active-queue"), 1));
        let chunks = subscription.drain_correlated("active-queue");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].queue_id, "active-queue");
        assert_eq!(subscription.status_value()["wrong_queue_chunks"], 1);
    }

    #[test]
    fn drain_drops_stale_backlog_instead_of_catching_up() {
        let hub = MediaPcmHub::new();
        let subscription = hub.subscribe("feed-a");
        let mut chunk = decode_pcm_event(&event("feed-a", Some("queue-a"), 0)).expect("PCM chunk");
        chunk.received_at = Instant::now() - MAX_CHUNK_AGE - Duration::from_millis(1);
        hub.route(chunk);
        assert!(subscription.drain_correlated("queue-a").is_empty());
        assert_eq!(subscription.status_value()["stale_chunks"], 1);
    }

    #[test]
    fn subscriptions_are_isolated_by_exact_feed_id() {
        let hub = MediaPcmHub::new();
        let a = hub.subscribe("feed-a");
        let b = hub.subscribe("feed-b");
        hub.ingest_raw(&event("feed-a", Some("queue-a"), 0));
        assert_eq!(a.drain_correlated("queue-a").len(), 1);
        assert!(b.drain_correlated("queue-a").is_empty());
    }
}
