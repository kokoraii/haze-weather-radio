use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, UNIX_EPOCH};

use anyhow::{Context, Result};
use base64::Engine;
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::bridge::{self, BridgeClient, ProductRenderRequest, RenderedProduct, SynthJob};
use crate::config::{display_text, resolve_path, FeedConfig, LoadedConfig};
use crate::sinks::{sinks_for_feed, Sink};
use haze_media::{decode_wav, normalize_pcm, silence_chunk, Pcm};
use memmap2::{Mmap, MmapOptions};

const SOURCE_ID: &str = "haze-playout";
const ALERT_QUEUE_DIR: &str = "runtime/queues/alerts";
// Keep playout's audio and alert timing at 20 ms. The bridge combines these into
// larger messages, while downstream media outputs retain their 20 ms framing.
const PCM_CHUNK_MS: u32 = 20;
const MEDIA_PUBLISH_CHUNK_MS: u32 = 100;
const LIVE_BREAKIN_MAX_BUFFER_MS: u32 = 750;
const PCM_PUBLISH_QUEUE_CAPACITY: usize = 3;
const ROUTINE_AUDIO_QUEUE_CAPACITY: usize = 16;
const PRIORITY_AUDIO_QUEUE_CAPACITY: usize = 16;
const PRIORITY_PREPARE_QUEUE_CAPACITY: usize = 16;
const PRIORITY_PENDING_QUEUE_CAPACITY: usize = PRIORITY_AUDIO_QUEUE_CAPACITY;
const BREAKIN_COMMAND_QUEUE_CAPACITY: usize = 128;
const PACKAGE_REQUEST_QUEUE_CAPACITY: usize = 8;
const CONTROL_QUEUE_CAPACITY: usize = 16;
const DEFERRED_ROUTINE_QUEUE_CAPACITY: usize = 2;
const COMPLETED_ROUTINE_HISTORY_CAPACITY: usize = 128;
const SEGMENT_GROUP_REVISION_QUEUE_CAPACITY: usize = 32;
const MAX_OPEN_SEGMENT_GROUPS: usize = ROUTINE_AUDIO_QUEUE_CAPACITY;
const MAX_SEGMENTS_PER_GROUP: usize = 64;
const MAX_SEGMENT_DURATION_MS: u64 = 180_000;
const MAX_SEGMENT_PAUSE_MS: u64 = 30_000;
const GROUP_ADMISSION_MIN_SEGMENTS: usize = 2;
const GROUP_ADMISSION_MIN_READY_MS: u64 = 2_000;
const REALTIME_LAG_WARN_BACKLOG_MS: u64 = 60;
const AUDIO_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;
const AUDIO_CACHE_MAX_READY_ENTRIES: usize = 96;
const AUDIO_CACHE_MAX_IDLE: Duration = Duration::from_secs(45);
const AUDIO_CACHE_PRUNE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub(crate) struct Options {
    pub(crate) config_path: PathBuf,
    pub(crate) bridge_addr: String,
    pub(crate) media_bridge_addr: String,
    pub(crate) alert_poll: Duration,
}

#[derive(Debug, Clone)]
struct PackageRequest {
    package_id: String,
    force: bool,
}

#[derive(Debug, Clone)]
struct PlayoutControl {
    action: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct AfterCurrentAction {
    action: String,
    target_item_id: String,
}

#[derive(Debug, Clone)]
struct AudioItem {
    id: String,
    package_id: String,
    title: String,
    pcm: SharedPcm,
    metadata: AudioItemMetadata,
    gap_after: Duration,
    not_before: Option<DateTime<Utc>>,
    queued_at: String,
    target_start: String,
    predicted_start: String,
    predicted_finish: String,
    source: ItemSource,
    segment_group: Option<Arc<SegmentGroup>>,
}

#[derive(Debug, Clone)]
struct SegmentGroup {
    queue_id: String,
    feed_id: String,
    package_id: String,
    title: String,
    manifest_path: String,
    expected_segments: usize,
    not_before: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct SegmentGroupRequest {
    queue_id: String,
    feed_id: String,
    package_id: String,
    title: String,
    manifest_path: String,
    revision: u64,
    status: SegmentGroupStatus,
    expected_segments: usize,
    not_before: Option<DateTime<Utc>>,
}

#[derive(Debug, Default)]
struct SegmentGroupMailbox {
    latest_by_queue_id: HashMap<String, SegmentGroupRequest>,
    queued_ids: HashSet<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SegmentGroupStatus {
    Open,
    Sealed,
    Aborted,
}

#[derive(Debug, Clone)]
struct SegmentGroupSegment {
    index: usize,
    audio_path: String,
    duration_ms: u64,
    pause_after_ms: u64,
    pcm: SharedPcm,
}

#[derive(Debug, Clone)]
struct SegmentGroupRevision {
    request: SegmentGroupRequest,
    status: SegmentGroupStatus,
    segments: Vec<SegmentGroupSegment>,
}

#[derive(Debug)]
enum SegmentGroupRevisionMessage {
    Ready(SegmentGroupRevision),
    Rejected {
        request: SegmentGroupRequest,
        error: String,
    },
}

#[derive(Debug)]
struct SegmentGroupPlayback {
    group: Arc<SegmentGroup>,
    last_revision: Option<u64>,
    status: SegmentGroupStatus,
    segments: Vec<Option<SegmentGroupSegment>>,
    segment_index: usize,
    segment_offset: usize,
    pause_remaining_bytes: usize,
    admitted: bool,
    admission_pending: bool,
    admission_item: Option<AudioItem>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SegmentGroupRender {
    Playing,
    Underflow,
    Completed,
    Aborted,
}

#[derive(Debug, Deserialize)]
struct SegmentGroupManifest {
    #[serde(default)]
    queue_id: String,
    #[serde(default)]
    revision: u64,
    #[serde(default)]
    expected_segments: Option<usize>,
    #[serde(default)]
    sample_rate: u32,
    #[serde(default)]
    channels: u16,
    #[serde(default)]
    status: String,
    #[serde(default)]
    segments: Vec<SegmentGroupManifestSegment>,
}

#[derive(Debug, Deserialize)]
struct SegmentGroupManifestSegment {
    #[serde(default)]
    index: usize,
    #[serde(default)]
    audio_path: String,
    #[serde(default)]
    duration_ms: u64,
    #[serde(default)]
    pause_after_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct AudioItemMetadata {
    audio_path: String,
    duration_ms: u64,
    alert_packet: Option<Value>,
    alert_text: String,
    banner_text: String,
    background_color: String,
    priority: String,
    feed_ids: Vec<String>,
    broadcast_immediate: bool,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
enum SharedAudioKey {
    Wav {
        path: PathBuf,
        modified_ns: Option<u128>,
        len: u64,
        sample_rate: u32,
        channels: u16,
    },
    RawPcm {
        path: PathBuf,
        modified_ns: Option<u128>,
        len: u64,
        source_rate: u32,
        source_channels: u16,
        sample_rate: u32,
        channels: u16,
    },
    Synth {
        text: String,
        reader_id: String,
        language: String,
        sample_rate: u32,
        channels: u16,
    },
}

#[derive(Debug)]
enum SharedAudioEntry {
    Ready {
        pcm: SharedPcm,
        bytes: usize,
        last_used: Instant,
    },
    Loading(Vec<oneshot::Sender<Result<SharedPcm, String>>>),
}

#[derive(Debug)]
struct MappedPcm {
    mmap: Mmap,
    offset: usize,
    len: usize,
}

#[derive(Debug, Clone)]
enum SharedPcm {
    Owned(Arc<[u8]>),
    Mapped(Arc<MappedPcm>),
}

impl SharedPcm {
    fn owned(data: Vec<u8>) -> Self {
        Self::Owned(Arc::from(data.into_boxed_slice()))
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::Owned(data) => data.len(),
            Self::Mapped(_) => 0,
        }
    }
}

impl AsRef<[u8]> for SharedPcm {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Owned(data) => data,
            Self::Mapped(data) => &data.mmap[data.offset..data.offset + data.len],
        }
    }
}

impl Deref for SharedPcm {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

#[derive(Debug, Default)]
struct AudioCache {
    entries: Mutex<HashMap<SharedAudioKey, SharedAudioEntry>>,
}

#[derive(Debug, Clone)]
struct PcmPublish {
    data: Vec<u8>,
    duration_ms: u32,
    sequence: u64,
    pts_ns: u64,
    discontinuity: bool,
    queue_id: Option<String>,
    media_kind: PcmMediaKind,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PcmMediaKind {
    Silence,
    Routine,
    Alert,
    OperatorBreakIn,
}

impl PcmMediaKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Silence => "silence",
            Self::Routine => "routine",
            Self::Alert => "alert",
            Self::OperatorBreakIn => "operator_breakin",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct PcmMediaContext {
    queue_id: Option<String>,
    media_kind: PcmMediaKind,
}

impl PcmMediaContext {
    fn active(live_breakin: Option<&LiveBreakIn>, current: Option<&AudioItem>) -> Self {
        if let Some(live) = live_breakin {
            return Self {
                queue_id: Some(live.id.clone()),
                media_kind: PcmMediaKind::OperatorBreakIn,
            };
        }
        if let Some(item) = current {
            let queue_id = item.is_alert().then(|| item.id.clone());
            return Self {
                queue_id,
                media_kind: if item.is_alert() {
                    PcmMediaKind::Alert
                } else {
                    PcmMediaKind::Routine
                },
            };
        }
        Self {
            queue_id: None,
            media_kind: PcmMediaKind::Silence,
        }
    }
}

#[derive(Debug)]
struct PcmPublishBuilder {
    data: Vec<u8>,
    duration_ms: u32,
    pts_ns: Option<u64>,
    context: Option<PcmMediaContext>,
    next_sequence: u64,
    needs_discontinuity: bool,
}

impl PcmPublishBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            duration_ms: 0,
            pts_ns: None,
            context: None,
            next_sequence: 0,
            needs_discontinuity: true,
        }
    }

    fn duration_ms(&self) -> u32 {
        self.duration_ms
    }

    fn push(&mut self, data: &[u8], duration_ms: u32, pts_ns: u64, context: PcmMediaContext) {
        debug_assert!(self
            .context
            .as_ref()
            .is_none_or(|active| active == &context));
        if self.data.is_empty() {
            self.pts_ns = Some(pts_ns);
            self.context = Some(context);
        }
        self.data.extend_from_slice(data);
        self.duration_ms = self.duration_ms.saturating_add(duration_ms);
    }

    fn take(&mut self) -> Option<PcmPublish> {
        if self.data.is_empty() {
            return None;
        }
        let context = self.context.take()?;
        let publish = PcmPublish {
            data: std::mem::take(&mut self.data),
            duration_ms: std::mem::take(&mut self.duration_ms),
            sequence: self.next_sequence,
            pts_ns: self.pts_ns.take().unwrap_or(0),
            discontinuity: std::mem::take(&mut self.needs_discontinuity),
            queue_id: context.queue_id,
            media_kind: context.media_kind,
        };
        self.next_sequence = self.next_sequence.saturating_add(1);
        Some(publish)
    }

    fn recycle(&mut self, mut data: Vec<u8>) {
        data.clear();
        self.data = data;
    }

    fn mark_discontinuity(&mut self) {
        self.needs_discontinuity = true;
    }
}

#[derive(Debug, Default)]
struct PcmDeliveryState {
    previous_publish_failed: bool,
}

impl PcmDeliveryState {
    fn prepare(&self, chunk: &mut PcmPublish) {
        chunk.discontinuity |= self.previous_publish_failed;
    }

    fn record_result(&mut self, succeeded: bool) {
        self.previous_publish_failed = !succeeded;
    }
}

struct PcmPublisher {
    tx: mpsc::Sender<PcmPublish>,
    recycled: mpsc::Receiver<Vec<u8>>,
}

#[derive(Serialize)]
struct PcmBridgeEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'static str,
    source: &'static str,
    feed_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_id: Option<&'a str>,
    timestamp: &'a str,
    data: PcmBridgeData<'a>,
}

#[derive(Serialize)]
struct PcmBridgeData<'a> {
    feed_id: &'a str,
    sample_rate: u32,
    channels: u16,
    channel_layout: &'a str,
    duration_ms: u32,
    sequence: u64,
    pts_ns: u64,
    discontinuity: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_id: Option<&'a str>,
    media_kind: &'a str,
    pcm: &'a str,
}

#[derive(Debug, Clone)]
enum BreakInCommand {
    Start {
        id: String,
        title: String,
    },
    Chunk {
        id: String,
        pcm: Vec<u8>,
        sample_rate: u32,
        channels: u16,
    },
    Finish {
        id: String,
    },
    Cancel {
        id: String,
    },
}

#[derive(Debug)]
struct LiveBreakIn {
    id: String,
    title: String,
    buffer: VecDeque<u8>,
    finishing: bool,
}

#[derive(Debug, Clone)]
enum ItemSource {
    Playlist,
    Generated,
    Alert {
        manifest_path: PathBuf,
        header: String,
        event: String,
    },
}

#[derive(Debug, Clone)]
struct FeedHandle {
    feed: FeedConfig,
    audio_tx: mpsc::Sender<AudioItem>,
    segment_group_mailbox: Arc<StdMutex<SegmentGroupMailbox>>,
    segment_group_tx: mpsc::Sender<String>,
    priority_prepare_tx: mpsc::Sender<Value>,
    breakin_tx: mpsc::Sender<BreakInCommand>,
    request_tx: mpsc::Sender<PackageRequest>,
    control_tx: mpsc::Sender<PlayoutControl>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
struct AlertQueueItem {
    #[serde(default)]
    id: String,
    #[serde(default)]
    alert_id: String,
    #[serde(default)]
    alert_packet: Option<Value>,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    priority: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    created_at: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    feed_id: String,
    #[serde(default)]
    feed_ids: Vec<String>,
    #[serde(default)]
    header: String,
    #[serde(default)]
    event: String,
    #[serde(default)]
    alert_text: String,
    #[serde(default)]
    banner_text: String,
    #[serde(default)]
    background_color: String,
    #[serde(default)]
    broadcast_immediate: bool,
    #[serde(default)]
    alert_sent_at: String,
    #[serde(default)]
    alert_expires_at: String,
    #[serde(default)]
    message_type: String,
    #[serde(default)]
    audio_path: String,
    #[serde(default)]
    sample_rate: u32,
    #[serde(default)]
    channels: u16,
    #[serde(default)]
    claimed_at: Option<String>,
    #[serde(default)]
    played_at: Option<String>,
    #[serde(default)]
    failed_at: Option<String>,
    #[serde(default)]
    last_error: Option<String>,
}

#[derive(Debug)]
struct AlertCandidate {
    manifest: PathBuf,
    item: AlertQueueItem,
    id: String,
    sort_key: AlertSortKey,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct AlertSortKey {
    group: String,
    rank: u8,
    created_at: String,
    id: String,
}

enum ConnectedOutcome {
    Reconnect,
    Shutdown,
}

#[derive(Clone)]
struct SessionTasks {
    cancellation: CancellationToken,
    tracker: TaskTracker,
}

impl SessionTasks {
    fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            tracker: TaskTracker::new(),
        }
    }

    fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let cancellation = self.cancellation.clone();
        self.tracker.spawn(async move {
            tokio::select! {
                _ = cancellation.cancelled() => {}
                _ = task => {}
            }
        });
    }

    fn spawn_cooperative<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tracker.spawn(task);
    }

    fn track_abortable(&self, mut task: JoinHandle<()>) {
        let cancellation = self.cancellation.clone();
        self.tracker.spawn(async move {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    task.abort();
                    let _ = task.await;
                }
                _ = &mut task => {}
            }
        });
    }

    async fn shutdown(&self) {
        self.cancellation.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }
}

pub(crate) async fn run(options: Options) -> Result<()> {
    if options.bridge_addr.trim().is_empty() {
        anyhow::bail!("missing host event bridge address");
    }

    loop {
        let cfg = Arc::new(crate::config::load_config(&options.config_path)?);
        let connection = bridge::connect_retry(&options.bridge_addr).await?;
        let media_bridge_addr = if options.media_bridge_addr.trim().is_empty() {
            options.bridge_addr.trim()
        } else {
            options.media_bridge_addr.trim()
        };
        match run_connected(Arc::clone(&cfg), connection, media_bridge_addr, &options).await {
            ConnectedOutcome::Shutdown => return Ok(()),
            ConnectedOutcome::Reconnect => {
                tracing::warn!("host event bridge disconnected; reconnecting playout service");
            }
        }
    }
}

async fn run_connected(
    cfg: Arc<LoadedConfig>,
    connection: bridge::BridgeConnection,
    media_bridge_addr: &str,
    options: &Options,
) -> ConnectedOutcome {
    let bridge::BridgeConnection {
        client,
        mut events,
        reader_task,
    } = connection;
    let mut handles = HashMap::<String, FeedHandle>::new();
    let audio_cache = Arc::new(AudioCache::default());
    let session_tasks = SessionTasks::new();
    session_tasks.track_abortable(reader_task);
    spawn_audio_cache_maintenance(&session_tasks, &audio_cache);
    for feed in cfg.enabled_feeds().cloned() {
        let media_client = match bridge::connect_publish_only_retry(media_bridge_addr).await {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!(
                    feed_id = feed.id,
                    "failed to connect feed media bridge publisher: {err}"
                );
                session_tasks.shutdown().await;
                return ConnectedOutcome::Reconnect;
            }
        };
        let handle = FeedHandle::spawn(
            Arc::clone(&cfg),
            client.clone(),
            media_client.clone(),
            feed,
            options.alert_poll,
            Arc::clone(&audio_cache),
            &session_tasks,
        );
        handles.insert(handle.feed.id.clone(), handle);
    }
    client.service_ready(handles.len()).await;

    let outcome = loop {
        let Some(event) = events.recv().await else {
            break ConnectedOutcome::Reconnect;
        };
        if bridge::string_at(&event, "type") == "system.shutdown" {
            break ConnectedOutcome::Shutdown;
        }
        dispatch_event(&cfg, &audio_cache, &handles, &session_tasks, event).await;
    };
    drop(handles);
    session_tasks.shutdown().await;
    outcome
}

fn spawn_audio_cache_maintenance(tasks: &SessionTasks, audio_cache: &Arc<AudioCache>) {
    let cache = Arc::downgrade(audio_cache);
    tasks.spawn(async move {
        let mut ticker = interval(AUDIO_CACHE_PRUNE_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let Some(cache) = cache.upgrade() else {
                break;
            };
            cache.prune_idle().await;
        }
    });
}

async fn dispatch_event(
    cfg: &Arc<LoadedConfig>,
    audio_cache: &Arc<AudioCache>,
    handles: &HashMap<String, FeedHandle>,
    tasks: &SessionTasks,
    event: Value,
) {
    match bridge::string_at(&event, "type") {
        "scheduled_package" => {
            if cfg.root.services.go.playlist.enabled
                && bridge::string_at(&event, "source") == "daemon-scheduler"
            {
                return;
            }
            let data = bridge::data(&event);
            let package_id = bridge::first_text(&event, data, &["pkg_id", "package_id"]);
            let feed_id = bridge::first_text(&event, data, &["feed_id"]);
            if !package_id.is_empty() {
                dispatch_package(handles, feed_id, package_id, true).await;
            }
        }
        "playlist_refill" => {
            if cfg.root.services.go.playlist.enabled {
                return;
            }
            for handle in handles.values() {
                if let Some(package_id) = cfg.routine_playlist_order().first() {
                    let _ = handle
                        .request_tx
                        .send(PackageRequest {
                            package_id: package_id.clone(),
                            force: false,
                        })
                        .await;
                }
            }
        }
        "playlist.item.ready" => {
            let data = bridge::data(&event);
            let feed_id = bridge::first_text(&event, data, &["feed_id"]);
            let Some(handle) = handles.get(feed_id).cloned() else {
                return;
            };
            let package_id = bridge::first_text(&event, data, &["package_id", "pkg_id"]);
            let Some(permit) = try_reserve_routine_slot(&handle.audio_tx, feed_id, package_id)
            else {
                return;
            };
            let cfg = Arc::clone(cfg);
            let audio_cache = Arc::clone(audio_cache);
            let data = data.clone();
            let feed_id = feed_id.to_string();
            tasks.spawn(async move {
                match audio_item_from_ready(&cfg, &audio_cache, &handle.feed, data).await {
                    Ok(item) => {
                        permit.send(item);
                    }
                    Err(err) => tracing::warn!(feed_id, "playlist item rejected: {err}"),
                }
            });
        }
        "playlist.item.group" => {
            let data = bridge::data(&event);
            let feed_id = bridge::first_text(&event, data, &["feed_id"]);
            let Some(handle) = handles.get(feed_id).cloned() else {
                return;
            };
            match segment_group_request_from_event(data) {
                Ok(request) if request.feed_id == handle.feed.id => {
                    if let Err(err) = submit_segment_group_request(
                        &handle.segment_group_mailbox,
                        &handle.segment_group_tx,
                        request,
                    ) {
                        tracing::warn!(
                            feed_id,
                            "segment group revision rejected before it could be queued: {err}"
                        );
                    }
                }
                Ok(_) => tracing::warn!(
                    feed_id,
                    "segment group feed_id did not match its target feed"
                ),
                Err(err) => tracing::warn!(feed_id, "segment group event rejected: {err}"),
            }
        }
        "cap.alert.audio.ready" => {
            let targets = feed_targets_from_event(&event);
            for handle in matching_feed_handles(handles, &targets) {
                let data = bridge::data(&event).clone();
                if handle.priority_prepare_tx.send(data).await.is_err() {
                    tracing::warn!(
                        feed_id = handle.feed.id,
                        "priority preparation queue closed before alert audio could be queued"
                    );
                }
            }
        }
        "playlist.control" => {
            let data = bridge::data(&event);
            let feed_id = bridge::first_text(&event, data, &["feed_id"]);
            let action = bridge::first_text(&event, data, &["action"]);
            dispatch_control(handles, feed_id, action).await;
        }
        "operator.breakin.start"
        | "operator.breakin.chunk"
        | "operator.breakin.finish"
        | "operator.breakin.cancel" => {
            dispatch_breakin(handles, event).await;
        }
        _ => {}
    }
}

async fn dispatch_package(
    handles: &HashMap<String, FeedHandle>,
    feed_id: &str,
    package_id: &str,
    force: bool,
) {
    let request = PackageRequest {
        package_id: package_id.to_string(),
        force,
    };
    if feed_id.is_empty() || feed_id == "*" {
        for handle in handles.values() {
            let _ = handle.request_tx.send(request.clone()).await;
        }
    } else if let Some(handle) = handles.get(feed_id) {
        let _ = handle.request_tx.send(request).await;
    }
}

async fn dispatch_control(handles: &HashMap<String, FeedHandle>, feed_id: &str, action: &str) {
    if action.trim().is_empty() {
        return;
    }
    let control = PlayoutControl {
        action: action.trim().to_ascii_lowercase(),
    };
    if feed_id.is_empty() || feed_id == "*" {
        for handle in handles.values() {
            let _ = handle.control_tx.send(control.clone()).await;
        }
    } else if let Some(handle) = handles.get(feed_id) {
        let _ = handle.control_tx.send(control).await;
    }
}

async fn dispatch_breakin(handles: &HashMap<String, FeedHandle>, event: Value) {
    let data = bridge::data(&event);
    let session_id = fallback_text(
        bridge::first_text(&event, data, &["session_id", "subject"]),
        "operator-breakin",
    );
    let command = match bridge::string_at(&event, "type") {
        "operator.breakin.start" => BreakInCommand::Start {
            id: session_id,
            title: fallback_text(
                bridge::first_text(&Value::Null, data, &["title", "header"]),
                "Operator Break-in",
            ),
        },
        "operator.breakin.chunk" => {
            let encoded = bridge::first_text(&Value::Null, data, &["data", "pcm"]);
            let Ok(pcm) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
                return;
            };
            if pcm.is_empty() {
                return;
            }
            BreakInCommand::Chunk {
                id: session_id,
                pcm,
                sample_rate: u32_at(data, "sample_rate", 48_000),
                channels: u16_at(data, "channels", 1),
            }
        }
        "operator.breakin.finish" => BreakInCommand::Finish { id: session_id },
        "operator.breakin.cancel" => BreakInCommand::Cancel { id: session_id },
        _ => return,
    };
    let targets = feed_targets_from_event(&event);
    for handle in matching_feed_handles(handles, &targets) {
        let _ = handle.breakin_tx.send(command.clone()).await;
    }
}

fn feed_targets_from_event(event: &Value) -> Vec<String> {
    let data = bridge::data(event);
    let mut out = Vec::new();
    for source in [data, event] {
        if let Some(feed_id) = source.get("feed_id").and_then(Value::as_str) {
            if !feed_id.trim().is_empty() {
                out.push(feed_id.trim().to_string());
            }
        }
        if let Some(values) = source.get("feed_ids").and_then(Value::as_array) {
            for value in values {
                if let Some(feed_id) = value.as_str() {
                    if !feed_id.trim().is_empty() {
                        out.push(feed_id.trim().to_string());
                    }
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

fn matching_feed_handles<'a>(
    handles: &'a HashMap<String, FeedHandle>,
    targets: &[String],
) -> Vec<&'a FeedHandle> {
    if targets.is_empty() || targets.iter().any(|target| target == "*") {
        return handles.values().collect();
    }
    targets
        .iter()
        .filter_map(|target| handles.get(target))
        .collect()
}

fn u32_at(value: &Value, key: &str, fallback: u32) -> u32 {
    value
        .get(key)
        .and_then(|value| {
            value
                .as_u64()
                .and_then(|raw| u32::try_from(raw).ok())
                .or_else(|| value.as_str().and_then(|raw| raw.trim().parse().ok()))
        })
        .unwrap_or(fallback)
}

fn u16_at(value: &Value, key: &str, fallback: u16) -> u16 {
    value
        .get(key)
        .and_then(|value| {
            value
                .as_u64()
                .and_then(|raw| u16::try_from(raw).ok())
                .or_else(|| value.as_str().and_then(|raw| raw.trim().parse().ok()))
        })
        .unwrap_or(fallback)
}

fn bool_at(value: &Value, key: &str) -> bool {
    value
        .get(key)
        .and_then(|value| {
            value.as_bool().or_else(|| {
                value.as_str().map(|raw| {
                    matches!(
                        raw.trim().to_ascii_lowercase().as_str(),
                        "true" | "1" | "yes" | "on"
                    )
                })
            })
        })
        .unwrap_or(false)
}

fn value_string_array(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn pcm_duration_ms(bytes: usize, sample_rate: u32, channels: u16) -> u64 {
    let frame_bytes = usize::from(channels.max(1)) * 2;
    if sample_rate == 0 || frame_bytes == 0 {
        return 0;
    }
    let frames = bytes / frame_bytes;
    ((frames as u128) * 1000 / u128::from(sample_rate)) as u64
}

fn segment_group_request_from_event(data: &Value) -> Result<SegmentGroupRequest> {
    let queue_id = required_text(data, "queue_id")?;
    let feed_id = required_text(data, "feed_id")?;
    let package_id = required_text(data, "package_id")?;
    let title = required_text(data, "title")?;
    let manifest_path = required_text(data, "manifest_path")?;
    let revision = required_u64(data, "revision")?;
    let expected_segments = required_usize(data, "expected_segments")?;
    if expected_segments == 0 || expected_segments > MAX_SEGMENTS_PER_GROUP {
        anyhow::bail!(
            "expected_segments must be between 1 and {MAX_SEGMENTS_PER_GROUP}, got {expected_segments}"
        );
    }
    let status = parse_segment_group_status(required_text(data, "status")?)?;
    Ok(SegmentGroupRequest {
        queue_id,
        feed_id,
        package_id,
        title,
        manifest_path,
        revision,
        status,
        expected_segments,
        not_before: parse_time(bridge::first_text(&Value::Null, data, &["not_before"])),
    })
}

fn required_text(value: &Value, key: &str) -> Result<String> {
    let Some(value) = value.get(key).and_then(Value::as_str) else {
        anyhow::bail!("{key} is required");
    };
    let value = value.trim();
    if value.is_empty() {
        anyhow::bail!("{key} is required");
    }
    Ok(value.to_string())
}

fn required_u64(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|raw| raw.trim().parse().ok()))
        })
        .ok_or_else(|| anyhow::anyhow!("{key} must be an unsigned integer"))
}

fn required_usize(value: &Value, key: &str) -> Result<usize> {
    let value = required_u64(value, key)?;
    usize::try_from(value).map_err(|_| anyhow::anyhow!("{key} exceeds platform limits"))
}

fn parse_segment_group_status(value: String) -> Result<SegmentGroupStatus> {
    match value.trim().to_ascii_lowercase().as_str() {
        "open" => Ok(SegmentGroupStatus::Open),
        "sealed" => Ok(SegmentGroupStatus::Sealed),
        "aborted" => Ok(SegmentGroupStatus::Aborted),
        _ => anyhow::bail!("invalid segment group status {value:?}"),
    }
}

async fn segment_group_builder(
    cfg: Arc<LoadedConfig>,
    audio_cache: Arc<AudioCache>,
    mailbox: Arc<StdMutex<SegmentGroupMailbox>>,
    mut queue_ids: mpsc::Receiver<String>,
    revisions: mpsc::Sender<SegmentGroupRevisionMessage>,
) {
    while let Some(queue_id) = queue_ids.recv().await {
        let Some(request) = take_segment_group_request(&mailbox, &queue_id) else {
            continue;
        };
        let result = if let Some(revision) = aborted_segment_group_revision(&request) {
            Ok(revision)
        } else {
            read_segment_group_revision(&cfg, &audio_cache, request.clone()).await
        };
        let message = match result {
            Ok(revision) => SegmentGroupRevisionMessage::Ready(revision),
            Err(err) => SegmentGroupRevisionMessage::Rejected {
                request,
                error: err.to_string(),
            },
        };
        if revisions.send(message).await.is_err() {
            break;
        }
    }
}

fn submit_segment_group_request(
    mailbox: &Arc<StdMutex<SegmentGroupMailbox>>,
    queue_ids: &mpsc::Sender<String>,
    request: SegmentGroupRequest,
) -> Result<()> {
    let queue_id = request.queue_id.clone();
    let mut mailbox = mailbox
        .lock()
        .map_err(|_| anyhow::anyhow!("segment group mailbox lock is poisoned"))?;
    if let Some(existing) = mailbox.latest_by_queue_id.get(&queue_id) {
        if request.revision <= existing.revision {
            return Ok(());
        }
    } else if mailbox.latest_by_queue_id.len() >= MAX_OPEN_SEGMENT_GROUPS {
        anyhow::bail!("segment group mailbox limit reached");
    }
    mailbox.latest_by_queue_id.insert(queue_id.clone(), request);
    if !mailbox.queued_ids.insert(queue_id.clone()) {
        return Ok(());
    }
    match queue_ids.try_send(queue_id.clone()) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => {
            mailbox.queued_ids.remove(&queue_id);
            mailbox.latest_by_queue_id.remove(&queue_id);
            anyhow::bail!("segment group token queue is full");
        }
        Err(TrySendError::Closed(_)) => {
            mailbox.queued_ids.remove(&queue_id);
            mailbox.latest_by_queue_id.remove(&queue_id);
            anyhow::bail!("segment group worker stopped");
        }
    }
}

fn take_segment_group_request(
    mailbox: &Arc<StdMutex<SegmentGroupMailbox>>,
    queue_id: &str,
) -> Option<SegmentGroupRequest> {
    let mut mailbox = mailbox.lock().ok()?;
    mailbox.queued_ids.remove(queue_id);
    mailbox.latest_by_queue_id.remove(queue_id)
}

fn aborted_segment_group_revision(request: &SegmentGroupRequest) -> Option<SegmentGroupRevision> {
    (request.status == SegmentGroupStatus::Aborted).then(|| SegmentGroupRevision {
        request: request.clone(),
        status: SegmentGroupStatus::Aborted,
        segments: Vec::new(),
    })
}

async fn read_segment_group_revision(
    cfg: &LoadedConfig,
    audio_cache: &AudioCache,
    mut request: SegmentGroupRequest,
) -> Result<SegmentGroupRevision> {
    let manifest_path = resolve_path(&cfg.base_dir, &request.manifest_path);
    let raw = tokio::fs::read(&manifest_path).await.with_context(|| {
        format!(
            "failed to read segment group manifest {}",
            manifest_path.display()
        )
    })?;
    let manifest: SegmentGroupManifest = serde_json::from_slice(&raw).with_context(|| {
        format!(
            "failed to parse segment group manifest {}",
            manifest_path.display()
        )
    })?;
    if !manifest.queue_id.trim().is_empty() && manifest.queue_id.trim() != request.queue_id {
        anyhow::bail!("manifest queue_id did not match the revision event");
    }
    if manifest
        .expected_segments
        .is_some_and(|expected| expected != request.expected_segments)
    {
        anyhow::bail!("manifest expected_segments did not match the revision event");
    }
    promote_request_to_manifest_revision(&mut request, manifest.revision)?;
    let status = parse_segment_group_status(manifest.status)?;
    if manifest.sample_rate == 0 || manifest.channels == 0 {
        anyhow::bail!("manifest sample_rate and channels must be positive");
    }
    if manifest.segments.len() > request.expected_segments {
        anyhow::bail!("manifest contains more segments than expected_segments");
    }
    if status == SegmentGroupStatus::Sealed && manifest.segments.len() != request.expected_segments
    {
        anyhow::bail!("sealed manifest does not contain every expected segment");
    }

    let mut segments = Vec::with_capacity(manifest.segments.len());
    for (expected_index, segment) in manifest.segments.into_iter().enumerate() {
        if segment.index != expected_index {
            anyhow::bail!("manifest segments must be a contiguous prefix starting at index zero");
        }
        if segment.audio_path.trim().is_empty() {
            anyhow::bail!("segment {expected_index} is missing audio_path");
        }
        if segment.duration_ms == 0 || segment.duration_ms > MAX_SEGMENT_DURATION_MS {
            anyhow::bail!("segment {expected_index} has an invalid duration_ms");
        }
        if segment.pause_after_ms > MAX_SEGMENT_PAUSE_MS {
            anyhow::bail!("segment {expected_index} has an excessive pause_after_ms");
        }
        let path = resolve_path(&cfg.base_dir, &segment.audio_path);
        let pcm = audio_cache
            .read_raw_pcm(&path, manifest.sample_rate, manifest.channels, cfg)
            .await?;
        let actual_duration_ms = pcm_duration_ms(
            pcm.len(),
            cfg.root.playout.sample_rate,
            cfg.root.playout.channels,
        );
        if actual_duration_ms.abs_diff(segment.duration_ms) > 250 {
            anyhow::bail!("segment {expected_index} duration does not match its PCM payload");
        }
        segments.push(SegmentGroupSegment {
            index: expected_index,
            audio_path: segment.audio_path,
            duration_ms: segment.duration_ms,
            pause_after_ms: segment.pause_after_ms,
            pcm,
        });
    }
    Ok(SegmentGroupRevision {
        request,
        status,
        segments,
    })
}

fn promote_request_to_manifest_revision(
    request: &mut SegmentGroupRequest,
    manifest_revision: u64,
) -> Result<()> {
    if manifest_revision == 0 {
        anyhow::bail!("manifest revision must be positive");
    }
    if manifest_revision < request.revision {
        anyhow::bail!("manifest revision is older than the revision event");
    }
    request.revision = manifest_revision;
    Ok(())
}

impl SegmentGroupPlayback {
    fn new(revision: &SegmentGroupRevision) -> Self {
        let request = &revision.request;
        Self {
            group: Arc::new(SegmentGroup {
                queue_id: request.queue_id.clone(),
                feed_id: request.feed_id.clone(),
                package_id: request.package_id.clone(),
                title: request.title.clone(),
                manifest_path: request.manifest_path.clone(),
                expected_segments: request.expected_segments,
                not_before: request.not_before,
            }),
            last_revision: None,
            status: SegmentGroupStatus::Open,
            segments: vec![None; request.expected_segments],
            segment_index: 0,
            segment_offset: 0,
            pause_remaining_bytes: 0,
            admitted: false,
            admission_pending: false,
            admission_item: None,
        }
    }

    fn apply_revision(&mut self, revision: SegmentGroupRevision) -> Result<()> {
        if self
            .last_revision
            .is_some_and(|last_revision| revision.request.revision <= last_revision)
        {
            return Ok(());
        }
        if self.group.feed_id != revision.request.feed_id
            || self.group.package_id != revision.request.package_id
            || self.group.title != revision.request.title
            || self.group.manifest_path != revision.request.manifest_path
            || self.group.expected_segments != revision.request.expected_segments
            || self.group.not_before != revision.request.not_before
        {
            anyhow::bail!("segment group identity changed across revisions");
        }
        if self.status == SegmentGroupStatus::Aborted {
            self.last_revision = Some(revision.request.revision);
            return Ok(());
        }
        if self.status == SegmentGroupStatus::Sealed
            && revision.status != SegmentGroupStatus::Sealed
        {
            anyhow::bail!("sealed segment group cannot reopen");
        }
        for segment in revision.segments {
            let Some(slot) = self.segments.get_mut(segment.index) else {
                anyhow::bail!("segment index exceeded expected_segments");
            };
            if let Some(previous) = slot {
                if previous.audio_path != segment.audio_path
                    || previous.duration_ms != segment.duration_ms
                    || previous.pause_after_ms != segment.pause_after_ms
                {
                    anyhow::bail!("segment entries must remain immutable across revisions");
                }
            } else {
                *slot = Some(segment);
            }
        }
        if revision.status == SegmentGroupStatus::Sealed
            && self.segments.iter().any(Option::is_none)
        {
            anyhow::bail!("sealed segment group is missing a final segment");
        }
        self.status = revision.status;
        self.last_revision = Some(revision.request.revision);
        Ok(())
    }

    fn ready_duration_ms(&self) -> u64 {
        self.segments
            .iter()
            .take_while(|segment| segment.is_some())
            .filter_map(|segment| segment.as_ref())
            .fold(0u64, |duration, segment| {
                duration
                    .saturating_add(segment.duration_ms)
                    .saturating_add(segment.pause_after_ms)
            })
    }

    fn ready_prefix_len(&self) -> usize {
        self.segments
            .iter()
            .take_while(|segment| segment.is_some())
            .count()
    }

    fn can_admit(&self) -> bool {
        if self.status == SegmentGroupStatus::Aborted || self.admitted || self.admission_pending {
            return false;
        }
        if self.status == SegmentGroupStatus::Sealed {
            return self.ready_prefix_len() == self.group.expected_segments;
        }
        self.ready_prefix_len() >= GROUP_ADMISSION_MIN_SEGMENTS
            && self.ready_duration_ms() >= GROUP_ADMISSION_MIN_READY_MS
    }
}

fn apply_segment_group_revision(
    cfg: &LoadedConfig,
    groups: &mut HashMap<String, SegmentGroupPlayback>,
    revision: SegmentGroupRevision,
) -> Result<bool> {
    let queue_id = revision.request.queue_id.clone();
    if let Some(group) = groups.get_mut(&queue_id) {
        if group
            .last_revision
            .is_some_and(|last_revision| revision.request.revision <= last_revision)
        {
            return Ok(false);
        }
        group.apply_revision(revision)?;
        if group.can_admit() {
            group.admission_pending = true;
            group.admission_item = Some(audio_item_from_segment_group(cfg, group));
            return Ok(true);
        }
        return Ok(false);
    }
    if revision.status == SegmentGroupStatus::Aborted {
        return Ok(false);
    }
    ensure_segment_group_capacity(groups)?;
    let mut group = SegmentGroupPlayback::new(&revision);
    group.apply_revision(revision)?;
    let should_admit = group.can_admit();
    if should_admit {
        group.admission_pending = true;
        group.admission_item = Some(audio_item_from_segment_group(cfg, &group));
    }
    groups.insert(queue_id, group);
    Ok(should_admit)
}

fn ensure_segment_group_capacity(groups: &HashMap<String, SegmentGroupPlayback>) -> Result<()> {
    if groups.len() >= MAX_OPEN_SEGMENT_GROUPS {
        anyhow::bail!("open segment group limit reached");
    }
    Ok(())
}

fn audio_item_from_segment_group(cfg: &LoadedConfig, group: &SegmentGroupPlayback) -> AudioItem {
    AudioItem {
        id: group.group.queue_id.clone(),
        package_id: group.group.package_id.clone(),
        title: group.group.title.clone(),
        pcm: SharedPcm::owned(Vec::new()),
        metadata: AudioItemMetadata {
            audio_path: group.group.manifest_path.clone(),
            duration_ms: group.ready_duration_ms(),
            ..Default::default()
        },
        gap_after: package_gap(cfg),
        not_before: group.group.not_before,
        queued_at: Utc::now().to_rfc3339(),
        target_start: String::new(),
        predicted_start: String::new(),
        predicted_finish: String::new(),
        source: ItemSource::Playlist,
        segment_group: Some(Arc::clone(&group.group)),
    }
}

fn try_admit_segment_groups(
    audio_tx: &mpsc::Sender<AudioItem>,
    groups: &mut HashMap<String, SegmentGroupPlayback>,
    order: &mut VecDeque<String>,
) {
    while audio_tx.capacity() > 0 {
        let Some(queue_id) = order.pop_front() else {
            break;
        };
        let Some(group) = groups.get_mut(&queue_id) else {
            continue;
        };
        if group.status == SegmentGroupStatus::Aborted {
            if !group.admitted {
                groups.remove(&queue_id);
            }
            continue;
        }
        let Some(item) = group.admission_item.take() else {
            continue;
        };
        match audio_tx.try_send(item) {
            Ok(()) => {
                group.admitted = true;
                group.admission_pending = false;
            }
            Err(TrySendError::Full(item)) => {
                group.admission_item = Some(item);
                order.push_front(queue_id);
                break;
            }
            Err(TrySendError::Closed(_)) => break,
        }
    }
}

fn abort_segment_group(
    groups: &mut HashMap<String, SegmentGroupPlayback>,
    queue_id: &str,
    revision: u64,
) {
    if let Some(group) = groups.get_mut(queue_id) {
        if group
            .last_revision
            .is_some_and(|last_revision| last_revision > revision)
        {
            return;
        }
        group.status = SegmentGroupStatus::Aborted;
        group.admission_pending = false;
        group.admission_item = None;
    }
}

fn segment_group_is_aborted(
    item: &AudioItem,
    groups: &HashMap<String, SegmentGroupPlayback>,
) -> bool {
    item.segment_group.is_some()
        && groups
            .get(&item.id)
            .is_none_or(|group| group.status == SegmentGroupStatus::Aborted)
}

fn take_aborted_current_group(
    current: &mut Option<AudioItem>,
    groups: &HashMap<String, SegmentGroupPlayback>,
) -> Option<AudioItem> {
    current
        .as_ref()
        .is_some_and(|item| segment_group_is_aborted(item, groups))
        .then(|| current.take())
        .flatten()
}

fn take_aborted_pending_group(
    pending: &mut Option<AudioItem>,
    groups: &HashMap<String, SegmentGroupPlayback>,
) -> Option<AudioItem> {
    pending
        .as_ref()
        .is_some_and(|item| segment_group_is_aborted(item, groups))
        .then(|| pending.take())
        .flatten()
}

fn remove_aborted_interrupted_groups(
    interrupted: &mut Option<AudioItem>,
    deferred: &mut VecDeque<AudioItem>,
    active_ids: &mut HashSet<String>,
    groups: &mut HashMap<String, SegmentGroupPlayback>,
) {
    if interrupted
        .as_ref()
        .is_some_and(|item| segment_group_is_aborted(item, groups))
    {
        if let Some(item) = interrupted.take() {
            active_ids.remove(&item.id);
            groups.remove(&item.id);
        }
    }
    deferred.retain(|item| {
        if segment_group_is_aborted(item, groups) {
            active_ids.remove(&item.id);
            groups.remove(&item.id);
            false
        } else {
            true
        }
    });
}

fn copy_segment_group_pcm(
    queue_id: &str,
    groups: &mut HashMap<String, SegmentGroupPlayback>,
    sample_rate: u32,
    channels: u16,
    out: &mut [u8],
) -> SegmentGroupRender {
    let Some(group) = groups.get_mut(queue_id) else {
        return SegmentGroupRender::Aborted;
    };
    if group.status == SegmentGroupStatus::Aborted {
        return SegmentGroupRender::Aborted;
    }
    let mut out_offset = 0usize;
    while out_offset < out.len() {
        if group.pause_remaining_bytes > 0 {
            let pause_bytes = (out.len() - out_offset).min(group.pause_remaining_bytes);
            group.pause_remaining_bytes -= pause_bytes;
            out_offset += pause_bytes;
            continue;
        }
        if group.segment_index >= group.group.expected_segments {
            return if group.status == SegmentGroupStatus::Sealed {
                SegmentGroupRender::Completed
            } else {
                SegmentGroupRender::Underflow
            };
        }
        let Some(segment) = group.segments[group.segment_index].as_ref() else {
            return SegmentGroupRender::Underflow;
        };
        let start = group.segment_offset.min(segment.pcm.len());
        let available = segment.pcm.len().saturating_sub(start);
        if available == 0 {
            group.segment_index += 1;
            group.segment_offset = 0;
            group.pause_remaining_bytes =
                pause_ms_to_pcm_bytes(segment.pause_after_ms, sample_rate, channels);
            continue;
        }
        let copied = available.min(out.len() - out_offset);
        out[out_offset..out_offset + copied].copy_from_slice(&segment.pcm[start..start + copied]);
        group.segment_offset = start + copied;
        out_offset += copied;
    }
    SegmentGroupRender::Playing
}

fn pause_ms_to_pcm_bytes(pause_ms: u64, sample_rate: u32, channels: u16) -> usize {
    let frame_bytes = u128::from(channels.max(1)) * 2;
    let bytes = u128::from(pause_ms)
        .saturating_mul(u128::from(sample_rate))
        .saturating_mul(frame_bytes)
        / 1_000;
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

impl FeedHandle {
    fn spawn(
        cfg: Arc<LoadedConfig>,
        client: BridgeClient,
        media_client: BridgeClient,
        feed: FeedConfig,
        alert_poll: Duration,
        audio_cache: Arc<AudioCache>,
        tasks: &SessionTasks,
    ) -> Self {
        let (audio_tx, audio_rx) = mpsc::channel(ROUTINE_AUDIO_QUEUE_CAPACITY);
        let (priority_tx, priority_rx) = mpsc::channel(PRIORITY_AUDIO_QUEUE_CAPACITY);
        let (priority_prepare_tx, priority_prepare_rx) =
            mpsc::channel(PRIORITY_PREPARE_QUEUE_CAPACITY);
        let (breakin_tx, breakin_rx) = mpsc::channel(BREAKIN_COMMAND_QUEUE_CAPACITY);
        let (request_tx, request_rx) = mpsc::channel(PACKAGE_REQUEST_QUEUE_CAPACITY);
        let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE_CAPACITY);
        let segment_group_mailbox = Arc::new(StdMutex::new(SegmentGroupMailbox::default()));
        let (segment_group_tx, segment_group_rx) = mpsc::channel(MAX_OPEN_SEGMENT_GROUPS);
        let (segment_group_revision_tx, segment_group_revision_rx) =
            mpsc::channel(SEGMENT_GROUP_REVISION_QUEUE_CAPACITY);

        tasks.spawn(package_builder(
            Arc::clone(&cfg),
            client.clone(),
            feed.clone(),
            Arc::clone(&audio_cache),
            request_rx,
            audio_tx.clone(),
        ));
        tasks.spawn(priority_builder(
            Arc::clone(&cfg),
            feed.clone(),
            Arc::clone(&audio_cache),
            priority_prepare_rx,
            priority_tx.clone(),
        ));
        tasks.spawn(segment_group_builder(
            Arc::clone(&cfg),
            Arc::clone(&audio_cache),
            Arc::clone(&segment_group_mailbox),
            segment_group_rx,
            segment_group_revision_tx,
        ));

        if feed.same_enabled() {
            tasks.spawn(alert_scanner(
                Arc::clone(&cfg),
                client.clone(),
                feed.clone(),
                Arc::clone(&audio_cache),
                priority_tx.clone(),
                alert_poll,
            ));
        }

        if feed.routine_enabled() && !cfg.root.services.go.playlist.enabled {
            if let Some(package_id) = cfg.routine_playlist_order().first() {
                let _ = request_tx.try_send(PackageRequest {
                    package_id: package_id.clone(),
                    force: true,
                });
            }
        }

        tasks.spawn_cooperative(
            FeedRunner {
                cfg,
                client,
                media_client,
                feed: feed.clone(),
                audio_rx,
                audio_tx: audio_tx.clone(),
                segment_group_revision_rx,
                priority_rx,
                breakin_rx,
                control_rx,
                sinks: Vec::new(),
                after_current_action: None,
                paused: false,
                tasks: tasks.clone(),
            }
            .run(tasks.cancellation.clone()),
        );

        Self {
            feed,
            audio_tx,
            segment_group_mailbox,
            segment_group_tx,
            priority_prepare_tx,
            breakin_tx,
            request_tx,
            control_tx,
        }
    }
}

struct FeedRunner {
    cfg: Arc<LoadedConfig>,
    client: BridgeClient,
    media_client: BridgeClient,
    feed: FeedConfig,
    audio_rx: mpsc::Receiver<AudioItem>,
    audio_tx: mpsc::Sender<AudioItem>,
    segment_group_revision_rx: mpsc::Receiver<SegmentGroupRevisionMessage>,
    priority_rx: mpsc::Receiver<AudioItem>,
    breakin_rx: mpsc::Receiver<BreakInCommand>,
    control_rx: mpsc::Receiver<PlayoutControl>,
    sinks: Vec<Box<dyn Sink>>,
    after_current_action: Option<AfterCurrentAction>,
    paused: bool,
    tasks: SessionTasks,
}

impl FeedRunner {
    async fn run(mut self, cancellation: CancellationToken) {
        self.sinks = sinks_for_feed(&self.cfg, &self.feed);
        let publish_buffer_capacity = media_publish_buffer_capacity(
            self.cfg.root.playout.sample_rate,
            self.cfg.root.playout.channels,
        );
        let pcm_publisher = spawn_pcm_publisher(
            &self.tasks,
            self.media_client.clone(),
            self.feed.id.clone(),
            self.cfg.root.playout.sample_rate,
            self.cfg.root.playout.channels,
            publish_buffer_capacity,
        );
        let ready = tokio::select! {
            _ = cancellation.cancelled() => false,
            _ = update_runtime(&self.cfg, &self.feed.id, "Idle") => true,
        };
        if ready {
            tokio::select! {
                _ = cancellation.cancelled() => {}
                _ = self.pump(pcm_publisher, publish_buffer_capacity) => {}
            }
        }
        for sink in &mut self.sinks {
            if let Err(err) = sink.close() {
                tracing::warn!(
                    feed_id = self.feed.id,
                    sink = sink.name(),
                    "failed to close playout sink: {err}"
                );
            }
        }
    }

    async fn pump(&mut self, mut pcm_publisher: PcmPublisher, publish_buffer_capacity: usize) {
        let sample_rate = self.cfg.root.playout.sample_rate;
        let channels = self.cfg.root.playout.channels;
        let chunk = silence_chunk(sample_rate, channels, PCM_CHUNK_MS);
        let publish_chunk_count = (MEDIA_PUBLISH_CHUNK_MS / PCM_CHUNK_MS).max(1) as usize;
        debug_assert_eq!(publish_buffer_capacity, chunk.len() * publish_chunk_count);
        let mut publish_builder = PcmPublishBuilder::new(publish_buffer_capacity);
        let mut last_publish_context: Option<PcmMediaContext> = None;
        let mut next_pts_frames = 0u64;
        let chunk_interval = Duration::from_millis(u64::from(PCM_CHUNK_MS));
        let mut ticker = interval(chunk_interval);
        ticker.set_missed_tick_behavior(realtime_tick_missed_behavior());
        let mut last_media_tick = Instant::now() - chunk_interval;
        let mut media_remainder = Duration::from_millis(0);
        let mut last_lag_log = Instant::now() - Duration::from_secs(60);
        let mut dropped_pcm_publishes = 0u64;
        let mut current: Option<AudioItem> = None;
        let mut pending: Option<AudioItem> = None;
        let mut priority_pending = VecDeque::<AudioItem>::new();
        let mut interrupted_priority: Option<AudioItem> = None;
        let mut interrupted_routine: Option<AudioItem> = None;
        let mut active_priority_ids = HashSet::<String>::new();
        let mut active_routine_ids = HashSet::<String>::new();
        let mut completed_routine_ids = HashSet::<String>::new();
        let mut completed_routine_order = VecDeque::<String>::new();
        let mut deferred_routine = VecDeque::<AudioItem>::new();
        let mut segment_groups = HashMap::<String, SegmentGroupPlayback>::new();
        let mut segment_group_admission_order = VecDeque::<String>::new();
        let mut live_breakin: Option<LiveBreakIn> = None;
        let mut out = vec![0u8; chunk.len()];
        let mut position = 0usize;
        let mut gap_until = Instant::now();

        loop {
            tokio::select! {
                Some(message) = self.segment_group_revision_rx.recv() => {
                    match message {
                        SegmentGroupRevisionMessage::Ready(revision) => {
                            let queue_id = revision.request.queue_id.clone();
                            let revision_number = revision.request.revision;
                            if completed_routine_ids.contains(&queue_id) {
                                continue;
                            }
                            match apply_segment_group_revision(&self.cfg, &mut segment_groups, revision) {
                                Ok(true) => {
                                    if segment_group_admission_order.len() < MAX_OPEN_SEGMENT_GROUPS {
                                        segment_group_admission_order.push_back(queue_id);
                                    } else if let Some(group) = segment_groups.get_mut(&queue_id) {
                                        group.admission_pending = false;
                                        tracing::warn!(
                                            feed_id = self.feed.id,
                                            queue_id,
                                            "segment group admission queue is full"
                                        );
                                    }
                                }
                                Ok(false) => {}
                                Err(err) => {
                                    tracing::warn!(
                                        feed_id = self.feed.id,
                                        queue_id,
                                        "segment group manifest revision rejected: {err}"
                                    );
                                    abort_segment_group(
                                        &mut segment_groups,
                                        &queue_id,
                                        revision_number,
                                    );
                                }
                            }
                        }
                        SegmentGroupRevisionMessage::Rejected { request, error } => {
                            tracing::warn!(
                                feed_id = self.feed.id,
                                queue_id = request.queue_id,
                                "segment group manifest rejected: {error}"
                            );
                            abort_segment_group(
                                &mut segment_groups,
                                &request.queue_id,
                                request.revision,
                            );
                        }
                    }
                }
                Some(control) = self.control_rx.recv() => {
                    let clears_deferred = clears_deferred_routine(&control.action);
                    self.apply_control(&control.action, current.as_ref());
                    if clears_deferred {
                        self.after_current_action = None;
                        clear_deferred_routine_state(
                            &mut interrupted_routine,
                            &mut deferred_routine,
                            &mut active_routine_ids,
                            &mut completed_routine_ids,
                            &mut completed_routine_order,
                        );
                    }
                }
                Some(command) = self.breakin_rx.recv() => {
                    match command {
                        BreakInCommand::Start { id, title } => {
                            if let Some(done) = live_breakin.take() {
                                spawn_complete_live_breakin(
                                    &self.tasks,
                                    self.client.clone(),
                                    Arc::clone(&self.cfg),
                                    self.feed.id.clone(),
                                    done,
                                    true,
                                );
                            }
                            if let Some(item) = current.take() {
                                if item.is_alert() {
                                    spawn_interrupt_item(
                                        &self.tasks,
                                        self.client.clone(),
                                        self.feed.id.clone(),
                                        item.clone(),
                                    );
                                    debug_assert!(interrupted_priority.is_none());
                                    interrupted_priority = Some(item);
                                } else {
                                    spawn_interrupt_item(
                                        &self.tasks,
                                        self.client.clone(),
                                        self.feed.id.clone(),
                                        item.clone(),
                                    );
                                    remember_interrupted_routine(
                                        &mut interrupted_routine,
                                        &mut deferred_routine,
                                        &mut active_routine_ids,
                                        &self.feed.id,
                                        item,
                                    );
                                }
                            }
                            if let Some(item) = pending.take() {
                                if item.is_alert() {
                                    enqueue_priority_front(
                                        &mut priority_pending,
                                        &mut active_priority_ids,
                                        &self.feed.id,
                                        item,
                                    );
                                } else {
                                    defer_routine_back(
                                        &mut deferred_routine,
                                        &mut active_routine_ids,
                                        &self.feed.id,
                                        item,
                                    );
                                }
                            }
                            position = 0;
                            gap_until = Instant::now();
                            let live = LiveBreakIn {
                                id,
                                title,
                                buffer: VecDeque::new(),
                                finishing: false,
                            };
                            start_live_breakin(&self.client, &self.cfg, &self.feed.id, &live).await;
                            live_breakin = Some(live);
                        }
                        BreakInCommand::Chunk { id, pcm, sample_rate, channels } => {
                            if let Some(live) = live_breakin.as_mut().filter(|live| live.id == id) {
                                let pcm = normalize_pcm(
                                    Pcm {
                                        sample_rate,
                                        channels: channels.max(1),
                                        data: pcm,
                                    },
                                    self.cfg.root.playout.sample_rate,
                                    self.cfg.root.playout.channels,
                                );
                                live.buffer.extend(pcm.data);
                                trim_live_breakin_buffer(
                                    live,
                                    self.cfg.root.playout.sample_rate,
                                    self.cfg.root.playout.channels,
                                );
                            }
                        }
                        BreakInCommand::Finish { id } => {
                            if let Some(live) = live_breakin.as_mut().filter(|live| live.id == id) {
                                live.finishing = true;
                            }
                        }
                        BreakInCommand::Cancel { id } => {
                            if live_breakin.as_ref().is_some_and(|live| live.id == id) {
                                if let Some(done) = live_breakin.take() {
                                    spawn_complete_live_breakin(
                                        &self.tasks,
                                        self.client.clone(),
                                        Arc::clone(&self.cfg),
                                        self.feed.id.clone(),
                                        done,
                                        true,
                                    );
                                }
                            }
                        }
                    }
                }
                _ = ticker.tick() => {
                    let tick_now = Instant::now();
                    let elapsed = tick_now.saturating_duration_since(last_media_tick);
                    last_media_tick = tick_now;
                    let (chunks_due, dropped_backlog) =
                        realtime_chunks_due(&mut media_remainder, elapsed);
                    if dropped_backlog > Duration::ZERO {
                        flush_pcm_publish(
                            &mut publish_builder,
                            &mut pcm_publisher,
                            publish_buffer_capacity,
                            &self.feed.id,
                            &mut dropped_pcm_publishes,
                        );
                        next_pts_frames = next_pts_frames.saturating_add(
                            duration_sample_frames(dropped_backlog, sample_rate),
                        );
                        publish_builder.mark_discontinuity();
                    }
                    if dropped_backlog > Duration::ZERO
                        && dropped_backlog >= realtime_lag_warn_backlog()
                        && last_lag_log.elapsed() >= Duration::from_secs(10)
                    {
                        tracing::warn!(
                            feed_id = self.feed.id,
                            elapsed_ms = elapsed.as_millis(),
                            backlog_ms = dropped_backlog.as_millis(),
                            "playout tick lagged; dropping missed realtime audio instead of bursting stale chunks"
                        );
                        last_lag_log = Instant::now();
                    }

                    debug_assert_eq!(chunks_due, 1);
                    {
                        let now = Utc::now();
                        try_admit_segment_groups(
                            &self.audio_tx,
                            &mut segment_groups,
                            &mut segment_group_admission_order,
                        );
                        if let Some(item) = take_aborted_current_group(&mut current, &segment_groups) {
                            active_routine_ids.remove(&item.id);
                            segment_groups.remove(&item.id);
                            spawn_interrupt_item(
                                &self.tasks,
                                self.client.clone(),
                                self.feed.id.clone(),
                                item,
                            );
                            position = 0;
                            gap_until = tick_now;
                            spawn_update_runtime(
                                &self.tasks,
                                Arc::clone(&self.cfg),
                                self.feed.id.clone(),
                                "Idle".to_string(),
                            );
                        }
                        if let Some(item) = take_aborted_pending_group(&mut pending, &segment_groups) {
                            active_routine_ids.remove(&item.id);
                            segment_groups.remove(&item.id);
                            spawn_interrupt_item(
                                &self.tasks,
                                self.client.clone(),
                                self.feed.id.clone(),
                                item,
                            );
                        }
                        remove_aborted_interrupted_groups(
                            &mut interrupted_routine,
                            &mut deferred_routine,
                            &mut active_routine_ids,
                            &mut segment_groups,
                        );
                        if let Some(done) = take_finished_breakin(&mut live_breakin) {
                            spawn_complete_live_breakin(
                                &self.tasks,
                                self.client.clone(),
                                Arc::clone(&self.cfg),
                                self.feed.id.clone(),
                                done,
                                false,
                            );
                        }
                        for item in drain_priority_receiver(
                            &mut self.priority_rx,
                            &mut priority_pending,
                            &mut active_priority_ids,
                            &self.feed.id,
                        ) {
                            spawn_accept_item(
                                &self.tasks,
                                self.client.clone(),
                                self.feed.id.clone(),
                                item.clone(),
                            );
                        }
                        let priority_ready =
                            interrupted_priority.is_some() || !priority_pending.is_empty();
                        if live_breakin.is_none() && priority_ready
                            && current.as_ref().is_some_and(|item| !item.is_alert())
                        {
                            if let Some(item) = current.take() {
                                spawn_interrupt_item(
                                    &self.tasks,
                                    self.client.clone(),
                                    self.feed.id.clone(),
                                    item.clone(),
                                );
                                remember_interrupted_routine(
                                    &mut interrupted_routine,
                                    &mut deferred_routine,
                                    &mut active_routine_ids,
                                    &self.feed.id,
                                    item,
                                );
                            }
                        }
                        if live_breakin.is_none() && priority_ready
                            && pending.as_ref().is_some_and(|item| !item.is_alert())
                        {
                            defer_pending_routine(
                                &mut pending,
                                &mut deferred_routine,
                                &mut active_routine_ids,
                                &self.feed.id,
                            );
                        }
                        gap_until = priority_gap_deadline(
                            gap_until,
                            live_breakin.is_none() && priority_ready,
                            tick_now,
                        );
                        if live_breakin.is_none() && current.is_none() && pending.is_none() {
                            pending = take_next_buffered_item(
                                &mut interrupted_priority,
                                &mut priority_pending,
                                &mut interrupted_routine,
                                &mut deferred_routine,
                                self.paused,
                            );
                            if pending.as_ref().is_some_and(AudioItem::is_alert) {
                                gap_until = tick_now;
                            } else if pending.is_none() && !self.paused {
                                if let Some(item) = next_unique_routine_item(
                                    &mut self.audio_rx,
                                    &mut active_routine_ids,
                                    &completed_routine_ids,
                                    &mut segment_groups,
                                    &self.feed.id,
                                ) {
                                    spawn_accept_item(
                                        &self.tasks,
                                        self.client.clone(),
                                        self.feed.id.clone(),
                                        item.clone(),
                                    );
                                    pending = Some(item);
                                }
                            }
                        }
                        let pending_can_start = pending
                            .as_ref()
                            .is_some_and(|item| item_can_start(item, self.paused));
                        if live_breakin.is_none()
                            && current.is_none()
                            && pending_can_start
                            && tick_now >= gap_until
                        {
                            if let Some(item) = pending.as_ref() {
                                if item.not_before.is_none_or(|not_before| now >= not_before) {
                                    current = pending.take();
                                    if let Some(item) = current.as_ref() {
                                        position = 0;
                                        spawn_start_item(
                                            &self.tasks,
                                            self.client.clone(),
                                            Arc::clone(&self.cfg),
                                            self.feed.id.clone(),
                                            item.clone(),
                                        );
                                    }
                                }
                            }
                        }

                        out.fill(0);
                        let mut breakin_drained = false;
                        let mut segment_group_render = SegmentGroupRender::Playing;
                        if let Some(live) = live_breakin.as_mut() {
                            drain_live_breakin_buffer(live, &mut out);
                            if live.finishing && live.buffer.is_empty() {
                                breakin_drained = true;
                            }
                        } else if let Some(item) = current.as_ref() {
                            if item.segment_group.is_some() {
                                segment_group_render = copy_segment_group_pcm(
                                    &item.id,
                                    &mut segment_groups,
                                    sample_rate,
                                    channels,
                                    &mut out,
                                );
                            } else {
                                copy_item_pcm(item, &mut position, &mut out);
                            }
                        }
                        let publish_context =
                            PcmMediaContext::active(live_breakin.as_ref(), current.as_ref());
                        let completed_breakin = if breakin_drained {
                            live_breakin.take()
                        } else {
                            None
                        };

                        for sink in &mut self.sinks {
                            if let Err(err) = sink.write(&out) {
                                tracing::warn!(feed_id = self.feed.id, sink = sink.name(), "sink write failed: {err}");
                            }
                        }
                        if last_publish_context
                            .as_ref()
                            .is_some_and(|previous| previous != &publish_context)
                        {
                            flush_pcm_publish(
                                &mut publish_builder,
                                &mut pcm_publisher,
                                publish_buffer_capacity,
                                &self.feed.id,
                                &mut dropped_pcm_publishes,
                            );
                            publish_builder.mark_discontinuity();
                        }
                        last_publish_context = Some(publish_context.clone());
                        let pts_ns = sample_frames_to_ns(next_pts_frames, sample_rate);
                        publish_builder.push(
                            &out,
                            PCM_CHUNK_MS,
                            pts_ns,
                            publish_context,
                        );
                        next_pts_frames = next_pts_frames.saturating_add(pcm_frame_count(
                            out.len(),
                            channels,
                        ));
                        if publish_builder.duration_ms() >= MEDIA_PUBLISH_CHUNK_MS {
                            flush_pcm_publish(
                                &mut publish_builder,
                                &mut pcm_publisher,
                                publish_buffer_capacity,
                                &self.feed.id,
                                &mut dropped_pcm_publishes,
                            );
                        }
                        if let Some(done) = completed_breakin {
                            spawn_complete_live_breakin(
                                &self.tasks,
                                self.client.clone(),
                                Arc::clone(&self.cfg),
                                self.feed.id.clone(),
                                done,
                                false,
                            );
                        }

                        let current_completed = current.as_ref().is_some_and(|item| {
                            if item.segment_group.is_some() {
                                segment_group_render == SegmentGroupRender::Completed
                            } else {
                                position >= item.pcm.len()
                            }
                        });
                        let current_aborted = current.as_ref().is_some_and(|item| {
                            item.segment_group.is_some()
                                && segment_group_render == SegmentGroupRender::Aborted
                        });
                        if live_breakin.is_none() && current_aborted {
                            if let Some(item) = current.take() {
                                active_routine_ids.remove(&item.id);
                                segment_groups.remove(&item.id);
                                spawn_interrupt_item(
                                    &self.tasks,
                                    self.client.clone(),
                                    self.feed.id.clone(),
                                    item,
                                );
                                position = 0;
                                gap_until = tick_now;
                                spawn_update_runtime(
                                    &self.tasks,
                                    Arc::clone(&self.cfg),
                                    self.feed.id.clone(),
                                    "Idle".to_string(),
                                );
                            }
                        } else if live_breakin.is_none() && current_completed {
                            if let Some(item) = current.take() {
                                let completed_item_id = item.id.clone();
                                if item.is_alert() {
                                    active_priority_ids.remove(&item.id);
                                } else {
                                    active_routine_ids.remove(&item.id);
                                    if item.segment_group.is_some() {
                                        segment_groups.remove(&item.id);
                                    }
                                    remember_completed_routine_item(
                                        &mut completed_routine_ids,
                                        &mut completed_routine_order,
                                        &item.id,
                                    );
                                }
                                let gap_after = item.gap_after;
                                spawn_finish_item(
                                    &self.tasks,
                                    self.client.clone(),
                                    self.feed.id.clone(),
                                    item,
                                );
                                gap_until = tick_now + gap_after;
                                spawn_update_runtime(
                                    &self.tasks,
                                    Arc::clone(&self.cfg),
                                    self.feed.id.clone(),
                                    "Idle".to_string(),
                                );
                                if let Some(action) = take_after_current_action(
                                    &mut self.after_current_action,
                                    &completed_item_id,
                                ) {
                                    let clears_deferred = clears_deferred_routine(&action);
                                    self.apply_control(&action, None);
                                    if clears_deferred {
                                        clear_deferred_routine_state(
                                            &mut interrupted_routine,
                                            &mut deferred_routine,
                                            &mut active_routine_ids,
                                            &mut completed_routine_ids,
                                            &mut completed_routine_order,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                else => break,
            }
        }
    }

    fn apply_control(&mut self, action: &str, current: Option<&AudioItem>) {
        match action {
            "pause" => self.paused = true,
            "resume" => self.paused = false,
            "restart" => {
                self.paused = false;
                self.drain_audio_queue();
            }
            "flush" | "flush_pending" => self.drain_audio_queue(),
            "flush_restart" => {
                self.paused = false;
                self.drain_audio_queue();
            }
            "flush_stop" => {
                self.paused = true;
                self.drain_audio_queue();
            }
            "pause_after_current" | "flush_restart_after_current" | "flush_stop_after_current" => {
                if let Some(current) = current {
                    self.after_current_action = Some(AfterCurrentAction {
                        action: action.to_string(),
                        target_item_id: current.id.clone(),
                    });
                } else {
                    self.apply_control(action.trim_end_matches("_after_current"), None);
                }
            }
            _ => {}
        }
    }

    fn drain_audio_queue(&mut self) {
        while self.audio_rx.try_recv().is_ok() {}
    }
}

impl AudioItem {
    fn is_alert(&self) -> bool {
        matches!(self.source, ItemSource::Alert { .. })
    }
}

fn item_can_start(item: &AudioItem, paused: bool) -> bool {
    item.is_alert() || !paused
}

fn take_after_current_action(
    deferred: &mut Option<AfterCurrentAction>,
    completed_item_id: &str,
) -> Option<String> {
    if deferred
        .as_ref()
        .is_some_and(|action| action.target_item_id == completed_item_id)
    {
        deferred.take().map(|action| action.action)
    } else {
        None
    }
}

fn priority_gap_deadline(gap_until: Instant, priority_ready: bool, now: Instant) -> Instant {
    if priority_ready {
        now
    } else {
        gap_until
    }
}

fn take_next_buffered_item(
    interrupted_priority: &mut Option<AudioItem>,
    priority_pending: &mut VecDeque<AudioItem>,
    interrupted_routine: &mut Option<AudioItem>,
    deferred_routine: &mut VecDeque<AudioItem>,
    paused: bool,
) -> Option<AudioItem> {
    interrupted_priority
        .take()
        .or_else(|| priority_pending.pop_front())
        .or_else(|| {
            (!paused)
                .then(|| {
                    interrupted_routine
                        .take()
                        .or_else(|| deferred_routine.pop_front())
                })
                .flatten()
        })
}

fn drain_priority_receiver(
    priority_rx: &mut mpsc::Receiver<AudioItem>,
    priority_pending: &mut VecDeque<AudioItem>,
    active_priority_ids: &mut HashSet<String>,
    feed_id: &str,
) -> Vec<AudioItem> {
    let mut accepted = Vec::new();
    while priority_pending.len() < PRIORITY_PENDING_QUEUE_CAPACITY {
        let Ok(item) = priority_rx.try_recv() else {
            break;
        };
        if !remember_priority_item(active_priority_ids, &item.id) {
            tracing::warn!(
                feed_id,
                queue_id = item.id,
                "skipping duplicate active priority alert"
            );
            continue;
        }
        accepted.push(item.clone());
        priority_pending.push_back(item);
    }
    accepted
}

fn remember_interrupted_routine(
    interrupted_routine: &mut Option<AudioItem>,
    deferred_routine: &mut VecDeque<AudioItem>,
    active_routine_ids: &mut HashSet<String>,
    feed_id: &str,
    item: AudioItem,
) {
    if let Some(previous) = interrupted_routine.replace(item) {
        defer_routine_front(deferred_routine, active_routine_ids, feed_id, previous);
    }
}

fn defer_pending_routine(
    pending: &mut Option<AudioItem>,
    deferred_routine: &mut VecDeque<AudioItem>,
    active_routine_ids: &mut HashSet<String>,
    feed_id: &str,
) -> bool {
    let Some(item) = pending.take() else {
        return false;
    };
    if item.is_alert() {
        *pending = Some(item);
        return false;
    }
    defer_routine_back(deferred_routine, active_routine_ids, feed_id, item);
    true
}

fn enqueue_priority_front(
    queue: &mut VecDeque<AudioItem>,
    active_priority_ids: &mut HashSet<String>,
    feed_id: &str,
    item: AudioItem,
) {
    if queue.len() >= PRIORITY_PENDING_QUEUE_CAPACITY {
        if let Some(dropped) = queue.pop_back() {
            active_priority_ids.remove(&dropped.id);
            tracing::warn!(
                feed_id,
                queue_id = dropped.id,
                "priority queue is full; dropping newest queued priority while restoring FIFO"
            );
        }
    }
    queue.push_front(item);
}

fn copy_item_pcm(item: &AudioItem, position: &mut usize, out: &mut [u8]) -> usize {
    let start = (*position).min(item.pcm.len());
    let take = out.len().min(item.pcm.len().saturating_sub(start));
    if take > 0 {
        out[..take].copy_from_slice(&item.pcm[start..start + take]);
        *position = start + take;
    }
    take
}

fn defer_routine_front(
    queue: &mut VecDeque<AudioItem>,
    active_routine_ids: &mut HashSet<String>,
    feed_id: &str,
    item: AudioItem,
) {
    trim_deferred_routine_for_insert(queue, active_routine_ids, feed_id);
    queue.push_front(item);
}

fn defer_routine_back(
    queue: &mut VecDeque<AudioItem>,
    active_routine_ids: &mut HashSet<String>,
    feed_id: &str,
    item: AudioItem,
) {
    trim_deferred_routine_for_insert(queue, active_routine_ids, feed_id);
    queue.push_back(item);
}

fn trim_deferred_routine_for_insert(
    queue: &mut VecDeque<AudioItem>,
    active_routine_ids: &mut HashSet<String>,
    feed_id: &str,
) {
    while queue.len() >= DEFERRED_ROUTINE_QUEUE_CAPACITY {
        let Some(dropped) = queue.pop_back() else {
            break;
        };
        active_routine_ids.remove(&dropped.id);
        tracing::warn!(
            feed_id,
            queue_id = dropped.id,
            package_id = dropped.package_id,
            "deferred routine queue is full; dropping stale interrupted audio"
        );
    }
}

fn remember_completed_routine_item(
    completed_ids: &mut HashSet<String>,
    completed_order: &mut VecDeque<String>,
    id: &str,
) {
    let id = id.trim();
    if id.is_empty() || !completed_ids.insert(id.to_string()) {
        return;
    }
    completed_order.push_back(id.to_string());
    while completed_order.len() > COMPLETED_ROUTINE_HISTORY_CAPACITY {
        if let Some(oldest) = completed_order.pop_front() {
            completed_ids.remove(&oldest);
        }
    }
}

fn spawn_accept_item(tasks: &SessionTasks, client: BridgeClient, feed_id: String, item: AudioItem) {
    tasks.spawn(async move {
        accept_item(&client, &feed_id, &item).await;
    });
}

fn spawn_start_item(
    tasks: &SessionTasks,
    client: BridgeClient,
    cfg: Arc<LoadedConfig>,
    feed_id: String,
    item: AudioItem,
) {
    tasks.spawn(async move {
        start_item(&client, &cfg, &feed_id, &item).await;
    });
}

fn spawn_finish_item(tasks: &SessionTasks, client: BridgeClient, feed_id: String, item: AudioItem) {
    tasks.spawn(async move {
        finish_item(&client, &feed_id, &item).await;
    });
}

fn spawn_interrupt_item(
    tasks: &SessionTasks,
    client: BridgeClient,
    feed_id: String,
    item: AudioItem,
) {
    tasks.spawn(async move {
        interrupt_item(&client, &feed_id, &item).await;
    });
}

fn spawn_complete_live_breakin(
    tasks: &SessionTasks,
    client: BridgeClient,
    cfg: Arc<LoadedConfig>,
    feed_id: String,
    live: LiveBreakIn,
    cancelled: bool,
) {
    tasks.spawn(async move {
        complete_live_breakin(&client, &cfg, &feed_id, &live, cancelled).await;
    });
}

fn spawn_update_runtime(
    tasks: &SessionTasks,
    cfg: Arc<LoadedConfig>,
    feed_id: String,
    now_playing: String,
) {
    tasks.spawn(async move {
        update_runtime(&cfg, &feed_id, &now_playing).await;
    });
}

async fn accept_item(client: &BridgeClient, feed_id: &str, item: &AudioItem) {
    let _ = client
        .publish(json!({
            "type": "playout.accepted",
            "source": SOURCE_ID,
            "feed_id": feed_id,
            "queue_id": item.id,
            "pkg_id": item.package_id,
            "title": item.title,
            "data": item_event_data(feed_id, item),
        }))
        .await;
}

async fn start_item(client: &BridgeClient, cfg: &LoadedConfig, feed_id: &str, item: &AudioItem) {
    tracing::info!("[{}] Now playing: {}", feed_id, item.title);
    update_runtime_for_item(cfg, feed_id, item).await;
    match &item.source {
        ItemSource::Alert {
            manifest_path,
            header,
            event,
        } => {
            if let Err(err) = mark_alert_started(manifest_path) {
                tracing::warn!(
                    feed_id,
                    queue_id = item.id,
                    "failed to mark alert started: {err}"
                );
            }
            let mut data = item_event_data(feed_id, item);
            if let Some(object) = data.as_object_mut() {
                object.insert("header".to_string(), json!(header));
                object.insert("event".to_string(), json!(event));
            }
            let _ = client
                .publish(json!({
                    "type": "alert.playout.started",
                    "source": SOURCE_ID,
                    "feed_ids": [feed_id],
                    "queue_id": item.id,
                    "header": header,
                    "event": event,
                    "data": data
                }))
                .await;
        }
        ItemSource::Playlist | ItemSource::Generated => {
            let _ = client
                .publish(json!({
                    "type": "playout.started",
                    "source": SOURCE_ID,
                    "feed_id": feed_id,
                    "queue_id": item.id,
                    "pkg_id": item.package_id,
                    "title": item.title,
                    "data": item_event_data(feed_id, item),
                }))
                .await;
        }
    }
}

async fn finish_item(client: &BridgeClient, feed_id: &str, item: &AudioItem) {
    match &item.source {
        ItemSource::Alert {
            manifest_path,
            header,
            event,
        } => {
            if let Err(err) = mark_alert_played(manifest_path) {
                tracing::warn!(
                    feed_id,
                    queue_id = item.id,
                    "failed to mark alert played: {err}"
                );
            }
            let mut data = item_event_data(feed_id, item);
            if let Some(object) = data.as_object_mut() {
                object.insert("header".to_string(), json!(header));
                object.insert("event".to_string(), json!(event));
            }
            let _ = client
                .publish(json!({
                    "type": "alert.playout.completed",
                    "source": SOURCE_ID,
                    "feed_ids": [feed_id],
                    "queue_id": item.id,
                    "header": header,
                    "event": event,
                    "data": data
                }))
                .await;
        }
        ItemSource::Playlist | ItemSource::Generated => {
            let _ = client
                .publish(json!({
                    "type": "playout.completed",
                    "source": SOURCE_ID,
                    "feed_id": feed_id,
                    "queue_id": item.id,
                    "pkg_id": item.package_id,
                    "title": item.title,
                    "data": item_event_data(feed_id, item),
                }))
                .await;
        }
    }
}

async fn interrupt_item(client: &BridgeClient, feed_id: &str, item: &AudioItem) {
    let _ = client
        .publish(json!({
            "type": "playout.interrupted",
            "source": SOURCE_ID,
            "feed_id": feed_id,
            "queue_id": item.id,
            "pkg_id": item.package_id,
            "title": item.title,
            "data": {
                "feed_id": feed_id,
                "queue_id": item.id,
                "pkg_id": item.package_id,
                "package_id": item.package_id,
                "title": item.title,
                "interrupted": true,
            }
        }))
        .await;
}

async fn start_live_breakin(
    client: &BridgeClient,
    cfg: &LoadedConfig,
    feed_id: &str,
    live: &LiveBreakIn,
) {
    tracing::info!("[{}] Operator break-in started: {}", feed_id, live.title);
    update_runtime(cfg, feed_id, &live.title).await;
    let _ = client
        .publish(json!({
            "type": "alert.playout.started",
            "source": SOURCE_ID,
            "feed_ids": [feed_id],
            "queue_id": live.id,
            "header": live.title,
            "event": "OPR",
            "data": {
                "feed_id": feed_id,
                "queue_id": live.id,
                "header": live.title,
                "event": "OPR",
                "source": "operator-breakin",
            }
        }))
        .await;
    let _ = client
        .publish(json!({
            "type": "operator.breakin.playout.started",
            "source": SOURCE_ID,
            "feed_id": feed_id,
            "queue_id": live.id,
            "title": live.title,
            "data": {
                "feed_id": feed_id,
                "session_id": live.id,
                "title": live.title,
            }
        }))
        .await;
}

async fn complete_live_breakin(
    client: &BridgeClient,
    cfg: &LoadedConfig,
    feed_id: &str,
    live: &LiveBreakIn,
    cancelled: bool,
) {
    tracing::info!("[{}] Operator break-in ended: {}", feed_id, live.title);
    update_runtime(cfg, feed_id, "Idle").await;
    let _ = client
        .publish(json!({
            "type": "alert.playout.completed",
            "source": SOURCE_ID,
            "feed_ids": [feed_id],
            "queue_id": live.id,
            "header": live.title,
            "event": "OPR",
            "data": {
                "feed_id": feed_id,
                "queue_id": live.id,
                "header": live.title,
                "event": "OPR",
                "source": "operator-breakin",
                "cancelled": cancelled,
            }
        }))
        .await;
    let _ = client
        .publish(json!({
            "type": "operator.breakin.playout.completed",
            "source": SOURCE_ID,
            "feed_id": feed_id,
            "queue_id": live.id,
            "title": live.title,
            "data": {
                "feed_id": feed_id,
                "session_id": live.id,
                "title": live.title,
                "cancelled": cancelled,
            }
        }))
        .await;
}

fn take_finished_breakin(live_breakin: &mut Option<LiveBreakIn>) -> Option<LiveBreakIn> {
    if live_breakin
        .as_ref()
        .is_some_and(|live| live.finishing && live.buffer.is_empty())
    {
        live_breakin.take()
    } else {
        None
    }
}

fn drain_live_breakin_buffer(live: &mut LiveBreakIn, out: &mut [u8]) {
    let take = out.len().min(live.buffer.len());
    if take == 0 {
        return;
    }
    let pending = live.buffer.make_contiguous();
    out[..take].copy_from_slice(&pending[..take]);
    live.buffer.drain(..take);
}

fn trim_live_breakin_buffer(live: &mut LiveBreakIn, sample_rate: u32, channels: u16) {
    let drop = live_breakin_drop_bytes(live.buffer.len(), sample_rate, channels);
    if drop > 0 {
        live.buffer.drain(..drop);
    }
}

fn align_down_to_frame(bytes: usize, frame_bytes: usize) -> usize {
    let frame_bytes = frame_bytes.max(1);
    bytes - (bytes % frame_bytes)
}

fn align_up_to_frame(bytes: usize, frame_bytes: usize) -> usize {
    let frame_bytes = frame_bytes.max(1);
    if bytes == 0 {
        return 0;
    }
    bytes + ((frame_bytes - (bytes % frame_bytes)) % frame_bytes)
}

fn live_breakin_max_buffer_bytes(sample_rate: u32, channels: u16) -> usize {
    let frame_bytes = usize::from(channels.max(1)) * 2;
    align_down_to_frame(
        usize::try_from(
            sample_rate
                .saturating_mul(u32::from(channels.max(1)))
                .saturating_mul(2)
                .saturating_mul(LIVE_BREAKIN_MAX_BUFFER_MS)
                / 1000,
        )
        .unwrap_or(usize::MAX),
        frame_bytes,
    )
}

#[cfg(test)]
fn max_live_breakin_buffer_for(sample_rate: u32, channels: u16) -> usize {
    live_breakin_max_buffer_bytes(sample_rate, channels)
}

fn realtime_lag_warn_backlog() -> Duration {
    Duration::from_millis(REALTIME_LAG_WARN_BACKLOG_MS)
}

fn realtime_chunks_due(media_remainder: &mut Duration, elapsed: Duration) -> (usize, Duration) {
    let chunk_interval = Duration::from_millis(u64::from(PCM_CHUNK_MS));
    let available = media_remainder.saturating_add(elapsed);
    *media_remainder = Duration::ZERO;
    let late_by = available.saturating_sub(chunk_interval);
    // Tokio replays short missed intervals immediately. Treating normal scheduler
    // jitter as dropped audio made the playout source run permanently slow.
    let dropped = if late_by >= realtime_lag_warn_backlog() {
        late_by
    } else {
        Duration::ZERO
    };
    (1, dropped)
}

fn pcm_frame_count(bytes: usize, channels: u16) -> u64 {
    let frame_bytes = usize::from(channels.max(1)).saturating_mul(2);
    u64::try_from(bytes / frame_bytes).unwrap_or(u64::MAX)
}

fn duration_sample_frames(duration: Duration, sample_rate: u32) -> u64 {
    let frames = duration
        .as_nanos()
        .saturating_mul(u128::from(sample_rate.max(1)))
        / 1_000_000_000u128;
    u64::try_from(frames).unwrap_or(u64::MAX)
}

fn sample_frames_to_ns(frames: u64, sample_rate: u32) -> u64 {
    let nanoseconds =
        u128::from(frames).saturating_mul(1_000_000_000u128) / u128::from(sample_rate.max(1));
    u64::try_from(nanoseconds).unwrap_or(u64::MAX)
}

fn realtime_tick_missed_behavior() -> MissedTickBehavior {
    MissedTickBehavior::Burst
}

fn pcm_publish_queue_capacity() -> usize {
    PCM_PUBLISH_QUEUE_CAPACITY
}

#[cfg(test)]
fn media_publish_chunk_duration() -> Duration {
    Duration::from_millis(u64::from(MEDIA_PUBLISH_CHUNK_MS))
}

#[cfg(test)]
fn media_publish_queue_duration() -> Duration {
    media_publish_chunk_duration() * u32::try_from(PCM_PUBLISH_QUEUE_CAPACITY).unwrap_or(u32::MAX)
}

fn live_breakin_frame_bytes(channels: u16) -> usize {
    usize::from(channels.max(1)) * 2
}

fn live_breakin_drop_bytes(buffer_len: usize, sample_rate: u32, channels: u16) -> usize {
    let frame_bytes = live_breakin_frame_bytes(channels);
    let max_bytes = live_breakin_max_buffer_bytes(sample_rate, channels);
    let overflow = buffer_len.saturating_sub(max_bytes);
    align_up_to_frame(overflow, frame_bytes).min(buffer_len)
}

fn clears_deferred_routine(action: &str) -> bool {
    let normalized = action.trim().to_ascii_lowercase();
    let action = normalized
        .strip_suffix("_after_current")
        .unwrap_or(&normalized);
    matches!(
        action,
        "restart" | "flush" | "flush_pending" | "flush_restart" | "flush_stop"
    )
}

fn clear_deferred_routine_state(
    interrupted_routine: &mut Option<AudioItem>,
    deferred_routine: &mut VecDeque<AudioItem>,
    active_routine_ids: &mut HashSet<String>,
    completed_routine_ids: &mut HashSet<String>,
    completed_routine_order: &mut VecDeque<String>,
) {
    if let Some(item) = interrupted_routine.take() {
        active_routine_ids.remove(&item.id);
    }
    for item in deferred_routine.drain(..) {
        active_routine_ids.remove(&item.id);
    }
    completed_routine_ids.clear();
    completed_routine_order.clear();
}

async fn update_runtime(cfg: &LoadedConfig, feed_id: &str, now_playing: &str) {
    let now = Utc::now().to_rfc3339();
    write_runtime_payload(cfg, feed_id, now_playing, now, None).await;
}

async fn update_runtime_for_item(cfg: &LoadedConfig, feed_id: &str, item: &AudioItem) {
    let started_at = Utc::now();
    let duration_ms = remaining_item_duration_ms(cfg, item);
    write_runtime_payload(
        cfg,
        feed_id,
        &item.title,
        started_at.to_rfc3339(),
        Some((started_at, duration_ms)),
    )
    .await;
}

fn remaining_item_duration_ms(cfg: &LoadedConfig, item: &AudioItem) -> u64 {
    let remaining_ms = pcm_duration_ms(
        item.pcm.len(),
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
    );
    if remaining_ms > 0 {
        remaining_ms
    } else {
        item.metadata.duration_ms
    }
}

async fn write_runtime_payload(
    cfg: &LoadedConfig,
    feed_id: &str,
    now_playing: &str,
    now: String,
    timing: Option<(DateTime<Utc>, u64)>,
) {
    let mut payload = json!({
        "feed_id": feed_id,
        "now_playing": now_playing,
        "on_air_now_playing": now_playing,
        "on_air_last_played_at": now,
        "public_stream_now_playing": now_playing,
        "public_stream_started_at": now,
        "updated_at": now,
    });
    if let Some((started_at, duration_ms)) = timing {
        let duration = ChronoDuration::milliseconds(duration_ms.min(i64::MAX as u64) as i64);
        if let Some(object) = payload.as_object_mut() {
            object.insert(
                "current_started_at".to_string(),
                json!(started_at.to_rfc3339()),
            );
            object.insert(
                "current_ends_at".to_string(),
                json!((started_at + duration).to_rfc3339()),
            );
            object.insert("current_duration_ms".to_string(), json!(duration_ms));
        }
    }
    let path = cfg
        .base_dir
        .join("runtime/feeds")
        .join(format!("{}.json", safe_id(feed_id)));
    if let Some(parent) = path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    if let Ok(raw) = serde_json::to_vec_pretty(&payload) {
        let tmp = path.with_extension("json.tmp");
        if tokio::fs::write(&tmp, raw).await.is_ok() {
            let _ = tokio::fs::rename(&tmp, &path).await;
        }
    }
}

fn spawn_pcm_publisher(
    tasks: &SessionTasks,
    client: BridgeClient,
    feed_id: String,
    sample_rate: u32,
    channels: u16,
    buffer_capacity: usize,
) -> PcmPublisher {
    let (tx, mut rx) = mpsc::channel::<PcmPublish>(pcm_publish_queue_capacity());
    let recycle_capacity = pcm_publish_queue_capacity() + 2;
    let (recycle_tx, recycled) = mpsc::channel::<Vec<u8>>(recycle_capacity);
    for _ in 0..recycle_capacity {
        recycle_tx
            .try_send(Vec::with_capacity(buffer_capacity))
            .expect("empty PCM recycler must accept its seeded buffers");
    }
    tasks.spawn(async move {
        let mut delivery_state = PcmDeliveryState::default();
        let mut encoded_pcm = String::with_capacity(buffer_capacity.saturating_mul(4).div_ceil(3));
        let mut event = Vec::with_capacity(buffer_capacity.saturating_mul(4).div_ceil(3) + 512);
        while let Some(mut chunk) = rx.recv().await {
            delivery_state.prepare(&mut chunk);
            serialize_pcm_publish_event_line(
                &mut event,
                &mut encoded_pcm,
                &feed_id,
                sample_rate,
                channels,
                &chunk,
            )
            .expect("PCM bridge event only contains serializable fields");
            let succeeded = client.write_event_line(&event).await.is_ok();
            delivery_state.record_result(succeeded);
            chunk.data.clear();
            let _ = recycle_tx.try_send(chunk.data);
        }
    });
    PcmPublisher { tx, recycled }
}

fn flush_pcm_publish(
    builder: &mut PcmPublishBuilder,
    publisher: &mut PcmPublisher,
    fallback_capacity: usize,
    feed_id: &str,
    dropped_publishes: &mut u64,
) {
    let Some(chunk) = builder.take() else {
        return;
    };
    match publisher.tx.try_send(chunk) {
        Ok(()) => {
            builder.recycle(
                publisher
                    .recycled
                    .try_recv()
                    .unwrap_or_else(|_| Vec::with_capacity(fallback_capacity)),
            );
        }
        Err(TrySendError::Full(unsent)) => {
            builder.recycle(unsent.data);
            builder.mark_discontinuity();
            *dropped_publishes = (*dropped_publishes).saturating_add(1);
            if *dropped_publishes == 1 || (*dropped_publishes).is_multiple_of(50) {
                tracing::warn!(
                    feed_id,
                    dropped_chunks = *dropped_publishes,
                    "media publisher is behind; dropping stale bridge PCM"
                );
            }
        }
        Err(TrySendError::Closed(unsent)) => {
            builder.recycle(unsent.data);
            builder.mark_discontinuity();
            tracing::warn!(
                feed_id,
                "media publisher stopped before PCM could be forwarded"
            );
        }
    }
}

fn media_publish_buffer_capacity(sample_rate: u32, channels: u16) -> usize {
    let chunk = silence_chunk(sample_rate, channels, PCM_CHUNK_MS);
    let publish_chunk_count = (MEDIA_PUBLISH_CHUNK_MS / PCM_CHUNK_MS).max(1) as usize;
    chunk.len().saturating_mul(publish_chunk_count)
}

fn serialize_pcm_publish_event_line(
    event: &mut Vec<u8>,
    encoded_pcm: &mut String,
    feed_id: &str,
    sample_rate: u32,
    channels: u16,
    chunk: &PcmPublish,
) -> Result<()> {
    encoded_pcm.clear();
    base64::engine::general_purpose::STANDARD.encode_string(&chunk.data, encoded_pcm);
    let timestamp = Utc::now().to_rfc3339();
    let channel_layout = pcm_channel_layout(channels);
    let queue_id = chunk.queue_id.as_deref();
    let payload = PcmBridgeEvent {
        event_type: "playout.pcm",
        source: SOURCE_ID,
        feed_id,
        queue_id,
        timestamp: &timestamp,
        data: PcmBridgeData {
            feed_id,
            sample_rate,
            channels,
            channel_layout: &channel_layout,
            duration_ms: chunk.duration_ms,
            sequence: chunk.sequence,
            pts_ns: chunk.pts_ns,
            discontinuity: chunk.discontinuity,
            queue_id,
            media_kind: chunk.media_kind.as_str(),
            pcm: encoded_pcm,
        },
    };
    event.clear();
    serde_json::to_writer(&mut *event, &payload)?;
    event.push(b'\n');
    Ok(())
}

#[cfg(test)]
fn pcm_publish_event(feed_id: &str, sample_rate: u32, channels: u16, chunk: &PcmPublish) -> Value {
    let mut event = Vec::new();
    let mut encoded_pcm = String::new();
    serialize_pcm_publish_event_line(
        &mut event,
        &mut encoded_pcm,
        feed_id,
        sample_rate,
        channels,
        chunk,
    )
    .expect("PCM bridge event only contains serializable fields");
    serde_json::from_slice(&event).expect("serialized PCM bridge event must be valid JSON")
}

fn pcm_channel_layout(channels: u16) -> String {
    match channels.max(1) {
        1 => "mono".to_string(),
        2 => "stereo".to_string(),
        6 => "5.1".to_string(),
        count => format!("{count}ch"),
    }
}

impl AudioCache {
    async fn read_wav_pcm(&self, path: &Path, cfg: &LoadedConfig) -> Result<SharedPcm> {
        let (modified_ns, len) = audio_file_stamp(path).await;
        let key = SharedAudioKey::Wav {
            path: path.to_path_buf(),
            modified_ns,
            len,
            sample_rate: cfg.root.playout.sample_rate,
            channels: cfg.root.playout.channels,
        };
        let path = path.to_path_buf();
        let sample_rate = cfg.root.playout.sample_rate;
        let channels = cfg.root.playout.channels;
        let allow_file_backed = runtime_audio_path(&path, &cfg.base_dir);
        self.get_or_load(key, move || async move {
            decode_wav_file_to_shared_pcm(path, sample_rate, channels, allow_file_backed).await
        })
        .await
    }

    async fn read_raw_pcm(
        &self,
        path: &Path,
        source_rate: u32,
        source_channels: u16,
        cfg: &LoadedConfig,
    ) -> Result<SharedPcm> {
        let (modified_ns, len) = audio_file_stamp(path).await;
        let key = SharedAudioKey::RawPcm {
            path: path.to_path_buf(),
            modified_ns,
            len,
            source_rate,
            source_channels,
            sample_rate: cfg.root.playout.sample_rate,
            channels: cfg.root.playout.channels,
        };
        let path = path.to_path_buf();
        let sample_rate = cfg.root.playout.sample_rate;
        let channels = cfg.root.playout.channels;
        let allow_file_backed = runtime_audio_path(&path, &cfg.base_dir);
        self.get_or_load(key, move || async move {
            if runtime_raw_pcm_mmap_supported()
                && allow_file_backed
                && source_rate == sample_rate
                && source_channels.max(1) == channels.max(1)
            {
                let mapped_path = path.clone();
                if let Ok(Ok(mapped)) = tokio::task::spawn_blocking(move || {
                    map_raw_pcm_file(&mapped_path, channels.max(1))
                })
                .await
                {
                    return Ok(mapped);
                }
            }
            let raw = tokio::fs::read(&path)
                .await
                .with_context(|| format!("failed to read audio {}", path.display()))?;
            shared_pcm(normalize_pcm(
                Pcm {
                    sample_rate: source_rate,
                    channels: source_channels.max(1),
                    data: raw,
                },
                sample_rate,
                channels,
            ))
        })
        .await
    }

    async fn synthesize_wav(
        &self,
        client: &BridgeClient,
        job: SynthJob,
        cfg: &LoadedConfig,
    ) -> Result<SharedPcm> {
        let key = SharedAudioKey::Synth {
            text: job.text.clone(),
            reader_id: job.reader_id.clone(),
            language: job.language.clone(),
            sample_rate: cfg.root.playout.sample_rate,
            channels: cfg.root.playout.channels,
        };
        let client = client.clone();
        let sample_rate = cfg.root.playout.sample_rate;
        let channels = cfg.root.playout.channels;
        self.get_or_load(key, move || async move {
            let wav_path = client.synthesize(job).await?;
            decode_wav_file_to_shared_pcm(PathBuf::from(wav_path), sample_rate, channels, true)
                .await
        })
        .await
    }

    async fn get_or_load<Load, Fut>(&self, key: SharedAudioKey, load: Load) -> Result<SharedPcm>
    where
        Load: FnOnce() -> Fut,
        Fut: Future<Output = Result<SharedPcm>>,
    {
        let now = Instant::now();
        let waiter = {
            let mut entries = self.entries.lock().await;
            prune_idle_audio_cache(&mut entries, now, Some(&key));
            match entries.get_mut(&key) {
                Some(SharedAudioEntry::Ready { pcm, last_used, .. }) => {
                    *last_used = now;
                    return Ok(pcm.clone());
                }
                Some(SharedAudioEntry::Loading(waiters)) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    Some(rx)
                }
                None => {
                    entries.insert(key.clone(), SharedAudioEntry::Loading(Vec::new()));
                    None
                }
            }
        };

        if let Some(waiter) = waiter {
            return match waiter.await {
                Ok(Ok(pcm)) => Ok(pcm),
                Ok(Err(err)) => anyhow::bail!(err),
                Err(_) => anyhow::bail!("audio cache loader cancelled"),
            };
        }

        let loaded = load().await;
        let notification = match &loaded {
            Ok(pcm) => Ok(pcm.clone()),
            Err(err) => Err(err.to_string()),
        };
        let waiters = {
            let mut entries = self.entries.lock().await;
            match entries.remove(&key) {
                Some(SharedAudioEntry::Loading(waiters)) => {
                    if let Ok(pcm) = &loaded {
                        entries.insert(
                            key.clone(),
                            SharedAudioEntry::Ready {
                                pcm: pcm.clone(),
                                bytes: pcm.heap_bytes(),
                                last_used: Instant::now(),
                            },
                        );
                        trim_audio_cache(&mut entries, &key);
                    }
                    waiters
                }
                Some(SharedAudioEntry::Ready {
                    pcm,
                    bytes,
                    last_used,
                }) => {
                    entries.insert(
                        key,
                        SharedAudioEntry::Ready {
                            pcm,
                            bytes,
                            last_used,
                        },
                    );
                    Vec::new()
                }
                None => Vec::new(),
            }
        };
        for waiter in waiters {
            let _ = waiter.send(notification.clone());
        }
        loaded
    }

    async fn prune_idle(&self) {
        let mut entries = self.entries.lock().await;
        prune_idle_audio_cache(&mut entries, Instant::now(), None);
    }
}

fn trim_audio_cache(
    entries: &mut HashMap<SharedAudioKey, SharedAudioEntry>,
    preserve: &SharedAudioKey,
) {
    prune_idle_audio_cache(entries, Instant::now(), Some(preserve));
    let mut ready = Vec::new();
    let mut total_bytes = 0usize;
    let mut ready_entries = 0usize;
    for (key, entry) in entries.iter() {
        if let SharedAudioEntry::Ready {
            bytes, last_used, ..
        } = entry
        {
            total_bytes = total_bytes.saturating_add(*bytes);
            ready_entries += 1;
            ready.push((key.clone(), *last_used, *bytes));
        }
    }
    if total_bytes <= AUDIO_CACHE_MAX_BYTES && ready_entries <= AUDIO_CACHE_MAX_READY_ENTRIES {
        return;
    }

    ready.sort_by_key(|(_, last_used, _)| *last_used);
    for (key, _, bytes) in ready {
        if &key == preserve {
            continue;
        }
        if entries.remove(&key).is_some() {
            total_bytes = total_bytes.saturating_sub(bytes);
            ready_entries = ready_entries.saturating_sub(1);
        }
        if total_bytes <= AUDIO_CACHE_MAX_BYTES && ready_entries <= AUDIO_CACHE_MAX_READY_ENTRIES {
            break;
        }
    }
}

fn prune_idle_audio_cache(
    entries: &mut HashMap<SharedAudioKey, SharedAudioEntry>,
    now: Instant,
    preserve: Option<&SharedAudioKey>,
) {
    let stale: Vec<SharedAudioKey> = entries
        .iter()
        .filter_map(|(key, entry)| {
            if preserve.is_some_and(|preserve| preserve == key) {
                return None;
            }
            match entry {
                SharedAudioEntry::Ready { last_used, .. }
                    if now.saturating_duration_since(*last_used) >= AUDIO_CACHE_MAX_IDLE =>
                {
                    Some(key.clone())
                }
                _ => None,
            }
        })
        .collect();
    for key in stale {
        entries.remove(&key);
    }
}

async fn audio_file_stamp(path: &Path) -> (Option<u128>, u64) {
    let Ok(metadata) = tokio::fs::metadata(path).await else {
        return (None, 0);
    };
    let modified_ns = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());
    (modified_ns, metadata.len())
}

async fn decode_wav_file_to_shared_pcm(
    path: PathBuf,
    sample_rate: u32,
    channels: u16,
    allow_file_backed: bool,
) -> Result<SharedPcm> {
    if allow_file_backed {
        let mapped_path = path.clone();
        if let Ok(Ok(Some(mapped))) = tokio::task::spawn_blocking(move || {
            map_pcm16_wav_file(&mapped_path, sample_rate, channels.max(1))
        })
        .await
        {
            return Ok(mapped);
        }
    }
    let raw = tokio::fs::read(&path)
        .await
        .with_context(|| format!("failed to read audio {}", path.display()))?;
    let pcm = tokio::task::spawn_blocking(move || decode_wav(&raw))
        .await
        .context("WAV decoder task failed")??;
    shared_pcm(normalize_pcm(pcm, sample_rate, channels))
}

fn shared_pcm(pcm: Pcm) -> Result<SharedPcm> {
    Ok(SharedPcm::owned(pcm.data))
}

fn runtime_audio_path(path: &Path, base_dir: &Path) -> bool {
    let audio_root = base_dir.join("runtime/audio");
    if path.starts_with(&audio_root) {
        return true;
    }
    if audio_root.is_relative() {
        return std::env::current_dir()
            .map(|current_dir| path.starts_with(current_dir.join(audio_root)))
            .unwrap_or(false);
    }
    false
}

#[cfg(windows)]
fn runtime_raw_pcm_mmap_supported() -> bool {
    false
}

#[cfg(not(windows))]
fn runtime_raw_pcm_mmap_supported() -> bool {
    true
}

fn map_raw_pcm_file(path: &Path, channels: u16) -> Result<SharedPcm> {
    let mmap = map_read_only(path)?;
    let frame_bytes = usize::from(channels.max(1)) * 2;
    if mmap.is_empty() || mmap.len() % frame_bytes != 0 {
        anyhow::bail!("raw PCM file does not contain complete audio frames");
    }
    let len = mmap.len();
    Ok(SharedPcm::Mapped(Arc::new(MappedPcm {
        mmap,
        offset: 0,
        len,
    })))
}

fn map_pcm16_wav_file(
    path: &Path,
    expected_sample_rate: u32,
    expected_channels: u16,
) -> Result<Option<SharedPcm>> {
    let mmap = map_read_only(path)?;
    if mmap.len() < 12 || &mmap[0..4] != b"RIFF" || &mmap[8..12] != b"WAVE" {
        return Ok(None);
    }

    let mut offset = 12usize;
    let mut format = None;
    let mut data = None;
    while offset.saturating_add(8) <= mmap.len() {
        let chunk_id = &mmap[offset..offset + 4];
        let chunk_len = u32::from_le_bytes([
            mmap[offset + 4],
            mmap[offset + 5],
            mmap[offset + 6],
            mmap[offset + 7],
        ]) as usize;
        let chunk_start = offset + 8;
        let Some(chunk_end) = chunk_start.checked_add(chunk_len) else {
            return Ok(None);
        };
        if chunk_end > mmap.len() {
            return Ok(None);
        }
        if chunk_id == b"fmt " && chunk_len >= 16 {
            format = Some((
                u16::from_le_bytes([mmap[chunk_start], mmap[chunk_start + 1]]),
                u16::from_le_bytes([mmap[chunk_start + 2], mmap[chunk_start + 3]]),
                u32::from_le_bytes([
                    mmap[chunk_start + 4],
                    mmap[chunk_start + 5],
                    mmap[chunk_start + 6],
                    mmap[chunk_start + 7],
                ]),
                u16::from_le_bytes([mmap[chunk_start + 14], mmap[chunk_start + 15]]),
            ));
        } else if chunk_id == b"data" && chunk_len > 0 {
            data = Some((chunk_start, chunk_len));
        }
        offset = chunk_end.saturating_add(chunk_len & 1);
    }

    let Some((audio_format, channels, sample_rate, bits_per_sample)) = format else {
        return Ok(None);
    };
    let Some((data_offset, data_len)) = data else {
        return Ok(None);
    };
    let frame_bytes = usize::from(channels.max(1)) * 2;
    if audio_format != 1
        || bits_per_sample != 16
        || sample_rate != expected_sample_rate
        || channels != expected_channels
        || data_len % frame_bytes != 0
    {
        return Ok(None);
    }

    Ok(Some(SharedPcm::Mapped(Arc::new(MappedPcm {
        mmap,
        offset: data_offset,
        len: data_len,
    }))))
}

fn map_read_only(path: &Path) -> Result<Mmap> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to open audio {}", path.display()))?;
    // Runtime audio files are atomically published and never mutated in place.
    let mmap = unsafe { MmapOptions::new().map(&file) }
        .with_context(|| format!("failed to map audio {}", path.display()))?;
    Ok(mmap)
}

async fn package_builder(
    cfg: Arc<LoadedConfig>,
    client: BridgeClient,
    feed: FeedConfig,
    audio_cache: Arc<AudioCache>,
    mut requests: mpsc::Receiver<PackageRequest>,
    audio_tx: mpsc::Sender<AudioItem>,
) {
    let mut recent = HashMap::<String, Instant>::new();
    while let Some(request) = requests.recv().await {
        if !request.force
            && recent
                .get(&request.package_id)
                .is_some_and(|last| last.elapsed() < Duration::from_secs(45))
        {
            continue;
        }
        let Some(permit) = try_reserve_routine_slot(&audio_tx, &feed.id, &request.package_id)
        else {
            if audio_tx.is_closed() {
                break;
            }
            continue;
        };
        match build_package(&cfg, &client, &audio_cache, &feed, &request.package_id).await {
            Ok(item) => {
                recent.insert(request.package_id.clone(), Instant::now());
                permit.send(item);
            }
            Err(err) => tracing::warn!(
                feed_id = feed.id,
                package_id = request.package_id,
                "package failed: {err}"
            ),
        }
    }
}

fn try_reserve_routine_slot(
    tx: &mpsc::Sender<AudioItem>,
    feed_id: &str,
    package_id: &str,
) -> Option<mpsc::OwnedPermit<AudioItem>> {
    match tx.clone().try_reserve_owned() {
        Ok(permit) => Some(permit),
        Err(TrySendError::Full(_)) => {
            tracing::debug!(
                feed_id,
                package_id,
                "routine playout queue is full; skipping stale rendered audio"
            );
            None
        }
        Err(TrySendError::Closed(_)) => {
            tracing::warn!(
                feed_id,
                package_id,
                "playout queue closed before routine item could be queued"
            );
            None
        }
    }
}

async fn priority_builder(
    cfg: Arc<LoadedConfig>,
    feed: FeedConfig,
    audio_cache: Arc<AudioCache>,
    mut requests: mpsc::Receiver<Value>,
    priority_tx: mpsc::Sender<AudioItem>,
) {
    while let Some(data) = requests.recv().await {
        match audio_item_from_priority_ready(&cfg, &audio_cache, &feed, data).await {
            Ok(item) => {
                if priority_tx.send(item).await.is_err() {
                    tracing::warn!(
                        feed_id = feed.id,
                        "priority queue closed before alert audio could be queued"
                    );
                    break;
                }
            }
            Err(err) => tracing::warn!(feed_id = feed.id, "priority alert rejected: {err}"),
        }
    }
}

async fn build_package(
    cfg: &LoadedConfig,
    client: &BridgeClient,
    audio_cache: &AudioCache,
    feed: &FeedConfig,
    package_id: &str,
) -> Result<AudioItem> {
    if !cfg.package_enabled(package_id) || !feed.routine_enabled() {
        anyhow::bail!("package {package_id} is disabled for feed {}", feed.id);
    }
    let product = render_product_with_fallback(cfg, client, feed, package_id).await?;
    if let Some(item) =
        audio_item_from_mixed_rendered_product(cfg, client, audio_cache, feed, package_id, &product)
            .await?
    {
        return Ok(item);
    }
    if let Some(item) =
        audio_item_from_rendered_product(cfg, audio_cache, feed, package_id, &product).await?
    {
        return Ok(item);
    }
    if product.text.trim().is_empty() {
        anyhow::bail!("package {package_id} rendered empty text");
    }
    let title = fallback_text(&product.title, &title_for_package(package_id));
    let reader_id = fallback_text(&product.reader_id, &cfg.reader_id(package_id));
    let language = fallback_text(&product.language, &feed.language());
    let job_id = queue_id(&format!("{}-{package_id}", feed.id));
    let output_path = cfg
        .base_dir
        .join("runtime/audio/playout")
        .join(&feed.id)
        .join(format!("{job_id}.wav"));
    let pcm = audio_cache
        .synthesize_wav(
            client,
            SynthJob {
                id: job_id.clone(),
                text: product.text,
                reader_id,
                language,
                output_path,
            },
            cfg,
        )
        .await?;
    Ok(AudioItem {
        id: job_id,
        package_id: package_id.to_string(),
        title,
        pcm,
        metadata: AudioItemMetadata::default(),
        gap_after: package_gap(cfg),
        not_before: None,
        queued_at: Utc::now().to_rfc3339(),
        target_start: String::new(),
        predicted_start: String::new(),
        predicted_finish: String::new(),
        source: ItemSource::Generated,
        segment_group: None,
    })
}

async fn audio_item_from_rendered_product(
    cfg: &LoadedConfig,
    audio_cache: &AudioCache,
    feed: &FeedConfig,
    package_id: &str,
    product: &RenderedProduct,
) -> Result<Option<AudioItem>> {
    let content_type = metadata_text(&product.metadata, "content_type");
    if !content_type.eq_ignore_ascii_case("audio") {
        return Ok(None);
    }
    let audio_path = metadata_text(&product.metadata, "audio_path");
    if audio_path.is_empty() {
        let audio_url = metadata_text(&product.metadata, "audio_url");
        if audio_url.is_empty() {
            anyhow::bail!("audio product {package_id} is missing audio_path or audio_url");
        }
        anyhow::bail!("audio URL products require the Go playlist downloader path");
    }
    let path = resolve_path(&cfg.base_dir, audio_path);
    let pcm = audio_cache.read_wav_pcm(&path, cfg).await?;
    let duration_ms = pcm_duration_ms(
        pcm.len(),
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
    );
    Ok(Some(AudioItem {
        id: queue_id(&format!("{}-{package_id}", feed.id)),
        package_id: package_id.to_string(),
        title: fallback_text(&product.title, &title_for_package(package_id)),
        pcm,
        metadata: AudioItemMetadata {
            audio_path: audio_path.to_string(),
            duration_ms,
            ..Default::default()
        },
        gap_after: package_gap(cfg),
        not_before: None,
        queued_at: Utc::now().to_rfc3339(),
        target_start: String::new(),
        predicted_start: String::new(),
        predicted_finish: String::new(),
        source: ItemSource::Generated,
        segment_group: None,
    }))
}

async fn audio_item_from_mixed_rendered_product(
    cfg: &LoadedConfig,
    client: &BridgeClient,
    audio_cache: &AudioCache,
    feed: &FeedConfig,
    package_id: &str,
    product: &RenderedProduct,
) -> Result<Option<AudioItem>> {
    if product
        .segments
        .iter()
        .all(|segment| segment.text.trim().is_empty() && segment.audio_path.trim().is_empty())
    {
        return Ok(None);
    }

    let title = fallback_text(&product.title, &title_for_package(package_id));
    let reader_id = fallback_text(&product.reader_id, &cfg.reader_id(package_id));
    let language = fallback_text(&product.language, &feed.language());
    let job_id = queue_id(&format!("{}-{package_id}", feed.id));
    let mut text_parts = Vec::<String>::new();
    let mut chunks = Vec::<SharedPcm>::new();
    let mut source_paths = Vec::<String>::new();

    async fn flush_text_parts(
        text_parts: &mut Vec<String>,
        chunks: &mut Vec<SharedPcm>,
        cfg: &LoadedConfig,
        client: &BridgeClient,
        audio_cache: &AudioCache,
        feed: &FeedConfig,
        package_id: &str,
        job_id: &str,
        reader_id: &str,
        language: &str,
    ) -> Result<()> {
        if text_parts.is_empty() {
            return Ok(());
        }
        let text = text_parts.join("\n\n");
        text_parts.clear();
        let output_path = cfg
            .base_dir
            .join("runtime/audio/playout")
            .join(&feed.id)
            .join(format!("{job_id}-part-{}.wav", chunks.len() + 1));
        let pcm = audio_cache
            .synthesize_wav(
                client,
                SynthJob {
                    id: queue_id(&format!("{}-{package_id}-part", feed.id)),
                    text,
                    reader_id: reader_id.to_string(),
                    language: language.to_string(),
                    output_path,
                },
                cfg,
            )
            .await?;
        chunks.push(pcm);
        Ok(())
    }

    for segment in &product.segments {
        let audio_path = segment.audio_path.trim();
        if audio_path.is_empty() {
            if !segment.text.trim().is_empty() {
                text_parts.push(segment.text.clone());
                flush_text_parts(
                    &mut text_parts,
                    &mut chunks,
                    cfg,
                    client,
                    audio_cache,
                    feed,
                    package_id,
                    &job_id,
                    &reader_id,
                    &language,
                )
                .await?;
            }
            continue;
        }
        flush_text_parts(
            &mut text_parts,
            &mut chunks,
            cfg,
            client,
            audio_cache,
            feed,
            package_id,
            &job_id,
            &reader_id,
            &language,
        )
        .await?;
        let path = resolve_path(&cfg.base_dir, audio_path);
        let pcm = audio_cache.read_wav_pcm(&path, cfg).await?;
        source_paths.push(audio_path.to_string());
        chunks.push(pcm);
    }
    flush_text_parts(
        &mut text_parts,
        &mut chunks,
        cfg,
        client,
        audio_cache,
        feed,
        package_id,
        &job_id,
        &reader_id,
        &language,
    )
    .await?;

    if chunks.is_empty() {
        return Ok(None);
    }
    let total_len = chunks
        .iter()
        .fold(0usize, |total, chunk| total.saturating_add(chunk.len()));
    let mut mixed = Vec::with_capacity(total_len);
    for chunk in chunks {
        mixed.extend_from_slice(chunk.as_ref());
    }
    let pcm = SharedPcm::owned(mixed);
    let duration_ms = pcm_duration_ms(
        pcm.len(),
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
    );
    Ok(Some(AudioItem {
        id: job_id,
        package_id: package_id.to_string(),
        title,
        pcm,
        metadata: AudioItemMetadata {
            audio_path: source_paths.join(","),
            duration_ms,
            ..Default::default()
        },
        gap_after: package_gap(cfg),
        not_before: None,
        queued_at: Utc::now().to_rfc3339(),
        target_start: String::new(),
        predicted_start: String::new(),
        predicted_finish: String::new(),
        source: ItemSource::Generated,
        segment_group: None,
    }))
}

fn metadata_text<'a>(metadata: &'a HashMap<String, String>, key: &str) -> &'a str {
    metadata
        .iter()
        .find_map(|(raw_key, value)| {
            if raw_key.trim().eq_ignore_ascii_case(key) {
                Some(value.trim())
            } else {
                None
            }
        })
        .unwrap_or("")
}

async fn render_product_with_fallback(
    cfg: &LoadedConfig,
    client: &BridgeClient,
    feed: &FeedConfig,
    package_id: &str,
) -> Result<RenderedProduct> {
    if cfg.root.services.go.product_render.enabled {
        let request_id = queue_id(&format!("{}-{package_id}", feed.id));
        let request = ProductRenderRequest {
            request_id,
            feed_id: feed.id.clone(),
            package_id: package_id.to_string(),
            force: true,
        };
        if let Ok(product) = client.render_product(request).await {
            return Ok(product);
        }
    }
    let (text, title) = package_text(cfg, feed, package_id);
    if text.trim().is_empty() {
        anyhow::bail!("package {package_id} requires the product render service");
    }
    Ok(RenderedProduct {
        id: queue_id(package_id),
        feed_id: feed.id.clone(),
        package_id: package_id.to_string(),
        title,
        text,
        reader_id: cfg.reader_id(package_id),
        language: feed.language(),
        metadata: HashMap::new(),
        segments: Vec::new(),
    })
}

fn package_text(cfg: &LoadedConfig, feed: &FeedConfig, package_id: &str) -> (String, String) {
    match package_id {
        "date_time" => (
            date_time_announcement(chrono::Local::now()),
            "Date and Time".to_string(),
        ),
        "station_id" => {
            let on_air = fallback_text(
                &display_text(&cfg.root.operator.on_air_name),
                "Haze Weather Radio",
            );
            let site = feed.site_name();
            let callsign = feed.station_callsign();
            let frequency = feed.station_frequency_mhz();
            let mut parts = vec![format!("You are listening to {on_air}.")];
            let callsign_spoken = spoken_callsign(&callsign);
            if !callsign_spoken.is_empty() {
                parts.push(format!("Callsign {callsign_spoken}."));
            }
            if !frequency.trim().is_empty() {
                let location = fallback_text(&site, &feed.id);
                parts.push(format!(
                    "Broadcasting from {location} on a frequency of {frequency} megahertz."
                ));
            } else if !site.is_empty() {
                parts.push(format!("Serving {site}."));
            }
            if let Some(replacement) = replacement_station_statement(feed) {
                parts.push(replacement);
            }
            (parts.join(" "), "Station Identification".to_string())
        }
        _ => (String::new(), String::new()),
    }
}

fn replacement_station_statement(feed: &FeedConfig) -> Option<String> {
    let transmitter = feed.replacement_transmitter()?;
    let callsign = transmitter.callsign.trim();
    let site = transmitter.site_name.trim();
    if callsign.is_empty() && site.is_empty() {
        return None;
    }
    let mut parts = vec![
        "This station replaces former".to_string(),
        "Weatheradio Canada".to_string(),
        "station".to_string(),
    ];
    if !callsign.is_empty() {
        parts.push(callsign.to_string());
    }
    if !site.is_empty() {
        parts.push("in".to_string());
        parts.push(site.to_string());
    }
    Some(format!("{}.", parts.join(" ")))
}

fn date_time_announcement(now: DateTime<chrono::Local>) -> String {
    format!(
        "{} The current time is {}.",
        time_greeting(now.hour()),
        spoken_clock_time(now)
    )
}

fn time_greeting(hour: u32) -> &'static str {
    match hour {
        5..=11 => "Good morning.",
        12..=16 => "Good afternoon.",
        17..=21 => "Good evening.",
        _ => "Good night.",
    }
}

fn spoken_clock_time(now: DateTime<chrono::Local>) -> String {
    let mut hour = now.hour() % 12;
    if hour == 0 {
        hour = 12;
    }
    let minute = now.minute();
    let ampm = if now.hour() >= 12 { "P.M." } else { "A.M." };
    let tz = timezone_name(&now.format("%Z").to_string());
    if minute == 0 {
        return format!("{} {ampm}, {tz}", number_to_words(hour));
    }
    let minute_text = if minute < 10 {
        format!("oh {}", number_to_words(minute))
    } else {
        number_to_words(minute)
    };
    format!("{} {minute_text} {ampm}, {tz}", number_to_words(hour))
}

fn number_to_words(value: u32) -> String {
    const ONES: [&str; 20] = [
        "zero",
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "eleven",
        "twelve",
        "thirteen",
        "fourteen",
        "fifteen",
        "sixteen",
        "seventeen",
        "eighteen",
        "nineteen",
    ];
    const TENS: [&str; 6] = ["", "", "twenty", "thirty", "forty", "fifty"];
    if value < ONES.len() as u32 {
        return ONES[value as usize].to_string();
    }
    if value < 60 {
        let ten = value / 10;
        let one = value % 10;
        if one == 0 {
            return TENS[ten as usize].to_string();
        }
        return format!("{} {}", TENS[ten as usize], ONES[one as usize]);
    }
    value.to_string()
}

fn timezone_name(abbrev: &str) -> String {
    match abbrev {
        "CST" => "Central Standard Time",
        "CDT" => "Central Daylight Time",
        "MST" => "Mountain Standard Time",
        "MDT" => "Mountain Daylight Time",
        "EST" => "Eastern Standard Time",
        "EDT" => "Eastern Daylight Time",
        "PST" => "Pacific Standard Time",
        "PDT" => "Pacific Daylight Time",
        "UTC" => "Coordinated Universal Time",
        _ => abbrev,
    }
    .to_string()
}

fn spoken_callsign(callsign: &str) -> String {
    callsign
        .trim()
        .chars()
        .filter(|ch| !matches!(ch, '-' | '_' | ' '))
        .map(|ch| ch.to_ascii_uppercase().to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

async fn audio_item_from_ready(
    cfg: &LoadedConfig,
    audio_cache: &AudioCache,
    _feed: &FeedConfig,
    data: Value,
) -> Result<AudioItem> {
    let audio_path = bridge::first_text(&Value::Null, &data, &["audio_path"]);
    if audio_path.is_empty() {
        anyhow::bail!("audio_path is required");
    }
    let path = resolve_path(&cfg.base_dir, audio_path);
    let pcm = audio_cache.read_wav_pcm(&path, cfg).await?;
    let duration_ms = pcm_duration_ms(
        pcm.len(),
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
    );
    let package_id = bridge::first_text(&Value::Null, &data, &["package_id", "pkg_id"]).to_string();
    let title = fallback_text(
        bridge::first_text(&Value::Null, &data, &["title"]),
        &title_for_package(&package_id),
    );
    Ok(AudioItem {
        id: fallback_text(
            bridge::first_text(&Value::Null, &data, &["queue_id", "id"]),
            &queue_id(&package_id),
        ),
        package_id,
        title,
        pcm,
        metadata: AudioItemMetadata {
            audio_path: audio_path.to_string(),
            duration_ms,
            ..Default::default()
        },
        gap_after: package_gap(cfg),
        not_before: parse_time(bridge::first_text(
            &Value::Null,
            &data,
            &["not_before", "predicted_start_at", "target_start_at"],
        )),
        queued_at: bridge::first_text(&Value::Null, &data, &["queued_at"]).to_string(),
        target_start: bridge::first_text(&Value::Null, &data, &["target_start_at"]).to_string(),
        predicted_start: bridge::first_text(&Value::Null, &data, &["predicted_start_at"])
            .to_string(),
        predicted_finish: bridge::first_text(&Value::Null, &data, &["predicted_finish_at"])
            .to_string(),
        source: ItemSource::Playlist,
        segment_group: None,
    })
}

async fn audio_item_from_priority_ready(
    cfg: &LoadedConfig,
    audio_cache: &AudioCache,
    feed: &FeedConfig,
    data: Value,
) -> Result<AudioItem> {
    if priority_request_is_cancelled(&data) {
        anyhow::bail!("priority alert is cancelled or superseded");
    }
    let audio_path = bridge::first_text(&Value::Null, &data, &["audio_path"]);
    if audio_path.is_empty() {
        anyhow::bail!("audio_path is required");
    }
    let path = resolve_path(&cfg.base_dir, audio_path);
    let queue_id = fallback_text(
        bridge::first_text(&Value::Null, &data, &["queue_id", "id"]),
        &queue_id("priority-alert"),
    );
    let sample_rate = u32_at(&data, "sample_rate", 48_000);
    let channels = u16_at(&data, "channels", 1);
    let pcm = audio_cache
        .read_raw_pcm(&path, sample_rate, channels.max(1), cfg)
        .await?;
    let duration_ms = pcm_duration_ms(
        pcm.len(),
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
    );
    let title = fallback_text(
        bridge::first_text(&Value::Null, &data, &["title", "header"]),
        "Weather Alert",
    );
    let event = bridge::first_text(&Value::Null, &data, &["same_event", "event"]).to_string();
    let manifest_path = cfg
        .base_dir
        .join(ALERT_QUEUE_DIR)
        .join(format!("{queue_id}.json"));
    if manifest_path.exists() {
        if let Err(err) = mark_alert_event_queued(&manifest_path) {
            tracing::warn!(
                feed_id = feed.id,
                queue_id,
                "failed to mark alert event queued: {err}"
            );
        }
    }
    Ok(AudioItem {
        id: queue_id.clone(),
        package_id: "same_alert".to_string(),
        title: title.clone(),
        pcm,
        metadata: AudioItemMetadata {
            audio_path: audio_path.to_string(),
            duration_ms,
            alert_packet: data.get("alert_packet").cloned(),
            alert_text: bridge::first_text(
                &Value::Null,
                &data,
                &["alert_text", "tts_text", "text", "message"],
            )
            .to_string(),
            banner_text: bridge::first_text(&Value::Null, &data, &["banner_text"]).to_string(),
            background_color: bridge::first_text(&Value::Null, &data, &["background_color"])
                .to_string(),
            priority: bridge::first_text(&Value::Null, &data, &["priority"]).to_string(),
            feed_ids: value_string_array(&data, "feed_ids"),
            broadcast_immediate: bool_at(&data, "broadcast_immediate"),
        },
        gap_after: Duration::from_millis(0),
        not_before: None,
        queued_at: Utc::now().to_rfc3339(),
        target_start: String::new(),
        predicted_start: String::new(),
        predicted_finish: String::new(),
        source: ItemSource::Alert {
            manifest_path,
            header: title,
            event,
        },
        segment_group: None,
    })
}

fn priority_request_is_cancelled(data: &Value) -> bool {
    let message_type = bridge::first_text(&Value::Null, data, &["message_type", "msg_type"]);
    if message_type.eq_ignore_ascii_case("cancel") {
        return true;
    }

    let status = bridge::first_text(&Value::Null, data, &["status"]).to_ascii_lowercase();
    if matches!(
        status.as_str(),
        "cancelled" | "canceled" | "superseded" | "failed"
    ) {
        return true;
    }

    let header = bridge::first_text(&Value::Null, data, &["header", "title"]).to_ascii_lowercase();
    ["ended", "cancelled", "canceled"]
        .iter()
        .any(|marker| header.contains(marker))
}

async fn alert_scanner(
    cfg: Arc<LoadedConfig>,
    _client: BridgeClient,
    feed: FeedConfig,
    audio_cache: Arc<AudioCache>,
    audio_tx: mpsc::Sender<AudioItem>,
    poll: Duration,
) {
    let mut seen = HashSet::<String>::new();
    let mut ticker = interval(poll.max(Duration::from_millis(25)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(err) = scan_alerts_once(&cfg, &audio_cache, &feed, &audio_tx, &mut seen).await {
            tracing::warn!(feed_id = feed.id, "alert queue scan failed: {err}");
        }
    }
}

async fn scan_alerts_once(
    cfg: &LoadedConfig,
    audio_cache: &AudioCache,
    feed: &FeedConfig,
    audio_tx: &mpsc::Sender<AudioItem>,
    seen: &mut HashSet<String>,
) -> Result<()> {
    let queue_dir = cfg.base_dir.join(ALERT_QUEUE_DIR);
    let Ok(entries) = fs::read_dir(&queue_dir) else {
        return Ok(());
    };
    let mut manifests: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        })
        .collect();
    manifests.sort();
    let mut candidates = Vec::<AlertCandidate>::new();
    for manifest in manifests {
        let Ok(item) = read_alert_item(&manifest) else {
            continue;
        };
        if split_same_part(&item) || legacy_split_alert_item(&item) {
            let mut item = item;
            item.status = "superseded".to_string();
            item.last_error = Some(
                "split legacy alert item is superseded by combined SAME alert audio".to_string(),
            );
            let _ = write_alert_item(&manifest, &item);
            continue;
        }
        if !alert_targets_feed(&item, feed) || !alert_pending(&item.status) {
            continue;
        }
        if alert_item_stale_for_priority(&item, Utc::now()) {
            let mut item = item;
            item.status = "superseded".to_string();
            item.last_error = Some("alert queue item is stale or cancelled".to_string());
            let _ = write_alert_item(&manifest, &item);
            continue;
        }
        let id = fallback_text(
            &item.id,
            &safe_id(
                manifest
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .unwrap_or("alert"),
            ),
        );
        if seen.contains(&id) {
            continue;
        }
        let sort_key = alert_sort_key(&item, &id, &feed.id);
        candidates.push(AlertCandidate {
            manifest,
            item,
            id,
            sort_key,
        });
    }
    candidates.sort_by(|left, right| left.sort_key.cmp(&right.sort_key));

    for candidate in candidates {
        let AlertCandidate {
            manifest,
            mut item,
            id,
            ..
        } = candidate;
        match audio_item_from_alert(cfg, audio_cache, feed, &manifest, &mut item, &id).await {
            Ok(audio) => {
                item.status = "queued".to_string();
                item.claimed_at = Some(Utc::now().to_rfc3339());
                item.failed_at = None;
                item.last_error = None;
                if let Err(err) = write_alert_item(&manifest, &item) {
                    tracing::warn!(
                        feed_id = feed.id,
                        alert_id = id,
                        "failed to claim alert queue item: {err}"
                    );
                    continue;
                }
                seen.insert(id);
                if audio_tx.send(audio).await.is_err() {
                    break;
                }
            }
            Err(err) => {
                item.status = "failed".to_string();
                item.failed_at = Some(Utc::now().to_rfc3339());
                item.last_error = Some(err.to_string());
                let _ = write_alert_item(&manifest, &item);
            }
        }
    }
    Ok(())
}

fn alert_sort_key(item: &AlertQueueItem, id: &str, feed_id: &str) -> AlertSortKey {
    AlertSortKey {
        group: alert_group_key(item, id, feed_id),
        rank: alert_sequence_rank(item, id),
        created_at: item.created_at.trim().to_string(),
        id: id.to_string(),
    }
}

fn alert_group_key(item: &AlertQueueItem, id: &str, feed_id: &str) -> String {
    let explicit = item.alert_id.trim();
    if !explicit.is_empty() {
        return explicit.to_string();
    }
    let mut value = id.trim().to_string();
    for prefix in [
        format!("000_{feed_id}_"),
        format!("001_{feed_id}_"),
        format!("002_{feed_id}_"),
        format!("cap_alert_{feed_id}_"),
    ] {
        if let Some(stripped) = value.strip_prefix(&prefix) {
            value = stripped.to_string();
            break;
        }
    }
    for suffix in ["_same_header", "_same_eom", "_same_full", "_same", "_cap"] {
        if let Some(stripped) = value.strip_suffix(suffix) {
            value = stripped.to_string();
            break;
        }
    }
    value
}

fn alert_sequence_rank(item: &AlertQueueItem, id: &str) -> u8 {
    let marker = format!(
        "{} {} {}",
        item.r#type.to_ascii_lowercase(),
        item.priority.to_ascii_lowercase(),
        id.to_ascii_lowercase()
    );
    if marker.contains("header") || marker.starts_with("  000_") || id.starts_with("000_") {
        0
    } else if marker.contains("eom") || id.starts_with("002_") {
        2
    } else if marker.contains("cap") || id.starts_with("001_") || id.starts_with("cap_alert_") {
        1
    } else {
        3
    }
}

async fn audio_item_from_alert(
    cfg: &LoadedConfig,
    audio_cache: &AudioCache,
    _feed: &FeedConfig,
    manifest: &Path,
    item: &mut AlertQueueItem,
    id: &str,
) -> Result<AudioItem> {
    let audio_path = resolve_path(&cfg.base_dir, &item.audio_path);
    let pcm = audio_cache
        .read_raw_pcm(
            &audio_path,
            first_positive(item.sample_rate, 48_000),
            item.channels.max(1),
            cfg,
        )
        .await?;
    let duration_ms = pcm_duration_ms(
        pcm.len(),
        cfg.root.playout.sample_rate,
        cfg.root.playout.channels,
    );
    let title = fallback_text(&item.header, "SAME Alert");
    Ok(AudioItem {
        id: id.to_string(),
        package_id: "same_alert".to_string(),
        title,
        pcm,
        metadata: AudioItemMetadata {
            audio_path: item.audio_path.clone(),
            duration_ms,
            alert_packet: item.alert_packet.clone(),
            alert_text: item.alert_text.clone(),
            banner_text: item.banner_text.clone(),
            background_color: item.background_color.clone(),
            priority: item.priority.clone(),
            feed_ids: item.feed_ids.clone(),
            broadcast_immediate: item.broadcast_immediate,
        },
        gap_after: Duration::from_millis(0),
        not_before: None,
        queued_at: Utc::now().to_rfc3339(),
        target_start: String::new(),
        predicted_start: String::new(),
        predicted_finish: String::new(),
        source: ItemSource::Alert {
            manifest_path: manifest.to_path_buf(),
            header: item.header.clone(),
            event: item.event.clone(),
        },
        segment_group: None,
    })
}

fn read_alert_item(path: &Path) -> Result<AlertQueueItem> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read alert manifest {}", path.display()))?;
    let mut item: AlertQueueItem = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse alert manifest {}", path.display()))?;
    if item.id.trim().is_empty() {
        item.id = path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("alert")
            .to_string();
    }
    Ok(item)
}

fn write_alert_item(path: &Path, item: &AlertQueueItem) -> Result<()> {
    let raw = serde_json::to_vec_pretty(item)?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, raw).with_context(|| format!("failed to write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("failed to replace {}", path.display()))
}

fn mark_alert_played(path: &Path) -> Result<()> {
    let mut item = read_alert_item(path)?;
    item.status = "played".to_string();
    item.played_at = Some(Utc::now().to_rfc3339());
    item.last_error = None;
    write_alert_item(path, &item)
}

fn mark_alert_started(path: &Path) -> Result<()> {
    let mut item = read_alert_item(path)?;
    item.status = "playing".to_string();
    item.claimed_at = Some(Utc::now().to_rfc3339());
    item.last_error = None;
    write_alert_item(path, &item)
}

fn mark_alert_event_queued(path: &Path) -> Result<()> {
    let mut item = read_alert_item(path)?;
    item.status = "event_queued".to_string();
    item.claimed_at = Some(Utc::now().to_rfc3339());
    item.failed_at = None;
    item.last_error = None;
    write_alert_item(path, &item)
}

fn alert_targets_feed(item: &AlertQueueItem, feed: &FeedConfig) -> bool {
    alert_feed_id_targets(&item.feed_id, feed)
        || item
            .feed_ids
            .iter()
            .any(|id| alert_feed_id_targets(id, feed))
}

fn alert_feed_id_targets(feed_id: &str, feed: &FeedConfig) -> bool {
    let feed_id = feed_id.trim();
    feed_id == feed.id
        || feed_id == "*"
        || (feed.alert_covers_all_locations() && !feed_id.is_empty())
}

fn alert_pending(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "" | "pending" | "queued" | "claimed" | "event_queued" | "playing"
    )
}

fn remember_priority_item(active_ids: &mut HashSet<String>, id: &str) -> bool {
    let id = id.trim();
    !id.is_empty() && active_ids.insert(id.to_string())
}

fn next_unique_routine_item(
    rx: &mut mpsc::Receiver<AudioItem>,
    active_ids: &mut HashSet<String>,
    completed_ids: &HashSet<String>,
    segment_groups: &mut HashMap<String, SegmentGroupPlayback>,
    feed_id: &str,
) -> Option<AudioItem> {
    while let Ok(item) = rx.try_recv() {
        if item.segment_group.is_some() && segment_group_is_aborted(&item, segment_groups) {
            segment_groups.remove(&item.id);
            tracing::debug!(
                feed_id,
                queue_id = item.id,
                "skipping aborted segment group"
            );
            continue;
        }
        if completed_ids.contains(item.id.trim()) {
            tracing::debug!(
                feed_id,
                queue_id = item.id,
                "skipping completed playlist item"
            );
            continue;
        }
        if remember_priority_item(active_ids, &item.id) {
            return Some(item);
        }
        tracing::debug!(
            feed_id,
            queue_id = item.id,
            "skipping duplicate active playlist item"
        );
    }
    None
}

fn alert_item_stale_for_priority(item: &AlertQueueItem, now: DateTime<Utc>) -> bool {
    if item.message_type.eq_ignore_ascii_case("cancel") {
        return true;
    }
    let header = item.header.to_ascii_lowercase();
    if header.contains("ended") || header.contains("cancelled") || header.contains("canceled") {
        return true;
    }
    if let Some(expires) = parse_time(&item.alert_expires_at) {
        if now > expires {
            return true;
        }
    }
    let Some(sent) = parse_time(&item.alert_sent_at) else {
        return false;
    };
    if now < sent {
        return false;
    }
    let event = item.event.trim().to_ascii_uppercase();
    let limit = if event == "SVR" || event == "TOR" {
        ChronoDuration::minutes(30)
    } else {
        ChronoDuration::minutes(60)
    };
    now.signed_duration_since(sent) > limit
}

fn split_same_part(item: &AlertQueueItem) -> bool {
    let marker = format!(
        "{} {} {}",
        item.r#type.to_ascii_lowercase(),
        item.priority.to_ascii_lowercase(),
        item.id.to_ascii_lowercase()
    );
    marker.contains("same_header") || marker.contains("same_eom")
}

fn legacy_split_alert_item(item: &AlertQueueItem) -> bool {
    if !item.source.trim().is_empty() {
        return false;
    }
    let id = item.id.to_ascii_lowercase();
    id.starts_with("000_") || id.starts_with("cap_alert_")
}

fn item_event_data(feed_id: &str, item: &AudioItem) -> Value {
    json!({
        "feed_id": feed_id,
        "feed_ids": if item.metadata.feed_ids.is_empty() { vec![feed_id.to_string()] } else { item.metadata.feed_ids.clone() },
        "queue_id": item.id,
        "pkg_id": item.package_id,
        "package_id": item.package_id,
        "title": item.title,
        "audio_path": item.metadata.audio_path,
        "duration_ms": item.metadata.duration_ms,
        "alert_packet": item.metadata.alert_packet,
        "alert_text": item.metadata.alert_text,
        "banner_text": item.metadata.banner_text,
        "background_color": item.metadata.background_color,
        "priority": item.metadata.priority,
        "broadcast_immediate": item.metadata.broadcast_immediate,
        "queued_at": item.queued_at,
        "target_start_at": item.target_start,
        "predicted_start_at": item.predicted_start,
        "predicted_finish_at": item.predicted_finish,
    })
}

fn package_gap(cfg: &LoadedConfig) -> Duration {
    Duration::from_secs_f64(cfg.root.playout.pacing.package_gap_s.max(0.0))
}

fn parse_time(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw.trim())
        .map(|value| value.with_timezone(&Utc))
        .ok()
}

fn first_positive(value: u32, fallback: u32) -> u32 {
    if value == 0 {
        fallback
    } else {
        value
    }
}

fn fallback_text(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.trim().to_string()
    } else {
        value.to_string()
    }
}

fn title_for_package(package_id: &str) -> String {
    match package_id.trim().to_ascii_lowercase().as_str() {
        "date_time" => "Date and Time".to_string(),
        "station_id" => "Station Identification".to_string(),
        "current_conditions" => "Current Conditions".to_string(),
        "forecast" => "Forecast".to_string(),
        "air_quality" => "Air Quality".to_string(),
        "climate_summary" => "Climate Summary".to_string(),
        "geophysical_alert" => "Geophysical Alert".to_string(),
        "user_bulletin" => "User Bulletin".to_string(),
        "alerts" => "Weather Alerts".to_string(),
        "" => "Queued Audio".to_string(),
        other => other.to_string(),
    }
}

fn queue_id(prefix: &str) -> String {
    safe_id(&format!(
        "{}-{}",
        fallback_text(prefix, "item"),
        Utc::now().timestamp_nanos_opt().unwrap_or(0)
    ))
}

fn safe_id(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
            out.push(ch);
        }
    }
    if out.is_empty() {
        "item".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn session_shutdown_cancels_and_awaits_tracked_tasks() {
        let tasks = SessionTasks::new();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        tasks.spawn(async move {
            let _drop_signal = DropSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });

        started_rx.await.expect("tracked task started");
        tasks.shutdown().await;

        dropped_rx.await.expect("tracked task was cancelled");
    }

    #[tokio::test]
    async fn session_shutdown_aborts_and_awaits_owned_join_handles() {
        let tasks = SessionTasks::new();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        tasks.track_abortable(task);

        started_rx.await.expect("owned task started");
        tasks.shutdown().await;

        dropped_rx.await.expect("owned task was aborted");
    }

    #[test]
    fn alert_pending_accepts_empty_and_pending_only() {
        assert!(alert_pending(""));
        assert!(alert_pending("pending"));
        assert!(alert_pending("queued"));
        assert!(alert_pending("claimed"));
        assert!(alert_pending("event_queued"));
        assert!(alert_pending("playing"));
        assert!(!alert_pending("played"));
    }

    #[test]
    fn active_priority_ids_reject_duplicate_alerts() {
        let mut active = HashSet::<String>::new();

        assert!(remember_priority_item(&mut active, "alert-1"));
        assert!(!remember_priority_item(&mut active, "alert-1"));

        active.remove("alert-1");
        assert!(remember_priority_item(&mut active, "alert-1"));
    }

    #[test]
    fn alert_manifest_status_updates_preserve_alert_text() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("alert.json");
        fs::write(
            &path,
            r#"{
  "id": "alert-1",
  "type": "same_alert",
  "status": "pending",
  "header": "DMO - Practice/demo Warning",
  "event": "DMO",
  "alert_text": "This is the actual alert script.",
  "banner_text": "This is the actual banner script."
}"#,
        )
        .expect("write manifest");

        mark_alert_started(&path).expect("mark started");
        let started = read_alert_item(&path).expect("read started");
        assert_eq!(started.alert_text, "This is the actual alert script.");
        assert_eq!(started.banner_text, "This is the actual banner script.");

        mark_alert_played(&path).expect("mark played");
        let played = read_alert_item(&path).expect("read played");
        assert_eq!(played.alert_text, "This is the actual alert script.");
        assert_eq!(played.banner_text, "This is the actual banner script.");
    }

    #[test]
    fn item_event_data_includes_cgen_alert_metadata() {
        let item = AudioItem {
            id: "alert-1".to_string(),
            package_id: "same_alert".to_string(),
            title: "Severe Thunderstorm Warning".to_string(),
            pcm: SharedPcm::owned(vec![0; 48_000]),
            metadata: AudioItemMetadata {
                audio_path: "runtime/audio/alerts/alert.raw".to_string(),
                duration_ms: 500,
                alert_packet: Some(json!({"id": "cap-1"})),
                alert_text: "spoken alert".to_string(),
                banner_text: "banner alert".to_string(),
                background_color: "#b91c1c".to_string(),
                priority: "same".to_string(),
                feed_ids: vec!["sk-0001".to_string(), "CAP-IT-ALL".to_string()],
                broadcast_immediate: true,
            },
            gap_after: Duration::from_millis(500),
            not_before: None,
            queued_at: String::new(),
            target_start: String::new(),
            predicted_start: String::new(),
            predicted_finish: String::new(),
            source: ItemSource::Alert {
                manifest_path: PathBuf::from("runtime/queues/alerts/alert-1.json"),
                header: "Severe Thunderstorm Warning".to_string(),
                event: "SVR".to_string(),
            },
            segment_group: None,
        };

        let data = item_event_data("sk-0001", &item);

        assert_eq!(data["audio_path"], "runtime/audio/alerts/alert.raw");
        assert_eq!(data["duration_ms"], 500);
        assert_eq!(data["banner_text"], "banner alert");
        assert_eq!(data["background_color"], "#b91c1c");
        assert_eq!(data["priority"], "same");
        assert_eq!(data["broadcast_immediate"], true);
        assert_eq!(data["feed_ids"][1], "CAP-IT-ALL");
        assert_eq!(data["alert_packet"]["id"], "cap-1");
    }

    #[test]
    fn all_location_alert_feed_accepts_concrete_feed_targets() {
        let feed = FeedConfig {
            id: "CAP-IT-ALL".to_string(),
            playout: crate::config::FeedPlayoutConfig {
                same: Some("true".to_string()),
                ..Default::default()
            },
            alerts: Some(crate::config::FeedAlertsConfig {
                cap_cp: crate::config::FeedAlertProviderConfig {
                    enabled: Some("true".to_string()),
                },
                ..Default::default()
            }),
            locations: crate::config::FeedLocationsConfig::default(),
            ..Default::default()
        };
        let item = AlertQueueItem {
            feed_ids: vec!["sk-0001".to_string()],
            ..Default::default()
        };

        assert!(alert_targets_feed(&item, &feed));
    }

    #[test]
    fn alert_queue_item_accepts_singular_feed_target() {
        let feed = FeedConfig {
            id: "CAP-IT-ALL".to_string(),
            ..Default::default()
        };
        let item = AlertQueueItem {
            feed_id: "CAP-IT-ALL".to_string(),
            ..Default::default()
        };

        assert!(alert_targets_feed(&item, &feed));
    }

    #[test]
    fn safe_id_removes_path_characters() {
        assert_eq!(safe_id("../sk 0001:forecast"), "sk0001forecast");
    }

    #[test]
    fn interrupted_routine_replays_from_sample_zero_after_all_priorities() {
        let routine_pcm = pcm16(&[10, 11, 12, 13]);
        let routine = test_audio_item_with_pcm("routine-1", "Forecast", routine_pcm.clone());
        let mut rendered = vec![0u8; 4];
        let mut position = 0;
        assert_eq!(copy_item_pcm(&routine, &mut position, &mut rendered), 4);

        let mut interrupted_priority = None;
        let mut priority_pending = VecDeque::from([
            test_priority_item("priority-1", &[20, 21]),
            test_priority_item("priority-2", &[30, 31]),
        ]);
        let mut interrupted_routine = None;
        let mut deferred_routine = VecDeque::new();
        let mut active_routine_ids = HashSet::from([routine.id.clone()]);
        remember_interrupted_routine(
            &mut interrupted_routine,
            &mut deferred_routine,
            &mut active_routine_ids,
            "sk-0001",
            routine,
        );

        let mut starts = Vec::new();
        while let Some(item) = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            false,
        ) {
            starts.push(item.id.clone());
            let mut item_position = 0;
            let mut item_pcm = vec![0u8; item.pcm.len()];
            assert_eq!(
                copy_item_pcm(&item, &mut item_position, &mut item_pcm),
                item.pcm.len()
            );
            rendered.extend(item_pcm);
        }

        let mut expected = pcm16(&[10, 11]);
        expected.extend(pcm16(&[20, 21]));
        expected.extend(pcm16(&[30, 31]));
        expected.extend(routine_pcm);
        assert_eq!(starts, ["priority-1", "priority-2", "routine-1"]);
        assert_eq!(rendered, expected);
    }

    #[test]
    fn repeated_priority_interruptions_restart_the_routine_again() {
        let routine = test_audio_item_with_pcm("routine-1", "Forecast", pcm16(&[1, 2, 3, 4]));
        let mut interrupted_priority = None;
        let mut priority_pending = VecDeque::new();
        let mut interrupted_routine = None;
        let mut deferred_routine = VecDeque::new();
        let mut active_routine_ids = HashSet::from([routine.id.clone()]);
        remember_interrupted_routine(
            &mut interrupted_routine,
            &mut deferred_routine,
            &mut active_routine_ids,
            "sk-0001",
            routine,
        );

        let replay = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            false,
        )
        .expect("first replay");
        let mut replay_position = 0;
        let mut prefix = [0u8; 4];
        assert_eq!(copy_item_pcm(&replay, &mut replay_position, &mut prefix), 4);

        remember_interrupted_routine(
            &mut interrupted_routine,
            &mut deferred_routine,
            &mut active_routine_ids,
            "sk-0001",
            replay,
        );
        priority_pending.push_back(test_priority_item("priority-2", &[8, 9]));

        let priority = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            false,
        )
        .expect("second priority");
        let replay = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            false,
        )
        .expect("second replay");
        let mut replay_position = 0;
        let mut replayed_pcm = vec![0u8; replay.pcm.len()];
        copy_item_pcm(&replay, &mut replay_position, &mut replayed_pcm);

        assert_eq!(priority.id, "priority-2");
        assert_eq!(replayed_pcm, pcm16(&[1, 2, 3, 4]));
        assert!(deferred_routine.is_empty());
    }

    #[test]
    fn interrupted_priority_replays_before_queued_priorities_and_routine() {
        let mut interrupted_priority = Some(test_priority_item("priority-active", &[1]));
        let mut priority_pending = VecDeque::from([
            test_priority_item("priority-queued-1", &[2]),
            test_priority_item("priority-queued-2", &[3]),
        ]);
        let mut interrupted_routine = Some(test_audio_item("routine-interrupted", "Forecast"));
        let mut deferred_routine = VecDeque::from([test_audio_item("routine-pending", "Air")]);
        let mut order = Vec::new();

        while let Some(item) = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            false,
        ) {
            order.push(item.id);
        }

        assert_eq!(
            order,
            [
                "priority-active",
                "priority-queued-1",
                "priority-queued-2",
                "routine-interrupted",
                "routine-pending",
            ]
        );
    }

    #[test]
    fn paused_routine_playout_still_allows_priority() {
        let priority = test_priority_item("priority-1", &[1]);
        let routine = test_audio_item("routine-1", "Forecast");
        let mut interrupted_priority = None;
        let mut priority_pending = VecDeque::from([priority.clone()]);
        let mut interrupted_routine = Some(routine.clone());
        let mut deferred_routine = VecDeque::new();

        let selected = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            true,
        )
        .expect("priority while paused");

        assert_eq!(selected.id, "priority-1");
        assert!(item_can_start(&priority, true));
        assert!(!item_can_start(&routine, true));
        assert!(take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            true,
        )
        .is_none());
        assert_eq!(
            interrupted_routine.as_ref().map(|item| item.id.as_str()),
            Some("routine-1")
        );
    }

    #[test]
    fn pending_routine_yields_to_priority_without_becoming_interrupted() {
        let not_before = Utc::now() + ChronoDuration::minutes(1);
        let mut routine = test_audio_item("routine-pending", "Forecast");
        routine.not_before = Some(not_before);
        let mut pending = Some(routine);
        let mut deferred_routine = VecDeque::new();
        let mut active_routine_ids = HashSet::from(["routine-pending".to_string()]);

        assert!(defer_pending_routine(
            &mut pending,
            &mut deferred_routine,
            &mut active_routine_ids,
            "sk-0001",
        ));

        let priority = test_priority_item("priority-1", &[1]);
        let mut interrupted_priority = None;
        let mut priority_pending = VecDeque::from([priority]);
        let mut interrupted_routine = None;
        let first = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            false,
        )
        .expect("priority");
        let second = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            false,
        )
        .expect("deferred routine");

        assert!(pending.is_none());
        assert_eq!(first.id, "priority-1");
        assert_eq!(second.id, "routine-pending");
        assert_eq!(second.not_before, Some(not_before));
        assert!(active_routine_ids.contains("routine-pending"));
    }

    #[test]
    fn priority_bypasses_only_the_existing_routine_gap() {
        let now = Instant::now();
        let routine_gap = now + Duration::from_secs(1);

        assert_eq!(priority_gap_deadline(routine_gap, true, now), now);
        assert_eq!(priority_gap_deadline(routine_gap, false, now), routine_gap);
    }

    #[test]
    fn after_current_action_stays_attached_to_interrupted_routine() {
        let mut deferred = Some(AfterCurrentAction {
            action: "pause_after_current".to_string(),
            target_item_id: "routine-1".to_string(),
        });

        assert_eq!(take_after_current_action(&mut deferred, "priority-1"), None);
        assert!(deferred.is_some());
        assert_eq!(
            take_after_current_action(&mut deferred, "routine-1"),
            Some("pause_after_current".to_string())
        );
        assert!(deferred.is_none());
    }

    #[test]
    fn after_current_flush_actions_clear_deferred_routine_state() {
        assert!(clears_deferred_routine("flush_restart_after_current"));
        assert!(clears_deferred_routine("flush_stop_after_current"));
        assert!(!clears_deferred_routine("pause_after_current"));

        let mut interrupted = Some(test_audio_item("routine-interrupted", "Forecast"));
        let mut deferred = VecDeque::from([test_audio_item("routine-pending", "Air")]);
        let mut active = HashSet::from([
            "routine-interrupted".to_string(),
            "routine-pending".to_string(),
        ]);
        let mut completed = HashSet::from(["routine-old".to_string()]);
        let mut completed_order = VecDeque::from(["routine-old".to_string()]);

        clear_deferred_routine_state(
            &mut interrupted,
            &mut deferred,
            &mut active,
            &mut completed,
            &mut completed_order,
        );

        assert!(interrupted.is_none());
        assert!(deferred.is_empty());
        assert!(active.is_empty());
        assert!(completed.is_empty());
        assert!(completed_order.is_empty());
    }

    #[test]
    fn breakin_completion_does_not_complete_or_consume_interrupted_routine() {
        let routine = test_audio_item_with_pcm("routine-1", "Forecast", pcm16(&[1, 2, 3]));
        let routine_pcm = routine.pcm.clone();
        let mut interrupted_routine = Some(routine);
        let mut live_breakin = Some(LiveBreakIn {
            id: "breakin-1".to_string(),
            title: "Operator Break-in".to_string(),
            buffer: VecDeque::new(),
            finishing: true,
        });

        let completed_breakin = take_finished_breakin(&mut live_breakin).expect("break-in done");
        assert_eq!(completed_breakin.id, "breakin-1");

        let mut after_current = Some(AfterCurrentAction {
            action: "pause_after_current".to_string(),
            target_item_id: "routine-1".to_string(),
        });
        assert_eq!(
            take_after_current_action(&mut after_current, &completed_breakin.id),
            None
        );
        assert!(interrupted_routine.is_some());

        let mut interrupted_priority = None;
        let mut priority_pending = VecDeque::new();
        let mut deferred_routine = VecDeque::new();
        let replay = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred_routine,
            false,
        )
        .expect("interrupted routine replay");
        assert_eq!(replay.id, "routine-1");
        assert_eq!(replay.pcm.as_ref(), routine_pcm.as_ref());
        assert_eq!(
            take_after_current_action(&mut after_current, &replay.id),
            Some("pause_after_current".to_string())
        );
    }

    #[test]
    fn priority_pending_queue_stops_draining_at_capacity() {
        let mut priority_pending = VecDeque::new();
        let mut active_priority_ids = HashSet::new();
        for index in 0..(PRIORITY_PENDING_QUEUE_CAPACITY - 1) {
            let item = test_priority_item(&format!("active-{index}"), &[1]);
            active_priority_ids.insert(item.id.clone());
            priority_pending.push_back(item);
        }
        let (tx, mut rx) = mpsc::channel(4);
        tx.try_send(test_priority_item("queued-1", &[2]))
            .expect("queue first priority");
        tx.try_send(test_priority_item("queued-2", &[3]))
            .expect("queue second priority");

        let accepted = drain_priority_receiver(
            &mut rx,
            &mut priority_pending,
            &mut active_priority_ids,
            "sk-0001",
        );

        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].id, "queued-1");
        assert_eq!(priority_pending.len(), PRIORITY_PENDING_QUEUE_CAPACITY);
        assert_eq!(
            rx.try_recv().expect("backpressured priority").id,
            "queued-2"
        );
    }

    #[test]
    fn restoring_priority_to_front_keeps_pending_queue_bounded() {
        let mut priority_pending = VecDeque::new();
        let mut active_priority_ids = HashSet::new();
        for index in 0..PRIORITY_PENDING_QUEUE_CAPACITY {
            let item = test_priority_item(&format!("priority-{index}"), &[1]);
            active_priority_ids.insert(item.id.clone());
            priority_pending.push_back(item);
        }

        let restored = test_priority_item("priority-restored", &[2]);
        active_priority_ids.insert(restored.id.clone());
        enqueue_priority_front(
            &mut priority_pending,
            &mut active_priority_ids,
            "sk-0001",
            restored,
        );

        assert_eq!(priority_pending.len(), PRIORITY_PENDING_QUEUE_CAPACITY);
        assert_eq!(
            priority_pending.front().map(|item| item.id.as_str()),
            Some("priority-restored")
        );
        assert!(!active_priority_ids.contains("priority-15"));
        assert!(active_priority_ids.contains("priority-restored"));
    }

    #[test]
    fn playout_input_queues_have_explicit_bounds() {
        const {
            assert!(ROUTINE_AUDIO_QUEUE_CAPACITY > 0);
            assert!(PRIORITY_AUDIO_QUEUE_CAPACITY > 0);
            assert!(PRIORITY_PREPARE_QUEUE_CAPACITY > 0);
            assert!(PRIORITY_PENDING_QUEUE_CAPACITY > 0);
            assert!(BREAKIN_COMMAND_QUEUE_CAPACITY > 0);
            assert!(PACKAGE_REQUEST_QUEUE_CAPACITY > 0);
            assert!(CONTROL_QUEUE_CAPACITY > 0);
        }
    }

    #[test]
    fn duplicate_priority_does_not_consume_a_pending_slot() {
        let mut priority_pending = VecDeque::new();
        let mut active_priority_ids = HashSet::from(["duplicate".to_string()]);
        let (tx, mut rx) = mpsc::channel(2);
        tx.try_send(test_priority_item("duplicate", &[1]))
            .expect("queue duplicate");
        tx.try_send(test_priority_item("unique", &[2]))
            .expect("queue unique priority");

        let accepted = drain_priority_receiver(
            &mut rx,
            &mut priority_pending,
            &mut active_priority_ids,
            "sk-0001",
        );

        assert_eq!(accepted.len(), 1);
        assert_eq!(accepted[0].id, "unique");
        assert_eq!(priority_pending.len(), 1);
        assert_eq!(priority_pending[0].id, "unique");
    }

    #[test]
    fn deferred_routine_queue_drops_tail_and_releases_active_id() {
        let mut deferred = VecDeque::new();
        let mut active = HashSet::new();

        active.insert("routine-1".to_string());
        defer_routine_back(
            &mut deferred,
            &mut active,
            "sk-0001",
            test_audio_item("routine-1", "One"),
        );
        active.insert("routine-2".to_string());
        defer_routine_back(
            &mut deferred,
            &mut active,
            "sk-0001",
            test_audio_item("routine-2", "Two"),
        );
        active.insert("routine-3".to_string());
        defer_routine_back(
            &mut deferred,
            &mut active,
            "sk-0001",
            test_audio_item("routine-3", "Three"),
        );

        assert_eq!(deferred.len(), DEFERRED_ROUTINE_QUEUE_CAPACITY);
        assert_eq!(deferred[0].id, "routine-1");
        assert_eq!(deferred[1].id, "routine-3");
        assert!(active.contains("routine-1"));
        assert!(!active.contains("routine-2"));
        assert!(active.contains("routine-3"));
    }

    #[test]
    fn completed_routine_history_is_bounded() {
        let mut completed = HashSet::<String>::new();
        let mut order = VecDeque::<String>::new();

        for index in 0..(COMPLETED_ROUTINE_HISTORY_CAPACITY + 2) {
            remember_completed_routine_item(
                &mut completed,
                &mut order,
                &format!("routine-{index}"),
            );
        }

        assert_eq!(completed.len(), COMPLETED_ROUTINE_HISTORY_CAPACITY);
        assert_eq!(order.len(), COMPLETED_ROUTINE_HISTORY_CAPACITY);
        assert!(!completed.contains("routine-0"));
        assert!(!completed.contains("routine-1"));
        assert!(completed.contains("routine-2"));
    }

    #[tokio::test]
    async fn duplicate_routine_items_are_discarded() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(test_audio_item("routine-1", "Forecast"))
            .await
            .expect("send first routine");
        tx.send(test_audio_item("routine-1", "Forecast duplicate"))
            .await
            .expect("send duplicate routine");
        tx.send(test_audio_item("routine-2", "Air Quality"))
            .await
            .expect("send second routine");
        drop(tx);

        let mut active = HashSet::<String>::new();
        let completed = HashSet::<String>::new();
        let mut groups = HashMap::new();
        let first =
            next_unique_routine_item(&mut rx, &mut active, &completed, &mut groups, "sk-0001")
                .expect("first routine item");
        let second =
            next_unique_routine_item(&mut rx, &mut active, &completed, &mut groups, "sk-0001")
                .expect("second routine item");

        assert_eq!(first.id, "routine-1");
        assert_eq!(second.id, "routine-2");
    }

    #[tokio::test]
    async fn completed_routine_items_are_discarded() {
        let (tx, mut rx) = mpsc::channel(3);
        tx.send(test_audio_item("routine-1", "Already done"))
            .await
            .expect("send completed routine");
        tx.send(test_audio_item("routine-2", "Forecast"))
            .await
            .expect("send next routine");
        drop(tx);

        let mut active = HashSet::<String>::new();
        let completed = HashSet::from(["routine-1".to_string()]);
        let mut groups = HashMap::new();
        let item =
            next_unique_routine_item(&mut rx, &mut active, &completed, &mut groups, "sk-0001")
                .expect("next routine item");

        assert_eq!(item.id, "routine-2");
    }

    fn test_audio_item(id: &str, title: &str) -> AudioItem {
        test_audio_item_with_pcm(id, title, vec![1; 16])
    }

    fn test_audio_item_with_pcm(id: &str, title: &str, pcm: Vec<u8>) -> AudioItem {
        AudioItem {
            id: id.to_string(),
            package_id: "forecast".to_string(),
            title: title.to_string(),
            pcm: SharedPcm::owned(pcm),
            metadata: AudioItemMetadata::default(),
            gap_after: Duration::from_secs(1),
            not_before: None,
            queued_at: String::new(),
            target_start: String::new(),
            predicted_start: String::new(),
            predicted_finish: String::new(),
            source: ItemSource::Playlist,
            segment_group: None,
        }
    }

    fn test_priority_item(id: &str, samples: &[i16]) -> AudioItem {
        AudioItem {
            id: id.to_string(),
            package_id: "same_alert".to_string(),
            title: id.to_string(),
            pcm: SharedPcm::owned(pcm16(samples)),
            metadata: AudioItemMetadata::default(),
            gap_after: Duration::ZERO,
            not_before: None,
            queued_at: String::new(),
            target_start: String::new(),
            predicted_start: String::new(),
            predicted_finish: String::new(),
            source: ItemSource::Alert {
                manifest_path: PathBuf::from(format!("runtime/queues/alerts/{id}.json")),
                header: id.to_string(),
                event: "SVR".to_string(),
            },
            segment_group: None,
        }
    }

    fn pcm16(samples: &[i16]) -> Vec<u8> {
        samples
            .iter()
            .flat_map(|sample| sample.to_le_bytes())
            .collect()
    }

    fn test_group_request(
        queue_id: &str,
        revision: u64,
        expected_segments: usize,
    ) -> SegmentGroupRequest {
        SegmentGroupRequest {
            queue_id: queue_id.to_string(),
            feed_id: "sk-0001".to_string(),
            package_id: "forecast".to_string(),
            title: "Forecast".to_string(),
            manifest_path: format!("runtime/audio/playout/{queue_id}.json"),
            revision,
            status: SegmentGroupStatus::Open,
            expected_segments,
            not_before: None,
        }
    }

    fn test_group_segment(
        index: usize,
        samples: &[i16],
        pause_after_ms: u64,
    ) -> SegmentGroupSegment {
        SegmentGroupSegment {
            index,
            audio_path: format!("runtime/audio/playout/segment-{index}.raw"),
            duration_ms: 1_000,
            pause_after_ms,
            pcm: SharedPcm::owned(pcm16(samples)),
        }
    }

    fn test_group_revision(
        queue_id: &str,
        revision: u64,
        expected_segments: usize,
        status: SegmentGroupStatus,
        segments: Vec<SegmentGroupSegment>,
    ) -> SegmentGroupRevision {
        SegmentGroupRevision {
            request: test_group_request(queue_id, revision, expected_segments),
            status,
            segments,
        }
    }

    fn test_group_item(group: Arc<SegmentGroup>) -> AudioItem {
        AudioItem {
            id: group.queue_id.clone(),
            package_id: group.package_id.clone(),
            title: group.title.clone(),
            pcm: SharedPcm::owned(Vec::new()),
            metadata: AudioItemMetadata::default(),
            gap_after: Duration::ZERO,
            not_before: group.not_before,
            queued_at: String::new(),
            target_start: String::new(),
            predicted_start: String::new(),
            predicted_finish: String::new(),
            source: ItemSource::Playlist,
            segment_group: Some(group),
        }
    }

    #[test]
    fn newer_manifest_revision_promotes_a_lagged_event_before_apply() {
        let mut request = test_group_request("group-1", 3, 2);
        let manifest: SegmentGroupManifest =
            serde_json::from_value(json!({"revision": 5})).expect("deserialize manifest revision");
        promote_request_to_manifest_revision(&mut request, manifest.revision)
            .expect("newer manifest revision");
        let revision = SegmentGroupRevision {
            request,
            status: SegmentGroupStatus::Open,
            segments: vec![
                test_group_segment(0, &[1], 0),
                test_group_segment(1, &[2], 0),
            ],
        };
        let mut group = SegmentGroupPlayback::new(&revision);

        group
            .apply_revision(revision)
            .expect("apply promoted revision");

        assert_eq!(group.last_revision, Some(5));
    }

    #[test]
    fn zero_or_older_manifest_revision_is_rejected() {
        let mut request = test_group_request("group-1", 5, 2);

        assert!(promote_request_to_manifest_revision(&mut request, 0).is_err());
        assert!(promote_request_to_manifest_revision(&mut request, 4).is_err());
        assert_eq!(request.revision, 5);
    }

    #[test]
    fn segment_group_mailbox_coalesces_per_queue_without_losing_other_groups() {
        let mailbox = Arc::new(StdMutex::new(SegmentGroupMailbox::default()));
        let (queue_ids, mut queued_ids) = mpsc::channel(MAX_OPEN_SEGMENT_GROUPS);

        for revision in 1..=64 {
            submit_segment_group_request(
                &mailbox,
                &queue_ids,
                test_group_request("group-a", revision, 2),
            )
            .expect("queue coalesced group-a revision");
        }
        submit_segment_group_request(&mailbox, &queue_ids, test_group_request("group-a", 63, 2))
            .expect("ignore stale group-a revision");
        submit_segment_group_request(&mailbox, &queue_ids, test_group_request("group-b", 1, 2))
            .expect("queue group-b");

        let first_queue_id = queued_ids.try_recv().expect("first queue token");
        assert_eq!(first_queue_id, "group-a");
        let first = take_segment_group_request(&mailbox, &first_queue_id).expect("group-a request");
        assert_eq!(first.revision, 64);

        submit_segment_group_request(&mailbox, &queue_ids, test_group_request("group-a", 65, 2))
            .expect("queue group-a update during processing");

        let second_queue_id = queued_ids.try_recv().expect("second queue token");
        assert_eq!(second_queue_id, "group-b");
        let second =
            take_segment_group_request(&mailbox, &second_queue_id).expect("group-b request");
        assert_eq!(second.revision, 1);

        let third_queue_id = queued_ids.try_recv().expect("updated queue token");
        assert_eq!(third_queue_id, "group-a");
        let third =
            take_segment_group_request(&mailbox, &third_queue_id).expect("updated group-a request");
        assert_eq!(third.revision, 65);
        assert!(queued_ids.try_recv().is_err());
    }

    #[test]
    fn segment_group_revisions_merge_immutable_prefixes_and_dedupe_old_revisions() {
        let first = test_group_revision(
            "group-1",
            1,
            3,
            SegmentGroupStatus::Open,
            vec![
                test_group_segment(0, &[1], 0),
                test_group_segment(1, &[2], 0),
            ],
        );
        let mut group = SegmentGroupPlayback::new(&first);
        group.apply_revision(first).expect("apply first revision");

        let second = test_group_revision(
            "group-1",
            2,
            3,
            SegmentGroupStatus::Open,
            vec![
                test_group_segment(0, &[1], 0),
                test_group_segment(1, &[2], 0),
                test_group_segment(2, &[3], 0),
            ],
        );
        group.apply_revision(second).expect("apply second revision");
        assert_eq!(group.last_revision, Some(2));
        assert!(group.segments.iter().all(Option::is_some));

        let stale = test_group_revision(
            "group-1",
            1,
            3,
            SegmentGroupStatus::Open,
            vec![test_group_segment(0, &[9], 0)],
        );
        group.apply_revision(stale).expect("ignore stale revision");
        assert_eq!(group.last_revision, Some(2));
        assert_eq!(
            group.segments[0].as_ref().expect("segment zero").audio_path,
            "runtime/audio/playout/segment-0.raw"
        );
    }

    #[test]
    fn segment_group_admission_requires_buffered_prefix_or_short_seal() {
        let long_single = test_group_revision(
            "group-1",
            1,
            3,
            SegmentGroupStatus::Open,
            vec![test_group_segment(0, &[1], 0)],
        );
        let mut group = SegmentGroupPlayback::new(&long_single);
        group
            .apply_revision(long_single)
            .expect("apply single segment");
        assert!(!group.can_admit());

        let buffered = test_group_revision(
            "group-1",
            2,
            3,
            SegmentGroupStatus::Open,
            vec![
                test_group_segment(0, &[1], 0),
                test_group_segment(1, &[2], 0),
            ],
        );
        group
            .apply_revision(buffered)
            .expect("apply buffered prefix");
        assert!(group.can_admit());

        let sealed = test_group_revision(
            "group-2",
            1,
            1,
            SegmentGroupStatus::Sealed,
            vec![test_group_segment(0, &[7], 0)],
        );
        let mut short_group = SegmentGroupPlayback::new(&sealed);
        short_group
            .apply_revision(sealed)
            .expect("apply sealed group");
        assert!(short_group.can_admit());
    }

    #[test]
    fn segment_group_underflow_holds_at_boundary_and_applies_segment_pause() {
        let revision = test_group_revision(
            "group-1",
            1,
            3,
            SegmentGroupStatus::Open,
            vec![
                test_group_segment(0, &[1], 2),
                test_group_segment(1, &[2], 0),
            ],
        );
        let mut group = SegmentGroupPlayback::new(&revision);
        group.apply_revision(revision).expect("apply revision");
        let mut groups = HashMap::from([("group-1".to_string(), group)]);
        let mut out = vec![0u8; 10];

        let state = copy_segment_group_pcm("group-1", &mut groups, 1_000, 1, &mut out);

        assert_eq!(state, SegmentGroupRender::Underflow);
        assert_eq!(&out[0..2], &pcm16(&[1]));
        assert_eq!(&out[2..6], &[0, 0, 0, 0]);
        assert_eq!(&out[6..8], &pcm16(&[2]));
        assert_eq!(groups["group-1"].segment_index, 2);
    }

    #[test]
    fn segment_group_abort_stops_rendering_without_completion() {
        let first = test_group_revision(
            "group-1",
            1,
            2,
            SegmentGroupStatus::Open,
            vec![
                test_group_segment(0, &[1], 0),
                test_group_segment(1, &[2], 0),
            ],
        );
        let mut group = SegmentGroupPlayback::new(&first);
        group.apply_revision(first).expect("apply open revision");
        let aborted = test_group_revision("group-1", 2, 2, SegmentGroupStatus::Aborted, Vec::new());
        group
            .apply_revision(aborted)
            .expect("apply aborted revision");
        let mut groups = HashMap::from([("group-1".to_string(), group)]);
        let mut out = vec![0u8; 2];

        assert_eq!(
            copy_segment_group_pcm("group-1", &mut groups, 1_000, 1, &mut out),
            SegmentGroupRender::Aborted
        );
    }

    #[test]
    fn aborted_group_revision_skips_a_missing_manifest_and_preserves_identity() {
        let mut first = test_group_revision(
            "group-1",
            1,
            2,
            SegmentGroupStatus::Open,
            vec![
                test_group_segment(0, &[1], 0),
                test_group_segment(1, &[2], 0),
            ],
        );
        first.request.manifest_path = "runtime/audio/playout/missing-group-1.json".to_string();
        let mut group = SegmentGroupPlayback::new(&first);
        group.apply_revision(first).expect("apply open revision");

        let mut aborted_request = test_group_request("group-1", 2, 2);
        aborted_request.status = SegmentGroupStatus::Aborted;
        aborted_request.manifest_path = "runtime/audio/playout/missing-group-1.json".to_string();
        assert!(!Path::new(&aborted_request.manifest_path).exists());
        let aborted = aborted_segment_group_revision(&aborted_request)
            .expect("abort revision does not read its manifest");

        group.apply_revision(aborted).expect("apply abort revision");
        assert_eq!(group.status, SegmentGroupStatus::Aborted);
        assert_eq!(group.last_revision, Some(2));
    }

    #[test]
    fn alert_preemption_preserves_segment_group_identity_and_order() {
        let revision = test_group_revision(
            "group-1",
            1,
            2,
            SegmentGroupStatus::Sealed,
            vec![
                test_group_segment(0, &[1], 0),
                test_group_segment(1, &[2], 0),
            ],
        );
        let mut group = SegmentGroupPlayback::new(&revision);
        group.apply_revision(revision).expect("apply revision");
        let routine = test_group_item(Arc::clone(&group.group));
        let mut groups = HashMap::from([("group-1".to_string(), group)]);
        let mut first = vec![0u8; 2];
        assert_eq!(
            copy_segment_group_pcm("group-1", &mut groups, 1_000, 1, &mut first),
            SegmentGroupRender::Playing
        );

        let mut interrupted_priority = None;
        let mut priority_pending = VecDeque::from([test_priority_item("alert-1", &[9])]);
        let mut interrupted_routine = Some(routine);
        let mut deferred = VecDeque::new();
        let priority = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred,
            false,
        )
        .expect("priority item");
        let resumed = take_next_buffered_item(
            &mut interrupted_priority,
            &mut priority_pending,
            &mut interrupted_routine,
            &mut deferred,
            false,
        )
        .expect("resumed group");
        let mut resumed_pcm = vec![0u8; 2];

        assert!(priority.is_alert());
        assert_eq!(resumed.id, "group-1");
        assert_eq!(
            copy_segment_group_pcm("group-1", &mut groups, 1_000, 1, &mut resumed_pcm),
            SegmentGroupRender::Playing
        );
        assert_eq!(resumed_pcm, pcm16(&[2]));
    }

    #[test]
    fn ordinary_playlist_items_still_copy_pcm_directly() {
        let item = test_audio_item_with_pcm("routine-1", "Forecast", pcm16(&[4, 5]));
        let mut out = vec![0u8; 4];
        let mut position = 0;

        assert!(item.segment_group.is_none());
        assert_eq!(copy_item_pcm(&item, &mut position, &mut out), 4);
        assert_eq!(out, pcm16(&[4, 5]));
    }

    #[test]
    fn segment_group_event_rejects_excessive_expected_segments_and_invalid_status() {
        let excessive = json!({
            "queue_id": "group-1",
            "feed_id": "sk-0001",
            "package_id": "forecast",
            "title": "Forecast",
            "manifest_path": "runtime/audio/playout/group-1.json",
            "revision": 1,
            "status": "open",
            "expected_segments": MAX_SEGMENTS_PER_GROUP + 1,
        });
        assert!(segment_group_request_from_event(&excessive).is_err());

        let invalid_status = json!({
            "queue_id": "group-1",
            "feed_id": "sk-0001",
            "package_id": "forecast",
            "title": "Forecast",
            "manifest_path": "runtime/audio/playout/group-1.json",
            "revision": 1,
            "status": "partial",
            "expected_segments": 2,
        });
        assert!(segment_group_request_from_event(&invalid_status).is_err());

        let template = test_group_revision("template", 1, 1, SegmentGroupStatus::Open, Vec::new());
        let mut groups = HashMap::new();
        for index in 0..MAX_OPEN_SEGMENT_GROUPS {
            groups.insert(
                format!("group-{index}"),
                SegmentGroupPlayback::new(&template),
            );
        }
        assert!(ensure_segment_group_capacity(&groups).is_err());
    }

    #[tokio::test]
    async fn audio_cache_coalesces_concurrent_loads() {
        let cache = Arc::new(AudioCache::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(12));
        let mut tasks = Vec::new();

        for _ in 0..12 {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .get_or_load(
                        SharedAudioKey::Synth {
                            text: "shared package".to_string(),
                            reader_id: "00".to_string(),
                            language: "en-US".to_string(),
                            sample_rate: 48_000,
                            channels: 1,
                        },
                        move || async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(25)).await;
                            Ok(SharedPcm::owned(vec![7u8; 4]))
                        },
                    )
                    .await
                    .expect("audio cache load")
            }));
        }

        let mut results = Vec::new();
        for task in tasks {
            results.push(task.await.expect("task"));
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for result in &results[1..] {
            assert_eq!(results[0].as_ref().as_ptr(), result.as_ref().as_ptr());
        }
    }

    #[test]
    fn audio_cache_prunes_idle_ready_entries() {
        let mut entries = HashMap::new();
        let stale_key = test_shared_audio_key("stale");
        let fresh_key = test_shared_audio_key("fresh");
        let now = Instant::now();
        entries.insert(
            stale_key.clone(),
            SharedAudioEntry::Ready {
                pcm: SharedPcm::owned(vec![1u8; 4]),
                bytes: 4,
                last_used: now
                    .checked_sub(AUDIO_CACHE_MAX_IDLE + Duration::from_secs(1))
                    .expect("past instant"),
            },
        );
        entries.insert(
            fresh_key.clone(),
            SharedAudioEntry::Ready {
                pcm: SharedPcm::owned(vec![2u8; 4]),
                bytes: 4,
                last_used: now,
            },
        );

        prune_idle_audio_cache(&mut entries, now, None);

        assert!(!entries.contains_key(&stale_key));
        assert!(entries.contains_key(&fresh_key));
    }

    #[test]
    fn audio_cache_idle_prune_preserves_requested_key() {
        let mut entries = HashMap::new();
        let key = test_shared_audio_key("preserve");
        let now = Instant::now();
        entries.insert(
            key.clone(),
            SharedAudioEntry::Ready {
                pcm: SharedPcm::owned(vec![1u8; 4]),
                bytes: 4,
                last_used: now
                    .checked_sub(AUDIO_CACHE_MAX_IDLE + Duration::from_secs(1))
                    .expect("past instant"),
            },
        );

        prune_idle_audio_cache(&mut entries, now, Some(&key));

        assert!(entries.contains_key(&key));
    }

    fn test_shared_audio_key(text: &str) -> SharedAudioKey {
        SharedAudioKey::Synth {
            text: text.to_string(),
            reader_id: "00".to_string(),
            language: "en-US".to_string(),
            sample_rate: 48_000,
            channels: 1,
        }
    }

    #[test]
    fn matching_pcm16_wav_uses_file_backed_audio() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("queued.wav");
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: 48_000,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut writer = hound::WavWriter::create(&path, spec).expect("create WAV");
        writer.write_sample::<i16>(1234).expect("write sample");
        writer.write_sample::<i16>(-1234).expect("write sample");
        writer.finalize().expect("finalize WAV");

        let pcm = map_pcm16_wav_file(&path, 48_000, 1)
            .expect("map WAV")
            .expect("matching PCM WAV");

        assert!(matches!(pcm, SharedPcm::Mapped(_)));
        assert_eq!(pcm.heap_bytes(), 0);
        assert_eq!(pcm.as_ref(), &[0xd2, 0x04, 0x2e, 0xfb]);
    }

    #[test]
    fn relative_workdir_recognizes_absolute_runtime_audio_path() {
        let absolute = std::env::current_dir()
            .expect("current directory")
            .join("runtime/audio/playlist/feed/item.wav");

        assert!(runtime_audio_path(&absolute, Path::new(".")));
    }

    #[test]
    fn runtime_raw_pcm_mapping_matches_file_deletion_platform_support() {
        #[cfg(windows)]
        assert!(!runtime_raw_pcm_mmap_supported());

        #[cfg(not(windows))]
        assert!(runtime_raw_pcm_mmap_supported());
    }

    #[test]
    fn mismatched_wav_format_falls_back_to_decoder() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("queued.wav");
        let spec = hound::WavSpec {
            channels: 2,
            sample_rate: 44_100,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let writer = hound::WavWriter::create(&path, spec).expect("create WAV");
        writer.finalize().expect("finalize WAV");

        assert!(map_pcm16_wav_file(&path, 48_000, 1)
            .expect("inspect WAV")
            .is_none());
    }

    #[test]
    fn alert_sort_key_keeps_same_speech_eom_sequence_together() {
        let feed_id = "sk-0001";
        let mut parts = [
            AlertQueueItem {
                id: "002_sk-0001_ALERT_same_eom".to_string(),
                alert_id: "ALERT".to_string(),
                r#type: "same_eom".to_string(),
                ..AlertQueueItem::default()
            },
            AlertQueueItem {
                id: "001_sk-0001_ALERT_cap".to_string(),
                alert_id: "ALERT".to_string(),
                r#type: "cap_alert".to_string(),
                ..AlertQueueItem::default()
            },
            AlertQueueItem {
                id: "000_sk-0001_ALERT_same_header".to_string(),
                alert_id: "ALERT".to_string(),
                r#type: "same_header".to_string(),
                ..AlertQueueItem::default()
            },
        ];

        parts.sort_by_key(|item| alert_sort_key(item, &item.id, feed_id));

        let ordered: Vec<&str> = parts.iter().map(|item| item.r#type.as_str()).collect();
        assert_eq!(ordered, ["same_header", "cap_alert", "same_eom"]);
    }

    #[test]
    fn split_same_parts_are_not_standalone_alert_items() {
        assert!(split_same_part(&AlertQueueItem {
            r#type: "same_header".to_string(),
            ..AlertQueueItem::default()
        }));
        assert!(split_same_part(&AlertQueueItem {
            id: "002_sk-0001_ALERT_same_eom".to_string(),
            ..AlertQueueItem::default()
        }));
        assert!(!split_same_part(&AlertQueueItem {
            r#type: "same_alert".to_string(),
            ..AlertQueueItem::default()
        }));
    }

    #[test]
    fn legacy_split_alert_items_are_superseded() {
        assert!(legacy_split_alert_item(&AlertQueueItem {
            id: "000_sk-0001_ALERT_same".to_string(),
            ..AlertQueueItem::default()
        }));
        assert!(legacy_split_alert_item(&AlertQueueItem {
            id: "cap_alert_sk-0001_ALERT".to_string(),
            ..AlertQueueItem::default()
        }));
        assert!(legacy_split_alert_item(&AlertQueueItem {
            id: "000_sk-0001_ALERT_same".to_string(),
            r#type: "same_alert".to_string(),
            priority: "same".to_string(),
            ..AlertQueueItem::default()
        }));
        assert!(!legacy_split_alert_item(&AlertQueueItem {
            id: "000_sk-0001_ALERT_same".to_string(),
            r#type: "same_alert".to_string(),
            priority: "same".to_string(),
            source: "cap-same-tts".to_string(),
            ..AlertQueueItem::default()
        }));
    }

    #[test]
    fn stale_priority_alerts_are_superseded_by_alert_sent_time() {
        let now = DateTime::parse_from_rfc3339("2026-06-16T23:40:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(!alert_item_stale_for_priority(
            &AlertQueueItem {
                event: "SVR".to_string(),
                alert_sent_at: "2026-06-16T23:11:00Z".to_string(),
                ..AlertQueueItem::default()
            },
            now
        ));
        assert!(alert_item_stale_for_priority(
            &AlertQueueItem {
                event: "SVR".to_string(),
                alert_sent_at: "2026-06-16T23:09:00Z".to_string(),
                ..AlertQueueItem::default()
            },
            now
        ));
        assert!(!alert_item_stale_for_priority(
            &AlertQueueItem {
                event: "SVA".to_string(),
                alert_sent_at: "2026-06-16T22:41:00Z".to_string(),
                ..AlertQueueItem::default()
            },
            now
        ));
        assert!(alert_item_stale_for_priority(
            &AlertQueueItem {
                event: "SVA".to_string(),
                alert_sent_at: "2026-06-16T22:39:00Z".to_string(),
                ..AlertQueueItem::default()
            },
            now
        ));
    }

    #[test]
    fn cancelled_priority_alerts_are_superseded() {
        let now = DateTime::parse_from_rfc3339("2026-06-16T23:40:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(alert_item_stale_for_priority(
            &AlertQueueItem {
                message_type: "Cancel".to_string(),
                ..AlertQueueItem::default()
            },
            now
        ));
        assert!(alert_item_stale_for_priority(
            &AlertQueueItem {
                header: "yellow warning - severe thunderstorm - ended".to_string(),
                ..AlertQueueItem::default()
            },
            now
        ));
    }

    #[test]
    fn cancelled_priority_ready_payloads_are_rejected_before_playout() {
        assert!(priority_request_is_cancelled(&json!({
            "message_type": "Cancel"
        })));
        assert!(priority_request_is_cancelled(&json!({
            "status": "superseded"
        })));
        assert!(priority_request_is_cancelled(&json!({
            "header": "Severe Thunderstorm Warning ended"
        })));
        assert!(!priority_request_is_cancelled(&json!({
            "status": "queued",
            "header": "Severe Thunderstorm Warning"
        })));
    }

    #[test]
    fn live_breakin_trim_drops_oldest_audio_on_frame_boundaries() {
        let channels = 1;
        let max = max_live_breakin_buffer_for(48_000, channels);
        let mut live = LiveBreakIn {
            id: "breakin".to_string(),
            title: "Break-in".to_string(),
            buffer: (0..(max + 8)).map(|value| (value % 251) as u8).collect(),
            finishing: false,
        };

        trim_live_breakin_buffer(&mut live, 48_000, channels);

        assert!(live.buffer.len() <= max);
        assert_eq!(live.buffer.len() % live_breakin_frame_bytes(channels), 0);
        assert_eq!(live.buffer.front().copied(), Some(8));
    }

    #[test]
    fn live_breakin_drain_copies_available_audio_without_touching_tail() {
        let mut live = LiveBreakIn {
            id: "breakin".to_string(),
            title: "Break-in".to_string(),
            buffer: VecDeque::from(vec![1, 2, 3]),
            finishing: false,
        };
        let mut out = [0u8; 6];

        drain_live_breakin_buffer(&mut live, &mut out);

        assert_eq!(out, [1, 2, 3, 0, 0, 0]);
        assert!(live.buffer.is_empty());
    }

    #[test]
    fn finished_empty_breakin_is_removed_before_rendering_silence() {
        let mut live = Some(LiveBreakIn {
            id: "breakin".to_string(),
            title: "Break-in".to_string(),
            buffer: VecDeque::new(),
            finishing: true,
        });

        let finished = take_finished_breakin(&mut live).expect("finished break-in");

        assert_eq!(finished.id, "breakin");
        assert!(live.is_none());
    }

    #[test]
    fn realtime_pcm_publish_queue_stays_short_but_smooth() {
        assert!(media_publish_chunk_duration() <= Duration::from_millis(100));
        assert!(media_publish_queue_duration() <= Duration::from_millis(300));
        assert!(pcm_publish_queue_capacity() >= 3);
    }

    #[test]
    fn alert_pcm_event_includes_queue_and_timing_contract() {
        let chunk = PcmPublish {
            data: vec![1, 0, 2, 0],
            duration_ms: 20,
            sequence: 7,
            pts_ns: 140_000_000,
            discontinuity: true,
            queue_id: Some("alert-7".to_string()),
            media_kind: PcmMediaKind::Alert,
        };

        let event = pcm_publish_event("sk-0001", 48_000, 1, &chunk);

        assert_eq!(event["type"], "playout.pcm");
        assert_eq!(event["feed_id"], "sk-0001");
        assert!(event["timestamp"].as_str().is_some());
        assert_eq!(event["queue_id"], "alert-7");
        assert_eq!(event["data"]["feed_id"], "sk-0001");
        assert_eq!(event["data"]["sample_rate"], 48_000);
        assert_eq!(event["data"]["channels"], 1);
        assert_eq!(event["data"]["channel_layout"], "mono");
        assert_eq!(event["data"]["duration_ms"], 20);
        assert_eq!(event["data"]["sequence"], 7);
        assert_eq!(event["data"]["pts_ns"], 140_000_000u64);
        assert_eq!(event["data"]["discontinuity"], true);
        assert_eq!(event["data"]["queue_id"], "alert-7");
        assert_eq!(event["data"]["media_kind"], "alert");
        assert_eq!(event["data"]["pcm"], "AQACAA==");
    }

    #[test]
    fn routine_pcm_event_preserves_audio_fields_without_queue_id() {
        let chunk = PcmPublish {
            data: vec![1, 2],
            duration_ms: 40,
            sequence: 8,
            pts_ns: 160_000_000,
            discontinuity: false,
            queue_id: None,
            media_kind: PcmMediaKind::Routine,
        };

        let event = pcm_publish_event("sk-0001", 48_000, 2, &chunk);

        assert!(event.get("queue_id").is_none());
        assert!(event["data"].get("queue_id").is_none());
        assert_eq!(event["data"]["sample_rate"], 48_000);
        assert_eq!(event["data"]["channels"], 2);
        assert_eq!(event["data"]["duration_ms"], 40);
        assert_eq!(event["data"]["channel_layout"], "stereo");
        assert_eq!(event["data"]["media_kind"], "routine");
        assert_eq!(event["data"]["pcm"], "AQI=");
    }

    #[test]
    fn pcm_builder_sequences_chunks_and_marks_explicit_discontinuities() {
        let context = PcmMediaContext {
            queue_id: Some("alert-1".to_string()),
            media_kind: PcmMediaKind::Alert,
        };
        let mut builder = PcmPublishBuilder::new(8);

        builder.push(&[1, 2], 20, 0, context.clone());
        let first = builder.take().expect("first publish");
        assert_eq!(first.sequence, 0);
        assert_eq!(first.pts_ns, 0);
        assert!(first.discontinuity);

        builder.recycle(first.data);
        builder.push(&[3, 4], 20, 20_000_000, context.clone());
        let second = builder.take().expect("second publish");
        assert_eq!(second.sequence, 1);
        assert_eq!(second.pts_ns, 20_000_000);
        assert!(!second.discontinuity);

        builder.recycle(second.data);
        builder.mark_discontinuity();
        builder.push(&[5, 6], 20, 90_000_000, context);
        let after_gap = builder.take().expect("publish after gap");
        assert_eq!(after_gap.sequence, 2);
        assert_eq!(after_gap.pts_ns, 90_000_000);
        assert!(after_gap.discontinuity);
    }

    #[test]
    fn publisher_failure_marks_first_reconnected_chunk_discontinuous() {
        let mut state = PcmDeliveryState::default();
        let mut failed = PcmPublish {
            data: Vec::new(),
            duration_ms: 20,
            sequence: 4,
            pts_ns: 80_000_000,
            discontinuity: false,
            queue_id: None,
            media_kind: PcmMediaKind::Routine,
        };
        state.prepare(&mut failed);
        assert!(!failed.discontinuity);
        state.record_result(false);

        let mut reconnected = PcmPublish {
            sequence: 5,
            pts_ns: 100_000_000,
            ..failed
        };
        reconnected.discontinuity = false;
        state.prepare(&mut reconnected);
        assert!(reconnected.discontinuity);
        state.record_result(true);

        let mut continuous = PcmPublish {
            sequence: 6,
            pts_ns: 120_000_000,
            discontinuity: false,
            ..reconnected
        };
        state.prepare(&mut continuous);
        assert!(!continuous.discontinuity);
    }

    #[test]
    fn active_pcm_context_correlates_only_priority_audio() {
        let routine = test_audio_item("routine-1", "Forecast");
        let alert = test_priority_item("alert-1", &[1]);
        let live = LiveBreakIn {
            id: "breakin-1".to_string(),
            title: "Break-in".to_string(),
            buffer: VecDeque::new(),
            finishing: false,
        };

        let routine_context = PcmMediaContext::active(None, Some(&routine));
        assert_eq!(routine_context.media_kind, PcmMediaKind::Routine);
        assert_eq!(routine_context.queue_id, None);

        let alert_context = PcmMediaContext::active(None, Some(&alert));
        assert_eq!(alert_context.media_kind, PcmMediaKind::Alert);
        assert_eq!(alert_context.queue_id.as_deref(), Some("alert-1"));

        let breakin_context = PcmMediaContext::active(Some(&live), Some(&alert));
        assert_eq!(breakin_context.media_kind, PcmMediaKind::OperatorBreakIn);
        assert_eq!(breakin_context.queue_id.as_deref(), Some("breakin-1"));

        assert_eq!(
            PcmMediaContext::active(None, None).media_kind,
            PcmMediaKind::Silence
        );
    }

    #[test]
    fn pcm_pts_uses_sample_frames_and_advances_across_dropped_time() {
        assert_eq!(pcm_frame_count(1_920, 1), 960);
        assert_eq!(sample_frames_to_ns(960, 48_000), 20_000_000);
        assert_eq!(
            duration_sample_frames(Duration::from_millis(50), 48_000),
            2_400
        );
        let after_gap =
            960u64.saturating_add(duration_sample_frames(Duration::from_millis(50), 48_000));
        assert_eq!(sample_frames_to_ns(after_gap, 48_000), 70_000_000);
    }

    #[test]
    fn pcm_channel_layout_names_are_stable() {
        assert_eq!(pcm_channel_layout(0), "mono");
        assert_eq!(pcm_channel_layout(1), "mono");
        assert_eq!(pcm_channel_layout(2), "stereo");
        assert_eq!(pcm_channel_layout(6), "5.1");
        assert_eq!(pcm_channel_layout(8), "8ch");
    }

    #[test]
    fn realtime_tick_preserves_short_scheduler_stalls_for_burst_recovery() {
        let mut remainder = Duration::ZERO;

        let (chunks, dropped) = realtime_chunks_due(&mut remainder, Duration::from_millis(70));

        assert_eq!(chunks, 1);
        assert_eq!(dropped, Duration::ZERO);
        assert_eq!(remainder, Duration::ZERO);
    }

    #[test]
    fn realtime_tick_preserves_short_scheduler_jitter_for_burst_recovery() {
        let mut remainder = Duration::ZERO;

        let (first_chunks, first_dropped) =
            realtime_chunks_due(&mut remainder, Duration::from_millis(30));
        let (second_chunks, second_dropped) =
            realtime_chunks_due(&mut remainder, Duration::from_millis(30));

        assert_eq!(first_chunks, 1);
        assert_eq!(first_dropped, Duration::ZERO);
        assert_eq!(second_chunks, 1);
        assert_eq!(second_dropped, Duration::ZERO);
        assert_eq!(remainder, Duration::ZERO);
    }

    #[test]
    fn realtime_tick_drops_backlog_without_catchup() {
        let mut remainder = Duration::ZERO;

        let (chunks, dropped) = realtime_chunks_due(&mut remainder, Duration::from_millis(180));

        assert_eq!(chunks, 1);
        assert!(dropped >= realtime_lag_warn_backlog());
        assert_eq!(remainder, Duration::ZERO);
    }

    #[test]
    fn realtime_tick_bounds_huge_scheduler_stalls() {
        let mut remainder = Duration::ZERO;

        let (chunks, dropped) = realtime_chunks_due(&mut remainder, Duration::from_millis(250));

        assert_eq!(chunks, 1);
        assert!(dropped >= realtime_lag_warn_backlog());
        assert_eq!(remainder, Duration::ZERO);
    }

    #[test]
    fn realtime_lag_warning_ignores_ordinary_scheduler_jitter() {
        assert!(realtime_lag_warn_backlog() > Duration::from_millis(20));
    }

    #[test]
    fn realtime_ticker_replays_short_missed_ticks() {
        assert_eq!(realtime_tick_missed_behavior(), MissedTickBehavior::Burst);
    }
}
