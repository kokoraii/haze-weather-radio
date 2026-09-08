use std::collections::{BTreeMap, HashMap, VecDeque};
use std::env;
#[cfg(feature = "gstreamer-backend")]
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
#[cfg(feature = "gstreamer-backend")]
use std::net::{Ipv4Addr, UdpSocket as StdUdpSocket};
use std::path::{Path, PathBuf};
#[cfg(feature = "gstreamer-backend")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use clap::Parser;
use haze_media::{normalize_pcm, pcm16_samples, push_i16, AudioFormat, Pcm};
use haze_media_protocol::{
    decode_pcm_frame, pcm_frame_len_from_prefix, PCM_FRAME_MAGIC, PCM_FRAME_PREFIX_LEN,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
#[cfg(feature = "gstreamer-backend")]
use tokio::net::UdpSocket;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{interval, sleep, MissedTickBehavior};
use tracing::{debug, info, warn};

#[cfg(feature = "gstreamer-backend")]
use std::io::ErrorKind;

#[cfg(windows)]
use windows_sys::Win32::Media::{timeBeginPeriod, timeEndPeriod};

const SOURCE_ID: &str = "haze-media";
const DEFAULT_CONFIG: &str = "config.yaml";
const DEFAULT_OUTPUTS_FILE: &str = "managed/configs/output.xml";
const DEFAULT_LISTEN: &str = "127.0.0.1:8097";
const SAMPLE_RATE: u32 = 48_000;
const CHANNELS: u16 = 1;
const OPUS_BITRATE_BPS: u32 = 32_000;
const OPUS_BITRATE_KBPS: u32 = OPUS_BITRATE_BPS / 1_000;
const FRAME_DURATION: Duration = Duration::from_millis(20);
const FRAME_SAMPLES: usize = SAMPLE_RATE as usize / 50;
const FRAME_BYTES: usize = FRAME_SAMPLES * CHANNELS as usize * 2;
const INPUT_QUEUE_CAPACITY: usize = 64;
const PACED_FRAME_CAPACITY: usize = 64;
// Live playout favours freshness. Queues beyond this target are stale audio,
// not useful jitter protection, and must be discarded rather than replayed.
const TARGET_SOURCE_QUEUE_MS: u64 = 120;
const SOFT_SOURCE_QUEUE_MS: u64 = 400;
const MAX_SOURCE_QUEUE_MS: u64 = 3_000;
const SOURCE_DRIFT_TRIM_MS: u64 = 0;
const FEED_CLOCK_REBASE_LAG_MS: u64 = 80;
const CONCEALMENT_FRAMES: u8 = 3;
const STATUS_INTERVAL: Duration = Duration::from_secs(5);
const AUDIO_HEALTH_WINDOW: Duration = Duration::from_secs(60);
const HTTP_HEADER_LIMIT: usize = 16 * 1024;
const HTTP_BODY_LIMIT: usize = 512 * 1024;
const MEDIA_BRIDGE_LINE_LIMIT: usize = 256 * 1024;
#[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
const STR0M_WEBRTC_UDP_BUFFER: usize = 2_048;
#[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
const STR0M_WEBRTC_ENCODED_QUEUE_CAPACITY: usize = 16;
const MAX_WEBRTC_UDP_PORTS: u32 = 4_096;
#[cfg(feature = "gstreamer-backend")]
static WEBRTC_UDP_PORT_CURSOR: AtomicU64 = AtomicU64::new(0);
#[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
const ENCODED_HTTP_QUEUE_CAPACITY: usize = 8;
#[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
const ENCODED_ICECAST_QUEUE_CAPACITY: usize = 64;
#[cfg(feature = "gstreamer-backend")]
const HTTP_AUDIO_GSTREAMER_QUEUE_MS: u64 = 120;
#[cfg(feature = "gstreamer-backend")]
const HTTP_AUDIO_GSTREAMER_QUEUE_BUFFERS: u64 = HTTP_AUDIO_GSTREAMER_QUEUE_MS / 20;
#[cfg(feature = "gstreamer-backend")]
const ICECAST_RETRY_BASE: Duration = Duration::from_secs(5);
#[cfg(feature = "gstreamer-backend")]
const ICECAST_RESPONSE_LIMIT: usize = 16 * 1024;
#[cfg(feature = "gstreamer-backend")]
const ICECAST_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RAW_HTTP_LAG_RESYNC_SILENCE_FRAMES: u64 = 1;
const WEBRTC_CONNECT_TIMEOUT: Duration = Duration::from_secs(45);
const WEBRTC_CONNECTED_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const NEAR_SILENT_PEAK: u16 = 20;
const CLIP_SAMPLE_PEAK: u16 = 32_760;
const AUDIO_HEALTH_INPUT_STALE_MS: u128 = 1_000;
const AUDIO_HEALTH_TICK_GAP_WARN_MS: u128 = 30;
const DEFAULT_LOUDNESS_ENABLED: bool = false;
const DEFAULT_LOUDNESS_GAIN_DB: f32 = 5.0;
const DEFAULT_COMPRESSOR_THRESHOLD_DBFS: f32 = -18.0;
const DEFAULT_COMPRESSOR_RATIO: f32 = 3.0;
const DEFAULT_COMPRESSOR_MAKEUP_DB: f32 = 1.5;
const DEFAULT_COMPRESSOR_ATTACK_MS: f32 = 5.0;
const DEFAULT_COMPRESSOR_RELEASE_MS: f32 = 120.0;
const DEFAULT_LIMITER_CEILING: f32 = 0.98;

#[derive(Debug, Parser)]
#[command(about = "Haze GStreamer-backed live media service")]
struct Args {
    #[arg(long, default_value = DEFAULT_CONFIG)]
    config: PathBuf,

    #[arg(long, env = "HAZE_HOST_BRIDGE_ADDR")]
    bridge: String,

    #[arg(long, env = "HAZE_MEDIA_BRIDGE_ADDR")]
    media_bridge: Option<String>,

    #[arg(long)]
    listen: Option<String>,

    #[arg(long)]
    outputs: Option<PathBuf>,

    #[arg(long)]
    backend: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum BackendMode {
    Auto,
    GStreamer,
    Legacy,
}

impl BackendMode {
    fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "gstreamer" | "gst" => Self::GStreamer,
            "legacy" | "builtin" => Self::Legacy,
            _ => Self::Auto,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::GStreamer => "gstreamer",
            Self::Legacy => "legacy",
        }
    }
}

#[derive(Debug, Deserialize)]
struct RootConfig {
    #[serde(default = "default_outputs_file")]
    outputs_file: String,
    #[serde(default)]
    services: ServicesConfig,
}

#[derive(Debug, Default, Deserialize)]
struct ServicesConfig {
    #[serde(default)]
    rust: RustServicesConfig,
}

#[derive(Debug, Default, Deserialize)]
struct RustServicesConfig {
    #[serde(default)]
    media: MediaServiceConfig,
}

#[derive(Debug, Default, Deserialize)]
struct MediaServiceConfig {
    #[serde(default, rename = "enabled")]
    _enabled: bool,
    #[serde(default)]
    addr: String,
    #[serde(default)]
    listen: String,
    #[serde(default)]
    backend: String,
    #[serde(default)]
    webrtc: MediaWebRTCConfig,
    #[serde(default)]
    audio_processing: MediaAudioProcessingConfig,
}

#[derive(Debug, Default, Deserialize)]
struct MediaWebRTCConfig {
    #[serde(default)]
    public_ip: String,
    #[serde(default)]
    udp_port_min: Option<u16>,
    #[serde(default)]
    udp_port_max: Option<u16>,
}

#[derive(Debug, Default, Deserialize)]
struct MediaAudioProcessingConfig {
    #[serde(default)]
    loudness_enabled: Option<bool>,
    #[serde(default)]
    gain_db: Option<f32>,
    #[serde(default)]
    compressor_threshold_dbfs: Option<f32>,
    #[serde(default)]
    compressor_ratio: Option<f32>,
    #[serde(default)]
    compressor_makeup_db: Option<f32>,
    #[serde(default)]
    limiter_ceiling: Option<f32>,
    #[serde(default)]
    compressor_attack_ms: Option<f32>,
    #[serde(default)]
    compressor_release_ms: Option<f32>,
}

#[derive(Debug, Default, Deserialize)]
struct OutputsXml {
    #[serde(rename = "feed", default)]
    feeds: Vec<OutputFeedXml>,
}

#[derive(Debug, Default, Deserialize)]
struct OutputFeedXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(default)]
    webrtc: OutputNodeXml,
    #[serde(default)]
    icecast: OutputNodeXml,
    #[serde(default)]
    stream: OutputNodeXml,
    #[serde(default)]
    udp: OutputNodeXml,
    #[serde(default)]
    rtp: OutputNodeXml,
    #[serde(default)]
    rtmp: OutputNodeXml,
    #[serde(default)]
    srt: OutputNodeXml,
    #[serde(default)]
    rtsp: OutputNodeXml,
    #[serde(default)]
    audio_device: OutputNodeXml,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct OutputNodeXml {
    #[serde(rename = "@enabled", default)]
    enabled: Option<String>,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: String,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    mount: String,
    #[serde(default)]
    ssl: String,
    #[serde(default)]
    format: String,
    #[serde(default)]
    acodec: String,
    #[serde(default)]
    bitrate_kbps: String,
}

impl OutputNodeXml {
    fn is_enabled(&self) -> bool {
        xml_bool(self.enabled.as_deref(), false)
    }

    fn is_configured(&self) -> bool {
        self.enabled.is_some()
            || !self.r#type.trim().is_empty()
            || !self.host.trim().is_empty()
            || !self.port.trim().is_empty()
            || !self.username.trim().is_empty()
            || !self.password.trim().is_empty()
            || !self.mount.trim().is_empty()
            || !self.ssl.trim().is_empty()
            || !self.format.trim().is_empty()
            || !self.acodec.trim().is_empty()
            || !self.bitrate_kbps.trim().is_empty()
    }

    fn portable_bitrate_kbps(&self) -> u32 {
        self.bitrate_kbps
            .trim()
            .parse::<u32>()
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or(OPUS_BITRATE_KBPS)
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct OutputFeed {
    id: String,
    webrtc: bool,
    http: bool,
    external: bool,
    icecast: bool,
    icecast_mount: Option<String>,
    #[serde(skip)]
    icecast_config: Option<IcecastOutput>,
    webrtc_ready: bool,
}

#[derive(Debug, Clone, Default)]
struct IcecastOutput {
    host: String,
    port: u16,
    username: String,
    password: String,
    mount: String,
    ssl: bool,
    format: String,
    acodec: String,
    bitrate_kbps: u32,
}

#[derive(Debug, Clone)]
struct PcmChunk {
    feed_id: String,
    sample_rate: u32,
    channels: u16,
    data: Vec<u8>,
    bypass_loudness: bool,
    sequence: Option<u64>,
    pts_ns: Option<u64>,
    discontinuity: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct QueuedSample {
    value: i16,
    bypass_loudness: bool,
}

#[derive(Debug, Clone)]
struct PacedFrame {
    _sequence: u64,
    source_pts_ns: Option<u64>,
    source_discontinuity: bool,
    data: Arc<[u8]>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum WebRTCAudioCodec {
    Opus,
    G722,
    Pcmu,
    Pcma,
}

impl WebRTCAudioCodec {
    fn id(self) -> &'static str {
        match self {
            Self::Opus => "opus",
            Self::G722 => "g722",
            Self::Pcmu => "pcmu",
            Self::Pcma => "pcma",
        }
    }

    fn rtpmap_match(self) -> &'static str {
        match self {
            Self::Opus => "OPUS/48000",
            Self::G722 => "G722/8000",
            Self::Pcmu => "PCMU/8000",
            Self::Pcma => "PCMA/8000",
        }
    }

    fn is_static_payload(self, payload_type: u8) -> bool {
        matches!(
            (self, payload_type),
            (Self::G722, 9) | (Self::Pcmu, 0) | (Self::Pcma, 8)
        )
    }
}

#[derive(Debug, Clone)]
struct AudioLoudness {
    enabled: bool,
    gain: f32,
    threshold_db: f32,
    ratio: f32,
    makeup: f32,
    ceiling: f32,
    attack_coeff: f32,
    release_coeff: f32,
    envelope: f32,
}

impl AudioLoudness {
    fn from_env() -> Self {
        let enabled = env_bool("HAZE_MEDIA_LOUDNESS_ENABLED", DEFAULT_LOUDNESS_ENABLED);
        Self {
            enabled,
            gain: db_to_gain(env_f32("HAZE_MEDIA_GAIN_DB", DEFAULT_LOUDNESS_GAIN_DB)),
            threshold_db: env_f32(
                "HAZE_MEDIA_COMPRESSOR_THRESHOLD_DBFS",
                DEFAULT_COMPRESSOR_THRESHOLD_DBFS,
            ),
            ratio: env_f32("HAZE_MEDIA_COMPRESSOR_RATIO", DEFAULT_COMPRESSOR_RATIO).max(1.0),
            makeup: db_to_gain(env_f32(
                "HAZE_MEDIA_COMPRESSOR_MAKEUP_DB",
                DEFAULT_COMPRESSOR_MAKEUP_DB,
            )),
            ceiling: env_f32("HAZE_MEDIA_LIMITER_CEILING", DEFAULT_LIMITER_CEILING).clamp(0.1, 1.0),
            attack_coeff: compressor_coeff_ms(env_f32(
                "HAZE_MEDIA_COMPRESSOR_ATTACK_MS",
                DEFAULT_COMPRESSOR_ATTACK_MS,
            )),
            release_coeff: compressor_coeff_ms(env_f32(
                "HAZE_MEDIA_COMPRESSOR_RELEASE_MS",
                DEFAULT_COMPRESSOR_RELEASE_MS,
            )),
            envelope: 0.0,
        }
    }

    fn process(&mut self, frame: &mut [u8]) {
        if !self.enabled || frame.is_empty() {
            return;
        }
        for sample in frame.chunks_exact_mut(2) {
            let raw = i16::from_le_bytes([sample[0], sample[1]]);
            let processed = self.process_sample(raw);
            sample.copy_from_slice(&processed.to_le_bytes());
        }
    }

    fn process_sample(&mut self, sample: i16) -> i16 {
        let mut value = f32::from(sample) / 32768.0;
        value *= self.gain;
        let level = value.abs();
        let coeff = if level > self.envelope {
            self.attack_coeff
        } else {
            self.release_coeff
        };
        self.envelope = (coeff * self.envelope) + ((1.0 - coeff) * level);
        if self.ratio > 1.0 && self.envelope > 0.0 {
            let envelope_db = gain_to_db(self.envelope.max(1.0e-9));
            if envelope_db > self.threshold_db {
                let compressed_db =
                    self.threshold_db + ((envelope_db - self.threshold_db) / self.ratio);
                value *= db_to_gain(compressed_db - envelope_db);
            }
        }
        value *= self.makeup;
        value = value.clamp(-self.ceiling, self.ceiling);
        (value * f32::from(i16::MAX)).round() as i16
    }
}

#[derive(Debug, Clone, Default, Serialize)]
struct FeedStats {
    feed_id: String,
    audio_ok: bool,
    audio_warnings: Vec<String>,
    frames: u64,
    real_frames: u64,
    silence_frames: u64,
    concealed_frames: u64,
    recent_concealed_frames: u64,
    partial_frames: u64,
    recent_partial_frames: u64,
    late_ticks: u64,
    recent_late_ticks: u64,
    catchup_ticks: u64,
    recent_catchup_ticks: u64,
    dropped_input_chunks: u64,
    recent_dropped_input_chunks: u64,
    source_discontinuities: u64,
    stale_samples_dropped: u64,
    recent_stale_samples_dropped: u64,
    source_drift_samples_trimmed: u64,
    near_silent_frames: u64,
    consecutive_near_silent_frames: u64,
    repeated_frames: u64,
    repeated_non_silent_frames: u64,
    recent_repeated_non_silent_frames: u64,
    consecutive_repeated_frames: u64,
    clipped_samples: u64,
    recent_clipped_samples: u64,
    last_peak: u16,
    last_rms_dbfs: f64,
    last_max_sample_jump: u16,
    subscribers: usize,
    queued_samples: usize,
    last_tick_gap_ms: Option<u128>,
    max_tick_gap_ms: u128,
    last_input_age_ms: Option<u128>,
    last_frame_age_ms: Option<u128>,
    #[serde(skip)]
    health_window_started_at: Option<Instant>,
}

struct FeedRuntime {
    feed_id: String,
    input_tx: mpsc::Sender<PcmChunk>,
    frame_tx: broadcast::Sender<PacedFrame>,
    stats: Arc<Mutex<FeedStats>>,
}

impl FeedRuntime {
    fn new(feed_id: String, output: Option<OutputFeed>, gstreamer_available: bool) -> Arc<Self> {
        let (input_tx, input_rx) = mpsc::channel(INPUT_QUEUE_CAPACITY);
        let (frame_tx, _) = broadcast::channel(PACED_FRAME_CAPACITY);
        let stats = Arc::new(Mutex::new(FeedStats {
            feed_id: feed_id.clone(),
            ..FeedStats::default()
        }));
        let runtime = Arc::new(Self {
            feed_id,
            input_tx,
            frame_tx,
            stats,
        });
        start_feed_clock_thread(Arc::clone(&runtime), input_rx);
        if let Some(mut icecast) = output.and_then(|output| output.icecast_config) {
            if icecast.mount.trim().is_empty() {
                icecast.mount = format!("/{}", runtime.feed_id.trim().trim_start_matches('/'));
            }
            start_icecast_output(Arc::clone(&runtime), icecast, gstreamer_available);
        }
        runtime
    }

    fn push(&self, chunk: PcmChunk) {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.input_tx.try_send(chunk) {
            if let Ok(mut stats) = self.stats.lock() {
                reset_audio_health_window(&mut stats, Instant::now());
                stats.dropped_input_chunks = stats.dropped_input_chunks.saturating_add(1);
                stats.recent_dropped_input_chunks =
                    stats.recent_dropped_input_chunks.saturating_add(1);
            }
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<PacedFrame> {
        self.frame_tx.subscribe()
    }

    fn snapshot(&self) -> FeedStats {
        let mut snapshot = self
            .stats
            .lock()
            .map(|stats| stats.clone())
            .unwrap_or_else(|_| FeedStats {
                feed_id: self.feed_id.clone(),
                ..FeedStats::default()
            });
        snapshot.subscribers = self.frame_tx.receiver_count();
        let (audio_ok, audio_warnings) = classify_audio_health(&snapshot);
        snapshot.audio_ok = audio_ok;
        snapshot.audio_warnings = audio_warnings;
        snapshot
    }
}

#[derive(Clone)]
struct MediaState {
    feeds: Arc<Mutex<HashMap<String, Arc<FeedRuntime>>>>,
    configured_outputs: Arc<BTreeMap<String, OutputFeed>>,
    http_clients: Arc<Mutex<HashMap<u64, HttpClientRuntime>>>,
    webrtc_peers: Arc<Mutex<HashMap<u64, WebRTCPeerRuntime>>>,
    next_http_client_id: Arc<AtomicU64>,
    next_webrtc_peer_id: Arc<AtomicU64>,
    backend: BackendMode,
    gstreamer_available: bool,
    webrtc_available: bool,
    webrtc_codecs: Arc<Vec<WebRTCAudioCodec>>,
}

#[derive(Debug, Clone)]
struct HttpClientRuntime {
    id: u64,
    feed_id: String,
    codec: &'static str,
    connected_at: Instant,
    last_write_at: Option<Instant>,
    bytes_written: u64,
    chunks_written: u64,
    encoded_chunks_dropped: u64,
    lagged_frames: u64,
}

#[derive(Debug, Serialize)]
struct HttpClientSnapshot {
    id: u64,
    feed_id: String,
    codec: &'static str,
    connected_ms: u128,
    last_write_age_ms: Option<u128>,
    bytes_written: u64,
    chunks_written: u64,
    encoded_chunks_dropped: u64,
    lagged_frames: u64,
}

#[derive(Debug, Clone)]
struct WebRTCPeerRuntime {
    id: u64,
    feed_id: String,
    client_id: String,
    codec: &'static str,
    connected_at: Instant,
    last_push_at: Option<Instant>,
    bytes_pushed: u64,
    frames_pushed: u64,
    dropped_frames: u64,
}

#[derive(Debug, Serialize)]
struct WebRTCPeerSnapshot {
    id: u64,
    feed_id: String,
    client_id: String,
    codec: &'static str,
    connected_ms: u128,
    last_push_age_ms: Option<u128>,
    bytes_pushed: u64,
    frames_pushed: u64,
    dropped_frames: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct WebRTCCodecSelection {
    codec: WebRTCAudioCodec,
    payload_type: u8,
}

struct HttpClientGuard {
    id: u64,
    clients: Arc<Mutex<HashMap<u64, HttpClientRuntime>>>,
}

impl HttpClientGuard {
    fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for HttpClientGuard {
    fn drop(&mut self) {
        if let Ok(mut clients) = self.clients.lock() {
            clients.remove(&self.id);
        }
    }
}

impl MediaState {
    fn new(
        mut outputs: BTreeMap<String, OutputFeed>,
        backend: BackendMode,
        gstreamer_available: bool,
        webrtc_codecs: Vec<WebRTCAudioCodec>,
    ) -> Self {
        let webrtc_available = gstreamer_available && !webrtc_codecs.is_empty();
        let webrtc_ready = webrtc_available;
        for output in outputs.values_mut() {
            output.webrtc_ready = output.webrtc && webrtc_ready;
        }
        Self {
            feeds: Arc::new(Mutex::new(HashMap::new())),
            configured_outputs: Arc::new(outputs),
            http_clients: Arc::new(Mutex::new(HashMap::new())),
            webrtc_peers: Arc::new(Mutex::new(HashMap::new())),
            next_http_client_id: Arc::new(AtomicU64::new(1)),
            next_webrtc_peer_id: Arc::new(AtomicU64::new(1)),
            backend,
            gstreamer_available,
            webrtc_available,
            webrtc_codecs: Arc::new(webrtc_codecs),
        }
    }

    fn feed(&self, feed_id: &str) -> Option<Arc<FeedRuntime>> {
        let feed_id = feed_id.trim().to_string();
        let output = self.configured_output_for_feed(&feed_id);
        if output.is_none() {
            return None;
        }
        let mut feeds = self.feeds.lock().expect("feed registry poisoned");
        if let Some(feed) = feeds.get(&feed_id) {
            return Some(Arc::clone(feed));
        }
        let runtime = FeedRuntime::new(feed_id.clone(), output, self.gstreamer_available);
        feeds.insert(feed_id, Arc::clone(&runtime));
        Some(runtime)
    }

    fn configured_output_for_feed(&self, feed_id: &str) -> Option<OutputFeed> {
        self.configured_outputs.get(feed_id).cloned().or_else(|| {
            self.configured_outputs
                .iter()
                .find(|(pattern, _)| {
                    output_id_is_wildcard(pattern) && wildcard_match(pattern, feed_id)
                })
                .map(|(_, output)| {
                    let mut output = output.clone();
                    output.id = feed_id.to_string();
                    output
                })
        })
    }

    fn publish_pcm(&self, chunk: PcmChunk) {
        if chunk.feed_id.trim().is_empty() || chunk.data.is_empty() {
            return;
        }
        if let Some(feed) = self.feed(&chunk.feed_id) {
            feed.push(chunk);
        }
    }

    fn snapshots(&self) -> Vec<FeedStats> {
        let feeds = self.feeds.lock().expect("feed registry poisoned");
        let mut out = feeds
            .values()
            .map(|feed| feed.snapshot())
            .collect::<Vec<_>>();
        out.sort_by(|a, b| a.feed_id.cmp(&b.feed_id));
        out
    }

    fn register_http_client(&self, feed_id: &str, codec: &'static str) -> HttpClientGuard {
        let id = self.next_http_client_id.fetch_add(1, Ordering::Relaxed);
        let client = HttpClientRuntime {
            id,
            feed_id: feed_id.to_string(),
            codec,
            connected_at: Instant::now(),
            last_write_at: None,
            bytes_written: 0,
            chunks_written: 0,
            encoded_chunks_dropped: 0,
            lagged_frames: 0,
        };
        if let Ok(mut clients) = self.http_clients.lock() {
            clients.insert(id, client);
        }
        HttpClientGuard {
            id,
            clients: Arc::clone(&self.http_clients),
        }
    }

    fn record_http_client_write(&self, client_id: u64, bytes: usize) {
        if let Ok(mut clients) = self.http_clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.last_write_at = Some(Instant::now());
                client.bytes_written = client.bytes_written.saturating_add(bytes as u64);
                client.chunks_written = client.chunks_written.saturating_add(1);
            }
        }
    }

    #[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
    fn record_http_client_encoded_drop(&self, client_id: u64) {
        if let Ok(mut clients) = self.http_clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.encoded_chunks_dropped = client.encoded_chunks_dropped.saturating_add(1);
            }
        }
    }

    fn record_http_client_lag(&self, client_id: u64, frames: u64) {
        if let Ok(mut clients) = self.http_clients.lock() {
            if let Some(client) = clients.get_mut(&client_id) {
                client.lagged_frames = client.lagged_frames.saturating_add(frames);
            }
        }
    }

    fn http_client_snapshots(&self) -> Vec<HttpClientSnapshot> {
        let now = Instant::now();
        let clients = self
            .http_clients
            .lock()
            .expect("HTTP client registry poisoned");
        let mut out = clients
            .values()
            .map(|client| HttpClientSnapshot {
                id: client.id,
                feed_id: client.feed_id.clone(),
                codec: client.codec,
                connected_ms: now.duration_since(client.connected_at).as_millis(),
                last_write_age_ms: client
                    .last_write_at
                    .map(|instant| now.duration_since(instant).as_millis()),
                bytes_written: client.bytes_written,
                chunks_written: client.chunks_written,
                encoded_chunks_dropped: client.encoded_chunks_dropped,
                lagged_frames: client.lagged_frames,
            })
            .collect::<Vec<_>>();
        out.sort_by_key(|client| client.id);
        out
    }

    fn register_webrtc_peer(
        &self,
        feed_id: &str,
        codec: &'static str,
        client_id: &str,
    ) -> Option<u64> {
        let feed_id = feed_id.to_string();
        let client_id = normalize_webrtc_listener_client_id(client_id);
        let Ok(mut peers) = self.webrtc_peers.lock() else {
            return None;
        };
        if peers
            .values()
            .any(|peer| peer.feed_id == feed_id && peer.client_id == client_id)
        {
            return None;
        }
        let id = self.next_webrtc_peer_id.fetch_add(1, Ordering::Relaxed);
        let peer = WebRTCPeerRuntime {
            id,
            feed_id,
            client_id,
            codec,
            connected_at: Instant::now(),
            last_push_at: None,
            bytes_pushed: 0,
            frames_pushed: 0,
            dropped_frames: 0,
        };
        peers.insert(id, peer);
        Some(id)
    }

    #[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
    fn unregister_webrtc_peer(&self, peer_id: u64) {
        if let Ok(mut peers) = self.webrtc_peers.lock() {
            peers.remove(&peer_id);
        }
    }

    #[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
    fn record_webrtc_peer_push(&self, peer_id: u64, bytes: usize) {
        if let Ok(mut peers) = self.webrtc_peers.lock() {
            if let Some(peer) = peers.get_mut(&peer_id) {
                peer.last_push_at = Some(Instant::now());
                peer.bytes_pushed = peer.bytes_pushed.saturating_add(bytes as u64);
                peer.frames_pushed = peer.frames_pushed.saturating_add(1);
            }
        }
    }

    #[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
    fn record_webrtc_peer_drop(&self, peer_id: u64) {
        if let Ok(mut peers) = self.webrtc_peers.lock() {
            if let Some(peer) = peers.get_mut(&peer_id) {
                peer.dropped_frames = peer.dropped_frames.saturating_add(1);
            }
        }
    }

    fn webrtc_peer_snapshots(&self) -> Vec<WebRTCPeerSnapshot> {
        let now = Instant::now();
        let peers = self
            .webrtc_peers
            .lock()
            .expect("WebRTC peer registry poisoned");
        let mut out = peers
            .values()
            .map(|peer| WebRTCPeerSnapshot {
                id: peer.id,
                feed_id: peer.feed_id.clone(),
                client_id: peer.client_id.clone(),
                codec: peer.codec,
                connected_ms: now.duration_since(peer.connected_at).as_millis(),
                last_push_age_ms: peer
                    .last_push_at
                    .map(|instant| now.duration_since(instant).as_millis()),
                bytes_pushed: peer.bytes_pushed,
                frames_pushed: peer.frames_pushed,
                dropped_frames: peer.dropped_frames,
            })
            .collect::<Vec<_>>();
        out.sort_by_key(|peer| peer.id);
        out
    }

    fn health(&self) -> Value {
        let http_clients = self.http_client_snapshots();
        let webrtc_peers = self.webrtc_peer_snapshots();
        let webrtc_ready = self.webrtc_available;
        let webrtc_codecs = self
            .webrtc_codecs
            .iter()
            .map(|codec| codec.id())
            .collect::<Vec<_>>();
        json!({
            "ok": true,
            "service": "haze-media",
            "backend": self.backend.as_str(),
            "gstreamer_available": self.gstreamer_available,
            "capabilities": {
                "http_audio": true,
                "encoded_http_audio": self.gstreamer_available,
                "webrtc": webrtc_ready,
                "webrtc_reason": if webrtc_ready { Value::Null } else if !self.gstreamer_available {
                    json!("gstreamer encoder backend is unavailable")
                } else {
                    json!("str0m WebRTC encoder elements are unavailable")
                },
                "webrtc_backend": "str0m",
                "webrtc_codecs": webrtc_codecs,
            },
            "audio_processing": audio_processing_health(),
            "clock": media_clock_health(),
            "configured_outputs": self.configured_outputs.values().collect::<Vec<_>>(),
            "http_client_count": http_clients.len(),
            "http_clients": http_clients,
            "webrtc_peer_count": webrtc_peers.len(),
            "webrtc_peers": webrtc_peers,
            "feeds": self.snapshots(),
        })
    }
}

fn audio_processing_health() -> Value {
    let enabled = env_bool("HAZE_MEDIA_LOUDNESS_ENABLED", DEFAULT_LOUDNESS_ENABLED);
    let gain_db = env_f32("HAZE_MEDIA_GAIN_DB", DEFAULT_LOUDNESS_GAIN_DB);
    let threshold_dbfs = env_f32(
        "HAZE_MEDIA_COMPRESSOR_THRESHOLD_DBFS",
        DEFAULT_COMPRESSOR_THRESHOLD_DBFS,
    );
    let ratio = env_f32("HAZE_MEDIA_COMPRESSOR_RATIO", DEFAULT_COMPRESSOR_RATIO).max(1.0);
    let makeup_db = env_f32(
        "HAZE_MEDIA_COMPRESSOR_MAKEUP_DB",
        DEFAULT_COMPRESSOR_MAKEUP_DB,
    );
    let limiter_ceiling =
        env_f32("HAZE_MEDIA_LIMITER_CEILING", DEFAULT_LIMITER_CEILING).clamp(0.1, 1.0);
    json!({
        "loudness_enabled": enabled,
        "gain_db": gain_db,
        "compressor_threshold_dbfs": threshold_dbfs,
        "compressor_ratio": ratio,
        "compressor_makeup_db": makeup_db,
        "limiter_ceiling": limiter_ceiling,
        "source": env::var("HAZE_MEDIA_AUDIO_PROCESSING_SOURCE")
            .unwrap_or_else(|_| "environment".to_string()),
    })
}

fn apply_media_audio_processing_config(config: &MediaAudioProcessingConfig) {
    let has_config = config.loudness_enabled.is_some()
        || config.gain_db.is_some()
        || config.compressor_threshold_dbfs.is_some()
        || config.compressor_ratio.is_some()
        || config.compressor_makeup_db.is_some()
        || config.limiter_ceiling.is_some()
        || config.compressor_attack_ms.is_some()
        || config.compressor_release_ms.is_some();
    if has_config && env::var_os("HAZE_MEDIA_AUDIO_PROCESSING_SOURCE").is_none() {
        env::set_var("HAZE_MEDIA_AUDIO_PROCESSING_SOURCE", "config.yaml");
    }
    set_env_bool_if_absent("HAZE_MEDIA_LOUDNESS_ENABLED", config.loudness_enabled);
    set_env_f32_if_absent("HAZE_MEDIA_GAIN_DB", config.gain_db);
    set_env_f32_if_absent(
        "HAZE_MEDIA_COMPRESSOR_THRESHOLD_DBFS",
        config.compressor_threshold_dbfs,
    );
    set_env_f32_if_absent("HAZE_MEDIA_COMPRESSOR_RATIO", config.compressor_ratio);
    set_env_f32_if_absent(
        "HAZE_MEDIA_COMPRESSOR_MAKEUP_DB",
        config.compressor_makeup_db,
    );
    set_env_f32_if_absent("HAZE_MEDIA_LIMITER_CEILING", config.limiter_ceiling);
    set_env_f32_if_absent(
        "HAZE_MEDIA_COMPRESSOR_ATTACK_MS",
        config.compressor_attack_ms,
    );
    set_env_f32_if_absent(
        "HAZE_MEDIA_COMPRESSOR_RELEASE_MS",
        config.compressor_release_ms,
    );
}

fn apply_media_webrtc_config(config: &MediaWebRTCConfig) {
    set_env_string_if_absent("HAZE_MEDIA_WEBRTC_HOST", &config.public_ip);
    set_env_u16_if_absent(
        "HAZE_MEDIA_WEBRTC_UDP_PORT_MIN",
        config.udp_port_min.filter(|port| *port > 0),
    );
    set_env_u16_if_absent(
        "HAZE_MEDIA_WEBRTC_UDP_PORT_MAX",
        config.udp_port_max.filter(|port| *port > 0),
    );
}

fn set_env_string_if_absent(name: &str, value: &str) {
    if env::var_os(name).is_some() {
        return;
    }
    let value = value.trim();
    if !value.is_empty() {
        env::set_var(name, value);
    }
}

fn set_env_u16_if_absent(name: &str, value: Option<u16>) {
    if env::var_os(name).is_some() {
        return;
    }
    if let Some(value) = value {
        env::set_var(name, value.to_string());
    }
}

fn set_env_bool_if_absent(name: &str, value: Option<bool>) {
    if env::var_os(name).is_some() {
        return;
    }
    if let Some(value) = value {
        env::set_var(name, if value { "true" } else { "false" });
    }
}

fn set_env_f32_if_absent(name: &str, value: Option<f32>) {
    if env::var_os(name).is_some() {
        return;
    }
    if let Some(value) = value {
        env::set_var(name, value.to_string());
    }
}

fn media_clock_health() -> Value {
    json!({
        "sample_rate": SAMPLE_RATE,
        "channels": CHANNELS,
        "frame_ms": FRAME_DURATION.as_millis(),
        "frame_samples": FRAME_SAMPLES,
        "target_source_queue_ms": TARGET_SOURCE_QUEUE_MS,
        "soft_source_queue_ms": SOFT_SOURCE_QUEUE_MS,
        "max_source_queue_ms": MAX_SOURCE_QUEUE_MS,
        "source_drift_trim_ms": SOURCE_DRIFT_TRIM_MS,
        "input_queue_capacity": INPUT_QUEUE_CAPACITY,
        "paced_frame_capacity": PACED_FRAME_CAPACITY,
    })
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "haze_media=info,info".into()),
        )
        .init();
    let _timer_resolution = TimerResolutionGuard::request();

    let args = Args::parse();
    let root = load_root_config(&args.config)?;
    apply_media_audio_processing_config(&root.services.rust.media.audio_processing);
    apply_media_webrtc_config(&root.services.rust.media.webrtc);
    let webrtc_udp_port_range = configured_webrtc_udp_port_range()?;
    let listen = first_non_blank(&[
        args.listen.as_deref(),
        Some(root.services.rust.media.listen.as_str()),
        Some(root.services.rust.media.addr.as_str()),
        Some(DEFAULT_LISTEN),
    ]);
    let backend = BackendMode::parse(&first_non_blank(&[
        args.backend.as_deref(),
        Some(root.services.rust.media.backend.as_str()),
        Some("auto"),
    ]));
    let base_dir = args
        .config
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let outputs_path = args.outputs.unwrap_or_else(|| {
        resolve_path(
            &base_dir,
            Path::new(first_non_blank(&[Some(root.outputs_file.as_str())]).as_str()),
        )
    });
    let outputs = load_outputs(&outputs_path)?;
    let gstreamer_available = initialize_gstreamer(backend)?;
    let webrtc_codecs = detect_gstreamer_webrtc_codecs(gstreamer_available);
    let state = MediaState::new(outputs, backend, gstreamer_available, webrtc_codecs);

    if let Some((udp_port_min, udp_port_max)) = webrtc_udp_port_range {
        info!(
            udp_port_min,
            udp_port_max, "str0m WebRTC UDP port range configured"
        );
    }

    info!(
        listen,
        bridge = %args.bridge,
        media_bridge = %first_non_blank(&[args.media_bridge.as_deref(), Some(args.bridge.as_str())]),
        backend = state.backend.as_str(),
        gstreamer_available,
        webrtc_available = state.webrtc_available,
        outputs = %outputs_path.display(),
        "starting haze-media"
    );

    let status_state = state.clone();
    let status_bridge_addr = args.bridge.clone();
    tokio::spawn(async move {
        run_status_bridge_loop(status_bridge_addr, status_state).await;
    });

    let pcm_state = state.clone();
    let pcm_bridge_addr =
        first_non_blank(&[args.media_bridge.as_deref(), Some(args.bridge.as_str())]);
    tokio::spawn(async move {
        run_pcm_bridge_loop(pcm_bridge_addr, pcm_state).await;
    });

    let server_state = state.clone();
    let server = tokio::spawn(async move { run_http_server(&listen, server_state).await });

    tokio::select! {
        result = server => {
            result.context("haze-media HTTP task panicked")??;
        }
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed waiting for shutdown signal")?;
            info!("haze-media shutdown requested");
        }
    }
    Ok(())
}

struct TimerResolutionGuard;

impl TimerResolutionGuard {
    fn request() -> Self {
        request_timer_resolution();
        Self
    }
}

impl Drop for TimerResolutionGuard {
    fn drop(&mut self) {
        release_timer_resolution();
    }
}

#[cfg(windows)]
fn request_timer_resolution() {
    // A 20 ms media clock is audibly sensitive to the default Windows timer quantum.
    let result = unsafe { timeBeginPeriod(1) };
    if result == 0 {
        info!("haze-media requested 1 ms Windows timer resolution");
    } else {
        warn!("haze-media failed to request 1 ms Windows timer resolution: {result}");
    }
}

#[cfg(not(windows))]
fn request_timer_resolution() {}

#[cfg(windows)]
fn release_timer_resolution() {
    unsafe {
        timeEndPeriod(1);
    }
}

#[cfg(not(windows))]
fn release_timer_resolution() {}

fn start_feed_clock_thread(runtime: Arc<FeedRuntime>, input_rx: mpsc::Receiver<PcmChunk>) {
    let feed_id = runtime.feed_id.clone();
    if let Err(err) = thread::Builder::new()
        .name(feed_clock_thread_name(&feed_id))
        .spawn(move || feed_clock_thread(runtime, input_rx))
    {
        warn!("failed to start haze-media feed clock thread for {feed_id}: {err}");
    }
}

fn feed_clock_thread_name(feed_id: &str) -> String {
    format!("haze-media-clock-{}", feed_id.trim())
}

fn next_feed_clock_tick(scheduled_tick: Instant, ticked_at: Instant) -> Instant {
    let schedule_lag = ticked_at.saturating_duration_since(scheduled_tick);
    if schedule_lag > Duration::from_millis(FEED_CLOCK_REBASE_LAG_MS) {
        ticked_at
            .checked_add(FRAME_DURATION)
            .unwrap_or_else(Instant::now)
    } else {
        scheduled_tick
            .checked_add(FRAME_DURATION)
            .unwrap_or_else(Instant::now)
    }
}

fn feed_clock_thread(runtime: Arc<FeedRuntime>, mut input_rx: mpsc::Receiver<PcmChunk>) {
    let mut next_tick = Instant::now();
    let mut samples =
        VecDeque::<QueuedSample>::with_capacity(max_source_queue_samples().max(FRAME_SAMPLES * 12));
    let mut frame = Vec::with_capacity(FRAME_BYTES);
    let mut last_real_frame = vec![0i16; FRAME_SAMPLES];
    let mut last_real_bypass_loudness = false;
    let mut previous_output_frame = Vec::with_capacity(FRAME_BYTES);
    let mut concealment_remaining = 0u8;
    let mut primed = false;
    let mut sequence = 0u64;
    let mut last_input_at: Option<Instant> = None;
    let mut last_tick_at: Option<Instant> = None;
    let mut expected_source_sequence: Option<u64> = None;
    let mut next_source_pts_ns: Option<u64> = None;
    let mut source_discontinuity_pending = false;
    let mut loudness = AudioLoudness::from_env();
    loop {
        let scheduled_tick = next_tick;
        let before_sleep = Instant::now();
        if next_tick > before_sleep {
            thread::sleep(next_tick.duration_since(before_sleep));
        }
        let ticked_at = Instant::now();
        next_tick = next_feed_clock_tick(scheduled_tick, ticked_at);
        let tick_gap = last_tick_at.map(|previous| ticked_at.duration_since(previous));
        last_tick_at = Some(ticked_at);
        let late_tick = tick_gap
            .map(|gap| gap > FRAME_DURATION + Duration::from_millis(10))
            .unwrap_or(false);
        let catchup_tick = tick_gap
            .map(|gap| gap < FRAME_DURATION.saturating_sub(Duration::from_millis(10)))
            .unwrap_or(false);
        let mut drained = 0usize;
        while drained < INPUT_QUEUE_CAPACITY {
            match input_rx.try_recv() {
                Ok(chunk) => {
                    let sequence_gap = chunk.sequence.is_some_and(|sequence| {
                        expected_source_sequence.is_some_and(|expected| sequence != expected)
                    });
                    if chunk.discontinuity || sequence_gap {
                        samples.clear();
                        primed = false;
                        concealment_remaining = 0;
                        next_source_pts_ns = chunk.pts_ns;
                        source_discontinuity_pending = true;
                        if let Ok(mut stats) = runtime.stats.lock() {
                            stats.source_discontinuities =
                                stats.source_discontinuities.saturating_add(1);
                        }
                    } else if samples.is_empty() && next_source_pts_ns.is_none() {
                        next_source_pts_ns = chunk.pts_ns;
                    }
                    expected_source_sequence =
                        chunk.sequence.map(|sequence| sequence.saturating_add(1));
                    append_normalized_chunk(&mut samples, chunk);
                    last_input_at = Some(Instant::now());
                    drained += 1;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return,
            }
        }
        let stale_dropped = trim_source_queue(&mut samples);

        frame.clear();
        let mut real = 0usize;
        let mut concealed = false;
        let frame_bypass_loudness;
        let target_samples = target_source_queue_samples();
        if !primed && samples.len() >= target_samples {
            primed = true;
            concealment_remaining = CONCEALMENT_FRAMES;
        }
        if primed && samples.len() >= FRAME_SAMPLES {
            frame_bypass_loudness =
                render_exact_frame(&mut samples, &mut frame, &mut last_real_frame);
            last_real_bypass_loudness = frame_bypass_loudness;
            real = FRAME_SAMPLES;
            concealment_remaining = CONCEALMENT_FRAMES;
        } else if primed && concealment_remaining > 0 {
            render_concealment_frame(&last_real_frame, concealment_remaining, &mut frame);
            frame_bypass_loudness = last_real_bypass_loudness;
            concealment_remaining = concealment_remaining.saturating_sub(1);
            concealed = true;
        } else {
            for _ in 0..FRAME_SAMPLES {
                push_i16(&mut frame, 0);
            }
            primed = false;
            last_real_bypass_loudness = false;
            frame_bypass_loudness = false;
        }
        if !frame_bypass_loudness {
            loudness.process(&mut frame);
        }
        sequence = sequence.saturating_add(1);
        let signal = analyze_pcm_frame(&frame, &previous_output_frame);
        previous_output_frame.clear();
        previous_output_frame.extend_from_slice(&frame);
        let data: Arc<[u8]> = Arc::from(frame.as_slice());
        let source_pts_ns = (real == FRAME_SAMPLES)
            .then_some(next_source_pts_ns)
            .flatten();
        let source_discontinuity = source_discontinuity_pending && real == FRAME_SAMPLES;
        if real == FRAME_SAMPLES {
            next_source_pts_ns =
                next_source_pts_ns.map(|pts| pts.saturating_add(FRAME_DURATION.as_nanos() as u64));
            source_discontinuity_pending = false;
        }
        let _ = runtime.frame_tx.send(PacedFrame {
            _sequence: sequence,
            source_pts_ns,
            source_discontinuity,
            data,
        });
        let now = Instant::now();
        if let Ok(mut stats) = runtime.stats.lock() {
            reset_audio_health_window(&mut stats, now);
            stats.frames = stats.frames.saturating_add(1);
            if late_tick {
                stats.late_ticks = stats.late_ticks.saturating_add(1);
                stats.recent_late_ticks = stats.recent_late_ticks.saturating_add(1);
            }
            if catchup_tick {
                stats.catchup_ticks = stats.catchup_ticks.saturating_add(1);
                stats.recent_catchup_ticks = stats.recent_catchup_ticks.saturating_add(1);
            }
            if real == FRAME_SAMPLES {
                stats.real_frames = stats.real_frames.saturating_add(1);
            } else if concealed {
                stats.concealed_frames = stats.concealed_frames.saturating_add(1);
                stats.recent_concealed_frames = stats.recent_concealed_frames.saturating_add(1);
            } else if real == 0 {
                stats.silence_frames = stats.silence_frames.saturating_add(1);
            } else {
                stats.partial_frames = stats.partial_frames.saturating_add(1);
                stats.recent_partial_frames = stats.recent_partial_frames.saturating_add(1);
            }
            stats.stale_samples_dropped = stats.stale_samples_dropped.saturating_add(stale_dropped);
            stats.recent_stale_samples_dropped = stats
                .recent_stale_samples_dropped
                .saturating_add(stale_dropped);
            if signal.near_silent {
                stats.near_silent_frames = stats.near_silent_frames.saturating_add(1);
                stats.consecutive_near_silent_frames =
                    stats.consecutive_near_silent_frames.saturating_add(1);
            } else {
                stats.consecutive_near_silent_frames = 0;
            }
            if signal.repeated {
                stats.repeated_frames = stats.repeated_frames.saturating_add(1);
                stats.consecutive_repeated_frames =
                    stats.consecutive_repeated_frames.saturating_add(1);
                if !signal.near_silent {
                    stats.repeated_non_silent_frames =
                        stats.repeated_non_silent_frames.saturating_add(1);
                    stats.recent_repeated_non_silent_frames =
                        stats.recent_repeated_non_silent_frames.saturating_add(1);
                }
            } else {
                stats.consecutive_repeated_frames = 0;
            }
            stats.clipped_samples = stats.clipped_samples.saturating_add(signal.clipped_samples);
            stats.recent_clipped_samples = stats
                .recent_clipped_samples
                .saturating_add(signal.clipped_samples);
            stats.last_peak = signal.peak;
            stats.last_rms_dbfs = signal.rms_dbfs;
            stats.last_max_sample_jump = signal.max_sample_jump;
            stats.queued_samples = samples.len();
            stats.last_tick_gap_ms = tick_gap.map(|gap| gap.as_millis());
            if let Some(gap) = tick_gap {
                stats.max_tick_gap_ms = stats.max_tick_gap_ms.max(gap.as_millis());
            }
            stats.last_input_age_ms =
                last_input_at.map(|instant| now.duration_since(instant).as_millis());
            stats.last_frame_age_ms = Some(0);
        }
    }
}

fn target_source_queue_samples() -> usize {
    (SAMPLE_RATE as u64 * TARGET_SOURCE_QUEUE_MS / 1_000) as usize
}

fn max_source_queue_samples() -> usize {
    (SAMPLE_RATE as u64 * MAX_SOURCE_QUEUE_MS / 1_000) as usize
}

fn soft_source_queue_samples() -> usize {
    let target = target_source_queue_samples();
    let max = max_source_queue_samples();
    let soft = (SAMPLE_RATE as u64 * SOFT_SOURCE_QUEUE_MS / 1_000) as usize;
    soft.clamp(target + FRAME_SAMPLES, max)
}

fn trim_source_queue(samples: &mut VecDeque<QueuedSample>) -> u64 {
    let max_samples = max_source_queue_samples();
    let target_samples = target_source_queue_samples();
    let soft_samples = soft_source_queue_samples();
    let mut stale_dropped = 0u64;
    let keep_samples = if samples.len() > soft_samples
        || (samples.len() > max_samples && source_queue_prefix_near_silent(samples))
    {
        target_samples
    } else {
        max_samples
    };
    while samples.len() > keep_samples {
        samples.pop_front();
        stale_dropped = stale_dropped.saturating_add(1);
    }
    stale_dropped
}

fn source_queue_prefix_near_silent(samples: &VecDeque<QueuedSample>) -> bool {
    samples.len() >= FRAME_SAMPLES
        && samples
            .iter()
            .take(FRAME_SAMPLES)
            .all(|sample| sample.value.unsigned_abs() <= NEAR_SILENT_PEAK)
}

fn render_exact_frame(
    samples: &mut VecDeque<QueuedSample>,
    frame: &mut Vec<u8>,
    last_real_frame: &mut [i16],
) -> bool {
    let mut bypass_loudness = false;
    for output_index in 0..FRAME_SAMPLES {
        let sample = samples.pop_front().unwrap_or_default();
        if let Some(slot) = last_real_frame.get_mut(output_index) {
            *slot = sample.value;
        }
        bypass_loudness |= sample.bypass_loudness;
        push_i16(frame, sample.value);
    }
    bypass_loudness
}

fn render_concealment_frame(last_real_frame: &[i16], remaining: u8, frame: &mut Vec<u8>) {
    let denominator = f32::from(CONCEALMENT_FRAMES) + 1.0;
    let gain = (f32::from(remaining) / denominator).clamp(0.0, 1.0);
    for sample in last_real_frame.iter().take(FRAME_SAMPLES) {
        push_i16(frame, (f32::from(*sample) * gain).round() as i16);
    }
    let missing = FRAME_SAMPLES.saturating_sub(last_real_frame.len().min(FRAME_SAMPLES));
    for _ in 0..missing {
        push_i16(frame, 0);
    }
}

#[derive(Debug, Clone, Copy)]
struct FrameSignal {
    peak: u16,
    rms_dbfs: f64,
    max_sample_jump: u16,
    clipped_samples: u64,
    near_silent: bool,
    repeated: bool,
}

fn analyze_pcm_frame(frame: &[u8], previous: &[u8]) -> FrameSignal {
    let mut peak = 0u16;
    let mut max_sample_jump = 0u16;
    let mut clipped_samples = 0u64;
    let mut sum_squares = 0f64;
    let mut count = 0usize;
    let mut previous_sample: Option<i16> = None;

    for sample_bytes in frame.chunks_exact(2) {
        let sample = i16::from_le_bytes([sample_bytes[0], sample_bytes[1]]);
        let abs = sample.unsigned_abs();
        peak = peak.max(abs);
        if abs >= CLIP_SAMPLE_PEAK {
            clipped_samples = clipped_samples.saturating_add(1);
        }
        if let Some(previous_sample) = previous_sample {
            let jump = i32::from(sample)
                .saturating_sub(i32::from(previous_sample))
                .unsigned_abs()
                .min(u32::from(u16::MAX)) as u16;
            max_sample_jump = max_sample_jump.max(jump);
        }
        previous_sample = Some(sample);
        let sample_f64 = f64::from(sample);
        sum_squares += sample_f64 * sample_f64;
        count += 1;
    }

    let rms = if count == 0 {
        0.0
    } else {
        (sum_squares / count as f64).sqrt()
    };
    let rms_dbfs = if rms <= f64::EPSILON {
        -120.0
    } else {
        20.0 * (rms / 32768.0).log10()
    };
    FrameSignal {
        peak,
        rms_dbfs,
        max_sample_jump,
        clipped_samples,
        near_silent: peak <= NEAR_SILENT_PEAK,
        repeated: !previous.is_empty() && previous == frame,
    }
}

fn classify_audio_health(stats: &FeedStats) -> (bool, Vec<String>) {
    let mut warnings = Vec::new();
    if stats.recent_dropped_input_chunks > 0 {
        warnings.push(format!(
            "recent_dropped_input_chunks={}",
            stats.recent_dropped_input_chunks
        ));
    }
    if stats.recent_stale_samples_dropped > 0 {
        warnings.push(format!(
            "recent_stale_samples_dropped={}",
            stats.recent_stale_samples_dropped
        ));
    }
    if stats.recent_concealed_frames > 0 {
        warnings.push(format!(
            "recent_concealed_frames={}",
            stats.recent_concealed_frames
        ));
    }
    if stats.recent_partial_frames > 0 {
        warnings.push(format!(
            "recent_partial_frames={}",
            stats.recent_partial_frames
        ));
    }
    if stats.recent_repeated_non_silent_frames > 0 {
        warnings.push(format!(
            "recent_repeated_non_silent_frames={}",
            stats.recent_repeated_non_silent_frames
        ));
    }
    if stats.recent_clipped_samples > 0 {
        warnings.push(format!(
            "recent_clipped_samples={}",
            stats.recent_clipped_samples
        ));
    }
    if stats.recent_late_ticks > 0 {
        warnings.push(format!("recent_late_ticks={}", stats.recent_late_ticks));
    }
    if stats.recent_catchup_ticks > 0 {
        warnings.push(format!(
            "recent_catchup_ticks={}",
            stats.recent_catchup_ticks
        ));
    }
    if stats.last_tick_gap_ms > Some(AUDIO_HEALTH_TICK_GAP_WARN_MS) {
        warnings.push(format!(
            "last_tick_gap_ms={}",
            stats.last_tick_gap_ms.unwrap_or_default()
        ));
    }
    if stats.frames > 50 && stats.queued_samples < FRAME_SAMPLES {
        warnings.push(format!("source_queue_low_samples={}", stats.queued_samples));
    }
    if stats.frames > 50 {
        match stats.last_input_age_ms {
            Some(age) if age > AUDIO_HEALTH_INPUT_STALE_MS => {
                warnings.push(format!("last_input_age_ms={age}"));
            }
            None => warnings.push("no_input_seen".to_string()),
            _ => {}
        }
    }
    (warnings.is_empty(), warnings)
}

fn reset_audio_health_window(stats: &mut FeedStats, now: Instant) {
    let started_at = stats.health_window_started_at.get_or_insert(now);
    if now.saturating_duration_since(*started_at) < AUDIO_HEALTH_WINDOW {
        return;
    }
    stats.recent_concealed_frames = 0;
    stats.recent_partial_frames = 0;
    stats.recent_late_ticks = 0;
    stats.recent_catchup_ticks = 0;
    stats.recent_dropped_input_chunks = 0;
    stats.recent_stale_samples_dropped = 0;
    stats.recent_repeated_non_silent_frames = 0;
    stats.recent_clipped_samples = 0;
    stats.health_window_started_at = Some(now);
}

fn append_normalized_chunk(samples: &mut VecDeque<QueuedSample>, chunk: PcmChunk) {
    let pcm = normalize_pcm(
        Pcm {
            sample_rate: chunk.sample_rate,
            channels: chunk.channels,
            data: chunk.data,
        },
        SAMPLE_RATE,
        CHANNELS,
    );
    samples.extend(pcm16_samples(&pcm.data).map(|value| QueuedSample {
        value,
        bypass_loudness: chunk.bypass_loudness,
    }));
}

async fn run_status_bridge_loop(addr: String, state: MediaState) {
    if addr.trim().is_empty() {
        warn!(
            "haze-media event bridge address is empty; media status events will not be published"
        );
        return;
    }
    loop {
        match TcpStream::connect(addr.trim()).await {
            Ok(stream) => {
                info!(
                    "haze-media connected to host event bridge at {}",
                    addr.trim()
                );
                if let Err(err) = run_status_bridge_connection(stream, &state).await {
                    warn!("haze-media host event bridge disconnected: {err:#}");
                }
            }
            Err(err) => {
                warn!(
                    "haze-media waiting for host event bridge at {}: {err}",
                    addr.trim()
                );
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn run_status_bridge_connection(stream: TcpStream, state: &MediaState) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while matches!(lines.next_line().await, Ok(Some(_))) {}
    });
    write_bridge_event(
        &mut writer,
        json!({
            "type": "bridge.client",
            "source": SOURCE_ID,
            "data": { "receive_events": false },
        }),
    )
    .await?;
    write_bridge_event(
        &mut writer,
        json!({
            "type": "service.ready",
            "source": SOURCE_ID,
            "data": {
                "service": SOURCE_ID,
                "backend": state.backend.as_str(),
                "gstreamer_available": state.gstreamer_available,
            },
        }),
    )
    .await?;

    let mut status = interval(STATUS_INTERVAL);
    status.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        status.tick().await;
        publish_status(&mut writer, state).await?;
    }
}

async fn run_pcm_bridge_loop(addr: String, state: MediaState) {
    if addr.trim().is_empty() {
        warn!("haze-media media bridge address is empty; media service will only serve silence");
        return;
    }
    loop {
        match TcpStream::connect(addr.trim()).await {
            Ok(stream) => {
                info!(
                    "haze-media connected to host media bridge at {}",
                    addr.trim()
                );
                if let Err(err) = run_pcm_bridge_connection(stream, &state).await {
                    warn!("haze-media host media bridge disconnected: {err:#}");
                }
            }
            Err(err) => {
                warn!(
                    "haze-media waiting for host media bridge at {}: {err}",
                    addr.trim()
                );
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn run_pcm_bridge_connection(stream: TcpStream, state: &MediaState) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    write_bridge_event(
        &mut writer,
        json!({
            "type": "bridge.client",
            "source": SOURCE_ID,
            "data": {
                "receive_events": true,
                "binary_pcm": true,
            },
        }),
    )
    .await?;

    let mut reader = BufReader::new(reader);
    let mut payload = Vec::with_capacity(16 * 1024);
    loop {
        let Some(is_binary) = read_pcm_bridge_payload(&mut reader, &mut payload).await? else {
            bail!("host media bridge closed");
        };
        if !is_binary && payload.len() > MEDIA_BRIDGE_LINE_LIMIT {
            warn!(
                bytes = payload.len(),
                "dropping oversized media bridge event"
            );
            continue;
        }
        let chunk = if is_binary {
            decode_binary_pcm_frame(&payload)
        } else {
            decode_pcm_event(&payload)
        };
        if let Some(chunk) = chunk {
            state.publish_pcm(chunk);
        }
    }
}

/// Reads either a newline-delimited legacy event or one compact PCM frame.
async fn read_pcm_bridge_payload<R>(reader: &mut R, payload: &mut Vec<u8>) -> Result<Option<bool>>
where
    R: AsyncBufRead + Unpin,
{
    payload.clear();
    let mut first = [0u8; 1];
    let read = reader
        .read(&mut first)
        .await
        .context("failed to read host media bridge payload")?;
    if read == 0 {
        return Ok(None);
    }
    payload.push(first[0]);
    if first[0] != PCM_FRAME_MAGIC[0] {
        reader
            .read_until(b'\n', payload)
            .await
            .context("failed to read host media bridge event")?;
        return Ok(Some(false));
    }

    let mut prefix_tail = [0u8; PCM_FRAME_PREFIX_LEN - 1];
    reader
        .read_exact(&mut prefix_tail)
        .await
        .context("failed to read host media bridge PCM prefix")?;
    payload.extend_from_slice(&prefix_tail);
    let frame_len =
        pcm_frame_len_from_prefix(payload).context("invalid host media bridge PCM prefix")?;
    payload.resize(frame_len, 0);
    reader
        .read_exact(&mut payload[PCM_FRAME_PREFIX_LEN..])
        .await
        .context("failed to read host media bridge PCM frame")?;
    Ok(Some(true))
}

async fn publish_status<W>(writer: &mut W, state: &MediaState) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    write_bridge_event(
        writer,
        json!({
            "type": "media.status",
            "source": SOURCE_ID,
            "data": state.health(),
        }),
    )
    .await?;
    for feed in state.snapshots() {
        write_bridge_event(
            writer,
            json!({
                "type": "media.feed.status",
                "source": SOURCE_ID,
                "feed_id": feed.feed_id,
                "data": feed,
            }),
        )
        .await?;
    }
    Ok(())
}

async fn write_bridge_event<W>(writer: &mut W, mut value: Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    if value.get("timestamp").is_none() {
        value["timestamp"] = json!(chrono::Utc::now().to_rfc3339());
    }
    let mut raw = serde_json::to_vec(&value)?;
    raw.push(b'\n');
    writer
        .write_all(&raw)
        .await
        .context("failed to write host bridge event")
}

fn decode_pcm_event(raw: &[u8]) -> Option<PcmChunk> {
    #[derive(Deserialize)]
    struct Event {
        #[serde(default)]
        r#type: String,
        #[serde(default)]
        feed_id: String,
        #[serde(default)]
        data: EventData,
    }
    #[derive(Default, Deserialize)]
    struct EventData {
        #[serde(default)]
        feed_id: String,
        #[serde(default)]
        sample_rate: u32,
        #[serde(default)]
        channels: u16,
        #[serde(default)]
        bypass_loudness: bool,
        #[serde(default)]
        audio_processing: AudioProcessingData,
        #[serde(default)]
        pcm: String,
    }
    #[derive(Default, Deserialize)]
    struct AudioProcessingData {
        #[serde(default)]
        bypass_loudness: bool,
    }
    let event = serde_json::from_slice::<Event>(raw).ok()?;
    if event.r#type != "playout.pcm" {
        return None;
    }
    let feed_id = first_non_blank(&[
        Some(event.data.feed_id.as_str()),
        Some(event.feed_id.as_str()),
    ]);
    if feed_id.is_empty() || event.data.pcm.trim().is_empty() {
        return None;
    }
    let mut data = base64::engine::general_purpose::STANDARD
        .decode(event.data.pcm.trim())
        .ok()?;
    let sample_rate = event.data.sample_rate.max(8_000);
    let channels = event.data.channels.max(1);
    let frame_bytes = AudioFormat::new(sample_rate, channels).frame_bytes();
    if frame_bytes == 0 || data.len() < frame_bytes {
        return None;
    }
    let aligned = data.len() - data.len() % frame_bytes;
    data.truncate(aligned);
    Some(PcmChunk {
        feed_id,
        sample_rate,
        channels,
        data,
        bypass_loudness: event.data.bypass_loudness || event.data.audio_processing.bypass_loudness,
        sequence: None,
        pts_ns: None,
        discontinuity: false,
    })
}

fn decode_binary_pcm_frame(raw: &[u8]) -> Option<PcmChunk> {
    let frame = decode_pcm_frame(raw).ok()?;
    let sample_rate = frame.sample_rate.max(8_000);
    let channels = frame.channels.max(1);
    let frame_bytes = AudioFormat::new(sample_rate, channels).frame_bytes();
    if frame_bytes == 0 || frame.pcm.len() < frame_bytes {
        return None;
    }
    let aligned = frame.pcm.len() - frame.pcm.len() % frame_bytes;
    Some(PcmChunk {
        feed_id: frame.feed_id.to_string(),
        sample_rate,
        channels,
        data: frame.pcm[..aligned].to_vec(),
        bypass_loudness: false,
        sequence: Some(frame.sequence),
        pts_ns: Some(frame.pts_ns),
        discontinuity: frame.discontinuity,
    })
}

async fn run_http_server(addr: &str, state: MediaState) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind haze-media HTTP listener at {addr}"))?;
    loop {
        let (stream, peer) = listener.accept().await?;
        if let Err(err) = stream.set_nodelay(true) {
            debug!(%peer, "failed to set TCP_NODELAY for haze-media HTTP connection: {err}");
        }
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_http_connection(stream, state).await {
                debug!(%peer, "haze-media HTTP connection ended: {err:#}");
            }
        });
    }
}

async fn handle_http_connection(mut stream: TcpStream, state: MediaState) -> Result<()> {
    let request = read_http_request(&mut stream).await?;
    match request.path.as_str() {
        "/health" | "/api/v1/health" => {
            write_json_response(&mut stream, 200, state.health()).await?;
        }
        "/api/v1/feed/audio" => {
            let feed_id = query_value(&request.query, "feed").unwrap_or_default();
            let codec = query_value(&request.query, "codec").unwrap_or_else(|| "pcm16".to_string());
            if !valid_feed_id(&feed_id) {
                write_text_response(&mut stream, 400, "feed is required\n").await?;
                return Ok(());
            }
            stream_feed_audio(&mut stream, &state, &feed_id, &codec).await?;
        }
        "/api/v1/webrtc/offer" => {
            handle_webrtc_offer(&mut stream, state, request).await?;
        }
        _ => {
            write_text_response(&mut stream, 404, "not found\n").await?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    query: String,
    body: Vec<u8>,
}

async fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest> {
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let header_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            bail!("connection closed before request");
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(pos) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos + 4;
        }
        if buffer.len() > HTTP_HEADER_LIMIT {
            bail!("HTTP header too large");
        }
    };
    let headers = String::from_utf8_lossy(&buffer[..header_end]);
    let mut header_lines = headers.lines();
    let first_line = header_lines.next().unwrap_or("");
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("/");
    if method != "GET" && method != "HEAD" && method != "POST" {
        bail!("unsupported HTTP method {method}");
    }
    let mut content_length = 0usize;
    for line in header_lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("content-length") {
            content_length = value
                .trim()
                .parse::<usize>()
                .context("invalid HTTP Content-Length")?;
            if content_length > HTTP_BODY_LIMIT {
                bail!("HTTP body too large");
            }
        }
    }
    let mut body = buffer[header_end..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            bail!("connection closed before HTTP body");
        }
        body.extend_from_slice(&chunk[..read]);
        if body.len() > HTTP_BODY_LIMIT {
            bail!("HTTP body too large");
        }
    }
    body.truncate(content_length);
    let (path, query) = target
        .split_once('?')
        .map(|(path, query)| (path.to_string(), query.to_string()))
        .unwrap_or_else(|| (target.to_string(), String::new()));
    Ok(HttpRequest {
        method: method.to_string(),
        path,
        query,
        body,
    })
}

async fn stream_feed_audio(
    stream: &mut TcpStream,
    state: &MediaState,
    feed_id: &str,
    codec: &str,
) -> Result<()> {
    let format = HttpAudioFormat::parse(codec);
    let Some(feed) = state.feed(feed_id) else {
        write_text_response(stream, 404, "feed is not configured\n").await?;
        return Ok(());
    };
    let client = state.register_http_client(feed_id, format.id);
    let client_id = client.id();
    match format.kind {
        HttpAudioKind::Wav => stream_wav(stream, state, client_id, feed).await,
        HttpAudioKind::Raw => stream_raw_pcm(stream, state, client_id, feed).await,
        HttpAudioKind::GStreamer => {
            if !state.gstreamer_available {
                write_text_response(stream, 503, "gstreamer backend is not available\n").await?;
                return Ok(());
            }
            stream_gstreamer_encoded(stream, state, client_id, feed, format).await
        }
        HttpAudioKind::Unsupported => {
            write_text_response(stream, 415, "unsupported media codec\n").await?;
            Ok(())
        }
    }
}

#[derive(Debug, Deserialize)]
struct WebRTCOfferRequest {
    #[serde(default)]
    feed_id: String,
    #[serde(default)]
    sdp: String,
    #[serde(default)]
    _sdp_type: String,
    #[serde(default)]
    preferred_codec: String,
    #[serde(default)]
    codec: String,
    #[serde(default)]
    require_opus: bool,
    #[serde(default)]
    disable_g722: bool,
    #[serde(default)]
    client_ip: String,
    #[serde(default)]
    remote_addr: String,
}

async fn handle_webrtc_offer(
    stream: &mut TcpStream,
    state: MediaState,
    request: HttpRequest,
) -> Result<()> {
    if request.method != "POST" {
        write_text_response(stream, 405, "method not allowed\n").await?;
        return Ok(());
    }
    if !state.webrtc_available {
        write_json_response(
            stream,
            503,
            json!({
                "error": "str0m WebRTC backend is unavailable",
            }),
        )
        .await?;
        return Ok(());
    }
    let payload = match serde_json::from_slice::<WebRTCOfferRequest>(&request.body) {
        Ok(payload) => payload,
        Err(err) => {
            write_json_response(
                stream,
                400,
                json!({"error": format!("invalid WebRTC offer JSON: {err}")}),
            )
            .await?;
            return Ok(());
        }
    };
    let feed_id = payload.feed_id.trim().to_string();
    if !valid_feed_id(&feed_id) {
        write_json_response(stream, 400, json!({"error": "feed_id is required"})).await?;
        return Ok(());
    }
    let offer_sdp = payload.sdp.trim().to_string();
    if offer_sdp.is_empty() || offer_sdp.len() > HTTP_BODY_LIMIT {
        write_json_response(stream, 400, json!({"error": "sdp is required"})).await?;
        return Ok(());
    }
    let preferred_codec = first_non_blank(&[
        Some(payload.preferred_codec.as_str()),
        Some(payload.codec.as_str()),
    ]);
    let Some(selection) = select_webrtc_audio_codec(
        &offer_sdp,
        &state.webrtc_codecs,
        &preferred_codec,
        payload.require_opus,
        payload.disable_g722,
    ) else {
        write_json_response(
            stream,
            415,
            json!({
                "error": "the WebRTC offer did not include a codec supported by haze-media",
                "supported_codecs": state.webrtc_codecs.iter().map(|codec| codec.id()).collect::<Vec<_>>(),
            }),
        )
        .await?;
        return Ok(());
    };

    let Some(feed) = state.feed(&feed_id) else {
        write_json_response(stream, 404, json!({"error": "feed is not configured"})).await?;
        return Ok(());
    };
    let media_recent = feed
        .snapshot()
        .last_input_age_ms
        .map(|age| age <= 5_000)
        .unwrap_or(false);
    let stream_peer_addr = stream.peer_addr().ok();
    let request_peer_ip =
        parse_webrtc_request_peer_ip(&payload).or_else(|| stream_peer_addr.map(|addr| addr.ip()));
    let listener_client_id = webrtc_listener_client_id(&payload, stream_peer_addr);
    let setup = build_str0m_webrtc_peer(&offer_sdp, selection, request_peer_ip).await;
    let peer = match setup {
        Ok(peer) => peer,
        Err(err) => {
            warn!(feed_id, "failed to create str0m WebRTC peer: {err:#}");
            write_json_response(
                stream,
                503,
                json!({"error": format!("failed to create str0m WebRTC peer: {err:#}")}),
            )
            .await?;
            return Ok(());
        }
    };
    let answer_sdp = peer.answer_sdp.clone();
    let Some(peer_id) =
        state.register_webrtc_peer(&feed_id, selection.codec.id(), &listener_client_id)
    else {
        write_json_response(
            stream,
            429,
            json!({
                "error": "listener already active for this IP and feed",
            }),
        )
        .await?;
        return Ok(());
    };
    if let Err(err) = write_json_response(
        stream,
        200,
        json!({
            "feed_id": feed_id,
            "sdp": answer_sdp,
            "sdp_type": "answer",
            "codec": selection.codec.id(),
            "payload_type": selection.payload_type,
            "media_recent": media_recent,
        }),
    )
    .await
    {
        state.unregister_webrtc_peer(peer_id);
        return Err(err);
    }
    start_webrtc_peer_feeder(state.clone(), peer_id, feed, peer);
    Ok(())
}

#[cfg(feature = "gstreamer-backend")]
struct Str0mWebRTCPeer {
    rtc: str0m::Rtc,
    socket: UdpSocket,
    local_candidate_addr: SocketAddr,
    answer_sdp: String,
    selection: WebRTCCodecSelection,
}

#[cfg(not(feature = "gstreamer-backend"))]
struct Str0mWebRTCPeer {
    answer_sdp: String,
}

#[cfg(feature = "gstreamer-backend")]
async fn build_str0m_webrtc_peer(
    offer_sdp: &str,
    selection: WebRTCCodecSelection,
    request_peer_ip: Option<IpAddr>,
) -> Result<Str0mWebRTCPeer> {
    let offer_sdp = offer_sdp.to_string();
    let socket = bind_str0m_webrtc_udp_socket().await?;
    let local_addr = socket
        .local_addr()
        .context("failed to read str0m WebRTC UDP socket address")?;
    let selected_ip = detect_webrtc_candidate_ip(&offer_sdp, request_peer_ip);
    let public_ip = configured_webrtc_public_candidate_ip();
    let host_ip = if public_ip.is_some_and(|candidate| candidate.ip == selected_ip.ip) {
        detect_webrtc_host_candidate_ip(&offer_sdp, request_peer_ip)
    } else {
        selected_ip
    };
    let host_addr = SocketAddr::new(host_ip.ip, local_addr.port());
    let mut rtc = str0m::RtcConfig::new()
        .set_crypto_provider(Arc::new(str0m::crypto::from_feature_flags()))
        .build(Instant::now());
    let host_candidate = str0m::Candidate::host(host_addr, "udp")
        .with_context(|| format!("failed to build host ICE candidate for {host_addr}"))?;
    rtc.add_local_candidate(host_candidate);
    info!(
        candidate = %host_addr,
        source = host_ip.source,
        "added str0m WebRTC ICE host candidate"
    );
    if let Some(public_ip) = public_ip.filter(|candidate| candidate.ip != host_ip.ip) {
        let public_addr = SocketAddr::new(public_ip.ip, local_addr.port());
        if public_addr.is_ipv4() == host_addr.is_ipv4() {
            let public_candidate =
                str0m::Candidate::server_reflexive(public_addr, host_addr, "udp").with_context(
                    || {
                        format!(
                    "failed to build server-reflexive ICE candidate {public_addr} for {host_addr}"
                )
                    },
                )?;
            rtc.add_local_candidate(public_candidate);
            info!(
                candidate = %public_addr,
                base = %host_addr,
                source = public_ip.source,
                "added str0m WebRTC ICE server-reflexive candidate"
            );
        } else {
            warn!(
                candidate = %public_addr,
                base = %host_addr,
                "ignored WebRTC public candidate with a different IP family than its host candidate"
            );
        }
    }
    let offer = str0m::change::SdpOffer::from_sdp_string(&offer_sdp)
        .context("failed to parse WebRTC offer SDP")?;
    let answer = rtc
        .sdp_api()
        .accept_offer(offer)
        .context("failed to accept WebRTC offer")?;
    let answer_sdp =
        ensure_str0m_answer_codec_lines(&answer.to_sdp_string(), &offer_sdp, selection);
    Ok(Str0mWebRTCPeer {
        rtc,
        socket,
        local_candidate_addr: host_addr,
        answer_sdp,
        selection,
    })
}

fn configured_webrtc_udp_port_range() -> Result<Option<(u16, u16)>> {
    parse_webrtc_udp_port_range(
        env::var("HAZE_MEDIA_WEBRTC_UDP_PORT_MIN").ok().as_deref(),
        env::var("HAZE_MEDIA_WEBRTC_UDP_PORT_MAX").ok().as_deref(),
    )
}

fn parse_webrtc_udp_port_range(
    raw_min: Option<&str>,
    raw_max: Option<&str>,
) -> Result<Option<(u16, u16)>> {
    let raw_min = raw_min.map(str::trim).filter(|value| !value.is_empty());
    let raw_max = raw_max.map(str::trim).filter(|value| !value.is_empty());
    if raw_min.is_none() && raw_max.is_none() {
        return Ok(None);
    }
    let Some(raw_min) = raw_min else {
        bail!("HAZE_MEDIA_WEBRTC_UDP_PORT_MIN is required when the WebRTC UDP range is configured");
    };
    let Some(raw_max) = raw_max else {
        bail!("HAZE_MEDIA_WEBRTC_UDP_PORT_MAX is required when the WebRTC UDP range is configured");
    };
    let min = raw_min
        .parse::<u16>()
        .with_context(|| format!("invalid HAZE_MEDIA_WEBRTC_UDP_PORT_MIN {raw_min:?}"))?;
    let max = raw_max
        .parse::<u16>()
        .with_context(|| format!("invalid HAZE_MEDIA_WEBRTC_UDP_PORT_MAX {raw_max:?}"))?;
    if min < 1_024 {
        bail!("WebRTC UDP ports below 1024 are not supported");
    }
    if max < min {
        bail!("WebRTC UDP port maximum must be greater than or equal to the minimum");
    }
    let count = u32::from(max) - u32::from(min) + 1;
    if count > MAX_WEBRTC_UDP_PORTS {
        bail!(
            "WebRTC UDP port range contains {count} ports; the maximum is {MAX_WEBRTC_UDP_PORTS}"
        );
    }
    Ok(Some((min, max)))
}

#[cfg(feature = "gstreamer-backend")]
async fn bind_str0m_webrtc_udp_socket() -> Result<UdpSocket> {
    let Some((min, max)) = configured_webrtc_udp_port_range()? else {
        return UdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
            .await
            .context("failed to bind str0m WebRTC UDP socket");
    };
    let count = u64::from(max) - u64::from(min) + 1;
    let start = WEBRTC_UDP_PORT_CURSOR.fetch_add(1, Ordering::Relaxed) % count;
    for offset in 0..count {
        let port = u64::from(min) + ((start + offset) % count);
        let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port as u16));
        match UdpSocket::bind(addr).await {
            Ok(socket) => return Ok(socket),
            Err(err) if err.kind() == ErrorKind::AddrInUse => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to bind str0m WebRTC UDP socket at {addr}"));
            }
        }
    }
    bail!("WebRTC UDP port range {min}-{max} is exhausted")
}

#[cfg(not(feature = "gstreamer-backend"))]
async fn build_str0m_webrtc_peer(
    _offer_sdp: &str,
    _selection: WebRTCCodecSelection,
    _request_peer_ip: Option<IpAddr>,
) -> Result<Str0mWebRTCPeer> {
    bail!("haze-media was built without GStreamer support")
}

#[cfg(feature = "gstreamer-backend")]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct WebRTCCandidateIp {
    ip: IpAddr,
    source: &'static str,
}

#[cfg(feature = "gstreamer-backend")]
fn detect_webrtc_candidate_ip(
    offer_sdp: &str,
    request_peer_ip: Option<IpAddr>,
) -> WebRTCCandidateIp {
    let overrides = [
        (
            "HAZE_MEDIA_WEBRTC_HOST",
            env::var("HAZE_MEDIA_WEBRTC_HOST").ok(),
        ),
        ("HAZE_WEBRTC_HOST", env::var("HAZE_WEBRTC_HOST").ok()),
        ("HAZE_PUBLIC_HOST", env::var("HAZE_PUBLIC_HOST").ok()),
    ];
    detect_webrtc_candidate_ip_inner(offer_sdp, request_peer_ip, &overrides)
}

#[cfg(feature = "gstreamer-backend")]
fn configured_webrtc_public_candidate_ip() -> Option<WebRTCCandidateIp> {
    let overrides = [
        (
            "HAZE_MEDIA_WEBRTC_HOST",
            env::var("HAZE_MEDIA_WEBRTC_HOST").ok(),
        ),
        ("HAZE_WEBRTC_HOST", env::var("HAZE_WEBRTC_HOST").ok()),
        ("HAZE_PUBLIC_HOST", env::var("HAZE_PUBLIC_HOST").ok()),
    ];
    configured_webrtc_public_candidate_ip_inner(&overrides)
}

#[cfg(feature = "gstreamer-backend")]
fn configured_webrtc_public_candidate_ip_inner(
    overrides: &[(&'static str, Option<String>)],
) -> Option<WebRTCCandidateIp> {
    for (name, raw) in overrides {
        let Some(raw) = raw.as_deref() else {
            continue;
        };
        if let Some(ip) = parse_webrtc_host_override(*name, raw) {
            return Some(WebRTCCandidateIp { ip, source: *name });
        }
    }
    None
}

#[cfg(feature = "gstreamer-backend")]
fn detect_webrtc_candidate_ip_inner(
    offer_sdp: &str,
    request_peer_ip: Option<IpAddr>,
    overrides: &[(&'static str, Option<String>)],
) -> WebRTCCandidateIp {
    if let Some(remote_ip) = request_peer_ip.filter(|ip| webrtc_peer_is_private(*ip)) {
        if let Some(ip) = local_ip_for_remote(remote_ip) {
            return WebRTCCandidateIp {
                ip,
                source: "private_http_peer_route",
            };
        }
    }
    if let Some(candidate) = configured_webrtc_public_candidate_ip_inner(overrides) {
        return candidate;
    }
    detect_webrtc_host_candidate_ip(offer_sdp, request_peer_ip)
}

#[cfg(feature = "gstreamer-backend")]
fn detect_webrtc_host_candidate_ip(
    offer_sdp: &str,
    request_peer_ip: Option<IpAddr>,
) -> WebRTCCandidateIp {
    if let Some(remote_ip) = request_peer_ip {
        if let Some(ip) = local_ip_for_remote(remote_ip) {
            return WebRTCCandidateIp {
                ip,
                source: "http_peer_route",
            };
        }
    }
    if let Some(remote_ip) = offered_host_candidate_ip(offer_sdp) {
        if let Some(ip) = local_ip_for_remote(remote_ip) {
            return WebRTCCandidateIp {
                ip,
                source: "offer_host_route",
            };
        }
    }
    let fallback = if let Some(ip) = local_ip_for_remote(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8))) {
        WebRTCCandidateIp {
            ip,
            source: "default_route",
        }
    } else {
        WebRTCCandidateIp {
            ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            source: "localhost_fallback",
        }
    };
    warn!(
        candidate = %fallback.ip,
        source = fallback.source,
        "WebRTC host candidate used fallback selection; set HAZE_MEDIA_WEBRTC_HOST for deterministic receiver access"
    );
    fallback
}

#[cfg(feature = "gstreamer-backend")]
fn webrtc_peer_is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_link_local() || ip.is_loopback(),
        IpAddr::V6(ip) => ip.is_unique_local() || ip.is_unicast_link_local() || ip.is_loopback(),
    }
}

fn parse_webrtc_request_peer_ip(payload: &WebRTCOfferRequest) -> Option<IpAddr> {
    parse_ip_hint(&payload.client_ip).or_else(|| parse_ip_hint(&payload.remote_addr))
}

fn webrtc_listener_client_id(
    payload: &WebRTCOfferRequest,
    stream_peer_addr: Option<SocketAddr>,
) -> String {
    parse_listener_ip_hint(&payload.client_ip)
        .or_else(|| parse_listener_ip_hint(&payload.remote_addr))
        .or_else(|| stream_peer_addr.map(|addr| addr.ip()))
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn parse_listener_ip_hint(raw: &str) -> Option<IpAddr> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(addr) = value.parse::<SocketAddr>() {
        let ip = addr.ip();
        if !ip.is_unspecified() {
            return Some(ip);
        }
        return None;
    }
    let value = value.trim_matches(['[', ']']);
    let Ok(ip) = value.parse::<IpAddr>() else {
        return None;
    };
    if ip.is_unspecified() {
        return None;
    }
    Some(ip)
}

fn normalize_webrtc_listener_client_id(client_id: &str) -> String {
    let client_id = client_id.trim();
    if client_id.is_empty() {
        "unknown".to_string()
    } else {
        client_id.to_string()
    }
}

fn parse_ip_hint(raw: &str) -> Option<IpAddr> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(addr) = value.parse::<SocketAddr>() {
        let ip = addr.ip();
        if !ip.is_unspecified() && !ip.is_loopback() {
            return Some(ip);
        }
        return None;
    }
    let value = value.trim_matches(['[', ']']);
    let Ok(ip) = value.parse::<IpAddr>() else {
        return None;
    };
    if ip.is_unspecified() || ip.is_loopback() {
        return None;
    }
    Some(ip)
}

#[cfg(feature = "gstreamer-backend")]
fn parse_webrtc_host_override(name: &'static str, raw: &str) -> Option<IpAddr> {
    let value = raw.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("auto") {
        return None;
    }
    match value.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) if ip.is_unspecified() => {
            warn!("{name} is unspecified; ignoring WebRTC host override");
            None
        }
        Ok(IpAddr::V4(ip)) => Some(IpAddr::V4(ip)),
        Ok(IpAddr::V6(_)) => {
            warn!("{name} is IPv6 but the str0m UDP socket is IPv4; ignoring WebRTC host override");
            None
        }
        Err(_) => {
            warn!("{name} is not an IP address; ignoring WebRTC host override");
            None
        }
    }
}

#[cfg(feature = "gstreamer-backend")]
fn local_ip_for_remote(remote_ip: IpAddr) -> Option<IpAddr> {
    if remote_ip.is_unspecified() || remote_ip.is_loopback() {
        return None;
    }
    if let Ok(socket) = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))) {
        if socket.connect(SocketAddr::new(remote_ip, 9)).is_ok() {
            if let Ok(addr) = socket.local_addr() {
                let ip = addr.ip();
                if !ip.is_unspecified() && !ip.is_loopback() {
                    return Some(ip);
                }
            }
        }
    }
    None
}

#[cfg(feature = "gstreamer-backend")]
fn offered_host_candidate_ip(sdp: &str) -> Option<IpAddr> {
    for line in sdp.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("a=candidate:") else {
            continue;
        };
        let parts = rest.split_whitespace().collect::<Vec<_>>();
        if parts.len() < 8 || !parts[7].eq_ignore_ascii_case("host") {
            continue;
        }
        let Ok(ip) = parts[4].parse::<IpAddr>() else {
            continue;
        };
        if ip.is_unspecified() || ip.is_loopback() {
            continue;
        }
        return Some(ip);
    }
    None
}

#[cfg(feature = "gstreamer-backend")]
struct GStreamerWebRTCEncoder {
    pipeline: gstreamer::Pipeline,
    appsrc: gstreamer_app::AppSrc,
}

#[cfg(feature = "gstreamer-backend")]
fn build_gstreamer_webrtc_encoder(
    state: MediaState,
    peer_id: u64,
    selection: WebRTCCodecSelection,
    encoded_tx: mpsc::Sender<Arc<[u8]>>,
) -> Result<GStreamerWebRTCEncoder> {
    use gst::prelude::*;
    use gstreamer as gst;
    use gstreamer_app as gst_app;

    let pipeline_description = gstreamer_webrtc_encoder_pipeline(selection)?;
    let element = gst::parse::launch(&pipeline_description)
        .context("failed to build GStreamer WebRTC encoder pipeline")?;
    let pipeline = element
        .downcast::<gst::Pipeline>()
        .map_err(|_| anyhow::anyhow!("GStreamer WebRTC encoder did not produce a pipeline"))?;
    let appsrc = pipeline
        .by_name("src")
        .context("GStreamer WebRTC encoder pipeline is missing appsrc")?
        .downcast::<gst_app::AppSrc>()
        .map_err(|_| anyhow::anyhow!("GStreamer WebRTC encoder src is not appsrc"))?;
    let appsink = pipeline
        .by_name("sink")
        .context("GStreamer WebRTC encoder pipeline is missing appsink")?
        .downcast::<gst_app::AppSink>()
        .map_err(|_| anyhow::anyhow!("GStreamer WebRTC encoder sink is not appsink"))?;
    appsink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                let Some(buffer) = sample.buffer() else {
                    return Err(gst::FlowError::Error);
                };
                let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                let data = Arc::<[u8]>::from(map.as_slice());
                match encoded_tx.try_send(data) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        state.record_webrtc_peer_drop(peer_id);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err(gst::FlowError::Eos);
                    }
                }
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );
    pipeline
        .set_state(gst::State::Playing)
        .map_err(|err| anyhow::anyhow!("failed to start GStreamer WebRTC encoder: {err:?}"))?;
    Ok(GStreamerWebRTCEncoder { pipeline, appsrc })
}

#[cfg(feature = "gstreamer-backend")]
fn gstreamer_webrtc_encoder_pipeline(selection: WebRTCCodecSelection) -> Result<String> {
    let source = "appsrc name=src is-live=true block=false leaky-type=downstream do-timestamp=false max-bytes=19200 format=time stream-type=stream \
         caps=audio/x-raw,format=S16LE,layout=interleaved,rate=48000,channels=1 \
         ! queue max-size-time=200000000 max-size-buffers=10 max-size-bytes=0 leaky=downstream \
         ! audioconvert ! audioresample quality=4";
    let pipeline = match selection.codec {
        WebRTCAudioCodec::Opus => format!(
            "{source} \
             ! opusenc bitrate={OPUS_BITRATE_BPS} bitrate-type=cbr frame-size=20 audio-type=generic dtx=false inband-fec=true \
             ! appsink name=sink emit-signals=true sync=false"
        ),
        WebRTCAudioCodec::Pcmu => format!(
            "{source} \
             ! audio/x-raw,format=S16LE,layout=interleaved,rate=8000,channels=1 \
             ! mulawenc \
             ! appsink name=sink emit-signals=true sync=false"
        ),
        WebRTCAudioCodec::Pcma => format!(
            "{source} \
             ! audio/x-raw,format=S16LE,layout=interleaved,rate=8000,channels=1 \
             ! alawenc \
             ! appsink name=sink emit-signals=true sync=false"
        ),
        WebRTCAudioCodec::G722 => bail!("str0m WebRTC does not expose G.722 packet writing"),
    };
    Ok(pipeline)
}

#[cfg(feature = "gstreamer-backend")]
fn next_gst_audio_pts(next_pts_ns: &mut Option<u64>, frame_duration_ns: u64) -> u64 {
    let pts_ns = next_pts_ns.unwrap_or(0);
    *next_pts_ns = Some(pts_ns.saturating_add(frame_duration_ns));
    pts_ns
}

#[cfg(feature = "gstreamer-backend")]
fn build_gst_audio_buffer(
    data: &[u8],
    pts_ns: u64,
    frame_duration_ns: u64,
) -> Result<gstreamer::Buffer> {
    use gstreamer as gst;

    let mut buffer =
        gst::Buffer::with_size(data.len()).context("failed to allocate GStreamer audio buffer")?;
    {
        let buffer_mut = buffer
            .get_mut()
            .context("new GStreamer audio buffer is shared")?;
        {
            let mut map = buffer_mut
                .map_writable()
                .context("failed to map GStreamer audio buffer")?;
            map.as_mut_slice().copy_from_slice(data);
        }
        buffer_mut.set_duration(gst::ClockTime::from_nseconds(frame_duration_ns));
        buffer_mut.set_pts(gst::ClockTime::from_nseconds(pts_ns));
    }
    Ok(buffer)
}

#[cfg(feature = "gstreamer-backend")]
fn start_webrtc_peer_feeder(
    state: MediaState,
    peer_id: u64,
    feed: Arc<FeedRuntime>,
    peer: Str0mWebRTCPeer,
) {
    use gstreamer::prelude::*;

    tokio::spawn(async move {
        let mut peer = peer;
        let frame_duration_ns = FRAME_DURATION.as_nanos() as u64;
        let mut next_pts_ns: Option<u64> = None;
        let (encoded_tx, mut encoded_rx) =
            mpsc::channel::<Arc<[u8]>>(STR0M_WEBRTC_ENCODED_QUEUE_CAPACITY);
        let encoder = match build_gstreamer_webrtc_encoder(
            state.clone(),
            peer_id,
            peer.selection,
            encoded_tx,
        ) {
            Ok(encoder) => encoder,
            Err(err) => {
                warn!("failed to start WebRTC audio encoder: {err:#}");
                state.unregister_webrtc_peer(peer_id);
                return;
            }
        };
        let encoder_appsrc = encoder.appsrc.clone();
        let feed_state = state.clone();
        let mut rx = feed.subscribe();
        let feed_task = tokio::spawn(async move {
            'feed: loop {
                let frame = match rx.recv().await {
                    Ok(frame) => frame,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        for _ in 0..skipped {
                            feed_state.record_webrtc_peer_drop(peer_id);
                        }
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break 'feed,
                };
                if frame.source_discontinuity {
                    next_pts_ns = frame.source_pts_ns;
                }
                let pts_ns = frame
                    .source_pts_ns
                    .unwrap_or_else(|| next_gst_audio_pts(&mut next_pts_ns, frame_duration_ns));
                next_pts_ns = Some(pts_ns.saturating_add(frame_duration_ns));
                let mut buffer =
                    match build_gst_audio_buffer(&frame.data, pts_ns, frame_duration_ns) {
                        Ok(buffer) => buffer,
                        Err(err) => {
                            warn!("failed to build WebRTC encoder buffer: {err:#}");
                            break;
                        }
                    };
                if frame.source_discontinuity {
                    if let Some(buffer) = buffer.get_mut() {
                        buffer.set_flags(gstreamer::BufferFlags::DISCONT);
                    }
                }
                if encoder_appsrc.push_buffer(buffer).is_err() {
                    break;
                }
                feed_state.record_webrtc_peer_push(peer_id, frame.data.len());
            }
        });

        let mut audio_mid: Option<str0m::media::Mid> = None;
        let mut connected = false;
        let mut next_timeout =
            match drain_str0m_outputs(&mut peer.rtc, &peer.socket, &mut audio_mid, &mut connected)
                .await
            {
                Ok(Some(timeout)) => timeout,
                Ok(None) => Instant::now() + Duration::from_secs(1),
                Err(err) => {
                    warn!("failed to drain initial str0m WebRTC output: {err:#}");
                    feed_task.abort();
                    let _ = encoder.appsrc.end_of_stream();
                    let _ = encoder.pipeline.set_state(gstreamer::State::Null);
                    state.unregister_webrtc_peer(peer_id);
                    return;
                }
            };
        let mut udp_buf = vec![0u8; STR0M_WEBRTC_UDP_BUFFER];
        let mut rtp_time = 0u64;
        let started_at = Instant::now();
        let mut last_network_at = started_at;
        loop {
            let stale_deadline = webrtc_peer_stale_deadline(started_at, last_network_at, connected);
            let sleep_until = earlier_instant(next_timeout, stale_deadline);
            let timeout_duration = sleep_until.saturating_duration_since(Instant::now());
            tokio::select! {
                received = peer.socket.recv_from(&mut udp_buf) => {
                    let (len, source) = match received {
                        Ok(result) => result,
                        Err(err) => {
                            warn!("str0m WebRTC UDP receive failed: {err:#}");
                            break;
                        }
                    };
                    let receive = match str0m::net::Receive::new(
                        str0m::net::Protocol::Udp,
                        source,
                        peer.local_candidate_addr,
                        &udp_buf[..len],
                    ) {
                        Ok(receive) => receive,
                        Err(err) => {
                            debug!("ignored invalid WebRTC datagram: {err}");
                            continue;
                        }
                    };
                    let received_at = Instant::now();
                    last_network_at = received_at;
                    if let Err(err) = peer.rtc.handle_input(str0m::Input::Receive(received_at, receive)) {
                        warn!("str0m WebRTC input failed: {err:#}");
                        break;
                    }
                    match drain_str0m_outputs(&mut peer.rtc, &peer.socket, &mut audio_mid, &mut connected).await {
                        Ok(Some(timeout)) => next_timeout = timeout,
                        Ok(None) => break,
                        Err(err) => {
                            warn!("str0m WebRTC output failed: {err:#}");
                            break;
                        }
                    }
                }
                encoded = encoded_rx.recv() => {
                    let Some(mut encoded) = encoded else {
                        break;
                    };
                    let frame_ticks = str0m_audio_frame_ticks(peer.selection.codec);
                    let mut skipped_encoded_frames = 0u64;
                    while let Ok(newer_encoded) = encoded_rx.try_recv() {
                        encoded = newer_encoded;
                        skipped_encoded_frames = skipped_encoded_frames.saturating_add(1);
                    }
                    if skipped_encoded_frames > 0 {
                        for _ in 0..skipped_encoded_frames {
                            state.record_webrtc_peer_drop(peer_id);
                        }
                        rtp_time = rtp_time.saturating_add(frame_ticks.saturating_mul(skipped_encoded_frames));
                    }
                    if connected {
                        if let Some(mid) = audio_mid {
                            if let Err(err) = write_str0m_audio_frame(
                                &mut peer.rtc,
                                mid,
                                peer.selection.codec,
                                rtp_time,
                                encoded,
                            ) {
                                warn!("str0m WebRTC audio write failed: {err:#}");
                                break;
                            }
                            rtp_time = rtp_time.saturating_add(frame_ticks);
                            match drain_str0m_outputs(&mut peer.rtc, &peer.socket, &mut audio_mid, &mut connected).await {
                                Ok(Some(timeout)) => next_timeout = timeout,
                                Ok(None) => break,
                                Err(err) => {
                                    warn!("str0m WebRTC output failed after media write: {err:#}");
                                    break;
                                }
                            }
                        } else {
                            state.record_webrtc_peer_drop(peer_id);
                        }
                    } else {
                        state.record_webrtc_peer_drop(peer_id);
                    }
                }
                _ = sleep(timeout_duration) => {
                    let now = Instant::now();
                    if now >= stale_deadline {
                        let stale_for = if connected {
                            now.saturating_duration_since(last_network_at)
                        } else {
                            now.saturating_duration_since(started_at)
                        };
                        warn!(
                            peer_id,
                            connected,
                            stale_ms = stale_for.as_millis(),
                            "closing stale str0m WebRTC peer"
                        );
                        break;
                    }
                    if let Err(err) = peer.rtc.handle_input(str0m::Input::Timeout(now)) {
                        warn!("str0m WebRTC timeout input failed: {err:#}");
                        break;
                    }
                    match drain_str0m_outputs(&mut peer.rtc, &peer.socket, &mut audio_mid, &mut connected).await {
                        Ok(Some(timeout)) => next_timeout = timeout,
                        Ok(None) => break,
                        Err(err) => {
                            warn!("str0m WebRTC timeout output failed: {err:#}");
                            break;
                        }
                    }
                }
            }
        }
        feed_task.abort();
        let _ = encoder.appsrc.end_of_stream();
        let _ = encoder.pipeline.set_state(gstreamer::State::Null);
        state.unregister_webrtc_peer(peer_id);
    });
}

#[cfg(not(feature = "gstreamer-backend"))]
fn start_webrtc_peer_feeder(
    _state: MediaState,
    _peer_id: u64,
    _feed: Arc<FeedRuntime>,
    _peer: Str0mWebRTCPeer,
) {
}

fn earlier_instant(left: Instant, right: Instant) -> Instant {
    if left <= right {
        left
    } else {
        right
    }
}

fn webrtc_peer_stale_deadline(
    started_at: Instant,
    last_network_at: Instant,
    connected: bool,
) -> Instant {
    if connected {
        last_network_at
            .checked_add(WEBRTC_CONNECTED_IDLE_TIMEOUT)
            .unwrap_or_else(Instant::now)
    } else {
        started_at
            .checked_add(WEBRTC_CONNECT_TIMEOUT)
            .unwrap_or_else(Instant::now)
    }
}

#[cfg(feature = "gstreamer-backend")]
async fn drain_str0m_outputs(
    rtc: &mut str0m::Rtc,
    socket: &UdpSocket,
    audio_mid: &mut Option<str0m::media::Mid>,
    connected: &mut bool,
) -> Result<Option<Instant>> {
    loop {
        match rtc.poll_output().context("failed to poll str0m output")? {
            str0m::Output::Timeout(timeout) => return Ok(Some(timeout)),
            str0m::Output::Transmit(transmit) => {
                socket
                    .send_to(&transmit.contents, transmit.destination)
                    .await
                    .context("failed to send str0m WebRTC datagram")?;
            }
            str0m::Output::Event(event) => match event {
                str0m::Event::Connected => {
                    *connected = true;
                }
                str0m::Event::Closed => return Ok(None),
                str0m::Event::IceConnectionStateChange(state) => {
                    if matches!(state, str0m::IceConnectionState::Disconnected) {
                        return Ok(None);
                    }
                }
                str0m::Event::MediaAdded(media) => {
                    if media.kind == str0m::media::MediaKind::Audio {
                        *audio_mid = Some(media.mid);
                    }
                }
                _ => {}
            },
        }
    }
}

#[cfg(feature = "gstreamer-backend")]
fn write_str0m_audio_frame(
    rtc: &mut str0m::Rtc,
    mid: str0m::media::Mid,
    codec: WebRTCAudioCodec,
    rtp_time: u64,
    data: Arc<[u8]>,
) -> Result<()> {
    let target = match codec {
        WebRTCAudioCodec::Opus => str0m::format::Codec::Opus,
        WebRTCAudioCodec::Pcmu => str0m::format::Codec::PCMU,
        WebRTCAudioCodec::Pcma => str0m::format::Codec::PCMA,
        WebRTCAudioCodec::G722 => bail!("str0m WebRTC does not expose G.722 packet writing"),
    };
    let writer = rtc
        .writer(mid)
        .context("str0m WebRTC media writer is unavailable")?;
    let Some(params) = writer
        .payload_params()
        .find(|params| params.spec().codec == target)
        .copied()
    else {
        bail!("str0m WebRTC writer did not negotiate {}", codec.id());
    };
    writer
        .write(
            params.pt(),
            Instant::now(),
            str0m::media::MediaTime::new(rtp_time, params.spec().clock_rate),
            data,
        )
        .context("failed to write str0m WebRTC media frame")
}

#[cfg(feature = "gstreamer-backend")]
fn str0m_audio_frame_ticks(codec: WebRTCAudioCodec) -> u64 {
    match codec {
        WebRTCAudioCodec::Opus => 960,
        WebRTCAudioCodec::G722 | WebRTCAudioCodec::Pcmu | WebRTCAudioCodec::Pcma => 160,
    }
}

fn select_webrtc_audio_codec(
    sdp: &str,
    available_codecs: &[WebRTCAudioCodec],
    preferred_codec: &str,
    require_opus: bool,
    disable_g722: bool,
) -> Option<WebRTCCodecSelection> {
    let mut candidates = Vec::new();
    let mut push_candidate = |codec: WebRTCAudioCodec| {
        if !candidates.contains(&codec) {
            candidates.push(codec);
        }
    };
    if let Some(codec) = parse_webrtc_audio_codec(preferred_codec) {
        push_candidate(codec);
    }
    if require_opus {
        push_candidate(WebRTCAudioCodec::Opus);
    } else {
        if let Some(codec) =
            parse_webrtc_audio_codec(&env::var("HAZE_WEBRTC_DEFAULT_CODEC").unwrap_or_default())
        {
            push_candidate(codec);
        } else {
            push_candidate(WebRTCAudioCodec::Opus);
        }
        if !disable_g722 {
            push_candidate(WebRTCAudioCodec::G722);
        }
        push_candidate(WebRTCAudioCodec::Pcmu);
        push_candidate(WebRTCAudioCodec::Pcma);
        push_candidate(WebRTCAudioCodec::Opus);
    }
    for codec in candidates {
        if disable_g722 && codec == WebRTCAudioCodec::G722 {
            continue;
        }
        if !available_codecs.contains(&codec) {
            continue;
        }
        if let Some(payload_type) = offered_audio_payload_type(sdp, codec) {
            return Some(WebRTCCodecSelection {
                codec,
                payload_type,
            });
        }
    }
    None
}

fn parse_webrtc_audio_codec(value: &str) -> Option<WebRTCAudioCodec> {
    match value
        .trim()
        .to_ascii_lowercase()
        .replace(['.', '-', '_'], "")
        .as_str()
    {
        "opus" => Some(WebRTCAudioCodec::Opus),
        "g722" => Some(WebRTCAudioCodec::G722),
        "pcmu" | "mulaw" | "ulaw" => Some(WebRTCAudioCodec::Pcmu),
        "pcma" | "alaw" => Some(WebRTCAudioCodec::Pcma),
        _ => None,
    }
}

fn offered_audio_payload_type(sdp: &str, target_codec: WebRTCAudioCodec) -> Option<u8> {
    for line in sdp.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("a=rtpmap:") else {
            continue;
        };
        let Some((payload, codec)) = rest.split_once(' ') else {
            continue;
        };
        if codec
            .to_ascii_uppercase()
            .starts_with(target_codec.rtpmap_match())
        {
            if let Ok(payload_type) = payload.parse::<u8>() {
                return Some(payload_type);
            }
        }
    }
    for payload_type in offered_media_payload_types(sdp) {
        if target_codec.is_static_payload(payload_type) {
            return Some(payload_type);
        }
    }
    None
}

fn offered_media_payload_types(sdp: &str) -> Vec<u8> {
    let mut payload_types = Vec::new();
    for line in sdp.lines() {
        let line = line.trim();
        if !line.to_ascii_lowercase().starts_with("m=audio ") {
            continue;
        }
        for part in line.split_whitespace().skip(3) {
            if let Ok(payload_type) = part.parse::<u8>() {
                payload_types.push(payload_type);
            }
        }
    }
    payload_types
}

fn ensure_str0m_answer_codec_lines(
    answer_sdp: &str,
    offer_sdp: &str,
    selection: WebRTCCodecSelection,
) -> String {
    let answer_attrs = answer_audio_attribute_lines(answer_sdp);
    let attrs = offer_audio_answer_attribute_lines(offer_sdp, selection.payload_type)
        .into_iter()
        .filter(|attr| {
            !answer_attrs
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(attr))
        })
        .collect::<Vec<_>>();
    let offer_bundle_group = offer_bundle_group_line(offer_sdp);
    let mut out = Vec::new();
    let mut patched_m_line = false;
    let mut patched_bundle_group = false;
    let mut pending_attrs = if attrs.is_empty() { None } else { Some(attrs) };
    let mut in_audio = false;
    for line in answer_sdp.replace("\r\n", "\n").lines() {
        let trimmed = line.trim();
        let lower = trimmed.to_ascii_lowercase();
        if lower == "a=group:bundle" {
            if let Some(group) = &offer_bundle_group {
                out.push(group.clone());
                patched_bundle_group = true;
                continue;
            }
        }
        if lower.starts_with("a=group:bundle ") {
            patched_bundle_group = true;
        }
        if lower.starts_with("m=") {
            if pending_attrs.is_some() && in_audio {
                out.extend(pending_attrs.take().unwrap());
            }
            in_audio = lower.starts_with("m=audio ");
        }
        if lower.starts_with("m=audio ") {
            out.push(accepted_audio_m_line(trimmed, selection.payload_type));
            patched_m_line = true;
            continue;
        }
        if in_audio && pending_attrs.is_some() && lower.starts_with("a=") {
            out.extend(pending_attrs.take().unwrap());
        }
        if in_audio && is_unselected_audio_payload_attr(trimmed, selection.payload_type) {
            continue;
        }
        out.push(line.to_string());
        if in_audio && pending_attrs.is_some() && lower.starts_with("c=") {
            out.extend(pending_attrs.take().unwrap());
        }
    }
    if pending_attrs.is_some() && in_audio {
        out.extend(pending_attrs.take().unwrap());
    }
    if !patched_bundle_group {
        if let Some(group) = offer_bundle_group {
            let insert_at = out
                .iter()
                .position(|line| line.trim().to_ascii_lowercase().starts_with("m="))
                .unwrap_or(out.len());
            out.insert(insert_at, group);
        }
    }
    if !patched_m_line {
        return answer_sdp.to_string();
    }
    let mut patched = out.join("\r\n");
    if answer_sdp.ends_with('\n') {
        patched.push_str("\r\n");
    }
    patched
}

fn is_unselected_audio_payload_attr(line: &str, selected_payload_type: u8) -> bool {
    audio_payload_attr_type(line)
        .map(|payload_type| payload_type != selected_payload_type)
        .unwrap_or(false)
}

fn audio_payload_attr_type(line: &str) -> Option<u8> {
    let lower = line.trim().to_ascii_lowercase();
    for prefix in ["a=rtpmap:", "a=fmtp:", "a=rtcp-fb:"] {
        if let Some(rest) = lower.strip_prefix(prefix) {
            let payload = rest
                .split(|ch: char| ch.is_ascii_whitespace())
                .next()
                .unwrap_or("");
            if let Ok(payload_type) = payload.parse::<u8>() {
                return Some(payload_type);
            }
        }
    }
    None
}

fn accepted_audio_m_line(line: &str, payload_type: u8) -> String {
    let mut parts = line.split_whitespace();
    let media = parts.next().unwrap_or("m=audio");
    let _port = parts.next();
    let proto = parts.next().unwrap_or("UDP/TLS/RTP/SAVPF");
    format!("{media} 9 {proto} {payload_type}")
}

fn offer_audio_answer_attribute_lines(offer_sdp: &str, payload_type: u8) -> Vec<String> {
    let payload_prefixes = [
        format!("a=rtpmap:{payload_type} "),
        format!("a=fmtp:{payload_type} "),
        format!("a=rtcp-fb:{payload_type} "),
    ];
    let media_attrs = ["a=rtcp-mux", "a=rtcp-rsize"];
    let mut in_audio = false;
    let mut payload_attrs = Vec::new();
    let mut transport_attrs = Vec::new();
    for line in offer_sdp.replace("\r\n", "\n").lines() {
        let line = line.trim();
        if line.to_ascii_lowercase().starts_with("m=") {
            in_audio = line.to_ascii_lowercase().starts_with("m=audio ");
            continue;
        }
        if !in_audio {
            continue;
        }
        if payload_prefixes
            .iter()
            .any(|prefix| line.starts_with(prefix))
        {
            payload_attrs.push(line.to_string());
        } else if media_attrs
            .iter()
            .any(|attr| line.eq_ignore_ascii_case(attr))
        {
            transport_attrs.push(line.to_string());
        }
    }
    let mut attrs = payload_attrs;
    attrs.extend(transport_attrs);
    attrs
}

fn offer_bundle_group_line(offer_sdp: &str) -> Option<String> {
    for line in offer_sdp.replace("\r\n", "\n").lines() {
        let line = line.trim();
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("a=group:bundle ") && line.split_whitespace().count() > 1 {
            return Some(line.to_string());
        }
    }
    None
}

fn answer_audio_attribute_lines(answer_sdp: &str) -> Vec<String> {
    let mut in_audio = false;
    let mut attrs = Vec::new();
    for line in answer_sdp.replace("\r\n", "\n").lines() {
        let line = line.trim();
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("m=") {
            in_audio = lower.starts_with("m=audio ");
            continue;
        }
        if in_audio && lower.starts_with("a=") {
            attrs.push(line.to_string());
        }
    }
    attrs
}

#[allow(dead_code)]
fn offered_opus_payload_type(sdp: &str) -> Option<u8> {
    offered_audio_payload_type(sdp, WebRTCAudioCodec::Opus)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum HttpAudioKind {
    Wav,
    Raw,
    GStreamer,
    Unsupported,
}

#[derive(Debug, Clone)]
struct HttpAudioFormat {
    kind: HttpAudioKind,
    #[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
    id: &'static str,
    #[cfg_attr(not(feature = "gstreamer-backend"), allow(dead_code))]
    content_type: &'static str,
}

impl HttpAudioFormat {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "" | "pcm16" | "wav" | "wav_pcm16" | "pcm_s16le" => Self {
                kind: HttpAudioKind::Wav,
                id: "pcm16",
                content_type: "audio/wav",
            },
            "raw" | "raw_pcm16" | "s16le" => Self {
                kind: HttpAudioKind::Raw,
                id: "raw",
                content_type: "audio/L16; rate=48000; channels=1",
            },
            "opus" | "ogg_opus" | "opus_ogg" => Self {
                kind: HttpAudioKind::GStreamer,
                id: "opus",
                content_type: "audio/ogg; codecs=opus",
            },
            "aac" | "adts" => Self {
                kind: HttpAudioKind::GStreamer,
                id: "aac",
                content_type: "audio/aac",
            },
            _ => Self {
                kind: HttpAudioKind::Unsupported,
                id: "unsupported",
                content_type: "application/octet-stream",
            },
        }
    }
}

async fn stream_wav(
    stream: &mut TcpStream,
    state: &MediaState,
    client_id: u64,
    feed: Arc<FeedRuntime>,
) -> Result<()> {
    write_stream_headers(stream, "audio/wav").await?;
    stream.write_all(&wav_stream_header()).await?;
    state.record_http_client_write(client_id, 44);
    let silence = vec![0u8; FRAME_BYTES];
    stream.write_all(&silence).await?;
    state.record_http_client_write(client_id, silence.len());
    let mut rx = feed.subscribe();
    loop {
        match rx.recv().await {
            Ok(frame) => {
                stream.write_all(&frame.data).await?;
                state.record_http_client_write(client_id, frame.data.len());
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                state.record_http_client_lag(client_id, skipped);
                let fill_frames = skipped.min(RAW_HTTP_LAG_RESYNC_SILENCE_FRAMES);
                for _ in 0..fill_frames {
                    stream.write_all(&silence).await?;
                    state.record_http_client_write(client_id, silence.len());
                }
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return Ok(()),
        }
    }
}

async fn stream_raw_pcm(
    stream: &mut TcpStream,
    state: &MediaState,
    client_id: u64,
    feed: Arc<FeedRuntime>,
) -> Result<()> {
    write_stream_headers(stream, "audio/L16; rate=48000; channels=1").await?;
    let silence = vec![0u8; FRAME_BYTES];
    stream.write_all(&silence).await?;
    state.record_http_client_write(client_id, silence.len());
    let mut rx = feed.subscribe();
    loop {
        match rx.recv().await {
            Ok(frame) => {
                stream.write_all(&frame.data).await?;
                state.record_http_client_write(client_id, frame.data.len());
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                state.record_http_client_lag(client_id, skipped);
                let fill_frames = skipped.min(RAW_HTTP_LAG_RESYNC_SILENCE_FRAMES);
                for _ in 0..fill_frames {
                    stream.write_all(&silence).await?;
                    state.record_http_client_write(client_id, silence.len());
                }
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return Ok(()),
        }
    }
}

#[cfg(feature = "gstreamer-backend")]
async fn stream_gstreamer_encoded(
    stream: &mut TcpStream,
    state: &MediaState,
    client_id: u64,
    feed: Arc<FeedRuntime>,
    format: HttpAudioFormat,
) -> Result<()> {
    use gst::prelude::*;
    use gstreamer as gst;
    use gstreamer_app as gst_app;

    let setup = (|| -> Result<(gst::Pipeline, gst_app::AppSrc, gst_app::AppSink)> {
        let pipeline_description = gstreamer_audio_pipeline(format.id)?;
        let element = gst::parse::launch(&pipeline_description)
            .with_context(|| format!("failed to build {} audio pipeline", format.id))?;
        let pipeline = element.downcast::<gst::Pipeline>().map_err(|_| {
            anyhow::anyhow!("GStreamer audio description did not produce a pipeline")
        })?;
        let appsrc = pipeline
            .by_name("src")
            .context("GStreamer audio pipeline is missing appsrc")?
            .downcast::<gst_app::AppSrc>()
            .map_err(|_| anyhow::anyhow!("GStreamer src is not appsrc"))?;
        let appsink = pipeline
            .by_name("sink")
            .context("GStreamer audio pipeline is missing appsink")?
            .downcast::<gst_app::AppSink>()
            .map_err(|_| anyhow::anyhow!("GStreamer sink is not appsink"))?;
        Ok((pipeline, appsrc, appsink))
    })();
    let (pipeline, appsrc, appsink) = match setup {
        Ok(parts) => parts,
        Err(err) => {
            warn!(
                "failed to prepare GStreamer {} audio stream: {err:#}",
                format.id
            );
            write_text_response(
                stream,
                503,
                &format!("gstreamer audio pipeline is unavailable: {err:#}\n"),
            )
            .await?;
            return Ok(());
        }
    };
    let (encoded_tx, mut encoded_rx) = mpsc::channel::<Vec<u8>>(ENCODED_HTTP_QUEUE_CAPACITY);
    let callback_state = state.clone();
    appsink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |sink| {
                let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                let Some(buffer) = sample.buffer() else {
                    return Err(gst::FlowError::Error);
                };
                let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                let data = map.as_slice().to_vec();
                match encoded_tx.try_send(data) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        callback_state.record_http_client_encoded_drop(client_id);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err(gst::FlowError::Eos);
                    }
                }
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );
    if let Err(err) = pipeline.set_state(gst::State::Playing) {
        warn!(
            "failed to start GStreamer {} audio stream: {err:#}",
            format.id
        );
        write_text_response(
            stream,
            503,
            &format!("gstreamer audio pipeline failed to start: {err:#}\n"),
        )
        .await?;
        let _ = pipeline.set_state(gst::State::Null);
        return Ok(());
    }
    let mut rx = feed.subscribe();
    let frame_duration_ns = FRAME_DURATION.as_nanos() as u64;
    let mut next_pts_ns: Option<u64> = None;
    let silence = vec![0u8; FRAME_BYTES];
    let pts_ns = next_gst_audio_pts(&mut next_pts_ns, frame_duration_ns);
    let buffer = build_gst_audio_buffer(&silence, pts_ns, frame_duration_ns)?;
    if let Err(err) = appsrc.push_buffer(buffer) {
        let _ = pipeline.set_state(gst::State::Null);
        bail!(
            "failed to prime GStreamer {} audio stream: {err:?}",
            format.id
        );
    }
    write_stream_headers(stream, format.content_type).await?;

    'stream: loop {
        tokio::select! {
            frame = rx.recv() => {
                match frame {
                    Ok(frame) => {
                        let pts_ns = next_gst_audio_pts(&mut next_pts_ns, frame_duration_ns);
                        let buffer = build_gst_audio_buffer(&frame.data, pts_ns, frame_duration_ns)?;
                        if appsrc.push_buffer(buffer).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        state.record_http_client_lag(client_id, skipped);
                        let fill_frames = skipped.min(RAW_HTTP_LAG_RESYNC_SILENCE_FRAMES);
                        for _ in 0..fill_frames {
                            let pts_ns = next_gst_audio_pts(&mut next_pts_ns, frame_duration_ns);
                            let buffer = build_gst_audio_buffer(&silence, pts_ns, frame_duration_ns)?;
                            if appsrc.push_buffer(buffer).is_err() {
                                break 'stream;
                            }
                        }
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            encoded = encoded_rx.recv() => {
                let Some(encoded) = encoded else {
                    break;
                };
                if let Err(err) = stream.write_all(&encoded).await {
                    if err.kind() != ErrorKind::BrokenPipe && err.kind() != ErrorKind::ConnectionReset {
                        warn!("haze-media encoded HTTP write failed: {err}");
                    }
                    break;
                }
                state.record_http_client_write(client_id, encoded.len());
            }
        }
    }
    let _ = appsrc.end_of_stream();
    let _ = pipeline.set_state(gst::State::Null);
    Ok(())
}

#[cfg(not(feature = "gstreamer-backend"))]
async fn stream_gstreamer_encoded(
    stream: &mut TcpStream,
    _state: &MediaState,
    _client_id: u64,
    _feed: Arc<FeedRuntime>,
    _format: HttpAudioFormat,
) -> Result<()> {
    write_text_response(
        stream,
        503,
        "haze-media was built without GStreamer support\n",
    )
    .await?;
    Ok(())
}

#[cfg(feature = "gstreamer-backend")]
fn gstreamer_audio_pipeline(codec: &str) -> Result<String> {
    let source = format!(
        "appsrc name=src is-live=true block=false leaky-type=downstream do-timestamp=false max-bytes={} format=time stream-type=stream caps=audio/x-raw,format=S16LE,layout=interleaved,rate=48000,channels=1",
        FRAME_BYTES * HTTP_AUDIO_GSTREAMER_QUEUE_BUFFERS as usize
    );
    let common = format!(
        "queue max-size-time={} max-size-buffers={} max-size-bytes=0 leaky=downstream ! audioconvert ! audioresample quality=4",
        HTTP_AUDIO_GSTREAMER_QUEUE_MS * 1_000_000,
        HTTP_AUDIO_GSTREAMER_QUEUE_BUFFERS
    );
    match codec {
        "opus" => Ok(format!(
            "{source} ! {common} ! opusenc bitrate={OPUS_BITRATE_BPS} frame-size=20 inband-fec=true ! oggmux max-delay=20000000 max-page-delay=20000000 ! appsink name=sink emit-signals=true sync=false max-buffers={ENCODED_HTTP_QUEUE_CAPACITY} drop=true wait-on-eos=false"
        )),
        "aac" => Ok(format!(
            "{source} ! {common} ! avenc_aac bitrate=96000 ! aacparse ! audio/mpeg,mpegversion=4,stream-format=adts ! appsink name=sink emit-signals=true sync=false max-buffers={ENCODED_HTTP_QUEUE_CAPACITY} drop=true wait-on-eos=false"
        )),
        _ => bail!("unsupported GStreamer audio codec {codec}"),
    }
}

fn start_icecast_output(
    runtime: Arc<FeedRuntime>,
    config: IcecastOutput,
    gstreamer_available: bool,
) {
    #[cfg(feature = "gstreamer-backend")]
    {
        if !gstreamer_available {
            warn!(
                feed_id = %runtime.feed_id,
                mount = %config.mount,
                "Icecast output requires GStreamer and will not start"
            );
            return;
        }
        tokio::spawn(run_icecast_output(runtime, config));
    }
    #[cfg(not(feature = "gstreamer-backend"))]
    {
        let _ = runtime;
        let _ = config;
        let _ = gstreamer_available;
        warn!("Icecast output requires a haze-media build with GStreamer support");
    }
}

#[cfg(feature = "gstreamer-backend")]
async fn run_icecast_output(runtime: Arc<FeedRuntime>, config: IcecastOutput) {
    use gst::prelude::*;
    use gstreamer as gst;
    use gstreamer_app as gst_app;

    let feed_id = runtime.feed_id.clone();
    sleep(icecast_initial_delay(&feed_id, &config.mount)).await;
    loop {
        match build_icecast_encoder_pipeline(&config, &feed_id) {
            Ok((pipeline, appsrc, appsink)) => {
                let (encoded_tx, mut encoded_rx) =
                    mpsc::channel::<Vec<u8>>(ENCODED_ICECAST_QUEUE_CAPACITY);
                let encoder_backpressured = Arc::new(AtomicBool::new(false));
                let callback_encoder_backpressured = Arc::clone(&encoder_backpressured);
                appsink.set_callbacks(
                    gst_app::AppSinkCallbacks::builder()
                        .new_sample(move |sink| {
                            let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                            let Some(buffer) = sample.buffer() else {
                                return Err(gst::FlowError::Error);
                            };
                            let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                            match encoded_tx.try_send(map.as_slice().to_vec()) {
                                Ok(()) => Ok(gst::FlowSuccess::Ok),
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    callback_encoder_backpressured.store(true, Ordering::Relaxed);
                                    Ok(gst::FlowSuccess::Ok)
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    Err(gst::FlowError::Eos)
                                }
                            }
                        })
                        .build(),
                );
                if let Err(err) = pipeline.set_state(gst::State::Playing) {
                    warn!(
                        feed_id = %feed_id,
                        mount = %config.mount,
                        "failed to start Icecast output: {err:?}"
                    );
                    let _ = pipeline.set_state(gst::State::Null);
                    sleep(icecast_retry_delay(&feed_id, &config.mount)).await;
                    continue;
                }
                let mut source = match open_icecast_source_stream(&config, &feed_id).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        warn!(
                            feed_id = %feed_id,
                            mount = %config.mount,
                            "failed to connect Icecast output: {err:#}"
                        );
                        let _ = appsrc.end_of_stream();
                        let _ = pipeline.set_state(gst::State::Null);
                        sleep(icecast_retry_delay(&feed_id, &config.mount)).await;
                        continue;
                    }
                };
                info!(
                    feed_id = %feed_id,
                    host = %config.host,
                    port = config.port,
                    mount = %config.mount,
                    "Icecast output connected"
                );
                let mut rx = runtime.subscribe();
                let mut next_pts_ns: Option<u64> = None;
                let frame_duration_ns = FRAME_DURATION.as_nanos() as u64;
                let silence = vec![0u8; FRAME_BYTES];
                loop {
                    if encoder_backpressured.swap(false, Ordering::Relaxed) {
                        warn!(
                            feed_id = %feed_id,
                            mount = %config.mount,
                            "Icecast encoded queue filled; reconnecting to drop stale stream backlog"
                        );
                        break;
                    }
                    tokio::select! {
                        frame = rx.recv() => {
                            match frame {
                                Ok(frame) => {
                                    let pts_ns =
                                        next_gst_audio_pts(&mut next_pts_ns, frame_duration_ns);
                                    match build_gst_audio_buffer(&frame.data, pts_ns, frame_duration_ns)
                                        .and_then(|buffer| {
                                            appsrc.push_buffer(buffer).map(|_| ()).map_err(|_| {
                                                anyhow::anyhow!("Icecast appsrc rejected audio buffer")
                                            })
                                        }) {
                                        Ok(()) => {}
                                        Err(err) => {
                                            warn!(
                                                feed_id = %feed_id,
                                                mount = %config.mount,
                                                "Icecast output stopped: {err:#}"
                                            );
                                            break;
                                        }
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                    warn!(
                                        feed_id = %feed_id,
                                        skipped,
                                        mount = %config.mount,
                                        "Icecast output lagged; inserting one silence frame"
                                    );
                                    let pts_ns =
                                        next_gst_audio_pts(&mut next_pts_ns, frame_duration_ns);
                                    if let Ok(buffer) =
                                        build_gst_audio_buffer(&silence, pts_ns, frame_duration_ns)
                                    {
                                        if appsrc.push_buffer(buffer).is_err() {
                                            break;
                                        }
                                    }
                                }
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                        encoded = encoded_rx.recv() => {
                            let Some(encoded) = encoded else {
                                warn!(
                                    feed_id = %feed_id,
                                    mount = %config.mount,
                                    "Icecast encoder stopped producing data"
                                );
                                break;
                            };
                            if let Err(err) = source.write_all(&encoded).await {
                                if err.kind() != ErrorKind::BrokenPipe
                                    && err.kind() != ErrorKind::ConnectionReset
                                {
                                    warn!(
                                        feed_id = %feed_id,
                                        mount = %config.mount,
                                        "Icecast source write failed: {err}"
                                    );
                                }
                                break;
                            }
                        }
                    }
                }
                let _ = appsrc.end_of_stream();
                let _ = pipeline.set_state(gst::State::Null);
            }
            Err(err) => {
                warn!(
                    feed_id = %feed_id,
                    mount = %config.mount,
                    "failed to build Icecast output: {err:#}"
                );
            }
        }
        sleep(icecast_retry_delay(&feed_id, &config.mount)).await;
    }
}

#[cfg(feature = "gstreamer-backend")]
fn build_icecast_encoder_pipeline(
    config: &IcecastOutput,
    feed_id: &str,
) -> Result<(
    gstreamer::Pipeline,
    gstreamer_app::AppSrc,
    gstreamer_app::AppSink,
)> {
    use gst::prelude::*;
    use gstreamer as gst;
    use gstreamer_app as gst_app;

    let bitrate = config.bitrate_kbps.saturating_mul(1_000).max(16_000);
    let format_hint = config.format.trim().to_ascii_lowercase();
    let codec_hint = config.acodec.trim().to_ascii_lowercase();
    let encoder = match (format_hint.as_str(), codec_hint.as_str()) {
        ("opus" | "ogg" | "oga", "" | "opus" | "libopus" | "opusenc") => {
            format!("opusenc bitrate={bitrate} frame-size=20 inband-fec=true ! oggmux")
        }
        (other, codec) => bail!("unsupported Icecast stream format {other} with codec {codec}"),
    };
    let pipeline_description = format!(
        "appsrc name=src is-live=true block=false do-timestamp=false max-bytes=38400 format=time stream-type=stream \
         caps=audio/x-raw,format=S16LE,layout=interleaved,rate=48000,channels=1 \
         ! queue max-size-time=1000000000 max-size-buffers=100 max-size-bytes=0 leaky=downstream \
         ! audioconvert ! audioresample quality=4 \
         ! {encoder} \
         ! appsink name=sink emit-signals=true sync=false"
    );
    let element = gst::parse::launch(&pipeline_description)
        .context("failed to build Icecast encoder GStreamer pipeline")?;
    let pipeline = element
        .downcast::<gst::Pipeline>()
        .map_err(|_| anyhow::anyhow!("Icecast encoder description did not produce a pipeline"))?;
    let appsrc = pipeline
        .by_name("src")
        .context("Icecast encoder pipeline is missing appsrc")?
        .downcast::<gst_app::AppSrc>()
        .map_err(|_| anyhow::anyhow!("Icecast encoder src is not appsrc"))?;
    let appsink = pipeline
        .by_name("sink")
        .context("Icecast encoder pipeline is missing appsink")?
        .downcast::<gst_app::AppSink>()
        .map_err(|_| anyhow::anyhow!("Icecast encoder sink is not appsink"))?;
    let _ = feed_id;
    Ok((pipeline, appsrc, appsink))
}

#[cfg(feature = "gstreamer-backend")]
async fn open_icecast_source_stream(config: &IcecastOutput, feed_id: &str) -> Result<TcpStream> {
    if config.ssl {
        bail!("native Icecast source output only supports plain HTTP for now");
    }
    let host_port = format!("{}:{}", config.host, config.port);
    let mut stream = tokio::time::timeout(ICECAST_CONNECT_TIMEOUT, TcpStream::connect(&host_port))
        .await
        .context("Icecast connect timed out")?
        .with_context(|| format!("failed to connect to Icecast at {host_port}"))?;
    let mount = if config.mount.starts_with('/') {
        config.mount.clone()
    } else {
        format!("/{}", config.mount)
    };
    let auth = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{}", config.username, config.password));
    let request = format!(
        "PUT {mount} HTTP/1.1\r\n\
         Host: {host_port}\r\n\
         Authorization: Basic {auth}\r\n\
         User-Agent: Haze Media\r\n\
         Content-Type: {}\r\n\
         Ice-Name: Haze {feed_id}\r\n\
         Ice-Description: Haze Weather Radio {feed_id}\r\n\
         Ice-Genre: weather\r\n\
         Ice-Public: 1\r\n\
         Connection: close\r\n\
         \r\n",
        icecast_content_type(config)?,
    );
    stream
        .write_all(request.as_bytes())
        .await
        .context("failed to send Icecast source request")?;
    let response = tokio::time::timeout(
        ICECAST_CONNECT_TIMEOUT,
        read_icecast_source_response(&mut stream),
    )
    .await
    .context("Icecast source response timed out")??;
    let mut accepted = false;
    for line in response.lines() {
        if let Some(status) = line.strip_prefix("HTTP/") {
            accepted = status.contains(" 200 ") || status.contains(" 100 ");
            break;
        }
    }
    if !accepted {
        let first_line = response.lines().next().unwrap_or("empty response");
        bail!("Icecast rejected source connection: {first_line}");
    }
    Ok(stream)
}

#[cfg(feature = "gstreamer-backend")]
async fn read_icecast_source_response(stream: &mut TcpStream) -> Result<String> {
    let mut response = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream
            .read(&mut byte)
            .await
            .context("failed to read Icecast source response")?;
        if n == 0 {
            break;
        }
        response.push(byte[0]);
        if response.ends_with(b"\r\n\r\n") {
            break;
        }
        if response.len() > ICECAST_RESPONSE_LIMIT {
            bail!("Icecast source response exceeded limit");
        }
    }
    String::from_utf8(response).context("Icecast source response was not UTF-8")
}

#[cfg(feature = "gstreamer-backend")]
fn icecast_content_type(config: &IcecastOutput) -> Result<&'static str> {
    let format_hint = config.format.trim().to_ascii_lowercase();
    let codec_hint = config.acodec.trim().to_ascii_lowercase();
    match (format_hint.as_str(), codec_hint.as_str()) {
        ("opus" | "ogg" | "oga", "" | "opus" | "libopus" | "opusenc") => Ok("application/ogg"),
        (other, codec) => bail!("unsupported Icecast stream format {other} with codec {codec}"),
    }
}

#[cfg(feature = "gstreamer-backend")]
fn icecast_initial_delay(feed_id: &str, mount: &str) -> Duration {
    Duration::from_millis(250 + stable_jitter_ms(feed_id, mount, 2_000))
}

#[cfg(feature = "gstreamer-backend")]
fn icecast_retry_delay(feed_id: &str, mount: &str) -> Duration {
    ICECAST_RETRY_BASE + Duration::from_millis(stable_jitter_ms(feed_id, mount, 4_000))
}

#[cfg(feature = "gstreamer-backend")]
fn stable_jitter_ms(feed_id: &str, mount: &str, span_ms: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    feed_id.hash(&mut hasher);
    mount.hash(&mut hasher);
    hasher.finish() % span_ms.max(1)
}

async fn write_stream_headers(stream: &mut TcpStream, content_type: &str) -> Result<()> {
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nCache-Control: no-store\r\nConnection: close\r\nX-Accel-Buffering: no\r\n\r\n"
    );
    stream.write_all(headers.as_bytes()).await?;
    Ok(())
}

async fn write_json_response(stream: &mut TcpStream, status: u16, value: Value) -> Result<()> {
    let body = serde_json::to_vec(&value)?;
    let reason = status_reason(status);
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(&body).await?;
    Ok(())
}

async fn write_text_response(stream: &mut TcpStream, status: u16, body: &str) -> Result<()> {
    let reason = status_reason(status);
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    Ok(())
}

fn wav_stream_header() -> [u8; 44] {
    let byte_rate = SAMPLE_RATE * u32::from(CHANNELS) * 16 / 8;
    let block_align = CHANNELS * 16 / 8;
    let mut header = [0u8; 44];
    header[0..4].copy_from_slice(b"RIFF");
    header[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    header[8..12].copy_from_slice(b"WAVE");
    header[12..16].copy_from_slice(b"fmt ");
    header[16..20].copy_from_slice(&16u32.to_le_bytes());
    header[20..22].copy_from_slice(&1u16.to_le_bytes());
    header[22..24].copy_from_slice(&CHANNELS.to_le_bytes());
    header[24..28].copy_from_slice(&SAMPLE_RATE.to_le_bytes());
    header[28..32].copy_from_slice(&byte_rate.to_le_bytes());
    header[32..34].copy_from_slice(&block_align.to_le_bytes());
    header[34..36].copy_from_slice(&16u16.to_le_bytes());
    header[36..40].copy_from_slice(b"data");
    header[40..44].copy_from_slice(&u32::MAX.to_le_bytes());
    header
}

fn status_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        415 => "Unsupported Media Type",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

fn load_root_config(path: &Path) -> Result<RootConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    serde_yaml::from_str(&expand_env_vars(&raw))
        .with_context(|| format!("failed to parse config {}", path.display()))
}

fn expand_env_vars(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '$' {
            out.push(ch);
            continue;
        }
        if chars.peek() == Some(&'{') {
            chars.next();
            let mut name = String::new();
            for next in chars.by_ref() {
                if next == '}' {
                    break;
                }
                name.push(next);
            }
            if name.is_empty() {
                continue;
            }
            out.push_str(&std::env::var(name).unwrap_or_default());
            continue;
        }
        out.push(ch);
    }
    out
}

fn load_outputs(path: &Path) -> Result<BTreeMap<String, OutputFeed>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read outputs XML {}", path.display()))?;
    let parsed: OutputsXml = quick_xml::de::from_str(&expand_env_vars(&raw))
        .with_context(|| format!("failed to parse outputs XML {}", path.display()))?;
    let mut exact = BTreeMap::new();
    let mut wildcard = Vec::new();
    for feed in parsed.feeds {
        let id = feed.id.trim();
        if id.is_empty() {
            continue;
        }
        let icecast_node = preferred_icecast_node(feed.icecast, feed.stream);
        let external = icecast_node.is_enabled()
            || feed.udp.is_enabled()
            || feed.rtp.is_enabled()
            || feed.rtmp.is_enabled()
            || feed.srt.is_enabled()
            || feed.rtsp.is_enabled()
            || feed.audio_device.is_enabled();
        let output = output_feed_from_node(id, icecast_node, feed.webrtc, external);
        if output_id_is_wildcard(id) {
            wildcard.push((id.to_string(), output));
        } else {
            exact.insert(id.to_string(), output);
        }
    }
    Ok(expand_output_wildcards(exact, wildcard))
}

fn output_feed_from_node(
    id: &str,
    icecast_node: OutputNodeXml,
    webrtc: OutputNodeXml,
    external: bool,
) -> OutputFeed {
    let icecast_config = icecast_output_from_node(&icecast_node);
    let icecast = icecast_config.is_some();
    let icecast_mount = icecast_config.as_ref().and_then(|stream| {
        if stream.mount.trim().is_empty() {
            None
        } else {
            Some(stream.mount.clone())
        }
    });
    OutputFeed {
        id: id.to_string(),
        webrtc: webrtc.is_enabled(),
        http: true,
        webrtc_ready: false,
        icecast,
        icecast_mount,
        icecast_config,
        external,
    }
}

fn expand_output_wildcards(
    exact: BTreeMap<String, OutputFeed>,
    wildcard: Vec<(String, OutputFeed)>,
) -> BTreeMap<String, OutputFeed> {
    let mut outputs = exact;
    for (pattern, output) in wildcard {
        outputs.entry(pattern).or_insert(output);
    }
    outputs
}

fn preferred_icecast_node(icecast: OutputNodeXml, stream: OutputNodeXml) -> OutputNodeXml {
    if icecast.is_configured() {
        icecast
    } else {
        stream
    }
}

fn icecast_output_from_node(node: &OutputNodeXml) -> Option<IcecastOutput> {
    if !node.is_enabled() {
        return None;
    }
    let kind = first_non_blank(&[Some(node.r#type.as_str()), Some("icecast")])
        .trim()
        .to_ascii_lowercase();
    if kind != "icecast" {
        return None;
    }
    let host = node.host.trim();
    let mount = node.mount.trim();
    let password = node.password.trim();
    if host.is_empty() || password.is_empty() {
        warn!("Icecast output is enabled but host or password is missing");
        return None;
    }
    Some(IcecastOutput {
        host: host.to_string(),
        port: node.port.trim().parse::<u16>().unwrap_or(8000),
        username: first_non_blank(&[Some(node.username.as_str()), Some("source")]),
        password: password.to_string(),
        mount: if mount.is_empty() {
            String::new()
        } else if mount.starts_with('/') {
            mount.to_string()
        } else {
            format!("/{mount}")
        },
        ssl: xml_bool(Some(node.ssl.as_str()), false),
        format: first_non_blank(&[Some(node.format.as_str()), Some("opus")]),
        acodec: first_non_blank(&[Some(node.acodec.as_str()), Some("libopus")]),
        bitrate_kbps: node.portable_bitrate_kbps(),
    })
}

fn output_id_is_wildcard(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?')
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    wildcard_match_inner(pattern.as_bytes(), value.as_bytes())
}

fn wildcard_match_inner(pattern: &[u8], value: &[u8]) -> bool {
    match pattern.split_first() {
        None => value.is_empty(),
        Some((&b'*', rest)) => {
            wildcard_match_inner(rest, value)
                || (!value.is_empty() && wildcard_match_inner(pattern, &value[1..]))
        }
        Some((&b'?', rest)) => !value.is_empty() && wildcard_match_inner(rest, &value[1..]),
        Some((&literal, rest)) => value
            .split_first()
            .is_some_and(|(&head, tail)| head == literal && wildcard_match_inner(rest, tail)),
    }
}

fn default_outputs_file() -> String {
    DEFAULT_OUTPUTS_FILE.to_string()
}

fn resolve_path(base_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

fn first_non_blank(values: &[Option<&str>]) -> String {
    for value in values.iter().flatten() {
        let value = value.trim();
        if !value.is_empty() {
            return value.to_string();
        }
    }
    String::new()
}

fn env_bool(name: &str, default: bool) -> bool {
    match env::var(name)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
    {
        Some(value) if matches!(value.as_str(), "1" | "true" | "yes" | "on" | "enabled") => true,
        Some(value) if matches!(value.as_str(), "0" | "false" | "no" | "off" | "disabled") => false,
        _ => default,
    }
}

fn env_f32(name: &str, default: f32) -> f32 {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<f32>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(default)
}

fn db_to_gain(db: f32) -> f32 {
    10.0_f32.powf(db / 20.0)
}

fn gain_to_db(gain: f32) -> f32 {
    20.0 * gain.max(1.0e-9).log10()
}

fn compressor_coeff_ms(ms: f32) -> f32 {
    let ms = ms.max(0.1);
    (-1.0 / ((SAMPLE_RATE as f32) * ms / 1_000.0)).exp()
}

fn query_value(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        if url_decode(raw_key) == key {
            return Some(url_decode(raw_value));
        }
    }
    None
}

fn url_decode(value: &str) -> String {
    let mut out = Vec::with_capacity(value.len());
    let mut bytes = value.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        match byte {
            b'+' => out.push(b' '),
            b'%' => {
                let hi = bytes.next();
                let lo = bytes.next();
                match (hi.and_then(hex_value), lo.and_then(hex_value)) {
                    (Some(hi), Some(lo)) => out.push((hi << 4) | lo),
                    _ => out.push(byte),
                }
            }
            _ => out.push(byte),
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn valid_feed_id(feed_id: &str) -> bool {
    let feed_id = feed_id.trim();
    !feed_id.is_empty()
        && feed_id.len() <= 128
        && feed_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ':'))
}

fn xml_bool(value: Option<&str>, default: bool) -> bool {
    match value.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some("true" | "yes" | "1" | "on" | "enabled") => true,
        Some("false" | "no" | "0" | "off" | "disabled") => false,
        _ => default,
    }
}

fn initialize_gstreamer(mode: BackendMode) -> Result<bool> {
    if mode == BackendMode::Legacy {
        return Ok(false);
    }
    initialize_gstreamer_inner().or_else(|err| {
        if mode == BackendMode::GStreamer {
            Err(err)
        } else {
            warn!("GStreamer backend unavailable; HTTP audio will use legacy direct PCM only: {err:#}");
            Ok(false)
        }
    })
}

fn detect_gstreamer_webrtc_codecs(gstreamer_available: bool) -> Vec<WebRTCAudioCodec> {
    if !gstreamer_available {
        return Vec::new();
    }
    detect_gstreamer_webrtc_codecs_inner()
}

#[cfg(feature = "gstreamer-backend")]
fn detect_gstreamer_webrtc_codecs_inner() -> Vec<WebRTCAudioCodec> {
    if let Err(err) = validate_gstreamer_webrtc_base_elements() {
        warn!("str0m WebRTC encoder backend unavailable: {err:#}");
        return Vec::new();
    }
    let mut codecs = Vec::new();
    for codec in [
        WebRTCAudioCodec::Opus,
        WebRTCAudioCodec::Pcmu,
        WebRTCAudioCodec::Pcma,
    ] {
        let missing = webrtc_codec_elements(codec)
            .iter()
            .copied()
            .filter(|name| gstreamer::ElementFactory::find(name).is_none())
            .collect::<Vec<_>>();
        if missing.is_empty() {
            codecs.push(codec);
        } else {
            warn!(
                codec = codec.id(),
                missing = missing.join(", "),
                "str0m WebRTC encoder codec is unavailable"
            );
        }
    }
    if codecs.is_empty() {
        warn!("str0m WebRTC encoder backend unavailable: no audio codec branches are available");
    }
    codecs
}

#[cfg(not(feature = "gstreamer-backend"))]
fn detect_gstreamer_webrtc_codecs_inner() -> Vec<WebRTCAudioCodec> {
    Vec::new()
}

#[cfg(feature = "gstreamer-backend")]
fn initialize_gstreamer_inner() -> Result<bool> {
    configure_portable_gstreamer_paths();
    gstreamer::init().context("failed to initialize GStreamer")?;
    validate_gstreamer_audio_elements()?;
    Ok(true)
}

#[cfg(feature = "gstreamer-backend")]
fn configure_portable_gstreamer_paths() {
    let Ok(exe_path) = env::current_exe() else {
        return;
    };
    let Some(exe_dir) = exe_path.parent() else {
        return;
    };
    let plugin_dir = exe_dir.join("gstreamer-1.0");
    if !plugin_dir.is_dir() {
        return;
    }
    prepend_env_path("GST_PLUGIN_PATH", &plugin_dir);
    prepend_env_path("GST_PLUGIN_SYSTEM_PATH_1_0", &plugin_dir);

    let scanner = plugin_dir.join(if cfg!(windows) {
        "gst-plugin-scanner.exe"
    } else {
        "gst-plugin-scanner"
    });
    if scanner.is_file() {
        env::set_var("GST_PLUGIN_SCANNER", scanner);
    }
}

#[cfg(feature = "gstreamer-backend")]
fn prepend_env_path(name: &str, path: &Path) {
    let mut paths = env::var_os(name)
        .map(|raw| env::split_paths(&raw).collect::<Vec<_>>())
        .unwrap_or_default();
    if paths.iter().any(|existing| existing == path) {
        return;
    }
    paths.insert(0, path.to_path_buf());
    match env::join_paths(paths) {
        Ok(joined) => env::set_var(name, joined),
        Err(err) => warn!("failed to set {name} for GStreamer runtime: {err}"),
    }
}

#[cfg(feature = "gstreamer-backend")]
fn validate_gstreamer_audio_elements() -> Result<()> {
    let required = [
        "appsrc",
        "appsink",
        "queue",
        "audioconvert",
        "audioresample",
        "opusenc",
        "oggmux",
    ];
    let missing = required
        .iter()
        .copied()
        .filter(|name| gstreamer::ElementFactory::find(name).is_none())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "GStreamer runtime is missing required audio element(s): {}",
            missing.join(", ")
        );
    }
    Ok(())
}

#[cfg(feature = "gstreamer-backend")]
fn validate_gstreamer_webrtc_base_elements() -> Result<()> {
    let required = [
        "appsrc",
        "appsink",
        "queue",
        "audioconvert",
        "audioresample",
    ];
    let missing = required
        .iter()
        .copied()
        .filter(|name| gstreamer::ElementFactory::find(name).is_none())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "GStreamer runtime is missing required WebRTC encoder element(s): {}",
            missing.join(", ")
        );
    }
    Ok(())
}

#[cfg(feature = "gstreamer-backend")]
fn webrtc_codec_elements(codec: WebRTCAudioCodec) -> &'static [&'static str] {
    match codec {
        WebRTCAudioCodec::Opus => &["opusenc"],
        WebRTCAudioCodec::G722 => &[],
        WebRTCAudioCodec::Pcmu => &["mulawenc"],
        WebRTCAudioCodec::Pcma => &["alawenc"],
    }
}

#[cfg(not(feature = "gstreamer-backend"))]
fn initialize_gstreamer_inner() -> Result<bool> {
    bail!("haze-media was built without the gstreamer-backend feature")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queued_samples<I>(values: I, bypass_loudness: bool) -> VecDeque<QueuedSample>
    where
        I: IntoIterator<Item = i16>,
    {
        values
            .into_iter()
            .map(|value| QueuedSample {
                value,
                bypass_loudness,
            })
            .collect()
    }

    #[test]
    fn decodes_playout_pcm_event() {
        let raw_pcm = vec![1u8, 0, 2, 0];
        let event = json!({
            "type": "playout.pcm",
            "feed_id": "sk-0001",
            "data": {
                "sample_rate": 48000,
                "channels": 1,
                "pcm": base64::engine::general_purpose::STANDARD.encode(&raw_pcm),
            }
        });
        let chunk = decode_pcm_event(serde_json::to_string(&event).unwrap().as_bytes()).unwrap();
        assert_eq!(chunk.feed_id, "sk-0001");
        assert_eq!(chunk.sample_rate, 48_000);
        assert_eq!(chunk.channels, 1);
        assert_eq!(chunk.data, raw_pcm);
        assert!(!chunk.bypass_loudness);
        assert_eq!(chunk.sequence, None);
        assert_eq!(chunk.pts_ns, None);
        assert!(!chunk.discontinuity);
    }

    #[test]
    fn decodes_playout_pcm_event_loudness_bypass() {
        let raw_pcm = vec![1u8, 0, 2, 0];
        let event = json!({
            "type": "playout.pcm",
            "feed_id": "sk-0001",
            "data": {
                "sample_rate": 48000,
                "channels": 1,
                "audio_processing": {
                    "bypass_loudness": true,
                },
                "pcm": base64::engine::general_purpose::STANDARD.encode(&raw_pcm),
            }
        });
        let chunk = decode_pcm_event(serde_json::to_string(&event).unwrap().as_bytes()).unwrap();

        assert!(chunk.bypass_loudness);
    }

    #[test]
    fn decodes_compact_pcm_frame() {
        let raw_pcm = [1u8, 0, 2, 0];
        let mut frame = Vec::new();
        haze_media_protocol::encode_pcm_frame(
            &mut frame,
            haze_media_protocol::PcmFrame {
                feed_id: "sk-0001",
                queue_id: Some("alert-1"),
                sample_rate: 48_000,
                channels: 1,
                duration_ms: 20,
                sequence: 7,
                pts_ns: 140_000_000,
                discontinuity: true,
                media_kind: haze_media_protocol::PcmMediaKind::Alert,
                pcm: &raw_pcm,
            },
        )
        .expect("encode PCM frame");

        let chunk = decode_binary_pcm_frame(&frame).expect("decode PCM frame");

        assert_eq!(chunk.feed_id, "sk-0001");
        assert_eq!(chunk.sample_rate, 48_000);
        assert_eq!(chunk.channels, 1);
        assert_eq!(chunk.data, raw_pcm);
        assert!(!chunk.bypass_loudness);
        assert_eq!(chunk.sequence, Some(7));
        assert_eq!(chunk.pts_ns, Some(140_000_000));
        assert!(chunk.discontinuity);
    }

    #[tokio::test]
    async fn reads_compact_pcm_frame_from_bridge() {
        let mut frame = Vec::new();
        haze_media_protocol::encode_pcm_frame(
            &mut frame,
            haze_media_protocol::PcmFrame {
                feed_id: "sk-0001",
                queue_id: None,
                sample_rate: 48_000,
                channels: 1,
                duration_ms: 20,
                sequence: 0,
                pts_ns: 0,
                discontinuity: false,
                media_kind: haze_media_protocol::PcmMediaKind::Routine,
                pcm: &[0, 0],
            },
        )
        .expect("encode PCM frame");
        let (mut writer, reader) = tokio::io::duplex(frame.len());
        writer.write_all(&frame).await.expect("write PCM frame");
        drop(writer);
        let mut reader = BufReader::new(reader);
        let mut payload = Vec::new();

        let is_binary = read_pcm_bridge_payload(&mut reader, &mut payload)
            .await
            .expect("read PCM payload");

        assert_eq!(is_binary, Some(true));
        assert_eq!(payload, frame);
    }

    #[test]
    fn parses_output_xml() {
        let parsed: OutputsXml = quick_xml::de::from_str(
            r#"<outputs><feed id="sk-0001"><webrtc enabled="true"/><udp enabled="false"/></feed></outputs>"#,
        )
        .unwrap();
        assert_eq!(parsed.feeds.len(), 1);
        assert!(parsed.feeds[0].webrtc.is_enabled());
        assert!(!parsed.feeds[0].udp.is_enabled());
    }

    #[test]
    fn parses_enabled_icecast_output() {
        let parsed: OutputsXml = quick_xml::de::from_str(
            r#"
            <outputs>
              <feed id="sk-0001">
                <icecast enabled="true">
                  <host>icecast.example.test</host>
                  <port>8000</port>
                  <password>secret</password>
                  <mount>sk-0001</mount>
                  <format>opus</format>
                </icecast>
              </feed>
            </outputs>
            "#,
        )
        .unwrap();

        let icecast = icecast_output_from_node(&parsed.feeds[0].icecast).unwrap();

        assert_eq!(icecast.host, "icecast.example.test");
        assert_eq!(icecast.port, 8000);
        assert_eq!(icecast.username, "source");
        assert_eq!(icecast.mount, "/sk-0001");
        assert_eq!(icecast.format, "opus");
        assert_eq!(icecast.acodec, "libopus");
    }

    #[test]
    fn icecast_output_allows_blank_mount_for_feed_id_default() {
        let parsed: OutputsXml = quick_xml::de::from_str(
            r#"
            <outputs>
              <feed id="sk-0001">
                <icecast enabled="true">
                  <host>icecast.example.test</host>
                  <password>secret</password>
                </icecast>
              </feed>
            </outputs>
            "#,
        )
        .unwrap();

        let icecast = icecast_output_from_node(&parsed.feeds[0].icecast).unwrap();
        assert_eq!(icecast.mount, "");
    }

    #[test]
    fn media_state_matches_wildcard_outputs() {
        let parsed: OutputsXml = quick_xml::de::from_str(
            r#"
            <outputs>
              <feed id="cwxr-*">
                <icecast enabled="true">
                  <host>icecast.example.test</host>
                  <password>secret</password>
                </icecast>
              </feed>
            </outputs>
            "#,
        )
        .unwrap();
        let feed = parsed.feeds.into_iter().next().unwrap();
        let icecast_node = preferred_icecast_node(feed.icecast, feed.stream);
        let output = output_feed_from_node(feed.id.trim(), icecast_node, feed.webrtc, true);
        let state = MediaState::new(
            BTreeMap::from([(feed.id, output)]),
            BackendMode::Legacy,
            false,
            Vec::new(),
        );
        let output = state.configured_output_for_feed("cwxr-sk01").unwrap();
        assert_eq!(output.id, "cwxr-sk01");
        assert!(output.icecast);
        assert_eq!(output.icecast_config.unwrap().mount, "");
    }

    #[test]
    fn wav_header_uses_streaming_lengths() {
        let header = wav_stream_header();
        assert_eq!(&header[0..4], b"RIFF");
        assert_eq!(&header[8..12], b"WAVE");
        assert_eq!(
            u32::from_le_bytes(header[40..44].try_into().unwrap()),
            u32::MAX
        );
    }

    #[test]
    fn health_reports_active_http_clients() {
        let state = MediaState::new(BTreeMap::new(), BackendMode::Legacy, false, Vec::new());
        let client = state.register_http_client("sk-0001", "raw");
        state.record_http_client_write(client.id(), FRAME_BYTES);
        state.record_http_client_encoded_drop(client.id());
        state.record_http_client_lag(client.id(), 3);

        let health = state.health();
        assert_eq!(health["http_client_count"], json!(1));
        assert_eq!(health["capabilities"]["http_audio"], json!(true));
        assert_eq!(health["capabilities"]["webrtc"], json!(false));
        assert_eq!(health["http_clients"][0]["feed_id"], json!("sk-0001"));
        assert_eq!(health["http_clients"][0]["codec"], json!("raw"));
        assert_eq!(
            health["http_clients"][0]["encoded_chunks_dropped"],
            json!(1)
        );
        assert_eq!(health["http_clients"][0]["lagged_frames"], json!(3));
        assert_eq!(
            health["http_clients"][0]["bytes_written"],
            json!(FRAME_BYTES)
        );
        assert_eq!(health["http_clients"][0]["chunks_written"], json!(1));
        assert_eq!(
            health["audio_processing"]["loudness_enabled"],
            json!(DEFAULT_LOUDNESS_ENABLED)
        );
        assert_eq!(health["clock"]["frame_ms"], json!(20));
        assert_eq!(
            health["clock"]["target_source_queue_ms"],
            json!(TARGET_SOURCE_QUEUE_MS)
        );

        drop(client);
        let health = state.health();
        assert_eq!(health["http_client_count"], json!(0));
    }

    #[test]
    fn feed_clock_uses_named_dedicated_thread() {
        assert_eq!(
            feed_clock_thread_name("sk-0001"),
            "haze-media-clock-sk-0001"
        );
    }

    #[test]
    fn feed_clock_next_tick_keeps_scheduled_cadence_for_small_jitter() {
        let scheduled_tick = Instant::now();
        let ticked_at = scheduled_tick + Duration::from_millis(5);
        let next_tick = next_feed_clock_tick(scheduled_tick, ticked_at);

        assert_eq!(next_tick.duration_since(scheduled_tick), FRAME_DURATION);
    }

    #[test]
    fn feed_clock_next_tick_rebases_after_large_scheduler_stall() {
        let scheduled_tick = Instant::now();
        let ticked_at =
            scheduled_tick + Duration::from_millis(FEED_CLOCK_REBASE_LAG_MS).saturating_mul(2);
        let next_tick = next_feed_clock_tick(scheduled_tick, ticked_at);

        assert_eq!(next_tick.duration_since(ticked_at), FRAME_DURATION);
    }

    #[test]
    fn webrtc_stale_deadline_waits_for_connection_before_idle_tracking() {
        let started_at = Instant::now();
        let last_network_at = started_at + Duration::from_secs(5);

        let deadline = webrtc_peer_stale_deadline(started_at, last_network_at, false);

        assert_eq!(deadline.duration_since(started_at), WEBRTC_CONNECT_TIMEOUT);
    }

    #[test]
    fn earlier_instant_selects_the_nearest_deadline() {
        let now = Instant::now();
        let earlier = now + Duration::from_secs(1);
        let later = now + Duration::from_secs(2);

        assert_eq!(earlier_instant(later, earlier), earlier);
        assert_eq!(earlier_instant(earlier, later), earlier);
    }

    #[test]
    fn webrtc_stale_deadline_tracks_connected_network_idle() {
        let started_at = Instant::now();
        let last_network_at = started_at + Duration::from_secs(5);

        let deadline = webrtc_peer_stale_deadline(started_at, last_network_at, true);

        assert_eq!(
            deadline.duration_since(last_network_at),
            WEBRTC_CONNECTED_IDLE_TIMEOUT
        );
    }

    #[test]
    fn exact_frame_renderer_outputs_one_unstretched_pcm_frame() {
        let mut samples = queued_samples((0..(FRAME_SAMPLES - 4)).map(|value| value as i16), false);
        samples.extend(
            (0..4)
                .map(|value| (FRAME_SAMPLES + value) as i16)
                .map(|value| QueuedSample {
                    value,
                    bypass_loudness: false,
                }),
        );
        let mut frame = Vec::with_capacity(FRAME_BYTES);
        let mut last = vec![0i16; FRAME_SAMPLES];

        let bypass_loudness = render_exact_frame(&mut samples, &mut frame, &mut last);

        assert!(!bypass_loudness);
        assert_eq!(frame.len(), FRAME_BYTES);
        assert!(samples.is_empty());
        assert_eq!(last[0], 0);
        assert_eq!(last[FRAME_SAMPLES - 1], (FRAME_SAMPLES + 3) as i16);
        assert_ne!(last[0], last[FRAME_SAMPLES - 1]);
    }

    #[test]
    fn exact_frame_renderer_preserves_loudness_bypass() {
        let mut samples = queued_samples((0..FRAME_SAMPLES).map(|value| value as i16), true);
        let mut frame = Vec::with_capacity(FRAME_BYTES);
        let mut last = vec![0i16; FRAME_SAMPLES];

        let bypass_loudness = render_exact_frame(&mut samples, &mut frame, &mut last);

        assert!(bypass_loudness);
        assert_eq!(frame.len(), FRAME_BYTES);
    }

    #[test]
    fn source_queue_hard_drop_returns_to_target_latency() {
        let max_samples = max_source_queue_samples();
        let mut samples = queued_samples((0..(max_samples + 10)).map(|value| value as i16), false);

        let stale_dropped = trim_source_queue(&mut samples);

        assert_eq!(
            stale_dropped as usize,
            max_samples + 10 - target_source_queue_samples()
        );
        assert_eq!(samples.len(), target_source_queue_samples());
    }

    #[test]
    fn source_queue_silent_overflow_drops_back_to_target_latency() {
        let target_samples = target_source_queue_samples();
        let max_samples = max_source_queue_samples();
        let starting_samples = max_samples + FRAME_SAMPLES * 2;
        let mut samples = VecDeque::from(vec![QueuedSample::default(); starting_samples]);

        let stale_dropped = trim_source_queue(&mut samples);

        assert_eq!(stale_dropped as usize, starting_samples - target_samples);
        assert_eq!(samples.len(), target_samples);
    }

    #[test]
    fn source_queue_soft_overflow_drops_back_to_target_latency() {
        let soft_samples = soft_source_queue_samples();
        let mut samples =
            queued_samples((0..(soft_samples + 500)).map(|value| value as i16), false);

        let stale_dropped = trim_source_queue(&mut samples);

        assert_eq!(
            stale_dropped as usize,
            soft_samples + 500 - target_source_queue_samples()
        );
        assert_eq!(samples.len(), target_source_queue_samples());
    }

    #[test]
    fn source_queue_under_soft_limit_preserves_real_audio() {
        let soft_samples = soft_source_queue_samples();
        let mut samples = queued_samples((0..soft_samples).map(|value| value as i16), false);

        let stale_dropped = trim_source_queue(&mut samples);

        assert_eq!(stale_dropped, 0);
        assert_eq!(samples.len(), soft_samples);
    }

    #[test]
    fn concealment_frame_decays_last_real_audio() {
        let mut frame = Vec::new();
        let last_real = vec![12_000i16; FRAME_SAMPLES];

        render_concealment_frame(&last_real, CONCEALMENT_FRAMES, &mut frame);

        let samples = pcm16_samples(&frame).collect::<Vec<_>>();
        assert_eq!(samples.len(), FRAME_SAMPLES);
        assert!(samples[0] > 0);
        assert!(samples[0] < last_real[0]);
        assert_eq!(samples[0], samples[FRAME_SAMPLES - 1]);
    }

    #[test]
    fn frame_signal_detects_silence_repetition_and_clipping() {
        let silence = vec![0u8; FRAME_BYTES];
        let silent_signal = analyze_pcm_frame(&silence, &[]);
        assert!(silent_signal.near_silent);
        assert!(!silent_signal.repeated);
        assert_eq!(silent_signal.clipped_samples, 0);

        let repeated_signal = analyze_pcm_frame(&silence, &silence);
        assert!(repeated_signal.repeated);

        let mut clipped = Vec::with_capacity(FRAME_BYTES);
        for _ in 0..FRAME_SAMPLES {
            push_i16(&mut clipped, i16::MAX);
        }
        let clipped_signal = analyze_pcm_frame(&clipped, &silence);
        assert!(!clipped_signal.near_silent);
        assert_eq!(clipped_signal.peak, i16::MAX as u16);
        assert_eq!(clipped_signal.clipped_samples, FRAME_SAMPLES as u64);
    }

    #[test]
    fn webrtc_selection_honors_receiver_g722_preference() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111 9 0 8\r\na=rtpmap:111 opus/48000/2\r\na=rtpmap:9 G722/8000\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:8 PCMA/8000\r\n";
        let selection = select_webrtc_audio_codec(
            offer,
            &[
                WebRTCAudioCodec::Opus,
                WebRTCAudioCodec::G722,
                WebRTCAudioCodec::Pcmu,
                WebRTCAudioCodec::Pcma,
            ],
            "g722",
            false,
            false,
        )
        .unwrap();

        assert_eq!(selection.codec, WebRTCAudioCodec::G722);
        assert_eq!(selection.payload_type, 9);
    }

    #[test]
    fn webrtc_selection_prefers_opus_for_browser_auto() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111 0 8\r\na=rtpmap:111 opus/48000/2\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:8 PCMA/8000\r\n";
        let selection = select_webrtc_audio_codec(
            offer,
            &[WebRTCAudioCodec::Opus, WebRTCAudioCodec::Pcmu],
            "",
            false,
            false,
        )
        .unwrap();

        assert_eq!(selection.codec, WebRTCAudioCodec::Opus);
        assert_eq!(selection.payload_type, 111);
    }

    #[test]
    fn webrtc_selection_falls_back_when_preferred_codec_is_unavailable() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111 9 0 8\r\na=rtpmap:111 opus/48000/2\r\na=rtpmap:9 G722/8000\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:8 PCMA/8000\r\n";
        let selection = select_webrtc_audio_codec(
            offer,
            &[WebRTCAudioCodec::Opus, WebRTCAudioCodec::Pcmu],
            "g722",
            false,
            false,
        )
        .unwrap();

        assert_eq!(selection.codec, WebRTCAudioCodec::Opus);
        assert_eq!(selection.payload_type, 111);
    }

    #[test]
    fn webrtc_selection_falls_back_to_static_pcmu_payload() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\n";
        let selection =
            select_webrtc_audio_codec(offer, &[WebRTCAudioCodec::Pcmu], "", false, false).unwrap();

        assert_eq!(selection.codec, WebRTCAudioCodec::Pcmu);
        assert_eq!(selection.payload_type, 0);
    }

    #[test]
    fn str0m_answer_keeps_offer_codec_attributes_for_gstreamer() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtcp-mux\r\na=rtcp-rsize\r\na=rtpmap:111 OPUS/48000/2\r\na=fmtp:111 minptime=10;useinbandfec=1\r\na=rtcp-fb:111 transport-cc\r\n";
        let answer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=sendonly\r\n";
        let selection = WebRTCCodecSelection {
            codec: WebRTCAudioCodec::Opus,
            payload_type: 111,
        };

        let patched = ensure_str0m_answer_codec_lines(answer, offer, selection);

        assert!(patched.contains("a=rtpmap:111 OPUS/48000/2"));
        assert!(patched.contains("a=fmtp:111 minptime=10;useinbandfec=1"));
        assert!(patched.contains("a=rtcp-fb:111 transport-cc"));
        assert!(patched.contains("a=rtcp-mux"));
        assert!(patched.contains("a=rtcp-rsize"));
        assert!(patched.contains("m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111"));
    }

    #[test]
    fn str0m_answer_rewrites_rejected_sparse_audio_mline_for_gstreamer() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtcp-mux\r\na=rtcp-rsize\r\na=rtpmap:111 OPUS/48000/2\r\na=rtcp-fb:111 transport-cc\r\n";
        let answer = "v=0\r\nm=audio 0 UDP/TLS/RTP/SAVPF\r\nc=IN IP4 0.0.0.0\r\na=sendonly\r\n";
        let selection = WebRTCCodecSelection {
            codec: WebRTCAudioCodec::Opus,
            payload_type: 111,
        };

        let patched = ensure_str0m_answer_codec_lines(answer, offer, selection);

        assert!(patched.contains("m=audio 9 UDP/TLS/RTP/SAVPF 111"));
        assert!(
            patched.contains("m=audio 9 UDP/TLS/RTP/SAVPF 111\r\nc=IN IP4 0.0.0.0\r\na=rtpmap:111")
        );
        assert!(patched.contains("a=rtpmap:111 OPUS/48000/2"));
        assert!(patched.contains("a=rtcp-fb:111 transport-cc"));
        assert!(patched.contains("a=rtcp-mux"));
        assert!(patched.contains("a=rtcp-rsize"));
        assert!(!patched.contains("m=audio 0 UDP/TLS/RTP/SAVPF\r\n"));
    }

    #[test]
    fn str0m_answer_adds_missing_webrtc_audio_transport_attributes() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtcp-mux\r\na=rtcp-rsize\r\na=rtpmap:111 OPUS/48000/2\r\n";
        let answer =
            "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 OPUS/48000/2\r\na=sendonly\r\n";
        let selection = WebRTCCodecSelection {
            codec: WebRTCAudioCodec::Opus,
            payload_type: 111,
        };

        let patched = ensure_str0m_answer_codec_lines(answer, offer, selection);

        assert_eq!(patched.matches("a=rtpmap:111").count(), 1);
        assert!(patched.contains("a=rtcp-mux"));
        assert!(patched.contains("a=rtcp-rsize"));
    }

    #[test]
    fn str0m_answer_drops_unselected_audio_payload_attributes() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111 0 8\r\na=rtcp-mux\r\na=rtpmap:111 OPUS/48000/2\r\na=fmtp:111 minptime=10;useinbandfec=1\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:8 PCMA/8000\r\n";
        let answer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111 0 8\r\na=rtpmap:111 OPUS/48000/2\r\na=rtpmap:0 PCMU/8000\r\na=rtpmap:8 PCMA/8000\r\na=sendonly\r\n";
        let selection = WebRTCCodecSelection {
            codec: WebRTCAudioCodec::Opus,
            payload_type: 111,
        };

        let patched = ensure_str0m_answer_codec_lines(answer, offer, selection);

        assert!(patched.contains("m=audio 9 UDP/TLS/RTP/SAVPF 111"));
        assert!(patched.contains("a=rtpmap:111 OPUS/48000/2"));
        assert!(patched.contains("a=fmtp:111 minptime=10;useinbandfec=1"));
        assert!(patched.contains("a=sendonly"));
        assert!(!patched.contains("a=rtpmap:0"));
        assert!(!patched.contains("a=rtpmap:8"));
    }

    #[test]
    fn str0m_answer_repairs_empty_bundle_group_for_gstreamer() {
        let offer = "v=0\r\na=group:BUNDLE audio0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 OPUS/48000/2\r\n";
        let answer = "v=0\r\na=group:BUNDLE\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=sendonly\r\n";
        let selection = WebRTCCodecSelection {
            codec: WebRTCAudioCodec::Opus,
            payload_type: 111,
        };

        let patched = ensure_str0m_answer_codec_lines(answer, offer, selection);

        assert!(patched.contains("a=group:BUNDLE audio0\r\nm=audio"));
        assert!(!patched.contains("a=group:BUNDLE\r\nm=audio"));
    }

    #[cfg(feature = "gstreamer-backend")]
    #[test]
    fn offered_host_candidate_ip_reads_gstreamer_candidate() {
        let offer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=candidate:1 1 UDP 2015363327 172.16.1.37 43521 typ host\r\n";

        assert_eq!(
            offered_host_candidate_ip(offer),
            Some(IpAddr::V4(Ipv4Addr::new(172, 16, 1, 37)))
        );
    }

    #[cfg(feature = "gstreamer-backend")]
    #[test]
    fn webrtc_candidate_ip_prefers_media_specific_override() {
        let overrides = [
            ("HAZE_MEDIA_WEBRTC_HOST", Some("172.16.1.30".to_string())),
            ("HAZE_WEBRTC_HOST", Some("172.20.48.1".to_string())),
            ("HAZE_PUBLIC_HOST", Some("203.0.113.10".to_string())),
        ];

        let selected = detect_webrtc_candidate_ip_inner("", None, &overrides);

        assert_eq!(selected.ip, IpAddr::V4(Ipv4Addr::new(172, 16, 1, 30)));
        assert_eq!(selected.source, "HAZE_MEDIA_WEBRTC_HOST");
    }

    #[cfg(feature = "gstreamer-backend")]
    #[test]
    fn webrtc_candidate_ip_ignores_invalid_override_before_generic_override() {
        let overrides = [
            ("HAZE_MEDIA_WEBRTC_HOST", Some("not-an-ip".to_string())),
            ("HAZE_WEBRTC_HOST", Some("172.16.1.30".to_string())),
            ("HAZE_PUBLIC_HOST", None),
        ];

        let selected = detect_webrtc_candidate_ip_inner("", None, &overrides);

        assert_eq!(selected.ip, IpAddr::V4(Ipv4Addr::new(172, 16, 1, 30)));
        assert_eq!(selected.source, "HAZE_WEBRTC_HOST");
    }

    #[cfg(feature = "gstreamer-backend")]
    #[test]
    fn webrtc_candidate_ip_treats_auto_override_as_autodetect() {
        let overrides = [
            ("HAZE_MEDIA_WEBRTC_HOST", Some("auto".to_string())),
            ("HAZE_WEBRTC_HOST", Some("172.16.1.38".to_string())),
            ("HAZE_PUBLIC_HOST", None),
        ];

        let selected = detect_webrtc_candidate_ip_inner("", None, &overrides);

        assert_eq!(selected.ip, IpAddr::V4(Ipv4Addr::new(172, 16, 1, 38)));
        assert_eq!(selected.source, "HAZE_WEBRTC_HOST");
    }

    #[test]
    fn webrtc_udp_port_range_is_bounded_and_validated() {
        assert_eq!(
            parse_webrtc_udp_port_range(Some("50000"), Some("50199")).unwrap(),
            Some((50_000, 50_199))
        );
        assert_eq!(parse_webrtc_udp_port_range(None, None).unwrap(), None);
        assert!(parse_webrtc_udp_port_range(Some("50000"), None).is_err());
        assert!(parse_webrtc_udp_port_range(Some("1023"), Some("1024")).is_err());
        assert!(parse_webrtc_udp_port_range(Some("50199"), Some("50000")).is_err());
        assert!(parse_webrtc_udp_port_range(Some("50000"), Some("55000")).is_err());
    }

    #[cfg(feature = "gstreamer-backend")]
    #[test]
    fn webrtc_private_peer_detection_keeps_lan_candidates_routable() {
        assert!(webrtc_peer_is_private(IpAddr::V4(Ipv4Addr::new(
            172, 16, 1, 30
        ))));
        assert!(webrtc_peer_is_private(IpAddr::V4(Ipv4Addr::new(
            169, 254, 1, 2
        ))));
        assert!(!webrtc_peer_is_private(IpAddr::V4(Ipv4Addr::new(
            198, 51, 100, 20
        ))));
    }

    #[cfg(feature = "gstreamer-backend")]
    #[test]
    fn webrtc_request_peer_ip_prefers_proxy_client_ip() {
        let payload = WebRTCOfferRequest {
            feed_id: String::new(),
            sdp: String::new(),
            _sdp_type: String::new(),
            preferred_codec: String::new(),
            codec: String::new(),
            require_opus: false,
            disable_g722: false,
            client_ip: "172.16.1.55".to_string(),
            remote_addr: "127.0.0.1:6444".to_string(),
        };

        assert_eq!(
            parse_webrtc_request_peer_ip(&payload),
            Some(IpAddr::V4(Ipv4Addr::new(172, 16, 1, 55)))
        );
    }

    #[test]
    fn webrtc_listener_client_id_prefers_proxy_client_ip() {
        let payload = WebRTCOfferRequest {
            feed_id: String::new(),
            sdp: String::new(),
            _sdp_type: String::new(),
            preferred_codec: String::new(),
            codec: String::new(),
            require_opus: false,
            disable_g722: false,
            client_ip: "172.16.1.56".to_string(),
            remote_addr: "127.0.0.1:6444".to_string(),
        };

        assert_eq!(
            webrtc_listener_client_id(
                &payload,
                Some("127.0.0.1:8097".parse().expect("valid socket address"))
            ),
            "172.16.1.56"
        );
    }

    #[test]
    fn webrtc_listener_registry_rejects_duplicate_feed_client() {
        let state = MediaState::new(BTreeMap::new(), BackendMode::Legacy, false, Vec::new());
        let first = state.register_webrtc_peer("feed-a", "opus", "172.16.1.56");
        let duplicate = state.register_webrtc_peer("feed-a", "opus", "172.16.1.56");
        let other_feed = state.register_webrtc_peer("feed-b", "opus", "172.16.1.56");
        let other_client = state.register_webrtc_peer("feed-a", "opus", "172.16.1.57");

        assert!(first.is_some());
        assert!(duplicate.is_none());
        assert!(other_feed.is_some());
        assert!(other_client.is_some());
    }

    #[test]
    fn loudness_processor_raises_quiet_audio_and_limits_hot_audio() {
        let mut loudness = AudioLoudness {
            enabled: true,
            gain: db_to_gain(6.0),
            threshold_db: -18.0,
            ratio: 3.0,
            makeup: db_to_gain(1.5),
            ceiling: 0.98,
            attack_coeff: compressor_coeff_ms(5.0),
            release_coeff: compressor_coeff_ms(120.0),
            envelope: 0.0,
        };

        let quiet = loudness.process_sample(1_000);
        let hot = loudness.process_sample(32_000);

        assert!(quiet > 1_000);
        assert!(hot <= (f32::from(i16::MAX) * 0.98).round() as i16);
    }

    #[test]
    fn disabled_loudness_processor_is_bypass() {
        let mut frame = Vec::new();
        push_i16(&mut frame, -1234);
        push_i16(&mut frame, 1234);
        let original = frame.clone();
        AudioLoudness {
            enabled: false,
            gain: db_to_gain(12.0),
            threshold_db: -30.0,
            ratio: 10.0,
            makeup: db_to_gain(12.0),
            ceiling: 0.5,
            attack_coeff: compressor_coeff_ms(5.0),
            release_coeff: compressor_coeff_ms(120.0),
            envelope: 0.0,
        }
        .process(&mut frame);

        assert_eq!(frame, original);
    }

    #[test]
    fn media_loudness_is_opt_in_by_default() {
        std::env::remove_var("HAZE_MEDIA_LOUDNESS_ENABLED");

        let loudness = AudioLoudness::from_env();

        assert!(!loudness.enabled);
    }

    #[test]
    fn audio_health_allows_standby_silence() {
        let stats = FeedStats {
            feed_id: "CAP-IT-ALL".to_string(),
            frames: 10_000,
            real_frames: 10_000,
            near_silent_frames: 10_000,
            repeated_frames: 9_999,
            consecutive_near_silent_frames: 9_999,
            consecutive_repeated_frames: 9_998,
            queued_samples: FRAME_SAMPLES * 12,
            last_input_age_ms: Some(20),
            max_tick_gap_ms: 20,
            ..FeedStats::default()
        };

        let (ok, warnings) = classify_audio_health(&stats);
        assert!(ok, "{warnings:?}");
        assert!(warnings.is_empty());
    }

    #[test]
    fn audio_health_flags_robotic_failure_modes() {
        let stats = FeedStats {
            feed_id: "sk-0001".to_string(),
            frames: 10_000,
            real_frames: 9_990,
            concealed_frames: 5,
            recent_concealed_frames: 5,
            repeated_non_silent_frames: 4,
            recent_repeated_non_silent_frames: 4,
            clipped_samples: 2,
            recent_clipped_samples: 2,
            late_ticks: 1,
            recent_late_ticks: 1,
            queued_samples: FRAME_SAMPLES * 12,
            last_input_age_ms: Some(20),
            last_tick_gap_ms: Some(55),
            ..FeedStats::default()
        };

        let (ok, warnings) = classify_audio_health(&stats);
        assert!(!ok);
        assert!(warnings
            .iter()
            .any(|warning| warning.starts_with("recent_concealed_frames=")));
        assert!(warnings
            .iter()
            .any(|warning| warning.starts_with("recent_repeated_non_silent_frames=")));
        assert!(warnings
            .iter()
            .any(|warning| warning.starts_with("recent_clipped_samples=")));
        assert!(warnings
            .iter()
            .any(|warning| warning.starts_with("recent_late_ticks=")));
    }

    #[test]
    fn audio_health_recovers_after_rolling_window() {
        let now = Instant::now();
        let mut stats = FeedStats {
            recent_concealed_frames: 8,
            recent_dropped_input_chunks: 2,
            health_window_started_at: Some(now - AUDIO_HEALTH_WINDOW),
            ..FeedStats::default()
        };

        reset_audio_health_window(&mut stats, now);

        assert_eq!(stats.recent_concealed_frames, 0);
        assert_eq!(stats.recent_dropped_input_chunks, 0);
    }

    #[cfg(feature = "gstreamer-backend")]
    #[test]
    fn gstreamer_opus_pipeline_uses_clean_broadcast_bitrate() {
        let pipeline = gstreamer_audio_pipeline("opus").unwrap();
        assert!(pipeline.contains("bitrate=32000"));
        assert!(pipeline.contains("frame-size=20"));
        assert!(pipeline.contains("max-bytes=11520"));
        assert!(pipeline.contains("max-size-time=120000000"));
        assert!(pipeline.contains("max-size-buffers=6"));
        assert!(pipeline.contains("max-delay=20000000"));
        assert!(pipeline.contains("max-page-delay=20000000"));
        assert!(pipeline.contains("max-buffers=8"));
        assert!(pipeline.contains("drop=true"));
    }

    #[cfg(feature = "gstreamer-backend")]
    #[test]
    fn gstreamer_aac_pipeline_emits_adts_frames() {
        let pipeline = gstreamer_audio_pipeline("aac").unwrap();
        assert!(pipeline.contains("stream-format=adts"));
    }
}
