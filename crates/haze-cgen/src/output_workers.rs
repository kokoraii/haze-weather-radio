use std::any::Any;
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::num::{NonZeroU16, NonZeroU32};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::architecture::{
    AudioCodec, AudioCodecPolicy, EncoderOutputSpec, OutputDestination, VideoCodec,
};

pub(crate) type SinkFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, OutputFailure>> + Send + 'a>>;

/// An output-specific GPU surface, DMA-BUF, D3D texture, or other retained frame handle.
///
/// The isolation layer never requires a CPU readback. Platform backends implement this
/// trait and perform any required interop inside the destination worker.
pub(crate) trait VideoFrameHandle: fmt::Debug + Send + Sync {
    fn backend(&self) -> &'static str;

    /// Exposes the concrete retained-handle type to an output adapter.
    ///
    /// Output workers keep the handle opaque. A sink may downcast only to a
    /// handle type it explicitly supports, without relying on backend strings
    /// or unsafe pointer casts.
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone)]
pub(crate) struct VideoFrame {
    pub(crate) sequence: u64,
    pub(crate) pts_ns: u64,
    pub(crate) duration_ns: u64,
    pub(crate) discontinuity: bool,
    pub(crate) width: NonZeroU32,
    pub(crate) height: NonZeroU32,
    pub(crate) surface: Arc<dyn VideoFrameHandle>,
}

impl fmt::Debug for VideoFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VideoFrame")
            .field("sequence", &self.sequence)
            .field("pts_ns", &self.pts_ns)
            .field("duration_ns", &self.duration_ns)
            .field("discontinuity", &self.discontinuity)
            .field("width", &self.width)
            .field("height", &self.height)
            .field("surface_backend", &self.surface.backend())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum AudioPayload {
    InterleavedF32(Arc<[f32]>),
    Encoded(Arc<[u8]>),
}

impl AudioPayload {
    fn sample_count(&self) -> usize {
        match self {
            Self::InterleavedF32(samples) => samples.len(),
            Self::Encoded(bytes) => bytes.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AudioPacket {
    pub(crate) sequence: u64,
    pub(crate) pts_ns: u64,
    pub(crate) duration_ns: u64,
    pub(crate) discontinuity: bool,
    pub(crate) sample_rate: NonZeroU32,
    pub(crate) channels: NonZeroU16,
    pub(crate) payload: AudioPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputFailureCode {
    Factory,
    Connect,
    ConnectTimeout,
    WriteTimeout,
    Authentication,
    Network,
    Protocol,
    Encoder,
    Muxer,
    Sink,
}

impl OutputFailureCode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Factory => "factory",
            Self::Connect => "connect",
            Self::ConnectTimeout => "connect_timeout",
            Self::WriteTimeout => "write_timeout",
            Self::Authentication => "authentication",
            Self::Network => "network",
            Self::Protocol => "protocol",
            Self::Encoder => "encoder",
            Self::Muxer => "muxer",
            Self::Sink => "sink",
        }
    }
}

/// A deliberately redacted failure. Sink implementations log detailed errors locally,
/// but only this stable code and retry policy cross the worker status boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutputFailure {
    pub(crate) code: OutputFailureCode,
    pub(crate) retryable: bool,
}

impl OutputFailure {
    pub(crate) const fn retryable(code: OutputFailureCode) -> Self {
        Self {
            code,
            retryable: true,
        }
    }

    pub(crate) const fn terminal(code: OutputFailureCode) -> Self {
        Self {
            code,
            retryable: false,
        }
    }
}

pub(crate) trait OutputSink: Send {
    fn connect(&mut self) -> SinkFuture<'_, ()>;
    fn write_video(&mut self, frame: Arc<VideoFrame>) -> SinkFuture<'_, ()>;
    fn write_audio(&mut self, packet: Arc<AudioPacket>) -> SinkFuture<'_, ()>;
    fn close(&mut self) -> SinkFuture<'_, ()>;
}

pub(crate) trait OutputSinkFactory: Send + Sync {
    /// Create a fresh sink for a connection attempt.
    ///
    /// Destination environment references must be expanded here, at activation time.
    /// Factories must not copy resolved locations into status or error messages.
    fn create(&self, output: Arc<EncoderOutputSpec>) -> Result<Box<dyn OutputSink>, OutputFailure>;
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(crate) enum OutputCompatibilityError {
    #[error("RTMP requires H.264 video and AAC audio")]
    InvalidRtmpCodecs,
    #[error("FLV requires H.264 video and AAC audio")]
    InvalidFlvCodecs,
    #[error("MP4 and MOV require H.264 or H.265 video and AAC audio")]
    InvalidIsoBmffCodecs,
    #[error("MPEG program stream requires MPEG-2 video and AC3 or MP2 audio")]
    InvalidMpegProgramStreamCodecs,
    #[error("{0}")]
    Rejected(&'static str),
}

pub(crate) trait OutputCompatibilityValidator: Send + Sync {
    fn validate(&self, output: &EncoderOutputSpec) -> Result<(), OutputCompatibilityError>;
}

#[derive(Debug, Default)]
pub(crate) struct StandardOutputCompatibility;

impl OutputCompatibilityValidator for StandardOutputCompatibility {
    fn validate(&self, output: &EncoderOutputSpec) -> Result<(), OutputCompatibilityError> {
        let encoded_audio = match output.audio.codec {
            AudioCodecPolicy::MatchInput => None,
            AudioCodecPolicy::Encode(codec) => Some(codec),
        };
        match &output.destination {
            OutputDestination::Rtmp { .. }
                if output.video.codec != VideoCodec::H264
                    || encoded_audio != Some(AudioCodec::Aac) =>
            {
                Err(OutputCompatibilityError::InvalidRtmpCodecs)
            }
            OutputDestination::File { container, .. } => {
                match container.trim().to_ascii_lowercase().as_str() {
                    "flv"
                        if output.video.codec != VideoCodec::H264
                            || encoded_audio != Some(AudioCodec::Aac) =>
                    {
                        Err(OutputCompatibilityError::InvalidFlvCodecs)
                    }
                    "mp4" | "mov"
                        if !matches!(output.video.codec, VideoCodec::H264 | VideoCodec::H265)
                            || encoded_audio != Some(AudioCodec::Aac) =>
                    {
                        Err(OutputCompatibilityError::InvalidIsoBmffCodecs)
                    }
                    "mpegps" | "mpeg_ps" | "ps"
                        if output.video.codec != VideoCodec::Mpeg2
                            || !matches!(
                                encoded_audio,
                                Some(AudioCodec::Ac3 | AudioCodec::Mp2)
                            ) =>
                    {
                        Err(OutputCompatibilityError::InvalidMpegProgramStreamCodecs)
                    }
                    _ => Ok(()),
                }
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Default)]
struct NoAdditionalCompatibility;

impl OutputCompatibilityValidator for NoAdditionalCompatibility {
    fn validate(&self, _output: &EncoderOutputSpec) -> Result<(), OutputCompatibilityError> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OutputWorkerConfig {
    pub(crate) audio_queue_capacity: usize,
    pub(crate) maximum_media_age: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) write_timeout: Duration,
    pub(crate) close_timeout: Duration,
    pub(crate) initial_backoff: Duration,
    pub(crate) maximum_backoff: Duration,
    pub(crate) backoff_reset_after: Duration,
}

impl Default for OutputWorkerConfig {
    fn default() -> Self {
        Self {
            audio_queue_capacity: 32,
            maximum_media_age: Duration::from_millis(500),
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(2),
            close_timeout: Duration::from_secs(2),
            initial_backoff: Duration::from_millis(250),
            maximum_backoff: Duration::from_secs(30),
            backoff_reset_after: Duration::from_secs(10),
        }
    }
}

impl OutputWorkerConfig {
    fn validate(&self) -> Result<(), OutputRuntimeError> {
        if self.audio_queue_capacity == 0 {
            return Err(OutputRuntimeError::InvalidConfig(
                "audio queue capacity must be non-zero",
            ));
        }
        if self.maximum_media_age.is_zero() {
            return Err(OutputRuntimeError::InvalidConfig(
                "maximum media age must be non-zero",
            ));
        }
        if self.connect_timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.close_timeout.is_zero()
        {
            return Err(OutputRuntimeError::InvalidConfig(
                "sink timeouts must be non-zero",
            ));
        }
        if self.initial_backoff.is_zero() || self.maximum_backoff < self.initial_backoff {
            return Err(OutputRuntimeError::InvalidConfig(
                "maximum backoff must be at least the non-zero initial backoff",
            ));
        }
        if self.backoff_reset_after.is_zero() {
            return Err(OutputRuntimeError::InvalidConfig(
                "backoff reset interval must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub(crate) enum OutputRuntimeError {
    #[error("output runtime requires at least one enabled destination")]
    NoEnabledOutputs,
    #[error("output ID {0} is configured more than once")]
    DuplicateOutputId(String),
    #[error("output {output_id} is incompatible: {source}")]
    Incompatible {
        output_id: String,
        #[source]
        source: OutputCompatibilityError,
    },
    #[error("invalid output worker configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("output workers require an active Tokio runtime")]
    RuntimeUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputWorkerPhase {
    Starting,
    Connecting,
    Online,
    Backoff,
    Failed,
    Stopped,
}

impl OutputWorkerPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Connecting => "connecting",
            Self::Online => "online",
            Self::Backoff => "backoff",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputStateView {
    pub(crate) phase: OutputWorkerPhase,
    pub(crate) connection_generation: u64,
    pub(crate) consecutive_failures: u32,
    pub(crate) backoff_ms: Option<u64>,
    pub(crate) last_error: Option<OutputFailureCode>,
}

impl Default for OutputStateView {
    fn default() -> Self {
        Self {
            phase: OutputWorkerPhase::Starting,
            connection_generation: 0,
            consecutive_failures: 0,
            backoff_ms: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Default)]
struct WorkerMetrics {
    next_video_dispatch: AtomicU64,
    video_enqueued: AtomicU64,
    video_written: AtomicU64,
    video_stale_dropped: AtomicU64,
    video_age_dropped: AtomicU64,
    video_pts_dropped: AtomicU64,
    video_sink_dropped: AtomicU64,
    audio_enqueued: AtomicU64,
    audio_written: AtomicU64,
    audio_queue_dropped: AtomicU64,
    audio_age_dropped: AtomicU64,
    audio_pts_dropped: AtomicU64,
    audio_sink_dropped: AtomicU64,
    connect_attempts: AtomicU64,
    reconnects: AtomicU64,
    failures: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputStatusSnapshot {
    pub(crate) output_id: String,
    pub(crate) destination_kind: &'static str,
    pub(crate) state: OutputStateView,
    pub(crate) video_enqueued: u64,
    pub(crate) video_written: u64,
    pub(crate) video_stale_dropped: u64,
    pub(crate) video_age_dropped: u64,
    pub(crate) video_pts_dropped: u64,
    pub(crate) video_sink_dropped: u64,
    pub(crate) audio_enqueued: u64,
    pub(crate) audio_written: u64,
    pub(crate) audio_queue_dropped: u64,
    pub(crate) audio_age_dropped: u64,
    pub(crate) audio_pts_dropped: u64,
    pub(crate) audio_sink_dropped: u64,
    pub(crate) connect_attempts: u64,
    pub(crate) reconnects: u64,
    pub(crate) failures: u64,
}

impl OutputStatusSnapshot {
    pub(crate) fn audio_drops(&self) -> u64 {
        self.audio_queue_dropped
            .saturating_add(self.audio_age_dropped)
            .saturating_add(self.audio_pts_dropped)
            .saturating_add(self.audio_sink_dropped)
    }

    pub(crate) fn status_value(&self) -> Value {
        json!({
            "output_id": self.output_id,
            "destination": self.destination_kind,
            "state": self.state.phase.as_str(),
            "connection_generation": self.state.connection_generation,
            "consecutive_failures": self.state.consecutive_failures,
            "backoff_ms": self.state.backoff_ms,
            "last_error": self.state.last_error.map(OutputFailureCode::as_str),
            "video": {
                "enqueued": self.video_enqueued,
                "written": self.video_written,
                "stale_dropped": self.video_stale_dropped,
                "age_dropped": self.video_age_dropped,
                "pts_dropped": self.video_pts_dropped,
                "sink_dropped": self.video_sink_dropped,
            },
            "audio": {
                "enqueued": self.audio_enqueued,
                "written": self.audio_written,
                "queue_dropped": self.audio_queue_dropped,
                "age_dropped": self.audio_age_dropped,
                "pts_dropped": self.audio_pts_dropped,
                "sink_dropped": self.audio_sink_dropped,
                "drops": self.audio_drops(),
            },
            "connect_attempts": self.connect_attempts,
            "reconnects": self.reconnects,
            "failures": self.failures,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct FanoutReport {
    pub(crate) accepted: usize,
    pub(crate) queue_dropped: usize,
    pub(crate) closed: usize,
}

#[derive(Clone)]
struct VideoEnvelope {
    dispatch_sequence: u64,
    enqueued_at: Instant,
    frame: Arc<VideoFrame>,
}

struct AudioEnvelope {
    enqueued_at: Instant,
    packet: Arc<AudioPacket>,
}

struct OutputWorkerHandle {
    output_id: String,
    destination_kind: &'static str,
    video_tx: watch::Sender<Option<VideoEnvelope>>,
    audio_tx: mpsc::Sender<AudioEnvelope>,
    state_rx: watch::Receiver<OutputStateView>,
    metrics: Arc<WorkerMetrics>,
}

pub(crate) struct OutputFanout {
    workers: Vec<OutputWorkerHandle>,
    shutdown_tx: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl OutputFanout {
    pub(crate) fn spawn(
        outputs: impl IntoIterator<Item = EncoderOutputSpec>,
        factory: Arc<dyn OutputSinkFactory>,
        config: OutputWorkerConfig,
    ) -> Result<Self, OutputRuntimeError> {
        Self::spawn_with_validator(
            outputs,
            factory,
            Arc::new(NoAdditionalCompatibility),
            config,
        )
    }

    pub(crate) fn spawn_with_validator(
        outputs: impl IntoIterator<Item = EncoderOutputSpec>,
        factory: Arc<dyn OutputSinkFactory>,
        validator: Arc<dyn OutputCompatibilityValidator>,
        config: OutputWorkerConfig,
    ) -> Result<Self, OutputRuntimeError> {
        config.validate()?;
        tokio::runtime::Handle::try_current()
            .map_err(|_| OutputRuntimeError::RuntimeUnavailable)?;

        let outputs = outputs
            .into_iter()
            .filter(|output| output.enabled)
            .collect::<Vec<_>>();
        if outputs.is_empty() {
            return Err(OutputRuntimeError::NoEnabledOutputs);
        }

        let mut ids = BTreeSet::new();
        for output in &outputs {
            let folded = output.id.as_str().to_ascii_lowercase();
            if !ids.insert(folded) {
                return Err(OutputRuntimeError::DuplicateOutputId(output.id.to_string()));
            }
            StandardOutputCompatibility
                .validate(output)
                .map_err(|source| OutputRuntimeError::Incompatible {
                    output_id: output.id.to_string(),
                    source,
                })?;
            validator
                .validate(output)
                .map_err(|source| OutputRuntimeError::Incompatible {
                    output_id: output.id.to_string(),
                    source,
                })?;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut workers = Vec::with_capacity(outputs.len());
        let mut tasks = Vec::with_capacity(outputs.len());

        for output in outputs {
            let output = Arc::new(output);
            let output_id = output.id.to_string();
            let destination_kind = output.destination.kind();
            let metrics = Arc::new(WorkerMetrics::default());
            let (video_tx, video_rx) = watch::channel(None);
            let (audio_tx, audio_rx) = mpsc::channel(config.audio_queue_capacity);
            let (state_tx, state_rx) = watch::channel(OutputStateView::default());
            let task = tokio::spawn(run_output_worker(OutputWorkerContext {
                output,
                factory: Arc::clone(&factory),
                config: config.clone(),
                video_rx,
                audio_rx,
                shutdown_rx: shutdown_rx.clone(),
                state_tx,
                metrics: Arc::clone(&metrics),
            }));
            workers.push(OutputWorkerHandle {
                output_id,
                destination_kind,
                video_tx,
                audio_tx,
                state_rx,
                metrics,
            });
            tasks.push(task);
        }

        Ok(Self {
            workers,
            shutdown_tx,
            tasks: Mutex::new(tasks),
        })
    }

    /// Publish one retained frame to every destination without awaiting any worker.
    /// Each worker retains only the newest unconsumed frame.
    pub(crate) fn publish_video(&self, frame: Arc<VideoFrame>) -> FanoutReport {
        let mut report = FanoutReport::default();
        let now = Instant::now();
        for worker in &self.workers {
            let dispatch_sequence = worker
                .metrics
                .next_video_dispatch
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            let envelope = VideoEnvelope {
                dispatch_sequence,
                enqueued_at: now,
                frame: Arc::clone(&frame),
            };
            if worker.video_tx.send(Some(envelope)).is_ok() {
                worker
                    .metrics
                    .video_enqueued
                    .fetch_add(1, Ordering::Relaxed);
                report.accepted += 1;
            } else {
                report.closed += 1;
            }
        }
        report
    }

    /// Publish one audio packet to every destination without awaiting any worker.
    /// Full destination queues drop only that destination's packet.
    pub(crate) fn publish_audio(&self, packet: Arc<AudioPacket>) -> FanoutReport {
        let mut report = FanoutReport::default();
        let now = Instant::now();
        for worker in &self.workers {
            let envelope = AudioEnvelope {
                enqueued_at: now,
                packet: Arc::clone(&packet),
            };
            match worker.audio_tx.try_send(envelope) {
                Ok(()) => {
                    worker
                        .metrics
                        .audio_enqueued
                        .fetch_add(1, Ordering::Relaxed);
                    report.accepted += 1;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    worker
                        .metrics
                        .audio_queue_dropped
                        .fetch_add(1, Ordering::Relaxed);
                    report.queue_dropped += 1;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    worker
                        .metrics
                        .audio_queue_dropped
                        .fetch_add(1, Ordering::Relaxed);
                    report.closed += 1;
                }
            }
        }
        report
    }

    pub(crate) fn statuses(&self) -> Vec<OutputStatusSnapshot> {
        self.workers
            .iter()
            .map(OutputWorkerHandle::status)
            .collect()
    }

    pub(crate) fn status_receiver(
        &self,
        output_id: &str,
    ) -> Option<watch::Receiver<OutputStateView>> {
        self.workers
            .iter()
            .find(|worker| worker.output_id.eq_ignore_ascii_case(output_id))
            .map(|worker| worker.state_rx.clone())
    }

    pub(crate) fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Signals all workers, then waits for their bounded teardown work.
    ///
    /// The task list is moved out of a short synchronous lock before awaiting,
    /// so worker shutdown cannot hold a lock across an await point. Repeated
    /// calls are harmless after the first caller drains the task list.
    pub(crate) async fn shutdown(&self) {
        self.request_shutdown();
        let tasks = self
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks))
            .unwrap_or_default();
        for task in tasks {
            let _ = task.await;
        }
    }
}

impl Drop for OutputFanout {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}

impl OutputWorkerHandle {
    fn status(&self) -> OutputStatusSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        OutputStatusSnapshot {
            output_id: self.output_id.clone(),
            destination_kind: self.destination_kind,
            state: self.state_rx.borrow().clone(),
            video_enqueued: load(&self.metrics.video_enqueued),
            video_written: load(&self.metrics.video_written),
            video_stale_dropped: load(&self.metrics.video_stale_dropped),
            video_age_dropped: load(&self.metrics.video_age_dropped),
            video_pts_dropped: load(&self.metrics.video_pts_dropped),
            video_sink_dropped: load(&self.metrics.video_sink_dropped),
            audio_enqueued: load(&self.metrics.audio_enqueued),
            audio_written: load(&self.metrics.audio_written),
            audio_queue_dropped: load(&self.metrics.audio_queue_dropped),
            audio_age_dropped: load(&self.metrics.audio_age_dropped),
            audio_pts_dropped: load(&self.metrics.audio_pts_dropped),
            audio_sink_dropped: load(&self.metrics.audio_sink_dropped),
            connect_attempts: load(&self.metrics.connect_attempts),
            reconnects: load(&self.metrics.reconnects),
            failures: load(&self.metrics.failures),
        }
    }
}

struct OutputWorkerContext {
    output: Arc<EncoderOutputSpec>,
    factory: Arc<dyn OutputSinkFactory>,
    config: OutputWorkerConfig,
    video_rx: watch::Receiver<Option<VideoEnvelope>>,
    audio_rx: mpsc::Receiver<AudioEnvelope>,
    shutdown_rx: watch::Receiver<bool>,
    state_tx: watch::Sender<OutputStateView>,
    metrics: Arc<WorkerMetrics>,
}

async fn run_output_worker(mut context: OutputWorkerContext) {
    let mut consecutive_failures = 0_u32;
    let mut connection_generation = 0_u64;
    let mut connected_once = false;
    let mut timestamps = TimestampTracker::default();
    let mut last_video_dispatch = 0_u64;

    loop {
        if shutdown_requested(&context.shutdown_rx) {
            set_state(
                &context.state_tx,
                OutputWorkerPhase::Stopped,
                connection_generation,
                consecutive_failures,
                None,
                None,
            );
            return;
        }

        set_state(
            &context.state_tx,
            OutputWorkerPhase::Connecting,
            connection_generation,
            consecutive_failures,
            None,
            None,
        );
        context
            .metrics
            .connect_attempts
            .fetch_add(1, Ordering::Relaxed);

        let mut sink = match context.factory.create(Arc::clone(&context.output)) {
            Ok(sink) => sink,
            Err(failure) => {
                if !handle_failure(
                    &mut context,
                    failure,
                    &mut consecutive_failures,
                    connection_generation,
                )
                .await
                {
                    return;
                }
                continue;
            }
        };

        let connect_result = tokio::select! {
            _ = wait_for_shutdown(&mut context.shutdown_rx) => {
                set_state(
                    &context.state_tx,
                    OutputWorkerPhase::Stopped,
                    connection_generation,
                    consecutive_failures,
                    None,
                    None,
                );
                return;
            }
            result = tokio::time::timeout(context.config.connect_timeout, sink.connect()) => {
                match result {
                    Ok(result) => result,
                    Err(_) => Err(OutputFailure::retryable(OutputFailureCode::ConnectTimeout)),
                }
            }
        };
        if let Err(failure) = connect_result {
            if !handle_failure(
                &mut context,
                failure,
                &mut consecutive_failures,
                connection_generation,
            )
            .await
            {
                return;
            }
            continue;
        }

        connection_generation = connection_generation.saturating_add(1);
        if connected_once {
            context.metrics.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        connected_once = true;
        set_state(
            &context.state_tx,
            OutputWorkerPhase::Online,
            connection_generation,
            consecutive_failures,
            None,
            None,
        );
        let connected_at = Instant::now();

        let exit = drive_connected(
            &mut context,
            sink.as_mut(),
            &mut timestamps,
            &mut last_video_dispatch,
        )
        .await;
        let _ = tokio::time::timeout(context.config.close_timeout, sink.close()).await;

        match exit {
            ConnectedExit::Shutdown => {
                set_state(
                    &context.state_tx,
                    OutputWorkerPhase::Stopped,
                    connection_generation,
                    consecutive_failures,
                    None,
                    None,
                );
                return;
            }
            ConnectedExit::Failure(failure) => {
                if connected_at.elapsed() >= context.config.backoff_reset_after {
                    consecutive_failures = 0;
                }
                if !handle_failure(
                    &mut context,
                    failure,
                    &mut consecutive_failures,
                    connection_generation,
                )
                .await
                {
                    return;
                }
            }
        }
    }
}

enum ConnectedExit {
    Shutdown,
    Failure(OutputFailure),
}

async fn drive_connected(
    context: &mut OutputWorkerContext,
    sink: &mut dyn OutputSink,
    timestamps: &mut TimestampTracker,
    last_video_dispatch: &mut u64,
) -> ConnectedExit {
    loop {
        tokio::select! {
            _ = wait_for_shutdown(&mut context.shutdown_rx) => return ConnectedExit::Shutdown,
            changed = context.video_rx.changed() => {
                if changed.is_err() {
                    return ConnectedExit::Shutdown;
                }
                let envelope = context.video_rx.borrow_and_update().clone();
                let Some(envelope) = envelope else {
                    continue;
                };
                if envelope.dispatch_sequence > last_video_dispatch.saturating_add(1) {
                    context.metrics.video_stale_dropped.fetch_add(
                        envelope.dispatch_sequence - last_video_dispatch.saturating_add(1),
                        Ordering::Relaxed,
                    );
                }
                *last_video_dispatch = envelope.dispatch_sequence;
                if envelope.enqueued_at.elapsed() > context.config.maximum_media_age {
                    context.metrics.video_age_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if !timestamps.video.accepts(envelope.frame.pts_ns, envelope.frame.discontinuity) {
                    context.metrics.video_pts_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                match controlled_video_write(context, sink, Arc::clone(&envelope.frame)).await {
                    ControlledWrite::Written => {
                        timestamps.video.commit(envelope.frame.pts_ns, envelope.frame.discontinuity);
                        context.metrics.video_written.fetch_add(1, Ordering::Relaxed);
                    }
                    ControlledWrite::Shutdown => return ConnectedExit::Shutdown,
                    ControlledWrite::Failure(failure) => {
                        context.metrics.video_sink_dropped.fetch_add(1, Ordering::Relaxed);
                        return ConnectedExit::Failure(failure);
                    }
                }
            }
            envelope = context.audio_rx.recv() => {
                let Some(envelope) = envelope else {
                    return ConnectedExit::Shutdown;
                };
                if envelope.enqueued_at.elapsed() > context.config.maximum_media_age {
                    context.metrics.audio_age_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                if !timestamps.audio.accepts(envelope.packet.pts_ns, envelope.packet.discontinuity) {
                    context.metrics.audio_pts_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                match controlled_audio_write(context, sink, Arc::clone(&envelope.packet)).await {
                    ControlledWrite::Written => {
                        timestamps.audio.commit(envelope.packet.pts_ns, envelope.packet.discontinuity);
                        context.metrics.audio_written.fetch_add(1, Ordering::Relaxed);
                    }
                    ControlledWrite::Shutdown => return ConnectedExit::Shutdown,
                    ControlledWrite::Failure(failure) => {
                        context.metrics.audio_sink_dropped.fetch_add(1, Ordering::Relaxed);
                        return ConnectedExit::Failure(failure);
                    }
                }
            }
        }
    }
}

enum ControlledWrite {
    Written,
    Shutdown,
    Failure(OutputFailure),
}

async fn controlled_video_write(
    context: &mut OutputWorkerContext,
    sink: &mut dyn OutputSink,
    frame: Arc<VideoFrame>,
) -> ControlledWrite {
    tokio::select! {
        _ = wait_for_shutdown(&mut context.shutdown_rx) => ControlledWrite::Shutdown,
        result = tokio::time::timeout(context.config.write_timeout, sink.write_video(frame)) => {
            match result {
                Ok(Ok(())) => ControlledWrite::Written,
                Ok(Err(failure)) => ControlledWrite::Failure(failure),
                Err(_) => ControlledWrite::Failure(OutputFailure::retryable(OutputFailureCode::WriteTimeout)),
            }
        }
    }
}

async fn controlled_audio_write(
    context: &mut OutputWorkerContext,
    sink: &mut dyn OutputSink,
    packet: Arc<AudioPacket>,
) -> ControlledWrite {
    tokio::select! {
        _ = wait_for_shutdown(&mut context.shutdown_rx) => ControlledWrite::Shutdown,
        result = tokio::time::timeout(context.config.write_timeout, sink.write_audio(packet)) => {
            match result {
                Ok(Ok(())) => ControlledWrite::Written,
                Ok(Err(failure)) => ControlledWrite::Failure(failure),
                Err(_) => ControlledWrite::Failure(OutputFailure::retryable(OutputFailureCode::WriteTimeout)),
            }
        }
    }
}

async fn handle_failure(
    context: &mut OutputWorkerContext,
    failure: OutputFailure,
    consecutive_failures: &mut u32,
    connection_generation: u64,
) -> bool {
    context.metrics.failures.fetch_add(1, Ordering::Relaxed);
    *consecutive_failures = consecutive_failures.saturating_add(1);
    if !failure.retryable {
        set_state(
            &context.state_tx,
            OutputWorkerPhase::Failed,
            connection_generation,
            *consecutive_failures,
            None,
            Some(failure.code),
        );
        return false;
    }

    let delay = retry_delay(
        context.config.initial_backoff,
        context.config.maximum_backoff,
        *consecutive_failures,
    );
    set_state(
        &context.state_tx,
        OutputWorkerPhase::Backoff,
        connection_generation,
        *consecutive_failures,
        Some(duration_millis(delay)),
        Some(failure.code),
    );
    tokio::select! {
        _ = tokio::time::sleep(delay) => true,
        _ = wait_for_shutdown(&mut context.shutdown_rx) => {
            set_state(
                &context.state_tx,
                OutputWorkerPhase::Stopped,
                connection_generation,
                *consecutive_failures,
                None,
                Some(failure.code),
            );
            false
        }
    }
}

fn retry_delay(initial: Duration, maximum: Duration, consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(31);
    initial.saturating_mul(1_u32 << exponent).min(maximum)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn shutdown_requested(shutdown_rx: &watch::Receiver<bool>) -> bool {
    *shutdown_rx.borrow()
}

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    while !*shutdown_rx.borrow() {
        if shutdown_rx.changed().await.is_err() {
            return;
        }
    }
}

fn set_state(
    state_tx: &watch::Sender<OutputStateView>,
    phase: OutputWorkerPhase,
    connection_generation: u64,
    consecutive_failures: u32,
    backoff_ms: Option<u64>,
    last_error: Option<OutputFailureCode>,
) {
    state_tx.send_replace(OutputStateView {
        phase,
        connection_generation,
        consecutive_failures,
        backoff_ms,
        last_error,
    });
}

#[derive(Debug, Default)]
struct TimestampTracker {
    video: StreamTimestamp,
    audio: StreamTimestamp,
}

#[derive(Debug, Default)]
struct StreamTimestamp {
    last_pts_ns: Option<u64>,
}

impl StreamTimestamp {
    fn accepts(&self, pts_ns: u64, discontinuity: bool) -> bool {
        discontinuity || self.last_pts_ns.is_none_or(|last| pts_ns >= last)
    }

    fn commit(&mut self, pts_ns: u64, _discontinuity: bool) {
        self.last_pts_ns = Some(pts_ns);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use crate::architecture::{AudioEncoderSpec, OutputId, RateControl, VideoEncoderSpec};

    #[derive(Debug)]
    struct TestSurface;

    impl VideoFrameHandle for TestSurface {
        fn backend(&self) -> &'static str {
            "test_gpu"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug, Default)]
    struct SinkState {
        video_pts: Mutex<Vec<u64>>,
        audio_pts: Mutex<Vec<u64>>,
        connect_count: AtomicUsize,
        video_failures_remaining: AtomicUsize,
        video_delay_ms: AtomicU64,
    }

    struct TestFactory {
        states: BTreeMap<String, Arc<SinkState>>,
    }

    struct RejectTestDestination;

    impl OutputCompatibilityValidator for RejectTestDestination {
        fn validate(&self, _output: &EncoderOutputSpec) -> Result<(), OutputCompatibilityError> {
            Err(OutputCompatibilityError::Rejected(
                "test destination is unavailable",
            ))
        }
    }

    impl OutputSinkFactory for TestFactory {
        fn create(
            &self,
            output: Arc<EncoderOutputSpec>,
        ) -> Result<Box<dyn OutputSink>, OutputFailure> {
            let state = self
                .states
                .get(output.id.as_str())
                .cloned()
                .ok_or_else(|| OutputFailure::terminal(OutputFailureCode::Factory))?;
            Ok(Box::new(TestSink { state }))
        }
    }

    struct TestSink {
        state: Arc<SinkState>,
    }

    impl OutputSink for TestSink {
        fn connect(&mut self) -> SinkFuture<'_, ()> {
            Box::pin(async move {
                self.state.connect_count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }

        fn write_video(&mut self, frame: Arc<VideoFrame>) -> SinkFuture<'_, ()> {
            Box::pin(async move {
                let delay = self.state.video_delay_ms.load(Ordering::Relaxed);
                if delay > 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                if self
                    .state
                    .video_failures_remaining
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    return Err(OutputFailure::retryable(OutputFailureCode::Network));
                }
                self.state
                    .video_pts
                    .lock()
                    .expect("video PTS lock")
                    .push(frame.pts_ns);
                Ok(())
            })
        }

        fn write_audio(&mut self, packet: Arc<AudioPacket>) -> SinkFuture<'_, ()> {
            Box::pin(async move {
                let _ = packet.payload.sample_count();
                self.state
                    .audio_pts
                    .lock()
                    .expect("audio PTS lock")
                    .push(packet.pts_ns);
                Ok(())
            })
        }

        fn close(&mut self) -> SinkFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    fn output(id: &str, destination: OutputDestination) -> EncoderOutputSpec {
        EncoderOutputSpec {
            id: OutputId::parse(id).expect("valid output ID"),
            enabled: true,
            destination,
            video: VideoEncoderSpec {
                codec: VideoCodec::H264,
                rate_control: RateControl::Cbr {
                    bitrate_kbps: NonZeroU32::new(4_000).expect("non-zero"),
                },
                gop_frames: NonZeroU32::new(30).expect("non-zero"),
                scan: crate::architecture::ScanMode::Progressive,
            },
            audio: AudioEncoderSpec {
                codec: AudioCodecPolicy::Encode(AudioCodec::Aac),
                bitrate_kbps: NonZeroU32::new(192).expect("non-zero"),
                sample_rate: NonZeroU32::new(48_000).expect("non-zero"),
            },
        }
    }

    fn rtmp_output(id: &str) -> EncoderOutputSpec {
        output(
            id,
            OutputDestination::Rtmp {
                location: "${TEST_SECRET_OUTPUT}".to_string(),
            },
        )
    }

    fn frame(sequence: u64, pts_ns: u64) -> Arc<VideoFrame> {
        Arc::new(VideoFrame {
            sequence,
            pts_ns,
            duration_ns: 33_366_667,
            discontinuity: false,
            width: NonZeroU32::new(1_920).expect("non-zero"),
            height: NonZeroU32::new(1_080).expect("non-zero"),
            surface: Arc::new(TestSurface),
        })
    }

    fn audio(sequence: u64, pts_ns: u64) -> Arc<AudioPacket> {
        Arc::new(AudioPacket {
            sequence,
            pts_ns,
            duration_ns: 20_000_000,
            discontinuity: false,
            sample_rate: NonZeroU32::new(48_000).expect("non-zero"),
            channels: NonZeroU16::new(2).expect("non-zero"),
            payload: AudioPayload::InterleavedF32(Arc::from([0.0_f32; 16])),
        })
    }

    fn test_config() -> OutputWorkerConfig {
        OutputWorkerConfig {
            audio_queue_capacity: 4,
            maximum_media_age: Duration::from_secs(2),
            connect_timeout: Duration::from_millis(100),
            write_timeout: Duration::from_millis(500),
            close_timeout: Duration::from_millis(100),
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(8),
            backoff_reset_after: Duration::from_secs(1),
        }
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if predicate() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("condition completed before timeout");
    }

    #[tokio::test]
    async fn repeatedly_failing_output_does_not_stall_healthy_output() {
        let healthy = Arc::new(SinkState::default());
        let flaky = Arc::new(SinkState::default());
        flaky.video_failures_remaining.store(8, Ordering::Relaxed);
        let factory = Arc::new(TestFactory {
            states: BTreeMap::from([
                ("healthy".to_string(), Arc::clone(&healthy)),
                ("flaky".to_string(), Arc::clone(&flaky)),
            ]),
        });
        let fanout = OutputFanout::spawn(
            [rtmp_output("healthy"), rtmp_output("flaky")],
            factory,
            test_config(),
        )
        .expect("valid fanout");

        wait_until(|| {
            fanout
                .statuses()
                .iter()
                .all(|status| status.state.phase == OutputWorkerPhase::Online)
        })
        .await;

        for sequence in 1..=120 {
            let report = fanout.publish_video(frame(sequence, sequence * 33_366_667));
            assert_eq!(report.accepted + report.closed, 2);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        wait_until(|| healthy.video_pts.lock().expect("healthy video lock").len() >= 80).await;
        wait_until(|| {
            fanout
                .statuses()
                .iter()
                .find(|status| status.output_id == "flaky")
                .is_some_and(|status| status.failures >= 8 && status.reconnects >= 8)
        })
        .await;
        let statuses = fanout.statuses();
        let healthy_status = statuses
            .iter()
            .find(|status| status.output_id == "healthy")
            .expect("healthy status");
        let flaky_status = statuses
            .iter()
            .find(|status| status.output_id == "flaky")
            .expect("flaky status");
        assert!(healthy_status.video_written >= 80);
        assert_eq!(healthy_status.failures, 0);
        assert!(flaky_status.failures >= 8);
        assert!(flaky_status.reconnects >= 8);
        assert!(healthy.video_pts.lock().expect("healthy video lock").len() >= 80);
        fanout.shutdown().await;
    }

    #[tokio::test]
    async fn slow_output_keeps_only_latest_video_frame() {
        let slow = Arc::new(SinkState::default());
        slow.video_delay_ms.store(25, Ordering::Relaxed);
        let factory = Arc::new(TestFactory {
            states: BTreeMap::from([("slow".to_string(), slow)]),
        });
        let fanout = OutputFanout::spawn([rtmp_output("slow")], factory, test_config())
            .expect("valid fanout");
        wait_until(|| fanout.statuses()[0].state.phase == OutputWorkerPhase::Online).await;

        for sequence in 1..=30 {
            fanout.publish_video(frame(sequence, sequence * 33_366_667));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        wait_until(|| fanout.statuses()[0].video_stale_dropped > 0).await;
        let statuses = fanout.statuses();
        let status = &statuses[0];
        assert!(status.video_written < status.video_enqueued);
        assert!(status.video_stale_dropped > 0);
        fanout.shutdown().await;
    }

    #[tokio::test]
    async fn regressing_pts_is_dropped_without_affecting_other_media() {
        let state = Arc::new(SinkState::default());
        let factory = Arc::new(TestFactory {
            states: BTreeMap::from([("primary".to_string(), Arc::clone(&state))]),
        });
        let fanout = OutputFanout::spawn([rtmp_output("primary")], factory, test_config())
            .expect("valid fanout");
        wait_until(|| fanout.statuses()[0].state.phase == OutputWorkerPhase::Online).await;

        fanout.publish_audio(audio(1, 100));
        wait_until(|| fanout.statuses()[0].audio_written == 1).await;
        fanout.publish_audio(audio(2, 90));
        fanout.publish_audio(audio(3, 110));
        wait_until(|| fanout.statuses()[0].audio_written == 2).await;

        assert_eq!(
            state.audio_pts.lock().expect("audio PTS lock").as_slice(),
            &[100, 110]
        );
        assert_eq!(fanout.statuses()[0].audio_pts_dropped, 1);
        fanout.shutdown().await;
    }

    #[tokio::test]
    async fn full_audio_queue_drops_only_the_slow_destination() {
        let healthy = Arc::new(SinkState::default());
        let slow = Arc::new(SinkState::default());
        slow.video_delay_ms.store(100, Ordering::Relaxed);
        let factory = Arc::new(TestFactory {
            states: BTreeMap::from([
                ("healthy".to_string(), Arc::clone(&healthy)),
                ("slow".to_string(), Arc::clone(&slow)),
            ]),
        });
        let mut config = test_config();
        config.audio_queue_capacity = 1;
        let fanout = OutputFanout::spawn(
            [rtmp_output("healthy"), rtmp_output("slow")],
            factory,
            config,
        )
        .expect("valid fanout");
        wait_until(|| {
            fanout
                .statuses()
                .iter()
                .all(|status| status.state.phase == OutputWorkerPhase::Online)
        })
        .await;

        fanout.publish_video(frame(1, 1));
        tokio::time::sleep(Duration::from_millis(2)).await;
        for sequence in 1..=20 {
            fanout.publish_audio(audio(sequence, sequence * 20_000_000));
            tokio::task::yield_now().await;
        }
        wait_until(|| healthy.audio_pts.lock().expect("healthy audio lock").len() > 1).await;
        let statuses = fanout.statuses();
        let healthy_status = statuses
            .iter()
            .find(|status| status.output_id == "healthy")
            .expect("healthy status");
        let slow_status = statuses
            .iter()
            .find(|status| status.output_id == "slow")
            .expect("slow status");
        assert!(healthy_status.audio_written > slow_status.audio_written);
        assert!(slow_status.audio_queue_dropped > 0);
        fanout.shutdown().await;
    }

    #[tokio::test]
    async fn status_never_contains_destination_location() {
        let state = Arc::new(SinkState::default());
        let factory = Arc::new(TestFactory {
            states: BTreeMap::from([("primary".to_string(), state)]),
        });
        let fanout = OutputFanout::spawn([rtmp_output("primary")], factory, test_config())
            .expect("valid fanout");
        let status = fanout.statuses()[0].status_value().to_string();
        assert!(!status.contains("TEST_SECRET_OUTPUT"));
        assert!(!status.contains("${"));
        fanout.shutdown().await;
        fanout.shutdown().await;
    }

    #[tokio::test]
    async fn incompatible_rtmp_profile_fails_before_worker_activation() {
        let state = Arc::new(SinkState::default());
        let factory = Arc::new(TestFactory {
            states: BTreeMap::from([("primary".to_string(), state)]),
        });
        let mut output = rtmp_output("primary");
        output.video.codec = VideoCodec::H265;
        let result = OutputFanout::spawn([output], factory, test_config());
        assert!(matches!(
            result,
            Err(OutputRuntimeError::Incompatible {
                source: OutputCompatibilityError::InvalidRtmpCodecs,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn deployment_compatibility_hook_runs_before_worker_activation() {
        let state = Arc::new(SinkState::default());
        let factory = Arc::new(TestFactory {
            states: BTreeMap::from([("primary".to_string(), state)]),
        });
        let result = OutputFanout::spawn_with_validator(
            [rtmp_output("primary")],
            factory,
            Arc::new(RejectTestDestination),
            test_config(),
        );
        assert!(matches!(
            result,
            Err(OutputRuntimeError::Incompatible {
                source: OutputCompatibilityError::Rejected(_),
                ..
            })
        ));
    }

    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        let initial = Duration::from_millis(250);
        let maximum = Duration::from_secs(2);
        assert_eq!(retry_delay(initial, maximum, 1), Duration::from_millis(250));
        assert_eq!(retry_delay(initial, maximum, 2), Duration::from_millis(500));
        assert_eq!(retry_delay(initial, maximum, 3), Duration::from_secs(1));
        assert_eq!(retry_delay(initial, maximum, 4), Duration::from_secs(2));
        assert_eq!(retry_delay(initial, maximum, 40), Duration::from_secs(2));
    }
}
