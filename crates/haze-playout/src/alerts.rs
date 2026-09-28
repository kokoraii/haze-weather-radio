use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use haze_cap::model::{Alert, AlertInfo};
use haze_media::{decode_wav, normalize_pcm};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Notify};
use tokio::time::{sleep, timeout};

use crate::bridge::{string_at, BridgeClient, SynthJob};
use crate::config::{
    AlertFilterConfig, AlertFilterListConfig, FeedAlertProviderConfig, FeedConfig,
};

const ROUTER_STATE_PATH: &str = "runtime/state/cap-alert-router.json";
const ALERT_AUDIO_DIR: &str = "runtime/audio/alerts";
const ALERT_QUEUE_DIR: &str = "runtime/queues/alerts";
const ROUTER_SUBSCRIPTIONS: &[&str] = &[
    "cap.alert.received",
    "cap.alert.playlist.dispatch.acknowledged",
    "cap.alert.playlist.dispatch.failed",
    "cap.alert.cancellation.applied",
    "alert.playout.started",
    "playout.started",
];

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DispatchKind {
    Priority,
    Routine,
    Cancellation,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct AlertDispatch {
    pub(crate) decision_id: String,
    #[serde(default)]
    pub(crate) sequence: u64,
    #[serde(default)]
    pub(crate) ready_sent: bool,
    #[serde(default)]
    request_sent: bool,
    #[serde(default)]
    cancellation_applied: bool,
    #[serde(default)]
    audio_path: String,
    pub(crate) delivery_id: String,
    pub(crate) alert_id: String,
    pub(crate) parent_alert_id: String,
    pub(crate) info_group_id: String,
    pub(crate) feed_id: String,
    pub(crate) kind: DispatchKind,
    pub(crate) alert: Alert,
    pub(crate) info: AlertInfo,
    pub(crate) locations: Vec<String>,
    pub(crate) newly_active_locations: Vec<String>,
    #[serde(default)]
    pub(crate) cancelled_alert_ids: Vec<String>,
    pub(crate) same_event: String,
    pub(crate) same_locations: Vec<String>,
    pub(crate) include_same: bool,
    pub(crate) title: String,
    pub(crate) text: String,
    pub(crate) language: String,
    pub(crate) received_at: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub(crate) struct RouterState {
    #[serde(default)]
    pub(crate) processed_delivery_ids: HashSet<String>,
    #[serde(default)]
    pub(crate) active: BTreeMap<String, Vec<ActiveProduct>>,
    #[serde(default)]
    pub(crate) outbox: BTreeMap<String, AlertDispatch>,
    #[serde(default)]
    next_sequence: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ActiveProduct {
    parent_alert_id: String,
    alert_id: String,
    #[serde(default)]
    lineage_ids: Vec<String>,
    info_group_id: String,
    hazard_key: String,
    locations: Vec<String>,
    #[serde(default)]
    expires_at: String,
    same_locations: Vec<String>,
    title: String,
    text: String,
}

pub(crate) struct AlertPipeline {
    state_path: PathBuf,
    state: Mutex<RouterState>,
    changed: Notify,
}

#[derive(Debug)]
pub(crate) struct RoutingResult {
    pub(crate) duplicate: bool,
    pub(crate) dispatches: Vec<AlertDispatch>,
}

impl AlertPipeline {
    pub(crate) fn load(base_dir: &Path) -> Result<Self> {
        let state_path = base_dir.join(ROUTER_STATE_PATH);
        let state = match std::fs::read(&state_path) {
            Ok(raw) => serde_json::from_slice(&raw).with_context(|| {
                format!(
                    "failed to parse alert router state {}",
                    state_path.display()
                )
            })?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => RouterState::default(),
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to read alert router state {}", state_path.display())
                })
            }
        };
        Ok(Self {
            state_path,
            state: Mutex::new(state),
            changed: Notify::new(),
        })
    }

    /// Commits routing decisions and their pending dispatches before intake is acknowledged.
    pub(crate) async fn route(&self, event: &Value, feeds: &[FeedConfig]) -> Result<RoutingResult> {
        let data = event.get("data").unwrap_or(&Value::Null);
        let delivery_id = first_text(event, data, &["delivery_id"]).to_string();
        if delivery_id.is_empty() {
            anyhow::bail!("cap.alert.received is missing delivery_id");
        }
        let alert: Alert =
            serde_json::from_value(data.get("alert").cloned().unwrap_or(Value::Null))
                .context("failed to decode parsed CAP document")?;
        if alert.identifier.trim().is_empty() {
            anyhow::bail!("CAP document has no identifier");
        }
        let received_at = first_text(event, data, &["timestamp", "received_at"]);
        let received_at = if received_at.is_empty() {
            Utc::now().to_rfc3339()
        } else {
            received_at.to_string()
        };
        let mut state = self.state.lock().await;
        if state.processed_delivery_ids.contains(&delivery_id) {
            return Ok(RoutingResult {
                duplicate: true,
                dispatches: Vec::new(),
            });
        }
        let mut next_state = state.clone();
        let mut dispatches =
            route_document(&alert, &delivery_id, &received_at, feeds, &mut next_state);
        next_state.processed_delivery_ids.insert(delivery_id);
        for dispatch in &mut dispatches {
            next_state.next_sequence = next_state.next_sequence.saturating_add(1);
            dispatch.sequence = next_state.next_sequence;
            next_state
                .outbox
                .entry(dispatch.decision_id.clone())
                .or_insert_with(|| dispatch.clone());
        }
        self.persist_locked(&next_state).await?;
        *state = next_state;
        drop(state);
        self.changed.notify_waiters();
        Ok(RoutingResult {
            duplicate: false,
            dispatches,
        })
    }

    pub(crate) async fn pending_dispatches(&self) -> Vec<AlertDispatch> {
        self.state.lock().await.outbox.values().cloned().collect()
    }

    pub(crate) async fn next_dispatch_for_feed(
        &self,
        feed_id: &str,
        urgent_lane: bool,
    ) -> Option<AlertDispatch> {
        self.state
            .lock()
            .await
            .outbox
            .values()
            .filter(|dispatch| {
                dispatch.feed_id == feed_id
                    && if urgent_lane {
                        dispatch.kind == DispatchKind::Priority
                            || (dispatch.kind == DispatchKind::Cancellation
                                && !dispatch.cancellation_applied)
                    } else {
                        dispatch.kind == DispatchKind::Routine
                            || (dispatch.kind == DispatchKind::Cancellation
                                && dispatch.cancellation_applied)
                    }
            })
            .min_by_key(|dispatch| {
                let priority = urgent_lane && dispatch.kind == DispatchKind::Cancellation;
                (if priority { 0 } else { 1 }, dispatch.sequence)
            })
            .cloned()
    }

    pub(crate) async fn is_pending(&self, decision_id: &str) -> bool {
        self.state.lock().await.outbox.contains_key(decision_id)
    }

    pub(crate) async fn mark_ready_sent(&self, decision_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next_state = state.clone();
        if let Some(dispatch) = next_state.outbox.get_mut(decision_id) {
            dispatch.ready_sent = true;
            self.persist_locked(&next_state).await?;
            *state = next_state;
        }
        Ok(())
    }

    pub(crate) async fn mark_request_sent(&self, decision_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next_state = state.clone();
        if let Some(dispatch) = next_state.outbox.get_mut(decision_id) {
            dispatch.request_sent = true;
            self.persist_locked(&next_state).await?;
            *state = next_state;
        }
        Ok(())
    }

    pub(crate) async fn mark_cancellation_applied(&self, decision_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next_state = state.clone();
        if let Some(dispatch) = next_state.outbox.get_mut(decision_id) {
            dispatch.cancellation_applied = true;
            self.persist_locked(&next_state).await?;
            *state = next_state;
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn wait_cancellation_applied(&self, decision_id: &str) -> Result<()> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let state = self.state.lock().await;
            match state.outbox.get(decision_id) {
                Some(dispatch) if dispatch.cancellation_applied => return Ok(()),
                Some(dispatch) if dispatch.kind == DispatchKind::Cancellation => {}
                Some(_) => anyhow::bail!("dispatch {decision_id} is not a cancellation"),
                None => return Ok(()),
            }
            drop(state);
            notified.await;
        }
    }

    pub(crate) async fn mark_prepared(&self, decision_id: &str, audio_path: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next_state = state.clone();
        if let Some(dispatch) = next_state.outbox.get_mut(decision_id) {
            dispatch.audio_path = audio_path.to_string();
            self.persist_locked(&next_state).await?;
            *state = next_state;
        }
        Ok(())
    }

    pub(crate) async fn retry_dispatch(&self, decision_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next_state = state.clone();
        if let Some(dispatch) = next_state.outbox.get_mut(decision_id) {
            dispatch.ready_sent = false;
            if dispatch.kind == DispatchKind::Cancellation && !dispatch.cancellation_applied {
                dispatch.request_sent = false;
            }
            self.persist_locked(&next_state).await?;
            *state = next_state;
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn retry_cancellation_request(&self, decision_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next_state = state.clone();
        if let Some(dispatch) = next_state.outbox.get_mut(decision_id) {
            if dispatch.kind == DispatchKind::Cancellation && !dispatch.cancellation_applied {
                dispatch.request_sent = false;
                self.persist_locked(&next_state).await?;
                *state = next_state;
            }
        }
        Ok(())
    }

    pub(crate) async fn mark_dispatched(&self, decision_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        let mut next_state = state.clone();
        next_state.outbox.remove(decision_id);
        self.persist_locked(&next_state).await?;
        *state = next_state;
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn persist_locked(&self, state: &RouterState) -> Result<()> {
        let parent = self
            .state_path
            .parent()
            .expect("router state path has a parent");
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed to create alert state directory {}",
                parent.display()
            )
        })?;
        let temporary = self.state_path.with_extension("json.tmp");
        let raw = serde_json::to_vec(state).context("failed to serialize alert router state")?;
        let mut file = tokio::fs::File::create(&temporary).await.with_context(|| {
            format!("failed to create alert state file {}", temporary.display())
        })?;
        use tokio::io::AsyncWriteExt;
        file.write_all(&raw).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temporary, &self.state_path)
            .await
            .with_context(|| {
                format!(
                    "failed to commit alert router state {}",
                    self.state_path.display()
                )
            })
    }
}

pub(crate) async fn connect_alert_router(addr: &str) -> Result<crate::bridge::BridgeConnection> {
    crate::bridge::connect_consumer_retry(addr, "haze-cap-alert-router", ROUTER_SUBSCRIPTIONS).await
}

pub(crate) async fn run_router(
    pipeline: std::sync::Arc<AlertPipeline>,
    cfg: std::sync::Arc<crate::config::LoadedConfig>,
    addr: String,
    cancellation: tokio_util::sync::CancellationToken,
) {
    loop {
        let connection = tokio::select! {
            _ = cancellation.cancelled() => break,
            connection = connect_alert_router(&addr) => connection,
        };
        let Ok(connection) = connection else {
            sleep(StdDuration::from_secs(1)).await;
            continue;
        };
        let crate::bridge::BridgeConnection {
            client,
            events,
            reader_task,
        } = connection;
        let session = run_router_session(
            std::sync::Arc::clone(&pipeline),
            client,
            events,
            std::sync::Arc::clone(&cfg),
        );
        tokio::pin!(session);
        tokio::select! {
            _ = cancellation.cancelled() => {}
            _ = &mut session => {}
        }
        reader_task.abort();
        let _ = reader_task.await;
        if !cancellation.is_cancelled() {
            sleep(StdDuration::from_secs(1)).await;
        }
    }
}

async fn run_router_session(
    pipeline: std::sync::Arc<AlertPipeline>,
    client: BridgeClient,
    mut events: tokio::sync::mpsc::Receiver<Value>,
    cfg: std::sync::Arc<crate::config::LoadedConfig>,
) {
    let enabled_feeds = cfg
        .enabled_feeds()
        .map(|feed| feed.id.clone())
        .collect::<Vec<_>>();
    let mut feed_workers = tokio::task::JoinSet::new();
    for feed_id in enabled_feeds {
        for urgent_lane in [true, false] {
            feed_workers.spawn(run_feed_dispatcher(
                feed_id.clone(),
                urgent_lane,
                std::sync::Arc::clone(&pipeline),
                client.clone(),
                std::sync::Arc::clone(&cfg),
            ));
        }
    }

    while let Some(event) = events.recv().await {
        match string_at(&event, "type") {
            "cap.alert.received" => {
                let data = event.get("data").unwrap_or(&Value::Null);
                let delivery_id = first_text(&event, data, &["delivery_id"]);
                let _ = client
                    .publish(stage_timing_event("receipt", delivery_id, &event, 0))
                    .await;
                match pipeline.route(&event, &cfg.feeds).await {
                    Ok(result) => {
                        let source_id = data
                            .get("ingest")
                            .and_then(|ingest| ingest.get("source_id"))
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let _ = client
                            .publish(json!({
                                "type": "cap.alert.router.acknowledged",
                                "source": "haze-cap-alert-router",
                                "target": format!("haze-cap-ingest:{source_id}"),
                                "subject": delivery_id,
                                "data": {
                                    "delivery_id": delivery_id,
                                    "status": "committed",
                                    "duplicate": result.duplicate,
                                }
                            }))
                            .await;
                        let _ = client
                            .publish(stage_timing_event(
                                "route_decided",
                                delivery_id,
                                &event,
                                result.dispatches.len(),
                            ))
                            .await;
                    }
                    Err(err) => tracing::error!("CAP router could not commit delivery: {err:#}"),
                }
            }
            "cap.alert.playlist.dispatch.acknowledged" => {
                let data = event.get("data").unwrap_or(&Value::Null);
                let decision_id = first_text(&event, data, &["decision_id"]);
                if !decision_id.is_empty() {
                    if let Err(err) = pipeline.mark_dispatched(decision_id).await {
                        tracing::error!(%decision_id, "failed to commit playlist acknowledgement: {err:#}");
                    }
                }
            }
            "cap.alert.playlist.dispatch.failed" => {
                let data = event.get("data").unwrap_or(&Value::Null);
                let decision_id = first_text(&event, data, &["decision_id"]);
                if !decision_id.is_empty() {
                    if let Err(err) = pipeline.retry_dispatch(decision_id).await {
                        tracing::error!(%decision_id, "failed to requeue alert dispatch: {err:#}");
                    }
                }
            }
            "cap.alert.cancellation.applied" => {
                let data = event.get("data").unwrap_or(&Value::Null);
                let decision_id = first_text(&event, data, &["decision_id"]);
                if !decision_id.is_empty() {
                    if let Err(err) = pipeline.mark_cancellation_applied(decision_id).await {
                        tracing::error!(%decision_id, "failed to persist CAP cancellation acknowledgement: {err:#}");
                    }
                }
            }
            "alert.playout.started" | "playout.started" => {
                let data = event.get("data").unwrap_or(&Value::Null);
                let decision_id = first_text(&event, data, &["decision_id"]);
                if !decision_id.is_empty() {
                    let delivery_id = first_text(&event, data, &["delivery_id"]);
                    let _ = client
                        .publish(stage_timing_event(
                            "playout_started",
                            delivery_id,
                            &event,
                            0,
                        ))
                        .await;
                }
            }
            _ => {}
        }
    }
    feed_workers.abort_all();
}

async fn run_feed_dispatcher(
    feed_id: String,
    urgent_lane: bool,
    pipeline: std::sync::Arc<AlertPipeline>,
    client: BridgeClient,
    cfg: std::sync::Arc<crate::config::LoadedConfig>,
) {
    loop {
        let changed = pipeline.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let Some(dispatch) = pipeline.next_dispatch_for_feed(&feed_id, urgent_lane).await else {
            changed.await;
            continue;
        };
        if dispatch.ready_sent {
            let notified = pipeline.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if timeout(StdDuration::from_secs(15), &mut notified)
                .await
                .is_ok()
            {
                continue;
            }
            if let Err(err) = pipeline.retry_dispatch(&dispatch.decision_id).await {
                tracing::error!(
                    decision_id = dispatch.decision_id,
                    "failed to retry stalled alert dispatch: {err:#}"
                );
            }
            continue;
        }
        if let Err(err) = prepare_dispatch(&dispatch, &pipeline, &client, &cfg).await {
            tracing::warn!(
                feed_id,
                decision_id = dispatch.decision_id,
                "alert preparation failed, retrying: {err:#}"
            );
            sleep(StdDuration::from_secs(1)).await;
        }
    }
}

async fn prepare_dispatch(
    dispatch: &AlertDispatch,
    pipeline: &AlertPipeline,
    client: &BridgeClient,
    cfg: &crate::config::LoadedConfig,
) -> Result<()> {
    if !pipeline.is_pending(&dispatch.decision_id).await {
        return Ok(());
    }
    if dispatch_expired(dispatch) {
        pipeline.mark_dispatched(&dispatch.decision_id).await?;
        return Ok(());
    }
    if !dispatch.request_sent {
        client.publish(dispatch_request_event(dispatch)).await?;
        client
            .publish(stage_timing_dispatch("dispatch", dispatch))
            .await?;
        pipeline.mark_request_sent(&dispatch.decision_id).await?;
    }

    if dispatch.kind == DispatchKind::Cancellation && !dispatch.cancellation_applied {
        match timeout(
            StdDuration::from_secs(5),
            pipeline.wait_cancellation_applied(&dispatch.decision_id),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                pipeline
                    .retry_cancellation_request(&dispatch.decision_id)
                    .await?;
                anyhow::bail!("timed out waiting for CAP cancellation retraction");
            }
        }
        return Ok(());
    }

    let prepared_path = cfg.base_dir.join(&dispatch.audio_path);
    let relative_audio_path = if !dispatch.audio_path.is_empty()
        && tokio::fs::try_exists(&prepared_path).await.unwrap_or(false)
    {
        dispatch.audio_path.clone()
    } else {
        let output_dir = cfg.base_dir.join(ALERT_AUDIO_DIR);
        tokio::fs::create_dir_all(&output_dir)
            .await
            .with_context(|| {
                format!(
                    "failed to create alert audio directory {}",
                    output_dir.display()
                )
            })?;
        let speech_output_path = output_dir.join(format!("{}_speech.wav", dispatch.decision_id));
        let speech_job = SynthJob {
            id: format!("{}-speech", dispatch.decision_id),
            text: dispatch.text.clone(),
            reader_id: cfg.reader_id("alerts"),
            language: dispatch.language.clone(),
            output_path: speech_output_path,
        };
        let speech = client.synthesize(speech_job);
        let same = async {
            if !dispatch.include_same {
                return Ok(None);
            }
            let feed = cfg
                .feeds
                .iter()
                .find(|feed| feed.id == dispatch.feed_id)
                .context("feed disappeared while preparing alert")?;
            let header = same_header(dispatch, feed)?;
            let tone = same_tone(feed, &cfg.root.same, &dispatch.same_event);
            let audio = tokio::task::spawn_blocking(move || {
                haze_same::generate_same_header_sequence(&header, tone).to_pcm16le()
            })
            .await
            .context("SAME generation task failed")?;
            Ok::<Option<Vec<u8>>, anyhow::Error>(Some(audio))
        };
        let (speech_result, same_result) = tokio::join!(speech, same);
        let speech_path = PathBuf::from(speech_result.context("alert speech synthesis failed")?);
        let same_audio = same_result?;
        if !pipeline.is_pending(&dispatch.decision_id).await {
            return Ok(());
        }
        if dispatch_expired(dispatch) {
            pipeline.mark_dispatched(&dispatch.decision_id).await?;
            return Ok(());
        }
        let speech_bytes = tokio::fs::read(&speech_path)
            .await
            .with_context(|| format!("failed to read alert speech {}", speech_path.display()))?;
        let speech_pcm = normalize_pcm(decode_wav(&speech_bytes)?, 48_000, 1);
        let audio_path = if let Some(mut same_audio) = same_audio {
            same_audio.extend_from_slice(&speech_pcm.data);
            let path = output_dir.join(format!("{}_complete.raw", dispatch.decision_id));
            write_atomic(&path, &same_audio).await?;
            write_priority_manifest(dispatch, &path, cfg).await?;
            path
        } else {
            speech_path
        };
        let relative_audio_path = audio_path
            .strip_prefix(&cfg.base_dir)
            .unwrap_or(&audio_path)
            .to_string_lossy()
            .replace('\\', "/");
        pipeline
            .mark_prepared(&dispatch.decision_id, &relative_audio_path)
            .await?;
        relative_audio_path
    };
    let feed = cfg
        .feeds
        .iter()
        .find(|feed| feed.id == dispatch.feed_id)
        .context("feed disappeared while preparing alert")?;
    let ready_event = match dispatch.kind {
        DispatchKind::Priority => json!({
            "type": "cap.alert.audio.ready",
            "source": "haze-cap-alert-router",
            "subject": dispatch.parent_alert_id,
            "feed_id": dispatch.feed_id,
            "feed_ids": [dispatch.feed_id],
            "decision_id": dispatch.decision_id,
            "delivery_id": dispatch.delivery_id,
            "parent_alert_id": dispatch.parent_alert_id,
            "alert_id": dispatch.alert_id,
            "info_group_id": dispatch.info_group_id,
            "data": dispatch_audio_data(dispatch, relative_audio_path, true, feed, &cfg.root.same),
        }),
        DispatchKind::Routine | DispatchKind::Cancellation => json!({
            "type": "playlist.item.ready",
            "source": "haze-cap-alert-router",
            "subject": dispatch.parent_alert_id,
            "feed_id": dispatch.feed_id,
            "decision_id": dispatch.decision_id,
            "delivery_id": dispatch.delivery_id,
            "parent_alert_id": dispatch.parent_alert_id,
            "alert_id": dispatch.alert_id,
            "info_group_id": dispatch.info_group_id,
            "data": {
                "feed_id": dispatch.feed_id,
                "package_id": "alerts",
                "queue_id": dispatch.decision_id,
                "decision_id": dispatch.decision_id,
                "delivery_id": dispatch.delivery_id,
                "parent_alert_id": dispatch.parent_alert_id,
                "alert_id": dispatch.alert_id,
                "info_group_id": dispatch.info_group_id,
                "title": dispatch.title,
                "audio_path": relative_audio_path,
                "alert_text": dispatch.text,
                "alert_sent_at": dispatch.alert.sent,
                "alert_expires_at": dispatch.info.expires,
                "received_at": dispatch.received_at,
                "message_type": dispatch.alert.message_type,
            },
        }),
    };
    if !pipeline.is_pending(&dispatch.decision_id).await {
        return Ok(());
    }
    client.publish(ready_event).await?;
    client
        .publish(stage_timing_dispatch("audio_ready", dispatch))
        .await?;
    pipeline.mark_ready_sent(&dispatch.decision_id).await
}

fn dispatch_expired(dispatch: &AlertDispatch) -> bool {
    dispatch.kind != DispatchKind::Cancellation
        && parse_cap_time(&dispatch.info.expires).is_some_and(|expires| expires <= Utc::now())
}

fn dispatch_request_event(dispatch: &AlertDispatch) -> Value {
    let identity = json!({
        "decision_id": dispatch.decision_id,
        "delivery_id": dispatch.delivery_id,
        "alert_id": dispatch.alert_id,
        "parent_alert_id": dispatch.parent_alert_id,
        "info_group_id": dispatch.info_group_id,
        "feed_id": dispatch.feed_id,
        "feed_ids": [dispatch.feed_id],
    });
    let mut data = identity;
    data["title"] = json!(dispatch.title);
    data["header"] = json!(dispatch.title);
    data["alert_text"] = json!(dispatch.text);
    data["locations"] = json!(dispatch.locations);
    data["newly_active_locations"] = json!(dispatch.newly_active_locations);
    data["severity"] = json!(dispatch.info.severity);
    data["urgency"] = json!(dispatch.info.urgency);
    data["certainty"] = json!(dispatch.info.certainty);
    data["description"] = json!(dispatch.info.description);
    data["instruction"] = json!(dispatch.info.instruction);
    data["same_event"] = json!(dispatch.same_event);
    data["same_locations"] = json!(dispatch.same_locations);
    data["include_same"] = json!(dispatch.include_same);
    data["message_type"] = json!(dispatch.alert.message_type);
    data["alert_sent_at"] = json!(dispatch.alert.sent);
    data["alert_expires_at"] = json!(dispatch.info.expires);
    data["received_at"] = json!(dispatch.received_at);
    data["alert_packet"] = json!({
        "id": dispatch.alert_id,
        "feed_id": dispatch.feed_id,
        "title": dispatch.title,
        "event": dispatch.same_event,
        "severity": dispatch.info.severity,
        "urgency": dispatch.info.urgency,
        "certainty": dispatch.info.certainty,
        "description": dispatch.info.description,
        "instruction": dispatch.info.instruction,
        "locations": dispatch.locations,
    });
    match dispatch.kind {
        DispatchKind::Priority => json!({
            "type": "cap.alert.broadcast.requested",
            "source": "haze-cap-alert-router",
            "subject": dispatch.parent_alert_id,
            "feed_id": dispatch.feed_id,
            "data": data,
        }),
        DispatchKind::Routine => json!({
            "type": "cap.alert.routine.requested",
            "source": "haze-cap-alert-router",
            "subject": dispatch.parent_alert_id,
            "feed_id": dispatch.feed_id,
            "data": data,
        }),
        DispatchKind::Cancellation => {
            let mut cancelled_ids = BTreeSet::from([dispatch.parent_alert_id.clone()]);
            cancelled_ids.extend(dispatch.cancelled_alert_ids.iter().cloned());
            data["alert_ids"] = json!(cancelled_ids);
            json!({
                "type": "cap.alert.cancelled",
                "source": "haze-cap-alert-router",
                "subject": dispatch.parent_alert_id,
                "feed_id": dispatch.feed_id,
                "data": data,
            })
        }
    }
}

fn dispatch_audio_data(
    dispatch: &AlertDispatch,
    audio_path: String,
    include_same: bool,
    feed: &crate::config::FeedConfig,
    same_defaults: &crate::config::SameConfig,
) -> Value {
    json!({
        "feed_id": dispatch.feed_id,
        "feed_ids": [dispatch.feed_id],
        "queue_id": dispatch.decision_id,
        "decision_id": dispatch.decision_id,
        "delivery_id": dispatch.delivery_id,
        "parent_alert_id": dispatch.parent_alert_id,
        "alert_id": dispatch.alert_id,
        "info_group_id": dispatch.info_group_id,
        "title": dispatch.title,
        "header": dispatch.title,
        "event": dispatch.same_event,
        "same_event": dispatch.same_event,
        "same_locations": dispatch.same_locations,
        "include_same": include_same,
        "same_originator": feed.playout.same_originator.as_deref().filter(|value| !value.trim().is_empty()).unwrap_or("WXR"),
        "same_tone": same_tone_name(feed, same_defaults, &dispatch.same_event),
        "same_duration": same_duration(&dispatch.info.expires),
        "audio_path": audio_path,
        "sample_rate": 48000,
        "channels": 1,
        "alert_text": dispatch.text,
        "alert_packet": {
            "id": dispatch.alert_id,
            "feed_id": dispatch.feed_id,
            "title": dispatch.title,
            "event": dispatch.same_event,
            "severity": dispatch.info.severity,
            "urgency": dispatch.info.urgency,
            "certainty": dispatch.info.certainty,
        },
        "alert_sent_at": dispatch.alert.sent,
        "alert_expires_at": dispatch.info.expires,
        "message_type": dispatch.alert.message_type,
        "received_at": dispatch.received_at,
        "broadcast_immediate": true,
    })
}

fn same_header(
    dispatch: &AlertDispatch,
    feed: &crate::config::FeedConfig,
) -> Result<haze_same::SameHeader> {
    let sent = parse_cap_time(&dispatch.alert.sent).unwrap_or_else(Utc::now);
    let issue_time = format!(
        "{:03}{:02}{:02}",
        sent.ordinal(),
        sent.hour(),
        sent.minute()
    );
    let originator = feed
        .playout
        .same_originator
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("WXR")
        .trim()
        .to_ascii_uppercase();
    haze_same::SameHeader::new(
        originator,
        dispatch.same_event.clone(),
        dispatch.same_locations.clone(),
        same_duration(&dispatch.info.expires),
        nonempty(&feed.station_callsign(), "", "HAZE"),
        issue_time,
    )
    .context("failed to build CAP SAME header")
}

fn same_tone(
    feed: &crate::config::FeedConfig,
    defaults: &crate::config::SameConfig,
    event: &str,
) -> Option<haze_same::ToneType> {
    match same_tone_name(feed, defaults, event).as_str() {
        "NONE" | "OFF" => None,
        "EAS" => Some(haze_same::ToneType::Eas),
        "EGG_TIMER" | "EGG-TIMER" | "EGG" => Some(haze_same::ToneType::EggTimer),
        "QUEBEC" | "QC" => Some(haze_same::ToneType::Quebec),
        _ => Some(haze_same::ToneType::Wxr),
    }
}

fn same_tone_name(
    feed: &crate::config::FeedConfig,
    defaults: &crate::config::SameConfig,
    event: &str,
) -> String {
    feed.playout
        .same_attention_tone
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            defaults
                .attention_tone_override
                .iter()
                .find_map(|overrides| {
                    overrides
                        .iter()
                        .find(|(code, _)| code.eq_ignore_ascii_case(event))
                        .map(|(_, tone)| tone.as_str())
                })
        })
        .or_else(|| {
            (!defaults.default_attention_tone.trim().is_empty())
                .then_some(defaults.default_attention_tone.as_str())
        })
        .unwrap_or("WXR")
        .trim()
        .to_ascii_uppercase()
}

fn same_duration(expires: &str) -> String {
    let minutes = parse_cap_time(expires)
        .map(|expires| (expires - Utc::now()).num_minutes().clamp(15, 3 * 60))
        .unwrap_or(15);
    format!("{:02}{:02}", minutes / 60, minutes % 60)
}

async fn write_priority_manifest(
    dispatch: &AlertDispatch,
    audio_path: &Path,
    cfg: &crate::config::LoadedConfig,
) -> Result<()> {
    let queue_dir = cfg.base_dir.join(ALERT_QUEUE_DIR);
    tokio::fs::create_dir_all(&queue_dir)
        .await
        .with_context(|| {
            format!(
                "failed to create alert queue directory {}",
                queue_dir.display()
            )
        })?;
    let relative_audio = audio_path
        .strip_prefix(&cfg.base_dir)
        .unwrap_or(audio_path)
        .to_string_lossy()
        .replace('\\', "/");
    let manifest_path = queue_dir.join(format!("{}.json", dispatch.decision_id));
    let value = json!({
        "id": dispatch.decision_id,
        "decision_id": dispatch.decision_id,
        "delivery_id": dispatch.delivery_id,
        "alert_id": dispatch.alert_id,
        "parent_alert_id": dispatch.parent_alert_id,
        "received_at": dispatch.received_at,
        "info_group_id": dispatch.info_group_id,
        "type": "cap_alert",
        "priority": "same",
        "source": "rust-cap-alert-router",
        "created_at": Utc::now().to_rfc3339(),
        "status": "pending",
        "feed_id": dispatch.feed_id,
        "feed_ids": [dispatch.feed_id],
        "header": dispatch.title,
        "event": dispatch.same_event,
        "alert_text": dispatch.text,
        "broadcast_immediate": true,
        "alert_sent_at": dispatch.alert.sent,
        "alert_expires_at": dispatch.info.expires,
        "message_type": dispatch.alert.message_type,
        "audio_path": relative_audio,
        "sample_rate": 48000,
        "channels": 1,
    });
    write_atomic(&manifest_path, &serde_json::to_vec_pretty(&value)?).await
}

async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().expect("alert output path has a parent");
    tokio::fs::create_dir_all(parent).await?;
    let temporary = path.with_extension("tmp");
    let mut file = tokio::fs::File::create(&temporary).await?;
    file.write_all(bytes).await?;
    file.sync_all().await?;
    tokio::fs::rename(&temporary, path)
        .await
        .with_context(|| format!("failed to commit alert output {}", path.display()))
}

fn stage_timing_event(
    stage: &str,
    delivery_id: &str,
    event: &Value,
    dispatch_count: usize,
) -> Value {
    let data = event.get("data").unwrap_or(&Value::Null);
    let received_at = parse_cap_time(first_text(event, data, &["received_at", "timestamp"]))
        .unwrap_or_else(Utc::now);
    json!({
        "type": "cap.alert.stage.timing",
        "source": "haze-cap-alert-router",
        "delivery_id": delivery_id,
        "alert_id": first_text(event, data, &["alert_id"]),
        "stage": stage,
        "received_at": received_at.to_rfc3339(),
        "at": Utc::now().to_rfc3339(),
        "dispatch_count": dispatch_count,
    })
}

fn stage_timing_dispatch(stage: &str, dispatch: &AlertDispatch) -> Value {
    json!({
        "type": "cap.alert.stage.timing",
        "source": "haze-cap-alert-router",
        "delivery_id": dispatch.delivery_id,
        "decision_id": dispatch.decision_id,
        "alert_id": dispatch.alert_id,
        "feed_id": dispatch.feed_id,
        "stage": stage,
        "received_at": dispatch.received_at,
        "at": Utc::now().to_rfc3339(),
    })
}

mod routing;

use routing::{first_text, nonempty, parse_cap_time, route_document};
