mod ancillary;
mod architecture;
mod atomic_file;
mod audio_routing;
mod bridge;
mod config;
mod graphics;
mod gst_backend;
mod gst_output_sink;
mod input_program_map;
mod media_pcm;
mod migration;
mod output_workers;
mod pipeline;
// The SCTE-35 section encoder is retained behind explicit output capability gates.
#[allow(dead_code)]
mod program_mapping;
mod scene;
mod scene_layout;
mod scene_runtime;
mod source_caps;
mod state;
mod sunny_cat;
mod wgpu_renderer;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

const SOURCE_ID: &str = "haze-cgen";
const QUEUE_ACTIVE_GRACE_SECS: i64 = 120;
const QUEUE_ACTIVE_MAX_SECS: i64 = 900;
const MAX_QUEUE_MANIFEST_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "haze-cgen", about = "Haze managed character generator service")]
struct Args {
    #[arg(long, default_value = "config.yaml")]
    config: PathBuf,
    #[arg(long, default_value = "managed/configs/cgen.xml")]
    cgen: PathBuf,
    #[arg(long, env = "HAZE_HOST_BRIDGE_ADDR")]
    bridge: Option<String>,
    #[arg(long, env = "HAZE_MEDIA_BRIDGE_ADDR")]
    media_bridge: Option<String>,
    #[arg(long)]
    gst_catalog: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Explicitly migrate managed CGEN configuration to schema version 2.
    MigrateConfig {
        #[arg(long, conflicts_with = "apply")]
        dry_run: bool,
        #[arg(long, conflicts_with = "dry_run")]
        apply: bool,
    },
    /// Validate managed CGEN configuration without connecting to the event bridge.
    ValidateConfig {
        /// Also initialize WGPU and preflight the installed GStreamer runtime.
        #[arg(long)]
        runtime: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "haze_cgen=info,warn".into()),
        )
        .init();

    let args = Args::parse();
    if args.gst_catalog {
        let catalog = gst_backend::gstreamer_catalog_json()?;
        println!("{}", serde_json::to_string(&catalog)?);
        return Ok(());
    }
    let base_dir = args
        .config
        .parent()
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| PathBuf::from("."));
    let cgen_path = config::resolve_path(&base_dir, &args.cgen);
    if let Some(command) = &args.command {
        match command {
            Command::MigrateConfig { dry_run, apply } => {
                if !*dry_run && !*apply {
                    anyhow::bail!("migrate-config requires either --dry-run or --apply");
                }
                let mode = if *apply {
                    migration::MigrationMode::Apply
                } else {
                    migration::MigrationMode::DryRun
                };
                let outcome = migration::migrate_config(&cgen_path, &base_dir, mode)?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&migration_report_value(&outcome.report))?
                );
                return Ok(());
            }
            Command::ValidateConfig { runtime } => {
                let report = validate_config_report(&cgen_path, &base_dir, *runtime)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
                return Ok(());
            }
        }
    }

    let bridge_addr = args
        .bridge
        .as_deref()
        .context("--bridge or HAZE_HOST_BRIDGE_ADDR is required")?;
    spawn_parent_watcher();

    let scene_directory = base_dir.join("managed").join("cgen").join("scenes");
    let scene_catalog = Arc::new(scene::load_scene_directory(&scene_directory));
    for warning in scene_catalog.warnings() {
        warn!(
            scene_id = warning.scene_id.as_ref().map(ToString::to_string),
            "cgen scene catalog degraded: {}", warning.message
        );
    }
    let root = config::load_config(&cgen_path)
        .with_context(|| format!("failed to load cgen config {}", cgen_path.display()))?;
    let feeds = root.enabled_feeds()?;
    let media_pcm_hub = media_pcm::MediaPcmHub::new();
    let media_bridge_configured = args
        .media_bridge
        .as_deref()
        .is_some_and(|addr| !addr.trim().is_empty());
    info!(
        feeds = feeds.len(),
        config = %cgen_path.display(),
        "haze-cgen config loaded"
    );

    let bridge = bridge::connect_retry(bridge_addr).await?;
    bridge
        .client
        .publish(json!({
            "type": "bridge.client",
            "source": SOURCE_ID,
            "data": { "receive_events": true }
        }))
        .await?;
    bridge
        .client
        .publish(json!({
            "type": "service.ready",
            "source": SOURCE_ID,
            "data": {
                "service": SOURCE_ID,
                "feeds": feeds.len(),
                "config": cgen_path,
                "media_pipeline": "gstreamer-rs",
                "sunny_cat_available": sunny_cat::available(),
                "scene_count": scene_catalog.scenes().count(),
                "scene_catalog_degraded": scene_catalog.is_degraded(),
                "scene_warning_count": scene_catalog.warnings().len(),
                "media_bridge_configured": media_bridge_configured
            }
        }))
        .await?;

    let (state_tx, state_rx) = watch::channel(state::RuntimeState::default());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    if let Some(media_bridge_addr) = args
        .media_bridge
        .as_deref()
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
    {
        tokio::spawn(media_pcm::run_bridge(
            media_bridge_addr.to_string(),
            media_pcm_hub.clone(),
            shutdown_rx.clone(),
        ));
    } else {
        warn!("HAZE_MEDIA_BRIDGE_ADDR is not configured; CGEN alert audio will remain silent");
    }
    let (queue_tx, queue_rx) = mpsc::channel::<serde_json::Value>(64);
    tokio::spawn(active_queue_poll_loop(
        base_dir.clone(),
        queue_tx,
        shutdown_rx.clone(),
    ));
    for feed in feeds {
        let worker = pipeline::PipelineWorker::new(
            feed,
            state_rx.clone(),
            shutdown_rx.clone(),
            base_dir.clone(),
            Some(bridge.client.clone()),
            Arc::clone(&scene_catalog),
            media_pcm_hub.clone(),
        );
        tokio::spawn(async move {
            if let Err(err) = worker.run().await {
                warn!("cgen feed worker exited: {err}");
            }
        });
    }

    run_event_loop(bridge.events, queue_rx, state_tx, shutdown_tx).await
}

fn migration_report_value(report: &migration::MigrationReport) -> serde_json::Value {
    json!({
        "source_schema_version": report.source_schema_version,
        "target_schema_version": report.target_schema_version,
        "config_changed": report.config_changed,
        "feeds_examined": report.feeds_examined,
        "feeds_changed": report.feeds_changed,
        "alert_routes_added": report.alert_routes_added,
        "alert_routes_normalized": report.alert_routes_normalized,
        "ancillary_sections_added": report.ancillary_sections_added,
        "ancillary_sections_augmented": report.ancillary_sections_augmented,
        "compositor_sections_added": report.compositor_sections_added,
        "compositor_sections_augmented": report.compositor_sections_augmented,
        "audio_sections_added": report.audio_sections_added,
        "audio_sections_augmented": report.audio_sections_augmented,
        "protected_scenes_missing_before_apply": report.protected_scenes_missing_before_apply,
        "protected_scenes_seeded": report.protected_scenes_seeded,
        "backup_created": report.backup_path.is_some(),
        "backup_file": report.backup_path.as_ref().and_then(|path| path.file_name()).and_then(|name| name.to_str()),
    })
}

fn validate_config_report(cgen_path: &Path, base_dir: &Path, runtime: bool) -> Result<Value> {
    let scene_directory = base_dir.join("managed").join("cgen").join("scenes");
    let scene_catalog = scene::load_scene_directory(&scene_directory);
    let root = config::load_config(cgen_path)
        .with_context(|| format!("failed to load cgen config {}", cgen_path.display()))?;
    let feeds = root.enabled_feeds()?;
    let mut feed_reports = Vec::with_capacity(feeds.len());
    for feed in feeds {
        let spec = feed.pipeline_spec()?;
        let alert_scene = scene_catalog
            .alert_scene(&spec.compositor.alert_scene_id)
            .with_context(|| {
                format!(
                    "cgen feed {} selects unavailable alert scene {}",
                    feed.id, spec.compositor.alert_scene_id
                )
            })?;
        let runtime_report = runtime
            .then(|| gst_backend::preflight_feed(&feed, base_dir))
            .transpose()?;
        feed_reports.push(json!({
            "feed_id": spec.feed_id.as_str(),
            "input_kind": spec.input.kind(),
            "alert_feed_id": spec.alert_feed_id.as_str(),
            "alert_scene_id": alert_scene.id.as_str(),
            "outputs": spec.outputs.iter().filter(|output| output.enabled).map(|output| json!({
                "output_id": output.id.as_str(),
                "destination": output.destination.kind(),
            })).collect::<Vec<_>>(),
            "runtime": runtime_report,
        }));
    }
    let warnings = scene_catalog
        .warnings()
        .iter()
        .map(|warning| {
            json!({
                "scene_id": warning.scene_id.as_ref().map(ToString::to_string),
                "message": warning.message,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "ok": true,
        "schema_version": root.schema_version,
        "enabled": root.enabled,
        "enabled_feed_count": feed_reports.len(),
        "runtime_checked": runtime,
        "gpu_wgpu_compiled": cfg!(feature = "gpu-wgpu"),
        "scene_count": scene_catalog.scenes().count(),
        "scene_catalog_degraded": scene_catalog.is_degraded(),
        "scene_warnings": warnings,
        "feeds": feed_reports,
    }))
}

async fn run_event_loop(
    mut events: tokio::sync::mpsc::Receiver<serde_json::Value>,
    mut queue_events: tokio::sync::mpsc::Receiver<serde_json::Value>,
    state_tx: watch::Sender<state::RuntimeState>,
    shutdown_tx: watch::Sender<bool>,
) -> Result<()> {
    let mut runtime = state::RuntimeState::default();
    loop {
        tokio::select! {
            event = events.recv() => {
                let Some(event) = event else {
                    let _ = shutdown_tx.send(true);
                    anyhow::bail!("event bridge closed");
                };
                if runtime.apply_event(&event) {
                    let _ = state_tx.send(runtime.clone());
                } else {
                    match event.get("type").and_then(serde_json::Value::as_str) {
                        Some("cgen.config.updated") => {
                            info!("cgen config update received; exiting for daemon restart");
                            let _ = shutdown_tx.send(true);
                            return Ok(());
                        }
                        Some("cgen.scenes.updated") => {
                            info!("cgen scene update received; exiting for daemon restart");
                            let _ = shutdown_tx.send(true);
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
            event = queue_events.recv() => {
                if let Some(event) = event {
                    if runtime.apply_event(&event) {
                        let _ = state_tx.send(runtime.clone());
                    }
                }
            }
            result = tokio::signal::ctrl_c() => {
                result.context("failed waiting for Ctrl-C")?;
                info!("haze-cgen shutting down");
                let _ = shutdown_tx.send(true);
                return Ok(());
            }
        }
    }
}

async fn active_queue_poll_loop(
    base_dir: PathBuf,
    tx: mpsc::Sender<serde_json::Value>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut active = BTreeMap::<String, Vec<String>>::new();
    let mut cache = ActiveQueueSnapshotCache::default();
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    return;
                }
                continue;
            }
        }
        let base = base_dir.clone();
        let scan_cache = std::mem::take(&mut cache);
        let snapshot = match tokio::task::spawn_blocking(move || {
            let mut cache = scan_cache;
            let snapshot = cache.scan(&base);
            (cache, snapshot)
        })
        .await
        {
            Ok((next_cache, Ok(snapshot))) => {
                cache = next_cache;
                snapshot
            }
            Ok((next_cache, Err(err))) => {
                cache = next_cache;
                warn!("failed to scan cgen active alert queues: {err:#}");
                continue;
            }
            Err(err) => {
                warn!("cgen active alert queue scanner panicked: {err}");
                cache = ActiveQueueSnapshotCache::default();
                continue;
            }
        };
        let current = snapshot
            .iter()
            .map(|item| item.queue_id.clone())
            .collect::<BTreeSet<_>>();
        for item in &snapshot {
            if !active.contains_key(&item.queue_id) {
                active.insert(item.queue_id.clone(), item.feed_ids.clone());
                if tx.send(item.started_event()).await.is_err() {
                    return;
                }
            } else {
                active.insert(item.queue_id.clone(), item.feed_ids.clone());
            }
        }
        let completed = active
            .keys()
            .filter(|queue_id| !current.contains(*queue_id))
            .cloned()
            .collect::<Vec<String>>();
        for queue_id in completed {
            let feed_ids = active
                .remove(&queue_id)
                .unwrap_or_else(|| vec!["*".to_string()]);
            if tx
                .send(queue_completed_event(&queue_id, &feed_ids))
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ActiveQueueItem {
    queue_id: String,
    feed_ids: Vec<String>,
    data: serde_json::Value,
}

impl ActiveQueueItem {
    fn started_event(&self) -> serde_json::Value {
        json!({
            "type": "alert.playout.started",
            "source": "haze-cgen-queue-watch",
            "feed_ids": self.feed_ids,
            "queue_id": self.queue_id,
            "data": self.data,
        })
    }
}

fn queue_completed_event(queue_id: &str, feed_ids: &[String]) -> serde_json::Value {
    json!({
        "type": "alert.playout.completed",
        "source": "haze-cgen-queue-watch",
        "feed_ids": feed_ids,
        "queue_id": queue_id,
        "data": { "queue_id": queue_id },
    })
}

#[cfg(test)]
fn active_queue_snapshot(base_dir: &std::path::Path) -> Result<Vec<ActiveQueueItem>> {
    ActiveQueueSnapshotCache::default().scan(base_dir)
}

#[derive(Debug, Clone, Default)]
struct ActiveQueueSnapshotCache {
    entries: BTreeMap<PathBuf, CachedQueueManifest>,
}

#[derive(Debug, Clone)]
struct CachedQueueManifest {
    fingerprint: QueueManifestFingerprint,
    item: Option<ActiveQueueItem>,
    active_until: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueueManifestFingerprint {
    len: u64,
    modified: Option<SystemTime>,
}

impl ActiveQueueSnapshotCache {
    fn scan(&mut self, base_dir: &Path) -> Result<Vec<ActiveQueueItem>> {
        let dir = base_dir.join("runtime").join("queues").join("alerts");
        let Ok(entries) = std::fs::read_dir(&dir) else {
            self.entries.clear();
            return Ok(Vec::new());
        };
        let now = chrono::Utc::now();
        let mut seen = BTreeSet::<PathBuf>::new();
        let mut out = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err).context("failed to read alert queue directory entry"),
            };
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            seen.insert(path.clone());
            let fingerprint = match queue_manifest_fingerprint(&path) {
                Ok(fingerprint) => fingerprint,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    self.entries.remove(&path);
                    continue;
                }
                Err(err) => {
                    warn!(
                        queue = %path.display(),
                        "ignoring unsafe or unreadable cgen alert queue manifest: {err}"
                    );
                    self.entries.remove(&path);
                    continue;
                }
            };
            let item = if let Some(cached) = self
                .entries
                .get_mut(&path)
                .filter(|cached| cached.fingerprint == fingerprint)
            {
                if cached
                    .active_until
                    .is_some_and(|expires_at| now > expires_at)
                {
                    cached.item = None;
                    cached.active_until = None;
                }
                cached.item.clone()
            } else {
                let parsed = match parse_active_queue_manifest(&path, now) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        warn!(
                            queue = %path.display(),
                            "ignoring unreadable cgen alert queue manifest: {err:#}"
                        );
                        ParsedQueueManifest {
                            item: None,
                            active_until: None,
                        }
                    }
                };
                self.entries.insert(
                    path.clone(),
                    CachedQueueManifest {
                        fingerprint,
                        item: parsed.item.clone(),
                        active_until: parsed.active_until,
                    },
                );
                parsed.item
            };
            if let Some(item) = item {
                out.push(item);
            }
        }
        self.entries.retain(|path, _| seen.contains(path));
        out.sort_by(|a, b| a.queue_id.cmp(&b.queue_id));
        Ok(out)
    }
}

fn queue_manifest_fingerprint(path: &Path) -> std::io::Result<QueueManifestFingerprint> {
    let metadata = std::fs::symlink_metadata(path)?;
    validate_queue_manifest_metadata(&metadata)?;
    Ok(QueueManifestFingerprint {
        len: metadata.len(),
        modified: metadata.modified().ok(),
    })
}

fn validate_queue_manifest_metadata(metadata: &std::fs::Metadata) -> io::Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "queue manifest must be a regular file",
        ));
    }
    if metadata.len() > MAX_QUEUE_MANIFEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("queue manifest exceeds the {MAX_QUEUE_MANIFEST_BYTES} byte limit"),
        ));
    }
    Ok(())
}

fn read_queue_manifest(path: &Path) -> io::Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)?;
    validate_queue_manifest_metadata(&metadata)?;
    let mut file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "queue manifest must remain a regular file while being read",
        ));
    }
    let initial_capacity = usize::try_from(metadata.len())
        .unwrap_or(0)
        .min(MAX_QUEUE_MANIFEST_BYTES as usize);
    let mut raw = Vec::with_capacity(initial_capacity);
    Read::by_ref(&mut file)
        .take(MAX_QUEUE_MANIFEST_BYTES + 1)
        .read_to_end(&mut raw)?;
    if raw.len() as u64 > MAX_QUEUE_MANIFEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("queue manifest exceeds the {MAX_QUEUE_MANIFEST_BYTES} byte limit"),
        ));
    }
    Ok(raw)
}

#[derive(Debug, Clone)]
struct ParsedQueueManifest {
    item: Option<ActiveQueueItem>,
    active_until: Option<chrono::DateTime<chrono::Utc>>,
}

fn parse_active_queue_manifest(
    path: &Path,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<ParsedQueueManifest> {
    let raw = match read_queue_manifest(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ParsedQueueManifest {
                item: None,
                active_until: None,
            })
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to read alert queue {}", path.display()))
        }
    };
    let value: serde_json::Value = match serde_json::from_slice(&raw) {
        Ok(value) => value,
        Err(err) => {
            warn!(
                queue = %path.display(),
                "ignoring unreadable cgen alert queue manifest: {err}"
            );
            return Ok(ParsedQueueManifest {
                item: None,
                active_until: None,
            });
        }
    };
    let Some(active_until) = queue_active_until(&value).filter(|active_until| now <= *active_until)
    else {
        return Ok(ParsedQueueManifest {
            item: None,
            active_until: None,
        });
    };
    if !queue_status_allows_active(&value)
        || !has_non_empty_field(&value, "claimed_at")
        || has_non_empty_field(&value, "played_at")
        || has_non_empty_field(&value, "failed_at")
    {
        return Ok(ParsedQueueManifest {
            item: None,
            active_until: None,
        });
    }
    let queue_id = text_field(&value, "id")
        .or_else(|| text_field(&value, "queue_id"))
        .unwrap_or_default();
    if queue_id.is_empty() {
        return Ok(ParsedQueueManifest {
            item: None,
            active_until: None,
        });
    }
    let feed_ids = queue_feed_ids(&value);
    if feed_ids.is_empty() {
        return Ok(ParsedQueueManifest {
            item: None,
            active_until: None,
        });
    }
    Ok(ParsedQueueManifest {
        item: Some(ActiveQueueItem {
            queue_id,
            feed_ids,
            data: queue_event_data(value),
        }),
        active_until: Some(active_until),
    })
}

fn queue_status_allows_active(value: &serde_json::Value) -> bool {
    match text_field(value, "status")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "played" | "failed" | "superseded" | "cancelled" | "canceled" => false,
        _ => true,
    }
}

fn queue_active_until(value: &serde_json::Value) -> Option<chrono::DateTime<chrono::Utc>> {
    let Some(claimed_at) = text_field(value, "claimed_at")
        .and_then(|text| chrono::DateTime::parse_from_rfc3339(&text).ok())
        .map(|time| time.with_timezone(&chrono::Utc))
    else {
        return None;
    };
    let max_age = chrono::Duration::seconds(queue_active_limit_secs(value));
    Some(claimed_at + max_age)
}

fn queue_active_limit_secs(value: &serde_json::Value) -> i64 {
    let calculated = value
        .get("audio")
        .and_then(audio_duration_secs)
        .or_else(|| {
            value
                .get("alert_packet")
                .and_then(|packet| packet.get("audio"))
                .and_then(audio_duration_secs)
        })
        .unwrap_or(0)
        .saturating_add(QUEUE_ACTIVE_GRACE_SECS);
    calculated.clamp(QUEUE_ACTIVE_GRACE_SECS, QUEUE_ACTIVE_MAX_SECS)
}

fn audio_duration_secs(value: &serde_json::Value) -> Option<i64> {
    let bytes = value.get("bytes").and_then(serde_json::Value::as_u64)?;
    let sample_rate = value
        .get("sample_rate")
        .and_then(serde_json::Value::as_u64)
        .filter(|value| *value > 0)?;
    let channels = value
        .get("channels")
        .and_then(serde_json::Value::as_u64)
        .filter(|value| *value > 0)?;
    let bytes_per_second = sample_rate.saturating_mul(channels).saturating_mul(2);
    if bytes_per_second == 0 {
        return None;
    }
    Some(i64::try_from(bytes.div_ceil(bytes_per_second)).unwrap_or(QUEUE_ACTIVE_MAX_SECS))
}

fn queue_feed_ids(value: &serde_json::Value) -> Vec<String> {
    let mut out = std::collections::BTreeSet::<String>::new();
    if let Some(feed_id) = text_field(value, "feed_id") {
        if !feed_id.is_empty() {
            out.insert(feed_id);
        }
    }
    if let Some(values) = value.get("feed_ids").and_then(serde_json::Value::as_array) {
        for value in values {
            if let Some(feed_id) = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                out.insert(feed_id.to_string());
            }
        }
    }
    if let Some(packet) = value.get("alert_packet") {
        if let Some(feed_id) = text_field(packet, "feed_id") {
            if !feed_id.is_empty() {
                out.insert(feed_id);
            }
        }
        if let Some(values) = packet.get("feed_ids").and_then(serde_json::Value::as_array) {
            for value in values {
                if let Some(feed_id) = value
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    out.insert(feed_id.to_string());
                }
            }
        }
    }
    out.into_iter().collect()
}

fn queue_event_data(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(object) = value.as_object_mut() {
        let queue_id = object
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if let Some(queue_id) = queue_id {
            object.insert("queue_id".to_string(), json!(queue_id));
        }
    }
    value
}

fn has_non_empty_field(value: &serde_json::Value, field: &str) -> bool {
    match value.get(field) {
        Some(serde_json::Value::String(text)) => !text.trim().is_empty(),
        Some(serde_json::Value::Null) | None => false,
        Some(_) => true,
    }
}

fn text_field(value: &serde_json::Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn spawn_parent_watcher() {
    let Ok(raw) = std::env::var("HAZE_PARENT_PID") else {
        return;
    };
    let Ok(parent_pid) = raw.parse::<u32>() else {
        return;
    };
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            if !process_alive(parent_pid) {
                warn!("parent process exited; haze-cgen exiting");
                std::process::exit(0);
            }
        }
    });
}

#[cfg(windows)]
fn process_alive(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, FALSE};
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION};

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
        if handle.is_null() {
            return false;
        }
        CloseHandle(handle);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn active_queue_snapshot_converts_claimed_item_to_started_event() {
        let dir = tempfile::tempdir().expect("tempdir");
        let queue_dir = dir.path().join("runtime").join("queues").join("alerts");
        std::fs::create_dir_all(&queue_dir).expect("queue dir");
        std::fs::write(
            queue_dir.join("q1.json"),
            serde_json::to_vec(&json!({
                "id": "q1",
                "feed_ids": ["CAP-IT-ALL"],
                "claimed_at": chrono::Utc::now().to_rfc3339(),
                "played_at": null,
                "failed_at": null,
                "audio_path": "runtime/audio/alerts/q1.pcm16le",
                "sample_rate": 48000,
                "channels": 1,
                "banner_text": "Test crawl"
            }))
            .expect("queue json"),
        )
        .expect("write queue");

        let snapshot = active_queue_snapshot(dir.path()).expect("snapshot");
        assert_eq!(snapshot.len(), 1);
        let event = snapshot[0].started_event();
        assert_eq!(event["type"], "alert.playout.started");
        assert_eq!(event["queue_id"], "q1");
        assert_eq!(event["feed_ids"][0], "CAP-IT-ALL");
        assert_eq!(event["data"]["queue_id"], "q1");
        assert_eq!(event["data"]["banner_text"], "Test crawl");
    }

    #[test]
    fn active_queue_snapshot_ignores_completed_items() {
        let dir = tempfile::tempdir().expect("tempdir");
        let queue_dir = dir.path().join("runtime").join("queues").join("alerts");
        std::fs::create_dir_all(&queue_dir).expect("queue dir");
        std::fs::write(
            queue_dir.join("q1.json"),
            serde_json::to_vec(&json!({
                "id": "q1",
                "feed_ids": ["CAP-IT-ALL"],
                "claimed_at": chrono::Utc::now().to_rfc3339(),
                "played_at": "2026-06-24T00:00:30Z",
                "failed_at": null
            }))
            .expect("queue json"),
        )
        .expect("write queue");

        let snapshot = active_queue_snapshot(dir.path()).expect("snapshot");
        assert!(snapshot.is_empty());
    }

    #[test]
    fn active_queue_snapshot_ignores_failed_status_without_failed_at() {
        let dir = tempfile::tempdir().expect("tempdir");
        let queue_dir = dir.path().join("runtime").join("queues").join("alerts");
        std::fs::create_dir_all(&queue_dir).expect("queue dir");
        std::fs::write(
            queue_dir.join("q1.json"),
            serde_json::to_vec(&json!({
                "id": "q1",
                "feed_ids": ["CAP-IT-ALL"],
                "status": "failed",
                "claimed_at": chrono::Utc::now().to_rfc3339(),
                "played_at": null,
                "failed_at": null
            }))
            .expect("queue json"),
        )
        .expect("write queue");

        let snapshot = active_queue_snapshot(dir.path()).expect("snapshot");
        assert!(snapshot.is_empty());
    }

    #[test]
    fn active_queue_snapshot_skips_unreadable_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let queue_dir = dir.path().join("runtime").join("queues").join("alerts");
        std::fs::create_dir_all(&queue_dir).expect("queue dir");
        std::fs::write(queue_dir.join("bad.json"), b"{").expect("bad queue");
        std::fs::write(
            queue_dir.join("good.json"),
            serde_json::to_vec(&json!({
                "id": "good",
                "feed_ids": ["CAP-IT-ALL"],
                "status": "playing",
                "claimed_at": chrono::Utc::now().to_rfc3339(),
                "audio_path": "runtime/audio/alerts/good.pcm16le"
            }))
            .expect("queue json"),
        )
        .expect("write good queue");

        let snapshot = active_queue_snapshot(dir.path()).expect("snapshot");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].queue_id, "good");
    }

    #[test]
    fn active_queue_snapshot_isolates_an_oversized_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let queue_dir = dir.path().join("runtime").join("queues").join("alerts");
        std::fs::create_dir_all(&queue_dir).expect("queue dir");
        let oversized = File::create(queue_dir.join("oversized.json")).expect("oversized queue");
        oversized
            .set_len(MAX_QUEUE_MANIFEST_BYTES + 1)
            .expect("oversized queue length");
        std::fs::write(
            queue_dir.join("good.json"),
            serde_json::to_vec(&json!({
                "id": "good",
                "feed_ids": ["CAP-IT-ALL"],
                "status": "playing",
                "claimed_at": chrono::Utc::now().to_rfc3339()
            }))
            .expect("queue json"),
        )
        .expect("write good queue");

        let snapshot = active_queue_snapshot(dir.path()).expect("snapshot");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].queue_id, "good");
    }

    #[cfg(unix)]
    #[test]
    fn active_queue_snapshot_isolates_a_symlinked_manifest() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let queue_dir = dir.path().join("runtime").join("queues").join("alerts");
        std::fs::create_dir_all(&queue_dir).expect("queue dir");
        let outside_manifest = outside.path().join("outside.json");
        std::fs::write(
            &outside_manifest,
            serde_json::to_vec(&json!({
                "id": "outside",
                "feed_ids": ["CAP-IT-ALL"],
                "status": "playing",
                "claimed_at": chrono::Utc::now().to_rfc3339()
            }))
            .expect("outside queue json"),
        )
        .expect("outside queue");
        symlink(&outside_manifest, queue_dir.join("linked.json")).expect("queue symlink");
        std::fs::write(
            queue_dir.join("good.json"),
            serde_json::to_vec(&json!({
                "id": "good",
                "feed_ids": ["CAP-IT-ALL"],
                "status": "playing",
                "claimed_at": chrono::Utc::now().to_rfc3339()
            }))
            .expect("queue json"),
        )
        .expect("write good queue");

        let snapshot = active_queue_snapshot(dir.path()).expect("snapshot");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].queue_id, "good");
    }

    #[test]
    fn active_queue_snapshot_ignores_stale_claims() {
        let dir = tempfile::tempdir().expect("tempdir");
        let queue_dir = dir.path().join("runtime").join("queues").join("alerts");
        std::fs::create_dir_all(&queue_dir).expect("queue dir");
        std::fs::write(
            queue_dir.join("q1.json"),
            serde_json::to_vec(&json!({
                "id": "q1",
                "feed_ids": ["CAP-IT-ALL"],
                "claimed_at": (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339(),
                "played_at": null,
                "failed_at": null
            }))
            .expect("queue json"),
        )
        .expect("write queue");

        let snapshot = active_queue_snapshot(dir.path()).expect("snapshot");
        assert!(snapshot.is_empty());
    }

    #[test]
    fn active_queue_cache_rereads_changed_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let queue_dir = dir.path().join("runtime").join("queues").join("alerts");
        std::fs::create_dir_all(&queue_dir).expect("queue dir");
        let path = queue_dir.join("q1.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&json!({
                "id": "q1",
                "feed_ids": ["CAP-IT-ALL"],
                "claimed_at": chrono::Utc::now().to_rfc3339(),
                "played_at": null,
                "failed_at": null,
                "banner_text": "first"
            }))
            .expect("queue json"),
        )
        .expect("write queue");

        let mut cache = ActiveQueueSnapshotCache::default();
        let snapshot = cache.scan(dir.path()).expect("first snapshot");
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].data["banner_text"], "first");

        std::fs::write(
            &path,
            serde_json::to_vec(&json!({
                "id": "q1",
                "feed_ids": ["CAP-IT-ALL"],
                "claimed_at": chrono::Utc::now().to_rfc3339(),
                "played_at": "2026-06-24T00:00:30Z",
                "failed_at": null,
                "banner_text": "completed and longer"
            }))
            .expect("queue json"),
        )
        .expect("write queue update");

        let snapshot = cache.scan(dir.path()).expect("second snapshot");
        assert!(snapshot.is_empty());
    }

    #[test]
    fn active_queue_cache_expires_unchanged_manifest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let queue_dir = dir.path().join("runtime").join("queues").join("alerts");
        std::fs::create_dir_all(&queue_dir).expect("queue dir");
        let path = queue_dir.join("q1.json");
        std::fs::write(&path, b"{}").expect("write queue");
        let fingerprint = queue_manifest_fingerprint(&path).expect("fingerprint");
        let mut cache = ActiveQueueSnapshotCache::default();
        cache.entries.insert(
            path,
            CachedQueueManifest {
                fingerprint,
                item: Some(ActiveQueueItem {
                    queue_id: "q1".to_string(),
                    feed_ids: vec!["CAP-IT-ALL".to_string()],
                    data: json!({"queue_id": "q1"}),
                }),
                active_until: Some(chrono::Utc::now() - chrono::Duration::seconds(1)),
            },
        );

        let snapshot = cache.scan(dir.path()).expect("snapshot");
        assert!(snapshot.is_empty());
    }

    #[tokio::test]
    async fn active_queue_poll_loop_stops_on_shutdown_signal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (tx, mut rx) = mpsc::channel::<serde_json::Value>(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = tokio::spawn(active_queue_poll_loop(
            dir.path().to_path_buf(),
            tx,
            shutdown_rx,
        ));

        shutdown_tx.send(true).expect("send shutdown");
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .expect("queue poller stopped")
            .expect("queue poller task");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn validate_config_command_parses_without_bridge_arguments() {
        let args = Args::try_parse_from(["haze-cgen", "validate-config", "--runtime"])
            .expect("validate-config arguments");
        assert!(matches!(
            args.command,
            Some(Command::ValidateConfig { runtime: true })
        ));
        assert!(args.bridge.is_none());
    }

    #[test]
    fn config_only_preflight_validates_managed_pipeline_and_protected_scene() {
        let directory = tempfile::tempdir().expect("temporary managed directory");
        let config_directory = directory.path().join("managed").join("configs");
        std::fs::create_dir_all(&config_directory).expect("managed config directory");
        let path = config_directory.join("cgen.xml");
        std::fs::write(
            &path,
            r#"<cgen schema_version="2" enabled="true">
  <feed id="CAP" name="CAP CGEN" enabled="true">
    <programInput type="dummy" width="720" height="480" fps="30000/1001"/>
    <priorityInput feed_id="CAP" audio_source="both"/>
    <alert feed_id="CAP"/>
    <compositor alert_scene_id="Standard_Crawl" engine="scene_v2"/>
    <video width="720" height="480" fps="30000/1001"/>
    <audio idle="source" alert_mode="replace" topology="force_layout" force_layout="stereo"/>
    <ladder>
      <video id="sd" enabled="true" width="720" height="480" fps="30000/1001"/>
      <audio id="stereo" enabled="true" channels="2"/>
    </ladder>
    <outputs>
      <output id="primary" enabled="true" destination="rtmp" url="rtmp://example.invalid/live/test" video_codec="h264" rate_control="cbr" video_bitrate_kbps="4000" video_max_bitrate_kbps="4000" gop_frames="60" audio_codec="aac" audio_bitrate_kbps="192" sample_rate="48000"/>
    </outputs>
  </feed>
</cgen>"#,
        )
        .expect("managed cgen config");

        let report =
            validate_config_report(&path, directory.path(), false).expect("config preflight");
        assert_eq!(report["ok"], true);
        assert_eq!(report["enabled_feed_count"], 1);
        assert_eq!(report["feeds"][0]["feed_id"], "CAP");
        assert_eq!(
            report["feeds"][0]["alert_scene_id"],
            scene::STANDARD_CRAWL_ID
        );
        assert_eq!(report["feeds"][0]["runtime"], Value::Null);
        assert!(!serde_json::to_string(&report)
            .expect("serialize preflight report")
            .contains("example.invalid"));
    }
}

#[cfg(unix)]
fn process_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

#[cfg(not(any(unix, windows)))]
fn process_alive(_pid: u32) -> bool {
    true
}
