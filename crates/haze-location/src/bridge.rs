//! Event bridge adapter for the location query and overlay contracts.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::catalog::{CatalogError, CatalogManager};
use crate::config;
use crate::contract::{OverlayUpsert, QueryFailure, QueryRequest, API_VERSION};
use crate::engine::{EngineError, QueryEngine};

const CLIENT_ID: &str = "haze-location";
const SUBSCRIPTIONS: &[&str] = &[
    "location.query.request",
    "location.overlay.upsert.request",
    "location.catalog.reload.request",
    "location.catalog.rollback.request",
];

#[derive(Clone)]
struct BridgeWriter {
    inner: Arc<Mutex<OwnedWriteHalf>>,
}

impl BridgeWriter {
    async fn publish<T: Serialize>(
        &self,
        event_type: &str,
        subject: Option<&str>,
        target: Option<&str>,
        data: &T,
    ) -> Result<()> {
        let mut event = json!({
            "type": event_type,
            "source": CLIENT_ID,
            "data": data,
        });
        if let Some(subject) = subject.filter(|value| !value.trim().is_empty()) {
            event["subject"] = Value::String(subject.to_string());
        }
        if let Some(target) = target.filter(|value| !value.trim().is_empty()) {
            event["target"] = Value::String(target.to_string());
        }
        let mut raw = serde_json::to_vec(&event).context("failed to serialize location event")?;
        raw.push(b'\n');
        let mut writer = self.inner.lock().await;
        writer.write_all(&raw).await?;
        writer.flush().await?;
        Ok(())
    }
}

pub async fn run(
    bridge_addr: &str,
    root_config: PathBuf,
    locations_config: Option<PathBuf>,
    manager: CatalogManager,
    engine: QueryEngine,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut backoff = Duration::from_millis(250);
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        match TcpStream::connect(bridge_addr).await {
            Ok(stream) => {
                info!(
                    bridge = bridge_addr,
                    "location service connected to host bridge"
                );
                let result = serve_connection(
                    stream,
                    &root_config,
                    locations_config.as_deref(),
                    &manager,
                    &engine,
                    &shutdown,
                )
                .await;
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                if let Err(err) = result {
                    warn!("location host bridge connection ended: {err:#}");
                }
                backoff = Duration::from_millis(250);
            }
            Err(err) => warn!(
                bridge = bridge_addr,
                "location host bridge connection failed: {err}"
            ),
        }
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(Duration::from_secs(15));
    }
}

async fn serve_connection(
    stream: TcpStream,
    root_config: &Path,
    locations_config: Option<&Path>,
    manager: &CatalogManager,
    engine: &QueryEngine,
    shutdown: &CancellationToken,
) -> Result<()> {
    let (read, write) = stream.into_split();
    let writer = BridgeWriter {
        inner: Arc::new(Mutex::new(write)),
    };
    writer
        .publish(
            "bridge.client",
            None,
            None,
            &json!({
                "client_id": CLIENT_ID,
                "receive_events": true,
                "subscriptions": SUBSCRIPTIONS,
            }),
        )
        .await?;
    publish_catalog_event(&writer, "location.catalog.ready", manager).await?;

    let mut lines = BufReader::new(read).lines();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            line = lines.next_line() => {
                let Some(line) = line? else {
                    anyhow::bail!("host bridge closed the connection");
                };
                if line.trim().is_empty() {
                    continue;
                }
                let event: Value = match serde_json::from_str(&line) {
                    Ok(value) => value,
                    Err(err) => {
                        warn!("location service ignored invalid bridge JSON: {err}");
                        continue;
                    }
                };
                handle_event(
                    event,
                    writer.clone(),
                    root_config.to_path_buf(),
                    locations_config.map(Path::to_path_buf),
                    manager.clone(),
                    engine.clone(),
                );
            }
        }
    }
}

fn handle_event(
    event: Value,
    writer: BridgeWriter,
    root_config: PathBuf,
    locations_config: Option<PathBuf>,
    manager: CatalogManager,
    engine: QueryEngine,
) {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let target = if event_type == "location.overlay.upsert.request" {
        event
            .get("reply_to")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty() && *value != CLIENT_ID)
    } else {
        requester(&event)
    }
    .map(ToOwned::to_owned);
    match event_type {
        "location.query.request" => {
            let request_value = event.get("data").cloned().unwrap_or(Value::Null);
            tokio::spawn(async move {
                let request = match serde_json::from_value::<QueryRequest>(request_value) {
                    Ok(request) => request,
                    Err(err) => {
                        let request_id = event
                            .get("subject")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let failure =
                            failure(request_id, "invalid_request", err.to_string(), false);
                        let _ = writer
                            .publish(
                                "location.query.failed",
                                Some(request_id),
                                target.as_deref(),
                                &failure,
                            )
                            .await;
                        return;
                    }
                };
                let request_id = request.request_id.clone();
                match engine.query(request).await {
                    Ok(response) => {
                        let _ = writer
                            .publish(
                                "location.query.completed",
                                Some(&request_id),
                                target.as_deref(),
                                &response,
                            )
                            .await;
                    }
                    Err(err) => {
                        let failure = engine_failure(&request_id, &err);
                        let _ = writer
                            .publish(
                                "location.query.failed",
                                Some(&request_id),
                                target.as_deref(),
                                &failure,
                            )
                            .await;
                    }
                }
            });
        }
        "location.overlay.upsert.request" => {
            let request_value = event.get("data").cloned().unwrap_or(Value::Null);
            tokio::spawn(async move {
                let request = match serde_json::from_value::<OverlayUpsert>(request_value) {
                    Ok(request) => request,
                    Err(err) => {
                        let failure = failure("", "invalid_request", err.to_string(), false);
                        if let Some(target) = target.as_deref() {
                            let _ = writer
                                .publish(
                                    "location.overlay.upsert_failed",
                                    None,
                                    Some(target),
                                    &failure,
                                )
                                .await;
                        }
                        return;
                    }
                };
                let request_id = request.request_id.clone();
                let manager_for_write = manager.clone();
                let write =
                    tokio::task::spawn_blocking(move || manager_for_write.upsert_overlay(&request))
                        .await;
                match write {
                    Ok(Ok(canonical_id)) => {
                        let data = json!({
                            "api_version": API_VERSION,
                            "request_id": request_id,
                            "canonical_id": canonical_id,
                        });
                        if let Some(target) = target.as_deref() {
                            let _ = writer
                                .publish(
                                    "location.overlay.upserted",
                                    Some(&request_id),
                                    Some(target),
                                    &data,
                                )
                                .await;
                        }
                    }
                    Ok(Err(err)) => {
                        let failure = catalog_failure(&request_id, &err);
                        if let Some(target) = target.as_deref() {
                            let _ = writer
                                .publish(
                                    "location.overlay.upsert_failed",
                                    Some(&request_id),
                                    Some(target),
                                    &failure,
                                )
                                .await;
                        }
                    }
                    Err(err) => {
                        let failure = failure(&request_id, "internal", err.to_string(), true);
                        if let Some(target) = target.as_deref() {
                            let _ = writer
                                .publish(
                                    "location.overlay.upsert_failed",
                                    Some(&request_id),
                                    Some(target),
                                    &failure,
                                )
                                .await;
                        }
                    }
                }
            });
        }
        "location.catalog.reload.request" | "location.catalog.rollback.request" => {
            let rollback = event_type == "location.catalog.rollback.request"
                || event
                    .get("data")
                    .and_then(|data| data.get("rollback"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
            tokio::spawn(async move {
                let manager_for_reload = manager.clone();
                let root_for_reload = root_config.clone();
                let locations_for_reload = locations_config.clone();
                let loaded = tokio::task::spawn_blocking(move || {
                    if rollback {
                        let snapshot = manager_for_reload.rollback()?;
                        return Ok::<_, anyhow::Error>((None, snapshot));
                    }
                    let config = config::load(&root_for_reload, locations_for_reload.as_deref())?;
                    let snapshot = manager_for_reload.reload(&config)?;
                    Ok::<_, anyhow::Error>((Some(config), snapshot))
                })
                .await;
                match loaded {
                    Ok(Ok((config, snapshot))) => {
                        if let Some(config) = config {
                            engine.update_config(&config);
                        }
                        let data = json!({
                            "api_version": API_VERSION,
                            "catalog_generation": snapshot.generation,
                            "catalog_packs": snapshot.pack_ids,
                        });
                        let _ = writer
                            .publish("location.catalog.reloaded", None, target.as_deref(), &data)
                            .await;
                    }
                    Ok(Err(err)) => {
                        let data = failure("", "reload_failed", format!("{err:#}"), false);
                        let _ = writer
                            .publish(
                                "location.catalog.reload_failed",
                                None,
                                target.as_deref(),
                                &data,
                            )
                            .await;
                    }
                    Err(err) => {
                        let data = failure("", "internal", err.to_string(), true);
                        let _ = writer
                            .publish(
                                "location.catalog.reload_failed",
                                None,
                                target.as_deref(),
                                &data,
                            )
                            .await;
                    }
                }
            });
        }
        _ => {}
    }
}

fn requester(event: &Value) -> Option<&str> {
    event
        .get("reply_to")
        .and_then(Value::as_str)
        .or_else(|| event.get("source").and_then(Value::as_str))
        .filter(|value| !value.trim().is_empty() && *value != CLIENT_ID)
}

async fn publish_catalog_event(
    writer: &BridgeWriter,
    event_type: &str,
    manager: &CatalogManager,
) -> Result<()> {
    let snapshot = manager.snapshot();
    writer
        .publish(
            event_type,
            None,
            None,
            &json!({
                "api_version": API_VERSION,
                "catalog_generation": snapshot.generation,
                "catalog_packs": snapshot.pack_ids,
            }),
        )
        .await
}

fn engine_failure(request_id: &str, error: &EngineError) -> QueryFailure {
    match error {
        EngineError::Busy => failure(request_id, "busy", error.to_string(), true),
        EngineError::Closed => failure(request_id, "unavailable", error.to_string(), true),
        EngineError::Catalog(error) => catalog_failure(request_id, error),
    }
}

fn catalog_failure(request_id: &str, error: &CatalogError) -> QueryFailure {
    match error {
        CatalogError::Invalid(_) => {
            failure(request_id, "invalid_request", error.to_string(), false)
        }
        CatalogError::OverlaySource(_) => {
            failure(request_id, "source_not_allowed", error.to_string(), false)
        }
        CatalogError::Sqlite(_) | CatalogError::Unavailable(_) => {
            failure(request_id, "catalog_unavailable", error.to_string(), true)
        }
    }
}

fn failure(
    request_id: &str,
    code: &str,
    error: impl Into<String>,
    retryable: bool,
) -> QueryFailure {
    QueryFailure {
        api_version: API_VERSION,
        request_id: request_id.to_string(),
        code: code.to_string(),
        error: error.into(),
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_to_wins_over_source() {
        let event = json!({"reply_to": "ivr-1", "source": "ivr"});
        assert_eq!(requester(&event), Some("ivr-1"));
    }
}
