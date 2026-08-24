use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::StreamExt;
use reqwest::header::ACCEPT;
use reqwest::{Client, StatusCode};
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

use crate::bridge::{EventEnvelope, EventPublisher};
use crate::model::Alert;
use crate::parse::parse_cap;

const MAX_STREAM_BUFFER_BYTES: usize = 8 << 20;
const MAX_CAP_XML_BYTES: usize = 5 << 20;
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);
const READ_CHUNK_BYTES: usize = 16 * 1024;
const ECCC_DATAMART_CAP_BASE_URL: &str = "https://dd.weather.gc.ca/today/alerts/cap";
const ECCC_DATAMART_POLL_INTERVAL: Duration = Duration::from_secs(30);
const ECCC_DATAMART_LOOKBACK_HOURS: i64 = 1;

#[derive(Clone, Debug)]
pub struct StreamConfig {
    pub source_id: String,
    pub stream_urls: Vec<String>,
    pub archive_urls: Vec<String>,
    pub timeout: Duration,
    pub user_agent: String,
    pub shadow: bool,
    pub startup_seed: bool,
}

pub struct NaadsTcpIngest {
    config: StreamConfig,
    publisher: Arc<EventPublisher>,
    http: Client,
    state: Arc<Mutex<StreamState>>,
}

#[derive(Debug, Default)]
struct StreamState {
    seen_alerts: HashSet<AlertKey>,
    seen_references: HashSet<ReferenceKey>,
    seeded_heartbeat: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AlertKey {
    identifier: String,
    sent: String,
    message_type: String,
    references: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReferenceKey {
    identifier: String,
    sent: String,
}

#[derive(Clone, Debug)]
struct Reference {
    sender: String,
    identifier: String,
    sent: String,
}

impl NaadsTcpIngest {
    pub fn new(config: StreamConfig, publisher: EventPublisher) -> Result<Self> {
        if config.stream_urls.is_empty() {
            return Err(anyhow!("no NAADS TCP stream URLs configured"));
        }
        let http = Client::builder()
            .timeout(config.timeout)
            .user_agent(config.user_agent.clone())
            .build()
            .context("failed to create NAADS repository HTTP client")?;
        Ok(Self {
            config,
            publisher: Arc::new(publisher),
            http,
            state: Arc::new(Mutex::new(StreamState::default())),
        })
    }

    pub async fn run(&self) -> Result<()> {
        let mut tasks = Vec::new();
        for url in &self.config.stream_urls {
            let worker = StreamWorker {
                source_id: self.config.source_id.clone(),
                stream_url: url.clone(),
                archive_urls: self.config.archive_urls.clone(),
                shadow: self.config.shadow,
                startup_seed: self.config.startup_seed,
                publisher: Arc::clone(&self.publisher),
                http: self.http.clone(),
                state: Arc::clone(&self.state),
            };
            tasks.push(tokio::spawn(async move { worker.run_forever().await }));
        }

        let fallback_worker = StreamWorker {
            source_id: self.config.source_id.clone(),
            stream_url: ECCC_DATAMART_CAP_BASE_URL.to_string(),
            archive_urls: self.config.archive_urls.clone(),
            shadow: self.config.shadow,
            startup_seed: self.config.startup_seed,
            publisher: Arc::clone(&self.publisher),
            http: self.http.clone(),
            state: Arc::clone(&self.state),
        };
        tasks.push(tokio::spawn(async move {
            EcccDatamartFallback {
                worker: fallback_worker,
            }
            .run_forever()
            .await
        }));

        for task in tasks {
            task.await??;
        }
        Ok(())
    }
}

struct StreamWorker {
    source_id: String,
    stream_url: String,
    archive_urls: Vec<String>,
    shadow: bool,
    startup_seed: bool,
    publisher: Arc<EventPublisher>,
    http: Client,
    state: Arc<Mutex<StreamState>>,
}

struct EcccDatamartFallback {
    worker: StreamWorker,
}

#[derive(Debug, Default)]
struct DatamartPollStats {
    date_directories: usize,
    alert_directories: usize,
    candidate_files: usize,
    processed_files: usize,
}

impl EcccDatamartFallback {
    async fn run_forever(self) -> Result<()> {
        let mut announced_ready = false;
        loop {
            match self.poll_once().await {
                Ok(stats) => {
                    if !announced_ready {
                        info!(
                            source = self.worker.source_id,
                            source_url = self.worker.stream_url,
                            poll_interval_seconds = ECCC_DATAMART_POLL_INTERVAL.as_secs(),
                            lookback_hours = ECCC_DATAMART_LOOKBACK_HOURS,
                            "ECCC Datamart CAP fallback enabled"
                        );
                        announced_ready = true;
                    }
                    if stats.processed_files > 0 {
                        info!(
                            source = self.worker.source_id,
                            date_directories = stats.date_directories,
                            alert_directories = stats.alert_directories,
                            candidate_files = stats.candidate_files,
                            processed_files = stats.processed_files,
                            "processed ECCC Datamart CAP fallback files"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        source = self.worker.source_id,
                        source_url = self.worker.stream_url,
                        %error,
                        "ECCC Datamart CAP fallback poll failed"
                    );
                    self.worker
                        .publish_status(json!({
                            "source_id": self.worker.source_id,
                            "source": "naads",
                            "mode": "tcp",
                            "shadow": self.worker.shadow,
                            "status": "datamart_fallback_error",
                            "transport": "eccc_datamart_https",
                            "url": self.worker.stream_url,
                            "error": error.to_string(),
                            "timestamp_unix_ms": unix_ms(),
                        }))
                        .await
                        .ok();
                }
            }
            sleep(ECCC_DATAMART_POLL_INTERVAL).await;
        }
    }

    async fn poll_once(&self) -> Result<DatamartPollStats> {
        let mut stats = DatamartPollStats::default();
        for (date, hours) in datamart_date_hours(Utc::now()) {
            let date_url = format!("{}/{date}/", self.worker.stream_url.trim_end_matches('/'));
            let offices = match self.fetch_listing(&date_url, false).await {
                Ok(entries) => listing_directories(&entries),
                Err(error) => {
                    warn!(
                        source = self.worker.source_id,
                        url = %date_url,
                        %error,
                        "failed to list ECCC Datamart CAP date directory"
                    );
                    continue;
                }
            };
            stats.date_directories += 1;
            for office in offices {
                for hour in &hours {
                    let directory_url = format!("{date_url}{office}/{hour}/");
                    let files = match self.fetch_listing(&directory_url, true).await {
                        Ok(entries) => listing_cap_files(&entries),
                        Err(error) => {
                            warn!(
                                source = self.worker.source_id,
                                url = %directory_url,
                                %error,
                                "failed to list ECCC Datamart CAP alert directory"
                            );
                            continue;
                        }
                    };
                    stats.alert_directories += 1;
                    stats.candidate_files += files.len();
                    for file in files {
                        let cap_url = format!("{directory_url}{file}");
                        match self.fetch_cap(&cap_url).await {
                            Ok(true) => stats.processed_files += 1,
                            Ok(false) => {}
                            Err(error) => warn!(
                                source = self.worker.source_id,
                                url = %cap_url,
                                %error,
                                "failed to process ECCC Datamart CAP file"
                            ),
                        }
                    }
                }
            }
        }
        if stats.date_directories == 0 {
            return Err(anyhow!(
                "no ECCC Datamart CAP date directories were reachable"
            ));
        }
        Ok(stats)
    }

    async fn fetch_listing(&self, url: &str, missing_is_empty: bool) -> Result<Vec<u8>> {
        let response = self
            .worker
            .http
            .get(url)
            .header(ACCEPT, "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1")
            .send()
            .await
            .with_context(|| format!("failed to fetch ECCC Datamart CAP listing {url}"))?;
        if response.status() == StatusCode::NOT_FOUND && missing_is_empty {
            return Ok(Vec::new());
        }
        if !response.status().is_success() {
            return Err(anyhow!(
                "unexpected HTTP status {} from {url}",
                response.status()
            ));
        }
        read_limited_response(response).await
    }

    async fn fetch_cap(&self, url: &str) -> Result<bool> {
        let started = Instant::now();
        let response = self
            .worker
            .http
            .get(url)
            .header(
                ACCEPT,
                "application/cap+xml, application/xml;q=0.9, */*;q=0.1",
            )
            .send()
            .await
            .with_context(|| format!("failed to fetch ECCC Datamart CAP file {url}"))?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "unexpected HTTP status {} from {url}",
                response.status()
            ));
        }
        let bytes = read_limited_response(response).await?;
        let fetch_ms = started.elapsed().as_millis();
        let parse_started = Instant::now();
        let alert = parse_cap(&bytes).with_context(|| format!("failed to parse CAP file {url}"))?;
        self.worker
            .handle_alert(
                alert,
                "eccc_datamart_https",
                Some(url.to_string()),
                fetch_ms,
                parse_started.elapsed().as_millis(),
            )
            .await
    }
}

fn datamart_date_hours(now: DateTime<Utc>) -> BTreeMap<String, Vec<String>> {
    let mut result = BTreeMap::<String, Vec<String>>::new();
    for offset in 0..=ECCC_DATAMART_LOOKBACK_HOURS {
        let instant = now - ChronoDuration::hours(offset);
        let date = instant.format("%Y%m%d").to_string();
        let hour = instant.format("%H").to_string();
        let hours = result.entry(date).or_default();
        if !hours.contains(&hour) {
            hours.push(hour);
        }
    }
    result
}

fn listing_directories(raw: &[u8]) -> Vec<String> {
    listing_links(raw)
        .into_iter()
        .filter_map(|link| link.strip_suffix('/').map(ToOwned::to_owned))
        .filter(|name| is_safe_datamart_component(name))
        .collect()
}

fn listing_cap_files(raw: &[u8]) -> Vec<String> {
    listing_links(raw)
        .into_iter()
        .filter(|name| name.ends_with(".cap") && is_safe_datamart_component(name))
        .collect()
}

fn listing_links(raw: &[u8]) -> Vec<String> {
    let raw = String::from_utf8_lossy(raw);
    let mut rest = raw.as_ref();
    let mut links = Vec::new();
    while let Some(start) = rest.find("href=\"") {
        let after_start = &rest[start + "href=\"".len()..];
        let Some(end) = after_start.find('"') else {
            break;
        };
        links.push(after_start[..end].to_string());
        rest = &after_start[end + 1..];
    }
    links.sort();
    links.dedup();
    links
}

fn is_safe_datamart_component(value: &str) -> bool {
    !matches!(value, "." | "..")
        && !value.is_empty()
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
}

impl StreamWorker {
    async fn run_forever(self) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.run_once().await {
                Ok(()) => backoff = Duration::from_secs(1),
                Err(err) => {
                    warn!(
                        source = self.source_id,
                        stream = self.stream_url,
                        "NAADS TCP stream disconnected: {err:#}"
                    );
                    self.publish_status(json!({
                        "source_id": self.source_id,
                        "source": "naads",
                        "mode": "tcp",
                        "shadow": self.shadow,
                        "status": "stream_disconnected",
                        "stream_url": self.stream_url,
                        "error": err.to_string(),
                        "reconnect_backoff_ms": backoff.as_millis(),
                        "timestamp_unix_ms": unix_ms(),
                    }))
                    .await
                    .ok();
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
                }
            }
        }
    }

    async fn run_once(&self) -> Result<()> {
        let addr = tcp_addr_from_url(&self.stream_url)?;
        info!(
            source = self.source_id,
            stream = self.stream_url,
            addr,
            "connecting to NAADS TCP stream"
        );
        let mut stream = TcpStream::connect(&addr)
            .await
            .with_context(|| format!("failed to connect to NAADS TCP stream {addr}"))?;
        self.publish_status(json!({
            "source_id": self.source_id,
            "source": "naads",
            "mode": "tcp",
            "shadow": self.shadow,
            "status": "stream_connected",
            "stream_url": self.stream_url,
            "timestamp_unix_ms": unix_ms(),
        }))
        .await?;

        let mut buffer = Vec::new();
        let mut chunk = vec![0u8; READ_CHUNK_BYTES];
        loop {
            let read = timeout(Duration::from_secs(130), stream.read(&mut chunk))
                .await
                .with_context(|| {
                    format!("NAADS TCP stream {addr} produced no heartbeat/data within timeout")
                })?
                .with_context(|| format!("failed reading NAADS TCP stream {addr}"))?;
            if read == 0 {
                return Err(anyhow!("NAADS TCP stream closed"));
            }
            buffer.extend_from_slice(&chunk[..read]);
            if buffer.len() > MAX_STREAM_BUFFER_BYTES {
                let keep_from = buffer.len().saturating_sub(MAX_STREAM_BUFFER_BYTES / 2);
                buffer.drain(..keep_from);
                warn!(
                    source = self.source_id,
                    stream = self.stream_url,
                    "trimmed oversized NAADS stream buffer"
                );
            }
            for message in drain_alert_xml_messages(&mut buffer) {
                self.handle_message(message).await?;
            }
        }
    }

    async fn handle_message(&self, raw: Vec<u8>) -> Result<()> {
        if raw.len() > MAX_CAP_XML_BYTES {
            warn!(
                source = self.source_id,
                stream = self.stream_url,
                bytes = raw.len(),
                "discarding oversized NAADS CAP message"
            );
            self.publish_status(json!({
                "source_id": self.source_id,
                "source": "naads",
                "mode": "tcp",
                "shadow": self.shadow,
                "status": "invalid_cap",
                "reason": "oversized",
                "bytes": raw.len(),
                "stream_url": self.stream_url,
                "timestamp_unix_ms": unix_ms(),
            }))
            .await
            .ok();
            return Ok(());
        }
        let parse_started = Instant::now();
        let alert = match parse_cap(&raw) {
            Ok(alert) => alert,
            Err(err) => {
                warn!(
                    source = self.source_id,
                    stream = self.stream_url,
                    "discarding invalid NAADS CAP message: {err:#}"
                );
                self.publish_status(json!({
                    "source_id": self.source_id,
                    "source": "naads",
                    "mode": "tcp",
                    "shadow": self.shadow,
                    "status": "invalid_cap",
                    "reason": err.to_string(),
                    "stream_url": self.stream_url,
                    "timestamp_unix_ms": unix_ms(),
                }))
                .await
                .ok();
                return Ok(());
            }
        };
        let parse_ms = parse_started.elapsed().as_millis();
        if is_heartbeat(&alert) {
            self.handle_heartbeat(&alert, parse_ms).await?;
            return Ok(());
        }
        self.handle_alert(alert, "tcp_stream", None, 0, parse_ms)
            .await
            .map(|_| ())
    }

    async fn handle_heartbeat(&self, alert: &Alert, parse_ms: u128) -> Result<()> {
        let references = parse_references(&alert.references);
        let seed_only = {
            let mut state = self.state.lock().await;
            if self.startup_seed && !state.seeded_heartbeat {
                for reference in &references {
                    state.seen_references.insert(ReferenceKey {
                        identifier: reference.identifier.clone(),
                        sent: reference.sent.clone(),
                    });
                }
                state.seeded_heartbeat = true;
                true
            } else {
                state.seeded_heartbeat = true;
                false
            }
        };

        self.publish_status(json!({
            "source_id": self.source_id,
            "source": "naads",
            "mode": "tcp",
            "shadow": self.shadow,
            "status": if seed_only { "heartbeat_seeded" } else { "heartbeat" },
            "stream_url": self.stream_url,
            "references": references.len(),
            "parse_ms": parse_ms,
            "timestamp_unix_ms": unix_ms(),
        }))
        .await?;

        if seed_only {
            return Ok(());
        }

        for reference in references {
            let should_fetch = {
                let mut state = self.state.lock().await;
                state.seen_references.insert(ReferenceKey {
                    identifier: reference.identifier.clone(),
                    sent: reference.sent.clone(),
                })
            };
            if should_fetch {
                if let Err(err) = self.fetch_reference(reference).await {
                    warn!(
                        source = self.source_id,
                        stream = self.stream_url,
                        "failed to recover referenced NAADS alert: {err:#}"
                    );
                }
            }
        }

        Ok(())
    }

    async fn fetch_reference(&self, reference: Reference) -> Result<()> {
        if self.archive_urls.is_empty() {
            return Ok(());
        }
        debug!(
            source = self.source_id,
            sender = reference.sender.as_str(),
            identifier = reference.identifier.as_str(),
            sent = reference.sent.as_str(),
            "recovering NAADS heartbeat reference from repository"
        );
        let mut last_error = None;
        for base in &self.archive_urls {
            let Some(url) = archive_url(base, &reference) else {
                continue;
            };
            let started = Instant::now();
            let result = self
                .http
                .get(&url)
                .header(
                    ACCEPT,
                    "application/cap+xml, application/xml;q=0.9, */*;q=0.1",
                )
                .send()
                .await
                .with_context(|| format!("failed to fetch NAADS repository CAP {url}"));
            let Ok(response) = result else {
                last_error = result.err();
                continue;
            };
            if !response.status().is_success() {
                last_error = Some(anyhow!(
                    "unexpected HTTP status {} from {url}",
                    response.status()
                ));
                continue;
            }
            let bytes = match read_limited_response(response).await {
                Ok(bytes) => bytes,
                Err(err) => {
                    last_error = Some(err);
                    continue;
                }
            };
            let parse_started = Instant::now();
            let alert = match parse_cap(&bytes) {
                Ok(alert) => alert,
                Err(err) => {
                    last_error = Some(err.into());
                    continue;
                }
            };
            self.handle_alert(
                alert,
                "heartbeat_repository",
                Some(url),
                started.elapsed().as_millis(),
                parse_started.elapsed().as_millis(),
            )
            .await?;
            return Ok(());
        }
        Err(last_error.unwrap_or_else(|| anyhow!("no repository URL could be built")))
    }

    async fn handle_alert(
        &self,
        alert: Alert,
        transport: &str,
        cap_url: Option<String>,
        fetch_ms: u128,
        parse_ms: u128,
    ) -> Result<bool> {
        let key = AlertKey {
            identifier: alert.identifier.clone(),
            sent: alert.sent.clone(),
            message_type: alert.message_type.clone(),
            references: alert.references.clone(),
        };
        let reference_key = ReferenceKey {
            identifier: alert.identifier.clone(),
            sent: alert.sent.clone(),
        };
        let is_new = {
            let mut state = self.state.lock().await;
            state.seen_references.insert(reference_key);
            state.seen_alerts.insert(key)
        };
        if !is_new {
            debug!(
                source = self.source_id,
                identifier = alert.identifier,
                "deduped NAADS TCP alert"
            );
            return Ok(false);
        }

        let alert_value = serde_json::to_value(&alert)?;
        if self.shadow {
            self.publish_status(json!({
                "source_id": self.source_id,
                "source": "naads",
                "mode": "tcp",
                "shadow": true,
                "status": "would_publish",
                "transport": transport,
                "stream_url": self.stream_url,
                "cap_url": cap_url,
                "identifier": alert.identifier,
                "sent": alert.sent,
                "message_type": alert.message_type,
                "fetch_ms": fetch_ms,
                "parse_ms": parse_ms,
                "timestamp_unix_ms": unix_ms(),
            }))
            .await?;
            return Ok(true);
        }

        let ingest = json!({
            "source": "naads",
            "source_id": self.source_id,
            "mode": "tcp",
            "shadow": false,
            "transport": transport,
            "stream_url": self.stream_url,
            "cap_url": cap_url,
            "fetch_latency_ms": fetch_ms,
            "parse_latency_ms": parse_ms,
            "publish_started_unix_ms": unix_ms(),
        });
        self.publisher
            .publish(&EventEnvelope::cap_alert(
                &self.source_id,
                alert_value,
                ingest,
            ))
            .await?;
        info!(
            source = self.source_id,
            identifier = alert.identifier,
            message_type = alert.message_type,
            %transport,
            "published CAP alert"
        );
        Ok(true)
    }

    async fn publish_status(&self, data: serde_json::Value) -> Result<()> {
        self.publisher
            .publish(&EventEnvelope::status(
                &self.source_id,
                &format!("naads:{}", self.source_id),
                data,
            ))
            .await
    }
}

async fn read_limited_response(response: reqwest::Response) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_CAP_XML_BYTES as u64)
    {
        return Err(anyhow!("response exceeds {MAX_CAP_XML_BYTES} bytes"));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > MAX_CAP_XML_BYTES {
            return Err(anyhow!("response exceeds {MAX_CAP_XML_BYTES} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn is_heartbeat(alert: &Alert) -> bool {
    alert.status.eq_ignore_ascii_case("System")
        || alert.sender.eq_ignore_ascii_case("NAADS-Heartbeat")
        || alert.identifier.to_ascii_lowercase().contains("heartbeat")
}

fn parse_references(raw: &str) -> Vec<Reference> {
    raw.split_whitespace()
        .filter_map(|item| {
            let mut parts = item.splitn(3, ',');
            Some(Reference {
                sender: parts.next()?.trim().to_string(),
                identifier: parts.next()?.trim().to_string(),
                sent: parts.next()?.trim().to_string(),
            })
        })
        .filter(|reference| !reference.identifier.is_empty() && !reference.sent.is_empty())
        .collect()
}

fn archive_url(base: &str, reference: &Reference) -> Option<String> {
    let date = reference.sent.get(0..10)?;
    let sent = sanitize_repository_component(&reference.sent);
    let identifier = sanitize_repository_component(&reference.identifier);
    Some(format!(
        "{}/{}/{}I{}.xml",
        base.trim_end_matches('/'),
        date,
        sent,
        identifier
    ))
}

fn sanitize_repository_component(raw: &str) -> String {
    raw.trim()
        .chars()
        .map(|ch| match ch {
            '-' | ':' => '_',
            '+' => 'p',
            _ => ch,
        })
        .collect()
}

fn tcp_addr_from_url(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("empty TCP URL"));
    }
    if let Some(rest) = trimmed.strip_prefix("tcp://") {
        return Ok(rest.trim_end_matches('/').to_string());
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Err(anyhow!("NAADS TCP stream URL must use tcp:// or host:port"));
    }
    Ok(trimmed.trim_end_matches('/').to_string())
}

fn drain_alert_xml_messages(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut messages = Vec::new();
    loop {
        let Some(alert_start) = find_subsequence(buffer, b"<alert") else {
            if buffer.len() > 1024 {
                let keep = buffer.split_off(buffer.len() - 1024);
                *buffer = keep;
            }
            break;
        };
        let start = find_subsequence(&buffer[..alert_start], b"<?xml").unwrap_or(alert_start);
        if start > 0 {
            buffer.drain(..start);
        }
        let search_from = alert_start.saturating_sub(start);
        let Some(end_rel) = find_subsequence(&buffer[search_from..], b"</alert>") else {
            break;
        };
        let end = search_from + end_rel + b"</alert>".len();
        messages.push(buffer[..end].to_vec());
        buffer.drain(..end);
    }
    messages
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or_default()
}

pub fn default_stream_urls() -> Vec<String> {
    vec![
        "tcp://streaming1.naad-adna.pelmorex.com:8080".to_string(),
        "tcp://streaming2.naad-adna.pelmorex.com:8080".to_string(),
    ]
}

pub fn default_archive_urls() -> Vec<String> {
    vec![
        "http://capcp1.naad-adna.pelmorex.com".to_string(),
        "http://capcp2.naad-adna.pelmorex.com".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn stream_message_drain_waits_for_complete_alert() {
        let mut buffer = b"noise<?xml version=\"1.0\"?><alert><identifier>a</identifier>".to_vec();
        assert!(drain_alert_xml_messages(&mut buffer).is_empty());
        buffer.extend_from_slice(b"</alert>trailing");
        let messages = drain_alert_xml_messages(&mut buffer);
        assert_eq!(messages.len(), 1);
        assert!(String::from_utf8_lossy(&messages[0]).starts_with("<?xml"));
        assert_eq!(String::from_utf8_lossy(&buffer), "trailing");
    }

    #[test]
    fn repository_url_matches_naads_filename_shape() {
        let reference = Reference {
            sender: "sender".to_string(),
            identifier: "2.49.0.1.124.abc".to_string(),
            sent: "2026-06-22T12:38:00-06:00".to_string(),
        };
        assert_eq!(
            archive_url("http://capcp1.naad-adna.pelmorex.com/", &reference).unwrap(),
            "http://capcp1.naad-adna.pelmorex.com/2026-06-22/2026_06_22T12_38_00_06_00I2.49.0.1.124.abc.xml"
        );
    }

    #[test]
    fn datamart_listing_keeps_only_safe_directories_and_cap_files() {
        let listing = br#"
            <a href="LAND/">LAND/</a>
            <a href="CWTO/">CWTO/</a>
            <a href="../">parent</a>
            <a href="T_ONCN00_C_LAND_202608121625_0588622998.cap">cap</a>
            <a href="not-a-cap.xml">xml</a>
            <a href="../../unsafe.cap">unsafe</a>
        "#;

        assert_eq!(listing_directories(listing), vec!["CWTO", "LAND"]);
        assert_eq!(
            listing_cap_files(listing),
            vec!["T_ONCN00_C_LAND_202608121625_0588622998.cap"]
        );
    }

    #[test]
    fn datamart_lookback_crosses_midnight_without_losing_the_prior_hour() {
        let now = Utc
            .with_ymd_and_hms(2026, 8, 12, 0, 5, 0)
            .single()
            .expect("UTC datetime");
        let buckets = datamart_date_hours(now);

        assert_eq!(buckets["20260811"], vec!["23"]);
        assert_eq!(buckets["20260812"], vec!["00"]);
    }
}
