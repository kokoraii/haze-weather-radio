use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use futures::{stream, StreamExt};
use reqwest::header::{HeaderMap, ACCEPT, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::bridge::{EventEnvelope, EventPublisher};
use crate::model::{Alert, AtomEntry};
use crate::parse::{parse_atom_entries, parse_cap};

const MAX_BODY_BYTES: usize = 5 << 20;
const POLLER_STATE_DIR: &str = "runtime/state/cap-ingest";
const MAX_SEEN_ENTRIES: usize = 5_000;
const MAX_SEEN_ALERTS: usize = 5_000;

#[derive(Clone, Debug)]
pub struct SourceConfig {
    pub id: String,
    pub source: SourceKind,
    pub urls: Vec<String>,
    pub interval: Duration,
    pub timeout: Duration,
    pub user_agent: String,
    pub shadow: bool,
    pub startup_seed: bool,
    pub concurrency: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceKind {
    Naads,
    Nws,
    Custom(String),
}

impl SourceKind {
    pub fn from_raw(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "naads" | "cap-cp" | "cap_cp" => Self::Naads,
            "nws" | "nws-cap" | "nws_cap" => Self::Nws,
            other => Self::Custom(other.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Naads => "naads",
            Self::Nws => "nws",
            Self::Custom(value) => value.as_str(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct EndpointCache {
    etag: Option<String>,
    last_modified: Option<String>,
}

#[derive(Clone, Debug)]
struct AtomFetch {
    url: String,
    status: FetchStatus,
    entries: Vec<AtomEntry>,
    headers: HeaderMap,
    fetch_ms: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FetchStatus {
    Modified,
    NotModified,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct AlertKey {
    identifier: String,
    updated: String,
    sent: String,
    message_type: String,
    references: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct PollerCheckpoint {
    #[serde(default)]
    seeded_startup: bool,
    #[serde(default)]
    seen_entries: VecDeque<(String, String)>,
    #[serde(default)]
    seen_alerts: VecDeque<AlertKey>,
    #[serde(default)]
    endpoint_cache: HashMap<String, EndpointCache>,
}

pub struct Poller {
    source: SourceConfig,
    client: Client,
    publisher: EventPublisher,
    state_path: PathBuf,
    endpoint_cache: HashMap<String, EndpointCache>,
    seen_entries: HashSet<(String, String)>,
    seen_entries_order: VecDeque<(String, String)>,
    seen_alerts: HashSet<AlertKey>,
    seen_alerts_order: VecDeque<AlertKey>,
    seeded_startup: bool,
    checkpoint_dirty: bool,
}

impl Poller {
    pub fn new(source: SourceConfig, publisher: EventPublisher) -> Result<Self> {
        let client = Client::builder()
            .timeout(source.timeout)
            .user_agent(source.user_agent.clone())
            .build()
            .context("failed to create CAP HTTP client")?;
        let state_path = poller_state_path(&source.id);
        let checkpoint = load_checkpoint(&state_path)?;
        let mut seen_entries_order = checkpoint.seen_entries;
        while seen_entries_order.len() > MAX_SEEN_ENTRIES {
            seen_entries_order.pop_front();
        }
        let seen_entries = seen_entries_order.iter().cloned().collect();
        let mut seen_alerts_order = checkpoint.seen_alerts;
        while seen_alerts_order.len() > MAX_SEEN_ALERTS {
            seen_alerts_order.pop_front();
        }
        let seen_alerts = seen_alerts_order.iter().cloned().collect();
        Ok(Self {
            source,
            client,
            publisher,
            state_path,
            endpoint_cache: checkpoint.endpoint_cache,
            seen_entries,
            seen_entries_order,
            seen_alerts,
            seen_alerts_order,
            seeded_startup: checkpoint.seeded_startup,
            checkpoint_dirty: false,
        })
    }

    pub async fn run(&mut self, once: bool) -> Result<()> {
        loop {
            if let Err(err) = self.poll_once().await {
                warn!(
                    source = self.source.id,
                    source_kind = self.source.source.as_str(),
                    "CAP poll failed: {err:#}"
                );
                self.publish_status(json!({
                    "source_id": self.source.id,
                    "source": self.source.source.as_str(),
                    "shadow": self.source.shadow,
                    "status": "error",
                    "error": err.to_string(),
                    "timestamp_unix_ms": unix_ms(),
                }))
                .await
                .ok();
            }
            if once {
                return Ok(());
            }
            sleep(self.source.interval).await;
        }
    }

    async fn poll_once(&mut self) -> Result<()> {
        let poll_started = Instant::now();
        let fetch = self.fetch_atom().await?;
        if fetch.status == FetchStatus::NotModified {
            self.publish_status(json!({
                "source_id": self.source.id,
                "source": self.source.source.as_str(),
                "shadow": self.source.shadow,
                "status": "not_modified",
                "url": fetch.url,
                "fetch_ms": fetch.fetch_ms,
                "poll_ms": poll_started.elapsed().as_millis(),
                "timestamp_unix_ms": unix_ms(),
            }))
            .await?;
            self.persist_checkpoint().await?;
            return Ok(());
        }

        let mut entries = fetch.entries;
        entries.sort_by(|left, right| {
            right
                .updated
                .cmp(&left.updated)
                .then(right.id.cmp(&left.id))
        });
        entries.dedup_by(|left, right| left.id == right.id && left.updated == right.updated);

        if self.source.startup_seed && !self.seeded_startup {
            let count = entries
                .iter()
                .filter(|entry| self.remember_entry((entry.id.clone(), entry.updated.clone())))
                .count();
            self.seeded_startup = true;
            self.checkpoint_dirty = true;
            info!(source = self.source.id, count, "seeded CAP startup entries");
            self.persist_checkpoint().await?;
            self.publish_status(json!({
                "source_id": self.source.id,
                "source": self.source.source.as_str(),
                "shadow": self.source.shadow,
                "status": "seeded",
                "url": fetch.url,
                "entries": entries.len(),
                "seeded": count,
                "fetch_ms": fetch.fetch_ms,
                "poll_ms": poll_started.elapsed().as_millis(),
                "timestamp_unix_ms": unix_ms(),
            }))
            .await?;
            return Ok(());
        }
        if !self.seeded_startup {
            self.seeded_startup = true;
            self.checkpoint_dirty = true;
        }

        let fresh_entries = entries
            .into_iter()
            .filter(|entry| {
                !self
                    .seen_entries
                    .contains(&(entry.id.clone(), entry.updated.clone()))
            })
            .collect::<Vec<_>>();
        let fresh_entry_count = fresh_entries.len();

        let cap_started = Instant::now();
        let mut published = 0usize;
        let mut deduped = 0usize;
        let source_id = self.source.id.clone();
        let source_kind = self.source.source.as_str().to_string();
        let shadow = self.source.shadow;
        let atom_url = fetch.url.clone();
        let publisher = self.publisher.clone();
        let mut pending_entries = HashMap::<AlertKey, Vec<(String, String)>>::new();
        let mut publish_tasks = JoinSet::new();
        let http_client = self.client.clone();
        let cap_fetches = stream::iter(fresh_entries.into_iter().map(move |entry| {
            let client = http_client.clone();
            async move {
                let atom_key = (entry.id.clone(), entry.updated.clone());
                (atom_key, fetch_first_cap(client, entry).await)
            }
        }))
        .buffer_unordered(self.source.concurrency.max(1));
        tokio::pin!(cap_fetches);
        while let Some((atom_key, fetched)) = cap_fetches.next().await {
            let fetched = match fetched {
                Ok(fetched) => fetched,
                Err(err) => {
                    warn!(
                        source = self.source.id,
                        "CAP fetch skipped and will be retried: {err:#}"
                    );
                    continue;
                }
            };
            let key = AlertKey {
                identifier: fetched.alert.identifier.clone(),
                updated: fetched.entry.updated.clone(),
                sent: fetched.alert.sent.clone(),
                message_type: fetched.alert.message_type.clone(),
                references: fetched.alert.references.clone(),
            };
            if self.seen_alerts.contains(&key) {
                self.remember_entry(atom_key);
                deduped += 1;
                debug!(
                    source = self.source.id,
                    identifier = fetched.alert.identifier,
                    updated = fetched.entry.updated,
                    "deduped CAP alert"
                );
                continue;
            }
            if let Some(entries) = pending_entries.get_mut(&key) {
                entries.push(atom_key);
                deduped += 1;
                continue;
            }

            let alert_value = serde_json::to_value(&fetched.alert)?;
            let ingest = json!({
                "source": source_kind,
                "source_id": source_id,
                "shadow": shadow,
                "atom_id": fetched.entry.id,
                "atom_updated": fetched.entry.updated,
                "atom_url": atom_url,
                "cap_url": fetched.url,
                "fetch_latency_ms": fetched.fetch_ms,
                "parse_latency_ms": fetched.parse_ms,
                "publish_started_unix_ms": unix_ms(),
            });
            if shadow {
                self.remember_alert(key);
                self.remember_entry(atom_key);
                self.publish_status(json!({
                    "source_id": self.source.id,
                    "source": self.source.source.as_str(),
                    "shadow": true,
                    "status": "would_publish",
                    "identifier": fetched.alert.identifier,
                    "message_type": fetched.alert.message_type,
                    "atom_updated": fetched.entry.updated,
                    "cap_url": fetched.url,
                    "fetch_ms": fetched.fetch_ms,
                    "parse_ms": fetched.parse_ms,
                    "timestamp_unix_ms": unix_ms(),
                }))
                .await?;
            } else {
                pending_entries.insert(key.clone(), vec![atom_key]);
                let event = EventEnvelope::cap_alert(&source_id, alert_value, ingest);
                let publisher = publisher.clone();
                publish_tasks.spawn(async move {
                    let result = publisher.publish(&event).await;
                    (key, result)
                });
            }
        }

        let mut publish_error = None;
        while let Some(result) = publish_tasks.join_next().await {
            let (key, result) = result.context("CAP publisher task failed")?;
            match result {
                Ok(()) => {
                    self.remember_alert(key.clone());
                    if let Some(entries) = pending_entries.remove(&key) {
                        for entry in entries {
                            self.remember_entry(entry);
                        }
                    }
                    published += 1;
                }
                Err(err) => {
                    pending_entries.remove(&key);
                    warn!(
                        source = self.source.id,
                        "CAP publish failed and will be retried: {err:#}"
                    );
                    publish_error.get_or_insert(err);
                }
            }
        }

        self.persist_checkpoint().await?;
        self.publish_status(json!({
            "source_id": self.source.id,
            "source": self.source.source.as_str(),
            "shadow": self.source.shadow,
            "status": "ok",
            "url": fetch.url,
            "entries": self.seen_entries.len(),
            "fresh_entries": fresh_entry_count,
            "published": published,
            "deduped": deduped,
            "fetch_ms": fetch.fetch_ms,
            "cap_fetch_parse_ms": cap_started.elapsed().as_millis(),
            "poll_ms": poll_started.elapsed().as_millis(),
            "timestamp_unix_ms": unix_ms(),
        }))
        .await?;
        if let Some(err) = publish_error {
            return Err(err);
        }
        Ok(())
    }

    async fn fetch_atom(&mut self) -> Result<AtomFetch> {
        if self.source.urls.is_empty() {
            return Err(anyhow!("no Atom URLs configured"));
        }
        let client = self.client.clone();
        let fetches = self
            .source
            .urls
            .iter()
            .cloned()
            .map(|url| {
                let cache = self.endpoint_cache.get(&url).cloned().unwrap_or_default();
                fetch_atom_url(client.clone(), url, cache)
            })
            .collect::<Vec<_>>();

        let mut last_error = None;
        let mut not_modified = None;
        let mut stream = stream::iter(fetches).buffer_unordered(self.source.urls.len());
        while let Some(result) = stream.next().await {
            match result {
                Ok(fetch) if fetch.status == FetchStatus::Modified => {
                    self.store_endpoint_headers(&fetch.url, &fetch.headers);
                    return Ok(fetch);
                }
                Ok(fetch) => {
                    self.store_endpoint_headers(&fetch.url, &fetch.headers);
                    not_modified = Some(fetch);
                }
                Err(err) => last_error = Some(err),
            }
        }
        if let Some(fetch) = not_modified {
            return Ok(fetch);
        }
        Err(last_error.unwrap_or_else(|| anyhow!("all Atom endpoints failed")))
    }

    fn store_endpoint_headers(&mut self, url: &str, headers: &HeaderMap) {
        let etag = headers.get(ETAG).and_then(|value| value.to_str().ok());
        let last_modified = headers
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok());
        if etag.is_none() && last_modified.is_none() {
            return;
        }
        let cache = self.endpoint_cache.entry(url.to_string()).or_default();
        if let Some(value) = etag {
            if cache.etag.as_deref() != Some(value) {
                cache.etag = Some(value.to_string());
                self.checkpoint_dirty = true;
            }
        }
        if let Some(value) = last_modified {
            if cache.last_modified.as_deref() != Some(value) {
                cache.last_modified = Some(value.to_string());
                self.checkpoint_dirty = true;
            }
        }
    }

    fn remember_entry(&mut self, key: (String, String)) -> bool {
        if !self.seen_entries.insert(key.clone()) {
            return false;
        }
        self.seen_entries_order.push_back(key);
        self.checkpoint_dirty = true;
        while self.seen_entries_order.len() > MAX_SEEN_ENTRIES {
            if let Some(expired) = self.seen_entries_order.pop_front() {
                self.seen_entries.remove(&expired);
            }
        }
        true
    }

    fn remember_alert(&mut self, key: AlertKey) -> bool {
        if !self.seen_alerts.insert(key.clone()) {
            return false;
        }
        self.seen_alerts_order.push_back(key);
        self.checkpoint_dirty = true;
        while self.seen_alerts_order.len() > MAX_SEEN_ALERTS {
            if let Some(expired) = self.seen_alerts_order.pop_front() {
                self.seen_alerts.remove(&expired);
            }
        }
        true
    }

    async fn persist_checkpoint(&mut self) -> Result<()> {
        if !self.checkpoint_dirty {
            return Ok(());
        }
        let parent = self
            .state_path
            .parent()
            .expect("CAP poller state path has a parent");
        tokio::fs::create_dir_all(parent).await.with_context(|| {
            format!(
                "failed to create CAP poller state directory {}",
                parent.display()
            )
        })?;
        let checkpoint = PollerCheckpoint {
            seeded_startup: self.seeded_startup,
            seen_entries: self.seen_entries_order.clone(),
            seen_alerts: self.seen_alerts_order.clone(),
            endpoint_cache: self.endpoint_cache.clone(),
        };
        let raw = serde_json::to_vec(&checkpoint).context("failed to encode CAP poller state")?;
        let temporary = self.state_path.with_extension("json.tmp");
        let mut file = tokio::fs::File::create(&temporary).await.with_context(|| {
            format!(
                "failed to create CAP poller state file {}",
                temporary.display()
            )
        })?;
        file.write_all(&raw).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temporary, &self.state_path)
            .await
            .with_context(|| {
                format!(
                    "failed to commit CAP poller state {}",
                    self.state_path.display()
                )
            })?;
        self.checkpoint_dirty = false;
        Ok(())
    }
}

fn poller_state_path(source_id: &str) -> PathBuf {
    let digest = Sha256::digest(source_id.as_bytes());
    PathBuf::from(POLLER_STATE_DIR).join(format!("{:x}.json", digest))
}

fn load_checkpoint(path: &PathBuf) -> Result<PollerCheckpoint> {
    match std::fs::read(path) {
        Ok(raw) => serde_json::from_slice(&raw)
            .with_context(|| format!("failed to parse CAP poller state {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(PollerCheckpoint::default()),
        Err(err) => {
            Err(err).with_context(|| format!("failed to read CAP poller state {}", path.display()))
        }
    }
}

async fn fetch_first_cap(client: Client, entry: AtomEntry) -> Result<FetchedAlert> {
    let mut last_error = None;
    for link in entry.links.clone() {
        let started = Instant::now();
        let result = client
            .get(&link)
            .header(
                ACCEPT,
                "application/cap+xml, application/xml;q=0.9, */*;q=0.1",
            )
            .send()
            .await
            .with_context(|| format!("failed to fetch CAP from {link}"));
        let Ok(response) = result else {
            last_error = result.err();
            continue;
        };
        if !response.status().is_success() {
            last_error = Some(anyhow!(
                "unexpected HTTP status {} from {link}",
                response.status()
            ));
            continue;
        }
        let body = match read_limited(response).await {
            Ok(body) => body,
            Err(err) => {
                last_error = Some(err);
                continue;
            }
        };
        let fetch_ms = started.elapsed().as_millis();
        let parse_started = Instant::now();
        match parse_cap(&body) {
            Ok(alert) if !alert.identifier.is_empty() => {
                return Ok(FetchedAlert {
                    entry,
                    alert,
                    url: link,
                    fetch_ms,
                    parse_ms: parse_started.elapsed().as_millis(),
                });
            }
            Ok(_) => last_error = Some(anyhow!("CAP alert from {link} had empty identifier")),
            Err(err) => last_error = Some(err.into()),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("no parseable CAP alert found")))
}

impl Poller {
    async fn publish_status(&self, data: serde_json::Value) -> Result<()> {
        self.publisher
            .publish(&EventEnvelope::status(
                &self.source.id,
                &format!("{}:{}", self.source.source.as_str(), self.source.id),
                data,
            ))
            .await
    }
}

#[derive(Debug)]
struct FetchedAlert {
    entry: AtomEntry,
    alert: Alert,
    url: String,
    fetch_ms: u128,
    parse_ms: u128,
}

async fn fetch_atom_url(client: Client, url: String, cache: EndpointCache) -> Result<AtomFetch> {
    let started = Instant::now();
    let mut request = client.get(&url).header(
        ACCEPT,
        "application/atom+xml, application/xml;q=0.9, */*;q=0.1",
    );
    if let Some(etag) = cache.etag {
        request = request.header(IF_NONE_MATCH, etag);
    }
    if let Some(last_modified) = cache.last_modified {
        request = request.header(IF_MODIFIED_SINCE, last_modified);
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("failed to fetch Atom from {url}"))?;
    let status = response.status();
    let headers = response.headers().clone();
    if status == StatusCode::NOT_MODIFIED {
        return Ok(AtomFetch {
            url,
            status: FetchStatus::NotModified,
            entries: Vec::new(),
            headers,
            fetch_ms: started.elapsed().as_millis(),
        });
    }
    if !status.is_success() {
        return Err(anyhow!("unexpected HTTP status {status} from {url}"));
    }
    let body = read_limited(response).await?;
    let entries = parse_atom_entries(&body)?;
    Ok(AtomFetch {
        url,
        status: FetchStatus::Modified,
        entries,
        headers,
        fetch_ms: started.elapsed().as_millis(),
    })
}

async fn read_limited(response: reqwest::Response) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BODY_BYTES as u64)
    {
        return Err(anyhow!("response exceeds {MAX_BODY_BYTES} bytes"));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
            return Err(anyhow!("response exceeds {MAX_BODY_BYTES} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or_default()
}

pub fn default_atom_urls(kind: &SourceKind) -> Vec<String> {
    match kind {
        SourceKind::Naads => Vec::new(),
        SourceKind::Nws => vec!["https://api.weather.gov/alerts/active.atom".to_string()],
        SourceKind::Custom(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_kind_atom_defaults_are_source_specific() {
        assert!(default_atom_urls(&SourceKind::from_raw("naads")).is_empty());
        assert_eq!(
            default_atom_urls(&SourceKind::from_raw("nws"))[0],
            "https://api.weather.gov/alerts/active.atom"
        );
    }

    #[tokio::test]
    async fn unchanged_endpoint_headers_do_not_rewrite_checkpoint() {
        let source_id = format!("test-source-{}", unix_ms());
        let mut poller = Poller::new(
            SourceConfig {
                id: source_id.clone(),
                source: SourceKind::Nws,
                urls: vec!["https://example.test/alerts.atom".to_string()],
                interval: Duration::from_secs(5),
                timeout: Duration::from_secs(15),
                user_agent: "haze-test".to_string(),
                shadow: false,
                startup_seed: false,
                concurrency: 1,
            },
            EventPublisher::new(None, source_id),
        )
        .expect("construct poller");
        poller.state_path = std::env::temp_dir().join(format!(
            "haze-cap-headers-test-{}-{}.json",
            std::process::id(),
            unix_ms()
        ));
        poller.persist_checkpoint().await.expect("clean checkpoint");
        assert!(!poller.state_path.exists());

        let mut headers = HeaderMap::new();
        headers.insert(ETAG, "etag-1".parse().expect("ETag header"));
        poller.store_endpoint_headers("https://example.test/alerts.atom", &headers);
        assert!(poller.checkpoint_dirty);
        poller
            .persist_checkpoint()
            .await
            .expect("changed checkpoint");
        assert!(!poller.checkpoint_dirty);
        assert!(poller.state_path.exists());

        poller.store_endpoint_headers("https://example.test/alerts.atom", &headers);
        assert!(!poller.checkpoint_dirty);
        poller
            .persist_checkpoint()
            .await
            .expect("unchanged checkpoint");
        std::fs::remove_file(&poller.state_path).expect("remove temporary checkpoint");
    }

    #[tokio::test]
    async fn startup_seed_checkpoint_survives_restart_and_bounds_seen_entries() {
        let state_path = std::env::temp_dir().join(format!(
            "haze-cap-poller-test-{}-{}.json",
            std::process::id(),
            unix_ms()
        ));
        let mut poller = Poller::new(
            SourceConfig {
                id: "test-source".to_string(),
                source: SourceKind::Nws,
                urls: vec!["https://example.test/alerts.atom".to_string()],
                interval: Duration::from_secs(5),
                timeout: Duration::from_secs(15),
                user_agent: "haze-test".to_string(),
                shadow: false,
                startup_seed: true,
                concurrency: 1,
            },
            EventPublisher::new(None, "test-source"),
        )
        .expect("construct poller");
        poller.state_path = state_path.clone();
        poller.seeded_startup = true;
        for entry in 0..(MAX_SEEN_ENTRIES + 3) {
            poller.remember_entry((format!("entry-{entry}"), "updated".to_string()));
        }

        poller
            .persist_checkpoint()
            .await
            .expect("persist checkpoint");
        let checkpoint = load_checkpoint(&poller.state_path).expect("reload checkpoint");

        assert!(checkpoint.seeded_startup);
        assert_eq!(checkpoint.seen_entries.len(), MAX_SEEN_ENTRIES);
        assert!(!checkpoint
            .seen_entries
            .iter()
            .any(|(id, _)| id == "entry-0"));
        assert!(checkpoint
            .seen_entries
            .iter()
            .any(|(id, _)| id == &format!("entry-{}", MAX_SEEN_ENTRIES + 2)));
        std::fs::remove_file(state_path).expect("remove temporary poller state");
    }
}
