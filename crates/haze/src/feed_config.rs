//! The daemon's ownership of managed feed config. It loads and validates
//! `managed/feeds` with `haze-feed-config`, answers `config.feeds.request` with
//! the snapshot, and broadcasts `config.feeds.updated` when the files change.
//! Services read coverage from that snapshot instead of parsing feed XML. See
//! docs/feed-config.md for the events.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use haze_feed_config::{load_snapshot, FeedConfigSnapshot};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::daemon_services::{decode_config_with_overlay, resolve_path};
use crate::ServiceHostConfig;

pub(crate) const REQUEST_EVENT: &str = "config.feeds.request";
const POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Default, Deserialize)]
struct RootConfig {
    feeds_file: Option<String>,
}

/// One file's identity for change detection: path, size, and modified time.
type Fingerprint = Vec<(PathBuf, u64, Option<SystemTime>)>;

pub(crate) struct FeedConfigService {
    path: PathBuf,
    snapshot: FeedConfigSnapshot,
    fingerprint: Fingerprint,
    next_poll: Instant,
    publisher: Sender<Value>,
}

impl FeedConfigService {
    /// Loads the feed config. An invalid config stops Haze before any service
    /// starts, so a bad edit cannot reach services half-applied.
    pub(crate) fn start(host: &ServiceHostConfig, publisher: Sender<Value>) -> Result<Self> {
        let config: RootConfig =
            decode_config_with_overlay(&host.config_path, &host.app_dir, &host.runtime_dir)?;
        let base_dir = host.config_path.parent().unwrap_or_else(|| Path::new("."));
        let configured = config
            .feeds_file
            .as_deref()
            .filter(|raw| !raw.trim().is_empty())
            .unwrap_or("managed/feeds");
        let path = resolve_path(base_dir, Path::new(configured));
        let fingerprint = fingerprint(&path);
        let snapshot = load_snapshot(&path).context("invalid feed config; fix it and start Haze again")?;
        info!(
            feeds = snapshot.feeds.len(),
            generation = %snapshot.generation,
            path = %path.display(),
            "feed config loaded"
        );
        Ok(Self {
            path,
            snapshot,
            fingerprint,
            next_poll: Instant::now() + POLL_INTERVAL,
            publisher,
        })
    }

    /// Answers `config.feeds.request` with the current snapshot, targeted at the
    /// requester's `reply_to` client id and echoing its `subject`.
    pub(crate) fn handle_event(&self, event: &Value) {
        if event.get("type").and_then(Value::as_str) != Some(REQUEST_EVENT) {
            return;
        }
        let Some(reply_to) = event
            .get("reply_to")
            .and_then(Value::as_str)
            .filter(|reply_to| !reply_to.trim().is_empty())
        else {
            warn!("ignoring config.feeds.request without reply_to");
            return;
        };
        let _ = self.publisher.send(json!({
            "type": "config.feeds.snapshot",
            "source": "haze",
            "target": reply_to,
            "subject": event.get("subject").cloned().unwrap_or(Value::Null),
            "data": self.snapshot,
        }));
    }

    /// Reloads after a feed file changes. A valid change is broadcast as
    /// `config.feeds.updated`; an invalid one keeps the last good snapshot and
    /// is reported as `config.feeds.invalid`.
    pub(crate) fn poll(&mut self) {
        let now = Instant::now();
        if now < self.next_poll {
            return;
        }
        self.next_poll = now + POLL_INTERVAL;
        let current = fingerprint(&self.path);
        if current == self.fingerprint {
            return;
        }
        self.fingerprint = current;
        match load_snapshot(&self.path) {
            Ok(snapshot) if snapshot.generation == self.snapshot.generation => {}
            Ok(snapshot) => {
                info!(
                    feeds = snapshot.feeds.len(),
                    generation = %snapshot.generation,
                    "feed config changed; publishing the new snapshot"
                );
                self.snapshot = snapshot;
                let _ = self.publisher.send(json!({
                    "type": "config.feeds.updated",
                    "source": "haze",
                    "data": self.snapshot,
                }));
            }
            Err(err) => {
                error!(%err, "feed config change rejected; services keep the previous config");
                let _ = self.publisher.send(json!({
                    "type": "config.feeds.invalid",
                    "source": "haze",
                    "data": {
                        "error": err.to_string(),
                        "generation": self.snapshot.generation,
                    },
                }));
            }
        }
    }
}

fn fingerprint(path: &Path) -> Fingerprint {
    let files = if path.is_dir() {
        fs::read_dir(path)
            .map(|entries| entries.filter_map(|entry| entry.ok().map(|entry| entry.path())).collect())
            .unwrap_or_default()
    } else {
        vec![path.to_path_buf()]
    };
    let mut out = files
        .into_iter()
        .filter_map(|file| {
            let metadata = fs::metadata(&file).ok()?;
            Some((file, metadata.len(), metadata.modified().ok()))
        })
        .collect::<Fingerprint>();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn service(dir: &Path) -> (FeedConfigService, mpsc::Receiver<Value>) {
        let config = dir.join("config.yaml");
        fs::write(&config, "feeds_file: ./feeds\n").expect("write config");
        let host = ServiceHostConfig {
            app_dir: dir.to_path_buf(),
            runtime_dir: dir.join("runtime"),
            config_path: config,
        };
        let (tx, rx) = mpsc::channel();
        (FeedConfigService::start(&host, tx).expect("start"), rx)
    }

    fn write_feed(dir: &Path, regions: &str) {
        fs::write(
            dir.join("feeds/cwxr-sk01.xml"),
            format!("<feed><locations><coverage>{regions}</coverage></locations></feed>"),
        )
        .expect("write feed");
    }

    #[test]
    fn requests_get_a_targeted_snapshot_and_changes_are_broadcast() {
        let dir = tempfile::tempdir().expect("temporary directory");
        fs::create_dir_all(dir.path().join("feeds")).expect("feeds directory");
        write_feed(dir.path(), r#"<region id="065100" source="eccc"/>"#);
        let (mut service, rx) = service(dir.path());

        service.handle_event(&json!({
            "type": REQUEST_EVENT, "reply_to": "haze-cap-alert-router", "subject": "req-1",
        }));
        let reply = rx.try_recv().expect("snapshot reply");
        assert_eq!(reply["type"], "config.feeds.snapshot");
        assert_eq!(reply["target"], "haze-cap-alert-router");
        assert_eq!(reply["subject"], "req-1");
        assert_eq!(reply["data"]["feeds"][0]["feed_id"], "cwxr-sk01");

        write_feed(dir.path(), r#"<region id="065100" source="eccc"/><region id="0654*" source="eccc"/>"#);
        service.next_poll = Instant::now();
        service.poll();
        let updated = rx.try_recv().expect("update broadcast");
        assert_eq!(updated["type"], "config.feeds.updated");
        assert_eq!(updated["data"]["feeds"][0]["entries"][1]["id"], "0654*");

        write_feed(dir.path(), r#"<region id="06*54" source="eccc"/>"#);
        service.next_poll = Instant::now();
        service.poll();
        let invalid = rx.try_recv().expect("invalid report");
        assert_eq!(invalid["type"], "config.feeds.invalid");
        assert!(invalid["data"]["error"].as_str().unwrap_or_default().contains("06*54"));
        assert_eq!(service.snapshot.feeds[0].entries.len(), 2, "kept the last good snapshot");
    }
}
