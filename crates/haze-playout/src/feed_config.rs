//! Playout's copy of the daemon's feed config snapshot. The CAP router fetches
//! it on connect and stores every update here; other playout code reads it
//! instead of parsing feed XML.

use std::sync::{Arc, RwLock};

use haze_feed_config::FeedConfigSnapshot;

static CURRENT: RwLock<Option<Arc<FeedConfigSnapshot>>> = RwLock::new(None);

pub(crate) fn set(snapshot: Arc<FeedConfigSnapshot>) {
    *CURRENT.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(snapshot);
}

fn current() -> Option<Arc<FeedConfigSnapshot>> {
    CURRENT
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// Whether the feed is configured with no coverage, so it takes alerts for
/// every location. False until the snapshot arrives, so a feed never plays
/// another feed's alerts by mistake.
pub(crate) fn has_no_coverage(feed_id: &str) -> bool {
    current()
        .and_then(|snapshot| snapshot.feed(feed_id).map(|feed| feed.entries.is_empty()))
        .unwrap_or(false)
}
