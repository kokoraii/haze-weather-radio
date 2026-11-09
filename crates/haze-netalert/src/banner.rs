//! On-air banner state per feed, driven by playout lifecycle events.
//!
//! Product render pre-renders each alert's banner card into the alert packet
//! (`presentation.banner`), so this board only tracks what is airing and
//! relays those cards. It publishes `banner.state.updated` for CGEN and pushes
//! the same payload to banner WebSocket subscribers whenever a feed changes.

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::util::{make_token, value_list, value_text};

pub const ALL_FEEDS: &str = "*";
const ON_AIR_TTL: Duration = Duration::from_secs(30 * 60);
const COMPLETED_GRACE: Duration = Duration::from_secs(15);
const SEVERITY_ORDER: [&str; 5] = ["Extreme", "Severe", "Moderate", "Minor", "Unknown"];
const WARNING_GRADIENT: [&str; 2] = ["#931102", "#370c16"];
const WATCH_GRADIENT: [&str; 2] = ["#929301", "#37380b"];
const ADVISORY_GRADIENT: [&str; 2] = ["#019310", "#0b3810"];

#[derive(Debug, Clone)]
struct OnAir {
    queue_id: String,
    alert_id: String,
    view: Value,
    expires_at: Instant,
}

/// The `banner.state.updated` payload shape CGEN and `banner.html` consume.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BannerPayload {
    pub active: bool,
    pub signature: String,
    pub feed_id: String,
    pub feed_name: String,
    pub generated_at: String,
    pub primary_color: String,
    pub primary_gradient: Vec<String>,
    pub alerts: Vec<Value>,
}

impl BannerPayload {
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("banner payload serializes")
    }
}

pub struct BannerBoard {
    on_air: HashMap<String, OnAir>,
    known_feeds: BTreeSet<String>,
    published: HashMap<String, String>,
}

impl BannerBoard {
    pub fn new(known_feeds: BTreeSet<String>) -> Self {
        let mut known_feeds = known_feeds;
        known_feeds.insert(ALL_FEEDS.to_string());
        Self {
            on_air: HashMap::new(),
            known_feeds,
            published: HashMap::new(),
        }
    }

    /// Applies a playout lifecycle event. Returns true when on-air state changed.
    pub fn apply(&mut self, event: &Value, now: Instant) -> bool {
        let kind = value_text(event.get("type")).unwrap_or_default();
        let data = event
            .get("data")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let queue_id = value_text(data.get("queue_id"))
            .or_else(|| value_text(event.get("queue_id")))
            .unwrap_or_default();
        let feeds = event_feed_ids(event, &data);
        let before = self.snapshot();
        for feed in &feeds {
            self.known_feeds.insert(feed.clone());
            match kind.as_str() {
                "alert.playout.started" => self.start(feed, &queue_id, event, &data, now),
                "alert.playout.completed" => self.complete(feed, &queue_id, &data, now),
                "playout.interrupted" => self.clear(feed),
                _ => {}
            }
        }
        self.snapshot() != before
    }

    /// Drops expired on-air entries. Returns true when any were removed.
    pub fn prune(&mut self, now: Instant) -> bool {
        let before = self.on_air.len();
        self.on_air.retain(|_, entry| entry.expires_at > now);
        self.on_air.len() != before
    }

    pub fn payload(&self, feed: &str, now: DateTime<Utc>) -> BannerPayload {
        let mut alerts = self
            .on_air
            .iter()
            .filter(|(key, _)| {
                feed == ALL_FEEDS || key.as_str() == feed || key.as_str() == ALL_FEEDS
            })
            .map(|(_, entry)| entry.view.clone())
            .collect::<Vec<_>>();
        alerts.sort_by_key(sort_key);
        let primary_gradient = alerts
            .first()
            .and_then(|alert| alert.get("background_gradient"))
            .map(|gradient| value_list(Some(gradient)))
            .filter(|gradient| !gradient.is_empty())
            .unwrap_or_else(|| ADVISORY_GRADIENT.map(str::to_string).to_vec());
        let feed_name = if feed == ALL_FEEDS {
            String::new()
        } else {
            alerts
                .iter()
                .find_map(|alert| value_text(alert.get("feed_name")))
                .unwrap_or_else(|| feed.to_string())
        };
        let signature =
            make_token(&json!([!alerts.is_empty(), feed, feed_name, alerts]).to_string());
        BannerPayload {
            active: !alerts.is_empty(),
            signature,
            feed_id: feed.to_string(),
            feed_name,
            generated_at: now.to_rfc3339(),
            primary_color: primary_gradient[0].clone(),
            primary_gradient,
            alerts,
        }
    }

    /// Payloads whose content changed since they were last taken, for every
    /// feed the board knows. The first call returns every feed.
    pub fn take_changed(&mut self, now: DateTime<Utc>) -> Vec<BannerPayload> {
        let mut changed = Vec::new();
        for feed in self.known_feeds.clone() {
            let payload = self.payload(&feed, now);
            if self.published.get(&feed) != Some(&payload.signature) {
                self.published.insert(feed, payload.signature.clone());
                changed.push(payload);
            }
        }
        changed
    }

    fn start(
        &mut self,
        feed: &str,
        queue_id: &str,
        event: &Value,
        data: &Map<String, Value>,
        now: Instant,
    ) {
        if let Some(active) = self.on_air.get(feed) {
            // The first item of an airing owns the banner until it ends.
            if active.queue_id != queue_id && now < active.expires_at {
                return;
            }
        }
        let view = banner_view(feed, queue_id, event, data);
        self.on_air.insert(
            feed.to_string(),
            OnAir {
                queue_id: queue_id.to_string(),
                alert_id: value_text(view.get("identifier")).unwrap_or_default(),
                view,
                expires_at: now + ON_AIR_TTL,
            },
        );
    }

    fn complete(&mut self, feed: &str, queue_id: &str, data: &Map<String, Value>, now: Instant) {
        if feed == ALL_FEEDS {
            self.on_air.clear();
            return;
        }
        let Some(active) = self.on_air.get_mut(feed) else {
            return;
        };
        let alert_id = value_text(data.get("alert_id")).unwrap_or_default();
        if active.queue_id != queue_id && !alert_id.is_empty() && alert_id != active.alert_id {
            return;
        }
        if active.queue_id == queue_id || queue_ends_alert(queue_id, data) {
            self.clear(feed);
        } else {
            // Another part of the same airing finished; keep the banner briefly
            // so the next part's start can take over without a flicker.
            active.expires_at = now + COMPLETED_GRACE;
        }
    }

    fn clear(&mut self, feed: &str) {
        if feed == ALL_FEEDS {
            self.on_air.clear();
        } else {
            self.on_air.remove(feed);
            self.on_air.remove(ALL_FEEDS);
        }
    }

    fn snapshot(&self) -> Vec<(String, String)> {
        let mut entries = self
            .on_air
            .iter()
            .map(|(feed, entry)| (feed.clone(), entry.queue_id.clone()))
            .collect::<Vec<_>>();
        entries.sort();
        entries
    }
}

fn event_feed_ids(event: &Value, data: &Map<String, Value>) -> Vec<String> {
    let mut feeds = Vec::new();
    for value in [
        event.get("feed_id"),
        event.get("feed_ids"),
        data.get("feed_id"),
        data.get("feed_ids"),
    ] {
        for feed in value_list(value) {
            if !feeds.contains(&feed) {
                feeds.push(feed);
            }
        }
    }
    feeds
}

fn queue_ends_alert(queue_id: &str, data: &Map<String, Value>) -> bool {
    let queue_id = queue_id.to_ascii_lowercase();
    let package = value_text(data.get("pkg_id"))
        .unwrap_or_default()
        .to_ascii_lowercase();
    ["same_eom", "same_full"]
        .iter()
        .any(|marker| queue_id.contains(marker))
        || package.contains("eom")
        || package.contains("full")
}

/// The pre-rendered card from the packet, or a minimal card for alerts that
/// have none (operator break-ins, manual and EAS NET alerts).
fn banner_view(feed: &str, queue_id: &str, event: &Value, data: &Map<String, Value>) -> Value {
    let packet = data.get("alert_packet").filter(|packet| packet.is_object());
    let text = |key: &str| value_text(data.get(key)).or_else(|| value_text(event.get(key)));
    let mut view = packet
        .and_then(|packet| packet.pointer("/presentation/banner"))
        .filter(|banner| banner.is_object())
        .cloned()
        .unwrap_or_else(|| fallback_view(packet, queue_id, &text));
    let object = view.as_object_mut().expect("banner view is an object");
    object.insert("feed_id".to_string(), json!(feed));
    let message = text("banner_text").or_else(|| {
        packet.and_then(|packet| value_text(packet.pointer("/presentation/banner_text")))
    });
    if let Some(message) = message {
        object.insert("message".to_string(), json!(message));
    }
    view
}

fn fallback_view(
    packet: Option<&Value>,
    queue_id: &str,
    text: &dyn Fn(&str) -> Option<String>,
) -> Value {
    let content = |key: &str| {
        packet.and_then(|packet| value_text(packet.pointer(&format!("/content/{key}"))))
    };
    let timing =
        |key: &str| packet.and_then(|packet| value_text(packet.pointer(&format!("/timing/{key}"))));
    let event = content("event")
        .or_else(|| text("event"))
        .unwrap_or_else(|| "Alert".to_string());
    let headline = content("headline")
        .or_else(|| content("title"))
        .or_else(|| text("header"))
        .unwrap_or_else(|| event.clone());
    let areas = packet
        .map(|packet| value_list(packet.pointer("/areas/names")))
        .unwrap_or_default();
    let broadcast_immediate = packet
        .and_then(|packet| packet.pointer("/options/broadcast_immediate"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || text("broadcast_immediate").is_some_and(|value| value == "true");
    let color = text("background_color").or_else(|| content("background_color"));
    let gradient = fallback_gradient(color.as_deref(), broadcast_immediate);
    json!({
        "identifier": packet
            .and_then(|packet| value_text(packet.get("id")))
            .or_else(|| text("alert_id"))
            .unwrap_or_else(|| queue_id.to_string()),
        "headline": headline,
        "issuer": "",
        "event": event,
        "severity": content("severity").unwrap_or_else(|| "Unknown".to_string()),
        "urgency": content("urgency").unwrap_or_default(),
        "certainty": content("certainty").unwrap_or_default(),
        "areas": areas,
        "area_text": areas.join(", "),
        "received_at": timing("sent_at").unwrap_or_default(),
        "expires_at": timing("expires_at").unwrap_or_default(),
        "description": content("description").unwrap_or_default(),
        "instruction": content("instruction").unwrap_or_default(),
        "message": text("alert_text").unwrap_or_else(|| headline.clone()),
        "background_color": gradient[0],
        "background_gradient": gradient,
    })
}

/// Product render picks the background color from the same palette, so a known
/// color maps back to its gradient.
fn fallback_gradient(color: Option<&str>, broadcast_immediate: bool) -> Vec<String> {
    if broadcast_immediate {
        return WARNING_GRADIENT.map(str::to_string).to_vec();
    }
    let Some(color) = color.map(str::trim).filter(|color| !color.is_empty()) else {
        return ADVISORY_GRADIENT.map(str::to_string).to_vec();
    };
    [WARNING_GRADIENT, WATCH_GRADIENT, ADVISORY_GRADIENT]
        .into_iter()
        .find(|gradient| gradient[0].eq_ignore_ascii_case(color))
        .map(|gradient| gradient.map(str::to_string).to_vec())
        .unwrap_or_else(|| vec![color.to_string(), color.to_string()])
}

/// Most severe first, then newest, then by identifier.
fn sort_key(view: &Value) -> (usize, std::cmp::Reverse<String>, String) {
    let severity = value_text(view.get("severity")).unwrap_or_default();
    let rank = SEVERITY_ORDER
        .iter()
        .position(|value| value.eq_ignore_ascii_case(&severity))
        .unwrap_or(SEVERITY_ORDER.len() - 1);
    let timestamp = ["effective_at", "received_at", "onset_at"]
        .iter()
        .find_map(|key| value_text(view.get(*key)))
        .and_then(|raw| DateTime::parse_from_rfc3339(&raw).ok())
        .map(|time| time.with_timezone(&Utc).to_rfc3339())
        .unwrap_or_default();
    (
        rank,
        std::cmp::Reverse(timestamp),
        value_text(view.get("identifier")).unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn started(feed: &str, queue: &str, packet: Value) -> Value {
        json!({
            "type": "alert.playout.started",
            "feed_ids": [feed],
            "queue_id": queue,
            "data": {"feed_id": feed, "queue_id": queue, "alert_packet": packet}
        })
    }

    fn rendered_packet(id: &str, severity: &str) -> Value {
        json!({
            "id": id,
            "presentation": {
                "banner": {
                    "identifier": id, "feed_name": "Regina", "headline": "Severe Thunderstorm Warning",
                    "severity": severity, "message": "rendered", "background_color": "#931102",
                    "background_gradient": ["#931102", "#370c16"]
                },
                "banner_text": "Playlist banner text."
            }
        })
    }

    #[test]
    fn relays_the_rendered_card_while_on_air() {
        let mut board = BannerBoard::new(BTreeSet::from(["CWRS-REGINA".to_string()]));
        let now = Instant::now();
        let initial = board.take_changed(Utc::now());
        assert_eq!(initial.len(), 2, "every known feed and * start inactive");
        assert!(initial.iter().all(|payload| !payload.active));

        assert!(board.apply(
            &started("CWRS-REGINA", "q1", rendered_packet("a1", "Severe")),
            now
        ));
        let payload = board.payload("CWRS-REGINA", Utc::now());
        assert!(payload.active);
        assert_eq!(payload.feed_name, "Regina");
        assert_eq!(payload.primary_gradient, vec!["#931102", "#370c16"]);
        assert_eq!(payload.alerts[0]["message"], "Playlist banner text.");
        assert_eq!(payload.alerts[0]["feed_id"], "CWRS-REGINA");
        assert!(board.payload(ALL_FEEDS, Utc::now()).active);
        assert_eq!(board.take_changed(Utc::now()).len(), 2);
        assert!(
            board.take_changed(Utc::now()).is_empty(),
            "unchanged feeds are not republished"
        );

        let completed = json!({"type": "alert.playout.completed", "feed_ids": ["CWRS-REGINA"], "queue_id": "q1", "data": {}});
        assert!(board.apply(&completed, now));
        assert!(!board.payload("CWRS-REGINA", Utc::now()).active);
    }

    #[test]
    fn later_items_do_not_replace_the_airing_banner() {
        let mut board = BannerBoard::new(BTreeSet::new());
        let now = Instant::now();
        board.apply(&started("F", "q1", rendered_packet("a1", "Severe")), now);
        assert!(!board.apply(&started("F", "q2", rendered_packet("a2", "Extreme")), now));
        assert_eq!(board.payload("F", Utc::now()).alerts[0]["identifier"], "a1");
        assert!(board.prune(now + ON_AIR_TTL + Duration::from_secs(1)));
    }

    #[test]
    fn builds_a_minimal_card_for_alerts_without_one() {
        let mut board = BannerBoard::new(BTreeSet::new());
        let event = json!({
            "type": "alert.playout.started", "feed_ids": ["F"], "queue_id": "live-1",
            "header": "Operator break-in", "event": "OPR",
            "data": {"feed_id": "F", "queue_id": "live-1", "header": "Operator break-in", "event": "OPR"}
        });
        board.apply(&event, Instant::now());
        let alert = &board.payload("F", Utc::now()).alerts[0];
        assert_eq!(alert["headline"], "Operator break-in");
        assert_eq!(alert["event"], "OPR");
        assert_eq!(alert["background_gradient"], json!(ADVISORY_GRADIENT));
    }
}
