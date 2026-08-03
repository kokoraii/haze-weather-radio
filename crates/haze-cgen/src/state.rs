use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const PRIORITY_AUDIO_EXPIRE_GRACE_MS: i64 = 2_000;
const MAX_BANNER_ALERTS: usize = 8;
const MAX_BANNER_TEXT_CHARS: usize = 1_800;
const MAX_CANONICAL_LOCATIONS: usize = 64;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct RuntimeState {
    pub(crate) banners: BTreeMap<String, BannerPayload>,
    pub(crate) active_audio: BTreeMap<String, PriorityAudio>,
    pub(crate) controls: BTreeMap<String, CgenControlOverride>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct BannerPayload {
    #[serde(default)]
    pub(crate) active: bool,
    #[serde(default)]
    pub(crate) signature: String,
    #[serde(default)]
    pub(crate) feed_id: String,
    #[serde(default)]
    pub(crate) feed_name: String,
    #[serde(default)]
    pub(crate) primary_color: String,
    #[serde(default)]
    pub(crate) primary_gradient: Vec<String>,
    #[serde(default)]
    pub(crate) alerts: Vec<SerializedAlert>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SerializedAlert {
    #[serde(flatten)]
    pub(crate) fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct CgenControlOverride {
    #[serde(flatten)]
    pub(crate) fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PriorityAudio {
    pub(crate) queue_id: String,
    pub(crate) audio_path: Option<PathBuf>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) sample_rate: u32,
    pub(crate) channels: u16,
    pub(crate) banner_text: Option<String>,
    pub(crate) background_color: Option<String>,
    pub(crate) priority: Option<String>,
    #[serde(default)]
    pub(crate) presentation: AlertPresentation,
    pub(crate) started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct AlertPresentation {
    #[serde(default)]
    pub(crate) organization: String,
    #[serde(default)]
    pub(crate) action: String,
    #[serde(default)]
    pub(crate) event: String,
    #[serde(default)]
    pub(crate) full_text: String,
    #[serde(default)]
    pub(crate) alert_class: String,
}

impl RuntimeState {
    pub(crate) fn apply_event(&mut self, event: &Value) -> bool {
        let mut changed = self.prune_inactive_priority_audio(Utc::now());
        let event_type = text_at(event, &["type"]);
        changed |= match event_type.as_str() {
            "banner.state.updated" => self.apply_banner_state(event),
            "alert.playout.started" => self.apply_priority_audio(event),
            "cgen.control" => self.apply_cgen_control(event),
            "cap.alert.audio.ready" => false,
            "alert.playout.completed" | "playout.interrupted" => self.clear_priority_audio(event),
            _ => false,
        };
        changed
    }

    pub(crate) fn control_for(&self, feed_id: &str) -> Option<&CgenControlOverride> {
        self.controls
            .get(feed_id)
            .or_else(|| self.controls.get("*"))
    }

    pub(crate) fn banner_for(&self, feed_id: &str) -> Option<&BannerPayload> {
        if feed_id.trim() == "*" {
            return self.banners.values().find(|payload| payload.active);
        }
        // CGEN pipelines bind to one concrete alert feed. A legacy wildcard
        // broker event remains inspectable through the explicit wildcard
        // query, but must never present on every concrete CGEN pipeline.
        self.banners.get(feed_id).filter(|payload| payload.active)
    }

    pub(crate) fn priority_audio_for(&self, feed_id: &str) -> Option<&PriorityAudio> {
        let now = Utc::now();
        if feed_id.trim() == "*" {
            return self
                .active_audio
                .values()
                .filter(|audio| audio.is_active_at(now))
                .min_by_key(|audio| audio.started_at);
        }
        // The configured pipeline feed is always concrete after migration.
        // Do not fall back to a wildcard alert or later alert from another
        // feed, because that would violate playout ownership and queue-ID
        // correlation for this pipeline.
        self.active_audio
            .get(feed_id)
            .filter(|audio| audio.is_active_at(now))
    }

    fn apply_banner_state(&mut self, event: &Value) -> bool {
        let payload_value = event.get("data").cloned().unwrap_or(Value::Null);
        let Ok(mut payload) = serde_json::from_value::<BannerPayload>(payload_value) else {
            return false;
        };
        compact_banner_payload(&mut payload);
        let feed_id = fallback_text(&payload.feed_id, &text_at(event, &["subject"]), "*");
        let changed = self.banners.get(&feed_id) != Some(&payload);
        if changed {
            self.banners.insert(feed_id, payload);
        }
        changed
    }

    fn apply_priority_audio(&mut self, event: &Value) -> bool {
        let data = event.get("data").unwrap_or(event);
        let queue_id = fallback_text(
            &text_at(data, &["queue_id"]),
            &text_at(event, &["queue_id"]),
            "",
        );
        if queue_id.trim().is_empty() {
            return false;
        }
        let audio = PriorityAudio {
            queue_id,
            audio_path: non_empty(fallback_text(
                &text_at(data, &["audio_path"]),
                &text_at(event, &["audio_path"]),
                "",
            ))
            .map(PathBuf::from),
            duration_ms: number_at(data, &["duration_ms"])
                .or_else(|| number_at(event, &["duration_ms"])),
            sample_rate: number_at(data, &["sample_rate"])
                .or_else(|| number_at(event, &["sample_rate"]))
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(48_000),
            channels: number_at(data, &["channels"])
                .or_else(|| number_at(event, &["channels"]))
                .and_then(|value| u16::try_from(value).ok())
                .filter(|value| *value > 0)
                .unwrap_or(1),
            banner_text: non_empty(fallback_text(
                &text_at(data, &["banner_text"]),
                &text_at(event, &["banner_text"]),
                "",
            )),
            background_color: non_empty(fallback_text(
                &text_at(data, &["background_color"]),
                &text_at(event, &["background_color"]),
                "",
            )),
            priority: non_empty(fallback_text(
                &text_at(data, &["priority"]),
                &text_at(event, &["priority"]),
                "",
            )),
            presentation: alert_presentation(data),
            started_at: Utc::now(),
        };
        let now = Utc::now();
        let mut changed = false;
        for feed_id in feed_ids(event) {
            if self
                .active_audio
                .get(&feed_id)
                .is_some_and(|active| active.queue_id != audio.queue_id && active.is_active_at(now))
            {
                continue;
            }
            changed |= self.active_audio.get(&feed_id) != Some(&audio);
            self.active_audio.insert(feed_id, audio.clone());
        }
        changed
    }

    fn apply_cgen_control(&mut self, event: &Value) -> bool {
        let data = event.get("data").unwrap_or(event);
        let feed_id = fallback_text(
            &text_at(data, &["feed_id"]),
            &fallback_text(&text_at(data, &["id"]), &text_at(event, &["subject"]), ""),
            "",
        );
        if feed_id.trim().is_empty() {
            return false;
        }
        let mut next = CgenControlOverride {
            fields: data
                .as_object()
                .map(|object| {
                    object
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect()
                })
                .unwrap_or_default(),
        };
        normalize_cgen_control(&mut next);
        let changed = self.controls.get(&feed_id) != Some(&next);
        if changed {
            self.controls.insert(feed_id, next);
        }
        changed
    }

    fn clear_priority_audio(&mut self, event: &Value) -> bool {
        let feed_ids = feed_ids(event);
        let queue_id = fallback_text(
            &text_at(event.get("data").unwrap_or(event), &["queue_id"]),
            &text_at(event, &["queue_id"]),
            "",
        );
        if feed_ids.iter().any(|feed_id| feed_id.trim() == "*") {
            if !queue_id.is_empty() {
                let before_audio = self.active_audio.len();
                self.active_audio
                    .retain(|_, active| active.queue_id != queue_id);
                return self.active_audio.len() != before_audio;
            }
            let changed = !self.active_audio.is_empty();
            self.active_audio.clear();
            return changed;
        }

        let mut changed = false;
        for feed_id in feed_ids {
            let should_clear_audio = self
                .active_audio
                .get(&feed_id)
                .is_some_and(|active| queue_id.is_empty() || active.queue_id == queue_id);
            if should_clear_audio {
                changed |= self.active_audio.remove(&feed_id).is_some();
            }
        }
        let should_clear_wildcard = self
            .active_audio
            .get("*")
            .is_some_and(|active| queue_id.is_empty() || active.queue_id == queue_id);
        if should_clear_wildcard {
            changed |= self.active_audio.remove("*").is_some();
        }
        changed
    }

    fn prune_inactive_priority_audio(&mut self, now: DateTime<Utc>) -> bool {
        let before = self.active_audio.len();
        self.active_audio.retain(|_, audio| audio.is_active_at(now));
        self.active_audio.len() != before
    }
}

fn alert_presentation(data: &Value) -> AlertPresentation {
    let organization = first_text_at(
        data,
        &[
            &["presentation", "organization"],
            &["alert_packet", "presentation", "organization"],
            &["organization"],
            &["issuer"],
            &["sender_name"],
        ],
    );
    let action = first_text_at(
        data,
        &[
            &["presentation", "action"],
            &["presentation", "action_phrase"],
            &["alert_packet", "presentation", "action"],
            &["action"],
            &["action_phrase"],
        ],
    );
    let event = first_text_at(
        data,
        &[
            &["presentation", "event"],
            &["alert_packet", "presentation", "event"],
            &["event_name"],
            &["event"],
            &["headline"],
        ],
    );
    let full_text = first_text_at(
        data,
        &[
            &["presentation", "full_text"],
            &["alert_packet", "presentation", "full_text"],
            &["banner_text"],
            &["message"],
            &["description"],
        ],
    );
    let alert_class = first_text_at(
        data,
        &[
            &["presentation", "alert_class"],
            &["alert_packet", "presentation", "alert_class"],
            &["alert_class"],
            &["priority"],
        ],
    );
    AlertPresentation {
        organization,
        action,
        event,
        full_text,
        alert_class,
    }
}

fn first_text_at(value: &Value, paths: &[&[&str]]) -> String {
    paths
        .iter()
        .map(|path| text_at(value, path))
        .find(|text| !text.is_empty())
        .unwrap_or_default()
}

fn compact_banner_payload(payload: &mut BannerPayload) {
    payload.alerts.truncate(MAX_BANNER_ALERTS);
    for alert in &mut payload.alerts {
        let mut fields = BTreeMap::new();
        for key in [
            "banner_text",
            "message",
            "scroll_text",
            "headline",
            "title",
            "event_name",
            "event",
            "description",
            "identifier",
        ] {
            if let Some(text) = alert
                .fields
                .get(key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                fields.insert(
                    key.to_string(),
                    Value::String(limit_chars(text, MAX_BANNER_TEXT_CHARS)),
                );
            }
        }
        if let Some(locations) = alert
            .fields
            .get("canonical_locations")
            .and_then(compact_canonical_locations)
        {
            fields.insert("canonical_locations".to_string(), locations);
        }
        alert.fields = fields;
    }
}

fn compact_canonical_locations(value: &Value) -> Option<Value> {
    let locations = value.as_array()?;
    let mut compact = Vec::with_capacity(locations.len().min(MAX_CANONICAL_LOCATIONS));
    for location in locations.iter().take(MAX_CANONICAL_LOCATIONS) {
        let Some(source) = location.as_object() else {
            continue;
        };
        let mut target = serde_json::Map::new();
        for key in [
            "canonical_id",
            "candidate_id",
            "name",
            "kind",
            "country",
            "region",
            "match_confidence",
            "match_method",
            "catalog_generation",
        ] {
            if let Some(text) = source
                .get(key)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                target.insert(key.to_string(), Value::String(limit_chars(text, 256)));
            }
        }
        for key in ["match_score", "source_quality"] {
            if let Some(number) = source.get(key).and_then(Value::as_f64) {
                if number.is_finite() {
                    target.insert(key.to_string(), serde_json::json!(number));
                }
            }
        }
        for key in ["actionable", "ambiguous"] {
            if let Some(flag) = source.get(key).and_then(Value::as_bool) {
                target.insert(key.to_string(), Value::Bool(flag));
            }
        }
        if !target.is_empty() {
            compact.push(Value::Object(target));
        }
    }
    (!compact.is_empty()).then_some(Value::Array(compact))
}

fn limit_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

impl CgenControlOverride {
    pub(crate) fn string_field(&self, key: &str) -> Option<&str> {
        self.fields
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
    }

    pub(crate) fn bool_field(&self, key: &str) -> Option<bool> {
        let value = self.fields.get(key)?;
        if let Some(value) = value.as_bool() {
            return Some(value);
        }
        value.as_str().and_then(parse_bool)
    }

    pub(crate) fn u32_field(&self, key: &str) -> Option<u32> {
        let value = self.fields.get(key)?;
        if let Some(value) = value.as_u64().and_then(|value| u32::try_from(value).ok()) {
            return Some(value);
        }
        value
            .as_str()
            .and_then(|text| text.trim().parse::<u32>().ok())
    }

    pub(crate) fn i32_field(&self, key: &str) -> Option<i32> {
        let value = self.fields.get(key)?;
        if let Some(value) = value.as_i64().and_then(|value| i32::try_from(value).ok()) {
            return Some(value);
        }
        value
            .as_str()
            .and_then(|text| text.trim().parse::<i32>().ok())
    }
}

fn normalize_cgen_control(control: &mut CgenControlOverride) {
    let action = control
        .string_field("action")
        .unwrap_or_default()
        .to_ascii_lowercase();
    match action.as_str() {
        "release" => {
            control
                .fields
                .insert("mode".into(), Value::String("release".into()));
            control
                .fields
                .insert("smpte_bars".into(), Value::Bool(false));
            control
                .fields
                .insert("sunny_cat".into(), Value::Bool(false));
            control
                .fields
                .insert("text_enabled".into(), Value::Bool(false));
            control
                .fields
                .insert("clock_enabled".into(), Value::Bool(false));
        }
        "overlay" => {
            control
                .fields
                .insert("mode".into(), Value::String("overlay".into()));
        }
        "smpte_bars" => {
            control
                .fields
                .insert("mode".into(), Value::String("overlay".into()));
        }
        "clock" => {
            control
                .fields
                .insert("mode".into(), Value::String("overlay".into()));
        }
        "insert_text" => {
            control
                .fields
                .insert("mode".into(), Value::String("overlay".into()));
            control
                .fields
                .insert("text_enabled".into(), Value::Bool(true));
        }
        "clear_text" => {
            control
                .fields
                .insert("text_enabled".into(), Value::Bool(false));
            control
                .fields
                .insert("text".into(), Value::String(String::new()));
        }
        "unleash_sunny" => {
            control
                .fields
                .insert("mode".into(), Value::String("overlay".into()));
            control.fields.insert("sunny_cat".into(), Value::Bool(true));
        }
        "banish_sunny" => {
            control
                .fields
                .insert("sunny_cat".into(), Value::Bool(false));
        }
        _ => {}
    }
}

impl PriorityAudio {
    fn is_active_at(&self, now: DateTime<Utc>) -> bool {
        let Some(duration_ms) = self.duration_ms else {
            return true;
        };
        let duration_ms = i64::try_from(duration_ms).unwrap_or(i64::MAX);
        let max_age = chrono::Duration::milliseconds(
            duration_ms.saturating_add(PRIORITY_AUDIO_EXPIRE_GRACE_MS),
        );
        now.signed_duration_since(self.started_at) <= max_age
    }
}

fn feed_ids(event: &Value) -> Vec<String> {
    let mut out = BTreeSet::<String>::new();
    add_feed_id(&mut out, text_at(event, &["feed_id"]));
    add_feed_id(
        &mut out,
        text_at(event.get("data").unwrap_or(event), &["feed_id"]),
    );
    for path in [&["feed_ids"][..], &["data", "feed_ids"][..]] {
        if let Some(values) = value_at(event, path).and_then(Value::as_array) {
            for value in values {
                add_feed_id(&mut out, value.as_str().unwrap_or_default().to_string());
            }
        }
    }
    if out.is_empty() {
        out.insert("*".to_string());
    }
    out.into_iter().collect()
}

fn add_feed_id(out: &mut BTreeSet<String>, value: String) {
    let value = value.trim();
    if !value.is_empty() {
        out.insert(value.to_string());
    }
}

fn text_at(value: &Value, path: &[&str]) -> String {
    value_at(value, path)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn number_at(value: &Value, path: &[&str]) -> Option<u64> {
    value_at(value, path).and_then(Value::as_u64)
}

fn value_at<'a>(mut value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    for part in path {
        value = value.get(*part)?;
    }
    Some(value)
}

fn fallback_text(values: &str, fallback: &str, default: &str) -> String {
    for value in [values, fallback, default] {
        let value = value.trim();
        if !value.is_empty() {
            return value.to_string();
        }
    }
    String::new()
}

fn non_empty(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn parse_bool(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" | "enabled" => Some(true),
        "false" | "0" | "no" | "off" | "disabled" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn consumes_canonical_banner_state() {
        let mut state = RuntimeState::default();
        let changed = state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "sk-0001",
            "data": {
                "active": true,
                "signature": "abc",
                "feed_id": "sk-0001",
                "primary_color": "#b91c1c",
                "primary_gradient": ["#b91c1c", "#7f1d1d"],
                "alerts": [{"message": "Alert text"}]
            }
        }));
        assert!(changed);
        let banner = state.banner_for("sk-0001").expect("banner");
        assert_eq!(banner.signature, "abc");
        assert_eq!(banner.alerts.len(), 1);
    }

    #[test]
    fn banner_state_keeps_only_render_fields() {
        let mut state = RuntimeState::default();
        let long_text = "x".repeat(MAX_BANNER_TEXT_CHARS + 50);
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "sk-0001",
            "data": {
                "active": true,
                "feed_id": "sk-0001",
                "alerts": [
                    {
                        "message": long_text,
                        "alert_packet": {"big": "payload"},
                        "ignored": "not rendered"
                    }
                ]
            }
        })));

        let banner = state.banner_for("sk-0001").expect("banner");
        let fields = &banner.alerts[0].fields;
        assert!(fields.contains_key("message"));
        assert!(!fields.contains_key("alert_packet"));
        assert!(!fields.contains_key("ignored"));
        assert_eq!(
            fields["message"].as_str().expect("message").chars().count(),
            MAX_BANNER_TEXT_CHARS
        );
    }

    #[test]
    fn priority_audio_is_per_feed_and_cleared_by_completion() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["sk-0001"],
            "queue_id": "alert-1",
            "data": {
                "queue_id": "alert-1",
                "audio_path": "runtime/audio/alerts/alert.raw",
                "duration_ms": 42000,
                "banner_text": "banner"
            }
        })));
        assert!(state.priority_audio_for("sk-0001").is_some());
        assert!(state.apply_event(&json!({
            "type": "alert.playout.completed",
            "feed_ids": ["sk-0001"],
            "queue_id": "alert-1",
            "data": {"queue_id": "alert-1"}
        })));
        assert!(state.priority_audio_for("sk-0001").is_none());
    }

    #[test]
    fn inactive_priority_audio_is_pruned_on_next_event() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["sk-0001"],
            "queue_id": "alert-1",
            "data": {
                "queue_id": "alert-1",
                "duration_ms": 1,
                "alert_packet": {"large": "payload"}
            }
        })));
        state
            .active_audio
            .get_mut("sk-0001")
            .expect("active audio")
            .started_at =
            Utc::now() - chrono::Duration::milliseconds(PRIORITY_AUDIO_EXPIRE_GRACE_MS + 100);

        assert!(state.apply_event(&json!({"type": "cap.alert.audio.ready"})));

        assert!(state.active_audio.is_empty());
    }

    #[test]
    fn cap_audio_ready_does_not_activate_priority_audio() {
        let mut state = RuntimeState::default();
        assert!(!state.apply_event(&json!({
            "type": "cap.alert.audio.ready",
            "data": {
                "feed_ids": ["*"],
                "queue_id": "alert-all",
                "audio_path": "runtime/audio/alerts/all.raw"
            }
        })));
        assert!(state.priority_audio_for("CAP-IT-ALL").is_none());
    }

    #[test]
    fn playout_start_without_queue_id_is_not_activated() {
        let mut state = RuntimeState::default();
        assert!(!state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "data": {"banner_text": "uncorrelated"}
        })));
        assert!(state.priority_audio_for("CAP-IT-ALL").is_none());
    }

    #[test]
    fn wildcard_priority_audio_does_not_cross_concrete_feed_boundaries() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "data": {
                "feed_ids": ["*"],
                "queue_id": "alert-all",
                "audio_path": "runtime/audio/alerts/all.raw"
            }
        })));
        assert!(state.priority_audio_for("CAP-IT-ALL").is_none());
        assert_eq!(
            state
                .priority_audio_for("*")
                .map(|audio| audio.queue_id.as_str()),
            Some("alert-all")
        );
    }

    #[test]
    fn wildcard_priority_query_uses_any_active_feed() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["sk-0001"],
            "queue_id": "alert-1",
            "data": {
                "queue_id": "alert-1",
                "audio_path": "runtime/audio/alerts/sk.raw"
            }
        })));

        let audio = state.priority_audio_for("*").expect("wildcard audio");
        assert_eq!(audio.queue_id, "alert-1");
    }

    #[test]
    fn active_priority_audio_is_not_replaced_before_completion() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "tor-1",
            "data": {
                "queue_id": "tor-1",
                "audio_path": "runtime/audio/alerts/tor.raw",
                "duration_ms": 60000,
                "banner_text": "Tornado warning"
            }
        })));
        assert!(!state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "svr-1",
            "data": {
                "queue_id": "svr-1",
                "audio_path": "runtime/audio/alerts/svr.raw",
                "duration_ms": 60000,
                "banner_text": "Severe thunderstorm warning"
            }
        })));

        let audio = state
            .priority_audio_for("CAP-IT-ALL")
            .expect("active audio");
        assert_eq!(audio.queue_id, "tor-1");
        assert_eq!(audio.banner_text.as_deref(), Some("Tornado warning"));

        assert!(state.apply_event(&json!({
            "type": "alert.playout.completed",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "tor-1",
            "data": {"queue_id": "tor-1"}
        })));
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "svr-1",
            "data": {
                "queue_id": "svr-1",
                "audio_path": "runtime/audio/alerts/svr.raw",
                "duration_ms": 60000
            }
        })));
        assert_eq!(
            state
                .priority_audio_for("CAP-IT-ALL")
                .map(|audio| audio.queue_id.as_str()),
            Some("svr-1")
        );
    }

    #[test]
    fn wildcard_priority_audio_keeps_oldest_active_alert() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["sk-0001"],
            "queue_id": "alert-1",
            "data": {"queue_id": "alert-1", "audio_path": "runtime/audio/alerts/one.raw", "duration_ms": 60000}
        })));
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "alert-2",
            "data": {"queue_id": "alert-2", "audio_path": "runtime/audio/alerts/two.raw", "duration_ms": 60000}
        })));

        let audio = state.priority_audio_for("*").expect("wildcard audio");
        assert_eq!(audio.queue_id, "alert-1");
    }

    #[test]
    fn wildcard_banner_query_uses_any_active_feed() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "signature": "wildcard-visual",
                "alerts": [{"message": "Alert text"}]
            }
        })));

        let banner = state.banner_for("*").expect("wildcard banner");
        assert_eq!(banner.signature, "wildcard-visual");
        assert!(state.banner_for("CAP-IT-ALL").is_some());
    }

    #[test]
    fn top_level_playout_started_fields_are_accepted() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "daemon-alert",
            "audio_path": "runtime/queues/alerts/daemon.raw",
            "sample_rate": 48000,
            "channels": 2,
            "duration_ms": 12000,
            "banner_text": "daemon banner"
        })));

        let audio = state
            .priority_audio_for("CAP-IT-ALL")
            .expect("daemon audio");
        assert_eq!(audio.queue_id, "daemon-alert");
        assert_eq!(
            audio.audio_path.as_deref(),
            Some(std::path::Path::new("runtime/queues/alerts/daemon.raw"))
        );
        assert_eq!(audio.duration_ms, Some(12000));
        assert_eq!(audio.banner_text.as_deref(), Some("daemon banner"));
    }

    #[test]
    fn completion_does_not_clear_visual_banner_for_feed() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "alerts": [{"message": "Alert text"}]
            }
        })));
        assert!(state.banner_for("CAP-IT-ALL").is_some());
        assert!(!state.apply_event(&json!({
            "type": "alert.playout.completed",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "alert-1",
            "data": {"queue_id": "alert-1"}
        })));
        assert!(state.banner_for("CAP-IT-ALL").is_some());
    }

    #[test]
    fn inactive_banner_state_clears_visual_banner_for_feed() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": true,
                "feed_id": "CAP-IT-ALL",
                "signature": "alert-on",
                "alerts": [{"message": "Alert text"}]
            }
        })));
        assert!(state.banner_for("CAP-IT-ALL").is_some());
        assert!(state.apply_event(&json!({
            "type": "banner.state.updated",
            "subject": "CAP-IT-ALL",
            "data": {
                "active": false,
                "feed_id": "CAP-IT-ALL",
                "signature": "alert-off",
                "alerts": []
            }
        })));
        assert!(state.banner_for("CAP-IT-ALL").is_none());
    }

    #[test]
    fn completion_clears_priority_audio_when_queue_id_matches() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "audio-ready-id",
            "data": {
                "queue_id": "audio-ready-id",
                "audio_path": "runtime/audio/alerts/alert.raw",
                "duration_ms": 60000
            }
        })));
        assert!(state.priority_audio_for("CAP-IT-ALL").is_some());
        assert!(state.apply_event(&json!({
            "type": "alert.playout.completed",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "audio-ready-id",
            "data": {"queue_id": "audio-ready-id"}
        })));
        assert!(state.priority_audio_for("CAP-IT-ALL").is_none());
    }

    #[test]
    fn cgen_control_updates_live_override_state() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "cgen.control",
            "subject": "CAP-IT-ALL",
            "data": {
                "feed_id": "CAP-IT-ALL",
                "action": "clock",
                "mode": "release",
                "clock_enabled": true,
                "clock_format": "Jan 02 15:04:05",
                "clock_x": "96",
                "smpte_bars": "false"
            }
        })));
        let control = state.control_for("CAP-IT-ALL").expect("control override");
        assert_eq!(control.string_field("mode"), Some("overlay"));
        assert_eq!(control.bool_field("clock_enabled"), Some(true));
        assert_eq!(
            control.string_field("clock_format"),
            Some("Jan 02 15:04:05")
        );
        assert_eq!(control.i32_field("clock_x"), Some(96));
        assert_eq!(control.bool_field("smpte_bars"), Some(false));

        assert!(state.apply_event(&json!({
            "type": "cgen.control",
            "subject": "CAP-IT-ALL",
            "data": {
                "feed_id": "CAP-IT-ALL",
                "action": "release",
                "clock_enabled": true,
                "text_enabled": true,
                "sunny_cat": true
            }
        })));
        let control = state.control_for("CAP-IT-ALL").expect("control override");
        assert_eq!(control.string_field("mode"), Some("release"));
        assert_eq!(control.bool_field("clock_enabled"), Some(false));
        assert_eq!(control.bool_field("text_enabled"), Some(false));
        assert_eq!(control.bool_field("sunny_cat"), Some(false));
    }

    #[test]
    fn mismatched_completion_does_not_clear_active_priority_audio() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "active-alert",
            "data": {
                "queue_id": "active-alert",
                "audio_path": "runtime/audio/alerts/active.raw",
                "duration_ms": 60000
            }
        })));
        assert!(!state.apply_event(&json!({
            "type": "alert.playout.completed",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "other-alert",
            "data": {"queue_id": "other-alert"}
        })));
        assert_eq!(
            state
                .priority_audio_for("CAP-IT-ALL")
                .map(|audio| audio.queue_id.as_str()),
            Some("active-alert")
        );
    }

    #[test]
    fn mismatched_specific_completion_does_not_clear_wildcard_priority_audio() {
        let mut state = RuntimeState::default();
        assert!(state.apply_event(&json!({
            "type": "alert.playout.started",
            "feed_ids": ["*"],
            "queue_id": "wildcard-start",
            "data": {
                "queue_id": "wildcard-start",
                "audio_path": "runtime/audio/alerts/wildcard.raw",
                "duration_ms": 60000
            }
        })));
        assert!(state.priority_audio_for("*").is_some());
        assert!(state.priority_audio_for("CAP-IT-ALL").is_none());
        assert!(!state.apply_event(&json!({
            "type": "alert.playout.completed",
            "feed_ids": ["CAP-IT-ALL"],
            "queue_id": "specific-complete",
            "data": {"queue_id": "specific-complete"}
        })));
        assert!(state.priority_audio_for("*").is_some());
        assert!(state.apply_event(&json!({
            "type": "alert.playout.completed",
            "feed_ids": ["*"],
            "queue_id": "wildcard-start",
            "data": {"queue_id": "wildcard-start"}
        })));
        assert!(state.priority_audio_for("*").is_none());
    }
}
