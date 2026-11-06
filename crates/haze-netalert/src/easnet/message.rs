//! EAS NET message format (`#BEGIN` / `KEY=value` / `#END`), peer filters,
//! and the inbound broadcast payload.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

use anyhow::{bail, Result};
use serde_json::{json, Value};

use super::config::PeerConfig;
use crate::util::{make_token, unix_now_ms, value_list, value_text};

#[derive(Debug, Clone)]
pub struct EasNetMessage {
    pub fields: BTreeMap<String, String>,
    pub audio_upload: Option<AudioUpload>,
}

/// Alert audio copied to an SSH peer before the message itself.
#[derive(Debug, Clone)]
pub struct AudioUpload {
    pub local_path: PathBuf,
    pub remote_name: String,
}

impl EasNetMessage {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.fields
            .get(&key.to_ascii_uppercase())
            .map(String::as_str)
    }

    pub fn first_nonblank(&self, keys: &[&str]) -> Option<&str> {
        keys.iter()
            .filter_map(|key| self.get(key))
            .find(|value| !value.trim().is_empty())
    }

    pub fn id(&self) -> String {
        self.first_nonblank(&["EAS.ID", "EAS.HEADER"])
            .map(str::to_string)
            .unwrap_or_else(|| format!("easnet-{}", unix_now_ms()))
    }

    pub fn locations(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for (key, value) in &self.fields {
            if key == "EAS.FIPS" || key.starts_with("EAS.FIPS_") {
                for part in value.split([',', ';', ' ']) {
                    let part = part.trim();
                    if part.len() == 6
                        && part.chars().all(|ch| ch.is_ascii_digit())
                        && seen.insert(part.to_string())
                    {
                        out.push(part.to_string());
                    }
                }
            }
        }
        if out.is_empty() {
            if let Some(header) = self.get("EAS.HEADER") {
                out.extend(header_locations(header));
            }
        }
        out
    }

    pub fn dedupe_key(&self, peer_id: &str) -> String {
        make_token(&format!(
            "{}:{}:{}:{}:{}",
            peer_id,
            self.id(),
            self.get("EAS.HEADER").unwrap_or(""),
            self.get("EAS.START_TIME").unwrap_or(""),
            self.get("EAS.END_TIME").unwrap_or("")
        ))
    }

    pub fn body_hash(&self) -> String {
        make_token(&serialize(self))
    }
}

pub fn parse(raw: &str) -> Result<EasNetMessage> {
    let mut in_body = false;
    let mut fields = BTreeMap::new();
    for raw_line in raw.lines() {
        let line = raw_line.trim();
        if line.eq_ignore_ascii_case("#BEGIN") {
            in_body = true;
            continue;
        }
        if line.eq_ignore_ascii_case("#END") {
            break;
        }
        if !in_body || line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim().to_ascii_uppercase();
        if key.is_empty() || key.len() > 128 {
            continue;
        }
        fields.insert(key, trim_value(value));
    }
    if fields.is_empty() {
        bail!("EAS NET message contained no fields");
    }
    Ok(EasNetMessage {
        fields,
        audio_upload: None,
    })
}

pub fn serialize(message: &EasNetMessage) -> String {
    let mut out = String::from("#BEGIN\n");
    for (key, value) in &message.fields {
        out.push_str(key);
        out.push('=');
        out.push_str(&value.replace(['\r', '\n'], " "));
        out.push('\n');
    }
    out.push_str("#END\n");
    out
}

pub fn contains_end_marker(data: &[u8]) -> bool {
    data.windows(4)
        .any(|window| window.eq_ignore_ascii_case(b"#END"))
}

/// DASDEC remote-control functions are never acted on.
pub fn is_remote_control(message: &EasNetMessage) -> bool {
    let func = message
        .get("EAS.NET.FUNC")
        .unwrap_or("")
        .to_ascii_uppercase();
    let alert_type = message
        .get("EAS.ALERT_EVENT.TYPE")
        .unwrap_or("")
        .to_ascii_uppercase();
    let blocked = [
        "RUN_PRESET_RWT",
        "MANUAL_FORWARD",
        "MSG_ABORT",
        "MSG_LIVE_START",
        "MSG_LIVE_END",
        "EAS_LIVE_START",
        "EAS_LIVE_END",
        "STOP LIVE",
    ];
    blocked
        .iter()
        .any(|value| func == *value || alert_type == *value)
}

pub fn message_matches(peer: &PeerConfig, message: &EasNetMessage) -> bool {
    set_matches(
        &peer.events,
        message
            .first_nonblank(&["EAS.TYPE", "EAS.ALERT_EVENT.TYPE"])
            .unwrap_or(""),
    ) && set_matches(&peer.severities, message.get("EAS.SEVERITY").unwrap_or(""))
        && locations_match(&peer.locations, &message.locations())
}

pub fn event_matches_peer(peer: &PeerConfig, data: &Value) -> bool {
    let Some(object) = data.as_object() else {
        return false;
    };
    set_matches(
        &peer.events,
        value_text(object.get("same_event"))
            .or_else(|| value_text(object.get("event")))
            .unwrap_or_default()
            .as_str(),
    ) && locations_match(
        &peer.locations,
        &value_list(
            object
                .get("same_locations")
                .or_else(|| object.get("locations")),
        ),
    ) && feed_matches(
        &peer.feeds,
        &value_list(object.get("feed_ids"))
            .into_iter()
            .chain(value_text(object.get("feed_id")))
            .collect::<Vec<_>>(),
    )
}

pub fn inbound_broadcast_payload(peer: &PeerConfig, message: &EasNetMessage) -> Value {
    let id = message.id();
    let event = message
        .first_nonblank(&["EAS.TYPE", "EAS.ALERT_EVENT.TYPE"])
        .unwrap_or("EAS");
    let headline = message
        .first_nonblank(&["EAS.HEADLINE", "EAS.TEXT"])
        .unwrap_or(event);
    let description = message
        .first_nonblank(&["EAS.DESCRIPTION", "EAS.TEXT"])
        .unwrap_or("");
    let instruction = message.get("EAS.INSTRUCTION").unwrap_or("");
    let locations = message.locations();
    let alert_text = [description, instruction]
        .into_iter()
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let banner_text = message
        .first_nonblank(&["EAS.TEXT", "EAS.DESCRIPTION", "EAS.HEADLINE"])
        .unwrap_or(headline);
    let audio_url = message
        .first_nonblank(&["EAS.AUDIO.STREAM.URL", "EAS.AUDIO.URL"])
        .unwrap_or("");
    let feed_ids = if peer.feeds.is_empty() {
        vec!["*".to_string()]
    } else {
        peer.feeds.clone()
    };
    json!({
        "feed_id": feed_ids.first().cloned().unwrap_or_else(|| "*".to_string()),
        "feed_ids": feed_ids,
        "alert_id": id,
        "id": id,
        "package_id": "alerts",
        "title": headline,
        "headline": headline,
        "event": event,
        "message_type": "Alert",
        "sender": peer.id,
        "severity": message.get("EAS.SEVERITY").unwrap_or("Unknown"),
        "urgency": message.get("EAS.URGENCY").unwrap_or("Unknown"),
        "certainty": message.get("EAS.CERTAINTY").unwrap_or("Unknown"),
        "description": description,
        "instruction": instruction,
        "alert_text": alert_text,
        "banner_text": banner_text,
        "include_same": !locations.is_empty(),
        "same_event": event,
        "same_originator": message.get("EAS.ORG").unwrap_or("EAS"),
        "same_locations": locations,
        "same_duration": message.get("EAS.DURATION").unwrap_or("0015"),
        "same_callsign": message.get("EAS.STATION_ID").unwrap_or("EASNET"),
        "same_header": message.get("EAS.HEADER").unwrap_or(""),
        "same_translation": message.get("EAS.TRANSLATION").unwrap_or(""),
        "alert_sent_at": message.get("EAS.START_TIME").unwrap_or(""),
        "alert_expires_at": message.get("EAS.END_TIME").unwrap_or(""),
        "audio_url": audio_url,
        "audio_format": "external",
        "source": "easnet",
        "alert_packet": {
            "version": 1,
            "id": id,
            "source": "easnet",
            "feed_ids": feed_ids,
            "message_type": "Alert",
            "content": {
                "title": headline,
                "headline": headline,
                "event": event,
                "event_name": event,
                "severity": message.get("EAS.SEVERITY").unwrap_or("Unknown"),
                "urgency": message.get("EAS.URGENCY").unwrap_or("Unknown"),
                "certainty": message.get("EAS.CERTAINTY").unwrap_or("Unknown"),
                "description": description,
                "instruction": instruction
            },
            "timing": {
                "sent_at": message.get("EAS.START_TIME").unwrap_or(""),
                "expires_at": message.get("EAS.END_TIME").unwrap_or("")
            },
            "areas": {
                "codes": locations
            },
            "same": {
                "include": !locations.is_empty(),
                "event": event,
                "originator": message.get("EAS.ORG").unwrap_or("EAS"),
                "locations": locations,
                "duration": message.get("EAS.DURATION").unwrap_or("0015"),
                "callsign": message.get("EAS.STATION_ID").unwrap_or("EASNET"),
                "header": message.get("EAS.HEADER").unwrap_or(""),
                "translation": message.get("EAS.TRANSLATION").unwrap_or("")
            },
            "audio": {
                "url": audio_url,
                "source": "easnet"
            },
            "presentation": {
                "speech_text": alert_text,
                "banner_text": banner_text
            },
            "meta": {
                "easnet_peer": peer.id,
                "easnet_fields": message.fields
            }
        }
    })
}

fn header_locations(header: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for part in header.split('-') {
        let part = part
            .split_once('+')
            .map(|(location, _)| location)
            .unwrap_or(part)
            .trim();
        if part.len() == 6
            && part.chars().all(|ch| ch.is_ascii_digit())
            && seen.insert(part.to_string())
        {
            out.push(part.to_string());
        }
    }
    out
}

fn feed_matches(filters: &[String], feeds: &[String]) -> bool {
    if filters.is_empty() || filters.iter().any(|value| value == "*") {
        return true;
    }
    feeds.iter().any(|feed| {
        filters
            .iter()
            .any(|filter| filter.eq_ignore_ascii_case(feed))
    })
}

fn locations_match(filters: &[String], locations: &[String]) -> bool {
    if filters.is_empty() || filters.iter().any(|value| value == "*") {
        return true;
    }
    locations
        .iter()
        .any(|location| filters.iter().any(|filter| filter == location))
}

fn set_matches(filters: &[String], value: &str) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| filter == "*")
        || filters
            .iter()
            .any(|filter| filter.eq_ignore_ascii_case(value))
}

fn trim_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].trim().to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::easnet::config::{Direction, PeerProtocol, ReceivePolicy};

    #[test]
    fn parses_and_serializes_easnet_message() {
        let raw =
            "#BEGIN\nEAS.NET.FUNC=EAS\nEAS.ID=test\nEAS.FIPS_1=065100\nEAS.TEXT=Hello\n#END\n";
        let message = parse(raw).unwrap();
        assert_eq!(message.get("EAS.ID"), Some("test"));
        assert_eq!(message.locations(), vec!["065100"]);
        let encoded = serialize(&message);
        assert!(encoded.starts_with("#BEGIN\n"));
        assert!(encoded.contains("EAS.TEXT=Hello\n"));
        assert!(encoded.ends_with("#END\n"));
    }

    #[test]
    fn extracts_locations_from_same_header() {
        let message = parse(
            "#BEGIN\nEAS.ID=x\nEAS.HEADER=ZCZC-WXR-SVR-065100-065200+0015-1841200-CAPALL  -\n#END\n",
        )
        .unwrap();
        assert_eq!(message.locations(), vec!["065100", "065200"]);
    }

    #[test]
    fn blocks_remote_control_messages() {
        let message = parse("#BEGIN\nEAS.NET.FUNC=RUN_PRESET_RWT\nEAS.ID=x\n#END\n").unwrap();
        assert!(is_remote_control(&message));
    }

    #[test]
    fn inbound_payload_carries_audio_url_and_same_fields() {
        let peer = PeerConfig {
            id: "dasdec".to_string(),
            enabled: true,
            host: "172.16.1.42".to_string(),
            port: 4098,
            protocol: PeerProtocol::Tcp,
            user: "root".to_string(),
            user_env: String::new(),
            password_env: String::new(),
            remote_command: "cat > /tmp/EAS_NET_IN".to_string(),
            direction: Direction::SendReceive,
            receive_policy: ReceivePolicy::RelayIfMatched,
            allowed_ips: vec![],
            feeds: vec!["CAP-IT-ALL".to_string()],
            locations: vec![],
            severities: vec![],
            events: vec![],
        };
        let message = parse(
            "#BEGIN\nEAS.ID=eas1\nEAS.TYPE=SVR\nEAS.ORG=WXR\nEAS.FIPS_1=065100\nEAS.AUDIO.STREAM.URL=http://device/audio.wav\nEAS.DESCRIPTION=Storm text.\n#END\n",
        )
        .unwrap();
        let payload = inbound_broadcast_payload(&peer, &message);
        assert_eq!(payload["feed_ids"][0], "CAP-IT-ALL");
        assert_eq!(payload["same_event"], "SVR");
        assert_eq!(payload["same_locations"][0], "065100");
        assert_eq!(payload["audio_url"], "http://device/audio.wav");
        assert_eq!(
            payload["alert_packet"]["audio"]["url"],
            "http://device/audio.wav"
        );
    }
}
