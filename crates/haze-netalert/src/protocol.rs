//! WebSocket wire format (see `docs/netalert.md`). Text frames are JSON
//! objects tagged by `type`. Binary frames carry Ogg/Opus pages for a stream
//! announced by `audio.start`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::interest::{Interest, InterestRequest};

pub const PROTOCOL_VERSION: u32 = 1;
pub const WS_PATH: &str = "/v1/netalert";

/// The only client message. Sending it again replaces the subscription.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SubscribeRequest {
    pub interest: Option<InterestRequest>,
    /// Alert payload formats. Defaults to both.
    pub formats: Option<Vec<Format>>,
    pub audio: Option<AudioRequest>,
    /// On-air banner state for one feed, or `*` for every feed.
    pub banner: Option<BannerRequest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    /// Normalized CAP as JSON (and the Haze alert packet on feed deliveries).
    Json,
    /// The original CAP XML document.
    Cap,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AudioRequest {
    pub enabled: bool,
    /// Preferred CAP info language for generated audio, such as `fr` or `en-CA`.
    pub language: Option<String>,
    /// IANA timezone for spoken times in generated audio.
    pub timezone: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BannerRequest {
    pub feed: String,
}

/// Parses one client text frame.
pub fn parse_client_message(raw: &str) -> Result<SubscribeRequest, String> {
    let mut value: Value =
        serde_json::from_str(raw).map_err(|err| format!("invalid JSON: {err}"))?;
    let Some(object) = value.as_object_mut() else {
        return Err("message must be a JSON object".to_string());
    };
    match object.remove("type").as_ref().and_then(Value::as_str) {
        Some("subscribe") => {
            serde_json::from_value(value).map_err(|err| format!("invalid subscribe: {err}"))
        }
        Some(other) => Err(format!("unknown message type {other:?}")),
        None => Err("message has no type".to_string()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Delivery {
    /// Matched by CAP interest as the alert arrived.
    Cap,
    /// Aired on a subscribed feed.
    Feed,
}

#[derive(Debug, Serialize)]
pub struct AudioCaps {
    pub codec: &'static str,
    pub container: &'static str,
    pub sample_rate: u32,
}

#[derive(Debug, Serialize)]
pub struct AudioPending {
    pub stream: u32,
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
pub struct AlertMessage<'a> {
    pub delivery: Delivery,
    pub alert_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed_id: Option<&'a str>,
    pub status: &'a str,
    pub msg_type: &'a str,
    pub sent: &'a str,
    /// An update or cancellation of an alert this client already received,
    /// delivered even though it no longer matches the area or severity filters.
    pub follow_up: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cap: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cap_xml: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub packet: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<AudioPending>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum ServerMessage<'a> {
    #[serde(rename = "hello")]
    Hello {
        protocol: u32,
        server: &'static str,
        audio: AudioCaps,
    },
    #[serde(rename = "subscribed")]
    Subscribed {
        #[serde(skip_serializing_if = "Option::is_none")]
        interest: Option<&'a Interest>,
        formats: &'a [Format],
        audio: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        banner: Option<&'a str>,
    },
    #[serde(rename = "alert")]
    Alert(AlertMessage<'a>),
    #[serde(rename = "alert.cancelled")]
    AlertCancelled {
        feed_id: &'a str,
        alert_ids: &'a [String],
    },
    #[serde(rename = "audio.start")]
    AudioStart {
        stream: u32,
        alert_id: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        feed_id: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        language: Option<&'a str>,
        codec: &'static str,
        container: &'static str,
        sample_rate: u32,
        channels: u16,
        duration_ms: u64,
    },
    #[serde(rename = "audio.end")]
    AudioEnd { stream: u32 },
    #[serde(rename = "audio.failed")]
    AudioFailed {
        stream: u32,
        alert_id: &'a str,
        error: &'a str,
    },
    #[serde(rename = "banner")]
    Banner(&'a Value),
    #[serde(rename = "error")]
    Error {
        code: &'static str,
        message: &'a str,
    },
}

impl ServerMessage<'_> {
    pub fn to_text(&self) -> String {
        serde_json::to_string(self).expect("server messages always serialize")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_subscribe_and_rejects_unknown_shapes() {
        let request = parse_client_message(
            r#"{"type":"subscribe","interest":{"severity":["Severe"]},"formats":["cap"],"audio":{"enabled":true,"language":"fr"},"banner":{"feed":"*"}}"#,
        )
        .unwrap();
        assert_eq!(request.formats, Some(vec![Format::Cap]));
        assert!(request.audio.unwrap().enabled);
        assert_eq!(request.banner.unwrap().feed, "*");

        assert!(parse_client_message(r#"{"type":"unsubscribe"}"#).is_err());
        assert!(parse_client_message(r#"{"type":"subscribe","colour":1}"#).is_err());
        assert!(parse_client_message("[]").is_err());
    }

    #[test]
    fn server_messages_are_tagged_by_type() {
        let banner = json!({"feed_id": "*", "active": false});
        let text = ServerMessage::Banner(&banner).to_text();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            json!({"type": "banner", "feed_id": "*", "active": false})
        );
        let text = ServerMessage::AudioEnd { stream: 3 }.to_text();
        assert_eq!(text, r#"{"type":"audio.end","stream":3}"#);
    }
}
