//! Normalized alert facts that interest lists are matched against. They come
//! from a CAP alert, from a Haze alert packet (alerts with no CAP source), or
//! from both when a feed airs a CAP alert.

use haze_cap::model::{Alert, AlertInfo};
use serde_json::Value;

use crate::geo::Shape;
use crate::util::{value_list, value_text};

const SGC_PREFIX: &str = "profile:CAP-CP:Location:";
const CLC_NAME: &str = "layer:EC-MSC-SMC:1.0:CLC";

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AlertFacts {
    /// CAP status. Empty when the source (a manual or EAS NET alert) has none.
    pub status: String,
    pub msg_type: String,
    /// Identifiers this message updates or cancels.
    pub references: Vec<String>,
    pub infos: Vec<InfoFacts>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct InfoFacts {
    pub language: String,
    pub categories: Vec<String>,
    /// Lowercased event name and event codes (CAP-CP, SAME, Haze's resolved SAME event).
    pub events: Vec<String>,
    pub urgency: String,
    pub severity: String,
    pub certainty: String,
    pub codes: AreaCodes,
    pub shapes: Vec<Shape>,
}

/// Location codes by scheme, normalized the way [`crate::interest`] stores them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AreaCodes {
    pub sgc: Vec<String>,
    /// SAME codes, including ECCC CLC codes (Canada's SAME location codes).
    pub same: Vec<String>,
    pub ugc: Vec<String>,
    /// Five-digit `SSCCC` county codes.
    pub fips: Vec<String>,
}

impl AreaCodes {
    fn add_same(&mut self, raw: &str) {
        if let Some(code) = normalize_same(raw) {
            push_unique(&mut self.same, code);
        }
    }

    /// An NWS SAME geocode is `PSSCCC`, so it also names a county. Canadian CLC
    /// codes share the SAME slot but are not FIPS, so they never come here.
    fn add_nws_same(&mut self, raw: &str) {
        if let Some(code) = normalize_same(raw) {
            push_unique(&mut self.fips, code[1..].to_string());
            push_unique(&mut self.same, code);
        }
    }
}

impl AlertFacts {
    pub fn from_cap(alert: &Alert) -> Self {
        Self {
            status: alert.status.trim().to_string(),
            msg_type: alert.message_type.trim().to_string(),
            references: reference_ids(&alert.references),
            infos: alert.infos.iter().map(InfoFacts::from_cap).collect(),
        }
    }

    /// Facts from a Haze alert packet (`alert_packet`) for alerts with no CAP.
    pub fn from_packet(packet: &Value) -> Self {
        let content = packet.get("content");
        let text = |key: &str| {
            value_text(content.and_then(|content| content.get(key))).unwrap_or_default()
        };
        let mut info = InfoFacts {
            urgency: text("urgency"),
            severity: text("severity"),
            certainty: text("certainty"),
            ..InfoFacts::default()
        };
        for event in [text("event"), text("event_name")] {
            push_event(&mut info.events, &event);
        }
        let mut facts = Self {
            msg_type: value_text(packet.get("message_type")).unwrap_or_default(),
            infos: vec![info],
            ..Self::default()
        };
        facts.add_packet_same(packet);
        facts
    }

    /// Adds Haze's resolved SAME event and locations to every info block, so a
    /// SAME filter also sees what the feed actually encoded.
    pub fn add_packet_same(&mut self, packet: &Value) {
        let same = packet.get("same");
        let event = value_text(same.and_then(|same| same.get("event")));
        let locations = value_list(same.and_then(|same| same.get("locations")));
        for info in &mut self.infos {
            if let Some(event) = &event {
                push_event(&mut info.events, event);
            }
            for location in &locations {
                info.codes.add_same(location);
            }
        }
    }
}

impl InfoFacts {
    fn from_cap(info: &AlertInfo) -> Self {
        let mut facts = Self {
            language: info.language.trim().to_string(),
            categories: info
                .category
                .iter()
                .map(|value| value.trim().to_string())
                .collect(),
            urgency: info.urgency.trim().to_string(),
            severity: info.severity.trim().to_string(),
            certainty: info.certainty.trim().to_string(),
            ..Self::default()
        };
        push_event(&mut facts.events, &info.event);
        for code in &info.event_codes {
            push_event(&mut facts.events, &code.value);
        }
        // ECCC threat areas carry hazard geometry, not where the alert applies.
        for area in info
            .areas
            .iter()
            .filter(|area| area.threat_status.trim().is_empty())
        {
            for geocode in &area.geocodes {
                facts.add_geocode(&geocode.name, &geocode.value);
            }
            facts.shapes.extend(
                area.polygons
                    .iter()
                    .filter_map(|raw| Shape::parse_polygon(raw)),
            );
            facts.shapes.extend(
                area.circles
                    .iter()
                    .filter_map(|raw| Shape::parse_circle(raw)),
            );
        }
        facts
    }

    fn add_geocode(&mut self, name: &str, value: &str) {
        let name = name.trim();
        let value = value.trim();
        if name.starts_with(SGC_PREFIX) {
            if let Some(code) = normalize_sgc(value) {
                push_unique(&mut self.codes.sgc, code);
            }
        } else if name.eq_ignore_ascii_case("SAME") {
            self.codes.add_nws_same(value);
        } else if name == CLC_NAME {
            self.codes.add_same(value);
        } else if name.eq_ignore_ascii_case("UGC") {
            if let Some(code) = normalize_ugc(value) {
                push_unique(&mut self.codes.ugc, code);
            }
        } else if name.eq_ignore_ascii_case("FIPS6") {
            if let Some(code) = normalize_fips(value) {
                push_unique(&mut self.codes.fips, code);
            }
        }
    }
}

/// CAP `references` is space-separated `sender,identifier,sent` triples.
fn reference_ids(raw: &str) -> Vec<String> {
    raw.split_whitespace()
        .filter_map(|triple| triple.split(',').nth(1))
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .collect()
}

fn push_event(events: &mut Vec<String>, raw: &str) {
    let event = raw.trim().to_lowercase();
    if !event.is_empty() {
        push_unique(events, event);
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}

/// SGC codes are 2 (province), 4 (division), or 7 (subdivision) digits.
pub fn normalize_sgc(raw: &str) -> Option<String> {
    let code = raw.trim();
    (matches!(code.len(), 2 | 4 | 7) && code.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| code.to_string())
}

/// SAME codes are 6 digits. A 5-digit code is a county FIPS with `P` = 0.
pub fn normalize_same(raw: &str) -> Option<String> {
    let code = raw.trim();
    if !code.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match code.len() {
        6 => Some(code.to_string()),
        5 => Some(format!("0{code}")),
        _ => None,
    }
}

/// UGC codes are `SSCNNN`, for example `OHC035` or `SKZ001`.
pub fn normalize_ugc(raw: &str) -> Option<String> {
    let code = raw.trim().to_ascii_uppercase();
    let bytes = code.as_bytes();
    (bytes.len() == 6
        && bytes[..2].iter().all(u8::is_ascii_alphabetic)
        && matches!(bytes[2], b'C' | b'Z')
        && bytes[3..].iter().all(u8::is_ascii_digit))
    .then_some(code)
}

/// County FIPS is `SSCCC`. A 6-digit `PSSCCC` value drops its subdivision digit.
pub fn normalize_fips(raw: &str) -> Option<String> {
    let code = raw.trim();
    if !code.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    match code.len() {
        5 => Some(code.to_string()),
        6 => Some(code[1..].to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use haze_cap::model::{AlertArea, NameValue};
    use serde_json::json;

    fn geocode(name: &str, value: &str) -> NameValue {
        NameValue {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn cap_facts_split_geocodes_by_scheme_and_skip_threat_areas() {
        let alert = Alert {
            identifier: "a".to_string(),
            status: "Actual".to_string(),
            message_type: "Update".to_string(),
            references: "cap-pac@canada.ca,urn:oid:1,2026-04-15T20:00:00-00:00 x,urn:oid:2,2026"
                .to_string(),
            infos: vec![AlertInfo {
                event: "Thunderstorm".to_string(),
                event_codes: vec![geocode("profile:CAP-CP:Event:0.4", "thunderstorm")],
                areas: vec![
                    AlertArea {
                        polygons: vec!["50,-104 51,-104 51,-105 50,-104".to_string()],
                        geocodes: vec![
                            geocode("profile:CAP-CP:Location:0.3", "4706027"),
                            geocode("layer:EC-MSC-SMC:1.0:CLC", "072300"),
                            geocode("UGC", "ohc035"),
                            geocode("FIPS6", "039035"),
                        ],
                        ..AlertArea::default()
                    },
                    AlertArea {
                        threat_status: "active".to_string(),
                        geocodes: vec![geocode("profile:CAP-CP:Location:0.3", "4799999")],
                        ..AlertArea::default()
                    },
                ],
                ..AlertInfo::default()
            }],
            ..Alert::default()
        };
        let facts = AlertFacts::from_cap(&alert);
        assert_eq!(facts.references, vec!["urn:oid:1", "urn:oid:2"]);
        let info = &facts.infos[0];
        assert_eq!(info.events, vec!["thunderstorm"]);
        assert_eq!(info.codes.sgc, vec!["4706027"]);
        assert_eq!(info.codes.same, vec!["072300"]);
        assert_eq!(info.codes.ugc, vec!["OHC035"]);
        assert_eq!(info.codes.fips, vec!["39035"]);
        assert_eq!(info.shapes.len(), 1);
    }

    #[test]
    fn packet_facts_carry_resolved_same_fields() {
        let facts = AlertFacts::from_packet(&json!({
            "message_type": "Alert",
            "content": {"event": "Civil Emergency", "severity": "Extreme"},
            "same": {"event": "CEM", "locations": ["047100", "47200"]}
        }));
        let info = &facts.infos[0];
        assert_eq!(info.events, vec!["civil emergency", "cem"]);
        assert_eq!(info.codes.same, vec!["047100", "047200"]);
        assert_eq!(facts.status, "");
    }
}
