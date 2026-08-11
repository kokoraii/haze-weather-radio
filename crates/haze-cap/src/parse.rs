use std::collections::HashSet;

use chrono::{DateTime, FixedOffset, NaiveDateTime};
use quick_xml::de::from_str;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

use crate::model::{
    Alert, AlertArea, AlertInfo, AtomEntry, GeoPoint, NameValue, Resource, StormInfo,
};

const ECCC_THREAT_AREA_GEOCODE: &str = "layer:EC-MSC-SMC:DLC:1.1";
const ECCC_STORM_SPEED: &str = "layer:EC-MSC-SMC:1.1:Storm_Speed";
const ECCC_STORM_DIRECTION: &str = "layer:EC-MSC-SMC:1.1:Storm_Direction";
const ECCC_STORM_GEOMETRY_TYPE: &str = "layer:EC-MSC-SMC:1.1:Storm_Geometry_Type";
const ECCC_STORM_POINT: &str = "layer:EC-MSC-SMC:1.1:Storm_Point";
const ECCC_STORM_TIME: &str = "layer:EC-MSC-SMC:1.1:Storm_Time";
const ECCC_MOTION_DESCRIPTION: &str = "layer:EC-MSC-SMC:1.1:Motion_Description";
const ECCC_STORM_POSITION: &str = "layer:EC-MSC-SMC:1.1:Storm_Position_Description";
const ECCC_REFERENCE_LOCATIONS: &str = "layer:EC-MSC-SMC:1.1:Reference_Location_Points";

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("CAP XML is not UTF-8")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("failed to parse XML: {0}")]
    Xml(#[from] quick_xml::DeError),
    #[error("invalid CAP alert {identifier}: {warnings}")]
    InvalidCap {
        identifier: String,
        warnings: String,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename = "alert")]
struct CapXml {
    #[serde(default)]
    identifier: String,
    #[serde(default)]
    sender: String,
    #[serde(default)]
    sent: String,
    #[serde(default)]
    status: String,
    #[serde(rename = "msgType", default)]
    msg_type: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    note: String,
    #[serde(rename = "code", default)]
    code: Vec<String>,
    #[serde(default)]
    references: String,
    #[serde(default)]
    incidents: String,
    #[serde(rename = "info", default)]
    infos: Vec<InfoXml>,
}

#[derive(Debug, Default, Deserialize)]
struct InfoXml {
    #[serde(default)]
    language: String,
    #[serde(rename = "category", default)]
    category: Vec<String>,
    #[serde(default)]
    event: String,
    #[serde(rename = "responseType", default)]
    response: Vec<String>,
    #[serde(default)]
    urgency: String,
    #[serde(default)]
    severity: String,
    #[serde(default)]
    certainty: String,
    #[serde(default)]
    audience: String,
    #[serde(default)]
    effective: String,
    #[serde(default)]
    onset: String,
    #[serde(default)]
    expires: String,
    #[serde(rename = "senderName", default)]
    sender_name: String,
    #[serde(default)]
    headline: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    instruction: String,
    #[serde(default)]
    web: String,
    #[serde(rename = "eventCode", default)]
    event_codes: Vec<PairXml>,
    #[serde(rename = "area", default)]
    areas: Vec<AreaXml>,
    #[serde(rename = "parameter", default)]
    parameters: Vec<PairXml>,
    #[serde(rename = "resource", default)]
    resources: Vec<ResourceXml>,
}

#[derive(Debug, Default, Deserialize)]
struct AreaXml {
    #[serde(rename = "areaDesc", default)]
    description: String,
    #[serde(rename = "polygon", default)]
    polygons: Vec<String>,
    #[serde(rename = "circle", default)]
    circles: Vec<String>,
    #[serde(rename = "geocode", default)]
    geocodes: Vec<PairXml>,
}

#[derive(Debug, Default, Deserialize)]
struct PairXml {
    #[serde(rename = "valueName", default)]
    name: String,
    #[serde(default)]
    value: String,
}

#[derive(Debug, Default, Deserialize)]
struct ResourceXml {
    #[serde(rename = "resourceDesc", default)]
    description: String,
    #[serde(rename = "mimeType", default)]
    mime_type: String,
    #[serde(default)]
    uri: String,
    #[serde(rename = "derefUri", default)]
    deref_uri: String,
}

#[derive(Debug, Default, Deserialize)]
struct AtomFeedXml {
    #[serde(rename = "entry", default)]
    entries: Vec<AtomEntryXml>,
}

#[derive(Debug, Default, Deserialize)]
struct AtomEntryXml {
    #[serde(default)]
    id: String,
    #[serde(default)]
    updated: String,
    #[serde(rename = "link", default)]
    links: Vec<AtomLinkXml>,
}

#[derive(Debug, Default, Deserialize)]
struct AtomLinkXml {
    #[serde(rename = "@href", default)]
    href: String,
}

pub fn parse_cap(raw: &[u8]) -> Result<Alert, ParseError> {
    let xml = std::str::from_utf8(raw)?;
    let parsed: CapXml = from_str(xml)?;
    let mut alert = Alert {
        identifier: clean(&parsed.identifier),
        sender: clean(&parsed.sender),
        sent: clean(&parsed.sent),
        status: clean(&parsed.status),
        message_type: clean(&parsed.msg_type),
        scope: clean(&parsed.scope),
        note: clean(&parsed.note),
        code: clean_slice(parsed.code),
        references: clean(&parsed.references),
        incidents: clean(&parsed.incidents),
        infos: parsed.infos.into_iter().map(normalize_info).collect(),
        raw_xml: xml.to_string(),
        warnings: Vec::new(),
    };
    alert.warnings = validate_cap(&alert);
    if alert
        .warnings
        .iter()
        .any(|warning| warning.starts_with("fatal:"))
    {
        return Err(ParseError::InvalidCap {
            identifier: alert.identifier.clone(),
            warnings: alert.warnings.join("; "),
        });
    }
    Ok(alert)
}

pub fn parse_atom_entries(raw: &[u8]) -> Result<Vec<AtomEntry>, ParseError> {
    let xml = std::str::from_utf8(raw)?;
    let parsed: AtomFeedXml = from_str(xml)?;
    let mut entries = Vec::new();
    for entry in parsed.entries {
        let id = clean(&entry.id);
        let updated = clean(&entry.updated);
        let mut links = Vec::new();
        for link in entry.links {
            append_cap_link(&mut links, &clean(&link.href));
        }
        if id.starts_with("http") && links.is_empty() {
            append_cap_link(&mut links, &id);
        }
        if !id.is_empty() && !links.is_empty() {
            entries.push(AtomEntry { id, updated, links });
        }
    }
    Ok(entries)
}

fn normalize_info(info: InfoXml) -> AlertInfo {
    let parameters = normalize_pairs(info.parameters);
    let storm = normalize_eccc_storm(&parameters);
    let areas = info
        .areas
        .into_iter()
        .map(|area| {
            let geocodes = normalize_pairs(area.geocodes);
            AlertArea {
                description: clean(&area.description),
                polygons: clean_slice(area.polygons),
                circles: clean_slice(area.circles),
                threat_status: eccc_threat_status(&geocodes),
                geocodes,
            }
        })
        .collect();
    AlertInfo {
        language: clean(&info.language),
        category: clean_slice(info.category),
        event: clean(&info.event),
        response: clean_slice(info.response),
        urgency: clean(&info.urgency),
        severity: clean(&info.severity),
        certainty: clean(&info.certainty),
        audience: clean(&info.audience),
        effective: clean(&info.effective),
        onset: clean(&info.onset),
        expires: clean(&info.expires),
        sender_name: clean(&info.sender_name),
        headline: clean(&info.headline),
        description: clean(&info.description),
        instruction: clean(&info.instruction),
        web: clean(&info.web),
        event_codes: normalize_pairs(info.event_codes),
        areas,
        parameters,
        resources: info
            .resources
            .into_iter()
            .map(|resource| Resource {
                description: clean(&resource.description),
                mime_type: clean(&resource.mime_type),
                uri: clean(&resource.uri),
                deref_uri: clean(&resource.deref_uri),
            })
            .collect(),
        storm,
    }
}

fn validate_cap(alert: &Alert) -> Vec<String> {
    let mut warnings = Vec::new();
    for (name, value) in [
        ("identifier", alert.identifier.as_str()),
        ("sender", alert.sender.as_str()),
        ("sent", alert.sent.as_str()),
        ("status", alert.status.as_str()),
        ("msgType", alert.message_type.as_str()),
        ("scope", alert.scope.as_str()),
    ] {
        if value.trim().is_empty() {
            warnings.push(format!("fatal: missing {name}"));
        }
    }
    if !alert.sent.is_empty() && parse_cap_time(&alert.sent).is_none() {
        warnings.push("fatal: invalid sent timestamp".to_string());
    }
    append_enum_warning(
        &mut warnings,
        "status",
        &alert.status,
        &["Actual", "Exercise", "System", "Test", "Draft"],
        true,
    );
    append_enum_warning(
        &mut warnings,
        "msgType",
        &alert.message_type,
        &["Alert", "Update", "Cancel", "Ack", "Error"],
        true,
    );
    append_enum_warning(
        &mut warnings,
        "scope",
        &alert.scope,
        &["Public", "Restricted", "Private"],
        true,
    );
    if alert.infos.is_empty()
        && !alert.message_type.eq_ignore_ascii_case("Cancel")
        && !alert.status.eq_ignore_ascii_case("System")
    {
        warnings.push("fatal: non-cancel alert has no info block".to_string());
    }
    for (index, info) in alert.infos.iter().enumerate() {
        let prefix = format!("info[{index}]");
        if info.event.is_empty() {
            warnings.push(format!("{prefix}: missing event"));
        }
        append_enum_warning(
            &mut warnings,
            &format!("{prefix}.urgency"),
            &info.urgency,
            &["Immediate", "Expected", "Future", "Past", "Unknown"],
            false,
        );
        append_enum_warning(
            &mut warnings,
            &format!("{prefix}.severity"),
            &info.severity,
            &["Extreme", "Severe", "Moderate", "Minor", "Unknown"],
            false,
        );
        append_enum_warning(
            &mut warnings,
            &format!("{prefix}.certainty"),
            &info.certainty,
            &["Observed", "Likely", "Possible", "Unlikely", "Unknown"],
            false,
        );
        for (name, value) in [
            ("effective", info.effective.as_str()),
            ("onset", info.onset.as_str()),
            ("expires", info.expires.as_str()),
        ] {
            if !value.is_empty() && parse_cap_time(value).is_none() {
                warnings.push(format!("{prefix}: invalid {name} timestamp"));
            }
        }
        for resource in &info.resources {
            if !resource.uri.is_empty() && !resource.uri.starts_with("cid:") {
                match Url::parse(&resource.uri) {
                    Ok(parsed) if !parsed.scheme().is_empty() => {}
                    _ => warnings.push(format!("{prefix}: resource URI is not absolute")),
                }
            }
        }
        validate_eccc_2026_info(info, &prefix, &mut warnings);
    }
    warnings
}

fn append_enum_warning(
    warnings: &mut Vec<String>,
    name: &str,
    value: &str,
    allowed: &[&str],
    fatal: bool,
) {
    if value.trim().is_empty() {
        return;
    }
    if allowed.iter().any(|item| value.eq_ignore_ascii_case(item)) {
        return;
    }
    if fatal {
        warnings.push(format!("fatal: invalid {name} {value}"));
    } else {
        warnings.push(format!("invalid {name} {value}"));
    }
}

fn parse_cap_time(raw: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(raw.trim()).ok()
}

fn normalize_pairs(values: Vec<PairXml>) -> Vec<NameValue> {
    values
        .into_iter()
        .filter_map(|value| {
            let name = clean(&value.name);
            let value = clean(&value.value);
            if name.is_empty() && value.is_empty() {
                None
            } else {
                Some(NameValue { name, value })
            }
        })
        .collect()
}

fn eccc_threat_status(geocodes: &[NameValue]) -> String {
    geocodes
        .iter()
        .find(|geocode| geocode.name.eq_ignore_ascii_case(ECCC_THREAT_AREA_GEOCODE))
        .map(|geocode| geocode.value.trim().to_ascii_lowercase())
        .unwrap_or_default()
}

fn normalize_eccc_storm(parameters: &[NameValue]) -> Option<StormInfo> {
    let mut storm = StormInfo::default();
    let mut found = false;
    for parameter in parameters {
        let name = parameter.name.trim();
        let value = parameter.value.trim();
        if name.eq_ignore_ascii_case(ECCC_STORM_SPEED) {
            found = true;
            storm.speed = value.to_string();
            if let Some((number, unit)) = parse_eccc_storm_speed(value) {
                storm.speed_value = Some(number);
                storm.speed_unit = unit;
            }
        } else if name.eq_ignore_ascii_case(ECCC_STORM_DIRECTION) {
            found = true;
            storm.direction_degrees = value
                .parse::<f64>()
                .ok()
                .filter(|number| number.is_finite() && (0.0..=360.0).contains(number));
        } else if name.eq_ignore_ascii_case(ECCC_STORM_GEOMETRY_TYPE) {
            found = true;
            storm.geometry_type = value.to_ascii_lowercase();
        } else if name.eq_ignore_ascii_case(ECCC_STORM_POINT) {
            found = true;
            storm
                .points
                .extend(value.split_whitespace().filter_map(parse_eccc_storm_point));
        } else if name.eq_ignore_ascii_case(ECCC_STORM_TIME) {
            found = true;
            storm.time = value.to_string();
        } else if name.eq_ignore_ascii_case(ECCC_MOTION_DESCRIPTION) {
            found = true;
            storm.motion_description = value.to_string();
        } else if name.eq_ignore_ascii_case(ECCC_STORM_POSITION) {
            found = true;
            storm.position_description = value.to_string();
        } else if name.eq_ignore_ascii_case(ECCC_REFERENCE_LOCATIONS) {
            found = true;
            storm.reference_location_points = value.to_string();
        }
    }
    found.then_some(storm)
}

fn validate_eccc_2026_info(info: &AlertInfo, prefix: &str, warnings: &mut Vec<String>) {
    for (index, area) in info.areas.iter().enumerate() {
        if area.threat_status.is_empty() {
            continue;
        }
        if !matches!(
            area.threat_status.as_str(),
            "issued" | "continued" | "ended" | "cancelled"
        ) {
            warnings.push(format!(
                "{prefix}.area[{index}]: invalid ECCC threat status {}",
                area.threat_status
            ));
        }
        if area.polygons.is_empty() {
            warnings.push(format!(
                "{prefix}.area[{index}]: ECCC threat area has no polygon"
            ));
        }
    }
    for parameter in &info.parameters {
        let name = parameter.name.trim();
        let value = parameter.value.trim();
        if name.eq_ignore_ascii_case(ECCC_STORM_SPEED) {
            if parse_eccc_storm_speed(value).is_none() {
                warnings.push(format!("{prefix}: invalid ECCC storm speed"));
            }
        } else if name.eq_ignore_ascii_case(ECCC_STORM_DIRECTION) {
            if value
                .parse::<f64>()
                .ok()
                .filter(|number| number.is_finite() && (0.0..=360.0).contains(number))
                .is_none()
            {
                warnings.push(format!("{prefix}: invalid ECCC storm direction"));
            }
        } else if name.eq_ignore_ascii_case(ECCC_STORM_GEOMETRY_TYPE) {
            if !matches!(
                value.to_ascii_lowercase().as_str(),
                "isolated_cell" | "area" | "line"
            ) {
                warnings.push(format!("{prefix}: invalid ECCC storm geometry type"));
            }
        } else if name.eq_ignore_ascii_case(ECCC_STORM_POINT) {
            let points: Vec<_> = value.split_whitespace().collect();
            if points.is_empty()
                || points
                    .iter()
                    .any(|point| parse_eccc_storm_point(point).is_none())
            {
                warnings.push(format!("{prefix}: invalid ECCC storm point"));
            }
        } else if name.eq_ignore_ascii_case(ECCC_STORM_TIME) && !valid_eccc_storm_time(value) {
            warnings.push(format!("{prefix}: invalid ECCC storm time"));
        }
    }
}

fn parse_eccc_storm_speed(raw: &str) -> Option<(f64, String)> {
    let fields: Vec<_> = raw.split_whitespace().collect();
    if fields.len() != 2 {
        return None;
    }
    let value = fields[0].parse::<f64>().ok()?;
    let unit = fields[1].to_ascii_lowercase();
    if !value.is_finite() || value < 0.0 || !matches!(unit.as_str(), "km/h" | "knots") {
        return None;
    }
    Some((value, unit))
}

fn parse_eccc_storm_point(raw: &str) -> Option<GeoPoint> {
    let (latitude, longitude) = raw.trim().split_once(',')?;
    let latitude = latitude.trim().parse::<f64>().ok()?;
    let longitude = longitude.trim().parse::<f64>().ok()?;
    if !latitude.is_finite()
        || !longitude.is_finite()
        || !(-90.0..=90.0).contains(&latitude)
        || !(-180.0..=180.0).contains(&longitude)
    {
        return None;
    }
    Some(GeoPoint {
        latitude,
        longitude,
    })
}

fn valid_eccc_storm_time(raw: &str) -> bool {
    raw.len() == 17
        && raw.bytes().all(|byte| byte.is_ascii_digit())
        && NaiveDateTime::parse_from_str(&raw[..14], "%Y%m%d%H%M%S").is_ok()
        && raw[14..].parse::<u16>().is_ok_and(|value| value <= 999)
}

fn clean_slice(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| clean(&value))
        .filter(|value| !value.is_empty())
        .collect()
}

fn clean(value: &str) -> String {
    value.trim().to_string()
}

fn append_cap_link(links: &mut Vec<String>, href: &str) {
    if href.is_empty() {
        return;
    }
    let mut seen: HashSet<String> = links.iter().cloned().collect();
    if seen.insert(href.to_string()) {
        links.push(href.to_string());
    }
    if href.starts_with("http") && !href.ends_with(".cap") {
        let cap_url = format!("{}.cap", href.trim_end_matches('/'));
        if seen.insert(cap_url.clone()) {
            links.push(cap_url);
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_cap_to_go_compatible_shape() {
        let raw = br#"
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>abc</identifier>
  <sender>sender@example.test</sender>
  <sent>2026-06-22T12:38:00-06:00</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <code>profile:CAP-CP:0.4</code>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>Severe Thunderstorm Warning</event>
    <responseType>Shelter</responseType>
    <urgency>Immediate</urgency>
    <severity>Severe</severity>
    <certainty>Likely</certainty>
    <senderName>Environment Canada</senderName>
    <headline>Severe thunderstorm warning</headline>
    <description>Storm text.</description>
    <instruction>Go indoors.</instruction>
    <eventCode><valueName>SAME</valueName><value>SVR</value></eventCode>
    <area>
      <areaDesc>Saskatoon</areaDesc>
      <geocode><valueName>CLC</valueName><value>065100</value></geocode>
    </area>
    <resource>
      <resourceDesc>audio</resourceDesc>
      <mimeType>audio/mpeg</mimeType>
      <uri>https://example.test/audio.mp3</uri>
    </resource>
  </info>
</alert>
"#;
        let alert = parse_cap(raw).expect("cap parsed");
        let value = serde_json::to_value(&alert).expect("json");
        assert_eq!(value["identifier"], "abc");
        assert_eq!(value["message_type"], "Alert");
        assert_eq!(value["infos"][0]["response_type"], json!(["Shelter"]));
        assert_eq!(value["infos"][0]["event_codes"][0]["value"], "SVR");
        assert_eq!(
            value["infos"][0]["areas"][0]["geocodes"][0]["value"],
            "065100"
        );
    }

    #[test]
    fn parses_atom_links_and_cap_fallbacks() {
        let raw = br#"
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>https://alerts.example/item/1</id>
    <updated>2026-06-22T12:39:00Z</updated>
    <link href="https://alerts.example/item/1"/>
  </entry>
</feed>
"#;
        let entries = parse_atom_entries(raw).expect("atom parsed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "https://alerts.example/item/1");
        assert_eq!(entries[0].links[0], "https://alerts.example/item/1");
        assert_eq!(entries[0].links[1], "https://alerts.example/item/1.cap");
    }

    #[test]
    fn parses_eccc_august_2026_threat_areas_and_storm_info() {
        let raw =
            include_bytes!("../../../services/go/testdata/cap/eccc_2026_true_threat_area.xml");
        let alert = parse_cap(raw).expect("ECCC August 2026 CAP parsed");
        let info = &alert.infos[0];
        assert_eq!(info.areas.len(), 5);
        assert_eq!(info.areas[0].threat_status, "");
        assert_eq!(info.areas[1].threat_status, "issued");
        assert_eq!(info.areas[2].threat_status, "continued");
        assert_eq!(info.areas[3].threat_status, "ended");
        assert_eq!(info.areas[4].threat_status, "cancelled");
        let storm = info.storm.as_ref().expect("storm info");
        assert_eq!(storm.speed_value, Some(40.0));
        assert_eq!(storm.speed_unit, "km/h");
        assert_eq!(storm.direction_degrees, Some(90.12841));
        assert_eq!(storm.geometry_type, "isolated_cell");
        assert_eq!(storm.points.len(), 1);
        assert_eq!(storm.points[0].latitude, 52.1433);
        assert_eq!(storm.points[0].longitude, -106.6732);
        assert_eq!(storm.time, "20260811153000000");
        assert!(alert.warnings.is_empty(), "warnings: {:?}", alert.warnings);
    }

    #[test]
    fn warns_on_non_finite_eccc_storm_values() {
        let raw = br#"
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>invalid-eccc-storm</identifier>
  <sender>cap-pac@canada.ca</sender>
  <sent>2026-08-11T15:30:00Z</sent>
  <status>Actual</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <info>
    <language>en-CA</language>
    <category>Met</category>
    <event>tornado warning</event>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:Storm_Speed</valueName><value>NaN km/h</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:Storm_Direction</valueName><value>NaN</value></parameter>
    <parameter><valueName>layer:EC-MSC-SMC:1.1:Storm_Point</valueName><value>NaN,-106.67</value></parameter>
  </info>
</alert>
"#;
        let alert = parse_cap(raw).expect("invalid typed values remain non-fatal");
        let warnings = alert.warnings.join(" | ");
        assert!(warnings.contains("invalid ECCC storm speed"));
        assert!(warnings.contains("invalid ECCC storm direction"));
        assert!(warnings.contains("invalid ECCC storm point"));
    }

    #[test]
    fn accepts_naads_heartbeat_without_info() {
        let raw = br#"
<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">
  <identifier>heartbeat-1</identifier>
  <sender>NAADS-Heartbeat</sender>
  <sent>2026-06-22T12:38:00-06:00</sent>
  <status>System</status>
  <msgType>Alert</msgType>
  <scope>Public</scope>
  <references>sender,abc,2026-06-22T12:37:00-06:00</references>
</alert>
"#;
        let alert = parse_cap(raw).expect("heartbeat parsed");
        assert_eq!(alert.status, "System");
        assert!(alert.infos.is_empty());
    }
}
