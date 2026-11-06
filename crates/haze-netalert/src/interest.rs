//! Client interest lists: validating a subscribe request and matching alert
//! facts against it.
//!
//! Every criterion narrows the match and an empty criterion matches anything,
//! except `status`, which defaults to `Actual` so tests, exercises, and NAADS
//! heartbeats are opt-in. Location codes and the radius area both describe
//! where the client is, so either one matching is enough.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::facts::{
    normalize_fips, normalize_same, normalize_sgc, normalize_ugc, AlertFacts, InfoFacts,
};
use crate::geo::{valid_point, Circle};

pub const MAX_LIST: usize = 64;
pub const MAX_CODES: usize = 2048;
pub const MAX_RADIUS_KM: f64 = 1000.0;

const STATUS: &[&str] = &["Actual", "Exercise", "System", "Test", "Draft"];
const MSG_TYPE: &[&str] = &["Alert", "Update", "Cancel", "Ack", "Error"];
const CATEGORY: &[&str] = &[
    "Geo",
    "Met",
    "Safety",
    "Security",
    "Rescue",
    "Fire",
    "Health",
    "Env",
    "Transport",
    "Infra",
    "CBRNE",
    "Other",
];
const URGENCY: &[&str] = &["Immediate", "Expected", "Future", "Past", "Unknown"];
const SEVERITY: &[&str] = &["Extreme", "Severe", "Moderate", "Minor", "Unknown"];
const CERTAINTY: &[&str] = &["Observed", "Likely", "Possible", "Unlikely", "Unknown"];

/// The `interest` object of a subscribe request, as sent by the client.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InterestRequest {
    pub locations: LocationsRequest,
    pub area: Option<AreaRequest>,
    pub status: Vec<String>,
    pub msg_type: Vec<String>,
    pub category: Vec<String>,
    pub event: Vec<String>,
    pub urgency: Vec<String>,
    pub severity: Vec<String>,
    pub certainty: Vec<String>,
    pub feeds: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LocationsRequest {
    pub sgc: Vec<String>,
    pub same: Vec<String>,
    pub ugc: Vec<String>,
    pub fips: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AreaRequest {
    pub lat: f64,
    pub lon: f64,
    pub radius_km: f64,
}

/// A validated interest list. It serializes back to the client in the
/// `subscribed` acknowledgement so defaults are visible.
#[derive(Debug, Clone, Serialize)]
pub struct Interest {
    #[serde(skip_serializing_if = "LocationCodes::is_empty")]
    pub locations: LocationCodes,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub area: Option<Circle>,
    pub status: Vec<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub msg_type: Vec<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub category: Vec<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub event: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub urgency: Vec<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub severity: Vec<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub certainty: Vec<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub feeds: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LocationCodes {
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub sgc: BTreeSet<String>,
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub same: BTreeSet<String>,
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub ugc: BTreeSet<String>,
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub fips: BTreeSet<String>,
    /// Division and province prefixes of `sgc`, for alerts that target a
    /// smaller area inside a subscribed one.
    #[serde(skip)]
    sgc_parents: BTreeSet<String>,
}

impl LocationCodes {
    pub fn is_empty(&self) -> bool {
        self.sgc.is_empty() && self.same.is_empty() && self.ugc.is_empty() && self.fips.is_empty()
    }

    /// SGC is hierarchical, so a code matches when either side contains the
    /// other: a subscribed province catches its subdivisions and a
    /// province-wide alert reaches a subscribed subdivision.
    fn matches(&self, info: &InfoFacts) -> bool {
        info.codes.sgc.iter().any(|code| {
            self.sgc_parents.contains(code)
                || [2, 4, 7]
                    .into_iter()
                    .filter(|len| *len <= code.len())
                    .any(|len| self.sgc.contains(&code[..len]))
        }) || info.codes.same.iter().any(|code| self.same.contains(code))
            || info.codes.ugc.iter().any(|code| self.ugc.contains(code))
            || info.codes.fips.iter().any(|code| {
                // A SAME `SS000` county means the whole state.
                self.fips.contains(code)
                    || (code.ends_with("000")
                        && self.fips.iter().any(|fips| fips[..2] == code[..2]))
            })
    }
}

impl Interest {
    pub fn parse(request: InterestRequest) -> Result<Self, String> {
        let mut locations = LocationCodes {
            sgc: codes("sgc", request.locations.sgc, normalize_sgc)?,
            same: codes("same", request.locations.same, normalize_same)?,
            ugc: codes("ugc", request.locations.ugc, normalize_ugc)?,
            fips: codes("fips", request.locations.fips, normalize_fips)?,
            sgc_parents: BTreeSet::new(),
        };
        locations.sgc_parents = locations
            .sgc
            .iter()
            .flat_map(|code| {
                [2, 4]
                    .into_iter()
                    .filter(|len| *len < code.len())
                    .map(|len| code[..len].to_string())
            })
            .collect();
        let area = request
            .area
            .map(|area| {
                if !valid_point(area.lat, area.lon) {
                    return Err("area lat/lon is out of range".to_string());
                }
                if !(area.radius_km.is_finite() && (0.0..=MAX_RADIUS_KM).contains(&area.radius_km))
                {
                    return Err(format!(
                        "area radius_km must be between 0 and {MAX_RADIUS_KM}"
                    ));
                }
                Ok(Circle {
                    lat: area.lat,
                    lon: area.lon,
                    radius_km: area.radius_km,
                })
            })
            .transpose()?;
        let mut status = enums("status", request.status, STATUS)?;
        if status.is_empty() {
            status.push("Actual");
        }
        Ok(Self {
            locations,
            area,
            status,
            msg_type: enums("msg_type", request.msg_type, MSG_TYPE)?,
            category: enums("category", request.category, CATEGORY)?,
            event: texts("event", request.event)?
                .into_iter()
                .map(|event| event.to_lowercase())
                .collect(),
            urgency: enums("urgency", request.urgency, URGENCY)?,
            severity: enums("severity", request.severity, SEVERITY)?,
            certainty: enums("certainty", request.certainty, CERTAINTY)?,
            feeds: texts("feeds", request.feeds)?,
        })
    }

    /// True when the alert satisfies every criterion.
    pub fn matches(&self, facts: &AlertFacts) -> bool {
        if !self.matches_message(facts) {
            return false;
        }
        if facts.infos.is_empty() {
            return !self.has_info_criteria();
        }
        facts.infos.iter().any(|info| self.matches_info(info))
    }

    /// The test for an update or cancellation of an alert the client already
    /// received. Area and severity may legitimately change on a follow-up, so
    /// only the message-level criteria apply.
    pub fn matches_message(&self, facts: &AlertFacts) -> bool {
        any_of(&self.status, &facts.status) && any_of(&self.msg_type, &facts.msg_type)
    }

    pub fn wants_feed(&self, feed_id: &str) -> bool {
        self.feeds
            .iter()
            .any(|feed| feed.eq_ignore_ascii_case(feed_id))
    }

    fn has_info_criteria(&self) -> bool {
        !self.locations.is_empty()
            || self.area.is_some()
            || !self.category.is_empty()
            || !self.event.is_empty()
            || !self.urgency.is_empty()
            || !self.severity.is_empty()
            || !self.certainty.is_empty()
    }

    fn matches_info(&self, info: &InfoFacts) -> bool {
        (self.category.is_empty()
            || info
                .categories
                .iter()
                .any(|category| any_of(&self.category, category)))
            && (self.event.is_empty() || info.events.iter().any(|event| self.event.contains(event)))
            && any_of(&self.urgency, &info.urgency)
            && any_of(&self.severity, &info.severity)
            && any_of(&self.certainty, &info.certainty)
            && self.matches_where(info)
    }

    fn matches_where(&self, info: &InfoFacts) -> bool {
        if self.locations.is_empty() && self.area.is_none() {
            return true;
        }
        self.locations.matches(info)
            || self
                .area
                .is_some_and(|area| info.shapes.iter().any(|shape| area.intersects(shape)))
    }
}

/// An empty filter accepts anything. A missing alert value never matches a
/// non-empty filter.
fn any_of(filter: &[&'static str], value: &str) -> bool {
    filter.is_empty()
        || filter
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(value.trim()))
}

fn enums(
    field: &str,
    values: Vec<String>,
    allowed: &[&'static str],
) -> Result<Vec<&'static str>, String> {
    let mut out = Vec::new();
    for value in texts(field, values)? {
        let Some(canonical) = allowed
            .iter()
            .find(|allowed| allowed.eq_ignore_ascii_case(&value))
        else {
            return Err(format!(
                "unknown {field} {value:?}; expected one of {}",
                allowed.join(", ")
            ));
        };
        if !out.contains(canonical) {
            out.push(*canonical);
        }
    }
    Ok(out)
}

fn texts(field: &str, values: Vec<String>) -> Result<Vec<String>, String> {
    if values.len() > MAX_LIST {
        return Err(format!("{field} lists at most {MAX_LIST} values"));
    }
    let mut out = Vec::new();
    for value in values {
        let value = value.trim().to_string();
        if !value.is_empty() && !out.contains(&value) {
            out.push(value);
        }
    }
    Ok(out)
}

fn codes(
    field: &str,
    values: Vec<String>,
    normalize: fn(&str) -> Option<String>,
) -> Result<BTreeSet<String>, String> {
    if values.len() > MAX_CODES {
        return Err(format!("locations.{field} lists at most {MAX_CODES} codes"));
    }
    values
        .iter()
        .map(|raw| normalize(raw).ok_or_else(|| format!("invalid {field} location code {raw:?}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facts::AreaCodes;
    use crate::geo::Shape;

    fn interest(json: serde_json::Value) -> Interest {
        Interest::parse(serde_json::from_value(json).unwrap()).unwrap()
    }

    fn facts(status: &str, msg_type: &str, info: InfoFacts) -> AlertFacts {
        AlertFacts {
            status: status.to_string(),
            msg_type: msg_type.to_string(),
            references: Vec::new(),
            infos: vec![info],
        }
    }

    fn saskatoon_warning() -> InfoFacts {
        InfoFacts {
            categories: vec!["Met".to_string()],
            events: vec!["thunderstorm".to_string()],
            urgency: "Immediate".to_string(),
            severity: "Severe".to_string(),
            certainty: "Likely".to_string(),
            codes: AreaCodes {
                sgc: vec!["4711066".to_string()],
                same: vec!["074100".to_string()],
                ..AreaCodes::default()
            },
            shapes: vec![
                Shape::parse_polygon("52.2,-106.8 52.2,-106.5 52.0,-106.5 52.0,-106.8").unwrap(),
            ],
            ..InfoFacts::default()
        }
    }

    #[test]
    fn status_defaults_to_actual() {
        let any = interest(serde_json::json!({}));
        assert!(any.matches(&facts("Actual", "Alert", saskatoon_warning())));
        assert!(!any.matches(&facts("Test", "Alert", saskatoon_warning())));
    }

    #[test]
    fn sgc_matches_in_both_directions_of_the_hierarchy() {
        let province = interest(serde_json::json!({"locations": {"sgc": ["47"]}}));
        assert!(province.matches(&facts("Actual", "Alert", saskatoon_warning())));

        let mut province_wide = saskatoon_warning();
        province_wide.codes.sgc = vec!["47".to_string()];
        let subdivision = interest(serde_json::json!({"locations": {"sgc": ["4711066"]}}));
        assert!(subdivision.matches(&facts("Actual", "Alert", province_wide)));

        let other = interest(serde_json::json!({"locations": {"sgc": ["4706027"]}}));
        assert!(!other.matches(&facts("Actual", "Alert", saskatoon_warning())));
    }

    #[test]
    fn location_codes_or_radius_either_place_the_client() {
        let regina_with_saskatoon_radius = interest(serde_json::json!({
            "locations": {"sgc": ["4706027"]},
            "area": {"lat": 52.13, "lon": -106.67, "radius_km": 5}
        }));
        assert!(regina_with_saskatoon_radius.matches(&facts(
            "Actual",
            "Alert",
            saskatoon_warning()
        )));

        let far_radius =
            interest(serde_json::json!({"area": {"lat": 50.45, "lon": -104.61, "radius_km": 25}}));
        assert!(!far_radius.matches(&facts("Actual", "Alert", saskatoon_warning())));
    }

    #[test]
    fn enum_and_event_filters_narrow_the_match() {
        let severe_storms = interest(serde_json::json!({
            "severity": ["severe", "extreme"], "event": ["Thunderstorm"], "category": ["met"]
        }));
        assert!(severe_storms.matches(&facts("Actual", "Alert", saskatoon_warning())));

        let mut moderate = saskatoon_warning();
        moderate.severity = "Moderate".to_string();
        assert!(!severe_storms.matches(&facts("Actual", "Alert", moderate)));
    }

    #[test]
    fn follow_ups_only_check_message_level_criteria() {
        let alerts_only =
            interest(serde_json::json!({"msg_type": ["Alert", "Update"], "severity": ["Severe"]}));
        let mut downgraded = saskatoon_warning();
        downgraded.severity = "Minor".to_string();
        let update = facts("Actual", "Update", downgraded);
        assert!(!alerts_only.matches(&update));
        assert!(alerts_only.matches_message(&update));
        assert!(!alerts_only.matches_message(&facts("Actual", "Cancel", saskatoon_warning())));
    }

    #[test]
    fn rejects_unknown_values_and_bad_codes() {
        let parse = |json| Interest::parse(serde_json::from_value(json).unwrap());
        assert!(parse(serde_json::json!({"severity": ["Huge"]})).is_err());
        assert!(parse(serde_json::json!({"locations": {"same": ["12"]}})).is_err());
        assert!(
            parse(serde_json::json!({"area": {"lat": 50, "lon": -104, "radius_km": 5000}}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<InterestRequest>(serde_json::json!({"colour": ["red"]}))
                .is_err()
        );
    }
}
