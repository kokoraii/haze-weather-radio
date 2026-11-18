//! Province lookups for alerts that name a whole province or territory.
//!
//! NAADS alerts (including the national Alert Ready tests) often name only an
//! SGC province code (`47` for Saskatchewan). A feed covers ECCC CLC codes, so
//! [`FeedCoverage::province_codes`] picks the feed's codes inside that province.
//! ECCC numbers land CLC codes by province in their first three digits; this
//! table matches the location catalog (`managed/locations/ca-weather.sqlite`).
//! `00x` codes (marine and multi-province areas) belong to no province.

use crate::coverage::is_same_code;
use crate::model::{FeedCoverage, MatchKind};

/// The province or territory an SGC province code (`profile:CAP-CP:Location`)
/// names, as its postal abbreviation.
pub fn sgc_province(code: &str) -> Option<&'static str> {
    Some(match code {
        "10" => "NL",
        "11" => "PE",
        "12" => "NS",
        "13" => "NB",
        "24" => "QC",
        "35" => "ON",
        "46" => "MB",
        "47" => "SK",
        "48" => "AB",
        "59" => "BC",
        "60" => "YT",
        "61" => "NT",
        "62" => "NU",
        _ => return None,
    })
}

/// The province or territory of an ECCC CLC code, by its first three digits.
pub fn clc_province(code: &str) -> Option<&'static str> {
    if !is_same_code(code) {
        return None;
    }
    let prefix: u16 = code[..3].parse().ok()?;
    Some(match prefix {
        11..=14 => "NS",
        15..=17 => "NB",
        18 => "PE",
        21..=29 => "NL",
        31..=39 => "QC",
        41..=49 => "ON",
        51..=59 => "MB",
        61..=68 => "SK",
        71..=79 => "AB",
        81..=89 => "BC",
        91..=93 => "YT",
        94..=96 => "NT",
        97..=99 => "NU",
        _ => return None,
    })
}

impl FeedCoverage {
    /// The feed's SAME codes inside `province`: its region codes, exact codes
    /// and listed subregions there. An alert for the whole province affects all
    /// of them. NWS entries and bare wildcard patterns are left out, since they
    /// name no single CLC code.
    pub fn province_codes(&self, province: &str) -> Vec<String> {
        let mut codes = Vec::<String>::new();
        for entry in self.entries.iter().filter(|entry| entry.source != "nws") {
            let subregions = entry
                .subregions
                .iter()
                .filter(|sub| sub.kind == MatchKind::Exact)
                .map(|sub| sub.value.as_str());
            for code in std::iter::once(entry.region_code.as_str()).chain(subregions) {
                if clc_province(code) == Some(province) && !codes.iter().any(|known| known == code) {
                    codes.push(code.to_string());
                }
            }
        }
        codes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile_feed_xml;

    #[test]
    fn province_codes_cover_the_feed_inside_the_province_only() {
        let feed = compile_feed_xml(
            "cwxr-sk01",
            r#"<feed><locations><coverage>
                <region id="065100" source="eccc"/>
                <region id="065500" source="eccc"><subregion id="065512"/></region>
                <region id="052600" source="eccc"/>
                <region id="038017" source="nws"/>
                <region id="06551*" source="eccc"/>
            </coverage></locations></feed>"#,
        )
        .expect("valid feed");
        assert_eq!(feed.province_codes("SK"), vec!["065100", "065500", "065512"]);
        assert_eq!(feed.province_codes("MB"), vec!["052600"]);
        assert!(feed.province_codes("QC").is_empty(), "NWS codes are not CLC codes");
        assert_eq!(sgc_province("47"), Some("SK"));
        assert_eq!(sgc_province("4711066"), None);
        assert_eq!(clc_province("004100"), None, "00x codes have no province");
    }
}
