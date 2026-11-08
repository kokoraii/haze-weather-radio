//! Coverage rules over a compiled [`FeedCoverage`]. Inputs are normalized
//! location codes (see [`normalize_code`]).

use std::collections::BTreeSet;

use crate::model::{CoverageEntry, FeedCoverage, HeaderPolicy, MatchKind};

/// The most location codes one SAME header can carry.
pub const SAME_MAX_LOCATIONS: usize = 31;

/// A SAME location list, plus the codes the 31-code limit pushed out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SameHeader {
    pub codes: Vec<String>,
    pub dropped: Vec<String>,
}

/// Uppercases a location code and strips everything but letters and digits.
pub fn normalize_code(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|value| value.to_ascii_uppercase())
        .collect()
}

pub(crate) fn is_same_code(value: &str) -> bool {
    value.len() == 6 && value.bytes().all(|byte| byte.is_ascii_digit())
}

impl FeedCoverage {
    /// Whether the feed's coverage includes `code`.
    pub fn covers(&self, code: &str) -> bool {
        self.lists(code) || self.region_for(code).is_some()
    }

    /// Whether `code` is configured itself: an exact entry or a listed subregion.
    pub fn lists(&self, code: &str) -> bool {
        self.entries.iter().any(|entry| {
            (entry.matcher.kind == MatchKind::Exact && entry.matcher.value == code)
                || entry.subregions.iter().any(|sub| sub.matches(code))
        })
    }

    /// The wildcard entry whose pattern contains `code`.
    pub fn region_for(&self, code: &str) -> Option<&CoverageEntry> {
        self.entries
            .iter()
            .find(|entry| entry.matcher.kind == MatchKind::Prefix && entry.matcher.matches(code))
    }

    /// The SAME locations for an alert affecting `codes`. Each covering region
    /// code is added, so radios set to the region or a subregion both alarm. A
    /// region with a subregion list keeps only listed subregions; one without
    /// keeps every affected code. Regions come first, then listed subregions,
    /// then the rest, so the 31-code limit drops the least specific codes.
    pub fn same_header<'a>(&self, codes: impl IntoIterator<Item = &'a str>) -> SameHeader {
        let mut regions = BTreeSet::new();
        let mut listed = BTreeSet::new();
        let mut rest = BTreeSet::new();
        for code in codes {
            let region = self.region_for(code);
            if let Some(region) = region {
                if !region.region_code.is_empty() {
                    regions.insert(region.region_code.clone());
                    if region.region_code == code {
                        // The whole region is affected, so its listed codes are too.
                        listed.extend(exact_subregions(region));
                        continue;
                    }
                }
            }
            if self.lists(code) {
                listed.insert(code.to_string());
            } else if region.is_none_or(|region| region.header == HeaderPolicy::All) {
                rest.insert(code.to_string());
            }
        }
        limit(regions.into_iter().chain(listed).chain(rest))
    }

    /// The SAME locations for the feed's whole coverage, used when an alert
    /// names no locations of its own (automations and operator alerts).
    pub fn full_header(&self) -> SameHeader {
        let regions = self
            .entries
            .iter()
            .filter(|entry| is_same_code(&entry.region_code))
            .map(|entry| entry.region_code.clone());
        let listed = self.entries.iter().flat_map(exact_subregions).collect::<BTreeSet<_>>();
        limit(regions.chain(listed))
    }
}

fn exact_subregions(entry: &CoverageEntry) -> impl Iterator<Item = String> + '_ {
    entry
        .subregions
        .iter()
        .filter(|sub| sub.kind == MatchKind::Exact && is_same_code(&sub.value))
        .map(|sub| sub.value.clone())
}

fn limit(codes: impl Iterator<Item = String>) -> SameHeader {
    let mut ordered = Vec::<String>::new();
    for code in codes {
        if !ordered.contains(&code) {
            ordered.push(code);
        }
    }
    let dropped = ordered.split_off(ordered.len().min(SAME_MAX_LOCATIONS));
    SameHeader {
        codes: ordered,
        dropped,
    }
}
