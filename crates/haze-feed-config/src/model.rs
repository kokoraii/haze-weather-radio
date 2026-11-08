//! Wire types of the feed config snapshot. The JSON shape is the contract in
//! `contracts/feedconfig/v1/snapshot.json`; Go mirrors it field for field.

use serde::{Deserialize, Serialize};

/// Bumped when a consumer could misread an older or newer snapshot.
pub const SNAPSHOT_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedConfigSnapshot {
    pub version: u32,
    /// Changes whenever any feed file changes; consumers compare it to skip
    /// redundant reloads.
    pub generation: String,
    pub feeds: Vec<FeedCoverage>,
}

impl FeedConfigSnapshot {
    pub fn feed(&self, feed_id: &str) -> Option<&FeedCoverage> {
        self.feeds.iter().find(|feed| feed.feed_id == feed_id)
    }
}

/// One feed's coverage. No entries means the feed takes alerts for every
/// location, subject to its own alert source settings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedCoverage {
    pub feed_id: String,
    pub entries: Vec<CoverageEntry>,
}

/// One configured `<region>`, compiled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageEntry {
    /// The id exactly as configured, such as `065500`, `0655*`, or `SKZ012`.
    pub id: String,
    /// Lowercase `source` attribute (`eccc`, `nws`, ...), empty when absent.
    pub source: String,
    pub name: String,
    pub derive_forecast: String,
    /// The SAME code this entry adds to a header, or empty when it has none
    /// (a bare `06551*` pattern, or a non-SAME code such as an NWS zone).
    pub region_code: String,
    /// Which alert location codes the entry covers.
    pub matcher: Matcher,
    pub header: HeaderPolicy,
    /// Listed `<subregion>` codes or patterns.
    pub subregions: Vec<Matcher>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Matcher {
    pub kind: MatchKind,
    /// The full code for `exact`, or the leading digits for `prefix`.
    pub value: String,
}

impl Matcher {
    /// Whether this matcher covers a normalized location code.
    pub fn matches(&self, code: &str) -> bool {
        match self.kind {
            MatchKind::Exact => code == self.value,
            MatchKind::Prefix => crate::coverage::is_same_code(code) && code.starts_with(&self.value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchKind {
    Exact,
    Prefix,
}

/// Which affected subregions of an entry go into a SAME header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeaderPolicy {
    /// Every affected code the entry covers.
    All,
    /// Only the listed subregions; other affected codes alert under
    /// `region_code`.
    Listed,
}
