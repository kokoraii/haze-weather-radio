//! The one parser for managed feed config.
//!
//! The daemon loads `managed/feeds` with [`load_snapshot`], validates it, and
//! publishes the resulting [`FeedConfigSnapshot`] on the host event bridge
//! (`config.feeds.snapshot` replies and `config.feeds.updated` broadcasts; see
//! `docs/feed-config.md`). Services never parse coverage XML themselves: they
//! read the snapshot and evaluate it with the rules in [`coverage`]. The Go
//! package `services/go/internal/feedconfig` implements the same rules, and both
//! run the shared cases in `contracts/feedconfig/v1/coverage-cases.json`.

mod compile;
pub mod coverage;
mod model;

pub use compile::{compile_feed_xml, load_snapshot, ConfigError};
pub use coverage::{normalize_code, SameHeader, SAME_MAX_LOCATIONS};
pub use model::{
    CoverageEntry, FeedConfigSnapshot, FeedCoverage, HeaderPolicy, MatchKind, Matcher,
    SNAPSHOT_VERSION,
};
