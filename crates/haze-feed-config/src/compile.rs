//! Reads feed XML and compiles each `<coverage>` region into a
//! [`CoverageEntry`]. Coverage syntax:
//!
//! - `<region id="065500"/>`: an ECCC forecast region code ending in `00`
//!   covers every `0655xx` subregion, and a header keeps every affected one.
//! - `<region id="065500">` with `<subregion>` children: the same coverage, but
//!   a header keeps only the listed subregions (plus the region code).
//! - `<region id="0655*"/>` or `<region id="06551*"/>`: an explicit prefix
//!   wildcard; a header keeps every affected code. Only a four-digit prefix adds
//!   a region code (`065500`).
//! - Any other code, such as a subregion `073110` or an NWS code, covers only
//!   itself. NWS codes never take wildcards.
//! - A listed subregion outside its region's codes adds that subregion too.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::coverage::{is_same_code, normalize_code};
use crate::model::{
    CoverageEntry, FeedConfigSnapshot, FeedCoverage, HeaderPolicy, MatchKind, Matcher,
    SNAPSHOT_VERSION,
};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read feed config {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse feed XML {path}: {message}")]
    Xml { path: PathBuf, message: String },
    #[error("feed {feed_id} defines a coverage region with no id")]
    EmptyRegionId { feed_id: String },
    #[error("feed {feed_id} has an invalid coverage code {value:?}: {reason}")]
    InvalidCode {
        feed_id: String,
        value: String,
        reason: &'static str,
    },
    #[error("feed {feed_id} is defined twice, in {first} and {second}")]
    DuplicateFeed {
        feed_id: String,
        first: PathBuf,
        second: PathBuf,
    },
}

#[derive(Debug, Default, Deserialize)]
struct FeedsXml {
    #[serde(rename = "feed", default)]
    feeds: Vec<FeedXml>,
}

#[derive(Debug, Default, Deserialize)]
struct FeedXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(default)]
    locations: LocationsXml,
}

#[derive(Debug, Default, Deserialize)]
struct LocationsXml {
    #[serde(default)]
    coverage: CoverageXml,
}

#[derive(Debug, Default, Deserialize)]
struct CoverageXml {
    #[serde(rename = "region", default)]
    regions: Vec<RegionXml>,
}

#[derive(Debug, Default, Deserialize)]
struct RegionXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(rename = "@source", default)]
    source: String,
    #[serde(rename = "@name", default)]
    name: String,
    #[serde(rename = "@derive_forecast", default)]
    derive_forecast: String,
    #[serde(rename = "subregion", default)]
    subregions: Vec<SubregionXml>,
}

#[derive(Debug, Default, Deserialize)]
struct SubregionXml {
    #[serde(rename = "@id", default)]
    id: String,
}

/// Loads a feeds directory (one `<feed>` per `*.xml` file, its id defaulting to
/// the file name) or a single `<feeds>` file, and compiles it. Any invalid feed
/// fails the whole load, so a bad edit never reaches services half-applied.
pub fn load_snapshot(path: &Path) -> Result<FeedConfigSnapshot, ConfigError> {
    let io = |source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    };
    let mut sources = Vec::<(PathBuf, String)>::new();
    if path.is_dir() {
        let mut files = fs::read_dir(path)
            .map_err(io)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(io)?;
        files.retain(|file| {
            file.is_file()
                && file
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("xml"))
        });
        files.sort();
        for file in files {
            let raw = fs::read_to_string(&file).map_err(|source| ConfigError::Io {
                path: file.clone(),
                source,
            })?;
            sources.push((file, raw));
        }
    } else {
        sources.push((path.to_path_buf(), fs::read_to_string(path).map_err(io)?));
    }

    let mut digest = Sha256::new();
    let mut feeds = BTreeMap::<String, (PathBuf, FeedCoverage)>::new();
    for (file, raw) in &sources {
        digest.update(file.file_name().map(|name| name.as_encoded_bytes()).unwrap_or_default());
        digest.update([0]);
        digest.update(raw.as_bytes());
        digest.update([0]);
        let parsed = if path.is_dir() {
            let mut feed = parse::<FeedXml>(file, raw)?;
            if feed.id.trim().is_empty() {
                feed.id = file
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or_default()
                    .to_string();
            }
            vec![feed]
        } else {
            parse::<FeedsXml>(file, raw)?.feeds
        };
        for feed in parsed {
            let coverage = compile_feed(feed)?;
            if let Some((first, _)) = feeds.get(&coverage.feed_id) {
                return Err(ConfigError::DuplicateFeed {
                    feed_id: coverage.feed_id,
                    first: first.clone(),
                    second: file.clone(),
                });
            }
            feeds.insert(coverage.feed_id.clone(), (file.clone(), coverage));
        }
    }
    let generation = digest
        .finalize()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(FeedConfigSnapshot {
        version: SNAPSHOT_VERSION,
        generation,
        feeds: feeds.into_values().map(|(_, coverage)| coverage).collect(),
    })
}

/// Compiles one `<feed>` document. Tests and the shared coverage cases use it.
pub fn compile_feed_xml(feed_id: &str, raw: &str) -> Result<FeedCoverage, ConfigError> {
    let mut feed = parse::<FeedXml>(Path::new(feed_id), raw)?;
    feed.id = feed_id.to_string();
    compile_feed(feed)
}

fn parse<T: for<'de> Deserialize<'de>>(path: &Path, raw: &str) -> Result<T, ConfigError> {
    quick_xml::de::from_str(raw).map_err(|err| ConfigError::Xml {
        path: path.to_path_buf(),
        message: err.to_string(),
    })
}

fn compile_feed(feed: FeedXml) -> Result<FeedCoverage, ConfigError> {
    let feed_id = feed.id.trim().to_string();
    let entries = feed
        .locations
        .coverage
        .regions
        .into_iter()
        .map(|region| compile_region(&feed_id, region))
        .collect::<Result<_, _>>()?;
    Ok(FeedCoverage { feed_id, entries })
}

fn compile_region(feed_id: &str, region: RegionXml) -> Result<CoverageEntry, ConfigError> {
    let id = region.id.trim().to_string();
    if id.is_empty() {
        return Err(ConfigError::EmptyRegionId {
            feed_id: feed_id.to_string(),
        });
    }
    let source = region.source.trim().to_ascii_lowercase();
    let pattern = parse_code(feed_id, &id, &source)?;
    let explicit_wildcard = pattern.kind == MatchKind::Prefix;
    let (matcher, region_code) = match pattern.kind {
        MatchKind::Prefix => {
            let code = if pattern.value.len() == 4 {
                format!("{}00", pattern.value)
            } else {
                String::new()
            };
            (pattern, code)
        }
        MatchKind::Exact if source != "nws" && is_same_code(&pattern.value) && pattern.value.ends_with("00") => {
            let code = pattern.value.clone();
            (
                Matcher {
                    kind: MatchKind::Prefix,
                    value: code[..4].to_string(),
                },
                code,
            )
        }
        MatchKind::Exact => {
            let code = if is_same_code(&pattern.value) {
                pattern.value.clone()
            } else {
                String::new()
            };
            (pattern, code)
        }
    };

    let mut subregions = Vec::new();
    for subregion in region.subregions {
        let raw = subregion.id.trim();
        if raw.is_empty() {
            continue;
        }
        // A subregion outside the region's own codes is allowed: it adds that
        // subregion to the feed, as a transmitter reaching into a neighbor does.
        subregions.push(parse_code(feed_id, raw, &source)?);
    }

    // An explicit asterisk keeps every affected subregion; a plain region code
    // keeps only its listed subregions when it has any.
    let header = if !explicit_wildcard && !subregions.is_empty() {
        HeaderPolicy::Listed
    } else {
        HeaderPolicy::All
    };
    Ok(CoverageEntry {
        id,
        source,
        name: region.name.trim().to_string(),
        derive_forecast: region.derive_forecast.trim().to_string(),
        region_code,
        matcher,
        header,
        subregions,
    })
}

/// Parses a configured code: `0655*` style prefixes (two to five digits, ECCC
/// only) or a plain code.
fn parse_code(feed_id: &str, raw: &str, source: &str) -> Result<Matcher, ConfigError> {
    let invalid = |reason| ConfigError::InvalidCode {
        feed_id: feed_id.to_string(),
        value: raw.to_string(),
        reason,
    };
    if let Some(stem) = raw.strip_suffix('*') {
        if source == "nws" {
            return Err(invalid("NWS codes cannot use a wildcard"));
        }
        if !(2..=5).contains(&stem.len()) || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid("a wildcard needs two to five digits before the *"));
        }
        return Ok(Matcher {
            kind: MatchKind::Prefix,
            value: stem.to_string(),
        });
    }
    if raw.contains('*') {
        return Err(invalid("* is only allowed at the end"));
    }
    let code = normalize_code(raw);
    if code.is_empty() {
        return Err(invalid("the code has no letters or digits"));
    }
    Ok(Matcher {
        kind: MatchKind::Exact,
        value: code,
    })
}
