//! Service settings read from `config.yaml` (`services.rust.netalert`).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::warn;

use crate::util::resolve_path;

const DEFAULT_ADDR: &str = "0.0.0.0:41802";
const DEFAULT_MAX_CLIENTS: usize = 512;
const DEFAULT_EASNET_XML: &str = "managed/configs/easnet.xml";

#[derive(Debug, Default, Deserialize)]
struct RootYaml {
    #[serde(default)]
    feeds_file: Option<String>,
    #[serde(default)]
    services: ServicesYaml,
}

#[derive(Debug, Default, Deserialize)]
struct ServicesYaml {
    #[serde(default)]
    rust: RustServicesYaml,
    #[serde(default)]
    go: GoServicesYaml,
}

#[derive(Debug, Default, Deserialize)]
struct RustServicesYaml {
    #[serde(default)]
    netalert: NetAlertYaml,
}

#[derive(Debug, Default, Deserialize)]
struct NetAlertYaml {
    #[serde(default)]
    addr: Option<String>,
    #[serde(default)]
    max_clients: Option<usize>,
    #[serde(default)]
    easnet: EasNetYaml,
}

#[derive(Debug, Default, Deserialize)]
struct EasNetYaml {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    config: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct GoServicesYaml {
    #[serde(default)]
    tts: TtsYaml,
}

#[derive(Debug, Default, Deserialize)]
struct TtsYaml {
    #[serde(default)]
    timezone: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Settings {
    /// Absolute runtime directory (the folder holding `config.yaml`).
    pub base_dir: PathBuf,
    /// WebSocket listener address.
    pub addr: String,
    pub max_clients: usize,
    /// Directory (or single file) of feed XML, used to seed banner feed IDs.
    pub feeds_path: PathBuf,
    /// Timezone used for spoken times when a client does not pick one.
    pub default_timezone: String,
    /// EAS NET XML path, present only when the legacy bridge is enabled.
    pub easnet: Option<PathBuf>,
}

impl Settings {
    pub fn load(config_path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?;
        let root: RootYaml = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse {}", config_path.display()))?;
        let base_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
        let base_dir = std::path::absolute(if base_dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            base_dir
        })?;
        let netalert = root.services.rust.netalert;
        let feeds_file = root
            .feeds_file
            .unwrap_or_else(|| "managed/feeds".to_string());
        Ok(Self {
            addr: netalert
                .addr
                .map(|addr| addr.trim().to_string())
                .filter(|addr| !addr.is_empty())
                .unwrap_or_else(|| DEFAULT_ADDR.to_string()),
            max_clients: netalert.max_clients.unwrap_or(DEFAULT_MAX_CLIENTS).max(1),
            feeds_path: resolve_path(&base_dir, Path::new(&feeds_file)),
            default_timezone: root
                .services
                .go
                .tts
                .timezone
                .map(|zone| zone.trim().to_string())
                .filter(|zone| !zone.is_empty())
                .unwrap_or_else(|| "Local".to_string()),
            easnet: netalert.easnet.enabled.then(|| {
                resolve_path(
                    &base_dir,
                    Path::new(
                        netalert
                            .easnet
                            .config
                            .as_deref()
                            .unwrap_or(DEFAULT_EASNET_XML),
                    ),
                )
            }),
            base_dir,
        })
    }

    /// Feed IDs from the feeds directory plus CGEN's feeds. The banner
    /// publishes an initial inactive state for each so a restart clears any
    /// stale on-air banner that CGEN still holds.
    pub fn known_feed_ids(&self) -> BTreeSet<String> {
        let mut ids = BTreeSet::new();
        let feed_files = if self.feeds_path.is_dir() {
            std::fs::read_dir(&self.feeds_path)
                .map(|entries| {
                    entries
                        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                        .filter(|path| {
                            path.extension()
                                .is_some_and(|ext| ext.eq_ignore_ascii_case("xml"))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        for path in feed_files {
            match feed_file_id(&path) {
                Ok(id) => {
                    ids.insert(id);
                }
                Err(err) => warn!("skipping feed {} for banner state: {err:#}", path.display()),
            }
        }
        let cgen = self.base_dir.join("managed/configs/cgen.xml");
        if let Ok(raw) = std::fs::read_to_string(&cgen) {
            match quick_xml::de::from_str::<CgenXml>(&raw) {
                Ok(parsed) => ids.extend(
                    parsed
                        .feeds
                        .into_iter()
                        .map(|feed| feed.id.trim().to_string())
                        .filter(|id| !id.is_empty()),
                ),
                Err(err) => warn!("skipping CGEN feeds for banner state: {err}"),
            }
        }
        ids
    }
}

#[derive(Debug, Deserialize)]
struct FeedXml {
    #[serde(rename = "@id", default)]
    id: String,
}

#[derive(Debug, Deserialize)]
struct CgenXml {
    #[serde(rename = "feed", default)]
    feeds: Vec<FeedXml>,
}

/// A feed's `id` attribute, or the file stem when it has none (as the Go
/// services resolve it).
fn feed_file_id(path: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(path)?;
    let feed: FeedXml = quick_xml::de::from_str(&raw)?;
    let id = feed.id.trim();
    if !id.is_empty() {
        return Ok(id.to_string());
    }
    Ok(path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default()
        .to_string())
}
