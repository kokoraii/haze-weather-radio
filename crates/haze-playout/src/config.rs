#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub(crate) struct LoadedConfig {
    pub(crate) root: RootConfig,
    pub(crate) feeds: Vec<FeedConfig>,
    pub(crate) packages: BTreeMap<String, PackageConfig>,
    pub(crate) base_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RootConfig {
    #[serde(default = "default_feeds_file")]
    pub(crate) feeds_file: String,
    #[serde(default = "default_outputs_file")]
    pub(crate) outputs_file: String,
    #[serde(default)]
    pub(crate) operator: OperatorConfig,
    #[serde(default)]
    pub(crate) playout: PlayoutConfig,
    #[serde(default)]
    pub(crate) services: ServicesConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ServicesConfig {
    #[serde(default)]
    pub(crate) rust: RustServicesConfig,
    #[serde(default)]
    pub(crate) go: GoServicesConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RustServicesConfig {
    #[serde(default)]
    pub(crate) playout: RustPlayoutConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RustPlayoutConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) ffmpeg: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct GoServicesConfig {
    #[serde(default)]
    pub(crate) product_render: ServiceToggle,
    #[serde(default)]
    pub(crate) playlist: ServiceToggle,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct ServiceToggle {
    #[serde(default)]
    pub(crate) enabled: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct OperatorConfig {
    #[serde(default)]
    pub(crate) on_air_name: serde_yaml::Value,
    #[serde(default)]
    pub(crate) operator_name: serde_yaml::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PlayoutConfig {
    #[serde(default = "default_sample_rate")]
    pub(crate) sample_rate: u32,
    #[serde(default = "default_channels")]
    pub(crate) channels: u16,
    #[serde(default)]
    pub(crate) playlist_order: Vec<String>,
    #[serde(default)]
    pub(crate) pacing: PacingConfig,
}

impl Default for PlayoutConfig {
    fn default() -> Self {
        Self {
            sample_rate: default_sample_rate(),
            channels: default_channels(),
            playlist_order: Vec::new(),
            pacing: PacingConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PacingConfig {
    #[serde(default = "default_gap_seconds")]
    pub(crate) package_gap_s: f64,
}

impl Default for PacingConfig {
    fn default() -> Self {
        Self {
            package_gap_s: default_gap_seconds(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FeedsXml {
    #[serde(rename = "feed", default)]
    feeds: Vec<FeedConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct OutputsXml {
    #[serde(rename = "feed", default)]
    feeds: Vec<FeedOutputConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FeedOutputConfig {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(default)]
    webrtc: OutputNodeConfig,
    #[serde(default)]
    icecast: OutputNodeConfig,
    #[serde(default)]
    stream: OutputNodeConfig,
    #[serde(default)]
    udp: OutputNodeConfig,
    #[serde(default)]
    rtp: OutputNodeConfig,
    #[serde(default)]
    rtmp: OutputNodeConfig,
    #[serde(default)]
    srt: OutputNodeConfig,
    #[serde(default)]
    rtsp: OutputNodeConfig,
    #[serde(default)]
    audio_device: OutputNodeConfig,
    #[serde(default)]
    file: OutputNodeConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct FeedConfig {
    #[serde(rename = "@id", default)]
    pub(crate) id: String,
    #[serde(rename = "@enabled", default)]
    pub(crate) enabled: Option<String>,
    #[serde(rename = "@timezone", default)]
    pub(crate) timezone: String,
    #[serde(default)]
    pub(crate) playout: FeedPlayoutConfig,
    #[serde(default)]
    pub(crate) alerts: Option<FeedAlertsConfig>,
    #[serde(default)]
    pub(crate) locations: FeedLocationsConfig,
    #[serde(default)]
    pub(crate) languages: LanguagesConfig,
    #[serde(default)]
    pub(crate) description: DescriptionConfig,
    #[serde(default)]
    pub(crate) transmitter_metadata: TransmitterMetadataConfig,
    #[serde(skip)]
    pub(crate) output: OutputConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct FeedPlayoutConfig {
    #[serde(rename = "@routine", default)]
    pub(crate) routine: Option<String>,
    #[serde(rename = "@same", default)]
    pub(crate) same: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct FeedAlertsConfig {
    #[serde(default)]
    pub(crate) cap_cp: FeedAlertProviderConfig,
    #[serde(default)]
    pub(crate) nws_cap: FeedAlertProviderConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct FeedAlertProviderConfig {
    #[serde(rename = "@enabled", default)]
    pub(crate) enabled: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct FeedLocationsConfig {
    #[serde(default)]
    pub(crate) coverage: FeedCoverageConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct FeedCoverageConfig {
    #[serde(rename = "region", default)]
    pub(crate) regions: Vec<FeedCoverageRegionConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct FeedCoverageRegionConfig {
    #[serde(rename = "@id", default)]
    pub(crate) id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct LanguagesConfig {
    #[serde(rename = "lang", default)]
    pub(crate) langs: Vec<LanguageConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct LanguageConfig {
    #[serde(rename = "@code", default)]
    pub(crate) code: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct DescriptionConfig {
    #[serde(rename = "lang", default)]
    pub(crate) langs: Vec<DescriptionLangConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct DescriptionLangConfig {
    #[serde(rename = "@code", default)]
    pub(crate) code: String,
    #[serde(rename = "@text", default)]
    pub(crate) text: String,
    #[serde(rename = "@suffix", default)]
    pub(crate) suffix: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct TransmitterMetadataConfig {
    #[serde(rename = "transmitter", default)]
    pub(crate) transmitters: Vec<TransmitterConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct TransmitterConfig {
    #[serde(default)]
    pub(crate) site_name: String,
    #[serde(default)]
    pub(crate) callsign: String,
    #[serde(default)]
    pub(crate) relationship: String,
    #[serde(default)]
    pub(crate) host_name: String,
    #[serde(default)]
    pub(crate) frequency_mhz: TransmitterFrequencyConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct TransmitterFrequencyConfig {
    #[serde(rename = "@gpclk", default)]
    pub(crate) gpclk: String,
    #[serde(rename = "@gpio", default)]
    pub(crate) gpio: String,
    #[serde(rename = "$text", default)]
    pub(crate) value: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct OutputConfig {
    #[serde(default)]
    pub(crate) webrtc: OutputNodeConfig,
    #[serde(default)]
    pub(crate) icecast: OutputNodeConfig,
    #[serde(default)]
    pub(crate) udp: OutputNodeConfig,
    #[serde(default)]
    pub(crate) rtp: OutputNodeConfig,
    #[serde(default)]
    pub(crate) rtmp: OutputNodeConfig,
    #[serde(default)]
    pub(crate) srt: OutputNodeConfig,
    #[serde(default)]
    pub(crate) rtsp: OutputNodeConfig,
    #[serde(default)]
    pub(crate) audio_device: OutputNodeConfig,
    #[serde(default)]
    pub(crate) file: OutputNodeConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct OutputNodeConfig {
    #[serde(rename = "@enabled", default)]
    pub(crate) enabled: Option<String>,
    #[serde(default)]
    pub(crate) r#type: String,
    #[serde(default)]
    pub(crate) ip: String,
    #[serde(default)]
    pub(crate) port: String,
    #[serde(default)]
    pub(crate) host: String,
    #[serde(default)]
    pub(crate) url: String,
    #[serde(default)]
    pub(crate) path: String,
    #[serde(default)]
    pub(crate) format: String,
    #[serde(default)]
    pub(crate) acodec: String,
    #[serde(default)]
    pub(crate) bitrate_kbps: String,
    #[serde(default)]
    pub(crate) device: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PackagesXml {
    #[serde(default)]
    defaults: PackageDefaultsXml,
    #[serde(rename = "package", default)]
    packages: Vec<PackageXml>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ProductsXml {
    #[serde(default)]
    defaults: PackageDefaultsXml,
    #[serde(rename = "product", default)]
    products: Vec<ProductXml>,
    #[serde(rename = "package", default)]
    packages: Vec<ProductXml>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PackageDefaultsXml {
    #[serde(default)]
    enabled: String,
    #[serde(default)]
    reader_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct PackageXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(rename = "@enabled", default)]
    enabled: Option<String>,
    #[serde(default)]
    reader_id: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ProductXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(rename = "@enabled", default)]
    enabled: Option<String>,
    #[serde(rename = "@reader_id", default)]
    reader_id_attr: String,
    #[serde(rename = "@readerid", default)]
    readerid_attr: String,
    #[serde(default)]
    reader_id: String,
    #[serde(rename = "lang", default)]
    langs: Vec<ProductLangXml>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ProductLangXml {
    #[serde(rename = "@reader_id", default)]
    reader_id_attr: String,
    #[serde(rename = "@readerid", default)]
    readerid_attr: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PackageConfig {
    pub(crate) enabled: bool,
    pub(crate) reader_id: String,
}

pub(crate) fn load_config(config_path: &Path) -> Result<LoadedConfig> {
    let config_path = dunce::simplified(config_path).to_path_buf();
    let raw = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let raw = expand_env_vars(&raw);
    let mut root: RootConfig = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse config {}", config_path.display()))?;
    if root.playout.sample_rate == 0 {
        root.playout.sample_rate = default_sample_rate();
    }
    if root.playout.channels == 0 {
        root.playout.channels = default_channels();
    }
    if root.playout.playlist_order.is_empty() {
        root.playout.playlist_order = vec![
            "current_conditions".to_string(),
            "air_quality".to_string(),
            "forecast".to_string(),
            "climate_summary".to_string(),
            "geophysical_alert".to_string(),
            "user_bulletin".to_string(),
        ];
    }

    let base_dir = config_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let feeds_path = resolve_path(&base_dir, &root.feeds_file);
    let outputs_path = resolve_path(&base_dir, &root.outputs_file);
    let mut feeds = load_feeds(&feeds_path)?;
    let outputs = load_outputs(&outputs_path)?;
    for feed in &mut feeds {
        feed.output = outputs.output_for_feed(feed.id.trim()).unwrap_or_default();
    }
    let packages = load_package_catalog(&base_dir)?;
    Ok(LoadedConfig {
        root,
        feeds,
        packages,
        base_dir,
    })
}

fn load_package_catalog(base_dir: &Path) -> Result<BTreeMap<String, PackageConfig>> {
    let products_path = resolve_path(base_dir, "managed/configs/products.xml");
    match load_products(&products_path) {
        Ok(packages) => Ok(packages),
        Err(err) => {
            if err
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
            {
                load_packages(&resolve_path(base_dir, "managed/configs/packages.xml"))
            } else {
                Err(err)
            }
        }
    }
}

fn load_feeds(path: &Path) -> Result<Vec<FeedConfig>> {
	if path.is_dir() {
		let mut entries = fs::read_dir(path)
			.with_context(|| format!("failed to read feeds directory {}", path.display()))?
			.collect::<Result<Vec<_>, _>>()?;
		entries.sort_by_key(|entry| entry.file_name());
		let mut feeds = Vec::new();
		for entry in entries {
			let file = entry.path();
			if file.extension().and_then(|ext| ext.to_str()) != Some("xml") { continue; }
			let raw = expand_env_vars(&fs::read_to_string(&file)
				.with_context(|| format!("failed to read feed XML {}", file.display()))?);
			let mut feed: FeedConfig = quick_xml::de::from_str(&raw)
				.with_context(|| format!("failed to parse feed XML {}", file.display()))?;
			if feed.id.trim().is_empty() {
				feed.id = file.file_stem().and_then(|stem| stem.to_str()).unwrap_or_default().to_string();
			}
			feeds.push(feed);
		}
		return Ok(feeds);
	}
	let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read feeds XML {}", path.display()))?;
    let raw = expand_env_vars(&raw);
    let parsed: FeedsXml = quick_xml::de::from_str(&raw)
        .with_context(|| format!("failed to parse feeds XML {}", path.display()))?;
    Ok(parsed.feeds)
}

#[derive(Debug, Clone, Default)]
struct OutputCatalog {
    exact: BTreeMap<String, OutputConfig>,
    wildcard: Vec<(String, OutputConfig)>,
}

impl OutputCatalog {
    fn output_for_feed(&self, feed_id: &str) -> Option<OutputConfig> {
        self.exact
            .get(feed_id)
            .cloned()
            .or_else(|| wildcard_output_for(feed_id, &self.wildcard))
    }
}

fn load_outputs(path: &Path) -> Result<OutputCatalog> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read outputs XML {}", path.display()))?;
    let raw = expand_env_vars(&raw);
    let parsed: OutputsXml = quick_xml::de::from_str(&raw)
        .with_context(|| format!("failed to parse outputs XML {}", path.display()))?;
    let mut catalog = OutputCatalog::default();
    for feed in parsed.feeds {
        let id = feed.id.trim();
        if id.is_empty() {
            continue;
        }
        let config = OutputConfig {
            webrtc: feed.webrtc,
            icecast: preferred_icecast_node(feed.icecast, feed.stream),
            udp: feed.udp,
            rtp: feed.rtp,
            rtmp: feed.rtmp,
            srt: feed.srt,
            rtsp: feed.rtsp,
            audio_device: feed.audio_device,
            file: feed.file,
        };
        if output_id_is_wildcard(id) {
            catalog.wildcard.push((id.to_string(), config));
        } else {
            catalog.exact.insert(id.to_string(), config);
        }
    }
    Ok(catalog)
}

fn preferred_icecast_node(icecast: OutputNodeConfig, stream: OutputNodeConfig) -> OutputNodeConfig {
    if icecast.is_configured() {
        icecast
    } else {
        stream
    }
}

fn output_id_is_wildcard(pattern: &str) -> bool {
    pattern.contains('*') || pattern.contains('?')
}

fn wildcard_output_for(feed_id: &str, entries: &[(String, OutputConfig)]) -> Option<OutputConfig> {
    entries
        .iter()
        .find(|(pattern, _)| wildcard_match(pattern, feed_id))
        .map(|(_, config)| config.clone())
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    wildcard_match_inner(pattern.as_bytes(), value.as_bytes())
}

fn wildcard_match_inner(pattern: &[u8], value: &[u8]) -> bool {
    match pattern.split_first() {
        None => value.is_empty(),
        Some((&b'*', rest)) => {
            wildcard_match_inner(rest, value)
                || (!value.is_empty() && wildcard_match_inner(pattern, &value[1..]))
        }
        Some((&b'?', rest)) => !value.is_empty() && wildcard_match_inner(rest, &value[1..]),
        Some((&literal, rest)) => value
            .split_first()
            .is_some_and(|(&head, tail)| head == literal && wildcard_match_inner(rest, tail)),
    }
}

fn load_packages(path: &Path) -> Result<BTreeMap<String, PackageConfig>> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", path.display())),
    };
    let raw = expand_env_vars(&raw);
    let parsed: PackagesXml = quick_xml::de::from_str(&raw)
        .with_context(|| format!("failed to parse packages XML {}", path.display()))?;
    let default_enabled = xml_bool(&parsed.defaults.enabled, true);
    let default_reader = parsed.defaults.reader_id.trim().to_string();
    let mut packages = BTreeMap::new();
    for package in parsed.packages {
        let id = package.id.trim();
        if id.is_empty() {
            continue;
        }
        let reader = if package.reader_id.trim().is_empty() {
            default_reader.clone()
        } else {
            package.reader_id.trim().to_string()
        };
        packages.insert(
            id.to_string(),
            PackageConfig {
                enabled: package
                    .enabled
                    .as_deref()
                    .map(|raw| xml_bool(raw, default_enabled))
                    .unwrap_or(default_enabled),
                reader_id: reader,
            },
        );
    }
    Ok(packages)
}

fn load_products(path: &Path) -> Result<BTreeMap<String, PackageConfig>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read products XML {}", path.display()))?;
    let raw = expand_env_vars(&raw);
    let parsed: ProductsXml = quick_xml::de::from_str(&raw)
        .with_context(|| format!("failed to parse products XML {}", path.display()))?;
    let default_enabled = xml_bool(&parsed.defaults.enabled, true);
    let default_reader = parsed.defaults.reader_id.trim().to_string();
    let mut packages = BTreeMap::new();
    for package in parsed.products.into_iter().chain(parsed.packages) {
        let id = package.id.trim();
        if id.is_empty() {
            continue;
        }
        let reader = first_non_blank([
            package.reader_id_attr.as_str(),
            package.readerid_attr.as_str(),
            package.reader_id.as_str(),
        ]);
        let reader = if reader.is_empty() {
            package
                .langs
                .iter()
                .map(|lang| {
                    first_non_blank([lang.reader_id_attr.as_str(), lang.readerid_attr.as_str()])
                })
                .find(|reader| !reader.is_empty())
                .unwrap_or("")
        } else {
            reader
        };
        let reader = if reader.is_empty() {
            default_reader.as_str()
        } else {
            reader
        };
        packages.insert(
            id.to_string(),
            PackageConfig {
                enabled: package
                    .enabled
                    .as_deref()
                    .map(|raw| xml_bool(raw, default_enabled))
                    .unwrap_or(default_enabled),
                reader_id: reader.to_string(),
            },
        );
    }
    Ok(packages)
}

fn first_non_blank<const N: usize>(values: [&str; N]) -> &str {
    for value in values {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    ""
}

fn expand_env_vars(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '$' {
            out.push(ch);
            continue;
        }
        if chars.peek() == Some(&'{') {
            chars.next();
            let mut name = String::new();
            for next in chars.by_ref() {
                if next == '}' {
                    break;
                }
                name.push(next);
            }
            out.push_str(&std::env::var(name).unwrap_or_default());
            continue;
        }
        let mut name = String::new();
        while let Some(next) = chars.peek().copied() {
            if next == '_' || next.is_ascii_alphanumeric() {
                name.push(next);
                chars.next();
            } else {
                break;
            }
        }
        if name.is_empty() {
            out.push('$');
        } else {
            out.push_str(&std::env::var(name).unwrap_or_default());
        }
    }
    out
}

impl LoadedConfig {
    pub(crate) fn enabled_feeds(&self) -> impl Iterator<Item = &FeedConfig> {
        self.feeds
            .iter()
            .filter(|feed| !feed.id.trim().is_empty() && feed.is_enabled())
    }

    pub(crate) fn package_enabled(&self, package_id: &str) -> bool {
        self.packages
            .get(package_id)
            .map(|package| package.enabled)
            .unwrap_or(true)
    }

    pub(crate) fn reader_id(&self, package_id: &str) -> String {
        self.packages
            .get(package_id)
            .map(|package| package.reader_id.trim().to_string())
            .unwrap_or_default()
    }

    pub(crate) fn routine_playlist_order(&self) -> Vec<String> {
        let order: Vec<String> = self
            .root
            .playout
            .playlist_order
            .iter()
            .map(|raw| raw.trim())
            .filter(|id| !id.is_empty() && *id != "alerts" && self.package_enabled(id))
            .map(str::to_string)
            .collect();
        if order.is_empty() {
            vec!["current_conditions".to_string(), "forecast".to_string()]
        } else {
            order
        }
    }
}

impl FeedConfig {
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
            .as_deref()
            .map(|raw| xml_bool(raw, true))
            .unwrap_or(true)
    }

    pub(crate) fn routine_enabled(&self) -> bool {
        self.playout
            .routine
            .as_deref()
            .map(|raw| xml_bool(raw, true))
            .unwrap_or(true)
    }

    pub(crate) fn same_enabled(&self) -> bool {
        self.playout
            .same
            .as_deref()
            .map(|raw| xml_bool(raw, true))
            .unwrap_or(true)
    }

    pub(crate) fn alert_covers_all_locations(&self) -> bool {
        if self
            .locations
            .coverage
            .regions
            .iter()
            .any(|region| !region.id.trim().is_empty())
        {
            return false;
        }
        let Some(alerts) = &self.alerts else {
            return false;
        };
        alerts
            .cap_cp
            .enabled
            .as_deref()
            .map(|raw| xml_bool(raw, true))
            .unwrap_or(true)
            || alerts
                .nws_cap
                .enabled
                .as_deref()
                .map(|raw| xml_bool(raw, false))
                .unwrap_or(false)
    }

    pub(crate) fn language(&self) -> String {
        self.languages
            .langs
            .iter()
            .find_map(|lang| {
                let code = lang.code.trim();
                (!code.is_empty()).then(|| code.to_string())
            })
            .unwrap_or_else(|| "en-US".to_string())
    }

    pub(crate) fn site_name(&self) -> String {
        self.station_transmitter()
            .map(|transmitter| fallback_text(&transmitter.site_name, &self.id))
            .unwrap_or_else(|| self.id.clone())
    }

    pub(crate) fn station_callsign(&self) -> String {
        self.station_transmitter()
            .map(|transmitter| transmitter.callsign.trim().to_string())
            .unwrap_or_default()
    }

    pub(crate) fn station_frequency_mhz(&self) -> String {
        self.station_transmitter()
            .map(|transmitter| transmitter.frequency_mhz.value.trim().to_string())
            .unwrap_or_default()
    }

    fn station_transmitter(&self) -> Option<&TransmitterConfig> {
        let transmitters: Vec<&TransmitterConfig> = self
            .transmitter_metadata
            .transmitters
            .iter()
            .filter(|transmitter| !transmitter.is_empty())
            .collect();
        transmitters
            .iter()
            .copied()
            .find(|transmitter| transmitter.is_relationship("primary"))
            .or_else(|| {
                transmitters.iter().copied().find(|transmitter| {
                    !transmitter.is_relationship("replaces")
                        && !transmitter.is_relationship("ip")
                        && (!transmitter.callsign.trim().is_empty()
                            || !transmitter.site_name.trim().is_empty())
                })
            })
            .or_else(|| {
                transmitters.iter().copied().find(|transmitter| {
                    !transmitter.callsign.trim().is_empty()
                        || !transmitter.site_name.trim().is_empty()
                })
            })
            .or_else(|| transmitters.first().copied())
    }

    pub(crate) fn replacement_transmitter(&self) -> Option<&TransmitterConfig> {
        self.transmitter_metadata
            .transmitters
            .iter()
            .find(|transmitter| !transmitter.is_empty() && transmitter.is_relationship("replaces"))
    }
}

impl TransmitterConfig {
    fn is_empty(&self) -> bool {
        self.site_name.trim().is_empty()
            && self.callsign.trim().is_empty()
            && self.relationship.trim().is_empty()
            && self.host_name.trim().is_empty()
            && self.frequency_mhz.value.trim().is_empty()
    }

    fn relationship(&self) -> String {
        let relationship = self.relationship.trim().to_ascii_lowercase();
        if relationship == "secondary/repeater" {
            return "secondary".to_string();
        }
        if relationship.is_empty() {
            return "unknown".to_string();
        }
        relationship
    }

    fn is_relationship(&self, relationship: &str) -> bool {
        let current = self.relationship();
        let wanted = relationship.trim().to_ascii_lowercase();
        current == wanted || (wanted == "repeater" && current == "secondary")
    }
}

impl OutputNodeConfig {
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
            .as_deref()
            .map(|raw| xml_bool(raw, false))
            .unwrap_or(false)
    }

    fn is_configured(&self) -> bool {
        self.enabled.is_some()
            || !self.r#type.trim().is_empty()
            || !self.ip.trim().is_empty()
            || !self.port.trim().is_empty()
            || !self.host.trim().is_empty()
            || !self.url.trim().is_empty()
            || !self.path.trim().is_empty()
            || !self.format.trim().is_empty()
            || !self.acodec.trim().is_empty()
            || !self.bitrate_kbps.trim().is_empty()
            || !self.device.trim().is_empty()
    }
}

pub(crate) fn xml_bool(raw: &str, fallback: bool) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => fallback,
        "1" | "true" | "yes" | "on" | "enabled" => true,
        "0" | "false" | "no" | "off" | "disabled" => false,
        _ => fallback,
    }
}

pub(crate) fn resolve_path(base: &Path, value: &str) -> PathBuf {
    let value = value.trim();
    if value.is_empty() {
        return base.to_path_buf();
    }
    let path = Path::new(value);
    if path.is_absolute() {
        dunce::simplified(path).to_path_buf()
    } else {
        dunce::simplified(&base.join(path)).to_path_buf()
    }
}

pub(crate) fn display_text(value: &serde_yaml::Value) -> String {
    match value {
        serde_yaml::Value::String(text) => text.trim().to_string(),
        serde_yaml::Value::Sequence(items) => items
            .iter()
            .map(display_text)
            .find(|text| !text.is_empty())
            .unwrap_or_default(),
        serde_yaml::Value::Mapping(map) => ["text", "name", "value", "address"]
            .iter()
            .find_map(|key| {
                map.get(serde_yaml::Value::String((*key).to_string()))
                    .map(display_text)
                    .filter(|text| !text.is_empty())
            })
            .unwrap_or_default(),
        _ => String::new(),
    }
}

pub(crate) fn fallback_text(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.trim().to_string()
    } else {
        value.to_string()
    }
}

fn default_feeds_file() -> String {
    "managed/feeds".to_string()
}

fn default_outputs_file() -> String {
    "managed/configs/output.xml".to_string()
}

fn default_sample_rate() -> u32 {
    48_000
}

fn default_channels() -> u16 {
    1
}

fn default_gap_seconds() -> f64 {
    1.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_feed_attributes() {
        let raw = r#"
<feeds>
  <feed id="sk-0001" enabled="true" timezone="America/Regina">
    <playout routine="true" same="false"/>
    <languages><lang code="en-US"/></languages>
    <transmitter_metadata>
      <transmitter><site_name>Saskatoon</site_name><relationship>primary</relationship><frequency_mhz>162.550</frequency_mhz></transmitter>
      <transmitter><site_name>Saskatoon</site_name><relationship>fm</relationship><frequency_mhz gpclk="2" gpio="6">87.900</frequency_mhz></transmitter>
      <transmitter><site_name>Saskatoon</site_name><callsign>XLF322</callsign><relationship>replaces</relationship><frequency_mhz>162.550</frequency_mhz></transmitter>
    </transmitter_metadata>
  </feed>
</feeds>
"#;
        let parsed: FeedsXml = quick_xml::de::from_str(raw).expect("feeds XML");
        assert_eq!(parsed.feeds.len(), 1);
        let feed = &parsed.feeds[0];
        assert_eq!(feed.id, "sk-0001");
        assert!(feed.is_enabled());
        assert!(feed.routine_enabled());
        assert!(!feed.same_enabled());
        assert_eq!(feed.language(), "en-US");
        assert_eq!(feed.site_name(), "Saskatoon");
        assert_eq!(feed.station_callsign(), "");
        assert_eq!(feed.station_frequency_mhz(), "162.550");
        assert!(!feed.alert_covers_all_locations());
        assert_eq!(feed.transmitter_metadata.transmitters.len(), 3);
        assert_eq!(
            feed.replacement_transmitter()
                .map(|transmitter| transmitter.callsign.as_str()),
            Some("XLF322")
        );
        assert_eq!(
            feed.transmitter_metadata.transmitters[1]
                .frequency_mhz
                .gpclk,
            "2"
        );
        assert_eq!(
            feed.transmitter_metadata.transmitters[1].frequency_mhz.gpio,
            "6"
        );
    }

    #[test]
    fn parses_all_location_alert_relay_feed() {
        let raw = r#"
<feeds>
  <feed id="CAP-IT-ALL" enabled="true">
    <playout routine="false" same="true"/>
    <alerts>
      <cap_cp enabled="true"/>
      <nws_cap enabled="true"/>
    </alerts>
    <locations><coverage/></locations>
  </feed>
</feeds>
"#;
        let parsed: FeedsXml = quick_xml::de::from_str(raw).expect("feeds XML");
        let feed = &parsed.feeds[0];

        assert!(feed.same_enabled());
        assert!(feed.alert_covers_all_locations());
    }

    #[test]
    fn parses_outputs_by_feed_id() {
        let raw = r#"
<outputs>
  <feed id="sk-0001">
    <udp enabled="true"><ip>127.0.0.1</ip><format>raw</format></udp>
  </feed>
</outputs>
"#;
        let parsed: OutputsXml = quick_xml::de::from_str(raw).expect("outputs XML");
        assert_eq!(parsed.feeds.len(), 1);
        assert_eq!(parsed.feeds[0].id, "sk-0001");
        assert!(parsed.feeds[0].udp.is_enabled());
    }

    #[test]
    fn output_catalog_matches_wildcard_feed_ids() {
        let dir = std::env::temp_dir().join(format!(
            "haze-playout-output-wildcard-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("output.xml");
        std::fs::write(
            &path,
            r#"
<outputs>
  <feed id="cwxr-*">
    <icecast enabled="true"><host>icecast.example.test</host></icecast>
  </feed>
  <feed id="cwxr-sk01">
    <udp enabled="true"><ip>127.0.0.1</ip></udp>
  </feed>
</outputs>
"#,
        )
        .expect("outputs xml");

        let catalog = load_outputs(&path).expect("load outputs");
        assert!(catalog
            .output_for_feed("cwxr-ab01")
            .unwrap()
            .icecast
            .is_enabled());
        assert!(catalog
            .output_for_feed("cwxr-sk01")
            .unwrap()
            .udp
            .is_enabled());
        assert!(catalog.output_for_feed("sk-0001").is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_stream_alias_loads_as_icecast_output() {
        let parsed: OutputsXml = quick_xml::de::from_str(
            r#"
<outputs>
  <feed id="sk-0001">
    <stream enabled="true"><type>icecast</type><host>icecast.example.test</host></stream>
  </feed>
</outputs>
"#,
        )
        .expect("outputs XML");
        let config = preferred_icecast_node(
            parsed.feeds[0].icecast.clone(),
            parsed.feeds[0].stream.clone(),
        );
        assert!(config.is_enabled());
        assert_eq!(config.host, "icecast.example.test");
    }

    #[test]
    fn loads_products_catalog_with_lang_reader_ids() {
        let dir =
            std::env::temp_dir().join(format!("haze-playout-products-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("products.xml");
        std::fs::write(
            &path,
            r#"
<ProductText version="1.1">
  <defaults><enabled>true</enabled><reader_id>00</reader_id></defaults>
  <product id="forecast" enabled="true">
    <lang iso="en" readerid="01"><text key="opener">Forecast.</text></lang>
  </product>
  <product id="alerts" enabled="false">
    <lang iso="en" readerid="02"><text key="opener">Alerts.</text></lang>
  </product>
</ProductText>
"#,
        )
        .expect("products xml");

        let packages = load_products(&path).expect("load products");
        assert_eq!(packages["forecast"].reader_id, "01");
        assert!(packages["forecast"].enabled);
        assert_eq!(packages["alerts"].reader_id, "02");
        assert!(!packages["alerts"].enabled);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
