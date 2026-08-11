//! Top-level YAML and operator-managed XML configuration loading.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::warn;

#[derive(Clone, Debug)]
pub struct ServiceConfig {
    pub rollout_mode: String,
    pub workers: usize,
    pub queue_size: usize,
    pub default_limit: usize,
    pub maximum_limit: usize,
    pub packs: Vec<PackConfig>,
    pub geometry_packs: Vec<GeometryPackConfig>,
    pub overlay_path: PathBuf,
    pub allowed_overlay_sources: Vec<String>,
    pub feed_bindings_path: Option<PathBuf>,
    pub force_groups: Vec<Vec<String>>,
    pub never_groups: Vec<(String, String)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackFormat {
    Normalized,
    Legacy,
}

#[derive(Clone, Debug)]
pub struct PackConfig {
    pub id: String,
    pub path: PathBuf,
    pub priority: i32,
    pub format: PackFormat,
    pub required: bool,
}

#[derive(Clone, Debug)]
pub struct GeometryPackConfig {
    pub id: String,
    pub path: PathBuf,
    pub core_id: String,
    pub priority: i32,
    pub format: PackFormat,
    pub required: bool,
}

#[derive(Debug, Default, Deserialize)]
struct RootConfig {
    services: Option<ServicesConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct ServicesConfig {
    rust: Option<RustServicesConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RustServicesConfig {
    location: Option<LocationYamlConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct LocationYamlConfig {
    #[serde(default)]
    config: Option<PathBuf>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    workers: Option<usize>,
    #[serde(default)]
    queue_size: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename = "locations")]
struct LocationsXml {
    #[serde(rename = "catalogs", default)]
    catalogs: CatalogsXml,
    #[serde(rename = "overlay", default)]
    overlay: OverlayXml,
    #[serde(rename = "query", default)]
    query: QueryXml,
    #[serde(rename = "feedBindings", default)]
    feed_bindings: FeedBindingsXml,
    #[serde(rename = "grouping", default)]
    grouping: GroupingXml,
}

#[derive(Debug, Default, Deserialize)]
struct CatalogsXml {
    #[serde(rename = "pack", default)]
    packs: Vec<PackXml>,
    #[serde(rename = "legacy", default)]
    legacy: Vec<PackXml>,
    #[serde(rename = "geometry", default)]
    geometry: Vec<GeometryPackXml>,
}

#[derive(Debug, Default, Deserialize)]
struct PackXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(rename = "@path", default)]
    path: PathBuf,
    #[serde(rename = "@priority", default)]
    priority: i32,
    #[serde(rename = "@enabled", default = "default_true")]
    enabled: bool,
    #[serde(rename = "@required", default)]
    required: bool,
    #[serde(rename = "@format", default)]
    format: String,
}

#[derive(Debug, Default, Deserialize)]
struct GeometryPackXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(rename = "@path", default)]
    path: PathBuf,
    #[serde(rename = "@core", default)]
    core: String,
    #[serde(rename = "@priority", default)]
    priority: i32,
    #[serde(rename = "@enabled", default = "default_true")]
    enabled: bool,
    #[serde(rename = "@required", default)]
    required: bool,
    #[serde(rename = "@format", default)]
    format: String,
}

#[derive(Debug, Default, Deserialize)]
struct OverlayXml {
    #[serde(rename = "@path", default)]
    path: PathBuf,
    #[serde(rename = "source", default)]
    sources: Vec<SourceXml>,
}

#[derive(Debug, Default, Deserialize)]
struct SourceXml {
    #[serde(rename = "@id", default)]
    id: String,
}

#[derive(Debug, Default, Deserialize)]
struct QueryXml {
    #[serde(rename = "@defaultLimit", default)]
    default_limit: usize,
    #[serde(rename = "@maximumLimit", default)]
    maximum_limit: usize,
}

#[derive(Debug, Default, Deserialize)]
struct FeedBindingsXml {
    #[serde(rename = "@path", default)]
    path: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
struct GroupingXml {
    #[serde(rename = "force", default)]
    force: Vec<GroupXml>,
    #[serde(rename = "never", default)]
    never: Vec<GroupXml>,
}

#[derive(Debug, Default, Deserialize)]
struct GroupXml {
    #[serde(rename = "@members", default)]
    members: String,
}

const fn default_true() -> bool {
    true
}

pub fn load(root_config_path: &Path, locations_override: Option<&Path>) -> Result<ServiceConfig> {
    let raw = fs::read_to_string(root_config_path)
        .with_context(|| format!("failed to read {}", root_config_path.display()))?;
    let root: RootConfig = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse {}", root_config_path.display()))?;
    let yaml = root
        .services
        .and_then(|services| services.rust)
        .and_then(|rust| rust.location)
        .unwrap_or_default();
    let root_dir = root_config_path.parent().unwrap_or_else(|| Path::new("."));
    let locations_path = locations_override
        .map(Path::to_path_buf)
        .or(yaml.config)
        .unwrap_or_else(|| PathBuf::from("managed/configs/locations.xml"));
    let locations_path = resolve_path(root_dir, &locations_path);
    let xml_raw = fs::read_to_string(&locations_path)
        .with_context(|| format!("failed to read {}", locations_path.display()))?;
    let xml: LocationsXml = quick_xml::de::from_str(&xml_raw)
        .with_context(|| format!("failed to parse {}", locations_path.display()))?;

    let mut packs = Vec::new();
    let mut declared_pack_ids = std::collections::HashSet::new();
    for (legacy, pack) in xml
        .catalogs
        .packs
        .into_iter()
        .map(|pack| (false, pack))
        .chain(xml.catalogs.legacy.into_iter().map(|pack| (true, pack)))
    {
        if !pack.enabled {
            continue;
        }
        let id = pack_id(&pack.id, &pack.path, "location-pack");
        if !declared_pack_ids.insert(id.clone()) {
            anyhow::bail!("duplicate location pack id: {id}");
        }
        let path = resolve_path(root_dir, &pack.path);
        if !path.exists() {
            if pack.required {
                anyhow::bail!("required location pack is missing: {}", path.display());
            }
            warn!(path = %path.display(), "optional location pack is not installed");
            continue;
        }
        let format = if legacy || pack.format.eq_ignore_ascii_case("legacy") {
            PackFormat::Legacy
        } else {
            PackFormat::Normalized
        };
        packs.push(PackConfig {
            id,
            path,
            priority: pack.priority,
            format,
            required: pack.required,
        });
    }
    packs.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.id.cmp(&right.id))
    });

    let loaded_pack_ids: std::collections::HashSet<_> =
        packs.iter().map(|pack| pack.id.as_str()).collect();
    let mut geometry_packs = Vec::new();
    let mut all_ids = declared_pack_ids.clone();
    for pack in xml.catalogs.geometry {
        if !pack.enabled {
            continue;
        }
        let id = pack_id(&pack.id, &pack.path, "location-geometry");
        if !all_ids.insert(id.clone()) {
            anyhow::bail!("duplicate location pack id: {id}");
        }
        let core_id = pack.core.trim().to_string();
        if core_id.is_empty() {
            anyhow::bail!("geometry pack {id} is missing its core pack id");
        }
        if !declared_pack_ids.contains(&core_id) {
            anyhow::bail!("geometry pack {id} references unknown core pack {core_id}");
        }
        if !loaded_pack_ids.contains(core_id.as_str()) {
            if pack.required {
                anyhow::bail!("geometry pack {id} requires unavailable core pack {core_id}");
            }
            warn!(geometry_pack = %id, core_pack = %core_id, "optional geometry pack core is not installed");
            continue;
        }
        let path = resolve_path(root_dir, &pack.path);
        if !path.exists() {
            if pack.required {
                anyhow::bail!(
                    "required location geometry pack is missing: {}",
                    path.display()
                );
            }
            warn!(path = %path.display(), "optional location geometry pack is not installed");
            continue;
        }
        geometry_packs.push(GeometryPackConfig {
            id,
            path,
            core_id,
            priority: pack.priority,
            format: if pack.format.eq_ignore_ascii_case("legacy") {
                PackFormat::Legacy
            } else {
                PackFormat::Normalized
            },
            required: pack.required,
        });
    }
    geometry_packs.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.id.cmp(&right.id))
    });

    let overlay_path = if xml.overlay.path.as_os_str().is_empty() {
        PathBuf::from("runtime/state/location-overlay.sqlite")
    } else {
        xml.overlay.path
    };
    let feed_bindings_path = (!xml.feed_bindings.path.as_os_str().is_empty())
        .then(|| resolve_path(root_dir, &xml.feed_bindings.path));
    let mut force_groups = Vec::new();
    for group in xml.grouping.force {
        let members = split_members(&group.members);
        if members.len() > 1 {
            force_groups.push(members);
        }
    }
    let mut never_groups = Vec::new();
    for group in xml.grouping.never {
        let members = split_members(&group.members);
        if members.len() == 2 {
            never_groups.push((members[0].clone(), members[1].clone()));
        }
    }

    Ok(ServiceConfig {
        rollout_mode: yaml.mode.unwrap_or_else(|| "legacy".to_string()),
        workers: yaml.workers.unwrap_or(4).clamp(1, 32),
        queue_size: yaml.queue_size.unwrap_or(128).clamp(1, 4096),
        default_limit: if xml.query.default_limit == 0 {
            10
        } else {
            xml.query.default_limit
        },
        maximum_limit: if xml.query.maximum_limit == 0 {
            100
        } else {
            xml.query.maximum_limit
        }
        .clamp(1, 100),
        packs,
        geometry_packs,
        overlay_path: resolve_path(root_dir, &overlay_path),
        allowed_overlay_sources: xml
            .overlay
            .sources
            .into_iter()
            .map(|source| source.id.trim().to_string())
            .filter(|source| !source.is_empty())
            .collect(),
        feed_bindings_path,
        force_groups,
        never_groups,
    })
}

fn pack_id(raw: &str, path: &Path, fallback: &str) -> String {
    let raw = raw.trim();
    if !raw.is_empty() {
        return raw.to_string();
    }
    path.file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(fallback)
        .to_string()
}

fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn split_members(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn loads_optional_legacy_pack_and_grouping_overrides() {
        let dir = tempdir().expect("temp directory");
        fs::create_dir_all(dir.path().join("managed/configs")).expect("config directory");
        fs::write(dir.path().join("legacy.sqlite"), b"").expect("legacy file");
        fs::write(
            dir.path().join("config.yaml"),
            "services:\n  rust:\n    location:\n      mode: shadow\n      config: managed/configs/locations.xml\n",
        )
        .expect("root config");
        fs::write(
            dir.path().join("managed/configs/locations.xml"),
            r#"<locations><catalogs><legacy id="legacy" path="legacy.sqlite"/></catalogs><overlay path="runtime/state/overlay.sqlite"><source id="haze-data-ingest"/></overlay><query defaultLimit="12" maximumLimit="50"/><grouping><force members="a,b"/><never members="c,d"/></grouping></locations>"#,
        )
        .expect("location config");

        let config = load(&dir.path().join("config.yaml"), None).expect("load config");
        assert_eq!(config.rollout_mode, "shadow");
        assert_eq!(config.default_limit, 12);
        assert_eq!(config.packs.len(), 1);
        assert_eq!(config.packs[0].format, PackFormat::Legacy);
        assert_eq!(
            config.force_groups,
            vec![vec!["a".to_string(), "b".to_string()]]
        );
        assert_eq!(
            config.never_groups,
            vec![("c".to_string(), "d".to_string())]
        );
    }

    #[test]
    fn loads_geometry_pack_paired_with_core() {
        let dir = tempdir().expect("temp directory");
        fs::create_dir_all(dir.path().join("managed/configs")).expect("config directory");
        fs::write(dir.path().join("core.sqlite"), b"").expect("core file");
        fs::write(dir.path().join("geometry.sqlite"), b"").expect("geometry file");
        fs::write(dir.path().join("config.yaml"), "services: {}\n").expect("root config");
        fs::write(
            dir.path().join("managed/configs/locations.xml"),
            r#"<locations><catalogs><legacy id="core" path="core.sqlite"/><geometry id="boundaries" path="geometry.sqlite" core="core" priority="20" format="legacy"/></catalogs></locations>"#,
        )
        .expect("location config");

        let config = load(&dir.path().join("config.yaml"), None).expect("load config");
        assert_eq!(config.geometry_packs.len(), 1);
        assert_eq!(config.geometry_packs[0].id, "boundaries");
        assert_eq!(config.geometry_packs[0].core_id, "core");
        assert_eq!(config.geometry_packs[0].format, PackFormat::Legacy);
    }

    #[test]
    fn rejects_unknown_geometry_core_and_skips_missing_optional_file() {
        let dir = tempdir().expect("temp directory");
        fs::create_dir_all(dir.path().join("managed/configs")).expect("config directory");
        fs::write(dir.path().join("core.sqlite"), b"").expect("core file");
        fs::write(dir.path().join("config.yaml"), "services: {}\n").expect("root config");
        let locations = dir.path().join("managed/configs/locations.xml");
        fs::write(
            &locations,
            r#"<locations><catalogs><legacy id="core" path="core.sqlite"/><geometry id="missing" path="missing.sqlite" core="core"/></catalogs></locations>"#,
        )
        .expect("location config");
        let config = load(&dir.path().join("config.yaml"), None).expect("optional geometry");
        assert!(config.geometry_packs.is_empty());

        fs::write(
            &locations,
            r#"<locations><catalogs><legacy id="core" path="core.sqlite"/><geometry id="bad" path="missing.sqlite" core="typo"/></catalogs></locations>"#,
        )
        .expect("location config");
        let error = load(&dir.path().join("config.yaml"), None).expect_err("unknown core");
        assert!(error.to_string().contains("unknown core pack typo"));
    }
}
