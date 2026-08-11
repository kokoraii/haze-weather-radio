//! Read-only catalog snapshots, legacy compatibility, and overlay persistence.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, Row};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use strsim::jaro_winkler;
use thiserror::Error;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::{GeometryPackConfig, PackConfig, PackFormat, ServiceConfig};
use crate::contract::{
    Candidate, ConfidenceLevel, DedupeMode, Deployment, Entity, Geometry, Identifier, LocationName,
    MatchInfo, OverlayUpsert, QueryFilters, QueryOptions, RelationshipStep, StationMode,
    StationModeRequirement, ALGORITHM_VERSION,
};
use crate::geometry::{bounding_box, contains_wkb, haversine_m, valid_point, wkb_to_geojson};
use crate::normalize::{normalize_identifier, normalize_name, normalize_station_mode, t9_digits};

const LOCATION_NAMESPACE: Uuid = Uuid::from_u128(0xf987bc4e56fe5f418bcffdcf8c5a8e3e);
const OVERLAY_SCHEMA: &str = include_str!("../schema/v1.sql");
#[cfg(test)]
const GEOMETRY_SCHEMA: &str = include_str!("../schema/geometry_v1.sql");
const MAX_SEARCH_CANDIDATES: usize = 500;
const MAX_LEGACY_NAME_SCAN_ROWS: usize = 100_000;
type Attributes = BTreeMap<String, Value>;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("catalog query failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("invalid query: {0}")]
    Invalid(String),
    #[error("catalog is unavailable: {0}")]
    Unavailable(String),
    #[error("overlay source is not allowed: {0}")]
    OverlaySource(String),
}

#[derive(Clone, Debug)]
pub struct CatalogSnapshot {
    pub generation: String,
    pub packs: Vec<PackConfig>,
    pub geometry_packs: Vec<GeometryPackConfig>,
    pub overlay_path: PathBuf,
    pub pack_ids: Vec<String>,
    pack_checksums: Vec<String>,
    geometry_pack_checksums: Vec<String>,
    legacy_name_indexes: Vec<Option<Arc<LegacyNameIndex>>>,
}

#[derive(Clone)]
pub struct CatalogManager {
    snapshot: Arc<RwLock<Arc<CatalogSnapshot>>>,
    previous: Arc<RwLock<Option<Arc<CatalogSnapshot>>>>,
    allowed_overlay_sources: Arc<RwLock<HashSet<String>>>,
    overlay_write_lock: Arc<std::sync::Mutex<()>>,
}

impl CatalogManager {
    pub fn load(config: &ServiceConfig) -> Result<Self> {
        initialize_overlay(&config.overlay_path)?;
        let mut snapshot = validate_snapshot(config)?;
        if sync_configured_feed_bindings(config, &snapshot)? {
            snapshot = validate_snapshot(config)?;
        }
        snapshot = pin_snapshot(snapshot)?;
        let snapshot = Arc::new(snapshot);
        Ok(Self {
            snapshot: Arc::new(RwLock::new(snapshot)),
            previous: Arc::new(RwLock::new(None)),
            allowed_overlay_sources: Arc::new(RwLock::new(
                config
                    .allowed_overlay_sources
                    .iter()
                    .map(|value| value.to_ascii_lowercase())
                    .collect(),
            )),
            overlay_write_lock: Arc::new(std::sync::Mutex::new(())),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<CatalogSnapshot> {
        self.snapshot.read().map_or_else(
            |poisoned| Arc::clone(poisoned.get_ref()),
            |guard| Arc::clone(&guard),
        )
    }

    pub fn reload(&self, config: &ServiceConfig) -> Result<Arc<CatalogSnapshot>> {
        initialize_overlay(&config.overlay_path)?;
        let mut replacement = validate_snapshot(config)?;
        if sync_configured_feed_bindings(config, &replacement)? {
            replacement = validate_snapshot(config)?;
        }
        replacement = pin_snapshot(replacement)?;
        let replacement = Arc::new(replacement);
        let allowed_sources = config
            .allowed_overlay_sources
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect();
        let mut guard = self
            .snapshot
            .write()
            .map_err(|_| anyhow::anyhow!("location catalog snapshot lock is poisoned"))?;
        let prior = std::mem::replace(&mut *guard, Arc::clone(&replacement));
        let mut previous = self
            .previous
            .write()
            .map_err(|_| anyhow::anyhow!("location catalog rollback lock is poisoned"))?;
        *previous = Some(prior);
        let mut allowed = self
            .allowed_overlay_sources
            .write()
            .map_err(|_| anyhow::anyhow!("location overlay allowlist lock is poisoned"))?;
        *allowed = allowed_sources;
        Ok(replacement)
    }

    pub fn rollback(&self) -> Result<Arc<CatalogSnapshot>> {
        let mut previous = self
            .previous
            .write()
            .map_err(|_| anyhow::anyhow!("location catalog rollback lock is poisoned"))?;
        let replacement = previous
            .take()
            .ok_or_else(|| anyhow::anyhow!("no prior location catalog generation is available"))?;
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| anyhow::anyhow!("location catalog snapshot lock is poisoned"))?;
        let prior = std::mem::replace(&mut *current, Arc::clone(&replacement));
        *previous = Some(prior);
        Ok(replacement)
    }

    pub fn upsert_overlay(&self, request: &OverlayUpsert) -> Result<String, CatalogError> {
        let source = request.source_id.trim().to_ascii_lowercase();
        let allowed_sources = self.allowed_overlay_sources.read().map_err(|_| {
            CatalogError::Unavailable("overlay allowlist lock is poisoned".to_string())
        })?;
        if !allowed_sources.is_empty() && !allowed_sources.contains(&source) {
            return Err(CatalogError::OverlaySource(request.source_id.clone()));
        }
        drop(allowed_sources);
        let mut request = request.clone();
        if request.entity.id.trim().is_empty() {
            request.entity.id = self.overlay_canonical_id(&request.entity)?;
        }
        validate_overlay_entity(&request.entity)?;
        let _guard = self.overlay_write_lock.lock().map_err(|_| {
            CatalogError::Unavailable("overlay writer lock is poisoned".to_string())
        })?;
        let snapshot = self.snapshot();
        let mut connection = Connection::open(&snapshot.overlay_path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        let transaction = connection.transaction()?;
        upsert_entity(&transaction, &request)?;
        transaction.commit()?;
        Ok(request.entity.id)
    }

    fn overlay_canonical_id(&self, entity: &Entity) -> Result<String, CatalogError> {
        let identifier = entity.identifiers.first().ok_or_else(|| {
            CatalogError::Invalid("overlay entity requires at least one identifier".to_string())
        })?;
        let mut worker = WorkerCatalog::new();
        let snapshot = self.snapshot();
        worker.prepare(&snapshot)?;
        let candidates = worker.resolve_identifier(
            &identifier.scheme,
            Some(&identifier.authority),
            &identifier.value,
            &QueryFilters::default(),
            &QueryOptions::default(),
        )?;
        let highest_priority = candidates
            .iter()
            .filter_map(|candidate| {
                candidate
                    .match_info
                    .evidence
                    .get("pack_priority")
                    .and_then(Value::as_i64)
            })
            .max();
        if let Some(priority) = highest_priority {
            let preferred: Vec<_> = candidates
                .iter()
                .filter(|candidate| {
                    candidate
                        .match_info
                        .evidence
                        .get("pack_priority")
                        .and_then(Value::as_i64)
                        == Some(priority)
                })
                .collect();
            if let [candidate] = preferred.as_slice() {
                return Ok(candidate.entity.id.clone());
            }
        }
        let key = format!(
            "overlay:{}:{}:{}",
            identifier.authority.to_ascii_lowercase(),
            canonical_scheme(&identifier.scheme),
            normalize_identifier(&identifier.scheme, &identifier.value)
        );
        Ok(stable_id(&key))
    }
}

fn validate_snapshot(config: &ServiceConfig) -> Result<CatalogSnapshot> {
    let mut generation_parts = Vec::new();
    let mut pack_ids = Vec::new();
    let mut pack_checksums = Vec::with_capacity(config.packs.len());
    for pack in &config.packs {
        let checksum = validate_pack(pack)?;
        pack_ids.push(pack.id.clone());
        generation_parts.push(format!("core:{}:{checksum}", pack.id));
        pack_checksums.push(checksum);
    }
    let mut geometry_pack_checksums = Vec::with_capacity(config.geometry_packs.len());
    for geometry_pack in &config.geometry_packs {
        let core_index = config
            .packs
            .iter()
            .position(|pack| pack.id == geometry_pack.core_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "geometry pack {} references unavailable core pack {}",
                    geometry_pack.id,
                    geometry_pack.core_id
                )
            })?;
        let core_pack = &config.packs[core_index];
        let core_checksum = &pack_checksums[core_index];
        let checksum = validate_geometry_pack(geometry_pack, core_pack, core_checksum)?;
        pack_ids.push(geometry_pack.id.clone());
        generation_parts.push(format!(
            "geometry:{}:{}:{checksum}",
            geometry_pack.id, geometry_pack.core_id
        ));
        geometry_pack_checksums.push(checksum);
    }
    let generation = Uuid::new_v5(&LOCATION_NAMESPACE, generation_parts.join("|").as_bytes());
    info!(
        generation = %generation,
        packs = ?pack_ids,
        overlay = %config.overlay_path.display(),
        "validated location catalog snapshot"
    );
    Ok(CatalogSnapshot {
        generation: generation.to_string(),
        packs: config.packs.clone(),
        geometry_packs: config.geometry_packs.clone(),
        overlay_path: config.overlay_path.clone(),
        pack_ids,
        pack_checksums,
        geometry_pack_checksums,
        legacy_name_indexes: Vec::new(),
    })
}

fn pin_snapshot(mut snapshot: CatalogSnapshot) -> Result<CatalogSnapshot> {
    if snapshot.packs.is_empty() && snapshot.geometry_packs.is_empty() {
        return Ok(snapshot);
    }
    let state_root = snapshot
        .overlay_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("location-catalog-generations")
        .join(&snapshot.generation);
    fs::create_dir_all(&state_root)
        .with_context(|| format!("failed to create {}", state_root.display()))?;
    for (pack, checksum) in snapshot
        .packs
        .iter_mut()
        .zip(snapshot.pack_checksums.iter())
    {
        pack.path = retain_pack(
            &state_root,
            &snapshot.generation,
            &pack.id,
            &pack.path,
            checksum,
        )?;
    }
    for (pack, checksum) in snapshot
        .geometry_packs
        .iter_mut()
        .zip(snapshot.geometry_pack_checksums.iter())
    {
        pack.path = retain_pack(
            &state_root,
            &snapshot.generation,
            &pack.id,
            &pack.path,
            checksum,
        )?;
    }
    snapshot.legacy_name_indexes = snapshot
        .packs
        .iter()
        .map(|pack| {
            (pack.format == PackFormat::Legacy)
                .then(|| LegacyNameIndex::open(&pack.path))
                .transpose()
                .with_context(|| {
                    format!(
                        "failed to build legacy name index for location pack {}",
                        pack.id
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(snapshot)
}

fn retain_pack(
    state_root: &Path,
    generation: &str,
    id: &str,
    source: &Path,
    expected_checksum: &str,
) -> Result<PathBuf> {
    let source_checksum = file_sha256(source)?;
    if source_checksum != expected_checksum {
        anyhow::bail!(
            "location pack {} changed while generation {generation} was being retained",
            source.display()
        );
    }
    let extension = source
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("sqlite");
    let target = state_root.join(format!(
        "{}-{}.{}",
        safe_filename(id),
        &expected_checksum[..16],
        extension
    ));
    if !target.exists() {
        let copied = if cfg!(windows) {
            fs::copy(source, &target).map(|_| ())
        } else {
            fs::hard_link(source, &target).or_else(|_| fs::copy(source, &target).map(|_| ()))
        };
        if let Err(error) = copied {
            return Err(error).with_context(|| {
                format!(
                    "failed to retain catalog generation {generation} at {}",
                    target.display()
                )
            });
        }
    }
    let retained_checksum = file_sha256(&target)?;
    if retained_checksum != expected_checksum {
        anyhow::bail!(
            "retained location pack {} checksum mismatch",
            target.display()
        );
    }
    Ok(target)
}

fn safe_filename(value: &str) -> String {
    let normalized: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if normalized.is_empty() {
        "pack".to_string()
    } else {
        normalized
    }
}

fn validate_pack(pack: &PackConfig) -> Result<String> {
    let checksum = file_sha256(&pack.path)?;
    if pack.format == PackFormat::Normalized {
        validate_checksum_sidecar(&pack.path, &checksum)?;
    }
    let connection = open_read_only(&pack.path)
        .with_context(|| format!("failed to open location pack {}", pack.path.display()))?;
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .with_context(|| format!("failed to validate {}", pack.path.display()))?;
    if integrity != "ok" {
        anyhow::bail!(
            "location pack {} failed integrity check: {integrity}",
            pack.path.display()
        );
    }
    let required_table = match pack.format {
        PackFormat::Normalized => "entities",
        PackFormat::Legacy => "places",
    };
    let present: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        [required_table],
        |row| row.get(0),
    )?;
    if !present {
        anyhow::bail!(
            "location pack {} is missing required table {required_table}",
            pack.path.display()
        );
    }
    if pack.format == PackFormat::Normalized {
        let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version != 1 {
            anyhow::bail!(
                "location pack {} has unsupported schema version {version}",
                pack.path.display()
            );
        }
        let rtree: String =
            connection.query_row("SELECT rtreecheck('entity_rtree')", [], |row| row.get(0))?;
        if rtree != "ok" {
            anyhow::bail!(
                "location pack {} failed RTree validation: {rtree}",
                pack.path.display()
            );
        }
        validate_normalized_counts(&connection, &pack.path)?;
    }
    Ok(checksum)
}

fn validate_geometry_pack(
    pack: &GeometryPackConfig,
    core: &PackConfig,
    core_checksum: &str,
) -> Result<String> {
    let checksum = file_sha256(&pack.path)?;
    if pack.format == PackFormat::Normalized {
        validate_checksum_sidecar(&pack.path, &checksum)?;
    }
    let connection = open_read_only(&pack.path).with_context(|| {
        format!(
            "failed to open location geometry pack {}",
            pack.path.display()
        )
    })?;
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .with_context(|| format!("failed to validate {}", pack.path.display()))?;
    if integrity != "ok" {
        anyhow::bail!(
            "location geometry pack {} failed integrity check: {integrity}",
            pack.path.display()
        );
    }
    let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if version != 1 {
        anyhow::bail!(
            "location geometry pack {} has unsupported schema version {version}",
            pack.path.display()
        );
    }
    for table in [
        "catalog_metadata",
        "sources",
        "area_geometries",
        "area_geometry_rtree",
    ] {
        if !table_exists(&connection, table)? {
            anyhow::bail!(
                "location geometry pack {} is missing required table {table}",
                pack.path.display()
            );
        }
    }
    let pack_kind = metadata_value(&connection, "pack_kind")?.unwrap_or_default();
    if pack_kind != "geometry" {
        anyhow::bail!(
            "location geometry pack {} has invalid pack_kind metadata",
            pack.path.display()
        );
    }
    let metadata_pack_id = metadata_value(&connection, "pack_id")?.unwrap_or_default();
    if metadata_pack_id != pack.id {
        anyhow::bail!(
            "location geometry pack {} declares pack id {metadata_pack_id}, expected {}",
            pack.path.display(),
            pack.id
        );
    }
    let metadata_core_id = metadata_value(&connection, "core_pack_id")?.unwrap_or_default();
    if metadata_core_id != pack.core_id {
        anyhow::bail!(
            "location geometry pack {} declares core pack {metadata_core_id}, expected {}",
            pack.path.display(),
            pack.core_id
        );
    }
    let metadata_core_checksum = metadata_value(&connection, "core_sha256")?.unwrap_or_default();
    if metadata_core_checksum.len() != 64
        || !metadata_core_checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        anyhow::bail!(
            "location geometry pack {} has invalid core_sha256 metadata",
            pack.path.display()
        );
    }
    if !metadata_core_checksum.eq_ignore_ascii_case(core_checksum) {
        anyhow::bail!(
            "location geometry pack {} was built for core SHA-256 {metadata_core_checksum}, but paired core {} is {core_checksum}",
            pack.path.display(),
            core.path.display()
        );
    }
    let rtree: String =
        connection.query_row("SELECT rtreecheck('area_geometry_rtree')", [], |row| {
            row.get(0)
        })?;
    if rtree != "ok" {
        anyhow::bail!(
            "location geometry pack {} failed RTree validation: {rtree}",
            pack.path.display()
        );
    }
    validate_geometry_counts(&connection, &pack.path)?;
    validate_geometry_members(&connection, core, &pack.path)?;
    Ok(checksum)
}

fn table_exists(connection: &Connection, table: &str) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?1)",
        [table],
        |row| row.get(0),
    )
}

fn metadata_value(connection: &Connection, key: &str) -> rusqlite::Result<Option<String>> {
    connection
        .query_row(
            "SELECT value FROM catalog_metadata WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()
}

fn validate_geometry_counts(connection: &Connection, path: &Path) -> Result<()> {
    let actual: i64 =
        connection.query_row("SELECT COUNT(*) FROM area_geometries", [], |row| row.get(0))?;
    let expected =
        metadata_value(connection, "count.geometries")?.and_then(|value| value.parse::<i64>().ok());
    if expected != Some(actual) {
        anyhow::bail!(
            "location geometry pack {} has invalid count metadata for geometries",
            path.display()
        );
    }
    let indexed: i64 =
        connection.query_row("SELECT COUNT(*) FROM area_geometry_rtree", [], |row| {
            row.get(0)
        })?;
    if indexed != actual {
        anyhow::bail!(
            "location geometry pack {} has {actual} geometries but {indexed} RTree rows",
            path.display()
        );
    }
    let source_ids = connection
        .prepare("SELECT source_id FROM sources ORDER BY source_id")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_ids.is_empty() {
        anyhow::bail!(
            "location geometry pack {} contains no source metadata",
            path.display()
        );
    }
    for source_id in source_ids {
        let actual: i64 = connection.query_row(
            "SELECT COUNT(*) FROM area_geometries WHERE source_id = ?1",
            [&source_id],
            |row| row.get(0),
        )?;
        let expected = metadata_value(connection, &format!("count.source.{source_id}"))?
            .and_then(|value| value.parse::<i64>().ok());
        if expected != Some(actual) {
            anyhow::bail!(
                "location geometry pack {} has invalid source count metadata for {source_id}",
                path.display()
            );
        }
    }
    let invalid: i64 = connection.query_row(
        "SELECT COUNT(*) FROM area_geometries
         WHERE canonical_id = '' OR min_lon > max_lon OR min_lat > max_lat
            OR min_lon < -180 OR max_lon > 180 OR min_lat < -90 OR max_lat > 90
            OR (latitude IS NOT NULL AND (latitude < -90 OR latitude > 90))
            OR (longitude IS NOT NULL AND (longitude < -180 OR longitude > 180))",
        [],
        |row| row.get(0),
    )?;
    if invalid != 0 {
        anyhow::bail!(
            "location geometry pack {} contains {invalid} invalid geometry rows",
            path.display()
        );
    }
    let mut statement = connection.prepare(
        "SELECT geometry_pk, geometry_type, geometry_wkb FROM area_geometries ORDER BY geometry_pk",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Vec<u8>>(2)?,
        ))
    })?;
    for row in rows {
        let (geometry_pk, geometry_type, wkb) = row?;
        let decoded = wkb_to_geojson(&wkb).with_context(|| {
            format!(
                "location geometry pack {} has invalid WKB for geometry {geometry_pk}",
                path.display()
            )
        })?;
        let decoded_type = decoded
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !decoded_type.eq_ignore_ascii_case(&geometry_type) {
            anyhow::bail!(
                "location geometry pack {} geometry {geometry_pk} type does not match its WKB",
                path.display()
            );
        }
    }
    Ok(())
}

fn validate_geometry_members(
    geometry: &Connection,
    core: &PackConfig,
    geometry_path: &Path,
) -> Result<()> {
    let core_connection = open_read_only(&core.path)
        .with_context(|| format!("failed to open paired core pack {}", core.path.display()))?;
    let mut valid_ids = HashSet::new();
    match core.format {
        PackFormat::Normalized => {
            let mut statement = core_connection.prepare("SELECT canonical_id FROM entities")?;
            valid_ids.extend(
                statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            );
        }
        PackFormat::Legacy => {
            let mut statement = core_connection.prepare("SELECT source, code FROM places")?;
            for row in statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })? {
                let (source, code) = row?;
                valid_ids.insert(stable_id(&format!("legacy:{source}:{code}")));
            }
        }
    }
    let mut statement = geometry
        .prepare("SELECT canonical_id, source, code FROM area_geometries ORDER BY geometry_pk")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    for row in rows {
        let (canonical_id, source, code) = row?;
        if !valid_ids.contains(&canonical_id) {
            anyhow::bail!(
                "location geometry pack {} contains orphan entity {canonical_id}",
                geometry_path.display()
            );
        }
        if core.format == PackFormat::Legacy {
            let (Some(source), Some(code)) = (source, code) else {
                anyhow::bail!(
                    "legacy geometry entity {canonical_id} in {} is missing source-qualified identity",
                    geometry_path.display()
                );
            };
            if stable_id(&format!("legacy:{source}:{code}")) != canonical_id {
                anyhow::bail!(
                    "legacy geometry entity {canonical_id} in {} has mismatched source-qualified identity",
                    geometry_path.display()
                );
            }
        }
    }
    Ok(())
}

fn validate_checksum_sidecar(path: &Path, actual: &str) -> Result<()> {
    let checksum_path = path.with_extension(format!(
        "{}.sha256",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("sqlite")
    ));
    let raw = fs::read_to_string(&checksum_path).with_context(|| {
        format!(
            "normalized location pack {} is missing checksum sidecar {}",
            path.display(),
            checksum_path.display()
        )
    })?;
    let expected = raw.split_whitespace().next().unwrap_or_default();
    if expected.len() != 64 || !expected.eq_ignore_ascii_case(actual) {
        anyhow::bail!(
            "location pack {} checksum mismatch: expected {expected}, calculated {actual}",
            path.display()
        );
    }
    Ok(())
}

fn validate_normalized_counts(connection: &Connection, path: &Path) -> Result<()> {
    let source_ids = connection
        .prepare("SELECT source_id FROM sources ORDER BY source_id")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if source_ids.is_empty() {
        anyhow::bail!(
            "location pack {} contains no source metadata",
            path.display()
        );
    }
    for source_id in source_ids {
        let count: Option<String> = connection
            .query_row(
                "SELECT value FROM catalog_metadata WHERE key = ?1",
                [format!("count.source.{source_id}")],
                |row| row.get(0),
            )
            .optional()?;
        let parsed_count = count.as_deref().and_then(|value| value.parse::<u64>().ok());
        if !matches!(parsed_count, Some(value) if value > 0) {
            anyhow::bail!(
                "location pack {} has invalid source count metadata for {source_id}",
                path.display()
            );
        }
    }
    for table in ["entities", "names", "identifiers", "geometries"] {
        let actual: i64 =
            connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
        let expected: Option<String> = connection
            .query_row(
                "SELECT value FROM catalog_metadata WHERE key = ?1",
                [format!("count.{table}")],
                |row| row.get(0),
            )
            .optional()?;
        if expected
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok())
            != Some(actual)
        {
            anyhow::bail!(
                "location pack {} has invalid count metadata for {table}",
                path.display()
            );
        }
    }
    let names: i64 = connection.query_row("SELECT COUNT(*) FROM names", [], |row| row.get(0))?;
    let fts: i64 = connection.query_row("SELECT COUNT(*) FROM names_fts", [], |row| row.get(0))?;
    let trigram: i64 =
        connection.query_row("SELECT COUNT(*) FROM names_trigram", [], |row| row.get(0))?;
    if names != fts || names != trigram {
        anyhow::bail!(
            "location pack {} has incomplete name search indexes",
            path.display()
        );
    }
    let invalid_geometry: i64 = connection.query_row(
        "SELECT COUNT(*) FROM geometries
           WHERE min_lon > max_lon OR min_lat > max_lat
              OR min_lon < -180 OR max_lon > 180 OR min_lat < -90 OR max_lat > 90
              OR (latitude IS NOT NULL AND (latitude < -90 OR latitude > 90))
              OR (longitude IS NOT NULL AND (longitude < -180 OR longitude > 180))",
        [],
        |row| row.get(0),
    )?;
    if invalid_geometry != 0 {
        anyhow::bail!(
            "location pack {} contains {invalid_geometry} invalid geometries",
            path.display()
        );
    }
    let mut geometry_statement = connection.prepare(
        "SELECT geometry_pk, geometry_wkb FROM geometries WHERE geometry_wkb IS NOT NULL",
    )?;
    let mut geometries = geometry_statement.query([])?;
    while let Some(row) = geometries.next()? {
        let geometry_pk: i64 = row.get(0)?;
        let geometry_wkb: Vec<u8> = row.get(1)?;
        if let Err(error) = contains_wkb(&geometry_wkb, 181.0, 91.0) {
            anyhow::bail!(
                "location pack {} has invalid WKB for geometry {geometry_pk}: {error}",
                path.display()
            );
        }
    }
    Ok(())
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn initialize_overlay(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let connection = Connection::open(path)
        .with_context(|| format!("failed to open location overlay {}", path.display()))?;
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
    connection
        .execute_batch(OVERLAY_SCHEMA)
        .with_context(|| format!("failed to initialize location overlay {}", path.display()))?;
    let has_match_status = connection
        .prepare("PRAGMA table_info(feed_bindings)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|column| column == "match_status");
    if !has_match_status {
        connection.execute(
            "ALTER TABLE feed_bindings ADD COLUMN match_status TEXT NOT NULL DEFAULT 'unresolved'",
            [],
        )?;
    }
    let has_t9_digits = connection
        .prepare("PRAGMA table_info(names)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .any(|column| column == "t9_digits");
    if !has_t9_digits {
        connection.execute(
            "ALTER TABLE names ADD COLUMN t9_digits TEXT NOT NULL DEFAULT ''",
            [],
        )?;
    }
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_names_t9 ON names(t9_digits)",
        [],
    )?;
    let missing_t9 = connection
        .prepare("SELECT name_pk, name FROM names WHERE t9_digits = ''")?
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (name_pk, name) in missing_t9 {
        connection.execute(
            "UPDATE names SET t9_digits = ?1 WHERE name_pk = ?2",
            params![t9_digits(&name), name_pk],
        )?;
    }
    connection.execute(
        "INSERT OR REPLACE INTO catalog_metadata(key, value) VALUES('pack_id', 'runtime-overlay')",
        [],
    )?;
    Ok(())
}

fn sync_configured_feed_bindings(
    config: &ServiceConfig,
    snapshot: &CatalogSnapshot,
) -> Result<bool> {
    let Some(path) = config.feed_bindings_path.as_deref() else {
        return Ok(false);
    };
    if !path.exists() {
        warn!(path = %path.display(), "feed binding config is not installed");
        return Ok(false);
    }
    crate::feed_bindings::sync(snapshot, path)?;
    Ok(true)
}

fn open_read_only(path: &Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
}

struct OpenPack {
    id: String,
    priority: i32,
    format: PackFormat,
    connection: Connection,
    legacy_name_index: Option<Arc<LegacyNameIndex>>,
}

struct OpenGeometryPack {
    id: String,
    core_id: String,
    priority: i32,
    connection: Connection,
}

struct ExternalAreaGeometry {
    geometry: Value,
    same_code: Option<String>,
    provider_version: Option<String>,
    source_url: Option<String>,
    updated_at: Option<String>,
    bbox: [f64; 4],
    attributes: BTreeMap<String, Value>,
}

pub struct WorkerCatalog {
    generation: String,
    packs: Vec<OpenPack>,
    geometry_packs: Vec<OpenGeometryPack>,
    feed_bound_entities: HashSet<String>,
}

impl WorkerCatalog {
    #[must_use]
    pub fn new() -> Self {
        Self {
            generation: String::new(),
            packs: Vec::new(),
            geometry_packs: Vec::new(),
            feed_bound_entities: HashSet::new(),
        }
    }

    pub fn prepare(&mut self, snapshot: &CatalogSnapshot) -> Result<(), CatalogError> {
        if self.generation == snapshot.generation {
            return Ok(());
        }
        let mut packs = Vec::with_capacity(snapshot.packs.len() + 1);
        for (index, pack) in snapshot.packs.iter().enumerate() {
            packs.push(OpenPack {
                id: pack.id.clone(),
                priority: pack.priority,
                format: pack.format,
                connection: open_read_only(&pack.path)?,
                legacy_name_index: snapshot
                    .legacy_name_indexes
                    .get(index)
                    .and_then(Clone::clone),
            });
        }
        let mut geometry_packs = Vec::with_capacity(snapshot.geometry_packs.len());
        for pack in &snapshot.geometry_packs {
            geometry_packs.push(OpenGeometryPack {
                id: pack.id.clone(),
                core_id: pack.core_id.clone(),
                priority: pack.priority,
                connection: open_read_only(&pack.path)?,
            });
        }
        packs.push(OpenPack {
            id: "runtime-overlay".to_string(),
            priority: i32::MIN,
            format: PackFormat::Normalized,
            connection: open_read_only(&snapshot.overlay_path)?,
            legacy_name_index: None,
        });
        let feed_bound_entities = packs
            .last()
            .expect("overlay pack is present")
            .connection
            .prepare(
                "SELECT DISTINCT canonical_id FROM feed_bindings
                 WHERE canonical_id IS NOT NULL AND match_status = 'resolved'",
            )?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<HashSet<_>>>()?;
        self.generation.clone_from(&snapshot.generation);
        self.packs = packs;
        self.geometry_packs = geometry_packs;
        self.feed_bound_entities = feed_bound_entities;
        Ok(())
    }

    fn finalize(
        &self,
        mut candidates: Vec<Candidate>,
        filters: &QueryFilters,
        options: &QueryOptions,
    ) -> Result<Vec<Candidate>, CatalogError> {
        for candidate in &mut candidates {
            if self.feed_bound_entities.contains(&candidate.entity.id) {
                candidate
                    .entity
                    .attributes
                    .insert("configured_feed_binding".to_string(), json!(true));
            }
        }
        let mut candidates = finalize_candidates(candidates, filters, options);
        if options.include_area_geometry {
            self.enrich_area_geometries(&mut candidates, options.as_of.as_deref())?;
        }
        Ok(candidates)
    }

    fn enrich_area_geometries(
        &self,
        candidates: &mut [Candidate],
        as_of: Option<&str>,
    ) -> Result<(), CatalogError> {
        for candidate in candidates {
            let core_id = candidate
                .match_info
                .evidence
                .get("pack")
                .and_then(Value::as_str);
            let mut external = None;
            for pack in self.geometry_packs.iter().filter(|pack| {
                core_id.is_none_or(|wanted| pack.core_id.eq_ignore_ascii_case(wanted))
            }) {
                if let Some(mut geometry) =
                    external_area_geometry(&pack.connection, &candidate.entity.id, as_of)?
                {
                    geometry.attributes.insert(
                        "geometry_pack".to_string(),
                        json!({"id": pack.id, "priority": pack.priority}),
                    );
                    external = Some(geometry);
                    break;
                }
            }
            if external.is_none() {
                if let Some((source, code)) = legacy_identity(&candidate.entity) {
                    for pack in self.packs.iter().filter(|pack| {
                        pack.format == PackFormat::Legacy
                            && core_id.is_none_or(|wanted| pack.id.eq_ignore_ascii_case(wanted))
                    }) {
                        if let Some(legacy) =
                            load_legacy_area_geometry(&pack.connection, source, code)?
                        {
                            external = Some(ExternalAreaGeometry {
                                geometry: legacy.geometry,
                                same_code: Some(legacy.same_code),
                                provider_version: Some(legacy.provider_version),
                                source_url: Some(legacy.source_url),
                                updated_at: Some(legacy.updated_at),
                                bbox: legacy.bbox,
                                attributes: BTreeMap::new(),
                            });
                            break;
                        }
                    }
                }
            }
            if let Some(geometry) = external {
                apply_area_geometry(&mut candidate.entity, geometry);
            }
        }
        Ok(())
    }

    pub fn resolve_identifier(
        &self,
        scheme: &str,
        authority: Option<&str>,
        value: &str,
        filters: &QueryFilters,
        options: &QueryOptions,
    ) -> Result<Vec<Candidate>, CatalogError> {
        let scheme = canonical_scheme(scheme);
        let normalized = normalize_identifier(&scheme, value);
        if normalized.is_empty() {
            return Err(CatalogError::Invalid(
                "identifier value is empty".to_string(),
            ));
        }
        let mut candidates = Vec::new();
        for pack in &self.packs {
            let mut found = match pack.format {
                PackFormat::Normalized => normalized_identifier_candidates(
                    &pack.connection,
                    &scheme,
                    authority,
                    &normalized,
                    options.as_of.as_deref(),
                )?,
                PackFormat::Legacy => {
                    legacy_identifier_candidates(&pack.connection, &scheme, authority, &normalized)?
                }
            };
            for candidate in &mut found {
                candidate
                    .match_info
                    .evidence
                    .insert("pack".to_string(), json!(pack.id));
                candidate
                    .match_info
                    .evidence
                    .insert("pack_priority".to_string(), json!(pack.priority));
            }
            candidates.extend(found);
        }
        self.finalize(candidates, filters, options)
    }

    pub fn entity_by_id(
        &self,
        canonical_id: &str,
        filters: &QueryFilters,
        options: &QueryOptions,
    ) -> Result<Vec<Candidate>, CatalogError> {
        let mut candidates = Vec::new();
        for pack in &self.packs {
            let entity = match pack.format {
                PackFormat::Normalized => normalized_entity_by_id(
                    &pack.connection,
                    canonical_id,
                    options.as_of.as_deref(),
                )?,
                PackFormat::Legacy => legacy_entity_by_id(&pack.connection, canonical_id)?,
            };
            if let Some(entity) = entity {
                candidates.push(Candidate {
                    entity,
                    match_info: exact_match("canonical_id", pack),
                    facet: None,
                    distance_m: None,
                    relationship_path: Vec::new(),
                    grouping: None,
                });
            }
        }
        self.finalize(candidates, filters, options)
    }

    pub fn search(
        &self,
        text: &str,
        filters: &QueryFilters,
        options: &QueryOptions,
    ) -> Result<Vec<Candidate>, CatalogError> {
        let t9 = matches!(
            options.input_mode.trim().to_ascii_lowercase().as_str(),
            "t9" | "dtmf"
        );
        let normalized = if t9 {
            t9_digits(text)
        } else {
            normalize_name(text)
        };
        if normalized.is_empty() {
            return Err(CatalogError::Invalid("search text is empty".to_string()));
        }
        let candidate_limit = options
            .limit
            .saturating_mul(20)
            .clamp(50, MAX_SEARCH_CANDIDATES);
        let legacy_candidate_limit = if options.dedupe_mode == DedupeMode::SimilarName {
            options
                .limit
                .saturating_mul(2)
                .clamp(20, MAX_SEARCH_CANDIDATES)
        } else if options.geographic_bias.is_some()
            || options.station_mode_preference.is_some()
            || options.locale.is_some()
        {
            options
                .limit
                .saturating_mul(5)
                .clamp(50, MAX_SEARCH_CANDIDATES)
        } else {
            options.limit.clamp(10, MAX_SEARCH_CANDIDATES)
        };
        let mut candidates = Vec::new();
        for pack in &self.packs {
            let mut found = match pack.format {
                PackFormat::Normalized if t9 => normalized_t9_candidates(
                    &pack.connection,
                    &normalized,
                    candidate_limit,
                    options.as_of.as_deref(),
                )?,
                PackFormat::Normalized => normalized_name_candidates(
                    &pack.connection,
                    &normalized,
                    candidate_limit,
                    options.as_of.as_deref(),
                )?,
                PackFormat::Legacy => legacy_name_candidates(
                    &pack.connection,
                    pack.legacy_name_index.as_deref().ok_or_else(|| {
                        CatalogError::Unavailable(format!(
                            "legacy name index for pack {} is unavailable",
                            pack.id
                        ))
                    })?,
                    &normalized,
                    legacy_candidate_limit,
                    t9,
                    filters,
                    options,
                )?,
            };
            if pack.format == PackFormat::Normalized {
                found.extend(normalized_unqualified_identifier_candidates(
                    &pack.connection,
                    text,
                    candidate_limit,
                    options.as_of.as_deref(),
                )?);
            }
            for candidate in &mut found {
                candidate
                    .match_info
                    .evidence
                    .insert("pack".to_string(), json!(pack.id));
                candidate
                    .match_info
                    .evidence
                    .insert("pack_priority".to_string(), json!(pack.priority));
                if candidate.match_info.method != "exact_identifier" {
                    if t9 {
                        apply_t9_score(candidate, &normalized);
                    } else {
                        apply_name_score(candidate, &normalized, options.locale.as_deref());
                    }
                }
                if let Some(bias) = options.geographic_bias {
                    apply_geographic_bias(candidate, bias.latitude, bias.longitude);
                }
            }
            candidates.extend(found);
        }
        self.finalize(candidates, filters, options)
    }

    pub fn nearest(
        &self,
        latitude: f64,
        longitude: f64,
        filters: &QueryFilters,
        options: &QueryOptions,
    ) -> Result<Vec<Candidate>, CatalogError> {
        if !valid_point(latitude, longitude) {
            return Err(CatalogError::Invalid(
                "latitude or longitude is outside WGS84 bounds".to_string(),
            ));
        }
        let mut radius_km = options.max_distance_km.unwrap_or(25.0).max(0.1);
        let maximum_radius = options.max_distance_km.unwrap_or(20_050.0).max(radius_km);
        let mut candidates = Vec::new();
        loop {
            candidates.clear();
            for pack in &self.packs {
                let mut found = match pack.format {
                    PackFormat::Normalized => normalized_spatial_candidates(
                        &pack.connection,
                        latitude,
                        longitude,
                        radius_km,
                        options.limit.saturating_mul(20).clamp(50, 1000),
                        options.as_of.as_deref(),
                    )?,
                    PackFormat::Legacy => legacy_spatial_candidates(
                        &pack.connection,
                        latitude,
                        longitude,
                        radius_km,
                        options.limit.saturating_mul(20).clamp(50, 1000),
                    )?,
                };
                for candidate in &mut found {
                    candidate
                        .match_info
                        .evidence
                        .insert("pack".to_string(), json!(pack.id));
                }
                candidates.extend(found);
            }
            let filtered = self.finalize(candidates.clone(), filters, options)?;
            if filtered.len() >= options.limit || radius_km >= maximum_radius {
                candidates = filtered;
                break;
            }
            radius_km = (radius_km * 2.0).min(maximum_radius);
        }
        candidates.sort_by(candidate_order);
        Ok(candidates)
    }

    pub fn point_facets(
        &self,
        latitude: f64,
        longitude: f64,
        filters: &QueryFilters,
        options: &QueryOptions,
    ) -> Result<Vec<Candidate>, CatalogError> {
        if !valid_point(latitude, longitude) {
            return Err(CatalogError::Invalid(
                "latitude or longitude is outside WGS84 bounds".to_string(),
            ));
        }
        let mut candidates = Vec::new();
        for pack in &self.packs {
            if pack.format != PackFormat::Normalized {
                continue;
            }
            let mut found = normalized_containing_candidates(
                &pack.connection,
                latitude,
                longitude,
                options.as_of.as_deref(),
            )?;
            for candidate in &mut found {
                candidate.facet = Some(facet_for_kind(&candidate.entity.kind).to_string());
                candidate
                    .match_info
                    .evidence
                    .insert("pack".to_string(), json!(pack.id));
            }
            candidates.extend(found);
        }
        for geometry_pack in &self.geometry_packs {
            let Some(core_pack) = self
                .packs
                .iter()
                .find(|pack| pack.id == geometry_pack.core_id)
            else {
                return Err(CatalogError::Unavailable(format!(
                    "geometry pack {} lost its paired core {}",
                    geometry_pack.id, geometry_pack.core_id
                )));
            };
            let mut found = external_containing_candidates(
                core_pack,
                geometry_pack,
                latitude,
                longitude,
                options.as_of.as_deref(),
            )?;
            for candidate in &mut found {
                candidate.facet = Some(facet_for_kind(&candidate.entity.kind).to_string());
            }
            candidates.extend(found);
        }
        let mut nearest_options = options.clone();
        nearest_options.include_area_geometry = false;
        nearest_options.limit = options.limit.saturating_mul(8).clamp(16, 200);
        nearest_options.max_distance_km = options.max_distance_km.or(Some(250.0));
        for mut candidate in self.nearest(latitude, longitude, filters, &nearest_options)? {
            if candidate.distance_m == Some(0.0)
                && candidates
                    .iter()
                    .any(|existing| existing.entity.id == candidate.entity.id)
            {
                continue;
            }
            candidate.facet = Some(facet_for_kind(&candidate.entity.kind).to_string());
            candidates.push(candidate);
        }
        let mut per_facet = HashMap::<String, usize>::new();
        let mut finalized = self.finalize(candidates, filters, options)?;
        finalized.retain(|candidate| {
            let facet = candidate
                .facet
                .clone()
                .unwrap_or_else(|| "other".to_string());
            if !filters.roles.is_empty()
                && !filters
                    .roles
                    .iter()
                    .any(|role| role.eq_ignore_ascii_case(&facet))
            {
                return false;
            }
            let count = per_facet.entry(facet).or_default();
            if *count >= options.limit {
                false
            } else {
                *count += 1;
                true
            }
        });
        Ok(finalized)
    }

    pub fn traverse(
        &self,
        start_id: &str,
        filters: &QueryFilters,
        options: &QueryOptions,
    ) -> Result<Vec<Candidate>, CatalogError> {
        if filters.relationship_types.is_empty() {
            return Err(CatalogError::Invalid(
                "traverse requires at least one relationship type".to_string(),
            ));
        }
        let allowed: HashSet<String> = filters
            .relationship_types
            .iter()
            .map(|value| value.trim().to_ascii_lowercase())
            .collect();
        let mut visited = HashSet::from([start_id.to_string()]);
        let mut queue =
            VecDeque::from([(start_id.to_string(), Vec::<RelationshipStep>::new(), 0usize)]);
        let mut candidates = Vec::new();
        while let Some((current, path, depth)) = queue.pop_front() {
            if depth >= options.max_depth || visited.len() >= options.max_visited {
                continue;
            }
            let mut edges = Vec::new();
            for pack in &self.packs {
                edges.extend(match pack.format {
                    PackFormat::Normalized => normalized_relationships(
                        &pack.connection,
                        &current,
                        &allowed,
                        options.as_of.as_deref(),
                    )?,
                    PackFormat::Legacy => {
                        legacy_relationships(&pack.connection, &current, &allowed)?
                    }
                });
            }
            edges.sort_by(|left, right| {
                left.relationship_type
                    .cmp(&right.relationship_type)
                    .then_with(|| {
                        right
                            .score
                            .partial_cmp(&left.score)
                            .unwrap_or(Ordering::Equal)
                    })
                    .then_with(|| left.to_id.cmp(&right.to_id))
            });
            for edge in edges {
                if !visited.insert(edge.to_id.clone()) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.push(edge.clone());
                let mut entities = self.entity_by_id(&edge.to_id, filters, options)?;
                for entity in &mut entities {
                    let minimum = next_path
                        .iter()
                        .map(|step| step.confidence)
                        .min()
                        .unwrap_or(ConfidenceLevel::Low);
                    entity.match_info = MatchInfo {
                        score: next_path.iter().map(|step| step.score).fold(1.0, f64::min),
                        confidence: minimum,
                        method: "graph_traversal".to_string(),
                        algorithm: ALGORITHM_VERSION.to_string(),
                        evidence: BTreeMap::from([
                            ("depth".to_string(), json!(next_path.len())),
                            ("start_id".to_string(), json!(start_id)),
                        ]),
                    };
                    entity.relationship_path.clone_from(&next_path);
                }
                candidates.extend(entities);
                queue.push_back((edge.to_id, next_path, depth + 1));
                if candidates.len() >= options.limit || visited.len() >= options.max_visited {
                    break;
                }
            }
            if candidates.len() >= options.limit {
                break;
            }
        }
        self.finalize(candidates, filters, options)
    }
}

impl Default for WorkerCatalog {
    fn default() -> Self {
        Self::new()
    }
}

fn normalized_identifier_candidates(
    connection: &Connection,
    scheme: &str,
    authority: Option<&str>,
    normalized: &str,
    as_of: Option<&str>,
) -> Result<Vec<Candidate>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT DISTINCT entity_pk FROM identifiers
         WHERE scheme = ?1 AND normalized_value = ?2
           AND (?3 = '' OR lower(authority) = lower(?3))
         ORDER BY is_primary DESC, entity_pk",
    )?;
    let authority = authority.unwrap_or("").trim();
    let rows = statement.query_map(params![scheme, normalized, authority], |row| {
        row.get::<_, i64>(0)
    })?;
    let mut out = Vec::new();
    for row in rows {
        if let Some(entity) = load_normalized_entity(connection, row?, as_of)? {
            out.push(Candidate {
                entity,
                match_info: MatchInfo {
                    score: 1.0,
                    confidence: ConfidenceLevel::Exact,
                    method: "exact_identifier".to_string(),
                    algorithm: ALGORITHM_VERSION.to_string(),
                    evidence: BTreeMap::from([
                        ("scheme".to_string(), json!(scheme)),
                        ("normalized_value".to_string(), json!(normalized)),
                    ]),
                },
                facet: None,
                distance_m: None,
                relationship_path: Vec::new(),
                grouping: None,
            });
        }
    }
    Ok(out)
}

fn normalized_unqualified_identifier_candidates(
    connection: &Connection,
    raw: &str,
    limit: usize,
    as_of: Option<&str>,
) -> Result<Vec<Candidate>, CatalogError> {
    let general = normalize_identifier("provider", raw);
    let postal = normalize_identifier("postal", raw);
    if general.is_empty() {
        return Ok(Vec::new());
    }
    let mut statement = connection.prepare(
        "SELECT entity_pk, scheme, normalized_value
         FROM identifiers
         WHERE normalized_value = ?1 OR normalized_value = ?2
         ORDER BY is_primary DESC, scheme, entity_pk
         LIMIT ?3",
    )?;
    let mut seen = HashSet::new();
    let rows = statement.query_map(params![general, postal, limit as i64], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (entity_pk, scheme, normalized_value) = row?;
        if !seen.insert(entity_pk) {
            continue;
        }
        if let Some(entity) = load_normalized_entity(connection, entity_pk, as_of)? {
            out.push(Candidate {
                entity,
                match_info: MatchInfo {
                    score: 1.0,
                    confidence: ConfidenceLevel::Exact,
                    method: "exact_identifier".to_string(),
                    algorithm: ALGORITHM_VERSION.to_string(),
                    evidence: BTreeMap::from([
                        ("scheme".to_string(), json!(scheme)),
                        ("normalized_value".to_string(), json!(normalized_value)),
                    ]),
                },
                facet: None,
                distance_m: None,
                relationship_path: Vec::new(),
                grouping: None,
            });
        }
    }
    Ok(out)
}

fn normalized_entity_by_id(
    connection: &Connection,
    canonical_id: &str,
    as_of: Option<&str>,
) -> Result<Option<Entity>, CatalogError> {
    let entity_pk = connection
        .query_row(
            "SELECT entity_pk FROM entities WHERE canonical_id = ?1",
            [canonical_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    entity_pk
        .map(|pk| load_normalized_entity(connection, pk, as_of))
        .transpose()
        .map(|value| value.flatten())
}

fn load_normalized_entity(
    connection: &Connection,
    entity_pk: i64,
    as_of: Option<&str>,
) -> Result<Option<Entity>, CatalogError> {
    let as_of = as_of.unwrap_or("");
    let base = connection
        .query_row(
            "SELECT canonical_id, kind, country, region, lifecycle_status, reporting_status,
                    source_quality, attributes_json
             FROM entities WHERE entity_pk = ?1
               AND (valid_from IS NULL OR date(valid_from) <= date(CASE WHEN ?2 = '' THEN 'now' ELSE ?2 END))
               AND (valid_to IS NULL OR date(valid_to) >= date(CASE WHEN ?2 = '' THEN 'now' ELSE ?2 END))",
            params![entity_pk, as_of],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, f64>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((id, kind, country, region, lifecycle, reporting, quality, attributes_json)) = base
    else {
        return Ok(None);
    };
    let capabilities = collect_strings(
        connection,
        "SELECT capability FROM entity_capabilities WHERE entity_pk = ?1 ORDER BY capability",
        entity_pk,
    )?;
    let mut identifiers_statement = connection.prepare(
        "SELECT authority, scheme, value, normalized_value, is_primary, confidence, source_id
         FROM identifiers WHERE entity_pk = ?1
         ORDER BY is_primary DESC, authority, scheme, normalized_value",
    )?;
    let identifiers = identifiers_statement
        .query_map([entity_pk], |row| {
            Ok(Identifier {
                authority: row.get(0)?,
                scheme: row.get(1)?,
                value: row.get(2)?,
                normalized_value: row.get(3)?,
                primary: row.get::<_, i64>(4)? != 0,
                confidence: ConfidenceLevel::from_raw(&row.get::<_, String>(5)?),
                source_id: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut names_statement = connection.prepare(
        "SELECT locale, name, normalized_name, name_kind, is_primary, source_id
         FROM names WHERE entity_pk = ?1
         ORDER BY is_primary DESC, locale, name",
    )?;
    let names = names_statement
        .query_map([entity_pk], |row| {
            Ok(LocationName {
                locale: row.get(0)?,
                value: row.get(1)?,
                normalized_value: row.get(2)?,
                name_kind: row.get(3)?,
                primary: row.get::<_, i64>(4)? != 0,
                source_id: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let geometry = connection
        .query_row(
            "SELECT geometry_type, latitude, longitude, min_lon, min_lat, max_lon, max_lat,
                    accuracy_m, source_id
             FROM geometries
             WHERE entity_pk = ?1 AND (?2 != '' OR is_current = 1)
               AND (valid_from IS NULL OR date(valid_from) <= date(CASE WHEN ?2 = '' THEN 'now' ELSE ?2 END))
               AND (valid_to IS NULL OR date(valid_to) >= date(CASE WHEN ?2 = '' THEN 'now' ELSE ?2 END))
             ORDER BY valid_from DESC, geometry_pk DESC LIMIT 1",
            params![entity_pk, as_of],
            geometry_from_row,
        )
        .optional()?;
    let mut deployments_statement = connection.prepare(
        "SELECT provider_deployment_id, owner, platform_type, latitude, longitude,
                  elevation_m, valid_from, valid_to, reporting_status, source_id, attributes_json
           FROM deployments WHERE entity_pk = ?1
           ORDER BY valid_from DESC, deployment_pk DESC",
    )?;
    let deployments = deployments_statement
        .query_map([entity_pk], |row| {
            let attributes_json: String = row.get(10)?;
            Ok(Deployment {
                provider_deployment_id: row.get(0)?,
                owner: row.get(1)?,
                platform_type: row.get(2)?,
                latitude: row.get(3)?,
                longitude: row.get(4)?,
                elevation_m: row.get(5)?,
                valid_from: row.get(6)?,
                valid_to: row.get(7)?,
                reporting_status: row.get(8)?,
                source_id: row.get(9)?,
                attributes: parse_attributes(&attributes_json),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let attributes = parse_attributes(&attributes_json);
    Ok(Some(Entity {
        id,
        kind,
        capabilities,
        country,
        region,
        lifecycle_status: lifecycle,
        reporting_status: reporting,
        source_quality: quality,
        identifiers,
        names,
        geometry,
        deployments,
        attributes,
    }))
}

fn geometry_from_row(row: &Row<'_>) -> rusqlite::Result<Geometry> {
    Ok(Geometry {
        geometry_type: row.get(0)?,
        latitude: row.get(1)?,
        longitude: row.get(2)?,
        bbox: [row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?],
        accuracy_m: row.get(7)?,
        source_id: row.get(8)?,
    })
}

fn collect_strings(
    connection: &Connection,
    sql: &str,
    entity_pk: i64,
) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare(sql)?;
    let values = statement
        .query_map([entity_pk], |row| row.get::<_, String>(0))?
        .collect();
    values
}

fn normalized_name_candidates(
    connection: &Connection,
    normalized: &str,
    limit: usize,
    as_of: Option<&str>,
) -> Result<Vec<Candidate>, CatalogError> {
    let mut pks = BTreeSet::new();
    let mut exact = connection
        .prepare("SELECT DISTINCT entity_pk FROM names WHERE normalized_name = ?1 LIMIT ?2")?;
    for row in exact.query_map(params![normalized, limit as i64], |row| {
        row.get::<_, i64>(0)
    })? {
        pks.insert(row?);
    }
    let fts_query = fts_phrase(normalized);
    let mut fts = connection.prepare(
        "SELECT DISTINCT CAST(entity_pk AS INTEGER) FROM names_fts
         WHERE names_fts MATCH ?1 LIMIT ?2",
    )?;
    for row in fts.query_map(params![fts_query, limit as i64], |row| row.get::<_, i64>(0))? {
        pks.insert(row?);
    }
    if normalized.chars().count() >= 3 {
        let mut trigram = connection.prepare(
            "SELECT DISTINCT CAST(entity_pk AS INTEGER) FROM names_trigram
             WHERE names_trigram MATCH ?1 LIMIT ?2",
        )?;
        for row in trigram.query_map(params![fts_phrase(normalized), limit as i64], |row| {
            row.get::<_, i64>(0)
        })? {
            pks.insert(row?);
        }
    }
    let mut out = Vec::new();
    for entity_pk in pks.into_iter().take(limit) {
        if let Some(entity) = load_normalized_entity(connection, entity_pk, as_of)? {
            out.push(name_candidate(entity));
        }
    }
    Ok(out)
}

fn normalized_t9_candidates(
    connection: &Connection,
    digits: &str,
    limit: usize,
    as_of: Option<&str>,
) -> Result<Vec<Candidate>, CatalogError> {
    if digits.len() < 2 || !digits.chars().all(|character| character.is_ascii_digit()) {
        return Err(CatalogError::Invalid(
            "T9/DTMF search requires at least two digits".to_string(),
        ));
    }
    let mut statement = connection.prepare(
        "SELECT DISTINCT entity_pk FROM names
         WHERE t9_digits = ?1 OR t9_digits LIKE ?2
         ORDER BY CASE WHEN t9_digits = ?1 THEN 0 ELSE 1 END, entity_pk
         LIMIT ?3",
    )?;
    let prefix = format!("{digits}%");
    let entity_pks = statement
        .query_map(params![digits, prefix, limit as i64], |row| {
            row.get::<_, i64>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = Vec::new();
    for entity_pk in entity_pks {
        if let Some(entity) = load_normalized_entity(connection, entity_pk, as_of)? {
            out.push(name_candidate(entity));
        }
    }
    Ok(out)
}

fn normalized_spatial_candidates(
    connection: &Connection,
    latitude: f64,
    longitude: f64,
    radius_km: f64,
    limit: usize,
    as_of: Option<&str>,
) -> Result<Vec<Candidate>, CatalogError> {
    let as_of = as_of.unwrap_or("");
    let bbox = bounding_box(latitude, longitude, radius_km);
    let mut statement = connection.prepare(
        "SELECT DISTINCT g.entity_pk, g.latitude, g.longitude, g.geometry_wkb
         FROM entity_rtree r
         JOIN geometries g ON g.geometry_pk = r.geometry_pk
         WHERE r.min_lon <= ?3 AND r.max_lon >= ?1
           AND r.min_lat <= ?4 AND r.max_lat >= ?2
           AND (?5 != '' OR g.is_current = 1)
           AND (g.valid_from IS NULL OR date(g.valid_from) <= date(CASE WHEN ?5 = '' THEN 'now' ELSE ?5 END))
           AND (g.valid_to IS NULL OR date(g.valid_to) >= date(CASE WHEN ?5 = '' THEN 'now' ELSE ?5 END))
         LIMIT ?6",
    )?;
    let rows = statement.query_map(
        params![bbox[0], bbox[1], bbox[2], bbox[3], as_of, limit as i64],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<f64>>(1)?,
                row.get::<_, Option<f64>>(2)?,
                row.get::<_, Option<Vec<u8>>>(3)?,
            ))
        },
    )?;
    let mut out = Vec::new();
    for row in rows {
        let (entity_pk, point_lat, point_lon, wkb) = row?;
        let Some(entity) = load_normalized_entity(connection, entity_pk, Some(as_of))? else {
            continue;
        };
        let distance = geometry_distance(
            &entity,
            wkb.as_deref(),
            latitude,
            longitude,
            point_lat,
            point_lon,
        );
        if distance <= radius_km * 1000.0 {
            out.push(spatial_candidate(entity, distance));
        }
    }
    Ok(out)
}

fn normalized_containing_candidates(
    connection: &Connection,
    latitude: f64,
    longitude: f64,
    as_of: Option<&str>,
) -> Result<Vec<Candidate>, CatalogError> {
    let as_of = as_of.unwrap_or("");
    let mut statement = connection.prepare(
        "SELECT DISTINCT g.entity_pk, g.geometry_wkb, g.geometry_type
         FROM entity_rtree r
         JOIN geometries g ON g.geometry_pk = r.geometry_pk
         WHERE r.min_lon <= ?1 AND r.max_lon >= ?1
           AND r.min_lat <= ?2 AND r.max_lat >= ?2
           AND (?3 != '' OR g.is_current = 1)
           AND (g.valid_from IS NULL OR date(g.valid_from) <= date(CASE WHEN ?3 = '' THEN 'now' ELSE ?3 END))
           AND (g.valid_to IS NULL OR date(g.valid_to) >= date(CASE WHEN ?3 = '' THEN 'now' ELSE ?3 END))",
    )?;
    let rows = statement.query_map(params![longitude, latitude, as_of], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<Vec<u8>>>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (entity_pk, wkb, geometry_type) = row?;
        if !matches!(geometry_type.as_str(), "polygon" | "multipolygon") {
            continue;
        }
        let contained = wkb
            .as_deref()
            .is_some_and(|bytes| contains_wkb(bytes, longitude, latitude).unwrap_or(false));
        if !contained {
            continue;
        }
        if let Some(entity) = load_normalized_entity(connection, entity_pk, Some(as_of))? {
            out.push(Candidate {
                entity,
                match_info: MatchInfo {
                    score: 1.0,
                    confidence: ConfidenceLevel::High,
                    method: "spatial_contains".to_string(),
                    algorithm: ALGORITHM_VERSION.to_string(),
                    evidence: BTreeMap::from([
                        ("latitude".to_string(), json!(latitude)),
                        ("longitude".to_string(), json!(longitude)),
                    ]),
                },
                facet: None,
                distance_m: Some(0.0),
                relationship_path: Vec::new(),
                grouping: None,
            });
        }
    }
    Ok(out)
}

fn external_containing_candidates(
    core: &OpenPack,
    geometry_pack: &OpenGeometryPack,
    latitude: f64,
    longitude: f64,
    as_of: Option<&str>,
) -> Result<Vec<Candidate>, CatalogError> {
    let as_of = as_of.unwrap_or("");
    let mut statement = geometry_pack.connection.prepare(
        "SELECT DISTINCT g.canonical_id, g.source, g.code, g.geometry_wkb, g.geometry_type
         FROM area_geometry_rtree r
         JOIN area_geometries g ON g.geometry_pk = r.geometry_pk
         WHERE r.min_lon <= ?1 AND r.max_lon >= ?1
           AND r.min_lat <= ?2 AND r.max_lat >= ?2
           AND (?3 != '' OR g.is_current = 1)
           AND (g.valid_from IS NULL OR date(g.valid_from) <= date(CASE WHEN ?3 = '' THEN 'now' ELSE ?3 END))
           AND (g.valid_to IS NULL OR date(g.valid_to) >= date(CASE WHEN ?3 = '' THEN 'now' ELSE ?3 END))",
    )?;
    let rows = statement.query_map(params![longitude, latitude, as_of], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (canonical_id, source, code, wkb, geometry_type) = row?;
        if !matches!(
            geometry_type.to_ascii_lowercase().as_str(),
            "polygon" | "multipolygon"
        ) {
            continue;
        }
        let contained = contains_wkb(&wkb, longitude, latitude).map_err(|error| {
            CatalogError::Unavailable(format!(
                "geometry pack {} contains invalid WKB for {canonical_id}: {error}",
                geometry_pack.id
            ))
        })?;
        if !contained {
            continue;
        }
        let entity = match core.format {
            PackFormat::Normalized => {
                normalized_entity_by_id(&core.connection, &canonical_id, Some(as_of))?
            }
            PackFormat::Legacy => match (source.as_deref(), code.as_deref()) {
                (Some(source), Some(code)) => {
                    let entity = load_legacy_entity(&core.connection, source, code)?;
                    entity.filter(|entity| entity.id == canonical_id)
                }
                _ => None,
            },
        };
        let Some(entity) = entity else {
            continue;
        };
        out.push(Candidate {
            entity,
            match_info: MatchInfo {
                score: 1.0,
                confidence: ConfidenceLevel::High,
                method: "spatial_contains".to_string(),
                algorithm: ALGORITHM_VERSION.to_string(),
                evidence: BTreeMap::from([
                    ("latitude".to_string(), json!(latitude)),
                    ("longitude".to_string(), json!(longitude)),
                    ("pack".to_string(), json!(core.id)),
                    ("pack_priority".to_string(), json!(core.priority)),
                    ("geometry_pack".to_string(), json!(geometry_pack.id)),
                ]),
            },
            facet: None,
            distance_m: Some(0.0),
            relationship_path: Vec::new(),
            grouping: None,
        });
    }
    Ok(out)
}

fn normalized_relationships(
    connection: &Connection,
    current: &str,
    allowed: &HashSet<String>,
    as_of: Option<&str>,
) -> Result<Vec<RelationshipStep>, CatalogError> {
    let as_of = as_of.unwrap_or("");
    let mut statement = connection.prepare(
        "SELECT from_id, to_id, relationship_type, confidence, score, method
         FROM relationships WHERE from_id = ?1
           AND (valid_from IS NULL OR date(valid_from) <= date(CASE WHEN ?2 = '' THEN 'now' ELSE ?2 END))
           AND (valid_to IS NULL OR date(valid_to) >= date(CASE WHEN ?2 = '' THEN 'now' ELSE ?2 END))
         ORDER BY relationship_type, score DESC, to_id",
    )?;
    let rows = statement.query_map(params![current, as_of], |row| {
        Ok(RelationshipStep {
            from_id: row.get(0)?,
            to_id: row.get(1)?,
            relationship_type: row.get(2)?,
            confidence: ConfidenceLevel::from_raw(&row.get::<_, String>(3)?),
            score: row.get(4)?,
            method: row.get(5)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        let edge = row?;
        if allowed.contains(&edge.relationship_type.to_ascii_lowercase()) {
            out.push(edge);
        }
    }
    Ok(out)
}

fn legacy_identifier_candidates(
    connection: &Connection,
    scheme: &str,
    authority: Option<&str>,
    normalized: &str,
) -> Result<Vec<Candidate>, CatalogError> {
    if matches!(scheme, "postal" | "postal_code" | "zip" | "zcta") {
        return legacy_postal_candidates(connection, normalized);
    }
    let source = legacy_source_for_scheme(scheme);
    let mut rows = Vec::new();
    if let Some(source) = source {
        let source = source.trim().to_ascii_lowercase();
        let code = normalized.trim().to_ascii_uppercase();
        let mut statement = connection
            .prepare("SELECT source, code FROM places WHERE source = ?1 AND code = ?2")?;
        rows.extend(
            statement
                .query_map(params![source, code], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        );
    } else if matches!(scheme, "icao" | "wmo" | "msc" | "eccc_station") {
        let attribute = match scheme {
            "icao" => "$.icao",
            "wmo" => "$.wmo_id",
            "msc" => "$.msc_id",
            _ => "$.icao",
        };
        let sql = format!(
            "SELECT source, code FROM places
             WHERE source = 'station' AND upper(COALESCE(json_extract(attrs_json, '{attribute}'), '')) = ?1"
        );
        let mut statement = connection.prepare(&sql)?;
        rows.extend(
            statement
                .query_map([normalized], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        );
    } else if authority.is_none() {
        let mut statement = connection
            .prepare("SELECT source, code FROM places WHERE upper(code) = ?1 ORDER BY source")?;
        rows.extend(
            statement
                .query_map([normalized], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        );
    }
    let mut out = Vec::new();
    for (source, code) in rows {
        if let Some(entity) = load_legacy_entity(connection, &source, &code)? {
            out.push(Candidate {
                entity,
                match_info: MatchInfo {
                    score: 1.0,
                    confidence: ConfidenceLevel::Exact,
                    method: "exact_identifier".to_string(),
                    algorithm: ALGORITHM_VERSION.to_string(),
                    evidence: BTreeMap::from([
                        ("scheme".to_string(), json!(scheme)),
                        ("legacy_source".to_string(), json!(source)),
                    ]),
                },
                facet: None,
                distance_m: None,
                relationship_path: Vec::new(),
                grouping: None,
            });
        }
    }
    Ok(out)
}

fn legacy_postal_candidates(
    connection: &Connection,
    normalized: &str,
) -> Result<Vec<Candidate>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT area_source, area_code, MIN(distance_km), MAX(score)
         FROM postal_links
         WHERE upper(replace(postal_code, ' ', '')) = ?1
         GROUP BY area_source, area_code
         ORDER BY MAX(score) DESC, MIN(distance_km), area_source, area_code",
    )?;
    let rows = statement.query_map([normalized], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(2)?,
            row.get::<_, f64>(3)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (source, code, distance_km, score) = row?;
        if let Some(entity) = load_legacy_entity(connection, &source, &code)? {
            out.push(Candidate {
                entity,
                match_info: MatchInfo {
                    score,
                    confidence: if score >= 0.9 {
                        ConfidenceLevel::High
                    } else {
                        ConfidenceLevel::Medium
                    },
                    method: "postal_crosswalk".to_string(),
                    algorithm: ALGORITHM_VERSION.to_string(),
                    evidence: BTreeMap::from([
                        ("postal".to_string(), json!(normalized)),
                        ("distance_m".to_string(), json!(distance_km * 1000.0)),
                    ]),
                },
                facet: Some("postal".to_string()),
                distance_m: Some(distance_km * 1000.0),
                relationship_path: Vec::new(),
                grouping: None,
            });
        }
    }
    Ok(out)
}

fn legacy_entity_by_id(
    connection: &Connection,
    canonical_id: &str,
) -> Result<Option<Entity>, CatalogError> {
    let mut statement =
        connection.prepare("SELECT source, code FROM places ORDER BY source, code")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (source, code) = row?;
        if stable_id(&format!("legacy:{source}:{code}")) == canonical_id {
            return load_legacy_entity(connection, &source, &code);
        }
    }
    Ok(None)
}

fn load_legacy_entity(
    connection: &Connection,
    source: &str,
    code: &str,
) -> Result<Option<Entity>, CatalogError> {
    let row = connection
        .query_row(
            "SELECT name, name_fr, region, country, kind, lat, lon, attrs_json
             FROM places WHERE source = ?1 AND code = ?2",
            params![source, code],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<f64>>(5)?,
                    row.get::<_, Option<f64>>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((name, name_fr, region, country, legacy_kind, latitude, longitude, attrs_json)) = row
    else {
        return Ok(None);
    };
    let kind = legacy_kind_name(source, &legacy_kind);
    let mut attributes = parse_attributes(&attrs_json);
    enrich_legacy_location_codes(connection, source, code, &mut attributes)?;
    if let Some(mode) = attributes
        .get("mode")
        .and_then(Value::as_str)
        .and_then(normalize_station_mode)
    {
        attributes.insert("station_mode".to_string(), json!(station_mode_name(mode)));
    }
    let mut identifiers = vec![Identifier {
        authority: legacy_authority(source).to_string(),
        scheme: canonical_scheme(source),
        value: code.to_string(),
        normalized_value: normalize_identifier(source, code),
        primary: true,
        confidence: ConfidenceLevel::Exact,
        source_id: Some("legacy-alert-location-map".to_string()),
    }];
    if source == "station" {
        for (scheme, key) in [("icao", "icao"), ("wmo", "wmo_id"), ("msc", "msc_id")] {
            if let Some(value) = attributes
                .get(key)
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
            {
                identifiers.push(Identifier {
                    authority: "eccc".to_string(),
                    scheme: scheme.to_string(),
                    value: value.to_string(),
                    normalized_value: normalize_identifier(scheme, value),
                    primary: false,
                    confidence: ConfidenceLevel::Exact,
                    source_id: Some("legacy-alert-location-map".to_string()),
                });
            }
        }
    }
    let mut names = vec![LocationName {
        locale: Some("en-CA".to_string()),
        normalized_value: normalize_name(&name),
        value: name,
        name_kind: "canonical".to_string(),
        primary: true,
        source_id: Some("legacy-alert-location-map".to_string()),
    }];
    if !name_fr.trim().is_empty() && normalize_name(&name_fr) != names[0].normalized_value {
        names.push(LocationName {
            locale: Some("fr-CA".to_string()),
            normalized_value: normalize_name(&name_fr),
            value: name_fr,
            name_kind: "canonical".to_string(),
            primary: false,
            source_id: Some("legacy-alert-location-map".to_string()),
        });
    }
    let geometry = latitude.zip(longitude).and_then(|(lat, lon)| {
        valid_point(lat, lon).then_some(Geometry {
            geometry_type: "point".to_string(),
            latitude: Some(lat),
            longitude: Some(lon),
            bbox: [lon, lat, lon, lat],
            accuracy_m: None,
            source_id: Some("legacy-alert-location-map".to_string()),
        })
    });
    Ok(Some(Entity {
        id: stable_id(&format!("legacy:{source}:{code}")),
        kind: kind.clone(),
        capabilities: capabilities_for_kind(&kind),
        country: non_empty(&country).map(|value| value.to_ascii_uppercase()),
        region: non_empty(&region).map(|value| value.to_ascii_uppercase()),
        lifecycle_status: "unknown".to_string(),
        reporting_status: if source == "station" {
            "unknown".to_string()
        } else {
            "not_applicable".to_string()
        },
        source_quality: 0.7,
        identifiers,
        names,
        geometry,
        deployments: Vec::new(),
        attributes,
    }))
}

struct LegacyAreaGeometry {
    geometry: Value,
    same_code: String,
    provider_version: String,
    source_url: String,
    updated_at: String,
    bbox: [f64; 4],
}

fn external_area_geometry(
    connection: &Connection,
    canonical_id: &str,
    as_of: Option<&str>,
) -> Result<Option<ExternalAreaGeometry>, CatalogError> {
    let as_of = as_of.unwrap_or("");
    let row = connection
        .query_row(
            "SELECT geometry_wkb, same_code, provider_version, source_url, updated_at,
                    min_lon, min_lat, max_lon, max_lat, attributes_json
             FROM area_geometries
             WHERE canonical_id = ?1 AND (?2 != '' OR is_current = 1)
               AND (valid_from IS NULL OR date(valid_from) <= date(CASE WHEN ?2 = '' THEN 'now' ELSE ?2 END))
               AND (valid_to IS NULL OR date(valid_to) >= date(CASE WHEN ?2 = '' THEN 'now' ELSE ?2 END))
             ORDER BY is_current DESC, valid_from DESC, geometry_pk DESC LIMIT 1",
            params![canonical_id, as_of],
            |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, f64>(5)?,
                    row.get::<_, f64>(6)?,
                    row.get::<_, f64>(7)?,
                    row.get::<_, f64>(8)?,
                    row.get::<_, String>(9)?,
                ))
            },
        )
        .optional()?;
    let Some((
        wkb,
        same_code,
        provider_version,
        source_url,
        updated_at,
        min_lon,
        min_lat,
        max_lon,
        max_lat,
        attributes_json,
    )) = row
    else {
        return Ok(None);
    };
    let geometry = wkb_to_geojson(&wkb).map_err(|error| {
        CatalogError::Unavailable(format!(
            "external geometry for {canonical_id} is invalid: {error}"
        ))
    })?;
    Ok(Some(ExternalAreaGeometry {
        geometry,
        same_code,
        provider_version,
        source_url,
        updated_at,
        bbox: [min_lon, min_lat, max_lon, max_lat],
        attributes: parse_attributes(&attributes_json),
    }))
}

fn legacy_identity(entity: &Entity) -> Option<(&str, &str)> {
    entity
        .identifiers
        .iter()
        .find(|identifier| {
            identifier.primary
                && identifier.source_id.as_deref() == Some("legacy-alert-location-map")
        })
        .map(|identifier| (identifier.scheme.as_str(), identifier.value.as_str()))
}

fn apply_area_geometry(entity: &mut Entity, geometry: ExternalAreaGeometry) {
    entity
        .attributes
        .insert("area_geometry".to_string(), geometry.geometry);
    entity
        .attributes
        .insert("area_bbox".to_string(), json!(geometry.bbox));
    for (key, value) in geometry.attributes {
        entity.attributes.entry(key).or_insert(value);
    }
    for (key, value) in [
        ("same_code", geometry.same_code),
        ("provider_version", geometry.provider_version),
        ("geometry_source_url", geometry.source_url),
        ("geometry_updated_at", geometry.updated_at),
    ] {
        if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
            entity.attributes.insert(key.to_string(), json!(value));
        }
    }
}

fn load_legacy_area_geometry(
    connection: &Connection,
    source: &str,
    code: &str,
) -> Result<Option<LegacyAreaGeometry>, CatalogError> {
    if !legacy_table_exists(connection, "area_geometries")? {
        return Ok(None);
    }
    let row = connection
        .query_row(
            "SELECT geometry_json, same_code, provider_version, source_url, updated_at,
                    min_lon, min_lat, max_lon, max_lat
             FROM area_geometries WHERE source = ?1 AND code = ?2",
            params![source, code],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, f64>(5)?,
                    row.get::<_, f64>(6)?,
                    row.get::<_, f64>(7)?,
                    row.get::<_, f64>(8)?,
                ))
            },
        )
        .optional()?;
    let Some((
        geometry_json,
        same_code,
        provider_version,
        source_url,
        updated_at,
        min_lon,
        min_lat,
        max_lon,
        max_lat,
    )) = row
    else {
        return Ok(None);
    };
    let geometry = serde_json::from_str(&geometry_json).map_err(|error| {
        CatalogError::Invalid(format!(
            "legacy area geometry {source}:{code} is invalid JSON: {error}"
        ))
    })?;
    Ok(Some(LegacyAreaGeometry {
        geometry,
        same_code,
        provider_version,
        source_url,
        updated_at,
        bbox: [min_lon, min_lat, max_lon, max_lat],
    }))
}

fn legacy_table_exists(connection: &Connection, table: &str) -> Result<bool, CatalogError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get(0),
        )
        .map_err(CatalogError::from)
}

fn enrich_legacy_location_codes(
    connection: &Connection,
    source: &str,
    code: &str,
    attributes: &mut BTreeMap<String, Value>,
) -> Result<(), CatalogError> {
    if source == "clc" {
        insert_string_values(
            attributes,
            "sgc_codes",
            legacy_related_codes(connection, source, code, "sgc")?,
        );
        return Ok(());
    }
    if !matches!(source, "nws_same" | "nws_zone") {
        return Ok(());
    }

    let mut same_codes = BTreeSet::new();
    let mut zone_codes = BTreeSet::new();
    if source == "nws_same" {
        same_codes.insert(code.to_ascii_uppercase());
        zone_codes.extend(legacy_related_codes(connection, source, code, "nws_zone")?);
    } else {
        zone_codes.insert(code.to_ascii_uppercase());
        same_codes.extend(legacy_related_codes(connection, source, code, "nws_same")?);
    }

    let mut fips_codes = BTreeSet::new();
    let mut county_codes = BTreeSet::new();
    for same_code in &same_codes {
        let Some((region, same_attributes)) =
            legacy_place_region_attributes(connection, "nws_same", same_code)?
        else {
            continue;
        };
        for value in attribute_string_values(&same_attributes, "fips") {
            if let Some(normalized) = normalize_fips_code(&value) {
                if let Some(county) = nws_county_ugc(&region, &normalized) {
                    county_codes.insert(county);
                }
                fips_codes.insert(normalized);
            }
        }
        zone_codes.extend(attribute_string_values(&same_attributes, "zones"));
        zone_codes.extend(legacy_related_codes(
            connection, "nws_same", same_code, "nws_zone",
        )?);
    }

    let same_codes: Vec<_> = same_codes.into_iter().collect();
    if let [same_code] = same_codes.as_slice() {
        attributes
            .entry("same_code".to_string())
            .or_insert_with(|| json!(same_code));
    }
    insert_string_values(attributes, "same_codes", same_codes);
    insert_string_values(attributes, "fips_codes", fips_codes.into_iter().collect());
    insert_string_values(
        attributes,
        "nws_county_codes",
        county_codes.into_iter().collect(),
    );
    insert_string_values(
        attributes,
        "nws_zone_codes",
        zone_codes.into_iter().collect(),
    );
    Ok(())
}

fn legacy_related_codes(
    connection: &Connection,
    source: &str,
    code: &str,
    related_source: &str,
) -> Result<Vec<String>, CatalogError> {
    if !legacy_table_exists(connection, "links")? {
        return Ok(Vec::new());
    }
    let source = source.trim().to_ascii_lowercase();
    let code = code.trim().to_ascii_uppercase();
    let related_source = related_source.trim().to_ascii_lowercase();
    let mut statement = connection.prepare(
        "SELECT CASE
             WHEN from_source = ?1 AND from_code = ?2
             THEN to_code ELSE from_code
         END
         FROM links
         WHERE (from_source = ?1 AND from_code = ?2 AND to_source = ?3)
            OR (to_source = ?1 AND to_code = ?2 AND from_source = ?3)
         ORDER BY 1",
    )?;
    let values = statement
        .query_map(params![source, code, related_source], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(values
        .into_iter()
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
        .collect())
}

fn legacy_place_region_attributes(
    connection: &Connection,
    source: &str,
    code: &str,
) -> Result<Option<(String, Attributes)>, CatalogError> {
    let source = source.trim().to_ascii_lowercase();
    let code = code.trim().to_ascii_uppercase();
    connection
        .query_row(
            "SELECT region, attrs_json FROM places WHERE source = ?1 AND code = ?2",
            params![source, code],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map(|value| value.map(|(region, raw)| (region, parse_attributes(&raw))))
        .map_err(CatalogError::from)
}

fn attribute_string_values(attributes: &BTreeMap<String, Value>, key: &str) -> Vec<String> {
    let Some(value) = attributes.get(key) else {
        return Vec::new();
    };
    match value {
        Value::String(value) => vec![value.clone()],
        Value::Array(values) => values
            .iter()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

fn insert_string_values(attributes: &mut BTreeMap<String, Value>, key: &str, values: Vec<String>) {
    let values: Vec<_> = values
        .into_iter()
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if !values.is_empty() {
        attributes.insert(key.to_string(), json!(values));
    }
}

fn normalize_fips_code(value: &str) -> Option<String> {
    let value = value.trim();
    (value.len() == 5 && value.bytes().all(|byte| byte.is_ascii_digit())).then(|| value.to_string())
}

fn nws_county_ugc(region: &str, fips: &str) -> Option<String> {
    let region = region.trim().to_ascii_uppercase();
    (region.len() == 2 && fips.len() == 5).then(|| format!("{region}C{}", &fips[2..]))
}

#[derive(Debug)]
struct LegacyNameEntry {
    source: String,
    code: String,
    kind: String,
    country: Option<String>,
    region: Option<String>,
    capabilities: Box<[String]>,
    station_mode: Option<StationMode>,
    normalized_name: String,
    normalized_name_fr: String,
    t9_name: String,
    t9_name_fr: String,
    normal_gram_counts: [u16; 2],
    t9_gram_counts: [u16; 2],
}

#[derive(Debug)]
struct LegacyNameIndex {
    entries: Box<[LegacyNameEntry]>,
    normalized_names: BTreeMap<String, Box<[usize]>>,
    normalized_grams: HashMap<u64, Box<[usize]>>,
    t9_names: BTreeMap<String, Box<[usize]>>,
    t9_grams: HashMap<u64, Box<[usize]>>,
}

#[derive(Debug, Default)]
struct LegacyRetrievalHit {
    exact: bool,
    prefix: bool,
    gram_similarity: f64,
    gram_overlap: u16,
}

impl LegacyNameIndex {
    fn open(path: &Path) -> Result<Arc<Self>, CatalogError> {
        let connection = open_read_only(path)?;
        Self::load(&connection).map(Arc::new)
    }

    fn load(connection: &Connection) -> Result<Self, CatalogError> {
        // Legacy packs predate the normalized FTS schema. Normalize each name
        // once per immutable catalog generation so concurrent searches only do
        // bounded in-memory ranking and candidate hydration.
        // The extra row makes an oversized pack fail instead of silently
        // omitting entries beyond the compatibility limit.
        let mut statement = connection.prepare(
            "SELECT source, code, name, name_fr, region, country, kind, attrs_json
             FROM places LIMIT ?1",
        )?;
        let rows = statement.query_map(params![MAX_LEGACY_NAME_SCAN_ROWS as i64 + 1], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?;
        let mut entries = Vec::new();
        let mut normalized_names = BTreeMap::<String, Vec<usize>>::new();
        let mut normalized_grams = HashMap::<u64, Vec<usize>>::new();
        let mut indexed_t9_names = BTreeMap::<String, Vec<usize>>::new();
        let mut indexed_t9_grams = HashMap::<u64, Vec<usize>>::new();
        for (index, row) in rows.enumerate() {
            if index == MAX_LEGACY_NAME_SCAN_ROWS {
                return Err(CatalogError::Unavailable(format!(
                    "legacy catalog exceeds the {MAX_LEGACY_NAME_SCAN_ROWS}-place fuzzy search limit; rebuild it in normalized format"
                )));
            }
            let (source, code, name, name_fr, region, country, legacy_kind, attrs_json) = row?;
            let kind = legacy_kind_name(&source, &legacy_kind);
            let attributes = parse_attributes(&attrs_json);
            let station_mode = attributes
                .get("mode")
                .and_then(Value::as_str)
                .and_then(normalize_station_mode);
            let normalized_name = normalize_name(&name);
            let normalized_name_fr = normalize_name(&name_fr);
            let t9_name = t9_digits(&normalized_name);
            let t9_name_fr = t9_digits(&normalized_name_fr);
            let entry_index = entries.len();
            let normal_gram_counts = index_legacy_name_variants(
                entry_index,
                [&normalized_name, &normalized_name_fr],
                &mut normalized_names,
                &mut normalized_grams,
            );
            let t9_gram_counts = index_legacy_name_variants(
                entry_index,
                [&t9_name, &t9_name_fr],
                &mut indexed_t9_names,
                &mut indexed_t9_grams,
            );
            entries.push(LegacyNameEntry {
                source,
                code,
                capabilities: capabilities_for_kind(&kind).into_boxed_slice(),
                kind,
                country: non_empty(&country).map(|value| value.to_ascii_uppercase()),
                region: non_empty(&region).map(|value| value.to_ascii_uppercase()),
                station_mode,
                normalized_name,
                normalized_name_fr,
                t9_name,
                t9_name_fr,
                normal_gram_counts,
                t9_gram_counts,
            });
        }
        Ok(Self {
            entries: entries.into_boxed_slice(),
            normalized_names: freeze_legacy_name_map(normalized_names),
            normalized_grams: freeze_legacy_gram_map(normalized_grams),
            t9_names: freeze_legacy_name_map(indexed_t9_names),
            t9_grams: freeze_legacy_gram_map(indexed_t9_grams),
        })
    }

    #[cfg(test)]
    fn ranked(&self, query: &str, limit: usize, t9: bool) -> Vec<&LegacyNameEntry> {
        self.ranked_filtered(
            query,
            limit,
            t9,
            &QueryFilters::default(),
            &QueryOptions::default(),
        )
    }

    fn ranked_filtered(
        &self,
        query: &str,
        limit: usize,
        t9: bool,
        filters: &QueryFilters,
        options: &QueryOptions,
    ) -> Vec<&LegacyNameEntry> {
        let limit = limit.clamp(1, MAX_SEARCH_CANDIDATES);
        let candidate_indices = self.candidate_indices_filtered(query, limit, t9, filters, options);
        let mut ranked = Vec::with_capacity(limit.saturating_mul(2));
        for entry_index in candidate_indices {
            let entry = &self.entries[entry_index];
            let (name, name_fr) = if t9 {
                (&entry.t9_name, &entry.t9_name_fr)
            } else {
                (&entry.normalized_name, &entry.normalized_name_fr)
            };
            let score = legacy_normalized_name_retrieval_score(name, name_fr, query);
            ranked.push((entry, score));
            if ranked.len() == limit.saturating_mul(2) {
                rank_legacy_name_hits(&mut ranked, limit);
            }
        }
        rank_legacy_name_hits(&mut ranked, limit);
        ranked.into_iter().map(|(entry, _)| entry).collect()
    }

    #[cfg(test)]
    fn candidate_indices(&self, query: &str, limit: usize, t9: bool) -> Vec<usize> {
        self.candidate_indices_filtered(
            query,
            limit,
            t9,
            &QueryFilters::default(),
            &QueryOptions::default(),
        )
    }

    fn candidate_indices_filtered(
        &self,
        query: &str,
        limit: usize,
        t9: bool,
        filters: &QueryFilters,
        options: &QueryOptions,
    ) -> Vec<usize> {
        let (names, grams) = if t9 {
            (&self.t9_names, &self.t9_grams)
        } else {
            (&self.normalized_names, &self.normalized_grams)
        };
        let mut hits = HashMap::<usize, LegacyRetrievalHit>::new();
        if let Some(references) = names.get(query) {
            for &name_reference in references.iter() {
                hits.entry(name_reference / 2).or_default().exact = true;
            }
        }

        for (name, references) in names.range(query.to_string()..) {
            if !name.starts_with(query) {
                break;
            }
            for &name_reference in references.iter() {
                hits.entry(name_reference / 2).or_default().prefix = true;
            }
        }

        let query_grams = legacy_query_grams(query);
        let mut overlaps = HashMap::<usize, u16>::new();
        for gram in &query_grams {
            if let Some(references) = grams.get(gram) {
                for &name_reference in references.iter() {
                    let overlap = overlaps.entry(name_reference).or_default();
                    *overlap = overlap.saturating_add(1);
                }
            }
        }
        for (name_reference, overlap) in overlaps {
            let entry_index = name_reference / 2;
            let alternate = name_reference % 2;
            let entry = &self.entries[entry_index];
            let gram_count = if t9 {
                entry.t9_gram_counts[alternate]
            } else {
                entry.normal_gram_counts[alternate]
            };
            let denominator = query_grams.len().saturating_add(usize::from(gram_count));
            if denominator == 0 {
                continue;
            }
            let similarity = 2.0 * f64::from(overlap) / denominator as f64;
            let hit = hits.entry(entry_index).or_default();
            if similarity > hit.gram_similarity
                || (similarity == hit.gram_similarity && overlap > hit.gram_overlap)
            {
                hit.gram_similarity = similarity;
                hit.gram_overlap = overlap;
            }
        }

        let mut candidates: Vec<_> = hits.into_iter().collect();
        candidates.sort_by(|left, right| {
            right
                .1
                .exact
                .cmp(&left.1.exact)
                .then_with(|| right.1.prefix.cmp(&left.1.prefix))
                .then_with(|| right.1.gram_similarity.total_cmp(&left.1.gram_similarity))
                .then_with(|| right.1.gram_overlap.cmp(&left.1.gram_overlap))
                .then_with(|| {
                    self.entries[left.0]
                        .source
                        .cmp(&self.entries[right.0].source)
                })
                .then_with(|| self.entries[left.0].code.cmp(&self.entries[right.0].code))
        });
        candidates
            .into_iter()
            .filter(|(entry_index, _)| {
                legacy_name_entry_matches(&self.entries[*entry_index], filters, options)
            })
            .take(self.candidate_pool_limit(limit))
            .map(|(entry_index, _)| entry_index)
            .collect()
    }

    fn candidate_pool_limit(&self, limit: usize) -> usize {
        limit
            .clamp(1, MAX_SEARCH_CANDIDATES)
            .saturating_mul(8)
            .clamp(64, 2_000)
            .min(self.entries.len())
    }
}

fn legacy_name_entry_matches(
    entry: &LegacyNameEntry,
    filters: &QueryFilters,
    options: &QueryOptions,
) -> bool {
    if !filters.kinds.is_empty()
        && !filters
            .kinds
            .iter()
            .any(|kind| kind.eq_ignore_ascii_case(&entry.kind))
    {
        return false;
    }
    if !filters.capabilities.is_empty()
        && !filters.capabilities.iter().all(|wanted| {
            entry
                .capabilities
                .iter()
                .any(|actual| actual.eq_ignore_ascii_case(wanted))
        })
    {
        return false;
    }
    if filters.country.as_deref().is_some_and(|country| {
        !entry
            .country
            .as_deref()
            .is_some_and(|actual| actual.eq_ignore_ascii_case(country))
    }) {
        return false;
    }
    if filters.region.as_deref().is_some_and(|region| {
        !entry
            .region
            .as_deref()
            .is_some_and(|actual| actual.eq_ignore_ascii_case(region))
    }) {
        return false;
    }
    match options.station_mode_requirement {
        StationModeRequirement::Any => true,
        StationModeRequirement::Auto => entry.station_mode == Some(StationMode::Auto),
        StationModeRequirement::Manual => entry.station_mode == Some(StationMode::Manual),
    }
}

fn index_legacy_name_variants(
    entry_index: usize,
    values: [&str; 2],
    names: &mut BTreeMap<String, Vec<usize>>,
    grams: &mut HashMap<u64, Vec<usize>>,
) -> [u16; 2] {
    let mut gram_counts = [0; 2];
    for (alternate, value) in values.into_iter().enumerate() {
        if value.is_empty() {
            continue;
        }
        let name_reference = entry_index.saturating_mul(2).saturating_add(alternate);
        names
            .entry(value.to_string())
            .or_default()
            .push(name_reference);
        for gram in legacy_index_grams(value) {
            grams.entry(gram).or_default().push(name_reference);
        }
        gram_counts[alternate] = u16::try_from(legacy_query_grams(value).len()).unwrap_or(u16::MAX);
    }
    gram_counts
}

fn freeze_legacy_name_map(values: BTreeMap<String, Vec<usize>>) -> BTreeMap<String, Box<[usize]>> {
    values
        .into_iter()
        .map(|(key, postings)| (key, postings.into_boxed_slice()))
        .collect()
}

fn freeze_legacy_gram_map(values: HashMap<u64, Vec<usize>>) -> HashMap<u64, Box<[usize]>> {
    values
        .into_iter()
        .map(|(key, postings)| (key, postings.into_boxed_slice()))
        .collect()
}

fn legacy_index_grams(value: &str) -> Vec<u64> {
    legacy_name_grams(value, true)
}

fn legacy_query_grams(value: &str) -> Vec<u64> {
    legacy_name_grams(value, value.chars().count() <= 4)
}

fn legacy_name_grams(value: &str, include_unigrams: bool) -> Vec<u64> {
    let characters: Vec<_> = value.chars().collect();
    let mut grams = Vec::with_capacity(characters.len().saturating_mul(3));
    let first_width = if include_unigrams { 1 } else { 2 };
    for width in first_width..=3.min(characters.len()) {
        grams.extend(
            characters
                .windows(width)
                .map(|window| legacy_gram_hash(width, window)),
        );
    }
    grams.sort_unstable();
    grams.dedup();
    grams
}

fn legacy_gram_hash(width: usize, characters: &[char]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET ^ width as u64;
    for character in characters {
        hash ^= u64::from(u32::from(*character));
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn legacy_name_candidates(
    connection: &Connection,
    index: &LegacyNameIndex,
    normalized: &str,
    limit: usize,
    t9: bool,
    filters: &QueryFilters,
    options: &QueryOptions,
) -> Result<Vec<Candidate>, CatalogError> {
    let ranked = index.ranked_filtered(normalized, limit, t9, filters, options);
    let mut out = Vec::with_capacity(ranked.len());
    for entry in ranked {
        if let Some(entity) = load_legacy_entity(connection, &entry.source, &entry.code)? {
            out.push(name_candidate(entity));
        }
    }
    Ok(out)
}

fn rank_legacy_name_hits(ranked: &mut Vec<(&LegacyNameEntry, f64)>, limit: usize) {
    ranked.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.source.cmp(&right.0.source))
            .then_with(|| left.0.code.cmp(&right.0.code))
    });
    ranked.truncate(limit);
}

fn legacy_normalized_name_retrieval_score(name: &str, name_fr: &str, query: &str) -> f64 {
    [name, name_fr]
        .into_iter()
        .map(|candidate| {
            if candidate == query {
                1.0
            } else if candidate.starts_with(query) {
                jaro_winkler(candidate, query).max(0.94)
            } else {
                jaro_winkler(candidate, query)
            }
        })
        .fold(0.0, f64::max)
}

fn legacy_spatial_candidates(
    connection: &Connection,
    latitude: f64,
    longitude: f64,
    radius_km: f64,
    limit: usize,
) -> Result<Vec<Candidate>, CatalogError> {
    let bbox = bounding_box(latitude, longitude, radius_km);
    let mut statement = connection.prepare(
        "SELECT source, code, lat, lon FROM places
         WHERE lon BETWEEN ?1 AND ?3 AND lat BETWEEN ?2 AND ?4
         ORDER BY source, code LIMIT ?5",
    )?;
    let rows = statement.query_map(
        params![bbox[0], bbox[1], bbox[2], bbox[3], limit as i64],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
            ))
        },
    )?;
    let mut out = Vec::new();
    for row in rows {
        let (source, code, lat, lon) = row?;
        let distance = haversine_m(latitude, longitude, lat, lon);
        if distance > radius_km * 1000.0 {
            continue;
        }
        if let Some(entity) = load_legacy_entity(connection, &source, &code)? {
            out.push(spatial_candidate(entity, distance));
        }
    }
    Ok(out)
}

fn legacy_relationships(
    connection: &Connection,
    current: &str,
    allowed: &HashSet<String>,
) -> Result<Vec<RelationshipStep>, CatalogError> {
    let Some((source, code)) = legacy_source_code_for_id(connection, current)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    let mut statement = connection.prepare(
        "SELECT to_source, to_code, link_type, confidence, score, method
         FROM links WHERE from_source = ?1 AND from_code = ?2
         ORDER BY link_type, score DESC, to_source, to_code",
    )?;
    let rows = statement.query_map(params![source, code], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, f64>(4)?,
            row.get::<_, String>(5)?,
        ))
    })?;
    for row in rows {
        let (to_source, to_code, link_type, confidence, score, method) = row?;
        let relationship_type = legacy_relationship_type(&link_type);
        if !allowed.contains(relationship_type) {
            continue;
        }
        out.push(RelationshipStep {
            from_id: current.to_string(),
            to_id: stable_id(&format!("legacy:{to_source}:{to_code}")),
            relationship_type: relationship_type.to_string(),
            confidence: ConfidenceLevel::from_raw(&confidence),
            score,
            method,
        });
    }
    if allowed.contains("served_by") {
        let mut station_statement = connection.prepare(
            "SELECT station_id FROM station_links WHERE area_source = ?1 AND area_code = ?2",
        )?;
        let stations =
            station_statement.query_map(params![source, code], |row| row.get::<_, String>(0))?;
        for station in stations {
            let station = station?;
            out.push(RelationshipStep {
                from_id: current.to_string(),
                to_id: stable_id(&format!("legacy:station:{station}")),
                relationship_type: "served_by".to_string(),
                confidence: ConfidenceLevel::High,
                score: 0.9,
                method: "legacy_nearest_station".to_string(),
            });
        }
    }
    Ok(out)
}

fn legacy_source_code_for_id(
    connection: &Connection,
    canonical_id: &str,
) -> Result<Option<(String, String)>, CatalogError> {
    let mut statement =
        connection.prepare("SELECT source, code FROM places ORDER BY source, code")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (source, code) = row?;
        if stable_id(&format!("legacy:{source}:{code}")) == canonical_id {
            return Ok(Some((source, code)));
        }
    }
    Ok(None)
}

fn upsert_entity(
    transaction: &rusqlite::Transaction<'_>,
    request: &OverlayUpsert,
) -> Result<(), CatalogError> {
    let entity = &request.entity;
    transaction.execute(
        "INSERT INTO sources(source_id, title, retrieved_at, attributes_json)
         VALUES(?1, ?1, ?2, '{}')
         ON CONFLICT(source_id) DO UPDATE SET retrieved_at = excluded.retrieved_at",
        params![request.source_id, Utc::now().to_rfc3339()],
    )?;
    transaction.execute(
        "INSERT INTO entities(canonical_id, kind, country, region, lifecycle_status,
                              reporting_status, source_quality, valid_to, attributes_json)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(canonical_id) DO UPDATE SET
             kind = excluded.kind,
             country = excluded.country,
             region = excluded.region,
             lifecycle_status = excluded.lifecycle_status,
             reporting_status = excluded.reporting_status,
             source_quality = excluded.source_quality,
             valid_to = excluded.valid_to,
             attributes_json = excluded.attributes_json",
        params![
            entity.id,
            entity.kind,
            entity.country,
            entity.region,
            entity.lifecycle_status,
            entity.reporting_status,
            entity.source_quality,
            request.expires_at,
            serde_json::to_string(&entity.attributes).unwrap_or_else(|_| "{}".to_string()),
        ],
    )?;
    let entity_pk: i64 = transaction.query_row(
        "SELECT entity_pk FROM entities WHERE canonical_id = ?1",
        [&entity.id],
        |row| row.get(0),
    )?;
    transaction.execute(
        "DELETE FROM entity_capabilities WHERE entity_pk = ?1",
        [entity_pk],
    )?;
    transaction.execute("DELETE FROM identifiers WHERE entity_pk = ?1", [entity_pk])?;
    transaction.execute(
        "DELETE FROM names_fts WHERE entity_pk = ?1",
        [entity_pk.to_string()],
    )?;
    transaction.execute(
        "DELETE FROM names_trigram WHERE entity_pk = ?1",
        [entity_pk.to_string()],
    )?;
    transaction.execute("DELETE FROM names WHERE entity_pk = ?1", [entity_pk])?;
    let geometry_ids = collect_i64(
        transaction,
        "SELECT geometry_pk FROM geometries WHERE entity_pk = ?1",
        entity_pk,
    )?;
    for geometry_pk in geometry_ids {
        transaction.execute(
            "DELETE FROM entity_rtree WHERE geometry_pk = ?1",
            [geometry_pk],
        )?;
    }
    transaction.execute("DELETE FROM geometries WHERE entity_pk = ?1", [entity_pk])?;
    for capability in &entity.capabilities {
        transaction.execute(
            "INSERT OR IGNORE INTO entity_capabilities(entity_pk, capability) VALUES(?1, ?2)",
            params![entity_pk, capability],
        )?;
    }
    for identifier in &entity.identifiers {
        transaction.execute(
            "INSERT INTO identifiers(entity_pk, authority, scheme, value, normalized_value,
                                     is_primary, confidence, source_id)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                entity_pk,
                identifier.authority,
                canonical_scheme(&identifier.scheme),
                identifier.value,
                normalize_identifier(&identifier.scheme, &identifier.value),
                i64::from(identifier.primary),
                confidence_name(identifier.confidence),
                request.source_id,
            ],
        )?;
    }
    for name in &entity.names {
        let normalized = normalize_name(&name.value);
        transaction.execute(
            "INSERT INTO names(entity_pk, locale, name, normalized_name, t9_digits,
                               name_kind, is_primary, source_id)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                entity_pk,
                name.locale,
                name.value,
                normalized,
                t9_digits(&name.value),
                name.name_kind,
                i64::from(name.primary),
                request.source_id,
            ],
        )?;
        transaction.execute(
            "INSERT INTO names_fts(name, normalized_name, entity_pk, locale) VALUES(?1, ?2, ?3, ?4)",
            params![name.value, normalized, entity_pk.to_string(), name.locale],
        )?;
        transaction.execute(
            "INSERT INTO names_trigram(normalized_name, entity_pk) VALUES(?1, ?2)",
            params![normalized, entity_pk.to_string()],
        )?;
    }
    if let Some(geometry) = &entity.geometry {
        transaction.execute(
            "INSERT INTO geometries(entity_pk, geometry_type, latitude, longitude,
                                    min_lon, max_lon, min_lat, max_lat, accuracy_m, source_id)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                entity_pk,
                geometry.geometry_type,
                geometry.latitude,
                geometry.longitude,
                geometry.bbox[0],
                geometry.bbox[2],
                geometry.bbox[1],
                geometry.bbox[3],
                geometry.accuracy_m,
                request.source_id,
            ],
        )?;
        let geometry_pk = transaction.last_insert_rowid();
        transaction.execute(
            "INSERT INTO entity_rtree(geometry_pk, min_lon, max_lon, min_lat, max_lat)
             VALUES(?1, ?2, ?3, ?4, ?5)",
            params![
                geometry_pk,
                geometry.bbox[0],
                geometry.bbox[2],
                geometry.bbox[1],
                geometry.bbox[3]
            ],
        )?;
    }
    Ok(())
}

fn collect_i64(connection: &Connection, sql: &str, entity_pk: i64) -> rusqlite::Result<Vec<i64>> {
    let mut statement = connection.prepare(sql)?;
    let values = statement
        .query_map([entity_pk], |row| row.get(0))?
        .collect();
    values
}

fn validate_overlay_entity(entity: &Entity) -> Result<(), CatalogError> {
    if entity.id.trim().is_empty() || !entity.id.starts_with("urn:haze:location:") {
        return Err(CatalogError::Invalid(
            "overlay canonical ID must start with urn:haze:location:".to_string(),
        ));
    }
    if entity.kind.trim().is_empty() {
        return Err(CatalogError::Invalid(
            "overlay entity kind is empty".to_string(),
        ));
    }
    if entity.identifiers.is_empty() || entity.names.is_empty() {
        return Err(CatalogError::Invalid(
            "overlay entity requires at least one identifier and one name".to_string(),
        ));
    }
    if let Some(geometry) = &entity.geometry {
        if let Some((latitude, longitude)) = geometry.latitude.zip(geometry.longitude) {
            if !valid_point(latitude, longitude) {
                return Err(CatalogError::Invalid(
                    "overlay entity has invalid coordinates".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn exact_match(method: &str, pack: &OpenPack) -> MatchInfo {
    MatchInfo {
        score: 1.0,
        confidence: ConfidenceLevel::Exact,
        method: method.to_string(),
        algorithm: ALGORITHM_VERSION.to_string(),
        evidence: BTreeMap::from([
            ("pack".to_string(), json!(pack.id)),
            ("pack_priority".to_string(), json!(pack.priority)),
        ]),
    }
}

fn name_candidate(entity: Entity) -> Candidate {
    Candidate {
        entity,
        match_info: MatchInfo {
            score: 0.0,
            confidence: ConfidenceLevel::Low,
            method: "fuzzy_name".to_string(),
            algorithm: ALGORITHM_VERSION.to_string(),
            evidence: BTreeMap::new(),
        },
        facet: None,
        distance_m: None,
        relationship_path: Vec::new(),
        grouping: None,
    }
}

fn apply_name_score(candidate: &mut Candidate, query: &str, locale: Option<&str>) {
    let score = best_name_similarity(&candidate.entity, query);
    let exact = candidate
        .entity
        .names
        .iter()
        .any(|name| name.normalized_value == query);
    let prefix = candidate
        .entity
        .names
        .iter()
        .any(|name| name.normalized_value.starts_with(query));
    candidate.match_info.score = if exact {
        1.0
    } else if prefix {
        score.max(0.94)
    } else {
        score
    };
    candidate.match_info.confidence = if exact {
        ConfidenceLevel::Exact
    } else if candidate.match_info.score >= 0.92 {
        ConfidenceLevel::High
    } else if candidate.match_info.score >= 0.78 {
        ConfidenceLevel::Medium
    } else {
        ConfidenceLevel::Low
    };
    candidate.match_info.method = if exact {
        "exact_name"
    } else if prefix {
        "name_prefix"
    } else {
        "fuzzy_name"
    }
    .to_string();
    candidate
        .match_info
        .evidence
        .insert("normalized_query".to_string(), json!(query));
    if locale.is_some_and(|wanted| {
        candidate.entity.names.iter().any(|name| {
            name.locale.as_deref().is_some_and(|actual| {
                actual.eq_ignore_ascii_case(wanted)
                    || actual
                        .split('-')
                        .next()
                        .zip(wanted.split('-').next())
                        .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right))
            })
        })
    }) {
        candidate.match_info.score = (candidate.match_info.score + 0.015).min(1.0);
        candidate
            .match_info
            .evidence
            .insert("locale_match".to_string(), json!(locale));
    }
    if !exact {
        let quality_boost = candidate.entity.source_quality.clamp(0.0, 1.0) * 0.01;
        let population = numeric_attribute(
            &candidate.entity.attributes,
            &["population", "pop_max", "pop_min"],
        );
        let population_boost = population
            .map(|value| (value.max(0.0).ln_1p() / 25.0).min(0.01))
            .unwrap_or(0.0);
        candidate.match_info.score =
            (candidate.match_info.score + quality_boost + population_boost).min(0.999);
        candidate.match_info.evidence.insert(
            "source_quality".to_string(),
            json!(candidate.entity.source_quality),
        );
        if let Some(population) = population {
            candidate
                .match_info
                .evidence
                .insert("population".to_string(), json!(population));
        }
        candidate.match_info.confidence = if candidate.match_info.score >= 0.92 {
            ConfidenceLevel::High
        } else if candidate.match_info.score >= 0.78 {
            ConfidenceLevel::Medium
        } else {
            ConfidenceLevel::Low
        };
    }
}

fn numeric_attribute(attributes: &BTreeMap<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|wanted| {
        attributes.iter().find_map(|(key, value)| {
            if !key.eq_ignore_ascii_case(wanted) {
                return None;
            }
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|raw| raw.parse::<f64>().ok()))
        })
    })
}

fn apply_t9_score(candidate: &mut Candidate, query: &str) {
    let exact = candidate
        .entity
        .names
        .iter()
        .any(|name| t9_digits(&name.value) == query);
    candidate.match_info.score = if exact { 0.96 } else { 0.84 };
    candidate.match_info.confidence = if exact {
        ConfidenceLevel::High
    } else {
        ConfidenceLevel::Medium
    };
    candidate.match_info.method = if exact { "t9_exact" } else { "t9_prefix" }.to_string();
    candidate
        .match_info
        .evidence
        .insert("t9_query".to_string(), json!(query));
}

fn apply_geographic_bias(candidate: &mut Candidate, latitude: f64, longitude: f64) {
    let Some((entity_latitude, entity_longitude)) = candidate
        .entity
        .geometry
        .as_ref()
        .and_then(|geometry| geometry.latitude.zip(geometry.longitude))
    else {
        return;
    };
    let distance_m = haversine_m(latitude, longitude, entity_latitude, entity_longitude);
    let boost = (0.05 * (1.0 - distance_m / 1_000_000.0)).clamp(0.0, 0.05);
    candidate.match_info.score = (candidate.match_info.score + boost).min(1.0);
    candidate
        .match_info
        .evidence
        .insert("geographic_bias_distance_m".to_string(), json!(distance_m));
}

fn best_name_similarity(entity: &Entity, query: &str) -> f64 {
    entity
        .names
        .iter()
        .map(|name| jaro_winkler(&name.normalized_value, query))
        .fold(0.0, f64::max)
}

fn spatial_candidate(entity: Entity, distance_m: f64) -> Candidate {
    let score = 1.0 / (1.0 + distance_m / 10_000.0);
    Candidate {
        entity,
        match_info: MatchInfo {
            score,
            confidence: if distance_m <= 10_000.0 {
                ConfidenceLevel::High
            } else if distance_m <= 100_000.0 {
                ConfidenceLevel::Medium
            } else {
                ConfidenceLevel::Low
            },
            method: "spatial_nearest".to_string(),
            algorithm: ALGORITHM_VERSION.to_string(),
            evidence: BTreeMap::from([("distance_m".to_string(), json!(distance_m))]),
        },
        facet: None,
        distance_m: Some(distance_m),
        relationship_path: Vec::new(),
        grouping: None,
    }
}

fn geometry_distance(
    entity: &Entity,
    wkb: Option<&[u8]>,
    latitude: f64,
    longitude: f64,
    point_lat: Option<f64>,
    point_lon: Option<f64>,
) -> f64 {
    if wkb.is_some_and(|bytes| contains_wkb(bytes, longitude, latitude).unwrap_or(false)) {
        return 0.0;
    }
    point_lat
        .zip(point_lon)
        .or_else(|| {
            entity
                .geometry
                .as_ref()?
                .latitude
                .zip(entity.geometry.as_ref()?.longitude)
        })
        .map_or(f64::MAX, |(lat, lon)| {
            haversine_m(latitude, longitude, lat, lon)
        })
}

fn finalize_candidates(
    candidates: Vec<Candidate>,
    filters: &QueryFilters,
    options: &QueryOptions,
) -> Vec<Candidate> {
    let mut merged = HashMap::<String, Candidate>::new();
    for candidate in candidates {
        if !entity_matches(&candidate.entity, filters, options)
            || candidate.match_info.confidence < options.minimum_confidence
        {
            continue;
        }
        match merged.entry(candidate.entity.id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(candidate);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                merge_candidate(entry.get_mut(), candidate);
            }
        }
    }
    let mut out: Vec<_> = merged.into_values().collect();
    out.sort_by(candidate_order);
    out
}

fn merge_candidate(existing: &mut Candidate, incoming: Candidate) {
    let incoming_better = candidate_order(&incoming, existing) == Ordering::Less;
    let mut capabilities: BTreeSet<_> = existing.entity.capabilities.iter().cloned().collect();
    capabilities.extend(incoming.entity.capabilities.iter().cloned());
    let mut identifiers: BTreeMap<_, _> = existing
        .entity
        .identifiers
        .iter()
        .cloned()
        .map(|value| {
            (
                (
                    value.authority.clone(),
                    value.scheme.clone(),
                    value.normalized_value.clone(),
                ),
                value,
            )
        })
        .collect();
    for value in incoming.entity.identifiers.iter().cloned() {
        identifiers
            .entry((
                value.authority.clone(),
                value.scheme.clone(),
                value.normalized_value.clone(),
            ))
            .or_insert(value);
    }
    let mut names: BTreeMap<_, _> = existing
        .entity
        .names
        .iter()
        .cloned()
        .map(|value| {
            (
                (value.locale.clone(), value.normalized_value.clone()),
                value,
            )
        })
        .collect();
    for value in incoming.entity.names.iter().cloned() {
        names
            .entry((value.locale.clone(), value.normalized_value.clone()))
            .or_insert(value);
    }
    if incoming_better {
        let mut replacement = incoming;
        replacement.entity.capabilities = capabilities.into_iter().collect();
        replacement.entity.identifiers = identifiers.into_values().collect();
        replacement.entity.names = names.into_values().collect();
        *existing = replacement;
    } else {
        existing.entity.capabilities = capabilities.into_iter().collect();
        existing.entity.identifiers = identifiers.into_values().collect();
        existing.entity.names = names.into_values().collect();
    }
}

fn entity_matches(entity: &Entity, filters: &QueryFilters, options: &QueryOptions) -> bool {
    if !options.include_inactive
        && matches!(entity.lifecycle_status.as_str(), "inactive" | "retired")
    {
        return false;
    }
    if !filters.kinds.is_empty()
        && !filters
            .kinds
            .iter()
            .any(|kind| kind.eq_ignore_ascii_case(&entity.kind))
    {
        return false;
    }
    if !filters.capabilities.is_empty()
        && !filters.capabilities.iter().all(|wanted| {
            entity
                .capabilities
                .iter()
                .any(|actual| actual.eq_ignore_ascii_case(wanted))
        })
    {
        return false;
    }
    if filters.country.as_deref().is_some_and(|country| {
        !entity
            .country
            .as_deref()
            .is_some_and(|actual| actual.eq_ignore_ascii_case(country))
    }) {
        return false;
    }
    if filters.region.as_deref().is_some_and(|region| {
        !entity
            .region
            .as_deref()
            .is_some_and(|actual| actual.eq_ignore_ascii_case(region))
    }) {
        return false;
    }
    match options.station_mode_requirement {
        StationModeRequirement::Any => true,
        StationModeRequirement::Auto => entity.station_mode() == Some(StationMode::Auto),
        StationModeRequirement::Manual => entity.station_mode() == Some(StationMode::Manual),
    }
}

fn candidate_order(left: &Candidate, right: &Candidate) -> Ordering {
    left.distance_m
        .partial_cmp(&right.distance_m)
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            right
                .match_info
                .score
                .partial_cmp(&left.match_info.score)
                .unwrap_or(Ordering::Equal)
        })
        // Immutable, operator-selected catalog packs are authoritative for
        // geographic identity. Runtime overlay discoveries can enrich an
        // entity, but must never displace an authoritative boundary or point.
        .then_with(|| candidate_pack_priority(right).cmp(&candidate_pack_priority(left)))
        .then_with(|| {
            right
                .entity
                .source_quality
                .partial_cmp(&left.entity.source_quality)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| left.entity.id.cmp(&right.entity.id))
}

fn candidate_pack_priority(candidate: &Candidate) -> i64 {
    candidate
        .match_info
        .evidence
        .get("pack_priority")
        .and_then(Value::as_i64)
        .unwrap_or(i64::MIN)
}

pub fn apply_station_preference(candidates: &mut [Candidate], preference: Option<StationMode>) {
    let Some(preference) = preference else {
        return;
    };
    candidates.sort_by(|left, right| {
        let left_exact = left.match_info.confidence == ConfidenceLevel::Exact;
        let right_exact = right.match_info.confidence == ConfidenceLevel::Exact;
        let left_preferred = left.entity.station_mode() == Some(preference);
        let right_preferred = right.entity.station_mode() == Some(preference);
        right_exact.cmp(&left_exact).then_with(|| {
            right_preferred
                .cmp(&left_preferred)
                .then_with(|| candidate_order(left, right))
        })
    });
}

fn stable_id(key: &str) -> String {
    format!(
        "urn:haze:location:{}",
        Uuid::new_v5(&LOCATION_NAMESPACE, key.as_bytes())
    )
}

fn canonical_scheme(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "iata_id" => "eccc_station".to_string(),
        "icao_id" => "icao".to_string(),
        "wmo_id" => "wmo".to_string(),
        "msc_id" => "msc".to_string(),
        "same" => "same".to_string(),
        "county_fips" => "fips".to_string(),
        "nws_county" => "nws_ugc_county".to_string(),
        "zip" | "postal_code" => "postal".to_string(),
        other => other.to_string(),
    }
}

fn legacy_source_for_scheme(scheme: &str) -> Option<&str> {
    match scheme {
        "clc" => Some("clc"),
        "sgc" => Some("sgc"),
        "hello_weather" => Some("hello_weather"),
        "eccc_citypage" | "forecast" => Some("forecast"),
        "nws_zone" | "nws_ugc" => Some("nws_zone"),
        "nws_marine_zone" => Some("nws_marine_zone"),
        "same" | "fips" => Some("nws_same"),
        "nws_marine_same" => Some("nws_marine_same"),
        "station" => Some("station"),
        _ => None,
    }
}

fn legacy_authority(source: &str) -> &str {
    match source {
        "clc" | "sgc" | "forecast" | "hello_weather" | "station" => "eccc",
        value if value.starts_with("nws_") => "nws",
        _ => "legacy",
    }
}

fn legacy_kind_name(source: &str, kind: &str) -> String {
    let lower = kind.to_ascii_lowercase();
    match source {
        "station" => "weather_station",
        "clc" => "forecast_zone",
        "sgc" => "administrative_area",
        "forecast" | "hello_weather" => "forecast_location",
        "nws_marine_zone" => "marine_forecast_zone",
        "nws_marine_same" => "marine_alert_area",
        "nws_same" => "alert_area",
        "nws_zone" if lower.contains("county") => "county",
        "nws_zone" => "forecast_zone",
        _ => "location",
    }
    .to_string()
}

fn capabilities_for_kind(kind: &str) -> Vec<String> {
    match kind {
        "weather_station" => vec!["observation.surface"],
        "marine_station" | "marine_buoy" => vec!["observation.marine"],
        "forecast_zone" | "forecast_location" => vec!["forecast.public"],
        "marine_forecast_zone" => vec!["forecast.marine"],
        "alert_area" | "marine_alert_area" | "county" => vec!["alerts"],
        "air_quality_station" => vec!["air_quality"],
        "airport" => vec!["aviation"],
        _ => Vec::new(),
    }
    .into_iter()
    .map(ToOwned::to_owned)
    .collect()
}

fn legacy_relationship_type(link_type: &str) -> &str {
    if link_type.contains("to") {
        "crosswalk"
    } else {
        link_type
    }
}

fn facet_for_kind(kind: &str) -> &str {
    match kind {
        "weather_station"
        | "marine_station"
        | "marine_buoy"
        | "climate_station"
        | "hydrometric_station" => "station",
        "airport" => "airport",
        "air_quality_station" => "air_quality",
        "forecast_zone"
        | "forecast_location"
        | "public_forecast_zone"
        | "nws_public_zone"
        | "nws_fire_zone" => "forecast",
        "marine_forecast_zone" | "nws_marine_zone" => "marine",
        "alert_area" | "marine_alert_area" | "nws_alert_zone" => "alert",
        "administrative_area" | "county" | "province" | "state" => "administration",
        "place" | "locality" | "city" | "postal_area" => "locality",
        _ => "other",
    }
}

fn parse_attributes(raw: &str) -> BTreeMap<String, Value> {
    serde_json::from_str::<Map<String, Value>>(raw)
        .unwrap_or_default()
        .into_iter()
        .collect()
}

fn non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn station_mode_name(mode: StationMode) -> &'static str {
    match mode {
        StationMode::Auto => "auto",
        StationMode::Manual => "manual",
    }
}

fn confidence_name(level: ConfidenceLevel) -> &'static str {
    match level {
        ConfidenceLevel::Exact => "exact",
        ConfidenceLevel::High => "high",
        ConfidenceLevel::Medium => "medium",
        ConfidenceLevel::Low => "low",
    }
}

fn fts_phrase(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn overlay_accepts_zero_coordinate() {
        let directory = tempdir().expect("temp dir");
        let path = directory.path().join("overlay.sqlite");
        initialize_overlay(&path).expect("overlay schema");
        let connection = Connection::open(path).expect("overlay open");
        assert_eq!(
            connection
                .query_row::<i64, _, _>("SELECT COUNT(*) FROM entities", [], |row| row.get(0))
                .expect("count"),
            0
        );
    }

    #[test]
    fn validates_normalized_pack_schema() {
        let directory = tempdir().expect("temp dir");
        let path = directory.path().join("pack.sqlite");
        let connection = Connection::open(&path).expect("pack open");
        connection.execute_batch(OVERLAY_SCHEMA).expect("schema");
        connection
            .execute(
                "INSERT INTO sources(source_id, title) VALUES('fixture', 'Fixture')",
                [],
            )
            .expect("source");
        connection
            .execute(
                "INSERT INTO entities(canonical_id, kind) VALUES('urn:haze:location:fixture', 'place')",
                [],
            )
            .expect("entity");
        connection
            .execute(
                "INSERT INTO identifiers(entity_pk, authority, scheme, value, normalized_value)
                 VALUES(1, 'fixture', 'fixture', '0001', '0001')",
                [],
            )
            .expect("identifier");
        connection
            .execute(
                "INSERT INTO names(entity_pk, name, normalized_name, t9_digits, is_primary)
                 VALUES(1, 'Zero Island', 'zero island', '9376475263', 1)",
                [],
            )
            .expect("name");
        connection
            .execute(
                "INSERT INTO names_fts(name, normalized_name, entity_pk)
                 VALUES('Zero Island', 'zero island', '1')",
                [],
            )
            .expect("fts");
        connection
            .execute(
                "INSERT INTO names_trigram(normalized_name, entity_pk) VALUES('zero island', '1')",
                [],
            )
            .expect("trigram");
        connection
            .execute(
                "INSERT INTO geometries(entity_pk, geometry_type, geometry_wkb, latitude, longitude,
                                         min_lon, max_lon, min_lat, max_lat)
                 VALUES(1, 'point', X'010100000000000000000000000000000000000000', 0, 0, 0, 0, 0, 0)",
                [],
            )
            .expect("geometry");
        connection
            .execute("INSERT INTO entity_rtree VALUES(1, 0, 0, 0, 0)", [])
            .expect("rtree");
        for (table, count) in [
            ("entities", "1"),
            ("identifiers", "1"),
            ("names", "1"),
            ("geometries", "1"),
        ] {
            connection
                .execute(
                    "INSERT INTO catalog_metadata(key, value) VALUES(?1, ?2)",
                    params![format!("count.{table}"), count],
                )
                .expect("count metadata");
        }
        connection
            .execute(
                "INSERT INTO catalog_metadata(key, value) VALUES('count.source.fixture', '1')",
                [],
            )
            .expect("source count metadata");
        let identifier_search =
            normalized_unqualified_identifier_candidates(&connection, "0001", 10, None)
                .expect("identifier search");
        assert_eq!(identifier_search.len(), 1);
        assert_eq!(identifier_search[0].match_info.method, "exact_identifier");
        drop(connection);
        let checksum = file_sha256(&path).expect("checksum");
        fs::write(
            path.with_extension("sqlite.sha256"),
            format!("{checksum}  pack.sqlite\n"),
        )
        .expect("checksum sidecar");
        let pack = PackConfig {
            id: "test".to_string(),
            path,
            priority: 0,
            format: PackFormat::Normalized,
            required: true,
        };
        validate_pack(&pack).expect("valid pack");
        fs::remove_file(pack.path).expect("remove fixture");
    }

    #[test]
    fn legacy_entity_exposes_managed_area_geometry() {
        let connection = Connection::open_in_memory().expect("legacy database");
        connection
            .execute_batch(
                "CREATE TABLE places(
                    source TEXT, code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT,
                    kind TEXT, lat REAL, lon REAL, attrs_json TEXT
                 );
                 CREATE TABLE area_geometries(
                    source TEXT, code TEXT, same_code TEXT, provider_version TEXT, source_url TEXT,
                    updated_at TEXT, min_lon REAL, min_lat REAL, max_lon REAL, max_lat REAL,
                    geometry_json TEXT
                 );
                 CREATE TABLE links(
                    link_type TEXT, from_source TEXT, from_code TEXT, to_source TEXT, to_code TEXT,
                    score REAL, confidence TEXT, distance_km REAL, method TEXT, components_json TEXT
                 );
                 INSERT INTO places VALUES(
                    'clc', '065100', 'City of Saskatoon', 'Ville de Saskatoon', 'SK', 'CA',
                    'land', 52.13, -106.67, '{}'
                 );
                 INSERT INTO places VALUES(
                    'sgc', '4711066', 'Saskatoon', '', 'SK', 'CA',
                    'administrative', 52.13, -106.67, '{}'
                 );
                 INSERT INTO area_geometries VALUES(
                    'clc', '065100', '065100', '6.15.0', 'https://example.invalid/source',
                    '2026-08-02T00:00:00Z', -107, 52, -106, 53,
                    '{\"type\":\"Polygon\",\"coordinates\":[[[-107,52],[-106,52],[-106,53],[-107,52]]]}'
                 );
                 INSERT INTO links VALUES(
                    'sgc_to_clc', 'sgc', '4711066', 'clc', '065100',
                    0.94, 'high', 0, 'fixture', '{}'
                 );",
            )
            .expect("legacy schema");
        let entity = load_legacy_entity(&connection, "clc", "065100")
            .expect("legacy query")
            .expect("legacy entity");
        assert_eq!(entity.attributes["sgc_codes"], json!(["4711066"]));
        assert!(!entity.attributes.contains_key("area_geometry"));
        let worker = WorkerCatalog {
            generation: "fixture".to_string(),
            packs: vec![OpenPack {
                id: "legacy".to_string(),
                priority: 10,
                format: PackFormat::Legacy,
                connection,
                legacy_name_index: None,
            }],
            geometry_packs: Vec::new(),
            feed_bound_entities: HashSet::new(),
        };
        let options = QueryOptions {
            include_area_geometry: true,
            ..QueryOptions::default()
        };
        let candidates = worker
            .finalize(
                vec![Candidate {
                    entity,
                    match_info: exact_match("fixture", &worker.packs[0]),
                    facet: None,
                    distance_m: None,
                    relationship_path: Vec::new(),
                    grouping: None,
                }],
                &QueryFilters::default(),
                &options,
            )
            .expect("geometry enrichment");
        let entity = &candidates[0].entity;
        assert_eq!(entity.attributes["provider_version"], json!("6.15.0"));
        assert_eq!(entity.attributes["same_code"], json!("065100"));
        assert_eq!(entity.attributes["area_geometry"]["type"], json!("Polygon"));
    }

    #[test]
    fn external_geometry_pack_enriches_and_contains_legacy_entity_on_request() {
        let directory = tempdir().expect("temp dir");
        let core_path = directory.path().join("core.sqlite");
        let geometry_path = directory.path().join("geometry.sqlite");
        let overlay_path = directory.path().join("state/overlay.sqlite");
        write_legacy_core(&core_path);
        let core_checksum = file_sha256(&core_path).expect("core checksum");
        write_geometry_pack(
            &geometry_path,
            "boundaries",
            "core",
            &core_checksum,
            "6.15.0",
        );
        let config = split_test_config(core_path, geometry_path, overlay_path, "boundaries");
        initialize_overlay(&config.overlay_path).expect("overlay");
        let snapshot =
            pin_snapshot(validate_snapshot(&config).expect("snapshot")).expect("pinned snapshot");
        assert!(snapshot.packs[0].path.exists());
        assert!(snapshot.geometry_packs[0].path.exists());
        assert_eq!(
            snapshot.packs[0].path.parent(),
            snapshot.geometry_packs[0].path.parent()
        );
        let mut worker = WorkerCatalog::new();
        worker.prepare(&snapshot).expect("worker catalog");

        let hidden = worker
            .resolve_identifier(
                "clc",
                Some("eccc"),
                "065100",
                &QueryFilters::default(),
                &QueryOptions::default(),
            )
            .expect("core-only result");
        assert!(!hidden[0].entity.attributes.contains_key("area_geometry"));

        let options = QueryOptions {
            limit: 10,
            include_area_geometry: true,
            ..QueryOptions::default()
        };
        let enriched = worker
            .resolve_identifier(
                "clc",
                Some("eccc"),
                "065100",
                &QueryFilters::default(),
                &options,
            )
            .expect("enriched result");
        assert_eq!(
            enriched[0].entity.attributes["area_geometry"]["type"],
            json!("Polygon")
        );
        assert_eq!(
            enriched[0].entity.attributes["provider_version"],
            json!("6.15.0")
        );

        let facets = worker
            .point_facets(52.25, -106.75, &QueryFilters::default(), &options)
            .expect("external containment");
        assert!(facets.iter().any(|candidate| {
            candidate.entity.id == stable_id("legacy:clc:065100")
                && candidate.match_info.method == "spatial_contains"
        }));
    }

    #[test]
    fn geometry_checksum_changes_generation_and_pins_complete_set() {
        let directory = tempdir().expect("temp dir");
        let core_path = directory.path().join("core.sqlite");
        let first_geometry = directory.path().join("geometry-one.sqlite");
        let second_geometry = directory.path().join("geometry-two.sqlite");
        write_legacy_core(&core_path);
        let core_checksum = file_sha256(&core_path).expect("core checksum");
        write_geometry_pack(&first_geometry, "boundaries", "core", &core_checksum, "one");
        write_geometry_pack(
            &second_geometry,
            "boundaries",
            "core",
            &core_checksum,
            "two",
        );
        let mut config = split_test_config(
            core_path,
            first_geometry,
            directory.path().join("state/overlay.sqlite"),
            "boundaries",
        );
        initialize_overlay(&config.overlay_path).expect("overlay");
        let first =
            pin_snapshot(validate_snapshot(&config).expect("first snapshot")).expect("first pin");
        config.geometry_packs[0].path = second_geometry;
        let second =
            pin_snapshot(validate_snapshot(&config).expect("second snapshot")).expect("second pin");
        assert_ne!(first.generation, second.generation);
        assert_eq!(first.pack_ids, vec!["core", "boundaries"]);
        assert_eq!(second.pack_ids, vec!["core", "boundaries"]);
        assert!(first.packs[0].path.starts_with(
            directory
                .path()
                .join("state/location-catalog-generations")
                .join(&first.generation)
        ));
        assert!(first.geometry_packs[0].path.starts_with(
            directory
                .path()
                .join("state/location-catalog-generations")
                .join(&first.generation)
        ));
    }

    #[test]
    fn geometry_pack_rejects_a_different_core_generation() {
        let directory = tempdir().expect("temp dir");
        let core_path = directory.path().join("core.sqlite");
        let geometry_path = directory.path().join("geometry.sqlite");
        write_legacy_core(&core_path);
        write_geometry_pack(
            &geometry_path,
            "boundaries",
            "core",
            &"0".repeat(64),
            "stale",
        );
        let config = split_test_config(
            core_path,
            geometry_path,
            directory.path().join("state/overlay.sqlite"),
            "boundaries",
        );

        let error = validate_snapshot(&config).expect_err("mismatched core generation");
        assert!(error.to_string().contains("was built for core SHA-256"));
    }

    #[test]
    fn legacy_nws_locations_expose_same_fips_county_and_zone_codes() {
        let connection = Connection::open_in_memory().expect("legacy database");
        connection
            .execute_batch(
                "CREATE TABLE places(
                    source TEXT, code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT,
                    kind TEXT, lat REAL, lon REAL, attrs_json TEXT
                 );
                 CREATE TABLE links(
                    link_type TEXT, from_source TEXT, from_code TEXT, to_source TEXT, to_code TEXT,
                    score REAL, confidence TEXT, distance_km REAL, method TEXT, components_json TEXT
                 );
                 INSERT INTO places VALUES(
                    'nws_same', '020001', 'Allen, KS', '', 'KS', 'US',
                    'county', 38.0, -95.0, '{\"fips\":\"20001\",\"same\":\"020001\"}'
                 );
                 INSERT INTO places VALUES(
                    'nws_zone', 'KSZ072', 'Allen County', '', 'KS', 'US',
                    'zone', 38.0, -95.0, '{}'
                 );
                 INSERT INTO links VALUES(
                    'nws_same_to_zone', 'nws_same', '020001', 'nws_zone', 'KSZ072',
                    1.0, 'exact', 0, 'fixture', '{}'
                 );",
            )
            .expect("legacy schema");
        let entity = load_legacy_entity(&connection, "nws_same", "020001")
            .expect("legacy query")
            .expect("legacy entity");
        assert_eq!(entity.attributes["same_code"], json!("020001"));
        assert_eq!(entity.attributes["fips_codes"], json!(["20001"]));
        assert_eq!(entity.attributes["nws_county_codes"], json!(["KSC001"]));
        assert_eq!(entity.attributes["nws_zone_codes"], json!(["KSZ072"]));
    }

    #[test]
    fn station_preference_never_displaces_an_exact_hit() {
        let mut candidates = vec![
            station_candidate("manual-exact", "manual", ConfidenceLevel::Exact, 1.0),
            station_candidate("auto-high", "auto", ConfidenceLevel::High, 0.99),
        ];
        apply_station_preference(&mut candidates, Some(StationMode::Auto));
        assert_eq!(candidates[0].entity.id, "manual-exact");

        candidates[0].match_info.confidence = ConfidenceLevel::High;
        apply_station_preference(&mut candidates, Some(StationMode::Auto));
        assert_eq!(candidates[0].entity.id, "auto-high");
    }

    #[test]
    fn station_requirement_filters_before_ranking() {
        let auto = station_candidate("auto", "auto", ConfidenceLevel::High, 0.8);
        let manual = station_candidate("manual", "manual", ConfidenceLevel::Exact, 1.0);
        let options = QueryOptions {
            station_mode_requirement: StationModeRequirement::Auto,
            ..QueryOptions::default()
        };
        let filtered = finalize_candidates(vec![manual, auto], &QueryFilters::default(), &options);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].entity.id, "auto");
    }

    #[test]
    fn managed_catalog_geometry_outranks_a_runtime_overlay_claim() {
        let id = "urn:haze:location:fixture";
        let mut managed = station_candidate(id, "manual", ConfidenceLevel::Exact, 1.0);
        managed.entity.kind = "forecast_zone".to_string();
        managed.entity.source_quality = 0.7;
        managed.entity.attributes.insert(
            "area_geometry".to_string(),
            json!({
                "type": "Polygon",
                "coordinates": [[[-107.0, 52.0], [-106.0, 52.0], [-106.0, 53.0], [-107.0, 52.0]]]
            }),
        );
        managed
            .match_info
            .evidence
            .insert("pack_priority".to_string(), json!(10));

        let mut overlay = station_candidate(id, "manual", ConfidenceLevel::Exact, 1.0);
        overlay.entity.kind = "public_forecast_location".to_string();
        overlay.entity.source_quality = 0.85;
        overlay
            .match_info
            .evidence
            .insert("pack_priority".to_string(), json!(i32::MIN));

        let results = finalize_candidates(
            vec![overlay, managed],
            &QueryFilters::default(),
            &QueryOptions::default(),
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].entity.kind, "forecast_zone");
        assert_eq!(
            results[0].entity.attributes["area_geometry"]["type"],
            json!("Polygon")
        );
    }

    fn station_candidate(
        id: &str,
        mode: &str,
        confidence: ConfidenceLevel,
        score: f64,
    ) -> Candidate {
        Candidate {
            entity: Entity {
                id: id.to_string(),
                kind: "weather_station".to_string(),
                capabilities: vec!["surface_observation".to_string()],
                country: Some("CA".to_string()),
                region: Some("SK".to_string()),
                lifecycle_status: "active".to_string(),
                reporting_status: "recently_reporting".to_string(),
                source_quality: 0.95,
                identifiers: Vec::new(),
                names: Vec::new(),
                geometry: None,
                deployments: Vec::new(),
                attributes: BTreeMap::from([("station_mode".to_string(), json!(mode))]),
            },
            match_info: MatchInfo {
                score,
                confidence,
                method: "fixture".to_string(),
                algorithm: ALGORITHM_VERSION.to_string(),
                evidence: BTreeMap::new(),
            },
            facet: None,
            distance_m: None,
            relationship_path: Vec::new(),
            grouping: None,
        }
    }

    fn split_test_config(
        core_path: PathBuf,
        geometry_path: PathBuf,
        overlay_path: PathBuf,
        geometry_id: &str,
    ) -> ServiceConfig {
        ServiceConfig {
            rollout_mode: "test".to_string(),
            workers: 1,
            queue_size: 1,
            default_limit: 10,
            maximum_limit: 100,
            packs: vec![PackConfig {
                id: "core".to_string(),
                path: core_path,
                priority: 10,
                format: PackFormat::Legacy,
                required: true,
            }],
            geometry_packs: vec![GeometryPackConfig {
                id: geometry_id.to_string(),
                path: geometry_path,
                core_id: "core".to_string(),
                priority: 10,
                format: PackFormat::Legacy,
                required: true,
            }],
            overlay_path,
            allowed_overlay_sources: Vec::new(),
            feed_bindings_path: None,
            force_groups: Vec::new(),
            never_groups: Vec::new(),
        }
    }

    fn write_legacy_core(path: &Path) {
        let connection = Connection::open(path).expect("legacy core");
        connection
            .execute_batch(
                "CREATE TABLE places(
                    source TEXT, code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT,
                    kind TEXT, lat REAL, lon REAL, attrs_json TEXT
                 );
                 INSERT INTO places VALUES(
                    'clc', '065100', 'City of Saskatoon', 'Ville de Saskatoon', 'SK', 'CA',
                    'land', 52.13, -106.67, '{}'
                 );",
            )
            .expect("legacy schema");
    }

    #[test]
    fn legacy_name_search_is_bounded_at_catalog_scale() {
        let mut connection = Connection::open_in_memory().expect("legacy catalog");
        connection
            .execute_batch(
                "CREATE TABLE places(
                    source TEXT, code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT,
                    kind TEXT, lat REAL, lon REAL, attrs_json TEXT
                 );",
            )
            .expect("legacy schema");
        let transaction = connection.transaction().expect("transaction");
        {
            let mut insert = transaction
                .prepare(
                    "INSERT INTO places VALUES(?1, ?2, ?3, '', 'SK', 'CA', 'land', 50, -105, '{}')",
                )
                .expect("insert");
            for index in 0..20_124 {
                insert
                    .execute(params![
                        "station",
                        format!("{index:06}"),
                        format!("Fixture Place {index}")
                    ])
                    .expect("decoy");
            }
            insert
                .execute(params!["station", "CYXE", "Saskatoon Airport"])
                .expect("airport");
            insert
                .execute(params!["station", "CPOX", "SASKATOON RCS"])
                .expect("rcs");
        }
        transaction.commit().expect("commit");

        let index = LegacyNameIndex::load(&connection).expect("legacy name index");
        let started = Instant::now();
        let candidates = legacy_name_candidates(
            &connection,
            &index,
            "saskatoon",
            10,
            false,
            &QueryFilters::default(),
            &QueryOptions::default(),
        )
        .expect("legacy search");
        let elapsed = started.elapsed();
        eprintln!("20,126-place legacy search completed in {elapsed:?}");
        let names: Vec<_> = candidates
            .iter()
            .map(|candidate| candidate.entity.display_name())
            .collect();
        assert!(names.contains(&"Saskatoon Airport"));
        assert!(names.contains(&"SASKATOON RCS"));
        assert_eq!(candidates.len(), 2);
        assert_eq!(index.ranked("saskatoon", 10, false).len(), 2);
        assert_eq!(
            index.ranked("fixture", usize::MAX, false).len(),
            MAX_SEARCH_CANDIDATES
        );
        let broad_pool = index.candidate_indices("fixture", 10, false);
        assert_eq!(broad_pool.len(), index.candidate_pool_limit(10));
        assert!(broad_pool.len() < index.entries.len());
        let typo_codes: Vec<_> = index
            .ranked("saskaton", 2, false)
            .into_iter()
            .map(|entry| entry.code.as_str())
            .collect();
        assert!(typo_codes.contains(&"CYXE"));
        assert!(typo_codes.contains(&"CPOX"));
        let t9_codes: Vec<_> = index
            .ranked("727528666", 2, true)
            .into_iter()
            .map(|entry| entry.code.as_str())
            .collect();
        assert!(t9_codes.contains(&"CYXE"));
        assert!(t9_codes.contains(&"CPOX"));
        let fuzzy_t9_codes: Vec<_> = index
            .ranked("727528660", 2, true)
            .into_iter()
            .map(|entry| entry.code.as_str())
            .collect();
        assert!(fuzzy_t9_codes.contains(&"CYXE"));
        assert!(fuzzy_t9_codes.contains(&"CPOX"));
        assert!(
            elapsed < Duration::from_secs(2),
            "legacy name search took {elapsed:?}"
        );
    }

    #[test]
    fn legacy_name_index_applies_filters_before_candidate_truncation() {
        let mut connection = Connection::open_in_memory().expect("legacy catalog");
        connection
            .execute_batch(
                "CREATE TABLE places(
                    source TEXT, code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT,
                    kind TEXT, lat REAL, lon REAL, attrs_json TEXT
                 );",
            )
            .expect("legacy schema");
        let transaction = connection.transaction().expect("transaction");
        {
            let mut insert = transaction
                .prepare(
                    "INSERT INTO places VALUES('clc', ?1, 'Washington', '', ?2, 'US',
                                                'land', 38, -77, '{}')",
                )
                .expect("insert");
            for index in 0..30 {
                let region = if index == 25 { "MD" } else { "WA" };
                insert
                    .execute(params![format!("{index:06}"), region])
                    .expect("homonym");
            }
        }
        transaction.commit().expect("commit");

        let index = LegacyNameIndex::load(&connection).expect("legacy name index");
        let filters = QueryFilters {
            region: Some("MD".to_string()),
            ..QueryFilters::default()
        };
        let candidates = legacy_name_candidates(
            &connection,
            &index,
            "washington",
            1,
            false,
            &filters,
            &QueryOptions::default(),
        )
        .expect("filtered legacy search");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].entity.region.as_deref(), Some("MD"));
    }

    #[test]
    fn legacy_name_index_preserves_unicode_and_localized_lookup() {
        let connection = Connection::open_in_memory().expect("legacy catalog");
        connection
            .execute_batch(
                "CREATE TABLE places(
                    source TEXT, code TEXT, name TEXT, name_fr TEXT, region TEXT, country TEXT,
                    kind TEXT, lat REAL, lon REAL, attrs_json TEXT
                 );
                 INSERT INTO places VALUES(
                    'station', 'CYUL', 'Montreal Airport', 'Aeroport de Montreal', 'QC', 'CA',
                    'land', 45.47, -73.74, '{}'
                 );",
            )
            .expect("legacy schema");
        let index = LegacyNameIndex::load(&connection).expect("legacy name index");

        let english = index.ranked(&normalize_name("Montréal Airport"), 1, false);
        let french = index.ranked(&normalize_name("Aéroport de Montréal"), 1, false);
        let localized_prefix = index.ranked(&normalize_name("Aéroport de Mont"), 1, false);
        let misspelled = index.ranked(&normalize_name("Montrel Airport"), 1, false);

        assert_eq!(english[0].code, "CYUL");
        assert_eq!(french[0].code, "CYUL");
        assert_eq!(localized_prefix[0].code, "CYUL");
        assert_eq!(misspelled[0].code, "CYUL");
    }

    #[test]
    fn legacy_name_retrieval_supports_t9() {
        let exact = legacy_normalized_name_retrieval_score("727528666", "", "727528666");
        let unrelated = legacy_normalized_name_retrieval_score("734462", "", "727528666");
        assert_eq!(exact, 1.0);
        assert!(exact > unrelated);
    }

    fn write_geometry_pack(
        path: &Path,
        pack_id: &str,
        core_id: &str,
        core_checksum: &str,
        version: &str,
    ) {
        let connection = Connection::open(path).expect("geometry pack");
        connection.execute_batch(GEOMETRY_SCHEMA).expect("schema");
        connection
            .execute(
                "INSERT INTO sources(source_id, title, source_version) VALUES('fixture', 'Fixture', ?1)",
                [version],
            )
            .expect("source");
        for (key, value) in [
            ("pack_kind", "geometry"),
            ("pack_id", pack_id),
            ("core_pack_id", core_id),
            ("core_sha256", core_checksum),
            ("schema_version", "1"),
            ("count.geometries", "1"),
            ("count.source.fixture", "1"),
        ] {
            connection
                .execute(
                    "INSERT INTO catalog_metadata(key, value) VALUES(?1, ?2)",
                    params![key, value],
                )
                .expect("metadata");
        }
        let wkb = test_polygon_wkb();
        connection
            .execute(
                "INSERT INTO area_geometries(
                    geometry_pk, canonical_id, source, code, same_code, geometry_type,
                    geometry_wkb, latitude, longitude, min_lon, max_lon, min_lat, max_lat,
                    source_id, provider_version, source_url, updated_at
                 ) VALUES(1, ?1, 'clc', '065100', '065100', 'polygon', ?2, 52.13, -106.67,
                          -107, -106, 52, 53, 'fixture', ?3, 'https://example.invalid',
                          '2026-08-02T00:00:00Z')",
                params![stable_id("legacy:clc:065100"), wkb, version],
            )
            .expect("geometry");
        connection
            .execute(
                "INSERT INTO area_geometry_rtree VALUES(1, -107, -106, 52, 53)",
                [],
            )
            .expect("rtree");
    }

    fn test_polygon_wkb() -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&3_u32.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&4_u32.to_le_bytes());
        for (longitude, latitude) in [
            (-107.0_f64, 52.0_f64),
            (-106.0, 52.0),
            (-106.0, 53.0),
            (-107.0, 52.0),
        ] {
            bytes.extend_from_slice(&longitude.to_le_bytes());
            bytes.extend_from_slice(&latitude.to_le_bytes());
        }
        bytes
    }
}
