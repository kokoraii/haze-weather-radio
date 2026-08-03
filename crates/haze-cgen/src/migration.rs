//! Explicit, reversible migration of legacy CGEN managed configuration.
//!
//! Migration deliberately operates on XML events instead of deserializing the
//! complete document. This keeps unknown operator-managed elements and
//! environment placeholders intact while adding only the schema version 2
//! defaults that have unambiguous legacy behavior.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use chrono::Utc;
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use quick_xml::{Reader, Writer};
use thiserror::Error;

use crate::scene::{protected_default_scene, ProtectedSceneKind};

const LEGACY_SCHEMA_VERSION: u16 = 1;
const TARGET_SCHEMA_VERSION: u16 = 2;
const MAX_MIGRATION_XML_BYTES: u64 = 4 * 1024 * 1024;
const BACKUP_DIRECTORY: &str = "runtime/backups/cgen";
const SCENE_DIRECTORY: &str = "managed/cgen/scenes";

const ANCILLARY_DEFAULTS: [(&str, &str); 3] = [
    ("captions", "drop"),
    ("scte35", "drop"),
    ("scte104", "drop"),
];
const COMPOSITOR_DEFAULTS: [(&str, &str); 2] =
    [("alert_scene_id", "Standard_Crawl"), ("engine", "scene_v2")];
fn audio_defaults(force_layout: &str) -> [(&str, &str); 6] {
    [
        ("topology", "force_layout"),
        ("force_layout", force_layout),
        ("idle_program_gain_db", "0"),
        ("alert_program_gain_db", "muted"),
        ("alert_gain_db", "0"),
        ("transition_ms", "20"),
    ]
}

/// Selects whether a migration is inspected or committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MigrationMode {
    DryRun,
    Apply,
}

/// Count-only migration details that are safe to print in operator logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationReport {
    pub(crate) source_schema_version: u16,
    pub(crate) target_schema_version: u16,
    pub(crate) config_changed: bool,
    pub(crate) feeds_examined: usize,
    pub(crate) feeds_changed: usize,
    pub(crate) alert_routes_added: usize,
    pub(crate) alert_routes_normalized: usize,
    pub(crate) ancillary_sections_added: usize,
    pub(crate) ancillary_sections_augmented: usize,
    pub(crate) compositor_sections_added: usize,
    pub(crate) compositor_sections_augmented: usize,
    pub(crate) audio_sections_added: usize,
    pub(crate) audio_sections_augmented: usize,
    pub(crate) protected_scenes_missing_before_apply: Vec<String>,
    pub(crate) protected_scenes_seeded: Vec<String>,
    pub(crate) backup_path: Option<PathBuf>,
}

impl MigrationReport {
    fn new(source_schema_version: u16) -> Self {
        Self {
            source_schema_version,
            target_schema_version: TARGET_SCHEMA_VERSION,
            config_changed: false,
            feeds_examined: 0,
            feeds_changed: 0,
            alert_routes_added: 0,
            alert_routes_normalized: 0,
            ancillary_sections_added: 0,
            ancillary_sections_augmented: 0,
            compositor_sections_added: 0,
            compositor_sections_augmented: 0,
            audio_sections_added: 0,
            audio_sections_augmented: 0,
            protected_scenes_missing_before_apply: Vec::new(),
            protected_scenes_seeded: Vec::new(),
            backup_path: None,
        }
    }
}

/// The migrated bytes and their redacted change report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationOutcome {
    pub(crate) migrated_xml: Vec<u8>,
    pub(crate) report: MigrationReport,
}

/// Failures raised before or while applying a CGEN migration.
#[derive(Debug, Error)]
pub(crate) enum MigrationError {
    #[error("could not {operation} during cgen migration: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },
    #[error("cgen XML is not well formed")]
    InvalidXml,
    #[error("the migration input must have cgen as its document root")]
    InvalidRoot,
    #[error("cgen schema_version must be an unsigned integer")]
    InvalidSchemaVersion,
    #[error("cgen schema version {actual} is unsupported by this migration")]
    UnsupportedSchemaVersion { actual: u16 },
    #[error("cgen migration input exceeds the 4194304 byte safety limit")]
    InputTooLarge,
    #[error("cgen feed {index} needs a concrete id before it can be migrated")]
    MissingConcreteFeedId { index: usize },
    #[error("protected scene defaults could not be serialized")]
    SceneSerialization,
}

/// Migrates one managed `cgen.xml` document without expanding environment
/// placeholders.
///
/// `runtime_root` controls the reversible backup and protected scene locations.
/// Dry-run mode performs reads only. Apply mode writes a timestamped backup,
/// replaces the configuration through a same-directory temporary file, and
/// creates protected scene files only when their target paths do not exist.
///
/// # Errors
///
/// Returns an error for malformed or unsupported XML, a feed without a concrete
/// ID, failed filesystem operations, or scene serialization failure.
pub(crate) fn migrate_config(
    cgen_path: &Path,
    runtime_root: &Path,
    mode: MigrationMode,
) -> Result<MigrationOutcome, MigrationError> {
    let original = read_migration_input(cgen_path)?;
    let source_schema_version = inspect_document(&original)?;

    let mut report = MigrationReport::new(source_schema_version);
    let migrated_xml = match source_schema_version {
        LEGACY_SCHEMA_VERSION => migrate_legacy_xml(&original, &mut report)?,
        TARGET_SCHEMA_VERSION => original.clone(),
        actual => return Err(MigrationError::UnsupportedSchemaVersion { actual }),
    };
    report.config_changed = migrated_xml != original;

    let missing_scenes = missing_protected_scenes(runtime_root)?;
    report.protected_scenes_missing_before_apply = missing_scenes
        .iter()
        .map(|kind| kind.filename().to_string())
        .collect();

    if mode == MigrationMode::DryRun {
        return Ok(MigrationOutcome {
            migrated_xml,
            report,
        });
    }

    let backup_path = write_backup(runtime_root, &original)?;
    report.backup_path = Some(backup_path);

    if report.config_changed {
        replace_from_same_directory_temp(cgen_path, &migrated_xml)?;
    }

    report.protected_scenes_seeded = seed_protected_scenes(runtime_root, &missing_scenes)?;

    Ok(MigrationOutcome {
        migrated_xml,
        report,
    })
}

fn read_migration_input(path: &Path) -> Result<Vec<u8>, MigrationError> {
    let metadata = fs::metadata(path).map_err(|source| io_error("inspect cgen.xml", source))?;
    if metadata.len() > MAX_MIGRATION_XML_BYTES {
        return Err(MigrationError::InputTooLarge);
    }
    let bytes = fs::read(path).map_err(|source| io_error("read cgen.xml", source))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_MIGRATION_XML_BYTES {
        return Err(MigrationError::InputTooLarge);
    }
    Ok(bytes)
}

fn inspect_document(xml: &[u8]) -> Result<u16, MigrationError> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(false);
    let mut depth = 0usize;
    let mut root_seen = false;
    let mut root_closed = false;
    let mut schema_version = LEGACY_SCHEMA_VERSION;

    loop {
        let event = reader
            .read_event()
            .map_err(|_| MigrationError::InvalidXml)?;
        match event {
            Event::Start(start) => {
                if depth == 0 {
                    if root_seen || element_name(&start)? != "cgen" {
                        return Err(MigrationError::InvalidRoot);
                    }
                    root_seen = true;
                    schema_version = schema_version_from_root(&start)?;
                }
                if root_closed {
                    return Err(MigrationError::InvalidRoot);
                }
                depth = depth.checked_add(1).ok_or(MigrationError::InvalidXml)?;
            }
            Event::Empty(start) => {
                if depth == 0 {
                    if root_seen || element_name(&start)? != "cgen" {
                        return Err(MigrationError::InvalidRoot);
                    }
                    root_seen = true;
                    root_closed = true;
                    schema_version = schema_version_from_root(&start)?;
                }
            }
            Event::End(_) => {
                depth = depth.checked_sub(1).ok_or(MigrationError::InvalidXml)?;
                if depth == 0 {
                    root_closed = true;
                }
            }
            Event::Text(text) if depth == 0 => {
                let decoded = text.decode().map_err(|_| MigrationError::InvalidXml)?;
                if !decoded.trim().is_empty() {
                    return Err(MigrationError::InvalidXml);
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }

    if !root_seen || depth != 0 {
        return Err(MigrationError::InvalidRoot);
    }
    if !matches!(
        schema_version,
        LEGACY_SCHEMA_VERSION | TARGET_SCHEMA_VERSION
    ) {
        return Err(MigrationError::UnsupportedSchemaVersion {
            actual: schema_version,
        });
    }
    Ok(schema_version)
}

fn schema_version_from_root(root: &BytesStart<'_>) -> Result<u16, MigrationError> {
    let Some(value) = attribute_value(root, b"schema_version")? else {
        return Ok(LEGACY_SCHEMA_VERSION);
    };
    value
        .trim()
        .parse::<u16>()
        .map_err(|_| MigrationError::InvalidSchemaVersion)
}

#[derive(Debug)]
struct FeedMigrationState {
    id: String,
    preferred_alert_feed: String,
    inferred_force_layout: &'static str,
    has_alert: bool,
    has_ancillary: bool,
    has_compositor: bool,
    has_audio: bool,
    changed: bool,
}

impl FeedMigrationState {
    fn new(id: String, inferred_force_layout: &'static str) -> Self {
        Self {
            preferred_alert_feed: id.clone(),
            id,
            inferred_force_layout,
            has_alert: false,
            has_ancillary: false,
            has_compositor: false,
            has_audio: false,
            changed: false,
        }
    }
}

fn migrate_legacy_xml(xml: &[u8], report: &mut MigrationReport) -> Result<Vec<u8>, MigrationError> {
    let inferred_force_layouts = infer_legacy_force_layouts(xml)?;
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(false);
    let mut writer = Writer::new(Vec::with_capacity(xml.len().saturating_add(1024)));
    let mut stack = Vec::<String>::new();
    let mut active_feed: Option<FeedMigrationState> = None;

    loop {
        let event = reader
            .read_event()
            .map_err(|_| MigrationError::InvalidXml)?;
        match event {
            Event::Start(start) => {
                let name = element_name(&start)?.to_string();
                if stack.is_empty() && name == "cgen" {
                    let (updated, _) = update_attributes(
                        &start,
                        &[AttributeUpdate::always("schema_version", "2")],
                    )?;
                    write_event(&mut writer, Event::Start(updated))?;
                } else if name == "feed" && stack.as_slice() == ["cgen"] {
                    if active_feed.is_some() {
                        return Err(MigrationError::InvalidXml);
                    }
                    report.feeds_examined += 1;
                    let id = concrete_feed_id(&start, report.feeds_examined)?;
                    let force_layout = inferred_force_layouts
                        .get(report.feeds_examined - 1)
                        .copied()
                        .ok_or(MigrationError::InvalidXml)?;
                    active_feed = Some(FeedMigrationState::new(id, force_layout));
                    write_event(&mut writer, Event::Start(start.into_owned()))?;
                } else {
                    let migrated =
                        migrate_feed_element(&start, &name, &stack, active_feed.as_mut(), report)?;
                    write_event(&mut writer, Event::Start(migrated))?;
                }
                stack.push(name);
            }
            Event::Empty(start) => {
                let name = element_name(&start)?.to_string();
                if stack.is_empty() && name == "cgen" {
                    let (updated, _) = update_attributes(
                        &start,
                        &[AttributeUpdate::always("schema_version", "2")],
                    )?;
                    write_event(&mut writer, Event::Empty(updated))?;
                } else if name == "feed" && stack.as_slice() == ["cgen"] {
                    report.feeds_examined += 1;
                    let id = concrete_feed_id(&start, report.feeds_examined)?;
                    let force_layout = inferred_force_layouts
                        .get(report.feeds_examined - 1)
                        .copied()
                        .ok_or(MigrationError::InvalidXml)?;
                    let mut state = FeedMigrationState::new(id, force_layout);
                    write_event(&mut writer, Event::Start(start.into_owned()))?;
                    append_missing_feed_sections(&mut writer, &mut state, report)?;
                    write_event(&mut writer, Event::End(BytesEnd::new("feed")))?;
                    report.feeds_changed += 1;
                } else {
                    let migrated =
                        migrate_feed_element(&start, &name, &stack, active_feed.as_mut(), report)?;
                    write_event(&mut writer, Event::Empty(migrated))?;
                }
            }
            Event::End(end) => {
                let closing_name = std::str::from_utf8(end.name().into_inner())
                    .map_err(|_| MigrationError::InvalidXml)?;
                if stack.last().map(String::as_str) != Some(closing_name) {
                    return Err(MigrationError::InvalidXml);
                }
                if closing_name == "feed" && stack.len() == 2 && stack[0] == "cgen" {
                    let mut state = active_feed.take().ok_or(MigrationError::InvalidXml)?;
                    append_missing_feed_sections(&mut writer, &mut state, report)?;
                    if state.changed {
                        report.feeds_changed += 1;
                    }
                }
                write_event(&mut writer, Event::End(end.into_owned()))?;
                stack.pop();
            }
            Event::Eof => break,
            other => write_event(&mut writer, other.into_owned())?,
        }
    }

    if active_feed.is_some() || !stack.is_empty() {
        return Err(MigrationError::InvalidXml);
    }
    if report.feeds_examined != inferred_force_layouts.len() {
        return Err(MigrationError::InvalidXml);
    }
    Ok(writer.into_inner())
}

fn infer_legacy_force_layouts(xml: &[u8]) -> Result<Vec<&'static str>, MigrationError> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(false);
    let mut stack = Vec::<String>::new();
    let mut active_channels: Option<u16> = None;
    let mut layouts = Vec::new();

    loop {
        let event = reader
            .read_event()
            .map_err(|_| MigrationError::InvalidXml)?;
        match event {
            Event::Start(start) => {
                let name = element_name(&start)?;
                if name == "feed" && stack.as_slice() == ["cgen"] {
                    if active_channels.replace(0).is_some() {
                        return Err(MigrationError::InvalidXml);
                    }
                } else if is_ladder_audio(&name, &stack) {
                    observe_legacy_audio_rendition(&start, &mut active_channels)?;
                }
                stack.push(name);
            }
            Event::Empty(start) => {
                let name = element_name(&start)?;
                if name == "feed" && stack.as_slice() == ["cgen"] {
                    layouts.push("stereo");
                } else if is_ladder_audio(&name, &stack) {
                    observe_legacy_audio_rendition(&start, &mut active_channels)?;
                }
            }
            Event::End(end) => {
                let closing_name = std::str::from_utf8(end.name().into_inner())
                    .map_err(|_| MigrationError::InvalidXml)?;
                if stack.last().map(String::as_str) != Some(closing_name) {
                    return Err(MigrationError::InvalidXml);
                }
                if closing_name == "feed" && stack.len() == 2 && stack[0] == "cgen" {
                    let channels = active_channels.take().ok_or(MigrationError::InvalidXml)?;
                    layouts.push(force_layout_for_channels(channels));
                }
                stack.pop();
            }
            Event::Eof => break,
            _ => {}
        }
    }

    if active_channels.is_some() || !stack.is_empty() {
        return Err(MigrationError::InvalidXml);
    }
    Ok(layouts)
}

fn is_ladder_audio(name: &str, stack: &[String]) -> bool {
    name == "audio"
        && stack.len() >= 2
        && stack[stack.len() - 1] == "ladder"
        && stack[stack.len() - 2] == "feed"
}

fn observe_legacy_audio_rendition(
    start: &BytesStart<'_>,
    active_channels: &mut Option<u16>,
) -> Result<(), MigrationError> {
    let Some(maximum_channels) = active_channels.as_mut() else {
        return Err(MigrationError::InvalidXml);
    };
    let enabled = attribute_value(start, b"enabled")?
        .map(|value| migration_bool_text(&value, true))
        .unwrap_or(true);
    if !enabled {
        return Ok(());
    }
    let channels = attribute_value(start, b"channels")?
        .map(|value| {
            value
                .trim()
                .parse::<u16>()
                .map_err(|_| MigrationError::InvalidXml)
        })
        .transpose()?
        .unwrap_or(2)
        .clamp(1, 6);
    *maximum_channels = (*maximum_channels).max(channels);
    Ok(())
}

fn migration_bool_text(value: &str, default: bool) -> bool {
    let value = value.trim();
    if value.is_empty() {
        return default;
    }
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "enabled"
    )
}

fn force_layout_for_channels(channels: u16) -> &'static str {
    match channels {
        6 => "surround_51",
        1 => "mono",
        _ => "stereo",
    }
}

fn concrete_feed_id(start: &BytesStart<'_>, feed_index: usize) -> Result<String, MigrationError> {
    let id = attribute_value(start, b"id")?.unwrap_or_default();
    let id = id.trim();
    if id.is_empty() || id == "*" {
        return Err(MigrationError::MissingConcreteFeedId { index: feed_index });
    }
    Ok(id.to_string())
}

fn migrate_feed_element(
    start: &BytesStart<'_>,
    name: &str,
    stack: &[String],
    active_feed: Option<&mut FeedMigrationState>,
    report: &mut MigrationReport,
) -> Result<BytesStart<'static>, MigrationError> {
    let Some(feed) = active_feed else {
        return Ok(start.to_owned());
    };

    let direct_feed_child = stack.last().map(String::as_str) == Some("feed");
    let media_audio = name == "audio"
        && stack.len() >= 2
        && stack[stack.len() - 1] == "media"
        && stack[stack.len() - 2] == "feed";
    let priority_input = (name == "priorityInput" && direct_feed_child)
        || (name == "input"
            && stack.len() >= 2
            && stack[stack.len() - 1] == "priority"
            && stack[stack.len() - 2] == "feed");

    if priority_input {
        if let Some(candidate) = attribute_value(start, b"feed_id")? {
            let candidate = candidate.trim();
            if !candidate.is_empty() && candidate != "*" {
                feed.preferred_alert_feed = candidate.to_string();
            }
        }
        return Ok(start.to_owned());
    }

    if name == "alert" && direct_feed_child {
        feed.has_alert = true;
        let (updated, changed) = update_attributes(
            start,
            &[AttributeUpdate::missing_blank_or_wildcard(
                "feed_id", &feed.id,
            )],
        )?;
        if changed > 0 {
            feed.changed = true;
            report.alert_routes_normalized += 1;
        }
        return Ok(updated);
    }

    if name == "ancillary" && direct_feed_child {
        feed.has_ancillary = true;
        let updates = default_attribute_updates(&ANCILLARY_DEFAULTS);
        let (updated, changed) = update_attributes(start, &updates)?;
        if changed > 0 {
            feed.changed = true;
            report.ancillary_sections_augmented += 1;
        }
        return Ok(updated);
    }

    if name == "compositor" && direct_feed_child {
        feed.has_compositor = true;
        let updates = default_attribute_updates(&COMPOSITOR_DEFAULTS);
        let (updated, changed) = update_attributes(start, &updates)?;
        if changed > 0 {
            feed.changed = true;
            report.compositor_sections_augmented += 1;
        }
        return Ok(updated);
    }

    if name == "audio" && (direct_feed_child || media_audio) {
        feed.has_audio = true;
        let defaults = audio_defaults(feed.inferred_force_layout);
        let updates = default_attribute_updates(&defaults);
        let (updated, changed) = update_attributes(start, &updates)?;
        if changed > 0 {
            feed.changed = true;
            report.audio_sections_augmented += 1;
        }
        return Ok(updated);
    }

    Ok(start.to_owned())
}

fn append_missing_feed_sections(
    writer: &mut Writer<Vec<u8>>,
    feed: &mut FeedMigrationState,
    report: &mut MigrationReport,
) -> Result<(), MigrationError> {
    if !feed.has_alert {
        write_indented_empty(
            writer,
            "alert",
            &[("feed_id", feed.preferred_alert_feed.as_str())],
        )?;
        feed.changed = true;
        report.alert_routes_added += 1;
    }
    if !feed.has_ancillary {
        write_indented_empty(writer, "ancillary", &ANCILLARY_DEFAULTS)?;
        feed.changed = true;
        report.ancillary_sections_added += 1;
    }
    if !feed.has_compositor {
        write_indented_empty(writer, "compositor", &COMPOSITOR_DEFAULTS)?;
        feed.changed = true;
        report.compositor_sections_added += 1;
    }
    if !feed.has_audio {
        let defaults = audio_defaults(feed.inferred_force_layout);
        write_indented_empty(writer, "audio", &defaults)?;
        feed.changed = true;
        report.audio_sections_added += 1;
    }
    if feed.changed {
        write_event(writer, Event::Text(BytesText::new("\n  ")))?;
    }
    Ok(())
}

fn write_indented_empty(
    writer: &mut Writer<Vec<u8>>,
    name: &str,
    attributes: &[(&str, &str)],
) -> Result<(), MigrationError> {
    write_event(writer, Event::Text(BytesText::new("\n    ")))?;
    let mut element = BytesStart::new(name);
    element.extend_attributes(attributes.iter().copied());
    write_event(writer, Event::Empty(element))
}

#[derive(Debug, Clone, Copy)]
enum UpdateCondition {
    Always,
    Missing,
    MissingBlankOrWildcard,
}

#[derive(Debug, Clone, Copy)]
struct AttributeUpdate<'a> {
    key: &'a str,
    value: &'a str,
    condition: UpdateCondition,
}

impl<'a> AttributeUpdate<'a> {
    const fn always(key: &'a str, value: &'a str) -> Self {
        Self {
            key,
            value,
            condition: UpdateCondition::Always,
        }
    }

    const fn missing(key: &'a str, value: &'a str) -> Self {
        Self {
            key,
            value,
            condition: UpdateCondition::Missing,
        }
    }

    const fn missing_blank_or_wildcard(key: &'a str, value: &'a str) -> Self {
        Self {
            key,
            value,
            condition: UpdateCondition::MissingBlankOrWildcard,
        }
    }
}

fn default_attribute_updates<'a>(defaults: &'a [(&'a str, &'a str)]) -> Vec<AttributeUpdate<'a>> {
    defaults
        .iter()
        .map(|(key, value)| AttributeUpdate::missing(key, value))
        .collect()
}

#[derive(Debug)]
struct OwnedAttribute {
    key: Vec<u8>,
    value: Vec<u8>,
}

fn update_attributes(
    start: &BytesStart<'_>,
    updates: &[AttributeUpdate<'_>],
) -> Result<(BytesStart<'static>, usize), MigrationError> {
    let mut attributes = Vec::<OwnedAttribute>::new();
    for attribute in start.attributes().with_checks(true) {
        let attribute = attribute.map_err(|_| MigrationError::InvalidXml)?;
        attributes.push(OwnedAttribute {
            key: attribute.key.as_ref().to_vec(),
            value: attribute.value.as_ref().to_vec(),
        });
    }

    let mut changed = 0usize;
    for update in updates {
        let existing = attributes
            .iter_mut()
            .find(|attribute| attribute.key.as_slice() == update.key.as_bytes());
        match existing {
            Some(attribute) => {
                let decoded = decode_attribute_bytes(&attribute.value)?;
                let should_replace = match update.condition {
                    UpdateCondition::Always => decoded != update.value,
                    UpdateCondition::Missing => false,
                    UpdateCondition::MissingBlankOrWildcard => {
                        decoded.trim().is_empty() || decoded.trim() == "*"
                    }
                };
                if should_replace {
                    attribute.value = escaped_attribute_bytes(update.value);
                    changed += 1;
                }
            }
            None => {
                attributes.push(OwnedAttribute {
                    key: update.key.as_bytes().to_vec(),
                    value: escaped_attribute_bytes(update.value),
                });
                changed += 1;
            }
        }
    }

    if changed == 0 {
        return Ok((start.to_owned(), 0));
    }

    let mut rebuilt = BytesStart::new(element_name(start)?.to_string());
    for attribute in &attributes {
        rebuilt.push_attribute((attribute.key.as_slice(), attribute.value.as_slice()));
    }
    Ok((rebuilt, changed))
}

fn attribute_value(start: &BytesStart<'_>, key: &[u8]) -> Result<Option<String>, MigrationError> {
    let attribute = start
        .try_get_attribute(key)
        .map_err(|_| MigrationError::InvalidXml)?;
    let Some(attribute) = attribute else {
        return Ok(None);
    };
    attribute
        .decode_and_unescape_value(start.decoder())
        .map(|value| Some(value.into_owned()))
        .map_err(|_| MigrationError::InvalidXml)
}

fn decode_attribute_bytes(value: &[u8]) -> Result<String, MigrationError> {
    let value = std::str::from_utf8(value).map_err(|_| MigrationError::InvalidXml)?;
    quick_xml::escape::unescape(value)
        .map(|value| value.into_owned())
        .map_err(|_| MigrationError::InvalidXml)
}

fn escaped_attribute_bytes(value: &str) -> Vec<u8> {
    quick_xml::escape::escape(value).as_bytes().to_vec()
}

fn element_name(start: &BytesStart<'_>) -> Result<String, MigrationError> {
    std::str::from_utf8(start.name().into_inner())
        .map(str::to_owned)
        .map_err(|_| MigrationError::InvalidXml)
}

fn write_event<'a>(writer: &mut Writer<Vec<u8>>, event: Event<'a>) -> Result<(), MigrationError> {
    writer
        .write_event(event)
        .map_err(|source| io_error("encode migrated XML", source))
}

fn missing_protected_scenes(
    runtime_root: &Path,
) -> Result<Vec<ProtectedSceneKind>, MigrationError> {
    let directory = runtime_root.join(SCENE_DIRECTORY);
    let mut missing = Vec::new();
    for kind in ProtectedSceneKind::ALL {
        match fs::metadata(directory.join(kind.filename())) {
            Ok(_) => {}
            Err(source) if source.kind() == io::ErrorKind::NotFound => missing.push(kind),
            Err(source) => return Err(io_error("inspect protected scene", source)),
        }
    }
    Ok(missing)
}

fn write_backup(runtime_root: &Path, original: &[u8]) -> Result<PathBuf, MigrationError> {
    let directory = runtime_root.join(BACKUP_DIRECTORY);
    fs::create_dir_all(&directory)
        .map_err(|source| io_error("create cgen backup directory", source))?;

    let timestamp = Utc::now().format("%Y%m%dT%H%M%S%3fZ");
    for sequence in 0..1000u16 {
        let suffix = if sequence == 0 {
            String::new()
        } else {
            format!("-{sequence}")
        };
        let path = directory.join(format!("cgen-{timestamp}{suffix}.xml"));
        match write_new_file(&path, original) {
            Ok(()) => return Ok(path),
            Err(MigrationError::Io { source, .. })
                if source.kind() == io::ErrorKind::AlreadyExists =>
            {
                continue;
            }
            Err(error) => return Err(error),
        }
    }
    Err(io_error(
        "create unique cgen backup",
        io::Error::new(io::ErrorKind::AlreadyExists, "backup name space exhausted"),
    ))
}

fn replace_from_same_directory_temp(
    destination: &Path,
    contents: &[u8],
) -> Result<(), MigrationError> {
    let directory = destination.parent().unwrap_or_else(|| Path::new("."));
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cgen.xml");
    let timestamp = Utc::now().format("%Y%m%dT%H%M%S%3fZ");
    let prefix = format!(
        ".{file_name}.migration-{timestamp}-{}.tmp",
        std::process::id()
    );
    let (temporary_path, mut temporary) = create_unique_file(directory, &prefix)?;

    let write_result = (|| {
        temporary
            .write_all(contents)
            .map_err(|source| io_error("write migrated cgen temporary file", source))?;
        temporary
            .sync_all()
            .map_err(|source| io_error("sync migrated cgen temporary file", source))?;
        if let Ok(metadata) = fs::metadata(destination) {
            fs::set_permissions(&temporary_path, metadata.permissions())
                .map_err(|source| io_error("preserve cgen file permissions", source))?;
        }
        drop(temporary);
        replace_path(&temporary_path, destination)
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    write_result
}

fn create_unique_file(directory: &Path, prefix: &str) -> Result<(PathBuf, File), MigrationError> {
    for sequence in 0..1000u16 {
        let path = directory.join(format!("{prefix}-{sequence}"));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => return Err(io_error("create cgen temporary file", source)),
        }
    }
    Err(io_error(
        "create unique cgen temporary file",
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "temporary name space exhausted",
        ),
    ))
}

#[cfg(not(windows))]
fn replace_path(temporary: &Path, destination: &Path) -> Result<(), MigrationError> {
    fs::rename(temporary, destination)
        .map_err(|source| io_error("replace cgen.xml from temporary file", source))?;
    let directory = destination.parent().unwrap_or_else(|| Path::new("."));
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("sync cgen configuration directory", source))
}

#[cfg(windows)]
fn replace_path(temporary: &Path, destination: &Path) -> Result<(), MigrationError> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{ReplaceFileW, REPLACEFILE_WRITE_THROUGH};

    let replaced = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let replacement = temporary
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    // SAFETY: Both path buffers are NUL-terminated and remain alive for the
    // call. The optional backup, exclude, and reserved pointers are null as
    // required by ReplaceFileW.
    let success = unsafe {
        ReplaceFileW(
            replaced.as_ptr(),
            replacement.as_ptr(),
            std::ptr::null(),
            REPLACEFILE_WRITE_THROUGH,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if success == 0 {
        Err(io_error(
            "atomically replace cgen.xml from temporary file",
            io::Error::last_os_error(),
        ))
    } else {
        Ok(())
    }
}

fn seed_protected_scenes(
    runtime_root: &Path,
    missing: &[ProtectedSceneKind],
) -> Result<Vec<String>, MigrationError> {
    if missing.is_empty() {
        return Ok(Vec::new());
    }

    let directory = runtime_root.join(SCENE_DIRECTORY);
    fs::create_dir_all(&directory)
        .map_err(|source| io_error("create protected scene directory", source))?;
    let mut seeded = Vec::with_capacity(missing.len());

    for kind in missing {
        let destination = directory.join(kind.filename());
        let scene_xml = protected_default_scene(*kind)
            .to_xml()
            .map_err(|_| MigrationError::SceneSerialization)?;
        let contents = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{scene_xml}\n");
        match write_new_file(&destination, contents.as_bytes()) {
            Ok(()) => seeded.push(kind.filename().to_string()),
            Err(MigrationError::Io { source, .. })
                if source.kind() == io::ErrorKind::AlreadyExists =>
            {
                // Another operator or process created the scene after inspection.
            }
            Err(error) => return Err(error),
        }
    }
    Ok(seeded)
}

fn write_new_file(path: &Path, contents: &[u8]) -> Result<(), MigrationError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| io_error("create migration file", source))?;
    if let Err(source) = file.write_all(contents).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(io_error("write migration file", source));
    }
    Ok(())
}

fn io_error(operation: &'static str, source: io::Error) -> MigrationError {
    MigrationError::Io { operation, source }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_fixture(xml: &str) -> (tempfile::TempDir, PathBuf) {
        let temporary = tempfile::tempdir().expect("create temporary runtime");
        let config_directory = temporary.path().join("managed/configs");
        fs::create_dir_all(&config_directory).expect("create config directory");
        let cgen_path = config_directory.join("cgen.xml");
        fs::write(&cgen_path, xml).expect("write legacy cgen config");
        (temporary, cgen_path)
    }

    fn migrated_text(outcome: &MigrationOutcome) -> String {
        String::from_utf8(outcome.migrated_xml.clone()).expect("migration writes UTF-8")
    }

    #[test]
    fn dry_run_retains_environment_placeholders_and_unknown_xml_without_writes() {
        let original = r#"<?xml version="1.0" encoding="UTF-8"?>
<cgen enabled="true" operator_extension="retained">
  <feed id="feed_one" custom="yes">
    <programInput url="${PROGRAM_URL}" format="mpegts"/>
    <priorityInput feed_id="*"/>
    <media>
      <audio idle="source" custom_audio="A &amp; B"/>
    </media>
    <operatorData mode="untouched"><nested>${PRIVATE_TOKEN}</nested></operatorData>
  </feed>
</cgen>
"#;
        let (runtime, cgen_path) = runtime_fixture(original);

        let outcome = migrate_config(&cgen_path, runtime.path(), MigrationMode::DryRun)
            .expect("dry-run migration succeeds");
        let migrated = migrated_text(&outcome);

        assert_eq!(
            fs::read_to_string(&cgen_path).expect("read source"),
            original
        );
        assert!(!runtime.path().join("backups").exists());
        assert!(!runtime.path().join("managed/cgen").exists());
        assert!(migrated.contains("schema_version=\"2\""));
        assert!(migrated.contains("${PROGRAM_URL}"));
        assert!(migrated.contains("${PRIVATE_TOKEN}"));
        assert!(migrated.contains("operator_extension=\"retained\""));
        assert!(migrated.contains("<operatorData mode=\"untouched\"><nested>"));
        assert!(migrated.contains("custom_audio=\"A &amp; B\""));
        assert!(migrated.contains("<alert feed_id=\"feed_one\"/>"));
        assert!(migrated.contains("topology=\"force_layout\""));
        assert!(migrated.contains("alert_program_gain_db=\"muted\""));
        assert_eq!(outcome.report.feeds_examined, 1);
        assert_eq!(outcome.report.alert_routes_added, 1);
        assert_eq!(outcome.report.audio_sections_augmented, 1);
        assert_eq!(
            outcome.report.protected_scenes_missing_before_apply.len(),
            4
        );
    }

    #[test]
    fn migration_preserves_concrete_alert_route_and_explicit_audio_settings() {
        let original = r#"<cgen schema_version="1">
  <feed id="local_feed">
    <priority><input feed_id="remote_alerts"/></priority>
    <alert feed_id="remote_alerts"/>
    <ancillary captions="pass"/>
    <compositor alert_scene_id="custom_scene" engine="scene_graph"/>
    <audio topology="preserve_native_tracks" alert_program_gain_db="-12"/>
  </feed>
</cgen>"#;
        let (runtime, cgen_path) = runtime_fixture(original);

        let outcome = migrate_config(&cgen_path, runtime.path(), MigrationMode::DryRun)
            .expect("migration succeeds");
        let migrated = migrated_text(&outcome);

        assert!(migrated.contains("<alert feed_id=\"remote_alerts\"/>"));
        assert!(migrated.contains("captions=\"pass\""));
        assert!(migrated.contains("scte35=\"drop\""));
        assert!(migrated.contains("alert_scene_id=\"custom_scene\""));
        assert!(migrated.contains("engine=\"scene_graph\""));
        assert!(migrated.contains("topology=\"preserve_native_tracks\""));
        assert!(migrated.contains("alert_program_gain_db=\"-12\""));
        assert!(migrated.contains("force_layout=\"stereo\""));
        assert_eq!(outcome.report.alert_routes_normalized, 0);
        assert_eq!(outcome.report.ancillary_sections_augmented, 1);
        assert_eq!(outcome.report.compositor_sections_augmented, 0);
        assert_eq!(outcome.report.audio_sections_augmented, 1);
    }

    #[test]
    fn migration_infers_surround_master_for_mixed_enabled_legacy_renditions() {
        let original = r#"<cgen schema_version="1">
  <feed id="sd">
    <media><audio idle="source"/></media>
    <ladder>
      <audio id="stereo" enabled="true" channels="2"/>
      <audio id="surround_51" enabled="true" channels="6"/>
    </ladder>
  </feed>
</cgen>"#;
        let (runtime, cgen_path) = runtime_fixture(original);

        let outcome = migrate_config(&cgen_path, runtime.path(), MigrationMode::DryRun)
            .expect("migration succeeds");
        let migrated = migrated_text(&outcome);

        assert!(migrated.contains("force_layout=\"surround_51\""));
    }

    #[test]
    fn migration_ignores_disabled_surround_rendition_when_inferring_master() {
        let original = r#"<cgen schema_version="1">
  <feed id="stereo_only">
    <media><audio idle="source"/></media>
    <ladder>
      <audio id="stereo" enabled="true" channels="2"/>
      <audio id="surround_51" enabled="false" channels="6"/>
    </ladder>
  </feed>
</cgen>"#;
        let (runtime, cgen_path) = runtime_fixture(original);

        let outcome = migrate_config(&cgen_path, runtime.path(), MigrationMode::DryRun)
            .expect("migration succeeds");
        let migrated = migrated_text(&outcome);

        assert!(migrated.contains("force_layout=\"stereo\""));
        assert!(!migrated.contains("force_layout=\"surround_51\""));
    }

    #[test]
    fn wildcard_existing_alert_is_normalized_to_its_own_feed() {
        let original = r#"<cgen><feed id="weather"><alert feed_id="*"/></feed></cgen>"#;
        let (runtime, cgen_path) = runtime_fixture(original);

        let outcome = migrate_config(&cgen_path, runtime.path(), MigrationMode::DryRun)
            .expect("migration succeeds");
        let migrated = migrated_text(&outcome);

        assert!(migrated.contains("<alert feed_id=\"weather\"/>"));
        assert_eq!(outcome.report.alert_routes_normalized, 1);
    }

    #[test]
    fn apply_writes_backup_migrates_config_and_preserves_existing_scene() {
        let original = r#"<cgen><feed id="weather"><unknown value="keep"/></feed></cgen>"#;
        let (runtime, cgen_path) = runtime_fixture(original);
        let scene_directory = runtime.path().join(SCENE_DIRECTORY);
        fs::create_dir_all(&scene_directory).expect("create scene directory");
        let existing_scene = scene_directory.join("crawl.xml");
        fs::write(&existing_scene, b"operator-owned-scene").expect("write operator-owned scene");

        let outcome = migrate_config(&cgen_path, runtime.path(), MigrationMode::Apply)
            .expect("apply migration succeeds");

        let applied = fs::read(&cgen_path).expect("read applied config");
        assert_eq!(applied, outcome.migrated_xml);
        assert!(String::from_utf8(applied)
            .expect("UTF-8 config")
            .contains("schema_version=\"2\""));
        let backup_path = outcome.report.backup_path.expect("backup path reported");
        assert!(backup_path.starts_with(runtime.path().join(BACKUP_DIRECTORY)));
        assert_eq!(
            fs::read(backup_path).expect("read backup"),
            original.as_bytes()
        );
        assert_eq!(
            fs::read(&existing_scene).expect("read existing scene"),
            b"operator-owned-scene"
        );
        assert_eq!(outcome.report.protected_scenes_seeded.len(), 3);
        assert!(!outcome
            .report
            .protected_scenes_seeded
            .iter()
            .any(|name| name == "crawl.xml"));
        for kind in ProtectedSceneKind::ALL {
            assert!(scene_directory.join(kind.filename()).exists());
        }
    }

    #[test]
    fn current_schema_is_byte_stable_but_apply_still_backs_up_and_seeds() {
        let original = b"<cgen schema_version=\"2\"><feed id=\"weather\"/></cgen>";
        let (runtime, cgen_path) = runtime_fixture(std::str::from_utf8(original).expect("UTF-8"));

        let dry_run = migrate_config(&cgen_path, runtime.path(), MigrationMode::DryRun)
            .expect("dry run succeeds");
        assert_eq!(dry_run.migrated_xml, original);
        assert!(!dry_run.report.config_changed);

        let applied = migrate_config(&cgen_path, runtime.path(), MigrationMode::Apply)
            .expect("apply succeeds");
        assert_eq!(fs::read(&cgen_path).expect("read config"), original);
        assert_eq!(
            fs::read(applied.report.backup_path.expect("backup exists")).expect("read backup"),
            original
        );
        assert_eq!(applied.report.protected_scenes_seeded.len(), 4);
    }

    #[test]
    fn unsupported_schema_is_rejected_before_any_write() {
        let original = r#"<cgen schema_version="99"><feed id="weather"/></cgen>"#;
        let (runtime, cgen_path) = runtime_fixture(original);

        let error = migrate_config(&cgen_path, runtime.path(), MigrationMode::Apply)
            .expect_err("unsupported migration must fail");

        assert!(matches!(
            error,
            MigrationError::UnsupportedSchemaVersion { actual: 99 }
        ));
        assert_eq!(
            fs::read_to_string(cgen_path).expect("source remains"),
            original
        );
        assert!(!runtime.path().join("backups").exists());
        assert!(!runtime.path().join("managed/cgen").exists());
    }

    #[test]
    fn oversized_input_is_rejected_before_backup_or_scene_writes() {
        let (runtime, cgen_path) = runtime_fixture("<cgen/>");
        fs::write(
            &cgen_path,
            vec![b' '; usize::try_from(MAX_MIGRATION_XML_BYTES + 1).expect("test size")],
        )
        .expect("write oversized input");

        let error = migrate_config(&cgen_path, runtime.path(), MigrationMode::Apply)
            .expect_err("oversized migration must fail");
        assert!(matches!(error, MigrationError::InputTooLarge));
        assert!(!runtime.path().join(BACKUP_DIRECTORY).exists());
        assert!(!runtime.path().join(SCENE_DIRECTORY).exists());
    }
}
