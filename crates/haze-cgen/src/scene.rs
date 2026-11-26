//! Validated scene graph documents and protected scene catalog handling.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

use quick_xml::events::Event;
use quick_xml::Reader;
use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

pub(crate) const SCENE_SCHEMA_VERSION: u16 = 1;
pub(crate) const MAX_SCENE_DEPTH: usize = 32;
pub(crate) const MAX_SCENE_NODES: usize = 4_096;
pub(crate) const MAX_SCENE_XML_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) const PROGRAM_PASSTHROUGH_ID: &str = "Program_Passthrough";
pub(crate) const STANDARD_CRAWL_ID: &str = "Standard_Crawl";
pub(crate) const FULLSCREEN_TAKEOVER_ID: &str = "Fullscreen_Takeover";
pub(crate) const STANDBY_ID: &str = "Standby";
pub(crate) const STANDBY_TITLE: &str = "Emergency Alert Details Channel";

const MAX_IDENTIFIER_LENGTH: usize = 96;
const MAX_SCENE_NAME_LENGTH: usize = 160;
const MAX_NODE_NAME_LENGTH: usize = 160;
const MAX_ASSET_ID_LENGTH: usize = 512;
const MAX_STATIC_TEXT_LENGTH: usize = 64 * 1024;
const MAX_FONT_FAMILY_LENGTH: usize = 256;
const MAX_GRADIENT_STOPS: usize = 64;
const MAX_ABSOLUTE_GEOMETRY: f32 = 1_000_000.0;

/// Errors raised while parsing, validating, or mutating scene documents.
#[derive(Debug, Error)]
pub(crate) enum SceneError {
    #[error("invalid {kind} `{value}`: {reason}")]
    InvalidIdentifier {
        kind: &'static str,
        value: String,
        reason: &'static str,
    },
    #[error("invalid RGBA color `{value}`: {reason}")]
    InvalidColor { value: String, reason: &'static str },
    #[error("invalid asset ID `{value}`: {reason}")]
    InvalidAssetId { value: String, reason: &'static str },
    #[error("scene XML could not be parsed: {0}")]
    XmlDeserialize(String),
    #[error("scene XML could not be serialized: {0}")]
    XmlSerialize(String),
    #[error("could not read scene file {path:?}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("scene file {path:?} exceeds the maximum size of {maximum} bytes")]
    SceneFileTooLarge { path: PathBuf, maximum: u64 },
    #[error("could not resolve asset path {path:?}: {source}")]
    ResolveAsset {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("asset `{asset_id}` resolves outside {asset_root:?}")]
    AssetOutsideRoot {
        asset_id: AssetId,
        asset_root: PathBuf,
    },
    #[error("scene `{scene_id}` uses unsupported schema version {actual}, expected {expected}")]
    UnsupportedSchema {
        scene_id: SceneId,
        actual: u16,
        expected: u16,
    },
    #[error("scene `{scene_id}` is invalid: {reason}")]
    InvalidScene { scene_id: SceneId, reason: String },
    #[error("scene `{scene_id}` contains duplicate node ID `{node_id}`")]
    DuplicateNodeId { scene_id: SceneId, node_id: NodeId },
    #[error("scene `{scene_id}` exceeds the maximum depth of {maximum}")]
    MaximumDepthExceeded { scene_id: SceneId, maximum: usize },
    #[error("scene `{scene_id}` exceeds the maximum node count of {maximum}")]
    MaximumNodeCountExceeded { scene_id: SceneId, maximum: usize },
    #[error("protected scene `{scene_id}` cannot be deleted")]
    ProtectedSceneDelete { scene_id: SceneId },
    #[error("protected scene `{scene_id}` cannot be renamed")]
    ProtectedSceneRename { scene_id: SceneId },
    #[error("scene `{scene_id}` is locked and cannot be edited")]
    LockedSceneEdit { scene_id: SceneId },
    #[error("scene `{scene_id}` cannot be selected as an alert scene")]
    InvalidAlertScene { scene_id: SceneId },
}

macro_rules! validated_identifier {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub(crate) struct $name(String);

        impl $name {
            /// Parses an identifier at the configuration boundary.
            pub(crate) fn new(value: impl Into<String>) -> Result<Self, SceneError> {
                let value = value.into();
                validate_identifier($kind, &value)?;
                Ok(Self(value))
            }

            pub(crate) fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = SceneError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

validated_identifier!(SceneId, "scene ID");
validated_identifier!(NodeId, "node ID");

fn validate_identifier(kind: &'static str, value: &str) -> Result<(), SceneError> {
    if value.is_empty() {
        return Err(SceneError::InvalidIdentifier {
            kind,
            value: value.to_string(),
            reason: "it cannot be empty",
        });
    }
    if value.trim() != value {
        return Err(SceneError::InvalidIdentifier {
            kind,
            value: value.to_string(),
            reason: "leading or trailing whitespace is not allowed",
        });
    }
    if value.len() > MAX_IDENTIFIER_LENGTH {
        return Err(SceneError::InvalidIdentifier {
            kind,
            value: value.to_string(),
            reason: "it is too long",
        });
    }
    if matches!(value, "." | "..") || is_windows_reserved_component(value) {
        return Err(SceneError::InvalidIdentifier {
            kind,
            value: value.to_string(),
            reason: "the value is reserved by a supported filesystem",
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(SceneError::InvalidIdentifier {
            kind,
            value: value.to_string(),
            reason: "only ASCII letters, numbers, underscore, hyphen, and period are allowed",
        });
    }

    if kind == "scene ID" {
        for canonical in [
            PROGRAM_PASSTHROUGH_ID,
            STANDARD_CRAWL_ID,
            FULLSCREEN_TAKEOVER_ID,
            STANDBY_ID,
        ] {
            if value.eq_ignore_ascii_case(canonical) && value != canonical {
                return Err(SceneError::InvalidIdentifier {
                    kind,
                    value: value.to_string(),
                    reason: "protected scene IDs must use their canonical spelling",
                });
            }
        }
    }
    Ok(())
}

/// A syntactically safe asset path relative to `managed/cgen/assets`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct AssetId(String);

impl AssetId {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, SceneError> {
        let value = value.into();
        validate_asset_id(&value)?;
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// Resolves the syntactically validated ID beneath the managed directory.
    pub(crate) fn path_under(&self, managed_directory: &Path) -> PathBuf {
        managed_directory.join("cgen").join("assets").join(&self.0)
    }

    /// Resolves an existing asset and verifies that symlinks remain in the asset root.
    pub(crate) fn resolve_existing_under(
        &self,
        managed_directory: &Path,
    ) -> Result<PathBuf, SceneError> {
        let asset_root = managed_directory.join("cgen").join("assets");
        let canonical_root =
            fs::canonicalize(&asset_root).map_err(|source| SceneError::ResolveAsset {
                path: asset_root.clone(),
                source,
            })?;
        let candidate = self.path_under(managed_directory);
        let canonical_candidate =
            fs::canonicalize(&candidate).map_err(|source| SceneError::ResolveAsset {
                path: candidate,
                source,
            })?;
        if !canonical_candidate.starts_with(&canonical_root) {
            return Err(SceneError::AssetOutsideRoot {
                asset_id: self.clone(),
                asset_root: canonical_root,
            });
        }
        Ok(canonical_candidate)
    }
}

impl fmt::Display for AssetId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for AssetId {
    type Err = SceneError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for AssetId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AssetId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

fn validate_asset_id(value: &str) -> Result<(), SceneError> {
    let invalid = |reason| SceneError::InvalidAssetId {
        value: value.to_string(),
        reason,
    };

    if value.is_empty() {
        return Err(invalid("it cannot be empty"));
    }
    if value.trim() != value {
        return Err(invalid("leading or trailing whitespace is not allowed"));
    }
    if value.len() > MAX_ASSET_ID_LENGTH {
        return Err(invalid("it is too long"));
    }
    if value.contains('\\') || value.contains('\0') {
        return Err(invalid("only forward slash path separators are allowed"));
    }
    if value.contains(':') {
        return Err(invalid("drive prefixes and URI schemes are not allowed"));
    }

    let path = Path::new(value);
    if path.is_absolute() {
        return Err(invalid("absolute paths are not allowed"));
    }
    let mut saw_component = false;
    for component in path.components() {
        match component {
            Component::Normal(_) => saw_component = true,
            Component::CurDir => {
                return Err(invalid("current-directory components are not allowed"))
            }
            Component::ParentDir => {
                return Err(invalid("parent-directory traversal is not allowed"));
            }
            Component::Prefix(_) | Component::RootDir => {
                return Err(invalid("absolute paths are not allowed"));
            }
        }
    }
    if !saw_component || value.split('/').any(str::is_empty) {
        return Err(invalid("empty path components are not allowed"));
    }
    if value
        .split('/')
        .any(|component| matches!(component, "." | ".."))
    {
        return Err(invalid("relative traversal components are not allowed"));
    }
    if value.split('/').any(is_windows_reserved_component) {
        return Err(invalid("a path component is reserved by Windows"));
    }
    Ok(())
}

fn is_windows_reserved_component(component: &str) -> bool {
    if component.ends_with(' ') || component.ends_with('.') {
        return true;
    }
    let stem = component.split('.').next().unwrap_or(component);
    let stem = stem.to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$")
        || stem
            .strip_prefix("COM")
            .or_else(|| stem.strip_prefix("LPT"))
            .is_some_and(|suffix| matches!(suffix.as_bytes(), [b'1'..=b'9']))
}

/// An eight-bit red, green, blue, and alpha color.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RgbaColor {
    pub(crate) red: u8,
    pub(crate) green: u8,
    pub(crate) blue: u8,
    pub(crate) alpha: u8,
}

impl RgbaColor {
    pub(crate) const BLACK: Self = Self::new(0, 0, 0, 255);
    pub(crate) const WHITE: Self = Self::new(255, 255, 255, 255);

    pub(crate) const fn new(red: u8, green: u8, blue: u8, alpha: u8) -> Self {
        Self {
            red,
            green,
            blue,
            alpha,
        }
    }
}

impl fmt::Display for RgbaColor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "#{:02X}{:02X}{:02X}{:02X}",
            self.red, self.green, self.blue, self.alpha
        )
    }
}

impl FromStr for RgbaColor {
    type Err = SceneError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_rgba(value)
    }
}

impl Serialize for RgbaColor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RgbaColor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

fn parse_rgba(value: &str) -> Result<RgbaColor, SceneError> {
    let value = value.trim();
    if let Some(hex) = value.strip_prefix('#') {
        return parse_hex_rgba(value, hex);
    }

    if let Some(body) = value
        .strip_prefix("rgba(")
        .and_then(|body| body.strip_suffix(')'))
    {
        let fields = body.split(',').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 4 {
            return Err(invalid_color(
                value,
                "rgba() requires red, green, blue, and alpha values",
            ));
        }
        let red = parse_color_channel(value, fields[0])?;
        let green = parse_color_channel(value, fields[1])?;
        let blue = parse_color_channel(value, fields[2])?;
        let alpha = parse_alpha_channel(value, fields[3])?;
        return Ok(RgbaColor::new(red, green, blue, alpha));
    }

    if let Some(body) = value
        .strip_prefix("rgb(")
        .and_then(|body| body.strip_suffix(')'))
    {
        let fields = body.split(',').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 3 {
            return Err(invalid_color(value, "rgb() requires three channel values"));
        }
        return Ok(RgbaColor::new(
            parse_color_channel(value, fields[0])?,
            parse_color_channel(value, fields[1])?,
            parse_color_channel(value, fields[2])?,
            255,
        ));
    }

    Err(invalid_color(
        value,
        "expected #RGB, #RGBA, #RRGGBB, #RRGGBBAA, rgb(), or rgba()",
    ))
}

fn parse_hex_rgba(original: &str, hex: &str) -> Result<RgbaColor, SceneError> {
    let expanded;
    let hex = match hex.len() {
        3 | 4 => {
            expanded = hex
                .chars()
                .flat_map(|character| [character, character])
                .collect::<String>();
            expanded.as_str()
        }
        6 | 8 => hex,
        _ => {
            return Err(invalid_color(
                original,
                "hex colors require 3, 4, 6, or 8 hexadecimal digits",
            ));
        }
    };
    if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_color(
            original,
            "hex colors contain a non-hexadecimal digit",
        ));
    }

    let pair = |start: usize| {
        u8::from_str_radix(&hex[start..start + 2], 16)
            .map_err(|_| invalid_color(original, "hex color channel is invalid"))
    };
    Ok(RgbaColor::new(
        pair(0)?,
        pair(2)?,
        pair(4)?,
        if hex.len() == 8 { pair(6)? } else { 255 },
    ))
}

fn parse_color_channel(original: &str, value: &str) -> Result<u8, SceneError> {
    value
        .parse::<u8>()
        .map_err(|_| invalid_color(original, "RGB channels must be integers from 0 through 255"))
}

fn parse_alpha_channel(original: &str, value: &str) -> Result<u8, SceneError> {
    if let Some(percent) = value.strip_suffix('%') {
        let percent = percent
            .parse::<f32>()
            .map_err(|_| invalid_color(original, "alpha percentage is invalid"))?;
        if !percent.is_finite() || !(0.0..=100.0).contains(&percent) {
            return Err(invalid_color(
                original,
                "alpha percentage must be between 0 and 100",
            ));
        }
        return Ok((percent * 2.55).round() as u8);
    }

    if let Ok(alpha) = value.parse::<f32>() {
        if alpha.is_finite() && (0.0..=1.0).contains(&alpha) {
            return Ok((alpha * 255.0).round() as u8);
        }
    }
    value.parse::<u8>().map_err(|_| {
        invalid_color(
            original,
            "alpha must be 0 through 255, 0.0 through 1.0, or a percentage",
        )
    })
}

fn invalid_color(value: &str, reason: &'static str) -> SceneError {
    SceneError::InvalidColor {
        value: value.to_string(),
        reason,
    }
}

/// Normalized anchors within a node's parent rectangle.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Anchors {
    #[serde(rename = "@left", default)]
    pub(crate) left: f32,
    #[serde(rename = "@top", default)]
    pub(crate) top: f32,
    #[serde(rename = "@right", default)]
    pub(crate) right: f32,
    #[serde(rename = "@bottom", default)]
    pub(crate) bottom: f32,
}

impl Default for Anchors {
    fn default() -> Self {
        Self {
            left: 0.0,
            top: 0.0,
            right: 0.0,
            bottom: 0.0,
        }
    }
}

/// Godot-style anchors combined with pixel offsets and drawing properties.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RectTransform {
    #[serde(rename = "@x", default)]
    pub(crate) x: f32,
    #[serde(rename = "@y", default)]
    pub(crate) y: f32,
    #[serde(rename = "@width", default)]
    pub(crate) width: f32,
    #[serde(rename = "@height", default)]
    pub(crate) height: f32,
    #[serde(rename = "@z_index", default)]
    pub(crate) z_index: i32,
    #[serde(rename = "@opacity", default = "default_opacity")]
    pub(crate) opacity: f32,
    #[serde(rename = "@clip_children", default)]
    pub(crate) clip_children: bool,
    #[serde(default)]
    pub(crate) anchors: Anchors,
}

impl Default for RectTransform {
    fn default() -> Self {
        Self {
            anchors: Anchors::default(),
            x: 0.0,
            y: 0.0,
            width: 0.0,
            height: 0.0,
            z_index: 0,
            opacity: default_opacity(),
            clip_children: false,
        }
    }
}

/// A resolved pixel rectangle after applying anchors and signed extent offsets.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ResolvedRect {
    pub(crate) x: f32,
    pub(crate) y: f32,
    pub(crate) width: f32,
    pub(crate) height: f32,
}

impl RectTransform {
    /// Resolves X and Y from the leading anchors and adds Width and Height to the anchor span.
    pub(crate) fn resolve(self, parent: ResolvedRect) -> Option<ResolvedRect> {
        let resolved = ResolvedRect {
            x: parent.x + parent.width * self.anchors.left + self.x,
            y: parent.y + parent.height * self.anchors.top + self.y,
            width: parent.width * (self.anchors.right - self.anchors.left) + self.width,
            height: parent.height * (self.anchors.bottom - self.anchors.top) + self.height,
        };
        let values = [resolved.x, resolved.y, resolved.width, resolved.height];
        if values.iter().all(|value| value.is_finite())
            && resolved.width >= 0.0
            && resolved.height >= 0.0
        {
            Some(resolved)
        } else {
            None
        }
    }
}

fn default_opacity() -> f32 {
    1.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VideoFit {
    Stretch,
    Contain,
    Cover,
}

impl Default for VideoFit {
    fn default() -> Self {
        Self::Contain
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GroupNode {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProgramVideoNode {
    #[serde(rename = "@fit", default)]
    pub(crate) fit: VideoFit,
    #[serde(rename = "@background", default = "default_black")]
    pub(crate) background: RgbaColor,
}

impl Default for ProgramVideoNode {
    fn default() -> Self {
        Self {
            fit: VideoFit::Contain,
            background: RgbaColor::BLACK,
        }
    }
}

fn default_black() -> RgbaColor {
    RgbaColor::BLACK
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ColorBinding {
    #[serde(rename = "Color_EAS_Warning")]
    EasWarning,
    #[serde(rename = "Color_EAS_Watch")]
    EasWatch,
    #[serde(rename = "Color_EAS_Advisory")]
    EasAdvisory,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColorSource {
    Static(RgbaColor),
    Binding(ColorBinding),
}

#[derive(Serialize)]
struct ColorSourceSerialize<'a> {
    #[serde(rename = "@value", skip_serializing_if = "Option::is_none")]
    value: Option<&'a RgbaColor>,
    #[serde(rename = "@binding", skip_serializing_if = "Option::is_none")]
    binding: Option<&'a ColorBinding>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ColorSourceDeserialize {
    #[serde(rename = "@value", default)]
    value: Option<RgbaColor>,
    #[serde(rename = "@binding", default)]
    binding: Option<ColorBinding>,
}

impl Serialize for ColorSource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = match self {
            Self::Static(value) => ColorSourceSerialize {
                value: Some(value),
                binding: None,
            },
            Self::Binding(binding) => ColorSourceSerialize {
                value: None,
                binding: Some(binding),
            },
        };
        encoded.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ColorSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let decoded = ColorSourceDeserialize::deserialize(deserializer)?;
        match (decoded.value, decoded.binding) {
            (Some(value), None) => Ok(Self::Static(value)),
            (None, Some(binding)) => Ok(Self::Binding(binding)),
            (None, None) => Err(de::Error::custom(
                "color source requires exactly one of value or binding",
            )),
            (Some(_), Some(_)) => Err(de::Error::custom(
                "color source cannot contain both value and binding",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColorNode {
    pub(crate) source: ColorSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum GradientBinding {
    #[serde(rename = "Color_EAS_Warning_Gradient")]
    EasWarningGradient,
    #[serde(rename = "Color_EAS_Watch_Gradient")]
    EasWatchGradient,
    #[serde(rename = "Color_EAS_Advisory_Gradient")]
    EasAdvisoryGradient,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GradientStop {
    #[serde(rename = "@offset")]
    pub(crate) offset: f32,
    #[serde(rename = "@color")]
    pub(crate) color: RgbaColor,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum GradientSource {
    Static(Vec<GradientStop>),
    Binding(GradientBinding),
}

#[derive(Serialize)]
struct GradientSourceSerialize<'a> {
    #[serde(rename = "@binding", skip_serializing_if = "Option::is_none")]
    binding: Option<&'a GradientBinding>,
    #[serde(rename = "stop", skip_serializing_if = "slice_reference_is_empty")]
    stops: &'a [GradientStop],
}

fn slice_reference_is_empty<T>(values: &&[T]) -> bool {
    values.is_empty()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GradientSourceDeserialize {
    #[serde(rename = "@binding", default)]
    binding: Option<GradientBinding>,
    #[serde(rename = "stop", default)]
    stops: Vec<GradientStop>,
}

impl Serialize for GradientSource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = match self {
            Self::Static(stops) => GradientSourceSerialize {
                binding: None,
                stops,
            },
            Self::Binding(binding) => GradientSourceSerialize {
                binding: Some(binding),
                stops: &[],
            },
        };
        encoded.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GradientSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let decoded = GradientSourceDeserialize::deserialize(deserializer)?;
        match (decoded.binding, decoded.stops.is_empty()) {
            (Some(binding), true) => Ok(Self::Binding(binding)),
            (None, false) => Ok(Self::Static(decoded.stops)),
            (Some(_), false) => Err(de::Error::custom(
                "gradient source cannot contain both binding and static stops",
            )),
            (None, true) => Err(de::Error::custom(
                "gradient source requires a binding or static stops",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColorGradientNode {
    #[serde(rename = "@angle_degrees", default)]
    pub(crate) angle_degrees: f32,
    pub(crate) source: GradientSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TextBinding {
    #[serde(rename = "Text_Alert_Org")]
    AlertOrganization,
    #[serde(rename = "Text_Alert_Action")]
    AlertAction,
    #[serde(rename = "Text_Alert_Event")]
    AlertEvent,
    #[serde(rename = "Text_Alert_Full")]
    AlertFull,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TextSource {
    Static(String),
    Binding(TextBinding),
}

#[derive(Serialize)]
struct TextSourceSerialize<'a> {
    #[serde(rename = "@value", skip_serializing_if = "Option::is_none")]
    value: Option<&'a str>,
    #[serde(rename = "@binding", skip_serializing_if = "Option::is_none")]
    binding: Option<&'a TextBinding>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TextSourceDeserialize {
    #[serde(rename = "@value", default)]
    value: Option<String>,
    #[serde(rename = "@binding", default)]
    binding: Option<TextBinding>,
}

impl Serialize for TextSource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = match self {
            Self::Static(value) => TextSourceSerialize {
                value: Some(value),
                binding: None,
            },
            Self::Binding(binding) => TextSourceSerialize {
                value: None,
                binding: Some(binding),
            },
        };
        encoded.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TextSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let decoded = TextSourceDeserialize::deserialize(deserializer)?;
        match (decoded.value, decoded.binding) {
            (Some(value), None) => Ok(Self::Static(value)),
            (None, Some(binding)) => Ok(Self::Binding(binding)),
            (None, None) => Err(de::Error::custom(
                "text source requires exactly one of value or binding",
            )),
            (Some(_), Some(_)) => Err(de::Error::custom(
                "text source cannot contain both value and binding",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HorizontalAlignment {
    Left,
    Right,
    Center,
    Justify,
}

impl Default for HorizontalAlignment {
    fn default() -> Self {
        Self::Left
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum VerticalAlignment {
    Top,
    Center,
    Bottom,
}

impl Default for VerticalAlignment {
    fn default() -> Self {
        Self::Top
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScrollDirection {
    Left,
    Right,
    Up,
    Down,
}

impl Default for ScrollDirection {
    fn default() -> Self {
        Self::Left
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TextOutline {
    #[serde(rename = "@width", default)]
    pub(crate) width: f32,
    #[serde(rename = "@color", default = "default_black")]
    pub(crate) color: RgbaColor,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TextShadow {
    #[serde(rename = "@x", default)]
    pub(crate) x: f32,
    #[serde(rename = "@y", default)]
    pub(crate) y: f32,
    #[serde(rename = "@blur", default)]
    pub(crate) blur: f32,
    #[serde(rename = "@color", default = "default_shadow")]
    pub(crate) color: RgbaColor,
}

fn default_shadow() -> RgbaColor {
    RgbaColor::new(0, 0, 0, 160)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TextStyle {
    #[serde(rename = "@font_family", default)]
    pub(crate) font_family: String,
    #[serde(rename = "@weight", default = "default_font_weight")]
    pub(crate) weight: u16,
    #[serde(rename = "@size", default = "default_font_size")]
    pub(crate) size: f32,
    #[serde(rename = "@color", default = "default_white")]
    pub(crate) color: RgbaColor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) outline: Option<TextOutline>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) shadow: Option<TextShadow>,
}

impl Default for TextStyle {
    fn default() -> Self {
        Self {
            font_family: String::new(),
            weight: default_font_weight(),
            size: default_font_size(),
            color: RgbaColor::WHITE,
            outline: None,
            shadow: None,
        }
    }
}

fn default_font_weight() -> u16 {
    400
}

fn default_font_size() -> f32 {
    48.0
}

fn default_white() -> RgbaColor {
    RgbaColor::WHITE
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TextLayout {
    #[serde(rename = "@horizontal", default)]
    pub(crate) horizontal: HorizontalAlignment,
    #[serde(rename = "@vertical", default)]
    pub(crate) vertical: VerticalAlignment,
    #[serde(rename = "@auto_wrap", default = "default_true")]
    pub(crate) auto_wrap: bool,
    #[serde(rename = "@clip", default = "default_true")]
    pub(crate) clip: bool,
}

impl Default for TextLayout {
    fn default() -> Self {
        Self {
            horizontal: HorizontalAlignment::Left,
            vertical: VerticalAlignment::Top,
            auto_wrap: true,
            clip: true,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TextScroll {
    #[serde(rename = "@enabled", default)]
    pub(crate) enabled: bool,
    #[serde(rename = "@direction", default)]
    pub(crate) direction: ScrollDirection,
    #[serde(rename = "@pixels_per_second", default)]
    pub(crate) pixels_per_second: f32,
    #[serde(rename = "@gap", default)]
    pub(crate) gap: f32,
    #[serde(rename = "@repeat", default = "default_true")]
    pub(crate) repeat: bool,
}

impl Default for TextScroll {
    fn default() -> Self {
        Self {
            enabled: false,
            direction: ScrollDirection::Left,
            pixels_per_second: 0.0,
            gap: 0.0,
            repeat: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TextNode {
    pub(crate) source: TextSource,
    #[serde(default)]
    pub(crate) style: TextStyle,
    #[serde(default)]
    pub(crate) layout: TextLayout,
    #[serde(default)]
    pub(crate) scroll: TextScroll,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImageNode {
    #[serde(rename = "@asset")]
    pub(crate) asset: AssetId,
    #[serde(rename = "@fit", default)]
    pub(crate) fit: VideoFit,
    #[serde(rename = "@tint", default = "default_white")]
    pub(crate) tint: RgbaColor,
}

/// The concrete payload for a scene node.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum NodeKind {
    Group(GroupNode),
    ProgramVideo(ProgramVideoNode),
    Color(ColorNode),
    ColorGradient(ColorGradientNode),
    Text(TextNode),
    Image(ImageNode),
}

/// A node in the scene tree. Child order is the stable tie-breaker for Z-index.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SceneNode {
    pub(crate) id: NodeId,
    pub(crate) name: String,
    pub(crate) enabled: bool,
    pub(crate) transform: RectTransform,
    pub(crate) kind: NodeKind,
    pub(crate) children: Vec<SceneNode>,
}

#[derive(Serialize)]
struct SceneNodeSerialize<'a> {
    #[serde(rename = "@id")]
    id: &'a NodeId,
    #[serde(rename = "@name")]
    name: &'a str,
    #[serde(rename = "@enabled")]
    enabled: bool,
    transform: &'a RectTransform,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<&'a GroupNode>,
    #[serde(rename = "programVideo", skip_serializing_if = "Option::is_none")]
    program_video: Option<&'a ProgramVideoNode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    color: Option<&'a ColorNode>,
    #[serde(rename = "colorGradient", skip_serializing_if = "Option::is_none")]
    color_gradient: Option<&'a ColorGradientNode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<&'a TextNode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<&'a ImageNode>,
    #[serde(rename = "node", skip_serializing_if = "slice_reference_is_empty")]
    children: &'a [SceneNode],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SceneNodeDeserialize {
    #[serde(rename = "@id")]
    id: NodeId,
    #[serde(rename = "@name", default)]
    name: String,
    #[serde(rename = "@enabled", default = "default_true")]
    enabled: bool,
    #[serde(default)]
    transform: RectTransform,
    #[serde(default)]
    group: Option<GroupNode>,
    #[serde(rename = "programVideo", default)]
    program_video: Option<ProgramVideoNode>,
    #[serde(default)]
    color: Option<ColorNode>,
    #[serde(rename = "colorGradient", default)]
    color_gradient: Option<ColorGradientNode>,
    #[serde(default)]
    text: Option<TextNode>,
    #[serde(default)]
    image: Option<ImageNode>,
    #[serde(rename = "node", default)]
    children: Vec<SceneNode>,
}

impl Serialize for SceneNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut encoded = SceneNodeSerialize {
            id: &self.id,
            name: &self.name,
            enabled: self.enabled,
            transform: &self.transform,
            group: None,
            program_video: None,
            color: None,
            color_gradient: None,
            text: None,
            image: None,
            children: &self.children,
        };
        match &self.kind {
            NodeKind::Group(value) => encoded.group = Some(value),
            NodeKind::ProgramVideo(value) => encoded.program_video = Some(value),
            NodeKind::Color(value) => encoded.color = Some(value),
            NodeKind::ColorGradient(value) => encoded.color_gradient = Some(value),
            NodeKind::Text(value) => encoded.text = Some(value),
            NodeKind::Image(value) => encoded.image = Some(value),
        }
        encoded.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SceneNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let decoded = SceneNodeDeserialize::deserialize(deserializer)?;
        let populated = [
            decoded.group.is_some(),
            decoded.program_video.is_some(),
            decoded.color.is_some(),
            decoded.color_gradient.is_some(),
            decoded.text.is_some(),
            decoded.image.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if populated != 1 {
            return Err(de::Error::custom(format!(
                "node `{}` must contain exactly one node kind",
                decoded.id
            )));
        }

        let kind = if let Some(value) = decoded.group {
            NodeKind::Group(value)
        } else if let Some(value) = decoded.program_video {
            NodeKind::ProgramVideo(value)
        } else if let Some(value) = decoded.color {
            NodeKind::Color(value)
        } else if let Some(value) = decoded.color_gradient {
            NodeKind::ColorGradient(value)
        } else if let Some(value) = decoded.text {
            NodeKind::Text(value)
        } else if let Some(value) = decoded.image {
            NodeKind::Image(value)
        } else {
            return Err(de::Error::custom("node kind was not present"));
        };

        Ok(Self {
            id: decoded.id,
            name: decoded.name,
            enabled: decoded.enabled,
            transform: decoded.transform,
            kind,
            children: decoded.children,
        })
    }
}

/// A versioned scene document stored as one XML file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename = "scene")]
#[serde(deny_unknown_fields)]
pub(crate) struct SceneDocument {
    #[serde(rename = "@schema_version")]
    pub(crate) schema_version: u16,
    #[serde(rename = "@id")]
    pub(crate) id: SceneId,
    #[serde(rename = "@name")]
    pub(crate) name: String,
    #[serde(rename = "node")]
    pub(crate) root: SceneNode,
}

impl SceneDocument {
    /// Validates all document invariants before the scene reaches runtime state.
    pub(crate) fn validate(&self) -> Result<(), SceneError> {
        if self.schema_version != SCENE_SCHEMA_VERSION {
            return Err(SceneError::UnsupportedSchema {
                scene_id: self.id.clone(),
                actual: self.schema_version,
                expected: SCENE_SCHEMA_VERSION,
            });
        }
        validate_scene_name(self)?;
        validate_protected_name(self)?;

        let mut ids = HashSet::new();
        let mut stack = vec![(&self.root, 1_usize)];
        let mut node_count = 0_usize;
        while let Some((node, depth)) = stack.pop() {
            node_count += 1;
            if node_count > MAX_SCENE_NODES {
                return Err(SceneError::MaximumNodeCountExceeded {
                    scene_id: self.id.clone(),
                    maximum: MAX_SCENE_NODES,
                });
            }
            if depth > MAX_SCENE_DEPTH {
                return Err(SceneError::MaximumDepthExceeded {
                    scene_id: self.id.clone(),
                    maximum: MAX_SCENE_DEPTH,
                });
            }
            if !ids.insert(node.id.as_str().to_ascii_lowercase()) {
                return Err(SceneError::DuplicateNodeId {
                    scene_id: self.id.clone(),
                    node_id: node.id.clone(),
                });
            }
            validate_node(self, node)?;
            stack.extend(node.children.iter().rev().map(|child| (child, depth + 1)));
        }
        Ok(())
    }

    /// Serializes a validated scene into its managed XML representation.
    pub(crate) fn to_xml(&self) -> Result<String, SceneError> {
        self.validate()?;
        quick_xml::se::to_string(self).map_err(|error| SceneError::XmlSerialize(error.to_string()))
    }

    /// Compiles the special passthrough scene without creating draw work.
    pub(crate) fn compile(&self) -> Result<RenderPlan<'_>, SceneError> {
        self.validate()?;
        if self.id.as_str() == PROGRAM_PASSTHROUGH_ID {
            Ok(RenderPlan::Bypass)
        } else {
            Ok(RenderPlan::Composite { scene: self })
        }
    }
}

fn validate_scene_name(scene: &SceneDocument) -> Result<(), SceneError> {
    if scene.name.trim().is_empty() {
        return Err(invalid_scene(scene, "the scene name cannot be empty"));
    }
    if scene.name.trim() != scene.name {
        return Err(invalid_scene(
            scene,
            "the scene name cannot contain leading or trailing whitespace",
        ));
    }
    if scene.name.chars().count() > MAX_SCENE_NAME_LENGTH {
        return Err(invalid_scene(scene, "the scene name is too long"));
    }
    Ok(())
}

fn validate_protected_name(scene: &SceneDocument) -> Result<(), SceneError> {
    if scene.id.protected_kind().is_some() && scene.name != scene.id.as_str() {
        return Err(invalid_scene(
            scene,
            "a protected scene name must match its stable scene ID",
        ));
    }
    Ok(())
}

fn validate_node(scene: &SceneDocument, node: &SceneNode) -> Result<(), SceneError> {
    if node.name.trim().is_empty() {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` has an empty name", node.id),
        ));
    }
    if node.name.trim() != node.name || node.name.chars().count() > MAX_NODE_NAME_LENGTH {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` has an invalid or overlong name", node.id),
        ));
    }
    validate_transform(scene, node)?;

    match &node.kind {
        NodeKind::Group(_) | NodeKind::ProgramVideo(_) | NodeKind::Color(_) => Ok(()),
        NodeKind::ColorGradient(gradient) => validate_gradient(scene, node, gradient),
        NodeKind::Text(text) => validate_text(scene, node, text),
        NodeKind::Image(_) => Ok(()),
    }
}

fn validate_transform(scene: &SceneDocument, node: &SceneNode) -> Result<(), SceneError> {
    let transform = node.transform;
    let finite_values = [
        transform.anchors.left,
        transform.anchors.top,
        transform.anchors.right,
        transform.anchors.bottom,
        transform.x,
        transform.y,
        transform.width,
        transform.height,
        transform.opacity,
    ];
    if finite_values.iter().any(|value| !value.is_finite()) {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` contains a non-finite transform value", node.id),
        ));
    }
    if [transform.x, transform.y, transform.width, transform.height]
        .iter()
        .any(|value| value.abs() > MAX_ABSOLUTE_GEOMETRY)
    {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` geometry exceeds the supported range", node.id),
        ));
    }
    let anchors = transform.anchors;
    if !(0.0..=1.0).contains(&anchors.left)
        || !(0.0..=1.0).contains(&anchors.top)
        || !(0.0..=1.0).contains(&anchors.right)
        || !(0.0..=1.0).contains(&anchors.bottom)
        || anchors.left > anchors.right
        || anchors.top > anchors.bottom
    {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` has invalid anchors", node.id),
        ));
    }
    if !(0.0..=1.0).contains(&transform.opacity) {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` opacity must be between 0 and 1", node.id),
        ));
    }
    Ok(())
}

fn validate_gradient(
    scene: &SceneDocument,
    node: &SceneNode,
    gradient: &ColorGradientNode,
) -> Result<(), SceneError> {
    if !gradient.angle_degrees.is_finite() || gradient.angle_degrees.abs() > 36_000.0 {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` has a non-finite gradient angle", node.id),
        ));
    }
    let GradientSource::Static(stops) = &gradient.source else {
        return Ok(());
    };
    if stops.len() < 2 {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` requires at least two gradient stops", node.id),
        ));
    }
    if stops.len() > MAX_GRADIENT_STOPS {
        return Err(invalid_scene(
            scene,
            &format!(
                "node `{}` exceeds the maximum of {MAX_GRADIENT_STOPS} gradient stops",
                node.id
            ),
        ));
    }
    let mut previous = None;
    for stop in stops {
        if !stop.offset.is_finite() || !(0.0..=1.0).contains(&stop.offset) {
            return Err(invalid_scene(
                scene,
                &format!("node `{}` has a gradient stop outside 0 through 1", node.id),
            ));
        }
        if previous.is_some_and(|offset| stop.offset < offset) {
            return Err(invalid_scene(
                scene,
                &format!("node `{}` gradient stops are not ordered", node.id),
            ));
        }
        previous = Some(stop.offset);
    }
    Ok(())
}

fn validate_text(
    scene: &SceneDocument,
    node: &SceneNode,
    text: &TextNode,
) -> Result<(), SceneError> {
    if let TextSource::Static(value) = &text.source {
        if value.chars().count() > MAX_STATIC_TEXT_LENGTH {
            return Err(invalid_scene(
                scene,
                &format!("node `{}` static text is too long", node.id),
            ));
        }
    }
    if text.style.font_family.chars().count() > MAX_FONT_FAMILY_LENGTH {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` font family is too long", node.id),
        ));
    }
    if !(1..=1_000).contains(&text.style.weight) {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` font weight must be 1 through 1000", node.id),
        ));
    }
    if !text.style.size.is_finite() || text.style.size <= 0.0 || text.style.size > 4_096.0 {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` font size must be positive", node.id),
        ));
    }
    if let Some(outline) = text.style.outline {
        if !outline.width.is_finite() || !(0.0..=1_024.0).contains(&outline.width) {
            return Err(invalid_scene(
                scene,
                &format!("node `{}` outline width must be non-negative", node.id),
            ));
        }
    }
    if let Some(shadow) = text.style.shadow {
        if !shadow.x.is_finite()
            || !shadow.y.is_finite()
            || !shadow.blur.is_finite()
            || shadow.x.abs() > MAX_ABSOLUTE_GEOMETRY
            || shadow.y.abs() > MAX_ABSOLUTE_GEOMETRY
            || !(0.0..=1_024.0).contains(&shadow.blur)
        {
            return Err(invalid_scene(
                scene,
                &format!("node `{}` has invalid shadow values", node.id),
            ));
        }
    }
    let scroll_speed_valid = text.scroll.pixels_per_second.is_finite()
        && (0.0..=MAX_ABSOLUTE_GEOMETRY).contains(&text.scroll.pixels_per_second)
        && (!text.scroll.enabled || text.scroll.pixels_per_second > 0.0);
    let scroll_gap_valid =
        text.scroll.gap.is_finite() && (0.0..=MAX_ABSOLUTE_GEOMETRY).contains(&text.scroll.gap);
    if !scroll_speed_valid || !scroll_gap_valid {
        return Err(invalid_scene(
            scene,
            &format!("node `{}` has invalid scrolling values", node.id),
        ));
    }
    Ok(())
}

fn invalid_scene(scene: &SceneDocument, reason: impl Into<String>) -> SceneError {
    SceneError::InvalidScene {
        scene_id: scene.id.clone(),
        reason: reason.into(),
    }
}

/// The compositor decision produced from a validated scene document.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum RenderPlan<'a> {
    Bypass,
    Composite { scene: &'a SceneDocument },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProtectedSceneKind {
    ProgramPassthrough,
    StandardCrawl,
    FullscreenTakeover,
    Standby,
}

impl ProtectedSceneKind {
    pub(crate) const ALL: [Self; 4] = [
        Self::ProgramPassthrough,
        Self::StandardCrawl,
        Self::FullscreenTakeover,
        Self::Standby,
    ];

    pub(crate) fn id(self) -> SceneId {
        let value = match self {
            Self::ProgramPassthrough => PROGRAM_PASSTHROUGH_ID,
            Self::StandardCrawl => STANDARD_CRAWL_ID,
            Self::FullscreenTakeover => FULLSCREEN_TAKEOVER_ID,
            Self::Standby => STANDBY_ID,
        };
        SceneId(value.to_string())
    }

    pub(crate) fn filename(self) -> &'static str {
        match self {
            Self::ProgramPassthrough => "program_passthrough.xml",
            Self::StandardCrawl => "crawl.xml",
            Self::FullscreenTakeover => "fullscreen.xml",
            Self::Standby => "standby.xml",
        }
    }

    pub(crate) fn protection(self) -> SceneProtection {
        match self {
            Self::ProgramPassthrough => SceneProtection::Locked,
            Self::StandardCrawl | Self::FullscreenTakeover | Self::Standby => {
                SceneProtection::ProtectedEditable
            }
        }
    }
}

impl SceneId {
    pub(crate) fn protected_kind(&self) -> Option<ProtectedSceneKind> {
        match self.as_str() {
            PROGRAM_PASSTHROUGH_ID => Some(ProtectedSceneKind::ProgramPassthrough),
            STANDARD_CRAWL_ID => Some(ProtectedSceneKind::StandardCrawl),
            FULLSCREEN_TAKEOVER_ID => Some(ProtectedSceneKind::FullscreenTakeover),
            STANDBY_ID => Some(ProtectedSceneKind::Standby),
            _ => None,
        }
    }

    pub(crate) fn protection(&self) -> SceneProtection {
        self.protected_kind()
            .map(ProtectedSceneKind::protection)
            .unwrap_or(SceneProtection::Unprotected)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SceneProtection {
    Unprotected,
    ProtectedEditable,
    Locked,
}

pub(crate) fn validate_scene_delete(scene_id: &SceneId) -> Result<(), SceneError> {
    if scene_id.protection() == SceneProtection::Unprotected {
        Ok(())
    } else {
        Err(SceneError::ProtectedSceneDelete {
            scene_id: scene_id.clone(),
        })
    }
}

pub(crate) fn validate_scene_update(
    existing: &SceneDocument,
    replacement: &SceneDocument,
) -> Result<(), SceneError> {
    replacement.validate()?;
    match existing.id.protection() {
        SceneProtection::Unprotected => {
            if replacement.id.protection() == SceneProtection::Unprotected {
                Ok(())
            } else {
                Err(SceneError::ProtectedSceneRename {
                    scene_id: replacement.id.clone(),
                })
            }
        }
        SceneProtection::ProtectedEditable => {
            if existing.id != replacement.id || existing.name != replacement.name {
                Err(SceneError::ProtectedSceneRename {
                    scene_id: existing.id.clone(),
                })
            } else {
                Ok(())
            }
        }
        SceneProtection::Locked => {
            if existing == replacement {
                Ok(())
            } else {
                Err(SceneError::LockedSceneEdit {
                    scene_id: existing.id.clone(),
                })
            }
        }
    }
}

pub(crate) fn validate_alert_scene(scene_id: &SceneId) -> Result<(), SceneError> {
    if matches!(scene_id.as_str(), PROGRAM_PASSTHROUGH_ID | STANDBY_ID) {
        Err(SceneError::InvalidAlertScene {
            scene_id: scene_id.clone(),
        })
    } else {
        Ok(())
    }
}

pub(crate) fn parse_scene_xml(xml: &str) -> Result<SceneDocument, SceneError> {
    if xml.len() as u64 > MAX_SCENE_XML_BYTES {
        return Err(SceneError::XmlDeserialize(format!(
            "scene exceeds the maximum XML size of {MAX_SCENE_XML_BYTES} bytes"
        )));
    }
    preflight_scene_xml(xml)?;
    let scene: SceneDocument = quick_xml::de::from_str(xml)
        .map_err(|error| SceneError::XmlDeserialize(error.to_string()))?;
    scene.validate()?;
    Ok(scene)
}

fn preflight_scene_xml(xml: &str) -> Result<(), SceneError> {
    let mut reader = Reader::from_str(xml);
    let mut node_depth = 0_usize;
    let mut node_count = 0_usize;
    loop {
        match reader.read_event() {
            Ok(Event::Start(start)) if start.name().as_ref() == "node" => {
                node_depth += 1;
                node_count += 1;
                if node_depth > MAX_SCENE_DEPTH {
                    return Err(SceneError::XmlDeserialize(format!(
                        "scene exceeds the maximum node depth of {MAX_SCENE_DEPTH}"
                    )));
                }
                if node_count > MAX_SCENE_NODES {
                    return Err(SceneError::XmlDeserialize(format!(
                        "scene exceeds the maximum node count of {MAX_SCENE_NODES}"
                    )));
                }
            }
            Ok(Event::Empty(empty)) if empty.name().as_ref() == "node" => {
                node_count += 1;
                if node_depth + 1 > MAX_SCENE_DEPTH {
                    return Err(SceneError::XmlDeserialize(format!(
                        "scene exceeds the maximum node depth of {MAX_SCENE_DEPTH}"
                    )));
                }
                if node_count > MAX_SCENE_NODES {
                    return Err(SceneError::XmlDeserialize(format!(
                        "scene exceeds the maximum node count of {MAX_SCENE_NODES}"
                    )));
                }
            }
            Ok(Event::End(end)) if end.name().as_ref() == "node" => {
                node_depth = node_depth.saturating_sub(1);
            }
            Ok(Event::Eof) => return Ok(()),
            Ok(_) => {}
            Err(error) => return Err(SceneError::XmlDeserialize(error.to_string())),
        }
    }
}

pub(crate) fn load_scene_file(path: &Path) -> Result<SceneDocument, SceneError> {
    let metadata = fs::metadata(path).map_err(|source| SceneError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.len() > MAX_SCENE_XML_BYTES {
        return Err(SceneError::SceneFileTooLarge {
            path: path.to_path_buf(),
            maximum: MAX_SCENE_XML_BYTES,
        });
    }
    let file = fs::File::open(path).map_err(|source| SceneError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    let mut xml = String::new();
    file.take(MAX_SCENE_XML_BYTES + 1)
        .read_to_string(&mut xml)
        .map_err(|source| SceneError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
    if xml.len() as u64 > MAX_SCENE_XML_BYTES {
        return Err(SceneError::SceneFileTooLarge {
            path: path.to_path_buf(),
            maximum: MAX_SCENE_XML_BYTES,
        });
    }
    parse_scene_xml(&xml)
}

/// A non-fatal problem encountered while loading a scene catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SceneLoadWarning {
    pub(crate) path: Option<PathBuf>,
    pub(crate) scene_id: Option<SceneId>,
    pub(crate) message: String,
}

/// Loaded scenes plus degraded-state warnings suitable for CGEN status events.
#[derive(Debug, Clone)]
pub(crate) struct SceneCatalog {
    scenes: BTreeMap<SceneId, SceneDocument>,
    warnings: Vec<SceneLoadWarning>,
}

impl SceneCatalog {
    pub(crate) fn scenes(&self) -> impl Iterator<Item = &SceneDocument> {
        self.scenes.values()
    }

    pub(crate) fn scene(&self, id: &SceneId) -> Option<&SceneDocument> {
        self.scenes.get(id)
    }

    /// Validates an alert selection and proves that the scene is loaded.
    pub(crate) fn alert_scene(&self, id: &SceneId) -> Result<&SceneDocument, SceneError> {
        validate_alert_scene(id)?;
        self.scene(id).ok_or_else(|| SceneError::InvalidAlertScene {
            scene_id: id.clone(),
        })
    }

    pub(crate) fn warnings(&self) -> &[SceneLoadWarning] {
        &self.warnings
    }

    pub(crate) fn is_degraded(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// Loads all XML scenes without creating, replacing, or repairing managed files.
pub(crate) fn load_scene_directory(directory: &Path) -> SceneCatalog {
    let mut scenes = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut paths = Vec::new();

    match fs::read_dir(directory) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(entry) => {
                        let path = entry.path();
                        if path
                            .extension()
                            .and_then(|extension| extension.to_str())
                            .is_some_and(|extension| extension.eq_ignore_ascii_case("xml"))
                        {
                            paths.push(path);
                        }
                    }
                    Err(error) => warnings.push(SceneLoadWarning {
                        path: Some(directory.to_path_buf()),
                        scene_id: None,
                        message: format!("scene directory entry could not be read: {error}"),
                    }),
                }
            }
        }
        Err(error) => warnings.push(SceneLoadWarning {
            path: Some(directory.to_path_buf()),
            scene_id: None,
            message: format!("scene directory could not be read: {error}"),
        }),
    }
    paths.sort();

    for path in paths {
        match load_scene_file(&path) {
            Ok(scene) => {
                if let Some(kind) = scene.id.protected_kind() {
                    let actual_filename = path.file_name().and_then(|name| name.to_str());
                    if actual_filename != Some(kind.filename()) {
                        warnings.push(SceneLoadWarning {
                            path: Some(path),
                            scene_id: Some(scene.id),
                            message: format!(
                                "protected scene must use filename `{}`",
                                kind.filename()
                            ),
                        });
                        continue;
                    }
                } else if ProtectedSceneKind::ALL.iter().any(|kind| {
                    path.file_name().and_then(|name| name.to_str()) == Some(kind.filename())
                }) {
                    warnings.push(SceneLoadWarning {
                        path: Some(path),
                        scene_id: Some(scene.id),
                        message: "protected scene filename contains a custom scene ID".to_string(),
                    });
                    continue;
                }

                let scene_id = scene.id.clone();
                if scenes.keys().any(|existing: &SceneId| {
                    existing.as_str().eq_ignore_ascii_case(scene_id.as_str())
                }) {
                    warnings.push(SceneLoadWarning {
                        path: Some(path),
                        scene_id: Some(scene_id.clone()),
                        message: format!(
                            "duplicate scene ID `{scene_id}` was ignored after the first definition"
                        ),
                    });
                } else {
                    scenes.insert(scene_id, scene);
                }
            }
            Err(error) => warnings.push(SceneLoadWarning {
                path: Some(path),
                scene_id: None,
                message: error.to_string(),
            }),
        }
    }

    for kind in ProtectedSceneKind::ALL {
        let id = kind.id();
        if !scenes.contains_key(&id) {
            scenes.insert(id.clone(), protected_default_scene(kind));
            warnings.push(SceneLoadWarning {
                path: Some(directory.join(kind.filename())),
                scene_id: Some(id),
                message: "embedded protected scene default is active".to_string(),
            });
        }
    }

    SceneCatalog { scenes, warnings }
}

pub(crate) fn protected_default_scene(kind: ProtectedSceneKind) -> SceneDocument {
    match kind {
        ProtectedSceneKind::ProgramPassthrough => scene_document(
            PROGRAM_PASSTHROUGH_ID,
            group_node("root", "Program Passthrough", full_canvas(), Vec::new()),
        ),
        ProtectedSceneKind::StandardCrawl => scene_document(
            STANDARD_CRAWL_ID,
            group_node(
                "root",
                "Standard Crawl",
                full_canvas(),
                vec![
                    scene_node(
                        "program_video",
                        "Program Video",
                        full_canvas(),
                        NodeKind::ProgramVideo(ProgramVideoNode {
                            fit: VideoFit::Stretch,
                            background: RgbaColor::BLACK,
                        }),
                    ),
                    scene_node(
                        "crawl_background",
                        "Crawl Background",
                        bottom_strip(120.0, 10),
                        NodeKind::ColorGradient(ColorGradientNode {
                            source: GradientSource::Binding(GradientBinding::EasWarningGradient),
                            angle_degrees: 0.0,
                        }),
                    ),
                    scene_node(
                        "crawl_text",
                        "Alert Text",
                        bottom_strip(120.0, 20),
                        NodeKind::Text(default_text_node(TextSource::Binding(
                            TextBinding::AlertFull,
                        ))),
                    ),
                ],
            ),
        ),
        ProtectedSceneKind::FullscreenTakeover => scene_document(
            FULLSCREEN_TAKEOVER_ID,
            group_node(
                "root",
                "Fullscreen Takeover",
                full_canvas(),
                vec![
                    scene_node(
                        "takeover_background",
                        "Takeover Background",
                        full_canvas(),
                        NodeKind::ColorGradient(ColorGradientNode {
                            source: GradientSource::Binding(GradientBinding::EasWarningGradient),
                            angle_degrees: 90.0,
                        }),
                    ),
                    scene_node(
                        "takeover_text",
                        "Alert Text",
                        inset_canvas(80.0, 10),
                        NodeKind::Text(TextNode {
                            source: TextSource::Binding(TextBinding::AlertFull),
                            style: TextStyle {
                                size: 64.0,
                                ..TextStyle::default()
                            },
                            layout: TextLayout {
                                horizontal: HorizontalAlignment::Center,
                                vertical: VerticalAlignment::Center,
                                ..TextLayout::default()
                            },
                            scroll: TextScroll::default(),
                        }),
                    ),
                ],
            ),
        ),
        ProtectedSceneKind::Standby => scene_document(
            STANDBY_ID,
            group_node(
                "root",
                "Standby",
                full_canvas(),
                vec![
                    scene_node(
                        "standby_background",
                        "Standby Background",
                        full_canvas(),
                        NodeKind::Color(ColorNode {
                            source: ColorSource::Static(RgbaColor::BLACK),
                        }),
                    ),
                    scene_node(
                        "standby_title",
                        "Standby Title",
                        inset_canvas(64.0, 10),
                        NodeKind::Text(TextNode {
                            source: TextSource::Static(STANDBY_TITLE.to_string()),
                            style: TextStyle {
                                size: 56.0,
                                ..TextStyle::default()
                            },
                            layout: TextLayout {
                                horizontal: HorizontalAlignment::Center,
                                vertical: VerticalAlignment::Center,
                                ..TextLayout::default()
                            },
                            scroll: TextScroll::default(),
                        }),
                    ),
                ],
            ),
        ),
    }
}

fn scene_document(id: &str, root: SceneNode) -> SceneDocument {
    SceneDocument {
        schema_version: SCENE_SCHEMA_VERSION,
        id: SceneId(id.to_string()),
        name: id.to_string(),
        root,
    }
}

fn scene_node(id: &str, name: &str, transform: RectTransform, kind: NodeKind) -> SceneNode {
    SceneNode {
        id: NodeId(id.to_string()),
        name: name.to_string(),
        enabled: true,
        transform,
        kind,
        children: Vec::new(),
    }
}

fn group_node(
    id: &str,
    name: &str,
    transform: RectTransform,
    children: Vec<SceneNode>,
) -> SceneNode {
    SceneNode {
        id: NodeId(id.to_string()),
        name: name.to_string(),
        enabled: true,
        transform,
        kind: NodeKind::Group(GroupNode::default()),
        children,
    }
}

fn full_canvas() -> RectTransform {
    RectTransform {
        anchors: Anchors {
            left: 0.0,
            top: 0.0,
            right: 1.0,
            bottom: 1.0,
        },
        ..RectTransform::default()
    }
}

fn inset_canvas(inset: f32, z_index: i32) -> RectTransform {
    RectTransform {
        anchors: Anchors {
            left: 0.0,
            top: 0.0,
            right: 1.0,
            bottom: 1.0,
        },
        x: inset,
        y: inset,
        width: inset * -2.0,
        height: inset * -2.0,
        z_index,
        ..RectTransform::default()
    }
}

fn bottom_strip(height: f32, z_index: i32) -> RectTransform {
    RectTransform {
        anchors: Anchors {
            left: 0.0,
            top: 1.0,
            right: 1.0,
            bottom: 1.0,
        },
        y: -height,
        height,
        z_index,
        ..RectTransform::default()
    }
}

fn default_text_node(source: TextSource) -> TextNode {
    TextNode {
        source,
        style: TextStyle::default(),
        layout: TextLayout {
            vertical: VerticalAlignment::Center,
            auto_wrap: false,
            ..TextLayout::default()
        },
        scroll: TextScroll {
            enabled: true,
            pixels_per_second: 120.0,
            gap: 80.0,
            ..TextScroll::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn custom_scene(root: SceneNode) -> SceneDocument {
        SceneDocument {
            schema_version: SCENE_SCHEMA_VERSION,
            id: SceneId::new("custom_scene").expect("valid scene ID"),
            name: "Custom Scene".to_string(),
            root,
        }
    }

    fn test_group(id: &str, children: Vec<SceneNode>) -> SceneNode {
        group_node(id, id, full_canvas(), children)
    }

    #[test]
    fn validated_identifiers_reject_ambiguous_or_unsafe_values() {
        assert!(SceneId::new("").is_err());
        assert!(SceneId::new(" crawl").is_err());
        assert!(SceneId::new("crawl/../../other").is_err());
        assert!(SceneId::new("standby").is_err());
        assert!(SceneId::new("CON").is_err());
        assert!(NodeId::new("..").is_err());
        assert_eq!(
            SceneId::new(STANDBY_ID)
                .expect("canonical protected ID")
                .as_str(),
            STANDBY_ID
        );
        assert!(NodeId::new("headline.primary").is_ok());
    }

    #[test]
    fn rgba_parser_accepts_hex_and_css_forms() {
        assert_eq!(
            "#102030".parse::<RgbaColor>().expect("hex color"),
            RgbaColor::new(16, 32, 48, 255)
        );
        assert_eq!(
            "#0F08".parse::<RgbaColor>().expect("short RGBA color"),
            RgbaColor::new(0, 255, 0, 136)
        );
        assert_eq!(
            "rgba(255, 128, 0, 0.5)"
                .parse::<RgbaColor>()
                .expect("CSS RGBA color"),
            RgbaColor::new(255, 128, 0, 128)
        );
        assert!("#not-a-color".parse::<RgbaColor>().is_err());
    }

    #[test]
    fn asset_ids_are_confined_to_the_managed_asset_directory() {
        let asset = AssetId::new("station/logos/main.png").expect("safe relative asset");
        assert_eq!(
            asset.path_under(Path::new("managed")),
            Path::new("managed")
                .join("cgen")
                .join("assets")
                .join("station/logos/main.png")
        );
        for unsafe_value in [
            "../secret.png",
            "station/../../secret.png",
            "/absolute/logo.png",
            "C:/outside/logo.png",
            "station\\logo.png",
            "station//logo.png",
            "station/CON.png",
        ] {
            assert!(
                AssetId::new(unsafe_value).is_err(),
                "`{unsafe_value}` should be rejected"
            );
        }
    }

    #[test]
    fn existing_assets_are_canonicalized_beneath_the_asset_root() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let asset_directory = temporary.path().join("cgen/assets/station");
        fs::create_dir_all(&asset_directory).expect("asset directory");
        let asset_path = asset_directory.join("logo.png");
        fs::write(&asset_path, b"image").expect("asset file");
        let asset = AssetId::new("station/logo.png").expect("asset ID");

        assert_eq!(
            asset
                .resolve_existing_under(temporary.path())
                .expect("resolved asset"),
            fs::canonicalize(asset_path).expect("canonical asset")
        );
    }

    #[test]
    fn scene_xml_round_trip_preserves_every_node_kind_and_binding() {
        let scene = custom_scene(test_group(
            "root",
            vec![
                scene_node(
                    "program",
                    "Program",
                    full_canvas(),
                    NodeKind::ProgramVideo(ProgramVideoNode::default()),
                ),
                scene_node(
                    "solid",
                    "Solid",
                    full_canvas(),
                    NodeKind::Color(ColorNode {
                        source: ColorSource::Binding(ColorBinding::EasWatch),
                    }),
                ),
                scene_node(
                    "gradient",
                    "Gradient",
                    full_canvas(),
                    NodeKind::ColorGradient(ColorGradientNode {
                        source: GradientSource::Static(vec![
                            GradientStop {
                                offset: 0.0,
                                color: RgbaColor::BLACK,
                            },
                            GradientStop {
                                offset: 1.0,
                                color: RgbaColor::WHITE,
                            },
                        ]),
                        angle_degrees: 45.0,
                    }),
                ),
                scene_node(
                    "text",
                    "Text",
                    full_canvas(),
                    NodeKind::Text(TextNode {
                        source: TextSource::Binding(TextBinding::AlertAction),
                        style: TextStyle {
                            outline: Some(TextOutline {
                                width: 2.0,
                                color: RgbaColor::BLACK,
                            }),
                            shadow: Some(TextShadow {
                                x: 3.0,
                                y: 4.0,
                                blur: 1.0,
                                color: default_shadow(),
                            }),
                            ..TextStyle::default()
                        },
                        layout: TextLayout {
                            horizontal: HorizontalAlignment::Justify,
                            ..TextLayout::default()
                        },
                        scroll: TextScroll::default(),
                    }),
                ),
                scene_node(
                    "image",
                    "Image",
                    full_canvas(),
                    NodeKind::Image(ImageNode {
                        asset: AssetId::new("station/logo.png").expect("asset ID"),
                        fit: VideoFit::Contain,
                        tint: RgbaColor::WHITE,
                    }),
                ),
            ],
        ));

        let xml = scene.to_xml().expect("serialize scene");
        let decoded = parse_scene_xml(&xml).expect("parse serialized scene");

        assert_eq!(decoded, scene);
        assert!(xml.contains("Text_Alert_Action"));
    }

    #[test]
    fn validation_rejects_duplicate_node_ids() {
        let scene = custom_scene(test_group(
            "root",
            vec![
                test_group("duplicate", Vec::new()),
                test_group("duplicate", Vec::new()),
            ],
        ));

        let error = scene.validate().expect_err("duplicate IDs must fail");
        assert!(matches!(error, SceneError::DuplicateNodeId { .. }));
    }

    #[test]
    fn validation_rejects_case_folded_duplicate_node_ids() {
        let scene = custom_scene(test_group(
            "root",
            vec![
                test_group("Headline", Vec::new()),
                test_group("headline", Vec::new()),
            ],
        ));

        assert!(matches!(
            scene.validate(),
            Err(SceneError::DuplicateNodeId { .. })
        ));
    }

    #[test]
    fn validation_enforces_maximum_scene_depth() {
        let mut root = test_group("leaf", Vec::new());
        for index in 0..MAX_SCENE_DEPTH {
            root = test_group(&format!("parent_{index}"), vec![root]);
        }
        let scene = custom_scene(root);

        let error = scene.validate().expect_err("overly deep scene must fail");
        assert!(matches!(error, SceneError::MaximumDepthExceeded { .. }));
    }

    #[test]
    fn validation_enforces_maximum_scene_node_count() {
        let children = (0..MAX_SCENE_NODES)
            .map(|index| test_group(&format!("child_{index}"), Vec::new()))
            .collect();
        let scene = custom_scene(test_group("root", children));

        let error = scene.validate().expect_err("oversized scene must fail");
        assert!(matches!(error, SceneError::MaximumNodeCountExceeded { .. }));
    }

    #[test]
    fn protected_scene_rules_allow_only_supported_mutations() {
        let crawl = protected_default_scene(ProtectedSceneKind::StandardCrawl);
        let mut edited_crawl = crawl.clone();
        edited_crawl.root.children.clear();
        assert!(validate_scene_update(&crawl, &edited_crawl).is_ok());
        assert!(validate_scene_delete(&crawl.id).is_err());

        let passthrough = protected_default_scene(ProtectedSceneKind::ProgramPassthrough);
        let mut edited_passthrough = passthrough.clone();
        edited_passthrough.root.name = "Edited".to_string();
        assert!(matches!(
            validate_scene_update(&passthrough, &edited_passthrough),
            Err(SceneError::LockedSceneEdit { .. })
        ));

        let custom = custom_scene(test_group("custom_root", Vec::new()));
        let replacement = protected_default_scene(ProtectedSceneKind::Standby);
        assert!(matches!(
            validate_scene_update(&custom, &replacement),
            Err(SceneError::ProtectedSceneRename { .. })
        ));
    }

    #[test]
    fn alert_selection_rejects_passthrough_and_standby() {
        assert!(validate_alert_scene(&ProtectedSceneKind::StandardCrawl.id()).is_ok());
        assert!(validate_alert_scene(&SceneId::new("custom").expect("custom ID")).is_ok());
        assert!(matches!(
            validate_alert_scene(&ProtectedSceneKind::ProgramPassthrough.id()),
            Err(SceneError::InvalidAlertScene { .. })
        ));
        assert!(matches!(
            validate_alert_scene(&ProtectedSceneKind::Standby.id()),
            Err(SceneError::InvalidAlertScene { .. })
        ));
    }

    #[test]
    fn passthrough_scene_compiles_to_zero_draw_bypass() {
        let scene = protected_default_scene(ProtectedSceneKind::ProgramPassthrough);
        assert!(matches!(scene.compile(), Ok(RenderPlan::Bypass)));
    }

    #[test]
    fn bundled_protected_scene_files_match_embedded_safe_defaults() {
        let scene_directory =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bundle/managed/cgen/scenes");
        for kind in ProtectedSceneKind::ALL {
            let loaded = load_scene_file(&scene_directory.join(kind.filename()))
                .expect("bundled protected scene parses");
            assert_eq!(loaded, protected_default_scene(kind));
        }
    }

    #[test]
    fn missing_scene_directory_uses_embedded_defaults_without_writing_files() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let missing = temporary.path().join("managed/cgen/scenes");

        let catalog = load_scene_directory(&missing);

        assert!(catalog.is_degraded());
        assert_eq!(catalog.scenes().count(), 4);
        assert!(!missing.exists(), "loader must not create managed files");
        for kind in ProtectedSceneKind::ALL {
            assert!(catalog.scene(&kind.id()).is_some());
        }
        assert!(catalog
            .alert_scene(&ProtectedSceneKind::StandardCrawl.id())
            .is_ok());
        assert!(catalog
            .alert_scene(&SceneId::new("not_loaded").expect("scene ID"))
            .is_err());
    }

    #[test]
    fn scene_file_loader_rejects_oversized_xml_before_allocation() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("large.xml");
        let file = fs::File::create(&path).expect("scene file");
        file.set_len(MAX_SCENE_XML_BYTES + 1)
            .expect("oversized sparse file");

        assert!(matches!(
            load_scene_file(&path),
            Err(SceneError::SceneFileTooLarge { .. })
        ));
    }

    #[test]
    fn preflight_rejects_deep_xml_before_recursive_deserialization() {
        let mut xml = String::from("<scene schema_version=\"1\" id=\"deep\" name=\"Deep\">");
        for index in 0..=MAX_SCENE_DEPTH {
            xml.push_str(&format!("<node id=\"node_{index}\" name=\"Node\"><group>"));
        }

        let error = parse_scene_xml(&xml).expect_err("preflight depth limit");
        assert!(error.to_string().contains("maximum node depth"));
    }

    #[test]
    fn inset_defaults_use_negative_extent_offsets() {
        let inset = inset_canvas(80.0, 10);
        assert_eq!(inset.x, 80.0);
        assert_eq!(inset.y, 80.0);
        assert_eq!(inset.width, -160.0);
        assert_eq!(inset.height, -160.0);
        assert_eq!(
            inset.resolve(ResolvedRect {
                x: 0.0,
                y: 0.0,
                width: 1_920.0,
                height: 1_080.0,
            }),
            Some(ResolvedRect {
                x: 80.0,
                y: 80.0,
                width: 1_760.0,
                height: 920.0,
            })
        );
    }

    #[test]
    fn invalid_protected_scene_uses_default_without_overwriting_operator_file() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("standby.xml");
        fs::write(&path, "<scene this-is-not-valid-xml").expect("write invalid scene");

        let catalog = load_scene_directory(temporary.path());

        let standby = catalog
            .scene(&ProtectedSceneKind::Standby.id())
            .expect("embedded standby");
        let title = standby
            .root
            .children
            .iter()
            .find_map(|node| match &node.kind {
                NodeKind::Text(TextNode {
                    source: TextSource::Static(value),
                    ..
                }) => Some(value.as_str()),
                _ => None,
            });
        assert_eq!(title, Some(STANDBY_TITLE));
        assert_eq!(
            fs::read_to_string(path).expect("operator file remains"),
            "<scene this-is-not-valid-xml"
        );
        assert!(catalog.warnings().len() >= 2);
    }
}
