//! Versioned event contract for the location service.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const API_VERSION: u16 = 1;
pub const ALGORITHM_VERSION: &str = "location-v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Resolve,
    BatchResolve,
    Search,
    PointFacets,
    Nearest,
    Traverse,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LocationInput {
    Identifier {
        scheme: String,
        #[serde(default)]
        authority: Option<String>,
        value: String,
    },
    Name {
        text: String,
    },
    Auto {
        text: String,
    },
    Point {
        latitude: f64,
        longitude: f64,
    },
    Entity {
        id: String,
    },
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct QueryFilters {
    #[serde(default)]
    pub kinds: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub relationship_types: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceLevel {
    Low,
    #[default]
    Medium,
    High,
    Exact,
}

impl ConfidenceLevel {
    #[must_use]
    pub fn from_raw(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "exact" => Self::Exact,
            "high" => Self::High,
            "medium" | "review" => Self::Medium,
            _ => Self::Low,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DedupeMode {
    #[default]
    None,
    SimilarName,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StationMode {
    Auto,
    Manual,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StationModeRequirement {
    #[default]
    Any,
    Auto,
    Manual,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct GeographicBias {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default)]
pub struct QueryOptions {
    pub limit: usize,
    pub max_distance_km: Option<f64>,
    pub minimum_confidence: ConfidenceLevel,
    pub include_inactive: bool,
    pub include_area_geometry: bool,
    pub dedupe_mode: DedupeMode,
    pub expand_members: bool,
    pub station_mode_preference: Option<StationMode>,
    pub station_mode_requirement: StationModeRequirement,
    pub locale: Option<String>,
    pub geographic_bias: Option<GeographicBias>,
    pub as_of: Option<String>,
    pub input_mode: String,
    pub max_depth: usize,
    pub max_visited: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            // Zero is the wire-level sentinel for the operator-configured default.
            // QueryEngine clamps it to 10 when no override is configured.
            limit: 0,
            max_distance_km: None,
            minimum_confidence: ConfidenceLevel::Medium,
            include_inactive: false,
            include_area_geometry: false,
            dedupe_mode: DedupeMode::None,
            expand_members: false,
            station_mode_preference: None,
            station_mode_requirement: StationModeRequirement::Any,
            locale: None,
            geographic_bias: None,
            as_of: None,
            input_mode: "text".to_string(),
            max_depth: 1,
            max_visited: 10_000,
        }
    }
}

impl QueryOptions {
    pub fn clamp(&mut self, default_limit: usize, maximum_limit: usize) {
        if self.limit == 0 {
            self.limit = default_limit;
        }
        self.limit = self.limit.clamp(1, maximum_limit.max(1));
        self.max_depth = self.max_depth.clamp(1, 5);
        self.max_visited = self.max_visited.clamp(1, 10_000);
        if self
            .max_distance_km
            .is_some_and(|distance| !distance.is_finite() || distance < 0.0)
        {
            self.max_distance_km = None;
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct QueryRequest {
    #[serde(default = "default_api_version")]
    pub api_version: u16,
    #[serde(default)]
    pub request_id: String,
    pub operation: Operation,
    #[serde(default)]
    pub input: Option<LocationInput>,
    #[serde(default)]
    pub inputs: Vec<LocationInput>,
    #[serde(default)]
    pub filters: QueryFilters,
    #[serde(default)]
    pub options: QueryOptions,
}

const fn default_api_version() -> u16 {
    API_VERSION
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Identifier {
    pub authority: String,
    pub scheme: String,
    pub value: String,
    pub normalized_value: String,
    pub primary: bool,
    pub confidence: ConfidenceLevel,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LocationName {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,
    pub value: String,
    pub normalized_value: String,
    pub name_kind: String,
    pub primary: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Geometry {
    pub geometry_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub longitude: Option<f64>,
    pub bbox: [f64; 4],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accuracy_m: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Deployment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_deployment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub longitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elevation_m: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<String>,
    pub reporting_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Entity {
    pub id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    pub lifecycle_status: String,
    pub reporting_status: String,
    pub source_quality: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub identifiers: Vec<Identifier>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<LocationName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub geometry: Option<Geometry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deployments: Vec<Deployment>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, Value>,
}

impl Entity {
    #[must_use]
    pub fn display_name(&self) -> &str {
        self.names
            .iter()
            .find(|name| name.primary)
            .or_else(|| self.names.first())
            .map(|name| name.value.as_str())
            .unwrap_or(self.id.as_str())
    }

    #[must_use]
    pub fn station_mode(&self) -> Option<StationMode> {
        self.attributes
            .get("station_mode")
            .and_then(Value::as_str)
            .and_then(|value| match value {
                "auto" => Some(StationMode::Auto),
                "manual" => Some(StationMode::Manual),
                _ => None,
            })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MatchInfo {
    pub score: f64,
    pub confidence: ConfidenceLevel,
    pub method: String,
    pub algorithm: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub evidence: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RelationshipStep {
    pub from_id: String,
    pub to_id: String,
    pub relationship_type: String,
    pub confidence: ConfidenceLevel,
    pub score: f64,
    pub method: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Grouping {
    pub group_id: String,
    pub representative_id: String,
    pub mode: DedupeMode,
    pub algorithm: String,
    pub confidence: f64,
    pub member_ids: Vec<String>,
    pub member_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<Entity>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub evidence: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Candidate {
    pub entity: Entity,
    #[serde(rename = "match")]
    pub match_info: MatchInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_m: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relationship_path: Vec<RelationshipStep>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouping: Option<Grouping>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BatchResult {
    pub input_index: usize,
    pub status: String,
    #[serde(default)]
    pub results: Vec<Candidate>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct QueryResponse {
    pub api_version: u16,
    pub request_id: String,
    pub operation: Operation,
    pub status: String,
    pub ambiguous: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_margin: Option<f64>,
    pub catalog_generation: String,
    #[serde(default)]
    pub catalog_packs: Vec<String>,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<Candidate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub batches: Vec<BatchResult>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct QueryFailure {
    pub api_version: u16,
    pub request_id: String,
    pub code: String,
    pub error: String,
    pub retryable: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OverlayUpsert {
    #[serde(default = "default_api_version")]
    pub api_version: u16,
    #[serde(default)]
    pub request_id: String,
    pub source_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    pub entity: Entity,
}

#[cfg(test)]
mod tests {
    use super::QueryOptions;

    #[test]
    fn omitted_limit_uses_the_operator_default_and_contract_cap() {
        let mut options: QueryOptions = serde_json::from_str("{}").expect("options");
        assert_eq!(options.limit, 0);
        options.clamp(12, 100);
        assert_eq!(options.limit, 12);

        options.limit = 500;
        options.clamp(12, 100);
        assert_eq!(options.limit, 100);
    }
}
