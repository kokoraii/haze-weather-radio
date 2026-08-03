//! Bounded synchronous worker pool used by the asynchronous bridge service.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::thread;

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use tokio::sync::oneshot;

use crate::catalog::{
    apply_station_preference, CatalogError, CatalogManager, CatalogSnapshot, WorkerCatalog,
};
use crate::config::ServiceConfig;
use crate::contract::{
    BatchResult, Candidate, DedupeMode, LocationInput, Operation, QueryRequest, QueryResponse,
    API_VERSION,
};
use crate::grouping::group_similar;

#[derive(Clone, Debug)]
struct EngineSettings {
    default_limit: usize,
    maximum_limit: usize,
    force_groups: Vec<Vec<String>>,
    never_groups: Vec<(String, String)>,
}

struct Work {
    snapshot: Arc<CatalogSnapshot>,
    settings: EngineSettings,
    request: QueryRequest,
    reply: oneshot::Sender<Result<QueryResponse, CatalogError>>,
}

#[derive(Clone)]
pub struct QueryEngine {
    sender: Sender<Work>,
    manager: CatalogManager,
    settings: Arc<RwLock<EngineSettings>>,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("location query queue is full")]
    Busy,
    #[error("location query worker pool is closed")]
    Closed,
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}

impl QueryEngine {
    pub fn start(manager: CatalogManager, config: &ServiceConfig) -> Self {
        let (sender, receiver) = bounded::<Work>(config.queue_size);
        for index in 0..config.workers {
            spawn_worker(index, receiver.clone());
        }
        Self {
            sender,
            manager,
            settings: Arc::new(RwLock::new(settings_from_config(config))),
        }
    }

    pub fn update_config(&self, config: &ServiceConfig) {
        let replacement = settings_from_config(config);
        match self.settings.write() {
            Ok(mut settings) => *settings = replacement,
            Err(mut poisoned) => **poisoned.get_mut() = replacement,
        }
    }

    pub async fn query(&self, mut request: QueryRequest) -> Result<QueryResponse, EngineError> {
        let settings = self.settings.read().map_or_else(
            |poisoned| poisoned.into_inner().clone(),
            |settings| settings.clone(),
        );
        request
            .options
            .clamp(settings.default_limit, settings.maximum_limit);
        let snapshot = self.manager.snapshot();
        let (reply, receive) = oneshot::channel();
        match self.sender.try_send(Work {
            snapshot,
            settings,
            request,
            reply,
        }) {
            Ok(()) => receive
                .await
                .map_err(|_| EngineError::Closed)?
                .map_err(EngineError::Catalog),
            Err(TrySendError::Full(_)) => Err(EngineError::Busy),
            Err(TrySendError::Disconnected(_)) => Err(EngineError::Closed),
        }
    }
}

fn settings_from_config(config: &ServiceConfig) -> EngineSettings {
    EngineSettings {
        default_limit: config.default_limit,
        maximum_limit: config.maximum_limit,
        force_groups: config.force_groups.clone(),
        never_groups: config.never_groups.clone(),
    }
}

fn spawn_worker(index: usize, receiver: Receiver<Work>) {
    let builder = thread::Builder::new().name(format!("haze-location-sqlite-{index}"));
    if let Err(err) = builder.spawn(move || {
        let mut catalog = WorkerCatalog::new();
        while let Ok(work) = receiver.recv() {
            let result = catalog
                .prepare(&work.snapshot)
                .and_then(|()| execute(&catalog, &work.snapshot, &work.settings, work.request));
            let _ = work.reply.send(result);
        }
    }) {
        tracing::error!(
            worker = index,
            "failed to start location SQLite worker: {err}"
        );
    }
}

fn execute(
    catalog: &WorkerCatalog,
    snapshot: &CatalogSnapshot,
    settings: &EngineSettings,
    request: QueryRequest,
) -> Result<QueryResponse, CatalogError> {
    if request.api_version != API_VERSION {
        return Err(CatalogError::Invalid(format!(
            "unsupported location API version {}",
            request.api_version
        )));
    }
    if request.request_id.trim().is_empty() {
        return Err(CatalogError::Invalid("request_id is required".to_string()));
    }
    if let Some(as_of) = request.options.as_of.as_deref() {
        let valid = chrono::DateTime::parse_from_rfc3339(as_of).is_ok()
            || chrono::NaiveDate::parse_from_str(as_of, "%Y-%m-%d").is_ok();
        if !valid {
            return Err(CatalogError::Invalid(
                "options.as_of must be an ISO 8601 date or timestamp".to_string(),
            ));
        }
    }
    if request
        .options
        .geographic_bias
        .is_some_and(|bias| !crate::geometry::valid_point(bias.latitude, bias.longitude))
    {
        return Err(CatalogError::Invalid(
            "options.geographic_bias is outside WGS84 bounds".to_string(),
        ));
    }
    if request.operation == Operation::BatchResolve {
        return execute_batch(catalog, snapshot, request);
    }
    let mut candidates = execute_single(catalog, &request)?;
    apply_station_preference(&mut candidates, request.options.station_mode_preference);
    if request.options.dedupe_mode == DedupeMode::SimilarName
        && matches!(
            request.operation,
            Operation::Search | Operation::Nearest | Operation::PointFacets
        )
    {
        candidates = group_similar(
            candidates,
            &snapshot.generation,
            request.options.expand_members,
            request.options.station_mode_preference,
            &request.filters,
            &settings.force_groups,
            &settings.never_groups,
        );
    }
    let per_facet = request.operation == Operation::PointFacets;
    let graph_cap_reached = request.operation == Operation::Traverse
        && (candidates.len() >= request.options.max_visited.saturating_sub(1)
            || candidates
                .iter()
                .any(|candidate| candidate.relationship_path.len() >= request.options.max_depth));
    let truncated = graph_cap_reached || (!per_facet && candidates.len() > request.options.limit);
    if !per_facet {
        candidates.truncate(request.options.limit);
    }
    Ok(response_for(
        snapshot,
        request,
        candidates,
        Vec::new(),
        truncated,
    ))
}

fn execute_batch(
    catalog: &WorkerCatalog,
    snapshot: &CatalogSnapshot,
    request: QueryRequest,
) -> Result<QueryResponse, CatalogError> {
    if request.inputs.is_empty() || request.inputs.len() > 100 {
        return Err(CatalogError::Invalid(
            "batch_resolve requires between 1 and 100 inputs".to_string(),
        ));
    }
    let mut batches = Vec::with_capacity(request.inputs.len());
    let mut truncated = false;
    for (index, input) in request.inputs.iter().cloned().enumerate() {
        let mut child = request.clone();
        child.operation = Operation::Resolve;
        child.input = Some(input);
        child.inputs.clear();
        let mut results = execute_single(catalog, &child)?;
        apply_station_preference(&mut results, child.options.station_mode_preference);
        truncated |= results.len() > child.options.limit;
        results.truncate(child.options.limit);
        batches.push(BatchResult {
            input_index: index,
            status: resolution_status(&results).to_string(),
            results,
        });
    }
    Ok(response_for(
        snapshot,
        request,
        Vec::new(),
        batches,
        truncated,
    ))
}

fn execute_single(
    catalog: &WorkerCatalog,
    request: &QueryRequest,
) -> Result<Vec<Candidate>, CatalogError> {
    let input = request
        .input
        .as_ref()
        .ok_or_else(|| CatalogError::Invalid("input is required".to_string()))?;
    match request.operation {
        Operation::Resolve => resolve_input(catalog, input, request),
        Operation::Search => match input {
            LocationInput::Name { text } | LocationInput::Auto { text } => {
                if matches!(input, LocationInput::Auto { .. }) {
                    auto_candidates(catalog, text, request)
                } else {
                    catalog.search(text, &request.filters, &request.options)
                }
            }
            _ => Err(CatalogError::Invalid(
                "search requires a name or auto input".to_string(),
            )),
        },
        Operation::PointFacets => match input {
            LocationInput::Point {
                latitude,
                longitude,
            } => catalog.point_facets(*latitude, *longitude, &request.filters, &request.options),
            _ => Err(CatalogError::Invalid(
                "point_facets requires a point input".to_string(),
            )),
        },
        Operation::Nearest => match input {
            LocationInput::Point {
                latitude,
                longitude,
            } => catalog.nearest(*latitude, *longitude, &request.filters, &request.options),
            _ => Err(CatalogError::Invalid(
                "nearest requires a point input".to_string(),
            )),
        },
        Operation::Traverse => {
            let start = match input {
                LocationInput::Entity { id } => id.clone(),
                _ => resolve_input(catalog, input, request)?
                    .first()
                    .map(|candidate| candidate.entity.id.clone())
                    .ok_or_else(|| {
                        CatalogError::Invalid("traverse start did not resolve".to_string())
                    })?,
            };
            catalog.traverse(&start, &request.filters, &request.options)
        }
        Operation::BatchResolve => unreachable!("batch handled by execute_batch"),
    }
}

fn resolve_input(
    catalog: &WorkerCatalog,
    input: &LocationInput,
    request: &QueryRequest,
) -> Result<Vec<Candidate>, CatalogError> {
    match input {
        LocationInput::Identifier {
            scheme,
            authority,
            value,
        } => catalog.resolve_identifier(
            scheme,
            authority.as_deref(),
            value,
            &request.filters,
            &request.options,
        ),
        LocationInput::Entity { id } => {
            catalog.entity_by_id(id, &request.filters, &request.options)
        }
        LocationInput::Name { text } => catalog.search(text, &request.filters, &request.options),
        LocationInput::Auto { text } => auto_candidates(catalog, text, request),
        LocationInput::Point {
            latitude,
            longitude,
        } => catalog.point_facets(*latitude, *longitude, &request.filters, &request.options),
    }
}

fn auto_candidates(
    catalog: &WorkerCatalog,
    text: &str,
    request: &QueryRequest,
) -> Result<Vec<Candidate>, CatalogError> {
    let mut candidates = catalog.search(text, &request.filters, &request.options)?;
    let compact: String = text
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect();
    let mut schemes = vec![
        "icao",
        "iata",
        "faa",
        "gnis",
        "gns",
        "eccc_station",
        "ndbc",
        "postal",
        "zip",
        "zcta",
        "nws_ugc",
        "nws_ugc_county",
        "nws_zone",
        "nws_marine_zone",
        "airnow",
        "aqhi",
        "climate",
        "hydrometric",
        "marine",
    ];
    if compact.chars().all(|character| character.is_ascii_digit()) {
        schemes.extend([
            "wmo",
            "msc",
            "same",
            "fips",
            "clc",
            "sgc",
            "hello_weather",
            "eccc_citypage",
            "nws_zone",
            "nws_marine_zone",
            "epa_aqs",
            "naps",
            "aqhi",
            "climate",
            "hydrometric",
            "nws_ugc",
            "nws_ugc_county",
        ]);
    }
    let mut seen_schemes = HashSet::new();
    for scheme in schemes {
        if seen_schemes.insert(scheme) {
            candidates.extend(catalog.resolve_identifier(
                scheme,
                None,
                text,
                &request.filters,
                &request.options,
            )?);
        }
    }
    candidates.sort_by(|left, right| {
        right
            .match_info
            .confidence
            .cmp(&left.match_info.confidence)
            .then_with(|| {
                right
                    .match_info
                    .score
                    .partial_cmp(&left.match_info.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.entity.id.cmp(&right.entity.id))
    });
    let mut seen = HashSet::new();
    candidates.retain(|candidate| seen.insert(candidate.entity.id.clone()));
    Ok(candidates)
}

fn response_for(
    snapshot: &CatalogSnapshot,
    request: QueryRequest,
    results: Vec<Candidate>,
    batches: Vec<BatchResult>,
    truncated: bool,
) -> QueryResponse {
    let (ambiguous, score_margin) = ambiguity(&results);
    QueryResponse {
        api_version: API_VERSION,
        request_id: request.request_id,
        operation: request.operation,
        status: if batches.is_empty() {
            resolution_status(&results).to_string()
        } else {
            "completed".to_string()
        },
        ambiguous,
        score_margin,
        catalog_generation: snapshot.generation.clone(),
        catalog_packs: snapshot.pack_ids.clone(),
        truncated,
        results,
        batches,
    }
}

fn ambiguity(results: &[Candidate]) -> (bool, Option<f64>) {
    if results.len() < 2 {
        return (false, None);
    }
    let margin = (results[0].match_info.score - results[1].match_info.score).abs();
    let both_exact = results[0].match_info.confidence == crate::contract::ConfidenceLevel::Exact
        && results[1].match_info.confidence == crate::contract::ConfidenceLevel::Exact;
    (both_exact || margin < 0.08, Some(margin))
}

fn resolution_status(results: &[Candidate]) -> &'static str {
    match results {
        [] => "not_found",
        [only] if only.match_info.confidence >= crate::contract::ConfidenceLevel::Medium => {
            "resolved"
        }
        values if ambiguity(values).0 => "ambiguous",
        _ => "resolved",
    }
}

#[cfg(test)]
mod tests {
    use crate::contract::{ConfidenceLevel, MatchInfo};

    use super::*;

    #[test]
    fn equal_exact_candidates_are_ambiguous() {
        let candidate = |id: &str| Candidate {
            entity: crate::contract::Entity {
                id: id.to_string(),
                kind: "place".to_string(),
                capabilities: Vec::new(),
                country: None,
                region: None,
                lifecycle_status: "active".to_string(),
                reporting_status: "not_applicable".to_string(),
                source_quality: 1.0,
                identifiers: Vec::new(),
                names: Vec::new(),
                geometry: None,
                deployments: Vec::new(),
                attributes: Default::default(),
            },
            match_info: MatchInfo {
                score: 1.0,
                confidence: ConfidenceLevel::Exact,
                method: "exact_identifier".to_string(),
                algorithm: "test".to_string(),
                evidence: Default::default(),
            },
            facet: None,
            distance_m: None,
            relationship_path: Vec::new(),
            grouping: None,
        };
        assert!(ambiguity(&[candidate("a"), candidate("b")]).0);
    }
}
