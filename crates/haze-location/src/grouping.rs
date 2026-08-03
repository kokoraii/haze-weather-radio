//! Reversible query-time grouping for nearby facilities with similar names.

use std::collections::{BTreeMap, HashSet};

use serde_json::json;
use strsim::jaro_winkler;
use uuid::Uuid;

use crate::contract::{Candidate, DedupeMode, Grouping, QueryFilters, StationMode};
use crate::geometry::haversine_m;
use crate::normalize::locality_stem;

const GROUP_NAMESPACE: Uuid = Uuid::from_u128(0x55224c634ac65cdb82b36c695bfb8911);
const COMPATIBLE_KINDS: &[&str] = &[
    "weather_station",
    "climate_station",
    "marine_station",
    "marine_buoy",
    "airport",
    "air_quality_station",
];

pub fn group_similar(
    candidates: Vec<Candidate>,
    catalog_generation: &str,
    expand_members: bool,
    station_preference: Option<StationMode>,
    filters: &QueryFilters,
    force_groups: &[Vec<String>],
    never_groups: &[(String, String)],
) -> Vec<Candidate> {
    let never: HashSet<(String, String)> = never_groups
        .iter()
        .flat_map(|(left, right)| [(left.clone(), right.clone()), (right.clone(), left.clone())])
        .collect();
    let mut consumed = HashSet::new();
    let mut output = Vec::new();
    for anchor_index in 0..candidates.len() {
        if consumed.contains(&anchor_index) {
            continue;
        }
        let mut member_indexes = vec![anchor_index];
        for candidate_index in (anchor_index + 1)..candidates.len() {
            if consumed.contains(&candidate_index)
                || never.contains(&(
                    candidates[anchor_index].entity.id.clone(),
                    candidates[candidate_index].entity.id.clone(),
                ))
            {
                continue;
            }
            let forced = same_forced_group(
                &candidates[anchor_index].entity.id,
                &candidates[candidate_index].entity.id,
                force_groups,
            );
            if !forced
                && (!similar_facilities(&candidates[anchor_index], &candidates[candidate_index])
                    || !member_indexes.iter().all(|existing_index| {
                        group_diameter_ok(
                            &candidates[*existing_index],
                            &candidates[candidate_index],
                        )
                    }))
            {
                continue;
            }
            member_indexes.push(candidate_index);
        }
        if member_indexes.len() == 1 {
            output.push(candidates[anchor_index].clone());
            continue;
        }
        for index in &member_indexes {
            consumed.insert(*index);
        }
        let representative_index =
            representative(&candidates, &member_indexes, station_preference, filters);
        let mut representative = candidates[representative_index].clone();
        let mut member_ids: Vec<_> = member_indexes
            .iter()
            .map(|index| candidates[*index].entity.id.clone())
            .collect();
        member_ids.sort();
        let group_id = format!(
            "urn:haze:location-group:{}",
            Uuid::new_v5(
                &GROUP_NAMESPACE,
                format!("{catalog_generation}:{}", member_ids.join("|")).as_bytes(),
            )
        );
        let (minimum_similarity, maximum_distance) = group_evidence(&candidates, &member_indexes);
        let confidence =
            (minimum_similarity * (1.0 - (maximum_distance / 10_000.0).min(0.25))).clamp(0.0, 1.0);
        representative.grouping = Some(Grouping {
            group_id,
            representative_id: representative.entity.id.clone(),
            mode: DedupeMode::SimilarName,
            algorithm: "similar-name-v1".to_string(),
            confidence,
            member_ids,
            member_count: member_indexes.len(),
            members: if expand_members {
                member_indexes
                    .iter()
                    .map(|index| candidates[*index].entity.clone())
                    .collect()
            } else {
                Vec::new()
            },
            evidence: BTreeMap::from([
                (
                    "minimum_name_similarity".to_string(),
                    json!(minimum_similarity),
                ),
                ("maximum_distance_m".to_string(), json!(maximum_distance)),
                (
                    "locality_stem".to_string(),
                    json!(locality_stem(representative.entity.display_name())),
                ),
                ("country".to_string(), json!(representative.entity.country)),
                ("region".to_string(), json!(representative.entity.region)),
            ]),
        });
        output.push(representative);
    }
    output
}

fn similar_facilities(left: &Candidate, right: &Candidate) -> bool {
    if !COMPATIBLE_KINDS.contains(&left.entity.kind.as_str())
        || !COMPATIBLE_KINDS.contains(&right.entity.kind.as_str())
        || !same_optional(&left.entity.country, &right.entity.country)
        || !same_optional(&left.entity.region, &right.entity.region)
    {
        return false;
    }
    let Some(distance) = point_distance(left, right) else {
        return authoritative_identifier_overlap(left, right);
    };
    let left_stem = locality_stem(left.entity.display_name());
    let right_stem = locality_stem(right.entity.display_name());
    let similarity = jaro_winkler(&left_stem, &right_stem);
    (!left_stem.is_empty() && left_stem == right_stem && distance <= 5_000.0)
        || similarity >= 0.94 && distance <= 2_000.0
}

fn group_diameter_ok(left: &Candidate, right: &Candidate) -> bool {
    point_distance(left, right).is_some_and(|distance| distance <= 5_000.0)
        || authoritative_identifier_overlap(left, right)
}

fn point_distance(left: &Candidate, right: &Candidate) -> Option<f64> {
    let left_geometry = left.entity.geometry.as_ref()?;
    let right_geometry = right.entity.geometry.as_ref()?;
    let (left_lat, left_lon) = left_geometry.latitude.zip(left_geometry.longitude)?;
    let (right_lat, right_lon) = right_geometry.latitude.zip(right_geometry.longitude)?;
    Some(haversine_m(left_lat, left_lon, right_lat, right_lon))
}

fn authoritative_identifier_overlap(left: &Candidate, right: &Candidate) -> bool {
    left.entity.identifiers.iter().any(|left_identifier| {
        left_identifier.authority != "legacy"
            && right.entity.identifiers.iter().any(|right_identifier| {
                left_identifier.authority == right_identifier.authority
                    && left_identifier.scheme == right_identifier.scheme
                    && left_identifier.normalized_value == right_identifier.normalized_value
            })
    })
}

fn representative(
    candidates: &[Candidate],
    members: &[usize],
    station_preference: Option<StationMode>,
    filters: &QueryFilters,
) -> usize {
    *members
        .iter()
        .min_by(|left, right| {
            let left_candidate = &candidates[**left];
            let right_candidate = &candidates[**right];
            let left_exact = matches!(
                left_candidate.match_info.method.as_str(),
                "exact_identifier" | "exact_name"
            );
            let right_exact = matches!(
                right_candidate.match_info.method.as_str(),
                "exact_identifier" | "exact_name"
            );
            right_exact
                .cmp(&left_exact)
                .then_with(|| {
                    requested_match(right_candidate, filters)
                        .cmp(&requested_match(left_candidate, filters))
                })
                .then_with(|| {
                    let left_mode = station_preference
                        .is_some_and(|mode| left_candidate.entity.station_mode() == Some(mode));
                    let right_mode = station_preference
                        .is_some_and(|mode| right_candidate.entity.station_mode() == Some(mode));
                    right_mode.cmp(&left_mode)
                })
                .then_with(|| {
                    configured_feed_binding(right_candidate)
                        .cmp(&configured_feed_binding(left_candidate))
                })
                .then_with(|| {
                    right_candidate
                        .match_info
                        .score
                        .partial_cmp(&left_candidate.match_info.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| {
                    left_candidate
                        .distance_m
                        .unwrap_or(f64::INFINITY)
                        .partial_cmp(&right_candidate.distance_m.unwrap_or(f64::INFINITY))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| {
                    right_candidate
                        .entity
                        .source_quality
                        .partial_cmp(&left_candidate.entity.source_quality)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| left_candidate.entity.id.cmp(&right_candidate.entity.id))
        })
        .unwrap_or(&members[0])
}

fn requested_match(candidate: &Candidate, filters: &QueryFilters) -> bool {
    (filters.kinds.is_empty()
        || filters
            .kinds
            .iter()
            .any(|kind| kind.eq_ignore_ascii_case(&candidate.entity.kind)))
        && (filters.capabilities.is_empty()
            || filters.capabilities.iter().all(|wanted| {
                candidate
                    .entity
                    .capabilities
                    .iter()
                    .any(|actual| actual.eq_ignore_ascii_case(wanted))
            }))
}

fn configured_feed_binding(candidate: &Candidate) -> bool {
    candidate
        .entity
        .attributes
        .get("configured_feed_binding")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn group_evidence(candidates: &[Candidate], members: &[usize]) -> (f64, f64) {
    let mut minimum_similarity = 1.0_f64;
    let mut maximum_distance = 0.0_f64;
    for (offset, left_index) in members.iter().enumerate() {
        for right_index in members.iter().skip(offset + 1) {
            minimum_similarity = minimum_similarity.min(jaro_winkler(
                &locality_stem(candidates[*left_index].entity.display_name()),
                &locality_stem(candidates[*right_index].entity.display_name()),
            ));
            if let Some(distance) =
                point_distance(&candidates[*left_index], &candidates[*right_index])
            {
                maximum_distance = maximum_distance.max(distance);
            }
        }
    }
    (minimum_similarity, maximum_distance)
}

fn same_optional(left: &Option<String>, right: &Option<String>) -> bool {
    left.as_deref()
        .zip(right.as_deref())
        .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn same_forced_group(left: &str, right: &str, groups: &[Vec<String>]) -> bool {
    groups.iter().any(|group| {
        group.iter().any(|member| member == left) && group.iter().any(|member| member == right)
    })
}

#[cfg(test)]
mod tests {
    use crate::contract::{ConfidenceLevel, Entity, Geometry, LocationName, MatchInfo};

    use super::*;

    fn station(id: &str, name: &str, latitude: f64, longitude: f64, mode: &str) -> Candidate {
        Candidate {
            entity: Entity {
                id: id.to_string(),
                kind: "weather_station".to_string(),
                capabilities: vec!["observation.surface".to_string()],
                country: Some("CA".to_string()),
                region: Some("SK".to_string()),
                lifecycle_status: "active".to_string(),
                reporting_status: "reporting_recently".to_string(),
                source_quality: 0.9,
                identifiers: Vec::new(),
                names: vec![LocationName {
                    locale: Some("en-CA".to_string()),
                    value: name.to_string(),
                    normalized_value: crate::normalize::normalize_name(name),
                    name_kind: "canonical".to_string(),
                    primary: true,
                    source_id: None,
                }],
                geometry: Some(Geometry {
                    geometry_type: "point".to_string(),
                    latitude: Some(latitude),
                    longitude: Some(longitude),
                    bbox: [longitude, latitude, longitude, latitude],
                    accuracy_m: None,
                    source_id: None,
                }),
                deployments: Vec::new(),
                attributes: BTreeMap::from([("station_mode".to_string(), json!(mode))]),
            },
            match_info: MatchInfo {
                score: 0.9,
                confidence: ConfidenceLevel::High,
                method: "fuzzy_name".to_string(),
                algorithm: "test".to_string(),
                evidence: BTreeMap::new(),
            },
            facet: None,
            distance_m: None,
            relationship_path: Vec::new(),
            grouping: None,
        }
    }

    #[test]
    fn saskatoon_stations_group_but_remain_members() {
        let grouped = group_similar(
            vec![
                station("cpo", "SASKATOON RCS", 52.173753, -106.718888, "auto"),
                station("cyxe", "Saskatoon Airport", 52.171, -106.7, "manual"),
            ],
            "generation",
            true,
            Some(StationMode::Manual),
            &QueryFilters::default(),
            &[],
            &[],
        );
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].entity.id, "cyxe");
        let grouping = grouped[0].grouping.as_ref().expect("grouping");
        assert_eq!(grouping.member_count, 2);
        assert_eq!(grouping.members.len(), 2);
        assert_eq!(grouping.representative_id, "cyxe");
    }
}
