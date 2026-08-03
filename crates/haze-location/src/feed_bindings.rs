//! Synchronizes configured feed location references into the service-owned overlay.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use roxmltree::Document;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};

use crate::catalog::{CatalogSnapshot, WorkerCatalog};
use crate::contract::{QueryFilters, QueryOptions};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Binding {
    feed_id: String,
    purpose: String,
    source: String,
    identifier: String,
}

pub fn sync(snapshot: &CatalogSnapshot, feeds_path: &Path) -> Result<()> {
    let raw = fs::read(feeds_path)
        .with_context(|| format!("failed to read feed bindings from {}", feeds_path.display()))?;
    let text = std::str::from_utf8(&raw)
        .with_context(|| format!("{} is not UTF-8 XML", feeds_path.display()))?;
    let bindings = parse(text)?;
    let config_sha256 = format!("{:x}", Sha256::digest(&raw));
    let mut worker = WorkerCatalog::new();
    worker.prepare(snapshot)?;
    let mut resolved = Vec::with_capacity(bindings.len());
    let filters = QueryFilters::default();
    let options = QueryOptions::default();
    for binding in bindings {
        let mut candidates = Vec::new();
        if !binding.identifier.contains('*') {
            for scheme in schemes_for(&binding) {
                candidates.extend(worker.resolve_identifier(
                    scheme,
                    None,
                    &binding.identifier,
                    &filters,
                    &options,
                )?);
            }
        }
        let mut seen = HashSet::new();
        candidates.retain(|candidate| seen.insert(candidate.entity.id.clone()));
        let (canonical_id, confidence, status) = match candidates.as_slice() {
            [] if binding.identifier.contains('*') => (None, None, "wildcard"),
            [] => (None, None, "unresolved"),
            [candidate] => (
                Some(candidate.entity.id.clone()),
                Some(confidence_name(candidate.match_info.confidence)),
                "resolved",
            ),
            _ => (None, None, "ambiguous"),
        };
        resolved.push((binding, canonical_id, confidence, status));
    }

    let mut connection = Connection::open(&snapshot.overlay_path)?;
    let transaction = connection.transaction()?;
    transaction.execute("DELETE FROM feed_bindings", [])?;
    let updated_at = Utc::now().to_rfc3339();
    for (binding, canonical_id, confidence, status) in resolved {
        transaction.execute(
            "INSERT INTO feed_bindings(
                   feed_id, purpose, raw_source, raw_identifier, canonical_id,
                   confidence, match_status, config_sha256, updated_at
               ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                binding.feed_id,
                binding.purpose,
                binding.source,
                binding.identifier,
                canonical_id,
                confidence,
                status,
                config_sha256,
                updated_at,
            ],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn parse(raw: &str) -> Result<Vec<Binding>> {
    let document =
        Document::parse(raw).context("failed to parse feeds XML for location bindings")?;
    let mut bindings = Vec::new();
    let mut seen = HashSet::new();
    for feed in document
        .descendants()
        .filter(|node| node.has_tag_name("feed"))
    {
        let Some(feed_id) = feed.attribute("id") else {
            continue;
        };
        let Some(locations) = feed
            .children()
            .find(|node| node.is_element() && node.has_tag_name("locations"))
        else {
            continue;
        };
        for node in locations.descendants().filter(|node| {
            node.is_element()
                && (node.has_tag_name("location")
                    || node.has_tag_name("region")
                    || node.has_tag_name("subregion"))
        }) {
            let Some(identifier) = node
                .attribute("id")
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let source = node.attribute("source").unwrap_or("auto").trim();
            let purpose = purpose_for(node, locations);
            let binding = Binding {
                feed_id: feed_id.to_string(),
                purpose: purpose.clone(),
                source: source.to_string(),
                identifier: identifier.to_string(),
            };
            if seen.insert(binding.clone()) {
                bindings.push(binding);
            }
            if let Some(derived) = node
                .attribute("derive_forecast")
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                let binding = Binding {
                    feed_id: feed_id.to_string(),
                    purpose: format!("{purpose}.derive_forecast"),
                    source: source.to_string(),
                    identifier: derived.to_string(),
                };
                if seen.insert(binding.clone()) {
                    bindings.push(binding);
                }
            }
        }
    }
    Ok(bindings)
}

fn purpose_for(node: roxmltree::Node<'_, '_>, locations: roxmltree::Node<'_, '_>) -> String {
    let names: Vec<_> = node
        .ancestors()
        .take_while(|ancestor| *ancestor != locations)
        .filter(|ancestor| ancestor.is_element())
        .map(|ancestor| ancestor.tag_name().name())
        .filter(|name| !matches!(*name, "location" | "region" | "subregion"))
        .collect();
    names.into_iter().rev().collect::<Vec<_>>().join(".")
}

fn schemes_for(binding: &Binding) -> &'static [&'static str] {
    let purpose = binding.purpose.to_ascii_lowercase();
    let source = binding.source.to_ascii_lowercase();
    if purpose.contains("derive_forecast") {
        &["eccc_citypage", "forecast", "clc"]
    } else if purpose.contains("coverage") {
        if source == "nws" {
            &["nws_zone", "nws_ugc_county", "same", "fips"]
        } else {
            &["clc", "same"]
        }
    } else if purpose.contains("aviation") {
        &["icao", "eccc_station", "iata"]
    } else if purpose.contains("airquality") {
        &["naps", "aqhi", "epa_aqs", "airnow"]
    } else if purpose.contains("climate") {
        &["climate", "msc"]
    } else if purpose.contains("hydrometric") {
        &["hydrometric"]
    } else if purpose.contains("marineforecast") {
        if source == "nws" {
            &["nws_marine_zone", "nws_zone"]
        } else {
            &["marine", "eccc_feature", "clc"]
        }
    } else if purpose.contains("marineconditions") {
        &["marine", "msc", "wmo", "eccc_station", "ndbc"]
    } else if source == "nws" {
        &["nws_zone", "nws_ugc_county", "nws_marine_zone"]
    } else {
        &["eccc_citypage", "forecast", "eccc_station"]
    }
}

fn confidence_name(confidence: crate::contract::ConfidenceLevel) -> String {
    format!("{confidence:?}").to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_feed_purpose_and_derived_forecast() {
        let bindings = parse(
            r#"<feeds><feed id="one"><locations><coverage><region id="001" source="eccc" derive_forecast="sk-1"/></coverage><hydrometricLocations><upstream><location id="05HG001" source="eccc"/></upstream></hydrometricLocations><marineForecastLocations><subregion id="m0000109" source="eccc"/></marineForecastLocations></locations></feed></feeds>"#,
        )
        .expect("feed bindings");
        assert_eq!(bindings.len(), 4);
        assert_eq!(bindings[0].purpose, "coverage");
        assert_eq!(bindings[1].purpose, "coverage.derive_forecast");
        assert_eq!(bindings[2].purpose, "hydrometricLocations.upstream");
        assert_eq!(bindings[3].purpose, "marineForecastLocations");
        assert_eq!(schemes_for(&bindings[1])[0], "eccc_citypage");
        assert_eq!(schemes_for(&bindings[3])[0], "marine");
    }

    #[test]
    fn duplicate_configured_bindings_are_idempotent() {
        let bindings = parse(
            r#"<feeds><feed id="one"><locations><coverage><region id="001" source="eccc" derive_forecast="sk-1"/><region id="001" source="eccc" derive_forecast="sk-1"/></coverage></locations></feed></feeds>"#,
        )
        .expect("feed bindings");

        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].purpose, "coverage");
        assert_eq!(bindings[1].purpose, "coverage.derive_forecast");
    }
}
