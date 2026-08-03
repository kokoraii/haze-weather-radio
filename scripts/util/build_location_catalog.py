#!/usr/bin/env python3
"""Build immutable Haze location packs outside the runtime daemon.

The builder accepts a JSON manifest, downloads only when explicitly allowed, and
atomically publishes a validated SQLite file plus a SHA-256 sidecar. Its input
adapters intentionally preserve provider identifiers and record provenance.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import struct
import tempfile
import unicodedata
import urllib.parse
import urllib.request
import uuid
import zipfile
from pathlib import Path
from typing import Any, Iterable, Iterator

LOCATION_NAMESPACE = uuid.UUID("f987bc4e-56fe-5f41-8bcf-fdcf8c5a8e3e")
USER_AGENT = "haze-location-catalog-builder/1.0"
SCHEMA = Path(__file__).resolve().parents[2] / "crates" / "haze-location" / "schema" / "v1.sql"


def normalize_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).casefold()
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def normalize_identifier(scheme: str, value: Any) -> str:
    raw = unicodedata.normalize("NFKC", str(value or "")).strip().upper()
    if scheme in {"postal", "zip", "zcta"}:
        return re.sub(r"[^A-Z0-9]", "", raw)
    return re.sub(r"\s+", "", raw)


def t9_digits(value: Any) -> str:
    mapping = {
        **{character: "2" for character in "abc"},
        **{character: "3" for character in "def"},
        **{character: "4" for character in "ghi"},
        **{character: "5" for character in "jkl"},
        **{character: "6" for character in "mno"},
        **{character: "7" for character in "pqrs"},
        **{character: "8" for character in "tuv"},
        **{character: "9" for character in "wxyz"},
    }
    return "".join(
        mapping.get(character, character if character.isdigit() else "")
        for character in normalize_name(value)
    )


def station_mode(value: Any) -> str | None:
    normalized = normalize_name(value)
    if normalized.startswith("auto"):
        return "auto"
    if normalized in {"man", "manual", "manned"} or normalized.startswith("man "):
        return "manual"
    return None


def canada_region(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    codes = {
        "alberta": "AB",
        "british columbia": "BC",
        "manitoba": "MB",
        "new brunswick": "NB",
        "newfoundland and labrador": "NL",
        "northwest territories": "NT",
        "nova scotia": "NS",
        "nunavut": "NU",
        "ontario": "ON",
        "prince edward island": "PE",
        "quebec": "QC",
        "saskatchewan": "SK",
        "yukon": "YT",
    }
    return codes.get(normalize_name(raw), raw.upper())


def stable_id(source_id: str, kind: str, provider_id: Any) -> str:
    normalized_provider_id = normalize_identifier("provider", provider_id)
    key = f"{source_id.strip().casefold()}:{kind.strip().casefold()}:{normalized_provider_id}"
    return f"urn:haze:location:{uuid.uuid5(LOCATION_NAMESPACE, key)}"


def source_bytes(location: str, allow_downloads: bool) -> bytes:
    parsed = urllib.parse.urlparse(location)
    if parsed.scheme in {"http", "https"}:
        if not allow_downloads:
            raise RuntimeError(f"network input requires --allow-downloads: {location}")
        request = urllib.request.Request(location, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read()
    return Path(location).read_bytes()


def load_json(location: str, allow_downloads: bool) -> Any:
    return json.loads(source_bytes(location, allow_downloads))


def iter_ogc_features(location: str, allow_downloads: bool) -> Iterator[dict[str, Any]]:
    next_location: str | None = location
    while next_location:
        document = load_json(next_location, allow_downloads)
        yield from document.get("features", [])
        next_location = None
        for link in document.get("links", []):
            if link.get("rel") == "next" and link.get("href"):
                next_location = str(link["href"])
                break


def walk_coordinates(value: Any) -> Iterator[tuple[float, float]]:
    if (
        isinstance(value, (list, tuple))
        and len(value) >= 2
        and isinstance(value[0], (int, float))
        and isinstance(value[1], (int, float))
    ):
        yield float(value[0]), float(value[1])
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            yield from walk_coordinates(child)


def sanitize_geometry(geometry: dict[str, Any]) -> dict[str, Any]:
    def sanitize_coordinates(value: Any) -> Any:
        if (
            isinstance(value, (list, tuple))
            and len(value) >= 2
            and isinstance(value[0], (int, float))
            and isinstance(value[1], (int, float))
        ):
            longitude = float(value[0])
            latitude = float(value[1])
            if 180 < longitude <= 180.000001:
                longitude = 180.0
            elif -180.000001 <= longitude < -180:
                longitude = -180.0
            if 90 < latitude <= 90.000001:
                latitude = 90.0
            elif -90.000001 <= latitude < -90:
                latitude = -90.0
            return [longitude, latitude, *value[2:]]
        if isinstance(value, (list, tuple)):
            return [sanitize_coordinates(child) for child in value]
        return value

    return {
        **geometry,
        "coordinates": sanitize_coordinates(geometry.get("coordinates")),
    }


def transform_geometry(geometry: dict[str, Any], source_crs: str | None) -> dict[str, Any]:
    if not source_crs:
        return geometry
    try:
        from pyproj import Transformer  # type: ignore
    except ImportError as error:
        raise RuntimeError(
            "projected shapefiles require pyproj from requirements-location-catalog.txt"
        ) from error
    transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)

    def transform_coordinates(value: Any) -> Any:
        if (
            isinstance(value, (list, tuple))
            and len(value) >= 2
            and isinstance(value[0], (int, float))
            and isinstance(value[1], (int, float))
        ):
            longitude, latitude = transformer.transform(float(value[0]), float(value[1]))
            return [longitude, latitude, *value[2:]]
        if isinstance(value, (list, tuple)):
            return [transform_coordinates(child) for child in value]
        return value

    return {**geometry, "coordinates": transform_coordinates(geometry.get("coordinates"))}


def geometry_bbox(geometry: dict[str, Any]) -> tuple[float, float, float, float]:
    coordinates = list(walk_coordinates(geometry.get("coordinates")))
    if not coordinates:
        raise ValueError("geometry has no coordinates")
    for longitude, latitude in coordinates:
        if not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
            raise ValueError(f"coordinate outside WGS84: {longitude},{latitude}")
    longitudes = [point[0] for point in coordinates]
    latitudes = [point[1] for point in coordinates]
    return min(longitudes), max(longitudes), min(latitudes), max(latitudes)


def ring_wkb(ring: Iterable[Iterable[float]]) -> bytes:
    points = [(float(point[0]), float(point[1])) for point in ring]
    if points and points[0] != points[-1]:
        points.append(points[0])
    return struct.pack("<I", len(points)) + b"".join(struct.pack("<dd", *point) for point in points)


def geometry_wkb(geometry: dict[str, Any]) -> bytes:
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if geometry_type == "Point":
        return b"\x01" + struct.pack("<I", 1) + struct.pack("<dd", float(coordinates[0]), float(coordinates[1]))
    if geometry_type == "Polygon":
        return b"\x01" + struct.pack("<I", 3) + struct.pack("<I", len(coordinates)) + b"".join(
            ring_wkb(ring) for ring in coordinates
        )
    if geometry_type == "MultiPolygon":
        polygons = [geometry_wkb({"type": "Polygon", "coordinates": polygon}) for polygon in coordinates]
        return b"\x01" + struct.pack("<I", 6) + struct.pack("<I", len(polygons)) + b"".join(polygons)
    raise ValueError(f"unsupported geometry type {geometry_type!r}")


def first(properties: dict[str, Any], *keys: str) -> Any:
    lowered = {str(key).lower(): value for key, value in properties.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value is not None and str(value).strip():
            return value
    return None


def identifier_value(properties: dict[str, Any], specification: dict[str, Any]) -> Any:
    if specification.get("parts"):
        pieces: list[str] = []
        for part in specification["parts"]:
            if "literal" in part:
                pieces.append(str(part["literal"]))
                continue
            value = first(properties, part["field"])
            if value is None:
                return None
            piece = str(value).strip()
            if part.get("last"):
                piece = piece[-int(part["last"]) :]
            pieces.append(piece)
        value: Any = "".join(pieces)
    else:
        value = first(properties, specification["field"])
    if value is None:
        return None
    return f"{specification.get('prefix', '')}{value}{specification.get('suffix', '')}"


class Catalog:
    def __init__(self, path: Path, pack_id: str, retrieved_at: str) -> None:
        self.connection = sqlite3.connect(path)
        self.connection.executescript(SCHEMA.read_text(encoding="utf-8"))
        self.pack_id = pack_id
        self.retrieved_at = retrieved_at
        self.source_counts: dict[str, int] = {}
        self.connection.executemany(
            "INSERT OR REPLACE INTO catalog_metadata(key, value) VALUES(?, ?)",
            [("pack_id", pack_id), ("schema_version", "1"), ("retrieved_at", retrieved_at)],
        )

    def add_source(self, source: dict[str, Any], digest: str) -> None:
        source_id = source["id"]
        self.connection.execute(
            """INSERT OR REPLACE INTO sources(
                   source_id, title, source_version, retrieved_at, valid_from, valid_to,
                   licence, attribution, source_sha256, attributes_json
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                source_id,
                source.get("title", source_id),
                source.get("version"),
                self.retrieved_at,
                source.get("valid_from"),
                source.get("valid_to"),
                source.get("licence"),
                source.get("attribution"),
                digest,
                json.dumps(source.get("attributes", {}), sort_keys=True, separators=(",", ":")),
            ),
        )
        self.source_counts[source_id] = 0

    def add_entity(
        self,
        *,
        source_id: str,
        identity_authority: str | None = None,
        provider_id: Any,
        kind: str,
        names: Iterable[tuple[str | None, Any, str, bool]],
        identifiers: Iterable[tuple[str, str, Any, bool]],
        geometry: dict[str, Any] | None,
        capabilities: Iterable[str] = (),
        country: str | None = None,
        region: str | None = None,
        lifecycle: str = "unknown",
        reporting: str = "unknown",
        source_quality: float = 0.8,
        valid_from: str | None = None,
        valid_to: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> str:
        canonical_id = stable_id(identity_authority or source_id, kind, provider_id)
        self.connection.execute(
            """INSERT INTO entities(
                   canonical_id, kind, country, region, lifecycle_status, reporting_status,
                   source_quality, valid_from, valid_to, attributes_json
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(canonical_id) DO UPDATE SET
                   source_quality = MAX(entities.source_quality, excluded.source_quality),
                   valid_from = COALESCE(entities.valid_from, excluded.valid_from),
                   valid_to = COALESCE(excluded.valid_to, entities.valid_to),
                   lifecycle_status = CASE
                       WHEN entities.lifecycle_status = 'active' OR excluded.lifecycle_status = 'active'
                           THEN 'active'
                       ELSE excluded.lifecycle_status
                   END""",
            (
                canonical_id,
                kind,
                country,
                region,
                lifecycle,
                reporting,
                source_quality,
                valid_from,
                valid_to,
                json.dumps(attributes or {}, sort_keys=True, separators=(",", ":")),
            ),
        )
        entity_pk = int(
            self.connection.execute(
                "SELECT entity_pk FROM entities WHERE canonical_id = ?", (canonical_id,)
            ).fetchone()[0]
        )
        for capability in sorted(set(filter(None, capabilities))):
            self.connection.execute(
                "INSERT OR IGNORE INTO entity_capabilities(entity_pk, capability) VALUES(?, ?)",
                (entity_pk, capability),
            )
        seen_identifiers: set[tuple[str, str, str]] = set()
        for authority, scheme, value, primary in identifiers:
            normalized = normalize_identifier(scheme, value)
            key = (authority, scheme, normalized)
            if not normalized or key in seen_identifiers:
                continue
            seen_identifiers.add(key)
            self.connection.execute(
                """INSERT INTO identifiers(
                       entity_pk, authority, scheme, value, normalized_value, is_primary,
                       confidence, source_id
                   ) VALUES(?, ?, ?, ?, ?, ?, 'exact', ?)
                   ON CONFLICT(entity_pk, authority, scheme, normalized_value) DO NOTHING""",
                (entity_pk, authority, scheme, str(value), normalized, int(primary), source_id),
            )
        seen_names: set[tuple[str | None, str, str]] = set()
        for locale, value, name_kind, primary in names:
            name = str(value or "").strip()
            normalized = normalize_name(name)
            key = (locale, name, name_kind)
            if not normalized or key in seen_names:
                continue
            seen_names.add(key)
            inserted = self.connection.execute(
                """INSERT INTO names(
                       entity_pk, locale, name, normalized_name, t9_digits,
                       name_kind, is_primary, source_id
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(entity_pk, locale, name, name_kind) DO NOTHING""",
                (entity_pk, locale, name, normalized, t9_digits(name), name_kind, int(primary), source_id),
            )
            if inserted.rowcount:
                self.connection.execute(
                    "INSERT INTO names_fts(name, normalized_name, entity_pk, locale) VALUES(?, ?, ?, ?)",
                    (name, normalized, entity_pk, locale),
                )
                self.connection.execute(
                    "INSERT INTO names_trigram(normalized_name, entity_pk) VALUES(?, ?)",
                    (normalized, entity_pk),
                )
        if geometry:
            geometry = sanitize_geometry(geometry)
            minimum_lon, maximum_lon, minimum_lat, maximum_lat = geometry_bbox(geometry)
            point = geometry.get("coordinates") if geometry.get("type") == "Point" else None
            encoded_geometry = geometry_wkb(geometry)
            existing_geometry = self.connection.execute(
                """SELECT geometry_pk FROM geometries
                   WHERE entity_pk = ? AND geometry_type = ? AND geometry_wkb = ?
                     AND COALESCE(valid_from, '') = COALESCE(?, '')
                     AND COALESCE(valid_to, '') = COALESCE(?, '')
                     AND source_id = ?
                   LIMIT 1""",
                (
                    entity_pk,
                    geometry["type"].lower(),
                    encoded_geometry,
                    valid_from,
                    valid_to,
                    source_id,
                ),
            ).fetchone()
            if existing_geometry is not None:
                self.source_counts[source_id] += 1
                return canonical_id
            cursor = self.connection.execute(
                """INSERT INTO geometries(
                       entity_pk, geometry_type, geometry_wkb, latitude, longitude,
                       min_lon, max_lon, min_lat, max_lat, valid_from, valid_to, source_id
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    entity_pk,
                    geometry["type"].lower(),
                    encoded_geometry,
                    float(point[1]) if point else None,
                    float(point[0]) if point else None,
                    minimum_lon,
                    maximum_lon,
                    minimum_lat,
                    maximum_lat,
                    valid_from,
                    valid_to,
                    source_id,
                ),
            )
            geometry_pk = int(cursor.lastrowid)
            self.connection.execute(
                "INSERT INTO entity_rtree VALUES(?, ?, ?, ?, ?)",
                (geometry_pk, minimum_lon, maximum_lon, minimum_lat, maximum_lat),
            )
        self.source_counts[source_id] += 1
        return canonical_id

    def finish(self, expected_counts: dict[str, int]) -> None:
        for source_id, minimum in expected_counts.items():
            actual = self.source_counts.get(source_id, 0)
            if actual < minimum:
                raise RuntimeError(f"source {source_id} has {actual} records, expected at least {minimum}")
        counts = {
            "entities": self.connection.execute("SELECT COUNT(*) FROM entities").fetchone()[0],
            "identifiers": self.connection.execute("SELECT COUNT(*) FROM identifiers").fetchone()[0],
            "names": self.connection.execute("SELECT COUNT(*) FROM names").fetchone()[0],
            "geometries": self.connection.execute("SELECT COUNT(*) FROM geometries").fetchone()[0],
        }
        for key, value in sorted(counts.items()):
            self.connection.execute(
                "INSERT OR REPLACE INTO catalog_metadata(key, value) VALUES(?, ?)",
                (f"count.{key}", str(value)),
            )
        for source_id, value in sorted(self.source_counts.items()):
            self.connection.execute(
                "INSERT OR REPLACE INTO catalog_metadata(key, value) VALUES(?, ?)",
                (f"count.source.{source_id}", str(value)),
            )
        self.connection.commit()
        integrity = self.connection.execute("PRAGMA integrity_check").fetchone()[0]
        rtree = self.connection.execute("SELECT rtreecheck('entity_rtree')").fetchone()[0]
        name_count = counts["names"]
        fts_count = self.connection.execute("SELECT COUNT(*) FROM names_fts").fetchone()[0]
        trigram_count = self.connection.execute("SELECT COUNT(*) FROM names_trigram").fetchone()[0]
        if integrity != "ok" or rtree != "ok" or fts_count != name_count or trigram_count != name_count:
            raise RuntimeError(
                f"catalog validation failed: integrity={integrity}, rtree={rtree}, "
                f"names={name_count}, fts={fts_count}, trigram={trigram_count}"
            )
        self.connection.execute("VACUUM")
        self.connection.close()

    def close(self) -> None:
        self.connection.close()


def swob_record(catalog: Catalog, source: dict[str, Any], feature: dict[str, Any]) -> None:
    properties = feature.get("properties", {})
    provider_id = first(properties, "msc_id", "wmo_id", "icao_id", "iata_id", "id") or feature.get("id")
    if provider_id is None:
        return
    marine = "marine" in source.get("collection", "")
    mode_raw = first(properties, "auto_man")
    attributes = {
        "station_mode": station_mode(mode_raw),
        "station_mode_original": mode_raw,
        "dataset_network": first(properties, "dataset_network"),
        "data_provider": first(properties, "data_provider", "data_provider_en"),
    }
    identifiers = [
        ("eccc", "msc", first(properties, "msc_id"), True),
        ("wmo", "wmo", first(properties, "wmo_id"), False),
        ("eccc", "eccc_station", first(properties, "iata_id"), False),
        ("icao", "icao", first(properties, "icao_id"), False),
    ]
    names = [
        ("en-CA", first(properties, "name_en", "name"), "canonical", True),
        ("fr-CA", first(properties, "name_fr"), "canonical", False),
    ]
    catalog.add_entity(
        source_id=source["id"],
        identity_authority=source.get("identity_authority", "eccc"),
        provider_id=provider_id,
        kind="marine_station" if marine else "weather_station",
        names=names,
        identifiers=identifiers,
        geometry=feature.get("geometry"),
        capabilities=("swob", "marine_observation" if marine else "surface_observation"),
        country="CA",
        region=canada_region(first(properties, "province_territory")),
        reporting="recently_reporting",
        source_quality=0.95,
        attributes={key: value for key, value in attributes.items() if value is not None},
    )


def eccc_zone_record(catalog: Catalog, source: dict[str, Any], feature: dict[str, Any]) -> None:
    properties = feature.get("properties", {})
    clc = first(properties, "CLC")
    code = clc or first(properties, "FEATURE_ID", "id") or feature.get("id")
    if code is None:
        return
    marine = "marine" in source.get("collection", "")
    identifiers = [
        ("eccc", "clc", clc, True),
        ("eccc", "eccc_feature", first(properties, "FEATURE_ID"), False),
    ]
    if marine:
        identifiers.append(("eccc", "marine", code, clc is None))
    catalog.add_entity(
        source_id=source["id"],
        identity_authority=source.get("identity_authority", "eccc"),
        provider_id=code,
        kind="marine_forecast_zone" if marine else "public_forecast_zone",
        names=(
            ("en-CA", first(properties, "NAME", "name_en"), "canonical", True),
            ("fr-CA", first(properties, "NOM", "name_fr"), "canonical", False),
        ),
        identifiers=identifiers,
        geometry=feature.get("geometry"),
        capabilities=("forecast", "marine_forecast" if marine else "public_forecast"),
        country=str(first(properties, "COUNTRY_C") or "CA"),
        region=first(properties, "PROVINCE_C"),
        source_quality=0.98,
        valid_from=source.get("valid_from"),
        valid_to=source.get("valid_to"),
        attributes={"provider_version": first(properties, "version")},
    )


def eccc_citypage_record(catalog: Catalog, source: dict[str, Any], feature: dict[str, Any]) -> None:
    properties = feature.get("properties", {})
    identifier = first(properties, "identifier") or feature.get("id")
    if identifier is None:
        return
    names = properties.get("name") if isinstance(properties.get("name"), dict) else {}
    region = properties.get("region") if isinstance(properties.get("region"), dict) else {}
    station = properties.get("currentConditions", {}).get("station", {})
    station_code = station.get("code", {}).get("en") if isinstance(station, dict) else None
    catalog.add_entity(
        source_id=source["id"],
        identity_authority="eccc-citypage",
        provider_id=identifier,
        kind="forecast_location",
        names=(
            ("en-CA", names.get("en"), "canonical", True),
            ("fr-CA", names.get("fr"), "canonical", False),
        ),
        identifiers=(
            ("eccc", "eccc_citypage", identifier, True),
            ("eccc", "eccc_station", station_code, False),
        ),
        geometry=feature.get("geometry"),
        capabilities=("public_forecast", "weather_on_demand"),
        country="CA",
        region=region.get("en"),
        lifecycle="unknown",
        reporting="recently_reporting",
        source_quality=0.94,
        attributes={"last_updated": properties.get("lastUpdated")},
    )


def ogc_source(catalog: Catalog, source: dict[str, Any], allow_downloads: bool) -> None:
    location = source["location"]
    raw = source_bytes(location, allow_downloads)
    source_hasher = hashlib.sha256(raw)
    document = json.loads(raw)
    features = list(document.get("features", []))
    next_url = next(
        (str(link["href"]) for link in document.get("links", []) if link.get("rel") == "next" and link.get("href")),
        None,
    )
    while next_url:
        page_raw = source_bytes(next_url, allow_downloads)
        source_hasher.update(page_raw)
        page = json.loads(page_raw)
        features.extend(page.get("features", []))
        next_url = next(
            (str(link["href"]) for link in page.get("links", []) if link.get("rel") == "next" and link.get("href")),
            None,
        )
    catalog.add_source(source, source_hasher.hexdigest())
    for feature in features:
        if source["adapter"] == "eccc_swob":
            swob_record(catalog, source, feature)
        elif source["adapter"] == "eccc_zone":
            eccc_zone_record(catalog, source, feature)
        elif source["adapter"] == "eccc_citypage":
            eccc_citypage_record(catalog, source, feature)
        else:
            generic_geojson_record(catalog, source, feature)


def generic_geojson_record(catalog: Catalog, source: dict[str, Any], feature: dict[str, Any]) -> None:
    properties = feature.get("properties", {})
    provider_id = first(properties, *(source.get("id_fields") or ["id", "code"])) or feature.get("id")
    if provider_id is None:
        return
    name_fields = source.get("name_fields") or ["name", "NAME"]
    names = []
    seen_names = set()
    primary_name = True
    primary_spec = source.get("primary_name_when")
    if primary_spec:
        primary_value = normalize_name(first(properties, primary_spec["field"]))
        primary_name = primary_value in {
            normalize_name(value) for value in primary_spec.get("values", [])
        }
    locale = source.get("locale")
    if source.get("locale_field"):
        locale = first(properties, source["locale_field"]) or locale
    for field in name_fields:
        name = first(properties, field)
        if name is None or str(name).strip() in seen_names:
            continue
        seen_names.add(str(name).strip())
        is_primary = primary_name and not names
        names.append((locale, name, "canonical" if is_primary else "alternate", is_primary))
    identifiers = []
    for spec in source.get("identifiers", []):
        value = identifier_value(properties, spec)
        identifiers.append((spec.get("authority", source["id"]), spec["scheme"], value, spec.get("primary", False)))
    kind = source.get("kind")
    if source.get("kind_field"):
        raw_kind = first(properties, source["kind_field"])
        if raw_kind:
            kind = normalize_name(raw_kind).replace(" ", "_")
    lifecycle = source.get("lifecycle", "unknown")
    lifecycle_spec = source.get("lifecycle_map")
    if lifecycle_spec:
        raw_lifecycle = normalize_name(first(properties, lifecycle_spec["field"]))
        lifecycle = lifecycle_spec.get("values", {}).get(raw_lifecycle, lifecycle)
    inactive_field = source.get("inactive_when_field_present")
    if inactive_field and first(properties, inactive_field) is not None:
        lifecycle = "inactive"
    reporting = source.get("reporting", "unknown")
    raw_region = first(properties, *(source.get("region_fields") or []))
    region = source.get("region_map", {}).get(str(raw_region), raw_region)
    if source.get("region_normalizer") == "canada":
        region = canada_region(region)
    valid_from = source.get("valid_from")
    if source.get("valid_from_field"):
        valid_from = first(properties, source["valid_from_field"]) or valid_from
    valid_to = source.get("valid_to")
    if source.get("valid_to_field"):
        valid_to = first(properties, source["valid_to_field"]) or valid_to
    attributes = properties
    if source.get("attribute_fields"):
        attributes = {
            field: value
            for field in source["attribute_fields"]
            if (value := first(properties, field)) is not None
        }
    catalog.add_entity(
        source_id=source["id"],
        identity_authority=source.get("identity_authority"),
        provider_id=provider_id,
        kind=kind or "place",
        names=names,
        identifiers=identifiers,
        geometry=feature.get("geometry"),
        capabilities=source.get("capabilities", []),
        country=source.get("country") or first(properties, *(source.get("country_fields") or [])),
        region=region,
        source_quality=float(source.get("source_quality", 0.8)),
        lifecycle=lifecycle,
        reporting=reporting,
        valid_from=valid_from,
        valid_to=valid_to,
        attributes=attributes,
    )


def delimited_source(catalog: Catalog, source: dict[str, Any], allow_downloads: bool) -> None:
    raw = source_bytes(source["location"], allow_downloads)
    catalog.add_source(source, hashlib.sha256(raw).hexdigest())
    if zipfile.is_zipfile(io.BytesIO(raw)):
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            candidates = [
                name
                for name in archive.namelist()
                if not name.endswith("/")
                and (not source.get("archive_suffix") or name.endswith(source["archive_suffix"]))
                and (not source.get("archive_contains") or source["archive_contains"] in name)
            ]
            if not candidates:
                raise RuntimeError(f"source {source['id']} archive has no matching delimited file")
            with archive.open(sorted(candidates)[0]) as binary_stream:
                with io.TextIOWrapper(
                    binary_stream, encoding=source.get("encoding", "utf-8-sig"), newline=""
                ) as text_stream:
                    ingest_delimited_rows(catalog, source, text_stream)
        return
    with io.StringIO(raw.decode(source.get("encoding", "utf-8-sig")), newline="") as text_stream:
        ingest_delimited_rows(catalog, source, text_stream)


def ingest_delimited_rows(
    catalog: Catalog, source: dict[str, Any], text_stream: Iterable[str]
) -> None:
    reader = csv.DictReader(text_stream, delimiter=source.get("delimiter", ","))
    for _ in range(int(source.get("skip_rows_after_header", 0))):
        next(reader, None)
    for properties in reader:
        include_when_present = source.get("include_when_present", [])
        if include_when_present and not any(
            first(properties, field) is not None for field in include_when_present
        ):
            continue
        latitude = first(properties, source.get("latitude_field", "latitude"))
        longitude = first(properties, source.get("longitude_field", "longitude"))
        geometry = None
        if latitude is not None and longitude is not None:
            geometry = {"type": "Point", "coordinates": [float(longitude), float(latitude)]}
        generic_geojson_record(catalog, source, {"properties": properties, "geometry": geometry})


def shapefile_source(catalog: Catalog, source: dict[str, Any], allow_downloads: bool) -> None:
    try:
        import shapefile  # type: ignore
    except ImportError as error:
        raise RuntimeError("shapefile sources require pyshp from requirements-location-catalog.txt") from error
    raw = source_bytes(source["location"], allow_downloads)
    catalog.add_source(source, hashlib.sha256(raw).hexdigest())
    with tempfile.TemporaryDirectory(prefix="haze-location-shape-") as directory:
        input_path = Path(directory) / "source"
        if zipfile.is_zipfile(io.BytesIO(raw)):
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                shape_path = extract_shapefile_archive(
                    archive, Path(directory), source.get("archive_contains")
                )
        else:
            input_path.with_suffix(".shp").write_bytes(raw)
            shape_path = input_path.with_suffix(".shp")
        reader = shapefile.Reader(str(shape_path), encoding=source.get("encoding", "utf-8"))
        try:
            for shape_record in reader.iterShapeRecords():
                properties = shape_record.record.as_dict()
                feature = {
                    "properties": properties,
                    "geometry": transform_geometry(
                        shape_record.shape.__geo_interface__, source.get("source_crs")
                    ),
                }
                generic_geojson_record(catalog, source, feature)
        finally:
            reader.close()


def extract_shapefile_archive(
    archive: zipfile.ZipFile, directory: Path, archive_contains: str | None
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    candidates = sorted(
        name
        for name in archive.namelist()
        if name.lower().endswith(".shp")
        and (not archive_contains or archive_contains.lower() in name.lower())
    )
    if not candidates:
        expected = f" matching {archive_contains!r}" if archive_contains else ""
        raise RuntimeError(f"shapefile archive has no .shp member{expected}")
    selected = candidates[0]
    prefix = selected[: -len(Path(selected).suffix)]
    output_prefix = directory / "selected"
    extracted = set()
    for suffix in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
        member = next(
            (
                name
                for name in archive.namelist()
                if name.lower() == (prefix + suffix).lower()
            ),
            None,
        )
        if member is None:
            continue
        output = output_prefix.with_suffix(suffix)
        with archive.open(member) as source_stream, output.open("wb") as output_stream:
            shutil.copyfileobj(source_stream, output_stream)
        extracted.add(suffix)
    if not {".shp", ".dbf"}.issubset(extracted):
        raise RuntimeError("selected shapefile is missing its .shp or .dbf component")
    return output_prefix.with_suffix(".shp")


def ndbc_source(catalog: Catalog, source: dict[str, Any], allow_downloads: bool) -> None:
    import xml.etree.ElementTree as ET

    raw = source_bytes(source["location"], allow_downloads)
    catalog.add_source(source, hashlib.sha256(raw).hexdigest())
    root = ET.fromstring(raw)
    for station in root.findall(".//station"):
        values = station.attrib
        station_id = values.get("id")
        latitude = values.get("lat")
        longitude = values.get("lon")
        if not station_id or latitude is None or longitude is None:
            continue
        geometry = {"type": "Point", "coordinates": [float(longitude), float(latitude)]}
        catalog.add_entity(
            source_id=source["id"],
            identity_authority=source.get("identity_authority", "ndbc"),
            provider_id=station_id,
            kind="marine_buoy",
            names=(("en-US", values.get("name") or station_id, "canonical", True),),
            identifiers=(("noaa", "ndbc", station_id, True),),
            geometry=geometry,
            capabilities=("marine_observation",),
            country="US",
            lifecycle="unknown",
            reporting="recently_reporting",
            source_quality=0.95,
            attributes={key: value for key, value in values.items() if key not in {"lat", "lon"}},
        )


def epa_aqs_sites_source(catalog: Catalog, source: dict[str, Any], allow_downloads: bool) -> None:
    raw = source_bytes(source["location"], allow_downloads)
    catalog.add_source(source, hashlib.sha256(raw).hexdigest())
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        csv_name = next(name for name in archive.namelist() if name.lower().endswith(".csv"))
        reader = csv.DictReader(io.TextIOWrapper(archive.open(csv_name), encoding="utf-8-sig"))
        for row in reader:
            raw_state = str(row.get("State Code") or "").strip()
            raw_county = str(row.get("County Code") or "").strip()
            raw_site = str(row.get("Site Number") or "").strip()
            if not raw_state or not raw_county or not raw_site:
                continue
            state = raw_state.zfill(2)
            county = raw_county.zfill(3)
            site = raw_site.zfill(4)
            identifier = f"{state}{county}{site}"
            latitude = first(row, "Latitude")
            longitude = first(row, "Longitude")
            geometry = None
            if latitude is not None and longitude is not None:
                geometry = {"type": "Point", "coordinates": [float(longitude), float(latitude)]}
            closed = first(row, "Site Closed Date")
            site_name = first(row, "Local Site Name", "Address", "City Name") or identifier
            catalog.add_entity(
                source_id=source["id"],
                identity_authority="epa-aqs",
                provider_id=identifier,
                kind="air_quality_station",
                names=(
                    ("en-US", site_name, "canonical", True),
                    ("en-US", first(row, "Address"), "alternate", False),
                    ("en-US", first(row, "City Name"), "locality", False),
                ),
                identifiers=(("epa", "epa_aqs", identifier, True),),
                geometry=geometry,
                capabilities=("air_quality", "aqs"),
                country="US",
                region=state,
                lifecycle="inactive" if closed else "active",
                reporting="unknown",
                source_quality=float(source.get("source_quality", 0.99)),
                valid_from=first(row, "Site Established Date"),
                valid_to=closed,
                attributes={key: value for key, value in row.items() if value},
            )


def airnow_sites_source(catalog: Catalog, source: dict[str, Any], allow_downloads: bool) -> None:
    raw = source_bytes(source["location"], allow_downloads)
    catalog.add_source(source, hashlib.sha256(raw).hexdigest())
    stations: dict[str, dict[str, Any]] = {}
    for line in raw.decode(source.get("encoding", "latin1"), errors="replace").splitlines():
        fields = line.split("|")
        if len(fields) < 21:
            continue
        station_id = fields[0].strip()
        if not station_id:
            continue
        station = stations.setdefault(
            station_id,
            {
                "name": fields[3].strip() or station_id,
                "status": fields[4].strip(),
                "agency_id": fields[5].strip(),
                "agency_name": fields[6].strip(),
                "latitude": fields[8].strip(),
                "longitude": fields[9].strip(),
                "elevation": fields[10].strip(),
                "country": fields[12].strip(),
                "region": fields[17].strip(),
                "region_abbreviation": fields[18].strip(),
                "county": fields[20].strip(),
                "parameters": set(),
            },
        )
        if fields[1].strip():
            station["parameters"].add(fields[1].strip())
    for station_id, station in stations.items():
        latitude = float(station["latitude"]) if station["latitude"] else None
        longitude = float(station["longitude"]) if station["longitude"] else None
        geometry = None
        if latitude is not None and longitude is not None:
            geometry = {"type": "Point", "coordinates": [longitude, latitude]}
        is_us_aqs = station["country"] == "US" and len(station_id) == 9 and station_id.isdigit()
        identifiers = [("airnow", "airnow", station_id, True)]
        if is_us_aqs:
            identifiers.append(("epa", "epa_aqs", station_id, False))
        active = normalize_name(station["status"]) == "active"
        catalog.add_entity(
            source_id=source["id"],
            identity_authority="epa-aqs" if is_us_aqs else "airnow",
            provider_id=station_id,
            kind="air_quality_station",
            names=(("en", station["name"], "canonical", True),),
            identifiers=identifiers,
            geometry=geometry,
            capabilities=("air_quality", "airnow", *sorted(station["parameters"])),
            country=station["country"] or None,
            region=station["region"] or station["region_abbreviation"] or None,
            lifecycle="active" if active else "inactive",
            reporting="recently_reporting" if active else "not_reporting",
            source_quality=float(source.get("source_quality", 0.88)),
            attributes={
                **station,
                "parameters": sorted(station["parameters"]),
            },
        )


def ndbc_metadata_source(catalog: Catalog, source: dict[str, Any], allow_downloads: bool) -> None:
    import xml.etree.ElementTree as ET

    raw = source_bytes(source["location"], allow_downloads)
    catalog.add_source(source, hashlib.sha256(raw).hexdigest())
    root = ET.fromstring(raw)
    for station in root.findall(".//station"):
        values = station.attrib
        station_id = str(values.get("id") or "").strip()
        if not station_id:
            continue
        canonical_id = catalog.add_entity(
            source_id=source["id"],
            identity_authority=source.get("identity_authority", "ndbc"),
            provider_id=station_id,
            kind="marine_buoy",
            names=(("en-US", values.get("name") or station_id, "canonical", True),),
            identifiers=(("noaa", "ndbc", station_id, True),),
            geometry=None,
            capabilities=("marine_observation",),
            country=source.get("country", "US"),
            lifecycle="unknown",
            reporting="unknown",
            source_quality=float(source.get("source_quality", 0.97)),
            attributes={
                "owner": values.get("owner"),
                "program": values.get("pgm"),
                "platform_type": values.get("type"),
            },
        )
        entity_pk = catalog.connection.execute(
            "SELECT entity_pk FROM entities WHERE canonical_id = ?", (canonical_id,)
        ).fetchone()[0]
        for sequence, history in enumerate(station.findall("history"), start=1):
            deployment = history.attrib
            latitude = float(deployment["lat"]) if deployment.get("lat") else None
            longitude = float(deployment["lng"]) if deployment.get("lng") else None
            if latitude is not None and longitude is not None:
                geometry_bbox({"type": "Point", "coordinates": [longitude, latitude]})
            valid_from = deployment.get("start") or None
            valid_to = deployment.get("stop") or None
            deployment_id = f"{station_id}:{valid_from or sequence}:{deployment.get('hull') or ''}"
            catalog.connection.execute(
                """INSERT INTO deployments(
                       entity_pk, provider_deployment_id, owner, platform_type, latitude, longitude,
                       elevation_m, valid_from, valid_to, reporting_status, attributes_json, source_id
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    entity_pk,
                    deployment_id,
                    values.get("owner"),
                    values.get("type"),
                    latitude,
                    longitude,
                    float(deployment["elev"]) if deployment.get("elev") else None,
                    valid_from,
                    valid_to,
                    "deployment_current" if not valid_to else "historical",
                    json.dumps(deployment, sort_keys=True, separators=(",", ":")),
                    source["id"],
                ),
            )
            if latitude is None or longitude is None:
                continue
            geometry = {"type": "Point", "coordinates": [longitude, latitude]}
            minimum_lon, maximum_lon, minimum_lat, maximum_lat = geometry_bbox(geometry)
            cursor = catalog.connection.execute(
                """INSERT INTO geometries(
                       entity_pk, geometry_type, geometry_wkb, latitude, longitude,
                       min_lon, max_lon, min_lat, max_lat, valid_from, valid_to,
                       is_current, source_id
                   ) VALUES(?, 'point', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    entity_pk,
                    geometry_wkb(geometry),
                    latitude,
                    longitude,
                    minimum_lon,
                    maximum_lon,
                    minimum_lat,
                    maximum_lat,
                    valid_from,
                    valid_to,
                    int(not valid_to),
                    source["id"],
                ),
            )
            catalog.connection.execute(
                "INSERT INTO entity_rtree VALUES(?, ?, ?, ?, ?)",
                (int(cursor.lastrowid), minimum_lon, maximum_lon, minimum_lat, maximum_lat),
            )


def deployment_history_source(catalog: Catalog, source: dict[str, Any], allow_downloads: bool) -> None:
    raw = source_bytes(source["location"], allow_downloads)
    catalog.add_source(source, hashlib.sha256(raw).hexdigest())
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    for row in reader:
        station_id = row.get("station_id", "").strip()
        if not station_id:
            continue
        canonical_id = stable_id(source.get("identity_authority", "ndbc"), "marine_buoy", station_id)
        entity = catalog.connection.execute(
            "SELECT entity_pk FROM entities WHERE canonical_id = ?", (canonical_id,)
        ).fetchone()
        if entity is None:
            canonical_id = catalog.add_entity(
                source_id=source["id"], identity_authority=source.get("identity_authority", "ndbc"),
                provider_id=station_id, kind="marine_buoy",
                names=(("en-US", row.get("name") or station_id, "canonical", True),),
                identifiers=(("noaa", "ndbc", station_id, True),), geometry=None,
                capabilities=("marine_observation",), country="US", source_quality=0.9,
            )
            entity = catalog.connection.execute(
                "SELECT entity_pk FROM entities WHERE canonical_id = ?", (canonical_id,)
            ).fetchone()
        latitude = float(row["latitude"]) if row.get("latitude") else None
        longitude = float(row["longitude"]) if row.get("longitude") else None
        catalog.connection.execute(
            """INSERT INTO deployments(
                   entity_pk, provider_deployment_id, owner, platform_type, latitude, longitude,
                   elevation_m, valid_from, valid_to, reporting_status, attributes_json, source_id
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                entity[0], row.get("deployment_id"), row.get("owner"), row.get("platform_type"),
                latitude,
                longitude,
                float(row["elevation_m"]) if row.get("elevation_m") else None,
                row.get("valid_from") or None, row.get("valid_to") or None,
                row.get("reporting_status") or "unknown",
                json.dumps(row, sort_keys=True, separators=(",", ":")), source["id"],
            ),
        )
        if latitude is not None and longitude is not None:
            geometry = {"type": "Point", "coordinates": [longitude, latitude]}
            minimum_lon, maximum_lon, minimum_lat, maximum_lat = geometry_bbox(geometry)
            cursor = catalog.connection.execute(
                """INSERT INTO geometries(
                       entity_pk, geometry_type, geometry_wkb, latitude, longitude,
                       min_lon, max_lon, min_lat, max_lat, valid_from, valid_to,
                       is_current, source_id
                   ) VALUES(?, 'point', ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)""",
                (
                    entity[0], geometry_wkb(geometry), latitude, longitude,
                    minimum_lon, maximum_lon, minimum_lat, maximum_lat,
                    row.get("valid_from") or None, row.get("valid_to") or None, source["id"],
                ),
            )
            catalog.connection.execute(
                "INSERT INTO entity_rtree VALUES(?, ?, ?, ?, ?)",
                (int(cursor.lastrowid), minimum_lon, maximum_lon, minimum_lat, maximum_lat),
            )
        catalog.source_counts[source["id"]] += 1


def build(manifest_path: Path, pack_id: str, output: Path, retrieved_at: str, allow_downloads: bool) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    pack = next((item for item in manifest["packs"] if item["id"] == pack_id), None)
    if pack is None:
        raise RuntimeError(f"pack {pack_id!r} is not present in {manifest_path}")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="haze-location-build-", dir=output.parent) as directory:
        temporary = Path(directory) / output.name
        catalog = Catalog(temporary, pack_id, retrieved_at)
        expected_counts: dict[str, int] = {}
        try:
            for source in pack.get("sources", []):
                expected_counts[source["id"]] = int(source.get("expected_min", 1))
                adapter = source["adapter"]
                if adapter.startswith("eccc_") or adapter == "geojson":
                    ogc_source(catalog, source, allow_downloads)
                elif adapter == "shapefile":
                    shapefile_source(catalog, source, allow_downloads)
                elif adapter in {"csv", "delimited"}:
                    delimited_source(catalog, source, allow_downloads)
                elif adapter == "ndbc_active":
                    ndbc_source(catalog, source, allow_downloads)
                elif adapter == "ndbc_metadata":
                    ndbc_metadata_source(catalog, source, allow_downloads)
                elif adapter == "epa_aqs_sites":
                    epa_aqs_sites_source(catalog, source, allow_downloads)
                elif adapter == "airnow_sites":
                    airnow_sites_source(catalog, source, allow_downloads)
                elif adapter == "deployment_history_csv":
                    deployment_history_source(catalog, source, allow_downloads)
                else:
                    raise RuntimeError(f"unsupported source adapter {adapter!r}")
            catalog.finish(expected_counts)
        finally:
            catalog.close()
        digest = hashlib.sha256(temporary.read_bytes()).hexdigest()
        output_tmp = output.with_suffix(output.suffix + ".new")
        shutil.copyfile(temporary, output_tmp)
        os.replace(output_tmp, output)
        checksum_tmp = output.with_suffix(output.suffix + ".sha256.new")
        checksum_tmp.write_text(f"{digest}  {output.name}\n", encoding="ascii")
        os.replace(checksum_tmp, output.with_suffix(output.suffix + ".sha256"))
        print(json.dumps({"pack": pack_id, "output": str(output), "sha256": digest}, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--pack", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--retrieved-at", required=True, help="ISO 8601 source retrieval time")
    parser.add_argument("--allow-downloads", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    build(
        arguments.manifest,
        arguments.pack,
        arguments.output,
        arguments.retrieved_at,
        arguments.allow_downloads,
    )
