PRAGMA foreign_keys = ON;
PRAGMA user_version = 1;

-- Every published pack must bind to its exact immutable core with these
-- catalog_metadata keys: pack_kind, pack_id, core_pack_id, and core_sha256.

CREATE TABLE IF NOT EXISTS catalog_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    source_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    source_version TEXT,
    retrieved_at TEXT,
    valid_from TEXT,
    valid_to TEXT,
    licence TEXT,
    attribution TEXT,
    source_sha256 TEXT,
    attributes_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS area_geometries (
    geometry_pk INTEGER PRIMARY KEY,
    canonical_id TEXT NOT NULL,
    source TEXT,
    code TEXT,
    same_code TEXT,
    geometry_type TEXT NOT NULL,
    geometry_wkb BLOB NOT NULL,
    latitude REAL,
    longitude REAL,
    min_lon REAL NOT NULL,
    max_lon REAL NOT NULL,
    min_lat REAL NOT NULL,
    max_lat REAL NOT NULL,
    accuracy_m REAL,
    valid_from TEXT,
    valid_to TEXT,
    is_current INTEGER NOT NULL DEFAULT 1 CHECK (is_current IN (0, 1)),
    source_id TEXT,
    provider_version TEXT,
    source_url TEXT,
    updated_at TEXT,
    attributes_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_area_geometries_canonical
    ON area_geometries(canonical_id, is_current);
CREATE INDEX IF NOT EXISTS idx_area_geometries_source_code
    ON area_geometries(source, code, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS idx_area_geometries_legacy_identity
    ON area_geometries(source, code)
    WHERE source IS NOT NULL AND code IS NOT NULL;

CREATE VIRTUAL TABLE IF NOT EXISTS area_geometry_rtree USING rtree(
    geometry_pk,
    min_lon,
    max_lon,
    min_lat,
    max_lat
);
