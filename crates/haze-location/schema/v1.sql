PRAGMA foreign_keys = ON;
PRAGMA user_version = 1;

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

CREATE TABLE IF NOT EXISTS entities (
    entity_pk INTEGER PRIMARY KEY,
    canonical_id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    country TEXT,
    region TEXT,
    lifecycle_status TEXT NOT NULL DEFAULT 'unknown',
    reporting_status TEXT NOT NULL DEFAULT 'unknown',
    source_quality REAL NOT NULL DEFAULT 0.5,
    valid_from TEXT,
    valid_to TEXT,
    attributes_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS entity_capabilities (
    entity_pk INTEGER NOT NULL REFERENCES entities(entity_pk) ON DELETE CASCADE,
    capability TEXT NOT NULL,
    PRIMARY KEY (entity_pk, capability)
);

CREATE TABLE IF NOT EXISTS identifiers (
    identifier_pk INTEGER PRIMARY KEY,
    entity_pk INTEGER NOT NULL REFERENCES entities(entity_pk) ON DELETE CASCADE,
    authority TEXT NOT NULL,
    scheme TEXT NOT NULL,
    value TEXT NOT NULL,
    normalized_value TEXT NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0, 1)),
    confidence TEXT NOT NULL DEFAULT 'exact',
    source_id TEXT,
    UNIQUE (entity_pk, authority, scheme, normalized_value)
);

CREATE INDEX IF NOT EXISTS idx_identifiers_lookup
    ON identifiers(authority, scheme, normalized_value);
CREATE INDEX IF NOT EXISTS idx_identifiers_scheme_lookup
    ON identifiers(scheme, normalized_value);

CREATE TABLE IF NOT EXISTS names (
    name_pk INTEGER PRIMARY KEY,
    entity_pk INTEGER NOT NULL REFERENCES entities(entity_pk) ON DELETE CASCADE,
    locale TEXT,
    name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    t9_digits TEXT NOT NULL DEFAULT '',
    name_kind TEXT NOT NULL DEFAULT 'alternate',
    is_primary INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0, 1)),
    source_id TEXT,
    UNIQUE (entity_pk, locale, name, name_kind)
);

CREATE INDEX IF NOT EXISTS idx_names_entity ON names(entity_pk);
CREATE INDEX IF NOT EXISTS idx_names_normalized ON names(normalized_name);
CREATE INDEX IF NOT EXISTS idx_names_t9 ON names(t9_digits);

CREATE VIRTUAL TABLE IF NOT EXISTS names_fts USING fts5(
    name,
    normalized_name,
    entity_pk UNINDEXED,
    locale UNINDEXED,
    tokenize = 'unicode61 remove_diacritics 2',
    prefix = '2 3'
);

CREATE VIRTUAL TABLE IF NOT EXISTS names_trigram USING fts5(
    normalized_name,
    entity_pk UNINDEXED,
    tokenize = 'trigram remove_diacritics 1'
);

CREATE TABLE IF NOT EXISTS geometries (
    geometry_pk INTEGER PRIMARY KEY,
    entity_pk INTEGER NOT NULL REFERENCES entities(entity_pk) ON DELETE CASCADE,
    geometry_type TEXT NOT NULL,
    geometry_wkb BLOB,
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
    source_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_geometries_entity ON geometries(entity_pk, is_current);

CREATE VIRTUAL TABLE IF NOT EXISTS entity_rtree USING rtree(
    geometry_pk,
    min_lon,
    max_lon,
    min_lat,
    max_lat
);

CREATE TABLE IF NOT EXISTS deployments (
    deployment_pk INTEGER PRIMARY KEY,
    entity_pk INTEGER NOT NULL REFERENCES entities(entity_pk) ON DELETE CASCADE,
    provider_deployment_id TEXT,
    owner TEXT,
    platform_type TEXT,
    latitude REAL,
    longitude REAL,
    elevation_m REAL,
    valid_from TEXT,
    valid_to TEXT,
    reporting_status TEXT NOT NULL DEFAULT 'unknown',
    attributes_json TEXT NOT NULL DEFAULT '{}',
    source_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_deployments_entity
    ON deployments(entity_pk, valid_from, valid_to);

CREATE TABLE IF NOT EXISTS relationships (
    relationship_pk INTEGER PRIMARY KEY,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    confidence TEXT NOT NULL,
    score REAL NOT NULL,
    method TEXT NOT NULL,
    distance_m REAL,
    valid_from TEXT,
    valid_to TEXT,
    source_id TEXT,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (from_id, to_id, relationship_type, source_id)
);

CREATE INDEX IF NOT EXISTS idx_relationships_from
    ON relationships(from_id, relationship_type);
CREATE INDEX IF NOT EXISTS idx_relationships_to
    ON relationships(to_id, relationship_type);

CREATE TABLE IF NOT EXISTS feed_bindings (
    binding_pk INTEGER PRIMARY KEY,
    feed_id TEXT NOT NULL,
    purpose TEXT NOT NULL,
    raw_source TEXT NOT NULL,
    raw_identifier TEXT NOT NULL,
    canonical_id TEXT,
    confidence TEXT,
    match_status TEXT NOT NULL DEFAULT 'unresolved',
    config_sha256 TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE (feed_id, purpose, raw_source, raw_identifier)
);

CREATE INDEX IF NOT EXISTS idx_feed_bindings_entity ON feed_bindings(canonical_id);
