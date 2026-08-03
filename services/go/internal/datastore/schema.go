package datastore

const postgresSchemaSQL = `
CREATE SCHEMA IF NOT EXISTS locations;
CREATE SCHEMA IF NOT EXISTS observations;
CREATE SCHEMA IF NOT EXISTS forecasts;
CREATE SCHEMA IF NOT EXISTS products;
CREATE SCHEMA IF NOT EXISTS archive;

CREATE TABLE IF NOT EXISTS locations.locations (
    canonical_id text,
    source text NOT NULL,
    location_id text NOT NULL,
    kind text NOT NULL DEFAULT 'unknown',
    name_en text,
    name_fr text,
    station_id text,
    citypage_id text,
    clc text,
    latitude double precision,
    longitude double precision,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, location_id)
);

CREATE INDEX IF NOT EXISTS idx_locations_clc
    ON locations.locations (clc)
    WHERE clc IS NOT NULL AND clc <> '';

CREATE INDEX IF NOT EXISTS idx_locations_station_id
    ON locations.locations (station_id)
    WHERE station_id IS NOT NULL AND station_id <> '';

ALTER TABLE locations.locations ADD COLUMN IF NOT EXISTS canonical_id text;

CREATE INDEX IF NOT EXISTS idx_locations_canonical_id
    ON locations.locations (canonical_id)
    WHERE canonical_id IS NOT NULL AND canonical_id <> '';

CREATE TABLE IF NOT EXISTS observations.current (
    source text NOT NULL,
    location_id text NOT NULL,
    station_id text,
    observed_at timestamptz,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, location_id)
);

CREATE INDEX IF NOT EXISTS idx_observations_observed_at
    ON observations.current (observed_at DESC);

CREATE TABLE IF NOT EXISTS forecasts.current (
    source text NOT NULL,
    forecast_id text NOT NULL,
    region_id text,
    issued_at timestamptz,
    source_updated_at timestamptz,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, forecast_id)
);

CREATE INDEX IF NOT EXISTS idx_forecasts_region_id
    ON forecasts.current (region_id)
    WHERE region_id IS NOT NULL AND region_id <> '';

CREATE INDEX IF NOT EXISTS idx_forecasts_issued_at
    ON forecasts.current (issued_at DESC);

CREATE TABLE IF NOT EXISTS products.payloads (
    kind text NOT NULL,
    source text NOT NULL,
    item_id text NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, source, item_id)
);

CREATE INDEX IF NOT EXISTS idx_products_payloads_updated_at
    ON products.payloads (updated_at DESC);

CREATE TABLE IF NOT EXISTS products.text_products (
    source text NOT NULL,
    item_id text NOT NULL,
    body text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, item_id)
);

CREATE INDEX IF NOT EXISTS idx_products_text_updated_at
    ON products.text_products (updated_at DESC);

CREATE TABLE IF NOT EXISTS archive.cap_xml (
    id bigserial PRIMARY KEY,
    alert_id text NOT NULL,
    feed_id text NOT NULL DEFAULT '',
    bucket text NOT NULL,
    status text NOT NULL,
    reason text,
    sender text,
    source text,
    sent_at timestamptz,
    updated_at timestamptz,
    expires_at timestamptz,
    event text,
    headline text,
    cap_xml_zstd bytea NOT NULL,
    cap_xml_sha256 text NOT NULL,
    original_bytes integer NOT NULL,
    compressed_bytes integer NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    stored_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (alert_id, feed_id, bucket)
);

CREATE INDEX IF NOT EXISTS idx_archive_cap_xml_bucket_time
    ON archive.cap_xml (bucket, stored_at DESC);

CREATE INDEX IF NOT EXISTS idx_archive_cap_xml_feed_time
    ON archive.cap_xml (feed_id, stored_at DESC);

CREATE INDEX IF NOT EXISTS idx_archive_cap_xml_expires
    ON archive.cap_xml (expires_at);
`

const sqliteSchemaSQL = `
CREATE TABLE IF NOT EXISTS locations_locations (
    canonical_id TEXT,
    source TEXT NOT NULL,
    location_id TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'unknown',
    name_en TEXT,
    name_fr TEXT,
    station_id TEXT,
    citypage_id TEXT,
    clc TEXT,
    latitude REAL,
    longitude REAL,
    metadata TEXT NOT NULL DEFAULT '{}',
    first_seen TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    last_seen TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (source, location_id)
);

CREATE INDEX IF NOT EXISTS idx_locations_clc
    ON locations_locations (clc)
    WHERE clc IS NOT NULL AND clc <> '';

CREATE INDEX IF NOT EXISTS idx_locations_station_id
    ON locations_locations (station_id)
    WHERE station_id IS NOT NULL AND station_id <> '';

CREATE TABLE IF NOT EXISTS observations_current (
    source TEXT NOT NULL,
    location_id TEXT NOT NULL,
    station_id TEXT,
    observed_at TEXT,
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (source, location_id)
);

CREATE INDEX IF NOT EXISTS idx_observations_observed_at
    ON observations_current (observed_at DESC);

CREATE TABLE IF NOT EXISTS forecasts_current (
    source TEXT NOT NULL,
    forecast_id TEXT NOT NULL,
    region_id TEXT,
    issued_at TEXT,
    source_updated_at TEXT,
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (source, forecast_id)
);

CREATE INDEX IF NOT EXISTS idx_forecasts_region_id
    ON forecasts_current (region_id)
    WHERE region_id IS NOT NULL AND region_id <> '';

CREATE INDEX IF NOT EXISTS idx_forecasts_issued_at
    ON forecasts_current (issued_at DESC);

CREATE TABLE IF NOT EXISTS products_payloads (
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    item_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (kind, source, item_id)
);

CREATE INDEX IF NOT EXISTS idx_products_payloads_updated_at
    ON products_payloads (updated_at DESC);

CREATE TABLE IF NOT EXISTS products_text (
    source TEXT NOT NULL,
    item_id TEXT NOT NULL,
    body TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (source, item_id)
);

CREATE INDEX IF NOT EXISTS idx_products_text_updated_at
    ON products_text (updated_at DESC);

CREATE TABLE IF NOT EXISTS archive_cap_xml (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id TEXT NOT NULL,
    feed_id TEXT NOT NULL DEFAULT '',
    bucket TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    sender TEXT,
    source TEXT,
    sent_at TEXT,
    updated_at TEXT,
    expires_at TEXT,
    event TEXT,
    headline TEXT,
    cap_xml_zstd BLOB NOT NULL,
    cap_xml_sha256 TEXT NOT NULL,
    original_bytes INTEGER NOT NULL,
    compressed_bytes INTEGER NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    stored_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (alert_id, feed_id, bucket)
);

CREATE INDEX IF NOT EXISTS idx_archive_cap_xml_bucket_time
    ON archive_cap_xml (bucket, stored_at DESC);

CREATE INDEX IF NOT EXISTS idx_archive_cap_xml_feed_time
    ON archive_cap_xml (feed_id, stored_at DESC);

CREATE INDEX IF NOT EXISTS idx_archive_cap_xml_expires
    ON archive_cap_xml (expires_at);
`
