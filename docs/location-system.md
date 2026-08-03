# Unified location system

`haze-location` is the canonical, offline location resolver for Haze. It runs as a standalone Rust service under the daemon and communicates with other services only through the host event bridge. It does not download source data at runtime.

## Runtime layout

- `managed/configs/locations.xml` selects immutable packs, pack priority, overlay source allowlists, query defaults, feed bindings, and grouping overrides.
- `managed/locations/global-core.sqlite` contains Natural Earth geography, NGA GNS foreign names, USGS GNIS domestic names, and OurAirports aviation identities.
- `managed/locations/ca-weather.sqlite` contains Statistics Canada FSA and SGC geography plus ECCC SWOB, climate, hydrometric, NAPS, AQHI, citypage, public forecast, and marine forecast data.
- `managed/locations/us-weather.sqlite` contains Census ZCTAs, authoritative FAA airports, NWS public, fire, county, coastal, offshore, and high-seas zones, EPA AQS, AirNow, and current plus historical NDBC deployments.
- `runtime/state/location-overlay.sqlite` is service-owned mutable state for feed bindings and validated discoveries.
- `runtime/state/location-catalog-generations/` retains validated pack snapshots so a reload can be rolled back without changing managed files.

The service validates checksums, the schema version, source counts, coordinates, FTS, RTree, and SQLite integrity before it publishes a new generation. A failed validation leaves the current generation active.

## Broker contract

Clients connect to the host bridge with a stable `client_id`, set `receive_events` to true, and subscribe to the location completion and failure events. Requests set `target` to `haze-location`, `reply_to` to the client ID, and `subject` to the request ID.

The v1 event types are:

- `location.query.request`, `location.query.completed`, and `location.query.failed`
- `location.overlay.upsert.request` and `location.overlay.upserted`
- `location.catalog.reload.request`, `location.catalog.rollback.request`, `location.catalog.ready`, `location.catalog.reloaded`, and `location.catalog.reload_failed`

Shared request and response fixtures are in `contracts/location/v1/`. Rust and Go tests both deserialize these files.

The query operations are `resolve`, `batch_resolve`, `search`, `point_facets`, `nearest`, and `traverse`. `batch_resolve` accepts at most 100 inputs. Traversal requires an explicit relationship allowlist, defaults to depth 1, and is capped at depth 5 and 10,000 visited entities. The operator default limit comes from `locations.xml`, and the contract maximum is 100.

An unattended public-safety consumer may act only on exact or high-confidence mappings. If `haze-location` is unavailable, preserve and compare the original CAP, SAME, or provider code exactly. Do not broaden coverage using a guessed match.

## Search and grouping

Name search combines normalized exact aliases, identifiers, FTS token and prefix retrieval, trigram typo retrieval, and deterministic reranking. It supports locale and geographic bias plus `text`, `voice`, and `t9` input modes.

Similar-name grouping is disabled by default. Set `dedupe_mode` to `similar_name` to group compatible point facilities for presentation. Grouping never merges identities or identifiers. It requires country and first-level region agreement, distance limits, and either a shared locality stem or strong lexical similarity. Exact identifier resolution is never grouped.

`station_mode_preference` reranks otherwise valid SWOB station candidates as `auto` or `manual`. `station_mode_requirement` filters candidates before ranking. Exact identifier and exact name hits remain ahead of station-mode preference.

## Rollout

`services.rust.location.mode` in `config.yaml` and `HAZE_LOCATION_MODE` for Go consumers support these phases:

1. `legacy`: start and validate the location service while consumers keep existing results.
2. `shadow`: consumers keep existing results and log canonical ID, confidence, ambiguity, and latency differences.
3. `authoritative`: consumers use exact and high-confidence canonical results while retaining raw source identifiers.

IVR and web weather-on-demand have the initial query-client integration. Product rendering consumes and republishes canonical IDs alongside raw codes, while data ingest publishes validated discoveries to the overlay instead of maintaining another gazetteer. Persisted ingest records can hold a nullable canonical location ID alongside their original location code.

## Building catalog packs

Catalogs are built outside the daemon. Downloads require an explicit flag:

```sh
python scripts/util/build_location_catalog.py \
  --manifest scripts/util/location-catalog-sources.json \
  --pack ca-weather \
  --output dist/location-catalogs/ca-weather.sqlite \
  --retrieved-at 2026-08-02T00:00:00Z \
  --allow-downloads
```

The builder writes the database atomically and creates a `.sha256` sidecar. The scheduled `Location Catalogs` workflow builds and validates `global-core`, `ca-weather`, and `us-weather`, then publishes them as workflow artifacts. Downloaded archives and generated SQLite packs are intentionally excluded from the repository.

Provider identifiers, source quality, record provenance, attribution, effective dates, and deployment history are retained. SWOB `AUTO`, `AUTO-minute`, `MAN`, and `MANNED` values are normalized while the provider value is preserved. The ECCC `iata_id` field is stored as an ECCC station identifier unless an authoritative airport crosswalk is present.

## Operator-supplied packs

Full Canadian postal addresses, official IATA and ICAO subscriptions, and licence-sensitive WMO or OSCAR data are not downloaded by the default builder. After validating the licence and schema, install an operator pack under `managed/locations/` and add it to `locations.xml` with the desired priority.
