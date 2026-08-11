# Unified location system

`haze-location` is the canonical, offline location resolver for Haze. It runs as a standalone Rust service under the daemon and communicates with other services only through the host event bridge. It does not download source data at runtime.

## Runtime layout

- `managed/configs/locations.xml` selects immutable packs, pack priority, overlay source allowlists, query defaults, feed bindings, and grouping overrides.
- `managed/locations/global-core.sqlite` contains Natural Earth names and point locations, NGA GNS foreign names, USGS GNIS domestic names, and OurAirports aviation identities.
- `managed/locations/ca-weather.sqlite` contains searchable Canadian identities, names, identifiers, relationships, stations, and representative points.
- The Canadian core enriches Statistics Canada census-subdivision identities with 2021 population and national/provincial population ranks from table 98-10-0002-01. These compact attributes are search priors only and are not duplicated into geometry packs.
- `managed/locations/us-weather.sqlite` contains searchable United States identities, names, identifiers, relationships, stations, and representative points.
- Sibling `*.geometry.sqlite` packs contain exact Polygon and MultiPolygon WKB, bounding boxes, and RTree indexes keyed by stable canonical location ID. They are independently replaceable and are opened with their configured core pack.
- `managed/alert_location_map.sqlite` remains the compact legacy core for existing numeric codes, names, links, and IVR search. Its optional `managed/locations/legacy-weather-geometry.sqlite` sibling contains legacy CLC and NWS boundaries keyed by canonical ID and `(source, code)`.
- `runtime/state/location-overlay.sqlite` is service-owned mutable state for feed bindings and validated discoveries.
- `runtime/state/location-catalog-generations/` retains validated pack snapshots so a reload can be rolled back without changing managed files.

The service validates checksums, schema versions, source counts, coordinates, FTS, RTree, cross-database canonical IDs, and SQLite integrity before it publishes a new generation. Core and geometry members are retained in the same generation directory and swapped as one in-memory snapshot. A failed validation leaves the current generation active.

## IVR location ranking

- Regional SIP and Twilio lines hard-filter name, T9, and multitap candidates to the configured province before ranking.
- Nationwide searches use a bounded logarithmic population boost from Statistics Canada table 98-10-0002-01. Population changes candidate order but does not bypass lexical confidence or homonym safeguards.
- Ambiguous candidates are played one at a time. Press `2` to select the current location, `3` for the next match, or `1` to return to location search.
- `services.go.ivr.search.caller_hint_enabled` optionally reduces validated Twilio caller ID or SIP identity headers to an unambiguous Canadian province. The province is a soft prior only. Raw and normalized phone numbers are not retained, logged, persisted, or exposed in status and metrics.
- Area codes shared by multiple provinces or territories do not produce a caller hint. Mobile portability and spoofable SIP identity mean caller hints must never be treated as proof of location.

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
  --geometry-output dist/location-catalogs/ca-weather.geometry.sqlite \
  --member-manifest-output dist/location-catalogs/ca-weather.manifest.json \
  --retrieved-at 2026-08-02T00:00:00Z \
  --allow-downloads
```

The builder writes each database atomically, creates SHA-256 sidecars, and records both members in a deterministic manifest. Point geometry remains in the core pack for fast nearest-location queries. Polygon and MultiPolygon data is stored only in the geometry pack. The scheduled `Location Catalogs` workflow builds and validates `global-core`, `ca-weather`, and `us-weather`, then publishes installable, checksummed artifacts. Downloaded archives and generated SQLite packs are intentionally excluded from the repository.

Map callers request boundary geometry explicitly. `haze-location` resolves names and identifiers from the core first, reads only the selected WKB from the paired geometry database, converts it to GeoJSON for the existing map contract, and leaves ordinary IVR and search responses geometry-free.

Operator ZIP imports may replace a legacy geometry member only when every source-qualified geometry identity exists in the active paired core. A provider generation that introduces new identities is rejected until the core is rebuilt. Web imports are successful only after `haze-location` acknowledges the reload. A failed, unavailable, or timed-out reload restores the prior geometry file and asks the service to reload that restored generation.

Provider identifiers, source quality, record provenance, attribution, effective dates, and deployment history are retained. SWOB `AUTO`, `AUTO-minute`, `MAN`, and `MANNED` values are normalized while the provider value is preserved. The ECCC `iata_id` field is stored as an ECCC station identifier unless an authoritative airport crosswalk is present.

## Operator-supplied packs

Full Canadian postal addresses, official IATA and ICAO subscriptions, and licence-sensitive WMO or OSCAR data are not downloaded by the default builder. After validating the licence and schema, install an operator pack under `managed/locations/` and add it to `locations.xml` with the desired priority.
