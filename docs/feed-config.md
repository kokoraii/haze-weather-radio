# Feed config

The Haze daemon owns the managed feed config in `managed/feeds`. It is the only place feed coverage XML is parsed. Services read the daemon's compiled snapshot over the host event bridge, so every service applies the same coverage rules.

- Parser and rules: `crates/haze-feed-config`
- Daemon service: `crates/haze/src/feed_config.rs`
- Go package: `services/go/internal/feedconfig`
- Shared test cases, run by both Rust and Go: `contracts/feedconfig/v1/coverage-cases.json`

## Coverage syntax

Coverage lives under `<locations><coverage>` in each feed file.

| Config | Covers | SAME header for an affected alert |
| --- | --- | --- |
| `<region id="065500"/>` | `065500` and every `0655xx` | `065500` plus every affected `0655xx` |
| `<region id="065500">` with `<subregion>` children | the same | `065500` plus only the listed subregions that are affected |
| `<region id="0655*"/>` | the same | `065500` plus every affected `0655xx`, even if subregions are listed |
| `<region id="06551*"/>` | every `06551x` | every affected `06551x`, with no region code |
| `<subregion id="06551*"/>` under a region | that region | the region code plus every affected `06551x` |
| `<region id="073110"/>` | only `073110` | `073110` |
| `<region id="SKZ012" source="nws"/>` | only that code | unchanged; NWS codes are always exact |

Rules behind the table:

- An ECCC region code ending in `00` is a forecast region and covers its subregions. Any other code covers only itself.
- A wildcard needs two to five digits before the `*`. Only a four-digit wildcard adds a region code.
- An unlisted subregion of a region with a list still alerts, under the region code only. That matches how ECCC's Weatheradio transmitters built their headers.
- A listed subregion outside its region's codes adds that subregion to the feed.
- A SAME header holds at most 31 codes. Region codes come first, then listed subregions, then other affected codes, so the limit drops the least specific codes. Drops are logged.
- An alert that names no locations (automations, whole-coverage operator alerts) uses the feed's full header: every region code plus every listed subregion.
- A feed with no coverage takes alerts for every location.

## Validation

The daemon validates the whole directory. Any invalid feed, such as a malformed wildcard, an NWS wildcard, a region with no id, or a feed id defined twice, fails the load:

- At startup, Haze does not start and logs the file and value at fault.
- On a later change, the daemon keeps the last good snapshot, logs an error, and publishes `config.feeds.invalid`.

## Events

| Event | Direction | Data |
| --- | --- | --- |
| `config.feeds.request` | service to daemon | none; set `reply_to` to the service's client id, and optionally `subject` |
| `config.feeds.snapshot` | daemon to the requester (targeted) | the snapshot, echoing `subject` |
| `config.feeds.updated` | daemon broadcast | the new snapshot, after a valid change |
| `config.feeds.invalid` | daemon broadcast | `error` and the `generation` still in effect |

The daemon checks the feed files every 5 seconds. Services request the snapshot on every bridge connection and subscribe to `config.feeds.updated`.

The snapshot is JSON:

```json
{
  "version": 1,
  "generation": "1bd7ef737745e530",
  "feeds": [
    {
      "feed_id": "cwxr-sk01",
      "entries": [
        {
          "id": "065500",
          "source": "eccc",
          "name": "",
          "derive_forecast": "sk-37",
          "region_code": "065500",
          "matcher": { "kind": "prefix", "value": "0655" },
          "header": "listed",
          "subregions": [{ "kind": "exact", "value": "065512" }]
        }
      ]
    }
  ]
}
```

`version` changes only when a consumer could misread the format. `generation` changes whenever a feed file changes.

## Consumers

| Service | Uses the snapshot for |
| --- | --- |
| `haze-playout-rs` (CAP router) | Whether a feed takes an alert, and the SAME locations of toned alerts. Routing waits for the first snapshot. |
| `haze-playlist` | The SAME locations of alerts that name none of their own |
| `haze-web` | The SAME locations of operator alerts, per target feed |
| `haze-product-render` | Each feed's coverage regions, which replace the XML coverage after every config load |

Not yet migrated, and still reading coverage XML themselves: the web panel's feed display and editor, `haze-ivr`, and `haze-data-ingest`. Until they are, do not put `*` wildcards in a live feed's coverage, because those services do not understand them. Plain region and subregion codes are safe everywhere.
