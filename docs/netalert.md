# Network alerts (`haze-netalert`)

`haze-netalert` is a Rust service under the daemon (`svc:netalert`). It does three jobs:

- Serves a WebSocket where clients subscribe with an interest list and receive matching alerts as CAP JSON and/or CAP XML, with optional Ogg/Opus audio.
- Tracks which alert is on air on each feed and publishes `banner.state.updated` for CGEN. The banner page (`/banner`) gets the same state over the WebSocket.
- Hosts the legacy DASDEC-style EAS NET bridge, formerly `haze-easnet`. It is off by default.

Like every Haze service, it only talks to the rest of Haze through the host event bridge.

## Configuration

`config.yaml`, under `services.rust.netalert`:

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | | Runs the service. The banner page needs it. |
| `addr` | `0.0.0.0:41802` | WebSocket listener. The path is `/v1/netalert`. |
| `max_clients` | `512` | Connections beyond this are refused with HTTP 503. |
| `easnet.enabled` | `false` | Starts the EAS NET bridge from `easnet.config` (`managed/configs/easnet.xml`). The XML `enabled` attribute must also allow it. |

The web panel relays the same socket at `/api/v1/netalert` (admin surface, used by `banner.html`) and `/api/public/v1/netalert` (public surface). Either path works over the panel's own TLS, so clients do not need port 41802 opened.

The listener is open: there is no authentication, because every alert it carries is public information. Restrict it with the bind address or a firewall if needed.

## Protocol

Text frames are JSON objects tagged by `type`. Binary frames carry Ogg/Opus audio. The server pings every 30 seconds. A client that falls far enough behind to fill its queue is disconnected with close code 1013 (try again later), so one slow client never stalls the others.

### Connecting

On connect the server sends:

```json
{"type": "hello", "protocol": 1, "server": "haze-netalert",
 "audio": {"codec": "opus", "container": "ogg", "sample_rate": 48000}}
```

### Subscribing

The client then sends one `subscribe` message. Sending another one replaces the subscription. Every field is optional.

```json
{
  "type": "subscribe",
  "interest": {
    "locations": {"sgc": ["4706027"], "same": ["072300"], "ugc": ["SKZ001"], "fips": ["48201"]},
    "area": {"lat": 50.45, "lon": -104.61, "radius_km": 25},
    "status": ["Actual"],
    "msg_type": ["Alert", "Update", "Cancel"],
    "category": ["Met"],
    "event": ["thunderstorm", "SVR"],
    "urgency": ["Immediate", "Expected"],
    "severity": ["Extreme", "Severe"],
    "certainty": ["Observed", "Likely"],
    "feeds": ["cwxr-ab01"]
  },
  "formats": ["json", "cap"],
  "audio": {"enabled": true, "language": "fr", "timezone": "America/Regina"},
  "banner": {"feed": "*"}
}
```

The server replies with `subscribed`, which echoes the normalized interest so the defaults are visible. An invalid request gets `{"type": "error", "code": "bad_request", "message": "..."}` and the previous subscription stays in place.

### How matching works

- **All criteria combine.** Every criterion narrows the match, and an empty criterion matches anything.
- **Status defaults to Actual.** `status` defaults to `["Actual"]`, so tests, exercises, and NAADS heartbeats only arrive if you ask for them.
- **CAP enum values are checked.** `status`, `msg_type`, `category`, `urgency`, `severity`, and `certainty` take CAP 1.2 values, case-insensitive. Unknown values are rejected.
- **Event names match exactly.** `event` matches the CAP `<event>` text, any `<eventCode>` value (CAP-CP or SAME), and the SAME event Haze resolved for a feed. Matching is exact and ignores case.
- **Location codes:**
  - `sgc`: Statistics Canada codes, 2, 4, or 7 digits. They match in either direction, so a subscribed province catches its subdivisions and a province-wide alert reaches a subscribed subdivision.
  - `same`: 6 digits, exact match. It matches NWS `SAME` geocodes, ECCC CLC geocodes (Canada's SAME location codes), and the SAME locations a feed encoded.
  - `ugc`: NWS zone or county codes such as `SKZ001` or `OHC035`, exact match.
  - `fips`: 5-digit county codes, exact match. They come from NWS `FIPS6` and `SAME` geocodes, and a SAME `SS000` code covers the whole state. ECCC CLC codes are never treated as FIPS.
- **Radius area.** `area` matches when the circle overlaps a CAP `<polygon>` or `<circle>` in a zone area. ECCC threat areas are ignored, because zones decide where an alert is in effect. The radius is at most 1000 km. Alerts with no geometry never match a radius.
- **Location codes and the radius are alternatives.** Both describe where the client is, so either one matching is enough.
- **Follow-ups.** An Update or Cancel that references an alert the client already received is delivered even if it no longer matches the area or severity filters. It is flagged `"follow_up": true` and still respects `status` and `msg_type`.

### How alerts are delivered

- **Without `feeds`:** alerts are delivered as soon as CAP ingest receives them (`"delivery": "cap"`). Duplicates of the same identifier are sent once.
- **With `feeds`:** alerts are delivered when they air on one of those feeds (`"delivery": "feed"`, from `cap.alert.audio.ready`). This includes manual and EAS NET alerts that have no CAP. Each airing is delivered once. If the feed drops an alert, clients that received it get `{"type": "alert.cancelled", "feed_id", "alert_ids"}`.

An alert message looks like this:

```json
{"type": "alert", "delivery": "cap", "alert_id": "urn:oid:...", "status": "Actual",
 "msg_type": "Alert", "sent": "2026-10-26T14:00:00-06:00", "follow_up": false,
 "cap": {...}, "cap_xml": "<?xml ...", "audio": {"stream": 12, "status": "pending"}}
```

These fields depend on the subscription:

- `cap` is the CAP alert as JSON (the `haze-cap` model). It is included with the `json` format.
- `cap_xml` is the original CAP document. It is included with the `cap` format.
- `packet` is the Haze alert packet, minus local file paths and service metadata. It appears on feed deliveries with the `json` format.
- `audio` is present only when the subscription asked for audio.

### Audio

With `"audio": {"enabled": true}`, every alert message carries `audio.stream`, and the server later sends:

1. `{"type": "audio.start", "stream": 12, "alert_id", "feed_id"?, "language"?, "codec": "opus", "container": "ogg", "sample_rate": 48000, "channels": 1, "duration_ms"}`
2. One or more binary frames. Each holds whole Ogg pages, and the Ogg bitstream serial number equals `stream`, so overlapping streams can be separated by serial alone. Concatenating them gives a playable `.ogg` file.
3. `{"type": "audio.end", "stream": 12}`. If the audio could not be produced, the server sends `{"type": "audio.failed", "stream": 12, "error": "audio unavailable"}` instead.

Where the audio comes from:

- **Feed deliveries** stream the audio exactly as it aired on that feed, including any SAME header, attention tone, and EOM.
- **CAP deliveries** stream generated speech. Product render writes the spoken text, using the same phrasing as playout (`alert.speech.request`). `haze-tts` voices it at 48 kHz mono.
  - It is voice only, with no SAME tones.
  - `language` picks the CAP info block by exact or base language (for example `fr` matches `fr-CA`), falling back to English.
  - `timezone` sets spoken times. It defaults to `services.go.tts.timezone`.
  - Each alert, language, and timezone is rendered once and shared by every client that asks.

Audio is sent as fast as the client reads it, not paced to real time.

### Banner state

`"banner": {"feed": "<feed id>"}` (or `"*"` for every feed) streams `{"type": "banner", ...}` messages with the same payload CGEN receives in `banner.state.updated`:

- The fields are `active`, `signature`, `feed_id`, `feed_name`, `generated_at`, `primary_color`, `primary_gradient`, and `alerts`.
- The current state is sent right after subscribing, then again on every change.
- Each entry in `alerts` is the banner card product render attached to the alert packet (`presentation.banner`). Alerts with no card, such as operator break-ins, get a minimal one.

## Bridge events

| Direction | Event | Use |
| --- | --- | --- |
| In | `cap.alert.received` | CAP deliveries, and the CAP attached to feed deliveries |
| In | `cap.alert.audio.ready` | Feed deliveries and their audio. Also outbound EAS NET when enabled |
| In | `cap.alert.cancelled` | `alert.cancelled` for feed subscribers |
| In | `alert.playout.started`, `alert.playout.completed`, `playout.interrupted` | On-air banner state |
| Out | `banner.state.updated` | Banner state for CGEN, published when a feed changes |
| Out | `alert.speech.request` / in `alert.speech.completed`, `alert.speech.failed` | Spoken CAP text from product render |
| Out | `tts.synthesize` / in `tts.synthesized`, `tts.failed` | Generated alert audio |
| Out | `netalert.status` | Startup status |
| Out | `easnet.*`, `cap.alert.broadcast.requested` | EAS NET, unchanged from `haze-easnet` and still published with source `haze-easnet` |
