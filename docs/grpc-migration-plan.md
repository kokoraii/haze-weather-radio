# gRPC migration plan

Status: Phases 0 and 1 are implemented in the working tree and not committed. Both are deployed to `la-servinor` with `control_transport: "http"` (see "Phase 0 results", "Phase 1 results", and "Phase 1 live deployment"). Phases 2 to 5 are proposals. Written 2026-10-25.

This plan moves Haze's internal service transports to gRPC in reversible phases while keeping the event-driven architecture, the alert semantics, and the live TeleWeather and CWRS instance intact. It was written from a read of the current code, so every number and port below should be re-checked when a phase starts.

## Summary

- Internal traffic is not HTTPS today. It is a loopback JSON-lines event bridge, a second bridge instance that carries binary PCM, and two small plaintext HTTP listeners. HTTPS exists only at the edges.
- The migration targets those internal transports. The browser edge, the Pi receiver, and every third-party API stay as they are.
- The daemon stays the broker. gRPC replaces the wire, not the topology.
- Every phase is dual-stack. The old transport keeps working until a phase has soaked on the live instance, and each service picks exactly one transport at a time.
- The honest benefit is typed contracts, generated clients, and explicit flow control. On loopback, do not expect a latency or CPU win. Phase 3 and later are gated on measurements for that reason.

## Scope

In scope:

| Transport | Where | Wire format today |
| --- | --- | --- |
| Event bridge | Daemon `HostBridge`, address exported as `HAZE_HOST_BRIDGE_ADDR` and passed as `--bridge` | One JSON document per line over loopback TCP |
| Media bridge | A second `HostBridge` instance, `HAZE_MEDIA_BRIDGE_ADDR` and `--media-bridge` | JSON handshake, then `HPCM` binary PCM frames (see `crates/haze-media-protocol`) |
| Log records | `log_record` messages on the event bridge (`HAZE_LOG_BRIDGE_ADDR` points at the same listener) | JSON lines, consumed by the daemon logger |
| `haze-media` control | `--listen` (127.0.0.1:8097 on the live instance) | Hand-written HTTP/1: `/health`, `/api/v1/feed/audio` (streamed body), `/api/v1/webrtc/offer` |

Out of scope, and why:

- `haze-web` HTTPS, WebSocket, and WebRTC signaling to browsers and to `hazeReceiver.py` on the Pi. Browsers cannot speak native gRPC, and the receiver is a separate deployment.
- `haze-ivr` HTTP (`--http-addr`, 127.0.0.1:8096 on the live instance). The Phase 0 audit found that it serves Twilio callbacks and TwiML (`/ivr/v1/twiml`, `/ivr/v1/twilio/*`) plus lookup, audio, and metrics routes. Twilio is the caller, so it stays HTTP.
- Outbound calls to ECCC, NWS, NAADS, EAS NET, Discord, Icecast, MistServer, and SpeakyAPI on i2jr. Those are other people's protocols.
- Account, session, and audit storage. Nothing in this plan changes persisted formats before Phase 4.

## Bridge semantics that must survive

The daemon bridge (`crates/haze/src/host_bridge.rs`) has behavior that clients depend on. A gRPC transport that drops any of these is a regression, so Phase 0 turns them into transport-independent tests.

- Envelope fields: `id`, `type`, `source`, `timestamp`, `subject`, `target`, `reply_to`, `data` (see `services/go/internal/events/events.go`).
- Registration through `bridge.client`: `receive_events`, `client_id`, `subscriptions`, `binary_pcm`.
- Subscription matching: exact type, `*` for everything, or a prefix ending in `*`. An empty list receives everything.
- Targeted delivery: an event with `target` goes only to the client whose `client_id` matches, and the daemon logs a warning when nobody does.
- An event is never echoed back to the connection that published it.
- Each client has a bounded outbound queue (256 messages). A client that falls behind is dropped, not waited for. The router must never block on a slow consumer.
- `cap.alert.received` replay: 15 minutes, up to 512 events, sent to a newly registered client that subscribes to that type.
- A 500 ms handshake grace for legacy clients that never send `bridge.client`.
- The daemon consumes only `service.control` and `automation.dispatch.accepted|completed|failed`. Everything else is routed to services.
- `log_record` messages are consumed by the daemon logger and not republished.
- PCM clients opt in with `binary_pcm`. Others get the same frame re-encoded as a base64 `playout.pcm` JSON event.

## Principles

These come from `AGENTS.md` and decide the design choices below.

1. Preserve the EDA model. Services keep publishing and consuming events through the broker. The only direct service-to-service calls that exist today are `haze-web` and `haze-ivr` calling `haze-media`, and they stay the only ones.
2. Keep latency-sensitive paths bounded and independent. Alert ingest, TTS, playout, IVR, CGEN, media output, and web updates must not be able to stall each other through the transport.
3. Do not catch up by speeding up. If a PCM consumer lags, it drops stale audio or reconnects. HTTP/2 flow control must never be what slows the producer.
4. Keep event contracts stable. During the migration the JSON `data` payload is carried verbatim. Typed schemas come later and only per event family.
5. Prefer small, reversible changes. One service per deploy, one phase per release.
6. Do not risk alert state. Phases are exercised with deliberate test or demo alerts, never by replaying real ones, and the alert path (`haze-cap`, `haze-playout`) is the last to move.

## Proto layout

```
contracts/
  location/v1/                existing JSON schemas, untouched
  bridge/v1/bridge.proto      event bridge service and envelope
  media/v1/pcm.proto          PCM frame and media bridge service
  media/v1/control.proto      haze-media control service
buf.yaml                      lint and breaking-change rules
```

Packages are `haze.bridge.v1` and `haze.media.v1`. A breaking change gets a new `v2` package that runs next to `v1`. It never edits `v1` in place.

### bridge.proto (abridged, see the file for comments)

```proto
service EventBridgeService {
  // One long-lived stream per client. The first request must be a Hello.
  rpc Connect(stream ConnectRequest) returns (stream ConnectResponse);
}

message ConnectRequest {
  oneof frame {
    Hello hello = 1;       // client_id, receive_events, subscriptions
    Envelope publish = 2;
    LogRecord log = 3;     // replaces log_record lines
  }
}

message ConnectResponse {
  oneof frame {
    Welcome welcome = 1;
    Envelope deliver = 2;
    Notice notice = 3;     // e.g. a slow-consumer notice before the stream closes
  }
}

message Envelope {
  string id = 1;
  string type = 2;
  string source = 3;
  string timestamp = 4;    // RFC 3339 text exactly as published
  string subject = 5;
  string target = 6;
  string reply_to = 7;
  bytes data_json = 8;     // exact JSON of the "data" object
  bytes extra_json = 9;    // JSON object of every other top-level member
}
```

Design notes:

- `data_json` and `extra_json` are bytes, not `google.protobuf.Struct`. `Struct` stores numbers as doubles, which loses precision on 64-bit values such as sequence counters and nanosecond timestamps. Carrying the JSON text keeps every existing producer and consumer byte-compatible. Typed messages are introduced per event family in Phase 4, with the JSON kept for families that have not moved.
- Events carry top-level members that are not part of the Go `Event` envelope: `delivery_id` on CAP alerts, `feed_id` and `queue_id` on PCM events, `header` and `feed_ids` on playout events. `extra_json` holds them so nothing is dropped. `services/go/internal/gen/contracts_test.go` proves the round trip for these shapes.
- The timestamp stays text because producers disagree on sub-second precision.
- Registration is explicit (`Hello`), so the legacy 500 ms grace is not needed for gRPC clients.
- The router in the daemon is untouched. Both transports feed the same internal queue, so a JSON-lines publisher and a gRPC subscriber see each other's events.
- The service and message names follow the buf `STANDARD` lint rules (`...Service` suffix, `ConnectRequest` and `ConnectResponse` for the streaming RPC). `PACKAGE_DIRECTORY_MATCH` is excepted so contracts stay grouped by domain next to the existing JSON contracts.

### pcm.proto and control.proto

- `PcmFrame` mirrors the current `HPCM` fields: `feed_id`, `queue_id`, `sample_rate`, `channels`, `duration_ms`, `sequence`, `pts_ns`, `discontinuity`, a `media_kind` enum (silence, routine, alert, operator break-in), and `pcm` bytes. The enum numbers are the `HPCM` kind plus one, because proto3 reserves zero for the unspecified value. `MediaBridgeService` has the same `Connect` stream shape as the event bridge. The maximum message size is set explicitly with headroom above the current 4 MiB frame limit, and the gRPC default is not relied on.
- `MediaControlService` has `Health` (unary), `StreamFeedAudio` (server stream, with the same feed and codec selection as the HTTP route), and `WebRtcOffer` (unary). The offer fields are copied from the current handler. It is not redesigned. HTTP statuses map to gRPC codes (404 to `NOT_FOUND`, 400 to `INVALID_ARGUMENT`, 415 to `UNIMPLEMENTED`, 503 to `UNAVAILABLE`, 429 to `RESOURCE_EXHAUSTED`).

## Runtime and dependency choices

- Rust: `tonic` 0.14 and `prost`, with default features off and only `codegen`, `router`, and `transport` on, so TLS is disabled. The links are loopback only, and leaving out `rustls` avoids pulling a C or assembly crypto build into the Windows `gnullvm` and FreeBSD builds.
- Go: `google.golang.org/grpc` and `google.golang.org/protobuf`. Both are pure Go, so the Windows `CGO_ENABLED=0` test runs are unaffected. The generators are not module dependencies. `buf.gen.yaml` and `scripts/gen-proto.*` run pinned versions through `go run module@version`, which keeps the large buf dependency tree out of `go.mod`.
- Daemon runtime: today the daemon bridge uses std threads and `mpsc` channels, while every service crate is already on tokio. The recommended approach is to run the gRPC server on a dedicated tokio runtime in its own thread and hand messages to the existing router through the existing channels. Rewriting the router on tokio is a larger change with no benefit and is not part of this plan.
- Transport: cleartext HTTP/2 over a loopback TCP port, bound to 127.0.0.1 only, with keepalive and explicit message size limits. Unix sockets are not used because the project targets Windows, Linux, and FreeBSD.
- Shared clients: before Phase 0 the repo had about a dozen Go and nine Rust dial sites that each hand-rolled connect, handshake, framing, and reconnect for the bridge. Phase 0 replaced them with one shared client per language (the Go package `internal/bridgeclient` and the Rust crate `haze-bridge-client`) over the JSON-lines transport. Selecting gRPC then happens in those two places.

## Phases

### Phase 0: groundwork, no behavior change (implemented)

1. Add the `.proto` files, `buf.yaml`, and the generation pipeline (see CI section). Nothing consumes them yet.
2. Write the shared bridge client for Go and Rust over the existing JSON-lines transport. Migrate the existing dial sites onto it one service at a time. This is valuable even if the project stops here, because it removes the duplicated handshake code and gives the later transport switch one place to live.
3. Write a transport-independent conformance suite covering every item in "Bridge semantics that must survive". Run it against the JSON-lines bridge first. The same suite later runs unchanged against gRPC.
4. Audit for out-of-tree bridge clients: scripts, tools, the Pi receiver, anything on the Samba share, and `haze-data-ingest`. Record the result in this document.
5. Audit `haze-ivr`'s HTTP surface and decide whether it joins Phase 1 or stays HTTP.

Exit: conformance suite green on JSON-lines, all in-repo services on the shared client, audit recorded. Rollback: revert the service commits. The transport is unchanged. See "Phase 0 results" for what is done and what is left.

### Phase 1: `haze-media` control plane (implemented, not yet soaked)

1. Add a `MediaControlService` gRPC listener to `haze-media` on a new loopback port configured next to `services.rust.media.listen`. The HTTP routes stay.
2. Add a config switch for `haze-web` to choose `http` or `grpc` for media control. The default stays `http`.
3. `haze-web` remains the HTTP and WebRTC edge for browsers. For `StreamFeedAudio` it reads the gRPC stream and writes the HTTP response, so public listen pages and the receiver see no change.
4. Soak on the live instance with `grpc` selected, then flip the default. Keep the HTTP routes for at least one more release.

Why first: it has two callers (`haze-web` and `haze-ivr`), no fan-out, no replay or routing semantics, and it is easy to switch back. Only `haze-web` moves in this phase. `haze-ivr` keeps calling the HTTP routes, which is one more reason they stay.

Exit: health, feed audio, and WebRTC offer work over gRPC on the live instance with no new audio discontinuities, and a rollback to `http` has been exercised once. Rollback: set the switch back to `http` and restart `haze-web`.

### Phase 2: event bridge, dual-stack

1. Add the `EventBridgeService` gRPC server to the daemon. It listens next to the JSON-lines listener and feeds the same router. Export its address as a new environment variable and `--bridge-grpc` argument. Do not repurpose `HAZE_HOST_BRIDGE_ADDR`.
2. Add a per-service transport setting in the shared client, defaulting to `jsonl`. A service uses exactly one transport. A service never publishes on both, because duplicate delivery of an alert-related event is worse than a missed experiment.
3. Move services one at a time, lowest risk first. Suggested order:
   1. `haze-webhook`
   2. `haze-asr`
   3. `haze-product-render`
   4. `haze-playlist`
   5. `haze-tts`
   6. `haze-ivr`
   7. `haze-location` (request and reply through `reply_to`, so it exercises targeted delivery)
   8. `haze-web` (banner hub, weather gateway, catalog import)
   9. `haze-cgen`
   10. `haze-cap`
   11. `haze-playout`
4. Soak each service on the live instance before the next. A soak should include at least one deliberate test alert end to end (archive, banner, audio, IVR, CGEN, webhook) and a daemon restart to confirm reconnect and CAP replay.
5. `log_record` moves with each service as part of its `ConnectRequest` log variant.

Exit: every service on gRPC, replay and targeted delivery verified on the live instance, per-transport counters in the status payload show zero unexpected drops. Rollback: set that service's transport back to `jsonl` and restart it. The daemon keeps both listeners, so no other service is affected.

### Phase 3: media bridge (PCM), gated

This phase is optional and only proceeds if measurements justify it.

1. Measure the current `HPCM` path on the live instance first: per-hop latency, CPU, drop counts, and discontinuity counts.
2. Prototype `MediaBridgeService.Connect` and compare. Proceed only if p99 delivery latency, CPU, and discontinuity counts are no worse.
3. Keep the drop policy. Each client has a bounded send queue in front of the gRPC stream. When it fills, the client is dropped or the stale frames are discarded. The router never waits on HTTP/2 flow control.
4. Move consumers first (`haze-media`, `haze-ivr`, `haze-cgen`, `haze-web`), then the producer (`haze-playout`), so there is always a consumer ready on the transport the producer picks.

Exit: the gate above is met and a day of live playout shows no new robotization, gaps, or catch-up. Rollback: switch the producer and consumers back to `HPCM`, which the daemon continues to serve.

### Phase 4: typed events, optional

1. Define typed messages per event family, starting with the alert lifecycle (`cap.alert.received`, updates and cancellations, banner events) because consistent metadata across archive, banner, audio, IVR, CGEN, webhooks, and playout is a stated project requirement.
2. Publishers send the typed message and also fill `data_json` for the same event until all consumers have moved. Consumers prefer the typed field.
3. Make `id` required for alert events so consumers can deduplicate.

Exit: archive records and webhook payloads are byte-identical before and after for the same alert. Rollback: consumers fall back to `data_json`.

### Phase 5: retire the legacy transports

1. Confirm the Phase 0 audit and a full release cycle with no JSON-lines or HTTP traffic from any client (the daemon counters from Phase 2 give this).
2. Remove the JSON-lines listener, the legacy handshake grace, the HTTP media routes, and the base64 PCM fallback.
3. Update `AGENTS.md`, `README`, and the docs to describe the gRPC transports.

This phase is deliberately last and is the only one with no cheap rollback, so it ships alone. Keep the previous release's bundle available until it has run on the live instance.

## Phase 0 results

Recorded 2026-10-25. Nothing here is committed. It was deployed together with Phase 1 (see "Phase 1 live deployment").

### What was added

- Contracts: `contracts/bridge/v1/bridge.proto`, `contracts/media/v1/pcm.proto`, `contracts/media/v1/control.proto`, linted by `buf.yaml` (`STANDARD` lint, `FILE` breaking rules).
- Generation: `buf.gen.yaml`, `scripts/gen-proto.sh`, and `scripts/gen-proto.ps1` run buf v1.50.0, `protoc-gen-go` v1.36.5, and `protoc-gen-go-grpc` v1.5.1 through `go run`. The generated Go is committed under `services/go/internal/gen`. The only new Go module dependencies are `google.golang.org/grpc` v1.70.0 and `google.golang.org/protobuf` v1.36.5, and only the generated code uses them so far.
- CI: a `proto` job in `.github/workflows/build.yml` runs `scripts/gen-proto.sh --check` (lint plus a stale-generation check) and, on pull requests, the breaking-change check against the base branch.
- Rust code generation was added in Phase 1 (see "Phase 1 results").
- Shared clients: the Go package `services/go/internal/bridgeclient` and the Rust crate `crates/haze-bridge-client`. Both keep the existing wire format: newline-delimited JSON, an optional `bridge.client` handshake, a bounded line reader, and serialized writes. The Rust crate also reads the media bridge's compact `HPCM` frames.
- Conformance suite: `crates/haze/src/host_bridge/conformance.rs`. Twelve scenarios drive the real daemon bridge over loopback sockets through two small traits, `Transport` and `Client`. A gRPC transport implements the same traits and `conformance_suite!` runs every scenario against it. Two deliberate breakages of the broker (allowing echo to the publisher, keeping a client whose queue is full) were confirmed to fail the suite.
- Contract tests: `services/go/internal/gen/contracts_test.go` proves the `Envelope` carries every member of representative JSON events, keeps 64-bit integers exact, and keeps the `MediaKind` numbers in step with `HPCM`.
- Round-trip tests added where none existed: the CAP durable publish and router acknowledgement path, the location query client, the playout consumer and publish-only connections, the `haze-media` status and PCM loops (registration, binary frames, legacy JSON events, oversized and malformed input), and the `haze-tts` message loop (queued jobs, byte order mark, shutdown, clean close, cancellation, full queue). The `haze-media` and `haze-tts` loop tests were also run against the pre-migration code and passed, which is how their behavior was confirmed unchanged.

### Services on the shared client

| Language | Services |
| --- | --- |
| Go | `haze-webhook`, `haze-asr`, `haze-product-render`, `haze-playlist`, `haze-tts` (dial, message loop, and publishers), `haze-ivr` (event and media connections), `haze-web` (weather gateway client, media hub, location catalog import), `locationclient`, `events.HostBridgePublisher` (used by `haze-data-ingest`) |
| Rust | `haze-cap` (durable publisher), `haze-location` (service loop and query client), `haze-playout` (event, consumer, and media publish-only connections), `haze-netalert` (formerly `haze-easnet`, now also the banner state), `haze-cgen` (event bridge and media consumer), `haze-media` (status publisher and PCM consumer) |

No in-repo bridge client is left. The only remaining `TcpStream::connect` and `net.Dial` calls in the services are for other protocols (NAADS, EAS NET peers, Icecast, and HTTP transports).

`haze-media` is the one client that reads `HPCM` binary frames. The Rust shared reader handles them through `BridgeReader::next_payload`, which returns either a bounded JSON line or one whole PCM frame. That is the piece Phase 3 would otherwise have had to add.

### Audit

Out-of-tree bridge clients: none found. The repository scripts, `hazeReceiver.py`, and the docs do not use the bridge. On the live host (one `ss` sample on 2026-10-25), the event bridge had these clients: `haze-asr`, `haze-cap-ingest`, `haze-data-ingest`, `haze-easnet`, `haze-ivr`, `haze-location`, `haze-media`, `haze-playlist`, `haze-playout-rs`, `haze-product-render`, `haze-tts`, `haze-webhook`, and two from `haze-web`. The media bridge had `haze-media` and 76 connections from `haze-playout-rs`. `haze-cgen` was not connected. Short-lived connections that `haze-web` opens per request were not sampled, and a machine outside the live host could still run a client.

`haze-data-ingest`: it is started without `--bridge` but publishes through `events.HostBridgePublisher`, which takes the address from `HAZE_HOST_BRIDGE_ADDR`.

`haze-ivr` HTTP: Twilio-facing, so it stays HTTP (see Scope).

### Findings that change later phases

- Registration differs by language. The Go services connect without a `bridge.client` handshake, so the broker treats them as legacy clients: they receive every event after the 500 ms grace period or their first publish, and filter on their side. The Rust services register with subscription lists. The shared Go client keeps the unregistered default so Phase 0 changes no behavior. In Phase 2, move each Go service to a registered subscription list derived from its handlers, one service at a time.
- The envelope has more than the Go `Event` fields. This is why `Envelope.extra_json` exists.
- CAP replay can lose events. The replay buffer holds up to 512 alerts, but each client queue holds 256, and the replay loop ignores a full queue. A late subscriber could miss replayed alerts when more than about 256 are retained. The conformance suite does not assert the 512 cap for that reason. The gRPC replay should send in order with backpressure instead of reproducing this.
- `haze-playout-rs` holds 76 publish-only connections to the media bridge on the live instance. Understand why (per feed or per stream) before Phase 3.

### Behavior differences in the migrated clients

These are small and were accepted for Phase 0, but they are real:

- Rust clients now bound inbound lines (4 MiB by default, 8 MiB for playout, 16 MiB for location, 2 MiB for CGEN) and skip malformed lines. `haze-easnet` used to fail on a malformed line.
- Every Rust connect now has a 5 second timeout.
- Go clients skip a JSON `null` line instead of passing a nil event to the service.
- `playlist`, `ivr`, and the web weather client no longer hold their request-correlation mutex while writing to the socket.
- The playout and `haze-media` registration handshakes no longer carry a timestamp, because registration now goes through the shared `register` call. They now always include a `subscriptions` list, empty when the client takes everything, which the broker treats the same as a missing one.
- `haze-media` now discards an oversized (over 256 KiB) legacy JSON line while reading it. It used to read the whole line first and then drop it, and its log line no longer includes the byte count.
- `haze-tts` writes are now serialized by the shared connection as well as by its existing publish lock.

### Verification

See the end of this document for what was run. Nothing was exercised on the live instance.

## Dual-stack cutover rules

- Daemon first, services second. A daemon with both listeners is backward compatible with every old service. Upgrade the daemon in a release that changes no service.
- Mixed versions are expected. A new service with `jsonl` selected must work against the old and new daemon. An old service must work against the new daemon.
- One transport per service at a time, selected by config and applied by restarting only that service. The daemon already supervises services with restart backoff, so confirm the exact restart mechanism (`service.control` or the daemon settings file) when Phase 2 starts. A restart of the whole systemd unit interrupts both TeleWeather and CWRS output and should be the exception.
- No dual publishing in production. Shadow comparison happens in tests only.
- Ordering is guaranteed within one publisher stream, as it is today. There is no global ordering across transports, which also matches today's behavior across separate TCP clients.
- Before rolling the daemon back to an older build, set every service to `jsonl`. An older daemon has no gRPC listener.

## Observability

Add these to the status payloads the daemon and services already publish, and make sure none of them can carry secrets:

- Per transport: connected clients, events published and delivered, slow-client drops, reconnects, and last error.
- Per PCM client: frames sent, frames dropped, and discontinuities.
- gRPC server: active streams and the last rejected frame (oversize, malformed).

## CI and protoc setup

Goal: no new native build dependency on Windows `CLANG64` or FreeBSD, and no drift between `.proto` files and generated code.

Recommended approach:

- Rust (done in Phase 1): `crates/haze-contracts` compiles the `.proto` files in `build.rs` with the pure-Rust `protox` compiler and feeds the descriptor set to `tonic-prost-build`. This keeps `protoc` off the native Windows and FreeBSD jobs. It uses tonic 0.14, whose generator is split into `tonic-prost-build`.
- Go (done in Phase 0): the generated `*.pb.go` and `*_grpc.pb.go` files are committed. The `GOFLAGS=-mod=readonly` setting and the FreeBSD VM job then need no generator. Generator versions are pinned in `buf.gen.yaml` and `scripts/gen-proto.*` and run through `go run`.
- A `proto` job on `ubuntu-latest` (done in Phase 0) runs `buf lint`, a regeneration check that fails when the committed code is stale, and on pull requests `buf breaking` against the base branch, skipping it when the base branch has no `buf.yaml` yet.
- `scripts/gen-proto.sh` and `scripts/gen-proto.ps1` regenerate locally. `--check` / `-Check` fails if the generated code changed, and `--breaking-against REF` / `-BreakingAgainst REF` compares wire compatibility.
- Commit the updated `services/go/go.sum` and `Cargo.lock`, because CI uses `-mod=readonly` and `--locked`.

If `protox` proves unsuitable, the fallback is a system `protoc`:

- Linux: `protobuf-compiler` in the existing `apt-get install` lines.
- Windows: `mingw-w64-clang-x86_64-protobuf` added to both the `test` job and the `windows-bundle` job, which must stay on the same CLANG64 package set. `C:\msys64\clang64\bin` and `C:\msys64\usr\bin` stay on `PATH`, and `PKG_CONFIG_LIBDIR` stays unset.
- FreeBSD: `protobuf` added to the `pkg install` line.

Bundles:

- `tonic` and `grpc-go` add no shared libraries, so the Windows, Linux, and FreeBSD portable bundles should need no new runtime DLLs or `.so` files. Verify this with the existing build-script checks on the first Phase 1 build.
- Expect longer cold Rust builds. `Swatinem/rust-cache` already covers warm runs.

## Rollback summary

| Phase | Rollback | Needs whole-unit restart? |
| --- | --- | --- |
| 0 | Revert commits | No |
| 1 | Set media control switch to `http`, restart `haze-web` | No |
| 2 | Set the service's transport to `jsonl`, restart that service | No |
| 3 | Switch PCM producer and consumers back to `HPCM` | Per service |
| 4 | Consumers fall back to `data_json` | No |
| 5 | Redeploy the previous release's bundle | Yes |

On the live host, keep the existing convention of a timestamped `.bak` copy of each binary before replacing it, and never touch `.env`, logs, runtime state, or managed operator files while switching transports.

## Risks and open questions

- Value: on loopback, gRPC is mostly a contract and tooling upgrade. If that is not worth the cost, the plan can stop after Phase 0 and Phase 1 and still leave the code in better shape (shared clients, a conformance suite, a typed media control API).
- HTTP/2 flow control can stall a producer that the current drop-the-slow-client design would have protected. Phase 3 depends on getting the bounded-queue-then-drop behavior right.
- Embedding tokio in the daemon adds a second concurrency model next to its std threads. The thread-plus-channel handoff in "Runtime and dependency choices" is meant to keep that boundary small.
- Out-of-tree bridge clients: none were found in the repository or on the live host, but the live check was one sample. Phase 5 must still confirm with the Phase 2 transport counters before removing the JSON-lines listener.
- Message size: the current 4 MiB PCM frame limit equals the usual 4 MiB gRPC default receive limit, and protobuf framing adds bytes on top, so a maximum-size frame would be rejected under the default. The plan sets the limit explicitly, with headroom, so neither that nor a library default change can silently alter it.
- Authentication: the loopback bridge has no authentication today and this plan does not add any. If bridge clients ever run on other hosts, that needs mutual TLS and a separate design.
- Estimated size, relative: Phase 0 is large because it touches every service, Phase 1 is small, Phase 2 is large but mechanical once the shared client exists, Phase 3 is medium and may be dropped, Phase 4 is open-ended, Phase 5 is small.

## Phase 0 verification

What was run on 2026-10-25, and where:

- Windows development machine: `go vet ./...` and `go test ./...` with `CGO_ENABLED=0`, all passing. `cargo test` for `haze-bridge-client` (18), `haze` (92, including the 12 conformance scenarios), `haze-cap`, `haze-location`, `haze-playout`, and `haze-media` (46), all passing. `scripts/gen-proto.ps1` lints, regenerates, and produces byte-identical Go on a second run.
- Linux (scratch copies on `la-servinor`, built at lowest priority and removed afterwards): `cargo test --locked --no-fail-fast` for `haze-cap` (18 and 1), `haze-location` (37 and 2), `haze-easnet` (7), `haze-playout` (99), `haze` (92), and `haze-cgen` (281), all passing. After the last two migrations, `haze-bridge-client` (18) and `haze-media` (46, and 54 with the `gstreamer-backend` feature that production builds use) passed there too, and `haze-playout` still compiles against the changed `haze-media`. `haze-easnet` and `haze-cgen` cannot be built on the Windows machine because it has no NASM or `pkg-config`, so Linux is the only place they were compiled. That run caught one real borrow error in the `haze-cgen` migration, since fixed.
- Mutation checks: two deliberate breakages of the daemon bridge failed the conformance suite as expected, and the bridge was restored.
- Characterization: the new `haze-media` and `haze-tts` loop tests were also run against copies of the pre-migration code and passed, so the migrated loops behave the same on those scenarios.

Not run:

- The `proto` CI job itself, the Windows `CLANG64` job, and the FreeBSD job. `buf breaking` has no base branch to compare against yet.
- Anything on the live instance at the time. The Phase 0 client migrations reached `la-servinor` together with Phase 1 (see "Phase 1 live deployment").

## Phase 1 results

Recorded 2026-10-25. Nothing here is committed. The deployment is described under "Phase 1 live deployment".

### What was added

- `crates/haze-contracts`: compiles all three contracts at build time with `protox` and `tonic-prost-build`, and exposes the generated types and stubs as `haze_contracts::bridge::v1` and `haze_contracts::media::v1`. Both languages generate from the same `.proto` files. The Rust output is not committed.
- `haze-media`: a `MediaControlService` gRPC listener in `crates/haze-media/src/grpc.rs`, started when `--grpc-listen` or `services.rust.media.grpc_addr` is set. It runs next to the HTTP listener and shares its code, so the two cannot drift. Audio streaming goes through an `AudioOut` sink (HTTP writes a response head and raw bytes, gRPC sends messages) and WebRTC offers go through one `prepare_webrtc_offer` function. The HTTP responses are byte-for-byte what they were.
- Daemon: `services.rust.media.grpc_addr` is passed to `haze-media` as `--grpc-listen`.
- `haze-web`: `services.rust.media.control_transport` (`http` by default, or `grpc`) picks how `haze-web` calls `haze-media`. `services/go/internal/webgateway/media_control.go` defines a `mediaControl` interface with an HTTP and a gRPC implementation. Every `haze-web` use goes through it: the health document, the WebRTC offer for the web panel, banner, and receiver, the audio proxy for public listeners, and the media hub's paced PCM source. The gRPC client is created once per address, reconnects within about two seconds, and does not fall back to HTTP on its own.
- `config.yaml` sets `grpc_addr: "127.0.0.1:8098"` and `control_transport: "http"`. Deploying therefore opens one new loopback port and changes nothing else until the switch is flipped. Port 8098 was free on the live host.

### Behavior notes

- Statuses map between the two transports as the proto comments say: 400 is `INVALID_ARGUMENT`, 404 is `NOT_FOUND`, 415 is `UNIMPLEMENTED`, 429 is `RESOURCE_EXHAUSTED`, and 503 is `UNAVAILABLE`. `haze-web` maps them back, so a refused WebRTC offer is final or falls back exactly as it did over HTTP. A transport failure is `UNAVAILABLE`, which also falls back.
- An audio stream is one gRPC message per 20 ms frame. The content type travels on the first message. The writer waits when a listener is slow, as it did over HTTP, so the broadcast channel still notices lag and resynchronizes with silence instead of queueing stale audio.
- A refusal that happens after a stream is opened (unknown feed, unsupported codec, missing encoder) arrives as the stream's first error.
- A gRPC listener that cannot bind logs an error and does not stop `haze-media`, because the HTTP routes carry the live audio. The listener has no authentication, so it logs a warning if it is bound to a non-loopback address.
- `haze-ivr` still calls `/api/v1/feed/audio` over HTTP, so that route stays whatever `control_transport` says.
- Health counts gRPC audio listeners in `http_client_count`, because they share the registry. The two transports are not distinguished there yet.

### Verification

- Rust: `haze-media` runs 4 new HTTP route tests (also run against the original pre-change code, where they pass), 6 new gRPC tests through a real tonic client, and the earlier bridge tests. The daemon has a new test for the `--grpc-listen` argument.
- Go: 13 new tests, most with an in-process gRPC server, cover health, offers and their status mapping, audio streaming and cancellation, the audio proxy, transport selection, and the media hub's PCM source over gRPC. The full Go suite passes.
- Cross-language: `TestMediaControlTransportsAgreeOnARealHazeMedia` starts the real `haze-media` binary and compares the HTTP and gRPC clients: health, the WAV and raw audio streams (content type, header, silence frame, paced frames), refusals, offers, and release of listeners. It is skipped unless `HAZE_MEDIA_BIN` is set. With a `--features gstreamer-backend` binary and `HAZE_MEDIA_BACKEND=auto` it also compares encoded audio and WebRTC refusals, and answers a real offer built by the pion WebRTC stack, including the refusal of a duplicate listener.
- Linux (a scratch copy on `la-servinor`, built at lowest priority and removed afterwards): `haze-media` passes 56 tests, and 64 with `gstreamer-backend`, which production builds use. `haze-contracts` and the shared client build with `--locked`, and the daemon argument tests pass. The cross-language test passes against the GStreamer build: Opus and AAC streams, WebRTC refusals, the pion browser offer answered the same way over both transports, and listeners released promptly after the streams closed. The same test passes on Windows against the default build.
- Local Windows: the full Go suite and the Rust tests for `haze-contracts`, the shared client, `haze-media` (56), `haze` (93), `haze-cap`, `haze-location`, and `haze-playout` pass.
- Not run: a soak on the live instance with `control_transport: "grpc"`, FreeBSD, the Windows `CLANG64` job, and a real browser or receiver listening through the gRPC path. The workspace already needs Rust 1.92 for the GStreamer crates, so tonic's 1.88 adds no new toolchain requirement.

### Phase 1 live deployment

Recorded 2026-10-25 on `la-servinor`. The first deployment kept `control_transport: "http"`. It was then flipped to `"grpc"` on the live `config.yaml` only (see "Flip to gRPC on the live instance"); the repository default is still `"http"`.

- Rebuilt and installed 13 binaries in `/home/rai/haze-weather-radio/bin`: `haze-media` (with `gstreamer-backend`), `haze-web`, `haze-tts`, `haze-webhook`, `haze-asr`, `haze-ivr`, `haze-playlist`, `haze-product-render`, `haze-location`, `haze-cap-ingest`, `haze-easnet`, `haze-playout-rs`, and `haze-data-ingest`. The daemon `haze` and `haze-cgen` were not rebuilt, since this change does not touch what they run. Builds ran at low priority inside a 4 GB memory cap, and the service stayed up throughout.
- Added only the two new media keys to the live `config.yaml`. `.env`, logs, runtime state, and managed operator files were not touched.
- Every replaced file has a `.bak-grpc-phase01-20261025-133255` copy next to it.
- After the restart: the service is active with no restarts, `haze-media` logs `gRPC control listening` on `127.0.0.1:8098`, the HTTP health route and the public listener answer, and all child services are running. The startup `target is unavailable` warnings stopped about 20 seconds after start, and `playout tick lagged` warnings stopped about two minutes after start (they were already present while the builds were running).
- A read-only probe of the live listener returned the same health document as the HTTP route (same keys, same `backend`, `capabilities`, and `audio_processing`), and an unknown feed was refused on both transports (HTTP 404, gRPC `NOT_FOUND`).
- Rollback: restore the `.bak-grpc-phase01-*` files and restart, or without any restore set `grpc_addr` blank to close the new listener. `control_transport` already selects HTTP.

### Flip to gRPC on the live instance

Recorded 2026-10-25 on `la-servinor`.

- `services.rust.media.control_transport` is `"grpc"` in the live `config.yaml` (backup `config.yaml.bak-grpc-flip-20261025-133803`). Only `haze-web` was restarted: the daemon supervisor restarted it within about a second after a `SIGTERM`, and the rest of the service kept running with no service restart.
- After the flip `haze-web` holds one connection to `haze-media` on 8098 and none on 8097. The web panel, the public listener, an Opus listener, and a WebRTC peer all kept working, with no lagged frames on the Opus listener.
- A defect showed up in the first minutes: every listener that hung up logged `haze-media HTTP audio proxy ended: rpc error: code = Canceled`. The HTTP path never logged that line, because the proxy ignores `context.Canceled` and a gRPC status does not match it. `grpcAudioBody.Read` now returns the stream's context error when the stream context is done, and `TestGRPCMediaControlListenerHangUpReadsAsContextCanceled` covers it (it failed with the exact live message before the change). The fixed `haze-web` was built on the host with `-tags=opus_cgo` like the previous one and installed with a `haze-web.bak-grpc-audiofix-*` backup. A 3 second public listener afterwards streamed WAV at real-time rate, left without a log line, and was released.
- Rollback: set `control_transport` back to `"http"` (or restore the flip backup) and send `haze-web` a `SIGTERM`. The HTTP routes were never removed.

### Still to do for Phase 1

- Soak with `control_transport: "grpc"` with a real listener, a WebRTC listener, and a `haze-media` restart.
- Compare audio health counters (late ticks, concealed frames, dropped input chunks) between the two transports under real load.
- Flip the default only after the soak, and keep the HTTP routes for at least one more release.

