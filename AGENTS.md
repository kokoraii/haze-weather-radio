# AGENTS.md
for the Haze Weather Radio project
Written by @meowraii, June 2026.
For more information, please check out my [GitHub profile](https://github.com/meowraii).
<hr>
Haze Weather Radio is a unified, weather information and alert aggregator designed to be easy to deploy in case of situations where common infrastructure would be damaged or limited during the event of an emergency or adverse weather conditions.

Its structure is defined as an Event-Driven Architecture (EDA), a highly asynchronous execution loop where components/modules communicate by producing and consuming events via the use of an event broker. This allows for high-throughput processing and I/O, thus achieving near real-time processing and communication of public safety information. It is very much adamant that you adhere to this architecture as strictly as possible.

## Development rules

- Preserve the EDA model. Services should publish and consume events instead of directly reaching across process boundaries or bypassing the broker.
- Keep latency-sensitive work asynchronous and bounded. Alert ingest, TTS, playout, IVR, CGEN, media output, and web updates must not block each other.
- Prefer small, reversible changes. This project has many live runtime paths, so avoid broad rewrites unless the task explicitly calls for one.
- Do not commit generated runtime data, large voice models, downloaded model weights, local bundles, `dist`, logs, secrets, or machine-specific artifacts.
- Do not stash or discard user changes unless explicitly asked. The worktree is often dirty during active development.
- Do not commit unless explicitly asked, or unless the user has clearly requested committing the current completed work.
- Do not use em dashes in docs, UI copy, logs, comments, or final user-facing text. Use commas, periods, parentheses, or a normal hyphen instead.

## Configuration layout

- `config.yaml` is for top-level service toggles, service executable paths, ports, and global defaults.
- Feed-specific and operator-managed configuration belongs in XML under `bundle/managed/configs/`.
- Runtime bundles should contain their own `audio`, `managed`, and `webroot` directories inside the portable bundle directory, not copied into the project root.
- Environment variables may be referenced by configs, but secrets must never be expanded into web-panel responses, logs, status payloads, or generated files committed to the repo.
- If `.env` is missing, Haze should warn and create it from `.env.example` when that path is supported by the runtime.

## Service boundaries

- Rust services live under `crates/` and should be managed by the daemon like the rest of Haze.
- Go services live under `services/go/` and should keep existing event contracts stable unless a coordinated migration is being implemented.
- The web panel is responsible for UI, authenticated control actions, and status display. It should not become the media, CAP, TTS, or alert playout engine.
- Playout remains the authoritative routine/priority feed mixer unless a task explicitly migrates that responsibility.
- Media output services should consume final feed PCM/events and provide sinks such as WebRTC, HTTP audio, UDP/RTP/RTMP/SRT, or device output without changing alert semantics.
- CGEN should consume canonical banner/alert events and render broadcast graphics without reinventing alert text, color, priority, or ordering logic.

## Alerts and public safety behavior

- Alert relay decisions must respect configured feed filters, locations, severity/urgency rules, update/cancellation semantics, and dedupe rules.
- SAME, EAS, NPAS, CAP, NAADS, NWS CAP, and EAS NET paths must keep generated alert metadata consistent across archive, banner, audio, IVR, CGEN, webhooks, and playout.
- Do not let a lower-priority or later alert interrupt an alert already airing unless the product explicitly supports that policy. Queue instead.
- Cancellation and update messages are first-class alert events, not just archive records.
- Test/demo alerts should be handled deliberately and must not accidentally poison normal alert state.

## Media and realtime behavior

- Keep audio and video lifecycles independent unless a format or protocol requires synchronization at the muxer boundary.
- Avoid catch-up behavior that speeds up, time-compresses, or robotizes live audio. Drop stale backlog or reconnect rather than gradually degrading the stream.
- Silence insertion should be discreet and preferably occur between products, not in the middle of active product audio.
- Long-running media paths must have bounded queues, monotonic timestamps, reconnect/backoff behavior, and useful status events.
- WebRTC, IVR, receiver, CGEN, and public listen paths should be isolated so one feed or client cannot stall the entire media plane.

## Native dependencies and packaging

- Prefer native bindings for long-running services instead of shelling out to subprocesses, especially for FFmpeg/rsmpeg, GStreamer, ONNX, TTS, CAP, and media paths.
- Portable Windows, Linux, and FreeBSD bundles must include the runtime DLLs/shared libraries they actually need, but should not include large downloaded voice/model data unless explicitly intended.
- Build scripts should validate missing runtime DLLs/shared libraries and fail with actionable messages.
- GitHub Actions should build portable archives for Windows, Linux, and FreeBSD and sync bundled `audio`, `managed`, and `webroot` assets into the portable package.
- Windows CI builds and tests use MSYS2 `CLANG64` with the `x86_64-pc-windows-gnullvm` Rust target. Keep the test job and Windows bundle build on the same CLANG64 toolchain, GStreamer, FFmpeg, Opus, and pkg-config package set.
- For the Windows CI environment, add both `C:\\msys64\\clang64\\bin` and `C:\\msys64\\usr\\bin` to `PATH`. Set `PKG_CONFIG_PATH` to the CLANG64 pkg-config directories, but do not set `PKG_CONFIG_LIBDIR`, which can hide transitive dependency metadata.

## Live instances

- Haze is consolidated on `la-servinor` at `172.16.1.31`, reachable with `ssh la-servinor`. Its portable runtime is `/home/rai/haze-weather-radio`; build source changes from this repository before deployment.
- This single instance serves both TeleWeather and CWRS. Managed feed, output, reader, webhook, account, and media configuration must preserve both services.
- For source changes that affect the live instance, rebuild the affected binaries, sync them into `/home/rai/haze-weather-radio/bin`, sync required config/assets, then restart with `systemctl --user restart haze-weather-radio.service`.
- After restarting the live instance, check `systemctl --user status haze-weather-radio.service` and relevant `journalctl --user -u haze-weather-radio.service` logs.
- The former CWRS/CWXR runtime is retired. `C:\Users\rai\Documents\cwrs-haze-weather-radio` is an archived migration source only, not a deployment target.
- The legacy CWXR Windows VM is `172.16.1.38`, reachable through `\\DESKTOP-QOQ6ERC\Users\rai`. It no longer runs Haze or SpeakyAPI.
- The i2jr Windows host is `172.16.1.32`, reachable with `ssh i2jr`. It hosts SpeakyAPI on port `5000` for the consolidated Haze runtime's remote SAPI voices.
- The RF receiver/transmitter Raspberry Pi runs at `172.16.1.37` and is reachable with `ssh pi`. It runs `haze-receiver.service`, which launches `/home/rai/hazeReceiver.py` and feeds PiFmAdv for local receiver testing.
- For RF receiver changes, sync `hazeReceiver.py` to `/home/rai/hazeReceiver.py` on the Pi, preserve an on-device backup when practical, restart with `sudo systemctl restart haze-receiver.service`, then confirm `systemctl status haze-receiver.service` and that PiFmAdv starts with the intended frequency, deviation, and power arguments.
- i2jr's production SpeakyAPI copy is started by the `SpeakyAPI` scheduled task from `C:\ProgramData\SpeakyAPI`. Its source checkout remains at `C:\SpeakyAPI`. Haze deployments must not replace either location unless the task specifically changes the remote voice server.
- Never overwrite live `.env`, logs, runtime state, local data, managed operator files, or generated audio unless the user explicitly asks for that specific destructive action.

## Web panel

- Keep admin controls fast, explicit, and event-driven.
- Web controls should round-trip through the same managed config files the services actually use.
- Public unauthenticated pages such as banner/listen views must not expose admin controls, secrets, environment variables, or signed internal URLs.
- Prefer server-side validation for control actions. The frontend should help the operator, but the backend must enforce safety.

## Testing and verification

- Run focused Go/Rust tests for touched packages when practical.
- After source changes that affect the running portable instance, rebuild before restarting unless the user specifically says not to.
- If a change only touches managed config or bundle assets, sync the bundle/config and restart only the affected runtime pieces when possible.
- For media changes, verify with real runtime tools when practical: service logs, `ffprobe`, `ffplay`, browser/WebRTC status, receiver behavior, or IVR calls.
- For security-sensitive changes, check that secrets are redacted from logs, status APIs, web panel payloads, and generated artifacts.
