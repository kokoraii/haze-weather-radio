# Remote services

Every managed service section in `config.yaml` takes a `host` and a `port`:

```yaml
services:
  go:
    tts:
      enabled: true
      host: "192.168.2.22"
      port: 41810
```

- A host of `127.0.0.1`, `localhost`, blank, or any name or address of this machine (one a local interface owns) launches the service here, as before. The port is not used.
- Any other host makes the Haze daemon connect to a remote instance at `host:port` instead of launching one.

Only TTS has a remote mode so far. Every other service shares the daemon's disk (playlist audio, product files, webhook payloads, IVR caches), so a remote host on one of them is refused: the service is not started, its status reads `refused`, and the web panel will not save that setting. The web panel edits these fields under **Daemon > Service Hosts**. As with other service settings, changes apply after restarting Haze.

## Remote TTS on Windows

A standalone haze-tts on a Windows machine speaks with its native SAPI5 voices directly. `sapi5` and `speakyapi` stay separate reader providers: `sapi5` calls SAPI in the haze-tts process (32-bit voices go through `haze-sapi5-shim.exe`), and `speakyapi` still calls a SpeakyAPI server over HTTP. Readers that should use the Windows voices directly need `provider="sapi5"`.

### 32-bit SAPI5 voices

Many older SAPI5 engines are 32-bit only. They register under the 32-bit registry view (`WOW6432Node`) and their COM servers only load in a 32-bit process, so haze-tts (64-bit) hands them to `haze-sapi5-shim.exe`, a 32-bit helper next to it:

- Voice lists merge both views. Each voice carries `arch`, `x64` for voices haze-tts loads itself and `x86` for voices only the shim can load. List them on the Windows machine with `haze-tts.exe --list-voices --provider sapi5`, and use the `id` or `name` as the reader's `voice_id`.
- Synthesis tries the 64-bit process first and falls back to the shim on any failure, not only a missing voice. That covers engines that register a voice token in the 64-bit view but install only a 32-bit COM server, which fails with "class not registered". When both fail, the error names both causes.
- Each 32-bit synthesis runs in its own short-lived shim process, so a crashing or leaking old engine cannot take haze-tts down. The shim follows the request's timeout and exits on its own when it passes, even if a hung engine never returns. It opens no console window.
- `HAZE_SAPI5_SHIM_PATH` points at a shim elsewhere, and `HAZE_SAPI5_SHIM_DISABLED=1` turns the fallback off.

### Build the bundle

On a Windows machine with MSYS2 CLANG64:

```powershell
scripts/build-haze-tts-standalone.ps1
```

This writes `dist/haze-tts-Windows-x86_64` with `haze-tts.exe`, the SAPI5 shim, the Piper and Kokoro runtime DLLs, `.env.example`, and `run-haze-tts.cmd`. The script ends with a smoke run that lists the SAPI5 voices with only System32 on `PATH`, so a missing bundled DLL fails the build. Voice models are not bundled. Copy Piper voices into `managed\voices\piper` and Kokoro models into `managed\voices\kokoro-multi-lang-v1_0` when readers use them.

### Set up the Windows machine

1. Copy the bundle folder to the machine.
2. Copy `.env.example` to `.env` and set `HAZE_REMOTE_TOKEN` to the same value as the Haze daemon's `.env`. Generate it once from 32 random bytes, base64-encoded.
3. Allow inbound TCP 41810 from the Haze host in Windows Firewall.
4. Run `run-haze-tts.cmd`. It listens on `0.0.0.0:41810` and logs `waiting for a Haze daemon`.

haze-tts refuses to start in listen mode without a token.

### Point Haze at it

1. Add the same `HAZE_REMOTE_TOKEN` to the daemon's `.env`.
2. Set `services.go.tts.host` to the Windows machine and `port` to 41810.
3. Restart Haze.

The managed service status (`runtime/state/goServiceRuntime.json`, and the Daemon page) shows the TTS entry as `remote 192.168.2.22:41810`, with status `running` while connected and `connecting` with the last error while not. The link reconnects on its own with backoff from 1 to 30 seconds.

## How the link works

The daemon dials the remote service and, before anything else, sends one line:

```json
{"type":"link.hello","data":{"protocol":1,"token":"...","settings":{"readers_xml":"...","dictionary_json":"...","provider":"auto","language":"en-CA","timezone":"Local","timeout":"60s"}}}
```

The settings are what a local launch would have passed as flags. `readers.xml` and `dictionary.json` are read fresh on every connect, so the web panel stays the one place they are edited. As with a local haze-tts, reader edits apply after restarting the TTS service, which reconnects the link.

The remote answers `link.ready`, or `link.refused` with a reason and closes. It then registers like any bridge client (`tts.synthesize` and `system.shutdown` only), and the daemon relays the connection into the host bridge as one more client. Requesters see no difference from a local haze-tts.

Audio comes back inline. A remote haze-tts never writes the `output_path` in a request, since that path is on the daemon's machine. It puts the audio in `tts.synthesized` as `output_b64` instead. Before the event reaches the bridge, the daemon:

- writes the audio to the `output_path` that request named, resolved against the runtime directory like a local launch,
- removes `output_b64` from the event.

The daemon only writes paths Haze itself requested, so a remote peer cannot choose where files land. A reply for a job the link never sent becomes `tts.failed`. A request without an `output_path` gets the default `runtime/audio/tts/<job>.wav` filled in on the way out.

A remote haze-tts serves one daemon link at a time. A newer authenticated link replaces the old one. Provider runtimes (Piper and Kokoro models) stay loaded across reconnects.

## Security

- The token travels in plain text, and so does the audio. Keep the link on a trusted network.
- The token is never logged, written to status files, or sent to the web panel.
- A peer that connects without the token is refused before it can submit synthesis requests.
