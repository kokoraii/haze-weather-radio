# Haze Weather Radio

Haze Weather Radio builds weather, forecast, and alert audio feeds from ECCC, NWS, and other configured sources. It includes web control, TTS, routine playout, CAP alert ingest, IVR, Icecast, and local or network audio outputs.

## Safety

Haze can generate valid SAME headers and attention tones. Only use RF or alert output where you are authorized to do so. Do not interfere with official alerting or broadcast services.

See [Account Sign-In Operations](docs/accounts-security.md) before enabling account mode on a live operator panel.

## Build

The build scripts create a portable bundle in `dist/` containing Haze, its managed services, web files, and default configuration.

### Windows

Install Git, Rust stable, Go 1.25, FFmpeg, CMake, Ninja, and the GStreamer development runtime. From PowerShell:

```powershell
git clone https://github.com/meowraii/haze-weather-radio.git
cd haze-weather-radio
.\scripts\build-haze.ps1
```

### Linux

Install the native dependencies for your platform.

#### Debian or Ubuntu

```bash
sudo apt update
sudo apt install -y \
  build-essential clang cmake pkg-config git curl \
  ffmpeg openssl redis-server libasound2-dev libopus-dev libopusfile-dev libssl-dev libudev-dev \
  libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
  gstreamer1.0-tools gstreamer1.0-libav \
  gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
  gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly
```

#### Fedora, Rocky Linux, or other DNF systems

```bash
sudo dnf install -y \
  gcc gcc-c++ clang cmake pkgconf-pkg-config git curl \
  ffmpeg-free ffmpeg-free-devel alsa-lib-devel openssl openssl-devel redis systemd-devel \
  opus-devel opusfile-devel gstreamer1-devel gstreamer1-plugins-base-devel \
  gstreamer1-plugins-base gstreamer1-plugins-good \
  gstreamer1-plugins-bad-free gstreamer1-plugins-ugly gstreamer1-libav
```

Fedora's `ffmpeg-free` build has restricted codec support. Enable RPM Fusion and install its `ffmpeg` packages if your required media format is unavailable. Rocky Linux may require EPEL and a multimedia repository for the equivalent FFmpeg and GStreamer plugin packages.

#### Arch Linux

```bash
sudo pacman -S --needed \
  base-devel clang cmake pkgconf git curl ffmpeg openssl redis systemd alsa-lib \
  opus opusfile gstreamer gst-plugins-base gst-plugins-good \
  gst-plugins-bad gst-plugins-ugly gst-libav
```

#### FreeBSD

```sh
sudo pkg install -y \
  bash git curl cmake pkgconf llvm gmake ffmpeg openssl redis alsa-lib opus opusfile \
  gstreamer1 gstreamer1-plugins-all
```

FreeBSD builds use the same `scripts/build-haze.sh` command. The portable bundle name is automatically tagged `FreeBSD`.

Install Rust stable and Go 1.25 or newer. The standard Rust installer works on Linux and FreeBSD:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
source "$HOME/.cargo/env"
```

Install Go using your distribution or [go.dev](https://go.dev/dl/), then ensure `go version` reports 1.25 or newer.

Build the bundle:

```bash
git clone https://github.com/meowraii/haze-weather-radio.git
cd haze-weather-radio
bash scripts/build-haze.sh
```

## Run

Copy the completed portable bundle to its runtime machine. Keep its `managed`, `audio`, and `webroot` directories beside the Haze executable.

```bash
./haze --config config.yaml --workdir /path/to/haze-runtime
```

On Windows:

```powershell
.\haze.exe --config config.yaml --workdir C:\path\to\haze-runtime
```

Use `config.yaml` for service settings. Feed, output, voice, and product wording belong in `managed/configs/`. Use the scripts in `scripts/` for packaging and service installation.

### Desktop tray

Interactive Haze launches on Windows and Linux register a desktop tray icon. Left-clicking the icon opens the public panel in the default browser. Its context menu provides `Open Public Panel`, `Restart All Services`, and `Exit`.

`Restart All Services` restarts every enabled managed Go and Rust service while leaving the Haze host, event bridges, and tray running. `Exit` uses the normal graceful shutdown path. The public panel address uses `HAZE_PUBLIC_BASE_URL`, `WEB_BASE_URL`, or `WEB_HOSTNAME` when configured, then falls back to the public web-panel listener in `config.yaml`. Wildcard listeners such as `0.0.0.0` open through loopback.

Windows Service Control Manager launches do not create a tray because Windows services run outside the interactive desktop session. On Linux, the tray requires a graphical session, a D-Bus session bus, and a desktop StatusNotifier host. Headless and system-service launches continue normally without a tray.

### Local Windows development server

This workstation uses `dist/Haze-RadioMET-Development` as its persistent, isolated development runtime. The public and admin panels are available only on [http://127.0.0.1:18080](http://127.0.0.1:18080). Runtime state, generated audio, logs, and the local catalog overlay stay inside that directory.

The development profile deliberately combines these configurations:

| Concern | Development source |
| --- | --- |
| Feed definitions | `managed/configs/cwxr-feeds.xml`, currently 11 CWRS feeds |
| Location feed bindings | `managed/configs/local-development-locations.xml` |
| TTS readers | RadioMET `managed/configs/readers.xml` |
| Product wording and personality | RadioMET `managed/configs/products.xml` and the `operator` settings in the local `config.yaml` |
| Outputs | `managed/configs/local-development-output.xml`, with feed WebRTC enabled and every other output type disabled |

The local `config.yaml` enables media and playout only to supply public WebRTC audio for the configured feeds. HTTP audio, Icecast, UDP, RTP, RTMP, SRT, RTSP, audio-device output, CGEN, EAS NET, webhooks, ASR, IVR, and the receiver endpoint remain disabled. CAP ingest, weather data ingest, product rendering, RadioMET TTS, playlists, the location service, and the web panel remain enabled. The panel binds to loopback and authentication is disabled only for this local profile. Do not expose port `18080` beyond this machine.

The public `/feeds` and `/listen` pages remain available when a profile has no WebRTC or HTTP output. In that state they show feed and served-location information without rendering audio players.

Start the existing development server from the repository root:

```powershell
.\scripts\start-local-development.ps1
```

Stop only that isolated instance:

```powershell
.\scripts\stop-local-development.ps1
```

Both scripts verify the executable path before acting on a PID. The stop script only terminates managed Haze processes whose executable paths remain inside the isolated development directory.

### Public coverage-map basemap

The public feed coverage map reads its basemap profile from `managed/maps/public-basemap.json`. The bundled profile points MapLibre GL JS at the OpenFreeMap dark vector style and an optional Esri World Imagery raster layer. The corner control switches between vector and satellite rendering, and both providers are fetched directly by the browser.

If the profile is missing, invalid, or the detailed style cannot be reached, the map falls back to a local dark coverage-only background. Served areas and alert polygons continue to render. The public endpoint validates the exact bundled HTTPS vector and satellite endpoints, and the public content-security policy allows map resources only from those hosts.

The default profile is intentionally small. It follows the upstream detailed style as it evolves instead of copying a large generated style bundle into each Haze portable runtime. Changing providers requires updating the managed profile and the server-side provider allowlist together.

Before refreshing binaries, stop the server. Rebuild the Go services and current MSVC Rust services, then copy the Rust executables into the same runtime:

```powershell
.\scripts\build-go-services.ps1 -OutputDir dist/Haze-RadioMET-Development
$Clang64Bin = "C:\msys64\clang64\bin"
$env:Path = "$Clang64Bin;C:\msys64\usr\bin;$env:Path"
$env:PKG_CONFIG = "$Clang64Bin\pkg-config.exe"
$env:PKG_CONFIG_PATH = "C:\msys64\clang64\lib\pkgconfig;C:\msys64\clang64\share\pkgconfig"
Remove-Item Env:PKG_CONFIG_LIBDIR -ErrorAction SilentlyContinue
cargo build -p haze -p haze-location -p haze-cap -p haze-playout
cargo build -p haze-media --features gstreamer-backend
Copy-Item .\target\debug\haze.exe .\dist\Haze-RadioMET-Development\haze.exe -Force
Copy-Item .\target\debug\haze-location.exe .\dist\Haze-RadioMET-Development\bin\haze-location.exe -Force
Copy-Item .\target\debug\haze-cap-ingest.exe .\dist\Haze-RadioMET-Development\bin\haze-cap-ingest.exe -Force
Copy-Item .\target\debug\haze-media.exe .\dist\Haze-RadioMET-Development\bin\haze-media.exe -Force
Copy-Item .\target\debug\haze-playout-rs.exe .\dist\Haze-RadioMET-Development\bin\haze-playout-rs.exe -Force
```

The Go build refreshes `managed`, `audio`, and `webroot` from the bundle while preserving the local root `config.yaml` and runtime state. The start script adds the installed MSYS2 CLANG64 GStreamer runtime to the isolated process environment so the MSVC media executable can provide WebRTC encoding. Verify the server with:

```powershell
Invoke-RestMethod http://127.0.0.1:18080/api/public/v1/health
Invoke-RestMethod http://127.0.0.1:18080/api/public/v1/panel/state
Invoke-RestMethod 'http://127.0.0.1:18080/api/public/v1/feed/map?feed=cwxr-sk01'
```

### Offline IVR location transcription

IVR voice location search uses the open-source [Whisper](https://github.com/openai/whisper) model through the pinned [whisper.cpp](https://github.com/ggml-org/whisper.cpp) runtime. Audio is processed on the Haze host. The ASR service does not use an OpenAI API key and has no hosted transcription fallback.

Portable bundles include `whisper-server`, but intentionally exclude model weights. Install the checksum-pinned multilingual `base-q5_1` model once in the runtime directory:

```bash
sh managed/scripts/install-whisper-model.sh
```

On Windows:

```powershell
.\managed\scripts\install-whisper-model.ps1
```

The installer writes `runtime/models/whisper/ggml-base-q5_1.bin` atomically and verifies its SHA-256 digest. Restart Haze after installing or replacing the model. If the model or runtime is unavailable, voice search remains degraded while T9, multitap, and numeric location entry continue working.

### Public WebRTC behind NAT

Set `services.rust.media.webrtc.public_ip` to the gateway's public IPv4 address and configure a bounded `udp_port_min` and `udp_port_max`. Forward that same UDP range one-to-one from the gateway to the Haze host and allow it through the host firewall. Each concurrent WebRTC listener uses one UDP port. Environment variables `HAZE_MEDIA_WEBRTC_HOST`, `HAZE_MEDIA_WEBRTC_UDP_PORT_MIN`, and `HAZE_MEDIA_WEBRTC_UDP_PORT_MAX` override these values for machine-specific deployments.

This static ICE path works for clients that can send outbound UDP. A TURN service with short-lived credentials is still required for networks that block WebRTC UDP completely. Do not publish a permanent TURN password in `config.yaml`, panel state, logs, or the web bundle.

### Account mode dependencies

Production account mode additionally requires:

- Redis running on a private or loopback interface for session leases, revocation, and atomic rate limits.
- Five private runtime variables: `HAZE_REDIS_URL`, `HAZE_PASETO_V4_LOCAL_KEY`, `HAZE_PASSWORD_PEPPER`, `HAZE_MFA_ENCRYPTION_KEY`, and `HAZE_AUDIT_HMAC_KEY`.
- HTTPS for the operator panel. Account cookies are always `Secure`, `HttpOnly`, and `SameSite=Strict`.
- Accurate system time for TOTP MFA.

Account mode fails closed if Redis or a required key is unavailable. Generate each cryptographic key independently, keep the live `.env` mode at `0600`, and never publish the environment file, account database, or audit keys through the web panel or Samba. See [Account Sign-In Operations](docs/accounts-security.md) for Redis setup, key generation, bootstrap administration, recovery, proxy trust, and audit-log guidance.

## Project layout

- `crates/`: Rust host, media, playout, ingest, and CGEN services.
- `services/go/`: web panel, TTS, product renderer, playlist, IVR, and webhooks.
- `bundle/`: portable runtime assets and operator-managed defaults.
- `scripts/`: build, packaging, and service helpers.
