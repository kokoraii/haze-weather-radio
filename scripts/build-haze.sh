#!/usr/bin/env bash
set -euo pipefail

profile="release"
output_dir=""
media_backend="auto"
include_env=0
skip_go_services=0
skip_cargo_build=0

usage() {
  cat <<'USAGE'
Usage: scripts/build-haze.sh [options]

Options:
  --profile debug|release   Cargo profile to build (default: release)
  --output-dir DIR          Bundle output directory under dist/ (default: dist/Haze_UAP-<OS>-<ARCH>-Portable)
  --media-backend NAME      Media backend: auto|builtin|ffmpeg (default: auto)
  --include-env             Copy .env into the bundle
  --skip-go-services        Do not build or copy managed Go service binaries
  --skip-cargo-build        Reuse an existing target artifact
  -h, --help                Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      profile="${2:?missing profile}"
      shift 2
      ;;
    --output-dir)
      output_dir="${2:?missing output dir}"
      shift 2
      ;;
    --media-backend)
      media_backend="${2:?missing media backend}"
      shift 2
      ;;
    --include-env)
      include_env=1
      shift
      ;;
    --skip-go-services)
      skip_go_services=1
      shift
      ;;
    --skip-cargo-build)
      skip_cargo_build=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$profile" != "debug" && "$profile" != "release" ]]; then
  echo "profile must be debug or release" >&2
  exit 2
fi
if [[ "$media_backend" != "auto" && "$media_backend" != "builtin" && "$media_backend" != "ffmpeg" && "$media_backend" != "rsmpeg" ]]; then
  echo "media backend must be auto, builtin, or ffmpeg" >&2
  exit 2
fi
if [[ "$media_backend" == "rsmpeg" ]]; then
  echo "Warning: rsmpeg is deprecated; using the version-independent FFmpeg runtime loader." >&2
fi

require_gstreamer_build_environment() {
  if ! command -v pkg-config >/dev/null 2>&1; then
    echo "pkg-config is required to build haze-cgen with gstreamer-rs" >&2
    exit 1
  fi
  if ! command -v gst-inspect-1.0 >/dev/null 2>&1; then
    echo "gst-inspect-1.0 is required to validate the haze-cgen runtime" >&2
    exit 1
  fi
  local missing=()
  local package
  for package in gstreamer-1.0 gstreamer-app-1.0 gstreamer-audio-1.0 gstreamer-video-1.0; do
    if ! pkg-config --exists "$package"; then
      missing+=("$package")
    fi
  done
  if (( ${#missing[@]} > 0 )); then
    printf 'Missing GStreamer pkg-config package(s) required for haze-cgen: %s\n' "${missing[*]}" >&2
    exit 1
  fi
  local missing_elements=()
  local element
  for element in x264enc avenc_aac flvmux; do
    if ! gst-inspect-1.0 "$element" >/dev/null 2>&1; then
      missing_elements+=("$element")
    fi
  done
  if ! gst-inspect-1.0 rtmp2sink >/dev/null 2>&1 \
    && ! gst-inspect-1.0 rtmpsink >/dev/null 2>&1; then
    missing_elements+=("rtmp2sink-or-rtmpsink")
  fi
  if (( ${#missing_elements[@]} > 0 )); then
    printf 'Missing GStreamer element(s) required by the bundled CGEN profile: %s\n' "${missing_elements[*]}" >&2
    echo "Install the GStreamer x264, libav, FLV, and RTMP plugins before building a portable runtime." >&2
    echo "Package examples: Debian/Ubuntu gstreamer1.0-plugins-ugly, RHEL/AlmaLinux gstreamer1-plugins-ugly, FreeBSD gstreamer1-plugins-x264." >&2
    exit 1
  fi
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd -- "$script_dir/.." && pwd)"
dist_root="$root/dist"

portable_os() {
  case "$(uname -s)" in
    Linux*) echo "Linux" ;;
    FreeBSD*) echo "FreeBSD" ;;
    Darwin*) echo "macOS" ;;
    MINGW*|MSYS*|CYGWIN*) echo "Windows" ;;
    *) uname -s | tr -cd '[:alnum:]_-' ;;
  esac
}

portable_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "x86_64" ;;
    aarch64|arm64) echo "aarch64" ;;
    armv7l|armv7*) echo "armv7" ;;
    armv6l|armv6*) echo "armv6" ;;
    i386|i686) echo "x86" ;;
    *) uname -m | tr -cd '[:alnum:]_-' ;;
  esac
}

portable_output_dir() {
  printf 'dist/Haze_UAP-%s-%s-Portable\n' "$(portable_os)" "$(portable_arch)"
}

resolve_path() {
  case "$1" in
    /*)
      mkdir -p "$(dirname -- "$1")"
      printf '%s/%s\n' "$(cd -- "$(dirname -- "$1")" && pwd)" "$(basename -- "$1")"
      ;;
    *)
      mkdir -p "$root/$(dirname -- "$1")"
      printf '%s/%s\n' "$(cd -- "$root/$(dirname -- "$1")" && pwd)" "$(basename -- "$1")"
      ;;
  esac
}

if [[ -z "$output_dir" ]]; then
  output_dir="$(portable_output_dir)"
fi

out_full="$(resolve_path "$output_dir")"
bin_full="$out_full/bin"
mkdir -p "$dist_root"
dist_full="$(cd -- "$dist_root" && pwd)"

case "$out_full" in
  "$dist_full"|"$dist_full"/*) ;;
  *)
    echo "Refusing to write outside the dist directory: $out_full" >&2
    exit 1
    ;;
esac

cd "$root"

if command -v clang >/dev/null 2>&1; then
  export CC="${CC:-clang}"
fi
if command -v clang++ >/dev/null 2>&1; then
  export CXX="${CXX:-clang++}"
fi
if command -v llvm-ar >/dev/null 2>&1; then
  export AR="${AR:-llvm-ar}"
fi

copy_bundle_dir() {
  local name="$1"
  local source="$root/bundle/$name"
  local target="$out_full/$name"
  local preserve_dir=""
  if [[ ! -d "$source" ]]; then
    return 0
  fi
  if [[ "$name" == "managed" && -d "$target" ]]; then
    preserve_dir="$(mktemp -d "${TMPDIR:-/tmp}/haze-preserve-managed.XXXXXX")"
    while IFS= read -r -d '' file; do
      local rel="${file#"$target"/}"
      mkdir -p "$(dirname -- "$preserve_dir/$rel")"
      cp -p -- "$file" "$preserve_dir/$rel"
    done < <(find "$target" \( -type f -name '*.onnx' -o -path "$target/voices/kokoro*/*" -type f -o -path "$target/locations/*" -type f \) -print0)
  fi
  rm -rf "$target"
  cp -a "$source" "$target"
  if [[ -n "$preserve_dir" ]]; then
    while IFS= read -r -d '' file; do
      local rel="${file#"$preserve_dir"/}"
      if [[ ! -e "$target/$rel" ]]; then
        mkdir -p "$(dirname -- "$target/$rel")"
        cp -p -- "$file" "$target/$rel"
      fi
    done < <(find "$preserve_dir" -type f -print0)
    rm -rf "$preserve_dir"
  fi
}

copy_linux_runtime_dependencies() {
  local destination="$1"
  shift
  if ! command -v ldd >/dev/null 2>&1; then
    return
  fi
  mkdir -p "$destination"
  local queue=()
  local seen=" "
  local entry dep
  for entry in "$@"; do
    [[ -e "$entry" ]] || continue
    case "$seen" in
      *" $entry "*) ;;
      *)
        seen="$seen$entry "
        queue+=("$entry")
        ;;
    esac
  done
  local index=0
  while (( index < ${#queue[@]} )); do
    entry="${queue[$index]}"
    index=$((index + 1))
    while IFS= read -r dep; do
      [[ -n "$dep" && -f "$dep" ]] || continue
      local name
      name="$(basename -- "$dep")"
      case "$name" in
        libc.so.*|libm.so.*|libpthread.so.*|libdl.so.*|librt.so.*|ld-linux*.so.*)
          continue
          ;;
      esac
      cp -p -- "$dep" "$destination/$name"
      case "$seen" in
        *" $dep "*) ;;
        *)
          seen="$seen$dep "
          queue+=("$dep")
          ;;
      esac
    done < <(ldd "$entry" 2>/dev/null | awk '
      /=>/ && $3 ~ /^\// { print $3 }
      /^[[:space:]]*\// { print $1 }
    ')
  done
}

copy_gstreamer_runtime() {
  local destination="$1"
  if ! command -v pkg-config >/dev/null 2>&1 || ! pkg-config --exists gstreamer-1.0; then
    echo "GStreamer pkg-config metadata is required to bundle haze-cgen" >&2
    exit 1
  fi
  local gst_plugin_source
  gst_plugin_source="$(pkg-config --variable=pluginsdir gstreamer-1.0 || true)"
  if [[ -n "$gst_plugin_source" && -d "$gst_plugin_source" ]]; then
    rm -rf "$destination/gstreamer-1.0"
    mkdir -p "$destination/gstreamer-1.0"
    cp -R "$gst_plugin_source"/. "$destination/gstreamer-1.0"/
    mapfile -d '' gst_plugin_files < <(find "$destination/gstreamer-1.0" -type f \( -name '*.so' -o -name '*.dylib' \) -print0)
    if (( ${#gst_plugin_files[@]} > 0 )); then
      copy_linux_runtime_dependencies "$destination" "${gst_plugin_files[@]}"
    fi
  else
    echo "GStreamer plugin directory was not found; cannot bundle haze-cgen runtime" >&2
    exit 1
  fi
  local scanner_source
  scanner_source="$(pkg-config --variable=pluginscannerdir gstreamer-1.0 2>/dev/null || true)"
  if [[ -n "$scanner_source" && -d "$scanner_source" && -f "$scanner_source/gst-plugin-scanner" ]]; then
    mkdir -p "$destination/gstreamer-1.0"
    cp -p -- "$scanner_source/gst-plugin-scanner" "$destination/gstreamer-1.0/gst-plugin-scanner"
    chmod +x "$destination/gstreamer-1.0/gst-plugin-scanner"
    copy_linux_runtime_dependencies "$destination" "$destination/gstreamer-1.0/gst-plugin-scanner"
  else
    echo "GStreamer plugin scanner was not found; cannot bundle haze-cgen runtime" >&2
    exit 1
  fi
}

require_gstreamer_build_environment
if [[ "$skip_cargo_build" -eq 0 ]]; then
  cargo_profile_args=()
  if [[ "$profile" == "release" ]]; then
    cargo_profile_args=(--release)
  fi
  if [[ "$media_backend" != "builtin" ]]; then
    cargo build "${cargo_profile_args[@]}" -p haze
    cargo build "${cargo_profile_args[@]}" -p haze-cap
    cargo build "${cargo_profile_args[@]}" -p haze-easnet
    cargo build "${cargo_profile_args[@]}" -p haze-location
    cargo build "${cargo_profile_args[@]}" -p haze-playout --features ffmpeg-runtime
    cargo build "${cargo_profile_args[@]}" -p haze-media --features gstreamer-backend
    cargo build "${cargo_profile_args[@]}" -p haze-cgen --features "gpu-wgpu"
  else
    cargo build "${cargo_profile_args[@]}" -p haze -p haze-cap -p haze-easnet -p haze-location -p haze-playout
    cargo build "${cargo_profile_args[@]}" -p haze-media --features gstreamer-backend
    cargo build "${cargo_profile_args[@]}" -p haze-cgen --features "gpu-wgpu"
  fi
fi

profile_dir="$profile"
exe_path="$root/target/$profile_dir/haze"
cap_rust_exe_path="$root/target/$profile_dir/haze-cap-ingest"
easnet_exe_path="$root/target/$profile_dir/haze-easnet"
location_exe_path="$root/target/$profile_dir/haze-location"
playout_exe_path="$root/target/$profile_dir/haze-playout-rs"
media_exe_path="$root/target/$profile_dir/haze-media"
cgen_exe_path="$root/target/$profile_dir/haze-cgen"
if [[ ! -x "$exe_path" ]]; then
  echo "Missing Haze executable: $exe_path" >&2
  exit 1
fi
if [[ ! -x "$cap_rust_exe_path" ]]; then
  echo "Missing Rust CAP ingest executable: $cap_rust_exe_path" >&2
  exit 1
fi
if [[ ! -x "$easnet_exe_path" ]]; then
  echo "Missing Rust EAS NET executable: $easnet_exe_path" >&2
  exit 1
fi
if [[ ! -x "$location_exe_path" ]]; then
  echo "Missing Rust location executable: $location_exe_path" >&2
  exit 1
fi
if [[ ! -x "$playout_exe_path" ]]; then
  echo "Missing Rust playout executable: $playout_exe_path" >&2
  exit 1
fi
if [[ ! -x "$media_exe_path" ]]; then
  echo "Missing Rust media executable: $media_exe_path" >&2
  exit 1
fi
if [[ ! -x "$cgen_exe_path" ]]; then
  echo "Missing Rust cgen executable: $cgen_exe_path" >&2
  exit 1
fi

mkdir -p "$out_full" "$bin_full"
rm -f \
  "$out_full/haze" \
  "$out_full/haze.sh" \
  "$out_full/README-runtime.txt" \
  "$out_full/config.yaml" \
  "$out_full/.env.example" \
  "$out_full/.haze-runtime"
rm -f \
  "$out_full/haze-web" \
  "$out_full/haze-data-ingest" \
  "$out_full/haze-cap-ingest" \
  "$out_full/haze-cap-ingest-rs" \
  "$out_full/haze-easnet" \
  "$out_full/haze-location" \
  "$out_full/haze-tts" \
  "$out_full/haze-product-render" \
  "$out_full/haze-playlist" \
  "$out_full/haze-webhook" \
  "$out_full/haze-asr" \
  "$out_full/haze-ivr" \
  "$out_full/haze-playout" \
  "$out_full/haze-playout-rs" \
  "$out_full/haze-media" \
  "$out_full/haze-cgen" \
  "$bin_full/haze-web" \
  "$bin_full/haze-data-ingest" \
  "$bin_full/haze-cap-ingest" \
  "$bin_full/haze-cap-ingest-rs" \
  "$bin_full/haze-easnet" \
  "$bin_full/haze-location" \
  "$bin_full/haze-tts" \
  "$bin_full/haze-product-render" \
  "$bin_full/haze-playlist" \
  "$bin_full/haze-webhook" \
  "$bin_full/haze-asr" \
  "$bin_full/whisper-server" \
  "$bin_full/haze-ivr" \
  "$bin_full/haze-playout" \
  "$bin_full/haze-playout-rs" \
  "$bin_full/haze-media" \
  "$bin_full/haze-cgen"
rm -rf "$out_full/webroot" "$out_full/audio"

cp "$exe_path" "$out_full/haze"
chmod +x "$out_full/haze"
cp "$cap_rust_exe_path" "$bin_full/haze-cap-ingest"
chmod +x "$bin_full/haze-cap-ingest"
cp "$easnet_exe_path" "$bin_full/haze-easnet"
chmod +x "$bin_full/haze-easnet"
cp "$location_exe_path" "$bin_full/haze-location"
chmod +x "$bin_full/haze-location"
cp "$playout_exe_path" "$bin_full/haze-playout-rs"
chmod +x "$bin_full/haze-playout-rs"
cp "$media_exe_path" "$bin_full/haze-media"
chmod +x "$bin_full/haze-media"
cp "$cgen_exe_path" "$bin_full/haze-cgen"
chmod +x "$bin_full/haze-cgen"

copy_linux_runtime_dependencies "$bin_full" "$out_full/haze" "$bin_full/haze-cap-ingest" "$bin_full/haze-easnet" "$bin_full/haze-location" "$bin_full/haze-playout-rs" "$bin_full/haze-media" "$bin_full/haze-cgen"
copy_gstreamer_runtime "$bin_full"

if [[ "$skip_go_services" -eq 0 ]]; then
  bash "$script_dir/build-go-services.sh" --output-dir "$output_dir"
  copy_linux_runtime_dependencies "$bin_full" "$bin_full/whisper-server"
fi

[[ -e config.yaml ]] && cp config.yaml "$out_full/"
[[ -e .env.example ]] && cp .env.example "$out_full/"

for bundled_dir in webroot managed audio; do
  copy_bundle_dir "$bundled_dir"
done

mkdir -p "$out_full/managed"
managed_scripts="$out_full/managed/scripts"
mkdir -p "$managed_scripts"
for script in scripts/tts/chatterbox_infer.py scripts/tts/f5_infer.py scripts/install-whisper-model.sh scripts/install-whisper-model.ps1; do
  if [[ -f "$root/$script" ]]; then
    cp "$root/$script" "$managed_scripts/"
  fi
done
printf '%s\n' "Haze Weather Radio runtime directory" > "$out_full/.haze-runtime"

mkdir -p "$out_full/audio"

for dir in audio/_uploads audio/_previews bin logs runtime runtime/audio/alerts runtime/audio/playlist runtime/audio/playout runtime/audio/tts runtime/feeds runtime/models/whisper runtime/playlists runtime/queues/alerts runtime/state; do
  mkdir -p "$out_full/$dir"
done

if [[ "$include_env" -eq 1 && -f .env ]]; then
  cp .env "$out_full/"
fi

cat > "$out_full/haze.sh" <<'SH'
#!/usr/bin/env sh
set -eu
bundle_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
export GST_PLUGIN_PATH="$bundle_dir/bin/gstreamer-1.0${GST_PLUGIN_PATH:+:$GST_PLUGIN_PATH}"
export GST_PLUGIN_SYSTEM_PATH_1_0="$bundle_dir/bin/gstreamer-1.0${GST_PLUGIN_SYSTEM_PATH_1_0:+:$GST_PLUGIN_SYSTEM_PATH_1_0}"
export GST_PLUGIN_SCANNER="$bundle_dir/bin/gstreamer-1.0/gst-plugin-scanner"
export LD_LIBRARY_PATH="$bundle_dir/bin:$bundle_dir/bin/gstreamer-1.0${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec "$bundle_dir/haze" "$@"
SH
chmod +x "$out_full/haze.sh"

cat > "$out_full/README-runtime.txt" <<EOF
Haze Weather Radio host bundle

Run:
  ./haze --config config.yaml

Bundled managed service executables are kept in:
  bin/

This bundle was built from:
  $root
EOF

echo "Built $out_full"
echo "Run: $out_full/haze --config config.yaml"
