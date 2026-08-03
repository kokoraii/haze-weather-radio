#!/usr/bin/env bash
set -euo pipefail

output_dir=""

usage() {
  cat <<'USAGE'
Usage: scripts/build-go-services.sh [options]

Options:
  --output-dir DIR   Bundle output directory under dist/ (default: dist/Haze_UAP-<OS>-<ARCH>-Portable)
  -h, --help         Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      output_dir="${2:?missing output dir}"
      shift 2
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

mkdir -p "$out_full" "$bin_full" "$root/target/go-build-cache" "$root/target/go-tmp"
rm -f \
  "$out_full/haze-web" \
  "$out_full/haze-data-ingest" \
  "$out_full/haze-tts" \
  "$out_full/haze-product-render" \
  "$out_full/haze-playlist" \
  "$out_full/haze-webhook" \
  "$out_full/haze-asr" \
  "$out_full/haze-ivr"

copy_bundle_dir() {
  local name="$1"
  local source="$root/bundle/$name"
  local target="$out_full/$name"
  local preserve_dir=""
  if [[ ! -d "$source" ]]; then
    return 0
  fi
  if [[ "$name" == "managed" && -d "$target" ]]; then
    preserve_dir="$(mktemp -d "${TMPDIR:-/tmp}/haze-preserve-onnx.XXXXXX")"
    while IFS= read -r -d '' file; do
      local rel="${file#"$target"/}"
      mkdir -p "$(dirname -- "$preserve_dir/$rel")"
      cp -p -- "$file" "$preserve_dir/$rel"
    done < <(find "$target" \( -type f -name '*.onnx' -o -path "$target/voices/kokoro*/*" -type f \) -print0)
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

copy_sherpa_onnx_runtime_libraries() {
  local destination="$1"
  local goos goarch module triple module_dir lib_dir
  goos="$(go env GOOS 2>/dev/null || true)"
  goarch="$(go env GOARCH 2>/dev/null || true)"
  case "$goos" in
    windows) module="github.com/k2-fsa/sherpa-onnx-go-windows" ;;
    linux) module="github.com/k2-fsa/sherpa-onnx-go-linux" ;;
    darwin) module="github.com/k2-fsa/sherpa-onnx-go-macos" ;;
    *) return 0 ;;
  esac
  case "$goos/$goarch" in
    windows/amd64) triple="x86_64-pc-windows-gnu" ;;
    windows/386) triple="i686-pc-windows-gnu" ;;
    linux/amd64) triple="x86_64-unknown-linux-gnu" ;;
    linux/arm64) triple="aarch64-unknown-linux-gnu" ;;
    linux/arm) triple="arm-unknown-linux-gnueabihf" ;;
    darwin/amd64) triple="x86_64-apple-darwin" ;;
    darwin/arm64) triple="aarch64-apple-darwin" ;;
    *) return 0 ;;
  esac
  module_dir="$(go list -m -f '{{.Dir}}' "$module" 2>/dev/null || true)"
  if [[ -z "$module_dir" ]]; then
    return 0
  fi
  lib_dir="$module_dir/lib/$triple"
  if [[ ! -d "$lib_dir" ]]; then
    return 0
  fi
  find "$lib_dir" -maxdepth 1 -type f \( -name '*.dll' -o -name '*.so' -o -name '*.so.*' -o -name '*.dylib' \) -exec cp -f -- {} "$destination/" \;
}

cd "$root/services/go"
export GOCACHE="$root/target/go-build-cache"
export GOTMPDIR="$root/target/go-tmp"
git_commit="unknown"
if git_commit_raw="$(git -C "$root" rev-parse --short=12 HEAD 2>/dev/null)" && [[ -n "$git_commit_raw" ]]; then
  git_commit="$git_commit_raw"
fi
web_ldflags="-X github.com/meowraii/haze-weather-radio/services/go/internal/webgateway.BuildGitCommit=$git_commit"

build_web_args=()
if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists opus; then
  export CGO_ENABLED=1
  build_web_args=(-tags opus_cgo)
elif [[ "${HAZE_ALLOW_NO_OPUS:-}" != "1" && "${HAZE_ALLOW_NO_OPUS:-}" != "true" ]]; then
  echo "Native Opus build inputs are required for receiver/WebRTC audio. Install pkg-config and libopus development files, or set HAZE_ALLOW_NO_OPUS=1 for a degraded dev-only build." >&2
  exit 1
else
  echo "Warning: building haze-web without native Opus support; receiver Opus/WebRTC audio will be degraded." >&2
fi

bash "$root/scripts/build-whisper-runtime.sh" \
  --output-dir "$bin_full" \
  --license-dir "$out_full/licenses"

go build "${build_web_args[@]}" -ldflags "$web_ldflags" -o "$bin_full/haze-web" ./cmd/haze-web
copy_sherpa_onnx_runtime_libraries "$bin_full"
if [[ "${#build_web_args[@]}" -gt 0 ]]; then
  "$bin_full/haze-web" --check-codecs --require-opus
fi
go build -o "$bin_full/haze-data-ingest" ./cmd/haze-data-ingest
go build -o "$bin_full/haze-tts" ./cmd/haze-tts
go build -o "$bin_full/haze-product-render" ./cmd/haze-product-render
go build -o "$bin_full/haze-playlist" ./cmd/haze-playlist
go build -o "$bin_full/haze-webhook" ./cmd/haze-webhook
go build -o "$bin_full/haze-asr" ./cmd/haze-asr
go build -o "$bin_full/haze-ivr" ./cmd/haze-ivr

chmod +x \
  "$bin_full/haze-web" \
  "$bin_full/haze-data-ingest" \
  "$bin_full/haze-tts" \
  "$bin_full/haze-product-render" \
  "$bin_full/haze-playlist" \
  "$bin_full/haze-webhook" \
  "$bin_full/haze-asr" \
  "$bin_full/haze-ivr"

for bundled_dir in webroot managed audio; do
  copy_bundle_dir "$bundled_dir"
done

managed_scripts="$out_full/managed/scripts"
mkdir -p "$managed_scripts"
for script in scripts/tts/chatterbox_infer.py scripts/tts/f5_infer.py scripts/install-whisper-model.sh scripts/install-whisper-model.ps1; do
  if [[ -f "$root/$script" ]]; then
    cp "$root/$script" "$managed_scripts/"
  fi
done

echo "Built Go services in $bin_full"
