#!/usr/bin/env bash
set -euo pipefail

whisper_commit="23ee03506a91ac3d3f0071b40e66a430eebdfa1d"
whisper_version="v1.8.6"
whisper_repository="https://github.com/ggml-org/whisper.cpp.git"

output_dir=""
license_dir=""

usage() {
  cat <<'USAGE'
Usage: scripts/build-whisper-runtime.sh --output-dir DIR [--license-dir DIR]

Builds the pinned open-source whisper.cpp runtime. Model weights are not downloaded.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      output_dir="${2:?missing output directory}"
      shift 2
      ;;
    --license-dir)
      license_dir="${2:?missing license directory}"
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

if [[ -z "$output_dir" ]]; then
  usage >&2
  exit 2
fi

for tool in git cmake; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "$tool is required to build the local Whisper runtime" >&2
    exit 1
  fi
done

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd -- "$script_dir/.." && pwd)"
cache_root="$root/target/whisper-cpp/$whisper_commit"
source_dir="${HAZE_WHISPER_CPP_SOURCE:-$cache_root/source}"
build_dir="$cache_root/build-$(uname -s)-$(uname -m)"

if [[ ! -d "$source_dir/.git" ]]; then
  if [[ -n "${HAZE_WHISPER_CPP_SOURCE:-}" ]]; then
    echo "HAZE_WHISPER_CPP_SOURCE is not a Git checkout: $source_dir" >&2
    exit 1
  fi
  mkdir -p "$source_dir"
  git -C "$source_dir" init --quiet
  git -C "$source_dir" remote add origin "$whisper_repository"
  git -C "$source_dir" fetch --quiet --depth 1 origin "$whisper_commit"
  git -C "$source_dir" checkout --quiet --detach FETCH_HEAD
fi

source_commit="$(git -C "$source_dir" rev-parse HEAD)"
if [[ "$source_commit" != "$whisper_commit" ]]; then
  echo "Refusing unpinned whisper.cpp source at $source_dir: $source_commit" >&2
  exit 1
fi

native_build="${HAZE_WHISPER_NATIVE:-OFF}"
case "${native_build^^}" in
  ON|OFF) native_build="${native_build^^}" ;;
  *)
    echo "HAZE_WHISPER_NATIVE must be ON or OFF" >&2
    exit 2
    ;;
esac

cmake -S "$source_dir" -B "$build_dir" \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=OFF \
  -DWHISPER_BUILD_TESTS=OFF \
  -DWHISPER_BUILD_EXAMPLES=ON \
  -DWHISPER_BUILD_SERVER=ON \
  -DWHISPER_CURL=OFF \
  -DWHISPER_SDL2=OFF \
  -DGGML_BACKEND_DL=OFF \
  -DGGML_OPENMP=OFF \
  -DGGML_NATIVE="$native_build" \
  -DGGML_CCACHE=OFF
cmake --build "$build_dir" --config Release --target whisper-server --parallel

runtime_binary=""
for candidate in "$build_dir/bin/whisper-server" "$build_dir/bin/Release/whisper-server"; do
  if [[ -f "$candidate" ]]; then
    runtime_binary="$candidate"
    break
  fi
done
if [[ -z "$runtime_binary" ]]; then
  echo "whisper-server was not produced under $build_dir/bin" >&2
  exit 1
fi

mkdir -p "$output_dir"
cp -f -- "$runtime_binary" "$output_dir/whisper-server"
chmod +x "$output_dir/whisper-server"
"$output_dir/whisper-server" --help >/dev/null 2>&1

if [[ -n "$license_dir" ]]; then
  mkdir -p "$license_dir"
  cp -f -- "$source_dir/LICENSE" "$license_dir/whisper.cpp-$whisper_version-LICENSE.txt"
fi

echo "Built local Whisper runtime $whisper_version in $output_dir"
