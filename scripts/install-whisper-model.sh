#!/usr/bin/env sh
set -eu

model_name="base-q5_1"
model_sha256="422f1ae452ade6f30a004d7e5c6a43195e4433bc370bf23fac9cc591f01a8898"
model_url="https://huggingface.co/ggerganov/whisper.cpp/resolve/5359861c739e955e79d9a303bcbc70fb988958b1/ggml-base-q5_1.bin"

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if [ "$(basename -- "$(dirname -- "$script_dir")")" = "managed" ]; then
  runtime_root="$(CDPATH= cd -- "$script_dir/../.." && pwd)"
else
  runtime_root="$(CDPATH= cd -- "$script_dir/.." && pwd)"
fi
destination="${1:-$runtime_root/runtime/models/whisper/ggml-$model_name.bin}"
destination_dir="$(dirname -- "$destination")"

file_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v sha256 >/dev/null 2>&1; then
    sha256 -q "$1"
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "A SHA-256 utility is required" >&2
    return 1
  fi
}

mkdir -p "$destination_dir"
if [ -f "$destination" ]; then
  existing_sha="$(file_sha256 "$destination")"
  if [ "$existing_sha" = "$model_sha256" ]; then
    echo "Local Whisper model is already installed: $destination"
    exit 0
  fi
  echo "Refusing to overwrite a model with an unexpected checksum: $destination" >&2
  exit 1
fi

temporary="$destination.part.$$"
trap 'rm -f -- "$temporary"' EXIT HUP INT TERM
if command -v curl >/dev/null 2>&1; then
  curl --fail --location --proto '=https' --tlsv1.2 --output "$temporary" "$model_url"
elif command -v fetch >/dev/null 2>&1; then
  fetch -o "$temporary" "$model_url"
else
  echo "curl or fetch is required to install the local Whisper model" >&2
  exit 1
fi

downloaded_sha="$(file_sha256 "$temporary")"
if [ "$downloaded_sha" != "$model_sha256" ]; then
  echo "Downloaded local Whisper model failed SHA-256 verification" >&2
  exit 1
fi
chmod 600 "$temporary"
mv -- "$temporary" "$destination"
trap - EXIT HUP INT TERM
echo "Installed local Whisper model: $destination"
