#!/usr/bin/env bash
# Checks a built portable bundle and packs it into dist/NAME.tar.xz.
# Used by the Windows, Linux and FreeBSD bundle jobs.
#
#   bash scripts/ci/package-bundle.sh Haze_UAP-Linux-x86_64-Portable
set -euo pipefail

name="${1:?usage: package-bundle.sh BUNDLE_NAME}"
bundle="dist/$name"

for dir in audio managed webroot bin; do
  test -d "$bundle/$dir" || { echo "Bundle is missing directory: $dir" >&2; exit 1; }
done

# Secrets and model weights must never ship in a public archive.
forbidden="$(find "$bundle" -type f \( -name '.env' -o -name '*.onnx' -o -name '*.onnx.data' \
  -o -name '*.safetensors' -o -name '*.gguf' -o -name '*.pt' -o -name '*.pth' -o -name 'ggml-*.bin' \))"
if [[ -n "$forbidden" ]]; then
  printf 'Bundle contains files that must not be published:\n%s\n' "$forbidden" >&2
  exit 1
fi

tar -cJf "dist/$name.tar.xz" -C dist "$name"
ls -lh "dist/$name.tar.xz"
