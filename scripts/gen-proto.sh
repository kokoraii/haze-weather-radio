#!/usr/bin/env bash
# Lint the protobuf contracts and regenerate the committed Go code.
#
#   scripts/gen-proto.sh                          lint and regenerate
#   scripts/gen-proto.sh --check                  also fail if generated code is stale
#   scripts/gen-proto.sh --breaking-against REF   also check wire compatibility
#                                                 against a git ref, e.g. main
#
# buf and the protoc plugins run through `go run` at pinned versions, so only
# Go is required. Rust compiles the same .proto files in its own build.rs.
set -euo pipefail

BUF_VERSION="v1.50.0"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

check=0
breaking_ref=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) check=1 ;;
    --breaking-against)
      shift
      breaking_ref="${1:?--breaking-against needs a git ref}"
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

buf() {
  go run "github.com/bufbuild/buf/cmd/buf@${BUF_VERSION}" "$@"
}

buf lint
if [[ -n "$breaking_ref" ]]; then
  buf breaking --against ".git#ref=${breaking_ref}"
fi
buf generate

if [[ "$check" == 1 ]]; then
  if [[ -n "$(git status --porcelain -- services/go/internal/gen)" ]]; then
    echo "Generated protobuf code is out of date. Run scripts/gen-proto.sh and commit the result." >&2
    git status --short -- services/go/internal/gen >&2
    exit 1
  fi
fi
