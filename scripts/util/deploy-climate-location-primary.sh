#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/rai/haze-weather-radio
STAGE=${1:-/home/rai/haze-climate-build-20260811/out}

[[ "$(realpath "$ROOT")" == /home/rai/haze-weather-radio ]]
for file in haze-ivr haze-product-render haze-data-ingest haze-location ca-weather.sqlite ca-weather.sqlite.sha256; do
    [[ -f "$STAGE/$file" ]]
done

expected_sha=$(awk 'NR == 1 { print $1 }' "$STAGE/ca-weather.sqlite.sha256")
[[ "$expected_sha" =~ ^[a-f0-9]{64}$ ]]
[[ "$(sha256sum "$STAGE/ca-weather.sqlite" | awk '{ print $1 }')" == "$expected_sha" ]]

python3 - "$STAGE/ca-weather.sqlite" <<'PY'
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1])
try:
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise SystemExit("candidate location catalog failed integrity check")
finally:
    connection.close()
PY

stamp=$(date -u +%Y%m%dT%H%M%SZ)
backup="$ROOT/backups/pre-climate-location-$stamp"
mkdir -p "$backup/bin" "$backup/managed/locations"

backup_file() {
    local source=$1
    local destination=$2
    if [[ -e "$source" ]]; then
        cp -a -- "$source" "$destination"
    fi
}

backup_file "$ROOT/bin/haze-ivr" "$backup/bin/haze-ivr"
backup_file "$ROOT/bin/haze-product-render" "$backup/bin/haze-product-render"
backup_file "$ROOT/bin/haze-data-ingest" "$backup/bin/haze-data-ingest"
backup_file "$ROOT/bin/haze-location" "$backup/bin/haze-location"
backup_file "$ROOT/managed/locations/ca-weather.sqlite" "$backup/managed/locations/ca-weather.sqlite"
backup_file "$ROOT/managed/locations/ca-weather.sqlite.sha256" "$backup/managed/locations/ca-weather.sqlite.sha256"

atomic_install() {
    local source=$1
    local destination=$2
    local mode=$3
    local pending="${destination}.climate-next.$$"
    install -m "$mode" "$source" "$pending"
    mv -f -- "$pending" "$destination"
}

atomic_install "$STAGE/haze-ivr" "$ROOT/bin/haze-ivr" 0755
atomic_install "$STAGE/haze-product-render" "$ROOT/bin/haze-product-render" 0755
atomic_install "$STAGE/haze-data-ingest" "$ROOT/bin/haze-data-ingest" 0755
atomic_install "$STAGE/haze-location" "$ROOT/bin/haze-location" 0755
atomic_install "$STAGE/ca-weather.sqlite" "$ROOT/managed/locations/ca-weather.sqlite" 0644
atomic_install "$STAGE/ca-weather.sqlite.sha256" "$ROOT/managed/locations/ca-weather.sqlite.sha256" 0644

systemctl --user restart haze-weather-radio.service
systemctl --user is-active --quiet haze-weather-radio.service

printf 'backup=%s\n' "$backup"
sha256sum \
    "$ROOT/bin/haze-ivr" \
    "$ROOT/bin/haze-product-render" \
    "$ROOT/bin/haze-data-ingest" \
    "$ROOT/bin/haze-location" \
    "$ROOT/managed/locations/ca-weather.sqlite"
