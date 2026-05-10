#!/usr/bin/env bash
# Standalone BundleArchiver runner — retries a failed raw cache upload to R2.
#
# Usage:
#   ./archive-raw-cache.sh --schema edu [--bundle-id run-20260509T2130]
#
# Reads credentials from .env.prod. The local cache dir and cache-directory
# are derived from the schema name using the same conventions as run-pool.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_env

SCHEMA=""
BUNDLE_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema)       SCHEMA="$2";    shift 2 ;;
    --bundle-id)    BUNDLE_ID="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$SCHEMA" ]]; then
  echo "Usage: $0 --schema <name> [--bundle-id <id>]" >&2
  exit 1
fi

LOCAL_CACHE_DIR="$SCRIPT_DIR/.aperio/$SCHEMA/cache/raw"
CACHE_DIRECTORY="${GOVDATA_CACHE_DIR%/}"  # strip trailing slash if present

JAR="$GOVDATA_DIR/build/libs/calcite-govdata-1.42.0-SNAPSHOT-all.jar"

if [[ ! -f "$JAR" ]]; then
  echo "Shadow JAR not found — building..." >&2
  cd "$GOVDATA_DIR/.." && ./gradlew :govdata:shadowJar -q
fi

EXTRA_ARGS=""
if [[ -n "$BUNDLE_ID" ]]; then
  EXTRA_ARGS="--bundle-id $BUNDLE_ID"
fi

exec java -Xms512m -Xmx4g \
  -cp "$JAR" \
  org.apache.calcite.adapter.govdata.etl.BundleArchiverRunner \
  --schema "$SCHEMA" \
  --local-cache-dir "$LOCAL_CACHE_DIR" \
  --cache-directory "$CACHE_DIRECTORY" \
  $EXTRA_ARGS
