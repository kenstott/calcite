#!/usr/bin/env bash
#
# Reports, and with --delete removes, Iceberg data files that no retained snapshot references, for every
# table whose schema declares iceberg.runMaintenance: true. Dry run unless --delete is given. A file is
# deleted only when it is also older than --min-age-days (default 3, never below 1). Schemas with a live
# ETL writer (run-pool pid markers) are skipped, and so is any table whose commit lock is held.
#
# Usage: orphan-file-cleanup.sh [--min-age-days N] [--delete]
# Requires AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_OVERRIDE and GOVDATA_PARQUET_DIR
# (source govdata/.env.prod first); GOVDATA_JAR overrides the jar.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=parallel/common.sh
source "$SCRIPT_DIR/parallel/common.sh"

: "${AWS_ACCESS_KEY_ID:?Set AWS_ACCESS_KEY_ID}"
# length test: xtrace prints the length, never the secret
[ "${#AWS_SECRET_ACCESS_KEY}" -gt 0 ] || { echo "Set AWS_SECRET_ACCESS_KEY" >&2; exit 1; }
: "${AWS_ENDPOINT_OVERRIDE:?Set AWS_ENDPOINT_OVERRIDE}"
: "${GOVDATA_PARQUET_DIR:?Set GOVDATA_PARQUET_DIR}"

JAR="${GOVDATA_JAR:-$HOME/sih-govdata.jar}"
[[ -f "$JAR" ]] || { echo "jar not found: $JAR" >&2; exit 2; }

detect_active_schemas "$SCRIPT_DIR/parallel/runs/pids"

SCHEMA_ARGS=()
for f in "$PROJECT_ROOT"/govdata/src/main/resources/*/*-schema.yaml; do
  schema_dir="$(basename "$(dirname "$f")")"
  case " $ACTIVE_SCHEMAS " in
    *" $schema_dir "*) echo "skipping $schema_dir: live ETL writer" >&2; continue ;;
  esac
  SCHEMA_ARGS+=(--schema-resource "${f#"$PROJECT_ROOT"/govdata/src/main/resources/}")
done
[[ ${#SCHEMA_ARGS[@]} -gt 0 ]] || { echo "every schema has a live writer; nothing to do" >&2; exit 0; }

exec java -Xmx2g -cp "$JAR" \
  org.apache.calcite.adapter.govdata.etl.IcebergOrphanCleanup \
  "${SCHEMA_ARGS[@]}" \
  --s3-credentials-from-env \
  --s3-endpoint "$AWS_ENDPOINT_OVERRIDE" \
  "$@"
