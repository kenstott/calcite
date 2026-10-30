#!/usr/bin/env bash
#
# Expires aged snapshots on every Iceberg table whose schema declares iceberg.runMaintenance: true,
# using each table's own snapshotRetentionDays. Covers tables that compacted on their last write and
# then idled, which no write-time expiry will ever revisit. A table with a commit in flight is skipped.
#
# Usage: idle-snapshot-expiry.sh [--dry-run]
# Requires AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_OVERRIDE and GOVDATA_PARQUET_DIR
# (source govdata/.env.prod first); GOVDATA_JAR overrides the jar.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

: "${AWS_ACCESS_KEY_ID:?Set AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?Set AWS_SECRET_ACCESS_KEY}"
: "${AWS_ENDPOINT_OVERRIDE:?Set AWS_ENDPOINT_OVERRIDE}"
: "${GOVDATA_PARQUET_DIR:?Set GOVDATA_PARQUET_DIR}"

JAR="${GOVDATA_JAR:-$HOME/sih-govdata.jar}"
[[ -f "$JAR" ]] || { echo "jar not found: $JAR" >&2; exit 2; }

SCHEMA_ARGS=()
for f in "$PROJECT_ROOT"/govdata/src/main/resources/*/*-schema.yaml; do
  SCHEMA_ARGS+=(--schema-resource "${f#"$PROJECT_ROOT"/govdata/src/main/resources/}")
done

exec java -Xmx2g -cp "$JAR" \
  org.apache.calcite.adapter.govdata.etl.IcebergIdleMaintenance \
  "${SCHEMA_ARGS[@]}" \
  --s3-access-key "$AWS_ACCESS_KEY_ID" \
  --s3-secret-key "$AWS_SECRET_ACCESS_KEY" \
  --s3-endpoint "$AWS_ENDPOINT_OVERRIDE" \
  "$@"
