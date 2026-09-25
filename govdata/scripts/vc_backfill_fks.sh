#!/usr/bin/env bash
#
# vc_backfill_fks.sh — fill vc_staging's per-source FK columns from stringified_fk for rows staged
# before ChunkOrganizer wrote them. Dry-run unless --apply is given.
#
# Usage:
#   vc_backfill_fks.sh [--apply] [--source-table <table>] [--batch-size <n>]
#
# The target namespace comes from GOVDATA_PARQUET_DIR (a DQ bucket path selects the DQ namespace).
# It must be exported by the caller, so an unset value cannot silently mean production. Rows whose
# key cannot be split unambiguously are left for the next ChunkOrganizer sweep; updated_at is not
# touched, so vss-local's backlog does not re-fetch them. Honours GOVDATA_JAR for a private build.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# The caller must choose the target explicitly: load_env sources .env.prod, which would otherwise
# supply the production bucket and make an unset GOVDATA_PARQUET_DIR silently mean production.
if [ -z "${GOVDATA_PARQUET_DIR:-}" ]; then
  echo "ERROR: export GOVDATA_PARQUET_DIR before running (selects the vc_staging namespace)" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$GOVDATA_ROOT/scripts/parallel/common.sh"
load_env

JAR=$(resolve_classpath) || exit 1

: "${CALCITE_TRACKER_PG_URL:?CALCITE_TRACKER_PG_URL not set -- required to reach vc_staging}"

echo "[vc_backfill_fks] namespace source: $GOVDATA_PARQUET_DIR (jar: $JAR)"
exec "$GOVDATA_JAVA_BIN" -cp "$JAR" org.apache.calcite.adapter.govdata.ref.ChunkFkBackfill "$@"
