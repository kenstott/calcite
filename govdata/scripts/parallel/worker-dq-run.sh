#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# ── argument parsing ──────────────────────────────────────────────────────────
SCHEMA="${1:-}"
if [ -z "$SCHEMA" ]; then
  echo "Usage: $(basename "$0") <schema> [--mode daily|historical] [--dry-run]" >&2
  exit 1
fi

MODE="daily"
DRY_RUN=false
REBUILD=false
REBUILD_START_YEAR=""
shift
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      shift
      MODE="${1:?--mode requires an argument: daily or historical}"
      ;;
    --dry-run)
      DRY_RUN=true
      ;;
    --rebuild)
      REBUILD=true
      ;;
    --start-year)
      shift
      REBUILD_START_YEAR="${1:?--start-year requires a 4-digit year}"
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

if [[ "$MODE" != "daily" && "$MODE" != "historical" ]]; then
  echo "ERROR: --mode must be 'daily' or 'historical', got: $MODE" >&2
  exit 1
fi

WORKER_ID="worker-dq-${SCHEMA}-${MODE}"
DQ_SQL="$GOVDATA_ROOT/scripts/${SCHEMA}_dq.sql"
RUN_DIR="$SCRIPT_DIR/runs/$WORKER_ID"
mkdir -p "$RUN_DIR"

if [ ! -f "$DQ_SQL" ]; then
  echo "ERROR: DQ script not found: $DQ_SQL" >&2
  exit 1
fi

RUN_DATE=$(date +%Y-%m-%d)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$RUN_DIR/dq_${TIMESTAMP}.log"

S3_RESULT_PATH="s3://govdata-tracker-v1/dq-results/schema=${SCHEMA}/run_date=${RUN_DATE}/type=${MODE}/results.parquet"


# ── rebuild: teardown + ETL before DQ ────────────────────────────────────────
if $REBUILD; then
  log_info "$WORKER_ID: --rebuild: starting teardown for schema=$SCHEMA"

  # 1. Discover existing Iceberg tables for this schema so we know which
  #    tracker type= patterns to remove.
  ICEBERG_TABLES=$(rclone lsd "r2:govdata-parquet-v1/$SCHEMA" 2>/dev/null | awk '{print $NF}' | grep -v "^$" || true)

  if [ -n "$ICEBERG_TABLES" ]; then
    for table in $ICEBERG_TABLES; do
      log_info "$WORKER_ID: --rebuild: removing tracker entries for type=$table"
      # Delete all source_key=*__type={table}* objects across every year partition.
      # rclone --include matches file paths relative to the remote root.
      rclone delete r2:govdata-tracker-v1 \
        --include "/year=*/source_key=*__type=${table}*/**" \
        2>/dev/null || true
    done
  else
    log_info "$WORKER_ID: --rebuild: no Iceberg tables found — skipping tracker cleanup"
  fi

  # 2. Delete Iceberg data and raw-parquet staging for this schema.
  # Raw cache (r2:govdata-raw-v1) is intentionally preserved — it is the source of truth
  # for large datasets (e.g. IPEDS) that cannot be cold-fetched reliably from the live API.
  log_info "$WORKER_ID: --rebuild: purging r2:govdata-parquet-v1/$SCHEMA (raw cache preserved)"
  rclone purge "r2:govdata-parquet-v1/$SCHEMA" 2>/dev/null || true

  # 3. Delete existing DQ results so the post-ETL run starts clean.
  log_info "$WORKER_ID: --rebuild: purging dq-results for schema=$SCHEMA"
  rclone purge "r2:govdata-tracker-v1/dq-results/schema=$SCHEMA" 2>/dev/null || true

  # 4. Run the Calcite ETL to rebuild Iceberg tables.
  log_info "$WORKER_ID: --rebuild: running ETL for schema=$SCHEMA"
  REBUILD_MODEL="$RUN_DIR/models/rebuild_${SCHEMA}_$(date +%Y%m%d_%H%M%S).json"
  mkdir -p "$(dirname "$REBUILD_MODEL")"
  # Override run mode and start year so generate_single_schema_model uses the correct range.
  # --start-year truncates historical rebuilds (e.g. smoke tests); omitting it uses GOVDATA_START_YEAR.
  export GOVDATA_RUN_MODE="$MODE"
  [ -n "$REBUILD_START_YEAR" ] && export GOVDATA_START_YEAR="$REBUILD_START_YEAR"
  if ! generate_single_schema_model "$SCHEMA" "$REBUILD_MODEL" 2>/dev/null; then
    log_info "$WORKER_ID: --rebuild: ERROR — no single-schema model generator for schema=$SCHEMA"
    log_info "$WORKER_ID: --rebuild: supported schemas: econ, census, edu, geo, crime, weather, fec, fedregister, lands"
    exit 1
  fi
  run_etl "$REBUILD_MODEL" "$WORKER_ID"
  log_info "$WORKER_ID: --rebuild: ETL complete — proceeding to DQ"
fi

# ── pre-flight anti-pattern checks (warn only — do not abort) ─────────────────
log_info "$WORKER_ID: pre-flight checks"

# Check for local directory named "s3" or "s3:" — signals broken storage wiring
if [ -n "${GOVDATA_PARQUET_DIR:-}" ]; then
  bad_dirs=$(find "$GOVDATA_PARQUET_DIR" -maxdepth 5 -type d \( -name "s3" -o -name "s3:" \) 2>/dev/null || true)
  if [ -n "$bad_dirs" ]; then
    log_info "WARNING: local 's3' or 's3:' directory found — storageProvider may be misconfigured:"
    echo "$bad_dirs" >&2
  fi
fi

# Check for deprecated source= partition on S3
deprecated_path="r2:govdata-parquet-v1/source=${SCHEMA}/"
deprecated_check=$(rclone ls "$deprecated_path" 2>/dev/null | head -1 || true)
if [ -n "$deprecated_check" ]; then
  log_info "WARNING: deprecated path exists: $deprecated_path — this should be removed"
fi

# ── run DQ ────────────────────────────────────────────────────────────────────
TMP_DIR=$(mktemp -d)
RESULT_LOCAL="$TMP_DIR/results.parquet"
trap 'rm -rf "$TMP_DIR"' EXIT

log_info "$WORKER_ID: running DQ for schema=$SCHEMA mode=$MODE"
log_info "$WORKER_ID: DQ script: $DQ_SQL"
log_info "$WORKER_ID: log: $LOG_FILE"

DQ_EXIT=0
{
  # DuckDB 1.5.2+ disables Iceberg version guessing by default; enable it for our internal tables.
  # DuckDB 1.5.2+ ignores SET s3_* for iceberg and auto-loads ~/.aws/credentials instead.
  # CREATE SECRET overrides that and routes all S3/iceberg traffic to R2.
  _r2_endpoint="${AWS_ENDPOINT_OVERRIDE:-21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com}"
  _r2_endpoint="${_r2_endpoint#https://}"
  cat <<_DUCKDB_PREAMBLE_
SET unsafe_enable_version_guessing = true;
SET http_timeout = 60000;
SET http_retries = 1;
CREATE OR REPLACE SECRET r2 (
    TYPE s3,
    KEY_ID '${AWS_ACCESS_KEY_ID}',
    SECRET '${AWS_SECRET_ACCESS_KEY}',
    ENDPOINT '${_r2_endpoint}',
    REGION 'auto',
    URL_STYLE 'path'
);
_DUCKDB_PREAMBLE_

  # Substitute env vars referenced in the DQ SQL (credentials, endpoints)
  envsubst < "$DQ_SQL"

  # Append COPY to write structured results to a local Parquet file before the session ends.
  # dq_results is a TEMP TABLE created inside the DQ SQL — it is still in scope here.
  cat <<SQL
-- Write structured DQ results to local Parquet (appended by worker-dq-run.sh)
COPY (
  SELECT
    schema,
    tbl            AS table_name,
    test,
    status,
    value,
    threshold,
    detail,
    DATE '${RUN_DATE}'  AS run_date,
    '${MODE}'           AS run_type,
    '${SCHEMA}'         AS schema_name
  FROM dq_results
) TO '${RESULT_LOCAL}' (FORMAT PARQUET);
SQL
} | duckdb 2>&1 | tee "$LOG_FILE" || DQ_EXIT=$?

if [ $DQ_EXIT -ne 0 ]; then
  log_info "$WORKER_ID: DuckDB exited with code $DQ_EXIT — table likely missing or schema misconfigured"
  log_info "$WORKER_ID: SCHEMA RESULT: FAIL (script error)"
  exit 1
fi

# ── verdict ───────────────────────────────────────────────────────────────────
if [ ! -f "$RESULT_LOCAL" ]; then
  log_info "$WORKER_ID: result Parquet not written — DQ script may have failed silently"
  exit 1
fi

VERDICT=$(duckdb -csv -c "
  SELECT CASE
    WHEN SUM(CASE WHEN status='fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status='warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END
  FROM read_parquet('${RESULT_LOCAL}');" 2>/dev/null | tail -1 | tr -d '"')

FAIL_COUNT=$(duckdb -csv -c "SELECT COUNT(*) FROM read_parquet('${RESULT_LOCAL}') WHERE status='fail';" 2>/dev/null | tail -1)
WARN_COUNT=$(duckdb -csv -c "SELECT COUNT(*) FROM read_parquet('${RESULT_LOCAL}') WHERE status='warn';" 2>/dev/null | tail -1)

log_info "$WORKER_ID: SCHEMA RESULT: $VERDICT (fails=$FAIL_COUNT warns=$WARN_COUNT)"

# Print failing tests for immediate visibility
if [ "$FAIL_COUNT" -gt 0 ]; then
  log_info "$WORKER_ID: failing tests:"
  duckdb -c "SELECT tbl AS table_name, test, value, threshold, detail FROM read_parquet('${RESULT_LOCAL}') WHERE status='fail' ORDER BY tbl, test;" 2>/dev/null || true
fi

# ── upload results ────────────────────────────────────────────────────────────
if $DRY_RUN; then
  log_info "$WORKER_ID: --dry-run — skipping S3 upload (local results at $RESULT_LOCAL)"
else
  log_info "$WORKER_ID: uploading results to $S3_RESULT_PATH"
  # rclone copyto uploads a single file to an exact destination path (not a directory)
  rclone copyto "$RESULT_LOCAL" "r2:govdata-tracker-v1/dq-results/schema=${SCHEMA}/run_date=${RUN_DATE}/type=${MODE}/results.parquet"
  log_info "$WORKER_ID: results written to $S3_RESULT_PATH"
fi

log_info "$WORKER_ID complete"

[ "$VERDICT" = "FAIL" ] && exit 1
exit 0
