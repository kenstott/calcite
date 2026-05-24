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
INCLUDE_DAILY=false
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
    --include-daily)
      INCLUDE_DAILY=true
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
ETL_LOG_DIR=""  # set after rebuild ETL so error handler finds the right log

S3_RESULT_PATH="s3://govdata-tracker-v1/dq-results/schema=${SCHEMA}/run_date=${RUN_DATE}/type=${MODE}/results.parquet"

# ── error handler + EXIT trap (must be defined before rebuild block) ──────────
_SCRIPT_COMPLETE=false

_collect_log_tail() {
  local log="$1" lines="${2:-30}"
  if [ -f "$log" ] && [ -s "$log" ]; then
    echo "<details><summary>\`$(basename "$log")\` — last ${lines} lines</summary>"
    echo ""
    echo '```'
    tail -n "$lines" "$log"
    echo '```'
    echo "</details>"
  else
    echo "_\`$(basename "$log")\` not found or empty_"
  fi
}

# Extract a one-line root-cause summary from a log file.
# Greps for the first Exception/Error/OOM line and returns it.
_extract_root_cause() {
  local log="$1"
  [ -f "$log" ] || { echo "(log not found)"; return; }
  local line
  line=$(grep -m1 -E "OutOfMemoryError|Exception|ERROR |FATAL |BUILD FAILURE|HTTP Error|HTTP [45][0-9][0-9]" "$log" 2>/dev/null || true)
  if [ -n "$line" ]; then
    echo "${line:0:200}"
  else
    echo "(no error pattern found in log)"
  fi
}

_file_script_error_issue() {
  _SCRIPT_COMPLETE=true
  local detail="$1"
  if ! command -v gh >/dev/null 2>&1; then
    log_info "$WORKER_ID: gh not found in PATH ($PATH) — skipping issue filing"
    return
  fi
  if [ -z "${GH_TOKEN:-}${GITHUB_TOKEN:-}" ]; then
    if ! gh auth status --hostname github.com >/dev/null 2>&1; then
      log_info "$WORKER_ID: gh not authenticated (no GH_TOKEN, no stored credential) — skipping issue filing"
      return
    fi
  fi
  gh label create "dq"      --color "#0075ca" --description "Data quality"    --repo kenstott/calcite 2>/dev/null || true
  gh label create "dq-fail" --color "#d93f0b" --description "DQ hard failure" --repo kenstott/calcite 2>/dev/null || true

  # Locate the ETL log — it lands in runs/worker-<schema>-initial/, not in RUN_DIR
  local latest_etl=""
  if [ -n "${ETL_LOG_DIR:-}" ]; then
    latest_etl=$(ls -t "${ETL_LOG_DIR}"/etl_*.log 2>/dev/null | head -1 || true)
  fi
  # Fallback: also check RUN_DIR in case a future code path writes there
  if [ -z "$latest_etl" ]; then
    latest_etl=$(ls -t "$RUN_DIR"/etl_*.log 2>/dev/null | head -1 || true)
  fi

  # Extract root cause: prefer DQ log when DuckDB ran (it has the actual crash),
  # fall back to ETL log when DQ log is empty/missing (ETL crashed before DuckDB started).
  local root_cause_log
  if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
    root_cause_log="$LOG_FILE"
  else
    root_cause_log="${latest_etl:-$LOG_FILE}"
  fi
  local root_cause
  root_cause=$(_extract_root_cause "$root_cause_log")

  # Build log sections (collapsible so the issue is readable without scrolling)
  local log_sections=""
  if [ -n "$latest_etl" ]; then
    log_sections="$(_collect_log_tail "$latest_etl" 40)"$'\n\n'
  fi
  if [ -f "$LOG_FILE" ] && [ -s "$LOG_FILE" ]; then
    log_sections="${log_sections}$(_collect_log_tail "$LOG_FILE" 30)"
  fi

  local issue_title="[DQ] ${SCHEMA}: ERROR — ${root_cause:0:80}"

  local open_issue
  open_issue=$(gh issue list \
    --repo kenstott/calcite \
    --state open \
    --label dq \
    --limit 200 \
    --json number,title \
    --jq ".[] | select(.title | startswith(\"[DQ] ${SCHEMA}:\")) | .number" \
    2>&1 | head -1)
  if [ -n "$open_issue" ] && [[ "$open_issue" =~ ^[0-9]+$ ]]; then
    gh issue comment "$open_issue" \
      --repo kenstott/calcite \
      --body "**Script error ${RUN_DATE}** — ${detail}

**Root cause:** \`${root_cause}\`

${log_sections}" \
      && log_info "$WORKER_ID: commented on DQ issue #${open_issue}" \
      || log_info "$WORKER_ID: WARNING: failed to comment on issue #${open_issue}"
  else
    gh issue create \
      --repo kenstott/calcite \
      --title "${issue_title}" \
      --label "dq" \
      --label "dq-fail" \
      --body "## DQ Script Error: \`${SCHEMA}\`

**Date:** ${RUN_DATE}
**Mode:** ${MODE}
**Worker:** \`${WORKER_ID}\`

## Root Cause

\`\`\`
${root_cause}
\`\`\`

## Detail

${detail}

## Logs

${log_sections}" \
      && log_info "$WORKER_ID: created DQ error issue" \
      || log_info "$WORKER_ID: WARNING: gh issue create failed"
  fi
}

_on_exit() {
  local code=$?
  [ -n "${TMP_DIR:-}" ] && rm -rf "$TMP_DIR"
  if [ "$code" -ne 0 ] && ! $_SCRIPT_COMPLETE; then
    _file_script_error_issue "Script exited unexpectedly with code ${code} — see log: \`${LOG_FILE}\`"
  fi
}
trap '_on_exit' EXIT

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

  # 4. Run historical ETL pass.
  log_info "$WORKER_ID: --rebuild: running historical ETL for schema=$SCHEMA"
  REBUILD_MODEL="$RUN_DIR/models/rebuild_${SCHEMA}_$(date +%Y%m%d_%H%M%S).json"
  mkdir -p "$(dirname "$REBUILD_MODEL")"
  export GOVDATA_RUN_MODE="historical"
  export GOVDATA_INCREMENTAL_START_YEAR="$(date +%Y)"
  if [ -n "$REBUILD_START_YEAR" ]; then
    export GOVDATA_START_YEAR="$REBUILD_START_YEAR"
  else
    export GOVDATA_START_YEAR="$(get_dq_start_year "$SCHEMA")"
  fi
  log_info "$WORKER_ID: --rebuild: year range ${GOVDATA_START_YEAR}–$((GOVDATA_INCREMENTAL_START_YEAR - 1)) for schema=$SCHEMA"
  if ! generate_single_schema_model "$SCHEMA" "$REBUILD_MODEL" 2>/dev/null; then
    log_info "$WORKER_ID: --rebuild: ERROR — no single-schema model generator for schema=$SCHEMA"
    exit 1
  fi
  run_etl "$REBUILD_MODEL" "worker-${SCHEMA}-initial"
  ETL_LOG_DIR="$SCRIPT_DIR/runs/worker-${SCHEMA}-initial"
  log_info "$WORKER_ID: --rebuild: historical ETL complete"

  # 5. Optionally run daily (current-year) ETL pass before DQ.
  if $INCLUDE_DAILY; then
    log_info "$WORKER_ID: --include-daily: running daily ETL pass for schema=$SCHEMA"
    DAILY_MODEL="$RUN_DIR/models/daily_${SCHEMA}_$(date +%Y%m%d_%H%M%S).json"
    export GOVDATA_RUN_MODE="daily"
    unset GOVDATA_START_YEAR
    if generate_single_schema_model "$SCHEMA" "$DAILY_MODEL" 2>/dev/null; then
      run_etl "$DAILY_MODEL" "worker-${SCHEMA}-daily"
      ETL_LOG_DIR="$SCRIPT_DIR/runs/worker-${SCHEMA}-daily"
      log_info "$WORKER_ID: --include-daily: daily ETL complete"
    else
      log_info "$WORKER_ID: --include-daily: no daily model for schema=$SCHEMA — skipping"
    fi
  fi

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
  _file_script_error_issue "DuckDB exited with code ${DQ_EXIT} — table likely missing or schema misconfigured. Check log: \`${LOG_FILE}\`"
  exit 1
fi

# ── verdict ───────────────────────────────────────────────────────────────────
if [ ! -f "$RESULT_LOCAL" ]; then
  log_info "$WORKER_ID: result Parquet not written — DQ script may have failed silently"
  _file_script_error_issue "Result Parquet not written — DQ script may have failed silently. Check log: \`${LOG_FILE}\`"
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

# ── GitHub issue filing ───────────────────────────────────────────────────────
_gh_available() {
  command -v gh >/dev/null 2>&1 || return 1
  [ -n "${GH_TOKEN:-}${GITHUB_TOKEN:-}" ] && return 0
  gh auth status --hostname github.com >/dev/null 2>&1
}
if _gh_available; then
  # Ensure labels exist (no-op if already present)
  gh label create "dq"      --color "#0075ca" --description "Data quality"    --repo kenstott/calcite 2>/dev/null || true
  gh label create "dq-warn" --color "#e4e669" --description "DQ warning"      --repo kenstott/calcite 2>/dev/null || true
  gh label create "dq-fail" --color "#d93f0b" --description "DQ hard failure" --repo kenstott/calcite 2>/dev/null || true

  # Find existing open DQ issue for this schema
  OPEN_ISSUE=$(gh issue list \
    --repo kenstott/calcite \
    --state open \
    --label dq \
    --limit 200 \
    --json number,title \
    --jq ".[] | select(.title | startswith(\"[DQ] ${SCHEMA}:\")) | .number" \
    2>/dev/null | head -1)

  if [ "$VERDICT" = "PASS" ]; then
    if [ -n "$OPEN_ISSUE" ]; then
      gh issue close "$OPEN_ISSUE" \
        --repo kenstott/calcite \
        --comment "DQ passed on ${RUN_DATE} (${MODE} mode). Closing." \
        2>/dev/null && log_info "$WORKER_ID: closed DQ issue #${OPEN_ISSUE}" || \
        log_info "$WORKER_ID: WARNING: failed to close DQ issue #${OPEN_ISSUE}"
    fi
  else
    case "$VERDICT" in
      FAIL) VERDICT_LABEL="dq-fail" ;;
      *)    VERDICT_LABEL="dq-warn" ;;
    esac

    FINDINGS_TABLE=$(duckdb -noheader -list -c "
      SELECT '| ' || tbl || ' | ' || test || ' | ' || status || ' | ' ||
             COALESCE(CAST(value AS VARCHAR), '—') || ' | ' ||
             COALESCE(CAST(threshold AS VARCHAR), '—') || ' | ' ||
             COALESCE(REPLACE(detail, '|', '/'), '') || ' |'
      FROM read_parquet('${RESULT_LOCAL}')
      WHERE status != 'pass'
      ORDER BY status DESC, tbl, test
      LIMIT 50;" 2>/dev/null || echo "| (could not read findings) | | | | | |")

    FINDINGS_MD="| Table | Test | Status | Value | Threshold | Detail |
|-------|------|--------|-------|-----------|--------|
${FINDINGS_TABLE}"

    if [ -n "$OPEN_ISSUE" ]; then
      gh issue comment "$OPEN_ISSUE" \
        --repo kenstott/calcite \
        --body "**Re-run ${RUN_DATE}** — ${VERDICT}: ${FAIL_COUNT} fails, ${WARN_COUNT} warns

${FINDINGS_MD}

Results: \`${S3_RESULT_PATH}\`" \
        2>/dev/null && log_info "$WORKER_ID: commented on DQ issue #${OPEN_ISSUE}" || \
        log_info "$WORKER_ID: WARNING: failed to comment on DQ issue #${OPEN_ISSUE}"
    else
      gh issue create \
        --repo kenstott/calcite \
        --title "[DQ] ${SCHEMA}: ${VERDICT} — ${FAIL_COUNT} fails, ${WARN_COUNT} warns" \
        --label "dq" \
        --label "$VERDICT_LABEL" \
        --body "## DQ ${VERDICT}: \`${SCHEMA}\`

**Date:** ${RUN_DATE}
**Mode:** ${MODE}
**Fails:** ${FAIL_COUNT} | **Warns:** ${WARN_COUNT}

## Findings

${FINDINGS_MD}

## Results

\`${S3_RESULT_PATH}\`" \
        2>/dev/null && log_info "$WORKER_ID: created DQ issue for ${VERDICT}" || \
        log_info "$WORKER_ID: WARNING: failed to create DQ issue (gh not authenticated?)"
    fi
  fi
fi

_SCRIPT_COMPLETE=true
log_info "$WORKER_ID complete"

[ "$VERDICT" = "FAIL" ] && exit 1
exit 0
