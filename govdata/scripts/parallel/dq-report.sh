#!/usr/bin/env bash
# ============================================================================
# DQ Results Report — reads all Parquet DQ result files from S3 and prints
# a schema-level pass/fail summary plus failing test details.
#
# Usage:
#   dq-report.sh [--run-date YYYY-MM-DD] [--mode daily|historical]
#                [--schemas s1,s2,...] [--show-warns] [--show-pass]
#   dq-report.sh --help
#
# --run-date YYYY-MM-DD   Read results for this date (default: today)
# --mode daily|historical Filter by run type (default: show both)
# --schemas s1,s2,...     Comma-separated schema subset (default: all)
# --show-warns            Include warnings in the detail table
# --show-pass             Include passing tests in the detail table
#
# Reads:  s3://govdata-tracker-v1/dq-results/schema=*/run_date=*/type=*/results.parquet
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# ── argument parsing ──────────────────────────────────────────────────────────
RUN_DATE=$(date +%Y-%m-%d)
MODE_FILTER=""
SCHEMA_FILTER=""
SHOW_WARNS=false
SHOW_PASS=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-date)
      shift
      RUN_DATE="${1:?--run-date requires YYYY-MM-DD}"
      ;;
    --mode)
      shift
      MODE_FILTER="${1:?--mode requires daily or historical}"
      ;;
    --schemas)
      shift
      SCHEMA_FILTER="${1:?--schemas requires a comma-separated list}"
      ;;
    --show-warns)
      SHOW_WARNS=true
      ;;
    --show-pass)
      SHOW_PASS=true
      ;;
    --help|-h)
      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

# ── S3 config ─────────────────────────────────────────────────────────────────
S3_ENDPOINT="${AWS_ENDPOINT_OVERRIDE:-}"
ENDPOINT_ARGS=""
if [ -n "$S3_ENDPOINT" ]; then
  [[ "$S3_ENDPOINT" != http* ]] && S3_ENDPOINT="https://${S3_ENDPOINT}"
  ENDPOINT_ARGS="--endpoint-url $S3_ENDPOINT"
fi

RESULT_BASE="s3://govdata-tracker-v1/dq-results"
GLOB_PATH="${RESULT_BASE}/schema=*/run_date=${RUN_DATE}/type=*/results.parquet"

# ── build WHERE clause fragments ──────────────────────────────────────────────
WHERE_CLAUSES=()
[ -n "$MODE_FILTER" ]   && WHERE_CLAUSES+=("run_type = '${MODE_FILTER}'")
[ -n "$SCHEMA_FILTER" ] && WHERE_CLAUSES+=("schema_name IN ($(echo "$SCHEMA_FILTER" | sed "s/\([^,]*\)/'\1'/g"))")

WHERE_SQL=""
if [ ${#WHERE_CLAUSES[@]} -gt 0 ]; then
  WHERE_SQL="WHERE $(IFS=' AND '; echo "${WHERE_CLAUSES[*]}")"
fi

DETAIL_STATUS_FILTER="status IN ('fail'"
$SHOW_WARNS && DETAIL_STATUS_FILTER="${DETAIL_STATUS_FILTER}, 'warn'"
$SHOW_PASS  && DETAIL_STATUS_FILTER="${DETAIL_STATUS_FILTER}, 'pass'"
DETAIL_STATUS_FILTER="${DETAIL_STATUS_FILTER})"

# ── run report via DuckDB ─────────────────────────────────────────────────────
duckdb <<SQL
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;

SET s3_access_key_id     = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint          = '${AWS_ENDPOINT_OVERRIDE:-21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com}';
SET s3_region            = 'auto';

-- ── schema-level verdict ─────────────────────────────────────────────────────
.print ''
.print '=== DQ Schema Summary  run_date=${RUN_DATE} ==='

SELECT
  schema_name                                                  AS schema,
  run_type                                                     AS mode,
  COUNT(*)                                                     AS total_tests,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)            AS pass,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END)            AS warn,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)            AS fail,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END                                                          AS verdict
FROM read_parquet('${GLOB_PATH}')
${WHERE_SQL}
GROUP BY schema_name, run_type
ORDER BY verdict DESC, schema_name;

-- ── failing (and optionally warning/passing) test details ────────────────────
.print ''
.print '=== DQ Test Details ==='

SELECT
  schema_name  AS schema,
  table_name,
  test,
  status,
  value,
  threshold,
  detail
FROM read_parquet('${GLOB_PATH}')
${WHERE_SQL}
  $( [ -n "$WHERE_SQL" ] && echo "AND ${DETAIL_STATUS_FILTER}" || echo "WHERE ${DETAIL_STATUS_FILTER}" )
ORDER BY schema_name, table_name, test;
SQL
