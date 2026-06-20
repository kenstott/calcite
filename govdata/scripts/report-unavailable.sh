#!/usr/bin/env bash
#
# report-unavailable.sh — show every combo currently backed off as HTTP-404 "unavailable"
# in the pipeline tracker, plus dimension tighten-candidates.
#
# A 404 is not an error and not a bad URI — it means a reachable, well-formed endpoint has no
# such resource (the dimension reached for a period/entity that is not published). EtlPipeline
# records these as state="unavailable" markers and skips them within errorHandling.notFoundRetryDays
# instead of re-requesting every run. This report surfaces that list so you can decide whether to
# tighten a dimension (cap its range) — or, when EVERY period of a table is unavailable, fix the URL
# template (that is effectively a bad URI, not a dimension problem).
#
# Usage: report-unavailable.sh [--env dq|prod]   (default: dq)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ENV_NAME="dq"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) shift; ENV_NAME="${1:?--env requires dq|prod}";;
    -h|--help) sed -n '2,14p' "$0"; exit 0;;
    *) echo "Unknown arg: $1" >&2; exit 1;;
  esac
  shift
done

# Load the matching env (object-store creds + tracker bucket + rclone remote).
ENV_FILE="$ROOT/.env.${ENV_NAME}"
[ -f "$ENV_FILE" ] || { echo "ERROR: $ENV_FILE not found" >&2; exit 1; }
set -a; source "$ENV_FILE"; set +a

if [ "$ENV_NAME" = "prod" ]; then
  BUCKET="${CALCITE_TRACKER_S3_BUCKET:?CALCITE_TRACKER_S3_BUCKET not set}"
  REMOTE="${GOVDATA_PROD_RCLONE_REMOTE:-r2:}"
else
  BUCKET="${GOVDATA_DQ_TRACKER_BUCKET:?GOVDATA_DQ_TRACKER_BUCKET not set}"
  REMOTE="${GOVDATA_DQ_RCLONE_REMOTE:-minio:}"
fi
# Normalize to exactly one trailing ':' (the env var may omit it).
REMOTE="${REMOTE%:}:"

# Pull the recent-year tracker markers local and query them with DuckDB over local files.
# Why local: reading the markers directly over MinIO via httpfs hits a DuckDB S3 range-read bug,
# and inlining object-store creds into a `duckdb -c` command leaks them into `ps`. Copying local
# with rclone avoids both. A 404 "unavailable" marker only occurs on a current/future period (an
# already-published past year does not 404), so a recent-year window is both complete and fast.
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
CUR="$(date +%Y)"
INCLUDES=()
for y in $((CUR - 1)) "$CUR" $((CUR + 1)) $((CUR + 2)); do
  INCLUDES+=(--include "*/year=${y}/**/*.parquet")
done
echo "Fetching recent-year tracker markers from ${REMOTE}${BUCKET} (years $((CUR-1))-$((CUR+2))) ..." >&2
rclone copy "${REMOTE}${BUCKET}/" "$TMP/" "${INCLUDES[@]}" --transfers 16 --checkers 16 2>/dev/null || true

if ! find "$TMP" -name '*.parquet' -print -quit 2>/dev/null | grep -q .; then
  echo "No recent-year tracker markers found — nothing to report."
  exit 0
fi

# Latest marker per (source_key, table_name); 'incremental' is the per-combo phase.
SETUP="CREATE OR REPLACE TEMP VIEW latest AS
SELECT * FROM (
  SELECT source_key, table_name, state, as_of, error_message,
         ROW_NUMBER() OVER (PARTITION BY source_key, table_name ORDER BY as_of DESC) AS rn
  FROM read_parquet('${TMP}/**/*.parquet', union_by_name=true)
  WHERE phase = 'incremental'
) WHERE rn = 1;
CREATE OR REPLACE TEMP VIEW parsed AS
SELECT table_name, source_key, state, as_of, error_message,
       TRY_CAST(regexp_extract(source_key, 'year=([0-9]+)', 1) AS INTEGER) AS yr
FROM latest;"

echo "=============================================================================="
echo " UNAVAILABLE (HTTP 404, backed off) — tracker bucket: ${BUCKET}  [env=${ENV_NAME}]"
echo "=============================================================================="
duckdb -box -c "${SETUP}
SELECT table_name,
       source_key,
       strftime(make_timestamp(as_of * 1000), '%Y-%m-%d %H:%M') AS marked_at,
       substr(coalesce(error_message, ''), 1, 70) AS reason
FROM parsed
WHERE state = 'unavailable'
ORDER BY table_name, yr DESC NULLS LAST, source_key;"

echo
echo "=============================================================================="
echo " DIMENSION TIGHTEN-CANDIDATES"
echo "   leading_edge_unavailable = the newest requested year is 404 -> cap the dimension"
echo "   every_period_unavailable = ALL requested periods 404 -> URL-template bug, not a dimension"
echo "=============================================================================="
duckdb -box -c "${SETUP}
WITH per_table AS (
  SELECT table_name, MAX(yr) AS max_year, COUNT(*) AS total_combos,
         COUNT(*) FILTER (WHERE state = 'unavailable') AS unavailable_combos,
         bool_and(state = 'unavailable') AS every_period_unavailable
  FROM parsed GROUP BY table_name
),
edge AS (
  SELECT p.table_name,
         bool_and(p.state = 'unavailable') AS leading_edge_unavailable
  FROM parsed p JOIN per_table t
    ON p.table_name = t.table_name AND p.yr = t.max_year
  GROUP BY p.table_name
)
SELECT t.table_name, t.max_year,
       t.unavailable_combos, t.total_combos,
       coalesce(e.leading_edge_unavailable, false) AS leading_edge_unavailable,
       t.every_period_unavailable
FROM per_table t LEFT JOIN edge e ON t.table_name = e.table_name
WHERE t.unavailable_combos > 0
ORDER BY t.every_period_unavailable DESC, leading_edge_unavailable DESC, t.table_name;"
