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

# Load the matching env (tracker database connection).
ENV_FILE="$ROOT/.env.${ENV_NAME}"
[ -f "$ENV_FILE" ] || { echo "ERROR: $ENV_FILE not found" >&2; exit 1; }
set -a; source "$ENV_FILE"; set +a

# Postgres is the canonical ETL state store. Its tracker tables live in a schema derived from the
# parquet bucket (PGPipelineTracker.sanitizeNamespace), which is what keeps dq and prod separate.
# shellcheck source=/dev/null
source "$SCRIPT_DIR/tracker_pg.sh"
if [ "$ENV_NAME" = "prod" ]; then
  PARQUET_DIR="${GOVDATA_PARQUET_DIR:?GOVDATA_PARQUET_DIR not set}"
else
  PARQUET_DIR="s3://${GOVDATA_DQ_BUCKET:?GOVDATA_DQ_BUCKET not set}"
fi
PG_NS="$(pg_ns_from_bucket "$PARQUET_DIR")" || exit 2

# One row per (source_key, table_name, phase) — the tracker upserts, so there is no marker history
# to reduce over and no year window to bound: the query below IS the current state.
# 'incremental' is the per-combo phase; the year lives inside source_key as "year=YYYY".
PARSED="WITH parsed AS (
  SELECT table_name, source_key, state, as_of, error_message,
         CAST(substring(source_key from 'year=([0-9]+)') AS INTEGER) AS yr
    FROM \"${PG_NS}\".pipeline_tracker
   WHERE phase = 'incremental'
)"

echo "=============================================================================="
echo " UNAVAILABLE (HTTP 404, backed off) — tracker schema: ${PG_NS}  [env=${ENV_NAME}]"
echo "=============================================================================="
pg_tracker_exec "${PARSED}
SELECT table_name,
       source_key,
       to_char(to_timestamp(as_of / 1000.0), 'YYYY-MM-DD HH24:MI') AS marked_at,
       substr(coalesce(error_message, ''), 1, 70) AS reason
  FROM parsed
 WHERE state = 'unavailable'
 ORDER BY table_name, yr DESC NULLS LAST, source_key;" || exit 2

echo
echo "=============================================================================="
echo " DIMENSION TIGHTEN-CANDIDATES"
echo "   leading_edge_unavailable = the newest requested year is 404 -> cap the dimension"
echo "   every_period_unavailable = ALL requested periods 404 -> URL-template bug, not a dimension"
echo "=============================================================================="
pg_tracker_exec "${PARSED},
per_table AS (
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
 ORDER BY t.every_period_unavailable DESC, leading_edge_unavailable DESC, t.table_name;" || exit 2
