#!/usr/bin/env bash
# ============================================================================
# Run DQ checks for all govdata schemas in parallel.
#
# Usage:
#   run-all-dq.sh [--mode daily|historical] [--dry-run] [--rebuild] [--schemas s1,s2,...]
#   run-all-dq.sh --help
#
# --mode daily|historical   DQ run type label (default: daily)
# --dry-run                 Run checks locally but do not upload to S3
# --rebuild                 For each schema: remove tracker entries + Iceberg data,
#                           run ETL to rebuild, then DQ (passed to worker-dq-run.sh)
# --schemas s1,s2,...       Comma-separated subset of schemas to run
#                           Default: all schemas with a *_dq.sql script
#
# Exit codes:
#   0  All schemas passed (or only warnings)
#   1  One or more schemas failed
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# ── argument parsing ──────────────────────────────────────────────────────────
MODE="daily"
DRY_RUN=false
REBUILD=false
SCHEMA_FILTER=""
START_YEAR=""

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
      START_YEAR="${1:?--start-year requires a 4-digit year}"
      ;;
    --schemas)
      shift
      SCHEMA_FILTER="${1:?--schemas requires a comma-separated list}"
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

if [[ "$MODE" != "daily" && "$MODE" != "historical" ]]; then
  echo "ERROR: --mode must be 'daily' or 'historical', got: $MODE" >&2
  exit 1
fi

# ── discover schemas ──────────────────────────────────────────────────────────
SCRIPTS_DIR="$GOVDATA_ROOT/scripts"
DQ_WORKER="$SCRIPT_DIR/worker-dq-run.sh"

if [ ! -f "$DQ_WORKER" ]; then
  echo "ERROR: worker-dq-run.sh not found at $DQ_WORKER" >&2
  exit 1
fi

if [ -n "$SCHEMA_FILTER" ]; then
  IFS=',' read -ra ALL_SCHEMAS <<< "$SCHEMA_FILTER"
else
  ALL_SCHEMAS=()
  for f in "$SCRIPTS_DIR"/*_dq.sql; do
    base=$(basename "$f" _dq.sql)
    ALL_SCHEMAS+=("$base")
  done
fi

if [ ${#ALL_SCHEMAS[@]} -eq 0 ]; then
  echo "ERROR: no *_dq.sql scripts found in $SCRIPTS_DIR" >&2
  exit 1
fi

log_info "run-all-dq: mode=$MODE dry_run=$DRY_RUN rebuild=$REBUILD schemas=${ALL_SCHEMAS[*]}"

# ── build worker args ─────────────────────────────────────────────────────────
WORKER_EXTRA_ARGS="--mode $MODE"
$DRY_RUN    && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --dry-run"
$REBUILD    && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --rebuild"
[ -n "$START_YEAR" ] && WORKER_EXTRA_ARGS="$WORKER_EXTRA_ARGS --start-year $START_YEAR"

# ── launch workers in parallel ────────────────────────────────────────────────
LOG_DIR="$SCRIPT_DIR/runs/dq-all-$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

pids=()
schema_list=()

for schema in "${ALL_SCHEMAS[@]}"; do
  log_file="$LOG_DIR/${schema}.log"
  # shellcheck disable=SC2086
  bash "$DQ_WORKER" "$schema" $WORKER_EXTRA_ARGS > "$log_file" 2>&1 &
  last_pid=$!
  pids+=($last_pid)
  schema_list+=("$schema")
  log_info "run-all-dq: launched $schema (PID $last_pid)"
done

# ── collect results ───────────────────────────────────────────────────────────
overall_exit=0
exit_codes=()

for i in "${!pids[@]}"; do
  pid="${pids[$i]}"
  if wait "$pid"; then
    exit_codes+=( 0 )
  else
    exit_codes+=( 1 )
    overall_exit=1
  fi
done

# ── print summary ─────────────────────────────────────────────────────────────
echo ""
echo "=== DQ Run Summary (mode=$MODE) ==="
printf "%-30s  %s\n" "SCHEMA" "RESULT"
printf "%-30s  %s\n" "------------------------------" "------"
for i in "${!schema_list[@]}"; do
  schema="${schema_list[$i]}"
  if [ "${exit_codes[$i]}" -eq 0 ]; then
    printf "%-30s  PASS\n" "$schema"
  else
    printf "%-30s  FAIL\n" "$schema"
  fi
done
echo ""

if [ $overall_exit -eq 0 ]; then
  log_info "run-all-dq: ALL SCHEMAS PASSED"
else
  log_info "run-all-dq: ONE OR MORE SCHEMAS FAILED"
fi

log_info "run-all-dq: logs in $LOG_DIR"
exit $overall_exit
