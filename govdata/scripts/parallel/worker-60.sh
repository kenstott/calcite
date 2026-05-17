#!/usr/bin/env bash
# Worker 60: FEC schema (campaign finance bulk downloads)
#
# Usage:
#   worker-60.sh [mode] [--force]
#   (or invoked by run-pool.sh which sets GOVDATA_RUN_MODE=historical|daily)
#
# Modes:
#   historical  One-time backfill: all FEC election cycles (2010-2026).
#
#   daily       Recurring cadence: current active cycle only (GOVDATA_INCREMENTAL_START_YEAR).
#               FEC bulk files are updated throughout the active cycle; incrementalTtlDays=30
#               in the schema causes the current cycle to be re-fetched monthly.
#
# Required env vars:
#   GOVDATA_PARQUET_DIR              Root Parquet directory
#   GOVDATA_CACHE_DIR                Root cache directory
#   GOVDATA_INCREMENTAL_START_YEAR   Current active election cycle year (e.g. 2026)
#
# GOVDATA_START_YEAR is unset before run_etl so that GovDataSchemaFactory's
# System.setProperty("GOVDATA_START_YEAR", startYear) takes effect in VariableResolver,
# which checks env before system properties.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# Accept positional mode arg, or fall back to GOVDATA_RUN_MODE env var
MODE="${1:-${GOVDATA_RUN_MODE:-}}"
if [ -z "$MODE" ]; then
  echo "Usage: $0 <historical|daily> [--force]  (or set GOVDATA_RUN_MODE)" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:2}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-60-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-2026}

case "$MODE" in

  historical)
    generate_fec_model "$MODEL_DIR/fec-historical.json" 2010
    # Unset env var so GovDataSchemaFactory's system property (startYear=2010) governs
    # minYear resolution — VariableResolver checks env before system property.
    unset GOVDATA_START_YEAR
    run_etl "$MODEL_DIR/fec-historical.json" "$WORKER_ID"
    ;;

  daily)
    generate_fec_model "$MODEL_DIR/fec-daily.json" "$INCREMENTAL_YEAR"
    # Unset env var so GovDataSchemaFactory's system property (startYear=INCREMENTAL_YEAR)
    # governs minYear resolution — filters fec_election_cycles to current cycle only.
    unset GOVDATA_START_YEAR
    run_etl "$MODEL_DIR/fec-daily.json" "$WORKER_ID"
    ;;

  *)
    echo "Unknown mode: $MODE. Valid modes: historical, daily" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
