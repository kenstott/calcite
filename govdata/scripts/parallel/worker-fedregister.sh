#!/usr/bin/env bash
# Federal Register ETL worker — parameterized by MODE.
#
# Usage:
#   worker-fedregister.sh <historical|daily> [--force]
#   (or invoked by run-pool.sh which sets GOVDATA_RUN_MODE=historical|daily)
#
# Modes:
#   historical  One-time backfill: all FR documents from GOVDATA_START_YEAR (default 2010)
#               through GOVDATA_INCREMENTAL_START_YEAR - 1, across all 4 doc_types.
#               fr_agencies (static reference) is also ingested.
#
#   daily       Recurring cadence: current year only (GOVDATA_INCREMENTAL_START_YEAR).
#               fr_documents re-fetched monthly per incrementalTtlDays=30 in the schema.
#               fr_agencies re-fetched per incrementalTtlDays=90.
#
# Required env vars (set in .env.prod or equivalent):
#   GOVDATA_PARQUET_DIR              Root Parquet directory
#   GOVDATA_CACHE_DIR                Root cache directory
#   GOVDATA_START_YEAR               Historical start year (default 2010)
#   GOVDATA_INCREMENTAL_START_YEAR   First year of incremental / daily window
#
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

export GOVDATA_RUN_MODE="$MODE"

WORKER_ID="worker-fedregister-${MODE}"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_single_schema_model "fedregister" "$MODEL_DIR/fedregister-${MODE}.json"

log_info "$WORKER_ID: running all fedregister tables (mode=$MODE)"
run_etl "$MODEL_DIR/fedregister-${MODE}.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
