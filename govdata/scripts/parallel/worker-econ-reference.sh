#!/usr/bin/env bash
# Economic Reference Data ETL worker.
#
# Single-mode: runs all 7 reference tables (jolts_industries, jolts_dataelements,
# bls_geographies, naics_sectors, nipa_tables, regional_linecodes, fred_series).
# Release windows and TTL are defined per-table in econ-reference-schema.yaml —
# each table gates its own refresh without a mode parameter.
#
# Usage:
#   worker-econ-reference.sh [--force]
#
# Required env vars (set in .env.prod or equivalent):
#   GOVDATA_PARQUET_DIR     Local/S3 path for Parquet output
#   GOVDATA_CACHE_DIR       Local/S3 path for raw download cache
#
# Optional env vars:
#   BEA_API_KEY             BEA API key (required for nipa_tables, regional_linecodes)
#   FRED_API_KEY            FRED API key (required for fred_series)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

FORCE=${FORCE:-false}
for arg in "${@:1}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

WORKER_ID="worker-econ-reference"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_single_schema_model "econ_reference" "$MODEL_DIR/econ-reference.json"

log_info "$WORKER_ID: running all reference tables"
run_etl "$MODEL_DIR/econ-reference.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
