#!/usr/bin/env bash
# Geographic Data ETL worker.
#
# Single-mode: runs all TIGER boundary tables, HUD crosswalk tables,
# USDA classification tables, Census Gazetteer tables, and USGS Watershed
# Boundary tables. TTL and release windows are defined per-table in
# geo-schema.yaml — each table gates its own refresh without a mode parameter.
#
# Usage:
#   worker-geo.sh [--force]
#
# Required env vars (set in .env.prod or equivalent):
#   GOVDATA_PARQUET_DIR     Local/S3 path for Parquet output
#   GOVDATA_CACHE_DIR       Local/S3 path for raw download cache
#
# Optional env vars:
#   HUD_TOKEN               HUD API bearer token (required for HUD crosswalk tables)
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

WORKER_ID="worker-geo"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_single_schema_model "geo" "$MODEL_DIR/geo.json"

log_info "$WORKER_ID: running all geo tables"
run_etl "$MODEL_DIR/geo.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
