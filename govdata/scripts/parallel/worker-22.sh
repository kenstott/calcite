#!/usr/bin/env bash
# Worker 22: Weather/Climate Data (NWS, NOAA, EPA)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-22"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_single_schema_model "weather" "$MODEL_DIR/weather.json"
run_etl "$MODEL_DIR/weather.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
