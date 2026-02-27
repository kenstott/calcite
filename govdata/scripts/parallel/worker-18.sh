#!/usr/bin/env bash
# Worker 18: Economic Data (BLS, FRED, BEA, Treasury)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-18"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

export ETL_HEAP_MIN="2g"
export ETL_HEAP_MAX="4g"

generate_single_schema_model "econ" "$MODEL_DIR/econ.json"
run_etl "$MODEL_DIR/econ.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
