#!/usr/bin/env bash
# Worker 21: Crime Statistics (FBI, BJS)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-21"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_single_schema_model "crime" "$MODEL_DIR/crime.json"
run_etl "$MODEL_DIR/crime.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
