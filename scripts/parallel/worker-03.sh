#!/usr/bin/env bash
# Worker 03: SEC 2024
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-03"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_model 2024 2024 "$MODEL_DIR/sec-2024.json"
run_etl "$MODEL_DIR/sec-2024.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
