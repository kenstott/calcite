#!/usr/bin/env bash
# Worker 04: SEC 2023
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-04"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_model 2023 2023 "$MODEL_DIR/sec-2023.json"
run_etl "$MODEL_DIR/sec-2023.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
