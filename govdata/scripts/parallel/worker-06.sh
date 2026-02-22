#!/usr/bin/env bash
# Worker 06: SEC 2021
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-06"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_model 2021 2021 "$MODEL_DIR/sec-2021.json"
run_etl "$MODEL_DIR/sec-2021.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
