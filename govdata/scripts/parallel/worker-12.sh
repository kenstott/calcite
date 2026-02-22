#!/usr/bin/env bash
# Worker 12: SEC 2015
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-12"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_model 2015 2015 "$MODEL_DIR/sec-2015.json"
run_etl "$MODEL_DIR/sec-2015.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
