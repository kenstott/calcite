#!/usr/bin/env bash
# Worker 13: SEC 2014
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-13"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_model 2014 2014 "$MODEL_DIR/sec-2014.json"
run_etl "$MODEL_DIR/sec-2014.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
