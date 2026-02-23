#!/usr/bin/env bash
# Worker 29: SEC Secondary 2020 (8-K, Proxy, Insider)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-29"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_secondary_model 2020 2020 "$MODEL_DIR/sec-secondary-2020.json"
run_etl "$MODEL_DIR/sec-secondary-2020.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
