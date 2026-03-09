#!/usr/bin/env bash
# Worker 33: SEC Secondary (8-K, Proxy, Insider, 13F, 13D/G)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-33"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_secondary_model 2016 2016 "$MODEL_DIR/sec-secondary-2016.json"
run_etl "$MODEL_DIR/sec-secondary-2016.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
