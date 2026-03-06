#!/usr/bin/env bash
# Worker 35: SEC Secondary (8-K, Proxy, Insider, 13F, 13D/G)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-35"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_secondary_model 2014 2014 "$MODEL_DIR/sec-secondary-2014.json"
ETL_NO_COMPACT=true run_etl "$MODEL_DIR/sec-secondary-2014.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
