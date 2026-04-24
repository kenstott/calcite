#!/usr/bin/env bash
# Worker 02: SEC Primary 2025 (10-K/10-Q)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-02"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

generate_sec_primary_model 2025 2025 "$MODEL_DIR/sec-primary-2025.json"
run_etl "$MODEL_DIR/sec-primary-2025.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
