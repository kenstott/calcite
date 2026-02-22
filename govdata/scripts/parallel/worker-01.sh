#!/usr/bin/env bash
# Worker 01: SEC 2026 + all non-SEC sources (econ, census, geo, crime, weather)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-01"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

# Phase 1: SEC filings 2026
generate_sec_model 2026 2026 "$MODEL_DIR/sec-2026.json"
run_etl "$MODEL_DIR/sec-2026.json" "$WORKER_ID"

# Phase 2: Non-SEC sources
generate_nonsec_model "$MODEL_DIR/nonsec.json"
run_etl "$MODEL_DIR/nonsec.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
