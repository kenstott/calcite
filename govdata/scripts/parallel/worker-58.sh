#!/usr/bin/env bash
# Worker 58: SEC Compact-Only 2010 (all filing types)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

WORKER_ID="worker-58"
MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
mkdir -p "$MODEL_DIR"

# Compact primary filings (10-K/10-Q)
generate_sec_primary_model 2010 2010 "$MODEL_DIR/sec-primary-2010.json"
ETL_COMPACT_ONLY=true run_etl "$MODEL_DIR/sec-primary-2010.json" "$WORKER_ID"

# Compact secondary filings (8-K, proxy, insider, 13F, 13D/G)
generate_sec_secondary_model 2010 2010 "$MODEL_DIR/sec-secondary-2010.json"
ETL_COMPACT_ONLY=true run_etl "$MODEL_DIR/sec-secondary-2010.json" "$WORKER_ID"

log_info "$WORKER_ID complete"
