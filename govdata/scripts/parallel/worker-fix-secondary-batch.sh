#!/usr/bin/env bash
# ONE-TIME FIX: Re-materialize SEC secondary batch-staging files for years 2019-2026.
#
# Background: Before the batch filename parser fix in IcebergMaterializer, all
# metadata_batch_*.parquet files for a year were collapsed to a single "batch" accession
# key, causing only the last batch file's rows to survive in Iceberg (~100 rows instead
# of ~103k). After the fix, each file gets its own unique accession key.
#
# Pre-requisite: Run scripts/clear-batch-sentinel.sh for years 2019 through 2026 FIRST
# to remove the legacy "batch" sentinel entries from the S3 tracker. Otherwise the
# materializer will skip all batch files for years that already have the old sentinel.
#
#   ./clear-batch-sentinel.sh 2019 2020 2021 2022 2023 2024 2025 2026
#
# Then run this script to materialize the missing filings:
#
#   ./worker-fix-secondary-batch.sh            # default: 2019-2026
#   ./worker-fix-secondary-batch.sh 2022 2024  # specific range
#
# Each year is launched as a separate JVM in parallel. Memory per JVM: 4g max (per
# govdata JVM settings). Running all 8 years needs ~32g RAM — narrow the range if needed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

START_YEAR="${1:-2019}"
END_YEAR="${2:-2026}"
CURRENT_YEAR=$(date +%Y)

log_info "Starting SEC secondary batch-staging re-materialization: years ${START_YEAR}-${END_YEAR}"
log_info "Filing types: 8-K, 8-K/A, DEF 14A, 3, 4, 5, 13F-HR, 13F-HR/A, SC 13D, SC 13D/A, SC 13G, SC 13G/A"

PIDS=()
YEAR_FOR_PID=()

for YEAR in $(seq "$START_YEAR" "$END_YEAR"); do
  WORKER_ID="worker-fix-sec-secondary-${YEAR}"
  MODEL_DIR="$SCRIPT_DIR/runs/$WORKER_ID/models"
  mkdir -p "$MODEL_DIR"
  MODEL_FILE="$MODEL_DIR/sec-secondary-${YEAR}.json"

  # 2026 is still the current active year — use open-ended range up to CURRENT_YEAR
  if [[ "$YEAR" -ge 2026 && "$CURRENT_YEAR" -ge 2026 ]]; then
    generate_sec_secondary_model "$YEAR" "$CURRENT_YEAR" "$MODEL_FILE"
  else
    generate_sec_secondary_model "$YEAR" "$YEAR" "$MODEL_FILE"
  fi

  log_info "Launching $WORKER_ID"
  (run_etl "$MODEL_FILE" "$WORKER_ID") &
  PIDS+=($!)
  YEAR_FOR_PID+=("$YEAR")
done

log_info "All ${#PIDS[@]} workers launched — PIDs: ${PIDS[*]}"

FAILED=0
for i in "${!PIDS[@]}"; do
  PID="${PIDS[$i]}"
  YEAR="${YEAR_FOR_PID[$i]}"
  if wait "$PID"; then
    log_info "worker-fix-sec-secondary-${YEAR} (PID ${PID}) complete"
  else
    log_info "WARNING: worker-fix-sec-secondary-${YEAR} (PID ${PID}) exited with error — check runs/worker-fix-sec-secondary-${YEAR}/"
    FAILED=$((FAILED + 1))
  fi
done

if [[ "$FAILED" -eq 0 ]]; then
  log_info "All years ${START_YEAR}-${END_YEAR} complete successfully"
else
  log_info "FAILED: ${FAILED} year(s) had errors"
  exit 1
fi
