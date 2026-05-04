#!/usr/bin/env bash
# ============================================================================
# Daily update runner — ETL pool followed by embeddings refresh.
#
# Usage:
#   ./run-daily.sh                 — run all recurring workers, then embeddings
#   ./run-daily.sh --etl-only      — skip embeddings pass
#   ./run-daily.sh --embed-only    — skip ETL pool (embeddings only)
#
# Passes any extra flags (e.g. -j, -t, -r, -p) through to run-pool.sh.
#
# Required env vars (set in .env.prod):
#   GOVDATA_PARQUET_DIR, GOVDATA_CACHE_DIR
#
# Optional env vars for embeddings:
#   VSS_YEARS          — which years to embed (default: current year only)
#   VSS_GPU_PLAN       — Vultr GPU plan (default: vcg-a100-1c-6g-4vram)
#   VSS_GPU_REGION     — Vultr region (default: ewr)
#   VSS_DRY_RUN        — set to 1 to skip GPU instance creation
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VSS_SCRIPT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

RUN_ETL=true
RUN_EMBED=true
POOL_ARGS=()

for arg in "$@"; do
  case "$arg" in
    --etl-only)   RUN_EMBED=false ;;
    --embed-only) RUN_ETL=false ;;
    *)            POOL_ARGS+=("$arg") ;;
  esac
done

# Default: run current year only for incremental embedding refresh
CURRENT_YEAR=$(date +%Y)
export VSS_YEARS="${VSS_YEARS:-$CURRENT_YEAR}"

# ── ETL pool ──────────────────────────────────────────────────────────────────

if $RUN_ETL; then
  log_info "run-daily: starting ETL pool (daily)"
  "$SCRIPT_DIR/run-pool.sh" "${POOL_ARGS[@]:-}" daily
  log_info "run-daily: ETL pool complete"
fi

# ── Embeddings ────────────────────────────────────────────────────────────────

if $RUN_EMBED; then
  log_info "run-daily: starting embeddings refresh for year(s): $VSS_YEARS"

  if [ -f "$VSS_SCRIPT_DIR/vss-gpu-runner.sh" ]; then
    bash "$VSS_SCRIPT_DIR/vss-gpu-runner.sh"
  else
    log_info "run-daily: vss-gpu-runner.sh not found — skipping GPU embedding pass"
  fi

  if [ -f "$VSS_SCRIPT_DIR/vss.sh" ]; then
    bash "$VSS_SCRIPT_DIR/vss.sh" refresh "$VSS_YEARS"
    bash "$VSS_SCRIPT_DIR/vss.sh" upload
  else
    log_info "run-daily: vss.sh not found — skipping VSS refresh/upload"
  fi

  log_info "run-daily: embeddings complete"
fi

log_info "run-daily: done"
