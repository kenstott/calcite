#!/usr/bin/env bash
#
# sync-to-r2.sh — copy new Iceberg files from MinIO to R2.
#
# Uses rclone copy --update which skips files that already exist on R2
# with an equal or newer modification time. Since Iceberg is append-only
# (new snapshots only add new files, never modify existing ones), this
# copies only the new parquet data files and metadata files from each
# incremental ETL run. Zero Class A ops for files already on R2.
#
# Runs automatically once per day from run-scheduled.sh when PROD_* publish creds are set.
# Can also be run manually: govdata/scripts/parallel/sync-to-r2.sh [--dry-run]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# R2 destination is built from the PROD_* creds in .env.prod (no .env.preprod, no rclone.conf).
configure_r2_remote

DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && DRY_RUN=true

MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"
SYNC_STAMP="${HOME}/.r2-last-sync"

# Compute --min-age from the local sentinel so we only look at source
# modification times — no LIST or GET ops on R2 at all.
_now=$(date +%s)
_last=$(cat "$SYNC_STAMP" 2>/dev/null || echo 0)
_elapsed=$(( _now - _last ))
# Add 60s buffer so files written right at the sentinel boundary aren't missed.
_age=$(( _elapsed > 60 ? _elapsed - 60 : 0 ))

BUCKETS=(
  "govdata-parquet-v1"
  "govdata-tracker-v1"
)

_flags="--min-age ${_age}s --transfers 16 --stats 60s"
$DRY_RUN && _flags="$_flags --dry-run"

log_info "sync-to-r2: syncing files newer than ${_age}s ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"

for bucket in "${BUCKETS[@]}"; do
  log_info "sync-to-r2: $bucket"
  rclone copy "${MINIO_REMOTE}:${bucket}" "${R2_REMOTE}:${bucket}" \
    $_flags 2>&1 | while IFS= read -r line; do
    log_info "sync-to-r2: [$bucket] $line"
  done
  log_info "sync-to-r2: $bucket done"
done

# Update sentinel only on live runs
if ! $DRY_RUN; then
  echo "$_now" > "$SYNC_STAMP"
fi

log_info "sync-to-r2: complete"
