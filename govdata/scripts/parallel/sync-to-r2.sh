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
# Runs automatically once per day from run-pool.sh when .env.preprod exists.
# Can also be run manually: govdata/scripts/parallel/sync-to-r2.sh [--dry-run]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

_env_preprod="$SCRIPT_DIR/../../.env.preprod"
if [ ! -f "$_env_preprod" ]; then
  echo "ERROR: .env.preprod not found" >&2
  exit 1
fi
set -a; source "$_env_preprod"; set +a

DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && DRY_RUN=true

MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"

# MinIO production buckets → R2 production buckets (same names)
BUCKETS=(
  "govdata-parquet-v1"
  "govdata-tracker-v1"
)

_flags="--update --transfers 16 --checkers 32 --stats 60s"
$DRY_RUN && _flags="$_flags --dry-run"

log_info "sync-to-r2: starting daily sync ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"

for bucket in "${BUCKETS[@]}"; do
  log_info "sync-to-r2: $bucket"
  rclone copy "${MINIO_REMOTE}:${bucket}" "${R2_REMOTE}:${bucket}" \
    $_flags 2>&1 | while IFS= read -r line; do
    log_info "sync-to-r2: [$bucket] $line"
  done
  log_info "sync-to-r2: $bucket done"
done

log_info "sync-to-r2: complete"
