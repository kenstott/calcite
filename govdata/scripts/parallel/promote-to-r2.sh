#!/usr/bin/env bash
#
# promote-to-r2.sh — sync validated pre-prod data from MinIO to R2.
#
# Runs after all schemas pass DQ on the MinIO pre-prod environment.
# Uses rclone sync --checksum so only new/changed files generate Class A
# PUT operations on R2. Existing unchanged files are skipped entirely.
#
# Usage:
#   govdata/scripts/parallel/promote-to-r2.sh [--dry-run] [--schema SCHEMA]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

_env_preprod="$SCRIPT_DIR/../../.env.preprod"
if [ ! -f "$_env_preprod" ]; then
  echo "ERROR: .env.preprod not found — promote-to-r2.sh must run on the MinIO machine" >&2
  exit 1
fi
set -a; source "$_env_preprod"; set +a

DRY_RUN=false
SCHEMA_FILTER=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --schema) shift; SCHEMA_FILTER="${1:?--schema requires a name}" ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
  shift
done

MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"

# Buckets to sync from MinIO → R2
BUCKETS=(
  "govdata-parquet-v1"
  "govdata-tracker-v1"
  "govdata-raw-v1"
)

_rclone_sync() {
  local src="$1" dst="$2" label="$3"
  local flags="--checksum --transfers 16 --checkers 32 --stats 30s"
  if $DRY_RUN; then
    flags="$flags --dry-run"
    log_info "promote: [DRY RUN] $label: $src → $dst"
  else
    log_info "promote: syncing $label: $src → $dst"
  fi
  rclone sync "$src" "$dst" $flags 2>&1 | while IFS= read -r line; do
    log_info "promote: [$label] $line"
  done
  log_info "promote: $label done"
}

log_info "promote: starting MinIO → R2 promotion ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"

for bucket in "${BUCKETS[@]}"; do
  if [ -n "$SCHEMA_FILTER" ]; then
    # Promote only a specific schema prefix within the parquet bucket
    _rclone_sync \
      "${MINIO_REMOTE}:${bucket}/${SCHEMA_FILTER}" \
      "${R2_REMOTE}:${bucket}/${SCHEMA_FILTER}" \
      "${bucket}/${SCHEMA_FILTER}"
  else
    _rclone_sync \
      "${MINIO_REMOTE}:${bucket}" \
      "${R2_REMOTE}:${bucket}" \
      "${bucket}"
  fi
done

log_info "promote: all buckets synced to R2"
