#!/usr/bin/env bash
#
# sync-to-r2.sh — copy new Iceberg files from MinIO to R2.
#
# rclone copy skips files already on R2 with matching size+modtime. Since
# Iceberg is append-only (new snapshots only add files, never modify existing
# ones), a --max-age window bounded by the last-sync sentinel copies only the
# new parquet data + metadata files from each incremental ETL run, with zero
# LIST/GET (Class A/B) ops on R2.
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

# Bound the copy set by source modification time via --max-age so we never
# LIST or GET on R2. --max-age selects files *younger* than the window, i.e.
# everything written since the last sync (Iceberg is append-only, so that is
# exactly the new snapshot files).
_now=$(date +%s)

# Only the parquet data bucket is published. The old S3 tracker bucket
# (govdata-tracker-v1) is deprecated — pipeline state now lives in Postgres —
# so it no longer exists on the source and must not be synced.
BUCKETS=(
  "govdata-parquet-v1"
)

_flags="--transfers 16 --stats 60s"
# Read the sentinel and validate it holds a positive-integer epoch before trusting
# it as an incremental boundary. A missing, empty, or corrupt sentinel must fall
# through to a full cold-start copy — never be treated as a boundary. (An empty
# file passed the old `[ -f ]` guard and only worked by luck: `$(( _now - "" ))`
# degrades to a ~now-second window ≈ full copy. A non-numeric value would make the
# arithmetic below throw and, under `set -e`, abort the entire sync.)
_last=""
[ -f "$SYNC_STAMP" ] && _last=$(cat "$SYNC_STAMP" 2>/dev/null)
if [[ "$_last" =~ ^[0-9]+$ ]]; then
  _elapsed=$(( _now - _last ))
  # Add 60s buffer so files written right at the previous boundary aren't missed.
  _age=$(( _elapsed + 60 ))
  _flags="--max-age ${_age}s $_flags"
  log_info "sync-to-r2: incremental — files newer than ${_age}s ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"
else
  # First run (or missing/empty/corrupt sentinel): copy the full backlog. rclone
  # copy already skips files present on R2 with matching size+modtime, so a cold
  # start re-transfers nothing that is already there.
  [ -n "$_last" ] && log_info "sync-to-r2: WARNING — ignoring non-numeric sentinel ('$_last'); treating as cold start"
  log_info "sync-to-r2: cold start (no valid sentinel) — full copy ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"
fi
$DRY_RUN && _flags="$_flags --dry-run"

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
