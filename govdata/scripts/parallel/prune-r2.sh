#!/usr/bin/env bash
#
# prune-r2.sh — reclaim space on R2 by deleting objects that no longer exist on the MinIO source.
#
# The daily publisher (sync-to-r2.sh) uses `rclone copy`, which NEVER deletes on R2. Iceberg
# maintenance (compaction, snapshot expiry, orphan cleanup) removes superseded data files,
# manifests and old metadata from MinIO over time, but those removals are never mirrored, so R2
# accumulates dead files indefinitely. This script reconciles R2 down to match MinIO with
# `rclone sync`, reclaiming that space.
#
# ── DESTRUCTIVE — read before running ────────────────────────────────────────────────────────
#   * Defaults to a DRY RUN. Nothing is deleted until you pass --confirm. Always review the
#     dry-run first.
#   * It MIRRORS SOURCE DELETIONS: any object absent from MinIO is removed from R2, including any
#     file that was deleted from MinIO *by mistake*. Because `copy` never deletes, R2 is currently
#     the only place such files still exist, so it can be a data-recovery source — do NOT prune a
#     table you may still need to recover until it has been re-ingested and verified on MinIO.
#   * `rclone sync` also RE-UPLOADS anything on MinIO but missing from R2 (normally a no-op if the
#     daily copy is current) — so after a prune R2 is a faithful mirror of MinIO, not just smaller.
#   * Unlike sync-to-r2.sh this must LIST R2 (Class A/B ops) to find what to delete. That is the
#     price of pruning; run it occasionally, not on a schedule.
#
# Best run while the ETL pool is idle (it warns otherwise): pruning the R2 mirror never touches
# MinIO or blocks ETL, but keeping the source stable makes the dry-run you reviewed match the run.
#
# Usage:
#   govdata/scripts/parallel/prune-r2.sh                 # DRY RUN (default) — shows what would go
#   govdata/scripts/parallel/prune-r2.sh --confirm       # actually delete
#   govdata/scripts/parallel/prune-r2.sh --bucket X      # restrict to one bucket
#   govdata/scripts/parallel/prune-r2.sh --max-delete N  # abort if more than N deletions (safety)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# R2 destination is built from the PROD_* creds in .env.prod (same as sync-to-r2.sh).
configure_r2_remote

CONFIRM=false
MAX_DELETE=""
BUCKET_OVERRIDE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --confirm|--yes) CONFIRM=true ;;
    --dry-run)       CONFIRM=false ;;
    --bucket)        shift; BUCKET_OVERRIDE="${1:-}" ;;
    --max-delete)    shift; MAX_DELETE="${1:-}" ;;
    *) echo "prune-r2: unknown argument '$1'" >&2
       echo "Usage: $0 [--confirm] [--bucket <name>] [--max-delete N]" >&2
       exit 2 ;;
  esac
  shift
done

MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"

# Only the parquet data bucket is published (see sync-to-r2.sh).
BUCKETS=( "govdata-parquet-v1" )
[ -n "$BUCKET_OVERRIDE" ] && BUCKETS=( "$BUCKET_OVERRIDE" )

# Warn (do not block) if ETL workers are running: pruning the R2 mirror is safe against a live
# pool (ETL reads/writes MinIO, never R2), but a moving source means the dry-run you reviewed may
# no longer match what gets deleted.
if pgrep -fa 'EtlRunner|parallel/worker\.sh' >/dev/null 2>&1; then
  log_info "prune-r2: WARNING — ETL workers appear to be running; source is moving. Prefer an idle pool."
fi

_flags="--delete-during --transfers 16 --stats 60s"
$CONFIRM || _flags="$_flags --dry-run"
if [ -n "$MAX_DELETE" ]; then
  [[ "$MAX_DELETE" =~ ^[0-9]+$ ]] || { echo "prune-r2: --max-delete must be an integer" >&2; exit 2; }
  _flags="$_flags --max-delete $MAX_DELETE"
fi

log_info "prune-r2: $( $CONFIRM && echo 'LIVE — will DELETE R2 objects not on MinIO' || echo 'DRY RUN — no deletions; review output, then re-run with --confirm')"

for bucket in "${BUCKETS[@]}"; do
  # Guard: never reconcile against an empty or unreachable source — that would delete the entire
  # R2 bucket. Require the source to list at least one top-level entry before proceeding.
  if ! rclone lsf "${MINIO_REMOTE}:${bucket}" --max-depth 1 2>/dev/null | grep -q .; then
    log_info "prune-r2: ABORT — source '${MINIO_REMOTE}:${bucket}' is empty or unreachable; refusing to prune R2 (would wipe the bucket)."
    exit 1
  fi

  log_info "prune-r2: reconciling $bucket (MinIO -> R2)"
  rclone sync "${MINIO_REMOTE}:${bucket}" "${R2_REMOTE}:${bucket}" \
    $_flags 2>&1 | while IFS= read -r line; do
    log_info "prune-r2: [$bucket] $line"
  done
  log_info "prune-r2: $bucket done"
done

log_info "prune-r2: complete$( $CONFIRM || echo ' (dry run — nothing deleted)')"
