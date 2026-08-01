#!/usr/bin/env bash
#
# catchup-sync-r2.sh — comprehensive backstop sync from MinIO to R2, prod bucket only.
#
# sync-to-r2.sh (the continuous, cost-optimized sync) HOLDS a schema's sentinel entirely
# while that schema has a live ETL writer, and resumes slicing forward once idle. That
# design assumes every schema goes idle often enough to drain its backlog. It does not:
# a schema can be busy across many consecutive worker runs (backfill years, a reprocess
# chain, etc.) for days at a stretch, and sync-to-r2.sh has no way to notice or alert —
# the sentinel just sits there. (Observed: govdata-parquet-v1/sec held at its 2026-07-28
# value for 4 days while historical-year workers and a fix-sec repair chain kept it
# "active" almost continuously, so none of that backfilled data ever reached R2 —
# the sliced sync literally never got a window to run in.)
#
# This script is the backstop: whenever a schema IS idle, do a full, non-sliced
# `rclone sync --checksum` of its ENTIRE subtree (not just a recent modtime slice),
# ignoring whatever sentinel sync-to-r2.sh was holding. That bounds the maximum
# staleness any idle schema can have to "however long since this last ran" instead of
# "however long sync-to-r2.sh happened to be denied an idle window."
#
# Deliberately NOT a replacement for sync-to-r2.sh: a full sync issues real R2 LIST
# (Class A, paged at 1000 keys) and per-file checksum compares, so it costs far more
# per run than the sliced HEAD-based approach. Run this infrequently (nightly by
# default — see GOVDATA_R2_CATCHUP_INTERVAL in run-scheduled.sh) as insurance, not as
# the primary sync path.
#
# Prod parquet/iceberg data only — this script must NEVER touch the raw cache bucket
# (govdata-raw-v1). Raw cache is source-of-truth HTTP response cache, not queryable
# prod data, and has no business being mirrored to R2 on this cadence.
#
# Uses `rclone sync` (not `copy`, unlike sync-to-r2.sh): this also removes R2 files
# that compaction has already deleted from MinIO, which a copy-only sync can never
# clean up. Same two-pass, pointer-last ordering as sync-to-r2.sh (data files, then
# version-hint.text) so a writer that starts mid-pass — the active-schema check is a
# snapshot, not a lock — still can't expose a torn snapshot to R2 readers.
#
# After a schema syncs cleanly, its sync-to-r2.sh sentinel (~/.r2-sync-state/<schema>)
# is advanced to "now": this pass just proved that schema fully caught up, so the next
# sliced pass should trust that rather than re-walking behind it.
#
# Usage:
#   govdata/scripts/parallel/catchup-sync-r2.sh [--dry-run] [--schema SCHEMA]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# R2 destination is built from the PROD_* creds in .env.prod (no .env.preprod, no rclone.conf).
configure_r2_remote

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
STATE_DIR="${HOME}/.r2-sync-state"
mkdir -p "$STATE_DIR"

# Only the parquet data bucket is ever synced to R2 — see sync-to-r2.sh for why the
# old tracker bucket is excluded, and the header above for why raw cache is excluded.
BUCKET="govdata-parquet-v1"

mapfile -t _schemas < <(rclone lsf --dirs-only "${MINIO_REMOTE}:${BUCKET}" 2>/dev/null | sed 's#/*$##')
if [ "${#_schemas[@]}" -eq 0 ]; then
  log_error "catchup-sync-r2: FATAL — no schemas (top-level prefixes) found under ${BUCKET}"
  exit 1
fi
if [ -n "$SCHEMA_FILTER" ]; then
  _schemas=("$SCHEMA_FILTER")
fi

log_info "catchup-sync-r2: ${#_schemas[@]} schema(s) considered ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"

# Same liveness check sync-to-r2.sh uses — a full sync races compaction exactly the
# same way a sliced one does, so a live schema is skipped here too (it'll get a
# comprehensive pass next time this runs, once idle).
detect_active_schemas "$SCRIPT_DIR/runs/pids"
_active_schemas="$ACTIVE_SCHEMAS"
if [ -n "$_active_schemas" ]; then
  log_info "catchup-sync-r2: holding active-writer schema(s) this pass —${_active_schemas}"
fi

_fail=0
_synced=0
_held=0
for s in "${_schemas[@]}"; do
  case " $_active_schemas " in
    *" $s "*) log_info "catchup-sync-r2: [$s] held (active writer)"; _held=$((_held + 1)); continue ;;
  esac

  log_info "catchup-sync-r2: [$s] starting full sync"
  _flags="--checksum --transfers 16 --checkers 32 --stats 60s"
  $DRY_RUN && _flags="$_flags --dry-run"

  set +e
  rclone sync "${MINIO_REMOTE}:${BUCKET}/$s" "${R2_REMOTE}:${BUCKET}/$s" \
    --exclude "**/version-hint.text" $_flags 2>&1 | while IFS= read -r line; do
    log_info "catchup-sync-r2: [$s] $line"
  done
  _rc_data=${PIPESTATUS[0]}
  rclone sync "${MINIO_REMOTE}:${BUCKET}/$s" "${R2_REMOTE}:${BUCKET}/$s" \
    --include "**/version-hint.text" $_flags 2>&1 | while IFS= read -r line; do
    log_info "catchup-sync-r2: [$s] pointer: $line"
  done
  _rc_ptr=${PIPESTATUS[0]}
  set -e

  if [ "$_rc_data" -ne 0 ] || [ "$_rc_ptr" -ne 0 ]; then
    log_error "catchup-sync-r2: [$s] FAILED (data rc=$_rc_data pointer rc=$_rc_ptr)"
    _fail=1
    continue
  fi

  if ! $DRY_RUN; then
    date +%s > "$STATE_DIR/$s"
  fi
  log_info "catchup-sync-r2: [$s] fully caught up"
  _synced=$((_synced + 1))
done

log_info "catchup-sync-r2: complete — ${_synced} schema(s) caught up, ${_held} held (active writer)"
if [ "$_fail" -ne 0 ]; then
  log_error "catchup-sync-r2: one or more schemas FAILED — see above; unaffected schemas still synced"
  exit 1
fi
