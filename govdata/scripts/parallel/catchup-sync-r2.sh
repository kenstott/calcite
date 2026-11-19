#!/usr/bin/env bash
#
# catchup-sync-r2.sh — comprehensive backstop sync from MinIO to R2, prod bucket only.
#
# VERSION-PINNED, not liveness-checked. The first version of this script (2026-08-01) gated
# safety on "is anything writing to this schema right now" — a point-in-time pid-file check —
# then ran a long, non-atomic, multi-pass rclone copy/sync. That produced a torn R2 pointer:
# a new Iceberg snapshot committed on MinIO between this script's data-file pass and its
# pointer pass, so R2 ended up with version-hint.text naming a version whose metadata.json
# (and some of whose data files) were never copied. Root cause wasn't bad luck, it was the
# wrong invariant — liveness checks have blind spots (a repair tool invoked directly rather
# than through run-pool.sh's pid-tracking convention is invisible to them) and are racy by
# construction over a long-running copy regardless.
#
# Iceberg's actual guarantee makes liveness checks unnecessary: once version-hint.text names
# version N, every file N's manifest chain depends on was already fully and immutably written
# before that pointer was published. Nothing a concurrent writer does afterward can alter what
# N depends on. So per Iceberg table this script runs the same unit as the continuous sync
# (sync_iceberg_table_closure, common.sh), with the window defined as the files the CURRENT
# version references:
#   1. ClosureResolver reads version-hint.text once and lists the closure (metadata.json,
#      manifest list, every manifest, every data/delete file) with the sizes the manifests
#      recorded. No MinIO listing of the table is involved.
#   2. One lookup per closure file on R2; only the missing or wrong-size files are copied
#      (rclone copy, never sync/delete). Every rclone call has a deadline.
#   3. The whole closure is re-verified on R2 (present, recorded size), then the PINNED version
#      is published as version-hint.text, last, through the row-drop guard.
# Any shortfall: the pointer is not written and the table and file are logged at error level.
# Safe to run at any time, including during active ingestion.
#
# Only Iceberg tables (a directory with metadata/version-hint.text) are handled here; any other
# directory is skipped and reported.
#
# Prod parquet/iceberg data only — this script must NEVER touch the raw cache bucket
# (govdata-raw-v1).
#
# Usage:
#   govdata/scripts/parallel/catchup-sync-r2.sh [--dry-run] [--schema SCHEMA] [--table TABLE]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# Carry a signal through to the in-flight rclone. Without this the shell exits on TERM
# while its foreground rclone keeps running as an orphan, so a copy to R2 continues
# after the scheduler that owns it is gone.
_term_children() {
  pkill -TERM -P $$ 2>/dev/null || true
  exit 143
}
trap _term_children TERM INT

# R2 destination is built from the PROD_* creds in .env.prod (no .env.preprod, no rclone.conf).
configure_r2_remote

DRY_RUN=false
export R2_SYNC_DRY_RUN=false
SCHEMA_FILTER=""
TABLE_FILTER=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; export R2_SYNC_DRY_RUN=true ;;
    --schema) shift; SCHEMA_FILTER="${1:?--schema requires a name}" ;;
    --table) shift; TABLE_FILTER="${1:?--table requires a name}" ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
  shift
done

MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"
BUCKET="govdata-parquet-v1"

# ── Table sync (Iceberg-aware, version-pinned) ─────────────────────────────────
sync_iceberg_table() {
  local schema=$1 table=$2 version
  local rc=0
  version=$(sync_iceberg_table_closure "$MINIO_REMOTE" "$R2_REMOTE" "$BUCKET" "$schema" "$table") || rc=$?
  if [ "$rc" -eq 3 ]; then
    log_info "catchup-sync-r2: [$schema/$table] skipped: paused by marker"
    return 0
  fi
  if [ "$rc" -ne 0 ]; then
    log_error "catchup-sync-r2: [$schema/$table] NOT caught up — version-hint.text not advanced"
    return 1
  fi
  log_info "catchup-sync-r2: [$schema/$table] caught up at version $version"
}

mapfile -t _schemas < <(r2_rclone 600 lsf --dirs-only "${MINIO_REMOTE}:${BUCKET}" 2>/dev/null | sed 's#/*$##')
if [ "${#_schemas[@]}" -eq 0 ]; then
  log_error "catchup-sync-r2: FATAL — no schemas (top-level prefixes) found under ${BUCKET}"
  exit 1
fi
if [ -n "$SCHEMA_FILTER" ]; then
  _schemas=("$SCHEMA_FILTER")
fi

log_info "catchup-sync-r2: ${#_schemas[@]} schema(s) ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"

_fail=0
_ok=0
for schema in "${_schemas[@]}"; do
  mapfile -t _tables < <(r2_rclone 600 lsf --dirs-only "${MINIO_REMOTE}:${BUCKET}/${schema}" 2>/dev/null \
    | sed 's#/*$##' | grep -v '^year=')
  if [ -n "$TABLE_FILTER" ]; then
    _tables=("$TABLE_FILTER")
  fi

  for table in "${_tables[@]}"; do
    # rclone lsf on a single nonexistent path exits 0 with empty stdout — NOT a nonzero exit —
    # so existence must be judged by output, not exit code (verified directly; an exit-code
    # check here silently misrouted a plain reference table into the Iceberg path and crashed).
    _vh=$(r2_rclone 300 lsf "${MINIO_REMOTE}:${BUCKET}/${schema}/${table}/metadata/version-hint.text" 2>/dev/null)
    if [ -n "$_vh" ]; then
      if sync_iceberg_table "$schema" "$table"; then _ok=$((_ok + 1)); else _fail=1; fi
    else
      log_info "catchup-sync-r2: [$schema/$table] skipped: not an Iceberg table (no version-hint.text)"
    fi
  done
done

log_info "catchup-sync-r2: complete — ${_ok} table(s) caught up"
if [ "$_fail" -ne 0 ]; then
  log_error "catchup-sync-r2: one or more tables FAILED — see above; unaffected tables still synced"
  exit 1
fi
