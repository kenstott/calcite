#!/usr/bin/env bash
#
# sync-to-r2.sh — copy new Iceberg files from MinIO to R2.
#
# rclone copy skips files already on R2 with matching size+modtime. Since
# Iceberg is append-only (new snapshots only add files, never modify existing
# ones), a --max-age window bounded by the last-sync sentinel selects only the
# new parquet data + metadata files from each incremental ETL run.
#
# Cost note (R2 billing): incremental passes add --no-traverse so rclone does NOT
# LIST the R2 destination — a full recursive LIST is Class A and paged at 1000
# keys/page, i.e. ceil(objects/1000) Class A ops *per pass* regardless of how few
# files changed. With --no-traverse rclone instead does one HEAD (Class B, ~12x
# cheaper) per candidate file and PUTs only what's missing, so R2 ops scale with
# the new-file count, not lake size or sync cadence. An idle pass (nothing newer
# than the window) costs zero R2 ops. Cold start deliberately KEEPS the traverse:
# for a full backlog one paged LIST (N/1000 Class A) is far cheaper than N HEADs,
# and it lets rclone skip anything already on R2.
#
# Runs continuously (one pass every GOVDATA_R2_SYNC_INTERVAL seconds) from
# run-scheduled.sh when PROD_* publish creds are set.
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
  # --no-traverse: skip the (Class A, paged) recursive LIST of the R2 destination and
  # HEAD-check each candidate individually instead. Only valid on incremental passes,
  # where the candidate set is the small --max-age window; cold start below omits it so
  # a full backlog resolves via one cheap paged traverse rather than N HEADs.
  _flags="--max-age ${_age}s --no-traverse $_flags"
  log_info "sync-to-r2: incremental — files newer than ${_age}s, no-traverse ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"
else
  # First run (or missing/empty/corrupt sentinel): copy the full backlog. rclone
  # copy already skips files present on R2 with matching size+modtime, so a cold
  # start re-transfers nothing that is already there.
  [ -n "$_last" ] && log_info "sync-to-r2: WARNING — ignoring non-numeric sentinel ('$_last'); treating as cold start"
  log_info "sync-to-r2: cold start (no valid sentinel) — full copy ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"
fi
$DRY_RUN && _flags="$_flags --dry-run"

# --- Skip schemas with an active ETL writer ------------------------------------------
# Syncing a live Iceberg table races its own compaction: the writer deletes superseded
# manifest .avro files mid-copy, so rclone (which listed them a moment earlier) hits a
# source 404 NoSuchKey — and worse, pass 2 could advance the R2 pointer to a snapshot
# whose manifests didn't finish uploading, briefly exposing a torn snapshot to R2
# readers. A schema is "active" when run-pool has a live worker-<schema>-<mode>.pid with
# no matching .exit marker. Exclude that schema's <schema>/** subtree from BOTH passes;
# the next pass (once the writer finishes and drops its .exit) picks it up. All sec_*
# workers write the shared sec/ tree, so they collapse to a single sec/ exclude.
_pid_dir="$SCRIPT_DIR/runs/pids"
active_excludes=()
_active_schemas=""
if [ -d "$_pid_dir" ]; then
  for _pf in "$_pid_dir"/worker-*.pid; do
    [ -e "$_pf" ] || continue
    _id=$(basename "$_pf" .pid)                        # worker-<schema>-<mode>
    [ -f "$_pid_dir/${_id}.exit" ] && continue         # worker already finished
    _wpid=$(head -1 "$_pf" 2>/dev/null | tr -d '[:space:]')
    { [ -n "$_wpid" ] && kill -0 "$_wpid" 2>/dev/null; } || continue  # pid not alive
    _rest=${_id#worker-}                               # <schema>-<mode>
    _schema=${_rest%-*}                                # strip the -<mode> suffix
    case "$_schema" in sec_*|sec) _schema=sec ;; esac  # sec_* all write sec/
    case " $_active_schemas " in *" $_schema "*) continue ;; esac      # dedup
    _active_schemas="$_active_schemas $_schema"
    active_excludes+=(--exclude "${_schema}/**")
  done
fi
if [ -n "$_active_schemas" ]; then
  log_info "sync-to-r2: skipping active-writer schema(s) this pass —${_active_schemas}"
fi

for bucket in "${BUCKETS[@]}"; do
  log_info "sync-to-r2: $bucket"
  # Two-pass, pointer-last copy. rclone transfers a pass's files concurrently in an
  # order it chooses; it has no knowledge that the Hadoop-catalog pointer
  # (version-hint.text) depends on the data/metadata files a snapshot references. The
  # tiny pointer would otherwise land on R2 before the multi-MB data files it points
  # at, exposing a torn snapshot to live R2 readers. So:
  #   Pass 1 — copy the whole data+metadata tree EXCEPT the pointer, in any order (no
  #            reader looks below a pointer that hasn't advanced yet, so order is moot).
  #   Pass 2 — advance version-hint.text only now that its referenced tree is present.
  # This re-imposes on R2 the write-order guarantee the writer already holds on MinIO.
  rclone copy "${MINIO_REMOTE}:${bucket}" "${R2_REMOTE}:${bucket}" \
    "${active_excludes[@]}" --exclude "**/version-hint.text" $_flags 2>&1 | while IFS= read -r line; do
    log_info "sync-to-r2: [$bucket] $line"
  done
  rclone copy "${MINIO_REMOTE}:${bucket}" "${R2_REMOTE}:${bucket}" \
    "${active_excludes[@]}" --include "**/version-hint.text" $_flags 2>&1 | while IFS= read -r line; do
    log_info "sync-to-r2: [$bucket] pointer: $line"
  done
  log_info "sync-to-r2: $bucket done"
done

# Update sentinel only on live runs
if ! $DRY_RUN; then
  echo "$_now" > "$SYNC_STAMP"
fi

log_info "sync-to-r2: complete"
