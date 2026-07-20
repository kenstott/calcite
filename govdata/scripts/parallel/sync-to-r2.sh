#!/usr/bin/env bash
#
# sync-to-r2.sh — copy new Iceberg files from MinIO to R2.
#
# rclone copy skips files already on R2 with matching size+modtime. Since Iceberg is
# append-only (new snapshots only add files, never modify existing ones), bounding the
# source by modification time selects exactly the new parquet data + metadata files.
#
# The copy is done PER SCHEMA in fixed-width MODTIME SLICES, oldest→newest, and each
# schema's sentinel (~/.r2-sync-state/<schema>, an epoch) is advanced after EACH slice that
# copies cleanly. Per-schema (not one global) sentinels are required because a schema with a
# live ETL writer is SKIPPED to avoid racing its compaction — a single global boundary would
# then step over that schema's unsynced files and never come back for them. With its own
# sentinel, a skipped schema is simply held and resumes from where it was left once idle.
# Per-slice stamping also makes progress durable: an errored or killed pass resumes from the
# last completed slice instead of re-scanning the backlog (the failure mode that let an
# 11-day backlog wedge indefinitely — one giant --max-age pass that kept erroring out before
# it ever reached the sentinel write). Slice width is GOVDATA_R2_SYNC_SLICE seconds
# (default 6h): smaller slices stamp progress more often and bound the retry window a
# persistent error (e.g. an R2 502) can wedge to one slice.
#
# Cost note (R2 billing): every slice uses --no-traverse so rclone does NOT LIST the R2
# destination — a full recursive LIST is Class A and paged at 1000 keys/page. Instead it
# does one HEAD (Class B, ~12x cheaper) per candidate file in the slice window and PUTs
# only what's missing, so R2 ops scale with the new-file count, not lake size or cadence.
# An empty slice (nothing in that window) costs zero R2 ops.
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

SLICE="${GOVDATA_R2_SYNC_SLICE:-21600}"   # slice width in seconds (default 6h)
BUFFER=120                                # overlap between slices so a file written at a
                                          # slice boundary is never dropped (rclone
                                          # idempotently skips the re-listed overlap)

# ── Per-schema sync state ──────────────────────────────────────────────────────
# A single global sentinel cannot coexist with active-writer skipping: if a schema's live
# subtree is skipped but one global boundary still advances, that schema's files below the
# boundary (its unsynced backlog + whatever it writes while skipped) get stepped over and
# never synced. So each schema carries its OWN sentinel — STATE_DIR/<schema> holds the epoch
# that schema is synced through. An active schema's sentinel is simply held; once it goes
# idle its backlog is picked up from exactly where it was left.
STATE_DIR="${HOME}/.r2-sync-state"
mkdir -p "$STATE_DIR"

# Schemas = top-level prefixes in the bucket (each schema writes under <schema>/). Fail
# loudly on an unreadable/empty source rather than silently syncing nothing.
mapfile -t _schemas < <(rclone lsf --dirs-only "${MINIO_REMOTE}:${BUCKETS[0]}" 2>/dev/null | sed 's#/*$##')
if [ "${#_schemas[@]}" -eq 0 ]; then
  log_error "sync-to-r2: FATAL — no schemas (top-level prefixes) found under ${BUCKETS[0]}"
  exit 1
fi
log_info "sync-to-r2: ${#_schemas[@]} schema(s); slice=${SLICE}s ($( $DRY_RUN && echo 'DRY RUN' || echo 'LIVE'))"

# --- Detect schemas with an active ETL writer ----------------------------------------
# Syncing a live Iceberg table races its own compaction: the writer deletes superseded
# manifest .avro files mid-copy, so rclone (which listed them a moment earlier) hits a
# source 404 NoSuchKey — and pass 2 could advance the R2 pointer to a snapshot whose
# manifests didn't finish uploading, exposing a torn snapshot to R2 readers. So a schema
# with a live writer is SKIPPED this pass and its sentinel HELD; it resumes from that held
# sentinel once the writer finishes. A schema is "active" when run-pool has a live
# worker-<schema>-<mode>.pid with no matching .exit marker. All sec_* workers write the
# shared sec/ tree, so they collapse to the single 'sec' schema.
_pid_dir="$SCRIPT_DIR/runs/pids"
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
  done
fi
if [ -n "$_active_schemas" ]; then
  log_info "sync-to-r2: holding active-writer schema(s) this pass —${_active_schemas}"
fi

# ── Per-schema slice drain ─────────────────────────────────────────────────────
# Each idle schema walks its own sentinel forward in modtime slices, oldest→newest, over
# its own <schema>/ subtree, stamping STATE_DIR/<schema> after EACH clean slice. Active
# schemas are skipped entirely (sentinel untouched). A failed slice holds that schema's
# sentinel and drops to the next schema; the pass exits non-zero so run-scheduled retries.
_fail=0
for s in "${_schemas[@]}"; do
  case " $_active_schemas " in
    *" $s "*) log_info "sync-to-r2: [$s] held (active writer)"; continue ;;
  esac

  _sf="$STATE_DIR/$s"
  _v=""
  [ -f "$_sf" ] && _v=$(cat "$_sf" 2>/dev/null)
  if ! [[ "$_v" =~ ^[0-9]+$ ]]; then
    # New schema (or missing/corrupt state): floor at the oldest modtime in ITS subtree so
    # the schema's whole history is covered. No legacy/global seed — that could assume a
    # skipped schema was synced when it wasn't. Files already on R2 are HEAD-skipped, so a
    # from-oldest walk re-transfers nothing that is already there.
    # NB: `| head -1` closes the pipe early → SIGPIPE(141) on sort; under set -o pipefail +
    # set -e that would abort the whole pass. Disable pipefail just for this probe (the
    # SIGPIPE is expected, not a failure) so a normal, non-empty listing yields the oldest.
    _o=$( set +o pipefail; rclone lsf -R --files-only --format t "${MINIO_REMOTE}:${BUCKETS[0]}/$s" 2>/dev/null | sort | head -1 )
    if [ -z "$_o" ]; then
      log_info "sync-to-r2: [$s] no source files — skipping"
      continue
    fi
    _v=$(date -d "$_o" +%s)
    log_info "sync-to-r2: [$s] cold start — floor at oldest modtime $_v ($_o)"
  fi

  _cursor=$_v
  while [ "$_cursor" -lt "$_now" ]; do
    _slice_end=$(( _cursor + SLICE ))
    [ "$_slice_end" -gt "$_now" ] && _slice_end=$_now

    # Window (_lo, _slice_end] in epoch → rclone age flags relative to now:
    #   modtime > _lo         ⇒ younger than (now - _lo)         ⇒ --max-age
    #   modtime <= _slice_end ⇒ older   than (now - _slice_end)  ⇒ --min-age
    _lo=$(( _cursor - BUFFER )); [ "$_lo" -lt 0 ] && _lo=0
    _max_age=$(( _now - _lo ))
    _min_age=$(( _now - _slice_end )); [ "$_min_age" -lt 0 ] && _min_age=0
    _slice_flags="--min-age ${_min_age}s --max-age ${_max_age}s --no-traverse --transfers 16 --stats 60s"
    $DRY_RUN && _slice_flags="$_slice_flags --dry-run"

    log_info "sync-to-r2: [$s] slice $(date -u -d "@$_lo" +%Y-%m-%dT%H:%MZ) .. $(date -u -d "@$_slice_end" +%Y-%m-%dT%H:%MZ)"

    # Two-pass, pointer-last copy, scoped to this schema's subtree. rclone orders a pass's
    # transfers itself and doesn't know the Hadoop-catalog pointer (version-hint.text)
    # depends on the data/metadata a snapshot references; copying it first would expose a
    # torn snapshot to live R2 readers. Pass 1 copies everything EXCEPT the pointer, pass 2
    # advances the pointer once its tree is present. PIPESTATUS[0] captures rclone's real
    # exit (the `| while read` + set -e would otherwise mask/abort it), so a failed slice
    # holds THIS schema's sentinel and retries next pass instead of skipping its data.
    set +e
    rclone copy "${MINIO_REMOTE}:${BUCKETS[0]}/$s" "${R2_REMOTE}:${BUCKETS[0]}/$s" \
      --exclude "**/version-hint.text" $_slice_flags 2>&1 | while IFS= read -r line; do
      log_info "sync-to-r2: [$s] $line"
    done
    _rc_data=${PIPESTATUS[0]}
    rclone copy "${MINIO_REMOTE}:${BUCKETS[0]}/$s" "${R2_REMOTE}:${BUCKETS[0]}/$s" \
      --include "**/version-hint.text" $_slice_flags 2>&1 | while IFS= read -r line; do
      log_info "sync-to-r2: [$s] pointer: $line"
    done
    _rc_ptr=${PIPESTATUS[0]}
    set -e

    if [ "$_rc_data" -ne 0 ] || [ "$_rc_ptr" -ne 0 ]; then
      log_error "sync-to-r2: [$s] slice FAILED (data rc=$_rc_data pointer rc=$_rc_ptr) — sentinel held at $_v, retry next pass"
      _fail=1
      break
    fi

    $DRY_RUN || echo "$_slice_end" > "$_sf"
    _v=$_slice_end
    _cursor=$_slice_end
  done

  [ "$_cursor" -ge "$_now" ] && log_info "sync-to-r2: [$s] synced through $(date -u -d "@$_now" +%Y-%m-%dT%H:%MZ)"
done

if [ "$_fail" -ne 0 ]; then
  log_error "sync-to-r2: incomplete — one or more schema slices failed; they resume from their held sentinels next pass"
  exit 1
fi

log_info "sync-to-r2: complete"
