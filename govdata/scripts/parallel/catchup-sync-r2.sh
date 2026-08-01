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
# before that pointer was published. Nothing a concurrent writer does afterward — new
# snapshots, more commits, compaction — can alter what N depends on. So instead of asking "is
# anyone writing", this script:
#   1. Resolves version-hint.text ONCE per table (ClosureResolver, which loads the table and
#      reads it a single time) and gets back that exact pinned version N plus the full list of
#      files N's manifest chain depends on (metadata.json, manifest-list, every manifest,
#      every data/delete file) — relative to the table root, ready for rclone --files-from.
#   2. Copies EXACTLY those files, copy-only (rclone copy, never sync/delete — R2 orphans from
#      superseded snapshots are wasted storage, not a correctness problem; deleting them is a
#      separate, lower-stakes cleanup this script does not attempt).
#   3. Only after every file in the closure is confirmed copied, writes N — the SAME pinned
#      value from step 1, never re-read from MinIO — as R2's version-hint.text.
# This is safe to run at any time, including during active ingestion, with no active-writer
# detection at all: concurrent commits produce version N+1, N+2, ... which this pass simply
# doesn't know about yet and will pick up next run.
#
# Prod parquet/iceberg data only — this script must NEVER touch the raw cache bucket
# (govdata-raw-v1). Raw cache is source-of-truth HTTP response cache, not queryable prod
# data, and has no business being mirrored to R2 on this cadence.
#
# Run this far less often than the continuous sliced sync (sync-to-r2.sh) — it does a real
# per-table Iceberg scan (JVM start + manifest read) rather than a cheap HEAD-based diff, so
# it costs meaningfully more per run. It exists to bound worst-case staleness for a table that
# sync-to-r2.sh's active-writer skip has held for a long stretch, not to replace it.
#
# A table directory that has no metadata/version-hint.text is not an Iceberg table (e.g. a
# plain compacted-parquet reference table) — those are still real prod data, so this script
# copy-only mirrors them whole rather than skipping them, just without the version-pin step
# that only makes sense for a table with a committed snapshot pointer.
#
# Usage:
#   govdata/scripts/parallel/catchup-sync-r2.sh [--dry-run] [--schema SCHEMA] [--table TABLE]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# R2 destination is built from the PROD_* creds in .env.prod (no .env.preprod, no rclone.conf).
configure_r2_remote

DRY_RUN=false
SCHEMA_FILTER=""
TABLE_FILTER=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true ;;
    --schema) shift; SCHEMA_FILTER="${1:?--schema requires a name}" ;;
    --table) shift; TABLE_FILTER="${1:?--table requires a name}" ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
  shift
done

MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"
BUCKET="govdata-parquet-v1"

# The Java tools (ClosureResolver) live in the file/ module and are bundled into the govdata
# shadow jar by the normal build. resolve_classpath (common.sh) finds it the same way every
# other worker script does.
JAR="$(resolve_classpath)"

# ── Table sync (Iceberg-aware, version-pinned) ─────────────────────────────────
# Usage: sync_iceberg_table <schema> <table>
sync_iceberg_table() {
  local schema=$1 table=$2
  local warehouse="s3://${BUCKET}/${schema}"

  local resolved
  if ! resolved=$(java -cp "$JAR" org.apache.calcite.adapter.file.iceberg.ClosureResolver \
      --warehouse "$warehouse" --table "$table" 2>&1); then
    log_error "catchup-sync-r2: [$schema/$table] closure resolution FAILED:"
    echo "$resolved" | tail -20 | while IFS= read -r line; do log_error "catchup-sync-r2:   $line"; done
    return 1
  fi

  local version
  version=$(echo "$resolved" | grep '^VERSION ' | awk '{print $2}')
  if [ -z "$version" ]; then
    log_error "catchup-sync-r2: [$schema/$table] no VERSION line in resolver output — treating as failure"
    return 1
  fi

  local files_list
  files_list=$(mktemp)
  echo "$resolved" | grep -v '^VERSION ' > "$files_list"
  local file_count
  file_count=$(wc -l < "$files_list" | tr -d ' ')

  log_info "catchup-sync-r2: [$schema/$table] pinned version $version, $file_count files in closure"

  local flags="--files-from $files_list --transfers 16 --checkers 32"
  $DRY_RUN && flags="$flags --dry-run"
  if ! rclone copy "${MINIO_REMOTE}:${BUCKET}/${schema}/${table}" "${R2_REMOTE}:${BUCKET}/${schema}/${table}" \
      $flags 2>&1 | while IFS= read -r line; do log_info "catchup-sync-r2: [$schema/$table] $line"; done; then
    log_error "catchup-sync-r2: [$schema/$table] closure copy FAILED — version-hint.text NOT advanced"
    rm -f "$files_list"
    return 1
  fi
  rm -f "$files_list"

  if $DRY_RUN; then
    log_info "catchup-sync-r2: [$schema/$table] DRY RUN — would set version-hint.text=$version"
    return 0
  fi

  # The pinned value from step 1, not a fresh read of MinIO's (possibly since-advanced)
  # pointer — that distinction is the entire point of this design.
  if ! printf '%s' "$version" | rclone rcat "${R2_REMOTE}:${BUCKET}/${schema}/${table}/metadata/version-hint.text"; then
    log_error "catchup-sync-r2: [$schema/$table] closure copied but version-hint.text write FAILED — R2 still on its prior version, retry next pass"
    return 1
  fi
  log_info "catchup-sync-r2: [$schema/$table] fully caught up at version $version"
}

# ── Table sync (non-Iceberg — no metadata/version-hint.text) ───────────────────
sync_plain_table() {
  local schema=$1 table=$2
  local flags="--transfers 16 --checkers 32"
  $DRY_RUN && flags="$flags --dry-run"
  log_info "catchup-sync-r2: [$schema/$table] not an Iceberg table (no version-hint.text) — plain copy-only mirror"
  if ! rclone copy "${MINIO_REMOTE}:${BUCKET}/${schema}/${table}" "${R2_REMOTE}:${BUCKET}/${schema}/${table}" \
      $flags 2>&1 | while IFS= read -r line; do log_info "catchup-sync-r2: [$schema/$table] $line"; done; then
    log_error "catchup-sync-r2: [$schema/$table] plain copy FAILED"
    return 1
  fi
  log_info "catchup-sync-r2: [$schema/$table] fully caught up (plain mirror)"
}

mapfile -t _schemas < <(rclone lsf --dirs-only "${MINIO_REMOTE}:${BUCKET}" 2>/dev/null | sed 's#/*$##')
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
  mapfile -t _tables < <(rclone lsf --dirs-only "${MINIO_REMOTE}:${BUCKET}/${schema}" 2>/dev/null \
    | sed 's#/*$##' | grep -v '^year=')
  if [ -n "$TABLE_FILTER" ]; then
    _tables=("$TABLE_FILTER")
  fi

  for table in "${_tables[@]}"; do
    # rclone lsf on a single nonexistent path exits 0 with empty stdout — NOT a nonzero exit —
    # so existence must be judged by output, not exit code (verified directly; an exit-code
    # check here silently misrouted a plain reference table into the Iceberg path and crashed).
    _vh=$(rclone lsf "${MINIO_REMOTE}:${BUCKET}/${schema}/${table}/metadata/version-hint.text" 2>/dev/null)
    if [ -n "$_vh" ]; then
      if sync_iceberg_table "$schema" "$table"; then _ok=$((_ok + 1)); else _fail=1; fi
    else
      if sync_plain_table "$schema" "$table"; then _ok=$((_ok + 1)); else _fail=1; fi
    fi
  done
done

log_info "catchup-sync-r2: complete — ${_ok} table(s) caught up"
if [ "$_fail" -ne 0 ]; then
  log_error "catchup-sync-r2: one or more tables FAILED — see above; unaffected tables still synced"
  exit 1
fi
