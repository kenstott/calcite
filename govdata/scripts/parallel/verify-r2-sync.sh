#!/usr/bin/env bash
#
# verify-r2-sync.sh — assert that R2 mirrors every file MinIO currently holds.
#
# sync-to-r2.sh walks each schema forward in modtime slices and stamps a sentinel after
# each clean slice. That makes it fast and cheap (no destination LIST), but it is blind in
# one direction: a source file the slice window missed — a table created after the sentinel
# passed, a manifest rewritten by compaction that kept an older modtime, a slice that was
# skipped for an active writer — is never revisited, because the walk only moves forward.
# A clean pass therefore proves the in-window files copied, NOT that R2 holds a complete
# snapshot tree. Nothing self-heals: waiting does not fix drift the sentinel has passed.
#
# This script closes that blind spot by comparing inventories directly. For each table it
# lists both sides and reports every file present in MinIO but absent from R2. Two failure
# classes it is meant to catch:
#
#   NEW TABLE   — a table added to a schema YAML and ingested to MinIO never reaches R2.
#                 Readers see 404 on version-hint.text, which fails information_schema
#                 enumeration for the WHOLE schema, not just that table.
#   TORN TABLE  — the table is on R2 but the snapshot its pointer names references a
#                 manifest or data file that never arrived. Reads 404 mid-scan.
#
# Cost: this DOES list R2 (Class A ops), which sync-to-r2.sh deliberately avoids. Default
# scope is metadata/ only — small, and where both failure classes above surface. Use --full
# to include data files when you need a complete audit.
#
# Exit 0 = R2 complete for the scanned scope. Exit 1 = drift found. Exit 2 = usage/setup.

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# shellcheck disable=SC1091
set -a; . "$GOVDATA_ROOT/.env.prod" 2>/dev/null; set +a

BUCKET="${GOVDATA_PARQUET_BUCKET:-govdata-parquet-v1}"
MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"

SCHEMAS=""
TABLES=""
SCOPE="metadata"
REPAIR=false

usage() {
  cat >&2 <<EOF
Usage: verify-r2-sync.sh [--schema <s1,s2,...>] [--tables <t1,t2,...>] [--full] [--repair]

  --schema   comma-separated schemas to check (default: all schemas in the bucket)
  --tables   comma-separated tables to check (default: all tables in each schema)
  --full     also compare data/ files, not just metadata/ (slower, more R2 LIST ops)
  --repair   copy the missing files to R2 (unbounded by modtime — this is the
             reconciliation the forward-only sentinel walk cannot perform)

Examples:
  verify-r2-sync.sh --schema fiscal
  verify-r2-sync.sh --schema fiscal,geo --repair
  verify-r2-sync.sh --schema fiscal --tables usaspending_by_county --full
EOF
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --schema)  SCHEMAS="$2"; shift 2 ;;
    --tables)  TABLES="$2";  shift 2 ;;
    --full)    SCOPE="full"; shift ;;
    --repair)  REPAIR=true;  shift ;;
    -h|--help) usage ;;
    *) echo "Unknown argument: $1" >&2; usage ;;
  esac
done

command -v rclone >/dev/null 2>&1 || { echo "rclone not found" >&2; exit 2; }

echo "=================================================="
echo "verify-r2-sync.sh"
echo "=================================================="
echo "  Source:  ${MINIO_REMOTE}:${BUCKET}"
echo "  Mirror:  ${R2_REMOTE}:${BUCKET}"
echo "  Scope:   ${SCOPE}"
echo "  Repair:  ${REPAIR}"
echo

if [ -n "$SCHEMAS" ]; then
  IFS=',' read -r -a _schemas <<< "$SCHEMAS"
else
  mapfile -t _schemas < <(rclone lsf --dirs-only "${MINIO_REMOTE}:${BUCKET}" 2>/dev/null | sed 's#/*$##')
fi

_missing_tables=0
_torn_tables=0
_missing_files=0
_checked=0

for s in "${_schemas[@]}"; do
  [ -z "$s" ] && continue

  mapfile -t _src_tables < <(rclone lsf --dirs-only "${MINIO_REMOTE}:${BUCKET}/${s}" 2>/dev/null | sed 's#/*$##')
  if [ "${#_src_tables[@]}" -eq 0 ]; then
    echo "── ${s}: no tables in source — skipping"
    continue
  fi

  if [ -n "$TABLES" ]; then
    IFS=',' read -r -a _want <<< "$TABLES"
    _filtered=()
    for t in "${_src_tables[@]}"; do
      for w in "${_want[@]}"; do
        [ "$t" = "$w" ] && _filtered+=("$t")
      done
    done
    _src_tables=("${_filtered[@]}")
  fi

  echo "── ${s}: ${#_src_tables[@]} table(s)"

  for t in "${_src_tables[@]}"; do
    [ -z "$t" ] && continue
    _checked=$((_checked + 1))

    if [ "$SCOPE" = "full" ]; then
      _src_path="${MINIO_REMOTE}:${BUCKET}/${s}/${t}"
      _dst_path="${R2_REMOTE}:${BUCKET}/${s}/${t}"
    else
      _src_path="${MINIO_REMOTE}:${BUCKET}/${s}/${t}/metadata"
      _dst_path="${R2_REMOTE}:${BUCKET}/${s}/${t}/metadata"
    fi

    _src_list=$(rclone lsf -R --files-only "$_src_path" 2>/dev/null | sort)
    if [ -z "$_src_list" ]; then
      continue   # nothing in source for this scope; nothing to mirror
    fi
    _dst_list=$(rclone lsf -R --files-only "$_dst_path" 2>/dev/null | sort)

    if [ -z "$_dst_list" ]; then
      echo "   MISSING TABLE  ${s}.${t} — absent from R2 entirely ($(wc -l <<< "$_src_list") source file(s))"
      _missing_tables=$((_missing_tables + 1))
      _missing_files=$((_missing_files + $(wc -l <<< "$_src_list")))
      if $REPAIR; then
        echo "                  repairing: copying ${s}/${t} to R2"
        rclone copy "${MINIO_REMOTE}:${BUCKET}/${s}/${t}" "${R2_REMOTE}:${BUCKET}/${s}/${t}" \
          --transfers 16 2>&1 | sed 's/^/                  /'
      fi
      continue
    fi

    _absent=$(comm -23 <(echo "$_src_list") <(echo "$_dst_list"))
    if [ -n "$_absent" ]; then
      _n=$(wc -l <<< "$_absent")
      echo "   TORN TABLE     ${s}.${t} — ${_n} file(s) in MinIO absent from R2:"
      head -5 <<< "$_absent" | sed 's/^/                    /'
      [ "$_n" -gt 5 ] && echo "                    … and $((_n - 5)) more"
      _torn_tables=$((_torn_tables + 1))
      _missing_files=$((_missing_files + _n))
      if $REPAIR; then
        echo "                  repairing: copying missing files (no modtime bound)"
        rclone copy "$_src_path" "$_dst_path" --transfers 16 2>&1 | sed 's/^/                  /'
      fi
    fi
  done
done

echo
echo "=================================================="
echo "  Tables checked:        ${_checked}"
echo "  Missing from R2:       ${_missing_tables}"
echo "  Incomplete on R2:      ${_torn_tables}"
echo "  Files absent from R2:  ${_missing_files}"
echo "=================================================="

if [ "$_missing_tables" -gt 0 ] || [ "$_torn_tables" -gt 0 ]; then
  if $REPAIR; then
    echo "Drift found and repair attempted — re-run without --repair to confirm clean."
  else
    echo "DRIFT: R2 is missing files MinIO holds. The sentinel walk will NOT recover these;"
    echo "re-run with --repair to reconcile."
  fi
  exit 1
fi

echo "R2 mirror is complete for the scanned scope."
exit 0
