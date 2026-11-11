#!/usr/bin/env bash
#
# r2-closure-report.sh — DRY-RUN-ONLY report of R2 objects outside the closure of a table's
# retained Iceberg snapshots (#1166).
#
# sync-to-r2.sh / catchup-sync-r2.sh are copy-only, so R2 keeps every orphan manifest,
# superseded snapshot file and compacted-away data file forever. This script measures that
# garbage per table. It has NO delete mode: it lists, compares and reports. Any delete is a
# separate, maintainer-approved change (destructive).
#
# Method, per <schema>/<table>:
#   1. RetainedClosureResolver (read-only, against MinIO) emits the pinned current version N and
#      the union of files of EVERY retained snapshot plus every metadata json in the metadata log.
#   2. R2's version-hint.text is read. If it is not N the table is SKIPPED (R2 is behind or ahead
#      of the closure computed here, so we cannot prove what R2's pointer needs; run
#      catchup-sync-r2.sh first). Nothing is reported as deletable for a skipped table.
#   3. R2's table prefix is listed (path, size, mtime). A candidate is an R2 object that is NOT in
#      the closure, is NOT version-hint.text, and is older than the safety window.
#   4. Closure files absent from R2 are counted (integrity check: a non-zero count means R2's
#      pointer references files R2 lacks — a separate defect, and a reason not to clean that table).
#
# Output: one summary line per table on stdout; candidate lists in --out-dir.
#
# Usage:
#   r2-closure-report.sh [--safety-days N] [--out-dir DIR] schema/table [schema/table ...]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env
configure_r2_remote

SAFETY_DAYS=7
OUT_DIR=""
TABLES=()
while [ $# -gt 0 ]; do
  case "$1" in
    --safety-days) shift; SAFETY_DAYS="${1:-}" ;;
    --out-dir)     shift; OUT_DIR="${1:-}" ;;
    -*) echo "r2-closure-report: unknown argument '$1'" >&2; exit 2 ;;
    */*) TABLES+=( "$1" ) ;;
    *) echo "r2-closure-report: table must be <schema>/<table>, got '$1'" >&2; exit 2 ;;
  esac
  shift
done
[[ "$SAFETY_DAYS" =~ ^[0-9]+$ ]] || { echo "r2-closure-report: --safety-days must be an integer" >&2; exit 2; }
[ ${#TABLES[@]} -gt 0 ] || { echo "Usage: $0 [--safety-days N] [--out-dir DIR] schema/table ..." >&2; exit 2; }
[ -n "$OUT_DIR" ] || OUT_DIR="$(mktemp -d)"
mkdir -p "$OUT_DIR"

MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"
BUCKET="govdata-parquet-v1"
JAR="$(resolve_classpath)"
CUTOFF="$(date -u -d "-${SAFETY_DAYS} days" '+%Y-%m-%d %H:%M:%S')"

printf 'table\tstatus\tr2_objects\tr2_bytes\tclosure_files\tcandidate_objects\tcandidate_bytes\tinside_safety_window\tclosure_missing_on_r2\n'

for spec in "${TABLES[@]}"; do
  schema="${spec%%/*}"; table="${spec#*/}"
  base="${BUCKET}/${schema}/${table}"
  tag="${schema}__${table}"

  if ! resolved=$(java -Dcalcite.file.staging.directory="${GOVDATA_TMPDIR:-/var/tmp/govdata}" \
        -cp "$JAR" org.apache.calcite.adapter.file.iceberg.RetainedClosureResolver \
        --warehouse "s3://${BUCKET}/${schema}" --table "$table" 2>"$OUT_DIR/$tag.resolver.err"); then
    printf '%s\tERROR:closure-resolution-failed(see %s.resolver.err)\t-\t-\t-\t-\t-\t-\t-\n' "$spec" "$tag"
    continue
  fi
  version=$(printf '%s\n' "$resolved" | awk '/^VERSION /{print $2}')
  printf '%s\n' "$resolved" | grep -v '^VERSION ' | LC_ALL=C sort -u > "$OUT_DIR/$tag.closure"
  closure_n=$(wc -l < "$OUT_DIR/$tag.closure" | tr -d ' ')

  r2_version=$(rclone cat "${R2_REMOTE}:${base}/metadata/version-hint.text" 2>/dev/null | tr -d '[:space:]' || true)
  if [ "$r2_version" != "$version" ]; then
    printf '%s\tSKIPPED:r2-pointer=%s,minio-pointer=%s(run catchup-sync-r2.sh first)\t-\t-\t%s\t-\t-\t-\t-\n' \
      "$spec" "${r2_version:-none}" "$version" "$closure_n"
    continue
  fi

  rclone lsf -R --files-only --format "pst" --separator $'\t' "${R2_REMOTE}:${base}" \
    | LC_ALL=C sort -t $'\t' -k1,1 > "$OUT_DIR/$tag.r2list"

  awk -F'\t' -v cutoff="$CUTOFF" -v closure="$OUT_DIR/$tag.closure" -v out="$OUT_DIR/$tag.candidates" '
    BEGIN { while ((getline p < closure) > 0) keep[p] = 1; close(closure) }
    { n++; bytes += $2; on_r2[$1] = 1
      if ($1 in keep || $1 == "metadata/version-hint.text") next
      if ($3 >= cutoff) { win++; next }
      c++; cb += $2; print $1 "\t" $2 "\t" $3 > out }
    END {
      miss = 0; for (p in keep) if (!(p in on_r2)) miss++
      printf "%d\t%d\t%d\t%d\t%d\t%d\n", n, bytes, c, cb, win, miss }' "$OUT_DIR/$tag.r2list" > "$OUT_DIR/$tag.counts"
  [ -f "$OUT_DIR/$tag.candidates" ] || : > "$OUT_DIR/$tag.candidates"
  IFS=$'\t' read -r n bytes c cb win miss < "$OUT_DIR/$tag.counts"
  printf '%s\tOK(v%s)\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$spec" "$version" "$n" "$bytes" "$closure_n" "$c" "$cb" "$win" "$miss"
done
echo "candidate lists and closures in: $OUT_DIR" >&2
