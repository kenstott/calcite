#!/usr/bin/env bash
#
# r2-closure-cleanup.sh — remove R2 objects outside the PINNED closure of a table (#1166).
#
# sync-to-r2.sh / catchup-sync-r2.sh are copy-only, so R2 keeps every orphan manifest, superseded
# snapshot file and compacted-away data file. Decision (maintainer, 2026-11-12): R2 keeps only the
# closure of the version named by R2's own metadata/version-hint.text; no time travel on R2.
#
# DRY RUN IS THE DEFAULT. Nothing is deleted unless BOTH --delete is given AND CLAUDE_APPROVED=1
# is set in the environment. Scope is exactly the tables named on the command line.
#
# Per table:
#   1. Read R2's version-hint.text (version N). Keep set = ClosureResolver run AGAINST R2, i.e. the
#      closure of the version R2 itself names, read through the R2 backend.
#   2. List R2 per subprefix (metadata/, data/) — a whole-prefix LIST of a large table takes hours.
#   3. R2OrphanPlanner: candidates = objects under metadata/ or data/, not in the keep set, not
#      version-hint.text, older than the safety window (default 7 days, so files a sync is still
#      copying ahead of a pointer move are never candidates).
#   4. ABORT the table if any keep-set file is missing from R2 (R2's pointer names files R2 lacks;
#      run catchup-sync-r2.sh instead).
#   5. With --delete: for each subprefix, re-read R2's version-hint.text immediately before the
#      delete and abort if it is no longer N; delete only the listed candidate paths
#      (rclone delete --files-from-raw, never sync/purge). Afterwards re-resolve the closure
#      through R2 and confirm every keep file is still present.
#
# Usage:
#   r2-closure-cleanup.sh [--safety-days N] [--out-dir DIR] [--delete] schema/table [...]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env
configure_r2_remote

SAFETY_DAYS=7
OUT_DIR=""
DELETE=false
TABLES=()
while [ $# -gt 0 ]; do
  case "$1" in
    --safety-days) shift; SAFETY_DAYS="${1:-}" ;;
    --out-dir)     shift; OUT_DIR="${1:-}" ;;
    --delete)      DELETE=true ;;
    -*) echo "r2-closure-cleanup: unknown argument '$1'" >&2; exit 2 ;;
    */*) TABLES+=( "$1" ) ;;
    *) echo "r2-closure-cleanup: table must be <schema>/<table>, got '$1'" >&2; exit 2 ;;
  esac
  shift
done
[[ "$SAFETY_DAYS" =~ ^[0-9]+$ ]] || { echo "r2-closure-cleanup: --safety-days must be an integer" >&2; exit 2; }
[ "$SAFETY_DAYS" -ge 1 ] || { echo "r2-closure-cleanup: --safety-days must be at least 1" >&2; exit 2; }
[ ${#TABLES[@]} -gt 0 ] || { echo "Usage: $0 [--safety-days N] [--out-dir DIR] [--delete] schema/table ..." >&2; exit 2; }
if [ "$DELETE" = true ] && [ "${CLAUDE_APPROVED:-}" != "1" ]; then
  echo "r2-closure-cleanup: --delete requires CLAUDE_APPROVED=1 (maintainer-approved remediation job)" >&2
  exit 2
fi
[ -n "$OUT_DIR" ] || OUT_DIR="$(mktemp -d)"
mkdir -p "$OUT_DIR"

R2_REMOTE="r2"
BUCKET="govdata-parquet-v1"
JAR="$(resolve_classpath)"
CUTOFF="$(date -u -d "-${SAFETY_DAYS} days" '+%Y-%m-%d %H:%M:%S')"
MODE="DRY-RUN"; [ "$DELETE" = true ] && MODE="DELETE"

r2_pointer() {
  rclone cat "${R2_REMOTE}:${BUCKET}/$1/metadata/version-hint.text" 2>/dev/null | tr -d '[:space:]' || true
}

# ClosureResolver reads credentials from AWS_*; point them at R2 for the child process only.
resolve_on_r2() {
  AWS_ACCESS_KEY_ID="$PROD_AWS_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$PROD_AWS_SECRET_ACCESS_KEY" \
  AWS_ENDPOINT_OVERRIDE="$PROD_AWS_ENDPOINT_OVERRIDE" \
    java -Dcalcite.file.staging.directory="${GOVDATA_TMPDIR:-/var/tmp/govdata}" \
      -cp "$JAR" org.apache.calcite.adapter.file.iceberg.ClosureResolver \
      --warehouse "s3://${BUCKET}/$1" --table "$2"
}

printf 'mode\ttable\tstatus\tr2_objects\tr2_bytes\tkeep_files\tcandidates\tcandidate_bytes\tinside_safety_window\tkeep_missing_on_r2\n'

for spec in "${TABLES[@]}"; do
  schema="${spec%%/*}"; table="${spec#*/}"
  base="${schema}/${table}"
  tag="${schema}__${table}"
  row() { printf '%s\t%s\t%s\t-\t-\t-\t-\t-\t-\t-\n' "$MODE" "$spec" "$1"; }

  pointer=$(r2_pointer "$base")
  if [ -z "$pointer" ]; then row "SKIPPED:no-r2-pointer"; continue; fi

  if ! resolve_on_r2 "$schema" "$table" > "$OUT_DIR/$tag.resolved" 2> "$OUT_DIR/$tag.resolver.err"; then
    row "ERROR:keep-set-resolution-via-r2-failed(see $tag.resolver.err)"; continue
  fi
  version=$(awk '/^VERSION /{print $2}' "$OUT_DIR/$tag.resolved")
  if [ "$version" != "$pointer" ]; then
    row "SKIPPED:r2-pointer-moved($pointer->$version)"; continue
  fi
  grep -v '^VERSION ' "$OUT_DIR/$tag.resolved" | LC_ALL=C sort -u > "$OUT_DIR/$tag.keep"

  : > "$OUT_DIR/$tag.r2list"
  for sub in metadata data; do
    TZ=UTC rclone lsf -R --files-only --format "pst" --separator $'\t' \
        "${R2_REMOTE}:${BUCKET}/${base}/${sub}" \
      | awk -F'\t' -v pfx="$sub" 'BEGIN{OFS="\t"} {print pfx "/" $1, $2, $3}' >> "$OUT_DIR/$tag.r2list"
  done

  set +e
  summary=$(java -cp "$JAR" org.apache.calcite.adapter.file.iceberg.R2OrphanPlanner \
      --keep "$OUT_DIR/$tag.keep" --r2list "$OUT_DIR/$tag.r2list" --cutoff "$CUTOFF" \
      --out "$OUT_DIR/$tag.candidates" 2> "$OUT_DIR/$tag.planner.err")
  rc=$?
  set -e
  IFS=$'\t' read -r n bytes c cb win oos miss <<< "${summary:-0	0	0	0	0	0	0}"
  keep_n=$(wc -l < "$OUT_DIR/$tag.keep" | tr -d ' ')
  if [ "$rc" -eq 3 ]; then
    printf '%s\t%s\tABORT:keep-files-missing-on-r2(run catchup-sync-r2.sh; see %s.planner.err)\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$MODE" "$spec" "$tag" "$n" "$bytes" "$keep_n" "$c" "$cb" "$win" "$miss"
    continue
  elif [ "$rc" -ne 0 ]; then
    printf '%s\t%s\tERROR:planner-failed(see %s.planner.err)\t-\t-\t-\t-\t-\t-\t-\n' "$MODE" "$spec" "$tag"
    continue
  fi

  status="OK(v${version})"
  if [ "$DELETE" = true ]; then
    for sub in data metadata; do
      java -cp "$JAR" org.apache.calcite.adapter.file.iceberg.R2OrphanPlanner \
        --keep "$OUT_DIR/$tag.keep" --r2list "$OUT_DIR/$tag.r2list" --cutoff "$CUTOFF" \
        --scope "$sub" --out "$OUT_DIR/$tag.$sub.todelete" > /dev/null 2>> "$OUT_DIR/$tag.planner.err"
      [ -s "$OUT_DIR/$tag.$sub.todelete" ] || continue
      if [ "$(r2_pointer "$base")" != "$version" ]; then
        status="ABORT:r2-pointer-moved-before-$sub-delete"; break
      fi
      if ! rclone delete "${R2_REMOTE}:${BUCKET}/${base}" --files-from-raw "$OUT_DIR/$tag.$sub.todelete" \
            --transfers 8 --checkers 8 > "$OUT_DIR/$tag.$sub.delete.log" 2>&1; then
        status="ERROR:delete-$sub-failed(see $tag.$sub.delete.log)"; break
      fi
    done
    if [ "$status" = "OK(v${version})" ]; then
      # Verify by a read through the R2 backend: re-resolve the closure there, and confirm every
      # keep file is still present.
      if ! resolve_on_r2 "$schema" "$table" > "$OUT_DIR/$tag.verify.resolved" 2>> "$OUT_DIR/$tag.resolver.err"; then
        status="VERIFY-FAILED:closure-unreadable-via-r2"
      else
        rclone lsf --files-only --files-from-raw "$OUT_DIR/$tag.keep" "${R2_REMOTE}:${BUCKET}/${base}" \
          | LC_ALL=C sort -u > "$OUT_DIR/$tag.verify.present"
        if [ "$(wc -l < "$OUT_DIR/$tag.verify.present" | tr -d ' ')" != "$keep_n" ]; then
          status="VERIFY-FAILED:keep-files-missing-after-delete"
        else
          status="DELETED(v${version})"
        fi
      fi
    fi
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$MODE" "$spec" "$status" "$n" "$bytes" "$keep_n" "$c" "$cb" "$win" "$miss"
done
echo "lists in: $OUT_DIR" >&2
