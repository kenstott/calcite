#!/usr/bin/env bash
# Read-only audit: for every Iceberg table on MinIO and on R2, resolve the CURRENT snapshot's
# closure (ClosureResolver reads that target's own version-hint.text) and look up every
# referenced file on that target (rclone lsf -R --files-from: one lookup per file, no
# recursive listing). Prints one line per table with missing files; summary at the end.
# Usage: audit-iceberg-closure.sh [--target minio|r2|both] [--schema S] [--jobs N]
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET=both SCHEMA="" JOBS=4
while [ $# -gt 0 ]; do
  case $1 in
    --target) TARGET=$2; shift 2 ;;
    --schema) SCHEMA=$2; shift 2 ;;
    --jobs) JOBS=$2; shift 2 ;;
    --one) shift; exec "$DIR/audit-iceberg-closure.sh" --_one "$@" ;;
    --_one) shift; MODE=one; break ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac
done

if [ "${MODE:-}" = one ]; then
  tgt=$1 s=$2 t=$3
  # shellcheck disable=SC1091
  source "$DIR/common.sh"
  JAR=$(resolve_classpath) || exit 3
  set -a; source "$(git -C "$DIR" rev-parse --show-toplevel)/govdata/.env.prod" 2>/dev/null || source /home/kstott/calcite/govdata/.env.prod; set +a
  BUCKET=govdata-parquet-v1
  if [ "$tgt" = r2 ]; then
    export AWS_ACCESS_KEY_ID=$PROD_AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$PROD_AWS_SECRET_ACCESS_KEY \
      AWS_ENDPOINT_OVERRIDE=$PROD_AWS_ENDPOINT_OVERRIDE AWS_REGION=${PROD_AWS_REGION:-auto}
    remote=r2
  else
    remote=minio
  fi
  out=$(java -Xmx1g -cp "$JAR" org.apache.calcite.adapter.file.iceberg.ClosureResolver \
        --warehouse "s3://$BUCKET/$s" --table "$t" 2>/dev/null) || {
    echo "RESOLVE_FAIL $tgt $s.$t"; exit 0; }
  ver=$(echo "$out" | grep '^VERSION ' | awk '{print $2}')
  l=$(mktemp); echo "$out" | grep -v '^VERSION ' | sort -u > "$l"
  p=$(mktemp)
  rclone lsf -R --files-only --checkers 16 "$remote:$BUCKET/$s/$t" --files-from "$l" > "$p" 2>/dev/null || {
    echo "LOOKUP_FAIL $tgt $s.$t v$ver"; rm -f "$l" "$p"; exit 0; }
  miss=$(LC_ALL=C comm -23 "$l" <(LC_ALL=C sort -u "$p"))
  n=$(wc -l < "$l")
  if [ -n "$miss" ]; then
    echo "MISSING $tgt $s.$t v$ver $(echo "$miss" | wc -l)/$n: $(echo "$miss" | head -3 | tr '\n' ' ')"
  else
    echo "OK $tgt $s.$t v$ver $n"
  fi
  rm -f "$l" "$p"
  exit 0
fi

targets=(minio r2); [ "$TARGET" != both ] && targets=("$TARGET")
for tgt in "${targets[@]}"; do
  remote=$tgt
  for s in $(rclone lsf "$remote:govdata-parquet-v1/" --dirs-only | tr -d /); do
    case $s in _*|type=*) continue ;; esac
    [ -n "$SCHEMA" ] && [ "$s" != "$SCHEMA" ] && continue
    for t in $(rclone lsf "$remote:govdata-parquet-v1/$s/" --dirs-only | tr -d /); do
      echo "$tgt $s $t"
    done
  done
done | xargs -P "$JOBS" -L1 bash -c '"$0" --one "$@"' "$DIR/audit-iceberg-closure.sh"
