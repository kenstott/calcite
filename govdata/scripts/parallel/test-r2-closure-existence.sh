#!/usr/bin/env bash
# verify_closure_on_r2 must fail, name the file and write the marker when a closure file is
# absent on the destination, and pass when every file is present.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
# shellcheck disable=SC1091
source "$DIR/common.sh"
set +e
export GOVDATA_R2_CLOSURE_MISSING_FILE="$T/marker"
R2=":local"
mkdir -p "$T/bkt/s/t/metadata" "$T/bkt/s/t/data"
echo x > "$T/bkt/s/t/metadata/v1.metadata.json"
echo x > "$T/bkt/s/t/data/a.parquet"
printf 'metadata/v1.metadata.json\ndata/a.parquet\n' > "$T/ok.txt"
printf 'metadata/v1.metadata.json\ndata/a.parquet\ndata/lost.parquet\n' > "$T/bad.txt"
verify_closure_on_r2 "$R2" "$T/bkt" s t "$T/ok.txt" 2>/dev/null \
  || { echo "FAIL: complete closure rejected"; exit 1; }
[ ! -e "$T/marker" ] || { echo "FAIL: marker written for complete closure"; exit 1; }
verify_closure_on_r2 "$R2" "$T/bkt" s t "$T/bad.txt" 2>"$T/err" \
  && { echo "FAIL: closure with missing file accepted"; exit 1; }
grep -q 'MISSING on R2: data/lost.parquet' "$T/err" || { echo "FAIL: file not named"; cat "$T/err"; exit 1; }
grep -q 's/t	data/lost.parquet' "$T/marker" || { echo "FAIL: marker"; cat "$T/marker"; exit 1; }
echo "PASS"
