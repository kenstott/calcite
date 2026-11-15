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

# run_closure_resolver: stderr noise is dropped, a non-path stdout line is a failure.
mkdir -p "$T/bin"
cat > "$T/bin/java" <<'JEOF'
#!/usr/bin/env bash
echo "WARNING: A restricted method in java.lang.System" >&2
echo "VERSION 7"
echo "metadata/v7.metadata.json"
[ -n "${STDOUT_NOISE:-}" ] && echo "[main] INFO something"
exit 0
JEOF
chmod +x "$T/bin/java"
out=$(PATH="$T/bin:$PATH" run_closure_resolver jar w t tag 2>/dev/null) \
  || { echo "FAIL: resolver with stderr noise rejected"; exit 1; }
[ "$out" = "$(printf 'VERSION 7\nmetadata/v7.metadata.json')" ] || { echo "FAIL: resolver output: $out"; exit 1; }
STDOUT_NOISE=1 PATH="$T/bin:$PATH" run_closure_resolver jar w t tag >/dev/null 2>&1 \
  && { echo "FAIL: stdout noise accepted"; exit 1; }
echo "PASS"
