#!/usr/bin/env bash
# Tests for the R2 sync unit (common.sh): closure existence and size gate, resolver output
# hygiene, copy-moves-nothing detection, row-drop guard with one-shot override, call deadlines.
set -uo pipefail
command -v rclone >/dev/null || export PATH="$HOME/.local/bin:$PATH"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
# shellcheck disable=SC1091
source "$DIR/common.sh"
fail() { echo "FAIL: $*"; [ -f "$T/stderr" ] && tail -15 "$T/stderr"; [ -f "$T/errors" ] && tail -15 "$T/errors"; exit 1; }

export GOVDATA_R2_CLOSURE_MISSING_FILE="$T/marker"
export GOVDATA_R2_SYNC_STATE_DIR="$T/state"
export GOVDATA_R2_SYNC_TIMEOUT=10s GOVDATA_R2_SYNC_CONTIMEOUT=5s
export RCLONE_CONFIG="$T/rclone.conf"
printf '[mn]\ntype = alias\nremote = %s/minio\n[rr]\ntype = alias\nremote = %s/r2\n' "$T" "$T" > "$RCLONE_CONFIG"
resolve_classpath() { echo jar; }
log_info() { :; }
log_error() { echo "$*" >> "$T/errors"; }

B=bkt; MT="$T/minio/$B/s/t"; RT="$T/r2/$B/s/t"
mkdir -p "$MT/metadata" "$MT/data" "$RT/metadata" "$T/bin"

# meta <dir> <version> <total> <added> <deleted> <operation>
meta() {
  jq -n --arg tot "$3" --arg add "$4" --arg del "$5" --arg op "$6" \
    '{"current-snapshot-id": 2, snapshots: [
       {"snapshot-id": 1, "timestamp-ms": 1000, summary: {operation: "append", "total-records": "1000", "added-records": "1000"}},
       {"snapshot-id": 2, "timestamp-ms": 2000, summary: {operation: $op, "total-records": $tot, "added-records": $add, "deleted-records": $del}}]}' \
    > "$1/metadata/v$2.metadata.json"
}
# pubmeta <dir> <version> <total>: the version R2 publishes now (one snapshot, id 1)
pubmeta() {
  jq -n --arg tot "$3" '{"current-snapshot-id": 1, snapshots: [
       {"snapshot-id": 1, "timestamp-ms": 1000, summary: {operation: "append", "total-records": $tot, "added-records": $tot}}]}' \
    > "$1/metadata/v$2.metadata.json"
}
# stub java: prints $RESOLVE_OUT; JVM warning on stderr like the real one
cat > "$T/bin/java" <<'JEOF'
#!/usr/bin/env bash
echo "WARNING: A restricted method in java.lang.System" >&2
cat "$RESOLVE_OUT"
[ -n "${STDOUT_NOISE:-}" ] && echo "[main] INFO something"
exit 0
JEOF
chmod +x "$T/bin/java"
export PATH="$T/bin:$PATH" RESOLVE_OUT="$T/resolve.txt"
hint() { cat "$RT/metadata/version-hint.text" 2>/dev/null || echo none; }
reset() { rm -rf "$MT" "$RT" "$T/state" "$T/marker" "$T/errors"; mkdir -p "$MT/metadata" "$MT/data" "$RT/metadata"; }
sync() { sync_iceberg_table_closure mn rr "$B" s t 2>>"$T/stderr"; }

# 1. complete closure: copied, verified, pointer published
reset
printf 'aaaa' > "$MT/data/a.parquet"; meta "$MT" 7 1000 0 0 append
printf 'VERSION 7\nmetadata/v7.metadata.json\t-1\ndata/a.parquet\t4\n' > "$RESOLVE_OUT"
sync >/dev/null || fail "complete closure not published"
[ "$(hint)" = 7 ] && [ -f "$RT/data/a.parquet" ] || fail "pointer/data after complete closure: $(hint)"
echo "PASS complete closure publishes"

# 2. a file absent from the source: copy exits 0 but moves nothing; pointer must not move, run reports it
reset
printf '3' > "$RT/metadata/version-hint.text"
printf 'aaaa' > "$MT/data/a.parquet"; meta "$MT" 7 1000 0 0 append
printf 'VERSION 7\nmetadata/v7.metadata.json\t-1\ndata/a.parquet\t4\ndata/lost.parquet\t9\n' > "$RESOLVE_OUT"
sync >/dev/null && fail "closure with a file absent from the source was published"
[ "$(hint)" = 3 ] || fail "pointer moved to $(hint)"
grep -q 's/t	data/lost.parquet	MISSING' "$T/marker" || fail "marker: $(cat "$T/marker" 2>/dev/null)"
grep -q 'MISSING on R2: data/lost.parquet' "$T/errors" || fail "error log"
echo "PASS missing source file: pointer unmoved, reported"

# 3. wrong-size file on R2 is re-copied, not trusted
reset
printf 'aaaa' > "$MT/data/a.parquet"; meta "$MT" 7 1000 0 0 append
mkdir -p "$RT/data"; printf 'aa' > "$RT/data/a.parquet"
printf 'VERSION 7\nmetadata/v7.metadata.json\t-1\ndata/a.parquet\t4\n' > "$RESOLVE_OUT"
sync >/dev/null || fail "wrong-size file not repaired"
[ "$(wc -c < "$RT/data/a.parquet")" = 4 ] || fail "size not repaired"
echo "PASS wrong size re-copied"

# 4. verify alone rejects a wrong size
printf 'metadata/v7.metadata.json\t-1\ndata/a.parquet\t99\n' > "$T/sz.txt"
verify_closure_on_r2 rr "$B" s t "$T/sz.txt" 2>/dev/null && fail "size mismatch accepted"
echo "PASS size mismatch rejected"

# 5. resolver: stderr noise dropped, stdout noise rejected
printf 'VERSION 7\nmetadata/v7.metadata.json\t-1\n' > "$RESOLVE_OUT"
out=$(run_closure_resolver jar w t tag 2>/dev/null) || fail "resolver with stderr noise rejected"
[ "$out" = "$(printf 'VERSION 7\nmetadata/v7.metadata.json\t-1')" ] || fail "resolver output: $out"
STDOUT_NOISE=1 run_closure_resolver jar w t tag >/dev/null 2>&1 && fail "stdout noise accepted"
echo "PASS resolver output hygiene"

# 6. row-drop guard: 20% drop holds, growth passes, override works once
setup_guard() { # <published total> <candidate total> <candidate added> <candidate deleted> <op>
  reset
  pubmeta "$RT" 3 "$1"; printf '3' > "$RT/metadata/version-hint.text"
  meta "$MT" 7 "$2" "$3" "$4" "$5"
  printf 'aaaa' > "$MT/data/a.parquet"
  printf 'VERSION 7\nmetadata/v7.metadata.json\t-1\ndata/a.parquet\t4\n' > "$RESOLVE_OUT"
}
setup_guard 1000 800 0 0 append
sync >/dev/null && fail "20% drop published"
[ "$(hint)" = 3 ] || fail "pointer moved on a held drop"
[ -f "$T/state/held/s__t.json" ] && grep -q '"candidate_total_records": "800"' "$T/state/held/s__t.json" || fail "held marker"
grep -q 'row-drop guard HELD' "$T/errors" || fail "guard not logged at error level"
mkdir -p "$T/state/force-publish"; : > "$T/state/force-publish/s__t"
sync >/dev/null || fail "override did not publish"
[ "$(hint)" = 7 ] && [ ! -e "$T/state/force-publish/s__t" ] || fail "override not applied once"
echo "PASS guard trips on 20% drop; override works"
setup_guard 1000 800 0 0 append; : > /dev/null
sync >/dev/null && fail "second hold after consumed override published"
echo "PASS override consumed once"
setup_guard 1000 1200 200 0 append
sync >/dev/null || fail "growth held"
[ "$(hint)" = 7 ] || fail "growth not published"
echo "PASS growth publishes"
# a rewrite in the window that deletes 30% more than it adds trips even if the final total recovered
setup_guard 1000 990 0 0 append
meta "$MT" 7 990 700 1000 overwrite   # deleted-added = 300 > 5% of 1000, total 990 within 5%
sync >/dev/null && fail "lossy overwrite published"
echo "PASS lossy overwrite snapshot holds"
setup_guard 1000 1000 1000 1000 replace   # compaction: deleted == added
sync >/dev/null || fail "compaction held"
echo "PASS compaction passes"
# first publish: no guard
reset; meta "$MT" 7 5 5 0 append; printf 'aaaa' > "$MT/data/a.parquet"
printf 'VERSION 7\nmetadata/v7.metadata.json\t-1\ndata/a.parquet\t4\n' > "$RESOLVE_OUT"
sync >/dev/null || fail "first publish held"
echo "PASS first publish unguarded"

# 7. an unreachable endpoint times out instead of hanging
cat > "$T/bin/rclone" <<'REOF'
#!/usr/bin/env bash
sleep 60
REOF
chmod +x "$T/bin/rclone"
start=$(date +%s)
r2_rclone 2 cat mn:x >/dev/null 2>&1 && fail "stalled rclone reported success"
[ $(( $(date +%s) - start )) -lt 20 ] || fail "stalled rclone not cut off"
echo "PASS stalled call times out"

# 8. a failing version-hint lookup (rc other than 0/3) is logged at error level, fails the run and
# is not silently skipped; rc 3 (no version-hint) is the non-Iceberg skip and does not fail it.
mkdir -p "$T/home"
rc=0
cat > "$T/bin/rclone" <<'REOF'
#!/usr/bin/env bash
last="${*: -1}"
case "$last" in
  minio:govdata-parquet-v1) echo "s/" ;;
  minio:govdata-parquet-v1/s) echo "t/"; echo "plain/" ;;
  *plain/metadata/version-hint.text) exit 3 ;;
  *t/metadata/version-hint.text) exit 1 ;;
esac
exit 0
REOF
chmod +x "$T/bin/rclone"
HOME="$T/home" PROD_AWS_ACCESS_KEY_ID=x PROD_AWS_SECRET_ACCESS_KEY=x PROD_AWS_ENDPOINT_OVERRIDE=http://127.0.0.1:1 \
  PATH="$T/bin:/usr/bin:/bin" bash "$DIR/catchup-sync-r2.sh" --dry-run --schema s > "$T/cu.out" 2>&1 || rc=$?
[ "$rc" -eq 1 ] || { cat "$T/cu.out"; fail "failing lookup did not fail the run (rc=$rc)"; }
grep -q "\[s/t\] version-hint lookup failed (rclone rc=1)" "$T/cu.out" || { cat "$T/cu.out"; fail "lookup failure not logged"; }
grep -q "\[s/plain\] skipped: not an Iceberg table" "$T/cu.out" || { cat "$T/cu.out"; fail "rc 3 not treated as non-Iceberg"; }
echo "PASS failing version-hint lookup is loud, rc 3 skips"
