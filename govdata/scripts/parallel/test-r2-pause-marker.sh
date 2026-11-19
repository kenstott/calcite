#!/usr/bin/env bash
# Tests for the per-table R2 sync pause marker: a paused table is skipped (bulk copy and pointer
# publish) while other tables still sync; an expired marker is ignored; reserve waits for an
# in-flight unit on the table. No network: stub rclone, stubbed closure body, throwaway state dir.
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
# shellcheck disable=SC1091
source "$DIR/common.sh"
set +e
fail() { echo "FAIL: $*"; exit 1; }
export GOVDATA_R2_SYNC_STATE_DIR="$T/state"
mkdir -p "$T/state"
log_info() { echo "$*" >> "$T/info"; }
log_error() { echo "$*" >> "$T/errors"; }

# ── closure unit (pointer publish path) ──
# stub body: records the table and "publishes" version 9
_sync_iceberg_table_closure_body() { echo "$5" >> "$T/body-calls"; echo 9; }
sync_unit() { sync_iceberg_table_closure mn rr bkt "$1" "$2"; }

r2_pause_create sch ta 3600 test >/dev/null
out=$(sync_unit sch ta); rc=$?
[ "$rc" -eq 3 ] || fail "paused table returned rc=$rc, expected 3"
[ -z "$out" ] && [ ! -e "$T/body-calls" ] || fail "pointer publish ran for a paused table"
grep -q 'sch/ta\] PAUSED by marker' "$T/errors" || fail "honouring a marker not logged loudly"
echo "PASS paused table: skipped, pointer not published, honoured loudly"

out=$(sync_unit sch tb); rc=$?
[ "$rc" -eq 0 ] && [ "$out" = 9 ] && grep -qx tb "$T/body-calls" || fail "unpaused table did not sync (rc=$rc out=$out)"
echo "PASS other tables still synced"

ls "$T/state/inflight.d"/* >/dev/null 2>&1 && fail "in-flight registrations leaked"
echo "PASS in-flight registrations cleaned up"

# expired marker: ignored, removed, logged loudly, table syncs
printf 'created_at=1000\nexpires_at=2000\nowner=test\n' > "$T/state/pause.d/sch__tc"
out=$(sync_unit sch tc); rc=$?
[ "$rc" -eq 0 ] && [ "$out" = 9 ] || fail "expired marker still paused the table (rc=$rc)"
grep -q 'sch/tc\] pause marker EXPIRED' "$T/errors" || fail "expiry not logged"
[ ! -e "$T/state/pause.d/sch__tc" ] || fail "expired marker not removed"
echo "PASS expired marker ignored"

# unreadable marker: honoured (mtime + ttl), logged at error level
echo garbage > "$T/state/pause.d/sch__td"
sync_unit sch td >/dev/null; [ $? -eq 3 ] || fail "unreadable marker not honoured"
grep -q 'sch__td is unreadable' "$T/errors" || fail "unreadable marker not logged"
rm -f "$T/state/pause.d/sch__td"
echo "PASS unreadable marker honoured until mtime+ttl"

# paused-table listing feeds the bulk copy exclude
mapfile -t pt < <(r2_paused_tables sch)
[ "${pt[*]}" = "ta" ] || fail "r2_paused_tables: ${pt[*]}"
[ -z "$(r2_paused_tables other)" ] || fail "marker leaked across schemas"
echo "PASS paused table list is per schema"

# ── reserve waits for an in-flight unit that started before the marker ──
r2_pause_remove sch ta >/dev/null
sleep 10 & held=$!
mkdir -p "$T/state/inflight.d"
printf 'sch\tta\t%s\n' "$(( $(date +%s) - 5 ))" > "$T/state/inflight.d/$held.1"
printf 'sch\ttb\t%s\n' "$(( $(date +%s) + 60 ))" > "$T/state/inflight.d/$held.2"
created=$(date +%s)
r2_pause_wait_idle sch ta "$created" 2 && fail "wait_idle returned while a unit on the table was running"
r2_pause_wait_idle sch tb "$created" 2 || fail "a unit that started after the marker blocked the wait"
r2_pause_wait_idle sch other "$created" 2 || fail "a unit on another table blocked the wait"
echo "PASS wait_idle blocks only on earlier in-flight units for the table"
printf 'sch\t*\t%s\n' "$(( $(date +%s) - 5 ))" > "$T/state/inflight.d/$held.3"
r2_pause_wait_idle sch other "$created" 2 && fail "whole-schema bulk slice did not block"
wait "$held"
r2_pause_wait_idle sch ta "$created" 5 || fail "finished pass still blocked"
echo "PASS bulk slice blocks; a finished pass releases the wait"

# ── bulk copy: sync-to-r2.sh excludes the paused table, still copies the schema ──
mkdir -p "$T/bin" "$T/home"
cat > "$T/bin/rclone" <<'STUB'
#!/bin/bash
case "$1" in
  lsf)
    if [[ " $* " == *" -R "* ]]; then echo "2026-10-01 00:00:00"; exit 0; fi
    last="${@: -1}"
    if [[ "$last" =~ ^[a-z0-9_]+:[A-Za-z0-9._-]+$ ]]; then printf 'sch/\n'; fi
    exit 0 ;;
  copy) echo "$*" >> "$STUB_LOG"; exit 0 ;;
  *) exit 0 ;;
esac
STUB
chmod +x "$T/bin/rclone"
run_sync() {
  ( export HOME="$T/home" PATH="$T/bin:$PATH" STUB_LOG="$T/copies.log" GOVDATA_R2_SYNC_STATE_DIR="$T/home/.r2-sync-state" \
      PROD_AWS_ACCESS_KEY_ID=x PROD_AWS_SECRET_ACCESS_KEY=x PROD_AWS_ENDPOINT_OVERRIDE=http://localhost:1 \
      GOVDATA_R2_SYNC_SPLIT_SCHEMAS=none
    bash "$DIR/sync-to-r2.sh" --schemas sch 2>&1 ) > "$T/sync.out"
}
mkdir -p "$T/home/.r2-sync-state/pause.d"
: > "$T/copies.log"
run_sync
[ -s "$T/copies.log" ] || fail "schema copy did not run: $(tail -5 "$T/sync.out")"
grep -q -- '--exclude /ta/\*\*' "$T/copies.log" && fail "unpaused schema copy carries a pause exclude"
printf 'created_at=%s\nexpires_at=%s\nowner=test\n' "$(date +%s)" "$(( $(date +%s) + 3600 ))" > "$T/home/.r2-sync-state/pause.d/sch__ta"
: > "$T/copies.log"; : > "$T/home/.r2-sync-state/sch"
run_sync
grep -q -- '--exclude /ta/\*\*' "$T/copies.log" || fail "paused table not excluded from the bulk copy: $(cat "$T/copies.log")"
grep -q 'sch/ta\] PAUSED by marker' "$T/sync.out" || fail "bulk pass did not log the honoured marker"
echo "PASS bulk copy excludes the paused table only"
echo "ALL PASS"
