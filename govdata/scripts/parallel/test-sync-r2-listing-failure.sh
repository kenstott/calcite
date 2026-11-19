#!/bin/bash
# A directory listing that fails or times out must fail the schema's pass, not read as "no subdirectories"
# (govdata-ops#1226). Runs _copy_split_schema against a stub rclone; no network, throwaway state dir.
set -u
S="$(cd "$(dirname "$0")" && pwd)/sync-to-r2.sh"; fail=0
ok() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi; }
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin" "$T/state"
cat > "$T/bin/rclone" <<'STUB'
#!/bin/bash
case "$1" in
  lsf)
    last="${@: -1}"
    [ "${STUB_FAIL_LSF:-}" = "$last" ] && exit 1
    case "$last" in
      *:b/sec)      printf 'alpha/\nbeta/\n' ;;
      *:b/sec/beta) printf 'c1/\nc2/\n' ;;
    esac
    exit 0 ;;
  copy) echo "$*" >> "$STUB_LOG"; exit 0 ;;
esac
STUB
chmod +x "$T/bin/rclone"
export PATH="$T/bin:$PATH" STUB_LOG="$T/copies"

{ sed -n '/^LEGACY_EXCLUDE=/,/^# ── Pointer pinning/p' "$S" | grep -v "^# ── Pointer"
  sed -n '/^# Seconds in a timeout-style/,/^run_pass() {/p' "$S" | grep -v '^run_pass() {'; } > "$T/helpers.sh"
log_error() { echo "$*" >&2; }; log_info() { :; }
r2_inflight_begin() { echo reg; }; r2_inflight_end() { :; }; r2_pause_created_at() { return 1; }; _r2_state_dir() { echo "$T/pause"; }
MINIO_REMOTE=minio; R2_REMOTE=r2; BUCKETS=(b); DRY_RUN=false; STATE_DIR="$T/state"; SPLIT_SCHEMAS=sec
SLICE_MAX=60s; UNIT_MAX=20s; TRANSFERS=1; CHECKERS=1; TPSLIMIT=10; TPSLIMIT_BURST=1; _VERBOSE_FLAG=""; BUFFER=120
source "$T/helpers.sh"
now=$(date +%s)
state_empty() { [ -z "$(ls -A "$STATE_DIR" | grep -v '^\.unit-try-')" ]; }

: > "$STUB_LOG"; STUB_FAIL_LSF="minio:b/sec" _copy_split_schema sec $((now-100)) $now $now 2>/dev/null; rc=$?
ok "failed schema directory listing returns non-zero (rc=$rc)"  '[ "$rc" -eq 75 ]'
ok "failed listing: no unit copied"                              '[ ! -s "$STUB_LOG" ]'
ok "failed listing: no cursor written"                           'state_empty'

: > "$STUB_LOG"; touch "$STATE_DIR/.deep-sec-beta"
STUB_FAIL_LSF="minio:b/sec/beta" _copy_split_schema sec $((now-100)) $now $now 2>/dev/null; rc=$?
ok "failed deep-directory listing returns non-zero (rc=$rc)"     '[ "$rc" -eq 75 ]'
ok "failed deep listing: no unit copied (not even sec/alpha)"    '[ ! -s "$STUB_LOG" ]'
ok "failed deep listing: no cursor written"                      '[ -z "$(ls -A "$STATE_DIR" | grep "^\.unit-cursor")" ]'

: > "$STUB_LOG"; _copy_split_schema sec $((now-100)) $now $now 2>/dev/null; rc=$?
ok "control: working listings copy every unit (rc=$rc)"          '[ "$rc" -eq 0 ] && grep -q "sec/alpha " "$STUB_LOG" && grep -q "sec/beta/c1 " "$STUB_LOG" && grep -q "sec/beta/c2 " "$STUB_LOG"'

# _capture_pins and the whole run_pass handling of a failed listing
sed -n '/^_capture_pins() {/,/^}/p' "$S" > "$T/pins.sh"; source "$T/pins.sh"
r2_rclone() { :; }
STUB_FAIL_LSF="minio:b/sec" _capture_pins sec >/dev/null 2>&1; rc=$?
ok "failed pointer-capture listing returns non-zero (rc=$rc)"    '[ "$rc" -ne 0 ]'
exit $fail
