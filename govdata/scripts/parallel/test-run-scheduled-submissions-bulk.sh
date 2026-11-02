#!/usr/bin/env bash
# prepare_sec_submissions_bulk (historical window): a failed submissions.zip fetch is retried with
# backoff and never lets the pool start without the zip; a successful retry exports the zip path.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

mkdir -p "$tmp/scripts/parallel" "$tmp/bulk"
SCRIPT_DIR="$tmp/scripts/parallel"
ts() { echo "T"; }
log_error() { echo "ERROR: $*" >> "$tmp/errors"; }
SUBMISSIONS_BULK_DIR="$tmp/bulk"
SUBMISSIONS_RETRY_MIN_SECS=1
SUBMISSIONS_RETRY_MAX_SECS=1
eval "$(sed -n '/^prepare_sec_submissions_bulk() {/,/^}/p' "$HERE/run-scheduled.sh")"

# fetch script: fails until the counter file reaches $tmp/succeed_after calls, then writes the zip
cat > "$tmp/scripts/fetch-submissions-bulk.sh" <<EOS
#!/usr/bin/env bash
n=\$(( \$(cat "$tmp/n" 2>/dev/null || echo 0) + 1 )); echo \$n > "$tmp/n"
if [ \$n -gt \$(cat "$tmp/succeed_after") ]; then : > "\$1/submissions.zip"; exit 0; fi
echo "curl: (6) Could not resolve host: www.sec.gov" >&2; exit 1
EOS
chmod +x "$tmp/scripts/fetch-submissions-bulk.sh"

# 1. never succeeds: returns 1 before the window ends, no zip exported, error reported
echo 1000 > "$tmp/succeed_after"; rm -f "$tmp/n"; : > "$tmp/errors"; : > "$tmp/w.log"
export SEC_BULK_SUBMISSIONS_ZIP=stale
prepare_sec_submissions_bulk "$tmp/w.log" $(( $(date +%s) + 4 )); rc=$?
[ "$rc" -eq 1 ] && ok "a failing fetch returns 1 (pool must not start)" || bad "rc=$rc"
[ -z "${SEC_BULK_SUBMISSIONS_ZIP:-}" ] && ok "no zip path exported after failure" || bad "zip var still set"
[ "$(cat "$tmp/n")" -ge 2 ] && ok "the fetch was retried ($(cat "$tmp/n") attempts)" || bad "not retried"
grep -q "pool not started" "$tmp/w.log" && grep -q "ERROR" "$tmp/errors" && ok "failure logged" || bad "failure not logged"

# 2. fails twice then succeeds: returns 0 and exports the zip
echo 2 > "$tmp/succeed_after"; rm -f "$tmp/n" "$tmp/bulk/submissions.zip"
prepare_sec_submissions_bulk "$tmp/w.log" $(( $(date +%s) + 60 )); rc=$?
[ "$rc" -eq 0 ] && [ "$SEC_BULK_SUBMISSIONS_ZIP" = "$tmp/bulk/submissions.zip" ] \
  && ok "retry succeeds and exports the zip" || bad "rc=$rc zip=${SEC_BULK_SUBMISSIONS_ZIP:-}"

# 3. the run_window call site must stop (not launch the pool) when preparation fails
grep -q 'prepare_sec_submissions_bulk "\$window_log" "\$window_end" || break' "$HERE/run-scheduled.sh" \
  && ok "run_window breaks out before run-pool.sh on failure" || bad "call site does not gate the pool"
exit $fail
