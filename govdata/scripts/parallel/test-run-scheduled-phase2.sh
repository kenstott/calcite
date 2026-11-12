#!/usr/bin/env bash
# run_pre_daily_release_phase2 (end of a daily window): calls pre-daily-release.sh --release once per
# window, reports a refused gate (exit 3) and any other failure, and never returns non-zero.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }
mkdir -p "$tmp/scripts"
ts() { echo "T"; }
log_error() { echo "ERROR: $*" >> "$tmp/errors"; }
SCRIPT_DIR="$tmp/scripts"
eval "$(sed -n '/^run_pre_daily_release_phase2() {/,/^}/p' "$HERE/run-scheduled.sh")"
fake() { printf '#!/usr/bin/env bash\necho "$*" >> "%s/calls"\nexit %s\n' "$tmp" "$1" > "$tmp/scripts/pre-daily-release.sh"; chmod +x "$tmp/scripts/pre-daily-release.sh"; }

fake 0; : > "$tmp/calls"; : > "$tmp/errors"; : > "$tmp/w.log"
run_pre_daily_release_phase2 "$tmp/w.log" >/dev/null; rc1=$?
run_pre_daily_release_phase2 "$tmp/w.log" >/dev/null
[ "$(cat "$tmp/calls")" = "--release" ] && [ "$rc1" -eq 0 ] && ok "runs --release once per window" || bad "calls: $(cat "$tmp/calls")"
fake 3; : > "$tmp/errors"; : > "$tmp/w.log"; run_pre_daily_release_phase2 "$tmp/w.log" >/dev/null; rc=$?
[ "$rc" -eq 0 ] && grep -q "gate refused" "$tmp/errors" && ok "refused gate reported, window continues" || bad "gate: rc=$rc $(cat "$tmp/errors")"
fake 1; : > "$tmp/errors"; : > "$tmp/w.log"; run_pre_daily_release_phase2 "$tmp/w.log" >/dev/null; rc=$?
[ "$rc" -eq 0 ] && grep -q "failed (exit 1)" "$tmp/errors" && ok "failure reported, window continues" || bad "failure: rc=$rc"
exit $fail
