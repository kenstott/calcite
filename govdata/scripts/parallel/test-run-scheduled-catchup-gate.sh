#!/usr/bin/env bash
# catchup_hold_secs: the R2 catchup waits while a daily window's pool and x-schema step run, and
# opens when the marker is cleared or its window-end epoch has passed.
# run_window marks the daily window busy and clears it after x-schema and phase 2.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }
DAILY_BUSY_FILE="$tmp/daily-busy"
eval "$(sed -n '/^mark_daily_busy() /p' "$HERE/run-scheduled.sh")"
eval "$(sed -n '/^clear_daily_busy() /p' "$HERE/run-scheduled.sh")"
eval "$(sed -n '/^catchup_hold_secs() {/,/^}/p' "$HERE/run-scheduled.sh")"

[ "$(catchup_hold_secs 1000)" = "0" ] && ok "no marker: open" || bad "no marker"
mark_daily_busy 5000
[ "$(catchup_hold_secs 1000)" = "4000" ] && ok "daily busy: holds until window end" || bad "busy: $(catchup_hold_secs 1000)"
[ "$(catchup_hold_secs 5000)" = "0" ] && ok "window end reached: open (crashed runner cannot hold it)" || bad "expired"
[ "$(catchup_hold_secs 6000)" = "0" ] && ok "past window end: open" || bad "past"
clear_daily_busy
[ "$(catchup_hold_secs 1000)" = "0" ] && ok "cleared after daily work: open" || bad "cleared"
echo "garbage" > "$DAILY_BUSY_FILE"
[ "$(catchup_hold_secs 1000)" = "0" ] && ok "unreadable marker: open" || bad "garbage"

src="$HERE/run-scheduled.sh"
grep -q '^  \[ "\$mode" = "daily" \] && mark_daily_busy "\$window_end"' "$src" && ok "daily window marks busy" || bad "mark site"
[ "$(grep -c '^ *clear_daily_busy$' "$src")" = "2" ] && ok "cleared at the fill transition and the end-of-window backstop" || bad "clear sites"
grep -q 'catchup_hold_secs "\$(date +%s)"' "$src" && ok "catchup loop consults the gate" || bad "gate not wired"
exit $fail
