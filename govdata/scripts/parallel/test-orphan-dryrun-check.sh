#!/usr/bin/env bash
# orphan-dryrun-check.sh runs once per day, stays metadata-only and read-only, and always exits 0 (a failed
# scan or alert step must not fail the caller); run-scheduled.sh launches it detached after the daily workers.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

mkdir -p "$tmp/parallel" "$tmp/run"
cp "$HERE/orphan-dryrun-check.sh" "$HERE/orphan-dryrun-alert.py" "$tmp/parallel/"
echo 'load_env() { :; }' > "$tmp/parallel/common.sh"
cat > "$tmp/orphan-file-cleanup.sh" <<'EOS'
#!/usr/bin/env bash
echo "$*" >> "$CALLS"
echo "x/y metadata: on disk 9, reachable 1, orphans 8, inside safety window 0, would delete 8 (80 bytes)"
EOS
chmod +x "$tmp"/orphan-file-cleanup.sh "$tmp"/parallel/orphan-dryrun-check.sh
export CALLS="$tmp/calls" GOVDATA_ORPHAN_DRYRUN_DIR="$tmp/run" GOVDATA_ORPHAN_THRESHOLD=1000

"$tmp/parallel/orphan-dryrun-check.sh" "$tmp/log"; rc=$?
[ "$rc" -eq 0 ] && ok "exits 0" || bad "rc $rc"
[ "$(cat "$tmp/calls")" = "--metadata-only" ] && ok "metadata-only, no other flag" || bad "args: $(cat "$tmp/calls")"
[ -f "$tmp/run/latest.json" ] && ok "result recorded" || bad "no latest.json"
"$tmp/parallel/orphan-dryrun-check.sh" "$tmp/log"
[ "$(wc -l < "$tmp/calls")" -eq 1 ] && grep -q 'already ran today' "$tmp/log" && ok "second call same day is a no-op" || bad "ran twice"

rm "$tmp/run/.last-run"; printf '#!/usr/bin/env bash\nexit 3\n' > "$tmp/orphan-file-cleanup.sh"
"$tmp/parallel/orphan-dryrun-check.sh" "$tmp/log"; rc=$?
[ "$rc" -eq 0 ] && grep -q 'alert step failed' "$tmp/log" && ok "failing scan: logged, still exit 0" || bad "failure case rc=$rc"
fn="$(sed -n '/^run_orphan_dryrun_check() {/,/^}/p' "$HERE/run-scheduled.sh")"
grep -q 'run_orphan_dryrun_check "\$window_log"' "$HERE/run-scheduled.sh" && grep -q ') &$' <<< "$fn" && ok "scheduler launches it detached" || bad "not wired"
exit $fail
