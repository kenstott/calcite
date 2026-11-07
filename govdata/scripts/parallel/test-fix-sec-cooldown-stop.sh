#!/usr/bin/env bash
# fix_sec_cooldown_ended (#1113): a worker round that ends on the sec.gov host cool-down stops fix-sec.sh.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/fix-sec-lib.sh"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }
t=$(mktemp -d); trap 'rm -rf "$t"' EXIT

fix_sec_cooldown_ended 75 "" && ok "exit 75 stops" || bad "exit 75"
echo "Host cool-down: stopping after 3 of 2000 accessions ... HTTP 403/429; no requests until X" > "$t/a.log"
fix_sec_cooldown_ended 1 "$t/a.log" && ok "cool-down line stops" || bad "cool-down line"
echo "HTTP 403 from https://data.sec.gov/x: no sec.gov requests from this host until X" > "$t/b.log"
fix_sec_cooldown_ended 1 "$t/b.log" && ok "403 refusal line stops" || bad "403 line"
echo "all fine" > "$t/c.log"
fix_sec_cooldown_ended 1 "$t/c.log" && bad "unrelated failure must not match" || ok "unrelated failure does not match"
fix_sec_cooldown_ended 0 "" && bad "clean exit must not match" || ok "clean exit does not match"

# fix-sec.sh wires it in right after the worker call, ahead of the generic failure path, and exits.
w=$(grep -n 'worker-sec-reprocess.sh" --schema' "$HERE/fix-sec.sh" | head -1 | cut -d: -f1)
c=$(grep -n 'fix_sec_cooldown_ended "\$_wrc"' "$HERE/fix-sec.sh" | head -1 | cut -d: -f1)
[ -n "$w" ] && [ -n "$c" ] && [ "$c" -gt "$w" ] && sed -n "${c},$((c+10))p" "$HERE/fix-sec.sh" | grep -q 'exit 3' \
  && ok "fix-sec.sh stops on cool-down (exit 3) after the worker call" || bad "wiring w=$w c=$c"
exit $fail
