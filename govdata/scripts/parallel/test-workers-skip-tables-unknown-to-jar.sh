#!/bin/bash
# govdata-ops#1393: workers with hand-maintained table lists skip, with a WARNING, tables the staged
# jar does not know and run the rest; an explicit GOVDATA_TABLES name is kept so it still fails loudly.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
fail=0
ok()  { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }

extract_fn() { awk -v n="$1" '$0 ~ "^"n"\\(\\) \\{" {p=1} p{print} p&&/^}/{exit}' "$HERE/common.sh"; }
schema_tables() { echo "known_a,known_b"; }
eval "$(extract_fn drop_tables_unknown_to_jar)"
eval "$(extract_fn filter_enabled_tables)"

for s in cyber_vuln lands edu patents; do
  unset GOVDATA_TABLES
  out=$(drop_tables_unknown_to_jar "$s" "known_a,new_table,known_b" 2>"$(mktemp)")
  [ "$out" = "known_a,known_b" ] && ok "$s: unknown table dropped, rest kept" || bad "$s: got '$out'"
  warn=$(drop_tables_unknown_to_jar "$s" "known_a,new_table" 2>&1 >/dev/null)
  case "$warn" in WARNING*new_table*) ok "$s: WARNING names the table" ;; *) bad "$s: no warning ('$warn')" ;; esac
done

GOVDATA_TABLES="new_table" out=$(drop_tables_unknown_to_jar lands "known_a,new_table" 2>/dev/null)
[ "$out" = "known_a,new_table" ] && ok "explicit GOVDATA_TABLES unknown name kept (fails loudly downstream)" || bad "explicit kept '$out'"

for w in cyber lands edu patents; do
  grep -q "drop_.*tables_unknown_to_jar" "$HERE/worker-$w.sh" && ok "worker-$w.sh calls the helper" || bad "worker-$w.sh does not call the helper"
done
exit $fail
