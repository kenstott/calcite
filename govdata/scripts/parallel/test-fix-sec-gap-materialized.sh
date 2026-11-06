#!/usr/bin/env bash
# An attempted pair stays open while its table lacks the row, and closes once the row exists.
# The "table" is a local parquet file read through duckdb, anti-joined the way fix-sec.sh does.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/fix-sec-lib.sh"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT

gaps() { # $1 = parquet of present accessions
  duckdb -noheader -list -c "SELECT a||'|filing_metadata' FROM (VALUES ('A1'),('A2'),('A3')) v(a)
    WHERE a NOT IN (SELECT acc FROM read_parquet('$1')) ORDER BY 1"
}
duckdb -c "COPY (SELECT 'A1' AS acc) TO '$tmp/before.parquet'" >/dev/null
gaps "$tmp/before.parquet" > "$tmp/before.txt"
printf 'A2|filing_metadata\nA3|filing_metadata\n' > "$tmp/attempted.txt"

[ "$(fix_sec_attempted_absent "$tmp/before.txt" "$tmp/attempted.txt")" = 2 ] \
  && ok "attempted and absent pairs are counted, not retired" || bad "attempted pairs retired"

duckdb -c "COPY (SELECT unnest(['A1','A2']) AS acc) TO '$tmp/after.parquet'" >/dev/null
gaps "$tmp/after.parquet" > "$tmp/after.txt"
[ "$(fix_sec_outcome "$tmp/before.txt" "$tmp/after.txt")" = "1 1" ] \
  && ok "present pair closes (1 materialized), absent pair stays open (1)" || bad "outcome wrong"

grep -q 'fix_sec_outcome' "$HERE/fix-sec.sh" && ok "fix-sec.sh reports the outcome" || bad "fix-sec.sh does not report it"
! grep -q 'comm -23 "\$ACC_FILE.pairs.all"' "$HERE/fix-sec.sh" && ok "attempted-list no longer subtracted" || bad "still subtracted"
exit $fail
