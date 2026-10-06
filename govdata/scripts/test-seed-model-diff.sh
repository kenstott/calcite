#!/usr/bin/env bash
# Tests seed-model-diff.py: only a real model change counts, never generated coverage metadata,
# comments, or formatting.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
cd "$tmp"; git init -q .; git config user.email t@t; git config user.name t
R=govdata/src/main/resources; mkdir -p $R/aa $R/bb
cat > $R/aa/aa-schema.yaml <<'Y'
partitionedTables:
  - name: t1
    comment: first
    observedCoverage:
      rowCount: 10
      checkedAt: "2026-10-01"
Y
cat > $R/bb/bb-schema.yaml <<'Y'
partitionedTables:
  - name: u1
Y
git add -A; git commit -qm base; base=$(git rev-parse HEAD)
fail=0
expect() { # name expected-output
  local got; got="$(python3 "$HERE/seed-model-diff.py" "$base")"
  if [ "$got" = "$2" ]; then echo "PASS $1"; else echo "FAIL $1: got '[$got]' want '[$2]'"; fail=1; fi
  git checkout -q -- . ; git clean -fdq; git reset -q --hard "$base"
}
# coverage-only change in aa
sed -i 's/rowCount: 10/rowCount: 99/; s/2026-10-01/2026-10-06/' $R/aa/aa-schema.yaml
git commit -qam cov; expect "coverage metadata only is ignored" ""
# a new observedCoverage block on bb
printf '    observedCoverage:\n      minYear: 2010\n' >> $R/bb/bb-schema.yaml
git commit -qam cov2; expect "a new coverage block is ignored" ""
# comment / formatting only
sed -i 's/^partitionedTables:/# a comment\npartitionedTables:/' $R/aa/aa-schema.yaml
git commit -qam cmt; expect "a comment is ignored" ""
# real model change in bb
printf '  - name: u2\n' >> $R/bb/bb-schema.yaml
git commit -qam model; expect "a new table is detected" "$R/bb/bb-schema.yaml"
# real change plus coverage noise in aa
sed -i 's/comment: first/comment: second/; s/rowCount: 10/rowCount: 5/' $R/aa/aa-schema.yaml
git commit -qam mix; expect "a changed comment field is detected" "$R/aa/aa-schema.yaml"
# new schema file
mkdir -p $R/cc; printf 'partitionedTables: []\n' > $R/cc/cc-schema.yaml
git add -A; git commit -qm new; expect "a new schema file is detected" "$R/cc/cc-schema.yaml"
# unparseable file is an error, not 'unchanged'
printf 'a: [unclosed\n' > $R/aa/aa-schema.yaml; git commit -qam bad
if python3 "$HERE/seed-model-diff.py" "$base" >/dev/null 2>&1; then echo "FAIL unparseable YAML did not error"; fail=1; else echo "PASS unparseable YAML errors"; fi
exit $fail
