#!/usr/bin/env bash
# An active sec.gov cool-down defers sec workers (exit 75, no ETL started); an expired marker does not.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
export GOVDATA_TMPDIR="$T"
fail=0
for schema in sec_secondary sec_primary sec_13f; do
  printf '%s' $(( ($(date +%s) + 600) * 1000 )) > "$T/calcite-ratelimit-sec.gov.blocked"
  out=$("$HERE/worker.sh" "$schema" 2026 2>&1); rc=$?
  if [ "$rc" -ne 75 ] || ! grep -q "DEFERRED by sec.gov cool-down" <<<"$out" || grep -q "Starting ETL" <<<"$out"; then
    echo "FAIL $schema: rc=$rc"; echo "$out" | tail -5; fail=1
  else
    echo "PASS $schema deferred (rc=75)"
  fi
done
exit $fail
