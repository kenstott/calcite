#!/bin/bash
# govdata-ops#1507: every worker that builds its own model scopes enabledTables to GOVDATA_TABLES
# (--tables). Runs the real worker scripts against the checkout's schema YAMLs, with the ETL
# launch, env loading and jar lookup replaced.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
RESOURCES="$(cd "$HERE/../../src/main/resources" && pwd)"
fail=0
ok()  { echo "ok   $1"; }
bad() { echo "FAIL $1"; fail=1; }

SANDBOX="$(mktemp -d)"
trap 'rm -rf "$SANDBOX"' EXIT
cp "$HERE"/worker-{edu,patents,lands,health,cyber}.sh "$SANDBOX"/
cat > "$SANDBOX/common.sh" <<STUB
source "$HERE/common.sh"
SCRIPT_DIR="$SANDBOX"
GOVDATA_ROOT="$HERE/../.."
load_env() { export GOVDATA_PARQUET_DIR=s3://test-dq GOVDATA_CACHE_DIR=s3://test-raw API_DATA_GOV=x; }
check_schema_year_conflict() { return 0; }
table_in_window() { return 0; }
within_release_window() { return 0; }
tracker_operand_json() { echo '"trackerStub": true'; }
log_info() { :; }
schema_tables() {
  python3 - "$RESOURCES/\$(schema_yaml_resource "\$1")" <<'PY'
import sys, yaml
doc = yaml.safe_load(open(sys.argv[1]))
print(",".join(t["name"] for k in ("partitionedTables", "tables") for t in doc.get(k) or []))
PY
}
run_etl() {
  python3 -c 'import json,sys; print(",".join(json.load(open(sys.argv[1]))["schemas"][0]["operand"]["enabledTables"]))' "\$1" >> "\$RUN_LOG"
}
STUB

# run <worker> <tables> <args...>: leaves one line per launched model (its enabledTables) in $RUN_LOG
export RUN_LOG="$SANDBOX/models.log"
run() {
  local worker=$1 tables=$2
  shift 2
  : > "$RUN_LOG"
  GOVDATA_TABLES="$tables" AWS_ACCESS_KEY_ID=k bash "$SANDBOX/worker-$worker.sh" "$@" \
    > "$SANDBOX/out.log" 2>&1
}

# expect_only <label> <table> <worker> <args...>: at least one model, each holding exactly <table>
expect_only() {
  local label=$1 table=$2 worker=$3
  shift 3
  if ! run "$worker" "$table" "$@"; then
    bad "$label: worker failed: $(tail -3 "$SANDBOX/out.log")"
    return
  fi
  local got
  got="$(sort -u "$RUN_LOG")"
  [ "$got" = "$table" ] && ok "$label" || bad "$label: models enabled '$(tr '\n' ';' < "$RUN_LOG")'"
}

for mode in historical 2017-2020 daily; do
  expect_only "edu $mode" ipeds_admissions edu "$mode" --force
  expect_only "patents $mode" patent_claims patents "$mode" --force
done
expect_only "lands 2017-2020" timber_sales lands 2017-2020
expect_only "health historical" who_gho_indicators health historical
expect_only "cyber_vuln daily" kev_catalog cyber daily cyber_vuln
expect_only "cyber_threat daily" threat_pulses cyber daily cyber_threat

for spec in "edu historical" "patents historical" "lands historical" "health historical" \
    "cyber daily cyber_threat"; do
  read -r -a args <<< "$spec"
  if run "${args[0]}" no_such_table "${args[@]:1}"; then
    bad "$spec: unknown table name accepted"
  elif [ -s "$RUN_LOG" ]; then
    bad "$spec: unknown table name still launched a model"
  else
    grep -q "no_such_table" "$SANDBOX/out.log" && ok "$spec: unknown table name fails, named" \
      || bad "$spec: failure does not name the table: $(tail -3 "$SANDBOX/out.log")"
  fi
done

if run edu "" historical && [ "$(wc -l < "$RUN_LOG")" -gt 5 ]; then
  ok "edu historical unscoped runs every group"
else
  bad "edu unscoped: $(wc -l < "$RUN_LOG") models"
fi
exit $fail
