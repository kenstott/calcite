#!/usr/bin/env bash
# The `serial` slot runs beside its schema's year and :once slots only while neither side's table scope
# (GOVDATA_TABLES) crosses the serial split; otherwise both would write one table, so it is refused.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fail=0
ok() { echo "PASS $1"; }
bad() { echo "FAIL $1"; fail=1; }

tmp=$(mktemp -d); pids="$tmp/pids"; mkdir -p "$pids"; mkfifo "$tmp/hold"
holder=""
# A holder blocks on the fifo and exits when it reads a line.
stop_holder() {
  [ -n "$holder" ] && { echo >&9; exec 9>&-; wait "$holder"; }
  holder=""; rm -f "$pids"/*.pid
}
trap 'stop_holder; rm -rf "$tmp"' EXIT

# A jar holding this checkout's housing schema YAML, so the serial set is the real one.
python3 - "$tmp/schema.jar" "$HERE/../../src/main/resources/housing/housing-schema.yaml" <<'PY'
import sys, zipfile
zipfile.ZipFile(sys.argv[1], "w").write(sys.argv[2], "housing/housing-schema.yaml")
PY

# start_worker <mode> [<tables>]: a live process registered the way run-pool.sh's wrapper registers one.
start_worker() {
  stop_holder
  local pf="$pids/worker-housing-$1.1.pid"
  if [ -n "${2:-}" ]; then
    GOVDATA_TABLES="$2" bash -c 'read -r _; true' -- "$pf" < "$tmp/hold" &
  else
    env -u GOVDATA_TABLES bash -c 'read -r _; true' -- "$pf" < "$tmp/hold" &
  fi
  holder=$!
  exec 9> "$tmp/hold"
  echo "$holder" > "$pf"
  sleep 0.3
}

# admit <new_mode> <new_tables> <start> <end>
admit() {
  (
    exec 9>&-
    export GOVDATA_PARQUET_DIR=s3://govdata-parquet-v1-dq
    if [ -n "$2" ]; then export GOVDATA_TABLES="$2"; else unset GOVDATA_TABLES; fi
    source "$HERE/common.sh" >/dev/null 2>&1
    resolve_classpath() { [ -f "$test_jar" ] && echo "$test_jar"; }
    check_schema_year_conflict "$pids" housing "$3" "$4" "$1"
  ) 2>/dev/null
}
test_jar="$tmp/schema.jar"

admitted() { admit "$@" && ok "$desc" || bad "$desc: refused"; }
refused() { admit "$@" && bad "$desc: admitted" || ok "$desc"; }

serial_t=income_limits_county
year_t=house_price_index_county
once_t=house_price_index

start_worker serial
desc="unscoped year slot beside serial";            admitted 2018 "" 2018 2018
desc="unscoped once slot beside serial";            admitted once "" 0 9999
desc="year slot scoped to a year table";            admitted 2018 "$year_t" 2018 2018
desc="year slot scoped to a serial table";          refused 2018 "$serial_t" 2018 2018
desc="range slot with a serial table among others"; refused 2010-2024 "$year_t, $serial_t" 2010 2024
desc="once slot scoped to a serial table";          refused once "$serial_t" 0 9999
desc="second serial slot";                          refused serial "" 0 9999

start_worker 2018 "$serial_t"
desc="serial beside a year slot scoped to a serial table"; refused serial "" 0 9999
start_worker once "$serial_t"
desc="serial beside a once slot scoped to a serial table"; refused serial "" 0 9999
start_worker 2018 "$year_t"
desc="serial beside a year slot scoped to a year table";   admitted serial "" 0 9999
start_worker 2018
desc="serial beside an unscoped year slot";                admitted serial "" 0 9999
desc="serial scoped to a year table beside a year slot";   refused serial "$year_t" 0 9999
desc="serial scoped to a serial table beside a year slot"; admitted serial "$serial_t" 0 9999
start_worker serial "$year_t"
desc="year slot beside a serial slot scoped to a year table"; refused 2018 "" 2018 2018
start_worker once
desc="serial beside an unscoped once slot";                admitted serial "" 0 9999
desc="serial scoped to a snapshot table beside once";      refused serial "$once_t" 0 9999

start_worker serial
test_jar="$tmp/missing.jar"
desc="scoped slot refused when the serial set cannot be derived"; refused 2018 "$year_t" 2018 2018

exit $fail
