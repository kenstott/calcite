#!/usr/bin/env bash
# Run INSIDE the NixOS guest, after prove.sh, by the `nixos` job of pgwire-adapters-release.yml.
# Shows AskAmerica's own data being served on NixOS: the govdata bundle that prove.sh unpacked
# read-only is started again, this time with the object store's credentials, and answers three
# small read-only queries. Nothing is written to the store.
#
# The four values arrive in this process's environment over ssh. They are never written to a
# file, never passed on a command line, and never printed: no `set -x`, and the server log is
# shown only through redact() below.
set +x
set -euo pipefail

for name in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_ENDPOINT_OVERRIDE GOVDATA_PARQUET_DIR; do
  [ -n "${!name:-}" ] || { echo "$name did not reach the guest" >&2; exit 1; }
done
unset JAVA_HOME

work="$HOME/proof"
state="$work/govdata-state"
log="$work/govdata-data.log"
test -x "$work/govdata/bin/pgwire-govdata" || { echo "prove.sh has not unpacked the govdata bundle" >&2; exit 1; }
test -f "$state/.duckdb/govdata.duckdb" || { echo "prove.sh has not put the seed catalog in place" >&2; exit 1; }

redact() {  # the server log, with the four values removed whatever else masks them
  sed -e "s|${AWS_SECRET_ACCESS_KEY}|***|g" -e "s|${AWS_ACCESS_KEY_ID}|***|g" \
      -e "s|${AWS_ENDPOINT_OVERRIDE}|***|g" -e "s|${GOVDATA_PARQUET_DIR}|***|g"
}
show_log() { tail -n 200 "$log" | redact >&2; }

(
  cd "$state"
  PGWIRE_CALCITE_STATE_DIR="$state" \
  GOVDATA_DUCKDB_CATALOG="$state/.duckdb/govdata.duckdb" \
    exec "$work/govdata/bin/pgwire-govdata" --port 5457 --auth trust
) > "$log" 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true' EXIT

waited=0
until (exec 3<>/dev/tcp/127.0.0.1/5457) 2>/dev/null; do
  waited=$((waited + 2))
  if [ "$waited" -ge 900 ]; then echo "port 5457 did not open within 900s" >&2; show_log; exit 1; fi
  sleep 2
done
echo "port 5457 is up after ${waited}s"

query() {  # one value back, or the redacted log and a failure
  local out
  if ! out="$(PGCONNECT_TIMEOUT=30 psql "host=127.0.0.1 port=5457 user=tester dbname=postgres" -At -c "$1" 2>&1)"; then
    echo "query failed: $1" >&2
    echo "$out" | redact >&2
    show_log
    exit 1
  fi
  echo "$out"
}

one="$(query "SELECT 1;")"
test "$one" = "1" || { echo "SELECT 1 returned '$one'" >&2; show_log; exit 1; }
echo "SELECT 1 -> 1"

tables="$(query "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'sec';")"
[ "$tables" -gt 0 ] 2>/dev/null || { echo "the catalog lists no table in schema sec (got '$tables')" >&2; show_log; exit 1; }
echo "tables in schema sec -> $tables"

# One bounded read of AskAmerica's own data: a single row of a small reference table.
rows="$(query "SELECT count(*) FROM (SELECT * FROM econ_reference.naics_sectors LIMIT 1) AS one_row;")"
test "$rows" = "1" || { echo "expected 1 row from econ_reference.naics_sectors, got '$rows'" >&2; show_log; exit 1; }
echo "econ_reference.naics_sectors LIMIT 1 -> 1 row"

grep -qF "JVM library: $work/govdata/jre/" "$log" \
  || { echo "the Calcite JVM did not start from the bundle's own runtime" >&2; show_log; exit 1; }
written="$(find "$work/govdata" -newer "$state/.duckdb" -type f | head -5)"
test -z "$written" || { echo "files were written into the read-only install tree: $written" >&2; exit 1; }
echo "pgwire-govdata: served AskAmerica's data on NixOS from a read-only tree on its own runtime"
