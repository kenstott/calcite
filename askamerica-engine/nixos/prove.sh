#!/usr/bin/env bash
# Run INSIDE the NixOS guest by the `nixos` job of pgwire-adapters-release.yml.
# Proves requirement ASKAM-001 for the pg-wire server bundles built by the same run: on a host
# that provides only askamerica-engine/nixos/host.nix, a bundle unpacked read-only starts from
# the runtimes it carries and answers a query.
set -euo pipefail

test -e /etc/NIXOS || { echo "this is not a NixOS host" >&2; exit 1; }
if command -v java >/dev/null 2>&1; then
  echo "the guest has a java on PATH; the proof must not depend on one" >&2
  exit 1
fi
unset JAVA_HOME

bundles=/mnt/bundles
work="$HOME/proof"
rm -rf "$work"
mkdir -p "$work"
cd "$work"

wait_for_port() {  # port, seconds, log
  local port="$1" seconds="$2" log="$3" waited=0
  until (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
    waited=$((waited + 2))
    if [ "$waited" -ge "$seconds" ]; then
      echo "port $port did not open within ${seconds}s" >&2
      tail -n 200 "$log" >&2
      return 1
    fi
    sleep 2
  done
  echo "port $port is up after ${waited}s"
}

bundled_jvm_started() {  # bundle directory, log
  grep -F "JVM library: $1/jre/" "$2" \
    || { echo "the Calcite JVM did not start from the bundle's own runtime" >&2; tail -n 200 "$2" >&2; return 1; }
  grep -F "JVM started" "$2" \
    || { echo "the log has no 'JVM started'" >&2; tail -n 200 "$2" >&2; return 1; }
}

# --- The file bundle: no credentials, a real query. -------------------------------------------
echo "=== pgwire-file"
mkdir file
tar -xzf "$bundles"/pgwire-file-*-linux-x86_64.tar.gz -C file --strip-components=1
mkdir -p file/model/data
printf 'id,name\n1,alice\n2,bob\n' > file/model/data/people.csv
# Read-only, as the release's smoke test has it: everything the bundle ships except model/,
# where the file adapter keeps its data and its cache.
for part in file/*; do
  [ "$(basename "$part")" = "model" ] || chmod -R a-w "$part"
done
chmod a-w file
PGWIRE_CALCITE_STATE_DIR="$work/file-state" \
  "$work/file/bin/pgwire-file" --port 5455 --auth trust > file.log 2>&1 &
file_pid=$!
wait_for_port 5455 180 file.log
rows="$(psql "host=127.0.0.1 port=5455 user=tester dbname=postgres" -At -c "SELECT count(*) FROM people;")"
test "$rows" = "2" || { echo "expected 2 rows in people, got '$rows'" >&2; tail -n 200 file.log >&2; exit 1; }
bundled_jvm_started "$work/file" file.log
test ! -e file/jars.classpath || { echo "jars.classpath was written into the install tree" >&2; exit 1; }
kill "$file_pid"
echo "pgwire-file: started from a read-only tree on its own runtime and answered a query"

# --- The govdata bundle: AskAmerica's own server. ---------------------------------------------
echo "=== pgwire-govdata"
mkdir govdata
tar -xzf "$bundles"/pgwire-govdata-*-linux-x86_64.tar.gz -C govdata --strip-components=1
state="$work/govdata-state"
mkdir -p "$state"
# The official catalog is the seed packaged in the bundle's jar. Until the server puts it in
# place itself, its caller does (as Provisa does): extract it and name it.
"$work/govdata/cpython/bin/python" - "$work/govdata/jars" "$state" <<'PY'
import glob, io, sys, zipfile
jars, state = sys.argv[1], sys.argv[2]
for jar in sorted(glob.glob(jars + "/*.jar")):
    with zipfile.ZipFile(jar) as z:
        if "duckdb/seed/govdata-seed.zip" in z.namelist():
            with zipfile.ZipFile(io.BytesIO(z.read("duckdb/seed/govdata-seed.zip"))) as seed:
                seed.extractall(state)
            print("seed taken from", jar)
            break
else:
    sys.exit("no jar in the bundle carries duckdb/seed/govdata-seed.zip")
PY
test -f "$state/.duckdb/govdata.duckdb"
# The whole tree read-only: nothing the server writes may land in it.
chmod -R a-w govdata
# The model requires these to be set. No store is read by this proof: placeholders.
(
  cd "$state"
  PGWIRE_CALCITE_STATE_DIR="$state" \
  GOVDATA_DUCKDB_CATALOG="$state/.duckdb/govdata.duckdb" \
  GOVDATA_PARQUET_DIR="${GOVDATA_PARQUET_DIR:-s3://askamerica-nixos-proof/placeholder}" \
  AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-placeholder}" \
  AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-placeholder}" \
  AWS_ENDPOINT_OVERRIDE="${AWS_ENDPOINT_OVERRIDE:-https://placeholder.invalid}" \
    exec "$work/govdata/bin/pgwire-govdata" --port 5456 --auth trust
) > govdata.log 2>&1 &
govdata_pid=$!
wait_for_port 5456 900 govdata.log
bundled_jvm_started "$work/govdata" govdata.log
one="$(psql "host=127.0.0.1 port=5456 user=tester dbname=postgres" -At -c "SELECT 1;")"
test "$one" = "1" || { echo "SELECT 1 returned '$one'" >&2; tail -n 200 govdata.log >&2; exit 1; }
tables="$(psql "host=127.0.0.1 port=5456 user=tester dbname=postgres" -At \
  -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'sec';")"
test "$tables" -gt 0 || { echo "the catalog lists no table in schema sec (got '$tables')" >&2; tail -n 200 govdata.log >&2; exit 1; }
written="$(find "$work/govdata" -newer "$state/.duckdb" -type f | head -5)"
test -z "$written" || { echo "files were written into the read-only install tree: $written" >&2; exit 1; }
kill "$govdata_pid"
echo "pgwire-govdata: started from a read-only tree on its own runtime, listed $tables tables in sec"
