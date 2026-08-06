#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# model-verify.sh — introspect every govdata table through Calcite and query each one,
# ONE SCHEMA AT A TIME.
#
# Each schema is verified in its own jdbc:govdata:source=<schema> connection (via
# GovDataDriver, which builds the autoDownload=false model from the schema YAMLs baked
# into the jar). A single connection that mounts all 18 schemas eagerly creates a view
# for every one of ~130 Iceberg tables before any probe runs; per-schema connects are
# each fast, so this loops the runner once per dataSource and streams results as it goes.
#
# Env comes from .env.prod (REQUIRED) — object store endpoint/credentials, GOVDATA_PARQUET_DIR.
#
# Usage:
#   scripts/model-verify.sh                          # all schemas, one at a time
#   scripts/model-verify.sh --source sec,geo,econ    # subset
#   scripts/model-verify.sh --limit 5
#
# Options:
#   --source LIST   comma/space-separated dataSource list (default: all supported)
#   --limit N       rows to fetch per table probe (default 1)
#   --no-probes     skip the geo/embedding feature probes
#   --no-dup        skip the primary-key duplicate-row check
#   --dup-threshold N  min duplicate keyed-row count to flag/fail (default 1)
#
# Duplicate-row check: for every base table with a declared primaryKey, the runner compares the
# count of fully-keyed rows (all PK columns non-null) against the count of DISTINCT PK tuples. A
# non-unique declared PK (>= threshold duplicate rows) is reported in the DUP column and fails the
# schema — it is the signature of the Iceberg file-duplication bug. Views / tables with no PK show
# DUP=- and are not checked.
#
# Exit code: 0 if every schema passed; 1 if any schema reported ERROR / probe failure / duplicate PK.
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"
RUNNER="org.apache.calcite.adapter.govdata.etl.GovDataModelVerificationRunner"

# Canonical primary dataSource names (GovDataSchemaFactory.getFactoryForDataSource switch).
# Must stay in step with GovDataModelVerificationRunner.SCHEMA_YAML and the *-schema.yaml
# resources; SchemaListDriftTest fails the build when they diverge. This list silently omitted
# fiscal from the day that schema was added, so fiscal was never verified and never made it into
# the JAR-bundled seed — leaving it to rebuild every view on a user's first query.
ALL_SCHEMAS="sec geo econ econ_reference census crime weather ref fec fedregister officials cyber_vuln cyber_threat health energy edu patents lands cftc disasters housing ag transport environment research fiscal"

SOURCE=""
LIMIT="1"
SINGLE_CONNECTION=false
PROBES_ENABLED=true
DUP_ARGS=()
PUBLISH_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)         SOURCE="$2"; shift 2 ;;
        --limit)          LIMIT="$2"; shift 2 ;;
        --no-probes)      PROBES_ENABLED=false; shift ;;
        --no-dup)         DUP_ARGS+=(--no-dup); shift ;;
        # Mount every selected schema on ONE connection instead of looping one per schema.
        # Used by build-seed.sh so inter-schema views are actually created in the catalog.
        --single-connection) SINGLE_CONNECTION=true; shift ;;
        # Publish the Iceberg schema cache that this run materializes to the warehouse. Only
        # meaningful with --single-connection: the enumeration is what fills the cache, and a
        # per-schema loop would publish 24 partial caches over the top of each other.
        --publish-schema-cache) PUBLISH_ARGS+=(--publish-schema-cache); shift ;;
        --dup-threshold)  DUP_ARGS+=(--dup-threshold "$2"); shift 2 ;;
        -h|--help)        sed -n '17,48p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)                echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ ${#PUBLISH_ARGS[@]} -gt 0 && "$SINGLE_CONNECTION" != "true" ]]; then
    echo "Error: --publish-schema-cache requires --single-connection." >&2
    echo "       The per-schema loop would publish one partial cache per schema, each" >&2
    echo "       overwriting the last, leaving only the final schema's tables published." >&2
    exit 2
fi

# Schemas to verify (normalize commas to spaces).
if [[ -n "$SOURCE" ]]; then
    SELECTED="${SOURCE//,/ }"
else
    SELECTED="$ALL_SCHEMAS"
fi

# ---- load environment from .env.prod (REQUIRED; no ambient fallback) ----
ENV_FILE="${ENV_FILE:-$GOVDATA_HOME/.env.prod}"
if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: env file not found: $ENV_FILE" >&2
    echo "       Provide govdata/.env.prod or set ENV_FILE to its path." >&2
    exit 2
fi
echo "Loading environment from $ENV_FILE"
set -a; source "$ENV_FILE"; set +a
# Full-schema validation against the prod bucket: strip any inherited smoke-test flag so a
# leaked value can't silently retarget reads at the truncated sample bucket.
unset GOVDATA_DQ

# ---- test isolation: run against a DEDICATED operating-dir base, separate from the ETL/pool ----
# The persistent DuckDB read-catalog (.duckdb/govdata.duckdb) and the FileSchema conversion-record
# store (.aperio/<schema>/.conversions.json) are keyed to the operating-dir base (GOVDATA_DATA_DIR,
# else ~/.govdata) and shared by EVERY jdbc:govdata connection — including the ETL pool, which also
# loads GovDataDriver. Two problems follow: (1) probing would share the pool's live catalog, and
# (2) neither store is reconciled against the current schema YAMLs, so a table moved between schemas
# (e.g. EPA AQS weather->environment) leaves an orphaned record that gets re-advertised and then
# fails to read (stale Iceberg path) — a false ERROR reflecting a prior state, not the model under
# test. Point this run at its OWN base so it never touches the pool's catalog, then purge that base's
# caches so each verify starts from the current model. Both are regenerated on first connect. The
# httpfs data cache (.duckdb_httpfs_cache) under this base is kept between runs (remote parquet
# bytes, not table definitions). Override the base with GOVDATA_VERIFY_DATA_DIR.
export GOVDATA_DATA_DIR="${GOVDATA_VERIFY_DATA_DIR:-$HOME/.govdata-verify}"
echo "Isolated operating-dir base: $GOVDATA_DATA_DIR (separate from the ETL pool's ~/.govdata)"
echo "Purging stale verify-local metadata (.aperio conversion records + .duckdb catalog)"
mkdir -p "$GOVDATA_DATA_DIR"
rm -rf "$GOVDATA_DATA_DIR/.aperio" "$GOVDATA_DATA_DIR/.duckdb"

# ---- classpath: pin a single shaded jar (avoid wildcard picking up stale strays) ----
# MODEL_VERIFY_JAR overrides jar selection so a PRIVATE build (e.g. sih-govdata-fixes.jar) can be
# verified without touching the unversioned sih-govdata.jar a running ETL pool is executing.
JVM_OPTS="${JVM_OPTS:--Xmx2g -Xms512m}"
LIBS="$GOVDATA_HOME/build/libs"
if [[ -n "${MODEL_VERIFY_JAR:-}" ]]; then
    if [[ ! -f "$MODEL_VERIFY_JAR" ]]; then
        echo "Error: MODEL_VERIFY_JAR set but not found: $MODEL_VERIFY_JAR" >&2
        exit 2
    fi
    CLASSPATH="$MODEL_VERIFY_JAR"
elif [[ -f "$LIBS/sih-govdata.jar" ]]; then
    CLASSPATH="$LIBS/sih-govdata.jar"
elif ls "$LIBS"/sih-govdata-[0-9]*.jar >/dev/null 2>&1; then
    CLASSPATH="$(ls -t "$LIBS"/sih-govdata-[0-9]*.jar | head -1)"
else
    echo "Error: Cannot find govdata shaded jar in $LIBS." >&2
    echo "Build it: ./gradlew :govdata:shadowJar" >&2
    exit 2
fi
echo "Using jar:  $CLASSPATH"
echo "Endpoint:   ${AWS_ENDPOINT_OVERRIDE:-<default>}   dir: ${GOVDATA_PARQUET_DIR:-<driver default>}"
echo "Schemas:    $SELECTED"
echo ""

# Write the per-schema feature probe file (returns path via stdout, or empty if none).
write_probes() {
    local schema="$1" file
    $PROBES_ENABLED || return 0
    case "$schema" in
        geo|lands|census)
            file="$(mktemp)"
            echo "geo_st_distance|||SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4))" >> "$file"
            echo "geo_st_astext|||SELECT ST_AsText(ST_Point(-73.9857, 40.7484))" >> "$file"
            echo "$file" ;;
        sec)
            file="$(mktemp)"
            echo "sec_cosine_similarity|||SELECT COSINE_SIMILARITY(embedding, embedding) AS sim FROM sec.vectorized_chunks WHERE embedding IS NOT NULL LIMIT 1" >> "$file"
            echo "$file" ;;
        *) return 0 ;;
    esac
}

fail=0
passed=0
failed_list=""

# ---- single connection mounting EVERY selected schema (seed generation) ----
# GovDataDriver.createModelFile splits `source` on commas and emits one schema per source into a
# single model, so this mounts them all on one root. That is REQUIRED for seeding: an inter-schema
# view can only be defined when both schemas are visible, and the per-schema loop below never has
# more than one mounted — so those views report MISSING (correctly treated as expected there) and
# their DDL never reaches the shared catalog. Since every table exists from creation (Iceberg
# commits an initial empty snapshot so a table is queryable at zero rows), mounting everything up
# front makes every view definable in one pass.
#
# Verification keeps the per-schema loop: it is faster to probe, and a single connection eagerly
# creates ~130 views before the first probe. Same behaviour, opposite sign — a cost when verifying,
# the entire deliverable when seeding.
if [[ "$SINGLE_CONNECTION" == "true" ]]; then
    joined="$(echo $SELECTED | tr ' ' ',')"
    echo "===== single connection, all schemas mounted: $joined ====="
    args=(--source "$joined" --limit "$LIMIT" "${DUP_ARGS[@]}" "${PUBLISH_ARGS[@]}")
    if java $JVM_OPTS -cp "$CLASSPATH" "$RUNNER" "${args[@]}"; then
        echo "  all-schema connection OK"
        exit 0
    fi
    echo "  all-schema connection FAILED" >&2
    exit 1
fi

# ---- run schemas concurrently, one connection each ----
# Each schema is an independent JDBC connection over its own bucket prefix — nothing is shared,
# so serialising them only added wall-clock. A full sweep ran for hours at ~0.4s/table of actual
# probe work, because 26 schemas waited on each other. Fan out instead, capped so the JVMs fit:
# each is -Xmx2g, so the default 4 needs ~8GB. Output is buffered per schema and printed whole,
# otherwise interleaved probe lines from concurrent schemas would be unreadable.
JOBS="${VERIFY_JOBS:-4}"
_outdir=$(mktemp -d "${TMPDIR:-/tmp}/model-verify-XXXXXX")
trap 'rm -rf "$_outdir"' EXIT

run_one() {
    local s=$1 probe args rc
    probe="$(write_probes "$s")"
    args=(--source "$s" --limit "$LIMIT" "${DUP_ARGS[@]}")
    [[ -n "$probe" ]] && args+=(--probes "$probe")
    {
        echo "========================= $s ========================="
        java $JVM_OPTS -cp "$CLASSPATH" "$RUNNER" "${args[@]}" 2>/dev/null
        rc=$?
        echo ""
        echo "$rc" > "$_outdir/$s.rc"
    } > "$_outdir/$s.out" 2>&1
    [[ -n "$probe" ]] && rm -f "$probe"
    # Always succeed: the dispatch loop below uses `wait -n`'s status purely for flow control,
    # and a non-zero return there trips its `|| wait` fallback into waiting for EVERY in-flight
    # schema — collapsing rolling concurrency into barriered batches. Without this, the trailing
    # `[[ -n "$probe" ]]` short-circuits to false for every schema that has no probe file (all but
    # geo/lands/census/sec) and the function returns 1. Per-schema pass/fail is read from
    # "$_outdir/$s.rc", never from this status.
    return 0
}

_running=0
for s in $SELECTED; do
    run_one "$s" &
    _running=$((_running + 1))
    if [ "$_running" -ge "$JOBS" ]; then
        wait -n 2>/dev/null || wait
        _running=$((_running - 1))
    fi
done
wait

# Report in the order the user asked for, not the order they happened to finish.
for s in $SELECTED; do
    [ -f "$_outdir/$s.out" ] && cat "$_outdir/$s.out"
    if [ "$(cat "$_outdir/$s.rc" 2>/dev/null || echo 1)" = "0" ]; then
        passed=$((passed + 1))
    else
        fail=1
        failed_list="$failed_list $s"
    fi
done

echo "===================== OVERALL ====================="
echo "  schemas passed: $passed / $(echo $SELECTED | wc -w)"
[[ -n "$failed_list" ]] && echo "  schemas with issues:$failed_list"
exit $fail
