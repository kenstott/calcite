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
ALL_SCHEMAS="sec geo econ econ_reference census crime weather ref fec fedregister cyber_vuln cyber_threat health energy edu patents lands cftc disasters housing ag transport environment research"

SOURCE=""
LIMIT="1"
PROBES_ENABLED=true
DUP_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)         SOURCE="$2"; shift 2 ;;
        --limit)          LIMIT="$2"; shift 2 ;;
        --no-probes)      PROBES_ENABLED=false; shift ;;
        --no-dup)         DUP_ARGS+=(--no-dup); shift ;;
        --dup-threshold)  DUP_ARGS+=(--dup-threshold "$2"); shift 2 ;;
        -h|--help)        sed -n '17,48p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)                echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

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

# ---- loop: one fast connection per schema ----
fail=0
passed=0
failed_list=""
for s in $SELECTED; do
    echo "========================= $s ========================="
    probe="$(write_probes "$s")"
    args=(--source "$s" --limit "$LIMIT" "${DUP_ARGS[@]}")
    [[ -n "$probe" ]] && args+=(--probes "$probe")
    if java $JVM_OPTS -cp "$CLASSPATH" "$RUNNER" "${args[@]}" 2>/dev/null; then
        passed=$((passed + 1))
    else
        fail=1
        failed_list="$failed_list $s"
    fi
    [[ -n "$probe" ]] && rm -f "$probe"
    echo ""
done

echo "===================== OVERALL ====================="
echo "  schemas passed: $passed / $(echo $SELECTED | wc -w)"
[[ -n "$failed_list" ]] && echo "  schemas with issues:$failed_list"
exit $fail
