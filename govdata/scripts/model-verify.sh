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
#
# Exit code: 0 if every schema passed; 1 if any schema reported ERROR/probe failure.
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"
RUNNER="org.apache.calcite.adapter.govdata.etl.GovDataModelVerificationRunner"

# Canonical primary dataSource names (GovDataSchemaFactory.getFactoryForDataSource switch).
ALL_SCHEMAS="sec geo econ econ_reference census crime weather ref fec fedregister cyber_vuln cyber_threat health energy edu patents lands cftc disasters housing ag transport environment"

SOURCE=""
LIMIT="1"
PROBES_ENABLED=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)     SOURCE="$2"; shift 2 ;;
        --limit)      LIMIT="$2"; shift 2 ;;
        --no-probes)  PROBES_ENABLED=false; shift ;;
        -h|--help)    sed -n '17,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)            echo "Unknown argument: $1" >&2; exit 2 ;;
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

# ---- classpath: pin a single shaded jar (avoid wildcard picking up stale strays) ----
JVM_OPTS="${JVM_OPTS:--Xmx2g -Xms512m}"
LIBS="$GOVDATA_HOME/build/libs"
if [[ -f "$LIBS/sih-govdata.jar" ]]; then
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
    args=(--source "$s" --limit "$LIMIT")
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
