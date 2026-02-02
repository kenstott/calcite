#!/bin/bash
#
# ETL Runner - Standalone script for downloading historical government data
#
# Usage:
#   ./etl-runner.sh --model src/main/resources/models/historical/sec/sec-10k-2026-all.json
#   ./etl-runner.sh --dry-run --model sec-10k-2026-all.json
#   ./etl-runner.sh --verbose --model sec-10k-2026-all.json
#
# Environment variables:
#   JVM_OPTS      - JVM options (default: -Xmx4g -Xms1g)
#   GOVDATA_HOME  - Base directory (default: script directory's parent)
#   ENV_FILE      - Environment file to load (default: .env.prod)
#

set -e

# Determine script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"

# Load environment file if present (default to .env.prod for production)
ENV_FILE="${ENV_FILE:-.env.prod}"
if [[ -f "$GOVDATA_HOME/$ENV_FILE" ]]; then
    echo "Loading environment from $ENV_FILE"
    set -a
    source "$GOVDATA_HOME/$ENV_FILE"
    set +a
elif [[ -f "$ENV_FILE" ]]; then
    echo "Loading environment from $ENV_FILE"
    set -a
    source "$ENV_FILE"
    set +a
fi

# JVM options (4GB heap for 8GB+ devices, adjust as needed)
JVM_OPTS="${JVM_OPTS:--Xmx4g -Xms1g}"

# Additional JVM flags for better GC and crash diagnostics
JVM_FLAGS="-XX:+HeapDumpOnOutOfMemoryError \
           -XX:HeapDumpPath=$GOVDATA_HOME/runs/heapdump.hprof \
           -XX:ErrorFile=$GOVDATA_HOME/runs/hs_err_pid%p.log"

# Build classpath
if [[ -d "$GOVDATA_HOME/build/libs" ]]; then
    CLASSPATH="$GOVDATA_HOME/build/libs/*"
elif [[ -f "$GOVDATA_HOME/calcite-govdata-all.jar" ]]; then
    CLASSPATH="$GOVDATA_HOME/calcite-govdata-all.jar"
else
    echo "Error: Cannot find govdata JAR files."
    echo "Run './gradlew :govdata:shadowJar' first."
    exit 2
fi

# Create runs directory if needed
mkdir -p "$GOVDATA_HOME/runs"

# Run ETL
echo "Starting ETL Runner..."
echo "JVM_OPTS: $JVM_OPTS"
echo "CLASSPATH: $CLASSPATH"
echo ""

exec java $JVM_OPTS $JVM_FLAGS -cp "$CLASSPATH" \
    org.apache.calcite.adapter.govdata.etl.EtlRunner "$@"
