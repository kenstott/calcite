#!/bin/bash
#
# ETL Verifier - Validates ETL results by checking table accessibility and data quality
#
# Usage:
#   ./etl-verify.sh --model path/to/model.json
#   ./etl-verify.sh --model model.json --schema SEC --verbose
#
# Environment variables:
#   JVM_OPTS      - JVM options (default: -Xmx2g -Xms512m)
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

# JVM options (smaller heap than ETL runner - verification is lighter)
JVM_OPTS="${JVM_OPTS:--Xmx2g -Xms512m}"

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

# Run verifier
echo "Starting ETL Verifier..."
echo ""

exec java $JVM_OPTS -cp "$CLASSPATH" \
    org.apache.calcite.adapter.govdata.etl.EtlVerifier "$@"
