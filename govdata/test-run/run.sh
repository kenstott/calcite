#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="$SCRIPT_DIR/../build/libs/calcite-govdata-1.42.0-SNAPSHOT-all.jar"
MODEL="$SCRIPT_DIR/model.json"
LOG="$SCRIPT_DIR/logs/etl_$(date +%Y%m%d_%H%M%S).log"

if [ ! -f "$JAR" ]; then
  echo "ERROR: Shadow JAR not found at $JAR" >&2
  exit 1
fi

# Load credentials (.env.prod for EDGAR API key etc.)
ENV_FILE="$SCRIPT_DIR/../.env.prod"
if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

echo "Starting local 8-CIK test run — log: $LOG"

export ETL_LOCAL_RAW_CACHE="$SCRIPT_DIR/cache"

java \
  -Xms2g -Xmx3g \
  -cp "$JAR" \
  org.apache.calcite.adapter.govdata.etl.EtlRunner \
  --model "$MODEL" \
  --verbose \
  2>&1 | tee "$LOG"

EXIT=${PIPESTATUS[0]}
if [ "$EXIT" -eq 0 ]; then
  echo "ETL completed successfully"
else
  echo "ETL FAILED (exit $EXIT) — check $LOG"
fi
exit $EXIT
