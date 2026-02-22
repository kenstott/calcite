#!/usr/bin/env bash
# ============================================================================
# Shared functions for parallel ETL workers
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="$(cd "$GOVDATA_ROOT/.." && pwd)"

# Load base credentials from govdata/.env.prod, then parallel overrides
load_env() {
  local env_prod="$GOVDATA_ROOT/.env.prod"
  if [ -f "$env_prod" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$env_prod"
    set +a
  else
    echo "WARNING: $env_prod not found — credentials may be missing" >&2
  fi

  local env_override="$SCRIPT_DIR/env.sh"
  if [ -f "$env_override" ]; then
    # shellcheck disable=SC1090
    source "$env_override"
  fi

  # Default tracker to s3 for parallel operation
  export CALCITE_TRACKER_BACKEND="${CALCITE_TRACKER_BACKEND:-s3}"
  export CALCITE_TRACKER_S3_BUCKET="${CALCITE_TRACKER_S3_BUCKET:-}"
}

# Resolve the govdata shadow JAR (fat JAR with all dependencies)
resolve_classpath() {
  local jar
  jar=$(find "$GOVDATA_ROOT/build/libs" -name "calcite-govdata-*-all.jar" 2>/dev/null | head -1)
  if [ -z "$jar" ]; then
    echo "ERROR: Shadow JAR not found. Run: ./gradlew :govdata:shadowJar" >&2
    exit 1
  fi
  echo "$jar"
}

# Generate a SEC filings model JSON for a year range
# Usage: generate_sec_model <start_year> <end_year> <output_file>
generate_sec_model() {
  local start_year=$1 end_year=$2 output_file=$3
  cat > "$output_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [{
    "name": "sec",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "sec",
      "ciks": "_ALL_EDGAR_FILERS",
      "filingTypes": ["10-K", "10-Q", "8-K"],
      "startYear": ${start_year},
      "endYear": ${end_year},
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "trackerBackend": "s3",
      "trackerConfig": {
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}"
      },
      "s3Config": {
        "accessKeyId": "\${AWS_ACCESS_KEY_ID}",
        "secretAccessKey": "\${AWS_SECRET_ACCESS_KEY}",
        "endpoint": "\${AWS_ENDPOINT_OVERRIDE}"
      }
    }
  }]
}
ENDJSON
}

# Generate a prices-only model JSON for a year range
# Usage: generate_prices_model <start_year> <end_year> <output_file>
generate_prices_model() {
  local start_year=$1 end_year=$2 output_file=$3
  cat > "$output_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [{
    "name": "sec",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "sec",
      "ciks": "_ALL_EDGAR_FILERS",
      "fetchStockPrices": true,
      "stockPriceSource": "stooq",
      "filingTypes": [],
      "startYear": ${start_year},
      "endYear": ${end_year},
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "trackerBackend": "s3",
      "trackerConfig": {
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}"
      },
      "s3Config": {
        "accessKeyId": "\${AWS_ACCESS_KEY_ID}",
        "secretAccessKey": "\${AWS_SECRET_ACCESS_KEY}",
        "endpoint": "\${AWS_ENDPOINT_OVERRIDE}"
      }
    }
  }]
}
ENDJSON
}

# Generate a non-SEC model JSON (econ, census, geo, crime, weather)
# Usage: generate_nonsec_model <output_file>
generate_nonsec_model() {
  local output_file=$1
  cat > "$output_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "econ",
  "schemas": [
    {
      "name": "econ",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "econ",
        "startYear": 2010,
        "endYear": 2026,
        "autoDownload": true,
        "directory": "${GOVDATA_PARQUET_DIR}",
        "cacheDirectory": "${GOVDATA_CACHE_DIR}",
        "trackerBackend": "s3",
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}" },
        "s3Config": {
          "accessKeyId": "\${AWS_ACCESS_KEY_ID}",
          "secretAccessKey": "\${AWS_SECRET_ACCESS_KEY}",
          "endpoint": "\${AWS_ENDPOINT_OVERRIDE}"
        }
      }
    },
    {
      "name": "census",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "census",
        "enabledSources": ["acs"],
        "startYear": 2010,
        "endYear": 2026,
        "autoDownload": true,
        "directory": "${GOVDATA_PARQUET_DIR}",
        "cacheDirectory": "${GOVDATA_CACHE_DIR}",
        "trackerBackend": "s3",
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}" },
        "s3Config": {
          "accessKeyId": "\${AWS_ACCESS_KEY_ID}",
          "secretAccessKey": "\${AWS_SECRET_ACCESS_KEY}",
          "endpoint": "\${AWS_ENDPOINT_OVERRIDE}"
        }
      }
    },
    {
      "name": "geo",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "geo",
        "enabledSources": ["tiger", "hud"],
        "tigerYear": 2024,
        "autoDownload": true,
        "directory": "${GOVDATA_PARQUET_DIR}",
        "cacheDirectory": "${GOVDATA_CACHE_DIR}",
        "trackerBackend": "s3",
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}" },
        "s3Config": {
          "accessKeyId": "\${AWS_ACCESS_KEY_ID}",
          "secretAccessKey": "\${AWS_SECRET_ACCESS_KEY}",
          "endpoint": "\${AWS_ENDPOINT_OVERRIDE}"
        }
      }
    },
    {
      "name": "crime",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "crime",
        "startYear": 2010,
        "endYear": 2026,
        "autoDownload": true,
        "directory": "${GOVDATA_PARQUET_DIR}",
        "cacheDirectory": "${GOVDATA_CACHE_DIR}",
        "trackerBackend": "s3",
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}" },
        "s3Config": {
          "accessKeyId": "\${AWS_ACCESS_KEY_ID}",
          "secretAccessKey": "\${AWS_SECRET_ACCESS_KEY}",
          "endpoint": "\${AWS_ENDPOINT_OVERRIDE}"
        }
      }
    },
    {
      "name": "weather",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
      "operand": {
        "dataSource": "weather",
        "startYear": 2010,
        "endYear": 2026,
        "autoDownload": true,
        "directory": "${GOVDATA_PARQUET_DIR}",
        "cacheDirectory": "${GOVDATA_CACHE_DIR}",
        "trackerBackend": "s3",
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}" },
        "s3Config": {
          "accessKeyId": "\${AWS_ACCESS_KEY_ID}",
          "secretAccessKey": "\${AWS_SECRET_ACCESS_KEY}",
          "endpoint": "\${AWS_ENDPOINT_OVERRIDE}"
        }
      }
    }
  ]
}
ENDJSON
}

# Run the ETL with a given model file
# Usage: run_etl <model_file> <worker_id> [extra_args...]
run_etl() {
  local model_file=$1 worker_id=$2
  shift 2

  local jar
  jar=$(resolve_classpath)

  local log_dir="$SCRIPT_DIR/runs/$worker_id"
  mkdir -p "$log_dir"

  local timestamp
  timestamp=$(date +%Y%m%d_%H%M%S)
  local log_file="$log_dir/etl_${timestamp}.log"

  echo "[$worker_id] Starting ETL with model: $model_file"
  echo "[$worker_id] Log: $log_file"

  # Ensure Ctrl-C kills the java process, not just tee
  trap 'kill 0' INT TERM

  java \
    -Xms"${ETL_HEAP_MIN:-1g}" \
    -Xmx"${ETL_HEAP_MAX:-4g}" \
    -cp "$jar" \
    org.apache.calcite.adapter.govdata.etl.EtlRunner \
    --model "$model_file" \
    --verbose \
    "$@" \
    2>&1 | tee "$log_file"

  local exit_code=${PIPESTATUS[0]}
  trap - INT TERM
  echo "[$worker_id] ETL finished with exit code: $exit_code"
  return $exit_code
}

log_info() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}
