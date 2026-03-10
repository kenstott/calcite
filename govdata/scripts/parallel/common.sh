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
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
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
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
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
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}", "endpoint": "${AWS_ENDPOINT_OVERRIDE}" },
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
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}", "endpoint": "${AWS_ENDPOINT_OVERRIDE}" },
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
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}", "endpoint": "${AWS_ENDPOINT_OVERRIDE}" },
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
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}", "endpoint": "${AWS_ENDPOINT_OVERRIDE}" },
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
        "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}", "endpoint": "${AWS_ENDPOINT_OVERRIDE}" },
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

# Generate a SEC primary model JSON (10-K/10-Q filings only) for a year range
# Usage: generate_sec_primary_model <start_year> <end_year> <output_file>
generate_sec_primary_model() {
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
      "filingTypes": ["10-K", "10-K/A", "10-Q", "10-Q/A"],
      "fetchStockPrices": false,
      "startYear": ${start_year},
      "endYear": ${end_year},
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "trackerBackend": "s3",
      "trackerConfig": {
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
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

# Generate a SEC secondary model JSON (8-K, proxy, insider, 13F, 13D/G) for a year range
# Usage: generate_sec_secondary_model <start_year> <end_year> <output_file>
generate_sec_secondary_model() {
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
      "filingTypes": ["8-K", "8-K/A", "DEF 14A", "3", "4", "5", "13F-HR", "13F-HR/A", "SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"],
      "startYear": ${start_year},
      "endYear": ${end_year},
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "trackerBackend": "s3",
      "trackerConfig": {
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
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

# Generate a single-schema model JSON for one non-SEC data source
# Usage: generate_single_schema_model <schema_name> <output_file>
generate_single_schema_model() {
  local schema_name=$1 output_file=$2
  local operand_body

  case "$schema_name" in
    econ)
      operand_body="\"dataSource\": \"econ\",
      \"startYear\": 2010,
      \"endYear\": 2026"
      ;;
    census)
      operand_body="\"dataSource\": \"census\",
      \"enabledSources\": [\"acs\"],
      \"startYear\": 2010,
      \"endYear\": 2026"
      ;;
    geo)
      operand_body="\"dataSource\": \"geo\",
      \"enabledSources\": [\"tiger\", \"hud\"],
      \"tigerYear\": 2024"
      ;;
    crime)
      operand_body="\"dataSource\": \"crime\",
      \"startYear\": 2010,
      \"endYear\": 2026"
      ;;
    weather)
      operand_body="\"dataSource\": \"weather\",
      \"startYear\": 2010,
      \"endYear\": 2026"
      ;;
    fec)
      operand_body="\"dataSource\": \"fec\""
      ;;
    *)
      echo "ERROR: unknown schema '$schema_name'" >&2
      return 1
      ;;
  esac

  cat > "$output_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "${schema_name}",
  "schemas": [{
    "name": "${schema_name}",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      ${operand_body},
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "trackerBackend": "s3",
      "trackerConfig": { "bucket": "${CALCITE_TRACKER_S3_BUCKET}", "endpoint": "${AWS_ENDPOINT_OVERRIDE}" },
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

# Discover the latest GLEIF golden copy CSV download URL.
# GLEIF publishes daily snapshots with date-embedded URLs. This function
# queries the golden copy API to resolve the current URL.
# Sets: GLEIF_CSV_URL (exported)
discover_gleif_url() {
  if [ -n "${GLEIF_CSV_URL:-}" ]; then
    return
  fi
  log_info "Discovering latest GLEIF golden copy CSV URL..."
  GLEIF_CSV_URL=$(curl -sf "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['lei2']['full_file']['csv']['url'])")
  export GLEIF_CSV_URL
  log_info "GLEIF CSV URL: $GLEIF_CSV_URL"
}

# Generate a REF (reference identifiers) model JSON
# Usage: generate_ref_model <output_file>
generate_ref_model() {
  local output_file=$1
  cat > "$output_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "ref",
  "schemas": [{
    "name": "ref",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "ref",
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "trackerBackend": "s3",
      "trackerConfig": {
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
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

# Generate a FEC (campaign finance) model JSON
# Usage: generate_fec_model <output_file>
generate_fec_model() {
  local output_file=$1
  cat > "$output_file" <<ENDJSON
{
  "version": "1.0",
  "defaultSchema": "fec",
  "schemas": [{
    "name": "fec",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "fec",
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}",
      "trackerBackend": "s3",
      "trackerConfig": {
        "bucket": "${CALCITE_TRACKER_S3_BUCKET}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
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

# Determine heap sizes for a worker based on its type.
# Crime worker (worker-21) needs more memory for large dimension expansion;
# SEC workers (worker-23+) are lighter and can run with less.
# Usage: get_heap_config <worker_id>
# Sets: _HEAP_MIN, _HEAP_MAX
get_heap_config() {
  local worker_id=$1

  # Allow env overrides to take precedence
  if [ -n "${ETL_HEAP_MIN:-}" ] && [ -n "${ETL_HEAP_MAX:-}" ]; then
    _HEAP_MIN="$ETL_HEAP_MIN"
    _HEAP_MAX="$ETL_HEAP_MAX"
    return
  fi

  # Extract worker number (e.g., "worker-21" -> 21)
  local num
  num=$(echo "$worker_id" | grep -oP '\d+' | head -1)

  if [ "$num" -eq 20 ]; then
    # GEO (20): TIGER shapefiles (census_tracts, ZCTAs) need large heap for geometry parsing
    _HEAP_MIN="4g"
    _HEAP_MAX="6g"
  elif [ "$num" -eq 41 ]; then
    # REF (41): GLEIF golden copy is ~450MB ZIP / 3.2M CSV rows, needs more heap
    _HEAP_MIN="3g"
    _HEAP_MAX="4g"
  elif [ "$num" -eq 60 ]; then
    # FEC (60): Individual contributions has 3M+ rows/year, streaming to Iceberg
    _HEAP_MIN="4g"
    _HEAP_MAX="5g"
  elif [ "$num" -ge 23 ] && [ "$num" -le 58 ]; then
    # SEC Secondary (23-39), prices (40), compact (42-58): tracker preload needs ~2.5GB
    _HEAP_MIN="2g"
    _HEAP_MAX="3g"
  else
    # SEC Primary (1-17: 10-K/10-Q), non-SEC data (18-22), crime (21): need full heap
    _HEAP_MIN="2g"
    _HEAP_MAX="3g"
  fi
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

  # Determine per-worker heap sizes
  local _HEAP_MIN _HEAP_MAX
  get_heap_config "$worker_id"

  # Build extra flags
  local extra_flags=""
  echo "[$worker_id] Starting ETL with model: $model_file (heap: ${_HEAP_MIN}/${_HEAP_MAX})"
  if [ -n "${ETL_PARALLEL_THREADS:-}" ] && [ "${ETL_PARALLEL_THREADS:-0}" -gt 1 ]; then
    extra_flags="$extra_flags --threads $ETL_PARALLEL_THREADS"
    echo "[$worker_id] Parallel entity threads: $ETL_PARALLEL_THREADS"
  fi
  echo "[$worker_id] Log: $log_file"

  # Ensure Ctrl-C kills the java process, not just tee
  trap 'kill 0' INT TERM

  java \
    -Xms"${_HEAP_MIN}" \
    -Xmx"${_HEAP_MAX}" \
    -cp "$jar" \
    org.apache.calcite.adapter.govdata.etl.EtlRunner \
    --model "$model_file" \
    --verbose \
    $extra_flags \
    "$@" \
    2>&1 | stdbuf -oL tee "$log_file"

  local exit_code=${PIPESTATUS[0]}
  trap - INT TERM
  echo "[$worker_id] ETL finished with exit code: $exit_code"
  return $exit_code
}

log_info() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}
