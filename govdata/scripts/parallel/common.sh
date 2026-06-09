#!/usr/bin/env bash
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
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_ROOT="$(cd "$GOVDATA_ROOT/.." && pwd)"

# Load base credentials from govdata/.env.prod, then parallel overrides.
# Caller-exported variables take precedence: if GOVDATA_START_YEAR or
# GOVDATA_CACHE_DIR are already set in the calling environment, .env.prod
# will NOT override them.  This allows per-run overrides like:
#   GOVDATA_START_YEAR=2025 GOVDATA_CACHE_DIR=/tmp/cache ./run-pool.sh ...
load_env() {
  local env_prod="$GOVDATA_ROOT/.env.prod"
  if [ -f "$env_prod" ]; then
    # Capture any caller-exported overrides before .env.prod stomps them.
    # GOVDATA_PARQUET_DIR is preserved so a DQ orchestrator can point the standard
    # daily/historical workers at the DQ bucket (s3://govdata-parquet-v1-dq) instead of prod.
    local _pre_start_year="${GOVDATA_START_YEAR+set}"
    local _pre_cache_dir="${GOVDATA_CACHE_DIR+set}"
    local _pre_parquet_dir="${GOVDATA_PARQUET_DIR+set}"
    local _pre_tracker_bucket="${CALCITE_TRACKER_S3_BUCKET+set}"
    local _saved_start_year="${GOVDATA_START_YEAR:-}"
    local _saved_cache_dir="${GOVDATA_CACHE_DIR:-}"
    local _saved_parquet_dir="${GOVDATA_PARQUET_DIR:-}"
    local _saved_tracker_bucket="${CALCITE_TRACKER_S3_BUCKET:-}"

    set -a
    # shellcheck disable=SC1090
    source "$env_prod"
    set +a

    # Restore caller-provided overrides. CALCITE_TRACKER_S3_BUCKET must be preserved alongside
    # GOVDATA_PARQUET_DIR: a DQ orchestrator points both at the -dq buckets, and if the tracker
    # bucket falls back to prod the tracker reads stale prod completions and skips every table
    # ("table complete") while writing to a bucket that doesn't exist on the DQ endpoint.
    [ "${_pre_start_year}" = "set" ] && export GOVDATA_START_YEAR="$_saved_start_year"
    [ "${_pre_cache_dir}" = "set" ] && export GOVDATA_CACHE_DIR="$_saved_cache_dir"
    [ "${_pre_parquet_dir}" = "set" ] && export GOVDATA_PARQUET_DIR="$_saved_parquet_dir"
    [ "${_pre_tracker_bucket}" = "set" ] && export CALCITE_TRACKER_S3_BUCKET="$_saved_tracker_bucket"
  else
    echo "WARNING: $env_prod not found — credentials may be missing" >&2
  fi

  # Source DQ overrides if present (e.g., GOVDATA_JAR from run-all-dq.sh)
  local env_dq="$GOVDATA_ROOT/.env.dq"
  if [ -f "$env_dq" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$env_dq"
    set +a
  fi

  local env_override="$SCRIPT_DIR/env.sh"
  if [ -f "$env_override" ]; then
    # shellcheck disable=SC1090
    source "$env_override"
  fi

  # In daily mode, use GOVDATA_INCREMENTAL_START_YEAR as the effective start boundary.
  # yearRange returns empty list when start > end, so tables with no data in the
  # incremental window are skipped cleanly rather than processing all historical years.
  if [ "${GOVDATA_RUN_MODE:-}" = "daily" ]; then
    export GOVDATA_START_YEAR="${GOVDATA_INCREMENTAL_START_YEAR:-$(date +%Y)}"
  fi

  # Default tracker to s3 for parallel operation
  export CALCITE_TRACKER_BACKEND="${CALCITE_TRACKER_BACKEND:-s3}"
  export CALCITE_TRACKER_S3_BUCKET="${CALCITE_TRACKER_S3_BUCKET:-}"
}

# Resolve the govdata shadow JAR (fat JAR with all dependencies).
# Override the default search path by setting GOVDATA_JAR to an explicit path,
# e.g. when building from a worktree in parallel with production:
#   export GOVDATA_JAR=/path/to/worktree/govdata/build/libs/calcite-govdata-*-all.jar
resolve_classpath() {
  local jar
  if [ -n "${GOVDATA_JAR:-}" ]; then
    # Expand glob in case the caller used a wildcard
    jar=$(echo $GOVDATA_JAR | head -1)
    if [ ! -f "$jar" ]; then
      echo "ERROR: GOVDATA_JAR set but file not found: $jar" >&2
      exit 1
    fi
    echo "$jar"
    return
  fi
  jar=$(find "$GOVDATA_ROOT/build/libs" -name "sih-govdata.jar" 2>/dev/null | head -1)
  if [ -z "$jar" ]; then
    jar=$(find "$GOVDATA_ROOT/build/libs" -name "calcite-govdata-*-all.jar" 2>/dev/null | head -1)
  fi
  if [ -z "$jar" ]; then
    jar=$(find "$GOVDATA_ROOT/build/libs" -name "sih-govdata-*-SNAPSHOT.jar" 2>/dev/null | head -1)
  fi
  if [ -z "$jar" ]; then
    echo "ERROR: Shadow JAR not found. Run: ./gradlew :govdata:shadowJar" >&2
    echo "       Or set GOVDATA_JAR=/path/to/sih-govdata.jar to use a downloaded release jar." >&2
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
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
        "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
        "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
        "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
        "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
        "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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

# Generate a SEC reprocess model JSON targeting specific accessions.
# Sets forceAccessions so filterAndSelfHeal bypasses tracker state for listed accessions.
# Uses _ALL_EDGAR_FILERS so the full EDGAR index is loaded regardless of who filed.
# Usage: generate_sec_reprocess_model <accessions_space_separated> <start_year> <end_year> <output_file>
generate_sec_reprocess_model() {
  local accessions_str=$1 start_year=$2 end_year=$3 output_file=$4

  local acc_json
  acc_json=$(printf '%s' "$accessions_str" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read().split()))")

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
      "filingTypes": ["10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A", "DEF 14A", "3", "4", "5", "13F-HR", "13F-HR/A", "SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"],
      "fetchStockPrices": false,
      "forceAccessions": ${acc_json},
      "startYear": ${start_year},
      "endYear": ${end_year},
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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

# Generate a SEC chunks-backfill model JSON for a year range.
# Identical to the primary model but sets chunksBackfill=true so filterAndSelfHeal
# routes base-complete (no chunks) accessions to reprocessing instead of skipping them.
# Usage: generate_sec_chunks_backfill_model <start_year> <end_year> <output_file>
generate_sec_chunks_backfill_model() {
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
      "chunksBackfill": true,
      "startYear": ${start_year},
      "endYear": ${end_year},
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
      "fetchStockPrices": false,
      "startYear": ${start_year},
      "endYear": ${end_year},
      "autoDownload": true,
      "directory": "${GOVDATA_PARQUET_DIR}",
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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

# Generate a single-schema model JSON for one non-SEC data source.
# Respects GOVDATA_RUN_MODE (historical | daily), GOVDATA_START_YEAR, and
# GOVDATA_INCREMENTAL_START_YEAR to set the correct year bounds.
# Usage: generate_single_schema_model <schema_name> <output_file>
generate_single_schema_model() {
  local schema_name=$1 output_file=$2
  local operand_body

  local _INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-$(date +%Y)}
  local _START_YEAR=${GOVDATA_START_YEAR:-2010}

  # Current calendar context — emitted in BOTH modes so transformers always have a
  # quarter token for quarterly cache busting (e.g. patents full-dump filenames).
  local _CURRENT_MONTH _CURRENT_YEAR _CURRENT_QUARTER
  _CURRENT_MONTH=$(date +%m)
  _CURRENT_YEAR=$(date +%Y)
  _CURRENT_QUARTER=$(( ($(date +%-m) - 1) / 3 + 1 ))

  # historical mode: cap at INCREMENTAL_YEAR-1; daily mode: start from INCREMENTAL_YEAR
  local _YEAR_RANGE
  if [ "${GOVDATA_RUN_MODE:-daily}" = "historical" ]; then
    _YEAR_RANGE="\"startYear\": ${_START_YEAR},
      \"endYear\": $((_INCREMENTAL_YEAR - 1)),
      \"currentMonth\": \"${_CURRENT_MONTH}\",
      \"currentYear\": \"${_CURRENT_YEAR}\",
      \"currentQuarter\": \"${_CURRENT_QUARTER}\""
  else
    _YEAR_RANGE="\"startYear\": ${_INCREMENTAL_YEAR},
      \"currentMonth\": \"${_CURRENT_MONTH}\",
      \"currentYear\": \"${_CURRENT_YEAR}\",
      \"currentQuarter\": \"${_CURRENT_QUARTER}\""
  fi

  case "$schema_name" in
    econ)
      operand_body="\"dataSource\": \"econ\",
      ${_YEAR_RANGE}"
      ;;
    census)
      operand_body="\"dataSource\": \"census\",
      \"enabledSources\": [\"acs\"],
      ${_YEAR_RANGE}"
      ;;
    geo)
      operand_body="\"dataSource\": \"geo\",
      \"enabledSources\": [\"tiger\", \"hud\"],
      ${_YEAR_RANGE}"
      ;;
    crime)
      operand_body="\"dataSource\": \"crime\",
      ${_YEAR_RANGE}"
      ;;
    weather)
      operand_body="\"dataSource\": \"weather\",
      ${_YEAR_RANGE}"
      ;;
    fec)
      operand_body="\"dataSource\": \"fec\",
      ${_YEAR_RANGE}"
      ;;
    fedregister)
      operand_body="\"dataSource\": \"fedregister\",
      ${_YEAR_RANGE}"
      ;;
    lands|public_lands)
      operand_body="\"dataSource\": \"lands\",
      ${_YEAR_RANGE}"
      ;;
    edu)
      local _EDU_TABLES='"ccd_districts","ccd_schools","naep_scores","naep_achievement_levels","crdc_schools","ipeds_institutions","ipeds_completions","ipeds_financials","ipeds_tuition"'
      if [ -n "${API_DATA_GOV:-}" ]; then
        _EDU_TABLES="${_EDU_TABLES},\"college_scorecard\",\"college_scorecard_programs\""
      fi
      operand_body="\"dataSource\": \"edu\",
      \"enabledTables\": [${_EDU_TABLES}],
      ${_YEAR_RANGE}"
      ;;
    econ_reference)
      operand_body="\"dataSource\": \"econ_reference\""
      ;;
    ref)
      generate_ref_model "$output_file"
      return
      ;;
    health)
      operand_body="\"dataSource\": \"health\",
      ${_YEAR_RANGE}"
      ;;
    patents)
      operand_body="\"dataSource\": \"patents\",
      ${_YEAR_RANGE}"
      ;;
    energy)
      operand_body="\"dataSource\": \"energy\",
      ${_YEAR_RANGE}"
      ;;
    cyber_vuln)
      operand_body="\"dataSource\": \"cyber_vuln\""
      ;;
    cyber_threat)
      operand_body="\"dataSource\": \"cyber_threat\""
      ;;
    cftc)
      operand_body="\"dataSource\": \"cftc\",
      ${_YEAR_RANGE}"
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
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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
# Usage: generate_fec_model <output_file> [start_year] [end_year] [force_tables_csv]
#   start_year       Filters fec_election_cycles list to years >= start_year (via GOVDATA_START_YEAR).
#                    Daily runs pass the current cycle year (e.g. 2026); historical omits or passes 2010.
#   force_tables_csv Comma-separated table names to force re-ingest (forceReprocessTables operand).
generate_fec_model() {
  local output_file=$1
  local start_year=${2:-}
  local end_year=${3:-}
  local force_tables_csv=${4:-}

  local start_year_json=""
  [ -n "$start_year" ] && start_year_json="
      \"startYear\": ${start_year},"

  local end_year_json=""
  [ -n "$end_year" ] && end_year_json="
      \"endYear\": ${end_year},"

  local force_tables_json=""
  if [ -n "$force_tables_csv" ]; then
    local tables_array
    tables_array=$(echo "$force_tables_csv" | sed 's/,/","/g')
    force_tables_json="
      \"forceReprocessTables\": [\"${tables_array}\"],"
  fi

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
      "directory": "${GOVDATA_PARQUET_DIR}",${start_year_json}${end_year_json}${force_tables_json}
      "cacheDirectory": "${GOVDATA_CACHE_DIR}/${schema_name}",
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

# ── Release-window helpers ────────────────────────────────────────────────────
#
# These functions let recurring workers fast-exit when their source is known to
# have no new data today. A skipped sub-run emits one log line and returns
# immediately — no network I/O, no model file written, no pool slot held.
#
# Set FORCE=true in the calling script (or pass --force) to bypass all checks.
#
# Usage:
#   within_release_window <label> <months> [<year_parity>]
#     months       Comma-separated month numbers (1=Jan … 12=Dec)
#     year_parity  Optional: "odd" or "even" — further constrains to odd/even calendar years
#
#   within_release_dow <label> <days>
#     days         Comma-separated day-of-week numbers (0=Sun, 1=Mon … 6=Sat)
#
# Both return 0 (proceed) or 1 (skip with a log message).

within_release_window() {
  local label=$1 months=$2 year_parity="${3:-any}"

  if [ "${FORCE:-false}" = true ]; then
    log_info "$(basename "$0"): --force — bypassing release window for $label"
    return 0
  fi

  local current_month current_year
  current_month=$(date +%-m)
  current_year=$(date +%Y)

  if [ "$year_parity" = "odd" ] && [ $(( current_year % 2 )) -eq 0 ]; then
    log_info "$(basename "$0"): skipping $label — odd-year source, current year $current_year is even"
    return 1
  fi
  if [ "$year_parity" = "even" ] && [ $(( current_year % 2 )) -ne 0 ]; then
    log_info "$(basename "$0"): skipping $label — even-year source, current year $current_year is odd"
    return 1
  fi

  IFS=',' read -ra month_list <<< "$months"
  for m in "${month_list[@]}"; do
    if [ "$current_month" -eq "$m" ]; then
      return 0
    fi
  done

  log_info "$(basename "$0"): skipping $label — outside release window (months: $months, today: month $current_month)"
  return 1
}

within_release_dow() {
  local label=$1 days=$2

  if [ "${FORCE:-false}" = true ]; then
    log_info "$(basename "$0"): --force — bypassing day-of-week check for $label"
    return 0
  fi

  local current_dow
  current_dow=$(date +%w)   # 0=Sunday … 6=Saturday

  IFS=',' read -ra dow_list <<< "$days"
  for d in "${dow_list[@]}"; do
    if [ "$current_dow" -eq "$d" ]; then
      return 0
    fi
  done

  log_info "$(basename "$0"): skipping $label — outside run day (days: $days, today: DOW $current_dow)"
  return 1
}

# Schema-YAML-driven window check. Reads releaseWindow: from the named table in
# the schema YAML and delegates to check-release-window.py.
# Falls back to proceed (0) if Python or the checker script is unavailable.
# Usage: table_in_window <schema_yaml_path> <table_name>
table_in_window() {
  local schema_yaml=$1 table_name=$2
  local checker="$SCRIPT_DIR/check-release-window.py"

  if [ ! -f "$checker" ]; then
    log_info "WARNING: check-release-window.py not found — proceeding for $table_name"
    return 0
  fi

  if python3 "$checker" "$schema_yaml" "$table_name" ${FORCE:+--force}; then
    return 0
  else
    return 1
  fi
}

# Determine heap sizes for a worker based on its schema/mode.
# Worker IDs follow the pattern worker-SCHEMA-MODE (e.g., worker-fec-daily).
# Usage: get_heap_config <worker_id>
# Sets: _HEAP_MIN, _HEAP_MAX
get_heap_config() {
  local worker_id=$1

  # DQ compound modes: handle before schema extraction to avoid "-dq" corrupting _schema
  if [[ "$worker_id" == *-dq-rebuild ]]; then
    local _base="${worker_id%-dq-rebuild}"
    _base="${_base#worker-}"
    get_heap_config "worker-${_base}-initial"
    return
  fi
  if [[ "$worker_id" == *-dq ]]; then
    # DuckDB-only DQ: no JVM; use 2g as memory-budget placeholder
    _HEAP_MIN="1g"; _HEAP_MAX="2g"
    return
  fi
  if [[ "$worker_id" == worker-dq-*-* ]]; then
    # DQ rebuild workers: worker-dq-<schema>-<mode> → use schema's initial heap
    local _dq_rest="${worker_id#worker-dq-}"  # e.g. "edu-historical"
    local _dq_schema="${_dq_rest%-*}"          # e.g. "edu"
    get_heap_config "worker-${_dq_schema}-initial"
    return
  fi

  # Allow env overrides to take precedence
  if [ -n "${ETL_HEAP_MIN:-}" ] && [ -n "${ETL_HEAP_MAX:-}" ]; then
    _HEAP_MIN="$ETL_HEAP_MIN"
    _HEAP_MAX="$ETL_HEAP_MAX"
    return
  fi

  # Extract schema from worker-SCHEMA-MODE pattern.
  # Strip "worker-" prefix, then peel off the last "-mode" suffix.
  local _id="${worker_id#worker-}"      # e.g. "fec-daily" or "cyber_threat-initial"
  local _schema="${_id%-*}"             # everything before the last dash
  local _mode="${_id##*-}"              # everything after the last dash

  case "$_schema" in
    geo)
      # TIGER shapefiles (census_tracts, ZCTAs) need large heap for geometry parsing
      _HEAP_MIN="4g"; _HEAP_MAX="6g" ;;
    ref)
      # GLEIF golden copy is ~450MB ZIP / 3.2M CSV rows
      _HEAP_MIN="3g"; _HEAP_MAX="4g" ;;
    fec)
      # Individual contributions: 3M+ rows/year, streaming to Iceberg
      _HEAP_MIN="4g"; _HEAP_MAX="5g" ;;
    cyber_threat|cyber_vuln)
      case "$_mode" in
        initial) _HEAP_MIN="4g"; _HEAP_MAX="6g" ;;  # full NVD ~350k CVEs + OTX backfill
        *)       _HEAP_MIN="2g"; _HEAP_MAX="3g" ;;  # delta/incremental
      esac ;;
    health)
      case "$_mode" in
        initial) _HEAP_MIN="4g"; _HEAP_MAX="6g" ;;  # all 15 tables, cursor pagination
        *)       _HEAP_MIN="2g"; _HEAP_MAX="3g" ;;
      esac ;;
    edu)
      case "$_mode" in
        initial) _HEAP_MIN="4g"; _HEAP_MAX="6g" ;;  # full K-12 + IPEDS + Scorecard
        *)       _HEAP_MIN="2g"; _HEAP_MAX="3g" ;;
      esac ;;
    crime)
      # Large dimension expansion (type × year × state × offense × ori)
      _HEAP_MIN="3g"; _HEAP_MAX="4g" ;;
    *)
      _HEAP_MIN="2g"; _HEAP_MAX="3g" ;;
  esac
}

# Return the per-schema default GOVDATA_START_YEAR for dq-rebuild runs.
# Encodes the minimum lookback needed to cover at least one full data cycle.
# Used when GOVDATA_START_YEAR is not set in the environment.
# Usage: get_dq_start_year <schema>  → prints year to stdout
get_dq_start_year() {
  local schema=$1
  case "$schema" in
    energy)   echo $(( $(date +%Y) - 4 )) ;;  # dataLag=2 tables map year=effective+dataLag, so a current-2 start emits only the current year. Need current-2-dataLag = current-4 to land historical generation years (T8 wants MIN<=current-2).
    edu)      echo $(( $(date +%Y) - 3 )) ;;  # 3-year lookback: covers NAEP/CRDC biennial lag; crdc_schools 2021 data DQ-flags correctly via existence check
    lands)    echo $(( $(date +%Y) - 3 )) ;;  # forest inventory biennial: ≥2-yr span to guarantee one published cycle
    census)   echo $(( $(date +%Y) - 3 )) ;;  # ACS 5-year: dataLag=2 + releaseMonth=12 → effective end current−3 before December
    patents)  echo $(( $(date +%Y) - 4 )) ;;  # USPTO TRCFECO2 trademark snapshot {year+1} settles slowly; window must reach the last published filing year
    crime)    echo $(( $(date +%Y) - 4 )) ;;  # BJS NCVS publication lag: $where year='{year}' returns 0 rows for current−2; need current−4 so bjs_ncvs_* land a published survey year
    *)        echo $(( $(date +%Y) - 2 )) ;;  # annual/sub-annual: N−2 safe default (covers dataLag≤2)
  esac
}

# Return the idle-timeout in minutes for a worker.
# A worker is killed and re-queued only when BOTH conditions hold:
#   1. It has been running for at least this many minutes
#   2. Its log file has been idle (no new output) for at least this many minutes
# Usage: get_timeout_config <worker_id>  → prints minutes to stdout
get_timeout_config() {
  local worker_id=$1
  local _id="${worker_id#worker-}"
  local _schema="${_id%-*}"

  # Inner DQ workers: worker-dq-<schema>-<mode> → use the inner schema
  if [[ "$worker_id" == worker-dq-*-* ]]; then
    local _dq_rest="${worker_id#worker-dq-}"
    _schema="${_dq_rest%-*}"
  # Outer DQ launchers: worker-<schema>-dq[-rebuild|-etl-resume] → strip the -dq* suffix.
  # Without this, worker-patents-dq-rebuild parses to schema "patents-dq", finds no YAML,
  # and falls back to the 60-min default — too short for a full rebuild.
  elif [[ "$worker_id" == worker-*-dq || "$worker_id" == worker-*-dq-* ]]; then
    _schema="${_id%%-dq*}"
  fi

  # Check schema YAML for workerTimeoutMinutes — takes precedence over hardcoded defaults
  local _yaml_file
  _yaml_file="$(dirname "${BASH_SOURCE[0]}")/../../src/main/resources/${_schema}/${_schema}-schema.yaml"
  if [[ -f "$_yaml_file" ]]; then
    local _yaml_timeout
    _yaml_timeout=$(grep -E "^workerTimeoutMinutes:" "$_yaml_file" | awk '{print $2}' | tr -d '"'"'"'')
    if [[ -n "$_yaml_timeout" ]] && [[ "$_yaml_timeout" =~ ^[0-9]+$ ]]; then
      echo "$_yaml_timeout"
      return
    fi
  fi

  case "$_schema" in
    edu)          echo 360 ;;  # K-12 + IPEDS + Scorecard — many sub-pipelines
    weather)      echo 360 ;;  # GHCND × 51 states × years — large volume
    geo)          echo 360 ;;  # TIGER shapefiles, geometry parsing
    fec)          echo 240 ;;  # individual contributions 3M+ rows/year
    crime)        echo 720 ;;  # cde_crime_agency ~19k ORIs × offenses; full sweep > 4h
    health)       echo 240 ;;  # 15 tables with cursor pagination
    lands)        echo 240 ;;  # forest inventory biennial, large parcels
    census)       echo 240 ;;  # ACS 5-year span
    energy)       echo 180 ;;  # SEDS multi-year
    *)            echo 60  ;;  # default: 1 hour idle
  esac
}

# Build an inline Calcite model JSON string for a govdata schema.
# All storage and credential config uses ${VAR} patterns resolved by Java's VariableResolver.
# Integer fields (startYear, endYear) are resolved from env vars at call time.
#
# Usage: build_inline_model <schema_name> [extra_operand_json]
#   schema_name        The govdata dataSource name (fec, econ, sec, etc.)
#   extra_operand_json Optional additional JSON fields (no leading comma), e.g.:
#                      '"filingTypes":["10-K","10-Q"],"chunksBackfill":true'
#
# Outputs the complete inline JSON string to stdout.
build_inline_model() {
  local schema_name=$1
  local extra_operand=${2:-}

  local _INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-$(date +%Y)}
  local _START_YEAR=${GOVDATA_START_YEAR:-}
  local _END_YEAR=${GOVDATA_END_YEAR:-}

  local year_json=""
  if [ -n "$_START_YEAR" ]; then
    year_json="${year_json}\"startYear\":${_START_YEAR},"
  fi
  if [ -n "$_END_YEAR" ]; then
    year_json="${year_json}\"endYear\":${_END_YEAR},"
  fi

  local extra_json=""
  if [ -n "$extra_operand" ]; then
    extra_json=",${extra_operand}"
  fi

  # data-fix.sh sets FORCE_FRESH=true to trigger freshStart (calls clearAllCompletions on the tracker)
  if [ "${FORCE_FRESH:-false}" = true ]; then
    extra_json="${extra_json},\"freshStart\":true"
  fi

  printf '{"version":"1.0","defaultSchema":"%s","schemas":[{"name":"%s","type":"custom","factory":"org.apache.calcite.adapter.govdata.GovDataSchemaFactory","operand":{"dataSource":"%s",%s"autoDownload":true,"directory":"${GOVDATA_PARQUET_DIR}","cacheDirectory":"${GOVDATA_CACHE_DIR}/%s","trackerBackend":"s3","trackerConfig":{"bucket":"${CALCITE_TRACKER_S3_BUCKET}","endpoint":"${AWS_ENDPOINT_OVERRIDE}"},"s3Config":{"accessKeyId":"${AWS_ACCESS_KEY_ID}","secretAccessKey":"${AWS_SECRET_ACCESS_KEY}","endpoint":"${AWS_ENDPOINT_OVERRIDE}"}%s}}]}' \
    "$schema_name" "$schema_name" "$schema_name" "$year_json" "$schema_name" "$extra_json"
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
    2>&1 | tee "$log_file"

  local exit_code=${PIPESTATUS[0]}
  trap - INT TERM
  echo "[$worker_id] ETL finished with exit code: $exit_code"
  return $exit_code
}

# Usage: run_etl_inline <inline_json> <worker_id> [extra etl args...]
# Runs ETL with an inline model JSON string — no temp file created.
# The caller builds the JSON (with ${VAR} patterns for Java resolution) and passes it.
run_etl_inline() {
  local inline_json=$1 worker_id=$2
  shift 2

  local jar
  jar=$(resolve_classpath)

  local log_dir="$SCRIPT_DIR/runs/$worker_id"
  mkdir -p "$log_dir"

  local timestamp
  timestamp=$(date +%Y%m%d_%H%M%S)
  local log_file="$log_dir/etl_${timestamp}.log"

  local _HEAP_MIN _HEAP_MAX
  get_heap_config "$worker_id"

  local extra_flags=""
  echo "[$worker_id] Starting ETL (inline model, heap: ${_HEAP_MIN}/${_HEAP_MAX})"
  if [ -n "${ETL_PARALLEL_THREADS:-}" ] && [ "${ETL_PARALLEL_THREADS:-0}" -gt 1 ]; then
    extra_flags="$extra_flags --threads $ETL_PARALLEL_THREADS"
    echo "[$worker_id] Parallel entity threads: $ETL_PARALLEL_THREADS"
  fi
  echo "[$worker_id] Log: $log_file"

  trap 'kill 0' INT TERM

  java \
    -Xms"${_HEAP_MIN}" \
    -Xmx"${_HEAP_MAX}" \
    -cp "$jar" \
    org.apache.calcite.adapter.govdata.etl.EtlRunner \
    --model "inline:${inline_json}" \
    --verbose \
    $extra_flags \
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
