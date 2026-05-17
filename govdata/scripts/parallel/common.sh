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

  # In daily mode, use GOVDATA_INCREMENTAL_START_YEAR as the effective start boundary.
  # yearRange returns empty list when start > end, so tables with no data in the
  # incremental window are skipped cleanly rather than processing all historical years.
  if [ "${GOVDATA_RUN_MODE:-}" = "daily" ]; then
    export GOVDATA_START_YEAR="${GOVDATA_INCREMENTAL_START_YEAR:-2026}"
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
  jar=$(find "$GOVDATA_ROOT/build/libs" -name "calcite-govdata-*-all.jar" 2>/dev/null | head -1)
  if [ -z "$jar" ]; then
    echo "ERROR: Shadow JAR not found. Run: ./gradlew :govdata:shadowJar" >&2
    echo "       Or set GOVDATA_JAR=/path/to/calcite-govdata-*-all.jar to use a worktree build." >&2
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

# Generate a single-schema model JSON for one non-SEC data source.
# Respects GOVDATA_RUN_MODE (historical | daily), GOVDATA_START_YEAR, and
# GOVDATA_INCREMENTAL_START_YEAR to set the correct year bounds.
# Usage: generate_single_schema_model <schema_name> <output_file>
generate_single_schema_model() {
  local schema_name=$1 output_file=$2
  local operand_body

  local _INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-2026}
  local _START_YEAR=${GOVDATA_START_YEAR:-2010}

  # historical mode: cap at INCREMENTAL_YEAR-1; daily mode: start from INCREMENTAL_YEAR
  local _YEAR_RANGE
  if [ "${GOVDATA_RUN_MODE:-daily}" = "historical" ]; then
    _YEAR_RANGE="\"startYear\": ${_START_YEAR},
      \"endYear\": $((_INCREMENTAL_YEAR - 1))"
  else
    _YEAR_RANGE="\"startYear\": ${_INCREMENTAL_YEAR}"
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
      \"tigerYear\": 2024"
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
  num=$(echo "$worker_id" | grep -oE '[0-9]+' | head -1 || true)

  if [ -z "$num" ]; then
    # Named historical workers get large heap; unrecognized non-numeric IDs get standard heap
    case "$worker_id" in
      *edu*historical*|*edu*initial*)
        _HEAP_MIN="4g"; _HEAP_MAX="6g" ;;
      *)
        _HEAP_MIN="2g"; _HEAP_MAX="3g" ;;
    esac
    return
  fi

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
  elif [ "$num" -eq 61 ]; then
    # FedRegister (61): 4 doc_types x 16 years x ~85 pages/partition pagination
    _HEAP_MIN="2g"
    _HEAP_MAX="3g"
  elif [ "$num" -eq 62 ]; then
    # Cyber initial (62): full NVD catalog ~350k CVEs + OTX backfill needs extra heap
    _HEAP_MIN="4g"
    _HEAP_MAX="6g"
  elif [ "$num" -ge 63 ] && [ "$num" -le 66 ]; then
    # Cyber recurring (63=daily, 64=weekly, 65=hourly, 66=static): delta/incremental, lighter
    _HEAP_MIN="2g"
    _HEAP_MAX="3g"
  elif [ "$num" -eq 67 ]; then
    # Health initial (67): all 15 tables including clinical trials cursor pagination
    _HEAP_MIN="4g"
    _HEAP_MAX="6g"
  elif [ "$num" -ge 68 ] && [ "$num" -le 70 ]; then
    # Health recurring (68=daily, 69=weekly, 70=monthly): incremental/delta loads
    _HEAP_MIN="2g"
    _HEAP_MAX="3g"
  elif [ "$num" -eq 71 ]; then
    # EDU initial (71): full historical K-12 + IPEDS + College Scorecard across all years
    _HEAP_MIN="4g"
    _HEAP_MAX="6g"
  elif [ "$num" -ge 72 ] && [ "$num" -le 73 ]; then
    # EDU recurring (72=annual, 73=biennial): incremental since-year loads
    _HEAP_MIN="2g"
    _HEAP_MAX="3g"
  elif [ "$num" -eq 21 ]; then
    # Crime (21): large dimension expansion (type × year × state × offense × ori), long-running
    _HEAP_MIN="3g"
    _HEAP_MAX="4g"
  elif [ "$num" -ge 23 ] && [ "$num" -le 58 ]; then
    # SEC Secondary (23-39), prices (40), compact (42-58): tracker preload needs ~2.5GB
    _HEAP_MIN="2g"
    _HEAP_MAX="3g"
  else
    # SEC Primary (1-17: 10-K/10-Q), non-SEC data (18-22): standard heap
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
    2>&1 | tee "$log_file"

  local exit_code=${PIPESTATUS[0]}
  trap - INT TERM
  echo "[$worker_id] ETL finished with exit code: $exit_code"
  return $exit_code
}

log_info() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}
