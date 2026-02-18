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
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"

# Setup logging - all output goes to both terminal and log file
LOG_DIR="${GOVDATA_HOME}/runs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/etl-$(date +%Y%m%d-%H%M%S).log"
LATEST_LOG="${LOG_DIR}/etl-latest.log"
exec > >(tee -a "$LOG_FILE") 2>&1
ln -sf "$LOG_FILE" "$LATEST_LOG"
echo "Logging to: $LOG_FILE"
echo "Latest log symlink: $LATEST_LOG"

# Load environment
ENV_FILE="${ENV_FILE:-.env.prod}"
if [[ -f "$GOVDATA_HOME/$ENV_FILE" ]]; then
    set -a
    source "$GOVDATA_HOME/$ENV_FILE"
    set +a
fi

# Parse arguments
START_AT=1
LIST_ONLY=false
COUNT_ONLY=false
RESET_STATE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --start)
            START_AT="$2"
            shift 2
            ;;
        --list)
            LIST_ONLY=true
            shift
            ;;
        --count)
            COUNT_ONLY=true
            shift
            ;;
        --reset)
            RESET_STATE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Generate all jobs in priority order
generate_jobs() {
    local job_num=0

    local cik_groups=("_DJIA_CONSTITUENTS" "_SP500_CONSTITUENTS" "_RUSSELL1000_CONSTITUENTS" "_ALL_EDGAR_FILERS")
    local cik_names=("dow30" "sp500" "russell1000" "all")

    # === SEC 10-K (2010-2026) - HIGHEST PRIORITY ===
    for cik_idx in "${!cik_groups[@]}"; do
        local cik="${cik_groups[$cik_idx]}"
        local cik_name="${cik_names[$cik_idx]}"
        for year in $(seq 2026 -1 2010); do
            ((job_num++))
            echo "$job_num|sec|$cik_name|10k|$year|$cik|10-K"
        done
    done

    # === SEC 10-Q (2010-2026) ===
    for cik_idx in "${!cik_groups[@]}"; do
        local cik="${cik_groups[$cik_idx]}"
        local cik_name="${cik_names[$cik_idx]}"
        for year in $(seq 2026 -1 2010); do
            ((job_num++))
            echo "$job_num|sec|$cik_name|10q|$year|$cik|10-Q"
        done
    done

    # === STOCK PRICES (2000-2026) ===
    for cik_idx in "${!cik_groups[@]}"; do
        local cik="${cik_groups[$cik_idx]}"
        local cik_name="${cik_names[$cik_idx]}"
        for year in $(seq 2026 -1 2000); do
            ((job_num++))
            echo "$job_num|stock_prices|$cik_name|prices|$year|$cik|"
        done
    done

    # === ECON (2000-2026) ===
    # Note: BLS only available 2004+
    local econ_sources=("bea" "fred" "treasury" "worldbank" "bls")
    for source in "${econ_sources[@]}"; do
        local start_year=2000
        if [[ "$source" == "bls" ]]; then
            start_year=2004
        fi
        for year in $(seq 2026 -1 $start_year); do
            ((job_num++))
            echo "$job_num|econ|$source||$year||"
        done
    done

    # === CENSUS (2005-2026) ===
    for year in $(seq 2026 -1 2005); do
        ((job_num++))
        echo "$job_num|census|acs||$year||"
    done

    # === GEO (2007-2025 + 2000) ===
    for year in $(seq 2025 -1 2007); do
        ((job_num++))
        echo "$job_num|geo|tiger||$year||"
    done
    ((job_num++))
    echo "$job_num|geo|tiger||2000||"

    # === SEC 8-K, Form 4, Other (2010-2026) - LOWER PRIORITY ===
    local low_priority_types=("8-K" "4" "3,5,DEF_14A,S-1,S-3,S-4,S-8")
    local low_priority_names=("8k" "form4" "other")

    for ft_idx in "${!low_priority_types[@]}"; do
        local ft="${low_priority_types[$ft_idx]}"
        local ft_name="${low_priority_names[$ft_idx]}"
        for cik_idx in "${!cik_groups[@]}"; do
            local cik="${cik_groups[$cik_idx]}"
            local cik_name="${cik_names[$cik_idx]}"
            for year in $(seq 2026 -1 2010); do
                ((job_num++))
                echo "$job_num|sec|$cik_name|$ft_name|$year|$cik|$ft"
            done
        done
    done
}

# Reset state for a job (clears DuckDB cache entries to allow re-processing)
#
# SEC cache keys are now structured as: cik_processed:{cik}:{year}:{filingType}
# This allows targeted reset by year and filing type.
#
reset_job_state() {
    local schema="$1"
    local source="$2"
    local type="$3"
    local year="$4"
    local cik_group="$5"
    local filing_types="$6"

    local cache_dir="${GOVDATA_CACHE_DIR:-$GOVDATA_HOME/build/.aperio}"

    echo "Resetting cache state for: $schema / $source / $type / $year"

    case "$schema" in
        sec|stock_prices)
            # SEC/stock cache is in DuckDB - clear cik_processed flags for this year+type
            local db_file="$cache_dir/cache_sec.duckdb"
            if [[ -f "$db_file" ]]; then
                echo "  SEC cache DB: $db_file"
                echo "  Year: $year, Filing types: $filing_types"
                # Clear cik_processed entries for this year and filing type(s)
                # Key format: cik_processed:{cik}:{year}:{filingType}
                # Use pattern: cik_processed:%:{year}:{filingType}
                IFS=',' read -ra TYPES <<< "$filing_types"
                for ft in "${TYPES[@]}"; do
                    local pattern="cik_processed:%:${year}:${ft}"
                    local count=$(duckdb "$db_file" -c "SELECT COUNT(*) FROM cache_entries WHERE cache_key LIKE '$pattern';" 2>/dev/null | tail -1)
                    duckdb "$db_file" -c "DELETE FROM cache_entries WHERE cache_key LIKE '$pattern';" 2>/dev/null || true
                    echo "  Cleared cik_processed entries for year=$year type=$ft (pattern: $pattern)"
                done
            else
                echo "  No SEC cache DB found at $db_file"
            fi
            ;;
        econ)
            # ECON cache is in DuckDB - clear entries for this source/year
            local db_file="$cache_dir/cache_econ.duckdb"
            if [[ -f "$db_file" ]]; then
                echo "  ECON cache DB: $db_file"
                # ECON cache keys include source and year, so we can be targeted
                local pattern="${source}:${year}:%"
                duckdb "$db_file" -c "DELETE FROM cache_entries WHERE cache_key LIKE '$pattern';" 2>/dev/null || true
                echo "  Cleared cache entries for $source year $year"
            else
                echo "  No ECON cache DB found at $db_file"
            fi
            ;;
        census)
            # Census cache is in DuckDB
            local db_file="$cache_dir/cache_census.duckdb"
            if [[ -f "$db_file" ]]; then
                echo "  Census cache DB: $db_file"
                local pattern="acs:${year}:%"
                duckdb "$db_file" -c "DELETE FROM cache_entries WHERE cache_key LIKE '$pattern';" 2>/dev/null || true
                echo "  Cleared cache entries for ACS year $year"
            else
                echo "  No Census cache DB found at $db_file"
            fi
            ;;
        geo)
            # GEO cache is in DuckDB
            local db_file="$cache_dir/cache_geo.duckdb"
            if [[ -f "$db_file" ]]; then
                echo "  GEO cache DB: $db_file"
                local pattern="tiger:${year}:%"
                duckdb "$db_file" -c "DELETE FROM cache_entries WHERE cache_key LIKE '$pattern';" 2>/dev/null || true
                echo "  Cleared cache entries for TIGER year $year"
            else
                echo "  No GEO cache DB found at $db_file"
            fi
            ;;
    esac

    echo "  Cache reset complete"
}

# Count jobs
JOBS=$(generate_jobs)
TOTAL_JOBS=$(echo "$JOBS" | wc -l | tr -d ' ')

if $COUNT_ONLY; then
    echo "Total jobs: $TOTAL_JOBS"
    exit 0
fi

if $LIST_ONLY; then
    echo "# ETL Full Schedule - $TOTAL_JOBS jobs"
    echo "#"
    echo "# Format: JOB_NUM|SCHEMA|SOURCE|TYPE|YEAR|CIK_GROUP|FILING_TYPES"
    echo "#"
    echo "$JOBS"
    exit 0
fi

# Run jobs
echo "=============================================="
echo "ETL Full Schedule"
echo "=============================================="
echo "Total jobs: $TOTAL_JOBS"
echo "Starting at: $START_AT"
echo "Reset state: $RESET_STATE"
echo "Environment: $ENV_FILE"
echo "=============================================="
echo ""

COMPLETED=0
FAILED=0

while IFS='|' read -r job_num schema source type year cik_group filing_types; do
    # Skip jobs before start point
    if [[ $job_num -lt $START_AT ]]; then
        continue
    fi

    echo "----------------------------------------------"
    echo "Job $job_num/$TOTAL_JOBS: $schema / $source / $type / $year"
    echo "----------------------------------------------"

    # Reset state if requested (only for the starting job)
    if $RESET_STATE && [[ $job_num -eq $START_AT ]]; then
        reset_job_state "$schema" "$source" "$type" "$year" "$cik_group" "$filing_types"
        echo ""
    fi

    # Build the model JSON dynamically
    MODEL_FILE=$(mktemp /tmp/etl-model-XXXXXX.json)

    # Build s3Config JSON if using S3
    s3_config=""
    if [[ "$GOVDATA_PARQUET_DIR" == s3://* ]]; then
        s3_config='"s3Config": {'
        [[ -n "$AWS_ACCESS_KEY_ID" ]] && s3_config+="\"accessKeyId\": \"$AWS_ACCESS_KEY_ID\","
        [[ -n "$AWS_SECRET_ACCESS_KEY" ]] && s3_config+="\"secretAccessKey\": \"$AWS_SECRET_ACCESS_KEY\","
        [[ -n "$AWS_ENDPOINT_OVERRIDE" ]] && s3_config+="\"endpoint\": \"$AWS_ENDPOINT_OVERRIDE\","
        [[ -n "$AWS_REGION" ]] && s3_config+="\"region\": \"$AWS_REGION\","
        # Remove trailing comma and close
        s3_config="${s3_config%,}},"
    fi

    case "$schema" in
        sec)
            # SEC filings
            cat > "$MODEL_FILE" << EOF
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [{
    "name": "sec",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "sec",
      "cacheDirectory": "$GOVDATA_CACHE_DIR",
      "directory": "$GOVDATA_PARQUET_DIR",
      $s3_config
      "ciks": "$cik_group",
      "filingTypes": ["${filing_types//,/\",\"}"],
      "fetchStockPrices": false,
      "startYear": $year,
      "endYear": $year,
      "autoDownload": true,
      "deferEmbeddings": true
    }
  }]
}
EOF
            ;;
        stock_prices)
            # Stock prices only
            cat > "$MODEL_FILE" << EOF
{
  "version": "1.0",
  "defaultSchema": "sec",
  "schemas": [{
    "name": "sec",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "sec",
      "cacheDirectory": "$GOVDATA_CACHE_DIR",
      "directory": "$GOVDATA_PARQUET_DIR",
      $s3_config
      "ciks": "$cik_group",
      "filingTypes": [],
      "fetchStockPrices": true,
      "stockPriceSource": "stooq",
      "startYear": $year,
      "endYear": $year,
      "autoDownload": true
    }
  }]
}
EOF
            ;;
        econ)
            # ECON data
            cat > "$MODEL_FILE" << EOF
{
  "version": "1.0",
  "defaultSchema": "econ",
  "schemas": [{
    "name": "econ",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "econ",
      "cacheDirectory": "$GOVDATA_CACHE_DIR",
      "directory": "$GOVDATA_PARQUET_DIR",
      $s3_config
      "enabledSources": ["$source"],
      "startYear": $year,
      "endYear": $year,
      "autoDownload": true
    }
  }]
}
EOF
            ;;
        census)
            # Census ACS
            cat > "$MODEL_FILE" << EOF
{
  "version": "1.0",
  "defaultSchema": "census",
  "schemas": [{
    "name": "census",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "census",
      "cacheDirectory": "$GOVDATA_CACHE_DIR",
      "directory": "$GOVDATA_PARQUET_DIR",
      $s3_config
      "enabledSources": ["acs"],
      "startYear": $year,
      "endYear": $year,
      "autoDownload": true
    }
  }]
}
EOF
            ;;
        geo)
            # GEO TIGER
            cat > "$MODEL_FILE" << EOF
{
  "version": "1.0",
  "defaultSchema": "geo",
  "schemas": [{
    "name": "geo",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
    "operand": {
      "dataSource": "geo",
      "cacheDirectory": "$GOVDATA_CACHE_DIR",
      "directory": "$GOVDATA_PARQUET_DIR",
      $s3_config
      "enabledSources": ["tiger", "hud"],
      "tigerYear": $year,
      "autoDownload": true
    }
  }]
}
EOF
            ;;
    esac

    # Run the ETL
    if "$SCRIPT_DIR/etl-runner.sh" --model "$MODEL_FILE"; then
        COMPLETED=$((COMPLETED + 1))
        echo "✓ Job $job_num completed successfully"
    else
        FAILED=$((FAILED + 1))
        echo "✗ Job $job_num FAILED"
        echo ""
        echo "Options:"
        echo "  1. Retry (recommended for transient errors):"
        echo "     $0 --start $job_num"
        echo ""
        echo "  2. Reset cache and retry (if you fixed code or need to reprocess):"
        echo "     $0 --start $job_num --reset"
        echo ""
        echo "  3. Skip this job and continue:"
        echo "     $0 --start $((job_num + 1))"
        echo ""
        rm -f "$MODEL_FILE"
        exit 1
    fi

    rm -f "$MODEL_FILE"
    echo ""

done <<< "$JOBS"

echo "=============================================="
echo "ETL Full Schedule Complete"
echo "=============================================="
echo "Completed: $COMPLETED"
echo "Failed: $FAILED"
echo "=============================================="

# Post-processing (GPU embeddings + VSS index) runs automatically per-job
# via schema-level postProcess configuration in sec-schema.yaml.
# This enables parallel job execution - each job handles its own post-processing.
#
# Schema postProcess runs after each job's tables are materialized:
#   1. gpu_embeddings: scripts/vss-gpu-runner.sh - generates embeddings on GPU
#   2. vss_refresh: scripts/vss.sh refresh - rebuilds HNSW index cache
