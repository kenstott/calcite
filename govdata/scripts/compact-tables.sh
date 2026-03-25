#!/bin/bash
# Compact Iceberg tables with small file problems
#
# Uses the existing IcebergMaintenanceRunner which handles R2/S3
# via IcebergCatalogManager (same path as ETL workers).
#
# Usage:
#   ./compact-tables.sh              # Compact all listed tables
#   ./compact-tables.sh <table>      # Compact a specific table
#   ./compact-tables.sh --dry-run    # Report only

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GOVDATA_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load environment
if [ -f "$GOVDATA_DIR/.env.prod" ]; then
  source "$GOVDATA_DIR/.env.prod"
elif [ -f "$GOVDATA_DIR/.env.sh" ]; then
  source "$GOVDATA_DIR/.env.sh"
fi

JAR=$(ls -t "$GOVDATA_DIR/build/libs/"*-all.jar 2>/dev/null | head -1)
if [ -z "$JAR" ]; then
  echo "ERROR: Shadow JAR not found. Run: ./gradlew :govdata:shadowJar"
  exit 1
fi

# Tables that need compaction: table_name|warehouse_path|min_files
TABLES=(
  # Crime — worst offenders
  "cde_offenses|s3a://govdata-parquet-v1/source=crime/CRIME|3"
  "cde_police_employment|s3a://govdata-parquet-v1/source=crime/CRIME|3"
  "cde_use_of_force|s3a://govdata-parquet-v1/source=crime/CRIME|3"
  "cde_hate_crimes|s3a://govdata-parquet-v1/source=crime/CRIME|5"
  "cde_leoka|s3a://govdata-parquet-v1/source=crime/CRIME|3"
  "cde_agencies|s3a://govdata-parquet-v1/source=crime/CRIME|5"
  # Census
  "qwi_employment|s3a://govdata-parquet-v1/source=census/CENSUS|3"
  "acs1_population|s3a://govdata-parquet-v1/source=census/CENSUS|3"
  "acs1_income|s3a://govdata-parquet-v1/source=census/CENSUS|3"
  # Econ
  "ita_data|s3a://govdata-parquet-v1/source=econ/econ|3"
  "fred_indicators|s3a://govdata-parquet-v1/source=econ/econ|3"
  "industry_gdp|s3a://govdata-parquet-v1/source=econ/econ|3"
  "gdp_statistics|s3a://govdata-parquet-v1/source=econ/econ|3"
  "wage_growth|s3a://govdata-parquet-v1/source=econ/econ|3"
  "national_accounts|s3a://govdata-parquet-v1/source=econ/econ|3"
  # Geo
  "state_legislative_lower|s3a://govdata-parquet-v1/source=geo/GEO|3"
  "state_legislative_upper|s3a://govdata-parquet-v1/source=geo/GEO|3"
  "pumas|s3a://govdata-parquet-v1/source=geo/GEO|3"
  "congressional_districts|s3a://govdata-parquet-v1/source=geo/GEO|3"
  "states|s3a://govdata-parquet-v1/source=geo/GEO|3"
  "watersheds_huc2|s3a://govdata-parquet-v1/source=geo/GEO|3"
  # Weather
  "cdo_annual_summaries|s3a://govdata-parquet-v1/source=weather/WEATHER|5"
  "cdo_monthly_summaries|s3a://govdata-parquet-v1/source=weather/WEATHER|5"
)

# Parse args
DRY_RUN=""
FILTER=""
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN="--dry-run" ;;
    *) FILTER="$arg" ;;
  esac
done

ENDPOINT="${AWS_ENDPOINT_OVERRIDE:-}"
S3_ARGS=""
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  S3_ARGS="--s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY"
  if [ -n "$ENDPOINT" ]; then
    S3_ARGS="$S3_ARGS --s3-endpoint $ENDPOINT"
  fi
fi

total=0
success=0
failed=0

for entry in "${TABLES[@]}"; do
  IFS='|' read -r table_name warehouse min_files <<< "$entry"

  if [ -n "$FILTER" ] && [ "$FILTER" != "--dry-run" ] && [ "$table_name" != "$FILTER" ]; then
    continue
  fi

  echo ""
  echo ">>> $table_name"

  java -Xms512m -Xmx2g \
    -cp "$JAR" \
    org.apache.calcite.adapter.govdata.etl.IcebergMaintenanceRunner \
    --warehouse "$warehouse" \
    --table "$table_name" \
    --min-files "$min_files" \
    --small-size-mb 10 \
    --target-size-mb 128 \
    --expire-days 1 \
    --orphan-days 1 \
    $S3_ARGS \
    $DRY_RUN \
    2>&1 | grep -v "^log4j:\|WARN.*hadoop\|WARN.*Metrics\|^$"

  rc=${PIPESTATUS[0]}
  total=$((total + 1))
  if [ $rc -eq 0 ]; then
    success=$((success + 1))
  else
    failed=$((failed + 1))
    echo "    FAILED (exit $rc)"
  fi
done

echo ""
echo "========================================="
echo "Compaction: $success/$total succeeded, $failed failed"
