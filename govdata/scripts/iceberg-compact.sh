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
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# All SEC Iceberg tables (from sec-schema.yaml)
ALL_TABLES=(
  filing_metadata
  financial_line_items
  filing_contexts
  mda_sections
  xbrl_relationships
  insider_transactions
  earnings_transcripts
  stock_prices
  vectorized_chunks
)

# Parse arguments: separate our flags from passthrough flags
SELECTED_TABLES=()
PASSTHROUGH_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --table)
      SELECTED_TABLES+=("$2")
      shift 2
      ;;
    *)
      PASSTHROUGH_ARGS+=("$1")
      shift
      ;;
  esac
done

# Default to all tables if none specified
if [[ ${#SELECTED_TABLES[@]} -eq 0 ]]; then
  SELECTED_TABLES=("${ALL_TABLES[@]}")
fi

# Build if needed
echo "Building govdata module..."
cd "$PROJECT_ROOT"
./gradlew :govdata:shadowJar --console=plain -q 2>&1 | tail -3

# Find the shadow JAR
SHADOW_JAR=$(find "$PROJECT_ROOT/govdata/build/libs" -name "*-all.jar" -o -name "*-shadow.jar" | head -1)
if [[ -z "$SHADOW_JAR" ]]; then
  # Fall back to regular classpath
  echo "No shadow JAR found, using classpath..."
  CLASSPATH=$(find "$PROJECT_ROOT/govdata/build/libs" -name "*.jar" | tr '\n' ':')
  CLASSPATH="$CLASSPATH:$(find "$PROJECT_ROOT/file/build/libs" -name "*.jar" | tr '\n' ':')"
  CLASSPATH="$CLASSPATH:$(find "$HOME/.gradle/caches" -name "iceberg-core-*.jar" 2>/dev/null | head -1)"
  CLASSPATH="$CLASSPATH:$(find "$HOME/.gradle/caches" -name "hadoop-common-*.jar" 2>/dev/null | head -1)"
else
  CLASSPATH="$SHADOW_JAR"
fi

# Validate environment
: "${AWS_ACCESS_KEY_ID:?Set AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?Set AWS_SECRET_ACCESS_KEY}"
: "${AWS_ENDPOINT_OVERRIDE:?Set AWS_ENDPOINT_OVERRIDE}"

S3_ENDPOINT="${AWS_ENDPOINT_OVERRIDE}"

echo ""
echo "============================================================"
echo "Iceberg Compaction - SEC Tables"
echo "============================================================"
echo "  Tables: ${SELECTED_TABLES[*]}"
echo "  Extra args: ${PASSTHROUGH_ARGS[*]:-none}"
echo ""

FAILED_TABLES=()
SUCCEEDED_TABLES=()

for TABLE in "${SELECTED_TABLES[@]}"; do
  echo ""
  echo "------------------------------------------------------------"
  echo "Compacting: $TABLE"
  echo "------------------------------------------------------------"

  if java -Xmx4g -cp "$CLASSPATH" \
    org.apache.calcite.adapter.govdata.etl.IcebergMaintenanceRunner \
    --warehouse "s3a://govdata-parquet-v1/sec" \
    --table "$TABLE" \
    --s3-access-key "$AWS_ACCESS_KEY_ID" \
    --s3-secret-key "$AWS_SECRET_ACCESS_KEY" \
    --s3-endpoint "$S3_ENDPOINT" \
    "${PASSTHROUGH_ARGS[@]+"${PASSTHROUGH_ARGS[@]}"}"; then
    SUCCEEDED_TABLES+=("$TABLE")
  else
    echo "WARNING: Compaction failed for $TABLE (exit code $?)"
    FAILED_TABLES+=("$TABLE")
  fi
done

# Summary
echo ""
echo "============================================================"
echo "Compaction Summary"
echo "============================================================"
echo "  Succeeded (${#SUCCEEDED_TABLES[@]}): ${SUCCEEDED_TABLES[*]:-none}"
echo "  Failed    (${#FAILED_TABLES[@]}): ${FAILED_TABLES[*]:-none}"
echo ""

if [[ ${#FAILED_TABLES[@]} -gt 0 ]]; then
  exit 1
fi
