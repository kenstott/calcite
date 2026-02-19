#!/usr/bin/env bash
# Iceberg compaction maintenance script for vectorized_chunks
# Usage: source .env.prod && bash scripts/iceberg-compact.sh [--dry-run]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

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

# Convert s3:// endpoint to just the hostname for Hadoop S3A
S3_ENDPOINT="${AWS_ENDPOINT_OVERRIDE}"

echo ""
echo "Running Iceberg compaction..."
echo ""

java -Xmx4g -cp "$CLASSPATH" \
  org.apache.calcite.adapter.govdata.etl.IcebergMaintenanceRunner \
  --warehouse "s3a://govdata-parquet-v1/source=sec/SEC" \
  --table vectorized_chunks \
  --s3-access-key "$AWS_ACCESS_KEY_ID" \
  --s3-secret-key "$AWS_SECRET_ACCESS_KEY" \
  --s3-endpoint "$S3_ENDPOINT" \
  "$@"
