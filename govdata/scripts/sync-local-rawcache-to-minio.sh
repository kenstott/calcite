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
# sync-local-rawcache-to-minio.sh
#
# Copies raw HTTP response cache files from the local filesystem into the
# govdata-raw-v1 MinIO bucket, preserving the hive-partition path structure
# that HttpSource expects when it resolves a rawCache hit.
#
# Usage:
#   ./sync-local-rawcache-to-minio.sh [OPTIONS]
#
# Options:
#   --local-dir DIR     Source directory (default: /tmp/etl-raw-cache)
#   --bucket BUCKET     MinIO bucket (default: govdata-raw-v1)
#   --schema SCHEMA     Only sync files for this schema table prefix
#                       e.g. --schema cftc_trades  (default: all)
#   --dry-run           Print what would be synced without transferring
#   --transfers N       Parallel transfers (default: 8)
#
# Credentials are loaded from govdata/.env.dq (MinIO endpoint + keys).
# The rclone minio remote is NOT used directly because it points to localhost;
# this script passes credentials inline so the correct host is always used.
#
# Path contract:
#   Local:  {LOCAL_DIR}/{table}/{partition_vars}/response.json
#   MinIO:  {BUCKET}/{table}/{partition_vars}/response.json
#
# Example:
#   Local:  /tmp/etl-raw-cache/cftc_trades/asset_class=RATES/day=06/.../response.json
#   MinIO:  govdata-raw-v1/cftc_trades/asset_class=RATES/day=06/.../response.json

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
LOCAL_DIR="/tmp/etl-raw-cache"
BUCKET="govdata-raw-v1"
SCHEMA_FILTER=""
DRY_RUN=false
TRANSFERS=8

while [[ $# -gt 0 ]]; do
  case "$1" in
    --local-dir)   LOCAL_DIR="$2";      shift 2 ;;
    --bucket)      BUCKET="$2";         shift 2 ;;
    --schema)      SCHEMA_FILTER="$2";  shift 2 ;;
    --dry-run)     DRY_RUN=true;        shift   ;;
    --transfers)   TRANSFERS="$2";      shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# Load DQ credentials (MinIO endpoint + access key)
ENV_DQ="$GOVDATA_ROOT/.env.dq"
if [[ ! -f "$ENV_DQ" ]]; then
  echo "ERROR: $ENV_DQ not found — MinIO credentials unavailable" >&2
  exit 1
fi
set -a; source "$ENV_DQ"; set +a

: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID not set in $ENV_DQ}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY not set in $ENV_DQ}"
: "${AWS_ENDPOINT_OVERRIDE:?AWS_ENDPOINT_OVERRIDE not set in $ENV_DQ}"
: "${AWS_REGION:=us-east-1}"

# Resolve source path
# Require --schema for safety — syncing the entire raw cache without filtering is
# almost never what you want and risks stomping other schemas' cache entries.
if [[ -z "$SCHEMA_FILTER" ]]; then
  echo "ERROR: --schema is required. Example: --schema cftc" >&2
  exit 1
fi

# The framework stores rawCache at {BUCKET}/{schema}/{table}/{partitions}/response.json.
# Local cache has no schema prefix: {LOCAL_DIR}/{table}/{partitions}/response.json.
# We add the schema as the destination prefix so the paths align.
SRC="$LOCAL_DIR"
DST=":s3:$BUCKET/$SCHEMA_FILTER"

if [[ ! -d "$SRC" ]]; then
  echo "ERROR: Source directory not found: $SRC" >&2
  exit 1
fi

FILE_COUNT=$(find "$SRC" -type f | wc -l | tr -d ' ')
echo "Source : $SRC ($FILE_COUNT files)"
echo "Dest   : $DST  (endpoint: $AWS_ENDPOINT_OVERRIDE)"
echo "Bucket : $BUCKET"
[[ -n "$SCHEMA_FILTER" ]] && echo "Filter : $SCHEMA_FILTER"
$DRY_RUN && echo "Mode   : DRY RUN"
echo ""

RCLONE_FLAGS=(
  --s3-provider=Other
  --s3-endpoint="$AWS_ENDPOINT_OVERRIDE"
  --s3-access-key-id="$AWS_ACCESS_KEY_ID"
  --s3-secret-access-key="$AWS_SECRET_ACCESS_KEY"
  --s3-region="$AWS_REGION"
  --transfers="$TRANSFERS"
  --checksum
  --progress
  --stats=10s
)

if $DRY_RUN; then
  rclone copy --dry-run "$SRC" "$DST" "${RCLONE_FLAGS[@]}"
else
  rclone copy "$SRC" "$DST" "${RCLONE_FLAGS[@]}"
  echo ""
  echo "Sync complete. Verifying object count in MinIO..."
  REMOTE_COUNT=$(rclone ls "$DST" "${RCLONE_FLAGS[@]}" 2>/dev/null | wc -l | tr -d ' ')
  echo "  Local files  : $FILE_COUNT"
  echo "  Remote objects: $REMOTE_COUNT"
fi
