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
# data_purge_minio.sh — data_purge.sh, hardcoded to the local MinIO remote.
#
# data_purge.sh defaults to `--env prod` -> rclone remote "r2" (Cloudflare R2), which is
# NOT the bucket that model-verify.sh --mode local / real queries read (they hit the local
# MinIO mirror at AWS_ENDPOINT_OVERRIDE, via rclone remote "minio"). Running data_purge.sh
# bare therefore reports a clean "Purged N files" / "already empty" while the actual data
# the rest of the toolchain reads is untouched — confirmed live 2026-08-29: 27 tables across
# sec/fedregister/cyber_threat/ref/patents were "purged" against r2 while MinIO still held
# full pre-purge row counts (sec.financial_line_items: 303M rows, xbrl_relationships: 292M).
#
# This wrapper forces --remote minio so that mistake cannot recur silently. It passes
# through every other flag unchanged, rejecting an explicit --remote from the caller (to
# avoid a confusing "minio but actually not minio" footgun) — use data_purge.sh directly if
# you genuinely need a remote other than minio.
#
# Usage: data_purge_minio.sh --schema <schema> --tables <t1,t2,...> [--env prod|dq] [--raw] [--dry-run]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for arg in "$@"; do
  if [[ "$arg" == "--remote" ]]; then
    echo "Error: data_purge_minio.sh always targets --remote minio; do not pass --remote yourself." >&2
    echo "       Use data_purge.sh directly for a different remote." >&2
    exit 2
  fi
done

exec "$SCRIPT_DIR/data_purge.sh" "$@" --remote minio
