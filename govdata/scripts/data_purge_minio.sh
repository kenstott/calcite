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
# data_purge_minio.sh — data_purge.sh, pinned to the local MinIO remote.
#
# Passes through every flag unchanged and rejects an explicit --remote, so a caller of this
# script can only ever purge MinIO. Use data_purge.sh directly for a different remote.
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
