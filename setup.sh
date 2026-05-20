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

REPO="git@github.com:kenstott/calcite-private.git"
TARGET=".claude"

if [[ -d "$TARGET/.git" ]]; then
  echo ".claude/ already initialised — skipping clone."
  echo "Run ./pull-claude.sh to fetch the latest changes."
  exit 0
fi

if [[ -d "$TARGET" ]] && [[ -n "$(ls -A "$TARGET" 2>/dev/null)" ]]; then
  echo "ERROR: $TARGET exists and is non-empty but is not a git repository." >&2
  echo "Remove or rename it before running setup.sh." >&2
  exit 1
fi

echo "Cloning $REPO into $TARGET/ ..."
git clone "$REPO" "$TARGET"
echo "Done. Claude configuration is ready in $TARGET/."
