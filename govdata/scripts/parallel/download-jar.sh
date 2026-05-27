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
# Downloads sih-govdata.jar from the latest (or specified) engine-vX.Y.Z
# GitHub release into govdata/build/libs/ and exports GOVDATA_JAR.
#
# Usage:
#   ./download-jar.sh                  # download latest release
#   ./download-jar.sh --tag engine-v0.4.15   # download specific release
#   source ./download-jar.sh           # download and export GOVDATA_JAR into current shell
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JAR_DIR="$GOVDATA_ROOT/build/libs"
JAR_PATH="$JAR_DIR/sih-govdata.jar"

TAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      shift
      TAG="${1:?--tag requires a release tag, e.g. engine-v0.4.15}"
      ;;
    --help|-h)
      sed -n '18,27p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
  shift
done

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: gh (GitHub CLI) not found in PATH" >&2
  echo "       Install from https://cli.github.com or run: brew install gh" >&2
  exit 1
fi

if [ -z "$TAG" ]; then
  TAG=$(gh release list --repo kenstott/calcite --limit 10 --json tagName \
    --jq '[.[] | select(.tagName | startswith("engine-v"))] | .[0].tagName' 2>/dev/null || true)
  if [ -z "$TAG" ] || [ "$TAG" = "null" ]; then
    echo "ERROR: could not determine latest engine-v* release tag — check gh auth status" >&2
    exit 1
  fi
fi

echo "Downloading sih-govdata.jar from release $TAG"
mkdir -p "$JAR_DIR"

if ! gh release download "$TAG" \
    --repo kenstott/calcite \
    --pattern "sih-govdata.jar" \
    --dir "$JAR_DIR" \
    --clobber; then
  echo "ERROR: download failed for $TAG" >&2
  exit 1
fi

if [ ! -f "$JAR_PATH" ]; then
  echo "ERROR: sih-govdata.jar not found after download — asset may be missing from $TAG" >&2
  exit 1
fi

SIZE=$(du -sh "$JAR_PATH" | cut -f1)
echo "Ready: $JAR_PATH ($SIZE)"
export GOVDATA_JAR="$JAR_PATH"
