#!/usr/bin/env bash
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE-BSL.txt file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# Is an engine release complete? Lists the assets of release <tag> and names every one a
# complete release must have that is not there. Reads only; changes nothing.
#
#   release-assets-complete.sh <owner/repo> engine-v<version>
#
# Exits 0 when the set is complete, 1 when something is missing, 2 when the release cannot be
# read. A release is published only when this exits 0 (askamerica-engine.yml, publish-release),
# so that "latest" never names a release whose downloads are not all there.
set -euo pipefail

repo="$1"
tag="$2"
version="${tag#engine-v}"

if ! assets="$(gh release view "$tag" --repo "$repo" --json assets -q '.assets[].name')"; then
  echo "cannot read release $tag of $repo" >&2
  exit 2
fi

required=(
  "askamerica-engine.jar"
  "sih-govdata.jar"
  "askamerica-extension.zip"
  "AskAmerica-MCP-${version}.pkg"
  "askamerica-mcp_${version}_amd64.deb"
  "AskAmerica.MCP-${version}.msi"
)
for plugin in calcite cloudops file salesforce sharepoint splunk; do
  required+=("trino-${plugin}-plugin.zip")
done
# The pg-wire server bundles the engine and Provisa download and run, each with its checksum.
for adapter in calcite cloudops file govdata salesforce sharepoint splunk; do
  for variant in linux-x86_64 macos-arm64 windows-x86_64; do
    required+=("pgwire-${adapter}-${version}-${variant}.tar.gz")
    required+=("pgwire-${adapter}-${version}-${variant}.tar.gz.sha256")
  done
done

missing=0
for name in "${required[@]}"; do
  if ! grep -Fxq -- "$name" <<<"$assets"; then
    echo "missing: $name"
    missing=$((missing + 1))
  fi
done
have="$(grep -c . <<<"$assets" || true)"
echo "$tag: ${#required[@]} assets required, $missing missing, $have attached"
[ "$missing" -eq 0 ]
