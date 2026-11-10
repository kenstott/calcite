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
  "askamerica-engine-linux-x86_64.jar"
  "askamerica-engine-macos-arm64.jar"
  "askamerica-engine-windows-x86_64.jar"
  "sih-govdata.jar"
  "askamerica-extension.zip"
  "AskAmerica-MCP-${version}.pkg"
  "askamerica-mcp_${version}_amd64.deb"
  "AskAmerica.MCP-${version}.msi"
)
# The Trino plugins; servicenow from 0.110.1, the first release that builds it.
plugins=(calcite cloudops file salesforce sharepoint splunk)
[ "$(printf '%s\n' 0.110.1 "$version" | sort -V | head -1)" = 0.110.1 ] && plugins+=(servicenow)
for plugin in "${plugins[@]}"; do
  required+=("trino-${plugin}-plugin.zip")
done
# The pg-wire server bundles the engine and Provisa download and run, each with its checksum: the
# pgwire-calcite server and every adapter in packaging/adapters.json, for every platform in
# packaging/platforms.json (an entry from the release its "since" names on); and, from the release
# that first builds them, the signed Maven Central bundles of every adapter and platform with their
# pgwire-shared-<platform> bundles (REQ-294), each with its sha256. pgwire_release_names.py names
# them, as it does for central-bundles.sh.
names="$(dirname "$0")/pgwire_release_names.py"
tarballs="$(python3 "$names" tarballs "$version")"
central="$(python3 "$names" central "$version")"
for name in $tarballs $central; do
  required+=("$name" "${name}.sha256")
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
