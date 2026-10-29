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
# The Maven Central bundles a release carries, one asset name per line, for the tag given.
# Every one is a signed bundle attached to the release by the tag's workflows; the
# "Promote to Maven Central" workflow uploads exactly this set.
#
#   central-bundles.sh engine-v<version> | sih-v<version>
#
# Exits 2 on a tag of any other kind.
set -euo pipefail

tag="$1"
case "$tag" in
  engine-v*)
    version="${tag#engine-v}"
    # askamerica-engine.yml, publish-govdata.yml, and publish-trino.yml's matrix.
    artifacts=(askamerica-engine govdata
      trino-calcite-base trino-calcite trino-cloudops trino-file
      trino-salesforce trino-servicenow trino-sharepoint trino-splunk) ;;
  sih-v*)
    version="${tag#sih-v}"
    # sih-drivers.yml.
    artifacts=(sih-govdata sih-splunk sih-sharepoint sih-aperio sih-cloudops) ;;
  *)
    echo "not an engine-v or sih-v tag: $tag" >&2
    exit 2 ;;
esac

for artifact in "${artifacts[@]}"; do
  echo "${artifact}-${version}-central-bundle.zip"
done
