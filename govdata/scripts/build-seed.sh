#!/bin/bash
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
# build-seed.sh — DEVELOPER tool. Generate the JAR-bundled DuckDB seed and stage it in-tree.
#
# Produces the pre-built shared catalog + per-schema .conversions.json trackers that
# GovDataSeedInstaller extracts on first connection to accelerate cold time-to-first-query. Two
# phases:
#   1. GENERATE — connect every govdata schema against S3 (via model-verify.sh) into a CLEAN,
#      dedicated staging operating directory, so GovDataDriver builds a pristine
#      <staging>/.duckdb/govdata.duckdb (all schemas) + <staging>/.aperio/*/.conversions.json.
#   2. PACKAGE — ./gradlew :govdata:bundleGovdataSeed zips those into
#      govdata/src/main/resources/duckdb/seed/{govdata-seed.zip,govdata-seed.version}.
#
# The seed is only portable when built in S3 mode: this script REFUSES to run unless
# GOVDATA_PARQUET_DIR is an s3:// URI (a local-parquet operating dir bakes machine-specific
# absolute paths into the catalog and trackers). Env comes from .env.prod (REQUIRED).
#
# After it finishes, review and commit the two staged files:
#   git add govdata/src/main/resources/duckdb/seed/govdata-seed.zip \
#           govdata/src/main/resources/duckdb/seed/govdata-seed.version
#
# Usage:
#   scripts/build-seed.sh                         # all schemas, staging under build/seed-staging
#   scripts/build-seed.sh --source sec,geo,econ   # subset (partial seed — for testing only)
#   scripts/build-seed.sh --version 2026.07.24    # override marker version (default: project version)
#   scripts/build-seed.sh --operating-dir /path   # override staging operating dir
#   scripts/build-seed.sh --keep                   # keep the staging dir after packaging
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"
REPO_ROOT="$(dirname "$GOVDATA_HOME")"

SOURCE=""
SEED_VERSION=""
STAGING="$GOVDATA_HOME/build/seed-staging"
KEEP=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)        SOURCE="$2"; shift 2 ;;
        --version)       SEED_VERSION="$2"; shift 2 ;;
        --operating-dir) STAGING="$2"; shift 2 ;;
        --keep)          KEEP=true; shift ;;
        -h|--help)       sed -n '17,45p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)               echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# --- Require S3 mode (portability guard) -----------------------------------------------------
ENV_PROD="$GOVDATA_HOME/.env.prod"
if [[ ! -f "$ENV_PROD" ]]; then
    echo "ERROR: $ENV_PROD not found — S3 endpoint/credentials + GOVDATA_PARQUET_DIR are required." >&2
    exit 1
fi
# shellcheck disable=SC1090
set -a; source "$ENV_PROD"; set +a

if [[ "${GOVDATA_PARQUET_DIR:-}" != s3://* ]]; then
    echo "ERROR: GOVDATA_PARQUET_DIR is '${GOVDATA_PARQUET_DIR:-<unset>}', not an s3:// URI." >&2
    echo "       The seed must be built in S3 mode or its paths are not portable. Aborting." >&2
    exit 1
fi

# --- Phase 1: GENERATE into a clean staging operating dir -------------------------------------
echo "=== build-seed: generating catalog into clean staging dir ==="
echo "Staging operating dir: $STAGING"
echo "Warehouse:             $GOVDATA_PARQUET_DIR"
rm -rf "$STAGING"
mkdir -p "$STAGING"

VERIFY_ARGS=(--no-probes --no-dup)
if [[ -n "$SOURCE" ]]; then
    VERIFY_ARGS+=(--source "$SOURCE")
    echo "WARNING: --source given; the resulting seed is PARTIAL (missing schemas cold-start normally)."
fi

# model-verify.sh connects jdbc:govdata:source=<schema> per schema, which forces GovDataDriver to
# build the shared catalog + every Iceberg view + .conversions.json. Redirect the operating dir to
# staging so we package a pristine catalog rather than the developer's working ~/.govdata.
GOVDATA_DATA_DIR="$STAGING" "$SCRIPT_DIR/model-verify.sh" "${VERIFY_ARGS[@]}"

CATALOG="$STAGING/.duckdb/govdata.duckdb"
if [[ ! -f "$CATALOG" ]]; then
    echo "ERROR: generation did not produce $CATALOG — check the model-verify output above." >&2
    exit 1
fi

# Fold any residual WAL into the catalog file, and prove it reopens cleanly, BEFORE packaging.
#
# DuckDBJdbcSchemaFactory already CHECKPOINTs after schema-init view setup, but views that
# reference cross-schema tables are enqueued for DEFERRED creation and only materialize on first
# getTable() — i.e. during model-verify's probing, AFTER that checkpoint. That DDL therefore lives
# in govdata.duckdb.wal and is absorbed only by a clean JVM shutdown. Relying on that left the
# Gradle task's `if (wal.exists()) throw` as the sole guarantee, which fails ~20 minutes into a run
# and only at packaging time. Doing it here makes the invariant true rather than hoped-for, and
# surfaces a torn catalog immediately.
#
# Idempotent: when model-verify exited cleanly there is nothing to fold and this is a no-op.
# Opening the catalog also proves it is readable before we spend time zipping it.
if ! command -v duckdb >/dev/null 2>&1; then
    echo "ERROR: duckdb CLI not on PATH — needed to CHECKPOINT the staged catalog." >&2
    echo "       Install: https://duckdb.org/docs/installation/" >&2
    exit 1
fi
echo "=== build-seed: checkpointing staged catalog ==="
if ! duckdb "$CATALOG" -c "CHECKPOINT;"; then
    echo "ERROR: CHECKPOINT of $CATALOG failed — refusing to bundle a torn catalog." >&2
    exit 1
fi

WAL="$CATALOG.wal"
if [[ -f "$WAL" ]]; then
    echo "ERROR: $WAL still present after CHECKPOINT — the catalog is still held open, or the" >&2
    echo "       checkpoint did not complete. Refusing to bundle." >&2
    exit 1
fi
echo "Catalog checkpointed, no WAL: $CATALOG"

# --- Phase 2: PACKAGE via the Gradle task ----------------------------------------------------
echo "=== build-seed: packaging seed zip ==="
GRADLE_ARGS=(":govdata:bundleGovdataSeed" "-PseedOperatingDir=$STAGING")
if [[ -n "$SEED_VERSION" ]]; then
    GRADLE_ARGS+=("-PseedVersion=$SEED_VERSION")
fi
(cd "$REPO_ROOT" && ./gradlew "${GRADLE_ARGS[@]}")

if [[ "$KEEP" == false ]]; then
    rm -rf "$STAGING"
fi

SEED_DIR="$GOVDATA_HOME/src/main/resources/duckdb/seed"
echo
echo "=== build-seed: done ==="
echo "Staged artifacts:"
ls -lh "$SEED_DIR/govdata-seed.zip" "$SEED_DIR/govdata-seed.version" 2>/dev/null || true
echo
echo "Review the size, then commit them in-tree:"
echo "  git add govdata/src/main/resources/duckdb/seed/govdata-seed.zip \\"
echo "          govdata/src/main/resources/duckdb/seed/govdata-seed.version"
