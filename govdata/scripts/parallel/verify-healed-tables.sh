#!/usr/bin/env bash
# verify-healed-tables.sh — confirm the healed tables were written correctly to MinIO.
#
# ── WHY ──────────────────────────────────────────────────────────────────────────────────────
# heal-sort-order.sh exiting 0 is not evidence that the heal worked. It commits a RewriteFiles:
# every data file in a partition is replaced and a new manifest is written. If that rewrite
# silently dropped rows, or produced files whose per-column ranges still span the whole domain,
# the table still READS correctly and every query is still slow. The defect is invisible until
# someone measures it — which is exactly how the original 177s lookup went unnoticed.
#
# Run this against MinIO after healing and BEFORE sync-healed-tables-to-r2.sh. That sync deletes
# the superseded objects on R2, and until it runs R2 still holds the pre-heal files and is the
# only place they exist. This is the last point at which a bad heal is recoverable.
#
# ── WHAT IT CHECKS, PER TABLE ────────────────────────────────────────────────────────────────
#   1. Rows preserved — the current snapshot's total-records against its parent's. A rewrite
#      must not change the row count; anything else means data was lost or duplicated.
#   2. Sort order recorded — the aperio.sorted-by property. Absent means the heal did not
#      finish, and it is also what makes a re-run a no-op.
#   3. Bound overlap — the payoff. Every data file carries a lower/upper bound for the leading
#      sort column in the manifest. Sorted, those ranges tile the key space and a point lookup
#      opens one or two files; unsorted, every range spans everything and a lookup opens them
#      all. Reported as worst-case and mean files touched, and FAILED above 80%.
#
# ── USAGE ────────────────────────────────────────────────────────────────────────────────────
#   govdata/scripts/parallel/verify-healed-tables.sh                      # all 13
#   govdata/scripts/parallel/verify-healed-tables.sh entity_org_bridge    # one table
#   govdata/scripts/parallel/verify-healed-tables.sh ref                  # one schema
#   govdata/scripts/parallel/verify-healed-tables.sh --list
#
# Read-only: it opens table metadata and manifests, and writes nothing.
# Exit 0 = every table checked passed. Non-zero = do NOT publish to R2.
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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# schema|table|leading sort column.
# Must stay in step with heal-sort-order.sh — the leading column is the one whose bounds decide
# whether a lookup can prune, so it is the one worth verifying.
TABLES=(
  "ref|entity_org_bridge|source_name_normalized"
  "ref|canonical_org_entity|lei"
  "ref|entity_person_bridge|source_a_name_raw"
  "ref|canonical_person_entity|canonical_name"
  "sec|filing_metadata|cik"
  "sec|filing_contexts|cik"
  "sec|mda_sections|cik"
  "sec|xbrl_relationships|cik"
  "sec|insider_transactions|cik"
  "sec|institutional_holdings|cik"
  "sec|beneficial_ownership|subject_cik"
  "sec|earnings_transcripts|cik"
  # sec.vectorized_chunks removed 2026-08-17 — retired write target, never healed.
  # See the note in heal-sort-order.sh.
  "sec|financial_line_items|cik"
)

FILTER=""
for arg in "$@"; do
  case "$arg" in
    --list)
      printf '%-6s %-26s %s\n' "SCHEMA" "TABLE" "LEADING SORT COLUMN"
      for entry in "${TABLES[@]}"; do
        IFS='|' read -r sc tb col <<< "$entry"
        printf '%-6s %-26s %s\n' "$sc" "$tb" "$col"
      done
      exit 0
      ;;
    --*) echo "ERROR: unknown option $arg" >&2; exit 1 ;;
    *) FILTER="$arg" ;;
  esac
done

if [ -z "${GOVDATA_PARQUET_DIR:-}" ]; then
  echo "ERROR: GOVDATA_PARQUET_DIR is not set — check .env.prod" >&2
  exit 1
fi

JAR="$(resolve_classpath)"
# s3:// is the config convention; the Hadoop catalog wants s3a://.
WAREHOUSE_BASE="${GOVDATA_PARQUET_DIR/#s3:\/\//s3a://}"

S3_ARGS=""
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  S3_ARGS="--s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY"
  if [ -n "${AWS_ENDPOINT_OVERRIDE:-}" ]; then
    S3_ARGS="$S3_ARGS --s3-endpoint $AWS_ENDPOINT_OVERRIDE"
  fi
fi

echo "============================================================"
echo "Verify healed tables on MinIO"
echo "============================================================"
echo "  Warehouse: $WAREHOUSE_BASE"
echo "  Jar:       $JAR"
[ -n "$FILTER" ] && echo "  Filter:    $FILTER"
echo "  Read-only — nothing is modified."
echo ""

total=0; passed=0; failed=0; errored=0
FAILED_TABLES=()

for entry in "${TABLES[@]}"; do
  IFS='|' read -r schema table sort_col <<< "$entry"

  if [ -n "$FILTER" ] && [ "$table" != "$FILTER" ] && [ "$schema" != "$FILTER" ]; then
    continue
  fi

  total=$((total + 1))
  echo ">>> ${schema}.${table}"

  set +e
  # shellcheck disable=SC2086
  java -Xms256m -Xmx2g \
    -cp "$JAR" \
    org.apache.calcite.adapter.govdata.etl.IcebergSortVerifier \
    --warehouse "$WAREHOUSE_BASE/$schema" \
    --table "$table" \
    --sort-column "$sort_col" \
    $S3_ARGS \
    2>&1 | grep -vE "^log4j:|WARN.*hadoop|WARN.*Metrics|^$" | sed 's/^/    /'
  rc=${PIPESTATUS[0]}
  set -e

  case $rc in
    0) passed=$((passed + 1)) ;;
    1) failed=$((failed + 1)); FAILED_TABLES+=("${schema}.${table} (verification failed)") ;;
    # rc=2 is an error opening the table (missing, credentials, endpoint) rather than a verdict
    # on the data. Kept distinct so "could not check" is never read as "checked and fine".
    *) errored=$((errored + 1)); FAILED_TABLES+=("${schema}.${table} (ERROR rc=$rc)") ;;
  esac
  echo ""
done

echo "============================================================"
echo "  Tables checked: $total"
echo "  Passed:         $passed"
echo "  Failed:         $failed"
echo "  Errored:        $errored"
if [ ${#FAILED_TABLES[@]} -gt 0 ]; then
  printf '    - %s\n' "${FAILED_TABLES[@]}"
fi
echo "============================================================"

if [ $failed -eq 0 ] && [ $errored -eq 0 ]; then
  echo ""
  echo "All checked tables verified. Safe to publish:"
  echo "  $SCRIPT_DIR/sync-healed-tables-to-r2.sh --confirm"
else
  echo ""
  echo "DO NOT PUBLISH. R2 still holds the pre-heal files and is the only remaining copy;"
  echo "sync-healed-tables-to-r2.sh would delete them."
fi

[ $failed -eq 0 ] && [ $errored -eq 0 ]
