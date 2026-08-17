#!/usr/bin/env bash
# heal-sort-order.sh — one-off: rewrite whole partitions so their rows are sorted.
#
# ── WHY THIS EXISTS ──────────────────────────────────────────────────────────────────────────
# Iceberg keeps per-file lower/upper bounds in its manifests and Parquet keeps min/max per row
# group, so the reader CAN skip files and row groups without opening them. On unsorted data it
# never does: rows arrive in ingest order, so every file's range for any given column spans the
# whole domain, every file overlaps every predicate, and an equality lookup reads the lot.
# Measured on ref.entity_org_bridge (1.07M rows, unpartitioned): an equality probe on
# source_name_normalized took 177 seconds.
#
# Compaction alone does not fix it. It bin-packs by SIZE — it only inspects files below the
# small-file threshold and merges them toward the target size, preserving whatever order they
# already had. And a table that is already packed into large files (or rebuilt in full each run,
# as the entity tables are) has no small files at all, so a size-based pass never revisits it.
#
# Sorting only newly-appended files does not fix it either: their ranges still overlap the
# untouched large files, so the reader still cannot prune. Ordering WITHIN a partition requires
# rewriting the whole partition, which is what --heal-sort does.
#
# ── WHAT IT DOES ─────────────────────────────────────────────────────────────────────────────
# For each table below, runs IcebergMaintenanceRunner --heal-sort against the MinIO warehouse.
# The runner reads every file in each partition, sorts the rows, and commits a RewriteFiles that
# replaces the old files with new sorted ones. Partitions within the sort memory budget sort in
# memory; larger ones use an external merge that spills sorted runs to local temp files and
# merges them holding one record per run, so peak heap is bounded by one input file rather than
# by partition size.
#
# ── IDEMPOTENT ───────────────────────────────────────────────────────────────────────────────
# Each table records the order it was rewritten in as the Iceberg table property
# `aperio.sorted-by`. A table already carrying the requested order is skipped, so re-running this
# script is cheap and safe. Changing a table's declared sortOrder makes it eligible again.
#
# ── COST — READ BEFORE RUNNING ───────────────────────────────────────────────────────────────
#   * Every file in every partition is rewritten, so every file gets a NEW modification time.
#     sync-to-r2.sh selects by modtime, so the next publish re-uploads the ENTIRE healed table
#     to R2 — not a delta. Small for the entity tables; substantial for the SEC set
#     (sec.financial_line_items is ~40M rows).
#   * sync-to-r2.sh uses `rclone copy`, which never deletes, so the superseded files stay on R2
#     until `prune-r2.sh --confirm` reclaims them. Expect R2 usage to roughly double for a
#     healed table in between.
#   * Correctness across the publish is safe by construction: data files are written before the
#     commit, so their modtimes precede the metadata's, and sync-to-r2.sh copies oldest→newest —
#     R2 receives the data before the metadata that references it.
#
# ── ORDER OF OPERATIONS ──────────────────────────────────────────────────────────────────────
#   1. This script (against MinIO).
#   2. Verify the healed tables read correctly on MinIO.
#   3. Let sync-to-r2.sh publish (it runs continuously from run-scheduled.sh), or run it manually.
#   4. Only then prune-r2.sh --confirm, to reclaim the superseded objects. Until you prune, R2
#      still holds the pre-heal files and is a recovery source.
#
# ── USAGE ────────────────────────────────────────────────────────────────────────────────────
#   govdata/scripts/parallel/heal-sort-order.sh                 # DRY RUN — prints the plan
#   govdata/scripts/parallel/heal-sort-order.sh --confirm       # actually rewrite
#   govdata/scripts/parallel/heal-sort-order.sh --confirm entity_org_bridge   # one table
#   govdata/scripts/parallel/heal-sort-order.sh --list          # show the table list and exit
#
# Env:
#   GOVDATA_HEAP        JVM max heap for the rewrite (default 4g)
#   GOVDATA_SORT_BUDGET Bytes above which a partition uses the external merge instead of an
#                       in-memory sort (default 512MB, i.e. the runner's own default). Lower it
#                       on a memory-constrained box to force spilling sooner.
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

# schema|table|sort columns
#
# Only tables that actually benefit are listed. A survey of all 361 tables found just 16
# unpartitioned, 12 of which are tiny reference dimensions that scan instantly — the four entity
# tables are the whole population that is both unpartitioned and large enough for sort to matter.
# The SEC tables are partitioned by year but looked up by cik, a NON-partition column, so their
# per-year partitions have the same problem inside each partition.
#
# These orders mirror the `sortOrder:` declared in the schema YAMLs. Keep the two in step: the
# YAML drives ongoing compaction, this script drives the one-off backfill of existing data.
TABLES=(
  # Entity resolution — unpartitioned, and what resolve_entity searches. Cheapest and highest
  # value; run these first and measure before touching SEC.
  "ref|entity_org_bridge|source_name_normalized,lei"
  "ref|canonical_org_entity|lei,canonical_name"
  "ref|entity_person_bridge|source_a_name_raw,source_b_name_raw"
  "ref|canonical_person_entity|canonical_name,canonical_entity_id"

  # SEC — partitioned by year, looked up by cik. Large: expect a full re-upload to R2 per table.
  "sec|filing_metadata|cik,accession_number"
  "sec|filing_contexts|cik,accession_number,context_id"
  "sec|mda_sections|cik,accession_number,section,paragraph_number"
  "sec|xbrl_relationships|cik,accession_number,linkbase_type,from_concept"
  "sec|insider_transactions|cik,transaction_date,reporting_person_cik"
  "sec|institutional_holdings|cik,report_period,cusip"
  "sec|beneficial_ownership|subject_cik,filing_date,filer_cik"
  "sec|earnings_transcripts|cik,accession_number,section_type,paragraph_number"
  # sec.vectorized_chunks is deliberately NOT here. It still declares a sortOrder in
  # sec-schema.yaml, but that block also carries `materialize.enabled: false`: the sec-side
  # Iceberg location is retired as a live write target and rows now land in
  # ref.vectorized_chunks (SecSchemaFactory#materializeSecChunksToRef). Healing it would sort
  # frozen pre-migration data that nothing writes to and queries no longer read through this
  # path. It failed in 3s on every pass for exactly that reason. Removed 2026-08-17.
  # Biggest table in the warehouse (~40M rows). Deliberately last.
  "sec|financial_line_items|cik,accession_number,concept"
)

CONFIRM=""
FILTER=""
for arg in "$@"; do
  case "$arg" in
    --confirm) CONFIRM="yes" ;;
    --list)
      printf '%-6s %-26s %s\n' "SCHEMA" "TABLE" "SORT ORDER"
      for entry in "${TABLES[@]}"; do
        IFS='|' read -r sc tb so <<< "$entry"
        printf '%-6s %-26s %s\n' "$sc" "$tb" "$so"
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
# Guard against healing straight into R2. This is a MinIO-source operation; R2 is the published
# mirror and is reconciled by sync-to-r2.sh / prune-r2.sh, not written to directly.
case "$GOVDATA_PARQUET_DIR" in
  *r2*|*cloudflarestorage*)
    echo "ERROR: GOVDATA_PARQUET_DIR looks like R2 ($GOVDATA_PARQUET_DIR)." >&2
    echo "       Heal rewrites the SOURCE warehouse (MinIO). Publish with sync-to-r2.sh." >&2
    exit 1
    ;;
esac

JAR="$(resolve_classpath)"
HEAP="${GOVDATA_HEAP:-4g}"
BUDGET_ARG=""
if [ -n "${GOVDATA_SORT_BUDGET:-}" ]; then
  BUDGET_ARG="-Dcalcite.iceberg.sort.memory.budget.bytes=$GOVDATA_SORT_BUDGET"
fi

S3_ARGS=""
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  S3_ARGS="--s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY"
  if [ -n "${AWS_ENDPOINT_OVERRIDE:-}" ]; then
    S3_ARGS="$S3_ARGS --s3-endpoint $AWS_ENDPOINT_OVERRIDE"
  fi
fi

# s3:// is the config convention; the Hadoop catalog wants s3a://.
WAREHOUSE_BASE="${GOVDATA_PARQUET_DIR/#s3:\/\//s3a://}"

echo "============================================================"
echo "Heal sort order"
echo "============================================================"
echo "  Warehouse: $WAREHOUSE_BASE"
echo "  Jar:       $JAR"
echo "  Heap:      $HEAP"
[ -n "$BUDGET_ARG" ] && echo "  Sort budget override: ${GOVDATA_SORT_BUDGET} bytes"
[ -n "$FILTER" ] && echo "  Filter:    $FILTER"
if [ -z "$CONFIRM" ]; then
  echo "  Mode:      DRY RUN (pass --confirm to rewrite)"
else
  echo "  Mode:      REWRITING — every file in every partition"
fi
echo ""

total=0; healed=0; skipped=0; failed=0
FAILED_TABLES=()

for entry in "${TABLES[@]}"; do
  IFS='|' read -r schema table sort_order <<< "$entry"

  if [ -n "$FILTER" ] && [ "$table" != "$FILTER" ] && [ "$schema" != "$FILTER" ]; then
    continue
  fi

  total=$((total + 1))
  echo ">>> $schema.$table  [$sort_order]"

  DRY_ARG=""
  [ -z "$CONFIRM" ] && DRY_ARG="--dry-run"

  set +e
  # shellcheck disable=SC2086
  java -Xms512m -Xmx"$HEAP" $BUDGET_ARG \
    -cp "$JAR" \
    org.apache.calcite.adapter.govdata.etl.IcebergMaintenanceRunner \
    --warehouse "$WAREHOUSE_BASE/$schema" \
    --table "$table" \
    --sort-order "$sort_order" \
    --heal-sort \
    --target-size-mb 128 \
    --expire-days 7 \
    $S3_ARGS \
    $DRY_ARG \
    2>&1 | grep -vE "^log4j:|WARN.*hadoop|WARN.*Metrics|^$"
  rc=${PIPESTATUS[0]}
  set -e

  if [ $rc -eq 0 ]; then
    healed=$((healed + 1))
  else
    failed=$((failed + 1))
    FAILED_TABLES+=("$schema.$table")
    # Keep going: each table is an independent rewrite, and one failure should not strand the
    # rest. Every failure is listed again in the summary so it cannot scroll past unnoticed.
    echo "    FAILED (rc=$rc) — continuing"
  fi
  echo ""
done

echo "============================================================"
echo "  Tables considered: $total"
echo "  Completed:         $healed"
echo "  Failed:            $failed"
if [ ${#FAILED_TABLES[@]} -gt 0 ]; then
  printf '    - %s\n' "${FAILED_TABLES[@]}"
fi
echo "============================================================"

if [ -z "$CONFIRM" ]; then
  echo ""
  echo "DRY RUN — nothing was rewritten. Re-run with --confirm."
else
  echo ""
  echo "Next:"
  echo "  1. Verify the healed tables read correctly on MinIO."
  echo "  2. Publish:  $SCRIPT_DIR/sync-to-r2.sh          (re-uploads each healed table in full)"
  echo "  3. Reclaim:  $SCRIPT_DIR/prune-r2.sh --confirm  (deletes the superseded R2 objects)"
  echo "     Do NOT prune until step 1 passes — until then R2 still holds the pre-heal files."
fi

[ $failed -eq 0 ]
