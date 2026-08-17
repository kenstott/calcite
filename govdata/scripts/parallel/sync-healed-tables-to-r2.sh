#!/usr/bin/env bash
# sync-healed-tables-to-r2.sh — publish just the sort-order-healed tables from MinIO to R2.
#
# ── WHY A SEPARATE SCRIPT ────────────────────────────────────────────────────────────────────
# The routine publisher, sync-to-r2.sh, is built on an assumption heal breaks. Its own header
# states it: "Since Iceberg is append-only (new snapshots only add files, never modify existing
# ones), bounding the source by modification time selects exactly the new parquet data +
# metadata files." It therefore uses `rclone copy`, which NEVER deletes at the destination.
#
# heal-sort-order.sh is not append-only. It commits a RewriteFiles: the old data files are
# removed from the table and new sorted ones take their place. Publishing that with `copy`
# leaves every superseded pre-heal file on R2 forever — correct to read, because the current
# metadata points only at the new files, but roughly double the storage per healed table until
# something reclaims it.
#
# This script uses `rclone sync` scoped to ONE TABLE PREFIX at a time, so R2 ends up an exact
# mirror of MinIO for that table: new sorted files uploaded, superseded ones deleted, in a
# single pass. That is the right tool for a known, freshly-verified table, and it avoids running
# the repo-wide prune-r2.sh — which reconciles the ENTIRE bucket and would also mirror any
# unrelated source deletion made by mistake.
#
# ── DESTRUCTIVE — READ BEFORE RUNNING ────────────────────────────────────────────────────────
#   * `rclone sync` DELETES objects on R2 that are absent from MinIO, within the table prefix.
#     Scoped per table, but still deletion.
#   * Until you run this, R2 still holds the pre-heal files and is the only place they exist —
#     it is a recovery source. Verify the healed table reads correctly on MinIO FIRST.
#   * Dry run by default. Nothing is transferred or deleted without --confirm.
#   * Unlike sync-to-r2.sh, this must LIST the R2 prefix to know what to delete, which costs
#     Class A/B operations. Bounded by table, not by lake size.
#
# ── USAGE ────────────────────────────────────────────────────────────────────────────────────
#   govdata/scripts/parallel/sync-healed-tables-to-r2.sh                    # DRY RUN, all 14
#   govdata/scripts/parallel/sync-healed-tables-to-r2.sh --confirm          # publish all 14
#   govdata/scripts/parallel/sync-healed-tables-to-r2.sh --confirm ref      # one schema
#   govdata/scripts/parallel/sync-healed-tables-to-r2.sh --confirm entity_org_bridge
#   govdata/scripts/parallel/sync-healed-tables-to-r2.sh --list
#
# Env:
#   GOVDATA_RCLONE_REMOTE  MinIO rclone remote name (default: minio)
#   GOVDATA_R2_TRANSFERS   parallel transfers (default 16, matching sync-to-r2.sh)
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

# Must match the table list in heal-sort-order.sh. Kept as schema|table so the prefix built
# below is exactly the table's directory and the sync can never widen past it.
TABLES=(
  "ref|entity_org_bridge"
  "ref|canonical_org_entity"
  "ref|entity_person_bridge"
  "ref|canonical_person_entity"
  "sec|filing_metadata"
  "sec|filing_contexts"
  "sec|mda_sections"
  "sec|xbrl_relationships"
  "sec|insider_transactions"
  "sec|institutional_holdings"
  "sec|beneficial_ownership"
  "sec|earnings_transcripts"
  "sec|vectorized_chunks"
  "sec|financial_line_items"
)

BUCKET="govdata-parquet-v1"
MINIO_REMOTE="${GOVDATA_RCLONE_REMOTE:-minio}"
R2_REMOTE="r2"
TRANSFERS="${GOVDATA_R2_TRANSFERS:-16}"

CONFIRM=""
FILTER=""
for arg in "$@"; do
  case "$arg" in
    --confirm) CONFIRM="yes" ;;
    --list)
      printf '%-6s %s\n' "SCHEMA" "TABLE"
      for entry in "${TABLES[@]}"; do
        IFS='|' read -r sc tb <<< "$entry"
        printf '%-6s %s\n' "$sc" "$tb"
      done
      exit 0
      ;;
    --*) echo "ERROR: unknown option $arg" >&2; exit 1 ;;
    *) FILTER="$arg" ;;
  esac
done

# Fails loudly if any publish credential is missing, rather than silently skipping.
configure_r2_remote

echo "============================================================"
echo "Sync healed tables: MinIO -> R2"
echo "============================================================"
echo "  Source:    ${MINIO_REMOTE}:${BUCKET}"
echo "  Dest:      ${R2_REMOTE}:${BUCKET}"
echo "  Transfers: $TRANSFERS"
[ -n "$FILTER" ] && echo "  Filter:    $FILTER"
if [ -z "$CONFIRM" ]; then
  echo "  Mode:      DRY RUN (pass --confirm to transfer and delete)"
else
  echo "  Mode:      LIVE — uploads new files AND deletes superseded ones on R2"
fi
echo ""

DRY_FLAG=""
[ -z "$CONFIRM" ] && DRY_FLAG="--dry-run"

total=0; ok=0; failed=0
FAILED_TABLES=()

for entry in "${TABLES[@]}"; do
  IFS='|' read -r schema table <<< "$entry"

  if [ -n "$FILTER" ] && [ "$table" != "$FILTER" ] && [ "$schema" != "$FILTER" ]; then
    continue
  fi

  SRC="${MINIO_REMOTE}:${BUCKET}/${schema}/${table}"
  DST="${R2_REMOTE}:${BUCKET}/${schema}/${table}"
  total=$((total + 1))
  echo ">>> ${schema}.${table}"

  # Skip a table that does not exist on the source. Syncing an empty source into a populated
  # destination would DELETE the whole table on R2 — the worst possible outcome of a typo or a
  # table that was never healed.
  if ! rclone lsf --max-depth 1 "$SRC" >/dev/null 2>&1; then
    echo "    SKIP — not found on source ($SRC)"
    echo ""
    continue
  fi
  src_count=$(rclone size --json "$SRC" 2>/dev/null | sed -n 's/.*"count":\([0-9]*\).*/\1/p')
  if [ -z "$src_count" ] || [ "$src_count" = "0" ]; then
    echo "    SKIP — source is empty; refusing to sync (would delete the table on R2)"
    echo ""
    continue
  fi
  echo "    source objects: $src_count"

  set +e
  rclone sync "$SRC" "$DST" \
    --transfers "$TRANSFERS" \
    --stats 60s --stats-one-line \
    $DRY_FLAG \
    2>&1 | sed 's/^/    /'
  rc=${PIPESTATUS[0]}
  set -e

  if [ $rc -eq 0 ]; then
    ok=$((ok + 1))
  else
    failed=$((failed + 1))
    FAILED_TABLES+=("${schema}.${table}")
    # Each table is an independent mirror; one failure should not strand the rest. Every failure
    # is repeated in the summary so it cannot scroll past.
    echo "    FAILED (rc=$rc) — continuing"
  fi
  echo ""
done

echo "============================================================"
echo "  Tables considered: $total"
echo "  Synced:            $ok"
echo "  Failed:            $failed"
if [ ${#FAILED_TABLES[@]} -gt 0 ]; then
  printf '    - %s\n' "${FAILED_TABLES[@]}"
fi
echo "============================================================"

if [ -z "$CONFIRM" ]; then
  echo ""
  echo "DRY RUN — nothing transferred or deleted. Re-run with --confirm."
else
  echo ""
  echo "R2 now mirrors MinIO for these tables. prune-r2.sh is NOT needed for them;"
  echo "the sync already removed their superseded objects."
fi

[ $failed -eq 0 ]
