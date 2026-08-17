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
# force-reprocess.sh — reprocess specific table(s) past their own tracker-complete state,
# using the pipeline's real self-healing path instead of manual tracker/Iceberg surgery.
#
# Background: data-fix.sh/data_purge.sh force a reprocess by deleting or invalidating tracker
# rows before re-running — surgery that is easy to get wrong (this session found two real bugs
# that way: data-fix.sh's clear touched a sentinel no per-combo skip check even reads, and a
# naive "clear every completed table" query reached into unrelated schemas because
# table_completion has no schema column). GOVDATA_FORCE_REPROCESS_TABLES instead makes
# EtlPipeline itself skip its normal tracker-based skip logic for the named table(s) — the exact
# same code path the pipeline already uses to self-heal a stale/missing-data table. No tracker
# row or Iceberg data is touched by this script; only rerun the affected table(s), so it is safe
# to invoke repeatedly and cheap to get wrong (worst case: some redundant reprocessing).
#
# Scoping:
#   Historical range defaults to [oldest year already marked complete for this schema in
#   historical-year-complete.sh, current year] — NOT the table's configured floor year. Years
#   older than that marker have never been processed by anything yet, so they need no force:
#   they get today's (already-fixed) code automatically on their first-ever pass. Override with
#   --start/--end if a schema's marker state doesn't reflect what you actually need reprocessed.
#
#   Daily is run in addition to (not instead of) historical, because daily and historical cover
#   non-overlapping windows for several schemas (e.g. econ's daily fetches only the current year;
#   officials has no historical mode at all) — fixing historical alone can leave the current/
#   most-recent period, which only daily's window reaches, still wrong. Skip with --skip-daily
#   for a schema where that doesn't apply (or that has no daily mode configured at all).
#
# SEC is special: run-pool.sh splits it into performance-partitioning pool slots (sec_primary:
# 10-K/10-Q, sec_secondary: 8-K/proxy/insider/13D-G, sec_13f: 13F-HR, sec_prices: Stooq prices)
# so historical backfills parallelize — but those are NOT what this script should target for a
# table fix. worker.sh's bare `sec` case applies no filingTypes filter by default, so a single
# `sec:<range>` run repopulates every document-derived table (filing_metadata,
# financial_line_items, filing_contexts, mda_sections, xbrl_relationships, insider_transactions,
# institutional_holdings, beneficial_ownership, earnings_transcripts, vectorized_chunks) across
# every form type in one pass — targeting one of the split slots instead would silently
# under-cover tables fed by multiple form types. Use --schema sec (bare) for all of the above;
# the one exception is stock_prices, which worker.sh's `sec` case hardcodes fetchStockPrices:false
# for — that table needs --schema sec_prices instead.
#
# Usage:
#   force-reprocess.sh --schema <schema> --tables <t1,t2,...> \
#       [--start YYYY] [--end YYYY] [--skip-historical] [--skip-daily] [--dry-run]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/common.sh"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/historical-year-complete.sh"
# shellcheck source=/dev/null
source "$(dirname "$SCRIPT_DIR")/tracker_pg.sh"
load_env

SCHEMA=""
TABLES=""
START_YEAR=""
END_YEAR=""
SKIP_HISTORICAL=false
SKIP_DAILY=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema)          SCHEMA="$2";     shift 2 ;;
    --tables)          TABLES="$2";     shift 2 ;;
    --start)           START_YEAR="$2"; shift 2 ;;
    --end)             END_YEAR="$2";   shift 2 ;;
    --skip-historical) SKIP_HISTORICAL=true; shift ;;
    --skip-daily)      SKIP_DAILY=true; shift ;;
    --dry-run)         DRY_RUN=true;    shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$SCHEMA" || -z "$TABLES" ]]; then
  cat <<EOF >&2
Usage: $0 --schema <schema> --tables <t1,t2,...> [--start YYYY] [--end YYYY] [--skip-historical] [--skip-daily] [--dry-run]

Examples:
  force-reprocess.sh --schema econ --tables industry_gdp
  force-reprocess.sh --schema census --tables cps_voting_supplement --start 1996
  force-reprocess.sh --schema officials --tables members
EOF
  exit 1
fi

IFS=',' read -ra TABLE_ARR <<< "$TABLES"
for i in "${!TABLE_ARR[@]}"; do
  TABLE_ARR[$i]="$(echo -n "${TABLE_ARR[$i]}" | xargs)"
done

case "$SCHEMA" in
  sec_primary|sec_secondary|sec_13f)
    echo "ERROR: '$SCHEMA' is a run-pool.sh performance-partitioning slot (one SEC form-type" >&2
    echo "       subset), not the right target for a table fix — most SEC tables are fed by" >&2
    echo "       accessions across multiple form types, so reprocessing only this slot would" >&2
    echo "       silently under-cover them. Use --schema sec (bare) instead; it applies no" >&2
    echo "       filingTypes filter and covers every SEC table except stock_prices, which" >&2
    echo "       needs --schema sec_prices. See this script's header comment for details." >&2
    exit 1
    ;;
esac

PG_NS="$(pg_ns_from_bucket "${GOVDATA_PARQUET_DIR:-s3://govdata-parquet-v1}")" || exit 2

echo "=================================================="
echo "force-reprocess.sh"
echo "=================================================="
echo "  Schema: $SCHEMA"
echo "  Tables: ${TABLE_ARR[*]}"
echo "  Tracker: $PG_NS (postgres)"
$DRY_RUN && echo "  *** DRY RUN — no ETL will actually run ***"
echo ""

# ── Baseline: capture each table's current max as_of, to prove afterward that a reprocess
#    actually touched it (not just that the pool exited 0 — see the marker bugs this replaces).
declare -A BEFORE_AS_OF
echo "── Baseline (before) ──"
for tbl in "${TABLE_ARR[@]}"; do
  ts=$(pg_tracker_exec "SELECT COALESCE(MAX(as_of),0) FROM \"${PG_NS}\".pipeline_tracker WHERE table_name = '${tbl}' AND phase = 'incremental' AND state = 'complete';" 2>/dev/null | tr -d '[:space:]')
  ts="${ts:-0}"
  BEFORE_AS_OF["$tbl"]="$ts"
  if [[ "$ts" -gt 0 ]]; then
    when=$(date -d "@$((ts/1000))" -u '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || echo "$ts")
    echo "  ${tbl}: last touched $when"
  else
    echo "  ${tbl}: no prior incremental-complete rows found"
  fi
done
echo ""

# ── SEC document tables have a SEPARATE per-accession tracker layer (phase='staging',
#    table_name=<suffix>) that SecFilingCache/DocumentETLProcessor consult independently of the
#    table-level phase='incremental' row GOVDATA_FORCE_REPROCESS_TABLES bypasses. Forcing the
#    table-level check alone does NOT make SEC accessions re-fetch — SecFilingCache still sees
#    them as staging-complete and skips them regardless of the force flag, so the table-level
#    as_of can advance (looking like a successful reprocess) while zero accessions actually
#    change. Purge the matching per-accession suffix(es) first so this force is real, not cosmetic.
if [[ "$SCHEMA" == "sec" || "$SCHEMA" == "sec_prices" ]]; then
  ACCESSION_SUFFIXES=()
  while IFS= read -r _s; do
    [[ -n "$_s" ]] && ACCESSION_SUFFIXES+=("$_s")
  done < <(sec_accession_suffixes_for_tables "${TABLE_ARR[@]}")
  if [[ ${#ACCESSION_SUFFIXES[@]} -gt 0 ]]; then
    IN_LIST=""
    for _s in "${ACCESSION_SUFFIXES[@]}"; do IN_LIST="${IN_LIST:+$IN_LIST,}'$_s'"; done
    echo "── Purging per-accession completions (table_name IN (${IN_LIST})) — required for these" >&2
    echo "   tables to actually re-fetch, not just advance their table-level marker ──" >&2
    pg_tracker_purge_accessions "$PG_NS" "$IN_LIST" "$($DRY_RUN && echo true || echo false)" || exit 2
    echo ""
  fi
fi

# ── Determine the historical range: [oldest existing year marker for this schema, end year] ──
if ! $SKIP_HISTORICAL; then
  if [[ -z "$START_YEAR" ]]; then
    # sec's historical alias enqueues sec_primary/sec_secondary/sec_13f (each with its own
    # marker file), never bare "sec" — so a plain ${SCHEMA}.done never exists for it. Union the
    # three split marker files instead; see the header comment on why bare `sec` is still the
    # right run-pool target for the reprocess itself.
    if [[ "$SCHEMA" == "sec" ]]; then
      HCY_DONE_FILES=("$(hcy_state_dir)/sec_primary.done" "$(hcy_state_dir)/sec_secondary.done" "$(hcy_state_dir)/sec_13f.done")
    else
      HCY_DONE_FILES=("$(hcy_state_dir)/${SCHEMA}.done")
    fi
    START_YEAR=""
    START_YEAR=$(cat "${HCY_DONE_FILES[@]}" 2>/dev/null | grep -E '^[0-9]{4}$' | sort -n | head -1 || true)
    if [[ -n "$START_YEAR" ]]; then
      echo "  Historical start year (oldest existing marker for ${SCHEMA}): ${START_YEAR}"
    elif [[ "$SCHEMA" == "lands" || "$SCHEMA" == "research" || "$SCHEMA" == "officials" ]]; then
      # Single-pass (:once) schemas never write a year-shaped marker line — hcy_enqueue's marker
      # is literally the token "once", and on a first-ever run the marker file may not exist at
      # all yet. Fall back to the same default floor worker.sh itself uses when GOVDATA_START_YEAR
      # is unset, rather than erroring on a schema that genuinely does support historical reprocessing.
      START_YEAR=2010
      echo "  Historical start year: 2010 (default floor — '${SCHEMA}' is a single-pass :once schema with no per-year marker)"
    else
      echo "  WARNING: no historical-year-complete markers found for '${SCHEMA}' — cannot infer" >&2
      echo "           a safe start year. Pass --start explicitly (or --skip-historical)." >&2
      exit 1
    fi
  fi
  if [[ -z "$END_YEAR" ]]; then
    END_YEAR=$(date +%Y)
  fi
  echo "  Historical range: ${START_YEAR}-${END_YEAR}"
fi
echo ""

export GOVDATA_FORCE_REPROCESS_TABLES="$TABLES"
RUN_POOL="$SCRIPT_DIR/run-pool.sh"

if ! $SKIP_HISTORICAL; then
  SLOT="${SCHEMA}:${START_YEAR}-${END_YEAR}"
  echo "── Historical: ${SLOT} (GOVDATA_FORCE_REPROCESS_TABLES=${TABLES}) ──"
  if $DRY_RUN; then
    echo "  [DRY RUN] Would run: $RUN_POOL $SLOT"
  else
    bash "$RUN_POOL" "$SLOT"
  fi
  echo ""
fi

if ! $SKIP_DAILY; then
  SLOT="${SCHEMA}:daily"
  echo "── Daily: ${SLOT} (GOVDATA_FORCE_REPROCESS_TABLES=${TABLES}) ──"
  if $DRY_RUN; then
    echo "  [DRY RUN] Would run: $RUN_POOL $SLOT"
  else
    bash "$RUN_POOL" "$SLOT"
  fi
  echo ""
fi

# ── Validate: each table's as_of must have moved forward, or the reprocess did not actually
#    touch it (the exact failure mode data-fix.sh's clear had — the pool exits 0 either way).
echo "=================================================="
echo "Validation"
echo "=================================================="
if $DRY_RUN; then
  echo "(skipped — dry run)"
  exit 0
fi

ERRORS=0
for tbl in "${TABLE_ARR[@]}"; do
  after=$(pg_tracker_exec "SELECT COALESCE(MAX(as_of),0) FROM \"${PG_NS}\".pipeline_tracker WHERE table_name = '${tbl}' AND phase = 'incremental' AND state = 'complete';" 2>/dev/null | tr -d '[:space:]')
  after="${after:-0}"
  before="${BEFORE_AS_OF[$tbl]:-0}"
  if [[ "$after" -gt "$before" ]]; then
    echo "  OK: ${tbl} — as_of advanced ($before -> $after)"
  else
    echo "  WARNING: ${tbl} — as_of did NOT advance (before=$before after=$after)."
    echo "           The reprocess ran but this table shows no confirmed new activity —"
    echo "           check the schema actually has this table configured, and that the"
    echo "           historical/daily range covers a period where this table has data."
    ERRORS=$((ERRORS+1))
  fi
done
echo ""
if [[ $ERRORS -gt 0 ]]; then
  echo "Completed with $ERRORS table(s) NOT confirmed reprocessed."
  exit 1
fi
echo "All ${#TABLE_ARR[@]} table(s) confirmed reprocessed."
