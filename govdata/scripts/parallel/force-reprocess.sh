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
# SEC: run-pool.sh splits it into performance-partitioning pool slots (sec_primary: 10-K/10-Q,
# sec_secondary: 8-K/proxy/insider/13D-G, sec_13f: 13F-HR, sec_prices: Stooq prices) so historical
# backfills parallelize. A table fix scoped to one of those slots (e.g. sec_primary for a bug
# confirmed to affect only 10-K/10-Q accessions) is fine and often the more precise choice — this
# script does not require --schema sec (bare) for that case. bare `sec` (no filingTypes filter)
# remains the right choice when a fix isn't known to be confined to one form type — and it must
# be used (or every relevant partitioned slot run together) whenever the reason for reprocessing
# applies to the table as a whole rather than one form type's data: a new/changed column, a bug
# in shared transform/materialization logic, or anything not proven form-type-specific. Scoping
# that kind of fix to just sec_primary silently leaves sec_secondary's and sec_13f's rows never
# touched — same missing-data outcome as under-covering after a purge (see below), just reached
# without ever purging anything. The litmus test: is the fix/change itself confined to one form
# type, or does it apply table-wide? Only the former justifies a partitioned slot alone.
#
# data_purge.sh should not be needed here at all. A forced reprocess already corrects existing
# wrong rows in place — IcebergTableWriter's row-level delete-then-rewrite (wired via the
# forceAccessions operand) targets exactly the rows a fix needs to touch and overwrites them,
# without wiping anything else first. Reach for data_purge.sh only for the genuine edge case a
# targeted rewrite can't handle — e.g. the table's Iceberg metadata itself is corrupted, a
# structural/partition-key change requires a full rebuild, or the defect's scope is truly
# unknown and can't be enumerated as a target list — not as a routine step before reprocessing.
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
FORCE_DOWNLOAD=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema)          SCHEMA="$2";     shift 2 ;;
    --tables)          TABLES="$2";     shift 2 ;;
    --start)           START_YEAR="$2"; shift 2 ;;
    --end)             END_YEAR="$2";   shift 2 ;;
    --skip-historical) SKIP_HISTORICAL=true; shift ;;
    --skip-daily)      SKIP_DAILY=true; shift ;;
    --dry-run)         DRY_RUN=true;    shift ;;
    --force-download)  FORCE_DOWNLOAD=true; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$SCHEMA" || -z "$TABLES" ]]; then
  cat <<EOF >&2
Usage: $0 --schema <schema> --tables <t1,t2,...> [--start YYYY] [--end YYYY] [--skip-historical] [--skip-daily] [--dry-run] [--force-download]

  --force-download   Makes the target table(s) treat their raw HTTP cache as always-miss for
                      this run (GOVDATA_FORCE_DOWNLOAD_TABLES), so every dimension combo is
                      live-fetched and the stale cached response gets overwritten — no files are
                      deleted. GOVDATA_FORCE_REPROCESS_TABLES alone only bypasses the table-level
                      tracker check — HttpSource still reads an existing cached response instead
                      of re-fetching, so a fix to how the response is FETCHED OR PARSED (e.g. a
                      bug that mishandled an API error shape and cached the bad result) needs
                      this flag to actually take effect. A fix that only changes materialization/
                      transform logic downstream of the cache does not need it — the cached raw
                      response was fine, only what was done with it was wrong. Determine which
                      case applies from the nature of the bug being fixed, not by default.

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

# SecSchemaFactory#getCiksFromConfig returns an EMPTY list — not "all CIKs" — when the operand
# has no "ciks" entry, and worker.sh's bare `sec`/`sec_prices` cases only add one when
# GOVDATA_CIKS is set. Left unset, the ETL run enumerates zero companies and exits clean in
# seconds: no error, no warning, table-level as_of can even still advance — a silent no-op that
# looks exactly like a successful reprocess (confirmed live: an unset-CIKS run against
# beneficial_ownership finished in 30s with no fetch activity at all). Refuse rather than risk
# repeating that silently.
#
# sec_primary/sec_secondary/sec_13f don't hit the empty-list case — worker.sh's cases for them
# default to the 6-company DQ_SAMPLE (`${GOVDATA_CIKS:-DQ_SAMPLE}`) rather than omitting ciks
# entirely — but that's its own silent-wrong-scope trap: a caller expecting a full-universe
# remediation run gets a real, non-empty, plausible-looking result from just 6 companies instead.
# Refuse the same way rather than let that pass quietly.
if [[ ( "$SCHEMA" == "sec" || "$SCHEMA" == "sec_prices" || "$SCHEMA" == "sec_primary" \
       || "$SCHEMA" == "sec_secondary" || "$SCHEMA" == "sec_13f" ) \
      && -z "${GOVDATA_CIKS:-}" ]]; then
  echo "ERROR: GOVDATA_CIKS is not set." >&2
  if [[ "$SCHEMA" == "sec" || "$SCHEMA" == "sec_prices" ]]; then
    echo "       For '$SCHEMA', an unset ciks operand resolves to an EMPTY company list — not" >&2
    echo "       'all companies' — so the ETL run would silently process nothing and still" >&2
    echo "       look successful." >&2
  else
    echo "       For '$SCHEMA', an unset ciks operand silently defaults to the 6-company" >&2
    echo "       DQ_SAMPLE set instead of a real full-universe run — a plausible-looking but" >&2
    echo "       badly under-scoped result, not an error." >&2
  fi
  echo "       Export GOVDATA_CIKS explicitly:" >&2
  echo "         GOVDATA_CIKS=_ALL_EDGAR_FILERS   for a real full-universe reprocess" >&2
  echo "         GOVDATA_CIKS=DQ_SAMPLE           for a small, fast correctness check" >&2
  echo "         GOVDATA_CIKS=<CIK,CIK,...>       for specific companies" >&2
  exit 1
fi

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

# ── SEC document tables have a SEPARATE per-accession tracker layer (phase='staging',
#    table_name=<suffix>) that SecFilingCache/DocumentETLProcessor consult independently of the
#    table-level phase='incremental' row GOVDATA_FORCE_REPROCESS_TABLES bypasses. Forcing the
#    table-level check alone does NOT make SEC accessions re-fetch — SecFilingCache still sees
#    them as staging-complete and skips them regardless of the force flag, so the table-level
#    as_of can advance (looking like a successful reprocess) while zero accessions actually
#    change. Purge the matching per-accession suffix(es) first so this force is real, not cosmetic.
#
#    MUST be year-scoped to exactly the range this invocation is about to run (computed above) —
#    an unscoped purge silently deletes per-accession completions for every OTHER year that
#    table has ever had too, corrupting already-correct years' tracker state and forcing them to
#    needlessly re-fetch on their next touch. Confirmed live 2026-08-17: an unscoped call from an
#    earlier version of this script deleted 5,957,362 rows spanning 2010-2026 from a single
#    2018-only request.
if [[ "$SCHEMA" == "sec" || "$SCHEMA" == "sec_prices" || "$SCHEMA" == "sec_primary" \
     || "$SCHEMA" == "sec_secondary" || "$SCHEMA" == "sec_13f" ]]; then
  ACCESSION_SUFFIXES=()
  while IFS= read -r _s; do
    [[ -n "$_s" ]] && ACCESSION_SUFFIXES+=("$_s")
  done < <(sec_accession_suffixes_for_tables "${TABLE_ARR[@]}")
  if [[ ${#ACCESSION_SUFFIXES[@]} -gt 0 ]]; then
    if ! $SKIP_HISTORICAL; then
      PURGE_YEAR_RANGE="${START_YEAR},${END_YEAR}"
    elif ! $SKIP_DAILY; then
      PURGE_YEAR_RANGE="$(date +%Y),$(date +%Y)"
    else
      PURGE_YEAR_RANGE=""
    fi
    IN_LIST=""
    for _s in "${ACCESSION_SUFFIXES[@]}"; do IN_LIST="${IN_LIST:+$IN_LIST,}'$_s'"; done
    if [[ -n "$PURGE_YEAR_RANGE" ]]; then
      echo "── Purging per-accession completions (table_name IN (${IN_LIST}), years ${PURGE_YEAR_RANGE}) —" >&2
      echo "   required for these tables to actually re-fetch, not just advance their table-level marker ──" >&2
      pg_tracker_purge_accessions "$PG_NS" "$IN_LIST" "$($DRY_RUN && echo true || echo false)" "$PURGE_YEAR_RANGE" || exit 2
      echo ""
    fi
  fi
fi

# ── --force-download: make HttpSource treat the target table(s)' raw cache as always-miss for
#    this run, instead of deleting the cached files. rawCache is immutable by design (see the
#    govdata-data-cadence skill) — once a response.json is written, it is read forever and NEVER
#    re-fetched on its own. If a bug lives in how the response was fetched or parsed (not just
#    in downstream materialization), GOVDATA_FORCE_REPROCESS_TABLES alone just re-materializes
#    the SAME bad cached response — confirmed live 2026-08-18: force-reprocessing
#    econ.regional_income for 2025 after fixing BeaResponseTransformer's error-shape handling
#    produced "0 unprocessed of 1 total (100% cached)" and zero row-count change.
#
#    FIRST implementation of this flag deleted the raw cache via rclone (superseded — see git
#    history): correctness required either an unreliable glob (rclone's --include matched every
#    year's directory, not just the target one) or purging a table's entire cache subtree
#    wholesale, and deleting thousands of small per-dimension response.json files one-by-one on
#    MinIO was also just slow (30+ min for one table-year in practice). This is simpler, faster,
#    and exactly scoped: GOVDATA_FORCE_DOWNLOAD_TABLES (HttpSource.bypassRawCache, wired in
#    EtlPipeline.isTableForceDownloaded, same shape as GOVDATA_FORCE_REPROCESS_TABLES) makes
#    hasValidRawCache() report a miss unconditionally for these tables, forcing a live fetch —
#    whose normal cache-write path then overwrites the stale entry (S3/MinIO PUT semantics
#    replace an existing key, no delete needed) with the corrected response.
if $FORCE_DOWNLOAD; then
  export GOVDATA_FORCE_DOWNLOAD_TABLES="$TABLES"
  echo "── force-download: GOVDATA_FORCE_DOWNLOAD_TABLES=${TABLES} — raw cache will be bypassed, not deleted ──"
  echo ""
fi

# GOVDATA_FORCE_REPROCESS_TABLES alone only bypasses the tracker skip-check for the named
# table(s) — it does NOT scope which tables the model loads. That's GOVDATA_TABLES, read by
# build_inline_model()/filter_enabled_tables() to set enabledTables. Exporting both keeps the
# model's enabledTables matching --tables exactly, so a run only ever touches the table(s)
# actually requested, per this script's own contract ("only rerun the affected table(s)").
export GOVDATA_FORCE_REPROCESS_TABLES="$TABLES"
export GOVDATA_TABLES="$TABLES"
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
