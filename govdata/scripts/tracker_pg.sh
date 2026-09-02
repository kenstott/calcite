#!/bin/bash
# Postgres tracker helpers shared by data_purge.sh, data_fix.sh, and parallel/data-fix.sh.
#
# The S3 tracker keeps its state as marker parquet under
# <tracker bucket>/<schema>/year=*/source_key=*/, so the scripts reach it with path globs and
# DuckDB rewrites. The Postgres tracker keeps the identical state as rows in pipeline_tracker /
# table_completion, so the same operations are plain DELETEs — but nothing in the scripts spoke
# SQL, which meant a pg deployment silently got the Iceberg table dropped with every accession
# still marked complete. These functions give both scripts the pg half.
#
# Mirrors PGPipelineTracker: the tables live in a schema derived from the parquet bucket name so
# dq and prod never mix, and markCleared is a DELETE (not a state change), which is why purge and
# fix both express themselves as DELETEs here.

# Derives the PG schema from a parquet bucket, matching PGPipelineTracker.sanitizeNamespace:
# strip the URI scheme, lowercase, collapse non-alphanumerics to _, trim leading/trailing _.
#   s3://govdata-parquet-v1 -> govdata_parquet_v1
pg_ns_from_bucket() {
  local raw="${1#*://}"
  raw="$(echo -n "$raw" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g; s/^_+|_+$//g')"
  if [[ -z "$raw" ]]; then
    echo "ERROR: cannot derive PG tracker schema from bucket '$1'" >&2
    return 1
  fi
  echo "$raw"
}

# Resolves the PG tracker schema for one govdata schema, given the parquet bucket.
#
# PGPipelineTracker derives its namespace from the `directory` operand the worker passes, NOT
# from the bucket — and worker-health.sh / worker-lands.sh pass "${GOVDATA_PARQUET_DIR}/health"
# and ".../lands", so their tracker rows live in govdata_parquet_v1_health /
# govdata_parquet_v1_lands while every other schema shares govdata_parquet_v1. A caller that
# derives the namespace from the bucket alone therefore reads an EMPTY schema for those two —
# which is how force-reprocess.sh's post-run check reported "as_of did NOT advance (before=0
# after=0)" for four health tables that had in fact just been reprocessed correctly.
#
# Resolution is by which namespace actually holds a pipeline_tracker, preferring the
# schema-suffixed one, so this tracks however the worker is configured instead of restating the
# worker's directory logic here. Neither present is an error, never a silent empty result.
pg_ns_for_schema() {
  local bucket="$1" schema="$2" base suffixed
  base="$(pg_ns_from_bucket "$bucket")" || return 1
  if [[ -z "$schema" ]]; then
    echo "$base"
    return 0
  fi
  suffixed="${base}_${schema}"
  local exists
  exists=$(pg_tracker_exec "SELECT to_regclass('\"${suffixed}\".pipeline_tracker') IS NOT NULL;" 2>/dev/null | tr -d '[:space:]')
  if [[ "$exists" == "t" ]]; then
    echo "$suffixed"
    return 0
  fi
  exists=$(pg_tracker_exec "SELECT to_regclass('\"${base}\".pipeline_tracker') IS NOT NULL;" 2>/dev/null | tr -d '[:space:]')
  if [[ "$exists" == "t" ]]; then
    echo "$base"
    return 0
  fi
  echo "ERROR: no pipeline_tracker found in PG schema '${suffixed}' or '${base}' for govdata schema '${schema}'" >&2
  return 1
}

# Runs SQL against the tracker database. Connection comes from the same variables the workers
# use, so the script and the ETL can never disagree about which tracker they are talking to.
pg_tracker_exec() {
  local sql="$1"
  local url="${GOVDATA_TRACKER_PG_URL:-${CALCITE_TRACKER_PG_URL:-}}"
  if [[ -z "$url" ]]; then
    echo "ERROR: tracker backend is pg but GOVDATA_TRACKER_PG_URL is unset" >&2
    return 1
  fi
  # jdbc:postgresql://host:port/db -> host, port, db
  local hostport="${url#*://}"
  local db="${hostport#*/}"
  hostport="${hostport%%/*}"
  local host="${hostport%%:*}"
  local port="${hostport##*:}"
  [[ "$port" == "$host" ]] && port=5432
  PGPASSWORD="${GOVDATA_TRACKER_PG_PASSWORD:-${CALCITE_TRACKER_PG_PASSWORD:-}}" \
    psql -h "$host" -p "$port" \
      -U "${GOVDATA_TRACKER_PG_USER:-${CALCITE_TRACKER_PG_USER:-govdata}}" \
      -d "$db" -v ON_ERROR_STOP=1 -At -c "$sql"
}

# Deletes every tracker row for one materialized table: the per-source_key markers and the
# completion record. Equivalent to the S3 path's "delete marker dirs where table_name=<table>"
# plus the _table_complete marker.
pg_tracker_purge_table() {
  local ns="$1" table="$2" dry_run="$3"
  local counts
  counts=$(pg_tracker_exec "
    SELECT (SELECT count(*) FROM \"${ns}\".pipeline_tracker WHERE table_name = '${table}')
        || '|'
        || (SELECT count(*) FROM \"${ns}\".table_completion WHERE pipeline_name = '${table}')") || return 1
  local markers="${counts%%|*}" completions="${counts##*|}"
  if [[ "$dry_run" == "true" ]]; then
    echo "        [DRY RUN] Would delete ${markers} marker row(s) and ${completions} completion row(s) from ${ns}"
    return 0
  fi
  pg_tracker_exec "
    BEGIN;
    DELETE FROM \"${ns}\".pipeline_tracker WHERE table_name = '${table}';
    DELETE FROM \"${ns}\".table_completion WHERE pipeline_name = '${table}';
    COMMIT;" > /dev/null || return 1
  echo "        Deleted ${markers} marker row(s) and ${completions} completion row(s) from ${ns}"
}

# Deletes the per-accession document completions for a document schema. The S3 path rewrites the
# marker parquet to exclude these table_names; here they are rows.
#
# @param in_list comma-separated, single-quoted table names, e.g. 'metadata','facts'
# @param year_range optional "START,END" (both years, inclusive) to scope the delete via
#   source_key's trailing "__year=YYYY" (format: "accession_number=<accession>__year=<year>",
#   confirmed against live pipeline_tracker rows). Omit (or pass "") for the original unscoped
#   whole-table-history behavior — that is data_purge.sh's correct, deliberate use (a full table
#   purge wants every year gone). force-reprocess.sh MUST pass this: without it, a single-year
#   reprocess call silently deletes per-accession completions for EVERY year that table has ever
#   had, not just the target year — confirmed live 2026-08-17, a single 2018-scoped
#   force-reprocess.sh call deleted 5,957,362 rows spanning 2010-2026, corrupting the "already
#   correctly processed" completion state for every other year and forcing them to needlessly
#   re-fetch on their next touch (Iceberg data itself is safe either way — accession-existence
#   dedup on write protects it — but tracker state for unrelated years should never be touched
#   by a request scoped to one).
pg_tracker_purge_accessions() {
  local ns="$1" in_list="$2" dry_run="$3" year_range="${4:-}"
  local year_clause=""
  if [[ -n "$year_range" ]]; then
    local y_start="${year_range%,*}" y_end="${year_range#*,}" y
    for (( y=y_start; y<=y_end; y++ )); do
      year_clause="${year_clause}${year_clause:+ OR }source_key LIKE '%__year=${y}'"
    done
    year_clause=" AND (${year_clause})"
  fi
  local n
  n=$(pg_tracker_exec "
    SELECT count(*) FROM \"${ns}\".pipeline_tracker
     WHERE phase = 'staging' AND table_name IN (${in_list})${year_clause}") || return 1
  if [[ "$dry_run" == "true" ]]; then
    echo "        [DRY RUN] Would delete ${n} accession completion row(s) from ${ns}${year_range:+ (years ${year_range})}"
    return 0
  fi
  pg_tracker_exec "
    DELETE FROM \"${ns}\".pipeline_tracker
     WHERE phase = 'staging' AND table_name IN (${in_list})${year_clause};" > /dev/null || return 1
  echo "        Deleted ${n} accession completion row(s) from ${ns}${year_range:+ (years ${year_range})}"
}

# Deletes the per-accession document completion(s) for ONE specific accession — narrower than
# pg_tracker_purge_accessions, which sweeps every accession for a table. Used when a caller wants
# to force reprocessing of a single filing without touching any other accession's completion
# state. source_key format: "accession_number=<accession>__year=<year>" (see
# pg_tracker_purge_accessions for where that was confirmed against live rows); year is optional —
# omit it to match the accession regardless of year (an accession belongs to exactly one year in
# practice, so this is a convenience, not a real ambiguity risk).
#
# @param in_list comma-separated, single-quoted table names (the per-accession suffixes, e.g.
#   'chunks' or 'chunks','mda')
pg_tracker_clear_accession() {
  local ns="$1" in_list="$2" accession="$3" dry_run="$4" year="${5:-}"
  local key_pattern="accession_number=${accession}__year=%"
  [[ -n "$year" ]] && key_pattern="accession_number=${accession}__year=${year}"
  local n
  n=$(pg_tracker_exec "
    SELECT count(*) FROM \"${ns}\".pipeline_tracker
     WHERE phase = 'staging' AND table_name IN (${in_list}) AND source_key LIKE '${key_pattern}'") || return 1
  if [[ "$dry_run" == "true" ]]; then
    echo "        [DRY RUN] Would delete ${n} accession completion row(s) for '${accession}' from ${ns}"
    return 0
  fi
  pg_tracker_exec "
    DELETE FROM \"${ns}\".pipeline_tracker
     WHERE phase = 'staging' AND table_name IN (${in_list}) AND source_key LIKE '${key_pattern}';" > /dev/null || return 1
  echo "        Deleted ${n} accession completion row(s) for '${accession}' from ${ns}"
}

# Maps a govdata table name to the per-accession completion suffix DocumentETLProcessor/
# SecFilingCache record it under (phase='staging', table_name=<suffix>) — a SEPARATE, lower-level
# tracker layer from the table's own phase='incremental' row that pg_tracker_clear_completion /
# EtlPipeline's forceReprocessAll operate on. A table-level clear alone does NOT make SEC
# accessions re-fetch: SecFilingCache's own filterUnprocessed/checkFiling still sees them as
# staging-complete regardless, so any SEC document-derived table fix needs BOTH layers purged —
# this map is what tells a caller which per-accession suffix(es) a table needs purged too.
# Source of truth: FormType.getExpectedOutputs() (which output types the ETL actually produces
# per form) — kept in sync by hand here since bash can't introspect the Java enum.
declare -gA SEC_TABLE_ACCESSION_SUFFIX=(
  [mda_sections]=mda [xbrl_relationships]=relationships [financial_line_items]=facts
  [filing_contexts]=contexts [filing_metadata]=metadata [vectorized_chunks]=chunks
  [insider_transactions]=insider [earnings_transcripts]=earnings
  [institutional_holdings]=13f [beneficial_ownership]=13dg
)

# Given a list of table names, echoes the matching per-accession suffixes (space-separated,
# deduplicated implicitly by the caller's usage — duplicates are harmless in a SQL IN-list).
# Tables with no entry (e.g. stock_prices, or a non-SEC schema's table) are silently skipped —
# not every table has document-level tracking, so an empty result is a normal, valid answer.
sec_accession_suffixes_for_tables() {
  local suffix
  for _t in "$@"; do
    suffix="${SEC_TABLE_ACCESSION_SUFFIX[$_t]:-}"
    [[ -n "$suffix" ]] && echo "$suffix"
  done
}

# Invalidates one table's completion the way data_fix.sh does on S3: there the script appends a
# marker row with state='cleared'; here the same two facts are the pipeline_tracker row and the
# separate table_completion record PGPipelineTracker keeps, so both are written.
pg_tracker_clear_completion() {
  local ns="$1" table="$2" dry_run="$3"
  if [[ "$dry_run" == "true" ]]; then
    echo "  [DRY RUN] Would clear completion for '${table}' in ${ns} (pipeline_tracker + table_completion)"
    return 0
  fi
  local as_of
  as_of=$(( $(date +%s) * 1000 ))
  pg_tracker_exec "
    BEGIN;
    DELETE FROM \"${ns}\".table_completion WHERE pipeline_name = '${table}';
    INSERT INTO \"${ns}\".pipeline_tracker
      (source_key, table_name, phase, state, row_count, as_of)
    VALUES ('_table_complete', '${table}', 'table_completion', 'cleared', 0, ${as_of})
    ON CONFLICT (source_key, table_name, phase)
      DO UPDATE SET state = 'cleared', as_of = EXCLUDED.as_of;
    COMMIT;" > /dev/null || return 1
  echo "  Cleared completion for '${table}' in ${ns}"
}
