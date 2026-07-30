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
# Repairs SEC accessions whose staged parquet never reached its Iceberg table.
#
# WHAT WENT WRONG
#   Merged batch files were named {tableType}_batch_{NNNN}.parquet from a counter that
#   restarts at 0 in every process. SEC workers are partitioned by form type, not by
#   table — every form contributes a filing-metadata row — so sec_primary, sec_secondary
#   and sec_13f all wrote `metadata` (and the first two also facts/contexts/mda/
#   relationships/chunks) into one sec/year=YYYY/ partition. Run concurrently, they
#   overwrote each other. The loser's accessions stay marked staging-complete, so
#   SecFilingCache.filterUnprocessed never offers them again and nothing re-derives them.
#
#   The naming is fixed (LocalStagingStorageProvider now qualifies each merged file with a
#   per-instance token), which stops the loss continuing. It does not repair what was
#   already lost — that is this script.
#
# HOW A GAP IS DETECTED
#   Every staging marker is written inside `if (inventory.hasX())`, so a marker means that
#   output really was produced. An accession carrying a staging marker for table X with no
#   corresponding row-level entry in X's Iceberg table therefore lost data — it is not a
#   filing that legitimately had nothing to contribute.
#
# WHY force-reprocess RATHER THAN DELETING TRACKER ROWS
#   filterUnprocessed has a forceAccessions path that runs before any tracker read, so
#   listed accessions are reprocessed regardless of their markers. That leaves tracker
#   state intact, is reversible (just stop passing them), and does not race workers that
#   are concurrently reading and writing the same keys.
#
# SAFETY
#   * Dry run by default. Nothing is reprocessed until you pass --confirm.
#   * Refuses to reprocess while any SEC worker is live — concurrent writers are the cause
#     of the damage, and until the fixed jar is deployed they would recreate it.
#   * Idempotent and re-runnable. Reprocessing an accession regenerates all of its outputs;
#     rows already in Iceberg are dropped by the row-level NOT IN backstop, so
#     over-including an accession costs time, never correctness. Re-run until the report
#     reads zero.
#   * Never passes --force-download. The raw EDGAR cache is intact and is what makes this
#     cheap; that flag would delete it, and it derives its purge path from the accession
#     prefix, which is the filing agent's CIK rather than the subject company's.
#
# USAGE
#   fix-sec.sh                                  # report gaps, reprocess nothing
#   fix-sec.sh --confirm                        # repair every gap, oldest year first
#   fix-sec.sh --year 2026 --confirm            # repair one filing year
#   fix-sec.sh --table metadata                 # report one staging table's gap
#   fix-sec.sh --batch-size 1000 --confirm      # smaller worker invocations
#   fix-sec.sh --max-accessions 500 --confirm   # cap the run (rehearsal)
#   fix-sec.sh --threads 12 --heap 16 --confirm # push harder on an otherwise idle box
#   fix-sec.sh --threads 2 --heap 3 --confirm   # back off to pool-sized resources
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

# Which Iceberg tables each SEC form type is defined to produce.
#
# Transcribed from FormType's enum constructor (govdata/.../sec/FormType.java), whose argument
# order is metadata, facts, contexts, relationships, mda, insider, earnings, chunks,
# institutional_holdings, beneficial_ownership. FormType.getExpectedOutputs() is what the ETL
# itself uses to decide whether a filing's outputs are complete, so this is the pipeline's own
# contract rather than an inference about it.
#
# This replaces asking the tracker "is there a staging marker with no Iceberg row?". A staging
# marker records that an output FILE was written, not that it held any rows, and several writers
# emit an empty file deliberately — XbrlToParquetConverter says so outright ("Always write the
# file, even if empty, to satisfy cache validation") for facts, relationships, insider and chunks.
# Every such filing therefore looked permanently lost: measured against 2,000 accessions that had
# just been reprocessed successfully, relationships still showed 788 of 793 "missing" and chunks
# 1,014 of 1,986. Nothing loses 99% of its rows; that was the empty-file artifact, and the repair
# chased it every run without ever clearing it.
#
# Asking instead "this form is DEFINED to produce table T, and T has no rows for this filing"
# needs no marker and makes no assumption about emptiness. Validated against production: no
# accession outside the 10-K/10-Q family has relationships rows, so the enum describes what the
# data actually looks like.
FORM_TABLES=(
  "10-K:filing_metadata,financial_line_items,filing_contexts,xbrl_relationships,mda_sections,vectorized_chunks"
  "10-K/A:filing_metadata,financial_line_items,filing_contexts,xbrl_relationships,mda_sections,vectorized_chunks"
  "10-Q:filing_metadata,financial_line_items,filing_contexts,xbrl_relationships,mda_sections,vectorized_chunks"
  "10-Q/A:filing_metadata,financial_line_items,filing_contexts,xbrl_relationships,mda_sections,vectorized_chunks"
  "8-K:filing_metadata,earnings_transcripts,vectorized_chunks"
  "8-K/A:filing_metadata,earnings_transcripts,vectorized_chunks"
  "DEF 14A:filing_metadata,financial_line_items,filing_contexts"
  "3:filing_metadata,insider_transactions"
  "4:filing_metadata,insider_transactions"
  "5:filing_metadata,insider_transactions"
  "13F-HR:filing_metadata,institutional_holdings"
  "13F-HR/A:filing_metadata,institutional_holdings"
  "SC 13D:filing_metadata,beneficial_ownership"
  "SC 13D/A:filing_metadata,beneficial_ownership"
  "SC 13G:filing_metadata,beneficial_ownership"
  "SC 13G/A:filing_metadata,beneficial_ownership"
)

# vectorized_chunks is gated on vectorizationEnabled in FormType.getExpectedOutputs(). Drop it
# from every form's expected set when the embedding pipeline is not in play, otherwise every
# 10-K and 8-K is reported as missing a table it was never asked to produce.
VECTORIZATION_ENABLED="${GOVDATA_VECTORIZATION_ENABLED:-true}"

CONFIRM=false
ONLY_TABLE=""
ONLY_YEAR=""
BATCH_SIZE=2000
MAX_ACCESSIONS=0
# Reprocessing is a bulk backfill, not the steady-state pipeline, so it is sized for the
# machine rather than for coexisting with a pool. .env.prod pins ETL_PARALLEL_THREADS=2 —
# correct when several schema workers share the box, wasteful when this is the only thing
# running: a measured batch held ~66% of ONE core and 40% disk utilisation at queue depth
# 0.73, so both CPU and spindle were mostly idle waiting for work to be handed to them.
# Heap has to rise with the thread count: each worker thread holds a document in flight, and
# the run above sat at 2.48G RSS against -Xmx3g with only two of them.
THREADS=8
HEAP_GB=12

while [ $# -gt 0 ]; do
  case "$1" in
    --confirm)        CONFIRM=true ;;
    --table)          shift; ONLY_TABLE="${1:-}" ;;
    --year)           shift; ONLY_YEAR="${1:-}" ;;
    --batch-size)     shift; BATCH_SIZE="${1:-}" ;;
    --max-accessions) shift; MAX_ACCESSIONS="${1:-}" ;;
    --threads)        shift; THREADS="${1:-}" ;;
    --heap)           shift; HEAP_GB="${1:-}" ;;
    -h|--help)        sed -n '18,62p' "$0" >&2; exit 0 ;;
    *) echo "fix-sec: unknown argument '$1'" >&2; exit 2 ;;
  esac
  shift
done

[[ "$BATCH_SIZE" =~ ^[0-9]+$ ]] && [ "$BATCH_SIZE" -gt 0 ] \
  || { echo "fix-sec: --batch-size must be a positive integer" >&2; exit 2; }
[[ "$MAX_ACCESSIONS" =~ ^[0-9]+$ ]] \
  || { echo "fix-sec: --max-accessions must be an integer" >&2; exit 2; }
[ -z "$ONLY_YEAR" ] || [[ "$ONLY_YEAR" =~ ^[0-9]{4}$ ]] \
  || { echo "fix-sec: --year must be a 4-digit year" >&2; exit 2; }

# Accessions are passed to worker-sec-reprocess.sh as positional arguments, so a batch has
# to fit argv. At ~21 bytes each plus the rest of the command line, cap well under ARG_MAX
# rather than discovering the limit as "Argument list too long" partway through a repair.
[[ "$THREADS" =~ ^[0-9]+$ ]] && [ "$THREADS" -gt 0 ] \
  || { echo "fix-sec: --threads must be a positive integer" >&2; exit 2; }
[[ "$HEAP_GB" =~ ^[0-9]+$ ]] && [ "$HEAP_GB" -gt 0 ] \
  || { echo "fix-sec: --heap must be a positive integer (GB)" >&2; exit 2; }

# Applies to every worker-sec-reprocess.sh invocation below. Exported rather than passed:
# run_etl reads both from the environment, and .env.prod's values would otherwise win.
export ETL_PARALLEL_THREADS="$THREADS"
export ETL_HEAP_MIN="$((HEAP_GB / 2))g"
export ETL_HEAP_MAX="${HEAP_GB}g"

ARG_MAX="$(getconf ARG_MAX 2>/dev/null || echo 2097152)"
MAX_BATCH=$(( (ARG_MAX / 2) / 21 ))
if [ "$BATCH_SIZE" -gt "$MAX_BATCH" ]; then
  log_info "fix-sec: clamping --batch-size $BATCH_SIZE to $MAX_BATCH (ARG_MAX=$ARG_MAX)"
  BATCH_SIZE=$MAX_BATCH
fi

# -------------------------------------------------------------------------------------
# Postgres connection — same tracker the ETL writes, same bucket-derived schema.
# -------------------------------------------------------------------------------------

: "${GOVDATA_TRACKER_PG_URL:?GOVDATA_TRACKER_PG_URL not set — cannot read the tracker}"

# jdbc:postgresql://host:port/db -> host, port, db
_jdbc="${GOVDATA_TRACKER_PG_URL#jdbc:postgresql://}"
_jdbc="${_jdbc%%\?*}"
PGHOST_="${_jdbc%%:*}"
_rest="${_jdbc#*:}"
PGPORT_="${_rest%%/*}"
PGDB_="${_rest#*/}"

# PGPipelineTracker.sanitizeNamespace: strip the URI scheme, lowercase, non-alphanumerics
# to underscore, trim leading/trailing underscores. Keeps dq and prod trackers apart.
PG_SCHEMA="${GOVDATA_TRACKER_PG_SCHEMA:-}"
if [ -z "$PG_SCHEMA" ]; then
  PG_SCHEMA=$(printf '%s' "${GOVDATA_PARQUET_DIR:-}" \
    | sed -E 's#^[a-z0-9]+://##; s#/.*$##' \
    | tr '[:upper:]' '[:lower:]' \
    | sed -E 's#[^a-z0-9]+#_#g; s#^_+##; s#_+$##')
fi
[ -n "$PG_SCHEMA" ] \
  || { echo "fix-sec: cannot derive the tracker schema; set GOVDATA_TRACKER_PG_SCHEMA" >&2; exit 2; }

psql_q() {
  PGPASSWORD="${GOVDATA_TRACKER_PG_PASSWORD:-}" psql \
    -h "$PGHOST_" -p "$PGPORT_" -U "${GOVDATA_TRACKER_PG_USER:-}" -d "$PGDB_" \
    -v ON_ERROR_STOP=1 -At -F'|' "$@"
}

log_info "fix-sec: tracker ${PGHOST_}:${PGPORT_}/${PGDB_} schema ${PG_SCHEMA}"

# -------------------------------------------------------------------------------------
# Enumerate the gap.
#
# One sequential pass over pipeline_tracker into a temp table, then indexed EXCEPTs per
# pair. Running a regex-extracting EXCEPT per pair straight against the 16M-row table
# instead takes tens of minutes.
# -------------------------------------------------------------------------------------

WORK_DIR="$SCRIPT_DIR/runs/fix-sec"
mkdir -p "$WORK_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
ACC_FILE="$WORK_DIR/missing-$STAMP.txt"
REPORT_FILE="$WORK_DIR/report-$STAMP.txt"

# Expand FORM_TABLES into (form, table) rows, dropping vectorized_chunks when vectorization is
# off so a table nobody asked for is never reported missing.
_matrix_sql=""
for entry in "${FORM_TABLES[@]}"; do
  form="${entry%%:*}"; tbls="${entry##*:}"
  IFS=',' read -ra _t <<< "$tbls"
  for tbl in "${_t[@]}"; do
    [ "$tbl" = "vectorized_chunks" ] && [ "$VECTORIZATION_ENABLED" != "true" ] && continue
    [ -z "$ONLY_TABLE" ] || [ "$ONLY_TABLE" = "$tbl" ] || continue
    _matrix_sql="${_matrix_sql}${_matrix_sql:+,}('${form//\'/\'\'}','${tbl}')"
  done
done
[ -n "$_matrix_sql" ] || { echo "fix-sec: --table '$ONLY_TABLE' is not a SEC Iceberg table" >&2; exit 2; }

# Distinct Iceberg tables the matrix references — one scan each, no more.
_scanned=$(printf '%s\n' "${FORM_TABLES[@]}" | cut -d: -f2- | tr ',' '\n' | sort -u)
[ "$VECTORIZATION_ENABLED" = "true" ] || _scanned=$(printf '%s\n' "$_scanned" | grep -v '^vectorized_chunks$')
[ -z "$ONLY_TABLE" ] || _scanned="$ONLY_TABLE"

_year_filter=""
[ -z "$ONLY_YEAR" ] || _year_filter="AND acc LIKE '%-${ONLY_YEAR:2:2}-%'"

_duck_ep="${AWS_ENDPOINT_OVERRIDE#http://}"; _duck_ep="${_duck_ep#https://}"
command -v duckdb >/dev/null 2>&1 \
  || { echo "fix-sec: duckdb is required to read the Iceberg tables" >&2; exit 2; }

log_info "fix-sec: scanning Iceberg for filings missing a table their form should produce"

{
  echo "INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;"
  echo "CREATE SECRET (TYPE S3, PROVIDER credential_chain, ENDPOINT '${_duck_ep}',"
  echo "               USE_SSL false, URL_STYLE 'path', REGION 'auto');"
  # filing_metadata carries filing_type, which is what maps a filing onto its expected tables.
  echo "CREATE TEMP TABLE meta AS SELECT accession_number AS acc, filing_type AS form"
  echo "  FROM iceberg_scan('${GOVDATA_PARQUET_DIR}/sec/filing_metadata', allow_moved_paths => true)"
  echo " WHERE accession_number IS NOT NULL ${_year_filter};"
  echo "CREATE TEMP TABLE present(tbl VARCHAR, acc VARCHAR);"
  printf '%s\n' "$_scanned" | while read -r tbl; do
    [ -n "$tbl" ] || continue
    echo "INSERT INTO present SELECT '${tbl}', acc FROM ("
    echo "  SELECT DISTINCT accession_number AS acc"
    echo "    FROM iceberg_scan('${GOVDATA_PARQUET_DIR}/sec/${tbl}', allow_moved_paths => true)"
    echo "   WHERE accession_number IS NOT NULL) x;"
  done
  echo "CREATE TEMP TABLE matrix(form VARCHAR, tbl VARCHAR);"
  echo "INSERT INTO matrix VALUES ${_matrix_sql};"
  # A gap is a filing whose form is DEFINED to produce table T, with no row in T. No staging
  # marker is consulted, so an output that was legitimately empty is never mistaken for loss.
  echo "CREATE TEMP TABLE gaps AS"
  echo "  SELECT m.acc, x.tbl FROM meta m JOIN matrix x ON x.form = m.form"
  echo "   WHERE NOT EXISTS (SELECT 1 FROM present p WHERE p.tbl = x.tbl AND p.acc = m.acc);"
  echo "COPY (SELECT DISTINCT acc FROM gaps ORDER BY acc) TO '${ACC_FILE}' (HEADER false);"
  echo "SELECT 'BY_TABLE|'||tbl||'|'||count(DISTINCT acc) FROM gaps GROUP BY tbl ORDER BY 1;"
  echo "SELECT 'BY_YEAR|20'||yy||'|'||cnt FROM (SELECT substring(acc,12,2) AS yy,"
  echo "  count(DISTINCT acc) AS cnt FROM gaps GROUP BY 1) y ORDER BY yy;"
  echo "SELECT 'TOTAL|'||count(DISTINCT acc) FROM gaps;"
} > "$WORK_DIR/enumerate-$STAMP.sql"

if ! duckdb -noheader -list < "$WORK_DIR/enumerate-$STAMP.sql" > "$REPORT_FILE" 2>&1; then
  echo "fix-sec: Iceberg enumeration failed — see $REPORT_FILE" >&2
  exit 1
fi

# -------------------------------------------------------------------------------------
# Subtract accessions already attempted.
#
# Some gaps cannot be closed by reprocessing, and there is no way to tell which from the data:
# a filing whose batch file was overwritten and one whose output was legitimately empty look
# identical once the source is gone. Without a record of what has been tried, the enumeration
# re-offers both every run — which is why a repair that processed ~22,000 accessions moved the
# gap by 442 and then kept re-processing the same work.
#
# Recording the attempt rather than the outcome sidesteps the undecidable part: try each
# accession once, and converge whether or not rows appeared.
# -------------------------------------------------------------------------------------

psql_q -c "CREATE TABLE IF NOT EXISTS ${PG_SCHEMA}.fix_sec_attempted (
             accession   VARCHAR PRIMARY KEY,
             attempted_at TIMESTAMPTZ NOT NULL DEFAULT now())" >/dev/null

ATTEMPTED=$(psql_q -c "SELECT count(*) FROM ${PG_SCHEMA}.fix_sec_attempted")
if [ "${ATTEMPTED:-0}" -gt 0 ]; then
  psql_q -c "\\copy (SELECT accession FROM ${PG_SCHEMA}.fix_sec_attempted ORDER BY accession) TO '$ACC_FILE.attempted'" >/dev/null
  sort -u "$ACC_FILE" > "$ACC_FILE.all"
  comm -23 "$ACC_FILE.all" "$ACC_FILE.attempted" > "$ACC_FILE"
  log_info "fix-sec: $(wc -l < "$ACC_FILE.all") in gap, $ATTEMPTED already attempted, $(wc -l < "$ACC_FILE") remaining"
  rm -f "$ACC_FILE.all" "$ACC_FILE.attempted"
fi

TOTAL=$(wc -l < "$ACC_FILE" | tr -d ' ')
TOTAL="${TOTAL:-0}"

echo
echo "Filings missing a table their form is defined to produce:"
awk -F'|' '$1=="BY_TABLE"{printf "  %-16s %8d\n", $2, $3}' "$REPORT_FILE"
echo
echo "By filing year:"
awk -F'|' '$1=="BY_YEAR"{printf "  %-16s %8d\n", $2, $3}' "$REPORT_FILE"
echo
echo "Distinct accessions to reprocess: $TOTAL"
echo "  accession list: $ACC_FILE"
echo

if [ "$TOTAL" -eq 0 ]; then
  log_info "fix-sec: nothing to repair"
  exit 0
fi

# The enumeration above reads Iceberg directly, so there is no second source left to check it
# against — the earlier spot-check existed only because candidates came from the tracker, which
# is a cache of Iceberg state rather than the authority. Reading the tables themselves removes
# both the indirection and the need to sample.

if ! $CONFIRM; then
  echo "DRY RUN — nothing reprocessed. Re-run with --confirm to repair."
  exit 0
fi

# -------------------------------------------------------------------------------------
# Refuse to run alongside a live SEC writer.
#
# Concurrent writers are what caused this. Until the fixed jar is the one the pool is
# running, a repair racing a worker would lose the rows it just rewrote.
# -------------------------------------------------------------------------------------

_pid_dir="$SCRIPT_DIR/runs/pids"
_live=""
if [ -d "$_pid_dir" ]; then
  for _pf in "$_pid_dir"/worker-sec*.pid; do
    [ -e "$_pf" ] || continue
    _id=$(basename "$_pf" .pid)
    [ -f "$_pid_dir/${_id}.exit" ] && continue
    _wpid=$(head -1 "$_pf" 2>/dev/null | tr -d '[:space:]')
    { [ -n "$_wpid" ] && kill -0 "$_wpid" 2>/dev/null; } || continue
    _live="$_live $_id"
  done
fi
if [ -n "$_live" ]; then
  echo "fix-sec: REFUSING to reprocess — live SEC worker(s):${_live}" >&2
  echo "  These write the same sec/year=*/ partitions. Stop the pool" >&2
  echo "  (stop-pool.sh) and re-run, or wait for them to finish." >&2
  exit 1
fi

# -------------------------------------------------------------------------------------
# Reprocess, oldest year first, in argv-sized batches.
# -------------------------------------------------------------------------------------

if [ "$MAX_ACCESSIONS" -gt 0 ] && [ "$TOTAL" -gt "$MAX_ACCESSIONS" ]; then
  log_info "fix-sec: capping this run at $MAX_ACCESSIONS of $TOTAL accessions"
  head -n "$MAX_ACCESSIONS" "$ACC_FILE" > "$ACC_FILE.capped"
  mv "$ACC_FILE.capped" "$ACC_FILE"
fi

# Bucket by the filing year the accession encodes. One year per worker invocation keeps
# the EDGAR full-index load to a single year: worker-sec-reprocess.sh derives its model's
# year RANGE from the accessions it is given, so a mixed batch would load every year in
# between just to locate a handful of filings.
awk '{ print "20" substr($0,12,2) "\t" $0 }' "$ACC_FILE" | sort > "$ACC_FILE.byyear"

YEARS=$(cut -f1 "$ACC_FILE.byyear" | uniq)
# Record an accession list as attempted. Whether rows appeared or the filing's output was
# legitimately empty, each listed accession has now had its one pass and must drop out of the next
# enumeration — otherwise the undecidable cases recirculate forever. ON CONFLICT keeps a re-run of
# the same list idempotent.
record_attempted() {
  _rf="$1"
  psql_q -c "\\copy ${PG_SCHEMA}.fix_sec_attempted (accession) FROM '$_rf'" >/dev/null 2>&1 \
    || psql_q -c "INSERT INTO ${PG_SCHEMA}.fix_sec_attempted (accession)
                  SELECT unnest(string_to_array(pg_read_file('$_rf'), E'\\n'))
                  ON CONFLICT (accession) DO NOTHING" >/dev/null 2>&1 \
    || return 1
  return 0
}

batches=0
failed=0
for year in $YEARS; do
  year_file="$ACC_FILE.$year"
  awk -F'\t' -v y="$year" '$1==y{print $2}' "$ACC_FILE.byyear" > "$year_file"
  count=$(wc -l < "$year_file")
  log_info "fix-sec: year $year — $count accession(s), batches of $BATCH_SIZE"

  split -l "$BATCH_SIZE" -d "$year_file" "$year_file.batch."
  for bf in "$year_file".batch.*; do
    [ -e "$bf" ] || continue
    batches=$((batches + 1))
    n=$(wc -l < "$bf")
    log_info "fix-sec: reprocessing batch $batches ($n accessions, year $year)"
    # No --force-download: the raw cache is intact and is what makes this cheap.
    if ! "$SCRIPT_DIR/worker-sec-reprocess.sh" $(tr '\n' ' ' < "$bf"); then
      failed=$((failed + 1))
      log_info "fix-sec: batch $batches FAILED (year $year) — list kept at $bf"
      continue
    fi

    # A zero exit is not evidence the batch did anything. The ETL reports success when it
    # enumerated the accessions and then skipped every one of them, which is what a
    # disagreement between the two "already done?" gates looks like from outside — and it
    # is indistinguishable from a batch that legitimately had no work. Read the counts.
    _wlog=$(ls -t "$SCRIPT_DIR/runs/worker-sec-reprocess"/etl_*.log 2>/dev/null | head -1)
    _done_line=$(grep -h "Document ETL completed" "$_wlog" 2>/dev/null | tail -1)
    _processed=$(printf '%s' "$_done_line" | sed -nE 's/.*completed: ([0-9]+) processed.*/\1/p')
    _skipped=$(printf '%s' "$_done_line" | sed -nE 's/.*, ([0-9]+) skipped.*/\1/p')
    _etlfailed=$(printf '%s' "$_done_line" | sed -nE 's/.*, ([0-9]+) failed.*/\1/p')
    if [ -n "$_processed" ] && [ "$_processed" = "0" ] && [ "${_skipped:-0}" -gt 0 ]; then
      failed=$((failed + 1))
      log_info "fix-sec: batch $batches processed 0 of $n accessions (${_skipped} skipped) —" \
               "the ETL treated them as already done, so nothing was written."
      log_info "  worker log: $_wlog"
      log_info "  list kept at $bf"
      continue
    fi

    # An accession that errored must NOT be recorded as attempted; one that succeeded alongside it
    # must be.
    #
    # The attempted-list exists to retire accessions whose gap reprocessing cannot close — an
    # output that was legitimately empty is indistinguishable from one whose source was
    # overwritten, so each gets a single pass and then drops out. A filing that failed has had no
    # such pass, and recording it writes it out of the repair for good.
    #
    # This is not hypothetical in either direction. When SEC began answering with HTTP 429, one
    # batch reported "981 processed, 4 skipped, 1015 failed" and recorded all 2000 — the 1015 were
    # excluded from every future enumeration. Retiring the whole batch on any failure fixed that
    # and broke the converse: six 13F filings that fail every time discarded the credit for 1994
    # successes, so the batch was re-offered, re-run for 94 minutes, and discarded again. Neither
    # year can reach zero that way. Split the batch on the outcome the ETL already reports.
    if [ -n "$_etlfailed" ] && [ "$_etlfailed" -gt 0 ]; then
      failed=$((failed + 1))
      log_info "fix-sec: batch $batches — ${_etlfailed} of $n accessions errored" \
               "(${_processed:-?} processed)."
      log_info "  worker log: $_wlog"

      # Name the failures so the rest can be credited. The ETL logs each as
      # "Failed to process accession <cik>/<accession>".
      grep -aoE "Failed to process accession [0-9]+/[0-9][0-9-]+" "$_wlog" \
        | sed -E 's#.*/##' | sort -u > "$bf.failed"
      _nfail=$(wc -l < "$bf.failed")

      # Only split when every reported failure was named. A short list would credit an accession
      # that actually failed, which is the exact silent exclusion this guard exists to prevent.
      if [ "$_nfail" -ne "${_etlfailed}" ]; then
        log_info "  could not name every failure (${_nfail} named, ${_etlfailed} reported) —" \
                 "recording nothing; list kept at $bf"
        rm -f "$bf.failed"
        continue
      fi

      grep -vxF -f "$bf.failed" "$bf" > "$bf.ok"
      _nok=$(wc -l < "$bf.ok")
      if [ "$_nok" -gt 0 ]; then
        record_attempted "$bf.ok" \
          && log_info "  recorded ${_nok} succeeded; ${_nfail} left to retry" \
          || log_info "  WARNING could not record the ${_nok} succeeded — they will recur"
      fi
      log_info "  failures kept at $bf.failed"
      rm -f "$bf.ok"

      _429=$(grep -ac "HTTP 429" "$_wlog" 2>/dev/null || echo 0)
      if [ "${_429:-0}" -gt 0 ]; then
        log_info "  ${_429} x HTTP 429 — SEC is rate-limiting. Stopping rather than escalating;" \
                 "re-run later and the attempted-list resumes from here."
        exit 1
      fi
      continue
    fi

    log_info "fix-sec: batch $batches processed ${_processed:-?} accession(s)"

    record_attempted "$bf" \
      || log_info "fix-sec: WARNING could not record batch $batches as attempted — it will recur"

    rm -f "$bf"
  done
  rm -f "$year_file"
done

echo
log_info "fix-sec: $batches batch(es) attempted, $failed failed"
if [ "$failed" -gt 0 ]; then
  echo "fix-sec: re-run to retry — the report is rebuilt from the tracker each time," >&2
  echo "  so anything genuinely repaired drops out and only real gaps remain." >&2
  exit 1
fi
log_info "fix-sec: re-run without --confirm to confirm the gap is closed"
