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

# Staging marker name -> Iceberg table, taken from sec-schema.yaml's per-table `pattern`
# (year=*/*<token>*.parquet). Reprocessing works per accession, not per table, so these
# only decide which accessions are pulled in — the repair regenerates every output.
TABLE_MAP=(
  "metadata:filing_metadata"
  "facts:financial_line_items"
  "contexts:filing_contexts"
  "mda:mda_sections"
  "relationships:xbrl_relationships"
  "insider:insider_transactions"
  "earnings:earnings_transcripts"
  "chunks:vectorized_chunks"
  "13f:institutional_holdings"
  "13dg:beneficial_ownership"
)

CONFIRM=false
ONLY_TABLE=""
ONLY_YEAR=""
BATCH_SIZE=2000
MAX_ACCESSIONS=0
VERIFY=true
VERIFY_N=12
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
    --no-verify)      VERIFY=false ;;
    --verify-n)       shift; VERIFY_N="${1:-}" ;;
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

_pairs_sql=""
_names_sql=""
for entry in "${TABLE_MAP[@]}"; do
  stage="${entry%%:*}"; ice="${entry##*:}"
  [ -z "$ONLY_TABLE" ] || [ "$ONLY_TABLE" = "$stage" ] || continue
  _pairs_sql="${_pairs_sql}${_pairs_sql:+,}('${stage}','${ice}')"
  _names_sql="${_names_sql}${_names_sql:+,}'${stage}','${ice}'"
done
[ -n "$_pairs_sql" ] || { echo "fix-sec: --table '$ONLY_TABLE' is not a SEC staging table" >&2; exit 2; }

_year_filter=""
[ -z "$ONLY_YEAR" ] || _year_filter="AND substring(acc from 12 for 2) = '${ONLY_YEAR:2:2}'"

log_info "fix-sec: scanning tracker for staged-but-unmaterialized accessions"

psql_q <<SQL > "$REPORT_FILE"
CREATE TEMP TABLE t AS
  SELECT table_name, split_part(split_part(source_key,'=',2),'__',1) AS acc
    FROM ${PG_SCHEMA}.pipeline_tracker
   WHERE table_name IN (${_names_sql});
CREATE INDEX ON t (table_name, acc);
ANALYZE t;

CREATE TEMP TABLE pairs(stage text, ice text);
INSERT INTO pairs VALUES ${_pairs_sql};

-- An accession is missing for a pair when it carries the staging marker but never
-- reached the Iceberg table. Year comes from the accession itself (NNNNNNNNNN-YY-NNNNNN).
CREATE TEMP TABLE gaps AS
  SELECT p.stage, s.acc
    FROM pairs p
    JOIN t s ON s.table_name = p.stage
   WHERE NOT EXISTS (SELECT 1 FROM t m WHERE m.table_name = p.ice AND m.acc = s.acc)
     AND s.acc ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}\$'
     ${_year_filter};

\\copy (SELECT DISTINCT acc FROM gaps ORDER BY acc) TO '${ACC_FILE}'

SELECT 'BY_TABLE|'||stage||'|'||count(DISTINCT acc) FROM gaps GROUP BY stage ORDER BY 1;
SELECT 'BY_YEAR|20'||yy||'|'||cnt FROM (
  SELECT substring(acc from 12 for 2) AS yy, count(DISTINCT acc) AS cnt
    FROM gaps GROUP BY 1) y ORDER BY yy;
SELECT 'TOTAL|'||count(DISTINCT acc) FROM gaps;
SQL

if [ $? -ne 0 ]; then
  echo "fix-sec: tracker query failed — see $REPORT_FILE" >&2
  exit 1
fi

TOTAL=$(awk -F'|' '$1=="TOTAL"{print $2}' "$REPORT_FILE")
TOTAL="${TOTAL:-0}"

echo
echo "Accessions staged but never materialized, by table:"
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

# -------------------------------------------------------------------------------------
# Spot-check the candidates against Iceberg.
#
# The counts above come from the tracker, which is a CACHE of Iceberg state, not the
# authority — getExcludedAccessions repopulates it from Iceberg per (table, year) as
# materialization runs, so a partition not visited recently can under-report and make
# healthy accessions look lost. Reprocessing is idempotent, but a false positive rate
# here is the difference between repairing thousands and needlessly rebuilding millions,
# so sample a few candidates per table and read the real answer out of Iceberg.
# -------------------------------------------------------------------------------------

verify_sample() {
  local stage="$1" ice="$2" n="$3"
  local accs
  accs=$(psql_q -c "
    SELECT split_part(split_part(s.source_key,'=',2),'__',1) AS acc
      FROM ${PG_SCHEMA}.pipeline_tracker s
     WHERE s.table_name = '${stage}' AND s.phase = 'staging'
       AND split_part(split_part(s.source_key,'=',2),'__',1) ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}\$'
       AND NOT EXISTS (
         SELECT 1 FROM ${PG_SCHEMA}.pipeline_tracker m
          WHERE m.table_name = '${ice}'
            AND split_part(split_part(m.source_key,'=',2),'__',1)
                = split_part(split_part(s.source_key,'=',2),'__',1))
     LIMIT ${n}" 2>/dev/null)
  [ -n "$accs" ] || { printf "  %-16s %s\n" "$stage" "no candidates"; return; }

  local in_list count found
  in_list=$(printf '%s\n' "$accs" | awk '{printf "%s'\''%s'\''", (NR>1?",":""), $0}')
  count=$(printf '%s\n' "$accs" | wc -l)
  found=$(duckdb -noheader -list -c "
    INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;
    CREATE SECRET (TYPE S3, PROVIDER credential_chain, ENDPOINT '${_duck_endpoint}',
                   USE_SSL false, URL_STYLE 'path', REGION 'auto');
    SELECT count(DISTINCT accession_number)
      FROM iceberg_scan('${GOVDATA_PARQUET_DIR}/sec/${ice}', allow_moved_paths=true)
     WHERE accession_number IN (${in_list});" 2>/dev/null | tail -1)
  found="${found:-?}"
  if [ "$found" = "0" ]; then
    printf "  %-16s %s\n" "$stage" "$count/$count confirmed missing"
  else
    printf "  %-16s %s\n" "$stage" \
      "$found of $count candidates ARE in Iceberg — tracker is stale, treat counts as an upper bound"
  fi
}

if $VERIFY; then
  # DuckDB wants a bare host:port; the ETL passes the same endpoint without its scheme.
  _duck_endpoint="${AWS_ENDPOINT_OVERRIDE:-}"
  _duck_endpoint="${_duck_endpoint#http://}"
  _duck_endpoint="${_duck_endpoint#https://}"
  if [ -z "$_duck_endpoint" ] || ! command -v duckdb >/dev/null 2>&1; then
    echo "Skipping Iceberg spot-check (need duckdb and AWS_ENDPOINT_OVERRIDE); counts are"
    echo "tracker-derived and may over-report. Pass --no-verify to silence this."
    echo
  else
    echo "Iceberg spot-check ($VERIFY_N candidates per table):"
    for entry in "${TABLE_MAP[@]}"; do
      stage="${entry%%:*}"; ice="${entry##*:}"
      [ -z "$ONLY_TABLE" ] || [ "$ONLY_TABLE" = "$stage" ] || continue
      awk -F'|' -v s="$stage" '$1=="BY_TABLE" && $2==s{f=1} END{exit !f}' "$REPORT_FILE" || continue
      verify_sample "$stage" "$ice" "$VERIFY_N"
    done
    echo
  fi
fi

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
    if [ -n "$_processed" ] && [ "$_processed" = "0" ] && [ "${_skipped:-0}" -gt 0 ]; then
      failed=$((failed + 1))
      log_info "fix-sec: batch $batches processed 0 of $n accessions (${_skipped} skipped) —" \
               "the ETL treated them as already done, so nothing was written."
      log_info "  worker log: $_wlog"
      log_info "  list kept at $bf"
      continue
    fi
    log_info "fix-sec: batch $batches processed ${_processed:-?} accession(s)"
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
