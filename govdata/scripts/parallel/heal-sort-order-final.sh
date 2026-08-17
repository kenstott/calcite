#!/usr/bin/env bash
# heal-sort-order-final.sh — third and last heal pass: the 3 tables heal-sort-order-retry.sh
# could not finish.
#
# ── WHAT HAPPENED ────────────────────────────────────────────────────────────────────────────
# Pass 1 (heal-sort-order.sh)       healed 8 of 13, failed on 5 large string-heavy SEC tables.
# Pass 2 (heal-sort-order-retry.sh) healed 2 of those 5 (filing_contexts, institutional_holdings)
#                                   and failed on 3:
#     sec.xbrl_relationships   rc=1 after  672s
#     sec.mda_sections         rc=1 after  596s
#     sec.financial_line_items rc=1 after 1330s
#
# All three got well into the rewrite before dying (596-1330s), which points at memory rather
# than a setup problem. Pass 2 already applied the decoded-heap sizing fix and 12g; this pass
# forces the external merge far sooner (64MB decoded budget vs 512MB) with much smaller spill
# runs (10k vs 50k records) and a bigger expansion factor, which is exactly the escalation
# pass 2 recommends.
#
# A fourth table, sec.vectorized_chunks, also failed pass 2 — in THREE SECONDS, before anything
# had been read. That was never a memory problem: sec-schema.yaml sets
# `materialize.enabled: false` on it, retiring its Iceberg location in the sec warehouse as a
# live write target, with rows now landing in ref.vectorized_chunks
# (SecSchemaFactory#materializeSecChunksToRef). Healing frozen pre-migration data that nothing
# writes to and no query path reads was pointless, so on 2026-08-17 it was dropped from every
# heal/verify/sync list. The healed set is 13 tables, not 14.
#
# ── WHY THIS SCRIPT EXISTS RATHER THAN MORE ENV VARS ON PASS 2 ───────────────────────────────
# Pass 2 pipes java through grep and keeps only the exit code, so a repeat failure leaves no
# evidence: the failures above produced no diagnosable output, and its -XX:HeapDumpPath
# pointed at /tmp, which is cleared on restart — no .hprof survived. This pass writes the FULL
# per-table output to a durable log directory, keeps heap dumps beside it, and classifies each
# failure (OOM / missing table / already healed / other) from that log instead of guessing.
#
# ── SAFE TO RE-RUN ───────────────────────────────────────────────────────────────────────────
# Same guarantee as pass 2: a rewrite commits atomically per partition, and `aperio.sorted-by`
# is written only after EVERY partition of a table succeeds, so none of these 3 recorded a sort
# order and all are eligible again. Partitions that DID complete before a failure were
# committed, so those tables are part-sorted and this run rewrites them again — wasted work,
# not incorrect work. Tables healed in passes 1 and 2 are marked and are not in this list.
#
# ── USAGE ────────────────────────────────────────────────────────────────────────────────────
#   govdata/scripts/parallel/heal-sort-order-final.sh                        # DRY RUN
#   govdata/scripts/parallel/heal-sort-order-final.sh --confirm              # rewrite all 3
#   govdata/scripts/parallel/heal-sort-order-final.sh --confirm mda_sections # one table
#   govdata/scripts/parallel/heal-sort-order-final.sh --list
#
# Env:
#   GOVDATA_HEAP           JVM max heap (default 24g; 12g in pass 2, 4g in pass 1)
#   GOVDATA_SORT_BUDGET    bytes of DECODED heap allowed for an in-memory sort (default 64MB)
#   GOVDATA_SORT_EXPANSION disk->heap expansion factor (default 24; code default is 12)
#   GOVDATA_SPILL_RECORDS  records per spilled run (default 10000; 50000 in pass 2)
#   GOVDATA_HEAL_LOG_DIR   where full per-table logs and heap dumps go
#                          (default govdata/logs/heal-sort-final — deliberately NOT /tmp,
#                           which is wiped on restart and lost pass 2's evidence)
#
# The defaults here ARE the escalation pass 2 tells you to try by hand. If a table still fails
# with an OutOfMemoryError at these settings, the next lever is GOVDATA_SPILL_RECORDS=2500
# rather than more heap: a 24g heap that cannot hold one 10k-record buffer is not short of
# heap, it has rows far wider than the spill size assumes.
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

# The 3 that pass 2 could not finish. Ordered cheapest-first so the smaller rewrites land
# before financial_line_items spends ~20+ minutes.
#
# Fields: schema|table|sort order|note
TABLES=(
  "sec|mda_sections|cik,accession_number,section,paragraph_number|"
  "sec|xbrl_relationships|cik,accession_number,linkbase_type,from_concept|"
  "sec|financial_line_items|cik,accession_number,concept|largest; expect the longest run"
)

CONFIRM=""
FILTER=""
for arg in "$@"; do
  case "$arg" in
    --confirm) CONFIRM="yes" ;;
    --list)
      printf '%-6s %-26s %-52s %s\n' "SCHEMA" "TABLE" "SORT ORDER" "NOTE"
      for entry in "${TABLES[@]}"; do
        IFS='|' read -r sc tb so nt <<< "$entry"
        printf '%-6s %-26s %-52s %s\n' "$sc" "$tb" "$so" "$nt"
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
case "$GOVDATA_PARQUET_DIR" in
  *r2*|*cloudflarestorage*)
    echo "ERROR: GOVDATA_PARQUET_DIR looks like R2 ($GOVDATA_PARQUET_DIR)." >&2
    echo "       Heal rewrites the SOURCE warehouse (MinIO)." >&2
    exit 1
    ;;
esac

JAR="$(resolve_classpath)"

# Fail early on a stale jar rather than after the first table dies confusingly.
#
# The output is captured BEFORE grepping, deliberately. Piping java straight into grep looks
# equivalent and is not: the runner exits non-zero when it prints usage, and under `set -o
# pipefail` the pipeline reports THAT status rather than grep's, so the guard fired
# unconditionally and reported every jar as stale — including correct ones.
_usage_out="$(java -cp "$JAR" org.apache.calcite.adapter.govdata.etl.IcebergMaintenanceRunner 2>&1 || true)"
if ! printf '%s' "$_usage_out" | grep -q -- "--heal-sort"; then
  echo "ERROR: staged jar has no --heal-sort: $JAR" >&2
  echo "       ./gradlew :govdata:shadowJar && \\" >&2
  echo "       cp govdata/build/libs/sih-govdata-*.jar govdata/build/libs/sih-govdata.jar" >&2
  exit 1
fi

HEAP="${GOVDATA_HEAP:-24g}"
# Unlike pass 2, these three have real defaults rather than being unset: this pass exists
# precisely to run at the escalated settings, so leaving them to the code defaults would make it
# a slower rerun of pass 2.
BUDGET="${GOVDATA_SORT_BUDGET:-67108864}"
EXPANSION="${GOVDATA_SORT_EXPANSION:-24}"
SPILL="${GOVDATA_SPILL_RECORDS:-10000}"

# Durable, NOT /tmp: pass 2 dumped to /tmp and the evidence was gone by the time anyone looked.
LOG_DIR="${GOVDATA_HEAL_LOG_DIR:-$GOVDATA_ROOT/logs/heal-sort-final}"
mkdir -p "$LOG_DIR"
RUN_STAMP="$(date +%Y%m%d-%H%M%S)"

JVM_PROPS="-Dcalcite.iceberg.sort.memory.budget.bytes=$BUDGET"
JVM_PROPS="$JVM_PROPS -Dcalcite.iceberg.sort.expansion.factor=$EXPANSION"
JVM_PROPS="$JVM_PROPS -Dcalcite.iceberg.sort.spill.records=$SPILL"
JVM_PROPS="$JVM_PROPS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_DIR"

WAREHOUSE_BASE="${GOVDATA_PARQUET_DIR/#s3:\/\//s3a://}"

S3_ARGS=""
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  S3_ARGS="--s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY"
  if [ -n "${AWS_ENDPOINT_OVERRIDE:-}" ]; then
    S3_ARGS="$S3_ARGS --s3-endpoint $AWS_ENDPOINT_OVERRIDE"
  fi
fi

# Name the reason a table failed, from its own log, so the summary is actionable instead of
# just "rc=1". Ordered most-specific first.
classify_failure() {
  local log="$1"
  if grep -q "OutOfMemoryError" "$log" 2>/dev/null; then
    echo "OOM — lower GOVDATA_SPILL_RECORDS (try 2500) before raising heap"
  elif grep -qiE "NoSuchTable|does not exist|no such table|table not found" "$log" 2>/dev/null; then
    echo "table missing in this warehouse — check the warehouse path and the schema YAML"
  elif grep -qiE "AccessDenied|InvalidAccessKeyId|SignatureDoesNotMatch|Forbidden" "$log" 2>/dev/null; then
    echo "S3 auth/endpoint rejected — check AWS_* in .env.prod"
  elif grep -qiE "Connection refused|UnknownHost|timed out|timeout" "$log" 2>/dev/null; then
    echo "MinIO unreachable at $AWS_ENDPOINT_OVERRIDE"
  elif grep -qiE "already sorted|sorted-by" "$log" 2>/dev/null; then
    echo "possibly already healed — inspect the log before rerunning"
  else
    echo "unclassified — read the log"
  fi
}

echo "============================================================"
echo "Heal pass 3 (final) — the 3 tables pass 2 could not finish"
echo "============================================================"
echo "  Warehouse:        $WAREHOUSE_BASE"
echo "  Jar:              $JAR"
echo "  Heap:             $HEAP"
echo "  Sort budget:      $BUDGET bytes (decoded)"
echo "  Expansion factor: $EXPANSION"
echo "  Spill records:    $SPILL"
echo "  Logs + dumps:     $LOG_DIR"
[ -n "$FILTER" ] && echo "  Filter:           $FILTER"
if [ -z "$CONFIRM" ]; then
  echo "  Mode:             DRY RUN (pass --confirm to rewrite)"
else
  echo "  Mode:             REWRITING"
fi
echo ""

total=0; ok=0; failed=0
FAILURE_REPORT=""

for entry in "${TABLES[@]}"; do
  IFS='|' read -r schema table sort_order note <<< "$entry"

  if [ -n "$FILTER" ] && [ "$table" != "$FILTER" ]; then
    continue
  fi

  total=$((total + 1))
  echo ">>> ${schema}.${table}  [$sort_order]"
  [ -n "$note" ] && echo "    note: $note"

  LOG="$LOG_DIR/${schema}.${table}-${RUN_STAMP}.log"
  echo "    log:  $LOG"
  started=$(date +%s)

  DRY_ARG=""
  [ -z "$CONFIRM" ] && DRY_ARG="--dry-run"

  set +e
  # Full output to the log; only the noise-filtered view goes to the terminal. This is the
  # difference from pass 2, where the filtered view WAS the only copy and a failure left
  # nothing to read.
  # shellcheck disable=SC2086
  java -Xms1g -Xmx"$HEAP" $JVM_PROPS \
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
    > "$LOG" 2>&1
  rc=$?
  set -e

  grep -vE "^log4j:|WARN.*hadoop|WARN.*Metrics|^$" "$LOG" | tail -40 || true

  elapsed=$(( $(date +%s) - started ))
  if [ $rc -eq 0 ]; then
    ok=$((ok + 1))
    echo "    done in ${elapsed}s"
  else
    failed=$((failed + 1))
    reason="$(classify_failure "$LOG")"
    FAILURE_REPORT="${FAILURE_REPORT}    - ${schema}.${table} (rc=$rc after ${elapsed}s)
        reason: ${reason}
        log:    ${LOG}
"
    echo "    FAILED (rc=$rc after ${elapsed}s) — $reason"
    echo "    full log: $LOG"
    echo "    continuing"
  fi
  echo ""
done

echo "============================================================"
echo "  Tables attempted: $total"
echo "  Completed:        $ok"
echo "  Failed:           $failed"
if [ $failed -gt 0 ]; then
  printf '%s' "$FAILURE_REPORT"
  echo "  Heap dumps (if any): $LOG_DIR/*.hprof"
  echo ""
  echo "  These are already the escalated settings. Next lever for a repeat OOM is a smaller"
  echo "  spill buffer, not more heap:"
  echo "    GOVDATA_SPILL_RECORDS=2500 $0 --confirm <table>"
fi
echo "============================================================"

if [ -n "$CONFIRM" ] && [ $failed -eq 0 ]; then
  echo ""
  echo "All 13 tables healed. Next:"
  echo "  $SCRIPT_DIR/verify-healed-tables.sh sec"
  echo "  then $SCRIPT_DIR/sync-healed-tables-to-r2.sh --confirm"
fi

[ $failed -eq 0 ]
