#!/usr/bin/env bash
# heal-sort-order-retry.sh — retry the large SEC tables that OOM'd on the first heal pass.
#
# ── WHAT HAPPENED ────────────────────────────────────────────────────────────────────────────
# The first run healed 8 of 13 tables and failed on 5, all large and string-heavy, dying inside
# the Parquet string decoder while buffering rows to sort.
#
# The cause was a sizing bug, now fixed: the decision between an in-memory sort and the external
# merge compared `fileSizeInBytes()` — COMPRESSED, dictionary-encoded, columnar Parquet — against
# a HEAP budget. Decoded Java Records with real String instances are an order of magnitude larger
# than their on-disk form for text-heavy tables, so a ~400MB partition passed a 512MB check and
# then exhausted the heap decoding it. The 8 tables that succeeded were simply small enough that
# the wrong comparison did not matter.
#
# The routing now estimates decoded heap (disk bytes x an expansion factor, default 12) before
# choosing, and the external merge spills more often (50k records per run, was 200k). This
# script also gives the JVM more headroom than the default 4g.
#
# ── SAFE TO RE-RUN ───────────────────────────────────────────────────────────────────────────
# The failed tables were left consistent. A rewrite commits atomically per partition, so a
# partition that failed was never committed, and `aperio.sorted-by` is written only after EVERY
# partition of a table succeeds — so none of these recorded a sort order and all are eligible
# again.
#
# One honest caveat: partitions that DID complete before the failure were committed. Those
# tables are part-sorted, and this rerun will rewrite every partition again, including the ones
# already done. That is wasted work but not incorrect — and it is why the tables are not marked.
#
# The 8 tables that succeeded ARE marked and will be skipped by heal-sort-order.sh, so there is
# no need to exclude them by hand.
#
# ── USAGE ────────────────────────────────────────────────────────────────────────────────────
#   govdata/scripts/parallel/heal-sort-order-retry.sh            # DRY RUN
#   govdata/scripts/parallel/heal-sort-order-retry.sh --confirm  # rewrite
#   govdata/scripts/parallel/heal-sort-order-retry.sh --confirm mda_sections   # one table
#   govdata/scripts/parallel/heal-sort-order-retry.sh --list
#
# Env:
#   GOVDATA_HEAP           JVM max heap (default 12g here, vs 4g in the main script)
#   GOVDATA_SORT_BUDGET    bytes of DECODED heap allowed for an in-memory sort (default 512MB)
#   GOVDATA_SORT_EXPANSION disk->heap expansion factor (default 12; raise if it still OOMs)
#   GOVDATA_SPILL_RECORDS  records per spilled run (default 50000; lower for very wide rows)
#
# If a table still OOMs, lower GOVDATA_SORT_BUDGET (forces the external merge sooner) and/or
# GOVDATA_SPILL_RECORDS (smaller pass-one buffers) before raising heap further.
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

# The tables that failed, smallest first so a sizing problem surfaces on a cheap table rather than
# after hours on financial_line_items.
TABLES=(
  "sec|filing_contexts|cik,accession_number,context_id"
  "sec|institutional_holdings|cik,report_period,cusip"
  "sec|xbrl_relationships|cik,accession_number,linkbase_type,from_concept"
  "sec|mda_sections|cik,accession_number,section,paragraph_number"
  # sec.vectorized_chunks removed 2026-08-17. It was in this list and failed in 3s on every
  # attempt — not an OOM like the others, but a retired write target
  # (materialize.enabled: false). See the note in heal-sort-order.sh.
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

HEAP="${GOVDATA_HEAP:-12g}"
BUDGET="${GOVDATA_SORT_BUDGET:-}"
EXPANSION="${GOVDATA_SORT_EXPANSION:-}"
SPILL="${GOVDATA_SPILL_RECORDS:-}"

JVM_PROPS=""
[ -n "$BUDGET" ]    && JVM_PROPS="$JVM_PROPS -Dcalcite.iceberg.sort.memory.budget.bytes=$BUDGET"
[ -n "$EXPANSION" ] && JVM_PROPS="$JVM_PROPS -Dcalcite.iceberg.sort.expansion.factor=$EXPANSION"
[ -n "$SPILL" ]     && JVM_PROPS="$JVM_PROPS -Dcalcite.iceberg.sort.spill.records=$SPILL"

# Dump the heap on OOM so a repeat failure is diagnosable instead of just a stack trace.
HEAP_DUMP_DIR="${GOVDATA_HEAP_DUMP_DIR:-/tmp}"
JVM_PROPS="$JVM_PROPS -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$HEAP_DUMP_DIR"

WAREHOUSE_BASE="${GOVDATA_PARQUET_DIR/#s3:\/\//s3a://}"

S3_ARGS=""
if [ -n "${AWS_ACCESS_KEY_ID:-}" ]; then
  S3_ARGS="--s3-access-key $AWS_ACCESS_KEY_ID --s3-secret-key $AWS_SECRET_ACCESS_KEY"
  if [ -n "${AWS_ENDPOINT_OVERRIDE:-}" ]; then
    S3_ARGS="$S3_ARGS --s3-endpoint $AWS_ENDPOINT_OVERRIDE"
  fi
fi

echo "============================================================"
echo "Heal retry — the tables that OOM'd on pass 1"
echo "============================================================"
echo "  Warehouse: $WAREHOUSE_BASE"
echo "  Jar:       $JAR"
echo "  Heap:      $HEAP"
[ -n "$BUDGET" ]    && echo "  Sort budget:     $BUDGET bytes (decoded)"
[ -n "$EXPANSION" ] && echo "  Expansion factor: $EXPANSION"
[ -n "$SPILL" ]     && echo "  Spill records:    $SPILL"
echo "  Heap dumps: $HEAP_DUMP_DIR"
[ -n "$FILTER" ] && echo "  Filter:    $FILTER"
if [ -z "$CONFIRM" ]; then
  echo "  Mode:      DRY RUN (pass --confirm to rewrite)"
else
  echo "  Mode:      REWRITING"
fi
echo ""

total=0; ok=0; failed=0
FAILED_TABLES=()

for entry in "${TABLES[@]}"; do
  IFS='|' read -r schema table sort_order <<< "$entry"

  if [ -n "$FILTER" ] && [ "$table" != "$FILTER" ]; then
    continue
  fi

  total=$((total + 1))
  echo ">>> ${schema}.${table}  [$sort_order]"
  started=$(date +%s)

  DRY_ARG=""
  [ -z "$CONFIRM" ] && DRY_ARG="--dry-run"

  set +e
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
    2>&1 | grep -vE "^log4j:|WARN.*hadoop|WARN.*Metrics|^$"
  rc=${PIPESTATUS[0]}
  set -e

  elapsed=$(( $(date +%s) - started ))
  if [ $rc -eq 0 ]; then
    ok=$((ok + 1))
    echo "    done in ${elapsed}s"
  else
    failed=$((failed + 1))
    FAILED_TABLES+=("${schema}.${table} (rc=$rc after ${elapsed}s)")
    echo "    FAILED (rc=$rc after ${elapsed}s) — continuing"
  fi
  echo ""
done

echo "============================================================"
echo "  Tables attempted: $total"
echo "  Completed:        $ok"
echo "  Failed:           $failed"
if [ ${#FAILED_TABLES[@]} -gt 0 ]; then
  printf '    - %s\n' "${FAILED_TABLES[@]}"
  echo ""
  echo "  Still failing? Force the external merge earlier and spill smaller:"
  echo "    GOVDATA_SORT_BUDGET=67108864 GOVDATA_SPILL_RECORDS=10000 \\"
  echo "      $0 --confirm <table>"
  echo "  Check $HEAP_DUMP_DIR for a .hprof if it was an OutOfMemoryError."
fi
echo "============================================================"

if [ -n "$CONFIRM" ] && [ $failed -eq 0 ]; then
  echo ""
  echo "Next:"
  echo "  $SCRIPT_DIR/verify-healed-tables.sh sec"
  echo "  then $SCRIPT_DIR/sync-healed-tables-to-r2.sh --confirm"
fi

[ $failed -eq 0 ]
