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
# Consolidated ETL worker.  Replaces all numbered worker-NN.sh shims.
#
# Usage: worker.sh <schema> <mode> [--force]
#
# Simple schemas (inline model, single ETL run):
#   sec_primary   <year|current>   — SEC 10-K/10-Q filings for one year
#   sec_secondary <year|current>   — SEC 8-K, proxy, insider, 13F, 13D/G
#   sec_prices    <daily|historical> — Stock prices via Stooq (year-range fixed)
#   econ          <historical|daily>
#   census        <historical|daily>
#   geo           <historical|daily>
#   crime         <historical|daily>
#   weather       <historical|daily>
#   ref           <daily|historical> — GLEIF + FIGI reference (year-agnostic)
#   fec           <historical|daily>
#   fedregister   <historical|daily>
#   econ_reference <daily>          — BLS area/industry codes (year-agnostic)
#
# Complex schemas (delegate to specialty worker scripts):
#   cyber_threat  <historical|daily>
#   cyber_vuln    <historical|daily>
#   health  <historical|daily>
#   edu     <historical|daily>
#   energy  <historical|daily>
#   patents <historical|daily>
#   lands   <historical|daily>
#   cftc    <historical|daily>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

SCHEMA="${1:-}"
MODE="${2:-}"

if [ -z "$SCHEMA" ] || [ -z "$MODE" ]; then
  echo "Usage: $0 <schema> <mode> [--force]" >&2
  echo "  Schemas: sec_primary, sec_secondary, sec_prices, econ, census, geo, crime," >&2
  echo "           weather, ref, fec, fedregister, econ_reference," >&2
  echo "           cyber_threat, cyber_vuln, health, edu, energy, patents, lands" >&2
  exit 1
fi

FORCE=${FORCE:-false}
for arg in "${@:3}"; do
  [ "$arg" = "--force" ] && FORCE=true
done
export FORCE

# DQ modes — schema-agnostic; delegate directly to worker-dq-run.sh
case "$MODE" in
  dq)
    exec "$SCRIPT_DIR/worker-dq-run.sh" "$SCHEMA" --mode historical
    ;;
  dq-rebuild)
    export GOVDATA_RUN_MODE="historical"
    exec "$SCRIPT_DIR/worker-dq-run.sh" "$SCHEMA" --mode historical --rebuild --include-daily
    ;;
  dq-etl-resume)
    # ETL resume: continue ETL (tracker skips completed partitions), then DQ.
    # No teardown of Iceberg/MinIO/dq-results/etl-tracker.
    export GOVDATA_RUN_MODE="historical"
    exec "$SCRIPT_DIR/worker-dq-run.sh" "$SCHEMA" --mode historical --etl-resume --include-daily
    ;;
esac

WORKER_ID="worker-${SCHEMA}-${MODE}"
INCREMENTAL_YEAR=${GOVDATA_INCREMENTAL_START_YEAR:-$(date +%Y)}

case "$SCHEMA" in

  # ── SEC primary (10-K / 10-Q) — one year per invocation ──────────────────

  sec_primary)
    case "$MODE" in
      current)              YEAR=$(date +%Y) ;;
      [0-9][0-9][0-9][0-9]) YEAR="$MODE" ;;
      *) echo "sec_primary: mode must be a 4-digit year or 'current'" >&2; exit 1 ;;
    esac
    export GOVDATA_START_YEAR="$YEAR"
    export GOVDATA_END_YEAR="$YEAR"
    run_etl_inline "$(build_inline_model sec \
      '"ciks":"_ALL_EDGAR_FILERS","filingTypes":["10-K","10-K/A","10-Q","10-Q/A"],"fetchStockPrices":false')" \
      "$WORKER_ID"
    ;;

  # ── SEC secondary (8-K, proxy, insider, 13F, 13D/G) — one year ────────────

  sec_secondary)
    case "$MODE" in
      current)              YEAR=$(date +%Y) ;;
      [0-9][0-9][0-9][0-9]) YEAR="$MODE" ;;
      *) echo "sec_secondary: mode must be a 4-digit year or 'current'" >&2; exit 1 ;;
    esac
    export GOVDATA_START_YEAR="$YEAR"
    export GOVDATA_END_YEAR="$YEAR"
    run_etl_inline "$(build_inline_model sec \
      '"ciks":"_ALL_EDGAR_FILERS","filingTypes":["8-K","8-K/A","DEF 14A","3","4","5","13F-HR","13F-HR/A","SC 13D","SC 13D/A","SC 13G","SC 13G/A"],"fetchStockPrices":false')" \
      "$WORKER_ID"
    ;;

  # ── Stock prices (Stooq) — fixed year range regardless of mode ────────────

  sec_prices)
    export GOVDATA_START_YEAR=2010
    export GOVDATA_END_YEAR=2026
    # DQ mode samples the same representative CIK group as the sec filings arm (see above);
    # prod fetches Stooq prices for the full EDGAR universe.
    if [ "${GOVDATA_DQ:-}" = "true" ]; then
      SEC_PRICES_CIKS="${GOVDATA_DQ_CIKS:-MAGNIFICENT7}"
    else
      SEC_PRICES_CIKS="_ALL_EDGAR_FILERS"
    fi
    run_etl_inline "$(build_inline_model sec \
      '"ciks":"'"$SEC_PRICES_CIKS"'","fetchStockPrices":true,"stockPriceSource":"stooq","filingTypes":[]')" \
      "$WORKER_ID"
    ;;

  # ── SEC (filings) — universal historical|daily entry point ────────────────
  # Translates the pool/DQ-harness historical|daily vocabulary into SEC's per-year
  # filing fetches, so `worker.sh sec historical|daily` (and run-all-dq --schema sec)
  # works like every other schema. sec is one data source served by two filing-type
  # passes (primary 10-K/10-Q, secondary 8-K/proxy/insider/13F/13D-G); sec_prices
  # (Stooq) stays its own slot. The standalone sec_primary:<year> / sec_secondary:<year>
  # slots remain for the pool's parallel prod backfill — this arm runs years
  # sequentially, which is fine for the narrow DQ window (GOVDATA_START_YEAR carries it).
  sec)
    # CIK scope: DQ mode (GOVDATA_DQ=true) samples a small representative group instead of
    # the full ~7,800-filer EDGAR universe — mega-caps file every form type (10-K/10-Q/8-K/
    # DEF 14A/3,4,5/13F/13D-G), so coverage is exercised in minutes, not the ~12h full run.
    # Override the DQ group via GOVDATA_DQ_CIKS (any CikRegistry group/ticker, e.g. FAANG).
    if [ "${GOVDATA_DQ:-}" = "true" ]; then
      SEC_CIKS="${GOVDATA_DQ_CIKS:-MAGNIFICENT7}"
    else
      SEC_CIKS="_ALL_EDGAR_FILERS"
    fi
    SEC_PRIMARY_FILINGS='"ciks":"'"$SEC_CIKS"'","filingTypes":["10-K","10-K/A","10-Q","10-Q/A"],"fetchStockPrices":false'
    SEC_SECONDARY_FILINGS='"ciks":"'"$SEC_CIKS"'","filingTypes":["8-K","8-K/A","DEF 14A","3","4","5","13F-HR","13F-HR/A","SC 13D","SC 13D/A","SC 13G","SC 13G/A"],"fetchStockPrices":false'
    case "$MODE" in
      historical) SEC_START_YEAR="${GOVDATA_START_YEAR:-2010}"; SEC_END_YEAR=$((INCREMENTAL_YEAR - 1)) ;;
      daily)      SEC_START_YEAR="$INCREMENTAL_YEAR";           SEC_END_YEAR="$INCREMENTAL_YEAR" ;;
      *) echo "sec: unknown mode '$MODE'. Valid modes: historical, daily" >&2; exit 1 ;;
    esac
    for (( SEC_YEAR=SEC_END_YEAR; SEC_YEAR>=SEC_START_YEAR; SEC_YEAR-- )); do
      export GOVDATA_START_YEAR="$SEC_YEAR"
      export GOVDATA_END_YEAR="$SEC_YEAR"
      run_etl_inline "$(build_inline_model sec "$SEC_PRIMARY_FILINGS")"   "worker-sec-${MODE}-primary-${SEC_YEAR}"
      run_etl_inline "$(build_inline_model sec "$SEC_SECONDARY_FILINGS")" "worker-sec-${MODE}-secondary-${SEC_YEAR}"
    done
    ;;

  # ── Simple year-range schemas ──────────────────────────────────────────────
  # econ, census, crime, weather, energy share historical/daily logic.
  # census adds enabledSources; all daily runs pass currentMonth. Per-table
  # cadence (energy's weekly/monthly/annual mix) lives in the schema YAML
  # dimensions (month cache-buster), not in worker flags.

  econ|census|crime|weather|energy)
    case "$MODE" in
      historical)
        export GOVDATA_START_YEAR="${GOVDATA_START_YEAR:-2010}"
        export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
        EXTRA=""
        ;;
      daily)
        export GOVDATA_START_YEAR="$INCREMENTAL_YEAR"
        export GOVDATA_END_YEAR=""
        CURRENT_MONTH=$(date +%m)
        EXTRA="\"currentMonth\":\"${CURRENT_MONTH}\""
        ;;
      *) echo "${SCHEMA}: unknown mode '$MODE'. Valid modes: historical, daily" >&2; exit 1 ;;
    esac
    case "$SCHEMA" in
      census) EXTRA="${EXTRA:+${EXTRA},}\"enabledSources\":[\"acs\"]" ;;
      crime)
        # Optional source fence for crime (e.g. GOVDATA_ENABLED_SOURCES=bjs to re-ingest
        # only BJS tables and skip the slow CDE agency sweep). When unset, no operand is
        # added and all sources run. The env fallback in GovDataSchemaFactory does not reach
        # the inline EtlRunner path, so thread it into the operand here where CrimeSchemaFactory
        # consumes it via isEnabled hooks.
        if [ -n "${GOVDATA_ENABLED_SOURCES:-}" ]; then
          _src_json=$(printf '%s' "$GOVDATA_ENABLED_SOURCES" \
            | awk -F, '{for(i=1;i<=NF;i++){printf (i>1?",":"")"\""$i"\""}}')
          EXTRA="${EXTRA:+${EXTRA},}\"enabledSources\":[${_src_json}]"
        fi
        ;;
    esac
    run_etl_inline "$(build_inline_model "$SCHEMA" "$EXTRA")" "$WORKER_ID"
    ;;

  # ── Geographic (TIGER + HUD) ───────────────────────────────────────────────

  geo)
    case "$MODE" in
      historical)
        export GOVDATA_START_YEAR="${GOVDATA_START_YEAR:-2010}"
        export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
        ;;
      daily)
        export GOVDATA_START_YEAR="$INCREMENTAL_YEAR"
        export GOVDATA_END_YEAR=""
        ;;
      *) echo "geo: unknown mode '$MODE'. Valid modes: historical, daily" >&2; exit 1 ;;
    esac
    run_etl_inline "$(build_inline_model geo '"enabledSources":["tiger","hud"]')" "$WORKER_ID"
    ;;

  # ── Reference identifiers (GLEIF + FIGI) — year-agnostic ─────────────────

  ref)
    discover_gleif_url
    run_etl_inline "$(build_inline_model ref)" "$WORKER_ID"
    ;;

  # ── FEC campaign finance ───────────────────────────────────────────────────

  fec)
    case "$MODE" in
      historical) export GOVDATA_START_YEAR="${GOVDATA_START_YEAR:-2010}" ;;
      daily)      export GOVDATA_START_YEAR="$INCREMENTAL_YEAR" ;;
      *) echo "fec: unknown mode '$MODE'. Valid modes: historical, daily" >&2; exit 1 ;;
    esac
    run_etl_inline "$(build_inline_model fec)" "$WORKER_ID"
    ;;

  # ── Federal Register ──────────────────────────────────────────────────────

  fedregister)
    case "$MODE" in
      historical)
        export GOVDATA_START_YEAR="${GOVDATA_START_YEAR:-2010}"
        export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
        EXTRA=""
        ;;
      daily)
        export GOVDATA_START_YEAR="$INCREMENTAL_YEAR"
        export GOVDATA_END_YEAR=""
        CURRENT_MONTH=$(date +%m)
        EXTRA="\"currentMonth\":\"${CURRENT_MONTH}\""
        ;;
      *) echo "fedregister: unknown mode '$MODE'. Valid modes: historical, daily" >&2; exit 1 ;;
    esac
    run_etl_inline "$(build_inline_model fedregister "$EXTRA")" "$WORKER_ID"
    ;;

  # ── BLS area/industry reference codes — year-agnostic ─────────────────────

  econ_reference)
    run_etl_inline "$(build_inline_model econ_reference)" "$WORKER_ID"
    ;;

  # ── Complex multi-sub-run schemas — delegated to specialty scripts ─────────
  # These have internal release-window checks and multiple sequential ETL runs
  # that depend on enabled-table partitioning; keep their logic in-place.

  cyber_threat|cyber_vuln)
    exec "$SCRIPT_DIR/worker-cyber.sh" "$MODE" "$SCHEMA"
    ;;

  health)
    exec "$SCRIPT_DIR/worker-health.sh" "$MODE"
    ;;

  edu)
    exec "$SCRIPT_DIR/worker-edu.sh" "$MODE"
    ;;

  patents)
    exec "$SCRIPT_DIR/worker-patents.sh" "$MODE"
    ;;

  lands)
    exec "$SCRIPT_DIR/worker-lands.sh" "$MODE"
    ;;

  # ── CFTC swap data (daily EOD files, 2024+) ───────────────────────────────

  cftc)
    case "$MODE" in
      historical)
        export GOVDATA_START_YEAR="${GOVDATA_START_YEAR:-2024}"
        export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
        ;;
      daily)
        export GOVDATA_START_YEAR="$INCREMENTAL_YEAR"
        export GOVDATA_END_YEAR=""
        ;;
      *) echo "cftc: unknown mode '$MODE'. Valid modes: historical, daily" >&2; exit 1 ;;
    esac
    run_etl_inline "$(build_inline_model cftc)" "$WORKER_ID"
    ;;

  *)
    echo "Unknown schema: $SCHEMA" >&2
    echo "Valid schemas: sec, sec_primary, sec_secondary, sec_prices, econ, census, geo, crime," >&2
    echo "               weather, ref, fec, fedregister, econ_reference," >&2
    echo "               cyber_threat, cyber_vuln, health, edu, energy, patents, lands, cftc" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
