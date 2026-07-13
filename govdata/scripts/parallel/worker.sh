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
# Usage: worker.sh <schema> <mode>
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
#   ag      <historical|daily>   — USDA NASS/ERS/RMA/FSA
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"
load_env

SCHEMA="${1:-}"
MODE="${2:-}"

if [ -z "$SCHEMA" ] || [ -z "$MODE" ]; then
  echo "Usage: $0 <schema> <mode>" >&2
  echo "  Schemas: sec_primary, sec_secondary, sec_prices, econ, census, geo, crime," >&2
  echo "           weather, ref, fec, fedregister, econ_reference," >&2
  echo "           cyber_threat, cyber_vuln, health, edu, energy, patents, lands, cftc, ag" >&2
  exit 1
fi

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

  # ── SEC secondary (8-K, proxy, insider, 13D/G) — one year ─────────────────
  #    13F-HR is handled by the separate sec_13f slot (parse-heavy; isolated so
  #    it doesn't throttle these lighter, download-bound forms).

  sec_secondary)
    # Secondary SEC forms → tables insider_transactions (3/4/5),
    # beneficial_ownership (13D/G), earnings_transcripts (8-K). Two vocabularies share this arm:
    #   • daily|historical (DQ harness)         → DQ_SAMPLE scope (or caller GOVDATA_CIKS)
    #   • <year>|current   (run-pool prod)      → _ALL_EDGAR_FILERS full universe
    SEC2_FORMS='"8-K","8-K/A","DEF 14A","3","4","5","SC 13D","SC 13D/A","SC 13G","SC 13G/A"'
    case "$MODE" in
      historical) SEC2_START="${GOVDATA_START_YEAR:-2010}"; SEC2_END=$((INCREMENTAL_YEAR - 1)); SEC2_DQ=1 ;;
      daily)      SEC2_START="$INCREMENTAL_YEAR";           SEC2_END="$INCREMENTAL_YEAR";       SEC2_DQ=1 ;;
      current)              SEC2_START=$(date +%Y); SEC2_END="$SEC2_START"; SEC2_DQ=0 ;;
      [0-9][0-9][0-9][0-9]) SEC2_START="$MODE";     SEC2_END="$MODE";       SEC2_DQ=0 ;;
      *) echo "sec_secondary: mode must be daily|historical (DQ) or a 4-digit year|current (prod)" >&2; exit 1 ;;
    esac
    if [ "$SEC2_DQ" = "1" ]; then
      SEC2_OPS="\"fetchStockPrices\":false,\"ciks\":\"${GOVDATA_CIKS:-DQ_SAMPLE}\",\"filingTypes\":[${SEC2_FORMS}]"
      for (( SEC2_YEAR=SEC2_END; SEC2_YEAR>=SEC2_START; SEC2_YEAR-- )); do
        export GOVDATA_START_YEAR="$SEC2_YEAR"
        export GOVDATA_END_YEAR="$SEC2_YEAR"
        run_etl_inline "$(build_inline_model sec "$SEC2_OPS")" "worker-sec_secondary-${MODE}-${SEC2_YEAR}"
      done
    else
      export GOVDATA_START_YEAR="$SEC2_START"
      export GOVDATA_END_YEAR="$SEC2_END"
      run_etl_inline "$(build_inline_model sec \
        "\"ciks\":\"_ALL_EDGAR_FILERS\",\"filingTypes\":[${SEC2_FORMS}],\"fetchStockPrices\":false")" \
        "$WORKER_ID"
    fi
    ;;

  # ── SEC 13F (institutional holdings) — one year; parse-heavy, isolated ─────
  #    Runs alongside sec_secondary on the same host: shares the host-wide EDGAR
  #    rate limiter so the download-bound secondary forms keep the IP budget
  #    saturated while 13F parses in parallel. CPU-bound → more entity threads.

  sec_13f)
    # 13F-HR/A → table institutional_holdings (+ filing_metadata). Same two vocabularies
    # as sec_secondary: daily|historical (DQ) vs <year>|current (run-pool prod).
    export ETL_PARALLEL_THREADS="${ETL_PARALLEL_THREADS:-4}"
    SEC13F_FORMS='"13F-HR","13F-HR/A"'
    case "$MODE" in
      historical) SEC13F_START="${GOVDATA_START_YEAR:-2010}"; SEC13F_END=$((INCREMENTAL_YEAR - 1)); SEC13F_DQ=1 ;;
      daily)      SEC13F_START="$INCREMENTAL_YEAR";           SEC13F_END="$INCREMENTAL_YEAR";       SEC13F_DQ=1 ;;
      current)              SEC13F_START=$(date +%Y); SEC13F_END="$SEC13F_START"; SEC13F_DQ=0 ;;
      [0-9][0-9][0-9][0-9]) SEC13F_START="$MODE";     SEC13F_END="$MODE";         SEC13F_DQ=0 ;;
      *) echo "sec_13f: mode must be daily|historical (DQ) or a 4-digit year|current (prod)" >&2; exit 1 ;;
    esac
    if [ "$SEC13F_DQ" = "1" ]; then
      SEC13F_OPS="\"fetchStockPrices\":false,\"ciks\":\"${GOVDATA_CIKS:-DQ_SAMPLE}\",\"filingTypes\":[${SEC13F_FORMS}]"
      for (( SEC13F_YEAR=SEC13F_END; SEC13F_YEAR>=SEC13F_START; SEC13F_YEAR-- )); do
        export GOVDATA_START_YEAR="$SEC13F_YEAR"
        export GOVDATA_END_YEAR="$SEC13F_YEAR"
        run_etl_inline "$(build_inline_model sec "$SEC13F_OPS")" "worker-sec_13f-${MODE}-${SEC13F_YEAR}"
      done
    else
      export GOVDATA_START_YEAR="$SEC13F_START"
      export GOVDATA_END_YEAR="$SEC13F_END"
      run_etl_inline "$(build_inline_model sec \
        "\"ciks\":\"_ALL_EDGAR_FILERS\",\"filingTypes\":[${SEC13F_FORMS}],\"fetchStockPrices\":false")" \
        "$WORKER_ID"
    fi
    ;;

  # ── Stock prices (Stooq) — fixed year range regardless of mode ────────────

  sec_prices)
    export GOVDATA_START_YEAR=2010
    export GOVDATA_END_YEAR=2026
    # Enables the stock_prices table (its YAML `enabled` reads this env at load time).
    export FETCH_STOCK_PRICES=true
    # The BULK pass ingests EVERY ticker in the Stooq bulk zip for all dates (NOT cik-scoped).
    # ciks scopes only the current-price TOP-UP (gap from bulk max date to today is fetched via
    # the Alpha Vantage JSON API for the configured SEC filers; DQ harness sets DQ_SAMPLE).
    PRICE_OPS='"fetchStockPrices":true,"stockPriceSource":"stooq","filingTypes":[]'
    if [ -n "${GOVDATA_CIKS:-}" ]; then
      PRICE_OPS="$PRICE_OPS,\"ciks\":\"${GOVDATA_CIKS}\""
    fi
    run_etl_inline "$(build_inline_model sec "$PRICE_OPS")" "$WORKER_ID"
    ;;

  # ── SEC (filings) — universal historical|daily entry point ────────────────
  # Translates the pool/DQ-harness historical|daily vocabulary into SEC's per-year filing
  # fetches, so `worker.sh sec historical|daily` (and run-all-dq --schema sec) works like every
  # other schema. SCOPE — which CIKs, which form types — is NOT hardcoded here: it flows through
  # the general GOVDATA_CIKS / GOVDATA_FILING_TYPES env->operand knobs (GovDataSchemaFactory) and
  # the sec-schema.yaml defaults. Examples:
  #   GOVDATA_CIKS=DQ_SAMPLE GOVDATA_FILING_TYPES=10-K,10-K/A,10-Q,10-Q/A  → DQ-sample, 10-K/10-Q only
  #   GOVDATA_CIKS=_ALL_EDGAR_FILERS                                       → full-universe prod run
  # fetchStockPrices:false keeps this the filings pass; sec_prices (Stooq) is the prices slot.
  sec)
    case "$MODE" in
      historical) _start="${GOVDATA_START_YEAR:-2010}"; _end=$((INCREMENTAL_YEAR - 1)) ;;
      daily)      _start="$INCREMENTAL_YEAR";           _end="$INCREMENTAL_YEAR" ;;
      *) echo "sec: unknown mode '$MODE'. Valid modes: historical, daily" >&2; exit 1 ;;
    esac
    # ciks + filingTypes are the SEC schema's scope operands (what SecSchemaFactory's
    # getCiksFromConfig / getFilingTypes read). The factory's GOVDATA_CIKS/GOVDATA_FILING_TYPES
    # env-wiring is not on the SEC create path, so carry the knobs onto the operand here.
    SEC_OPS='"fetchStockPrices":false'
    if [ -n "${GOVDATA_CIKS:-}" ]; then
      SEC_OPS="$SEC_OPS,\"ciks\":\"${GOVDATA_CIKS}\""
    fi
    if [ -n "${GOVDATA_FILING_TYPES:-}" ]; then
      # Split on comma only (filing types like "DEF 14A" / "SC 13D" contain spaces).
      IFS=',' read -ra _ft_arr <<< "$GOVDATA_FILING_TYPES"
      _ft_json=""
      for _ft in "${_ft_arr[@]}"; do _ft_json="$_ft_json\"$_ft\","; done
      SEC_OPS="$SEC_OPS,\"filingTypes\":[${_ft_json%,}]"
    fi
    for (( SEC_YEAR=_end; SEC_YEAR>=_start; SEC_YEAR-- )); do
      export GOVDATA_START_YEAR="$SEC_YEAR"
      export GOVDATA_END_YEAR="$SEC_YEAR"
      run_etl_inline "$(build_inline_model sec "$SEC_OPS")" "worker-sec-${MODE}-${SEC_YEAR}"
    done
    ;;

  # ── Simple year-range schemas ──────────────────────────────────────────────
  # econ, census, crime, weather, energy share historical/daily logic.
  # census adds enabledSources; all daily runs pass currentMonth. Per-table
  # cadence (energy's weekly/monthly/annual mix) lives in the schema YAML
  # dimensions (month cache-buster), not in worker flags.

  econ|census|crime|weather|energy|disasters)
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
      [0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
        # Backfill a single year (2025) or an inclusive range (2020-2023). No currentMonth —
        # a range is pure backfill; the daily refresh signal belongs only to the daily mode.
        export GOVDATA_START_YEAR="${MODE%-*}"
        export GOVDATA_END_YEAR="${MODE#*-}"
        EXTRA=""
        ;;
      *) echo "${SCHEMA}: unknown mode '$MODE'. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2; exit 1 ;;
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
      [0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
        export GOVDATA_START_YEAR="${MODE%-*}"
        export GOVDATA_END_YEAR="${MODE#*-}"
        ;;
      *) echo "geo: unknown mode '$MODE'. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2; exit 1 ;;
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
      [0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]) export GOVDATA_START_YEAR="${MODE%-*}"; export GOVDATA_END_YEAR="${MODE#*-}" ;;
      *) echo "fec: unknown mode '$MODE'. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2; exit 1 ;;
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
      [0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
        export GOVDATA_START_YEAR="${MODE%-*}"
        export GOVDATA_END_YEAR="${MODE#*-}"
        EXTRA=""
        ;;
      *) echo "fedregister: unknown mode '$MODE'. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2; exit 1 ;;
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
      [0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
        export GOVDATA_START_YEAR="${MODE%-*}"
        export GOVDATA_END_YEAR="${MODE#*-}"
        ;;
      *) echo "cftc: unknown mode '$MODE'. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2; exit 1 ;;
    esac
    run_etl_inline "$(build_inline_model cftc)" "$WORKER_ID"
    ;;

  # ── USDA agriculture (NASS, ERS, RMA, FSA) — annual, year-range ───────────
  # NASS crop/livestock, RMA crop insurance, and FSA payments carry a year
  # dimension (dataLag=1); ERS farm income is a single cumulative fetch that
  # partitions by row year and ignores the range. NASS digital record starts ~1997.

  ag)
    case "$MODE" in
      historical)
        export GOVDATA_START_YEAR="${GOVDATA_START_YEAR:-2010}"
        export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
        ;;
      daily)
        export GOVDATA_START_YEAR="$INCREMENTAL_YEAR"
        export GOVDATA_END_YEAR=""
        ;;
      [0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
        export GOVDATA_START_YEAR="${MODE%-*}"
        export GOVDATA_END_YEAR="${MODE#*-}"
        ;;
      *) echo "ag: unknown mode '$MODE'. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2; exit 1 ;;
    esac
    run_etl_inline "$(build_inline_model ag)" "$WORKER_ID"
    ;;

  # ── Housing (FHFA HPI, Census permits, HUD FMR/income limits) — annual ────
  # building_permits carries a year dimension (dataLag=1); HUD FMR/income_limits
  # fan out state×year (self-floored at 2017 via minYear); house_price_index is a
  # single snapshot file that ignores the year range.

  housing)
    case "$MODE" in
      historical)
        export GOVDATA_START_YEAR="${GOVDATA_START_YEAR:-2010}"
        export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
        ;;
      daily)
        export GOVDATA_START_YEAR="$INCREMENTAL_YEAR"
        export GOVDATA_END_YEAR=""
        ;;
      [0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
        export GOVDATA_START_YEAR="${MODE%-*}"
        export GOVDATA_END_YEAR="${MODE#*-}"
        ;;
      *) echo "housing: unknown mode '$MODE'. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2; exit 1 ;;
    esac
    run_etl_inline "$(build_inline_model housing)" "$WORKER_ID"
    ;;

  # ── Transport (NHTSA, BTS, FAA, FTA, FHWA) — mixed snapshot/annual/monthly ─
  # fatal_crashes/airline_ontime/transit_ridership/t100_segments/vehicle_registrations
  # carry a year (or year+month) dimension; vehicle_recalls/safety_complaints/airports
  # are snapshots that ignore the year range. All sources are keyless.

  transport)
    case "$MODE" in
      historical)
        export GOVDATA_START_YEAR="${GOVDATA_START_YEAR:-2010}"
        export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
        ;;
      daily)
        export GOVDATA_START_YEAR="$INCREMENTAL_YEAR"
        export GOVDATA_END_YEAR=""
        ;;
      [0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
        export GOVDATA_START_YEAR="${MODE%-*}"
        export GOVDATA_END_YEAR="${MODE#*-}"
        ;;
      *) echo "transport: unknown mode '$MODE'. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2; exit 1 ;;
    esac
    run_etl_inline "$(build_inline_model transport)" "$WORKER_ID"
    ;;

  # ── Environment (EPA AQS/TRI/GHGRP, USGS water, SDWIS, ECHO/FRS, SEMS, RCRA) ─
  # air_quality_*/tri/ghg/streamflow/water_quality carry a year (or year+state)
  # dimension; the snapshot tables (aqs_monitors, water_sites, drinking_water,
  # epa_facilities, superfund, rcra, violations) ignore the year range. All keyless.

  environment)
    case "$MODE" in
      historical)
        export GOVDATA_START_YEAR="${GOVDATA_START_YEAR:-2010}"
        export GOVDATA_END_YEAR=$((INCREMENTAL_YEAR - 1))
        ;;
      daily)
        export GOVDATA_START_YEAR="$INCREMENTAL_YEAR"
        export GOVDATA_END_YEAR=""
        ;;
      [0-9][0-9][0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9])
        export GOVDATA_START_YEAR="${MODE%-*}"
        export GOVDATA_END_YEAR="${MODE#*-}"
        ;;
      *) echo "environment: unknown mode '$MODE'. Valid modes: historical, daily, a year (2025), or a range (2020-2023)" >&2; exit 1 ;;
    esac
    run_etl_inline "$(build_inline_model environment)" "$WORKER_ID"
    ;;

  *)
    echo "Unknown schema: $SCHEMA" >&2
    echo "Valid schemas: sec, sec_primary, sec_secondary, sec_prices, econ, census, geo, crime," >&2
    echo "               weather, ref, fec, fedregister, econ_reference," >&2
    echo "               cyber_threat, cyber_vuln, health, edu, energy, patents, lands, cftc, ag," >&2
    echo "               housing, transport, environment" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
