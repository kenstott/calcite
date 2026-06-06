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
#   cyber_threat  <initial|daily|weekly|hourly|static>
#   cyber_vuln    <initial|daily|weekly|hourly|static>
#   health  <initial|daily|weekly|monthly>
#   edu     <initial|annual|biennial>
#   energy  <initial|weekly|monthly|annual>
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
    run_etl_inline "$(build_inline_model sec \
      '"ciks":"_ALL_EDGAR_FILERS","fetchStockPrices":true,"stockPriceSource":"stooq","filingTypes":[]')" \
      "$WORKER_ID"
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
    echo "Valid schemas: sec_primary, sec_secondary, sec_prices, econ, census, geo, crime," >&2
    echo "               weather, ref, fec, fedregister, econ_reference," >&2
    echo "               cyber_threat, cyber_vuln, health, edu, energy, patents, lands, cftc" >&2
    exit 1
    ;;
esac

log_info "$WORKER_ID complete"
