# AskAmerica market tools: relaunch notes

Written 2026-10-02 to restart this thread with minimal context. The full record is
`docs/askamerica-market-tools-plan.md`; its section "Where this landed (2026-10-02)" holds
the numbers behind every statement here.

## What this is

Prediction-market mispricing and arbitrage as AskAmerica engine MCP tools (not Claude
skills). Three decisions govern it:

- Every tool reads both Kalshi and Polymarket.
- Mispricing is the primitive. Arbitrage is a basket of correlated events at a price that
  improves the odds of a positive outcome; a strict lock is the limiting case.
- Fees are approximated and charged on every leg.

## Where the code is

Module `askamerica-engine` (Java 21), package `org.apache.calcite.adapter.askamerica`.

| File | Holds |
|---|---|
| `PredictionMarkets.java` | Venue listing, drivers, `find_market_candidates` |
| `MarketPricing.java` | `price_market_event`, `price_market_basket`, conditions, `DailyExtreme` |
| `MarketForecasts.java` | `forecast_market_event` |
| `MarketBacktest.java` | `backtest_market_forecast` |
| `MarketHistory.java` | `market_price_history`, settled markets, order books |
| `MarketRules.java` | `compare_settlement_rules` |
| `MarketBaskets.java` | `find_market_baskets`: recipes cross_venue, same_place, linked_drivers, series_run, range, calendar |
| `MarketScan.java` | `scan_market_opportunities`, `requote_market_opportunity` |
| `MarketBasketScan.java` | `scan_market_baskets` |
| `src/main/resources/recipes.json` | Recipes `prediction-market-*` |

Tests sit beside them under `src/test/java/...`; `MarketBasketScanTest` has 54 tests. Tool
descriptions are capped at 2048 characters and `scan_market_baskets` is at about 2033, so
new explanation goes in output fields, recipes or the plan doc.

## State on 2026-10-02

- `main` at `a03539f82` or later. Engine suite: 888 tests, 0 failures, 1 skipped.
- Last engine change: `eccd0891b`, daily temperature pairs by station and day, reported as
  `two_measurements`.
- Open sourcing issues: govdata-ops #853 (sub-daily station observations), #851 (BLS
  average-price series).

## What was found

- Strict locks exist and are worth cents to a few dollars. Depth is the limit, not capital.
- Near-locks are small expected-value bets (best: an expected $8.81 on $249).
- Large cross-venue gaps were each a rule difference, a converted quantity, or two
  measurements of one event.
- Daily temperature gaps recur every day (about $900 of capital, $76 if the two records
  agree, on 2026-10-02) but the records disagree: Kalshi's settled temperature fell inside
  Polymarket's winning bucket on 66.8% of 1,125 station-days, and Polymarket's record was
  the lower one on 368 of 373 misses. The lock reading is wrong; whether an edge remains is
  not known.
- All sizing assumes crossing the spread. Resting orders are not modelled.

## Next steps, none started

1. **Price `two_measurements` pairs under the measured offset.** Replace "assume the records
   agree" with the per-station distribution of (Kalshi record − Polymarket record), giving
   an expected value and a chance of loss. The offset can come from settled events at
   bucket level now, or exactly from sub-daily observations once #853 is sourced.
   `MarketHistory.settledMarkets` already reads both venues; Kalshi's `expiration_value` is
   the settled temperature.
2. **Price `linked_drivers` baskets.** Carry one market's implied distribution through the
   historical relationship and compare with the other market. First candidate: Texas daily
   highs against Kalshi's daily ERCOT peak demand series (ticker KXTXERCOTPEAKD): tight physical link,
   settles daily, about 640 contracts a day. Needs ERCOT load history; check the catalog
   properly before filing a sourcing issue.
3. **Resting-order pricing.** Cost per set at the bid, fill odds, maker fees.
4. Smaller: default `min_days: 1` hides the daily temperature pairs; a per-level ladder in
   `size`; "suspect" edges in `scan_market_opportunities`; scheduled scans.

## How to run

```bash
# Unit suite. Never run it bare on this Mac: integration tests hit the live server.
./gradlew :askamerica-engine:test '-PincludeTags=!integration'

# Build the jar and install it as the eval jar. Rename onto the path, never copy over
# it, and never while an MCP run is live.
./gradlew :askamerica-engine:shadowJar
J=askamerica-engine/build/libs/askamerica-engine-1.42.0-SNAPSHOT.jar
D=~/.askamerica/engine-eval/askamerica-engine.jar
cp $J $D.tmp.$$ && mv -f $D.tmp.$$ $D; cksum $J $D
```

Scripts and outputs from this session are in `scratch/market-tools/` (local, untracked;
they drive the eval jar over stdio through
`.claude/skills/askamerica-comparative-eval/tools/askengine_stdio.py`):

| File | Does |
|---|---|
| `smoke10.py` | `scan_market_baskets` with `max_events: 400`, writes `basket_scan_all.json` and the dashboard image; about 4 minutes |
| `smoke_temp.py` | The same with `driver: temperature, min_days: 0`, writes `basket_scan_temp.json` |
| `q.py` | One tool call: `python3 q.py 'tool_name::{json arguments}'` |
| `settle_cmp.py`, `settle_an.py` | The settled-events comparison: reads the pairs from `basket_scan_temp.json`, pulls both venues' settled history, prints agreement per city |

## Prompt to relaunch

> Read `docs/askamerica-market-tools-relaunch.md` and the "Where this landed" section of
> `docs/askamerica-market-tools-plan.md`. Then start on next step N.
