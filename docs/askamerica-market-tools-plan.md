# AskAmerica market tools: phased plan

Goal: make "find a mispriced event" and "find a basket that locks in a yield" results a user
can trust and act on. Baseline is commit `e47bc7dbb` (four tools: `find_market_candidates`,
`price_market_event`, `find_market_baskets`, `price_market_basket`).

Package: `askamerica-engine/src/main/java/org/apache/calcite/adapter/askamerica/` (Java 21).

## Rules for every phase

- At most two teammates run at once. Both are `data-engine-dev`. A `code-reviewer` pass runs
  after they finish, never alongside them.
- Teammates own disjoint files (listed per phase). `McpServer.java` is lead-only: a teammate
  exposes `toolDef()` and a handler in their own class; the lead registers them.
- Teammates run only their own test class
  (`./gradlew :askamerica-engine:test '-PincludeTags=!integration' --tests "*Name*"`).
  The lead runs the full suite once per phase. Never a bare `:askamerica-engine:test`.
- Venue calls go through the existing `Fetcher`; unit tests use `FakeFetcher`. No live calls
  in unit tests. One `@Tag("integration")` live test per new endpoint.
- No fallback values. A missing series, period or quote is an error or an explicit flag.
- Phase exit: full suite green, eval jar installed by atomic `mv`, one `askamerica-desktop`
  run (sonnet) per affected prompt, commit and push. Release only on the user's word.

## Phase 0 — feasibility (done, 2026-10-02)

Public endpoints answered 200 with the data each later phase needs:

| Need | Kalshi | Polymarket |
|---|---|---|
| Settled events | `/markets?status=settled` (`result`, `expiration_value`) | gamma `/events?closed=true` |
| Order book | `/markets/{ticker}/orderbook` | clob `/book?token_id=` |
| Price history | `/series/{s}/markets/{ticker}/candlesticks` | clob `/prices-history?market=` |

## Phase 1 — forecast builder and venue history

Prerequisite for everything after: no later result is credible on a hand-rolled forecast.

| Teammate | Delivers | Files owned |
|---|---|---|
| A | `forecast_market_event`: from an event's rules and driver, pick settlement series, table and transform (level, MoM, YoY, max-of-path), build the distribution from catalog rows, accept `as_of` to cut history off at a date. Returns mismatch flags: seasonal adjustment, last period vs settlement period, rounding, units. | `MarketForecasts.java`, `MarketForecastsTest.java` |
| B | Venue history layer: settled events with outcome and settlement value, price history, order book, for both venues. Tool `market_price_history`. | `MarketHistory.java`, `MarketHistoryTest.java`, `MarketHistoryLiveIntegrationTest.java` |

Lead: register both tools; make `price_market_event` accept the builder's output directly;
point `next` at `forecast_market_event` instead of hand-entered samples.

Exit test: the "random mispriced event" prompt forecasts through the builder, and a report
states any mismatch flag the builder raised.

## Phase 2 — calibration and tradability

Needs Phase 1 A (`as_of`) and Phase 1 B (settled events, order book).

| Teammate | Delivers | Files owned |
|---|---|---|
| A | `backtest_market_forecast`: run the builder as of each past settled event of a series; report hit rate, Brier score, and the same for the venue's closing price. | `MarketBacktest.java`, `MarketBacktestTest.java` |
| B | Size-aware pricing: `size` argument walks the order book for the fill price; output adds dollars available at `min_edge`, days to settlement, annualized return, and the breakeven fair value at which the edge is zero. | `MarketPricing.java`, `MarketPricingSizeTest.java` |

Lead: wire a confidence tier into `price_market_event` output (lock / backtested / weak)
from the backtest and the mismatch flags.

Lead, as soon as Phase 1 lands and before Phase 2 A and B — bound the search (approved by
the user 2026-10-02). Files: `MarketScan.java`, `MarketScanTest.java`. The model vets at most
three shortlisted events per question; the draw-and-price loop stays only for events the
builder cannot resolve. The backtest filter joins when Phase 2 A lands.
`scan_market_opportunities` runs the forecast builder and pricing over every matched event
inside the engine and returns a ranked shortlist, cached like the listing. The model vets
the top few instead of drawing and forecasting one event at a time. The scan universe is
narrowed by: forecast-free signals first (structural locks, cross-venue disagreement on
matching rules), drivers where the backtest beat the closing price, no mismatch flag,
settlement within a stated number of days, and a minimum dollar capacity.

Scan, first live run (2026-10-02): 113 events matched, 60 evaluated, 10 forecast, 8 past
a 0.10 edge. Findings that set the next work:

- The largest edges are baseline-versus-market disagreements in heavily traded markets
  (monthly CPI: baseline median 0.2, market 0.52), not mispricings. Each opportunity now
  carries `market_implied_median` and `baseline_vs_market`. The backtest (Phase 2 A) is what
  decides whether the baseline has any skill against the closing price for a series; until
  it lands the scan's shortlist is a list of places to look, and says so.
- Catalog series behind the market produce false edges (WTI and the 30-year mortgage rate
  were 8 to 10 days old). The builder raises `history_stale`; the scan leaves those out.
- Markets with a few dozen contracts of volume are not counted (`min_market_volume`).
- 50 of 60 events were not forecast. By driver: treasury_yield 17, precipitation 12,
  energy prices other than WTI 5, temperature 4, policy_rate 3. Resolvers for these are
  coverage work for the builder, most valuable first.

Lead — order ticket and re-quote (added 2026-10-02). Every opportunity carries a ticket:
side, limit price (the most one can pay and still clear `min_edge` after fees), contracts
available at or under that limit, quote time, and void conditions (next release of the
settlement series, settlement date). A basket ticket adds a maximum total cost, per-leg
limits and the leg to execute first. `requote_market_opportunity` takes a ticket and
returns open / partly open (size left) / gone, the current ask and edge, and whether a
release has printed since the quote. One venue call per leg, no forecast rerun.

Phase 2 as built (2026-10-02):

- Confidence tier is `backtested` or `weak`, on every forecast edge from `price_market_event`
  and every scan opportunity. `backtested` needs an engine-built forecast, no blocking flag
  (seasonal adjustment, units, rounding, stale history) and a series record whose verdict is
  `baseline_beats_market`. The record is kept per series for the life of the process, written
  only by a backtest of the engine's own forecast (no `forecast_args` override). The scan
  leaves out a series whose record says the price beat the baseline. The `lock` tier is
  deferred to Phase 3, where baskets get `rules_match`.
- Publication lag. `as_of` used to keep every month that had ended; a monthly row now counts
  as known only once its release has printed (CPI 16 days after month end, jobs 10, PCE 31,
  fed funds 4, housing starts and permits 21, Case-Shiller 62). Without this the backtest
  read the settlement month's own value.
- Kalshi serves settled markets from two tiers. `/markets?status=settled` holds only those
  settled after the venue's cutoff (`/historical/cutoff`, about two months back);
  `/historical/markets?series_ticker=` holds the rest, and their candles are at
  `/historical/markets/{ticker}/candlesticks`, whose fields drop the `_dollars` and `_fp`
  suffixes. Reading only the live tier gave 2 events per series and an `inconclusive`
  verdict every time. Both tiers are read now.
- Column names in the builder's series query are quoted. Unquoted `date` cost about 160 s on
  the first query of a process against 4 to 10 s quoted. The engine behaviour behind it
  (an unquoted keyword-named column triggering a load of every schema) affects any query and
  is not fixed here.
- Ticket depth is read for the three largest edges of an event (one book call each).
  `contracts_left` on a re-quote is what still rests at or under the limit, not the unfilled
  part of the ticket.

Exit test: a reported opportunity carries a backtest record, a dollar capacity, an
annualized return and an order ticket; re-quoting it returns its current status.

## Phase 3 — lock integrity and basket structures

Needs Phase 2 B (size, annualized return apply to legs).

| Teammate | Delivers | Files owned |
|---|---|---|
| A | `compare_settlement_rules`: structured diff of two events' source, series, period, rounding, release date and tie handling. `cross_venue` baskets carry `rules_match`; a lock on unmatched rules is reported as not a lock. | `MarketRules.java`, `MarketRulesTest.java` |
| B | Forecast-free structural locks within a venue (bucket asks summing under 1, non-monotone ladders). Basket recipes `range` (the collar analogue), `calendar`, `linked_drivers` refinements. Payoff curve by settlement value from `price_market_basket`. | `MarketBaskets.java`, `MarketBasketsTest.java` |

Lead: report gate — a basket reported as a lock must carry `rules_match` and the payoff curve.

Phase 3 as built (2026-10-02):

- `compare_settlement_rules` diffs nine dimensions (source agency, series, settlement
  period, transform, seasonal adjustment, rounding, release or close date, revision handling,
  tie handling). `rules_match` is `match` only when every dimension is stated on both sides
  and agrees; one stated disagreement is `differ`; anything unstated is `unverified`. Close
  dates within three days count as one release.
- `find_market_baskets` puts `rules_match` and the per-pair diff on every `cross_venue`
  basket, over the events it lists. `price_market_basket` puts them on any basket spanning
  venues, and returns `payoff_curve` (profit per cost by settlement value, fee-inclusive
  floor, break-evens) when every leg shares one column: of `search.best[0]` when a search
  ran, of all legs otherwise. With `lock=true` its `next` says a cross-venue lock whose
  verdict is not `match` is not a lock.
- First live listing: 12 cross-venue baskets, none `match` (5 `differ`, the rest `unverified`).
  Venue rule texts rarely name a series id, a seasonal adjustment or a revision policy, and
  for a rate decision or a storm count those terms do not apply. As built, a cross-venue
  lock is therefore almost always reported as unverified. Open decision: mark dimensions
  that cannot apply to a driver as not applicable, so that `match` is reachable.
- `price_market_event` returns `structural_locks`, net of fees: ladder pairs that still lock
  after fees, and the event's bucket partition (buy every YES for 1, or every NO for n - 1)
  with whether the buckets are exclusive and exhaustive. Exhaustiveness needs the settlement
  grid, taken from `round` or the built forecast's rounding. Polymarket markets carry no
  strikes, so they count only when `conditions` states them. The scan's `structural` list
  stays before fees (the listing reads no fee rates) and says so.
- Recipes `range` and `calendar` are in `find_market_baskets`; `linked_drivers` baskets carry
  `link_direction`.

Exit test: the "basket that locks in a yield over 10%" prompt returns a basket with a payoff
curve, a fee-inclusive floor and a rules verdict, or states that none exists.

## Phase 4 — standard layouts, recipes, refinement prompts

Needs the fields from Phases 1–3.

| Teammate | Delivers | Files owned |
|---|---|---|
| A | Three ready-made dashboards returned by the tools, as `chart_panel` is today: opportunity card (stat row, fair-vs-ask ladder, forecast vs market-implied distribution, series history with forecast fan), scan board (drawn → forecast → passed funnel, ranked table, edge vs standard error), basket sheet (legs, payoff diagram, fee waterfall, what breaks the lock). | `MarketLayouts.java`, `MarketLayoutsTest.java` |
| B | Per-driver forecasting recipes (CPI, jobs, GDP, mortgage rate, weather, oil barrier) and the structure recipes, in the recipe catalog. `follow_ups` in tool output: size it, breakeven, hedge leg on the other venue, re-quote, settle-within-N-days. | `recipes.json`, `MarketFollowUps.java`, `MarketFollowUpsTest.java` |

Chart types (asked for by the user 2026-10-02: candlesticks and what forecasting charts
use). `render_chart` draws line, bar, pie, scatter and bubble only, so these are new marks in
`ChartRenderer`, done before teammate A's layouts and owned by the lead (shared file):

| Chart | Shows | Data it needs |
|---|---|---|
| Candlestick with volume bars | venue price of one market over time | open, high, low, close per period — `PricePoint` keeps only the close today |
| Fan chart | series history, then the forecast's median and 50/80/95% bands | quantiles of the builder's samples per horizon |
| Distribution overlay | forecast density against the market-implied distribution, strikes marked | builder samples; implied probabilities per strike |
| Fair-versus-ask ladder | per strike: fair value, bid, ask, edge after fees | `price_market_event` rows |
| Calibration (reliability) plot | predicted probability against realised frequency, baseline and price | per-market rows of the backtest |
| Brier by event | baseline and price score per settled event, in time order | backtest `events` |
| Depth chart | cumulative contracts by price, the ticket's limit marked | order book |
| Payoff diagram | basket profit by settlement value, fee-inclusive floor marked | `price_market_basket` (Phase 3 B) |
| Edge decay | edge of an opportunity from quote to re-quote | ticket and re-quotes |

Lead: report gate requires the layout that matches the question (one event, N events, basket).

Exit test: all three prompts — one random event, five random events, a locking basket —
publish on the first attempt with the matching layout and follow-ups.

### Phase 4 as built — chart marks and their data (2026-10-02, `def43b0a9`)

| Chart in the table above | State | How it is drawn |
|---|---|---|
| Candlestick with volume bars | built | `render_chart` / dashboard panel `chart_type: candlestick`, `candles{open, high, low, close, volume?}` |
| Fan chart | built, 50% and 90% bands | `chart_type: fan`, `series` + `bands[{name, low, high}]` |
| Payoff diagram | built | `line` chart with `reference_lines` for the floor |
| Fair-versus-ask ladder, Brier by event | no new mark needed | grouped `bar` |
| Distribution overlay, calibration plot, edge decay | no new mark needed | `bar` / `scatter` / `line` with `reference_lines` |
| Depth chart | not built | needs a step line, and the tools return a depth summary at the ticket's limit, not the book's levels |

- `reference_lines[{value | category, label?}]` is accepted on line, fan and candlestick only.
- Both ordered types fit the y axis to the data (zero is not forced) and thin the period labels.
- `market_price_history` returns `open`, `high`, `low` per point and an `ohlc` line saying where
  they came from. Kalshi: the venue's own candles. Polymarket: daily points resampled from its
  hourly points, which the venue keeps for about 30 days, so older days carry a close only;
  hourly points have no candle at all.
- `forecast_market_event` returns `fan`: 24 periods of history, then the median and the 50% and
  90% intervals at up to 12 steps to settlement. One-period transforms (change, month-over-month)
  have no path, so their intervals stand at the settlement period alone. The 80/95% bands in the
  table were replaced by 50/90% to match the p05/p95 the tool already reports.

### Phase 4 as built — layouts, follow-ups, recipes (2026-10-02)

| Planned | Built |
|---|---|
| Standard layouts returned as dashboard JSON | `MarketLayouts` builds an opportunity card, a scan board and a basket sheet from the tool's own output. The result carries only `dashboard_layout` (an id: `event:<source>:<event_id>`, `scan:<n>`, `basket:<n>`) and `dashboard_panels` (the panel titles). `MarketPresentation` keeps the last 40 layouts; `compose_dashboard` and the report's `dashboard` take `{"layout": id}` or a list of ids, and panels given beside it are placed after. The model never echoes chart data. |
| Report gate on the chart panel | Replaced: once a market tool has returned a layout, a report with no `layout` is refused, naming the ids returned since the last report. `chart_panel` and `chart_panel_use` are gone from `price_market_event`. |
| Several events on one board | A list of ids: each layout opens with a heading tile (`1 of 2`, the event title, its forecast line); the board takes its own title and the distinct footnotes. |
| Refinement prompts | `MarketFollowUps`: at most 5 `follow_ups` per result, each `{question, tool, arguments}` checked against the tool's schema in tests. `follow_ups_use` tells the model to end the answer with them. |
| Recipes | 13 added to `recipes.json` (6 forecast-by-driver, 6 basket structures, 1 on capturing a point-in-time opportunity). Every claim was checked against the code by a separate reader; 16 sentences were corrected. |

Card panels: best edge, fair value against price, confidence, days to settlement; edge after
fees by strike; forecast probability against the YES bid and ask by strike; the fan, with the
best market's strike as its one reference line (a line per strike hid the fan). A market the
forecast does not price is left off; one quoted on neither side has no edge bar.

Gates, as changed after review and the three exit runs (q9011-q9013):

- The layout gate applies only when a market pricing call belongs to the report being
  published (the recent-call window the other gates use), so a layout from earlier work does
  not block an unrelated report.
- The pure-web-fallback gate counts a market pricing call that returned as the engine's own
  data. Both q9011 and q9012 were refused once for "no query call" and ran a query only to pass.
- A report's board is kept for `deliver_report` in eval mode; q9013 published through
  `create_report_artifact` and saved no `dashboard.png`.

Not built: a per-strike market-implied probability series (the output holds `implied_median`
only), so the line panel compares the forecast with the quote itself.

Known limit, seen live: the baseline forecast draws on every past change of the series. For
payrolls that includes 2020-2022, giving a 90% range of -70,000 to 942,000 jobs and a 55-point
"edge" at confidence `weak`. The backtest follow-up is what catches it; the scan does not.

## Phase 5 — verification (lead)

- Run the three prompts through `askamerica-desktop`, one at a time; audit calls from the
  task transcript.
- `code-reviewer` pass over the whole package; `test-strategist` pass for edge cases
  (empty book, one-sided quotes, unsettled history, series ending before settlement).
- File any catalog gap found on the way to `kenstott/govdata-ops`, verified live.

## Order and dependencies

```
Phase 1  A forecast builder ─┬─> Phase 2  A backtest ───────────┐
         B venue history  ───┴─> Phase 2  B size + annualized ──┼─> Phase 3 A rules diff
                                                                └─> Phase 3 B structures + payoff
                                                    Phases 1–3 ───> Phase 4 A layouts, B recipes + follow-ups
                                                                    Phase 5 verification
```

Known data gap that limits Phase 1: BLS average-price series are not in the catalog
(govdata-ops #851), so price-level events on those series return a "series not sourced"
flag rather than a forecast.
