# Config Redesign & Validation Plan

**Date:** 2026-08-30
**Status:** Plan — nothing implemented, nothing changed
**Inputs:** [schema-review-plan.md](../../schema-review-plan.md) (36-table research) ·
[data-month-lag-and-daily-lookback.md](data-month-lag-and-daily-lookback.md) (2 design requirements)

---

## Scope

**23 tables** get a config redesign. Every change below is blocked behind engine prerequisites —
applied today, most would be **unobservable**, not wrong.

| Change class | Tables |
|---|---|
| Widen `lookbackPeriods` | 7 |
| Narrow `lookbackPeriods` | 1 |
| Add `lookbackPeriods` alongside an existing freshness block | 2 |
| Add a new `freshness:` block **and** a `lookbackPeriods` | 5 |
| Add a plain freshness gate, no lookback | 2 |
| `dataLag` (years) → `dataMonthLag` (months) | 5 |
| Comment-only correction | 1 (+10 repeated sites) |


`eia_fossil_fuel_production` takes **two** changes (window widen *and* `dataMonthLag`), so the
distinct-table count is 23, not 24.

**Not in scope here:** the 21 tables still carrying the retired `lookbackPeriods` key (Phase 2 — research first),
and the 3 separate defects (`qwi_employment` stale time list, `state_personal_income` coverage,
anchor rename).

---

## Prerequisites — none of the 23 can be validated until these land

### P0 — Injectable "current date" ⭐ the enabling capability for this whole plan

There are **six direct `Calendar.getInstance()` calls** in the lag/window code and **no seam**:

| Location | Use |
|---|---|
| [DimensionIterator.java:188-189](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L188) | current year, last completed month (`monthlyYTD`) |
| [DimensionIterator.java:635](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L635) | `dataLag` on LIST dimensions |
| [DimensionIterator.java:728,739](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L728) | year-range end cap, `releaseMonth` |
| [EtlPipeline.java:3314](../../file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L3314) | `isWithinTrailingWindow` |

**Required:** make the current date injectable at these six sites, so **any date can be made
"now."**

This is not merely a convenience for edge cases — it changes the economics of the entire
validation plan:

**1. No waiting for data availability.** Pin the simulated date to a point where the whole
lookback window is already published. Testing `dataMonthLag: 2` does not require waiting for EIA
to release next month — set "now" to a past date and the data has existed for years.

**2. No waiting for calendar boundaries.** The January year-boundary case — the highest-risk
behavior in this plan — becomes a test that runs today, in milliseconds, by setting "now" to
`2026-01-15`. Without the seam it is a five-month wait.

**3. Deterministic, permanently reproducible tests.** A test written against the *real* clock is
non-reproducible by construction: it changes behavior every January and every new year, and its
expected row counts drift as sources publish. **Pin every test to a fixed simulated date in a
settled past window** and the expected values never move. Prefer a date at least ~2 years back so
the entire window is final upstream and assertions are stable forever.

**4. Most of the plan becomes a pure unit test with no network.** Which years and months get
requested is pure arithmetic over the clock, `dataLag`, `dataMonthLag`, `releaseMonth` and
`lookbackPeriods`. That logic — where nearly all the risk lives — can be asserted offline,
exhaustively, across many simulated dates, with no ETL run at all.

**Limitation to be explicit about.** Moving the clock back does **not** replay upstream history.
Pre-revision values are simply not retrievable: the source serves today's values for a 2022
period, never what it served in 2022. Setting "now" to 2022 changes *which periods are requested*,
not *what comes back*.

### A revision can only be simulated

This is the governing constraint on validating the revision guarantee, and it has no exceptions.

You cannot make a revision happen on demand, and you cannot retrieve a pre-revision value to
replay one. **There is no test mode in which the pipeline experiences a real revision arriving.**
Every revision test is therefore a simulation: serve payload A, ingest, serve payload B, re-run,
assert the period was re-ingested.

That is a **complete** test of the mechanism, not a compromise. The freshness gate compares
tokens and is indifferent to what produced the bytes, so stubbing loses no fidelity.

What varies is only the *realism of the fixture pair*:

| Fixture source | Realism | Use |
|---|---|---|
| Synthetic mutation (edit a byte) | Low | Fine for mechanism — this is what most tables should use |
| Real vintage pair (ALFRED for FRED, BEA archived releases) | High | Optional confidence for `fred_indicators`; still a simulation, just with authentic payloads |

Note the second row is **not** an exception to the rule — you are still hand-feeding the pipeline
two payloads. It buys realistic data, not a different kind of test, and is not worth per-source
plumbing unless a specific doubt calls for it.

Splitting the guarantee three ways:

- **Mechanism** (a changed token triggers re-ingest) → **simulated, stubbed source.** Acceptance gate.
- **Signal quality** (does *this* source's token actually discriminate?) → **Tier 1b probe.**
  The one part a stub cannot cover. Acceptance gate.
- **End-to-end proof** (a real revision, caught in the wild) → **production soak only.**
  Unpredictable timing; observational. **Must not be an acceptance gate** — gating the rollout on
  it would mean waiting indefinitely for an event no one controls.

### P1 — Year-range extension (Requirement 2a)

Until the daily run generates the prior years, **every window change in this plan is a no-op in
daily mode.** This is the load-bearing prerequisite.

### P2 — `dataMonthLag` support (Requirement 1)

Blocks the 5 EIA migrations. Independent of P1; can proceed in parallel.

> **Applying table configs before P0–P2 produces a green run that proves nothing.** The YAML will
> read correctly, the ETL will succeed, and the new windows will do nothing.

---

## Two tiers of test — most of the risk is in Tier 1

Splitting these keeps the expensive tier small.

### Tier 1 — Period generation (offline, no network, milliseconds)

**Asserts:** given a simulated date and a dimension config, exactly which `(year, month)` combos
are generated, and which are reopened.

This is pure arithmetic over the clock plus `dataLag` / `dataMonthLag` / `releaseMonth` /
`lookbackPeriods` — and it is where nearly all the risk in this plan lives. It needs no ETL run,
no bucket, no credentials, and no source availability.

Because it is nearly free, run it **exhaustively** rather than at one date: sweep all 12 months
across several simulated years per table. That is the only practical way to catch the
year-boundary and step/cadence cases, and it costs less than a single ETL run.

Every table in Groups 1–4 and 6 gets Tier 1 coverage. For most, Tier 1 **is** the real test.

### Required revision-simulation tests

**The simulation pattern already exists — extend it, do not build new machinery.**
[`FreshnessSkipGateTest`](../../file/src/test/java/org/apache/calcite/adapter/file/etl/FreshnessSkipGateTest.java)
already simulates a revision with `StubHttpSource` + `VaryingDataProvider`: it seeds tokens for
two years, changes one year's payload from `"stable"` to `"revised"`, and asserts the changed year
re-writes while the unchanged one skips (`testHashFreshnessDetectsChangedYearAndLeavesOtherUnchanged`).
Cold start, changed-token, unchanged-token and per-unit scoping are all covered.

**Two gaps, both confirmed by inspection:**

1. **Nothing tests the reopen path.** `grep` for `reopenTrailingWindowPeriods` across
   `file/src/test/` returns nothing. `markPeriodComplete` is exercised only in tracker tests. So
   the *core guarantee of the lookback* — that a period marked complete is forced back open —
   has no coverage at all.
2. **No test pins a simulated date.** The freshness tests reference neither `Calendar.getInstance`
   nor a current year; they hardcode fixture years (2023, 2024) while `isWithinTrailingWindow`
   compares against the **real** clock. In 2026 those years are both outside a
   a 2-period lookback — so `lookbackPeriods` in those tests now functions only as the *switch*
   that engages per-unit hash scoping, which is what they are named for, but the fixture years
   have silently aged out of the window. **This is the drift argument for P0, demonstrated in the
   existing suite rather than hypothesised.**

**Required tests** (all Tier 1 / stubbed source, all at a **pinned** simulated date):

| | Test | Asserts |
|---|---|---|
| **R1** | Revision **inside** the window is caught | now=2026-06; `lookbackPeriods: 6`; mark 2022 complete; change 2022's payload → re-ingested. **Fails at N=2, passes at N=6** — proves the *value*, not just the mechanism |
| **R2** | Revision **outside** the window is **not** caught | same, change 2019 → not re-ingested. Proves the window has an edge |
| **R3** | Completed period is reopened | mark complete, run, assert reopened and freshness re-evaluated — **the currently untested core guarantee** |
| **R4** | Reopen is a **no-op** on an already-unprocessed period | proves the lookback acts only on completed periods; must not double-add |
| **R5** | Unchanged period inside the window still **skips** | guards against "reopen" degrading into "always re-ingest" |
| **R6** | Per-table N | parameterised over the 23 tables' actual values, asserting the specific edge year each N implies — including `economic_census` N=8 reopening **exactly 1** combo and `communication_costs` N=4 in an **odd** simulated year |

R1 and R6 are the ones that justify this workstream: they fail against today's config and pass
against the redesigned one. R3 closes a pre-existing hole unrelated to any config change.

**Also fix as part of this work:** pin the existing tests' fixture years to a simulated date so
they stop drifting.

### Tier 1b — Signal-quality probe (a few HTTP requests, no ETL)

**The one thing a stub cannot tell you.** Revision capture itself is source-agnostic — a stubbed
source proves the mechanism completely. But whether a *given source's* freshness token actually
discriminates is a property of that endpoint, and it can only be measured against the real thing.

Two failure directions, with very different severity:

| Failure | Symptom | Cost | Detection |
|---|---|---|---|
| **False positive** — token moves while content is unchanged | Re-ingests every run | Wasteful; now a *daily* cost | Probe the real endpoint twice with no upstream change; assert the token is **stable** |
| **False negative** — token static while content changed | **Silently serves stale data forever** | The failure this whole workstream exists to prevent | Cross-check the probe token against a content hash over time |

This is not hypothetical — the research hit both live:

- **FEC** regenerates *every* cycle file daily. Verified: cycle files back to 2016 all carried a
  same-day `Last-Modified`. A `last_modified` gate would re-ingest every cycle, every day.
- **BLS `/pub/time.series/jt/`** serves one identical `Last-Modified` **and** `ETag` for every
  file in the directory, restamped on each monthly release regardless of content.

Both are why `communication_costs` and the `jolts_*` tables are specified as `hash`, not a probe
type — and that decision must be re-verified by probe, not assumed.

**Required for every table getting a new or changed freshness gate** (Groups 3, 4, 5): probe the
real endpoint twice, separated by enough time to catch a restamp, with no upstream change, and
confirm the token holds steady. Cheap — a handful of HEAD requests — and it is the only check
that catches a mis-chosen freshness *type* before it reaches production.

> `hash` cannot produce a false negative (it is content-derived), which is why it is the safe
> default when a probe type's discrimination is unproven. Its cost is the download — so the
> tradeoff is bandwidth against the risk of silent staleness, and for small files there is no
> real contest.

### Tier 2 — Ingestion behavior (real run, DQ bucket)

**Asserts:** the freshness decision and its cost — unchanged period skips, no Iceberg snapshot,
probe types issue a HEAD rather than a GET, cold start writes, failed commit records no token.

Needed only where a table's change affects fetch/write behavior rather than period selection:
Group 3 (HEAD-not-GET), Group 4 (new gate, cold-start risk), Group 5, and Group 6's row-count
verification.

### Minimum scope for Tier 2

```
minimum scope  =  full window span (years)  ×  ONE combo of every non-year dimension
```

**The year span cannot be shrunk** — it is the thing under test. A `lookbackPeriods: 6` cannot be
validated by a 2-year run.

**Every other dimension can be subset to 1.** The window and lag logic is evaluated per-combo and
is independent of how many combos exist, so a 51-state table validates identically with one
state. This is what keeps the plan cheap: it turns `fred_indicators` from 3,672 combos into ~6.

Per [DQ testing guidance](../../schema-review-plan.md): validate on 1–2 dimension combos first;
never run a full cross-product to check correctness.

### Operational notes for the validation runs

- Run against the **DQ bucket**, never production:
  `GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq worker-dq-run.sh --rebuild`.
  It **defaults to production** ([worker-dq-run.sh:95](../../govdata/scripts/parallel/worker-dq-run.sh)) — set it explicitly.
- **Exported `GOVDATA_START_YEAR` is silently ignored by `--rebuild`.** Use the
  `--start-year` flag ([worker-dq-run.sh:65](../../govdata/scripts/parallel/worker-dq-run.sh)).
  This plan is entirely about year ranges, so this trap will otherwise be hit repeatedly and
  produce misleading passes.
- Never run a schema+year concurrently with another session's job on the same schema+year.

---

## The 23 tables

`x/yr` = combos per year excluding the year axis. **Min scope** = years needed × combos after
subsetting to one non-year combo.

### Group 1 — Widen `lookbackPeriods` (7)

| Table | Schema | Change | x/yr | Min years | Min scope | Subset to |
|---|---|---|---|---|---|---|
| `gdp_statistics` | econ | 2→**6** | 2 | 6 (2021-26) | 6 | 1 frequency |
| `industry_gdp` | econ | 2→**6** | 44 | 6 | 6 | 1 freq × 1 industry |
| `ita_data` | econ | 2→**6** | 11 | 6 | 6 | 1 indicator |
| `iip_positions` | econ | 2→**6** | 1 | 6 | 6 | — |
| `fdi_direct_investment` | econ | 2→**6** | 4 | 6 | 6 | 1 direction × 1 class |
| `fred_indicators` | econ | 2→**6** | 612 | 6 | ~6 | **1 series** (of 51) |
| `nsf_herd_by_institution` | research | 2→**3** | 1 | 3 | 3 | — |

**Assert:** a year at `currentYear − 4` marked complete is force-reopened (fails at the old
value of 2, passes at 6). `nsf_herd` uses `currentYear − 2`.

> `fred_indicators` at full scope is 612 × 6 = **3,672 combos**. Subsetting to one series is not
> an optimization here — it is the difference between a runnable test and an unrunnable one.

### Group 2 — Narrow `lookbackPeriods` (1)

| Table | Schema | Change | x/yr | Min years | Min scope |
|---|---|---|---|---|---|
| `eia_capacity_changes` | energy | 2→**1** | 1 | 2 | 2 |

**Assert:** `currentYear − 1` is **no longer** reopened, and the current year still is. Needs 2
years generated to prove the exclusion. Lowest priority in the plan — N=2 is harmless, this is
tidiness.

### Group 3 — Add `lookbackPeriods` alongside an existing freshness block (2)

| Table | Schema | Change | Keep type | x/yr | Min years | Min scope |
|---|---|---|---|---|---|---|
| `county_qcew` | econ | add `lookbackPeriods: 2` | `etag` | 1 | 2 | 2 |
| `state_wages` | econ | add `lookbackPeriods: 2` | `etag` | 1 | 2 | 2 |

**Assert:** the prior year is reopened, and an unchanged year is skipped **with a HEAD and no
body download**. These files are ~75 MB/year — assert on request method, not just on "no
snapshot written", or a full download will pass silently.

### Group 4 — Add a new `freshness:` block **and** a window (5)

Highest risk in the plan: these tables have **no freshness gate at all** today, so the change
alters both whether they re-fetch and whether they skip writes.

| Table | Schema | Add | x/yr | Min years | Min scope | Note |
|---|---|---|---|---|---|---|
| `nps_visitation` | lands | `hash` + tw **2** | 1 | 2 | 2 | |
| `cde_police_employment` | crime | `hash` + tw **3** | 51 | 3 | 3 | subset to 1 state |
| `communication_costs` | fec | `hash` + tw **4** | 1 | 4 (2023-26) | **3 combos** | even-year cadence |
| `lodes_workplace` | census | `hash` + tw **4** | 51 | 4 | 4 | subset to 1 state |
| `economic_census` | census | `hash` + **2 periods** | 2 | 2 published editions | **2 combos** | step-5 axis, cadence-invisible under the walk |

Three carry a **non-obvious N** whose validation must prove the N specifically:

- **`economic_census: 2`** — the two most recent **published** editions (2022, 2017). **Assert it
  reopens 2, not 0.** Zero means the walk is counting calendar years, which the 5-year step would
  swallow. (A calendar-anchored window needed 8 here; the period walk removes that inflation.)
- **`communication_costs: 4`** — even-year cycles only. Validate in an **odd** simulated year
  (needs P0): N=2 must find the two most recent published cycles regardless of calendar parity.
- **`cde_police_employment: 3`** — assert `currentYear − 2` reopens. Also confirm `hash` is used,
  not `last_modified`: FEC/BLS-style daily restamping makes probe types unusable on some sources.

### Group 5 — Add a plain freshness gate, no window (2)

| Table | Schema | Add | x/yr | Min scope |
|---|---|---|---|---|
| `jolts_industries` | econ-reference | `freshness: {type: hash}` | 1 | 1 run |
| `jolts_dataelements` | econ-reference | `freshness: {type: hash}` | 1 | 1 run |

**Assert:** an unchanged ~1 KB code list produces **no** Iceberg snapshot on a second run, and a
mutated one does. **Must be `hash`:** BLS restamps every file under `/pub/time.series/jt/` with an
identical Last-Modified and ETag on each monthly release, so a probe type would fire monthly on
unchanged content. Worth an explicit negative test asserting `etag` would misfire here — this is
the cheapest table in the plan and documents a real trap.

No year axis, so no window and no multi-year scope. Two runs is the whole test.

### Group 6 — `dataLag` (years) → `dataMonthLag` (months) (5)

**These are coverage changes, not config tweaks.** Each table gains ~2 years it has never held.
Validate as a backfill: row counts per year before and after.

| Table | Schema | Change | x/yr | Min years | Min scope | Shape |
|---|---|---|---|---|---|---|
| `eia_electricity_prices` | energy | `dataLag:2` → `dataMonthLag:2` | 1 | 3 | 3 | year-grain |
| `eia_crude_oil_imports` | energy | `dataLag:2` → `dataMonthLag:2` | 1 | 3 | 3 | year-grain |
| `eia_electricity_generation` | energy | `dataLag:2` → `dataMonthLag:2` | 12 | 3 | ~14 | **month list** |
| `eia_refinery_operations` | energy | `dataLag:2` → `dataMonthLag:2` | 12 | 3 | ~14 | **month list** |
| `eia_fossil_fuel_production` | energy | + window 2→**3** | 12 | 3 | ~14 | **month list** |

**Assert, per table:**
1. *(Tier 2)* Data years previously absent (`currentYear − 1`, `currentYear`) now materialize
2. *(Tier 1)* No combo is generated past `vintageCeiling`
3. *(Tier 1)* **Year-boundary:** with "now" set to January, the current year must not be
   generated and the prior year must cap at month **11**, not 12

Point 3 is the case most likely to be wrong, and with P0 it is a **millisecond offline test at a
pinned January date** rather than a five-month wait. Sweep all 12 simulated months per table —
the boundary is the only place the arithmetic is interesting, and it costs nothing to cover it
exhaustively.

The three month-list tables exercise Shape B (cartesian month axis), which nothing caps today,
so they are the priority for the sweep.

### Group 7 — Comment-only (1 table + 10 sites)

| Target | Change |
|---|---|
| `eia_petroleum_stocks` | Rewrite rationale: window is for **weekly accrual**, not revision — EIA never revises the WPSR |
| `energy-schema.yaml` ×10 sites | Replace the repeated blanket-EIA comment with the program-specific rationale per table |

**No functional change; no ETL validation needed.** Verify by review only. Worth doing precisely
because the current comment would lead a future reader to "correct" a right value to a wrong one.

---

## P0 operand wiring — one `pipelineDate`, or the simulation is half-applied

`GovDataSchemaFactory` already derives calendar context and lets operands override it
([GovDataSchemaFactory.java:817-836](../../govdata/src/main/java/org/apache/calcite/adapter/govdata/GovDataSchemaFactory.java#L817)):

```java
LocalDate today = LocalDate.now();
Object currentMonthObj = operand.get("currentMonth");
... System.setProperty("GOVDATA_CURRENT_MONTH", currentMonth);   // also YEAR, QUARTER, QUARTER_TOKEN
```

**The trap:** adding `PipelineClock` alone would create *two* notions of "now." A run pinned to
2026-01-15 would generate dimension combos for January while `GOVDATA_CURRENT_MONTH` still said
August — so cache-buster dimensions, URL templates and period bounds would disagree with the range
logic. The simulation would be half-applied, and the resulting test would be misleading rather than
merely wrong.

**Required:** a single `pipelineDate` operand that seeds **both**:

```yaml
pipelineDate: "${GOVDATA_PIPELINE_DATE:}"     # empty = real clock
```

- When set, it replaces `LocalDate.now()` at line 817 **and** seeds `PipelineClock`.
- `currentMonth` / `currentYear` / `currentQuarter` remain explicit per-field overrides layered on
  top, so existing behavior is untouched when `pipelineDate` is absent.
- Read via `ModelOperand`, never `System.getenv` — the launch scripts may export
  `GOVDATA_PIPELINE_DATE`, but the schema YAML is where the dependency is declared.

**Acceptance:** with `pipelineDate` set, assert `GOVDATA_CURRENT_MONTH`/`YEAR`/`QUARTER` and
`PipelineClock.today()` all agree. A test that pins the date and checks only combo generation would
not catch this.

---

## DQ expectations must be updated *before* live runs

Several changes deliberately alter row counts and coverage. Without updating DQ first, the first
live run reports failures that are the *intended* outcome — and the real signal is lost in them.

| Change | Effect on DQ | Action before running |
|---|---|---|
| 5 EIA `dataMonthLag` tables | Each gains ~2 data years | Raise row-count thresholds; re-derive `observedCoverage` |
| `qwi_employment` → bulk files | 5 → 81 columns; coverage becomes **per-state** (CA 1991–2025, AK 2000–2016) | Replace the global coverage range with per-state expectations; a single range cannot describe it |
| Any table gaining a lookback | More reopens, but **no new rows** when unchanged | No threshold change; assert *snapshot count* stays flat |
| Layer 1 defect fix | 20 tables revert to all-or-nothing freshness | Expect more snapshots/runtime, same row counts |

> Per existing guidance, `T2_row_count` failures are usually the DQ year-window sample undercutting
> a full-range threshold rather than a defect — doubly likely here, where ranges are changing by
> design. Re-derive thresholds rather than interpreting the first failures.

---

## Rollback

The 37-table migration to `lookbackPeriods` is a **hard cutover**: the prior key fails loudly at
load. That is right for catching stragglers, but it means engine + 37 schema files
land together and there is no partial-revert.

| Change | Revert | Notes |
|---|---|---|
| **P0** (`PipelineClock`) | Plain revert | No data written; no-op when unset |
| **Layer 1 defect fix** | Plain revert | Only disables an optimization; writes nothing differently |
| **P1 + the 37-table migration** | **Revert together** — engine and YAML are coupled by the hard cutover | Data written under a wider lookback is not *wrong*, just refreshed; no data repair needed to roll back |
| **P2 + 5 EIA `dataMonthLag`** | Revert config; **data does not roll back** | The recovered years remain. Harmless (they are real published data) but `observedCoverage` will not match a reverted config |
| **`qwi_employment` bulk switch** | Revert config **+ purge + reprocess** | Partition scheme changed; the old `time=` layout is gone |
| **Layer 2 partition changes** | Revert config **+ purge + reprocess** | Same reason |

**Practical consequence:** the first three rows are cheap to reverse and should ship first. The last
three involve a purge and are effectively one-way within a release — do them last, one table at a
time, after the reversible work is proven.

---

## Validation waves

Ordered by risk and by what each wave de-risks for the next.

### Wave −1 — Fix the clobbering defect FIRST (independent of everything below)

[Layer 1 of the defect fix](DEFECT-replace-partitions-clobbering.md): derive `perUnitSkipSafe` and
disable per-unit freshness when the partition is coarser than the fetch unit.

**Why this precedes the prerequisites, not just the tables:** every wave below validates against the
write path, and three tables being changed (`fred_indicators`, `air_quality_daily`,
`regional_price_parities`) are themselves on the at-risk list. Testing on a write path with a known
data-loss hole produces results you cannot attribute.

It is also the cheapest thing in this document to ship: no schema changes, no purge, no reprocess,
and it can only *disable* an optimization — it never writes data differently.

**Exit criterion:** the reproduction test (two units, one skipped, assert the skipped unit's rows
survive) passes; the "per-unit freshness DISABLED" log line names the expected 20 tables — and does NOT name the 7 `patent_*` tables, whose single-valued `quarter` makes them already safe.

### Wave 0 — Prerequisites
P0 date injection · P1 year-range extension + period walk · P2 `dataMonthLag`.
**Exit criterion:** a test can assert lookback and lag behavior at an arbitrary simulated date,
offline. **Build P0 first** — it turns Waves 1–4's Tier 1 coverage into offline unit tests, so it
pays for itself immediately rather than being overhead.

Include in P0's acceptance the `pipelineDate` consistency check (above): `PipelineClock` and
`GOVDATA_CURRENT_MONTH`/`YEAR`/`QUARTER` must agree, or the simulation is half-applied.

### Wave 1 — Cheapest, highest signal (4 tables)
`jolts_industries`, `jolts_dataelements` (2 runs each) · `iip_positions`, `nsf_herd_by_institution`
(1 combo × 3–6 yrs).
**Why first:** smallest scope, and they prove P1 works end-to-end before anything expensive runs.

### Wave 2 — Window widening (5 tables)
`gdp_statistics`, `ita_data`, `fdi_direct_investment`, `industry_gdp`, `fred_indicators` — all
subset to one combo.
**Why:** same change shape, one source family (BEA/FRED). Validating one validates the pattern.

### Wave 3 — New freshness blocks (5 tables)
Group 4. Highest config risk: no gate exists today, so a misconfigured type could wedge a table
shut. **Each needs a cold-start assertion** (a first run must write data) in addition to its
window test.

### Wave 4 — `dataMonthLag` migration (5 tables)
Group 6. Treat as backfill: per-year row counts before/after, plus the January boundary
simulation. Highest data risk in the plan.

### Wave 5 — Low priority
`eia_capacity_changes` narrowing · Group 7 comments · `county_qcew`/`state_wages` (Group 3 — can
also ride in Wave 1; they are cheap, but their HEAD-not-GET assertion is worth its own attention).

---

## Production soak — the minimum observation period

Test-bench validation proves the logic. Two things can only be observed in a real daily run:

| Run | Proves |
|---|---|
| **Run 1** | Cold tokens seeded; prior years reachable and ingested |
| **Run 2** (next day, no upstream change) | Unchanged periods **skip** — no Iceberg snapshot, and for probe types no body download. *This is where the cost model is confirmed or refuted.* |
| **Run 3** (after a real or simulated upstream change) | The revision is actually caught |

**Minimum soak: 2 consecutive daily runs**, with a third triggered by a change. Watch Run 2's
cost closely — the lookback becomes a *daily recurring* cost, and if the skip does not fire, a
6-year window on a large-file `hash` table means a large download every day, forever.

Metric to capture per table on Run 2: bytes transferred and request count. A silent regression
here is the most likely way this work degrades after landing.

---

## Summary

| | Count |
|---|---|
| Tables redesigned | **23** |
| Engine prerequisites | **3** (P0 date injection, P1 range extension, P2 `dataMonthLag`) |
| Validation waves | **6** (incl. Wave 0) |
| Tier 1 (offline period-generation tests) | All 23 — where nearly all the risk lives |
| Tier 2 (real ETL runs) | Groups 3–6 only |
| Largest Tier 2 scope | `industry_gdp` / `fred_indicators` — 6 combos each after subsetting |
| Cheapest | `jolts_*` — 2 runs, no year axis |
| Highest data risk | Group 6 `dataMonthLag` — coverage change, ~2 years recovered per table |
| Highest-leverage single item | **P0** — makes any date "now", so nothing waits on data availability or the calendar |
