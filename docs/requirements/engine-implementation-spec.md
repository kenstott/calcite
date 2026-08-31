# Engine Implementation Spec — P0 / P1 / P2

**Date:** 2026-08-30
**Status:** Spec — not implemented
**Requirement:** [data-month-lag-and-daily-lookback.md](data-month-lag-and-daily-lookback.md)
**Consumer:** [config-redesign-and-validation-plan.md](config-redesign-and-validation-plan.md)

Three engine changes. **P0 first** — it converts most of the 23 tables' validation into offline
unit tests, so it pays for itself immediately.

> **Java 8 compatibility does not apply here.** The `file` module is already on Java 11+
> (`--add-modules jdk.incubator.vector`), so `java.time` and modern constructs are fine.

---

## P0 — Injectable current date

### Problem

Six direct `Calendar.getInstance()` calls, no seam:

| # | Site | Reads | Used for |
|---|---|---|---|
| 1 | [DimensionIterator.java:188](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L188) | YEAR | `monthlyYTD` current year |
| 2 | [DimensionIterator.java:189](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L189) | MONTH | `monthlyYTD` last completed month |
| 3 | [DimensionIterator.java:635](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L635) | YEAR | `dataLag` on LIST dimensions |
| 4 | [DimensionIterator.java:728](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L728) | YEAR | `resolveYearRange` ceiling |
| 5 | [DimensionIterator.java:739](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L739) | MONTH+1 | `releaseMonth` gate |
| 6 | [EtlPipeline.java:3314](../../file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L3314) | YEAR | `isWithinTrailingWindow` — **eliminated by P1**, not ported |

> Site 6 goes away rather than gaining a seam: under P1 the lookback is data-anchored, so
> reopen-eligibility becomes set membership and needs no notion of "now" at all. Five sites to port,
> one to delete.

### Design

Introduce a single resolution point rather than threading a `Clock` through every signature:

```java
/** Resolution point for "now" across dimension and freshness logic. */
final class PipelineClock {
  private static volatile LocalDate override;      // null = real clock

  static LocalDate today() {
    LocalDate o = override;
    return o != null ? o : LocalDate.now();
  }
  static int currentYear()  { return today().getYearValue(); }
  static int currentMonth() { return today().getMonthValue(); }   // 1-based

  static void setOverrideForTest(LocalDate d) { override = d; }
  static void clearOverride() { override = null; }
}
```

**Two decisions to settle:**

1. **1-based month.** Sites 2 and 5 disagree today — site 2 uses `Calendar.MONTH` (0-based, and
   relies on that to mean "last completed month"), site 5 adds `+1`. Normalise on **1-based**
   `getMonthValue()` and make "last completed month" an explicit `currentMonth() - 1`, so the
   semantics are stated rather than encoded in an off-by-one. This is the highest-risk part of
   P0: site 2's correctness currently *depends* on the 0-based quirk.

2. **How the override is set.** Per the GovData rule, an override must come from the **model
   operand**, not `System.getenv`. Add e.g. `pipelineDate: "${GOVDATA_PIPELINE_DATE:}"` read via
   `ModelOperand`, so a simulated date is declarable in a run model as well as in tests. A static
   test hook alone is sufficient for P0's unit-test value; the operand path is what makes an
   end-to-end DQ run at a simulated date possible, which Tier 2 wants.

### Acceptance

- [ ] All six sites route through `PipelineClock`; no `Calendar.getInstance()` remains in these paths
- [ ] With no override, behavior is **bit-identical** to today (site 2's off-by-one preserved in meaning)
- [ ] A test can pin any date and assert generated combos, with **no network**
- [ ] Override is settable from the model operand, not `System.getenv`
- [ ] Override is per-JVM and reset between tests (guard against leakage across the suite)

---

## P1 — Year-range extension (daily lookback reachability)

### Decision: `lookbackPeriods` — counted in periods, not years

The lookback is defined as a count of **published
periods**, grain-agnostic, ignoring whether the period is a year, quarter, month, week or day.

```yaml
lookbackPeriods: 6      # table-level
freshness:
  type: hash
```

**The concept already exists in the tracker** — this reuses it rather than inventing a parallel
year-specific notion:

```java
String[] PERIOD_SLOTS = {"year", "quarter", "month", "week", "day", "day_of_week"};
```
([IncrementalTracker.java:469](../../file/src/main/java/org/apache/calcite/adapter/file/partition/IncrementalTracker.java#L469))

`periodCompletionKey` builds its key from **only** those slots — *"all others are ignored"* — so
fan-out dimensions (`state`, `cik`, `series`, `frequency`) are already excluded. Example keys:
`2025_NA_NA_NA_NA_NA_patents_patent_grants` (annual),
`2026_NA_06_NA_NA_NA_energy_eia_electricity_generation` (monthly).

**Why periods beat years:**

1. **Grain-agnostic by construction.** One knob serves annual, quarterly, monthly and weekly
   tables. No per-grain special-casing, and no question of what N means on a month-grain table.
2. **It aligns with the unit the freshness gate already operates on.** The lookback and the
   per-period token are then scoped to the same thing.
3. **The walk gets simpler.** Stepping to the previous published period is one operation;
   counting years on a month-grain table would mean grouping months into years first.
4. **Fan-out exclusion is free.** `cde_police_employment` (51 states × year) has 51 combos per
   period, not 51 periods — already handled by the existing key.
5. **It removes the calendar from the lookback entirely**, completing the direction set by
   data-anchoring: the lookback neither computes dates nor knows units.

### Placement: table-level, not on a dimension

A period spans dimensions (`year` **and** `month`), so it cannot sit on one of them. It is
**top-level on the table**.

Validation carries the guarantee: **`lookbackPeriods` on a table where `hasCanonicalPeriod` is false must fail loudly at load.**
This rejects the setting on *any* non-period-tracked table,
not just one lacking a `year` dimension. All 11 tables the audit found with an inert
`lookbackPeriods` remain rejected, since none carries a canonical period slot.

> Note `qwi_employment` carries a real temporal axis named `time` (`YYYY-QN`), which is **not** a
> canonical slot — so it would be rejected too. That is correct here, but it points at a separate
> defect: that dimension should be expressed as canonical `year`/`quarter` slots.

### The tradeoff to accept

**N is no longer directly comparable to the upstream revision policy**, which publishers almost
always state in years. On a monthly-period table, 3 years of revision depth is
`lookbackPeriods: 36`; `fred_indicators` (year + month slots) needs **72** rather than 6.

Mitigation: the per-table comment must state the translation explicitly — e.g.
`# 36 periods = 3 years of monthly data; covers EIA's PSA restatement`. This is mandatory, not
optional: a bare `lookbackPeriods: 72` is unreviewable without it.

### Migration

**37 tables** currently set `lookbackPeriods` under `freshness:` (16 in the audited 36, 21 in the
unverified Phase 2 set), plus the 7 new ones from the config plan.

**Decided: research Phase 2 first, then migrate all 37 in one cutover.** The 21 unverified tables
get the same source-revision research as the audited 36 *before* the key moves, so each table is
touched once and lands with a correct value. Migrating first would leave 21 likely-wrong values
live — five look BEA-sourced and probably need `6` rather than `2` — and would touch them twice.
This makes the Phase 2 research a **prerequisite of the cutover**, not a follow-up.

**Hard cutover, no compatibility fallback.** Per the no-silent-fallback rule, `lookbackPeriods`
appearing under `freshness:` must **fail loudly at config load** with a message naming the table
and the new location — never be silently honored or silently ignored. A silent fallback here would
reproduce exactly the failure this move exists to eliminate.

`FreshnessConfig.getTrailingWindow()` and its parse at
[FreshnessConfig.java:236](../../file/src/main/java/org/apache/calcite/adapter/file/etl/FreshnessConfig.java#L236)
are removed; `EtlPipeline.reopenTrailingWindowPeriods` / `isWithinTrailingWindow` read the value
from the year dimension instead. `EtlPipeline` already has the dimension configs.

**Watch the second job this setting does.** `lookbackPeriods` is not only the reopen trigger — it
is also the switch that engages per-unit hash-token scoping
([EtlPipeline.java:795-797](../../file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L795)):
`hashPreDownloadExcluded` is false only when it is set. That coupling must be carried over to read
`lookbackPeriods`, or every `hash` table silently reverts to pipeline-scoped tokens. It is the
non-obvious half of this migration and the most likely thing to be missed.

### The lookback is data-anchored, not clock-anchored

**The lookback must not do lag or calendar arithmetic at all.** It walks backwards from the **last
published period**, over periods that were **previously published**, checking freshness on each.

```
1. cursor = top of the generatable range        // where published data could begin
2. walk down until isPeriodComplete(cursor)     // = the LAST PUBLISHED period, the anchor
3. keep walking, collecting previously-published periods, until N are found
4. those N periods are the lookback set; reopen them
5. per-period freshness then decides whether each is actually re-ingested
```

**Lags and lookback are orthogonal concerns:**

| | Governs | Derived from |
|---|---|---|
| `dataLag` / `dataMonthLag` | what is **available to fetch** — the top of the range | the clock |
| `lookbackPeriods` | what is **worth re-checking** for revisions | **what we actually published** |

A lookback computed as "N years back from a clock-derived anchor" would generate a year with no
published data at the top while dropping a real year off the back. **Anchoring on the last
published period makes that class of error unrepresentable** — every period in the lookback set is one we demonstrably
published, so none can be empty by construction.

**Consequences worth stating:**

- **The calendar-vs-vintage anchor question dissolves.** There is no anchor to choose; it is a
  fact in the tracker, not a computation.
- **It is robust to a stalled pipeline.** If the ETL has not run for a month, a clock-anchored
  lookback drifts past what was actually ingested. A data-anchored one starts where the data does.
- **Gaps do not consume the budget.** Counting *published* periods rather than calendar slots means
  a table with holes still re-checks N real periods, instead of spending the budget on slots that
  were never ingested. (The gaps themselves are a separate concern — they are already unprocessed
  and are handled by the normal path once generated.)
- **A cold table gets nothing.** No published periods ⇒ empty lookback set. This is consistent with
  the established semantics: the lookback acts **only on completed periods**, and a never-ingested
  period is reached by the normal unprocessed path, not by this.

### Walk mechanics (detailed)

**1. Derive the step grain from the period slots in use.** The *finest* canonical slot present
among the table's dimensions defines one step:

| Slots present | One step | Candidate sequence from `2026-08` |
|---|---|---|
| `year` | 1 year | 2026 → 2025 → 2024 … |
| `year, quarter` | 1 quarter | 2026Q3 → 2026Q2 → 2026Q1 → 2025Q4 … |
| `year, month` | 1 month | 2026-08 → 2026-07 → … → 2026-01 → 2025-12 … |
| `year, week` | 1 week | … |

Roll-under across the year boundary is ordinary date arithmetic, not a special case.

**2. Start at the vintage ceiling.** The walk begins at the newest period that *could* have been
published — i.e. `vintageCeiling` from `dataLag` / `dataMonthLag`, adjusted by `releaseMonth`.

> This is the **only** point where lag arithmetic touches the lookback, and it governs *where to
> start looking*, never *how far back to count*. The counting is purely "how many published periods
> have I found."

**3. Step back, testing each candidate.** For each, build the period value map (only canonical
slots; fan-out dimensions are excluded by `periodCompletionKey` already) and call
`isPeriodComplete(pipelineName, periodValues)`.

- **Complete** → add to the lookback set.
- **Not complete** → skip and keep walking. Gaps do **not** consume the budget (a never-ingested
  period is already unprocessed and is handled by the normal path).

**4. Stop** at whichever comes first: `N` published periods collected; the dimension's configured
`start` / `minYear` floor; or a hard iteration cap (suggest `max(10 × N, 500)`) so a
never-ingested table cannot walk indefinitely.

Cost is bounded and local — a 35-year monthly table walking to its floor is ~420 point lookups
against local tracker state, no network.

**5. Results feed two consumers.** The oldest period in the set sets `effectiveStart` for range
generation; the set itself is the reopen-eligibility test (membership, not a clock comparison).

### ⭐ Consequence: cadence tables no longer need inflated values

Because the walk counts **published periods** rather than calendar years, a stepped or cadenced
axis stops needing an N inflated to span its gaps. This removes two of the three "non-obvious N"
special cases the config plan was carrying:

| Table | Axis | Calendar-anchored N (old) | **Period-walk N (new)** | Meaning |
|---|---|---|---|---|
| `economic_census` | `step: 5` (2017, 2022, 2027…) | **8** — anything under 5 reopened *nothing* | **2** | the two most recent published editions |
| `communication_costs` | even-year cycles | **4** — 2 reopened nothing new in odd years | **2** | the two most recent published cycles |

Both old values were artifacts of counting calendar years against a sparse axis. Under the walk,
`2` means what a reader expects it to mean, and the odd-year / under-the-step failure modes cannot
occur. **The monthly rescale still applies** (`fred_indicators` = 72 periods = 6 years) — that is a
genuine grain difference, not an artifact.

### Feasibility — the tracker supports the walk, but not enumeration

`IncrementalTracker` exposes **point queries only**: `isPeriodComplete(pipeline, periodValues)`
([IncrementalTracker.java:579](../../file/src/main/java/org/apache/calcite/adapter/file/partition/IncrementalTracker.java#L579)),
`markPeriodComplete`, `invalidatePeriod`. There is **no** method returning the set of completed
periods.

That is sufficient — the walk is exactly a sequence of point queries, which is why "walking back
looking for previously published" is directly implementable today. Two notes:

- **Bound the walk.** Without an enumeration API the walk cannot know when to stop on a sparse
  table. Cap it at the dimension's configured `start` (and/or `minYear`) so a table with nothing
  published cannot walk to the beginning of time.
- **Optional optimisation, not required:** adding `latestCompletedPeriod(pipeline)` or an
  enumeration to the tracker would turn the walk into one lookup. Worth it only if the point-query
  walk proves slow — it is N cheap lookups against local state, so probably not.

### Range generation still has to cover the set

The combos must exist in `combinations` for reopening to index them, so `resolveYearRange` still
extends its start — but the target is now **the oldest period in the lookback set**, a value
derived from the tracker, rather than from clock arithmetic:

```
effectiveStart = min(configuredStart, oldestPeriodInLookbackSet)
```

`isWithinTrailingWindow`'s clock comparison
([EtlPipeline.java:3314](../../file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L3314))
is **replaced** by set membership: a period is reopen-eligible if it is in the lookback set. This
removes one of the six `Calendar.getInstance()` sites outright, and removes the requirement that
range generation and reopen-eligibility share an anchor — they now share a *set*.

### Why there is no `lookbackMonths`

Month-granular revision churn (EIA-914 re-estimates the two prior months every month) is subsumed:
reopening a published year reopens all of its month combos. `lookbackPeriods` is needed anyway for
the annual restatements (PSA in August, NGA in October), which reach deeper. A second, finer knob
would cover a case the coarser one already handles.

**Decided: the walk counts published YEARS**, on month-grain tables as much as year-grain ones. So
`lookbackPeriods: 3` on a 12-month table reopens 3 published years — roughly 36 month combos — and
one N means the same thing everywhere. This is also what the annual restatements require: EIA's
PSA (August) and NGA (October) reach back years, so a month-scoped count would miss them even
though it would match EIA-914's rolling 2-month churn more tightly.

`resolveYearRange`'s existing `minYear` clamp
([DimensionIterator.java:759-764](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L759))
still applies afterward, so a table cannot be extended below its declared data floor.

### Open item

the lookback counts **published periods** while `effective_year = year - dataLag` is the data
year. Both shift together, so a window of N reopens the N most recent *available* data years —
verify this against the stepped (`step > 1`) branch at
[DimensionIterator.java:751](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L751),
where the year **is** the data year and the offset does not apply. `economic_census` (step 5) is
the table that exercises this.

### Config plumbing

Add `lookbackPeriods` to `DimensionConfig` exactly as `dataLag` is done (field, getter, builder,
`fromMap` parse accepting Number and `${VAR}` String) — see the P2 table below; the two changes
touch the same methods and should land together.

### Acceptance

- [ ] `lookbackPeriods` parses on a `yearRange` dimension (Number and `${VAR}` String)
- [ ] The lookback set contains **only previously-published periods** — never a period that was
      never ingested, and never an empty future slot
- [ ] **No lag or calendar arithmetic in the lookback path.** A test asserting the lookback set is
      *identical* under `dataLag: 0`, `dataLag: 2` and `dataMonthLag: 2`, given the same published
      history, proves the concerns are actually separated
- [ ] Walking back is bounded by `start` / `minYear` — a table with nothing published cannot walk
      to the beginning of time
- [ ] **Cold table:** nothing published ⇒ empty lookback set ⇒ no reopening
- [ ] **Stalled pipeline:** with the last publication a month old, the set still anchors on it
- [ ] **Gaps:** a table with holes still collects N *published* periods, not N calendar slots
- [ ] Reopen-eligibility is **set membership**, not a clock comparison
- [ ] Range extends to cover the oldest period in the set; widening logged per table
- [ ] Dimensions with no `lookbackPeriods` keep single-year daily behavior
- [ ] Correct under both `step == 1` and `step > 1`
- [ ] `lookbackPeriods` under `freshness:` **fails loudly** at load, naming the table and the new
      location — never silently honored, never silently ignored
- [ ] Per-unit hash-token scoping (`hashPreDownloadExcluded`) now keys off `lookbackPeriods` —
      regression test that a `hash` table still gets per-unit tokens after the move

---

## P2 — `dataMonthLag`

### Config plumbing

Mirror `dataLag` exactly:

| Change | Location |
|---|---|
| Field + getter | [DimensionConfig.java:69-70, 161-185](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionConfig.java#L69) |
| Builder field + setter | [DimensionConfig.java:703-705, 748-759](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionConfig.java#L703) |
| `fromMap` parse (Number and `${VAR}` String) | [DimensionConfig.java:452-461](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionConfig.java#L452) |

### `dataMonthLag` is `dataLag` in a different unit — same behavior, mutually exclusive

**They are one concept — publication lag — expressed at two granularities.** Not two mechanisms,
not composable. Exactly one may be declared.

| | Unit | Equivalent |
|---|---|---|
| `dataLag: 2` | years | shifts the axis: `year` stays the publish year |
| `dataMonthLag: 2` | months | caps the axis: `year` already is the data year |

Generalise the existing offset rather than adding a parallel mechanism:

```java
// exactly one of the two is set; both set = config error
Period lagOffset = dataLag != null
    ? Period.ofYears(dataLag)
    : Period.ofMonths(dataMonthLag);
```

**Behavior is deliberately NOT identical** — see below. Both land the correct value where it is
consumed (`effective_year`, which the partition and URL read), but they reach it differently:

| | Axis iterates | Effective (data) period |
|---|---|---|
| `dataLag: N` | publish **years** up to the current year | `effective_year = year − N` |
| `dataMonthLag: M` | publish **periods** up to the current month | `effective period = period − M months` |

The year rollover that motivated this whole requirement then falls out of ordinary month
arithmetic rather than needing special handling: in January 2026 with `dataMonthLag: 2`, the publish
period `2026-01` has effective period `2025-11`. Nothing special-cases the boundary — it is
`minusMonths(2)`.


`injectEffectiveYear` ([DimensionIterator.java:429-450](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L429))
generalises to inject an effective **period**; with `dataLag` set it must emit exactly what it emits
today.

### Mutual exclusivity

Declaring both is ambiguous, so it is a **config error that fails loudly at load**, naming the
table — never silently preferring one, per the no-silent-fallback rule. `dataLag: 0` alongside
`dataMonthLag` counts as declaring both: require the key to be absent, not zero.

### Consumer 1 — `resolveYearRange` ceiling

[DimensionIterator.java:735-753](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L735).
`latestEffectiveYear` becomes `vintageCeiling(...).year`, so a month lag that crosses January
pulls the ceiling back a year automatically. Preserve the existing `step > 1` vs `step == 1`
ceiling distinction at line 751.

Interaction with `releaseMonth` (line 738): both narrow the ceiling. Apply `dataMonthLag` first,
then the `releaseMonth` gate, and **cover the combination in tests** — a table with both is a
plausible future config even if none exists today.

### Consumer 2 — `monthlyYTD` (Shape A)

[DimensionIterator.java:180-219](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L180).
Two changes:

1. Line 206's ceiling becomes the lag-adjusted month, not `lastCompletedMonth`.
2. **Line 204-205 is the real fix.** Today `year < currentYear` ⇒ `month = "12"`, i.e. any prior
   year is assumed complete. With a lag crossing the boundary that is false: in January with
   `dataMonthLag: 2`, the prior year must cap at **11**. The prior-year branch must carry a
   ceiling rather than a constant.

### Consumer 3 — cartesian `month` list (Shape B) — **new code path**

Nothing caps this today. Three in-scope tables use it (`eia_electricity_generation`,
`eia_fossil_fuel_production`, `eia_refinery_operations`: `month: {type: list, values: [01..12]}`).

Add a post-expansion filter dropping any combo whose `(effective_year, month)` exceeds
`vintageCeiling`. Natural home is alongside `injectEffectiveYear` /
`injectMonthEnd` in `expand()` ([DimensionIterator.java:172-226](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L172)),
**after** `effective_year` is injected — the filter must compare against the data year, not the
publish year.

> Shapes A and B need the same arithmetic in different places. Factor `vintageCeiling` into one
> helper and have all three consumers call it; duplicating the boundary logic three times is how
> the January case ends up right in one path and wrong in another.

### Acceptance

- [ ] Parses as Number and `${VAR}` String
- [ ] **Mutually exclusive with `dataLag`** — both declared fails loudly at load, naming the table
      (`dataLag: 0` plus `dataMonthLag` counts as both)
- [ ] **The two lags differ as designed:** `dataLag` shifts the axis (`effective_year = year - lag`),
      `dataMonthLag` caps it (`year` is already the data year). Asserted, not assumed
- [ ] Absent `dataMonthLag`, behavior is **bit-identical** to today for every existing table
- [ ] Caps Shape A (`monthlyYTD`) and Shape B (cartesian month list)
- [ ] **Prior year carries a ceiling** when the lag crosses a year boundary (not hardcoded `"12"`)
- [ ] Correct across all 12 simulated months (sweep, using P0) — the boundary should fall out of
      `minusMonths`, so a failure here means the arithmetic was special-cased somewhere
- [ ] Effective period / `period_start` / `period_end` consistent with the offset period
- [ ] Composes correctly with `releaseMonth`

---

## Test plan for the engine changes

All offline, using P0. See the validation plan for the table-level tests (R1–R6).

| Area | Tests |
|---|---|
| **P0** | No-override equivalence; month-base normalisation (site 2's 0-based semantics preserved); override isolation between tests |
| **P1** | Start extension at various N; no extension without a window; `minYear` floor; `step > 1` |
| **P2 arithmetic** | 12-month sweep × several years × {`dataLag` only, `dataMonthLag` only, both, plus `releaseMonth`} |
| **P2 boundary** | **January with `dataMonthLag: 2`** — current year absent, prior year caps at 11. The single highest-risk assertion in this workstream |
| **P2 Shape B** | Cartesian month combos past `vintageCeiling` are dropped; comparison uses `effective_year` |
| **Reopen** | R3/R4 from the validation plan — the currently untested `reopenTrailingWindowPeriods` path |

---

## Sequencing

```
P0 ──┬──> P1 ──> table window changes (Groups 1-5)
     └──> P2 ──> EIA dataMonthLag migration (Group 6)
```

P1 and P2 are independent and can proceed in parallel once P0 lands. **P0 is not optional
overhead** — without it the January boundary case cannot be tested until January, and every test
written against the real clock silently drifts (already demonstrated in `FreshnessSkipGateTest`,
whose hardcoded 2023/2024 fixtures have aged out of a 2-period lookback).

---

## Risk register

| Risk | Severity | Mitigation |
|---|---|---|
| P0 month-base normalisation breaks `monthlyYTD` | **High** — site 2's correctness depends on the 0-based quirk | No-override equivalence test asserted before any semantic change |
| P2 boundary right in Shape A, wrong in Shape B | **High** — silent, only visible in Jan/Feb | One shared `vintageCeiling` helper; sweep both shapes |
| P1 widening explodes fetch cost | Medium | Per-period freshness gate already skips unchanged; verify on Run 2 of the soak |
| P1 conflicts with an operand-set `startYear` | Medium | `min()` semantics + explicit log line; document that the window floor wins |
| Clock override leaks between tests | Medium | Reset in `@AfterEach`; assert default is the real clock |
| EIA migration is a coverage change | **High** — ~2 years recovered per table | Per-year row counts before/after; DQ bucket only |
