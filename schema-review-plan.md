# Schema Mode/Freshness Audit & Review Plan

**Date:** 2026-08-30
**Scope:** All 406 partitioned tables across 26 GovData schemas
**Status:** Audit complete; 36-table revision research complete; changes not yet applied


---

## Executive Summary

- **✅ 370 tables correct** — no action
- **⚠️ 36 tables researched** — 18 need a change, 18 confirmed correct as-is
- **❌ 0 tables in the wrong write mode** — the 2 initially flagged were false positives

**No `overwritePartitions` changes are required anywhere.** The entire remaining workload is
`lookbackPeriods` tuning on 18 tables.

### The three findings that matter most

1. **`lookbackPeriods: 2` is systematically too narrow on BEA-sourced tables.** BEA states it
   "typically updates the past five years of statistics," and its routine annual update
   outruns a 2-year window *every cycle*. Six tables need `6`.

2. **`lookbackPeriods` is structurally inert on 11 of the 36 tables.** It keys off
   `combo["year"]`; a table with no `year` dimension can never enter the window. No revision
   research can change that — the mechanism cannot fire. Correctly, most of those 11 already
   use a whole-file `last_modified`/`hash` re-read, which is *strictly stronger* than a window.

3. **A likely-larger defect surfaced incidentally:** five EIA tables carry `dataLag` in
   **years** against sources that publish with a **two-month** lag, so the most recent 2–3 data
   years may simply be absent. That is a coverage bug, not a freshness bug, and is tracked
   separately below.

### Verified mechanism facts (read from source, not assumed)

| Fact | Evidence |
|---|---|
| Default `overwritePartitions` is **`true`** when an `iceberg:` block is present | [MaterializeConfig.java:874](file/src/main/java/org/apache/calcite/adapter/file/etl/MaterializeConfig.java#L874) builder default; applied at [IcebergMaterializationWriter.java:263](file/src/main/java/org/apache/calcite/adapter/file/etl/IcebergMaterializationWriter.java#L263) |
| `overwritePartitions: false` is legitimate **only** for `dataset_type: computed_delta` | 3 tables: `gleif_entities`, `gleif_relationships`, `calendar` |
| `lookbackPeriods` reads `combo["year"]` → **inert with no year dimension** | [EtlPipeline.java:3302-3319](file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L3302) |
| `combo["year"]` is the **publish** year; `effective_year = year - dataLag` is the data year | [DimensionIterator.java:429-450](file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L429). Reopened data years = `[Y-L-N+1 .. Y-L]` |
| `lookbackPeriods` is **inert without a `freshness:` block** — `reopenTrailingWindowPeriods` returns early when `freshnessConfig == null` | [EtlPipeline.java](file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java) |
| It works with **any** freshness type, not just `hash` (`etag`/`last_modified` are fine) | same |
| Without `lookbackPeriods`, an in-progress year **freezes permanently** — `markPeriodComplete` has no calendar check | [EtlPipeline.java:3198-3206](file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L3198) |
| An over-wide window is **cheap**: unchanged bytes produce no Iceberg snapshot | [EtlPipeline.java:578-581](file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L578) |

**The last row is the key asymmetry: erring one year wide costs a re-fetch and a hash compare.
Erring one year narrow silently serves stale data forever.** When uncertain, go wider.

**The right classifier is `dataset_type` + presence of a `year` dimension** — not a keyword
match on names and comments. Any future audit should key on those.

---

## CHANGES REQUIRED (18 tables)

> All recommendations below are **research-derived and unvalidated in production.** Each cites
> the upstream policy it rests on. Apply and test per the plan in the Action section.

### A. Widen an existing window (8 tables)

| Table | Schema | Now | → | Why |
|---|---|---|---|---|
| `gdp_statistics` | econ | 2 | **6** | BEA annual update reopens ~5 yrs (2026 update: 2021–2026) |
| `industry_gdp` | econ | 2 | **6** | Revised in the same NEA annual update as NIPA, same open period |
| `ita_data` | econ | 2 | **6** | BEA: ITA "open for revision for at least the prior 3 years"; 2026 update covered 2021–2026 |
| `iip_positions` | econ | 2 | **6** | Same June release and open period as ITA |
| `fdi_direct_investment` | econ | 2 | **6** | Strongest case: 2026 update restated **2022–2025** off the 2022 BE-12 benchmark; a window of 2 caught half |
| `fred_indicators` | econ | 2 | **6** | BLS recomputes CPI seasonal factors each Feb, revising **5 prior years**; the curated series are all seasonally adjusted |
| `nsf_herd_by_institution` | research | 2 | **3** | NCSES solicits corrections against current + 2 prior years and restates microdata on amendment |
| `eia_fossil_fuel_production` | energy | 2 | **3** | EIA-914: 2 months rolling + PSA restates up to 10 prior years of oil; EIA calls a state final "prior to two calendar years back" — N=2 has zero margin |

### B. Narrow an existing window (1 table)

| Table | Schema | Now | → | Why |
|---|---|---|---|---|
| `eia_capacity_changes` | energy | 2 | **1** | The archived `december_generator{year}.xlsx` is an immutable point-in-time snapshot; EIA-860M corrections land in *later months' files*, a different URL. The window is only needed for the ~Feb publication lag. (N=2 is harmless; this is tidiness, lowest priority.) |

### C. Add a window where none exists (7 tables)

| Table | Schema | Add | Note |
|---|---|---|---|
| `county_qcew` | econ | `lookbackPeriods: 2` | Keep `type: etag` — BLS serves a strong ETag, so an unchanged year costs one HEAD, not a 75 MB download |
| `state_wages` | econ | `lookbackPeriods: 2` | Same QCEW bulk file as `county_qcew`; identical policy |
| `cde_police_employment` | crime | `freshness: {type: hash, lookbackPeriods: 3}` | No freshness block today |
| `communication_costs` | fec | `freshness: {type: hash, lookbackPeriods: 4}` | No freshness block today |
| `nps_visitation` | lands | `freshness: {type: hash, lookbackPeriods: 2}` | No freshness block today |
| `economic_census` | census | `freshness: {type: hash, lookbackPeriods: 8}` | No freshness block today |
| `lodes_workplace` | census | `freshness: {type: hash, lookbackPeriods: 4}` | No freshness block today |

Three of these carry a **non-obvious N** that must not be "simplified" later:

- **`economic_census: 8`** — the year axis is `step: 5` (2017, 2022, 2027…) with `dataLag: 0`, so
  in 2026 the newest vintage is already 4 years back. **Any N < 5 reopens nothing at all.** N=8
  reopens exactly one real combo (2022). Justified by Census re-releasing corrected files years
  late: 2017 NAICS 51 tables were revised 2020-06-02.
- **`communication_costs: 4`** — the dimension is **even-year election cycles only**. `2` in an
  even year reopens only that year; in an odd year it reopens nothing new. N=4 covers current +
  prior cycle in both even and odd calendar years. Also: use `hash`, **not** `last_modified` —
  the FEC regenerates every cycle file daily, so Last-Modified always moves (verified: every
  cycle file back to 2016 showed a 2026-08-30 timestamp).
- **`cde_police_employment: 3`** — FBI publishes **no closing date**; the endpoint serves
  `LATEST`, rebuilt monthly and silently absorbing amended prior-year submissions. The 2023
  release restated 2021 *and* 2022 (2022 violent crime went from −2.1% to +4.5%). The fetch is
  51 tiny per-state JSON calls, so the extra year is nearly free.

### D. Add a plain freshness gate, no window (2 tables)

| Table | Schema | Add | Why |
|---|---|---|---|
| `jolts_industries` | econ-reference | `freshness: {type: hash}` | Code list, not a time series — no values to restate, no year axis. **Must be `hash`:** BLS restamps every file under `/pub/time.series/jt/` with an identical Last-Modified and ETag on each monthly release, so `etag`/`last_modified` would fire monthly on unchanged content |
| `jolts_dataelements` | econ-reference | `freshness: {type: hash}` | Same |

---

## CONFIRMED CORRECT — NO CHANGE (18 of the 36)

### Genuinely never revised (3)

| Table | Evidence |
|---|---|
| `acs_employment` | Census corrects ACS errata **forward** into the next vintage rather than reissuing (errata 099, 142, 151). A published `year=Y` file never changes. |
| `cbp_establishments` | CBP FAQ verbatim: *"CBP does not revise data for prior years."* |
| `food_cpi` | Series are `CUUR…` = **unadjusted** CPI-U, which BLS considers *"final when released."* The famous 5-year restatement applies **only to seasonally adjusted** series. |

### `lookbackPeriods` inert (no `year` dimension) — and unnecessary (9)

Each already re-reads the whole file/response and replaces every year at once, which is
strictly stronger than a recent-N window.

| Table | Current gate | Why it is already right |
|---|---|---|
| `state_personal_income` | `last_modified` | BEA republishes `SAINC.zip` whole on each update, and those reach **1929–2024**. Whole-file reingest beats any window. |
| `world_indicators` | `last_modified` | **Full-history restatement source** (WDI revises back decades). One fetch per indicator returns all years; `overwritePartitions` replaces the indicator's entire history each run. |
| `ilostat_indicators` | `last_modified` | Same shape — ILO revises historical estimates when better sources appear; one gz CSV per indicator carries full history |
| `qwi_employment` | `hash` | **Re-releases the COMPLETE series every quarter.** Census explicitly warns against mixing releases. Needs *all* periods reopened, not recent ones. |
| `fred_series` | `hash` | Metadata catalog; "revision" is not meaningful |
| `state_minimum_wage_history` | `last_modified` | Statutory rates are settled law; changes are new-column appends |
| `bls_geographies` | *(none)* | `source: type: constants` — jar-local resource, **no upstream publisher**. Full re-read each run; a freshness gate would only add a way to miss a repo edit. |
| `naics_sectors` | *(none)* | Same |

### EIA windows that are correctly sized at 2 (6)

| Table | Why 2 is right |
|---|---|
| `eia_electricity_generation` | EIA-923 monthly is preliminary, restated **once** when the annual final lands ~Sept of year+1 |
| `eia_electricity_prices` | EIA-861 early (Aug) → final (Oct) in year+1, plus later re-posts (2024 final updated Dec 2025) |
| `eia_utility_annual` | EIA-861 archive ZIPs re-posted with corrections after the final |
| `eia_power_plants` | EIA-860 early (June) → final (Sept) in year+1; the 2nd year is late-posting insurance |
| `eia_crude_oil_imports` | PSM policy: revisions affect current year + up to 2 prior, finalized at the PSA each August |
| `eia_refinery_operations` | Same PSM policy |

### Correct value, wrong stated reason (1)

**`eia_petroleum_stocks`** — keep `lookbackPeriods: 2`, but **rewrite the comment.** EIA:
*"Absent significant errors, omissions, or natural disasters, we never revise the data
published in the WPSR."* The window here is for **accrual, not revision**: `dataLag: 0` with a
year-grain partition holding weekly rows means the current year keeps gaining ~52 rows, and
`markPeriodComplete` would otherwise freeze it after the first successful run. The second year
catches late-December weeks published after the calendar rolls over.

> This is the clearest example of why the comments matter as much as the values: the config is
> right, but anyone trusting its stated rationale would draw the wrong conclusion about the
> source — and might "correct" it to 1 or 0.

### A comment repeated at 10 sites that is half wrong

`energy-schema.yaml` carries this identical block at lines 222, 371, 471, 641, 840, 978, 1309,
1520, 1645, 1760:

> *"EIA states its monthly series are 'preliminary, likely to be revised the following month,'
> plus a later annual revision — continuous revision, not a Jan-Feb-only window."*

That is EIA's *Monthly Energy Review* blanket statement, not a per-program policy. It is
accurate for the EIA-914 petroleum/gas series (`eia_fossil_fuel_production`) and **inaccurate
for the electric-power series**, whose own technical notes say monthly data are revised **once**
and then final. Replace per-table with the program-specific rationale above.

---

## PHASE 2 — 21 UNVERIFIED TABLES CARRYING `lookbackPeriods: 2`

**This audit only researched the 36 tables flagged as anomalous.** A completeness check found
**21 more** tables that already carry `lookbackPeriods: 2` and were auto-classified CLEAR on the
first pass **purely because the value was present** — never verified against source policy.

Given that BEA revises ~5 years and the BEA tables in scope all needed `6`, several of these are
likely mis-sized by the same reasoning:

**Probable BEA-sourced (likely need 6, by the same evidence):**
`national_accounts`, `regional_income`, `regional_price_parities`, `state_industry`,
`fdi_activities`

**CPI tables that may share `food_cpi`'s wrong-justification problem** — check whether the
series are seasonally adjusted; if unadjusted, the window may be unnecessary:
`regional_food_cpi`, `metro_food_cpi`, `metro_cpi`, `regional_cpi`, `inflation_metrics`

> The BLS research explicitly flagged `regional_food_cpi`: its comment cites the CES/employment
> revision rationale, which does **not** apply to unadjusted CPI.

**Remaining, BLS/other — verify individually:**
`county_wages`, `employment_statistics`, `jolts_industry`, `jolts_regional`, `jolts_state`,
`labor_productivity`, `metro_industry`, `metro_wages`, `regional_employment`, `wage_growth`,
`eia_coal_mines`

All 21 have a `year` dimension, so their windows at least fire. The risk is **under-sizing**,
not inertness.

---

## SEPARATE DEFECTS (not freshness — track independently)

These surfaced during research and are out of scope for this plan, but two look more
consequential than anything above.

### 1. EIA `dataLag` may be years-vs-months — likely missing recent data ⚠️ HIGH

Five tables lag by 2–3 **years** against sources that publish with a ~2-**month** lag:

| Table | `dataLag` | Actual source lag |
|---|---|---|
| `eia_electricity_generation` | 2 yrs | EIA-923: "Aug 26 2026 release for June 2026 data" (~2 months) |
| `eia_electricity_prices` | 2 yrs | EIA-861 final ~Oct of year+1 |
| `eia_fossil_fuel_production` | 2 yrs | PSM: "within 60 days of the close of the reference month" |
| `eia_refinery_operations` | 2 yrs | Same PSM policy |
| `eia_crude_oil_imports` | 2 yrs | EIA-814: May 2026 data released Jul 31 2026 |

If confirmed, these tables are missing their most recent 2–3 data years entirely — a coverage
bug considerably larger than window tuning. The `dataLag: 3` on `eia_utility_annual` /
`eia_power_plants` **is** documented and justified in the YAML; these five carry no such
justification.

**Note the interaction:** if `dataLag` drops, `lookbackPeriods: 2` stops being belt-and-braces
on the electricity tables and becomes strictly necessary.

### 2. `qwi_employment` time dimension is hardcoded and stale ⚠️

The `time` dimension is a static 12-value literal, `2022-Q1 … 2024-Q4`. As of Aug 2026 that is
~6 quarters stale and will keep drifting. Needs a dynamic quarter-range dimension.

### 3. `state_personal_income` coverage gap ⚠️

`observedCoverage` reports `minYear: 1929, maxYear: 1957, distinctYears: 29`. BEA publishes this
series 1929–present. Appears to be loaded only through 1957.

### 4. Misleading anchor name (cosmetic)

`year_range_from_2020` in `econ-schema.yaml:331` actually starts at **2010**. Used by `ita_data`
and `gdp_statistics`.

---

## Action Plan

### Priority 0 — Correct the skill documentation (5 min)

`.claude/skills/govdata-data-cadence/SKILL.md`:
- [ ] It states `overwritePartitions: false` is "the default". The builder default is **`true`**
      ([MaterializeConfig.java:874](file/src/main/java/org/apache/calcite/adapter/file/etl/MaterializeConfig.java#L874)).
- [ ] Add the `dataset_type: computed_delta` exception to the "true is required on every table"
      rule, citing `gleif_entities` / `gleif_relationships` / `calendar`.
- [ ] Add a `lookbackPeriods` section: the retired `lookbackPeriods` was inert without a `year` dimension **and** inert
      without a `freshness:` block; it counts **publish** years, so reopened data years are
      `[Y-L-N+1 .. Y-L]`.

### Priority 1 — Apply the 18 changes

Order by risk, cheapest and safest first:

- [ ] **Batch 1 — pure value edits (9 tables).** The 8 widenings + 1 narrowing in sections A/B.
      No structural change; only a number moves.
- [ ] **Batch 2 — add `lookbackPeriods` alongside existing freshness blocks (2).**
      `county_qcew`, `state_wages`. Keep `type: etag`.
- [ ] **Batch 3 — add new `freshness:` blocks (7).** The 5 in section C plus the 2 plain-hash
      in section D. Higher risk: these tables have **no freshness gate at all** today, so this
      changes both *whether* they re-fetch and *whether* they skip writes.
- [ ] **Batch 4 — comment-only corrections.** `eia_petroleum_stocks` rationale; the 10 repeated
      energy-schema comment blocks.

Every edit must carry a YAML comment naming the upstream policy and the source URL. Each table's
recommended comment text is in the research record; do not paste a generic justification.

### Priority 2 — Phase 2 verification (21 tables)

- [ ] Group by source agency (BEA / BLS-CPI / BLS-other), research once per agency, apply to all
      its tables — the same method that made this round efficient.

### Priority 3 — File the separate defects

- [ ] EIA `dataLag` investigation (highest value of anything in this document)
- [ ] `qwi_employment` dynamic quarter dimension
- [ ] `state_personal_income` coverage gap
- [ ] `year_range_from_2020` rename

---

## Testing Strategy

An over-wide window is cheap and a narrow one fails silently, so tests should target the
**silent** failure modes:

**Per changed table:**
1. **Window actually fires.** Mark the table complete, run, and confirm the log line
   `Trailing window: force-reopened N combo(s)` names the expected count.
   For `economic_census` specifically, confirm it reopens **exactly one** combo (2022) — if it
   reopens zero, N is under the 5-year step and the change accomplished nothing.
2. **Write is still skipped when unrevised.** Re-run with no upstream change; confirm **no new
   Iceberg snapshot**. This is what makes a wide window cheap; if it fails, the cost argument
   collapses.
3. **Revision is actually caught.** Mutate a source fixture within the window; confirm the
   partition is rewritten.
4. **Window closes.** Confirm a year outside the window stays closed.

**For Batch 3 (new freshness blocks) additionally:**
5. Confirm the table still ingests correctly from cold — these tables have never had a freshness
   gate, so a misconfigured `type` could wedge them shut. Verify a first run writes data.

**Regression:** spot-check 2–3 unrelated tables per schema touched.

---

## Reusable Audit Script

```bash
# Correct classifier: dataset_type + year-dimension presence, NOT name/comment keywords.
# Flags: (a) a lookback on a table with no year dim  -> inert
#        (b) a lookback with no freshness block       -> inert
#        (c) overwritePartitions: false without computed_delta -> real defect
#        (d) year-dimensioned table with no lookback -> needs a revision decision
```

Store as `scripts/audit_schema_modes.py`; re-run after each batch and diff against this document.

---

## Follow-Up Schedule

- **After each batch:** re-run the audit script, confirm the expected deltas and nothing else
- **Quarterly:** full re-audit
- **Annually:** re-check source revision policies — they change (BLS moved JOLTS to NAICS 2022;
  FBI moved to monthly releases in Aug 2025)
- **On announcement:** BEA comprehensive revisions (~every 5 yrs, next ~2028) and LODES
  format-version bumps restate **full history**. No lookback can catch these — they
  need a deliberate full-table reprocess. Watch for the announcements.

---

## Sources

Research is recorded per-table above. Primary references:

**BEA** — [2026 annual update info](https://www.bea.gov/information-updates-national-regional-economic-accounts) ·
[2023 comprehensive update](https://www.bea.gov/node/20871) ·
[ITA/IIP revision policy](https://www.bea.gov/news/us-international-transactions-and-investment-position-release-additional-information)

**BLS** — [QCEW revisions](https://www.bls.gov/cew/revisions/) ·
[QCEW Handbook of Methods](https://www.bls.gov/opub/hom/cew/presentation.htm#revisions) ·
[CPI technical notes (5-yr SA rule)](https://www.bls.gov/cpi/technical-notes/)

**Census** — [Economic Census errata](https://www.census.gov/programs-surveys/economic-census/data/errata-notes.html) ·
[CBP FAQ](https://www.census.gov/programs-surveys/cbp/about/faqs.html) ·
[QWI data notices](https://lehd.ces.census.gov/doc/QWI_data_notices.pdf) ·
[LODES TechDoc 8.3](https://lehd.ces.census.gov/data/lodes/LODES8/LODESTechDoc8.3.pdf)

**EIA** — [EIA-914 revision policy](https://www.eia.gov/petroleum/production/pdf/eia914revisionpolicy.pdf) ·
[EPM technical notes](https://www.eia.gov/electricity/monthly/pdf/technotes.pdf) ·
[PSM explanatory notes](https://www.eia.gov/petroleum/supply/monthly/pdf/psmnotes.pdf) ·
[WPSR revision FAQ](https://www.eia.gov/tools/faqs/faq.php?id=1398&t=6)

**Other** — [FBI UCR data quality guidelines](https://ucr.fbi.gov/data-quality-guidelines-new) ·
[NPS VUStats FAQ](https://www.nps.gov/subjects/socialscience/statistics-faq.htm) ·
[NSF NCSES HERD technical notes](https://ncses.nsf.gov/surveys/higher-education-research-development/2023) ·
[World Bank data errata](https://datahelpdesk.worldbank.org/knowledgebase/articles/906522-data-updates-and-errata) ·
[ILO modelled estimates](https://ilostat.ilo.org/methods/concepts-and-definitions/ilo-modelled-estimates/)

---

## Implementation Files

- [FreshnessConfig.java](file/src/main/java/org/apache/calcite/adapter/file/etl/FreshnessConfig.java) — `lookbackPeriods` parsing
- [EtlPipeline.java](file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java) — `reopenTrailingWindowPeriods`, `isWithinTrailingWindow`, `markPeriodComplete`
- [DimensionIterator.java](file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java) — `injectEffectiveYear`
- [MaterializeConfig.java](file/src/main/java/org/apache/calcite/adapter/file/etl/MaterializeConfig.java) — `overwritePartitions` default
- [IcebergMaterializationWriter.java](file/src/main/java/org/apache/calcite/adapter/file/etl/IcebergMaterializationWriter.java) — replace vs. append commit
