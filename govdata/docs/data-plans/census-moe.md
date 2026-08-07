# Data Plan: ACS Margin-of-Error (MOE) sibling tables

**Status:** Proposed
**Schema:** `census`
**Author:** (kenstott)
**Date:** 2026-06-13

---

## 1. Motivation

Every ACS estimate this catalog loads (`median_household_income`, `bachelors`,
`below_poverty`, `age_5_to_17`, …) is a **sample** estimate with a published **margin of
error** — the 90% confidence half-width the Census Bureau ships alongside each value. The ETL
currently loads only the estimate (`…E`) variables and drops the margins. `census.md` already
notes this ("MOE columns available in detailed data exports") and `package-info.java` lists
"margin of error columns for statistical accuracy" as intended — neither was built.

The gap matters most for exactly the analysis this data invites: **small-population units.** At
state grain (n≈51) a Wyoming or DC estimate can carry a margin wide enough to flip a ranking;
without the MOE, a fuzzy estimate is treated as exact. Loading the margins lets a query (or a
`describe_table`-reading agent) flag "this estimate is too noisy to rank" instead of reporting
false precision — the data-level form of the "effect size *and confidence*" principle.

## 2. Design decision: two shapes, pick by whichever stays clean

There are two viable shapes; the choice is a matter of which is *less awkward*, not correctness —
both are non-destructive in the sense of §2a (no existing capability lost).

- **Shape A — inline `_moe` columns via re-ingest (recommended default).** Add each
  `<measure>_moe` column directly to its estimate table and re-ingest. Simplest to build (the
  existing fetch/transform/write path handles it once the mapper yields the `M` variables), no FK
  machinery, one table to query. Non-destructive because census data is re-ingestable from the
  Census API and the recreated table is a **strict superset** (all prior columns + the new
  `_moe` columns). Use this unless there's a concrete reason not to.

- **Shape B — separate `<table>_moe` table, FK-linked.** MOEs live in a sibling table keyed to
  the estimate row. Keeps estimate tables byte-unchanged and MOEs opt-in, but needs a valid
  unique key on the estimate table for the FK target and doubles the table count. Choose this
  only if keeping the estimate tables untouched genuinely matters and the FK stays clean.

Given the re-ingest is acceptable, **Shape A is the recommended path**; Shape B is documented
below for the case where it's clearly cleaner. The rest of this plan (derivation, sentinels,
scope) is identical for both.

```text
census.acs_income            census.acs_income_moe   (Shape B)
  (state, county_fips,   1───1  (state, county_fips, geography, year,
   geography, year,   ◄──FK───    median_household_income_moe,
   median_household_income,       per_capita_income_moe)
   per_capita_income, …)
```

- **Grain:** one MOE row per estimate row (1:1), same geography/year key.
- **Columns:** the estimate table's **key columns** + one `<measure>_moe` column per estimate
  measure. No `_moe` for geography/year/type keys themselves (those are exact).

## 2a. Constraint: additive in capability, never a loss

The invariant — for **whichever shape §2 selects** — is that **no existing capability is lost**:
no column, measure, or view that works today may stop working. This is stronger and more precise
than "never rewrite a table," because census parquet is **fully re-ingestable from the Census
API** — dropping and recreating a table is a *source refresh*, not data loss.

Both shapes satisfy the invariant:

- **Shape A (inline, recommended)** recreates each estimate table as a **strict superset** — every
  prior column retained, same partition layout, only the new `_moe` columns added.
- **Shape B (separate table)** leaves the estimate tables entirely untouched; the sole
  estimate-table change is an optional, additive `primaryKey` declaration (metadata only, no data
  change) to anchor the FK — and only when that key is genuinely unique.

Non-negotiables (both shapes):

- Never drop or rename a pre-existing column; never change the partition layout — existing queries
  and views must be unaffected.
- Never dedup/filter in a way that changes which rows an estimate table returns. If a table's
  natural key is not unique (pre-existing duplicate rows, a known failure mode elsewhere), do NOT
  force a PK for Shape B — use Shape A for that table and file the duplication as a separate bug.
- Reversible: removing the `_moe` columns/tables and any added PK returns the schema to its prior
  state.

## 3. Scope

- **In:** every **mapper-driven ACS estimate table** — i.e. tables whose columns come from
  `ConceptualVariableMapper`/`CensusVariableMapper` `Bxxxxx_yyyE` variables. Known set today:
  `acs_income`, `acs1_income`, `acs_poverty`, `acs_education`, `acs_age`,
  `acs_earnings_by_education`, `acs_income_by_nativity`, `acs_income_distribution`,
  `acs_nativity_by_education`, `acs_housing_tenure`, `acs_health_insurance`,
  `acs_vehicle_access`, and any other `acs_*`/`acs1_*` estimate table (derive from the mapper,
  do not hardcode — a table added later must get its `_moe` sibling automatically).

- **Out:**
  - **Decennial** tables (100% count, no sampling error → no MOE).
  - **Derived views** (`education_attainment`, `poverty_rate`, `income_summary`): these compute
    from base tables; a correct MOE for a ratio is a variance propagation, not a loaded column —
    out of scope here (a follow-up could compute view-level MOEs, but that is a separate design).

## 4. Derivation rule (the mechanical part)

For each estimate variable in a table's map, the MOE variable is the same base code with the
suffix `E`→`M`, and the friendly name gains a `_moe` suffix:

```text
B19013_001E → median_household_income        (estimate, stays in acs_income)
B19013_001M → median_household_income_moe    (margin,   goes to acs_income_moe)
```

- The Census API accepts estimate and margin variables **in one request** — no extra API calls,
  the `M` variants ride along with the existing `E` fetch.

- **Sentinel handling (required):** ACS uses `-555555555` (and `(X)`, `*`, `**`, `-`, `N`) to
  mean "MOE not appropriate / not applicable / controlled to a total." These MUST be normalized
  to NULL, mirroring the existing negative-sentinel handling already used for CCD counts — a
  sentinel silently stored as a real margin would be worse than a missing one.

## 5. Implementation

Steps 1–2 are shared. Step 3 differs by shape; do **3A** (recommended) or **3B**.

**Wiring gotcha (verified in source).** Do NOT derive the `_moe` entries in Java inside the
shared `ConceptualVariableMapper.getVariablesForTable`. The instance overload
(`getVariablesForTable(tableName, dimensions)`, used by the fetch at `CensusApiClient:411`)
re-resolves every conceptual name through `getVariableMapping(...)` and **drops any name it can't
find in the config** (`ConceptualVariableMapper.java:470`). A Java-derived `<name>_moe` has no
config entry, so it would be dropped from the *fetch* but kept by the *transform* → the `M`
variables are never requested and every `_moe` column comes back null. Therefore:

1. **Config, not Java — `census-variable-mappings.json`.** For each ACS `conceptualVariable`
   whose mapping is `Bxxxxx_yyyE`, add a sibling `conceptualVariable` `<name>_moe` → `Bxxxxx_yyyM`
   (same dataset, numeric dataType), and add `<name>_moe` to the `conceptualColumns` of every ACS
   table that lists `<name>`. This reuses the entire existing fetch/transform/instance-mapping
   pipeline with no mapper logic change, and survives the `:470` filter because the `_moe`
   conceptual now *is* in the config. Script-generate it (derive from the existing `…E` entries) —
   do not hand-edit dozens of entries. Max 6 estimates per table → ≤12 vars with margins, so the
   Census 50-variable request cap is never approached (no batching needed).

2. **Sentinel → null — DONE.** `CensusDataTransformer.convertValueByType` now returns null for any
   `_moe` column whose value is negative or a non-numeric marker (a real margin is never
   negative; `-555555555`/`(X)`/`*`/`-`/`N` all mean "no margin"). Guarded to `_moe` columns, so
   estimate columns are unchanged. This is the only Java change and it is already in place.

- **3A. Inline columns (recommended).** Append the `<measure>_moe` columns to the estimate
  table's write; the existing `CensusDataTransformer` path handles them with no split. In
  `census-schema.yaml`, add the `<measure>_moe` columns (nullable double/long) to each in-scope
  table with a per-column `comment:` "90% CI margin of error for `<measure>`; Census sentinels
  → null." Re-ingest. Strict-superset guarantee (§2a) holds: no prior column is removed.

- **3B. Separate table + FK (only if clearly cleaner).** Route MOE columns + the shared key
  columns to a new `<table>_moe` parquet partition (`CensusSchemaFactory` emits the second table
  per ACS table). In `census-schema.yaml`, add a `<table>_moe` table (same partition pattern and
  key columns, `<measure>_moe` columns, a table `comment:` naming it as the sibling's margins),
  and a `foreignKeys:` constraint from its keys to `<table>`. **FK target prerequisite:** the
  estimate table needs a declared `primaryKey`/unique on `(state, county_fips, geography, year)`;
  add it as additive metadata **only if that key is actually unique** — if not (pre-existing
  duplicate rows), fall back to 3A for that table rather than forcing a key (§2a).

## 5b. Dedicated (expression-defined) ACS tables

A second set of ACS tables is **not** on the `conceptualColumns` path — they are defined directly
in `census-schema.yaml` with per-column `expression:` over raw source variables, e.g.
`TRY_CAST(src."B20004_001E" AS BIGINT)` (tables: `acs_earnings_by_education`,
`acs_income_distribution`, `acs_income_by_nativity`, `acs_nativity_by_education`, `acs_age`,
`acs_vehicle_access`, `acs1_income`, and similar). Part 1's config change does **not** reach
these; they need their own `_moe` columns. They split into two kinds, and the distinction is
statistical, not cosmetic:

- **1:1 raw-variable columns** (e.g. `median_earnings_total` = `src."B20004_001E"`). Add a sibling
  `<name>_moe` column whose expression reads the margin twin and nulls the Census sentinels:
  `CASE WHEN TRY_CAST(src."B20004_001M" AS BIGINT) < 0 THEN NULL ELSE TRY_CAST(src."B20004_001M" AS BIGINT) END`.
  Correct and safe. The margin variable must also reach the **fetch** — verify whether the fetch
  derives its variable list from the `src."…"` references in the expressions (in which case adding
  the `_moe` expressions is sufficient) or from a separate list that must also gain the `…M` codes.

- **Derived columns** — a bracket built by **summing** several variables (e.g. `income_10k_to_25k`
  = `B19001_003E + B19001_004E + B19001_005E`), a **ratio/proportion** (rates), or an index
  (`B19083` Gini). For these the margin is **NOT** the sum of the component margins. Census's
  published rules apply:
  - **Sum/aggregate:** `MOE_agg = SQRT(Σ MOE_i²)` over the summed components.
  - **Proportion/ratio:** the Census derived-ratio MOE formula (with the under-the-root
    correction), not a component sum.
  - **A naive sum-of-margins is statistically wrong and MUST NOT ship** — a wrong margin is worse
    than a missing one, and misstates confidence in exactly the direction this whole effort exists
    to prevent. If the correct propagation for a given derived column is not implemented, that
    column gets **no** `_moe` (documented as omitted), never an approximate one.

Both kinds are additive (§2a): new `_moe` columns only, all prior columns retained, populated on
re-ingest. Recommended sequencing: land the **1:1** `_moe` columns first (mechanical, correct),
then the derived-column margins as a second pass once each propagation formula is verified.

## 6. Rollout

- Code changes above produce **empty** `_moe` tables until census is re-ingested — populating
  them requires an ACS ETL run (the pool), not just a jar rebuild.

- Verify with `verify-tables` (each in-scope table — or its `_moe` sibling under 3B — exists and
  returns a row) and a spot check that a known small-state margin (e.g. Wyoming, DC) is non-null
  and plausibly large relative to its estimate.

## 7. Downstream value

- Enables uncertainty-aware queries: filter/flag estimates whose MOE exceeds a share of the
  estimate (Census's own reliability guidance uses the coefficient of variation = MOE/1.645 ÷
  estimate).

- Directly serves the question-quality work (`askamerica-engine/docs/question-quality-spec.md`):
  a `small_n`/noisy-estimate diagnostic can cite an actual margin instead of a generic "n≈51"
  caveat.
