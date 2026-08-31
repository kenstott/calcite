# Requirement: `dataMonthLag` + daily-ETL lookback self-sufficiency

**Date:** 2026-08-30
**Status:** Requirement. Detailed design in [engine-implementation-spec.md](engine-implementation-spec.md).

**Option A — the dimension self-extends (recommended).** `resolveYearRange` widens its own start
year to satisfy the table's `lookbackPeriods`, regardless of the operand's `startYear`.

- *Pro:* structural — the table declares its need and the guarantee holds in every run mode; no
  script can silently defeat it. Keeps the dependency in the model, per the schema-YAML rule.
- *Pro:* per-table precision — `economic_census` needs 8 years, `nps_visitation` needs 2; a
  schema-wide floor would over-fetch for every table to satisfy the widest one.
- *Con:* the operand's `startYear` stops being the last word on the range, which is surprising.
  Needs a clear log line whenever the range is widened, naming the table and the reason.

**Option B — the worker computes a schema-wide floor.** `common.sh` sets
`startYear = currentYear − max(lookbackPeriods over the schema's tables)`.

- *Pro:* no engine change; the range stays exactly what the operand says.
- *Con:* the shell would have to read every table's `lookbackPeriods` out of the YAML, putting
  model knowledge in the launch script — against the "env vars and model knowledge belong in the
  schema YAML" rule.
- *Con:* over-fetches every table in the schema to the widest table's window.

**Recommendation: Option A.** It is the structural fix; B pushes model knowledge into shell and
over-fetches.

### Cost

The daily run **re-probes** its whole lookback every day. For most tables that is not a re-fetch:

- **Probe-type freshness costs one HEAD per unchanged period** — no body transferred. `county_qcew`
  is ~75 MB/year and uses `etag` specifically for this reason: a 2-year lookback costs 2 HEADs.
- **`hash`-type must download to compare**, but an unchanged period still produces **no Iceberg
  snapshot** ([EtlPipeline.java:578-581](../../file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L578)).
- Erring wide costs bandwidth. Erring narrow serves stale data **forever, silently**.

Two cost cases need attention during design:

- **Large files gated by `hash`** must download in full to be compared, every day, for every
  period in the lookback. Where the source supports a usable ETag, prefer `etag` /
  `last_modified` — this is now a daily recurring cost, not an occasional one. Note the audit
  found sources where probe types are **unusable** and `hash` is mandatory: the FEC regenerates
  every cycle file daily (Last-Modified always moves), and BLS restamps every file under
  `/pub/time.series/jt/` with an identical ETag regardless of content.
- **High-cardinality combos.** `industry_gdp` is 2 frequencies × 22 industries = 44 combos/year;
  a 6-year lookback is ~264 probes per daily run for that table alone.

> Because the lookback becomes a *daily recurring* cost rather than a backfill-time one, the
> freshness *type* chosen per table matters more than it did. Reviewing probe-type availability
> for the large-file tables is worth doing as part of this work.

### Acceptance criteria

Tested against the two mechanisms **separately**, since they cover different states and a test
that passes via one can mask the other being broken.

**(a) Year-range extension — the reachability guarantee**

- [ ] In **daily** mode, a table with `lookbackPeriods: N` generates all N periods as combos
- [ ] The widening is logged per table, naming requested vs. effective start year
- [ ] A year inside the window that was **never ingested** is ingested by a daily run —
      **with lookback reopening disabled**, to prove (a) carries this case alone
- [ ] Self-healing: after simulated run failures, the missed periods are ingested on the next
      successful run
- [ ] Tables with **no** `lookbackPeriods` keep today's single-year daily behavior

**(b) Lookback reopening — the revision guarantee**

- [ ] A period the tracker marked **complete** is force-reopened and re-evaluated
- [ ] A revision landing in year `currentYear − 4` on a `lookbackPeriods: 6` table is picked up by
      a daily run, with **no historical backfill involved** — the headline acceptance test
- [ ] Reopening is a **no-op on an already-unprocessed** period (it must not double-count or
      re-add) — the behavioral statement of "acts only on completed periods"
- [ ] A year **outside** the window that is marked complete stays closed

**Per-period freshness decision (shared by both)**

- [ ] An **unchanged** prior period is skipped, and for probe-type freshness is skipped
      **without downloading the body** (assert on request count/method, not just the outcome —
      a passing "no snapshot written" assertion would hide a full download)
- [ ] Unchanged periods produce **no** Iceberg snapshot (the cost model must hold)
- [ ] A period whose token was never recorded ingests rather than being skipped
- [ ] A failed commit leaves **no** token recorded, so the next run retries that period

---

## Sequencing

1. **Requirement 2 first, or jointly with Requirement 1.** Until the daily run generates the
   lookback, no `lookbackPeriods` value change is observable in production.
2. Then the audit's Priority 1 window changes become meaningful.
3. Then migrate the EIA tables from `dataLag: 2` to `dataMonthLag: 2`, treating it as a backfill.

## Verification that this is real, not inferred

Both defects were confirmed by reading the code, not by reasoning about intent:

| Claim | Evidence |
|---|---|
| Month ceiling is hardcoded to "last completed month" | [DimensionIterator.java:189](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L189) |
| Completed years are assumed full (`month = "12"`) | [DimensionIterator.java:204-205](../../file/src/main/java/org/apache/calcite/adapter/file/etl/DimensionIterator.java#L204) |
| Cartesian `month` list axes exist and are uncapped | `eia_electricity_generation`, `eia_fossil_fuel_production`, `eia_refinery_operations` — `month: {type: list, values: [01..12]}` |
| `lookbackPeriods` reopens only already-generated combos | `reopenTrailingWindowPeriods` iterates `combinations` by index |
| Daily mode generates a single year | [common.sh:626,645](../../govdata/scripts/parallel/common.sh) |


