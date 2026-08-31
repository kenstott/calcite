# DEFECT: per-unit freshness skip + coarse partition = silent data loss

**Date:** 2026-08-31
**Severity:** High — silent, intermittent data loss
**Status:** Confirmed by code inspection. **Not yet confirmed against production data.**
**Found via:** the partition-scoping hazard identified while redesigning `qwi_employment`

---

## The defect

When a table's Iceberg partition is **coarser than its fetch unit**, and per-unit freshness skips
some units, the commit replaces the partition with **only the units processed this run** — silently
dropping every skipped unit's data.

### Chain of evidence

1. **Fetch is per-unit.** `fia_plots` fetches one zip per state:
   `https://apps.fs.usda.gov/fia/datamart/CSV/{state}_CSV.zip`, over **51 states**
   (`/lands/fia-states.json`).

2. **Freshness skips per state.** `freshness: {type: etag}`. Per-unit skipping was **deliberately
   enabled** for exactly this shape ([EtlPipeline.java:780-787](../../file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L780)):

   > *"Per-unit freshness applies to any templated multi-unit fetch — period dimensions
   > ({year}/{month}) AND non-period fetch dimensions ({state}, ...). Gating only on hasPeriod
   > silently disabled the gate for state-only tables (**e.g. FIA fia_plots**), which declare
   > freshness:last_modified to skip unchanged states but instead re-fetched and re-materialized
   > every state on every resume run."*

3. **Partition is a single bucket.** `partition: {columns: [type]}` — all 51 states land in one
   partition, `type=fia_plots`.

4. **Replace mode is on.** No explicit `overwritePartitions`, so the builder default `true` applies
   ([MaterializeConfig.java:874](../../file/src/main/java/org/apache/calcite/adapter/file/etl/MaterializeConfig.java#L874)).

5. **The commit replaces the whole partition.**
   [IcebergTableWriter.java:280-298](../../file/src/main/java/org/apache/calcite/adapter/file/iceberg/IcebergTableWriter.java#L280)
   calls `table.newReplacePartitions()` — javadoc: *"Replaces all data in the affected partitions
   with the given files."* `commitInChunks` passes only the files written **this run**
   ([IcebergMaterializationWriter.java:1429-1434](../../file/src/main/java/org/apache/calcite/adapter/file/etl/IcebergMaterializationWriter.java#L1429)).

### Outcome by run shape

| Run | Behavior |
|---|---|
| **No** state changed | No files written → `allDataFiles.isEmpty()` → early return, no commit. **Safe** |
| **All** states changed | All 51 written → partition replaced with all 51. **Safe** |
| **Some** states changed | Partition replaced with **only those states** → **the rest are dropped** |

The all-or-nothing cases are safe, which is what makes this intermittent and easy to miss. A
subsequent full-update run restores the data, so the table appears to "heal" — masking the loss.

### This looks like a regression

Before per-unit freshness was extended to non-period fetch dimensions, these tables "re-fetched and
re-materialized every state on every run" — wasteful, but **safe**, because every run wrote all 51
states. The change that made them efficient is what opened the data-loss path. The fix addressed
the waste without accounting for the partition being coarser than the newly-skippable unit.

---

## Scope: 12 tables

Per-unit skip is possible **and** a multi-valued fetch dimension is absent from the partition key
**and** replace mode is on. Four exemptions apply, each established by measurement rather than
inspection:

| Exemption | Why a partial skip is impossible | Removes |
|---|---|---|
| `hash` without a lookback | Pipeline-scoped — every unit rewrites each run | many |
| Single-valued dimension | Cannot distinguish two units | 7 × `patent_*` |
| `valueSource` resolution | Partition column covers the dimension it sources from | 13 tables |
| **Unit-invariant freshness probe** | A constant `probe_url` yields the **same token for every unit**, so they cannot diverge | **6 × `fda_*`, 2 × cyber-vuln** |

| Schema | Tables | Freshness | Probe varies per unit? |
|---|---|---|---|
| lands | 8 × `fia_*` / `forest_*` | `etag` | yes — `{state}` templated |
| econ | `fred_indicators`, `regional_income`, `regional_price_parities` | `hash` + lookback | yes — content-derived |
| environment | `air_quality_daily` | `last_modified` | yes — `{param}` templated |

### The unit-invariant exemption, measured

All 6 `fda_*` tables probe a **constant** URL with an endpoint-level field —
`probe_url: https://api.fda.gov/download.json`, `version_field: results.drug.event.export_date`.
Every fetch unit therefore receives an identical token: either all skip or all process, and the
partial case cannot arise. The 2 cyber-vuln tables have the same shape (a constant NVD
`resultsPerPage=1` count probe).

**Set movement:** 20 → **12** today; −1 when `regional_price_parities` loses its lookback; +2 when
`cde_police_employment` and `economic_census` gain `hash` gates → **13 after the redesign.**

---

## Publisher behavior changes the Layer 2 answer

Two measurements (2026-08-31) removed most of the planned partition work.

### FIA republishes every state in one pass — per-state skipping buys nothing

Per-state `Last-Modified` across 10 states spans a single **7-hour window**
(28 Aug 19:43 → 29 Aug 02:59). FIA regenerates all 51 zips sequentially in one bulk run.

Consequences:
- When FIA republishes, **all** states change, so the ~23 GB is incurred either way. Partitioning
  by `state` would not reduce it.
- The partial-update case essentially never occurs — which is exactly why the defect has left no
  trace (7 of 8 tables hold all 51 states).
- **Layer 2 for FIA is therefore NOT justified.** Previously marked "required"; a partition change
  plus a 23 GB full reprocess would buy nothing.

Layer 1 alone is sufficient: between republishes it costs 51 HEADs and skips; after one it fetches
everything, which is correct rather than wasteful.

### openFDA re-exports wholesale — per-file skipping could never fire

`download.json` carries a single endpoint-level `export_date` (2026-08-26) for all 1,767
`drug.event` files. HEAD requests confirm the files themselves are rewritten together:

| File | `Last-Modified` |
|---|---|
| `2004q3/…0001-of-0005` | 31 Aug 2026 **15:12:38** |
| `2010q3/…0010-of-0010` | 31 Aug 2026 **15:12:53** |
| `2024q4/…0002-of-0029` | 31 Aug 2026 **15:15:06** |

A 2004 quarter — whose content cannot have changed — is rewritten in the same 2.5-minute window as
a 2024 one. Per-file `Last-Modified`/`ETag` is therefore useless as a change signal, and the
schema is already right to gate on the endpoint-level `export_date` instead.

**Layer 2 for `fda_*` is not justified either**, and the earlier proposal to have
`OpenFdaPartitionResolver` emit a year is unnecessary.

---

## What has NOT been established

- **No production evidence.** This is confirmed from code and config only. Whether it has actually
  fired depends on whether these sources ever update a subset of units — FIA state updates *are*
  staggered, so it is likely, but unproven.
- **No row counts checked.** Verifying requires querying the live tables (MinIO), which was not
  done here.

**Do not treat the tables as known-corrupt on this document alone.** The next step is measurement.

---

## Verification plan

1. **Measure current state.** For each of the 8 lands tables, count `DISTINCT state` against the 51
   in `fia-states.json` — a shortfall is direct evidence, and these are the highest-signal check.
   Then `DISTINCT series` on `fred_indicators` (expect 51) and `DISTINCT partition_file` on the 6
   `fda_*` tables. *(Not patents — they are exempt; see above.)*
2. **Check snapshot history.** Iceberg snapshot row counts over time — an abrupt drop followed by a
   recovery is the signature of a partial-update clobber.
3. **Reproduce deterministically.** Unit test: two units, freshness skips one, assert the skipped
   unit's rows survive the commit. This should fail today.

---

## THE FIX (designed)

Two layers. The engine layer makes the defect **impossible**; the per-table layer restores the lost
efficiency where it is worth it.

### Layer 1 — the freshness unit IS the partition key

**Group fetch units by their partition key. Probe every unit in a group. Skip the group only if
*all* its units are unchanged; otherwise process the whole group.**

This is safe by construction — a partition is either fully rewritten or untouched, so a replace can
never carry a subset — and it degrades gracefully rather than switching off:

| Partition vs fetch unit | Behavior | Equivalent to |
|---|---|---|
| Partition **==** fetch unit | Per-unit skip, unchanged | today's optimal path |
| Partition **coarser** | Skip at partition granularity | *new* — still useful |
| Partition is `[type]` only | All-or-nothing | the pre-regression behavior |

**Why not simply disable per-unit skip when unsafe** (the obvious fix): measurement showed it is
unaffordable. `fda_adverse_events` fetches openFDA's `drug.event` endpoint — **1,767 partition
files, 113 GB** (`api.fda.gov/download.json`, 2026-08-31). Forcing all-or-nothing there means
re-downloading 113 GB per run. Disabling is correct but not viable; grouping preserves the skip at
whatever granularity the partition allows.

Measured evidence for the two worst cases:

| Table group | Source volume per full pass | All-or-nothing viable? |
|---|---|---|
| 6 × `fda_*` (`drug.event` alone) | **113 GB / 1,767 files** | ❌ no |
| 8 × `fia_*` / `forest_*` | **~23 GB / 51 zips** | ❌ no |

Both therefore need Layer 2 as well — Layer 1 alone leaves them at `[type]`, i.e. all-or-nothing.

### Layer 1 implementation — engine derives the grouping

**The invariant that was left implicit:** per-unit freshness skip is safe **only if the partition
key is a function of the fetch unit** — i.e. two distinct fetch units can never land in the same
partition. When that holds, replacing a partition with one unit's files cannot disturb another's.

Compute it at pipeline init and **disable per-unit skip when it does not hold**:

```
fetchVars      = dimension names
singleValued   = dimensions resolving to exactly ONE value (cannot distinguish two units)
partitionVars  = partition columns, each mapped back through partition.valueSource
                 (e.g. `valueSource: {year: effective_year}` contributes `year`;
                  13 tables use this today — it must be resolved, not ignored)
derivedVars    = {effective_year, month_end}   // companions, not independent axes

perUnitSkipSafe = (fetchVars - derivedVars - singleValued) ⊆ partitionVars
```

Three subtleties, each of which makes the predicate wrong if missed:

1. **`valueSource` must be resolved, not ignored.** A partition column `year` declared as
   `valueSource: {year: effective_year}` still covers the `year` dimension. Treating the partition
   column name literally would mark 13 tables unsafe that are not.
2. **Single-valued dimensions are exempt.** A dimension with exactly one value (e.g.
   `type: [qwi]`) cannot distinguish two units, so it cannot cause a subset replace. Without this
   the predicate is over-conservative and would disable skipping on tables that are actually safe.
3. **No partition columns at all ⇒ unsafe** (unless every dimension is derived or single-valued).
   The whole table is one partition, so a subset replace clobbers everything. This falls out of the
   predicate naturally — `partitionVars` is empty — but is worth an explicit test.

Wire it into the existing gate at
[EtlPipeline.java:798](../../file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L798):

```java
perUnitFreshnessEnabled = (hasPeriod || templatedMultiUnitFetch)
    && freshnessConfig != null
    && !hashPreDownloadExcluded
    && perUnitSkipSafe            // <-- NEW
    && dataSource instanceof HttpSource;
```

and to the per-unit hash-token scoping at
[EtlPipeline.java:2158](../../file/src/main/java/org/apache/calcite/adapter/file/etl/EtlPipeline.java#L2158)
(`hashTokenPerUnit`), which has the same exposure via a lookback.

**Why this is the right layer:**

- **It cannot recur.** A future table with a coarse partition gets all-or-nothing freshness
  automatically. The invariant stops being implicit.
- **It only ever disables an optimization** — it never changes what data is written. Safe to ship
  ahead of everything else, and safe to ship without touching any schema YAML.
- **It degrades to the previously-safe behavior.** These tables re-materialize every unit each run,
  which is exactly what they did before per-unit skip was extended to non-period dimensions.
- **No purge, no reprocess, no partition change.** Nothing to migrate.

**Log loudly when it trips** — name the table and the offending dimensions, so the list of tables
wanting Layer 2 is generated by the system rather than maintained by hand:

```
Per-unit freshness DISABLED for 'lands.fia_plots': fetch dimension(s) [state] are not in the
partition key [type], so a per-unit skip could replace the partition with a subset. Falling back
to all-or-nothing. Add [state] to the partition to restore per-unit skipping.
```

### ⚠️ Layer 1's cost, measured — and why Layer 2 is REQUIRED for FIA

Layer 1 restores the pre-regression behavior: every unit re-fetched on every run. For most of the
20 tables that is negligible. **For the 8 FIA tables it is not.**

Measured zip sizes (`{state}_CSV.zip`, 2026-08-31):

| State | Size | |
|---|---|---|
| RI | 10 MB | smallest |
| CA | 236 MB | |
| TX | 354 MB | |
| OR | 423 MB | |
| GA | 610 MB | |
| MN | 689 MB | |

Mean across a 5-large-state sample: **462 MB** → **~23 GB per full 51-state pass.** All 8 FIA
tables read the same `{state}_CSV.zip` and none sets `rawCache` explicitly, so the multiplier
depends on cache sharing.

**Consequence:** for FIA, Layer 1 alone is correct but expensive; Layer 2 makes it correct *and*
cheap. **Ship both together for those 8 tables** — one-time ~23 GB reprocess, then only changed
states download. Do not leave FIA sitting on Layer 1 alone.

> This also sharpens the original defect: the per-unit skip was added to avoid exactly this 23 GB
> cost. The optimization was worth having — it was the missing partition-key invariant that made it
> unsafe, not the optimization itself.

### The partition-sizing rule (decides every Layer 2 case — no further design needed)

Layer 2 adds a dimension to the partition key, which trades partition count against partition size.
**The rule below decides it mechanically**, so each remaining case is a measurement, not an open
design question:

```
rows_per_partition = estimated total rows / (existing partitions × added dimension cardinality)

  ≥ 20,000  ->  ADOPT Layer 2 (partition by the fetch unit; per-unit skip restored)
  5,000 – 20,000 -> ADOPT only if the fetch cost saved is material (large files / many units);
                    otherwise keep the coarser partition
  < 5,000   ->  DO NOT adopt. Keep the coarser partition and accept Layer 1's
                pipeline-scoped freshness, including its snapshot churn.
```

Thresholds match the sizing already applied to `qwi_employment` (51 partitions × ~56k rows = adopt;
adding `year, quarter` would have produced ~400-row partitions = reject).

**Every case decided — measurements against the live warehouse and publishers, 2026-08-31:**

| Table(s) | Add | Measured | Decision |
|---|---|---|---|
| `economic_census` | `geography`, `naics_var` | ~4 partitions | ✅ **ADOPT** — trivial cardinality |
| `air_quality_daily` | `param` | 5 × years | ✅ **ADOPT** — low cardinality |
| `cde_police_employment` | `state_abbr` | **867 rows total** (~2/partition) | ❌ **REJECT** — far below 5k; its fetch is 51 tiny JSON calls, so all-or-nothing is cheap |
| 8 × `fia_*` / `forest_*` | ~~`state`~~ | all 51 states republished in one 7-hour window | ❌ **WITHDRAWN** — per-state skip buys nothing when the publisher rebuilds everything together |
| 6 × `fda_*` | ~~`year`~~ | constant probe ⇒ units cannot diverge; files re-exported wholesale | ❌ **WITHDRAWN** — not at risk, and per-file skip could never fire |
| `fred_indicators` | `series` (51) | 51 × yrs × 12 | ❌ **REJECT** — explodes |
| `regional_income` | 3 dims (65 × 7 × …) | very large | ❌ **REJECT** — explodes |
| `regional_price_parities` | — | — | ✅ **N/A** — leaves the list when its lookback is removed |

**Layer 2 shrinks from 15 candidate tables to 2 adoptions.** The two withdrawals came from
measuring publisher behavior rather than reasoning about cardinality — partitioning to enable
per-unit skipping is pointless when the publisher never updates units independently.

### Layer 2 (per table) — partition to match the fetch unit

Restores the skip by making the invariant hold.

| Table group | Add to partition | Resulting partitions | Verdict |
|---|---|---|---|
| 8 × `fia_*` / `forest_*` | `state` | 51 | ✅ **required** — Layer 1 alone costs ~23 GB/pass |
| ~~7 × `patent_*`~~ | — | — | **not needed** — single-valued `quarter`, already safe |
| 6 × `fda_*` | `partition_file` | = file count | ⚠️ size first; could be many small partitions |
| `air_quality_daily` | `param` (5) | 5 × years | ✅ likely fine |
| `regional_price_parities` | `line_code` (5) | 5 × years × 2 | ✅ likely fine |
| `fred_indicators` | `series` (51) | 51 × years × months | ❌ **do not** — explodes; leave all-or-nothing |
| `regional_income` | 3 dims (65 × 7 × …) | very large | ❌ **do not** |

**Each Layer 2 change is a partition scheme change** → `data_purge.sh` for that table + full
reprocess, per the standard partition-drift procedure. Batch them with Layer 1 **only for FIA**,
where the cost argument requires it; elsewhere ship Layer 1 first and take Layer 2 per table.

> `qwi_employment`'s redesign already adopts Layer 2 by construction — partition `[type, state]`
> matching its per-state file fetch.

### Rejected

- **All-or-nothing configured per table.** Same effect as Layer 1 but hand-maintained, so it can be
  forgotten on the next table. Layer 1 derives it.
- **A writer-side guard that refuses the commit.** Fails the run rather than degrading, and the
  writer lacks the expected-unit set — the pipeline has it, which is where Layer 1 sits.

---

## Relationship to the redesign

This is **independent of, and more urgent than**, the `lookbackPeriods` / `dataMonthLag` work. That
redesign assumes the write path is sound. If it is not, the assumption is load-bearing beneath all
23 tables being changed — and several of them (`fred_indicators`, `air_quality_daily`) are on this
list.
