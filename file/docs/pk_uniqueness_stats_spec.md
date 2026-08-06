# Requirements Specification: Cached Primary-Key Uniqueness Statistics

**Status:** Draft
**Module(s):** `file/statistics`, `govdata` (ETL stats collector, `GovDataModelVerificationRunner`)
**Author:** (kenstott)
**Date:** 2026-06-13

---

## 1. Background & Problem

`GovDataModelVerificationRunner` verifies, for every govdata base table with a declared
`primaryKey`, that the key is actually unique. It does so by comparing the count of
fully-keyed rows against the count of `DISTINCT` PK tuples
(`GovDataModelVerificationRunner.java:569,588`). This is the check that caught the Iceberg
file-duplication bug (SEC tables duplicated 7–19×).

The check is computed **at verify time** with a full-table scan:

- `SELECT COUNT(*), APPROX_COUNT_DISTINCT(ROW(pk…)) FROM t` (HLL screen), and on a positive
  screen the exact `SELECT COUNT(*) FROM (SELECT DISTINCT pk… FROM t)`.

Against a **local** object store (MinIO) this is tolerable. Against a **WAN** object store
(Cloudflare R2) it is not:

- Observed: `geo.census_tracts` (2.6 GiB / 1,169,345 rows) and `geo.zctas`
  (988 MiB / 236,537 rows) — the two largest tables in `geo` — intermittently fail the
  verify probe over R2, while every smaller table passes and MinIO passes all.
- Root-cause class: a large-scan read over WAN latency being cut short (stall / timeout /
  truncated object read) — same failure class as commit `441bf497b` ("drain object reads so
  a slow consumer can't truncate them"). Full PK-tuple `DISTINCT` aggregation is the slow
  consumer.

A full-table scan to answer a property that is fixed at write time is the anti-pattern to
remove. PK uniqueness of a committed snapshot does not change until the snapshot changes;
it should be computed once and cached, not recomputed on every verify run.

## 2. Goals

- **G1.** Eliminate the routine full-table scan from the verify DUP check by reading a
  cached, per-table PK-uniqueness statistic.
- **G2.** Preserve the check's ability to detect the file-duplication bug it exists for.
- **G3.** Reuse the existing statistics layer (`TableStatistics`, post-ETL collection at
  `9b483074e`, schema-open load, `sourceHash` invalidation) — no parallel mechanism.
- **G4.** Make the expensive path explicit and observable when it is taken, never silent.
- **G5.** Give the query planner a safe estimate on cache-miss without ever scanning.

## 3. Non-Goals

- Changing the semantics of the DUP metric (`keyedRows` vs `distinctKeys`, NULL-key
  exclusion, `--dup-threshold`) — only *where the numbers come from*.
- Per-column NDV changes. Existing per-column HLL sketches stay as they are; they do **not**
  answer composite-PK uniqueness and are out of scope for this metric.
- Any synchronous stat computation on the **live query-planner** metadata path (see R10).

## 4. Current State (verified in source)

- `TableStatistics` (`file/src/main/java/org/apache/calcite/adapter/file/statistics/TableStatistics.java`)
  caches: `rowCount`, `dataSize`, per-column `ColumnStatistics` (incl. HLL sketches),
  `lastUpdated`, `sourceHash`; invalidation via `isValidFor(currentSourceHash)`.
- **No composite-PK-tuple distinctness statistic exists** anywhere
  (`compositeKey`/`pkDistinct`/`tupleSketch` — no matches).
- Post-ETL collection persists `TableStatistics` and it is loaded at schema-open
  (commit `9b483074e`).
- Verify DUP phase computes distinctness by scan every run
  (`GovDataModelVerificationRunner.java:531–590`).

## 5. Requirements

### 5.1 Statistic definition

- **R1.** Add a table-level PK-uniqueness statistic to `TableStatistics`, populated only for
  tables with a declared `primaryKey`. It MUST carry enough to compute the existing DUP
  factor without a scan:
  - `keyedRowCount` — count of rows with **all** PK columns non-null.
  - `pkDistinctEstimate` — estimated count of DISTINCT PK tuples over those keyed rows,
    from a mergeable sketch (Theta sketch keyed on the hashed PK tuple; Theta is chosen for
    mergeability across files/partitions and idempotency under re-merge).
  - (Optional) the serialized sketch itself, to allow incremental merge on append.
- **R2.** The statistic MUST be keyed to a snapshot identity (reuse `sourceHash` /
  snapshot id) and be invalidated when that changes (`isValidFor`).

### 5.2 Population (write side)

- **R3.** The post-ETL stats collector / `StatisticsBackfillRunner` MUST populate the PK
  statistic when it collects the rest of `TableStatistics`.
- **R4.** Duplication-detection correctness. The statistic MUST expose file-duplication
  (identical-PK rows appended 2–19×). Either derivation is acceptable:
  - **(a) Write-stream sketch + committed row count** — build the PK Theta sketch from the
    logical write stream; take `keyedRowCount` from committed manifest metadata. Under a
    double-append: `keyedRowCount` inflates to ~2N, sketch stays at N (Theta re-merge is
    idempotent) → `factor ≈ 2.0×`, detected.
  - **(b) One-time committed-file scan** — scan the committed snapshot once at collection
    time. Duplicated rows share PKs and collapse under DISTINCT: `keyedRows = 2N`,
    `distinctKeys = N` → `factor = 2.0×`, detected.
  - Rationale note: a naive design that computed the "distinct" number by *also* doubling
    with the row count would report `1.0×` and miss the bug — that failure mode MUST NOT
    occur (both (a) and (b) avoid it).

### 5.3 Consumption — verify / DQ / ETL-collection side

- **R5.** The verify runner's DUP phase MUST prefer the cached statistic. When present and
  valid for the current snapshot, it computes the DUP factor from `keyedRowCount` /
  `pkDistinctEstimate` with **no scan and no object-store egress**.
- **R6.** Cache-miss / stale behavior: when the statistic is absent or invalid,
  **the verify runner computes it by full-table scan** (the current path), then the result
  MUST be persisted so subsequent runs are O(1). This is compute-the-true-value-on-miss, not
  a fabricated fallback (consistent with the project's no-silent-fallback rule).
- **R7.** Observability. Output MUST distinguish the source of the DUP metric — e.g. a
  `DUP` DETAIL of `from-cache` vs `scanned` — so the expensive path is never invisible.
  A summary line SHOULD report how many tables required a scan.

### 5.4 Consumption — live query-planner side

The scan-cost concern that first motivated this section is bounded by a separate
data-size cap on planner inputs, so a worst-case miss cannot trigger a runaway WAN scan.
The requirement below is therefore driven by **correctness**, not performance: the planner
must never assume uniqueness it has not verified, at any table size.

- **R10 (correctness, MUST).** On PK-statistic cache-miss, uniqueness / key metadata
  (`RelMdColumnUniqueness.areColumnsUnique`, `RelMdUniqueKeys.getUniqueKeys`) MUST return the
  **not-known-unique** answer (`null`/`false`) — never assume uniqueness it has not verified.
  A planner that assumes a unique key it does not have can drop a required de-dup or pick an
  invalid join strategy; the pessimistic answer is the safe one.
- **R11 (neutral estimate, SHOULD).** On miss, distinct-row / NDV estimates
  (`RelMdDistinctRowCount.getDistinctRowCount`, `RelMdPopulationSize.getPopulationSize`)
  SHOULD fall back to the existing heuristic estimate (the neutral signal). Because planner
  inputs are size-capped, a synchronous compute on miss is not a correctness hazard, but the
  heuristic is preferred to keep planning cheap; the planner is a pure reader of the cache
  and SHOULD NOT be the mechanism that populates it (that is R3 / R6).

### 5.5 Invalidation & consistency

- **R8.** A re-ingest, compaction, or any new snapshot MUST invalidate the cached statistic
  (via `sourceHash`/snapshot id), forcing recomputation on next collection or on next
  verify cache-miss.
- **R9.** The approximate nature of the sketch MUST NOT produce false DUP failures. The HLL/
  Theta screen already gates the exact check today; the cached path MUST keep an equivalent
  guard (e.g. only flag when the estimated factor exceeds `--dup-threshold` beyond the
  sketch's error bound, else mark approximate `~ok`, matching current `~ok`/`ok` semantics).

## 6. Acceptance Criteria

- **AC1.** A `--source geo` verify run against **R2** completes the DUP check for
  `census_tracts` and `zctas` reading cached stats, with **zero** full-table scans and no
  large object-store egress, and reports `DUP` as `from-cache`.
- **AC2.** With stats deliberately cleared, the same run recomputes by scan, reports
  `DUP … scanned`, persists the stat, and a second run reports `from-cache`.
- **AC3.** A synthetically duplicated table (rows appended 2×) is reported at `factor ≈ 2.0×`
  and **fails** the schema at the default `--dup-threshold`, via the cached path — i.e. the
  duplication is caught without a verify-time scan.
- **AC4.** Changing the snapshot invalidates the stat; the next run recomputes rather than
  serving a stale factor.
- **AC5.** No regression to per-column statistics or existing DUP semantics on MinIO.

## 7. Affected Components

- `file/src/main/java/org/apache/calcite/adapter/file/statistics/TableStatistics.java` — new
  PK statistic fields + accessors + serialization.
- `file/statistics` collection path — compute/persist the PK sketch (R3, R4).
- `govdata` `StatisticsBackfillRunner` — populate on backfill.
- `govdata` `GovDataModelVerificationRunner.java` (DUP phase, `:531–590`) — read cache;
  scan + persist on miss; observability (R5–R7).

## 8. Risks

- **Approximation error** producing false positives/negatives near the threshold — mitigated
  by R9 (keep the sketch-error-bound guard; exact recompute only when the estimate is within
  the bound of the threshold).
- **Composite-PK hashing collisions** in the tuple sketch inflating "distinct" — use a
  collision-resistant hash of the normalized tuple; document the negligible-collision
  assumption.
- **Sketch staleness on partial/failed ETL** — R8 ties validity to snapshot identity so a
  partially written snapshot cannot serve a stale-but-valid-looking stat.
