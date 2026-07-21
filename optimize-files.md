# GovData Parquet File-Size Optimization Plan

Derived from a full audit of the live prod R2 lake (`govdata-parquet-v1`):
**189,144 data parquet files across 324 tables** (`~/r2_fileaudit.json`).

## Root cause

Iceberg compaction (`IcebergMaterializationWriter.commit()` → `compactSmallFiles`,
target 128 MB) only ever merges files **within a single partition** and can only
build a target-size file if that partition already holds ≥128 MB of data. For
~300 of 324 tables **no single partition reaches 128 MB** (`compactable_parts = 0`),
so compaction is structurally powerless — the **partition grain**, not compaction
tuning, is the binding constraint on file size. The lake is small-file dominated
(~185k of 189k files are <10 MB; average file <1 MB for most tables).

## The decision rule

File-size optimization and partition pruning only conflict when a partition holds
**less than one target file (~128 MB)**. Below that floor a partition boundary
costs fragmentation and buys nothing that Parquet row-group min/max stats inside a
single compacted file wouldn't already give.

> **Partition on a column only if each resulting partition will hold ≥ ~128 MB.
> Otherwise don't.**

## Three fix types

| Fix | Meaning | Tables | Files |
|-----|---------|--------|-------|
| **UNPARTITION** | Table total < 128 MB — collapse to 1 file; any partitioning is pure overhead | 257 | 33,190 |
| **REPARTITION** | Table ≥ 128 MB but sharded so no partition reaches 128 MB — coarsen grain (usually to `(type, year)`) | 24 | 119,358 |
| **COMPACT** | Grain already correct (a partition exceeds 128 MB) but small files remain — enable/run compaction only, no rewrite | 43 | 36,596 |

## Priority-ranked table plan

Priority = files eliminated. `After` counts are estimates (`ceil(size/128)`,
floored at #years for year-grain).

| # | Table | Files now → ~after | Fix | Why |
|---|-------|-----|-----|-----|
| 1 | `weather.ghcnd_daily` | 49,229 → ~20 | REPARTITION `(type,year,state,month)`→`(type,year)` | 26% of the whole lake; `month`+`state_fips` shard 442 MB into 0.11 MB leaves. Biggest single win. |
| 2 | `environment.water_quality_samples` | 11,181 → ~25 | REPARTITION → `(type,year)` | 2.4 GB but median leaf 1.9 MB across 650 parts; none near 128 MB. |
| 3 | `health.who_gho_indicators` | 9,929 → 1 | UNPARTITION | 100 MB total across 426 parts = whole table fits in <1 file. |
| 4 | `ag.faostat_production` | 7,799 → ~2 | REPARTITION/coarsen | 213 MB in 143 parts; 196 files in one partition also shows compaction isn't running. |
| 5 | `environment.drinking_water_violations` | 7,369 → ~7 | REPARTITION → `(type,year)` **+ run compaction** | 52 state leaves ≤110 MB; 1,012 files in one partition = compaction dead. |
| 6 | `environment.streamflow` | 6,734 → ~6 | REPARTITION → `(type,year)` | 297 MB across 832 parts, median leaf 0.29 MB. |
| 7 | `weather.cdo_stations` | 6,666 → ~2 | UNPARTITION (dimension) | Station list, 224 MB; `state_fips` grain buys no pruning worth 132 files/state. |
| 8 | `weather.nws_alerts` | 6,631 → ~3 | REPARTITION → `(type,year)` | Event data, 51 state leaves @4.6 MB; year grain reaches file-size floor. |
| 9 | `weather.nws_stations` | 7,038 → 1 | UNPARTITION (dimension) | 130 MB; 138 files in one state partition — grain pointless for a dimension. |
| 10 | `cyber_threat.ioc_urls` | 4,928 → ~2 | REPARTITION/coarsen | 848 parts, median 0.03 MB; one 163 MB outlier partition. |
| 11 | `transport.airline_ontime` | 4,387 → ~10 | REPARTITION → `(type,year)` | 904 MB in 203 parts @1.6 MB; drop `month`/carrier. |
| 12 | `health.cms_open_payments` | 4,059 → ~8 | COMPACT | 15 year-parts, some >128 MB; grain fine, 1,472 files/part = not compacting. |
| 13 | `environment.rcra_facilities` | 3,281 → ~14 | COMPACT (+coarsen small states) | 1.76 GB, max part 852 MB, `compactable=2`; mostly a compaction miss. |
| 14 | `ag.ers_farm_income` | 3,055 → ~90 | COMPACT | 11.4 GB, median part 108 MB, `compactable=25`. Grain right. |
| 15 | `sec.vectorized_chunks` | 3,028 → ~114 | COMPACT | 14.6 GB, median part 2 GB, `compactable=6`. |
| 16 | `patents.patent_cpc_classes` | 2,991 → ~4 | ENABLE compaction (patents schema) | Single 496 MB partition split 2,991 ways — patents never compacts. |
| 17 | `cftc.cftc_trades` | 2,892 → ~71 | COMPACT | 9 GB, `compactable=28`. |
| 18 | `geo.places` | 2,764 → ~24 | REPARTITION → `(type,year)` | 3 GB across 802 parts @2.7 MB (tradeoff: loses per-state pruning; row-group stats cover it). |
| 19 | `census.qwi_employment` | 1,824 → 1 | UNPARTITION | 7.4 MB in 1,824 files (~4 KB each) — most egregious ratio in the lake. |
| 20 | `energy.eia_electricity_generation` | 1,782 → ~5 | ENABLE compaction (energy schema) | 7 year-parts, max 207 MB; energy largely not compaction-enabled. |
| 21 | `econ.trade_by_state` | 1,606 → ~5 | REPARTITION/compact | 545 MB, 36 parts @14.6 MB; 286 files/part. |
| 22 | `environment.air_quality_daily` | 1,546 → ~2 | UNPARTITION/coarsen | Only 152 MB total; year grain still can't fill a file. |
| 23 | `econ.comtrade_flows` | 1,278 → ~5 | REPARTITION → `(type,year)` | 614 MB across 213 parts @2.9 MB. |
| 24 | `environment.drinking_water` | 1,290 → 1 | UNPARTITION | 72 MB across 52 state parts. |
| 25 | `fec.individual_contributions` | 1,222 → ~28 | COMPACT | 3.5 GB in 2 partitions @1.7 GB → 611 files each; grain coarse, compaction not running. |
| 26 | `environment.water_sites` | 1,214 → 1 | UNPARTITION | 17 MB, 52 parts. |
| 27 | `patents.patent_inventors` | 1,202 → ~5 | ENABLE compaction (patents) | 582 MB single partition, 1,202 files. |
| 28 | `health.medicaid_drug_utilization` | 1,068 → ~3 | COMPACT | 354 MB single partition. |
| 29 | `sec.earnings_transcripts` / `sec.filing_metadata` | 830 / 587 → ~6 each | COMPACT | 6 year-parts (grain correct) but 286 / 187 files per year — compaction not keeping up. |
| 30 | `transport.safety_complaints` | 591 → ~140 | COMPACT | 18 GB in 1 partition, 591 files @30 MB. |
| — | **Long-tail bulk** (rest of the 257 unpartition tables: all `census.acs_*`, `econ.*` micro-series, `housing.hmda_*`, `geo.*_crosswalk`, `ref.*`, `research.*`, `cyber_threat` framework tables, `weather.climate_normals_monthly`, etc.) | ~24,000 → ~250 | UNPARTITION en masse | Each is <128 MB total; e.g. `econ.fred_indicators` = 0.7 MB in 331 files, `housing.hmda_loans` = 3.2 MB in 416 files. One sweeping grain change. |

**Net effect if executed top-to-bottom:** ~189k files → low thousands. Rows 1–2
and the long-tail bucket alone remove ~85k files.

## Caveats before any rewrite

1. The exact *current* partition columns must be confirmed per-YAML for each
   REPARTITION row. Targets above were derived from audit metrics plus the weather
   YAML; the other 24 schema files have not all been opened.
2. REPARTITION tables also need compaction re-run *after* the rewrite, and
   `overwritePartitions: true` means old-grain data must be fully re-ingested, not
   appended.
3. The audit counts *physical* R2 objects, so file counts include superseded files
   inside the 7-day post-compaction retention window and any leaked orphans; the
   finding about grain is unaffected but live-snapshot file counts are somewhat lower.

## Reproduce the audit

`~/r2_fileaudit.py` (boto3, prod R2 creds from `govdata/.env.prod`) →
per-table `files / total_mb / parts / med_part_mb / max_part_mb / small_files /
max_files_1part / compactable_parts`. Output cached in `~/r2_fileaudit.json`.

---

# Implementation Log

## Two findings that reframed the original plan

1. **Snapshot debt, not grain, was the dominant cause.** Most "REPARTITION" rows
   had inflated *physical* file counts because Iceberg snapshots were never expired
   (`runMaintenance` off while `runCompaction` on + 1 file/partition → compaction
   no-ops, nothing expires). Live file counts were a fraction of physical. Fixed
   systemically: `runMaintenance: true` + `snapshotRetentionDays: 1` on **all 332
   iceberg tables** (jar rebuilt). Each table's backlog clears on its next
   write-committing ingest. See memory `snapshot-bloat-runmaintenance`.

2. **Coarsening is only safe when the dropped column is NOT a crawl dimension.**
   A partition column emitted per-row from a *bulk* fetch (one fetch spans all its
   values) can be dropped without clobber. A column that IS a fetch dimension
   (per-state, per-(year,month)) already equals the fetch unit — dropping it makes
   per-fetch writes replace-partition each other → **silent data loss**.

## Decision rule (applied per table)

Drop a sub-partition column **only if ALL hold**:
- (a) it is not in the crawl `dimensions` (it's bulk-emitted per row), AND
- (b) the resulting partition stays ≈≤128 MB (too-large is also bad — one giant
  file kills parallelism/pruning, and compaction only *merges* small files, never
  *splits* a big one), AND
- (c) it isn't also serving OOM row-batching for a huge-row source.

When dropping a col, also override the inherited `batchPartitionColumns` /
`incrementalKeys` to the new fetch/partition unit (year-keyed batching under a
year-less partition would replace-partition per year → clobber).

## Compaction pass — large fragmented partitions (merge small → ~128 MB)

The "too-large" tables from the audit are actually **large partitions split into many
small files** (e.g. `safety_complaints` = 18 GB in 591×30 MB files; `patent_cpc_classes`
= 496 MB in 2,991×0.17 MB files). Their partitions exceed 128 MB, so compaction *can*
build full-size files — unlike the tiny tables where it can't. `runCompaction` merges
small files within a partition up toward the target regardless of whether the partition
exceeds 128 MB, so it also cuts file count on medium fragmented tables.

Audit of `runCompaction` across all 332 iceberg tables found 48 without it. Fixes:
- **Flipped `runCompaction: false`→`true` in edu (11), health (15), patents (1 anchor →
  covers all 7 patents tables)** — real-data tables wrongly disabled, incl. the big
  single-partition offenders (`patent_cpc_classes`/`inventors`/`abstracts`/`assignees`,
  `health.cms_open_payments` 941 MB, `medicaid_drug_utilization` 354 MB).
- **`fec.individual_contributions`: `compactionMinFiles` 2000 → 5** — 2000 mathematically
  disabled compaction (partition has ~611 files « 2000); the 1.7 GB/partition table never
  compacted its ~611 small files. Now merges to ~14×128 MB.
- **Left the 18 `runCompaction: None` static reference dims** (econ-reference/geo/ref) —
  already 1 file each; compaction would be a pointless no-op.

No purge/re-ingest needed for compaction — it merges existing files on the next ETL run.

## Per-table decisions

| Table | Decision | Grain change | Rationale |
|-------|----------|--------------|-----------|
| `weather.ghcnd_daily` | **REPARTITIONED** ✓ | `(type,year,state,month)`→`(type,year)` | Per-*year* bulk file carries all states/months; ~25 MB/yr. Lake+R2 purged. |
| `health.who_gho_indicators` | **REPARTITIONED** ✓ | `(type,indicator,year)`→`(type,indicator)` | Per-*indicator* bulk (all years); ~2.5 MB/indicator. `batchPartitionColumns→[indicator]`. Lake+R2 purged. |
| `ag.faostat_production` | **REPARTITIONED** ✓ | `(type,domain,year)`→`(type,domain)` | Per-*domain* bulk zip (all years). Overrode anchor `batchPartitionColumns`/`incrementalKeys`→`[domain]`. Lake+R2 purged. |
| `econ.ilostat_indicators` | **REPARTITIONED** ✓ | `(type,indicator,year)`→`(type,indicator)` | Per-*indicator* bulk; 4.3 MB total. `batchPartitionColumns→[indicator]`. Lake+R2 purged. |
| `research.nsf_national_rd` | **REPARTITIONED** ✓ | `(type,year)`→`(type)` | Single xlsx (all ~70 yrs); 2.5 MB → 1 file. Inlined `[type]` partition, `batchPartitionColumns: []`. Lake+R2 purged. |
| `cyber-threat.ioc_urls`/`ioc_hashes`/`ioc_ips`/`ioc_mixed`/`threat_pulses` | **NOT repartitioned** | — | `first_seen` is a **snapshot-date template var** (`${first_seen}`, `dataset_type: snapshot`), not a row value — partitions are retained daily dumps. Dropping it collapses history to latest-only (data loss). If accumulation is unwanted → a TTL-pruning decision, not a grain change. Same for `ref.sec_company_tickers` (`as_of`). |
| `environment.water_quality_samples` | **NOT repartitioned** | — | Per-`(state,year)` fetch = grain; dropping state would clobber. 11k files were snapshot debt → fixed by systemic `runMaintenance`. |
| `environment.drinking_water_violations` | **NOT repartitioned** | — | Grain already `(type,state)` = per-state fetch unit. 7.4k files = snapshot debt (1012/partition) → fixed systemically. |
| `transport.airline_ontime` | **NOT repartitioned** | — | Grain `(type,year,month)` = per-`(year,month)` fetch unit. Snapshot debt → fixed systemically. |
| `energy.eia_state_energy_consumption` | **SKIPPED** | — | `year` also drives OOM row-batching (millions of rows); marginal gain; snapshot fix already ~1714→~65 live. |
| `ag.ers_farm_income` | **KEEP year** | — | Year partitions already ~108 MB (well-sized). Class C — needs *compaction* (split), not coarsening. |
| ALL 332 iceberg tables | **runMaintenance enabled** ✓ | n/a | Systemic snapshot-expiry fix (205 added, 26 flipped from `false` in edu/health, 1 manual). |

## Pending

- **Consolidated jar rebuild** before any re-ingest (bundles all grain changes; last
  rebuild predates `ilostat`). ETL is currently stopped.
- Re-ingest of the 4 repartitioned tables (regenerates data at new grain).
- ~~**Too-large tables**~~ — **ADDRESSED via compaction** (see below). These weren't
  single giant files but large partitions fragmented into many small files; the fix is
  `runCompaction` (merges small files up toward 128 MB), not repartitioning.
- **High-value clean coarsening is now exhausted** (ghcnd, who_gho, faostat, ilostat,
  nsf_national_rd done). Remaining `droppable` scan hits are either:
  - *tiny/marginal* — already ~1 live file after the snapshot fix: `econ.state_gdp`,
    `state_personal_income`, `state_quarterly_*`, `state_consumption`,
    `energy.eia_natural_gas_storage`, `econ.regional_employment` (batchable if desired,
    low payoff);
  - *keep* — well-sized: `ag.ers_farm_income` (108 MB/yr); `sec.stock_prices`;
  - *snapshot-date partitions* — not droppable: cyber-threat `ioc_*`, `ref.sec_company_tickers`;
  - *complex / verify-first* — `econ.world_indicators` & `econ-reference.nipa_tables`
    (partition omits the fetch dimension — possible existing clobber/bug), `cyber-vuln.*`
    (deferred; NVD resolver upper-bound).
