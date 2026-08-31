# Table Ingestion Audit & Redesign — master plan

**Date:** 2026-08-31
**Status:** Design complete pending live-data testing. **Nothing implemented; no schema or code changed.**

Live testing is expected to revise this plan — it is a design, not a finding.

---

## Documents

| Doc | Contents |
|---|---|
| [../../schema-review-plan.md](../../schema-review-plan.md) | The original audit of all 406 tables + the 36-table revision research |
| [DEFECT-replace-partitions-clobbering.md](DEFECT-replace-partitions-clobbering.md) | ⚠️ Live data-loss defect + the two-layer fix |
| [data-month-lag-and-daily-lookback.md](data-month-lag-and-daily-lookback.md) | The two requirements, and why |
| [engine-implementation-spec.md](engine-implementation-spec.md) | P0 / P1 / P2 at file and method level |
| [table-config-changes.md](table-config-changes.md) | Per-table YAML, with line numbers and comments |
| [config-redesign-and-validation-plan.md](config-redesign-and-validation-plan.md) | Test tiers, waves, DQ, rollback |

Draft ledger entries (FILE-190..197, GOV-75..79) are staged but **not appended** — see
*Requirements ledger* below.


---

## Execution sequence

Ordered so that each step is independently verifiable, and the irreversible work comes last.

### Wave −1 · Fix the clobbering defect ⚠️ do this first
Layer 1: make the freshness unit the **partition key** — group fetch units by partition, skip a
group only when every unit in it is unchanged. Safe by construction; degrades to all-or-nothing
when the partition is `[type]`, and is identical to today when partition == fetch unit.
**Reversible. No schema change, no purge, no reprocess.**
**Blocks everything** — the rest of the plan validates against this write path.

**Layer 2 is needed for only 2 tables** (`economic_census`, `air_quality_daily`). The FIA and FDA
partition changes were **withdrawn** after measuring publisher behavior: both republish every unit
together, so per-unit partitioning would buy nothing.

### Wave 0 · Engine prerequisites
P0 (`pipelineDate` injection, incl. the consistency check) → then P1 (period walk + range
extension) and P2 (`dataMonthLag`) in parallel.
**Reversible.** P0 first: it converts most later validation into offline unit tests.

### Wave 1 · Cheapest table changes
`jolts_*` (2 runs each), `iip_positions`, `nsf_herd_by_institution`. Proves P1 end-to-end before
anything expensive runs.

### Wave 2 · Lookback resizing
The BEA/FRED family. Same change shape, one source family — validating one validates the pattern.

### Wave 3 · New freshness gates
The 5 tables with no gate today. Highest config risk: each needs a cold-start assertion as well as
its lookback test.

### Wave 4 · `dataMonthLag` migration (5 EIA tables)
Coverage change. Per-year row counts before/after, DQ bucket only.

### Wave 5 · Irreversible, one table at a time
`qwi_employment` bulk switch; Layer 2 partition changes. Each is purge + full reprocess.

> **Cutover coupling:** the retired-key migration to `lookbackPeriods` is a hard cutover (old
> key fails loudly at load), so the engine and all 37 schema files land together. Phase 2 research
> is a prerequisite of that cutover, not a follow-up.

---

## Completeness — three distinct states, do not conflate

### 1. Design decisions — **COMPLETE**

Every judgement call is made. No open questions require further design work, and nothing below
sends you back to a design discussion. Where a value depends on data, the *rule* that converts data
to a value is stated.

### 2. Configuration determined — **COMPLETE**

Measurements taken against the live warehouse (2026-08-31). The final YAML is writable for every
table.

| Table(s) | Measured | Decision |
|---|---|---|
| `cde_police_employment` | **867 rows total** (~2/partition) | reject `state_abbr`; all-or-nothing is cheap here |
| 6 × `fda_*` | 443,392 rows; **1,767 files / 113 GB** source | partition by **`year`** (~22k rows/partition), not `partition_file` |

**The measurement changed the design**, which is why it could not be deferred: `partition_file` is
openFDA's own chunking and explicitly unstable, so it cannot be a partition key — yet all-or-nothing
there means re-downloading 113 GB per run. That forced Layer 1 from *"disable per-unit skip"* to
*"group fetch units by partition key"*, and requires `OpenFdaPartitionResolver` to emit the year it
already parses.

### 3. Assumptions carrying falsification risk — **UNVALIDATED**

This is the category that can genuinely send work back to design. Each is an assumption the design
rests on that live testing could disprove:

| Assumption | If false | Impact |
|---|---|---|
| ~~The clobbering defect fires in production~~ | **MEASURED 2026-08-31** — 7 of 8 FIA tables hold all 51 states; `fia_invasives` is short only OR/WA/PR, a coherent protocol gap. **No evidence it has fired.** Fix still correct; urgency lower | resolved |
| **Per-source freshness tokens discriminate** (Tier 1b probes not yet run) | A source whose ETag restamps spuriously makes per-unit skip useless there | **High** — could invalidate the freshness *type* chosen for a table |
| Researched revision depths match observed behavior | A value is wrong in the direction that serves stale data | **High** — the whole point of the exercise |
| ~~`eia_coal_mines`~~ | **CLOSED** — measured by vintage diff of the MinIO raw cache (2026-06-12 vs 2026-08-28): restatement reaches report_year Y−1 and stops. Value is **2**, not 3 | resolved |
| QWI bulk files match the API in coverage | The source switch changes what the table holds | High — unverifiable without `CENSUS_API_KEY` |
| QWI per-state coverage (7 of 51 sampled) | Coverage expectations are wrong for unsampled states | Medium |
| EIA `dataMonthLag` recovers ~2 years cleanly | Row counts differ from expectation | Medium — caught by the before/after check |

**Verdict: design COMPLETE, configuration COMPLETE. Two of seven assumptions closed by
measurement; five remain.** The two closed both changed an answer rather than confirming it —
`eia_coal_mines` moved 3 → 2, and the clobbering defect proved never to have fired.

**Remaining, in priority order:**

1. **Per-source freshness signal quality** — Tier 1b probes not run for FIA / FDA / air-quality.
   The highest-risk open item: a source whose token restamps spuriously invalidates the freshness
   *type* chosen for it.
2. **Researched revision depths vs observed behavior** — 13 values rest on published policy. The
   `eia_coal_mines` vintage diff shows this is checkable where raw-cache vintages exist.
3. QWI bulk-vs-API coverage parity (needs `CENSUS_API_KEY`).
4. QWI per-state coverage (7 of 51 sampled).
5. EIA `dataMonthLag` recovers ~2 years cleanly (caught by the before/after row check).

---

## Definition of done for the design phase

- [x] Audit of all 406 tables
- [x] Revision research: 36 tables + CPI family (5)
- [x] Revision research: BEA family (5) — assumption wrong on 4 of 5
- [x] Revision research: BLS-other (11) — **8 of 11 wrong, all too narrow**
- [x] Clobbering defect identified, scoped (12 tables), and fixed in design
- [x] P0 / P1 / P2 specified at file and method level
- [x] Period-walk mechanics specified
- [x] Per-table YAML with comments and line numbers
- [x] QWI source decision + column spec + typing rules
- [x] LODES probe decision (verified)
- [x] Test tiers, revision-simulation tests, signal-quality probes
- [x] DQ expectations, rollback plan, operand wiring
- [x] Live measurements taken where design depended on them (partition sizing, source volumes)
- [ ] Requirements-ledger entries appended (drafted, awaiting approval)
- [ ] **Live-data validation — not started; expected to revise this plan**

---

## Requirements ledger

This repo tracks adapter guarantees formally in `docs/requirements/*.yaml`
(`FILE-NNN` / `GOV-NNN`, generated into `REQUIREMENTS.md`). This work should land there rather than
living only as plan documents.

Drafted, **not appended**: `FILE-190..197` (partition safety, time injection, publication lag,
revision lookback) and `GOV-75..79` (econ lookback sizing, EIA month lag, bulk-source selection,
whole-series freshness, per-unit coverage). All `status: proposed` with empty `tests:` — per the
ledger's lifecycle they become `accepted` on approval and `complete` only once a green golden test
is linked.

---

## Known-open, deliberately not resolved here

| Item | Why deferred |
|---|---|
| ~~`state_personal_income` coverage~~ | **CONFIRMED DEFECT** — measured 2026-08-31: 1,417,634 rows, **1929–1957 only** (29 distinct years) against BEA's 1929–present. ~67 years absent |
| ~~`eia_coal_mines` partition skew~~ | **CONFIRMED DEFECT** — measured 2026-08-31: every partition is off by one (`year=2024` holds `report_year=2025`). Static source URL vs `valueSource: year: effective_year`. `WHERE year = N` returns N+1 data |
| `year_range_from_2020` anchor actually starts 2010 | Cosmetic |
| QWI: other slices (county, metro, NAICS 3–6, `oslp`, `rh`) | Additional *tables*, not columns — separate scope decision |
| QWI/API coverage parity | Needs `CENSUS_API_KEY`, unavailable this session |
| Full per-state QWI coverage matrix | 7 of 51 sampled; a full sweep is ~51 file reads |
| Layer 2 partition changes for `fda_*`, `air_quality_daily` | Need partition sizing before committing |
