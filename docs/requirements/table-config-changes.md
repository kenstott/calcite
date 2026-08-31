# Table Config Changes — per-table specification

**Date:** 2026-08-31
**Status:** Spec — no edits applied.
**Blocked on:** [engine-implementation-spec.md](engine-implementation-spec.md) (P0/P1/P2)
**Research:** [schema-review-plan.md](../../schema-review-plan.md)

> **Do not apply before P1 lands.** In daily mode the run generates a single year, so every window
> change here is unobservable until the year-range extension exists. The ETL will succeed and the
> YAML will read correctly while the change does nothing.

Line numbers are as of 2026-08-30; re-locate by `- name: <table>` before editing.

Every comment must name the upstream policy **and** carry the source URL. Do not paste a generic
justification — the audit found a repeated 10-site comment in `energy-schema.yaml` that is half
wrong precisely because it was generic.

---

## Config shape change — read first

The setting is **`lookbackPeriods`** — table-level, counted in published **periods**
(grain-agnostic: year / quarter / month / week), not years.

```yaml
lookbackPeriods: 6        # table-level
freshness:
  type: hash
```


### Value translation — only 4 tables change number

A table whose only period slot is `year` keeps its number unchanged. Only tables with a `month`
slot rescale (×12):

| Table | Period slots | Was (years) | **Now (periods)** |
|---|---|---|---|
| `fred_indicators` | year, month | 6 | **72** |
| `eia_fossil_fuel_production` | year, month | 3 | **36** |
| `eia_electricity_generation` | year, month | 2 | **24** |
| `eia_refinery_operations` | year, month | 2 | **24** |
| *all 17 others* | year | *n* | *n* (unchanged) |

**Every rescaled value must carry the translation in its comment** — a bare `lookbackPeriods: 72`
is unreviewable. Write `# 72 periods = 6 years of monthly data` alongside the upstream policy.

Values shown in the per-table sections below are already in **periods**: unchanged from the
year-count for year-only tables, and rescaled for the four monthly ones.

---

## Group 1 — Widen the lookback (7)

All are `type: hash` already; only the integer changes, and it changes location.

| Table | File | table @ | Change |
|---|---|---|---|
| `gdp_statistics` | econ | 5785 | `2` → `6` |
| `industry_gdp` | econ | 5900 | `2` → `6` |
| `ita_data` | econ | 5199 | `2` → `6` |
| `iip_positions` | econ | 5647 | `2` → `6` |
| `fdi_direct_investment` | econ | 5414 | `2` → `6` |
| `fred_indicators` | econ | 3641 | `2` → `6` |
| `nsf_herd_by_institution` | research | 468 | `2` → `3` |

**`gdp_statistics` / `industry_gdp`** — revised in the same NEA annual update, same open period:

```yaml
year:
  type: yearRange
  dataLag: 1
  # BEA's annual update each September reopens ~5 years (2026 update: 2021:Q1-2026:Q1); 6 covers
  # that with a year of margin. Comprehensive updates (~every 5 yrs) restate the full history back
  # to 1929 and need a MANUAL FULL REPROCESS -- no lookback can catch them.
  # https://www.bea.gov/information-updates-national-regional-economic-accounts
  lookbackPeriods: 6
```

**`ita_data` / `iip_positions`** — same June release:

```yaml
  # BEA's June annual update reopens ITA/IIP for "at least the prior 3 years", in practice 5
  # (2026 update: 2021:Q1-2026:Q1). Methodology changes reaching back to 1999 need a manual full
  # reprocess. https://www.bea.gov/news/us-international-transactions-and-investment-position-release-additional-information
  lookbackPeriods: 6
```

**`fdi_direct_investment`** — the strongest case; a window of 2 demonstrably caught half:

```yaml
  # BEA restates direct investment back to the benchmark-survey year every ~5 years: the 2026
  # update revised 2022-2025 off the 2022 BE-12 benchmark, of which a 2-year lookback caught only
  # 2 of 4 years. 6 covers a full benchmark cycle. https://www.bea.gov/surveys/be12
  lookbackPeriods: 6
```

**`fred_indicators`** — different reason (BLS seasonal factors, not BEA):

```yaml
  # 72 periods = 6 YEARS of monthly data (this table has year+month period slots).
  # BLS recomputes CPI seasonal factors every February, revising the previous 5 years of
  # seasonally adjusted data; only data older than 5 years is final. The curated series here
  # (CPIAUCSL, PCEPI, UNRATE, PAYEMS, ...) are all seasonally adjusted. 6 years = current + 5.
  # https://www.bls.gov/cpi/technical-notes/
  lookbackPeriods: 72
```

**`nsf_herd_by_institution`** — 3, not 6:

```yaml
  # NCSES solicits corrections against the current + 2 prior HERD years and restates the microdata
  # when institutions amend, so reopen 3 publish years (data years N-2..N-4 under dataLag: 2).
  # NCSES serves no Last-Modified on the HERD zip, so hash is the only workable detector.
  # https://ncses.nsf.gov/surveys/higher-education-research-development/2023
  lookbackPeriods: 3
```

**Test (R1/R6):** at a pinned date, a revision in `currentYear − 4` is caught — **fails at 2,
passes at 6**. `nsf_herd` uses `currentYear − 2`.

---

## Group 2 — Narrow the lookback (1)

**`eia_capacity_changes`** — energy @ 839. Lowest priority; `2` is harmless, this is tidiness.

```yaml
  # The archived december_generator{year}.xlsx is an immutable point-in-time snapshot -- EIA-860M
  # corrections land in SUBSEQUENT months' files, a different URL, never by editing this one.
  # The lookback exists here only so the ~Feb publication of the prior December file isn't locked
  # out by the period-completion gate. https://www.eia.gov/electricity/data/eia860m/
  lookbackPeriods: 1
```

**Test:** `currentYear − 1` is **no longer** reopened; the current year still is.

---

## Group 3 — Add a lookback, keep `etag` (2)

Both pull the **same** QCEW bulk file, differing only in `rowFilter`. Keep `type: etag` — BLS
serves a strong content ETag, so an unchanged year costs one HEAD instead of a ~75 MB download.

| Table | File | table @ | Change |
|---|---|---|---|
| `county_qcew` | econ | 2168 | add `lookbackPeriods: 2` |
| `state_wages` | econ | 1772 | add `lookbackPeriods: 2` |

```yaml
year:
  type: yearRange
  dataLag: 1
  releaseMonth: 9
  # QCEW data year Y is preliminary until finalized with the following year's Q1 release
  # (~Sept Y+1); the annual singlefile is rewritten in between (2025's file was 62.9MB in May 2026
  # vs ~75MB final). BLS then never edits it again. 2 reopens publish years P and P-1 (data years
  # P-1, P-2) so the preliminary-to-final rewrite is picked up. https://www.bls.gov/cew/revisions/
  lookbackPeriods: 2

freshness:
  type: etag          # KEEP -- do not switch to hash; these files are ~75MB/year and the ETag
                      # probe makes an unchanged year cost one HEAD instead of a full download
```

**Test:** prior year reopened; unchanged year skipped **with a HEAD and no body download**.
Assert on request method — a "no snapshot written" assertion alone would let a 75 MB GET pass
silently, every day.

---

## Group 4 — Add a freshness gate **and** a lookback (5)

### ⚠️ Adding the gate exposes two of these to the clobbering defect

These tables are absent from the [defect](DEFECT-replace-partitions-clobbering.md) list only
because they have **no freshness gate today**. Adding one changes that. Checked against the
`perUnitSkipSafe` predicate as they would be *after* the change:

| Table | Partition | Multi-valued dims not in it | After adding the gate |
|---|---|---|---|
| `cde_police_employment` | `[type, year]` | **`state_abbr` (51)** | ⚠️ **newly at risk** |
| `economic_census` | `[type, year]` | **`geography` (2), `naics_var`** | ⚠️ **newly at risk** |
| `nps_visitation` | `[type, year]` | — | ✅ safe |
| `communication_costs` | `[type, year]` | — | ✅ safe |
| `lodes_workplace` | `[type, year, state]` | — | ✅ safe — already partitions by `state` |

**Layer 1 protects them** (it would detect the shape and fall back to pipeline-scoped freshness),
so there is no data-loss risk. But the *cost model breaks*: pipeline-scoped `hash` on a multi-unit
fetch churns — it writes a snapshot on every run rather than skipping when unchanged, which is
precisely the property the lookback's cost argument depends on.

**Fix: add the missing dimension to the partition in the same change.**

- `economic_census` → `[type, year, geography, naics_var]`. **Decided: adopt.** ~4 partitions;
  trivially clears the sizing rule.
- `cde_police_employment` → `[type, year, state_abbr]`. **Decided by rule, pending one
  measurement:** 51 × ~10 years ≈ 510 partitions. Apply the
  [partition-sizing rule](DEFECT-replace-partitions-clobbering.md) — adopt at ≥20k rows/partition,
  reject below 5k and accept snapshot churn on 3 reopened periods per run. One query settles it:
  `SELECT COUNT(*)/51 FROM cde_police_employment`.

> **`lodes_workplace` is the model here** — it already partitions by `state`, matching its
> per-(state, year) fetch, so adding `last_modified` is safe with no partition change. That is the
> same alignment `qwi_employment` adopts.

**Highest config risk: none of these has any freshness gate today.** The change alters both whether
they re-fetch and whether they skip writes. Each needs a **cold-start assertion** (a first run must
write data) alongside its lookback test — a misconfigured type could wedge the table shut.

### `nps_visitation` — lands @ 1073

```yaml
year:
  # NPS visitation is preliminary until the calendar year is finalized in Q1 of the following
  # year, so the current AND prior year can both still change; 2 reopens exactly that.
  # https://www.nps.gov/subjects/socialscience/statistics-faq.htm
  lookbackPeriods: 2
freshness:
  type: hash
```

### `cde_police_employment` — crime @ 418

```yaml
year:
  # FBI CDE serves the LATEST master file, rebuilt on every monthly release and silently absorbing
  # late and amended agency submissions for prior years -- the 2023 release restated both 2021 and
  # 2022 (2022 violent crime went from -2.1% to +4.5%). No closing date is published, so reopen the
  # current and two prior years; the fetch is 51 small per-state JSON calls, so the extra year is
  # nearly free. https://ucr.fbi.gov/data-quality-guidelines-new
  lookbackPeriods: 3
freshness:
  type: hash
```

### `communication_costs` — fec @ 1659 · **N=4 is deliberate**

```yaml
year:
  # 2 = the two most recent PUBLISHED election cycles. The walk counts published periods, so the
  # even-year cadence needs no inflated value (a calendar-year window would have needed 4).
  lookbackPeriods: 2
freshness:
  type: hash          # NOT last_modified: the FEC regenerates every cycle file DAILY, so
                      # Last-Modified always moves (verified 2026-08-30: every cycle file back to
                      # 2016 carried a same-day timestamp) and would re-ingest every cycle daily.
                      # FEC has no amendment deadline, so a closed cycle can still change.
                      # https://www.fec.gov/help-candidates-and-committees/filing-reports/electronic-filing/
```

> **Validate in an odd simulated year.** Under the period walk the even-year cadence is invisible —
> N=2 finds the two most recent *published* cycles regardless of what the calendar says. A
> calendar-anchored window needed 4 here; if 2 reopens nothing in an odd year, the walk is still
> counting calendar years somewhere.

### `lodes_workplace` — census @ 4644 · **revised: probe `version.txt`, do not hash the CSVs**

Live probe (2026-08-31) settled this. `https://lehd.ces.census.gov/data/lodes/LODES8/{state}/version.txt`
is per-state, 123 bytes:

```
LEHD Origin-Destination Employment Statistics (LODES)
AR (Arkansas)
Data Vintage: 20251202_1657
Release Format Version 8.4
```

| Candidate | Verdict |
|---|---|
| `type: checksum` | **Broken.** `firstTokenOf` returns `"LEHD"` — identical for every state and vintage |
| `type: version` | **Broken.** `jsonValue` requires JSON; this is plain text |
| `type: hash` over the CSVs | Works but costs 51 states × 4 years of gzipped CSV **daily** once P1 lands |
| `type: last_modified` + `probe_url` → `version.txt` | ⚠️ works, but **coarser than the fetch unit** — see the correction below |
| **`type: last_modified` on the data file itself** | ✅ **chosen** — per (state, year), no `probe_url` needed |

The file serves `Last-Modified: Wed, 03 Dec 2025` and **no ETag** — stable for ~9 months, so unlike
FEC (daily restamp) or BLS `/jt/` (identical ETags) this is a genuinely discriminating signal.

```yaml
year:
  type: yearRange
  dataLag: 3
  # LODES reissues corrected per-state-year files under a new data vintage (LODESTechDoc 8.3:
  # "New or corrected data may cause newer vintages of data to be released"); the 2025-07-23
  # Arkansas backfill rewrote 2019-2021. With dataLag: 3, a 4-year lookback reopens publish years
  # P-3..P (data years P-6..P-3), which covers that backfill depth.
  # Does NOT cover a format-version recast (LODES 8.0 recast all of 2002+ into 2020 census blocks)
  # -- that requires a full-table reprocess.
  # https://lehd.ces.census.gov/data/lodes/LODES8/LODESTechDoc8.3.pdf
  lookbackPeriods: 4
freshness:
  type: last_modified
  # NO probe_url -- HEAD the data file itself. Verified 2026-08-31: the .csv.gz carries its own
  # Last-Modified, and it is strictly more precise than the per-state version.txt sidecar.
```

**Probe the data file, not the `version.txt` sidecar.** Live comparison (2026-08-31) shows they
disagree, and the sidecar is worse:

| URL | `Last-Modified` | Granularity |
|---|---|---|
| `ar/wac/ar_wac_S000_JT00_2021.csv.gz` | **2024-10-03** | per (state, year) — exactly the fetch unit |
| `ar/version.txt` | **2025-12-03** | per state — fires for *any* change in that state |

The sidecar is 14 months newer than the 2021 WAC file, so keying on it would re-ingest **every
year** for Arkansas whenever *anything* in that state changed. The data file's own header is both
simpler (no `probe_url`) and correctly scoped.

> Correction to the lookback rationale: the 2025-07-23 Arkansas backfill cited in the research
> affected **RAC** files. This table reads **WAC**, whose 2021 file is untouched since 2024-10-03.
> The `lookbackPeriods: 4` still stands on the general vintage-reissue mechanism, but the comment
> must not cite that specific incident as evidence for *this* table.

**Granularity is intentional and correct.** `version.txt` is per-*state*, while the fetch unit is
(year, state) — so every year for a state shares the vintage signal, and a vintage bump reopens all
that state's in-window years together. That is exactly how the Arkansas correction behaved
(2019-2021 rewritten as one event).

**Follow-up worth considering (engine change, not required now):** the file also carries
`Release Format Version 8.4`. Extending `VERSION` to extract a line from plain text by pattern
would key on the vintage explicitly, be immune to a spurious restamp, and could detect a format
recast — the one case a lookback structurally cannot cover.

### `economic_census` — census @ 3805 · **N=8 is deliberate**

```yaml
year:
  type: yearRange
  step: 5
  dataLag: 0
  # Economic Census re-releases corrected files years after first publication (2017 NAICS 51 tables
  # revised 2020-06-02; 2022 Bridge/Comparative updated 2025-04-24) and publishes each vintage in
  # waves over ~2 years. 2 = the two most recent PUBLISHED editions (2022 and 2017) -- the walk
  # counts published periods, so the 5-year step needs no inflated value.
  # https://www.census.gov/programs-surveys/economic-census/data/errata-notes.html
  lookbackPeriods: 2
freshness:
  type: hash
```

> **The test must assert it reopens the two most recent published editions (2022, 2017) — not
> zero.** Zero means the walk is counting calendar years rather than published periods, which the
> 5-year step would then swallow entirely. Not-yet-published editions (2027+) are simply never
> complete, so the walk steps over them without consuming budget.

---

## Group 5 — Plain freshness gate, no lookback (2)

| Table | File | table @ |
|---|---|---|
| `jolts_industries` | econ-reference | 133 |
| `jolts_dataelements` | econ-reference | 197 |

```yaml
freshness:
  type: hash
  # No lookback: this is a CODE LIST, not a time series -- there are no values to restate, and the
  # table has no year dimension (so lookbackPeriods cannot even be expressed here, by design).
  # Must be hash: BLS restamps EVERY file under /pub/time.series/jt/ with an identical
  # Last-Modified and ETag on each monthly release, so etag/last_modified would fire monthly on
  # unchanged content. Plain hash re-reads the ~1KB file each run and writes only on a real change.
```

**Test:** two runs — unchanged ~1 KB list produces **no** Iceberg snapshot; a mutated one does.
Cheapest tables in the plan. Worth an explicit negative test asserting `etag` *would* misfire here.

> These two are the clearest illustration of why the setting moved to the dimension block: with no
> year dimension, `lookbackPeriods` is now **unwriteable** here rather than silently inert.

---

## Group 6 — `dataLag` → `dataMonthLag` (5) · **coverage change**

**Blocked on P2.** Each table gains ~2 years it has never held — validate as a **backfill**, with
per-year row counts before and after, DQ bucket only.

| Table | File | table @ | Change | Shape |
|---|---|---|---|---|
| `eia_electricity_prices` | energy | 370 | `dataLag: 2` → `dataMonthLag: 2` | year-grain |
| `eia_crude_oil_imports` | energy | 1519 | `dataLag: 2` → `dataMonthLag: 2` | year-grain |
| `eia_electricity_generation` | energy | 221 | `dataLag: 2` → `dataMonthLag: 2` | **month list** |
| `eia_refinery_operations` | energy | 1644 | `dataLag: 2` → `dataMonthLag: 2` | **month list** |
| `eia_fossil_fuel_production` | energy | 977 | `dataLag: 2` → `dataMonthLag: 2` **+ lookback 2→3** | **month list** |

```yaml
# BEFORE
year:
  type: yearRange
  dataLag: 2                        # YEARS -- caps data at 2024 in 2026

# AFTER
year:
  type: yearRange
  # EIA-923/PSM publish ~2 MONTHS after the reference month ("within 60 days of the close of the
  # reference month"; the Aug 26 2026 release carried June 2026 data). dataLag: 2 expressed that in
  # YEARS and discarded ~2 years of published data.
  # https://www.eia.gov/petroleum/supply/monthly/pdf/psmnotes.pdf
  dataMonthLag: 2
```

```yaml
# Table-level. eia_electricity_generation and eia_refinery_operations (year+month period slots):
# 24 periods = 2 YEARS of monthly data. Unchanged in duration, but now strictly necessary -- see
# the note below.
lookbackPeriods: 24
```

`eia_fossil_fuel_production` additionally goes to **3**:

```yaml
  # 36 periods = 3 YEARS of monthly data (this table has year+month period slots).
  # EIA-914: the two prior months are re-estimated every month, and the annual PSA (Aug) restates
  # up to 10 prior years of oil production, the NGA (Oct) two prior years of gas. EIA considers a
  # state final "prior to two calendar years back", so 2 years sat exactly on that boundary with
  # zero margin; 3 years covers it with a year to spare.
  # https://www.eia.gov/petroleum/production/pdf/eia914revisionpolicy.pdf
  lookbackPeriods: 36
```

**Note the interaction:** with `dataLag` reduced, the lookback on the electricity tables stops being
belt-and-braces and becomes **strictly necessary** — they will then be fetching data years still
inside their revision window.

**Tests:** (1) previously-absent years materialize; (2) no combo past `vintageCeiling`;
(3) **January boundary** — current year absent, prior year caps at month **11**. The three
month-list tables exercise Shape B, which nothing caps today.

---

## Group 7 — Comment-only (1 table + 10 sites)

**No functional change; verify by review, no ETL run.**

### `eia_petroleum_stocks` — energy @ 1308 · keep the lookback at 2, fix the rationale

```yaml
year:
  type: yearRange
  dataLag: 0
  # WPSR data are never revised: "Absent significant errors, omissions, or natural disasters, we
  # never revise the data published in the WPSR." The lookback here is for ACCRUAL, not revision --
  # dataLag: 0 with a year-grain partition holding weekly rows means the current year keeps gaining
  # ~52 rows and markPeriodComplete would freeze it after the first run; the second year catches
  # the late-December weeks published after the calendar rolls over.
  # https://www.eia.gov/tools/faqs/faq.php?id=1398&t=6
  lookbackPeriods: 2
```

### `energy-schema.yaml` — 10 repeated sites

Lines **222, 371, 471, 641, 840, 978, 1309, 1520, 1645, 1760** carry an identical block quoting
EIA's *Monthly Energy Review* blanket "revised the following month" language. That is accurate for
the EIA-914 petroleum/gas series and **wrong for the electric-power series**, whose technical notes
say monthly data are revised **once** then final. Replace each with the program-specific rationale.

> Worth doing precisely because the current text would lead a future reader to "correct" a right
> value to a wrong one — `eia_petroleum_stocks` is the live example.

---

## Group 8 — `qwi_employment`: express as canonical `year` + `quarter` periods

**census-schema.yaml @ 4535.** Promoted from "separate defect" to in-scope. Fixes three problems at
once, but it is the highest-risk item in this document because it changes the partition scheme.

### Current

```yaml
dimensions:
  type:  [qwi]
  time:  ["2022-Q1", ... "2024-Q4"]     # hardcoded 12 values -- ~6 quarters stale as of Aug 2026
  state: [51 FIPS]
source:
  url: "https://api.census.gov/data/timeseries/qwi/sa?get=...&for=state:{state}&time={time}&..."
pattern: "type=qwi/time=*/*.parquet"
materialize:
  partition: {columns: [type, time]}
  output:    {pattern: "type=qwi/time=*/"}
```

`time` is a real temporal axis, but **`time` is not a canonical period slot**
(`PERIOD_SLOTS = year, quarter, month, week, day, day_of_week`), so the table is not period-tracked
at all: `hasCanonicalPeriod` is false, period completion never applies, and `lookbackPeriods` would
be rejected.

### Target

```yaml
dimensions:
  type: [qwi]
  year:
    type: yearRange
    start: "${GOVDATA_START_YEAR:2010}"   # QWI reaches back to ~2002 for many states
    end: current
    dataMonthLag: 6                       # QWI publishes ~2 quarters behind
  quarter: ["1", "2", "3", "4"]
  state: [51 FIPS]
source:
  url: "...&for=state:{state}&time={year}-Q{quarter}&..."
pattern: "type=qwi/year=*/quarter=*/*.parquet"
materialize:
  partition: {columns: [type, year, quarter]}
  output:    {pattern: "type=qwi/year=*/quarter=*/"}

# NO lookbackPeriods -- deliberately absent, do not add one. Census rebuilds the COMPLETE QWI
# time series every quarterly release, so "which recent periods changed?" has no useful answer:
# it is always all of them. A single release-level freshness check gates the whole table instead.
# https://lehd.ces.census.gov/doc/QWI_data_notices.pdf
```

**What this fixes:**

1. **Stale hardcoded window.** The 12-value list ends at `2024-Q4` and drifts further every
   quarter. A `yearRange × quarter` cartesian advances on its own.
2. **Not period-tracked.** Canonical slots make period completion, the per-period freshness gate,
   and `lookbackPeriods` all applicable.
3. **Publication lag was unexpressed.** `dataMonthLag: 6` states QWI's ~2-quarter lag — and this is
   a good demonstration that the unified lag generalises: a quarterly cadence needs no
   `dataQuarterLag`, it is just 6 months.

### ⚠️ This is a partition scheme change — purge and full reprocess required

Existing data is written under `time=2022-Q1`; the new scheme writes `year=2022/quarter=1`. The old
partition values do not migrate and will not be overwritten in place. Per the standard fix for
partition-scheme drift: **`data_purge.sh` for this table, then a full reprocess** — not a bespoke
Iceberg delete, and not a plain re-run on top of the old layout.

Verify `${GOVDATA_START_YEAR}` before reprocessing — the target range is much wider than the 12
quarters currently held, so this is a substantial backfill, not a re-shape of existing rows.

### No lookback at all — QWI needs a single release-level freshness check

Census rebuilds the **complete** QWI time series on every quarterly release, and explicitly warns
against combining data across releases:

> *"The complete QWI time series is updated with every quarterly data release… we generally advise
> that users do not combine data from different QWI releases."*
> — [QWI Data Notices](https://lehd.ces.census.gov/doc/QWI_data_notices.pdf)

**So `lookbackPeriods` is not merely too small here — it is the wrong mechanism.** A lookback
answers *"which recent periods might have changed?"*, and for QWI the answer is always *"all of
them"*. There is nothing for it to narrow down.

The correct model is **one freshness check for the whole table**:

```
release signal unchanged  ->  skip the entire table (one probe)
release signal changed    ->  reprocess every period
```

`lookbackPeriods` must therefore be **absent** on this table — not set to a large number.

### Release signal — researched and found (live probes, 2026-08-31)

| Candidate | Result |
|---|---|
| QWI API endpoint headers | ❌ `HEAD` returns **302**, no `Last-Modified`/`ETag` |
| Census API dataset metadata `modified` | ❌ frozen at **2015-04-16** — 11 years stale, does not track releases |
| LODES-style `version.txt` | ❌ 404 at every path tried |
| Directory listings | ⚠️ release dirs **are** exposed (`R2025Q4` … `R2026Q3`), but listings serve no `Last-Modified`/`ETag` |
| **Per-state `md5sum` sidecar** | ✅ **usable** |

**The signal:** `https://lehd.ces.census.gov/data/qwi/latest_release/{state}/qwi_{state}.md5sum`

```
e0208cb287d752b9eadd7e87d3f28b5a  qwi_ak_sa_f_gc_n3_op_u.csv
a75f00860f15d8e6eb9f339f17a30ce6  qwi_ak_sa_f_gc_n3_oslp_u.csv
...                                                    (132 lines, 8,184 bytes)
Last-Modified: Thu, 06 Aug 2026 13:24:54 GMT
```

Also confirmed: **`latest_release/` is a live alias for the newest release** — its bytes are
identical to `R2026Q3/`, so probing `latest_release` tracks whatever is current without hardcoding
a label.

```yaml
freshness:
  type: last_modified
  # QWI publishes no version.txt and the API serves no usable headers (HEAD -> 302); the Census
  # API's own `modified` field is frozen at 2015-04-16. The per-state md5sum sidecar under
  # latest_release/ is the only real release signal: 8KB, a genuine Last-Modified, and
  # latest_release/ is byte-identical to the newest release dir (verified 2026-08-31 vs R2026Q3).
  probe_url: "https://lehd.ces.census.gov/data/qwi/latest_release/{state}/qwi_{state}.md5sum"
```

`type: checksum` on the same URL also works (`firstTokenOf` returns the first line's md5) and is
immune to a restamp-without-change, but it reads **only the first of 132 lines** — adequate for a
whole-series rebuild where every file changes, fragile for a partial one. Prefer `last_modified`
(a HEAD, no body) unless a spurious restamp is observed.

**Granularity is per-state, which is better than table-level.** QWI reprocesses delayed states
independently, so a state's sidecar bumping should reopen that state's periods and no others.

### ⭐ The design collapses: fetch files, ask only "did the file change?"

Switch the source from the per-quarter API to the per-state bulk files and **every complication
above stops being a design input.** Each file carries that state's entire history, so:

```yaml
source:
  url: "https://lehd.ces.census.gov/data/qwi/latest_release/{state}/qwi_{state}_sa_f_gs_ns_op_u.csv.gz"
dimensions:
  type:  [qwi]
  state: [51 FIPS]        # NO year/quarter fetch axis -- the file holds all periods
freshness:
  type: last_modified     # HEAD the file itself; no probe_url needed, the source URL IS the file
```

Verified: the `.csv.gz` serves `Last-Modified: Thu, 06 Aug 2026 13:24:44` directly, so the md5sum
sidecar is not even required — HEAD the file you are about to download.

**51 files. Did each change? If yes re-ingest it whole; if no skip it.** That is the entire
freshness model, and it subsumes everything else:

| Complication | Why it stops mattering |
|---|---|
| Release-level detection | The file's `Last-Modified` *is* the release signal |
| Retroactive backfills (WY +11 yrs) | The file changed → re-ingest it whole. Depth is irrelevant |
| Per-state currency churn | Whatever the file contains is what we materialize |
| Whole-series rebuild each release | Expected, not a problem — we replace the file's data wholesale |
| `lookbackPeriods` | **Not applicable.** No period fetch axis to walk back over |
| `dataMonthLag` / `dataLag` | **Not applicable.** No year range to cap |
| Coverage variance (1991–2010 starts, AK at 2016) | Descriptive only. Affects DQ expectations, not config |

This is the `world_indicators` / `ilostat_indicators` pattern exactly, and it removes this table
from Groups 1–6 entirely — no lag knobs, no lookback, no canonical-period migration.

### Partitioning is a free performance choice — with one coupling

Partitioning is normally chosen for query performance alone. Under `overwritePartitions: true` it
*also* sets the blast radius of a replace
([IcebergMaterializationWriter.java:1420](../../file/src/main/java/org/apache/calcite/adapter/file/etl/IcebergMaterializationWriter.java#L1420)):

> *"`replacePartitionsDataFiles` fully replaces whatever partitions its files touch — it does not
> merge with a prior commit's files for the same partition."*

That only bites when **both** hold: freshness skips some files, **and** the partition is coarser
than one file's scope. If only Alaska's file changed, the run's buffer holds Alaska rows only, and
a replace on `year=2000..2016` swaps those partitions to contain *just Alaska* — dropping the other
50 states that live there.

**The fix is to partition on `state`, matching the fetch unit — never to reprocess unchanged
files.** One partition per file makes a replace mean exactly "swap this state's data for this
file's contents": correctly scoped, nothing clobbered, nothing re-downloaded.

Sized against measured row counts (avg **55,763** rows/state across a 7-state sample → ~2.84M rows
for 51 states):

| Partition spec | Partitions | Rows/partition | Verdict |
|---|---|---|---|
| **`[type, state]`** | **51** | **55,763** | ✅ **clean — one partition per file** |
| `[type, state, year]` | 1,785 | 1,593 | too granular — small files |
| `[type, state, year, quarter]` | 7,140 | 398 | far too granular |
| `[type, year, quarter]` | 140 | 20,314 | unsafe with per-file skip |

**Use `[type, state]`** with `sortOrder: [year, quarter]`. Year-filtered queries prune via Parquet
row-group statistics within a state partition rather than via partition pruning — appropriate at
~56k rows per partition, and far better than the ~400-row partitions that adding `quarter` would
produce.

This also removes the need for per-row partitioning entirely: `state` is a fetch dimension, so the
partition value is known at write time. `year` and `quarter` stay ordinary row columns.

### DECIDED: use the bulk files. Scope = one slice, all 81 columns.

**Source:** `qwi_{st}_sa_f_gs_ns_op_u.csv.gz` — matching the table's existing grain (Sex-by-Age,
state geography, all-industry sector, private ownership). **One slice, not all 132.** Other grains
(county `gc`, metro `gm`, NAICS 3–6 digit, state-local-government `oslp`, Race-by-Ethnicity `rh`)
are additional *tables*, not additional columns — a separate scope decision, deliberately deferred.

**Materialize all 81 columns** (17 dimension/key + 64 measure/status), not the 5 the API fetch
currently projects.

### ⚠️ Do NOT type these columns by inference

Running type inference over a 2,000-row CA sample produces **two classes of error**, both silent:

| Trap | Example | Inferred | Correct |
|---|---|---|---|
| **Code columns read as numeric** | `geography` = `"02"` (Alaska FIPS) | `bigint` → **`2`** | **`varchar`** — leading zeros are significant |
| **Sparse columns read as text** | `HirN`, `HirR`, `HirNS`, `EarnHirNS`, `Payroll` — all-empty in the sample | `varchar` | numeric per QWI docs |

The FIPS case is the dangerous one: `"02"` → `2` silently breaks every join to a geography
reference table. **Type these against the QWI documentation, not against a sample.**

Rules to apply:
- **`varchar` (codes, leading zeros / non-numeric):** `geography`, `industry`, `agegrp`, `race`,
  `ethnicity`, `education`, `ownercode`, `geo_level`, `ind_level`, `periodicity`, `seasonadj`,
  `sex`, `firmage`, `firmsize`, `agg_level`
- **`int`:** `year`, `quarter`
- **`bigint`:** count measures (`Emp`, `EmpEnd`, `HirA`, `Sep`, `Payroll`, …)
- **`double`:** rate measures (`HirAEndR`, `SepBegR`, `HirAEndReplR`, `TurnOvrS`, …)
- **`int`:** the 32 `s*` status/suppression flags (small enumerated codes)

The 32 `s*` flags are **not optional** — without them a suppressed cell is indistinguishable from a
genuine zero.

### Supporting detail (why the data looks the way it does — not config input)

The state directory holds the **full time series** as bulk CSVs — the same files the md5sum covers.
Switching the source from the per-quarter API to those bulk files would make this the same shape as
`world_indicators` / `ilostat_indicators`: one fetch per state carrying all history, with
`overwritePartitions` replacing that state's data wholesale.

| | Fetches per release |
|---|---|
| Current: API, per (quarter, state) | ~4,700 |
| Bulk CSVs, per state | **51** |

That is a ~90× reduction, and it makes "the whole series rebuilt each release" a natural fit rather
than something to work around.

### The current design discards ~92% of the available measures

Inspected `qwi_ak_sa_f_gs_ns_op_u.csv.gz` live (2.2 MB gzipped, 35,748 rows for one state slice):

| | Count |
|---|---|
| Columns in the file | **81** — 17 dimension/key + 64 value columns |
| Of the 64: real measures | ~35 |
| Of the 64: `s`-prefixed status/suppression flags | ~29 |
| **Captured by the current API fetch** | **5** (`Emp, EmpS, HirA, Sep, EarnS`) |
| **Discarded** | **59 of 64 (92%)** |

Discarded measures include `EmpEnd`, `EmpTotal`, `HirN`, `HirR`, `SepBeg`, `TurnOvrS`, `FrmJbGn`,
`FrmJbLs`, `FrmJbC`, `EarnBeg`, `EarnHirAS`, `EarnSepS`, `Payroll` and their seasonally-adjusted
variants.

**The missing status flags are a correctness problem, not just a coverage one.** Every measure has
a paired `s`-prefixed flag (`sEmp`, `sHirA`, `sEarnS`, …) carrying its suppression/reliability
status. Without them a suppressed cell is indistinguishable from a genuine zero — the table cannot
tell "no employment" from "withheld for disclosure avoidance."

**Seven demographic and firm-characteristic dimensions are also unused entirely:**
`sex, agegrp, race, ethnicity, education, firmage, firmsize`. These are *columns within the same
file* — the current state-level API fetch collapses past them, so the breakouts are available at no
extra fetch cost and are simply being dropped.

### Design requirement: materialize the full file

**Ingest every column of whichever slice file is fetched — do not re-implement the 5-variable
subset.** The data is already paid for at download time; discarding 59 of 64 value columns and 7
breakout dimensions to reproduce the API's narrow projection would waste the entire benefit of
switching.

This does not mean fetching all 132 slices per state. Select the slice matching the table's grain
and materialize it whole. If additional grains are wanted later, they are additional slices — a
scope decision, not a column-projection one.

**Filename decode (verified, 2026-08-31)** — `qwi_{st}_{sa|rh}_f_g{s|c|m}_n{s|3|4|5|6}_o{p|slp}_u`:

| Token | Meaning |
|---|---|
| `sa` / `rh` | demographic breakout: **Sex-by-Age** / Race-by-Ethnicity |
| `gs` / `gc` / `gm` | geography: state / county / metro |
| `ns` / `n3`…`n6` | industry: sector / NAICS 3–6 digit |
| `op` / `oslp` | ownership: private / state-local-private |
| `u` | unadjusted |

> **`sa` means Sex-by-Age, not "seasonally adjusted".** Seasonal adjustment is the `seasonadj`
> **column** (value `U` throughout these files), not part of the filename. This matches the API path the table already uses (`/timeseries/qwi/sa`) and
> the Census dataset title, *"QWI: Sex by Age"*. The matching slice is therefore
> **`qwi_{st}_sa_f_gs_ns_op_u.csv.gz`**.

### ⚠️ Per-state coverage varies by decades — verified across a 7-state sample

| State | Coverage (`sa/gs/ns/op` slice) | Rows |
|---|---|---|
| CA | 1991–2025 (35 yrs) | 77,787 |
| TX | 1995–2025 (31 yrs) | 68,256 |
| SD | 1998–2025 (28 yrs) | 60,831 |
| NY | 2000–2025 (26 yrs) | 58,914 |
| WY | 2001–2025 (25 yrs) | 54,135 |
| MA | 2010–2025 (16 yrs) | 34,668 |
| **AK** | **2000–2016** (17 yrs) | 35,748 |

**Two independent axes of variance, both real:**

1. **Start years span 1991–2010** — a 19-year spread across just 7 states.
2. **Alaska terminates at 2016** and is the only sampled state that does.

**Alaska is real, but it is NOT a policy change or a withdrawal** — it is an operational lag.
Confirmed from LEHD's own [QWI Data Notices](https://lehd.ces.census.gov/doc/QWI_data_notices.pdf),
which carries a *"States not current in R20XXQY"* section in **every** release, with wording
unchanged from R2017Q2 to R2026Q2:

> *"The time series for the following states are not available through the latest quarter due to
> **delays in provision of input files or data issues pending remediation**."*

Alaska first appears in that list at **R2017Q2** at `2016Q2`, and has been pinned at exactly
`2016Q2` in every release since — ~9 years of unresolved remediation, not a partnership exit.
(Relabeling was independently ruled out: `geography` stays `02`, no blank-year rows, a clean
2,160 rows/yr through 2015 then exactly half in 2016, and the `rh` slice ends 2016 too.)

### The non-current list churns every release — and resumption backfills retroactively

| Release | States not current |
|---|---|
| R2017Q2 | Alaska (2016Q2), **Idaho** (2015Q4), **Wyoming** (2014Q4), PR/VI |
| R2025Q1 | Alaska, Kentucky (2024Q1), Michigan (2021Q4), North Carolina (2023Q2) |
| R2025Q2 | Alaska, **California** (2024Q2), Michigan, Missouri, North Carolina |
| R2026Q1 | Alaska, Florida (2025Q1), Massachusetts (2024Q3), Michigan, Missouri |
| R2026Q2 | Alaska (2016Q2), Michigan (2021Q4) |

The document also records explicit resumptions — *"Resumption of California and North Carolina
Production"* (R2025Q3), *"Resumption of Production for Florida, Massachusetts and Missouri"*
(R2026Q2), *"Resumption of Mississippi QWI Production"* (R2024Q4).

**Wyoming is the decisive case, and it is in our own sample.** It was listed at `2014Q4` in
R2017Q2; the file fetched today holds **2001–2025**. That is roughly **11 years of history
appearing retroactively** in a later release. California was non-current at `2024Q2` as recently as
R2025Q2 and is now current through 2025.

> **This is conclusive support for the no-lookback decision.** No bounded `lookbackPeriods` — 4, 40
> or 400 — would have caught Wyoming's 11-year retroactive backfill. Only whole-table detection
> plus a full reprocess does, which is exactly the release-level model settled above. Per-state
> currency is a *moving target that repairs itself backwards*.

**Bonus: the notices document is a per-release coverage manifest.** It names exactly which states
are behind and their last good quarter. That is the natural place to derive per-state coverage
expectations from, rather than hardcoding them — and it explains why a uniform year range can never
be right for this table.

**Consequences for the config:**

1. **A uniform year range is wrong in both directions.** Early years return nothing for
   late-joining states (MA before 2010); recent years return nothing for Alaska. Empty responses /
   404s must be absorbed, not treated as failures.
2. **Coverage expectations must be recorded per state**, not globally — a single
   `observedCoverage` range for this table cannot describe it honestly.
3. **The current hardcoded `2022-Q1 … 2024-Q4` window yields no Alaska rows at all**, and the
   uniform `time` list conceals that.
4. `observedCoverage` for this table should be treated as suspect until re-derived post-change.

> **Sample caveat:** 7 of 51 states checked. A full sweep before finalising the year range would
> cost ~51 file reads and would establish the real per-state matrix.

> **Not verified:** whether the API agrees with the bulk files on per-state coverage. The endpoint
> requires `CENSUS_API_KEY`, unavailable in this session (returns *"Missing Key"*). Confirm parity
> before switching sources; disagreement would itself be a finding.


**Recommend settling the bulk-vs-API question before implementing anything else here**, since it
changes the source URL, the fetch cadence, the column set, and removes the whole-series-reprocess
problem rather than solving it.

### Implementation detail either way

**Period completion must not block a reprocess.** When the release signal changes, every period has
to be re-examined, so completion markers must be invalidated
(`invalidateTableCompletion` / `clearPeriodCompletions`) — or period completion must not be used
for this table at all.

> **Note the canonical-period change is still required**, independently of this. It fixes the stale
> hardcoded window and the unexpressed lag, and makes the table period-tracked — which is what lets
> period completion be reasoned about at all.

---

## Group 9 — Phase 2: CPI family (5) · comment-only, values unchanged

Researched 2026-08-31. **All five keep `lookbackPeriods: 2`; all five have the wrong stated
reason.** Their comments were copied from `employment_statistics`, which loads `LNS*` CPS
*seasonally adjusted* series where the rationale is true — and does not apply here.

| Table | econ-schema line | Series | Revises? | Value |
|---|---|---|---|---|
| `regional_cpi` | 926 | `CUUR0100SA0` etc. — all CPI-U **unadjusted** | **No** | 2 (accrual only) |
| `metro_cpi` | 1057 | 20 × `CUURS**SA0` — unadjusted | **No** | 2 (accrual only) |
| `regional_food_cpi` | 1361 | `CUUR*SAF11` — unadjusted | **No** | 2 (accrual only) |
| `metro_food_cpi` | 1494 | 20 × `CUURS**SAF11` — unadjusted | **No** | 2 (accrual only) |
| `inflation_metrics` | 782 | 2 × `CUUR` + **`WPUFD49207` (PPI)** | **Yes — PPI only** | 2 |

**The four pure-CPI tables never revise.** BLS: *"The CPI-U and CPI-W are considered final when
released."* The 5-year restatement applies **only** to seasonally adjusted (`CUSR`) series. There is
no CPI "annual February benchmark revision" of unadjusted index levels — that phrase describes a
CES/CPS employment concept and was copied in error.

They still need `2`, for **accrual**: one combo covers a whole year, and the period-completion gate
would otherwise freeze the current year mid-stream and lock out December, which BLS publishes in
mid-January.

```yaml
    # CPI-U unadjusted (CUUR) index levels are FINAL WHEN RELEASED and never revised -- the 5-year
    # restatement applies only to seasonally adjusted (CUSR) series, whose seasonal factors are
    # recomputed each February. https://www.bls.gov/cpi/technical-notes/
    # lookbackPeriods is here for ACCRUAL, not revision: one combo covers a whole year, and the
    # period-completion gate would otherwise freeze the current year mid-stream and lock out
    # December, which BLS does not publish until mid-January of the following year.
    lookbackPeriods: 2
```

**`inflation_metrics` is the genuine trap — a mixed series set.** Two CPI-U unadjusted series that
never revise, plus one **PPI** series that does: *"All indexes are subject to revision for 4 months
after their originally scheduled publication."* That includes unadjusted PPI. December PPI stays
open until the following May, so the prior year must stay reopenable.

```yaml
    # Two of three series are CPI-U unadjusted (CUUR), final when released and never revised. The
    # third, WPUFD49207, is PPI: "All indexes are subject to revision for 4 months after their
    # originally scheduled publication" (https://www.bls.gov/news.release/ppi.htm) -- so December
    # PPI stays open until the following May and the prior year must stay reopenable.
    # Also covers accrual: the completion gate would otherwise freeze the current year mid-stream.
    lookbackPeriods: 2
```

> **Method note.** A "never revised" finding does **not** imply "no lookback needed" — accrual is a
> separate driver. Four of these five would have been wrongly dropped to 0 on the revision research
> alone. See the requirement doc's two-driver table.

---

## Group 10 — Phase 2: "BEA family" (5) · the assumption was wrong on 4 of 5

Researched 2026-08-31. The audit assumed these were BEA-sourced and would need `6` like the other
BEA tables. **Only one does.** One is not even a BEA table.

| Table | line | Program | Was | **Now** | Why |
|---|---|---|---|---|---|
| `national_accounts` | 3898 | BEA **NIPA** | 2 | **6** | 2026 annual update reopens 2021:Q1–2026:Q1 |
| `regional_income` | 4912 | BEA **Regional** | 2 | **8** | Open period is *per series*: GDP-by-state 2018–2024, personal income **2009**–2024 |
| `fdi_activities` | 5562 | BEA **AMNE** | 2 | **3** | One-year preliminary→revised cycle, +1 for `dataLag: 1` |
| `state_industry` | 1647 | **BLS CES SAE** — not BEA | 2 | **2** ✅ | NSA-only filter; the 5-year window is SA-only |
| `regional_price_parities` | 4104 | BEA Regional (SARPP) | 2 | **none — wrong tool** | Restates the **whole series** most years |

### `regional_price_parities` — a third full-series-rebuild table

Joins `qwi_employment` and `world_indicators`/`ilostat_indicators` in the category where a bounded
lookback is the wrong mechanism entirely. BEA restated **2008–2021** in the Dec 2023 release and
**2008–2023** in the Feb 2026 release. Sizing a value would mean `currentYear − 2008` — 19 today,
growing annually, which is the signature of a mechanism mismatch rather than a value to tune.

**No `lookbackPeriods`.** Full reprocess of 2008–present after each annual RPP release
(next: 2026-12-10). Same shape as the QWI decision.

### `regional_income` — 8 covers GDP, but not personal income

The 2026 update reopened state personal income to **2009** — 17 years — while GDP-by-state went to
2018. Both series live in the same partitions, so a lookback sized to GDP leaves stale income rows.
**8 is a floor, not sufficiency:** this table also needs a deliberate full reprocess after each
September regional annual update.

> `regional_income` is also on the [clobbering defect](DEFECT-replace-partitions-clobbering.md)
> list (`hash` + lookback, partition `[type, year]`, three unpartitioned fetch dims). Layer 1
> applies here too — and its high dimension count is why Layer 2 is **not** recommended for it.

### `state_industry` — not a BEA table at all

BLS CES State & Area, bulk TSV, `rowFilter ^SMU\d{2}00000\d{2}00000001` = **not seasonally adjusted
only**. The 2025 benchmark revised NSA data April 2024–December 2025 — exactly 2 calendar years.
The 5-year restatement applies only to SA series, which this filter excludes. **`2` was already
right, for a reason nobody had recorded.**

Two secondary fixes here: its comment says "annual February benchmark revision", but the 2025
benchmark shipped with January 2026 data **in April 2026**; and its `start:` uses
`{env:GOVDATA_START_YEAR:2020}` rather than the `${...}` form used everywhere else in the file.

### Three comments are wrong, not merely under-sized

`regional_price_parities`, `fdi_activities` and neighbours carry a pasted *"BEA advances quarterly
GDP-family estimates (advance → second → third)"* rationale against **annual-only, non-GDP**
products with no quarterly cycle at all. This is the generic-comment defect this document warns
about in its own preamble.

> **Evidence caveat recorded by the research:** BEA publishes no standing revision-policy page for
> RPP or AMNE — those two conclusions rest on release-note text, not a policy statement. The NIPA
> glossary says "at a minimum the 3 most recent calendar years… but BEA may extend", which is
> weaker than observed practice; the observed 6-year 2026 span is the better basis.

---

## Group 11 — Phase 2: BLS-other (11) · **9 of 11 were wrong, all too narrow**

Researched 2026-08-31. The largest correction in the audit. **The deciding factor is almost always
whether the loaded series are seasonally adjusted** — BLS restates 5 years of SA data annually, so
an SA table needs 6, not 2.

| Table | econ/energy line | Program | Adjusted? | Was | **Now** |
|---|---|---|---|---|---|
| `employment_statistics` | 510 | CPS + CES | **SA** (`LNS*`, `CES*S`) | 2 | **6** |
| `wage_growth` | 2896 | CES national | **SA** | 2 | **6** |
| `regional_employment` | 3135 | LAUS | **SA** (`LASST*`) | 2 | **6** |
| `jolts_regional` | 2427 | JOLTS | **SA** (`^JTS`) | 2 | **6** |
| `jolts_industry` | 2597 | JOLTS | **SA** (`^JTS`) | 2 | **6** |
| `jolts_state` | 2742 | JOLTS | **SA** (`^JTS`) | 2 | **6** |
| `labor_productivity` | 3012 | BLS Productivity (PR) | **SA** | 2 | **6** |
| `metro_industry` | 1916 | CES State & Area | **NSA** (`^SMU`) | 2 | **3** |
| `eia_coal_mines` | 1759 | **MSHA — not EIA** | n/a | 2 | **2** ✅ |
| `county_wages` | 2291 | QCEW | n/a | 2 | **2** ✅ |
| `metro_wages` | 2039 | QCEW | n/a | 2 | **2** ✅ |

**8 of 11 need widening; 3 are already correct.** Every error ran in the same direction — too
narrow — which is the failure mode that serves stale data silently.

### The JOLTS finding is the one that would have been missed

All three JOLTS tables filter `^JTS…`, and per `jt.txt` position 3 is the seasonal code — so
**every row loaded is seasonally adjusted**, with the unadjusted `JTU*` rows in the same file
excluded. The 2024 benchmark article: *"Both seasonally adjusted and not seasonally adjusted data
back to January 2020 are subject to revision"*, and both are recalculated for the most recent
5 years. A reader skimming the filter would not see this.

### `eia_coal_mines` — MSHA, not EIA; and `2` is correct (measured)

Source is `https://arlweb.msha.gov/OpenGovernmentData/DataSets/MinesProdYearly.zip` — **MSHA**
Form 7000-2 filings. No EIA involvement. Its current comment describes EIA monthly-series revision
behavior and is factually wrong about the publisher.

**Revision depth measured, not assumed.** MSHA publishes no finality policy, but the MinIO raw
cache retains an immutable copy of the zip per ingested year, giving real vintages to diff.
**2026-06-12 vs 2026-08-28** (11 weeks apart), keyed on `(mine_id, subunit_cd, calendar_yr)`:

| report year | common keys | changed |
|---|---|---|
| 2026 (in progress) | 1,584 | **1,024** |
| 2025 | 1,284 | **7** |
| 2024 | 1,397 | **0** |
| 2023 → 2009 | — | **0** |

Restatement reaches `report_year` Y−1 and stops. Combined with the partition skew below, the
publish years `{Y, Y−1}` that a 2-period lookback reopens carry exactly the two churning report
years. **`2` is correct — leave it.**

```yaml
# MSHA MinesProdYearly (Form 7000-2 quarterly filings rolled to calendar year), republished weekly
# as a complete file replacement. Vintage diff (2026-06-12 vs 2026-08-28) shows the in-progress
# year churning heavily and the prior year still absorbing late/amended filings (7 keys edited),
# with every earlier year byte-identical -- current + prior data year is the real revision surface.
# https://arlweb.msha.gov/opengovernmentdata/ogimsha.asp
lookbackPeriods: 2
```

> Caveat: one 11-week observation window. A filing arriving more than two years late would be
> missed, and there is no published basis to widen beyond 2.

### 🔴 CONFIRMED DEFECT: `eia_coal_mines` partitions are mislabeled by one year

Verified against the live warehouse 2026-08-31 — a systematic +1 skew on **every** partition:

| partition `year` | `report_year` contained | rows |
|---|---|---|
| 2025 | **2026** | 1,674 |
| 2024 | **2025** | 1,312 |
| 2023 | **2024** | 1,427 |
| 2022 | **2023** | 1,521 |

**`WHERE year = 2024` silently returns 2025 data.** Cause: the source URL is static (no
`{effective_year}`), so the transformer loads the *publish* year's data, while
`valueSource: {year: effective_year}` labels the partition `publish − 1`.

`county_wages` / `metro_wages` do **not** have this — their URLs interpolate `{effective_year}`, so
label and data agree. Separate from the lookback redesign; needs a Defect Register entry.

---

## Summary

| Group | Tables | Blocked on | Risk |
|---|---|---|---|
| 1 Widen | 7 | P1 | Low — integer change + relocation |
| 2 Narrow | 1 | P1 | Low — tidiness |
| 3 Add lookback (keep etag) | 2 | P1 | Low, but assert HEAD-not-GET |
| 4 Add freshness + lookback | 5 | P1 | **High** — no gate today; cold-start risk |
| 5 Plain gate | 2 | — | Low — cheapest in the plan |
| 6 `dataMonthLag` | 5 | **P2** | **High** — coverage change |
| 7 Comments | 1 (+10 sites) | — | None |
| 8 `qwi_employment` canonical periods | 1 | **P2** (`dataMonthLag`) | **Highest** — partition scheme change; purge + full reprocess |

**24 distinct tables** (`eia_fossil_fuel_production` appears in Groups 1 and 6).
**Plus 37 tables** needing the retired-key migration to `lookbackPeriods`.

**Not in scope:** the 21 unverified tables (Phase 2 — five look BEA-sourced and likely need 6 by
the same evidence), and the 3 separate defects (`qwi_employment` stale time list,
`state_personal_income` coverage gap, anchor rename).
