# Research & Development (NSF / NCSES) Data Plan — RESEARCH Schema

## Status: DELIVERED (2026-07-16) — in a standalone `research` schema

All three NCSES R&D tables are **implemented and merged to main** (`ab2248bff`) in a standalone **`research`** schema. They ship in [research-schema.yaml](../../../src/main/resources/research/research-schema.yaml) with transformers under [org.apache.calcite.adapter.govdata.research](../../../src/main/java/org/apache/calcite/adapter/govdata/research/). (An earlier draft of this plan proposed folding them into `econ`; the plan body below has been updated to describe the delivered `research` schema.)

| Planned table (plan said `econ.*`) | Delivered as | Java transformer |
|---|---|---|
| `nsf_national_rd` | `research.nsf_national_rd` | `NsfNationalRdTransformer` |
| `nsf_herd_by_institution` | `research.nsf_herd_by_institution` | `NsfHerdTransformer` |
| `nsf_federal_rd_obligations` | `research.nsf_federal_rd_obligations` | `NsfFederalRdTransformer` |

**Delivery deviations:**
- **Standalone `research` schema**, not `econ` — so the marquee patents-per-R&D-dollar join and the `fred_indicators` cross-check are cross-schema (`research` ⟂ `patents` / `econ`), not intra-`econ`.
- HERD keyed on `inst_id` / `ncses_inst_id` (NCSES institution ids), not IPEDS UnitID; the state×year rollup ships as the view `research_herd_rd_by_state_year`.
- No NCSES REST API exists — all three sourced from bulk XLSX/CSV. Locally validated end-to-end (576 / 721 / 196,593 rows).

The proposal below is preserved for historical context.

These tables land in a standalone **`research`** schema dedicated to the **National Science
Foundation's National Center for Science and Engineering Statistics (NCSES)** — one of the 13
Principal Federal Statistical Agencies and the one this project was missing on the science side —
covering U.S. R&D expenditure: how much is spent, who funds it, who performs it, and the
higher-education research base.

## Strategic Context

R&D spending is an **economic statistic** — it belongs beside GDP, employment, and the FRED
series, not inside `patents` (which stays pure USPTO intellectual property: patents + trademarks).
An earlier draft folded these tables directly into `econ`; the delivered design keeps them in
their own **`research`** schema instead, so NCSES R&D sits with its own analytical neighbors and
every downstream link is an explicit cross-schema join. The marquee join is **innovation input ÷
output**: NCSES R&D **spend** by state/year (`research`) against USPTO patent **grants** by
assignee state/year (`patents`) yields a patents-per-R&D-dollar productivity ratio. R&D also joins
cross-schema to `econ.fred_indicators` (R&D-share-of-GDP) and `fiscal.usaspending_by_agency` (the
R&D slice of agency budgets).

**Design** (project rule): one ETL table = one upstream NCSES series, **tall** (one row per
sector/institution/agency × year). Java reads all inputs from the model, never `System.getenv`
(rule #7); an unmapped real institution/state **fails loudly**, never silently defaulted
(rule #6).

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| National Patterns of R&D | NSF NCSES | Bulk CSV + NCSES API | Free, no key | Annual |
| Higher Education R&D (HERD) | NSF NCSES | Bulk CSV + NCSES API | Free, no key | Annual |
| Federal Funds for R&D | NSF NCSES | Bulk CSV + NCSES API | Free, no key | Annual |

NCSES data tables + API: `https://ncsesdata.nsf.gov/` (API base `https://ncsesdata.nsf.gov/api/`).
All series are free, **no key** — nothing to add to `.env.prod`.

---

## Proposed Tables (all in the `research` schema)

### `nsf_national_rd`

National aggregate R&D expenditures (the NCSES "National Patterns of R&D" series), one row per
year × performing sector × funding source. The top-line macro view of U.S. research spending —
joins semantically (cross-schema) to `econ.fred_indicators` R&D-share-of-GDP series and provides
the national denominator for the by-state HERD table.

**Source:** NCSES National Patterns of R&D Resources — `https://ncsesdata.nsf.gov/` (bulk CSV + API)
**Partition:** `[year]`
**Auth:** None
**Cadence:** Annual (prior-final + current-preliminary published ~Q4)
**Release window:** Months 10–12

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Statistics year — partition key |
| performing_sector | VARCHAR | Business / Federal government / Higher education / Nonprofit / FFRDC |
| funding_source | VARCHAR | Federal / Business / Higher education / Nonprofit / Other |
| rd_type | VARCHAR | Basic research / Applied research / Experimental development / Total |
| rd_expenditure_usd_million | DOUBLE | R&D expenditure (current USD millions) |
| rd_expenditure_constant_usd_million | DOUBLE | R&D expenditure (constant/chained USD millions) |
| pct_of_gdp | DOUBLE | Sector R&D as share of GDP (null for non-total rows) |

### `nsf_herd_by_institution`

Higher Education R&D (HERD) survey — R&D expenditures by individual academic institution ×
year × R&D field × funding source. The finest-grained NCSES table; joins to `geo` (institution
state/county), to `edu` (IPEDS) via institution identity for research-intensity-vs-enrollment
analysis, and semantically to `patents.patent_assignees` where the assignee is a university.

**Source:** NCSES HERD Survey — `https://ncsesdata.nsf.gov/` (HERD data tables + API)
**Partition:** `[year]`
**Auth:** None
**Cadence:** Annual (survey year published ~14 months later)
**Release window:** Months 10–12

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Fiscal year of expenditure — partition key |
| institution | VARCHAR | Institution legal name |
| ipeds_unitid | VARCHAR | IPEDS UnitID (join key to edu institution tables; null if unmatched) |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties (null if unresolved) |
| control | VARCHAR | Public / Private nonprofit |
| rd_field | VARCHAR | S&E field (Life sciences, Engineering, Computer & information sciences, Physical sciences, …); "All fields" for totals |
| funding_source | VARCHAR | Federal / State & local / Institution / Business / Nonprofit / Other |
| federal_agency | VARCHAR | Sponsoring federal agency (HHS/NIH, DOD, NSF, DOE, NASA, …); null unless funding_source = Federal detail |
| rd_expenditure_usd_thousand | DOUBLE | R&D expenditure (USD thousands) |

### `nsf_federal_rd_obligations`

Federal Funds for R&D survey — federal R&D obligations by funding agency × performing sector ×
field × year. Shows which federal agencies fund research and where it flows; joins to
`fiscal.usaspending_by_agency` for the R&D slice of agency budgets, and to `fedregister.agencies`
on agency name.

**Source:** NCSES Federal Funds for R&D Survey — `https://ncsesdata.nsf.gov/`
**Partition:** `[year]`
**Auth:** None
**Cadence:** Annual
**Release window:** Months 10–12

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Federal fiscal year — partition key |
| funding_agency | VARCHAR | Federal agency obligating funds (HHS, DOD, NASA, DOE, NSF, USDA, …) |
| performing_sector | VARCHAR | Intramural / Business / Higher education / FFRDC / Nonprofit / State & local |
| rd_field | VARCHAR | S&E field |
| rd_type | VARCHAR | Basic / Applied / Development / R&D plant |
| obligations_usd_million | DOUBLE | Federal R&D obligations (USD millions) |

**Materialize (all three):** `format: iceberg`, partition `[year]`, `overwritePartitions: true`
— NCSES restates prior-year figures on each annual release, so replace year partitions wholesale
(same pattern as `econ.world_indicators` in the sibling schema). New transformers `NsfNationalRdTransformer`,
`NsfHerdTransformer`, `NsfFederalRdTransformer`; each maps one NCSES series to one tall table.
Fail loudly (rule #6) on an unmapped real institution state or IPEDS id — never default to a
placeholder FIPS/UnitID.

---

## Join Architecture

```
geo.states / geo.counties (FIPS)
    └── research.nsf_herd_by_institution (state_fips/county_fips, year)   institution R&D by place

edu IPEDS tables (ipeds_unitid)
    └── research.nsf_herd_by_institution (ipeds_unitid → enrollment / research intensity)

econ.fred_indicators
    └── research.nsf_national_rd (semantic — R&D-share-of-GDP cross-check)

fiscal.usaspending_by_agency / fedregister.agencies
    └── research.nsf_federal_rd_obligations (funding_agency → R&D slice of agency budget)

[cross-schema — innovation input ÷ output]
research.nsf_herd_by_institution (state_fips, year)  ⟂  patents.patent_assignees (state_fips, grant_year)
    → patents-per-R&D-dollar by state over time
```

The productivity ratio is the marquee join: it needs both the `research` R&D-spend half and the
`patents` grant-count half, aggregated to `state_fips × year`. It is a **cross-schema** join
(`research` ⟂ `patents`), which is exactly why the R&D input sits in its own `research` schema and
the IP output stays in `patents` — each dataset lives with its own analytical neighbors, and the
cross-schema key (`state_fips`, `year`) does the rest.

---

## Environment Variables

The `research` schema uses only the global `GOVDATA_*` variables. NCSES needs **no new key** —
nothing to add to the schema YAML or `.env.prod`.

| Variable | Required | Description |
|---|---|---|
| `GOVDATA_PARQUET_DIR` | (global) | Output path |
| `GOVDATA_START_YEAR` / `GOVDATA_END_YEAR` | (global) | Year window |
| `GOVDATA_CACHE_DIR` | (global) | Raw download cache |

---

## Implementation Notes

- **Standalone `research` schema** — the three tables are `partitionedTables` in
  [research-schema.yaml](../../../src/main/resources/research/research-schema.yaml) with their own
  `source=research` Iceberg root and schema worker (`ResearchSchemaFactory`), not folded into
  `econ`.
- **Annual cadence.** Unlike the quarterly patents tables, NCSES is annual — gate the daily
  worker on a `10,11,12` release window (`within_release_window "research_nsf" "10,11,12"`). Small
  tables (national/agency aggregates + ~1K institutions/year); no heap or timeout concern.
- **HERD ↔ IPEDS join.** `nsf_herd_by_institution.ipeds_unitid` is the bridge to the `edu` IPEDS
  tables. NCSES publishes the UnitID crosswalk in the HERD files; fail loudly on an unmatched
  institution rather than dropping the row — an unmatched real university is a data-quality
  signal, not a silent skip (rule #6).
- **Cross-schema productivity view.** The patents-per-R&D-dollar ratio is best expressed as a SQL
  view joining `research.nsf_herd_by_institution` (aggregated to state × year) to
  `patents.patent_assignees` (grant counts by state × year) — a view, never Java reshaping.
- **Verification gate.** Not "done" until an ETL/DQ run + `verify-tables` passes end-to-end
  through the Calcite→DuckDB read path.
