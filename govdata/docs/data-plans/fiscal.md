# Fiscal Schema — IRS Revenue + Federal Outlays Data Plan

## Implementation Status (2026-07-16)

**Partially delivered.** Merged to `main` (`a1a83b220` / `c1c114c25`). 9 tables shipped, each
with a Java provider; `soi_national` is a real gap; `usaspending_awards` is a planned Phase 2
deferral (not a shortfall).

| Planned table | Status | Delivered as |
|---|---|---|
| `soi_income_by_zip` | ✅ Delivered | `SoiZipProvider` |
| `soi_income_by_county` | ✅ Delivered | `SoiCountyProvider` |
| `county_migration_flows` | ✅ Delivered | `SoiMigrationProvider` |
| `exempt_org_990` | ✅ Delivered | `Irs990Provider` |
| `exempt_org_master` | ✅ Delivered | `IrsEoBmfProvider` |
| `usaspending_by_agency` | ✅ Delivered | `UsaSpendingAgencyProvider` |
| `usaspending_by_state` | ✅ Delivered | `UsaSpendingStateProvider` |
| `ssa_benefits_by_geography` | ✅ Delivered | `SsaBenefitsProvider` |
| `sba_loan_approvals` | ✅ Delivered | `SbaLoansProvider` |
| `soi_national` | ❌ **NOT BUILT** | absent from YAML; no provider/transformer exists — real gap |
| `usaspending_awards` | ⏸ **Deferred (Phase 2)** | intentionally deferred per plan; not a shortfall |

**Extension views beyond the plan (delivered):** `ssa_benefits_by_geography_acs`,
`soi_income_by_county_year`, `federal_money_by_state_year`, `nonprofit_financials`.

**Not yet built:**
- `soi_national` — SOI national + corporate historical aggregates. No `SoiNationalProvider`/
  `SoiNationalTransformer` in the code or YAML. Genuine outstanding gap.
- `usaspending_awards` — award-level detail. **Planned Phase 2 deferral**, not a delivery
  shortfall (see the "Phase 2 — deferred" section below).

---

The `fiscal` schema is **new**. It closes the single largest thematic gap in govdata: the
federal government's own money. It pairs the **revenue side** (IRS Statistics of Income —
where tax revenue comes from, by geography and taxpayer class) with the **outlay side**
(USAspending / Treasury DATA Act — where federal dollars go, by agency, program, and
recipient). Both sides key to the existing geographic spine (`geo`) and entity spine (`ref`),
so revenue and spending are analyzable against population, income, and industry in one place.

## Strategic Context

Tax and spending are **government data** a platform of this breadth is expected to have, yet
neither IRS nor USAspending is ingested today. They belong in their own `fiscal` namespace
rather than folded into `econ`: `econ` carries *macro time series* (BLS/BEA/FRED aggregates),
whereas `fiscal` carries *micro-geographic and entity-level* records (income by ZIP, awards by
recipient EIN). Keeping them separate mirrors how `sec`, `patents`, and `crime` each own a
namespace.

- **IRS Statistics of Income (SOI)** is one of the 13 Principal Federal Statistical Agencies
  not yet covered (the others: SSA, NSF/NCSES). SOI is free bulk CSV, annual, no key.
- **USAspending.gov** (Treasury Bureau of the Fiscal Service, DATA Act) publishes every federal
  award. Free REST + bulk, no key.

**Design** (project rule): one ETL table = one upstream source, **tall** (one row per
geography/entity × year). Wide/pivot reshaping is SQL views, never Java. Java code reads all
inputs from the model, never `System.getenv` (rule #7); unmapped real geographies/entities
**fail loudly**, never silently defaulted (rule #6).

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| SOI ZIP-code data | IRS (Treasury) | Bulk CSV per year × state | Free, no key | Annual |
| SOI county data | IRS (Treasury) | Bulk CSV per year | Free, no key | Annual |
| SOI migration data | IRS (Treasury) | Bulk CSV (county in/out flows) | Free, no key | Annual |
| Exempt-org 990 e-file | IRS (Treasury) | Bulk XML index + XML/AWS | Free, no key | Rolling / annual |
| Exempt-org BMF | IRS (Treasury) | CSV extract per region | Free, no key | Monthly |
| SOI historical/corporate tables | IRS (Treasury) | Bulk XLS/CSV | Free, no key | Annual |
| USAspending awards + aggregates | Treasury Fiscal Service | REST + bulk archive | Free, no key | Daily (submit windows) |
| OASDI/SSI beneficiaries & benefits | Social Security Administration | Bulk CSV + API | Free, no key | Annual / Monthly |
| 7(a) & 504 loan approvals | Small Business Administration | Bulk CSV (FOIA data) | Free, no key | Quarterly |

- IRS SOI ZIP: `https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi`
- IRS SOI county: `https://www.irs.gov/statistics/soi-tax-stats-county-data`
- IRS migration: `https://www.irs.gov/statistics/soi-tax-stats-migration-data`
- IRS 990 e-file index: `https://apps.irs.gov/pub/epostcard/990/xml/` (per-year XML index)
- IRS EO BMF: `https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf`
- USAspending API: `https://api.usaspending.gov/` — bulk archive: `https://files.usaspending.gov/`

---

## Revenue Side — IRS Statistics of Income

### `soi_income_by_zip` — delivered as `SoiZipProvider`

IRS SOI individual income tax statistics aggregated by ZIP × AGI bracket × year. The schema's
finest-grained revenue table; joins to `geo` ZIP crosswalks for county/CBSA rollups and
reconciles against `census` ACS income.

**Source:** IRS SOI ZIP-code CSV (one file per year, per state)
**Partition:** `[year]` — fan-out dimension is `state`
**Auth:** None · **Cadence:** Annual (2-year lag) · **Release window:** Months 1–4

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Tax year — partition column |
| zip_code | VARCHAR | 5-digit ZIP (FK to geo ZIP crosswalk) |
| state_fips | VARCHAR | State FIPS (FK to geo.states) |
| agi_bracket | VARCHAR | SOI AGI size class code (1–7; 0 = all returns) |
| num_returns | BIGINT | Number of returns in bracket |
| num_exemptions | BIGINT | Number of exemptions/dependents |
| adjusted_gross_income | DOUBLE | Total AGI ($1000s) |
| total_income_tax | DOUBLE | Total income tax ($1000s) |
| statistic_code | VARCHAR | SOI field code (Nxxxxx count / Axxxxx amount) |

### `soi_income_by_county` — delivered as `SoiCountyProvider`

Same SOI series at county grain (coarser, longer history than ZIP). Joins directly to
`geo.counties`.

**Source:** IRS SOI county CSV · **Partition:** `[year]` · **Auth:** None · **Cadence:** Annual

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Tax year — partition column |
| county_fips | VARCHAR | 5-digit county FIPS (FK to geo.counties) |
| state_fips | VARCHAR | State FIPS (FK to geo.states) |
| agi_bracket | VARCHAR | SOI AGI size class code |
| num_returns | BIGINT | Number of returns |
| adjusted_gross_income | DOUBLE | Total AGI ($1000s) |
| total_income_tax | DOUBLE | Total income tax ($1000s) |

### `county_migration_flows` — delivered as `SoiMigrationProvider`

IRS SOI county-to-county migration (derived from year-over-year address changes on returns).
One row per origin→destination county × year, with in- and out-flow counts. High-value dataset;
joins both endpoints to `geo.counties`.

**Source:** IRS SOI migration CSV (inflow + outflow files) · **Partition:** `[year]`
**Auth:** None · **Cadence:** Annual

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Filing year — partition column |
| origin_county_fips | VARCHAR | Origin county FIPS (FK to geo.counties) |
| dest_county_fips | VARCHAR | Destination county FIPS (FK to geo.counties) |
| num_returns | BIGINT | Returns (households) that moved |
| num_exemptions | BIGINT | Individuals (exemptions) that moved |
| agi | DOUBLE | Aggregate AGI of movers ($1000s) |

### `exempt_org_990` — delivered as `Irs990Provider`

Nonprofit/tax-exempt organization financials from IRS Form 990 e-file XML, one row per EIN ×
filing year. Joins to `ref` on EIN (a nonprofit analog to the `ref` SEC CIK bridge).

**Source:** IRS 990 e-file XML index + per-filing XML · **Partition:** `[year]`
**Auth:** None · **Cadence:** Rolling (annual filings) · **Release window:** Months 3–9

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Tax/filing year — partition column |
| ein | VARCHAR | Employer Identification Number (FK to ref entity bridge) |
| org_name | VARCHAR | Organization name |
| ntee_code | VARCHAR | NTEE classification (nonprofit sector) |
| total_revenue | DOUBLE | Total revenue |
| total_expenses | DOUBLE | Total expenses |
| total_assets | DOUBLE | End-of-year total assets |
| form_type | VARCHAR | 990 / 990-EZ / 990-PF |

### `exempt_org_master` — delivered as `IrsEoBmfProvider`

IRS Exempt Organizations Business Master File — the roster/reference of all registered
tax-exempt entities (EIN, name, location, subsection, deductibility). Slowly-changing reference
that anchors `exempt_org_990`.

**Source:** IRS EO BMF CSV (per IRS region) · **Partition:** `[year]` (snapshot year)
**Auth:** None · **Cadence:** Monthly (freshness-gate to annual snapshot)

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Snapshot year — partition column |
| ein | VARCHAR | Employer Identification Number (PK within snapshot) |
| org_name | VARCHAR | Legal name |
| state | VARCHAR | State (FK to geo.states) |
| subsection_code | VARCHAR | IRC subsection (501(c)(3) = 03, …) |
| ntee_code | VARCHAR | NTEE classification |
| ruling_date | VARCHAR | IRS ruling date (YYYYMM) |

### `soi_national` — **NOT BUILT** (no provider/transformer; absent from YAML — real gap)

SOI national + corporate historical aggregate tables (Table-1 style national totals and
corporate income tax statistics). National time series, no geographic key.

**Source:** IRS SOI historical/corporate tables (XLS/CSV) · **Partition:** `[year]`
**Auth:** None · **Cadence:** Annual

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Tax year — partition column |
| table_id | VARCHAR | SOI source table identifier |
| line_item | VARCHAR | Income/tax line label |
| filer_class | VARCHAR | Individual / corporate / partnership |
| value | DOUBLE | Amount ($1000s) |
| unit | VARCHAR | Unit / basis |

---

## Outlay Side — USAspending (Treasury DATA Act)

USAspending is **enormous** (tens of millions of award records per year). First cut ingests the
**agency/geographic aggregates**; award-level detail is a documented **Phase 2** (log the scope
so coverage is never silently truncated).

### `usaspending_by_agency` *(Phase 1)* — delivered as `UsaSpendingAgencyProvider`

Federal obligations/outlays summarized by awarding agency × fiscal year × award type. Joins to
`fedregister` agency names semantically.

**Source:** USAspending REST (`/api/v2/spending/`) · **Partition:** `[year]` (fiscal year)
**Auth:** None · **Cadence:** Daily submit windows (freshness-gate to monthly)

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Federal fiscal year — partition column |
| agency_code | VARCHAR | Awarding agency (Treasury AGENCY code) |
| agency_name | VARCHAR | Awarding agency name |
| award_type | VARCHAR | Contract / grant / loan / direct payment / other |
| obligated_amount | DOUBLE | Total obligations |
| outlayed_amount | DOUBLE | Total outlays |

### `usaspending_by_state` *(Phase 1)* — delivered as `UsaSpendingStateProvider`

Federal spending summarized by recipient state/county × fiscal year. Joins to `geo`; the direct
spatial counterpart to the SOI revenue tables.

**Source:** USAspending REST (`/api/v2/spending/` by place-of-performance) · **Partition:** `[year]`
**Auth:** None · **Cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Federal fiscal year — partition column |
| state_fips | VARCHAR | Place-of-performance state (FK to geo.states) |
| county_fips | VARCHAR | Place-of-performance county (FK to geo.counties, nullable) |
| award_type | VARCHAR | Contract / grant / loan / direct payment |
| cfda_program | VARCHAR | Assistance listing (CFDA) program, nullable |
| obligated_amount | DOUBLE | Total obligations |

### `usaspending_awards` *(Phase 2 — deferred)* — **NOT BUILT (intentional Phase 2 deferral, not a shortfall)**

Award-level detail (one row per award × modification), keyed to recipient EIN/UEI and
place-of-performance. Deferred to Phase 2 because of volume; sourced from the USAspending bulk
archive (`https://files.usaspending.gov/`) rather than paging the API. Recipient EIN joins to
the same `ref` entity bridge as `exempt_org_990`, closing the loop between who receives federal
money and who pays/reports taxes.

---

## Mandatory Outlays & Federal Credit — SSA + SBA

USAspending covers **discretionary** awards (contracts/grants). The two largest categories it
does *not* fully surface are **mandatory benefit outlays** (Social Security) and **federal
credit** (SBA lending). Adding them completes the outlay side: entitlement payments to
individuals and guaranteed loans to businesses, both geo-keyed and joinable to the revenue tables.

### `ssa_benefits_by_geography` — delivered as `SsaBenefitsProvider`

Social Security Administration — OASDI (Old-Age, Survivors, Disability Insurance) and SSI
beneficiary counts and total benefit payments by state/county × program × year. The single
largest federal mandatory-spending stream. Joins to `geo` for per-capita benefit analysis
against `census` population and directly against `soi_income_by_county` (benefit dependence vs.
income) and `usaspending_by_state` (total federal money into a county).

**Source:** SSA OASDI/SSI statistics — `https://www.ssa.gov/policy/docs/statcomps/` (bulk CSV)
**Partition:** `[year]`
**Auth:** None · **Cadence:** Annual (county/state tables) · **Release window:** Months 9–12

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Statistics year — partition key |
| program | VARCHAR | OASI / DI / SSI |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties (null for state totals) |
| beneficiary_type | VARCHAR | Retired workers / Disabled workers / Survivors / Aged / Blind & disabled |
| num_beneficiaries | BIGINT | Number of beneficiaries |
| total_benefits_usd | DOUBLE | Total annual benefits paid (USD) |
| avg_monthly_benefit_usd | DOUBLE | Average monthly benefit (USD) |

### `sba_loan_approvals` — delivered as `SbaLoansProvider`

Small Business Administration — 7(a) and 504 loan approvals (FOIA public data), one row per
approved loan. Federal **credit** activity by lender, borrower geography, industry, and year.
Joins to `geo` for regional small-business lending, to `ref` on borrower name/EIN, and (Phase 2)
to `usaspending_awards` for the full picture of federal money reaching a business.

**Source:** SBA FOIA loan data — `https://data.sba.gov/dataset/` (7(a) + 504 bulk CSV)
**Partition:** `[type, year]` — `type` = program (`7a` / `504`), fan-out on `year`
**Auth:** None · **Cadence:** Quarterly · **Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Approval fiscal year — partition key |
| program | VARCHAR | 7(a) / 504 |
| borrower_name | VARCHAR | Borrower business name (join to ref entity bridge) |
| borrower_ein | VARCHAR | Borrower EIN when present (FK to ref entity bridge) |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties (nullable) |
| naics_code | VARCHAR | Borrower NAICS industry |
| lender_name | VARCHAR | Approving lender |
| gross_approval_usd | DOUBLE | Gross approved amount |
| sba_guaranteed_usd | DOUBLE | SBA-guaranteed portion |
| jobs_supported | INTEGER | Reported jobs supported |
| loan_status | VARCHAR | Approved / Disbursed / Paid in full / Charged off |

**Materialize (both):** `format: iceberg`, `overwritePartitions: true` — SSA restates prior years
and SBA reissues the FOIA extract each quarter. New `SsaBenefitsTransformer` and
`SbaLoansTransformer`; each maps one upstream source to one tall table. Neither needs a key. Fail
loudly (rule #6) on an unmapped real county/EIN; never default to a placeholder.

---

## Join Architecture

```
geo.states / geo.counties (FIPS)
    ├── soi_income_by_zip / soi_income_by_county (year)      revenue by geography
    ├── county_migration_flows (origin + dest county, year)  household movement
    ├── exempt_org_master.state (year)                       nonprofit roster
    ├── usaspending_by_state / by_agency (year)              outlays by geography
    ├── ssa_benefits_by_geography (state/county, year)       mandatory benefit outlays
    │       └── soi_income_by_county (benefit dependence vs. income, same county×year)
    └── sba_loan_approvals (state/county, year)              federal small-business credit

ref entity bridge (EIN)
    ├── exempt_org_master (ein)  ──<  exempt_org_990 (ein, year)
    ├── sba_loan_approvals.borrower_ein (year)               business credit recipient
    └── usaspending_awards.recipient_ein (Phase 2)           revenue⇄spending loop

census (ACS income) ── semantic reconcile ── soi_income_by_county
fedregister.agencies ── semantic ── usaspending_by_agency
```

- SOI + USAspending share `geo` as the spatial spine → revenue and spending are directly
  comparable per county/state.
- `exempt_org_990` and (Phase 2) `usaspending_awards` share the `ref` EIN spine → the same
  entity can be seen as taxpayer, nonprofit filer, and award recipient.

---

## Environment Variables

Per rule #7, every runtime input is declared in the schema YAML as `${VAR:default}` and read
from the model — **never** `System.getenv` in Java. Neither IRS bulk nor USAspending requires a
key, so **no new secret** is introduced.

| Variable | Required | Description |
|---|---|---|
| `SCHEMA_NAME` | (existing) | Schema name (default `fiscal`) |
| `GOVDATA_PARQUET_DIR` | (global) | Output path |
| `GOVDATA_START_YEAR` / `GOVDATA_END_YEAR` | (global) | Year window |
| `GOVDATA_CACHE_DIR` | (global) | Raw download cache |

---

## Implementation Notes

- **Bulk over API.** IRS SOI and the USAspending archive are bulk files carrying full history far
  more cheaply than paging; stream them in a `dataProvider`/transformer hook (same pattern as
  the USDA `ErsFarmIncome` / `RmaSummaryOfBusiness` providers). Use the USAspending REST API only
  for current-year aggregate deltas.
- **New transformers (Java 8):** `SoiZipTransformer`, `SoiCountyTransformer`,
  `SoiMigrationTransformer`, `Irs990Transformer`, `IrsEoBmfTransformer`, `SoiNationalTransformer`,
  `UsaSpendingAgencyTransformer`, `UsaSpendingStateTransformer`. Each maps one upstream source to
  one tall table. — *Delivered as `*Provider` classes (`SoiZipProvider`, `SoiCountyProvider`,
  `SoiMigrationProvider`, `Irs990Provider`, `IrsEoBmfProvider`, `UsaSpendingAgencyProvider`,
  `UsaSpendingStateProvider`, plus `SsaBenefitsProvider` / `SbaLoansProvider`); `SoiNationalTransformer`/`SoiNationalProvider` **NOT BUILT**.*
- **Geographic keys.** SOI ZIP files carry ZIP + state; map ZIP→county through the existing
  `geo` ZIP crosswalk (HUD USER), don't invent a new one. **Fail loudly** (rule #6) on an
  unmapped real ZIP/county; never default to a placeholder FIPS.
- **EIN spine.** `exempt_org_990` and `exempt_org_master` both key on EIN; register the EIN
  bridge in `ref` so nonprofit filings and (Phase 2) award recipients share one entity key —
  mirror the existing SEC CIK bridge.
- **Overwrite semantics.** IRS republishes full history per release and USAspending restates
  prior fiscal years → `materialize: format: iceberg`, partition `[year]`,
  `overwritePartitions: true` (same pattern as `econ.world_indicators` / `ag.faostat_production`).
- **AGI-bracket totals.** SOI files include an "all returns" bracket (`agi_bracket = 0`) that
  double-counts the size classes — keep it but document it; views that sum brackets must exclude
  `0` to avoid double counting.
- **USAspending phasing.** Ship Phase 1 aggregates first; `log()` that award-level detail is
  deferred so coverage is never silently truncated. Phase 2 (`usaspending_awards`) reads the bulk
  archive, not the API.
- **Verification gate.** Not "done" until an ETL/DQ run + `verify-tables` passes end-to-end
  through the Calcite→DuckDB read path.
