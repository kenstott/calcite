# Agriculture Schema Data Plan

## Status: DELIVERED

The `ag` schema is **implemented and wired into the ETL pool**. All five planned tables
ship in [ag-schema.yaml](../../../src/main/resources/ag/ag-schema.yaml) with providers/
transformers under
[org.apache.calcite.adapter.govdata.ag](../../../src/main/java/org/apache/calcite/adapter/govdata/ag/).

| Table | Source | Provider / Transformer | Layout |
|---|---|---|---|
| `nass_crop_production` | NASS QuickStats REST | `NassQuickStatsTransformer` | Tall (one row per commodity × data-item × geo × year) |
| `nass_livestock_inventory` | NASS QuickStats REST | `NassQuickStatsTransformer` | Tall |
| `rma_crop_insurance` | RMA Summary of Business (pipe-delimited zip) | `RmaSummaryOfBusinessTransformer` | Tall |
| `ers_farm_income` | ERS Farm Income & Wealth (scraped CSV) | `ErsFarmIncomeProvider` | Tall |
| `fsa_commodity_payments` | FSA EFOIA payment files (xlsx) | `FsaCommodityPaymentsProvider` | Aggregated (county × program × program-year) |

**Delivery deviations from the original proposal** (kept here so the plan matches reality):
- **Tall, source-mirroring layout** replaced the proposed wide/pivot column sets. Each ETL
  table mirrors exactly one upstream source; wide reshaping and cross-source joins are done
  as SQL views, never in Java.
- `ers_farm_income` keys on the state **USPS abbreviation** (`state`), not `state_fips`, and
  is a **single cumulative fetch** (no year fan-out) partitioned by row year.
- `fsa_commodity_payments` has **no commodity dimension** (program is the finest grain);
  `farms` is a distinct-payee proxy (FSA files carry no operation id); `year` is the
  disbursement vintage and differs from `program_year`.
- Source formats differ from "CSV bulk": RMA is a pipe-delimited `sobcov_{year}.zip`; FSA is
  scraped EFOIA **xlsx** read via POI.
- Backfill start defaults to **`GOVDATA_START_YEAR` (2010)**, not 1997.

---

## Strategic Context

USDA publishes the most granular county-level economic data available for agricultural
analysis. The `ag` schema's primary cross-schema value is **rural economic and commodity
market research**: crop production by county joins to DISASTERS drought monitor and storm
events for yield-damage correlation; farm income joins to CENSUS rural demographics for
economic distress analysis; crop insurance joins to SEC filings for agricultural commodity
companies and crop input manufacturers.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| Crop production (QuickStats) | USDA NASS | REST API (JSON) | Free, API key required | Annual (+ mid-season) |
| Livestock inventory (QuickStats) | USDA NASS | REST API (JSON) | Free, API key required | Annual |
| Farm income and wealth | USDA ERS | Scraped landing page → latin-1 CSV in release.zip | Free, no key | Annual |
| Crop insurance summary of business | USDA RMA | Pipe-delimited `sobcov_{year}.zip` | Free, no key | Annual |
| Program / conservation payments | USDA FSA | Scraped EFOIA name-and-address xlsx | Free, no key | Annual |

- NASS QuickStats API: `https://quickstats.nass.usda.gov/api/api_GET/` (API key: free, register at nass.usda.gov)
- USDA ERS: `https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/` (landing page scraped for the rotating `/media/.../release.zip`)
- USDA RMA: `https://pubfs-rma.fpac.usda.gov/pub/Web_Data_Files/Summary_of_Business/state_county_crop/sobcov_{year}.zip`
- USDA FSA: `https://www.fsa.usda.gov/tools/informational/freedom-information-act-foia/electronic-reading-room/frequently-requested/payment-files`

---

## Delivered Tables

### `nass_crop_production`

USDA NASS QuickStats field-crop statistics by county and state. **Tall layout** — one row
per commodity × data-item × geography × year, with `statisticcat_desc` (PRODUCTION, AREA
HARVESTED, AREA PLANTED, YIELD) and `unit_desc` (BU, LB, TONS, ...) selecting the measure
and unit; `value` holds the number (null when NASS suppressed it, see `value_flag`). Joins
to `geo.counties` via `state_fips_code` + `county_code`, and to disasters drought/storm
tables by county + year. Also joins to `econ.fred_indicators` commodity price series for
revenue estimation.

**Source:** NASS QuickStats — `sector_desc=CROPS`, `group_desc=FIELD CROPS`
**Partition:** `type`, `year` (Hive)
**Fan-out:** `agg_level_desc` (COUNTY, STATE) × `state_alpha` (all states) × year — keeps each
request under the QuickStats 50,000-row cap
**Auth:** `AGR_NASS_API_KEY` (free, register at nass.usda.gov)
**Cadence:** Annual, `dataLag=1` (principal crops published September; full survey December)
**Materialize:** Iceberg (hadoop catalog), incremental by `year`

Emits the shared canonical `nass_columns` (see `nass_livestock_inventory`). PK:
`(type, year, source_desc, agg_level_desc, state_fips_code, county_code, short_desc,
domaincat_desc, reference_period_desc)`.

---

### `nass_livestock_inventory`

USDA NASS QuickStats livestock and poultry inventory (head counts) by county and state —
cattle, hogs, sheep, chickens, turkeys, etc. Same tall `nass_columns` shape as crop
production; filter `commodity_desc` + `class_desc` for a specific animal class, `unit_desc`
= HEAD. Joins to `geo.counties` and to census rural demographics.

**Source:** NASS QuickStats — `sector_desc=ANIMALS & PRODUCTS`, `statisticcat_desc=INVENTORY`
**Partition:** `type`, `year` (Hive)
**Fan-out:** `agg_level_desc` × `state_alpha` × year
**Auth:** `AGR_NASS_API_KEY`
**Cadence:** Annual, `dataLag=1`

Canonical `nass_columns` (shared by both NASS tables):

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Data year (partition) |
| source_desc | VARCHAR | Program source: CENSUS (5-yearly, county-dense) or SURVEY (annual) |
| sector_desc | VARCHAR | CROPS or ANIMALS & PRODUCTS |
| group_desc | VARCHAR | Commodity group (FIELD CROPS, LIVESTOCK, POULTRY, ...) |
| commodity_desc | VARCHAR | CORN, SOYBEANS, CATTLE, HOGS, ... |
| class_desc | VARCHAR | Commodity class (ALL CLASSES, GRAIN, ...) |
| prodn_practice_desc | VARCHAR | Production practice (ALL, IRRIGATED, ...) |
| util_practice_desc | VARCHAR | Utilization practice (GRAIN, SILAGE, ...) |
| statisticcat_desc | VARCHAR | PRODUCTION, AREA HARVESTED, AREA PLANTED, YIELD, INVENTORY |
| domain_desc / domaincat_desc | VARCHAR | Domain and domain category |
| short_desc | VARCHAR | Full data-item description |
| unit_desc | VARCHAR | Unit of value (BU, LB, TONS, HEAD, ACRES, ...) |
| agg_level_desc | VARCHAR | COUNTY or STATE |
| state_alpha / state_fips_code / state_ansi / state_name | VARCHAR | State identifiers (FK `state_fips_code` → geo.states) |
| asd_code / asd_desc | VARCHAR | Agricultural statistics district |
| county_ansi / county_code / county_fips / county_name | VARCHAR | County identifiers (FK `county_fips` → geo.counties) |
| value | DOUBLE | Numeric statistic value (null when suppressed) |
| value_flag | VARCHAR | Suppression marker: (D) disclosure, (Z) rounds to zero, (X), (NA) |
| cv_pct | DOUBLE | Coefficient of variation (%) when published |
| freq_desc / reference_period_desc | VARCHAR | Frequency and reference period |
| load_time | VARCHAR | NASS record load timestamp |

---

### `rma_crop_insurance`

USDA RMA Summary of Business — crop insurance experience by state, county, crop, insurance
plan, coverage category, delivery type, and coverage level. Policies, acres, liability,
premium, subsidy, indemnity, and loss ratio. One bulk pipe-delimited `sobcov_{year}.zip`
per year (stable URL, 1989+), streamed by `RmaSummaryOfBusinessTransformer` (no per-request
cap; ~150k rows/yr). Joins to `geo.counties` via `county_fips` and to disasters storm/drought
tables by county + year for loss attribution.

**Source:** `https://pubfs-rma.fpac.usda.gov/.../sobcov_{year}.zip`
**Partition:** `type`, `year` (Hive) · **Auth:** none · **Cadence:** Annual, `dataLag=1`

| Column | Type | Description |
|---|---|---|
| year | INT | Commodity (insurance) year — partition |
| state_fips / state_abbr | VARCHAR | State FIPS (FK → geo.states) / USPS abbr |
| county_code / county_fips / county_name | VARCHAR | County (FK `county_fips` → geo.counties) |
| commodity_code / commodity_name | VARCHAR | RMA commodity code / insured crop |
| insurance_plan_code / insurance_plan_abbr | VARCHAR | Plan code / abbr (YP, APH, RP, ...) |
| coverage_category | VARCHAR | A=Buyup, C=CAT, E=Existing, L=Limited |
| delivery_type | VARCHAR | RCAT/RBUP/FCAT/FBUP |
| coverage_level | DOUBLE | Coverage level as decimal fraction (e.g. 0.65) |
| policies_sold_count / policies_earning_prem_count / policies_indemnified_count | LONG | Policy counts |
| units_earning_prem_count / units_indemnified_count | LONG | Insured-unit counts |
| quantity_type / net_reported_quantity | VARCHAR / DOUBLE | Net reporting level and its unit |
| endorsed_companion_acres | DOUBLE | Endorsed/companion acres |
| liability_amount / total_premium_amount | DOUBLE | Liability / total premium (USD) |
| subsidy_amount / state_private_subsidy / additional_subsidy / efa_premium_discount | DOUBLE | Subsidy components (USD) |
| indemnity_amount | DOUBLE | Indemnity paid (USD; signed) |
| loss_ratio | DOUBLE | Indemnity / total premium |

PK: `(year, state_fips, county_fips, commodity_code, insurance_plan_code, coverage_category,
delivery_type, coverage_level)`.

---

### `ers_farm_income`

USDA ERS Farm Income and Wealth Statistics by state and US total: cash receipts, government
payments, production expenses, net farm income, and balance-sheet items. **Tall** — one row
per year × state × variable. Values are generally thousands of USD (see `unit_desc`; some
rows are Percent/Ratio/per farm). The `source.url` is the **landing page** —
`ErsFarmIncomeProvider` scrapes it for the current rotating `/media/.../release.zip`, then
streams the latin-1 CSV. Single cumulative fetch (no year fan-out); partitioned by row year.
Joins to `geo.states` via the state **USPS abbreviation** (`state`), not FIPS.

**Source:** ERS Farm Income & Wealth landing page (scraped) · **Auth:** none · **Cadence:** Annual (most recent 1–2 years are ERS forecasts)

| Column | Type | Description |
|---|---|---|
| year | INT | Data year (partition); recent 1–2 years are forecasts |
| state | VARCHAR | USPS state abbr, or 'US' national total (FK → geo.states.state_abbr) |
| artificial_key | VARCHAR | ERS stable series code embedding state + variable (PK component) |
| variable_description_total | VARCHAR | Full variable label (category + subcategory) |
| category / subcategory | VARCHAR | VariableDescriptionPart1 / Part2 |
| amount | DOUBLE | Value in `unit_desc` units (usually $1,000; can be negative) |
| unit_desc | VARCHAR | $1,000 (dominant), Percent, Ratio, $1,000 per farm, 1,000 acres |
| publication_date | VARCHAR | ERS release stamp |
| source | VARCHAR | ERS attribution string |
| gdp_deflator | DOUBLE | GDP chain-type deflator for the row's year (nominal→real) |

PK: `(type, year, artificial_key)`. FK: `state` → geo.states.state_abbr (excludes 'US').

---

### `fsa_commodity_payments`

USDA FSA program and conservation payments **aggregated by county, program, and program
year** (ARC-CO, PLC, CRP, LFP, ERP, DMC, etc.). `payments` is the summed disbursement (USD);
`farms` is a distinct-payee count (proxy — FSA files carry no farm/operation id). **No
commodity dimension** (program is the finest grain). `year` is the disbursement (vintage)
year; `program_year` is the crop/program year and differs. `FsaCommodityPaymentsProvider`
scrapes the EFOIA landing page for the vintage, resolves node pages to xlsx, reads via POI,
and aggregates per county × program × program-year. One fetch per disbursement year. Joins
to `geo.counties` via `county_fips`; joins to census rural income for farm-program
dependency analysis.

**Source:** FSA EFOIA payment files (scraped xlsx) · **Auth:** none · **Cadence:** Annual, `dataLag=1`

| Column | Type | Description |
|---|---|---|
| year | INT | Disbursement (vintage) year — partition |
| program_year | VARCHAR | Crop/program year the payment applies to (differs from disbursement year) |
| state_fips | VARCHAR | 2-digit state FIPS (FK → geo.states) |
| county_fips | VARCHAR | 5-digit county FIPS (FK → geo.counties) |
| program_code / program_description | VARCHAR | FSA accounting program code / description |
| payments | DOUBLE | Summed disbursement (USD) for county/program/program-year |
| farms | INT | Distinct payee count (proxy for number of farms/operations) |

PK: `(type, year, program_year, county_fips, program_code)`.

---

## Join Architecture

```
geo.counties (state_fips_code + county_code / county_fips)
    ├── nass_crop_production (county + year)
    │       ├── disasters.storm_events (county + year → yield damage)
    │       ├── disasters.drought_monitor_weekly (county + year → drought yield loss)
    │       └── econ.fred_indicators (commodity price series → revenue estimate; semantic, by name)
    ├── nass_livestock_inventory (county + year)
    │       └── census rural demographics (county → ag employment context)
    ├── rma_crop_insurance (county_fips + year)
    │       └── disasters.storm_events (county + year → loss attribution)
    └── fsa_commodity_payments (county_fips + year)
            └── census.acs_income (county → farm program income share)

geo.states (state_fips / state_abbr)
    └── ers_farm_income (state_abbr + year)
            └── econ.fred_indicators (net farm income series cross-check; semantic)
```

Intra-schema FKs: none (each table mirrors a distinct USDA source with no shared parent/child
dimension). All cross-table linkage is via the shared geo spatial keys, declared as
inter-schema FKs in the YAML `constraints` block.

---

## Worker Assignment (Pool)

Wired into the ETL pool via [worker.sh](../../../scripts/parallel/worker.sh) and
[run-pool.sh](../../../scripts/parallel/run-pool.sh):

| Slot | Mode | Behavior |
|---|---|---|
| `ag:historical` | historical | `GOVDATA_START_YEAR=2010` (default) → `INCREMENTAL_YEAR-1`. Runs as a **single historical slot** (not year-sliced): ERS is one cumulative fetch; NASS/RMA/FSA backfill their full year range in one pass. |
| `ag:daily` | daily | `GOVDATA_START_YEAR=INCREMENTAL_YEAR`, open-ended end — incremental current-year refresh. |

Also accepts an explicit year (`ag 2025`) or range (`ag 2020-2023`). NASS digital record
starts ~1997; default backfill floor is 2010.

DQ status: `ag` is in the 17-schema DQ set but **PENDING until its first ETL run** populates R2.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `AGR_NASS_API_KEY` | Required | NASS QuickStats API key (free at nass.usda.gov) — the only per-source secret; RMA/ERS/FSA need no key |
| `SCHEMA_NAME` | Optional | Schema name (default `ag`) |
| `GOVDATA_PARQUET_DIR` | Required | Materialize/output root (`materializeDirectory`) |
| `GOVDATA_START_YEAR` | Optional | Backfill start year (default `2010`) |

Per the GovData Schema Rules, all env vars are declared only in
[ag-schema.yaml](../../../src/main/resources/ag/ag-schema.yaml); Java reads them from the
model, never via `System.getenv`.
