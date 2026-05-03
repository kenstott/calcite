# Agriculture Schema Data Plan

## Strategic Context

USDA publishes the most granular county-level economic data available for agricultural
analysis. The `agr` schema's primary cross-schema value is **rural economic and commodity
market research**: crop production by county joins to DISASTERS drought monitor and storm
events for yield-damage correlation; farm income joins to CENSUS rural demographics for
economic distress analysis; crop insurance joins to SEC filings for agricultural commodity
companies and crop input manufacturers.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| Crop production (QuickStats) | USDA NASS | REST API | Free, API key required | Annual (+ mid-season) |
| Livestock inventory (QuickStats) | USDA NASS | REST API | Free, API key required | Annual |
| Farm income and expenses | USDA ERS | CSV bulk download | Free, no key | Annual |
| Crop insurance summary | USDA RMA | CSV bulk download | Free, no key | Annual |
| Commodity program payments | USDA FSA | CSV bulk download | Free, no key | Annual |

NASS QuickStats API: `https://quickstats.nass.usda.gov/api/` (API key: free, register at nass.usda.gov)
USDA ERS data: `https://www.ers.usda.gov/data-products/`
USDA RMA: `https://www.rma.usda.gov/Information-Tools/Summary-of-Business`
USDA FSA: `https://www.fsa.usda.gov/news-room/efoia/`

---

## Proposed Tables

### `nass_crop_production`

Annual crop production by county — acres planted, acres harvested, yield, and production
volume. The foundational agricultural dataset; joins to DISASTERS drought and storm tables
for yield-impact analysis; joins to `econ.fred_indicators` commodity price series for
revenue estimation.

**Source:** USDA NASS QuickStats API — `sector=CROPS&group=FIELD CROPS`
**Partition:** `crop_year`
**Auth:** NASS API key (free, register at nass.usda.gov)
**Cadence:** Annual (principal crops published September; full survey December)
**Release window:** Months 9–12

| Column | Type | Description |
|---|---|---|
| crop_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties (null for state-level aggregates) |
| commodity | VARCHAR | Corn / Soybeans / Wheat / Cotton / Sorghum / etc. |
| data_item | VARCHAR | NASS data item description |
| domain | VARCHAR | Total / Irrigated / Non-Irrigated |
| acres_planted | DOUBLE | Acres planted (null if not applicable) |
| acres_harvested | DOUBLE | Acres harvested for grain/seed |
| yield_per_acre | DOUBLE | Yield (bushels, lbs, bales, etc. per acre) |
| production | DOUBLE | Total production (bushels, lbs, bales, etc.) |
| production_unit | VARCHAR | BU / LB / BALES / TONS / etc. |
| value_million | DOUBLE | Value of production (USD millions; state-level only) |

---

### `nass_livestock_inventory`

Annual livestock and poultry inventory by state and county. Joins to CENSUS rural
demographics for agricultural employment context; joins to `econ.bls_employment` for
slaughter/processing employment correlation.

**Source:** USDA NASS QuickStats API — `sector=ANIMALS & PRODUCTS`
**Partition:** `inventory_year`
**Auth:** NASS API key
**Cadence:** Annual (January inventory report)
**Release window:** Month 1

| Column | Type | Description |
|---|---|---|
| inventory_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties (null for state-level) |
| commodity | VARCHAR | Cattle / Hogs / Chickens / Turkeys / Sheep / etc. |
| inventory_class | VARCHAR | NASS category (e.g., "CATTLE, COWS, MILK") |
| head_count | DOUBLE | Inventory count (head/number) |
| value_million | DOUBLE | Inventory value (USD millions; state-level only) |

---

### `ers_farm_income`

USDA Economic Research Service annual farm income and expenses by state. Macro-level
signal for agricultural sector profitability; joins to `econ.fred_indicators` for
net farm income vs. commodity price correlation.

**Source:** USDA ERS Farm Income and Wealth Statistics — bulk CSV
**Partition:** `income_year`
**Auth:** None
**Cadence:** Annual (prior year published ~February)
**Release window:** Months 2–4

| Column | Type | Description |
|---|---|---|
| income_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states (null for US total) |
| category | VARCHAR | Cash receipts / Government payments / Farm expenses / Net farm income |
| subcategory | VARCHAR | Commodity or expense type |
| value_million | DOUBLE | Value (USD millions) |

---

### `rma_crop_insurance`

USDA Risk Management Agency Summary of Business — crop insurance policies, premiums,
indemnities, and loss ratios by county. Key signal for agricultural disaster severity;
joins to `disasters.drought_monitor_weekly` and `disasters.storm_events` for loss
attribution; joins to SEC filings for crop input and agribusiness companies.

**Source:** USDA RMA Summary of Business bulk CSV
**Partition:** `insurance_year`
**Auth:** None
**Cadence:** Annual (prior year finalized ~July)
**Release window:** Months 7–9

| Column | Type | Description |
|---|---|---|
| insurance_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| commodity | VARCHAR | Insured crop |
| insurance_plan | VARCHAR | Yield Protection / Revenue Protection / etc. |
| policies_sold | INTEGER | Number of policies |
| acres_insured | DOUBLE | Acres covered |
| liability | DOUBLE | Total liability (USD) |
| total_premium | DOUBLE | Total premium (USD) |
| subsidy | DOUBLE | Government subsidy (USD) |
| indemnity | DOUBLE | Indemnity paid (USD) |
| loss_ratio | DOUBLE | Indemnity / premium ratio |

---

### `fsa_commodity_payments`

USDA Farm Service Agency commodity program and conservation payments by county.
Quantifies direct federal agricultural support by geography; joins to CENSUS rural
income data to assess farm program income dependency.

**Source:** USDA FSA EFOIA direct payment data — bulk CSV
**Partition:** `payment_year`
**Auth:** None
**Cadence:** Annual (prior year published ~March)
**Release window:** Months 3–5

| Column | Type | Description |
|---|---|---|
| payment_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| program | VARCHAR | ARC-CO / PLC / CRP / EQIP / CSP / etc. |
| commodity | VARCHAR | Commodity or conservation practice covered |
| payments | DOUBLE | Total payments (USD) |
| farms | INTEGER | Number of farms receiving payments |

---

## Join Architecture

```
geo.counties (county_fips)
    ├── nass_crop_production (county_fips, crop_year)
    │       ├── disasters.storm_events (county_fips + year → yield damage)
    │       ├── disasters.drought_monitor_weekly (county_fips + year → drought yield loss)
    │       └── econ.fred_indicators (commodity price series → revenue estimate)
    ├── nass_livestock_inventory (county_fips, inventory_year)
    │       └── econ.bls_employment (county_fips → meat processing employment)
    ├── rma_crop_insurance (county_fips, insurance_year)
    │       └── disasters.storm_events (county_fips + year → loss attribution)
    └── fsa_commodity_payments (county_fips, payment_year)
            └── census.acs_income (county_fips → farm program income share)

geo.states (state_fips)
    └── ers_farm_income (state_fips, income_year)
            └── econ.fred_indicators (net farm income series cross-check)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 84 | initial | All 5 tables, full history (1997+) | 4 GB / 6 GB | Once |
| 85 | annual | All 5 tables, incremental | 2 GB / 3 GB | Annual |

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `nass_crop_production` | Months 9–12 | NASS crop production reports |
| `nass_livestock_inventory` | Month 1 | NASS January livestock inventory |
| `ers_farm_income` | Months 2–4 | ERS annual farm income release |
| `rma_crop_insurance` | Months 7–9 | RMA prior-year finalized data |
| `fsa_commodity_payments` | Months 3–5 | FSA prior-year payment data |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `AGR_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=agr`) |
| `AGR_CACHE_DIR` | Recommended | Raw download cache |
| `AGR_NASS_API_KEY` | Required | NASS QuickStats API key (free at nass.usda.gov) |
| `AGR_SINCE_YEAR` | Optional | Incremental start year (default: 1997 — NASS digital record start) |
