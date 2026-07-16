# Housing Schema Data Plan

## Strategic Context

Housing data spans three federal sources with complementary coverage: CFPB HMDA provides
loan-level mortgage origination data by census tract; HUD publishes fair market rents
and subsidized housing inventory; Census Bureau tracks building permits and housing cost
data. The `housing` schema's primary cross-schema value is **real estate and mortgage
market analysis**: HMDA originations join to SEC 10-K filings for banks and mortgage
REITs; fair market rents join to CENSUS income data for affordability analysis;
building permits join to ECON employment for construction sector leading-indicator work.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| HMDA loan originations | CFPB / FFIEC | REST API + bulk CSV | Free, no key | Annual |
| HUD Fair Market Rents | HUD | CSV bulk download | Free, no key | Annual |
| Census Building Permits (SOC) | Census Bureau | REST API | Free, no key | Monthly |
| HUD Picture of Subsidized Housing | HUD | CSV bulk download | Free, no key | Annual |
| HUD Opportunity Zones | HUD / Treasury | CSV bulk download | Free, no key | Static |

CFPB HMDA API: `https://ffiec.cfpb.gov/api/public/`
HUD User: `https://www.huduser.gov/portal/datasets/`
Census Building Permits: `https://www.census.gov/construction/bps/`

---

## Implementation Status (2026-07-16)

All planned datasets are now authored and compile. Two deviations from the
original loan-level / county-summary design were made for tractability, each
using a real public API instead of bulk ZIP microdata:

| Table | Built as | Deviation from plan |
|---|---|---|
| `house_price_index` | FHFA HPI master (extra, beyond plan) | added |
| `building_permits` | Census BPS **county-annual** | plan said monthly |
| `fair_market_rents` | HUD FMR (HUD_TOKEN) | as planned |
| `income_limits`, `income_limits_county` | HUD IL (HUD_TOKEN, extra) | added |
| `hmda_loans` | CFPB **Data Browser aggregations**, per `(year, state)`, broken out by `action_taken` × `loan_purpose` | not loan-level; the aggregations API allows only 2 breakdown dims |
| `hmda_applicant_demographics` | CFPB aggregations, per `(year, state)`, long-format `action_taken` × {`race`,`ethnicity`,`sex`} (3 fetches/unit) | recovers the fair-lending cut the 2-dim cap kept out of `hmda_loans` |
| `hud_subsidized_housing` | HUD Open Data **ArcGIS** "Picture of Subsidized Households" — Dec-2020 all-programs **tract** snapshot (63,584 tracts) | tract-grain all-programs summary (units/people/income/pct-disabled) |
| `hud_subsidized_county` | HUD USER annual **county workbook** (`COUNTY_<yr>_2020census.xlsx`, 2022+), county × HUD program, via POI `dataProvider` | the full plan realization: county grain, per-program, with pct minority / female-headed / elderly / disabled, income, tenant rent, and HUD subsidy |
| `opportunity_zones` | HUD Open Data **ArcGIS** `Opportunity_Zones` FeatureServer (~8,764 OZ 1.0 tracts) | as planned (static) |

`hmda_loans`, `opportunity_zones`, and `hud_subsidized_housing` need **no
secret** (public CFPB / ArcGIS endpoints); only the three HUD USER tables are
`HUD_TOKEN`-gated. Live ingest / DQ / verify still pending.

---

## Proposed Tables

### `hmda_loans`

Home Mortgage Disclosure Act — loan originations, applications, and denials by census
tract. One row per loan application. Largest fair-lending dataset in the U.S. government.
Joins to `sec.filing_metadata` via lender name for mortgage bank/REIT analysis; joins
to CENSUS demographics for CRA compliance and fair-lending research.

**Source:** CFPB HMDA API — `https://ffiec.cfpb.gov/api/public/lar/flat/`
**Partition:** `activity_year`
**Auth:** None
**Cadence:** Annual (prior year published ~June)
**Release window:** Months 6–8

| Column | Type | Description |
|---|---|---|
| activity_year | INTEGER | Partition key |
| lei | VARCHAR | Lender Legal Entity Identifier — FK to ref.lei_entities |
| respondent_name | VARCHAR | Lender name |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| census_tract | VARCHAR | 11-digit census tract FIPS — FK to geo.census_tracts |
| action_type | VARCHAR | Originated / Approved not accepted / Denied / Withdrawn / etc. |
| loan_type | VARCHAR | Conventional / FHA / VA / USDA Rural Housing |
| loan_purpose | VARCHAR | Home purchase / Home improvement / Refinancing / Cash-out refinancing |
| lien_status | VARCHAR | First lien / Subordinate lien |
| loan_amount | DOUBLE | Loan amount (USD thousands) |
| property_value | DOUBLE | Property value (USD thousands) |
| ltv_ratio | DOUBLE | Loan-to-value ratio |
| interest_rate | DOUBLE | Note interest rate |
| rate_spread | DOUBLE | Spread over APOR (basis points; null if < threshold) |
| debt_to_income | VARCHAR | DTI ratio band |
| applicant_race | VARCHAR | Race category (anonymized per HMDA) |
| applicant_ethnicity | VARCHAR | Hispanic or Latino / Not Hispanic / etc. |
| applicant_sex | VARCHAR | Male / Female / Joint / Not provided |
| applicant_income | DOUBLE | Gross annual income (USD thousands) |
| denial_reason | VARCHAR | Denial reason code (null if not denied) |

---

### `hud_fair_market_rents`

HUD annual fair market rent (FMR) estimates by county and bedroom size. FMRs set
Section 8 voucher payment standards — used by HUD, landlords, and researchers to
benchmark rental affordability. Joins to `census.acs_housing` for market vs. actual
rent comparison; joins to DISASTERS for post-disaster rental market impact.

**Source:** HUD FMR dataset — `https://www.huduser.gov/portal/datasets/fmr.html`
**Partition:** `fmr_year`
**Auth:** None
**Cadence:** Annual (new FMRs published ~October, effective the following year)
**Release window:** Month 10

| Column | Type | Description |
|---|---|---|
| fmr_year | INTEGER | Partition key (fiscal year effective) |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| metro_area | VARCHAR | HUD metro area name (null if non-metro) |
| fmr_0br | DOUBLE | FMR for efficiency (studio) unit (USD/month) |
| fmr_1br | DOUBLE | FMR for 1-bedroom unit |
| fmr_2br | DOUBLE | FMR for 2-bedroom unit |
| fmr_3br | DOUBLE | FMR for 3-bedroom unit |
| fmr_4br | DOUBLE | FMR for 4-bedroom unit |
| median_rent_2br | DOUBLE | Unadjusted median gross rent for 2BR (Census basis) |
| fmr_percentile | VARCHAR | 40th / 50th percentile designation |

---

### `census_building_permits`

U.S. Census Bureau Survey of Construction monthly building permits by county. Leading
economic indicator for construction employment and real estate supply. Joins to
`econ.bls_employment` for construction workforce correlation; joins to `census.acs_housing`
for supply-vs-demand analysis.

**Source:** Census Building Permits Survey — `https://www.census.gov/construction/bps/`
**Partition:** `permit_year`
**Auth:** None (Census API key recommended for stability)
**Cadence:** Monthly (prior month published ~end of following month)
**Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| permit_year | INTEGER | Partition key |
| permit_month | INTEGER | Month (1–12) |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| units_1 | INTEGER | Permits for 1-unit structures |
| units_2 | INTEGER | Permits for 2-unit structures |
| units_3_4 | INTEGER | Permits for 3–4 unit structures |
| units_5_plus | INTEGER | Permits for 5+ unit structures |
| total_units | INTEGER | Total permitted units |
| bldgs_1 | INTEGER | Buildings for 1-unit permits |
| value_1_million | DOUBLE | Valuation for 1-unit permits (USD millions) |
| value_total_million | DOUBLE | Total permit valuation (USD millions) |

---

### `hud_subsidized_housing`

HUD Picture of Subsidized Households — annual count of subsidized housing units and
assisted households by county. Covers Section 8 vouchers, public housing, and
project-based assistance. Joins to CENSUS demographics for housing assistance
dependency analysis.

**Source:** HUD Picture of Subsidized Housing — `https://www.huduser.gov/portal/datasets/assthsg.html`
**Partition:** `report_year`
**Auth:** None
**Cadence:** Annual
**Release window:** Months 6–9

| Column | Type | Description |
|---|---|---|
| report_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| program | VARCHAR | Housing Choice Voucher / Public Housing / Project-Based Section 8 / etc. |
| total_units | INTEGER | Total units under program |
| occupied_units | INTEGER | Occupied assisted units |
| pct_occupied | DOUBLE | Occupancy rate |
| avg_hh_income | DOUBLE | Average household annual income (USD) |
| avg_federal_rent_subsidy | DOUBLE | Average monthly federal rental subsidy (USD) |
| pct_minority | DOUBLE | Percent minority households |
| pct_female_headed | DOUBLE | Percent female-headed households |
| pct_elderly | DOUBLE | Percent elderly head-of-household (62+) |

---

## Join Architecture

```
geo.census_tracts (census_tract)
    └── hmda_loans (census_tract, activity_year)
            ├── sec.filing_metadata (respondent_name ~ company_name → mortgage lender 10-K)
            ├── ref.lei_entities (lei → lender legal entity)
            └── census.acs_demographics (census_tract → fair-lending demographics)

geo.counties (county_fips)
    ├── hud_fair_market_rents (county_fips, fmr_year)
    │       └── census.acs_housing (county_fips → FMR vs. actual rent gap)
    ├── census_building_permits (county_fips, permit_year)
    │       └── econ.bls_employment (county_fips → construction employment)
    └── hud_subsidized_housing (county_fips, report_year)
            └── census.acs_income (county_fips → subsidy dependency rate)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 92 | initial | All 5 tables, full history (2007+) | 6 GB / 8 GB | Once |
| 93 | recurring | `hmda_loans`, `census_building_permits`, `hud_fair_market_rents`, `hud_subsidized_housing` | 2 GB / 3 GB | Annual/Monthly |

*HMDA is large (~15M rows/year); initial worker needs extended heap and timeout.*

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `hmda_loans` | Months 6–8 | CFPB annual HMDA publication |
| `hud_fair_market_rents` | Month 10 | HUD annual FMR publication |
| `census_building_permits` | Months 1–12 | Monthly Census SOC release |
| `hud_subsidized_housing` | Months 6–9 | HUD annual Picture of Subsidized Households |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `HOUSING_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=housing`) |
| `HOUSING_CACHE_DIR` | Recommended | Raw download cache |
| `HOUSING_SINCE_YEAR` | Optional | HMDA start year (default: 2018 — post-HMDA reform) |

---

## Implementation Notes

- **HMDA volume.** Post-2018 HMDA reform expanded required fields. Annual files are ~15M
  rows / ~5GB CSV. The initial worker needs `-t 480` (8-hour timeout). Consider filtering
  to originated loans only (`action_taken = 1`) for the primary table; store all actions
  in a separate `hmda_applications` table if denial analysis is needed.
- **HMDA reform break.** The 2018 HMDA rule change added ~25 new fields and removed
  several legacy fields. Pre-2018 and post-2018 schemas differ significantly;
  `activity_year >= 2018` is recommended as the default start unless legacy comparison
  is required.
- **Census tract vintage.** HMDA census tracts use the vintage current at time of
  origination (2010 tracts for 2018–2021; 2020 tracts for 2022+). Join to
  `geo.census_tracts` using the matching vintage year.
