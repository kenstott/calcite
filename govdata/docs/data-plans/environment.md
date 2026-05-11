# Environment Schema Data Plan

## Strategic Context

The EPA publishes facility-level environmental compliance and emissions data covering
virtually every industrial site in the U.S. The `env` schema's primary cross-schema value
is **environmental risk and ESG analysis**: joining EPA Toxic Release Inventory and
greenhouse gas emissions to SEC 10-K disclosures identifies companies whose reported
emissions exceed regulatory thresholds or whose facilities appear on Superfund's NPL.
Drinking water violations join to CENSUS demographics for environmental justice research.
Air quality data joins to HEALTH mortality for pollution-health outcome analysis.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| Toxic Release Inventory (TRI) | EPA | REST API + bulk CSV | Free, no key | Annual |
| Superfund NPL sites | EPA CERCLIS | REST API (ECHO) | Free, no key | Continuous |
| Air Quality System (AQS) | EPA | REST API | Free, API key required | Annual |
| Safe Drinking Water violations | EPA SDWIS | REST API (ECHO) | Free, no key | Continuous |
| Greenhouse Gas Reporting (GHGRP) | EPA | REST API + bulk CSV | Free, no key | Annual |

EPA ECHO API: `https://echo.epa.gov/tools/web-services`
EPA TRI: `https://www.epa.gov/toxics-release-inventory-tri-program/tri-data-and-tools`
EPA AQS API: `https://aqs.epa.gov/aqsweb/documents/data_api.html` (key: free at epa.gov/aqs)
EPA GHGRP: `https://www.epa.gov/ghgreporting/ghg-reporting-program-data-sets`

---

## Proposed Tables

### `epa_tri_releases`

Facility-level toxic chemical releases to air, water, and land from the Toxics Release
Inventory. One row per facility-chemical-year. Joins to `sec.filing_metadata` via facility
parent company name for ESG/environmental liability research.

**Source:** EPA TRI Explorer bulk CSV — `tri_YYYY_us.csv`
**Partition:** `reporting_year`
**Auth:** None
**Cadence:** Annual (prior year published ~October)
**Release window:** Month 10

| Column | Type | Description |
|---|---|---|
| tri_facility_id | VARCHAR | EPA TRI facility ID |
| reporting_year | INTEGER | Partition key |
| facility_name | VARCHAR | Facility name |
| parent_company | VARCHAR | Parent company name (fuzzy-join to sec.filing_metadata) |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| latitude | DOUBLE | Facility latitude |
| longitude | DOUBLE | Facility longitude |
| chemical | VARCHAR | Chemical name |
| cas_number | VARCHAR | CAS registry number |
| industry_sector | VARCHAR | NAICS-based sector |
| total_releases | DOUBLE | Total on-site releases (pounds) |
| air_releases | DOUBLE | Fugitive + stack air emissions (pounds) |
| water_releases | DOUBLE | Discharges to surface water (pounds) |
| land_releases | DOUBLE | On-site land disposal (pounds) |
| off_site_transfers | DOUBLE | Off-site transfers for treatment/disposal (pounds) |
| carcinogen | BOOLEAN | True if chemical classified as known/suspected carcinogen |

---

### `epa_superfund_sites`

EPA National Priorities List (NPL) Superfund sites — contaminated sites undergoing
or awaiting cleanup under CERCLA. Joins to `geo.counties` for proximity analysis;
joins to `sec.filing_metadata` for potentially responsible party (PRP) identification.

**Source:** EPA ECHO Facility Search — `https://echo.epa.gov/tools/web-services/facility-search`
**Partition:** None (static snapshot; updated as site status changes)
**Auth:** None
**Cadence:** Quarterly (NPL additions and deletions quarterly)
**Release window:** Months 3, 6, 9, 12

| Column | Type | Description |
|---|---|---|
| site_id | VARCHAR | EPA Superfund site ID (CERCLIS ID) |
| site_name | VARCHAR | Site name |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| latitude | DOUBLE | Site latitude |
| longitude | DOUBLE | Site longitude |
| npl_status | VARCHAR | Final NPL / Proposed NPL / Deleted / Not NPL |
| npl_listing_date | DATE | Date added to NPL |
| npl_deletion_date | DATE | Date deleted (null if still listed) |
| site_score | DOUBLE | Hazard Ranking System (HRS) score |
| cleanup_status | VARCHAR | Assessment / Investigation / Cleanup / Construction Complete |
| federal_facility | BOOLEAN | True if federal government is responsible party |

---

### `epa_air_quality`

EPA Air Quality System (AQS) annual summary statistics by county. Air quality is a
leading indicator for respiratory health outcomes — joins to `health.cdc_mortality`
for pollution-mortality correlation; joins to `census.acs_demographics` for
environmental justice analysis.

**Source:** EPA AQS API — `dailyData/byCounty` aggregated to annual summaries
**Partition:** `aqs_year`
**Auth:** AQS API key (free at epa.gov/aqs/rfi)
**Cadence:** Annual (prior year published ~March)
**Release window:** Months 3–5

| Column | Type | Description |
|---|---|---|
| aqs_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| pollutant | VARCHAR | PM2.5 / PM10 / Ozone / NO2 / SO2 / CO / Lead |
| pollutant_standard | VARCHAR | EPA NAAQS standard applied |
| days_measured | INTEGER | Days with valid measurements |
| days_good | INTEGER | Days with AQI classified Good |
| days_moderate | INTEGER | Days Moderate |
| days_unhealthy_sensitive | INTEGER | Days Unhealthy for Sensitive Groups |
| days_unhealthy | INTEGER | Days Unhealthy |
| days_very_unhealthy | INTEGER | Days Very Unhealthy |
| days_hazardous | INTEGER | Days Hazardous |
| median_aqi | DOUBLE | Median AQI for the year |
| max_aqi | INTEGER | Maximum AQI observed |
| ninety_pct_aqi | DOUBLE | 90th percentile AQI |

---

### `epa_drinking_water_violations`

EPA Safe Drinking Water Information System (SDWIS) violations by public water system
and county. Joins to CENSUS demographics for environmental justice research; joins to
DISASTERS flood/wildfire for post-disaster contamination patterns.

**Source:** EPA ECHO Safe Drinking Water — `https://echo.epa.gov/tools/web-services/`
**Partition:** `violation_year`
**Auth:** None
**Cadence:** Continuous (violations added within weeks of detection)
**Release window:** Months 1–12

| Column | Type | Description |
|---|---|---|
| pws_id | VARCHAR | Public water system ID |
| violation_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| pws_name | VARCHAR | Water system name |
| pws_type | VARCHAR | Community / Non-Transient Non-Community / Transient |
| population_served | INTEGER | Population served by this system |
| violation_category | VARCHAR | Health-Based / Monitoring and Reporting / Treatment Technique |
| contaminant | VARCHAR | Contaminant name |
| contaminant_code | VARCHAR | EPA contaminant code |
| compliance_status | VARCHAR | In Violation / Returned to Compliance / Addressed |
| violation_begin | DATE | Date violation began |
| violation_end | DATE | Date returned to compliance (null if ongoing) |

---

### `epa_ghg_emissions`

EPA Greenhouse Gas Reporting Program (GHGRP) facility-level annual GHG emissions.
One row per facility-gas-year. Directly supports climate disclosure analysis —
joins to `sec.filing_metadata` via parent company for Scope 1 emissions vs. 10-K
climate risk disclosures.

**Source:** EPA GHGRP bulk data — `https://www.epa.gov/ghgreporting/ghg-reporting-program-data-sets`
**Partition:** `reporting_year`
**Auth:** None
**Cadence:** Annual (prior year published ~October)
**Release window:** Month 10

| Column | Type | Description |
|---|---|---|
| ghgrp_facility_id | VARCHAR | EPA GHGRP facility ID |
| reporting_year | INTEGER | Partition key |
| facility_name | VARCHAR | Facility name |
| parent_company | VARCHAR | Parent company name (fuzzy-join to sec.filing_metadata) |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| latitude | DOUBLE | Facility latitude |
| longitude | DOUBLE | Facility longitude |
| industry_type | VARCHAR | Power Plants / Petroleum Refining / Chemicals / etc. |
| gas | VARCHAR | CO2 / CH4 / N2O / HFCs / PFCs / SF6 |
| emissions_mt_co2e | DOUBLE | Emissions (metric tons CO2 equivalent) |
| biogenic_co2_mt | DOUBLE | Biogenic CO2 (excluded from totals per EPA rules) |

---

## Join Architecture

```
geo.counties (county_fips)
    ├── epa_tri_releases (county_fips, reporting_year)
    │       ├── sec.filing_metadata (parent_company ~ company_name → ESG liability)
    │       └── health.cdc_mortality (county_fips + year → pollution-health outcomes)
    ├── epa_superfund_sites (county_fips)
    │       └── census.acs_demographics (county_fips → environmental justice)
    ├── epa_air_quality (county_fips, aqs_year)
    │       ├── health.cdc_mortality (county_fips + year → respiratory mortality)
    │       └── census.acs_demographics (county_fips → EJ demographics)
    ├── epa_drinking_water_violations (county_fips, violation_year)
    │       ├── disasters.wildfire_perimeters (county_fips + year → post-fire contamination)
    │       └── census.acs_demographics (county_fips → EJ analysis)
    └── epa_ghg_emissions (county_fips, reporting_year)
            └── sec.filing_metadata (parent_company ~ company_name → Scope 1 vs. disclosure)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 86 | initial | All 5 tables, full history (2010+) | 4 GB / 6 GB | Once |
| 87 | annual | All 5 tables, incremental | 2 GB / 3 GB | Annual |

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `epa_tri_releases` | Month 10 | Annual TRI prior-year publication |
| `epa_superfund_sites` | Months 3, 6, 9, 12 | Quarterly NPL updates |
| `epa_air_quality` | Months 3–5 | AQS annual summary release |
| `epa_drinking_water_violations` | Months 1–12 | Continuous SDWIS updates |
| `epa_ghg_emissions` | Month 10 | Annual GHGRP prior-year publication |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ENV_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=env`) |
| `ENV_CACHE_DIR` | Recommended | Raw download cache |
| `ENV_AQS_API_KEY` | Required | EPA AQS API key (free at epa.gov) |
| `ENV_SINCE_YEAR` | Optional | Incremental start year (default: 2010) |
