# Environment Schema Data Plan

## Implementation Status (2026-07-16)

**Status: PARTIALLY DELIVERED.** All 5 planned datasets exist (renamed/restructured), plus a
number of extension tables and views beyond the original plan. Several planned columns — most
notably the entire AQI day-count model on air quality — were not built.

Verified against `environment-schema.yaml` and `.../govdata/environment/`.

| Planned table | Delivered as | Notes |
|---|---|---|
| `epa_tri_releases` | `tri_releases` | Renamed; missing `parent_company`, `land_releases`, `carcinogen`; no FIPS |
| `epa_superfund_sites` | `superfund_sites` | Renamed; missing NPL dates, HRS score, cleanup status |
| `epa_air_quality` | `air_quality_annual` | By monitor (not county summary); AQI day-count model NOT built |
| `epa_drinking_water_violations` | `drinking_water_violations` | Renamed; not year-partitioned; no county FIPS |
| `epa_ghg_emissions` | `ghg_emissions` + `ghg_facilities` + view `ghg_facility_annual` | Split; missing `biogenic_co2_mt` |

**Extension tables/views delivered beyond plan:** `air_quality_daily`, `aqs_monitors`,
`water_sites`, `streamflow`, `drinking_water`, `epa_facilities`, `rcra_facilities`,
`water_quality_samples`; views `ghg_facility_annual`, `facility_compliance`,
`air_quality_annual_by_county`.

**Missing columns / not yet built:**
- `tri_releases`: `parent_company`, `land_releases`, `carcinogen` all ABSENT; no `state_fips`/`county_fips` (uses `state_abbr` + county NAME); `industry_sector` only as `primary_naics`.
- `superfund_sites`: `npl_listing_date`, `npl_deletion_date`, `site_score` (HRS), `cleanup_status` all ABSENT (only `npl_status_code`/`npl_status_name`).
- `air_quality_annual`: entire AQI day-count model NOT built — `days_good`/`days_moderate`/`days_unhealthy_sensitive`/`days_unhealthy`/`days_very_unhealthy`/`days_hazardous`/`median_aqi`/`max_aqi`/`ninety_pct_aqi` all ABSENT; carries per-monitor `arithmetic_mean`/`first_max_value`/`aqi` instead.
- `drinking_water_violations`: `county_fips` ABSENT; NOT year-partitioned (per-state snapshot); `population_served` lives in separate `drinking_water` table; contaminant only as `contaminant_code` (no name).
- `ghg_emissions`: `biogenic_co2_mt` ABSENT; gas only as `gas_id` (no name); `emissions_mt_co2e` delivered as `co2e_emission`; `parent_company`/geography live in `ghg_facilities`.

---

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

### `epa_tri_releases` — delivered as `tri_releases`

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
| parent_company | VARCHAR | Parent company name (fuzzy-join to sec.filing_metadata) — **NOT BUILT** |
| state_fips | VARCHAR | FK to geo.states — **NOT BUILT** (uses `state_abbr` instead) |
| county_fips | VARCHAR | FK to geo.counties — **NOT BUILT** (uses county NAME instead) |
| latitude | DOUBLE | Facility latitude |
| longitude | DOUBLE | Facility longitude |
| chemical | VARCHAR | Chemical name |
| cas_number | VARCHAR | CAS registry number |
| industry_sector | VARCHAR | NAICS-based sector — delivered only as `primary_naics` |
| total_releases | DOUBLE | Total on-site releases (pounds) |
| air_releases | DOUBLE | Fugitive + stack air emissions (pounds) |
| water_releases | DOUBLE | Discharges to surface water (pounds) |
| land_releases | DOUBLE | On-site land disposal (pounds) — **NOT BUILT** |
| off_site_transfers | DOUBLE | Off-site transfers for treatment/disposal (pounds) |
| carcinogen | BOOLEAN | True if chemical classified as known/suspected carcinogen — **NOT BUILT** |

---

### `epa_superfund_sites` — delivered as `superfund_sites`

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
| npl_status | VARCHAR | Final NPL / Proposed NPL / Deleted / Not NPL — delivered as `npl_status_code`/`npl_status_name` |
| npl_listing_date | DATE | Date added to NPL — **NOT BUILT** |
| npl_deletion_date | DATE | Date deleted (null if still listed) — **NOT BUILT** |
| site_score | DOUBLE | Hazard Ranking System (HRS) score — **NOT BUILT** |
| cleanup_status | VARCHAR | Assessment / Investigation / Cleanup / Construction Complete — **NOT BUILT** |
| federal_facility | BOOLEAN | True if federal government is responsible party |

---

### `epa_air_quality` — delivered as `air_quality_annual` (by monitor, not county summary)

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
| days_measured | INTEGER | Days with valid measurements — **NOT BUILT** (entire AQI day-count model absent; table carries per-monitor `arithmetic_mean`/`first_max_value`/`aqi` instead) |
| days_good | INTEGER | Days with AQI classified Good — **NOT BUILT** |
| days_moderate | INTEGER | Days Moderate — **NOT BUILT** |
| days_unhealthy_sensitive | INTEGER | Days Unhealthy for Sensitive Groups — **NOT BUILT** |
| days_unhealthy | INTEGER | Days Unhealthy — **NOT BUILT** |
| days_very_unhealthy | INTEGER | Days Very Unhealthy — **NOT BUILT** |
| days_hazardous | INTEGER | Days Hazardous — **NOT BUILT** |
| median_aqi | DOUBLE | Median AQI for the year — **NOT BUILT** |
| max_aqi | INTEGER | Maximum AQI observed — **NOT BUILT** |
| ninety_pct_aqi | DOUBLE | 90th percentile AQI — **NOT BUILT** |

---

### `epa_drinking_water_violations` — delivered as `drinking_water_violations` (per-state snapshot, NOT year-partitioned)

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
| violation_year | INTEGER | Partition key — **NOT BUILT** (delivered as per-state snapshot, not year-partitioned) |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties — **NOT BUILT** |
| pws_name | VARCHAR | Water system name |
| pws_type | VARCHAR | Community / Non-Transient Non-Community / Transient |
| population_served | INTEGER | Population served by this system — delivered in separate `drinking_water` table |
| violation_category | VARCHAR | Health-Based / Monitoring and Reporting / Treatment Technique |
| contaminant | VARCHAR | Contaminant name — **NOT BUILT** (only `contaminant_code`, no name) |
| contaminant_code | VARCHAR | EPA contaminant code |
| compliance_status | VARCHAR | In Violation / Returned to Compliance / Addressed |
| violation_begin | DATE | Date violation began |
| violation_end | DATE | Date returned to compliance (null if ongoing) |

---

### `epa_ghg_emissions` — delivered as `ghg_emissions` + `ghg_facilities` + view `ghg_facility_annual`

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
| facility_name | VARCHAR | Facility name — lives in `ghg_facilities` |
| parent_company | VARCHAR | Parent company name (fuzzy-join to sec.filing_metadata) — lives in `ghg_facilities` |
| state_fips | VARCHAR | FK to geo.states — geography lives in `ghg_facilities` |
| county_fips | VARCHAR | FK to geo.counties — geography lives in `ghg_facilities` |
| latitude | DOUBLE | Facility latitude — lives in `ghg_facilities` |
| longitude | DOUBLE | Facility longitude — lives in `ghg_facilities` |
| industry_type | VARCHAR | Power Plants / Petroleum Refining / Chemicals / etc. |
| gas | VARCHAR | CO2 / CH4 / N2O / HFCs / PFCs / SF6 — delivered only as `gas_id` (no name) |
| emissions_mt_co2e | DOUBLE | Emissions (metric tons CO2 equivalent) — delivered as `co2e_emission` |
| biogenic_co2_mt | DOUBLE | Biogenic CO2 (excluded from totals per EPA rules) — **NOT BUILT** |

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
