# Crime Schema Data Plan

## Existing Coverage

The `crime` schema is **already implemented** with the following tables sourced from the
FBI Crime Data Explorer (CDE):

| Table | Content |
|---|---|
| `cde_agencies` | FBI agency directory — ORI codes, NIBRS participation, location |
| `cde_offenses` | Offense rates by state, offense type, and month |
| `cde_hate_crimes` | Hate crime statistics by state |
| `cde_police_employment` | Officers per 1,000 population, staffing ratios |
| `cde_use_of_force` | Use-of-force incident data |

These tables cover **state-level rate data** via the FBI CDE API. The gaps this plan
addresses are:

1. **County-level raw counts** — NIBRS offense counts joined to `geo.counties` via ORI crosswalk
2. **Historical UCR baseline** — legacy aggregated counts (1985–2020) for long trend analysis
3. **BJS corrections** — prison population by state (not in existing schema)

## Strategic Context (Extensions)

This plan extends the existing `crime` schema with county-level and historical data
not covered by the current CDE rate-based tables. The primary additional cross-schema
value is **neighborhood-level risk modeling**: county crime counts join to HOUSING
HMDA mortgage data for default risk analysis; violent crime joins to HEALTH mortality
for cause-of-death research.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| NIBRS incident data | FBI CDE | REST API + bulk CSV | Free, API key required | Annual |
| UCR legacy offenses (1985–2020) | FBI CDE | REST API + bulk CSV | Free, API key required | Annual (historical) |
| UCR hate crimes | FBI CDE | REST API + bulk CSV | Free, API key required | Annual |
| BJS corrections (prison population) | BJS | CSV bulk download | Free, no key | Annual |

FBI Crime Data Explorer API: `https://cde.fbi.gov/api` (key: free at cde.fbi.gov)
BJS: `https://bjs.ojp.gov/data`

---

## Proposed Tables

### `nibrs_offenses`

FBI NIBRS offense counts by reporting agency (ORI), offense type, and year. NIBRS
replaced UCR as the FBI's primary crime reporting program in 2021; data available
from agencies that adopted NIBRS earlier (many since the 1990s). Joins to
`geo.counties` via agency jurisdiction for county-level analysis.

**Source:** FBI CDE API — `/nibrs/{offense}` aggregated by agency and year
**Partition:** `incident_year`
**Auth:** FBI CDE API key (free at cde.fbi.gov)
**Cadence:** Annual (prior year published ~October)
**Release window:** Months 10–12

| Column | Type | Description |
|---|---|---|
| incident_year | INTEGER | Partition key |
| ori | VARCHAR | Agency ORI code |
| agency_name | VARCHAR | Reporting agency name |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties (derived from agency jurisdiction) |
| offense_category | VARCHAR | Crimes Against Persons / Property / Society |
| offense_type | VARCHAR | Murder / Rape / Robbery / Assault / Burglary / Larceny / etc. |
| offense_count | INTEGER | Number of offenses |
| cleared_count | INTEGER | Offenses cleared by arrest or exceptional means |
| clearance_rate | DOUBLE | cleared / offense_count |
| victim_count | INTEGER | Total victims |
| arrestee_count | INTEGER | Total arrestees |

---

### `ucr_offenses`

FBI UCR legacy Part I offense counts by jurisdiction and year (1985–2020). Provides
long historical baseline before NIBRS transition. Joins to `geo.counties` via FIPS for
trend analysis spanning decades.

**Source:** FBI CDE API — `/ucr/offenses/count/national/offense-type/criminal-offense`
**Partition:** `report_year`
**Auth:** FBI CDE API key
**Cadence:** Static historical (1985–2020; FBI no longer updates legacy UCR)
**Release window:** N/A (historical only; load once)

| Column | Type | Description |
|---|---|---|
| report_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| agency_name | VARCHAR | Reporting jurisdiction name |
| population | INTEGER | Covered population |
| violent_crime | INTEGER | Total Part I violent crimes |
| murder | INTEGER | Murder and non-negligent manslaughter |
| rape | INTEGER | Rape (legacy definition) |
| robbery | INTEGER | Robbery |
| aggravated_assault | INTEGER | Aggravated assault |
| property_crime | INTEGER | Total Part I property crimes |
| burglary | INTEGER | Burglary |
| larceny_theft | INTEGER | Larceny-theft |
| motor_vehicle_theft | INTEGER | Motor vehicle theft |
| arson | INTEGER | Arson |

---

### `ucr_hate_crimes`

FBI annual hate crime statistics by offense type, bias motivation, and jurisdiction.
Joins to CENSUS demographics for correlation of hate crime patterns with demographic
composition; joins to `geo.counties` for geographic clustering.

**Source:** FBI CDE API — `/ucr/hate-crime/`
**Partition:** `report_year`
**Auth:** FBI CDE API key
**Cadence:** Annual (prior year published ~October)
**Release window:** Months 10–12

| Column | Type | Description |
|---|---|---|
| report_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states |
| county_fips | VARCHAR | FK to geo.counties |
| agency_name | VARCHAR | Reporting agency |
| bias_motivation | VARCHAR | Race/Ethnicity / Religion / Sexual Orientation / Disability / Gender / Gender Identity |
| offense_type | VARCHAR | Intimidation / Simple Assault / Aggravated Assault / Vandalism / etc. |
| incident_count | INTEGER | Number of hate crime incidents |
| victim_count | INTEGER | Number of victims |
| offender_count | INTEGER | Number of known offenders |

---

### `bjs_state_corrections`

Bureau of Justice Statistics annual prison population by state — sentenced prisoners
under state and federal jurisdiction. Joins to `census.acs_demographics` for
incarceration rate analysis; joins to `econ.bls_employment` for criminal justice
employment.

**Source:** BJS National Prisoner Statistics (NPS) — annual CSV bulletin
**Partition:** `report_year`
**Auth:** None
**Cadence:** Annual (prior year published ~October)
**Release window:** Months 10–12

| Column | Type | Description |
|---|---|---|
| report_year | INTEGER | Partition key |
| state_fips | VARCHAR | FK to geo.states (null for federal) |
| jurisdiction | VARCHAR | State name or "Federal" |
| jurisdiction_type | VARCHAR | State / Federal |
| total_population | INTEGER | Sentenced prisoners under jurisdiction |
| male_population | INTEGER | Male sentenced prisoners |
| female_population | INTEGER | Female sentenced prisoners |
| admissions | INTEGER | Annual admissions |
| releases | INTEGER | Annual releases |
| capacity_operational | INTEGER | Rated operational capacity |
| capacity_pct | DOUBLE | Population / operational capacity |

---

## Join Architecture

```
geo.counties (county_fips)
    ├── nibrs_offenses (county_fips, incident_year)
    │       ├── census.acs_demographics (county_fips → socioeconomic correlates)
    │       ├── health.cdc_mortality (county_fips + year → violent death overlap)
    │       └── econ.bls_employment (county_fips → unemployment / crime correlation)
    └── ucr_offenses (county_fips, report_year)
            └── housing.hmda_loans (county_fips → property crime / mortgage default)

    [historical trend]
    ucr_offenses (1985–2020) → nibrs_offenses (2016+) [overlap for calibration]

geo.states (state_fips)
    └── bjs_state_corrections (state_fips, report_year)
            └── census.acs_demographics (state_fips → incarceration rate)
```

---

## Worker Assignment

| Worker | Mode | Tables | Heap | Schedule |
|---|---|---|---|---|
| 90 | initial | All 4 tables, full history (1985+) | 4 GB / 6 GB | Once |
| 91 | annual | `nibrs_offenses`, `ucr_hate_crimes`, `bjs_state_corrections` | 2 GB / 3 GB | Annual |

*`ucr_offenses` is historical-only (1985–2020); no annual update needed after initial load.*

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `nibrs_offenses` | Months 10–12 | FBI annual crime statistics release |
| `ucr_offenses` | N/A (historical) | Static load; FBI legacy program ended |
| `ucr_hate_crimes` | Months 10–12 | FBI annual hate crime statistics |
| `bjs_state_corrections` | Months 10–12 | BJS annual prisoner statistics |

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `CRIME_PARQUET_DIR` | Recommended | Output path (falls back to `${GOVDATA_PARQUET_DIR}/source=crime`) |
| `CRIME_CACHE_DIR` | Recommended | Raw download cache |
| `CRIME_FBI_API_KEY` | Required | FBI CDE API key (free at cde.fbi.gov) |
| `CRIME_SINCE_YEAR` | Optional | UCR/NIBRS start year (default: 1985) |

---

## Implementation Notes

- **UCR → NIBRS transition.** FBI officially moved to NIBRS-only reporting in 2021.
  UCR `report_year` 2020 is the last clean nationwide UCR dataset; 2021+ should use NIBRS.
  The overlap period 2016–2020 allows calibration between the two series.
- **Agency → county mapping.** Neither UCR nor NIBRS includes county FIPS directly.
  Use the FBI's ORI-to-FIPS crosswalk (available from ICPSR) to derive `county_fips`.
  Agencies that span multiple counties should be attributed to the primary county.
- **NIBRS coverage.** Not all agencies report to NIBRS. As of 2022, ~75% of U.S.
  population is covered. Offense counts are not directly comparable to UCR totals for
  non-participating jurisdictions.
