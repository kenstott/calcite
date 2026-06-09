# Crime Schema Documentation

## Overview

The `crime` schema provides access to U.S. crime statistics from the FBI Crime Data Explorer
(CDE) and Bureau of Justice Statistics (BJS). It covers reported offenses, police agency
rosters, hate crimes, use-of-force incidents, arrests, supplemental homicide reports, law
enforcement officer fatalities, multi-year trends, victimization survey estimates, and a
cross-agency equity view.

The schema is served by `CrimeSchemaFactory` via `GovDataSchemaFactory` and is driven by
`crime-schema.yaml`.

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `cde_agencies` | Law enforcement agency roster: ORI, name, type, county, NIBRS status and start date, lat/lon | FBI CDE `/cde/agency/byStateAbbr/{state}` | Annual |
| `cde_offenses` | Monthly state-level offense and clearance rates per 100k population across all UCR offense codes | FBI CDE `/LATEST/summarized/state/{state}/{offense}` | Monthly |
| `cde_police_employment` | Annual sworn officer and civilian employee counts by state with officers-per-1,000 ratio | FBI CDE `/LATEST/pe` | Annual |
| `cde_hate_crimes` | Annual hate crime counts by state, bias motivation, and offense type | FBI CDE `/LATEST/hate-crime/state/{state}` | Annual |
| `cde_use_of_force` | Use-of-force incidents reported to FBI by agency, year, and outcome type | FBI CDE use-of-force endpoint | Annual |
| `cde_reta` | Agency-level monthly Part I offense + clearance counts (reported/unfounded/actual/cleared/juvenile-cleared) plus officers killed/assaulted; one row per agency (ORI7) per month | FBI Return A bulk master file (signed-URL download, fixed-width) | Annual (one national file/year) |
| `cde_arrests` | Annual arrest counts by state, offense, and age/sex/race demographics | FBI CDE arrests endpoint | Annual |
| `cde_shr` | Supplemental Homicide Reports: homicide circumstances, weapon, victim/offender relationship | FBI CDE SHR endpoint | Annual |
| `cde_leoka` | Law Enforcement Officers Killed and Assaulted: annual fatalities and assaults by circumstance | FBI CDE LEOKA endpoint | Annual |
| `cde_trends` | Multi-year offense trend data by state and offense code | FBI CDE trends endpoint | Annual |
| `cde_supplemental` | Supplemental crime data (arson, cargo theft, human trafficking, etc.) | FBI CDE supplemental endpoint | Annual |
| `bjs_nibrs_estimates` | BJS national NIBRS-based crime estimates; adjusts for non-reporting agencies | BJS statistics API | Annual |
| `bjs_ncvs_personal` | National Crime Victimization Survey — personal crimes (assault, robbery, rape/sexual assault) by year, type, and demographic | BJS NCVS tables | Annual |
| `bjs_ncvs_personal_pop` | NCVS personal crime population denominators (survey coverage estimates) | BJS NCVS tables | Annual |
| `bjs_ncvs_household` | NCVS household crimes (burglary, motor vehicle theft, trespassing) by year and type | BJS NCVS tables | Annual |
| `bjs_ncvs_household_pop` | NCVS household crime population denominators | BJS NCVS tables | Annual |

---

## Views

| View | Description | Depends on |
|---|---|---|
| `crime_enforcement_equity` | Annual offense rates correlated with police employment by state; one row per state × year × offense code | `cde_offenses`, `cde_police_employment` |

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `GOVDATA_PARQUET_DIR` | Root Parquet directory |
| `API_DATA_GOV` | api.data.gov API key — required for FBI CDE endpoints |

---

## Sample Queries

```sql
-- Top 10 states by violent crime rate (latest year)
SELECT state_abbr, year,
       AVG(offense_rate) AS avg_violent_crime_per_100k
FROM crime.cde_offenses
WHERE offense_code = 'violent-crime'
  AND year = (SELECT MAX(year) FROM crime.cde_offenses)
GROUP BY state_abbr, year
ORDER BY avg_violent_crime_per_100k DESC
LIMIT 10;

-- States with lowest officers-per-1000 ratio
SELECT state_abbr, year, officers_per_1000,
       male_officers + female_officers AS total_sworn
FROM crime.cde_police_employment
WHERE year = (SELECT MAX(year) FROM crime.cde_police_employment)
ORDER BY officers_per_1000
LIMIT 10;

-- Hate crime counts by bias motivation (national, latest year)
SELECT year, COUNT(*) AS incidents
FROM crime.cde_hate_crimes
WHERE year = (SELECT MAX(year) FROM crime.cde_hate_crimes)
GROUP BY year;

-- Enforcement equity: offense rate vs. officers per 1000
SELECT e.state_abbr, e.year, e.offense_code,
       e.offense_rate, e.clearance_rate,
       p.officers_per_1000
FROM crime.crime_enforcement_equity e
JOIN crime.cde_police_employment p
  ON e.state_abbr = p.state_abbr AND e.year = p.year
WHERE e.offense_code = 'homicide'
ORDER BY e.offense_rate DESC;
```
