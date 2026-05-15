# Education Schema Documentation

## Overview

The `edu` schema provides access to U.S. education data spanning K-12 and postsecondary
institutions. It covers NCES Common Core of Data (CCD) district and school directories, NAEP
assessment scores and achievement levels, CRDC civil rights data for schools, IPEDS institutional
characteristics and completions/financials/tuition, College Scorecard outcomes, and program-level
earnings data.

The schema is served by `EduSchemaFactory` via `GovDataSchemaFactory` and is driven by
`edu-schema.yaml`. Data is sourced primarily from the Urban Institute Education Data API and
IPEDS bulk downloads.

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `ccd_districts` | NCES CCD local education agency directory: LEAID, name, state/county FIPS, locale code, CBSA, lat/lon, grades, enrollment, special ed, ELL, staff FTE, legislative districts | Urban Institute Education Data API `/school-districts/ccd/directory/{year}` | Annual |
| `ccd_schools` | NCES CCD school directory: NCESSCH, LEAID, name, state/county FIPS, lat/lon, locale, grades, charter, magnet, school level/type/status, enrollment, staff FTE | Urban Institute Education Data API `/schools/ccd/directory/{year}` | Annual |
| `naep_scores` | NAEP state assessment scores: subject, grade, year, scale score, standard error, by jurisdiction (52: NP + 51 states/DC) | Urban Institute Education Data API NAEP endpoint | Biennial |
| `naep_achievement_levels` | NAEP achievement level percentages: % below basic, basic, proficient, advanced by subject/grade/year/jurisdiction | Urban Institute Education Data API NAEP endpoint | Biennial |
| `crdc_schools` | Civil Rights Data Collection school-level data: discipline (suspensions, expulsions), arrests, law enforcement, chronic absenteeism, staff, harassment/bullying by race/sex | Urban Institute Education Data API CRDC endpoint | Biennial |
| `ipeds_institutions` | IPEDS institutional characteristics: UnitID, name, control type, HBCU flag, Tribal College flag, lat/lon, county FIPS, accreditor | IPEDS bulk XLSX | Annual |
| `ipeds_completions` | IPEDS degree completions by institution, CIP code, award level, and year | IPEDS bulk XLSX | Annual |
| `ipeds_financials` | IPEDS institutional finance: revenues, expenditures by function, assets, endowment by institution and year | IPEDS bulk XLSX | Annual |
| `ipeds_tuition` | IPEDS tuition and fees: in-state, out-of-state, on-campus room/board costs by institution and year | IPEDS bulk XLSX | Annual |
| `college_scorecard` | College Scorecard institutional outcomes: admission rate, SAT/ACT scores, retention, graduation rate, median earnings, median debt | College Scorecard API | Annual |
| `college_scorecard_programs` | College Scorecard program-level earnings and debt by institution, CIP code, and credential level | College Scorecard programs API | Annual |

---

## Views

| View | Description | Depends on |
|---|---|---|
| `district_resources` | District-level resource summary: enrollment, spending per pupil, teacher FTE, guidance counselor ratio | `ccd_districts` |
| `naep_state_trends` | NAEP scale score trends by state, subject, and grade across assessment years | `naep_scores` |
| `school_equity_profile` | School-level equity indicators: suspension rate, ELL share, special ed share, charter flag | `ccd_schools`, `crdc_schools` |
| `institution_outcomes` | College Scorecard outcomes enriched with IPEDS institutional characteristics and tuition | `college_scorecard`, `ipeds_institutions`, `ipeds_tuition` |
| `field_of_study_roi` | Program-level earnings minus estimated debt cost by CIP code and credential level | `college_scorecard_programs` |
| `district_charter_profile` | Charter vs. traditional school comparison within each district: enrollment share, staff ratios | `ccd_districts`, `ccd_schools` |
| `institution_completions_summary` | Institution-level degree completion totals by award level and broad CIP category | `ipeds_completions`, `ipeds_institutions` |
| `ipeds_financial_efficiency` | Expenditure per completion and revenue mix (tuition vs. government vs. auxiliary) by institution | `ipeds_financials`, `ipeds_completions` |
| `district_naep_spending` | Correlates district per-pupil spending with state NAEP scores | `ccd_districts`, `naep_scores` |
| `institution_county_context` | Postsecondary institutions enriched with county economic context from `econ.county_wages` | `ipeds_institutions`, `econ.county_wages` |
| `state_naep_geography` | NAEP scores enriched with state geography for choropleth mapping | `naep_scores`, `geo.states` |
| `naep_proficiency_summary` | % proficient or above by subject, grade, and year (national and state) | `naep_achievement_levels` |

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `GOVDATA_PARQUET_DIR` | Root Parquet directory |

---

## ETL Workers

| Worker | Cadence | Description |
|---|---|---|
| `worker-edu-annual` | Annual | Fetches CCD and IPEDS annual tables |
| `worker-edu-biennial` | Biennial | Fetches NAEP and CRDC biennial tables |

There is no `daily` worker for `edu`.

---

## Known DQ Warnings

- `crdc_schools.teachers_first_year_fte`, `teachers_absent_fte`, `law_enforcement_ind` — sparse/constant CRDC fields; `all_same_value` warning expected.
- `crdc_schools.pk_nulls` — 66,320 rows with `crdc_id=null` (chronic-absenteeism endpoint omits crdc_id; ncessch is populated); backfill pending.

---

## Sample Queries

```sql
-- States ranked by 8th-grade math NAEP proficiency (latest assessment)
SELECT jurisdiction, score, standard_error
FROM edu.naep_scores
WHERE subject = 'mathematics' AND grade = 8
  AND year = (SELECT MAX(year) FROM edu.naep_scores WHERE subject = 'mathematics' AND grade = 8)
ORDER BY score DESC;

-- District enrollment trends (largest 20 districts)
SELECT leaid, lea_name, state_abbr, year, enrollment
FROM edu.ccd_districts
WHERE leaid IN (
  SELECT leaid FROM edu.ccd_districts
  WHERE year = 2022
  ORDER BY enrollment DESC NULLS LAST LIMIT 20
)
ORDER BY leaid, year;

-- Top programs by median earnings 2 years after graduation
SELECT institution_name, cip_title, credential_level,
       earnings_2yr_median
FROM edu.college_scorecard_programs
WHERE earnings_2yr_median IS NOT NULL
ORDER BY earnings_2yr_median DESC
LIMIT 20;

-- Charter vs. traditional school suspension rate by state
SELECT s.state_abbr,
       AVG(CASE WHEN s.charter = 1 THEN CAST(c.out_of_school_suspensions AS DOUBLE) / NULLIF(s.enrollment, 0) END) AS charter_susp_rate,
       AVG(CASE WHEN s.charter = 0 THEN CAST(c.out_of_school_suspensions AS DOUBLE) / NULLIF(s.enrollment, 0) END) AS trad_susp_rate
FROM edu.ccd_schools s
JOIN edu.crdc_schools c ON s.ncessch = c.ncessch AND s.year = c.year
WHERE s.year = 2020
GROUP BY s.state_abbr
ORDER BY charter_susp_rate - trad_susp_rate DESC;
```
