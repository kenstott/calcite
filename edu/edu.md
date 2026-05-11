# Education Schema Plan (`edu-schema.yaml`)

## Purpose

Institutional education data — K-12 schools and districts, higher education institutions,
student outcomes, and equity metrics. Complements but does not duplicate the Census
ACS education attainment tables (`acs_education`, `acs_occupation`), which describe
population characteristics. This schema describes the institutions themselves.

## Join Architecture

```
geo.school_districts (sd_lea)
    └── ccd_districts (leaid)
            └── ccd_schools (ncessch, leaid)
                    ├── naep_scores (leaid / state_fips)
                    └── crdc_schools (ncessch)

geo.counties (county_fips)
    └── ipeds_institutions (county_fips, lat/lon)
            ├── ipeds_completions (unitid)
            ├── ipeds_financials (unitid)
            └── college_scorecard (opeid → unitid)
```

Key bridge: `ccd_districts.leaid` = `geo.school_districts.sd_lea` (both use the
NCES Local Education Agency ID). No crosswalk table needed — direct join.

---

## Data Sources

| Source | Publisher | Access | Update Cadence | Years Available |
|---|---|---|---|---|
| Common Core of Data (CCD) | NCES / Dept of Ed | Bulk CSV at nces.ed.gov | Annual | 1987–present |
| EDFacts / CRDC | Dept of Ed | Bulk CSV at data.ed.gov | Biennial (CRDC), Annual (EDFacts) | 2009–present |
| NAEP | NCES | API (api.nationsreportcard.gov) | Biennial | 1990–present |
| IPEDS | NCES | Bulk CSV at nces.ed.gov/ipeds | Annual | 1986–present |
| College Scorecard | Dept of Ed | API / Bulk CSV at collegescorecard.ed.gov | Annual | 1996–present |

All sources are freely available without API keys (NAEP API is unauthenticated).
IPEDS bulk downloads are ZIP files containing one CSV per survey component per year.

---

## Tables

### K-12

#### `ccd_districts`
Local Education Agency (district) directory. One row per LEA per year.

**Source:** NCES CCD — `LEA_directory_{year}.csv`
**Primary key:** `(leaid, year)`
**Join to geo:** `leaid = geo.school_districts.sd_lea`

| Column | Type | Description |
|---|---|---|
| leaid | VARCHAR | NCES LEA ID (7-digit) |
| year | INTEGER | School year (end year, e.g. 2023 = 2022-23) |
| lea_name | VARCHAR | District name |
| state_fips | VARCHAR | 2-digit state FIPS |
| county_fips | VARCHAR | 5-digit county FIPS |
| locale_code | VARCHAR | NCES locale (city/suburban/town/rural + size) |
| total_students | INTEGER | Total enrollment |
| title1_status | VARCHAR | Title I eligibility status |
| charter_lea | BOOLEAN | Charter district flag |
| operational_schools | INTEGER | Number of operational schools |
| teachers_fte | DOUBLE | FTE classroom teachers |
| pupil_teacher_ratio | DOUBLE | Students per FTE teacher |
| per_pupil_expenditure | DOUBLE | Total expenditure per pupil (from NCES F-33) |
| revenue_local_pct | DOUBLE | Local revenue share |
| revenue_state_pct | DOUBLE | State revenue share |
| revenue_federal_pct | DOUBLE | Federal revenue share |

---

#### `ccd_schools`
Individual school directory. One row per school per year.

**Source:** NCES CCD — `School_directory_{year}.csv`
**Primary key:** `(ncessch, year)`
**Join to district:** `leaid = ccd_districts.leaid`

| Column | Type | Description |
|---|---|---|
| ncessch | VARCHAR | NCES school ID (12-digit) |
| leaid | VARCHAR | Parent LEA ID |
| year | INTEGER | School year end year |
| school_name | VARCHAR | School name |
| state_fips | VARCHAR | State FIPS |
| county_fips | VARCHAR | County FIPS |
| latitude | DOUBLE | School latitude |
| longitude | DOUBLE | School longitude |
| grade_low | VARCHAR | Lowest grade offered |
| grade_high | VARCHAR | Highest grade offered |
| school_type | VARCHAR | Regular / Special / Vocational / Alternative |
| charter | BOOLEAN | Charter school flag |
| magnet | BOOLEAN | Magnet school flag |
| title1_status | VARCHAR | Title I eligibility |
| locale_code | VARCHAR | NCES locale code |
| total_students | INTEGER | Total enrollment |
| free_lunch_pct | DOUBLE | Free/reduced lunch eligibility pct |

---

#### `naep_scores`
NAEP assessment results by state and subject. Biennial. Nation's Report Card.

**Source:** NCES NAEP API — `api.nationsreportcard.gov/data/`
**Primary key:** `(state_fips, year, subject, grade, subgroup)`

| Column | Type | Description |
|---|---|---|
| state_fips | VARCHAR | State FIPS (or 'NT' for national) |
| year | INTEGER | Assessment year |
| subject | VARCHAR | reading / mathematics |
| grade | INTEGER | 4 or 8 |
| subgroup | VARCHAR | all / male / female / white / black / hispanic / asian / frpl / iep / ell |
| avg_score | DOUBLE | Average scale score |
| pct_below_basic | DOUBLE | % below basic proficiency |
| pct_basic | DOUBLE | % at basic |
| pct_proficient | DOUBLE | % at proficient |
| pct_advanced | DOUBLE | % at advanced |
| sample_size | INTEGER | Number of students assessed |

---

#### `crdc_schools`
Civil Rights Data Collection — equity and access metrics by school. Biennial.

**Source:** Dept of Ed CRDC — bulk CSV at ocrdata.ed.gov
**Primary key:** `(ncessch, year)`
**Join to school:** `ncessch = ccd_schools.ncessch`

| Column | Type | Description |
|---|---|---|
| ncessch | VARCHAR | NCES school ID |
| year | INTEGER | Survey year |
| ap_offered | BOOLEAN | Any AP courses offered |
| ap_enrollment | INTEGER | AP course enrollment |
| ib_offered | BOOLEAN | IB program offered |
| gifted_enrollment | INTEGER | Students in gifted/talented programs |
| chronic_absent_pct | DOUBLE | Chronically absent students (>10% days missed) |
| in_school_suspensions | INTEGER | In-school suspensions |
| out_school_suspensions | INTEGER | Out-of-school suspensions |
| expulsions | INTEGER | Students expelled |
| law_enforcement_referrals | INTEGER | Referrals to law enforcement |
| restraint_incidents | INTEGER | Physical restraint incidents |
| harassment_sex | INTEGER | Allegations of sex-based harassment |
| harassment_race | INTEGER | Allegations of race-based harassment |
| counselors_fte | DOUBLE | FTE school counselors |
| psychologists_fte | DOUBLE | FTE school psychologists |

---

### Higher Education

#### `ipeds_institutions`
Institution directory. One row per institution per year.

**Source:** NCES IPEDS — HD (Institutional Characteristics) survey
**Primary key:** `(unitid, year)`
**Join to geo:** `county_fips = geo.counties.county_fips`; lat/lon for spatial joins

| Column | Type | Description |
|---|---|---|
| unitid | INTEGER | IPEDS unit ID |
| opeid | VARCHAR | OPE ID (links to College Scorecard) |
| year | INTEGER | Survey year |
| institution_name | VARCHAR | Institution name |
| state_fips | VARCHAR | State FIPS |
| county_fips | VARCHAR | County FIPS |
| city | VARCHAR | City |
| latitude | DOUBLE | Latitude |
| longitude | DOUBLE | Longitude |
| sector | VARCHAR | Public 4yr / Private nonprofit 4yr / Private for-profit 4yr / 2yr / etc. |
| control | VARCHAR | Public / Private nonprofit / Private for-profit |
| carnegie_class | VARCHAR | Carnegie classification |
| historically_black | BOOLEAN | HBCU flag |
| tribal_college | BOOLEAN | Tribal college flag |
| total_enrollment | INTEGER | 12-month unduplicated headcount |
| undergrad_enrollment | INTEGER | Undergraduate enrollment |
| grad_enrollment | INTEGER | Graduate enrollment |
| full_time_retention_rate | DOUBLE | First-time full-time retention rate |
| graduation_rate_6yr | DOUBLE | 6-year graduation rate (150% time) |
| tuition_in_state | INTEGER | In-state tuition and fees |
| tuition_out_state | INTEGER | Out-of-state tuition and fees |

---

#### `ipeds_completions`
Degrees and certificates awarded by field of study.

**Source:** NCES IPEDS — C (Completions) survey
**Primary key:** `(unitid, year, cipcode, award_level)`

| Column | Type | Description |
|---|---|---|
| unitid | INTEGER | IPEDS unit ID |
| year | INTEGER | Award year |
| cipcode | VARCHAR | CIP field of study code |
| cip_title | VARCHAR | Field of study description |
| award_level | VARCHAR | Certificate <1yr / Certificate 1-2yr / Associates / Bachelors / Masters / Doctoral |
| total_awards | INTEGER | Total degrees/certificates awarded |
| awards_men | INTEGER | Men |
| awards_women | INTEGER | Women |
| awards_white | INTEGER | White non-Hispanic |
| awards_black | INTEGER | Black non-Hispanic |
| awards_hispanic | INTEGER | Hispanic |
| awards_asian | INTEGER | Asian |

---

#### `ipeds_financials`
Institution revenues and expenditures.

**Source:** NCES IPEDS — F (Finance) survey
**Primary key:** `(unitid, year)`

| Column | Type | Description |
|---|---|---|
| unitid | INTEGER | IPEDS unit ID |
| year | INTEGER | Fiscal year |
| total_revenue | BIGINT | Total revenues |
| tuition_revenue | BIGINT | Tuition and fees revenue |
| federal_grants | BIGINT | Federal grants and contracts |
| state_grants | BIGINT | State grants and contracts |
| endowment_revenue | BIGINT | Investment return applied to operations |
| endowment_assets_eoy | BIGINT | Endowment assets end of year |
| total_expenditure | BIGINT | Total expenditures |
| instruction_expenditure | BIGINT | Instruction |
| research_expenditure | BIGINT | Research |
| student_services_expenditure | BIGINT | Student services |
| institutional_support_expenditure | BIGINT | Institutional support (administration) |
| net_tuition_revenue | BIGINT | Tuition revenue net of discounts |

---

#### `college_scorecard`
Post-graduation outcomes by institution and field of study.

**Source:** Dept of Ed College Scorecard — collegescorecard.ed.gov/data/
**Primary key:** `(opeid, year, cip_code)` for program-level; `(opeid, year)` for institution-level

| Column | Type | Description |
|---|---|---|
| opeid | VARCHAR | OPE ID (joins to ipeds_institutions.opeid) |
| unitid | INTEGER | IPEDS unit ID |
| year | INTEGER | Cohort year |
| cip_code | VARCHAR | Field of study (NULL for institution-level rows) |
| cip_title | VARCHAR | Field of study title |
| median_earnings_10yr | INTEGER | Median earnings 10 years after enrollment |
| mean_earnings_10yr | INTEGER | Mean earnings 10 years after enrollment |
| median_debt_graduates | INTEGER | Median debt at graduation |
| median_debt_withdrawers | INTEGER | Median debt for non-completers |
| completion_rate | DOUBLE | Overall completion rate |
| pell_pct | DOUBLE | Share of students receiving Pell grants |
| first_gen_pct | DOUBLE | Share of first-generation students |
| median_family_income | INTEGER | Median family income of entering students |

---

## Cross-Schema Research Queries Enabled

```sql
-- Achievement gap by district poverty level
SELECT d.lea_name, d.per_pupil_expenditure,
       n.avg_score, c.free_lunch_pct
FROM ccd_districts d
JOIN geo.school_districts g ON d.leaid = g.sd_lea
JOIN naep_scores n ON d.state_fips = n.state_fips AND d.year = n.year
JOIN ccd_schools s ON s.leaid = d.leaid
JOIN (SELECT leaid, avg(free_lunch_pct) AS free_lunch_pct
      FROM ccd_schools GROUP BY leaid) c ON c.leaid = d.leaid
WHERE n.grade = 8 AND n.subject = 'mathematics' AND n.subgroup = 'all';

-- College outcomes by county economic conditions
SELECT i.institution_name, i.county_fips,
       sc.median_earnings_10yr, w.annual_avg_weekly_wage
FROM ipeds_institutions i
JOIN college_scorecard sc ON i.opeid = sc.opeid
JOIN econ.county_wages w ON i.county_fips = w.area_fips
WHERE i.year = 2022 AND sc.cip_code IS NULL;

-- For-profit college SEC filings joined to student outcomes
SELECT fm.company_name, sc.completion_rate, sc.median_debt_graduates,
       fli.value_numeric AS revenue
FROM sec.filing_metadata fm
JOIN ipeds_institutions i ON lower(fm.company_name) LIKE '%' || lower(i.institution_name) || '%'
JOIN college_scorecard sc ON i.opeid = sc.opeid
JOIN sec.financial_line_items fli ON fm.cik = fli.cik
WHERE fli.concept LIKE '%Revenue%' AND fm.filing_type = '10-K';
```

---

## Operations

### Run-Pool Integration

The edu schema uses **workers 71–73** in the parallel worker pool:

| Worker | Script | Mode | Heap | Schedule |
|--------|--------|------|------|----------|
| 71 | `worker-71.sh` | `initial` | 4 GB / 6 GB | One-time first-time setup |
| 72 | `worker-72.sh` | `annual` | 2 GB / 3 GB | Annually (e.g., November after IPEDS release) |
| 73 | `worker-73.sh` | `biennial` | 2 GB / 3 GB | Annually (safe; returns unchanged data off-cycle) |

```bash
# First-time setup — full historical load for all 10 tables
./run-pool.sh 71

# Recurring annual refresh (CCD + IPEDS + College Scorecard)
./run-pool.sh 72

# Recurring biennial refresh (NAEP + CRDC)
./run-pool.sh 73

# Include edu initial in a full historical run
./run-pool.sh all                    # includes 71

# Run edu alongside other domain refreshes
./run-pool.sh 72-73                  # edu annual + biennial
./run-pool.sh -j 2 71                # edu initial with memory cap
```

### Environment Variables

Set these in `.env.prod` (or `scripts/parallel/env.sh`) before running:

| Variable | Required | Description |
|----------|----------|-------------|
| `EDU_PARQUET_DIR` | Recommended | Output Parquet path (falls back to `${GOVDATA_PARQUET_DIR}/source=edu`) |
| `EDU_CACHE_DIR` | Recommended | Raw download cache path (falls back to `${GOVDATA_CACHE_DIR}/edu`) |
| `COLLEGE_SCORECARD_API_KEY` | For scorecard tables | Free key at [api.data.gov/signup](https://api.data.gov/signup) |
| `EDU_CCD_SINCE_YEAR` | Optional | Incremental CCD load start year (blank = full fetch) |
| `EDU_IPEDS_SINCE_YEAR` | Optional | Incremental IPEDS start year (blank = full fetch) |
| `EDU_IPEDS_FINANCE_SINCE_YEAR` | Optional | Incremental IPEDS financials start year |
| `EDU_NAEP_SINCE_YEAR` | Optional | Incremental NAEP start year |
| `EDU_CRDC_SINCE_YEAR` | Optional | Incremental CRDC start year |
| `EDU_SCORECARD_SINCE_YEAR` | Optional | Incremental College Scorecard start year |

### Suggested Run-Pool Parameters

**Initial load (worker 71)** — large historical datasets, run once:
```bash
./run-pool.sh -t 240 -r 2000 71
# -t 240  allow up to 4 hours; IPEDS full history is slow
# -r 2000 reserve 2 GB for OS when running on a shared machine
```

**Annual refresh (worker 72)** — with since-year filtering set in .env.prod:
```bash
./run-pool.sh -t 90 72
# typically completes in 30-60 min with EDU_CCD_SINCE_YEAR set
```

**Biennial refresh (worker 73)** — small datasets, fast:
```bash
./run-pool.sh -t 60 73
```

**Concurrent with other domains** — edu initial runs at 4 GB max heap:
```bash
# Run edu initial alongside health recurring on a 16 GB machine
./run-pool.sh -r 2000 68-70,71
```

### Data Release Calendar

| Source | Typical Release | Tables |
|--------|----------------|--------|
| CCD Districts/Schools | July–August | `ccd_districts`, `ccd_schools` |
| IPEDS Institutions/Completions/Tuition | November | `ipeds_institutions`, `ipeds_completions`, `ipeds_tuition` |
| IPEDS Financials | January | `ipeds_financials` |
| College Scorecard | October | `college_scorecard`, `college_scorecard_programs` |
| NAEP Assessments | Odd years (Jan–Mar) | `naep_scores` |
| CRDC Civil Rights Data | Even-year surveys, released ~18 months later | `crdc_schools` |

---

## Implementation Notes

- **No API keys required.** CCD, CRDC, and IPEDS bulk CSVs are anonymous downloads.
  NAEP API is unauthenticated. College Scorecard API is unauthenticated for read access.
- **Year alignment.** K-12 uses school year end year (2023 = 2022-23 school year).
  IPEDS uses fiscal year. College Scorecard uses cohort entry year. Document clearly
  in column descriptions — do not normalize to a single convention.
- **CRDC is biennial** (even school years). Schema should handle sparse year coverage
  without breaking annual joins — use latest available year logic at query time.
- **IPEDS finance survey has two variants** (FASB for private, GASB for public).
  Normalize to a common schema with a `accounting_standard` flag column.
- **CCD bulk files change column names across vintages.** Parser must handle schema
  evolution — map historical column aliases to canonical names during ETL.
- **College Scorecard suppresses small cell sizes** with 'PrivacySuppressed'. Treat
  as NULL in the schema; document in column descriptions.
- **Module placement:** New `edu` directory under `govdata/src/main/resources/edu/`
  alongside existing domain directories. Schema file: `edu-schema.yaml`.
  Worker script: `worker-70.sh` (or next available number after reviewing manifest).
