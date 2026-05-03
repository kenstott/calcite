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
| Common Core of Data (CCD) | NCES / Dept of Ed | Urban Institute Education Data Portal API | Annual | 1986–present |
| CRDC | Dept of Ed | Urban Institute Education Data Portal API (multiple sub-endpoints) | Biennial | 2011–present |
| NAEP | NCES | api.nationsreportcard.gov (not resolvable from all environments) | Biennial | 1990–present |
| IPEDS HD/IC/C/F surveys | NCES | Bulk CSV at nces.ed.gov/ipeds/datacenter + Urban Institute API | Annual | 1986–present |
| College Scorecard | Dept of Ed | API requires api_key; bulk CSV via collegescorecard.ed.gov/data/ | Annual | 1996–present |

**Primary recommended API:** `https://educationdata.urban.org/api/v1/` — provides
CCD, CRDC, and IPEDS data with consistent JSON responses, no API key required.

**College Scorecard:** The API (`api.collegescorecard.ed.gov`) requires a free API key
from api.data.gov/signup. The data dictionary is publicly available at
`https://collegescorecard.ed.gov/assets/CollegeScorecardDataDictionary.xlsx`.

**NAEP:** The hostname `api.nationsreportcard.gov` may not be resolvable from all
network environments. NAEP bulk data files are also available via the NCES NAEP data
explorer at `https://nces.ed.gov/nationsreportcard/naepdata/`.

---

## Data Source Samples

This section records actual column names retrieved from live APIs and bulk CSV files.
All samples were fetched May 2026.

### CCD Districts — Urban Institute API
`GET https://educationdata.urban.org/api/v1/school-districts/ccd/directory/{year}/`

Actual fields returned (year=2019):
```
year, leaid, lea_name, fips, state_leaid, street_mailing, city_mailing, state_mailing,
zip_mailing, zip4_mailing, street_location, city_location, state_location, zip_location,
zip4_location, phone, latitude, longitude, urban_centric_locale, cbsa, cbsa_type, csa,
cmsa, necta, county_code, county_name, congress_district_id, bureau_indian_education,
supervisory_union_number, agency_type, agency_level, boundary_change_indicator,
agency_charter_indicator, lowest_grade_offered, highest_grade_offered, number_of_schools,
enrollment, spec_ed_students, english_language_learners, migrant_students,
teachers_prek_fte, teachers_kindergarten_fte, teachers_elementary_fte,
teachers_secondary_fte, teachers_ungraded_fte, teachers_total_fte,
instructional_aides_fte, coordinators_fte, guidance_counselors_elem_fte,
guidance_counselors_sec_fte, guidance_counselors_other_fte, guidance_counselors_total_fte,
librarian_specialists_fte, librarian_support_staff_fte, lea_administrators_fte,
lea_admin_support_staff_fte, school_administrators_fte, school_admin_support_staff_fte,
support_staff_students_fte, support_staff_other_fte, staff_total_fte, lea_staff_total_fte,
other_staff_fte, school_staff_total_fte, school_counselors_fte,
state_leg_district_lower, state_leg_district_upper, school_psychologists_fte,
support_staff_stu_wo_psych_fte
```

### CCD Schools — Urban Institute API
`GET https://educationdata.urban.org/api/v1/schools/ccd/directory/{year}/`

Actual fields returned (year=2019):
```
year, ncessch, school_id, school_name, leaid, lea_name, state_leaid, seasch,
street_mailing, city_mailing, state_mailing, zip_mailing, street_location, city_location,
state_location, zip_location, phone, fips, latitude, longitude, csa, cbsa,
urban_centric_locale, county_code, school_level, school_type, school_status,
lowest_grade_offered, highest_grade_offered, bureau_indian_education, title_i_status,
title_i_eligible, title_i_schoolwide, charter, magnet, shared_time, virtual, teachers_fte,
free_lunch, reduced_price_lunch, free_or_reduced_price_lunch, elem_cedp, high_cedp,
middle_cedp, ungrade_cedp, enrollment, state_leg_district_lower, state_leg_district_upper,
ncessch_num, congress_district_id, direct_certification, lunch_program
```

### NAEP Scores — api.nationsreportcard.gov
The API hostname was not resolvable during data collection. Based on NCES documentation,
the endpoint structure is:
`GET https://api.nationsreportcard.gov/api/naep/{year}/G{grade}/{subject}?fips={fips}&subscriberid=`

Expected JSON fields (from NCES API documentation):
```
Avg, SE, TestYear, Grade, Subject, JurisdictionName, Jurisdiction (fips code),
Scale, VariableType, Name, IsStatDisplayable, PercentBelowBasic, PercentAtBasic,
PercentAtProficient, PercentAtAdvanced, SampleSize
```

### CRDC Schools — Urban Institute API (multiple sub-endpoints)
The CRDC data is split across several Urban Institute API sub-endpoints.
Each school has a `crdc_id` (= `ncessch` for most records) and `year`.

**crdc/directory/{year}/** — school characteristics:
```
crdc_id, year, fips, leaid, ncessch, school_name_crdc, schoolid_crdc, lea_name,
leaid_crdc, lea_state, prek, k, g1–g12, ug, primarily_serve_students_w_dis,
charter_crdc, magnet_crdc, entire_school_magnet, alt_school, alt_school_focus,
ability_grouped_math_or_eng, ug_elementary_school, ug_middle_school, ug_high_school
```

**crdc/chronic-absenteeism/{year}/** — chronically absent students (disaggregated by race/sex/disability/lep):
```
crdc_id, ncessch, fips, leaid, year, race, sex, disability, lep, homeless,
students_chronically_absent
```

**crdc/offenses/{year}/** — school safety incidents:
```
crdc_id, year, ncessch, leaid, fips, firearm_incident_ind, homicide_ind,
rape_incidents, sexual_battery_incidents, robbery_w_weapon_incidents,
robbery_w_firearm_incidents, robbery_no_weapon_incidents, attack_w_weapon_incidents,
attack_w_firearm_incidents, attack_no_weapon_incidents, threats_w_weapon_incidents,
threats_w_firearm_incidents, threats_no_weapon_incidents, possession_firearm_incidents,
rape_bystudents, sexual_battery_bystudents, rape_bystaff, sexual_battery_bystaff,
alleg_rape_staff_resign, alleg_sexual_batt_staff_resign, alleg_rape_staff_resp,
alleg_sexual_batt_staff_resp, alleg_rape_staff_notresp, alleg_sexual_batt_staff_notresp,
alleg_rape_staff_pending, alleg_sexual_batt_staff_pending, alleg_rape_staff_reassign,
alleg_sexual_batt_staff_reassign
```

**crdc/teachers-staff/{year}/** — staffing data:
```
crdc_id, year, ncessch, leaid, fips, teachers_fte_crdc, teachers_certified_fte,
teachers_uncertified_fte, teachers_first_year_fte, teachers_second_year_fte,
teachers_current_sy, teachers_previous_sy, teachers_absent_fte, counselors_fte,
psychologists_fte, social_workers_fte, nurses_fte, security_guard_fte,
law_enforcement_fte, law_enforcement_ind, teachers_current_male, teachers_current_female
```

### IPEDS Institutions — NCES HD Survey (HD2022.zip) + Urban Institute API
`GET https://educationdata.urban.org/api/v1/college-university/ipeds/directory/{year}/`

Actual bulk CSV columns (hd2022.csv):
```
UNITID, INSTNM, IALIAS, ADDR, CITY, STABBR, ZIP, FIPS, OPEID, SECTOR, ICLEVEL,
CONTROL, HLOFFER, UGOFFER, GROFFER, DEGGRANT, HBCU, HOSPITAL, MEDICAL, TRIBAL,
LOCALE, LANDGRNT, INSTSIZE, CBSA, CBSATYPE, CSA, COUNTYCD, COUNTYNM, CNGDSTCD,
LONGITUD, LATITUDE, C21BASIC (Carnegie 2021), C18BASIC, C15BASIC, F1SYSNAM
```

Urban Institute API fields (directory/2020/):
```
year, unitid, inst_name, address, state_abbr, zip, phone_number, city, county_name,
offering_highest_level, region, inst_control, institution_level, sector, fips,
chief_admin_name, chief_admin_title, hbcu, primarily_postsecondary, hospital,
medical_degree, tribal_college, ein, urban_centric_locale, opeid, offering_highest_degree,
currently_active_ipeds, date_closed, duns, title_iv_indicator, offering_undergrad,
offering_grad, inst_status, newid, year_deleted, url_school, degree_granting,
open_public, postsec_public_active, postsec_public_active_title_iv, inst_system_flag,
inst_system_name, reporting_method, inst_category, land_grant, inst_size, inst_alias,
url_fin_aid, url_application, cbsa, cbsa_type, csa, necta, comparison_group,
longitude, latitude, county_fips, congress_district_id,
cc_basic_2021, cc_instruc_undergrad_2021, cc_instruc_grad_2021, cc_undergrad_2021,
cc_enroll_2021, cc_size_setting_2021, cc_basic_2018, cc_basic_2015, cc_basic_2010
```

Tuition and fees come from IC_AY survey (ic2022_ay.csv), NOT from HD:
```
UNITID, TUITION1 (in-district), TUITION2 (in-state), TUITION3 (out-of-state),
FEE1, FEE2, FEE3, CHG1AT0/1/2/3 (in-state charges, 4 prior years),
CHG3AT0/1/2/3 (out-of-state charges, 4 prior years)
```

### IPEDS Completions — NCES C Survey (C2022_A.zip)
Actual bulk CSV columns (c2022_a.csv):
```
UNITID, CIPCODE, MAJORNUM, AWLEVEL,
CTOTALT (total), CTOTALM (men), CTOTALW (women),
CAIANT (American Indian/Alaska Native total), CAIANM, CAIANW,
CASIAT (Asian total), CASIAM, CASIAW,
CBKAAT (Black non-Hispanic total), CBKAAM, CBKAAW,
CHISPT (Hispanic total), CHISPM, CHISPW,
CNHPIT (Native Hawaiian/Pacific Islander total), CNHPIM, CNHPIW,
CWHITT (White non-Hispanic total), CWHITM, CWHITW,
C2MORT (Two or more races total), C2MORM, C2MORW,
CUNKNT (unknown race total), CUNKNM, CUNKNW,
CNRALT (nonresident alien total), CNRALM, CNRALW
```
Note: Each data column has a corresponding `X` imputation flag (e.g., `XCTOTALT`).
`AWLEVEL` codes: 01=<1yr cert, 02=1-2yr cert, 03=Associate, 05=Bachelor's, 07=Master's,
17=Doctor's research, 18=Doctor's professional, 19=Doctor's other.

### IPEDS Financials — NCES F Survey (F2122_F1A.zip = GASB public institutions)
Key variables from data dictionary (f2122_f1a.xlsx):
```
UNITID
-- Revenues (F1B series) --
F1B01: Tuition and fees (net of discounts/allowances)
F1B02: Federal operating grants and contracts
F1B03: State operating grants and contracts
F1B04: Local/private operating grants and contracts
F1B09: Total operating revenues
F1B10: Federal appropriations
F1B11: State appropriations
F1B13: Federal nonoperating grants
F1B14: State nonoperating grants
F1B19: Total nonoperating revenues
F1B25: Total all revenues and other additions
-- Expenditures by function (F1C series) --
F1C011: Instruction (total)
F1C021: Research (total)
F1C031: Public service (total)
F1C051: Academic support (total)
F1C061: Student services (total)
F1C071: Institutional support (total)
F1C101: Scholarships and fellowships
F1C191: Total expenses and deductions
-- Endowment (F1H series) --
F1H01: Endowment assets beginning of year
F1H02: Endowment assets end of year
F1H03B: Endowment net investment return
-- Net position (F1A series) --
F1A01: Total current assets
F1A06: Total assets
F1A13: Total liabilities
F1A18: Net position
```
Note: F2 (F2122_F2.zip) covers FASB private non-profit institutions with different
variable names. A normalized `accounting_standard` flag is needed to distinguish them.

### College Scorecard — Data Dictionary (CollegeScorecardDataDictionary.xlsx)
The API uses dot-notation field names prefixed with `latest.` for current year.
Institution-level key fields (developer-friendly names):

**Identity/location:**
```
id (unitid), ope8_id, ope6_id, name, city, state, zip, state_fips, region_id,
location.lat, location.lon, locale, ownership, degrees_awarded.predominant,
degrees_awarded.highest, carnegie_basic, carnegie_undergrad, carnegie_size_setting
```

**Equity flags:**
```
minority_serving.historically_black, minority_serving.predominantly_black,
minority_serving.hispanic, minority_serving.tribal, men_only, women_only,
online_only, religious_affiliation
```

**Size/enrollment:**
```
size (total enrollment), enrollment.all
```

**Cost:**
```
cost.avg_net_price.public, cost.avg_net_price.private,
cost.tuition.in_state, cost.tuition.out_of_state
```

**Aid:**
```
aid.pell_grant_rate, aid.median_debt.completers.overall,
aid.median_debt.noncompleters, aid.median_debt.pell_grant
```

**Completion:**
```
completion.completion_rate_4yr_150nt, completion.completion_rate_less_than_4yr_150nt,
completion.completion_cohort_4yr_150nt
```

**Student demographics:**
```
student.demographics.median_family_income, student.demographics.median_hh_income,
student.retention_rate.four_year.full_time, student.retention_rate.lt_four_year.full_time
```

**Earnings:**
```
earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings,
earnings.10_yrs_after_entry.median_earnings.lowest_tercile,
earnings.6_yrs_after_entry.working_not_enrolled.mean_earnings,
earnings.8_yrs_after_entry.median_earnings
```

**Repayment/default:**
```
repayment.2_yr_default_rate, repayment.3_yr_default_rate
```

Program-level (FieldOfStudy) key fields:
```
programs.cip_4_digit.unit_id, programs.cip_4_digit.ope6_id,
programs.cip_4_digit.code (CIP code), programs.cip_4_digit.title,
programs.cip_4_digit.credential.level, programs.cip_4_digit.credential.title,
programs.cip_4_digit.counts.ipeds_awards1, programs.cip_4_digit.counts.ipeds_awards2,
programs.cip_4_digit.earnings.1_yr.overall_median_earnings,
programs.cip_4_digit.earnings.4_yr.overall_median_earnings,
programs.cip_4_digit.earnings.5_yr.overall_median_earnings
```

---

## Tables

### K-12

#### `ccd_districts`
Local Education Agency (district) directory. One row per LEA per year.

**Source:** Urban Institute API — `school-districts/ccd/directory/{year}/`
**Primary key:** `(leaid, year)`
**Join to geo:** `leaid = geo.school_districts.sd_lea`

Discrepancies from original plan:
- `state_fips` is called `fips` in the API (integer, not VARCHAR)
- `county_fips` is called `county_code` in the API (4-digit string, not 5-digit)
- `locale_code` is called `urban_centric_locale` (integer code)
- `total_students` is called `enrollment`
- `title1_status` does not exist at the district level (it is at the school level)
- `charter_lea` does not exist directly; `agency_charter_indicator` is the flag
- `teachers_fte` is split into `teachers_total_fte` (all teachers) and multiple grade-level FTE fields
- `per_pupil_expenditure`, `revenue_*_pct` are not in the directory endpoint; they come from the NCES F-33 survey (not in scope for this API)
- Many additional staff FTE fields exist (counselors, psychologists, librarians, etc.)

| Column | Type | Source Field | Description |
|---|---|---|---|
| leaid | VARCHAR | `leaid` | NCES LEA ID (7-digit string) |
| year | INTEGER | `year` | School year (end year, e.g. 2019 = 2018-19) |
| lea_name | VARCHAR | `lea_name` | District name |
| fips | INTEGER | `fips` | State FIPS code (integer 1-56) |
| state_abbr | VARCHAR | `state_location` | 2-letter state abbreviation |
| county_code | VARCHAR | `county_code` | 4-digit county FIPS (leading zero if needed → pad to 5) |
| county_name | VARCHAR | `county_name` | County name |
| urban_centric_locale | INTEGER | `urban_centric_locale` | NCES locale code (11=city large, 21=suburb large, 31=town fringe, 41=rural fringe, etc.) |
| cbsa | INTEGER | `cbsa` | Core-Based Statistical Area code |
| latitude | DOUBLE | `latitude` | LEA latitude |
| longitude | DOUBLE | `longitude` | LEA longitude |
| agency_type | INTEGER | `agency_type` | Agency type (1=local, 2=supervisory union, etc.) |
| agency_level | INTEGER | `agency_level` | Level (1=elementary, 2=secondary, 3=K-12, 4=other) |
| agency_charter_indicator | INTEGER | `agency_charter_indicator` | Charter LEA indicator (1=yes, 0=no, null=N/A) |
| lowest_grade_offered | INTEGER | `lowest_grade_offered` | Lowest grade (-1=PK, 0=K, 1-12) |
| highest_grade_offered | INTEGER | `highest_grade_offered` | Highest grade offered |
| number_of_schools | INTEGER | `number_of_schools` | Number of schools in LEA |
| enrollment | INTEGER | `enrollment` | Total student enrollment (may be null for some LEAs) |
| spec_ed_students | INTEGER | `spec_ed_students` | Special education students |
| english_language_learners | INTEGER | `english_language_learners` | ELL students |
| migrant_students | INTEGER | `migrant_students` | Migrant students |
| teachers_total_fte | DOUBLE | `teachers_total_fte` | Total FTE classroom teachers |
| guidance_counselors_total_fte | DOUBLE | `guidance_counselors_total_fte` | Total FTE guidance counselors |
| school_counselors_fte | DOUBLE | `school_counselors_fte` | FTE school counselors |
| school_psychologists_fte | DOUBLE | `school_psychologists_fte` | FTE school psychologists |
| librarian_specialists_fte | DOUBLE | `librarian_specialists_fte` | FTE librarian/media specialists |
| staff_total_fte | DOUBLE | `staff_total_fte` | Total FTE all staff |
| state_leg_district_lower | VARCHAR | `state_leg_district_lower` | State lower legislative district |
| state_leg_district_upper | VARCHAR | `state_leg_district_upper` | State upper legislative district |
| congress_district_id | INTEGER | `congress_district_id` | Congressional district ID |

---

#### `ccd_schools`
Individual school directory. One row per school per year.

**Source:** Urban Institute API — `schools/ccd/directory/{year}/`
**Primary key:** `(ncessch, year)`
**Join to district:** `leaid = ccd_districts.leaid`

Discrepancies from original plan:
- `school_name` is correct
- `state_fips` is called `fips` (integer)
- `county_fips` is called `county_code` (4-digit string)
- `locale_code` is called `urban_centric_locale` (integer)
- `total_students` is called `enrollment`
- `free_lunch_pct` does not exist; raw counts are `free_lunch`, `reduced_price_lunch`, `free_or_reduced_price_lunch` — must compute percentage
- `grade_low`/`grade_high` are called `lowest_grade_offered`/`highest_grade_offered` (integer codes)
- `school_type` is an integer code (1=regular, 2=special ed, 3=vocational, 4=alternative)
- `charter` and `magnet` are integer 0/1 flags, not BOOLEAN
- `title1_status`, `title_i_eligible`, `title_i_schoolwide` — three separate fields
- Additional fields: `school_level`, `school_status`, `virtual`, `shared_time`, `direct_certification`

| Column | Type | Source Field | Description |
|---|---|---|---|
| ncessch | VARCHAR | `ncessch` | NCES school ID (12-digit string) |
| leaid | VARCHAR | `leaid` | Parent LEA ID (7-digit) |
| year | INTEGER | `year` | School year end year |
| school_name | VARCHAR | `school_name` | School name |
| fips | INTEGER | `fips` | State FIPS (integer) |
| county_code | VARCHAR | `county_code` | 4-digit county FIPS |
| latitude | DOUBLE | `latitude` | School latitude |
| longitude | DOUBLE | `longitude` | School longitude |
| urban_centric_locale | INTEGER | `urban_centric_locale` | NCES locale code |
| cbsa | VARCHAR | `cbsa` | CBSA code (string in API) |
| school_level | INTEGER | `school_level` | 1=elementary, 2=middle, 3=high, 4=other |
| school_type | INTEGER | `school_type` | 1=regular, 2=special ed, 3=vocational, 4=alternative |
| school_status | INTEGER | `school_status` | 1=open, 2=closed, 3=new, etc. |
| lowest_grade_offered | INTEGER | `lowest_grade_offered` | Lowest grade (-1=PK, 0=K, 1-12) |
| highest_grade_offered | INTEGER | `highest_grade_offered` | Highest grade offered |
| charter | INTEGER | `charter` | Charter school (1=yes, 0=no) |
| magnet | INTEGER | `magnet` | Magnet school (1=yes, 0=no) |
| virtual | INTEGER | `virtual` | Virtual school indicator |
| shared_time | INTEGER | `shared_time` | Shared-time school indicator |
| title_i_status | INTEGER | `title_i_status` | Title I status code |
| title_i_eligible | INTEGER | `title_i_eligible` | Title I eligibility (1=yes) |
| title_i_schoolwide | INTEGER | `title_i_schoolwide` | Schoolwide Title I (1=yes) |
| teachers_fte | DOUBLE | `teachers_fte` | FTE classroom teachers |
| enrollment | INTEGER | `enrollment` | Total student enrollment |
| free_lunch | INTEGER | `free_lunch` | Students approved for free lunch |
| reduced_price_lunch | INTEGER | `reduced_price_lunch` | Students approved for reduced-price lunch |
| free_or_reduced_price_lunch | INTEGER | `free_or_reduced_price_lunch` | Combined free/reduced lunch count |
| direct_certification | INTEGER | `direct_certification` | Directly certified students |
| lunch_program | INTEGER | `lunch_program` | Lunch program type code |
| bureau_indian_education | INTEGER | `bureau_indian_education` | BIE school indicator |
| congress_district_id | INTEGER | `congress_district_id` | Congressional district |
| state_leg_district_lower | VARCHAR | `state_leg_district_lower` | State lower legislative district |
| state_leg_district_upper | VARCHAR | `state_leg_district_upper` | State upper legislative district |

---

#### `naep_scores`
NAEP assessment results by state and subject. Biennial. Nation's Report Card.

**Source:** NCES NAEP API — `api.nationsreportcard.gov/api/naep/{year}/G{grade}/{subject}`
**Note:** The API hostname was not resolvable during live testing. The endpoint structure
is documented by NCES; the `fips=0` parameter returns national results.
**Primary key:** `(state_fips, year, subject, grade, subgroup)`

Discrepancies from original plan:
- API field names use camelCase; the plan used snake_case canonicalized names
- `state_fips` is called `Jurisdiction` (integer FIPS code) in the API, with `JurisdictionName` for the state name
- `avg_score` is called `Avg` in the API
- `pct_below_basic`, `pct_basic`, `pct_proficient`, `pct_advanced` are called `PercentBelowBasic`, `PercentAtBasic`, `PercentAtProficient`, `PercentAtAdvanced`
- `sample_size` is `SampleSize`
- `subgroup` is encoded via `VariableType` + `Name` fields
- Subject codes are `MAT` (mathematics) and `RED` (reading), not plain English in the URL

| Column | Type | Source Field | Description |
|---|---|---|---|
| jurisdiction | INTEGER | `Jurisdiction` | State FIPS code (0 = national) |
| jurisdiction_name | VARCHAR | `JurisdictionName` | State name or "National" |
| year | INTEGER | `TestYear` | Assessment year |
| subject | VARCHAR | `Subject` | MAT=mathematics, RED=reading |
| grade | INTEGER | `Grade` | 4 or 8 |
| variable_type | VARCHAR | `VariableType` | Disaggregation type (e.g., "TOTAL", "Race/Ethnicity", "Gender") |
| subgroup_name | VARCHAR | `Name` | Subgroup label (e.g., "All students", "Male", "White") |
| avg_score | DOUBLE | `Avg` | Average scale score |
| std_error | DOUBLE | `SE` | Standard error of the mean |
| pct_below_basic | DOUBLE | `PercentBelowBasic` | % below basic proficiency |
| pct_basic | DOUBLE | `PercentAtBasic` | % at basic |
| pct_proficient | DOUBLE | `PercentAtProficient` | % at proficient |
| pct_advanced | DOUBLE | `PercentAtAdvanced` | % at advanced |
| sample_size | INTEGER | `SampleSize` | Number of students assessed |
| is_displayable | INTEGER | `IsStatDisplayable` | 1 if statistically displayable |

---

#### `crdc_schools`
Civil Rights Data Collection — equity and access metrics by school. Biennial.
The CRDC is split across multiple Urban Institute API sub-endpoints; the schema
consolidates the most analytically useful fields into a single table.

**Source:** Urban Institute API — multiple `schools/crdc/{topic}/{year}/` endpoints
**Primary key:** `(crdc_id, year)`
**Join to school:** `ncessch = ccd_schools.ncessch` (crdc_id = ncessch for most records)
**Available years:** 2011, 2013, 2015, 2017 (biennial, even school years)

Sub-endpoints confirmed working:
- `crdc/directory/{year}/` — school attributes (charter, magnet, grades offered)
- `crdc/chronic-absenteeism/{year}/` — chronic absenteeism by race/sex/disability/lep
- `crdc/offenses/{year}/` — school safety incidents (firearms, assault, sexual battery)
- `crdc/teachers-staff/{year}/` — staffing FTE (teachers, counselors, psychologists)

Discrepancies from original plan:
- `ap_offered`, `ap_enrollment`, `ib_offered`, `gifted_enrollment` are not in confirmed working endpoints (AP/IB data is under a separate sub-endpoint not yet confirmed)
- `chronic_absent_pct` is not a percentage in the API — it is a count (`students_chronically_absent`) disaggregated by race/sex/disability/lep; the table must aggregate
- `in_school_suspensions`, `out_school_suspensions`, `expulsions` are in separate sub-endpoints; working endpoint is `crdc/offenses/` which contains incident types, not suspension counts
- `harassment_sex`, `harassment_race` are not direct fields — offenses endpoint contains `rape_incidents`, `sexual_battery_incidents` and staffing endpoint has law enforcement FTE
- `counselors_fte` and `psychologists_fte` come from `crdc/teachers-staff/` endpoint
- The CRDC uses `crdc_id` as its primary key (not `ncessch`), though they are equal for most schools

| Column | Type | Source Field | Description |
|---|---|---|---|
| crdc_id | VARCHAR | `crdc_id` | CRDC school ID (equals ncessch for most records) |
| ncessch | VARCHAR | `ncessch` | NCES school ID for joining to ccd_schools |
| leaid | VARCHAR | `leaid` | LEA ID |
| fips | INTEGER | `fips` | State FIPS |
| year | INTEGER | `year` | Survey year (biennial: 2011, 2013, 2015, 2017) |
| charter_crdc | INTEGER | `charter_crdc` | Charter flag per CRDC (may differ from CCD) |
| magnet_crdc | INTEGER | `magnet_crdc` | Magnet flag per CRDC |
| alt_school | INTEGER | `alt_school` | Alternative school |
| primarily_serve_students_w_dis | INTEGER | `primarily_serve_students_w_dis` | School primarily serves students with disabilities |
| students_chronically_absent | INTEGER | `students_chronically_absent` | Count of chronically absent students (aggregate across disaggregations) |
| firearm_incident_ind | INTEGER | `firearm_incident_ind` | Any firearm incident (0/1) |
| homicide_ind | DOUBLE | `homicide_ind` | Homicide indicator |
| rape_incidents | DOUBLE | `rape_incidents` | Rape allegations count |
| sexual_battery_incidents | DOUBLE | `sexual_battery_incidents` | Sexual battery incidents |
| attack_w_weapon_incidents | DOUBLE | `attack_w_weapon_incidents` | Attacks with weapon |
| attack_no_weapon_incidents | DOUBLE | `attack_no_weapon_incidents` | Attacks without weapon |
| possession_firearm_incidents | DOUBLE | `possession_firearm_incidents` | Firearm possession incidents |
| teachers_fte_crdc | DOUBLE | `teachers_fte_crdc` | Total FTE teachers (CRDC-reported) |
| teachers_certified_fte | DOUBLE | `teachers_certified_fte` | FTE certified teachers |
| teachers_uncertified_fte | DOUBLE | `teachers_uncertified_fte` | FTE uncertified teachers |
| teachers_first_year_fte | DOUBLE | `teachers_first_year_fte` | FTE first-year teachers |
| teachers_absent_fte | DOUBLE | `teachers_absent_fte` | FTE teachers chronically absent |
| counselors_fte | DOUBLE | `counselors_fte` | FTE school counselors |
| psychologists_fte | DOUBLE | `psychologists_fte` | FTE school psychologists |
| social_workers_fte | DOUBLE | `social_workers_fte` | FTE social workers |
| nurses_fte | DOUBLE | `nurses_fte` | FTE nurses |
| security_guard_fte | DOUBLE | `security_guard_fte` | FTE security guards |
| law_enforcement_fte | DOUBLE | `law_enforcement_fte` | FTE sworn law enforcement |
| law_enforcement_ind | INTEGER | `law_enforcement_ind` | Any law enforcement present (0/1) |

---

### Higher Education

#### `ipeds_institutions`
Institution directory. One row per institution per year.

**Source:** NCES IPEDS HD survey (hd{year}.csv) + Urban Institute API `college-university/ipeds/directory/{year}/`
**Tuition/fees source:** NCES IPEDS IC_AY survey (ic{year}_ay.csv) — joined by UNITID
**Primary key:** `(unitid, year)`
**Join to geo:** `county_fips = geo.counties.county_fips`; lat/lon for spatial joins

Discrepancies from original plan:
- `institution_name` is `INSTNM` in bulk CSV, `inst_name` in Urban Institute API
- `state_fips` is `FIPS` in bulk CSV (integer), `fips` in API (integer)
- `county_fips` is `COUNTYCD` in bulk CSV (integer), `county_fips` in API (integer) — pad to 5 digits
- `carnegie_class` is `C21BASIC` (most recent), `C18BASIC`, etc. (integer code, not string)
- `historically_black` is `HBCU` in bulk CSV (integer 1/0)
- `tribal_college` is `TRIBAL` in bulk CSV (integer 1/0)
- `total_enrollment`, `undergrad_enrollment`, `grad_enrollment` are NOT in the HD file; they come from the EF (enrollment) survey
- `full_time_retention_rate` is in the GR/SFA survey, not HD
- `graduation_rate_6yr` is in the GR200 survey, not HD
- `tuition_in_state` = `TUITION2` in IC_AY, `tuition_out_state` = `TUITION3`
- Urban Institute API does not expose enrollment or tuition directly from the directory endpoint; use `ipeds/fall-enrollment/` for FTE

| Column | Type | Source Field | Description |
|---|---|---|---|
| unitid | INTEGER | `UNITID` / `unitid` | IPEDS unit ID (primary key) |
| opeid | VARCHAR | `OPEID` / `opeid` | OPE ID (8-digit, links to College Scorecard `ope8_id`) |
| year | INTEGER | — | Survey year (derived from file name) |
| inst_name | VARCHAR | `INSTNM` / `inst_name` | Institution name |
| address | VARCHAR | `ADDR` / `address` | Street address |
| city | VARCHAR | `CITY` / `city` | City |
| state_abbr | VARCHAR | `STABBR` / `state_abbr` | State abbreviation |
| zip | VARCHAR | `ZIP` / `zip` | ZIP code |
| fips | INTEGER | `FIPS` / `fips` | State FIPS (integer) |
| county_fips | INTEGER | `COUNTYCD` / `county_fips` | County FIPS (integer, 5-digit with leading zeros when formatted) |
| county_name | VARCHAR | `COUNTYNM` / `county_name` | County name |
| longitude | DOUBLE | `LONGITUD` / `longitude` | Longitude |
| latitude | DOUBLE | `LATITUDE` / `latitude` | Latitude |
| sector | INTEGER | `SECTOR` / `sector` | 0=admin, 1=public 4yr, 2=private nonprofit 4yr, 3=private for-profit 4yr, 4=public 2yr, 5=private nonprofit 2yr, 6=private for-profit 2yr, etc. |
| inst_control | INTEGER | `CONTROL` / `inst_control` | 1=public, 2=private nonprofit, 3=private for-profit |
| institution_level | INTEGER | `ICLEVEL` / `institution_level` | 1=4yr, 2=2yr, 3=<2yr |
| hbcu | INTEGER | `HBCU` / `hbcu` | HBCU flag (1=yes, 0=no) |
| hospital | INTEGER | `HOSPITAL` / `hospital` | Hospital flag |
| medical_degree | INTEGER | `MEDICAL` / `medical_degree` | Medical degree offered |
| tribal_college | INTEGER | `TRIBAL` / `tribal_college` | Tribal college flag |
| land_grant | INTEGER | `LANDGRNT` / `land_grant` | Land grant institution |
| inst_size | INTEGER | `INSTSIZE` / `inst_size` | 1=<1000, 2=1000-4999, 3=5000-9999, 4=10000-19999, 5=20000+ |
| degree_granting | INTEGER | `DEGGRANT` / `degree_granting` | Degree-granting (1=yes) |
| title_iv_indicator | INTEGER | `OPEFLAG` / `title_iv_indicator` | Title IV eligible |
| offering_undergrad | INTEGER | `UGOFFER` / `offering_undergrad` | Undergraduate programs offered |
| offering_grad | INTEGER | `GROFFER` / `offering_grad` | Graduate programs offered |
| cc_basic_2021 | INTEGER | `C21BASIC` / `cc_basic_2021` | Carnegie Classification basic 2021 |
| cbsa | INTEGER | `CBSA` / `cbsa` | CBSA code |
| cbsa_type | INTEGER | `CBSATYPE` / `cbsa_type` | Metro/micro area type |
| inst_alias | VARCHAR | `IALIAS` / `inst_alias` | Institution alias/common name |
| url_school | VARCHAR | `WEBADDR` / `url_school` | Institution website |
| -- from IC_AY survey -- | | | |
| tuition_in_state | INTEGER | `TUITION2` | In-state undergraduate tuition (annual) |
| tuition_out_state | INTEGER | `TUITION3` | Out-of-state undergraduate tuition (annual) |
| tuition_in_district | INTEGER | `TUITION1` | In-district tuition (community colleges) |
| fees_in_state | INTEGER | `FEE2` | In-state required fees |
| fees_out_state | INTEGER | `FEE3` | Out-of-state required fees |

---

#### `ipeds_completions`
Degrees and certificates awarded by field of study.

**Source:** NCES IPEDS C survey — `c{year}_a.csv` (e.g., c2022_a.csv)
**Primary key:** `(unitid, year, cipcode, majornum, awlevel)`
**Note:** `majornum` distinguishes first major (1) from second major (2) programs.

Discrepancies from original plan:
- `cip_title` is NOT in this file — it must be joined from a CIP code reference table
- `award_level` is an integer code (`AWLEVEL`) not a string description
- Race/ethnicity columns use NCES codes: AIANT=American Indian/Alaska Native, ASIAT=Asian, BKAAT=Black, HISPT=Hispanic, NHPIT=Native Hawaiian/Pac Islander, WHITT=White, 2MORT=Two or more, UNKNT=Unknown, NRALT=Nonresident alien
- Each demographic column has T (total), M (men), W (women) variants
- Each data column has a corresponding X imputation flag (e.g., XCTOTALT)

| Column | Type | Source Field | Description |
|---|---|---|---|
| unitid | INTEGER | `UNITID` | IPEDS unit ID |
| year | INTEGER | — | Award year (derived from file name) |
| cipcode | VARCHAR | `CIPCODE` | CIP field of study code (e.g., "01.0999") |
| majornum | INTEGER | `MAJORNUM` | 1=first major, 2=second major |
| awlevel | INTEGER | `AWLEVEL` | Award level (01=<1yr cert, 02=1-2yr cert, 03=Associate, 05=Bachelor's, 07=Master's, 17/18/19=Doctoral) |
| ctotalt | INTEGER | `CTOTALT` | Total completions all students |
| ctotalm | INTEGER | `CTOTALM` | Total completions men |
| ctotalw | INTEGER | `CTOTALW` | Total completions women |
| caiant | INTEGER | `CAIANT` | American Indian/Alaska Native total |
| casiat | INTEGER | `CASIAT` | Asian total |
| cbkaat | INTEGER | `CBKAAT` | Black non-Hispanic total |
| chispt | INTEGER | `CHISPT` | Hispanic total |
| cnhpit | INTEGER | `CNHPIT` | Native Hawaiian/Pac Islander total |
| cwhitt | INTEGER | `CWHITT` | White non-Hispanic total |
| c2mort | INTEGER | `C2MORT` | Two or more races total |
| cunknt | INTEGER | `CUNKNT` | Unknown race total |
| cnralt | INTEGER | `CNRALT` | Nonresident alien total |

---

#### `ipeds_financials`
Institution revenues and expenditures.

**Source:** NCES IPEDS F survey:
  - GASB (public institutions): `f{year1}{year2}_f1a.csv`
  - FASB (private institutions): `f{year1}{year2}_f2.csv`
**Primary key:** `(unitid, year)`
**Note:** GASB and FASB have different variable naming schemes. The canonical schema
uses human-readable aliases. An `accounting_standard` flag distinguishes them.

Discrepancies from original plan:
- All column names in the actual files are coded (F1A01, F1B01, etc.) not human-readable
- `total_revenue` = F1D01 (GASB total revenues+additions), or F2D01 (FASB)
- `tuition_revenue` = F1B01 (net of discounts), not gross tuition
- `federal_grants` = F1B02 (operating) + F1B13 (nonoperating)
- `state_grants` = F1B03 + F1B14
- `endowment_revenue` does not map directly; F1H03B = endowment net investment return
- `net_tuition_revenue` = F1B01 already (it is net of discounts per data dictionary)
- `endowment_assets_eoy` = F1H02
- Column name `student_services_expenditure` = F1C061
- Column name `institutional_support_expenditure` = F1C071

| Column | Type | Source Field (GASB) | Source Field (FASB) | Description |
|---|---|---|---|---|
| unitid | INTEGER | `UNITID` | `UNITID` | IPEDS unit ID |
| year | INTEGER | — | — | Fiscal year end (derived from file name) |
| accounting_standard | VARCHAR | 'GASB' | 'FASB' | GASB=public, FASB=private |
| total_revenues | BIGINT | `F1D01` | `F2D01` | Total revenues and other additions |
| tuition_revenue_net | BIGINT | `F1B01` | `F2B01` | Tuition/fees net of discounts and allowances |
| federal_operating_grants | BIGINT | `F1B02` | `F2B02` | Federal operating grants and contracts |
| state_operating_grants | BIGINT | `F1B03` | `F2B03` | State operating grants and contracts |
| total_operating_revenues | BIGINT | `F1B09` | — | Total operating revenues (GASB only) |
| federal_appropriations | BIGINT | `F1B10` | — | Federal appropriations (GASB only) |
| state_appropriations | BIGINT | `F1B11` | `F2B11` | State appropriations |
| federal_nonoperating_grants | BIGINT | `F1B13` | — | Federal nonoperating grants (Pell, etc.) |
| state_nonoperating_grants | BIGINT | `F1B14` | — | State nonoperating grants |
| total_all_revenues | BIGINT | `F1B25` | `F2D01` | Grand total all revenues |
| instruction_expenditure | BIGINT | `F1C011` | `F2E011` | Instruction expenditures |
| research_expenditure | BIGINT | `F1C021` | `F2E021` | Research expenditures |
| public_service_expenditure | BIGINT | `F1C031` | `F2E031` | Public service expenditures |
| academic_support_expenditure | BIGINT | `F1C051` | `F2E051` | Academic support |
| student_services_expenditure | BIGINT | `F1C061` | `F2E061` | Student services |
| institutional_support_expenditure | BIGINT | `F1C071` | `F2E071` | Institutional support |
| scholarships_fellowships | BIGINT | `F1C101` | `F2E101` | Scholarships and fellowships |
| total_expenditure | BIGINT | `F1C191` | `F2E191` | Total expenses and deductions |
| endowment_assets_beg | BIGINT | `F1H01` | `F2H01` | Endowment assets beginning of year |
| endowment_assets_eoy | BIGINT | `F1H02` | `F2H02` | Endowment assets end of year |
| endowment_investment_return | BIGINT | `F1H03B` | `F2H03B` | Endowment net investment return |
| total_assets | BIGINT | `F1A06` | `F2A06` | Total assets |
| total_liabilities | BIGINT | `F1A13` | `F2A13` | Total liabilities |
| net_position | BIGINT | `F1A18` | `F2A18` | Net position / net assets |

---

#### `college_scorecard`
Post-graduation outcomes by institution and field of study.

**Source:** Dept of Ed College Scorecard API — `api.collegescorecard.ed.gov/v1/schools.json`
**API key required:** Free registration at api.data.gov/signup
**Data dictionary:** `https://collegescorecard.ed.gov/assets/CollegeScorecardDataDictionary.xlsx`
**Primary key (institution):** `(id, year)` where `id` = IPEDS unitid
**Primary key (program):** `(unit_id, ope6_id, cip_4_digit.code, cip_4_digit.credential.level, year)`

Discrepancies from original plan:
- `opeid` in API is `ope8_id` (8-digit) or `ope6_id` (6-digit) — not a single `opeid`
- `unitid` is `id` in the API (integer)
- `median_earnings_10yr` is `earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings` (mean, not median); true median is `earnings.8_yrs_after_entry.median_earnings` or `earnings.10_yrs_after_entry.median_earnings.lowest_tercile`
- `median_debt_graduates` = `aid.median_debt.completers.overall`
- `median_debt_withdrawers` = `aid.median_debt.noncompleters`
- `completion_rate` = `completion.completion_rate_4yr_150nt` (4yr) or `completion.completion_rate_less_than_4yr_150nt` (2yr)
- `pell_pct` = `aid.pell_grant_rate`
- `first_gen_pct` does not have a direct percentage field; first-gen data is in the completion cohort tables
- `median_family_income` = `student.demographics.median_family_income`
- `mean_earnings_10yr` = `earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings`
- `cip_code`/`cip_title` in program-level are `programs.cip_4_digit.code` and `.title`
- `PrivacySuppressed` values are returned as null

**Institution-level table:**

| Column | Type | API Field | Description |
|---|---|---|---|
| id | INTEGER | `id` | IPEDS unit ID (joins to ipeds_institutions.unitid) |
| ope8_id | VARCHAR | `ope8_id` | OPE ID 8-digit (joins to ipeds_institutions.opeid) |
| ope6_id | VARCHAR | `ope6_id` | OPE ID 6-digit |
| year | INTEGER | — | Cohort entry year (derived from API vintage) |
| name | VARCHAR | `school.name` | Institution name |
| city | VARCHAR | `school.city` | City |
| state | VARCHAR | `school.state` | State abbreviation |
| ownership | INTEGER | `school.ownership` | 1=public, 2=private nonprofit, 3=private for-profit |
| carnegie_basic | INTEGER | `school.carnegie_basic` | Carnegie basic classification code |
| hbcu | INTEGER | `school.minority_serving.historically_black` | HBCU flag |
| tribal | INTEGER | `school.minority_serving.tribal` | Tribal college flag |
| hispanic_serving | INTEGER | `school.minority_serving.hispanic` | Hispanic-serving institution |
| online_only | INTEGER | `school.online_only` | Online-only institution |
| locale | INTEGER | `school.locale` | NCES locale code |
| size | INTEGER | `latest.student.size` | Total enrollment |
| pell_grant_rate | DOUBLE | `latest.student.aid.pell_grant_rate` | Share of students receiving Pell grants |
| completion_rate_4yr | DOUBLE | `latest.completion.completion_rate_4yr_150nt` | 6-year completion rate (150% time) for 4yr institutions |
| completion_rate_2yr | DOUBLE | `latest.completion.completion_rate_less_than_4yr_150nt` | Completion rate for <4yr institutions |
| retention_rate_fulltime | DOUBLE | `latest.student.retention_rate.four_year.full_time` | Full-time retention rate (4yr) |
| median_debt_completers | DOUBLE | `latest.aid.median_debt.completers.overall` | Median debt at completion |
| median_debt_noncompleters | DOUBLE | `latest.aid.median_debt.noncompleters` | Median debt for non-completers |
| mean_earnings_10yr | INTEGER | `latest.earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings` | Mean earnings 10yr after entry |
| median_earnings_8yr | DOUBLE | `latest.earnings.8_yrs_after_entry.median_earnings` | Median earnings 8yr after entry |
| median_family_income | INTEGER | `latest.student.demographics.median_family_income` | Median family income of entering students |
| default_rate_3yr | DOUBLE | `latest.repayment.3_yr_default_rate` | 3-year student loan default rate |
| avg_net_price_public | INTEGER | `latest.cost.avg_net_price.public` | Average net price for public institutions |
| avg_net_price_private | INTEGER | `latest.cost.avg_net_price.private` | Average net price for private institutions |
| tuition_in_state | INTEGER | `latest.cost.tuition.in_state` | In-state tuition |
| tuition_out_state | INTEGER | `latest.cost.tuition.out_of_state` | Out-of-state tuition |

**Program-level table (`college_scorecard_programs`):**

| Column | Type | API Field | Description |
|---|---|---|---|
| unit_id | INTEGER | `programs.cip_4_digit.unit_id` | IPEDS unit ID |
| ope6_id | VARCHAR | `programs.cip_4_digit.ope6_id` | OPE ID 6-digit |
| year | INTEGER | — | Data vintage year |
| cip_code | VARCHAR | `programs.cip_4_digit.code` | 4-digit CIP code |
| cip_title | VARCHAR | `programs.cip_4_digit.title` | CIP title |
| credential_level | INTEGER | `programs.cip_4_digit.credential.level` | Credential level code |
| credential_title | VARCHAR | `programs.cip_4_digit.credential.title` | Credential description |
| ipeds_awards | INTEGER | `programs.cip_4_digit.counts.ipeds_awards1` | IPEDS awards count (completions) |
| median_earnings_1yr | INTEGER | `programs.cip_4_digit.earnings.1_yr.overall_median_earnings` | Median earnings 1yr after completion |
| median_earnings_4yr | INTEGER | `programs.cip_4_digit.earnings.4_yr.overall_median_earnings` | Median earnings 4yr after completion |
| median_earnings_5yr | INTEGER | `programs.cip_4_digit.earnings.5_yr.overall_median_earnings` | Median earnings 5yr after completion |
| pell_median_earnings_1yr | INTEGER | `programs.cip_4_digit.earnings.1_yr.pell_median_earnings` | Pell grant recipient median earnings 1yr |
| nonpell_median_earnings_1yr | INTEGER | `programs.cip_4_digit.earnings.1_yr.nonpell_median_earnings` | Non-Pell median earnings 1yr |

---

## Cross-Schema Research Queries Enabled

```sql
-- Achievement gap by district poverty level
SELECT d.lea_name, d.enrollment,
       n.avg_score, s.free_or_reduced_price_lunch * 1.0 / NULLIF(s.enrollment, 0) AS frpl_rate
FROM ccd_districts d
JOIN geo.school_districts g ON d.leaid = g.sd_lea
JOIN naep_scores n ON d.fips = n.jurisdiction AND d.year = n.year
JOIN (SELECT leaid, sum(free_or_reduced_price_lunch) AS free_or_reduced_price_lunch,
             sum(enrollment) AS enrollment
      FROM ccd_schools GROUP BY leaid) s ON s.leaid = d.leaid
WHERE n.grade = 8 AND n.subject = 'MAT' AND n.variable_type = 'TOTAL';

-- College outcomes by county economic conditions
SELECT i.inst_name, i.county_fips,
       sc.mean_earnings_10yr, w.annual_avg_weekly_wage
FROM ipeds_institutions i
JOIN college_scorecard sc ON i.unitid = sc.id
JOIN econ.county_wages w ON LPAD(CAST(i.county_fips AS VARCHAR), 5, '0') = w.area_fips
WHERE i.year = 2022 AND sc.year = 2022;

-- For-profit college SEC filings joined to student outcomes
SELECT fm.company_name, sc.completion_rate_4yr, sc.median_debt_completers,
       fli.value_numeric AS revenue
FROM sec.filing_metadata fm
JOIN ipeds_institutions i ON lower(fm.company_name) LIKE '%' || lower(i.inst_name) || '%'
JOIN college_scorecard sc ON i.unitid = sc.id
JOIN sec.financial_line_items fli ON fm.cik = fli.cik
WHERE fli.concept LIKE '%Revenue%' AND fm.filing_type = '10-K'
  AND i.inst_control = 3;  -- private for-profit only

-- Chronically absent students by school, joined to school resources
SELECT c.ncessch, c.students_chronically_absent,
       cs.counselors_fte, cs.teachers_fte_crdc, s.enrollment
FROM crdc_schools c
JOIN crdc_schools cs ON c.ncessch = cs.ncessch AND c.year = cs.year
JOIN ccd_schools s ON c.ncessch = s.ncessch AND c.year = s.year
WHERE c.year = 2015;
```

---

## Implementation Notes

- **API key for College Scorecard.** The API requires a free key from api.data.gov.
  Store in env var `COLLEGE_SCORECARD_API_KEY`. Without a key it returns `API_KEY_MISSING`.
- **NAEP network access.** The hostname `api.nationsreportcard.gov` is not resolvable
  from all networks (was unreachable during development). Implement with a fallback to
  cached/bulk data. No API key is required if the host is reachable.
- **Year alignment.** K-12 CCD uses school year end year (2019 API = 2018-19 school year).
  IPEDS uses fiscal year. College Scorecard uses cohort entry year. Document clearly
  in column descriptions — do not normalize to a single convention.
- **CRDC is biennial** (odd NCES years: 2011, 2013, 2015, 2017). Schema should handle
  sparse year coverage without breaking annual joins — use latest available year logic.
- **CRDC is multi-endpoint.** Do not try to load all CRDC data from a single API path.
  The Urban Institute splits it into: directory, chronic-absenteeism, offenses,
  teachers-staff, and additional topic endpoints. Confirmed working pattern:
  `GET https://educationdata.urban.org/api/v1/schools/crdc/{topic}/{year}/?limit=1000`
- **IPEDS finance survey has two variants** (GASB for public, FASB for private).
  File naming: `f{y1}{y2}_f1a.csv` = GASB, `f{y1}{y2}_f2.csv` = FASB. Both needed
  to cover all institutions. Use `accounting_standard` flag to track provenance.
- **IPEDS tuition is NOT in the HD file.** It is in IC_AY (`ic{year}_ay.csv`).
  Key columns: TUITION1 (in-district), TUITION2 (in-state), TUITION3 (out-of-state).
- **IPEDS completions have imputation flags.** Every data column (e.g., CTOTALT) has
  a corresponding flag column (XCTOTALT) with value "R" (reported), "Z" (zero not imputed),
  or missing. The ETL should preserve or surface these flags.
- **CCD bulk CSV column names change across vintages.** The Urban Institute API
  normalizes these; prefer the API over direct bulk CSV for consistency.
  If using bulk CSV directly, validate column names match expected schema.
- **College Scorecard suppresses small cell sizes** with `PrivacySuppressed`.
  Treat as NULL in the schema.
- **county_code padding.** The CCD API returns `county_code` as a 4-digit string
  (e.g., "1073"). Pad to 5 digits with leading zero to match geo.counties.county_fips
  format (e.g., "01073"). Same applies to IPEDS `COUNTYCD`.
- **Module placement:** New `edu` directory under `govdata/src/main/resources/edu/`
  alongside existing domain directories. Schema file: `edu-schema.yaml`.
  Worker script: `worker-70.sh` (or next available number after reviewing manifest).
