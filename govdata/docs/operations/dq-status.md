# GovData DQ Status

Last updated: 2026-05-15

## How to Read This

- **PASS** — all tests pass
- **WARN** — no failures; at least one warning (acceptable thresholds exceeded)
- **FAIL** — one or more hard failures
- **PENDING** — DQ not yet run or rebuild in progress
- **STALE** — last run > 30 days ago

Results are stored at: `r2:govdata-tracker-v1/dq-results/schema={schema}/run_date=.../type=.../results.parquet`

To re-query any schema:
```bash
duckdb -c "SELECT table_name, test, status, value, detail \
  FROM iceberg_scan('s3://govdata-parquet-v1/...') \
  WHERE status != 'pass';"
```

---

## Schema Status

| Schema       | Last Run   | Result  | Fails | Warns | Notes |
|--------------|------------|---------|-------|-------|-------|
| weather      | 2026-05-11 | WARN    | 0     | 2     | See details below |
| edu          | 2026-05-13 | WARN    | 0     | 2     | See details below |
| census       | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| econ         | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| crime        | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| geo          | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| fec          | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| fedregister  | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| lands        | 2026-05-15 | WARN    | 0     | 1     | See details below |
| lands (forest_metrics) | — | PENDING | — | — | New table; ETL not yet run |
| health       | 2026-05-15 | WARN    | 0     | 7     | See details below |
| patents      | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| ref          | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| sec          | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| energy       | —          | PENDING | —     | —     | Data in R2; DQ not yet run |
| econ_reference | —        | PENDING | —     | —     | Data in R2; DQ not yet run |
| cyber_threat | —          | NO DATA | —     | —     | No Iceberg data in R2; ETL not yet run |
| cyber_vuln   | —          | NO DATA | —     | —     | No Iceberg data in R2; ETL not yet run |

---

## Weather (2026-05-11) — WARN

All 13 tables readable and non-empty. Two warnings:

| Table | Test | Detail |
|-------|------|--------|
| ghcnd_daily | all_same_value | columns: `year`, `tmax_flag`, `tmin_flag` — expected for single-year partition writes |
| hms_smoke_daily | all_same_value | column: `year` — expected for daily partition writes |

These are structural artifacts of partitioned writes (each partition has one year value). Consider
loosening `all_same_value` threshold for partition key columns, or exempting known partition columns.

---

## Edu (2026-05-13) — WARN

0 fails, 2 warns. All NAEP tables rebuilt with full state-level data (52 jurisdictions per combination).

### WARN

| Table | Test | Detail |
|-------|------|--------|
| crdc_schools | all_same_value | `teachers_first_year_fte`, `teachers_absent_fte`, `law_enforcement_ind` — sparse/constant CRDC fields; expected |
| crdc_schools | pk_nulls | 66,320 rows with `crdc_id=null` (ncessch populated); chronic-absenteeism endpoint omits crdc_id; backfill needed |

### Previously Fixed (2026-05-13)

- `naep_scores` and `naep_achievement_levels`: rebuilt with all-jurisdiction data (NP + 51 states/DC); prior data was national-only
- `ccd_districts all_null_cols migrant_students`: Urban Institute CCD API field not mapped — column removed from schema
- `college_scorecard all_null_cols pell_grant_rate`: `CollegeScorecardResponseTransformer` mapping fixed

### Previously Fixed (2026-05-12)

- `negative_enrollment` (ccd_districts, ccd_schools): DQ test updated to exclude NCES sentinel codes -1/-2/-3/-9
- `ccd_schools pk_nulls leaid`: DQ test updated to exclude leaid (BIE schools 9000* legitimately have no leaid)
- `ipeds_completions pk_nulls award_level`: DQ test updated to exclude award_level (IPEDS aggregate rows)
- `crdc_id nullable: false` in schema YAML: corrected to `nullable: true` (Parquet is always nullable; this was a metadata declaration error)
- `naep_scores all_null_cols` (std_error, pct_below_basic, pct_basic, pct_proficient, pct_advanced, sample_size): columns removed from schema — NAEP GetAdhocData does provide these but each requires a separate `stattype` call (ALD:BA, ALD:BC, ALD:PR, ALD:AD, SE:MN, CN:MN); not implemented, so not declared
- `ipeds_financials all_null_cols` (est_fte): column removed — est_fte comes from the NCES EFIA (12-Month Enrollment) survey, a separate dataset from Finance forms F1A/F2/F3; no EFIA source implemented
- `crdc_schools pk_nulls crdc_id`: 63,747 null crdc_id rows in chronic-absenteeism data; transformer backfills crdc_id=ncessch for future ingestion; existing data needs re-ingestion

---

## Health (2026-05-15) — WARN

0 fails, 7 warns. All 15 tables populated in R2 Iceberg. Row counts:

| Table | Rows |
|-------|------|
| fda_ndc_products | 26,000 |
| fda_drug_approvals | 26,000 |
| fda_drug_recalls | 17,643 |
| fda_adverse_events | 26,000 |
| fda_device_recalls | 26,000 |
| clinical_trials | 584,989 |
| clinical_trial_conditions | 1,045,013 |
| clinical_trial_interventions | 989,020 |
| cdc_covid_vaccinations | 29,886 |
| cms_hospital_quality | 5,432 |
| medicaid_drug_utilization | 3,904,000 |
| cdc_mortality | 21,344 |
| cdc_brfss | 1,986,000 |
| cms_open_payments | 108,776 |
| rxnorm_drugs | 37,280 |

### WARN

| Table | Test | Detail |
|-------|------|--------|
| clinical_trials | funder_type | 3 rows with funder_type outside known set — source data variation |
| cms_hospital_quality | all_null_cols | 6 columns fully null: mort/safety/readm_measures_better/worse — CMS does not populate these fields |
| cms_hospital_quality | all_same_value | 7 single-value columns (same CMS fields + birthing_friendly) |
| cms_open_payments | all_same_value | `program_year` constant=2023 — expected; single-year PGYR2023 fetch |
| fda_device_recalls | pk_nulls | 7 rows with null cfres_id — source omits cfres_id for some entries |
| fda_drug_recalls | pk_nulls | 1 row with null recall_number — source data gap |
| medicaid_drug_utilization | all_same_value | `year` constant — expected for single-year partition writes |

### Notes

- `cms_open_payments` source switched from DKAN OFFSET pagination (broken; 500-row API cap caused 0-row fetches) to direct CMS CSV bulk download (`download.cms.gov`), streamed via CSV_STREAM. 108,776 rows across general/research/ownership payment types for PGYR2023.
- `clinical_trial_interventions T6_pk_nulls` relaxed to check only `nct_id IS NULL` (intervention_name is nullable source data — 177 rows in API have no intervention_name).

---

## Lands (2026-05-15) — WARN

0 fails, 7 warns. All 7 tables populated in R2 Iceberg. Row counts:

| Table | Rows |
|-------|------|
| national_forests | 112 |
| timber_sales | 4,000 |
| forest_inventory | 29,952 |
| nps_units | ~430 |
| nps_visitation | ~50,000+ |
| blm_field_offices | 220 |
| onrr_revenues | 51,000 |

### WARN

| Table | Test | Detail |
|-------|------|--------|
| forest_inventory | basal_area_positive | 2,019 rows with `basal_area_sqft = 0` — valid for non-forest land conditions |
| timber_sales | all_same_value | `uom` constant = "ACRES" — expected; all FACTS activities measured in acres |
| blm_field_offices | office_code_uniqueness | 1 duplicate `(office_code, office_type)` pair: `NVS02000` Field — Red Rock and Sloan Canyon visitor centers share code in BLM ArcGIS source |
| onrr_revenues | revenue_non_negative | 3,359 rows with `revenue < 0` — legitimate negative adjustments/corrections in ONRR source |
| onrr_revenues | county_fips_format | 9,589 rows with county_fips < 5 digits — existing data predates transformer fix; needs re-ingestion |
| nps_units | (nps_units T4/T5 pending) | nps_units T4/T5 tests run against current data; results consistent with expected |

### Notes

- DQ tests for `national_forests T7_forest_id_uniqueness` and `nps_units T7_unit_code_uniqueness` corrected: `forest_id` is per-region (composite key `region, forest_id`); NPS park+preserve units share `unit_code` by design.
- `blm_field_offices T7_office_code_uniqueness` downgraded to warn; BLM source reuses codes across office types and some visitor centers are classified as Field type.
- `onrr_revenues county_fips`: transformer fixed to zero-pad to 5 digits; existing 9,589 rows in R2 need re-ingestion to resolve.
