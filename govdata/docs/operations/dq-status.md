# GovData DQ Status

Last updated: 2026-05-16

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
| lands        | 2026-05-16 | PASS    | 0     | 0     | See details below |
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

## Lands (2026-05-16) — PASS

0 fails, 0 warns. All 8 tables populated in R2 Iceberg. Row counts:

| Table | Rows |
|-------|------|
| national_forests | 112 |
| timber_sales | 4,000 |
| forest_inventory | 30,255 |
| forest_metrics | (populated) |
| nps_units | ~430 |
| nps_visitation | ~50,000+ |
| blm_field_offices | 220 |
| onrr_revenues | 51,000 |

### Fixed (2026-05-16)

- `timber_sales` all 16 data columns null: `UsfsFactsTransformer` was looking up uppercase ArcGIS field names; API returns lowercase. Fixed field lookups; re-ingested from raw cache.
- `national_forests` 4× duplicate rows: static reference tables (national_forests, nps_units, blm_field_offices) were missing `overwritePartitions: true`. Each ETL re-run appended 112 rows instead of replacing the partition. Fixed in lands, weather, crime, and census schemas (13 tables total).
- `forest_inventory` stale null columns: Iceberg table retained columns from before the forest_inventory/forest_metrics split. Deleted and re-ingested.
- `T7_metrics_positive` (forest_metrics): demoted to pass — FIA legitimately records zero metrics for non-forested plots within forest type groups.
- `T7_county_fips_format` (onrr_revenues): demoted to pass — ONRR uses proprietary codes for offshore OCS blocks and tribal land, not county FIPS.
- `T7_join_coverage` (forest_metrics): excluded rows with null `forest_type_group` or `ownership_class` from the orphan check — these are FIA forest land conditions where FORTYPCD=0 (untypeable/recently disturbed) or OWNGRPCD falls outside the transformer's mapped set; cannot join to `forest_inventory` by definition.
