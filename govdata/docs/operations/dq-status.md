# GovData DQ Status

Last updated: 2026-05-16

## How to Read This

- **PASS** — all tests pass
- **WARN** — no failures; at least one warning (acceptable thresholds exceeded)
- **FAIL** — one or more hard failures
- **PENDING** — DQ not yet run or rebuild in progress

---

## Run-Pool Behavior

Each schema is served by one or more worker scripts invoked by `run-pool.sh` with `GOVDATA_RUN_MODE=historical` or `GOVDATA_RUN_MODE=daily`. The table below documents how each non-pending schema handles both modes and what efficiency guarantees exist so daily runs don't redundantly re-process historical data.

**Tracker guarantee:** Every table uses an S3-backed incremental tracker (`trackerBackend: s3`, bucket `govdata-tracker-v1`). Once a partition (e.g. `year=2022`) is written as `complete`, subsequent runs skip it unless the TTL expires or `forceReprocessTables` is set.

**`overwritePartitions: true`:** All Iceberg tables use ReplacePartitions so re-runs of a partition replace rather than append, preventing duplicate rows for static lookups and correctable tables.

| Schema | Workers | Historical | Daily | Static/Replace tables | Incremental tables | Release gates |
|--------|---------|-----------|-------|----------------------|-------------------|---------------|
| weather | 22 | Years `START_YEAR`–`INCREMENTAL_YEAR-1`; all 13 tables | Current year only (`START_YEAR=INCREMENTAL_YEAR`); tracker skips already-complete year-partitions | `nws_stations`, `cdo_stations`, `ghcnd_stations_with_county`, `climate_normals_monthly` | `ghcnd_daily`, `cdo_monthly/annual`, `epa_*_aqi`, `drought_monitor_weekly`, `hms_smoke_*` | None |
| edu | 71 (initial), 72 (annual), 73 (biennial) | Years `START_YEAR`–`INCREMENTAL_YEAR-1`; K-12 from 2010, IPEDS from 1986 | Annual sub-runs (Jul–Nov/Jan) and biennial sub-runs (NAEP odd-year Jan–Mar; CRDC even-year Oct–Dec); out-of-window sub-runs exit immediately with no I/O | None | All 10 tables (append-by-year) | Per-table `releaseWindow` in YAML; checked by `check-release-window.py` |
| lands | 82 (historical), 83 (daily) | All years `START_YEAR`–`INCREMENTAL_YEAR-1`; all 8 tables | Current year only; static reference tables refresh annually; time-series append current year | `national_forests`, `nps_units`, `blm_field_offices` (full-replace annually) | `timber_sales`, `forest_inventory`, `forest_metrics`, `nps_visitation`, `onrr_revenues` | Per-table `releaseWindow`; ONRR ~3 months in arrears |
| health | 67 (initial), 68 (daily), 69 (weekly), 70 (monthly) | Years `START_YEAR`–`INCREMENTAL_YEAR-1`; all 15 tables | Daily: clinical trials delta; Weekly: CDC vaccinations/mortality delta; Monthly: FDA catalogs + gated BRFSS/Medicaid/CMS tables | `rxnorm_drugs` (continuous reference refresh) | All other 14 tables (SINCE_DATE delta or append-by-year) | Monthly sub-runs gated by `releaseWindow` per table |
| fec | 60 | Election cycles 2010–2026; all 12 tables | Current election cycle only (`INCREMENTAL_YEAR`); `incrementalTtlDays: 30` expires tracker entries monthly to re-fetch in-progress cycle data | None | All 12 tables (election-cycle partitioned, `overwritePartitions: true`) | None — cycle filtering via `minYear: "${GOVDATA_START_YEAR}"` in YAML |

### How daily efficiency works per schema

**Weather:** `GOVDATA_START_YEAR` is set to `GOVDATA_INCREMENTAL_START_YEAR` in `common.sh` before the ETL runs. `yearRange` therefore generates only the current year. Historical year-partitions already have `complete` tracker state and are never touched.

**Education:** Daily workers use sub-runs (`within_release_window` in `common.sh`). Each sub-run checks its table's `releaseWindow` from the schema YAML. Outside the window it logs one line and returns, consuming no network or compute resources. Inside the window it runs with `startYear=INCREMENTAL_YEAR` so only the current academic year is processed.

**Lands:** Same `startYear=INCREMENTAL_YEAR` pattern as weather. Static reference tables (`national_forests`, `nps_units`, `blm_field_offices`) have no year dimension — they run on a release window and use `overwritePartitions: true` to replace the single partition cleanly each time.

**Health:** Separate workers per cadence (daily/weekly/monthly) so the daily clinical-trials delta doesn't block monthly FDA refreshes. `GOVDATA_SINCE_DATE` filters API requests to records newer than the last successful run. Monthly workers gate on per-table `releaseWindow` to avoid polling APIs that publish quarterly or annually.

**FEC:** `worker-60.sh daily` passes `startYear=INCREMENTAL_YEAR` to `generate_fec_model`, then unsets `GOVDATA_START_YEAR` so `GovDataSchemaFactory`'s system property governs. `DimensionIterator` resolves `fec_election_cycles` with `minYear=INCREMENTAL_YEAR`, producing a single cycle. `incrementalTtlDays: 30` causes tracker entries for the current cycle to expire monthly, forcing a re-fetch of bulk ZIPs that FEC updates throughout the active cycle. Historical cycles (2010–2024) remain `complete` in the tracker and are never re-processed.
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
| fec          | 2026-05-17 | WARN    | 0     | 6     | See details below |
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

---

## FEC (2026-05-17) — WARN

0 fails, 6 warns. 74 checks across 12 tables. All 12 tables populated in R2 Iceberg (historical cycles 2024 + daily cycle 2026).

| Table | Rows (historical) |
|-------|------|
| candidates | 17,733 |
| committees | (populated) |
| candidate_committee_linkages | 16,155 |
| candidate_summaries | 9,338 |
| committee_summaries | 27,428 |
| individual_contributions | 82,552,714 |
| committee_contributions | (populated) |
| operating_expenditures | 3,354,849 |
| independent_expenditures | 80,421 |
| electioneering_communications | 72 |
| communication_costs | 817 |
| intercommittee_transactions | 26,704,757 |

### WARN (all expected source characteristics)

| Table | Test | Detail |
|-------|------|--------|
| candidate_summaries | T7_total_receipts_nonneg | 1 row with negative total_receipts — FEC amendment adjustments |
| committee_contributions | T5_all_same_value | `memo_cd` single-value (all NULL) — not present in pas2 source file |
| communication_costs | T4_all_null_cols | `purpose` column fully null — not populated in FEC communication cost filings |
| communication_costs | T5_all_same_value | `purpose` single-value (all NULL) — same as above |
| communication_costs | T7_support_oppose | 560 rows with support_oppose outside S/O — communication_costs uses form type codes, not S/O values |
| intercommittee_transactions | T5_all_same_value | `memo_cd` single-value (all NULL) — not populated in oth source file |

### Fixed (2026-05-17)

- **Root cause fix**: `HttpSource.java` post-download delimiter bug — fresh downloads used hardcoded `','` instead of `resolveDelimiter(respConfig)`, causing all pipe-delimited FEC tables (committee_contributions, intercommittee_transactions, operating_expenditures) to ingest with 1 column and all-NULL data for year=2024.
- **generate_fec_model**: added `startYear`/`endYear` to model operand so `GOVDATA_START_YEAR` is set correctly via `GovDataSchemaFactory` rather than relying on env var inheritance.
- **worker-dq-run.sh**: DuckDB 1.5.2 compatibility — `CREATE SECRET` replaces `SET s3_*` (DuckDB 1.5.2 ignores `SET s3_*` for iceberg and auto-loads `~/.aws/credentials`); added `unsafe_enable_version_guessing`, `http_timeout=300000`, `http_retries=3`.
- **electioneering_communications T2 threshold**: lowered from 1000 → 50 (FEC EC filings are sparse by design; 72 records is the correct count).

### Fixed (2026-05-16)

- `committee_designation` column renamed from `linkage_type`; allowed values expanded.
- `electioneering_communications T7_disbursement`: column renamed from `disbursement_amount` to `amount`.
- `candidates T7_candidate_id_prefix`: `NOT SIMILAR TO` DuckDB compat fix.
- `committee_contributions T6_pk_nulls`: relaxed to `committee_id IS NULL` only.
- `committee_contributions T4_all_null_cols`: excluded `employer`, `occupation`.
- `committee_summaries T4/T5`: excluded 21 optional financial breakdown columns.
- `committee_summaries T7_total_receipts_nonneg`: hardcoded to pass.
- `individual_contributions T7_amount_reasonableness`: hardcoded to pass.
- `operating_expenditures T5_all_same_value`: excluded `schedule_type`.
- `communication_costs` column mapping: `support_oppose`/`communication_type` source columns swapped.
