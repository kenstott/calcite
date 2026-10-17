-- dq-lookback: 5
-- ============================================================================
-- Data Quality Checks: census schema
-- ============================================================================
-- Tests per table:
--   T1  existence        — at least 1 row is readable
--   T2  row_count        — row count meets conservative minimum threshold
--   T3  sample           — print 1 sample row to console (informational)
--   T4  all_null_cols    — no column is 100% NULL across all rows
--   T5  all_same_value   — no non-null column has only one distinct value
--   T6  pk_nulls         — SKIPPED: all census columns are declared nullable
--   T7  expected_values  — domain-specific sanity checks
--
-- Source: iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/{table}', allow_moved_paths := true)
-- Result: written to dq_results TEMP TABLE (worker appends COPY to Parquet)
-- ============================================================================

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs;  LOAD httpfs;

SET s3_access_key_id     = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint          = '21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region            = 'auto';

CREATE TEMP TABLE dq_results (
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,
  value     VARCHAR,
  threshold VARCHAR,
  detail    VARCHAR
);

-- ============================================================================
-- T1: EXISTENCE — each table must have at least 1 readable row
-- ============================================================================

INSERT INTO dq_results
SELECT
  'census'  AS schema,
  tbl,
  'existence' AS test,
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END AS status,
  CAST(n AS VARCHAR)  AS value,
  '1'                 AS threshold,
  CASE WHEN n > 0 THEN 'table is readable' ELSE 'table returned 0 rows — may not yet be ingested' END AS detail
FROM (
  SELECT 'acs_population'         AS tbl, (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population',         allow_moved_paths := true) LIMIT 1) t) AS n
  UNION ALL SELECT 'acs_income',              (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income',              allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_housing',             (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing',             allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_education',           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_education',           allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_employment',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_employment',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_poverty',             (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_poverty',             allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'decennial_population',    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_population',    allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_race_ethnicity',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_race_ethnicity',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_age',                 (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_age',                 allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_commuting',           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_commuting',           allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_health_insurance',    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_health_insurance',    allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_language',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_language',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_disability',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_disability',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_veterans',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_veterans_by_period',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_by_period',  allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_veterans_employment',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_employment',  allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_migration',           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration',           allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_occupation',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_industry',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_internet',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_vehicle_access',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_vehicle_access',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_earnings_by_education', (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_earnings_by_education', allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_nativity_by_education', (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity_by_education', allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_income_by_nativity',    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_by_nativity',    allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_industry_by_nativity',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry_by_nativity',    allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'cps_voting_supplement',     (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cps_voting_supplement',     allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'uscis_i765_employment_authorization',     (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/uscis_i765_employment_authorization',     allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_nativity',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_marital_status',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_household_type',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_housing_tenure',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_housing_tenure_by_age', (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure_by_age', allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_housing_year_built_detail', (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_year_built_detail', allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_income_distribution', (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_distribution', allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'decennial_housing',       (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_housing',       allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'pep_population',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'cbp_establishments',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cbp_establishments',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs1_population',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_population',         allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs1_income',             (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_income',             allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'economic_census',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/economic_census',         allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'saipe_poverty',           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/saipe_poverty',           allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'sahie_insurance',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/sahie_insurance',         allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'bds_dynamics',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/bds_dynamics',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'abs_characteristics',     (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/abs_characteristics',     allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'nonemployer_statistics',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/nonemployer_statistics',  allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'building_permits',        (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/building_permits',        allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'qwi_employment',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/qwi_employment',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'lodes_workplace',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/lodes_workplace',         allow_moved_paths := true) LIMIT 1) t)
) src;

-- ============================================================================
-- T2: ROW COUNT — conservative minimums
-- ============================================================================
-- ACS 5-year tables span state + county geographies across multiple years.
-- County-level tables: ~3200 counties * 5+ years = 16000+ rows.
-- State-level (ACS1): 51 jurisdictions * 5+ years = 250+ rows.
-- Decennial: 2 censuses (2010, 2020) * state+county = 5000+ rows.
-- ============================================================================

INSERT INTO dq_results
SELECT
  'census' AS schema,
  tbl,
  'row_count' AS test,
  CASE WHEN n >= threshold THEN 'pass' ELSE 'fail' END AS status,
  CAST(n AS VARCHAR)         AS value,
  CAST(threshold AS VARCHAR) AS threshold,
  CASE WHEN n >= threshold THEN 'row count meets minimum'
       ELSE 'row count below minimum — ingestion may be incomplete or failed'
  END AS detail
FROM (
  SELECT 'acs_population'         AS tbl, (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population',         allow_moved_paths := true)) AS n, 10000 AS threshold
  UNION ALL SELECT 'acs_income',              (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income',              allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_housing',             (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing',             allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_education',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_education',           allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_employment',          (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_employment',          allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_poverty',             (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_poverty',             allow_moved_paths := true)), 10000
  UNION ALL SELECT 'decennial_population',    (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_population',    allow_moved_paths := true)), 5000
  UNION ALL SELECT 'acs_race_ethnicity',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_race_ethnicity',      allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_age',                 (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_age',                 allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_commuting',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_commuting',           allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_health_insurance',    (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_health_insurance',    allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_language',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_language',            allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_disability',          (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_disability',          allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_veterans',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans',            allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_veterans_by_period',  (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_by_period',  allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_veterans_employment',  (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_employment',  allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_migration',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration',           allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_occupation',          (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation',          allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_industry',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry',            allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_internet',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet',            allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_vehicle_access',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_vehicle_access',      allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_earnings_by_education', (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_earnings_by_education', allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_nativity_by_education', (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity_by_education', allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_income_by_nativity',    (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_by_nativity',    allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_industry_by_nativity',  (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry_by_nativity',    allow_moved_paths := true)), 150
  UNION ALL SELECT 'cps_voting_supplement',     (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cps_voting_supplement',     allow_moved_paths := true)), 10000
  UNION ALL SELECT 'uscis_i765_employment_authorization',     (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/uscis_i765_employment_authorization',     allow_moved_paths := true)), 500
  UNION ALL SELECT 'acs_nativity',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity',            allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_marital_status',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status',      allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_household_type',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type',      allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_housing_tenure',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure',      allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_housing_tenure_by_age', (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure_by_age', allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_housing_year_built_detail', (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_year_built_detail', allow_moved_paths := true)), 9000
  UNION ALL SELECT 'acs_income_distribution', (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_distribution', allow_moved_paths := true)), 10000
  UNION ALL SELECT 'decennial_housing',       (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_housing',       allow_moved_paths := true)), 5000
  UNION ALL SELECT 'pep_population',          (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population',          allow_moved_paths := true)), 2000
  -- cbp: 3 yrs × (51 states + ~3143 counties) = ~9582 max; threshold 80% of max
  UNION ALL SELECT 'cbp_establishments',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cbp_establishments',      allow_moved_paths := true)), 8500
  UNION ALL SELECT 'acs1_population',         (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_population',         allow_moved_paths := true)), 250
  UNION ALL SELECT 'acs1_income',             (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_income',             allow_moved_paths := true)), 250
  UNION ALL SELECT 'economic_census',         (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/economic_census',         allow_moved_paths := true)), 5000
  UNION ALL SELECT 'saipe_poverty',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/saipe_poverty',           allow_moved_paths := true)), 9000
  -- sahie: 3 yrs × (51 states + ~3143 counties) = ~9582 max; threshold 80% of max
  UNION ALL SELECT 'sahie_insurance',         (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/sahie_insurance',         allow_moved_paths := true)), 8500
  -- bds: 3 yrs × 51 states = 153 max (state-level only); threshold 80% of max
  UNION ALL SELECT 'bds_dynamics',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/bds_dynamics',            allow_moved_paths := true)), 120
  -- abs: 2 yrs × 51 states = 102 max (state-level only); threshold 80% of max
  UNION ALL SELECT 'abs_characteristics',     (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/abs_characteristics',     allow_moved_paths := true)), 80
  UNION ALL SELECT 'nonemployer_statistics',  (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/nonemployer_statistics',  allow_moved_paths := true)), 10000
  UNION ALL SELECT 'building_permits',        (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/building_permits',        allow_moved_paths := true)), 250
  -- qwi: 12 quarters × 51 states = 612 max; threshold 80% of max
  UNION ALL SELECT 'qwi_employment',          (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/qwi_employment',          allow_moved_paths := true)), 500
  UNION ALL SELECT 'lodes_workplace',         (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/lodes_workplace',         allow_moved_paths := true)), 10000
) src;

-- ============================================================================
-- T3: SAMPLE — print 1 row per table to console (informational only)
-- ============================================================================

SELECT 'acs_population'         AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population',         allow_moved_paths := true) LIMIT 1;
SELECT 'acs_income'             AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income',             allow_moved_paths := true) LIMIT 1;
SELECT 'acs_housing'            AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing',            allow_moved_paths := true) LIMIT 1;
SELECT 'acs_education'          AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_education',          allow_moved_paths := true) LIMIT 1;
SELECT 'acs_employment'         AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_employment',         allow_moved_paths := true) LIMIT 1;
SELECT 'acs_poverty'            AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_poverty',            allow_moved_paths := true) LIMIT 1;
SELECT 'decennial_population'   AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_population',   allow_moved_paths := true) LIMIT 1;
SELECT 'acs_race_ethnicity'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_race_ethnicity',     allow_moved_paths := true) LIMIT 1;
SELECT 'acs_age'                AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_age',                allow_moved_paths := true) LIMIT 1;
SELECT 'acs_commuting'          AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_commuting',          allow_moved_paths := true) LIMIT 1;
SELECT 'acs_health_insurance'   AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_health_insurance',   allow_moved_paths := true) LIMIT 1;
SELECT 'acs_language'           AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_language',           allow_moved_paths := true) LIMIT 1;
SELECT 'acs_disability'         AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_disability',         allow_moved_paths := true) LIMIT 1;
SELECT 'acs_veterans'           AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans',           allow_moved_paths := true) LIMIT 1;
SELECT 'acs_veterans_by_period' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_by_period', allow_moved_paths := true) LIMIT 1;
SELECT 'acs_veterans_employment' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_employment', allow_moved_paths := true) LIMIT 1;
SELECT 'acs_migration'          AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration',          allow_moved_paths := true) LIMIT 1;
SELECT 'acs_occupation'         AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation',         allow_moved_paths := true) LIMIT 1;
SELECT 'acs_industry'           AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry',           allow_moved_paths := true) LIMIT 1;
SELECT 'acs_internet'           AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet',           allow_moved_paths := true) LIMIT 1;
SELECT 'acs_vehicle_access'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_vehicle_access',     allow_moved_paths := true) LIMIT 1;
SELECT 'acs_earnings_by_education' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_earnings_by_education', allow_moved_paths := true) LIMIT 1;
SELECT 'acs_nativity_by_education' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity_by_education', allow_moved_paths := true) LIMIT 1;
SELECT 'acs_income_by_nativity' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_by_nativity', allow_moved_paths := true) LIMIT 1;
SELECT 'acs_industry_by_nativity' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry_by_nativity', allow_moved_paths := true) LIMIT 1;
SELECT 'cps_voting_supplement' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cps_voting_supplement', allow_moved_paths := true) LIMIT 1;
SELECT 'uscis_i765_employment_authorization' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/uscis_i765_employment_authorization', allow_moved_paths := true) LIMIT 1;
SELECT 'acs_nativity'           AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity',           allow_moved_paths := true) LIMIT 1;
SELECT 'acs_marital_status'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status',     allow_moved_paths := true) LIMIT 1;
SELECT 'acs_household_type'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type',     allow_moved_paths := true) LIMIT 1;
SELECT 'acs_housing_tenure'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure',     allow_moved_paths := true) LIMIT 1;
SELECT 'acs_housing_tenure_by_age' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure_by_age', allow_moved_paths := true) LIMIT 1;
SELECT 'acs_housing_year_built_detail' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_year_built_detail', allow_moved_paths := true) LIMIT 1;
SELECT 'acs_income_distribution'AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_distribution',allow_moved_paths := true) LIMIT 1;
SELECT 'decennial_housing'      AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_housing',      allow_moved_paths := true) LIMIT 1;
SELECT 'pep_population'         AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population',         allow_moved_paths := true) LIMIT 1;
SELECT 'cbp_establishments'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cbp_establishments',     allow_moved_paths := true) LIMIT 1;
SELECT 'acs1_population'        AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_population',        allow_moved_paths := true) LIMIT 1;
SELECT 'acs1_income'            AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_income',            allow_moved_paths := true) LIMIT 1;
SELECT 'economic_census'        AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/economic_census',        allow_moved_paths := true) LIMIT 1;
SELECT 'saipe_poverty'          AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/saipe_poverty',          allow_moved_paths := true) LIMIT 1;
SELECT 'sahie_insurance'        AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/sahie_insurance',        allow_moved_paths := true) LIMIT 1;
SELECT 'bds_dynamics'           AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/bds_dynamics',           allow_moved_paths := true) LIMIT 1;
SELECT 'abs_characteristics'    AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/abs_characteristics',    allow_moved_paths := true) LIMIT 1;
SELECT 'nonemployer_statistics' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/nonemployer_statistics', allow_moved_paths := true) LIMIT 1;
SELECT 'building_permits'       AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/building_permits',       allow_moved_paths := true) LIMIT 1;
SELECT 'qwi_employment'         AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/qwi_employment',         allow_moved_paths := true) LIMIT 1;
SELECT 'lodes_workplace'        AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/lodes_workplace',        allow_moved_paths := true) LIMIT 1;

-- ============================================================================
-- T4: ALL-NULL COLUMNS — no column should be 100% NULL
-- ============================================================================

INSERT INTO dq_results SELECT 'census', 'acs_population', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_income', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_housing', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_education', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_education', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_employment', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_employment', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_poverty', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_poverty', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'decennial_population', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_population', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_race_ethnicity', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_race_ethnicity', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_age', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_age', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_commuting', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_commuting', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_health_insurance', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_health_insurance', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_language', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_language', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_disability', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_disability', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_veterans', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_veterans_by_period', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_by_period', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_veterans_employment', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_employment', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_migration', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_occupation', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_industry', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_internet', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_vehicle_access', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_vehicle_access', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_earnings_by_education', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_earnings_by_education', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_nativity_by_education', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity_by_education', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_income_by_nativity', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_by_nativity', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_industry_by_nativity', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry_by_nativity', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'cps_voting_supplement', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cps_voting_supplement', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'uscis_i765_employment_authorization', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/uscis_i765_employment_authorization', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_nativity', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_marital_status', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_household_type', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_housing_tenure', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_housing_tenure_by_age', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure_by_age', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_housing_year_built_detail', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_year_built_detail', allow_moved_paths := true)) WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'geography');
INSERT INTO dq_results SELECT 'census', 'acs_income_distribution', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_distribution', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'decennial_housing', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_housing', allow_moved_paths := true)) WHERE null_percentage = 100.0;
-- pep_population: `density` is only provided by the pep/population dataset (data year 2019); the 2020+ pep/charv
-- endpoint returns POP (no DENSITY), so any build of 2020+ data years legitimately has
-- density 100% null — warn, not fail. Every other entirely-null column still fails.
-- (Vintage boundary moved from 2020 to 2021 in the fix for the 2020 double-claim defect —
-- both APIs could previously answer for data year 2020 with conflicting values; 2020 is now
-- claimed exclusively by the <=2020 API, which does provide density.)
INSERT INTO dq_results SELECT 'census', 'pep_population', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population', allow_moved_paths := true)) WHERE null_percentage = 100.0 AND column_name <> 'density';
INSERT INTO dq_results SELECT 'census', 'pep_population', 'all_null_cols', 'warn', column_name, '< 100% null', 'density only exists for pep/population data years (2019); expected null for 2020+ builds' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population', allow_moved_paths := true)) WHERE null_percentage = 100.0 AND column_name = 'density';
INSERT INTO dq_results SELECT 'census', 'cbp_establishments', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cbp_establishments', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs1_population', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_population', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs1_income', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_income', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'economic_census', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/economic_census', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'saipe_poverty', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/saipe_poverty', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'sahie_insurance', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/sahie_insurance', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'bds_dynamics', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/bds_dynamics', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'abs_characteristics', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/abs_characteristics', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'nonemployer_statistics', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/nonemployer_statistics', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'building_permits', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/building_permits', allow_moved_paths := true)) WHERE null_percentage = 100.0;
-- Payroll is excluded: this table ingests only the LEHD QWI "SA" (Sex-by-Age) tabulation,
-- which never publishes Payroll at source (confirmed 100% null in LEHD's own raw file, not
-- an ingestion gap).
INSERT INTO dq_results SELECT 'census', 'qwi_employment', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/qwi_employment', allow_moved_paths := true)) WHERE null_percentage = 100.0 AND column_name <> 'Payroll';
INSERT INTO dq_results SELECT 'census', 'lodes_workplace', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/lodes_workplace', allow_moved_paths := true)) WHERE null_percentage = 100.0;

-- ============================================================================
-- T5: ALL-SAME-VALUE — no non-null column should have only one distinct value
-- ============================================================================

INSERT INTO dq_results SELECT 'census', 'acs_population', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_income', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_housing', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_education', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_education', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_employment', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_employment', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_poverty', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_poverty', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'decennial_population', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_population', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_race_ethnicity', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_race_ethnicity', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_age', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_age', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_commuting', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_commuting', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_health_insurance', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_health_insurance', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_language', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_language', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_disability', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_disability', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_veterans', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_veterans_by_period', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_by_period', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_veterans_employment', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans_employment', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_migration', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_occupation', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_industry', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_internet', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_vehicle_access', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_vehicle_access', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_earnings_by_education', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_earnings_by_education', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_nativity_by_education', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity_by_education', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_income_by_nativity', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_by_nativity', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
-- acs_industry_by_nativity: geography is constant 'state' by design (S0501 state grain only).
INSERT INTO dq_results SELECT 'census', 'acs_industry_by_nativity', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry_by_nativity', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name NOT IN ('type', 'geography');
INSERT INTO dq_results SELECT 'census', 'cps_voting_supplement', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cps_voting_supplement', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'uscis_i765_employment_authorization', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/uscis_i765_employment_authorization', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_nativity', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_marital_status', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_household_type', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_housing_tenure', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_housing_tenure_by_age', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure_by_age', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs_housing_year_built_detail', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_year_built_detail', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name NOT IN ('type', 'year', 'geography');
INSERT INTO dq_results SELECT 'census', 'acs_income_distribution', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_distribution', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'decennial_housing', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_housing', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'pep_population', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'cbp_establishments', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cbp_establishments', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs1_population', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_population', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'acs1_income', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_income', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'economic_census', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/economic_census', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'saipe_poverty', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/saipe_poverty', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'sahie_insurance', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/sahie_insurance', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'bds_dynamics', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/bds_dynamics', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'abs_characteristics', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/abs_characteristics', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'nonemployer_statistics', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/nonemployer_statistics', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
INSERT INTO dq_results SELECT 'census', 'building_permits', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/building_permits', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';
-- This table deliberately ingests only one fixed dimensional slice of the QWI cube (state-level,
-- all industries, private ownership, quarterly, not-seasonally-adjusted, Sex-by-Age tabulation);
-- only sex/agegrp vary by design, so these 9 columns are intentionally constant, not broken.
INSERT INTO dq_results SELECT 'census', 'qwi_employment', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/qwi_employment', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name NOT IN ('type', 'periodicity', 'seasonadj', 'geo_level', 'ownercode', 'race', 'ethnicity', 'education', 'firmage', 'firmsize');
INSERT INTO dq_results SELECT 'census', 'lodes_workplace', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/lodes_workplace', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';

-- ============================================================================
-- T6: PK NULLS — SKIPPED
-- All columns in the census schema are declared nullable: true.
-- No non-nullable business key columns exist to check.
-- ============================================================================

-- ============================================================================
-- T7: EXPECTED VALUES — domain-specific sanity checks
-- ============================================================================

-- acs1_income: each year with data carries exactly one national row with a plausible median household income
INSERT INTO dq_results
SELECT
  'census', 'acs1_income', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'years without exactly one United States row, or whose national median household income is outside 20000-200000'
FROM (
  SELECT COUNT(*) AS bad
  FROM (
    SELECT year, SUM(CASE WHEN geo_name = 'United States' THEN 1 ELSE 0 END) AS n_us,
           MAX(CASE WHEN geo_name = 'United States' THEN median_household_income END) AS us_median
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_income', allow_moved_paths := true)
    GROUP BY year
  )
  WHERE n_us != 1 OR us_median IS NULL OR us_median NOT BETWEEN 20000 AND 200000
);

-- acs_population: state FIPS code must be 2 characters
INSERT INTO dq_results
SELECT
  'census', 'acs_population', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where state FIPS is not 2 characters'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population', allow_moved_paths := true)
  WHERE state IS NOT NULL AND LENGTH(state) != 2
);

-- acs_population: total_population must be >= 0
INSERT INTO dq_results
SELECT
  'census', 'acs_population', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where total_population is negative'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population', allow_moved_paths := true)
  WHERE total_population IS NOT NULL AND total_population < 0
);

-- acs_population: must cover all 51 US states+DC
INSERT INTO dq_results
SELECT
  'census', 'acs_population', 'expected_values',
  CASE WHEN n >= 51 THEN 'pass' ELSE 'fail' END,
  CAST(n AS VARCHAR), '51',
  'distinct state codes (expecting 51 — 50 states + DC)'
FROM (
  SELECT COUNT(DISTINCT state) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population', allow_moved_paths := true)
  WHERE state IS NOT NULL AND county IS NULL
);

-- acs_income: median_household_income must be > 0
INSERT INTO dq_results
SELECT
  'census', 'acs_income', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where median_household_income is <= 0'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income', allow_moved_paths := true)
  WHERE median_household_income IS NOT NULL AND median_household_income <= 0
);

-- acs_housing: total_housing_units must be >= 0
INSERT INTO dq_results
SELECT
  'census', 'acs_housing', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where total_housing_units is negative'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing', allow_moved_paths := true)
  WHERE total_housing_units IS NOT NULL AND total_housing_units < 0
);

-- acs_housing_year_built_detail: total_units must be >= 0
INSERT INTO dq_results
SELECT
  'census', 'acs_housing_year_built_detail', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where total_units is negative'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_year_built_detail', allow_moved_paths := true)
  WHERE total_units IS NOT NULL AND total_units < 0
);

-- acs_housing_year_built_detail: the ten B25034 brackets are a partition of the table's
-- own total — they must sum to total_units exactly (verified live at onboard: Kings County
-- NY 2023 sums to the unit; ACS B25034_002E..011E == B25034_001E by construction)
INSERT INTO dq_results
SELECT
  'census', 'acs_housing_year_built_detail', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where the ten built-brackets do not sum to total_units'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_year_built_detail', allow_moved_paths := true)
  WHERE total_units IS NOT NULL AND built_2020_or_later IS NOT NULL AND built_2010_to_2019 IS NOT NULL
    AND built_2000_to_2009 IS NOT NULL AND built_1990_to_1999 IS NOT NULL AND built_1980_to_1989 IS NOT NULL
    AND built_1970_to_1979 IS NOT NULL AND built_1960_to_1969 IS NOT NULL AND built_1950_to_1959 IS NOT NULL
    AND built_1940_to_1949 IS NOT NULL AND built_1939_or_earlier IS NOT NULL
    AND built_2020_or_later + built_2010_to_2019 + built_2000_to_2009 + built_1990_to_1999
      + built_1980_to_1989 + built_1970_to_1979 + built_1960_to_1969 + built_1950_to_1959
      + built_1940_to_1949 + built_1939_or_earlier <> total_units
);

-- acs_housing_year_built_detail: must cover all 51 US states+DC at state level
INSERT INTO dq_results
SELECT
  'census', 'acs_housing_year_built_detail', 'expected_values',
  CASE WHEN n >= 51 THEN 'pass' ELSE 'fail' END,
  CAST(n AS VARCHAR), '51',
  'distinct state codes at state geography (expecting 51 — 50 states + DC)'
FROM (
  SELECT COUNT(DISTINCT state) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_year_built_detail', allow_moved_paths := true)
  WHERE geography = 'state'
);

-- acs_housing_year_built_detail: no duplicate (year, geography, state/county_fips) rows
INSERT INTO dq_results
SELECT
  'census', 'acs_housing_year_built_detail', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'duplicate (year, geography, state, county) rows'
FROM (
  SELECT COUNT(*) AS bad FROM (
    SELECT year, geography, state, county, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_year_built_detail', allow_moved_paths := true)
    GROUP BY year, geography, state, county HAVING COUNT(*) > 1
  )
);

-- county_fips integrity: if county is present, county_fips must be 5 characters
INSERT INTO dq_results
SELECT
  'census', 'acs_population', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where county_fips is present but not 5 characters'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population', allow_moved_paths := true)
  WHERE county_fips IS NOT NULL AND LENGTH(county_fips) != 5
);

-- acs_nativity: native_born + foreign_born must equal total_population (Defect Register #1
-- guardrail — catches a re-mapping to the wrong ACS variable the way foreign_born was wrongly
-- mapped to not_a_citizen alone before this fix)
INSERT INTO dq_results
SELECT
  'census', 'acs_nativity', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where native_born + foreign_born != total_population'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity', allow_moved_paths := true)
  WHERE native_born IS NOT NULL AND foreign_born IS NOT NULL AND total_population IS NOT NULL
    AND native_born + foreign_born != total_population
);

-- acs_nativity: full component identity — with born_in_us/born_in_pr_island_areas/
-- born_abroad_american_parents exposed as real columns, the whole B05001 breakdown
-- (002+003+004+005+006 = 001) is checkable in-row, not just the two-term sum above
INSERT INTO dq_results
SELECT
  'census', 'acs_nativity', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where born_in_us + born_in_pr_island_areas + born_abroad_american_parents + naturalized_citizen + not_a_citizen != total_population'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity', allow_moved_paths := true)
  WHERE born_in_us IS NOT NULL AND born_in_pr_island_areas IS NOT NULL
    AND born_abroad_american_parents IS NOT NULL AND naturalized_citizen IS NOT NULL
    AND not_a_citizen IS NOT NULL AND total_population IS NOT NULL
    AND born_in_us + born_in_pr_island_areas + born_abroad_american_parents
        + naturalized_citizen + not_a_citizen != total_population
);

-- pep_population: population estimates must be >= 0
INSERT INTO dq_results
SELECT
  'census', 'pep_population', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where population estimate is negative'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population', allow_moved_paths := true)
  WHERE population IS NOT NULL AND population < 0
);

-- ----------------------------------------------------------------------------
-- pep_components_of_change: PEP county components (bulk co-est{vintage}-alldata.csv),
-- one row per county per data year 2010..latest. Not sampled: the whole table is read.
-- ----------------------------------------------------------------------------
INSERT INTO dq_results
SELECT 'census', 'pep_components_of_change', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '1', 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'census', 'pep_components_of_change', 'T2_row_count',
  CASE WHEN n >= 50000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '50000',
  'Expected >= 50000 rows (16 data years 2010-2025 x about 3,140 counties)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true));

SELECT 'pep_components_of_change' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true) LIMIT 1;

INSERT INTO dq_results SELECT 'census', 'pep_components_of_change', 'T4_all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'pep_components_of_change', 'T5_all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';

INSERT INTO dq_results
SELECT 'census', 'pep_components_of_change', 'T6_pk_nulls_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '0',
  'NULL county_fips rows plus duplicate (year, county_fips) keys'
FROM (SELECT
        (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true) WHERE county_fips IS NULL)
      + (SELECT COUNT(*) FROM (SELECT year, county_fips FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true) GROUP BY 1, 2 HAVING COUNT(*) > 1)) AS n);

-- Both vintages present: data years 2010-2019 (vintage 2019) and 2020 onward (latest vintage).
INSERT INTO dq_results
SELECT 'census', 'pep_components_of_change', 'T7_years_and_vintages',
  CASE WHEN y_old = 10 AND y_new >= 6 AND v_old = 1 AND v_new = 1 THEN 'pass' ELSE 'fail' END,
  CAST(y_old AS VARCHAR) || ' + ' || CAST(y_new AS VARCHAR), '10 + >=6',
  'Distinct data years 2010-2019 and 2020+, one vintage each'
FROM (SELECT
        COUNT(DISTINCT CASE WHEN CAST(year AS INTEGER) < 2020 THEN year END) AS y_old,
        COUNT(DISTINCT CASE WHEN CAST(year AS INTEGER) >= 2020 THEN year END) AS y_new,
        COUNT(DISTINCT CASE WHEN CAST(year AS INTEGER) < 2020 THEN vintage END) AS v_old,
        COUNT(DISTINCT CASE WHEN CAST(year AS INTEGER) >= 2020 THEN vintage END) AS v_new
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true));

-- Identities of the PEP components: natural change = births - deaths and net migration =
-- international + domestic, per row; the domestic moves of all counties net to zero
-- nationally in each year (a move out of one county is a move into another).
INSERT INTO dq_results
SELECT 'census', 'pep_components_of_change', 'T7_component_identities',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, CAST(bad AS VARCHAR), '0',
  'Rows where natural_change <> births - deaths or net_migration <> international + domestic'
FROM (SELECT COUNT(*) AS bad
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true)
      WHERE natural_change <> births - deaths
         OR net_migration <> international_migration + domestic_migration);

INSERT INTO dq_results
SELECT 'census', 'pep_components_of_change', 'T7_domestic_nets_to_zero',
  CASE WHEN worst <= 100 THEN 'pass' ELSE 'fail' END, CAST(worst AS VARCHAR), '<= 100',
  'Largest absolute national sum of domestic_migration over counties in any data year'
FROM (SELECT COALESCE(MAX(ABS(s)), 0) AS worst FROM (
        SELECT year, SUM(domestic_migration) AS s
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true)
        GROUP BY year));

-- ----------------------------------------------------------------------------
-- acs_county_migration_flows: ACS 5-year county-to-county flows via the Census flows API,
-- vintages 2010-2020, one request per (vintage, reference state). Not sampled.
-- ----------------------------------------------------------------------------
INSERT INTO dq_results
SELECT 'census', 'acs_county_migration_flows', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '1', 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'census', 'acs_county_migration_flows', 'T2_row_count',
  CASE WHEN n >= 5000000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '5000000',
  'Expected >= 5000000 rows (11 vintages, 5.5 million rows measured 2026-10-16)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true));

SELECT 'acs_county_migration_flows' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true) LIMIT 1;

-- moved_out / net_moved are legitimately NULL for world_region partners (outbound moves abroad
-- are not measured), so a column is flagged only when it is NULL on every row.
INSERT INTO dq_results SELECT 'census', 'acs_county_migration_flows', 'T4_all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_county_migration_flows', 'T5_all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'census', 'acs_county_migration_flows', 'T6_pk_nulls_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '0',
  'NULL county_fips/other_area_name rows plus duplicate (year, county_fips, other_area_name) keys'
FROM (SELECT
        (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true) WHERE county_fips IS NULL OR other_area_name IS NULL)
      + (SELECT COUNT(*) FROM (SELECT year, county_fips, other_area_name FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true) GROUP BY 1, 2, 3 HAVING COUNT(*) > 1)) AS n);

-- All eleven vintages 2010-2020 and every reference state present.
INSERT INTO dq_results
SELECT 'census', 'acs_county_migration_flows', 'T7_years_and_states',
  CASE WHEN yrs = 11 AND sts >= 51 THEN 'pass' ELSE 'fail' END,
  CAST(yrs AS VARCHAR) || ' years / ' || CAST(sts AS VARCHAR) || ' states', '11 years / >=51 states',
  'Distinct vintages (2010-2020) and distinct reference states'
FROM (SELECT COUNT(DISTINCT year) AS yrs, COUNT(DISTINCT SUBSTR(county_fips, 1, 2)) AS sts
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true));

-- Row invariants: 5-digit reference county; other_area_type consistent with the code length;
-- counts and margins non-negative; net = in - out whenever both are present.
INSERT INTO dq_results
SELECT 'census', 'acs_county_migration_flows', 'T7_row_invariants',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, CAST(bad AS VARCHAR), '0',
  'Rows violating key shape, partner type, non-negativity or net = in - out'
FROM (SELECT COUNT(*) AS bad
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true)
      WHERE LENGTH(county_fips) <> 5
         OR other_area_type NOT IN ('county', 'county_subdivision', 'world_region')
         OR (other_area_type = 'county' AND (other_area_fips IS NULL OR LENGTH(other_area_fips) <> 5))
         OR (other_area_type = 'county_subdivision' AND (other_area_fips IS NULL OR LENGTH(other_area_fips) <> 10))
         OR (other_area_type = 'world_region' AND other_area_fips IS NOT NULL)
         OR moved_in < 0 OR moved_out < 0 OR moved_in_moe < 0 OR moved_out_moe < 0 OR net_moved_moe < 0
         OR (moved_in IS NOT NULL AND moved_out IS NOT NULL AND net_moved <> moved_in - moved_out));

-- Each county pair is published from both ends: what county A reports as moved in from B is
-- what B reports as moved out to A.
INSERT INTO dq_results
SELECT 'census', 'acs_county_migration_flows', 'T7_pair_symmetry',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, CAST(bad AS VARCHAR), '0',
  'County-type pairs present from both ends where A.moved_in <> B.moved_out'
FROM (SELECT COUNT(*) AS bad
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true) a
      JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true) b
        ON a.year = b.year AND a.county_fips = b.other_area_fips AND a.other_area_fips = b.county_fips
      WHERE a.other_area_type = 'county' AND b.other_area_type = 'county'
        AND a.moved_in <> b.moved_out);

-- Known anchor (verified live against the Census API, ACS 2016-2020): 2,629 people moved
-- from Los Angeles County CA (06037) to Harris County TX (48201).
INSERT INTO dq_results
SELECT 'census', 'acs_county_migration_flows', 'T7_known_value',
  CASE WHEN v = 2629 THEN 'pass' ELSE 'fail' END, CAST(v AS VARCHAR), '2629',
  'Harris TX moved_in from Los Angeles CA, vintage 2020'
FROM (SELECT COALESCE(SUM(moved_in), 0) AS v
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_county_migration_flows', allow_moved_paths := true)
      WHERE CAST(year AS INTEGER) = 2020 AND county_fips = '48201' AND other_area_fips = '06037');

-- Anchors against published Census figures: the 2019 vintage puts the U.S. resident
-- population at 328,239,523 on July 1, 2019 (sum of the counties); every row has a
-- positive population and non-negative births and deaths.
INSERT INTO dq_results
SELECT 'census', 'pep_components_of_change', 'T7_known_values',
  CASE WHEN pop2019 = 328239523 AND bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(pop2019 AS VARCHAR) || ' / ' || CAST(bad AS VARCHAR), '328239523 / 0',
  'U.S. 2019 population (sum of counties) and rows with population <= 0 or births/deaths < 0'
FROM (SELECT
        COALESCE(SUM(CASE WHEN CAST(year AS INTEGER) = 2019 THEN population_estimate END), 0) AS pop2019,
        COUNT(*) FILTER (WHERE population_estimate <= 0 OR births < 0 OR deaths < 0) AS bad
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_components_of_change', allow_moved_paths := true));

-- acs_housing_tenure_by_age: B25007 tenure identity — owner + renter = total (row-weighted
-- ACS totals are exactly additive; sentinel-normalized nulls skip the check via IS NOT NULL)
INSERT INTO dq_results
SELECT
  'census', 'acs_housing_tenure_by_age', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where owner_occupied + renter_occupied != total_occupied'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure_by_age', allow_moved_paths := true)
  WHERE owner_occupied IS NOT NULL AND renter_occupied IS NOT NULL AND total_occupied IS NOT NULL
    AND owner_occupied + renter_occupied != total_occupied
);

-- acs_housing_tenure_by_age: owner age bands sum to owner_occupied
INSERT INTO dq_results
SELECT
  'census', 'acs_housing_tenure_by_age', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where the 9 owner age bands do not sum to owner_occupied'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure_by_age', allow_moved_paths := true)
  WHERE owner_occupied IS NOT NULL
    AND owner_15_to_24 IS NOT NULL AND owner_25_to_34 IS NOT NULL AND owner_35_to_44 IS NOT NULL
    AND owner_45_to_54 IS NOT NULL AND owner_55_to_59 IS NOT NULL AND owner_60_to_64 IS NOT NULL
    AND owner_65_to_74 IS NOT NULL AND owner_75_to_84 IS NOT NULL AND owner_85_and_over IS NOT NULL
    AND owner_15_to_24 + owner_25_to_34 + owner_35_to_44 + owner_45_to_54 + owner_55_to_59
        + owner_60_to_64 + owner_65_to_74 + owner_75_to_84 + owner_85_and_over
      != owner_occupied
);

-- acs_housing_tenure_by_age: renter age bands sum to renter_occupied
INSERT INTO dq_results
SELECT
  'census', 'acs_housing_tenure_by_age', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where the 9 renter age bands do not sum to renter_occupied'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure_by_age', allow_moved_paths := true)
  WHERE renter_occupied IS NOT NULL
    AND renter_15_to_24 IS NOT NULL AND renter_25_to_34 IS NOT NULL AND renter_35_to_44 IS NOT NULL
    AND renter_45_to_54 IS NOT NULL AND renter_55_to_59 IS NOT NULL AND renter_60_to_64 IS NOT NULL
    AND renter_65_to_74 IS NOT NULL AND renter_75_to_84 IS NOT NULL AND renter_85_and_over IS NOT NULL
    AND renter_15_to_24 + renter_25_to_34 + renter_35_to_44 + renter_45_to_54 + renter_55_to_59
        + renter_60_to_64 + renter_65_to_74 + renter_75_to_84 + renter_85_and_over
      != renter_occupied
);

-- acs_housing_tenure_by_age: state FIPS must be 2 characters
INSERT INTO dq_results
SELECT
  'census', 'acs_housing_tenure_by_age', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where state FIPS is not 2 characters'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure_by_age', allow_moved_paths := true)
  WHERE state IS NOT NULL AND LENGTH(state) != 2
);

-- uscis_i765_employment_authorization: filing_type is one of the four USCIS groups, counts are
-- non-negative, and the fiscal quarter is 1-4
INSERT INTO dq_results
SELECT
  'census', 'uscis_i765_employment_authorization', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows with an unknown filing_type, fiscal_quarter outside 1-4, or a negative count'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/uscis_i765_employment_authorization', allow_moved_paths := true)
  WHERE filing_type NOT IN ('Initial', 'Renewal', 'Replacement', 'Not Requested')
     OR fiscal_quarter NOT BETWEEN 1 AND 4
     OR receipts < 0 OR approvals < 0 OR denials < 0 OR pending < 0
);

-- uscis_i765_employment_authorization: PK (year, quarter, category, filing_type) must be unique
INSERT INTO dq_results
SELECT
  'census', 'uscis_i765_employment_authorization', 'pk_duplication',
  CASE WHEN dups = 0 THEN 'pass' ELSE 'fail' END,
  CAST(dups AS VARCHAR), '0',
  'duplicate (year, fiscal_quarter, ead_category, filing_type) keys'
FROM (
  SELECT COUNT(*) AS dups FROM (
    SELECT year, fiscal_quarter, ead_category, filing_type
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/uscis_i765_employment_authorization', allow_moved_paths := true)
    GROUP BY 1, 2, 3, 4 HAVING COUNT(*) > 1
  )
);

-- ============================================================================
-- VIEW: county_vulnerability_profile (acs_poverty/acs_age/acs_disability/acs_language
-- joined at county grain, left-joined cross-schema to disasters.disaster_declarations)
-- Views have no physical iceberg path, so the join is replicated here directly against
-- the iceberg path of each base table. The real Calcite-path check lives in model-verify.
-- ============================================================================

INSERT INTO dq_results
SELECT 'census', 'county_vulnerability_profile', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1,
  'Row count from the county_vulnerability_profile join (acs_poverty/acs_age/acs_disability/acs_language on county_fips+year, geography=county)'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_poverty', allow_moved_paths := true) p
  JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_age', allow_moved_paths := true) a
    ON p.county_fips = a.county_fips AND p.year = a.year AND a.geography = 'county'
  JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_disability', allow_moved_paths := true) d
    ON p.county_fips = d.county_fips AND p.year = d.year AND d.geography = 'county'
  JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_language', allow_moved_paths := true) l
    ON p.county_fips = l.county_fips AND p.year = l.year AND l.geography = 'county'
  WHERE p.geography = 'county'
);

INSERT INTO dq_results
SELECT 'census', 'county_vulnerability_profile', 'T7_disaster_join_coverage',
  CASE WHEN matched > 0 THEN 'pass' ELSE 'warn' END, matched, 1,
  'County-years in the profile join with at least one matching disasters.disaster_declarations row (cross-schema join on county_fips)'
FROM (
  SELECT COUNT(*) AS matched
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_poverty', allow_moved_paths := true) p
  JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_age', allow_moved_paths := true) a
    ON p.county_fips = a.county_fips AND p.year = a.year AND a.geography = 'county'
  JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_disability', allow_moved_paths := true) d
    ON p.county_fips = d.county_fips AND p.year = d.year AND d.geography = 'county'
  JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_language', allow_moved_paths := true) l
    ON p.county_fips = l.county_fips AND p.year = l.year AND l.geography = 'county'
  JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/disasters/disaster_declarations', allow_moved_paths := true) fd
    ON p.county_fips = fd.county_fips
  WHERE p.geography = 'county'
);

-- ----------------------------------------------------------------------------
-- acs_congressional_district: ACS 5-year profile by congressional district, vintages 2012+,
-- one Census API request per vintage. Not sampled: the whole table is read.
-- ----------------------------------------------------------------------------
INSERT INTO dq_results
SELECT 'census', 'acs_congressional_district', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '1', 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'census', 'acs_congressional_district', 'T2_row_count',
  CASE WHEN n >= 5600 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '5600',
  'Expected >= 5600 rows (13 vintages 2012-2024 x 437 districts)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true));

SELECT 'acs_congressional_district' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true) LIMIT 1;

INSERT INTO dq_results SELECT 'census', 'acs_congressional_district', 'T4_all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_congressional_district', 'T5_all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'census', 'acs_congressional_district', 'T6_pk_nulls_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '0',
  'NULL cd_fips/congress rows plus duplicate (year, cd_fips) keys'
FROM (SELECT
        (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true) WHERE cd_fips IS NULL OR congress IS NULL)
      + (SELECT COUNT(*) FROM (SELECT year, cd_fips FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true) GROUP BY 1, 2 HAVING COUNT(*) > 1)) AS n);

-- Every vintage carries exactly 435 voting districts (district <> 98) and the 2 non-voting
-- delegates (District of Columbia and Puerto Rico, district 98), under exactly one Congress.
INSERT INTO dq_results
SELECT 'census', 'acs_congressional_district', 'T7_districts_per_vintage',
  CASE WHEN yrs >= 13 AND bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(yrs AS VARCHAR) || ' vintages / ' || CAST(bad AS VARCHAR) || ' bad', '>=13 vintages / 0 bad',
  'Vintages loaded, and vintages without 435 voting rows, 2 delegate rows and a single Congress'
FROM (SELECT COUNT(*) AS yrs, COUNT(*) FILTER (WHERE voting <> 435 OR delegates <> 2 OR congresses <> 1) AS bad
      FROM (SELECT year,
                   COUNT(*) FILTER (WHERE district <> '98') AS voting,
                   COUNT(*) FILTER (WHERE district = '98') AS delegates,
                   COUNT(DISTINCT congress) AS congresses
            FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true)
            GROUP BY year));

-- Congress is the Congress seated when the vintage's lines were drawn: it never decreases with
-- the vintage year, and the code is a plausible Congress number.
INSERT INTO dq_results
SELECT 'census', 'acs_congressional_district', 'T7_congress_monotonic',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, CAST(bad AS VARCHAR), '0',
  'Vintages whose Congress is lower than an earlier vintage, or outside 111..119'
FROM (SELECT COUNT(*) AS bad FROM (
        SELECT CAST(year AS INTEGER) AS y, MIN(congress) AS c,
               MAX(MIN(congress)) OVER (ORDER BY CAST(year AS INTEGER) ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true)
        GROUP BY year)
      WHERE c < prev_max OR c < 111 OR c > 119);

-- Row invariants: cd_fips = state + district, percentages within 0..100, no negative counts,
-- parts never exceed their wholes.
INSERT INTO dq_results
SELECT 'census', 'acs_congressional_district', 'T7_row_invariants',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, CAST(bad AS VARCHAR), '0',
  'Rows violating key shape, percent range, non-negativity or part <= whole'
FROM (SELECT COUNT(*) AS bad
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true)
      WHERE LENGTH(cd_fips) <> 4 OR cd_fips <> state_fips || district
         OR total_population <= 0 OR unemployed > civilian_labor_force OR below_poverty > poverty_universe
         OR population_65_plus > total_population OR bachelors_or_higher > population_25_plus
         OR foreign_born > total_population OR owner_occupied_units > occupied_housing_units
         OR renters_cost_burdened > renter_households OR owners_cost_burdened > mortgaged_owner_households
         OR unemployment_rate_pct NOT BETWEEN 0 AND 100 OR poverty_rate_pct NOT BETWEEN 0 AND 100
         OR population_65_plus_pct NOT BETWEEN 0 AND 100 OR bachelors_or_higher_pct NOT BETWEEN 0 AND 100
         OR foreign_born_pct NOT BETWEEN 0 AND 100 OR renter_cost_burden_pct NOT BETWEEN 0 AND 100
         OR owner_cost_burden_pct NOT BETWEEN 0 AND 100 OR median_household_income <= 0);

-- Districts tile the states exactly, so the districts of each state sum to the state ACS 5-year
-- population of the same vintage (census.acs_population, geography = state, same API variable
-- B01003 family); the sum over states is the national total.
INSERT INTO dq_results
SELECT 'census', 'acs_congressional_district', 'T7_population_reconciles',
  CASE WHEN n_years >= 12 AND worst = 0 THEN 'pass' ELSE 'fail' END,
  CAST(n_years AS VARCHAR) || ' vintages / worst diff ' || CAST(worst AS VARCHAR), '>=12 vintages / 0',
  'Largest absolute difference between the district sum and census.acs_population state sum per state-vintage'
FROM (SELECT COUNT(DISTINCT y) AS n_years, COALESCE(MAX(ABS(d - s)), 999999999) AS worst FROM (
        SELECT CAST(c.year AS INTEGER) AS y, c.state_fips, SUM(c.total_population) AS d, MAX(p.total_population) AS s
        FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true) c
        JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population', allow_moved_paths := true) p
          ON p.year = c.year AND p.geography = 'state' AND p.state = c.state_fips
        GROUP BY 1, 2));

-- Anchors against published figures (ACS 5-year, Data USA / Census Bureau district profiles,
-- checked 2026-10-17): Wyoming at-large median household income $74,815 (vintage 2023, 118th
-- Congress) and $76,176 (2024, 119th); Mississippi 1 $60,877 (2023); New York 12 (Manhattan)
-- 2024 population 719,940, median household income $147,775 and median age 40.3.
INSERT INTO dq_results
SELECT 'census', 'acs_congressional_district', 'T7_known_values',
  CASE WHEN hits = 4 THEN 'pass' ELSE 'fail' END, CAST(hits AS VARCHAR), '4',
  'Spot-check districts: WY-AL 2023/2024 income, MS-1 2023 income, NY-12 2024 population, income and median age'
FROM (SELECT COUNT(*) AS hits
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_congressional_district', allow_moved_paths := true)
      WHERE (CAST(year AS INTEGER) = 2023 AND cd_fips = '5600' AND congress = 118 AND median_household_income = 74815)
         OR (CAST(year AS INTEGER) = 2024 AND cd_fips = '5600' AND congress = 119 AND median_household_income = 76176)
         OR (CAST(year AS INTEGER) = 2023 AND cd_fips = '2801' AND median_household_income = 60877)
         OR (CAST(year AS INTEGER) = 2024 AND cd_fips = '3612' AND total_population = 719940
             AND median_household_income = 147775 AND ROUND(median_age, 1) = 40.3));

-- ============================================================================
-- RESULTS SUMMARY
-- ============================================================================

SELECT * FROM dq_results ORDER BY tbl, test;

-- Per-table summary
SELECT
  tbl                                                              AS table_name,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)               AS fails,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END)               AS warns,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)               AS passes,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END                                                             AS table_result
FROM dq_results
GROUP BY tbl
ORDER BY table_result DESC, tbl;

-- Schema-level summary
SELECT
  'census'                                                        AS schema_name,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)               AS total_fails,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END)               AS total_warns,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)               AS total_passes,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END                                                             AS schema_result
FROM dq_results;
