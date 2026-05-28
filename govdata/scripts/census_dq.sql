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
  UNION ALL SELECT 'acs_migration',           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration',           allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_occupation',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_industry',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_internet',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_nativity',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_marital_status',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_household_type',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'acs_housing_tenure',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure',      allow_moved_paths := true) LIMIT 1) t)
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
  UNION ALL SELECT 'trade_exports',           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_exports',           allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'trade_imports',           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_imports',           allow_moved_paths := true) LIMIT 1) t)
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
  UNION ALL SELECT 'acs_migration',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration',           allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_occupation',          (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation',          allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_industry',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry',            allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_internet',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet',            allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_nativity',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity',            allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_marital_status',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status',      allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_household_type',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type',      allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_housing_tenure',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure',      allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs_income_distribution', (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_distribution', allow_moved_paths := true)), 10000
  UNION ALL SELECT 'decennial_housing',       (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_housing',       allow_moved_paths := true)), 5000
  UNION ALL SELECT 'pep_population',          (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population',          allow_moved_paths := true)), 10000
  UNION ALL SELECT 'cbp_establishments',      (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cbp_establishments',      allow_moved_paths := true)), 10000
  UNION ALL SELECT 'acs1_population',         (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_population',         allow_moved_paths := true)), 250
  UNION ALL SELECT 'acs1_income',             (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_income',             allow_moved_paths := true)), 250
  UNION ALL SELECT 'economic_census',         (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/economic_census',         allow_moved_paths := true)), 5000
  UNION ALL SELECT 'saipe_poverty',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/saipe_poverty',           allow_moved_paths := true)), 10000
  UNION ALL SELECT 'sahie_insurance',         (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/sahie_insurance',         allow_moved_paths := true)), 10000
  UNION ALL SELECT 'bds_dynamics',            (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/bds_dynamics',            allow_moved_paths := true)), 1000
  UNION ALL SELECT 'abs_characteristics',     (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/abs_characteristics',     allow_moved_paths := true)), 1000
  UNION ALL SELECT 'nonemployer_statistics',  (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/nonemployer_statistics',  allow_moved_paths := true)), 10000
  UNION ALL SELECT 'building_permits',        (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/building_permits',        allow_moved_paths := true)), 250
  UNION ALL SELECT 'qwi_employment',          (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/qwi_employment',          allow_moved_paths := true)), 10000
  UNION ALL SELECT 'lodes_workplace',         (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/lodes_workplace',         allow_moved_paths := true)), 10000
  UNION ALL SELECT 'trade_exports',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_exports',           allow_moved_paths := true)), 1000
  UNION ALL SELECT 'trade_imports',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_imports',           allow_moved_paths := true)), 1000
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
SELECT 'acs_migration'          AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration',          allow_moved_paths := true) LIMIT 1;
SELECT 'acs_occupation'         AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation',         allow_moved_paths := true) LIMIT 1;
SELECT 'acs_industry'           AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry',           allow_moved_paths := true) LIMIT 1;
SELECT 'acs_internet'           AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet',           allow_moved_paths := true) LIMIT 1;
SELECT 'acs_nativity'           AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity',           allow_moved_paths := true) LIMIT 1;
SELECT 'acs_marital_status'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status',     allow_moved_paths := true) LIMIT 1;
SELECT 'acs_household_type'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type',     allow_moved_paths := true) LIMIT 1;
SELECT 'acs_housing_tenure'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure',     allow_moved_paths := true) LIMIT 1;
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
SELECT 'trade_exports'          AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_exports',          allow_moved_paths := true) LIMIT 1;
SELECT 'trade_imports'          AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_imports',          allow_moved_paths := true) LIMIT 1;

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
INSERT INTO dq_results SELECT 'census', 'acs_migration', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_occupation', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_industry', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_internet', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_nativity', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_marital_status', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_household_type', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_housing_tenure', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_income_distribution', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_distribution', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'decennial_housing', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_housing', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'pep_population', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population', allow_moved_paths := true)) WHERE null_percentage = 100.0;
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
INSERT INTO dq_results SELECT 'census', 'qwi_employment', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/qwi_employment', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'lodes_workplace', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/lodes_workplace', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'trade_exports', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_exports', allow_moved_paths := true)) WHERE null_percentage = 100.0;
INSERT INTO dq_results SELECT 'census', 'trade_imports', 'all_null_cols', 'fail', column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_imports', allow_moved_paths := true)) WHERE null_percentage = 100.0;

-- ============================================================================
-- T5: ALL-SAME-VALUE — no non-null column should have only one distinct value
-- ============================================================================

INSERT INTO dq_results SELECT 'census', 'acs_population', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_population', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_income', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_housing', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_education', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_education', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_employment', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_employment', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_poverty', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_poverty', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'decennial_population', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_population', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_race_ethnicity', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_race_ethnicity', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_age', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_age', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_commuting', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_commuting', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_health_insurance', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_health_insurance', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_language', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_language', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_disability', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_disability', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_veterans', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_veterans', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_migration', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_migration', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_occupation', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_occupation', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_industry', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_industry', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_internet', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_internet', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_nativity', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_nativity', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_marital_status', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_marital_status', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_household_type', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_household_type', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_housing_tenure', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_housing_tenure', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs_income_distribution', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_income_distribution', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'decennial_housing', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/decennial_housing', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'pep_population', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/pep_population', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'cbp_establishments', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/cbp_establishments', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs1_population', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_population', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'acs1_income', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs1_income', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'economic_census', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/economic_census', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'saipe_poverty', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/saipe_poverty', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'sahie_insurance', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/sahie_insurance', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'bds_dynamics', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/bds_dynamics', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'abs_characteristics', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/abs_characteristics', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'nonemployer_statistics', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/nonemployer_statistics', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'building_permits', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/building_permits', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'qwi_employment', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/qwi_employment', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'lodes_workplace', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/lodes_workplace', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'trade_exports', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_exports', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;
INSERT INTO dq_results SELECT 'census', 'trade_imports', 'all_same_value', 'warn', column_name, '> 1 distinct value', 'column has only 1 distinct value — may be a constant or ingestion issue' FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/trade_imports', allow_moved_paths := true)) WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- ============================================================================
-- T6: PK NULLS — SKIPPED
-- All columns in the census schema are declared nullable: true.
-- No non-nullable business key columns exist to check.
-- ============================================================================

-- ============================================================================
-- T7: EXPECTED VALUES — domain-specific sanity checks
-- ============================================================================

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
