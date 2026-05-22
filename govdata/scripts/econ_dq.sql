-- dq-lookback: 1
-- ============================================================================
-- Data Quality Checks: econ schema
-- ============================================================================
-- Tests per table:
--   T1  existence        — at least 1 row is readable
--   T2  row_count        — row count meets conservative minimum threshold
--   T3  sample           — print 1 sample row to console (informational)
--   T4  all_null_cols    — no column is 100% NULL across all rows
--   T5  all_same_value   — no non-null column has only one distinct value
--   T6  pk_nulls         — SKIPPED: all econ columns are declared nullable
--   T7  expected_values  — domain-specific sanity checks
--
-- Source: iceberg_scan('s3://govdata-parquet-v1/econ/{table}', allow_moved_paths := true)
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
  'econ'    AS schema,
  tbl,
  'existence' AS test,
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END AS status,
  CAST(n AS VARCHAR)  AS value,
  '1'                 AS threshold,
  CASE WHEN n > 0 THEN 'table is readable' ELSE 'table returned 0 rows — may not yet be ingested' END AS detail
FROM (
  SELECT 'employment_statistics' AS tbl, (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/employment_statistics', allow_moved_paths := true) LIMIT 1) t) AS n
  UNION ALL SELECT 'inflation_metrics',    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/inflation_metrics',    allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'regional_cpi',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_cpi',         allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'metro_cpi',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_cpi',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'state_industry',       (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_industry',       allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'state_wages',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_wages',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'metro_industry',       (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_industry',       allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'metro_wages',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_wages',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'county_qcew',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_qcew',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'county_wages',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_wages',         allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'jolts_regional',       (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_regional',       allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'jolts_state',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_state',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'wage_growth',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/wage_growth',          allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'regional_employment',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_employment',  allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'treasury_yields',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/treasury_yields',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'federal_debt',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/federal_debt',         allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'world_indicators',     (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/world_indicators',     allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'fred_indicators',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/fred_indicators',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'national_accounts',    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/national_accounts',    allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'state_personal_income',(SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_personal_income',allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'state_gdp',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_gdp',            allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'state_quarterly_income',(SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_income',allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'state_quarterly_gdp',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_gdp',  allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'state_consumption',    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_consumption',    allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'regional_income',      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_income',      allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'ita_data',             (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/ita_data',             allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'gdp_statistics',       (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/gdp_statistics',       allow_moved_paths := true) LIMIT 1) t)
  UNION ALL SELECT 'industry_gdp',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/econ/industry_gdp',         allow_moved_paths := true) LIMIT 1) t)
) src;

-- ============================================================================
-- T2: ROW COUNT — conservative minimums based on known data scale
-- ============================================================================

INSERT INTO dq_results
SELECT
  'econ' AS schema,
  tbl,
  'row_count' AS test,
  CASE WHEN n >= threshold THEN 'pass' ELSE 'fail' END AS status,
  CAST(n AS VARCHAR)         AS value,
  CAST(threshold AS VARCHAR) AS threshold,
  CASE WHEN n >= threshold THEN 'row count meets minimum'
       ELSE 'row count below minimum — ingestion may be incomplete or failed'
  END AS detail
FROM (
  SELECT 'employment_statistics'  AS tbl, (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/employment_statistics',  allow_moved_paths := true)) AS n, 100    AS threshold
  UNION ALL SELECT 'inflation_metrics',     (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/inflation_metrics',     allow_moved_paths := true)), 100
  UNION ALL SELECT 'regional_cpi',          (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_cpi',          allow_moved_paths := true)), 200
  UNION ALL SELECT 'metro_cpi',             (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_cpi',             allow_moved_paths := true)), 500
  UNION ALL SELECT 'state_industry',        (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_industry',        allow_moved_paths := true)), 50000
  UNION ALL SELECT 'state_wages',           (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_wages',           allow_moved_paths := true)), 200
  UNION ALL SELECT 'metro_industry',        (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_industry',        allow_moved_paths := true)), 10000
  UNION ALL SELECT 'metro_wages',           (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_wages',           allow_moved_paths := true)), 100
  UNION ALL SELECT 'county_qcew',           (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_qcew',           allow_moved_paths := true)), 10000
  UNION ALL SELECT 'county_wages',          (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_wages',          allow_moved_paths := true)), 10000
  UNION ALL SELECT 'jolts_regional',        (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_regional',        allow_moved_paths := true)), 500
  UNION ALL SELECT 'jolts_state',           (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_state',           allow_moved_paths := true)), 10000
  UNION ALL SELECT 'wage_growth',           (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/wage_growth',           allow_moved_paths := true)), 100
  UNION ALL SELECT 'regional_employment',   (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_employment',   allow_moved_paths := true)), 200
  UNION ALL SELECT 'treasury_yields',       (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/treasury_yields',       allow_moved_paths := true)), 1000
  UNION ALL SELECT 'federal_debt',          (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/federal_debt',          allow_moved_paths := true)), 1000
  UNION ALL SELECT 'world_indicators',      (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/world_indicators',      allow_moved_paths := true)), 1000
  UNION ALL SELECT 'fred_indicators',       (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/fred_indicators',       allow_moved_paths := true)), 10000
  UNION ALL SELECT 'national_accounts',     (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/national_accounts',     allow_moved_paths := true)), 100
  UNION ALL SELECT 'state_personal_income', (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_personal_income', allow_moved_paths := true)), 300
  UNION ALL SELECT 'state_gdp',             (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_gdp',             allow_moved_paths := true)), 300
  UNION ALL SELECT 'state_quarterly_income',(SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_income',allow_moved_paths := true)), 1000
  UNION ALL SELECT 'state_quarterly_gdp',   (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_gdp',   allow_moved_paths := true)), 1000
  UNION ALL SELECT 'state_consumption',     (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_consumption',     allow_moved_paths := true)), 300
  UNION ALL SELECT 'regional_income',       (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_income',       allow_moved_paths := true)), 100
  UNION ALL SELECT 'ita_data',              (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/ita_data',              allow_moved_paths := true)), 100
  UNION ALL SELECT 'gdp_statistics',        (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/gdp_statistics',        allow_moved_paths := true)), 100
  UNION ALL SELECT 'industry_gdp',          (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/econ/industry_gdp',          allow_moved_paths := true)), 1000
) src;

-- ============================================================================
-- T3: SAMPLE — print 1 row per table to console (informational only)
-- ============================================================================

SELECT 'employment_statistics'  AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/employment_statistics',  allow_moved_paths := true) LIMIT 1;
SELECT 'inflation_metrics'      AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/inflation_metrics',      allow_moved_paths := true) LIMIT 1;
SELECT 'regional_cpi'           AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_cpi',           allow_moved_paths := true) LIMIT 1;
SELECT 'metro_cpi'              AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_cpi',              allow_moved_paths := true) LIMIT 1;
SELECT 'state_industry'         AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_industry',         allow_moved_paths := true) LIMIT 1;
SELECT 'state_wages'            AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_wages',            allow_moved_paths := true) LIMIT 1;
SELECT 'metro_industry'         AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_industry',         allow_moved_paths := true) LIMIT 1;
SELECT 'metro_wages'            AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_wages',            allow_moved_paths := true) LIMIT 1;
SELECT 'county_qcew'            AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_qcew',            allow_moved_paths := true) LIMIT 1;
SELECT 'county_wages'           AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_wages',           allow_moved_paths := true) LIMIT 1;
SELECT 'jolts_regional'         AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_regional',         allow_moved_paths := true) LIMIT 1;
SELECT 'jolts_state'            AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_state',            allow_moved_paths := true) LIMIT 1;
SELECT 'wage_growth'            AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/wage_growth',            allow_moved_paths := true) LIMIT 1;
SELECT 'regional_employment'    AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_employment',    allow_moved_paths := true) LIMIT 1;
SELECT 'treasury_yields'        AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/treasury_yields',        allow_moved_paths := true) LIMIT 1;
SELECT 'federal_debt'           AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/federal_debt',           allow_moved_paths := true) LIMIT 1;
SELECT 'world_indicators'       AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/world_indicators',       allow_moved_paths := true) LIMIT 1;
SELECT 'fred_indicators'        AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/fred_indicators',        allow_moved_paths := true) LIMIT 1;
SELECT 'national_accounts'      AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/national_accounts',      allow_moved_paths := true) LIMIT 1;
SELECT 'state_personal_income'  AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_personal_income',  allow_moved_paths := true) LIMIT 1;
SELECT 'state_gdp'              AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_gdp',              allow_moved_paths := true) LIMIT 1;
SELECT 'state_quarterly_income' AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_income', allow_moved_paths := true) LIMIT 1;
SELECT 'state_quarterly_gdp'    AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_gdp',    allow_moved_paths := true) LIMIT 1;
SELECT 'state_consumption'      AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_consumption',      allow_moved_paths := true) LIMIT 1;
SELECT 'regional_income'        AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_income',        allow_moved_paths := true) LIMIT 1;
SELECT 'ita_data'               AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/ita_data',               allow_moved_paths := true) LIMIT 1;
SELECT 'gdp_statistics'         AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/gdp_statistics',         allow_moved_paths := true) LIMIT 1;
SELECT 'industry_gdp'           AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/econ/industry_gdp',           allow_moved_paths := true) LIMIT 1;

-- ============================================================================
-- T4: ALL-NULL COLUMNS — no column should be 100% NULL
-- ============================================================================

-- employment_statistics
INSERT INTO dq_results
SELECT 'econ', 'employment_statistics', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/employment_statistics', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- inflation_metrics
INSERT INTO dq_results
SELECT 'econ', 'inflation_metrics', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/inflation_metrics', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- regional_cpi
INSERT INTO dq_results
SELECT 'econ', 'regional_cpi', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_cpi', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- metro_cpi
INSERT INTO dq_results
SELECT 'econ', 'metro_cpi', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_cpi', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- state_industry
INSERT INTO dq_results
SELECT 'econ', 'state_industry', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_industry', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- state_wages
INSERT INTO dq_results
SELECT 'econ', 'state_wages', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_wages', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- metro_industry
INSERT INTO dq_results
SELECT 'econ', 'metro_industry', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_industry', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- metro_wages
INSERT INTO dq_results
SELECT 'econ', 'metro_wages', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_wages', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- county_qcew
INSERT INTO dq_results
SELECT 'econ', 'county_qcew', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_qcew', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- county_wages
INSERT INTO dq_results
SELECT 'econ', 'county_wages', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_wages', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- jolts_regional
INSERT INTO dq_results
SELECT 'econ', 'jolts_regional', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_regional', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- jolts_state
INSERT INTO dq_results
SELECT 'econ', 'jolts_state', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_state', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- wage_growth
INSERT INTO dq_results
SELECT 'econ', 'wage_growth', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/wage_growth', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- regional_employment
INSERT INTO dq_results
SELECT 'econ', 'regional_employment', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_employment', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- treasury_yields
INSERT INTO dq_results
SELECT 'econ', 'treasury_yields', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/treasury_yields', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- federal_debt
INSERT INTO dq_results
SELECT 'econ', 'federal_debt', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/federal_debt', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- world_indicators
INSERT INTO dq_results
SELECT 'econ', 'world_indicators', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/world_indicators', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- fred_indicators
INSERT INTO dq_results
SELECT 'econ', 'fred_indicators', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/fred_indicators', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- national_accounts
INSERT INTO dq_results
SELECT 'econ', 'national_accounts', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/national_accounts', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- state_personal_income
INSERT INTO dq_results
SELECT 'econ', 'state_personal_income', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_personal_income', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- state_gdp
INSERT INTO dq_results
SELECT 'econ', 'state_gdp', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_gdp', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- state_quarterly_income
INSERT INTO dq_results
SELECT 'econ', 'state_quarterly_income', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_income', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- state_quarterly_gdp
INSERT INTO dq_results
SELECT 'econ', 'state_quarterly_gdp', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_gdp', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- state_consumption
INSERT INTO dq_results
SELECT 'econ', 'state_consumption', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_consumption', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- regional_income
INSERT INTO dq_results
SELECT 'econ', 'regional_income', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_income', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- ita_data
INSERT INTO dq_results
SELECT 'econ', 'ita_data', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/ita_data', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- gdp_statistics
INSERT INTO dq_results
SELECT 'econ', 'gdp_statistics', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/gdp_statistics', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- industry_gdp
INSERT INTO dq_results
SELECT 'econ', 'industry_gdp', 'all_null_cols', 'fail',
  column_name, '< 100% null', 'column is entirely NULL — likely a schema or ingestion bug'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/industry_gdp', allow_moved_paths := true))
WHERE null_percentage = 100.0;

-- ============================================================================
-- T5: ALL-SAME-VALUE — no non-null column should have only one distinct value
-- ============================================================================

-- employment_statistics
INSERT INTO dq_results
SELECT 'econ', 'employment_statistics', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/employment_statistics', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- inflation_metrics
INSERT INTO dq_results
SELECT 'econ', 'inflation_metrics', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/inflation_metrics', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- regional_cpi
INSERT INTO dq_results
SELECT 'econ', 'regional_cpi', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_cpi', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- metro_cpi
INSERT INTO dq_results
SELECT 'econ', 'metro_cpi', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_cpi', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- state_industry
INSERT INTO dq_results
SELECT 'econ', 'state_industry', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_industry', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- state_wages
INSERT INTO dq_results
SELECT 'econ', 'state_wages', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_wages', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- metro_industry
INSERT INTO dq_results
SELECT 'econ', 'metro_industry', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_industry', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- metro_wages
INSERT INTO dq_results
SELECT 'econ', 'metro_wages', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/metro_wages', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- county_qcew
INSERT INTO dq_results
SELECT 'econ', 'county_qcew', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_qcew', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- county_wages
INSERT INTO dq_results
SELECT 'econ', 'county_wages', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/county_wages', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- jolts_regional
INSERT INTO dq_results
SELECT 'econ', 'jolts_regional', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_regional', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- jolts_state
INSERT INTO dq_results
SELECT 'econ', 'jolts_state', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_state', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- wage_growth
INSERT INTO dq_results
SELECT 'econ', 'wage_growth', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/wage_growth', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- regional_employment
INSERT INTO dq_results
SELECT 'econ', 'regional_employment', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_employment', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- treasury_yields
INSERT INTO dq_results
SELECT 'econ', 'treasury_yields', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/treasury_yields', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- federal_debt
INSERT INTO dq_results
SELECT 'econ', 'federal_debt', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/federal_debt', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- world_indicators
INSERT INTO dq_results
SELECT 'econ', 'world_indicators', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/world_indicators', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- fred_indicators
INSERT INTO dq_results
SELECT 'econ', 'fred_indicators', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/fred_indicators', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- national_accounts
INSERT INTO dq_results
SELECT 'econ', 'national_accounts', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/national_accounts', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- state_personal_income
INSERT INTO dq_results
SELECT 'econ', 'state_personal_income', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_personal_income', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- state_gdp
INSERT INTO dq_results
SELECT 'econ', 'state_gdp', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_gdp', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- state_quarterly_income
INSERT INTO dq_results
SELECT 'econ', 'state_quarterly_income', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_income', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- state_quarterly_gdp
INSERT INTO dq_results
SELECT 'econ', 'state_quarterly_gdp', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_quarterly_gdp', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- state_consumption
INSERT INTO dq_results
SELECT 'econ', 'state_consumption', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_consumption', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- regional_income
INSERT INTO dq_results
SELECT 'econ', 'regional_income', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_income', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- ita_data
INSERT INTO dq_results
SELECT 'econ', 'ita_data', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/ita_data', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- gdp_statistics
INSERT INTO dq_results
SELECT 'econ', 'gdp_statistics', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/gdp_statistics', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- industry_gdp
INSERT INTO dq_results
SELECT 'econ', 'industry_gdp', 'all_same_value', 'warn',
  column_name, '> 1 distinct value', 'column has only 1 distinct value across all rows — may be a constant or ingestion issue'
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ/industry_gdp', allow_moved_paths := true))
WHERE approx_unique <= 1 AND null_percentage < 100.0;

-- ============================================================================
-- T6: PK NULLS — SKIPPED
-- All columns in the econ schema are declared nullable: true.
-- No non-nullable business key columns exist to check.
-- ============================================================================

-- ============================================================================
-- T7: EXPECTED VALUES — domain-specific sanity checks
-- ============================================================================

-- employment_statistics: unemployment rate series (LNS14000000) value in [0, 100]
INSERT INTO dq_results
SELECT
  'econ', 'employment_statistics', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where unemployment rate series value is outside [0,100]'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/econ/employment_statistics', allow_moved_paths := true)
  WHERE series = 'LNS14000000' AND (value < 0 OR value > 100)
);

-- inflation_metrics: CPI All Urban (CUUR0000SA0) index value should be > 0
INSERT INTO dq_results
SELECT
  'econ', 'inflation_metrics', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where CPI All Urban (CUUR0000SA0) value is <= 0'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/econ/inflation_metrics', allow_moved_paths := true)
  WHERE series = 'CUUR0000SA0' AND value IS NOT NULL AND value <= 0
);

-- regional_cpi: area_code must be one of the 4 Census region codes
INSERT INTO dq_results
SELECT
  'econ', 'regional_cpi', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows with area_code not in known Census region codes (0100,0200,0300,0400)'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/econ/regional_cpi', allow_moved_paths := true)
  WHERE area_code IS NOT NULL AND area_code NOT IN ('0100','0200','0300','0400')
);

-- jolts_regional: region_code must be known JOLTS region codes
INSERT INTO dq_results
SELECT
  'econ', 'jolts_regional', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows with region_code not in known JOLTS codes (00,NE,MW,SO,WE)'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/econ/jolts_regional', allow_moved_paths := true)
  WHERE region_code IS NOT NULL AND region_code NOT IN ('00','NE','MW','SO','WE')
);

-- federal_debt: total public debt outstanding must be > 0
INSERT INTO dq_results
SELECT
  'econ', 'federal_debt', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where tot_pub_debt_out_amt is NULL or <= 0'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/econ/federal_debt', allow_moved_paths := true)
  WHERE tot_pub_debt_out_amt IS NULL OR tot_pub_debt_out_amt <= 0
);

-- treasury_yields: avg interest rate should be >= 0 (warn, not fail — rates can go negative)
INSERT INTO dq_results
SELECT
  'econ', 'treasury_yields', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  CAST(bad AS VARCHAR), '0',
  'rows where avg_interest_rate_amt < 0 (negative rates are unusual)'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/econ/treasury_yields', allow_moved_paths := true)
  WHERE avg_interest_rate_amt IS NOT NULL AND avg_interest_rate_amt < 0
);

-- state_industry: employment value in thousands must be >= 0
INSERT INTO dq_results
SELECT
  'econ', 'state_industry', 'expected_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  CAST(bad AS VARCHAR), '0',
  'rows where employment value (thousands) is negative'
FROM (
  SELECT COUNT(*) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_industry', allow_moved_paths := true)
  WHERE value IS NOT NULL AND value < 0
);

-- state_wages: must cover all 51 US states+DC (distinct area_fips ending in 000)
INSERT INTO dq_results
SELECT
  'econ', 'state_wages', 'expected_values',
  CASE WHEN n >= 51 THEN 'pass' ELSE 'fail' END,
  CAST(n AS VARCHAR), '51',
  'distinct state area_fips values (expecting 51 — 50 states + DC)'
FROM (
  SELECT COUNT(DISTINCT area_fips) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/econ/state_wages', allow_moved_paths := true)
  WHERE area_fips LIKE '%000'
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
  'econ'                                                          AS schema_name,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)               AS total_fails,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END)               AS total_warns,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)               AS total_passes,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END                                                             AS schema_result
FROM dq_results;
