-- Crime Data Quality Check
-- Sources: FBI Crime Data Explorer (CDE) + Bureau of Justice Statistics (BJS)
-- Tables: 16 partitioned tables (11 CDE + 5 BJS)
-- Run: source .env.prod && envsubst < scripts/crime_dq.sql | duckdb

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs;  LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_region='auto';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_url_style='path';

CREATE TEMP TABLE dq_results (
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,
  actual    DOUBLE,
  expected  DOUBLE,
  detail    VARCHAR
);

-- ============================================================================
-- cde_agencies (~19,600 law enforcement agencies; partitioned by type/state_abbr)
-- No year dimension — T8 checks row count instead of year range.
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'T2_row_count',
  CASE WHEN COUNT(*) >= 19000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 19000, 'expected >= 19,000 agencies across 51 jurisdictions'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols (warn — expected source characteristics)
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year', 'state_abbr')
) t;

-- T5: all_same_value (warn — partition columns expected constant per-partition scan)
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year', 'state_abbr')
) t;

-- T6: pk_nulls (ori NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'ori IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true)
WHERE ori IS NULL;

-- T7: 51 distinct state_abbr values (50 states + DC)
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT state_abbr) >= 51 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT state_abbr), 51, 'distinct state_abbr (50 states + DC)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true);

-- T8: worker coverage — no year dimension; row count serves as proxy
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'T8_worker_coverage',
  CASE WHEN COUNT(*) >= 19000 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 19000,
  'no year dim; row count >= 19000 confirms historical worker wrote agency registry'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true);

-- ============================================================================
-- cde_offenses (crime rates by state, offense, month; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true);

-- T2: row_count (51 states × 10 offenses × 12 months × 4+ years)
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'T2_row_count',
  CASE WHEN COUNT(*) >= 20000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 20000, '51 × 10 offenses × 12 months × 4 years'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (state_abbr, offense_code NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'state_abbr IS NULL OR offense_code IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true)
WHERE state_abbr IS NULL OR offense_code IS NULL;

-- T7: offense_code in known UCR set
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'T7_expected_values',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, 'offense_code outside known UCR set'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true)
WHERE offense_code NOT IN (
  'violent-crime','property-crime','homicide','aggravated-assault','rape',
  'robbery','burglary','larceny','motor-vehicle-theft','arson'
);

-- T8: worker coverage — historical: MIN(year) <= 2025; daily: MAX(year) >= 2026
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2025 AND MAX(CAST(year AS INT)) >= 2026 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2026,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | historical: MIN<=2025, daily: MAX>=2026'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true);

-- ============================================================================
-- cde_police_employment (officers/1000 pop by state/year; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'T2_row_count',
  CASE WHEN COUNT(*) >= 200 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 200, '51 states × 4+ years'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'state_abbr IS NULL OR year IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true)
WHERE state_abbr IS NULL OR year IS NULL;

-- T7: officers_per_1000 non-negative where not null
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'T7_expected_values',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, 'officers_per_1000 < 0'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true)
WHERE officers_per_1000 IS NOT NULL AND officers_per_1000 < 0;

-- T8: worker coverage
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2025 AND MAX(CAST(year AS INT)) >= 2026 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2026,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | historical: MIN<=2025, daily: MAX>=2026'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true);

-- ============================================================================
-- cde_hate_crimes (bias/incident stats by state/year; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'T2_row_count',
  CASE WHEN COUNT(*) >= 5000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 5000, 'bias/incident categories across 51 states × 4+ years'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'state_abbr IS NULL OR year IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true)
WHERE state_abbr IS NULL OR year IS NULL;

-- T7: count non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'T7_expected_values',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, 'count < 0 (unexpected negative hate crime count)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true)
WHERE count < 0;

-- T8: worker coverage
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2025 AND MAX(CAST(year AS INT)) >= 2026 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2026,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | historical: MIN<=2025, daily: MAX>=2026'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true);

-- ============================================================================
-- cde_use_of_force (use-of-force metrics by state/year; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1000, 'participation/incident breakdowns across 51 states × 4+ years'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'state_abbr IS NULL OR year IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true)
WHERE state_abbr IS NULL OR year IS NULL;

-- T7: 51 distinct state_abbr values
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT state_abbr) >= 51 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT state_abbr), 51, 'distinct state_abbr (50 states + DC)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true);

-- T8: worker coverage
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2025 AND MAX(CAST(year AS INT)) >= 2026 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2026,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | historical: MIN<=2025, daily: MAX>=2026'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true);

-- ============================================================================
-- cde_crime_agency (agency-level offenses by ORI; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true);

-- T2: row_count (~19,600 agencies × 10 offenses × 12 months)
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'T2_row_count',
  CASE WHEN COUNT(*) >= 100000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 100000, '~19,600 agencies × 10 offenses × 12 months'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (ori, offense_code NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'ori IS NULL OR offense_code IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true)
WHERE ori IS NULL OR offense_code IS NULL;

-- T7: distinct ORI count (expect close to 19,600)
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT ori) >= 10000 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT ori), 10000, 'distinct ORI count (expect close to 19,600)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true);

-- T8: worker coverage
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2025 AND MAX(CAST(year AS INT)) >= 2026 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2026,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | historical: MIN<=2025, daily: MAX>=2026'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true);

-- ============================================================================
-- cde_arrests (arrests by state/offense/demographics; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'T2_row_count',
  CASE WHEN COUNT(*) >= 10000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 10000, '51 states × age/sex/race breakdowns × 4+ years'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (state_abbr, year, offense_code NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'state_abbr IS NULL OR year IS NULL OR offense_code IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true)
WHERE state_abbr IS NULL OR year IS NULL OR offense_code IS NULL;

-- T7: arrest count non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'T7_expected_values',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, 'count < 0 (unexpected negative arrest count)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true)
WHERE count < 0;

-- T8: worker coverage
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2025 AND MAX(CAST(year AS INT)) >= 2026 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2026,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | historical: MIN<=2025, daily: MAX>=2026'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true);

-- ============================================================================
-- cde_shr (Supplementary Homicide Reports; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'T2_row_count',
  CASE WHEN COUNT(*) >= 2000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 2000, 'victim/offender demographics × 51 states × 4+ years'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'state_abbr IS NULL OR year IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true)
WHERE state_abbr IS NULL OR year IS NULL;

-- T7: count non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'T7_expected_values',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, 'count < 0 (unexpected negative SHR count)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true)
WHERE count < 0;

-- T8: worker coverage
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2025 AND MAX(CAST(year AS INT)) >= 2026 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2026,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | historical: MIN<=2025, daily: MAX>=2026'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true);

-- ============================================================================
-- cde_leoka (Officers Killed/Assaulted; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1000, 'killed/assaulted breakdowns × 51 states × 4+ years'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'state_abbr IS NULL OR year IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true)
WHERE state_abbr IS NULL OR year IS NULL;

-- T7: count non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'T7_expected_values',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, 'count < 0 (unexpected negative LEOKA count)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true)
WHERE count < 0;

-- T8: worker coverage
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2025 AND MAX(CAST(year AS INT)) >= 2026 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2026,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | historical: MIN<=2025, daily: MAX>=2026'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true);

-- ============================================================================
-- cde_trends (national rolling crime trend %; partitioned by type only)
-- No year dimension — T8 checks row count as proxy.
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'T2_row_count',
  CASE WHEN COUNT(*) >= 5 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 5, 'at least 5 offense trend rows'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (offense_name NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'offense_name IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true)
WHERE offense_name IS NULL;

-- T7: trend_pct non-null
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'T7_expected_values',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, 'trend_pct IS NULL (trend data missing)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true)
WHERE trend_pct IS NULL;

-- T8: worker coverage — no year dim; daily worker overwrites with fresh rolling data
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'T8_worker_coverage',
  CASE WHEN COUNT(*) >= 5 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 5,
  'no year dim; row count >= 5 confirms at least one worker wrote national trend data'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true);

-- ============================================================================
-- cde_supplemental (stolen/recovered property values; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'T2_row_count',
  CASE WHEN COUNT(*) >= 2000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 2000, '51 states × property types × 4+ years'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (state_abbr, year, property_type NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'state_abbr IS NULL OR year IS NULL OR property_type IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true)
WHERE state_abbr IS NULL OR year IS NULL OR property_type IS NULL;

-- T7: stolen_value non-negative where not null
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'T7_expected_values',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, 'stolen_value < 0'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true)
WHERE stolen_value IS NOT NULL AND stolen_value < 0;

-- T8: worker coverage
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2025 AND MAX(CAST(year AS INT)) >= 2026 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2026,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | historical: MIN<=2025, daily: MAX>=2026'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true);

-- ============================================================================
-- bjs_nibrs_estimates (6 SODA endpoints; partitioned by type/endpoint_id)
-- No year dimension — T8 checks endpoint count as proxy.
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'T2_row_count',
  CASE WHEN COUNT(*) >= 100 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 100, '6 SODA endpoints × multiple indicators'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: pk_nulls (endpoint_id NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'T6_pk_nulls',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 0, 'endpoint_id IS NULL'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true)
WHERE endpoint_id IS NULL;

-- T7: all 6 SODA endpoints present
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT endpoint_id) >= 6 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT endpoint_id), 6, 'distinct endpoint_id count (expect 6 SODA datasets)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true);

-- T8: worker coverage — no year dim; endpoint count confirms worker wrote all 6 sources
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'T8_worker_coverage',
  CASE WHEN COUNT(DISTINCT endpoint_id) >= 6 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT endpoint_id), 6,
  'no year dim; 6 distinct endpoint_ids confirms full BJS NIBRS ingest by at least one worker'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true);

-- ============================================================================
-- bjs_ncvs_personal (NCVS personal victimization microdata; type/year)
-- 2-year publication lag — historical: MIN<=2022, daily confirmed by MAX>=2022
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'T2_row_count',
  CASE WHEN COUNT(*) >= 10000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 10000, 'microdata rows from 2022+ survey years'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: skipped — all columns declared nullable in bjs_ncvs_personal

-- T7: multiple distinct years present
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT year) >= 2 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT year), 2, 'distinct survey years (expect >= 2022)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true);

-- T8: worker coverage — NCVS 2-year lag: historical covers 2022+; MAX>=2022 sufficient
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2022 AND MAX(CAST(year AS INT)) >= 2022 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2022,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | NCVS 2yr lag: historical: MIN<=2022, MAX>=2022'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true);

-- ============================================================================
-- bjs_ncvs_personal_pop (NCVS personal population/demographics; type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1000, 'survey respondent demographic rows from 2022+'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: skipped — all columns declared nullable

-- T7: multiple distinct years present
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT year) >= 2 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT year), 2, 'distinct survey years (expect >= 2022)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true);

-- T8: worker coverage — NCVS 2-year lag
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2022 AND MAX(CAST(year AS INT)) >= 2022 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2022,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | NCVS 2yr lag: historical: MIN<=2022, MAX>=2022'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true);

-- ============================================================================
-- bjs_ncvs_household (NCVS household property crime victimization; type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1000, 'household victimization incidents from 2022+'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: skipped — all columns declared nullable

-- T7: multiple distinct years present
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT year) >= 2 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT year), 2, 'distinct survey years (expect >= 2022)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true);

-- T8: worker coverage — NCVS 2-year lag
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2022 AND MAX(CAST(year AS INT)) >= 2022 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2022,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | NCVS 2yr lag: historical: MIN<=2022, MAX>=2022'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true);

-- ============================================================================
-- bjs_ncvs_household_pop (NCVS household demographics denominator; type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'T1_existence',
  CASE WHEN COUNT(*) > 0 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1, 'row count'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true);

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'T2_row_count',
  CASE WHEN COUNT(*) >= 1000 THEN 'pass' ELSE 'fail' END,
  COUNT(*), 1000, 'household demographic rows from 2022+'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true);

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'T4_all_null_cols',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true))
  WHERE null_percentage = 100.0
    AND column_name NOT IN ('type', 'year')
) t;

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'T5_all_same_value',
  CASE WHEN COUNT(*) = 0 THEN 'pass' ELSE 'warn' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true))
  WHERE approx_unique <= 1
    AND column_name NOT IN ('type', 'year')
) t;

-- T6: skipped — all columns declared nullable

-- T7: multiple distinct years present
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'T7_expected_values',
  CASE WHEN COUNT(DISTINCT year) >= 2 THEN 'pass' ELSE 'warn' END,
  COUNT(DISTINCT year), 2, 'distinct survey years (expect >= 2022)'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true);

-- T8: worker coverage — NCVS 2-year lag
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'T8_worker_coverage',
  CASE WHEN MIN(CAST(year AS INT)) <= 2022 AND MAX(CAST(year AS INT)) >= 2022 THEN 'pass' ELSE 'warn' END,
  MAX(CAST(year AS INT)), 2022,
  'MIN=' || MIN(year) || ' MAX=' || MAX(year) || ' | NCVS 2yr lag: historical: MIN<=2022, MAX>=2022'
FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true);

-- ============================================================================
-- T8 worker coverage summary
-- ============================================================================
SELECT '=== WORKER COVERAGE (T8) ===' AS section;
SELECT tbl, status, detail
FROM dq_results
WHERE test = 'T8_worker_coverage'
ORDER BY tbl;

-- ============================================================================
-- Final verdict
-- ============================================================================
SELECT
  schema,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END AS verdict,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS fails,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) AS warns,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS passes
FROM dq_results
GROUP BY schema;
