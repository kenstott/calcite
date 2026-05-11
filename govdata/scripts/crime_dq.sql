-- ============================================================================
-- Data Quality Script: crime schema
-- Sources: FBI Crime Data Explorer (CDE) + Bureau of Justice Statistics (BJS)
-- Tables: 15 partitioned tables (11 CDE + 4 BJS)
-- ============================================================================

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_region='auto';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';

CREATE TEMP TABLE dq_results(
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,
  value     DOUBLE,
  threshold DOUBLE,
  detail    VARCHAR
);

-- ============================================================================
-- cde_agencies (~19,600 law enforcement agencies, partitioned by type/state_abbr)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'row_count',
  CASE WHEN n < 19000 THEN 'fail' ELSE 'pass' END,
  n, 19000, 'expect ~19,600 agencies across 51 jurisdictions'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (ori NOT NULL, state_abbr NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL ori or state_abbr'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true)
  WHERE ori IS NULL OR state_abbr IS NULL
);

-- T7: 51 distinct state_abbr values
INSERT INTO dq_results
SELECT 'crime', 'cde_agencies', 'expected_values',
  CASE WHEN n < 51 THEN 'fail' ELSE 'pass' END,
  n, 51, 'distinct state_abbr (50 states + DC)'
FROM (SELECT COUNT(DISTINCT state_abbr) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_agencies', allow_moved_paths := true));

-- ============================================================================
-- cde_offenses (crime rates by state, offense, month; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'row_count',
  CASE WHEN n < 50000 THEN 'fail' ELSE 'pass' END,
  n, 50000, '51 states × 10 offenses × 12 months × multiple years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (state_abbr, offense_code NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL state_abbr or offense_code'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true)
  WHERE state_abbr IS NULL OR offense_code IS NULL
);

-- T7: offense_code values in expected set
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'expected_values',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'offense_code values outside known UCR set'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true)
  WHERE offense_code NOT IN (
    'violent-crime','property-crime','homicide','aggravated-assault','rape',
    'robbery','burglary','larceny','motor-vehicle-theft','arson'
  )
);

-- T7: offense_rate non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_offenses', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'offense_rate < 0 (unexpected negative rates)'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_offenses', allow_moved_paths := true)
  WHERE offense_rate < 0
);

-- ============================================================================
-- cde_police_employment (officers per state/year; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'row_count',
  CASE WHEN n < 500 THEN 'fail' ELSE 'pass' END,
  n, 500, '51 states × ~10 years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL state_abbr or year'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true)
  WHERE state_abbr IS NULL OR year IS NULL
);

-- T7: officers_per_1000 non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_police_employment', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'officers_per_1000 < 0'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_police_employment', allow_moved_paths := true)
  WHERE officers_per_1000 < 0
);

-- ============================================================================
-- cde_hate_crimes (bias/incident stats by state/year; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'row_count',
  CASE WHEN n < 10000 THEN 'fail' ELSE 'pass' END,
  n, 10000, 'flattened bias/incident categories across 51 states × multiple years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL state_abbr or year'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true)
  WHERE state_abbr IS NULL OR year IS NULL
);

-- T7: count non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_hate_crimes', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'negative hate crime count'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_hate_crimes', allow_moved_paths := true)
  WHERE count < 0
);

-- ============================================================================
-- cde_use_of_force (use-of-force metrics by state/year; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'row_count',
  CASE WHEN n < 5000 THEN 'fail' ELSE 'pass' END,
  n, 5000, 'participation/incident breakdowns across 51 states × multiple years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL state_abbr or year'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true)
  WHERE state_abbr IS NULL OR year IS NULL
);

-- T7: (no strict domain constraints for use-of-force metrics section labels)
INSERT INTO dq_results
SELECT 'crime', 'cde_use_of_force', 'expected_values',
  CASE WHEN n < 51 THEN 'fail' ELSE 'pass' END,
  n, 51, 'distinct state_abbr coverage'
FROM (SELECT COUNT(DISTINCT state_abbr) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_use_of_force', allow_moved_paths := true));

-- ============================================================================
-- cde_crime_agency (agency-level offenses by ORI; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'row_count',
  CASE WHEN n < 100000 THEN 'fail' ELSE 'pass' END,
  n, 100000, '~19,600 agencies × 10 offenses × 12 months'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (ori, state_abbr, offense_code NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL ori, state_abbr, or offense_code'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true)
  WHERE ori IS NULL OR state_abbr IS NULL OR offense_code IS NULL
);

-- T7: distinct ORI count vs agency registry
INSERT INTO dq_results
SELECT 'crime', 'cde_crime_agency', 'expected_values',
  CASE WHEN n < 10000 THEN 'warn' ELSE 'pass' END,
  n, 10000, 'distinct ORI count (expect close to 19,600)'
FROM (SELECT COUNT(DISTINCT ori) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_crime_agency', allow_moved_paths := true));

-- ============================================================================
-- cde_arrests (arrests by state, offense, demographics; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'row_count',
  CASE WHEN n < 10000 THEN 'fail' ELSE 'pass' END,
  n, 10000, '51 states × age/sex/race breakdowns × multiple years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (state_abbr, year, offense_code NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL state_abbr, year, or offense_code'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true)
  WHERE state_abbr IS NULL OR year IS NULL OR offense_code IS NULL
);

-- T7: count non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_arrests', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'negative arrest count'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_arrests', allow_moved_paths := true)
  WHERE count < 0
);

-- ============================================================================
-- cde_shr (Supplementary Homicide Reports; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'row_count',
  CASE WHEN n < 5000 THEN 'fail' ELSE 'pass' END,
  n, 5000, 'victim/offender demographics across 51 states × multiple years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL state_abbr or year'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true)
  WHERE state_abbr IS NULL OR year IS NULL
);

-- T7: count non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_shr', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'negative SHR count'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_shr', allow_moved_paths := true)
  WHERE count < 0
);

-- ============================================================================
-- cde_leoka (Officers Killed/Assaulted; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'row_count',
  CASE WHEN n < 5000 THEN 'fail' ELSE 'pass' END,
  n, 5000, 'killed/assaulted breakdowns across 51 states × multiple years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (state_abbr, year NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL state_abbr or year'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true)
  WHERE state_abbr IS NULL OR year IS NULL
);

-- T7: count non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_leoka', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'negative LEOKA count'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_leoka', allow_moved_paths := true)
  WHERE count < 0
);

-- ============================================================================
-- cde_trends (national rolling crime trend %; partitioned by type only)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'row_count',
  CASE WHEN n < 5 THEN 'fail' ELSE 'pass' END,
  n, 5, 'at least 5 offense trend rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (offense_name NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL offense_name'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true)
  WHERE offense_name IS NULL
);

-- T7: trend_pct is a percentage change (any real number is valid; check it is non-null)
INSERT INTO dq_results
SELECT 'crime', 'cde_trends', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'rows with NULL trend_pct (trend data missing)'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_trends', allow_moved_paths := true)
  WHERE trend_pct IS NULL
);

-- ============================================================================
-- cde_supplemental (stolen/recovered property values; partitioned by type/year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'row_count',
  CASE WHEN n < 5000 THEN 'fail' ELSE 'pass' END,
  n, 5000, '51 states × ~10 property types × multiple years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (state_abbr, year, property_type NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL state_abbr, year, or property_type'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true)
  WHERE state_abbr IS NULL OR year IS NULL OR property_type IS NULL
);

-- T7: stolen_value non-negative
INSERT INTO dq_results
SELECT 'crime', 'cde_supplemental', 'expected_values',
  CASE WHEN n > 0 THEN 'warn' ELSE 'pass' END,
  n, 0, 'negative stolen_value'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/cde_supplemental', allow_moved_paths := true)
  WHERE stolen_value < 0
);

-- ============================================================================
-- bjs_nibrs_estimates (6 SODA endpoints; partitioned by type/endpoint_id)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'row_count',
  CASE WHEN n < 100 THEN 'fail' ELSE 'pass' END,
  n, 100, '6 SODA endpoints × multiple indicators'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (endpoint_id NOT NULL)
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL endpoint_id'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true)
  WHERE endpoint_id IS NULL
);

-- T7: all 6 SODA endpoints present
INSERT INTO dq_results
SELECT 'crime', 'bjs_nibrs_estimates', 'expected_values',
  CASE WHEN n < 6 THEN 'fail' ELSE 'pass' END,
  n, 6, 'distinct endpoint_id count (expect 6 SODA datasets)'
FROM (SELECT COUNT(DISTINCT endpoint_id) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_nibrs_estimates', allow_moved_paths := true));

-- ============================================================================
-- bjs_ncvs_personal (NCVS personal victimization microdata; partitioned type/year)
-- All columns nullable — T6 skipped.
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'row_count',
  CASE WHEN n < 50000 THEN 'fail' ELSE 'pass' END,
  n, 50000, 'microdata — many rows per year from 2015 onwards'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: skipped — all columns declared nullable in bjs_ncvs_personal

-- T7: multiple distinct years present
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal', 'expected_values',
  CASE WHEN n < 2 THEN 'warn' ELSE 'pass' END,
  n, 2, 'distinct year values (expect multiple survey years)'
FROM (SELECT COUNT(DISTINCT year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal', allow_moved_paths := true));

-- ============================================================================
-- bjs_ncvs_personal_pop (NCVS personal population/demographics; type/year)
-- All columns nullable — T6 skipped.
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'row_count',
  CASE WHEN n < 50000 THEN 'fail' ELSE 'pass' END,
  n, 50000, 'all survey respondent records from 2015 onwards'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: skipped — all columns declared nullable in bjs_ncvs_personal_pop

-- T7: multiple distinct years present
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_personal_pop', 'expected_values',
  CASE WHEN n < 2 THEN 'warn' ELSE 'pass' END,
  n, 2, 'distinct year values (expect multiple survey years)'
FROM (SELECT COUNT(DISTINCT year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_personal_pop', allow_moved_paths := true));

-- ============================================================================
-- bjs_ncvs_household (NCVS household property crime victimization; type/year)
-- All columns nullable — T6 skipped.
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'row_count',
  CASE WHEN n < 10000 THEN 'fail' ELSE 'pass' END,
  n, 10000, 'household victimization incidents from 2015 onwards'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: skipped — all columns declared nullable in bjs_ncvs_household

-- T7: multiple distinct years present
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household', 'expected_values',
  CASE WHEN n < 2 THEN 'warn' ELSE 'pass' END,
  n, 2, 'distinct year values (expect multiple survey years)'
FROM (SELECT COUNT(DISTINCT year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household', allow_moved_paths := true));

-- ============================================================================
-- bjs_ncvs_household_pop (NCVS household demographics denominator; type/year)
-- All columns nullable — T6 skipped.
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'row_count',
  CASE WHEN n < 10000 THEN 'fail' ELSE 'pass' END,
  n, 10000, 'all participating household records from 2015 onwards'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true))
  WHERE null_percentage = 100.0
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: skipped — all columns declared nullable in bjs_ncvs_household_pop

-- T7: multiple distinct years present
INSERT INTO dq_results
SELECT 'crime', 'bjs_ncvs_household_pop', 'expected_values',
  CASE WHEN n < 2 THEN 'warn' ELSE 'pass' END,
  n, 2, 'distinct year values (expect multiple survey years)'
FROM (SELECT COUNT(DISTINCT year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/crime/bjs_ncvs_household_pop', allow_moved_paths := true));

-- ============================================================================
-- Final results
-- ============================================================================
SELECT schema, tbl AS table_name, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
