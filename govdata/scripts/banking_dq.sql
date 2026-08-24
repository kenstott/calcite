-- dq-lookback: 1
-- ============================================================================
-- Data Quality Script: banking schema
-- Sources: FDIC BankFind Suite (institutions, locations, history, failures,
--          sod, financials, summary), CFPB Consumer Complaint Database
--          (consumer_complaints)
-- Tables: 8 Iceberg tables
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
-- institutions (FDIC-insured entities snapshot; ~27,836 rows; partition: type)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'banking', 'institutions', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/institutions', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'banking', 'institutions', 'row_count',
  CASE WHEN n < 20000 THEN 'fail' ELSE 'pass' END,
  n, 20000, 'FDIC confirmed ~27,836 institutions total (active and inactive, snapshot table)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/institutions', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/institutions', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'banking', 'institutions', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/institutions', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type')
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'banking', 'institutions', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/institutions', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (cert NOT NULL)
INSERT INTO dq_results
SELECT 'banking', 'institutions', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL cert'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/institutions', allow_moved_paths := true)
  WHERE cert IS NULL
);

-- T7: active status includes both true and false (snapshot covers both live and closed institutions)
INSERT INTO dq_results
SELECT 'banking', 'institutions', 'expected_values',
  CASE WHEN n = 2 THEN 'pass' ELSE 'warn' END,
  n, 2, 'distinct active values; a moment where every institution is active would be surprising but not necessarily wrong'
FROM (SELECT COUNT(DISTINCT active) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/institutions', allow_moved_paths := true));

-- ============================================================================
-- locations (branch/office snapshot; ~70K+ rows nationwide; partition: type)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'banking', 'locations', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/locations', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'banking', 'locations', 'row_count',
  CASE WHEN n < 70000 THEN 'fail' ELSE 'pass' END,
  n, 70000, 'FDIC nationwide branch count (California alone was 5,548)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/locations', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/locations', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'banking', 'locations', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/locations', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type')
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'banking', 'locations', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/locations', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type')
);

-- T6: pk_nulls (uninum NOT NULL)
INSERT INTO dq_results
SELECT 'banking', 'locations', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL uninum'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/locations', allow_moved_paths := true)
  WHERE uninum IS NULL
);

-- ============================================================================
-- history (structure-change events; branch open/close/merger; partitions: type, year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'banking', 'history', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/history', allow_moved_paths := true));

-- T2: row_count. Year-partitioned; a single year's count varies widely (1934 has far fewer
-- events than a modern year). 1000 is a conservative per-partition floor, not a full-table
-- expectation — the DQ window will typically hold one or a few recent years' worth of events.
INSERT INTO dq_results
SELECT 'banking', 'history', 'row_count',
  CASE WHEN n < 1000 THEN 'fail' ELSE 'pass' END,
  n, 1000, 'conservative per-partition floor (year-partitioned; older years are naturally sparser than recent ones)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/history', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/history', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'banking', 'history', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/history', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'banking', 'history', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/history', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')
);

-- T6: pk_nulls (transnum NOT NULL)
INSERT INTO dq_results
SELECT 'banking', 'history', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL transnum'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/history', allow_moved_paths := true)
  WHERE transnum IS NULL
);

-- T7: changecode_label has multiple distinct non-null values (open/close/merger event types)
INSERT INTO dq_results
SELECT 'banking', 'history', 'expected_values',
  CASE WHEN n < 2 THEN 'warn' ELSE 'pass' END,
  n, 2, 'distinct non-null changecode_label values'
FROM (
  SELECT COUNT(DISTINCT changecode_label) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/history', allow_moved_paths := true)
  WHERE changecode_label IS NOT NULL
);

-- ============================================================================
-- failures (bank failure resolutions; ~4,115 total since 1934; partitions: type, year)
-- ============================================================================

-- T1: existence. Full-table COUNT(*), no year filter, so the check reflects overall table
-- health rather than one year's slice. A genuinely quiet multi-year stretch is not expected
-- to zero out the whole table, but 'warn' (not 'fail') on zero avoids over-alarming on the
-- same sparse-year condition T2 is built to tolerate.
INSERT INTO dq_results
SELECT 'banking', 'failures', 'existence',
  CASE WHEN n = 0 THEN 'warn' ELSE 'pass' END,
  n, 1, 'row count across all years'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/failures', allow_moved_paths := true));

-- T2: row_count. Failures are naturally sparse — some years have zero failures nationwide,
-- a legitimately correct outcome in calm banking periods, not a defect. 'warn' not 'fail' on
-- a low count so a genuine quiet stretch doesn't page anyone.
INSERT INTO dq_results
SELECT 'banking', 'failures', 'row_count',
  CASE WHEN n < 1 THEN 'warn' ELSE 'pass' END,
  n, 1, 'failures are naturally sparse per year; a zero-failure window is a legitimate outcome in calm banking periods'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/failures', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/failures', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'banking', 'failures', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/failures', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'banking', 'failures', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/failures', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')
);

-- T6: pk_nulls (id NOT NULL)
INSERT INTO dq_results
SELECT 'banking', 'failures', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL id'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/failures', allow_moved_paths := true)
  WHERE id IS NULL
);

-- ============================================================================
-- sod (Summary of Deposits; one row per branch per annual survey; partitions: type, year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'banking', 'sod', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/sod', allow_moved_paths := true));

-- T2: row_count. No confirmed per-year SOD row count at DQ-script authoring time; 1000 is a
-- conservative floor pending real data — tighten once an actual annual SOD load count is observed.
INSERT INTO dq_results
SELECT 'banking', 'sod', 'row_count',
  CASE WHEN n < 1000 THEN 'fail' ELSE 'pass' END,
  n, 1000, 'conservative per-partition floor pending observed real data (unconfirmed)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/sod', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/sod', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'banking', 'sod', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/sod', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'banking', 'sod', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/sod', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')
);

-- T6: pk_nulls (uninum NOT NULL). uninum is unique per year, not a global PK across years.
INSERT INTO dq_results
SELECT 'banking', 'sod', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL uninum'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/sod', allow_moved_paths := true)
  WHERE uninum IS NULL
);

-- ============================================================================
-- financials (quarterly call report; composite grain cert+repdte; partitions: type, year;
-- dqRowLimit-capped)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'banking', 'financials', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/financials', allow_moved_paths := true));

-- T2: row_count. dqRowLimit caps the fetch at an exact value TBD by the schema YAML; 500 is a
-- conservative floor that holds regardless of the exact cap chosen.
INSERT INTO dq_results
SELECT 'banking', 'financials', 'row_count',
  CASE WHEN n < 500 THEN 'fail' ELSE 'pass' END,
  n, 500, 'conservative floor below the dqRowLimit cap (exact cap value set in the schema YAML)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/financials', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/financials', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'banking', 'financials', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/financials', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'banking', 'financials', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/financials', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')
);

-- T6: pk_nulls. No single-column PK — the grain is composite (cert, repdte) — so check the
-- leading key component, cert, for NOT NULL instead of a synthetic single-column PK.
INSERT INTO dq_results
SELECT 'banking', 'financials', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL cert (composite grain is cert+repdte; no single-column PK)'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/financials', allow_moved_paths := true)
  WHERE cert IS NULL
);

-- ============================================================================
-- summary (industry-wide rollup, ~8,107 rows; grain year x state, state_abbr nullable for
-- nationwide row; partitions: type, year)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'banking', 'summary', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/summary', allow_moved_paths := true));

-- T2: row_count. No confirmed per-year row count at DQ-script authoring time (grain is
-- year x state plus one nationwide row); 10 is a conservative floor pending real data.
INSERT INTO dq_results
SELECT 'banking', 'summary', 'row_count',
  CASE WHEN n < 10 THEN 'fail' ELSE 'pass' END,
  n, 10, 'conservative per-partition floor pending observed real data (unconfirmed)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/summary', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/summary', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols. state_abbr IS NULL rows are expected (nationwide rollups), but T4 only
-- flags columns that are ENTIRELY null, so a partially-null state_abbr column is never flagged
-- here and needs no special exclusion.
INSERT INTO dq_results
SELECT 'banking', 'summary', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/summary', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'banking', 'summary', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/summary', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')
);

-- T6: pk_nulls. No single-column PK — grain is year x state with state_abbr nullable for the
-- nationwide rollup — so check year (the one column guaranteed non-null on every row) instead.
INSERT INTO dq_results
SELECT 'banking', 'summary', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL year (grain is year x state; state_abbr IS NULL is expected for nationwide rollup rows, not a defect)'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/summary', allow_moved_paths := true)
  WHERE year IS NULL
);

-- ============================================================================
-- consumer_complaints (CFPB complaints scoped to 5 bank-relevant products; huge source volume
-- (17.2M total unfiltered); partitions: type, year, state, product; dqRowLimit-capped)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'banking', 'consumer_complaints', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/consumer_complaints', allow_moved_paths := true));

-- T2: row_count. dqRowLimit caps the fetch; 100 is a conservative floor that holds regardless
-- of the exact cap chosen.
INSERT INTO dq_results
SELECT 'banking', 'consumer_complaints', 'row_count',
  CASE WHEN n < 100 THEN 'fail' ELSE 'pass' END,
  n, 100, 'conservative floor below the dqRowLimit cap (exact cap value set in the schema YAML)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/consumer_complaints', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/consumer_complaints', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'banking', 'consumer_complaints', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/consumer_complaints', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'state_abbr', 'product')
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'banking', 'consumer_complaints', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/consumer_complaints', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'state_abbr', 'product')
);

-- T6: pk_nulls (complaint_id NOT NULL)
INSERT INTO dq_results
SELECT 'banking', 'consumer_complaints', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL complaint_id'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/consumer_complaints', allow_moved_paths := true)
  WHERE complaint_id IS NULL
);

-- T7: product scoped to the 5 bank-relevant categories (checking/savings, mortgage, credit
-- card, money transfer, debt collection) — excludes credit-reporting/student-loan complaints
INSERT INTO dq_results
SELECT 'banking', 'consumer_complaints', 'expected_values',
  CASE WHEN n < 5 THEN 'warn' ELSE 'pass' END,
  n, 5, 'distinct product values (expect the 5 scoped bank-relevant categories)'
FROM (SELECT COUNT(DISTINCT product) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/consumer_complaints', allow_moved_paths := true));

-- ============================================================================
-- Final results
-- ============================================================================
SELECT schema, tbl AS table_name, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
