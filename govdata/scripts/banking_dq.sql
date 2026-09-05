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

-- T7: CRE loan / total risk-based capital fields are genuinely populated for at least some
-- banks in the sample (only banks with CRE exposure report LNRENRES/LNRECONS/LNREMULT; RBC
-- is a near-universal post-Basel-III field). Warns, not fails, since a narrow DQ sample
-- window could genuinely miss every CRE-exposed bank.
INSERT INTO dq_results
SELECT 'banking', 'financials', 'cre_rbc_fields_populated',
  CASE WHEN n_cre = 0 OR n_rbc = 0 THEN 'warn' ELSE 'pass' END,
  n_cre, 1, 'rows with a non-null CRE loan field (n_cre) and total_risk_based_capital (n_rbc=' || n_rbc || ')'
FROM (
  SELECT
    COUNT(*) FILTER (WHERE cre_nonfarm_nonresidential_thousands IS NOT NULL
                         OR cre_construction_land_dev_thousands IS NOT NULL
                         OR cre_multifamily_thousands IS NOT NULL) AS n_cre,
    COUNT(*) FILTER (WHERE total_risk_based_capital_thousands IS NOT NULL) AS n_rbc
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/financials', allow_moved_paths := true)
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
-- ncua_branch_locations (NCUA credit union branches; quarterly, 2017-03 to 2026-03)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'banking', 'ncua_branch_locations', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/ncua_branch_locations', allow_moved_paths := true));

-- T2: row_count. ~5,000-6,000 branches per quarter x 37 quarters queried; skipOn:[404]
-- may drop a handful of early quarters, so the floor is generously below the naive product.
INSERT INTO dq_results
SELECT 'banking', 'ncua_branch_locations', 'row_count',
  CASE WHEN n < 100000 THEN 'fail' ELSE 'pass' END,
  n, 100000, 'Expected >=100,000 rows across ~37 quarterly cycles (~5-6K branches/quarter)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/ncua_branch_locations', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/ncua_branch_locations', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'banking', 'ncua_branch_locations', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/ncua_branch_locations', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'cycle')
);

-- T6: pk_nulls (cycle, cu_number, site_id)
INSERT INTO dq_results
SELECT 'banking', 'ncua_branch_locations', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL cycle, cu_number, or site_id rows'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/ncua_branch_locations', allow_moved_paths := true)
  WHERE cycle IS NULL OR cu_number IS NULL OR site_id IS NULL
);

-- T6b: pk_dupes (cycle, cu_number, site_id) — site_id alone is scoped per credit union,
-- not globally unique, so cu_number is part of the real key.
INSERT INTO dq_results
SELECT 'banking', 'ncua_branch_locations', 'pk_dupes',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'Duplicate (cycle, cu_number, site_id) rows'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT cycle, cu_number, site_id, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/ncua_branch_locations', allow_moved_paths := true)
    GROUP BY cycle, cu_number, site_id HAVING COUNT(*) > 1
  )
);

-- T7: cycle_count. Some early quarters may 404; expect most of the 37 requested cycles present.
INSERT INTO dq_results
SELECT 'banking', 'ncua_branch_locations', 'cycle_count',
  CASE WHEN n < 25 THEN 'fail' ELSE 'pass' END,
  n, 25, 'Distinct cycle values present (37 quarters requested, 2017-03 to 2026-03)'
FROM (SELECT COUNT(DISTINCT cycle) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/ncua_branch_locations', allow_moved_paths := true));

-- ============================================================================
-- cra_small_business_lending (FFIEC CRA aggregate A1-1 - small biz originations
-- by county/tract/income group; partitions: type, year; ~29 years 1996-2024)
-- ============================================================================

-- T1: existence
INSERT INTO dq_results
SELECT 'banking', 'cra_small_business_lending', 'existence',
  CASE WHEN n = 0 THEN 'fail' ELSE 'pass' END,
  n, 1, 'row count'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/cra_small_business_lending', allow_moved_paths := true));

-- T2: row_count. A recent year holds ~100K A1-1 records (activity year 2024:
-- 100,498 rows across ~3,100 counties x ~14 MFI buckets x tract level plus
-- county/MSA totals); earlier years are sparser. 25K is a conservative
-- per-partition floor that older years (1996-2004, smaller CRA reporter
-- universe) can also clear.
INSERT INTO dq_results
SELECT 'banking', 'cra_small_business_lending', 'row_count',
  CASE WHEN n < 25000 THEN 'fail' ELSE 'pass' END,
  n, 25000, 'Expected >=25,000 rows per year (year-partitioned; production spans 1996-2024)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/cra_small_business_lending', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/cra_small_business_lending', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'banking', 'cra_small_business_lending', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/cra_small_business_lending', allow_moved_paths := true))
  WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')
);

-- T5: all_same_value — activity_year is genuinely constant within any single-year
-- partition, so exclude it (real constant, not a defect). Same for the two
-- source-constant discriminators loan_type=4 (Small Business) and
-- action_taken_type=1 (Originations), which A1-1's spec fixes for every row.
-- split_county is also excluded: per the FFIEC spec it is 'Y' only for a county
-- newly split by an OMB MSA/MD boundary change that year — most years have zero
-- such counties, so a single-year DQ window legitimately seeing only 'N' is
-- expected, not evidence of a broken feed (confirmed: activity year 2024 has
-- 96,798 'N' rows and 0 'Y' rows).
INSERT INTO dq_results
SELECT 'banking', 'cra_small_business_lending', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  COUNT(*), 0, STRING_AGG(column_name, ', ')
FROM (
  SELECT column_name
  FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/cra_small_business_lending', allow_moved_paths := true))
  WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'activity_year', 'split_county')
);

-- T6: pk_nulls. No single-column PK, but every row below the MSA/MD-total grain
-- must at minimum name a state FIPS. report_level='210' rows are MSA/MD totals,
-- which by the FFIEC spec span multiple states/counties and so correctly carry
-- blank state/county (confirmed: all 417 NULL-state_fips rows in activity year
-- 2024 are report_level=210) — excluded here rather than treated as a defect.
INSERT INTO dq_results
SELECT 'banking', 'cra_small_business_lending', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'NULL state_fips outside MSA/MD-total (report_level=210) rows'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/cra_small_business_lending', allow_moved_paths := true)
  WHERE (state_fips IS NULL OR state_fips = '') AND report_level <> '210'
);

-- T7: report_level takes the four documented shapes (NULL for tract-x-income-group
-- rows, '100' income-group totals, '200' county totals, '210' MSA/MD totals).
INSERT INTO dq_results
SELECT 'banking', 'cra_small_business_lending', 'expected_values',
  CASE WHEN n < 3 THEN 'warn' ELSE 'pass' END,
  n, 3, 'distinct non-null report_level values (expect 100 / 200 / 210)'
FROM (
  SELECT COUNT(DISTINCT report_level) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/cra_small_business_lending', allow_moved_paths := true)
  WHERE report_level IS NOT NULL AND report_level <> ''
);

-- T7b: county-total rows carry a resolvable county_fips joinable to geo.counties.
-- report_level '200' is the county-total grain; on those rows both state_fips and
-- county_code must be populated and the derived county_fips must be 5 chars.
INSERT INTO dq_results
SELECT 'banking', 'cra_small_business_lending', 'county_fips_shape',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  n, 0, 'county-total rows (report_level=200) whose county_fips is not 5 chars'
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/banking/cra_small_business_lending', allow_moved_paths := true)
  WHERE report_level = '200' AND (county_fips IS NULL OR LENGTH(county_fips) <> 5)
);

-- ============================================================================
-- Final results
-- ============================================================================
SELECT schema, tbl AS table_name, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
