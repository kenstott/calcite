-- dq-lookback: 1
-- U.S. Research & Development (NSF NCSES) Data Quality Checks
-- Schema: research
-- Tables: nsf_national_rd, nsf_federal_rd_obligations, nsf_herd_by_institution
-- All tables are Iceberg; reads via iceberg_scan (single-nested path).
-- T4/T5 exclude partition columns 'type' and 'year'.
-- NOTE: thresholds are provisional — no prod data has been ingested yet; revise
-- T2/T7 against actual row counts after the first research ETL run.

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';

CREATE TEMP TABLE dq_results (
  schema   VARCHAR,
  tbl      VARCHAR,
  test     VARCHAR,
  status   VARCHAR,
  value    DOUBLE,
  threshold DOUBLE,
  detail   VARCHAR
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nsf_national_rd
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'research', 'nsf_national_rd', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_national_rd', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'research', 'nsf_national_rd', 'T2_row_count',
  CASE WHEN n >= 300 THEN 'pass' ELSE 'fail' END, n, 300, 'Expected >=300 tall rows (>=50 yrs x total+performer+source)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_national_rd', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_national_rd', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'research', 'nsf_national_rd', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_national_rd', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'research', 'nsf_national_rd', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_national_rd', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'rd_type')));

INSERT INTO dq_results
SELECT 'research', 'nsf_national_rd', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_national_rd', allow_moved_paths := true) WHERE year IS NULL);

INSERT INTO dq_results
SELECT 'research', 'nsf_national_rd', 'T7_gdp_total_present',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END, n, 1, 'Rows with non-null pct_of_gdp (the all/all total rows)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_national_rd', allow_moved_paths := true) WHERE pct_of_gdp IS NOT NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nsf_federal_rd_obligations
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'research', 'nsf_federal_rd_obligations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_federal_rd_obligations', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'research', 'nsf_federal_rd_obligations', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 agency x performer rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_federal_rd_obligations', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_federal_rd_obligations', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'research', 'nsf_federal_rd_obligations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_federal_rd_obligations', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'research', 'nsf_federal_rd_obligations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_federal_rd_obligations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'rd_field', 'rd_type')));

INSERT INTO dq_results
SELECT 'research', 'nsf_federal_rd_obligations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_federal_rd_obligations', allow_moved_paths := true) WHERE year IS NULL);

INSERT INTO dq_results
SELECT 'research', 'nsf_federal_rd_obligations', 'T7_all_agencies_present',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END, n, 1, 'Presence of the All agencies total row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_federal_rd_obligations', allow_moved_paths := true) WHERE funding_agency = 'All agencies');

-- ─────────────────────────────────────────────────────────────
-- TABLE: nsf_herd_by_institution
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'research', 'nsf_herd_by_institution', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_herd_by_institution', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'research', 'nsf_herd_by_institution', 'T2_row_count',
  CASE WHEN n >= 10000 THEN 'pass' ELSE 'fail' END, n, 10000, 'Expected >=10k institution x field x source rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_herd_by_institution', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_herd_by_institution', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'research', 'nsf_herd_by_institution', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_herd_by_institution', allow_moved_paths := true))
    -- county_fips and control are structurally null (not published in HERD microdata)
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'county_fips', 'control')));

INSERT INTO dq_results
SELECT 'research', 'nsf_herd_by_institution', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_herd_by_institution', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'county_fips', 'control')));

INSERT INTO dq_results
SELECT 'research', 'nsf_herd_by_institution', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL year or institution rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_herd_by_institution', allow_moved_paths := true) WHERE year IS NULL OR institution IS NULL);

INSERT INTO dq_results
SELECT 'research', 'nsf_herd_by_institution', 'T7_institution_coverage',
  CASE WHEN n >= 300 THEN 'pass' ELSE 'warn' END, n, 300, 'Distinct institutions'
FROM (SELECT COUNT(DISTINCT institution) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_herd_by_institution', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
