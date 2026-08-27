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
-- TABLE: nsf_rd_by_field
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'research', 'nsf_rd_by_field', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_rd_by_field', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'research', 'nsf_rd_by_field', 'T2_row_count',
  CASE WHEN n >= 300 THEN 'pass' ELSE 'fail' END, n, 300, 'Expected >=300 field x year rows (58 fields x up to 10 years)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_rd_by_field', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_rd_by_field', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'research', 'nsf_rd_by_field', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_rd_by_field', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'research', 'nsf_rd_by_field', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_rd_by_field', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'research', 'nsf_rd_by_field', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL (year, rd_field) rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_rd_by_field', allow_moved_paths := true) WHERE year IS NULL OR rd_field IS NULL);

INSERT INTO dq_results
SELECT 'research', 'nsf_rd_by_field', 'T7_all_fields_total_present',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Presence of the All fields (field_level=0) total row'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_rd_by_field', allow_moved_paths := true) WHERE rd_field = 'All fields' AND field_level = 0);

-- T8: level-1 fields sum to the level-0 total for each year that has both a
-- level-0 value and at least one level-1 value (years where the taxonomy was
-- mid-revision can have a level-0 total with no populated level-1 rows, or
-- vice versa — those years are excluded from this check, not counted as fails).
INSERT INTO dq_results
SELECT 'research', 'nsf_rd_by_field', 'T8_level1_sums_to_total',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END, n, 0,
  'Years where SUM(level-1 obligations) does not match the level-0 total within $50M rounding tolerance'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT l1.year,
      SUM(l1.obligations_usd_million) AS level1_sum,
      MAX(l0.obligations_usd_million) AS level0_total
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_rd_by_field', allow_moved_paths := true) l1
    JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_rd_by_field', allow_moved_paths := true) l0
      ON l1.year = l0.year AND l0.field_level = 0 AND l0.rd_field = 'All fields'
    WHERE l1.field_level = 1 AND l1.obligations_usd_million IS NOT NULL
    GROUP BY l1.year
    HAVING ABS(SUM(l1.obligations_usd_million) - MAX(l0.obligations_usd_million)) > 50
  )
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nih_award_projects (NIH RePORTER; FY2022-2024 recent-years window, not full history)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'research', 'nih_award_projects', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nih_award_projects', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'research', 'nih_award_projects', 'T2_row_count',
  CASE WHEN n >= 50000 THEN 'pass' ELSE 'fail' END, n, 50000,
  'Expected >=50000 awards for a single fiscal year (national FY total runs ~85,000)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nih_award_projects', allow_moved_paths := true) WHERE fiscal_year = 2023);

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nih_award_projects', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'research', 'nih_award_projects', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nih_award_projects', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'research', 'nih_award_projects', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL appl_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nih_award_projects', allow_moved_paths := true) WHERE appl_id IS NULL);

INSERT INTO dq_results
SELECT 'research', 'nih_award_projects', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate appl_id rows (offset-cap slicing by agency_ic should make each appl_id unique)'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT appl_id, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nih_award_projects', allow_moved_paths := true)
  GROUP BY appl_id HAVING COUNT(*) > 1
));

INSERT INTO dq_results
SELECT 'research', 'nih_award_projects', 'T7_ic_count',
  CASE WHEN n >= 20 THEN 'pass' ELSE 'fail' END, n, 20,
  'Distinct agency_ic values present (24 ICs queried; expect most to have at least one award per year)'
FROM (SELECT COUNT(DISTINCT agency_ic) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nih_award_projects', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'research', 'nih_award_projects', 'T7_amount_plausible',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END, bad, 0,
  'award_amount outside plausible [0, 50000000] range for a single project-year'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nih_award_projects', allow_moved_paths := true)
      WHERE award_amount IS NOT NULL AND (award_amount < 0 OR award_amount > 50000000));

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
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL year or inst_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/research/nsf_herd_by_institution', allow_moved_paths := true) WHERE year IS NULL OR inst_id IS NULL);

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
