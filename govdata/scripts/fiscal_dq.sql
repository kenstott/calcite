-- dq-lookback: 1
-- U.S. Federal Fiscal Data Quality Checks
-- Schema: fiscal
-- Tables: soi_income_by_zip, soi_income_by_county, county_migration_flows,
--         exempt_org_master, exempt_org_990, usaspending_by_agency,
--         usaspending_by_state, sba_loan_approvals, ssa_benefits_by_geography,
--         ssa_benefits_by_geography_acs, govt_finance_by_unit,
--         state_minimum_wage_history
-- All tables are Iceberg; reads via iceberg_scan (single-nested path).
-- T4/T5 exclude partition columns ('type' for all; also 'year' or 'program' where present).
-- Large tables carry dqRowLimit and sample in DQ mode; T2 thresholds reflect the sample.

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
-- TABLE: soi_income_by_zip (IRS SOI; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 ZIP-bracket rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_zip', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL zip_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_zip', allow_moved_paths := true) WHERE zip_code IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: soi_income_by_county (IRS SOI; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 county-bracket rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'soi_income_by_county', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/soi_income_by_county', allow_moved_paths := true) WHERE county_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: county_migration_flows (IRS SOI; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END, n, 500, 'Expected >=500 migration flow rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'county_migration_flows', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL origin_county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/county_migration_flows', allow_moved_paths := true) WHERE origin_county_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: exempt_org_master (IRS EO BMF snapshot; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 exempt-org rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_master', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL ein rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_master', allow_moved_paths := true) WHERE ein IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: exempt_org_990 (IRS Form 990 XML; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 990 filing rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'exempt_org_990', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL object_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/exempt_org_990', allow_moved_paths := true) WHERE object_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: usaspending_by_agency (USAspending; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T2_row_count',
  CASE WHEN n >= 20 THEN 'pass' ELSE 'fail' END, n, 20, 'Expected >=20 agency-year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_agency', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL agency_name rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_agency', allow_moved_paths := true) WHERE agency_name IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: usaspending_by_state (USAspending; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T2_row_count',
  CASE WHEN n >= 40 THEN 'pass' ELSE 'fail' END, n, 40, 'Expected >=40 state-year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_state', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_abbr rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_state', allow_moved_paths := true) WHERE state_abbr IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: sba_loan_approvals (SBA FOIA; partition cols: type, program)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END, n, 500, 'Expected >=500 loan rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'program')));

INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'program')));

INSERT INTO dq_results
SELECT 'fiscal', 'sba_loan_approvals', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END, n, 0, 'NULL approval fiscal year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/sba_loan_approvals', allow_moved_paths := true) WHERE year IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: ssa_benefits_by_geography (SSA OASDI/SSI; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 county-benefit rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography', allow_moved_paths := true) WHERE county_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: ssa_benefits_by_geography_acs (Census ACS derived; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T2_row_count',
  CASE WHEN n >= 3000 THEN 'pass' ELSE 'fail' END, n, 3000, 'Expected >=3000 county rows per vintage'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'ssa_benefits_by_geography_acs', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/ssa_benefits_by_geography_acs', allow_moved_paths := true) WHERE county_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: govt_finance_by_unit (Census govs-finance Individual Unit File; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END, n, 5000, 'Expected >=5000 unit-item rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_fips/gov_type_code/unit_id/item_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true)
  WHERE state_fips IS NULL OR gov_type_code IS NULL OR unit_id IS NULL OR item_code IS NULL);

-- T7 regression guard: the SIII view depends on E12/F12/E36/F36/E61/F61 actually being
-- present (confirmed real codes against the FY2023 technical documentation) — a parser
-- regression that silently dropped these would break the view without failing T1/T2.
INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T7_siii_item_codes_present',
  CASE WHEN n = 6 THEN 'pass' ELSE 'fail' END, n, 6,
  'Distinct SIII item codes found among E12/F12/E36/F36/E61/F61 (expect all 6)'
FROM (SELECT COUNT(DISTINCT item_code) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true)
  WHERE item_code IN ('E12', 'F12', 'E36', 'F36', 'E61', 'F61'));

INSERT INTO dq_results
SELECT 'fiscal', 'govt_finance_by_unit', 'T7_gov_type_coverage',
  CASE WHEN n = 6 THEN 'pass' ELSE 'warn' END, n, 6,
  'Distinct gov_type_code values found (expect all 6: state/county/city/township/special district/school district)'
FROM (SELECT COUNT(DISTINCT gov_type_code) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/govt_finance_by_unit', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: state_minimum_wage_history (DOL WHD state minimum wage history; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T2_row_count',
  CASE WHEN n >= 1500 THEN 'pass' ELSE 'fail' END, n, 1500, 'Expected >=1500 jurisdiction-year rows (55 jurisdictions x >=27 published years)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL year/jurisdiction_name rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true)
  WHERE year IS NULL OR jurisdiction_name IS NULL);

-- T7 regression guard: confirm the parser still sees all 4 jurisdiction types and still
-- classifies a majority of cells as the unambiguous SINGLE value_type — a classifier
-- regression that dumped everything into OTHER would pass T1/T2/T6 silently.
INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T7_jurisdiction_type_coverage',
  CASE WHEN n = 4 THEN 'pass' ELSE 'warn' END, n, 4,
  'Distinct jurisdiction_type values found (expect all 4: FEDERAL/STATE/DISTRICT/TERRITORY)'
FROM (SELECT COUNT(DISTINCT jurisdiction_type) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'state_minimum_wage_history', 'T7_single_value_type_present',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000,
  'Rows classified value_type=SINGLE (majority of cells are unambiguous single wage figures)'
FROM (SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_minimum_wage_history', allow_moved_paths := true)
  WHERE value_type = 'SINGLE');

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
