-- dq-lookback: 1
-- U.S. Federal Fiscal Data Quality Checks
-- Schema: fiscal
-- Tables: soi_income_by_zip, soi_income_by_county, county_migration_flows,
--         exempt_org_master, exempt_org_990, usaspending_by_agency,
--         usaspending_by_state, usaspending_by_district,
--         usaspending_recipients_by_district,
--         entitlement_spending_by_state, sba_loan_approvals,
--         ssa_benefits_by_geography, ssa_benefits_by_geography_acs,
--         govt_finance_by_unit, state_minimum_wage_history,
--         state_corporate_income_tax_collections
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
-- TABLE: usaspending_by_district (USAspending; partition cols: type, year; new 5 Sep 2026)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true));

-- ~441-442 districts per year (435 numbered + at-large/DC/territories)
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T2_row_count',
  CASE WHEN n >= 400 THEN 'pass' ELSE 'fail' END, n, 400, 'Expected >=400 district-year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL cd_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true) WHERE cd_fips IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, cd_fips) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, cd_fips, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true)
  GROUP BY year, cd_fips HAVING COUNT(*) > 1));

-- cd_fips must be a well-formed 4-digit code (state FIPS + district number)
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_by_district', 'T7_fips_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with malformed (non-4-digit) cd_fips'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_by_district', allow_moved_paths := true)
      WHERE cd_fips NOT SIMILAR TO '[0-9]{4}');

-- ─────────────────────────────────────────────────────────────
-- TABLE: usaspending_recipients_by_district (USAspending; partition cols: type, year; new 6 Sep 2026)
-- Top 100 recipients by district x fiscal year -- one call per (year, district)
-- against spending_by_category/recipient/, scoped by default to 2 recent fiscal years.
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true));

-- Up to 100 recipients x ~441-442 districts per year; a district with fewer than
-- 100 distinct recipients yields fewer rows, so this is a loose floor, not exact.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T2_row_count',
  CASE WHEN n >= 400 THEN 'pass' ELSE 'fail' END, n, 400, 'Expected >=400 district-year-rank rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true) LIMIT 3;

-- recipient_id/uei/duns are expected to be all-null only for the narrow
-- MULTIPLE RECIPIENTS aggregate rows, never for the whole table -- excluded from
-- the blanket all-null-cols check below via the WHERE clause on cnt, not a column
-- exclusion, since a real all-null column here would still be a defect.
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL cd_fips or rank rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
      WHERE cd_fips IS NULL OR rank IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, cd_fips, rank) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, cd_fips, rank, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
  GROUP BY year, cd_fips, rank HAVING COUNT(*) > 1));

-- cd_fips must be a well-formed 4-digit code (state FIPS + district number)
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T7_fips_format',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with malformed (non-4-digit) cd_fips'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
      WHERE cd_fips NOT SIMILAR TO '[0-9]{4}');

-- rank must fall within the documented 1-100 top-N cut
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T8_rank_domain',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with rank outside 1-100'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
      WHERE rank < 1 OR rank > 100);

-- Sanity check: a known large federal contractor/grantee should appear as rank 1
-- somewhere in the table for a recent year (matches the live-tested PA-04 result:
-- AmerisourceBergen Drug Corp topped that district's FY2025 recipient ranking).
INSERT INTO dq_results
SELECT 'fiscal', 'usaspending_recipients_by_district', 'T9_expected_values',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END, n, 1, 'Rows with rank=1 and obligated_amount > 0'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/usaspending_recipients_by_district', allow_moved_paths := true)
      WHERE rank = 1 AND obligated_amount > 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: entitlement_spending_by_state (USAspending, CFDA-filtered; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true));

-- ~56 states/territories x 3 programs per fiscal year
INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 state-program-year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_abbr/program rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
      WHERE state_abbr IS NULL OR program IS NULL);

INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, state_abbr, program) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, state_abbr, program, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
  GROUP BY year, state_abbr, program HAVING COUNT(*) > 1));

-- program must be one of the three CFDA-coded entitlement programs this table covers
INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T7_program_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with unexpected program value'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
      WHERE program NOT IN ('social_security_retirement', 'medicare_hospital_insurance',
                             'medicare_supplementary_medical_insurance'));

-- Sanity check: a large state's Social Security total should be tens of billions of
-- dollars for its most recent fiscal year, not near-zero (confirmed live: CA FY2025
-- Social Security Retirement Insurance = $127.5B).
INSERT INTO dq_results
SELECT 'fiscal', 'entitlement_spending_by_state', 'T8_expected_values',
  CASE WHEN amt >= 1e10 THEN 'pass' ELSE 'fail' END, COALESCE(amt, 0), 1e10,
  'CA social_security_retirement obligated_amount for most recent year (expect >= $10B)'
FROM (
  SELECT MAX(CASE WHEN year = (
      SELECT MAX(year) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
      WHERE state_abbr = 'CA' AND program = 'social_security_retirement')
    THEN obligated_amount END) AS amt
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/entitlement_spending_by_state', allow_moved_paths := true)
  WHERE state_abbr = 'CA' AND program = 'social_security_retirement');

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
-- TABLE: snap_benefits_by_geography (USDA FNA; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T2_row_count',
  CASE WHEN n >= 20000 THEN 'pass' ELSE 'fail' END, n, 20000, 'Expected >=20000 state-year-month rows (FY1989-present x 56 geographies x ~13 rows/year)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true))
    -- month is legitimately null on every Total (fiscal-year rollup) row, not a data defect
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'month')));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'snap_benefits_by_geography', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/snap_benefits_by_geography', allow_moved_paths := true) WHERE state_fips IS NULL);

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
-- TABLE: state_corporate_income_tax_collections (Census STC govsstatetax
-- timeseries API, ITEM_CODE=T41 only; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true));

-- 51 (50 states + DC) x N years; a full-range run covers 2016-current (10+
-- years), but a scoped DQ run may cover far fewer years, so the floor is one
-- year's worth (51 rows), not the full-range expectation.
INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T2_row_count',
  CASE WHEN n >= 51 THEN 'pass' ELSE 'fail' END, n, 51, 'Expected >=51 state rows (>=1 year, DQ-scoped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true) LIMIT 3;

-- collections_thousands is expected to carry real NULLs (states with no T41-
-- classified tax that year), so 100%-null would only occur if every state
-- were unmapped — a real defect. Excluded from the "all_null" NOT IN list
-- like every other data column; only type/year are exempted as partitions.
INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL/duplicate (year, state_fips) rows'
FROM (SELECT COUNT(*) AS n FROM (
    SELECT year, state_fips, COUNT(*) AS dup_count
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true)
    GROUP BY year, state_fips
    HAVING year IS NULL OR state_fips IS NULL OR COUNT(*) > 1
  ));

-- Expected-value sanity check: California's T41 collections should be a
-- plausible multi-billion-dollar figure, not near-zero or absurdly large.
-- Confirmed live 2026-09-06: CA 2024 = $41.4B (41,408,314 thousand).
INSERT INTO dq_results
SELECT 'fiscal', 'state_corporate_income_tax_collections', 'T7_ca_magnitude_sane',
  CASE WHEN n BETWEEN 5000000 AND 100000000 THEN 'pass' ELSE 'fail' END, n, 41408314,
  'California collections_thousands for its latest available year (expect $5B-$100B range)'
FROM (
  SELECT collections_thousands AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/state_corporate_income_tax_collections', allow_moved_paths := true)
  WHERE state_fips = '06'
  ORDER BY year DESC
  LIMIT 1
);

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
-- broadband_reconnect_awards (USDA ReConnect CFDA 10.752 awards; snapshot)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true));

-- T2: row_count. Confirmed live 5 Sep 2026: 145 awards across all ReConnect rounds.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50, 'Expected >=50 award rows (145 confirmed live 5 Sep 2026)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols — cfda_number is a real constant (this table is single-CFDA by
-- design), excluded alongside the partition column.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'cfda_number')));

-- awarding_agency is a real constant (USDA Rural Utilities Service runs ReConnect
-- alone), excluded alongside the partition/CFDA columns.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'cfda_number', 'awarding_agency')));

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_reconnect_awards', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL award_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_reconnect_awards', allow_moved_paths := true)
  WHERE award_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- broadband_bead_state_allocations (NTIA BEAD CFDA 11.035 awards; snapshot)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true));

-- T2: row_count. One prime grant per state/territory/DC; confirmed live 5 Sep 2026 at 63.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50, 'Expected >=50 state/territory rows (63 confirmed live 5 Sep 2026)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'cfda_number')));

-- awarding_agency is a real constant (NTIA runs BEAD alone), excluded alongside the
-- partition/CFDA columns.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'cfda_number', 'awarding_agency')));

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_bead_state_allocations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL award_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_bead_state_allocations', allow_moved_paths := true)
  WHERE award_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- broadband_caf_deployment_locations (USAC CAF-II deployment locations; delta by filing year)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true));

-- T2: row_count. Year-partitioned; per-year counts vary widely (4 in 2015's partial
-- first year to 1.3M in 2020's peak reporting year) — 1000 is a conservative floor
-- that even a sparse DQ-window year clears.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows per year (year-partitioned; production spans 2015-2025)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols — fund_type is a real constant (this table is CAF-II-only by
-- design; other_technology/latency/overlapping_locations are legitimately rare and
-- may be all-null within a single-year DQ window, not a defect).
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'fund_type')));

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'fund_type')));

-- T6: pk_nulls. No natural single-column PK (a given census_block can carry multiple
-- filing-year rows); census_block is the geographic join key and must always be present.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL census_block rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true)
  WHERE census_block IS NULL);

-- T7: census_block is always the full 15-digit GEOID (state+county+tract+block),
-- confirmed live 5 Sep 2026 (0 malformed rows across 2024-2025) — a shorter/malformed
-- value would break the county_fips derivation (first 5 digits) documented in the
-- table's schema comment.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T7_census_block_shape',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'census_block rows not exactly 15 digits'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true)
  WHERE LENGTH(census_block) <> 15);

-- T7b: fund_type scoped to the 2 documented CAF-II variants.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_caf_deployment_locations', 'T7_fund_type_scope',
  CASE WHEN n = 2 THEN 'pass' ELSE 'warn' END, n, 2,
  'Distinct fund_type values found (expect exactly 2: CAF II / CAF II Auc)'
FROM (SELECT COUNT(DISTINCT fund_type) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_caf_deployment_locations', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- broadband_high_cost_disbursements (USAC USF high-cost program disbursements; delta by year)
-- ─────────────────────────────────────────────────────────────

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true));

-- T2: row_count. Year-partitioned; ~100K rows per year across all high-cost
-- programs. Confirmed live 5 Sep 2026: total 1,045,128 rows across 2016-2026.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T2_row_count',
  CASE WHEN n >= 20000 THEN 'pass' ELSE 'fail' END, n, 20000, 'Expected >=20,000 rows per year (year-partitioned; production spans 2016-2026)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

-- T5: all_same_value — disbursement_year is genuinely constant within any single-year
-- partition (real constant, not a defect), excluded alongside the partition columns.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'disbursement_year')));

-- T6: pk_nulls. No natural single-column PK (a given (form_498_id, month, fund_type)
-- can recur with different study_area_codes when a carrier serves multiple areas).
-- form_498_id itself is null on genuine zero-disbursement rows (a study area with no
-- filed Form 498 that period — confirmed live: 449/149,425 rows in the 2024-2025
-- window, all amount_disbursed=0), so study_area_code is the column that must always
-- be present instead.
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL study_area_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true)
  WHERE study_area_code IS NULL);

-- T7: fund_type coverage — at least 15 of the 25+ documented USF programs must be
-- present in any full year (some legacy programs may be absent in the most recent
-- year as they wind down, so the threshold sits below the full 25 count).
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T7_fund_type_coverage',
  CASE WHEN n >= 15 THEN 'pass' ELSE 'warn' END, n, 15,
  'Distinct fund_type values found (expect >=15 across a full year; 25+ across full history)'
FROM (SELECT COUNT(DISTINCT fund_type) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true));

-- T7b: month values are all 1-12 (a bad row would surface here rather than as a
-- silently-mistyped disbursement).
INSERT INTO dq_results
SELECT 'fiscal', 'broadband_high_cost_disbursements', 'T7_month_range',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with disbursement_month outside 1-12'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fiscal/broadband_high_cost_disbursements', allow_moved_paths := true)
  WHERE disbursement_month IS NULL OR disbursement_month < 1 OR disbursement_month > 12);

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
