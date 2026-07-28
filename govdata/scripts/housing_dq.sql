-- dq-lookback: 1
-- U.S. Housing Data Quality Checks
-- Schema: housing
-- Tables: house_price_index, building_permits, fair_market_rents, income_limits
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition columns ('type','year' for all; also 'state' for the HUD tables).
-- fair_market_rents/income_limits are disabled when HUD_TOKEN is absent, so their
-- checks fail-fast (0 rows) in that configuration — expected, not a data defect.

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
-- TABLE: house_price_index (FHFA snapshot; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'housing', 'house_price_index', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/house_price_index', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'housing', 'house_price_index', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 index observation rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/house_price_index', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/house_price_index', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'housing', 'house_price_index', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/house_price_index', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'housing', 'house_price_index', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/house_price_index', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'housing', 'house_price_index', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL place_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/house_price_index', allow_moved_paths := true) WHERE place_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: building_permits (Census BPS; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'housing', 'building_permits', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'housing', 'building_permits', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 county-year permit rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'housing', 'building_permits', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'housing', 'building_permits', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'housing', 'building_permits', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits', allow_moved_paths := true) WHERE county_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: fair_market_rents (HUD; partition cols: type, year, state)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'housing', 'fair_market_rents', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/fair_market_rents', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'housing', 'fair_market_rents', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 county-year FMR rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/fair_market_rents', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/fair_market_rents', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'housing', 'fair_market_rents', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/fair_market_rents', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'state')));

INSERT INTO dq_results
SELECT 'housing', 'fair_market_rents', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/fair_market_rents', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'state')));

INSERT INTO dq_results
SELECT 'housing', 'fair_market_rents', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL hud_area_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/fair_market_rents', allow_moved_paths := true) WHERE hud_area_code IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: income_limits (HUD; partition cols: type, year, state)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'housing', 'income_limits', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'housing', 'income_limits', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 state-year income-limit rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'housing', 'income_limits', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'state')));

INSERT INTO dq_results
SELECT 'housing', 'income_limits', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'state')));

INSERT INTO dq_results
SELECT 'housing', 'income_limits', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits', allow_moved_paths := true) WHERE state_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: income_limits_county (HUD; partition cols: type, year, state)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'housing', 'income_limits_county', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits_county', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'housing', 'income_limits_county', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 area-year income-limit rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits_county', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits_county', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'housing', 'income_limits_county', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits_county', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'state')));

INSERT INTO dq_results
SELECT 'housing', 'income_limits_county', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits_county', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'state')));

INSERT INTO dq_results
SELECT 'housing', 'income_limits_county', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL hud_area_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/income_limits_county', allow_moved_paths := true) WHERE hud_area_code IS NULL);

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
-- ============================================================================
-- T1/T2 — tables previously absent from this file entirely.
-- A table with no checks emits no dq rows, which reads as healthy rather than as
-- untested; that is how four zero-row geo tables went unnoticed. Existence +
-- row_count only — deliberately minimal, to be deepened per table.
-- ============================================================================

INSERT INTO dq_results
WITH counts AS (
  SELECT 'hmda_applicant_demographics' AS tbl, (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hmda_applicant_demographics', allow_moved_paths := true) LIMIT 1)) AS n
  UNION ALL
  SELECT 'hmda_loans'                 , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hmda_loans', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'hud_subsidized_county'      , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hud_subsidized_county', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'hud_subsidized_housing'     , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hud_subsidized_housing', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'opportunity_zones'          , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/opportunity_zones', allow_moved_paths := true) LIMIT 1))
)
SELECT 'housing', tbl, 'existence',
       CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
       n, 1,
       CASE WHEN n > 0 THEN 'readable' ELSE 'NO ROWS — table unreadable or never written' END
FROM counts;


SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
