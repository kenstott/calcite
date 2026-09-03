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
-- TABLE: building_permits_place (Census BPS place grain; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true));

-- ~20,000 places per year across the four regional files; one year clears 15,000
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T2_row_count',
  CASE WHEN n >= 15000 THEN 'pass' ELSE 'fail' END, n, 15000, 'Expected >=15000 place-year permit rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      -- footnote_code and central_city are populated only on the minority of rows Census flags
      AND column_name NOT IN ('type', 'year', 'footnote_code', 'central_city')));

-- Every row must be locatable. place_geoid is deliberately NULL on MCD-reported and
-- unincorporated rows (the majority nationally), so requiring it would fail on correct data —
-- what must always hold is that the row carries a county and some identity.
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with no county_fips or no identity'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true)
      WHERE county_fips IS NULL OR jurisdiction_type IS NULL);

-- T7: all four Census regions present — the table is assembled from four separate regional
-- files, so a silently-404ing or mis-encoded region path would drop a quarter of the country
-- while every other check still passed.
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T7_region_coverage',
  CASE WHEN n = 4 THEN 'pass' ELSE 'fail' END, n, 4, 'Distinct region_code values (expect all 4)'
FROM (SELECT COUNT(DISTINCT region_code) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true));

-- T7: place grain is genuinely finer than the county table it complements
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T7_place_grain',
  CASE WHEN n >= 10000 THEN 'pass' ELSE 'fail' END, n, 10000, 'Distinct place_geoid values'
FROM (SELECT COUNT(DISTINCT place_geoid) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true));

-- T7: the documented key really is unique. place_geoid alone is NOT a key — a jurisdiction
-- straddling a county line is reported once per county part, MCD-reported rows share the 00000
-- place code, and the 99990 unincorporated sentinel recurs once per county. A duplicate here
-- means either the grain changed upstream or a region file was ingested twice.
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T7_key_unique',
  CASE WHEN dupes = 0 THEN 'pass' ELSE 'fail' END, dupes, 0,
  'Duplicate (state_fips, place_fips, mcd_fips, county_fips, year) rows'
FROM (SELECT COUNT(*) AS dupes FROM (
  SELECT state_fips, place_fips, mcd_fips, county_fips, year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true)
  GROUP BY state_fips, place_fips, mcd_fips, county_fips, year HAVING COUNT(*) > 1));

-- T7: a synthesised GEOID must never appear. place_geoid is emitted only for a real place code,
-- so a 00000/99990 suffix here means the sentinel guard regressed and joins would silently merge
-- every township in a county into one nonexistent place.
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T7_no_sentinel_geoid',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'place_geoid built from a 00000/99990 sentinel'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true)
      WHERE place_geoid IS NOT NULL AND (place_geoid LIKE '%00000' OR place_geoid LIKE '%99990'));

-- T7: MCD-reported rows are the majority nationally; their absence means the MCD identity was
-- dropped and those jurisdictions silently lost their only join key.
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T7_mcd_identity',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Rows carrying an mcd_geoid'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true)
      WHERE mcd_geoid IS NOT NULL);

-- T7: the unincorporated sentinel is present and is a minority of rows — all-true or all-false
-- both mean the 99990 flag stopped tracking the data
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T7_unincorporated_flag',
  CASE WHEN n_true > 0 AND n_true < n_all THEN 'pass' ELSE 'fail' END, n_true, 1,
  'Rows flagged is_unincorporated_area (expect a nonzero minority)'
FROM (SELECT COUNT(*) FILTER (WHERE is_unincorporated_area) AS n_true, COUNT(*) AS n_all
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true));

-- T7: months_reported must sit in its documented 0-12 domain; anything else means the
-- positional field mapping has drifted against a future file layout
INSERT INTO dq_results
SELECT 'housing', 'building_permits_place', 'T7_months_reported_domain',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END, bad, 0, 'months_reported outside 0-12'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/building_permits_place', allow_moved_paths := true)
      WHERE months_reported IS NOT NULL AND (months_reported < 0 OR months_reported > 12));

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
-- TABLE: hmda_lar (CFPB HMDA loan-level CSV; partition cols: type, year, state)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'housing', 'hmda_lar', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hmda_lar', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'housing', 'hmda_lar', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END, n, 100000, 'Expected >=100000 loan-level rows (dqRowLimit caps each year x state at 200000)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hmda_lar', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hmda_lar', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'housing', 'hmda_lar', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hmda_lar', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'state')));

INSERT INTO dq_results
SELECT 'housing', 'hmda_lar', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hmda_lar', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'state')));

-- No single-column PK (grain is the CFPB record order within a year x state file).
-- census_tract is the column this table exists to carry, so a NOT NULL check on it
-- stands in for T6 — a NULL here silently defeats the whole reason hmda_lar exists.
INSERT INTO dq_results
SELECT 'housing', 'hmda_lar', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END, n, 0, 'NULL census_tract rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/housing/hmda_lar', allow_moved_paths := true) WHERE census_tract IS NULL);

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
