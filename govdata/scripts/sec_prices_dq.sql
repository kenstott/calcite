-- dq-lookback: 1
-- sec_prices_dq.sql — SEC Prices Data Quality (stock price time series)
-- Optional dataset within sec schema. Run separately: run-pool sec_prices:dq
-- Usage: source .env.prod && envsubst < scripts/sec_prices_dq.sql | duckdb
-- Output: structured dq_results table + per-table summary + schema-level pass/fail

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';
SET unsafe_enable_version_guessing=true;

CREATE TEMP TABLE dq_results (
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,   -- pass | warn | fail
  value     VARCHAR,
  threshold VARCHAR,
  detail    VARCHAR
);

-- ============================================================
-- T1: EXISTENCE
-- COUNT of a LIMIT 1 subquery: 1 = readable, 0 = empty table.
-- If the Iceberg metadata is missing, DuckDB aborts here — that IS the signal.
-- ============================================================
SELECT '=== T1: EXISTENCE ===' AS section;

INSERT INTO dq_results
SELECT 'sec_prices', tbl, 'existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  CAST(n AS VARCHAR), '1',
  CASE WHEN n > 0 THEN 'readable and non-empty' ELSE 'accessible but empty' END
FROM (
  SELECT 'stock_prices' AS tbl, COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices', allow_moved_paths=true) LIMIT 1)
);

-- ============================================================
-- T2: ROW COUNTS
-- Thresholds set conservatively to catch zero/near-zero tables.
-- stock_prices: minimum ~10k rows expected for multi-year, multi-ticker historical data
-- ============================================================
SELECT '=== T2: ROW COUNTS ===' AS section;

INSERT INTO dq_results
SELECT 'sec_prices', tbl, 'row_count',
  CASE WHEN n >= thresh THEN 'pass' ELSE 'fail' END,
  CAST(n AS VARCHAR), CAST(thresh AS VARCHAR), NULL
FROM (
  SELECT 'stock_prices' AS tbl, COUNT(*) AS n, 10000 AS thresh FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices', allow_moved_paths=true)
);

-- ============================================================
-- T3: SAMPLE ROW — one row per table for visual inspection (not stored in dq_results)
-- ============================================================
SELECT '=== T3: SAMPLE ROWS ===' AS section;

SELECT 'stock_prices' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices', allow_moved_paths=true) LIMIT 1;

-- ============================================================
-- REPORT: dq_results summary
-- ============================================================
SELECT '=== DQ RESULTS ===' AS section;

SELECT * FROM dq_results ORDER BY schema, tbl, test;

SELECT '=== SCHEMA SUMMARY ===' AS section;

SELECT
  schema,
  COUNT(*) AS total_tests,
  COUNT(*) FILTER (WHERE status = 'pass') AS passed,
  COUNT(*) FILTER (WHERE status = 'warn') AS warned,
  COUNT(*) FILTER (WHERE status = 'fail') AS failed,
  CASE WHEN COUNT(*) FILTER (WHERE status = 'fail') > 0 THEN 'FAIL' ELSE 'PASS' END AS overall
FROM dq_results
GROUP BY schema
ORDER BY schema;
