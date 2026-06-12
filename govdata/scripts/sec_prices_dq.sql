-- dq-lookback: 1
-- sec_prices_dq.sql — SEC stock_prices (Stooq bulk + top-up) Data Quality.
-- The sec_prices slot is ETL-only for the rest of the SEC schema; its DQ is just the
-- stock_prices table (the full SEC DQ lives in sec_dq.sql). Resilient: glob()-based existence
-- never throws on a missing table; the row_count is its own statement so a missing table is
-- skipped rather than aborting the run.

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
  status    VARCHAR,
  value     VARCHAR,
  threshold VARCHAR,
  detail    VARCHAR
);

SELECT '=== T1: EXISTENCE ===' AS section;

INSERT INTO dq_results
SELECT 'sec_prices', 'stock_prices', 'existence',
  CASE WHEN nmeta > 0 THEN 'pass' ELSE 'fail' END,
  CAST(nmeta AS VARCHAR), '1',
  CASE WHEN nmeta > 0 THEN 'iceberg table present' ELSE 'iceberg table missing' END
FROM (SELECT (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices/metadata/*.json')) AS nmeta);

SELECT '=== T2: ROW COUNTS ===' AS section;

INSERT INTO dq_results SELECT 'sec_prices','stock_prices','row_count',
  CASE WHEN n>=10000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'10000',
  CASE WHEN n>=10000 THEN 'row count meets minimum' ELSE 'row count below minimum' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices', allow_moved_paths=true)) t;

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
