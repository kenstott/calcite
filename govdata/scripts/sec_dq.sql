-- dq-lookback: 1
-- sec_dq.sql — SEC Schema Data Quality
-- Follows the reference template established in weather_dq.sql / econ_dq.sql.
-- Usage: source .env.prod && envsubst < scripts/sec_dq.sql | duckdb
-- Output: structured dq_results table + per-table summary + schema-level pass/fail
--
-- DQ SAMPLE NOTE: SEC DQ runs against the CikRegistry DQ_SAMPLE group (worker.sh sec/
-- sec_prices DQ mode) — the MAGNIFICENT7 mega-caps + Berkshire Hathaway. Mega-caps supply
-- rich 10-K/10-Q/8-K/DEF 14A/insider data; Berkshire supplies Form 13F-HR (institutional_
-- holdings) and Schedule 13D/13G (beneficial_ownership) so every table gets real data. The
-- T2 floors below are CONSERVATIVE placeholders sized to catch zero/near-zero tables for this
-- ~8-CIK × DQ-window sample — calibrate them up to the actual counts after the first run
-- (the iterate loop), exactly as weather/econ did.

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
-- T1: EXISTENCE — COUNT of a LIMIT 1 subquery: 1 = readable, 0 = empty.
-- vectorized_chunks is existence-WARN-not-fail: it needs the Jina embedding pipeline
-- (SecTextVectorizer), which is environment-dependent and legitimately empty in a DQ run
-- without embedding credentials.
-- ============================================================
SELECT '=== T1: EXISTENCE ===' AS section;

INSERT INTO dq_results
SELECT 'sec', tbl, 'existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  CAST(n AS VARCHAR), '1',
  CASE WHEN n > 0 THEN 'readable and non-empty' ELSE 'accessible but empty' END
FROM (
  SELECT 'filing_metadata'       AS tbl, (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata',       allow_moved_paths=true) LIMIT 1) t) AS n
  UNION ALL SELECT 'financial_line_items',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/financial_line_items',  allow_moved_paths=true) LIMIT 1) t)
  UNION ALL SELECT 'filing_contexts',       (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_contexts',       allow_moved_paths=true) LIMIT 1) t)
  UNION ALL SELECT 'mda_sections',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/mda_sections',          allow_moved_paths=true) LIMIT 1) t)
  UNION ALL SELECT 'xbrl_relationships',    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/xbrl_relationships',    allow_moved_paths=true) LIMIT 1) t)
  UNION ALL SELECT 'insider_transactions',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/insider_transactions',  allow_moved_paths=true) LIMIT 1) t)
  UNION ALL SELECT 'institutional_holdings',(SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings',allow_moved_paths=true) LIMIT 1) t)
  UNION ALL SELECT 'beneficial_ownership',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/beneficial_ownership',  allow_moved_paths=true) LIMIT 1) t)
  UNION ALL SELECT 'earnings_transcripts',  (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/earnings_transcripts',  allow_moved_paths=true) LIMIT 1) t)
  UNION ALL SELECT 'stock_prices',          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices',          allow_moved_paths=true) LIMIT 1) t)
  UNION ALL SELECT 'vectorized_chunks',     (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/vectorized_chunks',     allow_moved_paths=true) LIMIT 1) t)
);

-- ============================================================
-- T2: ROW COUNTS — conservative, sample-aware floors. Tiers:
--   rich (10-K/10-Q XBRL):  financial_line_items, filing_contexts, xbrl_relationships
--   moderate:               filing_metadata, insider_transactions, mda_sections,
--                           earnings_transcripts, stock_prices
--   filer-type (Berkshire): institutional_holdings (13F), beneficial_ownership (13D/G)
-- vectorized_chunks is EXCLUDED from T2 (existence-warn only) — embedding-pipeline dependent.
-- CALIBRATE these up to the real DQ_SAMPLE counts after the first run.
-- ============================================================
SELECT '=== T2: ROW COUNTS ===' AS section;

INSERT INTO dq_results
SELECT 'sec', tbl, 'row_count',
  CASE WHEN n >= thresh THEN 'pass' ELSE 'fail' END,
  CAST(n AS VARCHAR), CAST(thresh AS VARCHAR),
  CASE WHEN n >= thresh THEN 'row count meets minimum' ELSE 'row count below minimum — ingestion may be incomplete or filer-type missing from DQ sample' END
FROM (
  SELECT 'filing_metadata'        AS tbl, (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata',        allow_moved_paths=true)) AS n, 200   AS thresh
  UNION ALL SELECT 'financial_line_items',   (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/financial_line_items',   allow_moved_paths=true)), 2000
  UNION ALL SELECT 'filing_contexts',        (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_contexts',        allow_moved_paths=true)), 1000
  UNION ALL SELECT 'mda_sections',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/mda_sections',           allow_moved_paths=true)), 20
  UNION ALL SELECT 'xbrl_relationships',     (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/xbrl_relationships',     allow_moved_paths=true)), 1000
  UNION ALL SELECT 'insider_transactions',   (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/insider_transactions',   allow_moved_paths=true)), 200
  UNION ALL SELECT 'institutional_holdings', (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths=true)), 50
  UNION ALL SELECT 'beneficial_ownership',   (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/beneficial_ownership',   allow_moved_paths=true)), 5
  UNION ALL SELECT 'earnings_transcripts',   (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/earnings_transcripts',   allow_moved_paths=true)), 10
  UNION ALL SELECT 'stock_prices',           (SELECT COUNT(*) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices',           allow_moved_paths=true)), 10000
);

-- ============================================================
-- REPORT
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
