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
-- T1: EXISTENCE — checks for the Iceberg table's metadata via glob(), which returns 0 (never
-- throws) when a table directory is absent. This is RESILIENT: a missing table reports a row
-- instead of aborting the whole run, unlike iceberg_scan() which raises "Could not guess
-- version" and kills the enclosing statement. A present table → pass; a missing table → fail,
-- EXCEPT vectorized_chunks (warn-only: it needs the Jina embedding pipeline, environment-
-- dependent and legitimately absent in a DQ run without embedding credentials).
-- ============================================================
SELECT '=== T1: EXISTENCE ===' AS section;

INSERT INTO dq_results
SELECT 'sec', tbl, 'existence',
  CASE WHEN nmeta > 0 THEN 'pass'
       WHEN tbl = 'vectorized_chunks' THEN 'warn'
       ELSE 'fail' END,
  CAST(nmeta AS VARCHAR), '1',
  CASE WHEN nmeta > 0 THEN 'iceberg table present' ELSE 'iceberg table missing' END
FROM (
  SELECT 'filing_metadata'       AS tbl, (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata/metadata/*.json'))       AS nmeta
  UNION ALL SELECT 'financial_line_items',  (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/financial_line_items/metadata/*.json'))
  UNION ALL SELECT 'filing_contexts',       (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/filing_contexts/metadata/*.json'))
  UNION ALL SELECT 'mda_sections',          (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/mda_sections/metadata/*.json'))
  UNION ALL SELECT 'xbrl_relationships',    (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/xbrl_relationships/metadata/*.json'))
  UNION ALL SELECT 'insider_transactions',  (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/insider_transactions/metadata/*.json'))
  UNION ALL SELECT 'institutional_holdings',(SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings/metadata/*.json'))
  UNION ALL SELECT 'beneficial_ownership',  (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/beneficial_ownership/metadata/*.json'))
  UNION ALL SELECT 'earnings_transcripts',  (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/earnings_transcripts/metadata/*.json'))
  UNION ALL SELECT 'stock_prices',          (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices/metadata/*.json'))
  UNION ALL SELECT 'vectorized_chunks',     (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/vectorized_chunks/metadata/*.json'))
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
-- Each table is a SEPARATE INSERT so a missing table's iceberg_scan() error aborts only that
-- one statement; the DuckDB CLI continues to the next (a missing table is already flagged FAIL
-- by T1). A present-but-empty table reads 0 here and fails the floor. vectorized_chunks is
-- EXCLUDED (existence-warn only — embedding-pipeline dependent).
SELECT '=== T2: ROW COUNTS ===' AS section;

INSERT INTO dq_results SELECT 'sec','filing_metadata','row_count', CASE WHEN n>=200 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'200', CASE WHEN n>=200 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','financial_line_items','row_count', CASE WHEN n>=2000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'2000', CASE WHEN n>=2000 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/financial_line_items', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','filing_contexts','row_count', CASE WHEN n>=1000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'1000', CASE WHEN n>=1000 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_contexts', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','mda_sections','row_count', CASE WHEN n>=20 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'20', CASE WHEN n>=20 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/mda_sections', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','xbrl_relationships','row_count', CASE WHEN n>=1000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'1000', CASE WHEN n>=1000 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/xbrl_relationships', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','insider_transactions','row_count', CASE WHEN n>=200 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'200', CASE WHEN n>=200 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/insider_transactions', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','institutional_holdings','row_count', CASE WHEN n>=50 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'50', CASE WHEN n>=50 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','beneficial_ownership','row_count', CASE WHEN n>=5 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'5', CASE WHEN n>=5 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/beneficial_ownership', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','earnings_transcripts','row_count', CASE WHEN n>=10 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'10', CASE WHEN n>=10 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/earnings_transcripts', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec','stock_prices','row_count', CASE WHEN n>=10000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'10000', CASE WHEN n>=10000 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/stock_prices', allow_moved_paths=true)) t;

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
