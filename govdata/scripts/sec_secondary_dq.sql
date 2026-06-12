-- dq-lookback: 1
-- sec_secondary_dq.sql — SEC secondary-filing tables Data Quality.
-- The sec_secondary slot fetches the 8-K / proxy / insider / 13F / 13D-G forms; its DQ covers the
-- tables those forms produce: insider_transactions (3/4/5), institutional_holdings (13F-HR),
-- beneficial_ownership (SC 13D/G), earnings_transcripts (8-K Item 2.02), plus the shared
-- filing_metadata. (10-K/10-Q XBRL tables belong to sec_primary; stock_prices to sec_prices; the
-- full SEC DQ lives in sec_dq.sql.) Resilient: glob()-based existence never throws on a missing
-- table; each row_count is its own statement so a missing table is skipped, not fatal.
--
-- DQ SAMPLE NOTE: runs against CikRegistry DQ_SAMPLE (Magnificent 7 + Berkshire Hathaway).
-- Mega-caps supply insider (3/4/5), 8-K earnings, and DEF 14A; Berkshire supplies 13F-HR
-- (institutional_holdings) and 13D/13G (beneficial_ownership). T2 floors are CONSERVATIVE
-- placeholders sized to catch zero/near-zero tables — calibrate up to real counts after the
-- first run (the iterate loop), exactly as sec_dq.sql / weather_dq.sql did.

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
SELECT 'sec_secondary', tbl, 'existence',
  CASE WHEN nmeta > 0 THEN 'pass' ELSE 'fail' END,
  CAST(nmeta AS VARCHAR), '1',
  CASE WHEN nmeta > 0 THEN 'iceberg table present' ELSE 'iceberg table missing' END
FROM (
  SELECT 'filing_metadata'        AS tbl, (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata/metadata/*.json'))        AS nmeta
  UNION ALL SELECT 'insider_transactions',   (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/insider_transactions/metadata/*.json'))
  UNION ALL SELECT 'institutional_holdings', (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings/metadata/*.json'))
  UNION ALL SELECT 'beneficial_ownership',   (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/beneficial_ownership/metadata/*.json'))
  UNION ALL SELECT 'earnings_transcripts',   (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/earnings_transcripts/metadata/*.json'))
);

SELECT '=== T2: ROW COUNTS ===' AS section;

-- Each table a SEPARATE INSERT so a missing table's iceberg_scan() error aborts only that
-- statement (the CLI continues; T1 already flagged it FAIL). Conservative floors — calibrate up.
INSERT INTO dq_results SELECT 'sec_secondary','filing_metadata','row_count', CASE WHEN n>=50 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'50', CASE WHEN n>=50 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/filing_metadata', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec_secondary','insider_transactions','row_count', CASE WHEN n>=100 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'100', CASE WHEN n>=100 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/insider_transactions', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec_secondary','institutional_holdings','row_count', CASE WHEN n>=50 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'50', CASE WHEN n>=50 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/institutional_holdings', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec_secondary','beneficial_ownership','row_count', CASE WHEN n>=1 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'1', CASE WHEN n>=1 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/beneficial_ownership', allow_moved_paths=true)) t;
INSERT INTO dq_results SELECT 'sec_secondary','earnings_transcripts','row_count', CASE WHEN n>=10 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'10', CASE WHEN n>=10 THEN 'row count meets minimum' ELSE 'row count below minimum' END FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/earnings_transcripts', allow_moved_paths=true)) t;

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
