-- dq-lookback: 1
-- sec_enforcement_dq.sql — SEC enforcement_actions (litigation releases + administrative
-- proceedings) Data Quality. The sec_enforcement slot populates only this table.

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
SELECT 'sec_enforcement','enforcement_actions','existence',
  CASE WHEN nmeta > 0 THEN 'pass' ELSE 'fail' END, CAST(nmeta AS VARCHAR),'1',
  CASE WHEN nmeta > 0 THEN 'iceberg table present' ELSE 'iceberg table missing' END
FROM (SELECT (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_actions/metadata/*.json')) AS nmeta);

SELECT '=== T2: ROW COUNTS ===' AS section;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_actions','row_count_civil',
  CASE WHEN n>=2500 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'2500',
  CASE WHEN n>=2500 THEN 'civil rows meet minimum' ELSE 'civil rows below minimum' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_actions', allow_moved_paths=true) WHERE action_type='civil') t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_actions','row_count_administrative',
  CASE WHEN n>=15000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'15000',
  CASE WHEN n>=15000 THEN 'administrative rows meet minimum' ELSE 'administrative rows below minimum' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_actions', allow_moved_paths=true) WHERE action_type='administrative') t;

SELECT '=== T6: KEY NULLS ===' AS section;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_actions','key_nulls',
  CASE WHEN n=0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'0',
  CASE WHEN n=0 THEN 'no null key columns' ELSE 'null key columns' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_actions', allow_moved_paths=true) WHERE release_number IS NULL OR filed_date IS NULL OR respondents IS NULL OR action_type IS NULL OR fiscal_year IS NULL) t;

-- The AP listing carries no File Number for older orders (verified in the DQ bucket: 2678 of 18753
-- administrative rows, mostly 2005-2008, link to a .txt order with no file number on the listing).
-- The check is therefore a coverage floor, not an all-rows rule.
INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_actions','administrative_file_number_coverage',
  CASE WHEN pct >= 80 THEN 'pass' ELSE 'fail' END, CAST(ROUND(pct,1) AS VARCHAR),'>=80',
  CASE WHEN pct >= 80 THEN 'file_number present on most administrative rows' ELSE 'file_number coverage below floor' END
FROM (SELECT 100.0*COUNT(file_number)/COUNT(*) pct FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_actions', allow_moved_paths=true) WHERE action_type='administrative') t;

SELECT '=== T7: VALUE RANGES ===' AS section;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_actions','oldest_release',
  CASE WHEN d <= DATE '1995-12-31' THEN 'pass' ELSE 'fail' END, CAST(d AS VARCHAR),'<=1995-12-31',
  CASE WHEN d <= DATE '1995-12-31' THEN 'history reaches 1995' ELSE 'history does not reach 1995' END
FROM (SELECT MIN(filed_date) d FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_actions', allow_moved_paths=true) WHERE action_type='civil') t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_actions','newest_release',
  CASE WHEN d >= current_date - INTERVAL 60 DAY THEN 'pass' ELSE 'fail' END, CAST(d AS VARCHAR),'within 60 days',
  CASE WHEN d >= current_date - INTERVAL 60 DAY THEN 'recent releases present' ELSE 'newest release older than 60 days' END
FROM (SELECT MAX(filed_date) d FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_actions', allow_moved_paths=true)) t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_actions','fiscal_year_consistent',
  CASE WHEN n=0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'0',
  CASE WHEN n=0 THEN 'fiscal_year matches filed_date' ELSE 'fiscal_year disagrees with filed_date' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_actions', allow_moved_paths=true) WHERE fiscal_year <> year(filed_date) + CASE WHEN month(filed_date)>=10 THEN 1 ELSE 0 END) t;

SELECT '=== T8: DUPLICATES ===' AS section;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_actions','pk_duplicates',
  CASE WHEN n=0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'0',
  CASE WHEN n=0 THEN 'no duplicate (release_number, file_number, respondents, release_url)' ELSE 'duplicate rows' END
FROM (SELECT COUNT(*) n FROM (SELECT release_number,file_number,respondents,release_url FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_actions', allow_moved_paths=true) GROUP BY ALL HAVING COUNT(*)>1)) t;

-- enforcement_annual_results: one row per fiscal year, FY2019-FY2024 from the FY2024 release plus FY2025 and FY2026.
INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_annual_results','existence',
  CASE WHEN n>0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'1',
  CASE WHEN n>0 THEN 'iceberg table present' ELSE 'iceberg table missing' END
FROM (SELECT (SELECT COUNT(*) FROM glob('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_annual_results/metadata/*.json')) n) t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_annual_results','row_count',
  CASE WHEN n=8 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'8',
  CASE WHEN n=8 THEN 'one row per FY2019-FY2026' ELSE 'expected 8 fiscal-year rows' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_annual_results', allow_moved_paths=true)) t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_annual_results','fiscal_year_range',
  CASE WHEN lo=2019 AND hi=2026 THEN 'pass' ELSE 'fail' END, CAST(lo || '-' || hi AS VARCHAR),'2019-2026',
  CASE WHEN lo=2019 AND hi=2026 THEN 'FY2019-FY2026 present' ELSE 'fiscal-year range wrong' END
FROM (SELECT MIN(fiscal_year) lo, MAX(fiscal_year) hi FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_annual_results', allow_moved_paths=true)) t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_annual_results','pk_duplicates',
  CASE WHEN n=0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'0',
  CASE WHEN n=0 THEN 'one row per fiscal_year' ELSE 'duplicate fiscal_year' END
FROM (SELECT COUNT(*) n FROM (SELECT fiscal_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_annual_results', allow_moved_paths=true) GROUP BY fiscal_year HAVING COUNT(*)>1)) t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_annual_results','key_nulls',
  CASE WHEN n=0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'0',
  CASE WHEN n=0 THEN 'no null key columns' ELSE 'null key columns' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_annual_results', allow_moved_paths=true) WHERE fiscal_year IS NULL OR total_actions IS NULL OR release_url IS NULL OR addendum_url IS NULL) t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_annual_results','actions_sum_to_total',
  CASE WHEN n=0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'0',
  CASE WHEN n=0 THEN 'standalone + follow-on + delinquent = total where all published' ELSE 'components do not sum to total' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_annual_results', allow_moved_paths=true) WHERE delinquent_filings IS NOT NULL AND standalone_actions+follow_on_actions+delinquent_filings <> total_actions) t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_annual_results','fy2026_values',
  CASE WHEN n=1 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'1',
  CASE WHEN n=1 THEN 'FY2026 matches the SEC addendum (235/47/66/348, $343M penalties, $752M disgorgement)' ELSE 'FY2026 row differs from the addendum' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_annual_results', allow_moved_paths=true) WHERE fiscal_year=2026 AND standalone_actions=235 AND follow_on_actions=47 AND delinquent_filings=66 AND total_actions=348 AND penalties_millions=343 AND disgorgement_millions=752 AND trading_suspensions=13) t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_annual_results','fy2022_revised',
  CASE WHEN n=1 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'1',
  CASE WHEN n=1 THEN 'FY2022 carries the revised 463 standalone actions' ELSE 'FY2022 not the revised 463' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_annual_results', allow_moved_paths=true) WHERE fiscal_year=2022 AND standalone_actions=463 AND total_actions=761) t;

INSERT INTO dq_results SELECT 'sec_enforcement','enforcement_annual_results','money_sum_to_total',
  CASE WHEN n=0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR),'0',
  CASE WHEN n=0 THEN 'penalties + disgorgement = total money ordered (within $2M rounding) where all published' ELSE 'money components do not sum to total' END
FROM (SELECT COUNT(*) n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/sec/enforcement_annual_results', allow_moved_paths=true) WHERE penalties_millions IS NOT NULL AND disgorgement_millions IS NOT NULL AND abs(penalties_millions+disgorgement_millions-total_money_ordered_millions)>2) t;

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
