-- dq-lookback: 4
-- edu_dq.sql — Education Schema Data Quality
-- Reference template: implements all 7 DQ test categories.
-- Usage: source .env.prod && envsubst < scripts/edu_dq.sql | duckdb
-- Output: structured dq_results table + per-table summary + schema-level pass/fail

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';

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
-- One INSERT per table: a table with no Iceberg metadata loses only its own row, and the
-- runner records the failed statement as a sql_error result.
-- ============================================================
SELECT '=== T1: EXISTENCE ===' AS section;

CREATE TEMP TABLE dq_existence_counts (tbl VARCHAR, n BIGINT);
INSERT INTO dq_existence_counts SELECT 'ccd_districts'            AS tbl, COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_districts',           allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'ccd_schools',            COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools',            allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'naep_scores',            COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores',            allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'crdc_schools',           COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools',           allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'ipeds_institutions',     COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions',     allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'ipeds_completions',      COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions',      allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'ipeds_tuition',          COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition',          allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'ipeds_financials',       COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials',       allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'ipeds_fall_enrollment', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_fall_enrollment', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'ipeds_admissions', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_admissions', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'ipeds_financial_aid',    COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financial_aid', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'college_scorecard',      COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard',      allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'college_scorecard_programs', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'college_scorecard_latest', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_latest', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'naep_achievement_levels',   COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels',   allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'library_outlets',           COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/library_outlets',          allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'f33_district_finance',      COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/f33_district_finance',     allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'district_special_ed_counts', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_special_ed_counts', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'district_el_enrollment',   COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_el_enrollment', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'edfacts_chronic_absenteeism', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/edfacts_chronic_absenteeism', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'district_assessments',      COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_assessments',    allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'fsa_pslf_discharge_summary', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_pslf_discharge_summary', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'fsa_loan_portfolio_status', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_portfolio_status', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'fsa_loan_delinquency', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_delinquency', allow_moved_paths=true) LIMIT 1);
INSERT INTO dq_existence_counts SELECT 'sevis_student_counts', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/sevis_student_counts', allow_moved_paths=true) LIMIT 1);

INSERT INTO dq_results
SELECT 'edu', tbl, 'existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  CAST(n AS VARCHAR), '1',
  CASE WHEN n > 0 THEN 'readable and non-empty' ELSE 'accessible but empty' END
FROM dq_existence_counts;

-- ============================================================
-- T2: ROW COUNTS
-- Thresholds set conservatively to catch zero/near-zero tables.
-- ============================================================
SELECT '=== T2: ROW COUNTS ===' AS section;

-- Tables with releaseWindow constraints are legitimately empty outside their ingest window.
-- always_populated: ccd_schools, college_scorecard, college_scorecard_programs, college_scorecard_latest, ipeds_institutions → fail when empty
-- release_window:   all others → warn when empty (expected during off-window periods)
--
-- Cap-aware thresholds: three tables declare a table-level dqRowLimit in edu-schema.yaml, so in
-- DQ mode (GOVDATA_DQ=true) their row count is ~dqRowLimit × (#periods loaded), NOT the prod
-- magnitude. Thresholds below are floors set well under one full capped period so they still
-- catch under-population without false-failing when only one recent (lagged) period publishes:
--   ipeds_completions          dqRowLimit=5000/year      → floor 1000
--   crdc_schools               dqRowLimit=5000/yr×topic  → floor  500
--   college_scorecard_programs dqRowLimit=3000/year      → floor  500
-- Uncapped tables keep the original non-zero floor (100, or 10 for biennial state-level NAEP).
CREATE TEMP TABLE dq_row_count_counts (tbl VARCHAR, n BIGINT, thresh BIGINT);
INSERT INTO dq_row_count_counts SELECT 'ccd_districts'            AS tbl, COUNT(*) AS n, 100 AS thresh FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_districts',           allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'ccd_schools',                          COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools',            allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'naep_scores',                          COUNT(*),    10       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores',            allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'crdc_schools',                         COUNT(*),   500       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools',           allow_moved_paths=true);  -- dqRowLimit=5000/yr×topic
INSERT INTO dq_row_count_counts SELECT 'ipeds_institutions',                   COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions',     allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'ipeds_completions',                    COUNT(*),  1000       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions',      allow_moved_paths=true);  -- dqRowLimit=5000/year
INSERT INTO dq_row_count_counts SELECT 'ipeds_tuition',                        COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition',          allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'ipeds_financials',                     COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials',       allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'ipeds_fall_enrollment', COUNT(*), 10000 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_fall_enrollment', allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'ipeds_admissions', COUNT(*), 1000 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_admissions', allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'ipeds_financial_aid',                  COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financial_aid', allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'college_scorecard',                    COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard',      allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'college_scorecard_programs',           COUNT(*),   500       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true);  -- dqRowLimit=3000/year
INSERT INTO dq_row_count_counts SELECT 'college_scorecard_latest',             COUNT(*),  5500       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_latest', allow_moved_paths=true);  -- 6,272 institutions served 2026-11-25
INSERT INTO dq_row_count_counts SELECT 'naep_achievement_levels',              COUNT(*),    10       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels',   allow_moved_paths=true);
INSERT INTO dq_row_count_counts SELECT 'library_outlets',                      COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/library_outlets',          allow_moved_paths=true);  -- dqRowLimit=50000/year
INSERT INTO dq_row_count_counts SELECT 'f33_district_finance',                 COUNT(*),  5000       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/f33_district_finance',     allow_moved_paths=true);  -- ~14,088 LEAs/year verified FY2023
INSERT INTO dq_row_count_counts SELECT 'district_special_ed_counts',           COUNT(*),  5000       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_special_ed_counts', allow_moved_paths=true);  -- ~16,000-18,500 LEAs/year verified 2020-2024
INSERT INTO dq_row_count_counts SELECT 'district_el_enrollment',              COUNT(*),  5000       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_el_enrollment', allow_moved_paths=true);  -- ~17,700 LEAs/collection verified 2020-21 and 2021-22
INSERT INTO dq_row_count_counts SELECT 'edfacts_chronic_absenteeism',          COUNT(*), 100000       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/edfacts_chronic_absenteeism', allow_moved_paths=true);  -- ~390,000-475,000 rows per year (state + ~17,900 LEAs x subgroup x sex), 2020-2022
INSERT INTO dq_row_count_counts SELECT 'district_assessments',                 COUNT(*),   500       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_assessments',    allow_moved_paths=true);  -- dqRowLimit=2000/(year,grade)
INSERT INTO dq_row_count_counts SELECT 'fsa_pslf_discharge_summary',            COUNT(*),    30       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_pslf_discharge_summary', allow_moved_paths=true);  -- ~44 rows verified live 2026-09-27
INSERT INTO dq_row_count_counts SELECT 'fsa_loan_portfolio_status',            COUNT(*),   500       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_portfolio_status', allow_moved_paths=true);  -- 4 portfolios x ~50 quarters x 7-8 statuses
INSERT INTO dq_row_count_counts SELECT 'fsa_loan_delinquency',                 COUNT(*),   300       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_delinquency', allow_moved_paths=true);  -- 3 portfolios x ~45 quarters x 6 buckets
INSERT INTO dq_row_count_counts SELECT 'sevis_student_counts',                 COUNT(*), 500000       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/sevis_student_counts', allow_moved_paths=true);  -- ~72 months x ~229 countries x non-zero breakdown cells; ~844k rows verified 2026-11-16 (all non-zero cells of the feed)

INSERT INTO dq_results
SELECT 'edu', tbl, 'row_count',
  CASE
    WHEN n >= thresh                     THEN 'pass'
    WHEN tbl IN ('ccd_schools','college_scorecard','college_scorecard_programs','college_scorecard_latest','ipeds_institutions','fsa_pslf_discharge_summary','fsa_loan_portfolio_status','fsa_loan_delinquency','sevis_student_counts') THEN 'fail'
    ELSE 'warn'
  END,
  CAST(n AS VARCHAR), CAST(thresh AS VARCHAR), NULL
FROM dq_row_count_counts;

-- ============================================================
-- T3: SAMPLE ROW — one row per table for visual inspection (not stored in dq_results)
-- ============================================================
SELECT '=== T3: SAMPLE ROWS ===' AS section;

SELECT 'ccd_districts'            AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_districts',           allow_moved_paths=true) LIMIT 1;
SELECT 'ccd_schools'              AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools',            allow_moved_paths=true) LIMIT 1;
SELECT 'naep_scores'              AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores',            allow_moved_paths=true) LIMIT 1;
SELECT 'crdc_schools'             AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools',           allow_moved_paths=true) LIMIT 1;
SELECT 'ipeds_institutions'       AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions',     allow_moved_paths=true) LIMIT 1;
SELECT 'ipeds_completions'        AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions',      allow_moved_paths=true) LIMIT 1;
SELECT 'ipeds_tuition'            AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition',          allow_moved_paths=true) LIMIT 1;
SELECT 'ipeds_financials'         AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials',       allow_moved_paths=true) LIMIT 1;
SELECT 'ipeds_fall_enrollment' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_fall_enrollment', allow_moved_paths=True) LIMIT 1;
SELECT 'ipeds_admissions' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_admissions', allow_moved_paths=True) LIMIT 1;
SELECT 'ipeds_financial_aid'      AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financial_aid', allow_moved_paths=true) LIMIT 1;
SELECT 'college_scorecard'        AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard',      allow_moved_paths=true) LIMIT 1;
SELECT 'college_scorecard_programs' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true) LIMIT 1;
SELECT 'college_scorecard_latest' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_latest', allow_moved_paths=true) LIMIT 1;
SELECT 'naep_achievement_levels'   AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels',   allow_moved_paths=true) LIMIT 1;
SELECT 'library_outlets'          AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/library_outlets',          allow_moved_paths=true) LIMIT 1;
SELECT 'f33_district_finance'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/f33_district_finance',     allow_moved_paths=true) LIMIT 1;
SELECT 'district_special_ed_counts' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_special_ed_counts', allow_moved_paths=true) LIMIT 1;
SELECT 'district_el_enrollment'  AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_el_enrollment', allow_moved_paths=true) LIMIT 1;
SELECT 'edfacts_chronic_absenteeism' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/edfacts_chronic_absenteeism', allow_moved_paths=true) LIMIT 1;
SELECT 'district_assessments'     AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_assessments',    allow_moved_paths=true) LIMIT 1;

-- ============================================================
-- T4: ALL-NULL COLUMNS
-- SUMMARIZE reports null_percentage per column; 100.0 = entire column is null.
-- ============================================================
SELECT '=== T4: ALL-NULL COLUMNS ===' AS section;

INSERT INTO dq_results
SELECT 'edu', 'ccd_districts', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_districts', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'spec_ed_students', 'english_language_learners');

INSERT INTO dq_results
SELECT 'edu', 'ccd_schools', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'naep_scores', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'crdc_schools', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_institutions', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_completions', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_tuition', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_financials', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_fall_enrollment', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_fall_enrollment', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_admissions', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_admissions', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name',
                          'satwr25', 'satwr75', 'actwr25', 'actwr75');

-- ipeds_admissions writing percentiles: the NCES ADM files carry them for year 2014-2015 only,
-- so each must be populated whenever those years are loaded and null in every later year.
INSERT INTO dq_results
SELECT 'edu', 'ipeds_admissions', 'writing_scores_year_scope',
  CASE WHEN (n_early > 0 AND LEAST(s25, s75, a25, a75) = 0) OR n_late > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n_late AS VARCHAR), '0',
  'rows<=2015=' || n_early || ' satwr25=' || s25 || ' satwr75=' || s75 || ' actwr25=' || a25
    || ' actwr75=' || a75 || ' rows>2015 with a writing score=' || n_late
FROM (SELECT
  COUNT(*) FILTER (WHERE year <= 2015) AS n_early,
  COUNT(satwr25) FILTER (WHERE year <= 2015) AS s25,
  COUNT(satwr75) FILTER (WHERE year <= 2015) AS s75,
  COUNT(actwr25) FILTER (WHERE year <= 2015) AS a25,
  COUNT(actwr75) FILTER (WHERE year <= 2015) AS a75,
  COUNT(*) FILTER (WHERE year > 2015 AND COALESCE(satwr25, satwr75, actwr25, actwr75) IS NOT NULL) AS n_late
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_admissions', allow_moved_paths=true));

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_programs', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'naep_achievement_levels', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'library_outlets', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/library_outlets', allow_moved_paths=true))
WHERE null_percentage = 100.0
  -- square_feet/latitude/longitude legitimately null for some outlets (sentinel-code or
  -- unreported source rows); library_id excluded from PK but never itself all-null.
  AND column_name NOT IN ('type', 'year');

INSERT INTO dq_results
SELECT 'edu', 'f33_district_finance', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/f33_district_finance', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year');

INSERT INTO dq_results
SELECT 'edu', 'district_el_enrollment', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_el_enrollment', allow_moved_paths=true))
WHERE null_percentage = 100.0;

INSERT INTO dq_results
SELECT 'edu', 'district_assessments', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_assessments', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'grade');

INSERT INTO dq_results
SELECT 'edu', 'fsa_pslf_discharge_summary', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_pslf_discharge_summary', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

-- ============================================================
-- T5: ALL-SAME-VALUE COLUMNS
-- approx_unique <= 1 and not already 100% null = every non-null row has same value.
-- Reported as warn — may be legitimate for constant or sparse seed data.
-- ============================================================
SELECT '=== T5: ALL-SAME-VALUE COLUMNS ===' AS section;

INSERT INTO dq_results
SELECT 'edu', 'ccd_districts', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_districts', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ccd_schools', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'naep_scores', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  -- variable_type, subgroup_name, is_displayable are constant by design (variable=TOTAL, All students)
  AND column_name NOT IN ('type', 'variable_type', 'subgroup_name', 'is_displayable');

INSERT INTO dq_results
SELECT 'edu', 'crdc_schools', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_institutions', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_completions', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_tuition', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_financials', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_fall_enrollment', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_fall_enrollment', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_admissions', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_admissions', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_programs', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'naep_achievement_levels', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  -- variable_type, subgroup_name, is_displayable are constant by design (variable=TOTAL, All students)
  AND column_name NOT IN ('type', 'variable_type', 'subgroup_name', 'is_displayable');

INSERT INTO dq_results
SELECT 'edu', 'library_outlets', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/library_outlets', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year');

-- early_childhood_missing / school_age_missing exist only in the 2020-21..2022-23 source files; a
-- run covering 2023 onward has them entirely null by design.
INSERT INTO dq_results
SELECT 'edu', 'district_special_ed_counts', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_special_ed_counts', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'early_childhood_missing', 'school_age_missing');

INSERT INTO dq_results
SELECT 'edu', 'f33_district_finance', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/f33_district_finance', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type', 'year');

INSERT INTO dq_results
SELECT 'edu', 'district_el_enrollment', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_el_enrollment', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  -- el_nonbinary: null for 2020 (not collected) and 0 for every LEA in 2021 (the CRDC's first
  -- nonbinary collection; verified live 2026-11-15) — a real constant, not a defect.
  AND column_name <> 'el_nonbinary';

INSERT INTO dq_results
SELECT 'edu', 'district_assessments', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_assessments', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type', 'year', 'grade');

INSERT INTO dq_results
SELECT 'edu', 'fsa_pslf_discharge_summary', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_pslf_discharge_summary', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  -- report_as_of_date is constant by design: every row in one snapshot shares the same
  -- covering-month date (see table comment in edu-schema.yaml)
  AND column_name NOT IN ('type', 'report_as_of_date');

-- ============================================================
-- T6: BUSINESS NON-NULLS
-- Primary key and business-critical columns declared nullable:false in schema YAML.
-- Parquet is always nullable at format level; this validates business rules.
-- ============================================================
SELECT '=== T6: BUSINESS NON-NULLS (PK COLUMNS) ===' AS section;

-- ccd_districts PK: leaid, year
INSERT INTO dq_results
SELECT 'edu', 'ccd_districts', 'pk_nulls',
  CASE WHEN null_leaid + null_year > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_leaid + null_year AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_leaid > 0 THEN 'leaid:' || null_leaid ELSE NULL END,
    CASE WHEN null_year  > 0 THEN 'year:'  || null_year  ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN leaid IS NULL THEN 1 ELSE 0 END) AS null_leaid,
    SUM(CASE WHEN year  IS NULL THEN 1 ELSE 0 END) AS null_year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_districts', allow_moved_paths=true)
);

-- ccd_schools PK: ncessch, year (leaid excluded — BIE schools 9000* legitimately have no leaid)
INSERT INTO dq_results
SELECT 'edu', 'ccd_schools', 'pk_nulls',
  CASE WHEN null_ncessch + null_year > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_ncessch + null_year AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_ncessch > 0 THEN 'ncessch:' || null_ncessch ELSE NULL END,
    CASE WHEN null_year    > 0 THEN 'year:'    || null_year    ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN ncessch IS NULL THEN 1 ELSE 0 END) AS null_ncessch,
    SUM(CASE WHEN year    IS NULL THEN 1 ELSE 0 END) AS null_year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools', allow_moved_paths=true)
);

-- naep_scores PK: jurisdiction, year, subject, grade, variable_type, subgroup_name
INSERT INTO dq_results
SELECT 'edu', 'naep_scores', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'jurisdiction:'   || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'           || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'subject:'        || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'grade:'          || n4 ELSE NULL END,
    CASE WHEN n5 > 0 THEN 'variable_type:'  || n5 ELSE NULL END,
    CASE WHEN n6 > 0 THEN 'subgroup_name:'  || n6 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN jurisdiction  IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year          IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN subject       IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN grade         IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN variable_type IS NULL THEN 1 ELSE 0 END) AS n5,
    SUM(CASE WHEN subgroup_name IS NULL THEN 1 ELSE 0 END) AS n6,
    SUM(CASE WHEN jurisdiction IS NULL OR year IS NULL OR subject IS NULL
                              OR grade IS NULL OR variable_type IS NULL
                              OR subgroup_name IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores', allow_moved_paths=true)
);

-- crdc_schools PK: crdc_id, year, crdc_topic
-- crdc_id = ncessch for all records; chronic-absenteeism endpoint historically omitted crdc_id.
-- Transformer now backfills crdc_id from ncessch; existing data may have crdc_id null where ncessch
-- is populated — those rows are identifiable and counted as warn, not fail.
INSERT INTO dq_results
SELECT 'edu', 'crdc_schools', 'pk_nulls',
  CASE WHEN unidentifiable + null_year + null_topic > 0 THEN 'fail'
       WHEN null_crdc_id > 0 THEN 'warn'
       ELSE 'pass' END,
  CAST(null_crdc_id + null_year + null_topic AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_crdc_id   > 0 THEN 'crdc_id (backfill needed):' || null_crdc_id ELSE NULL END,
    CASE WHEN unidentifiable > 0 THEN 'crdc_id+ncessch both null:' || unidentifiable ELSE NULL END,
    CASE WHEN null_year      > 0 THEN 'year:'        || null_year    ELSE NULL END,
    CASE WHEN null_topic     > 0 THEN 'crdc_topic:'  || null_topic   ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN crdc_id IS NULL THEN 1 ELSE 0 END) AS null_crdc_id,
    SUM(CASE WHEN crdc_id IS NULL AND ncessch IS NULL THEN 1 ELSE 0 END) AS unidentifiable,
    SUM(CASE WHEN year       IS NULL THEN 1 ELSE 0 END) AS null_year,
    SUM(CASE WHEN crdc_topic IS NULL THEN 1 ELSE 0 END) AS null_topic
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools', allow_moved_paths=true)
);

-- ipeds_institutions PK: unitid, year
INSERT INTO dq_results
SELECT 'edu', 'ipeds_institutions', 'pk_nulls',
  CASE WHEN null_unitid + null_year > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_unitid + null_year AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_unitid > 0 THEN 'unitid:' || null_unitid ELSE NULL END,
    CASE WHEN null_year   > 0 THEN 'year:'   || null_year   ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN unitid IS NULL THEN 1 ELSE 0 END) AS null_unitid,
    SUM(CASE WHEN year   IS NULL THEN 1 ELSE 0 END) AS null_year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions', allow_moved_paths=true)
);

-- ipeds_completions PK: unitid, year, cipcode, majornum, award_level (NCES wide format)
INSERT INTO dq_results
SELECT 'edu', 'ipeds_completions', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'unitid:'      || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'        || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'cipcode:'     || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'majornum:'    || n4 ELSE NULL END,
    CASE WHEN n5 > 0 THEN 'award_level:' || n5 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN unitid      IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year        IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN cipcode     IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN majornum    IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN award_level IS NULL THEN 1 ELSE 0 END) AS n5,
    SUM(CASE WHEN unitid IS NULL OR year IS NULL OR cipcode IS NULL
                         OR majornum IS NULL OR award_level IS NULL
                         THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions', allow_moved_paths=true)
);

-- ipeds_tuition PK: unitid, year, level_of_study, tuition_type
INSERT INTO dq_results
SELECT 'edu', 'ipeds_tuition', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'unitid:'         || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'           || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'level_of_study:' || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'tuition_type:'   || n4 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN unitid         IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year           IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN level_of_study IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN tuition_type   IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN unitid IS NULL OR year IS NULL
                         OR level_of_study IS NULL OR tuition_type IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition', allow_moved_paths=true)
);

-- ipeds_financials PK: unitid, year, form_type; is_provisional also non-null
INSERT INTO dq_results
SELECT 'edu', 'ipeds_financials', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'unitid:'        || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'          || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'form_type:'     || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'is_provisional:'|| n4 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN unitid        IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year          IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN form_type     IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN is_provisional IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN unitid IS NULL OR year IS NULL
                         OR form_type IS NULL OR is_provisional IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials', allow_moved_paths=true)
);

-- ipeds_fall_enrollment PK: unitid, year, efalevel (no nulls, no duplicates)
INSERT INTO dq_results
SELECT 'edu', 'ipeds_fall_enrollment', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', ''
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_fall_enrollment', allow_moved_paths=true)
      WHERE unitid IS NULL OR year IS NULL OR efalevel IS NULL);

INSERT INTO dq_results
SELECT 'edu', 'ipeds_fall_enrollment', 'pk_duplicates',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', ''
FROM (SELECT COUNT(*) AS n FROM (
        SELECT unitid, year, efalevel FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_fall_enrollment', allow_moved_paths=true)
        GROUP BY 1, 2, 3 HAVING COUNT(*) > 1));

-- ipeds_fall_enrollment: nonresident count never exceeds the grand total, and gender
-- columns sum to at most the total (another-gender/unknown columns are not loaded)
INSERT INTO dq_results
SELECT 'edu', 'ipeds_fall_enrollment', 'count_consistency',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', ''
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_fall_enrollment', allow_moved_paths=true)
      WHERE efnralt > eftotlt OR eftotlm + eftotlw > eftotlt OR eftotlt < 0);

-- ipeds_fall_enrollment: efalevel domain
INSERT INTO dq_results
SELECT 'edu', 'ipeds_fall_enrollment', 'attendance_level_values',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', ''
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_fall_enrollment', allow_moved_paths=true)
      WHERE attendance NOT IN ('all', 'full_time', 'part_time')
         OR level_of_study NOT IN ('all', 'undergraduate', 'graduate')
         OR efalevel_label IS NULL);

-- ipeds_admissions PK: unitid, year
INSERT INTO dq_results
SELECT 'edu', 'ipeds_admissions', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', ''
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_admissions', allow_moved_paths=true)
      WHERE unitid IS NULL OR year IS NULL);

INSERT INTO dq_results
SELECT 'edu', 'ipeds_admissions', 'pk_duplicates',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', ''
FROM (SELECT COUNT(*) AS n FROM (
        SELECT unitid, year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_admissions', allow_moved_paths=true)
        GROUP BY 1, 2 HAVING COUNT(*) > 1));

-- ipeds_admissions: funnel ordering (admitted <= applicants, enrolled <= admitted), rates in [0,1]
INSERT INTO dq_results
SELECT 'edu', 'ipeds_admissions', 'funnel_consistency',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', ''
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_admissions', allow_moved_paths=true)
      WHERE admssn > applcn OR enrlt > admssn OR admit_rate NOT BETWEEN 0 AND 1
         OR yield_rate NOT BETWEEN 0 AND 1);

-- college_scorecard PK: id, year
INSERT INTO dq_results
SELECT 'edu', 'college_scorecard', 'pk_nulls',
  CASE WHEN null_id + null_year > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_id + null_year AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_id   > 0 THEN 'id:'   || null_id   ELSE NULL END,
    CASE WHEN null_year > 0 THEN 'year:' || null_year ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN id   IS NULL THEN 1 ELSE 0 END) AS null_id,
    SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard', allow_moved_paths=true)
);

-- naep_achievement_levels PK: jurisdiction, year, subject, grade, variable_type, subgroup_name, level
INSERT INTO dq_results
SELECT 'edu', 'naep_achievement_levels', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'jurisdiction:'   || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'           || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'subject:'        || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'grade:'          || n4 ELSE NULL END,
    CASE WHEN n5 > 0 THEN 'variable_type:'  || n5 ELSE NULL END,
    CASE WHEN n6 > 0 THEN 'subgroup_name:'  || n6 ELSE NULL END,
    CASE WHEN n7 > 0 THEN 'level:'          || n7 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN jurisdiction  IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year          IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN subject       IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN grade         IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN variable_type IS NULL THEN 1 ELSE 0 END) AS n5,
    SUM(CASE WHEN subgroup_name IS NULL THEN 1 ELSE 0 END) AS n6,
    SUM(CASE WHEN level         IS NULL THEN 1 ELSE 0 END) AS n7,
    SUM(CASE WHEN jurisdiction IS NULL OR year IS NULL OR subject IS NULL
                              OR grade IS NULL OR variable_type IS NULL
                              OR subgroup_name IS NULL OR level IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels', allow_moved_paths=true)
);

-- college_scorecard_programs PK: unit_id, year, cip_code, credential_level
INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_programs', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'unit_id:'          || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'              || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'cip_code:'          || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'credential_level:'  || n4 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN unit_id          IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year             IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN cip_code         IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN credential_level IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN unit_id IS NULL OR year IS NULL
                          OR cip_code IS NULL OR credential_level IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true)
);

-- library_outlets PK: year, state, fscs_key, fscs_seq (NOT library_id — see table comment
-- in edu-schema.yaml: library_id collides across distinct library systems in the source data)
INSERT INTO dq_results
SELECT 'edu', 'library_outlets', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'year:'      || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'state:'     || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'fscs_key:'  || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'fscs_seq:'  || n4 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN year     IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN state    IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN fscs_key IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN fscs_seq IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN year IS NULL OR state IS NULL
                      OR fscs_key IS NULL OR fscs_seq IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/library_outlets', allow_moved_paths=true)
);

-- library_outlets PK duplicate check (regression guard for the library_id-collision fix —
-- (year, state, fscs_key, fscs_seq) must be unique even though library_id is not)
INSERT INTO dq_results
SELECT 'edu', 'library_outlets', 'pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  CAST(n AS VARCHAR), '0', 'Duplicate (year, state, fscs_key, fscs_seq) rows'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT year, state, fscs_key, fscs_seq, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/library_outlets', allow_moved_paths=true)
    GROUP BY year, state, fscs_key, fscs_seq HAVING COUNT(*) > 1
  )
);

-- f33_district_finance PK: leaid, year
INSERT INTO dq_results
SELECT 'edu', 'f33_district_finance', 'pk_nulls',
  CASE WHEN null_leaid + null_year > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_leaid + null_year AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_leaid > 0 THEN 'leaid:' || null_leaid ELSE NULL END,
    CASE WHEN null_year  > 0 THEN 'year:'  || null_year  ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN leaid IS NULL THEN 1 ELSE 0 END) AS null_leaid,
    SUM(CASE WHEN year  IS NULL THEN 1 ELSE 0 END) AS null_year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/f33_district_finance', allow_moved_paths=true)
);

-- f33_district_finance: leaid must be unique per year (one row per LEA per fiscal year)
INSERT INTO dq_results
SELECT 'edu', 'f33_district_finance', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (
  SELECT leaid, year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/f33_district_finance', allow_moved_paths=true)
  GROUP BY leaid, year HAVING COUNT(*) > 1
);

-- district_special_ed_counts PK: leaid, year
INSERT INTO dq_results
SELECT 'edu', 'district_special_ed_counts', 'pk_nulls',
  CASE WHEN null_leaid + null_year > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_leaid + null_year AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_leaid > 0 THEN 'leaid:' || null_leaid ELSE NULL END,
    CASE WHEN null_year  > 0 THEN 'year:'  || null_year  ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN leaid IS NULL THEN 1 ELSE 0 END) AS null_leaid,
    SUM(CASE WHEN year  IS NULL THEN 1 ELSE 0 END) AS null_year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_special_ed_counts', allow_moved_paths=true)
);

-- district_special_ed_counts: one row per LEA per school year
INSERT INTO dq_results
SELECT 'edu', 'district_special_ed_counts', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (
  SELECT leaid, year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_special_ed_counts', allow_moved_paths=true)
  GROUP BY leaid, year HAVING COUNT(*) > 1
);

-- district_special_ed_counts: the -8/-9 suppression codes must have become null, and the
-- NCES LEA ID must be 7 digits (the 2021-22 source file drops the leading zero).
INSERT INTO dq_results
SELECT 'edu', 'district_special_ed_counts', 'suppression_or_id_anomaly',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(
    CASE WHEN total_children < 0 OR school_age_all_disabilities < 0
       OR early_childhood_all_disabilities < 0 OR length(leaid) <> 7
    THEN 1 ELSE 0 END
  ) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_special_ed_counts', allow_moved_paths=true)
);

-- district_el_enrollment PK: leaid, year (one row per LEA per CRDC collection)
INSERT INTO dq_results
SELECT 'edu', 'district_el_enrollment', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(CASE WHEN leaid IS NULL OR year IS NULL OR length(leaid) <> 7 THEN 1 ELSE 0 END) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_el_enrollment', allow_moved_paths=true)
);

INSERT INTO dq_results
SELECT 'edu', 'district_el_enrollment', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (
  SELECT leaid, year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_el_enrollment', allow_moved_paths=true)
  GROUP BY leaid, year HAVING COUNT(*) > 1
);

-- district_assessments PK: leaid, year, grade
INSERT INTO dq_results
SELECT 'edu', 'district_assessments', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'leaid:' || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'  || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'grade:' || n3 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN leaid IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year  IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN grade IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN leaid IS NULL OR year IS NULL OR grade IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_assessments', allow_moved_paths=true)
);

-- district_assessments: leaid must be unique per (year, grade)
INSERT INTO dq_results
SELECT 'edu', 'district_assessments', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (
  SELECT leaid, year, grade
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_assessments', allow_moved_paths=true)
  GROUP BY leaid, year, grade HAVING COUNT(*) > 1
);

-- fsa_pslf_discharge_summary PK: section, program, metric_name (report_as_of_date and
-- metric_unit are also declared nullable:false in edu-schema.yaml, folded into this check)
INSERT INTO dq_results
SELECT 'edu', 'fsa_pslf_discharge_summary', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'report_as_of_date:' || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'section:'           || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'program:'           || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'metric_name:'       || n4 ELSE NULL END,
    CASE WHEN n5 > 0 THEN 'metric_unit:'       || n5 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN report_as_of_date IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN section           IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN program           IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN metric_name       IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN metric_unit       IS NULL THEN 1 ELSE 0 END) AS n5,
    SUM(CASE WHEN report_as_of_date IS NULL OR section IS NULL OR program IS NULL
                                  OR metric_name IS NULL OR metric_unit IS NULL
             THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_pslf_discharge_summary', allow_moved_paths=true)
);

-- fsa_pslf_discharge_summary: (section, program, metric_name) must be unique
INSERT INTO dq_results
SELECT 'edu', 'fsa_pslf_discharge_summary', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (
  SELECT section, program, metric_name
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_pslf_discharge_summary', allow_moved_paths=true)
  GROUP BY section, program, metric_name HAVING COUNT(*) > 1
);

-- ============================================================
-- T7: EXPECTED VALUE DISTRIBUTIONS
-- Dimension columns must fall within known enumerated sets.
-- Also catches impossible numeric values (negative enrollment, etc.)
-- ============================================================
SELECT '=== T7: EXPECTED VALUE DISTRIBUTIONS ===' AS section;

-- naep_scores: subject must be MAT or RED
INSERT INTO dq_results
SELECT 'edu', 'naep_scores', 'subject_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct subjects: ' || vals
FROM (
  SELECT SUM(CASE WHEN subject NOT IN ('MAT','RED') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT subject, ', ' ORDER BY subject) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores', allow_moved_paths=true)
);

-- naep_scores: grade must be 4 or 8
INSERT INTO dq_results
SELECT 'edu', 'naep_scores', 'grade_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct grades: ' || vals
FROM (
  SELECT SUM(CASE WHEN grade NOT IN (4, 8) THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(grade AS VARCHAR), ', ' ORDER BY CAST(grade AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores', allow_moved_paths=true)
);

-- naep_scores: year cadence {2013,2015,2017,2019,2022,2024}
INSERT INTO dq_results
SELECT 'edu', 'naep_scores', 'year_cadence',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct years: ' || vals
FROM (
  SELECT SUM(CASE WHEN year NOT IN (2013,2015,2017,2019,2022,2024) THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(year AS VARCHAR), ', ' ORDER BY CAST(year AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores', allow_moved_paths=true)
);

-- naep_achievement_levels: level must be one of 4 known values
INSERT INTO dq_results
SELECT 'edu', 'naep_achievement_levels', 'level_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct levels: ' || vals
FROM (
  SELECT SUM(CASE WHEN level NOT IN ('below_basic','basic','proficient','advanced') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT level, ', ' ORDER BY level) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels', allow_moved_paths=true)
);

-- naep_achievement_levels: subject must be MAT or RED
INSERT INTO dq_results
SELECT 'edu', 'naep_achievement_levels', 'subject_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct subjects: ' || vals
FROM (
  SELECT SUM(CASE WHEN subject NOT IN ('MAT','RED') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT subject, ', ' ORDER BY subject) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels', allow_moved_paths=true)
);

-- naep_achievement_levels: grade must be 4 or 8
INSERT INTO dq_results
SELECT 'edu', 'naep_achievement_levels', 'grade_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct grades: ' || vals
FROM (
  SELECT SUM(CASE WHEN grade NOT IN (4, 8) THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(grade AS VARCHAR), ', ' ORDER BY CAST(grade AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels', allow_moved_paths=true)
);

-- naep_achievement_levels: year cadence {2013,2015,2017,2019,2022,2024}
INSERT INTO dq_results
SELECT 'edu', 'naep_achievement_levels', 'year_cadence',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct years: ' || vals
FROM (
  SELECT SUM(CASE WHEN year NOT IN (2013,2015,2017,2019,2022,2024) THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(year AS VARCHAR), ', ' ORDER BY CAST(year AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels', allow_moved_paths=true)
);

-- crdc_schools: crdc_topic must be one of 4 known values
INSERT INTO dq_results
SELECT 'edu', 'crdc_schools', 'crdc_topic_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct topics: ' || vals
FROM (
  SELECT SUM(CASE WHEN crdc_topic NOT IN ('directory','chronic-absenteeism','offenses','teachers-staff') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT crdc_topic, ', ' ORDER BY crdc_topic) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools', allow_moved_paths=true)
);

-- crdc_schools: year cadence {2013,2015,2017,2020,2021,2022}
-- CRDC is biennial; Urban Institute API used start-year through 2017-18 (years 2013,2015,2017)
-- then switched to end-year from 2019-20 onward (years 2020,2022).
-- 2021 = 2020-21 survey cycle, published separately by OCR.
INSERT INTO dq_results
SELECT 'edu', 'crdc_schools', 'year_cadence',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct years: ' || vals
FROM (
  SELECT SUM(CASE WHEN year NOT IN (2013,2015,2017,2020,2021,2022) THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(year AS VARCHAR), ', ' ORDER BY CAST(year AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools', allow_moved_paths=true)
);

-- ipeds_institutions: sector must be -1–9 (-1 = IPEDS unknown/not applicable)
INSERT INTO dq_results
SELECT 'edu', 'ipeds_institutions', 'sector_range',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct sectors: ' || vals
FROM (
  SELECT SUM(CASE WHEN sector < -1 OR sector > 9 THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(sector AS VARCHAR), ', ' ORDER BY CAST(sector AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions', allow_moved_paths=true)
  WHERE sector IS NOT NULL
);

-- ipeds_completions: award_level codes (IPEDS standard + extended codes)
INSERT INTO dq_results
SELECT 'edu', 'ipeds_completions', 'award_level_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct award_levels: ' || vals
FROM (
  SELECT SUM(CASE WHEN award_level NOT IN (1,2,3,4,5,6,7,8,9,17,18,19,20,21,22,23,24,30,31,32,33) THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(award_level AS VARCHAR), ', ' ORDER BY CAST(award_level AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions', allow_moved_paths=true)
  WHERE award_level IS NOT NULL
);

-- ipeds_completions: ctotalt should be non-negative where present
INSERT INTO dq_results
SELECT 'edu', 'ipeds_completions', 'ctotalt_nonneg',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(CASE WHEN ctotalt < 0 THEN 1 ELSE 0 END) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions', allow_moved_paths=true)
  WHERE ctotalt IS NOT NULL
);

-- ipeds_tuition: level_of_study IN (1,2); tuition_type IN (1,2,3,4)
INSERT INTO dq_results
SELECT 'edu', 'ipeds_tuition', 'dimension_values',
  CASE WHEN bad_lvl + bad_type > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad_lvl + bad_type AS VARCHAR), '0',
  CONCAT_WS('; ',
    CASE WHEN bad_lvl  > 0 THEN 'bad level_of_study:' || bad_lvl  ELSE NULL END,
    CASE WHEN bad_type > 0 THEN 'bad tuition_type:'   || bad_type ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN level_of_study NOT IN (1,2)      THEN 1 ELSE 0 END) AS bad_lvl,
    SUM(CASE WHEN tuition_type   NOT IN (1,2,3,4)  THEN 1 ELSE 0 END) AS bad_type
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition', allow_moved_paths=true)
  WHERE level_of_study IS NOT NULL AND tuition_type IS NOT NULL
);

-- ipeds_tuition: no negative published tuition
INSERT INTO dq_results
SELECT 'edu', 'ipeds_tuition', 'negative_tuition',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT COUNT(*) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition', allow_moved_paths=true)
  WHERE tuition_fees_ft < 0 OR tuition_ft < 0 OR fees_ft < 0 OR credit_hour_charge_pt < 0
    OR tuition_published < 0 OR fees_published < 0 OR tuition_fees_published < 0
);

-- ipeds_financial_aid: all-null / all-same-value / PK nulls / PK duplicates / percent range
INSERT INTO dq_results
SELECT 'edu', 'ipeds_financial_aid', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financial_aid', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_financial_aid', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financial_aid', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type'
  AND column_name NOT IN ('type', 'year', 'frequency', 'latest', 'table_name');

-- ipeds_financial_aid PK: unitid, year
INSERT INTO dq_results
SELECT 'edu', 'ipeds_financial_aid', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'unitid:' || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'   || n2 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN unitid IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year   IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN unitid IS NULL OR year IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financial_aid', allow_moved_paths=true)
);

INSERT INTO dq_results
SELECT 'edu', 'ipeds_financial_aid', 'pk_duplicates',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT unitid, year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financial_aid', allow_moved_paths=true)
    GROUP BY unitid, year HAVING COUNT(*) > 1
  )
);

-- ipeds_financial_aid: *_pct columns are whole-number percents 0-100
INSERT INTO dq_results
SELECT 'edu', 'ipeds_financial_aid', 'pct_range',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(CASE WHEN any_aid_pct NOT BETWEEN 0 AND 100
                    OR any_grant_pct NOT BETWEEN 0 AND 100
                    OR institutional_grant_pct NOT BETWEEN 0 AND 100
                    OR pell_grant_pct NOT BETWEEN 0 AND 100
                    OR any_loan_pct NOT BETWEEN 0 AND 100 THEN 1 ELSE 0 END) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financial_aid', allow_moved_paths=true)
);

-- district_assessments: grade must be 3-12
INSERT INTO dq_results
SELECT 'edu', 'district_assessments', 'grade_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct grades: ' || vals
FROM (
  SELECT SUM(CASE WHEN grade NOT BETWEEN 3 AND 12 THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(grade AS VARCHAR), ', ' ORDER BY CAST(grade AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_assessments', allow_moved_paths=true)
);

-- district_assessments: proficiency percentages must fall in [0,100] once the
-- source's own negative sentinel codes (-1/-2/-3 — see table comment) are excluded;
-- those are documented missingness codes, not out-of-range data.
INSERT INTO dq_results
SELECT 'edu', 'district_assessments', 'pct_prof_range',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(CASE
    WHEN read_test_pct_prof_midpt > 100 OR math_test_pct_prof_midpt > 100
    THEN 1 ELSE 0 END) AS bad
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_assessments', allow_moved_paths=true)
  WHERE read_test_pct_prof_midpt IS NOT NULL OR math_test_pct_prof_midpt IS NOT NULL
);

-- ipeds_financials: form_type must be F1A, F2, or F3
INSERT INTO dq_results
SELECT 'edu', 'ipeds_financials', 'form_type_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct form_types: ' || vals
FROM (
  SELECT SUM(CASE WHEN form_type NOT IN ('F1A','F2','F3') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT form_type, ', ' ORDER BY form_type) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials', allow_moved_paths=true)
  WHERE form_type IS NOT NULL
);

-- ccd_districts: no negative enrollment (excludes NCES sentinels -1,-2,-3,-9 = missing/suppressed)
INSERT INTO dq_results
SELECT 'edu', 'ccd_districts', 'negative_enrollment',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(CASE WHEN enrollment < 0 THEN 1 ELSE 0 END) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_districts', allow_moved_paths=true)
  WHERE enrollment IS NOT NULL AND enrollment NOT IN (-1, -2, -3, -9)
);

-- ccd_schools: no negative enrollment (excludes NCES sentinels -1,-2,-3,-9 = missing/suppressed)
INSERT INTO dq_results
SELECT 'edu', 'ccd_schools', 'negative_enrollment',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(CASE WHEN enrollment < 0 THEN 1 ELSE 0 END) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools', allow_moved_paths=true)
  WHERE enrollment IS NOT NULL AND enrollment NOT IN (-1, -2, -3, -9)
);

-- library_outlets: outlet_type must be one of the four documented PLS codes, plus the
-- single confirmed source anomaly 'MO' (library_id='MO0004-037' "Library By Mail",
-- year=2019 only — verbatim in IMLS's own FY2019 C_OUT_TY column, not a mapping error
-- on our side; see the outlet_type column comment in edu-schema.yaml for detail).
INSERT INTO dq_results
SELECT 'edu', 'library_outlets', 'outlet_type_distribution',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0', vals
FROM (
  SELECT SUM(CASE WHEN outlet_type NOT IN ('CE','BR','BS','BM','MO') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT outlet_type, ', ' ORDER BY outlet_type) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/library_outlets', allow_moved_paths=true)
  WHERE outlet_type IS NOT NULL
);

-- library_outlets: no negative square_feet (the transformer already nulls NCES sentinels
-- -1/-3/-4; any negative value reaching Iceberg is a transformer regression, not source data)
INSERT INTO dq_results
SELECT 'edu', 'library_outlets', 'negative_square_feet',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(CASE WHEN square_feet < 0 THEN 1 ELSE 0 END) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/library_outlets', allow_moved_paths=true)
  WHERE square_feet IS NOT NULL
);

-- f33_district_finance: no negative revenue/expenditure or enrollment. Unlike ccd_districts,
-- this table does not null out NCES sentinel codes (verified live FY2023/FY2024: none observed
-- in TOTALREV/TOTALEXP/TCURSPND/ENROLL) — any negative value here is a real anomaly, not a
-- known sentinel to exclude.
INSERT INTO dq_results
SELECT 'edu', 'f33_district_finance', 'negative_dollar_or_enrollment',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(
    CASE WHEN total_revenue_thousand < 0
       OR total_expenditure_thousand < 0
       OR current_expenditure_thousand < 0
       OR enrollment < 0
    THEN 1 ELSE 0 END
  ) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/f33_district_finance', allow_moved_paths=true)
);

-- f33_district_finance payroll columns (Z32/Z34): never negative, and the enrollment-weighted
-- national payroll share of current spending sits in a plausible band (published F-33 national
-- figures put salaries+benefits at roughly 80% of current spending).
INSERT INTO dq_results
SELECT 'edu', 'f33_district_finance', 'payroll_value_ranges',
  CASE WHEN neg > 0 OR share IS NULL OR share < 0.6 OR share > 0.95 THEN 'fail' ELSE 'pass' END,
  CAST(neg AS VARCHAR) || ' negative; national payroll share ' || COALESCE(CAST(ROUND(share, 3) AS VARCHAR), 'NULL'),
  '0 negative; share 0.6-0.95', NULL
FROM (
  SELECT
    SUM(CASE WHEN total_salaries_wages_thousand < 0 OR employee_benefits_thousand < 0
               OR instruction_salaries_thousand < 0 OR instruction_employee_benefits_thousand < 0
               OR per_pupil_salaries_wages < 0 OR per_pupil_employee_benefits < 0
        THEN 1 ELSE 0 END) AS neg,
    SUM(total_salaries_wages_thousand + employee_benefits_thousand)
      / NULLIF(SUM(CASE WHEN total_salaries_wages_thousand IS NOT NULL
                          AND employee_benefits_thousand IS NOT NULL
                   THEN current_expenditure_thousand END), 0) AS share
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/f33_district_finance', allow_moved_paths=true)
);

-- district_el_enrollment: counts are never negative (the CRDC's negative non-reported codes are
-- dropped), EL never exceeds total enrollment, and reporting schools never exceed schools.
INSERT INTO dq_results
SELECT 'edu', 'district_el_enrollment', 'value_ranges',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(
    CASE WHEN el_enrollment < 0 OR total_enrollment < 0 OR el_program_enrollment < 0
       OR el_enrollment > total_enrollment
       OR schools_reporting_el > schools OR schools_reporting_el < 0
    THEN 1 ELSE 0 END
  ) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_el_enrollment', allow_moved_paths=true)
);

-- district_el_enrollment: Texas 2021-22 reconciles to the CRDC source file (8,924 school rows in
-- 1,203 LEAs summing to 1,107,116 EL students; verified live 2026-11-15).
INSERT INTO dq_results
SELECT 'edu', 'district_el_enrollment', 'texas_2021_reconciles',
  CASE WHEN leas = 1203 AND schools = 8924 AND el = 1107116 THEN 'pass' ELSE 'fail' END,
  leas || '/' || schools || '/' || el, '1203/8924/1107116', NULL
FROM (
  SELECT COUNT(*) AS leas, SUM(schools) AS schools, SUM(el_enrollment) AS el
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/district_el_enrollment', allow_moved_paths=true)
  WHERE year = 2021 AND state_abbr = 'TX'
);

-- fsa_pslf_discharge_summary: section must be one of the three combined-report sheets
INSERT INTO dq_results
SELECT 'edu', 'fsa_pslf_discharge_summary', 'section_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct sections: ' || vals
FROM (
  SELECT SUM(CASE WHEN section NOT IN ('application_status','cumulative_portfolio','discharges')
                   THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT section, ', ' ORDER BY section) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_pslf_discharge_summary', allow_moved_paths=true)
);

-- fsa_pslf_discharge_summary: no negative discharge/application counts or dollar amounts
INSERT INTO dq_results
SELECT 'edu', 'fsa_pslf_discharge_summary', 'negative_metric_value',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(CASE WHEN metric_value < 0 THEN 1 ELSE 0 END) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_pslf_discharge_summary', allow_moved_paths=true)
  WHERE metric_value IS NOT NULL
);

-- fsa_loan_portfolio_status: PK (portfolio, fiscal_year, fiscal_quarter, status) non-null and unique
INSERT INTO dq_results
SELECT 'edu', 'fsa_loan_portfolio_status', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_portfolio_status', allow_moved_paths=true)
  WHERE portfolio IS NULL OR fiscal_year IS NULL OR fiscal_quarter IS NULL OR status IS NULL OR period_end_date IS NULL
);

INSERT INTO dq_results
SELECT 'edu', 'fsa_loan_portfolio_status', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END, CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (
  SELECT portfolio, fiscal_year, fiscal_quarter, status FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_portfolio_status', allow_moved_paths=true)
  GROUP BY portfolio, fiscal_year, fiscal_quarter, status HAVING COUNT(*) > 1
);

-- fsa_loan_portfolio_status: quarter in 1-4, no negative amounts, period_end_date consistent with fiscal year/quarter
INSERT INTO dq_results
SELECT 'edu', 'fsa_loan_portfolio_status', 'value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_portfolio_status', allow_moved_paths=true)
  WHERE fiscal_quarter NOT BETWEEN 1 AND 4 OR dollars_outstanding < 0 OR recipients < 0
     OR YEAR(period_end_date) NOT IN (fiscal_year, fiscal_year - 1)
);

-- fsa_loan_delinquency: PK (portfolio, fiscal_year, fiscal_quarter, status) non-null and unique
INSERT INTO dq_results
SELECT 'edu', 'fsa_loan_delinquency', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_delinquency', allow_moved_paths=true)
  WHERE portfolio IS NULL OR fiscal_year IS NULL OR fiscal_quarter IS NULL OR status IS NULL OR period_end_date IS NULL
);

INSERT INTO dq_results
SELECT 'edu', 'fsa_loan_delinquency', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END, CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (
  SELECT portfolio, fiscal_year, fiscal_quarter, status FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_delinquency', allow_moved_paths=true)
  GROUP BY portfolio, fiscal_year, fiscal_quarter, status HAVING COUNT(*) > 1
);

-- fsa_loan_delinquency: quarter in 1-4, no negative amounts, period_end_date consistent with fiscal year/quarter
INSERT INTO dq_results
SELECT 'edu', 'fsa_loan_delinquency', 'value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/fsa_loan_delinquency', allow_moved_paths=true)
  WHERE fiscal_quarter NOT BETWEEN 1 AND 4 OR dollars_outstanding < 0 OR recipients < 0
     OR YEAR(period_end_date) NOT IN (fiscal_year, fiscal_year - 1)
);

-- sevis_student_counts: PK (reporting_month, country_name, breakdown, category) non-null and unique
INSERT INTO dq_results
SELECT 'edu', 'sevis_student_counts', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END, CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/sevis_student_counts', allow_moved_paths=true)
  WHERE reporting_month IS NULL OR country_name IS NULL OR breakdown IS NULL OR category IS NULL OR students IS NULL
);

INSERT INTO dq_results
SELECT 'edu', 'sevis_student_counts', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END, CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (
  SELECT reporting_month, country_name, breakdown, category FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/sevis_student_counts', allow_moved_paths=true)
  GROUP BY reporting_month, country_name, breakdown, category HAVING COUNT(*) > 1
);

-- sevis_student_counts: counts positive, breakdown from the known set, month is the 1st
INSERT INTO dq_results
SELECT 'edu', 'sevis_student_counts', 'value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/sevis_student_counts', allow_moved_paths=true)
  WHERE students <= 0 OR DAY(reporting_month) <> 1
     OR breakdown NOT IN ('total','sex','education_level','state','stem_sex','stem_education_level')
);

-- sevis_student_counts: latest month's national total is plausible (source ~1.19M in Oct 2026)
INSERT INTO dq_results
SELECT 'edu', 'sevis_student_counts', 'latest_total_plausible',
  CASE WHEN n BETWEEN 800000 AND 1800000 THEN 'pass' ELSE 'fail' END, CAST(n AS VARCHAR), '800000-1800000', NULL
FROM (
  SELECT COALESCE(SUM(students), 0) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/sevis_student_counts', allow_moved_paths=true)
  WHERE breakdown = 'total'
    AND reporting_month = (SELECT MAX(reporting_month) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/sevis_student_counts', allow_moved_paths=true))
);

-- sevis_student_counts: China and India are the two largest origins in the latest month
INSERT INTO dq_results
SELECT 'edu', 'sevis_student_counts', 'top_origins',
  CASE WHEN COUNT(*) = 2 THEN 'pass' ELSE 'fail' END, CAST(COUNT(*) AS VARCHAR), '2', NULL
FROM (
  SELECT country_name FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/sevis_student_counts', allow_moved_paths=true)
  WHERE breakdown = 'total'
    AND reporting_month = (SELECT MAX(reporting_month) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/sevis_student_counts', allow_moved_paths=true))
  ORDER BY students DESC LIMIT 2
) WHERE country_name IN ('CHINA', 'INDIA');

-- edfacts_chronic_absenteeism: no column may be entirely null. enrolled / percent_numerator /
-- rate_pct are populated only on all-students rows, so they are non-null overall.
INSERT INTO dq_results
SELECT 'edu', 'edfacts_chronic_absenteeism', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/edfacts_chronic_absenteeism', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'year');

-- edfacts_chronic_absenteeism PK: year, geo_level, state_name, leaid (district rows), subgroup_code, sex
INSERT INTO dq_results
SELECT 'edu', 'edfacts_chronic_absenteeism', 'pk_nulls',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(
    CASE WHEN year IS NULL OR geo_level IS NULL OR state_name IS NULL OR subgroup_code IS NULL
       OR sex IS NULL OR (geo_level = 'district' AND leaid IS NULL)
       OR (geo_level <> 'district' AND leaid IS NOT NULL)
    THEN 1 ELSE 0 END
  ) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/edfacts_chronic_absenteeism', allow_moved_paths=true)
);

INSERT INTO dq_results
SELECT 'edu', 'edfacts_chronic_absenteeism', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (
  SELECT year, geo_level, state_name, leaid, subgroup_code, sex
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/edfacts_chronic_absenteeism', allow_moved_paths=true)
  GROUP BY year, geo_level, state_name, leaid, subgroup_code, sex HAVING COUNT(*) > 1
);

-- edfacts_chronic_absenteeism: counts never negative, rate within 0-100, a rate never carried by
-- a sex split, and a 7-digit LEA ID on every district row.
INSERT INTO dq_results
SELECT 'edu', 'edfacts_chronic_absenteeism', 'value_ranges',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(
    CASE WHEN students_chronically_absent < 0 OR enrolled < 0 OR percent_numerator < 0
       OR rate_pct < 0 OR rate_pct > 100
       OR (rate_pct IS NOT NULL AND sex <> 'all')
       OR (geo_level = 'district' AND length(leaid) <> 7)
    THEN 1 ELSE 0 END
  ) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/edfacts_chronic_absenteeism', allow_moved_paths=true)
);

-- edfacts_chronic_absenteeism: 2022-23 national all-students rate is 27.8 percent (13,406,900 of
-- 48,268,200) and Oregon's 2022-23 state rate is 43.8 percent, as published by ED Data Express
-- (verified live 2026-11-21).
INSERT INTO dq_results
SELECT 'edu', 'edfacts_chronic_absenteeism', 'known_values',
  CASE WHEN COUNT(*) = 2 THEN 'pass' ELSE 'fail' END,
  CAST(COUNT(*) AS VARCHAR), '2', NULL
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/edfacts_chronic_absenteeism', allow_moved_paths=true)
WHERE year = 2022 AND sex = 'all'
  AND ((geo_level = 'national' AND enrolled = 48268200 AND percent_numerator = 13406900 AND rate_pct = 27.8)
    OR (geo_level = 'state' AND state_name = 'OREGON' AND subgroup_code = 'ALLSEA' AND rate_pct = 43.8));

-- college_scorecard_latest: latest-snapshot table, PK id
INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_latest', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_latest', allow_moved_paths=true))
WHERE null_percentage = 100.0 AND column_name <> 'type';

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_latest', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_latest', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0 AND column_name <> 'type';

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_latest', 'pk_nulls',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_latest', allow_moved_paths=true)
WHERE id IS NULL;

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_latest', 'pk_duplicates',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM (SELECT id FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_latest', allow_moved_paths=true) GROUP BY id HAVING COUNT(*) > 1);

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_latest', 'value_ranges',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', NULL
FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_latest', allow_moved_paths=true)
WHERE ownership NOT IN (1, 2, 3)
  OR size < 0
  OR pell_grant_rate NOT BETWEEN 0 AND 1
  OR completion_rate_4yr NOT BETWEEN 0 AND 1
  OR completion_rate_2yr NOT BETWEEN 0 AND 1
  OR retention_rate_fulltime NOT BETWEEN 0 AND 1
  OR default_rate_3yr NOT BETWEEN 0 AND 1
  OR mean_earnings_10yr NOT BETWEEN 1000 AND 1000000
  OR median_earnings_8yr NOT BETWEEN 1000 AND 1000000
  OR median_family_income < 0
  OR median_debt_completers < 0
  OR tuition_in_state < 0
  OR tuition_out_state < 0;

-- The table exists to hold the latest earnings and family income: most institutions that
-- report anything report these, so a mostly-null column means a field mapping broke
INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_latest', 'latest_measures_populated',
  CASE WHEN LEAST(n_earn, n_income, n_size) >= 3000 THEN 'pass' ELSE 'fail' END,
  CONCAT_WS(', ', 'mean_earnings_10yr:' || n_earn, 'median_family_income:' || n_income,
    'size:' || n_size),
  '>=3000 each', NULL
FROM (
  SELECT COUNT(mean_earnings_10yr) AS n_earn, COUNT(median_family_income) AS n_income,
    COUNT(size) AS n_size
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_latest', allow_moved_paths=true)
);

-- ============================================================
-- ALL RESULTS
-- ============================================================
SELECT '=== ALL DQ RESULTS ===' AS section;
SELECT * FROM dq_results ORDER BY tbl, test;

-- ============================================================
-- PASS/FAIL SUMMARY PER TABLE
-- ============================================================
SELECT '=== TABLE SUMMARY ===' AS section;

SELECT
  schema,
  tbl AS table_name,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS fails,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) AS warns,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS passes,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END AS overall
FROM dq_results
GROUP BY schema, tbl
ORDER BY overall DESC, tbl;

-- ============================================================
-- SCHEMA-LEVEL PASS/FAIL
-- ============================================================
SELECT '=== SCHEMA SUMMARY ===' AS section;

SELECT
  schema,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) AS total_fails,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) AS total_warns,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0 THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0 THEN 'WARN'
    ELSE 'PASS'
  END AS schema_result
FROM dq_results
GROUP BY schema;
