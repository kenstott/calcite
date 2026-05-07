-- Education Data Quality Check
-- Run: source .env.prod && envsubst < scripts/edu_dq.sql | duckdb

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';
SET s3_url_style='path';

-- ============================================================
-- 1. READABILITY — SELECT 1 ROW FROM EACH TABLE
-- COUNT(*) reads only metadata; a row fetch confirms data files are accessible
-- ============================================================
SELECT '=== READABILITY CHECKS ===' AS section;

SELECT 'ccd_districts'            AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ccd_districts', allow_moved_paths=true) LIMIT 1;
SELECT 'ccd_schools'              AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ccd_schools', allow_moved_paths=true) LIMIT 1;
SELECT 'naep_scores'              AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/naep_scores', allow_moved_paths=true) LIMIT 1;
SELECT 'crdc_schools'             AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/crdc_schools', allow_moved_paths=true) LIMIT 1;
SELECT 'ipeds_institutions'       AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_institutions', allow_moved_paths=true) LIMIT 1;
SELECT 'ipeds_completions'        AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_completions', allow_moved_paths=true) LIMIT 1;
SELECT 'ipeds_tuition'            AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_tuition', allow_moved_paths=true) LIMIT 1;
SELECT 'ipeds_financials'         AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_financials', allow_moved_paths=true) LIMIT 1;
SELECT 'college_scorecard'        AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/college_scorecard', allow_moved_paths=true) LIMIT 1;
SELECT 'college_scorecard_programs' AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/college_scorecard_programs', allow_moved_paths=true) LIMIT 1;

-- ============================================================
-- 2. ROW COUNTS
-- ============================================================
SELECT '=== ROW COUNTS ===' AS section;

SELECT 'ccd_districts'             AS tbl, COUNT(*) AS rows FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ccd_districts', allow_moved_paths=true)
UNION ALL
SELECT 'ccd_schools',                       COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ccd_schools', allow_moved_paths=true)
UNION ALL
SELECT 'naep_scores',                        COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/naep_scores', allow_moved_paths=true)
UNION ALL
SELECT 'crdc_schools',                       COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/crdc_schools', allow_moved_paths=true)
UNION ALL
SELECT 'ipeds_institutions',                 COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_institutions', allow_moved_paths=true)
UNION ALL
SELECT 'ipeds_completions',                  COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_completions', allow_moved_paths=true)
UNION ALL
SELECT 'ipeds_tuition',                      COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_tuition', allow_moved_paths=true)
UNION ALL
SELECT 'ipeds_financials',                   COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_financials', allow_moved_paths=true)
UNION ALL
SELECT 'college_scorecard',                  COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/college_scorecard', allow_moved_paths=true)
UNION ALL
SELECT 'college_scorecard_programs',         COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/college_scorecard_programs', allow_moved_paths=true)
ORDER BY tbl;

-- ============================================================
-- 2. CCD DISTRICTS — year range, key nulls, enrollment
-- ============================================================
SELECT '=== CCD_DISTRICTS: YEAR RANGE + NULLS ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT fips) AS distinct_states,
  SUM(CASE WHEN leaid IS NULL THEN 1 ELSE 0 END) AS null_leaid,
  SUM(CASE WHEN fips IS NULL THEN 1 ELSE 0 END) AS null_fips,
  SUM(CASE WHEN enrollment < 0 THEN 1 ELSE 0 END) AS negative_enrollment
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ccd_districts', allow_moved_paths=true);

-- ============================================================
-- 3. CCD SCHOOLS — year range, key nulls, enrollment
-- ============================================================
SELECT '=== CCD_SCHOOLS: YEAR RANGE + NULLS ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT fips) AS distinct_states,
  SUM(CASE WHEN ncessch IS NULL THEN 1 ELSE 0 END) AS null_ncessch,
  SUM(CASE WHEN leaid IS NULL THEN 1 ELSE 0 END) AS null_leaid,
  SUM(CASE WHEN fips IS NULL THEN 1 ELSE 0 END) AS null_fips,
  SUM(CASE WHEN enrollment < 0 THEN 1 ELSE 0 END) AS negative_enrollment
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ccd_schools', allow_moved_paths=true);

-- ============================================================
-- 4. NAEP SCORES — year range, key nulls, subject coverage
-- ============================================================
SELECT '=== NAEP_SCORES: YEAR RANGE + SUBJECT/GRADE COVERAGE ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT subject) AS distinct_subjects,
  COUNT(DISTINCT grade) AS distinct_grades,
  SUM(CASE WHEN jurisdiction IS NULL THEN 1 ELSE 0 END) AS null_jurisdiction,
  SUM(CASE WHEN subject IS NULL THEN 1 ELSE 0 END) AS null_subject,
  SUM(CASE WHEN grade IS NULL THEN 1 ELSE 0 END) AS null_grade
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/naep_scores', allow_moved_paths=true);

-- ============================================================
-- 5. CRDC SCHOOLS — year range, key nulls, topic coverage
-- ============================================================
SELECT '=== CRDC_SCHOOLS: YEAR RANGE + TOPIC COVERAGE ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT crdc_topic) AS distinct_topics,
  SUM(CASE WHEN crdc_id IS NULL THEN 1 ELSE 0 END) AS null_crdc_id,
  SUM(CASE WHEN ncessch IS NULL THEN 1 ELSE 0 END) AS null_ncessch
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/crdc_schools', allow_moved_paths=true);

-- ============================================================
-- 6. IPEDS INSTITUTIONS — year range, key nulls, state coverage
-- ============================================================
SELECT '=== IPEDS_INSTITUTIONS: YEAR RANGE + NULLS ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT fips) AS distinct_states,
  SUM(CASE WHEN unitid IS NULL THEN 1 ELSE 0 END) AS null_unitid,
  SUM(CASE WHEN fips IS NULL THEN 1 ELSE 0 END) AS null_fips
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_institutions', allow_moved_paths=true);

-- ============================================================
-- 7. IPEDS COMPLETIONS — year range, key nulls
-- ============================================================
SELECT '=== IPEDS_COMPLETIONS: YEAR RANGE + NULLS ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT unitid) AS distinct_institutions,
  SUM(CASE WHEN unitid IS NULL THEN 1 ELSE 0 END) AS null_unitid,
  SUM(CASE WHEN cipcode IS NULL THEN 1 ELSE 0 END) AS null_cipcode
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_completions', allow_moved_paths=true);

-- ============================================================
-- 8. IPEDS TUITION — year range, key nulls, tuition ranges
-- ============================================================
SELECT '=== IPEDS_TUITION: YEAR RANGE + VALUE RANGES ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT unitid) AS distinct_institutions,
  SUM(CASE WHEN unitid IS NULL THEN 1 ELSE 0 END) AS null_unitid,
  SUM(CASE WHEN tuition_fees_ft < 0 THEN 1 ELSE 0 END) AS negative_tuition
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_tuition', allow_moved_paths=true);

-- ============================================================
-- 9. IPEDS FINANCIALS — year range, key nulls, form_type coverage
-- ============================================================
SELECT '=== IPEDS_FINANCIALS: YEAR RANGE + FORM_TYPE COVERAGE ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT form_type) AS distinct_form_types,
  COUNT(DISTINCT unitid) AS distinct_institutions,
  SUM(CASE WHEN unitid IS NULL THEN 1 ELSE 0 END) AS null_unitid
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/ipeds_financials', allow_moved_paths=true);

-- ============================================================
-- 10. COLLEGE SCORECARD — year range, key nulls
-- ============================================================
SELECT '=== COLLEGE_SCORECARD: YEAR RANGE + NULLS ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT id) AS distinct_institutions,
  SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_id,
  SUM(CASE WHEN ope8_id IS NULL THEN 1 ELSE 0 END) AS null_ope8_id
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/college_scorecard', allow_moved_paths=true);

-- ============================================================
-- 11. COLLEGE SCORECARD PROGRAMS — year range, key nulls, CIP coverage
-- ============================================================
SELECT '=== COLLEGE_SCORECARD_PROGRAMS: YEAR RANGE + NULLS ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(year) AS min_year, MAX(year) AS max_year,
  COUNT(DISTINCT unit_id) AS distinct_institutions,
  COUNT(DISTINCT cip_code) AS distinct_cip_codes,
  SUM(CASE WHEN unit_id IS NULL THEN 1 ELSE 0 END) AS null_unit_id,
  SUM(CASE WHEN cip_code IS NULL THEN 1 ELSE 0 END) AS null_cip_code
FROM iceberg_scan('s3://govdata-parquet-v1/source=edu/edu/college_scorecard_programs', allow_moved_paths=true);
