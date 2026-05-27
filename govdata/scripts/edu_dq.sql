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
-- If the Iceberg metadata is missing, DuckDB aborts here — that IS the signal.
-- ============================================================
SELECT '=== T1: EXISTENCE ===' AS section;

INSERT INTO dq_results
SELECT 'edu', tbl, 'existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  CAST(n AS VARCHAR), '1',
  CASE WHEN n > 0 THEN 'readable and non-empty' ELSE 'accessible but empty' END
FROM (
  SELECT 'ccd_districts'            AS tbl, COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_districts',           allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'ccd_schools',            COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools',            allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'naep_scores',            COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores',            allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'crdc_schools',           COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools',           allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'ipeds_institutions',     COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions',     allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'ipeds_completions',      COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions',      allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'ipeds_tuition',          COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition',          allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'ipeds_financials',       COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials',       allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'college_scorecard',      COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard',      allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'college_scorecard_programs', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'naep_achievement_levels',   COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels',   allow_moved_paths=true) LIMIT 1)
);

-- ============================================================
-- T2: ROW COUNTS
-- Thresholds set conservatively to catch zero/near-zero tables.
-- ============================================================
SELECT '=== T2: ROW COUNTS ===' AS section;

-- Tables with releaseWindow constraints are legitimately empty outside their ingest window.
-- always_populated: ccd_schools, college_scorecard, college_scorecard_programs, ipeds_institutions → fail when empty
-- release_window:   all others → warn when empty (expected during off-window periods)
INSERT INTO dq_results
SELECT 'edu', tbl, 'row_count',
  CASE
    WHEN n >= thresh                     THEN 'pass'
    WHEN tbl IN ('ccd_schools','college_scorecard','college_scorecard_programs','ipeds_institutions') THEN 'fail'
    ELSE 'warn'
  END,
  CAST(n AS VARCHAR), CAST(thresh AS VARCHAR), NULL
FROM (
  SELECT 'ccd_districts'            AS tbl, COUNT(*) AS n, 100 AS thresh FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_districts',           allow_moved_paths=true)
  UNION ALL SELECT 'ccd_schools',                          COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools',            allow_moved_paths=true)
  UNION ALL SELECT 'naep_scores',                          COUNT(*),    10       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores',            allow_moved_paths=true)
  UNION ALL SELECT 'crdc_schools',                         COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools',           allow_moved_paths=true)
  UNION ALL SELECT 'ipeds_institutions',                   COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions',     allow_moved_paths=true)
  UNION ALL SELECT 'ipeds_completions',                    COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions',      allow_moved_paths=true)
  UNION ALL SELECT 'ipeds_tuition',                        COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition',          allow_moved_paths=true)
  UNION ALL SELECT 'ipeds_financials',                     COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials',       allow_moved_paths=true)
  UNION ALL SELECT 'college_scorecard',                    COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard',      allow_moved_paths=true)
  UNION ALL SELECT 'college_scorecard_programs',           COUNT(*),   100       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true)
  UNION ALL SELECT 'naep_achievement_levels',              COUNT(*),    10       FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels',   allow_moved_paths=true)
);

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
SELECT 'college_scorecard'        AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard',      allow_moved_paths=true) LIMIT 1;
SELECT 'college_scorecard_programs' AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true) LIMIT 1;
SELECT 'naep_achievement_levels'   AS tbl, * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels',   allow_moved_paths=true) LIMIT 1;

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
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'naep_scores', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'crdc_schools', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_institutions', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_completions', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_tuition', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_financials', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_programs', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'naep_achievement_levels', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels', allow_moved_paths=true))
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
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'ccd_schools', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ccd_schools', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'naep_scores', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_scores', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  -- variable_type, subgroup_name, is_displayable are constant by design (variable=TOTAL, All students)
  AND column_name NOT IN ('type', 'variable_type', 'subgroup_name', 'is_displayable');

INSERT INTO dq_results
SELECT 'edu', 'crdc_schools', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/crdc_schools', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_institutions', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_institutions', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_completions', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_tuition', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'ipeds_financials', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_financials', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'college_scorecard_programs', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/college_scorecard_programs', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'edu', 'naep_achievement_levels', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/naep_achievement_levels', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  -- variable_type, subgroup_name, is_displayable are constant by design (variable=TOTAL, All students)
  AND column_name NOT IN ('type', 'variable_type', 'subgroup_name', 'is_displayable');

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

-- ipeds_completions PK: unitid, year, cipcode, majornum, race, sex
-- award_level excluded — IPEDS aggregate/total rows use null award_level by convention
INSERT INTO dq_results
SELECT 'edu', 'ipeds_completions', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'unitid:'   || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'     || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'cipcode:'  || n3 ELSE NULL END,
    CASE WHEN n5 > 0 THEN 'majornum:' || n5 ELSE NULL END,
    CASE WHEN n6 > 0 THEN 'race:'     || n6 ELSE NULL END,
    CASE WHEN n7 > 0 THEN 'sex:'      || n7 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN unitid   IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year     IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN cipcode  IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN majornum IS NULL THEN 1 ELSE 0 END) AS n5,
    SUM(CASE WHEN race     IS NULL THEN 1 ELSE 0 END) AS n6,
    SUM(CASE WHEN sex      IS NULL THEN 1 ELSE 0 END) AS n7,
    SUM(CASE WHEN unitid IS NULL OR year IS NULL OR cipcode IS NULL
                         OR majornum IS NULL OR race IS NULL
                         OR sex IS NULL THEN 1 ELSE 0 END) AS total
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
  SELECT SUM(CASE WHEN award_level NOT IN (1,2,3,4,5,6,7,8,9,17,18,19,22,23,24,30,31,32,33) THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(award_level AS VARCHAR), ', ' ORDER BY CAST(award_level AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions', allow_moved_paths=true)
  WHERE award_level IS NOT NULL
);

-- ipeds_completions: race codes 1-9 and 99 (total); sex codes 1,2,99
INSERT INTO dq_results
SELECT 'edu', 'ipeds_completions', 'race_sex_values',
  CASE WHEN bad_race + bad_sex > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad_race + bad_sex AS VARCHAR), '0',
  CONCAT_WS('; ',
    CASE WHEN bad_race > 0 THEN 'bad race rows:' || bad_race ELSE NULL END,
    CASE WHEN bad_sex  > 0 THEN 'bad sex rows:'  || bad_sex  ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN race NOT IN (1,2,3,4,5,6,7,8,9,99) THEN 1 ELSE 0 END) AS bad_race,
    SUM(CASE WHEN sex  NOT IN (1,2,99)                THEN 1 ELSE 0 END) AS bad_sex
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_completions', allow_moved_paths=true)
  WHERE race IS NOT NULL AND sex IS NOT NULL
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
  SELECT SUM(CASE WHEN tuition_fees_ft < 0 THEN 1 ELSE 0 END) AS n
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/edu/ipeds_tuition', allow_moved_paths=true)
  WHERE tuition_fees_ft IS NOT NULL
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
