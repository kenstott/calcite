-- U.S. Patents and Trademarks Data Quality Checks
-- Schema: patents
-- Tables: patent_grants, patent_assignees, patent_inventors, patent_cpc_classes,
--         patent_claims, patent_summaries, trademark_applications
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition columns 'type' and 'year' for all tables.
-- Both workers verified: historical (2025) and daily (2026).

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs;  LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';
SET s3_url_style='path';
SET unsafe_enable_version_guessing=true;

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
-- TABLE: patent_grants
-- ─────────────────────────────────────────────────────────────

-- T0: readability
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true) LIMIT 1;

-- T1: existence
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true));

-- T2: row_count (~300k+ grants/year)
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T2_row_count',
  CASE WHEN n >= 50000 THEN 'pass' ELSE 'fail' END,
  n, 50000, 'Expected at least 50000 patent grant records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true));

-- T2b: historical coverage (historical worker writes 2025)
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T2b_historical_coverage',
  CASE WHEN min_year <= 2025 THEN 'pass' ELSE 'fail' END,
  min_year, 2025, 'MIN(grant_year) must be <= 2025 (historical worker)'
FROM (SELECT MIN(grant_year) AS min_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true));

-- T2c: daily coverage (daily worker writes 2026)
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T2c_daily_coverage',
  CASE WHEN max_year >= 2026 THEN 'pass' ELSE 'fail' END,
  max_year, 2026, 'MAX(grant_year) must be >= 2026 (daily worker)'
FROM (SELECT MAX(grant_year) AS max_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (patent_id, grant_year NOT NULL)
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL patent_id or grant_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true)
      WHERE patent_id IS NULL OR grant_year IS NULL);

-- T7: patent_type values (utility, design, plant)
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T7_patent_type_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with patent_type outside known set (utility, design, plant)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true)
      WHERE patent_type IS NOT NULL AND patent_type NOT IN ('utility', 'design', 'plant'));

-- T7: patent_date format (YYYY-MM-DD)
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T7_patent_date_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'patent_date not matching YYYY-MM-DD format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true)
      WHERE patent_date IS NOT NULL
        AND NOT REGEXP_MATCHES(patent_date, '^\d{4}-\d{2}-\d{2}$'));

-- T7: distinct grant years present (at least 2: 2025 and 2026)
INSERT INTO dq_results
SELECT 'patents', 'patent_grants', 'T7_grant_year_coverage',
  CASE WHEN n >= 2 THEN 'pass' ELSE 'fail' END,
  n, 2, 'Distinct grant_year values (expect >= 2: 2025 historical + 2026 daily)'
FROM (SELECT COUNT(DISTINCT grant_year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_grants', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: patent_assignees
-- ─────────────────────────────────────────────────────────────

-- T0: readability
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true) LIMIT 1;

-- T1: existence
INSERT INTO dq_results
SELECT 'patents', 'patent_assignees', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true));

-- T2: row_count
INSERT INTO dq_results
SELECT 'patents', 'patent_assignees', 'T2_row_count',
  CASE WHEN n >= 50000 THEN 'pass' ELSE 'fail' END,
  n, 50000, 'Expected at least 50000 patent assignee records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true));

-- T2b: historical coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_assignees', 'T2b_historical_coverage',
  CASE WHEN min_year <= 2025 THEN 'pass' ELSE 'fail' END,
  min_year, 2025, 'MIN(grant_year) must be <= 2025 (historical worker)'
FROM (SELECT MIN(grant_year) AS min_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true));

-- T2c: daily coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_assignees', 'T2c_daily_coverage',
  CASE WHEN max_year >= 2026 THEN 'pass' ELSE 'fail' END,
  max_year, 2026, 'MAX(grant_year) must be >= 2026 (daily worker)'
FROM (SELECT MAX(grant_year) AS max_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'patents', 'patent_assignees', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'patents', 'patent_assignees', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (patent_id, grant_year NOT NULL)
INSERT INTO dq_results
SELECT 'patents', 'patent_assignees', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL patent_id or grant_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true)
      WHERE patent_id IS NULL OR grant_year IS NULL);

-- T7: assignee_type values (PatentsView numeric codes: 1-9)
INSERT INTO dq_results
SELECT 'patents', 'patent_assignees', 'T7_assignee_type_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with assignee_type outside known PatentsView codes (1-9)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true)
      WHERE assignee_type IS NOT NULL
        AND assignee_type NOT IN ('1','2','3','4','5','6','7','8','9'));

-- T7: country_code format (2-letter ISO)
INSERT INTO dq_results
SELECT 'patents', 'patent_assignees', 'T7_country_code_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'country_code not matching 2-letter ISO format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_assignees', allow_moved_paths := true)
      WHERE country_code IS NOT NULL
        AND NOT REGEXP_MATCHES(country_code, '^[A-Z]{2}$'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: patent_inventors
-- ─────────────────────────────────────────────────────────────

-- T0: readability
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true) LIMIT 1;

-- T1: existence
INSERT INTO dq_results
SELECT 'patents', 'patent_inventors', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true));

-- T2: row_count (~2+ inventors/patent on average)
INSERT INTO dq_results
SELECT 'patents', 'patent_inventors', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END,
  n, 100000, 'Expected at least 100000 patent inventor records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true));

-- T2b: historical coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_inventors', 'T2b_historical_coverage',
  CASE WHEN min_year <= 2025 THEN 'pass' ELSE 'fail' END,
  min_year, 2025, 'MIN(grant_year) must be <= 2025 (historical worker)'
FROM (SELECT MIN(grant_year) AS min_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true));

-- T2c: daily coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_inventors', 'T2c_daily_coverage',
  CASE WHEN max_year >= 2026 THEN 'pass' ELSE 'fail' END,
  max_year, 2026, 'MAX(grant_year) must be >= 2026 (daily worker)'
FROM (SELECT MAX(grant_year) AS max_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'patents', 'patent_inventors', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'patents', 'patent_inventors', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (patent_id, grant_year NOT NULL)
INSERT INTO dq_results
SELECT 'patents', 'patent_inventors', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL patent_id or grant_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true)
      WHERE patent_id IS NULL OR grant_year IS NULL);

-- T7: gender_code values (M, F, U=unknown from PatentsView disambiguation)
INSERT INTO dq_results
SELECT 'patents', 'patent_inventors', 'T7_gender_code_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with gender_code outside expected values (M, F, U)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true)
      WHERE gender_code IS NOT NULL AND gender_code NOT IN ('M', 'F', 'U'));

-- T7: country_code format (2-letter ISO)
INSERT INTO dq_results
SELECT 'patents', 'patent_inventors', 'T7_country_code_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'country_code not matching 2-letter ISO format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_inventors', allow_moved_paths := true)
      WHERE country_code IS NOT NULL
        AND NOT REGEXP_MATCHES(country_code, '^[A-Z]{2}$'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: patent_cpc_classes
-- ─────────────────────────────────────────────────────────────

-- T0: readability
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true) LIMIT 1;

-- T1: existence
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true));

-- T2: row_count (~3+ CPC codes per patent)
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T2_row_count',
  CASE WHEN n >= 200000 THEN 'pass' ELSE 'fail' END,
  n, 200000, 'Expected at least 200000 patent CPC classification records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true));

-- T2b: historical coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T2b_historical_coverage',
  CASE WHEN min_year <= 2025 THEN 'pass' ELSE 'fail' END,
  min_year, 2025, 'MIN(grant_year) must be <= 2025 (historical worker)'
FROM (SELECT MIN(grant_year) AS min_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true));

-- T2c: daily coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T2c_daily_coverage',
  CASE WHEN max_year >= 2026 THEN 'pass' ELSE 'fail' END,
  max_year, 2026, 'MAX(grant_year) must be >= 2026 (daily worker)'
FROM (SELECT MAX(grant_year) AS max_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (patent_id, grant_year NOT NULL)
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL patent_id or grant_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true)
      WHERE patent_id IS NULL OR grant_year IS NULL);

-- T7: cpc_section values (A-H, Y)
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T7_cpc_section_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with cpc_section outside known CPC sections (A-H, Y)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true)
      WHERE cpc_section IS NOT NULL
        AND cpc_section NOT IN ('A','B','C','D','E','F','G','H','Y'));

-- T7: cpc_type values (i=inventive, a=additional)
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T7_cpc_type_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with cpc_type outside expected values (i, a)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true)
      WHERE cpc_type IS NOT NULL AND cpc_type NOT IN ('i', 'a'));

-- T7: all 9 CPC sections present
INSERT INTO dq_results
SELECT 'patents', 'patent_cpc_classes', 'T7_cpc_section_coverage',
  CASE WHEN n >= 9 THEN 'pass' ELSE 'warn' END,
  n, 9, 'Distinct cpc_section values (expect all 9: A-H plus Y)'
FROM (SELECT COUNT(DISTINCT cpc_section) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_cpc_classes', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: patent_claims
-- ─────────────────────────────────────────────────────────────

-- T0: readability
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true) LIMIT 1;

-- T1: existence
INSERT INTO dq_results
SELECT 'patents', 'patent_claims', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true));

-- T2: row_count (~20+ claims per patent on average)
INSERT INTO dq_results
SELECT 'patents', 'patent_claims', 'T2_row_count',
  CASE WHEN n >= 200000 THEN 'pass' ELSE 'fail' END,
  n, 200000, 'Expected at least 200000 patent claim records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true));

-- T2b: historical coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_claims', 'T2b_historical_coverage',
  CASE WHEN min_year <= 2025 THEN 'pass' ELSE 'fail' END,
  min_year, 2025, 'MIN(grant_year) must be <= 2025 (historical worker)'
FROM (SELECT MIN(grant_year) AS min_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true));

-- T2c: daily coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_claims', 'T2c_daily_coverage',
  CASE WHEN max_year >= 2026 THEN 'pass' ELSE 'fail' END,
  max_year, 2026, 'MAX(grant_year) must be >= 2026 (daily worker)'
FROM (SELECT MAX(grant_year) AS max_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'patents', 'patent_claims', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'fail' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'patents', 'patent_claims', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (patent_id, grant_year NOT NULL)
INSERT INTO dq_results
SELECT 'patents', 'patent_claims', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL patent_id or grant_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true)
      WHERE patent_id IS NULL OR grant_year IS NULL);

-- T7: independent claims present (dependent = 0 means independent)
INSERT INTO dq_results
SELECT 'patents', 'patent_claims', 'T7_independent_claims_present',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  n, 1, 'Count of independent claims (dependent = 0) — expect at least 1'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true)
      WHERE dependent = 0);

-- T7: claim_text not blank
INSERT INTO dq_results
SELECT 'patents', 'patent_claims', 'T7_claim_text_not_blank',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with non-null claim_text that is empty or whitespace-only'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_claims', allow_moved_paths := true)
      WHERE claim_text IS NOT NULL AND TRIM(claim_text) = '');

-- ─────────────────────────────────────────────────────────────
-- TABLE: patent_summaries
-- ─────────────────────────────────────────────────────────────

-- T0: readability
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true) LIMIT 1;

-- T1: existence
INSERT INTO dq_results
SELECT 'patents', 'patent_summaries', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true));

-- T2: row_count (~1 summary per patent; not all patents have summaries)
INSERT INTO dq_results
SELECT 'patents', 'patent_summaries', 'T2_row_count',
  CASE WHEN n >= 50000 THEN 'pass' ELSE 'fail' END,
  n, 50000, 'Expected at least 50000 patent summary records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true));

-- T2b: historical coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_summaries', 'T2b_historical_coverage',
  CASE WHEN min_year <= 2025 THEN 'pass' ELSE 'fail' END,
  min_year, 2025, 'MIN(grant_year) must be <= 2025 (historical worker)'
FROM (SELECT MIN(grant_year) AS min_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true));

-- T2c: daily coverage
INSERT INTO dq_results
SELECT 'patents', 'patent_summaries', 'T2c_daily_coverage',
  CASE WHEN max_year >= 2026 THEN 'pass' ELSE 'fail' END,
  max_year, 2026, 'MAX(grant_year) must be >= 2026 (daily worker)'
FROM (SELECT MAX(grant_year) AS max_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'patents', 'patent_summaries', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'patents', 'patent_summaries', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (patent_id, grant_year NOT NULL)
INSERT INTO dq_results
SELECT 'patents', 'patent_summaries', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL patent_id or grant_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true)
      WHERE patent_id IS NULL OR grant_year IS NULL);

-- T7: summary_text populated (> 50% non-null)
INSERT INTO dq_results
SELECT 'patents', 'patent_summaries', 'T7_summary_text_populated',
  CASE WHEN ratio >= 0.5 THEN 'pass' ELSE 'warn' END,
  ratio, 0.5, 'Fraction of rows with non-null summary_text (expect >= 0.5)'
FROM (
  SELECT CAST(SUM(CASE WHEN summary_text IS NOT NULL THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS ratio
  FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true)
);

-- T7: summary_text not blank
INSERT INTO dq_results
SELECT 'patents', 'patent_summaries', 'T7_summary_text_not_blank',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with non-null summary_text that is empty or whitespace-only'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/patent_summaries', allow_moved_paths := true)
      WHERE summary_text IS NOT NULL AND TRIM(summary_text) = '');

-- ─────────────────────────────────────────────────────────────
-- TABLE: trademark_applications
-- ─────────────────────────────────────────────────────────────

-- T0: readability
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true) LIMIT 1;

-- T1: existence
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true));

-- T2: row_count (TRCFECO2 is an economics subset, ~10k-15k records per year)
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END,
  n, 5000, 'Expected at least 5000 trademark application records (TRCFECO2 economics subset)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true));

-- T2b: historical coverage (application_year <= 2025 from historical worker)
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T2b_historical_coverage',
  CASE WHEN min_year <= 2025 THEN 'pass' ELSE 'fail' END,
  min_year, 2025, 'MIN(application_year) must be <= 2025 (historical worker)'
FROM (SELECT MIN(application_year) AS min_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true));

-- T2c: trademark data is capped at 2022 (USPTO TRCFECO2 snapshot 2023 is latest published)
-- No daily worker coverage expected; this is a source availability constraint not a pipeline gap.
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T2c_daily_coverage',
  CASE WHEN max_year >= 2020 THEN 'warn' ELSE 'fail' END,
  max_year, 2020, 'MAX(application_year) expected <= 2022 — USPTO TRCFECO2 snapshot 2024 not yet published'
FROM (SELECT MAX(application_year) AS max_year FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (serial_no, application_year NOT NULL)
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL serial_no or application_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true)
      WHERE serial_no IS NULL OR application_year IS NULL);

-- T7: mark_draw_cd values (USPTO 4-digit codes: 1000=typed 2000=unlined drawing 3000=illustration
--     4000=standard characters 5000=words/letters/numbers in stylized form)
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T7_mark_draw_cd_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with mark_draw_cd outside known USPTO codes (1000-5000)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true)
      WHERE mark_draw_cd IS NOT NULL
        AND mark_draw_cd NOT IN ('1000','2000','3000','4000','5000'));

-- T7: filing_dt format (YYYY-MM-DD as returned by TRCFECO2)
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T7_filing_dt_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'filing_dt not matching YYYY-MM-DD format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true)
      WHERE filing_dt IS NOT NULL
        AND NOT REGEXP_MATCHES(filing_dt, '^\d{4}-\d{2}-\d{2}$'));

-- T7: serial_no uniqueness (rare duplicates possible across multi-year snapshots)
INSERT INTO dq_results
SELECT 'patents', 'trademark_applications', 'T7_serial_no_uniqueness',
  CASE WHEN dups = 0 THEN 'pass' ELSE 'warn' END,
  dups, 0, 'Duplicate serial_no values across all partitions (rare, from multi-year snapshot overlap)'
FROM (
  SELECT COUNT(*) AS dups
  FROM (
    SELECT serial_no, COUNT(*) AS cnt
    FROM iceberg_scan('s3://govdata-parquet-v1/patents/trademark_applications', allow_moved_paths := true)
    WHERE serial_no IS NOT NULL
    GROUP BY serial_no
    HAVING COUNT(*) > 1
  )
);

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
