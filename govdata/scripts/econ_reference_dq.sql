-- Economic Reference Data Quality Checks
-- Schema: econ_reference
-- Tables: jolts_industries, jolts_dataelements, bls_geographies, naics_sectors,
--         nipa_tables, regional_linecodes, fred_series
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition columns per table.
-- NOTE: fred_series T7 expects 5 distinct categories (5/7 configured; categories 1
--       and 3 are capped at 1,000 rows by the FRED API per-category limit).

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';

CREATE TEMP TABLE dq_results (
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,
  value     DOUBLE,
  threshold DOUBLE,
  detail    VARCHAR
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: jolts_industries
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_industries', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_industries', allow_moved_paths := true));

-- T2: row_count (BLS lists ~20 JOLTS industry groupings)
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_industries', 'T2_row_count',
  CASE WHEN n >= 20 THEN 'pass' ELSE 'fail' END,
  n, 20, 'Expected at least 20 JOLTS industry codes'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_industries', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_industries', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_industries', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_industries', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_industries', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_industries', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (industry_code NOT NULL)
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_industries', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with NULL industry_code'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_industries', allow_moved_paths := true) WHERE industry_code IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: jolts_dataelements
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_dataelements', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_dataelements', allow_moved_paths := true));

-- T2: row_count (5 JOLTS metric codes: JOR, HIR, QUR, TSR, LDR)
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_dataelements', 'T2_row_count',
  CASE WHEN n >= 5 THEN 'pass' ELSE 'fail' END,
  n, 5, 'Expected at least 5 JOLTS data element codes'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_dataelements', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_dataelements', allow_moved_paths := true) LIMIT 5;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_dataelements', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_dataelements', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_dataelements', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_dataelements', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (dataelement_code NOT NULL)
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_dataelements', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with NULL dataelement_code'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_dataelements', allow_moved_paths := true) WHERE dataelement_code IS NULL);

-- T7: expected_values — 5 core JOLTS metric codes must be present (JO, HI, QU, TS, LD)
INSERT INTO dq_results
SELECT 'econ_reference', 'jolts_dataelements', 'T7_expected_values',
  CASE WHEN n >= 5 THEN 'pass' ELSE 'fail' END,
  n, 5,
  'Expected core JOLTS codes: JO, HI, QU, TS, LD. Found: ' || CAST(n AS VARCHAR)
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/jolts_dataelements', allow_moved_paths := true)
  WHERE dataelement_code IN ('JO', 'HI', 'QU', 'TS', 'LD')
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bls_geographies
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'econ_reference', 'bls_geographies', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/bls_geographies', allow_moved_paths := true));

-- T2: row_count (50 states + DC + territories + ~400 metro areas + regions)
INSERT INTO dq_results
SELECT 'econ_reference', 'bls_geographies', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END,
  n, 50, 'Expected at least 50 geography records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/bls_geographies', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/bls_geographies', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'econ_reference', 'bls_geographies', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/bls_geographies', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value (geo_type excluded: only 3 values state/metro/region by design)
INSERT INTO dq_results
SELECT 'econ_reference', 'bls_geographies', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/bls_geographies', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'geo_type')
  )
);

-- T6: pk_nulls (geo_code NOT NULL)
INSERT INTO dq_results
SELECT 'econ_reference', 'bls_geographies', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with NULL geo_code'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/bls_geographies', allow_moved_paths := true) WHERE geo_code IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: naics_sectors
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'econ_reference', 'naics_sectors', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/naics_sectors', allow_moved_paths := true));

-- T2: row_count (~18 NAICS supersector codes including total nonfarm and subtotals)
INSERT INTO dq_results
SELECT 'econ_reference', 'naics_sectors', 'T2_row_count',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'fail' END,
  n, 10, 'Expected at least 10 NAICS supersector codes'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/naics_sectors', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/naics_sectors', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'econ_reference', 'naics_sectors', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/naics_sectors', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'econ_reference', 'naics_sectors', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/naics_sectors', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type')
  )
);

-- T6: pk_nulls (supersector_code NOT NULL)
INSERT INTO dq_results
SELECT 'econ_reference', 'naics_sectors', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with NULL supersector_code'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/naics_sectors', allow_moved_paths := true) WHERE supersector_code IS NULL);

-- T7: expected_values — total nonfarm sector '00000000' must be present
INSERT INTO dq_results
SELECT 'econ_reference', 'naics_sectors', 'T7_expected_values',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Expected total nonfarm code 00000000 to be present'
FROM (
  SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/naics_sectors', allow_moved_paths := true)
  WHERE supersector_code = '00000000'
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nipa_tables
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'econ_reference', 'nipa_tables', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/nipa_tables', allow_moved_paths := true));

-- T2: row_count (BEA publishes 100+ NIPA tables across 8 sections)
INSERT INTO dq_results
SELECT 'econ_reference', 'nipa_tables', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END,
  n, 100, 'Expected at least 100 NIPA table catalog entries'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/nipa_tables', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/nipa_tables', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'econ_reference', 'nipa_tables', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/nipa_tables', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'section')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'econ_reference', 'nipa_tables', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/nipa_tables', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'section')
  )
);

-- T6: pk_nulls (table_name NOT NULL)
INSERT INTO dq_results
SELECT 'econ_reference', 'nipa_tables', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with NULL table_name'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/nipa_tables', allow_moved_paths := true) WHERE table_name IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: regional_linecodes
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'econ_reference', 'regional_linecodes', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/regional_linecodes', allow_moved_paths := true));

-- T2: row_count (57 Regional tables × ~30-60 line codes each → well over 2,000)
INSERT INTO dq_results
SELECT 'econ_reference', 'regional_linecodes', 'T2_row_count',
  CASE WHEN n >= 2000 THEN 'pass' ELSE 'fail' END,
  n, 2000, 'Expected at least 2,000 regional line code entries'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/regional_linecodes', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/regional_linecodes', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'econ_reference', 'regional_linecodes', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/regional_linecodes', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'tablename')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'econ_reference', 'regional_linecodes', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/regional_linecodes', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'tablename')
  )
);

-- T6: pk_nulls (key NOT NULL)
INSERT INTO dq_results
SELECT 'econ_reference', 'regional_linecodes', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with NULL key'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/regional_linecodes', allow_moved_paths := true) WHERE key IS NULL);

-- T7: expected_values — at least 50 distinct tablenames (57 configured)
INSERT INTO dq_results
SELECT 'econ_reference', 'regional_linecodes', 'T7_expected_values',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END,
  n, 50, 'Expected at least 50 distinct BEA Regional table names'
FROM (
  SELECT COUNT(DISTINCT tablename) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/regional_linecodes', allow_moved_paths := true)
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: fred_series
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'econ_reference', 'fred_series', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/fred_series', allow_moved_paths := true));

-- T2: row_count (7 categories × up to 1,000 series each → ≥1,000 expected)
INSERT INTO dq_results
SELECT 'econ_reference', 'fred_series', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1,000 FRED series entries across configured categories'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/fred_series', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/fred_series', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'econ_reference', 'fred_series', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/fred_series', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'category')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'econ_reference', 'fred_series', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/fred_series', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'category')
  )
);

-- T6: pk_nulls (series NOT NULL)
INSERT INTO dq_results
SELECT 'econ_reference', 'fred_series', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'Rows with NULL series'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/fred_series', allow_moved_paths := true) WHERE series IS NULL);

-- T7: expected_values — 5 of 7 configured categories return data (categories 1 and 3
--     are capped at 1,000 rows by FRED API per-category limit; all 5 should be present)
INSERT INTO dq_results
SELECT 'econ_reference', 'fred_series', 'T7_expected_values',
  CASE WHEN n >= 5 THEN 'pass' ELSE 'fail' END,
  n, 5,
  'Expected at least 5 distinct FRED categories (7 configured; 2 may be capped). Found: ' || CAST(n AS VARCHAR)
FROM (
  SELECT COUNT(DISTINCT category) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/econ_reference/fred_series', allow_moved_paths := true)
);

-- ─────────────────────────────────────────────────────────────
-- SUMMARY
-- ─────────────────────────────────────────────────────────────
SELECT
  schema, tbl, test, status,
  ROUND(value, 2) AS value,
  ROUND(threshold, 2) AS threshold,
  detail
FROM dq_results
ORDER BY tbl, test;

SELECT
  tbl,
  COUNT(*) FILTER (WHERE status = 'pass') AS pass,
  COUNT(*) FILTER (WHERE status = 'warn') AS warn,
  COUNT(*) FILTER (WHERE status = 'fail') AS fail
FROM dq_results
GROUP BY tbl
ORDER BY tbl;
