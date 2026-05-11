-- U.S. Federal Lands Data Quality Checks
-- Schema: lands
-- Tables: national_forests, timber_sales, forest_inventory,
--         nps_units, nps_visitation, blm_field_offices, onrr_revenues
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition columns 'type' and 'year'.
-- NOTE: forest_inventory FIA API requires per-state EVALID; T1/T2 set to warn
--       until source redesign is complete.

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';

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
-- TABLE: national_forests
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'national_forests', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true));

-- T2: row_count (154 national forests in the USFS system)
INSERT INTO dq_results
SELECT 'lands', 'national_forests', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END,
  n, 100, 'Expected at least 100 national forest records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'lands', 'national_forests', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'lands', 'national_forests', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (forest_id NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'national_forests', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL forest_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true)
      WHERE forest_id IS NULL);

-- T7: forest_id uniqueness
INSERT INTO dq_results
SELECT 'lands', 'national_forests', 'T7_forest_id_uniqueness',
  CASE WHEN dups = 0 THEN 'pass' ELSE 'fail' END,
  dups, 0, 'Duplicate forest_id values'
FROM (
  SELECT COUNT(*) AS dups
  FROM (
    SELECT forest_id, COUNT(*) AS cnt
    FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true)
    WHERE forest_id IS NOT NULL
    GROUP BY forest_id
    HAVING COUNT(*) > 1
  )
);

-- T7: gross_acres positive
INSERT INTO dq_results
SELECT 'lands', 'national_forests', 'T7_gross_acres_positive',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with gross_acres <= 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true)
      WHERE gross_acres IS NOT NULL AND gross_acres <= 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: timber_sales  (source: EDW_TimberHarvest_01/MapServer/0)
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true));

-- T2: row_count (thousands of harvest activities per year)
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1000 timber harvest activity records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (facts_id NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL facts_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true)
      WHERE facts_id IS NULL);

-- T7: fy_completed coverage (at least 1 distinct fiscal year)
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T7_fy_completed_coverage',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Distinct fy_completed values'
FROM (SELECT COUNT(DISTINCT fy_completed) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true));

-- T7: gis_acres non-negative
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T7_gis_acres_non_negative',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with gis_acres < 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true)
      WHERE gis_acres IS NOT NULL AND gis_acres < 0);

-- T7: state_abbr format (2-char uppercase)
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T7_state_abbr_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with state_abbr not matching 2-char uppercase format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true)
      WHERE state_abbr IS NOT NULL
        AND NOT REGEXP_MATCHES(state_abbr, '^[A-Z]{2}$'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: forest_inventory
-- Source: FIA bulk COND CSV per state (apps.fs.usda.gov/fia/datamart/CSV/{state}_COND.csv)
-- Partitioned by stateAbbr (51 states + PR). Null columns (land_area_acres,
-- live_volume_cuft, carbon_stock_tons, trees_per_acre) are intentional —
-- expansion factors not available in COND table.
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true));

-- T2: row_count (51 states × ~500 groups avg = ~25000+ rows)
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T2_row_count',
  CASE WHEN n >= 10000 THEN 'pass' ELSE 'fail' END,
  n, 10000, 'Expected 10000+ rows (51 states × forest type × ownership × year aggregations)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true));

-- T3: sample (no-op if empty)
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true) LIMIT 3;

-- T4: null columns — exclude partition cols and intentionally-null metric cols
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'stateAbbr',
        'land_area_acres', 'live_volume_cuft', 'carbon_stock_tons', 'trees_per_acre')
  )
);

INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'stateAbbr')
  )
);

-- T6: state coverage (at least 40 distinct states)
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T6_state_coverage',
  CASE WHEN n >= 40 THEN 'pass' ELSE 'fail' END,
  n, 40, 'Distinct state_fips values'
FROM (SELECT COUNT(DISTINCT state_fips) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true));

-- T7: inventory_year coverage (at least 10 distinct years)
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T7_inventory_year_coverage',
  CASE WHEN n >= 10 THEN 'pass' ELSE 'warn' END,
  n, 10, 'Distinct inventory_year values'
FROM (SELECT COUNT(DISTINCT inventory_year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true));

-- T7: basal_area_sqft positive where not null
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T7_basal_area_positive',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with basal_area_sqft <= 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true)
      WHERE basal_area_sqft IS NOT NULL AND basal_area_sqft <= 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nps_units  (state_abbr instead of state_fips)
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'nps_units', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true));

-- T2: row_count (400+ NPS units)
INSERT INTO dq_results
SELECT 'lands', 'nps_units', 'T2_row_count',
  CASE WHEN n >= 300 THEN 'pass' ELSE 'fail' END,
  n, 300, 'Expected at least 300 NPS unit records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'lands', 'nps_units', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'lands', 'nps_units', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (unit_code NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'nps_units', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL unit_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true)
      WHERE unit_code IS NULL);

-- T7: unit_code uniqueness
INSERT INTO dq_results
SELECT 'lands', 'nps_units', 'T7_unit_code_uniqueness',
  CASE WHEN dups = 0 THEN 'pass' ELSE 'fail' END,
  dups, 0, 'Duplicate unit_code values'
FROM (
  SELECT COUNT(*) AS dups
  FROM (
    SELECT unit_code, COUNT(*) AS cnt
    FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true)
    WHERE unit_code IS NOT NULL
    GROUP BY unit_code
    HAVING COUNT(*) > 1
  )
);

-- T7: unit_code format (4 uppercase letters)
INSERT INTO dq_results
SELECT 'lands', 'nps_units', 'T7_unit_code_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with unit_code not matching 4-letter uppercase format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true)
      WHERE unit_code IS NOT NULL
        AND NOT REGEXP_MATCHES(unit_code, '^[A-Z]{4}$'));

-- T7: gross_acres positive
INSERT INTO dq_results
SELECT 'lands', 'nps_units', 'T7_gross_acres_positive',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with gross_acres <= 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true)
      WHERE gross_acres IS NOT NULL AND gross_acres <= 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nps_visitation  (XML API, monthly only, no camping cols)
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true));

-- T2: row_count (400+ units × 12 months × multiple years)
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END,
  n, 5000, 'Expected at least 5000 NPS visitation records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (unit_code, visit_year, visit_month NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL unit_code, visit_year, or visit_month rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true)
      WHERE unit_code IS NULL OR visit_year IS NULL OR visit_month IS NULL);

-- T7: visit_month range (1-12)
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T7_visit_month_range',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows with visit_month outside 1-12'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true)
      WHERE visit_month IS NOT NULL AND (visit_month < 1 OR visit_month > 12));

-- T7: recreation_visits non-negative
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T7_recreation_visits_non_negative',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with recreation_visits < 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true)
      WHERE recreation_visits IS NOT NULL AND recreation_visits < 0);

-- T7: visit_year coverage (at least 1 distinct year)
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T7_visit_year_coverage',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Distinct visit_year values'
FROM (SELECT COUNT(DISTINCT visit_year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: blm_field_offices  (state_abbr, no adm_acres)
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true));

-- T2: row_count (~150+ BLM field offices and districts)
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END,
  n, 100, 'Expected at least 100 BLM field office records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (office_code NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL office_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true)
      WHERE office_code IS NULL);

-- T7: office_code uniqueness
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T7_office_code_uniqueness',
  CASE WHEN dups = 0 THEN 'pass' ELSE 'fail' END,
  dups, 0, 'Duplicate office_code values'
FROM (
  SELECT COUNT(*) AS dups
  FROM (
    SELECT office_code, COUNT(*) AS cnt
    FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true)
    WHERE office_code IS NOT NULL
    GROUP BY office_code
    HAVING COUNT(*) > 1
  )
);

-- T7: state_abbr format (2-char uppercase)
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T7_state_abbr_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with state_abbr not matching 2-char uppercase format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true)
      WHERE state_abbr IS NOT NULL
        AND NOT REGEXP_MATCHES(state_abbr, '^[A-Z]{2}$'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: onrr_revenues  (bulk all-years CSV, fiscal_year)
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true));

-- T2: row_count (bulk file FY 2004-present, ~100k+ rows)
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T2_row_count',
  CASE WHEN n >= 10000 THEN 'pass' ELSE 'fail' END,
  n, 10000, 'Expected at least 10000 ONRR revenue records (bulk all-years file)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (fiscal_year, revenue_type NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL fiscal_year or revenue_type rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true)
      WHERE fiscal_year IS NULL OR revenue_type IS NULL);

-- T7: fiscal_year coverage (at least 1 distinct year)
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T7_fiscal_year_coverage',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Distinct fiscal_year values'
FROM (SELECT COUNT(DISTINCT fiscal_year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true));

-- T7: revenue non-negative (royalties/rents; adjustments flagged as warn)
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T7_revenue_non_negative',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with revenue < 0 (adjustments possible but flag for review)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true)
      WHERE revenue IS NOT NULL AND revenue < 0);

-- T7: county_fips format when present (5-digit)
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T7_county_fips_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with county_fips not matching 5-digit format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true)
      WHERE county_fips IS NOT NULL
        AND NOT REGEXP_MATCHES(county_fips, '^\d{5}$'));

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
