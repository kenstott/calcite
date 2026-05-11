-- U.S. Federal Lands Data Quality Checks
-- Schema: lands
-- Tables: national_forests, timber_sales, forest_inventory,
--         nps_units, nps_visitation, blm_field_offices, onrr_revenues
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition columns 'type' and 'year'.

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

-- T7: state_fips format (2-digit zero-padded)
INSERT INTO dq_results
SELECT 'lands', 'national_forests', 'T7_state_fips_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with state_fips not matching 2-digit format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true)
      WHERE state_fips IS NOT NULL
        AND NOT REGEXP_MATCHES(state_fips, '^\d{2}$'));

-- T7: gis_acres positive
INSERT INTO dq_results
SELECT 'lands', 'national_forests', 'T7_gis_acres_positive',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with gis_acres <= 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/national_forests', allow_moved_paths := true)
      WHERE gis_acres IS NOT NULL AND gis_acres <= 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: timber_sales
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true));

-- T2: row_count (thousands of sales per year across all forests)
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END,
  n, 1000, 'Expected at least 1000 timber sale records'
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

-- T6: pk_nulls (contract_id, sale_year NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL contract_id or sale_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true)
      WHERE contract_id IS NULL OR sale_year IS NULL);

-- T7: sale_year coverage (at least 1 distinct year)
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T7_sale_year_coverage',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Distinct sale_year values'
FROM (SELECT COUNT(DISTINCT sale_year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true));

-- T7: appraised_value non-negative
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T7_appraised_value_non_negative',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with appraised_value < 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true)
      WHERE appraised_value IS NOT NULL AND appraised_value < 0);

-- T7: state_fips format (2-digit)
INSERT INTO dq_results
SELECT 'lands', 'timber_sales', 'T7_state_fips_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with state_fips not matching 2-digit format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/timber_sales', allow_moved_paths := true)
      WHERE state_fips IS NOT NULL
        AND NOT REGEXP_MATCHES(state_fips, '^\d{2}$'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: forest_inventory
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true));

-- T2: row_count (state × forest-type × owner groups per year)
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END,
  n, 500, 'Expected at least 500 forest inventory records'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
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
      AND column_name NOT IN ('type', 'year')
  )
);

-- T5: all_same_value
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
      AND column_name NOT IN ('type', 'year')
  )
);

-- T6: pk_nulls (state_fips, inventory_year NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL state_fips or inventory_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true)
      WHERE state_fips IS NULL OR inventory_year IS NULL);

-- T7: state_fips format (2-digit)
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T7_state_fips_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with state_fips not matching 2-digit format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true)
      WHERE state_fips IS NOT NULL
        AND NOT REGEXP_MATCHES(state_fips, '^\d{2}$'));

-- T7: land_area positive
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T7_land_area_positive',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with land_area <= 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true)
      WHERE land_area IS NOT NULL AND land_area <= 0);

-- T7: carbon_ag non-negative (above-ground carbon cannot be negative)
INSERT INTO dq_results
SELECT 'lands', 'forest_inventory', 'T7_carbon_ag_non_negative',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with carbon_ag < 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/forest_inventory', allow_moved_paths := true)
      WHERE carbon_ag IS NOT NULL AND carbon_ag < 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nps_units
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

-- T7: unit_code uniqueness (4-letter alpha codes are unique)
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

-- T7: gis_acres positive
INSERT INTO dq_results
SELECT 'lands', 'nps_units', 'T7_gis_acres_positive',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with gis_acres <= 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_units', allow_moved_paths := true)
      WHERE gis_acres IS NOT NULL AND gis_acres <= 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: nps_visitation
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

-- T6: pk_nulls (unit_code, year, month NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL unit_code, year, or month rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true)
      WHERE unit_code IS NULL OR year IS NULL OR month IS NULL);

-- T7: month range (1-12)
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T7_month_range',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows with month outside 1-12'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true)
      WHERE month IS NOT NULL AND (month < 1 OR month > 12));

-- T7: recreation_visitors non-negative
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T7_recreation_visitors_non_negative',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with recreation_visitors < 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true)
      WHERE recreation_visitors IS NOT NULL AND recreation_visitors < 0);

-- T7: year coverage (at least 1 distinct year)
INSERT INTO dq_results
SELECT 'lands', 'nps_visitation', 'T7_year_coverage',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Distinct year values'
FROM (SELECT COUNT(DISTINCT year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/nps_visitation', allow_moved_paths := true));

-- ─────────────────────────────────────────────────────────────
-- TABLE: blm_field_offices
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

-- T6: pk_nulls (adm_unit_cd NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL adm_unit_cd rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true)
      WHERE adm_unit_cd IS NULL);

-- T7: adm_unit_cd uniqueness
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T7_adm_unit_cd_uniqueness',
  CASE WHEN dups = 0 THEN 'pass' ELSE 'fail' END,
  dups, 0, 'Duplicate adm_unit_cd values'
FROM (
  SELECT COUNT(*) AS dups
  FROM (
    SELECT adm_unit_cd, COUNT(*) AS cnt
    FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true)
    WHERE adm_unit_cd IS NOT NULL
    GROUP BY adm_unit_cd
    HAVING COUNT(*) > 1
  )
);

-- T7: state_fips format (2-digit)
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T7_state_fips_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with state_fips not matching 2-digit format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true)
      WHERE state_fips IS NOT NULL
        AND NOT REGEXP_MATCHES(state_fips, '^\d{2}$'));

-- T7: gis_acres positive
INSERT INTO dq_results
SELECT 'lands', 'blm_field_offices', 'T7_gis_acres_positive',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with gis_acres <= 0'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/blm_field_offices', allow_moved_paths := true)
      WHERE gis_acres IS NOT NULL AND gis_acres <= 0);

-- ─────────────────────────────────────────────────────────────
-- TABLE: onrr_revenues
-- ─────────────────────────────────────────────────────────────

-- T1: existence
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true));

-- T2: row_count (millions of revenue line items across all years)
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T2_row_count',
  CASE WHEN n >= 10000 THEN 'pass' ELSE 'fail' END,
  n, 10000, 'Expected at least 10000 ONRR revenue records'
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

-- T6: pk_nulls (revenue_type, calendar_year NOT NULL)
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL revenue_type or calendar_year rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true)
      WHERE revenue_type IS NULL OR calendar_year IS NULL);

-- T7: calendar_year coverage (at least 1 distinct year)
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T7_calendar_year_coverage',
  CASE WHEN n >= 1 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Distinct calendar_year values'
FROM (SELECT COUNT(DISTINCT calendar_year) AS n FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true));

-- T7: revenue non-negative (royalties and rents cannot be negative in valid rows)
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T7_revenue_non_negative',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with revenue < 0 (adjustments possible but flag for review)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true)
      WHERE revenue IS NOT NULL AND revenue < 0);

-- T7: state_fips format (2-digit) when present
INSERT INTO dq_results
SELECT 'lands', 'onrr_revenues', 'T7_state_fips_format',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END,
  bad, 0, 'Rows with state_fips not matching 2-digit format'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://govdata-parquet-v1/lands/onrr_revenues', allow_moved_paths := true)
      WHERE state_fips IS NOT NULL
        AND NOT REGEXP_MATCHES(state_fips, '^\d{2}$'));

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
