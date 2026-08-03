-- dq-lookback: 1
-- U.S. Environment Data Quality Checks
-- Schema: environment
-- Air quality (AQS, moved from weather), TRI, GHGRP, eGRID, USGS water, SDWIS, ECHO/FRS,
-- Superfund, RCRA, Water Quality Portal. All Iceberg; single-nested iceberg_scan path.
-- T4/T5 exclude partition columns (type[,year][,state]).

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';

CREATE TEMP TABLE dq_results (
  schema   VARCHAR, tbl VARCHAR, test VARCHAR, status VARCHAR,
  value DOUBLE, threshold DOUBLE, detail VARCHAR
);

-- ------------------------------------------------------------
-- TABLE: air_quality_annual (partition cols: type, year)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'air_quality_annual', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_annual', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'air_quality_annual', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_annual', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_annual', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'air_quality_annual', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_annual', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'air_quality_annual', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_annual', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'air_quality_annual', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_annual', allow_moved_paths := true) WHERE county_fips IS NULL);

-- ------------------------------------------------------------
-- TABLE: air_quality_daily (partition cols: type, year)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'air_quality_daily', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_daily', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'air_quality_daily', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_daily', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_daily', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'air_quality_daily', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_daily', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'air_quality_daily', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_daily', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'air_quality_daily', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_daily', allow_moved_paths := true) WHERE county_fips IS NULL);

-- ------------------------------------------------------------
-- TABLE: aqs_monitors (partition cols: type)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'aqs_monitors', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/aqs_monitors', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'aqs_monitors', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/aqs_monitors', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/aqs_monitors', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'aqs_monitors', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/aqs_monitors', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'aqs_monitors', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/aqs_monitors', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'aqs_monitors', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL site_number rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/aqs_monitors', allow_moved_paths := true) WHERE site_number IS NULL);

-- ------------------------------------------------------------
-- TABLE: tri_releases (partition cols: type, year)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'tri_releases', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/tri_releases', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'tri_releases', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/tri_releases', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/tri_releases', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'tri_releases', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/tri_releases', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'tri_releases', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/tri_releases', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'tri_releases', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL tri_facility_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/tri_releases', allow_moved_paths := true) WHERE tri_facility_id IS NULL);

-- ------------------------------------------------------------
-- TABLE: ghg_facilities (partition cols: type, year)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'ghg_facilities', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_facilities', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'ghg_facilities', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_facilities', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_facilities', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'ghg_facilities', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_facilities', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'ghg_facilities', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_facilities', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'ghg_facilities', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL facility_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_facilities', allow_moved_paths := true) WHERE facility_id IS NULL);

-- ------------------------------------------------------------
-- TABLE: ghg_emissions (partition cols: type, year)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'ghg_emissions', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_emissions', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'ghg_emissions', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_emissions', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_emissions', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'ghg_emissions', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_emissions', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'ghg_emissions', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_emissions', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'ghg_emissions', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL facility_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ghg_emissions', allow_moved_paths := true) WHERE facility_id IS NULL);

-- ------------------------------------------------------------
-- TABLE: egrid_emission_rates (partition cols: type, year)
-- Small table: ~27 eGRID subregion rows per year (dataLag 2 + releaseMonth 2 — a DQ
-- window starting later than current-2 yields zero rows for this table, not a defect).
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'egrid_emission_rates', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/egrid_emission_rates', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'egrid_emission_rates', 'T2_row_count',
  CASE WHEN n >= 20 THEN 'pass' ELSE 'fail' END, n, 20, 'Expected >=20 rows (~27 subregions per covered year)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/egrid_emission_rates', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/egrid_emission_rates', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'egrid_emission_rates', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/egrid_emission_rates', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'egrid_emission_rates', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/egrid_emission_rates', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'egrid_emission_rates', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL subregion_acronym rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/egrid_emission_rates', allow_moved_paths := true) WHERE subregion_acronym IS NULL);

-- ------------------------------------------------------------
-- TABLE: water_sites (partition cols: type, state)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'water_sites', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_sites', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'water_sites', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_sites', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_sites', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'water_sites', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_sites', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'state')));
INSERT INTO dq_results
SELECT 'environment', 'water_sites', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_sites', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'state')));
INSERT INTO dq_results
SELECT 'environment', 'water_sites', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL site_no rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_sites', allow_moved_paths := true) WHERE site_no IS NULL);

-- ------------------------------------------------------------
-- TABLE: streamflow (partition cols: type, state, year)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'streamflow', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/streamflow', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'streamflow', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/streamflow', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/streamflow', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'streamflow', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/streamflow', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'state', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'streamflow', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/streamflow', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'state', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'streamflow', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL site_no rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/streamflow', allow_moved_paths := true) WHERE site_no IS NULL);

-- ------------------------------------------------------------
-- TABLE: drinking_water (partition cols: type, state)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'drinking_water', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'drinking_water', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'drinking_water', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'state')));
INSERT INTO dq_results
SELECT 'environment', 'drinking_water', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'state')));
INSERT INTO dq_results
SELECT 'environment', 'drinking_water', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL pwsid rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water', allow_moved_paths := true) WHERE pwsid IS NULL);

-- ------------------------------------------------------------
-- TABLE: epa_facilities (partition cols: type)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'epa_facilities', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/epa_facilities', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'epa_facilities', 'T2_row_count',
  CASE WHEN n >= 10000 THEN 'pass' ELSE 'fail' END, n, 10000, 'Expected >=10000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/epa_facilities', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/epa_facilities', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'epa_facilities', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/epa_facilities', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'epa_facilities', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/epa_facilities', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'epa_facilities', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL registry_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/epa_facilities', allow_moved_paths := true) WHERE registry_id IS NULL);

-- ------------------------------------------------------------
-- TABLE: drinking_water_violations (partition cols: type, state)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_violations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_violations', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_violations', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_violations', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_violations', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_violations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_violations', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'state')));
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_violations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_violations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'state')));
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_violations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL pwsid rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_violations', allow_moved_paths := true) WHERE pwsid IS NULL);

-- ------------------------------------------------------------
-- TABLE: superfund_sites (partition cols: type)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'superfund_sites', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/superfund_sites', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'superfund_sites', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END, n, 500, 'Expected >=500 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/superfund_sites', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/superfund_sites', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'superfund_sites', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/superfund_sites', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'superfund_sites', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/superfund_sites', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'superfund_sites', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL site_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/superfund_sites', allow_moved_paths := true) WHERE site_id IS NULL);

-- ------------------------------------------------------------
-- TABLE: rcra_facilities (partition cols: type, state)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'rcra_facilities', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/rcra_facilities', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'rcra_facilities', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/rcra_facilities', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/rcra_facilities', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'rcra_facilities', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/rcra_facilities', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'state')));
INSERT INTO dq_results
SELECT 'environment', 'rcra_facilities', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/rcra_facilities', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'state')));
INSERT INTO dq_results
SELECT 'environment', 'rcra_facilities', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL handler_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/rcra_facilities', allow_moved_paths := true) WHERE handler_id IS NULL);

-- ------------------------------------------------------------
-- TABLE: water_quality_samples (partition cols: type, state, year)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'water_quality_samples', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_quality_samples', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'water_quality_samples', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END, n, 500, 'Expected >=500 rows (DQ-sampled where capped)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_quality_samples', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_quality_samples', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'water_quality_samples', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_quality_samples', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'state', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'water_quality_samples', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_quality_samples', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'state', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'water_quality_samples', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL monitoring_location_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_quality_samples', allow_moved_paths := true) WHERE monitoring_location_id IS NULL);

-- ============================================================================
-- VIEW: environmental_burden_by_county (air_quality_annual left-joined cross-schema to
-- census.acs_poverty and census.acs_race_ethnicity on county_fips+year)
-- Views have no physical iceberg path, so the join is replicated here directly against
-- the iceberg path of each base table. The real Calcite-path check lives in model-verify.
-- ============================================================================

INSERT INTO dq_results
SELECT 'environment', 'environmental_burden_by_county', 'T7_census_poverty_join_coverage',
  CASE WHEN matched > 0 THEN 'pass' ELSE 'warn' END, matched, 1,
  'air_quality_annual county-years with a matching census.acs_poverty row (cross-schema join on county_fips+year)'
FROM (
  SELECT COUNT(*) AS matched
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_annual', allow_moved_paths := true) aq
  JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_poverty', allow_moved_paths := true) p
    ON aq.county_fips = p.county_fips AND aq.year = p.year AND p.geography = 'county'
);

INSERT INTO dq_results
SELECT 'environment', 'environmental_burden_by_county', 'T7_census_race_join_coverage',
  CASE WHEN matched > 0 THEN 'pass' ELSE 'warn' END, matched, 1,
  'air_quality_annual county-years with a matching census.acs_race_ethnicity row (cross-schema join on county_fips+year)'
FROM (
  SELECT COUNT(*) AS matched
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/air_quality_annual', allow_moved_paths := true) aq
  JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/census/acs_race_ethnicity', allow_moved_paths := true) re
    ON aq.county_fips = re.county_fips AND aq.year = re.year AND re.geography = 'county'
);

SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results ORDER BY schema, tbl, test;
