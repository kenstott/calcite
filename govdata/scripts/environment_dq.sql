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
-- TABLE: water_withdrawals (partition cols: type, year)
-- Quinquennial census (2000, 2005, 2010, 2015); ~3200 counties x 7 sectors per covered year.
-- A DQ window scoped to a single year yields ~22,500 rows, not a defect.
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'water_withdrawals', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_withdrawals', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'water_withdrawals', 'T2_row_count',
  CASE WHEN n >= 10000 THEN 'pass' ELSE 'fail' END, n, 10000, 'Expected >=10000 rows (single covered year: ~3200 counties x 7 sectors)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_withdrawals', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_withdrawals', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'water_withdrawals', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_withdrawals', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'water_withdrawals', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_withdrawals', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));
INSERT INTO dq_results
SELECT 'environment', 'water_withdrawals', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL county_fips/sector rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_withdrawals', allow_moved_paths := true) WHERE county_fips IS NULL OR sector IS NULL);

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
-- T6: the keyless ECHO Exporter feed itself omits REGISTRY_ID on a small
-- fraction of rows and there is no other natural key to substitute. Threshold
-- allows up to 1% so a real regression (a mapping break, not source-inherent
-- gaps) still fails the check.
INSERT INTO dq_results
SELECT 'environment', 'epa_facilities', 'T6_pk_nulls',
  CASE WHEN pct <= 1.0 THEN 'pass' ELSE 'fail' END, pct, 1.0,
  'Percent of rows with NULL registry_id (source-inherent, expect <=1%)'
FROM (SELECT 100.0 * COUNT(*) FILTER (WHERE registry_id IS NULL) / COUNT(*) AS pct
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/epa_facilities', allow_moved_paths := true));
-- T7: violation-frequency rollups — quarter counts are bounded by the 12-quarter
-- window; the exceedance count is a non-negative count (null when none)
INSERT INTO dq_results
SELECT 'environment', 'epa_facilities', 'T7_qtrs_range',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'qtrs_in_noncompliance values outside 0..12'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/epa_facilities', allow_moved_paths := true)
  WHERE qtrs_in_noncompliance NOT BETWEEN 0 AND 12
     OR caa_qtrs_in_noncompliance NOT BETWEEN 0 AND 12
     OR cwa_qtrs_in_noncompliance NOT BETWEEN 0 AND 12
     OR rcra_qtrs_in_noncompliance NOT BETWEEN 0 AND 12
     OR cwa_effluent_exceedance_count < 0);
-- T7: the CWA effluent exceedance count is populated for a real slice of the
-- CWA universe (~33k of 3.2M facilities in the full snapshot; ~1k in a 100k DQ sample)
INSERT INTO dq_results
SELECT 'environment', 'epa_facilities', 'T7_exceedance_coverage',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END, n, 500, 'Facilities with a CWA effluent exceedance count (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/epa_facilities', allow_moved_paths := true)
  WHERE cwa_effluent_exceedance_count IS NOT NULL);

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
