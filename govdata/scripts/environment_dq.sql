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

INSERT INTO dq_results
SELECT 'environment', 'water_withdrawals', 'T7_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (county_fips, year, sector) keys'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT county_fips, year, sector
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_withdrawals', allow_moved_paths := true)
  GROUP BY county_fips, year, sector HAVING COUNT(*) > 1));
INSERT INTO dq_results
SELECT 'environment', 'water_withdrawals', 'T8_year_completeness',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Census years with fewer than 3100 counties or other than 7 sectors'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_withdrawals', allow_moved_paths := true)
  GROUP BY year HAVING COUNT(DISTINCT county_fips) < 3100 OR COUNT(DISTINCT sector) <> 7));

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
-- TABLE: drinking_water_sample_results (partition cols: type)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_sample_results', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_sample_results', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_sample_results', 'T2_row_count',
  CASE WHEN n >= 1000000 THEN 'pass' ELSE 'fail' END, n, 1000000, 'Expected >=1M nitrate sample rows (SYR4 file has 1.40M)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_sample_results', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_sample_results', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_sample_results', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_sample_results', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));
-- contaminant_code / contaminant_name are expected constants: the table loads the nitrate file only.
-- result_unit is UG/L for every detected result (non-detects have no unit), so it is constant by design.
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_sample_results', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_sample_results', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'contaminant_code', 'contaminant_name', 'result_unit')));
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_sample_results', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL pwsid / six_year_id / sample_date rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_sample_results', allow_moved_paths := true) WHERE pwsid IS NULL OR six_year_id IS NULL OR sample_date IS NULL);
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_sample_results', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with negative result_value or sample year outside 2012-2019'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_sample_results', allow_moved_paths := true) WHERE result_value < 0 OR year < 2012 OR year > 2019);
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_sample_results', 'T7_nitrate_magnitude',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Detected samples above the 10 mg/L (10000 UG/L) nitrate MCL (EPA SYR4 reports exceedances)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_sample_results', allow_moved_paths := true) WHERE detected AND result_unit = 'UG/L' AND result_value > 10000);
INSERT INTO dq_results
SELECT 'environment', 'drinking_water_sample_results', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (pwsid, water_facility_id, sampling_point_id, sample_date, six_year_id) rows beyond the first'
FROM (SELECT COALESCE(SUM(c - 1), 0) AS n FROM (SELECT COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/drinking_water_sample_results', allow_moved_paths := true) GROUP BY pwsid, water_facility_id, sampling_point_id, sample_date, six_year_id HAVING COUNT(*) > 1));

-- ------------------------------------------------------------
-- TABLE: ucmr_results (partition cols: type)
-- sampling_point_type (EP) and monitoring_requirement (AM) are constant across the UCMR 5 file by design.
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'ucmr_results', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'ucmr_results', 'T2_row_count',
  CASE WHEN n >= 1500000 THEN 'pass' ELSE 'fail' END, n, 1500000, 'Expected >=1.5M UCMR 5 results (EPA file has 1.99M at the Oct 2026 release)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'ucmr_results', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'ucmr_results', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'sampling_point_type', 'monitoring_requirement')));
INSERT INTO dq_results
SELECT 'environment', 'ucmr_results', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL pwsid / sampling_point_id / sample_id / contaminant / collection_date rows (facility_id is excluded: the one facility EPA ids as the literal NA reads as null)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true) WHERE pwsid IS NULL OR sampling_point_id IS NULL OR sample_id IS NULL OR contaminant IS NULL OR collection_date IS NULL);
INSERT INTO dq_results
SELECT 'environment', 'ucmr_results', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with negative result_value, a detection without a value, a non-detect with a value, or year outside 2023-2030'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true) WHERE result_value < 0 OR (detected AND result_value IS NULL) OR (NOT detected AND result_value IS NOT NULL) OR year < 2023 OR year > 2030);
INSERT INTO dq_results
SELECT 'environment', 'ucmr_results', 'T7_pfas_magnitude',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Detected PFOA results (EPA UCMR 5 data summary reports several thousand PFOA detections)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true) WHERE contaminant = 'PFOA' AND detected);
INSERT INTO dq_results
SELECT 'environment', 'ucmr_results', 'T7_contaminant_count',
  CASE WHEN n = 30 THEN 'pass' ELSE 'fail' END, n, 30, 'UCMR 5 lists 29 PFAS plus lithium'
FROM (SELECT COUNT(DISTINCT contaminant) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'ucmr_results', 'T8_pk_duplication',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (pwsid, facility_id, sampling_point_id, sample_id, contaminant, collection_date) rows beyond the first'
FROM (SELECT COALESCE(SUM(c - 1), 0) AS n FROM (SELECT COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ucmr_results', allow_moved_paths := true) GROUP BY pwsid, facility_id, sampling_point_id, sample_id, contaminant, collection_date HAVING COUNT(*) > 1));

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
INSERT INTO dq_results
SELECT 'environment', 'water_quality_samples', 'T7_non_finite_result_value',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'result_value rows holding Infinity or NaN'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/water_quality_samples', allow_moved_paths := true) WHERE NOT isfinite(result_value));

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

-- ------------------------------------------------------------
-- TABLE: ocean_sst_monthly (partition cols: type, year, month)
-- 11 US LMEs x months; a DQ window of one year yields <= 132 rows.
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'ocean_sst_monthly', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ocean_sst_monthly', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'ocean_sst_monthly', 'T2_row_count',
  CASE WHEN n >= 11 THEN 'pass' ELSE 'fail' END, n, 11, 'Expected >=11 rows (one published month x 11 LMEs)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ocean_sst_monthly', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ocean_sst_monthly', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'ocean_sst_monthly', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ocean_sst_monthly', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'month')));
INSERT INTO dq_results
SELECT 'environment', 'ocean_sst_monthly', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ocean_sst_monthly', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'month')));
INSERT INTO dq_results
SELECT 'environment', 'ocean_sst_monthly', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL PK rows (lme_number, year, month)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ocean_sst_monthly', allow_moved_paths := true) WHERE lme_number IS NULL OR year IS NULL OR month IS NULL);
INSERT INTO dq_results
SELECT 'environment', 'ocean_sst_monthly', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'mean_sst_c outside [-2,35], |anomaly| > 10, or cell_count <= 0'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ocean_sst_monthly', allow_moved_paths := true)
  WHERE mean_sst_c < -2 OR mean_sst_c > 35 OR ABS(mean_sst_anomaly_c) > 10 OR cell_count <= 0);
INSERT INTO dq_results
SELECT 'environment', 'ocean_sst_monthly', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (lme_number, year, month) rows'
FROM (SELECT COUNT(*) AS n FROM (SELECT lme_number, year, month FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/ocean_sst_monthly', allow_moved_paths := true)
  GROUP BY lme_number, year, month HAVING COUNT(*) > 1));

-- ------------------------------------------------------------
-- TABLE: attains_state_use_summary (partition cols: type, reporting_cycle, organization_id)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'attains_state_use_summary', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_state_use_summary', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'attains_state_use_summary', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 rows (many states x water types x uses)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_state_use_summary', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_state_use_summary', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'attains_state_use_summary', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_state_use_summary', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'reporting_cycle', 'organization_id')));
INSERT INTO dq_results
SELECT 'environment', 'attains_state_use_summary', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_state_use_summary', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'reporting_cycle', 'organization_id')));
INSERT INTO dq_results
SELECT 'environment', 'attains_state_use_summary', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL PK rows (organization_id, reporting_cycle, water_type, use_name, attainment_category)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_state_use_summary', allow_moved_paths := true) WHERE organization_id IS NULL OR reporting_cycle IS NULL OR water_type IS NULL OR use_name IS NULL OR attainment_category IS NULL);
INSERT INTO dq_results
SELECT 'environment', 'attains_state_use_summary', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'size < 0, unit_count < 0, units not Miles/Acres/Square Miles, or cycle odd/out of range'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_state_use_summary', allow_moved_paths := true)
  WHERE size < 0 OR unit_count < 0 OR units NOT IN ('Miles','Acres','Square Miles') OR reporting_cycle % 2 <> 0 OR reporting_cycle < 2018);
INSERT INTO dq_results
SELECT 'environment', 'attains_state_use_summary', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (organization_id, reporting_cycle, water_type, use_name, attainment_category) rows'
FROM (SELECT COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_state_use_summary', allow_moved_paths := true)
  GROUP BY organization_id, reporting_cycle, water_type, use_name, attainment_category HAVING COUNT(*) > 1));

-- ------------------------------------------------------------
-- TABLE: attains_cause_summary (partition cols: type, reporting_cycle, organization_id)
-- ------------------------------------------------------------
INSERT INTO dq_results
SELECT 'environment', 'attains_cause_summary', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_cause_summary', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'attains_cause_summary', 'T2_row_count',
  CASE WHEN n >= 100 THEN 'pass' ELSE 'fail' END, n, 100, 'Expected >=100 rows (many states x water types x uses)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_cause_summary', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_cause_summary', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'attains_cause_summary', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_cause_summary', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'reporting_cycle', 'organization_id')));
INSERT INTO dq_results
SELECT 'environment', 'attains_cause_summary', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_cause_summary', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'reporting_cycle', 'organization_id')));
INSERT INTO dq_results
SELECT 'environment', 'attains_cause_summary', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL PK rows (organization_id, reporting_cycle, water_type, use_name, parameter_group, attainment_category)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_cause_summary', allow_moved_paths := true) WHERE organization_id IS NULL OR reporting_cycle IS NULL OR water_type IS NULL OR use_name IS NULL OR parameter_group IS NULL OR attainment_category IS NULL);
INSERT INTO dq_results
SELECT 'environment', 'attains_cause_summary', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'size < 0, unit_count < 0, units not Miles/Acres/Square Miles, or cycle odd/out of range'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_cause_summary', allow_moved_paths := true)
  WHERE size < 0 OR unit_count < 0 OR units NOT IN ('Miles','Acres','Square Miles') OR reporting_cycle % 2 <> 0 OR reporting_cycle < 2018);
INSERT INTO dq_results
SELECT 'environment', 'attains_cause_summary', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (organization_id, reporting_cycle, water_type, use_name, parameter_group, attainment_category) rows'
FROM (SELECT COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/attains_cause_summary', allow_moved_paths := true)
  GROUP BY organization_id, reporting_cycle, water_type, use_name, parameter_group, attainment_category HAVING COUNT(*) > 1));

-- ===== npdes_permits =====
INSERT INTO dq_results
SELECT 'environment', 'npdes_permits', 'T1_exists',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'npdes_permits', 'T2_row_count',
  CASE WHEN n >= 1000000 THEN 'pass' ELSE 'fail' END, n, 1000000, 'Expected >=1000000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'npdes_permits', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'npdes_permits', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'npdes_permits', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL PK rows (permit_number, state_abbr)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true) WHERE permit_number IS NULL OR state_abbr IS NULL);
INSERT INTO dq_results
SELECT 'environment', 'npdes_permits', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Bad state code, unknown status, or expiration before 1960 (source sentinel 0001-01-01 on 13 permits allowed)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true) WHERE LENGTH(state_abbr) <> 2 OR permit_status_code NOT IN ('EFF','ADC','EXP','TRM','PND','RET','NON','DEN') OR (expiration_date < DATE '1960-01-01' AND expiration_date <> DATE '0001-01-01'));
INSERT INTO dq_results
SELECT 'environment', 'npdes_permits', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (permit_number, version_number) rows'
FROM (SELECT COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true) GROUP BY permit_number, version_number HAVING COUNT(*) > 1));
-- Michigan active CAFO general-permit coverages, current permit versions (226 EFF+ADC at 2026-11-23)
INSERT INTO dq_results
SELECT 'environment', 'npdes_permits', 'T9_michigan_cafo',
  CASE WHEN n BETWEEN 150 AND 400 THEN 'pass' ELSE 'fail' END, n, 270, 'Active Michigan MIG010000 coverages (EFF+ADC) expected 150-400'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true) WHERE version_number = 0 AND master_permit_number = 'MIG010000' AND permit_status_code IN ('EFF','ADC'));
-- View npdes_cafo_permits promises one row per permit: same selection and joins as the view
INSERT INTO dq_results
SELECT 'environment', 'npdes_permits', 'T10_cafo_view_one_row_per_permit',
  CASE WHEN n > 0 AND n = d THEN 'pass' ELSE 'fail' END, n - d, 0, 'Rows beyond one per permit in the view selection (fails also when empty)'
FROM (SELECT COUNT(*) AS n, COUNT(DISTINCT p.permit_number) AS d
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permits', allow_moved_paths := true) p
  LEFT JOIN iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_facilities', allow_moved_paths := true) f ON f.permit_number = p.permit_number
  WHERE p.version_number = 0
    AND (p.permit_number IN (SELECT permit_number FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permit_components', allow_moved_paths := true) WHERE component_type_code = 'CAF')
         OR p.master_permit_number = 'MIG010000'));

-- ===== npdes_facilities =====
INSERT INTO dq_results
SELECT 'environment', 'npdes_facilities', 'T1_exists',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_facilities', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'npdes_facilities', 'T2_row_count',
  CASE WHEN n >= 1000000 THEN 'pass' ELSE 'fail' END, n, 1000000, 'Expected >=1000000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_facilities', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_facilities', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'npdes_facilities', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_facilities', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));
-- impaired_waters is a constant in the source (only the value '303(D) Listed' or null): excluded from T5.
INSERT INTO dq_results
SELECT 'environment', 'npdes_facilities', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_facilities', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'impaired_waters')));
INSERT INTO dq_results
SELECT 'environment', 'npdes_facilities', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL PK rows (permit_number)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_facilities', allow_moved_paths := true) WHERE permit_number IS NULL);
INSERT INTO dq_results
SELECT 'environment', 'npdes_facilities', 'T7_value_ranges',
  CASE WHEN n <= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Latitude/longitude out of range (source has ~220 bad geocodes in 1.26M rows; fail above 1000)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_facilities', allow_moved_paths := true) WHERE (latitude IS NOT NULL AND (latitude NOT BETWEEN -15 AND 75 OR longitude NOT BETWEEN -180 AND 180)));

-- ===== npdes_permit_components =====
INSERT INTO dq_results
SELECT 'environment', 'npdes_permit_components', 'T1_exists',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permit_components', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'npdes_permit_components', 'T2_row_count',
  CASE WHEN n >= 100000 THEN 'pass' ELSE 'fail' END, n, 100000, 'Expected >=100000 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permit_components', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permit_components', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'npdes_permit_components', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permit_components', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'npdes_permit_components', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permit_components', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'npdes_permit_components', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL PK rows (permit_number, component_type_code)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permit_components', allow_moved_paths := true) WHERE permit_number IS NULL OR component_type_code IS NULL);
INSERT INTO dq_results
SELECT 'environment', 'npdes_permit_components', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Unknown component code'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permit_components', allow_moved_paths := true) WHERE component_type_code NOT IN ('SWC','CSO','SWI','SWS','POT','PRE','CAF','BIO','SWM'));
INSERT INTO dq_results
SELECT 'environment', 'npdes_permit_components', 'T9_cafo_count',
  CASE WHEN n BETWEEN 3000 AND 15000 THEN 'pass' ELSE 'fail' END, n, 6700, 'CAF component rows expected ~6,700'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_permit_components', allow_moved_paths := true) WHERE component_type_code = 'CAF');

-- ===== npdes_cafo_status =====
INSERT INTO dq_results
SELECT 'environment', 'npdes_cafo_status', 'T1_exists',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'npdes_cafo_status', 'T2_row_count',
  CASE WHEN n >= 750 THEN 'pass' ELSE 'fail' END, n, 750, 'Expected >=750 rows (14+ reports of 54+ areas)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true));
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true) LIMIT 3;
INSERT INTO dq_results
SELECT 'environment', 'npdes_cafo_status', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'npdes_cafo_status', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));
INSERT INTO dq_results
SELECT 'environment', 'npdes_cafo_status', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL PK rows (report_year, area_name)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true) WHERE report_year IS NULL OR area_name IS NULL);
INSERT INTO dq_results
SELECT 'environment', 'npdes_cafo_status', 'T7_value_ranges',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Region outside 1-10, count out of range, or state row without state_abbr'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true) WHERE epa_region NOT BETWEEN 1 AND 10 OR total_cafos NOT BETWEEN 0 AND 10000 OR permitted_cafos NOT BETWEEN 0 AND 10000 OR (state_abbr IS NULL AND area_name NOT LIKE 'Indian Country%'));
INSERT INTO dq_results
SELECT 'environment', 'npdes_cafo_status', 'T8_pk_duplicates',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (report_year, area_name)'
FROM (SELECT COUNT(*) AS n FROM (SELECT report_year, area_name FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true) GROUP BY 1, 2 HAVING COUNT(*) > 1));
INSERT INTO dq_results
SELECT 'environment', 'npdes_cafo_status', 'T9_year_coverage',
  CASE WHEN n >= 14 AND lo = 2011 AND hi - lo + 1 = n THEN 'pass' ELSE 'fail' END, n, 14, 'One report per year from 2011 with no gap'
FROM (SELECT COUNT(DISTINCT report_year) AS n, MIN(report_year) AS lo, MAX(report_year) AS hi FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true));
INSERT INTO dq_results
SELECT 'environment', 'npdes_cafo_status', 'T10_michigan_2024',
  CASE WHEN n = 1 THEN 'pass' ELSE 'fail' END, n, 1, 'Michigan end-2024 published as 292 total, 278 permitted'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/environment/npdes_cafo_status', allow_moved_paths := true) WHERE report_year = 2024 AND state_abbr = 'MI' AND total_cafos = 292 AND permitted_cafos = 278);

SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results ORDER BY schema, tbl, test;
