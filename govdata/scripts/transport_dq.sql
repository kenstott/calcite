-- dq-lookback: 1
-- U.S. Transportation Data Quality Checks
-- Schema: transport
-- Tables: vehicle_recalls, safety_complaints, fatal_crashes, airline_ontime,
--         airports, transit_ridership, t100_segments, vehicle_registrations
-- All tables are Iceberg; reads via iceberg_scan (single-nested path).
-- T4/T5 exclude partition columns ('type' for all; also 'year'/'month' where present).
-- safety_complaints (dqRowLimit 200000), airline_ontime (dqRowLimit 100000),
-- rail_service_performance (dqRowLimit 50000) sample in DQ mode.

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
-- TABLE: vehicle_recalls (NHTSA Socrata snapshot; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'vehicle_recalls', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_recalls', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'vehicle_recalls', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 recall campaigns'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_recalls', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_recalls', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'vehicle_recalls', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_recalls', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'vehicle_recalls', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_recalls', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'vehicle_recalls', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL nhtsa_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_recalls', allow_moved_paths := true) WHERE nhtsa_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: safety_complaints (NHTSA flat file snapshot; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'safety_complaints', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/safety_complaints', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'safety_complaints', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 complaint rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/safety_complaints', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/safety_complaints', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'safety_complaints', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/safety_complaints', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'safety_complaints', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/safety_complaints', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'safety_complaints', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL cmplid rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/safety_complaints', allow_moved_paths := true) WHERE cmplid IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: fatal_crashes (FARS; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'fatal_crashes', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/fatal_crashes', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'fatal_crashes', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 fatal-crash rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/fatal_crashes', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/fatal_crashes', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'fatal_crashes', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/fatal_crashes', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 'fatal_crashes', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/fatal_crashes', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 'fatal_crashes', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL st_case rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/fatal_crashes', allow_moved_paths := true) WHERE st_case IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: airline_ontime (BTS on-time; partition cols: type, year, month)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'airline_ontime', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airline_ontime', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'airline_ontime', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 flight rows (DQ-sampled)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airline_ontime', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airline_ontime', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'airline_ontime', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airline_ontime', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'month')));

INSERT INTO dq_results
SELECT 'transport', 'airline_ontime', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airline_ontime', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'month')));

INSERT INTO dq_results
SELECT 'transport', 'airline_ontime', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL origin rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airline_ontime', allow_moved_paths := true) WHERE origin IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: airports (FAA ArcGIS snapshot; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'airports', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airports', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'airports', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END, n, 5000, 'Expected >=5000 airport facilities'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airports', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airports', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'airports', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airports', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'airports', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airports', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'airports', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL arpt_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/airports', allow_moved_paths := true) WHERE arpt_id IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: transit_ridership (FTA Socrata; partition cols: type, year, month)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'transit_ridership', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/transit_ridership', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'transit_ridership', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 agency-mode-month rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/transit_ridership', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/transit_ridership', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'transit_ridership', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/transit_ridership', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year', 'month')));

INSERT INTO dq_results
SELECT 'transport', 'transit_ridership', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/transit_ridership', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year', 'month')));

INSERT INTO dq_results
SELECT 'transport', 'transit_ridership', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL agency_mode_tos_date rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/transit_ridership', allow_moved_paths := true) WHERE agency_mode_tos_date IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: t100_segments (BTS T-100; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 't100_segments', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/t100_segments', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 't100_segments', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 segment rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/t100_segments', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/t100_segments', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 't100_segments', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/t100_segments', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 't100_segments', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/t100_segments', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 't100_segments', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL origin rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/t100_segments', allow_moved_paths := true) WHERE origin IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: vehicle_registrations (FHWA MV-1; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'vehicle_registrations', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_registrations', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'vehicle_registrations', 'T2_row_count',
  CASE WHEN n >= 50 THEN 'pass' ELSE 'fail' END, n, 50, 'Expected >=50 state-year registration rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_registrations', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_registrations', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'vehicle_registrations', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_registrations', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 'vehicle_registrations', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_registrations', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 'vehicle_registrations', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_fips rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/vehicle_registrations', allow_moved_paths := true) WHERE state_fips IS NULL);

-- ─────────────────────────────────────────────────────────────
-- TABLE: pavement_roughness (FHWA HM-64; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'pavement_roughness', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/pavement_roughness', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'pavement_roughness', 'T2_row_count',
  CASE WHEN n >= 500 THEN 'pass' ELSE 'fail' END, n, 500,
  'Expected >=500 rows (51 states/territories x 7 area/road-class combos x 2 years)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/pavement_roughness', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/pavement_roughness', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'pavement_roughness', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/pavement_roughness', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 'pavement_roughness', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/pavement_roughness', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 'pavement_roughness', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_fips/area_type/road_class rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/pavement_roughness', allow_moved_paths := true)
  WHERE state_fips IS NULL OR area_type IS NULL OR road_class IS NULL);

INSERT INTO dq_results
SELECT 'transport', 'pavement_roughness', 'T7_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate (year, state_fips, area_type, road_class) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT year, state_fips, area_type, road_class, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/pavement_roughness', allow_moved_paths := true)
  GROUP BY year, state_fips, area_type, road_class HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'transport', 'pavement_roughness', 'T8_expected_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'area_type values outside {rural, urban}'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/pavement_roughness', allow_moved_paths := true)
  WHERE area_type NOT IN ('rural', 'urban'));

-- ─────────────────────────────────────────────────────────────
-- TABLE: usace_locks (USACE Waterway Locks; partition col: type)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'usace_locks', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/usace_locks', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'usace_locks', 'T2_row_count',
  CASE WHEN n >= 200 THEN 'pass' ELSE 'fail' END, n, 200, 'Expected >=200 navigation lock chambers nationally'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/usace_locks', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/usace_locks', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'usace_locks', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/usace_locks', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'usace_locks', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/usace_locks', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'usace_locks', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL lock_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/usace_locks', allow_moved_paths := true) WHERE lock_id IS NULL);

INSERT INTO dq_results
SELECT 'transport', 'usace_locks', 'T7_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate lock_id rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT lock_id, COUNT(*) AS c FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/usace_locks', allow_moved_paths := true)
  GROUP BY lock_id HAVING COUNT(*) > 1));

INSERT INTO dq_results
SELECT 'transport', 'usace_locks', 'T8_expected_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'year_open outside plausible [1800, 2026] range'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/usace_locks', allow_moved_paths := true)
  WHERE year_open IS NOT NULL AND (year_open < 1800 OR year_open > 2026));

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
-- ============================================================================
-- T1/T2 — tables previously absent from this file entirely.
-- A table with no checks emits no dq rows, which reads as healthy rather than as
-- untested; that is how four zero-row geo tables went unnoticed. Existence +
-- row_count only — deliberately minimal, to be deepened per table.
-- ============================================================================

INSERT INTO dq_results
WITH counts AS (
  SELECT 'cfs_shipments'           AS tbl, (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/cfs_shipments', allow_moved_paths := true) LIMIT 1)) AS n
  UNION ALL
  SELECT 'faa_aircraft_master'    , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/faa_aircraft_master', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'faa_aircraft_reference' , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/faa_aircraft_reference', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'faa_engine_reference'   , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/faa_engine_reference', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'fmcsa_carriers'         , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/fmcsa_carriers', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'ntsb_aviation_accidents', (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/ntsb_aviation_accidents', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'bridges'                , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bridges', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'cfs_sctg_ref'           , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/cfs_sctg_ref', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'cfs_mode_ref'           , (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/cfs_mode_ref', allow_moved_paths := true) LIMIT 1))
)
SELECT 'transport', tbl, 'existence',
       CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
       n, 1,
       CASE WHEN n > 0 THEN 'readable' ELSE 'NO ROWS — table unreadable or never written' END
FROM counts;

-- ─────────────────────────────────────────────────────────────
-- TABLE: bridges (FHWA NBI; partition cols: type, year; dqRowLimit 20000 — single-fetch
-- bulk table, so the DQ sample is the first N rows of one year's national file, not a
-- representative cross-section — see the dqRowLimit comment on the table in the schema)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'bridges', 'T2_row_count',
  CASE WHEN n >= 1000 THEN 'pass' ELSE 'fail' END, n, 1000, 'Expected >=1000 bridge rows (DQ-sampled, single fetch)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bridges', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bridges', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'bridges', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bridges', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 'bridges', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL state_code or structure_number rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bridges', allow_moved_paths := true) WHERE state_code IS NULL OR structure_number IS NULL);

INSERT INTO dq_results
SELECT 'transport', 'bridges', 'T7_condition_code_domain',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END, n, 0, 'deck_condition values outside 0-9 or N'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bridges', allow_moved_paths := true)
      WHERE deck_condition IS NOT NULL AND NOT regexp_matches(deck_condition, '^[0-9N]$'));

-- Regression guard: decimal_latitude/decimal_longitude decode NBI's packed DDMMSS.ss /
-- DDDMMSS.ss fields with length-relative substring extraction specifically because a
-- fixed-position split silently misreads once the upstream CSV staging strips a source
-- column's leading zero (caught live: -873.68 instead of -87.57 for an Alabama bridge).
-- Bound to plausible CONUS+territories coordinates. WARN not FAIL: verified live that a
-- small fraction of implausible values (e.g. LONG_017="865391200" for an Alabama county
-- bridge, structure 019930) are genuinely bad in NBI's own source data, not a decode bug
-- — the raw field itself has no valid degrees/minutes/seconds split. ~4/20000 in a DQ
-- sample (0.02%) is consistent with known NBI location-field QA gaps across state DOT
-- submissions; a jump far above that ratio would indicate a real regression.
INSERT INTO dq_results
SELECT 'transport', 'bridges', 'T7_latlong_plausible',
  CASE WHEN n = 0 THEN 'pass' ELSE 'warn' END, n, 0,
  'decimal_latitude/decimal_longitude outside plausible US range (lat 13-72, lon -180..-64) — expect a small count from source data noise, not zero'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bridges', allow_moved_paths := true)
      WHERE (decimal_latitude IS NOT NULL AND (decimal_latitude < 13 OR decimal_latitude > 72))
         OR (decimal_longitude IS NOT NULL AND (decimal_longitude < -180 OR decimal_longitude > -64)));


-- ─────────────────────────────────────────────────────────────
-- TABLE: cfs_sctg_ref / cfs_mode_ref (CFS PUF Data Users Guide appendix code lists; static,
-- one PDF fetch — see the table comments in the schema for why there is no CSV/XLSX source)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'cfs_sctg_ref', 'T2_row_count',
  CASE WHEN n = 43 THEN 'pass' ELSE 'fail' END, n, 43,
  'Fixed 2017 CFS PUF appendix — exactly 43 SCTG codes (41 detailed + "43" mixed freight + "00" suppressed)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/cfs_sctg_ref', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'cfs_mode_ref', 'T2_row_count',
  CASE WHEN n = 21 THEN 'pass' ELSE 'fail' END, n, 21,
  'Fixed 2017 CFS PUF appendix — exactly 21 mode codes (20 detailed + "00" suppressed)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/cfs_mode_ref', allow_moved_paths := true));

-- Join coverage: every non-null, non-suppressed cfs_shipments.sctg_code/mode_code that is NOT
-- a confidentiality-collapsed "NN-NN" range should resolve against the ref table. WARN not
-- FAIL — a scoped/partial cfs_shipments DQ sample can legitimately miss a rare code.
INSERT INTO dq_results
SELECT 'transport', 'cfs_shipments', 'T8_sctg_code_resolves',
  CASE WHEN n_unresolved = 0 THEN 'pass' ELSE 'warn' END, n_unresolved, 0,
  'sctg_code values (excluding NN-NN collapsed ranges) with no match in cfs_sctg_ref'
FROM (
  SELECT COUNT(*) AS n_unresolved
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/cfs_shipments', allow_moved_paths := true) s
  WHERE s.sctg_code IS NOT NULL AND s.sctg_code NOT LIKE '%-%'
    AND NOT EXISTS (
      SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/cfs_sctg_ref', allow_moved_paths := true) r
      WHERE r.sctg_code = s.sctg_code)
);

INSERT INTO dq_results
SELECT 'transport', 'cfs_shipments', 'T8_mode_code_resolves',
  CASE WHEN n_unresolved = 0 THEN 'pass' ELSE 'warn' END, n_unresolved, 0,
  'mode_code values with no match in cfs_mode_ref'
FROM (
  SELECT COUNT(*) AS n_unresolved
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/cfs_shipments', allow_moved_paths := true) s
  WHERE s.mode_code IS NOT NULL
    AND NOT EXISTS (
      SELECT 1 FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/cfs_mode_ref', allow_moved_paths := true) r
      WHERE r.mode_code = s.mode_code)
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: bts_port_teu (BTS Port Performance Program; static, Jan 2019-Oct 2022, not currently
-- updated — see the table comment in the schema)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'bts_port_teu', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bts_port_teu', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'bts_port_teu', 'T2_row_count',
  CASE WHEN n = 414 THEN 'pass' ELSE 'fail' END, n, 414,
  'Fixed static series — exactly 46 months x 9 ports = 414 rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bts_port_teu', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'bts_port_teu', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL report_date/port_code rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bts_port_teu', allow_moved_paths := true)
      WHERE report_date IS NULL OR port_code IS NULL);

INSERT INTO dq_results
SELECT 'transport', 'bts_port_teu', 'T7_date_range',
  CASE WHEN miny = '2019-01-01' AND maxy = '2022-10-01' THEN 'pass' ELSE 'fail' END,
  0, 0, 'Expected exactly 2019-01-01 through 2022-10-01: got ' || miny || ' to ' || maxy
FROM (SELECT MIN(report_date)::VARCHAR AS miny, MAX(report_date)::VARCHAR AS maxy
      FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bts_port_teu', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'bts_port_teu', 'T7_teu_plausible',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'warn' END, bad, 0,
  'teu outside plausible [1000, 2000000] range for a single port-month'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/bts_port_teu', allow_moved_paths := true)
      WHERE teu IS NOT NULL AND (teu < 1000 OR teu > 2000000));

-- ─────────────────────────────────────────────────────────────
-- TABLE: phmsa_hazardous_liquid_incidents (PHMSA F 7000-1 accident reports, 2010-present, snapshot)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_incidents', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_incidents', allow_moved_paths := true));

-- T2: row_count. Confirmed live 5 Sep 2026: 5,850 accident reports 2010-present.
INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_incidents', 'T2_row_count',
  CASE WHEN n >= 3000 THEN 'pass' ELSE 'fail' END, n, 3000, 'Expected >=3,000 reports (5,850 confirmed live 5 Sep 2026)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_incidents', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_incidents', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_incidents', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_incidents', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_incidents', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_incidents', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

-- T6: pk_nulls + pk_dupes on report_number.
INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_incidents', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL report_number rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_incidents', allow_moved_paths := true)
  WHERE report_number IS NULL);

INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_incidents', 'T6_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Duplicate report_number rows'
FROM (
  SELECT COUNT(*) AS n FROM (
    SELECT report_number, COUNT(*) AS c
    FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_incidents', allow_moved_paths := true)
    GROUP BY report_number HAVING COUNT(*) > 1
  )
);

-- T7: activity_year within the documented 2010-present window.
INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_incidents', 'T7_year_range',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with activity_year outside [2010, current year]'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_incidents', allow_moved_paths := true)
  WHERE activity_year IS NOT NULL AND (activity_year < 2010 OR activity_year > YEAR(CURRENT_DATE)));

-- ─────────────────────────────────────────────────────────────
-- TABLE: phmsa_hazardous_liquid_mileage (PHMSA F 7000-1.1 annual report Part A-E, 2017-2024, snapshot)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_mileage', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_mileage', allow_moved_paths := true));

-- T2: row_count. ~800-900 operators/commodities per year x 8 years (2017-2024).
INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_mileage', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END, n, 5000, 'Expected >=5,000 rows across 2017-2024 (~800-900 operator/commodity rows per year)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_mileage', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_mileage', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_mileage', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_mileage', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_mileage', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_mileage', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type')));

INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_mileage', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'NULL operator_id rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_mileage', allow_moved_paths := true)
  WHERE operator_id IS NULL);

-- T7: report_year within the documented 2017-2024 window (Part A-E CSVs only; 2010-2016 XLSX-only years not ingested).
INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_mileage', 'T7_year_range',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0, 'Rows with report_year outside [2017, 2024]'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_mileage', allow_moved_paths := true)
  WHERE report_year IS NOT NULL AND (report_year < 2017 OR report_year > 2024));

-- T8: total_miles is populated and non-negative for the vast majority of rows (the
-- per-mile normalization field this table exists to carry).
INSERT INTO dq_results
SELECT 'transport', 'phmsa_hazardous_liquid_mileage', 'T8_total_miles_populated',
  CASE WHEN pct >= 90.0 THEN 'pass' ELSE 'warn' END, pct, 90.0,
  'Percent of rows with a non-null, non-negative total_miles'
FROM (
  SELECT 100.0 * SUM(CASE WHEN total_miles IS NOT NULL AND total_miles >= 0 THEN 1 ELSE 0 END) / COUNT(*) AS pct
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/phmsa_hazardous_liquid_mileage', allow_moved_paths := true)
);

-- ─────────────────────────────────────────────────────────────
-- TABLE: rail_service_performance (STB EP 724; partition cols: type, year)
-- ─────────────────────────────────────────────────────────────
INSERT INTO dq_results
SELECT 'transport', 'rail_service_performance', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END, n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/rail_service_performance', allow_moved_paths := true));

INSERT INTO dq_results
SELECT 'transport', 'rail_service_performance', 'T2_row_count',
  CASE WHEN n >= 5000 THEN 'pass' ELSE 'fail' END, n, 5000,
  'Expected >=5000 rows (dqRowLimit caps each year at 50000; years 2017-present)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/rail_service_performance', allow_moved_paths := true));

SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/rail_service_performance', allow_moved_paths := true) LIMIT 3;

INSERT INTO dq_results
SELECT 'transport', 'rail_service_performance', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/rail_service_performance', allow_moved_paths := true))
    WHERE null_percentage = 100.0 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 'rail_service_performance', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END, cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/rail_service_performance', allow_moved_paths := true))
    WHERE approx_unique <= 1 AND column_name NOT IN ('type', 'year')));

INSERT INTO dq_results
SELECT 'transport', 'rail_service_performance', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'NULL railroad_mark/measure_name_analytics/sub_measure/report_period_start_date rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/rail_service_performance', allow_moved_paths := true)
  WHERE railroad_mark IS NULL OR measure_name_analytics IS NULL OR sub_measure IS NULL
    OR report_period_start_date IS NULL);

INSERT INTO dq_results
SELECT 'transport', 'rail_service_performance', 'T7_pk_dupes',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'Duplicate (railroad_mark, measure_name_analytics, sub_measure, report_period_start_date) rows'
FROM (SELECT COUNT(*) AS n FROM (
  SELECT railroad_mark, measure_name_analytics, sub_measure, report_period_start_date, COUNT(*) AS c
  FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/rail_service_performance', allow_moved_paths := true)
  GROUP BY railroad_mark, measure_name_analytics, sub_measure, report_period_start_date HAVING COUNT(*) > 1));

-- T8: railroad_mark restricted to the 8 Class I carriers STB's EP724 program covers.
INSERT INTO dq_results
SELECT 'transport', 'rail_service_performance', 'T8_expected_values',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END, n, 0,
  'railroad_mark values outside {BNSF, CN, CP, CPKC, CSXT, KCS, NS, UP}'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/transport/rail_service_performance', allow_moved_paths := true)
  WHERE railroad_mark NOT IN ('BNSF', 'CN', 'CP', 'CPKC', 'CSXT', 'KCS', 'NS', 'UP'));

SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
