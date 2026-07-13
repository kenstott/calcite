-- dq-lookback: 1
-- U.S. Transportation Data Quality Checks
-- Schema: transport
-- Tables: vehicle_recalls, safety_complaints, fatal_crashes, airline_ontime,
--         airports, transit_ridership, t100_segments, vehicle_registrations
-- All tables are Iceberg; reads via iceberg_scan (single-nested path).
-- T4/T5 exclude partition columns ('type' for all; also 'year'/'month' where present).
-- safety_complaints (dqRowLimit 200000), airline_ontime (dqRowLimit 100000) sample in DQ mode.

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
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
