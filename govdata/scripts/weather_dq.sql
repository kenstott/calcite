-- dq-lookback: 1
-- weather_dq.sql — Weather Schema Data Quality
-- Follows the 7-test reference template established in edu_dq.sql.
-- Usage: source .env.prod && envsubst < scripts/weather_dq.sql | duckdb
-- Output: structured dq_results table + per-table summary + schema-level pass/fail

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';
SET unsafe_enable_version_guessing=true;

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
SELECT 'weather', tbl, 'existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END,
  CAST(n AS VARCHAR), '1',
  CASE WHEN n > 0 THEN 'readable and non-empty' ELSE 'accessible but empty' END
FROM (
  SELECT 'nws_stations'              AS tbl, COUNT(*) AS n FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_stations',              allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'nws_alerts',             COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_alerts',             allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'cdo_stations',           COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_stations',           allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'cdo_monthly_summaries',  COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_monthly_summaries',  allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'cdo_annual_summaries',   COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_annual_summaries',   allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'epa_annual_aqi',         COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi',         allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'epa_daily_aqi',          COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_daily_aqi',          allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'ghcnd_stations_with_county', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_stations_with_county', allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'ghcnd_daily',            COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily',            allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'drought_monitor_weekly', COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'hms_smoke_daily',        COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily',        allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'hms_smoke_polygons',     COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_polygons',     allow_moved_paths=true) LIMIT 1)
  UNION ALL SELECT 'climate_normals_monthly',COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly',allow_moved_paths=true) LIMIT 1)
);

-- ============================================================
-- T2: ROW COUNTS
-- Thresholds set conservatively to catch zero/near-zero tables.
-- nws_alerts threshold=0: snapshot data that may legitimately be empty (T1 catches it).
-- ============================================================
SELECT '=== T2: ROW COUNTS ===' AS section;

INSERT INTO dq_results
SELECT 'weather', tbl, 'row_count',
  CASE WHEN n >= thresh THEN 'pass' ELSE 'fail' END,
  CAST(n AS VARCHAR), CAST(thresh AS VARCHAR), NULL
FROM (
  SELECT 'nws_stations'              AS tbl, COUNT(*) AS n,   500 AS thresh FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_stations',              allow_moved_paths=true)
  UNION ALL SELECT 'nws_alerts',             COUNT(*),          0            FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_alerts',             allow_moved_paths=true)
  UNION ALL SELECT 'cdo_stations',           COUNT(*),       1000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_stations',           allow_moved_paths=true)
  UNION ALL SELECT 'cdo_monthly_summaries',  COUNT(*),      10000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_monthly_summaries',  allow_moved_paths=true)
  UNION ALL SELECT 'cdo_annual_summaries',   COUNT(*),       1000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_annual_summaries',   allow_moved_paths=true)
  UNION ALL SELECT 'epa_annual_aqi',         COUNT(*),       1000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi',         allow_moved_paths=true)
  UNION ALL SELECT 'epa_daily_aqi',          COUNT(*),      10000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_daily_aqi',          allow_moved_paths=true)
  UNION ALL SELECT 'ghcnd_stations_with_county', COUNT(*),   5000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_stations_with_county', allow_moved_paths=true)
  UNION ALL SELECT 'ghcnd_daily',            COUNT(*),     100000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily',            allow_moved_paths=true)
  UNION ALL SELECT 'drought_monitor_weekly', COUNT(*),      50000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true)
  UNION ALL SELECT 'hms_smoke_daily',        COUNT(*),       1000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily',        allow_moved_paths=true)
  UNION ALL SELECT 'hms_smoke_polygons',     COUNT(*),       1000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_polygons',     allow_moved_paths=true)
  UNION ALL SELECT 'climate_normals_monthly',COUNT(*),      10000            FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly',allow_moved_paths=true)
);

-- ============================================================
-- T3: SAMPLE ROW — one row per table for visual inspection (not stored in dq_results)
-- ============================================================
SELECT '=== T3: SAMPLE ROWS ===' AS section;

SELECT 'nws_stations'               AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_stations',              allow_moved_paths=true) LIMIT 1;
SELECT 'nws_alerts'                 AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_alerts',             allow_moved_paths=true) LIMIT 1;
SELECT 'cdo_stations'               AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_stations',           allow_moved_paths=true) LIMIT 1;
SELECT 'cdo_monthly_summaries'      AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_monthly_summaries',  allow_moved_paths=true) LIMIT 1;
SELECT 'cdo_annual_summaries'       AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_annual_summaries',   allow_moved_paths=true) LIMIT 1;
SELECT 'epa_annual_aqi'             AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi',         allow_moved_paths=true) LIMIT 1;
SELECT 'epa_daily_aqi'              AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_daily_aqi',          allow_moved_paths=true) LIMIT 1;
SELECT 'ghcnd_stations_with_county' AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_stations_with_county', allow_moved_paths=true) LIMIT 1;
SELECT 'ghcnd_daily'                AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily',            allow_moved_paths=true) LIMIT 1;
SELECT 'drought_monitor_weekly'     AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true) LIMIT 1;
SELECT 'hms_smoke_daily'            AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily',        allow_moved_paths=true) LIMIT 1;
SELECT 'hms_smoke_polygons'         AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_polygons',     allow_moved_paths=true) LIMIT 1;
SELECT 'climate_normals_monthly'    AS tbl, * FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly',allow_moved_paths=true) LIMIT 1;

-- ============================================================
-- T4: ALL-NULL COLUMNS
-- SUMMARIZE reports null_percentage per column; 100.0 = entire column is null.
-- ============================================================
SELECT '=== T4: ALL-NULL COLUMNS ===' AS section;

INSERT INTO dq_results
SELECT 'weather', 'nws_stations', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_stations', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'nws_alerts', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_alerts', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'cdo_stations', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_stations', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'cdo_monthly_summaries', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_monthly_summaries', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'cdo_annual_summaries', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_annual_summaries', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'epa_annual_aqi', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'aqi');  -- aqi not populated in annual summary records

INSERT INTO dq_results
SELECT 'weather', 'epa_daily_aqi', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_daily_aqi', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'ghcnd_stations_with_county', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_stations_with_county', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'ghcnd_daily', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'drought_monitor_weekly', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'hms_smoke_daily', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'hms_smoke_polygons', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_polygons', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'climate_normals_monthly', 'all_null_cols',
  CASE WHEN COUNT(*) > 0 THEN 'fail' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly', allow_moved_paths=true))
WHERE null_percentage = 100.0
  AND column_name NOT IN ('type', 'county_fips', 'prcp_stddev');  -- not in CDO normals source

-- ============================================================
-- T5: ALL-SAME-VALUE COLUMNS
-- approx_unique <= 1 and not already 100% null = every non-null row has same value.
-- Reported as warn — may be legitimate for constant or sparse seed data.
-- ============================================================
SELECT '=== T5: ALL-SAME-VALUE COLUMNS ===' AS section;

INSERT INTO dq_results
SELECT 'weather', 'nws_stations', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_stations', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'nws_alerts', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_alerts', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'cdo_stations', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_stations', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'cdo_monthly_summaries', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_monthly_summaries', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'cdo_annual_summaries', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_annual_summaries', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'epa_annual_aqi', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'epa_daily_aqi', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_daily_aqi', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'ghcnd_stations_with_county', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_stations_with_county', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'ghcnd_daily', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'drought_monitor_weekly', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'hms_smoke_daily', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

INSERT INTO dq_results
SELECT 'weather', 'hms_smoke_polygons', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_polygons', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type', 'year');  -- year is single-valued for daily rebuild snapshot

INSERT INTO dq_results
SELECT 'weather', 'climate_normals_monthly', 'all_same_value',
  CASE WHEN COUNT(*) > 0 THEN 'warn' ELSE 'pass' END,
  CAST(COUNT(*) AS VARCHAR), '0', COALESCE(STRING_AGG(column_name, ', '), '')
FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly', allow_moved_paths=true))
WHERE approx_unique <= 1 AND null_percentage < 100.0
  AND column_name NOT IN ('type');

-- ============================================================
-- T6: BUSINESS NON-NULLS
-- Primary key and business-critical columns declared nullable:false in schema YAML.
-- Parquet is always nullable at format level; this validates business rules.
-- ============================================================
SELECT '=== T6: BUSINESS NON-NULLS (PK COLUMNS) ===' AS section;

-- nws_stations PK: station_id; non-null: station_id, state_abbr
INSERT INTO dq_results
SELECT 'weather', 'nws_stations', 'pk_nulls',
  CASE WHEN null_station_id + null_state_abbr > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_station_id + null_state_abbr AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_station_id > 0 THEN 'station_id:' || null_station_id ELSE NULL END,
    CASE WHEN null_state_abbr > 0 THEN 'state_abbr:' || null_state_abbr ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN station_id IS NULL THEN 1 ELSE 0 END) AS null_station_id,
    SUM(CASE WHEN state_abbr IS NULL THEN 1 ELSE 0 END) AS null_state_abbr
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_stations', allow_moved_paths=true)
);

-- nws_alerts PK: alert_id, state_abbr; non-null: alert_id, state_abbr
INSERT INTO dq_results
SELECT 'weather', 'nws_alerts', 'pk_nulls',
  CASE WHEN null_alert_id + null_state_abbr > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_alert_id + null_state_abbr AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_alert_id  > 0 THEN 'alert_id:'  || null_alert_id  ELSE NULL END,
    CASE WHEN null_state_abbr > 0 THEN 'state_abbr:' || null_state_abbr ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN alert_id   IS NULL THEN 1 ELSE 0 END) AS null_alert_id,
    SUM(CASE WHEN state_abbr IS NULL THEN 1 ELSE 0 END) AS null_state_abbr
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_alerts', allow_moved_paths=true)
);

-- cdo_stations PK: station_id; non-null: station_id, state_fips
INSERT INTO dq_results
SELECT 'weather', 'cdo_stations', 'pk_nulls',
  CASE WHEN null_station_id + null_state_fips > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_station_id + null_state_fips AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_station_id > 0 THEN 'station_id:' || null_station_id ELSE NULL END,
    CASE WHEN null_state_fips > 0 THEN 'state_fips:' || null_state_fips ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN station_id IS NULL THEN 1 ELSE 0 END) AS null_station_id,
    SUM(CASE WHEN state_fips IS NULL THEN 1 ELSE 0 END) AS null_state_fips
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_stations', allow_moved_paths=true)
);

-- cdo_monthly_summaries non-null: state_fips, year
INSERT INTO dq_results
SELECT 'weather', 'cdo_monthly_summaries', 'pk_nulls',
  CASE WHEN null_state_fips + null_year > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_state_fips + null_year AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_state_fips > 0 THEN 'state_fips:' || null_state_fips ELSE NULL END,
    CASE WHEN null_year       > 0 THEN 'year:'       || null_year       ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN state_fips IS NULL THEN 1 ELSE 0 END) AS null_state_fips,
    SUM(CASE WHEN year       IS NULL THEN 1 ELSE 0 END) AS null_year
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_monthly_summaries', allow_moved_paths=true)
);

-- cdo_annual_summaries non-null: state_fips, year
INSERT INTO dq_results
SELECT 'weather', 'cdo_annual_summaries', 'pk_nulls',
  CASE WHEN null_state_fips + null_year > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_state_fips + null_year AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_state_fips > 0 THEN 'state_fips:' || null_state_fips ELSE NULL END,
    CASE WHEN null_year       > 0 THEN 'year:'       || null_year       ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN state_fips IS NULL THEN 1 ELSE 0 END) AS null_state_fips,
    SUM(CASE WHEN year       IS NULL THEN 1 ELSE 0 END) AS null_year
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_annual_summaries', allow_moved_paths=true)
);

-- epa_annual_aqi non-null: state_fips, year
INSERT INTO dq_results
SELECT 'weather', 'epa_annual_aqi', 'pk_nulls',
  CASE WHEN null_state_fips + null_year > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_state_fips + null_year AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN null_state_fips > 0 THEN 'state_fips:' || null_state_fips ELSE NULL END,
    CASE WHEN null_year       > 0 THEN 'year:'       || null_year       ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN state_fips IS NULL THEN 1 ELSE 0 END) AS null_state_fips,
    SUM(CASE WHEN year       IS NULL THEN 1 ELSE 0 END) AS null_year
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi', allow_moved_paths=true)
);

-- epa_daily_aqi non-null: state_fips, date, year
INSERT INTO dq_results
SELECT 'weather', 'epa_daily_aqi', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'state_fips:' || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'date:'       || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'year:'       || n3 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN state_fips IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN date       IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN year       IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN state_fips IS NULL OR date IS NULL OR year IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_daily_aqi', allow_moved_paths=true)
);

-- ghcnd_stations_with_county PK: station_id only; state_fips excluded — US territories (e.g. Midway Island) have no state
INSERT INTO dq_results
SELECT 'weather', 'ghcnd_stations_with_county', 'pk_nulls',
  CASE WHEN null_station_id > 0 THEN 'fail' ELSE 'pass' END,
  CAST(null_station_id AS VARCHAR), '0',
  CASE WHEN null_station_id > 0 THEN 'station_id:' || null_station_id ELSE '' END
FROM (
  SELECT SUM(CASE WHEN station_id IS NULL THEN 1 ELSE 0 END) AS null_station_id
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_stations_with_county', allow_moved_paths=true)
);

-- ghcnd_daily PK: station_id, date; non-null: station_id, state_fips, date, year, month
INSERT INTO dq_results
SELECT 'weather', 'ghcnd_daily', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'station_id:' || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'state_fips:' || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'date:'       || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'year:'       || n4 ELSE NULL END,
    CASE WHEN n5 > 0 THEN 'month:'      || n5 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN station_id IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN state_fips IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN date       IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN year       IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN month      IS NULL THEN 1 ELSE 0 END) AS n5,
    SUM(CASE WHEN station_id IS NULL OR state_fips IS NULL OR date IS NULL
                             OR year IS NULL OR month IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily', allow_moved_paths=true)
);

-- drought_monitor_weekly PK: county_fips, week_date; non-null: county_fips, state_fips, state_abbr, year, week_date
INSERT INTO dq_results
SELECT 'weather', 'drought_monitor_weekly', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'county_fips:' || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'state_fips:'  || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'state_abbr:'  || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'year:'        || n4 ELSE NULL END,
    CASE WHEN n5 > 0 THEN 'week_date:'   || n5 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN county_fips IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN state_fips  IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN state_abbr  IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN year        IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN week_date   IS NULL THEN 1 ELSE 0 END) AS n5,
    SUM(CASE WHEN county_fips IS NULL OR state_fips IS NULL OR state_abbr IS NULL
                              OR year IS NULL OR week_date IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true)
);

-- hms_smoke_daily PK: county_fips, date; non-null: county_fips, state_fips, date, year, month
INSERT INTO dq_results
SELECT 'weather', 'hms_smoke_daily', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'county_fips:' || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'state_fips:'  || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'date:'        || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'year:'        || n4 ELSE NULL END,
    CASE WHEN n5 > 0 THEN 'month:'       || n5 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN county_fips IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN state_fips  IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN date        IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN year        IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN month       IS NULL THEN 1 ELSE 0 END) AS n5,
    SUM(CASE WHEN county_fips IS NULL OR state_fips IS NULL OR date IS NULL
                              OR year IS NULL OR month IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily', allow_moved_paths=true)
);

-- hms_smoke_polygons non-null: date, year, month, density
INSERT INTO dq_results
SELECT 'weather', 'hms_smoke_polygons', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'date:'    || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'year:'    || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'month:'   || n3 ELSE NULL END,
    CASE WHEN n4 > 0 THEN 'density:' || n4 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN date    IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN year    IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN month   IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN density IS NULL THEN 1 ELSE 0 END) AS n4,
    SUM(CASE WHEN date IS NULL OR year IS NULL OR month IS NULL OR density IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_polygons', allow_moved_paths=true)
);

-- climate_normals_monthly PK: station_id, month; non-null: station_id, state_fips, month
INSERT INTO dq_results
SELECT 'weather', 'climate_normals_monthly', 'pk_nulls',
  CASE WHEN total > 0 THEN 'fail' ELSE 'pass' END,
  CAST(total AS VARCHAR), '0',
  CONCAT_WS(', ',
    CASE WHEN n1 > 0 THEN 'station_id:' || n1 ELSE NULL END,
    CASE WHEN n2 > 0 THEN 'state_fips:' || n2 ELSE NULL END,
    CASE WHEN n3 > 0 THEN 'month:'      || n3 ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN station_id IS NULL THEN 1 ELSE 0 END) AS n1,
    SUM(CASE WHEN state_fips IS NULL THEN 1 ELSE 0 END) AS n2,
    SUM(CASE WHEN month      IS NULL THEN 1 ELSE 0 END) AS n3,
    SUM(CASE WHEN station_id IS NULL OR state_fips IS NULL OR month IS NULL THEN 1 ELSE 0 END) AS total
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly', allow_moved_paths=true)
);

-- ============================================================
-- T7: EXPECTED VALUE DISTRIBUTIONS
-- Dimension columns must fall within known enumerated sets.
-- Also catches impossible numeric values.
-- ============================================================
SELECT '=== T7: EXPECTED VALUE DISTRIBUTIONS ===' AS section;

-- nws_alerts: severity in known values
INSERT INTO dq_results
SELECT 'weather', 'nws_alerts', 'severity_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct severities: ' || vals
FROM (
  SELECT SUM(CASE WHEN severity NOT IN ('Extreme','Severe','Moderate','Minor','Unknown') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT severity, ', ' ORDER BY severity) AS vals
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_alerts', allow_moved_paths=true)
  WHERE severity IS NOT NULL
);

-- nws_alerts: urgency in known values
INSERT INTO dq_results
SELECT 'weather', 'nws_alerts', 'urgency_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct urgencies: ' || vals
FROM (
  SELECT SUM(CASE WHEN urgency NOT IN ('Immediate','Expected','Future','Past','Unknown') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT urgency, ', ' ORDER BY urgency) AS vals
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_alerts', allow_moved_paths=true)
  WHERE urgency IS NOT NULL
);

-- cdo_monthly_summaries: month must be 1–12
INSERT INTO dq_results
SELECT 'weather', 'cdo_monthly_summaries', 'month_range',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct months: ' || vals
FROM (
  SELECT SUM(CASE WHEN month < 1 OR month > 12 THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(month AS VARCHAR), ', ' ORDER BY CAST(month AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_monthly_summaries', allow_moved_paths=true)
  WHERE month IS NOT NULL
);

-- epa_annual_aqi: negative aqi is a data error (fail); aqi > 500 is Beyond Index (warn — valid for extreme events)
INSERT INTO dq_results
SELECT 'weather', 'epa_annual_aqi', 'aqi_negative',
  CASE WHEN neg > 0 THEN 'fail' ELSE 'pass' END,
  CAST(neg AS VARCHAR), '0',
  'rows with aqi < 0 (impossible value)'
FROM (SELECT SUM(CASE WHEN aqi < 0 THEN 1 ELSE 0 END) AS neg FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi', allow_moved_paths=true) WHERE aqi IS NOT NULL);

INSERT INTO dq_results
SELECT 'weather', 'epa_annual_aqi', 'aqi_beyond_index',
  CASE WHEN hi > 0 THEN 'warn' ELSE 'pass' END,
  CAST(hi AS VARCHAR), '0',
  'rows with aqi > 500 (EPA Beyond Index — rare but valid for extreme events)'
FROM (SELECT SUM(CASE WHEN aqi > 500 THEN 1 ELSE 0 END) AS hi FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi', allow_moved_paths=true) WHERE aqi IS NOT NULL);

-- epa_annual_aqi: parameter_code must be one of 5 known pollutants
INSERT INTO dq_results
SELECT 'weather', 'epa_annual_aqi', 'parameter_code_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct codes: ' || vals
FROM (
  SELECT SUM(CASE WHEN parameter_code NOT IN ('88101','44201','42401','42602','42101') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT parameter_code, ', ' ORDER BY parameter_code) AS vals
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi', allow_moved_paths=true)
  WHERE parameter_code IS NOT NULL
);

-- epa_daily_aqi: negative aqi is a data error (fail); aqi > 500 is Beyond Index (warn — valid for extreme events)
INSERT INTO dq_results
SELECT 'weather', 'epa_daily_aqi', 'aqi_negative',
  CASE WHEN neg > 0 THEN 'fail' ELSE 'pass' END,
  CAST(neg AS VARCHAR), '0',
  'rows with aqi < 0 (impossible value)'
FROM (SELECT SUM(CASE WHEN aqi < 0 THEN 1 ELSE 0 END) AS neg FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_daily_aqi', allow_moved_paths=true) WHERE aqi IS NOT NULL);

INSERT INTO dq_results
SELECT 'weather', 'epa_daily_aqi', 'aqi_beyond_index',
  CASE WHEN hi > 0 THEN 'warn' ELSE 'pass' END,
  CAST(hi AS VARCHAR), '0',
  'rows with aqi > 500 (EPA Beyond Index — rare but valid for extreme events)'
FROM (SELECT SUM(CASE WHEN aqi > 500 THEN 1 ELSE 0 END) AS hi FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_daily_aqi', allow_moved_paths=true) WHERE aqi IS NOT NULL);

-- ghcnd_daily: temperature range -90 to 60 °C
INSERT INTO dq_results
SELECT 'weather', 'ghcnd_daily', 'temperature_range',
  CASE WHEN bad_tmax + bad_tmin > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad_tmax + bad_tmin AS VARCHAR), '0',
  CONCAT_WS('; ',
    CASE WHEN bad_tmax > 0 THEN 'tmax out of range:' || bad_tmax ELSE NULL END,
    CASE WHEN bad_tmin > 0 THEN 'tmin out of range:' || bad_tmin ELSE NULL END
  )
FROM (
  SELECT
    SUM(CASE WHEN tmax_c < -90 OR tmax_c > 60 THEN 1 ELSE 0 END) AS bad_tmax,
    SUM(CASE WHEN tmin_c < -90 OR tmin_c > 60 THEN 1 ELSE 0 END) AS bad_tmin
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily', allow_moved_paths=true)
  WHERE tmax_c IS NOT NULL OR tmin_c IS NOT NULL
);

-- ghcnd_daily: no negative precipitation
INSERT INTO dq_results
SELECT 'weather', 'ghcnd_daily', 'negative_prcp',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0', NULL
FROM (
  SELECT SUM(CASE WHEN prcp_mm < 0 THEN 1 ELSE 0 END) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily', allow_moved_paths=true)
  WHERE prcp_mm IS NOT NULL
);

-- ghcnd_daily: month must be '01'–'12'
INSERT INTO dq_results
SELECT 'weather', 'ghcnd_daily', 'month_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct months: ' || vals
FROM (
  SELECT SUM(CASE WHEN month NOT IN ('01','02','03','04','05','06','07','08','09','10','11','12') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT month, ', ' ORDER BY month) AS vals
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily', allow_moved_paths=true)
  WHERE month IS NOT NULL
);

-- drought_monitor_weekly: no negative drought percentages
INSERT INTO dq_results
SELECT 'weather', 'drought_monitor_weekly', 'negative_pct',
  CASE WHEN n > 0 THEN 'fail' ELSE 'pass' END,
  CAST(n AS VARCHAR), '0',
  'rows with any d0–d4/none pct < 0'
FROM (
  SELECT SUM(CASE WHEN none_pct < 0 OR d0_pct < 0 OR d1_pct < 0
                                    OR d2_pct < 0 OR d3_pct < 0 OR d4_pct < 0 THEN 1 ELSE 0 END) AS n
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true)
  WHERE none_pct IS NOT NULL
);

-- drought_monitor_weekly: dsci range 0–500
INSERT INTO dq_results
SELECT 'weather', 'drought_monitor_weekly', 'dsci_range',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'rows with dsci out of [0,500]'
FROM (
  SELECT SUM(CASE WHEN dsci < 0 OR dsci > 500 THEN 1 ELSE 0 END) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true)
  WHERE dsci IS NOT NULL
);

-- hms_smoke_daily: smoke_coverage_pct must be 0–100
INSERT INTO dq_results
SELECT 'weather', 'hms_smoke_daily', 'smoke_pct_range',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'rows with smoke_coverage_pct out of [0,100]'
FROM (
  SELECT SUM(CASE WHEN smoke_coverage_pct < 0 OR smoke_coverage_pct > 100 THEN 1 ELSE 0 END) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily', allow_moved_paths=true)
  WHERE smoke_coverage_pct IS NOT NULL
);

-- hms_smoke_polygons: density must be Light, Medium, or Heavy
INSERT INTO dq_results
SELECT 'weather', 'hms_smoke_polygons', 'density_values',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct densities: ' || vals
FROM (
  SELECT SUM(CASE WHEN density NOT IN ('Light','Medium','Heavy') THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT density, ', ' ORDER BY density) AS vals
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_polygons', allow_moved_paths=true)
  WHERE density IS NOT NULL
);

-- climate_normals_monthly: month must be 1–12
INSERT INTO dq_results
SELECT 'weather', 'climate_normals_monthly', 'month_range',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'distinct months: ' || vals
FROM (
  SELECT SUM(CASE WHEN month < 1 OR month > 12 THEN 1 ELSE 0 END) AS bad,
         STRING_AGG(DISTINCT CAST(month AS VARCHAR), ', ' ORDER BY CAST(month AS VARCHAR)) AS vals
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly', allow_moved_paths=true)
  WHERE month IS NOT NULL
);

-- climate_normals_monthly: normal_tmax_c range — CDO normals are in °F despite the _c suffix (ETL naming)
INSERT INTO dq_results
SELECT 'weather', 'climate_normals_monthly', 'normal_tmax_range',
  CASE WHEN bad > 0 THEN 'fail' ELSE 'pass' END,
  CAST(bad AS VARCHAR), '0',
  'rows with normal_tmax_c out of [-60,130] (values are °F)'
FROM (
  SELECT SUM(CASE WHEN normal_tmax_c < -60 OR normal_tmax_c > 130 THEN 1 ELSE 0 END) AS bad
  FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly', allow_moved_paths=true)
  WHERE normal_tmax_c IS NOT NULL
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
