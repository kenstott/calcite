-- Weather Data Quality Check
-- Run: source .env.prod && envsubst < scripts/weather_dq.sql | duckdb

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';
SET s3_url_style='path';

-- ============================================================
-- 1. ROW COUNTS
-- ============================================================
SELECT '=== ROW COUNTS ===' AS section;

SELECT 'nws_stations'            AS tbl, COUNT(*) AS rows FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_stations', allow_moved_paths=true)
UNION ALL
SELECT 'nws_alerts',                     COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_alerts', allow_moved_paths=true)
UNION ALL
SELECT 'cdo_stations',                   COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_stations', allow_moved_paths=true)
UNION ALL
SELECT 'cdo_monthly_summaries',          COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_monthly_summaries', allow_moved_paths=true)
UNION ALL
SELECT 'cdo_annual_summaries',           COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_annual_summaries', allow_moved_paths=true)
UNION ALL
SELECT 'epa_annual_aqi',                 COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi', allow_moved_paths=true)
UNION ALL
SELECT 'ghcnd_stations_with_county',     COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_stations_with_county', allow_moved_paths=true)
UNION ALL
SELECT 'ghcnd_daily',                    COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily', allow_moved_paths=true)
UNION ALL
SELECT 'drought_monitor_weekly',         COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true)
UNION ALL
SELECT 'hms_smoke_daily',                COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily', allow_moved_paths=true)
UNION ALL
SELECT 'hms_smoke_polygons',             COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_polygons', allow_moved_paths=true)
UNION ALL
SELECT 'climate_normals_monthly',        COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly', allow_moved_paths=true)
ORDER BY tbl;

-- ============================================================
-- 2. HMS SMOKE DAILY — partition coverage + value ranges
-- ============================================================
SELECT '=== HMS_SMOKE_DAILY: YEAR/MONTH COVERAGE ===' AS section;

SELECT year, month, COUNT(*) AS rows,
  ROUND(AVG(smoke_coverage_pct),2)  AS avg_smoke_pct,
  ROUND(MAX(smoke_coverage_pct),2)  AS max_smoke_pct,
  SUM(CASE WHEN smoke_coverage_pct < 0 OR smoke_coverage_pct > 100 THEN 1 ELSE 0 END) AS out_of_range,
  SUM(CASE WHEN county_fips IS NULL THEN 1 ELSE 0 END) AS null_county_fips,
  SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date
FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily', allow_moved_paths=true)
GROUP BY year, month
ORDER BY year, month;

SELECT '=== HMS_SMOKE_DAILY: NULL RATES ON SMOKE COLUMNS ===' AS section;

SELECT
  COUNT(*) AS total_rows,
  SUM(CASE WHEN smoke_coverage_pct IS NULL THEN 1 ELSE 0 END) AS null_smoke_pct,
  SUM(CASE WHEN heavy_smoke_pct IS NULL THEN 1 ELSE 0 END)    AS null_heavy,
  SUM(CASE WHEN medium_smoke_pct IS NULL THEN 1 ELSE 0 END)   AS null_medium,
  SUM(CASE WHEN light_smoke_pct IS NULL THEN 1 ELSE 0 END)    AS null_light,
  SUM(CASE WHEN county_fips IS NULL THEN 1 ELSE 0 END)        AS null_fips,
  SUM(CASE WHEN state_fips IS NULL THEN 1 ELSE 0 END)         AS null_state_fips,
  ROUND(MIN(smoke_coverage_pct),4) AS min_smoke,
  ROUND(MAX(smoke_coverage_pct),4) AS max_smoke
FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_daily', allow_moved_paths=true);

-- ============================================================
-- 3. HMS SMOKE POLYGONS — density values + geometry nulls
-- ============================================================
SELECT '=== HMS_SMOKE_POLYGONS: DENSITY DISTRIBUTION ===' AS section;

SELECT year, month, density, COUNT(*) AS rows,
  SUM(CASE WHEN geometry IS NULL THEN 1 ELSE 0 END) AS null_geometry
FROM iceberg_scan('s3://govdata-parquet-v1/weather/hms_smoke_polygons', allow_moved_paths=true)
GROUP BY year, month, density
ORDER BY year, month, density;

-- ============================================================
-- 4. NWS STATIONS — key field nulls + state coverage
-- ============================================================
SELECT '=== NWS_STATIONS: NULL RATES + STATE COUNT ===' AS section;

SELECT
  COUNT(*) AS total,
  COUNT(DISTINCT state_abbr) AS distinct_states,
  SUM(CASE WHEN station_id IS NULL THEN 1 ELSE 0 END)   AS null_station_id,
  SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END)     AS null_lat,
  SUM(CASE WHEN longitude IS NULL THEN 1 ELSE 0 END)    AS null_lon,
  SUM(CASE WHEN state_abbr IS NULL THEN 1 ELSE 0 END)   AS null_state
FROM iceberg_scan('s3://govdata-parquet-v1/weather/nws_stations', allow_moved_paths=true);

-- ============================================================
-- 5. DROUGHT MONITOR — date range + FIPS nulls
-- ============================================================
SELECT '=== DROUGHT_MONITOR_WEEKLY: DATE RANGE + NULLS ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(week_date) AS min_date, MAX(week_date) AS max_date,
  COUNT(DISTINCT county_fips) AS distinct_counties,
  SUM(CASE WHEN county_fips IS NULL THEN 1 ELSE 0 END)  AS null_fips,
  SUM(CASE WHEN d0_pct + d1_pct + d2_pct + d3_pct + d4_pct + none_pct < 99.0
           OR d0_pct + d1_pct + d2_pct + d3_pct + d4_pct + none_pct > 101.0 THEN 1 ELSE 0 END) AS pct_sum_bad
FROM iceberg_scan('s3://govdata-parquet-v1/weather/drought_monitor_weekly', allow_moved_paths=true);

-- ============================================================
-- 6. GHCND STATIONS — county join coverage
-- ============================================================
SELECT '=== GHCND_STATIONS_WITH_COUNTY: COVERAGE ===' AS section;

SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN county_fips IS NULL THEN 1 ELSE 0 END) AS null_fips,
  SUM(CASE WHEN station_id IS NULL THEN 1 ELSE 0 END)  AS null_station_id,
  ROUND(100.0 * SUM(CASE WHEN county_fips IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_with_county
FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_stations_with_county', allow_moved_paths=true);

-- ============================================================
-- 7. GHCND DAILY — date range + temp/precip nulls
-- ============================================================
SELECT '=== GHCND_DAILY: DATE RANGE + NULL RATES ===' AS section;

SELECT
  COUNT(*) AS total,
  MIN(date) AS min_date, MAX(date) AS max_date,
  COUNT(DISTINCT station_id) AS distinct_stations,
  ROUND(100.0 * SUM(CASE WHEN tmax_c IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_tmax,
  ROUND(100.0 * SUM(CASE WHEN tmin_c IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_tmin,
  ROUND(100.0 * SUM(CASE WHEN prcp_mm IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_null_prcp,
  SUM(CASE WHEN tmax_c < -90 OR tmax_c > 60 THEN 1 ELSE 0 END) AS tmax_out_of_range,
  SUM(CASE WHEN tmin_c < -90 OR tmin_c > 60 THEN 1 ELSE 0 END) AS tmin_out_of_range
FROM iceberg_scan('s3://govdata-parquet-v1/weather/ghcnd_daily', allow_moved_paths=true);

-- ============================================================
-- 8. CDO STATIONS — date range, lat/lon nulls
-- ============================================================
SELECT '=== CDO_STATIONS: COVERAGE ===' AS section;

SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN station_id IS NULL THEN 1 ELSE 0 END)  AS null_id,
  SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END)    AS null_lat,
  SUM(CASE WHEN longitude IS NULL THEN 1 ELSE 0 END)   AS null_lon,
  MIN(min_date) AS earliest_data, MAX(max_date) AS latest_data
FROM iceberg_scan('s3://govdata-parquet-v1/weather/cdo_stations', allow_moved_paths=true);

-- ============================================================
-- 9. EPA ANNUAL AQI — year range + nulls
-- ============================================================
SELECT '=== EPA_ANNUAL_AQI: YEAR RANGE + NULLS ===' AS section;

SELECT
  COUNT(*) AS total,
  COUNT(DISTINCT county_fips) AS distinct_counties,
  SUM(CASE WHEN county_fips IS NULL THEN 1 ELSE 0 END) AS null_fips,
  SUM(CASE WHEN aqi IS NULL THEN 1 ELSE 0 END)         AS null_aqi,
  SUM(CASE WHEN aqi < 0 OR aqi > 500 THEN 1 ELSE 0 END) AS aqi_out_of_range
FROM iceberg_scan('s3://govdata-parquet-v1/weather/epa_annual_aqi', allow_moved_paths=true);

-- ============================================================
-- 10. CLIMATE NORMALS — station/month coverage
-- ============================================================
SELECT '=== CLIMATE_NORMALS_MONTHLY: COVERAGE ===' AS section;

SELECT
  COUNT(*) AS total,
  COUNT(DISTINCT station_id) AS distinct_stations,
  COUNT(DISTINCT month) AS distinct_months,
  SUM(CASE WHEN station_id IS NULL THEN 1 ELSE 0 END)         AS null_station,
  SUM(CASE WHEN normal_tmax_c IS NULL THEN 1 ELSE 0 END)      AS null_tmax,
  SUM(CASE WHEN normal_tmin_c IS NULL THEN 1 ELSE 0 END)      AS null_tmin,
  ROUND(MIN(normal_tmax_c),1) AS min_tmax, ROUND(MAX(normal_tmax_c),1) AS max_tmax
FROM iceberg_scan('s3://govdata-parquet-v1/weather/climate_normals_monthly', allow_moved_paths=true);
