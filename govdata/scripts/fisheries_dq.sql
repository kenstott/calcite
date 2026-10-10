-- dq-lookback: 1
-- Fisheries Data Quality Checks
-- Schema: fisheries
-- Tables: noaa_commercial_landings
-- All tables are Iceberg; reads via iceberg_scan.
-- T4/T5 exclude partition column 'year' and the constant partition column 'type'.
-- No PK check: NOAA publishes several source rows per state + species + source + collection
-- (MRIP and confidential-species rows are split), so duplicates of the natural key are expected.

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs;  LOAD httpfs;

SET s3_access_key_id='${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key='${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint='21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region='auto';
SET unsafe_enable_version_guessing=true;

CREATE TEMP TABLE dq_results (
  schema   VARCHAR,
  tbl      VARCHAR,
  test     VARCHAR,
  status   VARCHAR,
  value    DOUBLE,
  threshold DOUBLE,
  detail   VARCHAR
);

-- T1: existence
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T1_existence',
  CASE WHEN n > 0 THEN 'pass' ELSE 'fail' END,
  n, 1, 'Row count from iceberg_scan'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true));

-- T2: row_count (~4,900 rows/year; DQ scope covers several landing years)
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T2_row_count',
  CASE WHEN n >= 9000 THEN 'pass' ELSE 'fail' END,
  n, 9000, 'Expected at least 9000 landings rows (2+ years x ~4,900)'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true));

-- T3: sample
SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true) LIMIT 3;

-- T4: all_null_cols
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T4_all_null_cols',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No fully-null columns' ELSE 'Fully-null columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, null_percentage
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true))
    WHERE null_percentage = 100.0
      AND column_name NOT IN ('year', 'type')
  )
);

-- T5: all_same_value
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T5_all_same_value',
  CASE WHEN cnt = 0 THEN 'pass' ELSE 'warn' END,
  cnt, 0,
  CASE WHEN cnt = 0 THEN 'No single-value columns' ELSE 'Single-value columns: ' || cols END
FROM (
  SELECT COUNT(*) AS cnt, STRING_AGG(column_name, ', ') AS cols
  FROM (
    SELECT column_name, approx_unique
    FROM (SUMMARIZE SELECT * FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true))
    WHERE approx_unique <= 1
      AND column_name NOT IN ('year', 'type')
  )
);

-- T6: not-null columns (landing_year, collection, source, state_name, is_state, is_salmon)
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T6_pk_nulls',
  CASE WHEN n = 0 THEN 'pass' ELSE 'fail' END,
  n, 0, 'NULL landing_year, collection, source, state_name, is_state or is_salmon rows'
FROM (SELECT COUNT(*) AS n FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
      WHERE landing_year IS NULL OR collection IS NULL OR source IS NULL
         OR state_name IS NULL OR is_state IS NULL OR is_salmon IS NULL);

-- T7: collection values
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_collection_values',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows with collection outside (Commercial, Recreational)'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
      WHERE collection NOT IN ('Commercial', 'Recreational'));

-- T7: partition year matches landing_year
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_year_matches_landing_year',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows where partition year <> landing_year'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true) WHERE year <> landing_year);

-- T7: every state_name maps to a state_abbr except PROCESS AT SEA
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_state_abbr_mapped',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows with null state_abbr whose state_name is not PROCESS AT SEA'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
      WHERE state_abbr IS NULL AND state_name <> 'PROCESS AT SEA');

-- T7: Alaska commercial salmon pounds are positive in every landing year loaded
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_alaska_salmon_every_year',
  CASE WHEN bad = 0 AND yrs > 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Landing years without Alaska commercial salmon pounds (years checked: ' || yrs || ')'
FROM (
  SELECT (SELECT COUNT(DISTINCT landing_year) FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)) AS yrs,
         (SELECT COUNT(*) FROM (SELECT DISTINCT landing_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)) y
          WHERE y.landing_year NOT IN (
            SELECT landing_year FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
            WHERE state_abbr = 'AK' AND is_salmon AND collection = 'Commercial' AND pounds > 0)) AS bad
);

-- T7: Alaska sockeye pounds in the best loaded year are within the published order of magnitude
-- (2022: 381,177,520 lb; recent years range ~100M-400M lb)
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_alaska_sockeye_magnitude',
  CASE WHEN lbs BETWEEN 5e7 AND 6e8 THEN 'pass' ELSE 'fail' END,
  lbs, 5e7, 'Max single-year Alaska commercial sockeye pounds (expect 50M-600M)'
FROM (SELECT MAX(p) AS lbs FROM (
        SELECT landing_year, SUM(pounds) AS p FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
        WHERE state_abbr = 'AK' AND salmon_species = 'sockeye' AND collection = 'Commercial'
        GROUP BY landing_year));

-- T7: salmon flag consistency (salmon rows carry species + origin; others carry neither)
INSERT INTO dq_results
SELECT 'fisheries', 'noaa_commercial_landings', 'T7_salmon_flag_consistency',
  CASE WHEN bad = 0 THEN 'pass' ELSE 'fail' END,
  bad, 0, 'Rows where is_salmon disagrees with salmon_species / salmon_origin nullness'
FROM (SELECT COUNT(*) AS bad FROM iceberg_scan('s3://${GOVDATA_DQ_BUCKET}/fisheries/noaa_commercial_landings', allow_moved_paths := true)
      WHERE (is_salmon AND (salmon_species IS NULL OR salmon_origin IS NULL))
         OR (NOT is_salmon AND (salmon_species IS NOT NULL OR salmon_origin IS NOT NULL)));

-- ─────────────────────────────────────────────────────────────
-- Final results
-- ─────────────────────────────────────────────────────────────
SELECT schema, tbl, test, status, value, threshold, detail
FROM dq_results
ORDER BY schema, tbl, test;
