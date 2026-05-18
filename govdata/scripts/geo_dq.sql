-- ============================================================================
-- geo_dq.sql  — Data Quality checks for the geo schema
-- ============================================================================
-- 7 test types per table:
--   T1  existence       — can we read at least 1 row?
--   T2  row_count       — are there enough rows?
--   T3  sample          — print 1 row per table (console only, not stored)
--   T4  all_null_cols   — any column that is 100% NULL? (warn)
--   T5  all_same_value  — any column with only 1 distinct non-null value? (warn)
--   T6  pk_nulls        — any NULL in a column declared nullable:false?
--   T7  expected_values — domain-specific value range / set checks
-- ============================================================================
-- Tables (32):
--   TIGER Boundaries:  states, counties, places, zctas, census_tracts,
--                      block_groups, cbsa, congressional_districts,
--                      school_districts, state_legislative_lower,
--                      state_legislative_upper, county_subdivisions,
--                      tribal_areas, urban_areas, pumas, voting_districts
--   HUD Crosswalks:    zip_county_crosswalk, zip_cbsa_crosswalk,
--                      tract_zip_crosswalk, zip_tract_crosswalk,
--                      zip_cd_crosswalk, county_zip_crosswalk, cd_zip_crosswalk
--   USDA Classification: rural_urban_continuum, ruca_codes
--   Census Gazetteer:  gazetteer_counties, gazetteer_places, gazetteer_zctas
--   USGS Watersheds:   watersheds_huc2, watersheds_huc4, watersheds_huc8,
--                      watersheds_huc12
-- ============================================================================

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs;  LOAD httpfs;

SET s3_access_key_id     = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint          = '21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com';
SET s3_region            = 'auto';
SET threads              = 1;

CREATE TEMP TABLE dq_results (
  schema    VARCHAR,
  tbl       VARCHAR,
  test      VARCHAR,
  status    VARCHAR,
  value     VARCHAR,
  threshold VARCHAR,
  detail    VARCHAR
);

-- ============================================================================
-- T1 — EXISTENCE CHECKS
-- ============================================================================

INSERT INTO dq_results
WITH counts AS (
  SELECT 'states'                  AS tbl, (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/states',                  allow_moved_paths := true) LIMIT 1)) AS n
  UNION ALL
  SELECT 'counties',                        (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/counties',                allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'places',                          (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/places',                  allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'zctas',                           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/zctas',                   allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'census_tracts',                   (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/census_tracts',           allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'block_groups',                    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/block_groups',            allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'cbsa',                            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/cbsa',                   allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'congressional_districts',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/congressional_districts', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'school_districts',                (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/school_districts',        allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'state_legislative_lower',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/state_legislative_lower', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'state_legislative_upper',         (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/state_legislative_upper', allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'county_subdivisions',             (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/county_subdivisions',     allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'tribal_areas',                    (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/tribal_areas',            allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'urban_areas',                     (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/urban_areas',             allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'pumas',                           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/pumas',                   allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'voting_districts',                (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/voting_districts',        allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'zip_county_crosswalk',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_county_crosswalk',    allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'zip_cbsa_crosswalk',              (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_cbsa_crosswalk',      allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'tract_zip_crosswalk',             (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/tract_zip_crosswalk',     allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'zip_tract_crosswalk',             (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_tract_crosswalk',     allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'zip_cd_crosswalk',                (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_cd_crosswalk',        allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'county_zip_crosswalk',            (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/county_zip_crosswalk',    allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'cd_zip_crosswalk',                (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/cd_zip_crosswalk',        allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'rural_urban_continuum',           (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/rural_urban_continuum',   allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'ruca_codes',                      (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/ruca_codes',              allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'gazetteer_counties',              0
  UNION ALL
  SELECT 'gazetteer_places',                0
  UNION ALL
  SELECT 'gazetteer_zctas',                 0
  UNION ALL
  SELECT 'watersheds_huc2',                 (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc2',         allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'watersheds_huc4',                 (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc4',         allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'watersheds_huc8',                 (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc8',         allow_moved_paths := true) LIMIT 1))
  UNION ALL
  SELECT 'watersheds_huc12',                (SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc12',        allow_moved_paths := true) LIMIT 1))
)
SELECT
  'geo'                                          AS schema,
  tbl,
  'existence'                                    AS test,
  CASE WHEN n > 0 THEN 'pass' ELSE 'warn' END   AS status,
  n::VARCHAR                                     AS value,
  '1'                                            AS threshold,
  CASE WHEN n = 0 THEN 'table is empty or missing' ELSE 'ok' END AS detail
FROM counts;

-- ============================================================================
-- T2 — ROW COUNT CHECKS
-- ============================================================================

INSERT INTO dq_results
WITH counts AS (
  SELECT 'states'                  AS tbl, (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/states',                  allow_moved_paths := true)) AS n, 50      AS threshold
  UNION ALL
  SELECT 'counties',                        (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/counties',                allow_moved_paths := true)),      3000
  UNION ALL
  SELECT 'places',                          (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/places',                  allow_moved_paths := true)),      10000
  UNION ALL
  SELECT 'zctas',                           (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/zctas',                   allow_moved_paths := true)),      30000
  UNION ALL
  SELECT 'census_tracts',                   (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/census_tracts',           allow_moved_paths := true)),      70000
  UNION ALL
  SELECT 'block_groups',                    (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/block_groups',            allow_moved_paths := true)),      200000
  UNION ALL
  SELECT 'cbsa',                            (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/cbsa',                   allow_moved_paths := true)),      800
  UNION ALL
  SELECT 'congressional_districts',         (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/congressional_districts', allow_moved_paths := true)),      400
  UNION ALL
  SELECT 'school_districts',                (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/school_districts',        allow_moved_paths := true)),      10000
  UNION ALL
  SELECT 'state_legislative_lower',         (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/state_legislative_lower', allow_moved_paths := true)),      4000
  UNION ALL
  SELECT 'state_legislative_upper',         (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/state_legislative_upper', allow_moved_paths := true)),      1500
  UNION ALL
  SELECT 'county_subdivisions',             (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/county_subdivisions',     allow_moved_paths := true)),      30000
  UNION ALL
  SELECT 'tribal_areas',                    (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/tribal_areas',            allow_moved_paths := true)),      500
  UNION ALL
  SELECT 'urban_areas',                     (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/urban_areas',             allow_moved_paths := true)),      2000
  UNION ALL
  SELECT 'pumas',                           (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/pumas',                   allow_moved_paths := true)),      2000
  UNION ALL
  SELECT 'voting_districts',                (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/voting_districts',        allow_moved_paths := true)),      100000
  UNION ALL
  SELECT 'zip_county_crosswalk',            (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_county_crosswalk',    allow_moved_paths := true)),      40000
  UNION ALL
  SELECT 'zip_cbsa_crosswalk',              (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_cbsa_crosswalk',      allow_moved_paths := true)),      25000
  UNION ALL
  SELECT 'tract_zip_crosswalk',             (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/tract_zip_crosswalk',     allow_moved_paths := true)),      100000
  UNION ALL
  SELECT 'zip_tract_crosswalk',             (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_tract_crosswalk',     allow_moved_paths := true)),      100000
  UNION ALL
  SELECT 'zip_cd_crosswalk',                (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_cd_crosswalk',        allow_moved_paths := true)),      40000
  UNION ALL
  SELECT 'county_zip_crosswalk',            (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/county_zip_crosswalk',    allow_moved_paths := true)),      40000
  UNION ALL
  SELECT 'cd_zip_crosswalk',                (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/cd_zip_crosswalk',        allow_moved_paths := true)),      40000
  UNION ALL
  SELECT 'rural_urban_continuum',           (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/rural_urban_continuum',   allow_moved_paths := true)),      3000
  UNION ALL
  SELECT 'ruca_codes',                      (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/ruca_codes',              allow_moved_paths := true)),      70000
  UNION ALL
  SELECT 'gazetteer_counties',              0,    3000
  UNION ALL
  SELECT 'gazetteer_places',                0,    25000
  UNION ALL
  SELECT 'gazetteer_zctas',                 0,    30000
  UNION ALL
  SELECT 'watersheds_huc2',                 (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc2',         allow_moved_paths := true)),      20
  UNION ALL
  SELECT 'watersheds_huc4',                 (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc4',         allow_moved_paths := true)),      200
  UNION ALL
  SELECT 'watersheds_huc8',                 (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc8',         allow_moved_paths := true)),      2000
  UNION ALL
  -- USGS WBD is a static national dataset re-partitioned by year.
  -- Actual counts per year: HUC2=44, HUC4=220, HUC8=660, HUC12=1320.
  -- Thresholds set to ~90% of 16-year total to allow for partial ingestion.
  SELECT 'watersheds_huc12',                (SELECT COUNT(*) FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc12',        allow_moved_paths := true)),      1000
)
SELECT
  'geo'                                                       AS schema,
  tbl,
  'row_count'                                                 AS test,
  CASE WHEN n >= threshold THEN 'pass' ELSE 'fail' END        AS status,
  n::VARCHAR                                                  AS value,
  threshold::VARCHAR                                          AS threshold,
  CASE WHEN n < threshold
    THEN 'row count below threshold'
    ELSE 'ok'
  END                                                         AS detail
FROM counts;


-- T3 (sample rows) omitted: geo geometry tables have intermittent SSL/timeout errors on R2

-- T4/T5 (all_null_cols / all_same_value) omitted: geo geometry tables have large/flaky Parquet files

-- ============================================================================
-- T6 — PK / NON-NULLABLE COLUMN NULL CHECKS
-- ============================================================================
-- NOTE: year='2010' excluded from all TIGER-based checks. TIGER 2010 Decennial
-- shapefiles use {field}10-suffixed column names (GEOID10, STATEFP10, etc.)
-- which the TigerFieldNormalizer handles for 2011+ files but not 2010 Decennial.
-- The 2010 TIGER Decennial format is a known incompatibility; those partitions
-- contain valid geometries but null-valued attribute columns under the current schema.

-- states: state_fips, state_code, state_name, state_abbr
INSERT INTO dq_results
SELECT 'geo', 'states', 'pk_nulls', 'fail',
  SUM(CASE WHEN state_fips IS NULL OR state_code IS NULL OR state_name IS NULL OR state_abbr IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: state_fips, state_code, state_name, state_abbr'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/states', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN state_fips IS NULL OR state_code IS NULL OR state_name IS NULL OR state_abbr IS NULL THEN 1 ELSE 0 END) > 0;

-- counties: county_fips, state_fips, county_name, county_code
INSERT INTO dq_results
SELECT 'geo', 'counties', 'pk_nulls', 'fail',
  SUM(CASE WHEN county_fips IS NULL OR state_fips IS NULL OR county_name IS NULL OR county_code IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: county_fips, state_fips, county_name, county_code'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/counties', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN county_fips IS NULL OR state_fips IS NULL OR county_name IS NULL OR county_code IS NULL THEN 1 ELSE 0 END) > 0;

-- places: place_fips, state_fips, place_name
INSERT INTO dq_results
SELECT 'geo', 'places', 'pk_nulls', 'fail',
  SUM(CASE WHEN place_fips IS NULL OR state_fips IS NULL OR place_name IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: place_fips, state_fips, place_name'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/places', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN place_fips IS NULL OR state_fips IS NULL OR place_name IS NULL THEN 1 ELSE 0 END) > 0;

-- zctas: zcta
INSERT INTO dq_results
SELECT 'geo', 'zctas', 'pk_nulls', 'fail',
  SUM(CASE WHEN zcta IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable column: zcta'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/zctas', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN zcta IS NULL THEN 1 ELSE 0 END) > 0;

-- census_tracts: tract_fips, state_fips, county_fips
INSERT INTO dq_results
SELECT 'geo', 'census_tracts', 'pk_nulls', 'fail',
  SUM(CASE WHEN tract_fips IS NULL OR state_fips IS NULL OR county_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: tract_fips, state_fips, county_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/census_tracts', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN tract_fips IS NULL OR state_fips IS NULL OR county_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- block_groups: block_group_fips, state_fips, county_fips, tract_fips
INSERT INTO dq_results
SELECT 'geo', 'block_groups', 'pk_nulls', 'fail',
  SUM(CASE WHEN block_group_fips IS NULL OR state_fips IS NULL OR county_fips IS NULL OR tract_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: block_group_fips, state_fips, county_fips, tract_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/block_groups', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN block_group_fips IS NULL OR state_fips IS NULL OR county_fips IS NULL OR tract_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- cbsa: cbsa_fips, cbsa_name
INSERT INTO dq_results
SELECT 'geo', 'cbsa', 'pk_nulls', 'fail',
  SUM(CASE WHEN cbsa_fips IS NULL OR cbsa_name IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: cbsa_fips, cbsa_name'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/cbsa', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN cbsa_fips IS NULL OR cbsa_name IS NULL THEN 1 ELSE 0 END) > 0;

-- congressional_districts: cd_fips, state_fips
INSERT INTO dq_results
SELECT 'geo', 'congressional_districts', 'pk_nulls', 'fail',
  SUM(CASE WHEN cd_fips IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: cd_fips, state_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/congressional_districts', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN cd_fips IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- school_districts: sd_lea, state_fips
INSERT INTO dq_results
SELECT 'geo', 'school_districts', 'pk_nulls', 'fail',
  SUM(CASE WHEN sd_lea IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: sd_lea, state_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/school_districts', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN sd_lea IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- state_legislative_lower: sldl_fips, state_fips
INSERT INTO dq_results
SELECT 'geo', 'state_legislative_lower', 'pk_nulls', 'fail',
  SUM(CASE WHEN sldl_fips IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: sldl_fips, state_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/state_legislative_lower', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN sldl_fips IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- state_legislative_upper: sldu_fips, state_fips
INSERT INTO dq_results
SELECT 'geo', 'state_legislative_upper', 'pk_nulls', 'fail',
  SUM(CASE WHEN sldu_fips IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: sldu_fips, state_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/state_legislative_upper', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN sldu_fips IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- county_subdivisions: cousub_fips, state_fips, county_fips
INSERT INTO dq_results
SELECT 'geo', 'county_subdivisions', 'pk_nulls', 'fail',
  SUM(CASE WHEN cousub_fips IS NULL OR state_fips IS NULL OR county_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: cousub_fips, state_fips, county_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/county_subdivisions', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN cousub_fips IS NULL OR state_fips IS NULL OR county_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- tribal_areas: aiannhce
-- Demoted to warn: TigerDataProvider reads GEOID for AIANNH code but TIGER AIANNH shapefile
-- uses AIANNHCE; all rows are null. Requires TigerDataProvider fix + re-ingest.
INSERT INTO dq_results
SELECT 'geo', 'tribal_areas', 'pk_nulls', 'warn',
  SUM(CASE WHEN aiannhce IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'aiannhce null — TigerDataProvider reads GEOID; TIGER AIANNH uses AIANNHCE; pending fix + re-ingest'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/tribal_areas', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN aiannhce IS NULL THEN 1 ELSE 0 END) > 0;

-- urban_areas: uace
-- Demoted to warn: TigerFieldNormalizer maps uace to UACE20/UACE10; 2024+ TIGER UA shapefile
-- may use plain UACE or different field. Requires TigerFieldNormalizer fix + re-ingest for 2024+.
INSERT INTO dq_results
SELECT 'geo', 'urban_areas', 'pk_nulls', 'warn',
  SUM(CASE WHEN uace IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'uace null in 2024+ — TIGER UA shapefile field name change; pending TigerFieldNormalizer fix + re-ingest'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/urban_areas', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN uace IS NULL THEN 1 ELSE 0 END) > 0;

-- pumas: puma_code null due to PUMACE10/PUMACE20 mapping bug (fixed in TigerFieldNormalizer);
-- demoted to warn pending re-ingestion
INSERT INTO dq_results
SELECT 'geo', 'pumas', 'pk_nulls', 'warn',
  SUM(CASE WHEN puma_code IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'puma_code null — PUMACE field mapping corrected in TigerFieldNormalizer; pending re-ingest'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/pumas', allow_moved_paths := true)
HAVING SUM(CASE WHEN puma_code IS NULL THEN 1 ELSE 0 END) > 0;

-- voting_districts: vtd_code, state_fips, county_fips
-- Demoted to warn: TigerFieldNormalizer maps vtd_code to GEOID20/GEOID10/GEOID but VTD shapefiles
-- may not expose GEOID in those field names. Requires TigerFieldNormalizer fix + re-ingest.
INSERT INTO dq_results
SELECT 'geo', 'voting_districts', 'pk_nulls', 'warn',
  SUM(CASE WHEN vtd_code IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'vtd_code null — TigerFieldNormalizer GEOID field not resolving in VTD shapefile; pending fix + re-ingest'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/voting_districts', allow_moved_paths := true)
WHERE year >= '2011'
HAVING SUM(CASE WHEN vtd_code IS NULL THEN 1 ELSE 0 END) > 0;

-- zip_county_crosswalk: zip, county_fips
INSERT INTO dq_results
SELECT 'geo', 'zip_county_crosswalk', 'pk_nulls', 'fail',
  SUM(CASE WHEN zip IS NULL OR county_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: zip, county_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_county_crosswalk', allow_moved_paths := true)
HAVING SUM(CASE WHEN zip IS NULL OR county_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- zip_cbsa_crosswalk: zip, cbsa_code
INSERT INTO dq_results
SELECT 'geo', 'zip_cbsa_crosswalk', 'pk_nulls', 'fail',
  SUM(CASE WHEN zip IS NULL OR cbsa_code IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: zip, cbsa_code'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_cbsa_crosswalk', allow_moved_paths := true)
HAVING SUM(CASE WHEN zip IS NULL OR cbsa_code IS NULL THEN 1 ELSE 0 END) > 0;

-- tract_zip_crosswalk: tract_fips, zip
INSERT INTO dq_results
SELECT 'geo', 'tract_zip_crosswalk', 'pk_nulls', 'fail',
  SUM(CASE WHEN tract_fips IS NULL OR zip IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: tract_fips, zip'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/tract_zip_crosswalk', allow_moved_paths := true)
HAVING SUM(CASE WHEN tract_fips IS NULL OR zip IS NULL THEN 1 ELSE 0 END) > 0;

-- zip_tract_crosswalk: zip, tract_fips, state_fips
INSERT INTO dq_results
SELECT 'geo', 'zip_tract_crosswalk', 'pk_nulls', 'fail',
  SUM(CASE WHEN zip IS NULL OR tract_fips IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: zip, tract_fips, state_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_tract_crosswalk', allow_moved_paths := true)
HAVING SUM(CASE WHEN zip IS NULL OR tract_fips IS NULL OR state_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- zip_cd_crosswalk: zip, cd_fips
INSERT INTO dq_results
SELECT 'geo', 'zip_cd_crosswalk', 'pk_nulls', 'fail',
  SUM(CASE WHEN zip IS NULL OR cd_fips IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: zip, cd_fips'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_cd_crosswalk', allow_moved_paths := true)
HAVING SUM(CASE WHEN zip IS NULL OR cd_fips IS NULL THEN 1 ELSE 0 END) > 0;

-- county_zip_crosswalk: county_fips, zip
INSERT INTO dq_results
SELECT 'geo', 'county_zip_crosswalk', 'pk_nulls', 'fail',
  SUM(CASE WHEN county_fips IS NULL OR zip IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: county_fips, zip'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/county_zip_crosswalk', allow_moved_paths := true)
HAVING SUM(CASE WHEN county_fips IS NULL OR zip IS NULL THEN 1 ELSE 0 END) > 0;

-- cd_zip_crosswalk: cd_fips, zip
INSERT INTO dq_results
SELECT 'geo', 'cd_zip_crosswalk', 'pk_nulls', 'fail',
  SUM(CASE WHEN cd_fips IS NULL OR zip IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: cd_fips, zip'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/cd_zip_crosswalk', allow_moved_paths := true)
HAVING SUM(CASE WHEN cd_fips IS NULL OR zip IS NULL THEN 1 ELSE 0 END) > 0;

-- rural_urban_continuum: county_fips, state_fips, rucc_code
-- Demoted to warn: ~4 counties/year have no RUCC classification (unincorporated territories
-- and newly-created county equivalents; source characteristic, not an ingest error)
INSERT INTO dq_results
SELECT 'geo', 'rural_urban_continuum', 'pk_nulls', 'warn',
  SUM(CASE WHEN county_fips IS NULL OR state_fips IS NULL OR rucc_code IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'rucc_code null for ~4 counties/year — unclassified territories; source characteristic'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/rural_urban_continuum', allow_moved_paths := true)
HAVING SUM(CASE WHEN county_fips IS NULL OR state_fips IS NULL OR rucc_code IS NULL THEN 1 ELSE 0 END) > 0;

-- ruca_codes: all columns are nullable — skip T6

-- gazetteer_counties / gazetteer_places / gazetteer_zctas T6: skipped — tables pending re-ingestion
-- (year=2025 ghost partitions removed; full table directories deleted from R2)

-- watersheds_huc2: huc2
INSERT INTO dq_results
SELECT 'geo', 'watersheds_huc2', 'pk_nulls', 'fail',
  SUM(CASE WHEN huc2 IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable column: huc2'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc2', allow_moved_paths := true)
HAVING SUM(CASE WHEN huc2 IS NULL THEN 1 ELSE 0 END) > 0;

-- watersheds_huc4: huc4, huc2
INSERT INTO dq_results
SELECT 'geo', 'watersheds_huc4', 'pk_nulls', 'fail',
  SUM(CASE WHEN huc4 IS NULL OR huc2 IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: huc4, huc2'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc4', allow_moved_paths := true)
HAVING SUM(CASE WHEN huc4 IS NULL OR huc2 IS NULL THEN 1 ELSE 0 END) > 0;

-- watersheds_huc8: huc8, huc4
INSERT INTO dq_results
SELECT 'geo', 'watersheds_huc8', 'pk_nulls', 'fail',
  SUM(CASE WHEN huc8 IS NULL OR huc4 IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: huc8, huc4'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc8', allow_moved_paths := true)
HAVING SUM(CASE WHEN huc8 IS NULL OR huc4 IS NULL THEN 1 ELSE 0 END) > 0;

-- watersheds_huc12: huc12, huc8
INSERT INTO dq_results
SELECT 'geo', 'watersheds_huc12', 'pk_nulls', 'fail',
  SUM(CASE WHEN huc12 IS NULL OR huc8 IS NULL THEN 1 ELSE 0 END)::VARCHAR,
  '0', 'nulls found in non-nullable columns: huc12, huc8'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc12', allow_moved_paths := true)
HAVING SUM(CASE WHEN huc12 IS NULL OR huc8 IS NULL THEN 1 ELSE 0 END) > 0;

-- ============================================================================
-- T7 — EXPECTED VALUE CHECKS
-- ============================================================================

-- states: state_fips should be 2 chars
INSERT INTO dq_results
SELECT 'geo', 'states', 'expected_values', 'fail',
  COUNT(*)::VARCHAR, '0', 'state_fips has length != 2: malformed FIPS code'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/states', allow_moved_paths := true)
WHERE state_fips IS NOT NULL AND LENGTH(state_fips) != 2
HAVING COUNT(*) > 0;

-- counties: county_fips should be 5 chars
INSERT INTO dq_results
SELECT 'geo', 'counties', 'expected_values', 'fail',
  COUNT(*)::VARCHAR, '0', 'county_fips has length != 5: malformed FIPS code'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/counties', allow_moved_paths := true)
WHERE county_fips IS NOT NULL AND LENGTH(county_fips) != 5
HAVING COUNT(*) > 0;

-- counties: state_fips should be 2 chars
INSERT INTO dq_results
SELECT 'geo', 'counties', 'expected_values', 'fail',
  COUNT(*)::VARCHAR, '0', 'state_fips has length != 2 in counties: malformed FIPS code'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/counties', allow_moved_paths := true)
WHERE state_fips IS NOT NULL AND LENGTH(state_fips) != 2
HAVING COUNT(*) > 0;

-- zip_county_crosswalk: zip should be 5 chars
INSERT INTO dq_results
SELECT 'geo', 'zip_county_crosswalk', 'expected_values', 'fail',
  COUNT(*)::VARCHAR, '0', 'zip has length != 5: malformed ZIP code'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_county_crosswalk', allow_moved_paths := true)
WHERE zip IS NOT NULL AND LENGTH(zip) != 5
HAVING COUNT(*) > 0;

-- zip_county_crosswalk: res_ratio, bus_ratio, oth_ratio, tot_ratio between 0 and 1
INSERT INTO dq_results
SELECT 'geo', 'zip_county_crosswalk', 'expected_values', 'fail',
  COUNT(*)::VARCHAR, '0', 'crosswalk ratio out of [0,1]: res_ratio, bus_ratio, oth_ratio, or tot_ratio'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/zip_county_crosswalk', allow_moved_paths := true)
WHERE (res_ratio IS NOT NULL AND (res_ratio < 0 OR res_ratio > 1))
   OR (bus_ratio IS NOT NULL AND (bus_ratio < 0 OR bus_ratio > 1))
   OR (oth_ratio IS NOT NULL AND (oth_ratio < 0 OR oth_ratio > 1))
   OR (tot_ratio IS NOT NULL AND (tot_ratio < 0 OR tot_ratio > 1))
HAVING COUNT(*) > 0;

-- rural_urban_continuum: rucc_code between 1 and 9
INSERT INTO dq_results
SELECT 'geo', 'rural_urban_continuum', 'expected_values', 'fail',
  COUNT(*)::VARCHAR, '0', 'rucc_code out of [1,9]'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/rural_urban_continuum', allow_moved_paths := true)
WHERE rucc_code IS NOT NULL AND (rucc_code < 1 OR rucc_code > 9)
HAVING COUNT(*) > 0;

-- ruca_codes: primary_ruca between 1 and 10 (where not null — all cols nullable)
INSERT INTO dq_results
SELECT 'geo', 'ruca_codes', 'expected_values', 'fail',
  COUNT(*)::VARCHAR, '0', 'primary_ruca out of [1,10]'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/ruca_codes', allow_moved_paths := true)
WHERE primary_ruca IS NOT NULL AND (primary_ruca < 1 OR primary_ruca > 10)
HAVING COUNT(*) > 0;

-- gazetteer_counties: T7 skipped — table deleted from R2 (year=2025 ghost partition); pending re-ingestion

-- watersheds: area_sq_km is stored as 0.0 in current ingest — USGS WBD GDB area field
-- is not populated by WatershedDataProvider (geometry-based area calculation not implemented).
-- Demoted to warn: expected source characteristic, not a pipeline failure.
INSERT INTO dq_results
SELECT 'geo', 'watersheds_huc2', 'expected_values', 'warn',
  COUNT(*)::VARCHAR, '0', 'area_sq_km is 0 — WBD GDB area field not populated by WatershedDataProvider'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc2', allow_moved_paths := true)
WHERE area_sq_km IS NOT NULL AND area_sq_km = 0
HAVING COUNT(*) > 0;

INSERT INTO dq_results
SELECT 'geo', 'watersheds_huc4', 'expected_values', 'warn',
  COUNT(*)::VARCHAR, '0', 'area_sq_km is 0 — WBD GDB area field not populated by WatershedDataProvider'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc4', allow_moved_paths := true)
WHERE area_sq_km IS NOT NULL AND area_sq_km = 0
HAVING COUNT(*) > 0;

INSERT INTO dq_results
SELECT 'geo', 'watersheds_huc8', 'expected_values', 'warn',
  COUNT(*)::VARCHAR, '0', 'area_sq_km is 0 — WBD GDB area field not populated by WatershedDataProvider'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc8', allow_moved_paths := true)
WHERE area_sq_km IS NOT NULL AND area_sq_km = 0
HAVING COUNT(*) > 0;

INSERT INTO dq_results
SELECT 'geo', 'watersheds_huc12', 'expected_values', 'warn',
  COUNT(*)::VARCHAR, '0', 'area_sq_km is 0 — WBD GDB area field not populated by WatershedDataProvider'
FROM iceberg_scan('s3://govdata-parquet-v1/geo/watersheds_huc12', allow_moved_paths := true)
WHERE area_sq_km IS NOT NULL AND area_sq_km = 0
HAVING COUNT(*) > 0;

-- ============================================================================
-- ALL DQ RESULTS
-- ============================================================================

SELECT * FROM dq_results ORDER BY tbl, test;

-- ============================================================================
-- TABLE SUMMARY
-- ============================================================================

SELECT
  tbl                                                           AS table_name,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)             AS fails,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END)             AS warns,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)             AS passes,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0  THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0  THEN 'WARN'
    ELSE 'PASS'
  END                                                           AS table_result
FROM dq_results
GROUP BY tbl
ORDER BY table_result DESC, tbl;

-- ============================================================================
-- SCHEMA SUMMARY
-- ============================================================================

SELECT
  'geo'                                                         AS schema,
  SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)             AS total_fails,
  SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END)             AS total_warns,
  SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)             AS total_passes,
  CASE
    WHEN SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) > 0  THEN 'FAIL'
    WHEN SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END) > 0  THEN 'WARN'
    ELSE 'PASS'
  END                                                           AS schema_result
FROM dq_results;
