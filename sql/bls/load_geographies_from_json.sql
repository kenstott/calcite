-- Loads BLS geographies from JSON strings using DuckDB's JSON capabilities
-- Uses json_each() to iterate over nested objects and extract key-value pairs
--
-- Parameters (use substituteSqlParameters to substitute):
--   {stateFipsJson} - JSON string of stateFipsCodes object
--   {censusRegionsJson} - JSON string of censusRegions object
--   {metroBlsAreaCodesJson} - JSON string of metroBlsAreaCodes object

CREATE TABLE bls_geographies (
  geo_code VARCHAR,
  geo_name VARCHAR,
  geo_type VARCHAR,
  state_fips VARCHAR,
  region_code VARCHAR,
  metro_publication_code VARCHAR,
  metro_cpi_area_code VARCHAR,
  metro_bls_area_code VARCHAR
);

-- Load state FIPS codes from JSON string
INSERT INTO bls_geographies
SELECT
  key AS geo_code,
  key AS geo_name,
  'state' AS geo_type,
  value AS state_fips,
  NULL AS region_code,
  NULL AS metro_publication_code,
  NULL AS metro_cpi_area_code,
  NULL AS metro_bls_area_code
FROM json_each('{stateFipsJson}'::JSON);

-- Load census regions from JSON string
INSERT INTO bls_geographies
SELECT
  key AS geo_code,
  value AS geo_name,
  'region' AS geo_type,
  NULL AS state_fips,
  key AS region_code,
  NULL AS metro_publication_code,
  NULL AS metro_cpi_area_code,
  NULL AS metro_bls_area_code
FROM json_each('{censusRegionsJson}'::JSON);

-- Load metro BLS area codes from JSON string
INSERT INTO bls_geographies
SELECT
  key AS geo_code,
  value AS geo_name,
  'metro' AS geo_type,
  NULL AS state_fips,
  NULL AS region_code,
  key AS metro_publication_code,
  NULL AS metro_cpi_area_code,
  value AS metro_bls_area_code
FROM json_each('{metroBlsAreaCodesJson}'::JSON);
