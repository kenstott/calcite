-- Creates and populates BLS geographies table from JSON constants and hardcoded metro maps
-- Note: This is a hybrid approach - loads some data from JSON, some from Java-inserted data
--
-- This template is for the CREATE TABLE statement only
-- Data insertion happens via Java loops for dynamic data

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
