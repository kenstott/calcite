-- Generates BLS geographies reference table partitioned by geo_type (state, region, metro)
-- Combines data from state FIPS codes, census regions, and metro area codes
--
-- Parameters (use substituteSqlParameters to substitute):
--   {parquetPath} - Output Parquet file path for the current geo_type partition
--   {geoType} - Geography type: 'state', 'region', or 'metro'

COPY (
  SELECT *
  FROM bls_geographies
  WHERE geo_type = '{geoType}'
  ORDER BY geo_code
) TO '{parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD);
