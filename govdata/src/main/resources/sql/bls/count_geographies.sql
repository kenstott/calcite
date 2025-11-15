-- Counts BLS geographies by type
--
-- Parameters (use substituteSqlParameters to substitute):
--   {geoType} - Geography type: 'state', 'region', or 'metro'

SELECT count(*) FROM bls_geographies WHERE geo_type = '{geoType}';
