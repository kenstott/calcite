-- Loads NAICS supersectors from JSON string using DuckDB's JSON capabilities
--
-- Parameters (use substituteSqlParameters to substitute):
--   {naicsSupersectorsJson} - JSON string of naicsSupersectors object

CREATE TABLE bls_naics_sectors (
  supersector_code VARCHAR,
  supersector_name VARCHAR
);

-- Load NAICS supersectors using json_each to iterate over the object
INSERT INTO bls_naics_sectors
SELECT
  key AS supersector_code,
  value AS supersector_name
FROM json_each('{naicsSupersectorsJson}'::JSON);
