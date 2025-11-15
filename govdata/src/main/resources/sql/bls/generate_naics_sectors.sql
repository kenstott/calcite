-- Generates NAICS supersectors reference table from BLS constants
--
-- Parameters (use substituteSqlParameters to substitute):
--   {parquetPath} - Output Parquet file path

COPY (
  SELECT *
  FROM bls_naics_sectors
  ORDER BY supersector_code
) TO '{parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD);
