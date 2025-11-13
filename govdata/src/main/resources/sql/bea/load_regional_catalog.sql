-- Load BEA Regional LineCode catalog from all parquet partitions
-- Uses wildcard pattern to read multiple parquet files at once
-- Parameters: {wildcardPath}
-- Usage (CLI):
--   sed 's|{wildcardPath}|s3://bucket/source=econ/type=reference/tablename=*/linecodes.parquet|g' \
--       load_regional_catalog.sql | duckdb
--
-- Returns: TableName, LineCode pairs sorted by TableName then LineCode

SELECT TableName, LineCode
FROM read_parquet('{wildcardPath}')
ORDER BY TableName, LineCode
