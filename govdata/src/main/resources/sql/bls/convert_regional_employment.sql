-- Converts BLS regional employment JSON to Parquet
-- Flattens nested JSON structure: Results.series[*].data[*] → rows with seriesID, year, period, value
--
-- Parameters (substituted by Java):
--   {json_path} - Path to BLS API JSON response file
--   {parquet_path} - Output Parquet file path
--   {column_expressions} - SELECT clause with column expressions
--
-- BLS API response structure:
-- {
--   "Results": {
--     "series": [
--       {
--         "seriesID": "LAUST010000000000003",
--         "data": [
--           {"year": "2020", "period": "M01", "value": "3.1"}
--         ]
--       }
--     ]
--   }
-- }
--
-- Note: DuckDB UNNEST always creates a column named "unnest" (ignores alias)
--       containing a STRUCT with the array elements

COPY (
  SELECT
{column_expressions}
  FROM (
    SELECT
      series_id AS seriesID,
      UNNEST(data_array, recursive := true)
    FROM (
      SELECT
        unnest.seriesID AS series_id,
        unnest.data AS data_array
      FROM read_json('{json_path}', format := 'auto') AS root,
      UNNEST(root.Results.series)
    )
  )
) TO '{parquet_path}' (FORMAT PARQUET);
