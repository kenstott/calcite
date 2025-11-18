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
-- Note: Using two UNNEST operations with subquery to flatten the nested structure:
--   1. Inner subquery: UNNEST(root.Results.series) - expands the series array
--   2. Outer query: UNNEST(series.data_array) - expands the data array within each series
--   IMPORTANT: UNNEST always creates a column called "unnest", aliases are ignored

COPY
(
SELECT
    {column_expressions}
FROM (
    SELECT
        unnest.year AS "year",
        unnest.period AS "period",
        unnest.value AS "value",
        series.seriesID AS "seriesID"
    FROM (
        SELECT
            unnest.seriesID AS seriesID,
            unnest.data AS data_array
        FROM read_json('{json_path}', format := 'auto') AS root,
        UNNEST(root.Results.series)
    ) AS series,
    UNNEST(series.data_array)
) AS flattened_data
) TO '{parquet_path}' (FORMAT PARQUET);
