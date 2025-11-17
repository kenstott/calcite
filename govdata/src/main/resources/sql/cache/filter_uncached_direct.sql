-- Filter uncached download requests using direct query (optimal for <1000 requests)
-- Parameters: {manifestPath}, {keysArray}, {nowTimestamp}, {includeParquetCheck}
-- Usage: Build keys array in Java, substitute parameters, execute query

WITH
  manifest AS (
    SELECT
      key,
      json_extract(value, '$.refreshAfter')::BIGINT as refresh_after,
      json_extract(value, '$.downloadRetry')::BIGINT as download_retry,
      json_extract(value, '$.parquetConvertedAt')::BIGINT as parquet_converted_at,
      json_extract(value, '$.etag')::VARCHAR as etag
    FROM read_json('{manifestPath}', format='unstructured', records='false', maximum_object_size=10000000) AS t,
    json_each(json_extract(t.json, '$.entries')) AS entries(key, value)
  ),
  needed AS (
    SELECT unnest({keysArray}::VARCHAR[]) as cache_key
  )
SELECT n.cache_key
FROM needed n
LEFT JOIN manifest m ON n.cache_key = m.key
WHERE m.key IS NULL
   OR m.refresh_after < {nowTimestamp}
   OR (m.download_retry > 0 AND m.download_retry < {nowTimestamp})
   {includeParquetCheck}
