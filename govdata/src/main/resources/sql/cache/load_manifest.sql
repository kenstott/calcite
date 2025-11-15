-- Load cache manifest JSON into DuckDB temp table
-- Parameters: {manifestPath}
-- Usage (CLI): sed 's|{manifestPath}|/path/to/manifest.json|g' load_manifest.sql | duckdb

CREATE TEMP TABLE cached_files AS
SELECT
  key,
  json_extract(value, '$.cachedAt')::BIGINT as cached_at,
  json_extract(value, '$.refreshAfter')::BIGINT as refresh_after,
  json_extract(value, '$.downloadRetry')::BIGINT as download_retry,
  json_extract(value, '$.etag')::VARCHAR as etag,
  json_extract(value, '$.lastError')::VARCHAR as last_error
FROM read_json('{manifestPath}',
  format='unstructured',
  records='false',
  maximum_object_size=104857600,
  ignore_errors=true
) AS manifest,
json_each(json_extract(manifest.json, '$.entries')) AS entries(key, value)
