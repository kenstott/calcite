-- Filter uncached download requests using direct lookup (no json_each expansion)
-- Parameters: {manifestPath}, {keysArray}, {nowTimestamp}, {includeMaterializedCheck}
-- This version directly looks up each key instead of expanding all entries

WITH
  manifest_root AS (
    SELECT json FROM read_json('{manifestPath}', format='unstructured', records='false', maximum_object_size=104857600)
  ),
  needed AS (
    SELECT unnest({keysArray}::VARCHAR[]) as cache_key
  )
SELECT n.cache_key
FROM needed n
LEFT JOIN (
  SELECT
    n.cache_key as key,
    json_extract(m.json, '$.entries."' || n.cache_key || '".refreshAfter')::BIGINT as refresh_after,
    json_extract(m.json, '$.entries."' || n.cache_key || '".downloadRetry')::BIGINT as download_retry,
    json_extract(m.json, '$.entries."' || n.cache_key || '".materializedAt')::BIGINT as materialized_at
  FROM needed n, manifest_root m
) manifest ON n.cache_key = manifest.key
WHERE manifest.key IS NULL
   OR manifest.refresh_after IS NULL
   OR manifest.refresh_after < {nowTimestamp}
   OR (manifest.download_retry > 0 AND manifest.download_retry < {nowTimestamp})
   {includeMaterializedCheck}
