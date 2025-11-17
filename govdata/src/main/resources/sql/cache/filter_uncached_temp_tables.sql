-- Filter uncached download requests using temp tables (optimal for >1000 requests)
-- Parameters: {nowTimestamp}, {includeParquetCheck}
-- Usage: Load manifest first, create needed_downloads table, then execute this query
-- Note: Caller must populate needed_downloads table before executing

SELECT nd.cache_key
FROM needed_downloads nd
LEFT JOIN cached_files cf ON nd.cache_key = cf.key
WHERE cf.key IS NULL
   OR cf.refresh_after < {nowTimestamp}
   OR (cf.download_retry > 0 AND cf.download_retry < {nowTimestamp})
   {includeParquetCheck}
