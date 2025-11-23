-- Creates ECON-specific catalog_series_cache table.
-- Caches extracted FRED catalog series lists by popularity threshold.
CREATE TABLE IF NOT EXISTS catalog_series_cache (
  cache_key VARCHAR PRIMARY KEY,
  min_popularity INTEGER NOT NULL,
  series_ids VARCHAR NOT NULL,
  cached_at BIGINT NOT NULL,
  refresh_after BIGINT NOT NULL,
  refresh_reason VARCHAR
)
