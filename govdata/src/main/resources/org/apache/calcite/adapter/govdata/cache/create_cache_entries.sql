-- Creates the main cache_entries table.
-- Used by all schemas (ECON, GEO, SEC).
CREATE TABLE IF NOT EXISTS cache_entries (
  cache_key VARCHAR PRIMARY KEY,
  data_type VARCHAR NOT NULL,
  parameters VARCHAR,
  file_path VARCHAR,
  file_size BIGINT DEFAULT 0,
  cached_at BIGINT NOT NULL,
  refresh_after BIGINT NOT NULL,
  refresh_reason VARCHAR,
  etag VARCHAR,
  output_path VARCHAR,
  materialized_at BIGINT DEFAULT 0,
  last_error VARCHAR,
  error_count INTEGER DEFAULT 0,
  last_attempt_at BIGINT DEFAULT 0,
  download_retry BIGINT DEFAULT 0
)
