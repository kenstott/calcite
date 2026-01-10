-- Tracks incremental reorganization status for alternate partitions.
-- Supports incremental processing by tracking completion per incremental key combination.
-- For example, with incremental_keys=[year], tracks completion per year.
CREATE TABLE IF NOT EXISTS partition_status (
  alternate_name VARCHAR NOT NULL,
  incremental_key_values VARCHAR NOT NULL,  -- JSON: '{"year":"2020"}' or '{}'
  source_table VARCHAR NOT NULL,
  target_pattern VARCHAR,
  processed_at BIGINT NOT NULL,
  row_count INTEGER DEFAULT NULL,  -- NULL=legacy/unknown, 0=empty (requery after TTL), >0=has data
  error_status BOOLEAN DEFAULT FALSE,  -- TRUE=error occurred, retry after error TTL
  error_message VARCHAR DEFAULT NULL,  -- Optional error message for debugging
  PRIMARY KEY (alternate_name, incremental_key_values)
);

-- Add row_count column to existing tables (migration)
ALTER TABLE partition_status ADD COLUMN IF NOT EXISTS row_count INTEGER DEFAULT NULL;

-- Add error tracking columns (migration)
ALTER TABLE partition_status ADD COLUMN IF NOT EXISTS error_status BOOLEAN DEFAULT FALSE;
ALTER TABLE partition_status ADD COLUMN IF NOT EXISTS error_message VARCHAR DEFAULT NULL;
