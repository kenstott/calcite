-- Tracks incremental reorganization status for alternate partitions.
-- Supports incremental processing by tracking completion per incremental key combination.
-- For example, with incremental_keys=[year], tracks completion per year.
CREATE TABLE IF NOT EXISTS partition_status (
  alternate_name VARCHAR NOT NULL,
  incremental_key_values VARCHAR NOT NULL,  -- JSON: '{"year":"2020"}' or '{}'
  source_table VARCHAR NOT NULL,
  target_pattern VARCHAR,
  processed_at BIGINT NOT NULL,
  PRIMARY KEY (alternate_name, incremental_key_values)
);
