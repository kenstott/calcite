-- Creates the manifest_metadata table (root table).
-- Stores schema-level information like version and last updated time.
CREATE TABLE IF NOT EXISTS manifest_metadata (
  key VARCHAR PRIMARY KEY,
  value VARCHAR NOT NULL
)
