-- Creates the table_geo_support table.
-- Caches supported geography types per BEA Regional table to avoid
-- generating invalid geography combinations.
-- Populated by querying BEA GetParameterValuesFiltered API for each table.
CREATE TABLE IF NOT EXISTS table_geo_support (
  data_source VARCHAR NOT NULL,      -- 'bea_regional', etc.
  table_name VARCHAR NOT NULL,       -- 'CAINC1', 'CAINC91', etc.
  supports_state BOOLEAN DEFAULT FALSE,
  supports_county BOOLEAN DEFAULT FALSE,
  supports_msa BOOLEAN DEFAULT FALSE,
  supports_mic BOOLEAN DEFAULT FALSE,
  supports_port BOOLEAN DEFAULT FALSE,
  supports_div BOOLEAN DEFAULT FALSE,
  supports_csa BOOLEAN DEFAULT FALSE,
  cached_at BIGINT NOT NULL,
  refresh_after BIGINT NOT NULL,
  PRIMARY KEY (data_source, table_name)
)
