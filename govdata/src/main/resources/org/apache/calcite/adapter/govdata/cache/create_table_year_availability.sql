-- Creates the table_year_availability table.
-- Caches available years per BEA/BLS table to avoid querying
-- unavailable year+linecode combinations.
CREATE TABLE IF NOT EXISTS table_year_availability (
  data_source VARCHAR NOT NULL,      -- 'bea_regional', 'bls', 'fred', etc.
  table_name VARCHAR NOT NULL,       -- 'SAACCompRatio', 'SAINC1', etc.
  available_years VARCHAR NOT NULL,  -- JSON array: '[2010,2011,2012,...,2022]'
  max_year INTEGER,                  -- Quick lookup: max year available
  min_year INTEGER,                  -- Quick lookup: min year available
  cached_at BIGINT NOT NULL,
  refresh_after BIGINT NOT NULL,
  PRIMARY KEY (data_source, table_name)
)
