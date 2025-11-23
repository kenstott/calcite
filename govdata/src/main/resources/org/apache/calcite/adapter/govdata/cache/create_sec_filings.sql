-- Creates SEC-specific filings tracking table.
-- Tracks individual filing files (downloaded, not_found, etc).
CREATE TABLE IF NOT EXISTS sec_filings (
  filing_key VARCHAR PRIMARY KEY,
  cik VARCHAR NOT NULL,
  accession VARCHAR NOT NULL,
  file_name VARCHAR NOT NULL,
  state VARCHAR NOT NULL,
  reason VARCHAR,
  checked_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sec_filings_cik ON sec_filings(cik)
