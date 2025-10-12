-- Test Apple SEC data with MinIO S3 and DuckDB execution engine
-- This should read parquet files from s3://govdata-parquet

-- Show all available tables
!tables

-- Count records in financial_line_items for Apple
SELECT COUNT(*) as total_facts
FROM financial_line_items
WHERE cik = '0000320193';

-- Create a view with Apple's revenue over time
CREATE OR REPLACE VIEW apple_revenue AS
SELECT
  filing_date,
  fiscal_year,
  fiscal_period,
  label,
  value,
  unit
FROM financial_line_items
WHERE cik = '0000320193'
  AND label LIKE '%Revenue%'
  AND value IS NOT NULL
ORDER BY filing_date DESC;

-- Query the view
SELECT * FROM apple_revenue LIMIT 10;

-- Verify DuckDB is being used by checking execution
EXPLAIN PLAN FOR
SELECT filing_date, SUM(value) as total_revenue
FROM financial_line_items
WHERE cik = '0000320193'
  AND label LIKE '%Revenue%'
GROUP BY filing_date
ORDER BY filing_date DESC;

-- Test aggregation
SELECT
  fiscal_year,
  COUNT(*) as num_facts,
  COUNT(DISTINCT filing_date) as num_filings
FROM financial_line_items
WHERE cik = '0000320193'
GROUP BY fiscal_year
ORDER BY fiscal_year DESC;
