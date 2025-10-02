-- DJIA Production Data Loading Verification Queries
-- Run these queries in sqlline after connecting to verify data completeness

-- ====================================================
-- 1. SEC DATA VERIFICATION
-- ====================================================

-- Check DJIA companies are loaded (should return 30 companies)
SELECT COUNT(DISTINCT cik) as djia_company_count
FROM sec_filings
WHERE cik IN (
  SELECT cik FROM cik_registry WHERE group_name = 'DJIA'
);

-- Verify date range coverage (2010-present)
SELECT
  MIN(filing_date) as earliest_filing,
  MAX(filing_date) as latest_filing,
  COUNT(*) as total_filings
FROM sec_filings;

-- Check filing types are properly filtered (no 424B forms)
SELECT
  filing_type,
  COUNT(*) as filing_count
FROM sec_filings
GROUP BY filing_type
ORDER BY filing_count DESC;

-- Verify insider transactions data
SELECT
  COUNT(*) as insider_transaction_count,
  COUNT(DISTINCT cik) as companies_with_insider_data,
  MIN(filing_date) as earliest_transaction,
  MAX(filing_date) as latest_transaction
FROM insider_transactions;

-- Check XBRL facts data completeness
SELECT
  COUNT(*) as xbrl_fact_count,
  COUNT(DISTINCT cik) as companies_with_xbrl,
  COUNT(DISTINCT fact_name) as unique_facts
FROM xbrl_facts;

-- ====================================================
-- 2. ECON DATA VERIFICATION
-- ====================================================

-- Check FRED treasury rates (custom series)
SELECT
  COUNT(*) as treasury_rate_count,
  MIN(date) as earliest_date,
  MAX(date) as latest_date,
  COUNT(DISTINCT series_id) as unique_series
FROM fred_treasury_rates;

-- Verify employment indicators
SELECT
  COUNT(*) as employment_indicator_count,
  MIN(date) as earliest_date,
  MAX(date) as latest_date,
  COUNT(DISTINCT series_id) as unique_indicators
FROM fred_employment_indicators;

-- Check BLS employment statistics
SELECT
  COUNT(*) as employment_stat_count,
  MIN(period_date) as earliest_period,
  MAX(period_date) as latest_period
FROM employment_statistics;

-- Verify Treasury yields data
SELECT
  COUNT(*) as treasury_yield_count,
  MIN(date) as earliest_date,
  MAX(date) as latest_date,
  COUNT(DISTINCT maturity) as unique_maturities
FROM treasury_yields;

-- Check GDP components from BEA
SELECT
  COUNT(*) as gdp_component_count,
  MIN(period_date) as earliest_period,
  MAX(period_date) as latest_period,
  COUNT(DISTINCT component_name) as unique_components
FROM gdp_components;

-- ====================================================
-- 3. GEO DATA VERIFICATION
-- ====================================================

-- Check Census demographic data
SELECT
  COUNT(*) as demographic_record_count,
  COUNT(DISTINCT state) as states_covered,
  COUNT(DISTINCT county) as counties_covered,
  MIN(year) as earliest_year,
  MAX(year) as latest_year
FROM census_demographics;

-- Verify TIGER boundary data
SELECT
  COUNT(*) as boundary_record_count,
  COUNT(DISTINCT state_fips) as states_with_boundaries,
  COUNT(DISTINCT boundary_type) as boundary_types
FROM tiger_boundaries;

-- ====================================================
-- 4. CROSS-DOMAIN INTEGRATION VERIFICATION
-- ====================================================

-- Verify data alignment: SEC companies with economic indicators
SELECT
  c.company_name,
  c.cik,
  COUNT(DISTINCT s.filing_date) as sec_filings,
  -- Economic data should be available for all time periods
  (SELECT COUNT(*) FROM fred_treasury_rates WHERE date >= '2010-01-01') as econ_data_points
FROM cik_registry c
LEFT JOIN sec_filings s ON c.cik = s.cik
WHERE c.group_name = 'DJIA'
GROUP BY c.company_name, c.cik
ORDER BY c.company_name;

-- Check data coverage by year
SELECT
  EXTRACT(YEAR FROM filing_date) as year,
  COUNT(DISTINCT cik) as companies_filing,
  COUNT(*) as total_filings
FROM sec_filings
WHERE cik IN (SELECT cik FROM cik_registry WHERE group_name = 'DJIA')
GROUP BY EXTRACT(YEAR FROM filing_date)
ORDER BY year;

-- ====================================================
-- 5. DATA QUALITY CHECKS
-- ====================================================

-- Check for missing or null critical fields
SELECT
  'sec_filings' as table_name,
  SUM(CASE WHEN cik IS NULL THEN 1 ELSE 0 END) as null_ciks,
  SUM(CASE WHEN filing_date IS NULL THEN 1 ELSE 0 END) as null_dates,
  SUM(CASE WHEN filing_type IS NULL THEN 1 ELSE 0 END) as null_types
FROM sec_filings

UNION ALL

SELECT
  'insider_transactions' as table_name,
  SUM(CASE WHEN cik IS NULL THEN 1 ELSE 0 END) as null_ciks,
  SUM(CASE WHEN filing_date IS NULL THEN 1 ELSE 0 END) as null_dates,
  SUM(CASE WHEN transaction_code IS NULL THEN 1 ELSE 0 END) as null_codes
FROM insider_transactions;

-- Check date range consistency
SELECT
  'Data Range Check' as check_type,
  CASE
    WHEN MIN(filing_date) <= '2010-01-01' AND MAX(filing_date) >= '2024-01-01'
    THEN 'PASS: Full date range covered'
    ELSE 'FAIL: Incomplete date range'
  END as result
FROM sec_filings;

-- ====================================================
-- 6. SAMPLE DATA QUERIES
-- ====================================================

-- Sample: Apple's recent 10-K filings
SELECT
  filing_date,
  filing_type,
  accession_number
FROM sec_filings
WHERE cik = '0000320193'  -- Apple
  AND filing_type IN ('10-K', '10K')
  AND filing_date >= '2020-01-01'
ORDER BY filing_date DESC
LIMIT 5;

-- Sample: Treasury 10-year rates for last 12 months
SELECT
  date,
  value as ten_year_rate
FROM fred_treasury_rates
WHERE series_id = 'DGS10'
  AND date >= CURRENT_DATE - INTERVAL '12' MONTH
ORDER BY date DESC
LIMIT 10;

-- Sample: Unemployment rate trend
SELECT
  date,
  value as unemployment_rate
FROM fred_employment_indicators
WHERE series_id = 'UNRATE'
  AND date >= '2023-01-01'
ORDER BY date DESC
LIMIT 12;

-- ====================================================
-- 7. AWS S3 PERSISTENCE VERIFICATION
-- ====================================================

-- Check if data is being persisted to S3 (this would be environment-specific)
-- The following query checks if the storage provider is configured correctly
SELECT
  'S3 Configuration' as check_type,
  CASE
    WHEN '${GOVDATA_S3_BUCKET}' IS NOT NULL AND '${GOVDATA_S3_BUCKET}' != ''
    THEN 'PASS: S3 bucket configured'
    ELSE 'FAIL: S3 bucket not configured'
  END as result;

-- ====================================================
-- 8. COMPREHENSIVE SUMMARY REPORT
-- ====================================================

-- Final summary of all data sources
SELECT
  'SEC Filings' as data_source,
  COUNT(*) as record_count,
  MIN(filing_date) as start_date,
  MAX(filing_date) as end_date
FROM sec_filings

UNION ALL

SELECT
  'Insider Transactions' as data_source,
  COUNT(*) as record_count,
  MIN(filing_date) as start_date,
  MAX(filing_date) as end_date
FROM insider_transactions

UNION ALL

SELECT
  'XBRL Facts' as data_source,
  COUNT(*) as record_count,
  MIN(filing_date) as start_date,
  MAX(filing_date) as end_date
FROM xbrl_facts

UNION ALL

SELECT
  'Treasury Rates' as data_source,
  COUNT(*) as record_count,
  MIN(date) as start_date,
  MAX(date) as end_date
FROM fred_treasury_rates

UNION ALL

SELECT
  'Employment Indicators' as data_source,
  COUNT(*) as record_count,
  MIN(date) as start_date,
  MAX(date) as end_date
FROM fred_employment_indicators

ORDER BY data_source;