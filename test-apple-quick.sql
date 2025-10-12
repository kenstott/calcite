-- Quick test - just show tables
!tables

-- Count Apple filings
SELECT COUNT(*) FROM financial_line_items WHERE cik = '0000320193';
