-- Verification query for issue #138: sec filing_metadata business_address fix
-- Expected: with_business_addr for FY2024 should be close to with_mailing_addr (~4883)

SELECT year, filing_type,
       COUNT(DISTINCT cik) AS filers,
       COUNT(DISTINCT CASE WHEN business_address IS NOT NULL THEN cik END) AS with_business_addr,
       COUNT(DISTINCT CASE WHEN mailing_address IS NOT NULL THEN cik END) AS with_mailing_addr,
       ROUND(100.0 * COUNT(DISTINCT CASE WHEN business_address IS NOT NULL THEN cik END) /
             COUNT(DISTINCT cik), 2) AS pct_with_business_addr,
       ROUND(100.0 * COUNT(DISTINCT CASE WHEN mailing_address IS NOT NULL THEN cik END) /
             COUNT(DISTINCT cik), 2) AS pct_with_mailing_addr
FROM iceberg_scan('s3://govdata-parquet-v1/sec/filing_metadata')
WHERE filing_type = '10-K' AND year IN (2023, 2024, 2025)
GROUP BY year, filing_type ORDER BY year;
