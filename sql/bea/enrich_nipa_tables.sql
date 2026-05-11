-- Enrich BEA NIPA tables catalog with structured metadata
-- Parses TableName to extract section, family, metric components
-- Parameters: {jsonPath}
-- Usage (CLI): sed 's|{jsonPath}|/path/to/nipa_tables.json|g' enrich_nipa_tables.sql | duckdb
--
-- TableName format: T{section}{family}{metric}
--   Example: T10101 → section=1, family=01, metric=01
--
-- Section codes:
--   1=domestic_product_income, 2=personal_income_outlays, 3=government,
--   4=foreign_transactions, 5=saving_investment, 6=industry,
--   7=supplemental, 8=not_seasonally_adjusted

CREATE TEMP TABLE enriched AS
SELECT
  TableName,
  Description,
  CASE
    WHEN TableName LIKE 'T%' AND length(TableName) >= 4 THEN substring(TableName, 2, 1)
    ELSE NULL
  END AS section,
  CASE substring(TableName, 2, 1)
    WHEN '1' THEN 'domestic_product_income'
    WHEN '2' THEN 'personal_income_outlays'
    WHEN '3' THEN 'government'
    WHEN '4' THEN 'foreign_transactions'
    WHEN '5' THEN 'saving_investment'
    WHEN '6' THEN 'industry'
    WHEN '7' THEN 'supplemental'
    WHEN '8' THEN 'not_seasonally_adjusted'
    ELSE 'unknown'
  END AS section_name,
  CASE
    WHEN TableName LIKE 'T%' AND length(TableName) >= 4 THEN substring(TableName, 3, 2)
    ELSE NULL
  END AS family,
  CASE
    WHEN TableName LIKE 'T%' AND length(TableName) > 4 THEN substring(TableName, 5)
    ELSE NULL
  END AS metric,
  CASE
    WHEN TableName LIKE 'T%' AND length(TableName) >= 4 THEN
      substring(TableName, 2, 1) || '.' || substring(TableName, 3, 2) || '.' || substring(TableName, 5)
    ELSE NULL
  END AS table_number,
  contains(Description, '(A)') AS annual,
  contains(Description, '(Q)') AS quarterly
FROM read_json('{jsonPath}', format='array', maximum_object_size=104857600)
