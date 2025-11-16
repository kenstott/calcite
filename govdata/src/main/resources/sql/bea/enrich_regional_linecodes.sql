-- Enrich BEA Regional LineCode catalog with metadata derived from table name
-- Parameters: {tableName}, {jsonPath}, {parquetPath}
-- Usage (CLI):
--   sed -e 's/{tableName}/SAINC1/g' \
--       -e 's|{jsonPath}|/path/to/linecodes.json|g' \
--       -e 's|{parquetPath}|/path/to/output.parquet|g' \
--       enrich_regional_linecodes.sql | duckdb
--
-- Table name prefixes:
--   SA*/SQ* = State (Annual/Quarterly)
--   CA* = County
--   MA* = Metropolitan Statistical Area
--   MI* = Micropolitan
--
-- Data categories extracted from table name patterns:
--   *INC* = Income, *GDP* = GDP, *ACE* = Accounts, *RP* = Regional Price, *PCE* = Personal Consumption

COPY (
  SELECT
    '{tableName}' AS TableName,
    Key AS LineCode,
    "Desc" AS Description,
    substring('{tableName}', 1, 2) AS table_prefix,
    CASE
      WHEN ('{tableName}' LIKE '%INC%') THEN 'INC'
      WHEN ('{tableName}' LIKE '%GDP%') THEN 'GDP'
      WHEN ('{tableName}' LIKE '%ACE%' OR '{tableName}' LIKE '%SAAC%') THEN 'ACE'
      WHEN ('{tableName}' LIKE '%RP%') THEN 'RPP'
      WHEN ('{tableName}' LIKE '%PCE%') THEN 'PCE'
      ELSE NULL
    END AS data_category,
    CASE
      WHEN ('{tableName}' LIKE 'SA%' OR '{tableName}' LIKE 'SQ%') THEN 'state'
      WHEN ('{tableName}' LIKE 'CA%') THEN 'county'
      WHEN ('{tableName}' LIKE 'MA%') THEN 'msa'
      WHEN ('{tableName}' LIKE 'MI%') THEN 'micropolitan'
      ELSE NULL
    END AS geography_level,
    CASE
      WHEN ('{tableName}' LIKE 'SA%' OR '{tableName}' LIKE 'CA%' OR '{tableName}' LIKE 'MA%') THEN 'annual'
      WHEN ('{tableName}' LIKE 'SQ%') THEN 'quarterly'
      ELSE NULL
    END AS frequency
  FROM read_json('{jsonPath}', format='array', maximum_object_size=104857600)
) TO '{parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
