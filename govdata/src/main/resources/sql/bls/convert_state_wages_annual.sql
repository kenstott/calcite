-- Convert QCEW state wages CSV to Parquet with state enrichment (ANNUAL format)
-- Uses DuckDB's zipfs extension to read CSV directly from ZIP archive
-- Parameters: {year}, {zipPath}, {csvFilename}, {stateFipsPath}, {parquetPath}
-- Filters for state-level (agglvl_code=50), all ownership (own_code=0), all industries (industry_code=10)
-- State codes end with 000 (e.g., 01000 for Alabama statewide)

INSTALL zipfs;
LOAD zipfs;

COPY (
  SELECT
    substring(q.area_fips, 1, 2) AS state_fips,
    s.state_name,
    TRY_CAST(q.annual_avg_wkly_wage AS INTEGER) AS average_weekly_wage,
    TRY_CAST(q.annual_avg_emplvl AS INTEGER) AS total_employment,
    {year} AS "year"
  FROM read_csv_auto('zip://{zipPath}/{csvFilename}') q
  LEFT JOIN read_json_auto('{stateFipsPath}') s
    ON substring(q.area_fips, 1, 2) = s.fips_code
  WHERE CAST(q.agglvl_code AS VARCHAR) = '50'
    AND CAST(q.own_code AS VARCHAR) = '0'
    AND CAST(q.industry_code AS VARCHAR) = '10'
    AND length(q.area_fips) = 5
    AND q.area_fips LIKE '%000'
) TO '{parquetPath}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
