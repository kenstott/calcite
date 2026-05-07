-- Convert QCEW county wages CSV to Parquet with state enrichment (ANNUAL format)
-- Uses DuckDB's zipfs extension to read CSV directly from ZIP archive
-- Parameters: {year}, {zipPath}, {csvFilename}, {stateFipsPath}, {parquetPath}
-- Filters for county-level (agglvl_code=70), all ownership (own_code=0), all industries (industry_code=10)

INSTALL zipfs;
LOAD zipfs;

COPY (
  SELECT
    q.area_fips AS county_fips,
    substring(q.area_fips, 1, 2) AS state_fips,
    s.state_name,
    TRY_CAST(q.annual_avg_wkly_wage AS INTEGER) AS average_weekly_wage,
    TRY_CAST(q.annual_avg_emplvl AS INTEGER) AS total_employment,
    {year} AS "year"
  FROM read_csv_auto('zip://{zipPath}/{csvFilename}') q
  LEFT JOIN read_json_auto('{stateFipsPath}') s
    ON substring(q.area_fips, 1, 2) = s.fips_code
  WHERE CAST(q.agglvl_code AS VARCHAR) = '70'
    AND CAST(q.own_code AS VARCHAR) = '0'
    AND CAST(q.industry_code AS VARCHAR) = '10'
    AND length(q.area_fips) = 5
) TO '{parquetPath}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
