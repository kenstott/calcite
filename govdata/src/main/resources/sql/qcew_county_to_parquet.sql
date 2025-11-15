-- Converts QCEW CSV (inside ZIP) to Parquet for county-level data
-- Uses DuckDB's zipfs extension to read CSV directly from ZIP archive
--
-- Parameters (use String.format to substitute):
--   %1$s - Full path to ZIP file containing QCEW CSV
--   %2$s - CSV filename inside ZIP (e.g., "2020.annual.singlefile.csv")
--   %3$s - Output Parquet file path

INSTALL zipfs;
LOAD zipfs;

COPY (
  SELECT
    q.area_fips,
    q.own_code,
    q.industry_code,
    q.agglvl_code,
    TRY_CAST(q.annual_avg_estabs AS INTEGER) AS annual_avg_estabs,
    TRY_CAST(q.annual_avg_emplvl AS INTEGER) AS annual_avg_emplvl,
    TRY_CAST(q.total_annual_wages AS BIGINT) AS total_annual_wages,
    TRY_CAST(q.annual_avg_wkly_wage AS INTEGER) AS annual_avg_wkly_wage
  FROM read_csv_auto('zip://%1$s/%2$s') q
  WHERE CAST(q.agglvl_code AS VARCHAR) LIKE '7%%'  -- County-level aggregations (70-78)
    AND length(q.area_fips) = 5                     -- 5-digit FIPS codes only
    AND q.area_fips != 'US000'                      -- Exclude national aggregate
) TO '%3$s' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
