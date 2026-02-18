-- VSS Incremental Refresh - Single Year
-- Usage: duckdb db.duckdb -c "SET VARIABLE target_year = 2024;" < vss-refresh-year.sql

INSTALL vss;
LOAD vss;
INSTALL iceberg;
LOAD iceberg;
SET unsafe_enable_version_guessing = true;

-- Insert only new accessions for target year
INSERT INTO vectorized_chunks
SELECT src.*
FROM iceberg_scan('s3://govdata-parquet-v1/source=sec/SEC/vectorized_chunks') src
WHERE src.year = getvariable('target_year')
  AND src.accession_number NOT IN (
    SELECT accession_number FROM loaded_accessions
  )
  AND src.embedding IS NOT NULL;

-- Track newly loaded accessions
INSERT OR IGNORE INTO loaded_accessions (accession_number)
SELECT DISTINCT accession_number 
FROM iceberg_scan('s3://govdata-parquet-v1/source=sec/SEC/vectorized_chunks')
WHERE year = getvariable('target_year')
  AND accession_number NOT IN (
    SELECT accession_number FROM loaded_accessions
  );

-- Rebuild HNSW index
DROP INDEX IF EXISTS vec_hnsw_idx;
CREATE INDEX vec_hnsw_idx ON vectorized_chunks USING HNSW (embedding)
WITH (metric = 'cosine');

SELECT 
    getvariable('target_year') as year_loaded,
    (SELECT COUNT(DISTINCT accession_number) FROM vectorized_chunks) as total_accessions,
    (SELECT COUNT(*) FROM vectorized_chunks) as total_chunks;

