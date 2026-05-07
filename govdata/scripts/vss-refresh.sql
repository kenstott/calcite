-- VSS Incremental Refresh
-- Only loads accessions not already in local table

INSTALL vss;
LOAD vss;
INSTALL iceberg;
LOAD iceberg;
SET unsafe_enable_version_guessing = true;

-- Insert only new accessions
INSERT INTO vectorized_chunks
SELECT src.*
FROM iceberg_scan('s3://govdata-parquet-v1/sec/vectorized_chunks') src
WHERE src.accession_number NOT IN (
    SELECT accession_number FROM loaded_accessions
)
AND src.embedding IS NOT NULL;

-- Track newly loaded accessions
INSERT INTO loaded_accessions (accession_number)
SELECT DISTINCT accession_number 
FROM iceberg_scan('s3://govdata-parquet-v1/sec/vectorized_chunks')
WHERE accession_number NOT IN (
    SELECT accession_number FROM loaded_accessions
);

-- Rebuild HNSW index after refresh
DROP INDEX IF EXISTS vec_hnsw_idx;
CREATE INDEX vec_hnsw_idx ON vectorized_chunks USING HNSW (embedding)
WITH (metric = 'cosine');

-- Stats
SELECT 
    (SELECT COUNT(DISTINCT accession_number) FROM vectorized_chunks) as total_accessions,
    (SELECT COUNT(*) FROM vectorized_chunks) as total_chunks,
    (SELECT MAX(loaded_at) FROM loaded_accessions) as last_refresh;

