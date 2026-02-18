-- VSS Index Setup for vectorized_chunks
INSTALL vss;
LOAD vss;
SET hnsw_enable_experimental_persistence = true;

-- Create table matching Iceberg schema exactly
CREATE TABLE IF NOT EXISTS vectorized_chunks (
    cik VARCHAR,
    accession_number VARCHAR,
    year INTEGER,
    chunk_id VARCHAR,
    source_type VARCHAR,
    section VARCHAR,
    sequence INTEGER,
    filing_date VARCHAR,
    chunk_text VARCHAR,
    enriched_text VARCHAR,
    embedding DOUBLE[],
    content_type VARCHAR,
    financial_concepts VARCHAR,
    exhibit_number VARCHAR,
    speaker_name VARCHAR,
    speaker_role VARCHAR,
    paragraph_number INTEGER
);

-- Track loaded accessions for incremental refresh
CREATE TABLE IF NOT EXISTS loaded_accessions (
    accession_number VARCHAR PRIMARY KEY,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
