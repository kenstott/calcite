#!/bin/bash
# VSS - Vector Similarity Search for SEC filings
# Uses DuckDB with quackformers embeddings (snowflake-arctic-embed-xs, 384 dims)
#
# Client usage:
#   1. Download: aws s3 cp s3://govdata-parquet-v1/cache/vss/chunks_vss.duckdb ./chunks_vss.duckdb
#   2. Search:   ./vss.sh search "your query" [limit]
#
# Server usage (refresh from Iceberg):
#   ./vss.sh refresh 2024
#   ./vss.sh upload

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VSS_DB="${VSS_DB:-/root/calcite/govdata/build/.aperio/vss/chunks_vss.duckdb}"

# Load environment
if [ -f /root/calcite/govdata/.env.prod ]; then
    source /root/calcite/govdata/.env.prod
fi

case "$1" in
    search)
        QUERY="$2"
        LIMIT="${3:-10}"
        if [ -z "$QUERY" ]; then
            echo "Usage: $0 search \"query\" [limit]"
            exit 1
        fi
        duckdb "$VSS_DB" << EOSQL
INSTALL quackformers FROM community;
LOAD quackformers;
LOAD vss;

SELECT cik, accession_number, yr as year, section,
       LEFT(chunk_text, 250) as preview,
       ROUND(array_cosine_similarity(embedding, embed('${QUERY}')::FLOAT[384]), 3) as score
FROM chunks
ORDER BY score DESC
LIMIT ${LIMIT};
EOSQL
        ;;
    
    refresh)
        YEAR="$2"
        if [ -z "$YEAR" ]; then
            echo "Usage: $0 refresh YEAR"
            exit 1
        fi

        mkdir -p "$(dirname "$VSS_DB")"
        TEMP_PARQUET="/tmp/vss_chunks_${YEAR}.parquet"

        echo "Step 1: Extracting chunks for year $YEAR from Iceberg..."
        duckdb :memory: << EOSQL
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-east-1';
SET s3_access_key_id = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint = '${AWS_ENDPOINT_OVERRIDE}';
SET unsafe_enable_version_guessing = true;

CREATE TEMP TABLE keys AS
SELECT cik, accession_number, chunk_id
FROM iceberg_scan('s3://govdata-parquet-v1/source=sec/SEC/vectorized_chunks')
WHERE "year" = ${YEAR}
LIMIT 50000;

SELECT COUNT(*) as keys FROM keys;

CREATE TABLE chunks AS
SELECT src.cik, src.accession_number, src."year" as yr, src.chunk_id,
       src.section, src.chunk_text, src.embedding
FROM iceberg_scan('s3://govdata-parquet-v1/source=sec/SEC/vectorized_chunks') src
WHERE EXISTS (SELECT 1 FROM keys k WHERE k.cik = src.cik AND k.accession_number = src.accession_number AND k.chunk_id = src.chunk_id);

SELECT COUNT(*) as chunks FROM chunks;
COPY chunks TO '${TEMP_PARQUET}' (FORMAT PARQUET);
EOSQL

        echo "Step 2: Merging into VSS database..."
        if [ ! -f "$VSS_DB" ]; then
            # First run - create new database
            duckdb "$VSS_DB" << EOSQL
INSTALL vss; LOAD vss;
SET hnsw_enable_experimental_persistence = true;

CREATE TABLE chunks (
    cik VARCHAR, accession_number VARCHAR, yr INTEGER, chunk_id VARCHAR,
    section VARCHAR, chunk_text VARCHAR, embedding FLOAT[384]
);
EOSQL
        fi

        # Merge new data (delete old year data first, then insert new)
        duckdb "$VSS_DB" << EOSQL
INSTALL vss; LOAD vss;
SET hnsw_enable_experimental_persistence = true;

DELETE FROM chunks WHERE yr = ${YEAR};

INSERT INTO chunks
SELECT cik, accession_number, yr, chunk_id, section, chunk_text, embedding::FLOAT[384]
FROM read_parquet('${TEMP_PARQUET}');

SELECT ${YEAR} as year, COUNT(*) as chunks FROM chunks WHERE yr = ${YEAR};

-- Rebuild index after data changes
DROP INDEX IF EXISTS chunks_hnsw;
CREATE INDEX chunks_hnsw ON chunks USING HNSW (embedding) WITH (metric = 'cosine');
SELECT 'VSS index rebuilt' as status;
EOSQL
        rm -f "$TEMP_PARQUET"
        ;;
    
    upload)
        echo "Uploading VSS database to R2..."
        export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
        aws s3 cp "$VSS_DB" s3://govdata-parquet-v1/cache/vss/chunks_vss.duckdb \
            --endpoint-url "$AWS_ENDPOINT_OVERRIDE"
        
        ROWS=$(duckdb "$VSS_DB" -c "SELECT COUNT(*) FROM chunks;" -noheader)
        echo "{\"updated\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"rows\": $ROWS, \"embed_dim\": 384, \"model\": \"snowflake-arctic-embed-xs\"}" \
            | aws s3 cp - s3://govdata-parquet-v1/cache/vss/metadata.json \
            --endpoint-url "$AWS_ENDPOINT_OVERRIDE"
        
        echo "Uploaded to s3://govdata-parquet-v1/cache/vss/"
        ;;
    
    stats)
        duckdb "$VSS_DB" -c "SELECT yr as year, COUNT(DISTINCT accession_number) as accessions, COUNT(*) as chunks FROM chunks GROUP BY yr ORDER BY yr;"
        ;;

    *)
        echo "VSS - Vector Similarity Search for SEC filings"
        echo ""
        echo "Usage: $0 [search|refresh|upload|stats]"
        echo ""
        echo "Commands:"
        echo "  search QUERY [LIMIT]  - Semantic search (default limit: 10)"
        echo "  refresh YEAR          - Rebuild index from Iceberg for year"
        echo "  upload                - Upload to R2 for client distribution"
        echo "  stats                 - Show loaded data statistics"
        echo ""
        echo "Client download:"
        echo "  aws s3 cp s3://govdata-parquet-v1/cache/vss/chunks_vss.duckdb ./"
        ;;
esac
