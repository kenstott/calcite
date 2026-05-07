#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GOVDATA_HOME="${GOVDATA_HOME:-$(dirname "$SCRIPT_DIR")}"
VSS_DB="${VSS_DB:-$GOVDATA_HOME/build/.aperio/vss/chunks_vss.duckdb}"

# Load environment
if [[ -f "$GOVDATA_HOME/.env.prod" ]]; then
    source "$GOVDATA_HOME/.env.prod"
fi

if [[ -z "$AWS_ACCESS_KEY_ID" ]]; then
    echo "Error: AWS credentials not set. Source .env.prod first."
    exit 1
fi

echo "=============================================="
echo "VSS Full Rebuild"
echo "=============================================="
echo "Target: $VSS_DB"
echo ""

# Remove existing database
rm -f "$VSS_DB"
mkdir -p "$(dirname "$VSS_DB")"

# Get all years with embeddings
echo "Querying Iceberg for available years..."
YEARS=$(duckdb -csv -noheader << EOSQL 2>/dev/null | grep -E '^[0-9]+$' | sort -rn
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-east-1';
SET s3_access_key_id = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';
SET s3_endpoint = '${AWS_ENDPOINT_OVERRIDE}';
SET unsafe_enable_version_guessing = true;

SELECT DISTINCT "year"
FROM iceberg_scan('s3://govdata-parquet-v1/sec/vectorized_chunks')
WHERE embedding IS NOT NULL;
EOSQL
)

if [[ -z "$YEARS" ]]; then
    echo "Error: No years found with embeddings"
    exit 1
fi

echo "Found years: $(echo $YEARS | tr '\n' ' ')"
echo ""

# Initialize empty VSS database
echo "Initializing VSS database..."
duckdb "$VSS_DB" << 'EOSQL'
INSTALL vss; LOAD vss;
SET hnsw_enable_experimental_persistence = true;

CREATE TABLE chunks (
    cik VARCHAR,
    accession_number VARCHAR,
    yr INTEGER,
    chunk_id VARCHAR,
    section VARCHAR,
    chunk_text VARCHAR,
    embedding FLOAT[384]
);
EOSQL

# Process each year
TOTAL_CHUNKS=0
for YEAR in $YEARS; do
    echo "Processing year $YEAR..."
    TEMP_PARQUET="/tmp/vss_chunks_${YEAR}.parquet"

    # Extract from Iceberg (using workaround for predicate pushdown bug)
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
FROM iceberg_scan('s3://govdata-parquet-v1/sec/vectorized_chunks')
WHERE "year" = ${YEAR};

CREATE TABLE chunks AS
SELECT src.cik, src.accession_number, src."year" as yr, src.chunk_id,
       src.section, src.chunk_text, src.embedding
FROM iceberg_scan('s3://govdata-parquet-v1/sec/vectorized_chunks') src
WHERE EXISTS (SELECT 1 FROM keys k WHERE k.cik = src.cik AND k.accession_number = src.accession_number AND k.chunk_id = src.chunk_id);

COPY chunks TO '${TEMP_PARQUET}' (FORMAT PARQUET);
EOSQL

    # Load into VSS database
    YEAR_CHUNKS=$(duckdb "$VSS_DB" << EOSQL
INSTALL vss; LOAD vss;
SET hnsw_enable_experimental_persistence = true;

INSERT INTO chunks
SELECT cik, accession_number, yr, chunk_id, section, chunk_text, embedding::FLOAT[384]
FROM read_parquet('${TEMP_PARQUET}');

SELECT COUNT(*) FROM chunks WHERE yr = ${YEAR};
EOSQL
    )

    YEAR_COUNT=$(echo "$YEAR_CHUNKS" | tail -1 | tr -d '[:space:]|')
    echo "  Year $YEAR: $YEAR_COUNT chunks"
    TOTAL_CHUNKS=$((TOTAL_CHUNKS + YEAR_COUNT))

    rm -f "$TEMP_PARQUET"
done

# Build HNSW index
echo ""
echo "Building HNSW index..."
duckdb "$VSS_DB" << 'EOSQL'
INSTALL vss; LOAD vss;
SET hnsw_enable_experimental_persistence = true;

CREATE INDEX chunks_hnsw ON chunks USING HNSW (embedding) WITH (metric = 'cosine');
EOSQL

echo ""
echo "=============================================="
echo "VSS Full Rebuild Complete"
echo "=============================================="
echo "Total chunks: $TOTAL_CHUNKS"
echo "Database: $VSS_DB"
echo "Size: $(ls -lh "$VSS_DB" | awk '{print $5}')"
echo ""

# Upload to S3
read -p "Upload to S3? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Uploading to S3..."
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
    aws s3 cp "$VSS_DB" s3://govdata-parquet-v1/cache/vss/chunks_vss.duckdb \
        --endpoint-url "$AWS_ENDPOINT_OVERRIDE"

    echo "{\"rebuilt\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"chunks\": $TOTAL_CHUNKS, \"years\": [$(echo $YEARS | tr '\n' ',' | sed 's/,$//')], \"embed_dim\": 384}" \
        | aws s3 cp - s3://govdata-parquet-v1/cache/vss/metadata.json \
        --endpoint-url "$AWS_ENDPOINT_OVERRIDE"

    echo "Uploaded to s3://govdata-parquet-v1/cache/vss/"
fi
