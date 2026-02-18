#!/bin/bash
# VSS Semantic Search for vectorized_chunks
# Usage: ./vss-search.sh "your search query" [limit]

set -e

DB_FILE="${VSS_DB_FILE:-/root/calcite/govdata/build/.aperio/sec/vectorized_chunks_vss.duckdb}"
QUERY="$1"
LIMIT="${2:-10}"

if [ -z "$QUERY" ]; then
    echo "Usage: $0 \"search query\" [limit]"
    echo "Example: $0 \"asia tariffs\" 10"
    exit 1
fi

# Use OpenAI to generate embedding for query (requires OPENAI_API_KEY)
if [ -z "$OPENAI_API_KEY" ]; then
    echo "Error: OPENAI_API_KEY not set"
    exit 1
fi

# Get embedding for query
EMBEDDING=$(curl -s https://api.openai.com/v1/embeddings \
    -H "Authorization: Bearer $OPENAI_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{\"input\": \"$QUERY\", \"model\": \"text-embedding-3-small\"}" \
    | jq -r '.data[0].embedding | @json')

if [ "$EMBEDDING" == "null" ] || [ -z "$EMBEDDING" ]; then
    echo "Error: Failed to generate embedding"
    exit 1
fi

# Run similarity search
duckdb "$DB_FILE" << EOSQL
LOAD vss;
SELECT 
    cik,
    accession_number,
    year,
    section,
    LEFT(chunk_text, 300) as chunk_preview,
    array_cosine_similarity(embedding, ${EMBEDDING}::FLOAT[1536]) as similarity
FROM vectorized_chunks
ORDER BY array_cosine_similarity(embedding, ${EMBEDDING}::FLOAT[1536]) DESC
LIMIT ${LIMIT};
EOSQL

