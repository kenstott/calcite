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
