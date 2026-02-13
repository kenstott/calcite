# VSS Integration Plan for GovData Adapter

## Problem

The `vectorized_chunks` Iceberg table contains embeddings but similarity search is slow (full table scan, no vector index). Clients need fast semantic search.

## Solution

ATTACH a pre-built VSS DuckDB file with HNSW index.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    GovData Adapter                       │
├─────────────────────────────────────────────────────────┤
│  DuckDB Catalog                                          │
│  ├── sec.filing_metadata (Iceberg)                      │
│  ├── sec.financial_line_items (Iceberg)                 │
│  ├── sec.mda_sections (Iceberg)                         │
│  └── [vectorized_chunks - SKIP, use vss.chunks]         │
│                                                          │
│  ATTACHED: vss (read-only)                              │
│  └── vss.chunks (HNSW indexed, FLOAT[384])              │
└─────────────────────────────────────────────────────────┘
```

## Implementation

### 1. Download VSS Cache on Init

In `SecSchemaFactory` during schema initialization:

```java
Path vssCache = baseDirectory.resolve("vss/chunks_vss.duckdb");
String vssUrl = "s3://govdata-parquet-v1/cache/vss/chunks_vss.duckdb";

if (!Files.exists(vssCache) || isStale(vssCache, maxAge)) {
    downloadFromS3(vssUrl, vssCache);
}
```

### 2. ATTACH and Load Extensions

When creating DuckDB connection:

```sql
INSTALL vss;
LOAD vss;
INSTALL quackformers FROM community;
LOAD quackformers;

ATTACH '/path/to/vss/chunks_vss.duckdb' AS vss (READ_ONLY);
```

### 3. Skip vectorized_chunks Iceberg Table

In `sec-schema.yaml` or `SecSchemaFactory`, skip exposing `vectorized_chunks` since it's unusable for similarity search without an index.

## Client Usage

```sql
-- Semantic search
SELECT cik, accession_number, yr as year, chunk_text,
       array_cosine_similarity(embedding, embed('china tariffs')::FLOAT[384]) as score
FROM vss.chunks
ORDER BY score DESC
LIMIT 10;

-- Join with filing metadata
SELECT v.chunk_text, v.section, f.company_name, f.filing_date,
       array_cosine_similarity(v.embedding, embed('supply chain risk')::FLOAT[384]) as score
FROM vss.chunks v
JOIN sec.filing_metadata f ON v.accession_number = f.accession_number
ORDER BY score DESC
LIMIT 10;
```

## VSS Table Schema

| Column | Type | Description |
|--------|------|-------------|
| cik | VARCHAR | SEC company identifier |
| accession_number | VARCHAR | Filing identifier |
| yr | INTEGER | Filing year |
| chunk_id | VARCHAR | Unique chunk identifier |
| section | VARCHAR | Document section |
| chunk_text | VARCHAR | Text content |
| embedding | FLOAT[384] | Vector embedding (snowflake-arctic-embed-xs) |

## Configuration

Model.json operand options:

```json
{
  "dataSource": "sec",
  "vssEnabled": true,
  "vssCacheUrl": "s3://govdata-parquet-v1/cache/vss/chunks_vss.duckdb",
  "vssMaxAge": "24h"
}
```

## ETL Integration

The `etl-full-schedule.sh` script automatically rebuilds VSS after ETL:

```bash
# At end of etl-full-schedule.sh
if [[ $COMPLETED -gt 0 ]]; then
    # Extract unique years from SEC jobs (dynamic, not hardcoded)
    SEC_YEARS=$(echo "$JOBS" | grep "|sec|" | awk -F'|' '{print $5}' | sort -rn | uniq)

    for YEAR in $SEC_YEARS; do
        ./vss.sh refresh "$YEAR"
    done
    ./vss.sh upload
fi
```

This ensures the VSS cache stays in sync with Iceberg data for all processed years.

## Files

| File | Purpose |
|------|---------|
| `scripts/vss.sh` | VSS management (refresh, upload, search, stats) |
| `scripts/vss-rebuild-full.sh` | One-time full rebuild from all Iceberg data |
| `s3://govdata-parquet-v1/cache/vss/chunks_vss.duckdb` | Pre-built VSS database |
| `s3://govdata-parquet-v1/cache/vss/metadata.json` | VSS metadata (updated timestamp, row count) |

## Tasks

- [ ] Add VSS download logic to `SecSchemaFactory`
- [ ] Add ATTACH + extension loading to DuckDB connection init
- [ ] Skip `vectorized_chunks` from exposed schema
- [ ] Add `vssEnabled` config option
- [ ] Test semantic search via Calcite SQL
