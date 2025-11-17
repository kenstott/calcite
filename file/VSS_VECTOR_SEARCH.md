# DuckDB VSS (Vector Similarity Search) Guide

## Overview

The DuckDB file adapter now automatically loads the **vss** extension for query-time optimization of vector similarity searches. This enables approximate nearest neighbor search using HNSW (Hierarchical Navigable Small World) indexes, providing 10-100x speedup for similarity queries.

## Key Concepts

### VSS = Query-Time Optimization

VSS is **NOT** used during data conversion - it's loaded in the persistent DuckDB catalog to optimize similarity queries at runtime.

**Separation of Concerns:**
- **Conversion time**: Use quackformers extension to generate embeddings
- **Query time**: Use vss extension to search embeddings efficiently

## Extension Loading

The vss extension is automatically loaded when creating a DuckDB JDBC schema:

```java
// In DuckDBJdbcSchemaFactory.java
private static void loadQueryExtensions(Connection conn) {
  String[][] extensions = {
      {"vss", ""},    // Vector Similarity Search
      {"fts", ""}     // Full-Text Search
  };
  // ... gracefully loads extensions
}
```

**Graceful Degradation**: If vss is not available, queries fall back to DuckDB's built-in `list_cosine_similarity()` function (slower but still functional).

## HNSW Index Creation

### Basic Index

Create an HNSW index on an embedding column:

```sql
CREATE INDEX embedding_hnsw_idx ON table_name
USING HNSW (embedding_column)
WITH (metric = 'cosine');
```

### Index Parameters

```sql
CREATE INDEX embedding_hnsw_idx ON table_name
USING HNSW (embedding_column)
WITH (
  metric = 'cosine',     -- Distance metric: 'cosine', 'l2', or 'ip' (inner product)
  ef_construction = 128, -- Build quality (higher = better accuracy, slower build)
  M = 16                 -- Connections per node (higher = better recall, more memory)
);
```

**Metrics:**
- `cosine`: Cosine distance (1 - cosine_similarity), best for normalized vectors
- `l2`: Euclidean distance
- `ip`: Inner product (for maximum similarity)

**Parameters:**
- `ef_construction` (default: 128): Higher values = better index quality but slower build
- `M` (default: 16): More connections = better recall but higher memory usage

## Query Patterns

### Without Index (Brute Force)

Scans all rows - O(n) complexity:

```sql
SELECT id, text,
  list_cosine_similarity(embedding, [0.5, 0.5, ...]) as similarity
FROM documents
ORDER BY similarity DESC
LIMIT 10;
```

**Use when:**
- Small datasets (<1000 rows)
- One-time queries
- Index build time not worth the cost

### With HNSW Index (Approximate)

Uses approximate nearest neighbor - O(log n) complexity:

```sql
SELECT id, text,
  array_distance(embedding, [0.5, 0.5, ...]::FLOAT[384]) as distance
FROM documents
ORDER BY distance
LIMIT 10;
-- Automatically uses HNSW index if available
```

**Use when:**
- Large datasets (>10,000 rows)
- Frequent similarity queries
- Near-exact results acceptable (99%+ recall)

### Threshold Filtering

Combine with WHERE clause for threshold-based search:

```sql
SELECT id, text, array_distance(embedding, query_vector) as dist
FROM documents
WHERE array_distance(embedding, query_vector) < 0.3  -- Distance threshold
ORDER BY dist
LIMIT 10;
```

## Performance Characteristics

### Index Build Time

| Rows | Dimensions | Build Time (approx) |
|------|------------|---------------------|
| 10K  | 384        | 5-10 seconds        |
| 100K | 384        | 1-2 minutes         |
| 1M   | 384        | 10-15 minutes       |

**Note**: Build once, query many times for best ROI.

### Query Speedup

**Expected speedup with HNSW index:**
- Small datasets (1K-10K): 2-5x faster
- Medium datasets (10K-100K): 10-50x faster
- Large datasets (100K-1M+): 50-100x faster

**Trade-off**: Approximate results (typically 95-99% recall vs exact search)

## Integration with Quackformers

### Data Conversion (Quackformers)

Generate embeddings during Parquet conversion:

```json
{
  "columns": [
    {"name": "text", "type": "varchar"},
    {
      "name": "text_embedding",
      "type": "array<double>",
      "expression": "embed(text)::FLOAT[384]",
      "comment": "384-dim embedding via quackformers"
    }
  ]
}
```

### Query Optimization (VSS)

After data is loaded, create HNSW index for fast queries:

```sql
-- Create index (one-time operation)
CREATE INDEX text_emb_idx ON documents
USING HNSW (text_embedding)
WITH (metric = 'cosine');

-- Query with index optimization
SELECT text, array_distance(text_embedding, embed('search query')) as relevance
FROM documents
ORDER BY relevance
LIMIT 10;
```

## Complete Example

### Step 1: Schema with Expression Column

```json
{
  "partitionedTables": [{
    "name": "knowledge_base",
    "columns": [
      {"name": "id", "type": "integer"},
      {"name": "title", "type": "varchar"},
      {"name": "content", "type": "varchar"},
      {
        "name": "content_embedding",
        "type": "array<double>",
        "expression": "CASE WHEN length(content) > 100 THEN embed(content)::FLOAT[384] ELSE NULL END"
      }
    ]
  }]
}
```

### Step 2: Data Conversion

```java
// Conversion happens with quackformers extension loaded
// AbstractGovDataDownloader.convertCachedJsonToParquet()
// Embeddings generated via SQL expression
```

### Step 3: Create HNSW Index

```sql
-- After data is loaded into DuckDB
CREATE INDEX kb_content_idx ON knowledge_base
USING HNSW (content_embedding)
WITH (metric = 'cosine', ef_construction = 200, M = 32);
```

### Step 4: Semantic Search

```sql
-- Find documents similar to a query
SELECT
  id,
  title,
  array_distance(content_embedding, embed('risk management strategies')) as relevance
FROM knowledge_base
WHERE content_embedding IS NOT NULL
ORDER BY relevance
LIMIT 10;
```

## Best Practices

### When to Use HNSW Indexes

✅ **Use HNSW when:**
- Dataset has >10,000 embeddings
- Similarity queries are frequent
- 95-99% recall is acceptable
- Query latency is important

❌ **Don't use HNSW when:**
- Dataset is small (<1,000 rows)
- Exact nearest neighbors required (use brute force)
- Embeddings change frequently (index rebuild overhead)

### Index Maintenance

**Rebuilding indexes** after data updates:

```sql
-- Drop old index
DROP INDEX kb_content_idx;

-- Recreate with fresh data
CREATE INDEX kb_content_idx ON knowledge_base
USING HNSW (content_embedding)
WITH (metric = 'cosine');
```

**Incremental updates**: HNSW indexes don't auto-update. For frequently changing data, consider:
- Rebuilding nightly
- Partitioning by date and indexing stable partitions only
- Using brute force for recent data, HNSW for historical

### Monitoring Index Usage

Check if index is being used:

```sql
EXPLAIN SELECT id, array_distance(embedding, query) as dist
FROM documents
ORDER BY dist
LIMIT 10;
-- Look for "Index Scan using HNSW" in plan
```

## Troubleshooting

### VSS Extension Not Available

**Symptom**: Warnings in logs about failed extension load

**Solution**: VSS extension may not be available for your DuckDB version or platform.

**Fallback**: Queries automatically use `list_cosine_similarity()` (slower but functional)

### Poor Index Performance

**Symptoms**: Index queries slower than expected

**Diagnoses:**
1. Check index parameters - try higher `ef_construction` and `M`
2. Verify metric matches your use case (cosine vs l2 vs ip)
3. Ensure embeddings are normalized if using cosine distance
4. Check dataset size - HNSW benefit grows with size

### Memory Issues

**Symptom**: OOM during index creation

**Solution**: Reduce `M` parameter (fewer connections per node):

```sql
CREATE INDEX emb_idx ON table
USING HNSW (embedding)
WITH (metric = 'cosine', M = 8);  -- Reduced from default 16
```

## References

- **DuckDB VSS Extension**: https://github.com/duckdb/duckdb/tree/main/extension/vss
- **HNSW Algorithm**: https://arxiv.org/abs/1603.09320
- **Quackformers** (conversion-time): `govdata/DUCKDB_EXTENSIONS.md`
- **Phase 5 Implementation**: `duckdb_upgrades.md`

## Version History

- **Phase 5 (2025-11-10)**: Added automatic VSS extension loading in DuckDBJdbcSchemaFactory
- Query-time optimization for vector similarity search
- Graceful degradation when VSS not available
