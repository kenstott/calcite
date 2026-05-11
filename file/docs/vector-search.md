# Vector Search and Similarity Functions

## Overview

The File Adapter includes built-in support for vector similarity search and text embeddings. This enables semantic search, document similarity, and nearest-neighbor queries directly in SQL — no external vector database required.

## SQL Similarity Functions

The following functions are automatically registered in every File Adapter schema and work with all execution engines.

### Vector Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `COSINE_SIMILARITY` | `(vector1, vector2) → DOUBLE` | Cosine similarity between two vectors (-1 to 1, where 1 = identical direction) |
| `COSINE_DISTANCE` | `(vector1, vector2) → DOUBLE` | Cosine distance (1 - cosine_similarity, 0 to 2, where 0 = identical) |
| `EUCLIDEAN_DISTANCE` | `(vector1, vector2) → DOUBLE` | L2 distance between two vectors |
| `DOT_PRODUCT` | `(vector1, vector2) → DOUBLE` | Scalar product of two vectors |
| `VECTORS_SIMILAR` | `(vector1, vector2, threshold) → BOOLEAN` | Whether cosine similarity exceeds threshold (default 0.7) |
| `VECTOR_NORM` | `(vector) → DOUBLE` | L2 norm (magnitude) of a vector |
| `NORMALIZE_VECTOR` | `(vector) → VARCHAR` | Normalizes a vector to unit length |

### Text Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `TEXT_SIMILARITY` | `(text1, text2) → DOUBLE` | Jaccard word-overlap similarity between two texts |

### Supported Vector Formats

All vector functions accept multiple input formats:

- **Comma-separated strings**: `'1.0,2.0,3.0'`
- **DuckDB array format**: `'[1.0, 2.0, 3.0]'`
- **Parentheses format**: `'(1.0, 2.0, 3.0)'`
- **Native arrays**: `float[]`, `double[]`, `int[]`
- **Collections**: `List<Float>`, `List<Double>`, `List<Number>`
- **Avro arrays**: `GenericData.Array` (via reflection)

### Query Examples

```sql
-- Find the 10 most similar documents to a query vector
SELECT doc_id, title,
       COSINE_SIMILARITY(embedding, '0.1,0.5,0.3,...') AS similarity
FROM documents
ORDER BY similarity DESC
LIMIT 10;

-- Filter to only similar documents
SELECT doc_id, title
FROM documents
WHERE VECTORS_SIMILAR(embedding, '0.1,0.5,0.3,...', 0.8);

-- Compare two document embeddings
SELECT
  a.title AS doc_a,
  b.title AS doc_b,
  COSINE_SIMILARITY(a.embedding, b.embedding) AS similarity
FROM documents a
CROSS JOIN documents b
WHERE a.doc_id < b.doc_id
  AND COSINE_SIMILARITY(a.embedding, b.embedding) > 0.9;

-- Text similarity without embeddings
SELECT title, TEXT_SIMILARITY(description, 'quarterly revenue report') AS relevance
FROM reports
WHERE TEXT_SIMILARITY(description, 'quarterly revenue report') > 0.3
ORDER BY relevance DESC;
```

## Embedding Generation

### DuckDB Engine with Quackformers

When using the DuckDB execution engine, the adapter automatically loads the [quackformers](https://duckdb.org/community_extensions/extensions/quackformers.html) extension, which provides in-process embedding generation using sentence transformer models.

Use embedding functions in computed columns during ETL materialization:

```yaml
materialize:
  enabled: true
  format: iceberg
  computedColumns:
    embedding: "embed_jina(text)::FLOAT[768]"
  options:
    rowBatchSize: 30    # Process in small batches to prevent OOM
```

This generates embeddings at materialization time, so similarity queries at read time are fast vector comparisons — no model inference needed.

### Embedding Configuration

Configure embedding settings in the ETL schema definition:

```yaml
embedding:
  provider: duckdb_quackformers          # Embedding provider
  model: sentence-transformers/all-MiniLM-L6-v2  # Model name
  dimension: 256                         # Vector dimensions
  maxTextLength: 8192                    # Maximum input text length
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `provider` | String | `duckdb_quackformers` | Embedding provider |
| `model` | String | `sentence-transformers/all-MiniLM-L6-v2` | Sentence transformer model |
| `dimension` | Integer | 256 | Output vector dimensions |
| `maxTextLength` | Integer | 8192 | Maximum text length for embedding input |

## DuckDB Function Pushdown

When using the DuckDB engine, similarity functions are pushed down to DuckDB's native array functions for better performance:

| Calcite Function | DuckDB Equivalent |
|-----------------|-------------------|
| `COSINE_SIMILARITY` | `array_cosine_similarity` |
| `COSINE_DISTANCE` | `array_cosine_distance` |

This means similarity queries on DuckDB-backed tables execute entirely within DuckDB, leveraging its optimized array operations.

### HNSW Index Support

DuckDB's VSS (Vector Similarity Search) extension supports HNSW indexes for approximate nearest-neighbor queries. When the VSS extension is available, you can create indexes on vector columns for faster similarity search on large datasets:

```sql
-- Create an HNSW index on the embedding column (DuckDB-specific)
CREATE INDEX embedding_idx ON documents USING HNSW (embedding)
  WITH (metric = 'cosine');
```

## Architecture

### Component Overview

| Component | Location | Role |
|-----------|----------|------|
| `SimilarityFunctions` | `o.a.c.a.f.similarity` | SQL UDF implementations for all vector/text operations |
| `DuckDBFunctionMapping` | `o.a.c.a.f.duckdb` | Maps Calcite functions to DuckDB native equivalents |
| `EmbeddingConfig` | `o.a.c.a.f.etl.HttpSourceConfig` | Configuration for embedding providers and models |
| `IcebergMaterializer` | `o.a.c.a.f.iceberg` | Loads quackformers extension, processes computed embedding columns |

### Engine Compatibility

| Feature | LINQ4J | Arrow | Parquet | DuckDB |
|---------|--------|-------|---------|--------|
| Similarity UDFs | Yes | Yes | Yes | Yes |
| Function pushdown | No | No | No | Yes |
| Embedding generation | No | No | No | Yes |
| HNSW indexes | No | No | No | Yes (with VSS extension) |
