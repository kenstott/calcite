# Lake Features

Both ingestion paths — tabular and document ETL — write to the same lake. This page covers what the lake provides on top of raw storage: materialization formats, the relational model, views, computed columns, and semantic search.

---

## Materialization Formats

### Apache Iceberg (Recommended)

Iceberg is the default and recommended format. It adds a metadata layer on top of Parquet files that enables:

- **Schema evolution** — add, rename, or reorder columns without rewriting existing data files. New columns appear as null in historical records; removed columns are hidden from queries.
- **Point-in-time queries** — query the lake as it existed at any past snapshot. Useful for auditing, reproducible analysis, and debugging ETL issues.
- **ACID semantics** — each write is a transaction. Concurrent readers always see a consistent snapshot.
- **Partition evolution** — change how data is partitioned without rewriting existing files.

```json
{
  "operand": {
    "format": "iceberg",
    "directory": "s3://my-lake/warehouse/"
  }
}
```

For time travel and other Iceberg-specific features, see [Apache Iceberg Integration](iceberg-integration.md).

### Hive-Partitioned Parquet

Simpler format: Parquet files organized in Hive-style directory partitions (`year=2024/month=01/data.parquet`). No metadata catalog, no time travel, no schema evolution.

```json
{
  "operand": {
    "format": "parquet",
    "directory": "/mnt/data/warehouse/"
  }
}
```

Use Hive partitioned when:
- Maximum compatibility with external tools is required
- Simplicity is preferred over features
- Air-gapped deployment on local storage without S3

### Storage Backends

Both formats work on any supported storage backend:

| Backend | Notes |
|---------|-------|
| Amazon S3 | Standard AWS S3 |
| Cloudflare R2 | S3-compatible, no egress fees |
| MinIO | Self-hosted S3-compatible |
| Local filesystem | Any mounted path |
| Network file mount | NAS, NFS, SMB — any OS-mounted path |

---

## Relational Model

The lake has a declared relational structure, not just a collection of files.

### Primary and Foreign Keys

```json
{
  "tables": [
    {
      "name": "orders",
      "primaryKey": ["order_id"],
      "foreignKeys": [{
        "columns": ["customer_id"],
        "targetTable": "customers",
        "targetColumns": ["id"]
      }]
    },
    {
      "name": "customers",
      "primaryKey": ["id"]
    },
    {
      "name": "order_items",
      "primaryKey": ["item_id"],
      "foreignKeys": [
        {
          "columns": ["order_id"],
          "targetTable": "orders",
          "targetColumns": ["order_id"]
        },
        {
          "columns": ["product_id"],
          "targetTable": "products",
          "targetColumns": ["id"]
        }
      ]
    }
  ]
}
```

Constraints are metadata-only — they are not enforced during writes. They serve three purposes:

**1. Query optimization**: Calcite's planner uses PK uniqueness for join elimination (removes unnecessary self-joins), FK relationships for join reordering (drives from the smaller side), and cardinality estimation.

**2. BI tool integration**: Tools that read JDBC metadata (`DatabaseMetaData.getImportedKeys`, `getPrimaryKeys`) use FK relationships to auto-suggest join paths. Tableau, Power BI, and Metabase all benefit from declared FKs.

**3. Documentation**: The relational model is explicit in config and visible via JDBC metadata APIs — consumers understand table relationships without reading code.

### Unique Constraints

```json
{
  "name": "users",
  "primaryKey": ["user_id"],
  "uniqueKeys": [["email"], ["username"]]
}
```

Unique constraints give the optimizer additional uniqueness guarantees beyond the primary key.

---

## Views

### Transient Views

Computed at query time from live source data. Always current, no storage cost, re-executed on every query.

```json
{
  "views": [{
    "name": "recent_orders",
    "sql": "SELECT * FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY"
  }]
}
```

```sql
SELECT * FROM myschema.recent_orders;
-- Equivalent to: SELECT * FROM orders WHERE order_date >= CURRENT_DATE - 30 days
```

Transient views can reference tables from other schemas:

```json
{
  "views": [{
    "name": "enriched_orders",
    "sql": "SELECT o.*, c.name as customer_name, c.tier FROM orders o JOIN customers.profiles c ON o.customer_id = c.id"
  }]
}
```

### Materialized Views

Pre-computed and stored in Iceberg. The optimizer rewrites queries to use the materialized view when beneficial — callers don't need to know it exists.

```json
{
  "executionEngine": "DUCKDB",
  "materializations": [{
    "view":  "monthly_revenue",
    "table": "monthly_revenue_cache",
    "sql":   "SELECT DATE_TRUNC('month', order_date) as month, SUM(amount) as revenue FROM orders GROUP BY 1"
  }]
}
```

The materialized view is computed once on first access and stored as `monthly_revenue_cache.parquet`. Subsequent queries hit the cache.

```sql
-- This query may automatically rewrite to use the materialized view:
SELECT month, SUM(revenue) FROM orders WHERE year = 2024 GROUP BY month;
```

**Refreshing**: delete the `.materialized_views/<table>.parquet` file to force recomputation on next query.

**Cross-schema materialized views**: the schema containing the materialization must use DuckDB or Parquet engine, but the SQL can reference any schema:

```json
{
  "name": "analytics",
  "executionEngine": "DUCKDB",
  "materializations": [{
    "view":  "customer_lifetime_value",
    "table": "clv_cache",
    "sql":   "SELECT c.id, c.name, SUM(o.amount) as ltv FROM crm.customers c JOIN transactions.orders o ON c.id = o.customer_id GROUP BY c.id, c.name"
  }]
}
```

---

## Computed Columns

Columns can be derived at ingestion time and stored in the lake. They are computed once when data lands — not recomputed at query time.

### Text Embeddings

The most powerful computed column type. Generate a vector embedding for any text column and store it alongside the source data:

```json
{
  "name": "documents",
  "computedColumns": [{
    "name":   "embedding",
    "type":   "ARRAY(FLOAT)",
    "source": "content",
    "model":  "sentence-transformers/all-MiniLM-L6-v2"
  }]
}
```

When data is ingested, the adapter calls the embedding model on the `content` column and stores the resulting vector as `embedding`. Requires DuckDB engine with the `quackformers` extension.

### Derived Columns

Any SQL expression can be a computed column:

```json
{
  "computedColumns": [
    {
      "name":       "amount_usd",
      "type":       "DOUBLE",
      "expression": "amount / exchange_rate"
    },
    {
      "name":       "year",
      "type":       "INTEGER",
      "expression": "YEAR(transaction_date)"
    },
    {
      "name":       "content_hash",
      "type":       "VARCHAR",
      "expression": "MD5(content)"
    }
  ]
}
```

---

## Semantic Search

With embeddings stored in the lake, semantic similarity search is a standard SQL query.

### Similarity Functions

```sql
-- Find documents semantically similar to a query string
SELECT id, title, COSINE_SIMILARITY(embedding, :query_embedding) AS score
FROM documents
ORDER BY score DESC
LIMIT 10;

-- Filter by similarity threshold
SELECT id, title
FROM documents
WHERE VECTORS_SIMILAR(embedding, :query_embedding, 0.75);

-- Available functions:
-- COSINE_SIMILARITY(a, b)        → 0..1, higher is more similar
-- COSINE_DISTANCE(a, b)          → 0..2, lower is more similar
-- EUCLIDEAN_DISTANCE(a, b)       → lower is more similar
-- DOT_PRODUCT(a, b)              → for normalized vectors
-- VECTORS_SIMILAR(a, b, thresh)  → boolean, cosine similarity >= thresh
-- TEXT_SIMILARITY(s1, s2)        → word-overlap similarity for plain text
```

### HNSW Indexes (DuckDB + VSS)

For large document sets, approximate nearest-neighbor search via HNSW index is dramatically faster than full table scans:

```json
{
  "duckdbConfig": {
    "extensions": ["vss"]
  }
}
```

With the VSS extension loaded, `COSINE_SIMILARITY` and `COSINE_DISTANCE` predicates are automatically pushed down to HNSW indexes when available.

### Embedding Generation at Query Time

For ad-hoc queries where you have a text string rather than a pre-computed vector:

```sql
-- Generate embedding for query text inline (DuckDB + quackformers)
SELECT id, title, COSINE_SIMILARITY(embedding, EMBED('quarterly earnings growth')) AS score
FROM financial_documents
ORDER BY score DESC
LIMIT 5;
```

See [Vector Search](vector-search.md) for full function reference and engine compatibility.

---

## Iceberg Compaction

After many incremental writes, Iceberg tables accumulate many small files. Compaction merges them into larger files for faster query performance.

```bash
java -cp file/build/libs/calcite-file-adapter-*-all.jar org.apache.calcite.adapter.file.iceberg.CompactionRunner \
  --warehouse s3://my-lake/warehouse \
  --table financial_facts \
  --target-file-size 134217728
```

See [Apache Iceberg Integration](iceberg-integration.md#table-compaction) for full options.
