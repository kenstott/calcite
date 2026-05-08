# Apache Calcite File Adapter

> **Point to files. Query in minutes. Scale when you're ready.**

A zero-infrastructure data lake that runs on a desktop and grows to a distributed cluster — same SQL, same JDBC connection, same config — no DBOps required.

Built on [Apache Calcite](https://calcite.apache.org) — the proven SQL foundation underneath Apache Drill, Dremio, Apache Flink, Apache Hive, and others.

**License**: [Business Source License 1.1](LICENSE-BSL.txt). This adapter is an original work in the fork — not derived from Apache Calcite source — and is licensed under BSL 1.1. Free for teams with annual revenue under $1M; commercial license required above that threshold. The Calcite core (SQL planner, optimizer, algebraic framework) that this adapter builds on remains under the [Apache License 2.0](../LICENSE).

---

## What It Does

Point it at a directory, an S3 bucket, an HTTP API, or a set of documents. It discovers the data, infers schemas, and makes everything queryable as SQL tables through a standard JDBC connection. Results materialize into an Iceberg data lake on S3 (or local storage) — with a declared relational model, materialized views, computed columns, and semantic search built in.

**Small teams without DBOps** — DuckDB runs embedded in the JVM. Nothing to install, no cloud account, no services to start. Point at files you already have and query in minutes on a desktop.

**Teams that grow** — Swap the execution engine (DuckDB → Trino, ClickHouse, or Spark) in a single config line. Schema, SQL, and JDBC connection are identical across engines. No migration, no rewrite.

**Air-gapped or regulated environments** — No cloud dependency required. Runs fully on-prem with local or network-mounted storage. Data never leaves your perimeter.

---

## Two Ingestion Paths

### Tabular Mode — the source defines the schema

Point at data, get tables. Schema is discovered automatically from the source. No transformer code required.

**What you can point at:**

| Transport | Examples |
|-----------|---------|
| Local filesystem | `/data/reports/`, `/mnt/nas/feeds/` |
| S3 / R2 / MinIO | `s3://my-bucket/data/` |
| HTTP / HTTPS | REST APIs, file servers, data feeds |
| FTP / SFTP | Legacy data feeds |
| SharePoint | Document libraries |

**Formats with intrinsic tabular structure** (schema comes directly from the file):
CSV, TSV, JSON, YAML, Parquet, Apache Arrow, Apache Iceberg

**Formats with embedded tabular data** (adapter extracts tables automatically):
Excel (each sheet → table), Word (tables → tables), PowerPoint (slide tables → tables),
HTML (each `<table>` → table), Markdown (GFM tables), XML (XPath extraction)

**Crawlers**: Point at a directory or S3 prefix — the adapter discovers all files recursively and creates tables from everything it finds. No enumeration required.

**API to lake**: Point at a complex HTTP API with multiple query endpoints. Configure pagination, authentication, parameters, and dimension expansion in JSON. Each endpoint becomes a table. No code.

```json
{
  "name": "weather_stations",
  "url": "https://api.example.com/stations",
  "storageType": "http",
  "storageConfig": {
    "auth": "bearer",
    "token": "${API_TOKEN}"
  }
}
```

For multi-endpoint APIs that produce multiple tables, configure each endpoint as a table in the schema. The adapter handles pagination, retries, and Iceberg materialization automatically.

---

### Document ETL Mode — the schema pre-exists, sources populate it

For complex sources where a single canonical schema should be populated from many documents or API responses. The target schema is defined in advance; a `ResponseTransformer` normalizes each source into it.

This is the path for:
- APIs that return domain-specific response shapes requiring normalization
- Large document corpora (SEC filings, regulatory documents) feeding a unified table
- Any source where hundreds of differently-structured inputs must produce one canonical table

```java
public class MyTransformer implements ResponseTransformer {
  @Override
  public String transform(String response, RequestContext context) {
    // Parse response, return JSON array matching your target schema
  }
}
```

Both paths write to the same Iceberg lake. The ingestion complexity is variable; the output is always the same queryable store.

---

## The Lake

### Materialization Formats

| Format | Recommendation |
|--------|---------------|
| **Apache Iceberg** | Recommended. Schema evolution, time travel (point-in-time queries), ACID. |
| **Hive-partitioned Parquet** | Simpler. No schema evolution or time travel. |

Storage backends: S3, R2, MinIO, or local/network file mounts. Fully air-gapped deployments use file mounts — no cloud account, no external network calls.

### Relational Model

Declare primary keys and foreign keys on any table. Constraints are unenforced (metadata-only) but serve three purposes:

1. **Query optimization** — Calcite's planner uses uniqueness and FK relationships for join elimination and reordering
2. **BI tool integration** — tools like Tableau, Power BI, and Metabase read FK metadata to auto-suggest joins
3. **Schema documentation** — the relational model is explicit, not inferred at query time

```json
{
  "name": "orders",
  "primaryKey": ["order_id"],
  "foreignKeys": [{
    "columns": ["customer_id"],
    "targetTable": "customers",
    "targetColumns": ["id"]
  }]
}
```

### Views

**Transient views** — defined in config, computed at query time from live source data:
```json
{
  "views": [{
    "name": "recent_orders",
    "sql": "SELECT * FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '30' DAY"
  }]
}
```

**Materialized views** — pre-computed and stored in Iceberg. Query hits the lake, not the source:
```json
{
  "materializations": [{
    "view": "monthly_summary",
    "table": "monthly_summary_cache",
    "sql": "SELECT YEAR(order_date) as year, MONTH(order_date) as month, SUM(total) as revenue FROM orders GROUP BY 1, 2"
  }]
}
```

### Computed Columns and Semantic Search

Columns can be computed at ingestion time — derived metrics, normalized text, hash keys, and text embeddings. Embeddings are stored as vector columns alongside the source data.

With the DuckDB engine and the VSS extension, similarity queries use HNSW indexes for fast approximate nearest-neighbor search:

```sql
-- Find records semantically similar to a query
SELECT id, title, COSINE_SIMILARITY(embedding, query_embedding) as score
FROM documents
WHERE VECTORS_SIMILAR(embedding, query_embedding, 0.8)
ORDER BY score DESC
LIMIT 10;
```

Semantic search is a first-class feature of the lake — not a bolt-on vector pipeline.

---

## The Query Layer

All data is accessible through a standard Calcite JDBC connection. SQL dialect is Postgres-like. Works with any JDBC-compatible BI tool (Tableau, Power BI, Metabase, DBeaver, etc.).

### Execution Engines

The execution engine is a deployment decision, not an architectural one. Swap it as your needs grow — schema, SQL, and JDBC connection stay the same.

| Engine | When to use |
|--------|-------------|
| **DuckDB** (recommended) | Start here. Embedded, zero infrastructure, handles up to ~100GB well. |
| **Trino** | Production scale, distributed queries, federated sources. |
| **ClickHouse** | Sub-second aggregations on billions of rows, high query concurrency. |
| **Spark** | Existing Spark infrastructure, batch-oriented workloads. |
| **Arrow** | In-memory SIMD processing for hot datasets. |
| **Parquet** | Large datasets with automatic disk spillover. |

Configure per schema — different schemas in the same model can use different engines:

```json
{
  "executionEngine": "${CALCITE_FILE_ENGINE_TYPE:DUCKDB}"
}
```

---

## Quick Start

### Minimal Setup — Query Files in 5 Minutes

**1. Create `model.json`:**
```json
{
  "version": "1.0",
  "defaultSchema": "data",
  "schemas": [{
    "name": "data",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/path/to/your/data"
    }
  }]
}
```

**2. Connect and query:**
```bash
./sqlline "jdbc:calcite:model=model.json"
```
```sql
-- Files in /path/to/your/data/ are immediately queryable
SELECT * FROM data.sales LIMIT 10;
SELECT COUNT(*) FROM data.customers;
SELECT s.total, c.name FROM data.sales s JOIN data.customers c ON s.customer_id = c.id;
```

That's it. CSV, JSON, Parquet, Excel, and other formats are auto-detected. Schema is inferred from the files.

### With S3 and Iceberg Materialization

```json
{
  "version": "1.0",
  "defaultSchema": "lake",
  "schemas": [{
    "name": "lake",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "${GOVDATA_PARQUET_DIR}",
      "executionEngine": "DUCKDB",
      "storageType": "s3",
      "storageConfig": {
        "accessKey": "${AWS_ACCESS_KEY_ID}",
        "secretKey": "${AWS_SECRET_ACCESS_KEY}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
      }
    }
  }]
}
```

### Air-Gapped Deployment

```json
{
  "operand": {
    "directory": "/mnt/nas/data",
    "executionEngine": "DUCKDB",
    "format": "parquet"
  }
}
```

Local file mount, Hive-partitioned Parquet, DuckDB embedded. No cloud dependency.

---

## Documentation

### Start Here
- [Concepts](docs/concepts.md) — Mental model: two ingestion paths, the lake, the query layer
- [Quick Start](docs/quickstart.md) — Working data lake in an afternoon

### Ingestion
- [Tabular Mode](docs/tabular-mode.md) — Files, crawlers, HTTP APIs, API-to-lake
- [Document ETL](docs/document-etl.md) — Custom transformers, canonical schemas, complex document feeds
- [Supported Formats](docs/supported-formats.md) — All formats, extraction behavior, performance characteristics
- [Storage Providers](docs/storage-providers.md) — S3, HTTP, SharePoint, FTP, local filesystem

### Lake Features
- [Lake Features](docs/lake-features.md) — Iceberg vs Hive, PK/FK model, views, computed columns, embeddings
- [Apache Iceberg Integration](docs/iceberg-integration.md) — Time travel, schema evolution, compaction
- [Table Constraints](docs/CONSTRAINTS.md) — PK/FK declaration, optimizer impact, BI tool integration
- [Vector Search](docs/vector-search.md) — Similarity functions, embedding generation, HNSW indexes

### Deployment
- [Deployment](docs/deployment.md) — Cloud, air-gapped, and hybrid profiles

### Reference
- [Configuration Reference](docs/configuration-reference.md) — Complete configuration options
- [JDBC Engines](docs/jdbc-engines.md) — Engine selection, setup, and trade-offs
- [Performance Tuning](docs/performance-tuning.md) — Optimization strategies
- [Troubleshooting](docs/troubleshooting.md) — Common issues and solutions
- [API Reference](docs/api-reference.md) — Programmatic usage

---

## Origin

This adapter shares a module name with the original Apache Calcite file adapter and retains the same module structure and factory class names for compatibility. It is a complete rewrite — the original read CSV files; this is a ground-up rebuild that builds a full data lake platform on top of the same Calcite SQL foundation.

The Apache Calcite core (query planner, SQL parser, optimizer) is unchanged and used as a library. Calcite powers Apache Drill, Dremio, Apache Flink, Apache Hive, Apache Phoenix, and Apache Beam — the SQL layer here has been proven at scale across the industry.
