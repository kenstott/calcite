# Concepts

This page explains the mental model behind the file adapter — how the pieces fit together and why they're designed the way they are. Read this before the reference docs.

---

## The Core Idea

The file adapter turns data sources into a queryable relational database accessible over JDBC. The output is always the same: an Iceberg (or Hive-partitioned) data lake that looks like a database — tables, views, declared relationships, standard SQL.

The ingestion path to get there depends on the nature of your source data. There are two.

---

## Two Ingestion Paths

### Tabular Mode

**The source defines the schema.**

Point at a file, a directory, an S3 prefix, or an HTTP endpoint. The adapter reads the data, infers the schema from the source structure, and makes it immediately queryable. No schema definition required.

This covers a wide range of sources:

- **Native tabular formats** — CSV, TSV, JSON, YAML, Parquet, Arrow, Iceberg. The file structure is the table structure.
- **Embedded tabular formats** — Excel (each sheet), Word (tables), PowerPoint (slide tables), HTML (`<table>` elements), Markdown (GFM tables), XML (XPath). The adapter extracts the tables automatically.
- **HTTP APIs** — a REST endpoint becomes a table. Multi-endpoint APIs become multiple tables. Pagination, authentication, and dimension expansion are handled through config alone, no code.

**Crawlers** extend tabular mode to whole corpora: point at a directory or S3 prefix and the adapter discovers all files recursively, creating a table from each one. You don't enumerate sources — you point at a location.

The defining characteristic of tabular mode: **the schema comes from the data**. You don't define it; the adapter discovers it.

---

### Document ETL Mode

**The schema pre-exists; sources populate it.**

For complex sources where many documents or API responses must be normalized into a single canonical table. You define the target schema in advance, write a `ResponseTransformer` that maps each source response to that schema, and the adapter materializes the unified result into Iceberg.

The classic example: SEC EDGAR filings. Thousands of 10-K documents, each structured differently, all contributing rows to one `financial_facts` table with canonical column names. No single document defines the schema — the schema is an external design choice, and each document is mined to populate it.

Document ETL mode is the right choice when:
- A single target table should aggregate data from many heterogeneous sources
- The source response shape doesn't directly map to the table you want
- You need custom normalization, unit conversion, or field derivation logic

The defining characteristic of document ETL mode: **the schema is defined externally; sources contribute to it**.

---

### Both Paths, Same Lake

Both modes write to the same Iceberg store. A schema can mix tabular tables and document ETL tables. You can join across them in a single query. The ingestion path is an implementation detail; the output is one unified data lake.

---

## The Lake

### Materialization Formats

Data lands in one of two formats:

**Apache Iceberg** (recommended) — supports schema evolution (columns can be added without rewriting data), point-in-time queries (query the lake as it was at any past snapshot), and ACID semantics. The right choice for most cases.

**Hive-partitioned Parquet** — simpler format, no schema evolution or time travel. Useful when you need maximum compatibility with external tools or prefer the simplest possible storage layout.

Both formats work on S3, R2, MinIO, or local/network file mounts.

### Air-Gapped Deployment

The adapter has no mandatory cloud dependency. Using Hive-partitioned Parquet on a local or network file mount with DuckDB as the execution engine, it runs completely on-prem with no external network calls. This makes it suitable for regulated environments where data cannot leave the perimeter.

---

## The Relational Model

The lake is not just a pile of files — it has a declared relational structure.

**Primary and foreign keys** can be declared on any table. These constraints are not enforced during writes (the lake is append-oriented), but they serve real purposes:

- **Query optimization** — Calcite's planner uses PK/FK metadata for join elimination, join reordering, and cardinality estimation
- **BI tool integration** — tools like Tableau, Power BI, and Metabase read FK metadata to auto-suggest join paths
- **Schema documentation** — the relationships are explicit in the config, not inferred at query time

**Views** come in two forms:
- *Transient* — computed at query time from live source data; always current, no storage cost
- *Materialized* — pre-computed and stored in Iceberg; fast reads, refreshed explicitly

**Computed columns** are derived at ingestion time and stored in the lake. Text embeddings are the most powerful example — generated once when data lands, stored as vector columns, enabling semantic search without a separate vector pipeline.

---

## The Query Layer

All data is accessed through a standard Calcite JDBC connection. The SQL dialect is Postgres-like. Any JDBC-compatible tool — Tableau, Power BI, Metabase, DBeaver, sqlline — connects and queries without special configuration.

### Execution Engines

The execution engine handles the physical query execution against the lake. It is a deployment choice, not an architectural one. The schema, SQL, and JDBC connection are identical regardless of which engine is configured.

| Engine | Profile |
|--------|---------|
| **DuckDB** | Embedded, zero infrastructure. The right default for most teams. Handles up to ~100GB well on a single machine. |
| **Trino** | Distributed. Production-grade for large datasets and concurrent users. Requires a Trino cluster. |
| **ClickHouse** | Extremely fast aggregations on billions of rows. High query concurrency. |
| **Spark** | Batch-oriented. For teams with existing Spark infrastructure. |

**The typical growth path**: start with DuckDB (no infrastructure, free, works immediately). As data volume and query concurrency grow, switch to Trino or ClickHouse. Nothing else changes — same model, same SQL, same JDBC driver.

Different schemas within the same model can use different engines. A hot dataset can use Arrow (in-memory) while a large historical archive uses Parquet spillover.

---

## Summary

| | Tabular Mode | Document ETL Mode |
|---|---|---|
| Schema source | Discovered from data | Defined externally |
| Discovery | Crawlers, auto-detection | Explicit sources |
| Transformation | Built-in handlers | Custom `ResponseTransformer` |
| Best for | Files, APIs, office docs | Complex document corpora, multi-source canonical tables |
| Output | Iceberg or Hive Parquet | Iceberg or Hive Parquet |

The lake has: Iceberg (schema evolution, time travel), PK/FK relational model, transient and materialized views, computed columns including embeddings, semantic search.

The query layer has: Calcite JDBC, Postgres-like SQL, swappable execution engine (DuckDB → Trino → Spark → ClickHouse).
