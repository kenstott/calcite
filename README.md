# Apache Calcite — Extended Fork

A permanent fork of [Apache Calcite](https://calcite.apache.org) that adds new adapters, rewrites existing ones, and builds a data lake platform on top of Calcite's proven SQL foundation.

This fork tracks upstream Calcite and merges periodically. All Calcite core modules — the SQL parser, query planner, optimizer, and algebraic framework — are unchanged. What this fork adds is entirely in the adapter layer and above.

**Last merged with upstream**: commit `b5fb7233`

## Licensing

This repository contains code under two licenses:

- **Apache License 2.0** — all code originating from Apache Calcite, including the core modules and any modifications to upstream files. See [LICENSE](LICENSE).
- **Business Source License 1.1** — all new adapters and original work added in this fork (file adapter rewrite, Splunk rewrite, SharePoint, Salesforce, GraphQL, OpenAPI adapters, and all govdata schemas). Free for teams with annual revenue under $1M; commercial license required above that threshold. See [LICENSE-BSL.txt](LICENSE-BSL.txt).

Each module's source files carry the appropriate license header.

---

## What This Fork Adds

### File Adapter — Rewritten as a Data Lake Platform

The original Calcite file adapter read CSV files. This is a complete rewrite that turns it into a full data lake platform.

> **Point to files. Get a data lake.**

Point it at a directory, S3 bucket, HTTP API, or document corpus. It discovers data, infers schemas, and makes everything queryable as SQL tables through a standard JDBC connection. Results materialize into Apache Iceberg (or Hive-partitioned Parquet) on S3 or local storage.

Key capabilities:
- **Tabular ingestion** — CSV, JSON, Parquet, Arrow, Iceberg, Excel, Word, PowerPoint, HTML, Markdown, XML, YAML; auto-discovered from directories, S3 prefixes, or glob patterns
- **API to lake** — complex multi-endpoint HTTP APIs wired into tables entirely through config; no code
- **Document ETL** — custom `ResponseTransformer` pipeline for normalizing complex document corpora into canonical Iceberg tables
- **Relational model** — declarative PK/FK constraints for query optimization and BI tool integration
- **Views** — transient (query-time) and materialized (stored in Iceberg)
- **Computed columns** — derived fields, text embeddings, generated at ingestion
- **Semantic search** — vector similarity functions and HNSW indexes via DuckDB VSS
- **Pluggable execution** — DuckDB (embedded, zero infra) → Trino → Spark → ClickHouse as scale demands; same SQL, config change only
- **Air-gapped** — runs fully on-prem with file mounts and DuckDB; no cloud dependency required

**[Full documentation →](file/README.md)**

---

### Splunk Adapter — Rewritten

Complete replacement of the original Calcite Splunk adapter:
- Full predicate pushdown with field comparisons
- HTTP timeout configuration and connection pooling
- Token expiration and retry logic
- Support for Splunk Common Information Models (CIM)
- Nullable field handling

**[Documentation →](splunk/README.md)**

---

### New Adapters

**SharePoint List Adapter** — Full read/write access to SharePoint lists as SQL tables. Supports both SharePoint Online (OAuth) and On-Premises (NTLM). Create, read, update, and delete list items through standard SQL DML.

**Salesforce Adapter** — Query Salesforce objects as SQL tables. Full SOQL pushdown, relationship traversal, and bulk API support.

**GraphQL Adapter** — Query GraphQL endpoints using SQL. Translates SQL (including window functions, CTEs, and set operations) to GraphQL queries. Built-in caching.

**OpenAPI Adapter** — Query any REST API described by an OpenAPI spec as SQL tables. Parameterized calls, response transformation, and type mapping from OpenAPI schemas.

---

### Government Data Schemas

A suite of domain schemas built on the file adapter's Document ETL pipeline. Each schema materializes a public government data source into queryable Iceberg tables:

| Module | Source |
|--------|--------|
| `sec` | SEC EDGAR — company filings, financial facts |
| `weather` | NOAA/NWS — weather stations, observations, forecasts |
| `crime` | FBI UCR / local agencies — crime statistics |
| `edu` | NCES — school districts, NAEP scores, IPEDS |
| `econ` | BLS, FRED, BEA — economic indicators, time series |
| `health` | CDC, FDA, CMS — clinical, drug, and claims data |
| `cyber` | NVD, OTX, ThreatFox — CVEs, threat intelligence |
| `geo` | Census TIGER, HUD — geographic boundaries, demographics |
| `census` | Census Bureau API — population, demographic data |
| `bls` | Bureau of Labor Statistics — employment, inflation |
| `fec` | FEC — campaign finance, contributions |
| `fedregister` | Federal Register — regulatory notices |

All schemas write to a shared Iceberg lake on S3/R2, queryable through a single JDBC connection.

---

## Repository Setup

### Clone and initialise

```bash
git clone git@github.com:kenstott/calcite.git
cd calcite
./setup.sh        # clones private Claude configuration into .claude/
```

### Keep in sync

```bash
./pull-claude.sh  # pull latest for both this repo and .claude/
```

### Save Claude configuration changes

```bash
./push-claude.sh  # commit + push any changes in .claude/
```

---

## About Apache Calcite

Apache Calcite is a dynamic data management framework used as the SQL layer in Apache Drill, Dremio, Apache Flink, Apache Hive, Apache Phoenix, Apache Beam, and many others. It provides an industry-standard SQL parser, a customizable optimizer with pluggable rules and cost functions, and a rich adapter framework for connecting SQL to external data sources.

This fork builds on that foundation without modifying it. For the upstream project, see [calcite.apache.org](https://calcite.apache.org).

---

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.calcite/calcite-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.calcite/calcite-core)
