# Aperio: Product Positioning Document

## Executive Summary

Aperio is a zero-infrastructure SQL layer for file-based data. It transforms shared file storage into a collaborative query platform with automatic performance optimization—no servers, no data movement, no cost.

**One-liner:** *Mount a drive, get a database.*

---

## The Problem

Teams have data scattered across files—CSVs from marketing, Excel from finance, Parquet exports from ops—landing in shared storage (S3, network drives, SharePoint). Getting SQL access to this data today requires:

| Approach | Pain |
|----------|------|
| **Data warehouse (Snowflake, BigQuery)** | $1K+/month, requires data ingestion pipelines, weeks to set up |
| **Self-hosted query engine (Trino, Spark)** | Cluster management, DevOps overhead, operational burden |
| **Python/notebooks** | Not everyone codes; notebooks don't share well |
| **Raw DuckDB** | No schema management, no sharing, "just files" |
| **Email files around** | Chaos |

**The gap:** There's no simple way to give a team SQL access to files without infrastructure or engineering effort.

---

## The Solution

Aperio provides SQL access to files with:

- **Zero infrastructure** — No servers to run or maintain
- **Zero data movement** — Query files where they land
- **Zero cost** — Open source, no per-seat licensing
- **Automatic optimization** — Files converted to Parquet, cached, shared across users

### Setup

```
Shared Mount (S3, NFS, SMB)
├── data/           ← drop files here
├── parquet-cache/  ← auto-populated
└── model.json      ← config file
```

**model.json (single schema):**
```json
{
  "schemas": [{
    "name": "DATA",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/mnt/shared/data",
      "parquetCacheDirectory": "/mnt/shared/parquet-cache"
    }
  }]
}
```

**That's it.** Connect any JDBC client and query.

### Multi-Schema Federation

One model file can define multiple schemas, each pointing to a different data source:

```json
{
  "schemas": [
    { "name": "SALES", "factory": "...FileSchemaFactory",
      "operand": { "directory": "/mnt/sales" } },
    { "name": "MARKETING", "factory": "...FileSchemaFactory",
      "operand": { "directory": "s3://marketing-bucket" } },
    { "name": "EXTERNAL", "factory": "...FileSchemaFactory",
      "operand": { "directory": "https://data.example.com/feeds" } }
  ]
}
```

**Cross-schema queries just work:**
```sql
SELECT s.order_id, s.amount, m.campaign_name
FROM SALES.orders s
JOIN MARKETING.campaigns m ON s.campaign_id = m.id
WHERE s.region IN (SELECT region FROM EXTERNAL.territories WHERE active = true)
```

**Key concept:** One schema = one data source. One model = many schemas. Joins happen seamlessly across sources.

### Pluggable Data Source Providers

The adapter uses a pluggable data source engine supporting multiple protocols:

| Protocol | Example | Use Case |
|----------|---------|----------|
| **File** | `/mnt/data`, `/home/user/files` | Local or network-mounted filesystems |
| **S3** | `s3://bucket/prefix` | AWS, MinIO, R2, any S3-compatible |
| **HDFS** | `hdfs://cluster/path` | Hadoop environments |
| **HTTP/HTTPS** | `https://data.gov/files` | Public datasets, APIs |
| **SharePoint** | `sharepoint://site/library` | Microsoft 365 document libraries |
| **FTP** | `ftp://server/path` | Legacy file servers |
| **SFTP** | `sftp://server/path` | Secure file transfer |

Each schema can use a different data source provider. The query layer doesn't care where the data lives—it's all just tables.

**Automatic discovery:**
- **Folder crawling:** If a data source supports folders, the adapter crawls them recursively to discover files. Point at a directory and every file becomes a table—no manual registration required.
- **Link crawling:** If a file format supports hyperlinks (HTML index pages, etc.), the adapter follows links to discover additional files. Point at a web page listing data files and they all become queryable.

---

## How It Works

### Auto-Discovery
Point at a directory. Files become tables automatically.

```
/data/
├── sales_2024.csv      → SELECT * FROM sales_2024
├── inventory.xlsx      → SELECT * FROM inventory
└── metrics/*.parquet   → SELECT * FROM metrics
```

### Shared Parquet Cache
First user queries a CSV → Aperio converts to Parquet → cached on shared storage → all subsequent users get instant performance.

```
User A: SELECT * FROM sales.csv     [3.2s - conversion]
User B: SELECT * FROM sales.csv     [0.1s - cache hit]
User C: SELECT * FROM sales.csv     [0.1s - cache hit]
```

The team's performance improves as more queries run. Conversion cost is paid once, benefit is shared.

### Multi-User Concurrency
- File-level locking prevents race conditions
- Distributed locks (Redis) for cloud deployments
- Change detection triggers re-conversion when source files update

### Progressive Optimization
Start simple, add optimization as needed:

| Stage | Configuration | Performance |
|-------|---------------|-------------|
| **Basic** | Point at files | Works |
| **Cached** | Add parquetCacheDirectory | 10-100x faster |
| **Partitioned** | Define partition columns | Scalable to TB+ |
| **Distributed** | Swap to Spark/Trino backend | Unlimited scale |

Same data, same queries—just configuration changes.

---

## Target Market

### Primary: Small Data Teams (2-15 people)

**Profile:**
- Data landing in shared storage from various sources
- SQL skills but no data engineering capacity
- Budget-conscious (Snowflake too expensive)
- Need to query and share insights, not build pipelines

**Examples:**
- Startup analytics teams
- Department-level data groups in enterprises
- Consulting/agency teams analyzing client data
- Research teams with instrument outputs

### Secondary: Embedded Analytics

**Profile:**
- JVM applications needing built-in analytics
- ISVs adding query capability to products
- Edge deployments without cloud connectivity

---

## Competitive Positioning

### vs. Cloud Data Warehouses (Snowflake, BigQuery, Redshift)

| Aspect | Cloud Warehouse | Aperio |
|--------|-----------------|--------|
| Setup time | Days/weeks | Minutes |
| Monthly cost | $1,000+ | $0 |
| Data movement | Required (ingest) | None (query in place) |
| Infrastructure | Managed service | None |
| Vendor lock-in | High | None |

**When to choose Aperio:** Data under 100GB, budget constraints, need for speed, no existing warehouse.

**When to choose warehouse:** Need ACID transactions, complex governance, massive scale from day one.

### vs. DuckDB (direct usage)

| Aspect | DuckDB CLI | Aperio |
|--------|------------|--------|
| Schema management | Manual | Automatic |
| Sharing across users | Not built-in | Shared config + cache |
| Format conversion | Manual | Automatic |
| BI tool integration | Limited | Full JDBC |

**When to choose Aperio:** Team needs shared access, non-technical users, need BI tool connectivity.

**When to choose DuckDB direct:** Single-user exploration, Python/R workflows, maximum control.

### vs. Trino/Presto

| Aspect | Trino | Aperio |
|--------|-------|--------|
| Deployment | Coordinator + workers | None |
| Operations | Cluster management | None |
| Scale | Distributed | Single-node (or Trino backend) |

**When to choose Aperio:** No ops capacity, smaller data, want simplicity.

**When to choose Trino:** Multi-TB data, existing cluster, need distributed execution.

### vs. dbt

| Aspect | dbt | Aperio |
|--------|-----|--------|
| Requires warehouse | Yes | No |
| Transformation model | Version-controlled SQL | Query-time |
| Data testing | Built-in | Not built-in |

**Position:** Complementary. Aperio provides the execution layer; dbt could target Aperio as a backend.

---

## Unique Differentiation

### 1. Zero Infrastructure, Not "Low" Infrastructure
No containers. No Kubernetes. No managed service. Just files.

### 2. Shared Performance Cache
Team benefits compound. The more queries run, the faster everything gets. This doesn't exist elsewhere.

### 3. Scale-Up Without Rewrite
Start with embedded DuckDB. When data grows, change one config line to use Spark or Trino. No migration, no rewrite, no new queries.

### 4. Query Files in Place
No ETL to load data. No storage duplication. No sync jobs. Files are the source of truth.

---

## Supported Formats & Storage

### File Formats
| Format | Read | Write |
|--------|------|-------|
| CSV | Yes | Yes |
| JSON | Yes | Yes |
| Parquet | Yes | Yes |
| Excel (.xlsx) | Yes | - |
| XML | Yes | - |
| HTML tables | Yes | - |
| Markdown tables | Yes | - |
| Word (.docx) | Yes | - |
| PowerPoint (.pptx) | Yes | - |

### Data Sources vs. Cache Storage

Aperio separates where data comes **from** and where cache is **written**:

**Data Source Providers (read):**
| Source | Supported |
|--------|-----------|
| Local filesystem | Yes |
| Amazon S3 / S3-compatible | Yes |
| HDFS | Yes |
| HTTP/HTTPS | Yes |
| SharePoint | Yes |
| FTP/SFTP | Yes |

**Cache Storage (write):**
| Storage | Supported |
|---------|-----------|
| Local filesystem | Yes |
| Amazon S3 / S3-compatible | Yes |
| HDFS | Yes |

This means you can read from HTTP or SharePoint, but cache writes go to local, S3, or HDFS.

**Default behavior:** Parquet files are read in place. Other formats (CSV, Excel, JSON, etc.) are converted to Parquet and cached for performance.

**Alternative:** Schemas can be configured to read CSV/JSON directly into memory on startup—no file movement, but limited to smaller datasets.

### Progressive Optimization (Config, Not Code)

Start simple. Add performance features as needed—all through model configuration:

| Feature | What It Does | When to Use |
|---------|--------------|-------------|
| **Parquet cache** | Auto-convert CSV/Excel to columnar | Default, automatic |
| **Materialized views** | Pre-compute query results | Repeated expensive queries |
| **Partitioned tables** | Reorganize data by columns (year, region) | Large datasets, common filters |
| **Lattices** | Pre-aggregate combinations | Dashboard-style rollups |
| **HLL sketches** | Sub-millisecond COUNT(DISTINCT) | Cardinality estimates |
| **Parquet statistics** | Min/max/null counts from file metadata | Query planning, partition pruning |
| **Declarative API sourcing** *(roadmap)* | Fetch from REST APIs, materialize to Parquet | External data integration |

```
Complexity spectrum:

[Point at files] → [Auto Parquet cache] → [Materialized views] → [Partitions] → [Lattices]
     Simple                                                                      Advanced
     Zero config                                                            More config
```

The same model file grows with your needs. No rewrites, no migrations—just add config.

### Roadmap: Declarative API Sourcing

Turn REST APIs into queryable tables with YAML configuration—no code required.

```yaml
tables:
  - name: sales_by_region

    # Fetch from API
    source:
      type: http
      url: "https://api.example.com/v1/sales"
      parameters:
        year: "{year}"
        region: "{region}"
      auth:
        type: apiKey
        value: "{env:SALES_API_KEY}"
      response:
        dataPath: "$.data.records"
      rateLimit:
        requestsPerSecond: 10
      cache:
        enabled: true
        ttlSeconds: 86400

    # Iterate over dimensions
    dimensions:
      year:
        type: range
        start: 2020
        end: 2024
      region:
        type: list
        values: [NORTH, SOUTH, EAST, WEST]

    # Materialize to partitioned Parquet
    materialize:
      partition:
        columns: [year, region]
      output:
        location: "s3://bucket/sales/"
```

**What this does:**
1. Expands dimensions → 20 combinations (5 years × 4 regions)
2. Fetches each combination from the API (with rate limiting, caching)
3. Materializes to hive-partitioned Parquet
4. Registers as a queryable table

**Result:** `SELECT * FROM sales_by_region WHERE year = 2024` — queries local Parquet, not the API.

**Key capabilities:**
- Variable substitution in URLs, parameters, headers
- Pagination support (offset, cursor, page-based)
- Rate limiting with exponential backoff
- Response caching
- Dimension types: range, list, SQL query
- Incremental updates (only fetch new data)
- Extensibility hooks for custom transformations

**Computed columns and transforms:**

```yaml
columns:
  - {name: region_code, type: VARCHAR, source: regionCode}
  - {name: revenue, type: DECIMAL(15,2), source: totalRevenue}
  # Computed from other columns
  - {name: quarter, type: VARCHAR, expression: "SUBSTR(period, 1, 2)"}
  - {name: revenue_millions, type: DECIMAL(10,2), expression: "revenue / 1000000"}
  # Complex transforms
  - {name: value_clean, type: DECIMAL, expression: "CASE WHEN raw_value IN ('(NA)', '(D)') THEN NULL ELSE CAST(raw_value AS DECIMAL) END"}
```

SQL expressions execute during materialization—no post-processing scripts needed.

---

## Technical Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Aperio                                   │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Schema    │  │   Format    │  │    Execution Engine     │  │
│  │  Discovery  │──│  Converter  │──│  (DuckDB/Spark/Trino)   │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│         │               │                      │                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │            Data Source Provider Abstraction                  ││
│  │         (Local / S3 / HDFS / SharePoint / FTP)              ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │   Shared Mount    │
                    │  (files + cache)  │
                    └───────────────────┘
```

**Key components:**
- **Apache Calcite** — SQL parsing, planning, optimization
- **DuckDB** — Default execution engine (embedded, vectorized)
- **Storage Providers** — Unified interface across storage systems
- **Parquet Cache** — Automatic conversion with cross-user sharing

---

## Go-to-Market Considerations

### Positioning Statement

> **For small data teams** who need SQL access to files scattered across shared storage, **Aperio** is a query layer that provides instant, collaborative analytics **without infrastructure, data movement, or cost**. Unlike cloud warehouses that require ingestion pipelines and monthly fees, Aperio queries files where they land with automatic performance optimization.

### Key Messages

1. **"Mount a drive, get a database."**
   — Emphasizes simplicity of setup

2. **"Team performance that improves itself."**
   — Highlights shared cache benefit

3. **"Start local, scale to distributed."**
   — Addresses growth concerns

4. **"Query files, not copies."**
   — Differentiates from warehouse model

### Adoption Path

```
Discovery     → "I just need to query these files"
Trial         → 5-minute setup, immediate value
Team Adoption → Shared config, everyone benefits
Growth        → Add partitioning, then distributed backend
```

### Potential Objections

| Objection | Response |
|-----------|----------|
| "Will it scale?" | Start embedded, swap to Spark/Trino backend with config change |
| "Is it production-ready?" | Built on Apache Calcite (used by Hive, Flink, Druid) |
| "No UI?" | Standard JDBC works with any BI tool (Tableau, DBeaver, etc.) |
| "What about governance?" | Filesystem permissions apply; add catalog integration as needed |

---

## Success Metrics

### User Value
- Time to first query: < 5 minutes
- Query performance vs raw files: 10-100x improvement (with cache)
- Setup complexity: 5-line config file

### Adoption Indicators
- Teams sharing single config across 3+ users
- Cache hit rates > 80%
- Progressive feature adoption (basic → cached → partitioned)

---

## Roadmap Considerations

### Current Strengths
- Zero-config file discovery
- Multi-format support
- Shared Parquet cache
- JDBC standard interface
- Storage provider abstraction

### Potential Enhancements
| Area | Enhancement | Value |
|------|-------------|-------|
| Onboarding | `aperio init` CLI wizard | Lower barrier to entry |
| Discoverability | Auto-generated catalog/docs | Help users find tables |
| Collaboration | Query sharing/saving | Team workflows |
| Monitoring | Cache statistics dashboard | Operational visibility |
| Governance | Column-level access control | Enterprise requirements |

---

## Business Model

### Open Core + Data Services

```
┌─────────────────────────────────────────────────────────────────┐
│                         FREE (Open Source)                       │
├─────────────────────────────────────────────────────────────────┤
│  • JDBC Adapter                                                  │
│  • All file formats (CSV, Parquet, Excel, JSON, etc.)           │
│  • All storage providers (S3, HDFS, SharePoint, etc.)           │
│  • Schema discovery & Parquet cache                              │
│  • DuckDB execution engine                                       │
│  • Partitioning & materialization                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         PAID SERVICES                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────┐  ┌─────────────────────────────┐   │
│  │     MCP Tooling         │  │    Curated Datasets         │   │
│  │     (Metered)           │  │    (Subscription)           │   │
│  ├─────────────────────────┤  ├─────────────────────────────┤   │
│  │ • AI/LLM integration    │  │ • GovData (BLS, BEA, etc.)  │   │
│  │ • Natural language      │  │ • Financial datasets        │   │
│  │   queries               │  │ • Industry benchmarks       │   │
│  │ • Schema understanding  │  │ • Pre-cleaned & normalized  │   │
│  │ • Query generation      │  │ • Regular updates           │   │
│  │                         │  │ • Ready-to-query schemas    │   │
│  └─────────────────────────┘  └─────────────────────────────┘   │
│                                                                  │
│  Pricing: Usage-based         Pricing: Subscription tiers       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Free Tier Value

The free JDBC adapter is fully functional—not a trial or limited version:
- Query any files you have
- Full performance (DuckDB engine, Parquet cache)
- Unlimited users (just share the config)
- No time limits, no feature gates

**Why free works:** The adapter creates value by making data accessible. That accessibility creates demand for:
1. **AI tooling** to make querying even easier
2. **Curated data** to enrich what users already have

### Paid: MCP Tooling (Metered)

Model Context Protocol integration for AI/LLM workflows:

| Feature | Value |
|---------|-------|
| Natural language → SQL | "Show me sales by region last quarter" |
| Schema explanation | AI understands your tables automatically |
| Query optimization hints | AI suggests indexes, partitions |
| Anomaly detection | "Flag unusual patterns in this data" |

**Metering model:** Per-query or per-token, similar to API pricing.

**Target users:** Teams using Claude, GPT, or other LLMs for data analysis.

### Paid: Curated Datasets (Subscription)

Pre-built, query-ready datasets:

| Dataset | Content | Value |
|---------|---------|-------|
| **GovData** | BLS employment, BEA economic indicators, Census data | No scraping, no cleaning, always current |
| **Financial** | SEC filings, market data, company fundamentals | Normalized, linked entities |
| **Industry** | Benchmarks, market sizing, trends | Hard-to-find reference data |

**Subscription model:** Tiered by dataset access and update frequency.

**Target users:** Analysts who need reference data alongside their own data.

### Revenue Logic

```
Free JDBC Adapter
     │
     ├── Builds user base & habit
     │
     ├── Users want easier queries ──► MCP Tooling (metered)
     │
     └── Users want more data ──────► Curated Datasets (subscription)
```

### Competitive Advantage of This Model

| Competitor Model | Aperio Model |
|------------------|--------------|
| Snowflake: Pay for compute + storage | Free compute, pay for AI + data |
| Databricks: Platform lock-in | Open core, no lock-in |
| Data vendors: Sell raw data | Sell query-ready data + schema |
| AI wrappers: Generic tools | Deep integration with your schema |

---

## Summary

Aperio occupies a unique position: **the simplest path from "files in storage" to "queryable data platform."**

It's not trying to replace Snowflake for enterprises with petabytes and governance requirements. It's serving the vast market of teams who just want to query their data without becoming data engineers or spending thousands per month.

**The value proposition is simplicity with a growth path:**
- Start: Mount + config + JDBC
- Grow: Partitioning, materialization
- Scale: Distributed backends
- Enhance: AI tooling, curated data

**Business model advantage:** Free core builds adoption; paid services (AI tooling, curated data) monetize without gatekeeping core functionality.

No other tool offers this combination of zero-infrastructure start with credible scale-up path and value-add monetization.
