# Quick Start

Get a working data lake in an afternoon. This guide covers three paths: querying local files immediately, materializing to S3 with Iceberg, and connecting a BI tool.

---

## Prerequisites

- Java 8+
- The adapter JAR (build with `./gradlew :file:shadowJar`, output in `file/build/libs/`)
- For S3: AWS credentials or compatible endpoint (R2, MinIO)

---

## Path 1: Query Local Files in 5 Minutes

No S3, no Iceberg, no config beyond a single JSON file. Just point at a directory and query.

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

**2. Connect:**

```bash
./sqlline "jdbc:calcite:model=model.json"
```

**3. Query:**

```sql
-- Files are auto-discovered and named from their filename (without extension)
SELECT * FROM data.sales LIMIT 10;
SELECT COUNT(*) FROM data.customers;

-- Join across formats transparently
SELECT s.amount, c.name
FROM data.sales s
JOIN data.customers c ON s.customer_id = c.id;
```

That's it. CSV, JSON, Parquet, Excel, Word, and other formats in the directory are all queryable immediately. Schema is inferred from each file.

**File → table name mapping:**
- `sales.csv` → `sales`
- `customer_orders.json` → `customer_orders`
- `report.xlsx` (sheets: Summary, Detail) → `report__summary`, `report__detail`
- `q1/revenue.csv` → `q1__revenue`

---

## Path 2: Materialize to S3 with Iceberg

For a persistent, shareable data lake. Data materializes to Iceberg on S3 and survives restarts.

**1. Set credentials:**

```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-1
# For R2/MinIO:
export AWS_ENDPOINT_OVERRIDE=https://your-endpoint
```

**2. Create `model.json`:**

```json
{
  "version": "1.0",
  "defaultSchema": "lake",
  "schemas": [{
    "name": "lake",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "s3://your-bucket/data/",
      "executionEngine": "DUCKDB",
      "storageType": "s3",
      "storageConfig": {
        "accessKey": "${AWS_ACCESS_KEY_ID}",
        "secretKey": "${AWS_SECRET_ACCESS_KEY}",
        "region": "${AWS_REGION}",
        "endpoint": "${AWS_ENDPOINT_OVERRIDE}"
      }
    }
  }]
}
```

**3. Connect and query — same as Path 1.**

Data discovered in S3 is read via DuckDB and cached as Iceberg. On subsequent queries the Iceberg cache is used directly.

---

## Path 3: Add a Relational Model

Declare primary and foreign keys to enable join optimization and BI tool auto-join suggestions.

```json
{
  "operand": {
    "directory": "/path/to/data",
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
      }
    ]
  }
}
```

No schema changes, no data changes. The constraints are metadata — they inform the query planner and appear in JDBC metadata APIs.

---

## Path 4: Connect a BI Tool

The adapter exposes a standard JDBC endpoint. Any JDBC-compatible tool works.

**Connection string:**
```
jdbc:calcite:model=/absolute/path/to/model.json
```

**Driver class:**
```
org.apache.calcite.jdbc.Driver
```

**JAR**: `file/build/libs/file-*-all.jar`

Works with Tableau (via "Other Databases (JDBC)"), DBeaver, Metabase (custom driver), and any tool that accepts a JDBC driver JAR and connection string.

Once connected, BI tools that read JDBC metadata (like Tableau) will see the declared foreign keys and offer automatic join path suggestions.

---

## Path 5: Air-Gapped / On-Prem

No S3, no cloud. File mounts and Hive-partitioned Parquet.

```json
{
  "operand": {
    "directory": "/mnt/nas/data",
    "executionEngine": "DUCKDB",
    "format": "hive"
  }
}
```

Data is materialized to `/mnt/nas/data` in Hive-partitioned Parquet format. No external network calls. Runs on a laptop, a server, or a NAS.

---

## What's Next

- **More ingestion options** — [Tabular Mode](tabular-mode.md): crawlers, HTTP APIs, API-to-lake
- **Complex document sources** — [Document ETL](document-etl.md): custom transformers, canonical schemas
- **Full lake capabilities** — [Lake Features](lake-features.md): views, computed columns, semantic search
- **Scale the execution engine** — [JDBC Engines](jdbc-engines.md): DuckDB → Trino → Spark → ClickHouse
- **Deployment profiles** — [Deployment](deployment.md): cloud, air-gapped, hybrid
- **All configuration options** — [Configuration Reference](configuration-reference.md)
