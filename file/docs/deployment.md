# Deployment

The adapter has no mandatory infrastructure dependencies. It runs embedded in any JVM process, with storage on S3, local filesystem, or a network mount. This page describes the common deployment profiles and how to configure each.

---

## Execution Engine Choice

The execution engine handles physical query execution. It is a config change — schema, SQL, and JDBC connection string are identical across engines.

| Engine | Infrastructure required | Best for |
|--------|------------------------|---------|
| **DuckDB** | None (embedded) | Recommended for analytics. Single-node, up to ~100GB, zero setup. |
| **Trino** | Trino cluster | Production scale, distributed queries, high concurrency. |
| **ClickHouse** | ClickHouse server or embedded chDB | Sub-second aggregations, billions of rows. |
| **Spark** | Spark Thrift Server | Existing Spark infrastructure. |
| **Arrow** | None (embedded) | Hot in-memory datasets, SIMD processing. |
| **Parquet** | None (embedded) | Default (no config). Very large datasets with automatic disk spillover. |

Configure per schema:

```json
{
  "operand": {
    "executionEngine": "${CALCITE_FILE_ENGINE_TYPE:DUCKDB}"
  }
}
```

The typical growth path: **DuckDB → Trino**. The system default (when nothing is configured) is `PARQUET` — a safe embedded fallback. Set `executionEngine` to `DUCKDB` to enable DuckDB's analytical engine, then switch to Trino when you need distributed execution or high query concurrency. Nothing else in the stack changes.

---

## Profile 1: Local / Development

Zero infrastructure. Query files from a local directory with DuckDB embedded.

```json
{
  "version": "1.0",
  "defaultSchema": "data",
  "schemas": [{
    "name": "data",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/path/to/data",
      "executionEngine": "DUCKDB"
    }
  }]
}
```

No credentials, no cloud accounts, no services to start. Works on a laptop.

---

## Profile 2: Cloud — S3 + Iceberg + DuckDB

The recommended production starting point for teams without existing data infrastructure. Data materializes to Iceberg on S3/R2. DuckDB handles queries locally.

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
        "accessKey":  "${AWS_ACCESS_KEY_ID}",
        "secretKey":  "${AWS_SECRET_ACCESS_KEY}",
        "region":     "${AWS_REGION:auto}",
        "endpoint":   "${AWS_ENDPOINT_OVERRIDE}"
      }
    }
  }]
}
```

**Cloudflare R2** is cost-effective for this profile — S3-compatible, no egress fees, set `endpoint` to your R2 endpoint and `region` to `auto`.

**Cost profile**: storage at S3/R2 rates (~$0.015/GB/month on R2), no compute costs except when queries run. A small team's lake typically costs a few dollars per month in storage.

---

## Profile 3: Cloud — S3 + Iceberg + Trino

For larger datasets or higher query concurrency. Trino executes queries distributed across a cluster; the lake stays on S3.

**Step 1**: Export the schema as Trino catalog files (run once, regenerated when schema changes):

```bash
./gradlew :file:exportTrinoCatalog \
  -Dmodel=/path/to/model.json \
  -DoutputDir=/etc/trino/catalog
```

**Step 2**: Configure the schema to use Trino:

```json
{
  "operand": {
    "executionEngine": "TRINO",
    "trinoConfig": {
      "host":    "trino-coordinator",
      "port":    "8080",
      "catalog": "iceberg",
      "user":    "${TRINO_USER}"
    }
  }
}
```

Queries are pushed to the Trino cluster. The Calcite JDBC layer handles query planning and result delivery.

**Docker Compose for local Trino:**

```yaml
services:
  trino:
    image: trinodb/trino:439
    ports:
      - "8080:8080"
    volumes:
      - ./catalog:/etc/trino/catalog
```

See [JDBC Engines](jdbc-engines.md) for full Trino configuration.

---

## Profile 4: Air-Gapped / On-Prem

No cloud dependency. Runs completely on-prem with file mounts and DuckDB embedded.

```json
{
  "version": "1.0",
  "defaultSchema": "lake",
  "schemas": [{
    "name": "lake",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/mnt/nas/lake",
      "executionEngine": "DUCKDB",
      "format": "parquet"
    }
  }]
}
```

Data materializes to `/mnt/nas/lake` in Hive-partitioned Parquet. No network calls beyond what your data sources require. Works on a server, a NAS, or any machine with a mounted filesystem.

**For Iceberg without S3**: Iceberg can be used on local/NFS storage with a Hadoop catalog:

```json
{
  "operand": {
    "format": "iceberg",
    "directory": "/mnt/nas/warehouse",
    "icebergCatalog": {
      "type": "hadoop",
      "warehouse": "/mnt/nas/warehouse"
    }
  }
}
```

This gives you schema evolution and time travel on-prem.

**Suitable for**: regulated industries (financial services, healthcare, government) where data must remain within a controlled perimeter.

---

## Profile 5: Hybrid

Different schemas using different storage backends and engines in the same model. A common pattern: live operational data on a fast local disk, historical archive on S3.

```json
{
  "version": "1.0",
  "schemas": [
    {
      "name": "live",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "/fast-ssd/live",
        "executionEngine": "ARROW"
      }
    },
    {
      "name": "archive",
      "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
      "operand": {
        "directory": "s3://my-lake/archive/",
        "executionEngine": "DUCKDB",
        "storageType": "s3",
        "storageConfig": {
          "accessKey": "${AWS_ACCESS_KEY_ID}",
          "secretKey": "${AWS_SECRET_ACCESS_KEY}"
        }
      }
    }
  ]
}
```

Join across schemas in a single query — Calcite handles the cross-schema execution:

```sql
SELECT l.event_id, l.timestamp, a.historical_baseline
FROM live.events l
JOIN archive.baselines a ON l.metric_id = a.metric_id;
```

---

## Horizontal Scale-Out

Multiple JVM instances can share the same lake by pointing at the same S3 path or mounted directory. They share the Iceberg catalog and Parquet cache. This works because:

- Iceberg provides consistent reads across concurrent writers
- The Parquet cache is content-addressed — multiple readers see the same files
- Set the same `baseDirectory` across instances to share the cache

```json
{
  "operand": {
    "directory":   "s3://shared-lake/warehouse/",
    "baseDirectory": "s3://shared-lake/cache/"
  }
}
```

For write-heavy workloads with many concurrent ETL workers, use the S3 pipeline tracker to coordinate batch completion across processes.

---

## Resource Sizing

| Profile | JVM heap | Notes |
|---------|----------|-------|
| Local/dev (DuckDB) | 4–8 GB | DuckDB manages its own memory pool |
| Production (DuckDB) | 8–16 GB | `duckdbConfig.memory_limit` controls DuckDB's share |
| Large dataset spillover (Parquet engine) | 4–8 GB | Spillover is to disk; heap holds metadata |
| Trino | JVM is a thin client | Query execution is on the Trino cluster |

DuckDB memory limit:

```json
{
  "duckdbConfig": {
    "memory_limit": "12GB",
    "threads": 8
  }
}
```

For Parquet engine spillover:

```bash
-Dcalcite.spillover.dir=/fast-disk/spill
```
