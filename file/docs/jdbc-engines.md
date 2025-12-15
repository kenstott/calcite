# JDBC Query Engines

The File Adapter supports pluggable JDBC-based query engines for executing SQL against parquet files. This enables you to leverage existing SQL infrastructure or choose the best engine for your workload.

## Supported Engines

| Engine | Deployment | Best For | Iceberg Support |
|--------|------------|----------|-----------------|
| **DuckDB** | Embedded | Single-node, <100GB datasets | Yes |
| **Trino** | Distributed cluster | Federated queries, data mesh | Via catalog |
| **Spark SQL** | Distributed cluster | Existing Spark infrastructure | Via catalog |
| **ClickHouse** | Embedded (chDB) or cluster | Fast OLAP, time-series | Yes |

## Configuration

### Environment Variable

Set the default query engine:

```bash
export CALCITE_FILE_ENGINE_TYPE=duckdb   # or trino, spark, clickhouse
```

### Model Configuration

Configure per-schema in `model.json`:

```json
{
  "schemas": [{
    "name": "analytics",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "directory": "/data/parquet",
      "queryEngine": {
        "type": "duckdb"
      }
    }
  }]
}
```

## Engine-Specific Configuration

### DuckDB (Default)

Embedded SQL engine, no external server required.

```json
{
  "queryEngine": {
    "type": "duckdb",
    "path": "/tmp/cache.duckdb",
    "threads": 4,
    "memoryLimit": "4GB"
  }
}
```

**Environment Variables:**
- `DUCKDB_MEMORY_LIMIT` - Memory limit (default: 75% of system memory)
- `DUCKDB_THREADS` - Thread count (default: CPU cores)

### Trino

Requires external Trino cluster. Tables must be registered in Trino's catalog.

```json
{
  "queryEngine": {
    "type": "trino",
    "host": "trino.example.com",
    "port": "8080",
    "catalog": "hive",
    "schema": "default",
    "user": "${TRINO_USER}"
  }
}
```

**Notes:**
- Trino does not support direct glob patterns - files must be cataloged
- Use Hive metastore or AWS Glue for table registration
- Authentication via Kerberos, LDAP, or OAuth2 supported

### Spark SQL

Requires Spark Thrift Server (HiveServer2 interface).

```json
{
  "queryEngine": {
    "type": "spark",
    "host": "spark-thrift.example.com",
    "port": "10000",
    "database": "default"
  }
}
```

**Notes:**
- Uses `parquet.\`path\`` syntax for direct file access
- Supports limited glob patterns

### ClickHouse

Supports embedded (chDB) or distributed cluster.

```json
{
  "queryEngine": {
    "type": "clickhouse",
    "host": "clickhouse.example.com",
    "port": "8123",
    "database": "default"
  }
}
```

**Notes:**
- Uses `s3()` function for S3 paths with native glob support
- Uses `iceberg()` function for Iceberg tables
- Excellent for time-series and OLAP workloads

## Parquet File Access

Each engine has different syntax for reading parquet files:

| Engine | Syntax |
|--------|--------|
| DuckDB | `read_parquet('s3://bucket/path/*.parquet')` |
| Trino | Catalog table (pre-registered) |
| Spark | `parquet.\`s3://bucket/path/*.parquet\`` |
| ClickHouse | `s3('s3://bucket/path/*.parquet', 'Parquet')` |

The File Adapter abstracts these differences - you write standard SQL.

## Iceberg Table Access

For engines with Iceberg support:

| Engine | Syntax |
|--------|--------|
| DuckDB | `iceberg_scan('s3://bucket/warehouse/table')` |
| ClickHouse | `iceberg('s3://bucket/warehouse/table')` |
| Trino | Catalog table via Iceberg connector |
| Spark | Catalog table via Iceberg connector |

## Write Operations

JDBC engines are **read-only**. All write operations (materialization, partition reorganization) use DuckDB with the Iceberg Java API regardless of the configured query engine.

## Performance Considerations

### DuckDB
- Best for single-node datasets under 100GB
- Excellent columnar scan performance
- In-memory caching for repeated queries

### Trino
- Distributed execution for large datasets
- Query latency 5-15 seconds typical
- Best for federated queries across multiple sources

### Spark SQL
- Leverage existing Spark infrastructure
- Good for ETL integration
- Higher latency than DuckDB for simple queries

### ClickHouse
- Excellent for OLAP and aggregations
- Native time-series optimizations
- Sub-second queries on pre-aggregated data

## Dependencies

JDBC drivers are optional dependencies. Add the appropriate driver for your engine:

```gradle
// DuckDB (included by default)
implementation 'org.duckdb:duckdb_jdbc:1.0.0'

// Trino
runtimeOnly 'io.trino:trino-jdbc:428'

// Spark (Hive JDBC)
runtimeOnly 'org.apache.hive:hive-jdbc:3.1.3'

// ClickHouse
runtimeOnly 'com.clickhouse:clickhouse-jdbc:0.6.0'
```

## Troubleshooting

### Connection Issues

**DuckDB**: Check file permissions and disk space for database file.

**Trino/Spark/ClickHouse**: Verify network connectivity:
```bash
# Test Trino
curl -v http://trino.example.com:8080/v1/info

# Test ClickHouse
curl -v 'http://clickhouse.example.com:8123/?query=SELECT%201'
```

### Driver Not Found

Ensure the JDBC driver JAR is on the classpath:
```bash
java -cp "lib/*:your-app.jar" ...
```

### Authentication Errors

For Trino with Kerberos:
```bash
kinit your-principal@REALM
export KRB5CCNAME=/tmp/krb5cc_$(id -u)
```

## See Also

- [Configuration Reference](configuration-reference.md) - Full configuration options
- [Performance Tuning](performance-tuning.md) - Optimization strategies
- [Iceberg Integration](iceberg-integration.md) - Iceberg table format details
