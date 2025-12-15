# JDBC Query Engines

The File Adapter supports pluggable JDBC-based query engines for executing SQL against parquet files. This enables you to leverage existing SQL infrastructure or choose the best engine for your workload.

## Supported Engines

| Engine | Deployment | Best For | Iceberg Support |
|--------|------------|----------|-----------------|
| **DuckDB** | Embedded | Single-node analytics, <100GB datasets | Yes |
| **Trino** | Distributed cluster | Federated queries across multiple sources | Via catalog |
| **ClickHouse** | Embedded (chDB) or cluster | Fast OLAP, time-series, aggregations | Yes |
| **Spark SQL** | Distributed cluster | Legacy integration only | Via catalog |

**Recommended engines**: DuckDB (default), Trino (federated), ClickHouse (OLAP). Spark support exists for infrastructure compatibility but offers no performance advantage.

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

Requires external Trino cluster. The dialect generates DDL to register tables in Trino's catalog automatically.

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

**Requirements:**
- External Trino cluster with Hive connector (or similar) for external table support
- Authentication via Kerberos, LDAP, or OAuth2 supported

### Spark SQL (Legacy)

Requires Spark Thrift Server (HiveServer2 interface). **Not recommended for new deployments** - DuckDB is faster for single-node, Trino better for federation.

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

**Requirements:**
- Spark Thrift Server running (exposes HiveServer2 interface)

**When to use:** Only if you have existing Spark infrastructure and want to avoid adding another system. Spark's Thrift Server is batch-oriented with higher latency than interactive engines.

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

**Requirements:**
- ClickHouse server (external cluster) or chDB (embedded)
- For S3 access, configure ClickHouse with appropriate credentials

## Write Operations

JDBC engines are **read-only**. All write operations (materialization, partition reorganization) use DuckDB with the Iceberg Java API regardless of the configured query engine.

## Choosing an Engine

### DuckDB (Recommended Default)
- Best for single-node datasets under 100GB
- Excellent columnar scan performance
- In-memory caching for repeated queries
- No external infrastructure required

### Trino (Federated Queries)
- Query across multiple data sources (Postgres, S3, Kafka, etc.)
- Distributed execution for datasets exceeding single-node capacity
- Query latency 5-15 seconds typical (not for sub-second interactive)

### ClickHouse (OLAP Workloads)
- Sub-second aggregations on billions of rows
- Native time-series optimizations
- Excellent for dashboards and analytics

### Spark SQL (Legacy Only)
- Higher latency than alternatives for interactive queries
- Thrift Server is batch-oriented, not designed for low-latency
- Use only when Spark infrastructure already exists and consolidation is priority

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
