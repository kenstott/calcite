# Design: Pluggable JDBC Query Engines

## Overview

The file adapter currently supports multiple query engines for executing queries against parquet files:

- **Linq4j** - Calcite's native enumerable execution
- **Arrow** - Apache Arrow columnar processing
- **Parquet** - Direct parquet reading
- **DuckDB** - Embedded SQL engine via JDBC

This document describes extending JDBC-based engine support to include **Trino**, **Spark SQL**, and **ClickHouse**, enabling the file adapter to leverage existing distributed SQL infrastructure.

## Motivation

Organizations with existing data lake infrastructure often have Trino or Spark clusters available. Rather than requiring a separate DuckDB instance, the file adapter could delegate query execution to these existing systems, providing:

1. **Infrastructure reuse** - No additional compute infrastructure needed
2. **Scale** - Distributed execution for multi-TB datasets
3. **Governance** - Leverage existing security, auditing, and catalog management
4. **Flexibility** - Choose engine based on data scale and available infrastructure

## Architecture

### Current State

```
FileQueryEngine (enum)
├── LINQ4J      → Linq4jQueryEngine
├── ARROW       → ArrowQueryEngine
├── PARQUET     → ParquetQueryEngine
└── DUCKDB      → DuckDBJdbcSchema (JDBC-based)
```

### Proposed State

```
FileQueryEngine (enum)
├── LINQ4J      → Linq4jQueryEngine
├── ARROW       → ArrowQueryEngine
├── PARQUET     → ParquetQueryEngine
└── JDBC-based engines (share common framework)
    ├── DUCKDB      → JdbcQueryEngine + DuckDBDialect
    ├── TRINO       → JdbcQueryEngine + TrinoDialect
    ├── SPARK       → JdbcQueryEngine + SparkSqlDialect
    └── CLICKHOUSE  → JdbcQueryEngine + ClickHouseDialect
```

## JDBC Engine Comparison

| Aspect | DuckDB | Trino | Spark SQL | ClickHouse |
|--------|--------|-------|-----------|------------|
| **Deployment** | Embedded | Distributed cluster | Distributed cluster | Embedded (chDB) or Distributed |
| **JDBC Driver** | `org.duckdb.DuckDBDriver` | `io.trino.jdbc.TrinoDriver` | `org.apache.hive.jdbc.HiveDriver` | `com.clickhouse.jdbc.ClickHouseDriver` |
| **URL Pattern** | `jdbc:duckdb:` | `jdbc:trino://host:port/catalog` | `jdbc:hive2://host:10000/default` | `jdbc:clickhouse://host:8123/db` |
| **Read Parquet** | `read_parquet('s3://...')` | Catalog table | `parquet.\`s3://...\`` | `s3('s3://...', 'Parquet')` |
| **Read Iceberg** | `iceberg_scan('s3://...')` | Catalog table | Catalog table | `iceberg('s3://...')` |
| **Direct Glob Support** | Yes | No (requires catalog) | Limited | Yes |
| **Best For** | Single-node, <100GB | Federated queries, data mesh | Existing Spark infrastructure | Fast OLAP, time-series analytics |

**Note**: JDBC engines are used for **query execution (reads)** only. Materialization (writes) uses DuckDB + Iceberg Java API per the Partition Alternates design.

## Interface Design

### JdbcDialect Interface

```java
package org.apache.calcite.adapter.file.jdbc;

/**
 * Abstraction for SQL dialect differences between JDBC-based query engines.
 */
public interface JdbcDialect {

    /**
     * Get the JDBC driver class name.
     */
    String getDriverClassName();

    /**
     * Build JDBC connection URL from configuration.
     */
    String buildJdbcUrl(Map<String, String> config);

    /**
     * Generate SQL to read parquet files.
     * @param globPattern Path pattern (e.g., "s3://bucket/path/*.parquet")
     * @param columns Columns to select, or empty for all
     * @return SQL query string
     */
    String readParquetSql(String globPattern, List<String> columns);

    /**
     * Generate SQL to read Iceberg tables.
     * @param tablePath Path to Iceberg table (e.g., "s3://bucket/warehouse/table")
     * @param columns Columns to select, or empty for all
     * @return SQL query string, or null if not supported
     */
    default String readIcebergSql(String tablePath, List<String> columns) {
        return null; // Override if engine supports Iceberg
    }

    /**
     * Whether this engine supports direct glob pattern access.
     * If false, files must be registered in a catalog first.
     */
    boolean supportsDirectGlob();

    /**
     * Whether this engine supports reading Iceberg tables directly.
     */
    default boolean supportsIceberg() {
        return false;
    }

    /**
     * Generate SQL to create a view spanning multiple Iceberg snapshots.
     * Enables time travel queries via standard SQL WHERE clauses.
     *
     * @param viewName Name of the view to create
     * @param tablePath Path to Iceberg table
     * @param snapshots List of snapshot info (id, timestamp)
     * @param snapshotColumnName Name for the snapshot timestamp column
     * @return SQL to create the view, or null if not supported
     */
    default String createIcebergTimeRangeViewSql(String viewName, String tablePath,
                                                  List<SnapshotInfo> snapshots,
                                                  String snapshotColumnName) {
        return null; // Override if engine supports Iceberg time travel
    }

    /**
     * Snapshot information for time range views.
     */
    class SnapshotInfo {
        public final long snapshotId;
        public final Instant timestamp;

        public SnapshotInfo(long snapshotId, Instant timestamp) {
            this.snapshotId = snapshotId;
            this.timestamp = timestamp;
        }
    }

    /**
     * Register a path as a table in the engine's catalog.
     * Only needed when supportsDirectGlob() returns false.
     */
    default String registerTableSql(String tableName, String path,
                                    String format) {
        throw new UnsupportedOperationException(
            "This dialect requires catalog registration");
    }
}
```

### Dialect Implementations

#### DuckDBDialect

```java
public class DuckDBDialect implements JdbcDialect {

    @Override
    public String getDriverClassName() {
        return "org.duckdb.DuckDBDriver";
    }

    @Override
    public String buildJdbcUrl(Map<String, String> config) {
        String path = config.getOrDefault("path", "");
        return path.isEmpty() ? "jdbc:duckdb:" : "jdbc:duckdb:" + path;
    }

    @Override
    public String readParquetSql(String globPattern, List<String> columns) {
        String cols = columns.isEmpty() ? "*" : String.join(", ", columns);
        return String.format("SELECT %s FROM read_parquet('%s')", cols, globPattern);
    }

    @Override
    public String readIcebergSql(String tablePath, List<String> columns) {
        String cols = columns.isEmpty() ? "*" : String.join(", ", columns);
        return String.format("SELECT %s FROM iceberg_scan('%s')", cols, tablePath);
    }

    @Override
    public boolean supportsDirectGlob() {
        return true;
    }

    @Override
    public boolean supportsIceberg() {
        return true;
    }

    @Override
    public String createIcebergTimeRangeViewSql(String viewName, String tablePath,
                                                 List<SnapshotInfo> snapshots,
                                                 String snapshotColumnName) {
        StringBuilder sql = new StringBuilder("CREATE OR REPLACE VIEW ");
        sql.append(viewName).append(" AS\n");

        for (int i = 0; i < snapshots.size(); i++) {
            if (i > 0) sql.append("\nUNION ALL\n");
            SnapshotInfo snap = snapshots.get(i);
            sql.append(String.format(
                "SELECT *, TIMESTAMP '%s' AS %s FROM iceberg_scan('%s', version = '%d')",
                snap.timestamp.toString(),
                snapshotColumnName,
                tablePath,
                snap.snapshotId
            ));
        }
        return sql.toString();
    }
}
```

#### TrinoDialect

```java
public class TrinoDialect implements JdbcDialect {

    @Override
    public String getDriverClassName() {
        return "io.trino.jdbc.TrinoDriver";
    }

    @Override
    public String buildJdbcUrl(Map<String, String> config) {
        String host = config.getOrDefault("host", "localhost");
        String port = config.getOrDefault("port", "8080");
        String catalog = config.getOrDefault("catalog", "hive");
        String schema = config.getOrDefault("schema", "default");
        return String.format("jdbc:trino://%s:%s/%s/%s", host, port, catalog, schema);
    }

    @Override
    public String readParquetSql(String globPattern, List<String> columns) {
        // Trino requires tables to be registered in catalog
        // This returns a placeholder - actual implementation needs catalog lookup
        String cols = columns.isEmpty() ? "*" : String.join(", ", columns);
        return String.format("SELECT %s FROM %s", cols, pathToTableName(globPattern));
    }

    @Override
    public boolean supportsDirectGlob() {
        return false; // Requires catalog registration
    }

    @Override
    public String registerTableSql(String tableName, String path, String format) {
        return String.format(
            "CREATE TABLE IF NOT EXISTS %s WITH (external_location = '%s', format = '%s')",
            tableName, path, format.toUpperCase());
    }
}
```

#### SparkSqlDialect

```java
public class SparkSqlDialect implements JdbcDialect {

    @Override
    public String getDriverClassName() {
        return "org.apache.hive.jdbc.HiveDriver";
    }

    @Override
    public String buildJdbcUrl(Map<String, String> config) {
        String host = config.getOrDefault("host", "localhost");
        String port = config.getOrDefault("port", "10000");
        String database = config.getOrDefault("database", "default");
        return String.format("jdbc:hive2://%s:%s/%s", host, port, database);
    }

    @Override
    public String readParquetSql(String globPattern, List<String> columns) {
        String cols = columns.isEmpty() ? "*" : String.join(", ", columns);
        // Spark SQL supports direct parquet path access with backticks
        return String.format("SELECT %s FROM parquet.`%s`", cols, globPattern);
    }

    @Override
    public boolean supportsDirectGlob() {
        return true; // Spark supports parquet.`path` syntax
    }
}
```

#### ClickHouseDialect

```java
public class ClickHouseDialect implements JdbcDialect {

    @Override
    public String getDriverClassName() {
        return "com.clickhouse.jdbc.ClickHouseDriver";
    }

    @Override
    public String buildJdbcUrl(Map<String, String> config) {
        String host = config.getOrDefault("host", "localhost");
        String port = config.getOrDefault("port", "8123");
        String database = config.getOrDefault("database", "default");
        return String.format("jdbc:clickhouse://%s:%s/%s", host, port, database);
    }

    @Override
    public String readParquetSql(String globPattern, List<String> columns) {
        String cols = columns.isEmpty() ? "*" : String.join(", ", columns);
        // ClickHouse s3() function supports globs natively
        return String.format("SELECT %s FROM s3('%s', 'Parquet')", cols, globPattern);
    }

    @Override
    public String readIcebergSql(String tablePath, List<String> columns) {
        String cols = columns.isEmpty() ? "*" : String.join(", ", columns);
        // ClickHouse iceberg() table function for Iceberg tables
        return String.format("SELECT %s FROM iceberg('%s')", cols, tablePath);
    }

    @Override
    public boolean supportsDirectGlob() {
        return true; // s3() and file() functions support globs
    }

    @Override
    public boolean supportsIceberg() {
        return true; // Native iceberg() table function
    }

    @Override
    public String createIcebergTimeRangeViewSql(String viewName, String tablePath,
                                                 List<SnapshotInfo> snapshots,
                                                 String snapshotColumnName) {
        StringBuilder sql = new StringBuilder("CREATE OR REPLACE VIEW ");
        sql.append(viewName).append(" AS\n");

        for (int i = 0; i < snapshots.size(); i++) {
            if (i > 0) sql.append("\nUNION ALL\n");
            SnapshotInfo snap = snapshots.get(i);
            sql.append(String.format(
                "SELECT *, toDateTime('%s') AS %s FROM iceberg('%s', snapshot_id = %d)",
                snap.timestamp.toString(),
                snapshotColumnName,
                tablePath,
                snap.snapshotId
            ));
        }
        return sql.toString();
    }
}
```

## Configuration

### Environment Variable

```bash
# Current
export CALCITE_FILE_ENGINE_TYPE=duckdb

# Extended
export CALCITE_FILE_ENGINE_TYPE=trino
export CALCITE_FILE_ENGINE_TYPE=spark
export CALCITE_FILE_ENGINE_TYPE=clickhouse
```

### Schema Configuration (model.json)

```json
{
  "schemas": [{
    "name": "datalake",
    "type": "custom",
    "factory": "org.apache.calcite.adapter.file.FileSchemaFactory",
    "operand": {
      "queryEngine": {
        "type": "trino",
        "host": "trino.example.com",
        "port": "8080",
        "catalog": "hive",
        "schema": "datalake"
      }
    }
  }]
}
```

Or for Spark:

```json
{
  "operand": {
    "queryEngine": {
      "type": "spark",
      "host": "spark-thrift.example.com",
      "port": "10000",
      "database": "default"
    }
  }
}
```

Or for ClickHouse:

```json
{
  "operand": {
    "queryEngine": {
      "type": "clickhouse",
      "host": "clickhouse.example.com",
      "port": "8123",
      "database": "default"
    }
  }
}
```

## Implementation Plan

### Phase 1: Extract Dialect Abstraction
1. Create `JdbcDialect` interface
2. Implement `DuckDBDialect` by extracting from current `DuckDBJdbcSchema`
3. Create `JdbcQueryEngine` that uses dialect for SQL generation
4. Refactor `DuckDBJdbcSchema` to use new abstraction
5. Verify no regression in existing DuckDB functionality

### Phase 2: Add Trino Support
1. Implement `TrinoDialect`
2. Add Trino JDBC driver dependency (optional/provided scope)
3. Handle catalog registration requirement
4. Add integration tests with Trino testcontainer

### Phase 3: Add Spark SQL Support
1. Implement `SparkSqlDialect`
2. Add Hive JDBC driver dependency (optional/provided scope)
3. Handle Spark Thrift Server connection specifics
4. Add integration tests with Spark testcontainer

### Phase 4: Add ClickHouse Support
1. Implement `ClickHouseDialect`
2. Add ClickHouse JDBC driver dependency (optional/provided scope)
3. Support both `s3()` for parquet and `iceberg()` for Iceberg tables
4. Add integration tests with ClickHouse testcontainer

### Phase 5: Documentation and Polish
1. Update user documentation
2. Add configuration examples
3. Performance benchmarking across engines
4. Error handling improvements

## Considerations

### Catalog Management

DuckDB can read arbitrary parquet paths directly. Trino and Spark typically require files to be registered in a metastore (Hive, AWS Glue, etc.).

Options:
1. **Require pre-registration** - User manages catalog externally
2. **Auto-register** - Adapter creates temporary tables as needed
3. **Hybrid** - Support both modes via configuration

### Authentication

Each engine has different authentication mechanisms:
- **DuckDB**: File-based, no auth for embedded
- **Trino**: Kerberos, LDAP, OAuth2, certificate
- **Spark**: Kerberos, simple auth

The dialect should expose authentication configuration options.

### Connection Pooling

- **DuckDB**: Single embedded connection, or pool for remote
- **Trino/Spark**: Standard JDBC pooling (HikariCP, etc.)

Consider making pooling configurable per dialect.

### Error Handling

Different engines return different error formats. The `JdbcDialect` could include:

```java
SQLException translateException(SQLException e);
```

To normalize error messages across engines.

## Testing Strategy

1. **Unit tests**: Dialect SQL generation with expected output
2. **Integration tests**: Testcontainers for each engine
3. **Performance tests**: Compare engines on standard workloads
4. **Compatibility tests**: Ensure same queries work across engines

## Dependencies

```gradle
// Optional dependencies - only needed if using that engine
compileOnly 'io.trino:trino-jdbc:4xx'
compileOnly 'org.apache.hive:hive-jdbc:3.x.x'
compileOnly 'com.clickhouse:clickhouse-jdbc:0.6.x'
```

## Open Questions

1. Should dialect selection be automatic based on JDBC URL pattern?
2. How to handle engine-specific optimizations (e.g., DuckDB's parallel read)?
3. Should we support engine failover (try DuckDB, fall back to Trino)?
4. How to expose engine-specific features without breaking abstraction?

## References

- [Trino JDBC Driver](https://trino.io/docs/current/client/jdbc.html)
- [Spark Thrift Server](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html)
- [DuckDB JDBC](https://duckdb.org/docs/api/java)
- [ClickHouse JDBC Driver](https://clickhouse.com/docs/integrations/java/jdbc-driver)
- [ClickHouse Iceberg Table Engine](https://clickhouse.com/docs/engines/table-engines/integrations/iceberg)
