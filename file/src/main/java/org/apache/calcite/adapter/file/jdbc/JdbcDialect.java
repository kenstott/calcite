/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.jdbc;

import java.util.List;
import java.util.Map;

/**
 * Abstraction for SQL dialect differences between JDBC-based query engines.
 *
 * <p>This interface provides a common abstraction for generating SQL statements
 * that work across different analytical query engines (DuckDB, Trino, Spark SQL,
 * ClickHouse). Each engine has different syntax for reading Parquet files,
 * connecting via JDBC, and handling advanced features like Iceberg tables.
 *
 * <p>Implementations of this interface are used by the file adapter to delegate
 * query execution to the appropriate JDBC-based engine while maintaining a
 * consistent programming model.
 *
 * @see DuckDBDialect
 * @see TrinoDialect
 * @see SparkSqlDialect
 * @see ClickHouseDialect
 */
public interface JdbcDialect {

  /**
   * Returns the fully-qualified JDBC driver class name for this dialect.
   *
   * <p>This is used to load the driver via {@code Class.forName()} before
   * establishing connections.
   *
   * @return the JDBC driver class name (e.g., "org.duckdb.DuckDBDriver")
   */
  String getDriverClassName();

  /**
   * Builds a JDBC connection URL from the provided configuration.
   *
   * <p>The configuration map may contain engine-specific settings such as:
   * <ul>
   *   <li>{@code path} - database file path (DuckDB)</li>
   *   <li>{@code host} - server hostname (Trino, Spark, ClickHouse)</li>
   *   <li>{@code port} - server port number</li>
   *   <li>{@code catalog} - catalog name (Trino)</li>
   *   <li>{@code schema} - schema name (Trino)</li>
   *   <li>{@code database} - database name (Spark, ClickHouse)</li>
   * </ul>
   *
   * @param config configuration map with connection parameters
   * @return a valid JDBC URL for this dialect
   */
  String buildJdbcUrl(Map<String, String> config);

  /**
   * Generates SQL to read Parquet files matching a glob pattern.
   *
   * <p>Different engines have different syntax for reading Parquet:
   * <ul>
   *   <li>DuckDB: {@code SELECT * FROM read_parquet('path/*.parquet')}</li>
   *   <li>Spark: {@code SELECT * FROM parquet.`path/*.parquet`}</li>
   *   <li>ClickHouse: {@code SELECT * FROM s3('path/*.parquet', 'Parquet')}</li>
   *   <li>Trino: Requires catalog registration (no direct file access)</li>
   * </ul>
   *
   * @param globPattern path pattern (e.g., "s3://bucket/path/*.parquet")
   * @param columns columns to select, or empty list for all columns (*)
   * @return SQL query string for reading the Parquet files
   */
  String readParquetSql(String globPattern, List<String> columns);

  /**
   * Generates SQL to read an Iceberg table.
   *
   * <p>Different engines have different Iceberg support:
   * <ul>
   *   <li>DuckDB: {@code SELECT * FROM iceberg_scan('path')}</li>
   *   <li>ClickHouse: {@code SELECT * FROM iceberg('path')}</li>
   *   <li>Trino/Spark: Require catalog table registration</li>
   * </ul>
   *
   * @param tablePath path to Iceberg table (e.g., "s3://bucket/warehouse/table")
   * @param columns columns to select, or empty list for all columns (*)
   * @return SQL query string, or null if Iceberg is not supported directly
   */
  default String readIcebergSql(String tablePath, List<String> columns) {
    return null; // Override if engine supports Iceberg
  }

  /**
   * Returns whether this engine supports direct glob pattern access to files.
   *
   * <p>Engines like DuckDB and Spark SQL can read files directly using glob patterns.
   * Engines like Trino require files to be registered in a catalog first.
   *
   * @return true if the engine can read files directly via glob patterns
   */
  boolean supportsDirectGlob();

  /**
   * Returns whether this engine supports reading Iceberg tables directly.
   *
   * <p>Some engines (DuckDB, ClickHouse) have native functions for reading Iceberg.
   * Others (Trino, Spark) require Iceberg tables to be registered in a catalog.
   *
   * @return true if the engine has direct Iceberg reading capability
   */
  default boolean supportsIceberg() {
    return false;
  }

  /**
   * Generates SQL to register a file path as a table in the engine's catalog.
   *
   * <p>This is primarily needed for engines that don't support direct glob access
   * (like Trino), where files must be registered as external tables before querying.
   *
   * @param tableName the name to give the registered table
   * @param path the file or directory path
   * @param format the file format (e.g., "PARQUET", "ORC")
   * @return SQL statement to register the table
   * @throws UnsupportedOperationException if the dialect doesn't support table registration
   */
  default String registerTableSql(String tableName, String path, String format) {
    throw new UnsupportedOperationException(
        "This dialect requires catalog registration but doesn't implement registerTableSql");
  }

  /**
   * Generates SQL to create a view that references external Parquet files.
   *
   * <p>This is the primary mechanism for making external data queryable. Each engine
   * has its own syntax for referencing external files:
   * <ul>
   *   <li>DuckDB: {@code CREATE VIEW ... AS SELECT * FROM read_parquet('path')}</li>
   *   <li>ClickHouse: {@code CREATE VIEW ... AS SELECT * FROM s3('path', 'Parquet')}</li>
   *   <li>Spark: {@code CREATE VIEW ... AS SELECT * FROM parquet.`path`}</li>
   *   <li>Trino: {@code CREATE TABLE ... WITH (external_location = 'path')}</li>
   * </ul>
   *
   * @param schemaName the schema name (may be null for engines without schema support)
   * @param viewName the view name to create
   * @param path the path or glob pattern to the Parquet files
   * @param hivePartitioning whether to enable Hive-style partition detection
   * @return SQL statement to create the view
   */
  String createParquetViewSql(String schemaName, String viewName, String path,
      boolean hivePartitioning);

  /**
   * Generates SQL to create a view that references an Iceberg table.
   *
   * @param schemaName the schema name (may be null for engines without schema support)
   * @param viewName the view name to create
   * @param tablePath the path to the Iceberg table metadata
   * @return SQL statement to create the view, or null if Iceberg is not supported
   */
  default String createIcebergViewSql(String schemaName, String viewName, String tablePath) {
    return null; // Override if engine supports Iceberg
  }

  /**
   * Generates SQL to drop a view if it exists.
   *
   * @param schemaName the schema name (may be null)
   * @param viewName the view name to drop
   * @return SQL statement to drop the view
   */
  default String dropViewSql(String schemaName, String viewName) {
    String qualifiedName = qualifyName(schemaName, viewName);
    return String.format("DROP VIEW IF EXISTS %s", qualifiedName);
  }

  /**
   * Generates SQL to create or replace a view (for refresh operations).
   *
   * @param schemaName the schema name (may be null)
   * @param viewName the view name
   * @param path the path or glob pattern to the Parquet files
   * @param hivePartitioning whether to enable Hive-style partition detection
   * @return SQL statement to create or replace the view
   */
  default String createOrReplaceParquetViewSql(String schemaName, String viewName, String path,
      boolean hivePartitioning) {
    // Default implementation: drop then create
    // Dialects can override with native CREATE OR REPLACE if supported
    return createParquetViewSql(schemaName, viewName, path, hivePartitioning);
  }

  /**
   * Generates SQL to create a schema if it doesn't exist.
   *
   * @param schemaName the schema name to create
   * @return SQL statement to create the schema
   */
  default String createSchemaSql(String schemaName) {
    return String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schemaName);
  }

  /**
   * Returns the dialect name for display and logging purposes.
   *
   * @return human-readable dialect name (e.g., "DuckDB", "Trino")
   */
  String getName();

  /**
   * Qualifies a view/table name with schema if provided.
   *
   * @param schemaName the schema name (may be null)
   * @param objectName the object name
   * @return qualified name (e.g., "schema"."table" or just "table")
   */
  default String qualifyName(String schemaName, String objectName) {
    if (schemaName == null || schemaName.isEmpty()) {
      return String.format("\"%s\"", objectName);
    }
    return String.format("\"%s\".\"%s\"", schemaName, objectName);
  }
}
