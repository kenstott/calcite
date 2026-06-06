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
 * JDBC dialect implementation for ClickHouse.
 *
 * <p>ClickHouse is a columnar OLAP database optimized for:
 * <ul>
 *   <li>Fast OLAP queries and time-series analytics</li>
 *   <li>Real-time data ingestion and querying</li>
 *   <li>Direct S3 file access via the s3() table function</li>
 *   <li>Native Iceberg table support via the iceberg() function</li>
 * </ul>
 *
 * <p>ClickHouse can be deployed as a distributed cluster or embedded via chDB.
 * This dialect supports both deployment modes through the same JDBC interface.
 *
 * <p>Example usage:
 * <pre>{@code
 * JdbcDialect dialect = new ClickHouseDialect();
 * String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", Arrays.asList("id", "name"));
 * // Returns: SELECT id, name FROM s3('s3://bucket/data/*.parquet', 'Parquet')
 * }</pre>
 *
 * @see JdbcDialect
 */
public class ClickHouseDialect implements JdbcDialect {

  /** Singleton instance for reuse. */
  public static final ClickHouseDialect INSTANCE = new ClickHouseDialect();

  /** Default ClickHouse HTTP port. */
  private static final String DEFAULT_PORT = "8123";

  /** Default database name. */
  private static final String DEFAULT_DATABASE = "default";

  @Override public String getDriverClassName() {
    return "com.clickhouse.jdbc.ClickHouseDriver";
  }

  @Override public String buildJdbcUrl(Map<String, String> config) {
    String host = getConfigValue(config, "host", "localhost");
    String port = getConfigValue(config, "port", DEFAULT_PORT);
    String database = getConfigValue(config, "database", DEFAULT_DATABASE);

    return String.format("jdbc:clickhouse://%s:%s/%s", host, port, database);
  }

  @Override public String readParquetSql(String globPattern, List<String> columns) {
    // ClickHouse s3() function supports globs natively for S3 paths
    // For local files, the file() function would be used
    String cols = formatColumns(columns);
    return String.format("SELECT %s FROM s3('%s', 'Parquet')", cols, globPattern);
  }

  @Override public String readIcebergSql(String tablePath, List<String> columns) {
    // ClickHouse has a native iceberg() table function
    String cols = formatColumns(columns);
    return String.format("SELECT %s FROM iceberg('%s')", cols, tablePath);
  }

  @Override public boolean supportsDirectGlob() {
    return true; // s3() and file() functions support globs
  }

  @Override public boolean supportsIceberg() {
    return true; // Native iceberg() table function
  }

  @Override public String createParquetViewSql(String schemaName, String viewName, String path,
      boolean hivePartitioning) {
    // ClickHouse uses s3() table function for S3 paths, file() for local
    // Note: ClickHouse doesn't support IF NOT EXISTS for views, use CREATE OR REPLACE
    String qualifiedName = qualifyName(schemaName, viewName);
    String tableFunc = isS3Path(path) ? "s3" : "file";

    // s3() function signature: s3(path, [aws_access_key_id, aws_secret_access_key,] format)
    // For simplicity, assume credentials are configured at server level
    return String.format(
        "CREATE OR REPLACE VIEW %s AS SELECT * FROM %s('%s', 'Parquet')",
        qualifiedName, tableFunc, path);
  }

  @Override public String createIcebergViewSql(String schemaName, String viewName, String tablePath) {
    // ClickHouse has native iceberg() table function
    String qualifiedName = qualifyName(schemaName, viewName);
    return String.format(
        "CREATE OR REPLACE VIEW %s AS SELECT * FROM iceberg('%s')",
        qualifiedName, tablePath);
  }

  @Override public String createOrReplaceParquetViewSql(String schemaName, String viewName, String path,
      boolean hivePartitioning) {
    // ClickHouse uses CREATE OR REPLACE by default
    return createParquetViewSql(schemaName, viewName, path, hivePartitioning);
  }

  @Override public String dropViewSql(String schemaName, String viewName) {
    String qualifiedName = qualifyName(schemaName, viewName);
    return String.format("DROP VIEW IF EXISTS %s", qualifiedName);
  }

  @Override public String getName() {
    return "ClickHouse";
  }

  /**
   * Checks if a path is an S3 path.
   *
   * @param path the path to check
   * @return true if it's an S3 path
   */
  private static boolean isS3Path(String path) {
    return path != null && (path.startsWith("s3://") || path.startsWith("s3a://"));
  }

  /**
   * Gets a configuration value with a default fallback.
   *
   * @param config the configuration map
   * @param key the key to look up
   * @param defaultValue the default if key is not found
   * @return the configuration value or default
   */
  private static String getConfigValue(Map<String, String> config, String key, String defaultValue) {
    if (config == null) {
      return defaultValue;
    }
    String value = config.get(key);
    return (value != null && !value.isEmpty()) ? value : defaultValue;
  }

  /**
   * Formats a list of columns for a SELECT clause.
   *
   * @param columns list of column names, or empty for all columns
   * @return column list string (e.g., "col1, col2" or "*")
   */
  private static String formatColumns(List<String> columns) {
    if (columns == null || columns.isEmpty()) {
      return "*";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(columns.get(i));
    }
    return sb.toString();
  }
}
