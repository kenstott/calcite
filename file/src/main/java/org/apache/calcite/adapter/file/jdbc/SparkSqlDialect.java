/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.jdbc;

import java.util.List;
import java.util.Map;

/**
 * JDBC dialect implementation for Spark SQL via Thrift Server.
 *
 * <p>Spark SQL is a distributed query engine that provides:
 * <ul>
 *   <li>Integration with existing Spark infrastructure</li>
 *   <li>Direct Parquet file access using backtick syntax</li>
 *   <li>Unified analytics across batch and streaming data</li>
 *   <li>Support for Hive metastore catalog</li>
 * </ul>
 *
 * <p>Spark SQL connections are made via the Spark Thrift Server using the
 * HiveServer2 JDBC driver. The Thrift Server must be running for connections
 * to succeed.
 *
 * <p>Example usage:
 * <pre>{@code
 * JdbcDialect dialect = new SparkSqlDialect();
 * String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", Arrays.asList("id", "name"));
 * // Returns: SELECT id, name FROM parquet.`s3://bucket/data/*.parquet`
 * }</pre>
 *
 * @see JdbcDialect
 */
public class SparkSqlDialect implements JdbcDialect {

  /** Singleton instance for reuse. */
  public static final SparkSqlDialect INSTANCE = new SparkSqlDialect();

  /** Default Spark Thrift Server port. */
  private static final String DEFAULT_PORT = "10000";

  /** Default database name. */
  private static final String DEFAULT_DATABASE = "default";

  @Override
  public String getDriverClassName() {
    return "org.apache.hive.jdbc.HiveDriver";
  }

  @Override
  public String buildJdbcUrl(Map<String, String> config) {
    String host = getConfigValue(config, "host", "localhost");
    String port = getConfigValue(config, "port", DEFAULT_PORT);
    String database = getConfigValue(config, "database", DEFAULT_DATABASE);

    return String.format("jdbc:hive2://%s:%s/%s", host, port, database);
  }

  @Override
  public String readParquetSql(String globPattern, List<String> columns) {
    // Spark SQL supports direct parquet path access with backticks
    String cols = formatColumns(columns);
    return String.format("SELECT %s FROM parquet.`%s`", cols, globPattern);
  }

  @Override
  public boolean supportsDirectGlob() {
    return true; // Spark supports parquet.`path` syntax with globs
  }

  @Override
  public String createParquetViewSql(String schemaName, String viewName, String path,
      boolean hivePartitioning) {
    // Spark SQL uses backtick syntax for direct parquet file access
    // hivePartitioning is automatically detected by Spark when reading parquet
    String qualifiedName = qualifyName(schemaName, viewName);
    return String.format(
        "CREATE OR REPLACE VIEW %s AS SELECT * FROM parquet.`%s`",
        qualifiedName, path);
  }

  @Override
  public String createIcebergViewSql(String schemaName, String viewName, String tablePath) {
    // Spark requires Iceberg tables to be registered in a catalog
    // Using the Iceberg catalog reference syntax
    String qualifiedName = qualifyName(schemaName, viewName);
    return String.format(
        "CREATE OR REPLACE VIEW %s AS SELECT * FROM iceberg.`%s`",
        qualifiedName, tablePath);
  }

  @Override
  public String createOrReplaceParquetViewSql(String schemaName, String viewName, String path,
      boolean hivePartitioning) {
    // Spark supports CREATE OR REPLACE VIEW natively
    return createParquetViewSql(schemaName, viewName, path, hivePartitioning);
  }

  @Override
  public boolean supportsIceberg() {
    return true; // Spark supports Iceberg via the iceberg catalog
  }

  @Override
  public String getName() {
    return "Spark SQL";
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
