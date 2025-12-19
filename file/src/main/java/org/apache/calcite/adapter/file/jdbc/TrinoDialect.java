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
 * JDBC dialect implementation for Trino (formerly PrestoSQL).
 *
 * <p>Trino is a distributed SQL query engine designed for:
 * <ul>
 *   <li>Federated queries across multiple data sources</li>
 *   <li>Interactive analytics on large datasets</li>
 *   <li>Data mesh architectures with catalog-based data access</li>
 * </ul>
 *
 * <p>Unlike DuckDB, Trino does not support direct file access via glob patterns.
 * Files must be registered in a catalog (Hive, AWS Glue, etc.) before querying.
 * The {@link #registerTableSql} method generates the DDL to create external tables.
 *
 * <p>Example configuration:
 * <pre>{@code
 * Map<String, String> config = new HashMap<>();
 * config.put("host", "trino.example.com");
 * config.put("port", "8080");
 * config.put("catalog", "hive");
 * config.put("schema", "datalake");
 *
 * JdbcDialect dialect = new TrinoDialect();
 * String url = dialect.buildJdbcUrl(config);
 * // Returns: jdbc:trino://trino.example.com:8080/hive/datalake
 * }</pre>
 *
 * @see JdbcDialect
 */
public class TrinoDialect implements JdbcDialect {

  /** Singleton instance for reuse. */
  public static final TrinoDialect INSTANCE = new TrinoDialect();

  /** Default Trino port. */
  private static final String DEFAULT_PORT = "8080";

  /** Default catalog name. */
  private static final String DEFAULT_CATALOG = "hive";

  /** Default schema name. */
  private static final String DEFAULT_SCHEMA = "default";

  @Override public String getDriverClassName() {
    return "io.trino.jdbc.TrinoDriver";
  }

  @Override public String buildJdbcUrl(Map<String, String> config) {
    String host = getConfigValue(config, "host", "localhost");
    String port = getConfigValue(config, "port", DEFAULT_PORT);
    String catalog = getConfigValue(config, "catalog", DEFAULT_CATALOG);
    String schema = getConfigValue(config, "schema", DEFAULT_SCHEMA);

    return String.format("jdbc:trino://%s:%s/%s/%s", host, port, catalog, schema);
  }

  @Override public String readParquetSql(String globPattern, List<String> columns) {
    // Trino requires tables to be registered in a catalog first.
    // This method assumes the table has been registered and derives a table name from the path.
    String cols = formatColumns(columns);
    String tableName = pathToTableName(globPattern);
    return String.format("SELECT %s FROM %s", cols, tableName);
  }

  @Override public boolean supportsDirectGlob() {
    return false; // Trino requires catalog registration
  }

  @Override public String registerTableSql(String tableName, String path, String format) {
    // Trino external table creation syntax
    // Note: The exact syntax depends on the connector (Hive, Delta Lake, etc.)
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s WITH (external_location = '%s', format = '%s')",
        tableName, path, format.toUpperCase());
  }

  @Override public String createParquetViewSql(String schemaName, String viewName, String path,
      boolean hivePartitioning) {
    // Trino requires external tables in the Hive catalog
    // The schema must be pre-configured with the Hive connector
    String qualifiedName = qualifyName(schemaName, viewName);

    // For Hive connector, create an external table pointing to the parquet location
    // Note: hive_partitioning is handled by the metastore, not in the SQL
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE IF NOT EXISTS ");
    sql.append(qualifiedName);
    sql.append(" WITH (external_location = '");
    sql.append(path);
    sql.append("', format = 'PARQUET'");
    if (hivePartitioning) {
      sql.append(", partitioned_by = ARRAY[]"); // Placeholder - actual partitions from metastore
    }
    sql.append(")");

    return sql.toString();
  }

  @Override public String createIcebergViewSql(String schemaName, String viewName, String tablePath) {
    // For Trino with Iceberg connector, register the Iceberg table location
    // This requires the iceberg catalog to be configured
    String qualifiedName = qualifyName(schemaName, viewName);
    return String.format(
        "CALL iceberg.system.register_table('%s', '%s', '%s')",
        schemaName != null ? schemaName : "default",
        viewName,
        tablePath);
  }

  @Override public String dropViewSql(String schemaName, String viewName) {
    // Trino uses DROP TABLE for external tables
    String qualifiedName = qualifyName(schemaName, viewName);
    return String.format("DROP TABLE IF EXISTS %s", qualifiedName);
  }

  @Override public String getName() {
    return "Trino";
  }

  @Override public boolean supportsIceberg() {
    return true; // Trino supports Iceberg via the iceberg connector
  }

  /**
   * Converts a file path or glob pattern to a table name.
   *
   * <p>This is a simple heuristic that extracts the last path component
   * before any file extension or glob characters.
   *
   * @param path the file path or glob pattern
   * @return a table name derived from the path
   */
  private static String pathToTableName(String path) {
    if (path == null || path.isEmpty()) {
      return "unknown_table";
    }

    // Remove glob patterns
    String cleaned = path;
    int starIndex = cleaned.indexOf('*');
    if (starIndex > 0) {
      cleaned = cleaned.substring(0, starIndex);
    }

    // Remove trailing slashes
    while (cleaned.endsWith("/") || cleaned.endsWith("\\")) {
      cleaned = cleaned.substring(0, cleaned.length() - 1);
    }

    // Get the last path component
    int lastSlash = Math.max(cleaned.lastIndexOf('/'), cleaned.lastIndexOf('\\'));
    if (lastSlash >= 0) {
      cleaned = cleaned.substring(lastSlash + 1);
    }

    // Remove file extension
    int dotIndex = cleaned.lastIndexOf('.');
    if (dotIndex > 0) {
      cleaned = cleaned.substring(0, dotIndex);
    }

    // Sanitize for use as SQL identifier
    return cleaned.replaceAll("[^a-zA-Z0-9_]", "_");
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
