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
 * JDBC dialect implementation for DuckDB.
 *
 * <p>DuckDB is an embedded analytical database that excels at:
 * <ul>
 *   <li>Single-node analytics on datasets up to ~100GB</li>
 *   <li>Direct reading of Parquet files with glob patterns</li>
 *   <li>Native Iceberg table support via the iceberg extension</li>
 *   <li>Vectorized columnar execution with automatic parallelization</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * JdbcDialect dialect = new DuckDBDialect();
 * String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", Arrays.asList("id", "name"));
 * // Returns: SELECT id, name FROM read_parquet('s3://bucket/data/*.parquet')
 * }</pre>
 *
 * @see JdbcDialect
 */
public class DuckDBDialect implements JdbcDialect {

  /** Singleton instance for reuse. */
  public static final DuckDBDialect INSTANCE = new DuckDBDialect();

  @Override public String getDriverClassName() {
    return "org.duckdb.DuckDBDriver";
  }

  @Override public String buildJdbcUrl(Map<String, String> config) {
    String path = config != null ? config.get("path") : null;
    if (path == null || path.isEmpty()) {
      return "jdbc:duckdb:";
    }
    return "jdbc:duckdb:" + path;
  }

  @Override public String readParquetSql(String globPattern, List<String> columns) {
    String cols = formatColumns(columns);
    return String.format("SELECT %s FROM read_parquet('%s')", cols, globPattern);
  }

  @Override public String readIcebergSql(String tablePath, List<String> columns) {
    String cols = formatColumns(columns);
    return String.format("SELECT %s FROM iceberg_scan('%s')", cols, tablePath);
  }

  @Override public boolean supportsDirectGlob() {
    return true;
  }

  @Override public boolean supportsIceberg() {
    return true;
  }

  @Override public String createParquetViewSql(String schemaName, String viewName, String path,
      boolean hivePartitioning) {
    String qualifiedName = qualifyName(schemaName, viewName);
    if (hivePartitioning) {
      return String.format(
          "CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM read_parquet('%s', hive_partitioning = true)",
          qualifiedName, path);
    }
    return String.format(
        "CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM read_parquet('%s')",
        qualifiedName, path);
  }

  @Override public String createIcebergViewSql(String schemaName, String viewName, String tablePath) {
    String qualifiedName = qualifyName(schemaName, viewName);
    return String.format(
        "CREATE VIEW IF NOT EXISTS %s AS SELECT * FROM iceberg_scan('%s')",
        qualifiedName, tablePath);
  }

  @Override public String createOrReplaceParquetViewSql(String schemaName, String viewName, String path,
      boolean hivePartitioning) {
    String qualifiedName = qualifyName(schemaName, viewName);
    if (hivePartitioning) {
      return String.format(
          "CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s', hive_partitioning = true)",
          qualifiedName, path);
    }
    return String.format(
        "CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
        qualifiedName, path);
  }

  @Override public String getName() {
    return "DuckDB";
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
