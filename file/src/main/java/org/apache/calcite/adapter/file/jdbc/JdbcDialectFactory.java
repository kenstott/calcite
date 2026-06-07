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

import java.util.Locale;

/**
 * Factory for creating {@link JdbcDialect} instances based on engine type.
 *
 * <p>This factory provides a centralized way to obtain the appropriate dialect
 * implementation for a given JDBC-based query engine. Engine types are
 * case-insensitive strings.
 *
 * <p>Supported engine types:
 * <ul>
 *   <li>{@code duckdb} - DuckDB embedded analytical database</li>
 *   <li>{@code trino} - Trino distributed SQL engine (formerly PrestoSQL)</li>
 *   <li>{@code spark} - Spark SQL via Thrift Server</li>
 *   <li>{@code clickhouse} - ClickHouse OLAP database</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * JdbcDialect dialect = JdbcDialectFactory.createDialect("duckdb");
 * String sql = dialect.readParquetSql("s3://bucket/data/*.parquet", Collections.emptyList());
 * }</pre>
 *
 * @see JdbcDialect
 */
public final class JdbcDialectFactory {

  /** Engine type constant for DuckDB. */
  public static final String ENGINE_DUCKDB = "duckdb";

  /** Engine type constant for Trino. */
  public static final String ENGINE_TRINO = "trino";

  /** Engine type constant for Spark SQL. */
  public static final String ENGINE_SPARK = "spark";

  /** Engine type constant for ClickHouse. */
  public static final String ENGINE_CLICKHOUSE = "clickhouse";

  private JdbcDialectFactory() {
    // Utility class - prevent instantiation
  }

  /**
   * Creates a JDBC dialect for the specified engine type.
   *
   * <p>Engine types are case-insensitive. The factory returns singleton instances
   * for each dialect type to minimize object creation.
   *
   * @param engineType the engine type (e.g., "duckdb", "trino", "spark", "clickhouse")
   * @return the appropriate JdbcDialect implementation
   * @throws IllegalArgumentException if the engine type is not recognized
   */
  public static JdbcDialect createDialect(String engineType) {
    if (engineType == null || engineType.isEmpty()) {
      throw new IllegalArgumentException("Engine type cannot be null or empty");
    }

    String normalizedType = engineType.toLowerCase(Locale.ROOT);

    switch (normalizedType) {
    case ENGINE_DUCKDB:
      return DuckDBDialect.INSTANCE;
    case ENGINE_TRINO:
      return TrinoDialect.INSTANCE;
    case ENGINE_SPARK:
      return SparkSqlDialect.INSTANCE;
    case ENGINE_CLICKHOUSE:
      return ClickHouseDialect.INSTANCE;
    default:
      throw new IllegalArgumentException(
          "Unknown JDBC engine type: " + engineType
          + ". Supported types: duckdb, trino, spark, clickhouse");
    }
  }

  /**
   * Returns whether the specified engine type is supported.
   *
   * @param engineType the engine type to check
   * @return true if the engine type is supported
   */
  public static boolean isSupported(String engineType) {
    if (engineType == null || engineType.isEmpty()) {
      return false;
    }

    String normalizedType = engineType.toLowerCase(Locale.ROOT);
    return ENGINE_DUCKDB.equals(normalizedType)
        || ENGINE_TRINO.equals(normalizedType)
        || ENGINE_SPARK.equals(normalizedType)
        || ENGINE_CLICKHOUSE.equals(normalizedType);
  }

  /**
   * Returns an array of all supported engine type names.
   *
   * @return array of supported engine type strings
   */
  public static String[] getSupportedEngineTypes() {
    return new String[]{ENGINE_DUCKDB, ENGINE_TRINO, ENGINE_SPARK, ENGINE_CLICKHOUSE};
  }
}
