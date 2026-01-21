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

/**
 * JDBC dialect abstraction for pluggable query engines.
 *
 * <p>This package provides a common abstraction layer for JDBC-based analytical
 * query engines. It allows the file adapter to delegate query execution to
 * different engines (DuckDB, Trino, Spark SQL, ClickHouse) while maintaining
 * a consistent programming model.
 *
 * <h2>Key Components</h2>
 *
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.jdbc.JdbcDialect} - Interface
 *       defining the contract for JDBC dialect implementations</li>
 *   <li>{@link org.apache.calcite.adapter.file.jdbc.JdbcDialectFactory} - Factory
 *       for creating dialect instances based on engine type</li>
 *   <li>{@link org.apache.calcite.adapter.file.jdbc.DuckDBDialect} - DuckDB
 *       implementation (embedded, direct file access)</li>
 *   <li>{@link org.apache.calcite.adapter.file.jdbc.TrinoDialect} - Trino
 *       implementation (distributed, catalog-based)</li>
 *   <li>{@link org.apache.calcite.adapter.file.jdbc.SparkSqlDialect} - Spark SQL
 *       implementation (distributed, backtick syntax)</li>
 *   <li>{@link org.apache.calcite.adapter.file.jdbc.ClickHouseDialect} - ClickHouse
 *       implementation (OLAP, s3() function)</li>
 * </ul>
 *
 * <h2>Engine Comparison</h2>
 *
 * <table border="1">
 *   <caption>JDBC Engine Comparison</caption>
 *   <tr>
 *     <th>Feature</th>
 *     <th>DuckDB</th>
 *     <th>Trino</th>
 *     <th>Spark SQL</th>
 *     <th>ClickHouse</th>
 *   </tr>
 *   <tr>
 *     <td>Deployment</td>
 *     <td>Embedded</td>
 *     <td>Distributed</td>
 *     <td>Distributed</td>
 *     <td>Embedded/Distributed</td>
 *   </tr>
 *   <tr>
 *     <td>Direct Glob Access</td>
 *     <td>Yes</td>
 *     <td>No</td>
 *     <td>Yes</td>
 *     <td>Yes</td>
 *   </tr>
 *   <tr>
 *     <td>Iceberg Support</td>
 *     <td>Yes</td>
 *     <td>Via Catalog</td>
 *     <td>Via Catalog</td>
 *     <td>Yes</td>
 *   </tr>
 *   <tr>
 *     <td>Best For</td>
 *     <td>Single-node, &lt;100GB</td>
 *     <td>Federated queries</td>
 *     <td>Existing Spark</td>
 *     <td>OLAP, time-series</td>
 *   </tr>
 * </table>
 *
 * <h2>Usage Example</h2>
 *
 * <pre>{@code
 * // Create a dialect for the desired engine
 * JdbcDialect dialect = JdbcDialectFactory.createDialect("duckdb");
 *
 * // Generate SQL for reading Parquet files
 * String sql = dialect.readParquetSql(
 *     "s3://bucket/data/*.parquet",
 *     Arrays.asList("id", "name", "amount"));
 *
 * // Build JDBC URL for connection
 * Map<String, String> config = new HashMap<>();
 * config.put("path", "/tmp/analytics.duckdb");
 * String jdbcUrl = dialect.buildJdbcUrl(config);
 *
 * // Connect and execute
 * Class.forName(dialect.getDriverClassName());
 * Connection conn = DriverManager.getConnection(jdbcUrl);
 * ResultSet rs = conn.createStatement().executeQuery(sql);
 * }</pre>
 *
 * @see org.apache.calcite.adapter.file.jdbc.JdbcDialect
 * @see org.apache.calcite.adapter.file.jdbc.JdbcDialectFactory
 */
package org.apache.calcite.adapter.file.jdbc;
