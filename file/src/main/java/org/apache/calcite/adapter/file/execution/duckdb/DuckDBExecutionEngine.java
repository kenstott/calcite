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
package org.apache.calcite.adapter.file.execution.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DuckDB-based execution engine for high-performance analytical processing.
 *
 * <p>This engine leverages DuckDB's high-performance columnar query engine
 * for analytical workloads on Parquet files.
 *
 * <p>Key advantages:
 * <ul>
 *   <li>10-100x performance improvements for complex analytics</li>
 *   <li>Native support for complex aggregations, joins, and window functions</li>
 *   <li>Automatic vectorization and parallelization</li>
 *   <li>Efficient handling of compressed Parquet data</li>
 *   <li>Fallback to Parquet engine when DuckDB unavailable</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * {
 *   "schemas": [{
 *     "name": "analytics",
 *     "operand": {
 *       "directory": "/data",
 *       "executionEngine": "DUCKDB",
 *       "duckdbConfig": {
 *         "memory_limit": "4GB",
 *         "threads": 4
 *       }
 *     }
 *   }]
 * }
 * }</pre>
 */
public final class DuckDBExecutionEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBExecutionEngine.class);

  /**
   * Check if DuckDB JDBC driver is available on the classpath.
   *
   * @return true if DuckDB is available, false otherwise
   */
  public static boolean isAvailable() {
    try {
      Class.forName("org.duckdb.DuckDBDriver");
      return true;
    } catch (ClassNotFoundException e) {
      LOGGER.debug("DuckDB JDBC driver not found on classpath: {}", e.getMessage());
      return false;
    } catch (Exception e) {
      LOGGER.debug("Error checking DuckDB availability: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Get the engine type identifier.
   *
   * @return "DUCKDB"
   */
  public static String getEngineType() {
    return "DUCKDB";
  }

  private DuckDBExecutionEngine() {
    // Utility class - all methods are static
  }
}
