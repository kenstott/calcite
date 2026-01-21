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
package org.apache.calcite.adapter.file.refresh;

/**
 * Extended refresh listener that supports pattern-based refresh notifications.
 * Used by DuckDB+Hive optimization where the pattern is sufficient for view recreation
 * without requiring explicit file lists.
 */
public interface PatternAwareRefreshListener extends TableRefreshListener {
  /**
   * Called when a table has been refreshed with a file pattern.
   * This is used for DuckDB views where the pattern can be used directly
   * in the DDL (e.g., parquet_scan('s3://bucket/table/**\/*.parquet')).
   *
   * @param tableName the name of the table that was refreshed
   * @param pattern the file pattern (glob) for the table
   */
  void onTableRefreshedWithPattern(String tableName, String pattern);

  /**
   * Called when an Iceberg table has been refreshed or recreated.
   * This is used to update DuckDB views that use iceberg_scan() when
   * the underlying Iceberg table schema has changed.
   *
   * @param tableName the name of the table that was refreshed
   * @param tableLocation the Iceberg table location (e.g., s3://bucket/warehouse/table)
   */
  default void onIcebergTableRefreshed(String tableName, String tableLocation) {
    // Default no-op for backward compatibility
  }
}
