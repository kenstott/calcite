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
package org.apache.calcite.adapter.file.statistics;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;

/**
 * Interface for tables that can provide statistics for cost-based optimization.
 * This is the key integration point between file adapter tables and the
 * Calcite Volcano planner.
 */
public interface StatisticsProvider {

  /**
   * Get comprehensive table statistics including row count, data size,
   * and column-level statistics with HLL sketches.
   *
   * @param table The table to get statistics for
   * @return Table statistics, or null if not available
   */
  TableStatistics getTableStatistics(RelOptTable table);

  /**
   * Get statistics for a specific column.
   *
   * @param table The table
   * @param columnName The column name
   * @return Column statistics, or null if not available
   */
  ColumnStatistics getColumnStatistics(RelOptTable table, String columnName);

  /**
   * Calculate selectivity estimate for a predicate.
   * This is used by the Volcano planner for cost-based optimization.
   *
   * @param table The table
   * @param predicate The filter predicate
   * @return Selectivity estimate between 0.0 and 1.0
   */
  double getSelectivity(RelOptTable table, RexNode predicate);

  /**
   * Get the estimated number of distinct values for a column.
   * Uses HyperLogLog sketches when available for high accuracy.
   *
   * @param table The table
   * @param columnName The column name
   * @return Estimated distinct count
   */
  long getDistinctCount(RelOptTable table, String columnName);

  /**
   * Check if statistics are available for this table.
   *
   * @param table The table
   * @return true if statistics are available
   */
  boolean hasStatistics(RelOptTable table);

  /**
   * Trigger asynchronous statistics generation for this table.
   * This allows the query to proceed with estimates while statistics
   * are built in the background for future queries.
   *
   * @param table The table
   */
  void scheduleStatisticsGeneration(RelOptTable table);
}
