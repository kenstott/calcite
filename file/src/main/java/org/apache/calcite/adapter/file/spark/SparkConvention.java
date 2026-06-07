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
package org.apache.calcite.adapter.file.spark;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.sql.SqlDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark SQL-specific convention that ensures maximum query pushdown.
 * Extends JdbcConvention to leverage existing JDBC infrastructure
 * while registering Spark-compatible optimization rules.
 *
 * <p>Registers the same parquet statistics-based optimization rules as DuckDB/ClickHouse
 * (filter pushdown, join reorder, column pruning, count star, partition distinct)
 * since these are engine-agnostic and work with any JDBC convention.
 */
public class SparkConvention extends JdbcConvention {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkConvention.class);

  public SparkConvention(SqlDialect dialect, Expression expression, String name) {
    super(dialect, expression, name);
    LOGGER.debug("[SPARK-CONVENTION] Instance created for: {}", name);
  }

  /**
   * Creates a Spark convention with pushdown rules.
   */
  public static SparkConvention of(SqlDialect dialect, Expression expression, String name) {
    return new SparkConvention(dialect, expression, name);
  }

  @Override public void register(RelOptPlanner planner) {
    LOGGER.debug("SparkConvention.register() called");

    // Register the VALUES converter rule so statistics results can become enumerable
    planner.addRule(org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_VALUES_RULE);

    // Register parquet statistics-based optimization rules (engine-agnostic)

    // 1. Filter pushdown based on parquet min/max statistics
    if (!"false".equals(System.getProperty("calcite.file.statistics.filter.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.SimpleFileFilterPushdownRule.INSTANCE);
    }

    // 2. Join reordering based on table size statistics
    if (!"false".equals(System.getProperty("calcite.file.statistics.join.reorder.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.SimpleFileJoinReorderRule.INSTANCE);
    }

    // 3. Column pruning to reduce I/O based on column statistics
    if (!"false".equals(System.getProperty("calcite.file.statistics.column.pruning.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.SimpleFileColumnPruningRule.INSTANCE);
    }

    // 4. COUNT(*) optimization using table statistics for instant row count
    if (!"false".equals(System.getProperty("calcite.file.statistics.count.star.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.CountStarStatisticsRule.INSTANCE);
    }

    // 5. DISTINCT on partition columns optimization
    if (!"false".equals(System.getProperty("calcite.file.partition.distinct.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.PartitionDistinctRule.INSTANCE);
    }

    // Register all standard JDBC rules for comprehensive pushdown
    for (RelOptRule rule : JdbcRules.rules(this)) {
      planner.addRule(rule);
    }

    LOGGER.debug("Registered Spark convention with parquet statistics optimizations + JDBC pushdown rules");
  }
}
