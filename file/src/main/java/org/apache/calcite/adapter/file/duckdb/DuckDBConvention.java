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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.sql.SqlDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DuckDB-specific convention that ensures maximum query pushdown.
 * Extends JdbcConvention to leverage existing JDBC infrastructure
 * while customizing for DuckDB's capabilities.
 */
public class DuckDBConvention extends JdbcConvention {
  private static final Logger LOGGER = LoggerFactory.getLogger(DuckDBConvention.class);

  static {
    LOGGER.debug("[DUCKDB-CONVENTION] Class loaded");
  }

  public DuckDBConvention(SqlDialect dialect, Expression expression, String name) {
    super(dialect, expression, name);
    LOGGER.debug("[DUCKDB-CONVENTION] Instance created for: {}", name);
  }

  /**
   * Creates a DuckDB convention with aggressive pushdown rules.
   */
  public static DuckDBConvention of(SqlDialect dialect, Expression expression, String name) {
    return new DuckDBConvention(dialect, expression, name);
  }

  @Override public void register(RelOptPlanner planner) {
    LOGGER.debug("register() called");

    // CRITICAL: Register HLL optimization rules FIRST before JDBC pushdown
    // This allows COUNT(DISTINCT) to be optimized with HLL sketches before
    // being pushed down to DuckDB as raw SQL
    // Safe to leave on: the rule answers only APPROX_COUNT_DISTINCT — calls Calcite marks
    // AggregateCall.isApproximate() — so it is a fast path for a query that asked for an estimate,
    // never a silent downgrade of an exact COUNT(DISTINCT). Turn it off with
    // -Dcalcite.file.statistics.hll.enabled=false to force even approximate counts to scan.
    String hllEnabled = System.getProperty("calcite.file.statistics.hll.enabled");
    LOGGER.debug("HLL property value: '{}'", hllEnabled);

    if (!"false".equals(System.getProperty("calcite.file.statistics.hll.enabled", "true"))) {
      // Use the DuckDB-specific HLL rule that handles both JDBC and file adapter patterns
      planner.addRule(DuckDBHLLCountDistinctRule.INSTANCE);

      LOGGER.debug("Added HLL rule to planner");
    }

    // Also register the VALUES converter rule so HLL results can become enumerable
    planner.addRule(org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_VALUES_RULE);

    // Register parquet statistics-based optimization rules for DuckDB engine
    // These provide the same optimizations available to the parquet engine

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
    // This avoids expensive S3 file listing for hive-partitioned tables
    if (!"false".equals(System.getProperty("calcite.file.statistics.count.star.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.CountStarStatisticsRule.INSTANCE);
      // Also add DuckDB-specific Iceberg COUNT(*) rule that works with JDBC convention
      planner.addRule(DuckDBIcebergCountStarRule.INSTANCE);
    }

    // 5. DISTINCT on partition columns optimization using directory listing
    // This avoids scanning parquet files to get distinct partition values
    if (!"false".equals(System.getProperty("calcite.file.partition.distinct.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.PartitionDistinctRule.INSTANCE);
    }

    // Register all standard JDBC rules for comprehensive pushdown, EXCEPT the stock
    // JdbcProjectRule/JdbcFilterRule: DuckDBProjectRule/DuckDBFilterRule below fully
    // supersede them (same behavior for every Project/Filter the stock rules already
    // allowed -- DuckDB's dialect returns true from supportsWindowFunctions(), the only
    // other condition the stock rules check -- plus two DuckDB-specific corrections the
    // stock rules can't express: allowing a recognized pushdown stub UDF through, and
    // declining an unbindable correlate-array shape). Registering both the stock and the
    // DuckDB rule for the same RelNode would leave the stock rule's unguarded conversion
    // in the search space regardless of which one blocks it, so the stock rule must be
    // excluded here, not merely outcompeted.
    for (RelOptRule rule : JdbcRules.rules(this)) {
      if (rule instanceof JdbcRules.JdbcProjectRule || rule instanceof JdbcRules.JdbcFilterRule) {
        continue;
      }
      planner.addRule(rule);
    }

    // See DuckDBProjectRule/DuckDBFilterRule class comments for what each generalizes and
    // restricts relative to the stock rules excluded above.
    planner.addRule(DuckDBProjectRule.create(this));
    planner.addRule(DuckDBFilterRule.create(this));

    LOGGER.debug("Registered DuckDB convention with HLL + parquet statistics optimizations + comprehensive JDBC pushdown rules");
  }
}
