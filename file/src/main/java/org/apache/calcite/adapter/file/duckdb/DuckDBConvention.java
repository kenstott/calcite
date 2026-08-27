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

    // Register all standard JDBC rules for comprehensive pushdown
    // These will only apply to queries that weren't optimized by statistics-based rules
    for (RelOptRule rule : JdbcRules.rules(this)) {
      planner.addRule(rule);
    }

    // Stock JdbcProjectRule/JdbcFilterRule (just registered above) unconditionally refuse to
    // push down a Project/Filter containing ANY user-defined-function call, without consulting
    // the dialect -- so this project's own DuckDB-pushdown stub UDFs (JSON_EXTRACT,
    // STRING_SPLIT, ...; see DuckDBFunctionMapping) could never reach DuckDB even though
    // DuckDBFunctionMapping already knows how to render them. These two rules generalize that
    // block to allow recognized stub UDFs through while still blocking any other unrecognized
    // one -- added alongside, not instead of, the stock rules (see their class comments).
    planner.addRule(DuckDBProjectRule.create(this));
    planner.addRule(DuckDBFilterRule.create(this));

    LOGGER.debug("Registered DuckDB convention with HLL + parquet statistics optimizations + comprehensive JDBC pushdown rules");
  }
}
