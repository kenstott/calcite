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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.sql.SqlDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClickHouse-specific convention that ensures maximum query pushdown.
 * Extends JdbcConvention to leverage existing JDBC infrastructure
 * while registering ClickHouse-compatible optimization rules.
 *
 * <p>Registers the same parquet statistics-based optimization rules as DuckDB
 * (filter pushdown, join reorder, column pruning, count star, partition distinct)
 * since these are engine-agnostic and work with any JDBC convention.
 */
public class ClickHouseConvention extends JdbcConvention {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseConvention.class);

  public ClickHouseConvention(SqlDialect dialect, Expression expression, String name) {
    super(dialect, expression, name);
    LOGGER.debug("[CLICKHOUSE-CONVENTION] Instance created for: {}", name);
  }

  /**
   * Creates a ClickHouse convention with pushdown rules.
   */
  public static ClickHouseConvention of(SqlDialect dialect, Expression expression, String name) {
    return new ClickHouseConvention(dialect, expression, name);
  }

  @Override public void register(RelOptPlanner planner) {
    LOGGER.debug("ClickHouseConvention.register() called");

    // Register ClickHouse-specific HLL COUNT(DISTINCT) optimization rule
    // Uses pre-computed HLL sketches from the statistics cache for instant approximate counts
    if (!"false".equals(System.getProperty("calcite.file.statistics.hll.enabled", "true"))) {
      planner.addRule(ClickHouseHLLCountDistinctRule.INSTANCE);
      LOGGER.debug("Added ClickHouse HLL COUNT(DISTINCT) rule to planner");
    }

    // Register the VALUES converter rule so HLL/Iceberg results can become enumerable
    planner.addRule(org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_VALUES_RULE);

    // Register parquet statistics-based optimization rules
    // These are engine-agnostic and work with any JDBC convention

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
    // Also add ClickHouse-specific Iceberg COUNT(*) rule that works with JDBC convention
    if (!"false".equals(System.getProperty("calcite.file.statistics.count.star.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.CountStarStatisticsRule.INSTANCE);
      planner.addRule(ClickHouseIcebergCountStarRule.INSTANCE);
    }

    // 5. DISTINCT on partition columns optimization
    if (!"false".equals(System.getProperty("calcite.file.partition.distinct.enabled"))) {
      planner.addRule(org.apache.calcite.adapter.file.rules.PartitionDistinctRule.INSTANCE);
    }

    // Register all standard JDBC rules for comprehensive pushdown
    for (RelOptRule rule : JdbcRules.rules(this)) {
      planner.addRule(rule);
    }

    LOGGER.debug("Registered ClickHouse convention with HLL + Iceberg + parquet statistics optimizations + JDBC pushdown rules");
  }
}
