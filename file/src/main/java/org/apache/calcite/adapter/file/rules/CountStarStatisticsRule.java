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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Rule that replaces COUNT(*) with a pre-computed row count estimate from table statistics.
 *
 * <p>This rule provides instant row counts for tables that have statistics with rowCount,
 * avoiding expensive full table scans or S3 file listings for partitioned tables.
 *
 * <p>The rule matches:
 * <ul>
 *   <li>Aggregate with no GROUP BY (grand total)</li>
 *   <li>Single COUNT(*) call (no arguments, not distinct)</li>
 *   <li>Table has statistics with non-null rowCount</li>
 * </ul>
 *
 * <p>Example transformation:
 * <pre>
 * SELECT COUNT(*) FROM regional_income
 * -- Before: Full scan of ~20K parquet files from S3
 * -- After: Returns VALUES(102000000) instantly
 * </pre>
 */
public class CountStarStatisticsRule extends RelRule<CountStarStatisticsRule.Config> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CountStarStatisticsRule.class);

  public static final CountStarStatisticsRule INSTANCE =
      (CountStarStatisticsRule) Config.DEFAULT.toRule();

  private CountStarStatisticsRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();

    // Only handle simple aggregates without GROUP BY (grand total)
    if (!aggregate.getGroupSet().isEmpty()) {
      LOGGER.debug("[COUNT* STATS]: Skipping - has GROUP BY");
      return;
    }

    // Check if this is a simple COUNT(*) query
    List<AggregateCall> aggCalls = aggregate.getAggCallList();
    if (aggCalls.size() != 1) {
      LOGGER.debug("[COUNT* STATS]: Skipping - {} agg calls (need exactly 1)", aggCalls.size());
      return;
    }

    AggregateCall aggCall = aggCalls.get(0);

    // Must be COUNT function
    if (aggCall.getAggregation().getKind() != SqlKind.COUNT) {
      LOGGER.debug("[COUNT* STATS]: Skipping - not COUNT function");
      return;
    }

    // Must not be DISTINCT (that's handled by HLL rule)
    if (aggCall.isDistinct()) {
      LOGGER.debug("[COUNT* STATS]: Skipping - is COUNT(DISTINCT), use HLL rule instead");
      return;
    }

    // Must be COUNT(*) with no arguments
    if (!aggCall.getArgList().isEmpty()) {
      LOGGER.debug("[COUNT* STATS]: Skipping - COUNT has arguments (not COUNT(*))");
      return;
    }

    // The statistic describes ONE table, so the input must reduce to exactly one scan with
    // nothing above it that changes the row count. Without this the rule fired on an
    // Aggregate over a Join and answered from whichever scan findTableScan reached first:
    // COUNT(*) over a self cross join of a 33,791-row table returned 33,791 instead of
    // ~1.1e9, a three-way join returned the same, and adding a join predicate changed
    // nothing. A WHERE clause was wrong the same way, since there was no Filter guard here
    // either -- the count returned was always the whole table's.
    int scans = countPlainTableScans(input);
    if (scans != 1) {
      LOGGER.debug("[COUNT* STATS]: Skipping - input reduces to {} plain table scans, not 1",
          scans);
      return;
    }

    // Find the table scan to get statistics
    TableScan tableScan = findTableScan(input);
    if (tableScan == null) {
      LOGGER.debug("[COUNT* STATS]: Skipping - no TableScan found in input");
      return;
    }

    // Get table statistics
    RelOptTable relOptTable = tableScan.getTable();
    Table table = relOptTable.unwrap(Table.class);
    if (table == null) {
      LOGGER.debug("[COUNT* STATS]: Skipping - cannot unwrap Table");
      return;
    }

    Statistic statistic = table.getStatistic();
    if (statistic == null) {
      LOGGER.debug("[COUNT* STATS]: Skipping - no statistics available");
      return;
    }

    Double rowCount = statistic.getRowCount();
    if (rowCount == null) {
      LOGGER.debug("[COUNT* STATS]: Skipping - rowCount is null");
      return;
    }

    // Get table name for logging
    List<String> qualifiedName = relOptTable.getQualifiedName();
    String tableName = qualifiedName.isEmpty() ? "unknown" : qualifiedName.get(qualifiedName.size() - 1);

    LOGGER.info("[COUNT* STATS]: Replacing COUNT(*) on '{}' with estimated rowCount: {}",
        tableName, rowCount.longValue());

    // Create VALUES node with the row count
    RelNode valuesNode = createCountStarValues(aggregate, rowCount.longValue());
    if (valuesNode != null) {
      call.transformTo(valuesNode, com.google.common.collect.ImmutableMap.of());
    }
  }

  /** Sentinel: the input is not a plain single-table scan, so a stored count cannot apply. */
  private static final int NOT_SIMPLE = -1;

  /**
   * Number of {@link TableScan}s under {@code node}, or {@link #NOT_SIMPLE} when the tree
   * holds anything that makes the row count differ from a single table's.
   *
   * <p>A whitelist on purpose. For a rewrite that swaps a stored number in for a real scan,
   * a blacklist is the wrong shape: every operator nobody thought of defaults to "safe" and
   * silently returns a wrong count. Only nodes that cannot change cardinality pass through,
   * so anything unfamiliar stops the rule rather than corrupting the answer. Notably absent
   * are Join and Union (multiply or add rows), Filter (removes them), Sort (a fetch/offset
   * truncates) and a nested Aggregate (collapses them).
   */
  private int countPlainTableScans(RelNode node) {
    if (node == null) {
      return NOT_SIMPLE;
    }
    if (node instanceof TableScan) {
      return 1;
    }
    // Volcano wrapper — inspect the chosen (or original) plan, as findTableScan does.
    if (node.getClass().getName().contains("RelSubset")) {
      try {
        java.lang.reflect.Method getBest = node.getClass().getMethod("getBest");
        RelNode best = (RelNode) getBest.invoke(node);
        if (best != null && best != node) {
          return countPlainTableScans(best);
        }
        java.lang.reflect.Method getOriginal = node.getClass().getMethod("getOriginal");
        RelNode original = (RelNode) getOriginal.invoke(node);
        if (original != null && original != node) {
          return countPlainTableScans(original);
        }
      } catch (Exception e) {
        return NOT_SIMPLE;
      }
      return NOT_SIMPLE;
    }
    if (node instanceof org.apache.calcite.rel.core.Project
        || node instanceof org.apache.calcite.rel.core.Calc) {
      int total = 0;
      for (RelNode child : node.getInputs()) {
        int n = countPlainTableScans(child);
        if (n == NOT_SIMPLE) {
          return NOT_SIMPLE;
        }
        total += n;
      }
      return total;
    }
    return NOT_SIMPLE;
  }

  /**
   * Find the TableScan in the input tree, handling RelSubset nodes from Volcano planner.
   */
  private TableScan findTableScan(RelNode node) {
    if (node == null) {
      return null;
    }

    // Handle RelSubset nodes from Volcano planner
    if (node.getClass().getName().contains("RelSubset")) {
      try {
        // Try to get the best or original node from the subset
        java.lang.reflect.Method getBest = node.getClass().getMethod("getBest");
        RelNode best = (RelNode) getBest.invoke(node);
        if (best != null && best != node) {
          return findTableScan(best);
        }

        // Try getOriginal if getBest didn't work
        java.lang.reflect.Method getOriginal = node.getClass().getMethod("getOriginal");
        RelNode original = (RelNode) getOriginal.invoke(node);
        if (original != null && original != node) {
          return findTableScan(original);
        }
      } catch (Exception e) {
        // Silently continue
      }
    }

    if (node instanceof TableScan) {
      return (TableScan) node;
    }

    // Recursively search through all inputs
    for (RelNode input : node.getInputs()) {
      TableScan scan = findTableScan(input);
      if (scan != null) {
        return scan;
      }
    }

    return null;
  }

  /**
   * Create a VALUES node containing the row count estimate.
   */
  private RelNode createCountStarValues(Aggregate aggregate, long rowCount) {
    RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

    // Build row type matching the aggregate output
    AggregateCall aggCall = aggregate.getAggCallList().get(0);
    String fieldName = aggCall.getName() != null ? aggCall.getName() : "EXPR$0";

    RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    RelDataType rowType = typeFactory.builder()
        .add(fieldName, bigIntType)
        .build();

    // Create literal with row count
    RexLiteral literal = (RexLiteral) rexBuilder.makeLiteral(rowCount, bigIntType, true);

    // Create VALUES node with single row
    ImmutableList<ImmutableList<RexLiteral>> tuples =
        ImmutableList.of(ImmutableList.of(literal));

    // Check if we need EnumerableValues for the enumerable convention
    if (aggregate.getTraitSet().contains(EnumerableConvention.INSTANCE)) {
      return EnumerableValues.create(aggregate.getCluster(), rowType, tuples);
    } else {
      return LogicalValues.create(aggregate.getCluster(), rowType, tuples);
    }
  }

  /** Configuration for CountStarStatisticsRule. */
  public static class Config implements RelRule.Config {
    private final OperandTransform operandSupplier;

    private Config() {
      // Match any aggregate with any input - let onMatch handle the details
      this.operandSupplier = b0 ->
          b0.operand(Aggregate.class).anyInputs();
    }

    public static final Config DEFAULT = new Config();

    @Override public RelRule.Config withOperandSupplier(OperandTransform transform) {
      return this;
    }

    @Override public RelRule.Config withDescription(String description) {
      return this;
    }

    @Override public RelRule.Config withRelBuilderFactory(org.apache.calcite.tools.RelBuilderFactory factory) {
      return this;
    }

    @Override public RelOptRule toRule() {
      return new CountStarStatisticsRule(this);
    }

    @Override public String description() {
      return "CountStarStatisticsRule";
    }

    @Override public OperandTransform operandSupplier() {
      return operandSupplier;
    }
  }
}
