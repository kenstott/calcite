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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.adapter.file.table.PartitionedParquetTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that rewrites SELECT DISTINCT on partition columns to use
 * directory listing instead of scanning parquet files.
 *
 * <p>For hive-partitioned parquet tables, partition column values are encoded
 * in directory names (e.g., year=2020/geo=STATE/). This rule detects when a
 * query only needs distinct partition column values and retrieves them via
 * fast directory listing instead of expensive file scans.
 *
 * <p>Example:
 * <pre>
 * -- Original query (would scan all parquet files)
 * SELECT DISTINCT year FROM regional_income
 *
 * -- Optimized (uses directory listing)
 * VALUES (2020), (2021), (2022), (2023), (2024)
 * </pre>
 */
public class PartitionDistinctRule extends RelRule<PartitionDistinctRule.Config> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionDistinctRule.class);

  public static final PartitionDistinctRule INSTANCE =
      (PartitionDistinctRule) Config.DEFAULT.toRule();

  private PartitionDistinctRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();

    // Must have GROUP BY (which is how DISTINCT is represented)
    ImmutableBitSet groupSet = aggregate.getGroupSet();
    if (groupSet.isEmpty()) {
      return;
    }

    // Must not have any aggregate functions (pure DISTINCT/GROUP BY)
    if (!aggregate.getAggCallList().isEmpty()) {
      return;
    }

    // Find the underlying TableScan
    TableScan tableScan = findTableScan(input);
    if (tableScan == null) {
      return;
    }

    // Must be a PartitionedParquetTable
    RelOptTable relOptTable = tableScan.getTable();
    PartitionedParquetTable table = relOptTable.unwrap(PartitionedParquetTable.class);
    if (table == null) {
      return;
    }

    // Get partition columns
    List<String> partitionColumns = table.getPartitionColumns();
    if (partitionColumns == null || partitionColumns.isEmpty()) {
      return;
    }

    // Check if all GROUP BY columns are partition columns
    RelDataType inputRowType = input.getRowType();
    List<String> groupByColumns = new ArrayList<>();

    for (int idx : groupSet) {
      RelDataTypeField field = inputRowType.getFieldList().get(idx);
      String columnName = field.getName();
      if (!table.isPartitionColumn(columnName)) {
        LOGGER.debug("Column '{}' is not a partition column, cannot optimize", columnName);
        return;
      }
      groupByColumns.add(columnName);
    }

    LOGGER.info("[PartitionDistinct] Optimizing DISTINCT on partition columns {} for table '{}'",
        groupByColumns, relOptTable.getQualifiedName());

    // Get distinct values for each partition column via directory listing
    List<List<String>> columnValues = new ArrayList<>();
    for (String col : groupByColumns) {
      List<String> values = table.getDistinctPartitionValues(col);
      if (values.isEmpty()) {
        LOGGER.debug("No partition values found for column '{}', cannot optimize", col);
        return;
      }
      columnValues.add(values);
      LOGGER.debug("Found {} distinct values for partition column '{}'", values.size(), col);
    }

    // Build cartesian product of all column values
    List<List<String>> rows = buildCartesianProduct(columnValues);

    // Create VALUES node
    RelNode valuesNode = createValuesNode(aggregate, groupByColumns, rows);
    if (valuesNode != null) {
      LOGGER.info("[PartitionDistinct] Successfully replaced DISTINCT with {} pre-computed rows",
          rows.size());
      call.transformTo(valuesNode, com.google.common.collect.ImmutableMap.of());
    }
  }

  /**
   * Find the TableScan in the plan tree.
   */
  private TableScan findTableScan(RelNode node) {
    if (node == null) {
      return null;
    }

    // Handle RelSubset nodes from Volcano planner
    if (node.getClass().getName().contains("RelSubset")) {
      try {
        java.lang.reflect.Method getBest = node.getClass().getMethod("getBest");
        RelNode best = (RelNode) getBest.invoke(node);
        if (best != null && best != node) {
          return findTableScan(best);
        }

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
   * Build cartesian product of column values.
   */
  private List<List<String>> buildCartesianProduct(List<List<String>> columnValues) {
    List<List<String>> result = new ArrayList<>();
    buildCartesianProductRecursive(columnValues, 0, new ArrayList<>(), result);
    return result;
  }

  private void buildCartesianProductRecursive(List<List<String>> columnValues, int depth,
      List<String> current, List<List<String>> result) {
    if (depth == columnValues.size()) {
      result.add(new ArrayList<>(current));
      return;
    }

    for (String value : columnValues.get(depth)) {
      current.add(value);
      buildCartesianProductRecursive(columnValues, depth + 1, current, result);
      current.remove(current.size() - 1);
    }
  }

  /**
   * Create a VALUES node containing the partition values.
   */
  private RelNode createValuesNode(Aggregate aggregate, List<String> columnNames,
      List<List<String>> rows) {
    RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = aggregate.getCluster().getTypeFactory();

    // Use the aggregate's row type
    RelDataType rowType = aggregate.getRowType();

    // Build tuples
    List<ImmutableList<RexLiteral>> tuples = new ArrayList<>();
    for (List<String> row : rows) {
      List<RexLiteral> tuple = new ArrayList<>();
      for (int i = 0; i < row.size(); i++) {
        String value = row.get(i);
        RelDataType fieldType = rowType.getFieldList().get(i).getType();

        // Convert value to appropriate type
        RexLiteral literal = createLiteral(rexBuilder, typeFactory, value, fieldType);
        tuple.add(literal);
      }
      tuples.add(ImmutableList.copyOf(tuple));
    }

    // Create VALUES node
    if (aggregate.getTraitSet().contains(EnumerableConvention.INSTANCE)) {
      return EnumerableValues.create(
          aggregate.getCluster(),
          rowType,
          ImmutableList.copyOf(tuples));
    } else {
      return LogicalValues.create(
          aggregate.getCluster(),
          rowType,
          ImmutableList.copyOf(tuples));
    }
  }

  /**
   * Create a literal value of the appropriate type.
   */
  private RexLiteral createLiteral(RexBuilder rexBuilder, RelDataTypeFactory typeFactory,
      String value, RelDataType targetType) {
    SqlTypeName typeName = targetType.getSqlTypeName();

    switch (typeName) {
    case INTEGER:
      return (RexLiteral) rexBuilder.makeLiteral(Integer.parseInt(value), targetType, true);
    case BIGINT:
      return (RexLiteral) rexBuilder.makeLiteral(Long.parseLong(value), targetType, true);
    case VARCHAR:
    case CHAR:
    default:
      // Default to string
      RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      return (RexLiteral) rexBuilder.makeLiteral(value, varcharType, true);
    }
  }

  /** Configuration for PartitionDistinctRule. */
  public static class Config implements RelRule.Config {
    private final OperandTransform operandSupplier;

    private Config() {
      // Match Aggregate with any input
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

    @Override public RelRule.Config withRelBuilderFactory(
        org.apache.calcite.tools.RelBuilderFactory factory) {
      return this;
    }

    @Override public RelOptRule toRule() {
      return new PartitionDistinctRule(this);
    }

    @Override public String description() {
      return "PartitionDistinctRule";
    }

    @Override public OperandTransform operandSupplier() {
      return operandSupplier;
    }
  }
}
