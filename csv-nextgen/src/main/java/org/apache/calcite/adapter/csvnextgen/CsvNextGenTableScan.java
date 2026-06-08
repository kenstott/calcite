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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Relational expression representing a scan of a CSV NextGen table.
 *
 * <p>This table scan can be optimized by the Calcite planner and supports
 * different execution engines based on the table configuration.
 */
public class CsvNextGenTableScan extends TableScan implements EnumerableRel {
  final CsvNextGenTable csvTable;

  protected CsvNextGenTableScan(RelOptCluster cluster, RelOptTable table,
      CsvNextGenTable csvTable) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
    this.csvTable = csvTable;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new CsvNextGenTableScan(getCluster(), table, csvTable);
  }

  @Override public RelDataType deriveRowType() {
    return csvTable.getRowType(getCluster().getTypeFactory());
  }

  @Override public void register(RelOptPlanner planner) {
    // Register CSV NextGen specific rules here if needed
    // For now, we rely on the default enumerable conventions
  }

  /**
   * Gets the underlying CSV table.
   */
  public CsvNextGenTable getCsvTable() {
    return csvTable;
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(),
        getRowType(),
        pref.preferArray());

    return implementor.result(
        physType,
        Blocks.toBlock(
            Expressions.call(
                Expressions.new_(
                    csvTable.getClass(),
                    Expressions.constant(csvTable.getSource()),
                    Expressions.constant(csvTable.hasHeader()),
                    Expressions.constant(csvTable.getExecutionEngine()),
                    Expressions.constant(csvTable.getBatchSize())),
                "scan",
                implementor.getRootExpression())));
  }
}
