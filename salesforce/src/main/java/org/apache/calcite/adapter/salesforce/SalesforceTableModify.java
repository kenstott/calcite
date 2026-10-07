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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * INSERT, UPDATE or DELETE against a Salesforce sObject, executed through
 * {@link SalesforceTable#modify}.
 */
public class SalesforceTableModify extends TableModify implements EnumerableRel {

  private static final Method MODIFY =
      Types.lookupMethod(SalesforceTable.class, "modify",
          String.class, Enumerable.class, String.class);

  public SalesforceTableModify(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, Prepare.CatalogReader catalogReader, RelNode input,
      Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traitSet, table, catalogReader, input, operation,
        updateColumnList, sourceExpressionList, flattened);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SalesforceTableModify(getCluster(), traitSet, getTable(),
        getCatalogReader(), sole(inputs), getOperation(), getUpdateColumnList(),
        getSourceExpressionList(), isFlattened());
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Cheaper than EnumerableTableModify, which also matches a ModifiableTable
    // but can only write through getModifiableCollection()
    RelOptCost cost = super.computeSelfCost(planner, mq);
    return cost == null ? null : cost.multiplyBy(.1);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final Result input =
        implementor.visitChild(this, 0, (EnumerableRel) getInput(), Prefer.ARRAY);
    final Expression child = builder.append("child", input.block);
    final Expression rows =
        builder.append("rows", input.physType.convertTo(child, JavaRowFormat.ARRAY));

    final Expression table =
        requireNonNull(getTable().getExpression(SalesforceTable.class),
            "Salesforce table expression");
    final List<String> updateColumns = getUpdateColumnList();
    builder.add(
        Expressions.return_(null,
            Expressions.call(table, MODIFY,
                Expressions.constant(getOperation().name()),
                rows,
                Expressions.constant(
                    updateColumns == null ? "" : String.join(",", updateColumns)))));

    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.SCALAR);
    return implementor.result(physType, builder.toBlock());
  }
}
