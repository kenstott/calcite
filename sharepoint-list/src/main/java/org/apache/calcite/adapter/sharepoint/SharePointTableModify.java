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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
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
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * UPDATE or DELETE of a SharePoint list, executed through {@link SharePointListTable#modify}.
 *
 * <p>INSERT goes through the table's modifiable collection. {@code EnumerableTableModify},
 * which writes through that collection, has no implementation of UPDATE, and its DELETE calls
 * {@code Collection.removeAll}, which removes rows the collection holds: the rows of a list
 * are in SharePoint, not in the collection, so nothing was deleted.
 */
public class SharePointTableModify extends TableModify implements EnumerableRel {

  private static final Method MODIFY =
      Types.lookupMethod(SharePointListTable.class, "modify",
          String.class, Enumerable.class, int.class, String.class);

  public SharePointTableModify(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, Prepare.CatalogReader catalogReader, RelNode input,
      Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traitSet, table, catalogReader, input, operation,
        updateColumnList, sourceExpressionList, flattened);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SharePointTableModify(getCluster(), traitSet, getTable(),
        getCatalogReader(), sole(inputs), getOperation(), getUpdateColumnList(),
        getSourceExpressionList(), isFlattened());
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Cheaper than EnumerableTableModify, which also matches a ModifiableTable
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

    // Asked for as a SharePointListTable, a scannable table, the expression would be its scan
    final Expression table =
        Expressions.convert_(
            requireNonNull(getTable().getExpression(ModifiableTable.class),
                "SharePoint table expression"),
            SharePointListTable.class);
    final List<String> updateColumns = getUpdateColumnList();
    // The input row of an UPDATE is the table's row followed by the new value of each updated
    // column; the input row of a DELETE is the table's row
    builder.add(
        Expressions.return_(null,
            Expressions.call(table, MODIFY,
                Expressions.constant(getOperation().name()),
                rows,
                Expressions.constant(getTable().getRowType().getFieldCount()),
                Expressions.constant(
                    updateColumns == null ? "" : String.join(",", updateColumns)))));

    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.SCALAR);
    return implementor.result(physType, builder.toBlock());
  }

  /**
   * Rule that converts an UPDATE or DELETE of a {@link SharePointListTable} into a
   * {@link SharePointTableModify}.
   */
  static class Rule extends ConverterRule {
    static final Rule INSTANCE = Config.INSTANCE
        .withConversion(TableModify.class, Convention.NONE,
            EnumerableConvention.INSTANCE, "SharePointTableModifyRule")
        .withRuleFactory(Rule::new)
        .toRule(Rule.class);

    protected Rule(Config config) {
      super(config);
    }

    @Override public @Nullable RelNode convert(RelNode rel) {
      final TableModify modify = (TableModify) rel;
      if (!modify.isUpdate() && !modify.isDelete()
          || modify.getTable().unwrap(SharePointListTable.class) == null) {
        return null;
      }
      final RelTraitSet traitSet =
          modify.getTraitSet().replace(EnumerableConvention.INSTANCE);
      return new SharePointTableModify(modify.getCluster(), traitSet,
          modify.getTable(), modify.getCatalogReader(),
          convert(modify.getInput(), traitSet), modify.getOperation(),
          modify.getUpdateColumnList(),
          modify.getSourceExpressionList(), modify.isFlattened());
    }
  }
}
