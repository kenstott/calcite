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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableIntList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Pushes an ORDER BY and/or a literal OFFSET/FETCH that sits directly on a cloud-ops table
 * scan into the table, as a {@link CloudOpsSortedScan}.
 *
 * <p>Does not fire when a filter lies between the sort and the scan, or when the scan
 * already carries filters: the tables do not evaluate every filter themselves, and a row
 * limit taken before a filter the planner still has to apply would drop rows.
 */
public class CloudOpsSortScanRule extends RelRule<CloudOpsSortScanRule.Config> {
  public static final CloudOpsSortScanRule INSTANCE = Config.DEFAULT.toRule();

  protected CloudOpsSortScanRule(Config config) {
    super(config);
  }

  private static boolean isCloudOpsScan(TableScan scan) {
    final RelOptTable table = scan.getTable();
    return table != null && table.unwrap(AbstractCloudOpsTable.class) != null;
  }

  private static boolean literalOrAbsent(@Nullable RexNode node) {
    return node == null || node instanceof RexLiteral;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalSort sort = call.rel(0);
    final TableScan scan = call.rel(1);
    if (!literalOrAbsent(sort.offset) || !literalOrAbsent(sort.fetch)) {
      return; // OFFSET ? / FETCH ? are bound at run time; leave them to the engine
    }

    final RelOptTable table = scan.getTable();
    final int columnCount = table.getRowType().getFieldCount();
    final ImmutableIntList projects;
    if (scan instanceof Bindables.BindableTableScan) {
      final Bindables.BindableTableScan bindable = (Bindables.BindableTableScan) scan;
      if (!bindable.filters.isEmpty()) {
        return;
      }
      projects = bindable.projects;
    } else if (scan.getRowType().getFieldCount() == columnCount) {
      projects = ImmutableIntList.identity(columnCount);
    } else {
      return; // some other scan that changed the columns; its mapping is not known here
    }

    // The sort refers to the scan's output columns; the table is told its own ordinals
    final List<RelFieldCollation> tableFields = new ArrayList<>();
    for (RelFieldCollation field : sort.getCollation().getFieldCollations()) {
      tableFields.add(field.withFieldIndex(projects.get(field.getFieldIndex())));
    }
    final RelCollation tableCollation = RelCollations.of(tableFields);

    call.transformTo(
        new CloudOpsSortedScan(sort.getCluster(),
            sort.getTraitSet().replace(EnumerableConvention.INSTANCE), table,
            table.unwrap(AbstractCloudOpsTable.class), projects, tableCollation,
            sort.offset, sort.fetch));
  }

  /** Rule configuration. Written out by hand: this module does not run the Immutables
   * annotation processor. */
  public static class Config implements RelRule.Config {
    public static final Config DEFAULT =
        new Config(RelFactories.LOGICAL_BUILDER, null, b0 ->
            b0.operand(LogicalSort.class).oneInput(b1 ->
                b1.operand(TableScan.class)
                    .predicate(CloudOpsSortScanRule::isCloudOpsScan).noInputs()));

    private final RelBuilderFactory relBuilderFactory;
    private final @Nullable String description;
    private final OperandTransform operandSupplier;

    private Config(RelBuilderFactory relBuilderFactory, @Nullable String description,
        OperandTransform operandSupplier) {
      this.relBuilderFactory = relBuilderFactory;
      this.description = description;
      this.operandSupplier = operandSupplier;
    }

    @Override public CloudOpsSortScanRule toRule() {
      return new CloudOpsSortScanRule(this);
    }

    @Override public RelBuilderFactory relBuilderFactory() {
      return relBuilderFactory;
    }

    @Override public Config withRelBuilderFactory(RelBuilderFactory factory) {
      return new Config(factory, description, operandSupplier);
    }

    @Override public @Nullable String description() {
      return description;
    }

    @Override public Config withDescription(@Nullable String description) {
      return new Config(relBuilderFactory, description, operandSupplier);
    }

    @Override public OperandTransform operandSupplier() {
      return operandSupplier;
    }

    @Override public Config withOperandSupplier(OperandTransform transform) {
      return new Config(relBuilderFactory, description, transform);
    }
  }
}
