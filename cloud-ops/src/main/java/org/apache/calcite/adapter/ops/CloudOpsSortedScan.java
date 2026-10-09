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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Scan of a cloud-ops table that hands the table an ORDER BY and/or OFFSET/FETCH together
 * with the projected columns, by calling
 * {@link AbstractCloudOpsTable#scan(DataContext, List, int[], RelCollation, RexNode, RexNode)}.
 *
 * <p>Deliberately not a {@link org.apache.calcite.rel.core.TableScan}: the planner's
 * filter-into-scan and project-into-scan rules rebuild any table scan they match as a plain
 * scan, which would drop the sort and the row limit.
 */
public class CloudOpsSortedScan extends AbstractRelNode implements EnumerableRel {
  private final RelOptTable table;
  private final AbstractCloudOpsTable cloudOpsTable;
  /** Table ordinals of the columns this scan returns, in output order. */
  private final ImmutableIntList projects;
  /** Sort order, in table ordinals. */
  private final RelCollation tableCollation;
  private final @Nullable RexNode offset;
  private final @Nullable RexNode fetch;

  CloudOpsSortedScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
      AbstractCloudOpsTable cloudOpsTable, ImmutableIntList projects,
      RelCollation tableCollation, @Nullable RexNode offset, @Nullable RexNode fetch) {
    super(cluster, traitSet);
    assert getConvention() instanceof EnumerableConvention;
    this.table = table;
    this.cloudOpsTable = cloudOpsTable;
    this.projects = projects;
    this.tableCollation = tableCollation;
    this.offset = offset;
    this.fetch = fetch;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new CloudOpsSortedScan(getCluster(), traitSet, table, cloudOpsTable, projects,
        tableCollation, offset, fetch);
  }

  @Override public @Nullable RelOptTable getTable() {
    return table;
  }

  @Override protected RelDataType deriveRowType() {
    final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
    final List<RelDataTypeField> fields = table.getRowType().getFieldList();
    for (int project : projects) {
      builder.add(fields.get(project));
    }
    return builder.build();
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("table", table.getQualifiedName())
        .item("projects", projects)
        .itemIf("sort", tableCollation, !tableCollation.getFieldCollations().isEmpty())
        .itemIf("offset", offset, offset != null)
        .itemIf("fetch", fetch, fetch != null);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    double rows = table.getRowCount();
    if (offset != null) {
      rows = Math.max(rows - RexLiteral.intValue(offset), 0d);
    }
    if (fetch != null) {
      rows = Math.min(rows, RexLiteral.intValue(fetch));
    }
    return rows;
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Always cheaper than sorting and limiting the full scan above the table
    return planner.getCostFactory().makeTinyCost();
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
    final boolean allColumns =
        projects.equals(ImmutableIntList.identity(table.getRowType().getFieldCount()));
    final Scanner scanner =
        new Scanner(cloudOpsTable, allColumns ? null : projects.toIntArray(), tableCollation,
            offset, fetch);
    final Expression scannerExpression = implementor.stash(scanner, Scanner.class);
    return implementor.result(physType,
        Blocks.toBlock(
            Expressions.call(scannerExpression, "scan", implementor.getRootExpression())));
  }

  /** What one sorted scan asks of its table; the generated code calls {@link #scan}. */
  public static class Scanner {
    private final AbstractCloudOpsTable table;
    private final int @Nullable [] projects;
    private final RelCollation collation;
    private final @Nullable RexNode offset;
    private final @Nullable RexNode fetch;

    Scanner(AbstractCloudOpsTable table, int @Nullable [] projects, RelCollation collation,
        @Nullable RexNode offset, @Nullable RexNode fetch) {
      this.table = table;
      this.projects = projects;
      this.collation = collation;
      this.offset = offset;
      this.fetch = fetch;
    }

    public Enumerable<Object[]> scan(DataContext root) {
      return table.scan(root, new ArrayList<RexNode>(), projects, collation, offset, fetch);
    }
  }
}
