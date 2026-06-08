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
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression representing a scan of an OpenAPI endpoint.
 *
 * <p>Additional operations might be applied using the "find" method.
 */
public class OpenAPITableScan extends TableScan implements OpenAPIRel {

  private final OpenAPITable openAPITable;
  private final RelDataType projectRowType;

  /**
   * Creates an OpenAPITableScan.
   *
   * @param cluster Cluster
   * @param traitSet Trait set
   * @param table Table
   * @param openAPITable OpenAPI table
   * @param projectRowType Fields and types to project; null to project raw row
   */
  OpenAPITableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, OpenAPITable openAPITable,
      RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.openAPITable = requireNonNull(openAPITable, "openAPITable");
    this.projectRowType = projectRowType;

    assert getConvention() == OpenAPIRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new OpenAPITableScan(getCluster(), traitSet, table, openAPITable, projectRowType);
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Estimate cost based on projected fields
    final float projectionFactor =
        projectRowType == null ? 1f
            : (float) projectRowType.getFieldCount() / 100f;
    return super.computeSelfCost(planner, mq).multiplyBy(0.1 * projectionFactor);
  }

  @Override public void register(RelOptPlanner planner) {
    // Register converter rule to convert back to enumerable
    planner.addRule(OpenAPIToEnumerableConverterRule.INSTANCE);

    // Register pushdown rules
    for (RelOptRule rule : OpenAPIRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public void implement(Implementor implementor) {
    implementor.openAPITable = openAPITable;
    implementor.table = table;
  }
}
